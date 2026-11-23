"""
amazon_inbound.py: Amazon FBA shipments through the Fulfillment Inbound API v2024-03-20
(Amazon's "Send to Amazon" as an API), for Transcal cartons the agent planned.

David's rule (Nov 23 2026): the agent may set up an UNCONFIRMED draft before asking him, so his
email shows the real Amazon warehouses, the placement fee and the UPS cost. Nothing is confirmed,
charged or sent until he replies YES.

    create_draft(client, name, source_address, cartons, box_spec, ready_start)
        createInboundPlan -> packing option -> box contents -> placement options (cheapest)
        -> UPS partnered small-parcel quotes. Returns a JSON-safe draft; confirms NOTHING.
    confirm_draft(client, draft)
        confirmPlacementOption -> confirmTransportationOptions -> FBA shipment ids -> box labels.
    cancel_draft(client, inbound_plan_id)
    ticket_shipments(draft, confirmed=None) -> [amazon_pick_ticket.Shipment] for the Transcal ticket

Every Amazon step is asynchronous (an operationId to poll). A failed step raises InboundError
naming the step and Amazon's own problem text, so the email can say exactly what went wrong.
Written to Amazon's v2024-03-20 reference; the first live drafts are checked by hand.
"""
import time
from datetime import datetime, timedelta, timezone

import amazon_skus as K
from amazon_pick_ticket import Address, Carton, Shipment

API = '/inbound/fba/2024-03-20'
LABELS_V0 = '/fba/inbound/v0/shipments/{}/labels'
PARTNERED = 'AMAZON_PARTNERED_CARRIER'
SMALL_PARCEL = 'GROUND_SMALL_PARCEL'
BOX_CONTENT = 'BOX_CONTENT_PROVIDED'
DEFAULT_PAGE_TYPE = 'PackageLabel_Thermal'      # thermal labels, works with partnered UPS


class InboundError(Exception):
    def __init__(self, step, message):
        self.step = step
        self.message = str(message)[:600]
        super().__init__(f'{step}: {self.message}')


# FC street addresses read off real FBA box labels (Jul-Sep 2026 tickets). Amazon's own address
# for a partnered-UPS shipment can be empty or not the real dock before confirmation, so a known
# label address wins; a new FC falls back to what Amazon returns.
KNOWN_FC = {
    'SWF2': ('557 Tuckahoe Road', 'Yonkers', 'NY', '10710'),
    'RDU2': ('2150 US HWY 70 Business West', 'Smithfield', 'NC', '27577'),
    'LGB8': ('1568 N Linden Ave', 'Rialto', 'CA', '92376'),
    'FWA4': ('9798 Smith Road', 'Fort Wayne', 'IN', '46809'),
    'PSP3': ('20821 Krameria Avenue', 'March Air Reserve Base', 'CA', '92518'),
    'ONT8': ('24300 Nandina Ave', 'Moreno Valley', 'CA', '92551'),
    'AVP1': ('550 Oak Ridge Road', 'Hazle Township', 'PA', '18202'),
    'FTW1': ('33333 Lyndon B Johnson Fwy', 'Dallas', 'TX', '75241'),
    'LAX9': ('11263 Oleander Ave', 'Fontana', 'CA', '92337'),
    'VGT2': ('6401 Howdy Wells Ave', 'Las Vegas', 'NV', '89115'),
    'GYR2': ('17341 W Minnezona Ave', 'Goodyear', 'AZ', '85395'),
    'PSC2': ('1351 S Road 40 E', 'Pasco', 'WA', '99301'),
    'IND9': ('1151 S Graham Rd', 'Greenwood', 'IN', '46143'),
    'MDW2': ('250 Emerald Dr', 'Joliet', 'IL', '60433'),
    'ABQ2': ('6251 Pioneer Trail NW', 'Los Lunas', 'NM', '87031'),
}


def fc_address(fc, amazon_address=None):
    """(address dict, source): a label-verified address when the FC is known, else Amazon's,
    else a placeholder that points Transcal at the box labels."""
    known = KNOWN_FC.get(str(fc or '').upper())
    if known:
        return {'line1': known[0], 'line2': '', 'city': known[1], 'state': known[2], 'zip': known[3]}, 'label-verified'
    a = amazon_address or {}
    addr = {'line1': a.get('addressLine1') or '', 'line2': a.get('addressLine2') or '',
            'city': a.get('city') or '', 'state': a.get('stateOrProvinceCode') or '',
            'zip': str(a.get('postalCode') or '')}
    if addr['line1'] and addr['city'] and addr['state'] and addr['zip']:
        return addr, 'amazon'
    return {'line1': f'Amazon {fc} (address on the box labels)', 'line2': '', 'city': 'See box labels',
            'state': 'NA', 'zip': '00000'}, 'missing'


# ── plumbing ─────────────────────────────────────────────────────────────────
def _sleep_clock(client):
    return getattr(client, '_sleep', time.sleep), getattr(client, '_clock', time.monotonic)


def wait(client, operation_id, step, timeout=600.0, poll=2.0):
    """Poll an async operation. Returns its WARNING problems; raises on FAILED or timeout."""
    sleep, clock = _sleep_clock(client)
    deadline = clock() + timeout
    while True:
        op = client.call('GET', f'{API}/operations/{operation_id}', 'getInboundOperationStatus')
        status = str(op.get('operationStatus') or '')
        problems = op.get('operationProblems') or []
        if status == 'SUCCESS':
            return [f"{p.get('code')}: {p.get('message')}" for p in problems]
        if status == 'FAILED':
            text = '; '.join(f"{p.get('code')}: {p.get('message')}" for p in problems) or 'Amazon gave no reason'
            raise InboundError(step, text)
        if clock() >= deadline:
            raise InboundError(step, 'Amazon did not finish in time')
        sleep(poll)


def _post_and_wait(client, path, op, step, body=None, method='POST'):
    r = client.call(method, path, op, body=body)
    op_id = r.get('operationId')
    if not op_id:
        raise InboundError(step, 'Amazon returned no operation id')
    return r, wait(client, op_id, step)


def _paged(client, path, op, key, page_size, params=None, max_pages=100):
    out, token = [], None
    for _ in range(max_pages):
        q = dict(params or {})
        q['pageSize'] = page_size
        if token:
            q['paginationToken'] = token
        body = client.call('GET', path, op, params=q)
        out.extend(body.get(key) or [])
        token = (body.get('pagination') or {}).get('nextToken')
        if not token:
            return out
    return out


def _net_fee(option):
    """Fees minus discounts, in the option's currency (Amazon Incentive objects)."""
    total = 0.0
    for inc in (option.get('fees') or []) + (option.get('discounts') or []):
        amt = float(((inc.get('value') or {}).get('amount')) or 0)
        total += -amt if str(inc.get('type') or '').upper() == 'DISCOUNT' else amt
    return round(total, 2)


def _currency(option):
    for inc in (option.get('fees') or []) + (option.get('discounts') or []):
        code = (inc.get('value') or {}).get('code')
        if code:
            return code
    return 'USD'


def ready_window_start(now=None, days_ahead=1):
    """Next business day at 16:00 UTC (morning in California), Amazon's yyyy-MM-ddTHH:mmZ."""
    d = (now or datetime.now(timezone.utc)) + timedelta(days=days_ahead)
    while d.weekday() >= 5:
        d += timedelta(days=1)
    return d.replace(hour=16, minute=0, second=0, microsecond=0).strftime('%Y-%m-%dT%H:%MZ')


# ── owners (US: never AMAZON) ────────────────────────────────────────────────
def owners_for(client, mskus, label_default='NONE'):
    """{msku: (labelOwner, prepOwner)} from Amazon's prep details. Versa ships manufacturer
    barcodes (no FNSKU sticker, David Aug 2026), so labels default to NONE unless Amazon says the
    SKU must carry a seller label. Prep is SELLER when Amazon lists a prep type, else NONE."""
    out = {}
    mskus = list(dict.fromkeys(mskus))
    for i in range(0, len(mskus), 100):
        chunk = mskus[i:i + 100]
        body = client.call('GET', f'{API}/items/prepDetails', 'listPrepDetails',
                           params={'marketplaceId': client.config.marketplace_id, 'mskus': chunk})
        for d in body.get('mskuPrepDetails') or []:
            types = [t for t in (d.get('prepTypes') or []) if t not in ('ITEM_NO_PREP', 'NONE')]
            prep = 'SELLER' if types else 'NONE'
            if d.get('prepOwnerConstraint') == 'NONE_ONLY':
                prep = 'NONE'
            elif d.get('prepOwnerConstraint') == 'SELLER_ONLY':
                prep = 'SELLER'
            label = label_default
            if d.get('labelOwnerConstraint') == 'SELLER_ONLY':
                label = 'SELLER'
            elif d.get('labelOwnerConstraint') == 'NONE_ONLY':
                label = 'NONE'
            out[str(d.get('msku'))] = (label, prep)
    for m in mskus:
        out.setdefault(m, (label_default, 'NONE'))
    return out


# ── the draft ────────────────────────────────────────────────────────────────
def _carton_groups(cartons):
    """Identical cartons grouped: [(contents dict, count)]."""
    groups = []
    for c in cartons:
        for g in groups:
            if g[0] == c.contents:
                g[1] += 1
                break
        else:
            groups.append([dict(c.contents), 1])
    return groups


def _assign(cartons, shipment_items):
    """Give each carton to the shipment whose items cover it. Amazon places whole boxes, so the
    shipments' item totals are sums of our boxes; a carton that fits nowhere means Amazon split
    the boxes differently and the draft cannot be trusted."""
    left = {sid: dict(items) for sid, items in shipment_items.items()}
    placed = {sid: [] for sid in shipment_items}
    order = sorted(cartons, key=lambda c: (-len(c.contents), c.warehouse_sku))
    for c in order:
        for sid in placed:
            if all(left[sid].get(m, 0) >= q for m, q in c.contents.items()):
                for m, q in c.contents.items():
                    left[sid][m] -= q
                placed[sid].append(c)
                break
        else:
            raise InboundError('placement', f'Amazon did not place a whole carton of {c.warehouse_sku}')
    extra = {sid: {m: q for m, q in v.items() if q} for sid, v in left.items()}
    if any(extra.values()):
        raise InboundError('placement', f'Amazon placed units outside the planned cartons: {extra}')
    return placed


def create_draft(client, name, source_address, cartons, box_spec, ready_start, label_default='NONE'):
    """Set up an UNCONFIRMED inbound plan for these cartons and price it. Confirms nothing.
    source_address: Amazon AddressInput dict (Transcal). box_spec: {'length','width','height',
    'weight_lb'} for a 36-pc carton. Returns a JSON-safe draft dict."""
    if not cartons:
        raise InboundError('plan', 'no cartons')
    units = {}
    for c in cartons:
        for m, q in c.contents.items():
            units[m] = units.get(m, 0) + q
    owners = owners_for(client, list(units), label_default)
    items = [{'msku': m, 'quantity': q, 'labelOwner': owners[m][0], 'prepOwner': owners[m][1]}
             for m, q in sorted(units.items())]
    warnings = []

    r, w = _post_and_wait(client, f'{API}/inboundPlans', 'createInboundPlan', 'create the plan', body={
        'destinationMarketplaces': [client.config.marketplace_id], 'sourceAddress': source_address,
        'items': items, 'name': str(name)[:40]})
    warnings += w
    plan_id = r.get('inboundPlanId')
    draft = {'inbound_plan_id': plan_id, 'name': str(name)[:40], 'warnings': warnings,
             'owners': {m: list(v) for m, v in owners.items()}, 'box_spec': dict(box_spec)}
    try:
        _, w = _post_and_wait(client, f'{API}/inboundPlans/{plan_id}/packingOptions',
                              'generatePackingOptions', 'packing options')
        warnings += w
        options = _paged(client, f'{API}/inboundPlans/{plan_id}/packingOptions', 'listPackingOptions',
                         'packingOptions', 20)
        usable = [o for o in options if o.get('status') == 'OFFERED' and _packs_by_box_content(o)]
        if not usable:
            raise InboundError('packing options', 'Amazon offered no packing option with box contents and UPS')
        packing = sorted(usable, key=_net_fee)[0]
        draft['packing_option_id'] = packing['packingOptionId']
        _, w = _post_and_wait(client, f"{API}/inboundPlans/{plan_id}/packingOptions/{packing['packingOptionId']}/confirmation",
                              'confirmPackingOption', 'packing option')
        warnings += w

        groups = {}
        for gid in packing.get('packingGroups') or []:
            rows = _paged(client, f'{API}/inboundPlans/{plan_id}/packingGroups/{gid}/items',
                          'listPackingGroupItems', 'items', 100)
            groups[gid] = {str(x.get('msku')) for x in rows}
        packings = []
        for gid, mskus in groups.items():
            mine = [c for c in cartons if set(c.contents) <= mskus]
            if mine:
                packings.append({'packingGroupId': gid, 'boxes': [
                    {'contentInformationSource': BOX_CONTENT,
                     'dimensions': {'length': box_spec['length'], 'width': box_spec['width'],
                                    'height': box_spec['height'], 'unitOfMeasurement': 'IN'},
                     'weight': {'unit': 'LB', 'value': box_spec['weight_lb']},
                     'quantity': n,
                     'items': [{'msku': m, 'quantity': q, 'labelOwner': owners[m][0], 'prepOwner': owners[m][1]}
                               for m, q in sorted(contents.items())]}
                    for contents, n in _carton_groups(mine)]})
        boxed = sum(b['quantity'] for p in packings for b in p['boxes'])
        if boxed != len(cartons):
            raise InboundError('box contents', 'Amazon split one carton\'s sizes across packing groups')
        _, w = _post_and_wait(client, f'{API}/inboundPlans/{plan_id}/packingInformation',
                              'setPackingInformation', 'box contents', body={'packageGroupings': packings})
        warnings += w

        _, w = _post_and_wait(client, f'{API}/inboundPlans/{plan_id}/placementOptions',
                              'generatePlacementOptions', 'placement options')
        warnings += w
        options = _paged(client, f'{API}/inboundPlans/{plan_id}/placementOptions', 'listPlacementOptions',
                         'placementOptions', 20)
        offered = [o for o in options if o.get('status') == 'OFFERED' and o.get('shipmentIds')]
        if not offered:
            raise InboundError('placement options', 'Amazon offered no placement option')
        placement = sorted(offered, key=lambda o: (_net_fee(o), len(o['shipmentIds'])))[0]
        draft.update(placement_option_id=placement['placementOptionId'], placement_fee=_net_fee(placement),
                     currency=_currency(placement))

        items_by_shipment, dest = {}, {}
        for sid in placement['shipmentIds']:
            rows = _paged(client, f'{API}/inboundPlans/{plan_id}/shipments/{sid}/items', 'listShipmentItems',
                          'items', 1000)
            items_by_shipment[sid] = {}
            for x in rows:
                m = str(x.get('msku'))
                items_by_shipment[sid][m] = items_by_shipment[sid].get(m, 0) + int(x.get('quantity') or 0)
            dest[sid] = (client.call('GET', f'{API}/inboundPlans/{plan_id}/shipments/{sid}', 'getShipment')
                         .get('destination') or {})
        placed = _assign(cartons, items_by_shipment)

        _, w = _post_and_wait(client, f'{API}/inboundPlans/{plan_id}/transportationOptions',
                              'generateTransportationOptions', 'UPS quotes', body={
                                  'placementOptionId': placement['placementOptionId'],
                                  'shipmentTransportationConfigurations': [
                                      {'shipmentId': sid, 'readyToShipWindow': {'start': ready_start}}
                                      for sid in placement['shipmentIds']]})
        warnings += w
        transport = _paged(client, f'{API}/inboundPlans/{plan_id}/transportationOptions',
                           'listTransportationOptions', 'transportationOptions', 20,
                           params={'placementOptionId': placement['placementOptionId']})
        picks = _pick_transport(transport, placement['shipmentIds'])

        draft['shipments'] = []
        for i, sid in enumerate(placement['shipmentIds']):
            d = dest[sid]
            t = picks[sid]
            fc = str(d.get('warehouseId') or f'FC{i + 1}').upper()
            addr, source = fc_address(fc, d.get('address'))
            draft['shipments'].append({
                'shipment_id': sid, 'fc': fc, 'address': addr, 'address_source': source,
                'cartons': [[c.warehouse_sku, c.contents] for c in placed[sid]],
                'transport_option_id': t['transportationOptionId'],
                'carrier': ((t.get('carrier') or {}).get('name') or 'UPS'),
                'shipping_cost': float((((t.get('quote') or {}).get('cost') or {}).get('amount')) or 0)})
        draft['shipping_cost'] = round(sum(s['shipping_cost'] for s in draft['shipments']), 2)
        draft['warnings'] = warnings
        return draft
    except Exception:
        try:
            cancel_draft(client, plan_id)        # never leave a half-built plan sitting in Seller Central
        except Exception:
            pass
        raise


def _packs_by_box_content(option):
    for conf in option.get('supportedConfigurations') or []:
        if BOX_CONTENT not in (conf.get('boxPackingMethods') or []):
            continue
        for req in conf.get('shippingRequirements') or []:
            if req.get('solution') == PARTNERED and SMALL_PARCEL in (req.get('modes') or []):
                return True
    for s in option.get('supportedShippingConfigurations') or []:         # older field, same meaning
        if s.get('shippingSolution') == PARTNERED and s.get('shippingMode') == SMALL_PARCEL:
            return True
    return not option.get('supportedConfigurations')


def _pick_transport(options, shipment_ids):
    """Cheapest partnered UPS small-parcel quote per shipment, one carrier for all (Amazon's rule)."""
    by_ship = {}
    for o in options:
        if o.get('shippingSolution') != PARTNERED or o.get('shippingMode') != SMALL_PARCEL:
            continue
        if o.get('preconditions'):
            continue
        by_ship.setdefault(o.get('shipmentId'), []).append(o)
    carriers = None
    for sid in shipment_ids:
        names = {((o.get('carrier') or {}).get('alphaCode') or (o.get('carrier') or {}).get('name')) for o in by_ship.get(sid, [])}
        carriers = names if carriers is None else carriers & names
    if not carriers:
        raise InboundError('UPS quotes', 'Amazon offered no partnered small-parcel carrier for every shipment')
    carrier = sorted(carriers)[0]
    picks = {}
    for sid in shipment_ids:
        mine = [o for o in by_ship[sid]
                if ((o.get('carrier') or {}).get('alphaCode') or (o.get('carrier') or {}).get('name')) == carrier]
        picks[sid] = sorted(mine, key=lambda o: float((((o.get('quote') or {}).get('cost') or {}).get('amount')) or 0))[0]
    return picks


# ── after David's YES ────────────────────────────────────────────────────────
def confirm_draft(client, draft, page_type=DEFAULT_PAGE_TYPE):
    """Confirm placement and UPS, then read the FBA shipment ids and download the box labels.
    Returns {'shipments': [{'shipment_id', 'confirmation_id', 'fc', 'boxes', 'labels_pdf'}], 'warnings'}."""
    plan_id = draft['inbound_plan_id']
    warnings = []
    _, w = _post_and_wait(client, f"{API}/inboundPlans/{plan_id}/placementOptions/{draft['placement_option_id']}/confirmation",
                          'confirmPlacementOption', 'confirm placement')
    warnings += w
    _, w = _post_and_wait(client, f'{API}/inboundPlans/{plan_id}/transportationOptions/confirmation',
                          'confirmTransportationOptions', 'confirm UPS', body={'transportationSelections': [
                              {'shipmentId': s['shipment_id'], 'transportationOptionId': s['transport_option_id']}
                              for s in draft['shipments']]})
    warnings += w
    out = []
    sleep, _clock = _sleep_clock(client)
    for s in draft['shipments']:
        info = client.call('GET', f"{API}/inboundPlans/{plan_id}/shipments/{s['shipment_id']}", 'getShipment')
        conf = info.get('shipmentConfirmationId')
        if not conf:
            raise InboundError('shipment id', f"Amazon has not issued an FBA id for {s['fc']} yet")
        dest = info.get('destination') or {}
        fc = str(dest.get('warehouseId') or s['fc']).upper()
        addr, source = fc_address(fc, dest.get('address'))
        boxes = _paged(client, f"{API}/inboundPlans/{plan_id}/shipments/{s['shipment_id']}/boxes",
                       'listShipmentBoxes', 'boxes', 1000)
        n_boxes = sum(int(b.get('quantity') or 1) for b in boxes) or len(s['cartons'])
        labels = client.call('GET', LABELS_V0.format(conf), 'getLabels', params={
            'PageType': page_type, 'LabelType': 'UNIQUE', 'NumberOfPackages': n_boxes})
        url = ((labels.get('payload') or {}).get('DownloadURL'))
        if not url:
            raise InboundError('box labels', f'Amazon returned no label file for {conf}')
        pdf = client.download_document({'url': url})        # the link lives 15 seconds: fetch at once
        out.append({'shipment_id': s['shipment_id'], 'confirmation_id': conf, 'fc': fc,
                    'address': addr, 'address_source': source, 'boxes': n_boxes, 'labels_pdf': pdf})
        sleep(0.5)
    return {'shipments': out, 'warnings': warnings}


def cancel_draft(client, inbound_plan_id):
    r = client.call('PUT', f'{API}/inboundPlans/{inbound_plan_id}/cancellation', 'cancelInboundPlan')
    op_id = r.get('operationId')
    return wait(client, op_id, 'cancel the draft') if op_id else []


def ticket_shipments(draft, confirmed=None):
    """Transcal ticket shipments from a draft. Before David's YES there is no FBA id yet, so the
    reference reads PENDING-<FC>; after confirmation it is the real FBA shipment id."""
    done = {s['shipment_id']: s for s in ((confirmed or {}).get('shipments') or [])}
    out = []
    for s in draft['shipments']:
        c_ship = done.get(s['shipment_id']) or {}
        a = c_ship.get('address') or s['address']
        fc = c_ship.get('fc') or s['fc']
        cartons = [Carton(wh, contents) for wh, contents in s['cartons']]
        ref = c_ship.get('confirmation_id') or f'PENDING-{fc}'
        out.append(Shipment(ref, fc, Address(a['line1'], a['line2'], a['city'], a['state'], a['zip']), cartons,
                            expected_units=sum(c.units for c in cartons)))
    return out
