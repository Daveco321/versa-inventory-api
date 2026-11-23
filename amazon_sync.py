"""
amazon_sync.py: one read-only pull of what the Amazon agent needs from Amazon, turned into
the planner's Listing rows.

    pull(client, now=None, days=30) -> snapshot
        listings  [[seller SKU exactly as listed, ASIN, status, channel, title]]
                  from the All Listings report (every listing, inactive and zero stock included:
                  MRP's export drops sizes with no stock and no sales, which made sizes look unlisted)
        fba       {seller SKU: [available, inbound, reserved, unfulfillable]} from FBA inventory
        sold      {seller SKU: units ordered in the last `days` days}, cancelled lines left out
        at, days, counts, problems
    listings_from(snapshot) -> [amazon_planner.Listing]

Data kept to the minimum (Amazon's Data Protection Policy): the orders report is summed per SKU
in memory and dropped. No order number, city or zip is ever stored. A report that fails is
named in `problems` instead of stopping the pull, and the planner then works with what came back.
"""
from datetime import datetime, timedelta, timezone

import amazon_spapi as S
from amazon_planner import Listing

SNAPSHOT_VERSION = 1


def _int(v):
    try:
        return int(float(str(v).replace(',', '').strip() or 0))
    except (TypeError, ValueError):
        return 0


def _fba_rows(summaries):
    out = {}
    for s in summaries or []:
        sku = str(s.get('sellerSku') or '').strip()
        if not sku:
            continue
        d = s.get('inventoryDetails') or {}
        inbound = (_int(d.get('inboundWorkingQuantity')) + _int(d.get('inboundShippedQuantity'))
                   + _int(d.get('inboundReceivingQuantity')))
        reserved = _int((d.get('reservedQuantity') or {}).get('totalReservedQuantity'))
        unsellable = _int((d.get('unfulfillableQuantity') or {}).get('totalUnfulfillableQuantity'))
        row = out.setdefault(sku, [0, 0, 0, 0])
        row[0] += _int(d.get('fulfillableQuantity'))
        row[1] += inbound
        row[2] += reserved
        row[3] += unsellable
    return out


def _listing_rows(rows):
    out = []
    for r in rows:
        sku = str(r.get('seller-sku') or '').strip()
        if sku:
            out.append([sku, str(r.get('asin1') or '').strip(), str(r.get('status') or '').strip(),
                        str(r.get('fulfillment-channel') or '').strip(), str(r.get('item-name') or '').strip()[:200]])
    return out


def _sold_rows(rows):
    sold = {}
    for r in rows:
        if str(r.get('order-status') or '').strip().lower() == 'cancelled':
            continue
        if str(r.get('item-status') or '').strip().lower() == 'cancelled':
            continue
        channel = str(r.get('sales-channel') or '').strip().lower()
        if channel and not channel.startswith('amazon.'):
            continue                                   # multi-channel (non-Amazon) orders are not Amazon demand
        sku = str(r.get('sku') or '').strip()
        if sku:
            sold[sku] = sold.get(sku, 0) + _int(r.get('quantity'))
    return sold


def pull(client, now=None, days=30):
    """Everything the agent reads from Amazon in one go. Never raises for one failed report."""
    now = now or datetime.now(timezone.utc)
    snap = {'v': SNAPSHOT_VERSION, 'at': now.strftime('%Y-%m-%dT%H:%M:%SZ'), 'days': int(days),
            'listings': [], 'fba': {}, 'sold': {}, 'problems': []}
    try:
        snap['listings'] = _listing_rows(S.parse_tsv(client.run_report(S.REPORT_ALL_LISTINGS)))
    except S.SpApiError as e:
        snap['problems'].append(f'all listings report: {e}')
    try:
        snap['fba'] = _fba_rows(client.inventory_summaries())
    except S.SpApiError as e:
        snap['problems'].append(f'FBA inventory: {e}')
    try:
        data = client.run_report(S.REPORT_ORDERS_BY_DATE, start=now - timedelta(days=int(days)), end=now)
        snap['sold'] = _sold_rows(S.parse_tsv(data))
    except S.SpApiError as e:
        snap['problems'].append(f'orders report: {e}')
    snap['counts'] = {'listings': len(snap['listings']), 'fba_skus': len(snap['fba']),
                      'sold_skus': len(snap['sold']), 'units_sold': sum(snap['sold'].values()),
                      'api_calls': getattr(client, 'calls', None)}
    return snap


def listings_from(snapshot):
    """Planner rows: one per seller SKU in the All Listings report, plus any FBA SKU it lacks.
    Only an 'Active' listing can receive a shipment."""
    fba = snapshot.get('fba') or {}
    sold = snapshot.get('sold') or {}
    out, seen = [], set()
    for sku, asin, status, _channel, _title in snapshot.get('listings') or []:
        if sku in seen:
            continue
        seen.add(sku)
        f = fba.get(sku) or [0, 0, 0, 0]
        out.append(Listing(sku, asin, None, f[0], f[1], sold.get(sku, 0), status.lower() == 'active'))
    for sku, f in fba.items():
        if sku not in seen:
            seen.add(sku)
            out.append(Listing(sku, '', None, f[0], f[1], sold.get(sku, 0), False))
    return out
