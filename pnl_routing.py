"""Pure port of the desktop smart-routing engine, with claim-level detail, for the P&L tool.

ADMIN-ONLY P&L support module. It holds no cost data, does no I/O, never imports app and
uses the standard library only. Given the same inputs and the same `today` it returns the
same result (no wall-clock reads).

What it reproduces
==================
The desktop engine `_routeSku` (index.html ~3387-3881, driven by `_buildSmartRoutingCache`)
and its server twin `app.py _pres_route_sku` (~15626), fed the way `_pres_stock_cards`
(~16084) feeds it:
  * duplicate ATS rows merged: warehouses summed, committed / allocated keep the largest
    magnitude; manual + VW allocation qty subtracted into committed (app.py ~16100-16108,
    ~16132-16152, ~16172);
  * per exact SKU supply slots: past productions, the pooled warehouse (JTW+TR+DCW+QA),
    NJ and ABFI as their own last-resort slots, then future productions (date-less last);
  * arrival suppression (14 days, 10 percent) with the S3 suppression overrides
    (app.py ~16184-16202, index.html _isProductionSuppressed);
  * the reconciliation gate, the three claim tiers (pre-pick FIFO, dated latest-feasible,
    APO + VW FIFO), the FOB doctrine, NJ last resort and forced pulls.
Every sort key and visit order is the engine's. Extra claim fields never enter a sort.

route_all(inventory_items, ledger_rows, orders, apo_rows, vw_rows, manual_rows,
          routing_inputs, today, fob_codes=None, options=None) -> dict
==========================================================================
Inputs (B3 wires the production getters, H1 the review getters):

inventory_items  list of ATS rows (the /inventory row shape). Keys read: sku, jtw, tr, dcw,
                 qa, nj, abfi, incoming, committed, allocated (numbers or numeric strings).
                 prod:   `with _inv_lock: list(_inventory.get('items') or [])`
                 review: GET /inventory -> body['inventory']
                 Empty raises RoutingInputError('INVENTORY_UNAVAILABLE') unless
                 options['allowEmptyInventory'].
ledger_rows      list of style-ledger rows (the /production row shape). Keys read:
                 production (the ref), poName, style, units, etd, arrival, port_dated,
                 fob_flag, warehouse (landing, ledger column I), shipmentNo.
                 prod:   `_ledger_rows()`
                 review: GET /production -> body['production']
                 Empty means the ledger is UNAVAILABLE (app.py fails closed with [] for 60 s):
                 raises RoutingInputError('LEDGER_UNAVAILABLE') unless
                 options['allowEmptyLedger']. Never attribute everything to the warehouse.
orders           open-order lines INCLUDING bulks (the engine counts them). Keys read:
                 ctrlNo, orderNo, po, style, customer, customerFull, shipToName, openQty,
                 pickQty, startDate, cancelDate.
                 prod:   `_fetch_all_open_orders()[0]` (stamp the run stale when ok is False)
                 review: open-orders GET /api/orders -> body['orders']
apo_rows         APO rows: style, qty, customer, po.
                 prod:   `with _apo_lock: list(_apo_data or [])`
                 review: GET /apo -> body['apo']
vw_rows          the VIRTUAL WAREHOUSE sheet rows ONLY: sku, qty, customer, po. They are a
                 deduction (subtracted into committed) AND engine claims.
                 prod:   `load_allocation_from_s3() or []`
                 review: GET /allocations -> the rows whose source == 's3'
manual_rows      manual allocations: sku, qty. A deduction only, never a claim.
                 prod:   load_manual_allocations_from_s3(), then
                         `with _manual_alloc_lock: list(_manual_allocations)`
                 review: GET /allocations -> the rows whose source == 'manual'
routing_inputs   dict, defined below.
today            'YYYY-MM-DD', the US Eastern business date
                 (prod: `_pres_now_et().date().isoformat()`). A date, a datetime or
                 'YYYY-MM-DDTHH:MM[:SS]' is also accepted.
fob_codes        the FOB account codes THE ENGINE uses. None (recommended) means
                 ENGINE_FOB_CODES, the curated index.html FOB_CUSTOMER_CODES list, which
                 equals app.py _PRES_FOB_CUSTOMERS. The live /api/fob-customers list may be
                 passed instead; stats['fobCodesMatchEngine'] then records any drift. (The
                 P&L's duty treatment uses its own FOB list; that is not this parameter.)
options          see DEFAULT_OPTIONS.

routing_inputs (exact definition)
---------------------------------
{
  'suppression_overrides': [SKU, ...] | None
        SKUs exempt from arrival suppression (compared stripped and upper-cased).
        prod:   load_suppression_overrides_from_s3(), then
                `with _suppression_overrides_lock: list(_suppression_overrides or [])`
                (S3 key S3_SUPPRESSION_OVERRIDES_KEY, default inventory/suppression_overrides.json)
        review: GET /suppression-overrides -> body['overrides']
        None or missing: no overrides applied; stats['inputs']['suppressionOverrides'] is None.
  'deduction_assignments': {SKU: 'warehouse' | 'overseas'} | None
        Per-SKU manual deduction routing. Other values are ignored (desktop rule,
        index.html ~26448-26452).
        prod:   load_deduction_assignments_from_s3(), then
                `with _deduction_assign_lock: dict(_deduction_assignments or {})`
                (S3 key S3_DEDUCTION_ASSIGN_KEY, default inventory/deduction_assignments.json)
        review: GET /deduction-assignments -> body['assignments']
        None or missing means UNAVAILABLE: assignments are not honored in this run
        (stats['assignmentsHonored'] is False). {} means loaded and empty.
  'now': 'YYYY-MM-DDTHH:MM:SS' | None
        Naive US Eastern wall clock, prod `_pres_now_et().isoformat(timespec='seconds')`.
        Used only by the 14-day arrival-suppression window, which the engine measures from
        the time of day. Missing: `today` when it carries a time, else today at 00:00
        (stats['clock']['source'] says which).
}
Reload all three per build, as _pres_stock_cards does (app.py ~16087-16093): several gunicorn
workers can hold stale copies.

Output
======
{
  'skus': {SKU: {
      'status': 'routed' | 'gate_fail' | 'no_deduction' | 'no_slots' | 'no_claims',
                 what the engine itself decides (checked in the engine's order).
      'mode':   'engine' | 'fifo' | 'engine_ungated' | 'assignment' | 'none',
                 what placed the claims: the engine, the R5 FIFO fallback (gate failure or no
                 deduction), the ungated engine (options gateFallback='engine'), an honored
                 deduction assignment, or nothing (no slots or no claims).
      'assignment': 'warehouse' | 'overseas' | None   (only when honored)
      'gate':   {'demand', 'deduction', 'tolerance', 'pass'}
      'q':      merged ATS row {'jtw','tr','dcw','qa','nj','abfi','incoming','committed',
                 'allocated','virtual'}; committed already includes -virtual.
      'slots':  [{'i', 'type' ('warehouse'|'production'), 'cls', 'ref', 'poName', 'landing',
                  'etd', 'arrival', 'orig', 'left', 'fobFlag', 'nj', 'ledgerIndex',
                  'shipmentNo', 'consumers': [{'claim', 'units', 'forced', 'fobWhFallback',
                  'njPull', 'routing'}]}]
                 cls: warehouse | landed (arrived within 30 days) | future | dateless |
                 stale (arrived over 30 days ago) | nj (NJ or ABFI stock, NJ/AE/AW/ABFI-landing
                 batches). landing: 'WH' for the pooled JTW/TR/DCW/QA slot, 'NJ', 'ABFI', or the
                 ledger column I value. Warehouse slots have arrival None. Consumers keep the
                 engine's push order. njPull follows the engine's _nj flag (forced pulls only).
      'claims': [{'i', 'source' ('order'|'apo'|'vw'), 'key' (line key), 'line' (orders index),
                  'apo' (apo_rows index), 'vw' (vw_rows index), 'orderNo', 'code', 'name',
                  'qty', 'part' ('all'|'open'|'pick'), 'tier' ('prepick'|'dated'|'apovw'),
                  'start', 'cancel', 'fob', 'placed', 'short'}]
      'suppressed': [{'ledgerIndex', 'units', 'arrival'}]   arrival-suppressed ledger rows
  }},
  'lineAlloc':   {lineKey: [row, ...]}      every order line, keyed by line_key() ('ctrlNo|STYLE';
                                             ctrlNo alone is NOT unique). Sum of units ==
                                             openQty + pickQty for qty > 0; [] when qty <= 0.
  'lineSummary': {lineKey: {'sku', 'qty', 'placed', 'unsourced', 'routing' (worst label),
                            'mix' {label: units}, 'status', 'mode'}}
  'apoAlloc':    {apoIndex (int): [row, ...]}   sum == qty for qty > 0
  'vwAlloc':     {vwIndex (int): [row, ...]}
  'ledgerUse':   {ledgerIndex (int): {'sku', 'units', 'claimed', 'left', 'suppressed', 'slot',
                                      'status', 'mode'}}   claimed + left == units on slots
  'stats':       counts only (no money); see route_all.
}
row = {'units', 'kind' ('warehouse'|'production'|'unsourced'), 'sku', 'slot', 'slotCls', 'ref',
       'poName', 'landing', 'etd', 'arrival', 'forced', 'fobWhFallback', 'njPull', 'routing',
       'ledgerIndex', 'shipmentNo', 'part', 'reason'}
reason (unsourced rows only): 'short' (supply ran out) | 'no_slots' | 'not_in_feed' | 'no_style'.
Dates are 'YYYY-MM-DD' strings or None.

Routing labels (DESIGN 5.7, d2 5.3), per row:
  R1 clean engine placement; R2 forced pull; R3 stale or date-less batch; R4 FOB account from
  the US warehouse; R5 FIFO fallback (gate failure, no deduction, ungated engine or a manual
  assignment); R6 no supply record (not in the ATS feed, no slots, or unsourced remainder).
  Precedence within an engine placement: R4, then R3, then R2, else R1.

Known latent divergences between the two reference implementations (0 rows on Sep 14 2026):
  * arrival: the desktop uses the ledger arrival only when port_dated (followed here);
    app.py _apo_prod_arrival uses it whenever present.
  * NJ/ABFI twin for suppression: app.py uses the merged row (followed here); the desktop
    uses the last feed row carrying NJ or ABFI stock.
  * order dates: parsed as naive local timestamps. The desktop would read a date-only or
    'Z' string as UTC midnight, which shifts a same-day feasibility test.
"""
from datetime import date, datetime, timedelta

__all__ = [
    'route_all', 'route_sku', 'engine_totals', 'merge_inventory', 'build_lots', 'line_key',
    'line_keys', 'parse_date', 'parse_datetime', 'is_pants', 'to_int', 'RoutingInputError',
    'ENGINE_FOB_CODES', 'ENGINE_CUSTOMER_NAMES', 'ROUTING_LABELS', 'DEFAULT_OPTIONS',
]

# ── Engine constants (index.html / app.py; public business rules, no cost data) ──
# index.html FOB_CUSTOMER_CODES == app.py _PRES_FOB_CUSTOMERS
ENGINE_FOB_CODES = frozenset({'CENT1', 'GLOB', 'BFL', 'TJXAU', 'TJXUK', 'HALF', 'MULT', 'MULT1'})
# index.html OPEN_ORDERS_CUSTOMER_MAP == app.py _PRES_OO_CUSTOMER_MAP (the engine's name
# fallback, which the pre-pick test reads)
ENGINE_CUSTOMER_NAMES = {
    'BEAL': 'Bealls', 'BEAL1': 'Bealls', 'BJS': "BJ's Wholesale", 'CENT': 'Centric Brands',
    'CENT1': 'Centric Brands', 'COST': 'Costco', 'COST1': 'Costco', 'MULT': 'Multi Brands',
    'MULT1': 'Multi Brands', 'VETE': 'Veterans Canteen', 'WINN': 'Winners/TJX Canada',
    'AAFE': 'AAFES/Military', 'AMAZ': 'Amazon', 'BELK': 'Belk', 'BFL': 'Brands for Less',
    'BLOO': "Bloomingdale's", 'BOSC': 'Boscovs', 'BURL': 'Burlington', 'CITI': 'Citi Trends',
    'COPP': 'Coppel', 'DDS': "DD's Discounts", 'GLOB': 'BBZ (Global)', 'HALF': 'Half Price',
    'HAMR': 'Hamricks', 'HUCK': 'Huckberry', 'JCP01': 'JCPenney', 'KOHL': "Kohl's",
    'MACY': "Macy's", 'MACY1': "Macy's", 'MARS': 'Marshalls', 'NORD': 'Nordstrom',
    'PEER': 'Peerless', 'RED': 'Red Apple', 'ROSS': 'Ross Stores', 'RUEN': 'Rue 21',
    'SEAR': 'Sears', 'TJMA': 'TJ Maxx', 'TJXAU': 'TJX Australia', 'TJXUK': 'TJX UK/TK Maxx',
    'VARI': 'Variety Wholesalers', 'WALM1': 'Walmart'}
HIDDEN_LANDING = frozenset({'NJ', 'AE', 'AW', 'ABFI'})   # index.html HIDDEN_LANDING_WH
SUPPRESS_WINDOW_SECONDS = 14 * 24 * 3600                  # _SUPPRESS_WINDOW_MS
SUPPRESS_TOLERANCE = 0.10                                 # _SUPPRESS_TOLERANCE
LANDED_WINDOW_DAYS = 30                                   # _LANDED_WINDOW_DAYS
GATE_MIN_UNITS = 100                                      # reconciliation gate floor
GATE_SHARE = 0.10                                         # reconciliation gate share
TRANSIT_DAYS = 45                                         # loadProductionFromS3 transit
TRANSIT_DAYS_PANTS = 55
BOTTOM_FABRIC_CODES = frozenset({'BC', 'BR', 'BH', 'BA'})  # SPORTSWEAR_BOTTOM_CODES
FAR_DATE = date(2099, 1, 1)
FAR_DT = datetime(2099, 1, 1)
WAREHOUSE_KEYS = ('jtw', 'tr', 'dcw', 'qa', 'nj', 'abfi')

ROUTING_LABELS = {
    'R1': 'Clean placement',
    'R2': 'Forced pull',
    'R3': 'Stale or undated batch',
    'R4': 'FOB account from US warehouse',
    'R5': 'FIFO fallback',
    'R6': 'No supply record',
}
_ROUTING_RANK = {'R1': 1, 'R2': 2, 'R3': 3, 'R4': 4, 'R5': 5, 'R6': 6}

DEFAULT_OPTIONS = {
    # False = strict engine parity. True = a non-FOB line's pickQty goes to the engine's
    # pre-pick tier (FIFO: warehouse first), as if its customer name contained "pick".
    'picksAsWarehouse': False,
    # Honor per-SKU deduction assignments when routing_inputs carries them (views do).
    'honorAssignments': True,
    # Gate failure / no deduction: 'fifo' = the engine's own FIFO order (R5, DESIGN default);
    # 'engine' = the engine run without the gate (also labeled R5).
    'gateFallback': 'fifo',
    'allowEmptyLedger': False,
    'allowEmptyInventory': False,
}


class RoutingInputError(ValueError):
    """Raised when an input the engine cannot do without is unavailable."""

    def __init__(self, code, detail=''):
        super().__init__(f'{code}: {detail}' if detail else code)
        self.code = code


# ── Small helpers ──
def to_int(v):
    """app.py _pres_int: int(float(v)), 0 when it does not parse."""
    try:
        return int(float(v))
    except (TypeError, ValueError, OverflowError):
        return 0


def _u(v):
    return str(v or '').strip().upper()


_TEXT_DATE_FORMATS = ('%Y-%m-%d', '%m/%d/%Y', '%m/%d/%y', '%m-%d-%Y', '%m-%d-%y', '%Y/%m/%d')


def parse_datetime(v):
    """Naive datetime or None. app.py _apo_parse_date (+ _norm_ledger_date text forms),
    keeping the time of day so claim sorting matches the desktop's Date comparisons."""
    if not v:
        return None
    if isinstance(v, datetime):
        return v.replace(tzinfo=None)
    if isinstance(v, date):
        return datetime(v.year, v.month, v.day)
    s = str(v).strip()
    if not s:
        return None
    try:
        return datetime.fromisoformat(s[:19])
    except ValueError:
        pass
    t = s.split(' ')[0].split('T')[0]
    for fmt in _TEXT_DATE_FORMATS:
        try:
            return datetime.strptime(t, fmt)
        except ValueError:
            continue
    return None


def parse_date(v):
    dt = parse_datetime(v)
    return dt.date() if dt else None


def _dt(d):
    return datetime(d.year, d.month, d.day)


def _iso(d):
    return d.isoformat() if d else None


def is_pants(sku):
    """index.html isPants == app.py _py_is_bottom: the P##X dress-pants serial or a
    sportswear bottoms fabric code (BC/BR/BH/BA)."""
    base = str(sku or '').split('-')[0].upper()
    if (len(base) >= 10 and base[6] == 'P' and '0' <= base[7] <= '9' and '0' <= base[8] <= '9'
            and 'A' <= base[9] <= 'Z'):
        return True
    return len(base) >= 6 and base[4:6] in BOTTOM_FABRIC_CODES


def line_key(order):
    """The P&L line id: 'ctrlNo|STYLE'. ctrlNo alone repeats across a PO's styles."""
    return f"{str((order or {}).get('ctrlNo') or '').strip()}|{_u((order or {}).get('style'))}"


def line_keys(orders):
    """line_key for every order, in order. A repeated key gets '#2', '#3', ... so every line
    stays addressable. pnl_engine must use this same function on the same list."""
    seen, out = {}, []
    for o in orders or []:
        k = line_key(o)
        n = seen.get(k, 0) + 1
        seen[k] = n
        out.append(k if n == 1 else f'{k}#{n}')
    return out


def _resolve_clock(today, routing_inputs):
    """-> (today date, now datetime, source label)."""
    t = parse_datetime(today)
    if t is None:
        raise RoutingInputError('BAD_TODAY', 'today must be YYYY-MM-DD')
    today_d = t.date()
    now = parse_datetime((routing_inputs or {}).get('now'))
    if now is not None:
        return today_d, now, 'routing_inputs.now'
    has_time = isinstance(today, datetime) or (isinstance(today, str) and 'T' in today)
    if has_time:
        return today_d, t, 'today'
    return today_d, _dt(today_d), 'midnight'


# ── Inputs: merged ATS rows and ledger lots (app.py _pres_stock_cards) ──
def merge_inventory(items, manual_rows=(), vw_rows=()):
    """One row per exact SKU (app.py ~16132-16152): warehouses and incoming sum, committed /
    allocated keep the largest magnitude (first row wins ties). Then committed -= the SKU's
    manual + VW qty (app.py ~16100-16104, ~16172; desktop applyManualAllocationsToInventory).
    Returns (merged {SKU: q}, feed order [SKU], duplicate row count)."""
    virt = {}
    for a in list(vw_rows or []) + list(manual_rows or []):
        s = _u((a or {}).get('sku'))
        if s:
            virt[s] = virt.get(s, 0) + to_int(a.get('qty'))
    merged, order, dup = {}, [], 0
    for it in items or []:
        sku = _u((it or {}).get('sku'))
        if not sku:
            continue
        m = merged.get(sku)
        if m is None:
            m = {'jtw': 0, 'tr': 0, 'dcw': 0, 'qa': 0, 'nj': 0, 'abfi': 0, 'incoming': 0,
                 'committed': 0, 'allocated': 0}
            merged[sku] = m
            order.append(sku)
        else:
            dup += 1
        for k in WAREHOUSE_KEYS + ('incoming',):
            m[k] += to_int(it.get(k))
        for k in ('committed', 'allocated'):
            v = to_int(it.get(k))
            if abs(v) > abs(m[k]):
                m[k] = v
    for sku in order:
        m = merged[sku]
        m['virtual'] = virt.get(sku, 0)
        m['committed'] = m['committed'] - m['virtual']
    return merged, order, dup


def build_lots(sku, q, ledger_items, no_suppress, now):
    """This SKU's ledger rows as engine lots (app.py ~16184-16202 + desktop PRODUCTION_DATA
    mapping). ledger_items: [(ledger_index, row)] in ledger order. no_suppress: set of SKUs
    exempt from suppression. now: naive datetime for the 14-day window."""
    pants = is_pants(sku)
    nj, abfi = q['nj'], q['abfi']
    wh_all = sum(q[k] for k in WAREHOUSE_KEYS)
    exempt = sku in no_suppress
    lots = []
    for li, p in ledger_items:
        units = to_int(p.get('units'))
        etd = parse_date(p.get('etd'))
        if p.get('port_dated') and p.get('arrival'):
            arr = parse_date(p.get('arrival'))
        else:
            arr = (etd + timedelta(days=TRANSIT_DAYS_PANTS if pants else TRANSIT_DAYS)) if etd else None
        landing = _u(p.get('warehouse'))
        hidden = landing in HIDDEN_LANDING
        sup = False
        if not exempt and arr and units > 0:
            gap = abs((now - _dt(arr)).total_seconds())
            if gap <= SUPPRESS_WINDOW_SECONDS:
                # NJ-landing batches compare with NJ stock, AE/AW/ABFI with ABFI; any other
                # batch with the factory-served warehouses (desktop _isProductionSuppressed).
                twin = (nj if landing == 'NJ' else abfi) if hidden else max(0, wh_all - nj - abfi)
                sup = abs(twin - units) / units <= SUPPRESS_TOLERANCE
        lots.append({'units': units, 'arr': arr, 'etd': etd, 'fob_flag': bool(p.get('fob_flag')),
                     'hidden': hidden, 'sup': sup, 'ref': str(p.get('production') or '').strip(),
                     'poName': str(p.get('poName') or '').strip(), 'landing': landing,
                     'ledgerIndex': li, 'shipmentNo': str(p.get('shipmentNo') or '').strip()})
    return lots


# ── Engine core (index.html _routeSku; app.py _pres_route_sku) ──
def _build_slots(lots, q, today):
    """[past productions ASC, warehouse, NJ, ABFI, future productions ASC (date-less last)]."""
    wh_total = sum(q[k] for k in WAREHOUSE_KEYS)
    prods = sorted((l for l in lots if not l['sup']), key=lambda l: l['arr'] or l['etd'] or FAR_DATE)

    def pslot(l):
        return {'type': 'production', 'ref': l['ref'], 'poName': l['poName'], 'landing': l['landing'],
                'orig': l['units'], 'left': l['units'], 'arrival': l['arr'] or l['etd'], 'etd': l['etd'],
                'fobFlag': l['fob_flag'], 'nj': l['hidden'], 'ledgerIndex': l['ledgerIndex'],
                'shipmentNo': l['shipmentNo'], 'consumers': []}

    slots = [pslot(l) for l in prods
             if l['units'] > 0 and (l['arr'] or l['etd']) and (l['arr'] or l['etd']) <= today]
    nj_u, abfi_u = max(0, q['nj']), max(0, q['abfi'])
    other = wh_total - nj_u - abfi_u
    for units, restricted, land in ((other, False, 'WH'), (nj_u, True, 'NJ'), (abfi_u, True, 'ABFI')):
        if units > 0:
            slots.append({'type': 'warehouse', 'ref': None, 'poName': None, 'landing': land,
                          'orig': units, 'left': units, 'arrival': today, 'etd': None, 'fobFlag': False,
                          'nj': restricted, 'ledgerIndex': None, 'shipmentNo': None, 'consumers': []})
    slots += [pslot(l) for l in prods
              if l['units'] > 0 and (not (l['arr'] or l['etd']) or (l['arr'] or l['etd']) > today)]
    cutoff = today - timedelta(days=LANDED_WINDOW_DAYS)
    for i, s in enumerate(slots):
        s['i'] = i
        if s['nj']:
            s['cls'] = 'nj'
        elif s['type'] == 'warehouse':
            s['cls'] = 'warehouse'
        elif s['arrival'] and s['arrival'] <= today:
            s['cls'] = 'landed' if s['arrival'] >= cutoff else 'stale'
        else:
            s['cls'] = 'future' if s['arrival'] else 'dateless'
    return slots


def _visit_orders(slots):
    """(fifoOrder, lfOrder, landed index set) exactly as the engine builds them."""
    wh_i, landed_i, fut_i, stale_i, nj_i = [], [], [], [], []
    for i, s in enumerate(slots):
        c = s['cls']
        if c == 'nj':
            nj_i.append(i)
        elif c == 'warehouse':
            wh_i.append(i)
        elif c == 'landed':
            landed_i.append(i)
        elif c == 'stale':
            stale_i.append(i)
        else:
            fut_i.append(i)   # future and date-less

    def arr_desc(idx):   # latest arrival first, date-less last, ties on the higher slot index
        return sorted(idx, key=lambda i: ((-slots[i]['arrival'].toordinal()) if slots[i]['arrival']
                                          else float('inf'), -i))
    fifo = wh_i + landed_i + fut_i + stale_i + nj_i
    lf = arr_desc(fut_i) + wh_i + arr_desc(landed_i) + arr_desc(stale_i) + nj_i
    return fifo, lf, set(landed_i)


def _build_claims(orders, apos, vws, fob_set, picks_as_warehouse):
    """orders: [(orders index, line key, row)]; apos / vws: [(index, row)]. Claim order is the
    engine's: orders in feed order, then APO, then VW."""
    claims = []
    for gi, key, o in orders:
        open_q, pick_q = to_int(o.get('openQty')), to_int(o.get('pickQty'))
        qty = open_q + pick_q
        if qty <= 0:
            continue
        code = _u(o.get('customer'))
        name = str(o.get('customerFull') or ENGINE_CUSTOMER_NAMES.get(code) or o.get('shipToName')
                   or code or 'Order')
        fob = code in fob_set
        base = {'source': 'order', 'key': key, 'line': gi, 'apo': None, 'vw': None,
                'orderNo': str(o.get('orderNo') or o.get('po') or ''), 'code': code, 'name': name,
                'fob': fob, 'startDt': parse_datetime(o.get('startDate')),
                'cancelDt': parse_datetime(o.get('cancelDate'))}
        named_pick = 'pick' in name.lower()   # the engine's pre-pick test
        if picks_as_warehouse and not fob and not named_pick and pick_q > 0:
            pick_part = min(pick_q, qty)
            claims.append(dict(base, qty=pick_part, part='pick', tier='prepick'))
            if qty - pick_part > 0:
                claims.append(dict(base, qty=qty - pick_part, part='open', tier='dated'))
        else:
            claims.append(dict(base, qty=qty, part='all', tier='prepick' if named_pick else 'dated'))
    for rows, src in ((apos, 'apo'), (vws, 'vw')):
        for gi, a in rows:
            qty = to_int(a.get('qty'))
            if qty > 0:
                claims.append({'source': src, 'key': None, 'line': None,
                               'apo': gi if src == 'apo' else None, 'vw': gi if src == 'vw' else None,
                               'orderNo': '', 'code': '', 'name': str(a.get('customer') or src.upper()),
                               'fob': False, 'startDt': None, 'cancelDt': None, 'qty': qty,
                               'part': 'all', 'tier': 'apovw'})
    for i, c in enumerate(claims):
        c['i'] = i
        c['short'] = c['qty']
    return claims


def _take(slot, claim, rem, forced=False, fob_wh=False, nj_pull=False):
    t = min(rem, slot['left'])
    if t > 0:
        slot['left'] -= t
        slot['consumers'].append({'claim': claim['i'], 'units': t, 'forced': forced,
                                  'fobWhFallback': fob_wh, 'njPull': nj_pull})
    return rem - t


def _dated_sort_key(c):
    return c['startDt'] or c['cancelDt'] or FAR_DT


def _alloc_engine(slots, claims):
    """The engine's three tiers: pre-pick FIFO, dated latest-feasible, APO + VW FIFO."""
    fifo, lf, landed_set = _visit_orders(slots)
    has_non_nj = any(not s['nj'] and s['left'] > 0 for s in slots)

    def fifo_walk(c, rem, forced):
        for i in fifo:
            if rem <= 0:
                break
            s = slots[i]
            if s['left'] > 0:
                rem = _take(s, c, rem, forced=forced, nj_pull=bool(s['nj']) if forced else False)
        return rem

    def latest_feasible(c):
        rem, fob, start = c['qty'], c['fob'], c['startDt']
        cancel = c['cancelDt'] or start or FAR_DT          # claim.cancelDate || claim.latestNeed
        if fob:   # pass 0: FOB accounts drain FOB-flagged batches first (slot array order)
            for s in slots:
                if rem <= 0:
                    break
                if s['fobFlag'] and s['type'] == 'production' and s['left'] > 0:
                    rem = _take(s, c, rem)
        for i in lf:   # pass 1: latest feasible
            if rem <= 0:
                break
            s = slots[i]
            if s['left'] <= 0:
                continue
            if s['nj'] and has_non_nj:
                continue   # NJ / ABFI only when it is the style's ONLY supply
            if s['type'] == 'warehouse':
                if fob:
                    continue   # FOB accounts never pull from the US warehouse here
            elif s['fobFlag']:
                continue       # no firm date: not promised to a dated claim
            elif not fob and i in landed_set:
                pass           # landed goods are in hand for any ship window
            else:
                sd = (s['etd'] or s['arrival']) if fob else s['arrival']
                if not sd:
                    continue   # a date-less batch is never feasible for a dated claim
                sdt = _dt(sd)
                if start and sdt > start:
                    continue   # placement targets the START date (David's rule)
                if sdt > cancel:
                    continue
            rem = _take(s, c, rem)
        if rem > 0:
            if fob:   # roll FORWARD by ETD, NJ-landing last; then the US warehouse as last resort
                fwd = sorted([i for i, s in enumerate(slots) if s['type'] == 'production' and s['left'] > 0],
                             key=lambda i: (1 if slots[i]['nj'] else 0,
                                            slots[i]['etd'] or slots[i]['arrival'] or FAR_DATE))
                for i in fwd:
                    if rem <= 0:
                        break
                    if slots[i]['left'] > 0:
                        rem = _take(slots[i], c, rem, forced=True, nj_pull=bool(slots[i]['nj']))
                for s in slots:
                    if rem <= 0:
                        break
                    if s['type'] == 'warehouse' and s['left'] > 0:
                        rem = _take(s, c, rem, forced=True, fob_wh=True)
            else:
                rem = fifo_walk(c, rem, True)
        return rem

    prepick = [c for c in claims if c['tier'] == 'prepick']
    dated = sorted([c for c in claims if c['tier'] == 'dated'], key=_dated_sort_key)
    apovw = [c for c in claims if c['tier'] == 'apovw']
    for c in prepick:
        c['short'] = fifo_walk(c, c['qty'], False)
    for c in dated:
        c['short'] = latest_feasible(c)
    for c in apovw:
        c['short'] = fifo_walk(c, c['qty'], False)


def _alloc_fallback(slots, claims, pool):
    """R5: every claim through the engine's fifoOrder, in the engine's tier order (pre-pick,
    dated by start date, APO + VW). FOB accounts take FOB-flagged batches, then production,
    and the US warehouse only as a flagged last resort.
    pool 'all' = gate failure or no deduction; 'warehouse' / 'overseas' = a deduction
    assignment (the other pool only takes the overflow, flagged forced)."""
    fifo, _lf, _landed = _visit_orders(slots)
    wh_seq = [i for i in fifo if slots[i]['type'] == 'warehouse']
    prod_seq = [i for i in fifo if slots[i]['type'] == 'production']
    flag_seq = [i for i, s in enumerate(slots) if s['type'] == 'production' and s['fobFlag']]

    def walk(seq, c, rem, forced=False, fob_wh=False):
        for i in seq:
            if rem <= 0:
                break
            s = slots[i]
            if s['left'] > 0:
                rem = _take(s, c, rem, forced=forced, fob_wh=fob_wh)
        return rem

    ordered = ([c for c in claims if c['tier'] == 'prepick']
               + sorted([c for c in claims if c['tier'] == 'dated'], key=_dated_sort_key)
               + [c for c in claims if c['tier'] == 'apovw'])
    for c in ordered:
        rem, fob = c['qty'], c['fob']
        if pool == 'warehouse':
            rem = walk(wh_seq, c, rem, fob_wh=fob)
            rem = walk(prod_seq, c, rem, forced=True)
        elif pool == 'overseas':
            if fob:
                rem = walk(flag_seq, c, rem)
            rem = walk(prod_seq, c, rem)
            rem = walk(wh_seq, c, rem, forced=True, fob_wh=fob)
        elif fob:
            rem = walk(flag_seq, c, rem)
            rem = walk(prod_seq, c, rem)
            rem = walk(wh_seq, c, rem, forced=True, fob_wh=True)
        else:
            rem = walk(fifo, c, rem)
        c['short'] = rem


def _label_consumers(slots, mode):
    for s in slots:
        for cons in s['consumers']:
            if mode != 'engine':
                cons['routing'] = 'R5'
            elif cons['fobWhFallback']:
                cons['routing'] = 'R4'
            elif s['type'] == 'production' and s['cls'] in ('stale', 'dateless'):
                cons['routing'] = 'R3'
            elif cons['forced']:
                cons['routing'] = 'R2'
            else:
                cons['routing'] = 'R1'


def route_sku(sku, q, lots, orders, apos, vws, today, fob_set=ENGINE_FOB_CODES, *,
              picks_as_warehouse=False, assignment=None, gate_fallback='fifo'):
    """Route ONE exact SKU. q: merged row (merge_inventory); lots: build_lots();
    orders: [(orders index, line key, row)]; apos / vws: [(index, row)]; today: date.
    Returns {'sku','status','mode','assignment','gate','slots','claims'} with internal date
    objects (route_all converts them). Status checks run in the engine's order."""
    total_ded = abs(q['committed']) + abs(q['allocated'])
    slots = _build_slots(lots, q, today)
    claims = _build_claims(orders, apos, vws, fob_set, picks_as_warehouse)
    demand = (sum(to_int(o.get('openQty')) + to_int(o.get('pickQty')) for _, _, o in orders)
              + sum(to_int(a.get('qty')) for _, a in apos) + sum(to_int(v.get('qty')) for _, v in vws))
    tol = max(GATE_MIN_UNITS, total_ded * GATE_SHARE)
    gate = {'demand': demand, 'deduction': total_ded, 'tolerance': tol,
            'pass': abs(demand - total_ded) <= tol}
    if total_ded == 0:
        status = 'no_deduction'
    elif not slots:
        status = 'no_slots'
    elif not gate['pass']:
        status = 'gate_fail'
    elif not claims:
        status = 'no_claims'
    else:
        status = 'routed'
    if assignment not in ('warehouse', 'overseas'):
        assignment = None
    if not slots or not claims:
        mode = 'none'
    elif assignment:
        mode = 'assignment'
    elif status == 'routed':
        mode = 'engine'
    elif gate_fallback == 'engine':
        mode = 'engine_ungated'
    else:
        mode = 'fifo'
    if mode in ('engine', 'engine_ungated'):
        _alloc_engine(slots, claims)
    elif mode == 'assignment':
        _alloc_fallback(slots, claims, assignment)
    elif mode == 'fifo':
        _alloc_fallback(slots, claims, 'all')
    _label_consumers(slots, mode)
    for c in claims:
        c['placed'] = c['qty'] - c['short']
    return {'sku': sku, 'status': status, 'mode': mode,
            'assignment': assignment if mode == 'assignment' else None,
            'gate': gate, 'slots': slots, 'claims': claims}


def engine_totals(res):
    """What app.py _pres_route_sku returns for this SKU: {'wh','os','os_visible'}, or None when
    the engine does not route it (only mode 'engine'; compare with default options)."""
    if not res or res.get('mode') != 'engine':
        return None
    wh = os_ = vis = 0
    for s in res['slots']:
        used = sum(c['units'] for c in s['consumers'])
        if s['type'] == 'warehouse':
            wh += used
        else:
            os_ += used
            if not s['nj']:
                vis += used
    return {'wh': wh, 'os': os_, 'os_visible': vis}


# ── route_all: every SKU, every order line, APO row and VW row ──
def _alloc_row(sku, slot, cons, claim):
    wh = slot['type'] == 'warehouse'
    return {'units': cons['units'], 'kind': slot['type'], 'sku': sku, 'slot': slot['i'],
            'slotCls': slot['cls'], 'ref': slot['ref'], 'poName': slot['poName'],
            'landing': slot['landing'], 'etd': _iso(slot['etd']),
            'arrival': None if wh else _iso(slot['arrival']),
            'forced': cons['forced'], 'fobWhFallback': cons['fobWhFallback'],
            'njPull': cons['njPull'], 'routing': cons['routing'], 'ledgerIndex': slot['ledgerIndex'],
            'shipmentNo': slot['shipmentNo'], 'part': claim['part'], 'reason': None}


def _unsourced_row(sku, units, part, reason):
    return {'units': units, 'kind': 'unsourced', 'sku': sku, 'slot': None, 'slotCls': None,
            'ref': None, 'poName': None, 'landing': None, 'etd': None, 'arrival': None,
            'forced': False, 'fobWhFallback': False, 'njPull': False, 'routing': 'R6',
            'ledgerIndex': None, 'shipmentNo': None, 'part': part, 'reason': reason}


def _sku_out(res, q, lots):
    slots = [{'i': s['i'], 'type': s['type'], 'cls': s['cls'], 'ref': s['ref'], 'poName': s['poName'],
              'landing': s['landing'], 'etd': _iso(s['etd']),
              'arrival': None if s['type'] == 'warehouse' else _iso(s['arrival']),
              'orig': s['orig'], 'left': s['left'], 'fobFlag': s['fobFlag'], 'nj': s['nj'],
              'ledgerIndex': s['ledgerIndex'], 'shipmentNo': s['shipmentNo'],
              'consumers': s['consumers']} for s in res['slots']]
    claims = [{'i': c['i'], 'source': c['source'], 'key': c['key'], 'line': c['line'],
               'apo': c['apo'], 'vw': c['vw'], 'orderNo': c['orderNo'], 'code': c['code'],
               'name': c['name'], 'qty': c['qty'], 'part': c['part'], 'tier': c['tier'],
               'start': _iso(c['startDt'].date()) if c['startDt'] else None,
               'cancel': _iso(c['cancelDt'].date()) if c['cancelDt'] else None,
               'fob': c['fob'], 'placed': c['placed'], 'short': c['short']} for c in res['claims']]
    return {'status': res['status'], 'mode': res['mode'], 'assignment': res['assignment'],
            'gate': res['gate'],
            'q': {k: q[k] for k in WAREHOUSE_KEYS + ('incoming', 'committed', 'allocated', 'virtual')},
            'slots': slots, 'claims': claims,
            'suppressed': [{'ledgerIndex': l['ledgerIndex'], 'units': l['units'], 'arrival': _iso(l['arr'])}
                           for l in lots if l['sup']]}


def _bump(d, k, n=1):
    d[k] = d.get(k, 0) + n


def _block_stats(allocs, qtys):
    out = {'rows': len(allocs), 'units': 0, 'placed': 0, 'unsourced': 0, 'byKind': {},
           'byRouting': {}, 'notInFeed': {'rows': 0, 'units': 0}}
    for k, rows in allocs.items():
        if qtys.get(k, 0) > 0:
            out['units'] += qtys[k]
        for r in rows:
            _bump(out['byKind'], r['kind'], r['units'])
            _bump(out['byRouting'], r['routing'], r['units'])
            out['unsourced' if r['kind'] == 'unsourced' else 'placed'] += r['units']
            if r['reason'] in ('not_in_feed', 'no_style'):
                out['notInFeed']['rows'] += 1
                out['notInFeed']['units'] += r['units']
    return out


def _outside(style_key):
    return 'not_in_feed' if style_key else 'no_style'


def route_all(inventory_items, ledger_rows, orders, apo_rows, vw_rows, manual_rows,
              routing_inputs, today, fob_codes=None, options=None):
    """Route every SKU of the ATS feed and attribute every order line, APO row and VW row to
    the supply that serves it. Inputs and output: see the module docstring.
    Raises RoutingInputError on unusable input (empty ledger or ATS feed, bad today)."""
    opts = dict(DEFAULT_OPTIONS)
    for k, v in (options or {}).items():
        if k in opts:
            opts[k] = v
    if opts['gateFallback'] not in ('fifo', 'engine'):
        raise RoutingInputError('BAD_OPTION', 'gateFallback must be fifo or engine')
    ri = routing_inputs or {}
    today_d, now, clock_src = _resolve_clock(today, ri)
    inventory_items = list(inventory_items or [])
    ledger_rows = list(ledger_rows or [])
    orders = [o or {} for o in (orders or [])]
    apo_rows = [a or {} for a in (apo_rows or [])]
    vw_rows = [v or {} for v in (vw_rows or [])]
    manual_rows = list(manual_rows or [])
    if not inventory_items and not opts['allowEmptyInventory']:
        raise RoutingInputError('INVENTORY_UNAVAILABLE', 'the ATS feed is empty')
    if not ledger_rows and not opts['allowEmptyLedger']:
        raise RoutingInputError('LEDGER_UNAVAILABLE', 'the style ledger is empty')
    fob_set = ENGINE_FOB_CODES if fob_codes is None else frozenset(_u(c) for c in fob_codes if _u(c))
    sup_raw = ri.get('suppression_overrides')
    no_suppress = {_u(s) for s in (sup_raw or []) if _u(s)}
    asg_raw = ri.get('deduction_assignments')
    honor = bool(opts['honorAssignments']) and isinstance(asg_raw, dict)
    assignments = {}
    if isinstance(asg_raw, dict):
        for k, v in asg_raw.items():
            if _u(k) and v in ('warehouse', 'overseas'):
                assignments[_u(k)] = v
    picks = bool(opts['picksAsWarehouse'])

    merged, feed_order, dup_rows = merge_inventory(inventory_items, manual_rows, vw_rows)
    keys = line_keys(orders)
    led_by, ord_by, apo_by, vw_by = {}, {}, {}, {}
    for i, p in enumerate(ledger_rows):
        s = _u((p or {}).get('style'))
        if s:
            led_by.setdefault(s, []).append((i, p))
    for i, o in enumerate(orders):
        s = _u(o.get('style'))
        if s:
            ord_by.setdefault(s, []).append((i, keys[i], o))
    for i, a in enumerate(apo_rows):
        s = _u(a.get('style'))
        if s:
            apo_by.setdefault(s, []).append((i, a))
    for i, v in enumerate(vw_rows):
        s = _u(v.get('sku'))
        if s:
            vw_by.setdefault(s, []).append((i, v))

    skus_out = {}
    line_alloc = {k: [] for k in keys}
    apo_alloc = {i: [] for i in range(len(apo_rows))}
    vw_alloc = {i: [] for i in range(len(vw_rows))}
    targets = {'order': line_alloc, 'apo': apo_alloc, 'vw': vw_alloc}
    ledger_use = {}
    for i, p in enumerate(ledger_rows):
        u = to_int((p or {}).get('units'))
        ledger_use[i] = {'sku': _u((p or {}).get('style')), 'units': u, 'claimed': 0, 'left': u,
                         'suppressed': False, 'slot': None, 'status': 'not_in_feed', 'mode': None}
    status_n, mode_n = {}, {}
    sup_lots = sup_units = 0

    def target_key(c):
        return c['key'] if c['source'] == 'order' else c[c['source']]

    for sku in feed_order:
        q = merged[sku]
        lots = build_lots(sku, q, led_by.get(sku, []), no_suppress, now)
        res = route_sku(sku, q, lots, ord_by.get(sku, []), apo_by.get(sku, []), vw_by.get(sku, []),
                        today_d, fob_set, picks_as_warehouse=picks,
                        assignment=assignments.get(sku) if honor else None,
                        gate_fallback=opts['gateFallback'])
        _bump(status_n, res['status'])
        _bump(mode_n, res['mode'])
        for l in lots:
            lu = ledger_use[l['ledgerIndex']]
            lu['status'], lu['mode'] = res['status'], res['mode']
            if l['sup']:
                lu['suppressed'] = True
                sup_lots += 1
                sup_units += max(0, l['units'])
        claims = res['claims']
        for s in res['slots']:
            if s['type'] == 'production':
                lu = ledger_use[s['ledgerIndex']]
                lu['slot'], lu['claimed'], lu['left'] = s['i'], s['orig'] - s['left'], s['left']
            for cons in s['consumers']:
                c = claims[cons['claim']]
                targets[c['source']][target_key(c)].append(_alloc_row(sku, s, cons, c))
        reason = 'short' if res['slots'] else 'no_slots'
        for c in claims:
            if c['short'] > 0:
                targets[c['source']][target_key(c)].append(
                    _unsourced_row(sku, c['short'], c['part'], reason))
        skus_out[sku] = _sku_out(res, q, lots)

    # Demand whose SKU is not in the ATS feed has no supply record (R6).
    for i, o in enumerate(orders):
        s = _u(o.get('style'))
        qty = to_int(o.get('openQty')) + to_int(o.get('pickQty'))
        if s not in merged and qty > 0:
            line_alloc[keys[i]].append(_unsourced_row(s or None, qty, 'all', _outside(s)))
    for rows, alloc, fld in ((apo_rows, apo_alloc, 'style'), (vw_rows, vw_alloc, 'sku')):
        for i, a in enumerate(rows):
            s = _u(a.get(fld))
            qty = to_int(a.get('qty'))
            if s not in merged and qty > 0:
                alloc[i].append(_unsourced_row(s or None, qty, 'all', _outside(s)))

    line_summary = {}
    ls = {'count': len(orders), 'units': 0, 'placed': 0, 'unsourced': 0, 'byKind': {},
          'byRouting': {}, 'linesByRouting': {}, 'notInFeed': {'lines': 0, 'units': 0},
          'zeroQty': 0, 'forcedUnits': 0, 'fobWhFallbackUnits': 0,
          'pick': {'lines': 0, 'units': 0, 'byKindNonFob': {}, 'byKindFob': {}}}
    for i, o in enumerate(orders):
        k = keys[i]
        s = _u(o.get('style'))
        open_q, pick_q = to_int(o.get('openQty')), to_int(o.get('pickQty'))
        qty = open_q + pick_q
        rows = line_alloc[k]
        mix, placed, uns = {}, 0, 0
        for r in rows:
            _bump(mix, r['routing'], r['units'])
            if r['kind'] == 'unsourced':
                uns += r['units']
            else:
                placed += r['units']
            _bump(ls['byKind'], r['kind'], r['units'])
            _bump(ls['byRouting'], r['routing'], r['units'])
            if r['forced']:
                ls['forcedUnits'] += r['units']
            if r['fobWhFallback']:
                ls['fobWhFallbackUnits'] += r['units']
        worst = max(mix, key=lambda lb: _ROUTING_RANK[lb]) if mix else None
        if s in skus_out:
            st, md = skus_out[s]['status'], skus_out[s]['mode']
        else:
            st, md = _outside(s), 'none'
        line_summary[k] = {'sku': s or None, 'qty': qty, 'placed': placed, 'unsourced': uns,
                           'routing': worst, 'mix': mix, 'status': st, 'mode': md}
        if qty <= 0:
            ls['zeroQty'] += 1
            continue
        ls['units'] += qty
        ls['placed'] += placed
        ls['unsourced'] += uns
        _bump(ls['linesByRouting'], worst)
        if st in ('not_in_feed', 'no_style'):
            ls['notInFeed']['lines'] += 1
            ls['notInFeed']['units'] += qty
        if pick_q > 0:   # where the engine puts units that are already on a pick ticket
            ls['pick']['lines'] += 1
            ls['pick']['units'] += pick_q
            tgt = ls['pick']['byKindFob' if _u(o.get('customer')) in fob_set else 'byKindNonFob']
            for r in rows:
                if r['part'] == 'pick' or (r['part'] == 'all' and open_q <= 0):
                    kind = r['kind'] if r['kind'] != 'production' else 'production_' + str(r['slotCls'])
                    _bump(tgt, kind, r['units'])

    nf = [lu for lu in ledger_use.values() if lu['status'] == 'not_in_feed']
    stats = {
        'clock': {'today': today_d.isoformat(), 'now': now.isoformat(timespec='seconds'),
                  'source': clock_src},
        'options': dict(opts, honorAssignments=honor),
        'assignmentsHonored': honor,
        'fobCodes': sorted(fob_set),
        'fobCodesMatchEngine': fob_set == ENGINE_FOB_CODES,
        'inputs': {'inventoryRows': len(inventory_items), 'skus': len(feed_order),
                   'duplicateRowsMerged': dup_rows, 'ledgerRows': len(ledger_rows),
                   'orders': len(orders), 'apoRows': len(apo_rows), 'vwRows': len(vw_rows),
                   'manualRows': len(manual_rows),
                   'suppressionOverrides': None if sup_raw is None else len(no_suppress),
                   'deductionAssignments': len(assignments) if isinstance(asg_raw, dict) else None},
        'skuStatus': status_n,
        'skuMode': mode_n,
        'assignmentsApplied': mode_n.get('assignment', 0),
        'suppressedLots': sup_lots,
        'suppressedUnits': sup_units,
        'ledgerNotInFeed': {'rows': len(nf), 'units': sum(max(0, lu['units']) for lu in nf)},
        'lines': ls,
        'apo': _block_stats(apo_alloc, {i: to_int(a.get('qty')) for i, a in enumerate(apo_rows)}),
        'vw': _block_stats(vw_alloc, {i: to_int(v.get('qty')) for i, v in enumerate(vw_rows)}),
    }
    return {'skus': skus_out, 'lineAlloc': line_alloc, 'lineSummary': line_summary,
            'apoAlloc': apo_alloc, 'vwAlloc': vw_alloc, 'ledgerUse': ledger_use, 'stats': stats}
