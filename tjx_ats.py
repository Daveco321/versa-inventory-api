"""TJMAXX ATS: the TJX catalog's two-tab line sheet (Warehouse + Overseas), built on the
server for the Monday email (David, Oct 5 2026).

It mirrors what the TJX catalog page (customer_logo TM, catalog_slug fffwr26a) computes in
the browser, step for step, from the same anonymous catalog feeds:
  - index.html applyManualAllocationsToInventory -> the per-row committed
  - _buildSmartRoutingCache / _routeSku           -> pnl_routing.route_sku (the Python port)
  - rebuildAppData, filterMode 'ats' and 'incoming'
  - expandItemsToFlowRows + _computeBatchAvailability (one row per delivery)
  - buildCatalogExportItem + _tjxDecorateRow       -> tjx_display.TjxDisplay
Pure: no app import, so it can be unit tested with synthetic feeds. app.py gathers the
feeds and renders the workbook (export_tjx_ats)."""
import math
from datetime import date, datetime, timedelta

import pnl_routing as R

HIDDEN_LANDING_WH = frozenset({'NJ', 'AE', 'AW', 'ABFI'})
SUPPRESS_WINDOW_SECONDS = 14 * 24 * 60 * 60   # index.html _SUPPRESS_WINDOW_MS
SUPPRESS_TOLERANCE = 0.10
STOCK_FIELDS = ('jtw', 'tr', 'dcw', 'qa', 'nj', 'abfi')
NAMED_WAREHOUSES = (('JTW', 'jtw'), ('TR', 'tr'), ('DCW', 'dcw'), ('QA', 'qa'))
NO_DATE = 'TBD'   # the browser prints an em dash; David's copy rule forbids them (Oct 5 2026)


def _num(v):
    """JS `(v || 0)` on a JSON number."""
    try:
        return float(v) if v not in (None, '') else 0
    except (TypeError, ValueError):
        return 0


def _int(v):
    """JS parseInt(v) || 0."""
    return R.to_int(v)


def _js_round(x):
    """JS Math.round: halves round up (Python's round() rounds halves to even)."""
    return int(math.floor(x + 0.5))


def _stock(row):
    return sum(_num(row.get(k)) for k in STOCK_FIELDS)


def ledger_date(raw):
    """index.html _parseLedgerDate: 'YYYY-MM-DD' is a local calendar date, other text goes
    through Date parsing (pnl_routing.parse_date covers the ledger's forms)."""
    return R.parse_date(raw)


def short_date(d):
    """index.html formatDateShort: 'Oct 5, 2026'."""
    if not d:
        return NO_DATE
    return f"{d:%b} {d.day}, {d.year}"


# ── Production (index.html loadProductionFromS3, CATALOG_MODE) ──
def map_production(rows):
    """The page's PRODUCTION_DATA: hidden-landing lots dropped (the catalog feed already
    drops them), arrival = the port date when column G is filled, else ETD + 45 days
    (55 for pants)."""
    out = []
    for i, p in enumerate(rows or []):
        wh = str((p or {}).get('warehouse') or '').strip().upper()
        if wh in HIDDEN_LANDING_WH:
            continue
        style = str(p.get('style') or '').strip().upper()
        etd = ledger_date(p.get('etd'))
        if p.get('port_dated') and p.get('arrival'):
            arrival = ledger_date(p.get('arrival'))
        else:
            arrival = (etd + timedelta(days=R.TRANSIT_DAYS_PANTS if R.is_pants(style) else R.TRANSIT_DAYS)
                       if etd else None)
        out.append({'i': i, 'raw': p, 'production': str(p.get('production') or ''),
                    'poName': str(p.get('poName') or ''), 'style': style, 'units': _int(p.get('units')),
                    'etd': etd, 'arrival': arrival, 'fob_flag': bool(p.get('fob_flag')), 'warehouse': wh})
    return out


class TjxAtsBuilder:
    """One run of the catalog page's numbers. `now` is a naive ET datetime (the page's
    `new Date()`); today is its date (the page's local midnight)."""

    def __init__(self, inventory, production, apo, s3_allocations, manual_allocations,
                 assignments, suppression_overrides, orders, display, now):
        self.now = now
        self.today = now.date()
        self.display = display
        self.apo = list(apo or [])
        self.vw = [a for a in (s3_allocations or []) if (a or {}).get('source') == 's3']
        self.manual = list(manual_allocations or [])
        self.assignments = dict(assignments or {})
        self.no_suppress = {str(s).strip().upper() for s in (suppression_overrides or []) if str(s).strip()}
        self.orders = list(orders) if orders is not None else None
        self.prod = map_production(production)
        self.prod_by_style = {}
        for p in self.prod:
            self.prod_by_style.setdefault(p['style'], []).append(p)
        self.raw_production = list(production or [])
        self.inventory = self._apply_manual_allocations([dict(r) for r in (inventory or [])])
        self.smart = {}         # SKU (upper) -> {'wh', 'os', 'slots'}: the page's _smartRoutingPerSku/Cache
        self._route_all()

    # ── index.html applyManualAllocationsToInventory ──
    def _apply_manual_allocations(self, rows):
        virt = {}
        for a in self.vw + self.manual:
            s = str((a or {}).get('sku') or '').upper()
            if s:
                virt[s] = virt.get(s, 0) + _int(a.get('qty'))
        base = {}
        for r in rows:   # _baseCommitted is keyed by the raw sku; a later duplicate overwrites
            base[r.get('sku')] = (_num(r.get('committed')), _num(r.get('allocated')))
        for r in rows:
            c, a = base.get(r.get('sku'), (_num(r.get('committed')), _num(r.get('allocated'))))
            r['committed'] = c - virt.get(r.get('sku'), 0)   # looked up by the RAW sku, like the page
            r['allocated'] = a
        return rows

    # ── arrival suppression (index.html _isProductionSuppressed / _getSuppressedIncoming) ──
    def _suppressed(self, warehouse_qty, p, sku):
        if sku and str(sku).upper() in self.no_suppress:
            return False
        if not p['arrival'] or p['units'] <= 0:
            return False
        gap = abs((self.now - datetime(p['arrival'].year, p['arrival'].month, p['arrival'].day)).total_seconds())
        if gap > SUPPRESS_WINDOW_SECONDS:
            return False
        # Catalog feeds carry no NJ / ABFI stock and no hidden-landing lots, so the twin is
        # always the factory-served warehouses.
        wq = max(0, warehouse_qty)
        return abs(wq - p['units']) / p['units'] <= SUPPRESS_TOLERANCE

    def _production_for(self, sku):
        return self.prod_by_style.get(str(sku or '').upper(), [])

    def active_production(self, sku, warehouse_qty):
        return [p for p in self._production_for(sku) if not self._suppressed(warehouse_qty, p, sku)]

    def suppressed_incoming(self, sku, warehouse_qty):
        return sum(p['units'] for p in self._production_for(sku) if self._suppressed(warehouse_qty, p, sku))

    # ── index.html _buildSmartRoutingCache + _routeSku ──
    def _route_all(self):
        if self.orders is None and not self.apo and not self.vw:
            return   # the page's early return: no demand data at all
        merged, order = {}, []
        for it in self.inventory:
            sku = str(it.get('sku') or '').upper()
            if not sku:
                continue
            m = merged.get(sku)
            if m is None:
                m = {'sku': sku, 'jtw': 0, 'tr': 0, 'dcw': 0, 'qa': 0, 'nj': 0, 'abfi': 0,
                     'incoming': 0, 'committed': 0, 'allocated': 0}
                merged[sku] = m
                order.append(sku)
            for k in STOCK_FIELDS:
                m[k] += _num(it.get(k))
            for k in ('committed', 'allocated'):   # largest magnitude wins, never summed
                v = _num(it.get(k))
                if abs(v) > abs(m[k]):
                    m[k] = v
        ord_by, apo_by, vw_by = {}, {}, {}
        for i, o in enumerate(self.orders or []):
            s = str((o or {}).get('style') or '').upper()
            if s:
                ord_by.setdefault(s, []).append((i, f'o{i}', o))
        for i, a in enumerate(self.apo):
            s = str((a or {}).get('style') or '').upper()
            if s:
                apo_by.setdefault(s, []).append((i, a))
        for i, v in enumerate(self.vw):
            s = str((v or {}).get('sku') or '').upper()
            if s:
                vw_by.setdefault(s, []).append((i, v))
        led_by = {}
        for i, p in enumerate(self.raw_production):
            if str((p or {}).get('warehouse') or '').strip().upper() in HIDDEN_LANDING_WH:
                continue
            s = str((p or {}).get('style') or '').strip().upper()
            if s:
                led_by.setdefault(s, []).append((i, p))
        for sku in order:
            q = merged[sku]
            q_int = {k: int(q[k]) for k in STOCK_FIELDS + ('committed', 'allocated', 'incoming')}
            lots = R.build_lots(sku, q_int, led_by.get(sku, []), self.no_suppress, self.now)
            res = R.route_sku(sku, q_int, lots, ord_by.get(sku, []), apo_by.get(sku, []), vw_by.get(sku, []),
                              self.today)
            # The page keeps an entry only when the engine ran: a deduction, at least one
            # slot, a passing reconciliation gate and at least one claim (_routeSku returns
            # null for a SKU whose only deduction is a manual allocation, Oct 5 2026 parity).
            if res['status'] != 'routed':
                continue
            wh = os_ = 0
            for s in res['slots']:
                used = sum(c['units'] for c in s['consumers'])
                if s['type'] == 'warehouse':
                    wh += used
                else:
                    os_ += used
            self.smart[sku] = {'wh': wh, 'os': os_,
                               'slots': [s for s in res['slots'] if s['type'] == 'production']}

    def _smart_for(self, raw_sku):
        """The page reads _smartRoutingPerSku[item.sku] with the RAW sku (stored upper)."""
        s = raw_sku if isinstance(raw_sku, str) else ''
        return self.smart.get(s) if s == s.upper() else None

    # ── rebuildAppData ──
    def ats_items(self):
        out = []
        for item in self.inventory:
            warehouse = _stock(item)
            if warehouse <= 0:
                continue
            deductions = abs(_num(item.get('committed'))) + abs(_num(item.get('allocated')))
            incoming = _num(item.get('incoming'))
            assign = self.assignments.get(item.get('sku'))
            smart = self._smart_for(item.get('sku'))
            if assign == 'overseas':
                apply_ded = 0
            elif assign == 'warehouse':
                apply_ded = deductions
            elif smart and incoming > 0:
                apply_ded = smart['wh']
            elif incoming > 0:
                apply_ded = min(deductions, warehouse)
            else:
                apply_ded = deductions
            out.append(dict(item, total_ats=warehouse - apply_ded, total_warehouse=warehouse, incoming=0,
                            _display_mode='ats'))
        return out

    def incoming_items(self):
        out = []
        for item in self.inventory:
            incoming = _num(item.get('incoming'))
            if incoming <= 0:
                continue
            warehouse = _stock(item)
            suppressed = self.suppressed_incoming(item.get('sku'), warehouse)
            adjusted = max(0, incoming - suppressed)
            if adjusted <= 0:
                continue
            deductions = abs(_num(item.get('committed'))) + abs(_num(item.get('allocated')))
            os_ded = 0
            if deductions > 0:
                assign = self.assignments.get(item.get('sku'))
                smart = self._smart_for(item.get('sku'))
                if assign == 'overseas':
                    os_ded = deductions
                elif assign == 'warehouse':
                    os_ded = 0
                elif smart:
                    os_ded = smart['os']
                else:
                    os_ded = max(0, deductions - min(deductions, warehouse))
            row = dict(item, incoming=adjusted, _suppressed_incoming=suppressed, total_ats=adjusted - os_ded,
                       total_warehouse=0, jtw=0, tr=0, dcw=0, qa=0, nj=0, abfi=0,
                       _overseas_deducted=os_ded, _display_mode='overseas')
            if row['total_ats'] <= 0:
                continue   # CATALOG_MODE hides fully-deducted overseas items
            out.append(row)
        return out

    # ── index.html _computeBatchAvailability ──
    def batch_availability(self, sorted_prods, ats_incoming, total_units, sku, legacy_deduction):
        n = len(sorted_prods)
        gross = [0] * n
        so_far = 0
        for idx, p in enumerate(sorted_prods):
            if idx == n - 1:
                units = ats_incoming - so_far   # the last batch absorbs rounding
            else:
                units = _js_round(p['units'] / total_units * ats_incoming) if total_units > 0 else ats_incoming
            so_far += units
            gross[idx] = units
        sku_u = str(sku or '').upper()
        src = self.smart.get(sku_u)
        if src:
            pslots = src['slots']
            n_units = sum(1 for p in sorted_prods if p['units'] > 0)
            if len(pslots) == n_units:
                buckets = {}
                for i, s in enumerate(pslots):
                    buckets.setdefault((s['ref'] or '', s['arrival'] or s['etd']), []).append(i)
                used = [False] * len(pslots)
                g2, d2, a2 = [0] * n, [0] * n, [0] * n
                matched = True
                for idx, p in enumerate(sorted_prods):
                    if p['units'] <= 0:
                        continue   # a cleared row has no engine slot
                    si = -1
                    for c in buckets.get((p['production'].strip(), p['arrival'] or p['etd']), []):
                        if not used[c]:
                            si = c
                            break
                    if si < 0:
                        matched = False
                        break
                    used[si] = True
                    s = pslots[si]
                    g2[idx], a2[idx], d2[idx] = s['orig'], s['left'], s['orig'] - s['left']
                if matched:
                    return g2, d2, a2
        smart = self.smart.get(sku_u)
        remaining = (max(0, min(ats_incoming, smart['os'] or 0)) if smart else max(0, legacy_deduction or 0))
        deducted = [0] * n
        for k in range(n):
            i = (n - 1 - k) if smart else k   # engine total: latest batch first; legacy: FIFO
            d = min(remaining, gross[i])
            deducted[i] = d
            remaining -= d
        return gross, deducted, [gross[i] - deducted[i] for i in range(n)]

    # ── index.html expandItemsToFlowRows ──
    def flow_rows(self, items):
        raw_by_sku = {}
        for r in self.inventory:   # a Map: the last duplicate wins
            raw_by_sku[r.get('sku')] = r
        out = []
        for item in items:
            raw = raw_by_sku.get(item.get('sku'))
            raw_wh = _stock(raw) if raw else 0
            prods = self.active_production(item.get('sku'), raw_wh)
            if not prods:
                out.append(dict(item, _flow=True, _flow_production='', _flow_po='No Production Data',
                                _flow_units=item.get('total_ats') or 0, _flow_deducted=0,
                                _flow_etd=None, _flow_arrival=None))
                continue
            far = date(2099, 1, 1)
            sorted_prods = sorted(prods, key=lambda p: p['arrival'] or p['etd'] or far)
            ats_in = _num(item.get('incoming'))
            total = sum(p['units'] for p in sorted_prods)
            gross, deducted, avail = self.batch_availability(sorted_prods, ats_in, total, item.get('sku'),
                                                             _num(item.get('_overseas_deducted')))
            for idx, p in enumerate(sorted_prods):
                out.append(dict(item, total_ats=avail[idx], incoming=gross[idx], _flow=True,
                                _flow_production=p['production'], _flow_po=p['poName'],
                                _flow_units=gross[idx], _flow_deducted=deducted[idx],
                                _flow_etd=p['etd'], _flow_arrival=p['arrival']))
        return [r for r in out if (r.get('total_ats') or 0) > 0]   # CATALOG_MODE drops 0-ATS rows

    # ── brand keys (rebuildAppData step 4) and buildCatalogExportItem ──
    def with_brands(self, items):
        for it in items:
            key = self.display.brand_key(it.get('sku') or '', it.get('brand'))
            it['brand_abbr'] = key
            it['brand_full'] = self.display.brand_full(key)
        return items

    def export_row(self, item, view):
        d = self.display
        sku = item.get('sku') or ''
        brand = item.get('brand_abbr')
        code, fabrication = d.fabric(sku)
        row = {
            'sku': sku, 'brand_abbr': brand, 'brand_full': item.get('brand_full'),
            'color': d.color_display(sku, brand) or '', 'fit': d.fit_label(sku),
            'fabric_code': code, 'fabrication': fabrication,
            'total_ats': max(0, int(item.get('total_ats') or 0)),
            '_export_category': d.export_category(sku, brand), '_export_fit': d.export_fit(sku),
            '_export_customer': d.export_customer(sku), '_override_size_pack': d.size_pack_override(sku),
            'incoming': int(item.get('incoming') or 0),
        }
        if view != 'incoming':
            names = [lab for lab, k in NAMED_WAREHOUSES if _num(item.get(k)) > 0]
            row['warehouse'] = ', '.join(names) or NO_DATE
        else:
            row['ex_factory'] = short_date(item.get('_flow_etd'))
            row['arrival'] = short_date(item.get('_flow_arrival'))
            if item.get('_flow_production'):
                row['po_ref'] = item['_flow_production']
        row['tjx_color_family'] = d.color_family(sku, brand)
        row['tjx_new_fabric'] = d.new_fabric(sku, brand)
        if item.get('_flow'):
            src = 'Overseas'
        else:
            wh, inc = _stock(item), _num(item.get('incoming'))
            src = ('Warehouse + Overseas' if wh > 0 and inc > 0 else 'Warehouse' if wh > 0
                   else 'Overseas' if inc > 0 else NO_DATE)
        row['tjx_source'] = src
        return row

    def tab_rows(self, view, sku_filter=None, brand_filter=None, min_ats=None):
        """Export rows for one tab, grouped by brand in the platform's brand order and sorted
        by ATS (highest first) inside each brand, like the approved Oct 2 workbook.
        sku_filter(sku) / brand_filter(brand_key) keep rows; min_ats drops smaller rows."""
        items = self.with_brands(self.ats_items() if view == 'ats' else self.incoming_items())
        if sku_filter:
            items = [it for it in items if sku_filter(str(it.get('sku') or ''))]
        if brand_filter:
            items = [it for it in items if brand_filter(it.get('brand_abbr'))]
        by_brand, order = {}, []
        for it in items:
            k = it.get('brand_abbr')
            if k not in by_brand:
                by_brand[k] = []
                order.append(k)
            by_brand[k].append(it)
        rows = []
        for k in sorted(order, key=self.display.brand_sort_key):
            group = sorted(by_brand[k], key=lambda it: -(it.get('total_ats') or 0))
            if view == 'incoming':
                group = self.flow_rows(group)
            out = [self.export_row(it, view) for it in group]
            if min_ats is not None:
                out = [r for r in out if r['total_ats'] >= min_ats]
            rows += sorted(out, key=lambda r: -r['total_ats'])   # stable: ties keep lot order
        return rows


def summarize(rows, overseas=False):
    """Counts for the email body: rows, units, distinct styles, a per-brand split and a
    per-customer-prefix split (TJ/TM against the Ross RO/RM rows on the Warehouse tab)."""
    brands, order = {}, []
    for r in rows:
        b = r.get('brand_full') or r.get('brand_abbr') or ''
        if b not in brands:
            brands[b] = {'brand': b, 'rows': 0, 'units': 0}
            order.append(b)
        brands[b]['rows'] += 1
        brands[b]['units'] += int(r.get('total_ats') or 0)
    prefixes, porder = {}, []
    for r in rows:
        p = str(r.get('sku') or '')[:2].upper()
        if p not in prefixes:
            prefixes[p] = {'prefix': p, 'rows': 0, 'units': 0}
            porder.append(p)
        prefixes[p]['rows'] += 1
        prefixes[p]['units'] += int(r.get('total_ats') or 0)
    out = {'rows': len(rows), 'units': sum(int(r.get('total_ats') or 0) for r in rows),
           'brands': [brands[b] for b in order], 'prefixes': [prefixes[p] for p in porder]}
    if overseas:
        out['styles'] = len({r.get('sku') for r in rows})
    return out
