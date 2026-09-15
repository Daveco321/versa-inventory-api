"""Offline support for the Past Orders AI tool tests (INV-01 to INV-09).

Everything here is synthetic: made-up customer codes (all start with Z),
made-up PO numbers, styles and prices. No test touches the network: sockets
are blocked before app.py is imported, and the open-orders service is
replaced by FakeOpenOrders, which answers either like the current service
(mode 'old': no history block, exact-text PO matching) or with the new
history contract (mode 'new')."""
import copy
import os
import socket
import sys
from datetime import datetime

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BASE = 'http://open-orders.test'
LATEST = '2026-09-15'


def _blocked(*_a, **_k):
    raise OSError('network is blocked in these tests')


_APP = None


def load_app():
    """Import app.py once, offline."""
    global _APP
    if _APP is not None:
        return _APP
    for s in (sys.stdout, sys.stderr):
        try:
            s.reconfigure(encoding='utf-8', errors='replace')
        except Exception:
            pass
    socket.socket.connect = _blocked
    socket.socket.connect_ex = _blocked
    socket.create_connection = _blocked
    os.environ['OPEN_ORDERS_API_URL'] = BASE
    if ROOT not in sys.path:
        sys.path.insert(0, ROOT)
    import app as a
    _APP = a
    return a


# ── synthetic data ─────────────────────────────────────────────────────────

def _line(style, qty, price):
    return {'style': style, 'baseStyle': style, 'qty': qty, 'price': price, 'value': round(qty * price, 2)}


def _po(cust, full, order_no, start, last_seen, shipped, lines, peak=None, new=None):
    lines = [_line(*l) for l in lines]
    peak_lines = [_line(*l) for l in peak] if peak else None
    p = {'customer': cust, 'customerFull': full, 'orderNo': order_no, 'start': start,
         'cancel': '', 'firstSeen': '2026-06-03', 'lastSeen': last_seen,
         'lines': lines, 'linesPeak': peak_lines,
         'units': sum(l['qty'] for l in lines), 'value': round(sum(l['value'] for l in lines), 2),
         'unitsPeak': sum(l['qty'] for l in (peak_lines or lines)),
         'valuePeak': round(sum(l['value'] for l in (peak_lines or lines)), 2),
         'styleCount': len(lines), 'shipped': shipped}
    p['_new'] = new or {}
    return p


def _inv(inv, date, cust, po, qty, style, value, div='OB', color='NAVY'):
    return {'inv': inv, 'date': date, 'cust': cust, 'po': po, 'div': div, 'qty': qty,
            'style': style, 'value': value, 'color': color}


def _invoiced(first, last, qty, value, raw_pos, n=1):
    return {'status': 'invoiced', 'bucket': 'invoiced', 'counted': True,
            'statusLabel': 'Invoiced ' + last,
            'invoice': {'rawPos': raw_pos, 'qty': qty, 'value': value, 'firstInv': first,
                        'lastInv': last, 'invoices': n},
            'countUnits': qty, 'countValue': value, 'countBasis': 'invoice', 'countDate': last}


def synthetic_data():
    A, AF = 'ZALPHA', 'Alpha Test Stores'
    B1, BF = 'ZBETA1', 'Beta Test Group'
    archive = [
        # A1: zero-padded on the book, unpadded on the invoice.
        _po(A, AF, '0077001001', '2026-07-01', '2026-07-10', True, [('ZZTEST001SLS', 1000, 3.0)],
            new=_invoiced('2026-07-09', '2026-07-09', 1000, 3000.0, ['77001001'])),
        # A2: invoiced Jul 31, left the book Aug 5 (a month crosser).
        _po(A, AF, '77001002', '2026-07-15', '2026-08-05', True, [('ZZTEST002SLS', 600, 2.5)],
            new=_invoiced('2026-07-31', '2026-07-31', 600, 1500.0, ['77001002'])),
        # A3: left before its window, but invoiced (an early shipment).
        _po(A, AF, '77001003', '2026-09-01', '2026-08-10', False, [('ZZTEST003SLS', 400, 2.0)],
            new=_invoiced('2026-08-10', '2026-08-10', 400, 800.0, ['77001003'])),
        # A4: shipped after the cut-off, no invoice yet; partly cut on the book.
        _po(A, AF, '77001004', '2026-08-20', '2026-09-02', True, [('ZZTEST001SLS', 300, 3.0)],
            peak=[('ZZTEST001SLS', 500, 3.0)],
            new={'status': 'shipped_awaiting_invoice', 'bucket': 'awaiting', 'counted': True,
                 'statusLabel': 'Left the book Sep 3. Awaiting invoice (estimate)',
                 'estUnits': 480, 'estValue': 1440.0,
                 'estLines': [{'style': 'ZZTEST001SLS', 'qty': 480, 'price': 3.0, 'value': 1440.0}],
                 'estShipDate': '2026-09-01', 'leftBook': ['2026-09-03', '2026-09-03'],
                 'countUnits': 480, 'countValue': 1440.0, 'countBasis': 'estimate',
                 'countDate': '2026-09-01'}),
        # A5: left before its window, nothing picked, no invoice.
        _po(A, AF, '77001005', '2026-10-01', '2026-09-03', False, [('ZZTEST004SLS', 200, 2.0)],
            new={'status': 'left_before_window', 'bucket': 'not_counted', 'counted': False,
                 'statusLabel': 'Left before its ship window. No invoice'}),
        # A6: invoiced before the cut-off, the rest shipped after it (a split PO).
        _po(A, AF, '66-000001', '2026-08-01', '2026-09-04', True, [('ZZTEST002SLS', 100, 2.5)],
            peak=[('ZZTEST002SLS', 700, 2.5)],
            new=dict(_invoiced('2026-08-21', '2026-08-21', 600, 1500.0, ['66-000001']),
                     afterCutoffEstimate={'units': 100, 'value': 250.0, 'estShipDate': '2026-09-03'})),
        # A7: left on the newest file: not confirmed yet.
        _po(A, AF, '10-424242', '2026-09-01', '2026-09-14', True, [('ZZTEST005SLS', 250, 4.0)],
            new={'status': 'left_pending', 'bucket': 'pending', 'counted': False,
                 'statusLabel': 'Left the book Sep 14. Not confirmed yet'}),
        # A8: the goods moved to another PO number.
        _po(A, AF, '77001008', '2026-08-01', '2026-08-12', True, [('ZZTEST006SLS', 300, 2.0)],
            new={'status': 'moved', 'bucket': 'not_counted', 'counted': False,
                 'statusLabel': 'Moved to PO 77001009', 'movedTo': ['77001009'], 'movedKind': 'ctrl'}),
        # A9: left inside its window with nothing picked: likely shipped.
        _po(A, AF, '77001010', '2026-08-25', '2026-09-08', True, [('ZZTEST001SLS', 120, 3.0)],
            new={'status': 'left_in_window_unpicked', 'bucket': 'awaiting', 'counted': True,
                 'statusLabel': 'Left the book Sep 8, not picked. Likely shipped (estimate)',
                 'estUnits': 120, 'estValue': 360.0,
                 'estLines': [{'style': 'ZZTEST001SLS', 'qty': 120, 'price': 3.0, 'value': 360.0}],
                 'estShipDate': '2026-09-04', 'countUnits': 120, 'countValue': 360.0,
                 'countBasis': 'estimate', 'countDate': '2026-09-04'}),
        # B1: an archive-only customer; renumbered, so never counted on the new service.
        _po(B1, BF, '88000001', '2026-07-01', '2026-07-14', True, [('ZZTEST008SLS', 900, 2.0)],
            new={'status': 'moved', 'bucket': 'not_counted', 'counted': False,
                 'statusLabel': 'Moved to PO 88000101', 'movedTo': ['88000101'], 'movedKind': 'ctrl'}),
    ]
    invoices = [
        _inv('90001', '2026-07-09', A, '77001001', 1000, 'ZZTEST001SLS', 3000.0),
        _inv('90002', '2026-07-31', A, '77001002', 600, 'ZZTEST002SLS', 1500.0),
        _inv('90003', '2026-08-10', A, '77001003', 400, 'ZZTEST003SLS', 800.0),
        _inv('90006', '2026-08-21', A, '66-000001', 600, 'ZZTEST002SLS', 1500.0),
        _inv('90101', '2025-05-01', 'ZBETA', '12345', 300, 'ZZTEST009SLS', 600.0),
        _inv('90201', '2026-07-20', 'ZGAMDROP', '6.00E+11', 5, 'ZZTEST001SLS', 15.0, div='DS'),
        _inv('90301', '2026-06-15', 'ZOMEGA', '10424242', 999, 'ZZTEST005SLS', 3996.0),
    ]
    # A style with more invoice lines than one page: the newest 300 belong to
    # ZOMEGA, the older 320 to ZALPHA.
    n = 0
    for i in range(300):
        n += 1
        invoices.append(_inv(f'8{n:05d}', f'2025-{1 + i % 12:02d}-{1 + i % 28:02d}', 'ZOMEGA',
                             f'4400{i:04d}', 10, 'ZZCAP001SLS', 20.0))
    for i in range(320):
        n += 1
        invoices.append(_inv(f'8{n:05d}', f'2024-{1 + i % 12:02d}-{1 + i % 28:02d}', A,
                             f'4500{i:04d}', 7, 'ZZCAP001SLS', 14.0))
    open_orders = [
        {'customer': B1, 'customerFull': BF, 'orderNo': '88000101', 'style': 'ZZTEST008SLS',
         'openQty': 900, 'pickQty': 0, 'openValue': 1800.0, 'pickValue': 0.0,
         'startDate': '2026-10-01', 'cancelDate': '2026-10-15', 'isPipeline': False},
        {'customer': A, 'customerFull': AF, 'orderNo': '77002001', 'style': 'ZZTEST001SLS',
         'openQty': 240, 'pickQty': 60, 'openValue': 720.0, 'pickValue': 180.0,
         'startDate': '2026-10-05', 'cancelDate': '2026-10-20', 'isPipeline': False},
    ]
    history = {'contract': 1, 'invoicesThrough': {'OB': '2026-08-21', 'DS': '2026-07-31'},
               'invoiceIngestedAt': '2026-08-27T19:00:00', 'viewReady': True,
               'viewBuiltAt': '2026-09-15T05:00:00', 'viewKey': 'view-1', 'estimateRule': 'pick',
               'archive': {'from': '2026-06-03', 'latestDate': LATEST, 'confirmedThrough': '2026-09-12'},
               'flags': {'staleDays': [], 'suspectDays': [], 'degraded': False, 'unscopedMatches': 0},
               'label': ('Invoices through Aug 21, 2026. Dropship invoices through Jul 31, 2026. '
                         'Shipments after that are estimates from the order book.')}
    return {'archive': archive, 'invoices': invoices, 'open_orders': open_orders, 'history': history}


# ── fake open-orders service ───────────────────────────────────────────────

class FakeResp:
    def __init__(self, status, payload):
        self.status_code = status
        self._payload = payload

    def json(self):
        if isinstance(self._payload, Exception):
            raise self._payload
        return copy.deepcopy(self._payload)


def _rollups(rows, with_lines=True):
    """Per-PO rollups, grouped by PO text the way the current service does."""
    pos = {}
    for r in rows:
        po = r['po'] or ('INV ' + r['inv'])
        p = pos.get(po)
        if not p:
            p = pos[po] = {'po': po, 'customer': r['cust'], 'div': r['div'], 'qty': 0, 'value': 0.0,
                           'firstInv': r['date'], 'lastInv': r['date'], '_invs': set(), '_lines': {}}
        p['qty'] += r['qty']
        p['value'] += r['value']
        p['_invs'].add(r['inv'])
        p['firstInv'] = min(p['firstInv'], r['date'])
        p['lastInv'] = max(p['lastInv'], r['date'])
        ln = p['_lines'].setdefault(r['style'].upper(), {'style': r['style'], 'qty': 0, 'value': 0.0,
                                                         'color': r['color']})
        ln['qty'] += r['qty']
        ln['value'] = round(ln['value'] + r['value'], 2)
    out = []
    for p in pos.values():
        p['invoices'] = len(p.pop('_invs'))
        p['value'] = round(p['value'], 2)
        lines = sorted(p.pop('_lines').values(), key=lambda l: -l['qty'])
        p['styleCount'] = len(lines)
        if with_lines:
            p['lines'] = lines
        out.append(p)
    out.sort(key=lambda p: p['lastInv'], reverse=True)
    return out


class FakeOpenOrders:
    """Stands in for app.http_requests. Records every call."""

    def __init__(self, data, mode='old'):
        self.data = data
        self.mode = mode
        self.calls = []
        self.fail = {}          # (method, path) -> HTTP status, or an Exception to raise
        self.po_history_override = {}   # account code or None -> payload

    # requests-like surface used by app._oo_json
    def get(self, url, headers=None, params=None, timeout=None):
        return self._handle('GET', url, dict(params or {}), None)

    def post(self, url, headers=None, json=None, timeout=None):
        return self._handle('POST', url, {}, copy.deepcopy(json))

    def count(self, method, path):
        return sum(1 for c in self.calls if c[0] == method and c[1] == path)

    def _handle(self, method, url, params, body):
        assert url.startswith(BASE), url
        path = url[len(BASE):]
        self.calls.append((method, path, params, body))
        f = self.fail.get((method, path))
        if isinstance(f, Exception):
            raise f
        if f:
            return FakeResp(f, {'error': f'simulated failure {f}'} if f != 503 else
                            {'error': 'Past orders are temporarily unavailable. Try again in a minute.'})
        if path == '/api/po-history':
            acct = (params.get('account') or '').strip().upper() or None
            if acct in self.po_history_override:
                return FakeResp(200, self.po_history_override[acct])
            return FakeResp(200, self.po_history(acct))
        if path == '/api/sales-history':
            return FakeResp(200, self.sales(params))
        if path == '/api/sales-history/po-lookup':
            return FakeResp(200, self.po_lookup(body or {}))
        return FakeResp(404, {'error': 'not found'})

    # ── archive ──
    def rows(self, account=None):
        out = []
        for p in self.data['archive']:
            if account and p['customer'] != account:
                continue
            q = {k: v for k, v in p.items() if k != '_new'}
            if self.mode == 'new':
                q.update(copy.deepcopy(p['_new']))
                q['poKey'], q['poKind'] = _key(p['orderNo'])
            out.append(q)
        out.sort(key=lambda p: p['lastSeen'], reverse=True)
        return out

    def _acct_new_fields(self, rows):
        f = {'invoicedPos': 0, 'invoicedUnits': 0, 'invoicedValue': 0.0,
             'awaitingPos': 0, 'awaitingUnits': 0, 'awaitingValue': 0.0,
             'pendingPos': 0, 'pendingValue': 0.0, 'notCountedPos': 0, 'notCountedValue': 0.0}
        for p in rows:
            b = p.get('bucket')
            if b == 'invoiced':
                f['invoicedPos'] += 1
                f['invoicedUnits'] += p['invoice']['qty']
                f['invoicedValue'] += p['invoice']['value']
            elif b == 'awaiting':
                f['awaitingPos'] += 1
                f['awaitingUnits'] += p['estUnits']
                f['awaitingValue'] += p['estValue']
            elif b == 'pending':
                f['pendingPos'] += 1
                f['pendingValue'] += p['value']
            else:
                f['notCountedPos'] += 1
                f['notCountedValue'] += p['value']
            ace = p.get('afterCutoffEstimate')
            if ace:
                f['awaitingUnits'] += ace['units']
                f['awaitingValue'] += ace['value']
        for k in list(f):
            if k.endswith('Value'):
                f[k] = round(f[k], 2)
        return f

    def po_history(self, account=None):
        base = {'success': True, 'building': False, 'progress': {'done': 0, 'total': 0}, 'ready': True,
                'latestDate': LATEST}
        if self.mode == 'new':
            base['history'] = copy.deepcopy(self.data['history'])
        if account:
            rows = self.rows(account)
            base.update(account=account, pos=rows,
                        count=sum(1 for r in rows if r['shipped']),
                        cancelledCount=sum(1 for r in rows if not r['shipped']))
            return base
        accts = {}
        for p in self.rows():
            a = accts.setdefault(p['customer'], {'customer': p['customer'], 'customerFull': p['customerFull'],
                                                 'poCount': 0, 'units': 0, 'value': 0.0,
                                                 'cancelledCount': 0, 'cancelledValue': 0.0,
                                                 'firstReceived': '', 'lastReceived': '', '_rows': []})
            a['_rows'].append(p)
            if self.mode == 'new':
                counted = p['counted']
            else:
                counted = p['shipped']
            if counted:
                a['poCount'] += 1
                if self.mode == 'new':
                    a['units'] += p.get('countUnits') or 0
                    a['value'] += (p.get('countValue') or 0) + ((p.get('afterCutoffEstimate') or {}).get('value') or 0)
                    a['units'] += (p.get('afterCutoffEstimate') or {}).get('units') or 0
                else:
                    a['units'] += p['units']
                    a['value'] += p['value']
                a['firstReceived'] = min(a['firstReceived'] or p['lastSeen'], p['lastSeen'])
                a['lastReceived'] = max(a['lastReceived'], p['lastSeen'])
            elif self.mode == 'new' and p.get('bucket') == 'pending':
                pass
            else:
                a['cancelledCount'] += 1
                a['cancelledValue'] += p['value']
        accounts = []
        for a in accts.values():
            rows = a.pop('_rows')
            a['value'] = round(a['value'], 2)
            a['cancelledValue'] = round(a['cancelledValue'], 2)
            if self.mode == 'new':
                a.update(self._acct_new_fields(rows))
            accounts.append(a)
        accounts.sort(key=lambda a: -a['value'])
        totals = {'accounts': len(accounts), 'pos': sum(a['poCount'] for a in accounts),
                  'units': sum(a['units'] for a in accounts),
                  'value': round(sum(a['value'] for a in accounts), 2),
                  'cancelled': sum(a['cancelledCount'] for a in accounts)}
        base.update(accounts=accounts, totals=totals, datesCount=105, builtAt='2026-09-15T04:00:00')
        return base

    # ── invoices ──
    def _pending_rows(self, account=None, lo=None, hi=None):
        if self.mode != 'new':
            return []
        out = []
        for p in self.rows(account):
            if p.get('bucket') == 'awaiting':
                d = p.get('estShipDate') or ''
            elif p.get('afterCutoffEstimate'):
                d = p['afterCutoffEstimate'].get('estShipDate') or ''
            else:
                continue
            if lo and not (lo <= d <= hi):
                continue
            out.append(p)
        return out

    def _history_extra(self):
        return {'history': copy.deepcopy(self.data['history'])} if self.mode == 'new' else {}

    def sales(self, params):
        inv = self.data['invoices']
        account = (params.get('account') or '').strip().upper()
        style_q = (params.get('style') or '').strip().upper()
        if not account and not style_q:
            return self.sales_summary()
        if style_q:
            hits = [{'customer': r['cust'], 'po': r['po'], 'invoice': r['inv'], 'date': r['date'],
                     'style': r['style'], 'qty': r['qty'], 'value': r['value'], 'color': r['color'],
                     'div': r['div']} for r in inv if style_q in r['style'].upper()]
            hits.sort(key=lambda h: h['date'], reverse=True)
            out = {'ready': True, 'style_query': style_q, 'matched_qty': sum(h['qty'] for h in hits),
                   'matched_value': round(sum(h['value'] for h in hits), 2), 'matched_rows': len(hits)}
            if self.mode == 'new':
                off = int(params.get('offset') or 0)
                lim = min(int(params.get('limit') or 500), 2000)
                out.update(rows=hits[off:off + lim], truncated=off + lim < len(hits), offset=off, limit=lim)
                pend = []
                for p in self._pending_rows():
                    for l in (p.get('estLines') or []):
                        if style_q in l['style'].upper():
                            pend.append({'customer': p['customer'], 'po': p['orderNo'], 'style': l['style'],
                                         'qty': l['qty'], 'value': l['value'], 'estShipDate': p['estShipDate'],
                                         'status': p['status']})
                out['pendingRows'] = pend
                out.update(self._history_extra())
            else:
                out.update(rows=hits[:500], truncated=len(hits) > 500)
            return out
        mine = [r for r in inv if r['cust'] == account]
        month_f = (params.get('month') or '').strip()
        from_f = (params.get('from') or '').strip()
        to_f = (params.get('to') or '').strip()
        if month_f or from_f or to_f:
            lo = (month_f + '-01') if month_f else (from_f or '0000-00-00')
            hi = (month_f + '-31') if month_f else (to_f or '9999-99-99')
            sel = [r for r in mine if lo <= r['date'] <= hi]
            pos = _rollups(sel, with_lines=True)
            out = {'ready': True, 'account': account, 'from': lo, 'to': hi, 'poCount': len(pos),
                   'units': sum(p['qty'] for p in pos), 'value': round(sum(p['value'] for p in pos), 2),
                   'months': [], 'topStyles': [], 'styleCount': 0, 'pos': pos[:1500],
                   'truncated': len(pos) > 1500}
            if self.mode == 'new':
                rows = self._pending_rows(account, lo, hi)
                out['pending'] = {'pos': len(rows), 'rows': rows}
                out.update(self._history_extra())
            return out
        if not mine:
            out = {'ready': True, 'account': account, 'rows': 0, 'months': [], 'years': [],
                   'topStyles': [], 'posRecent': [], 'poCount': 0}
        else:
            pos_all = _rollups(mine, with_lines=False)
            out = {'ready': True, 'account': account, 'rows': len(mine), 'months': [], 'years': [],
                   'divisions': {'OB': {'units': sum(r['qty'] for r in mine), 'value': 0.0}},
                   'topStyles': [], 'poCount': len(pos_all), 'posRecent': pos_all[:300]}
        if self.mode == 'new':
            rows = self._pending_rows(account)
            u = sum((p['afterCutoffEstimate']['units'] if p.get('bucket') == 'invoiced' else p['estUnits'])
                    for p in rows)
            v = round(sum((p['afterCutoffEstimate']['value'] if p.get('bucket') == 'invoiced' else p['estValue'])
                          for p in rows), 2)
            out['pending'] = {'pos': len(rows), 'units': u, 'value': v, 'months': [], 'rows': rows}
            out['excluded'] = {'count': 0, 'byStatus': {}}
            out.update(self._history_extra())
        return out

    def sales_summary(self):
        inv = self.data['invoices']
        custs = {}
        for r in inv:
            c = custs.setdefault(r['cust'], {'customer': r['cust'], 'units': 0, 'value': 0.0,
                                             'firstInv': r['date'], 'lastInv': r['date'], '_i': set(),
                                             'divisions': {}, 'monthly': {}})
            c['units'] += r['qty']
            c['value'] += r['value']
            c['_i'].add(r['inv'])
            c['firstInv'] = min(c['firstInv'], r['date'])
            c['lastInv'] = max(c['lastInv'], r['date'])
        rows = []
        for c in custs.values():
            c['invoiceCount'] = len(c.pop('_i'))
            c['value'] = round(c['value'], 2)
            rows.append(c)
        rows.sort(key=lambda c: -c['value'])
        dates = sorted(r['date'] for r in inv)
        src = {'rows': len(inv), 'from': dates[0], 'to': dates[-1], 'ingestedAt': '2026-08-27T19:00:00'}
        out = {'ready': True, 'source': src,
               'totals': {'units': sum(r['qty'] for r in inv), 'value': round(sum(r['value'] for r in inv), 2),
                          'invoices': len({r['inv'] for r in inv}), 'customers': len(rows)},
               'years': [], 'byMonth': [], 'customers': rows}
        if self.mode == 'new':
            ob = sorted(r['date'] for r in inv if r['div'] == 'OB')
            ds = sorted(r['date'] for r in inv if r['div'] == 'DS')
            src['byDiv'] = {'OB': {'from': ob[0], 'to': ob[-1], 'rows': len(ob)},
                            'DS': {'from': ds[0], 'to': ds[-1], 'rows': len(ds)}}
            rows_p = self._pending_rows()
            out['pending'] = {'ready': True,
                              'totals': {'pos': len(rows_p)},
                              'byMonth': [], 'customers': {}}
            out.update(self._history_extra())
        return out

    def po_lookup(self, body):
        inv = self.data['invoices']
        want = [str(p).strip() for p in (body.get('pos') or []) if str(p).strip()][:1500]
        if self.mode == 'old':
            ws = set(want)
            sel = [r for r in inv if r['po'] in ws]
            found = {p['po']: {'qty': p['qty'], 'value': p['value'], 'firstInv': p['firstInv'],
                               'lastInv': p['lastInv'], 'invoices': p['invoices'], 'lines': p.get('lines') or []}
                     for p in _rollups(sel, with_lines=True)}
            return {'ready': True, 'found': found, 'requested': len(want), 'matched': len(found)}
        # new: key-aware, scoped by customer (account, or each item's customer)
        account = (body.get('account') or '').strip().upper()
        asks = [(str(it.get('customer') or account).upper(), str(it.get('po'))) for it in (body.get('items') or [])]
        asks += [(account, s) for s in want if account]
        found = {}
        for cust, raw in asks:
            jk = _join(cust, raw)
            if jk is None:
                continue
            sel = [r for r in inv if _join(r['cust'], r['po']) == jk]
            if not sel:
                continue
            ro = _rollups(sel, with_lines=True)
            agg = {'qty': sum(p['qty'] for p in ro), 'value': round(sum(p['value'] for p in ro), 2),
                   'firstInv': min(p['firstInv'] for p in ro), 'lastInv': max(p['lastInv'] for p in ro),
                   'invoices': sum(p['invoices'] for p in ro),
                   'lines': [l for p in ro for l in p['lines']],
                   'customer': cust, 'poKey': jk.split('|', 1)[1],
                   'matchedBy': 'exact' if any(r['po'] == raw for r in sel) else 'key',
                   'pos': sorted({r['po'] for r in sel})}
            found[raw] = agg
        # A bare number that another customer owns: the new service reports the owner.
        for s in want:
            if s not in found and not account:
                sel = [r for r in inv if r['po'] == s]
                if sel:
                    ro = _rollups(sel, with_lines=True)[0]
                    found[s] = dict(ro, customer=sel[0]['cust'], poKey=s, matchedBy='exact', pos=[s])
        return {'ready': True, 'found': found, 'ambiguous': {}, 'requested': len(want), 'matched': len(found)}


def _key(raw):
    return load_app()._po_key(raw)


def _join(cust, raw):
    return load_app()._po_join_key(cust, raw)


class FakeS3:
    def __init__(self):
        self.puts = []

    def put_object(self, **kw):
        self.puts.append(kw)
        return {}


class AppHarness:
    """Swap the app's outside world for fakes, and put it back afterwards."""

    def __init__(self, fake, open_orders=None):
        self.app = load_app()
        self.fake = fake
        self.s3 = FakeS3()
        self.open_orders = open_orders if open_orders is not None else fake.data.get('open_orders', [])
        self._saved = {}

    def __enter__(self):
        a = self.app
        for name, value in (('http_requests', self.fake),
                            ('get_s3', lambda: self.s3),
                            ('download_images_for_items', lambda items, url, use_cache=True: {}),
                            ('_fetch_all_open_orders', lambda: (list(self.open_orders), True)),
                            ('_pres_now_et', lambda: datetime(2026, 9, 15, 10, 0, 0))):
            self._saved[name] = getattr(a, name)
            setattr(a, name, value)
        reset_caches(a)
        return self

    def __exit__(self, *exc):
        for name, value in self._saved.items():
            setattr(self.app, name, value)
        reset_caches(self.app)
        return False


def reset_caches(a):
    with a._po_hist_proxy_lock:
        a._po_hist_proxy_cache.update({'summary': None, 'summary_at': 0.0, 'accounts': {},
                                       'accounts_at': {}, 'fail_until': {}, 'errors': {}})
    with a._sales_sum_lock:
        a._sales_sum_cache.update({'data': None, 'at': 0.0, 'fail_until': 0.0, 'error': None})
