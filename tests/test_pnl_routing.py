"""Synthetic unit tests for pnl_routing (the P&L port of the desktop smart-routing engine).

No real data: every SKU, ref, customer and quantity here is invented. Routing carries no
cost values at all. Run: python -m unittest discover -s tests -p "test_pnl_routing.py"
"""
import os
import random
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import pnl_routing as R  # noqa: E402

TODAY = '2026-03-02'
FOB = ['FOBX']


def item(sku, wh=0, nj=0, abfi=0, orders=0, apo=0, **kw):
    """ATS row whose deduction equals `orders` + `apo` (so the gate passes by default)."""
    row = {'sku': sku, 'jtw': 0, 'tr': wh, 'dcw': 0, 'qa': 0, 'nj': nj, 'abfi': abfi,
           'incoming': 0, 'committed': -orders, 'allocated': -apo}
    row.update(kw)
    return row


def lot(sku, units, arrival=None, etd=None, ref='ZZ26001', po='SYNTH PO', landing='TR', fob_flag=False):
    return {'production': ref, 'poName': po, 'style': sku, 'units': units, 'etd': etd,
            'arrival': arrival, 'port_dated': bool(arrival), 'fob_flag': fob_flag,
            'warehouse': landing, 'shipmentNo': ''}


def order(ctrl, sku, qty, start, cancel=None, cust='CUSTA', pick=0, order_no=None):
    return {'ctrlNo': ctrl, 'orderNo': order_no or f'PO{ctrl}', 'style': sku, 'customer': cust,
            'customerFull': f'Customer {cust}', 'openQty': qty, 'pickQty': pick,
            'startDate': f'{start}T00:00:00' if start else None,
            'cancelDate': f'{cancel or start}T00:00:00' if (cancel or start) else None}


def run(items, ledger, orders, apo=(), vw=(), manual=(), ri=None, fob=FOB, **opts):
    return R.route_all(items, ledger, orders, list(apo), list(vw), list(manual), ri or {}, TODAY,
                       fob_codes=None if fob is None else list(fob), options=opts)


def rows(res, ctrl, sku):
    return res['lineAlloc'][f'{ctrl}|{sku}']


def kinds(rs):
    out = {}
    for r in rs:
        k = r['kind'] if r['kind'] != 'production' else 'prod:' + r['ref']
        out[k] = out.get(k, 0) + r['units']
    return out


class EngineRules(unittest.TestCase):
    def test_warehouse_first_when_no_batch_is_ready(self):
        res = run([item('AAA', wh=500, orders=300)], [lot('AAA', 400, arrival='2026-05-01')],
                  [order('1', 'AAA', 300, '2026-03-10')])
        self.assertEqual(kinds(rows(res, '1', 'AAA')), {'warehouse': 300})
        self.assertEqual({r['routing'] for r in rows(res, '1', 'AAA')}, {'R1'})
        self.assertEqual(res['skus']['AAA']['status'], 'routed')

    def test_dated_batch_feasible_on_the_start_date(self):
        items = [item('B1', wh=500, orders=200), item('B2', wh=500, orders=200)]
        ledger = [lot('B1', 400, arrival='2026-04-01', ref='ZZ26011'),
                  lot('B2', 400, arrival='2026-04-01', ref='ZZ26012')]
        res = run(items, ledger, [order('1', 'B1', 200, '2026-04-01'), order('2', 'B2', 200, '2026-03-31')])
        self.assertEqual(kinds(rows(res, '1', 'B1')), {'prod:ZZ26011': 200})   # arrives by the start date
        self.assertEqual(kinds(rows(res, '2', 'B2')), {'warehouse': 200})      # one day late: warehouse

    def test_latest_feasible_batch_first(self):
        ledger = [lot('C', 100, arrival='2026-04-01', ref='ZZ26021'),
                  lot('C', 100, arrival='2026-05-01', ref='ZZ26022')]
        res = run([item('C', wh=100, orders=150)], ledger, [order('1', 'C', 150, '2026-05-05')])
        self.assertEqual(kinds(rows(res, '1', 'C')), {'prod:ZZ26022': 100, 'prod:ZZ26021': 50})

    def test_forced_pull_when_nothing_feasible_is_left(self):
        res = run([item('D', wh=100, orders=300)], [lot('D', 500, arrival='2026-05-01')],
                  [order('1', 'D', 300, '2026-03-10')])
        rs = rows(res, '1', 'D')
        self.assertEqual(kinds(rs), {'warehouse': 100, 'prod:ZZ26001': 200})
        forced = [r for r in rs if r['forced']]
        self.assertEqual(sum(r['units'] for r in forced), 200)
        self.assertEqual({r['routing'] for r in forced}, {'R2'})
        self.assertEqual(res['lineSummary']['1|D']['routing'], 'R2')

    def test_landed_batch_is_ready_now_and_stale_batch_is_last(self):
        ledger = [lot('E', 100, arrival='2026-02-20', ref='ZZ26031'),     # landed (10 days ago)
                  lot('E', 100, arrival='2025-12-01', ref='ZZ26032')]     # stale (over 30 days)
        res = run([item('E', orders=150)], ledger, [order('1', 'E', 150, '2026-03-05')])
        rs = rows(res, '1', 'E')
        self.assertEqual(kinds(rs), {'prod:ZZ26031': 100, 'prod:ZZ26032': 50})
        by = {r['ref']: r for r in rs}
        self.assertEqual((by['ZZ26031']['slotCls'], by['ZZ26031']['routing']), ('landed', 'R1'))
        self.assertEqual((by['ZZ26032']['slotCls'], by['ZZ26032']['routing']), ('stale', 'R3'))

    def test_fob_account_never_takes_us_warehouse_first(self):
        ledger = [lot('F', 300, etd='2026-04-10', arrival='2026-05-25', ref='ZZ26041')]
        res = run([item('F', wh=1000, orders=200)], ledger, [order('1', 'F', 200, '2026-04-15', cust='FOBX')])
        self.assertEqual(kinds(rows(res, '1', 'F')), {'prod:ZZ26041': 200})   # ETD test, not arrival
        # start before the ETD: rolls FORWARD to production (forced), still not the warehouse
        res = run([item('G', wh=1000, orders=200)], [lot('G', 300, etd='2026-04-10', arrival='2026-05-25')],
                  [order('1', 'G', 200, '2026-03-05', cust='FOBX')])
        rs = rows(res, '1', 'G')
        self.assertEqual(kinds(rs), {'prod:ZZ26001': 200})
        self.assertTrue(all(r['forced'] and not r['fobWhFallback'] for r in rs))
        # no production at all: the US warehouse is the flagged last resort (R4)
        res = run([item('H', wh=1000, orders=200)], [lot('OTHER', 1, arrival='2026-05-01')],
                  [order('1', 'H', 200, '2026-03-05', cust='FOBX')])
        rs = rows(res, '1', 'H')
        self.assertEqual(kinds(rs), {'warehouse': 200})
        self.assertEqual({(r['fobWhFallback'], r['routing']) for r in rs}, {(True, 'R4')})

    def test_fob_flagged_batch_drains_first_for_fob_accounts(self):
        ledger = [lot('I', 100, fob_flag=True, ref='ZZ26051'), lot('I', 500, etd='2026-03-20',
                                                                   arrival='2026-05-04', ref='ZZ26052')]
        res = run([item('I', orders=150)], ledger, [order('1', 'I', 150, '2026-04-01', cust='FOBX')])
        self.assertEqual(kinds(rows(res, '1', 'I')), {'prod:ZZ26051': 100, 'prod:ZZ26052': 50})

    def test_nj_is_last_resort(self):
        res = run([item('J', wh=100, nj=500, orders=300)], [lot('J', 500, arrival='2026-05-01')],
                  [order('1', 'J', 300, '2026-03-10')])
        self.assertEqual(kinds(rows(res, '1', 'J')), {'warehouse': 100, 'prod:ZZ26001': 200})
        res = run([item('K', wh=100, nj=500, orders=300)], [lot('OTHER', 1, arrival='2026-05-01')],
                  [order('1', 'K', 300, '2026-03-10')])
        rs = rows(res, '1', 'K')
        nj = [r for r in rs if r['landing'] == 'NJ']
        self.assertEqual(sum(r['units'] for r in nj), 200)
        self.assertTrue(all(r['forced'] and r['njPull'] and r['slotCls'] == 'nj' for r in nj))
        # NJ is the only supply: taken normally, not forced
        res = run([item('L', nj=500, orders=300)], [lot('OTHER', 1, arrival='2026-05-01')],
                  [order('1', 'L', 300, '2026-03-10')])
        rs = rows(res, '1', 'L')
        self.assertEqual([(r['landing'], r['forced'], r['routing']) for r in rs], [('NJ', False, 'R1')])

    def test_dateless_batches_are_consumed_last(self):
        ledger = [lot('M', 100, ref='ZZ26061'),                              # no date, not FOB-flagged
                  lot('M', 100, arrival='2026-05-01', ref='ZZ26062')]
        res = run([item('M', orders=150, apo=40)], ledger, [order('1', 'M', 150, '2026-03-10')],
                  apo=[{'style': 'M', 'qty': 40, 'customer': 'BOOKING', 'po': 'X'}])
        rs = rows(res, '1', 'M')
        self.assertEqual(kinds(rs), {'prod:ZZ26062': 100, 'prod:ZZ26061': 50})
        self.assertEqual({r['ref']: r['routing'] for r in rs}, {'ZZ26062': 'R2', 'ZZ26061': 'R3'})
        self.assertEqual(kinds(res['apoAlloc'][0]), {'prod:ZZ26061': 40})

    def test_named_pick_customer_goes_fifo(self):
        o = order('1', 'N', 100, '2026-05-10')
        o['customerFull'] = 'Pick Ticket Account'
        res = run([item('N', wh=500, orders=100)], [lot('N', 500, arrival='2026-05-01')], [o])
        self.assertEqual(kinds(rows(res, '1', 'N')), {'warehouse': 100})
        self.assertEqual(res['skus']['N']['claims'][0]['tier'], 'prepick')


class Fallbacks(unittest.TestCase):
    def test_gate_failure_uses_fifo_fallback(self):
        items = [item('P', wh=500, orders=5000)]          # deduction far above visible demand
        ledger = [lot('P', 500, arrival='2026-04-01')]
        res = run(items, ledger, [order('1', 'P', 300, '2026-05-01')])
        self.assertEqual(res['skus']['P']['status'], 'gate_fail')
        self.assertEqual(res['skus']['P']['mode'], 'fifo')
        rs = rows(res, '1', 'P')
        self.assertEqual(kinds(rs), {'warehouse': 300})    # the engine would have used the batch
        self.assertEqual({r['routing'] for r in rs}, {'R5'})
        self.assertFalse(res['skus']['P']['gate']['pass'])
        self.assertIsNone(R.engine_totals(res['skus']['P']))
        eng = run(items, ledger, [order('1', 'P', 300, '2026-05-01')], gateFallback='engine')
        self.assertEqual(eng['skus']['P']['mode'], 'engine_ungated')
        self.assertEqual(kinds(rows(eng, '1', 'P')), {'prod:ZZ26001': 300})
        self.assertEqual({r['routing'] for r in rows(eng, '1', 'P')}, {'R5'})

    def test_gate_fallback_keeps_fob_off_the_warehouse(self):
        res = run([item('Q', wh=500, orders=5000)], [lot('Q', 100, arrival='2026-05-01')],
                  [order('1', 'Q', 300, '2026-04-01', cust='FOBX')])
        rs = rows(res, '1', 'Q')
        self.assertEqual(kinds(rs), {'prod:ZZ26001': 100, 'warehouse': 200})
        wh = [r for r in rs if r['kind'] == 'warehouse']
        self.assertTrue(all(r['fobWhFallback'] and r['forced'] and r['routing'] == 'R5' for r in wh))

    def test_no_deduction_with_orders_uses_fallback(self):
        res = run([item('S', wh=50)], [lot('S', 100, arrival='2026-04-01')], [order('1', 'S', 80, '2026-05-01')])
        self.assertEqual((res['skus']['S']['status'], res['skus']['S']['mode']), ('no_deduction', 'fifo'))
        self.assertEqual(kinds(rows(res, '1', 'S')), {'warehouse': 50, 'prod:ZZ26001': 30})

    def test_deduction_assignments(self):
        items = [item('T', wh=500, orders=300)]
        ledger = [lot('T', 200, arrival='2026-05-01')]
        orders = [order('1', 'T', 300, '2026-03-10')]
        res = run(items, ledger, orders, ri={'deduction_assignments': {'T': 'overseas'}})
        sk = res['skus']['T']
        self.assertEqual((sk['mode'], sk['assignment']), ('assignment', 'overseas'))
        rs = rows(res, '1', 'T')
        self.assertEqual(kinds(rs), {'prod:ZZ26001': 200, 'warehouse': 100})   # overflow spills
        self.assertTrue(all(r['routing'] == 'R5' for r in rs))
        self.assertTrue(all(r['forced'] for r in rs if r['kind'] == 'warehouse'))
        res = run(items, ledger, orders, ri={'deduction_assignments': None})       # unavailable
        self.assertEqual(res['skus']['T']['mode'], 'engine')
        self.assertFalse(res['stats']['assignmentsHonored'])
        res = run(items, ledger, orders, ri={'deduction_assignments': {'T': 'overseas'}}, honorAssignments=False)
        self.assertEqual(res['skus']['T']['mode'], 'engine')
        res = run(items, ledger, orders, ri={'deduction_assignments': {'T': 'fifo'}})   # not an override
        self.assertEqual(res['skus']['T']['mode'], 'engine')


class ClaimsAndOptions(unittest.TestCase):
    def test_apo_and_vw_claims(self):
        items = [item('U', wh=100, orders=50, apo=200)]
        ledger = [lot('U', 400, arrival='2026-05-01')]
        vw = [{'sku': 'U', 'qty': 100, 'customer': 'VW BOOK', 'po': 'V1'}]
        apo = [{'style': 'U', 'qty': 200, 'customer': 'APO BOOK', 'po': 'A1'}]
        res = run(items, ledger, [order('1', 'U', 50, '2026-03-10')], apo=apo, vw=vw)
        self.assertEqual(res['skus']['U']['status'], 'routed')       # VW qty is in the deduction
        self.assertEqual(res['skus']['U']['q']['virtual'], 100)
        self.assertEqual(kinds(rows(res, '1', 'U')), {'warehouse': 50})
        self.assertEqual(kinds(res['apoAlloc'][0]), {'warehouse': 50, 'prod:ZZ26001': 150})
        self.assertEqual(kinds(res['vwAlloc'][0]), {'prod:ZZ26001': 100})
        srcs = [c['source'] for c in res['skus']['U']['claims']]
        self.assertEqual(srcs, ['order', 'apo', 'vw'])

    def test_manual_allocation_is_a_deduction_not_a_claim(self):
        manual = [{'sku': 'V', 'qty': 1000}]
        res = run([item('V', wh=500, orders=100)], [lot('V', 100, arrival='2026-05-01')],
                  [order('1', 'V', 100, '2026-03-10')], manual=manual)
        sk = res['skus']['V']
        self.assertEqual(sk['gate']['deduction'], 1100)
        self.assertEqual(sk['status'], 'gate_fail')
        self.assertEqual(len(sk['claims']), 1)

    def test_picks_as_warehouse_option(self):
        items = [item('W', wh=500, orders=300)]
        ledger = [lot('W', 500, arrival='2026-05-01')]
        orders = [order('1', 'W', 0, '2026-05-10', pick=100),
                  order('2', 'W', 0, '2026-05-10', pick=200, cust='FOBX')]
        base = run(items, ledger, orders)
        self.assertEqual(kinds(rows(base, '1', 'W')), {'prod:ZZ26001': 100})     # strict engine parity
        opt = run(items, ledger, orders, picksAsWarehouse=True)
        rs = rows(opt, '1', 'W')
        self.assertEqual(kinds(rs), {'warehouse': 100})
        self.assertEqual({r['part'] for r in rs}, {'pick'})
        self.assertEqual(kinds(rows(opt, '2', 'W')), kinds(rows(base, '2', 'W')))   # FOB picks unchanged
        self.assertEqual(base['stats']['lines']['pick']['byKindNonFob'], {'production_future': 100})
        self.assertEqual(opt['stats']['lines']['pick']['byKindNonFob'], {'warehouse': 100})

    def test_unsourced_remainder_and_no_supply(self):
        res = run([item('X', wh=100, orders=300), item('Y', orders=50)], [lot('OTHER', 1, arrival='2026-05-01')],
                  [order('1', 'X', 300, '2026-03-10'), order('2', 'Y', 50, '2026-03-10'),
                   order('3', 'NOTINFEED', 70, '2026-03-10'), order('4', '', 5, '2026-03-10')])
        rs = rows(res, '1', 'X')
        self.assertEqual(kinds(rs), {'warehouse': 100, 'unsourced': 200})
        uns = [r for r in rs if r['kind'] == 'unsourced'][0]
        self.assertEqual((uns['routing'], uns['reason']), ('R6', 'short'))
        self.assertEqual([(r['reason'], r['units']) for r in rows(res, '2', 'Y')], [('no_slots', 50)])
        self.assertEqual([(r['reason'], r['units']) for r in rows(res, '3', 'NOTINFEED')], [('not_in_feed', 70)])
        self.assertEqual([r['reason'] for r in res['lineAlloc']['4|']], ['no_style'])
        self.assertEqual(res['lineSummary']['1|X']['routing'], 'R6')
        self.assertEqual(res['lineSummary']['1|X']['mix'], {'R1': 100, 'R6': 200})

    def test_line_key_is_ctrl_and_style(self):
        orders = [order('9', 'Z1', 10, '2026-03-10'), order('9', 'Z2', 20, '2026-03-10'),
                  order('9', 'Z2', 5, '2026-03-10')]
        self.assertEqual(R.line_keys(orders), ['9|Z1', '9|Z2', '9|Z2#2'])
        res = run([item('Z1', wh=10, orders=10), item('Z2', wh=25, orders=25)], [lot('OTHER', 1)], orders)
        self.assertEqual(sum(r['units'] for r in res['lineAlloc']['9|Z2#2']), 5)

    def test_default_fob_list_is_the_engines(self):
        res = run([item('HF', wh=500, orders=100)], [lot('HF', 200, etd='2026-03-20', arrival='2026-05-04')],
                  [order('1', 'HF', 100, '2026-04-01', cust='HALF')], fob=None)
        self.assertTrue(res['stats']['fobCodesMatchEngine'])
        self.assertEqual(kinds(rows(res, '1', 'HF')), {'prod:ZZ26001': 100})
        res = run([item('HF', wh=500, orders=100)], [lot('HF', 200)], [order('1', 'HF', 100, '2026-04-01')])
        self.assertFalse(res['stats']['fobCodesMatchEngine'])


class InputsAndSafety(unittest.TestCase):
    def test_duplicate_feed_rows_merge(self):
        items = [item('DUP', wh=300, orders=0), item('DUP', wh=200, orders=400)]
        res = run(items, [lot('OTHER', 1)], [order('1', 'DUP', 400, '2026-03-10')])
        sk = res['skus']['DUP']
        self.assertEqual((sk['q']['tr'], sk['q']['committed']), (500, -400))
        self.assertEqual(res['stats']['inputs']['duplicateRowsMerged'], 1)
        self.assertEqual(kinds(rows(res, '1', 'DUP')), {'warehouse': 400})

    def test_arrival_suppression_and_override(self):
        items = [item('SUP', wh=1000, orders=100)]
        ledger = [lot('SUP', 1000, arrival='2026-03-10', ref='ZZ26071')]
        res = run(items, ledger, [order('1', 'SUP', 100, '2026-04-01')])
        self.assertEqual([s['type'] for s in res['skus']['SUP']['slots']], ['warehouse'])
        self.assertTrue(res['ledgerUse'][0]['suppressed'])
        res = run(items, ledger, [order('1', 'SUP', 100, '2026-04-01')], ri={'suppression_overrides': ['sup']})
        self.assertEqual([s['type'] for s in res['skus']['SUP']['slots']], ['warehouse', 'production'])
        self.assertFalse(res['ledgerUse'][0]['suppressed'])

    def test_suppression_window_uses_now(self):
        items = [item('WIN', wh=1000, orders=100)]
        ledger = [lot('WIN', 1000, arrival='2026-02-16')]   # exactly 14 days before TODAY
        mid = run(items, ledger, [order('1', 'WIN', 100, '2026-04-01')])
        late = run(items, ledger, [order('1', 'WIN', 100, '2026-04-01')], ri={'now': '2026-03-02T17:09:00'})
        self.assertTrue(mid['ledgerUse'][0]['suppressed'])
        self.assertFalse(late['ledgerUse'][0]['suppressed'])
        self.assertEqual((mid['stats']['clock']['source'], late['stats']['clock']['source']),
                         ('midnight', 'routing_inputs.now'))

    def test_empty_inputs_fail_closed(self):
        with self.assertRaises(R.RoutingInputError) as cm:
            run([item('A', wh=1)], [], [])
        self.assertEqual(cm.exception.code, 'LEDGER_UNAVAILABLE')
        with self.assertRaises(R.RoutingInputError) as cm:
            run([], [lot('A', 1)], [])
        self.assertEqual(cm.exception.code, 'INVENTORY_UNAVAILABLE')
        res = run([item('A', wh=10, orders=10)], [], [order('1', 'A', 10, '2026-03-10')], allowEmptyLedger=True)
        self.assertEqual(kinds(rows(res, '1', 'A')), {'warehouse': 10})
        with self.assertRaises(R.RoutingInputError):
            R.route_all([item('A')], [lot('A', 1)], [], [], [], [], {}, 'not a date')

    def test_deterministic(self):
        args = ([item('A', wh=100, orders=150)], [lot('A', 100, arrival='2026-04-01')],
                [order('1', 'A', 150, '2026-04-02')])
        self.assertEqual(run(*args), run(*args))


class Conservation(unittest.TestCase):
    """Randomized synthetic book: every order line, APO row and VW row is fully attributed."""

    def _scenario(self, seed):
        rnd = random.Random(seed)
        items, ledger, orders, apo, vw, manual, asg = [], [], [], [], [], [], {}
        dates = [None, '2025-11-01', '2026-02-15', '2026-03-02', '2026-03-20', '2026-04-15', '2026-06-01']
        ctrl = 0
        for n in range(40):
            sku = f'SYN{n:03d}'
            demand = 0
            k_orders = rnd.randint(0, 5)
            for _ in range(k_orders):
                ctrl += 1
                qty = rnd.choice([0, 12, 36, 100, 240, 500])
                pick = rnd.choice([0, 0, 24, 60])
                o = order(str(ctrl // 2), sku, qty, rnd.choice(dates[1:]), cust=rnd.choice(['CUSTA', 'CUSTB', 'FOBX']),
                          pick=pick)
                if rnd.random() < 0.2:
                    o['startDate'] = None
                orders.append(o)
                demand += qty + pick
            for _ in range(rnd.randint(0, 2)):
                q = rnd.choice([50, 150, 400])
                apo.append({'style': sku, 'qty': q, 'customer': rnd.choice(['BOOK1', 'BOOK2'])})
                demand += q
            if rnd.random() < 0.15:
                q = rnd.choice([20, 80])
                vw.append({'sku': sku, 'qty': q, 'customer': 'VWB'})
            if rnd.random() < 0.1:
                manual.append({'sku': sku, 'qty': rnd.choice([10, 300])})
            ded = rnd.choice([demand, demand, demand + rnd.randint(-50, 50), 0, demand * 3 + 500])
            items.append(item(sku, wh=rnd.choice([0, 0, 100, 700]), nj=rnd.choice([0, 0, 60]),
                              abfi=rnd.choice([0, 0, 40]), orders=max(0, ded)))
            for _ in range(rnd.randint(0, 3)):
                d = rnd.choice(dates)
                ledger.append(lot(sku, rnd.choice([0, 60, 300, 900]), arrival=d,
                                  etd=None if d is None else '2026-01-10', ref=f'ZZ26{rnd.randint(100, 999)}',
                                  landing=rnd.choice(['TR', 'JTW', 'NJ', 'FOB', 'AE']),
                                  fob_flag=(d is None and rnd.random() < 0.5)))
            if rnd.random() < 0.1:
                asg[sku] = rnd.choice(['warehouse', 'overseas'])
        orders.append(order('99999', 'MISSINGSKU', 77, '2026-03-10'))
        ledger.append(lot('LEDGERONLY', 10, arrival='2026-05-01'))
        return items, ledger, orders, apo, vw, manual, asg

    def test_units_are_conserved(self):
        for seed in range(12):
            items, ledger, orders, apo, vw, manual, asg = self._scenario(seed)
            for opts in ({}, {'picksAsWarehouse': True}, {'gateFallback': 'engine'}, {'honorAssignments': False}):
                res = R.route_all(items, ledger, orders, apo, vw, manual, {'deduction_assignments': asg},
                                  TODAY, FOB, opts)
                keys = R.line_keys(orders)
                for o, k in zip(orders, keys):
                    qty = R.to_int(o['openQty']) + R.to_int(o['pickQty'])
                    got = sum(r['units'] for r in res['lineAlloc'][k])
                    self.assertEqual(got, max(qty, 0), (seed, opts, k))
                for i, a in enumerate(apo):
                    self.assertEqual(sum(r['units'] for r in res['apoAlloc'][i]), a['qty'], (seed, opts, i))
                for i, v in enumerate(vw):
                    self.assertEqual(sum(r['units'] for r in res['vwAlloc'][i]), v['qty'], (seed, opts, i))
                for sku, sk in res['skus'].items():
                    for s in sk['slots']:
                        used = sum(c['units'] for c in s['consumers'])
                        self.assertEqual(used, s['orig'] - s['left'], (seed, sku))
                        self.assertGreaterEqual(s['left'], 0)
                        if s['type'] == 'production':
                            lu = res['ledgerUse'][s['ledgerIndex']]
                            self.assertEqual((lu['claimed'], lu['left']), (used, s['left']))
                    tot = R.engine_totals(sk)
                    if sk['mode'] == 'engine':
                        wh = sum(sum(c['units'] for c in s['consumers']) for s in sk['slots'] if s['type'] == 'warehouse')
                        self.assertEqual(tot['wh'], wh)
                    else:
                        self.assertIsNone(tot)
                self.assertEqual(res['ledgerUse'][len(ledger) - 1]['status'], 'not_in_feed')
                ls = res['stats']['lines']
                self.assertEqual(ls['placed'] + ls['unsourced'], ls['units'])


if __name__ == '__main__':
    unittest.main()
