"""One-time NJ exception (David, Sep 30 2026): TMVDSL032SSE's orders take its NJ units
first, the customer screens drop those NJ units together with the order they cover, and
the exception ends by itself after Dec 31 2026. Every other style keeps NJ as the last
resort. Quantities mirror the real case: a 6,048 u order starting Oct 7, a 6,048 u lot
landing NJ on Oct 22 and a 4,032 u lot landing TR on Feb 13."""
import os
import sys
import unittest
from datetime import date, datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from _support import load_app  # noqa: E402
import pnl_routing as R  # noqa: E402

SKU = 'TMVDSL032SSE'
OTHER = 'ZZVDSL032SSE'


def q(nj=0, committed=-6048):
    return {'jtw': 0, 'tr': 0, 'dcw': 0, 'qa': 0, 'nj': nj, 'abfi': 0,
            'committed': committed, 'allocated': 0, 'incoming': 10080}


def lots():
    return [{'units': 6048, 'arr': date(2026, 10, 22), 'etd': date(2026, 9, 15), 'fob_flag': False,
             'hidden': True, 'sup': False},
            {'units': 4032, 'arr': date(2027, 2, 13), 'etd': date(2026, 12, 30), 'fob_flag': False,
             'hidden': False, 'sup': False}]


def orders(sku):
    return [{'orderNo': '1', 'ctrlNo': '1', 'customer': 'ZCUST', 'customerFull': 'Z Customer',
             'style': sku, 'openQty': 6048, 'pickQty': 0,
             'startDate': '2026-10-07T00:00:00', 'cancelDate': '2026-10-13T00:00:00'}]


class OneTimeNjTests(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.app = load_app()

    def setUp(self):
        self._now = self.app._pres_now_et
        self._ledger = self.app._ledger_rows
        self.at(2026, 9, 30)
        self.app._ledger_rows = lambda: [
            {'style': SKU, 'warehouse': 'NJ', 'units': 6048}, {'style': SKU, 'warehouse': 'TR', 'units': 4032},
            {'style': OTHER, 'warehouse': 'NJ', 'units': 6048}, {'style': OTHER, 'warehouse': 'TR', 'units': 4032}]

    def tearDown(self):
        self.app._pres_now_et = self._now
        self.app._ledger_rows = self._ledger

    def at(self, y, m, d):
        self.app._pres_now_et = lambda: datetime(y, m, d, 12, 0)

    def route(self, sku):
        return self.app._pres_route_sku(q(), lots(), orders(sku), [], [], date(2026, 9, 30), sku=sku)

    def test_this_style_takes_the_nj_lot_first(self):
        self.assertEqual(self.route(SKU), {'wh': 0, 'os': 6048, 'os_visible': 0})

    def test_every_other_style_keeps_nj_last(self):
        self.assertEqual(self.route(OTHER), {'wh': 0, 'os': 6048, 'os_visible': 4032})

    def test_it_ends_after_dec_31_2026(self):
        self.at(2026, 12, 31)
        self.assertEqual(self.route(SKU)['os_visible'], 0)
        self.at(2027, 1, 1)
        self.assertEqual(self.route(SKU)['os_visible'], 4032)

    def test_nj_stock_after_landing(self):
        res = self.app._pres_route_sku(q(nj=6048), lots()[1:], orders(SKU), [], [], date(2026, 10, 26), sku=SKU)
        self.assertEqual(res, {'wh': 6048, 'os': 0, 'os_visible': 0})

    def test_customer_feed_drops_the_nj_lot_with_the_order(self):
        rows = [{'sku': SKU, 'incoming': 10080, 'total_ats': 4032, 'total_warehouse': 0, 'committed': -6048},
                {'sku': OTHER, 'incoming': 10080, 'total_ats': 4032, 'total_warehouse': 0, 'committed': -6048}]
        out = {r['sku']: r for r in self.app._strip_hidden_landing_rows(rows)}
        self.assertEqual((out[SKU]['incoming'], out[SKU]['total_ats'], out[SKU]['committed']), (4032, 4032, 0))
        self.assertEqual((out[OTHER]['incoming'], out[OTHER]['total_ats'], out[OTHER]['committed']),
                         (4032, -2016, -6048))
        self.assertEqual(rows[0]['committed'], -6048)   # the caller's rows are not touched

    def test_customer_feed_drops_nj_stock_with_the_order(self):
        rows = [{'sku': SKU, 'nj': 6048, 'abfi': 0, 'incoming': 4032, 'total_ats': 4032,
                 'total_warehouse': 6048, 'committed': -6048}]
        out = self.app._strip_nj_rows(rows)[0]
        self.assertEqual((out['total_warehouse'], out['total_ats'], out['committed']), (0, 4032, 0))
        self.assertNotIn('nj', out)

    def test_pnl_routing_mirrors_it(self):
        items = [{'sku': s, 'jtw': 0, 'tr': 0, 'dcw': 0, 'qa': 0, 'nj': 0, 'abfi': 0, 'incoming': 10080,
                  'committed': -6048, 'allocated': 0} for s in (SKU, OTHER)]
        ledger = []
        for s in (SKU, OTHER):
            ledger += [{'production': 'ZZ26003', 'poName': 'P', 'style': s, 'units': 6048, 'etd': '2026-09-15',
                        'arrival': '2026-10-22', 'port_dated': True, 'fob_flag': False, 'warehouse': 'NJ',
                        'shipmentNo': ''},
                       {'production': 'ZZ26049', 'poName': 'P', 'style': s, 'units': 4032, 'etd': '2026-12-30',
                        'arrival': '2027-02-13', 'port_dated': True, 'fob_flag': False, 'warehouse': 'TR',
                        'shipmentNo': ''}]
        ords = [dict(orders(s)[0], ctrlNo=c) for c, s in (('1', SKU), ('2', OTHER))]

        def by_ref(res, key):
            out = {}
            for r in res['lineAlloc'][key]:
                out[r.get('ref') or r['kind']] = out.get(r.get('ref') or r['kind'], 0) + r['units']
            return out
        res = R.route_all(items, ledger, ords, [], [], [], {}, '2026-09-30', fob_codes=[], options={})
        self.assertEqual(by_ref(res, f'1|{SKU}'), {'ZZ26003': 6048})
        self.assertEqual(by_ref(res, f'2|{OTHER}'), {'ZZ26049': 4032, 'ZZ26003': 2016})
        res = R.route_all(items, ledger, ords, [], [], [], {}, '2027-01-01', fob_codes=[], options={})
        self.assertEqual(by_ref(res, f'1|{SKU}'), {'ZZ26049': 4032, 'ZZ26003': 2016})


    def test_landed_nj_stock_on_size_rows_still_covers_the_order(self):
        rows = [{'sku': SKU, 'nj': 0, 'abfi': 0, 'incoming': 4032, 'total_ats': -2016,
                 'total_warehouse': 0, 'committed': -6048},
                {'sku': SKU + '-S', 'nj': 3000, 'abfi': 0, 'incoming': 0, 'total_ats': 3000,
                 'total_warehouse': 3000, 'committed': 0},
                {'sku': SKU + '-M', 'nj': 3048, 'abfi': 0, 'incoming': 0, 'total_ats': 3048,
                 'total_warehouse': 3048, 'committed': 0}]
        out = {r['sku']: r for r in self.app._strip_nj_rows(rows)}
        self.assertEqual((out[SKU]['total_ats'], out[SKU]['committed']), (4032, 0))
        self.assertNotIn(SKU + '-S', out)   # NJ-only size rows never reach customers
        self.assertEqual(self.app._one_time_nj_sized_nj(SKU, {r['sku']: r for r in rows}), 6048)

    def test_pnl_after_landing_takes_the_nj_stock_on_size_rows(self):
        items = [{'sku': SKU, 'jtw': 0, 'tr': 0, 'dcw': 0, 'qa': 0, 'nj': 0, 'abfi': 0, 'incoming': 4032,
                  'committed': -6048, 'allocated': 0},
                 {'sku': SKU + '-S', 'jtw': 0, 'tr': 0, 'dcw': 0, 'qa': 0, 'nj': 3000, 'abfi': 0,
                  'incoming': 0, 'committed': 0, 'allocated': 0},
                 {'sku': SKU + '-M', 'jtw': 0, 'tr': 0, 'dcw': 0, 'qa': 0, 'nj': 3048, 'abfi': 0,
                  'incoming': 0, 'committed': 0, 'allocated': 0}]
        ledger = [{'production': 'ZZ26049', 'poName': 'P', 'style': SKU, 'units': 4032, 'etd': '2026-12-30',
                   'arrival': '2027-02-13', 'port_dated': True, 'fob_flag': False, 'warehouse': 'TR',
                   'shipmentNo': ''}]
        res = R.route_all(items, ledger, orders(SKU), [], [], [], {}, '2026-10-26', fob_codes=[], options={})
        got = res['lineAlloc'][f'1|{SKU}']
        self.assertEqual(sum(r['units'] for r in got if r.get('landing') == 'NJ'), 6048)
        self.assertEqual(sum(r['units'] for r in got if r.get('ref') == 'ZZ26049'), 0)

    def test_pnl_one_time_take_from_a_late_lot_stays_forced(self):
        items = [{'sku': SKU, 'jtw': 0, 'tr': 0, 'dcw': 0, 'qa': 0, 'nj': 0, 'abfi': 0, 'incoming': 10080,
                  'committed': -6048, 'allocated': 0}]
        ledger = [{'production': 'ZZ26003', 'poName': 'P', 'style': SKU, 'units': 6048, 'etd': '2026-09-15',
                   'arrival': '2026-10-22', 'port_dated': True, 'fob_flag': False, 'warehouse': 'NJ',
                   'shipmentNo': ''}]
        res = R.route_all(items, ledger, orders(SKU), [], [], [], {}, '2026-09-30', fob_codes=[], options={})
        got = res['lineAlloc'][f'1|{SKU}']
        self.assertEqual([(r.get('ref'), r['units'], r['forced']) for r in got], [('ZZ26003', 6048, True)])


if __name__ == '__main__':
    unittest.main()
