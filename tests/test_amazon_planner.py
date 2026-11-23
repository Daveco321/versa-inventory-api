"""amazon_planner.py: what the Amazon agent proposes to send from Transcal (whole 36-pc cartons,
prepacks first, then single sizes; never a store account's stock). Synthetic styles only."""
import os
import sys
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

import amazon_planner as P  # noqa: E402

BASE = 'AMZZSU901SLS'
SLIM = {'S': 4, 'M': 8, 'MT': 4, 'L': 4, 'LT': 8, 'XL': 8}
SOLD = {'S': 20, 'M': 100, 'MT': 40, 'L': 60, 'LT': 60, 'XL': 40}          # 320 = HOT


def listings(base=BASE, sold=SOLD, available=10, extra=()):
    out = [P.Listing(f'{base}-{s}-FBA', f'B0ZZ{base[-6:-3]}{s}', 'B0ZZPARENT', available, 0, q) for s, q in sold.items()]
    return out + list(extra)


def plan(lst, stock, ratio=SLIM, **settings):
    return P.plan_shipment(lst, stock, lambda base: ratio, settings or None)


class PlanTests(unittest.TestCase):

    def test_prepacks_first_then_single_sizes(self):
        stock = [P.StockRow(BASE, 360, 0), P.StockRow(f'{BASE}-1515.53233', 144, 0)]
        res = plan(listings(), stock)
        self.assertEqual(1, len(res['styles']))
        st = res['styles'][0]
        self.assertEqual(('HOT', 1512, 320, 60), (st['tier'], st['target'], st['velocity'], st['at_amazon']))
        self.assertEqual(10, st['prepack_cartons'])           # the neediest size wants more; the pool has 10
        self.assertEqual(4, st['single_cartons'])             # M is still short: all 4 single M cartons
        self.assertEqual(504, st['units'])
        self.assertTrue(all(c.units == 36 for c in res['cartons']))
        prepack = [c for c in res['cartons'] if c.is_prepack][0]
        self.assertEqual({f'{BASE}-S-FBA': 4, f'{BASE}-M-FBA': 8, f'{BASE}-MT-FBA': 4, f'{BASE}-L-FBA': 4,
                          f'{BASE}-LT-FBA': 8, f'{BASE}-XL-FBA': 8}, prepack.contents)
        single = [c for c in res['cartons'] if not c.is_prepack]
        self.assertEqual({f'{BASE}-1515.53233'}, {c.warehouse_sku for c in single})
        self.assertEqual({f'{BASE}-M-FBA': 36}, single[0].contents)
        m_line = [ln for ln in st['lines'] if ln['kind'] == 'single'][0]
        self.assertEqual((144, 4, 10, 100, 3), (m_line['units'], m_line['cartons'], m_line['at_amazon'],
                                                 m_line['sold30'], m_line['days_left']))
        self.assertIn('Sold 320 in the last 30 days', st['why'])

    def test_units_promised_to_other_customers_stay(self):
        stock = [P.StockRow(BASE, 360, 340)]                   # 20 free: not one whole carton
        res = plan(listings(), stock)
        self.assertEqual([], res['styles'])
        self.assertEqual([], res['cartons'])
        stock = [P.StockRow(BASE, 360, 300)]                   # 60 free: one prepack carton
        self.assertEqual(1, plan(listings(), stock)['totals']['cartons'])

    def test_store_account_stock_and_partial_cartons_never_ship(self):
        stock = [P.StockRow('ROZZSU901SLS', 3600, 0), P.StockRow(f'{BASE}-V', 30, 0), P.StockRow('AMZZSU901SLS-V', 72, 0)]
        self.assertEqual(0, plan(listings(), stock)['totals']['cartons'])

    def test_covered_slow_and_excluded_styles_are_skipped_with_a_reason(self):
        stock = [P.StockRow(BASE, 360, 0)]
        covered = plan(listings(available=400), stock)              # 2,400 at Amazon: 225 days
        self.assertEqual([], covered['styles'])
        self.assertIn('target', covered['skipped'][0]['reason'])
        slow = plan(listings(sold={'M': 5, 'L': 3}), stock)
        self.assertIn('sold only 8', slow['skipped'][0]['reason'])
        excluded = plan(listings(), stock, excluded_bases=(BASE,))
        self.assertIn('WH1/WH2/WH3', excluded['skipped'][0]['reason'])
        unlisted = plan([], stock)
        self.assertEqual('not listed on Amazon', unlisted['skipped'][0]['reason'])

    def test_lots_of_cover_is_skipped_even_below_target(self):
        sold = {'M': 10, 'L': 2}                               # SLOW (12): target 504, 6 a month at Amazon pace
        res = plan(listings(sold=sold, available=60), [P.StockRow(BASE, 360, 0)])   # 120 at Amazon = 300 days
        self.assertIn('days', res['skipped'][0]['reason'])

    def test_fbm_twin_sales_count_but_cartons_go_to_the_fba_listing(self):
        twin = P.Listing(f'{BASE}-M-FBM', 'B0ZZTWIN', 'B0ZZPARENT', 0, 0, 400)
        res = plan(listings(sold={'S': 1, 'M': 1, 'MT': 1, 'L': 1, 'LT': 1, 'XL': 1}, extra=[twin]),
                   [P.StockRow(f'{BASE}-1515.53233', 360, 0)])
        st = res['styles'][0]
        self.assertEqual(406, st['velocity'])
        self.assertTrue(all(set(c.contents) == {f'{BASE}-M-FBA'} for c in res['cartons']))

    def test_no_prepack_when_a_size_is_not_listed(self):
        sold = {'S': 20, 'M': 100, 'MT': 40, 'L': 60, 'LT': 60}            # no XL listing
        res = plan(listings(sold=sold), [P.StockRow(BASE, 360, 0)])
        self.assertEqual([], res['styles'])
        self.assertIn('no active Amazon listing for size XL', res['skipped'][0]['reason'])

    def test_inactive_listing_is_not_shipped_to(self):
        lst = listings()
        lst = [x._replace(active=False) if x.sku.endswith('-M-FBA') else x for x in lst]
        res = plan(lst, [P.StockRow(f'{BASE}-1515.53233', 144, 0)])
        self.assertEqual(0, res['totals']['cartons'])


class RuleTests(unittest.TestCase):

    def test_neck_sleeve_rules_by_fit(self):
        slim = {'sizes': [['14-14.5 / 32-33', 4], ['15-15.5 / 32-33', 8], ['15-15.5 / 34-35', 4],
                          ['16-16.5 / 32-33', 4], ['16-16.5 / 34-35', 8], ['17-17.5 / 34-35', 8]], 'master_qty': 36}
        self.assertEqual((SLIM, ''), P.ratio_from_rule(slim, 'SL'))
        regular = {'sizes': [['15-15.5 / 32-33', 4], ['15-15.5 / 34-35', 4], ['16-16.5 / 32-33', 8],
                             ['16-16.5 / 34-35', 4], ['17-17.5 / 32-33', 4], ['17-17.5 / 34-35', 8],
                             ['18-18.5 / 34-35', 4]], 'master_qty': 36}
        self.assertEqual({'M': 4, 'MT': 4, 'L': 8, 'LT': 4, 'XL': 4, 'XLT': 8, 'XXL': 4},
                         P.ratio_from_rule(regular, 'RF')[0])

    def test_inner_packs_scale_to_the_carton(self):
        ss = {'sizes': [['14-14.5 (S)', 1], ['15-15.5 (M)', 3], ['16-16.5 (L)', 3], ['17-17.5 (XL)', 2]],
              'master_qty': 36, 'inner_qty': '9pc Inner'}
        self.assertEqual({'S': 4, 'M': 12, 'L': 12, 'XL': 8}, P.ratio_from_rule(ss, 'SS')[0])
        vd = {'sizes': [['S (14-14.5)', 1], ['M (15-15.5)', 2], ['L (16-16.5)', 2], ['XL (17-17.5)', 1]], 'master_qty': 36}
        self.assertEqual({'S': 6, 'M': 12, 'L': 12, 'XL': 6}, P.ratio_from_rule(vd, 'SL')[0])
        words = {'sizes': [['Small', 1], ['Medium', 3], ['Large', 3], ['X-Large', 2]], 'master_qty': 36}
        self.assertEqual({'S': 4, 'M': 12, 'L': 12, 'XL': 8}, P.ratio_from_rule(words, 'SS')[0])

    def test_rules_that_cannot_make_an_amazon_carton(self):
        pants = {'sizes': [['29 x 29', 1], ['30 x 30', 2]], 'master_qty': 12}
        self.assertIsNone(P.ratio_from_rule(pants, 'PS')[0])
        odd = {'sizes': [['S', 5], ['M', 5]], 'master_qty': 36}           # 10 does not divide 36
        self.assertIsNone(P.ratio_from_rule(odd, 'SL')[0])
        self.assertIsNone(P.ratio_from_rule(None, 'SL')[0])


if __name__ == '__main__':
    unittest.main()
