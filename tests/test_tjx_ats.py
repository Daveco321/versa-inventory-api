"""Synthetic tests for tjx_ats (the TJMAXX ATS workbook rows, a server mirror of the TJX
catalog page). No real data: every SKU, production number, customer and quantity is
invented. Run: python -m unittest discover -s tests -p "test_tjx_ats.py"
"""
import os
import sys
import unittest
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import tjx_ats as T  # noqa: E402

NOW = datetime(2026, 3, 2, 9, 30)


class FakeDisplay:
    """Just enough of tjx_display.TjxDisplay for row building."""
    ORDER = ['AAA', 'BBB']

    def brand_key(self, sku, feed_brand):
        return feed_brand

    def brand_full(self, key):
        return {'AAA': 'Brand A', 'BBB': 'Brand B'}.get(key, key)

    def brand_sort_key(self, key):
        return (self.ORDER.index(key) if key in self.ORDER else 99, key)

    def color_display(self, sku, brand):
        return 'White Solid'

    def color_family(self, sku, brand):
        return 'White'

    def fit_label(self, sku):
        return 'Slim Fit'

    def fabric(self, sku):
        return ('XX', 'Synthetic Fabric')

    def new_fabric(self, sku, brand):
        return 'YES' if sku.endswith('N') else ''

    def export_category(self, sku, brand):
        return 'long_sleeve'

    def export_fit(self, sku):
        return 'SL'

    def export_customer(self, sku):
        return sku[:2]

    def size_pack_override(self, sku):
        return None


def inv(sku, brand='AAA', wh=0, incoming=0, committed=0, allocated=0):
    return {'sku': sku, 'brand': brand, 'jtw': 0, 'tr': wh, 'dcw': 0, 'qa': 0, 'incoming': incoming,
            'committed': -committed, 'allocated': -allocated}


def lot(sku, units, ref, arrival=None, etd=None, landing='TR'):
    return {'production': ref, 'poName': '', 'style': sku, 'units': units, 'etd': etd,
            'arrival': arrival, 'port_dated': bool(arrival), 'fob_flag': False, 'warehouse': landing}


def order(sku, qty, start, cancel, customer='ZZZ'):
    return {'style': sku, 'openQty': qty, 'pickQty': 0, 'customer': customer, 'customerFull': 'Synthetic Co',
            'startDate': start + 'T00:00:00', 'cancelDate': cancel + 'T00:00:00', 'orderNo': '1'}


def build(inventory, production=(), orders=(), apo=(), allocations=(), manual=(), assignments=None,
          suppression=()):
    return T.TjxAtsBuilder(list(inventory), list(production), list(apo), list(allocations), list(manual),
                           assignments or {}, list(suppression), list(orders), FakeDisplay(), NOW)


class JsRound(unittest.TestCase):
    def test_halves_round_up_like_math_round(self):
        self.assertEqual(T._js_round(2.5), 3)
        self.assertEqual(T._js_round(3.5), 4)
        self.assertEqual(T._js_round(0.49), 0)


class WarehouseTab(unittest.TestCase):
    def test_engine_keeps_a_later_order_off_the_warehouse(self):
        # 100 in stock, 500 arriving in April; the order starts in May, so it parks on the lot
        b = build([inv('TJAAXX001SLS', wh=100, incoming=500, committed=300)],
                  [lot('TJAAXX001SLS', 500, 'ZZ26001', arrival='2026-04-10')],
                  [order('TJAAXX001SLS', 300, '2026-05-01', '2026-05-20')])
        self.assertIn('TJAAXX001SLS', b.smart)
        row = b.ats_items()[0]
        self.assertEqual(row['total_ats'], 100)

    def test_gate_failure_falls_back_to_warehouse_first(self):
        # deduction 300 but no matching demand: no engine entry, warehouse absorbs first
        b = build([inv('TJAAXX002SLS', wh=100, incoming=500, committed=300)],
                  [lot('TJAAXX002SLS', 500, 'ZZ26002', arrival='2026-04-10')], orders=[])
        self.assertNotIn('TJAAXX002SLS', b.smart)
        self.assertEqual(b.ats_items()[0]['total_ats'], 0)

    def test_no_incoming_takes_the_full_deduction(self):
        b = build([inv('TJAAXX003SLS', wh=100, committed=150)])
        self.assertEqual(b.ats_items()[0]['total_ats'], -50)

    def test_assignment_overseas_spares_the_warehouse(self):
        b = build([inv('TJAAXX004SLS', wh=100, incoming=500, committed=300)],
                  [lot('TJAAXX004SLS', 500, 'ZZ26004', arrival='2026-04-10')],
                  assignments={'TJAAXX004SLS': 'overseas'})
        self.assertEqual(b.ats_items()[0]['total_ats'], 100)

    def test_manual_allocations_raise_the_deduction(self):
        b = build([inv('TJAAXX005SLS', wh=100)], manual=[{'sku': 'TJAAXX005SLS', 'qty': 40}])
        self.assertEqual(b.ats_items()[0]['total_ats'], 60)

    def test_manual_only_deduction_is_not_routed(self):
        # a deduction with no demand claim behind it (manual allocation): the page's engine
        # skips the SKU, so the warehouse takes the deduction the legacy way
        b = build([inv('TJAAXX007SLS', wh=100, incoming=500)],
                  [lot('TJAAXX007SLS', 500, 'ZZ26007', arrival='2026-04-10')],
                  manual=[{'sku': 'TJAAXX007SLS', 'qty': 80}])
        self.assertNotIn('TJAAXX007SLS', b.smart)
        self.assertEqual(b.ats_items()[0]['total_ats'], 20)

    def test_vw_rows_count_only_from_the_s3_source(self):
        b = build([inv('TJAAXX006SLS', wh=100)],
                  allocations=[{'sku': 'TJAAXX006SLS', 'qty': 30, 'source': 's3'},
                               {'sku': 'TJAAXX006SLS', 'qty': 999, 'source': 'manual'}])
        self.assertEqual(b.ats_items()[0]['total_ats'], 70)


class OverseasTab(unittest.TestCase):
    def test_one_row_per_delivery_from_engine_slots(self):
        b = build([inv('TJAAXX010SLS', incoming=600, committed=200)],
                  [lot('TJAAXX010SLS', 400, 'ZZ26010', arrival='2026-04-01'),
                   lot('TJAAXX010SLS', 200, 'ZZ26011', arrival='2026-06-01')],
                  [order('TJAAXX010SLS', 200, '2026-04-15', '2026-04-30')])
        rows = b.tab_rows('incoming')
        by_ref = {r['po_ref']: r for r in rows}
        self.assertEqual(by_ref['ZZ26010']['total_ats'], 200)   # the April order sits on the April lot
        self.assertEqual(by_ref['ZZ26011']['total_ats'], 200)
        self.assertEqual(by_ref['ZZ26010']['arrival'], 'Apr 1, 2026')
        self.assertEqual(by_ref['ZZ26010']['tjx_source'], 'Overseas')

    def test_legacy_fifo_spread_when_the_engine_does_not_route(self):
        # no orders at all: committed 300 is not routed; legacy FIFO eats the earliest lot first
        b = build([inv('TJAAXX012SLS', incoming=600, committed=300)],
                  [lot('TJAAXX012SLS', 400, 'ZZ26012', arrival='2026-04-01'),
                   lot('TJAAXX012SLS', 200, 'ZZ26013', arrival='2026-06-01')], orders=[])
        rows = {r['po_ref']: r['total_ats'] for r in b.tab_rows('incoming')}
        self.assertEqual(rows, {'ZZ26012': 100, 'ZZ26013': 200})

    def test_landed_lot_is_suppressed(self):
        # a lot that arrived 3 days ago with the same units now in the warehouse is not supply
        b = build([inv('TJAAXX014SLS', wh=300, incoming=300)],
                  [lot('TJAAXX014SLS', 300, 'ZZ26014', arrival='2026-02-27')])
        self.assertEqual(b.tab_rows('incoming'), [])

    def test_hidden_landing_lots_never_show(self):
        b = build([inv('TJAAXX015SLS', incoming=300)],
                  [lot('TJAAXX015SLS', 300, 'ZZ26015', arrival='2026-05-01', landing='NJ')])
        rows = b.tab_rows('incoming')
        self.assertEqual(len(rows), 1)
        self.assertNotIn('po_ref', rows[0])        # no visible delivery: one style row
        self.assertEqual(rows[0]['arrival'], 'TBD')

    def test_undated_lot_prints_tbd_not_a_dash(self):
        b = build([inv('TJAAXX016SLS', incoming=300)], [lot('TJAAXX016SLS', 300, 'ZZ26016')])
        row = b.tab_rows('incoming')[0]
        self.assertEqual(row['arrival'], 'TBD')
        self.assertNotIn('—', ''.join(str(v) for v in row.values()))


class Filters(unittest.TestCase):
    def test_brand_order_ats_sort_and_row_minimum(self):
        b = build([inv('TJBBXX020SLS', brand='BBB', wh=500), inv('TJAAXX021SLS', wh=40),
                   inv('TJAAXX022SLS', wh=900), inv('TJAAXX023SLS', wh=35), inv('RXAAXX024SLS', wh=800)])
        rows = b.tab_rows('ats', sku_filter=lambda s: s[:2] in ('TJ', 'TM'), min_ats=36)
        self.assertEqual([r['sku'] for r in rows], ['TJAAXX022SLS', 'TJAAXX021SLS', 'TJBBXX020SLS'])
        self.assertEqual(rows[0]['warehouse'], 'TR')
        self.assertEqual(rows[0]['tjx_source'], 'Warehouse')

    def test_brand_filter(self):
        b = build([inv('TJBBXX030SLS', brand='BBB', wh=500), inv('TJAAXX031SLS', wh=500)])
        rows = b.tab_rows('ats', brand_filter=lambda k: k != 'BBB')
        self.assertEqual([r['sku'] for r in rows], ['TJAAXX031SLS'])

    def test_summary(self):
        rows = [{'sku': 'A', 'brand_full': 'Brand A', 'total_ats': 50},
                {'sku': 'A', 'brand_full': 'Brand A', 'total_ats': 70},
                {'sku': 'B', 'brand_full': 'Brand B', 'total_ats': 40}]
        s = T.summarize(rows, overseas=True)
        self.assertEqual((s['rows'], s['units'], s['styles']), (3, 160, 2))
        self.assertEqual(s['brands'][0], {'brand': 'Brand A', 'rows': 2, 'units': 120})


if __name__ == '__main__':
    unittest.main()
