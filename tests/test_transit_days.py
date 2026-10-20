"""Ex-factory to warehouse transit (David, Oct 20 2026).

Inventory platform: a lot with no port date arrives ETD + 55 days, 60 for pants,
and 60 for any style on a Pinnacle (PC) production order. Was 45 / 55.
Factory page (/factory-view, Versa-Docs): ETD + 35 days, 45 for pants and for
Pinnacle. Was a flat 35. A port-dated lot keeps its port date + 10 on both.

The rule has three Python copies (app.py, pnl_routing, and through it tjx_ats and
pnl_engine). These tests pin the numbers and keep the copies equal.

Everything here is synthetic: made-up style numbers (fabric ZZ, serial 9xx) and
production numbers.
"""
import os
import sys
import types
import unittest
from datetime import date, datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _support import load_app  # noqa: E402

SHIRT = 'TJNAZZ931SLS'
DRESS_PANTS = 'TJUSZZP91SLS'     # P##X serial
BOTTOMS = 'TJVDBA932SCP'         # sportswear bottoms fabric code BA
ETD = '2026-10-01'

REFS = ['TF26901', 'NB26901', 'DP26901', 'FR26901', 'NK26901', 'N/A', '', None,
        'PC26901', 'pc26901', '  PC26901 ', 'EXT-1']


def _row(style, ref, etd=ETD, arrival=None, port_dated=False, fob=False):
    return {'production': ref, 'poName': 'TEST', 'style': style, 'units': 100, 'brand': 'X',
            'etd': etd, 'arrival': arrival, 'port_dated': port_dated, 'fob_flag': fob,
            'fob_note': 'be ready' if fob else '', 'warehouse': 'JTW'}


class TestInventoryTransit(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.app = load_app()
        import pnl_routing
        cls.R = pnl_routing

    def test_the_numbers(self):
        days = self.app._inventory_transit_days
        self.assertEqual(days(False, 'TF26901'), 55)
        self.assertEqual(days(True, 'TF26901'), 60)
        self.assertEqual(days(False, 'PC26901'), 60)
        self.assertEqual(days(True, 'PC26901'), 60)
        self.assertEqual(days(False, ' pc26901 '), 60)
        self.assertEqual(days(False, ''), 55)
        self.assertEqual(days(False, None), 55)
        self.assertEqual(days(False, 'N/A'), 55)

    def test_app_and_routing_module_agree(self):
        for pants in (False, True):
            for ref in REFS:
                self.assertEqual(self.app._inventory_transit_days(pants, ref),
                                 self.R.transit_days(pants, ref), (pants, ref))

    def test_apo_prod_arrival(self):
        arr = self.app._apo_prod_arrival
        self.assertEqual(arr(_row(SHIRT, 'TF26901'), False), date(2026, 11, 25))
        self.assertEqual(arr(_row(DRESS_PANTS, 'TF26901'), True), date(2026, 11, 30))
        self.assertEqual(arr(_row(SHIRT, 'PC26901'), False), date(2026, 11, 30))
        self.assertEqual(arr(_row(BOTTOMS, 'PC26901'), True), date(2026, 11, 30))
        # A port date wins, Pinnacle included.
        self.assertEqual(arr(_row(SHIRT, 'PC26901', arrival='2026-11-02', port_dated=True), False),
                         date(2026, 11, 2))
        self.assertIsNone(arr(_row(SHIRT, 'TF26901', etd=None), False))

    def test_every_pants_rule_counts(self):
        self.assertTrue(self.app._py_is_bottom(DRESS_PANTS))
        self.assertTrue(self.app._py_is_bottom(BOTTOMS))
        self.assertFalse(self.app._py_is_bottom(SHIRT))
        self.assertTrue(self.R.is_pants(DRESS_PANTS))
        self.assertTrue(self.R.is_pants(BOTTOMS))
        self.assertFalse(self.R.is_pants(SHIRT))

    def test_routing_lots(self):
        q = {k: 0 for k in self.R.WAREHOUSE_KEYS}
        items = list(enumerate([_row(SHIRT, 'TF26901'), _row(SHIRT, 'PC26902'),
                                _row(SHIRT, 'TF26903', arrival='2026-11-02', port_dated=True)]))
        lots = self.R.build_lots(SHIRT, q, items, set(), datetime(2026, 10, 20, 9, 0))
        self.assertEqual([l['arr'] for l in lots],
                         [date(2026, 11, 25), date(2026, 11, 30), date(2026, 11, 2)])
        pants = self.R.build_lots(DRESS_PANTS, q, [(0, _row(DRESS_PANTS, 'TF26901'))], set(),
                                  datetime(2026, 10, 20, 9, 0))
        self.assertEqual(pants[0]['arr'], date(2026, 11, 30))

    def test_tjmaxx_email_production(self):
        import tjx_ats
        out = tjx_ats.map_production([_row(SHIRT, 'TF26901'), _row(SHIRT, 'PC26902'),
                                      _row(BOTTOMS, 'NB26903'),
                                      _row(SHIRT, 'PC26904', arrival='2026-11-02', port_dated=True)])
        self.assertEqual([p['arrival'] for p in out],
                         [date(2026, 11, 25), date(2026, 11, 30), date(2026, 11, 30), date(2026, 11, 2)])

    def test_pnl_production_arrival(self):
        import pnl_engine
        fake = types.SimpleNamespace(rt=self.R)
        get = pnl_engine._Build.arrival_of
        self.assertEqual(get(fake, _row(SHIRT, 'TF26901'), SHIRT), '2026-11-25')
        self.assertEqual(get(fake, _row(SHIRT, 'PC26901'), SHIRT), '2026-11-30')
        self.assertEqual(get(fake, _row(DRESS_PANTS, 'DP26901'), DRESS_PANTS), '2026-11-30')
        self.assertEqual(get(fake, _row(SHIRT, 'PC26901', arrival='2026-11-02', port_dated=True), SHIRT),
                         '2026-11-02')


class TestFactoryViewTransit(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.app = load_app()

    def test_the_numbers(self):
        t = self.app.FACTORY_VIEW_TRANSIT
        self.assertEqual((t['default_days'], t['pants_days'], t['pinnacle_days']), (35, 45, 45))
        self.assertEqual(t['pinnacle_prefixes'], ['PC'])

    def test_arrival(self):
        fv = self.app._factory_view_arrival
        self.assertEqual(fv(_row(SHIRT, 'TF26901')), '2026-11-05')
        self.assertEqual(fv(_row(DRESS_PANTS, 'TF26901')), '2026-11-15')
        self.assertEqual(fv(_row(BOTTOMS, 'NB26901')), '2026-11-15')
        self.assertEqual(fv(_row(SHIRT, 'PC26901')), '2026-11-15')
        self.assertEqual(fv(_row(SHIRT, 'PC26901', arrival='2026-11-02', port_dated=True)), '2026-11-02')
        self.assertIsNone(fv(_row(SHIRT, 'PC26901', etd=None, fob=True)))
        self.assertIsNone(fv(_row(SHIRT, 'TF26901', etd=None)))


if __name__ == '__main__':
    unittest.main()
