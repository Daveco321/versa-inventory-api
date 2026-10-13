"""Synthetic tests for the awaiting-invoice pair costs in the analytics cost maps (Oct 13 2026).

David: there will never be another invoice report; every style must be costed from what the platform holds.
A style that only shipped since the last invoice takes its cost from shipped.pending.byPair. Sentinel values only.
Run: python -m unittest discover -s tests -p "test_pnl_pending_pairs.py"
"""
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import pnl  # noqa: E402

FIELDS = ['cust', 'base', 'units', 'invUnits', 'rev', 'fobU', 'fob', 'duty', 'freight', 'fees', 'level', 'grade', 'pcs']


def shipped(rows):
    return {'pending': {'ready': True, 'byPair': {'fields': FIELDS, 'rows': rows}}}


class PendingPairCosts(unittest.TestCase):
    def test_pairs_fill_only_what_the_history_lacks(self):
        by_cust = {'ROSS': {'ZZAAAA001SLS': 1.1111}}
        by_cust2 = {'ROSS': {'ZZAAAA001SLS': [1.1111, 0.5555]}}
        pieces = {}
        rows = [
            ['ROSS', 'ZZAAAA001SLS', 100, 100, 900.0, 9.9999, 999.99, 10.0, 0.0, 0.0, 'L4a', 'B', 1],   # invoiced: kept
            ['MARS', 'ZZAAAA002SLS', 60, 60, 540.0, 1.2345, 74.07, 29.63, 0.0, 0.0, 'L4a', 'B', 1],      # new pair
            ['BJS', 'ZZKIT01', 72, 2, 240.0, 1.5, 108.0, 43.2, 0.0, 0.0, 'L1', 'A', 36],                 # cartons of 36
            ['MARS', 'ZZNOCOST9', 4, 4, 40.0, None, None, 0.0, 0.0, 0.0, 'L7', 'D', 1],                 # no cost: skipped
        ]
        n = pnl._PnlService._pending_pair_costs(shipped(rows), by_cust, by_cust2, pieces)
        self.assertEqual(n, 2)
        self.assertEqual(by_cust['ROSS']['ZZAAAA001SLS'], 1.1111)                 # the invoiced cost wins
        self.assertEqual(by_cust2['ROSS']['ZZAAAA001SLS'], [1.1111, 0.5555])
        self.assertEqual(by_cust['MARS']['ZZAAAA002SLS'], 1.2345)
        self.assertEqual(by_cust2['MARS']['ZZAAAA002SLS'], [round(74.07 / 60, 4), round(29.63 / 60, 4)])
        self.assertEqual(by_cust2['BJS']['ZZKIT01'], [54.0, 21.6])                 # per order-book unit (a carton)
        self.assertEqual(pieces, {'BJS': {'ZZKIT01': 36.0}})
        self.assertNotIn('ZZNOCOST9', by_cust.get('MARS', {}))

    def test_missing_or_old_shapes_add_nothing(self):
        for sh in (None, {}, {'pending': {}}, {'pending': {'byPair': {}}},
                   {'pending': {'byPair': {'fields': ['cust', 'base'], 'rows': [['ROSS', 'X']]}}}):
            by_cust, by_cust2, pieces = {}, {}, {}
            self.assertEqual(pnl._PnlService._pending_pair_costs(sh, by_cust, by_cust2, pieces), 0)
            self.assertEqual((by_cust, by_cust2, pieces), ({}, {}, {}))
        # fobU without the money columns still gives the per-piece map
        sh = {'pending': {'byPair': {'fields': ['cust', 'base', 'fobU'], 'rows': [['ROSS', 'ZZAAAA003SLS', 2.5]]}}}
        by_cust, by_cust2, pieces = {}, {}, {}
        self.assertEqual(pnl._PnlService._pending_pair_costs(sh, by_cust, by_cust2, pieces), 0)
        self.assertEqual(by_cust, {'ROSS': {'ZZAAAA003SLS': 2.5}})
        self.assertEqual(by_cust2, {})


if __name__ == '__main__':
    unittest.main()
