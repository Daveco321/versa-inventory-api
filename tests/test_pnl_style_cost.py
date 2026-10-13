"""Synthetic tests for the Inventory Analytics "where does this cost come from" helpers in pnl.py (Oct 13 2026).

No real data: a tiny dataset body with sentinel prices stands in for the built dataset memo.
Run: python -m unittest discover -s tests -p "test_pnl_style_cost.py"
"""
import json
import os
import sys
import threading
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import pnl  # noqa: E402
import pnl_engine as E  # noqa: E402


def service():
    svc = pnl._PnlService.__new__(pnl._PnlService)
    svc._lock = threading.Lock()
    svc._memo = None
    svc._why_memo = None
    svc._quote_memo = None
    return svc


STYLE_FIELDS = ['base', 'brand', 'fobU', 'level', 'grade', 'costRule', 'costSpread', 'costByFactory', 'costRow',
                'costRowsSkipped', 'costFlags', 'factories', 'ev', 'kitPcs']
DATASET = {
    'builtAt': '2026-10-13T12:00:00Z',
    'inputs': {'costbook_generated': '2026-10-07T00:00:00Z'},
    'settings': {'costRule': {'mode': 'factories'}},
    'dict': {'factories': {'QF1': {'name': 'Factory One'}, 'QF2': {'name': 'Factory Two'}},
             'brands': {'QA': 'Brand QA'}, 'levels': {'L4a': {'label': 'Calculator row'}},
             'grades': {'B': {'label': 'Good', 'desc': 'x'}}, 'costRules': {}, 'flags': {}, 'basis': {},
             'fiberClasses': {}, 'regimes': {}, 'categories': {},
             'customers': {'ROSS': {'name': 'Ross Stores', 'group': 'TJX'}}},
    'styles': {'fields': STYLE_FIELDS, 'rows': [
        ['ROQAQF101SLS', 'QA', 1.5555, 'L4a', 'B', 'average', 0.05,
         [['QF1', 'calculator', 'S1', 1.4444, 'L4a'], ['QF2', 'list', 'PO1', 1.6666, 'L1']],
         ['NF', 'F10', 'QA solid', 'slim', 1.4444], [['NF!F11', 'QA print', 1.9999, 'other_pattern']],
         ['ambiguous_rows'], ['QF1', 'QF2'], ['NF!F10'], None],
        ['TJQAQF522-304', 'QA', 1.2222, 'L0', 'A', 'manual', None, None, None, [], [], [], ['OVR:m1'], None],
    ]},
    'shipped': {'byCustomer': {'fields': ['cust', 'base', 'units', 'fobU', 'level', 'grade'], 'rows': [
        ['ROSS', 'ROQAQF101SLS', 100, 1.5555, 'L4a', 'B'],
        ['ROSS', 'TJQAQF522-304', 10, 1.2222, 'L0', 'A'],
    ]},
                'pending': {'ready': True, 'byPair': {'fields': ['cust', 'base', 'units', 'invUnits', 'rev', 'fobU', 'fob', 'level', 'grade'],
                                                      'rows': [['MARS', 'ROQAQF101SLS', 72, 2, 240.0, 1.5555, 112.0, 'L4a', 'B']]}}},
    'evidence': {'NF!F10': {'src': 'calc.xlsx', 'sheet': 'TJX', 'cell': 'F10', 'row': 10},
                 'NF!F11': {'src': 'calc.xlsx', 'sheet': 'TJX', 'cell': 'F11', 'row': 11},
                 'NF!F99': {'src': 'calc.xlsx', 'sheet': 'TJX', 'cell': 'F99', 'row': 99},
                 'OVR:m1': {'kind': 'override', 'by': 'tester'}},
}


def memo(key, built_at='2026-10-13T12:00:00Z', started=1.0):
    return {'key': key, 'body': json.dumps(DATASET).encode('utf-8'), 'builtAt': built_at, 'started': started,
            'checked': started, 'stamps': {}, 'gz': None}


class WhyIndex(unittest.TestCase):
    def test_index_follows_the_dataset_build(self):
        svc, key = service(), ('cb', 'st', 'ov')
        self.assertIsNone(svc._why_index(key))                 # no memo yet
        svc._memo = memo(key)
        idx = svc._why_index(key)
        self.assertEqual(set(idx['styles']), {'ROQAQF101SLS', 'TJQAQF522-304'})
        self.assertEqual(set(idx['custs']), {('ROSS', 'ROQAQF101SLS'), ('ROSS', 'TJQAQF522-304')})
        self.assertEqual(idx['costRuleMode'], 'factories')
        self.assertEqual(idx['costbookGenerated'], '2026-10-07T00:00:00Z')
        self.assertIs(svc._why_index(key), idx)                 # memoized for the same build
        svc._memo = memo(key, built_at='2026-10-13T13:00:00Z', started=2.0)
        self.assertIsNot(svc._why_index(key), idx)              # a new build replaces it
        self.assertIs(svc._why_index(('a', 'b', 'c')), None)   # another store key: nothing


class Payload(unittest.TestCase):
    def setUp(self):
        self.svc, self.key = service(), ('cb', 'st', 'ov')
        self.svc._memo = memo(self.key)
        self.idx = self.svc._why_index(self.key)

    def test_style_with_a_sheet_row(self):
        out = self.svc._style_cost_payload(E, self.idx, 'ROQAQF101SLS', 'ROSS')
        s = out['styleCost']
        self.assertEqual((out['base'], out['customer'], out['customerName']), ('ROQAQF101SLS', 'ROSS', 'Ross Stores'))
        self.assertEqual((s['fobU'], s['costRule'], s['costSpread']), (1.5555, 'average', 0.05))
        self.assertEqual(s['costRow'], ['NF', 'F10', 'QA solid', 'slim', 1.4444])
        self.assertEqual(s['costRowsSkipped'], [['NF!F11', 'QA print', 1.9999, 'other_pattern']])
        self.assertEqual(len(s['costByFactory']), 2)
        # evidence: only the cells this style names (the row used, the rows set aside, its ev list)
        self.assertEqual(set(out['evidence']), {'NF!F10', 'NF!F11'})
        self.assertEqual(out['customerCost']['units'], 100)
        self.assertIsNone(out['groupQuote'])                   # same cost for the customer: no block quote
        self.assertIsNone(out['reason'])
        self.assertEqual(out['dict']['factories']['QF1']['name'], 'Factory One')
        self.assertEqual(out['datasetBuiltAt'], '2026-10-13T12:00:00Z')

    def test_manual_cost_on_a_dashed_history_code(self):
        out = self.svc._style_cost_payload(E, self.idx, 'TJQAQF522-304', 'ROSS')
        self.assertEqual(out['styleCost']['level'], 'L0')
        self.assertEqual(set(out['evidence']), {'OVR:m1'})
        self.assertEqual(out['customerCost']['fobU'], 1.2222)

    def test_a_customer_that_only_shipped_awaiting_invoice_takes_the_pair_cost(self):
        out = self.svc._style_cost_payload(E, self.idx, 'ROQAQF101SLS', 'MARS')
        c = out['customerCost']
        self.assertEqual((c['cust'], c['fobU'], c['units'], c['pieces'], c['awaitingInvoice']), ('MARS', 1.5555, 2, 72, True))
        self.assertEqual((c['level'], c['grade']), ('L4a', 'B'))
        self.assertIsNone(out['groupQuote'])                   # same cost as the style: no block quote
        self.assertNotIn('awaitingInvoice', self.svc._style_cost_payload(E, self.idx, 'ROQAQF101SLS', 'ROSS')['customerCost'])

    def test_size_suffix_folds_to_the_base_and_no_customer(self):
        out = self.svc._style_cost_payload(E, self.idx, 'ROQAQF101SLS-L', None)
        self.assertEqual(out['base'], 'ROQAQF101SLS')
        self.assertIsNotNone(out['styleCost'])
        self.assertIsNone(out['customerCost'])
        self.assertIsNone(out['customerName'])

    def test_unknown_style_answers_with_a_reason(self):
        out = self.svc._style_cost_payload(E, self.idx, 'ZZQAQF999SLS', 'ROSS')
        self.assertIsNone(out['styleCost'])
        self.assertIsNone(out['customerCost'])
        self.assertEqual(out['evidence'], {})
        self.assertIn('no cost yet', out['reason'])

    def test_customer_at_another_cost_asks_for_the_block_quote_safely(self):
        # The customer row carries a different cost; the block quote needs the cost index, which this bare
        # service cannot build, so it must stay None rather than raise.
        idx = dict(self.idx)
        idx['custs'] = dict(idx['custs'])
        idx['custs'][('ROSS', 'ROQAQF101SLS')] = ['ROSS', 'ROQAQF101SLS', 100, 1.1111, 'L4a', 'B']
        out = self.svc._style_cost_payload(E, idx, 'ROQAQF101SLS', 'ROSS', self.key)
        self.assertEqual(out['customerCost']['fobU'], 1.1111)
        self.assertIsNone(out['groupQuote'])


class RouteTable(unittest.TestCase):
    def test_route_is_registered_as_get_only(self):
        self.assertIn(('/api/pnl/style-cost', ('GET',)), pnl.ROUTE_TABLE)


if __name__ == '__main__':
    unittest.main()
