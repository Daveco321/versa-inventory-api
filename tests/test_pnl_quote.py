"""Synthetic tests for the Cost Glossary quote helpers in pnl.py (Oct 8 2026).

No real data: the cost book, brands, fabrics and prices come from tests/test_pnl_engine.py (sentinel values).
Run: python -m unittest discover -s tests -p "test_pnl_quote.py"
"""
import os
import sys
import threading
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import pnl  # noqa: E402
import pnl_engine as E  # noqa: E402
from tests.test_pnl_engine import COSTBOOK, PARAMS  # noqa: E402


def service():
    svc = pnl._PnlService.__new__(pnl._PnlService)
    svc._lock = threading.Lock()
    svc._quote_memo = None
    return svc


def index(colour_map=None, overrides=None):
    return E.CostIndex(COSTBOOK, {}, overrides or [], [], today='2026-03-02', colour_map=colour_map)


class SyntheticStyle(unittest.TestCase):
    def test_builds_a_style_number_from_the_inputs(self):
        svc, ci = service(), index()
        s, problem = svc._quote_style_of(ci, {'brand': 'qa', 'fabric': 'qf', 'fit': 'REGULAR', 'sleeve': 'SS',
                                              'pattern': 'PRINT', 'group': 'CLUB'})
        self.assertIsNone(problem)
        self.assertEqual(s, 'CLQAQF000SRP')          # CL is the CLUB prefix in the synthetic params; serial 000
        d = E.decode_sku(s, PARAMS)
        self.assertEqual((d['brand'], d['fab'], d['fit'], d['sleeve'], d['pat'], d['group']),
                         ('QA', 'QF', 'REGULAR', 'SS', 'PRINT', 'CLUB'))

    def test_defaults_and_unknown_group(self):
        svc, ci = service(), index()
        s, problem = svc._quote_style_of(ci, {'brand': 'QA', 'fabric': 'QF'})
        self.assertEqual((s, problem), ('ZZQAQF000SLS', None))   # slim, long sleeve, solid, no group prefix
        for bad, field in (({'brand': 'Q', 'fabric': 'QF'}, 'brand'), ({'brand': 'QA', 'fabric': 'Q1F'}, 'fabric'),
                           ({'brand': 'QA', 'fabric': 'QF', 'fit': 'HUGE'}, 'fit'),
                           ({'brand': 'QA', 'fabric': 'QF', 'sleeve': 'XL'}, 'sleeve'),
                           ({'brand': 'QA', 'fabric': 'QF', 'pattern': 'PLAID'}, 'pattern'),
                           ({'brand': 'QA', 'fabric': 'QF', 'group': 'bad group!'}, 'group')):
            self.assertEqual(svc._quote_style_of(ci, bad), (None, field), bad)


class QuoteOne(unittest.TestCase):
    def test_shows_its_work(self):
        svc, ci = service(), index()
        listed = svc._quote_one(E, ci, 'ROQAQF101SLS')          # on the AA factory list: its list price
        self.assertEqual((listed['combined']['fobU'], listed['combined']['level']), (7.7777, 'L1'))
        self.assertEqual(listed['combined']['row']['id'], 'AA!J2')
        out = svc._quote_one(E, ci, 'ROQAQF105SLS')              # not listed: the calculator row
        self.assertIsNone(out['reason'])
        self.assertEqual(out['decoded']['brand'], 'QA')
        c = out['combined']
        self.assertIsNotNone(c)
        self.assertEqual(c['fobU'], 4.4444)                       # GN!F1 under the default grid (GN first)
        self.assertEqual(c['row']['id'], 'GN!F1')
        self.assertEqual((c['row']['sheet'], c['row']['cell'], c['row']['fit']), ('S', 'F1', 'slim'))
        self.assertEqual(c['row']['priceSheet'], 4.4444)
        self.assertEqual(c['row']['terms'], 'FOB')
        self.assertIn('landedU', c['landed'])
        self.assertEqual(c['landed']['regime'], 'us')
        self.assertGreater(c['landed']['landedU'], c['fobU'])     # itemized duty, freight and fees
        self.assertFalse(c['manual'])
        facs = {p['factory']: p for p in out['perFactory']}
        self.assertEqual(facs['AA']['level'], 'L3')                # the list factory: its list for the design
        self.assertEqual(facs['AA']['row']['ref'], 'AA26002')
        self.assertTrue(all('factoryName' in p for p in out['perFactory']))
        tt = svc._quote_one(E, ci, 'ROQAQF105SLS', factory='TT')  # a named factory: TT uses the GY grid first
        self.assertEqual([p['factory'] for p in tt['perFactory']], ['TT'])
        self.assertEqual((tt['perFactory'][0]['fobU'], tt['perFactory'][0]['row']['id']), (4.1717, 'GY!F1'))

    def test_unknown_fabric_and_bad_style(self):
        svc, ci = service(), index()
        out = svc._quote_one(E, ci, 'ROQAQZ101SLS')
        self.assertIsNone(out['combined'])
        self.assertEqual(out['perFactory'], [])
        self.assertIn('No sheet row', out['reason'])
        bad = svc._quote_one(E, ci, 'NOT-A-STYLE')
        self.assertIn('cannot be decoded', bad['reason'])

    def test_manual_cost_and_ddp_are_named(self):
        svc = service()
        ov = [{'id': 'o1', 'scope': 'style', 'key': {'style': 'ROQAQF101SLS'}, 'fobU': 9.9999, 'terms': 'DDP',
               'reason': 'synthetic', 'at': '2026-01-01T00:00:00Z'}]
        ci = index(overrides=ov)
        out = svc._quote_one(E, ci, 'ROQAQF101SLS')
        self.assertEqual(out['ladder']['fobU'], 9.9999)
        lad = out['ladder']
        self.assertTrue(lad['manual'])
        self.assertEqual((lad['fobU'], lad['level']), (9.9999, 'L0'))
        self.assertIn('ddp', lad['flags'])
        self.assertEqual(lad['landed']['regime'], 'none')
        self.assertEqual(lad['landed']['landedU'], 9.9999)       # a delivered price: nothing added

    def test_ref_style_manual_cost_shows_by_ref(self):
        svc = service()
        ov = [{'id': 'o2', 'scope': 'ref_style', 'key': {'ref': 'AA26009', 'style': 'ROQAQZ101SLS'}, 'fobU': 8.8888,
               'terms': 'DDP', 'reason': 'synthetic', 'at': '2026-01-01T00:00:00Z'}]
        ci = index(overrides=ov)
        out = svc._quote_one(E, ci, 'ROQAQZ101SLS')           # no sheet row for fabric QZ: only the PO's own cost
        self.assertIsNone(out['reason'])
        self.assertEqual([m['ref'] for m in out['manualRefs']], ['AA26009'])
        self.assertEqual((out['ladder']['fobU'], out['ladder']['level'], out['ladder']['manual']), (8.8888, 'L0', True))
        self.assertEqual(out['ladder']['landed']['regime'], 'none')

    def test_skipped_rows_name_their_reason(self):
        svc, ci = service(), index()
        out = svc._quote_one(E, ci, 'ROQAQF105SLS', factory='TT')
        p = out['perFactory'][0]
        self.assertEqual(p['factory'], 'TT')
        self.assertTrue(all(set(s) == {'id', 'text', 'priceSheet', 'reason'} for s in p['skipped']))


class BatchShape(unittest.TestCase):
    def test_batch_rows_follow_the_single_quote(self):
        svc, ci = service(), index()
        one = svc._quote_one(E, ci, 'ROQAQF105SLS')
        best = one['combined'] or one['ladder']
        self.assertEqual(best['row']['id'], 'GN!F1')
        self.assertEqual(svc._QUOTE_MAX_ROWS, 2000)
        self.assertIn(('/api/pnl/quote', ('GET',)), pnl.ROUTE_TABLE)
        self.assertIn(('/api/pnl/quote/batch', ('POST',)), pnl.ROUTE_TABLE)


if __name__ == '__main__':
    unittest.main()
