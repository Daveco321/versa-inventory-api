"""Machine-key fee maintenance (Oct 6 2026): /admin/pnl-fees and /admin/pnl-missing-costs.

The machine key may read and change ONLY the four fee blocks of the P&L settings and list the
style numbers that have no cost. Synthetic values only (this repo is public)."""
import json
import os
import re
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pnl  # noqa: E402
from tests.test_pnl_auth import Harness, PnlTestCase  # noqa: E402

FEES = ('revenueCosts', 'opex', 'royalty', 'deductions')


class _Resp:
    def __init__(self, status):
        self.status_code = status


class MachineFeesTest(PnlTestCase):
    def harness(self):
        h = Harness(self._root)
        self._harnesses.append(h)
        return h

    def seed(self, h, settings):
        h.svc.store.put_obj(pnl.SETTINGS, settings, expected_etag=None)

    def base_settings(self):
        return {'revenueCosts': {'items': [{'key': 'alpha', 'name': 'Alpha', 'pct': 1.13},
                                           {'key': 'beta', 'name': 'Beta', 'pct': 0.77}]},
                'opex': {'items': [{'name': 'Gamma', 'monthly': 4321}]},
                'royalty': {'defaultPct': 3.31, 'base': 'net', 'byBrand': {'ZZ': 3.31}},
                'deductions': {'byGroup': {'other': 1.97}, 'byCustomer': {}},
                'fx': {'rate': None}}

    def test_get_returns_only_the_fee_blocks(self):
        h = self.harness()
        self.seed(h, self.base_settings())
        status, body = h.svc.machine_fees('GET')
        self.assertEqual(status, 200)
        self.assertEqual(set(body['fees']), set(FEES))
        self.assertEqual(body['fees']['opex']['items'][0]['monthly'], 4321)
        self.assertNotIn('fx', json.dumps(body))
        self.assertIsNotNone(body['etag'])

    def test_post_changes_fees_and_keeps_everything_else(self):
        h = self.harness()
        self.seed(h, self.base_settings())
        _s, cur = h.svc.machine_fees('GET')
        rc = {'items': [{'key': 'alpha', 'name': 'Alpha renamed', 'pct': 1.27},
                        {'key': 'beta', 'name': 'Beta', 'pct': 0.77},
                        {'key': 'delta', 'name': 'Delta', 'pct': 2.43}]}
        status, body = h.svc.machine_fees('POST', {'expected_etag': cur['etag'], 'revenueCosts': rc,
                                                   'opex': {'items': []}})
        self.assertEqual(status, 200, body)
        stored, _etag = h.svc.store.get_obj(pnl.SETTINGS)
        self.assertEqual([i['name'] for i in stored['revenueCosts']['items']], ['Alpha renamed', 'Beta', 'Delta'])
        self.assertEqual(stored['opex']['items'], [])
        self.assertEqual(stored['royalty']['defaultPct'], 3.31)        # untouched block kept
        self.assertIn('fx', stored)                                    # non-fee setting kept
        self.assertEqual(stored['updatedBy'], 'machine key (fee rates)')
        self.assertIn('fee rates changed with the machine key: opex,revenueCosts', self._out.getvalue())

    def test_post_refuses_any_other_setting(self):
        h = self.harness()
        self.seed(h, self.base_settings())
        _s, cur = h.svc.machine_fees('GET')
        status, body = h.svc.machine_fees('POST', {'expected_etag': cur['etag'], 'fx': {'rate': 7.0}})
        self.assertEqual(status, 400)
        self.assertEqual(body['refused'], ['fx'])
        stored, _ = h.svc.store.get_obj(pnl.SETTINGS)
        self.assertIsNone(stored['fx']['rate'])

    def test_post_needs_a_fresh_etag(self):
        h = self.harness()
        self.seed(h, self.base_settings())
        status, _ = h.svc.machine_fees('POST', {'royalty': {'defaultPct': 4.03, 'base': 'net', 'byBrand': {}}})
        self.assertEqual(status, 400)
        status, _ = h.svc.machine_fees('POST', {'expected_etag': 'stale', 'royalty': {'defaultPct': 4.03,
                                                                                       'base': 'net', 'byBrand': {}}})
        self.assertEqual(status, 409)

    def test_post_rejects_an_out_of_range_rate(self):
        h = self.harness()
        self.seed(h, self.base_settings())
        _s, cur = h.svc.machine_fees('GET')
        status, body = h.svc.machine_fees('POST', {'expected_etag': cur['etag'],
                                                   'revenueCosts': {'items': [{'key': 'alpha', 'name': 'A', 'pct': 99}]}})
        self.assertEqual(status, 422, body)

    def test_missing_costs_lists_uncosted_styles_without_cost_values(self):
        h = self.harness()
        svc = h.svc
        svc.store.head = lambda n: 'e1'                     # a cost book exists
        svc._module = lambda kind: object()
        svc._dataset_for = lambda key, fresh: _Resp(200)
        svc.sales_matrix = lambda: {'customers': {'AAA': {'TTXXAA001SLS': {'2026-01': [10, 50]},
                                                          'TTXXAA002SLS': {'2026-02': [5, 20]}},
                                                  'BBB1': {'TTXXAA002SLS': {'2026-03': [7, 30]}}},
                                    'source': {'to': '2026-03-31'}}
        svc._analytics_cost_maps = lambda key: {'byStyle': {'TTXXAA001SLS': 7.0013}, 'byCust': {}, 'byCust2': {},
                                                'alias': {'BBB1': 'BBB'}, 'builtAt': 't'}
        ds = {'styles': {'fields': ['base', 'brand', 'fobU', 'onHand', 'incoming', 'openUnits'],
                         'rows': [['TTXXAA003SLS', 'XX', None, 36, 0, 12], ['TTXXAA001SLS', 'XX', 7.0013, 1, 0, 0]]}}
        svc._memo = {'key': ('e1', 'e1', 'e1'), 'body': json.dumps(ds)}
        status, body = svc.machine_missing_costs()
        self.assertEqual(status, 200, body)
        self.assertEqual([r['base'] for r in body['sold']], ['TTXXAA002SLS'])
        self.assertEqual(body['sold'][0]['units'], 12)
        self.assertEqual(body['sold'][0]['customers'], ['BBB', 'AAA'])
        self.assertEqual((body['sold'][0]['first'], body['sold'][0]['last']), ('2026-02', '2026-03'))
        self.assertEqual([r['base'] for r in body['active']], ['TTXXAA003SLS'])
        self.assertNotIn('7.0013', json.dumps(body))                     # never a cost value
        self.assertNotIn('fobU', json.dumps(body))


class MachineRoutesWiringTest(unittest.TestCase):
    """The host app: machine key only, GETs on the machine list, POST on the machine POST list."""

    def setUp(self):
        here = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        with open(os.path.join(here, 'app.py'), encoding='utf-8') as f:
            self.src = f.read()

    def test_allow_lists(self):
        extra = re.search(r"_AUTHZ_MACHINE_EXTRA = \{(.*?)\}", self.src, re.S).group(1)
        self.assertIn("'/admin/pnl-fees'", extra)
        self.assertIn("'/admin/pnl-missing-costs'", extra)
        self.assertRegex(self.src, r"'/admin/aging/seed',\s*'/admin/pnl-fees'[,)]")

    def test_routes_check_the_machine_tier(self):
        for fn in ('def admin_pnl_fees', 'def admin_pnl_missing_costs'):
            i = self.src.index(fn)
            self.assertIn("ident.get('tier') != 'machine'", self.src[i:i + 900])

    def test_no_api_pnl_path_was_opened(self):
        extra = re.search(r"_AUTHZ_MACHINE_EXTRA = \{(.*?)\}", self.src, re.S).group(1)
        self.assertNotIn('/api/pnl', extra)


if __name__ == '__main__':
    unittest.main()
