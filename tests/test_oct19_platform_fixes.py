"""Three platform fixes (David, Oct 19 2026), offline and synthetic (serial 9xx, fabric ZZ):

1. The TJMAXX ATS email reads the TJX catalog feeds with the catalog's brand list widened,
   so its own brand rules decide; an ordinary request on the same slug stays narrowed.
2. On an 'all' deck the chip says 'In Warehouse' only when routing leaves sellable
   warehouse units; a card whose warehouse stock is all committed shows its arrival.
3. Arrival suppression is off: a lot arriving inside 14 days whose size matches the
   warehouse stock still counts as incoming.
"""
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _support import load_app  # noqa: E402
import test_line_sheet_overseas as T  # noqa: E402

S_WH_GONE = 'TJDKZZ951SLS'   # 100 in the warehouse, all of it on an October order; 1,000 arriving Dec 15
S_SAMESIZE = 'TJDKZZ952SLS'  # 836 in TR from an earlier lot; the next 840-unit lot arrives in 10 days


class AllDeckChip(T._Base):
    EXTRA_FEED = [T._item(S_WH_GONE, 'DKNY', jtw=100, inc=1000, committed=-100)]
    EXTRA_LEDGER = [T._lot(S_WH_GONE, 1000, '2026-12-15', prod='ZZ26951')]
    EXTRA_ORDERS = [T._order(S_WH_GONE, 100, '2026-10-20', '2026-10-30', '9100951')]

    def cards(self, customer_view=True):
        got = self.app._pres_stock_cards('all', customer_view, self.app._pres_filter_spec({}), {})
        return {c['sku']: c for c in got['cards']}

    def test_warehouse_all_committed_shows_the_arrival(self):
        for cv in (True, False):
            with self.subTest(customer_view=cv):
                c = self.cards(cv)[S_WH_GONE]
                self.assertEqual(c['number'], 1000)
                self.assertEqual(c['chip'][1], 'arr')
                self.assertTrue(c['chip'][0].startswith('Arrives '))

    def test_sellable_warehouse_stock_keeps_in_warehouse(self):
        # S_SPLIT: 6,732 in JTW, nothing committed
        c = self.cards()[T.S_SPLIT]
        self.assertEqual(c['chip'], ('In Warehouse · JTW', 'wh'))


class NoArrivalSuppression(T._Base):
    EXTRA_FEED = [dict(T._item(S_SAMESIZE, 'DKNY', inc=840), tr=836)]
    EXTRA_LEDGER = [T._lot(S_SAMESIZE, 840, '2026-10-26', wh='JTW', prod='ZZ26952')]

    def test_same_size_lot_inside_14_days_still_counts(self):
        got = self.app._pres_stock_cards('overseas', True, self.app._pres_filter_spec({'skus': [S_SAMESIZE]}), {})
        self.assertEqual([(c['sku'], c['number']) for c in got['cards']], [(S_SAMESIZE, 840)])
        out = self.sheet([{'title': 'OS', 'skus': [S_SAMESIZE], 'stock': 'overseas'}], customer_view=True)
        self.assertNotIn('error', out)
        self.assertEqual(self.printed(), {S_SAMESIZE: 840})


class TjxEmailBrandScope(unittest.TestCase):
    CATS = [{'slug': 'tjxzz', 'name': 'Z TJX', 'url': 'https://x.test/?brands=NAUTICA,DKNY&all_brands=1&view=ats'}]

    @classmethod
    def setUpClass(cls):
        cls.app = load_app()

    def setUp(self):
        a = self.app
        self._saved = (a._load_saved_catalogs_cached, a._TJX_ATS_SLUG)
        a._load_saved_catalogs_cached = lambda max_age=60: self.CATS
        a._TJX_ATS_SLUG = 'tjxzz'

    def tearDown(self):
        self.app._load_saved_catalogs_cached, self.app._TJX_ATS_SLUG = self._saved

    def scope_for(self, slug, flag):
        a = self.app
        env = {'versa.tjx_ats_all_brands': True} if flag else {}
        with a.app.test_request_context(f'/inventory?catalog_slug={slug}', environ_base=env):
            a.g.pop('_catalog_scope', None)
            a.authz_gate()
            return a.g.get('_catalog_scope')

    def test_email_feed_gets_every_brand(self):
        self.assertTrue(self.scope_for('tjxzz', True)['all'])

    def test_ordinary_catalog_request_stays_narrowed(self):
        s = self.scope_for('tjxzz', False)
        self.assertFalse(s['all'])
        self.assertEqual(s['brands'], {'NAUTICA', 'DKNY'})

    def test_flag_never_widens_another_slug(self):
        self.CATS = self.CATS + [{'slug': 'otherzz', 'name': 'Z Other', 'url': 'https://x.test/?brands=DKNY'}]
        self.app._load_saved_catalogs_cached = lambda max_age=60: self.CATS
        self.assertFalse(self.scope_for('otherzz', True)['all'])

    def test_feed_helper_sends_the_flag(self):
        seen = {}
        flask_app = self.app.app

        class FakeClient:
            def __enter__(s):
                return s

            def __exit__(s, *a):
                return False

            def get(s, path, query_string=None, environ_base=None):
                seen['env'] = environ_base
                seen['qs'] = query_string

                class R:
                    status_code = 200
                    headers = {'X-Catalog-Scope': 'tjxzz'}

                    def get_json(self, silent=False):
                        return {'inventory': []}
                return R()
        orig = flask_app.test_client
        flask_app.test_client = lambda: FakeClient()
        try:
            self.app._tjx_catalog_feed('/inventory', 'inventory')
        finally:
            flask_app.test_client = orig
        self.assertEqual(seen['env'], {'versa.tjx_ats_all_brands': True})
        self.assertEqual(seen['qs']['catalog_slug'], 'tjxzz')


if __name__ == '__main__':
    unittest.main()
