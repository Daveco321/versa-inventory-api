"""TJMAXX ATS email: U.S. Polo Assn., Nicole Miller and Von Dutch stay out of BOTH tabs
(David, Oct 19 2026: TJX does not buy them). Offline: the feeds, the routing builder and the
decorations are fakes; synthetic styles only."""
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _support import load_app  # noqa: E402

import tjx_ats  # noqa: E402
import tjx_display  # noqa: E402

BRANDS = ['DKNY', 'NAUTICA', 'USPA', 'NICOLE', 'VD']


class FakeBuilder:
    def __init__(self, *a, **k):
        pass

    def tab_rows(self, view, sku_filter=None, brand_filter=None, min_ats=0):
        rows = [{'sku': 'TMZZXX9%02dSLS' % i, 'brand_key': b, 'view': view} for i, b in enumerate(BRANDS)]
        return [r for r in rows if brand_filter is None or brand_filter(r['brand_key'])]


class TjxAtsBrandExclusion(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.app = load_app()

    def setUp(self):
        a = self.app
        self.saved = [(a, n, getattr(a, n)) for n in
                      ('_tjx_catalog_feed', '_fetch_all_open_orders', '_apo_color_map', '_customer_export_scrub')]
        self.saved += [(tjx_ats, 'TjxAtsBuilder', tjx_ats.TjxAtsBuilder), (tjx_ats, 'summarize', tjx_ats.summarize),
                       (tjx_display, 'TjxDisplay', tjx_display.TjxDisplay)]
        a._tjx_catalog_feed = lambda path, key, **p: [{'sku': 'TMZZXX901SLS'}]
        a._fetch_all_open_orders = lambda: ([{'po': 'ZZ1'}], True)
        a._apo_color_map = lambda: {}
        a._customer_export_scrub = lambda rows, *x: rows
        tjx_ats.TjxAtsBuilder = FakeBuilder
        tjx_ats.summarize = lambda rows, overseas=False: len(rows)
        tjx_display.TjxDisplay = lambda *x, **k: None

    def tearDown(self):
        for mod, name, val in self.saved:
            setattr(mod, name, val)

    def test_neither_tab_carries_brands_tjx_does_not_buy(self):
        out = self.app.build_tjx_ats()
        for tab in ('warehouse', 'overseas'):
            keys = {r['brand_key'] for r in out[tab]}
            self.assertEqual(keys, {'DKNY', 'NAUTICA'}, tab)


if __name__ == '__main__':
    unittest.main()
