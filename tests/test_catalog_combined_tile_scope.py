"""Catalog scope (David, Oct 15 2026): 'all_brands=1' on a regular catalog link is the
builder's Combined Tile checkbox; the page merges the SELECTED brands into one tile. The
server must scope such a link to its listed brands, never serve every brand. Segment
links (wh_/os_all_brands) and links that name no brand keep the all-brand scope.
Synthetic catalogs only, offline. Run from tests/: python -B -m unittest test_catalog_combined_tile_scope
"""
import unittest

from _support import load_app

CATS = [
    {'slug': 'tile', 'name': 'Z Tile', 'url': 'https://x.test/?brands=NAUTICA,DKNY&all_brands=1&view=ats'},
    {'slug': 'plain', 'name': 'Z Plain', 'url': 'https://x.test/?brands=NAUTICA,DKNY&view=ats'},
    {'slug': 'seg', 'name': 'Z Seg', 'url': 'https://x.test/?segment=1&wh_all_brands=1&os_brands=DKNY'},
    {'slug': 'tile-nobrand', 'name': 'Z Tile none', 'url': 'https://x.test/?brands=__SEG__&all_brands=1'},
]


class CombinedTileScope(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.app = load_app()

    def setUp(self):
        self._saved = self.app._load_saved_catalogs_cached
        self.app._load_saved_catalogs_cached = lambda max_age=60: CATS

    def tearDown(self):
        self.app._load_saved_catalogs_cached = self._saved

    def test_combined_tile_link_is_scoped_to_its_brands(self):
        s = self.app._catalog_scope_for_slug('tile')
        self.assertFalse(s['all'])
        self.assertEqual(s['brands'], {'NAUTICA', 'DKNY'})
        self.assertEqual(s['brands'], self.app._catalog_scope_for_slug('plain')['brands'])

    def test_segment_all_brands_still_means_every_brand(self):
        self.assertTrue(self.app._catalog_scope_for_slug('seg')['all'])

    def test_a_tile_link_naming_no_brand_keeps_every_brand(self):
        self.assertTrue(self.app._catalog_scope_for_slug('tile-nobrand')['all'])


if __name__ == '__main__':
    unittest.main()
