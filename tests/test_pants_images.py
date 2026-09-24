"""Pants photos in exports match the catalog tile (David, Sep 24 2026:
TMDVBAP10SCP / TMDVBAP32SCP showed on the catalog, not in the export)."""
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _support import load_app


class PantsImageKeyTests(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.app = load_app()

    def test_the_keys_match_the_catalog_tile(self):
        self.assertEqual(('VD_P10', 'VD_P10'), self.app.pants_image_keys('TMDVBAP10SCP', 'VD'))
        self.assertEqual(('VD_P32', 'VD_P32'), self.app.pants_image_keys('TMDVBAP32SCP', 'VD'))

    def test_dropbox_drops_the_leading_zero_swatch_keeps_it(self):
        self.assertEqual(('VD_P7', 'VD_P07'), self.app.pants_image_keys('BUVDBAP07SCP', 'VD'))

    def test_a_size_suffix_is_ignored(self):
        self.assertEqual(('VD_P10', 'VD_P10'), self.app.pants_image_keys('TMDVBAP10SCP-32', 'VD'))

    def test_non_pants_serials_get_no_keys(self):
        self.assertEqual((None, None), self.app.pants_image_keys('RODKPK001SLS', 'DKNY'))
        self.assertEqual((None, None), self.app.pants_image_keys('BUGBRPB05BRV', 'BEENE'))

    def test_bottoms_still_never_use_the_fabric_blind_key(self):
        self.assertTrue(self.app.is_bottoms_style('TMDVBAP10SCP'))
        self.assertNotIn('VD_010', self.app.pants_image_keys('TMDVBAP10SCP', 'VD'))


if __name__ == '__main__':
    unittest.main()
