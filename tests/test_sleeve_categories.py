"""Long / short sleeve list dress shirts only (David, Sep 28 2026: long sleeve polos
were showing under Long Sleeve; they belong under Sportswear only). Prepack rules keep
the old inclusive read, so size packs on those styles do not change."""
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _support import load_app


class SleeveCategoryTests(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.app = load_app()

    def m(self, sku, cat, brand='BEENE', prepack=False):
        return self.app._py_matches_category(sku, brand, cat, for_prepack=prepack)

    def test_long_sleeve_polos_are_sportswear_only(self):
        for sku in ('BRGBPJ012SLM', 'BRGBPO010SLM'):
            self.assertFalse(self.m(sku, 'long_sleeve'), sku)
            self.assertTrue(self.m(sku, 'sportswear'), sku)

    def test_short_sleeve_polos_are_sportswear_only(self):
        for sku in ('BRGBPH002SSM', 'BRGBPJ012SSM'):
            self.assertFalse(self.m(sku, 'short_sleeve'), sku)
            self.assertTrue(self.m(sku, 'sportswear'), sku)

    def test_young_men_shirts_leave_the_sleeve_filters(self):
        self.assertFalse(self.m('AMVDCO027SLS', 'long_sleeve', 'VD'))
        self.assertTrue(self.m('AMVDCO027SLS', 'young_men', 'VD'))

    def test_dress_shirts_stay(self):
        self.assertTrue(self.m('AMNASU820SLP', 'long_sleeve', 'NAUTICA'))
        self.assertTrue(self.m('AMDKPK007SLP', 'long_sleeve', 'DKNY'))

    def test_blazers_stay_out_of_long_sleeve(self):
        self.assertFalse(self.m('BUGBRPB05BRV', 'long_sleeve'))

    def test_prepack_rules_keep_the_inclusive_read(self):
        self.assertTrue(self.m('BRGBPJ012SLM', 'long_sleeve', prepack=True))
        self.assertTrue(self.m('BRGBPH002SSM', 'short_sleeve', prepack=True))
        self.assertTrue(self.m('BUGBRPB05BRV', 'long_sleeve', prepack=True))


if __name__ == '__main__':
    unittest.main()
