"""Synthetic tests for CostIndex.style_keys (Oct 8 2026): the manual cost keys a SKU may be filed under.

Run: python -m unittest discover -s tests -p "test_pnl_style_keys.py"
"""
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import pnl_engine as E  # noqa: E402
from tests.test_pnl_engine import COSTBOOK  # noqa: E402


class StyleKeys(unittest.TestCase):
    def test_modern_code_keys(self):
        self.assertEqual(E.CostIndex.style_keys('ROQAQF101SLS-15-32', 'ROQAQF101SLS'),
                         ('ROQAQF101SLS', 'ROQAQF101SLS-15', ))
        self.assertEqual(E.CostIndex.style_keys(None, 'ROQAQF101SLS'), ('ROQAQF101SLS',))

    def test_legacy_dash_code_keys(self):
        # The history base keeps the dash parts; each shorter prefix follows; the base comes last.
        self.assertEqual(E.CostIndex.style_keys('ZZQA123-RED-M', 'ZZQA123'),
                         ('ZZQA123-RED', 'ZZQA123'))
        self.assertEqual(E.CostIndex.style_keys('4700-BLACK 2XL', '4700'), ('4700-BLACK', '4700'))

    def test_colour_letter_suffix(self):
        # A legacy code ending in one letter after its digits also tries the plain code.
        self.assertEqual(E.CostIndex.style_keys('SSQQNP001C', 'SSQQNP001C'), ('SSQQNP001C', 'SSQQNP001'))
        self.assertEqual(E.CostIndex.style_keys('SSQQNP001C-OS', 'SSQQNP001C'),
                         ('SSQQNP001C-OS', 'SSQQNP001C', 'SSQQNP001'))
        # Not for a code that ends in a digit, a modern code, or a two-letter code.
        self.assertEqual(E.CostIndex.style_keys('CTQ01', 'CTQ01'), ('CTQ01',))
        self.assertNotIn('ROQAQF101SL', E.CostIndex.style_keys('ROQAQF101SLS', 'ROQAQF101SLS'))

    def test_override_reaches_the_stock_sku(self):
        ov = [{'id': 'j1', 'scope': 'style', 'key': {'style': 'SSQQNP001'}, 'fobU': 3.3333, 'reason': 'synthetic',
               'at': '2026-01-01T00:00:00Z'}]
        ci = E.CostIndex(COSTBOOK, {}, ov, [], today='2026-03-02')
        hit = ci.raw('SSQQNP001C', 'UNKNOWN', full='SSQQNP001C')
        self.assertEqual((hit['level'], hit['price']), ('L0', 3.3333))
        miss = ci.raw('SSQQNP001C', 'UNKNOWN')          # without the SKU the base alone does not match
        self.assertNotEqual(miss['level'], 'L0')


if __name__ == '__main__':
    unittest.main()
