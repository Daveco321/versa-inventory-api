"""amazon_skus.py: David's Amazon SKU rules (AM prefix, prepack vs single size, -FBA/-FBM,
neck/sleeve to letter sizes, -V partial cartons). Synthetic style numbers only."""
import os
import sys
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

import amazon_skus as K  # noqa: E402


class SplitTests(unittest.TestCase):

    def test_spacing_and_case_are_normalised(self):
        self.assertEqual('AMZZOX975SLB-XL', K.norm_sku(' amzzox975slb - xl '))

    def test_markers_and_sizes(self):
        self.assertEqual(('AMZZSU901SLS', 'L', 'FBA'), tuple(K.split_sku('AMZZSU901SLS-L-FBA')))
        self.assertEqual(('AMZZSU901SLS', 'M', 'FBM'), tuple(K.split_sku('AMZZSU901SLS-M-FBM')))
        self.assertEqual(('AMZZSU901SLS', '', ''), tuple(K.split_sku('AMZZSU901SLS')))
        self.assertEqual(('AMZZSU901SLS', 'MT', ''), tuple(K.split_sku('AMZZSU901SLS-MT')))
        self.assertEqual(('AMZZSU901SLS', 'XXL', 'FBA'), tuple(K.split_sku('amzzsu901sls - xxl - fba')))

    def test_neck_codes_stay_as_written(self):
        self.assertEqual(('AMZZCD902RFB', '1616.53233', ''), tuple(K.split_sku('AMZZCD902RFB-1616.53233')))
        self.assertEqual(('AMZZSU903SLS', '1515.553435', ''), tuple(K.split_sku('AMZZSU903SLS-1515.553435')))

    def test_fit_codes(self):
        self.assertEqual('SL', K.fit_code('AMZZSU901SLS'))
        self.assertEqual('RF', K.fit_code('AMZZCD902RFB'))
        self.assertEqual('SS', K.fit_code('AMZZLC904SSS'))
        self.assertEqual('', K.fit_code('NOSERIAL'))


class NeckTests(unittest.TestCase):

    def test_three_ways_of_writing_mt(self):
        for code in ('1515.53435', '1515.3435', '1515.553435'):
            self.assertEqual('MT', K.neck_to_alpha(code, 'SL'), code)
        self.assertEqual('15-15.5 / 34-35', K.parse_neck('1515.553435').label)

    def test_slim_scale(self):
        expect = {'1414.53233': 'S', '1515.53233': 'M', '1515.53435': 'MT', '1616.53233': 'L',
                  '1616.53435': 'LT', '1717.53435': 'XL'}
        for code, size in expect.items():
            self.assertEqual(size, K.neck_to_alpha(code, 'SL'), code)

    def test_regular_scale_has_talls_and_no_xxlt(self):
        codes = ['1515.53233', '1515.53435', '1616.53233', '1616.53435', '1717.53233', '1717.53435', '1818.53435']
        self.assertEqual(['M', 'MT', 'L', 'LT', 'XL', 'XLT', 'XXL'], [K.neck_to_alpha(c, 'RF') for c in codes])

    def test_unknown_codes_are_not_guessed(self):
        self.assertIsNone(K.neck_to_alpha('1515.52930', 'SL'))     # unknown sleeve
        self.assertIsNone(K.neck_to_alpha('2020.53233', 'SL'))     # unknown neck
        self.assertIsNone(K.neck_to_alpha('XL', 'SL'))
        self.assertIsNone(K.alpha_size('1515.52930', 'SL'))
        self.assertEqual('LT', K.alpha_size('lt', 'SL'))


class StockTests(unittest.TestCase):

    def test_only_am_whole_cartons_are_amazon_stock(self):
        self.assertTrue(K.is_amazon_stock('AMZZSU901SLS'))
        self.assertTrue(K.is_amazon_stock('amzzsu901sls-1515.53233'))
        self.assertFalse(K.is_amazon_stock('ROZZSU901SLS'))          # a store account's goods
        self.assertFalse(K.is_amazon_stock('AMZZSU901SLS-V'))        # partial carton
        self.assertTrue(K.is_partial_carton('BUZZYD905RFD - V'))

    def test_size_order(self):
        sizes = ['XXL', 'LT', 'S', 'XL', 'MT', 'M', 'XLT', 'L', 'ODD']
        self.assertEqual(['S', 'M', 'MT', 'L', 'LT', 'XL', 'XLT', 'XXL', 'ODD'], sorted(sizes, key=K.size_rank))

    def test_alpha_index_keeps_the_exact_warehouse_row(self):
        idx = K.alpha_index(['AMZZSU903SLS-1515.553435', 'AMZZSU903SLS-1616.53233', 'AMZZSU903SLS',
                             'AMZZLC904SSS-M', 'AMZZSU903SLS-1515.52930'])
        self.assertEqual(['AMZZSU903SLS-1515.553435'], idx[('AMZZSU903SLS', 'MT')])
        self.assertEqual(['AMZZSU903SLS-1616.53233'], idx[('AMZZSU903SLS', 'L')])
        self.assertEqual(['AMZZLC904SSS-M'], idx[('AMZZLC904SSS', 'M')])
        self.assertEqual(3, len(idx))       # the base row and the unknown code are not sizes


if __name__ == '__main__':
    unittest.main()
