"""Pants, blazers and vests keep their brand on decks (Oct 27 2026).

A P## / B## / V## serial is a modern style number, but _APO_MODERN_RE only
takes 3-digit serials, so the DD's Discounts open-order deck put two USPA
pants (DDUSDPP10SRS, DDUSDPP01SRS) on an OTHER page with no logo or colour.
Legacy style numbers must still never decode by position.
All offline; open-order lines are synthetic (ZZ customer, made-up PO).
"""
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _support import load_app


class PantsBrandTests(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.app = load_app()

    def brand(self, sku, feed=''):
        return self.app._pres_card_brand(sku, feed)

    def test_letter_serials_decode_to_their_brand(self):
        for sku, want in (('DDUSDPP10SRS', 'USPA'), ('WMUSAWP06SRS', 'USPA'), ('BUVDBAP12SHP', 'VD'),
                          ('HPNATBP01SRP', 'NAUTICA'), ('TJGBAWB07BRV', 'BEENE'), ('BUGBRPV01SLS', 'BEENE')):
            self.assertEqual(want, self.brand(sku), sku)

    def test_three_digit_serials_are_unchanged(self):
        self.assertEqual('CHAPS', self.brand('ROCHPT604SLD'))
        self.assertEqual('DKNY', self.brand('MWDKPK001SLS'))
        self.assertEqual('NAUTICA', self.brand('DDNATS986SLY'))

    def test_legacy_and_unknown_codes_never_decode_by_position(self):
        self.assertEqual('LUCKY', self.brand('LUCK2221SL'))
        self.assertEqual('OTHER', self.brand('NOTASTYLE'))
        self.assertEqual('OTHER', self.brand('ZZQQDPP01SRS'), 'QQ is no brand code')

    def test_a_known_feed_brand_still_wins(self):
        self.assertEqual('NAUTICA', self.brand('DDUSDPP10SRS', 'NAUTICA'))

    def test_open_order_deck_files_pants_under_the_brand(self):
        orders = [
            {'customer': 'ZZTS', 'customerFull': 'Zed Test Store', 'orderNo': '99000001',
             'style': 'ZZUSDPP10SRS', 'openQty': 120, 'pickQty': 0,
             'startDate': '2026-11-06', 'cancelDate': '2026-11-11'},
            {'customer': 'ZZTS', 'customerFull': 'Zed Test Store', 'orderNo': '99000002',
             'style': 'ZZNATS986SLY', 'openQty': 60, 'pickQty': 0,
             'startDate': '2026-11-24', 'cancelDate': '2026-11-29'},
        ]
        saved = self.app._fetch_all_open_orders
        self.app._fetch_all_open_orders = lambda: ([dict(o) for o in orders], True)
        self.addCleanup(setattr, self.app, '_fetch_all_open_orders', saved)
        res = self.app._pres_order_cards({'customer': 'ZZTS'}, self.app._pres_filter_spec({}))
        self.assertEqual({'ZZUSDPP10SRS': 'USPA', 'ZZNATS986SLY': 'NAUTICA'},
                         {cd['sku']: cd['brand'] for cd in res['cards']})

    def test_pitch_deck_accepts_a_pants_number(self):
        self.app._inventory['items'] = []
        cards, invalid, already = self.app._pres_proposed_cards(['ZZUSDPP98SRS'])
        self.assertEqual([], invalid)
        self.assertEqual([], already)
        self.assertEqual([('ZZUSDPP98SRS', 'USPA')], [(c['sku'], c['brand']) for c in cards])


if __name__ == '__main__':
    unittest.main()
