"""Proposed-styles pitch decks (David, Sep 24 2026).

A pitch deck draws style numbers that do NOT exist in inventory, production
or bookings; the tool must keep that world strictly apart from real decks:
existing styles bounce back, invalid numbers are named, no card carries a
quantity, and the mode refuses to mix with the normal filters.
All synthetic: Z-prefixed serials on real code dictionaries, offline.
"""
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _support import load_app


class ProposedCardsTests(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.app = load_app()

    def setUp(self):
        self.app._inventory['items'] = [{'sku': 'RODKPK001SLS', 'jtw': 5}]
        self.addCleanup(self.app._inventory.__setitem__, 'items', [])

    def cards(self, raw):
        return self.app._pres_proposed_cards(raw)

    def test_a_new_style_number_decodes_through_the_dictionaries(self):
        cards, invalid, already = self.cards(['MWDKPK998SLS'])
        self.assertEqual([], invalid)
        self.assertEqual([], already)
        self.assertEqual(1, len(cards))
        cd = cards[0]
        self.assertEqual('MWDKPK998SLS', cd['sku'])
        self.assertEqual('PROPOSED', cd['label'])
        self.assertEqual(('Not in inventory', 'arr'), cd['chip'])
        self.assertEqual(0, cd['number'], 'a pitch card must never carry units')
        self.assertIn('Knit', cd['fab'])

    def test_an_existing_style_is_bounced_never_pitched(self):
        cards, invalid, already = self.cards(['RODKPK001SLS', 'MWDKPK998SLS'])
        self.assertEqual(['RODKPK001SLS'], already)
        self.assertEqual(['MWDKPK998SLS'], [c['sku'] for c in cards])

    def test_garbage_and_unknown_codes_are_named(self):
        cards, invalid, already = self.cards(['NOTASTYLE', 'MWDKQQ998SLS', 'MWDKPK998QQS'])
        self.assertEqual([], cards)
        reasons = {i['style']: i['reason'] for i in invalid}
        self.assertIn('pattern', reasons['NOTASTYLE'])
        self.assertIn('unknown fabric code QQ', reasons['MWDKQQ998SLS'])
        self.assertIn('unknown fit code QQ', reasons['MWDKPK998QQS'])

    def test_a_color_override_wins(self):
        cards, _, _ = self.cards(['MWDKPK998SLS | Glacier Blue Solid'])
        self.assertEqual('Glacier Blue Solid', cards[0]['color'])
        cards, _, _ = self.cards([{'style': 'MWDKPK997SLS', 'color': 'Bone Solid'}])
        self.assertEqual('Bone Solid', cards[0]['color'])

    def test_the_mode_refuses_to_mix_with_normal_filters(self):
        out = self.app._pres_proposed_build({'proposed_styles': ['MWDKPK998SLS'],
                                             'skus': ['RODKPK001SLS']}, 8)
        self.assertIn('pitch deck of its own', out['error'])

    def test_all_existing_gives_the_normal_deck_hint(self):
        out = self.app._pres_proposed_build({'proposed_styles': ['RODKPK001SLS']}, 8)
        self.assertEqual(['RODKPK001SLS'], out['already_exist'])
        self.assertIn('normal filters', out['hint'])


if __name__ == '__main__':
    unittest.main()
