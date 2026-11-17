"""The AI agent / MCP grouping key keeps older dashed style numbers whole.

_ai_agent_base used to cut every key at its first dash, so NAU-22-207SL, NAU-1045 and the
other NAU-* styles became one "NAU" line (also BEN, AC, LUCK, T: 200 SKUs under 25 cuts).
A size written after a space split one style by neck size (MCNMPP401SLS 15 / 16 / 17).
Colors of whole older keys are read the desktop way, never from a code row built from
their digits. Synthetic keys and data; no test touches the network (tests._support).

Run: python -B -m unittest tests.test_ai_agent_base
"""
import unittest

from tests._support import load_app


class AiAgentBase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.app = load_app()

    def test_rule(self):
        b = self.app._ai_agent_base
        cases = {
            # modern keys: unchanged, cut at the first dash
            'WMUSAWP06SRS-V-30X30': 'WMUSAWP06SRS', 'BUCHPT587SLP-V': 'BUCHPT587SLP',
            'TJNAST415SLP': 'TJNAST415SLP', 'ROUSDPP12SRS-R': 'ROUSDPP12SRS',
            'wmuspppp02shs-44x30-v': 'WMUSPPPP02SHS',
            # a size after a space is dropped first
            'MCNMPP401SLS 15-15.5 32': 'MCNMPP401SLS', 'BONAU100CF 17-17.5 32': 'BONAU100CF',
            'NW19377 L LONG': 'NW19377', 'NOVDTS056PSP M': 'NOVDTS056PSP',
            # older dashed style numbers stay whole
            'NAU-22-207SL': 'NAU-22-207SL', 'NAU-1045': 'NAU-1045', 'BEN-22-116': 'BEN-22-116',
            'AC-KN-235': 'AC-KN-235', 'T-612REG-XL17': 'T-612REG-XL17', 'NAU-22': 'NAU-22',
            'NAU-008-BLACK L LONG': 'NAU-008-BLACK', '4W-LTBL 2XL': '4W-LTBL',
            'ZY-CLNG-STR-BURG M 32/33': 'ZY-CLNG-STR-BURG', '413226-20 M': '413226-20',
            # their vendor -V twin joins them
            'CTH08-V': 'CTH08', 'LUCK-NWNV-SL-V': 'LUCK-NWNV-SL', 'NAU-22-204-V': 'NAU-22-204',
            '': '', None: '', '  ': '',
        }
        for k, want in cases.items():
            self.assertEqual(b(k), want, k)


class AiAgentAggregation(unittest.TestCase):
    NAMES = ('_style_overrides', '_apo_color_map')

    @classmethod
    def setUpClass(cls):
        cls.app = load_app()

    def setUp(self):
        a = self.app
        self.saved = {n: getattr(a, n) for n in self.NAMES}
        with a._inv_lock:
            self.saved_items = a._inventory.get('items')

    def tearDown(self):
        a = self.app
        for n, v in self.saved.items():
            setattr(a, n, v)
        with a._inv_lock:
            a._inventory['items'] = self.saved_items

    def feed(self, rows):
        with self.app._inv_lock:
            self.app._inventory['items'] = [dict({'brand_abbr': 'NAUTICA', 'jtw': 0, 'tr': 0, 'dcw': 0, 'qa': 0,
                                                  'nj': 0, 'abfi': 0, 'incoming': 0, 'total_ats': 0,
                                                  'committed': 0, 'allocated': 0}, **r) for r in rows]

    def test_old_styles_are_separate_lines_and_sizes_join(self):
        self.feed([{'sku': 'NAU-22-207SL', 'tr': 10, 'total_ats': 10},
                   {'sku': 'NAU-1045', 'tr': 5, 'total_ats': 5},
                   {'sku': 'NAU-008-BLACK L', 'nj': 3, 'total_ats': 3},
                   {'sku': 'NAU-008-BLACK M', 'nj': 4, 'total_ats': 4},
                   {'sku': 'ZZNASU931SLP-V-15', 'dcw': 7, 'total_ats': 7},
                   {'sku': 'ZZNASU931SLP', 'dcw': 1, 'total_ats': 1}])
        agg = self.app._ai_agent_agg_inventory()
        self.assertNotIn('NAU', agg)
        self.assertEqual(agg['NAU-22-207SL']['total_ats'], 10)
        self.assertEqual(agg['NAU-1045']['total_ats'], 5)
        self.assertEqual(agg['NAU-008-BLACK']['nj'], 7)
        self.assertEqual(agg['NAU-008-BLACK']['size_rows'], 2)
        self.assertEqual(agg['ZZNASU931SLP']['dcw'], 8)          # modern merge unchanged
        self.assertEqual(agg['NAU-008-BLACK']['_skus'], ['NAU-008-BLACK L', 'NAU-008-BLACK M'])

    def test_style_detail_and_search_find_the_whole_style(self):
        self.feed([{'sku': 'NAU-22-207SL', 'tr': 10, 'total_ats': 10},
                   {'sku': 'NAU-1045', 'tr': 5, 'total_ats': 5}])
        a = self.app
        a._apo_color_map = lambda: {'NAU-22-207SL': 'WHITE GRND BLACK-RED MICRO PLAID'}
        rows, _ = a._ai_agent_filter({'search': 'NAU-22-207'})
        self.assertEqual([r['style'] for r in rows], ['NAU-22-207SL'])
        d = a._ai_tool_style_detail({'style': 'NAU-22-207SL'})
        self.assertEqual(d['style'], 'NAU-22-207SL')
        self.assertEqual(d['warehouse']['tr'], 10)
        self.assertEqual([s['sku'] for s in d['size_variants']], ['NAU-22-207SL'])

    def test_colors_of_old_keys_never_come_from_their_digits(self):
        a = self.app
        a._style_overrides = {}
        cmap = {'BE_116': 'NAVY SOLID', 'NA_204': 'PINK CHECK', 'NAU-22-204-V': 'WHITE GRND LT BLUE PLAID',
                'NAU-22-207SL': 'WHITE GRND BLACK-RED MICRO PLAID', 'CH_587': 'WHITE GRND CHOCOLATE BRWN GRID CHECK'}
        a._apo_color_map = lambda: cmap
        c = a._ai_agent_color
        self.assertEqual(c('BEN-22-116', 'BEN'), '')                     # not Ben Sherman BE_116
        self.assertEqual(c('NAU-22-207SL', 'NAUTICA').lower(), 'white grnd black-red micro plaid')
        self.assertEqual(c('NAU-22-204', 'NAUTICA', ['NAU-22-204-V']).lower(), 'white grnd lt blue plaid')
        self.assertEqual(c('NAU-22-204', 'NAUTICA'), '')                 # never NA_204
        # a modern base reads exactly as _apo_style_color does
        self.assertEqual(c('BUCHPT587SLP', 'CHAPS'), a._apo_style_color('BUCHPT587SLP', 'CHAPS'))
        self.assertTrue(c('BUCHPT587SLP', 'CHAPS'))


if __name__ == '__main__':
    unittest.main()
