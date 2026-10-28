"""Category filter fixes (David, Oct 28 2026): JCPenney's Shaq shirt & tie sets under
Big & Tall, Von Dutch jewelry under Accessories, every other Von Dutch style under
Sportswear and Young Men only, BJNASSP27 under Short Sleeve, and old keys under Long or
Short Sleeve where the data names the sleeve. Size packs, ribbons and photos keep their
rules (the for_prepack path and the structural read). Synthetic and live style numbers,
offline. Run from tests/: python -B -m unittest test_filter_categories
"""
import unittest

from _support import load_app

CATS = ('long_sleeve', 'short_sleeve', 'big_tall', 'pants', 'sportswear', 'blazers', 'young_men',
        'accessories')


class FilterCategories(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.app = load_app()

    def setUp(self):
        self._saved = dict(self.app._style_overrides)
        self.app._style_overrides.clear()

    def tearDown(self):
        self.app._style_overrides.clear()
        self.app._style_overrides.update(self._saved)

    def shows(self, sku, brand=''):
        a = self.app
        got = {c for c in CATS if a._py_matches_category(sku, brand, c)}
        self.assertEqual(got, a._py_filter_categories(sku, brand), sku)
        return got

    def test_shaq_sets_are_big_and_tall(self):
        for sku in ('JTSHST002BBS', 'JTSHST002TTS', 'JTSHST049BTP'):
            self.assertEqual(self.shows(sku, 'SHAQ'), {'big_tall', 'long_sleeve'}, sku)
            self.assertEqual(self.app._py_get_item_category(sku, 'SHAQ'), 'big_tall')
        # real Shaq ties keep Accessories: the T is in the third place
        for sku in ('JCTSHPP031', 'JCTSHAQA02'):
            self.assertEqual(self.shows(sku, 'SHAQ'), {'accessories'}, sku)

    def test_von_dutch_jewelry_is_accessories(self):
        for sku in ('SSVDNP001C', 'BVDNP007R', 'BVDR002', 'SSVDR001-SZ10', 'STVDR003'):
            self.assertEqual(self.shows(sku, 'VD'), {'accessories'}, sku)

    def test_von_dutch_is_sportswear_and_young_men_only(self):
        for sku in ('TTVDTS075PSP', 'CIVDTS128SSS', 'ROVDTS120WBK', 'NOVDTS056PSP M', 'NOVDTS056PSP L',
                    'TMVDBAP34SCP', 'ROVDBA008SCP', 'BUVDBAP18BHS', 'AMVDSL007SLC', 'PMDVBA008SCP'):
            self.assertEqual(self.shows(sku, 'VD'), {'sportswear', 'young_men'}, sku)

    def test_old_keys_the_data_names(self):
        for sku, brand in (('WACHA18', 'CHAPS'), ('WACHA19', 'CHAPS'), ('CU-NAUSP272', 'NAUTICA'),
                           ('BJNALSP27-1', 'NAUTICA'), ('WACHAW309RF', 'CHAPS'), ('MATAYA08SL', 'TAYION'),
                           ('LUCK-22-170-SL', 'LUCKY'), ('TJCHPK07SLBD', 'CHAPS'), ('ROUSPK12SCSL', 'USPA'),
                           ('HPNALT888RFPD', 'NAUTICA'), ('BONAU001CF 16-16.5 32', 'NAUTICA'),
                           ('LUCK-NWNV-SL-V', 'LUCKY')):
            self.assertEqual(self.shows(sku, brand), {'long_sleeve'}, sku)
        for sku, brand in (('BJNASSP27-4', 'NAUTICA'), ('1661337', 'NAUTICA'), ('1661338', 'NAUTICA'),
                           ('BONAU001SLSS', 'NAUTICA')):
            self.assertEqual(self.shows(sku, brand), {'short_sleeve'}, sku)

    def test_old_keys_the_data_does_not_name_stay_out(self):
        for sku, brand in (('NAU-008-BLACK XL', 'NAUTICA'), ('AC-KN-235', 'AMERICA'), ('T-SLD-FLWR', 'TAYION'),
                           ('AMNASU201WH1', 'NAUTICA'), ('RODKPK144SPL', 'DKNY'), ('1961027', 'EB')):
            self.assertEqual(self.shows(sku, brand), set(), sku)

    def test_override_naming_the_sleeve(self):
        self.app._style_overrides['ZZNAXX12'] = {'fit': 'Short Sleeve'}
        self.assertEqual(self.shows('ZZNAXX12', 'NAUTICA'), {'short_sleeve'})

    def test_size_after_a_space_follows_its_style(self):
        self.assertEqual(self.shows('TMDKPK001SLS 15-15.5 32', 'DKNY'), {'long_sleeve'})
        self.assertEqual(self.shows('TMDKPK001SLS', 'DKNY'), {'long_sleeve'})

    def test_untouched_styles_keep_their_filters(self):
        for sku, brand, want in (('ROEBFT007SLS', 'EB', {'long_sleeve'}),
                                 ('AMDKPK001SSS', 'DKNY', {'short_sleeve'}),
                                 ('HANASU201BBB', 'NAUTICA', {'long_sleeve', 'big_tall'}),
                                 ('BRGBPO001SSM', 'BEENE', {'sportswear', 'young_men'}),
                                 ('AMUSDPP01SRS', 'USPA', {'pants'}),
                                 ('BUGBRPB05BRV', 'BEENE', {'blazers'}),
                                 ('BUGBRPV01SLS', 'BEENE', {'blazers'}),
                                 ('CTH01', 'CHAPS', {'accessories'}),
                                 ('JCSHTP009BBS', 'SHAQ', {'long_sleeve', 'big_tall'})):
            self.assertEqual(self.shows(sku, brand), want, sku)

    def test_size_packs_and_ribbons_keep_the_inclusive_read(self):
        m = self.app._py_matches_category
        self.assertTrue(m('TMVDBAP34SCP', 'VD', 'pants', for_prepack=True))        # Dress Pants pack + PANTS ribbon
        self.assertTrue(m('CIVDTS128SSS', 'VD', 'short_sleeve', for_prepack=True))  # SHORT SLEEVE ribbon
        self.assertTrue(m('AMVDSL007SLC', 'VD', 'long_sleeve', for_prepack=True))   # Von Dutch packs
        self.assertFalse(m('WACHA18', 'CHAPS', 'long_sleeve', for_prepack=True))    # old keys: packs unchanged
        self.assertFalse(m('BJNASSP27-4', 'NAUTICA', 'short_sleeve', for_prepack=True))

    def test_photos_keep_the_structural_read(self):
        f = self.app._photo_no_serial_fallback
        self.assertFalse(f('TTVDTS075PSP', 'VD'))      # a Von Dutch woven still uses its serial photo keys
        self.assertTrue(f('BRGBPO001SSM', 'BEENE'))     # polos never do
        self.assertTrue(f('TMVDBAP34SCP', 'VD'))        # bottoms never do


if __name__ == '__main__':
    unittest.main()
