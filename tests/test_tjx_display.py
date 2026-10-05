"""tjx_display: the browser's TJX catalog row decoration, ported to Python.

Synthetic data only: customer codes start with Z, styles, colours, fabrics
and banner rules are made up. The module is imported on its own (never via
app.py), so nothing here touches the network."""
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import tjx_display as T  # noqa: E402

RULE_NEW_FABRIC = {'id': 'r1', 'text': 'New Fabric', 'category': 'any', 'brands': ['NAUTICA'],
                   'fabrics': ['SU'], 'fits': [], 'customers': [], 'skus': []}


def make(overrides=None, colors=None, rules=None, customers=None, inventory=None):
    return T.TjxDisplay(overrides or {}, colors or {}, rules if rules is not None else [RULE_NEW_FABRIC],
                        customers, inventory_rows=inventory)


class BrandKeyTests(unittest.TestCase):

    def test_alias_and_sku_code(self):
        d = make()
        self.assertEqual('NICOLE', d.brand_key('ZQNMSU201SLS', 'NM'))
        # KLP is not a brand key, so the SKU's code decides
        self.assertEqual('KL', d.brand_key('ZQKLSU201SLS', 'KLP'))
        # BL belongs to both BLO and BLACK; the later prefix wins
        self.assertEqual('BLACK', d.brand_key('ZQBLSU201SLS', 'XYZ'))
        self.assertEqual('NAUTICA', d.brand_key('ZQNTSU201SLS', 'XYZ'))

    def test_known_feed_brand_is_kept(self):
        self.assertEqual('DKNY', make().brand_key('ZQNASU201SLS', 'DKNY'))

    def test_special_prefixes(self):
        d = make()
        self.assertEqual('LUCKY', d.brand_key('LUCK0001', 'NAUTICA'))
        self.assertEqual('VERSA', d.brand_key('VPNASU201SLS', 'NAUTICA'))

    def test_override_brand_wins(self):
        d = make(overrides={'ZQNASU201SLS': {'brand': 'CHAPS'}, 'ZQDK-': {'brand': 'EB'}})
        self.assertEqual('CHAPS', d.brand_key('ZQNASU201SLS', 'NAUTICA'))
        self.assertEqual('EB', d.brand_key('ZQDKSU201SLS', 'DKNY'))

    def test_missing_brand_and_unknown_code(self):
        d = make()
        self.assertEqual('undefined', d.brand_key('ZQQQSU201SLS', None))
        self.assertEqual('', d.brand_key('ZQQQSU201SLS', ''))

    def test_full_name_and_order(self):
        d = make()
        self.assertEqual('Karl Lagerfeld Paris', d.brand_full('KL'))
        self.assertEqual('XYZ', d.brand_full('XYZ'))
        keys = ['ZY', 'DKNY', 'XYZ', 'NAUTICA', 'CHEROKEE', 'abc', 'DN', 'BLACK']
        self.assertEqual(['NAUTICA', 'DKNY', 'DN', 'abc', 'BLACK', 'CHEROKEE', 'XYZ', 'ZY'],
                         sorted(keys, key=d.brand_sort_key))

    def test_order_ties_put_lowercase_first(self):
        d = make()
        self.assertEqual(['zz top', 'zZ top', 'Zz Top'],
                         sorted(['Zz Top', 'zz top', 'zZ top'], key=d.brand_sort_key))


class ColorTests(unittest.TestCase):

    def test_override_then_sized_key_then_base(self):
        colors = {'ZQNASU201SLS': 'NVY SOLID', 'ZQNASU201SLS-L': 'WHT SOLID'}
        d = make(overrides={'ZQNASU202SLS': {'color': 'as typed  '}}, colors=colors)
        self.assertEqual('as typed  ', d.color_display('ZQNASU202SLS-M', 'NAUTICA'))
        self.assertEqual('White Solid', d.color_display('ZQNASU201SLS-L', 'NAUTICA'))
        self.assertEqual('Navy Solid', d.color_display('zqnasu201sls-m', 'NAUTICA'))

    def test_empty_base_override_hides_the_sized_override(self):
        d = make(overrides={'ZQNASU201SLS': {}, 'ZQNASU201SLS-L': {'color': 'Red Solid'}})
        self.assertEqual('', d.color_display('ZQNASU201SLS-L', 'NAUTICA'))

    def test_fallback_keys(self):
        k = T.style_color_fallback_keys
        self.assertEqual(['NA_201'], k('ZQNASU201SLS', 'NAUTICA'))
        self.assertEqual(['NT_201'], k('ZQNTSU201SLS', 'NAUTICA'))
        self.assertEqual(['GB_P01', 'GB_001'], k('ZQGBPPP01SRS', 'BEENE'))
        self.assertEqual(['GB_B01', 'GB_001'], k('ZQGBTBB01BRQ', 'BEENE'))
        self.assertEqual(['GB_V03', 'GB_003'], k('ZQGBRPV03SLS', 'BEENE'))
        self.assertEqual(['GB_SW_036', 'GB_036'], k('ZQGBPO036SLS', 'BEENE'))
        # equal-length digit runs: the LAST one wins
        self.assertEqual(['NA_034'], k('ZQNA12AB34SLS', 'NAUTICA'))
        self.assertEqual([], k('ZQNASUABCSLS', 'NAUTICA'))

    def test_two_part_and_abbreviations(self):
        d = make(colors={'NA_301': 'BLK GRND||WHT  STRIPE', 'NA_302': '  srnty trq   blu '})
        self.assertEqual('Black Grnd White Stripe', d.color_display('ZQNASU301SLS', 'NAUTICA'))
        self.assertEqual('Serenity Turquoise Blue', d.color_display('ZQNASU302SLS', 'NAUTICA'))
        self.assertEqual('', d.color_display('ZQNASU303SLS', 'NAUTICA'))

    def test_color_family(self):
        cases = {
            'White Solid W/ Chest Embroidery': 'White', 'Bright White Solid': 'White',
            'Black Sld': 'Black', 'French Blue Solid': 'Navy Solid', 'TONY BLUE': 'Navy Solid',
            'White Solid / Navy Grnd Geo Print': 'White', 'White Grnd W/ Blue Gingham': 'Fancy',
            'Navy Jacquard Stripe Dobby': 'Navy Solid', 'Denim Grey Solid': 'Other Solid',
            'Burgundy Chambray Solid': 'Other Solid', 'Navy Solid Check': 'Fancy',
        }
        for i, (desc, family) in enumerate(cases.items()):
            key = 'NA_%03d' % (400 + i)
            d = make(colors={key: desc})
            self.assertEqual(family, d.color_family('ZQNASU%03dSLS' % (400 + i), 'NAUTICA'), desc)
        self.assertEqual('Fancy', make().color_family('ZQNASU499SLS', 'NAUTICA'))

    def test_non_text_colour_blanks_the_family(self):
        d = make(overrides={'ZQNASU201SLS': {'color': 5}})
        self.assertEqual(5, d.color_display('ZQNASU201SLS', 'NAUTICA'))
        self.assertEqual('', d.color_family('ZQNASU201SLS', 'NAUTICA'))

    def test_js_whitespace_and_word_boundaries(self):
        self.assertEqual('navy', T.classify_color('﻿Navy Solid'))
        self.assertEqual('Café Solid', T.format_color_name('CAFé SOLID'))
        self.assertEqual('Blue2 Solid', T.format_color_name('Blue2 SOLID'))


class ColorMapTests(unittest.TestCase):

    def test_rows_like_sheet_to_json(self):
        rows = [
            ('Key', 'Brand_Prefix', 'Style_Number', 'Color_Description'),
            (' zq_001 ', None, None, 'NAVY SOLID'),
            (None, None, 'ZQNASU002SLS', 'WHITE SOLID'),
            ('ZQ_003', None, None, None),
            ('ZQ_004', None, None, '   '),
            ('ZQ_001', None, None, 'BLACK SOLID'),
            (201, None, None, 'RED SOLID'),
        ]
        cmap = T.color_map_from_rows(rows)
        self.assertEqual({'ZQ_001': 'BLACK SOLID', 'ZQNASU002SLS': 'WHITE SOLID',
                          'ZQ_004': '   ', '201': 'RED SOLID'}, cmap)

    def test_whitespace_description_blocks_the_fallback(self):
        d = make(colors={'ZQNASU201SLS': '   ', 'NA_201': 'Navy Solid'})
        self.assertEqual('', d.color_display('ZQNASU201SLS', 'NAUTICA'))
        self.assertEqual('Fancy', d.color_family('ZQNASU201SLS', 'NAUTICA'))


class FitAndFabricTests(unittest.TestCase):

    def test_fit_labels(self):
        d = make()
        self.assertEqual('Slim Fit', d.fit_label('ZQNASU201SLS'))
        self.assertEqual('Slim Fit Short Sleeve', d.fit_label('ZQNASU201SSS-L'))
        # TE is a pants code only: a shirt falls to the switch default
        self.assertEqual('Slim Fit', d.fit_label('ZQNASU201TES'))
        self.assertEqual('Big & Tall Fit / Extended Button', d.fit_label('ZQUSPPP01TES'))
        # pants keep no sleeve wording
        self.assertEqual('Slim Fit', d.fit_label('ZQUSPPP01SSS'))
        self.assertEqual('Slim Fit', d.fit_label('ZQNA175-SL'))
        self.assertEqual('Big & Tall (Von Dutch)', d.fit_label('ZQVDWB201JJ'))

    def test_fit_override_exact_then_prefix(self):
        d = make(overrides={'ZQNASU-': {'fit': 'Prefix Fit'}, 'ZQNASU201SLS': {}})
        self.assertEqual('Prefix Fit', d.fit_label('ZQNASU202SLS'))
        # an exact {} entry is truthy in JS and hides the prefix key
        self.assertEqual('Slim Fit', d.fit_label('ZQNASU201SLS'))

    def test_fabric(self):
        d = make(overrides={'ZQNAZZ201SLS': {'fabrication': 'Custom Blend'},
                            'ZQNAZZ202SLS': {'fabrication': 'Other Blend', 'fabricCode': 'QQ'},
                            'ZQ': {'fabrication': 'Short Key'}})
        self.assertEqual(('ZZ', 'Custom Blend'), d.fabric('ZQNAZZ201SLS'))
        self.assertEqual(('QQ', 'Other Blend'), d.fabric('ZQNAZZ202SLS'))
        self.assertEqual(('✎', 'Short Key'), d.fabric('ZQ'))
        self.assertEqual(('ZZ', 'ZZ'), d.fabric('ZQNAZZ203SLS'))
        self.assertEqual(('N/A', 'Standard Fabric'), d.fabric('ZQNA'))
        self.assertEqual(('PP', '100% Polyester - 150D'), d.fabric('ZQNAPP201SLS'))
        self.assertEqual(('PP', '100% Polyester Woven Dress Pant'), d.fabric('ZQNAPPP01SRS'))
        self.assertEqual(('YD', '50% Microfiber / 50% Polyester Yarn Dye'), d.fabric('ZQCHYD201SLS'))
        self.assertEqual(('YD', '77% Poly / 20% Cotton / 3% Spandex'), d.fabric('ZQBEYD201SLS'))
        self.assertEqual(('CD', '65% Polyester / 35% Cotton'), d.fabric('ZQUSCD201SLS'))
        self.assertEqual(('CX', '97% Cotton / 3% Polyester'), d.fabric('ZQNACX201SLS'))

    def test_format_fabric_name(self):
        f = T.format_fabric_name
        self.assertEqual('50% Viscose 50% Polyester', f('50% Viscose 50%Polyester'))
        self.assertEqual('95% Poly / 5% Spandex - Perforated', f('95% poly / 5%spandex ---Perforated'))
        self.assertEqual('CVC TC GSM', f('cvc tc gsm'))
        self.assertEqual('Rayon made from Bamboo', f('Rayon made from Bamboo'))
        self.assertEqual('Made with Cotton', f('made with cotton'))
        self.assertEqual('AB', f('AB'))


class CategoryTests(unittest.TestCase):

    def test_export_fields(self):
        d = make(overrides={'ZQNASU-': {'sizePack': {}}})
        self.assertEqual('long_sleeve', d.export_category('ZQNASU201SLS', 'NAUTICA'))
        self.assertEqual('short_sleeve', d.export_category('ZQNASU201SSS', 'NAUTICA'))
        self.assertEqual('big_tall', d.export_category('ZQNASU201BTS', 'NAUTICA'))
        self.assertEqual('pants', d.export_category('ZQNAPPP01SRS', 'NAUTICA'))
        self.assertEqual('sportswear', d.export_category('ZQNAPO201SLS', 'NAUTICA'))
        self.assertEqual('young_men', d.export_category('ZQNAKN201SLS', 'NAUTICA'))
        self.assertEqual('blazers', d.export_category('ZQNATBB01BRQ', 'NAUTICA'))
        self.assertEqual('blazers', d.export_category('ZQNARPV01SLS', 'NAUTICA'))
        self.assertEqual('accessories', d.export_category('CTHZQ201', 'CHAPS'))
        self.assertEqual('WB', d.export_fit('ZQVDWB201JJ'))
        self.assertEqual('ZQ', d.export_customer('zqnasu201sls'))
        self.assertEqual({}, d.size_pack_override('ZQNASU201SLS'))
        self.assertIsNone(d.size_pack_override('ZQDKSU201SLS'))

    def test_catalog_customer_wins(self):
        self.assertEqual('ZR', make(customers=['ZR']).export_customer('ZQNASU201SLS'))

    def test_category_without_brand_reads_the_feed(self):
        d = make(inventory=[{'sku': 'CTHZQ201-L', 'brand': 'CHAPS', 'brand_abbr': ''}])
        self.assertEqual('accessories', d.export_category('CTHZQ201', ''))
        self.assertEqual('long_sleeve', make().export_category('CTHZQ201', ''))

    def test_override_fit_decides_the_sleeve(self):
        d = make(overrides={'ZQNASU201SLS': {'fit': 'Classic Short  Sleeve'}})
        self.assertEqual('short_sleeve', d.export_category('ZQNASU201SLS', 'NAUTICA'))


class NewFabricTests(unittest.TestCase):

    def test_brand_and_fabric_rule(self):
        d = make()
        self.assertEqual('YES', d.new_fabric('ZQNASU201SLS', 'NAUTICA'))
        self.assertEqual('', d.new_fabric('ZQNAOX201SLS', 'NAUTICA'))
        self.assertEqual('', d.new_fabric('ZQDKSU201SLS', 'DKNY'))

    def test_sku_rules_and_text_match(self):
        rules = [
            {'id': 'a', 'text': ' new FABRIC ', 'skus': ['zqnaox2'], 'brands': ['DKNY']},
            {'id': 'b', 'text': 'New Fabric', 'alsoSkus': ['ZQDKOX201SLS'], 'brands': ['CHAPS']},
            {'id': 'c', 'text': 'Not New Fabric', 'brands': ['EB']},
        ]
        d = make(rules=rules)
        self.assertEqual('YES', d.new_fabric('ZQNAOX201SLS-L', 'NAUTICA'))   # sku prefix, brand ignored
        self.assertEqual('YES', d.new_fabric('ZQDKOX201SLS', 'DKNY'))        # alsoSkus
        self.assertEqual('', d.new_fabric('ZQDKOX202SLS', 'DKNY'))
        self.assertEqual('', d.new_fabric('ZQEBOX201SLS', 'EB'))

    def test_customer_fit_and_category_dimensions(self):
        rules = [
            {'id': 'a', 'text': 'New Fabric', 'customers': ['ZR'], 'brands': ['DKNY']},
            {'id': 'b', 'text': 'New Fabric', 'customer': ' zq ', 'fit': 'SS', 'brands': ['EB']},
            {'id': 'c', 'text': 'New Fabric', 'category': 'button_down', 'brands': ['CHAPS']},
        ]
        self.assertEqual('', make(rules=rules).new_fabric('ZQDKSU201SLS', 'DKNY'))
        self.assertEqual('YES', make(rules=rules, customers=['zr']).new_fabric('ZQDKSU201SLS', 'DKNY'))
        self.assertEqual('YES', make(rules=rules).new_fabric('ZQEBSU201SSS', 'EB'))
        self.assertEqual('', make(rules=rules).new_fabric('ZQEBSU201SLS', 'EB'))
        self.assertEqual('YES', make(rules=rules).new_fabric('ZQCHOX201SLB', 'CHAPS'))
        self.assertEqual('', make(rules=rules).new_fabric('ZQCHOX201SLS', 'CHAPS'))

    def test_one_bad_rule_blanks_every_row(self):
        for bad in (None, {'id': 'x', 'text': 'x', 'skus': 'ZQ'}, {'id': 'y', 'text': 'x', 'customers': [5]}):
            d = make(rules=[RULE_NEW_FABRIC, bad])
            self.assertEqual('', d.new_fabric('ZQNASU201SLS', 'NAUTICA'), bad)


class BannerMergeTests(unittest.TestCase):

    def test_seeds_when_empty(self):
        ids = [r['id'] for r in T.merge_banner_rules([])]
        self.assertEqual([r['id'] for r in T.BANNER_RULES_SEED], ids)
        self.assertEqual(ids, [r['id'] for r in T.merge_banner_rules({'rules': 'not a list'})])

    def test_builtin_appended_once(self):
        merged = T.merge_banner_rules([RULE_NEW_FABRIC])
        self.assertEqual(['r1', 'seed-bd'], [r['id'] for r in merged])
        again = T.merge_banner_rules(merged)
        self.assertEqual(['r1', 'seed-bd'], [r['id'] for r in again])


if __name__ == '__main__':
    unittest.main()
