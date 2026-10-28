"""Line sheets by category by brand, sportswear by sleeve, and Tanzine out of the
blues (David, Oct 28 2026).

"Update the AI email that creates line sheets to know to create line sheets by
category by brand. It should also know to split short sleeve and long sleeve
sportswear from the style number (if this is specifically requested) by looking at
the last 3 letters: SS(last letter) or SL(last letter)." And: the DKNY 104 knit
"Tanzine Solid" is a tan shirt, so it must not sit under All Shades of Blue.

Synthetic and offline: made-up serials (9xx), the workbook builder and S3 replaced
by recorders, as in test_line_sheet_tabs.py.
Run from tests/: python -B -m unittest test_line_sheet_categories
"""
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _support import load_app  # noqa: E402


def _row(style, brand, wh=0, inc=0):
    return {'style': style, 'brand_abbr': brand, 'total_warehouse': wh, 'incoming': inc,
            'total_ats': wh + inc, 'jtw': wh, 'tr': 0, 'dcw': 0, 'qa': 0,
            'nj': 0, 'abfi': 0, 'committed': 0, 'allocated': 0}


AGG = {r['style']: r for r in [
    _row('TJNAZZ901SLS', 'NAUTICA', wh=900, inc=500),   # dress shirt, long sleeve
    _row('TJNAZZ902SSS', 'NAUTICA', wh=100),            # dress shirt, short sleeve
    _row('AMNADPP01SRS', 'NAUTICA', wh=50),             # dress pants (P## serial)
    _row('ROGBPO901SSM', 'BEENE', wh=1000),             # polo, SS
    _row('ROGBPO902SLM', 'BEENE', wh=800),              # polo, SL
    _row('BUGBZZB01SLS', 'BEENE', wh=60),               # blazer
    _row('BUGBZZV01SLS', 'BEENE', wh=40),               # vest
    _row('ROGBZZ903SLS', 'BEENE', wh=30),               # dress shirt
    _row('ROVDBA908SCP', 'VD', wh=300),                 # Von Dutch pinstripe pants
    _row('TTVDTS909PSP', 'VD', wh=200),                 # Von Dutch woven, PS fit: no sleeve in the number
    _row('BUVDSH910TBE', 'VD', wh=150),                 # Von Dutch Big & Tall short sleeve
    _row('ROVDSL911BTC', 'VD', wh=100),                 # Von Dutch Big & Tall long sleeve
    _row('TMKLZZ913SLS', 'KLP', wh=20),                 # Karl Lagerfeld Paris: a long tab name
]}


class Sleeve(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.app = load_app()

    def test_the_fit_code_before_the_last_letter_names_the_sleeve(self):
        s = self.app._py_sleeve_from_style
        self.assertEqual(s('ROGBPO901SSM'), 'short')     # SS + collar
        self.assertEqual(s('ROGBPO902SLM'), 'long')      # SL + collar
        self.assertEqual(s('BUVDSH910TBE'), 'short')     # Big & Tall short sleeve
        self.assertEqual(s('ROVDSL911BTC'), 'long')      # Big & Tall long sleeve
        self.assertEqual(s('ROVDTS120WBK'), 'long')
        self.assertEqual(s('TJNAZZ902SSS-L'), 'short')   # a size row reads its style

    def test_no_sleeve_is_ever_guessed(self):
        s = self.app._py_sleeve_from_style
        for sku in ('ROVDBA908SCP', 'BUVDBAP14SHS', 'AMNADPP01SRS',   # bottoms
                    'BUGBZZB01SLS', 'BUGBZZV01SLS',                    # blazer, vest
                    'TTVDTS909PSP', 'NOVDTS063PSS',                    # PS fit (not "SS")
                    'SSVDNP001C', ''):
            self.assertEqual(s(sku), '', sku)

    def test_an_override_naming_the_sleeve_decides_an_unreadable_fit(self):
        a = self.app
        saved = a._style_overrides
        try:
            a._style_overrides = {'TTVDTS909PSP': {'fit': 'Relaxed Fit Short Sleeve'}}
            self.assertEqual(a._py_sleeve_from_style('TTVDTS909PSP'), 'short')
        finally:
            a._style_overrides = saved

    def test_old_keys_keep_the_sleeve_their_data_names(self):
        s = self.app._py_sleeve_from_style
        self.assertEqual(s('BJNASSP27-4'), 'short')
        self.assertEqual(s('WACHA18-2'), 'long')

    def test_a_key_with_no_collar_letter_reads_the_fit_at_its_end(self):
        # as the Long Sleeve category filter reads them (_py_legacy_sleeve)
        s = self.app._py_sleeve_from_style
        for sku in ('ROJNAW051SL', 'BONASU352SL', 'JCSHAW010BT', 'WACHAW283RF', 'TJCHPK07SLBD'):
            self.assertEqual(s(sku), 'long', sku)
        self.assertEqual(s('RODKPK144SPL'), '')

    def test_both_copies_of_the_rule_agree_on_the_page(self):
        # the desktop sleeveFromStyle carries the same branches in the same order
        src = open(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'app.py'),
                   encoding='utf-8').read()
        self.assertIn("if not _py_is_legacy_key(s):", src)
        self.assertIn("if _py_is_vd_apparel(s):", src)


class Categories(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.app = load_app()

    def setUp(self):
        self._saved = self.app._ai_agent_agg_inventory
        self.app._ai_agent_agg_inventory = lambda: AGG

    def tearDown(self):
        self.app._ai_agent_agg_inventory = self._saved

    def _styles(self, **params):
        rows, _ = self.app._ai_agent_filter(params)
        return {r['style'] for r in rows}

    def test_sportswear_by_sleeve(self):
        self.assertEqual(self._styles(category='sportswear_short_sleeve'), {'ROGBPO901SSM', 'BUVDSH910TBE'})
        self.assertEqual(self._styles(category='sportswear_long_sleeve'), {'ROGBPO902SLM', 'ROVDSL911BTC'})
        self.assertEqual(self._styles(category='sportswear_ss'), {'ROGBPO901SSM', 'BUVDSH910TBE'})

    def test_the_category_column_names_filter_too(self):
        self.assertEqual(self._styles(category='dress_shirt'),
                         {'TJNAZZ901SLS', 'TJNAZZ902SSS', 'ROGBZZ903SLS', 'TMKLZZ913SLS'})
        self.assertEqual(self._styles(category='dress_pants'), {'AMNADPP01SRS'})

    def test_the_platform_filters_did_not_move(self):
        # short_sleeve is still DRESS SHIRTS only; a short-sleeve polo is Sportswear
        self.assertEqual(self._styles(category='short_sleeve'), {'TJNAZZ902SSS'})
        self.assertIn('ROGBPO901SSM', self._styles(category='sportswear'))

    def test_prepack_rules_never_read_the_new_names(self):
        m = self.app._py_matches_category
        self.assertTrue(m('ROGBPO901SSM', 'BEENE', 'sportswear_short_sleeve'))
        self.assertFalse(m('ROGBPO901SSM', 'BEENE', 'sportswear_short_sleeve', for_prepack=True))


class CategoryTabs(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.app = load_app()

    def setUp(self):
        a = self.app
        self._saved = {k: getattr(a, k) for k in (
            '_ai_agent_agg_inventory', '_ai_agent_enrich', 'build_multi_brand_excel', 'get_s3',
            '_production_data', '_fresh_prepack_defaults', '_hidden_landing_maps', '_pres_stock_cards')}
        self.built = {}
        a._ai_agent_agg_inventory = lambda: AGG

        def _num(source, r):
            return r['total_warehouse'] if source == 'warehouse' else r['incoming']
        a._pres_stock_cards = lambda source, cv, spec, params: {
            'cards': [{'sku': s, 'base': s, 'number': _num(source, r)} for s, r in AGG.items()
                      if _num(source, r) > 0 and (not spec['sku_set'] or s in spec['sku_set'])],
            'stats': {'orders_feed_ok': True}}
        a._ai_agent_enrich = lambda r: ('White Solid', 'Slim Fit', 'Poly')
        a._production_data = []
        a._fresh_prepack_defaults = lambda: None
        a._hidden_landing_maps = lambda: ({}, {})

        def fake_excel(tabs, *args, **kw):
            self.built['tabs'] = tabs
            return b'xlsx'
        a.build_multi_brand_excel = fake_excel

        class S3:
            def put_object(_s, **kw):
                self.built['key'] = kw['Key']
        a.get_s3 = lambda: S3()

    def tearDown(self):
        for k, v in self._saved.items():
            setattr(self.app, k, v)

    def _tabs(self):
        return [(t['tab_name'], [it['sku'] for it in t['items']]) for t in self.built['tabs']]

    def _build(self, *tabs):
        out = self.app._ai_tool_build_line_sheet({'tabs': list(tabs)})
        self.assertNotIn('error', out)
        return out

    def test_by_category_by_brand_is_one_tab_spec(self):
        bt = self.app._line_sheet_brand_title
        beene, naut, vd = bt('BEENE'), bt('NAUTICA'), bt('VD')
        self._build({'stock': 'warehouse', 'split_by_brand': True, 'split_by_category': True})
        self.assertEqual(self._tabs(), [
            (f'{beene} Dress Shirts', ['ROGBZZ903SLS']),
            (f'{beene} Sportswear', ['ROGBPO901SSM', 'ROGBPO902SLM']),
            (f'{beene} Blazers', ['BUGBZZB01SLS']),
            (f'{beene} Vests', ['BUGBZZV01SLS']),
            (f'{naut} Dress Shirts', ['TJNAZZ901SLS', 'TJNAZZ902SSS']),
            (f'{naut} Dress Pants', ['AMNADPP01SRS']),
            (f'{vd} Sportswear', ['ROVDBA908SCP', 'TTVDTS909PSP', 'BUVDSH910TBE', 'ROVDSL911BTC']),
            ('Karl Lagerfeld Dress Shirts', ['TMKLZZ913SLS']),   # 'Karl Lagerfeld Paris Dress Shirts' is 33
        ])

    def test_split_sleeve_splits_sportswear_only_when_asked(self):
        vd = self.app._line_sheet_brand_title('VD')
        self._build({'brands': ['VD'], 'stock': 'warehouse', 'split_sleeve': ['sportswear']})
        self.assertEqual(self._tabs(), [
            (f'{vd} Sportswear SS', ['BUVDSH910TBE']),
            (f'{vd} Sportswear LS', ['ROVDSL911BTC']),
            (f'{vd} Sportswear Bottoms', ['ROVDBA908SCP']),
            (f'{vd} Sportswear Other', ['TTVDTS909PSP']),
        ])

    def test_dress_shirts_stay_whole_unless_asked(self):
        naut = self.app._line_sheet_brand_title('NAUTICA')
        self._build({'brands': ['NAUTICA'], 'stock': 'warehouse', 'split_sleeve': ['sportswear']})
        self.assertEqual([n for n, _ in self._tabs()], [f'{naut} Dress Shirts', f'{naut} Dress Pants'])
        self._build({'brands': ['NAUTICA'], 'stock': 'warehouse', 'split_sleeve': ['sportswear', 'dress_shirt']})
        self.assertEqual(self._tabs()[:2], [(f'{naut} Dress Shirts SS', ['TJNAZZ902SSS']),
                                            (f'{naut} Dress Shirts LS', ['TJNAZZ901SLS'])])

    def test_true_means_sportswear(self):
        self.assertEqual(self.app._line_sheet_sleeve_labels({'split_sleeve': True}), {'Sportswear'})
        self.assertEqual(self.app._line_sheet_sleeve_labels({'split_sleeve': 'Dress Shirt'}), {'Dress Shirt'})
        self.assertEqual(self.app._line_sheet_sleeve_labels({}), set())

    def test_the_parent_category_still_applies(self):
        self._build({'stock': 'warehouse', 'category': 'sportswear', 'split_by_category': True})
        self.assertEqual([n for n, _ in self._tabs()], ['Sportswear'])
        self.assertEqual(len(self._tabs()[0][1]), 6)

    def test_two_category_splits_keep_their_titles(self):
        naut = self.app._line_sheet_brand_title('NAUTICA')
        self._build({'title': 'WH', 'brands': ['NAUTICA'], 'stock': 'warehouse', 'split_by_category': True},
                    {'title': 'OS', 'brands': ['NAUTICA'], 'stock': 'overseas', 'split_by_category': True})
        self.assertEqual([n for n, _ in self._tabs()],
                         [f'{naut} Dress Shirts - WH', f'{naut} Dress Pants - WH', f'{naut} Dress Shirts - OS'])

    def test_a_curated_tab_splits_in_the_callers_order(self):
        out = self._build({'skus': ['ROGBPO902SLM', 'TJNAZZ901SLS', 'ROGBPO901SSM', 'NOPE999'],
                           'stock': 'warehouse', 'split_sleeve': ['sportswear']})
        self.assertEqual(self._tabs(), [('Dress Shirts', ['TJNAZZ901SLS']),
                                        ('Sportswear SS', ['ROGBPO901SSM']),
                                        ('Sportswear LS', ['ROGBPO902SLM'])])
        missing = next(e for e in out['tabs'] if e.get('tab') == 'Not found')
        self.assertEqual(missing['skus_not_found'], ['NOPE999'])

    def test_brand_splits_alone_are_unchanged(self):
        self._build({'stock': 'warehouse', 'split_by_brand': True})
        bt = self.app._line_sheet_brand_title
        self.assertEqual([n for n, _ in self._tabs()],
                         [bt('BEENE'), bt('NAUTICA'), bt('VD'), 'Karl Lagerfeld Paris'])

    def test_names_fit_excel(self):
        f = self.app._line_sheet_category_name
        self.assertEqual(f('Karl Lagerfeld Paris', 'Dress Shirts'), 'Karl Lagerfeld Dress Shirts')
        self.assertEqual(f('Nautica', 'Dress Shirts', 'Warehouse ATS for TJX'), 'Nautica Dress Shirts - Warehous')
        self.assertTrue(len(f('Karl Lagerfeld Paris', 'Shirt & Tie Sets', 'Overseas')) <= 31)


class Tanzine(unittest.TestCase):
    """The DKNY 104 knit 'Tanzine Solid' is tan: Other Solids, never All Shades of Blue."""
    @classmethod
    def setUpClass(cls):
        cls.app = load_app()
        sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        import tjx_display
        import pnl_engine
        cls.tjx, cls.pnl = tjx_display, pnl_engine

    def test_every_api_copy_reads_tanzine_as_another_solid(self):
        self.assertEqual(self.app._apo_classify_color('Tanzine Solid', 'DKNY'), 'other_solids')
        self.assertEqual(self.tjx.classify_color('Tanzine Solid'), 'other_solids')
        self.assertEqual(self.pnl.classify_colour('Tanzine Solid'), 'other_solids')

    def test_the_real_blues_stay_blue(self):
        for c in ('Navy Solid', 'Serenity Blue Solid', 'Teal Solid', 'Cobalt Solid'):
            self.assertEqual(self.app._apo_classify_color(c, 'DKNY'), 'navy', c)
            self.assertEqual(self.tjx.classify_color(c), 'navy', c)
            self.assertEqual(self.pnl.classify_colour(c), 'navy', c)


class Schema(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.app = load_app()

    def test_the_bot_is_told(self):
        a = self.app
        tool = next(t for t in a._AI_AGENT_TOOLS if t['name'] == 'build_line_sheet')
        props = tool['input_schema']['properties']['tabs']['items']['properties']
        self.assertIn('split_by_category', props)
        self.assertEqual(props['split_sleeve']['items']['enum'], ['sportswear', 'dress_shirt'])
        self.assertIn('Up to 60 tabs', tool['description'])
        self.assertEqual(a._LINE_SHEET_MAX_TABS, 60)
        q = next(t for t in a._AI_AGENT_TOOLS if t['name'] == 'query_inventory')
        self.assertIn('sportswear_short_sleeve', q['description'])
        self.assertNotIn('tanzine', q['description'].lower())
        self.assertIn('split_by_category', a._AI_AGENT_TOOL_GUIDANCE_CORE)
        self.assertIn('SS = short sleeve, SL = long sleeve', a._AI_AGENT_TOOL_GUIDANCE_CORE)


if __name__ == '__main__':
    unittest.main()
