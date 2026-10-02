"""Line sheets with one tab per brand and a customer-prefix filter (Oct 2 2026).

David asked for TJ + TM styles on one sheet with a tab per brand. The tool
used to stop at 4 tabs (silently) and could only match one prefix at a time.
Everything here is synthetic and offline: the style numbers are made up (serial
9xx, fabric ZZ), and the workbook builder and S3 are replaced by recorders."""
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _support import load_app  # noqa: E402


def _row(style, brand, wh=0, inc=0, ats=None, nj=0):
    return {'style': style, 'brand_abbr': brand, 'total_warehouse': wh, 'incoming': inc,
            'total_ats': wh + inc if ats is None else ats, 'jtw': wh - nj, 'tr': 0, 'dcw': 0, 'qa': 0,
            'nj': nj, 'abfi': 0, 'committed': 0, 'allocated': 0}


AGG = {r['style']: r for r in [
    _row('TJNAZZ901SLS', 'NAUTICA', wh=500),
    _row('TMNAZZ902SLS', 'NAUTICA', wh=100, inc=900),
    _row('TJDKZZ903SLS', 'DKNY', inc=3000),
    _row('TMDKZZ904SLS', 'DKNY', wh=50, inc=200),
    _row('TMVCZZ905SLS', 'VINCE', wh=2000),
    _row('TJCHZZ906SLS', 'CHAPS', wh=40),
    _row('RONAZZ907SLS', 'NAUTICA', wh=9000, inc=9000),   # other customer prefix
    _row('AMDKZZ908SLS', 'DKNY', wh=7000),                # other customer prefix
    _row('XXTJZZ909SLS', 'EB', wh=10),                    # "TJ" inside, not a prefix
]}

# Brand codes that name the same brand, and a brand whose stock is mostly NJ.
AGG_CODES = dict(AGG, **{r['style']: r for r in [
    _row('TMNTZZ910SLS', 'NT', wh=30),                    # Nautica overflow serial
    _row('TJKLZZ911SLS', 'KLP', wh=20),                   # feed spells Karl Lagerfeld KLP
    _row('TMKLZZ912SLS', 'KL', wh=5),
    _row('TJEBZZ913SLS', 'EB', wh=5000, nj=4990),         # 10 visible, 4,990 admin-only
]})


class LineSheetTabs(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.app = load_app()

    def setUp(self):
        a = self.app
        self._saved = {k: getattr(a, k) for k in (
            '_ai_agent_agg_inventory', '_ai_agent_enrich', 'build_multi_brand_excel', 'get_s3',
            '_production_data', '_fresh_prepack_defaults', '_hidden_landing_maps', '_wh_split_by_base')}
        self.built = {}
        self.agg = AGG
        a._ai_agent_agg_inventory = lambda: self.agg
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

    # ── the shared filter ─────────────────────────────────────────────────
    def test_customer_prefixes_match_only_the_first_two_letters(self):
        rows, _ = self.app._ai_agent_filter({'customer_prefixes': ['tj', 'TM']})
        got = {r['style'] for r in rows}
        self.assertEqual(got, {'TJNAZZ901SLS', 'TMNAZZ902SLS', 'TJDKZZ903SLS', 'TMDKZZ904SLS',
                               'TMVCZZ905SLS', 'TJCHZZ906SLS'})

    def test_single_customer_prefix_string_works_too(self):
        rows, _ = self.app._ai_agent_filter({'customer_prefix': 'TM', 'stock': 'warehouse'})
        self.assertEqual({r['style'] for r in rows}, {'TMNAZZ902SLS', 'TMDKZZ904SLS', 'TMVCZZ905SLS'})

    def test_a_comma_separated_prefix_string_keeps_every_prefix(self):
        rows, _ = self.app._ai_agent_filter({'customer_prefix': 'TJ, TM', 'stock': 'overseas'})
        self.assertEqual({r['style'] for r in rows}, {'TMNAZZ902SLS', 'TJDKZZ903SLS', 'TMDKZZ904SLS'})

    def test_no_prefix_means_no_prefix_filter(self):
        rows, _ = self.app._ai_agent_filter({})
        self.assertEqual(len(rows), len(AGG))

    # ── split_by_brand ────────────────────────────────────────────────────
    def test_split_by_brand_gives_one_tab_per_brand_biggest_first(self):
        out = self.app._ai_tool_build_line_sheet({'tabs': [
            {'customer_prefixes': ['TJ', 'TM'], 'stock': 'warehouse', 'split_by_brand': True}]})
        self.assertNotIn('error', out)
        self.assertEqual(self._tabs(), [
            ('Vince Camuto', ['TMVCZZ905SLS']),
            ('Nautica', ['TMNAZZ902SLS', 'TJNAZZ901SLS']),
            ('DKNY', ['TMDKZZ904SLS']),
            ('Chaps', ['TJCHZZ906SLS']),
        ])
        self.assertTrue(all(t['view_mode'] == 'ats' for t in self.built['tabs']))

    def test_overseas_split_orders_by_incoming_units(self):
        self.app._ai_tool_build_line_sheet({'tabs': [
            {'customer_prefixes': ['TJ', 'TM'], 'stock': 'overseas', 'split_by_brand': True}]})
        self.assertEqual(self._tabs(), [
            ('DKNY', ['TJDKZZ903SLS', 'TMDKZZ904SLS']),
            ('Nautica', ['TMNAZZ902SLS']),
        ])
        self.assertTrue(all(t['view_mode'] == 'incoming' for t in self.built['tabs']))

    def test_split_keeps_the_given_title_as_a_suffix(self):
        self.app._ai_tool_build_line_sheet({'tabs': [
            {'title': 'Overseas', 'customer_prefixes': ['TJ'], 'stock': 'overseas', 'split_by_brand': True}]})
        self.assertEqual([n for n, _ in self._tabs()], ['DKNY - Overseas'])

    def test_a_brand_filter_on_the_parent_narrows_the_split(self):
        tabs = self.app._line_sheet_split_by_brand(
            {'brands': ['NAUTICA', 'DKNY'], 'customer_prefixes': ['TJ', 'TM'], 'split_by_brand': True})
        self.assertEqual([t['brands'] for t in tabs], [['DKNY'], ['NAUTICA']])
        self.assertTrue(all('split_by_brand' not in t for t in tabs))

    def test_split_of_a_curated_skus_tab_keeps_caller_order_per_brand(self):
        tabs = self.app._line_sheet_split_by_brand(
            {'skus': ['TMDKZZ904SLS', 'TJNAZZ901SLS', 'TJDKZZ903SLS', 'NOPE123'], 'split_by_brand': True})
        self.assertEqual([(t['title'], t['skus']) for t in tabs], [
            ('DKNY', ['TMDKZZ904SLS', 'TJDKZZ903SLS']),
            ('Nautica', ['TJNAZZ901SLS']),
            ('Not found', ['NOPE123']),
        ])

    def test_split_mixed_with_plain_tabs_keeps_tab_order(self):
        self.app._ai_tool_build_line_sheet({'tabs': [
            {'title': 'All TJ', 'customer_prefixes': ['TJ']},
            {'customer_prefixes': ['TM'], 'stock': 'warehouse', 'split_by_brand': True}]})
        self.assertEqual([n for n, _ in self._tabs()], ['All TJ', 'Vince Camuto', 'Nautica', 'DKNY'])

    # ── brand codes and customer view ─────────────────────────────────────
    def test_codes_for_the_same_brand_share_one_full_name_tab(self):
        self.agg = AGG_CODES
        self.app._ai_tool_build_line_sheet({'tabs': [
            {'customer_prefixes': ['TJ', 'TM'], 'stock': 'warehouse', 'split_by_brand': True}]})
        tabs = dict(self._tabs())
        self.assertEqual(sorted(tabs['Nautica']), ['TJNAZZ901SLS', 'TMNAZZ902SLS', 'TMNTZZ910SLS'])
        self.assertEqual(sorted(tabs['Karl Lagerfeld Paris']), ['TJKLZZ911SLS', 'TMKLZZ912SLS'])
        self.assertNotIn('KLP', tabs)
        self.assertNotIn('NT', tabs)

    def test_customer_view_ranks_brands_by_the_units_it_shows(self):
        self.agg = AGG_CODES
        admin = self.app._line_sheet_split_by_brand(
            {'customer_prefixes': ['TJ', 'TM'], 'stock': 'warehouse', 'split_by_brand': True})
        cust = self.app._line_sheet_split_by_brand(
            {'customer_prefixes': ['TJ', 'TM'], 'stock': 'warehouse', 'split_by_brand': True},
            customer_view=True)
        self.assertEqual(admin[0]['title'], 'Eddie Bauer')        # 5,000 gross, mostly NJ
        self.assertEqual(cust[-1]['title'], 'Eddie Bauer')        # 10 visible
        self.assertEqual(cust[0]['title'], 'Vince Camuto')

    def test_emailed_sheet_counts_describe_the_file_sent(self):
        # The mailbox's per-warehouse pass drops styles with no sellable stock;
        # each tab's count must follow, and an emptied tab must say so.
        a = self.app
        a._wh_split_by_base = lambda skus: {s: {'jtw': 0 if s == 'TJCHZZ906SLS' else 1, 'tr': 0, 'dcw': 0,
                                                 'qa': 0, 'total': 0 if s == 'TJCHZZ906SLS' else 1,
                                                 'exact': True} for s in skus}
        out = a._ai_tool_build_line_sheet({'customer_view': True, 'warehouse_breakdown': True, 'tabs': [
            {'customer_prefixes': ['TJ', 'TM'], 'stock': 'warehouse', 'split_by_brand': True}]})
        entries = {e['tab']: e for e in out['tabs'] if 'tab' in e}
        self.assertEqual(entries['Nautica']['styles'], 2)
        self.assertTrue(entries['Chaps'].get('skipped'))
        self.assertEqual(entries['Chaps']['styles'], 0)
        self.assertNotIn('Chaps', [n for n, _ in self._tabs()])

    # ── tab cap ───────────────────────────────────────────────────────────
    def test_more_than_four_tabs_are_built(self):
        tabs = [{'title': f'T{i}', 'skus': ['TJNAZZ901SLS']} for i in range(9)]
        out = self.app._ai_tool_build_line_sheet({'tabs': tabs})
        self.assertEqual(len(self.built['tabs']), 9)
        self.assertFalse(any('tabs_dropped_over_cap' in e for e in out['tabs']))

    def test_tabs_over_the_cap_are_named_never_dropped_silently(self):
        cap = self.app._LINE_SHEET_MAX_TABS
        tabs = [{'title': f'T{i}', 'skus': ['TJNAZZ901SLS']} for i in range(cap + 2)]
        out = self.app._ai_tool_build_line_sheet({'tabs': tabs})
        self.assertEqual(len(self.built['tabs']), cap)
        note = next(e for e in out['tabs'] if 'tabs_dropped_over_cap' in e)
        self.assertEqual(note['tabs_dropped_over_cap'], [f'T{cap}', f'T{cap + 1}'])

    # ── schema ────────────────────────────────────────────────────────────
    def test_schema_advertises_the_new_options(self):
        tool = next(t for t in self.app._AI_AGENT_TOOLS if t['name'] == 'build_line_sheet')
        props = tool['input_schema']['properties']['tabs']['items']['properties']
        for k in ('split_by_brand', 'customer_prefixes', 'search'):
            self.assertIn(k, props)
        self.assertNotIn('Max 4 tabs', tool['description'])
        q = next(t for t in self.app._AI_AGENT_TOOLS if t['name'] == 'query_inventory')
        self.assertIn('customer_prefixes', q['input_schema']['properties'])


if __name__ == '__main__':
    unittest.main()
