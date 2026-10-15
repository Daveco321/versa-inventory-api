"""TJX colour-key exports (David, Oct 15 2026): every tab opens with a live key, rows run
colour section by colour section (tjx_sections) and then by fabrication, a one-brand tab
drops its brand band, and a Summary tab comes first with every brand's colour totals.
Nothing else changes: the Monday TJMAXX ATS email (no color_key) keeps its old layout.
Synthetic rows only, offline. Run from tests/: python -B -m unittest test_tjx_color_key
"""
import io
import json
import os
import sys
import unittest
from datetime import date

import openpyxl

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import tjx_sections as T  # noqa: E402
from test_tjx_workbook import Base, fake_image, row  # noqa: E402

HEAD = 10      # Excel row of the column header under the key

WH = [row('TJAAXX001SLS', 'AAA', 'Brand A', 900),
      row('TJAAXX001SLS-V', 'AAA', 'Brand A', 40),                      # same style as 001
      row('TJAAXX002SLS', 'AAA', 'Brand A', 500, tjx_color_family='Black', color='Black Solid'),
      row('TMAAXX003SLP', 'AAA', 'Brand A', 100, tjx_color_family='Fancy', color='Navy Grnd Print'),
      row('TJBBXX010SLS', 'BBB', 'Brand B', 800, tjx_color_family='Navy Solid', color='Tony Blue Solid'),
      row('TJBBXX011SLS', 'BBB', 'Brand B', 60, tjx_color_family='Other Solid', color='Silver Solid')]
OS = [row('TJAAXX001SLS', 'AAA', 'Brand A', 300, po_ref='ZZ26001', arrival='Nov 3, 2026',
          tjx_source='Overseas', incoming=300),
      row('TJAAXX001SLS', 'AAA', 'Brand A', 200, po_ref='ZZ26004', arrival='Jan 3, 2027',
          tjx_source='Overseas', incoming=200),
      row('TMAAXX003SLP', 'AAA', 'Brand A', 72, po_ref='ZZ26002', arrival='TBD', tjx_source='Overseas',
          incoming=72, tjx_color_family='Fancy')]


def tabs():
    return [{'brand_name': 'Warehouse', 'tab_name': 'Warehouse', 'view_mode': 'ats', 'flow_mode': False,
             'keep_order': True, 'items': [dict(r) for r in WH]},
            {'brand_name': 'Overseas', 'tab_name': 'Overseas', 'view_mode': 'incoming', 'flow_mode': True,
             'keep_order': True, 'items': [dict(r) for r in OS]}]


def col_b(ws, rows):
    return [ws.cell(r, 2).value if ws.cell(r, 2).value is not None else ws.cell(r, 1).value for r in rows]


class KeyBase(Base):
    def render_key(self, color_key=True, images=None, tab_list=None, tjx_layout=True, summary_title=None):
        self.images = images or {}
        data = self.app.build_multi_brand_excel(tab_list or tabs(), 'http://photos.test/', catalog_mode=True,
                                                view_mode='ats', flow_mode=False, prepack_defaults=None,
                                                tjx_layout=tjx_layout, color_key=color_key,
                                                summary_title=summary_title)
        return (openpyxl.load_workbook(io.BytesIO(data)),
                openpyxl.load_workbook(io.BytesIO(data), data_only=True))


class Layout(KeyBase):
    def test_summary_first_then_the_tabs(self):
        wf, _ = self.render_key()
        self.assertEqual(wf.sheetnames, ['Summary', 'Warehouse', 'Overseas'])

    def test_header_moves_under_the_key_and_freezes_there(self):
        wf, _ = self.render_key()
        for name in ('Warehouse', 'Overseas'):
            ws = wf[name]
            self.assertEqual(ws.cell(HEAD, 1).value, 'IMAGE', name)
            self.assertEqual(ws.freeze_panes, f'A{HEAD + 1}', name)
            self.assertEqual(ws.cell(2, 1).value, 'COLOR KEY')
            self.assertEqual([ws.cell(r, 1).value for r in range(3, 9)],
                             ['White Solids', 'Black Solids', 'Navy & Blue Solids', 'Other Solids', 'Fancies', 'TOTAL'])

    def test_rows_run_by_brand_then_colour_section(self):
        wf, _ = self.render_key()
        ws = wf['Warehouse']
        self.assertEqual(col_b(ws, range(11, 25)), [
            'Brand A', 'White Solids', 'TJAAXX001SLS', 'TJAAXX001SLS-V',
            'Black Solids', 'TJAAXX002SLS',
            'Fancies · Prints', 'TMAAXX003SLP',
            'Brand B', 'Navy & Blue Solids', 'TJBBXX010SLS',
            'Other Solids · Grey', 'TJBBXX011SLS', None])

    def test_one_brand_tab_has_no_brand_band(self):
        one = [{'brand_name': 'Brand A', 'tab_name': 'Brand A', 'view_mode': 'ats', 'flow_mode': False,
                'keep_order': True, 'items': [dict(r) for r in WH[:4]]}]
        wf, _ = self.render_key(tab_list=one)
        ws = wf['Brand A']
        self.assertEqual(col_b(ws, range(11, 18)), ['White Solids', 'TJAAXX001SLS', 'TJAAXX001SLS-V',
                                                    'Black Solids', 'TJAAXX002SLS',
                                                    'Fancies · Prints', 'TMAAXX003SLP'])

    def test_a_style_stays_together_when_its_v_row_is_spelled_differently(self):
        items = [row('TJCCAW001SLP', 'CCC', 'Brand C', 500, tjx_color_family='Fancy', color='White Grnd Navy Check',
                     fabrication='4 Way Stretch'),
                 row('TJCCTS002SLP', 'CCC', 'Brand C', 300, tjx_color_family='Fancy', color='Blue Check',
                     fabrication='TC Stretch'),
                 row('TJCCAW001SLP-V', 'CCC', 'Brand C', 40, tjx_color_family='Fancy', color='White Grnd Navy Pld',
                     fabrication='4 Way Stretch 95% Polyester / 5% Spandex')]
        one = [{'brand_name': 'C', 'tab_name': 'C', 'view_mode': 'ats', 'keep_order': True, 'items': items}]
        wf, _ = self.render_key(tab_list=one)
        self.assertEqual(col_b(wf['C'], range(11, 15)),
                         ['Fancies · Checks', 'TJCCAW001SLP', 'TJCCAW001SLP-V', 'TJCCTS002SLP'])

    def test_fabrication_groups_inside_a_section(self):
        # fabric code = characters 5-6 of the style number (CT cotton, PE poly)
        items = [row('TJCCCT001SLS', 'CCC', 'Brand C', 100, fabrication='100% Cotton'),
                 row('TJCCPE002SLS', 'CCC', 'Brand C', 500, fabrication='95% Polyester / 5% Spandex'),
                 row('TJCCCT003SLS', 'CCC', 'Brand C', 400, fabrication='100% cotton'),
                 row('TJCCPE004SLS', 'CCC', 'Brand C', 50, fabrication='95% polyester, 5% Spandex (new)')]
        one = [{'brand_name': 'C', 'tab_name': 'C', 'view_mode': 'ats', 'keep_order': True, 'items': items}]
        wf, _ = self.render_key(tab_list=one)
        ws = wf['C']
        # cotton 500 beats poly 550? no: poly 550 first, then cotton 500; big style first inside each
        self.assertEqual(col_b(ws, range(11, 16)),
                         ['White Solids', 'TJCCPE002SLS', 'TJCCPE004SLS', 'TJCCCT003SLS', 'TJCCCT001SLS'])

    def test_deliveries_of_one_style_stay_together_by_date(self):
        wf, _ = self.render_key()
        ws = wf['Overseas']
        self.assertEqual(col_b(ws, range(11, 15)), ['White Solids', 'TJAAXX001SLS', 'TJAAXX001SLS', 'Fancies · Prints'])
        self.assertEqual([ws.cell(r, 13).value for r in (12, 13)], ['Nov 3, 2026', 'Jan 3, 2027'])

    def test_pictures_stay_on_their_rows(self):
        imgs = {i: fake_image() for i in range(len(WH))}
        wf, _ = self.render_key(images=imgs, tab_list=[tabs()[0]])
        ws = wf['Warehouse']
        rows = sorted(im.anchor._from.row + 1 for im in ws._images)
        self.assertEqual(sorted(ws.cell(r, 2).value for r in rows), sorted(r['sku'] for r in WH))

    def test_title_names_the_brands_view_and_date(self):
        app = self.app
        self.assertEqual(app._tjx_key_title(WH, 'ats', date(2026, 10, 15)),
                         'Brand A, Brand B  ·  Warehouse ATS  ·  as of Oct 15, 2026')
        many = [row(f'TJ{c}{c}XX001SLS', c * 3, f'Brand {c}', 50) for c in 'ABCDE']
        self.assertTrue(app._tjx_key_title(many, 'incoming', date(2026, 10, 15)).startswith('5 brands  ·  Overseas ATS'))

    def test_custom_titles(self):
        t = tabs()
        t[0]['key_title'] = 'My warehouse title'
        wf, _ = self.render_key(tab_list=t, summary_title='My summary')
        self.assertEqual(wf['Warehouse'].cell(1, 1).value, 'My warehouse title')
        self.assertEqual(wf['Summary'].cell(1, 1).value, 'My summary')


class Values(KeyBase):
    def key(self, ws, overseas=False):
        cols = (2, 3, 4) if overseas else (2, 3)
        return [[ws.cell(r, c).value for c in cols] for r in range(3, 9)]

    def test_cached_values_warehouse(self):
        _, wv = self.render_key()
        ws = wv['Warehouse']
        self.assertEqual(self.key(ws), [[1, 940], [1, 500], [1, 800], [1, 60], [1, 100], [5, 2400]])
        self.assertAlmostEqual(ws.cell(3, 4).value, 940 / 2400)
        self.assertAlmostEqual(ws.cell(8, 4).value, 1.0)

    def test_cached_values_overseas_no_deliveries_column(self):
        _, wv = self.render_key()
        ws = wv['Overseas']
        self.assertEqual([ws.cell(2, c).value for c in range(1, 5)], ['COLOR KEY', 'Styles', 'Units (ATS)', '% of Units'])
        self.assertEqual(self.key(ws), [[1, 500], [0, 0], [0, 0], [0, 0], [1, 72], [2, 572]])

    def test_formulas_cover_header_to_the_blank_row(self):
        wf, _ = self.render_key()
        ws = wf['Warehouse']      # data + bands end on row 23, blank row 24
        self.assertEqual(ws.cell(3, 3).value, '=SUMIFS($K$10:$K$24,$G$10:$G$24,"White")')
        self.assertIn('ISNUMBER($K$10:$K$24)', ws.cell(3, 2).value)
        self.assertEqual(ws.cell(8, 3).value, '=SUM($K$10:$K$24)')
        self.assertEqual(ws.cell(8, 4).value, '=SUM(D3:D7)')

    def test_colour_family_cells_are_painted(self):
        wf, _ = self.render_key()
        ws = wf['Warehouse']
        rules = [(str(rng.sqref), [r.formula for r in rs]) for rng, rs in ws.conditional_formatting._cf_rules.items()]
        self.assertEqual([sq for sq, _ in rules], ['G11:G23'])
        crits = sorted(f[0] for _, fs in rules for f in fs)
        self.assertEqual(crits, sorted(['"White"', '"Black"', '"Navy Solid"', '"Other Solid"', '"Fancy"']))


class Summary(KeyBase):
    def test_every_brand_then_all_brands(self):
        wf, wv = self.render_key()
        ws, sv = wf['Summary'], wv['Summary']
        labels = [ws.cell(r, 1).value for r in range(3, 25)]
        self.assertEqual(labels[:8], ['Brand A', 'White Solids', 'Black Solids', 'Navy & Blue Solids',
                                      'Other Solids', 'Fancies', 'Brand A Total', None])
        self.assertEqual(labels[8:15], ['Brand B', 'White Solids', 'Black Solids', 'Navy & Blue Solids',
                                        'Other Solids', 'Fancies', 'Brand B Total'])
        self.assertEqual(labels[16], 'ALL BRANDS')
        # Brand A comes from the Warehouse tab (its first tab); mixed views: no Deliveries column
        self.assertEqual(ws.cell(2, 3).value, 'Units (ATS)')
        self.assertEqual([sv.cell(r, 3).value for r in range(4, 10)], [940, 500, 0, 0, 100, 1540])
        self.assertEqual([sv.cell(r, 2).value for r in range(4, 10)], [1, 1, 0, 0, 1, 3])
        self.assertEqual(ws.cell(4, 3).value,
                         "=SUMIFS('Warehouse'!$K$10:$K$24,'Warehouse'!$G$10:$G$24,\"White\",'Warehouse'!$C$10:$C$24,\"Brand A\")")
        self.assertEqual([sv.cell(r, 3).value for r in range(20, 26)], [940, 500, 800, 60, 100, 2400])
        self.assertAlmostEqual(sv.cell(25, 4).value, 1.0)

    def test_a_brand_with_its_own_tab_is_read_from_it(self):
        both = [{'brand_name': 'All Brands', 'tab_name': 'All Brands', 'view_mode': 'ats', 'keep_order': True,
                 'items': [dict(r) for r in WH]},
                {'brand_name': 'Brand A', 'tab_name': 'Brand A', 'view_mode': 'ats', 'keep_order': True,
                 'items': [dict(r) for r in WH[:4]]}]
        wf, _ = self.render_key(tab_list=both)
        ws = wf['Summary']
        self.assertIn("'Brand A'!", ws.cell(4, 3).value)        # Brand A: its own tab
        self.assertIn("'All Brands'!", ws.cell(12, 3).value)    # Brand B: only in All Brands

    def test_overseas_summary_has_no_deliveries_column(self):
        _, wv = self.render_key(tab_list=[tabs()[1]])
        sv = wv['Summary']
        self.assertEqual([sv.cell(2, c).value for c in range(1, 5)], ['BRAND / COLOR', 'Styles', 'Units (ATS)', '% of Total'])
        self.assertEqual([sv.cell(r, 3).value for r in range(4, 10)], [500, 0, 0, 0, 72, 572])


class Gating(KeyBase):
    def test_no_color_key_keeps_the_old_layout(self):
        wf, _ = self.render_key(color_key=False)
        ws = wf['Warehouse']
        self.assertEqual(wf.sheetnames, ['Warehouse', 'Overseas'])
        self.assertEqual(ws.cell(1, 1).value, 'IMAGE')
        self.assertEqual(ws.freeze_panes, 'A2')
        self.assertEqual(ws.cell(2, 1).value, 'Brand A')          # brand band, no sections
        self.assertEqual(ws.cell(3, 2).value, 'TJAAXX001SLS')

    def test_key_needs_the_tjx_layout(self):
        wf, _ = self.render_key(tjx_layout=False)
        self.assertEqual(wf['Warehouse'].cell(1, 1).value, 'IMAGE')

    def test_single_brand_export_gets_the_key_and_summary(self):
        self.images = {}
        data = self.app.build_brand_excel('Brand A', [dict(r) for r in WH[:4]], 'http://photos.test/',
                                          view_mode='ats', catalog_mode=True, tjx_layout=True, color_key=True)
        wb = openpyxl.load_workbook(io.BytesIO(data), data_only=True)
        self.assertEqual(wb.sheetnames, ['Summary', 'Brand A'])
        ws = wb['Brand A']
        self.assertEqual(ws.cell(HEAD, 1).value, 'IMAGE')
        self.assertEqual(ws.cell(8, 3).value, 1540)
        self.assertEqual(ws.cell(11, 1).value, 'White Solids')     # no brand band on one brand

    def test_monday_email_workbook_is_unchanged(self):
        import inspect
        src = inspect.getsource(self.app.export_tjx_ats)
        self.assertIn('build_multi_brand_excel(', src)
        self.assertNotIn('color_key', src)


class Routes(KeyBase):
    def test_export_multi_route_passes_the_key_for_tjx_layout(self):
        seen = {}
        orig = self.app.build_multi_brand_excel

        def spy(*a, **k):
            seen.update(k)
            return orig(*a, **k)
        self.app.build_multi_brand_excel = spy
        try:
            c = self.app.app.test_client()
            for flag, want in ((True, True), (False, False)):
                seen.clear()
                body = {'brands': tabs(), 'catalog_mode': True, 'tjx_layout': flag, 'view_mode': 'ats',
                        'prepack_defaults': [{'x': 1}], 'summary_title': 'S'}
                r = c.post('/export-multi', data=json.dumps(body), content_type='application/json')
                if r.status_code in (401, 403):
                    self.skipTest('auth gate active in this environment')
                self.assertEqual(r.status_code, 200, r.data[:300])
                self.assertEqual(seen.get('color_key'), want)
                self.assertEqual(seen.get('summary_title'), 'S')
        finally:
            self.app.build_multi_brand_excel = orig


class Sections(unittest.TestCase):
    def test_other_solid_groups(self):
        g = T.other_solid_group
        self.assertEqual([g('Silver Solid'), g('Charcoal Solid Basketweave Dobby'), g('Khaki Solid'),
                          g('Peanut Butter Solid'), g('Sage Green Solid'), g('Lt Pink Solid'),
                          g('Eggplant Solid'), g('Burgundy Solid'), g('Burnt Orange Solid'),
                          g('Butter Yellow Solid'), g('Mystery Solid')],
                         ['Grey', 'Grey', 'Khaki & Brown', 'Khaki & Brown', 'Green', 'Pink', 'Purple', 'Red',
                          'Orange', 'Yellow', T.OTHER_SOLID_LAST])

    def test_fancy_groups_follow_the_brand_page_words(self):
        f = T.fancy_group
        self.assertEqual([f('White Grnd Navy Check'), f('Blue & White Plaid'), f('Gray Grnd White Pinstripe Print'),
                          f('Navy Grnd White Dots Print'), f('White Grnd Black Geo Print'),
                          f('Blue Grnd White Floral Print'), f('Black & White Herringbone')],
                         ['Checks', 'Checks', 'Stripes', 'Geos', 'Geos', 'Prints', 'Prints'])

    def test_section_order_and_empty_family(self):
        labels = [T.section_of({'tjx_color_family': f, 'color': c})[1] for f, c in (
            ('White', 'White Solid'), ('Black', ''), ('Navy Solid', ''), ('Other Solid', 'Silver Solid'),
            ('Fancy', 'Check'), ('', 'Something'))]
        self.assertEqual(labels, ['White Solids', 'Black Solids', 'Navy & Blue Solids', 'Other Solids · Grey',
                                  'Fancies · Checks', 'Fancies · Prints'])
        keys = [T.section_of({'tjx_color_family': f, 'color': c})[0] for f, c in (
            ('White', ''), ('Other Solid', 'Silver'), ('Other Solid', 'Red'), ('Fancy', 'Check'), ('Fancy', 'Print'))]
        self.assertEqual(keys, sorted(keys))

    def test_fabric_key_folds_spellings(self):
        k = T.fabric_key
        self.assertEqual(k('95% Polyester / 5% Spandex'), k('95% polyester, 5% Spandex'))
        self.assertNotEqual(k('100% Cotton'), k('100% Polyester'))


class ReviewFixes(KeyBase):
    def test_blank_or_odd_family_counts_as_fancy(self):
        items = [row('TJCCPE001SLS', 'CCC', 'Brand C', 100),
                 row('TJCCPE002SLP', 'CCC', 'Brand C', 30, tjx_color_family='', color='Mystery'),
                 row('TJCCPE003SLP', 'CCC', 'Brand C', 20, tjx_color_family='Weird', color='Mystery'),
                 row('TJCCPE004SLP', 'CCC', 'Brand C', 10, tjx_color_family='Fancy', color='Blue Print')]
        one = [{'brand_name': 'C', 'tab_name': 'C', 'view_mode': 'ats', 'keep_order': True, 'items': items}]
        wf, wv = self.render_key(tab_list=one)
        ws = wv['C']
        self.assertEqual([ws.cell(r, 3).value for r in range(3, 9)], [100, 0, 0, 0, 60, 160])
        self.assertEqual([ws.cell(r, 2).value for r in range(3, 9)], [1, 0, 0, 0, 3, 4])
        self.assertAlmostEqual(ws.cell(8, 4).value, 1.0)
        self.assertIn('<>"White"', wf['C'].cell(7, 2).value)
        sv = wv['Summary']
        self.assertEqual([sv.cell(r, 3).value for r in range(4, 10)], [100, 0, 0, 0, 60, 160])

    def test_summary_adds_a_brand_over_its_one_brand_tabs_of_one_view(self):
        a1 = [row('TJAAPE001SLS', 'AAA', 'Brand A', 100)]
        a2 = [row('TJAAPE002SLS', 'AAA', 'Brand A', 50, tjx_color_family='Black')]
        os_ = [row('TJAAPE003SLS', 'AAA', 'Brand A', 999, po_ref='ZZ1', arrival='Nov 3, 2026')]
        t = [{'brand_name': 'A1', 'tab_name': 'A1', 'view_mode': 'ats', 'keep_order': True, 'items': a1},
             {'brand_name': 'A2', 'tab_name': 'A2', 'view_mode': 'ats', 'keep_order': True, 'items': a2},
             {'brand_name': 'AO', 'tab_name': 'AO', 'view_mode': 'incoming', 'flow_mode': True, 'keep_order': True,
              'items': os_}]
        wf, wv = self.render_key(tab_list=t)
        sv = wv['Summary']
        self.assertEqual([sv.cell(r, 3).value for r in range(4, 10)], [100, 50, 0, 0, 0, 150])
        self.assertIn("'A1'!", wf['Summary'].cell(4, 3).value)
        self.assertIn("'A2'!", wf['Summary'].cell(4, 3).value)
        self.assertNotIn("'AO'!", wf['Summary'].cell(9, 3).value)
        self.assertIn('Warehouse ATS', wf['Summary'].cell(1, 1).value)

    def test_arrival_width_only_changes_under_the_key(self):
        wf, _ = self.render_key(tab_list=[tabs()[1]])
        self.assertEqual(round(wf['Overseas'].column_dimensions['M'].width), 22)
        wf, _ = self.render_key(tab_list=[tabs()[1]], color_key=False)
        self.assertEqual(round(wf['Overseas'].column_dimensions['M'].width), 17)

    def test_fabric_key_keeps_percent_with_its_fibre(self):
        self.assertNotEqual(T.fabric_key('65% Polyester / 35% Cotton'), T.fabric_key('65% Cotton / 35% Polyester'))


if __name__ == '__main__':
    unittest.main()
