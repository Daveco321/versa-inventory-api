"""Inventory Aging (Oct 5 2026): inventory_aging.py and its app.py routes.

Everything here is synthetic: made-up styles (TTXXAA..., ZZYYBB...), made-up
container numbers (ABCU...), made-up quantities. Workbooks are built in memory
with openpyxl in the layout of the hourly Inventory_ATS.xlsx (ATS sheet plus the
JTW/TR/DCW raw sheets and their *_INVENTORY sheets, pivot block included). The
app is imported offline (tests/_support.py blocks sockets)."""
import gzip
import io
import json
import os
import shutil
import sys
import tempfile
import unittest
from datetime import date, datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import inventory_aging as ia  # noqa: E402

TODAY = date(2026, 10, 5)

ATS_HDR = ['SKU', 'Brand', 'Container', 'Receive Date', 'Lot Number', 'Incoming', 'JTW', 'TR', 'DCW', 'QA',
           'ABFI', 'NJ', 'Committed', 'Allocated', 'Total ATS']
JTW_HDR = ['Inventory Date', 'Container #', 'Style', 'Received', 'Location', 'PO #', 'Available Inv', 'SourceFile']
JTW_OLD_HDR = ['Date Received', 'Container', 'Style', 'Cartons', 'Location', 'QTY Shipped', 'Ballance', 'SourceFile']
JTWI_HDR = ['Container #', 'Style', 'QTY', 'Quantity', 'Brand', 'Receive Date', 'SourceFile', None, None,
            'Style', 'Brand', 'Container #', 'Sum of Quantity']
JTWI_OLD_HDR = ['Container', 'Style', 'QTY', 'Quantity', 'Brand', 'SourceFile', None, None, 'Style', 'Brand',
                'Container', 'Sum of Quantity']
# TR/DCW column orders differ from the live file on purpose: everything is looked up by name.
TR_HDR = ['#', 'Hold Status', 'SKU', 'Lot Number', 'Reference ID', 'Receipt Date', 'SourceFile']
TRI_HDR = ['SKU', 'Container', 'Lot Number', 'Available Primary', 'On Hand Primary', 'QTY', 'BRAND',
           'Receive Date', 'SourceFile', None, None, 'SKU', 'BRAND', 'Container', 'Lot Number', 'Sum of QTY']
DCW_HDR = ['#', 'SKU', 'Receipt Date', 'Reference ID', 'Lot Number', 'SourceFile']
DCWI_HDR = ['SKU', 'Style', 'Brand', 'Container', 'Lot Number', 'Available Primary', 'On Hand Primary', 'QTY',
            'Receive Date', 'SourceFile', None, None, 'Style', 'Brand', 'Container', 'Lot Number', 'Sum of QTY']

A, B, C, E = 'TTXXAA001SLS', 'TTXXAA002SLS', 'TTXXAA005SLS', 'ZZYYBB004RFS-V'
DM, DL, G, H = 'TTXXAA003SLS-M', 'TTXXAA003SLS-L', 'TTXXAA006SLS', 'TTXXAA007SLS'
F, I, AFBA = 'OLDSTYLE-22-38', 'TTXXAA008SLS', 'TTXXAA001SLS-FBA'


def ats(sku, brand, receive=None, jtw=0, tr=0, dcw=0, qa=0, abfi=0, nj=0, committed=0, allocated=0, total=0):
    return [sku, brand, 'N/A', receive, 'N/A', 0, jtw, tr, dcw, qa, abfi, nj, committed, allocated, total]


def jtw(d, cont, style):
    return [d, cont, style, 1, 'R1', None, '1', 'JTW']


def jtwi(cont, style, units, brand='ZZ'):
    # the pivot block on the right repeats Style/Brand/Container # with decoy values
    return [cont, style, units // 36, units, brand, '01-01-2020', 'JTW', None, None, 'DECOY', 'ZZ', 'DECOY', 999]


def tr(d, sku):
    return [1, 'False', sku, 'nan', 'REF', d, 'TR']


def tri(sku, cont, lot, units, brand='ZZ'):
    return [sku, cont, lot, units, units, units, brand, '01-01-2020', 'TR', None, None, 'DECOY', 'ZZ', 'DECOY',
            'DECOY', 999]


def dcw(d, sku):
    return [1, sku, d, 'REF', 'lot', 'DCW']


def dcwi(sku, style, cont, lot, units, brand='ZZ'):
    return [sku, style, brand, cont, lot, units, units, units, '01-01-2020', 'DCW', None, None, 'DECOY', 'ZZ',
            'DECOY', 'DECOY', 999]


def std_sheets():
    """The standard synthetic file (ages as of 2026-10-05):
    A  JTW 360 on 6/18/2026 + 720 dated 1/15/1900 in the same container (fixed to 6/18/2026)
    B  JTW 300 on the count date, restored by (container, style) to 1/10/2025;
       200 on the count date with container N/A (count_date); 100 on 3/20 whose map date is newer (count_date)
    C  JTW 150 on 3/20, restored by container only to 2/1/2025
    DM TR 200 on 10/5/2025 by CARSON TRANSFER (reset); DL TR 100 on 9/6/2026
    E  DCW 48 on 10/6/2023 (exactly 1,095 days), lot RMA 12 (reset)
    G  JTW 50 dated 10/7/2026 (future, no sibling: undated); H JTW 40 dated 10/6/2026 (tomorrow: valid)
    plus 3 units of a TR item the ATS sheet does not list, and a zero-unit JTW line."""
    return {
        'ATS': [ATS_HDR,
                ats(A, 'DKNY', '06-18-2026', jtw=1080, qa=12, abfi=5, nj=7, committed=-100, total=1004),
                ats(B, 'NAUTICA', '03-18-2026', jtw=600, total=600),
                ats(C, 'CHAPS', None, jtw=150, total=150),
                ats(DM, 'USPA', '10-05-2025', tr=200, total=200),
                ats(DL, 'USPA', None, tr=100, total=100),
                ats(E, 'VD', '10-06-2023', dcw=48, total=48),
                ats(F, 'BEN', None, qa=30, total=30),
                ats(G, 'DKNY', None, jtw=50, total=50),
                ats(H, 'DKNY', '10-06-2026', jtw=40, total=40),
                ats(I, 'NT'),
                ats(AFBA, 'DKNY', nj=9),
                ats('N/A', 'DKNY', jtw=999)],
        'JTW': [JTW_HDR,
                jtw('2026-06-18 00:00:00', ' ABCU1234567', A),
                jtw('1900-01-15 00:00:00', 'ABCU1234567 ', A),
                jtw('2026-03-18 00:00:00', 'ABCU7654321', B),
                jtw('2026-03-18 00:00:00', 'N/A', B),
                jtw('2026-03-20 00:00:00', 'ABCU5555555', B),
                jtw('2026-03-20 00:00:00', 'abcu7654321 ', C),
                jtw('2026-10-07 00:00:00', 'ABCU0000001', G),
                jtw('2026-10-06 00:00:00', 'ABCU0000002', H),
                jtw('2026-05-01 00:00:00', 'ABCU0000003', A)],
        'JTW_INVENTORY': [JTWI_HDR,
                          jtwi(' ABCU1234567', A, 360), jtwi('ABCU1234567 ', A, 720),
                          jtwi('ABCU7654321', B, 300), jtwi('N/A', B, 200), jtwi('ABCU5555555', B, 100),
                          jtwi('abcu7654321 ', C, 150), jtwi('ABCU0000001', G, 50), jtwi('ABCU0000002', H, 40),
                          jtwi('ABCU0000003', A, 0)],
        'TR': [TR_HDR, tr('2025-10-05 10:52:19', DM), tr('2026-09-06 08:00:00', DL),
               tr('2026-01-02 08:00:00', 'ZZSUPPLYLABELS'), [None] * 7, [None] * 7],
        'TR_INVENTORY': [TRI_HDR, tri(DM, 'CARSON TRANSFER', 'nan', 200), tri(DL, 'ABCU1112223', 'PO 77', 100),
                         tri('ZZSUPPLYLABELS', 'N/A', 'N/A', 3)],
        'DCW': [DCW_HDR, dcw('2023-10-06 09:00:00', 'ZZYYBB004RFS-V(LOT1)')],
        'DCW_INVENTORY': [DCWI_HDR, dcwi('ZZYYBB004RFS-V(LOT1)', E, 'ABCU2223334', 'RMA 12', 48)],
    }


RESTORE = {'pairs': {'ABCU7654321|' + B: '2025-01-10'},
           'containers': {'ABCU7654321': '2025-02-01', 'ABCU5555555': '2026-04-01'},
           'source': 'synthetic', 'builtAt': '2026-10-05'}


def make_wb(sheets):
    import openpyxl
    wb = openpyxl.Workbook()
    wb.remove(wb.active)
    for name, rows in sheets.items():
        ws = wb.create_sheet(name)
        for r in rows:
            ws.append(r)
    buf = io.BytesIO()
    wb.save(buf)
    return buf.getvalue()


def std_bytes():
    return make_wb(std_sheets())


def lot_by(lots, sku, wh=None, units=None):
    return [l for l in lots if l['sku'] == sku and (wh is None or l['wh'] == wh)
            and (units is None or l['units'] == units)]


class Buckets(unittest.TestCase):
    def test_edges(self):
        # bucket index by age in days: 0 under 1 month ... 6 three years or more
        cases = {0: 0, 29: 0, 30: 1, 89: 1, 90: 2, 179: 2, 180: 3, 364: 3, 365: 4, 729: 4, 730: 5, 1094: 5,
                 1095: 6, 5000: 6, -1: 0}
        for days, i in cases.items():
            self.assertEqual(ia.bucket_index(days), i, days)
            self.assertEqual(ia.bucket_key(days), ia.BUCKETS[i][0], days)

    def test_nuri_edges(self):
        cases = {29: 'lt1m', 30: 'm1', 89: 'm1', 90: 'm3', 179: 'm3', 180: 'm6', 359: 'm6', 360: 'y1',
                 719: 'y1', 720: 'y2', 1079: 'y2', 1080: 'y3'}
        for days, key in cases.items():
            self.assertEqual(ia.bucket_key(days, ia.NURI_BUCKETS), key, days)

    def test_contract_keys(self):
        self.assertEqual([b[0] for b in ia.BUCKETS], ['lt1m', 'm1_3', 'm3_6', 'm6_12', 'y1_2', 'y2_3', 'y3p'])
        self.assertEqual([b[0] for b in ia.NURI_BUCKETS], ['lt1m', 'm1', 'm3', 'm6', 'y1', 'y2', 'y3'])
        self.assertEqual([b[2] for b in ia.NURI_BUCKETS], [0, 30, 90, 180, 360, 720, 1080])
        for label in [b[1] for b in ia.BUCKETS + ia.NURI_BUCKETS]:
            self.assertNotIn('\u2014', label)


class Dates(unittest.TestCase):
    def test_forms(self):
        d = date(2026, 7, 13)
        for v in (datetime(2026, 7, 13, 10, 52, 19), d, '2026-07-13 10:52:19', '2026-07-13', '07-13-2026',
                  '7/13/2026', '7/13/26', ' 2026-07-13 ', '2026-07-13T10:52:19'):
            self.assertEqual(ia.parse_date(v), d, v)
        self.assertEqual(ia.parse_date('12/31/99'), date(1999, 12, 31))
        self.assertEqual(ia.parse_date('1900-01-15 00:00:00'), date(1900, 1, 15))

    def test_junk(self):
        for v in (None, '', 'nan', 'N/A', 'NaT', 'garbage', '2026-02-30', '13/45/2026', True, 12, '278', 0,
                  40178, '39999', 60000, '99999', float('nan'), '123456'):
            self.assertIsNone(ia.parse_date(v), v)

    def test_excel_serials(self):
        # a serial day number left in a date column (as a number or as text) is a date from 2010 on
        self.assertEqual(ia.parse_date(45000), date(2023, 3, 15))
        self.assertEqual(ia.parse_date('45000'), date(2023, 3, 15))
        self.assertEqual(ia.parse_date(' 45000.0 '), date(2023, 3, 15))
        self.assertEqual(ia.parse_date(45000.75), date(2023, 3, 15))
        self.assertEqual(ia.parse_date(40179), date(2010, 1, 1))
        self.assertEqual(ia.parse_date(59999), date(2064, 4, 7))
        # the ATS Receive Date always reaches the payload as MM-DD-YYYY text
        self.assertEqual(ia.receive_text('45000'), '03-15-2023')
        self.assertEqual(ia.receive_text(45000), '03-15-2023')
        self.assertEqual(ia.receive_text(datetime(2026, 6, 18, 9, 0)), '06-18-2026')
        self.assertEqual(ia.receive_text(' 06-18-2026 '), '06-18-2026')
        self.assertIsNone(ia.receive_text('nan'))
        self.assertIsNone(ia.receive_text(None))


class StyleKeys(unittest.TestCase):
    def test_base_and_prefix(self):
        self.assertEqual(ia.base_style('TTXXAA001SLS'), 'TTXXAA001SLS')
        self.assertEqual(ia.base_style('TTXXAA001SLS-V'), 'TTXXAA001SLS')
        self.assertEqual(ia.base_style('TTXXAA001SLS-M'), 'TTXXAA001SLS')
        self.assertEqual(ia.base_style('TTXXAA001SLS-M-V'), 'TTXXAA001SLS')
        self.assertEqual(ia.base_style('TTXXAA001SLS-1515.3435'), 'TTXXAA001SLS')
        self.assertEqual(ia.base_style('OLDSTYLE-22-38'), 'OLDSTYLE-22-38')     # legacy key, not a size
        self.assertEqual(ia.base_style('TTXXAA001SLS-FBA'), 'TTXXAA001SLS-FBA')
        self.assertEqual(ia.customer_prefix('TTXXAA001SLS'), 'TT')
        self.assertEqual(ia.customer_prefix('ZZYYBB004RFS-V'), 'ZZ')
        self.assertEqual(ia.customer_prefix('OLDSTYLE-22-38'), 'Other')
        self.assertEqual(ia.customer_prefix('ZZSUPPLYLABELS'), 'Other')

    def test_matches_the_app_rule(self):
        from _support import load_app
        a = load_app()
        for s in ('TTXXAA001SLS', 'TTXXAA001SLS-V', 'TTXXAA001SLS-M', 'TTXXAA001SLS-XL-FBA', 'TTXXAA001SLS-FBA',
                  'TTXXAA001SLS-1515.53233', 'TTXXAA001SLS-15-15.532/35', 'TTXXAA001SLS-V-32X30',
                  'TTXXAA001SLS-32W30L', 'OLDSTYLE-22-38', 'AB-CD-001', 'TTXXAA001SLS-RED', 'ZZYYBB004RFS-16 34/35'):
            self.assertEqual(ia.is_sized_sku(s), a._is_sized_sku(s), s)


class Parse(unittest.TestCase):
    def test_standard_file(self):
        p = ia.parse_workbook(std_bytes())
        self.assertTrue(p['lotsOk'], p['lotsError'])
        self.assertEqual(p['jtwFormat'], 'new')
        self.assertEqual(p['notInAts'], 3)
        self.assertEqual(p['notInAtsSkus'], ['ZZSUPPLYLABELS'])
        self.assertEqual(p['lotTotals'], {'JTW': 1920, 'TR': 300, 'DCW': 48})
        self.assertEqual(p['whTotals'], {'JTW': 1920, 'TR': 300, 'DCW': 48})
        self.assertEqual(len(p['lots']), 11)                       # the zero-unit JTW line is ignored
        self.assertNotIn('N/A', p['skus'])                         # same skips as parse_inventory_excel
        self.assertEqual(p['skus'][I]['brand'], 'NAUTICA')         # NT is Nautica
        self.assertEqual(p['skus'][AFBA]['nj'], 0)                 # an FBA row is never restricted stock
        self.assertEqual(p['skus'][A]['nj'], 7)
        self.assertEqual(p['skus'][DM]['base'], 'TTXXAA003SLS')
        self.assertEqual(p['skus'][E]['base'], 'ZZYYBB004RFS')
        # values come from the first occurrence of each header, never the pivot block
        self.assertEqual({l['sku'] for l in p['lots']}, {A, B, C, DM, DL, E, G, H})
        dm = lot_by(p['lots'], DM)[0]
        self.assertEqual((dm['container'], dm['lot'], dm['date']), ('CARSON TRANSFER', '', date(2025, 10, 5)))
        self.assertEqual(lot_by(p['lots'], A, units=360)[0]['container'], 'ABCU1234567')

    def test_row_count_mismatch_makes_that_warehouse_unusable(self):
        s = std_sheets()
        s['TR'].insert(2, tr('2026-01-01 00:00:00', DL))
        p = ia.parse_workbook(make_wb(s))
        self.assertFalse(p['lotsOk'])
        self.assertIn('TR', p['whErrors'])
        self.assertIn('TR_INVENTORY', p['lotsError'])
        self.assertEqual({l['wh'] for l in p['lots']}, {'JTW', 'DCW'})     # the others still parse
        an = ia.analyze(p, RESTORE, TODAY)
        self.assertIsNone(ia.trend_record(an))
        pay = ia.build_payload(an, True)
        self.assertFalse(pay['lotsOk'])
        chk = next(c for c in pay['checks'] if c['code'] == 'lots_unavailable')
        self.assertEqual((chk['severity'], chk['warehouses']), ('high', ['TR']))
        self.assertFalse(any(c['code'] == 'lot_mismatch' and any(x['wh'] == 'TR' for x in c['pairs'])
                             for c in pay['checks']))

    def test_trailing_rows_without_a_style_are_not_lots(self):
        """The raw JTW listing often ends in rows that hold only 'Available Inv' 0 and SourceFile.
        They are dropped before pairing; a style-less row in the middle still makes JTW unusable."""
        junk = [None, None, None, None, None, None, '0', 'JTW']
        s = std_sheets()
        s['JTW'] += [list(junk), list(junk)]
        s['TR'] += [[None, None, None, None, None, None, 'TR']]
        s['TR_INVENTORY'] += [[None] * 8 + ['TR']]
        p = ia.parse_workbook(make_wb(s))
        self.assertTrue(p['lotsOk'], p['lotsError'])
        self.assertEqual(p['lotTotals'], {'JTW': 1920, 'TR': 300, 'DCW': 48})
        self.assertEqual(p['lotTotals'], p['whTotals'])
        self.assertIsNotNone(ia.trend_record(ia.analyze(p, RESTORE, TODAY)))
        s = std_sheets()
        s['JTW'].insert(3, list(junk))
        s['JTW'] += [list(junk)]
        p = ia.parse_workbook(make_wb(s))
        self.assertIn('JTW', p['whErrors'])

    def test_jtw_style_mismatch(self):
        s = std_sheets()
        s['JTW'][3][2] = 'TTXXAA099SLS'
        p = ia.parse_workbook(make_wb(s))
        self.assertIn('JTW', p['whErrors'])
        self.assertFalse(any(l['wh'] == 'JTW' for l in p['lots']))
        # case and spaces are ignored
        s = std_sheets()
        s['JTW'][3][2] = ' ttxxaa002sls '
        self.assertTrue(ia.parse_workbook(make_wb(s))['lotsOk'])

    def test_missing_sheet_and_missing_columns(self):
        s = std_sheets()
        del s['DCW_INVENTORY']
        p = ia.parse_workbook(make_wb(s))
        self.assertIn('missing', p['whErrors']['DCW'])
        s = std_sheets()
        s['TR'][0] = [h if h != 'Receipt Date' else 'Received On' for h in TR_HDR]
        p = ia.parse_workbook(make_wb(s))
        self.assertIn('Receipt Date', p['whErrors']['TR'])

    def test_old_jtw_format(self):
        """Before Mar 23 2026: 'Date Received'/'Container', every receipt listed, JTW_INVENTORY = the
        lines with a positive balance. A 3/18 date there is a real receipt, never a count date."""
        s = std_sheets()
        s['JTW'] = [JTW_OLD_HDR,
                    [datetime(2024, 5, 1), 'ABCU1234567', A, 30, 'R1', 30, '0', 'JTW'],      # shipped out
                    [datetime(2026, 3, 18), 'ABCU1234567', A, 30, 'R1', 0, '30', 'JTW'],
                    ['2025-02-02 00:00:00', 'ABCU7654321', B, 20, 'R2', 10, '10', 'JTW'],
                    [datetime(2023, 1, 1), 'ABCU7654321', B, 5, 'R2', 5, '0', 'JTW']]
        s['JTW_INVENTORY'] = [JTWI_OLD_HDR,
                              ['ABCU1234567', A, 30, 1080, 'DKNY', 'JTW', None, None, 'X', 'X', 'X', 1],
                              ['ABCU7654321', B, 10, 360, 'NAUTICA', 'JTW', None, None, 'X', 'X', 'X', 1]]
        s['ATS'][2] = ats(B, 'NAUTICA', jtw=360)
        s['ATS'][3] = ats(C, 'CHAPS')
        s['ATS'][8] = ats(G, 'DKNY')
        s['ATS'][9] = ats(H, 'DKNY')
        p = ia.parse_workbook(make_wb(s))
        self.assertTrue(p['lotsOk'], p['lotsError'])
        self.assertEqual(p['jtwFormat'], 'old')
        jl = [l for l in p['lots'] if l['wh'] == 'JTW']
        self.assertEqual([(l['sku'], l['units'], l['date']) for l in jl],
                         [(A, 1080, date(2026, 3, 18)), (B, 360, date(2025, 2, 2))])
        an = ia.analyze(p, RESTORE, TODAY)
        a_lot = lot_by(an['lots'], A, 'JTW')[0]
        self.assertEqual((a_lot['date'], a_lot['flags']), (date(2026, 3, 18), []))
        self.assertEqual(an['restore']['matchedUnits'] + an['restore']['unmatchedUnits'], 0)
        # an Excel serial left as text in Date Received (45990 = 11/29/2025) is a real date
        s['JTW'][3][0] = '45990'
        an = ia.analyze(ia.parse_workbook(make_wb(s)), RESTORE, date(2025, 12, 5))
        b_lot = lot_by(an['lots'], B, 'JTW')[0]
        self.assertEqual((b_lot['date'], b_lot['flags']), (date(2025, 11, 29), []))
        # a balance line that does not match its JTW_INVENTORY line: unusable, never guessed
        s['JTW_INVENTORY'][2][1] = C
        p = ia.parse_workbook(make_wb(s))
        self.assertIn('JTW', p['whErrors'])

    def test_server_rows_from_items(self):
        items = [{'sku': A, 'brand': 'DKNY', 'jtw': 1080, 'tr': 0, 'dcw': 0, 'qa': 12, 'nj': 7, 'abfi': 5,
                  'committed': -100, 'allocated': 0, 'total_ats': 1004, 'receive_date': '06-18-2026'},
                 {'sku': A, 'brand': 'DKNY', 'jtw': 0, 'tr': 0, 'dcw': 0, 'qa': 0, 'nj': 0, 'abfi': 0,
                  'committed': -100, 'allocated': 0, 'total_ats': 0, 'receive_date': ''}]
        rows = ia.sku_rows_from_items(items)
        self.assertEqual(list(rows), [A])
        self.assertEqual((rows[A]['jtw'], rows[A]['committed'], rows[A]['receiveDate']), (1080, -100, '06-18-2026'))
        p = ia.parse_workbook(std_bytes(), sku_rows=rows)
        self.assertEqual({l['sku'] for l in p['lots']}, {A})
        self.assertEqual(p['notInAts'], 1920 - 1080 + 300 + 48 + 3)


class Corrections(unittest.TestCase):
    def setUp(self):
        self.p = ia.parse_workbook(std_bytes())
        self.an = ia.analyze(self.p, RESTORE, TODAY)
        self.lots = self.an['lots']

    def test_bad_date_fixed_from_the_same_container(self):
        fixed = lot_by(self.lots, A, units=720)[0]
        self.assertEqual(fixed['date'], date(2026, 6, 18))
        self.assertEqual(fixed['origDate'], date(1900, 1, 15))
        self.assertEqual(fixed['flags'], ['bad_date_fixed'])

    def test_bad_date_without_a_sibling_is_undated(self):
        g = lot_by(self.lots, G)[0]
        self.assertIsNone(g['date'])
        self.assertEqual((g['flags'], g['origDate']), (['bad_date'], date(2026, 10, 7)))
        h = lot_by(self.lots, H)[0]                      # tomorrow is still valid (time zones)
        self.assertEqual((h['date'], h['flags']), (date(2026, 10, 6), []))

    def test_restore(self):
        b = {l['units']: l for l in lot_by(self.lots, B)}
        self.assertEqual((b[300]['date'], b[300]['flags'], b[300]['origDate']),
                         (date(2025, 1, 10), ['restored'], date(2026, 3, 18)))          # by container + style
        self.assertEqual((b[200]['date'], b[200]['flags']), (date(2026, 3, 18), ['count_date']))   # N/A container
        self.assertEqual((b[100]['date'], b[100]['flags']), (date(2026, 3, 20), ['count_date']))   # newer map date
        c = lot_by(self.lots, C)[0]
        self.assertEqual((c['date'], c['flags']), (date(2025, 2, 1), ['restored']))   # container only
        self.assertEqual(self.an['restore'], {'available': True, 'source': 'synthetic', 'matchedUnits': 450,
                                              'unmatchedUnits': 300})
        self.assertEqual(self.p['lots'][2]['date'], date(2026, 3, 18))                 # the parse is not mutated

    def test_junk_containers_never_match(self):
        for junk in ('NA', 'N/A', 'NONE', '', 'DELIVERY', 'ABCU123456', 'ABC1234567'):
            self.assertFalse(ia.is_container(junk), junk)
        rm = {'pairs': {}, 'containers': {'N/A': '2024-01-01', 'NONE': '2024-01-01', '': '2024-01-01'}}
        lot = dict(self.p['lots'][3], flags=[])           # B, container N/A, count date
        out, st = ia.apply_corrections([lot], rm, TODAY)
        self.assertEqual(out[0]['flags'], ['count_date'])

    def test_no_restore_map(self):
        an = ia.analyze(self.p, None, TODAY)
        self.assertEqual(an['restore'], {'available': False, 'source': None, 'matchedUnits': 0,
                                         'unmatchedUnits': 750})
        self.assertEqual(sorted(l['units'] for l in an['lots'] if 'count_date' in l['flags']), [100, 150, 200, 300])

    def test_reset(self):
        self.assertEqual(lot_by(self.lots, DM)[0]['flags'], ['reset'])     # CARSON TRANSFER
        self.assertEqual(lot_by(self.lots, E)[0]['flags'], ['reset'])      # RMA 12
        self.assertEqual(lot_by(self.lots, DL)[0]['flags'], [])            # PO 77
        hit = ('TRANSFER TO SKUS', 'TRUCK #4', 'INVENTORY 2025', 'INVENTORY2025', 'INV 24', 'INV24', 'CYCLE COUNT',
               'RMA', 'RESTACK', 'AISLE 4', 'INVADJ', 'CARSON')
        miss = ('FARMACY', 'INVOICE', 'ABCU1234567', 'PO 77', 'RMAX', 'INVENTORY')
        for t in hit:
            self.assertTrue(ia.RESET_RE.search(t), t)
        for t in miss:
            self.assertFalse(ia.RESET_RE.search(t), t)

    def test_restore_map_builder(self):
        wb = make_wb({'JTW': [['Date Received', 'Container', 'Style', 'Ballance'],
                              [datetime(2024, 3, 1), ' abcu7654321', 'tt xxaa002sls', '1'],
                              ['2023-01-05 00:00:00', 'ABCU7654321', 'TTXXAA002SLS', '0'],
                              [datetime(2024, 1, 1), 'ABCU7654321', 'TTXXAA009SLS', '0'],
                              [datetime(1900, 1, 1), 'ABCU1111111', 'TTXXAA001SLS', '1'],
                              [datetime(2024, 1, 1), 'N/A', 'TTXXAA001SLS', '1'],
                              [None, 'ABCU2222222', 'TTXXAA001SLS', '1']]})
        m = ia.restore_map_from_jtw_sheet(wb, 'synthetic', '2026-10-05')
        self.assertEqual(m['pairs'], {'ABCU7654321|TTXXAA002SLS': '2023-01-05', 'ABCU7654321|TTXXAA009SLS': '2024-01-01'})
        self.assertEqual(m['containers'], {'ABCU7654321': '2023-01-05'})
        self.assertEqual((m['source'], m['builtAt']), ('synthetic', '2026-10-05'))
        self.assertEqual(ia.validate_seed({'restore': m})[2], None)
        with self.assertRaises(ValueError):
            ia.restore_map_from_jtw_sheet(std_bytes())          # the new JTW format has no Date Received

    def test_lot_mismatch(self):
        s = std_sheets()
        s['ATS'][1] = ats(A, 'DKNY', '06-18-2026', jtw=1100, qa=12, abfi=5, nj=7, committed=-100, total=1004)
        an = ia.analyze(ia.parse_workbook(make_wb(s)), RESTORE, TODAY)
        self.assertEqual(an['mismatches'], [{'sku': A, 'wh': 'JTW', 'lots': 1080, 'ats': 1100}])
        chk = next(c for c in ia.build_payload(an, True)['checks'] if c['code'] == 'lot_mismatch')
        self.assertEqual((chk['severity'], chk['units'], chk['skus']), ('high', 20, [A]))
        self.assertEqual(self.an['mismatches'], [])


class Payload(unittest.TestCase):
    def setUp(self):
        self.an = ia.analyze(ia.parse_workbook(std_bytes()), RESTORE, TODAY)

    def test_admin_payload(self):
        p = ia.build_payload(self.an, True, wh_ats={'TTXXAA001SLS': 900}, rev='r1', as_of='2026-10-05T16:03:22Z',
                             built_at='2026-10-05T16:20:01Z')
        self.assertEqual((p['ready'], p['rev'], p['today'], p['isAdmin'], p['lotsOk']), (True, 'r1', '2026-10-05', True, True))
        self.assertEqual(p['lotCols'], ia.LOT_COLS)
        self.assertEqual(p['skuCols'], ia.SKU_COLS)
        self.assertEqual(p['baseCols'], ['base', 'brand', 'whAts'])
        self.assertEqual(len(p['buckets']), 7)
        self.assertEqual(p['buckets'][4], {'key': 'y1_2', 'label': '1 to 2 years', 'min': 365})
        row = next(r for r in p['skus'] if r[0] == A)
        self.assertEqual(row, [A, A, 'DKNY', 'TT', 1080, 0, 0, 12, 7, 5, -100, 0, '06-18-2026'])
        bases = {b[0]: b for b in p['bases']}
        self.assertEqual(bases[A][2], 900)
        self.assertEqual(bases['TTXXAA003SLS'], ['TTXXAA003SLS', 'USPA', 0])
        fixed = next(l for l in p['lots'] if l[0] == A and l[2] == 720)
        self.assertEqual(fixed, [A, 'JTW', 720, '2026-06-18', 'ABCU1234567', '', ['bad_date_fixed'], '1900-01-15'])
        self.assertEqual(p['excluded'], {'qa': 42, 'nj': 7, 'abfi': 5, 'notInAts': 3})
        self.assertEqual(p['restore'], {'available': True, 'source': 'synthetic', 'matchedUnits': 450, 'unmatchedUnits': 300})
        chk = {c['code']: c for c in p['checks']}
        self.assertEqual(set(chk), {'bad_date', 'count_date', 'restored', 'reset', 'undated', 'not_in_ats'})
        self.assertEqual((chk['bad_date']['units'], chk['bad_date']['skus'], chk['bad_date']['severity']), (770, [A, G], 'high'))
        self.assertEqual((chk['count_date']['units'], chk['count_date']['severity']), (300, 'medium'))
        self.assertEqual((chk['restored']['units'], chk['restored']['skus']), (450, [B, C]))
        self.assertEqual(chk['reset']['byWh'], {'JTW': 0, 'TR': 200, 'DCW': 48})
        self.assertEqual(chk['undated']['parts'], {'qa': 42, 'nj': 7, 'abfi': 5, 'lots': 50})
        self.assertEqual(chk['undated']['units'], 104)
        self.assertEqual(json.loads(json.dumps(p, allow_nan=False)), p)       # JSON clean
        self.assertNotIn('\u2014', json.dumps(p, ensure_ascii=False))

    def test_non_admin_scrub(self):
        p = ia.build_payload(self.an, False, wh_ats={})
        cols = p['skuCols']
        self.assertTrue(all(r[cols.index('nj')] == 0 and r[cols.index('abfi')] == 0 for r in p['skus']))
        self.assertEqual(p['excluded'], {'qa': 42, 'notInAts': 3})
        text = json.dumps(p['checks'])
        self.assertNotIn('NJ', text)
        self.assertNotIn('ABFI', text)
        und = next(c for c in p['checks'] if c['code'] == 'undated')
        self.assertEqual((und['parts'], und['units']), ({'qa': 42, 'lots': 50}, 92))
        self.assertNotIn(AFBA, und['skus'])
        self.assertFalse(p['isAdmin'])

    def test_unknown_wh_ats_is_null(self):
        p = ia.build_payload(self.an, True, wh_ats=None)
        self.assertTrue(all(b[2] is None for b in p['bases']))


class Trend(unittest.TestCase):
    def setUp(self):
        self.an = ia.analyze(ia.parse_workbook(std_bytes()), RESTORE, TODAY)

    def test_record(self):
        r = ia.trend_record(self.an, src='backfill')
        self.assertEqual(r['d'], '2026-10-05')
        self.assertEqual(r['units'], [140, 0, 1080, 300, 650, 0, 48])
        self.assertEqual(r['wh'], {'JTW': [40, 0, 1080, 300, 450, 0, 0], 'TR': [100, 0, 0, 0, 200, 0, 0],
                                   'DCW': [0, 0, 0, 0, 0, 0, 48]})
        self.assertEqual(r['brand'], {'CHAPS': [0, 0, 0, 0, 150, 0, 0], 'DKNY': [40, 0, 1080, 0, 0, 0, 0],
                                      'NAUTICA': [0, 0, 0, 300, 300, 0, 0], 'USPA': [100, 0, 0, 0, 200, 0, 0],
                                      'VD': [0, 0, 0, 0, 0, 0, 48]})
        self.assertEqual(r['prefix'], {'TT': [140, 0, 1080, 300, 650, 0, 0], 'ZZ': [0, 0, 0, 0, 0, 0, 48]})
        self.assertEqual(r['nuri'], [40, 0, 1080, 600, 200, 0, 48])
        self.assertEqual(r['wavg'], round(587830 / 2218, 1))
        self.assertEqual((r['restored'], r['countDate'], r['src']), (450, 300, 'backfill'))
        self.assertEqual(set(r), set(ia.TREND_KEYS))
        self.assertIsNone(ia.validate_trend_record(r))
        self.assertNotIn('nj', json.dumps(r).lower())

    def test_top_prefixes(self):
        by = {f'{chr(65 + i)}A': [i + 1, 0, 0, 0, 0, 0, 0] for i in range(18)}     # AA=1 ... RA=18
        by['Other'] = [5, 0, 0, 0, 0, 0, 0]
        out = ia._top_prefixes(by)
        self.assertEqual(len(out), 16)
        self.assertEqual(out['Other'][0], 5 + 1 + 2 + 3)          # the three smallest fold into Other
        self.assertNotIn('AA', out)

    def test_no_receive_date_column_means_no_nuri(self):
        s = std_sheets()
        s['ATS'] = [row[:3] + row[4:] for row in s['ATS']]          # drop the Receive Date column
        p = ia.parse_workbook(make_wb(s))
        self.assertFalse(p['atsHasReceiveDate'])
        self.assertIsNone(ia.trend_record(ia.analyze(p, RESTORE, TODAY))['nuri'])

    def test_upsert_and_merge(self):
        r1 = ia.trend_record(self.an, src='backfill')
        r0 = dict(r1, d='2026-10-02')
        doc = {'series': [r0, r1], 'dropped': [{'d': '2026-10-04', 'reason': 'x'}], 'updatedAt': 'u0'}
        live = dict(r1, src='live', wavg=1.0)
        new, changed = ia.upsert_trend(doc, live, 'u1')
        self.assertTrue(changed)
        self.assertEqual([x['d'] for x in new['series']], ['2026-10-02', '2026-10-05'])
        self.assertEqual(new['series'][0], r0)                    # another date's backfill record stays
        self.assertEqual(new['series'][1]['src'], 'live')
        self.assertEqual(ia.upsert_trend(new, live, 'u2'), (new, False))
        new2, _ = ia.upsert_trend(new, dict(live, d='2026-10-04'), 'u3')
        self.assertEqual(new2['dropped'], [])                     # a filled day is no longer dropped
        m = ia.merge_trend(doc, [dict(r0, wavg=2.0), dict(r1, d='2026-10-06')], [{'d': '2026-10-03', 'reason': 'y'}],
                           'merge', 'u4')
        self.assertEqual([x['d'] for x in m['series']], ['2026-10-02', '2026-10-05', '2026-10-06'])
        self.assertEqual(m['series'][0]['wavg'], 2.0)
        self.assertEqual([x['d'] for x in m['dropped']], ['2026-10-03', '2026-10-04'])
        rep = ia.merge_trend(doc, [r1], [], 'replace', 'u5')
        self.assertEqual((len(rep['series']), rep['dropped'], rep['updatedAt']), (1, [], 'u5'))


class SeedValidation(unittest.TestCase):
    def setUp(self):
        self.rec = ia.trend_record(ia.analyze(ia.parse_workbook(std_bytes()), RESTORE, TODAY), src='backfill')

    def test_restore(self):
        kind, val, err = ia.validate_seed({'restore': RESTORE})
        self.assertEqual((kind, err), ('restore', None))
        self.assertEqual(val['pairs'], RESTORE['pairs'])
        bad = [{'restore': dict(RESTORE, pairs={'N/A|TTXXAA001SLS': '2024-01-01'})},
               {'restore': dict(RESTORE, pairs={'ABCU1234567|ttxxaa001sls': '2024-01-01'})},
               {'restore': dict(RESTORE, pairs={'ABCU1234567|TT XX': '2024-01-01'})},
               {'restore': dict(RESTORE, containers={'ABCU1234567': '2024-13-01'})},
               {'restore': dict(RESTORE, containers={'NONE': '2024-01-01'})},
               {'restore': dict(RESTORE, extra=1)},
               {'restore': {'pairs': []}},
               {'restore': RESTORE, 'mode': 'merge'}]
        for b in bad:
            self.assertIsNotNone(ia.validate_seed(b)[2], b)

    def test_trend(self):
        body = {'trend': {'series': [self.rec], 'dropped': [{'d': '2026-10-04', 'reason': 'no file'}]}, 'mode': 'merge'}
        kind, val, err = ia.validate_seed(body)
        self.assertEqual((kind, err, val['mode']), ('trend', None, 'merge'))
        self.assertEqual(ia.validate_seed({'trend': {'series': [self.rec]}})[1]['mode'], 'replace')
        r = self.rec
        bad = [dict(r, extra=1), {k: v for k, v in r.items() if k != 'nuri'}, dict(r, units=r['units'][:6]),
               dict(r, units=[True] + r['units'][1:]), dict(r, units=[-1] + r['units'][1:]), dict(r, d='2026-10-5'),
               dict(r, wavg='1.0'), dict(r, wavg=float('nan')), dict(r, brand={'DKNY': [1, 2]}), dict(r, src='other'),
               dict(r, restored=1.5), dict(r, nuri=[0] * 6), dict(r, wh=[])]
        for rec in bad:
            self.assertIsNotNone(ia.validate_seed({'trend': {'series': [rec]}})[2], rec)
        self.assertIsNotNone(ia.validate_seed({'trend': {'series': [r, r]}})[2])            # duplicate date
        self.assertIsNotNone(ia.validate_seed({'trend': {'series': [r]}, 'mode': 'append'})[2])
        self.assertIsNotNone(ia.validate_seed({'trend': {'series': [r], 'dropped': [{'d': 'x', 'reason': 'y'}]}})[2])
        self.assertIsNotNone(ia.validate_seed({'trend': {'rows': []}})[2])
        self.assertIsNotNone(ia.validate_seed({'other': 1})[2])
        self.assertIsNotNone(ia.validate_seed([1])[2])
        self.assertIsNone(ia.validate_seed({'trend': {'series': [dict(r, wavg=None, nuri=None)]}})[2])


# ── app.py routes ────────────────────────────────────────────────────────────

class AgingAppTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from _support import load_app
        cls.a = load_app()
        cls.client = cls.a.app.test_client()

    def setUp(self):
        a = self.a
        self.tmp = tempfile.mkdtemp(prefix='aging-test-')
        self._saved = {k: getattr(a, k) for k in ('_request_identity', '_aging_today', '_wh_split_by_base',
                                                  '_aging_store', '_hold_tick')}
        self._leader = a.worker_leader.is_leader
        self.ident = {'tier': 'staff', 'is_admin': False, 'email': 'zz@example.test'}
        a._request_identity = lambda: self.ident
        a._aging_today = lambda: TODAY
        self.split_calls = []

        def fake_split(bases, customer_view=True, key_fn=None):
            self.split_calls.append(key_fn)
            return {A: {'jtw': 900, 'tr': 0, 'dcw': 0, 'qa': 5, 'total': 905}}
        a._wh_split_by_base = fake_split
        a.worker_leader.is_leader = lambda: True
        with a._inv_lock:
            self._inv_saved = dict(a._inventory)
            a._inventory['items'] = [{'sku': A, 'brand': 'DKNY', 'jtw': 1080}]
        self.reset_state()
        with a._aging_lock:
            a._aging_src.update(rev='rev-1', bytes=std_bytes(), asOf='2026-10-05T16:03:22Z', items=None)

    def tearDown(self):
        a = self.a
        for k in ('thread', 'whatsThread', 'trendThread', 'trendLoadThread'):
            t = a._aging_state.get(k)
            if t is not None:
                t.join(10)
        for k, v in self._saved.items():
            setattr(a, k, v)
        a.worker_leader.is_leader = self._leader
        with a._inv_lock:
            a._inventory.clear()
            a._inventory.update(self._inv_saved)
        self.reset_state()
        shutil.rmtree(self.tmp, ignore_errors=True)

    def reset_state(self):
        a = self.a
        with a._aging_lock:
            a._aging_src.update(rev=None, bytes=None, asOf=None, items=None, at=0.0)
            a._aging_state.update(memo=None, thread=None, fail=None, parsed=None, whats=None, whatsAt=0.0,
                                  whatsRev=None, whatsThread=None, trendThread=None, trendAgain=False,
                                  trendDone=None, trendDoc=None, trendLoadThread=None)

    def use_store(self):
        from cryptography.fernet import Fernet
        from pnl_store import EncryptedStore, LocalDirStore
        st = EncryptedStore(LocalDirStore(self.tmp), Fernet.generate_key().decode('ascii'))
        self.assertTrue(st.ok)
        self.a._aging_store = st
        return st

    def no_store(self):
        from pnl_store import EncryptedStore, LocalDirStore
        self.a._aging_store = EncryptedStore(LocalDirStore(self.tmp), '')

    def wait_trend(self):
        t = self.a._aging_state['trendThread']
        if t is not None:
            t.join(20)
            self.assertFalse(t.is_alive())

    def get_built(self, path='/api/inventory-aging', **kw):
        r = self.client.get(path, **kw)
        if r.status_code == 202:
            self.assertEqual(r.get_json(), {'building': True})
            self.a._aging_state['trendLoadThread' if path.endswith('/trend') else 'thread'].join(20)
            r = self.client.get(path, **kw)
        return r

    # ── access ──
    def test_access(self):
        self.no_store()
        for ident in (None, {'tier': 'oo'}, {'tier': 'factory', 'prefix': 'ZZ'}):
            self.ident = ident
            self.assertEqual(self.client.get('/api/inventory-aging').status_code, 401, ident)
            self.assertEqual(self.client.get('/api/inventory-aging/trend').status_code, 401, ident)
        a = self.a
        self.assertIn('/api/inventory-aging', a._AUTHZ_MACHINE_EXTRA)
        self.assertIn('/api/inventory-aging/trend', a._AUTHZ_MACHINE_EXTRA)
        self.assertNotIn('/api/inventory-aging', a._AUTHZ_CATALOG_READS)
        self.assertNotIn('/api/inventory-aging', a._SCOPE_FILTERS)

    def test_build_in_background_then_serve(self):
        self.no_store()
        r = self.client.get('/api/inventory-aging')
        self.assertEqual(r.status_code, 202)
        self.assertEqual(r.get_json(), {'building': True})
        a = self.a
        a._aging_state['thread'].join(20)
        r = self.client.get('/api/inventory-aging', headers={'Accept-Encoding': 'gzip'})
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.headers.get('Content-Encoding'), 'gzip')
        p = json.loads(gzip.decompress(r.get_data()))
        self.assertEqual((p['rev'], p['asOf'], p['today'], p['isAdmin']), ('rev-1', '2026-10-05T16:03:22Z', '2026-10-05', False))
        self.assertTrue(all(row[8] == 0 and row[9] == 0 for row in p['skus']))
        self.assertNotIn('nj', p['excluded'])
        self.assertEqual(p['restore']['available'], False)                        # no store: count dates stay
        self.assertEqual(p['restore']['unmatchedUnits'], 750)
        self.assertEqual({b[0]: b[2] for b in p['bases']}[A], 900)              # whAts = JTW+TR+DCW, QA left out
        self.assertIsNotNone(self.split_calls[0])                                # the aging base rule is passed
        # no gzip asked: plain JSON; admin and machine get NJ/ABFI
        self.ident = {'tier': 'staff', 'is_admin': True}
        r = self.client.get('/api/inventory-aging')
        self.assertIsNone(r.headers.get('Content-Encoding'))
        p = r.get_json()
        self.assertTrue(p['isAdmin'])
        self.assertEqual(p['excluded']['nj'], 7)
        self.ident = {'tier': 'machine'}
        self.assertTrue(self.client.get('/api/inventory-aging').get_json()['isAdmin'])
        # a new rev is rebuilt (202 again), the old memo is not served for it
        with a._aging_lock:
            a._aging_src.update(rev='rev-2')
        r = self.client.get('/api/inventory-aging')
        self.assertEqual(r.status_code, 202)
        a._aging_state['thread'].join(20)
        self.assertEqual(self.client.get('/api/inventory-aging').get_json()['rev'], 'rev-2')

    def test_a_slow_build_never_holds_the_request(self):
        import threading
        import time as _time
        self.no_store()
        a = self.a
        gate = threading.Event()
        real = a._aging_parsed

        def slow(src):
            gate.wait(20)
            return real(src)
        a._aging_parsed = slow
        try:
            t0 = _time.time()
            for _ in range(3):
                self.assertEqual(self.client.get('/api/inventory-aging').status_code, 202)
            self.assertLess(_time.time() - t0, 2.0)
            self.assertTrue(a._aging_state['thread'].is_alive())       # one build thread, still waiting
        finally:
            gate.set()
            a._aging_state['thread'].join(20)
            a._aging_parsed = real
        self.assertEqual(self.client.get('/api/inventory-aging').status_code, 200)

    def test_stale_whats_and_a_new_restore_map_refresh_in_the_background(self):
        self.use_store()
        a = self.a
        self.ident = {'tier': 'machine'}
        p = self.get_built().get_json()
        self.assertEqual(({b[0]: b[2] for b in p['bases']}[A], p['restore']['available']), (900, False))

        def fake_split(bases, customer_view=True, key_fn=None):
            return {A: {'jtw': 800, 'tr': 0, 'dcw': 0, 'qa': 0, 'total': 800}}
        a._wh_split_by_base = fake_split
        self.assertEqual(self.client.post('/admin/aging/seed', json={'restore': RESTORE}).status_code, 200)
        p = self.client.get('/api/inventory-aging').get_json()          # served at once from the memo
        self.assertEqual({b[0]: b[2] for b in p['bases']}[A], 900)
        a._aging_state['whatsThread'].join(20)
        p = self.client.get('/api/inventory-aging').get_json()
        self.assertEqual({b[0]: b[2] for b in p['bases']}[A], 800)
        self.assertEqual((p['restore']['available'], p['restore']['matchedUnits']), (True, 450))

    def test_failed_build_answers_503_then_retries(self):
        self.no_store()
        a = self.a
        with a._aging_lock:
            a._aging_src.update(bytes=b'not a workbook')
        r = self.client.get('/api/inventory-aging')
        self.assertEqual(r.status_code, 202)
        a._aging_state['thread'].join(20)
        r = self.client.get('/api/inventory-aging')
        self.assertEqual(r.status_code, 503)
        self.assertNotIn('\u2014', r.get_data(as_text=True))
        with a._aging_lock:
            a._aging_state['fail']['at'] -= 120
        self.assertEqual(self.client.get('/api/inventory-aging').status_code, 202)

    # ── store: seed, restore, trend ──
    def test_seed_route(self):
        self.use_store()
        self.ident = {'tier': 'staff', 'is_admin': True}
        self.assertEqual(self.client.post('/admin/aging/seed', json={'restore': RESTORE}).status_code, 403)
        self.ident = {'tier': 'machine'}
        r = self.client.post('/admin/aging/seed', json={'restore': RESTORE})
        self.assertEqual(r.status_code, 200, r.get_data(as_text=True))
        self.assertEqual(r.get_json()['pairs'], 1)
        r = self.client.post('/admin/aging/seed', json={'restore': dict(RESTORE, pairs={'BAD': '2024-01-01'})})
        self.assertEqual(r.status_code, 400)
        r = self.client.post('/admin/aging/seed', data=b'{', content_type='application/json')
        self.assertEqual(r.status_code, 400)
        self.assertEqual(self.a._AGING_SEED_MAX, ia.SEED_MAX_BYTES)
        big = b'{"restore": "' + b'x' * (5 * 1024 * 1024) + b'"}'
        self.assertEqual(self.client.post('/admin/aging/seed', data=big, content_type='application/json').status_code, 413)
        # the stored map is encrypted and is used by the next build
        with open(os.path.join(self.tmp, 'jtw-restore'), 'rb') as f:
            raw = f.read()
        self.assertNotIn(b'ABCU7654321', raw)
        p = self.get_built().get_json()
        self.assertEqual(p['restore'], {'available': True, 'source': 'synthetic', 'matchedUnits': 450,
                                        'unmatchedUnits': 300})

    def test_seed_unconfigured(self):
        self.no_store()
        self.ident = {'tier': 'machine'}
        r = self.client.post('/admin/aging/seed', json={'restore': RESTORE})
        self.assertEqual((r.status_code, r.get_json()), (503, {'configured': False}))
        self.assertEqual(self.client.get('/api/inventory-aging/trend').status_code, 503)
        self.assertEqual(self.client.get('/api/inventory-aging/trend').get_json(), {'configured': False})

    def test_trend_route_and_seed_modes(self):
        self.use_store()
        r = self.client.get('/api/inventory-aging/trend')
        self.assertEqual((r.status_code, r.get_json()), (202, {'building': True}))
        r = self.get_built('/api/inventory-aging/trend')
        self.assertEqual((r.status_code, r.get_json()), (200, {'series': [], 'dropped': [], 'updatedAt': None}))
        rec = ia.trend_record(ia.analyze(ia.parse_workbook(std_bytes()), RESTORE, TODAY), src='backfill')
        self.ident = {'tier': 'machine'}
        r = self.client.post('/admin/aging/seed', json={'trend': {'series': [dict(rec, d='2026-10-01')],
                                                                  'dropped': [{'d': '2026-10-02', 'reason': 'no file'}]},
                                                        'mode': 'replace'})
        self.assertEqual(r.status_code, 200, r.get_data(as_text=True))
        r = self.client.post('/admin/aging/seed', json={'trend': {'series': [rec]}, 'mode': 'merge'})
        self.assertEqual(r.get_json()['records'], 2)
        self.ident = {'tier': 'staff', 'is_admin': False}
        r = self.client.get('/api/inventory-aging/trend', headers={'Accept-Encoding': 'gzip'})
        self.assertEqual(r.headers.get('Content-Encoding'), 'gzip')
        doc = json.loads(gzip.decompress(r.get_data()))
        self.assertEqual([x['d'] for x in doc['series']], ['2026-10-01', '2026-10-05'])
        self.assertEqual(doc['dropped'], [{'d': '2026-10-02', 'reason': 'no file'}])
        self.assertTrue(doc['updatedAt'])
        self.assertEqual(self.client.get('/api/inventory-aging/trend').get_json(), doc)       # served from memory

    def test_trend_route_never_reads_the_store_on_the_request_thread(self):
        import threading
        st = self.use_store()
        rec = ia.trend_record(ia.analyze(ia.parse_workbook(std_bytes()), RESTORE, TODAY), src='backfill')
        st.put_obj('trend', {'series': [rec], 'dropped': [], 'updatedAt': 'u0'})
        calls = []
        for name in ('head', 'get_obj', 'readable'):
            real = getattr(st, name)

            def spy(*args, _real=real, _name=name, **kw):
                calls.append((_name, threading.current_thread() is threading.main_thread()))
                return _real(*args, **kw)
            setattr(st, name, spy)
        a = self.a
        self.assertEqual(self.client.get('/api/inventory-aging/trend').status_code, 202)
        a._aging_state['trendLoadThread'].join(20)
        doc = self.client.get('/api/inventory-aging/trend').get_json()
        self.assertEqual([x['d'] for x in doc['series']], ['2026-10-05'])
        # a stale copy is still served at once; the re-check (HEAD only, same etag) runs in the background
        with a._aging_lock:
            a._aging_state['trendDoc']['at'] -= 3600
        self.assertEqual(self.client.get('/api/inventory-aging/trend').get_json(), doc)
        a._aging_state['trendLoadThread'].join(20)
        self.assertTrue(calls)
        self.assertFalse(any(on_request for _n, on_request in calls), calls)
        self.assertEqual([n for n, _r in calls], ['get_obj', 'head'])
        # a store that fails later: the last good copy keeps being served
        def boom(*_a, **_k):
            raise OSError('synthetic S3 failure')
        st.head = boom
        with a._aging_lock:
            a._aging_state['trendDoc']['at'] -= 3600
        self.assertEqual(self.client.get('/api/inventory-aging/trend').get_json(), doc)
        a._aging_state['trendLoadThread'].join(20)
        self.assertEqual(self.client.get('/api/inventory-aging/trend').get_json(), doc)

    def test_leader_trend_upsert(self):
        st = self.use_store()
        a = self.a
        st.put_obj('jtw-restore', RESTORE)
        old = dict(ia.trend_record(ia.analyze(ia.parse_workbook(std_bytes()), RESTORE, TODAY), src='backfill'),
                   d='2026-10-01')
        st.put_obj('trend', {'series': [old], 'dropped': [], 'updatedAt': 'x'})
        a._aging_after_sync()
        self.wait_trend()
        doc, _ = st.get_obj('trend')
        self.assertEqual([x['d'] for x in doc['series']], ['2026-10-01', '2026-10-05'])
        live = doc['series'][1]
        self.assertEqual((live['src'], live['restored'], live['units']), ('live', 450, [140, 0, 1080, 300, 650, 0, 48]))
        self.assertEqual(doc['series'][0], old)
        etag = st.head('trend')
        a._aging_after_sync()                                       # same rev and day: nothing to write
        self.wait_trend()
        self.assertEqual(st.head('trend'), etag)
        # a non-leader worker never writes
        a.worker_leader.is_leader = lambda: False
        with a._aging_lock:
            a._aging_state['trendDone'] = None
            a._aging_src.update(asOf='2026-10-06T16:00:00Z', rev='rev-9')
        a._aging_after_sync()
        self.assertIsNone(a._aging_state['trendThread'])
        self.assertEqual(st.head('trend'), etag)

    def test_trend_upsert_retries_once_on_conflict(self):
        st = self.use_store()
        from pnl_store import VersionConflict
        real = st.put_obj
        calls = []

        def flaky(name, obj, expected_etag=None, replace_unreadable=False):
            calls.append(name)
            if len(calls) == 1:
                raise VersionConflict('etag')
            return real(name, obj, expected_etag=expected_etag)
        st.put_obj = flaky
        rec = ia.trend_record(ia.analyze(ia.parse_workbook(std_bytes()), RESTORE, TODAY), src='live')
        self.assertTrue(self.a._aging_trend_upsert(rec))
        self.assertEqual(len(calls), 2)
        self.assertEqual(st.get_obj('trend')[0]['series'][0]['d'], '2026-10-05')

    def test_whats_is_unknown_while_the_worker_has_no_inventory(self):
        self.no_store()
        a = self.a
        with a._inv_lock:
            a._inventory['items'] = []          # fresh worker: the startup sync has not landed
        p = self.get_built().get_json()
        self.assertTrue(all(b[2] is None for b in p['bases']))     # null = unknown, never 0
        self.assertEqual(self.split_calls, [])
        with a._inv_lock:
            a._inventory['items'] = [{'sku': A, 'brand': 'DKNY', 'jtw': 1080}]
        # retried after _AGING_WHATS_RETRY seconds, not the full 5 minute TTL
        import time as _time
        memo = a._aging_state['memo']
        self.assertAlmostEqual(_time.time() - memo['whatsAt'], a._AGING_WHATS_TTL - a._AGING_WHATS_RETRY, delta=10)
        with a._aging_lock:
            memo['whatsAt'] -= a._AGING_WHATS_RETRY + 1
        p = self.client.get('/api/inventory-aging').get_json()     # served at once, refresh kicked
        self.assertTrue(all(b[2] is None for b in p['bases']))
        a._aging_state['whatsThread'].join(20)
        p = self.client.get('/api/inventory-aging').get_json()
        self.assertEqual({b[0]: b[2] for b in p['bases']}[A], 900)

    def test_key_rotation_covers_the_aging_objects(self):
        """The aging objects share PNL_DATA_KEYS: the P&L rotate re-encrypts them too, and a seed can
        recover a truly lost key only with replace_unreadable."""
        from cryptography.fernet import Fernet
        from pnl_store import EncryptedStore, LocalDirStore, StoreUnreadable
        a = self.a
        k1, k2 = Fernet.generate_key().decode(), Fernet.generate_key().decode()
        old = EncryptedStore(LocalDirStore(self.tmp), k1)
        old.put_obj('jtw-restore', RESTORE)
        old.put_obj('trend', {'series': [], 'dropped': [], 'updatedAt': 'u0'})
        # registered with the P&L service at boot
        svc = getattr(a, '_pnl_svc', None)
        if svc is not None:
            self.assertIn(a._aging_rotate_store, svc.extra_rotations)
        # step 1 of the key runbook: new key first, old key kept, rotate
        a._aging_store = EncryptedStore(LocalDirStore(self.tmp), k2 + ',' + k1)
        self.assertEqual(a._aging_rotate_store(), {'aging/jtw-restore': 'rotated', 'aging/trend': 'rotated'})
        only_new = EncryptedStore(LocalDirStore(self.tmp), k2)
        self.assertEqual(only_new.get_obj('jtw-restore')[0]['pairs'], RESTORE['pairs'])
        with self.assertRaises(StoreUnreadable):
            EncryptedStore(LocalDirStore(self.tmp), k1).get_obj('trend')
        # a truly lost key: refused without replace_unreadable, recovered with it
        k3 = Fernet.generate_key().decode()
        a._aging_store = EncryptedStore(LocalDirStore(self.tmp), k3)
        self.assertEqual(a._aging_rotate_store(), {'aging/jtw-restore': 'unreadable', 'aging/trend': 'unreadable'})
        self.ident = {'tier': 'machine'}
        r = self.client.post('/admin/aging/seed', json={'restore': RESTORE})
        self.assertEqual((r.status_code, r.get_json()['code']), (409, 'StoreUnreadable'))
        self.assertNotIn('\u2014', r.get_data(as_text=True))
        r = self.client.post('/admin/aging/seed', json={'restore': RESTORE, 'replace_unreadable': 'yes'})
        self.assertEqual(r.status_code, 400)
        r = self.client.post('/admin/aging/seed', json={'trend': {'series': []}, 'mode': 'merge',
                                                        'replace_unreadable': True})
        self.assertEqual(r.status_code, 400)                     # an unreadable trend cannot be merged
        r = self.client.post('/admin/aging/seed', json={'restore': RESTORE, 'replace_unreadable': True})
        self.assertEqual(r.status_code, 200, r.get_data(as_text=True))
        rec = ia.trend_record(ia.analyze(ia.parse_workbook(std_bytes()), RESTORE, TODAY), src='backfill')
        r = self.client.post('/admin/aging/seed', json={'trend': {'series': [rec]}, 'mode': 'replace',
                                                        'replace_unreadable': True})
        self.assertEqual(r.status_code, 200, r.get_data(as_text=True))
        self.assertEqual(a._aging_restore_map()[0]['pairs'], RESTORE['pairs'])
        doc = self.get_built('/api/inventory-aging/trend').get_json()
        self.assertEqual([x['d'] for x in doc['series']], ['2026-10-05'])
        # and a normal seed works again
        self.assertEqual(self.client.post('/admin/aging/seed', json={'restore': RESTORE}).status_code, 200)

    def test_routes_without_the_module(self):
        """app.py boots and answers even when inventory_aging.py is missing from a deploy."""
        a = self.a
        saved = a._aging
        self.use_store()
        try:
            a._aging = None
            self.assertEqual(self.client.get('/api/inventory-aging').status_code, 503)
            self.assertEqual(self.client.get('/api/inventory-aging/trend').status_code, 503)
            self.ident = {'tier': 'machine'}
            self.assertEqual(self.client.post('/admin/aging/seed', json={'restore': RESTORE}).status_code, 503)
            a._aging_note_source('rev-x', b'data', None, [])          # the sync hook is a no-op
            self.assertIsNone(a._aging_state['trendThread'])
        finally:
            a._aging = saved

    def test_sync_keeps_the_workbook_and_never_fails_because_of_it(self):
        a = self.a
        self.no_store()
        data = std_bytes()

        class Resp:
            status_code = 200
            content = data
            text = ''
            headers = {'Dropbox-API-Result': json.dumps({'rev': 'abc123', 'server_modified': '2026-10-05T16:03:22Z'})}
        items = [{'sku': A, 'brand': 'DKNY', 'brand_abbr': 'DKNY', 'brand_full': 'DKNY', 'jtw': 1080, 'tr': 0,
                  'dcw': 0, 'qa': 0, 'nj': 0, 'abfi': 0, 'incoming': 0, 'committed': 0, 'allocated': 0,
                  'total_ats': 1080, 'total_warehouse': 1080, 'container': '', 'receive_date': '', 'lot_number': '',
                  'image': ''}]
        names = ('get_dropbox_token', '_dropbox_inventory_download', 'parse_inventory_excel',
                 '_sync_passes_sanity_check', '_hold_filter_synced_items', '_aging_note_source')
        saved = {n: getattr(a, n) for n in names}
        with a._inv_lock:
            inv_saved = dict(a._inventory)
        acc_saved = dict(a._dbx_inv_rev)
        try:
            a.get_dropbox_token = lambda: 'token'
            a._dropbox_inventory_download = lambda token: Resp()
            a.parse_inventory_excel = lambda b: [dict(i) for i in items]
            a._sync_passes_sanity_check = lambda it: (True, 'passed', {'committed_nonzero_count': 0,
                                                                      'committed_abs_sum': 0})
            a._hold_filter_synced_items = lambda it: (it, {'held': []})
            self.reset_state()
            self.assertTrue(a.sync_from_dropbox())
            self.assertEqual((a._aging_src['rev'], a._aging_src['asOf']), ('abc123', '2026-10-05T16:03:22Z'))
            self.assertEqual(a._aging_src['bytes'], data)
            self.assertEqual(a._aging_src['items'][0]['sku'], A)

            def boom(*_a, **_k):
                raise RuntimeError('synthetic failure')
            a._aging_note_source = boom
            self.assertTrue(a.sync_from_dropbox())
        finally:
            for n, v in saved.items():
                setattr(a, n, v)
            with a._inv_lock:
                a._inventory.clear()
                a._inventory.update(inv_saved)
            a._dbx_inv_rev.update(acc_saved)


class WarehouseSplitKey(unittest.TestCase):
    """_wh_split_by_base keeps its default rollup; key_fn only changes the grouping."""

    @classmethod
    def setUpClass(cls):
        from _support import load_app
        cls.a = load_app()

    def test_key_fn(self):
        a = self.a
        zero = {k: 0 for k in ('jtw', 'tr', 'dcw', 'qa', 'nj', 'abfi', 'incoming', 'committed', 'allocated')}
        merged = {'OLDSTYLE-22-38': dict(zero, jtw=10), 'OLDSTYLE-22-40': dict(zero, jtw=5),
                  A: dict(zero, tr=7), A + '-V': dict(zero, dcw=3), A + '-M': dict(zero, jtw=100)}
        saved = (a._wh_supply_context, a._wh_applied_for_sku)
        try:
            a._wh_supply_context = lambda: {'merged': merged, 'virt': {}}
            a._wh_applied_for_sku = lambda sku, q, ctx, now, today: (0, 'no deduction')
            default = a._wh_split_by_base(['OLDSTYLE', A])
            self.assertEqual(default['OLDSTYLE']['total'], 15)
            self.assertEqual(default[A]['total'], 10)              # sized row left out, -V folded in
            keyed = a._wh_split_by_base(['OLDSTYLE-22-38', 'OLDSTYLE-22-40', A],
                                        key_fn=lambda s: ia.base_style(s, a._is_sized_sku).upper())
            self.assertEqual((keyed['OLDSTYLE-22-38']['jtw'], keyed['OLDSTYLE-22-40']['jtw']), (10, 5))
            self.assertEqual((keyed[A]['tr'], keyed[A]['dcw'], keyed[A]['jtw']), (7, 3, 0))
        finally:
            a._wh_supply_context, a._wh_applied_for_sku = saved


class CatalogReceiveDate(unittest.TestCase):
    """Catalog links (anonymous, scoped) never see receive_date; staff keep it."""

    @classmethod
    def setUpClass(cls):
        from _support import load_app
        cls.a = load_app()
        cls.client = cls.a.app.test_client()

    def item(self):
        return {'sku': A, 'brand': 'DKNY', 'jtw': 10, 'tr': 0, 'dcw': 0, 'qa': 0, 'nj': 0, 'abfi': 0,
                'incoming': 0, 'committed': -2, 'allocated': -1, 'total_ats': 7, 'total_warehouse': 10,
                'container': 'ABCU1234567', 'receive_date': '06-18-2026', 'lot_number': 'LOT 1', 'image': ''}

    def test_anon_row(self):
        r = self.a._anon_inventory_row(self.item())
        self.assertEqual((r['receive_date'], r['container'], r['lot_number']), ('', '', ''))
        self.assertEqual((r['committed'], r['allocated']), (-3, 0))

    def test_scoped_feed(self):
        a = self.a
        saved = a._ledger_rows
        try:
            a._ledger_rows = lambda: [{'style': 'ZZNONE001SLS', 'warehouse': 'JTW', 'units': 1}]
            data = a._flt_inventory({'inventory': [self.item()]}, {'all': True})
            self.assertEqual(data['inventory'][0]['receive_date'], '')
        finally:
            a._ledger_rows = saved

    def test_staff_inventory_keeps_it(self):
        a = self.a
        saved = (a._request_identity, a._hold_tick)
        with a._inv_lock:
            inv_saved = dict(a._inventory)
        try:
            a._request_identity = lambda: {'tier': 'staff', 'is_admin': False}
            a._hold_tick = lambda: None
            with a._inv_lock:
                a._inventory.update(items=[self.item()], item_count=1, last_sync='2099-01-01T00:00:00Z')
            inv = self.client.get('/inventory').get_json()['inventory']
            self.assertEqual(inv[0]['receive_date'], '06-18-2026')
        finally:
            a._request_identity, a._hold_tick = saved
            with a._inv_lock:
                a._inventory.clear()
                a._inventory.update(inv_saved)


if __name__ == '__main__':
    unittest.main()
