"""'-FBA' rows count in NJ like every other row (David, Nov 17 2026).

parse_inventory_excel zeroed NJ and ABFI on every '-FBA' SKU and backed the units out
of Total ATS, so Amazon carton SKUs at the NJ 3PL showed 0 in NJ on the cards while
the 3PL file and the A2000 ATS file both had them in stock. Negatives still never
count and are still backed out of Total ATS. Synthetic styles (ZZ customer code);
no test touches the network (tests._support blocks sockets before app.py loads).

Run: python -B -m unittest tests.test_nj_fba_rows
"""
import unittest
from io import BytesIO

import openpyxl

from tests._support import load_app

HEAD = ['SKU', 'Brand', 'Container', 'Receive Date', 'Lot Number', 'Incoming',
        'JTW', 'TR', 'DCW', 'QA', 'ABFI', 'NJ', 'Committed', 'Allocated', 'Total ATS']


def row(sku, brand, jtw=0, abfi=0, nj=0, committed=0):
    return [sku, brand, 'N/A', None, 'N/A', 0, jtw, 0, 0, 0, abfi, nj, committed, 0,
            jtw + abfi + nj + committed]


def xlsx(rows):
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = 'ATS'
    ws.append(HEAD)
    for r in rows:
        ws.append(r)
    buf = BytesIO()
    wb.save(buf)
    return buf.getvalue()


class NjFbaRows(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.app = load_app()

    def parse(self, rows):
        return {it['sku']: it for it in self.app.parse_inventory_excel(xlsx(rows))}

    def test_fba_rows_keep_nj_and_count_like_the_file(self):
        out = self.parse([
            row('ZZNASU901SLP-L-FBA', 'NAUTICA', nj=19),
            row('ZZNASU901SLP-XL-FBA', 'NAUTICA', jtw=5, nj=40, committed=-5),
            row('ZZNASU901SLP-M-FBA', 'NAUTICA', abfi=12),
        ])
        l, xl, m = out['ZZNASU901SLP-L-FBA'], out['ZZNASU901SLP-XL-FBA'], out['ZZNASU901SLP-M-FBA']
        self.assertEqual((l['nj'], l['total_warehouse'], l['total_ats']), (19, 19, 19))
        self.assertEqual((xl['nj'], xl['total_warehouse'], xl['total_ats']), (40, 45, 40))
        self.assertEqual((m['abfi'], m['total_warehouse'], m['total_ats']), (12, 12, 12))

    def test_negatives_still_never_count(self):
        out = self.parse([
            row('ZZNASU902SLP', 'NAUTICA', jtw=100, nj=-9),
            row('ZZNASU902SLP-S-FBA', 'NAUTICA', nj=-3),
        ])
        base, fba = out['ZZNASU902SLP'], out['ZZNASU902SLP-S-FBA']
        self.assertEqual((base['nj'], base['total_warehouse'], base['total_ats']), (0, 100, 100))
        self.assertEqual((fba['nj'], fba['total_ats']), (0, 0))

    def test_admin_report_counts_fba_units_as_nj(self):
        self.parse([row('ZZNASU903SLP-L-FBA', 'NAUTICA', nj=20),
                    row('ZZNASU903SLP', 'NAUTICA', nj=-4)])
        rep = dict(self.app._nj_report_pending)
        self.assertEqual(rep['units_by_warehouse'], {'nj': 20, 'abfi': 0})
        self.assertEqual((rep['negative_rows'], rep['negative_units']), (1, -4))
        self.assertNotIn('fba_units', rep)
        self.assertEqual([e['reason'] for e in rep['excluded']], ['negative on-hand'])


if __name__ == '__main__':
    unittest.main()
