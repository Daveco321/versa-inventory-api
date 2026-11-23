"""amazon_proposal.py: the email David gets before any Amazon shipment, and its Amazon SKU list
attachment (his Sep 4 ship-tool export columns plus why). Synthetic styles only."""
import io
import os
import sys
import unittest
from datetime import date

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

import amazon_planner as P  # noqa: E402
import amazon_proposal as M  # noqa: E402
from tests.test_amazon_planner import BASE, SLIM, listings  # noqa: E402


def sample_plan():
    stock = [P.StockRow(BASE, 360, 0), P.StockRow(f'{BASE}-1515.53233', 144, 0)]
    return P.plan_shipment(listings(), stock, lambda b: SLIM)


class SkuListTests(unittest.TestCase):

    def test_columns_rows_and_totals(self):
        import openpyxl
        meta = {BASE: {'brand': 'Testbrand', 'color': 'White Solid', 'fit': 'Slim Fit', 'fabrication': '95/5 Stretch'}}
        ws = openpyxl.load_workbook(io.BytesIO(M.sku_list_xlsx(sample_plan(), meta.get))).active
        self.assertEqual(list(M.COLUMNS), [c.value for c in ws[1]])
        rows = [[c.value for c in r] for r in ws.iter_rows(min_row=2)]
        body, total = rows[:-1], rows[-1]
        self.assertEqual(7, len(body))                              # 6 prepack sizes + 1 single-size line
        self.assertEqual(BASE, body[0][0])
        self.assertEqual('Testbrand', body[0][2])
        self.assertTrue(body[0][11].startswith('Sold 320'))         # the reason sits on the style's first row
        self.assertTrue(all(r[11] in (None, '') for r in body[1:]))
        self.assertEqual(['Total', 504, 14], [total[0], total[6], total[7]])   # 10 prepack + 4 single boxes
        single = [r for r in body if r[0] == f'{BASE}-1515.53233'][0]
        self.assertEqual([144, 4, 10, 100, 3], single[6:11])

    def test_filename(self):
        self.assertEqual('Amazon SKU List 11.23.xlsx', M.sku_list_filename(date(2026, 11, 23)))


class EmailTests(unittest.TestCase):

    def test_subject_body_and_reply_instructions(self):
        subject, body, text = M.proposal_email(sample_plan(), date(2026, 11, 23),
                                               fcs=[('FBATEST0001', 'TST1', 14, 504)])
        self.assertEqual('Amazon shipment for your OK: 14 cartons, 504 units (11/23)', subject)
        self.assertIn('Reply YES to go ahead', text)
        self.assertIn('Nothing goes to Amazon or Transcal until you reply YES', text)
        self.assertIn('FBATEST0001 to TST1: 14 cartons, 504 units', text)
        self.assertIn(f'{BASE}: 14 cartons (10 prepack + 4 single-size), 504 units.', text)
        for part in (subject, body, text):
            self.assertNotIn('—', part)                        # David: never em dashes
        self.assertIn('<table', body)

    def test_html_is_escaped(self):
        plan = sample_plan()
        plan['styles'][0]['why'] = 'Sold <b>a lot</b> & more'
        _s, body, _t = M.proposal_email(plan)
        self.assertIn('Sold &lt;b&gt;a lot&lt;/b&gt; &amp; more', body)


if __name__ == '__main__':
    unittest.main()
