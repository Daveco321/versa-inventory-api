"""amazon_pick_ticket.py: the Transcal pick ticket for Amazon shipments, in Transcal's
order-import layout. Synthetic styles, shipments and addresses only."""
import io
import os
import sys
import unittest
from datetime import date

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

import amazon_pick_ticket as T  # noqa: E402

ADDR_1 = T.Address('100 Test Road', '', 'Testville', 'NJ', '07001')
ADDR_2 = T.Address('200 Sample Ave', 'Dock 4', 'Exampleton', 'CA', '90001')
PREPACK = {'AMZZST906SLS-S': 4, 'AMZZST906SLS-M': 8, 'AMZZST906SLS-MT': 4,
           'AMZZST906SLS-L': 4, 'AMZZST906SLS-LT': 8, 'AMZZST906SLS-XL': 8}


def two_shipments(expected=(None, None)):
    s1 = T.Shipment('FBATEST0001', 'TST1', ADDR_1, [
        T.Carton('AMZZSU903SLS-1515.53233', {'AMZZSU903SLS-M-FBA': 36}),
        T.Carton('AMZZLC904SSS', {'AMZZLC904SSS-S': 4, 'AMZZLC904SSS-M': 12, 'AMZZLC904SSS-L': 12, 'AMZZLC904SSS-XL': 8}),
        T.Carton('AMZZSU903SLS-1515.53233', {'AMZZSU903SLS-M-FBA': 36}),
        T.Carton('AMZZLC904SSS', {'AMZZLC904SSS-XL': 8, 'AMZZLC904SSS-L': 12, 'AMZZLC904SSS-M': 12, 'AMZZLC904SSS-S': 4}),
    ], expected_units=expected[0])
    s2 = T.Shipment('FBATEST0002', 'TST2', ADDR_2, [
        T.Carton('AMZZST906SLS', PREPACK),
        T.Carton('AMZZLC904SSS-M', {'AMZZLC904SSS-M': 36}),
    ], expected_units=expected[1])
    return [s1, s2]


class RowTests(unittest.TestCase):

    def test_rows_blocks_letters_and_order(self):
        rows = T.ticket_rows(two_shipments())
        self.assertEqual(5, len(rows))
        self.assertIsNone(rows[2])                                   # blank row between shipments
        self.assertEqual(['AMZZLC904SSS', 'AMZZSU903SLS-1515.53233'], [r['X'] for r in rows[:2]])
        self.assertEqual({'FBATEST0001-A'}, {r['A'] for r in rows[:2]} | {r['B'] for r in rows[:2]})
        self.assertEqual({'FBATEST0002-B'}, {r['A'] for r in rows[3:]})
        self.assertEqual([72, 72, 36, 36], [r['Y'] for r in rows if r])
        self.assertTrue(all(r['C'] == 'UPS' for r in rows if r))
        self.assertEqual(('TST1', '100 Test Road', None, 'Testville', 'NJ', '07001'),
                         tuple(rows[0][k] for k in 'KLMNOP'))
        self.assertEqual('Dock 4', rows[3]['M'])

    def test_notes_say_what_is_inside(self):
        rows = [r for r in T.ticket_rows(two_shipments()) if r]
        self.assertEqual('Amazon SKUs AMZZLC904SSS-S 8 / AMZZLC904SSS-M 24 / AMZZLC904SSS-L 24 / AMZZLC904SSS-XL 16'
                         ' - warehouse SKU is the style, packed in 36pc prepack cartons (S4/M12/L12/XL8) - 2 cartons',
                         rows[0]['I'])
        self.assertEqual('Amazon SKU AMZZSU903SLS-M-FBA (size M) - warehouse SKU differs - packed by size,'
                         ' 36pc per carton (2 cartons)', rows[1]['I'])
        # shipment 2: alphabetical by warehouse SKU, so the LC single-size row comes before the ST prepack
        self.assertEqual('Amazon SKU AMZZLC904SSS-M (size M) - packed by size, 36pc per carton (1 cartons)', rows[2]['I'])
        self.assertIn('(S4/M8/MT4/L4/LT8/XL8) - 1 cartons', rows[3]['I'])

    def test_units_must_match_amazon(self):
        T.ticket_rows(two_shipments(expected=(144, 72)))
        with self.assertRaises(T.TicketError):
            T.ticket_rows(two_shipments(expected=(144, 108)))

    def test_bad_input_is_refused(self):
        with self.assertRaises(T.TicketError):
            T.Carton('AMZZSU903SLS', {})
        with self.assertRaises(T.TicketError):
            T.Carton('AMZZSU903SLS', {'AMZZSU903SLS-M': 0})
        with self.assertRaises(T.TicketError):
            T.Shipment('FBATEST0003', 'TST3', T.Address('', '', 'Testville', 'NJ', '07001'),
                       [T.Carton('AMZZLC904SSS-M', {'AMZZLC904SSS-M': 36})])
        with self.assertRaises(T.TicketError):
            T.Shipment('FBATEST0003', 'TST3', ADDR_1, [])


class WorkbookTests(unittest.TestCase):

    def test_written_into_the_transcal_template(self):
        import openpyxl
        data = T.build_ticket(two_shipments())
        wb = openpyxl.load_workbook(io.BytesIO(data))
        self.assertEqual(['Order Import Template', 'Instructions', 'Country Values', 'State Values'], wb.sheetnames)
        ws = wb['Order Import Template']
        self.assertEqual('SKU', ws['X2'].value)
        self.assertEqual('Quantity', ws['Y2'].value)
        self.assertEqual('FBATEST0001-A', ws['A3'].value)
        self.assertEqual('AMZZLC904SSS', ws['X3'].value)
        self.assertEqual(72, ws['Y3'].value)
        self.assertEqual('07001', ws['P3'].value)                   # leading zero kept
        self.assertEqual('@', ws['P3'].number_format)
        self.assertIsNone(ws['A5'].value)                            # blank row
        self.assertEqual('FBATEST0002-B', ws['A6'].value)
        self.assertEqual(7, ws.max_row)

    def test_filename(self):
        self.assertEqual('Transcal Pick Ticket 11.23 - Amazon (2 FBA).xlsx',
                         T.ticket_filename(date(2026, 11, 23), 'Amazon (2 FBA)'))
        self.assertEqual('Transcal Pick Ticket 1.5 - Amazon.xlsx', T.ticket_filename(date(2027, 1, 5), 'Ama/zon'))


if __name__ == '__main__':
    unittest.main()
