"""The style number decides the brand (Nov 2 2026). A known feed brand that disagrees
with a modern style number gives way to it, and the copies a style then has under one
brand merge into one row. Offline (tests/_support.py); styles and numbers are synthetic
(ZZ customer code) but keep the shapes of the real cases."""
import os
import sys
import unittest
from io import BytesIO

import openpyxl

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _support import load_app  # noqa: E402

HEAD = ['SKU', 'Brand', 'Container', 'Receive Date', 'Lot Number', 'Incoming',
        'JTW', 'TR', 'DCW', 'QA', 'ABFI', 'NJ', 'Committed', 'Allocated', 'Total ATS']


def row(sku, brand, incoming=0, jtw=0, tr=0, committed=0, allocated=0, container='N/A', lot='N/A', received=None):
    total = jtw + tr + incoming + committed + allocated   # committed / allocated are negative in the file
    return [sku, brand, container, received, lot, incoming, jtw, tr, 0, 0, 0, 0, committed, allocated, total]


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


class StyleNumberBrand(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.app = load_app()

    def test_which_style_numbers_decide(self):
        f = self.app._style_number_brand
        self.assertEqual(f('ZZDKPK001SLS'), 'DKNY')
        self.assertEqual(f('zzdkpk002sls-v'), 'DKNY')       # size suffix, any case
        self.assertEqual(f('ZZNTSU048TBP'), 'NAUTICA')      # NT = Nautica overflow
        self.assertEqual(f('ZZUSDPP10SRS'), 'USPA')         # pants serial P10
        self.assertEqual(f('ZZCSAW001SLS'), 'CHAPS')        # the frontends' extra codes
        self.assertEqual(f('ZZBLPQ001SLS'), '')             # BL: Black Label or Bloomingdale's
        self.assertEqual(f('ZZCKPQ001SLS'), '')             # CK: only ever LUCK
        self.assertEqual(f('ZVDNP003C'), '')                # not a modern style number
        self.assertEqual(f('99RB75301'), '')
        self.assertEqual(f('ZZCSHAW131'), '')
        self.assertEqual(f('BEN-22-116'), '')
        self.assertEqual(f('ZZQQPK001SLS'), '')             # QQ is no brand
        self.assertEqual(f('VPNACV201SLS-XL'), 'VERSA')     # VP = Versa Group Purchase, whatever the code
        self.assertEqual(f('LUCK2221SL'), 'LUCKY')

    def test_ledger_brand(self):
        f = self.app._brand_by_style_number
        self.assertEqual(f('ZZDKPK001SLS', 'JNY'), 'DKNY')
        self.assertEqual(f('ZZCHPT632SLP', 'DKNY'), 'CHAPS')
        self.assertEqual(f('ZZDKPK001SLS', 'DKNY'), 'DKNY')
        self.assertEqual(f('ZZGBPJ013SLM', 'GB'), 'GB')      # a key the platform does not know: as typed
        self.assertEqual(f('ZVDNP003C', 'VD'), 'VD')
        self.assertEqual(f('ZZDKPK001SLS', ''), '')

    def test_parse_refiles_and_merges(self):
        items = self.app.parse_inventory_excel(xlsx([
            row('ZZDKPK001SLS', 'DKNY', incoming=300, jtw=100, tr=20, committed=-40, allocated=-60,
                container='ZZCU1234567', lot='ZZ KNIT LOT', received='08-14-2026'),
            row('ZZDKPK001SLS', 'JNY', incoming=50, committed=-40, allocated=-60),
            row('ZZDKPK002SLS-V', 'DKNY', tr=4),
            row('ZZCHPT632SLP', 'DKNY', incoming=5, committed=-50),
            row('ZZCHPT632SLP', 'CHAPS', incoming=10, jtw=70, committed=-50, container='ZZCU7654321'),
            row('ZZNASU375TBP', 'VD', incoming=30),
            row('ZZUSDPP10SRS', 'NAUTICA', jtw=9),
            row('ZVDNP003C', 'VD', jtw=11),
            row('99RB75301', 'BEN', jtw=13),
            row('ZZCSHAW131', 'NAUTICA', jtw=14),
            row('ZZNMPP001RFS', 'NM', jtw=27),
            row('ZZBLPQ001SLS', 'BLACK', jtw=3),
            row('ZZNTSU048TBP', 'NT', incoming=12),
            row('VPNACV201SLS', 'VERSA', jtw=8),
        ]))
        by = {}
        for it in items:
            by.setdefault(it['sku'], []).append(it)
        self.assertEqual(len(items), 12)
        self.assertEqual(by['VPNACV201SLS'][0]['brand'], 'VERSA')

        dk = by['ZZDKPK001SLS']
        self.assertEqual(len(dk), 1)
        dk = dk[0]
        self.assertEqual((dk['brand'], dk['brand_abbr'], dk['brand_full']), ('DKNY', 'DKNY', 'DKNY'))
        self.assertEqual((dk['jtw'], dk['tr'], dk['incoming']), (100, 20, 350))
        self.assertEqual((dk['committed'], dk['allocated']), (-40, -60))
        self.assertEqual(dk['total_ats'], 120 + 350 - 40 - 60)
        self.assertEqual(dk['total_warehouse'], 120)
        self.assertEqual((dk['container'], dk['lot_number'], dk['receive_date']), ('ZZCU1234567', 'ZZ KNIT LOT', '08-14-2026'))

        ch = by['ZZCHPT632SLP']
        self.assertEqual(len(ch), 1)
        ch = ch[0]
        self.assertEqual(ch['brand'], 'CHAPS')
        self.assertEqual((ch['jtw'], ch['incoming'], ch['committed']), (70, 15, -50))
        self.assertEqual(ch['total_ats'], 70 + 15 - 50)
        self.assertEqual(ch['container'], 'ZZCU7654321')     # the lot of the row holding the stock

        brands = {s: v[0]['brand'] for s, v in by.items()}
        self.assertEqual(brands['ZZNASU375TBP'], 'NAUTICA')
        self.assertEqual(brands['ZZUSDPP10SRS'], 'USPA')
        self.assertEqual(brands['ZZDKPK002SLS-V'], 'DKNY')
        self.assertEqual(brands['ZVDNP003C'], 'VD')
        self.assertEqual(brands['99RB75301'], 'BEN')
        self.assertEqual(brands['ZZCSHAW131'], 'NAUTICA')
        self.assertEqual(brands['ZZNMPP001RFS'], 'NM')
        self.assertEqual(brands['ZZBLPQ001SLS'], 'BLACK')
        self.assertEqual(brands['ZZNTSU048TBP'], 'NAUTICA')
        self.assertEqual(by['ZZNASU375TBP'][0]['brand_full'], 'Nautica')

    def test_three_copies_merge(self):
        items = self.app.parse_inventory_excel(xlsx([
            row('ZZDKPK009SLS', 'DKNY', jtw=5, committed=-7, allocated=-2, lot='LOT A'),
            row('ZZDKPK009SLS', 'JNY', jtw=4, incoming=20, committed=-7, allocated=-2, lot='LOT B'),
            row('ZZDKPK009SLS', 'CHAPS', jtw=6, incoming=30, committed=-7, allocated=-2, lot='LOT C'),
        ]))
        self.assertEqual(len(items), 1)
        it = items[0]
        self.assertEqual((it['jtw'], it['incoming'], it['committed'], it['allocated']), (15, 50, -7, -2))
        self.assertEqual(it['total_ats'], 15 + 50 - 7 - 2)
        self.assertEqual(it['lot_number'], 'LOT C')            # the copy holding the most stock

    def test_different_brands_stay_apart(self):
        # a legacy number under two brands: nothing settles it, both rows stay
        items = self.app.parse_inventory_excel(xlsx([
            row('99RB75301', 'BEN', jtw=13),
            row('99RB75301', 'VERSA', jtw=2),
        ]))
        self.assertEqual(sorted(i['brand'] for i in items), ['BEN', 'VERSA'])


if __name__ == '__main__':
    unittest.main()
