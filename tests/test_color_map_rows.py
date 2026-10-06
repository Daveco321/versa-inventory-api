"""POST /admin/color-map/rows (Oct 6 2026): the machine key adds or updates rows of the S3 master
color map. Offline: S3 and the workbook are faked. Synthetic keys only."""
import os
import sys
import unittest

import openpyxl

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _support import load_app  # noqa: E402
import swatch_extractor  # noqa: E402


class FakeS3:
    def __init__(self):
        self.copies = []

    def copy_object(self, **kw):
        self.copies.append(kw['Key'])


def master():
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = 'Color Map'
    ws.append(['Key', 'Color_Description'])
    ws.append(['TTXXAA001SLS', 'Old Name'])
    ws.append(['XX_002', 'Navy Solid'])
    return wb


class ColorMapRowsTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.app = load_app()

    def setUp(self):
        a = self.app
        self.saved_app = {n: getattr(a, n) for n in ('get_s3', '_request_identity')}
        self.saved_sx = {n: getattr(swatch_extractor, n) for n in ('_download_color_map', '_upload_color_map')}
        self.s3 = FakeS3()
        self.wb = master()
        self.uploads = []
        a.get_s3 = lambda: self.s3
        self.tier = 'machine'
        a._request_identity = lambda: {'tier': self.tier}
        swatch_extractor._download_color_map = lambda get_s3, bucket: (self.wb, 'e', 1)
        swatch_extractor._upload_color_map = lambda get_s3, bucket, wb: self.uploads.append(wb) or 'e2'
        self.client = a.app.test_client()

    def tearDown(self):
        for n, v in self.saved_app.items():
            setattr(self.app, n, v)
        for n, v in self.saved_sx.items():
            setattr(swatch_extractor, n, v)

    def post(self, body):
        return self.client.post('/admin/color-map/rows', json=body, headers={'X-Api-Key': 'test'})

    def rows(self):
        ws = self.wb['Color Map']
        return {str(r[0].value): r[1].value for r in ws.iter_rows(min_row=2)}

    def test_updates_and_adds_with_a_backup(self):
        r = self.post({'rows': {'TTXXAA001SLS': 'White  Solid', 'leg-008-wht': 'White Solid'}})
        self.assertEqual(r.status_code, 200, r.get_json())
        d = r.get_json()
        self.assertTrue(d['wrote'])
        self.assertEqual(d['updated'], [{'key': 'TTXXAA001SLS', 'old': 'Old Name', 'new': 'White Solid'}])
        self.assertEqual(d['added'], [{'key': 'LEG-008-WHT', 'new': 'White Solid'}])
        self.assertEqual(self.rows()['TTXXAA001SLS'], 'White Solid')
        self.assertEqual(self.rows()['LEG-008-WHT'], 'White Solid')
        self.assertEqual(self.rows()['XX_002'], 'Navy Solid')
        self.assertEqual(len(self.s3.copies), 1)
        self.assertEqual(len(self.uploads), 1)

    def test_dry_run_writes_nothing(self):
        r = self.post({'rows': {'TTXXAA001SLS': 'White Solid'}, 'dry_run': True})
        self.assertEqual(r.status_code, 200)
        self.assertFalse(r.get_json()['wrote'])
        self.assertEqual(self.rows()['TTXXAA001SLS'], 'Old Name')
        self.assertEqual(self.uploads, [])
        self.assertEqual(self.s3.copies, [])

    def test_refuses_other_tiers_and_bad_input(self):
        self.tier = 'staff'
        self.assertEqual(self.post({'rows': {'TTXXAA001SLS': 'X'}}).status_code, 403)
        self.tier = 'machine'
        self.assertEqual(self.post({'rows': {}}).status_code, 400)
        self.assertEqual(self.post({'rows': {'bad key!': 'X'}}).status_code, 400)
        self.assertEqual(self.post({'rows': {'TTXXAA001SLS': ''}}).status_code, 400)
        self.assertEqual(self.uploads, [])


if __name__ == '__main__':
    unittest.main()
