"""Pants with a P## serial get their pants photo from the server too (Nov 9 2026).

The desktop catalog already shows a pant's '<prefix>_P<n>' photo (resolveImage pants
rung: US_P2 for any USPA P02 style). The server only took that path when the FABRIC code
was a bottoms code (BA/BC/BR/BH), so U.S. Polo pants (DP / PP / AW, codes shirts use too)
fell to the fabric-blind 'US_02' key and showed no photo in /image (the phone), exports
and decks. A new style number of a known pant colour should show that colour's photo
everywhere, like the desktop. All offline: synthetic ZY customer code, no network.
"""
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _support import load_app


class PantsSerialPhotoTests(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.app = load_app()

    def test_p_serial_pants_skip_the_fabric_blind_keys(self):
        f = self.app._photo_no_serial_fallback
        for sku in ('ZYUSPPP02SRS', 'ZYUSDPP10SRS', 'ZYUSAWP06SRS', 'ROUSDPP13SHS', 'WMUSPPP02SHS-42X30'):
            self.assertTrue(f(sku, 'USPA'), sku)
        self.assertTrue(f('BUVDBAP12SHP', 'VD'), 'bottoms fabric codes keep working')

    def test_shirts_keep_their_serial_photo(self):
        f = self.app._photo_no_serial_fallback
        self.assertFalse(f('ROUSPP010SLS', 'USPA'), 'USPA PP dress shirt')
        self.assertFalse(f('ROUSDP401SLB', 'USPA'))
        self.assertFalse(f('MWDKPK001SLS', 'DKNY'))

    def test_pants_keys(self):
        self.assertEqual(('US_P2', 'US_P02'), self.app.pants_image_keys('ZYUSPPP02SRS', 'USPA'))
        self.assertEqual(('US_P13', 'US_P13'), self.app.pants_image_keys('ZYUSDPP13SHS', 'USPA'))

    def _with_dropbox(self, codes):
        a = self.app
        saved = (dict(a._dropbox_photo_index), a.get_dropbox_image_bytes, a.get_dropbox_thumbnail,
                 a._process_image_from_url, dict(a._style_overrides))
        a._dropbox_photo_index.clear()
        a._dropbox_photo_index.update({c: f'/versa share files/PHOTOS INVENTORY/US POLO/{c}.jpg' for c in codes})
        a._style_overrides.clear()
        asked = []

        def fake_bytes(code):
            asked.append(code)
            return (b'PHOTO:' + code.encode(), 'image/jpeg') if code in codes else (None, None)

        def fake_thumb(code):
            asked.append(code)
            if code not in codes:
                return None
            return {'raw_bytes': b'THUMB:' + code.encode(), 'x_scale': 1, 'y_scale': 1,
                    'x_offset': 0, 'y_offset': 0, 'url': 'dropbox://' + code}

        a.get_dropbox_image_bytes = fake_bytes
        a.get_dropbox_thumbnail = fake_thumb
        a._process_image_from_url = lambda url: None
        return saved, asked

    def _restore(self, saved):
        a = self.app
        idx, b, t, p, ov = saved
        a._dropbox_photo_index.clear(); a._dropbox_photo_index.update(idx)
        a.get_dropbox_image_bytes, a.get_dropbox_thumbnail, a._process_image_from_url = b, t, p
        a._style_overrides.clear(); a._style_overrides.update(ov)

    def test_image_route_serves_the_serial_photo_for_a_new_style(self):
        a = self.app
        saved, asked = self._with_dropbox({'US_P2'})
        orig_get = a.http_requests.get

        class Miss:
            status_code = 404
            headers = {}
            content = b''
        a.http_requests.get = lambda *x, **k: Miss()
        try:
            raw, ct = a._fetch_raw_image('ZYUSPPP02SRS', 'USPA')
        finally:
            a.http_requests.get = orig_get
            self._restore(saved)
        self.assertEqual(b'PHOTO:US_P2', raw)
        self.assertIn('US_P2', asked)

    def test_exports_use_the_serial_photo_for_a_new_style(self):
        a = self.app
        saved, asked = self._with_dropbox({'US_P6'})
        try:
            with a._img_lock:
                a._img_cache.pop('ZYUSPPP06SRS', None)
            got = a.get_image_cached({'sku': 'ZYUSPPP06SRS', 'brand_abbr': 'USPA'}, 'https://s3.test')
        finally:
            with a._img_lock:
                a._img_cache.pop('ZYUSPPP06SRS', None)
            self._restore(saved)
        self.assertIsNotNone(got)
        self.assertEqual(b'THUMB:US_P6', got['image_data'].getvalue())
        self.assertNotIn('US_06', asked, 'never the fabric-blind key')


if __name__ == '__main__':
    unittest.main()
