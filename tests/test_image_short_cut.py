"""/image never borrows a photo from a cut shorter than 8 characters (Nov 17 2026).

The phone asks /image/<sku cut at the first dash>?sku=<full sku>. For older dashed keys the
cut is not a style (AC-KN-620 -> 'AC'), and a stray AC.jpg became the photo of every
American Crew card on the phone. The desktop never borrows from such a cut. Modern size
rows and stocked short keys (CTH08 tie sets) keep their base photo.
Synthetic keys; no test touches the network (tests._support blocks sockets).

Run: python -B -m unittest tests.test_image_short_cut
"""
import io
import unittest

from tests._support import load_app


def _jpeg():
    from PIL import Image
    buf = io.BytesIO()
    Image.new('RGB', (40, 40), 'tan').save(buf, 'JPEG')
    return buf.getvalue()


class ImageShortCut(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.app = load_app()
        cls.client = cls.app.app.test_client()

    def keep(self, obj, name):
        self.addCleanup(setattr, obj, name, getattr(obj, name))

    def setUp(self):
        a = self.app
        self.keep(a, '_worker_initialized')
        a._worker_initialized = True
        import email_agent
        self.keep(email_agent, '_sweeper_started')
        email_agent._sweeper_started = True
        a._thumb_drop()
        self.addCleanup(a._thumb_drop)
        saved_web = dict(a._web_img_cache)
        self.addCleanup(lambda: (a._web_img_cache.clear(), a._web_img_cache.update(saved_web)))
        a._web_img_cache.clear()
        self.calls = []
        photo = _jpeg()

        def fake_fetch(base, brand):
            self.calls.append(base)
            return photo, 'image/jpeg'
        self.keep(a, '_fetch_raw_image')
        a._fetch_raw_image = fake_fetch
        self.keep(a, '_full_sku_override_photo')
        a._full_sku_override_photo = lambda full: None          # no photo of the row's own
        saved_inv = a._inventory.get('items')
        self.addCleanup(lambda: a._inventory.__setitem__('items', saved_inv))
        a._inventory['items'] = [{'sku': 'ZZ-KN-620', 'brand_abbr': 'AMERICA'},
                                 {'sku': 'ZZTH08', 'brand_abbr': 'CHAPS'},
                                 {'sku': 'ZZTH08-V', 'brand_abbr': 'CHAPS'},
                                 {'sku': 'ZZNASU901SLP-L', 'brand_abbr': 'NAUTICA'}]

    def test_short_cut_that_is_not_a_style_gets_no_photo(self):
        r = self.client.get('/image/ZZ?brand=AMERICA&sku=ZZ-KN-620')
        self.assertEqual(r.status_code, 404)
        self.assertEqual(self.calls, [])

    def test_stocked_short_key_keeps_its_photo(self):
        r = self.client.get('/image/ZZTH08?brand=CHAPS&sku=ZZTH08-V')
        self.assertEqual(r.status_code, 200)
        self.assertEqual(self.calls, ['ZZTH08'])

    def test_modern_size_row_keeps_its_base_photo(self):
        r = self.client.get('/image/ZZNASU901SLP?brand=NAUTICA&sku=ZZNASU901SLP-L')
        self.assertEqual(r.status_code, 200)
        self.assertEqual(self.calls, ['ZZNASU901SLP'])

    def test_direct_request_without_sku_is_unchanged(self):
        r = self.client.get('/image/ZZ?brand=AMERICA')
        self.assertEqual(r.status_code, 200)


if __name__ == '__main__':
    unittest.main()
