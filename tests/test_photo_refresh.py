"""POST /admin/photos/refresh (Sep 30 2026): a photo replaced in the Dropbox PHOTOS
INVENTORY folder is pushed through the disk cache and the S3 DROPBOX_SYNC mirror,
and named style overrides lose their photo. Offline: Dropbox, S3 and CloudFront
are fakes."""
import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _support import load_app  # noqa: E402

PNG = b'\x89PNG\r\n\x1a\n' + b'fake-png-body'
JPG = b'\xff\xd8\xff\xe0' + b'fake-jpg-body'


class FakeS3:
    def __init__(self):
        self.puts, self.deletes = [], []

    def put_object(self, **kw):
        self.puts.append(kw)

    def delete_object(self, **kw):
        self.deletes.append(kw['Key'])


class PhotoRefreshTests(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.app = load_app()

    def setUp(self):
        a = self.app
        self.saved = {n: getattr(a, n) for n in (
            '_download_dropbox_file', 'get_s3', '_invalidate_cloudfront', 'sync_dropbox_photos',
            'DROPBOX_DISK_CACHE', 'DROPBOX_THUMB_CACHE_DIR', '_dropbox_photo_index', '_inventory',
            'load_overrides_from_s3', 'save_overrides_to_s3', '_backup_overrides_to_s3',
            'trigger_background_generation', '_style_overrides', '_s3_overrides_etag')}
        self.tmp = tempfile.mkdtemp()
        self.s3 = FakeS3()
        self.saves = []
        a.DROPBOX_DISK_CACHE = os.path.join(self.tmp, 'raw')
        a.DROPBOX_THUMB_CACHE_DIR = os.path.join(self.tmp, 'thumbs')
        os.makedirs(a.DROPBOX_DISK_CACHE)
        os.makedirs(a.DROPBOX_THUMB_CACHE_DIR)
        a.get_s3 = lambda: self.s3
        a._invalidate_cloudfront = lambda paths: None
        a.sync_dropbox_photos = lambda: None
        a._dropbox_photo_index = {'BE_545': '/Versa Share Files/PHOTOS INVENTORY/BEN SHERMAN/BE_545.jpg'}
        a._inventory = {'items': [{'sku': 'TMBEPU545SLS', 'brand_abbr': 'BEN'},
                                  {'sku': 'TMBEAW545SLD-V', 'brand_abbr': 'BEN'},
                                  {'sku': 'TMBEPU546SLS', 'brand_abbr': 'BEN'}]}
        a.load_overrides_from_s3 = lambda: None
        a.save_overrides_to_s3 = lambda snap=None: self.saves.append(dict(snap)) or True
        a._backup_overrides_to_s3 = lambda d: None
        a.trigger_background_generation = lambda: None
        a._s3_overrides_etag = 'etag'
        self.client = a.app.test_client()

    def tearDown(self):
        for n, v in self.saved.items():
            setattr(self.app, n, v)

    def test_png_saved_as_jpg_replaces_every_cached_copy(self):
        a = self.app
        a._download_dropbox_file = lambda path: (PNG, 'image/jpeg')   # the name says .jpg
        disk = a._get_disk_cache_path('BE_545')
        for stale in (disk + '.jpg', a._get_thumb_disk_path('BE_545')):
            with open(stale, 'wb') as f:
                f.write(b'old check shirt')
        a._dropbox_thumb_cache['BE_545'] = {'raw_bytes': b'old'}
        res = a._refresh_photo_code('be-545')
        self.assertTrue(res['ok'], res)
        self.assertEqual(res['type'], 'image/png')
        with open(disk + '.png', 'rb') as f:
            self.assertEqual(f.read(), PNG)
        self.assertFalse(os.path.exists(disk + '.jpg'))
        self.assertFalse(os.path.exists(a._get_thumb_disk_path('BE_545')))
        self.assertNotIn('BE_545', a._dropbox_thumb_cache)
        self.assertEqual(self.s3.deletes, ['ALL INVENTORY Photos/DROPBOX_SYNC/BE_545.jpg'])
        self.assertEqual([(p['Key'], p['ContentType']) for p in self.s3.puts],
                         [('ALL INVENTORY Photos/DROPBOX_SYNC/BE_545.png', 'image/png')])
        self.assertIn('TMBEPU545SLS', res['styles'])
        self.assertIn('TMBEAW545SLD', res['styles'])
        self.assertNotIn('TMBEPU546SLS', res['styles'])

    def test_jpeg_removes_the_old_png_mirror_copy(self):
        self.app._download_dropbox_file = lambda path: (JPG, 'image/jpeg')
        res = self.app._refresh_photo_code('BE_545')
        self.assertEqual(self.s3.deletes, ['ALL INVENTORY Photos/DROPBOX_SYNC/BE_545.png'])
        self.assertEqual(self.s3.puts[0]['Key'], 'ALL INVENTORY Photos/DROPBOX_SYNC/BE_545.jpg')
        self.assertTrue(res['ok'])

    def test_unknown_code_and_failed_download_touch_nothing(self):
        self.app._download_dropbox_file = lambda path: (None, None)
        self.assertFalse(self.app._refresh_photo_code('BE_999')['ok'])
        self.assertFalse(self.app._refresh_photo_code('BE_545')['ok'])
        self.assertFalse(self.app._refresh_photo_code('../etc')['ok'])
        self.assertEqual((self.s3.puts, self.s3.deletes), ([], []))

    def test_clear_override_images(self):
        a = self.app
        a._style_overrides = {'TMBEPU545SLS': {'image': 'data:image/jpeg;base64,xx'},
                              'TMBEPU545SLS-V': {'image': '/image/ovr/abc', 'fit': 'Slim Fit'},
                              'OTHER': {'image': 'data:image/jpeg;base64,yy'}}
        res = a._clear_override_images(['tmbepu545sls', 'TMBEPU545SLS-V', 'NOT-THERE'])
        self.assertEqual(res, {'ok': True, 'cleared': ['TMBEPU545SLS', 'TMBEPU545SLS-V']})
        saved = self.saves[-1]
        self.assertNotIn('TMBEPU545SLS', saved)
        self.assertEqual(saved['TMBEPU545SLS-V'], {'fit': 'Slim Fit'})
        self.assertEqual(saved['OTHER'], {'image': 'data:image/jpeg;base64,yy'})

    def test_clear_refuses_without_loaded_overrides(self):
        self.app._style_overrides, self.app._s3_overrides_etag = {}, None
        self.assertFalse(self.app._clear_override_images(['TMBEPU545SLS'])['ok'])
        self.assertEqual(self.saves, [])

    def test_route_validates_input(self):
        r = self.client.post('/admin/photos/refresh', json={'codes': 'BE_545'})
        self.assertEqual(r.status_code, 400)
        self.app._download_dropbox_file = lambda path: (JPG, 'image/jpeg')
        r = self.client.post('/admin/photos/refresh', json={'codes': ['BE_545']})
        self.assertEqual(r.status_code, 200)
        self.assertTrue(r.get_json()['codes'][0]['ok'])


if __name__ == '__main__':
    unittest.main()
