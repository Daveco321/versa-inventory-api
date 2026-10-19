"""Photo, colour and override fixes (David, Oct 19 2026). Offline: Dropbox, S3 and the
image fetches are fakes; synthetic styles only (serial 9xx, fabric ZZ, made-up codes).

1. Sportswear (polos, rugbys, tees) never take the fabric-blind '<brand>_<serial>' photo,
   which is a dress shirt; the '=SPORTSWEAR' Dropbox folder is indexed.
2. Exports resolve a row's OWN photo (a -V, a dashed style) before its style's.
3. Photos replaced in Dropbox under the same name are refreshed (content_hash).
4. A variant's own colour override wins over a base record without a colour (TJX email).
5. Override saves write conditionally and re-merge when another save landed first.
"""
import hashlib
import json
import os
import sys
import tempfile
import unittest
from io import BytesIO

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _support import load_app  # noqa: E402


def _res(tag):
    return {'raw_bytes': tag.encode(), 'x_scale': 1, 'y_scale': 1, 'x_offset': 0, 'y_offset': 0, 'url': tag}


class _Patch(unittest.TestCase):
    NAMES = ()

    @classmethod
    def setUpClass(cls):
        cls.app = load_app()

    def setUp(self):
        self.saved = {n: getattr(self.app, n) for n in self.NAMES}

    def tearDown(self):
        for n, v in self.saved.items():
            setattr(self.app, n, v)


class SportswearPhotos(_Patch):
    NAMES = ('http_requests', 'get_dropbox_token', '_dropbox_photo_index', '_dropbox_photo_hashes',
             '_sportswear_photo_index', '_thumb_drop')

    def test_sportswear_skips_the_brand_serial_photo(self):
        a = self.app
        self.assertTrue(a._photo_no_serial_fallback('ROGBPL940SSR', 'BEENE'))     # pique polo
        self.assertTrue(a._photo_no_serial_fallback('ROGBPO939SSU', 'BEENE'))
        self.assertFalse(a._photo_no_serial_fallback('RONASU901SLS', 'NAUTICA'))  # dress shirt

    def _listing(self, entries, has_more=False, fail_continue=False):
        a = self.app

        class R:
            def __init__(s, code, js):
                s.status_code, s._js, s.text = code, js, ''

            def json(s):
                return s._js

        calls = []

        class H:
            @staticmethod
            def post(url, headers=None, json=None, timeout=None):
                calls.append(url)
                if url.endswith('/continue'):
                    return R(500, {}) if fail_continue else R(200, {'entries': [], 'has_more': False})
                return R(200, {'entries': entries, 'has_more': has_more, 'cursor': 'c1'})
        a.http_requests = H
        a.get_dropbox_token = lambda: 'tok'
        a._thumb_drop = lambda names=None: None
        return calls

    @staticmethod
    def _file(path, h):
        return {'.tag': 'file', 'name': path.rsplit('/', 1)[-1], 'path_lower': path.lower(),
                'path_display': path, 'content_hash': h}

    def test_equals_sportswear_folder_is_indexed_with_hashes(self):
        a = self.app
        self._listing([self._file('/P/GEOFFREY BEENE/=SPORTSWEAR/GBPJ912.jpeg', 'h1'),
                       self._file('/P/GEOFFREY BEENE/GB_912.jpg', 'h2')])
        a.sync_dropbox_photos()
        self.assertIn('GBPJ912', a._sportswear_photo_index)
        self.assertEqual(a._dropbox_photo_hashes.get('GB_912'), 'h2')
        self.assertEqual(a._dropbox_photo_hashes.get('GBPJ912'), 'h1')

    def test_failed_continue_keeps_the_previous_index(self):
        a = self.app
        a._dropbox_photo_index = {'OLD_901': '/P/OLD_901.jpg'}
        self._listing([self._file('/P/NEW_901.jpg', 'h')], has_more=True, fail_continue=True)
        a.sync_dropbox_photos()
        self.assertEqual(a._dropbox_photo_index, {'OLD_901': '/P/OLD_901.jpg'})


class ExportPhotosPerSku(_Patch):
    NAMES = ('_process_image_from_url', '_style_overrides', '_img_cache', 'find_sportswear_image_match',
             'get_dropbox_thumbnail')

    def setUp(self):
        super().setUp()
        a = self.app
        a._img_cache = {}
        a._style_overrides = {}
        a.find_sportswear_image_match = lambda *x, **k: None
        a.get_dropbox_thumbnail = lambda code, *x, **k: None
        files = {'ZZD-22-901', 'ZZD-22-902', 'TJZZXX901SLS', 'TJZZXX901SLS-V', 'TJZZXX902SLS'}

        def fake(url):
            name = url.rsplit('/', 1)[-1].rsplit('.', 1)[0]
            return _res(name) if '/STYLE' in url.replace('%20', '+').upper() and name in files else None
        a._process_image_from_url = fake

    def url_of(self, sku):
        img = self.app.get_image_cached({'sku': sku, 'brand_abbr': 'DKNY'}, '')
        return img and img['url']

    def test_dashed_styles_keep_their_own_photos(self):
        self.assertEqual(self.url_of('ZZD-22-901'), 'ZZD-22-901')
        self.assertEqual(self.url_of('ZZD-22-902'), 'ZZD-22-902')

    def test_variant_own_photo_then_base(self):
        self.assertEqual(self.url_of('TJZZXX901SLS'), 'TJZZXX901SLS')
        self.assertEqual(self.url_of('TJZZXX901SLS-V'), 'TJZZXX901SLS-V')   # its own file
        self.assertEqual(self.url_of('TJZZXX902SLS-V'), 'TJZZXX902SLS')     # inherits the style's

    def test_download_dedupes_by_full_sku(self):
        a = self.app
        items = [{'sku': 'TJZZXX901SLS', 'brand_abbr': 'DKNY'}, {'sku': 'TJZZXX901SLS-V', 'brand_abbr': 'DKNY'},
                 {'sku': 'ZZD-22-901', 'brand_abbr': 'DKNY'}, {'sku': 'ZZD-22-902', 'brand_abbr': 'DKNY'}]
        got = a.download_images_for_items(items, '')
        self.assertEqual([got[i]['url'] for i in range(4)],
                         ['TJZZXX901SLS', 'TJZZXX901SLS-V', 'ZZD-22-901', 'ZZD-22-902'])


class DropboxFreshness(_Patch):
    NAMES = ('get_s3', '_dropbox_photo_hashes', 'DROPBOX_DISK_CACHE', '_refresh_photo_code')

    def test_dropbox_hash_matches_the_published_algorithm(self):
        data = b'x' * (4 * 1024 * 1024 + 10)
        blocks = hashlib.sha256(data[:4 * 1024 * 1024]).digest() + hashlib.sha256(data[4 * 1024 * 1024:]).digest()
        self.assertEqual(self.app._dropbox_content_hash(data), hashlib.sha256(blocks).hexdigest())

    def test_only_changed_photos_are_refreshed(self):
        a = self.app
        tmp = tempfile.mkdtemp()
        a.DROPBOX_DISK_CACHE = tmp
        same, old = b'same-bytes', b'old-bytes'
        open(os.path.join(tmp, 'ZZ_901.jpg'), 'wb').write(same)   # disk == Dropbox == S3
        open(os.path.join(tmp, 'ZZ_902.jpg'), 'wb').write(old)    # Dropbox has a new file
        h = a._dropbox_content_hash
        a._dropbox_photo_hashes = {'ZZ_901': h(same), 'ZZ_902': h(b'new-bytes'), 'ZZ_903': 'h903'}
        manifest = {'ZZ_903': 'h903-older'}                       # known, changed since
        puts = []

        class S3:
            def get_object(s, Bucket, Key):
                return {'Body': BytesIO(json.dumps(manifest).encode())}

            def get_paginator(s, name):
                class P:
                    def paginate(p, **kw):
                        md5 = lambda b: hashlib.md5(b).hexdigest()
                        yield {'Contents': [{'Key': 'x/ZZ_901.jpg', 'ETag': '"%s"' % md5(same)},
                                            {'Key': 'x/ZZ_902.jpg', 'ETag': '"%s"' % md5(old)},
                                            {'Key': 'x/ZZ_903.jpg', 'ETag': '"e"'}]}
                return P()

            def put_object(s, **kw):
                puts.append(kw)
        a.get_s3 = lambda: S3()
        refreshed = []
        a._refresh_photo_code = lambda code: refreshed.append(code) or {'ok': True}
        a._refresh_changed_dropbox_photos()
        self.assertEqual(sorted(refreshed), ['ZZ_902', 'ZZ_903'])
        saved = json.loads(puts[-1]['Body'])
        self.assertEqual(saved['ZZ_901'], h(same))
        self.assertEqual(saved['ZZ_902'], h(b'new-bytes'))
        self.assertEqual(saved['ZZ_903'], 'h903')


class VariantColourOverride(unittest.TestCase):
    def test_variant_colour_beats_a_base_record_without_colour(self):
        from tjx_display import TjxDisplay
        d = TjxDisplay({'TJZZXX901SLS': {'fit': 'Slim Fit'}, 'TJZZXX901SLS-V': {'color': 'Black Solid'},
                        'TJZZXX902SLS': {'color': 'Navy Solid'}, 'TJZZXX902SLS-V': {'color': 'Wine Solid'}},
                       {}, [], catalog_customers=[])
        self.assertEqual(d.color_info('TJZZXX901SLS-V', 'DKNY')['display'], 'Black Solid')
        self.assertEqual(d.color_info('TJZZXX902SLS-V', 'DKNY')['display'], 'Wine Solid')
        self.assertEqual(d.color_info('TJZZXX902SLS', 'DKNY')['display'], 'Navy Solid')


class ConditionalOverrideSave(_Patch):
    NAMES = ('get_s3', 'load_overrides_from_s3', '_style_overrides', '_s3_overrides_etag',
             '_backup_overrides_to_s3', 'trigger_background_generation', '_invalidate_cloudfront')

    def test_a_concurrent_save_is_merged_not_lost(self):
        from botocore.exceptions import ClientError
        a = self.app
        s3_state = {'map': {'ZZA': {'color': 'A'}}, 'etag': 'e1'}
        writes = []

        def load():
            with a._overrides_lock:
                a._style_overrides = dict(s3_state['map'])
                a._s3_overrides_etag = s3_state['etag']

        class S3:
            def put_object(s, **kw):
                if kw.get('IfMatch') and kw['IfMatch'].strip('"') != s3_state['etag']:
                    raise ClientError({'Error': {'Code': 'PreconditionFailed'}}, 'PutObject')
                s3_state['map'] = json.loads(kw['Body'])
                s3_state['etag'] = 'e%d' % (len(writes) + 2)
                writes.append(s3_state['map'])
                return {'ETag': '"%s"' % s3_state['etag']}
        a.get_s3 = lambda: S3()
        a._backup_overrides_to_s3 = lambda d: None
        a.trigger_background_generation = lambda: None
        a._invalidate_cloudfront = lambda paths: None
        # someone else's save lands on S3 between this request's load and its write
        first = {'n': 0}

        def load_then_race():
            load()
            if first['n'] == 0:
                first['n'] = 1
                s3_state['map'] = dict(s3_state['map'], ZZB={'color': 'B'})
                s3_state['etag'] = 'other'
        a.load_overrides_from_s3 = load_then_race
        r = a.app.test_client().post('/overrides', json={'overrides': {'ZZC': {'color': 'C'}}})
        self.assertEqual(r.status_code, 200, r.get_data(as_text=True)[:300])
        self.assertEqual(sorted(s3_state['map']), ['ZZA', 'ZZB', 'ZZC'])
        self.assertEqual(len(writes), 1)


if __name__ == '__main__':
    unittest.main()
