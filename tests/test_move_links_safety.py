"""Moving override photos to S3 while keeping the records (as links) changes no other photo.

1. The move job can skip base copies ("base_copies": false): only each key's own file is
   written, so a style whose sizes carry photos gets no new base photo.
2. When a record's photo changes (embedded -> link, a move overwrite, a cleared photo), every
   worker drops the row's own-file export cache ('sku:<SKU>' in get_image_cached) as well,
   so an export never keeps a file cached before the move.
Synthetic keys; no test touches the network (tests._support blocks sockets).

Run: python -B -m unittest tests.test_move_links_safety
"""
import base64
import io
import json
import unittest

from tests._support import load_app


def _jpeg(color):
    from PIL import Image
    buf = io.BytesIO()
    Image.new('RGB', (8, 8), color).save(buf, 'JPEG')
    return 'data:image/jpeg;base64,' + base64.b64encode(buf.getvalue()).decode()


class _Body:
    def __init__(self, b):
        self.b = b

    def read(self):
        return self.b


class _S3:
    def __init__(self, data):
        self.data = data

    def get_object(self, Bucket, Key):
        return {'Body': _Body(json.dumps(self.data).encode('utf-8')), 'ETag': '"etag-after"'}


class MovePlanBaseCopies(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.app = load_app()

    def writes(self, overrides, **kw):
        plan, _data = self.app._ovr_move_plan(overrides, {}, **kw)
        return {w['file']: w['action'] for w in plan['writes']}

    def test_default_still_writes_the_base_copy(self):
        w = self.writes({'ZZNASU911SLP-14-32': {'image': _jpeg('blue')}})
        self.assertEqual(w.get('ZZNASU911SLP.jpg'), 'base copy')

    def test_base_copies_off_writes_only_own_files(self):
        w = self.writes({'ZZNASU911SLP-14-32': {'image': _jpeg('blue')},
                         'ZZNASU912SLP': {'image': _jpeg('red')}}, base_copies=False)
        self.assertEqual(w, {'ZZNASU911SLP-14-32.jpg': 'new', 'ZZNASU912SLP.jpg': 'new'})


class OwnFileCacheDropped(unittest.TestCase):
    NAMES = ('get_s3', '_style_overrides', '_s3_overrides_etag', '_overrides_loaded')

    @classmethod
    def setUpClass(cls):
        cls.app = load_app()

    def setUp(self):
        a = self.app
        self.saved = {n: getattr(a, n) for n in self.NAMES}
        self.seen = dict(a._s3_etag_seen)

    def tearDown(self):
        a = self.app
        for n, v in self.saved.items():
            setattr(a, n, v)
        a._s3_etag_seen.clear()
        a._s3_etag_seen.update(self.seen)
        for k in ('sku:ZZNASU921SLP-V', 'sku:ZZNASU922SLP-V', 'sku:ZZNASU923SLP-V'):
            a._img_cache.pop(k, None)

    def test_reload_drops_own_file_cache_of_a_changed_record(self):
        a = self.app
        a._style_overrides = {'ZZNASU921SLP-V': {'image': _jpeg('white'), 'color': 'White'},
                              'ZZNASU923SLP-V': {'image': _jpeg('black')}}
        a._img_cache['sku:ZZNASU921SLP-V'] = {'raw_bytes': b'old file'}
        a._img_cache['sku:ZZNASU923SLP-V'] = {'raw_bytes': b'kept'}
        link = 'https://b.s3.us-east-2.amazonaws.com/ALL+INVENTORY+Photos/STYLE+OVERRIDES/ZZNASU921SLP-V.jpg'
        a.get_s3 = lambda: _S3({'ZZNASU921SLP-V': {'image': link, 'color': 'White'},
                                'ZZNASU923SLP-V': a._style_overrides['ZZNASU923SLP-V']})
        a.load_overrides_from_s3()
        self.assertNotIn('sku:ZZNASU921SLP-V', a._img_cache)       # photo moved: cache dropped
        self.assertIn('sku:ZZNASU923SLP-V', a._img_cache)          # unchanged record: kept
        self.assertEqual(a._style_overrides['ZZNASU921SLP-V']['image'], link)

    def test_photos_changed_drops_own_file_cache(self):
        a = self.app
        a._img_cache['sku:ZZNASU922SLP-V'] = {'raw_bytes': b'old file'}
        a._photos_changed(['zznasu922slp-v'])
        self.assertNotIn('sku:ZZNASU922SLP-V', a._img_cache)


if __name__ == '__main__':
    unittest.main()
