"""Phone app speed (Sep 24 2026: "the phone app's inventory is extremely slow").
Thumbnails on /image, the lite overrides feed + /image/ovr/<hash>, and the
/sync path that skips the Dropbox download while the file is unchanged."""
import base64
import json
import os
import sys
import unittest
from io import BytesIO

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _support import load_app

from PIL import Image


def _jpeg(w, h, color=(200, 30, 30)):
    out = BytesIO()
    Image.new('RGB', (w, h), color).save(out, format='JPEG', quality=90)
    return out.getvalue()


def _png_alpha(w, h):
    out = BytesIO()
    Image.new('RGBA', (w, h), (0, 0, 255, 0)).save(out, format='PNG')
    return out.getvalue()


def _size(b):
    with Image.open(BytesIO(b)) as im:
        return im.size, im.format


class _Head:
    def __init__(self, etag):
        self.etag = etag
        self.heads = 0

    def head_object(self, **_k):
        self.heads += 1
        return {'ETag': '"%s"' % self.etag}


class _Base(unittest.TestCase):

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
        email_agent._sweeper_started = True   # no mailbox sweeper thread in these tests
        a._thumb_drop()
        self.addCleanup(a._thumb_drop)
        saved_web = dict(a._web_img_cache)
        self.addCleanup(lambda: (a._web_img_cache.clear(), a._web_img_cache.update(saved_web)))
        a._web_img_cache.clear()


class ThumbnailTests(_Base):

    def test_widths_snap_up_to_a_bucket(self):
        tw = self.app._thumb_width
        self.assertEqual(160, tw('1'))
        self.assertEqual(640, tw('640'))
        self.assertEqual(960, tw('641'))
        self.assertEqual(1280, tw(1280))
        for bad in (None, '', 'x', '0', '-5', '5000'):
            self.assertIsNone(tw(bad), bad)

    def test_a_large_jpeg_shrinks_to_the_width(self):
        t = self.app._make_thumb(_jpeg(2400, 3200), 640)
        (w, h), fmt = _size(t)
        self.assertEqual((640, 853), (w, h))
        self.assertEqual('JPEG', fmt)

    def test_a_small_photo_is_left_alone(self):
        self.assertIsNone(self.app._make_thumb(_jpeg(300, 400), 640))

    def test_transparent_png_becomes_a_white_jpeg(self):
        t = self.app._make_thumb(_png_alpha(1000, 1000), 320)
        with Image.open(BytesIO(t)) as im:
            self.assertEqual('JPEG', im.format)
            self.assertEqual(320, im.width)
            r, g, b = im.convert('RGB').getpixel((10, 10))
            self.assertTrue(r > 240 and g > 240 and b > 240)

    def test_a_heavy_narrow_png_becomes_a_small_jpeg(self):
        import random
        rnd = random.Random(3)
        im = Image.new('RGB', (450, 450))
        im.putdata([(min(255, x // 2 + rnd.randint(0, 12)), 90, 140) for _y in range(450) for x in range(450)])
        out = BytesIO()
        im.save(out, format='PNG')
        raw = out.getvalue()
        t = self.app._make_thumb(raw, 640)
        self.assertIsNotNone(t)
        self.assertLess(len(t) * 2, len(raw))
        self.assertEqual(((450, 450), 'JPEG'), _size(t))

    def test_a_light_narrow_png_is_left_alone(self):
        out = BytesIO()
        Image.new('RGB', (300, 300), (10, 10, 10)).save(out, format='PNG')
        self.assertIsNone(self.app._make_thumb(out.getvalue(), 640))

    def test_garbage_is_not_a_thumbnail(self):
        self.assertIsNone(self.app._make_thumb(b'not an image', 640))

    def test_cache_is_byte_capped_lru(self):
        a = self.app
        self.keep(a, '_THUMB_CACHE_MAX_BYTES')
        a._THUMB_CACHE_MAX_BYTES = 250
        for i in range(4):
            a._thumb_put(('S%d' % i, 640), (b'x' * 100, 'image/jpeg'))
        self.assertIsNone(a._thumb_get(('S0', 640)))
        self.assertIsNone(a._thumb_get(('S1', 640)))
        self.assertIsNotNone(a._thumb_get(('S3', 640)))
        self.assertLessEqual(a._thumb_bytes, 250)

    def test_oversized_entries_are_not_cached(self):
        a = self.app
        a._thumb_put(('BIG', 640), (b'x' * (a._THUMB_MAX_ENTRY_BYTES + 1), 'image/jpeg'))
        self.assertIsNone(a._thumb_get(('BIG', 640)))

    def test_drop_by_name_takes_the_base_style_too(self):
        a = self.app
        for k in ('RODKPK001SLS', 'RODKPK001SLS-V', 'OTHER001SLS'):
            a._thumb_put((k, 640), (b'x', 'image/jpeg'))
        a._thumb_drop(['rodkpk001sls-v'])
        self.assertIsNone(a._thumb_get(('RODKPK001SLS', 640)))
        self.assertIsNone(a._thumb_get(('RODKPK001SLS-V', 640)))
        self.assertIsNotNone(a._thumb_get(('OTHER001SLS', 640)))
        a._thumb_drop()
        self.assertIsNone(a._thumb_get(('OTHER001SLS', 640)))
        self.assertEqual(0, a._thumb_bytes)


class ImageRouteTests(_Base):

    def setUp(self):
        super().setUp()
        a = self.app
        self.big = _jpeg(2000, 2500)
        self.calls = []

        def fake_fetch(base, brand):
            self.calls.append(base)
            return self.big, 'image/jpeg'
        self.keep(a, '_fetch_raw_image')
        a._fetch_raw_image = fake_fetch

    def test_thumbnail_request_is_small_and_cached_apart(self):
        r = self.client.get('/image/ZZTEST001SLS?brand=VD&w=640')
        self.assertEqual(200, r.status_code)
        self.assertEqual((640, 800), _size(r.data)[0])
        self.assertIn('max-age=86400', r.headers['Cache-Control'])
        self.assertLess(len(r.data), len(self.big))
        self.assertNotIn('ZZTEST001SLS', self.app._web_img_cache)
        r2 = self.client.get('/image/ZZTEST001SLS?brand=VD&w=600')
        self.assertEqual(r.data, r2.data)
        self.assertEqual(['ZZTEST001SLS'], self.calls)

    def test_full_size_request_is_unchanged(self):
        r = self.client.get('/image/ZZTEST002SLS?brand=VD')
        self.assertEqual(self.big, r.data)
        self.assertIn('ZZTEST002SLS', self.app._web_img_cache)
        r2 = self.client.get('/image/ZZTEST002SLS?brand=VD&w=320')
        self.assertEqual(320, _size(r2.data)[0][0])
        self.assertEqual(['ZZTEST002SLS'], self.calls)

    def test_a_known_miss_stays_404(self):
        self.app._web_img_cache['ZZNONE001SLS'] = None
        self.assertEqual(404, self.client.get('/image/ZZNONE001SLS?w=640').status_code)

    def test_override_save_eviction_drops_the_thumbnail(self):
        self.client.get('/image/ZZTEST003SLS?brand=VD&w=640')
        self.app._thumb_drop(['ZZTEST003SLS-L'])
        self.client.get('/image/ZZTEST003SLS?brand=VD&w=640')
        self.assertEqual(['ZZTEST003SLS', 'ZZTEST003SLS'], self.calls)


class LiteOverridesTests(_Base):

    def setUp(self):
        super().setUp()
        a = self.app
        for n in ('_style_overrides', '_s3_overrides_etag', 'get_s3', '_overrides_loaded'):
            self.keep(a, n)
        a._overrides_loaded = True
        self.keep(a, 'load_overrides_from_s3')
        self.addCleanup(a._ovr_lite.update, dict(a._ovr_lite))
        a._ovr_lite.update(etag=None, body=None, by_hash={}, checked=0.0, retired={})
        self.img = _jpeg(1500, 1500)
        self.uri = 'data:image/jpeg;base64,' + base64.b64encode(self.img).decode()
        a._style_overrides = {
            'ZZOVR001SLS': {'image': self.uri, 'color': 'NAVY'},
            'ZZOVR002SLS': {'fabric': 'SR'},
            'ZZOVR003SLS': {'image': 'https://example.test/x.jpg'},
        }
        a._s3_overrides_etag = 'etag-1'
        self.s3 = _Head('etag-1')
        a.get_s3 = lambda: self.s3
        self.loads = []
        a.load_overrides_from_s3 = lambda: self.loads.append(1)

    def test_embedded_photos_become_hash_urls(self):
        st = self.app._ovr_lite_state(max_age=0)
        body = json.loads(st['body'])
        ov = body['overrides']
        self.assertTrue(body['lite'])
        self.assertEqual('etag-1', body['version'])
        url = ov['ZZOVR001SLS']['image']
        self.assertRegex(url, r'^/image/ovr/[0-9a-f]{24}\?k=ZZOVR001SLS$')
        self.assertEqual('NAVY', ov['ZZOVR001SLS']['color'])
        self.assertEqual({'fabric': 'SR'}, ov['ZZOVR002SLS'])
        self.assertEqual('https://example.test/x.jpg', ov['ZZOVR003SLS']['image'])
        self.assertLess(len(st['body']), 1000)
        self.assertEqual(self.uri, self.app._style_overrides['ZZOVR001SLS']['image'])
        self.assertEqual([], self.loads)

    def test_the_hash_route_serves_the_photo(self):
        st = self.app._ovr_lite_state(max_age=0)
        url = json.loads(st['body'])['overrides']['ZZOVR001SLS']['image']
        r = self.client.get(url)
        self.assertEqual(200, r.status_code)
        self.assertEqual(self.img, r.data)
        self.assertIn('immutable', r.headers['Cache-Control'])
        r = self.client.get(url + '&w=320')
        self.assertEqual(320, _size(r.data)[0][0])

    def test_unknown_or_malformed_hashes_are_404(self):
        self.app._ovr_lite_state(max_age=0)
        self.assertEqual(404, self.client.get('/image/ovr/' + '0' * 24).status_code)
        self.assertEqual(404, self.client.get('/image/ovr/..%2Fsecret').status_code)
        self.assertEqual(404, self.client.get('/image/ovr/XYZ').status_code)

    def test_a_new_s3_etag_reloads_and_rebuilds(self):
        a = self.app
        a._ovr_lite_state(max_age=0)
        new_uri = 'data:image/jpeg;base64,' + base64.b64encode(_jpeg(900, 900, (0, 90, 0))).decode()

        def reload():
            self.loads.append(1)
            a._style_overrides = {'ZZOVR001SLS': {'image': new_uri}}
            a._s3_overrides_etag = 'etag-2'
        a.load_overrides_from_s3 = reload
        self.s3.etag = 'etag-2'
        body = json.loads(a._ovr_lite_state(max_age=0)['body'])
        self.assertEqual([1], self.loads)
        self.assertEqual('etag-2', body['version'])
        self.assertEqual(['ZZOVR001SLS'], list(body['overrides']))

    def test_etag_checks_are_throttled(self):
        a = self.app
        a._ovr_lite_state(max_age=0)
        n = self.s3.heads
        a._ovr_lite_state(max_age=15)
        a._ovr_lite_state(max_age=15)
        self.assertEqual(n, self.s3.heads)

    def test_lite_query_on_the_route(self):
        r = self.client.get('/overrides?lite=1')
        self.assertEqual(200, r.status_code)
        body = json.loads(r.data)
        self.assertTrue(body['lite'])
        self.assertTrue(body['overrides']['ZZOVR001SLS']['image'].startswith('/image/ovr/'))


class SyncFastPathTests(_Base):

    def setUp(self):
        super().setUp()
        a = self.app
        for n in ('_dropbox_inventory_rev', 'sync_inventory'):
            self.keep(a, n)
        saved_inv = dict(a._inventory)
        self.addCleanup(lambda: (a._inventory.clear(), a._inventory.update(saved_inv)))
        saved_rev = dict(a._dbx_inv_rev)
        self.addCleanup(a._dbx_inv_rev.update, saved_rev)
        a._inventory.update(items=[{'sku': 'ZZ001SLS'}], source='dropbox', last_sync='2026-01-01T00:00:00Z')
        a._dbx_inv_rev.update(accepted='rev-a', checked_at=0.0)
        self.revs = []
        self.syncs = []
        self.rev = 'rev-a'

        def fake_rev():
            self.revs.append(1)
            return self.rev
        a._dropbox_inventory_rev = fake_rev
        a.sync_inventory = lambda: (self.syncs.append(1), True)[1]

    def test_unchanged_file_skips_the_download(self):
        a = self.app
        self.assertFalse(a.sync_inventory_if_changed())
        self.assertEqual([], self.syncs)
        self.assertEqual([1], self.revs)
        self.assertNotEqual('2026-01-01T00:00:00Z', a._inventory['last_sync'])

    def test_rev_checks_are_throttled(self):
        a = self.app
        a.sync_inventory_if_changed()
        a.sync_inventory_if_changed()
        self.assertEqual([1], self.revs)

    def test_a_new_rev_syncs(self):
        self.rev = 'rev-b'
        self.assertTrue(self.app.sync_inventory_if_changed())
        self.assertEqual([1], self.syncs)

    def test_no_rev_answer_syncs(self):
        self.rev = None
        self.assertTrue(self.app.sync_inventory_if_changed())
        self.assertEqual([1], self.syncs)

    def test_cold_worker_syncs_without_asking(self):
        self.app._inventory['items'] = []
        self.assertTrue(self.app.sync_inventory_if_changed())
        self.assertEqual([], self.revs)

    def test_s3_fallback_data_always_syncs(self):
        self.app._inventory['source'] = 's3'
        self.assertTrue(self.app.sync_inventory_if_changed())
        self.assertEqual([], self.revs)

class ExportRebuildTests(_Base):
    """/sync used to rebuild exports on every call; it now rebuilds when the
    inventory, overrides or prepack rules they were made from changed."""

    def setUp(self):
        super().setUp()
        a = self.app
        for n in ('trigger_background_generation', 'get_s3', '_s3_overrides_etag',
                  'load_overrides_from_s3', 'sync_inventory_if_changed'):
            self.keep(a, n)
        saved = dict(a._exports)
        self.addCleanup(lambda: (a._exports.clear(), a._exports.update(saved)))
        saved_inv = dict(a._inventory)
        self.addCleanup(lambda: (a._inventory.clear(), a._inventory.update(saved_inv)))
        saved_seen = dict(a._s3_etag_seen)
        self.addCleanup(lambda: (a._s3_etag_seen.clear(), a._s3_etag_seen.update(saved_seen)))
        a._s3_etag_seen.clear()
        self.regen = []
        a.trigger_background_generation = lambda: self.regen.append(1)
        a.sync_inventory_if_changed = lambda: False
        self.etags = {a.S3_OVERRIDES_KEY: 'ov-1', a.S3_PREPACK_DEFAULTS_KEY: 'pp-1'}
        etags = self.etags

        class S3:
            def head_object(self, Bucket, Key):
                return {'ETag': '"%s"' % etags[Key]}
        a.get_s3 = lambda: S3()
        a._s3_overrides_etag = 'ov-1'
        self.reloads = []

        def reload():
            self.reloads.append(1)
            a._s3_overrides_etag = etags[a.S3_OVERRIDES_KEY]
        a.load_overrides_from_s3 = reload
        a._inventory.update(gen=7, items=[{'sku': 'ZZ001SLS'}], brands={})
        a._exports.update(brands={'VD': {}}, generating=False)
        a._exports['inputs'] = a._export_inputs(max_age=0)

    def sync(self):
        r = self.client.get('/sync')
        self.assertEqual(200, r.status_code)
        return json.loads(r.data)

    def test_unchanged_inputs_do_not_rebuild(self):
        self.assertFalse(self.sync()['updated'])
        self.sync()
        self.assertEqual([], self.regen)

    def test_new_inventory_rebuilds_even_if_another_call_loaded_it(self):
        self.app._inventory['gen'] = 8
        self.sync()
        self.assertEqual([1], self.regen)

    def test_an_overrides_save_on_another_worker_rebuilds_without_a_request_path_reload(self):
        self.etags[self.app.S3_OVERRIDES_KEY] = 'ov-2'
        self.app._s3_etag_seen.clear()
        self.sync()
        self.assertEqual([], self.reloads)      # the 18MB file is never loaded inside /sync
        self.assertEqual([1], self.regen)
        self.app._export_inputs(max_age=0)       # what the export run does first
        self.assertEqual([1], self.reloads)

    def test_a_prepack_rule_save_rebuilds(self):
        self.etags[self.app.S3_PREPACK_DEFAULTS_KEY] = 'pp-2'
        self.app._s3_etag_seen.clear()
        self.sync()
        self.assertEqual([1], self.regen)

    def test_s3_is_asked_at_most_once_a_minute(self):
        self.sync()
        self.etags[self.app.S3_PREPACK_DEFAULTS_KEY] = 'pp-2'
        self.sync()
        self.assertEqual([], self.regen)

    def test_a_change_during_a_run_queues_a_rebuild(self):
        self.app._exports['generating'] = True
        self.app._inventory['gen'] = 8
        self.sync()
        self.assertEqual([1], self.regen)

    def test_a_run_that_is_starting_is_left_alone(self):
        self.app._exports.update(generating=True, inputs=None)
        self.sync()
        self.assertEqual([], self.regen)

    def test_no_exports_yet_builds(self):
        self.app._exports['brands'] = {}
        self.sync()
        self.assertEqual([1], self.regen)

    def test_a_run_records_its_inputs_and_a_failed_run_clears_them(self):
        a = self.app
        self.keep(a, '_fresh_prepack_defaults')
        a._exports['inputs'] = None
        a._inventory['brands'] = {}
        a.generate_all_exports()   # nothing to build: returns after recording
        self.assertEqual((7, 'ov-1', 'pp-1', a._production_rev), a._exports['inputs'])
        a._inventory['brands'] = {'VD': {'name': 'Von Dutch', 'items': []}}

        def boom():
            raise RuntimeError('s3 down')
        a._fresh_prepack_defaults = boom
        a.generate_all_exports()
        self.assertIsNone(a._exports['inputs'])
        self.assertFalse(a._exports['generating'])


class DropboxRevRecordTests(_Base):

    def test_download_records_the_accepted_rev(self):
        a = self.app
        for n in ('get_dropbox_token', 'parse_inventory_excel', '_sync_passes_sanity_check',
                  '_group_by_brand', '_nj_commit_report'):
            self.keep(a, n)
        self.keep(a.http_requests, 'post')
        saved_inv = dict(a._inventory)
        self.addCleanup(lambda: (a._inventory.clear(), a._inventory.update(saved_inv)))
        saved_rev = dict(a._dbx_inv_rev)
        self.addCleanup(a._dbx_inv_rev.update, saved_rev)

        class R:
            status_code = 200
            content = b'x' * 2000
            text = ''
            headers = {'Dropbox-API-Result': json.dumps({'rev': 'rev-zz', 'name': 'Inventory_ATS.xlsx'})}
        a.get_dropbox_token = lambda: 'tok'
        a.http_requests.post = lambda *x, **k: R()
        a.parse_inventory_excel = lambda data: [{'sku': 'ZZ001SLS'}]
        a._sync_passes_sanity_check = lambda items: (True, 'passed', {'committed_nonzero_count': 0,
                                                                       'committed_abs_sum': 0})
        a._group_by_brand = lambda items: {}
        a._nj_commit_report = lambda: None
        self.assertTrue(a.sync_from_dropbox())
        self.assertEqual('rev-zz', a._dbx_inv_rev['accepted'])


class CrossWorkerEvictionTests(_Base):

    def test_reload_drops_proxy_cache_and_thumbnails_of_changed_styles(self):
        a = self.app
        for n in ('_style_overrides', '_s3_overrides_etag', 'get_s3', '_overrides_loaded'):
            self.keep(a, n)
        a._style_overrides = {'ZZCH001SLS': {'image': 'data:image/jpeg;base64,AAAA'},
                              'ZZSAME01SLS': {'image': 'data:image/jpeg;base64,BBBB'}}
        new = {'ZZCH001SLS': {'image': 'data:image/jpeg;base64,CCCC'},
               'ZZSAME01SLS': {'image': 'data:image/jpeg;base64,BBBB'}}

        class Body:
            def read(self):
                return json.dumps(new).encode()

        class S3:
            def get_object(self, **_k):
                return {'Body': Body(), 'ETag': '"e2"'}
        a.get_s3 = lambda: S3()
        a._web_img_cache['ZZCH001SLS'] = (b'old', 'image/jpeg')
        a._web_img_cache['ZZSAME01SLS'] = (b'same', 'image/jpeg')
        a._thumb_put(('ZZCH001SLS', 640), (b'old', 'image/jpeg'))
        a._thumb_put(('ZZSAME01SLS', 640), (b'same', 'image/jpeg'))
        a.load_overrides_from_s3()
        self.assertNotIn('ZZCH001SLS', a._web_img_cache)
        self.assertIn('ZZSAME01SLS', a._web_img_cache)
        self.assertIsNone(a._thumb_get(('ZZCH001SLS', 640)))
        self.assertIsNotNone(a._thumb_get(('ZZSAME01SLS', 640)))



class PhotoFreshnessTests(_Base):
    """The phone keeps photo URLs stable now, so the server must notice photo
    changes itself: serve-then-refresh after _PHOTO_TTL, catch-up on a newer
    overrides tag, swatch uploads, and retired override-photo hashes."""

    def setUp(self):
        super().setUp()
        a = self.app
        for n in ('_fetch_raw_image', 'threading', '_ovr_lite_state', '_s3_overrides_etag'):
            self.keep(a, n)
        for d in (a._web_img_at, a._web_img_md5):
            saved = dict(d)
            self.addCleanup(lambda d=d, saved=saved: (d.clear(), d.update(saved)))
            d.clear()
        self.photo = _jpeg(2000, 2000, (200, 0, 0))
        self.calls = []

        def fetch(base, brand):
            self.calls.append(base)
            return (self.photo, 'image/jpeg') if self.photo else (None, None)
        a._fetch_raw_image = fetch

        class Inline:
            """Runs 'background' threads at once so the test can see the result."""
            Lock = a.threading.Lock

            class Thread:
                def __init__(self, target, args=(), daemon=None):
                    self.t, self.a = target, args

                def start(self):
                    self.t(*self.a)
        a.threading = Inline

    def red(self, data):
        with Image.open(BytesIO(data)) as im:
            return im.convert('RGB').getpixel((5, 5))[0] > 150

    def age(self, base):
        self.app._web_img_at[base] -= self.app._PHOTO_TTL + 1

    def test_a_changed_photo_replaces_the_cached_thumbnail_after_the_ttl(self):
        c = self.client
        self.assertTrue(self.red(c.get('/image/ZZF001SLS?brand=VD&w=640').data))
        self.photo = _jpeg(2000, 2000, (0, 0, 200))
        self.assertTrue(self.red(c.get('/image/ZZF001SLS?brand=VD&w=640').data))   # inside the TTL
        self.assertEqual(1, len(self.calls))
        self.age('ZZF001SLS')
        self.assertTrue(self.red(c.get('/image/ZZF001SLS?brand=VD&w=640').data))   # served, then re-checked
        self.assertFalse(self.red(c.get('/image/ZZF001SLS?brand=VD&w=640').data))  # new photo
        self.assertEqual(2, len(self.calls))

    def test_an_unchanged_photo_only_resets_the_clock(self):
        c = self.client
        c.get('/image/ZZF002SLS?brand=VD&w=640')
        self.age('ZZF002SLS')
        c.get('/image/ZZF002SLS?brand=VD&w=640')
        c.get('/image/ZZF002SLS?brand=VD&w=640')
        self.assertEqual(2, len(self.calls))

    def test_a_cached_miss_is_retried_after_the_ttl(self):
        c = self.client
        self.photo = None
        self.assertEqual(404, c.get('/image/ZZF003SLS?brand=VD&w=640').status_code)
        self.photo = _jpeg(1500, 1500)
        self.assertEqual(404, c.get('/image/ZZF003SLS?brand=VD&w=640').status_code)   # inside the TTL
        self.age('ZZF003SLS')
        c.get('/image/ZZF003SLS?brand=VD&w=640')
        r = c.get('/image/ZZF003SLS?brand=VD&w=640')
        self.assertEqual(200, r.status_code)
        self.assertEqual(640, _size(r.data)[0][0])

    def test_a_failed_recheck_keeps_the_photo(self):
        c = self.client
        c.get('/image/ZZF004SLS?brand=VD')
        self.photo = None
        self.age('ZZF004SLS')
        c.get('/image/ZZF004SLS?brand=VD')
        self.assertEqual(200, c.get('/image/ZZF004SLS?brand=VD').status_code)

    def test_full_size_requests_are_rechecked_too(self):
        c = self.client
        c.get('/image/ZZF005SLS?brand=VD')
        self.photo = _jpeg(2000, 2000, (0, 0, 200))
        self.age('ZZF005SLS')
        c.get('/image/ZZF005SLS?brand=VD')
        self.assertFalse(self.red(c.get('/image/ZZF005SLS?brand=VD').data))

    def test_a_newer_overrides_tag_makes_the_worker_catch_up(self):
        a = self.app
        seen = []
        a._ovr_lite_state = lambda max_age=15: seen.append(max_age) or {}
        a._s3_overrides_etag = 'abcdef0123456789'
        self.client.get('/image/ZZF006SLS?brand=VD&w=640&v=abcdef01-2026-09-24')
        self.assertEqual([], seen)
        self.client.get('/image/ZZF006SLS?brand=VD&w=640&v=99999999-2026-09-24')
        self.assertEqual([5], seen)
        for v in ('0-2026-09-24', '1727190000000', 'zzzzzzzz-1', ''):
            self.client.get('/image/ZZF006SLS?brand=VD&w=640&v=' + v)
        self.assertEqual([5], seen)

    def test_swatch_uploads_drop_this_workers_copies(self):
        a = self.app
        a._web_img_cache['ZZF007SLS'] = (b'old', 'image/jpeg')
        a._thumb_put(('ZZF007SLS', 640), (b'old', 'image/jpeg'))
        a._photos_changed(['zzf007sls-l'], None)
        self.assertNotIn('ZZF007SLS', a._web_img_cache)
        self.assertIsNone(a._thumb_get(('ZZF007SLS', 640)))
        a._web_img_cache['ZZF008SLS'] = (b'old', 'image/jpeg')
        a._photos_changed(['VD_010'], 'swatch_fallback')
        self.assertIn('ZZF008SLS', a._web_img_cache)

    def test_swatch_routes_get_the_callback(self):
        import inspect
        src = inspect.getsource(self.app)
        self.assertIn('register_swatch_routes(app, get_s3, S3_BUCKET, on_images_changed=_photos_changed)', src)


class RetiredOverrideHashTests(LiteOverridesTests):

    def test_a_replaced_photo_still_answers_under_its_old_hash(self):
        a = self.app
        st = a._ovr_lite_state(max_age=0)
        old_url = json.loads(st['body'])['overrides']['ZZOVR001SLS']['image']
        new_uri = 'data:image/jpeg;base64,' + base64.b64encode(_jpeg(900, 900, (0, 90, 0))).decode()

        def reload():
            a._style_overrides = {'ZZOVR001SLS': {'image': new_uri}}
            a._s3_overrides_etag = 'etag-2'
        a.load_overrides_from_s3 = reload
        self.s3.etag = 'etag-2'
        new_url = json.loads(a._ovr_lite_state(max_age=0)['body'])['overrides']['ZZOVR001SLS']['image']
        self.assertNotEqual(old_url, new_url)
        r = self.client.get(old_url)
        self.assertEqual(200, r.status_code)
        self.assertEqual(self.img, r.data)
        self.assertEqual(200, self.client.get(new_url).status_code)

    def test_retired_hashes_expire(self):
        a = self.app
        a._ovr_lite_state(max_age=0)
        a._ovr_lite['retired'] = {'f' * 24: ('data:image/jpeg;base64,AAAA', 0.0)}
        a._ovr_lite['etag'] = 'force-rebuild'
        st = a._ovr_lite_state(max_age=0)
        self.assertNotIn('f' * 24, st['retired'])



class ReviewFixTests(LiteOverridesTests):
    """Findings from the Sep 24 review of this change."""

    def test_a_known_hash_needs_no_s3_call(self):
        a = self.app
        url = json.loads(a._ovr_lite_state(max_age=0)['body'])['overrides']['ZZOVR001SLS']['image']
        n = self.s3.heads
        a._ovr_lite['checked'] = 0.0      # state is due for a re-check
        for _ in range(5):
            self.assertEqual(200, self.client.get(url + '&w=320').status_code)
        self.assertEqual(n, self.s3.heads)
        self.assertEqual([], self.loads)

    def test_a_stored_label_is_never_trusted(self):
        a = self.app
        svg = b'<svg xmlns="http://www.w3.org/2000/svg"><script>alert(1)</script></svg>'
        a._style_overrides['ZZOVR009SLS'] = {'image': 'data:image/svg+xml;base64,' + base64.b64encode(svg).decode()}
        a._s3_overrides_etag = 'etag-svg'
        self.s3.etag = 'etag-svg'
        url = json.loads(a._ovr_lite_state(max_age=0)['body'])['overrides']['ZZOVR009SLS']['image']
        r = self.client.get(url)
        self.assertEqual('image/jpeg', r.headers['Content-Type'])
        self.assertEqual('nosniff', r.headers['X-Content-Type-Options'])
        r = self.client.get(url.replace('/image/ovr/', '/image/ovr/'))
        png = BytesIO()
        Image.new('RGB', (10, 10)).save(png, format='PNG')
        self.assertEqual('image/png', a._sniff_image_type(png.getvalue()))

    def test_a_waiting_caller_reuses_the_refresh(self):
        a = self.app
        a._ovr_lite_state(max_age=0)
        a._ovr_lite_gate.acquire()
        a._ovr_lite_gate.release()
        st = a._ovr_lite_state(max_age=15)
        self.assertIsNotNone(st['body'])

    def test_thumbnails_run_through_the_helper(self):
        self.assertEqual(7, self.app._off_loop(lambda x: x + 1, 6))


class SaveEvictionTests(_Base):
    """POST /overrides that clears or deletes a photo evicts this worker's copies."""

    def setUp(self):
        super().setUp()
        a = self.app
        for n in ('_style_overrides', '_s3_overrides_etag', 'load_overrides_from_s3',
                  'save_overrides_to_s3', '_backup_overrides_to_s3', '_invalidate_cloudfront',
                  'trigger_background_generation', '_overrides_last_saved'):
            self.keep(a, n)
        a.load_overrides_from_s3 = lambda: None
        a.save_overrides_to_s3 = lambda snap=None: True
        a._backup_overrides_to_s3 = lambda d: None
        a._invalidate_cloudfront = lambda paths: None
        a.trigger_background_generation = lambda: None
        a._s3_overrides_etag = 'e1'
        a._style_overrides = {'ZZSV001SLS': {'image': 'data:image/jpeg;base64,AAAA', 'color': 'NAVY'},
                              'ZZSV002SLS': {'image': 'data:image/jpeg;base64,BBBB'},
                              'ZZSV003SLS': {'image': 'data:image/jpeg;base64,CCCC'}}
        for k in ('ZZSV001SLS', 'ZZSV002SLS', 'ZZSV003SLS'):
            a._web_img_cache[k] = (b'x', 'image/jpeg')
            a._thumb_put((k, 480), (b'x', 'image/jpeg'))

    def test_cleared_and_deleted_photos_are_evicted(self):
        a = self.app
        r = self.client.post('/overrides', json={'overrides': {'ZZSV001SLS': {'color': 'NAVY'}},
                                                 'deleted': ['ZZSV002SLS']})
        self.assertEqual(200, r.status_code, r.data[:300])
        body = json.loads(r.data)
        self.assertEqual(2, body['invalidated'])
        for k in ('ZZSV001SLS', 'ZZSV002SLS'):
            self.assertNotIn(k, a._web_img_cache)
            self.assertIsNone(a._thumb_get((k, 480)))
        self.assertIn('ZZSV003SLS', a._web_img_cache)
        self.assertIsNotNone(a._thumb_get(('ZZSV003SLS', 480)))



class RoundTwoTests(PhotoFreshnessTests):
    """Findings from the second review round (Sep 24 2026)."""

    def test_a_changed_photo_seen_at_another_size_drops_the_old_thumbnail(self):
        c = self.client
        self.assertTrue(self.red(c.get('/image/ZZR001SLS?brand=VD&w=960').data))
        self.photo = _jpeg(2000, 2000, (0, 0, 200))
        self.assertFalse(self.red(c.get('/image/ZZR001SLS?brand=VD&w=480').data))
        self.assertFalse(self.red(c.get('/image/ZZR001SLS?brand=VD&w=960').data))

    def test_a_full_size_fetch_of_a_changed_photo_drops_the_old_thumbnail(self):
        c = self.client
        c.get('/image/ZZR002SLS?brand=VD&w=960')
        self.photo = _jpeg(2000, 2000, (0, 0, 200))
        self.assertFalse(self.red(c.get('/image/ZZR002SLS?brand=VD').data))
        self.assertFalse(self.red(c.get('/image/ZZR002SLS?brand=VD&w=960').data))

    def test_exact_multiple_widths_still_shrink(self):
        for (w, h), tw in (((1920, 1920), 960), ((1920, 2400), 480), ((960, 1200), 480)):
            t = self.app._make_thumb(_jpeg(w, h), tw)
            self.assertIsNotNone(t, (w, h, tw))
            self.assertEqual(tw, _size(t)[0][0])

    def test_decks_refetch_a_stale_miss(self):
        a = self.app
        a._web_img_cache['ZZR003SLS'] = None
        a._web_img_at['ZZR003SLS'] = 0.0
        self.assertIsNotNone(a._pres_photo_fetch('ZZR003SLS', 'VD'))
        self.assertIn('ZZR003SLS', self.calls)

    def test_decks_refetch_a_stale_photo(self):
        a = self.app
        a._web_img_cache['ZZR004SLS'] = (_jpeg(900, 900, (200, 0, 0)), 'image/jpeg')
        a._web_img_at['ZZR004SLS'] = 0.0
        self.photo = _jpeg(900, 900, (0, 0, 200))
        self.assertFalse(self.red(a._pres_photo_fetch('ZZR004SLS', 'VD')))
        self.assertFalse(self.red(a._web_img_cache['ZZR004SLS'][0]))

    def test_decks_use_a_fresh_cache_entry(self):
        a = self.app
        a._web_img_cache['ZZR005SLS'] = (_jpeg(900, 900), 'image/jpeg')
        a._web_img_at['ZZR005SLS'] = __import__('time').time()
        self.assertIsNotNone(a._pres_photo_fetch('ZZR005SLS', 'VD'))
        self.assertEqual([], self.calls)


class SaveAtomicityTests(_Base):

    def test_a_reload_that_lands_mid_upload_cannot_leave_old_data_under_the_new_etag(self):
        a = self.app
        for n in ('_style_overrides', '_s3_overrides_etag', 'get_s3'):
            self.keep(a, n)
        saved_seen = dict(a._s3_etag_seen)
        self.addCleanup(lambda: (a._s3_etag_seen.clear(), a._s3_etag_seen.update(saved_seen)))
        old = {'ZZA001SLS': {'color': 'OLD'}}
        new = {'ZZA001SLS': {'color': 'NEW'}}
        a._style_overrides = new
        a._s3_overrides_etag = 'e1'

        class S3:
            def put_object(self, **k):
                a._style_overrides = old      # a reload of the pre-save file finishes now
                a._s3_overrides_etag = 'e1'
                return {'ETag': '"e2"'}
        a.get_s3 = lambda: S3()
        self.assertTrue(a.save_overrides_to_s3(new))
        self.assertIs(new, a._style_overrides)
        self.assertEqual('e2', a._s3_overrides_etag)
        self.assertEqual('e2', a._s3_etag_seen[a.S3_OVERRIDES_KEY][0])

    def test_the_save_route_writes_the_merged_map_it_built(self):
        import inspect
        src = inspect.getsource(self.app.save_overrides)
        self.assertIn('save_overrides_to_s3(merged)', src)


class RunClaimTests(ExportRebuildTests):

    def test_claiming_a_run_clears_the_recorded_inputs_first(self):
        a = self.app
        seen = []

        def spy(max_age=60, reload=True):
            seen.append(a._exports.get('inputs'))
            return (7, 'ov-1', 'pp-1', None)
        self.keep(a, '_export_inputs')
        a._export_inputs = spy
        a._inventory['brands'] = {}
        a.generate_all_exports()
        self.assertEqual([None], seen)


class OverrideFallbackTests(LiteOverridesTests):

    def test_an_unknown_hash_with_a_key_redirects_to_the_normal_lookup(self):
        self.app._ovr_lite_state(max_age=0)
        r = self.client.get('/image/ovr/' + '0' * 24 + '?k=ZZOVR001SLS-L&w=960')
        self.assertEqual(302, r.status_code)
        self.assertEqual('/image/ZZOVR001SLS?w=960&sku=ZZOVR001SLS-L', r.headers['Location'])
        self.assertEqual('no-store', r.headers['Cache-Control'])

    def test_a_bad_key_is_still_404(self):
        self.app._ovr_lite_state(max_age=0)
        for k in ('..%2Fx', '', '%3Cscript%3E'):
            self.assertEqual(404, self.client.get('/image/ovr/' + '0' * 24 + '?k=' + k).status_code)

    def test_retired_hashes_expire_on_read_too(self):
        a = self.app
        a._ovr_lite_state(max_age=0)
        a._ovr_lite['retired'] = {'e' * 24: ('data:image/jpeg;base64,' + base64.b64encode(_jpeg(50, 50)).decode(), 0.0)}
        self.assertEqual(404, self.client.get('/image/ovr/' + 'e' * 24).status_code)


if __name__ == '__main__':
    unittest.main()
