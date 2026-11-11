"""Saved catalogs (Nov 11 2026 audit). Four things are pinned here:

  1. every worker sees a save made by another worker within seconds (the copy each
     worker keeps is checked against the S3 object's ETag), and a list that could
     not be read never replaces a good one;
  2. a write never starts from a list that failed to load (it used to come back as
     [] and the save then wrote one catalog over all of them);
  3. a delete names the catalog by its slug, so a list that moved since the page
     loaded it cannot make the server delete another catalog;
  4. force-included styles are part of a catalog's scope.

Offline: the app is imported with sockets blocked (tests/_support.py) and S3 is an
in-memory store. Every name, slug and style here is synthetic (ZZ prefixes)."""
import io
import json
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _support import load_app  # noqa: E402


def cat(name, slug, query, folder=''):
    return {'name': name, 'url': 'https://example.test/?' + query, 'brands': [], 'slug': slug,
            'folder': folder, 'savedAt': 1}


class FakeS3:
    """Just enough of boto3's client for the saved-catalogs object."""

    def __init__(self, key, cats=None):
        self.key = key
        self.objects = {}
        self.rev = 0
        self.gets = self.heads = self.puts = 0
        self.fail_reads = False
        if cats is not None:
            self.store(cats)

    def store(self, cats):
        """What ANOTHER worker's save does: the object changes, this worker is not told."""
        self.objects[self.key] = json.dumps(cats).encode('utf-8')
        self.rev += 1

    def stored(self):
        return json.loads(self.objects[self.key].decode('utf-8'))

    def _err(self, code, op):
        from botocore.exceptions import ClientError
        return ClientError({'Error': {'Code': code, 'Message': 'synthetic'}}, op)

    def get_object(self, Bucket, Key):
        self.gets += 1
        if self.fail_reads:
            raise self._err('SlowDown', 'GetObject')
        if Key not in self.objects:
            raise self._err('NoSuchKey', 'GetObject')
        return {'Body': io.BytesIO(self.objects[Key])}

    def head_object(self, Bucket, Key):
        self.heads += 1
        if self.fail_reads or Key not in self.objects:
            raise self._err('SlowDown', 'HeadObject')
        return {'ETag': '"rev%d"' % self.rev}

    def put_object(self, Bucket, Key, Body, ContentType=None):
        self.puts += 1
        self.objects[Key] = Body if isinstance(Body, bytes) else str(Body).encode('utf-8')
        if Key == self.key:
            self.rev += 1


class SavedCatalogTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.a = load_app()
        cls.client = cls.a.app.test_client()

    def setUp(self):
        a = self.a
        self.cats = [cat('ZZ Alpha', 'zzalpha1', 'brands=ZZA'),
                     cat('ZZ Beta', 'zzbeta22', 'brands=ZZB', folder='ZZ Folder'),
                     cat('ZZ Gamma', 'zzgamma3', 'brands=ZZA,ZZB')]
        self.s3 = FakeS3(a.S3_SAVED_CATALOGS_KEY, self.cats)
        self._saved = (a.get_s3, a._request_identity, a._CATALOG_CACHE_RECHECK_SECONDS)
        a.get_s3 = lambda: self.s3
        a._request_identity = lambda: {'tier': 'staff', 'is_admin': True, 'email': 'zz@example.test'}
        self._reset_cache()

    def tearDown(self):
        a = self.a
        a.get_s3, a._request_identity, a._CATALOG_CACHE_RECHECK_SECONDS = self._saved
        self._reset_cache()

    def _reset_cache(self):
        with self.a._catalog_cache_lock:
            self.a._catalog_cache.update(ts=0.0, list=None, etag=None, checked=0.0)

    def _window_passed(self):
        with self.a._catalog_cache_lock:
            self.a._catalog_cache['checked'] = 0.0

    # ── 1. the per-worker copy ──────────────────────────────────────────────
    def test_calls_inside_the_window_do_not_touch_s3(self):
        a = self.a
        self.assertEqual(len(a._load_saved_catalogs_cached()), 3)
        gets, heads = self.s3.gets, self.s3.heads
        for _ in range(5):
            a._load_saved_catalogs_cached()
        self.assertEqual((self.s3.gets, self.s3.heads), (gets, heads))

    def test_unchanged_list_costs_one_head_and_no_read(self):
        a = self.a
        a._load_saved_catalogs_cached()
        gets, heads = self.s3.gets, self.s3.heads
        self._window_passed()
        self.assertEqual(len(a._load_saved_catalogs_cached()), 3)
        self.assertEqual((self.s3.gets, self.s3.heads), (gets, heads + 1))

    def test_a_save_by_another_worker_is_seen(self):
        a = self.a
        self.assertIsNone(a._catalog_scope_for_slug('zznew999'))          # warms this worker's copy
        self.s3.store([cat('ZZ New', 'zznew999', 'brands=ZZC')] + self.cats)
        self.assertIsNone(a._catalog_scope_for_slug('zznew999'))          # still inside the window
        self._window_passed()
        scope = a._catalog_scope_for_slug('zznew999')
        self.assertIsNotNone(scope)                                        # no minute of "not found"
        self.assertIn('ZZC', scope['brands'])

    def test_an_edit_by_another_worker_is_seen(self):
        a = self.a
        self.assertEqual(a._catalog_scope_for_slug('zzalpha1')['brands'], {'ZZA'})
        edited = [dict(self.cats[0], url='https://example.test/?brands=ZZA,ZZD')] + self.cats[1:]
        self.s3.store(edited)
        self._window_passed()
        self.assertEqual(a._catalog_scope_for_slug('zzalpha1')['brands'], {'ZZA', 'ZZD'})

    def test_a_failed_read_keeps_the_last_good_list(self):
        a = self.a
        a._load_saved_catalogs_cached()
        self.s3.fail_reads = True
        self._window_passed()
        self.assertEqual(len(a._load_saved_catalogs_cached()), 3)
        self.assertIsNotNone(a._catalog_scope_for_slug('zzbeta22'))

    def test_a_failed_first_read_is_not_cached(self):
        a = self.a
        self.s3.fail_reads = True
        self.assertEqual(a._load_saved_catalogs_cached(), [])
        self.s3.fail_reads = False
        self.assertEqual(len(a._load_saved_catalogs_cached()), 3)          # the very next call recovers

    def test_this_workers_own_save_is_seen_at_once(self):
        a = self.a
        a._load_saved_catalogs_cached()
        r = self.client.post('/saved-catalogs', json={'name': 'ZZ Mine', 'slug': 'zzmine44',
                                                      'url': 'https://example.test/?brands=ZZE', 'folder': ''})
        self.assertEqual(r.status_code, 200)
        self.assertIsNotNone(a._catalog_scope_for_slug('zzmine44'))        # no waiting for the window

    # ── 2. writes never start from a list that failed to load ───────────────
    def test_strict_read(self):
        a = self.a
        self.s3.fail_reads = True
        self.assertEqual(a._load_saved_catalogs(), [])                     # readers: as before
        with self.assertRaises(Exception):
            a._load_saved_catalogs(strict=True)
        self.s3.fail_reads = False
        del self.s3.objects[a.S3_SAVED_CATALOGS_KEY]
        self.assertEqual(a._load_saved_catalogs(strict=True), [])          # no file yet = an empty list

    def test_save_during_a_failed_read_writes_nothing(self):
        before = self.s3.stored()
        self.s3.fail_reads = True
        r = self.client.post('/saved-catalogs', json={'name': 'ZZ During Blip', 'slug': 'zzblip55',
                                                      'url': 'https://example.test/?brands=ZZA'})
        self.assertEqual(r.status_code, 500)
        self.assertEqual(self.s3.puts, 0)
        self.assertEqual(self.s3.stored(), before)                         # all three still there

    def test_delete_and_reorder_during_a_failed_read_write_nothing(self):
        before = self.s3.stored()
        self.s3.fail_reads = True
        self.assertEqual(self.client.delete('/saved-catalogs/0').status_code, 500)
        self.assertEqual(self.client.post('/saved-catalogs/reorder', json={'order': [2, 1, 0]}).status_code, 500)
        self.assertEqual(self.s3.puts, 0)
        self.assertEqual(self.s3.stored(), before)

    # ── 3. a delete names its catalog ───────────────────────────────────────
    def test_delete_by_slug_ignores_a_stale_position(self):
        # The page loaded the list, then someone saved a new catalog at the top: every
        # position moved by one. The page still sends the position it knows (1 for Beta).
        self.s3.store([cat('ZZ Newer', 'zznewer7', 'brands=ZZA')] + self.cats)
        r = self.client.delete('/saved-catalogs/1?slug=zzbeta22')
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.get_json()['name'], 'ZZ Beta')
        self.assertEqual([c['slug'] for c in self.s3.stored()], ['zznewer7', 'zzalpha1', 'zzgamma3'])

    def test_delete_with_an_unknown_slug_deletes_nothing(self):
        before = self.s3.stored()
        r = self.client.delete('/saved-catalogs/0?slug=zzgone000')
        self.assertEqual(r.status_code, 404)
        self.assertEqual(self.s3.stored(), before)

    def test_delete_by_position_still_works_without_a_slug(self):
        r = self.client.delete('/saved-catalogs/2')
        self.assertEqual(r.status_code, 200)
        self.assertEqual([c['slug'] for c in self.s3.stored()], ['zzalpha1', 'zzbeta22'])
        self.assertEqual(self.client.delete('/saved-catalogs/9').status_code, 404)

    # ── 4. force-included styles are in scope ───────────────────────────────
    def test_force_included_styles_are_in_scope(self):
        a = self.a
        self.s3.store([cat('ZZ Force', 'zzforce8',
                           'brands=ZZA&override_include=ZZBBAW001SLS&segment=1&wh_brands=ZZA'
                           '&wh_override_include=ZZCCAW002SLS-M&os_override_include=ZZDDAW003SLS')])
        self._reset_cache()
        scope = a._catalog_scope_for_slug('zzforce8')
        self.assertFalse(scope['all'])
        for sku in ('ZZBBAW001SLS', 'ZZCCAW002SLS-M', 'ZZDDAW003SLS'):
            self.assertIn(a.get_base_style(sku), scope['styles'])
            self.assertTrue(a._scope_has_style(scope, sku))
        self.assertFalse(a._scope_has_style(scope, 'ZZEEAW004SLS'))        # anything else stays out

    def test_a_catalog_of_force_includes_only_is_not_served_everything(self):
        a = self.a
        self.s3.store([cat('ZZ Only Force', 'zzonly99',
                           'brands=__SEG__&segment=1&wh_override_include=ZZBBAW001SLS')])
        self._reset_cache()
        scope = a._catalog_scope_for_slug('zzonly99')
        self.assertFalse(scope['all'])                                     # it used to fall back to "everything"
        self.assertTrue(a._scope_has_style(scope, 'ZZBBAW001SLS'))
        self.assertFalse(a._scope_has_style(scope, 'ZZEEAW004SLS'))


if __name__ == '__main__':
    unittest.main()
