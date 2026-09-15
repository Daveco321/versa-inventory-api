"""
Synthetic tests for pnl_store.py and the store-facing P&L routes.

Fail-closed encryption (missing or bad key: 503 and zero writes), tamper and
envelope checks (500 PNL_STORE_UNREADABLE), optimistic concurrency (409 on a
stale expected_etag), key rotation, versions and restore, the S3 backend
mapping (fake client, no network) and "ciphertext only" on disk. Also the
wrong-but-valid key case: writes over data the keys cannot open are refused,
status says store:keys_mismatch, and each object names the key id it needs.

SYNTHETIC ONLY: sentinel numbers, made-up codes. Run from the repo root:
    python -B -m unittest discover -s tests -p "test_pnl_[as]*.py" -v
"""
import hashlib
import io
import json
import os
import re
import sys
import time
import unittest
from datetime import datetime, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
for _p in (REPO, HERE):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from botocore.exceptions import ClientError, ParamValidationError  # noqa: E402

import pnl  # noqa: E402
from pnl_store import (EncryptedStore, LocalDirStore, S3Store, StoreNotConfigured,  # noqa: E402
                       StoreNotFound, StoreUnavailable, StoreUnreadable, VersionConflict, key_id)
from test_pnl_auth import (ADMIN, SENTINEL_COST, SENTINEL_LABEL, SENTINEL_TEXT,  # noqa: E402
                           PnlTestCase, RecordingBackend, files_under, new_key, synthetic_costbook)

CB, ST, OV = 'costbook.v1.bin', 'settings.v1.bin', 'overrides.v1.bin'
_ABSENT = object()
_KID_RE = re.compile(r'^[0-9a-f]{12}$')


def payload():
    return {'label': SENTINEL_LABEL, 'price_usd': SENTINEL_COST, 'rows': [[1, SENTINEL_COST]]}


def tamper(path):
    with open(path, 'rb') as f:
        data = bytearray(f.read())
    i = len(data) // 2
    data[i] = ord('A') if data[i] != ord('A') else ord('B')
    with open(path, 'wb') as f:
        f.write(bytes(data))


def is_kid_file(path):
    return os.sep + '.meta' + os.sep in path and path.endswith('.kid')


def snapshot(d):
    out = {}
    for p in files_under(d):
        with open(p, 'rb') as f:
            out[p] = f.read()
    return out


class FakeS3:
    """In-memory, versioned stand-in for the boto3 S3 client. No network."""

    def __init__(self, page_size=1000, conditional=True, fail=None):
        self.objects = {}
        self.calls = []
        self.page_size = page_size
        self.conditional = conditional
        self.fail = fail
        self._seq = 0

    @staticmethod
    def _err(code, status, op):
        return ClientError({'Error': {'Code': code, 'Message': 'x'},
                            'ResponseMetadata': {'HTTPStatusCode': status}}, op)

    def _latest(self, key):
        vs = self.objects.get(key) or []
        return vs[-1] if vs else None

    def get_object(self, Bucket, Key, VersionId=None):
        self.calls.append(('get', Bucket, Key, VersionId))
        if self.fail:
            raise self._err(self.fail, 403, 'GetObject')
        vs = self.objects.get(Key) or []
        if VersionId is None:
            v = vs[-1] if vs else None
            if v is None:
                raise self._err('NoSuchKey', 404, 'GetObject')
        else:
            v = next((x for x in vs if x['vid'] == VersionId), None)
            if v is None:
                raise self._err('NoSuchVersion', 404, 'GetObject')
        return {'Body': io.BytesIO(v['body']), 'ETag': '"%s"' % v['etag'], 'VersionId': v['vid'],
                'Metadata': dict(v['meta'])}

    def head_object(self, Bucket, Key):
        self.calls.append(('head', Bucket, Key))
        if self.fail:
            raise self._err(self.fail, 403, 'HeadObject')
        v = self._latest(Key)
        if v is None:
            raise self._err('404', 404, 'HeadObject')
        return {'ETag': '"%s"' % v['etag'], 'ContentLength': len(v['body']), 'Metadata': dict(v['meta'])}

    def put_object(self, **kw):
        self.calls.append(('put', dict(kw)))
        if self.fail:
            raise self._err(self.fail, 403, 'PutObject')
        if not self.conditional and ('IfMatch' in kw or 'IfNoneMatch' in kw):
            raise ParamValidationError(report='Unknown parameter in input: "IfMatch"')
        key, cur = kw['Key'], self._latest(kw['Key'])
        if kw.get('IfNoneMatch') == '*' and cur is not None:
            raise self._err('PreconditionFailed', 412, 'PutObject')
        if 'IfMatch' in kw and (cur is None or kw['IfMatch'].strip('"') != cur['etag']):
            raise self._err('PreconditionFailed', 412, 'PutObject')
        self._seq += 1
        body = bytes(kw['Body'])
        v = {'vid': 'v%04d' % self._seq, 'body': body, 'etag': hashlib.md5(body).hexdigest(),
             'at': datetime(2026, 1, 1, 0, 0, self._seq % 60, tzinfo=timezone.utc),
             'meta': dict(kw.get('Metadata') or {})}
        self.objects.setdefault(key, []).append(v)
        return {'ETag': '"%s"' % v['etag'], 'VersionId': v['vid']}

    def list_object_versions(self, Bucket, Prefix, MaxKeys=1000, KeyMarker=None, VersionIdMarker=None):
        self.calls.append(('list', Prefix, KeyMarker, VersionIdMarker))
        if self.fail:
            raise self._err(self.fail, 403, 'ListObjectVersions')
        entries = []
        for key in sorted(self.objects):
            if key.startswith(Prefix):
                for i, v in enumerate(reversed(self.objects[key])):
                    entries.append({'Key': key, 'VersionId': v['vid'], 'LastModified': v['at'],
                                    'Size': len(v['body']), 'IsLatest': i == 0})
        start = 0
        if KeyMarker is not None:
            start = 1 + next(i for i, e in enumerate(entries)
                             if e['Key'] == KeyMarker and e['VersionId'] == VersionIdMarker)
        page = entries[start:start + min(MaxKeys, self.page_size)]
        out = {'Versions': page, 'IsTruncated': start + len(page) < len(entries),
               'DeleteMarkers': [{'Key': Prefix, 'VersionId': 'dm1', 'IsLatest': False}]}
        if out['IsTruncated']:
            out['NextKeyMarker'], out['NextVersionIdMarker'] = page[-1]['Key'], page[-1]['VersionId']
        return out


# ─────────────────────────────────────────────────────────────────────────────
class TestEncryptedStore(PnlTestCase):

    def _dir(self):
        return os.path.join(self._root, 'store-%d' % len(os.listdir(self._root)))

    def test_missing_or_invalid_keys_fail_closed_with_zero_writes(self):
        cases = ((None, 'keys_missing'), ('', 'keys_missing'), (' , ', 'keys_missing'),
                 ('abc', 'keys_invalid'), (new_key() + ',abc', 'keys_invalid'))
        for keys, reason in cases:
            d = self._dir()
            rec = RecordingBackend(LocalDirStore(d))
            st = EncryptedStore(rec, keys)
            self.assertFalse(st.ok)
            self.assertEqual(st.reason, reason, keys)
            self.assertEqual((st.key_ids, st.kid), ([], None))
            for call in (lambda: st.get_obj(CB), lambda: st.put_obj(CB, payload()),
                         lambda: st.rotate(CB), lambda: st.get_version_obj(CB, 'x'),
                         lambda: st.restore(CB, 'x')):
                with self.assertRaises(StoreNotConfigured):
                    call()
            self.assertEqual(rec.puts, [])
            self.assertEqual(files_under(d), [])
        st = EncryptedStore(None, new_key())
        self.assertEqual((st.ok, st.reason), (False, 'no_backend'))
        self.assertNotIn(new_key()[:10], repr(st))

    def test_crypto_library_missing_fails_closed(self):
        key = new_key()
        saved = sys.modules.get('cryptography.fernet', _ABSENT)
        sys.modules['cryptography.fernet'] = None
        try:
            st = EncryptedStore(RecordingBackend(LocalDirStore(self._dir())), key)
        finally:
            if saved is _ABSENT:
                sys.modules.pop('cryptography.fernet', None)
            else:
                sys.modules['cryptography.fernet'] = saved
        self.assertEqual((st.ok, st.reason), (False, 'crypto_unavailable'))
        with self.assertRaises(StoreNotConfigured):
            st.put_obj(CB, payload())

    def test_round_trip_holds_ciphertext_only(self):
        d = self._dir()
        rec = RecordingBackend(LocalDirStore(d))
        key = new_key()
        st = EncryptedStore(rec, key)
        self.assertTrue(st.ok)
        self.assertEqual(st.key_count, 1)
        self.assertRegex(st.fingerprint, _KID_RE)
        self.assertEqual((st.kid, st.key_ids), (key_id(key), [key_id(key)]))
        self.assertEqual(st.get_obj(CB), (None, None))
        etag = st.put_obj(CB, payload())
        self.assertEqual(st.get_obj(CB), (payload(), etag))
        self.assertEqual(st.head(CB), etag)
        self.assertEqual(len(rec.puts), 1)
        paths = files_under(d)
        self.assertGreaterEqual(len(paths), 3)                 # object, its version copy, its key id
        for p in paths:
            with open(p, 'rb') as f:
                raw = f.read()
            if is_kid_file(p):
                self.assertEqual(raw.decode('ascii'), st.kid, p)   # the key id, never the key
                continue
            self.assertTrue(raw.startswith(b'gAAAAA'), p)       # a Fernet token
            for bad in (SENTINEL_LABEL.encode(), SENTINEL_TEXT.encode(), b'price_usd', b'"k"'):
                self.assertNotIn(bad, raw, p)

    def test_decrypted_copy_is_cached_per_etag(self):
        rec = RecordingBackend(LocalDirStore(self._dir()))
        st = EncryptedStore(rec, new_key())
        st.put_obj(CB, payload())
        gets = rec.gets
        a, _ = st.get_obj(CB)
        b, _ = st.get_obj(CB)
        self.assertEqual(rec.gets, gets)                       # served from the per-etag cache
        a['label'] = 'changed'
        self.assertEqual(b['label'], SENTINEL_LABEL)           # each caller gets its own copy

    def test_tampered_ciphertext_is_unreadable(self):
        d = self._dir()
        key = new_key()
        st = EncryptedStore(LocalDirStore(d), key)
        st.put_obj(CB, payload())
        tamper(os.path.join(d, CB))
        for store in (st, EncryptedStore(LocalDirStore(d), key)):
            with self.assertRaises(StoreUnreadable) as cm:
                store.get_obj(CB)
            self.assertEqual(str(cm.exception), 'token')
            self.assertEqual(cm.exception.obj, CB)
            self.assertEqual(store.diagnose(CB, 'token'), 'damaged')     # right key id, bad bytes

    def test_envelope_bound_to_object_name(self):
        backend = LocalDirStore(self._dir())
        st = EncryptedStore(backend, new_key())
        st.put_obj(CB, payload())
        raw, _etag = backend.get(CB)
        backend.put(ST, raw)                                   # swap the blob under another name
        with self.assertRaises(StoreUnreadable) as cm:
            st.get_obj(ST)
        self.assertEqual(str(cm.exception), 'envelope')
        with self.assertRaises(StoreUnreadable):
            st.rotate(ST)                                      # never rotates a swapped blob
        self.assertEqual(st.get_obj(CB)[0], payload())

    def test_wrong_key_is_unreadable(self):
        backend = LocalDirStore(self._dir())
        EncryptedStore(backend, new_key()).put_obj(CB, payload())
        other = EncryptedStore(backend, new_key())
        with self.assertRaises(StoreUnreadable):
            other.get_obj(CB)
        self.assertEqual(other.diagnose(CB, 'token'), 'keys_mismatch')

    def test_expected_etag_semantics(self):
        backend = LocalDirStore(self._dir())
        st = EncryptedStore(backend, new_key())
        e1 = st.put_obj(CB, {'n': 1}, expected_etag='')       # create-only on an absent object
        with self.assertRaises(VersionConflict):
            st.put_obj(CB, {'n': 2}, expected_etag='')         # exists now
        with self.assertRaises(VersionConflict):
            st.put_obj(CB, {'n': 2}, expected_etag='stale')
        self.assertEqual(st.get_obj(CB), ({'n': 1}, e1))       # nothing was written
        e2 = st.put_obj(CB, {'n': 3}, expected_etag=e1)
        e3 = st.put_obj(CB, {'n': 4})                          # None: no caller check
        self.assertNotIn(e3, (e1, e2))
        with self.assertRaises(TypeError):
            st.put_obj(CB, {'n': 5}, expected_etag=5)

    def test_non_finite_values_are_refused_before_any_write(self):
        rec = RecordingBackend(LocalDirStore(self._dir()))
        st = EncryptedStore(rec, new_key())
        with self.assertRaises(ValueError):
            st.put_obj(CB, {'x': float('nan')})
        self.assertEqual(rec.puts, [])

    def test_rotation_moves_objects_to_the_newest_key(self):
        backend = LocalDirStore(self._dir())
        ka, kb = new_key(), new_key()
        EncryptedStore(backend, ka).put_obj(CB, payload())
        both = EncryptedStore(backend, kb + ',' + ka)
        self.assertEqual(both.key_count, 2)
        self.assertEqual(both.key_ids, [key_id(kb), key_id(ka)])
        self.assertEqual(both.object_kid(CB), key_id(ka))
        self.assertEqual(both.get_obj(CB)[0], payload())
        self.assertTrue(both.rotate(CB))
        self.assertEqual(both.object_kid(CB), key_id(kb))      # the new key id travels with the object
        self.assertIsNone(both.rotate(ST))                     # absent object
        self.assertEqual(EncryptedStore(backend, kb).get_obj(CB)[0], payload())
        with self.assertRaises(StoreUnreadable):
            EncryptedStore(backend, ka).get_obj(CB)

    def test_versions_and_restore(self):
        backend = LocalDirStore(self._dir())
        st = EncryptedStore(backend, new_key())
        for n in (1, 2, 3):
            st.put_obj(CB, {'n': n})
            time.sleep(0.005)
        vs = st.versions(CB)
        self.assertEqual(len(vs), 3)
        self.assertEqual([v['current'] for v in vs], [True, False, False])
        self.assertEqual(st.get_version_obj(CB, vs[-1]['id']), {'n': 1})
        st.restore(CB, vs[-1]['id'])
        self.assertEqual(st.get_obj(CB)[0], {'n': 1})
        self.assertEqual(len(st.versions(CB)), 4)
        with self.assertRaises(StoreNotFound):
            st.get_version_obj(CB, '9999999999999-ffffffffffff')
        with self.assertRaises(StoreNotFound):
            st.get_version_obj(CB, '../../x')

    def test_names_are_validated(self):
        st = EncryptedStore(LocalDirStore(self._dir()), new_key())
        for bad in ('../x', 'a/b', 'A.bin', '', 'x' * 80, None):
            with self.assertRaises(ValueError):
                st.put_obj(bad, {})
            with self.assertRaises(ValueError):
                st.head(bad)


class TestKeyMismatch(PnlTestCase):
    """A valid but wrong key passes the self-test. It must never be able to overwrite data
    the right key can read (that would split the store across two keys)."""

    def _dir(self):
        return os.path.join(self._root, 'km-%d' % len(os.listdir(self._root)))

    def _written_with_k1(self):
        d = self._dir()
        k1, k2 = new_key(), new_key()
        s1 = EncryptedStore(LocalDirStore(d), k1)
        s1.put_obj(CB, payload())
        s1.put_obj(ST, {'fx': {'rate': None}})
        rec = RecordingBackend(LocalDirStore(d))
        return d, k1, k2, s1, EncryptedStore(rec, k2), rec

    def test_wrong_key_refuses_to_overwrite_and_writes_nothing(self):
        d, k1, k2, s1, s2, rec = self._written_with_k1()
        self.assertTrue(s2.ok)                                  # the self-test cannot tell
        before = snapshot(d)
        for name in (CB, ST):
            for expected in (None, s2.head(name)):
                with self.assertRaises(StoreUnreadable) as cm:
                    s2.put_obj(name, payload(), expected_etag=expected)
                self.assertEqual(str(cm.exception), 'current_unreadable')
                self.assertEqual(cm.exception.obj, name)
        vs = s2.versions(CB)
        with self.assertRaises(StoreUnreadable):
            s2.restore(CB, vs[-1]['id'])                        # opens the old version first
        with self.assertRaises(StoreUnreadable):
            s2.rotate(CB)
        self.assertEqual(rec.puts, [])
        self.assertEqual(snapshot(d), before)                   # not one byte changed
        self.assertEqual(s1.get_obj(CB)[0], payload())          # the right key still opens it
        self.assertEqual(s2.readable(CB)[1:], (False, 'token'))
        self.assertEqual(s2.diagnose(CB, 'token'), 'keys_mismatch')
        self.assertEqual(s2.object_kid(CB), key_id(k1))

    def test_unreadable_etag_is_read_once(self):
        d, k1, k2, s1, s2, rec = self._written_with_k1()
        with self.assertRaises(StoreUnreadable):
            s2.get_obj(CB)
        gets = rec.gets
        for _ in range(3):
            with self.assertRaises(StoreUnreadable):
                s2.get_obj(CB)
            self.assertEqual(s2.readable(CB)[1], False)
            with self.assertRaises(StoreUnreadable):
                s2.put_obj(CB, payload())
        self.assertEqual(rec.gets, gets)                        # the failure is cached per etag
        s1.put_obj(CB, {'n': 2})                                # a new etag is looked at again
        with self.assertRaises(StoreUnreadable):
            s2.get_obj(CB)
        self.assertEqual(rec.gets, gets + 1)

    def test_replace_unreadable_is_an_explicit_escape_hatch(self):
        d, k1, k2, s1, s2, rec = self._written_with_k1()
        etag = s2.put_obj(ST, {'fresh': True}, replace_unreadable=True)
        self.assertEqual(s2.get_obj(ST), ({'fresh': True}, etag))
        self.assertEqual(s2.object_kid(ST), key_id(k2))
        with self.assertRaises(StoreUnreadable):
            s1.get_obj(ST)
        with self.assertRaises(VersionConflict):                # still honors expected_etag
            s2.put_obj(CB, {}, expected_etag='stale', replace_unreadable=True)

    def test_wrong_key_refuses_to_create_an_absent_object(self):
        d, k1, k2, s1, s2, rec = self._written_with_k1()
        self.assertIsNone(s2.head(OV))                          # overrides were never saved
        before = snapshot(d)
        for expected in (None, ''):
            with self.assertRaises(StoreUnreadable) as cm:
                s2.put_obj(OV, [], expected_etag=expected)
            self.assertEqual(str(cm.exception), 'keys_mismatch')
            self.assertEqual(cm.exception.obj, CB)              # the object that needs the other key
            self.assertEqual(s2.diagnose(cm.exception.obj, str(cm.exception)), 'keys_mismatch')
        self.assertIsNone(s2.head(OV))
        self.assertEqual(rec.puts, [])
        self.assertEqual(snapshot(d), before)                   # the same bytes and the same file list
        etag = s1.put_obj(OV, [])                               # the key that opens the store may create it
        self.assertEqual(s1.get_obj(OV), ([], etag))

    def test_creating_is_allowed_when_no_object_needs_another_key(self):
        d, key = self._dir(), new_key()
        st = EncryptedStore(LocalDirStore(d), key)
        st.put_obj(CB, payload())
        self.assertTrue(st.put_obj(ST, {'fx': {'rate': None}}))  # every stored object opens
        tamper(os.path.join(d, ST))                              # damaged, but written with this key
        fresh = EncryptedStore(LocalDirStore(d), key)
        _etag, ok, code = fresh.readable(ST)
        self.assertEqual((ok, fresh.diagnose(ST, code)), (False, 'damaged'))
        self.assertTrue(fresh.put_obj(OV, []))                  # damage is not a second key
        other = EncryptedStore(LocalDirStore(self._dir()), new_key())
        self.assertTrue(other.put_obj(OV, []))                  # an empty store creates freely

    def test_replace_unreadable_may_create_beside_a_foreign_object(self):
        d, k1, k2, s1, s2, rec = self._written_with_k1()
        etag = s2.put_obj(OV, [], replace_unreadable=True)      # the logged escape hatch only
        self.assertEqual(s2.get_obj(OV), ([], etag))
        self.assertEqual(s2.object_kid(OV), key_id(k2))

    def test_both_keys_listed_open_everything_and_rotate_heals(self):
        d, k1, k2, s1, s2, rec = self._written_with_k1()
        s2.put_obj(OV, [], replace_unreadable=True)             # a split store (only the escape hatch makes one now)
        both = EncryptedStore(LocalDirStore(d), k2 + ',' + k1)
        for name in (CB, ST, OV):
            self.assertIsNotNone(both.get_obj(name)[1])
            self.assertTrue(both.rotate(name))
        only2 = EncryptedStore(LocalDirStore(d), k2)
        for name in (CB, ST, OV):
            self.assertEqual(only2.readable(name)[1], True)
            self.assertEqual(only2.object_kid(name), key_id(k2))

    def test_write_is_conditional_on_the_checked_etag(self):
        d = self._dir()
        key = new_key()
        s1 = EncryptedStore(LocalDirStore(d), key)
        s1.put_obj(CB, {'n': 1})

        class Racing(RecordingBackend):
            race = True

            def get(self, name):
                out = super().get(name)
                if self.race:                                   # another worker writes right after
                    self.race = False
                    s1.put_obj(CB, {'n': 'other'})
                return out
        s2 = EncryptedStore(Racing(LocalDirStore(d)), key)
        with self.assertRaises(VersionConflict):
            s2.put_obj(CB, {'n': 2})
        self.assertEqual(s1.get_obj(CB)[0], {'n': 'other'})

    def test_key_ids_are_not_secret_and_differ_per_key(self):
        k1, k2 = new_key(), new_key()
        self.assertRegex(key_id(k1), _KID_RE)
        self.assertNotEqual(key_id(k1), key_id(k2))
        self.assertNotIn(key_id(k1), k1)
        self.assertNotEqual(key_id(k1), hashlib.sha256(k1.encode()).hexdigest()[:12])


class TestLocalDirStore(PnlTestCase):

    def test_atomic_put_etag_and_versions(self):
        d = os.path.join(self._root, 'local')
        s = LocalDirStore(d)
        etag = s.put(CB, b'abc')
        self.assertEqual(etag, hashlib.sha256(b'abc').hexdigest()[:32])
        self.assertEqual(s.head(CB), etag)
        self.assertEqual(s.get(CB), (b'abc', etag))
        self.assertEqual([p for p in files_under(d) if os.path.basename(p).startswith('.tmp-')], [])
        with self.assertRaises(VersionConflict):
            s.put(CB, b'def', expected_etag='stale')
        with self.assertRaises(VersionConflict):
            s.put(CB, b'def', expected_etag='')
        s.put(CB, b'def', expected_etag=etag)
        vs = s.versions(CB)
        self.assertEqual(len(vs), 2)
        self.assertEqual(sum(v['current'] for v in vs), 1)
        self.assertEqual(s.get_version(CB, vs[-1]['id']), b'abc')
        for bad in ('../../etc/passwd', 'abc', '', None, 5):
            self.assertIsNone(s.get_version(CB, bad))
        with self.assertRaises(TypeError):
            s.put(CB, 'text')
        self.assertEqual(s.versions(ST), [])

    def test_key_id_sidecar(self):
        d = os.path.join(self._root, 'local-kid')
        s = LocalDirStore(d)
        self.assertIsNone(s.head_kid(CB))
        s.put(CB, b'abc', kid='0123456789ab')
        self.assertEqual(s.head_kid(CB), '0123456789ab')
        s.put(CB, b'def', kid='not a key id')                  # invalid ids are not written
        self.assertIsNone(s.head_kid(CB))
        s.put(CB, b'ghi', kid='ABCDEF012345')
        self.assertEqual(s.head_kid(CB), 'abcdef012345')
        s.put(CB, b'jkl')                                      # a write without an id clears it
        self.assertIsNone(s.head_kid(CB))


class TestS3Store(PnlTestCase):

    def test_keys_headers_and_missing_objects(self):
        fake = FakeS3()
        s3 = S3Store(lambda: fake, 'bucket-x', prefix='inventory/pnl')
        self.assertEqual(s3.key(CB), 'inventory/pnl/costbook.v1.bin')
        self.assertEqual(s3.get(CB), (None, None))
        self.assertIsNone(s3.head(CB))
        self.assertIsNone(s3.head_kid(CB))
        etag = s3.put(CB, b'ciphertext')
        put = [c[1] for c in fake.calls if c[0] == 'put'][-1]
        self.assertEqual(put['Bucket'], 'bucket-x')
        self.assertEqual(put['Key'], 'inventory/pnl/costbook.v1.bin')
        self.assertEqual(put['ContentType'], 'application/octet-stream')
        self.assertEqual(put['CacheControl'], 'no-store')
        for forbidden in ('ACL', 'Metadata', 'IfMatch', 'IfNoneMatch'):
            self.assertNotIn(forbidden, put)
        self.assertEqual(s3.get(CB), (b'ciphertext', etag))
        self.assertEqual(s3.head(CB), etag)
        for bad in ('../x', 'a/b', 'A.bin', ''):
            with self.assertRaises(ValueError):
                s3.key(bad)

    def test_conditional_writes(self):
        fake = FakeS3()
        s3 = S3Store(lambda: fake, 'b')
        e1 = s3.put(CB, b'one', expected_etag='')
        self.assertEqual([c[1] for c in fake.calls if c[0] == 'put'][-1]['IfNoneMatch'], '*')
        with self.assertRaises(VersionConflict):
            s3.put(CB, b'two', expected_etag='')
        with self.assertRaises(VersionConflict):
            s3.put(CB, b'two', expected_etag='stale')
        e2 = s3.put(CB, b'two', expected_etag=e1)
        self.assertEqual([c[1] for c in fake.calls if c[0] == 'put'][-1]['IfMatch'], '"%s"' % e1)
        s3.head = lambda name: e2                              # another worker wrote in between
        fake.put_object(Bucket='b', Key=s3.key(CB), Body=b'three')
        with self.assertRaises(VersionConflict):
            s3.put(CB, b'four', expected_etag=e2)              # S3 answers 412

    def test_old_botocore_without_conditional_writes(self):
        fake = FakeS3(conditional=False)
        s3 = S3Store(lambda: fake, 'b')
        etag = s3.put(CB, b'one', expected_etag='')
        self.assertEqual(s3.get(CB), (b'one', etag))
        self.assertNotIn('IfNoneMatch', [c[1] for c in fake.calls if c[0] == 'put'][-1])

    def test_errors_map_to_unavailable(self):
        s3 = S3Store(lambda: FakeS3(fail='AccessDenied'), 'b')
        for call in (lambda: s3.get(CB), lambda: s3.head(CB), lambda: s3.put(CB, b'x'),
                     lambda: s3.versions(CB), lambda: s3.get_version(CB, 'v0001'), lambda: s3.head_kid(CB)):
            with self.assertRaises(StoreUnavailable):
                call()

        def broken():
            raise RuntimeError('no credentials')
        with self.assertRaises(StoreUnavailable):
            S3Store(broken, 'b').head(CB)

    def test_versions_exact_key_and_pagination(self):
        fake = FakeS3(page_size=2)
        s3 = S3Store(lambda: fake, 'b')
        for body in (b'1', b'2', b'3'):
            s3.put(CB, body)
        s3.put('costbook.v1.bin.bak', b'other')               # same prefix, different key
        vs = s3.versions(CB)
        self.assertEqual(len(vs), 3)
        self.assertEqual(sum(v['current'] for v in vs), 1)
        self.assertTrue(vs[0]['current'])
        self.assertEqual([v['id'] for v in vs], ['v0003', 'v0002', 'v0001'])
        self.assertGreater(len([c for c in fake.calls if c[0] == 'list']), 1)
        self.assertEqual(s3.get_version(CB, 'v0001'), b'1')
        self.assertIsNone(s3.get_version(CB, 'v9999'))
        n = len(fake.calls)
        self.assertIsNone(s3.get_version(CB, 'bad id!'))
        self.assertEqual(len(fake.calls), n)                   # rejected without calling S3

    def test_encrypted_over_s3_stores_ciphertext_and_the_key_id(self):
        fake = FakeS3()
        st = EncryptedStore(S3Store(lambda: fake, 'b'), new_key())
        etag = st.put_obj(CB, synthetic_costbook(2))
        put = [c[1] for c in fake.calls if c[0] == 'put'][-1]
        self.assertEqual(put['Metadata'], {'pnl-kid': st.kid})  # outside the ciphertext
        self.assertEqual(put['IfNoneMatch'], '*')                # every write is conditional
        self.assertEqual(st.get_obj(CB), (synthetic_costbook(2), etag))
        self.assertEqual(st.object_kid(CB), st.kid)
        st.put_obj(CB, synthetic_costbook(1))
        self.assertEqual([c[1] for c in fake.calls if c[0] == 'put'][-1]['IfMatch'], '"%s"' % etag)
        for versions in fake.objects.values():
            for v in versions:
                self.assertTrue(v['body'].startswith(b'gAAAAA'))
                for bad in (SENTINEL_LABEL.encode(), SENTINEL_TEXT.encode(), b'records'):
                    self.assertNotIn(bad, v['body'])


# ─────────────────────────────────────────────────────────────────────────────
class TestStoreRoutes(PnlTestCase):

    def test_missing_key_gives_503_and_zero_writes(self):
        h = self.harness(keys='')
        calls = [('POST', '/api/pnl/costbook', synthetic_costbook(2)),
                 ('POST', '/api/pnl/costbook', {'costbook': synthetic_costbook(2), 'expected_etag': None}),
                 ('POST', '/api/pnl/settings', {'settings': {}}),
                 ('POST', '/api/pnl/overrides', {'overrides': []}),
                 ('POST', '/api/pnl/costbook/restore', {'version_id': 'x'}),
                 ('POST', '/api/pnl/rotate', {}),
                 ('GET', '/api/pnl/costbook', None), ('GET', '/api/pnl/settings', None),
                 ('GET', '/api/pnl/overrides', None), ('GET', '/api/pnl/costbook/versions', None),
                 ('GET', '/api/pnl/dataset', None)]
        for method, path, body in calls:
            self.assert_error(h.admin(method, path, body=body), 503, 'PNL_NOT_CONFIGURED',
                              '%s %s' % (method, path), reason='store')
        self.assertEqual(h.backend.puts, [])
        self.assertEqual(files_under(h.store_dir), [])

    def test_tampered_costbook_gives_500_unreadable(self):
        h = self.harness()
        h.upload()
        tamper(os.path.join(h.store_dir, CB))
        self.assert_error(h.admin('GET', '/api/pnl/costbook'), 500, 'PNL_STORE_UNREADABLE', reason='damaged')
        self.assert_error(h.dataset(), 500, 'PNL_STORE_UNREADABLE', reason='damaged')
        st = h.admin('GET', '/api/pnl/status')
        self.assertEqual(st.status_code, 200)
        body = st.get_json()
        self.assertIs(body['objects']['costbook']['readable'], False)
        self.assertEqual(body['objects']['costbook']['problem'], 'damaged')
        self.assertIn('store:damaged', body['reasons'])
        self.assertFalse(body['configured'])
        self.assert_clean_text(st.get_data(as_text=True))

    def test_envelope_mismatch_gives_500_unreadable(self):
        h = self.harness()
        h.upload()
        raw, _etag = h.backend.inner.get(CB)
        h.backend.inner.put(ST, raw)
        h.backend.inner.put(OV, raw)
        self.assert_error(h.admin('GET', '/api/pnl/settings'), 500, 'PNL_STORE_UNREADABLE', reason='damaged')
        self.assert_error(h.admin('GET', '/api/pnl/overrides'), 500, 'PNL_STORE_UNREADABLE', reason='damaged')
        st = h.admin('GET', '/api/pnl/status').get_json()
        self.assertIs(st['objects']['overrides']['readable'], False)
        self.assertIs(st['objects']['settings']['readable'], False)
        self.assertIs(st['objects']['costbook']['readable'], True)

    def test_stale_expected_etag_gives_409(self):
        h = self.harness()
        s1 = h.admin('POST', '/api/pnl/settings', body={'settings': {}, 'expected_etag': None})
        self.assertEqual(s1.status_code, 200)
        s1 = s1.get_json()['etag']
        self.assert_error(h.admin('POST', '/api/pnl/settings', body={'settings': {}, 'expected_etag': None}),
                          409, 'VERSION_CONFLICT')
        self.assert_error(h.admin('POST', '/api/pnl/settings', body={'settings': {}, 'expected_etag': 'stale'}),
                          409, 'VERSION_CONFLICT')
        self.assertEqual(h.admin('POST', '/api/pnl/settings',
                                 body={'settings': {}, 'expected_etag': s1}).status_code, 200)
        self.assertEqual(h.admin('POST', '/api/pnl/settings', body={'settings': {}}).status_code, 200)
        c1 = h.upload()
        self.assert_error(h.admin('POST', '/api/pnl/costbook',
                                  body={'costbook': synthetic_costbook(1), 'expected_etag': 'stale'}),
                          409, 'VERSION_CONFLICT')
        self.assert_error(h.admin('POST', '/api/pnl/costbook', body=synthetic_costbook(1),
                                  query={'expected_etag': 'stale'}), 409, 'VERSION_CONFLICT')
        r = h.admin('POST', '/api/pnl/costbook', body=synthetic_costbook(1), query={'expected_etag': c1})
        self.assertEqual(r.status_code, 200)
        self.assert_error(h.admin('POST', '/api/pnl/overrides', body={'overrides': [], 'expected_etag': 'stale'}),
                          409, 'VERSION_CONFLICT')
        self.assertEqual(h.admin('POST', '/api/pnl/overrides',
                                 body={'overrides': [], 'expected_etag': None}).status_code, 200)
        self.assert_error(h.admin('POST', '/api/pnl/settings', body={'settings': {}, 'expected_etag': 7}),
                          400, 'BAD_REQUEST')

    def test_rotate_route(self):
        ka, kb = new_key(), new_key()
        h1 = self.harness(keys=ka)
        h1.upload()
        self.assertEqual(h1.admin('POST', '/api/pnl/settings', body={'settings': {}}).status_code, 200)
        h2 = self.harness(keys=kb + ',' + ka, backend=RecordingBackend(LocalDirStore(h1.store_dir)))
        r = h2.admin('POST', '/api/pnl/rotate')
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.get_json(), {'objects': {CB: 'rotated', ST: 'rotated', OV: 'absent'}, 'keys': 2})
        only_b = EncryptedStore(LocalDirStore(h1.store_dir), kb)
        self.assertEqual(len(only_b.get_obj(CB)[0]['records']), 3)
        with self.assertRaises(StoreUnreadable):
            EncryptedStore(LocalDirStore(h1.store_dir), ka).get_obj(CB)

    def test_versions_and_restore_routes(self):
        h = self.harness()
        h.upload(3)
        time.sleep(0.005)
        h.upload(5)
        r = h.admin('GET', '/api/pnl/costbook/versions')
        self.assertEqual(r.status_code, 200)
        v = r.get_json()
        self.assertTrue(v['supported'])
        self.assertEqual(len(v['versions']), 2)
        self.assertEqual([x['current'] for x in v['versions']], [True, False])
        self.assertEqual(set(v['versions'][0]), {'id', 'at', 'size', 'current'})
        oldest = v['versions'][-1]['id']
        self.assert_error(h.admin('POST', '/api/pnl/costbook/restore',
                                  body={'version_id': oldest, 'expected_etag': 'stale'}), 409, 'VERSION_CONFLICT')
        r = h.admin('POST', '/api/pnl/costbook/restore', body={'version_id': oldest})
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.get_json()['records'], 3)
        self.assertEqual(r.get_json()['paramChangeCount'], 0)
        self.assertEqual(len(h.admin('GET', '/api/pnl/costbook').get_json()['costbook']['records']), 3)
        self.assert_error(h.admin('POST', '/api/pnl/costbook/restore',
                                  body={'version_id': '9999999999999-ffffffffffff'}), 404, 'NOT_FOUND')
        self.assert_error(h.admin('POST', '/api/pnl/costbook/restore', body={'version_id': 5}),
                          400, 'BAD_REQUEST')

    def test_versions_route_without_backend_support(self):
        class NoVersions:
            def __init__(self, inner):
                self.inner = inner

            def get(self, name):
                return self.inner.get(name)

            def head(self, name):
                return self.inner.head(name)

            def put(self, name, data, expected_etag=None):
                return self.inner.put(name, data, expected_etag=expected_etag)
        h = self.harness(backend=NoVersions(LocalDirStore(os.path.join(self._root, 'nov'))))
        self.assertEqual(h.admin('GET', '/api/pnl/costbook/versions').get_json(), {'supported': False, 'versions': []})
        self.assert_error(h.admin('POST', '/api/pnl/costbook/restore', body={'version_id': 'x'}), 404, 'NOT_FOUND')
        st = h.admin('GET', '/api/pnl/status').get_json()
        self.assertIs(st['versions'], False)
        h.upload()                                             # a backend without key ids still works
        st = h.admin('GET', '/api/pnl/status').get_json()
        self.assertEqual((st['objects']['costbook']['readable'], st['objects']['costbook']['kid']), (True, None))

    def test_store_never_holds_plaintext_after_a_full_flow(self):
        h = self.harness()
        h.upload(3)
        h.admin('POST', '/api/pnl/settings', body={'settings': {'confirmed': {'fx': True}}})
        h.admin('POST', '/api/pnl/overrides', body={'overrides': [
            {'id': 'o1', 'scope': 'style', 'key': {'style': 'ZZAAAA001SLS'}, 'fobU': SENTINEL_COST}]})
        self.assertEqual(h.dataset().status_code, 200)
        paths = files_under(h.store_dir)
        self.assertGreaterEqual(len(paths), 6)
        for p in paths:
            with open(p, 'rb') as f:
                raw = f.read()
            if is_kid_file(p):
                self.assertEqual(raw.decode('ascii'), h.store.kid, p)
                continue
            self.assertTrue(raw.startswith(b'gAAAAA'), p)
            for bad in (SENTINEL_LABEL, SENTINEL_TEXT, ADMIN, 'records', 'fobU', 'ZZAAAA001SLS'):
                self.assertNotIn(bad.encode(), raw, '%s holds %r' % (p, bad))
        self.assertEqual({os.path.basename(p) for p in files_under(h.store_dir)
                          if os.sep + '.versions' + os.sep not in p and not is_kid_file(p)}, {CB, ST, OV})


class TestKeyMismatchRoutes(PnlTestCase):
    """Objects written under one key, the server restarted with another (valid) key."""

    def _mismatch(self):
        k1, k2 = new_key(), new_key()
        h1 = self.harness(keys=k1)
        h1.upload(3)
        self.assertEqual(h1.admin('POST', '/api/pnl/settings', body={'settings': {}}).status_code, 200)
        self.assertEqual(h1.admin('POST', '/api/pnl/overrides', body={'overrides': []}).status_code, 200)
        h2 = self.harness(keys=k2, backend=RecordingBackend(LocalDirStore(h1.store_dir)))
        return k1, k2, h1, h2

    def test_status_reports_keys_mismatch_and_every_write_is_refused(self):
        k1, k2, h1, h2 = self._mismatch()
        before = snapshot(h1.store_dir)
        st = h2.admin('GET', '/api/pnl/status').get_json()
        self.assertFalse(st['configured'])
        self.assertIn('store:keys_mismatch', st['reasons'])
        self.assertEqual(st['keys'], {'count': 1, 'ids': [key_id(k2)]})
        for label in ('costbook', 'settings', 'overrides'):
            o = st['objects'][label]
            self.assertEqual((o['present'], o['readable'], o['problem']), (True, False, 'keys_mismatch'), label)
            self.assertEqual((o['kid'], o['keyKnown']), (key_id(k1), False), label)
        heads = {label: st['objects'][label]['etag'] for label in ('costbook', 'settings')}
        refused = [
            ('POST', '/api/pnl/costbook', synthetic_costbook(2)),
            ('POST', '/api/pnl/costbook', {'costbook': synthetic_costbook(2), 'expected_etag': heads['costbook']}),
            ('POST', '/api/pnl/settings', {'settings': {}, 'expected_etag': heads['settings']}),
            ('POST', '/api/pnl/settings', {'settings': {}}),
        ]
        for method, path, body in refused:
            self.assert_error(h2.admin(method, path, body=body), 500, 'PNL_STORE_UNREADABLE', path,
                              reason='current_unreadable')
        vid = h2.admin('GET', '/api/pnl/costbook/versions').get_json()['versions'][-1]['id']
        for method, path, body in (('POST', '/api/pnl/overrides', {'overrides': []}),
                                   ('POST', '/api/pnl/costbook/restore', {'version_id': vid}),
                                   ('POST', '/api/pnl/rotate', {}),
                                   ('GET', '/api/pnl/costbook', None), ('GET', '/api/pnl/settings', None)):
            self.assert_error(h2.admin(method, path, body=body), 500, 'PNL_STORE_UNREADABLE', path,
                              reason='keys_mismatch')
        self.assert_error(h2.dataset(), 500, 'PNL_STORE_UNREADABLE', reason='keys_mismatch')
        self.assertEqual(h2.backend.puts, [])
        self.assertEqual(snapshot(h1.store_dir), before)
        self.assertNotIn(k1, self._out.getvalue())
        self.assertNotIn(k2, self._out.getvalue())

    def test_replace_unreadable_is_logged_and_explicit(self):
        k1, k2, h1, h2 = self._mismatch()
        r = h2.admin('POST', '/api/pnl/costbook', body={'costbook': synthetic_costbook(2), 'replace_unreadable': 'yes'})
        self.assert_error(r, 500, 'PNL_STORE_UNREADABLE', reason='current_unreadable')   # only literal true
        r = h2.admin('POST', '/api/pnl/costbook', body={'costbook': synthetic_costbook(2), 'replace_unreadable': True})
        self.assertEqual(r.status_code, 200, r.get_data(as_text=True)[:200])
        r = h2.admin('POST', '/api/pnl/settings', body={'settings': {}, 'replace_unreadable': True})
        self.assertEqual(r.status_code, 200)
        r = h2.admin('POST', '/api/pnl/overrides', body={'overrides': [], 'replace_unreadable': True})
        self.assertEqual(r.status_code, 200)
        log = self._out.getvalue()
        self.assertIn('[PnL] costbook saved records=2 params_changed=0 replaced_unreadable=yes user=%s' % ADMIN, log)
        self.assertIn('[PnL] settings saved replaced_unreadable=yes user=%s' % ADMIN, log)
        self.assertIn('[PnL] overrides saved replaced_unreadable=yes user=%s' % ADMIN, log)
        st = h2.admin('GET', '/api/pnl/status').get_json()
        self.assertTrue(st['configured'], st['reasons'])
        self.assertEqual({st['objects'][x]['kid'] for x in ('costbook', 'settings', 'overrides')}, {key_id(k2)})

    def test_wrong_key_refuses_to_create_absent_objects(self):
        k1, k2 = new_key(), new_key()
        h1 = self.harness(keys=k1)
        h1.upload(3)                                           # only the cost book is saved, under k1
        h2 = self.harness(keys=k2, backend=RecordingBackend(LocalDirStore(h1.store_dir)))
        before = snapshot(h1.store_dir)
        st = h2.admin('GET', '/api/pnl/status').get_json()
        self.assertIn('store:keys_mismatch', st['reasons'])
        self.assertEqual((st['objects']['settings']['present'], st['objects']['overrides']['present']), (False, False))
        ov = {'overrides': [{'id': 'o1', 'scope': 'style', 'key': {'style': 'ZZAAAA001SLS'}, 'fobU': SENTINEL_COST}]}
        for path, body in (('/api/pnl/overrides', ov), ('/api/pnl/overrides', dict(ov, expected_etag=None)),
                           ('/api/pnl/overrides', dict(ov, expected_etag='')),
                           ('/api/pnl/overrides', dict(ov, replace_unreadable='yes')),   # only literal true
                           ('/api/pnl/settings', {'settings': {}}),
                           ('/api/pnl/settings', {'settings': {}, 'expected_etag': None})):
            self.assert_error(h2.admin('POST', path, body=body), 500, 'PNL_STORE_UNREADABLE', path,
                              reason='keys_mismatch')
        self.assertEqual(h2.backend.puts, [])
        self.assertEqual(snapshot(h1.store_dir), before)       # the same bytes and the same file list
        st = h2.admin('GET', '/api/pnl/status').get_json()
        self.assertEqual((st['objects']['settings']['present'], st['objects']['overrides']['present']), (False, False))
        self.assertEqual(h1.admin('POST', '/api/pnl/overrides', body=ov).status_code, 200)   # the right key may
        r = h2.admin('POST', '/api/pnl/settings', body={'settings': {}, 'replace_unreadable': True})
        self.assertEqual(r.status_code, 200)
        self.assertIn('[PnL] settings saved replaced_unreadable=yes user=%s' % ADMIN, self._out.getvalue())

    def test_both_keys_then_rotate_restores_health(self):
        k1, k2, h1, h2 = self._mismatch()
        h3 = self.harness(keys=k2 + ',' + k1, backend=RecordingBackend(LocalDirStore(h1.store_dir)))
        st = h3.admin('GET', '/api/pnl/status').get_json()
        self.assertTrue(st['configured'], st['reasons'])
        self.assertEqual({st['objects'][x]['keyKnown'] for x in ('costbook', 'settings', 'overrides')}, {True})
        self.assertEqual(h3.admin('POST', '/api/pnl/rotate').status_code, 200)
        h4 = self.harness(keys=k2, backend=RecordingBackend(LocalDirStore(h1.store_dir)))
        self.assertTrue(h4.admin('GET', '/api/pnl/status').get_json()['configured'])

    def test_boot_store_check_names_the_needed_key(self):
        k1, k2, h1, h2 = self._mismatch()
        h2.svc._check_store_once()
        h1.svc._check_store_once()
        log = self._out.getvalue()
        self.assertIn('[PnL] store check: the keys do not open the saved objects | '
                      'costbook=keys_mismatch:needs_%s' % key_id(k1), log)
        self.assertIn('| keys %s |' % key_id(k2), log)
        self.assertIn('[PnL] store check ok | costbook=ok settings=ok overrides=ok', log)
        self.assertNotIn(k1, log)
        self.assertNotIn(k2, log)

    def test_boot_store_check_runs_in_the_background(self):
        h = self.harness()
        h.upload()
        saved = pnl.STORE_CHECK_DELAY
        pnl.STORE_CHECK_DELAY = 0
        try:
            from flask import Flask
            app = Flask('pnl_store_check')
            pnl.register_pnl_routes(app, store=h.store, caller_identity=h.identity, sources={}, env=h.env,
                                    engine=h.engine, routing=h.routing)
            deadline = time.time() + 5
            while 'store check ok' not in self._out.getvalue() and time.time() < deadline:
                time.sleep(0.02)
        finally:
            pnl.STORE_CHECK_DELAY = saved
        self.assertIn('[PnL] store check ok | costbook=ok settings=absent overrides=absent', self._out.getvalue())


if __name__ == '__main__':
    unittest.main()
