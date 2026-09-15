"""
pnl_store.py: storage for the admin-only Profit & Loss module (pnl.py).

CONFIDENTIAL DATA. Factory costs and margins are the most sensitive data in the
company. Every P&L object is encrypted in this process before it reaches any
backend. There is no plaintext path, no fallback and no debug dump. Exceptions
carry short codes only, never content.

Backends share one duck-typed API:
    get(name)                                     -> (bytes | None, etag | None)
    head(name)                                    -> etag | None
    put(name, data, expected_etag=None, kid=None) -> etag
        expected_etag None: unconditional write.
        expected_etag '':   the object must not exist yet.
        any other string:   the current etag must equal it.
        A failed condition raises VersionConflict (HTTP 409 in pnl.py).
        kid: the non-secret id of the key that encrypted `data`. It is stored OUTSIDE the
             ciphertext (S3 object metadata 'pnl-kid'; LocalDirStore: .meta/<name>.kid) so
             status and the boot log can say which key an object needs. Optional.
    head_kid(name)                                -> kid | None   (optional)
    versions(name)                                -> [{'id', 'at', 'size', 'current'}], newest first
    get_version(name, version_id)                 -> bytes | None

    LocalDirStore(path)                                  review harness and local runs
    S3Store(get_s3, bucket, prefix='inventory/pnl/')     production (versioned bucket)

EncryptedStore(backend, keys_csv) wraps a backend:
    keys_csv    PNL_DATA_KEYS: comma separated Fernet keys, NEWEST FIRST.
    ok, reason  ok is False with a short reason code when anything is missing:
                'no_backend', 'keys_missing', 'crypto_unavailable', 'keys_invalid',
                'selftest_failed'. When not ok, every read and write raises
                StoreNotConfigured, so nothing is ever written.
    key_ids     non-secret ids of the configured keys, newest first:
                sha256('pnl-kid:' + key)[:12]. They name a key; they never reveal it.
                kid is the newest one. fingerprint == kid (boot log).
    envelope    {'k': name, 'v': 1, 'at': epoch, 'data': obj} as JSON, then Fernet.
                Decrypt rejects a token whose 'k' is not the object name, so blobs
                cannot be swapped between objects.
    get_obj(name)                              -> (obj | None, etag)
    put_obj(name, obj, expected_etag=None, replace_unreadable=False) -> etag
                Refuses to overwrite an existing object that these keys cannot open:
                StoreUnreadable('current_unreadable'), nothing written. Also refuses to CREATE
                an absent object while another stored object (OBJECT_NAMES) needs a key this
                server does not have: StoreUnreadable('keys_mismatch') naming that object,
                nothing written, so a wrong key can never split the store across two keys.
                replace_unreadable=True skips both checks (a truly lost key; pnl.py logs it).
                The write is always conditional on the etag that was checked, so a concurrent
                writer gets VersionConflict instead of a silent overwrite.
    get_version_obj(name, version_id)          -> obj (StoreNotFound when absent)
    restore(name, version_id, expected_etag=None, replace_unreadable=False) -> etag
    rotate(name)                               -> etag | None (re-encrypts under the newest key)
    readable(name)                             -> (etag, True | False | None, code)
    object_kid(name)                           -> kid | None (the key id stored with the object)
    diagnose(name, code)                       -> 'keys_mismatch' | 'damaged'
    StoreUnreadable carries .obj (object name) when raised for one object.

KEY RUNBOOK (read this before touching PNL_DATA_KEYS on Render)
    1. Never replace PNL_DATA_KEYS. To change the key, PREPEND the new one: "new,old".
       Restart, then call POST /api/pnl/rotate. It re-encrypts every object under the
       newest key. Only then may the old key leave the list.
    2. Keep every old key offline until rotate has run and its old S3 versions are no
       longer needed. Old versions stay encrypted under the key that wrote them.
    3. If /api/pnl/status says store:keys_mismatch, or the boot log says the keys do not
       open the saved objects: do NOT upload again. Put the old key back after the new
       one ("new,old"), restart, check status, then call rotate. Status names the key id
       each object needs (objects.<name>.kid) and the configured ids (keys.ids).
    4. If data was already written under two keys, list both keys and call rotate.
    5. Only when a key is truly lost: POST the cost book, settings or overrides with
       replace_unreadable: true. That overwrites data nobody can read. pnl.py logs it.

Generate a key:
    python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
"""

import hashlib
import inspect
import json
import os
import re
import tempfile
import threading
import time
from datetime import datetime, timezone

OBJECT_NAMES = ('costbook.v1.bin', 'settings.v1.bin', 'overrides.v1.bin')
ENVELOPE_VERSION = 1
MAX_VERSIONS = 100
KID_META = 'pnl-kid'          # S3 user metadata key for the key id

_NAME_RE = re.compile(r'^[a-z0-9][a-z0-9._-]{0,63}$')
_LOCAL_VERSION_RE = re.compile(r'^\d{13}-[0-9a-f]{12}$')
# S3 version ids are opaque URL-safe strings ('null' for objects written before versioning).
_S3_VERSION_RE = re.compile(r'^[A-Za-z0-9._+/=-]{1,1024}$')
_KID_RE = re.compile(r'^[0-9a-f]{12}$')


class StoreError(Exception):
    """Base class. The message is a short code, never content."""


class StoreNotConfigured(StoreError):
    """No backend, no keys, no crypto library or a failed self-test: fail closed."""


class StoreUnreadable(StoreError):
    """Bad token, wrong key, tampered bytes or an envelope for another object.
    'current_unreadable': a write was refused because the object it would replace
    cannot be opened with these keys."""


class StoreUnavailable(StoreError):
    """The backend could not be reached (S3 or disk I/O failure)."""


class StoreNotFound(StoreError):
    """A requested version does not exist."""


class VersionConflict(StoreError):
    """expected_etag did not match the stored object."""


def key_id(key):
    """Non-secret id of a Fernet key: 12 hex characters of a domain-separated hash."""
    return hashlib.sha256(('pnl-kid:' + str(key)).encode('utf-8')).hexdigest()[:12]


def _check_name(name):
    if not isinstance(name, str) or not _NAME_RE.match(name) or '..' in name:
        raise ValueError('invalid object name')
    return name


def _etag_matches(expected, current):
    if expected is None:
        return True
    if expected == '':
        return current is None
    return current is not None and current == expected


def _iso(epoch):
    return datetime.fromtimestamp(epoch, timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')


def _clean_etag(value):
    if value is None:
        return None
    return str(value).strip().strip('"') or None


def _clean_kid(value):
    v = str(value or '').strip().lower()
    return v if _KID_RE.match(v) else None


def _accepts_kw(fn, name):
    try:
        params = inspect.signature(fn).parameters
    except (TypeError, ValueError):
        return False
    return name in params or any(p.kind is inspect.Parameter.VAR_KEYWORD for p in params.values())


def _unreadable(code, name=None):
    err = StoreUnreadable(code)
    err.obj = name
    return err


def _botocore_errors():
    """(ClientError, ParamValidationError) without making botocore an import-time need."""
    try:
        from botocore.exceptions import ClientError, ParamValidationError
        return ClientError, ParamValidationError
    except Exception:  # pragma: no cover - botocore ships with boto3 everywhere we run
        class _Never(Exception):
            pass
        return _Never, _Never


# ─────────────────────────────────────────────────────────────────────────────
# Local folder backend
# ─────────────────────────────────────────────────────────────────────────────
class LocalDirStore:
    """Objects as files in one folder. Every write also keeps a copy under
    .versions/<name>/ so versions and restore work locally. Holds ciphertext only
    (EncryptedStore encrypts before calling put). The key id of each current object
    sits in .meta/<name>.kid (12 hex characters, not secret)."""

    def __init__(self, path):
        self.path = os.path.abspath(path)
        os.makedirs(self.path, exist_ok=True)
        self._lock = threading.Lock()

    def _file(self, name):
        return os.path.join(self.path, _check_name(name))

    def _vdir(self, name):
        return os.path.join(self.path, '.versions', _check_name(name))

    def _kid_file(self, name):
        return os.path.join(self.path, '.meta', _check_name(name) + '.kid')

    @staticmethod
    def _etag(data):
        return hashlib.sha256(data).hexdigest()[:32]

    @staticmethod
    def _atomic_write(path, data):
        fd, tmp = tempfile.mkstemp(prefix='.tmp-', dir=os.path.dirname(path))
        try:
            with os.fdopen(fd, 'wb') as f:
                f.write(data)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp, path)
        except BaseException:
            try:
                os.remove(tmp)
            except OSError:
                pass
            raise

    def get(self, name):
        path = self._file(name)
        try:
            with open(path, 'rb') as f:
                data = f.read()
        except FileNotFoundError:
            return None, None
        except OSError:
            raise StoreUnavailable('local_read') from None
        return data, self._etag(data)

    def head(self, name):
        return self.get(name)[1]

    def head_kid(self, name):
        try:
            with open(self._kid_file(name), 'rb') as f:
                return _clean_kid(f.read(64).decode('ascii', 'replace'))
        except OSError:
            return None

    def _write_kid(self, name, kid):
        path = self._kid_file(name)
        kid = _clean_kid(kid)
        if kid is None:
            try:
                os.remove(path)
            except FileNotFoundError:
                pass
            return
        os.makedirs(os.path.dirname(path), exist_ok=True)
        self._atomic_write(path, kid.encode('ascii'))

    def put(self, name, data, expected_etag=None, kid=None):
        if not isinstance(data, (bytes, bytearray)):
            raise TypeError('bytes required')
        data = bytes(data)
        path = self._file(name)
        with self._lock:
            if expected_etag is not None and not _etag_matches(expected_etag, self.head(name)):
                raise VersionConflict('etag')
            etag = self._etag(data)
            try:
                self._atomic_write(path, data)
                self._write_kid(name, kid)
                vdir = self._vdir(name)
                os.makedirs(vdir, exist_ok=True)
                vid = '%013d-%s' % (int(time.time() * 1000), etag[:12])
                self._atomic_write(os.path.join(vdir, vid + '.bin'), data)
            except OSError:
                raise StoreUnavailable('local_write') from None
        return etag

    def versions(self, name):
        vdir = self._vdir(name)
        try:
            files = [f for f in os.listdir(vdir)
                     if f.endswith('.bin') and _LOCAL_VERSION_RE.match(f[:-4])]
        except FileNotFoundError:
            return []
        except OSError:
            raise StoreUnavailable('local_list') from None
        current = self.head(name)
        out, seen_current = [], False
        for fn in sorted(files, reverse=True)[:MAX_VERSIONS]:
            vid = fn[:-4]
            try:
                size = os.stat(os.path.join(vdir, fn)).st_size
            except OSError:
                continue
            is_current = bool(current) and not seen_current and vid.endswith(current[:12])
            seen_current = seen_current or is_current
            out.append({'id': vid, 'at': _iso(int(vid.split('-')[0]) / 1000.0),
                        'size': size, 'current': is_current})
        return out

    def get_version(self, name, version_id):
        if not isinstance(version_id, str) or not _LOCAL_VERSION_RE.match(version_id):
            return None
        path = os.path.join(self._vdir(name), version_id + '.bin')
        try:
            with open(path, 'rb') as f:
                return f.read()
        except FileNotFoundError:
            return None
        except OSError:
            raise StoreUnavailable('local_read') from None


# ─────────────────────────────────────────────────────────────────────────────
# S3 backend
# ─────────────────────────────────────────────────────────────────────────────
class S3Store:
    """One object per name under `prefix` in `bucket`. The Versa bucket is
    publicly readable and versioned, so only EncryptedStore may call put()."""

    _MISSING_CODES = ('NoSuchKey', 'NoSuchVersion', '404', 'NotFound')
    _CONFLICT_CODES = ('PreconditionFailed', 'ConditionalRequestConflict')
    _NO_CONDITION_CODES = ('NotImplemented',)

    def __init__(self, get_s3, bucket, prefix='inventory/pnl/'):
        prefix = (prefix or '').strip()
        if prefix and not prefix.endswith('/'):
            prefix += '/'
        self._get_s3 = get_s3
        self.bucket = bucket
        self.prefix = prefix

    def key(self, name):
        return self.prefix + _check_name(name)

    @staticmethod
    def _code(exc):
        try:
            return str((exc.response.get('Error') or {}).get('Code') or '')
        except Exception:
            return ''

    @staticmethod
    def _status(exc):
        try:
            return int((exc.response.get('ResponseMetadata') or {}).get('HTTPStatusCode') or 0)
        except Exception:
            return 0

    def _is_missing(self, exc):
        return self._code(exc) in self._MISSING_CODES or self._status(exc) == 404

    def get(self, name):
        key = self.key(name)
        ClientError, _ = _botocore_errors()
        try:
            r = self._get_s3().get_object(Bucket=self.bucket, Key=key)
            data = r['Body'].read()
        except ClientError as e:
            if self._is_missing(e):
                return None, None
            raise StoreUnavailable('s3_get') from None
        except Exception:
            raise StoreUnavailable('s3_get') from None
        return data, _clean_etag(r.get('ETag'))

    def _head_raw(self, name):
        key = self.key(name)
        ClientError, _ = _botocore_errors()
        try:
            return self._get_s3().head_object(Bucket=self.bucket, Key=key)
        except ClientError as e:
            if self._is_missing(e):
                return None
            raise StoreUnavailable('s3_head') from None
        except Exception:
            raise StoreUnavailable('s3_head') from None

    def head(self, name):
        r = self._head_raw(name)
        return _clean_etag(r.get('ETag')) if r is not None else None

    def head_kid(self, name):
        r = self._head_raw(name)
        if r is None:
            return None
        meta = r.get('Metadata') or {}
        return _clean_kid(meta.get(KID_META)) if isinstance(meta, dict) else None

    def put(self, name, data, expected_etag=None, kid=None):
        if not isinstance(data, (bytes, bytearray)):
            raise TypeError('bytes required')
        key = self.key(name)
        kw = {'Bucket': self.bucket, 'Key': key, 'Body': bytes(data),
              'ContentType': 'application/octet-stream', 'CacheControl': 'no-store'}
        kid = _clean_kid(kid)
        if kid:
            kw['Metadata'] = {KID_META: kid}
        cond = {}
        if expected_etag is not None:
            current = self.head(name)
            if not _etag_matches(expected_etag, current):
                raise VersionConflict('etag')
            # Conditional write closes the race between workers where S3 supports it.
            cond = {'IfNoneMatch': '*'} if current is None else {'IfMatch': '"%s"' % current}
        ClientError, ParamValidationError = _botocore_errors()
        s3 = self._get_s3()
        try:
            try:
                r = s3.put_object(**kw, **cond)
            except ParamValidationError:
                # Older botocore without conditional writes: the head check above stands.
                r = s3.put_object(**kw)
            except ClientError as e:
                if cond and self._code(e) in self._NO_CONDITION_CODES:
                    r = s3.put_object(**kw)
                else:
                    raise
        except ClientError as e:
            if self._code(e) in self._CONFLICT_CODES or self._status(e) in (409, 412):
                raise VersionConflict('etag') from None
            raise StoreUnavailable('s3_put') from None
        except StoreError:
            raise
        except Exception:
            raise StoreUnavailable('s3_put') from None
        return _clean_etag(r.get('ETag')) or self.head(name)

    def versions(self, name):
        key = self.key(name)
        ClientError, _ = _botocore_errors()
        s3 = self._get_s3()
        kw = {'Bucket': self.bucket, 'Prefix': key, 'MaxKeys': 1000}
        out = []
        try:
            for _page in range(20):
                r = s3.list_object_versions(**kw)
                for v in r.get('Versions') or []:
                    if v.get('Key') != key:
                        continue
                    lm = v.get('LastModified')
                    if hasattr(lm, 'astimezone'):
                        at = lm.astimezone(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
                    else:
                        at = str(lm or '')
                    out.append({'id': str(v.get('VersionId') or 'null'), 'at': at,
                                'size': int(v.get('Size') or 0), 'current': bool(v.get('IsLatest'))})
                if not r.get('IsTruncated'):
                    break
                kw['KeyMarker'] = r.get('NextKeyMarker')
                kw['VersionIdMarker'] = r.get('NextVersionIdMarker')
        except ClientError:
            raise StoreUnavailable('s3_versions') from None
        except Exception:
            raise StoreUnavailable('s3_versions') from None
        out.sort(key=lambda v: v['at'], reverse=True)
        return out[:MAX_VERSIONS]

    def get_version(self, name, version_id):
        if not isinstance(version_id, str) or not _S3_VERSION_RE.match(version_id):
            return None
        key = self.key(name)
        ClientError, _ = _botocore_errors()
        try:
            r = self._get_s3().get_object(Bucket=self.bucket, Key=key, VersionId=version_id)
            return r['Body'].read()
        except ClientError as e:
            if self._is_missing(e) or self._code(e) == 'InvalidArgument' or self._status(e) == 400:
                return None
            raise StoreUnavailable('s3_get') from None
        except Exception:
            raise StoreUnavailable('s3_get') from None


# ─────────────────────────────────────────────────────────────────────────────
# Encryption layer
# ─────────────────────────────────────────────────────────────────────────────
class EncryptedStore:
    """MultiFernet over a backend. Fails closed: when ok is False nothing is read
    or written. The decrypted plaintext is cached per etag in this process only, and
    so is the fact that an etag could NOT be opened (no repeated reads of a bad blob)."""

    def __init__(self, backend, keys_csv):
        self._backend = backend
        self._lock = threading.RLock()
        self._cache = {}              # name -> (etag, envelope plaintext bytes)
        self._bad = {}                # name -> (etag, code): the current keys cannot open it
        self._mf = None
        self._invalid_token = ()
        self._put_takes_kid = backend is not None and _accepts_kw(getattr(backend, 'put', None), 'kid')
        self.ok = False
        self.reason = 'not_initialised'
        self.key_count = 0
        self.key_ids = []             # non-secret ids of the configured keys, newest first
        self.kid = None               # id of the newest key (the one every write uses)
        self.fingerprint = None       # == kid, for boot logs
        self._init(keys_csv)

    def __repr__(self):
        return '<EncryptedStore ok=%s reason=%s>' % (self.ok, self.reason or '-')

    def _init(self, keys_csv):
        if self._backend is None:
            self.reason = 'no_backend'
            return
        keys = [k.strip() for k in str(keys_csv or '').split(',') if k.strip()]
        if not keys:
            self.reason = 'keys_missing'
            return
        try:
            from cryptography.fernet import Fernet, InvalidToken, MultiFernet
        except Exception:
            self.reason = 'crypto_unavailable'
            return
        try:
            fernets = [Fernet(k.encode('ascii')) for k in keys]
        except Exception:
            self.reason = 'keys_invalid'
            return
        try:
            mf = MultiFernet(fernets)
            probe = os.urandom(24)
            if mf.decrypt(mf.encrypt(probe)) != probe:
                raise ValueError('probe')
            for f in fernets:          # every listed key must work, not only the newest
                if f.decrypt(f.encrypt(probe)) != probe:
                    raise ValueError('probe')
        except Exception:
            self.reason = 'selftest_failed'
            return
        # A valid key that is not the one the data was written with still passes this
        # self-test. put_obj() refuses to overwrite what it cannot open, and pnl.py status
        # reports store:keys_mismatch, so that mistake cannot split the store.
        self._mf = mf
        self._invalid_token = InvalidToken
        self.key_count = len(keys)
        self.key_ids = [key_id(k) for k in keys]
        self.kid = self.fingerprint = self.key_ids[0]
        self.ok = True
        self.reason = ''

    # ── helpers ──
    def _require_ok(self):
        if not self.ok or self._mf is None:
            raise StoreNotConfigured(self.reason or 'not_configured')

    def _seal(self, name, obj):
        env = {'k': name, 'v': ENVELOPE_VERSION, 'at': int(time.time()), 'data': obj}
        plain = json.dumps(env, separators=(',', ':'), allow_nan=False).encode('utf-8')
        return self._mf.encrypt(plain), plain

    def _open(self, name, token):
        try:
            plain = self._mf.decrypt(bytes(token))
        except Exception:
            raise _unreadable('token', name) from None
        try:
            env = json.loads(plain.decode('utf-8'))
        except Exception:
            raise _unreadable('json', name) from None
        if (not isinstance(env, dict) or env.get('k') != name
                or env.get('v') != ENVELOPE_VERSION or 'data' not in env):
            raise _unreadable('envelope', name)
        return env, plain

    def _open_current(self, name, token, etag):
        """_open for the current object; remembers a failure for this etag."""
        try:
            env, plain = self._open(name, token)
        except StoreUnreadable as e:
            with self._lock:
                self._bad[name] = (etag, str(e))
            raise
        with self._lock:
            self._bad.pop(name, None)
            self._cache[name] = (etag, plain)
        return env, plain

    def _backend_put(self, name, data, expected_etag):
        if self._put_takes_kid and self.kid:
            return self._backend.put(name, data, expected_etag=expected_etag, kid=self.kid)
        return self._backend.put(name, data, expected_etag=expected_etag)

    def _verified_current(self, name, replace_unreadable):
        """Etag of the object a write would replace (None when absent), after proving these
        keys can open it. Caller holds self._lock."""
        etag = self._backend.head(name)
        if etag is None:
            return None
        hit = self._cache.get(name)
        if (hit is not None and hit[0] == etag) or replace_unreadable:
            return etag
        bad = self._bad.get(name)
        if bad is None or bad[0] != etag:
            token, got = self._backend.get(name)
            if token is None:
                return None
            got = got or etag
            try:
                self._open_current(name, token, got)
                return got
            except StoreUnreadable:
                pass
        raise _unreadable('current_unreadable', name)

    # ── metadata (no decryption; allowed while not configured) ──
    def head(self, name):
        _check_name(name)
        if self._backend is None:
            raise StoreNotConfigured('no_backend')
        return self._backend.head(name)

    def object_kid(self, name):
        """Key id stored with the object (outside the ciphertext), or None."""
        _check_name(name)
        fn = getattr(self._backend, 'head_kid', None)
        if not callable(fn):
            return None
        try:
            return _clean_kid(fn(name))
        except StoreError:
            return None

    def diagnose(self, name, code):
        """Why these keys cannot open an object: 'keys_mismatch' or 'damaged'. A Fernet
        failure alone cannot tell a wrong key from tampered bytes; the stored key id can."""
        if code in ('json', 'envelope'):
            return 'damaged'
        kid = self.object_kid(name) if name else None
        if kid and kid in self.key_ids:
            return 'damaged'
        return 'keys_mismatch'

    def supports_versions(self):
        b = self._backend
        return b is not None and callable(getattr(b, 'versions', None)) \
            and callable(getattr(b, 'get_version', None))

    def versions(self, name):
        _check_name(name)
        if not self.supports_versions():
            return None
        return self._backend.versions(name)

    # ── data ──
    def get_obj(self, name):
        _check_name(name)
        self._require_ok()
        etag = self._backend.head(name)
        if etag is None:
            with self._lock:
                self._cache.pop(name, None)
                self._bad.pop(name, None)
            return None, None
        with self._lock:
            hit = self._cache.get(name)
            bad = self._bad.get(name)
        if hit is not None and hit[0] == etag:
            return json.loads(hit[1].decode('utf-8'))['data'], etag
        if bad is not None and bad[0] == etag:
            raise _unreadable(bad[1], name)
        token, etag = self._backend.get(name)
        if token is None:
            return None, None
        env, _plain = self._open_current(name, token, etag)
        return env['data'], etag

    def readable(self, name):
        """(etag, True | False | None, code). readable None: absent, or the store is not
        configured. Opens the object at most once per etag."""
        _check_name(name)
        if self._backend is None:
            raise StoreNotConfigured('no_backend')
        etag = self._backend.head(name)
        if etag is None:
            return None, None, None
        if not self.ok or self._mf is None:
            return etag, None, None
        with self._lock:
            hit = self._cache.get(name)
            bad = self._bad.get(name)
        if hit is not None and hit[0] == etag:
            return etag, True, None
        if bad is not None and bad[0] == etag:
            return etag, False, bad[1]
        try:
            self.get_obj(name)
        except StoreUnreadable as e:
            return etag, False, str(e)
        return etag, True, None

    def _needs_other_key(self, name):
        """The first stored object other than `name` that these keys cannot open because a key
        this server does not have wrote it (the same test status uses for store:keys_mismatch),
        or None. Opens each object at most once per etag. Caller holds self._lock (an RLock)."""
        for other in OBJECT_NAMES:
            if other == name:
                continue
            _etag, ok, code = self.readable(other)
            if ok is False and self.diagnose(other, code) == 'keys_mismatch':
                return other
        return None

    def put_obj(self, name, obj, expected_etag=None, replace_unreadable=False):
        _check_name(name)
        self._require_ok()
        if expected_etag is not None and not isinstance(expected_etag, str):
            raise TypeError('expected_etag must be a string or None')
        token, plain = self._seal(name, obj)
        with self._lock:
            current = self._verified_current(name, bool(replace_unreadable))
            if current is None and not replace_unreadable:
                # Creating an object while another one needs a key this server lacks would leave
                # the store under two keys. Refuse; only replace_unreadable (logged) bypasses it.
                other = self._needs_other_key(name)
                if other is not None:
                    raise _unreadable('keys_mismatch', other)
            if expected_etag is not None and not _etag_matches(expected_etag, current):
                raise VersionConflict('etag')
            # Always conditional on what was just checked: a concurrent writer gets a 409.
            etag = self._backend_put(name, token, '' if current is None else current)
            self._bad.pop(name, None)
            if etag:
                self._cache[name] = (etag, plain)
            else:
                self._cache.pop(name, None)
        return etag

    def get_version_obj(self, name, version_id):
        _check_name(name)
        self._require_ok()
        if not self.supports_versions():
            raise StoreNotFound('versions_unsupported')
        token = self._backend.get_version(name, version_id)
        if token is None:
            raise StoreNotFound('version')
        try:
            env, _plain = self._open(name, token)
        except StoreUnreadable as e:
            e.obj = None       # an old version: the current object's key id says nothing about it
            raise
        return env['data']

    def restore(self, name, version_id, expected_etag=None, replace_unreadable=False):
        """Re-encrypt an old version as the new current version (new envelope, newest key)."""
        return self.put_obj(name, self.get_version_obj(name, version_id), expected_etag=expected_etag,
                            replace_unreadable=replace_unreadable)

    def rotate(self, name):
        """Re-encrypt the current object under the newest key. None when absent."""
        _check_name(name)
        self._require_ok()
        with self._lock:
            token, etag = self._backend.get(name)
            if token is None:
                return None
            self._open(name, token)            # verify first: never rotate a swapped or foreign blob
            try:
                new_token = self._mf.rotate(bytes(token))
            except Exception:
                raise _unreadable('token', name) from None
            new_etag = self._backend_put(name, new_token, etag)
            self._cache.pop(name, None)
            self._bad.pop(name, None)
        return new_etag
