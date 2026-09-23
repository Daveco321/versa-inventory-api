"""
Synthetic tests for the admin-only P&L routes (pnl.py) and the app.py wiring.

Covers the DESIGN section 8 auth matrix on EVERY route (anonymous, malformed
bearer, factory, staff non-admin, admin not on the allowlist, admin on the
allowlist, machine key only, machine key plus admin bearer, open-orders token
shape), with and without the allowlist and the keys; the response headers on
every /api/pnl response; generic error bodies (never exception text); route
behavior (background build, 202 polling, validation, audit lines); and static
checks that app.py registers pnl and never lists /api/pnl in an open prefix or
allow-list.

SYNTHETIC ONLY. No real cost data: numbers are sentinels (7.7777) and codes are
made up (ZZ...). Run from the repo root:
    python -B -m unittest discover -s tests -p "test_pnl_[as]*.py" -v
"""
import ast
import contextlib
import gzip
import hashlib
import importlib.metadata
import io
import json
import os
import re
import shutil
import sys
import tempfile
import threading
import time
import types
import unittest
import uuid
from datetime import datetime

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
for _p in (REPO, HERE):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from flask import Flask  # noqa: E402

import pnl  # noqa: E402
from pnl_store import EncryptedStore, LocalDirStore, StoreUnavailable  # noqa: E402

try:
    from cryptography.fernet import Fernet
except Exception:  # pragma: no cover
    Fernet = None

SENTINEL_COST = 7.7777
SENTINEL_TEXT = '7.7777'
SENTINEL_LABEL = 'PNL_SENTINEL_7F3A'
SECRET_EXC = 'SECRET-EXCEPTION-TEXT-9Q'
ADMIN = 'admin@example.test'
ADMIN2 = 'other-admin@example.test'
MACHINE_KEY = 'test-machine-key-123'
T_ADMIN = 'eyJx.pay.admin'

IDENTITIES = {
    'eyJx.pay.admin': {'role': 'staff', 'is_admin': True, 'email': ADMIN, 'uid': 'u-admin'},
    'eyJx.pay.admincase': {'role': 'staff', 'is_admin': True, 'email': 'Admin@Example.TEST', 'uid': 'u-admin'},
    'eyJx.pay.admin2': {'role': 'staff', 'is_admin': True, 'email': ADMIN2, 'uid': 'u-admin2'},
    'eyJx.pay.noemail': {'role': 'staff', 'is_admin': True, 'email': '', 'uid': 'u-noemail'},
    'eyJx.pay.staff': {'role': 'staff', 'is_admin': False, 'email': 'staff@example.test', 'uid': 'u-staff'},
    'eyJx.pay.truthy': {'role': 'staff', 'is_admin': 'true', 'email': ADMIN, 'uid': 'u-truthy'},
    'eyJx.pay.fact': {'role': 'factory', 'factory_prefix': 'ZZ', 'is_admin': False,
                      'email': 'factory@example.test', 'uid': 'u-fact'},
    'eyJx.pay.factadmin': {'role': 'factory', 'factory_prefix': 'ZZ', 'is_admin': True,
                           'email': ADMIN, 'uid': 'u-factadmin'},
}

# Callers refused with 401 AUTH_REQUIRED on every route, whatever the configuration.
DENY_401 = [
    ('anonymous', {}),
    ('empty_bearer', {'Authorization': 'Bearer '}),
    ('garbage_bearer', {'Authorization': 'Bearer abc'}),
    ('jwt_one_part', {'Authorization': 'Bearer eyJabc'}),
    ('jwt_three_dots', {'Authorization': 'Bearer eyJa.b.c.d'}),
    ('basic_scheme', {'Authorization': 'Basic eyJx.pay.admin'}),
    ('bearer_without_space', {'Authorization': 'BearereyJx.pay.admin'}),
    ('non_ascii_token', {'Authorization': 'Bearer eyJx.pay.admïn'}),
    ('too_long_token', {'Authorization': 'Bearer eyJ' + 'a' * 5000 + '.b.c'}),
    ('expired_session', {'Authorization': 'Bearer eyJx.pay.noprof'}),
    ('identity_error', {'Authorization': 'Bearer eyJx.pay.boom'}),
    ('open_orders_token', {'Authorization': 'Bearer b29rZW4.deadbeef'}),
    ('machine_key_only', {'X-Api-Key': MACHINE_KEY}),
    ('wrong_machine_key', {'X-Api-Key': 'wrong'}),
    ('machine_key_plus_admin_bearer', {'X-Api-Key': MACHINE_KEY, 'Authorization': 'Bearer ' + T_ADMIN}),
]
IDENTITY_CONSULTED = {'expired_session', 'identity_error'}   # shape passes, so the lookup runs

NON_ADMIN = [   # 403 ADMIN_ONLY always
    ('factory', 'eyJx.pay.fact'),
    ('factory_with_is_admin', 'eyJx.pay.factadmin'),
    ('staff_non_admin', 'eyJx.pay.staff'),
    ('staff_is_admin_string', 'eyJx.pay.truthy'),
]
ADMIN_NOT_LISTED = [   # 403 with an allowlist; 503 allowlist when the list is empty
    ('admin_not_on_allowlist', 'eyJx.pay.admin2'),
    ('admin_without_email', 'eyJx.pay.noemail'),
]
ALLOW = [
    ('admin_on_allowlist', {'Authorization': 'Bearer ' + T_ADMIN}),
    ('admin_email_case_insensitive', {'Authorization': 'Bearer eyJx.pay.admincase'}),
    ('lowercase_bearer_scheme', {'Authorization': 'bearer ' + T_ADMIN}),
]


SYNTH_RATE = 7.3579          # a made-up RMB rate inside the plausibility band


def synthetic_params(**over):
    """Every params key the engine reads, with made-up codes and sentinel-style values."""
    p = {'fxBase': SENTINEL_COST, 'ssDelta': -0.1111, 'fitPremium': {'regular': 0.2229, 'bigTall': 0.3331},
         'gridPrecedence': {'ZZ': ['YY', 'XX'], '_default': ['XX', 'YY']},
         'poolOrder': {'all': ['XX-ZZ:ANY', 'YY-ZZ:ANY'],
                       'polo': {'primary': ['XX-ZZ:ANY'], 'fallback': ['YY-ZZ:ANY']},
                       'XX': {'base': ['XX-ZZ:ANY'], 'byGroup': {'ZZGROUP': ['XX-ZZ:ANY']}},
                       'recordKinds': ['calculator'], 'excludeFlags': ['ZZ_FLAG'], 'rule': 'synthetic'},
         'programMap': {'ZZ-PROGRAM1': {'brand': 'ZZ', 'fab': 'ZZ', 'fit': 'SLIM', 'sleeve': 'LS', 'pat': None,
                                        'group': 'ZZGROUP', 'cat': 'dress_shirt', 'src': 'ZZ00001 synthetic'}},
         'kitPcs': {'ZZKIT0001': 12}, 'customerGroupPrefix': {'ZZ': 'ZZGROUP', '_default': 'OTHER'}}
    p.update(over)
    return p


def synthetic_costbook(n=3, price=SENTINEL_COST):
    return {
        'v': 1, 'generatedAt': '2026-01-01T00:00:00Z', 'parser': {'name': 'synthetic', 'version': '0'},
        'sources': [{'file': 'synthetic.xlsx', 'sha256': '0' * 64, 'modified': '2026-01-01',
                     'sheets': ['S'], 'records': n}],
        'params': synthetic_params(),
        'records': [{'id': 'ZZ!A%d' % i, 'source_code': 'ZZ', 'record_kind': 'ref_price_list',
                     'scope': 'ref', 'factory_code': 'ZZ', 'production_ref': 'ZZ00001',
                     'production_ref_resolved': 'ZZ00001', 'style': None, 'brand_code': 'ZZ',
                     'fabric_codes': ['ZZ'], 'price_usd': price, 'rmb_price': None, 'flags': [],
                     'inputs': {'C': 1}} for i in range(n)],
        'conflict_groups': [], 'meta': {'counts': {}, 'note': SENTINEL_LABEL},
    }


ROUTE_CALLS = [
    ('/api/pnl/status', 'GET', None),
    ('/api/pnl/dataset', 'GET', None),
    ('/api/pnl/costbook', 'GET', None),
    ('/api/pnl/costbook', 'POST', synthetic_costbook(2)),
    ('/api/pnl/costbook/versions', 'GET', None),
    ('/api/pnl/costbook/restore', 'POST', {'version_id': '0000000000000-000000000000'}),
    ('/api/pnl/settings', 'GET', None),
    ('/api/pnl/settings', 'POST', {'settings': {'bulk': {'includeInTotals': True}}}),
    ('/api/pnl/overrides', 'GET', None),
    ('/api/pnl/overrides', 'POST', {'overrides': []}),
    ('/api/pnl/audit', 'POST', {'action': 'export', 'detail': 'tab=orders'}),
    ('/api/pnl/rotate', 'POST', {}),
    ('/api/pnl/analytics', 'GET', None),
    ('/api/pnl', 'GET', None),
    ('/api/pnl', 'POST', {}),
    ('/api/pnl/some/unknown/path', 'GET', None),
    ('/api/pnl/some/unknown/path', 'POST', {}),
]
STORE_ROUTES = {('/api/pnl/dataset', 'GET'), ('/api/pnl/costbook', 'GET'), ('/api/pnl/costbook', 'POST'),
                ('/api/pnl/costbook/versions', 'GET'), ('/api/pnl/costbook/restore', 'POST'),
                ('/api/pnl/settings', 'GET'), ('/api/pnl/settings', 'POST'),
                ('/api/pnl/overrides', 'GET'), ('/api/pnl/overrides', 'POST'), ('/api/pnl/rotate', 'POST'),
                ('/api/pnl/analytics', 'GET')}


def new_key():
    return Fernet.generate_key().decode()


def files_under(path):
    out = []
    for dirpath, _dirs, names in os.walk(path):
        out.extend(os.path.join(dirpath, n) for n in names)
    return out


class FakeIdentity:
    def __init__(self):
        self.calls = 0
        self._lock = threading.Lock()

    def __call__(self, token):
        with self._lock:
            self.calls += 1
        if token == 'eyJx.pay.boom':
            raise RuntimeError(SECRET_EXC)
        ident = IDENTITIES.get(token)
        return dict(ident) if ident else None


def make_engine(mode='ok', gate=None):
    eng = types.ModuleType('fake_pnl_engine')
    eng.calls = []
    eng.DEFAULT_SETTINGS = {'v': 1, 'fx': {'rate': SYNTH_RATE, 'asOf': None, 'basis': 'current_usd'},
                            'routing': {'picksAsWarehouse': False, 'honorAssignments': True},
                            'bulk': {'includeInTotals': False}, 'extraBlock': {'k': 1}}

    def build_dataset(src, costbook, settings, overrides, now_iso, routing_module):
        eng.calls.append({'src': src, 'costbook': costbook, 'settings': settings,
                          'overrides': overrides, 'now_iso': now_iso, 'routing': routing_module})
        if gate is not None:
            gate.wait(10)
        if mode == 'raise':
            raise ValueError(SECRET_EXC + ' ' + SENTINEL_TEXT)
        if mode == 'nan':
            return {'v': 1, 'x': float('nan')}
        if mode == 'mutate':
            src['inventory']['items'][0]['factory_fob_usd'] = SENTINEL_COST
        return {'v': 1, 'builtAt': now_iso, 'asOf': src['today'],
                'inputs': {'ledger_rows': len(src['ledger']['rows'])},
                'lines': {'fields': ['id', 'fobU'], 'rows': [['L1', SENTINEL_COST]]},
                'notes': [SENTINEL_LABEL, len(src['open_orders']['orders'])]}

    eng.build_dataset = build_dataset
    return eng


def make_routing():
    mod = types.ModuleType('fake_pnl_routing')
    mod.route_all = lambda *a, **k: {'skus': {}, 'lineAlloc': {}, 'apoAlloc': {}, 'stats': {}}
    return mod


def make_sources(ledger=True, inventory=True, orders=True, state=None):
    state = state if state is not None else {}
    state.setdefault('orders', [{'ctrlNo': 1, 'orderNo': 'P1', 'customer': 'ZZCUST',
                                 'style': 'ZZAAAA001SLS', 'openQty': 1, 'pickQty': 0, 'salesPrice': 9.5}])

    def inv():
        items = [{'sku': 'ZZAAAA001SLS', 'jtw': 10, 'committed': 0}] if inventory else []
        return {'items': items, 'last_sync': '2026-01-01T00:00:00Z'}

    def led():
        rows = [{'production': 'ZZ00001', 'style': 'ZZAAAA001SLS', 'units': 5}] if ledger else []
        return {'rows': rows, 'last_sync': '2026-01-01T00:00:00Z'}

    def oo():
        return {'orders': json.loads(json.dumps(state['orders'])) if orders else [], 'ok': True,
                'fetched_at': '2026-01-01T00:00:00Z'}

    return {'inventory': inv, 'ledger': led, 'apo': lambda: {'rows': [], 'last_sync': None},
            'manual_allocations': lambda: [], 'vw_allocations': lambda: [], 'open_orders': oo,
            'fob_customers': lambda: ['zzfob'],
            'routing_inputs': lambda: {'suppression_overrides': [], 'deduction_assignments': {},
                                       'now': datetime.now().isoformat(timespec='seconds')},
            'sales_analytics': lambda: None, 'today': lambda: '2026-01-02'}


class RecordingBackend:
    """Backend wrapper that records every write attempt (for 'zero writes' checks)."""

    def __init__(self, inner):
        self.inner = inner
        self.puts = []
        self.gets = 0
        self.heads = 0

    def get(self, name):
        self.gets += 1
        return self.inner.get(name)

    def head(self, name):
        self.heads += 1
        return self.inner.head(name)

    def put(self, name, data, expected_etag=None, **kw):
        self.puts.append((name, bytes(data)))
        return self.inner.put(name, data, expected_etag=expected_etag, **kw)

    def head_kid(self, name):
        fn = getattr(self.inner, 'head_kid', None)
        return fn(name) if callable(fn) else None

    def versions(self, name):
        return self.inner.versions(name)

    def get_version(self, name, version_id):
        return self.inner.get_version(name, version_id)


class RaisingBackend:
    def __init__(self, exc):
        self.exc = exc

    def _raise(self, *a, **k):
        raise self.exc

    get = head = put = versions = get_version = _raise


class Harness:
    def __init__(self, root, keys='new', allow=ADMIN, enabled=None, engine='ok', routing='ok',
                 sources=None, backend=None, store='auto', gate=None):
        self.root = root
        self.store_dir = os.path.join(root, 'store')
        self.backend = backend if backend is not None else RecordingBackend(LocalDirStore(self.store_dir))
        self.keys = new_key() if keys == 'new' else keys
        self.store = EncryptedStore(self.backend, self.keys) if store == 'auto' else store
        env = {}
        if allow is not None:
            env['PNL_ALLOWED_EMAILS'] = allow
        if enabled is not None:
            env['PNL_ENABLED'] = enabled
        self.env = env
        self.identity = FakeIdentity()
        self.engine = make_engine(engine, gate) if isinstance(engine, str) else engine
        self.routing = make_routing() if routing == 'ok' else routing
        self.app = Flask('pnl_test_' + uuid.uuid4().hex[:8])
        self.svc = pnl.register_pnl_routes(self.app, store=self.store, caller_identity=self.identity,
                                           sources=sources if sources is not None else make_sources(),
                                           env=env, engine=self.engine, routing=self.routing,
                                           store_check=False)
        # the host app wires this after registration (app.py does the same)
        self.svc.sales_matrix = lambda: {'ready': True, 'customers': {}, 'source': {},
                                         'history': None, 'pending': None, 'pendingReady': False}
        self.client = self.app.test_client()

    def req(self, method, path, token=None, headers=None, body=None, raw=None, query=None):
        h = dict(headers or {})
        if token is not None:
            h['Authorization'] = 'Bearer ' + token
        kw = {'method': method, 'headers': h}
        if query:
            kw['query_string'] = query
        if raw is not None:
            kw['data'] = raw
            kw['content_type'] = 'application/json'
        elif body is not None:
            kw['json'] = body
        return self.client.open(path, **kw)

    def admin(self, method, path, **kw):
        return self.req(method, path, token=T_ADMIN, **kw)

    def wait_idle(self, timeout=10):
        deadline = time.time() + timeout
        while time.time() < deadline:
            job = self.svc._job
            if job is None:
                return True
            job['event'].wait(0.05)
        return False

    def dataset(self, query=None, timeout=10):
        deadline = time.time() + timeout
        while True:
            r = self.admin('GET', '/api/pnl/dataset', query=query)
            if r.status_code != 202 or time.time() > deadline:
                return r
            self.wait_idle(timeout)

    def upload(self, n=3):
        r = self.admin('POST', '/api/pnl/costbook', body=synthetic_costbook(n))
        assert r.status_code == 200, r.get_data(as_text=True)[:300]
        return r.get_json()['etag']


class PnlTestCase(unittest.TestCase):
    """Captures stdout for every test and fails if a sentinel, exception text or a
    traceback was ever printed."""
    ERROR_KEYS = {'error', 'code', 'reason', 'ref', 'problems', 'problemCount', 'missing'}

    def setUp(self):
        if Fernet is None:
            self.skipTest('cryptography is not installed')
        self._root = tempfile.mkdtemp(prefix='pnl-test-')
        self._harnesses = []
        self._out = io.StringIO()
        self._redirect = contextlib.redirect_stdout(self._out)
        self._redirect.__enter__()

    def tearDown(self):
        for h in self._harnesses:
            h.wait_idle(10)
        self._redirect.__exit__(None, None, None)
        shutil.rmtree(self._root, ignore_errors=True)
        log = self._out.getvalue()
        for bad in (SENTINEL_TEXT, SENTINEL_LABEL, SECRET_EXC, 'Traceback'):
            self.assertNotIn(bad, log, 'stdout leaked %r' % bad)

    def harness(self, **kw):
        h = Harness(tempfile.mkdtemp(dir=self._root), **kw)
        self._harnesses.append(h)
        return h

    def assert_pnl_headers(self, resp, ctx=''):
        cc = resp.headers.get('Cache-Control', '')
        for part in ('no-store', 'private', 'max-age=0'):
            self.assertIn(part, cc, ctx)
        self.assertEqual(resp.headers.get('Pragma'), 'no-cache', ctx)
        self.assertIn('Authorization', resp.headers.get('Vary', ''), ctx)
        self.assertIn('Origin', resp.headers.get('Vary', ''), ctx)     # the CORS grant varies by Origin
        self.assertEqual(resp.headers.get('X-Content-Type-Options'), 'nosniff', ctx)

    def assert_clean_text(self, text, ctx=''):
        for bad in (SECRET_EXC, SENTINEL_TEXT, SENTINEL_LABEL, 'Traceback', 'File "'):
            self.assertNotIn(bad, text, '%s leaked %r' % (ctx, bad))

    def assert_error(self, resp, status, code, ctx='', reason=None):
        self.assertEqual(resp.status_code, status, '%s: %s' % (ctx, resp.get_data(as_text=True)[:200]))
        self.assert_pnl_headers(resp, ctx)
        body = resp.get_json()
        self.assertIsInstance(body, dict, ctx)
        self.assertEqual(body.get('code'), code, ctx)
        self.assertEqual(body.get('error'), pnl.message_for(code, body.get('reason')), ctx)
        self.assertTrue(set(body) <= self.ERROR_KEYS, '%s: unexpected keys %s' % (ctx, set(body) - self.ERROR_KEYS))
        if reason is not None:
            self.assertEqual(body.get('reason'), reason, ctx)
        self.assert_clean_text(resp.get_data(as_text=True), ctx)
        return body


# ─────────────────────────────────────────────────────────────────────────────
class TestAuthMatrix(PnlTestCase):

    def _deny_everywhere(self, h, headers, status, code, ctx, identity_expected=True, reason=None):
        for path, method, body in ROUTE_CALLS:
            before = h.identity.calls
            r = h.req(method, path, headers=headers, body=body)
            self.assert_error(r, status, code, '%s %s %s' % (ctx, method, path), reason=reason)
            if not identity_expected:
                self.assertEqual(h.identity.calls, before, 'identity was consulted for %s' % ctx)

    def _deny_401_all(self, h):
        for name, headers in DENY_401:
            self._deny_everywhere(h, headers, 401, 'AUTH_REQUIRED', name,
                                  identity_expected=name in IDENTITY_CONSULTED)

    def test_route_calls_cover_every_rule_and_method(self):
        h = self.harness()
        rules = {r.rule for r in h.app.url_map.iter_rules() if r.rule.startswith('/api/pnl')}
        self.assertEqual(rules, {rule for rule, _m in pnl.ROUTE_TABLE})
        adapter = h.app.url_map.bind('localhost')
        covered = {(adapter.match(path, method=method)[0], method) for path, method, _b in ROUTE_CALLS}
        expected = set()
        for rule in h.app.url_map.iter_rules():
            if rule.rule.startswith('/api/pnl'):
                expected |= {(rule.endpoint, m) for m in rule.methods - {'HEAD', 'OPTIONS'}}
        self.assertEqual(covered, expected)

    def test_matrix_configured(self):
        h = self.harness()
        self._deny_401_all(h)
        for name, token in NON_ADMIN + ADMIN_NOT_LISTED:
            self._deny_everywhere(h, {'Authorization': 'Bearer ' + token}, 403, 'ADMIN_ONLY', name)
        for name, headers in ALLOW:
            for path, method, body in ROUTE_CALLS:
                r = h.req(method, path, headers=headers, body=body)
                ctx = '%s %s %s' % (name, method, path)
                self.assertNotIn(r.status_code, (401, 403), ctx)
                self.assertLess(r.status_code, 500, '%s: %s' % (ctx, r.get_data(as_text=True)[:200]))
                self.assert_pnl_headers(r, ctx)

    def test_matrix_allowlist_missing_or_blank(self):
        for allow in (None, '', ' , ,'):
            h = self.harness(allow=allow)
            self._deny_401_all(h)
            for name, token in NON_ADMIN:
                self._deny_everywhere(h, {'Authorization': 'Bearer ' + token}, 403, 'ADMIN_ONLY', name)
            for name, token in ADMIN_NOT_LISTED + [('admin', T_ADMIN)]:
                self._deny_everywhere(h, {'Authorization': 'Bearer ' + token}, 503, 'PNL_NOT_CONFIGURED',
                                      '%s allow=%r' % (name, allow), reason='allowlist')
            self.assertEqual(h.backend.puts, [])

    def test_matrix_keys_missing_or_invalid(self):
        for keys, reason in (('', 'keys_missing'), ('not-a-fernet-key', 'keys_invalid'),
                             (new_key() + ',not-a-fernet-key', 'keys_invalid')):
            h = self.harness(keys=keys)
            self._deny_401_all(h)
            for name, token in NON_ADMIN + ADMIN_NOT_LISTED:
                self._deny_everywhere(h, {'Authorization': 'Bearer ' + token}, 403, 'ADMIN_ONLY', name)
            st = h.admin('GET', '/api/pnl/status')
            self.assertEqual(st.status_code, 200)
            self.assertFalse(st.get_json()['configured'])
            self.assertIn('store:' + reason, st.get_json()['reasons'])
            for path, method, body in ROUTE_CALLS:
                r = h.admin(method, path, body=body)
                ctx = '%s %s %s' % (reason, method, path)
                if (path, method) in STORE_ROUTES:
                    self.assert_error(r, 503, 'PNL_NOT_CONFIGURED', ctx, reason='store')
                elif path == '/api/pnl/audit':
                    self.assertEqual(r.status_code, 204, ctx)
                elif path != '/api/pnl/status':
                    self.assert_error(r, 404, 'NOT_FOUND', ctx)
            self.assertEqual(h.backend.puts, [])
            self.assertEqual(files_under(h.store_dir), [])

    def test_matrix_no_store_object(self):
        h = self.harness(store=None)
        self._deny_everywhere(h, {}, 401, 'AUTH_REQUIRED', 'anonymous')
        st = h.admin('GET', '/api/pnl/status').get_json()
        self.assertFalse(st['configured'])
        self.assertIn('store:no_backend', st['reasons'])
        for path, method, body in ROUTE_CALLS:
            if (path, method) in STORE_ROUTES:
                self.assert_error(h.admin(method, path, body=body), 503, 'PNL_NOT_CONFIGURED', path, reason='store')

    def test_matrix_kill_switch(self):
        h = self.harness(enabled='off')
        self._deny_401_all(h)
        for name, token in NON_ADMIN + ADMIN_NOT_LISTED:
            self._deny_everywhere(h, {'Authorization': 'Bearer ' + token}, 403, 'ADMIN_ONLY', name)
        self._deny_everywhere(h, {'Authorization': 'Bearer ' + T_ADMIN}, 503, 'PNL_NOT_CONFIGURED',
                              'admin kill switch', reason='disabled')
        for word in ('OFF', 'false', '0', 'disabled', 'no'):
            hw = self.harness(enabled=word)
            self.assert_error(hw.admin('GET', '/api/pnl/status'), 503, 'PNL_NOT_CONFIGURED', word, reason='disabled')
        for word in ('on', 'ON', 'true', ''):
            hw = self.harness(enabled=word)
            self.assertEqual(hw.admin('GET', '/api/pnl/status').status_code, 200, word)
        self.assertEqual(h.backend.puts, [])

    def test_engine_or_routing_missing_answers_503(self):
        h = self.harness(engine=types.ModuleType('engine_without_build_dataset'))
        st = h.admin('GET', '/api/pnl/status').get_json()
        self.assertFalse(st['configured'])
        self.assertIn('engine', st['reasons'])
        self.assert_error(h.admin('GET', '/api/pnl/dataset'), 503, 'PNL_NOT_CONFIGURED', reason='engine')
        r = h.admin('GET', '/api/pnl/settings')
        self.assertEqual(r.status_code, 200)
        self.assertIsNone(r.get_json()['defaults'])
        h2 = self.harness(routing=types.ModuleType('routing_without_route_all'))
        self.assert_error(h2.admin('GET', '/api/pnl/dataset'), 503, 'PNL_NOT_CONFIGURED', reason='routing')

    def test_default_import_failure_keeps_boot_alive(self):
        saved = {m: sys.modules.get(m, None) for m in ('pnl_engine', 'pnl_routing')}
        present = {m: m in sys.modules for m in saved}
        sys.modules['pnl_engine'] = None           # makes "import pnl_engine" raise ImportError
        sys.modules['pnl_routing'] = None
        try:
            h = self.harness(engine=None, routing=None)   # registration itself must not raise
            st = h.admin('GET', '/api/pnl/status').get_json()
            self.assertIn('engine', st['reasons'])
            self.assertIn('routing', st['reasons'])
            self.assert_error(h.admin('GET', '/api/pnl/dataset'), 503, 'PNL_NOT_CONFIGURED', reason='engine')
            self.assertEqual(h.admin('POST', '/api/pnl/costbook', body=synthetic_costbook(1)).status_code, 200)
        finally:
            for m, mod in saved.items():
                if present[m]:
                    sys.modules[m] = mod
                else:
                    sys.modules.pop(m, None)
        self.assertRegex(self._out.getvalue(), r'pnl_engine unavailable: (ModuleNotFound|Import)Error')

    def test_options_preflight_needs_no_credentials(self):
        h = self.harness(allow=None, enabled='off', keys='')
        for path, method, _b in ROUTE_CALLS:
            r = h.client.open(path, method='OPTIONS', headers={'Origin': 'https://example.test',
                                                               'Access-Control-Request-Method': method})
            self.assertEqual(r.status_code, 204, path)
            self.assertEqual(r.get_data(), b'', path)
            self.assert_pnl_headers(r, path)
        self.assertEqual(h.identity.calls, 0)

    def test_head_trailing_slash_and_other_methods(self):
        h = self.harness()
        self.assertEqual(h.req('HEAD', '/api/pnl/status').status_code, 401)
        self.assertEqual(h.req('HEAD', '/api/pnl/status', token='eyJx.pay.staff').status_code, 403)
        r = h.req('HEAD', '/api/pnl/status', token=T_ADMIN)
        self.assertEqual(r.status_code, 200)
        self.assert_pnl_headers(r)
        for token, allowed in ((None, (401, 404)), ('eyJx.pay.staff', (403, 404)), (T_ADMIN, (404,))):
            r = h.req('GET', '/api/pnl/status/', token=token)
            self.assertIn(r.status_code, allowed, token)
            self.assert_pnl_headers(r)
        for method in ('PUT', 'DELETE', 'PATCH'):
            r = h.req(method, '/api/pnl/settings', token=T_ADMIN, body={'settings': {}})
            self.assertEqual(r.status_code, 405, method)
            self.assert_pnl_headers(r, method)
            self.assert_clean_text(r.get_data(as_text=True), method)


# ─────────────────────────────────────────────────────────────────────────────
class TestHeadersAndErrors(PnlTestCase):

    def test_messages_are_plain(self):
        for code, msg in list(pnl.MESSAGES.items()) + list(pnl.REASON_MESSAGES.items()):
            self.assertNotIn('—', msg, code)
            self.assertNotIn('–', msg, code)
            self.assertTrue(msg.endswith('.'), code)
        for code, reason in pnl.REASON_MESSAGES:
            self.assertIn(code, pnl.MESSAGES)
            self.assertEqual(pnl.message_for(code, reason), pnl.REASON_MESSAGES[(code, reason)])
        self.assertEqual(pnl.message_for('INPUTS_UNAVAILABLE', 'other'), pnl.MESSAGES['INPUTS_UNAVAILABLE'])

    def test_identity_exception_is_generic(self):
        h = self.harness()
        self.assert_error(h.req('GET', '/api/pnl/status', token='eyJx.pay.boom'), 401, 'AUTH_REQUIRED')
        self.assertIn('[PnL] identity check failed: RuntimeError', self._out.getvalue())

    def test_unexpected_exception_is_generic(self):
        h = self.harness(backend=RaisingBackend(RuntimeError(SECRET_EXC + ' ' + SENTINEL_TEXT)))
        body = self.assert_error(h.admin('GET', '/api/pnl/costbook'), 500, 'INTERNAL')
        self.assertRegex(body['ref'], r'^[0-9a-f]{8}$')
        self.assertRegex(self._out.getvalue(), r'\[PnL\] error ref=[0-9a-f]{8} GET /api/pnl/costbook RuntimeError')

    def test_backend_unavailable(self):
        h = self.harness(backend=RaisingBackend(StoreUnavailable('s3_head')))
        self.assert_error(h.admin('GET', '/api/pnl/costbook'), 503, 'PNL_STORE_UNAVAILABLE')
        self.assert_error(h.admin('GET', '/api/pnl/dataset'), 503, 'PNL_STORE_UNAVAILABLE')
        st = h.admin('GET', '/api/pnl/status')
        self.assertEqual(st.status_code, 200)
        self.assertIn('store:unavailable', st.get_json()['reasons'])

    def test_engine_exception_is_generic(self):
        h = self.harness(engine='raise')
        h.upload()
        self.assert_error(h.dataset(), 500, 'BUILD_FAILED')
        st = h.admin('GET', '/api/pnl/status').get_json()
        self.assertEqual(st['build']['state'], 'error')
        self.assertEqual(st['build']['error'], {'code': 'BUILD_FAILED', 'class': 'ValueError', 'missing': None})
        self.assertIn('[PnL] build failed code=BUILD_FAILED class=ValueError', self._out.getvalue())

    def test_engine_non_finite_output_is_refused(self):
        h = self.harness(engine='nan')
        h.upload()
        self.assert_error(h.dataset(), 500, 'BUILD_FAILED')

    def test_bad_json_and_non_finite_literals(self):
        h = self.harness()
        for raw in ('not json', '[1, 2]', '{"settings": {"fx": {"rate": NaN}}}',
                    '{"settings": {"fx": {"rate": Infinity}}}', ''):
            self.assert_error(h.admin('POST', '/api/pnl/settings', raw=raw), 400, 'BAD_REQUEST', raw)
        self.assert_error(h.admin('POST', '/api/pnl/audit', raw='{"action": 5}'), 400, 'BAD_REQUEST')

    def test_log_lines_carry_no_values(self):
        h = self.harness()
        h.admin('GET', '/api/pnl/status')
        h.req('GET', '/api/pnl/status')
        log = self._out.getvalue()
        self.assertRegex(log, r'\[PnL\] GET /api/pnl/status 200 \d+ms user=admin@example\.test')
        self.assertRegex(log, r'\[PnL\] GET /api/pnl/status 401 \d+ms user=-')
        self.assertNotIn(T_ADMIN, log)


# ─────────────────────────────────────────────────────────────────────────────
class TestRouteBehavior(PnlTestCase):

    def test_dataset_build_memo_and_inputs(self):
        h = self.harness()
        self.assert_error(h.admin('GET', '/api/pnl/dataset'), 409, 'NO_COSTBOOK')
        etag = h.upload(3)
        r = h.dataset()
        self.assertEqual(r.status_code, 200)
        self.assert_pnl_headers(r)
        ds = r.get_json()
        self.assertEqual(ds['lines']['rows'][0][1], SENTINEL_COST)
        self.assertEqual(ds['inputs']['costbook_etag'], etag)
        self.assertIsNone(ds['inputs']['settings_etag'])
        self.assertIsNone(ds['inputs']['overrides_etag'])
        self.assertEqual(ds['inputs']['ledger_rows'], 1)
        self.assertEqual(len(h.engine.calls), 1)
        call = h.engine.calls[0]
        self.assertEqual(set(call['src']), set(pnl.SOURCE_KEYS))
        self.assertEqual(call['src']['fob_customers'], ['ZZFOB'])
        self.assertEqual(call['src']['today'], '2026-01-02')
        self.assertEqual(call['src']['open_orders']['ok'], True)
        self.assertEqual(call['settings'], {})
        self.assertEqual(call['overrides'], [])
        self.assertIs(call['routing'], h.routing)
        self.assertNotIn('inputs', call['costbook']['records'][0])
        r2 = h.dataset()
        self.assertEqual(r2.get_data(), r.get_data())
        self.assertEqual(len(h.engine.calls), 1)

    def test_dataset_202_while_building_then_200(self):
        gate = threading.Event()
        h = self.harness(gate=gate)
        h.svc.wait_seconds = 0.05
        h.upload()
        r = h.admin('GET', '/api/pnl/dataset')
        self.assertEqual(r.status_code, 202)
        self.assert_pnl_headers(r)
        self.assertTrue(r.get_json()['building'])
        self.assertRegex(r.get_json()['startedAt'], r'^\d{4}-\d\d-\d\dT')
        self.assertEqual(h.admin('GET', '/api/pnl/status').get_json()['build']['state'], 'building')
        gate.set()
        self.assertTrue(h.wait_idle())
        self.assertEqual(h.admin('GET', '/api/pnl/dataset').status_code, 200)
        st = h.admin('GET', '/api/pnl/status').get_json()['build']
        self.assertEqual(st['state'], 'ready')
        self.assertTrue(st['builtAt'])

    def test_refresh_forces_rebuild_at_most_once_per_window(self):
        h = self.harness()
        h.upload()
        self.assertEqual(h.dataset().status_code, 200)
        self.assertEqual(h.dataset(query={'refresh': '1'}).status_code, 200)   # memo is fresh
        self.assertEqual(len(h.engine.calls), 1)
        h.svc.refresh_window = 0
        h.svc._memo['started'] -= 1.0      # the Windows clock can repeat a value for ~15 ms
        self.assertEqual(h.dataset(query={'refresh': '1'}).status_code, 200)
        self.assertEqual(len(h.engine.calls), 2)
        h.svc.refresh_window = 30
        self.assertEqual(h.dataset(query={'refresh': '1'}).status_code, 200)   # rate limited
        self.assertEqual(len(h.engine.calls), 2)

    def test_background_recheck_rebuilds_only_on_change(self):
        state = {}
        h = self.harness(sources=make_sources(state=state))
        h.upload()
        first = h.dataset().get_json()
        h.svc.recheck_seconds = 0
        self.assertEqual(h.dataset().status_code, 200)      # served memo, starts a check
        h.wait_idle()
        self.assertEqual(len(h.engine.calls), 1)             # inputs unchanged: no rebuild
        state['orders'] = state['orders'] + [dict(state['orders'][0], ctrlNo=2)]
        self.assertEqual(h.dataset().get_json(), first)      # still the memo while re-checking
        h.wait_idle()
        self.assertEqual(len(h.engine.calls), 2)
        self.assertEqual(h.dataset().get_json()['notes'][1], 2)

    def test_saved_settings_trigger_rebuild(self):
        h = self.harness()
        h.upload()
        h.dataset()
        r = h.admin('POST', '/api/pnl/settings', body={'settings': {'bulk': {'includeInTotals': True}},
                                                        'expected_etag': None})
        self.assertEqual(r.status_code, 200)
        s_etag = r.get_json()['etag']
        ds = h.dataset().get_json()
        self.assertEqual(ds['inputs']['settings_etag'], s_etag)
        self.assertEqual(len(h.engine.calls), 2)
        seen = h.engine.calls[-1]['settings']
        self.assertEqual(seen['bulk'], {'includeInTotals': True})
        self.assertEqual(seen['updatedBy'], ADMIN)
        self.assertEqual(seen['v'], 1)

    def test_missing_inputs_abort_the_build(self):
        for kw, missing in (({'ledger': False}, ['ledger']), ({'inventory': False}, ['inventory']),
                            ({'orders': False}, ['open_orders'])):
            h = self.harness(sources=make_sources(**kw))
            h.upload()
            body = self.assert_error(h.dataset(), 503, 'INPUTS_UNAVAILABLE', str(kw))
            self.assertEqual(body['missing'], missing)
            self.assertEqual(h.engine.calls, [])
            err = h.admin('GET', '/api/pnl/status').get_json()['build']['error']
            self.assertEqual(err['code'], 'INPUTS_UNAVAILABLE')
            self.assertEqual(err['missing'], missing)
            self.assert_error(h.admin('GET', '/api/pnl/dataset'), 503, 'INPUTS_UNAVAILABLE')  # no new build
            self.assertEqual(h.engine.calls, [])

    def test_engine_cannot_touch_source_rows(self):
        shared = [{'sku': 'ZZAAAA001SLS', 'jtw': 10}]
        sources = make_sources()
        sources['inventory'] = lambda: {'items': shared, 'last_sync': '2026-01-01T00:00:00Z'}
        h = self.harness(engine='mutate', sources=sources)
        h.upload()
        self.assertEqual(h.dataset().status_code, 200)
        self.assertNotIn('factory_fob_usd', shared[0])

    def test_failing_source_is_logged_by_class_only(self):
        sources = make_sources()

        def boom():
            raise RuntimeError(SECRET_EXC)
        sources['sales_analytics'] = boom
        h = self.harness(sources=sources)
        h.upload()
        self.assertEqual(h.dataset().status_code, 200)
        self.assertIsNone(h.engine.calls[0]['src']['sales_analytics'])
        self.assertIn('[PnL] source sales_analytics failed: RuntimeError', self._out.getvalue())

    def test_settings_roundtrip_and_validation(self):
        h = self.harness()
        r = h.admin('GET', '/api/pnl/settings').get_json()
        self.assertEqual(r['settings'], None)
        self.assertEqual(r['etag'], None)
        self.assertEqual(r['defaults']['fx']['rate'], SYNTH_RATE)
        good = {'fx': {'rate': 7.4681, 'asOf': '2026-01-01', 'basis': 'current_usd', 'extra': 1},
                'tariff': {'addonPct': {'zz': 5}}, 'royalty': {'defaultPct': 3, 'byBrand': {'zz': 0}},
                'gridPrecedence': {'zz': ['aa', 'bb'], '_default': ['BB']},
                'opex': {'items': [{'name': 'Rent', 'monthly': 10}]}, 'confirmed': {'fx': True},
                'bogus': 1, 'extraBlock': {'k': 2}, 'updatedBy': 'forged@example.test', 'v': 99}
        r = h.admin('POST', '/api/pnl/settings', body={'settings': good, 'expected_etag': None})
        self.assertEqual(r.status_code, 200, r.get_data(as_text=True)[:300])
        out = r.get_json()
        s = out['settings']
        self.assertEqual(s['tariff']['addonPct'], {'ZZ': 5})
        self.assertEqual(s['royalty']['byBrand'], {'ZZ': 0})
        self.assertEqual(s['gridPrecedence'], {'ZZ': ['AA', 'BB'], '_default': ['BB']})
        self.assertEqual(s['extraBlock'], {'k': 2})
        self.assertEqual(s['updatedBy'], ADMIN)
        self.assertEqual(s['v'], 1)
        self.assertNotIn('bogus', s)
        self.assertIn('bogus', out['dropped'])
        self.assertIn('fx.extra', out['dropped'])
        got = h.admin('GET', '/api/pnl/settings').get_json()
        self.assertEqual(got['settings'], s)
        self.assertEqual(got['etag'], out['etag'])
        cases = [('{"settings": {"fx": {"rate": 1e999}}}', 'fx.rate', 'not_a_finite_number'),
                 (json.dumps({'settings': {'tariff': {'addonPct': {'ZZ': 500}}}}), 'tariff.addonPct.ZZ', 'out_of_range'),
                 (json.dumps({'settings': {'routing': {'picksAsWarehouse': 'yes'}}}), 'routing.picksAsWarehouse', 'not_a_boolean'),
                 (json.dumps({'settings': {'fx': {'asOf': 'Jan 1'}}}), 'fx.asOf', 'invalid_date'),
                 (json.dumps({'settings': {'factories': {'bad code!': {'name': 'x'}}}}), 'factories.bad?code?', 'invalid_code')]
        for raw, path, issue in cases:
            body = self.assert_error(h.admin('POST', '/api/pnl/settings', raw=raw), 422, 'INVALID_SETTINGS', raw)
            self.assertIn({'path': path, 'issue': issue}, body['problems'])
        self.assert_error(h.admin('POST', '/api/pnl/settings', body={'settings': 'x'}), 400, 'BAD_REQUEST')
        self.assertEqual(h.admin('GET', '/api/pnl/settings').get_json()['etag'], out['etag'])   # nothing saved

    def test_settings_null_rate_and_gate_fallback(self):
        # fx.rate null means "the cost book's own base rate" (the engine default). gateFallback is
        # one of two fixed values.
        h = self.harness()
        body = {'settings': {'fx': {'rate': None, 'asOf': None, 'basis': 'current_usd'},
                             'routing': {'picksAsWarehouse': False, 'honorAssignments': True,
                                         'gateFallback': 'engine'}},
                'expected_etag': None}
        r = h.admin('POST', '/api/pnl/settings', body=body)
        self.assertEqual(r.status_code, 200, r.get_data(as_text=True)[:300])
        s = r.get_json()['settings']
        self.assertIsNone(s['fx']['rate'])
        self.assertEqual(s['routing']['gateFallback'], 'engine')
        etag = r.get_json()['etag']
        for bad in ('maybe', 1, None, ['fifo']):
            raw = json.dumps({'settings': {'routing': {'gateFallback': bad}}, 'expected_etag': etag})
            b = self.assert_error(h.admin('POST', '/api/pnl/settings', raw=raw), 422, 'INVALID_SETTINGS', raw)
            self.assertIn({'path': 'routing.gateFallback', 'issue': 'invalid_choice'}, b['problems'])
        self.assertEqual(h.admin('GET', '/api/pnl/settings').get_json()['etag'], etag)     # nothing saved

    def test_real_engine_defaults_pass_validation(self):
        # The engine's public defaults must pass the settings check unchanged: the review server
        # seeds them, and the Assumptions tab posts them back. Loaded from the file, so the fake
        # engines other tests put in sys.modules never interfere.
        import importlib.util
        spec = importlib.util.spec_from_file_location('pnl_engine_defaults_check', os.path.join(REPO, 'pnl_engine.py'))
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        d = json.loads(json.dumps(mod.DEFAULT_SETTINGS))
        clean, problems, dropped = pnl.validate_settings(d, d)
        self.assertEqual((problems, dropped), ([], []))
        self.assertIsNone(clean['fx']['rate'])
        self.assertEqual(clean['routing'], d['routing'])

    def test_overrides_roundtrip_stamps_and_validation(self):
        h = self.harness()
        o1 = {'id': 'ovr_1', 'scope': 'ref_style', 'key': {'ref': 'zz00001', 'style': 'zzaaaa001sls', 'junk': 'x'},
              'fobU': SENTINEL_COST, 'reason': 'synthetic', 'effective': '2026-01-01',
              'by': 'forged@example.test', 'at': 'forged'}
        r = h.admin('POST', '/api/pnl/overrides', body={'overrides': [o1], 'expected_etag': None})
        self.assertEqual(r.status_code, 200, r.get_data(as_text=True)[:300])
        self.assertEqual(r.get_json()['count'], 1)
        self.assertIn('overrides[0].key.junk', r.get_json()['dropped'])
        got = h.admin('GET', '/api/pnl/overrides').get_json()
        rec = got['overrides'][0]
        self.assertEqual(rec['by'], ADMIN)
        self.assertNotEqual(rec['at'], 'forged')
        self.assertEqual(rec['key'], {'ref': 'ZZ00001', 'style': 'ZZAAAA001SLS', 'design': '', 'fabric': '', 'category': ''})
        r = h.admin('POST', '/api/pnl/overrides', body={'overrides': got['overrides'], 'expected_etag': got['etag']})
        self.assertEqual(r.status_code, 200)
        again = h.admin('GET', '/api/pnl/overrides').get_json()['overrides'][0]
        self.assertEqual((again['by'], again['at']), (rec['by'], rec['at']))
        bad = [
            ([dict(o1, scope='bogus')], 'overrides[0].scope', 'invalid'),
            ([dict(o1, scope='ref', key={'style': 'ZZAAAA001SLS'})], 'overrides[0].key.ref', 'missing'),
            ([dict(o1, fobU=0)], 'overrides[0].fobU', 'out_of_range'),
            ([dict(o1, fobU='7')], 'overrides[0].fobU', 'not_a_finite_number'),
            ([o1, dict(o1)], 'overrides[1].id', 'duplicate'),
            ([dict(o1, effective='soon')], 'overrides[0].effective', 'invalid_date'),
            ([dict(o1, scope='fabric_category', key={'fabric': 'ZZ', 'category': 'Dress Shirt'})],
             'overrides[0].key.category', 'invalid'),
        ]
        for items, path, issue in bad:
            body = self.assert_error(h.admin('POST', '/api/pnl/overrides', body={'overrides': items}),
                                     422, 'INVALID_OVERRIDES', path)
            self.assertIn({'path': path, 'issue': issue}, body['problems'])
        self.assert_error(h.admin('POST', '/api/pnl/overrides', body={'overrides': 'x'}), 400, 'BAD_REQUEST')

    def test_costbook_validation(self):
        h = self.harness()

        def with_params(**over):
            cb = synthetic_costbook(1)
            cb['params'] = synthetic_params(**over)
            return cb
        dup = synthetic_costbook(2)
        dup['records'][1]['id'] = dup['records'][0]['id']
        big = synthetic_costbook(1)
        big['records'][0]['price_usd'] = 5000
        rmb = synthetic_costbook(1)
        rmb['records'][0]['rmb_price'] = -1
        nodict = synthetic_costbook(1)
        nodict['records'].append('x')
        noprice = synthetic_costbook(1)
        del noprice['records'][0]['price_usd']
        huge = synthetic_costbook(1)
        huge['records'][0]['price_usd'] = 10 ** 400
        huge_other = synthetic_costbook(1)
        huge_other['records'][0]['gsm'] = 10 ** 400
        cases = [
            (json.dumps(dup), 'records[1].id', 'duplicate'),
            (json.dumps(synthetic_costbook(1)).replace(SENTINEL_TEXT, '1e999'), 'records[0].price_usd', 'not_finite'),
            (json.dumps(big), 'records[0].price_usd', 'out_of_range'),
            (json.dumps(rmb), 'records[0].rmb_price', 'out_of_range'),
            (json.dumps(nodict), 'records[1]', 'not_an_object'),
            (json.dumps(noprice), 'records[0].price_usd', 'missing'),
            (json.dumps(dict(synthetic_costbook(1), v=2)), 'v', 'unsupported_version'),
            (json.dumps(dict(synthetic_costbook(1), records=[])), 'records', 'missing'),
            (json.dumps(huge), 'records[0].price_usd', 'out_of_range'),
            (json.dumps(huge_other), 'records[0].gsm', 'out_of_range'),
            (json.dumps(with_params(fxBase=12345.5)).replace('12345.5', '1e999'),
             'params.fxBase', 'not_a_finite_number'),
        ]
        no_params = synthetic_costbook(1)
        del no_params['params']
        no_fit = synthetic_costbook(1)
        del no_fit['params']['fitPremium']
        param_cases = [
            (no_params, 'params', 'missing'),
            (dict(synthetic_costbook(1), params='x'), 'params', 'wrong_type'),
            (no_fit, 'params.fitPremium', 'missing'),
            (with_params(fitPremium={'regular': 0.2229}), 'params.fitPremium.bigTall', 'missing'),
            (with_params(fitPremium={'regular': 500, 'bigTall': 0.3331}), 'params.fitPremium.regular', 'out_of_range'),
            (with_params(gridPrecedence='abc'), 'params.gridPrecedence', 'not_an_object'),
            (with_params(gridPrecedence=5), 'params.gridPrecedence', 'not_an_object'),
            (with_params(gridPrecedence={'ZZ': 'YY'}), 'params.gridPrecedence.ZZ', 'not_a_list'),
            (with_params(kitPcs=[1, 2]), 'params.kitPcs', 'not_an_object'),
            (with_params(kitPcs={'ZZKIT0001': 1}), 'params.kitPcs.ZZKIT0001', 'out_of_range'),
            (with_params(kitPcs={'ZZKIT0001': 12.5}), 'params.kitPcs.ZZKIT0001', 'not_an_integer'),
            (with_params(kitPcs={'Z': 12}), 'params.kitPcs.Z', 'invalid_style'),
            (with_params(programMap='x'), 'params.programMap', 'not_an_object'),
            (with_params(programMap={'ZZ-PROGRAM1': 'x'}), 'params.programMap.ZZ-PROGRAM1', 'not_an_object'),
            (with_params(customerGroupPrefix=['a']), 'params.customerGroupPrefix', 'not_an_object'),
            (with_params(customerGroupPrefix='x'), 'params.customerGroupPrefix', 'not_an_object'),
            (with_params(poolOrder=5), 'params.poolOrder', 'not_an_object'),
            (with_params(poolOrder={'all': 'x'}), 'params.poolOrder.all', 'not_a_list'),
            (with_params(poolOrder={'XX': 5}), 'params.poolOrder.XX', 'not_an_object'),
            (with_params(poolOrder={'all': ['not a pool']}), 'params.poolOrder.all[0]', 'invalid_pool'),
            (with_params(fxBase='abc'), 'params.fxBase', 'not_a_finite_number'),
            (with_params(fxBase=None), 'params.fxBase', 'not_a_finite_number'),
            (with_params(fxBase=0), 'params.fxBase', 'out_of_range'),
            (with_params(fxBase=-3), 'params.fxBase', 'out_of_range'),
            (with_params(fxBase=0.67), 'params.fxBase', 'out_of_range'),
            (with_params(ssDelta=10 ** 400), 'params.ssDelta', 'out_of_range'),
            (with_params(ssDelta='x'), 'params.ssDelta', 'not_a_finite_number'),
            (with_params(fiberByStyle={'ZZAAAA001SLS': 'silk'}), 'params.fiberByStyle.ZZAAAA001SLS', 'invalid_choice'),
        ]
        cases += [(json.dumps(cb), path, issue) for cb, path, issue in param_cases]
        for raw, path, issue in cases:
            r = h.admin('POST', '/api/pnl/costbook', raw=raw)
            body = self.assert_error(r, 422, 'INVALID_COSTBOOK', path)
            self.assertIn({'path': path, 'issue': issue}, body['problems'])
            for leak in ('inf', 'Infinity', '5000', 'ZZ!A0', '0.2229', '0.3331', SENTINEL_TEXT):
                self.assertNotIn(leak, r.get_data(as_text=True), path)
        # NaN / Infinity literals never reach validation: the JSON parser refuses them
        raw = json.dumps(dict(synthetic_costbook(1), params={'x': float('inf')}))
        self.assertIn('Infinity', raw)
        self.assert_error(h.admin('POST', '/api/pnl/costbook', raw=raw), 400, 'BAD_REQUEST')
        self.assertEqual(h.backend.puts, [])
        cb = dict(synthetic_costbook(2), junk={'a': 1})
        cb['records'][0]['production_ref_resolved'] = 'N/A'
        cb['params'] = dict(cb['params'], label=SENTINEL_LABEL)
        r = h.admin('POST', '/api/pnl/costbook', body={'costbook': cb})
        self.assertEqual(r.status_code, 200, r.get_data(as_text=True)[:300])
        out = r.get_json()
        self.assertEqual(out['records'], 2)
        self.assertEqual(out['dropped'], ['junk', 'params.label'])
        self.assertEqual(out['warningCount'], 1)
        self.assertEqual((out['paramChanges'], out['paramChangeCount']), ([], 0))     # first upload
        stored = h.admin('GET', '/api/pnl/costbook').get_json()
        self.assertEqual(stored['etag'], out['etag'])
        self.assertNotIn('junk', stored['costbook'])
        self.assertEqual(stored['costbook']['params'], synthetic_params())
        self.assertNotIn('inputs', stored['costbook']['records'][0])
        self.assertEqual(stored['costbook']['records'][0]['price_usd'], SENTINEL_COST)

    def test_costbook_too_large(self):
        h = self.harness()
        h.svc.max_costbook_bytes = 200
        self.assert_error(h.admin('POST', '/api/pnl/costbook', body=synthetic_costbook(3)), 413, 'TOO_LARGE')
        self.assert_error(h.admin('POST', '/api/pnl/costbook', body={'costbook': synthetic_costbook(3)}),
                          413, 'TOO_LARGE')
        self.assertEqual(h.backend.puts, [])

    def test_audit_line_keeps_only_allowlisted_keys(self):
        h = self.harness()
        r = h.admin('POST', '/api/pnl/audit', body={'action': 'export', 'detail': {
            'tab': 'orders', 'rows': 12, 'total': 1234.56, 'note': 'a<b>$', 'gp': 1234567,
            'rev': '$1,234,567.89', 'cost': 88, 'sheets': ['Orders', 'Lines 2,500,000'], 'whatIf': True,
            'file': 'Versa P&L 2026-09-14 GP 9876543.xlsx', 'count': '42', 'n': 3.5}})
        self.assertEqual(r.status_code, 204)
        self.assertEqual(r.get_data(), b'')
        self.assert_pnl_headers(r)
        line = [ln for ln in self._out.getvalue().splitlines() if ln.startswith('[PnL] audit ')][-1]
        self.assertEqual(line, '[PnL] audit action=export detail=tab=orders file=Versa PL 2026-09-14 GP #.xlsx '
                               'sheets=Orders,Lines # rows=12 count=42 whatIf=yes user=%s' % ADMIN)
        for leak in ('1234', '567', '88', 'total', 'note', 'gp=', 'rev', 'cost', '<b>', '3.5', ' n='):
            self.assertNotIn(leak, line)
        for detail, expected in (('tab=orders rows=5 total=9999 gp=123', 'tab=orders rows=5'),
                                 ({'drill': 'style', 'Tab': 'styles', 'ROWS': True, 'whatif': 'no'},
                                  'tab=styles drill=style whatIf=no'),
                                 ({'rows': -1, 'count': 10 ** 12, 'file': 7}, '-'),
                                 (['tab', 'orders'], '-'), (None, '-')):
            h.admin('POST', '/api/pnl/audit', body={'action': 'print', 'detail': detail})
            line = [ln for ln in self._out.getvalue().splitlines() if ln.startswith('[PnL] audit ')][-1]
            self.assertEqual(line, '[PnL] audit action=print detail=%s user=%s' % (expected, ADMIN), detail)
        self.assert_error(h.admin('POST', '/api/pnl/audit', body={'action': 'Export Now'}), 400, 'BAD_REQUEST')

    def test_audit_accepts_only_the_page_actions(self):
        h = self.harness()

        def audit_lines():
            return [ln for ln in self._out.getvalue().splitlines() if ln.startswith('[PnL] audit ')]
        for action in ('export', 'print'):
            self.assertEqual(h.admin('POST', '/api/pnl/audit', body={'action': action, 'detail': {'tab': 'orders'}})
                             .status_code, 204, action)
        self.assertEqual(audit_lines(), ['[PnL] audit action=%s detail=tab=orders user=%s' % (a, ADMIN)
                                         for a in ('export', 'print')])
        for action in ('gp_1234567.89', 'export.1234567', 'print-99999', 'exports', 'view', 'EXPORT', ' export',
                       'print ', '', 7, 12.5, True, None, ['export'], {'a': 'export'}):
            r = h.admin('POST', '/api/pnl/audit', body={'action': action, 'detail': {'tab': 'orders'}})
            self.assert_error(r, 400, 'BAD_REQUEST', repr(action))
        self.assert_error(h.admin('POST', '/api/pnl/audit', body={'detail': {'tab': 'orders'}}), 400, 'BAD_REQUEST')
        self.assertEqual(len(audit_lines()), 2)                 # a refused action logs nothing
        log = self._out.getvalue()
        for leak in ('1234567', '99999', 'gp_'):
            self.assertNotIn(leak, log)

    def test_status_shape_has_no_money(self):
        h = self.harness()
        h.upload(3)
        h.admin('POST', '/api/pnl/overrides', body={'overrides': [
            {'id': 'o1', 'scope': 'style', 'key': {'style': 'ZZAAAA001SLS'}, 'fobU': SENTINEL_COST}]})
        r = h.admin('GET', '/api/pnl/status')
        self.assertEqual(r.status_code, 200)
        self.assert_pnl_headers(r)
        st = r.get_json()
        self.assertTrue(st['configured'], st['reasons'])
        self.assertEqual(st['reasons'], [])
        self.assertEqual(st['you'], {'email': ADMIN})
        self.assertTrue(st['versions'])
        cb = st['objects']['costbook']
        self.assertEqual((cb['present'], cb['records'], cb['generatedAt'], cb['readable']),
                         (True, 3, '2026-01-01T00:00:00Z', True))
        self.assertEqual(st['objects']['settings']['present'], False)
        self.assertEqual((st['objects']['overrides']['present'], st['objects']['overrides']['count']), (True, 1))
        self.assertEqual(st['build']['state'], 'idle')
        self.assertEqual(st['keys'], {'count': 1, 'ids': [h.store.kid]})
        for label in ('costbook', 'overrides'):
            self.assertEqual((st['objects'][label]['kid'], st['objects'][label]['keyKnown'],
                              st['objects'][label]['readable']), (h.store.kid, True, True), label)
        self.assertEqual((st['objects']['settings']['readable'], st['objects']['settings']['kid']), (None, None))
        self.assert_clean_text(r.get_data(as_text=True), 'status')


# ─────────────────────────────────────────────────────────────────────────────
class _FakeResp:
    def __init__(self, status, payload):
        self.status_code = status
        self.content = json.dumps(payload).encode('utf-8')

    def json(self):
        return json.loads(self.content)


class _QueueHttp:
    """requests stand-in: returns queued responses (or raises queued exceptions)."""

    def __init__(self):
        self.queue = []
        self.calls = []

    def get(self, url, headers=None, timeout=None, params=None):
        self.calls.append({'url': url, 'headers': dict(headers or {}), 'timeout': timeout})
        item = self.queue.pop(0)
        if isinstance(item, Exception):
            raise item
        return item


class _IdentityHttp:
    def __init__(self, user_status=200, user=None, prof_status=200, rows=None):
        self.user_status, self.prof_status = user_status, prof_status
        self.user = user if user is not None else {'id': 'u-1', 'email': ' Admin@Example.TEST '}
        self.rows = rows if rows is not None else [{'role': 'Staff', 'factory_prefix': None, 'is_admin': True}]
        self.calls = 0

    def get(self, url, headers=None, timeout=None):
        self.calls += 1
        if url.endswith('/auth/v1/user'):
            return _FakeResp(self.user_status, self.user)
        return _FakeResp(self.prof_status, self.rows)


class TestAnalyticsRoute(PnlTestCase):
    """/api/pnl/analytics (Sep 22 2026): the invoiced-history cube joined with
    the dataset's factory-cost maps for the Inventory Analytics tool."""

    CUBE = {'ready': True, 'v': 1,
            'customers': {'ROSS': {'ZZAAAA001': {'2026-08': [10, 95.0]}},
                          'NORD_DROP': {'ZZBBBB002': {'2026-07': [3, 30.0]}}},
            'source': {'rows': 13, 'from': '2019-11-01', 'to': '2026-08-21', 'ingestedAt': 'stamp1'},
            'history': {'label': 'Invoices through Aug 21, 2026.'},
            'pending': {'ready': True, 'basis': 'estimate', 'totals': {'units': 5},
                        'byMonth': [], 'customers': {'ROSS': {'units': 5}}},
            'pendingCube': {'ROSS': {'ZZAAAA001': {'2026-09': [5, 55.0]}}},
            'pendingReady': True}

    def eng_with_history(self):
        eng = make_engine()
        eng.HISTORY_CUSTOMER_ALIAS = {'NORD_DROP': 'NORD'}

        def build_dataset(src, costbook, settings, overrides, now_iso, routing_module):
            return {'v': 1, 'builtAt': now_iso, 'asOf': src['today'],
                    'shipped': {'byCustomer': {'fields': ['cust', 'base', 'fobU', 'grade',
                                                          'units', 'fob', 'duty', 'freight', 'fees'],
                                               'rows': [['ROSS', 'ZZAAAA001', SENTINEL_COST, 'A',
                                                         10, 10 * SENTINEL_COST, SENTINEL_COST, SENTINEL_COST, 0],
                                                        ['NORD', 'ZZBBBB002', None, 'D',
                                                         0, None, 0, 0, 0]]}},
                    'styles': {'fields': ['base', 'fobU', 'grade'],
                               'rows': [['ZZAAAA001', SENTINEL_COST + 1, 'A'],
                                        ['ZZBBBB002', SENTINEL_COST + 2, 'B']]}}
        eng.build_dataset = build_dataset
        return eng

    def cube(self):
        return json.loads(json.dumps(self.CUBE))

    def test_states_and_the_joined_payload(self):
        h = self.harness(engine=self.eng_with_history())
        h.upload()
        h.svc.sales_matrix = None                                   # getter not wired
        self.assert_error(h.admin('GET', '/api/pnl/analytics'), 503, 'INPUTS_UNAVAILABLE',
                          reason='sales_matrix')
        h.svc.sales_matrix = lambda: {'building': True}
        r = h.admin('GET', '/api/pnl/analytics')
        self.assertEqual(r.status_code, 202)
        self.assertEqual(r.get_json(), {'building': True, 'part': 'matrix'})
        h.svc.sales_matrix = self.cube
        deadline = time.time() + 10
        r = h.admin('GET', '/api/pnl/analytics')
        while r.status_code == 202 and time.time() < deadline:
            h.wait_idle()
            r = h.admin('GET', '/api/pnl/analytics')
        self.assertEqual(r.status_code, 200, r.get_data(as_text=True)[:200])
        self.assert_pnl_headers(r)
        d = json.loads(gzip.decompress(r.data)) if r.headers.get('Content-Encoding') == 'gzip' else r.get_json()
        self.assertTrue(d['ready'])
        self.assertEqual(d['matrix']['customers']['ROSS']['ZZAAAA001']['2026-08'], [10, 95.0])
        self.assertEqual(d['custAlias']['NORD_DROP'], 'NORD')
        self.assertEqual(d['costByCustomer'], {'ROSS': {'ZZAAAA001': SENTINEL_COST}})   # a None fobU is dropped
        # cost2: [factory cost per history unit, import add per history unit], the
        # customs formula's dollars from the engine (duty + freight + fees over units).
        self.assertEqual(d['cost2ByCustomer'],
                         {'ROSS': {'ZZAAAA001': [round(10 * SENTINEL_COST / 10, 4),
                                                 round(2 * SENTINEL_COST / 10, 4)]}})
        self.assertEqual(d['costByStyle'], {'ZZAAAA001': SENTINEL_COST + 1, 'ZZBBBB002': SENTINEL_COST + 2})
        self.assertEqual(d['costGrades'], {'ZZAAAA001': 'A', 'ZZBBBB002': 'B'})
        self.assertEqual(d['history']['label'], 'Invoices through Aug 21, 2026.')
        self.assertTrue(d['pendingReady'])
        self.assertEqual(d['pendingCube'], {'ROSS': {'ZZAAAA001': {'2026-09': [5, 55.0]}}})
        self.assertTrue(d['datasetBuiltAt'])

    def test_gzip_when_accepted(self):
        h = self.harness(engine=self.eng_with_history())
        h.upload()
        h.svc.sales_matrix = self.cube
        self.assertEqual(h.dataset().status_code, 200)              # warm the dataset memo
        r = h.admin('GET', '/api/pnl/analytics', headers={'Accept-Encoding': 'gzip'})
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.headers.get('Content-Encoding'), 'gzip')
        self.assertIn('Accept-Encoding', r.headers.get('Vary', ''))
        self.assertTrue(json.loads(gzip.decompress(r.data))['ready'])

    def test_cost_maps_follow_a_rebuild(self):
        h = self.harness(engine=self.eng_with_history())
        h.upload()
        h.svc.sales_matrix = self.cube
        self.assertEqual(h.dataset().status_code, 200)
        maps1 = h.svc._analytics_cost_maps(tuple(h.store.head(n) for n in pnl.OBJECTS))
        self.assertEqual(maps1['byStyle'], {'ZZAAAA001': SENTINEL_COST + 1, 'ZZBBBB002': SENTINEL_COST + 2})
        h.upload(4)                                                 # a new costbook: new key, new build
        self.assertEqual(h.dataset().status_code, 200)
        key2 = tuple(h.store.head(n) for n in pnl.OBJECTS)
        maps2 = h.svc._analytics_cost_maps(key2)
        self.assertEqual(maps2['key'], key2)


class TestAppWiring(unittest.TestCase):
    """Static and extracted checks on app.py. app.py is never imported (it starts
    threads and touches folders at import); functions are pulled out with ast."""

    SA_NAMES = ['_pnl_sa_cache', '_pnl_sa_lock', '_PNL_SA_TTL', '_PNL_SA_FAIL_BACKOFF',
                '_PNL_SA_BUILDING_POLL', '_pnl_sa_fetch', '_pnl_sales_analytics', '_pnl_sources']

    @classmethod
    def setUpClass(cls):
        with open(os.path.join(REPO, 'app.py'), encoding='utf-8') as f:
            cls.src = f.read()
        cls.tree = ast.parse(cls.src)
        cls.body = cls.tree.body

    def _assign_value(self, name):
        for node in self.body:
            if isinstance(node, ast.Assign) and any(isinstance(t, ast.Name) and t.id == name for t in node.targets):
                return node.value
        self.fail('%s not found in app.py' % name)

    def _exec_nodes(self, names, ns):
        nodes, found = [], set()
        for node in self.body:
            if isinstance(node, ast.FunctionDef) and node.name in names:
                nodes.append(node)
                found.add(node.name)
            elif isinstance(node, ast.Assign):
                for t in node.targets:
                    if isinstance(t, ast.Name) and t.id in names:
                        nodes.append(node)
                        found.add(t.id)
        self.assertEqual(found, set(names))
        exec(compile(ast.Module(body=nodes, type_ignores=[]), 'app.py', 'exec'), ns)
        return ns

    def test_pnl_never_in_open_prefixes_or_allow_lists(self):
        exact = ast.literal_eval(self._assign_value('_AUTHZ_OPEN_EXACT'))
        prefixes = ast.literal_eval(self._assign_value('_AUTHZ_OPEN_PREFIXES'))
        reads = ast.literal_eval(self._assign_value('_AUTHZ_CATALOG_READS'))
        machine = ast.literal_eval(self._assign_value('_AUTHZ_MACHINE_EXTRA'))
        posts = ast.literal_eval(self._assign_value('_AUTHZ_CATALOG_POSTS'))
        scope_keys = {ast.literal_eval(k) for k in self._assign_value('_SCOPE_FILTERS').keys}
        samples = ['/api/pnl', '/api/pnl/'] + [rule.replace('<path:rest>', 'x/y') for rule, _m in pnl.ROUTE_TABLE]
        for s in samples:
            for coll in (exact, reads, machine, posts, scope_keys):
                self.assertNotIn(s, coll)
                self.assertNotIn(s.rstrip('/'), coll)
            self.assertFalse(s.startswith(tuple(prefixes)), s)
            self.assertFalse(s.startswith('/download/'), s)
        for coll in (exact, prefixes, reads, machine, posts, scope_keys):
            for entry in coll:
                self.assertNotIn('pnl', str(entry).lower())

    def test_authz_gate_has_no_pnl_allowance(self):
        fn = next(n for n in self.body if isinstance(n, ast.FunctionDef) and n.name == 'authz_gate')
        self.assertNotIn('pnl', ast.get_source_segment(self.src, fn).lower())

    def test_registration_block(self):
        scorecard = main = block = None
        for node in self.body:
            if (isinstance(node, ast.Expr) and isinstance(node.value, ast.Call)
                    and getattr(node.value.func, 'id', '') == 'register_scorecard_routes'):
                scorecard = node.lineno
            if (isinstance(node, ast.If) and isinstance(node.test, ast.Compare)
                    and getattr(node.test.left, 'id', '') == '__name__'):
                main = node.lineno
            if isinstance(node, ast.Try) and any(isinstance(s, ast.ImportFrom) and s.module == 'pnl'
                                                 for s in node.body):
                block = node
        self.assertIsNotNone(scorecard)
        self.assertIsNotNone(main)
        self.assertIsNotNone(block, 'no try-wrapped "from pnl import ..." block')
        self.assertLess(scorecard, block.lineno)
        self.assertLess(block.lineno, main)
        self.assertEqual(len(block.handlers), 1)
        handler = block.handlers[0]
        self.assertEqual(getattr(handler.type, 'id', None), 'Exception')
        seg = ast.get_source_segment(self.src, handler)
        self.assertIn('[PnL] disabled: ', seg)
        self.assertIn('type(', seg)
        self.assertIn('.__name__', seg)
        self.assertNotIn('str(', seg)
        self.assertNotIn('format_exc', seg)
        call = next(n for n in ast.walk(block) if isinstance(n, ast.Call)
                    and getattr(n.func, 'id', '') == 'register_pnl_routes')
        kws = {k.arg: k.value for k in call.keywords}
        self.assertEqual(set(kws), {'store', 'caller_identity', 'sources'})
        self.assertEqual(kws['caller_identity'].id, '_pnl_caller_identity')
        self.assertEqual(kws['sources'].func.id, '_pnl_sources')
        store_src = ast.get_source_segment(self.src, kws['store'])
        for part in ('EncryptedStore(', 'S3Store(', 'PNL_DATA_KEYS'):
            self.assertIn(part, store_src)

    def test_pnl_names_only_inside_the_pnl_block(self):
        start = next(n.lineno for n in self.body if isinstance(n, ast.Assign)
                     and any(getattr(t, 'id', '') == '_pnl_sa_cache' for t in n.targets))
        scorecard = next(n.lineno for n in self.body if isinstance(n, ast.Expr) and isinstance(n.value, ast.Call)
                         and getattr(n.value.func, 'id', '') == 'register_scorecard_routes')
        self.assertGreater(start, scorecard)
        # The one allowed exception: the P&L resource inside the app-wide CORS(...) call. It has to
        # sit in that same call (flask-cors skips a response that already carries CORS headers).
        cors = next(n for n in self.body if isinstance(n, ast.Expr) and isinstance(n.value, ast.Call)
                    and getattr(n.value.func, 'id', '') == 'CORS')
        cors_lines = range(cors.lineno, cors.end_lineno + 1)
        offenders = []
        for node in ast.walk(self.tree):
            text = None
            if isinstance(node, ast.Name):
                text = node.id
            elif isinstance(node, ast.Attribute):
                text = node.attr
            elif isinstance(node, ast.Constant) and isinstance(node.value, str):
                text = node.value
            elif isinstance(node, (ast.FunctionDef, ast.ClassDef)):
                text = node.name
            elif isinstance(node, ast.ImportFrom):
                text = node.module or ''
            if (text and 'pnl' in text.lower() and getattr(node, 'lineno', start) < start
                    and getattr(node, 'lineno', 0) not in cors_lines):
                offenders.append((node.lineno, text[:60]))
        self.assertEqual(offenders, [], 'P&L names outside the registration block (AI tools, MCP, gate?)')

    def test_caller_identity_returns_uid_and_email(self):
        def run(http):
            ns = {'time': time, 'http_requests': http, 'VERSA_DOCS_SUPABASE_URL': 'https://sb.example.test',
                  'VERSA_DOCS_SUPABASE_ANON': 'anon', '_identity_cache': {}, '_identity_lock': threading.Lock(),
                  '_IDENTITY_TTL': 300}
            self._exec_nodes(['_caller_identity'], ns)
            return ns['_caller_identity']
        http = _IdentityHttp()
        ci = run(http)
        self.assertEqual(ci('eyJx.pay.any'), {'role': 'staff', 'factory_prefix': '', 'is_admin': True,
                                              'uid': 'u-1', 'email': 'admin@example.test'})
        calls = http.calls
        self.assertEqual(ci('eyJx.pay.any')['uid'], 'u-1')
        self.assertEqual(http.calls, calls)            # served from the 300 s cache
        self.assertIsNone(run(_IdentityHttp(user_status=401))('eyJx.pay.any'))
        self.assertIsNone(run(_IdentityHttp(rows=[]))('eyJx.pay.any'))
        self.assertIsNone(run(_IdentityHttp(user={'email': 'x@example.test'}))('eyJx.pay.any'))
        truthy = run(_IdentityHttp(rows=[{'role': 'staff', 'is_admin': 'true'}]))('eyJx.pay.any')
        self.assertIs(truthy['is_admin'], False)
        self.assertEqual(run(_IdentityHttp(user={'id': 'u-2'}))('eyJx.pay.any')['email'], '')

    def _sources_ns(self, http):
        counters = {'manual': 0, 'assign': 0, 'suppress': 0}

        def bump(k):
            def f():
                counters[k] += 1
            return f

        class DeferredThread:
            pending = []

            def __init__(self, target=None, args=(), kwargs=None, daemon=None, name=None):
                self.target, self.args, self.kwargs = target, args, kwargs or {}

            def start(self):
                DeferredThread.pending.append(self)

            def run_now(self):
                return self.target(*self.args, **self.kwargs)

        ns = {
            'json': json, 'time': time, 'datetime': datetime,
            'threading': types.SimpleNamespace(Lock=threading.Lock, Thread=DeferredThread),
            'http_requests': http, 'OPEN_ORDERS_API_URL': 'https://oo.example.test',
            '_oo_api_headers': lambda: {'X-Api-Key': 'k'},
            '_inv_lock': threading.Lock(),
            '_inventory': {'items': [{'sku': 'ZZAAAA001SLS'}], 'last_sync': '2026-01-01T00:00:00Z'},
            '_ledger_rows': lambda: [{'style': 'ZZAAAA001SLS', 'units': 1}],
            '_production_last_sync': 1767225600.0,
            '_apo_lock': threading.Lock(), '_apo_data': [{'style': 'ZZAAAA001SLS', 'qty': 1}], '_apo_last_sync': 0,
            'load_manual_allocations_from_s3': bump('manual'), '_manual_alloc_lock': threading.Lock(),
            '_manual_allocations': [{'sku': 'ZZAAAA001SLS', 'qty': 1}],
            'load_allocation_from_s3': lambda: [{'sku': 'ZZAAAA001SLS', 'qty': 2}],
            '_fetch_all_open_orders': lambda: ([{'ctrlNo': 1}], True),
            '_all_open_orders_lock': threading.Lock(), '_all_open_orders_cache': {'fetched_at': 1767225600.0},
            '_fetch_fob_customers': lambda: ['ZZFOB'],
            'load_deduction_assignments_from_s3': bump('assign'),
            'load_suppression_overrides_from_s3': bump('suppress'),
            '_deduction_assign_lock': threading.Lock(), '_deduction_assignments': {'ZZAAAA001SLS': 'warehouse'},
            '_suppression_overrides_lock': threading.Lock(), '_suppression_overrides': [' zzb ', 'ZZA', 'zzb'],
            '_PRES_SUPPRESS_SECONDS': 1209600, '_PRES_SUPPRESS_TOL': 0.1, '_HIDDEN_LANDING_WH': {'NJ', 'AE'},
            '_pres_now_et': lambda: datetime(2026, 1, 2, 9, 30, 0),
        }
        self._exec_nodes(self.SA_NAMES, ns)
        return ns, counters, DeferredThread

    def test_pnl_sources_getters(self):
        ns, counters, _t = self._sources_ns(_QueueHttp())
        src = ns['_pnl_sources']()
        self.assertEqual(set(src), set(pnl.SOURCE_KEYS))
        self.assertTrue(all(callable(v) for v in src.values()))
        inv = src['inventory']()
        self.assertEqual(inv, {'items': [{'sku': 'ZZAAAA001SLS'}], 'last_sync': '2026-01-01T00:00:00Z'})
        self.assertIsNot(inv['items'], ns['_inventory']['items'])
        self.assertEqual(src['ledger'](), {'rows': [{'style': 'ZZAAAA001SLS', 'units': 1}],
                                           'last_sync': '2026-01-01T00:00:00Z'})
        apo = src['apo']()
        self.assertEqual(apo['last_sync'], None)
        self.assertIsNot(apo['rows'], ns['_apo_data'])
        man = src['manual_allocations']()
        self.assertEqual(counters['manual'], 1)
        self.assertEqual(man, ns['_manual_allocations'])
        self.assertIsNot(man[0], ns['_manual_allocations'][0])
        self.assertEqual(src['vw_allocations'](), [{'sku': 'ZZAAAA001SLS', 'qty': 2}])
        self.assertEqual(src['open_orders'](), {'orders': [{'ctrlNo': 1}], 'ok': True,
                                                'fetched_at': '2026-01-01T00:00:00Z'})
        self.assertEqual(src['fob_customers'](), ['ZZFOB'])
        ri = src['routing_inputs']()
        self.assertEqual((counters['assign'], counters['suppress']), (1, 1))
        # exactly the routing_inputs contract in pnl_routing.py's docstring
        self.assertEqual(ri, {'suppression_overrides': [' zzb ', 'ZZA', 'zzb'],
                              'deduction_assignments': {'ZZAAAA001SLS': 'warehouse'},
                              'now': '2026-01-02T09:30:00'})
        self.assertIsNot(ri['deduction_assignments'], ns['_deduction_assignments'])
        self.assertIsNot(ri['suppression_overrides'], ns['_suppression_overrides'])
        self.assertEqual(src['today'](), '2026-01-02')
        norm = pnl._normalize_sources({k: fn() for k, fn in src.items() if k != 'sales_analytics'})
        self.assertEqual(norm['today'], '2026-01-02')
        self.assertEqual(pnl._missing_inputs(norm), [])

    def test_sales_analytics_proxy(self):
        http = _QueueHttp()
        ns, _c, thread_cls = self._sources_ns(http)
        sa, cache = ns['_pnl_sales_analytics'], ns['_pnl_sa_cache']
        # upstream building: propagated, and not re-asked inside the poll window
        http.queue = [_FakeResp(200, {'ready': False, 'building': True})]
        self.assertEqual(sa(), {'building': True})
        self.assertEqual(sa(), {'building': True})
        self.assertEqual(len(http.calls), 1)
        self.assertEqual(http.calls[0]['url'], 'https://oo.example.test/api/sales-analytics')
        self.assertEqual(http.calls[0]['headers'], {'X-Api-Key': 'k'})
        self.assertIsNotNone(http.calls[0]['timeout'])
        # ready: cached, every caller gets its own copy
        cache['poll_after'] = 0.0
        payload = {'v': 2, 'ready': True, 'source': {'ingestedAt': '2026-01-01', 'rows': 3},
                   'styleCount': 1, 'fields': ['style'], 'styles': [['ZZAAAA001']]}
        http.queue = [_FakeResp(200, payload)]
        a = sa()
        self.assertEqual(a['styleCount'], 1)
        a['styles'].append('mutated')
        self.assertEqual(len(sa()['styles']), 1)
        self.assertEqual(len(http.calls), 2)
        # stale: served at once, exactly one background refresh
        cache['at'] = 0.0
        self.assertEqual(sa()['styleCount'], 1)
        self.assertEqual(sa()['styleCount'], 1)
        self.assertEqual(len(thread_cls.pending), 1)
        self.assertEqual(len(http.calls), 2)
        http.queue = [_FakeResp(200, dict(payload, styleCount=2))]
        thread_cls.pending.pop().run_now()
        self.assertEqual(sa()['styleCount'], 2)
        self.assertFalse(cache['refreshing'])
        # failure: None, class-only log line, backoff without re-hitting upstream
        cache.update(body=None, at=0.0, fail_until=0.0, poll_after=0.0, refreshing=False)
        http.queue = [ConnectionError(SECRET_EXC)]
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            self.assertIsNone(sa())
        self.assertIn('[PnL] sales-analytics fetch failed: ConnectionError', buf.getvalue())
        self.assertNotIn(SECRET_EXC, buf.getvalue())
        n = len(http.calls)
        self.assertIsNone(sa())
        self.assertEqual(len(http.calls), n)
        for resp in (_FakeResp(503, {}), _FakeResp(200, {'ready': False, 'note': 'x'}), _FakeResp(200, [1])):
            cache['fail_until'] = 0.0
            http.queue = [resp]
            with contextlib.redirect_stdout(io.StringIO()):
                self.assertIsNone(sa())

    def _cors_app(self, env):
        from flask import Flask
        from flask_cors import CORS
        cors = next(n for n in self.body if isinstance(n, ast.Expr) and isinstance(n.value, ast.Call)
                    and getattr(n.value.func, 'id', '') == 'CORS')
        app = Flask('cors_check_' + uuid.uuid4().hex[:8])
        app.add_url_rule('/api/pnl/status', 'st', lambda: 'ok', methods=['GET', 'POST', 'OPTIONS'])
        app.add_url_rule('/api/pnl', 'root', lambda: 'ok', methods=['GET'])
        app.add_url_rule('/other', 'other', lambda: 'ok')
        exec(compile(ast.Module(body=[cors], type_ignores=[]), 'app.py', 'exec'),
             {'CORS': CORS, 'app': app, 'os': types.SimpleNamespace(environ=env)})
        return app.test_client()

    def test_pnl_cors_allows_only_the_inventory_app_origins(self):
        c = self._cors_app({})
        app_origin, evil = 'https://versainventory.netlify.app', 'https://evil.example'
        r = c.get('/api/pnl/status', headers={'Origin': app_origin})
        self.assertEqual(r.headers.get('Access-Control-Allow-Origin'), app_origin)
        self.assertIsNone(r.headers.get('Access-Control-Allow-Credentials'))
        # With one allowed origin flask-cors sends no Vary: Origin; pnl.py adds it to every P&L
        # response (assert_pnl_headers checks it on the real routes).
        # The review server answers /api/pnl itself, so the production default lists no local origin.
        for local in ('http://localhost:8142', 'http://127.0.0.1:8142'):
            for path in ('/api/pnl/status', '/api/pnl'):
                self.assertIsNone(c.get(path, headers={'Origin': local}).headers.get('Access-Control-Allow-Origin'),
                                  local + path)
            pre = c.options('/api/pnl/status', headers={'Origin': local, 'Access-Control-Request-Method': 'GET',
                                                        'Access-Control-Request-Headers': 'authorization'})
            self.assertIsNone(pre.headers.get('Access-Control-Allow-Origin'), local)
        c3 = self._cors_app({'PNL_CORS_ORIGINS': app_origin + ', http://localhost:8142'})
        self.assertEqual(c3.get('/api/pnl/status', headers={'Origin': 'http://localhost:8142'})
                         .headers.get('Access-Control-Allow-Origin'), 'http://localhost:8142')
        pre = c.options('/api/pnl/status', headers={'Origin': app_origin, 'Access-Control-Request-Method': 'POST',
                                                    'Access-Control-Request-Headers': 'authorization, content-type'})
        self.assertEqual(pre.headers.get('Access-Control-Allow-Origin'), app_origin)
        self.assertIn('authorization', pre.headers.get('Access-Control-Allow-Headers', '').lower())
        self.assertIn('POST', pre.headers.get('Access-Control-Allow-Methods', ''))
        self.assertIsNone(pre.headers.get('Access-Control-Allow-Credentials'))
        for path in ('/api/pnl/status', '/api/pnl'):
            r = c.get(path, headers={'Origin': evil})
            self.assertIsNone(r.headers.get('Access-Control-Allow-Origin'), path)
        pre = c.options('/api/pnl/status', headers={'Origin': evil, 'Access-Control-Request-Method': 'GET',
                                                    'Access-Control-Request-Headers': 'authorization'})
        self.assertIsNone(pre.headers.get('Access-Control-Allow-Origin'))
        self.assertIsNone(pre.headers.get('Access-Control-Allow-Headers'))
        self.assertIsNotNone(c.get('/other', headers={'Origin': evil}).headers.get('Access-Control-Allow-Origin'))
        c2 = self._cors_app({'PNL_CORS_ORIGINS': 'https://example.test'})
        self.assertEqual(c2.get('/api/pnl/status', headers={'Origin': 'https://example.test'})
                         .headers.get('Access-Control-Allow-Origin'), 'https://example.test')
        self.assertIsNone(c2.get('/api/pnl/status', headers={'Origin': app_origin})
                          .headers.get('Access-Control-Allow-Origin'))

    def test_pnl_identity_wrapper_ttls(self):
        clock = {'t': 1000.0}
        calls, shared = [], {}
        profiles = {'eyJx.pay.admin': {'role': 'staff', 'is_admin': True, 'email': ADMIN, 'uid': 'u-admin'},
                    'eyJx.pay.gate': {'role': 'staff', 'is_admin': True, 'email': ADMIN2, 'uid': 'u-gate'}}

        def fake_caller_identity(token):             # like app.py _caller_identity: 300 s shared cache
            calls.append(token)
            prof = profiles.get(token)
            if prof:
                shared[token] = (clock['t'] + 300, prof)
            return prof
        ns = {'time': types.SimpleNamespace(time=lambda: clock['t']), 'threading': threading,
              '_pnl_hashlib': hashlib, '_caller_identity': fake_caller_identity, '_identity_cache': shared,
              '_identity_lock': threading.Lock(), '_IDENTITY_TTL': 300}
        self._exec_nodes(['_PNL_IDENTITY_TTL', '_PNL_IDENTITY_FAIL_TTL', '_PNL_IDENTITY_MAX',
                          '_pnl_ident_cache', '_pnl_ident_lock', '_pnl_caller_identity'], ns)
        ci, tok = ns['_pnl_caller_identity'], 'eyJx.pay.admin'
        self.assertEqual(ci(tok)['email'], ADMIN)
        self.assertEqual(len(calls), 1)
        clock['t'] += 59
        ci(tok)
        self.assertEqual(len(calls), 1)                  # a success is reused for 60 s
        clock['t'] += 2
        ci(tok)
        self.assertEqual(len(calls), 2)                  # then re-checked, although the shared cache keeps 300 s
        profiles.pop(tok)                                # is_admin removed or signed out
        clock['t'] += 61
        self.assertIsNone(ci(tok))
        self.assertEqual(len(calls), 3)
        clock['t'] += 29
        self.assertIsNone(ci(tok))
        self.assertEqual(len(calls), 3)                  # a refusal is remembered for 30 s
        clock['t'] += 2
        self.assertIsNone(ci(tok))
        self.assertEqual(len(calls), 4)
        gate = 'eyJx.pay.gate'                           # the global gate fetched it 5 s ago
        shared[gate] = (clock['t'] + 295, profiles[gate])
        self.assertEqual(ci(gate)['uid'], 'u-gate')
        self.assertEqual(len(calls), 4)                  # no second lookup for the same request
        clock['t'] += 56
        ci(gate)
        self.assertEqual(len(calls), 5)                  # 60 s after the gate fetched it
        got = ci(gate)
        got['role'] = 'changed'
        self.assertEqual(ci(gate)['role'], 'staff')      # callers get copies
        self.assertTrue(ns['_pnl_ident_cache'])
        for k in ns['_pnl_ident_cache']:
            self.assertRegex(k, r'^[0-9a-f]{64}$')      # a hash of the token, never the token
        self.assertIsNone(ci(''))
        self.assertIsNone(ci(None))

    def test_requirements_pin_cryptography(self):
        with open(os.path.join(REPO, 'requirements.txt'), encoding='utf-8') as f:
            lines = [ln.strip() for ln in f if ln.strip() and not ln.strip().startswith('#')]
        pins = [ln for ln in lines if re.match(r'(?i)^cryptography\b', ln)]
        self.assertEqual(pins, ['cryptography==' + importlib.metadata.version('cryptography')])


# ─────────────────────────────────────────────────────────────────────────────
class TestSettingsAndParamsContracts(PnlTestCase):

    def test_settings_nulls_destinations_and_canada_rate(self):
        h = self.harness()
        s = {'fx': {'rate': None}, 'destinations': {'ch': 'none', 'AE': 'ca', 'AW': 'ca', 'ZZ': None},
             'tariff': {'caMfnPct': 18.0, 'addonPct': {'ZZ': None, 'YY': 12}, 'mfnPct': {'shirt_cotton': None}},
             'royalty': {'byBrand': {'ZZ': None}}, 'factories': {'ZZ': None, 'YY': {'name': 'Y', 'origin': 'CN'}},
             'freight': {'oceanPerUnit': {'shirt': None}, 'inlandPerUnit': {'knit': None}},
             'deductions': {'byGroup': {'club': None}, 'byCustomer': {'ZZ SHOP': None, 'ZZ2': 1.5}},
             'customerGroups': {'zz shop': 'offprice', 'ZZ2': None}, 'fobCustomers': ['ZZ SHOP', 'zz2']}
        r = h.admin('POST', '/api/pnl/settings', body={'settings': s, 'expected_etag': None})
        self.assertEqual(r.status_code, 200, r.get_data(as_text=True)[:300])
        out = r.get_json()['settings']
        self.assertEqual(out['destinations'], {'CH': 'none', 'AE': 'ca', 'AW': 'ca', 'ZZ': None})
        self.assertEqual(out['tariff'], {'caMfnPct': 18.0, 'addonPct': {'ZZ': None, 'YY': 12},
                                         'mfnPct': {'shirt_cotton': None}})
        self.assertEqual((out['royalty']['byBrand'], out['factories']['ZZ']), ({'ZZ': None}, None))
        self.assertEqual(out['freight'], {'oceanPerUnit': {'shirt': None}, 'inlandPerUnit': {'knit': None}})
        self.assertEqual(out['deductions'], {'byGroup': {'club': None}, 'byCustomer': {'ZZ SHOP': None, 'ZZ2': 1.5}})
        self.assertEqual(out['customerGroups'], {'ZZ SHOP': 'offprice', 'ZZ2': None})
        self.assertEqual(out['fobCustomers'], ['ZZ SHOP', 'ZZ2'])
        etag = r.get_json()['etag']
        bad = [({'destinations': {'ZZ': 'mars'}}, 'destinations.ZZ', 'invalid_choice'),
               ({'destinations': {'bad code!': 'us'}}, 'destinations.bad?code?', 'invalid_code'),
               ({'tariff': {'caMfnPct': 150}}, 'tariff.caMfnPct', 'out_of_range'),
               ({'tariff': {'caMfnPct': None}}, 'tariff.caMfnPct', 'not_a_finite_number'),
               ({'fx': {'rate': 0.67}}, 'fx.rate', 'out_of_range'),
               ({'fx': {'rate': 4.99}}, 'fx.rate', 'out_of_range'),
               ({'fx': {'rate': 16.7}}, 'fx.rate', 'out_of_range'),
               ({'fx': {'rate': 50}}, 'fx.rate', 'out_of_range'),
               ({'royalty': {'defaultPct': None}}, 'royalty.defaultPct', 'not_a_finite_number'),
               ({'customerGroups': {'ZZ.SHOP': 'offprice'}}, 'customerGroups.ZZ.SHOP', 'invalid_custkey'),
               ({'tariff': {'addonPct': None}}, 'tariff.addonPct', 'not_an_object'),
               ({'opex': {'items': [{'name': 'Rent', 'monthly': 10 ** 400}]}}, 'opex.items[0].monthly', 'out_of_range')]
        for settings, path, issue in bad:
            b = self.assert_error(h.admin('POST', '/api/pnl/settings',
                                          body={'settings': settings, 'expected_etag': etag}),
                                  422, 'INVALID_SETTINGS', path)
            self.assertIn({'path': path, 'issue': issue}, b['problems'])
        for rate in (5, SYNTH_RATE, 10, None):
            r = h.admin('POST', '/api/pnl/settings', body={'settings': {'fx': {'rate': rate}}, 'expected_etag': etag})
            self.assertEqual(r.status_code, 200, rate)
            etag = r.get_json()['etag']

    def test_overrides_huge_integer_is_422(self):
        h = self.harness()
        o = {'id': 'o1', 'scope': 'style', 'key': {'style': 'ZZAAAA001SLS'}, 'fobU': 10 ** 400}
        b = self.assert_error(h.admin('POST', '/api/pnl/overrides', body={'overrides': [o]}), 422, 'INVALID_OVERRIDES')
        self.assertIn({'path': 'overrides[0].fobU', 'issue': 'out_of_range'}, b['problems'])

    def test_restore_of_a_version_with_bad_params_is_422(self):
        h = self.harness()
        h.upload()
        time.sleep(0.005)
        bad = synthetic_costbook(1)
        bad['params']['gridPrecedence'] = 'abc'
        h.store.put_obj(pnl.COSTBOOK, bad)              # an old cost book stored before the check existed
        time.sleep(0.005)
        h.upload()
        vs = h.admin('GET', '/api/pnl/costbook/versions').get_json()['versions']
        self.assertEqual(len(vs), 3)
        etag = h.admin('GET', '/api/pnl/costbook').get_json()['etag']
        b = self.assert_error(h.admin('POST', '/api/pnl/costbook/restore', body={'version_id': vs[1]['id']}),
                              422, 'INVALID_COSTBOOK')
        self.assertIn({'path': 'params.gridPrecedence', 'issue': 'not_an_object'}, b['problems'])
        self.assertEqual(h.admin('GET', '/api/pnl/costbook').get_json()['etag'], etag)   # nothing restored

    def test_param_changes_are_named_never_valued(self):
        h = self.harness()
        h.upload()
        cb = synthetic_costbook(3)
        cb['params']['fitPremium'] = {'regular': 0.4441, 'bigTall': 0.3331}
        cb['params']['programMap']['ZZ-PROGRAM2'] = dict(cb['params']['programMap']['ZZ-PROGRAM1'])
        cb['params']['kitPcs'] = {'ZZKIT0002': 24}
        r = h.admin('POST', '/api/pnl/costbook', body={'costbook': cb})
        self.assertEqual(r.status_code, 200)
        out = r.get_json()
        self.assertEqual(sorted(out['paramChanges'], key=lambda p: p['path']), [
            {'path': 'params.fitPremium.regular', 'issue': 'changed'},
            {'path': 'params.kitPcs.ZZKIT0001', 'issue': 'removed'},
            {'path': 'params.kitPcs.ZZKIT0002', 'issue': 'added'},
            {'path': 'params.programMap.ZZ-PROGRAM2', 'issue': 'added'}])
        self.assertEqual((out['paramChangeCount'], out['warningCount']), (4, 4))
        self.assertEqual(out['warnings'], out['paramChanges'])
        text = r.get_data(as_text=True)
        for leak in ('0.4441', '0.2229', '0.3331', SENTINEL_TEXT):
            self.assertNotIn(leak, text)
        self.assertIn('[PnL] costbook saved records=3 params_changed=4 user=%s' % ADMIN, self._out.getvalue())
        same = h.admin('POST', '/api/pnl/costbook', body={'costbook': cb}).get_json()
        self.assertEqual(same['paramChangeCount'], 0)

    def test_price_field_roles(self):
        h = self.harness()
        legacy = synthetic_costbook(1)
        legacy['meta']['priceFields'] = {'price_usd': 'x', 'price_usd_v2old': 'y'}
        r = h.admin('POST', '/api/pnl/costbook', body={'costbook': legacy})
        self.assertIn({'path': 'meta.priceFields', 'issue': 'legacy_field_names'}, r.get_json()['warnings'])
        neutral = synthetic_costbook(1)
        neutral['meta']['priceFieldRoles'] = {'usd': 'price_usd', 'base': 'price_usd_base', 'cut': 'price_usd_cut',
                                              'rmb': 'rmb_price'}
        neutral['records'][0]['price_usd_base'] = SENTINEL_COST
        r = h.admin('POST', '/api/pnl/costbook', body={'costbook': neutral})
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.get_json()['warnings'], [])
        bad = synthetic_costbook(1)
        bad['meta']['priceFieldRoles'] = {'usd': 5}
        b = self.assert_error(h.admin('POST', '/api/pnl/costbook', body={'costbook': bad}), 422, 'INVALID_COSTBOOK')
        self.assertIn({'path': 'meta.priceFieldRoles', 'issue': 'invalid'}, b['problems'])
        big = synthetic_costbook(1)
        big['records'][0]['price_usd_cut'] = 5000
        b = self.assert_error(h.admin('POST', '/api/pnl/costbook', body={'costbook': big}), 422, 'INVALID_COSTBOOK')
        self.assertIn({'path': 'records[0].price_usd_cut', 'issue': 'out_of_range'}, b['problems'])

    def test_validate_params_directly(self):
        clean, problems, dropped = pnl.validate_params(synthetic_params(fiberByStyle={'zzaaaa001sls': 'cotton'},
                                                                        rmbBase=SYNTH_RATE, cutFactor=0.8887,
                                                                        fiberByFabric={'zz': 'linen'}, extra=1))
        self.assertEqual(problems, [])
        self.assertEqual(dropped, ['params.extra'])
        self.assertEqual(clean['fiberByStyle'], {'ZZAAAA001SLS': 'cotton'})
        self.assertEqual(clean['fiberByFabric'], {'ZZ': 'linen'})
        self.assertEqual(set(pnl.PARAMS_REQUIRED) - set(clean), set())
        for k in pnl.PARAMS_REQUIRED:
            p = synthetic_params()
            del p[k]
            self.assertIn({'path': 'params.' + k, 'issue': 'missing'}, pnl.validate_params(p)[1])


def alloc_engine(alloc_rows, alerts=None):
    eng = make_engine()
    base = eng.build_dataset

    def build_dataset(*a, **k):
        ds = base(*a, **k)
        ds['alloc'] = {'fields': ['line', 'units', 'kind'], 'rows': alloc_rows}
        if alerts is not None:
            ds['alerts'] = list(alerts)
        return ds
    eng.build_dataset = build_dataset
    return eng


ITEMS = [{'sku': 'ZZAAAA%03dSLS' % i, 'jtw': 1, 'incoming': 100} for i in range(10)]        # ATS incoming 1000
LEDGER = [{'production': 'ZZ%05d' % i, 'style': 'ZZAAAA%03dSLS' % i, 'units': 100} for i in range(10)]


def ledger_sources(rows, items=ITEMS):
    s = make_sources()
    s['inventory'] = lambda: {'items': [dict(i) for i in items], 'last_sync': '2026-01-01T00:00:00Z'}
    s['ledger'] = lambda: {'rows': [dict(r) for r in rows], 'last_sync': '2026-01-01T00:00:00Z'}
    return s


class TestLedgerGate(PnlTestCase):

    def test_healthy_ledger_builds_and_reports_counts(self):
        h = self.harness(sources=ledger_sources(LEDGER))
        h.upload()
        ds = h.dataset().get_json()
        self.assertEqual(ds['inputs']['ledger_check'], {'rows': 10, 'usableRows': 10, 'usableUnits': 1000,
                                                        'atsIncoming': 1000, 'unitsVsAts': 1.0})
        self.assertNotIn('alerts', ds)

    def test_unusable_ledgers_abort_before_the_engine(self):
        cases = {
            'units_zero': [dict(r, units=0) for r in LEDGER],
            'units_text': [dict(r, units='n/a') for r in LEDGER],
            'styles_not_in_feed': [dict(r, style='QQ' + r['style'][2:]) for r in LEDGER],
            'column_shift': [dict(r, style='PO TEXT %d' % i, units=r['production']) for i, r in enumerate(LEDGER)],
            'one_row': LEDGER[:1],
            'wrong_file': [{'production': 'QQ00001', 'style': 'QQOTHER%03d' % i, 'units': 50} for i in range(40)],
        }
        for name, rows in cases.items():
            h = self.harness(sources=ledger_sources(rows))
            h.upload()
            body = self.assert_error(h.dataset(), 503, 'INPUTS_UNAVAILABLE', name, reason='ledger_unusable')
            self.assertEqual(body['missing'], ['ledger'], name)
            self.assertEqual(h.engine.calls, [], name)
            build = h.admin('GET', '/api/pnl/status').get_json()['build']
            self.assertEqual((build['state'], build['error']['code'], build['error']['reason']),
                             ('error', 'INPUTS_UNAVAILABLE', 'ledger_unusable'), name)
        self.assertIn('[PnL] build failed code=INPUTS_UNAVAILABLE class=LedgerUnusable missing=ledger '
                      'reason=ledger_unusable', self._out.getvalue())

    def test_partial_ledger_builds_with_an_alert(self):
        h = self.harness(sources=ledger_sources(LEDGER[:7]))
        h.upload()
        ds = h.dataset().get_json()
        self.assertEqual(ds['inputs']['ledger_check']['unitsVsAts'], 0.7)
        al = {a['id']: a for a in ds['alerts']}['al_ledger_partial']
        self.assertEqual((al['severity'], al['kind'], al['unit'], al['count'], al['value'], al['valueLabel']),
                         ('medium', 'stale_input', 'refs', 7, None, None))
        self.assertIn('70 percent', al['detail'])
        for text in (al['title'], al['detail']):
            self.assertNotIn('—', text)
            self.assertNotIn('–', text)

    def test_ats_without_incoming_skips_the_unit_share(self):
        h = self.harness(sources=ledger_sources(LEDGER[:1], [dict(i, incoming=0) for i in ITEMS]))
        h.upload()
        ds = h.dataset().get_json()
        self.assertIsNone(ds['inputs']['ledger_check']['unitsVsAts'])
        self.assertNotIn('alerts', ds)

    def test_no_production_placement_raises_a_high_alert(self):
        engine_alert = {'id': 'al_engine', 'kind': 'late', 'severity': 'medium', 'title': 'x', 'detail': 'y'}
        h = self.harness(sources=ledger_sources(LEDGER),
                         engine=alloc_engine([['L1', 5, 'warehouse'], ['L2', 3, 'unsourced']], [engine_alert]))
        h.upload()
        ds = h.dataset().get_json()
        self.assertEqual([a['id'] for a in ds['alerts']], ['al_engine', 'al_ledger_no_production'])
        al = ds['alerts'][1]
        self.assertEqual((al['severity'], al['kind'], al['unit'], al['count'], al['value'], al['valueLabel']),
                         ('high', 'stale_input', 'refs', 10, None, None))
        for rows in ([['L1', 5, 'warehouse'], ['L2', 3, 'production'], ['L3', 1, 'unsourced']],
                     [['L1', 5, 'warehouse']]):          # production used, or nothing went unsourced
            h2 = self.harness(sources=ledger_sources(LEDGER), engine=alloc_engine(rows))
            h2.upload()
            self.assertNotIn('alerts', h2.dataset().get_json(), rows)


class TestBuildStateAndTransport(PnlTestCase):

    def _state(self, h):
        return h.admin('GET', '/api/pnl/status').get_json()['build']

    def test_state_follows_the_current_objects(self):
        state = {}
        h = self.harness(sources=make_sources(state=state))
        h.upload()
        self.assertEqual(h.dataset().status_code, 200)
        self.assertEqual(self._state(h)['state'], 'ready')
        r = h.admin('POST', '/api/pnl/settings', body={'settings': {}, 'expected_etag': None})
        self.assertEqual(r.status_code, 200)
        self.assertEqual(self._state(h)['state'], 'stale')      # built for older objects
        state['orders'] = []
        body = self.assert_error(h.dataset(), 503, 'INPUTS_UNAVAILABLE')
        self.assertEqual(body['missing'], ['open_orders'])
        build = self._state(h)
        self.assertEqual((build['state'], build['error']['code']), ('error', 'INPUTS_UNAVAILABLE'))
        self.assertTrue(build['builtAt'])                        # the older copy still exists

    def test_state_stale_when_a_recheck_fails(self):
        state = {}
        h = self.harness(sources=make_sources(state=state))
        h.upload()
        self.assertEqual(h.dataset().status_code, 200)
        h.svc.recheck_seconds = 0
        state['orders'] = []
        self.assertEqual(h.dataset().status_code, 200)          # the last good copy is served
        h.wait_idle()
        self.assertEqual(self._state(h)['state'], 'stale')

    def test_dataset_is_gzip_encoded_when_accepted(self):
        h = self.harness()
        h.upload()
        plain = h.dataset()
        self.assertEqual(plain.status_code, 200)
        self.assertIsNone(plain.headers.get('Content-Encoding'))
        self.assertIn('Accept-Encoding', plain.headers.get('Vary', ''))
        r = h.admin('GET', '/api/pnl/dataset', headers={'Accept-Encoding': 'gzip, deflate, br'})
        self.assertEqual(r.headers.get('Content-Encoding'), 'gzip')
        for v in ('Accept-Encoding', 'Authorization'):
            self.assertIn(v, r.headers.get('Vary', ''))
        self.assert_pnl_headers(r)
        self.assertEqual(gzip.decompress(r.get_data()), plain.get_data())
        self.assertLess(len(r.get_data()), len(plain.get_data()) + 64)
        r0 = h.admin('GET', '/api/pnl/dataset', headers={'Accept-Encoding': 'gzip;q=0, identity'})
        self.assertIsNone(r0.headers.get('Content-Encoding'))
        self.assertEqual(r0.get_data(), plain.get_data())


# ─────────────────────────────────────────────────────────────────────────────
# Contract C14: the '@admins' allowlist entry.
class TestAdminsAllowlistC14(PnlTestCase):
    LOOKALIKE = 'eyJx.pay.lookalike'           # an admin whose email merely contains '@admins'

    def setUp(self):
        super().setUp()
        IDENTITIES[self.LOOKALIKE] = {'role': 'staff', 'is_admin': True, 'email': 'x@admins.com', 'uid': 'u-look'}
        self.addCleanup(IDENTITIES.pop, self.LOOKALIKE, None)

    def status(self, h, token=None, headers=None):
        return h.req('GET', '/api/pnl/status', token=token, headers=headers)

    def test_admins_entry_admits_every_admin(self):
        for allow in ('@admins', ' @ADMINS ', 'someone@example.test, @Admins', ',@admins,'):
            h = self.harness(allow=allow)
            for token in (T_ADMIN, 'eyJx.pay.admin2', 'eyJx.pay.admincase', self.LOOKALIKE):
                self.assertEqual(self.status(h, token).status_code, 200, '%r %s' % (allow, token))
            for name, token in NON_ADMIN:
                self.assert_error(self.status(h, token), 403, 'ADMIN_ONLY', '%r %s' % (allow, name))
            self.assert_error(self.status(h, 'eyJx.pay.noemail'), 403, 'ADMIN_ONLY', 'an admin with no email')
            for name, headers in DENY_401:
                self.assert_error(self.status(h, headers=headers), 401, 'AUTH_REQUIRED', '%r %s' % (allow, name))
        h = self.harness(allow='@admins')
        for path, method, body in ROUTE_CALLS:               # an admin who is not listed by email
            r = h.req(method, path, token='eyJx.pay.admin2', body=body)
            self.assertNotIn(r.status_code, (401, 403), '%s %s' % (method, path))
            self.assertLess(r.status_code, 500, '%s %s: %s' % (method, path, r.get_data(as_text=True)[:200]))
            self.assert_pnl_headers(r, path)
        self.assertIn('allowlist all admins', self._out.getvalue())

    def test_lookalikes_are_ordinary_entries(self):
        for allow in ('x@admins.com', '@admins.com', 'admins', '@admins;x', 'admins@', '"@admins"'):
            h = self.harness(allow=allow)
            for token in (T_ADMIN, 'eyJx.pay.admin2'):
                self.assert_error(self.status(h, token), 403, 'ADMIN_ONLY', '%r %s' % (allow, token))
        self.assertEqual(self.status(self.harness(allow='x@admins.com'), self.LOOKALIKE).status_code, 200)
        self.assert_error(self.status(self.harness(allow='@admins.com'), self.LOOKALIKE), 403, 'ADMIN_ONLY', 'suffix')

    def test_still_mandatory_and_the_machine_key_never_opens_it(self):
        for allow in (None, '', ' , ,'):
            h = self.harness(allow=allow)
            self.assert_error(self.status(h, T_ADMIN), 503, 'PNL_NOT_CONFIGURED', repr(allow), reason='allowlist')
        h = self.harness(allow='@admins')
        for headers in ({'X-Api-Key': MACHINE_KEY}, {'X-Api-Key': MACHINE_KEY, 'Authorization': 'Bearer ' + T_ADMIN}):
            self.assert_error(self.status(h, headers=headers), 401, 'AUTH_REQUIRED', 'machine key')
        self.assertEqual(h.backend.puts, [])
        off = self.harness(allow='@admins', enabled='off')
        self.assert_error(self.status(off, T_ADMIN), 503, 'PNL_NOT_CONFIGURED', 'disabled', reason='disabled')


# Contracts C11 to C14: the new settings blocks. Sentinel values only.
class TestSettingsC11ToC14(PnlTestCase):
    GOOD = {'landed': {'mode': 'multiplier', 'multiplier': {'natural': 1.37, 'synthetic': 1.53}},
            'royalty': {'defaultPct': 6.5, 'base': 'revenue'},
            'revenueCosts': {'items': [{'key': 'warehouse', 'name': 'Warehouse', 'pct': 2.25},
                                       {'key': 'factoring', 'name': 'Factoring', 'pct': 1.75},
                                       {'key': 'rent', 'name': 'Rent', 'pct': 0.8}]},
            'regimeByCustomer': {'zzpeer': 'us', 'ZZ SHOP': 'none', 'YY': None},
            'costRule': {'mode': 'cascade', 'wideSpreadPct': 12.5},
            'history': {'caveat': 'Synthetic caveat.'},
            'confirmed': {'landed': True, 'revenueCosts': False},
            'opex': {'items': [{'name': 'Payroll', 'monthly': 111111}]}}

    def test_new_blocks_round_trip(self):
        h = self.harness()
        r = h.admin('POST', '/api/pnl/settings', body={'settings': self.GOOD, 'expected_etag': None})
        self.assertEqual(r.status_code, 200, r.get_data(as_text=True)[:300])
        out = r.get_json()
        s = out['settings']
        for k in ('landed', 'revenueCosts', 'costRule', 'history', 'confirmed', 'opex'):
            self.assertEqual(s[k], self.GOOD[k], k)
        self.assertEqual(s['royalty'], {'defaultPct': 6.5, 'base': 'revenue'})
        self.assertEqual(s['regimeByCustomer'], {'ZZPEER': 'us', 'ZZ SHOP': 'none', 'YY': None})
        self.assertEqual(out['dropped'], [])
        self.assertEqual(h.admin('GET', '/api/pnl/settings').get_json()['settings'], s)
        unset = {'landed': {'mode': 'multiplier', 'multiplier': {'natural': None, 'synthetic': 1.53}},
                 'history': {'caveat': None}, 'revenueCosts': {'items': []}}
        r = h.admin('POST', '/api/pnl/settings', body={'settings': unset, 'expected_etag': out['etag']})
        self.assertEqual(r.status_code, 200, r.get_data(as_text=True)[:300])  # an empty multiplier is allowed
        self.assertIsNone(r.get_json()['settings']['landed']['multiplier']['natural'])

    def test_bad_values_are_refused(self):
        h = self.harness()
        etag = h.admin('POST', '/api/pnl/settings', body={'settings': {}, 'expected_etag': None}).get_json()['etag']
        item = {'key': 'rent', 'name': 'Rent', 'pct': 0.8}
        bad = [({'landed': {'mode': 'factor'}}, 'landed.mode', 'invalid_choice'),
               ({'landed': {'multiplier': {'natural': 0.99}}}, 'landed.multiplier.natural', 'out_of_range'),
               ({'landed': {'multiplier': {'synthetic': 3.01}}}, 'landed.multiplier.synthetic', 'out_of_range'),
               ({'landed': {'multiplier': {'natural': '1.37'}}}, 'landed.multiplier.natural', 'not_a_finite_number'),
               ({'landed': {'multiplier': 1.37}}, 'landed.multiplier', 'not_an_object'),
               ({'royalty': {'base': 'gross'}}, 'royalty.base', 'invalid_choice'),
               ({'royalty': {'base': None}}, 'royalty.base', 'invalid_choice'),
               ({'revenueCosts': {'items': [dict(item, pct=50.01)]}}, 'revenueCosts.items[0].pct', 'out_of_range'),
               ({'revenueCosts': {'items': [dict(item, pct=-0.8)]}}, 'revenueCosts.items[0].pct', 'out_of_range'),
               ({'revenueCosts': {'items': [dict(item, pct=True)]}}, 'revenueCosts.items[0].pct', 'not_a_finite_number'),
               ({'revenueCosts': {'items': [{'key': 'rent', 'name': 'Rent'}]}}, 'revenueCosts.items[0].pct', 'missing'),
               ({'revenueCosts': {'items': [{'name': 'Rent', 'pct': 0.8}]}}, 'revenueCosts.items[0].key', 'missing'),
               ({'revenueCosts': {'items': [item, dict(item, pct=2.25)]}}, 'revenueCosts.items[1].key', 'duplicate'),
               ({'revenueCosts': {'items': [dict(item, key='bad key!')]}}, 'revenueCosts.items[0].key', 'invalid_token'),
               ({'revenueCosts': {'items': 'rent'}}, 'revenueCosts.items', 'not_a_list'),
               # A list over its cap is a list: the page words 'too_long' as "is too long".
               ({'revenueCosts': {'items': [dict(item, key='k%d' % i) for i in range(51)]}}, 'revenueCosts.items', 'too_long'),
               ({'opex': {'items': [{'name': 'Payroll', 'monthly': 111111}] * 51}}, 'opex.items', 'too_long'),
               ({'regimeByCustomer': {'ZZPEER': 'mars'}}, 'regimeByCustomer.ZZPEER', 'invalid_choice'),
               ({'regimeByCustomer': {'ZZ.SHOP': 'us'}}, 'regimeByCustomer.ZZ.SHOP', 'invalid_custkey'),
               ({'costRule': {'mode': 'median'}}, 'costRule.mode', 'invalid_choice'),
               ({'costRule': {'wideSpreadPct': 101}}, 'costRule.wideSpreadPct', 'out_of_range'),
               ({'costRule': {'wideSpreadPct': -1}}, 'costRule.wideSpreadPct', 'out_of_range'),
               ({'history': {'caveat': 5}}, 'history.caveat', 'invalid_text'),
               ({'history': {'caveat': 'x' * 501}}, 'history.caveat', 'invalid_text'),
               ({'confirmed': {'landed': 'yes'}}, 'confirmed.landed', 'not_a_boolean')]
        for settings, path, issue in bad:
            b = self.assert_error(h.admin('POST', '/api/pnl/settings', body={'settings': settings, 'expected_etag': etag}),
                                  422, 'INVALID_SETTINGS', path)
            self.assertIn({'path': path, 'issue': issue}, b['problems'])
        self.assertEqual(h.admin('GET', '/api/pnl/settings').get_json()['etag'], etag)    # nothing saved
        text = h.admin('GET', '/api/pnl/settings').get_data(as_text=True)
        self.assertNotIn('50.01', text)


if __name__ == '__main__':
    unittest.main()
