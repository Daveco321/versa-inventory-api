"""
pnl.py: Profit & Loss routes for the Versa inventory platform. ADMIN ONLY.

CONFIDENTIAL. Factory costs and margins are the most sensitive data in the
company. This module never logs values, never returns exception text, never
writes into shared app structures and registers nothing with the AI agent or
MCP. Stored objects are encrypted by pnl_store.EncryptedStore.

Wire-up (bottom of app.py, after the scorecard):
    from pnl import register_pnl_routes
    from pnl_store import EncryptedStore, S3Store
    register_pnl_routes(app,
        store=EncryptedStore(S3Store(get_s3, bucket), os.environ.get('PNL_DATA_KEYS', '')),
        caller_identity=_pnl_caller_identity, sources=_pnl_sources())

register_pnl_routes(app, *, store, caller_identity, sources, env=os.environ, engine=None, routing=None,
                    store_check=True)
    store            pnl_store.EncryptedStore, or None when not configured.
    caller_identity  token -> {'role', 'is_admin', 'email', 'uid'} | None.
    sources          dict of zero-argument callables (see SOURCES below).
    env              mapping read on every request: PNL_ALLOWED_EMAILS, PNL_ENABLED.
    engine, routing  modules; default import pnl_engine / pnl_routing lazily. If that
                     fails, boot still succeeds and dataset routes answer 503.
    store_check      a few seconds after boot, one background check logs whether the
                     configured keys open the saved objects (codes and key ids only).
    Returns the service object (diagnostics and tests).

ACCESS (inline on every route, independent of the global gate and AUTH_MODE)
    1. Authorization: Bearer <token>. The token must be ASCII, start with 'eyJ',
       contain exactly two dots and be under 4096 chars. Otherwise 401 AUTH_REQUIRED.
    2. Any X-Api-Key header is refused with 401. The machine key never opens the P&L.
    3. caller_identity(token) None (or an error) gives 401 AUTH_REQUIRED.
    4. role != 'staff' or is_admin is not True gives 403 ADMIN_ONLY.
    5. PNL_ALLOWED_EMAILS (comma separated, case-insensitive) is mandatory. Empty gives
       503 PNL_NOT_CONFIGURED reason 'allowlist'. An admin not listed gets 403 ADMIN_ONLY.
       The entry '@admins' (the whole entry after trimming, any case, alone or in the list)
       admits every caller who passed step 4 and has an email (contract C14). Listed emails
       keep working. An email that only contains it (x@admins.com) is an ordinary entry, and
       the machine key never gets in (step 2).
    6. PNL_ENABLED=off gives 503 PNL_NOT_CONFIGURED reason 'disabled'.
    OPTIONS answers 204 with no body and no auth (CORS preflight).

ROUTES (GET, POST, OPTIONS only; every response carries Cache-Control no-store)
    GET  /api/pnl/status              {configured, reasons[], you:{email}, versions, keys:{count, ids},
                                       objects:{costbook:{present, etag, readable, kid, keyKnown,
                                                          generatedAt, records},
                                                settings:{present, etag, readable, kid, keyKnown},
                                                overrides:{present, etag, readable, kid, keyKnown, count}},
                                       build:{state, startedAt, builtAt, error}}   no money
        readable: true | false | null (absent or unknown). A present object the keys cannot open
        carries problem 'keys_mismatch' or 'damaged', and reasons gets 'store:<problem>'.
        kid: the non-secret id of the key that wrote the object; keys.ids: the configured ids.
        build.state: idle | building | ready | stale (an older copy is served, or the objects
        changed since it was built) | error (the current objects and inputs cannot build).
    GET  /api/pnl/dataset[?refresh=1] 200 dataset | 202 {building:true, startedAt} | 409 NO_COSTBOOK
                                      | 503 | 500. Poll every 3 s while 202. After a Refresh click keep
                                      refresh=1 on the polls until a 200 arrives. Served gzip encoded
                                      when the request accepts gzip (Vary: Accept-Encoding).
    GET  /api/pnl/costbook            {costbook: obj|null, etag}
    POST /api/pnl/costbook            body {costbook, expected_etag?, replace_unreadable?} or a bare
                                      cost book (then ?expected_etag= and ?replace_unreadable=1).
                                      12 MB cap. -> {etag, records, warnings[], warningCount, dropped[],
                                      paramChanges[{path, issue}], paramChangeCount} | 409 | 413 | 422
    GET  /api/pnl/costbook/versions   {supported, versions:[{id, at, size, current}]}
    POST /api/pnl/costbook/restore    {version_id, expected_etag?} -> {etag, records, paramChanges,
                                      paramChangeCount}
    GET  /api/pnl/settings            {settings: obj|null, defaults: obj|null, etag}
    POST /api/pnl/settings            {settings, expected_etag, replace_unreadable?} -> {etag, settings, dropped[]}
    GET  /api/pnl/overrides           {overrides: [], etag}
    POST /api/pnl/overrides           {overrides, expected_etag, replace_unreadable?} -> {etag, count}
    POST /api/pnl/audit               {action: 'export' | 'print', detail} -> 204 (one log line;
                                      only the detail keys tab, drill, file, sheets, rows, count,
                                      n and whatIf are logged). Any other action gives 400.
    POST /api/pnl/rotate              re-encrypt all objects under the newest key -> {objects, keys}
    expected_etag: key absent = no check; null or '' = the object must not exist yet;
    a string = must equal the stored etag. A mismatch gives 409 VERSION_CONFLICT.
    replace_unreadable: true writes over an object the server's keys cannot open (a truly
    lost key only; see the pnl_store.py KEY RUNBOOK). Logged. Without it such a write gets
    500 PNL_STORE_UNREADABLE reason 'current_unreadable' and nothing is written. While any
    saved object needs a key this server does not have (status store:keys_mismatch), creating
    an ABSENT object is refused too (cost book, settings, overrides, restore): 500
    PNL_STORE_UNREADABLE reason 'keys_mismatch', nothing written, so the store never ends up
    under two keys. replace_unreadable is the only bypass, and it is logged.

ERRORS  {error: <friendly message>, code} plus only codes, counts or field paths:
    400 BAD_REQUEST, 401 AUTH_REQUIRED, 403 ADMIN_ONLY, 404 NOT_FOUND, 409 NO_COSTBOOK,
    409 VERSION_CONFLICT, 413 TOO_LARGE, 422 INVALID_COSTBOOK | INVALID_SETTINGS |
    INVALID_OVERRIDES {problems, problemCount}, 500 PNL_STORE_UNREADABLE {reason:
    keys_mismatch|damaged|current_unreadable}, 500 BUILD_FAILED, 500 INTERNAL {ref},
    503 PNL_NOT_CONFIGURED {reason: allowlist|disabled|store|engine|routing},
    503 PNL_STORE_UNAVAILABLE, 503 INPUTS_UNAVAILABLE {missing: [ledger|inventory|open_orders],
    reason?: ledger_unusable}. The error text follows the reason when one is given.

SOURCES (DESIGN section 3.4; called only on the build thread)
    inventory          -> {'items': [...], 'last_sync': str}
    ledger             -> {'rows': [...], 'last_sync': str}      rows == [] means UNAVAILABLE
    apo                -> {'rows': [...], 'last_sync': str}
    manual_allocations -> [...]
    vw_allocations     -> [...]
    open_orders        -> {'orders': [...], 'ok': bool, 'fetched_at': str}
    fob_customers      -> [codes]
    routing_inputs     -> {...}  (pnl_routing defines it; app.py documents its shape)
    sales_analytics    -> payload | {'building': True} | None
    today              -> 'YYYY-MM-DD' (US Eastern business date)
    The engine receives `src`: the same keys with the getter RESULTS, normalized to the
    shapes above. Rows are shallow copies and the analytics payload a deep copy, so the
    engine can never touch app state. Missing ledger, inventory or open orders abort the
    build with 503 INPUTS_UNAVAILABLE instead of showing wrong numbers. So does a ledger
    that is present but unusable (reason ledger_unusable): under half its rows name an ATS
    SKU with units, or its usable units are under half of the ATS feed's incoming units.

BUILD  engine.build_dataset(src, costbook, settings, overrides, now_iso, routing) runs in one
    background thread per process. Memo key: the three object etags plus digests of every
    input. A request waits up to 2.5 s, then answers 202. A served memo is re-checked in the
    background every 180 s. pnl.py fills dataset.inputs.{costbook,settings,overrides}_etag and
    inputs.ledger_check (row and unit counts), and adds two alerts of its own: 'Style ledger
    looks incomplete' (medium) and 'No order is placed on production' (high).
    Build errors are kept as {code, class} and shown generically.

LOGS   [PnL] <METHOD> <path> <status> <ms>ms user=<email>. Never values or bodies.
"""

import copy
import functools
import gzip
import hashlib
import importlib
import json
import math
import os
import re
import secrets
import threading
import time
from datetime import date, datetime, timedelta, timezone

from flask import Response, g, request
from werkzeug.exceptions import HTTPException, RequestEntityTooLarge

from pnl_store import (StoreError, StoreNotConfigured, StoreNotFound, StoreUnavailable,
                       StoreUnreadable, VersionConflict)

__all__ = ['register_pnl_routes', 'ROUTE_TABLE', 'MESSAGES', 'REASON_MESSAGES', 'message_for',
           'SOURCE_KEYS', 'COSTBOOK', 'SETTINGS', 'OVERRIDES', 'PARAMS_SCHEMA', 'PARAMS_REQUIRED',
           'SETTINGS_SCHEMA', 'validate_costbook', 'validate_params', 'validate_settings',
           'validate_overrides']

COSTBOOK = 'costbook.v1.bin'
SETTINGS = 'settings.v1.bin'
OVERRIDES = 'overrides.v1.bin'
OBJECTS = (COSTBOOK, SETTINGS, OVERRIDES)
_LABELS = ((COSTBOOK, 'costbook'), (SETTINGS, 'settings'), (OVERRIDES, 'overrides'))

SOURCE_KEYS = ('inventory', 'ledger', 'apo', 'manual_allocations', 'vw_allocations',
               'open_orders', 'fob_customers', 'routing_inputs', 'sales_analytics', 'today')

MAX_COSTBOOK_BYTES = 12 * 1024 * 1024
MAX_SETTINGS_BYTES = 1 * 1024 * 1024
MAX_OVERRIDES_BYTES = 4 * 1024 * 1024
MAX_SMALL_BYTES = 64 * 1024
MAX_TOKEN_CHARS = 4096

WAIT_SECONDS = 2.5          # a dataset request waits this long for a build, then answers 202
REFRESH_WINDOW = 30         # ?refresh=1 forces a rebuild at most once per 30 s per process
RECHECK_SECONDS = 180       # a served memo re-checks its inputs in the background this often
ERROR_RETRY_SECONDS = 30    # after a failed build, retry on the next request after this long
MODULE_RETRY_SECONDS = 60   # retry a failed engine/routing import after this long
STORE_CHECK_DELAY = 5       # the boot-time key check runs this long after registration

# Ledger sanity gates (row and unit shares, not cost data). Measured on the Sep 14 2026 feed:
# a healthy ledger names ATS SKUs with units on almost every row, and its units equal the ATS
# feed's incoming units (share 1.0). A column shift, a wrong sheet or a truncated file drops
# both towards 0.
LEDGER_MIN_USABLE_SHARE = 0.5    # under this share of usable rows: the ledger is unusable
LEDGER_MIN_VS_ATS = 0.5          # usable units under this share of ATS incoming: unusable
LEDGER_WARN_VS_ATS = 0.85        # under this share: alert 'Style ledger looks incomplete'

MESSAGES = {
    'AUTH_REQUIRED': 'Sign in to Versa Docs to view the P&L.',
    'ADMIN_ONLY': 'The P&L is for Versa admins only.',
    'PNL_NOT_CONFIGURED': 'The P&L tool is not set up on this server yet.',
    'NO_COSTBOOK': 'No cost book has been uploaded yet.',
    'VERSION_CONFLICT': 'Someone saved a newer version. Reload and try again.',
    'BAD_REQUEST': 'The request was not valid.',
    'TOO_LARGE': 'The upload is too large.',
    'INVALID_COSTBOOK': 'The cost book did not pass the checks.',
    'INVALID_SETTINGS': 'The settings did not pass the checks.',
    'INVALID_OVERRIDES': 'The manual costs did not pass the checks.',
    'NOT_FOUND': 'Not found.',
    'PNL_STORE_UNREADABLE': 'The saved P&L data could not be read.',
    'PNL_STORE_UNAVAILABLE': 'The P&L storage cannot be reached right now. Try again soon.',
    'INPUTS_UNAVAILABLE': 'Live data is still loading. Try again in a minute.',
    'BUILD_FAILED': 'The P&L could not be built. Try again later.',
    'INTERNAL': 'The P&L is unavailable right now.',
}
# A more precise message when the error carries one of these reasons.
REASON_MESSAGES = {
    ('INPUTS_UNAVAILABLE', 'ledger_unusable'):
        'The style ledger could not be read correctly. Check the ledger file. The P&L will try again.',
    ('PNL_STORE_UNREADABLE', 'keys_mismatch'):
        'The server key does not match the saved P&L data. Do not upload again. '
        'Keep the old key in PNL_DATA_KEYS after the new one.',
    ('PNL_STORE_UNREADABLE', 'current_unreadable'):
        'Nothing was saved. The server key does not open the saved P&L data. '
        'Keep the old key in PNL_DATA_KEYS after the new one.',
    ('PNL_STORE_UNREADABLE', 'damaged'): 'The saved P&L data looks damaged.',
}


def message_for(code, reason=None):
    return REASON_MESSAGES.get((code, reason)) or MESSAGES[code]


# (rule, methods besides OPTIONS). Every rule also answers OPTIONS with 204.
ROUTE_TABLE = (
    ('/api/pnl/status', ('GET',)),
    ('/api/pnl/dataset', ('GET',)),
    ('/api/pnl/costbook', ('GET', 'POST')),
    ('/api/pnl/costbook/versions', ('GET',)),
    ('/api/pnl/costbook/restore', ('POST',)),
    ('/api/pnl/settings', ('GET', 'POST')),
    ('/api/pnl/overrides', ('GET', 'POST')),
    ('/api/pnl/audit', ('POST',)),
    ('/api/pnl/rotate', ('POST',)),
    ('/api/pnl/analytics', ('GET',)),
    ('/api/pnl', ('GET', 'POST')),
    ('/api/pnl/<path:rest>', ('GET', 'POST')),
)

_SERVICES = []   # every registered service (diagnostics and tests)

_DISABLED_WORDS = ('off', '0', 'false', 'no', 'disabled')
ALLOW_ALL_ADMINS = '@admins'     # PNL_ALLOWED_EMAILS entry that admits every admin (contract C14)
_BIG_INT = 2 ** 53              # larger integers are refused before any float conversion
_DATE_RE = re.compile(r'^\d{4}-\d{2}-\d{2}$')
_CODE_RE = re.compile(r'^(?:_default|[A-Z0-9][A-Z0-9_]{0,11})$')
_TOKEN_RE = re.compile(r'^[A-Za-z0-9_]{1,40}$')
_REF_RE = re.compile(r'^[A-Z]{2}\d{5}$')
_STYLE_RE = re.compile(r'^[A-Z0-9][A-Z0-9-]{3,39}$')
_POOL_RE = re.compile(r'^[A-Z0-9]{2,4}-[A-Z0-9]{1,12}:[A-Z0-9_]{1,24}$')
_CUSTKEY_RE = re.compile(r'^[A-Z0-9][A-Z0-9 _/&-]{0,39}$')
_FIELD_RE = re.compile(r'^[a-z][a-z0-9_]{0,63}$')
_RECORD_ID_RE = re.compile(r'^[A-Za-z0-9!_.:+/-]{1,80}$')
_OVR_ID_RE = re.compile(r'^[A-Za-z0-9_.:-]{1,64}$')
_CODEISH_RE = re.compile(r'^[A-Z0-9][A-Z0-9 ._/-]{0,63}$')
_KEY_SAFE_RE = re.compile(r'[^A-Za-z0-9_.-]')
_DETAIL_SAFE_RE = re.compile(r'[^A-Za-z0-9 _.,:=/#-]')
_DECIMAL_RE = re.compile(r'\d+\.\d+')
_GROUPED_RE = re.compile(r'\d{1,3}(?:,\d{3})+')
_LONG_DIGITS_RE = re.compile(r'\d{5,}')
_INT_TEXT_RE = re.compile(r'^[0-9]{1,9}$')
_CTRL_RE = re.compile(r'[\x00-\x1f\x7f]')


# ─────────────────────────────────────────────────────────────────────────────
# Small helpers
# ─────────────────────────────────────────────────────────────────────────────
class _BadRequest(Exception):
    pass


class _TooLarge(Exception):
    pass


class _BuildError(Exception):
    def __init__(self, code, cls=None, missing=None, reason=None):
        super().__init__(code)
        self.code = code
        self.cls = cls or 'BuildError'
        self.missing = list(missing or [])
        self.reason = reason


def _json_default(o):
    if isinstance(o, (datetime, date)):
        return o.isoformat()
    if isinstance(o, (set, frozenset)):
        return sorted(o, key=str)
    raise TypeError('not serializable')


def _json(obj, status=200):
    body = json.dumps(obj, separators=(',', ':'), allow_nan=False, default=_json_default)
    return Response(body, status=status, mimetype='application/json')


def _iso(epoch):
    if not epoch:
        return None
    return datetime.fromtimestamp(epoch, timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')


def _utc_iso():
    return datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')


def _et_today_fallback():
    try:
        from zoneinfo import ZoneInfo
        return datetime.now(ZoneInfo('America/New_York')).date().isoformat()
    except Exception:
        return (datetime.now(timezone.utc) - timedelta(hours=4)).date().isoformat()


def _is_pnl_path(path):
    return path == '/api/pnl' or path.startswith('/api/pnl/')


def _clean_path(path):
    return re.sub(r'[^\x21-\x7e]', '?', str(path or ''))[:200]


def _safe_key(k):
    return _KEY_SAFE_RE.sub('?', str(k))[:60]


def _code_str(v):
    return re.sub(r'[^a-z0-9_]', '', str(v or '').lower())[:40] or 'not_configured'


def _token_shape_ok(token):
    return (isinstance(token, str) and 0 < len(token) < MAX_TOKEN_CHARS
            and all(33 <= ord(c) < 127 for c in token)
            and token.startswith('eyJ') and token.count('.') == 2)


def _digest(obj):
    raw = json.dumps(obj, sort_keys=True, separators=(',', ':'), default=str)
    return hashlib.sha256(raw.encode('utf-8')).hexdigest()[:20]


def _rows(v):
    if isinstance(v, (list, tuple)):
        return [dict(r) for r in v if isinstance(r, dict)]
    return []


def _str_or_none(v):
    if v is None or v == '':
        return None
    if isinstance(v, (int, float)) and not isinstance(v, bool):
        return _iso(v)
    return str(v)[:64]


def _problem(path, issue):
    return {'path': str(path)[:160], 'issue': issue}


def _huge_int(v):
    return isinstance(v, int) and not isinstance(v, bool) and abs(v) > _BIG_INT


# ── Audit detail: an allowlist, because the log is not protected like the P&L ──
# The page sends exactly these actions (pnl.core.js export and print). Any other action is
# refused with 400, so the action text can never carry an amount into the log.
_AUDIT_ACTIONS = frozenset({'export', 'print'})
_AUDIT_FIELDS = ('tab', 'drill', 'file', 'sheets', 'rows', 'count', 'n', 'whatIf')
_AUDIT_INTS = ('rows', 'count', 'n')
_AUDIT_CANON = {k.lower(): k for k in _AUDIT_FIELDS}


def _audit_text(v, limit=80):
    """Text for the audit line: amounts-like digit groups are masked with '#'."""
    s = _CTRL_RE.sub('', str(v))
    s = _DECIMAL_RE.sub('#', s)
    s = _GROUPED_RE.sub('#', s)
    s = _LONG_DIGITS_RE.sub('#', s)
    return _DETAIL_SAFE_RE.sub('', s).strip()[:limit]


def _safe_detail(detail):
    """Audit detail -> one short log fragment. Only the keys the page sends are kept: tab,
    drill, file and sheets (text), rows, count and n (whole numbers), whatIf (yes or no).
    Every other key is dropped, whatever its value. Never logs money."""
    if isinstance(detail, dict):
        pairs = list(detail.items())[:24]
    elif isinstance(detail, str):
        pairs = []
        for tok in detail.split()[:24]:
            k, sep, v = tok.partition('=')
            if sep:
                pairs.append((k, v))
    else:
        pairs = []
    out = {}
    for k, v in pairs:
        key = _AUDIT_CANON.get(str(k).strip().lower())
        if key is None or key in out:
            continue
        if key in _AUDIT_INTS:
            if isinstance(v, str) and _INT_TEXT_RE.match(v):
                v = int(v)
            if isinstance(v, bool) or not isinstance(v, int) or v < 0 or v > 10 ** 9:
                continue
            out[key] = str(v)
        elif key == 'whatIf':
            if isinstance(v, str):
                v = {'true': True, 'yes': True, '1': True, 'false': False, 'no': False, '0': False}.get(
                    v.strip().lower())
            if isinstance(v, bool):
                out[key] = 'yes' if v else 'no'
        else:
            if isinstance(v, (list, tuple)):
                v = ','.join(str(x) for x in list(v)[:20] if isinstance(x, str))
            elif not isinstance(v, str):
                continue
            text = _audit_text(v)
            if text:
                out[key] = text
    line = ' '.join('%s=%s' % (k, out[k]) for k in _AUDIT_FIELDS if k in out)
    return line[:240] or '-'


# ─────────────────────────────────────────────────────────────────────────────
# Source normalization (what the engine receives as `src`)
# ─────────────────────────────────────────────────────────────────────────────
def _normalize_sources(raw):
    def as_dict(v, list_key, extra):
        if isinstance(v, tuple):              # tolerated: (rows, stamp) or (orders, ok)
            v = {list_key: v[0] if v else [], extra: v[1] if len(v) > 1 else None}
        return v if isinstance(v, dict) else {}

    inv = as_dict(raw.get('inventory'), 'items', 'last_sync')
    led = as_dict(raw.get('ledger'), 'rows', 'last_sync')
    apo = as_dict(raw.get('apo'), 'rows', 'last_sync')
    oo = as_dict(raw.get('open_orders'), 'orders', 'ok')
    fob = raw.get('fob_customers')
    ri = raw.get('routing_inputs')
    sa = raw.get('sales_analytics')
    today = raw.get('today')
    if not (isinstance(today, str) and _DATE_RE.match(today)):
        today = _et_today_fallback()
    try:
        sa = json.loads(json.dumps(sa, allow_nan=False)) if isinstance(sa, dict) else None
    except (TypeError, ValueError):
        sa = None
    return {
        'inventory': {'items': _rows(inv.get('items')), 'last_sync': _str_or_none(inv.get('last_sync'))},
        'ledger': {'rows': _rows(led.get('rows')), 'last_sync': _str_or_none(led.get('last_sync'))},
        'apo': {'rows': _rows(apo.get('rows')), 'last_sync': _str_or_none(apo.get('last_sync'))},
        'manual_allocations': _rows(raw.get('manual_allocations')),
        'vw_allocations': _rows(raw.get('vw_allocations')),
        'open_orders': {'orders': _rows(oo.get('orders')), 'ok': oo.get('ok') is True,
                        'fetched_at': _str_or_none(oo.get('fetched_at'))},
        'fob_customers': ([str(c).strip().upper() for c in fob if str(c).strip()]
                          if isinstance(fob, (list, tuple)) else []),
        'routing_inputs': copy.deepcopy(ri) if isinstance(ri, dict) else {},
        'sales_analytics': sa,
        'today': today,
    }


def _missing_inputs(src):
    missing = []
    if not src['ledger']['rows']:
        missing.append('ledger')
    if not src['inventory']['items']:
        missing.append('inventory')
    if not src['open_orders']['orders']:
        missing.append('open_orders')
    return missing


def _stamps(src):
    sa = src.get('sales_analytics')
    if isinstance(sa, dict):
        s = sa.get('source') if isinstance(sa.get('source'), dict) else {}
        sa_key = 'building' if sa.get('building') else (str(s.get('ingestedAt')), str(s.get('rows')),
                                                         str(sa.get('styleCount')), str(sa.get('v')))
    else:
        sa_key = None
    ri = {k: v for k, v in (src.get('routing_inputs') or {}).items() if k not in ('now_et', 'now')}
    return (_digest(src['inventory']['items']), _digest(src['ledger']['rows']),
            _digest(src['apo']['rows']), _digest(src['open_orders']['orders']),
            src['open_orders']['ok'], _digest(src['manual_allocations']),
            _digest(src['vw_allocations']), _digest(src['fob_customers']), _digest(ri),
            sa_key, src['today'])


# ─────────────────────────────────────────────────────────────────────────────
# Ledger sanity (contracts review F3): an unusable ledger must not build silently
# ─────────────────────────────────────────────────────────────────────────────
def _to_int(v):
    try:
        return int(float(v))
    except (TypeError, ValueError, OverflowError):
        return 0


def _upper(v):
    return str(v or '').strip().upper()


def _ledger_check(src, routing=None):
    """Row and unit counts of the style ledger against the ATS feed. A row is usable when
    its style is an exact ATS SKU and it carries units (the rules pnl_routing uses)."""
    to_int = getattr(routing, 'to_int', None)
    to_int = to_int if callable(to_int) else _to_int
    norm = getattr(routing, '_u', None)
    norm = norm if callable(norm) else _upper
    feed, ats_incoming = set(), 0
    for it in src['inventory']['items']:
        sku = norm(it.get('sku'))
        if sku:
            feed.add(sku)
        ats_incoming += max(0, to_int(it.get('incoming')))
    rows = src['ledger']['rows']
    usable = units = 0
    refs = set()
    for r in rows:
        u = to_int(r.get('units'))
        if u > 0 and norm(r.get('style')) in feed:
            usable += 1
            units += u
            ref = norm(r.get('production'))
            if ref:
                refs.add(ref)
    ratio = (units / ats_incoming) if ats_incoming > 0 else None
    return {'rows': len(rows), 'usableRows': usable, 'usableUnits': units, 'atsIncoming': ats_incoming,
            'unitsVsAts': (math.floor(ratio * 1000 + 0.5) / 1000) if ratio is not None else None,
            'refs': len(refs), '_ratio': ratio}


def _ledger_unusable(chk):
    if not chk['rows']:
        return False                      # an empty ledger is reported by _missing_inputs
    if chk['usableRows'] < LEDGER_MIN_USABLE_SHARE * chk['rows']:
        return True
    return chk['_ratio'] is not None and chk['_ratio'] < LEDGER_MIN_VS_ATS


def _alloc_units(ds):
    """{kind: placed units} from dataset.alloc (warehouse, production, unsourced), or None
    when the dataset has no such table."""
    t = ds.get('alloc')
    if not isinstance(t, dict):
        return None
    fields, rows = t.get('fields'), t.get('rows')
    if not isinstance(fields, list) or not isinstance(rows, list) or 'kind' not in fields or 'units' not in fields:
        return None
    ik, iu = fields.index('kind'), fields.index('units')
    out = {}
    for r in rows:
        if not isinstance(r, (list, tuple)) or len(r) <= max(ik, iu):
            continue
        u = r[iu]
        if isinstance(u, bool) or not isinstance(u, (int, float)) or _huge_int(u) or not math.isfinite(u):
            continue
        out[str(r[ik])] = out.get(str(r[ik]), 0) + u
    return out


def _input_alerts(ds, chk):
    """Alerts pnl.py adds after the engine (C4 shape). Counts only, no money."""
    alerts = ds.get('alerts') if isinstance(ds.get('alerts'), list) else []
    have = {a.get('id') for a in alerts if isinstance(a, dict)}
    add = []
    ratio = chk.get('_ratio')
    if ratio is not None and ratio < LEDGER_WARN_VS_ATS and 'al_ledger_partial' not in have:
        add.append({'id': 'al_ledger_partial', 'kind': 'stale_input', 'severity': 'medium',
                    'title': 'Style ledger looks incomplete',
                    'detail': 'The style ledger holds %d percent of the incoming units in the ATS feed. '
                              'Some production costs and supply matches may be missing.'
                              % int(ratio * 100 + 0.5),
                    'count': chk['refs'], 'unit': 'refs', 'value': None, 'valueLabel': None, 'refs': {},
                    'refsTotal': 0})
    placed = _alloc_units(ds)
    # Demand that found no supply while nothing at all was placed on the ledger's production:
    # the ledger is most likely disconnected from the feed (a column shift, a wrong sheet).
    if (placed is not None and chk['usableRows'] > 0 and placed.get('production', 0) <= 0
            and placed.get('unsourced', 0) > 0 and 'al_ledger_no_production' not in have):
        add.append({'id': 'al_ledger_no_production', 'kind': 'stale_input', 'severity': 'high',
                    'title': 'No order is placed on production',
                    'detail': 'Some open units found no supply, yet routing placed nothing on the production '
                              'in the style ledger. Check the ledger file before you trust supply and cost matches.',
                    'count': chk['refs'], 'unit': 'refs', 'value': None, 'valueLabel': None, 'refs': {},
                    'refsTotal': 0})
    if add:
        ds['alerts'] = alerts + add


# ─────────────────────────────────────────────────────────────────────────────
# Validation (cost book, settings, overrides). Problems name paths, never values.
# ─────────────────────────────────────────────────────────────────────────────
_CB_TOP_KEYS = ('v', 'generatedAt', 'parser', 'sources', 'params', 'records', 'conflict_groups', 'meta')
MAX_RECORDS = 50000
_USD_BAND = (0.0, 1000.0)       # plausibility band for any price_usd* field
_RMB_BAND = (0.0, 100000.0)     # plausibility band for any rmb* field
_FX_BAND = (5, 10)              # RMB per US dollar: settings fx.rate and params.fxBase (contract C3)
_MAX_PROBLEMS = 200


def _walk_finite(value, path, problems, depth=0):
    if len(problems) >= _MAX_PROBLEMS:
        return
    if depth > 12:
        problems.append(_problem(path, 'too_deep'))
        return
    if value is None or isinstance(value, bool):
        return
    if isinstance(value, str):
        if len(value) > 5000:
            problems.append(_problem(path, 'too_long'))
        return
    if isinstance(value, (int, float)):
        if _huge_int(value):
            problems.append(_problem(path, 'out_of_range'))
        elif isinstance(value, float) and not math.isfinite(value):
            problems.append(_problem(path, 'not_finite'))
        return
    if isinstance(value, list):
        if len(value) > MAX_RECORDS:
            problems.append(_problem(path, 'too_many'))
            return
        for i, v in enumerate(value):
            _walk_finite(v, '%s[%d]' % (path, i), problems, depth + 1)
        return
    if isinstance(value, dict):
        for k, v in value.items():
            _walk_finite(v, '%s.%s' % (path, _safe_key(k)), problems, depth + 1)
        return
    problems.append(_problem(path, 'wrong_type'))


def _check_band(value, band, path, problems):
    if value is None:
        return
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        problems.append(_problem(path, 'not_a_number'))
    elif _huge_int(value):
        problems.append(_problem(path, 'out_of_range'))
    elif not math.isfinite(value):
        problems.append(_problem(path, 'not_finite'))
    elif value < band[0] or value > band[1]:
        problems.append(_problem(path, 'out_of_range'))


def _n(lo, hi):
    return ('num', lo, hi)


_BOOL = ('bool',)
_TEXT80 = ('text', 80)
_FIBERS = ('cotton', 'mmf', 'linen', 'wool')
_POOL_LIST = ('list', ('pool',), 200)
_PROGRAM_FIELDS = ('brand', 'cat', 'fab', 'fit', 'group', 'pat', 'sleeve', 'src')

# Cost book params (sec review B2). The engine reads the required keys; a wrong type or an
# absurd value either moves every margin or stops the build, so each one is checked here.
# Bands are wide on purpose: they refuse typos and nonsense and reveal nothing.
PARAMS_REQUIRED = ('fxBase', 'ssDelta', 'fitPremium', 'gridPrecedence', 'poolOrder', 'programMap',
                   'kitPcs', 'customerGroupPrefix')
PARAMS_SCHEMA = {
    'fxBase': _n(*_FX_BAND),
    'ssDelta': ('nullable', _n(-20, 20)),
    'fitPremium': ('obj', {'regular': _n(-20, 20), 'bigTall': _n(-20, 20)}),
    'gridPrecedence': ('map', ('code',), ('list', ('code',), 12)),
    'poolOrder': ('objmap', {'all': _POOL_LIST,
                             'polo': ('obj', {'primary': _POOL_LIST, 'fallback': _POOL_LIST}),
                             'recordKinds': ('list', ('token',), 20),
                             'excludeFlags': ('list', ('token',), 100),
                             'rule': ('text', 2000)},
                  ('code',), ('obj', {'base': _POOL_LIST, 'byGroup': ('map', ('token',), _POOL_LIST)})),
    'programMap': ('map', ('style',), ('obj', {f: ('nullable', ('text', 200)) for f in _PROGRAM_FIELDS}), 5000),
    'kitPcs': ('map', ('style',), ('int', 2, 1000), 5000),
    'customerGroupPrefix': ('map', ('token',), ('token',)),
    'fiberByStyle': ('map', ('style',), ('enum', _FIBERS), 50000),     # exact style -> fiber
    'fiberByFabric': ('map', ('code',), ('enum', _FIBERS)),            # fabric code -> fiber
    'rmbBase': _n(0.5, 50),                                            # not read by the engine
    'cutFactor': _n(0.1, 2.0),                                         # not read by the engine
}


def validate_params(params):
    """-> (clean | None, problems, dropped). Cost book params: required keys, types and
    plausibility bands. Unknown keys are dropped (listed as params.<key>)."""
    problems, dropped = [], []
    if not isinstance(params, dict):
        return None, [_problem('params', 'wrong_type')], dropped
    out = {}
    for k, v in params.items():
        spec = PARAMS_SCHEMA.get(k)
        path = 'params.%s' % _safe_key(k)
        if spec is None:
            dropped.append(path)
            continue
        r = _sanitize(v, spec, path, problems, dropped)
        if r is not _INVALID:
            out[k] = r
    for k in PARAMS_REQUIRED:
        if k not in params:
            problems.append(_problem('params.' + k, 'missing'))
    fp = params.get('fitPremium')
    if isinstance(fp, dict):
        for k in ('regular', 'bigTall'):
            if k not in fp:
                problems.append(_problem('params.fitPremium.' + k, 'missing'))
    if problems:
        return None, problems, dropped
    return out, problems, dropped


def _param_changes(old, new, path='params', out=None, limit=200):
    """Paths that differ between two params objects ({path, issue} with issue changed, added
    or removed). Never values."""
    out = [] if out is None else out
    if len(out) >= limit:
        return out
    if isinstance(old, dict) and isinstance(new, dict):
        for k in sorted(set(old) | set(new), key=str):
            p = '%s.%s' % (path, _safe_key(k))
            if k not in old:
                out.append(_problem(p, 'added'))
            elif k not in new:
                out.append(_problem(p, 'removed'))
            else:
                _param_changes(old[k], new[k], p, out, limit)
            if len(out) >= limit:
                break
    else:
        both_numbers = all(isinstance(x, (int, float)) and not isinstance(x, bool) for x in (old, new))
        if old != new or (type(old) is not type(new) and not both_numbers):   # 1 vs 1.0 is no change
            out.append(_problem(path, 'changed'))
    return out


def validate_costbook(cb):
    """-> (clean | None, problems, warnings, dropped). Section 5.1 shape; every number
    finite; price fields inside plausibility bands; unique record ids; params checked
    against PARAMS_SCHEMA; the bulky record 'inputs' field is not stored."""
    problems, warnings, dropped = [], [], []
    if not isinstance(cb, dict):
        return None, [_problem('costbook', 'not_an_object')], warnings, dropped
    if cb.get('v') != 1:
        problems.append(_problem('v', 'unsupported_version'))
    recs = cb.get('records')
    if not isinstance(recs, list) or not recs:
        problems.append(_problem('records', 'missing'))
        return None, problems, warnings, dropped
    if len(recs) > MAX_RECORDS:
        problems.append(_problem('records', 'too_many'))
        return None, problems, warnings, dropped
    for k in cb:
        if k not in _CB_TOP_KEYS:
            dropped.append(_safe_key(k))
    if cb.get('generatedAt') is not None and not isinstance(cb.get('generatedAt'), str):
        problems.append(_problem('generatedAt', 'wrong_type'))
    params = cb.get('params')
    clean_params = None
    if params is None:
        problems.append(_problem('params', 'missing'))
    else:
        clean_params, p_problems, p_dropped = validate_params(params)
        problems.extend(p_problems)
        dropped.extend(p_dropped)
    for k in ('parser', 'meta'):
        if k in cb and cb[k] is not None:
            if not isinstance(cb[k], dict):
                problems.append(_problem(k, 'wrong_type'))
            else:
                _walk_finite(cb[k], k, problems)
    meta = cb.get('meta') if isinstance(cb.get('meta'), dict) else {}
    roles = meta.get('priceFieldRoles')
    if roles is not None and (not isinstance(roles, dict) or len(roles) > 20 or any(
            not isinstance(v, str) or not _FIELD_RE.match(v) for v in roles.values())):
        problems.append(_problem('meta.priceFieldRoles', 'invalid'))
    names = list(meta.get('priceFields') or {}) if isinstance(meta.get('priceFields'), dict) else []
    names += [v for v in (roles.values() if isinstance(roles, dict) else ()) if isinstance(v, str)]
    if any(re.search(r'\d', str(n)) for n in names):
        warnings.append(_problem('meta.priceFields', 'legacy_field_names'))   # rebuild with pnl_costs.py
    for k in ('sources', 'conflict_groups'):
        if k in cb and cb[k] is not None:
            if not isinstance(cb[k], list):
                problems.append(_problem(k, 'wrong_type'))
            else:
                _walk_finite(cb[k], k, problems)
    seen, out = set(), []
    for i, r in enumerate(recs):
        if len(problems) >= _MAX_PROBLEMS:
            break
        path = 'records[%d]' % i
        if not isinstance(r, dict):
            problems.append(_problem(path, 'not_an_object'))
            continue
        rid = r.get('id')
        if not isinstance(rid, str) or not _RECORD_ID_RE.match(rid):
            problems.append(_problem(path + '.id', 'invalid'))
        elif rid in seen:
            problems.append(_problem(path + '.id', 'duplicate'))
        else:
            seen.add(rid)
        if 'price_usd' not in r:
            problems.append(_problem(path + '.price_usd', 'missing'))
        rec = {}
        for k, v in r.items():
            if k == 'inputs':
                continue
            ks = str(k)
            kpath = '%s.%s' % (path, _safe_key(ks))
            if ks.startswith('price_usd'):
                _check_band(v, _USD_BAND, kpath, problems)
            elif ks.startswith('rmb'):
                _check_band(v, _RMB_BAND, kpath, problems)
            else:
                _walk_finite(v, kpath, problems, 1)
            rec[k] = v
        ref = r.get('production_ref_resolved')
        if isinstance(ref, str) and ref and not _REF_RE.match(ref):
            warnings.append(_problem(path + '.production_ref_resolved', 'unusual_ref'))
        style = r.get('style')
        if isinstance(style, str) and style and not _STYLE_RE.match(style.upper()):
            warnings.append(_problem(path + '.style', 'unusual_style'))
        out.append(rec)
    if problems:
        return None, problems, warnings, dropped
    clean = {k: cb[k] for k in _CB_TOP_KEYS if k in cb and k not in ('records', 'params')}
    clean['params'] = clean_params
    clean['records'] = out
    return clean, problems, warnings, dropped


def _nullable(spec):
    return ('nullable', spec)


_REGIMES = ('us', 'ca', 'none')
# Settings (DESIGN 5.2 plus contract C3). In every map below, a null value means "use the
# default for this key": the engine's merge_settings drops it.
SETTINGS_SCHEMA = {
    'v': ('server',),
    # rate null = as printed on each sheet (the engine default, contract C10). A number reprices every
    # RMB-based cost at that rate.
    'fx': ('obj', {'rate': ('nullable', _n(*_FX_BAND)), 'asOf': ('date',), 'basis': ('token',)}),
    'gridPrecedence': ('nullable', ('map', ('code',), ('list', ('code',), 12))),
    'factories': ('map', ('code',), _nullable(('obj', {'name': _TEXT80, 'origin': ('code',)}))),
    # Where goods land, by A2000 warehouse, ledger landing or stock warehouse code:
    # 'us' (US duty, freight and fees), 'ca' (Canadian duty) or 'none' (no import costs).
    'destinations': ('map', ('code',), _nullable(('enum', _REGIMES))),
    'tariff': ('obj', {'addonPct': ('map', ('code',), _nullable(_n(0, 200))),
                       'mfnPct': ('map', ('token',), _nullable(_n(0, 100))),
                       'caMfnPct': _n(0, 100),
                       'mmfShirtSpecificPerUnit': _n(0, 20),
                       'mpfPct': _n(0, 10), 'hmfPct': _n(0, 10)}),
    'freight': ('obj', {'oceanPerUnit': ('map', ('token',), _nullable(_n(0, 100))),
                        'inlandPerUnit': ('map', ('token',), _nullable(_n(0, 100)))}),
    # base: royalty on net sales or on revenue (contract C11).
    'royalty': ('obj', {'defaultPct': _n(0, 50), 'base': ('enum', ('net', 'revenue')),
                        'byBrand': ('map', ('code',), _nullable(_n(0, 50)))}),
    # Customer keys are A2000 codes, or the allocation sheet's customer text (it can hold spaces).
    'deductions': ('obj', {'byGroup': ('map', ('token',), _nullable(_n(0, 50))),
                           'byCustomer': ('map', ('custkey',), _nullable(_n(0, 50)))}),
    'customerGroups': ('map', ('custkey',), _nullable(('token',))),
    'fobCustomers': ('nullable', ('list', ('custkey',), 500)),
    'opex': ('obj', {'items': ('list', ('obj', {'name': _TEXT80, 'monthly': _n(0, 1e9)}), 50)}),
    'routing': ('obj', {'picksAsWarehouse': _BOOL, 'honorAssignments': _BOOL,
                        'gateFallback': ('enum', ('fifo', 'engine'))}),
    'bulk': ('obj', {'includeInTotals': _BOOL}),
    # Cost model (contract C11): 'itemized' duty, freight and fees, or one import multiplier per fiber
    # class (1 to 3; null = not set yet). Revenue costs: items, each a percent of revenue from 0 to 50.
    'landed': ('obj', {'mode': ('enum', ('itemized', 'multiplier')),
                       'multiplier': ('obj', {'natural': _nullable(_n(1, 3)), 'synthetic': _nullable(_n(1, 3))})}),
    'revenueCosts': ('obj', {'items': ('list', ('obj', {'key': ('token',), 'name': _TEXT80, 'pct': _n(0, 50)}), 50)}),
    # Duty regime by A2000 customer code (contract C12). It beats the FOB list and the destinations.
    'regimeByCustomer': ('map', ('custkey',), _nullable(('enum', _REGIMES))),
    # How a style's cost combines the prices of its factories (contract C13).
    'costRule': ('obj', {'mode': ('enum', ('combined', 'cascade')), 'wideSpreadPct': _n(0, 100)}),
    # A note shown next to the shipped history (contract C14).
    'history': ('obj', {'caveat': _nullable(('text', 500))}),
    'confirmed': ('map', ('token',), _BOOL),
    'updatedAt': ('server',),
    'updatedBy': ('server',),
}
_INVALID = object()


def _sanitize(value, spec, path, problems, dropped):
    kind = spec[0]
    if kind == 'nullable':
        return None if value is None else _sanitize(value, spec[1], path, problems, dropped)
    if kind == 'num':
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            problems.append(_problem(path, 'not_a_finite_number'))
            return _INVALID
        if _huge_int(value):
            problems.append(_problem(path, 'out_of_range'))
            return _INVALID
        if not math.isfinite(value):
            problems.append(_problem(path, 'not_a_finite_number'))
            return _INVALID
        if value < spec[1] or value > spec[2]:
            problems.append(_problem(path, 'out_of_range'))
            return _INVALID
        return value
    if kind == 'int':
        if isinstance(value, bool) or not isinstance(value, (int, float)) or (
                isinstance(value, float) and not (math.isfinite(value) and value.is_integer())):
            problems.append(_problem(path, 'not_an_integer'))
            return _INVALID
        if _huge_int(value) or value < spec[1] or value > spec[2]:
            problems.append(_problem(path, 'out_of_range'))
            return _INVALID
        return int(value)
    if kind == 'bool':
        if not isinstance(value, bool):
            problems.append(_problem(path, 'not_a_boolean'))
            return _INVALID
        return value
    if kind == 'text':
        if not isinstance(value, str) or len(value) > spec[1]:
            problems.append(_problem(path, 'invalid_text'))
            return _INVALID
        return _CTRL_RE.sub('', value).strip()
    if kind == 'token':
        if not isinstance(value, str) or not _TOKEN_RE.match(value.strip()):
            problems.append(_problem(path, 'invalid_token'))
            return _INVALID
        return value.strip()
    if kind == 'code':
        if not isinstance(value, str):
            problems.append(_problem(path, 'invalid_code'))
            return _INVALID
        v = value.strip()
        v = v if v == '_default' else v.upper()
        if not _CODE_RE.match(v):
            problems.append(_problem(path, 'invalid_code'))
            return _INVALID
        return v
    if kind in ('style', 'pool', 'custkey'):
        rx = {'style': _STYLE_RE, 'pool': _POOL_RE, 'custkey': _CUSTKEY_RE}[kind]
        v = value.strip().upper() if isinstance(value, str) else None
        if v is None or not rx.match(v):
            problems.append(_problem(path, 'invalid_' + kind))
            return _INVALID
        return v
    if kind == 'enum':
        if not isinstance(value, str) or value not in spec[1]:
            problems.append(_problem(path, 'invalid_choice'))
            return _INVALID
        return value
    if kind == 'date':
        if value is None or value == '':
            return None
        if not isinstance(value, str) or not _DATE_RE.match(value):
            problems.append(_problem(path, 'invalid_date'))
            return _INVALID
        return value
    if kind == 'obj':
        if not isinstance(value, dict):
            problems.append(_problem(path, 'not_an_object'))
            return _INVALID
        out = {}
        for k, v in value.items():
            sub = spec[1].get(k)
            if sub is None:
                dropped.append('%s.%s' % (path, _safe_key(k)))
                continue
            r = _sanitize(v, sub, '%s.%s' % (path, _safe_key(k)), problems, dropped)
            if r is not _INVALID:
                out[k] = r
        return out
    if kind == 'objmap':          # fixed keys with their own specs; every other key is a map entry
        if not isinstance(value, dict) or len(value) > 500:
            problems.append(_problem(path, 'not_an_object'))
            return _INVALID
        out = {}
        for k, v in value.items():
            kp = '%s.%s' % (path, _safe_key(k))
            sub = spec[1].get(k)
            if sub is not None:
                r = _sanitize(v, sub, kp, problems, dropped)
                if r is not _INVALID:
                    out[k] = r
                continue
            kk = _sanitize(k, spec[2], kp, problems, dropped)
            vv = _sanitize(v, spec[3], kp, problems, dropped)
            if kk is not _INVALID and vv is not _INVALID:
                out[kk] = vv
        return out
    if kind == 'map':
        cap = spec[3] if len(spec) > 3 else 500
        if not isinstance(value, dict) or len(value) > cap:
            problems.append(_problem(path, 'not_an_object'))
            return _INVALID
        out = {}
        for k, v in value.items():
            if len(problems) >= _MAX_PROBLEMS:
                return _INVALID
            kp = '%s.%s' % (path, _safe_key(k))
            kk = _sanitize(k, spec[1], kp, problems, dropped)
            vv = _sanitize(v, spec[2], kp, problems, dropped)
            if kk is not _INVALID and vv is not _INVALID:
                out[kk] = vv
        return out
    if kind == 'list':
        if not isinstance(value, list):
            problems.append(_problem(path, 'not_a_list'))
            return _INVALID
        if len(value) > spec[2]:          # a list over its cap is a list: say it is too long
            problems.append(_problem(path, 'too_long'))
            return _INVALID
        out = []
        for i, v in enumerate(value):
            r = _sanitize(v, spec[1], '%s[%d]' % (path, i), problems, dropped)
            if r is not _INVALID:
                out.append(r)
        return out
    problems.append(_problem(path, 'unsupported'))
    return _INVALID


def _revenue_cost_problems(raw):
    """Every revenue cost item names its key and its percent, and keys are unique (contract C11).
    Types and bands are the schema's job."""
    out = []
    items = raw.get('items') if isinstance(raw, dict) else None
    if not isinstance(items, list):
        return out
    seen = set()
    for i, it in enumerate(items):
        if not isinstance(it, dict):
            continue
        for f in ('key', 'pct'):
            if f not in it:
                out.append(_problem('revenueCosts.items[%d].%s' % (i, f), 'missing'))
        k = it.get('key')
        if isinstance(k, str) and k.strip():
            if k.strip() in seen:
                out.append(_problem('revenueCosts.items[%d].key' % i, 'duplicate'))
            seen.add(k.strip())
    return out


def validate_settings(settings, defaults=None):
    """-> (clean | None, problems, dropped). Section 5.2 keys with type and plausibility
    checks. Unknown keys are dropped, except top-level keys that also exist in the
    engine's DEFAULT_SETTINGS (kept after a finite-number check). Null values inside maps
    are kept (they mean "use the default"). The server stamps v, updatedAt and updatedBy.
    Contracts C11 to C14 add landed, royalty.base, revenueCosts, regimeByCustomer, costRule
    and history."""
    problems, dropped = [], []
    if not isinstance(settings, dict):
        return None, [_problem('settings', 'not_an_object')], dropped
    out = {}
    for k, v in settings.items():
        spec = SETTINGS_SCHEMA.get(k)
        if spec is not None:
            if spec[0] == 'server':
                continue
            r = _sanitize(v, spec, _safe_key(k), problems, dropped)
            if k == 'revenueCosts':
                problems.extend(_revenue_cost_problems(v))
            if r is not _INVALID:
                out[k] = r
        elif isinstance(defaults, dict) and k in defaults:
            before = len(problems)
            _walk_finite(v, _safe_key(k), problems)
            if len(problems) == before:
                out[k] = v
        else:
            dropped.append(_safe_key(k))
    if problems:
        return None, problems, dropped
    out['v'] = 1
    return out, problems, dropped


_OVR_SCOPES = {'ref_style': ('ref', 'style'), 'style': ('style',), 'design': ('design',),
               'ref': ('ref',), 'fabric_category': ('fabric', 'category')}
_OVR_KEY_FIELDS = ('ref', 'style', 'design', 'fabric', 'category')
_OVR_FIELDS = ('id', 'scope', 'key', 'fobU', 'reason', 'effective', 'by', 'at')
MAX_OVERRIDES = 5000
_OVR_FOB_BAND = (0.0, 1000.0)


def validate_overrides(items, previous=None, email='', now_iso=None):
    """-> (clean | None, problems, dropped). Section 5.3 entries. by/at are stamped
    by the server on new or changed entries and kept on unchanged ones."""
    problems, dropped = [], []
    if not isinstance(items, list):
        return None, [_problem('overrides', 'not_a_list')], dropped
    if len(items) > MAX_OVERRIDES:
        return None, [_problem('overrides', 'too_many')], dropped
    now_iso = now_iso or _utc_iso()
    prev = {o.get('id'): o for o in (previous or []) if isinstance(o, dict)}
    seen, out = set(), []
    for i, o in enumerate(items):
        if len(problems) >= _MAX_PROBLEMS:
            break
        path = 'overrides[%d]' % i
        if not isinstance(o, dict):
            problems.append(_problem(path, 'not_an_object'))
            continue
        for k in o:
            if k not in _OVR_FIELDS:
                dropped.append('%s.%s' % (path, _safe_key(k)))
        oid = o.get('id')
        if not isinstance(oid, str) or not _OVR_ID_RE.match(oid):
            problems.append(_problem(path + '.id', 'invalid'))
        elif oid in seen:
            problems.append(_problem(path + '.id', 'duplicate'))
        else:
            seen.add(oid)
        scope = o.get('scope')
        if scope not in _OVR_SCOPES:
            problems.append(_problem(path + '.scope', 'invalid'))
            continue
        key_in = o.get('key')
        if not isinstance(key_in, dict):
            problems.append(_problem(path + '.key', 'missing'))
            continue
        key = {}
        for f in _OVR_KEY_FIELDS:
            v = key_in.get(f)
            v = '' if v is None else v
            if not isinstance(v, str):
                problems.append(_problem('%s.key.%s' % (path, f), 'wrong_type'))
                continue
            v = v.strip()
            if f == 'category':
                if v and not _TOKEN_RE.match(v):
                    problems.append(_problem('%s.key.%s' % (path, f), 'invalid'))
            else:
                v = v.upper()
                if v and not _CODEISH_RE.match(v):
                    problems.append(_problem('%s.key.%s' % (path, f), 'invalid'))
            key[f] = v
        for f in key_in:
            if f not in _OVR_KEY_FIELDS:
                dropped.append('%s.key.%s' % (path, _safe_key(f)))
        for f in _OVR_SCOPES[scope]:
            if not key.get(f):
                problems.append(_problem('%s.key.%s' % (path, f), 'missing'))
        fob = o.get('fobU')
        if isinstance(fob, bool) or not isinstance(fob, (int, float)):
            problems.append(_problem(path + '.fobU', 'not_a_finite_number'))
        elif _huge_int(fob):
            problems.append(_problem(path + '.fobU', 'out_of_range'))
        elif not math.isfinite(fob):
            problems.append(_problem(path + '.fobU', 'not_a_finite_number'))
        elif fob <= _OVR_FOB_BAND[0] or fob > _OVR_FOB_BAND[1]:
            problems.append(_problem(path + '.fobU', 'out_of_range'))
        reason = o.get('reason')
        reason = '' if reason is None else reason
        if not isinstance(reason, str) or len(reason) > 500:
            problems.append(_problem(path + '.reason', 'invalid_text'))
            reason = ''
        eff = o.get('effective')
        eff = '' if eff is None else eff
        if not isinstance(eff, str) or (eff and not _DATE_RE.match(eff)):
            problems.append(_problem(path + '.effective', 'invalid_date'))
            eff = ''
        rec = {'id': oid, 'scope': scope, 'key': key, 'fobU': fob,
               'reason': _CTRL_RE.sub('', reason).strip(), 'effective': eff}
        old = prev.get(oid)
        if old is not None and all(old.get(f) == rec[f] for f in ('scope', 'key', 'fobU', 'reason', 'effective')):
            rec['by'], rec['at'] = old.get('by'), old.get('at')
        else:
            rec['by'], rec['at'] = email, now_iso
        out.append(rec)
    if problems:
        return None, problems, dropped
    return out, problems, dropped


# ─────────────────────────────────────────────────────────────────────────────
# The service: auth, routes, memo, background build
# ─────────────────────────────────────────────────────────────────────────────
class _PnlService:
    _MODULE_SPEC = {'engine': ('pnl_engine', 'build_dataset'), 'routing': ('pnl_routing', 'route_all')}

    def __init__(self, store, caller_identity, sources, env, engine, routing):
        self.store = store
        self.caller_identity = caller_identity
        self.sources = sources if isinstance(sources, dict) else {}
        self.env = env if env is not None else {}
        self._explicit = {'engine': engine, 'routing': routing}
        self._loaded = {}
        self._module_fail = {}
        self._lock = threading.Lock()
        self._memo = None           # {'key', 'stamps', 'body', 'gz', 'started', 'checked', 'builtAt'}
        self._job = None            # {'id', 'key', 'kind', 'forced', 'started', 'event'}
        self._job_seq = 0
        self._last_error = None     # {'key', 'at', 'code', 'cls', 'missing', 'reason'}
        self._last_forced = 0.0
        self._cb_summary = {}
        self._an_memo = None        # {'key', 'builtAt', 'alias', 'byCust', 'byStyle', 'grades'}
        self.sales_matrix = None    # set by the host app after registration (invoiced-history cube getter)
        # tunables (instance attributes so tests can shorten them)
        self.wait_seconds = WAIT_SECONDS
        self.refresh_window = REFRESH_WINDOW
        self.recheck_seconds = RECHECK_SECONDS
        self.error_retry_seconds = ERROR_RETRY_SECONDS
        self.module_retry_seconds = MODULE_RETRY_SECONDS
        self.max_costbook_bytes = MAX_COSTBOOK_BYTES
        self.max_settings_bytes = MAX_SETTINGS_BYTES
        self.max_overrides_bytes = MAX_OVERRIDES_BYTES
        self.max_small_bytes = MAX_SMALL_BYTES

    # ── configuration ──
    def _allowlist(self):
        raw = str(self.env.get('PNL_ALLOWED_EMAILS', '') or '')
        return {e.strip().lower() for e in raw.split(',') if e.strip()}

    def _disabled(self):
        return str(self.env.get('PNL_ENABLED', 'on') or 'on').strip().lower() in _DISABLED_WORDS

    def _store_ready(self):
        return self.store is not None and getattr(self.store, 'ok', False) is True

    def _module(self, kind):
        name, attr = self._MODULE_SPEC[kind]
        explicit = self._explicit.get(kind)
        if explicit is not None:
            return explicit if callable(getattr(explicit, attr, None)) else None
        with self._lock:
            mod = self._loaded.get(kind)
            failed_at = self._module_fail.get(kind)
        if mod is not None:
            return mod
        if failed_at is not None and time.time() - failed_at < self.module_retry_seconds:
            return None
        try:
            mod = importlib.import_module(name)
            if not callable(getattr(mod, attr, None)):
                raise ImportError(attr)
        except Exception as e:
            print('[PnL] %s unavailable: %s' % (name, type(e).__name__), flush=True)
            with self._lock:
                self._module_fail[kind] = time.time()
            return None
        with self._lock:
            self._loaded[kind] = mod
            self._module_fail.pop(kind, None)
        return mod

    def _config_reasons(self):
        reasons = []
        if self.store is None:
            reasons.append('store:no_backend')
        elif not self._store_ready():
            reasons.append('store:' + _code_str(getattr(self.store, 'reason', '')))
        for kind in ('engine', 'routing'):
            if self._module(kind) is None:
                reasons.append(kind)
        return reasons

    def _require_store(self):
        if not self._store_ready():
            raise StoreNotConfigured('store')

    def _object_kid(self, name):
        fn = getattr(self.store, 'object_kid', None)
        if not callable(fn):
            return None
        try:
            return fn(name)
        except Exception:
            return None

    def _reason_for(self, name, code):
        """'keys_mismatch' | 'damaged' | 'current_unreadable' for an unreadable object."""
        code = str(code or '')
        if code == 'current_unreadable':
            return 'current_unreadable'
        if code in ('json', 'envelope'):
            return 'damaged'
        diag = getattr(self.store, 'diagnose', None)
        if callable(diag):
            try:
                why = diag(name, code)
                if why in ('keys_mismatch', 'damaged'):
                    return why
            except Exception:
                pass
        return 'keys_mismatch'

    def _unreadable_reason(self, exc):
        return self._reason_for(getattr(exc, 'obj', None), str(exc))

    # ── responses ──
    @staticmethod
    def _err(status, code, **extra):
        extra = {k: v for k, v in extra.items() if v is not None}
        body = {'error': message_for(code, extra.get('reason')), 'code': code}
        body.update(extra)
        return _json(body, status)

    def _before(self):
        if _is_pnl_path(request.path or ''):
            g._pnl_t0 = time.perf_counter()
            g._pnl_user = '-'
        return None

    def _after(self, resp):
        path = request.path or ''
        if not _is_pnl_path(path):
            return resp
        resp.headers['Cache-Control'] = 'no-store, private, max-age=0'
        resp.headers['Pragma'] = 'no-cache'
        resp.headers['X-Content-Type-Options'] = 'nosniff'
        resp.vary.add('Authorization')
        # The CORS grant depends on the Origin. flask-cors adds this only when several origins are
        # allowed, and production allows one, so add it here for every P&L response.
        resp.vary.add('Origin')
        if request.method != 'OPTIONS':
            t0 = getattr(g, '_pnl_t0', None)
            ms = int((time.perf_counter() - t0) * 1000) if t0 is not None else -1
            print('[PnL] %s %s %s %dms user=%s' % (request.method, _clean_path(path), resp.status_code,
                                                    ms, getattr(g, '_pnl_user', '-') or '-'), flush=True)
        return resp

    # ── auth ──
    def _auth(self):
        if (request.headers.get('X-Api-Key') or '').strip():
            return None, self._err(401, 'AUTH_REQUIRED')      # the machine key never opens the P&L
        auth = request.headers.get('Authorization') or ''
        token = auth[7:].strip() if auth[:7].lower() == 'bearer ' else ''
        if not _token_shape_ok(token):
            return None, self._err(401, 'AUTH_REQUIRED')
        try:
            ident = self.caller_identity(token)
        except Exception as e:
            print('[PnL] identity check failed: %s' % type(e).__name__, flush=True)
            ident = None
        if not isinstance(ident, dict):
            return None, self._err(401, 'AUTH_REQUIRED')
        email = str(ident.get('email') or '').strip().lower()
        g._pnl_user = _clean_path(email) if email else '-'
        if ident.get('role') != 'staff' or ident.get('is_admin') is not True:
            return None, self._err(403, 'ADMIN_ONLY')
        allow = self._allowlist()
        if not allow:
            return None, self._err(503, 'PNL_NOT_CONFIGURED', reason='allowlist')
        # '@admins' (C14) admits every admin that passed the check above. An email is still required,
        # so every P&L action names its user.
        if not email or (ALLOW_ALL_ADMINS not in allow and email not in allow):
            return None, self._err(403, 'ADMIN_ONLY')
        if self._disabled():
            return None, self._err(503, 'PNL_NOT_CONFIGURED', reason='disabled')
        return {'email': email, 'uid': str(ident.get('uid') or '')}, None

    def _wrap(self, fn):
        @functools.wraps(fn)
        def view(*args, **kwargs):
            if request.method == 'OPTIONS':
                return Response(status=204)
            ident, denied = self._auth()
            if denied is not None:
                return denied
            try:
                return fn(ident, *args, **kwargs)
            except StoreNotConfigured:
                return self._err(503, 'PNL_NOT_CONFIGURED', reason='store')
            except StoreUnreadable as e:
                return self._err(500, 'PNL_STORE_UNREADABLE', reason=self._unreadable_reason(e))
            except StoreNotFound:
                return self._err(404, 'NOT_FOUND')
            except VersionConflict:
                return self._err(409, 'VERSION_CONFLICT')
            except StoreUnavailable:
                return self._err(503, 'PNL_STORE_UNAVAILABLE')
            except (_TooLarge, RequestEntityTooLarge):
                return self._err(413, 'TOO_LARGE')
            except (_BadRequest, HTTPException):
                return self._err(400, 'BAD_REQUEST')
            except Exception as e:
                ref = secrets.token_hex(4)
                print('[PnL] error ref=%s %s %s %s' % (ref, request.method, _clean_path(request.path),
                                                      type(e).__name__), flush=True)
                return self._err(500, 'INTERNAL', ref=ref)
        return view

    # ── request bodies ──
    def _read_json(self, cap):
        length = request.content_length
        if length is not None and length > cap:
            raise _TooLarge()
        data = request.stream.read(cap + 1)
        if len(data) > cap:
            raise _TooLarge()
        if not data:
            raise _BadRequest()

        def no_constants(_name):
            raise ValueError('non-finite')
        try:
            return json.loads(data.decode('utf-8'), parse_constant=no_constants)
        except (ValueError, UnicodeDecodeError, RecursionError):
            raise _BadRequest() from None

    @staticmethod
    def _expected_from(body):
        if not isinstance(body, dict) or 'expected_etag' not in body:
            return None
        v = body.get('expected_etag')
        if v is None or v == '':
            return ''
        if isinstance(v, str) and len(v) <= 200:
            return v.strip().strip('"')
        raise _BadRequest()

    @staticmethod
    def _expected_from_query():
        v = request.args.get('expected_etag')
        if v is None:
            return None
        v = v.strip().strip('"')
        return '' if v.lower() in ('', 'none', 'null') else v[:200]

    @staticmethod
    def _replace_flag(body):
        """replace_unreadable: exactly true in the body (a truly lost key only)."""
        return isinstance(body, dict) and body.get('replace_unreadable') is True

    def _defaults(self):
        eng = self._module('engine')
        d = getattr(eng, 'DEFAULT_SETTINGS', None) if eng is not None else None
        if not isinstance(d, dict):
            return None
        try:
            return json.loads(json.dumps(d, allow_nan=False))
        except (TypeError, ValueError):
            return None

    # ── build ──
    def _gather(self):
        raw = {}
        for key in SOURCE_KEYS:
            fn = self.sources.get(key)
            val = None
            if callable(fn):
                try:
                    val = fn()
                except Exception as e:
                    print('[PnL] source %s failed: %s' % (key, type(e).__name__), flush=True)
            raw[key] = val
            time.sleep(0)            # yield between sources under gevent
        return _normalize_sources(raw)

    def _start_locked(self, key, kind, forced):
        self._job_seq += 1
        job = {'id': self._job_seq, 'key': key, 'kind': kind, 'forced': bool(forced),
               'started': time.time(), 'event': threading.Event()}
        self._job = job
        try:
            threading.Thread(target=self._run_job, args=(job,), daemon=True, name='pnl-build').start()
        except Exception:
            self._job = None
            raise
        return job

    def _record_error(self, job, code, cls, missing=None, reason=None):
        now = time.time()
        with self._lock:
            self._last_error = {'key': job['key'], 'at': now, 'code': code, 'cls': str(cls)[:60],
                                'missing': list(missing or []), 'reason': reason}
            if self._memo is not None:
                self._memo['checked'] = now
        extra = (' missing=' + ','.join(missing)) if missing else ''
        extra += (' reason=' + _code_str(reason)) if reason else ''
        print('[PnL] build failed code=%s class=%s%s' % (code, cls, extra), flush=True)

    def _run_job(self, job):
        t0 = time.perf_counter()
        try:
            src = self._gather()
            stamps = _stamps(src)
            if job['kind'] == 'check':
                with self._lock:
                    memo = self._memo
                    if memo is not None and memo['key'] == job['key'] and memo['stamps'] == stamps:
                        memo['checked'] = time.time()
                        return
            missing = _missing_inputs(src)
            if missing:
                raise _BuildError('INPUTS_UNAVAILABLE', cls='MissingInput', missing=missing)
            engine, routing = self._module('engine'), self._module('routing')
            if engine is None or routing is None:
                raise _BuildError('PNL_NOT_CONFIGURED', cls='ModuleUnavailable',
                                  reason='engine' if engine is None else 'routing')
            chk = _ledger_check(src, routing)
            if _ledger_unusable(chk):
                raise _BuildError('INPUTS_UNAVAILABLE', cls='LedgerUnusable', missing=['ledger'],
                                  reason='ledger_unusable')
            costbook, cb_etag = self.store.get_obj(COSTBOOK)
            if not isinstance(costbook, dict):
                raise _BuildError('NO_COSTBOOK', cls='NoCostbook')
            settings, st_etag = self.store.get_obj(SETTINGS)
            overrides, ov_etag = self.store.get_obj(OVERRIDES)
            now_iso = _utc_iso()
            time.sleep(0)
            ds = engine.build_dataset(src, costbook, settings if isinstance(settings, dict) else {},
                                      overrides if isinstance(overrides, list) else [], now_iso, routing)
            if not isinstance(ds, dict):
                raise _BuildError('BUILD_FAILED', cls='BadShape')
            inputs = ds.get('inputs')
            if not isinstance(inputs, dict):
                inputs = {}
                ds['inputs'] = inputs
            inputs['costbook_etag'] = cb_etag
            inputs['settings_etag'] = st_etag
            inputs['overrides_etag'] = ov_etag
            inputs['ledger_check'] = {k: chk[k] for k in ('rows', 'usableRows', 'usableUnits',
                                                          'atsIncoming', 'unitsVsAts')}
            _input_alerts(ds, chk)
            if not isinstance(ds.get('builtAt'), str):
                ds['builtAt'] = now_iso
            time.sleep(0)
            try:
                body = json.dumps(ds, separators=(',', ':'), allow_nan=False,
                                  default=_json_default).encode('utf-8')
            except (TypeError, ValueError):
                raise _BuildError('BUILD_FAILED', cls='NotSerializable') from None
            time.sleep(0)
            gz = gzip.compress(body, compresslevel=6, mtime=0)
            with self._lock:
                self._memo = {'key': (cb_etag, st_etag, ov_etag), 'stamps': stamps, 'body': body, 'gz': gz,
                              'started': job['started'], 'checked': time.time(), 'builtAt': ds['builtAt']}
                self._last_error = None
            print('[PnL] build ok %dms bytes=%d gzip=%d' % (int((time.perf_counter() - t0) * 1000),
                                                          len(body), len(gz)), flush=True)
        except _BuildError as e:
            self._record_error(job, e.code, e.cls, missing=e.missing, reason=e.reason)
        except StoreNotConfigured:
            self._record_error(job, 'PNL_NOT_CONFIGURED', 'StoreNotConfigured', reason='store')
        except StoreUnreadable as e:
            self._record_error(job, 'PNL_STORE_UNREADABLE', 'StoreUnreadable', reason=self._unreadable_reason(e))
        except StoreError as e:
            self._record_error(job, 'PNL_STORE_UNAVAILABLE', type(e).__name__)
        except Exception as e:
            self._record_error(job, 'BUILD_FAILED', type(e).__name__)
        finally:
            with self._lock:
                if self._job is job:
                    self._job = None
            job['event'].set()

    @staticmethod
    def _memo_response(memo):
        gz = memo.get('gz')
        try:
            use_gz = gz is not None and request.accept_encodings.quality('gzip') > 0
        except Exception:
            use_gz = False
        resp = Response(gz if use_gz else memo['body'], status=200, mimetype='application/json')
        if use_gz:
            resp.headers['Content-Encoding'] = 'gzip'
        resp.vary.add('Accept-Encoding')
        return resp

    def _build_error_response(self, err):
        code = err.get('code')
        if code == 'INPUTS_UNAVAILABLE':
            return self._err(503, 'INPUTS_UNAVAILABLE', missing=list(err.get('missing') or []),
                             reason=err.get('reason'))
        if code == 'NO_COSTBOOK':
            return self._err(409, 'NO_COSTBOOK')
        if code == 'PNL_NOT_CONFIGURED':
            return self._err(503, 'PNL_NOT_CONFIGURED', reason=err.get('reason') or 'engine')
        if code == 'PNL_STORE_UNREADABLE':
            return self._err(500, 'PNL_STORE_UNREADABLE', reason=err.get('reason'))
        if code == 'PNL_STORE_UNAVAILABLE':
            return self._err(503, 'PNL_STORE_UNAVAILABLE')
        return self._err(500, 'BUILD_FAILED')

    def _build_status(self, key=None):
        """state: idle | building | ready | stale | error. key: the current object etags
        (None when the store could not be asked)."""
        with self._lock:
            job, memo, err = self._job, self._memo, self._last_error
        if job:
            state = 'building'
        elif key is None:
            state = 'ready' if memo else ('error' if err else 'idle')
        elif memo is not None and memo['key'] == key:
            # served; 'stale' when a later rebuild for these same objects failed
            state = 'stale' if (err is not None and err['key'] == key and err['at'] >= memo['started']) else 'ready'
        elif err is not None and err['key'] == key:
            state = 'error'
        elif memo is not None:
            state = 'stale'              # built for older objects; the next request rebuilds
        else:
            state = 'idle'
        error = None
        if err:
            error = {'code': err['code'], 'class': err['cls'], 'missing': err['missing'] or None}
            if err.get('reason'):
                error['reason'] = err['reason']
        return {'state': state,
                'startedAt': _iso(job['started']) if job else None,
                'builtAt': memo['builtAt'] if memo else None,
                'error': error}

    def _dataset_for(self, key, want_fresh):
        now = time.time()
        with self._lock:
            job, memo = self._job, self._memo
            usable = memo is not None and memo['key'] == key
            if job is None:
                refresh_due = (want_fresh and usable and memo['started'] < now - self.refresh_window
                               and now - self._last_forced >= self.refresh_window)
                if usable and not refresh_due:
                    if now - memo['checked'] >= self.recheck_seconds:
                        self._start_locked(key, 'check', forced=False)
                    return self._memo_response(memo)
                err = self._last_error
                if (not usable and not want_fresh and err is not None and err['key'] == key
                        and now - err['at'] < self.error_retry_seconds):
                    return self._build_error_response(err)
                if refresh_due:
                    self._last_forced = now
                job = self._start_locked(key, 'build', forced=refresh_due)
            elif usable and not job['forced']:
                return self._memo_response(memo)
        job['event'].wait(self.wait_seconds)
        with self._lock:
            memo = self._memo
            if memo is not None and memo['key'] == key and (not job['forced'] or memo['started'] >= job['started']):
                return self._memo_response(memo)
            running = self._job
            if running is not None:
                return _json({'building': True, 'startedAt': _iso(running['started'])}, 202)
            err = self._last_error
            if err is not None and err['key'] == key and err['at'] >= job['started']:
                return self._build_error_response(err)
        return _json({'building': True, 'startedAt': _iso(job['started'])}, 202)

    def _costbook_summary(self, etag):
        with self._lock:
            hit = self._cb_summary.get(etag)
        if hit is not None:
            return dict(hit)
        try:
            cb, got = self.store.get_obj(COSTBOOK)
        except StoreUnreadable:
            return {'readable': False}
        except StoreError:
            return {}
        if not isinstance(cb, dict):
            return {'readable': False}
        recs = cb.get('records')
        summary = {'generatedAt': cb.get('generatedAt') if isinstance(cb.get('generatedAt'), str) else None,
                   'records': len(recs) if isinstance(recs, list) else 0, 'readable': True}
        with self._lock:
            self._cb_summary = {got: summary}
        return dict(summary)

    def _param_changes_vs_stored(self, clean):
        """Changed params paths between the stored cost book and `clean`. [] on a first upload
        or when the stored one cannot be read (put_obj then decides)."""
        try:
            old, _etag = self.store.get_obj(COSTBOOK)
        except StoreUnreadable:
            return []
        if not isinstance(old, dict):
            return []
        old_p = old.get('params') if isinstance(old.get('params'), dict) else {}
        return _param_changes(old_p, clean.get('params') or {})

    def _check_store_once(self):
        """Background check after boot: do the configured keys open the saved objects?
        Logs object labels, codes and key ids only (never content)."""
        if not self._store_ready() or not callable(getattr(self.store, 'readable', None)):
            return
        parts, bad = [], False
        for name, label in _LABELS:
            try:
                etag, ok, code = self.store.readable(name)
            except StoreError:
                parts.append('%s=unavailable' % label)
                continue
            if etag is None:
                parts.append('%s=absent' % label)
            elif ok:
                parts.append('%s=ok' % label)
            else:
                bad = True
                kid = self._object_kid(name)
                parts.append('%s=%s%s' % (label, self._reason_for(name, code), (':needs_' + kid) if kid else ''))
        ids = ','.join(getattr(self.store, 'key_ids', None) or []) or '-'
        if bad:
            print('[PnL] store check: the keys do not open the saved objects | %s | keys %s '
                  '| see the KEY RUNBOOK in pnl_store.py' % (' '.join(parts), ids), flush=True)
        else:
            print('[PnL] store check ok | %s' % ' '.join(parts), flush=True)

    # ── route handlers (auth already passed; ident = {'email', 'uid'}) ──
    def h_status(self, ident):
        reasons = self._config_reasons()
        objects = {label: {'present': None, 'etag': None, 'readable': None, 'kid': None, 'keyKnown': None}
                   for _name, label in _LABELS}
        objects['costbook'].update({'generatedAt': None, 'records': None})
        objects['overrides']['count'] = None
        key = None
        ids = list(getattr(self.store, 'key_ids', None) or []) if self.store is not None else []
        if self.store is not None:
            try:
                etags = {n: self.store.head(n) for n in OBJECTS}
            except StoreNotConfigured:
                etags = None                       # already reported as store:<reason>
            except StoreError:
                etags = None
                reasons.append('store:unavailable')
            if etags is not None:
                key = tuple(etags[n] for n in OBJECTS)
                ready = self._store_ready() and callable(getattr(self.store, 'readable', None))
                found = set()
                for n, label in _LABELS:
                    o = objects[label]
                    o['present'] = etags[n] is not None
                    o['etag'] = etags[n]
                    if etags[n] is None:
                        continue
                    o['kid'] = self._object_kid(n)
                    o['keyKnown'] = (o['kid'] in ids) if o['kid'] else None
                    if not ready:
                        continue
                    try:
                        _etag, ok, code = self.store.readable(n)
                    except StoreError:
                        continue
                    o['readable'] = ok
                    if ok is False:
                        o['problem'] = self._reason_for(n, code)
                        found.add(o['problem'])
                if ready:
                    if etags[COSTBOOK] and objects['costbook']['readable']:
                        objects['costbook'].update(self._costbook_summary(etags[COSTBOOK]))
                    if etags[OVERRIDES] is None:
                        objects['overrides']['count'] = 0
                    elif objects['overrides']['readable']:
                        try:
                            ov, _etag = self.store.get_obj(OVERRIDES)
                            objects['overrides']['count'] = len(ov) if isinstance(ov, list) else 0
                        except StoreError:
                            pass
                for why in ('keys_mismatch', 'damaged'):
                    if why in found:
                        reasons.append('store:' + why)
        keys = {'count': int(getattr(self.store, 'key_count', 0) or 0) if self.store is not None else 0,
                'ids': ids}
        return _json({'configured': not reasons, 'reasons': reasons, 'you': {'email': ident['email']},
                      'objects': objects, 'keys': keys, 'build': self._build_status(key),
                      'versions': bool(self.store is not None and self.store.supports_versions())})

    def h_dataset(self, ident):
        self._require_store()
        for kind in ('engine', 'routing'):
            if self._module(kind) is None:
                return self._err(503, 'PNL_NOT_CONFIGURED', reason=kind)
        key = tuple(self.store.head(n) for n in OBJECTS)
        if key[0] is None:
            return self._err(409, 'NO_COSTBOOK')
        return self._dataset_for(key, request.args.get('refresh') == '1')

    def h_analytics(self, ident):
        """GET /api/pnl/analytics: the invoiced-history cube (customer x base
        style x month, uncapped, from the open-orders service) joined with the
        factory cost maps derived from the built dataset. Feeds the admin-only
        Inventory Analytics tool. The cube itself carries no cost (units and
        invoiced selling value); the cost maps come from the encrypted cost
        book through the dataset memo, so this route shares the dataset's
        freshness and never builds or fetches on the request thread."""
        self._require_store()
        for kind in ('engine', 'routing'):
            if self._module(kind) is None:
                return self._err(503, 'PNL_NOT_CONFIGURED', reason=kind)
        key = tuple(self.store.head(n) for n in OBJECTS)
        if key[0] is None:
            return self._err(409, 'NO_COSTBOOK')
        get_matrix = self.sales_matrix
        matrix = get_matrix() if callable(get_matrix) else None
        if matrix is None:
            return self._err(503, 'INPUTS_UNAVAILABLE', missing=['sales_matrix'], reason='sales_matrix')
        if matrix.get('building'):
            return _json({'building': True, 'part': 'matrix'}, 202)
        cost = self._analytics_cost_maps(key)
        if cost is None:
            r = self._dataset_for(key, False)
            if r.status_code == 200:
                cost = self._analytics_cost_maps(key)
            elif r.status_code != 202:
                return r
            if cost is None:
                return _json({'building': True, 'part': 'dataset'}, 202)
        out = {'ready': True,
               'matrix': {'customers': matrix.get('customers') or {},
                          'source': matrix.get('source') or {}},
               'pending': matrix.get('pending'),
               'pendingReady': bool(matrix.get('pendingReady')),
               'history': matrix.get('history'),
               'custAlias': cost['alias'],
               'costByCustomer': cost['byCust'],
               'cost2ByCustomer': cost.get('byCust2') or {},
               'costByStyle': cost['byStyle'],
               'costGrades': cost['grades'],
               'datasetBuiltAt': cost['builtAt']}
        body = json.dumps(out, separators=(',', ':'), allow_nan=False, default=_json_default).encode('utf-8')
        gz = gzip.compress(body, compresslevel=6, mtime=0)
        try:
            use_gz = request.accept_encodings.quality('gzip') > 0
        except Exception:
            use_gz = False
        resp = Response(gz if use_gz else body, status=200, mimetype='application/json')
        if use_gz:
            resp.headers['Content-Encoding'] = 'gzip'
        resp.vary.add('Accept-Encoding')
        return resp

    def _analytics_cost_maps(self, key):
        """Cost joins for h_analytics, derived at most once per dataset build:
        fobU per (account, base) from shipped.byCustomer (covers every
        historically invoiced pair, including customer-group price grids),
        base-level fobU and grade from the styles table (current activity),
        and the history customer alias fold. None when no dataset memo for
        this key exists yet."""
        with self._lock:
            memo = self._memo
            hit = self._an_memo
            if memo is None or memo['key'] != key:
                return hit if (hit is not None and hit['key'] == key) else None
            if hit is not None and hit['key'] == key and hit['builtAt'] == memo['builtAt']:
                return hit
            body, built_at = memo['body'], memo['builtAt']
        ds = json.loads(body)
        eng = self._module('engine')
        alias = dict(getattr(eng, 'HISTORY_CUSTOMER_ALIAS', None) or {})
        by_cust = {}
        by_cust2 = {}
        bc = (ds.get('shipped') or {}).get('byCustomer') or {}
        f = {n: i for i, n in enumerate(bc.get('fields') or [])}
        if 'cust' in f and 'base' in f and 'fobU' in f:
            have2 = all(k in f for k in ('units', 'fob', 'duty', 'freight', 'fees'))
            for row in bc.get('rows') or []:
                fob = row[f['fobU']]
                if fob is not None:
                    by_cust.setdefault(row[f['cust']], {})[row[f['base']]] = fob
                # Per HISTORY unit (kit cartons stay cartons: the byCustomer totals
                # already carry the kit piece scaling), so the client multiplies by
                # the cube's own units: [factory cost per unit, import add per unit].
                # The import add is the engine's duty + freight + fees for THIS
                # customer and style (the customs formula; an FOB account gets 0).
                if have2:
                    u = row[f['units']] or 0
                    tot = row[f['fob']]
                    if u and tot is not None:
                        imp = (row[f['duty']] or 0) + (row[f['freight']] or 0) + (row[f['fees']] or 0)
                        by_cust2.setdefault(row[f['cust']], {})[row[f['base']]] = [
                            round(tot / u, 4), round(imp / u, 4)]
        by_style, grades = {}, {}
        st = ds.get('styles') or {}
        sf = {n: i for i, n in enumerate(st.get('fields') or [])}
        if 'base' in sf and 'fobU' in sf:
            for row in st.get('rows') or []:
                fob = row[sf['fobU']]
                if fob is not None:
                    by_style[row[sf['base']]] = fob
                    if 'grade' in sf:
                        grades[row[sf['base']]] = row[sf['grade']]
        hit = {'key': key, 'builtAt': built_at, 'alias': alias, 'byCust': by_cust,
               'byCust2': by_cust2, 'byStyle': by_style, 'grades': grades}
        with self._lock:
            self._an_memo = hit
        return hit

    def h_costbook(self, ident):
        self._require_store()
        if request.method != 'POST':
            cb, etag = self.store.get_obj(COSTBOOK)
            return _json({'costbook': cb, 'etag': etag})
        body = self._read_json(self.max_costbook_bytes)
        if not isinstance(body, dict):
            raise _BadRequest()
        if isinstance(body.get('costbook'), dict):
            cb, expected, replace = body['costbook'], self._expected_from(body), self._replace_flag(body)
        else:
            cb, expected = body, self._expected_from_query()
            replace = request.args.get('replace_unreadable') == '1'
        clean, problems, warnings, dropped = validate_costbook(cb)
        if problems:
            return self._err(422, 'INVALID_COSTBOOK', problems=problems[:50], problemCount=len(problems))
        changes = [] if replace else self._param_changes_vs_stored(clean)
        etag = self.store.put_obj(COSTBOOK, clean, expected_etag=expected, replace_unreadable=replace)
        print('[PnL] costbook saved records=%d params_changed=%d%s user=%s' % (
            len(clean['records']), len(changes), ' replaced_unreadable=yes' if replace else '',
            ident['email']), flush=True)
        allw = changes + warnings
        return _json({'etag': etag, 'records': len(clean['records']), 'warnings': allw[:50],
                      'warningCount': len(allw), 'dropped': dropped[:50],
                      'paramChanges': changes[:50], 'paramChangeCount': len(changes)})

    def h_versions(self, ident):
        self._require_store()
        if not self.store.supports_versions():
            return _json({'supported': False, 'versions': []})
        return _json({'supported': True, 'versions': self.store.versions(COSTBOOK) or []})

    def h_restore(self, ident):
        self._require_store()
        body = self._read_json(self.max_small_bytes)
        vid = body.get('version_id') if isinstance(body, dict) else None
        if not isinstance(vid, str) or not vid or len(vid) > 1024:
            raise _BadRequest()
        expected = self._expected_from(body)
        cb = self.store.get_version_obj(COSTBOOK, vid)
        clean, problems, _warnings, _dropped = validate_costbook(cb)
        if problems:
            return self._err(422, 'INVALID_COSTBOOK', problems=problems[:50], problemCount=len(problems))
        changes = self._param_changes_vs_stored(clean)
        etag = self.store.put_obj(COSTBOOK, clean, expected_etag=expected)
        print('[PnL] costbook restored records=%d params_changed=%d user=%s' % (
            len(clean['records']), len(changes), ident['email']), flush=True)
        return _json({'etag': etag, 'records': len(clean['records']), 'paramChanges': changes[:50],
                      'paramChangeCount': len(changes)})

    def h_settings(self, ident):
        self._require_store()
        defaults = self._defaults()
        if request.method != 'POST':
            st, etag = self.store.get_obj(SETTINGS)
            return _json({'settings': st, 'defaults': defaults, 'etag': etag})
        body = self._read_json(self.max_settings_bytes)
        if not isinstance(body, dict) or not isinstance(body.get('settings'), dict):
            raise _BadRequest()
        expected = self._expected_from(body)
        replace = self._replace_flag(body)
        clean, problems, dropped = validate_settings(body['settings'], defaults)
        if problems:
            return self._err(422, 'INVALID_SETTINGS', problems=problems[:50], problemCount=len(problems))
        clean['updatedAt'] = _utc_iso()
        clean['updatedBy'] = ident['email']
        etag = self.store.put_obj(SETTINGS, clean, expected_etag=expected, replace_unreadable=replace)
        if replace:
            print('[PnL] settings saved replaced_unreadable=yes user=%s' % ident['email'], flush=True)
        return _json({'etag': etag, 'settings': clean, 'dropped': dropped[:50]})

    def h_overrides(self, ident):
        self._require_store()
        if request.method != 'POST':
            current, etag = self.store.get_obj(OVERRIDES)
            return _json({'overrides': current if isinstance(current, list) else [], 'etag': etag})
        body = self._read_json(self.max_overrides_bytes)
        if not isinstance(body, dict) or not isinstance(body.get('overrides'), list):
            raise _BadRequest()
        expected = self._expected_from(body)
        replace = self._replace_flag(body)
        current = [] if replace else self.store.get_obj(OVERRIDES)[0]
        clean, problems, dropped = validate_overrides(body['overrides'],
                                                      current if isinstance(current, list) else [],
                                                      ident['email'], _utc_iso())
        if problems:
            return self._err(422, 'INVALID_OVERRIDES', problems=problems[:50], problemCount=len(problems))
        new_etag = self.store.put_obj(OVERRIDES, clean, expected_etag=expected, replace_unreadable=replace)
        if replace:
            print('[PnL] overrides saved replaced_unreadable=yes user=%s' % ident['email'], flush=True)
        return _json({'etag': new_etag, 'count': len(clean), 'dropped': dropped[:50]})

    def h_audit(self, ident):
        body = self._read_json(self.max_small_bytes)
        action = body.get('action') if isinstance(body, dict) else None
        if not isinstance(action, str) or action not in _AUDIT_ACTIONS:
            raise _BadRequest()
        print('[PnL] audit action=%s detail=%s user=%s' % (action, _safe_detail(body.get('detail')),
                                                          ident['email']), flush=True)
        return Response(status=204)

    def h_rotate(self, ident):
        self._require_store()
        result = {}
        for name in OBJECTS:
            result[name] = 'rotated' if self.store.rotate(name) else 'absent'
        with self._lock:
            self._cb_summary = {}
        print('[PnL] store rotated user=%s' % ident['email'], flush=True)
        return _json({'objects': result, 'keys': getattr(self.store, 'key_count', 0)})

    def h_not_found(self, ident, rest=None):
        return self._err(404, 'NOT_FOUND')

    def routes(self):
        handlers = {
            '/api/pnl/status': ('pnl_status', self.h_status),
            '/api/pnl/dataset': ('pnl_dataset', self.h_dataset),
            '/api/pnl/costbook': ('pnl_costbook', self.h_costbook),
            '/api/pnl/costbook/versions': ('pnl_costbook_versions', self.h_versions),
            '/api/pnl/costbook/restore': ('pnl_costbook_restore', self.h_restore),
            '/api/pnl/settings': ('pnl_settings', self.h_settings),
            '/api/pnl/overrides': ('pnl_overrides', self.h_overrides),
            '/api/pnl/audit': ('pnl_audit', self.h_audit),
            '/api/pnl/rotate': ('pnl_rotate', self.h_rotate),
            '/api/pnl/analytics': ('pnl_analytics', self.h_analytics),
            '/api/pnl': ('pnl_root', self.h_not_found),
            '/api/pnl/<path:rest>': ('pnl_unknown', self.h_not_found),
        }
        for rule, methods in ROUTE_TABLE:
            endpoint, fn = handlers[rule]
            yield rule, endpoint, list(methods) + ['OPTIONS'], fn


def register_pnl_routes(app, *, store, caller_identity, sources, env=os.environ, engine=None, routing=None,
                        store_check=True):
    """Register /api/pnl/* on `app`. Never raises for a missing key, engine or routing
    module: those routes answer 503 PNL_NOT_CONFIGURED instead. Returns the service."""
    svc = _PnlService(store, caller_identity, sources, env, engine, routing)
    app.before_request(svc._before)
    app.after_request(svc._after)
    for rule, endpoint, methods, fn in svc.routes():
        app.add_url_rule(rule, endpoint, svc._wrap(fn), methods=methods)
    _SERVICES.append(svc)
    if store is None:
        state = 'NOT CONFIGURED (no store)'
    elif getattr(store, 'ok', False):
        n_keys = int(getattr(store, 'key_count', 0) or 0)
        state = 'ready (%d key%s, newest %s)' % (n_keys, '' if n_keys == 1 else 's',
                                                 getattr(store, 'fingerprint', None) or '-')
    else:
        state = 'NOT CONFIGURED (%s)' % _code_str(getattr(store, 'reason', ''))
    allow = svc._allowlist()
    print('[PnL] routes registered | store %s | allowlist %s | %s' % (
        state, 'all admins' if ALLOW_ALL_ADMINS in allow else len(allow),
        'DISABLED by PNL_ENABLED' if svc._disabled() else 'enabled'), flush=True)
    if store_check and svc._store_ready():
        # A valid but wrong key passes the store self-test, so "store ready" above cannot prove
        # the keys open the saved data. Check once in the background; never at import time.
        def later():
            time.sleep(STORE_CHECK_DELAY)
            try:
                svc._check_store_once()
            except Exception as e:
                print('[PnL] store check failed: %s' % type(e).__name__, flush=True)
        try:
            threading.Thread(target=later, daemon=True, name='pnl-store-check').start()
        except Exception:
            pass
    return svc
