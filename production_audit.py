"""
PRODUCTION AUDIT: what changed on the Style Ledger, day by day (David, Nov 9 2026).

The phone app's Production Audit tab asks "what changed on the ledger today / this week /
this month, for one factory or all". This module keeps the answer.

How the history is kept
  * A copy of the ledger per Eastern day ("day snapshot"): one line per production # + style
    (the ledger can hold the same style twice on one production, e.g. part shipped; those rows
    are summed, the way the hourly ledger tracker counts them).
      - From Nov 9 2026 on, the leader worker captures the API's own parsed ledger every
        PA_CAPTURE_SECONDS and rewrites today's copy whenever the ledger changed. The last
        capture of a day is that day's copy. These copies hold every column the audit reads:
        units, PO name, brand, ex-factory (column F as typed), shipment # (H), port date (G),
        landing warehouse (I).
      - Days before that come from the hourly ledger tracker (production-inventory/snapshot.json,
        S3 versioning keeps every copy since Apr 15 2026): the last sane copy of each Eastern day.
        The tracker only keeps units, PO name, brand and ex-factory, so older days compare
        those four.
  * A day's changes = that day's copy against the copy before it. Today's changes are worked
    out live against yesterday's copy.

What counts as a change, per production # + style
  new   the line appeared (np = its production # is new on the ledger that day)
  rem   the line left the ledger (gone = its production # has no lines left)
  chg   units, ex-factory, PO name, brand, shipment #, port date or warehouse changed
  A style that left (or lost units on) one production and arrived on (or gained units on)
  another the same day carries mv (moved to) / mvf (moved from).

Storage: the bucket is publicly readable, so every object lives under a prefix derived from the
machine key and is Fernet-encrypted with a key derived from it (the email agent's scheme, with
its own domain separation). Nothing here is ever written in plain text.

Routes (Versa-Docs staff session or the machine key; held factories are left out, as in /production)
  GET  /production-audit?from=YYYY-MM-DD&to=YYYY-MM-DD   changes per day in the range (max 62 days)
  GET  /production-audit/since?from=&to=&po=             everything from the start of a day until now (or `to`):
                                                         the ledger then against the ledger at the end, per line,
                                                         with how often each changed in between (steps for one PO)
  GET  /production-audit/status                          capture + backfill state
  POST /production-audit/backfill  {start?, force?}      machine key only: rebuild day copies from
                                                         the tracker and the changes from them
"""
import base64
import hashlib
import hmac
import json
import os
import re
import threading
import time
import traceback
import datetime as dt
from collections import defaultdict

from flask import request, jsonify

import worker_leader

PA_TRACKER_KEY = os.environ.get('PA_TRACKER_KEY', 'production-inventory/snapshot.json')
PA_CAPTURE_SECONDS = int(os.environ.get('PA_CAPTURE_SECONDS', '600'))
PA_HISTORY_START = dt.date(2026, 4, 16)     # the tracker's first full copy (Apr 15 was one row per PO)
PA_MAX_RANGE_DAYS = 62
PA_EVENTS_CACHE_DAYS = 400                  # day-change docs kept per worker (a few MB; ranges reuse them)
PA_PARALLEL_READS = int(os.environ.get('PA_PARALLEL_READS', '24'))   # S3 reads at once for a long range
PA_WARM_AFTER_SECONDS = 150                 # each worker reads the whole history once after it starts
PA_BASE_LOOKBACK_DAYS = 10                  # how far back a day looks for the copy before it
PA_DROP_GUARD = 0.70                        # a copy with fewer lines than this share of the last one is suspect
PA_DROP_CONFIRM_SECONDS = 1800              # ... and is only accepted once it has held for 30 minutes

OWN_FIELDS = ('u', 'pn', 'b', 'e', 'sh', 'g', 'w')
# The tracker keeps the PO name a line had when it was first seen (about a third of its lines
# differed from the ledger on Nov 9 2026), so tracker days never compare PO names. Brand is kept
# but only compared between our own copies (see diff_snapshots).
TRACKER_FIELDS = ('u', 'b', 'e')
FACTORY_NAMES = {'TF': 'Top Find', 'NB': 'Yuxiu', 'PC': 'Pinnacle', 'DP': 'David Peng', 'FR': 'Frank', 'NK': 'Najmul'}

_deps = {}
_mem_lock = threading.Lock()
_mem = {
    'days': {},          # day iso -> snapshot (past days only; immutable once the day is over)
    'events': {},        # day iso -> events doc (past days only)
    'live': None,        # (key, doc) for today's live changes
    'state': None,
    'state_at': 0.0,
}
_backfill_lock = threading.Lock()
_backfill = {'running': False, 'started': None, 'finished': None, 'progress': '', 'error': None, 'counts': None}
_capture_thread_started = False
_last_capture = {'at': None, 'result': None, 'error': None}


# ─────────────────────────────────────────────────────────────────────────────
# Eastern time (zoneinfo when the system has tz data, else the US DST rule)
# ─────────────────────────────────────────────────────────────────────────────
def _et_offset_fallback(utc_now):
    y = utc_now.year
    # DST: 2nd Sunday of March 07:00 UTC (2 AM EST) to 1st Sunday of November 06:00 UTC (2 AM EDT)
    mar = dt.datetime(y, 3, 8, tzinfo=dt.timezone.utc)
    start = mar + dt.timedelta(days=(6 - mar.weekday()) % 7, hours=7)
    nov = dt.datetime(y, 11, 1, tzinfo=dt.timezone.utc)
    end = nov + dt.timedelta(days=(6 - nov.weekday()) % 7, hours=6)
    return dt.timedelta(hours=-4 if start <= utc_now < end else -5)


def _et_zone():
    try:
        from zoneinfo import ZoneInfo
        return ZoneInfo('America/New_York')
    except Exception:
        return None


def et_now():
    z = _et_zone()
    utc = dt.datetime.now(dt.timezone.utc)
    if z is not None:
        return utc.astimezone(z)
    return utc.astimezone(dt.timezone(_et_offset_fallback(utc)))


def et_today():
    return et_now().date()


def et_day_of(ts):
    """A datetime (aware or naive UTC) or ISO string -> its Eastern calendar day."""
    if isinstance(ts, str):
        ts = dt.datetime.fromisoformat(ts.replace('Z', '+00:00'))
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=dt.timezone.utc)
    z = _et_zone()
    if z is not None:
        return ts.astimezone(z).date()
    utc = ts.astimezone(dt.timezone.utc)
    return utc.astimezone(dt.timezone(_et_offset_fallback(utc))).date()


def _iso(d):
    return d.isoformat() if d else None


def _parse_day(s):
    try:
        return dt.date.fromisoformat(str(s or '').strip()[:10])
    except ValueError:
        return None


# ─────────────────────────────────────────────────────────────────────────────
# Building a day copy: ledger rows -> lines keyed "PRODUCTION::STYLE"
# ─────────────────────────────────────────────────────────────────────────────
_REF_RE = re.compile(r'^([A-Z]{2})\d')


def norm_prod(v):
    return re.sub(r'\s+', ' ', str(v or '').strip().upper())


def factory_of(prod):
    m = _REF_RE.match(prod or '')
    return m.group(1) if m else ''


def norm_etd(v):
    """Column F as a comparable value: ISO when it reads as a date (2026-10-22, 10/22/2026,
    ' 2026/10/1' and '2026/10//15' all do), else the text itself (TBD, 10/30-11/30, be ready)."""
    if v is None:
        return None
    if hasattr(v, 'strftime'):
        try:
            return v.strftime('%Y-%m-%d')
        except Exception:
            return None
    s = str(v).strip()
    if not s:
        return None
    norm = _deps.get('norm_date')
    for cand in (s, re.sub(r'([/\-.])\1+', r'\1', s)):
        if norm:
            iso = norm(cand)
            if iso:
                return iso
        for fmt in ('%Y-%m-%d', '%m/%d/%Y', '%m/%d/%y', '%Y/%m/%d', '%m-%d-%Y', '%m-%d-%y', '%m.%d.%Y'):
            try:
                return dt.datetime.strptime(cand.split(' ')[0], fmt).strftime('%Y-%m-%d')
            except ValueError:
                continue
    return s.upper()[:40]


def _top(weights):
    """The value with the most units (ties: the alphabetically first, so copies are stable)."""
    if not weights:
        return ''
    return sorted(weights.items(), key=lambda kv: (-kv[1], kv[0]))[0][0]


def lines_from_rows(rows):
    """The API's parsed ledger rows (load_production_from_dropbox, held factories included)."""
    agg = {}
    for r in rows or []:
        prod = norm_prod(r.get('production'))
        style = str(r.get('style') or '').strip().upper()
        if not prod or not style:
            continue
        k = prod + '::' + style
        a = agg.get(k)
        if a is None:
            a = agg[k] = {'p': prod, 's': style, 'u': 0, '_po': {}, '_b': {}, 'e': set(), 'sh': set(),
                          'g': set(), '_w': {}, 'n': 0}
        try:
            u = int(r.get('units') or 0)
        except (TypeError, ValueError):
            u = 0
        a['u'] += u
        a['n'] += 1
        wgt = max(u, 0) + 1
        po = str(r.get('poName') or '').strip()
        if po:
            a['_po'][po] = a['_po'].get(po, 0) + wgt
        b = str(r.get('brand') or '').strip()
        if b:
            a['_b'][b] = a['_b'].get(b, 0) + wgt
        e = norm_etd(r.get('etd_raw') or r.get('fob_note') or None)
        if e:
            a['e'].add(e)
        sh = str(r.get('shipmentNo') or '').strip()
        if sh:
            a['sh'].add(sh)
        if r.get('port_dated') and r.get('arrival'):
            arr = _parse_day(r.get('arrival'))
            if arr:
                a['g'].add((arr - dt.timedelta(days=10)).isoformat())   # the loader stores G + 10
        w = str(r.get('warehouse') or '').strip().upper()
        if w:
            a['_w'][w] = a['_w'].get(w, 0) + wgt
    out = {}
    for k, a in agg.items():
        out[k] = {'p': a['p'], 's': a['s'], 'u': a['u'], 'po': _top(a['_po']), 'pn': sorted(a['_po']),
                  'b': _top(a['_b']), 'e': sorted(a['e']), 'sh': sorted(a['sh']),
                  'g': min(a['g']) if a['g'] else None, 'w': _top(a['_w']), 'n': a['n']}
    return out


def lines_from_tracker(obj):
    """One version of the hourly tracker -> the lines still on the ledger at that moment.

    The tracker keys a line PRODUCTION::STYLE. For a few days in April 2026 it also wrote
    PRODUCTION::STYLE::ETD keys, and both kinds stayed live side by side (the Apr 16-19 copies
    hold every line twice). So per production + style: the plain entry when there is one (the
    latest one when spacing variants repeat it), else the dated entries added up (one per date)."""
    brand_of = _deps.get('brand_of')
    groups = defaultdict(list)
    for key, e in ((obj or {}).get('orders') or {}).items():
        if e.get('fell_off_date'):
            continue
        prod = norm_prod(e.get('production'))
        style = str(e.get('style') or '').strip().upper()
        if not prod or not style:
            continue
        groups[(prod, style)].append((str(key), e))
    out = {}
    for (prod, style), items in groups.items():
        plain = [(k, e) for k, e in items if k.count('::') < 2]
        if plain:
            use = [max(plain, key=lambda ke: str(ke[1].get('last_seen') or ke[1].get('last_seen_on_ledger') or ''))]
        else:
            use = items
        units, etds, brand, po = 0, set(), '', ''
        for _k, e in use:
            try:
                units += int(e.get('units') or 0)
            except (TypeError, ValueError):
                pass
            etd = norm_etd(e.get('etd'))
            if etd:
                etds.add(etd)
            brand = brand or str(e.get('brand') or '').strip()
            po = po or str(e.get('po_name') or '').strip()
        if brand_of:
            try:
                brand = brand_of(style, brand) or brand
            except Exception:
                pass
        out[prod + '::' + style] = {'p': prod, 's': style, 'u': units, 'po': po, 'b': brand, 'e': sorted(etds)}
    return out


def snapshot_sig(lines):
    h = hashlib.sha256()
    for k in sorted(lines):
        h.update(json.dumps(lines[k], sort_keys=True, default=str).encode('utf-8'))
    return h.hexdigest()[:24]


# ─────────────────────────────────────────────────────────────────────────────
# The diff: two day copies -> the changes between them
# ─────────────────────────────────────────────────────────────────────────────
def _etd_changed(a, b, exact):
    a, b = list(a or []), list(b or [])
    if exact:
        return a != b
    # A tracker copy keeps one row's column F per line, so against a full copy it only
    # counts as a change when none of the line's dates is the same.
    if not a and not b:
        return False
    if not a or not b:
        return True
    return not (set(a) & set(b))


def diff_snapshots(base, cur):
    """(events, pos) for the change from copy `base` to copy `cur`.
    events: list of dicts (see the module docstring); pos: {prod: {po, u0, u1, n0, n1}}."""
    bl = (base or {}).get('lines') or {}
    cl = (cur or {}).get('lines') or {}
    common = set((base or {}).get('fields') or ()) & set((cur or {}).get('fields') or ())
    exact_etd = (base or {}).get('source') == 'own' and (cur or {}).get('source') == 'own'
    tot0, tot1 = defaultdict(int), defaultdict(int)
    n0, n1 = defaultdict(int), defaultdict(int)
    for l in bl.values():
        tot0[l['p']] += l['u']
        n0[l['p']] += 1
    for l in cl.values():
        tot1[l['p']] += l['u']
        n1[l['p']] += 1
    events = []
    for k in sorted(set(bl) | set(cl)):
        a, b = bl.get(k), cl.get(k)
        src = b or a
        ev = {'p': src['p'], 's': src['s'], 'f': factory_of(src['p']), 'b': src.get('b') or '',
              'po': src.get('po') or ''}
        if a and b:
            ch = False
            if a['u'] != b['u']:
                ev['u0'], ev['u1'] = a['u'], b['u']
                ch = True
            if 'e' in common and _etd_changed(a.get('e'), b.get('e'), exact_etd):
                ev['e0'], ev['e1'] = a.get('e') or [], b.get('e') or []
                ch = True
            # PO names compare as a set: a line split over rows with different names (some
            # productions carry one buyer PO per row) must not flip when units move between them.
            if 'pn' in common and set(a.get('pn') or []) != set(b.get('pn') or []):
                ev['po0'] = ', '.join(a.get('pn') or []) or ''
                ev['po1'] = ', '.join(b.get('pn') or []) or ''
                ch = True
            # Brand only between our own copies: the tracker writes the ledger's brand words
            # (BEENE, USPA, VINCE) where the API's rows carry codes (GB, US, VC).
            if exact_etd and 'b' in common and (a.get('b') or '') != (b.get('b') or '') and a.get('b') and b.get('b'):
                ev['b0'] = a.get('b') or ''
                ch = True
            if 'sh' in common and list(a.get('sh') or []) != list(b.get('sh') or []):
                ev['sh0'], ev['sh1'] = a.get('sh') or [], b.get('sh') or []
                ch = True
            if 'g' in common and (a.get('g') or None) != (b.get('g') or None):
                ev['g0'], ev['g1'] = a.get('g'), b.get('g')
                ch = True
            if 'w' in common and (a.get('w') or '') != (b.get('w') or ''):
                ev['w0'], ev['w1'] = a.get('w') or '', b.get('w') or ''
                ch = True
            if not ch:
                continue
            ev['k'] = 'chg'
            if 'u0' not in ev:
                ev['u'] = b['u']           # units for context when they did not change
        elif b:
            ev.update(k='new', u1=b['u'])
            if b.get('e'):
                ev['e1'] = b['e']
            if b.get('sh'):
                ev['sh1'] = b['sh']
            if b.get('w'):
                ev['w1'] = b['w']
            if n0.get(b['p'], 0) == 0:
                ev['np'] = 1
        else:
            ev.update(k='rem', u0=a['u'])
            if a.get('e'):
                ev['e0'] = a['e']
            if a.get('sh'):
                ev['sh0'] = a['sh']
            if n1.get(a['p'], 0) == 0:
                ev['gone'] = 1
        events.append(ev)

    # Moves: the same style lost units on one production and gained them on another that day.
    losers, gainers = defaultdict(list), defaultdict(list)
    for ev in events:
        d = (ev.get('u1') or 0) - (ev.get('u0') or 0) if ('u0' in ev or 'u1' in ev) else 0
        if ev['k'] == 'rem' or (ev['k'] == 'chg' and d < 0):
            losers[ev['s']].append(ev)
        elif ev['k'] == 'new' or (ev['k'] == 'chg' and d > 0):
            gainers[ev['s']].append(ev)
    for style, ls in losers.items():
        gs = gainers.get(style)
        if not gs:
            continue
        for l in ls:
            to = sorted({g['p'] for g in gs if g['p'] != l['p']})
            if to:
                l['mv'] = to
        for g in gs:
            fr = sorted({l['p'] for l in ls if l['p'] != g['p']})
            if fr:
                g['mvf'] = fr

    pos = {}
    for ev in events:
        p = ev['p']
        if p in pos:
            continue
        po_name = ''
        for l in cl.values():
            if l['p'] == p and l.get('po'):
                po_name = l['po']
                break
        if not po_name:
            po_name = ev.get('po') or ''
        pos[p] = {'po': po_name, 'u0': tot0.get(p, 0), 'u1': tot1.get(p, 0), 'n0': n0.get(p, 0), 'n1': n1.get(p, 0)}
    return events, pos


def summarize(events):
    s = {'n': len(events), 'new': 0, 'rem': 0, 'chg': 0, 'up': 0, 'down': 0, 'etd': 0, 'ship': 0, 'pos': 0}
    prods = set()
    for ev in events:
        s[ev['k']] += 1
        prods.add(ev['p'])
        d = (ev.get('u1') or 0) - (ev.get('u0') or 0) if ('u0' in ev or 'u1' in ev) else 0
        if d > 0:
            s['up'] += d
        elif d < 0:
            s['down'] -= d
        if 'e0' in ev and 'e1' in ev and ev['k'] == 'chg':
            s['etd'] += 1
        if ev['k'] == 'chg' and 'sh1' in ev and len(ev.get('sh1') or []) > len(ev.get('sh0') or []):
            s['ship'] += 1
    s['pos'] = len(prods)
    return s


# ─────────────────────────────────────────────────────────────────────────────
# Storage (private prefix + Fernet, keyed off the machine key)
# ─────────────────────────────────────────────────────────────────────────────
def _secret():
    return (_deps.get('machine_key') or '').strip()


def _prefix():
    sec = _secret()
    digest = hashlib.sha256(f'versa-production-audit-prefix::{sec}'.encode()).hexdigest()[:32]
    return f'production-audit/{digest}'


def _fernet():
    sec = _secret()
    if not sec:
        return None
    try:
        from cryptography.fernet import Fernet
    except Exception:
        return None
    raw = hashlib.sha256(f'versa-production-audit-at-rest::{sec}'.encode()).digest()
    return Fernet(base64.urlsafe_b64encode(raw))


def _s3():
    return _deps['get_s3']()


def _put(name, obj):
    f = _fernet()
    if f is None:
        raise RuntimeError('production audit storage is not configured (machine key or cryptography missing)')
    body = f.encrypt(json.dumps(obj, separators=(',', ':'), default=str).encode('utf-8'))
    _s3().put_object(Bucket=_deps['s3_bucket'], Key=f'{_prefix()}/{name}', Body=body,
                     ContentType='application/octet-stream')


def _get(name):
    f = _fernet()
    if f is None:
        return None
    try:
        r = _s3().get_object(Bucket=_deps['s3_bucket'], Key=f'{_prefix()}/{name}')
    except Exception as e:
        code = ''
        try:
            code = e.response['Error']['Code']
        except Exception:
            pass
        if code in ('NoSuchKey', '404', 'NotFound'):
            return None
        raise
    return json.loads(f.decrypt(r['Body'].read()).decode('utf-8'))


def _list_names(sub):
    out = []
    token = None
    pre = f'{_prefix()}/{sub}'
    while True:
        kw = {'Bucket': _deps['s3_bucket'], 'Prefix': pre, 'MaxKeys': 1000}
        if token:
            kw['ContinuationToken'] = token
        r = _s3().list_objects_v2(**kw)
        out.extend(o['Key'][len(_prefix()) + 1:] for o in r.get('Contents', []))
        if not r.get('IsTruncated'):
            return out
        token = r.get('NextContinuationToken')


def _day_name(day):
    return f'days/{day}.json.enc'


def _events_name(day):
    return f'events/{day}.json.enc'


def save_day(snap):
    _put(_day_name(snap['day']), snap)
    with _mem_lock:
        _mem['days'].pop(snap['day'], None)
        # the changes of this day and of the next one read this copy
        _mem['events'].pop(snap['day'], None)
        nxt = (_parse_day(snap['day']) + dt.timedelta(days=1)).isoformat()
        _mem['events'].pop(nxt, None)


def load_day(day_iso, use_cache=True):
    today = et_today().isoformat()
    if use_cache and day_iso != today:
        with _mem_lock:
            hit = _mem['days'].get(day_iso)
        if hit is not None:
            return hit or None
    snap = _get(_day_name(day_iso))
    if day_iso != today and snap:
        # (a missing day is not remembered: a backfill on another worker may write it any minute)
        with _mem_lock:
            # a day copy is ~2,000 lines; keep only the few a diff needs (memory is tight on Render)
            if len(_mem['days']) >= 4:
                _mem['days'].pop(next(iter(_mem['days'])))
            _mem['days'][day_iso] = snap
    return snap


def _state():
    now = time.time()
    with _mem_lock:
        if _mem['state'] is not None and now - _mem['state_at'] < 60:
            return dict(_mem['state'])
    st = _get('state.json.enc') or {}
    with _mem_lock:
        _mem['state'], _mem['state_at'] = dict(st), now
    return st


def _save_state(st):
    _put('state.json.enc', st)
    with _mem_lock:
        _mem['state'], _mem['state_at'] = dict(st), time.time()


# ─────────────────────────────────────────────────────────────────────────────
# Capture: the leader copies today's ledger whenever it changes
# ─────────────────────────────────────────────────────────────────────────────
def own_snapshot(day_iso, rows, rev=None):
    lines = lines_from_rows(rows)
    return {'v': 1, 'day': day_iso, 'source': 'own', 'fields': list(OWN_FIELDS),
            'captured_at': dt.datetime.now(dt.timezone.utc).isoformat(), 'rev': rev,
            'rows': len(rows or []), 'sig': snapshot_sig(lines), 'lines': lines}


def capture_once(force=False):
    """Copy the ledger into today's day copy when it changed (or a new day started).
    Returns a short result dict. Safe to call from any worker; the loop calls it on the leader."""
    rows, rev = _deps['ledger_rows']()
    if not rows:
        return {'ok': False, 'why': 'ledger not loaded'}
    today = et_today().isoformat()
    snap = own_snapshot(today, rows, rev)
    if len(snap['lines']) < 50:
        return {'ok': False, 'why': f"only {len(snap['lines'])} lines; not a full ledger"}
    st = _state()
    same_day = st.get('day') == today
    if same_day and st.get('sig') == snap['sig'] and not force:
        return {'ok': True, 'changed': False, 'day': today, 'lines': len(snap['lines'])}
    last_n = int(st.get('lines') or 0)
    if last_n and len(snap['lines']) < PA_DROP_GUARD * last_n:
        # A sudden big drop is more often a half-saved workbook than a real edit: hold it
        # until it has stayed that way for half an hour.
        pend = st.get('drop_pending') or {}
        now = time.time()
        if pend.get('sig') != snap['sig']:
            st['drop_pending'] = {'sig': snap['sig'], 'since': now, 'lines': len(snap['lines'])}
            _save_state(st)
            return {'ok': False, 'why': f"line count fell from {last_n} to {len(snap['lines'])}; waiting to confirm"}
        if now - float(pend.get('since') or now) < PA_DROP_CONFIRM_SECONDS:
            return {'ok': False, 'why': 'drop not confirmed yet'}
    prev_day = st.get('day')
    save_day(snap)
    st.update(day=today, sig=snap['sig'], lines=len(snap['lines']), rows=snap['rows'], rev=rev,
              captured_at=snap['captured_at'], drop_pending=None)
    if not st.get('own_since'):
        st['own_since'] = today
    _save_state(st)
    # A new day started: settle the finished days' changes now (lazily computed otherwise).
    if prev_day and prev_day != today:
        d = _parse_day(prev_day)
        end = _parse_day(today)
        while d and d < end:
            try:
                events_for_day(d.isoformat(), persist=True)
            except Exception as e:
                print(f'[ProductionAudit] could not settle {d}: {e}', flush=True)
            d += dt.timedelta(days=1)
    return {'ok': True, 'changed': True, 'day': today, 'lines': len(snap['lines'])}


def _warm_history():
    """Read every finished day's changes into this worker's memory once after it starts, so a long
    Range (\"All history\") answers in a second instead of reading 200+ S3 objects first."""
    time.sleep(PA_WARM_AFTER_SECONDS)
    try:
        end = et_today() - dt.timedelta(days=1)
        days, d = [], PA_HISTORY_START
        while d <= end:
            days.append(d.isoformat())
            d += dt.timedelta(days=1)
        t0 = time.time()
        docs = load_event_docs(days)
        print(f'[ProductionAudit] warmed {sum(1 for v in docs.values() if v)} days in {time.time() - t0:.1f}s', flush=True)
    except Exception as e:
        print(f'[ProductionAudit] warm-up failed: {e}', flush=True)


def _capture_loop():
    time.sleep(120)   # let the worker load the ledger first
    while True:
        try:
            if worker_leader.is_leader():
                res = capture_once()
                _last_capture.update(at=dt.datetime.now(dt.timezone.utc).isoformat(), result=res, error=None)
        except Exception as e:
            _last_capture.update(at=dt.datetime.now(dt.timezone.utc).isoformat(), error=str(e)[:300])
            print(f'[ProductionAudit] capture failed: {e}', flush=True)
        time.sleep(PA_CAPTURE_SECONDS)


# ─────────────────────────────────────────────────────────────────────────────
# Changes per day
# ─────────────────────────────────────────────────────────────────────────────
def _base_for(day):
    """The copy just before `day` (looking back up to PA_BASE_LOOKBACK_DAYS)."""
    d = day - dt.timedelta(days=1)
    stop = day - dt.timedelta(days=PA_BASE_LOOKBACK_DAYS)
    while d >= stop and d >= PA_HISTORY_START - dt.timedelta(days=1):
        snap = load_day(d.isoformat())
        if snap and snap.get('lines'):
            return snap
        d -= dt.timedelta(days=1)
    return None


def copy_at_end_of(day):
    """The ledger copy at the end of `day` (a date): that day's copy, else the latest earlier one
    (looking back up to PA_BASE_LOOKBACK_DAYS). Days left out as glitches have no lines and are passed over."""
    d = day
    stop = day - dt.timedelta(days=PA_BASE_LOOKBACK_DAYS)
    while d >= stop and d >= PA_HISTORY_START:
        snap = load_day(d.isoformat())
        if snap and snap.get('lines'):
            return snap
        d -= dt.timedelta(days=1)
    return None


def load_event_docs(days):
    """{day iso: changes doc or None} for many days, reading the ones not in memory PA_PARALLEL_READS at a time."""
    out, missing = {}, []
    for iso in days:
        with _mem_lock:
            hit = _mem['events'].get(iso)
        if hit is not None:
            out[iso] = hit or None
        else:
            missing.append(iso)
    if missing:
        from concurrent.futures import ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=PA_PARALLEL_READS) as ex:
            for iso, doc in zip(missing, ex.map(lambda i: events_for_day(i, persist=True), missing)):
                out[iso] = doc
    return out


def range_view(frm, to, po=None, held=frozenset()):
    """Everything that changed from the start of `frm` until the end of `to` (or now when `to` is
    today): the ledger then against the ledger at the end, one line per production + style, plus how
    many times each line changed in between (and every dated step when `po` names one production).
    Lines that changed and came back to where they were are kept, as kind 'same'."""
    today = et_today()
    start = copy_at_end_of(frm - dt.timedelta(days=1))
    if to >= today:
        rows, rev = _deps['ledger_rows']()
        end = own_snapshot(today.isoformat(), rows, rev) if rows else None
    else:
        end = copy_at_end_of(to)
    if not start or not end:
        return None
    net, _pos = diff_snapshots(start, end)
    days = []
    d = frm
    while d <= to:
        days.append(d.isoformat())
        d += dt.timedelta(days=1)
    docs = load_event_docs(days)
    cnt, first, last, info, full = defaultdict(int), {}, {}, {}, defaultdict(list)
    per_day = []
    for iso in days:
        doc = docs.get(iso) or {}
        n = 0
        for ev in doc.get('events') or []:
            if ev.get('f') in held:
                continue
            n += 1
            k = ev['p'] + '::' + ev['s']
            cnt[k] += 1
            first.setdefault(k, iso)
            last[k] = iso
            info.setdefault(k, ev)
            if po and ev['p'] == po:
                full[k].append(dict(ev, d=iso))
        per_day.append({'d': iso, 'n': n, 'skipped': doc.get('skipped'), 'missing': not doc})
    net_by = {e['p'] + '::' + e['s']: e for e in net if e.get('f') not in held}
    keys = set(net_by) | set(cnt)
    if po:
        keys = {k for k in keys if k.split('::', 1)[0] == po}
    sl, el = start.get('lines') or {}, end.get('lines') or {}
    lines = []
    for k in keys:
        ev = net_by.get(k)
        if ev is not None:
            ev = dict(ev)
        else:
            src = el.get(k) or sl.get(k) or {}
            first_ev = info.get(k) or {}
            p = src.get('p') or first_ev.get('p') or k.split('::', 1)[0]
            ev = {'p': p, 's': src.get('s') or first_ev.get('s') or k.split('::', 1)[1], 'f': factory_of(p),
                  'b': src.get('b') or first_ev.get('b') or '', 'po': src.get('po') or first_ev.get('po') or '',
                  'k': 'same', 'u': (el.get(k) or {}).get('u', 0)}
            if k not in el and k not in sl:
                ev['ab'] = 1          # added and taken off again inside the range
            if ev['f'] in held:
                continue
        ev['n'] = cnt.get(k, 0)
        if ev['n']:
            ev['d1'], ev['d2'] = first.get(k), last.get(k)
        if po:
            ev['steps'] = full.get(k, [])
        lines.append(ev)
    tot0, tot1, n0, n1 = defaultdict(int), defaultdict(int), defaultdict(int), defaultdict(int)
    for l in sl.values():
        tot0[l['p']] += l['u']
        n0[l['p']] += 1
    for l in el.values():
        tot1[l['p']] += l['u']
        n1[l['p']] += 1
    pos = {}
    for ev in lines:
        p = ev['p']
        if p not in pos:
            pos[p] = {'po': ev.get('po') or '', 'u0': tot0.get(p, 0), 'u1': tot1.get(p, 0),
                      'n0': n0.get(p, 0), 'n1': n1.get(p, 0)}
    lines.sort(key=lambda e: (e['p'], e['s']))
    return {'from': frm.isoformat(), 'to': to.isoformat(), 'today': today.isoformat(), 'live': to >= today,
            'base': start.get('day'), 'base_src': start.get('source'), 'end_src': end.get('source'),
            'end_at': end.get('captured_at'),
            'fields': sorted(set(start.get('fields') or ()) & set(end.get('fields') or ())),
            'lines': lines, 'pos': pos, 'days': per_day}


def _make_doc(day_iso, base, cur):
    events, pos = diff_snapshots(base, cur)
    return {'v': 1, 'd': day_iso, 'base': base.get('day') if base else None,
            'src': [base.get('source') if base else None, cur.get('source') if cur else None],
            'fields': sorted(set(base.get('fields') or ()) & set(cur.get('fields') or ())) if base and cur else [],
            'events': events, 'pos': pos, 'sum': summarize(events),
            'computed_at': dt.datetime.now(dt.timezone.utc).isoformat()}


def _glitch_doc(day_iso, cur):
    return {'v': 1, 'd': day_iso, 'base': None, 'src': [None, cur.get('source')], 'fields': [], 'events': [],
            'pos': {}, 'sum': summarize([]), 'skipped': cur.get('glitch'),
            'computed_at': dt.datetime.now(dt.timezone.utc).isoformat()}


def live_doc():
    """Today's changes so far: yesterday's copy (or the one before it) against the ledger now."""
    rows, rev = _deps['ledger_rows']()
    today = et_today()
    base = _base_for(today)
    key = (today.isoformat(), rev, len(rows or []), base.get('sig') if base else None)
    with _mem_lock:
        hit = _mem['live']
    if hit and hit[0] == key:
        return hit[1]
    cur = own_snapshot(today.isoformat(), rows, rev) if rows else None
    if not cur or not base:
        doc = {'v': 1, 'd': today.isoformat(), 'base': base.get('day') if base else None, 'events': [], 'pos': {},
               'sum': summarize([]), 'live': True, 'missing': 'no ledger yet' if not cur else 'no earlier copy'}
    else:
        doc = _make_doc(today.isoformat(), base, cur)
        doc['live'] = True
        doc['as_of'] = cur['captured_at']
    with _mem_lock:
        _mem['live'] = (key, doc)
    return doc


def events_for_day(day_iso, persist=True):
    """The changes of one finished day (cached in memory, stored once worked out)."""
    today = et_today().isoformat()
    if day_iso >= today:
        return live_doc() if day_iso == today else None
    with _mem_lock:
        hit = _mem['events'].get(day_iso)
    if hit is not None:
        return hit or None
    doc = _get(_events_name(day_iso))
    if doc is None:
        day = _parse_day(day_iso)
        cur = load_day(day_iso)
        base = _base_for(day) if (cur and cur.get('lines')) else None
        if cur and cur.get('glitch'):
            doc = _glitch_doc(day_iso, cur)
        elif cur and cur.get('lines') and base:
            doc = _make_doc(day_iso, base, cur)
            if persist:
                try:
                    _put(_events_name(day_iso), doc)
                except Exception as e:
                    print(f'[ProductionAudit] could not store {day_iso}: {e}', flush=True)
        else:
            return None          # no copy for this day (yet): not remembered, see load_day
    with _mem_lock:
        _mem['events'].pop(day_iso, None)
        _mem['events'][day_iso] = doc
        while len(_mem['events']) > PA_EVENTS_CACHE_DAYS:      # least recently stored goes first
            _mem['events'].pop(next(iter(_mem['events'])))
    return doc or None


# ─────────────────────────────────────────────────────────────────────────────
# Backfill: day copies from the tracker's hourly versions
# ─────────────────────────────────────────────────────────────────────────────
def _tracker_versions():
    """{day iso: [(modified, version_id), ...] newest first} over the tracker's whole history."""
    by_day = defaultdict(list)
    kw = {'Bucket': _deps['s3_bucket'], 'Prefix': PA_TRACKER_KEY, 'MaxKeys': 1000}
    while True:
        r = _s3().list_object_versions(**kw)
        for v in r.get('Versions', []):
            if v.get('Key') != PA_TRACKER_KEY:
                continue
            by_day[et_day_of(v['LastModified']).isoformat()].append((v['LastModified'], v.get('VersionId')))
        if not r.get('IsTruncated'):
            break
        kw['KeyMarker'] = r.get('NextKeyMarker')
        kw['VersionIdMarker'] = r.get('NextVersionIdMarker')
    for d in by_day:
        by_day[d].sort(key=lambda x: x[0], reverse=True)
    return by_day


def tracker_snapshot(day_iso, versions, min_lines=0):
    """The day's last tracker copy that looks like a whole ledger (more than min_lines lines)."""
    for modified, vid in versions[:30]:
        try:
            r = _s3().get_object(Bucket=_deps['s3_bucket'], Key=PA_TRACKER_KEY, VersionId=vid)
            obj = json.loads(r['Body'].read().decode('utf-8'))
        except Exception as e:
            print(f'[ProductionAudit] tracker {day_iso} {vid}: {e}', flush=True)
            continue
        lines = lines_from_tracker(obj)
        if len(lines) < 50 or (min_lines and len(lines) < min_lines):
            continue
        return {'v': 1, 'day': day_iso, 'source': 'tracker', 'fields': list(TRACKER_FIELDS),
                'captured_at': modified.isoformat() if hasattr(modified, 'isoformat') else str(modified),
                'rev': vid, 'rows': len(lines), 'sig': snapshot_sig(lines), 'lines': lines}
    return None


PA_SPIKE_RATIO = 1.5      # a tracker day whose unit total is 1.5x both neighbours (or 1/1.5) is a glitch


def find_glitch_days(totals):
    """totals: [(day_iso, units, lines, source)] in day order (days with a copy only).
    A tracker day whose total units jump above BOTH neighbours by PA_SPIKE_RATIO, or fall below
    both by the same factor, is a copy the tracker got wrong (Apr 22 2026 counted every line twice
    and the next day went back): {day: reason}. Our own captured days are never judged."""
    out = {}
    for i in range(1, len(totals) - 1):
        d, u, n, src = totals[i]
        if src != 'tracker' or not u:
            continue
        pu, nu = totals[i - 1][1], totals[i + 1][1]
        if not pu or not nu:
            continue
        if u > PA_SPIKE_RATIO * pu and u > PA_SPIKE_RATIO * nu:
            out[d] = f'the copy counted {u:,} units against {pu:,} the day before and {nu:,} the day after'
        elif u * PA_SPIKE_RATIO < pu and u * PA_SPIKE_RATIO < nu:
            out[d] = f'the copy held {u:,} units against {pu:,} the day before and {nu:,} the day after'
    return out


def _set_bf(**kw):
    with _backfill_lock:
        _backfill.update(**kw)
    if 'progress' in kw:
        print(f"[ProductionAudit] {kw['progress']}", flush=True)
        _persist_backfill()      # so /status on any worker can show it


def backfill(start=None, end=None, force=False):
    """Fill every missing day copy from the tracker, then (re)work out each day's changes."""
    start = start or PA_HISTORY_START
    end = end or (et_today() - dt.timedelta(days=1))
    counts = {'tracker_days': 0, 'own_days': 0, 'kept': 0, 'missing': 0, 'event_days': 0, 'events': 0}
    _set_bf(running=True, started=dt.datetime.now(dt.timezone.utc).isoformat(), finished=None, error=None,
            progress='listing tracker copies', counts=counts)
    try:
        versions = _tracker_versions()
        have = set()
        for name in _list_names('days/'):
            m = re.search(r'days/(\d{4}-\d{2}-\d{2})\.json\.enc$', name)
            if m:
                have.add(m.group(1))
        prev_n = 0
        totals = []
        d = start
        while d <= end:
            iso = d.isoformat()
            snap = load_day(iso, use_cache=False) if iso in have else None
            if snap and snap.get('source') == 'own':
                counts['own_days'] += 1          # a day we captured ourselves is never replaced
            elif snap and not force:
                counts['kept'] += 1
            else:
                snap = None
                if versions.get(iso):
                    snap = tracker_snapshot(iso, versions[iso], int(prev_n * PA_DROP_GUARD) if prev_n else 0)
                if snap:
                    save_day(snap)
                    counts['tracker_days'] += 1
                else:
                    counts['missing'] += 1
            if snap and snap.get('lines'):
                prev_n = len(snap['lines'])
                totals.append((iso, sum(l['u'] for l in snap['lines'].values()), len(snap['lines']), snap.get('source')))
            elif snap and snap.get('glitch_lines'):
                totals.append((iso, snap.get('glitch_units') or 0, snap.get('glitch_lines') or 0, snap.get('source')))
            if d.day == 1 or d == end:
                _set_bf(progress=f'day copies through {iso}', counts=dict(counts))
            time.sleep(0.05)     # give the worker's other requests a turn (gevent)
            d += dt.timedelta(days=1)
        # tracker days that counted the ledger wrong are left out (their lines are set aside)
        glitches = find_glitch_days(totals)
        for iso, why in glitches.items():
            snap = load_day(iso, use_cache=False)
            if snap and snap.get('lines'):
                snap['glitch'] = why
                snap['glitch_lines'] = len(snap['lines'])
                snap['glitch_units'] = sum(l['u'] for l in snap['lines'].values())
                snap['lines'] = {}
                save_day(snap)
        counts['glitch_days'] = sorted(glitches)
        # the changes, oldest first, always rebuilt from the copies
        d = start + dt.timedelta(days=1)
        while d <= end:
            iso = d.isoformat()
            with _mem_lock:
                _mem['events'].pop(iso, None)
            cur = load_day(iso)
            base = _base_for(d) if cur and cur.get('lines') else None
            if cur and cur.get('lines') and base:
                doc = _make_doc(iso, base, cur)
            elif cur and cur.get('glitch'):
                doc = _glitch_doc(iso, cur)
            else:
                doc = None
            if doc is not None:
                _put(_events_name(iso), doc)
                with _mem_lock:
                    _mem['events'][iso] = doc
                counts['event_days'] += 1
                counts['events'] += len(doc['events'])
            if d.day == 1 or d == end:
                _set_bf(progress=f'changes through {iso}', counts=dict(counts))
            time.sleep(0.05)
            d += dt.timedelta(days=1)
        st = _state()
        st['history_start'] = start.isoformat()
        st['backfilled_at'] = dt.datetime.now(dt.timezone.utc).isoformat()
        _save_state(st)
        _set_bf(running=False, finished=dt.datetime.now(dt.timezone.utc).isoformat(), progress='done',
                counts=dict(counts))
    except Exception as e:
        traceback.print_exc()
        _set_bf(running=False, finished=dt.datetime.now(dt.timezone.utc).isoformat(), error=str(e)[:400],
                progress='failed', counts=dict(counts))
    _persist_backfill()


def _persist_backfill():
    try:
        with _backfill_lock:
            snap = dict(_backfill)
        _put('backfill.json.enc', snap)
    except Exception as e:
        print(f'[ProductionAudit] backfill status not saved: {e}', flush=True)


# ─────────────────────────────────────────────────────────────────────────────
# Routes
# ─────────────────────────────────────────────────────────────────────────────
def _caller():
    """'machine' | 'staff' | None. Staff = a Versa-Docs staff session (the phone app's sign-in)."""
    mk = (request.headers.get('X-Api-Key') or '').strip()
    key = _secret()
    if mk and key and hmac.compare_digest(mk, key):
        return 'machine'
    ident = None
    try:
        ident = _deps['identity']()
    except Exception:
        ident = None
    if ident and ident.get('tier') == 'staff':
        return 'staff'
    if ident and ident.get('tier') == 'machine':
        return 'machine'
    return None


def _deny():
    return jsonify({'error': 'Sign in with your Versa office account to see the Production Audit.'}), 401


def _held():
    try:
        return set(_deps['held_factories']() or ())
    except Exception:
        return set()


def register_production_audit_routes(app, *, get_s3, s3_bucket, machine_key, identity, ledger_rows,
                                     held_factories, norm_date=None, brand_of=None, start_capture=True):
    """ledger_rows() -> (rows, rev): the full parsed ledger (held factories included) after making
    sure it is loaded (load_production_from_dropbox), and the Dropbox rev it came from."""
    global _capture_thread_started
    _deps.update(get_s3=get_s3, s3_bucket=s3_bucket, machine_key=machine_key, identity=identity,
                 ledger_rows=ledger_rows, held_factories=held_factories, norm_date=norm_date, brand_of=brand_of)

    @app.route('/production-audit', methods=['GET', 'OPTIONS'])
    def production_audit_get():
        if request.method == 'OPTIONS':
            return ('', 204)
        if not _caller():
            return _deny()
        if _fernet() is None:
            return jsonify({'error': 'Production Audit storage is not configured on the server.'}), 503
        today = et_today()
        frm = _parse_day(request.args.get('from')) or today
        to = _parse_day(request.args.get('to')) or frm
        if to < frm:
            frm, to = to, frm
        if to > today:
            to = today
        if frm < PA_HISTORY_START:
            frm = PA_HISTORY_START
        if (to - frm).days + 1 > PA_MAX_RANGE_DAYS:
            return jsonify({'error': f'Pick at most {PA_MAX_RANGE_DAYS} days at a time.'}), 400
        held = _held()
        days, events, pos_ctx = [], [], {}
        d = frm
        while d <= to:
            iso = d.isoformat()
            try:
                doc = events_for_day(iso, persist=True) or {}
            except Exception as e:
                print(f'[ProductionAudit] {iso}: {e}', flush=True)
                doc = {'error': str(e)[:200]}
            evs = [dict(ev, d=iso) for ev in (doc.get('events') or []) if ev.get('f') not in held]
            for p, info in (doc.get('pos') or {}).items():
                if factory_of(p) in held:
                    continue
                pos_ctx.setdefault(iso, {})[p] = info
            s = summarize(evs)
            s.update(d=iso, base=doc.get('base'), live=bool(doc.get('live')), skipped=doc.get('skipped'),
                     fields=doc.get('fields') or [], src=doc.get('src') or [],
                     missing=bool(not doc or doc.get('missing')) if iso != today.isoformat() else bool(doc.get('missing')))
            days.append(s)
            events.extend(evs)
            d += dt.timedelta(days=1)
        # the productions' totals on the ledger now, for context on each card
        rows, _rev = _deps['ledger_rows']()
        now_tot, now_n, now_po = defaultdict(int), defaultdict(int), {}
        touched = {ev['p'] for ev in events}
        for r in rows or []:
            p = norm_prod(r.get('production'))
            if p not in touched:
                continue
            try:
                now_tot[p] += int(r.get('units') or 0)
            except (TypeError, ValueError):
                pass
            now_n[p] += 1
            if not now_po.get(p) and r.get('poName'):
                now_po[p] = str(r.get('poName')).strip()
        st = {}
        try:
            st = _state()
        except Exception:
            pass
        resp = jsonify({
            'from': frm.isoformat(), 'to': to.isoformat(), 'today': today.isoformat(),
            'history_start': PA_HISTORY_START.isoformat(), 'own_since': st.get('own_since'),
            'as_of': st.get('captured_at'), 'days': days, 'events': events, 'day_pos': pos_ctx,
            'now': {p: {'u': now_tot.get(p, 0), 'n': now_n.get(p, 0), 'po': now_po.get(p, '')} for p in touched},
            'factories': FACTORY_NAMES,
        })
        resp.headers['Cache-Control'] = 'no-store'
        return resp

    @app.route('/production-audit/since', methods=['GET', 'OPTIONS'])
    def production_audit_since():
        """?from=YYYY-MM-DD[&to=YYYY-MM-DD][&po=PRODUCTION]: everything that changed from the start of
        `from` until now (or the end of `to`). David, Nov 9 2026: "if i click on a date a month ago, it
        shows me the changes made from that day until today"."""
        if request.method == 'OPTIONS':
            return ('', 204)
        if not _caller():
            return _deny()
        if _fernet() is None:
            return jsonify({'error': 'Production Audit storage is not configured on the server.'}), 503
        today = et_today()
        frm = _parse_day(request.args.get('from'))
        if not frm:
            return jsonify({'error': 'Pick the day to start from.'}), 400
        to = _parse_day(request.args.get('to')) or today
        to = min(to, today)
        frm = max(frm, PA_HISTORY_START + dt.timedelta(days=1))   # the first day with a day before it
        if frm > to:
            frm, to = to, frm
        po = norm_prod(request.args.get('po')) or None
        try:
            view = range_view(frm, to, po=po, held=frozenset(_held()))
        except Exception as e:
            traceback.print_exc()
            return jsonify({'error': f'Could not read the ledger history: {str(e)[:200]}'}), 500
        if view is None:
            return jsonify({'error': 'There is no copy of the ledger for that range.'}), 404
        st = {}
        try:
            st = _state()
        except Exception:
            pass
        view.update(history_start=PA_HISTORY_START.isoformat(), own_since=st.get('own_since'),
                    as_of=st.get('captured_at'), factories=FACTORY_NAMES, po=po)
        resp = jsonify(view)
        resp.headers['Cache-Control'] = 'no-store'
        return resp

    @app.route('/production-audit/status', methods=['GET'])
    def production_audit_status():
        if not _caller():
            return _deny()
        with _backfill_lock:
            bf = dict(_backfill)
        stored_bf = None
        try:
            stored_bf = _get('backfill.json.enc')
        except Exception:
            pass
        st = {}
        try:
            st = _state()
        except Exception as e:
            st = {'error': str(e)[:200]}
        st.pop('sig', None)
        return jsonify({'configured': _fernet() is not None, 'state': st, 'last_capture_here': dict(_last_capture),
                        'backfill_here': bf, 'backfill_stored': stored_bf, 'leader_here': worker_leader.is_leader(),
                        'capture_seconds': PA_CAPTURE_SECONDS})

    @app.route('/production-audit/backfill', methods=['POST'])
    def production_audit_backfill():
        if _caller() != 'machine':
            return jsonify({'error': 'machine key only'}), 401
        body = request.get_json(silent=True) or {}
        with _backfill_lock:
            if _backfill.get('running'):
                return jsonify({'started': False, 'why': 'already running', 'state': dict(_backfill)}), 409
        start = _parse_day(body.get('start')) or PA_HISTORY_START
        end = _parse_day(body.get('end'))
        force = bool(body.get('force'))
        threading.Thread(target=backfill, kwargs={'start': start, 'end': end, 'force': force}, daemon=True).start()
        return jsonify({'started': True, 'start': start.isoformat(), 'end': _iso(end), 'force': force})

    @app.route('/production-audit/capture', methods=['POST'])
    def production_audit_capture():
        if _caller() != 'machine':
            return jsonify({'error': 'machine key only'}), 401
        try:
            res = capture_once(force=bool((request.get_json(silent=True) or {}).get('force')))
        except Exception as e:
            traceback.print_exc()
            return jsonify({'ok': False, 'error': str(e)[:300]}), 500
        return jsonify(res)

    if start_capture and not _capture_thread_started:
        _capture_thread_started = True
        threading.Thread(target=_capture_loop, daemon=True, name='production-audit-capture').start()
        threading.Thread(target=_warm_history, daemon=True, name='production-audit-warm').start()
