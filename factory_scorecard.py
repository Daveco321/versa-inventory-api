"""
FACTORY SCORECARD — BACKEND (Versa-Docs admin page "Factory Scorecard")

Tracks how each factory performs against what it promised: ETD movement per
style line, styles moved between production orders, actual ship dates against
the first stated ETD, and the volume the factory was carrying when each PO was
placed. David, Sep 8 2026.

Two ledger histories feed it (both already exist, nothing new is asked of anyone):

  master  - David's Style Ledger as tracked in S3 (production-inventory/snapshot.json,
            rewritten hourly by the production tracker; bucket versioning keeps every
            copy since 2026-04-15). One version per Eastern day = the ledger at end of day.
  factory - each factory's own Production_Style_Ledger workbook, dated copies kept in
            Dropbox under "Ongoing Order/<factory>/Archive" (weekly, Topfind back to
            Aug 2025). Their Production Ledger sheet carries DATE PO RECEIVED and the
            Style Ledger sheet carries shipped units and ATD (actual ship date).

Going forward the module also writes its own daily copy of the parsed master ledger
(inventory/factory_scorecard/ledger/<day>.json) so the history no longer depends on
the tracker service.

Routes (all: Versa-Docs admin session OR the X-Api-Key machine key):
  GET  /factory-scorecard            the scorecard dataset (per factory, no line index)
  GET  /factory-scorecard/lines      per-line index for search (ref, style, dates, changes)
  GET  /factory-scorecard/status     build status / last run
  POST /factory-scorecard/rebuild    start a rebuild in the background (?full=1 ignores caches)

Wire-up in app.py (after _caller_identity and load_production_from_dropbox exist):
    from factory_scorecard import register_scorecard_routes
    register_scorecard_routes(app, get_s3=get_s3, s3_bucket=S3_BUCKET,
                              get_dropbox_token=get_dropbox_token,
                              caller_identity=_caller_identity,
                              machine_key=INVENTORY_API_KEY,
                              load_master=load_production_from_dropbox)
"""
import io
import os
import re
import json
import time
import hmac
import threading
import traceback
import datetime as dt
from collections import defaultdict

from flask import request, jsonify
import openpyxl
import requests as http_requests

SC_PREFIX = os.environ.get('SC_S3_PREFIX', 'inventory/factory_scorecard/')
SC_TRACKER_KEY = os.environ.get('SC_TRACKER_KEY', 'production-inventory/snapshot.json')
SC_DROPBOX_ROOT = os.environ.get('SC_DROPBOX_ROOT', '/Versa Share Files/Ongoing Order')
SC_DAILY_HOUR_ET = int(os.environ.get('SC_DAILY_HOUR_ET', '11'))     # after the 10:00 ledger pulls
SC_RECENT_DAYS = 90

FACTORY_NAMES = {'TF': 'Topfind', 'NB': 'Yuxiu', 'PC': 'Pinnacle', 'DP': 'David Peng', 'FR': 'Frank Zhang', 'NK': 'Najmul'}
FOLDER_TO_CODE = {'topfind': 'TF', 'yuxiu': 'NB', 'pinnacle': 'PC', 'david peng': 'DP', 'frank zhang': 'FR', 'najmul': 'NK'}
_REF_RE = re.compile(r'^([A-Z]{2})(\d{5})$')
_EXCEL_EPOCH = dt.date(1899, 12, 30)
LINE_UNITS_CAP = 400_000        # no single style line is bigger than this
PO_UNITS_CAP = 2_000_000        # no single production order is bigger than this
DATE_FIELDS = ('etd', 'atd', 'exf', 'req_exf', 'eta', 'placed', 'received')


# ─────────────────────────────────────────────────────────────────────────────
# Time helpers
# ─────────────────────────────────────────────────────────────────────────────
def _et_zone():
    try:
        from zoneinfo import ZoneInfo
        return ZoneInfo('America/New_York')
    except Exception:
        return dt.timezone(dt.timedelta(hours=-4))


def _et_now():
    return dt.datetime.now(_et_zone())


def _et_today():
    return _et_now().date()


def _et_day_of(ts):
    """ISO timestamp (or datetime) -> Eastern calendar date."""
    if isinstance(ts, str):
        ts = dt.datetime.fromisoformat(ts.replace('Z', '+00:00'))
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=dt.timezone.utc)
    return ts.astimezone(_et_zone()).date()


# ─────────────────────────────────────────────────────────────────────────────
# Cell helpers
# ─────────────────────────────────────────────────────────────────────────────
def _norm(s):
    return re.sub(r'[^a-z0-9]+', ' ', str(s or '').lower()).strip()


def to_date(v):
    """Cell -> date or None. Accepts datetime/date, Excel serials and the text forms the
    factories type (3/20/27, 06/25/26, 9/10/2026, 2026-10-15). TBC / Will Advise / NA -> None."""
    if v is None:
        return None
    if isinstance(v, dt.datetime):
        return v.date()
    if isinstance(v, dt.date):
        return v
    if isinstance(v, (int, float)):
        if 30000 < v < 80000:
            return _EXCEL_EPOCH + dt.timedelta(days=int(v))
        return None
    s = str(v).strip()
    if not s:
        return None
    m = re.match(r'^(\d{1,2})[/.-](\d{1,2})[/.-](\d{2,4})', s)
    if m:
        a, b, y = int(m.group(1)), int(m.group(2)), int(m.group(3))
        if y < 100:
            y += 2000
        try:
            return dt.date(y, a, b)
        except ValueError:
            try:
                return dt.date(y, b, a)
            except ValueError:
                return None
    m = re.match(r'^(\d{4})-(\d{1,2})-(\d{1,2})', s)
    if m:
        try:
            return dt.date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
        except ValueError:
            return None
    return None


def to_int(v, cap=None):
    """Units cell -> int. A cap turns typo'd cells (a date serial or a run-on number in a
    units column) into 0 instead of poisoning totals."""
    if v is None:
        return 0
    if isinstance(v, (int, float)):
        n = int(round(v))
    else:
        s = re.sub(r'[^0-9.-]', '', str(v))
        try:
            n = int(round(float(s))) if s not in ('', '-', '.') else 0
        except ValueError:
            n = 0
    if n < 0:
        return 0
    if cap and n > cap:
        return 0
    return n


def median(vals):
    v = sorted(x for x in vals if x is not None)
    if not v:
        return None
    n = len(v)
    return v[n // 2] if n % 2 else round((v[n // 2 - 1] + v[n // 2]) / 2.0, 1)


def norm_ref(v):
    return re.sub(r'\s+', '', str(v or '')).upper()


def ref_factory(ref):
    m = _REF_RE.match(ref or '')
    return m.group(1) if m else None


def _iso(d):
    return d.isoformat() if d else None


def _header_map(row):
    out = {}
    for i, c in enumerate(row):
        n = _norm(c)
        if n and n not in out:
            out[n] = i
    return out


def _find(hm, *cands, prefix=False):
    for c in cands:
        if c in hm:
            return hm[c]
    if prefix:
        for c in cands:
            for k, i in hm.items():
                if k.startswith(c):
                    return i
    return None


def _blank_line(ref, style, folder_code=None):
    return {'ref': ref, 'factory': ref_factory(ref) or folder_code, 'po_name': '', 'style': style,
            'po_units': 0, 'ship_units': 0, 'brand': '', 'shipment': '', 'exf': None, 'req_exf': None,
            'etd': None, 'atd': None, 'eta': None, 'accept': False, 'note': '', 'placed': None}


# ─────────────────────────────────────────────────────────────────────────────
# Parsers
# ─────────────────────────────────────────────────────────────────────────────
def parse_workbook(src, folder_code=None):
    """Factory workbook (path or bytes) -> {'lines': [...], 'prods': [...]} across every sheet
    ('25 and '26 tabs). Production Ledger sheets give DATE PO RECEIVED / first ETD per PO;
    Style Ledger sheets give per-style units, shipped units, shipment #, ETD and ATD."""
    wb = openpyxl.load_workbook(io.BytesIO(src) if isinstance(src, (bytes, bytearray)) else src,
                                read_only=True, data_only=True)
    lines, prods = [], []
    for ws in wb.worksheets:
        header = None
        kind = None
        c = {}
        for row in ws.iter_rows(values_only=True):
            if row is None:
                continue
            if header is None:
                hm = _header_map(row)
                if 'style' in hm and ('po name' in hm or 'po units' in hm):
                    kind, header = 'style', hm
                    c = {
                        'ref': _find(hm, 'production', 'po ref', 'production #', 'po ref #', prefix=True),
                        'po_name': _find(hm, 'po name'),
                        'style': hm['style'],
                        'po_units': _find(hm, 'po units'),
                        'ship_units': _find(hm, 'ship units'),
                        'brand': _find(hm, 'brand'),
                        'shipment': _find(hm, 'shipment', prefix=True),
                        'exf': _find(hm, 'ex factory date', 'ex factory'),
                        'req_exf': _find(hm, 'req ex factory'),
                        'etd': _find(hm, 'etd'),
                        'atd': _find(hm, 'atd'),
                        'eta': _find(hm, 'eta'),
                        'accept': _find(hm, 'accept'),
                        'note': _find(hm, 'note', 'remarks', 'remark pnnacle', 'remark', prefix=True),
                    }
                    if c['ref'] is None:
                        header = None; kind = None   # mid-2025 Topfind files carry no ref column: unusable
                    continue
                if 'date po received' in hm and 'po number' in hm:
                    kind, header = 'prod', hm
                    c = {
                        'received': hm['date po received'],
                        'ref': hm['po number'],
                        'po_name': _find(hm, 'po name'),
                        'total_units': _find(hm, 'total units on po'),
                        'shipment': _find(hm, 'shipment', prefix=True),
                        'units_shipped': _find(hm, 'unit shipped', 'units shipped'),
                        'brand': _find(hm, 'brand'),
                        'etd': _find(hm, 'etd', prefix=True),
                        'state': _find(hm, 'production state'),
                    }
                    continue
                continue
            g = lambda k: (row[c[k]] if c.get(k) is not None and c[k] < len(row) else None)
            if kind == 'style':
                ref = norm_ref(g('ref'))
                style = str(g('style') or '').strip().upper()
                if not ref or not style or not _REF_RE.match(ref):
                    continue
                if not re.match(r'^[A-Z0-9][A-Z0-9_\-. ]{2,}$', style):
                    continue
                acc = g('accept')
                lines.append({
                    'ref': ref, 'factory': ref_factory(ref) or folder_code,
                    'po_name': str(g('po_name') or '').strip(),
                    'style': style,
                    'po_units': to_int(g('po_units'), LINE_UNITS_CAP), 'ship_units': to_int(g('ship_units'), LINE_UNITS_CAP),
                    'brand': str(g('brand') or '').strip(),
                    'shipment': str(g('shipment') or '').strip(),
                    'exf': to_date(g('exf')), 'req_exf': to_date(g('req_exf')),
                    'etd': to_date(g('etd')), 'atd': to_date(g('atd')), 'eta': to_date(g('eta')),
                    'accept': bool(acc) and str(acc).strip().lower() not in ('0', 'false', 'no', 'n'),
                    'note': str(g('note') or '').strip()[:120],
                    'placed': None,
                })
            elif kind == 'prod':
                ref = norm_ref(g('ref'))
                if not ref or not _REF_RE.match(ref):
                    continue
                po_name = str(g('po_name') or '').strip()
                if not po_name and not to_int(g('total_units')):
                    continue   # unused reserved ref number
                prods.append({
                    'ref': ref, 'factory': ref_factory(ref) or folder_code,
                    'received': to_date(g('received')),
                    'po_name': po_name,
                    'total_units': to_int(g('total_units'), PO_UNITS_CAP),
                    'shipment': str(g('shipment') or '').strip(),
                    'units_shipped': to_int(g('units_shipped'), PO_UNITS_CAP),
                    'brand': str(g('brand') or '').strip(),
                    'etd': to_date(g('etd')),
                    'state': str(g('state') or '').strip(),
                })
    wb.close()
    return {'lines': lines, 'prods': prods}


def parse_tracker(obj):
    """One version of production-inventory/snapshot.json -> lines present on the master
    ledger at that moment (entries that have not fallen off). 'placed' = the day the row
    first appeared on the master ledger (the office assigning the PO to that production)."""
    lines = []
    for key, e in (obj.get('orders') or {}).items():
        if e.get('fell_off_date'):
            continue
        ref = norm_ref(e.get('production'))
        style = str(e.get('style') or '').strip().upper()
        if not _REF_RE.match(ref) or not style:
            continue
        l = _blank_line(ref, style)
        l.update({
            'po_name': str(e.get('po_name') or '').strip(),
            'po_units': to_int(e.get('units'), LINE_UNITS_CAP),
            'brand': str(e.get('brand') or '').strip(),
            'etd': to_date(e.get('etd')),
            'placed': to_date((e.get('first_seen') or '')[:10]),
        })
        lines.append(l)
    return {'lines': lines, 'prods': []}


def parse_master_rows(rows, day):
    """The API's own parsed Style Ledger rows (load_production_from_dropbox) -> lines."""
    lines = []
    for r in rows or []:
        ref = norm_ref(r.get('production'))
        style = str(r.get('style') or '').strip().upper()
        if not _REF_RE.match(ref) or not style:
            continue
        l = _blank_line(ref, style)
        l.update({
            'po_name': str(r.get('poName') or '').strip(),
            'po_units': to_int(r.get('units'), LINE_UNITS_CAP),
            'brand': str(r.get('brand') or '').strip(),
            # keyed exactly like the tracker's copies ((ref, style), no shipment) so an own
            # copy filling a missed tracker day continues the same timeline
            'shipment': '',
            'shipment_no': str(r.get('shipmentNo') or '').strip(),
            'etd': to_date(r.get('etd_raw')) or to_date(r.get('etd')),
            'placed': None,
        })
        lines.append(l)
    return {'lines': lines, 'prods': []}


# ─────────────────────────────────────────────────────────────────────────────
# Engine
# ─────────────────────────────────────────────────────────────────────────────
def _line_key(l):
    return (l['ref'], l['style'])


def _fold_rows(rows):
    """Several rows of one (ref, style) on one day (split shipments in a factory file, batches
    in the ledger) -> one observation: earliest ETD, latest corroborated ATD, units summed."""
    if len(rows) == 1:
        return dict(rows[0])
    base = dict(rows[0])
    etds = [r['etd'] for r in rows if r['etd']]
    base['etd'] = min(etds) if etds else None
    base['po_units'] = sum(r['po_units'] for r in rows)
    base['ship_units'] = sum(r['ship_units'] for r in rows)
    base['accept'] = any(r['accept'] for r in rows)
    atds = [r['atd'] for r in rows if r['atd']]
    base['atd'] = max(atds) if atds else None
    base['shipment'] = ', '.join(sorted({r['shipment'] for r in rows if r['shipment']}))[:60]
    base['po_name'] = base['po_name'] or next((r['po_name'] for r in rows if r['po_name']), '')
    base['note'] = base.get('note') or next((r.get('note') for r in rows if r.get('note')), '')
    placed = [r['placed'] for r in rows if r.get('placed')]
    base['placed'] = min(placed) if placed else None
    return base


def build_scorecard(snapshots, today=None, recent_days=SC_RECENT_DAYS):
    """snapshots: list of {'source': 'master'|'factory', 'factory': code|None, 'date': date,
    'lines': [...], 'prods': [...]}. Rows are routed to factories by production-ref prefix and
    folded to ONE observation per (ref, style) per source per day. Returns the scorecard
    dataset (JSON-safe) with a 'lines' index."""
    today = today or _et_today()
    # series[fac][day] = {'master': {key: row}, 'factory': {key: row}, 'prods': {ref: prod}}
    series = defaultdict(lambda: defaultdict(lambda: {'master': {}, 'factory': {}, 'prods': {}}))
    for snap in snapshots:
        d = snap['date']
        src = 'master' if (snap.get('source') or 'factory') == 'master' else 'factory'
        groups = defaultdict(list)
        for l in snap['lines']:
            f = l['factory'] or snap.get('factory')
            if f:
                groups[(f, _line_key(l))].append(l)
        for (f, k), rows in groups.items():
            row = _fold_rows(rows)
            row['source'] = src
            series[f][d][src][k] = row
        for p in snap['prods']:
            f = p['factory'] or snap.get('factory')
            if f:
                series[f][d]['prods'].setdefault(p['ref'], p)

    # ── master copies that are not a real ledger (e.g. the tracker's first, one-row-per-PO
    #    copy on 2026-04-15) would fake entries and exits: drop any day whose rows collapse
    #    to (almost) a single style ──
    master_days_all = sorted({d for f in series for d in series[f] if series[f][d]['master']})
    for d in master_days_all:
        rows = [r for f in series for r in series[f][d]['master'].values()]
        styles = {r['style'] for r in rows}
        if len(rows) >= 20 and len(styles) <= max(1, int(len(rows) * 0.05)):
            for f in series:
                series[f][d]['master'] = {}
    all_master = sorted({d for f in series for d in series[f] if series[f][d]['master']})
    hist_start = all_master[0] if all_master else None
    last_master = all_master[-1] if all_master else None
    prev_master = {all_master[i]: all_master[i - 1] for i in range(1, len(all_master))}

    out = {'generated_at': dt.datetime.utcnow().isoformat(timespec='seconds') + 'Z', 'today': today.isoformat(),
           'recent_days': recent_days, 'factories': [], 'lines': [],
           'history_start': hist_start.isoformat() if hist_start else None,
           'history_end': last_master.isoformat() if last_master else None}
    for fac in sorted(series):
        fs = series[fac]
        dates = sorted(d for d in fs if fs[d]['master'] or fs[d]['factory'] or fs[d]['prods'])
        if not dates:
            continue
        m_dates = [d for d in dates if fs[d]['master']]
        f_dates = [d for d in dates if fs[d]['factory']]
        hist = defaultdict(list)
        for d in dates:
            for src in ('master', 'factory'):
                for k, row in fs[d][src].items():
                    hist[k].append((d, row))
        prod_hist = defaultdict(list)
        for d in dates:
            for ref, p in fs[d]['prods'].items():
                prod_hist[ref].append((d, p))

        # ── per line timeline ──
        line_rows = []
        etd_changes = []
        for k, seq in hist.items():
            ref, style = k
            seq.sort(key=lambda x: (x[0], 0 if x[1]['source'] == 'master' else 1))
            first_d, last_d = seq[0][0], seq[-1][0]
            m_seq = [(d, l) for d, l in seq if l['source'] == 'master']
            f_seq = [(d, l) for d, l in seq if l['source'] == 'factory']
            # ETD promises: the office's master ledger (daily) when we have it, else the factory's file
            e_seq = m_seq if (len(m_seq) >= 2 or not f_seq) else f_seq
            last_l = (m_seq[-1][1] if m_seq else f_seq[-1][1])
            f_last = f_seq[-1][1] if f_seq else None
            first_etd = next((l['etd'] for _, l in e_seq if l['etd']), None)
            latest_etd = next((l['etd'] for _, l in reversed(e_seq) if l['etd']), None)
            # actual departure: factory files only. An ATD counts when it is not later than
            # the copy that carries it AND the row shows shipped units / accepted / a shipment
            # number (David Peng types the planned date into ATD: a plan, not a departure).
            atd = None
            for d, l in reversed(f_seq):
                if l['atd'] and l['atd'] <= d and (l['ship_units'] > 0 or l['accept'] or l['shipment']):
                    atd = l['atd']; break
            changes = []
            prev = None
            for d, l in e_seq:
                if l['etd'] and prev and prev['etd'] and l['etd'] != prev['etd']:
                    days = (l['etd'] - prev['etd']).days
                    ch = {'date': d.isoformat(), 'from': prev['etd'].isoformat(), 'to': l['etd'].isoformat(), 'days': days}
                    changes.append(ch)
                    etd_changes.append({**ch, 'ref': ref, 'style': style, 'po_name': l['po_name'] or last_l['po_name']})
                prev = l
            placed = next((l['placed'] for _, l in seq if l.get('placed')), None) or first_d
            # ── ENTRY -> EXIT on the master ledger (David's core metric, Sep 9 2026) ──
            # entry = the first daily copy the line appears in, entry_etd = the ETD it carried
            # then; exit = the last copy it appears in, final_etd = the ETD it carried on that
            # last day. shift = final - entry. 'tracked' = the entry itself was observed: the
            # previous ledger copy is close (no gap) and the tracker did not know the line
            # before the history began.
            entry_d = entry_etd = exit_d = final_etd = None
            shift = None
            outcome = None
            tracked = False
            entry_etd_late = False
            if m_seq:
                entry_d, exit_d = m_seq[0][0], m_seq[-1][0]
                entry_etd = m_seq[0][1]['etd']
                if not entry_etd:
                    entry_etd = next((l['etd'] for _, l in m_seq if l['etd']), None)
                    entry_etd_late = entry_etd is not None
                final_etd = next((l['etd'] for _, l in reversed(m_seq) if l['etd']), None)
                pm = prev_master.get(entry_d)
                m_placed = next((l['placed'] for _, l in m_seq if l.get('placed')), None)
                tracked = bool(pm) and (entry_d - pm).days <= 10 and (not m_placed or not hist_start or m_placed > hist_start)
                if exit_d == last_master:
                    outcome = 'open'
                elif final_etd and final_etd <= exit_d + dt.timedelta(days=21):
                    outcome = 'completed'      # fell off the ledger around its ETD: shipped
                else:
                    outcome = 'removed'        # fell off with the ETD still out: cancelled or moved
                if entry_etd and final_etd:
                    shift = max(-120, min(400, (final_etd - entry_etd).days))
            ship_units = f_last['ship_units'] if f_last else 0
            units = (m_seq[-1][1]['po_units'] if m_seq else 0) or (f_last['po_units'] if f_last else 0)
            shipped = bool(atd) or (f_last is not None and ship_units > 0 and ship_units >= max(1, int((f_last['po_units'] or 1) * 0.9)))
            gone = bool((m_seq and exit_d < last_master) or (not m_seq and f_seq and f_seq[-1][0] < f_dates[-1]))
            observed = bool(first_etd) and (not atd or first_d <= atd)
            slip = None
            if first_etd:
                if atd:
                    slip = (atd - first_etd).days
                elif latest_etd:
                    slip = (latest_etd - first_etd).days
            if slip is not None:
                slip = max(-90, min(365, slip))
            if shipped:
                status = 'shipped'
            elif gone:
                status = 'done'
            elif latest_etd and latest_etd < today:
                status = 'late'
            else:
                status = 'open'
            line_rows.append({
                'factory': fac, 'ref': ref, 'style': style,
                'shipment': (f_last['shipment'] if f_last else '') or (m_seq[-1][1].get('shipment_no') if m_seq else '') or '',
                'po_name': last_l['po_name'] or (f_last['po_name'] if f_last else ''),
                'brand': last_l['brand'] or (f_last['brand'] if f_last else ''),
                'units': units, 'ship_units': ship_units,
                'placed': _iso(placed), 'first_seen': first_d.isoformat(), 'last_seen': last_d.isoformat(),
                'first_etd': _iso(first_etd), 'latest_etd': _iso(latest_etd), 'atd': _iso(atd),
                'slip_days': slip, 'etd_changes': len(changes), 'changes': changes[-8:],
                'status': status, 'observed': observed, 'src': 'master' if m_seq else 'factory',
                'entry_date': _iso(entry_d), 'entry_etd': _iso(entry_etd), 'exit_date': _iso(exit_d), 'final_etd': _iso(final_etd),
                'entry_etd_late': entry_etd_late,
                'shift_days': shift, 'outcome': outcome, 'tracked': tracked,
            })

        # ── style moves between production orders: consecutive copies of the SAME source ──
        moves = []
        seen_moves = set()
        for stream, src in ((m_dates, 'master'), (f_dates, 'factory')):
            for i in range(1, len(stream)):
                d0, d1 = stream[i - 1], stream[i]
                prev_by_style, cur_by_style = defaultdict(set), defaultdict(set)
                for (r0, s0) in fs[d0][src]:
                    prev_by_style[s0].add(r0)
                for (r1, s1) in fs[d1][src]:
                    cur_by_style[s1].add(r1)
                for style, prev_refs in prev_by_style.items():
                    cur_refs = cur_by_style.get(style, set())
                    gone_refs, new_refs = prev_refs - cur_refs, cur_refs - prev_refs
                    if not gone_refs or not new_refs:
                        continue
                    for a in sorted(gone_refs):
                        for b in sorted(new_refs):
                            old_l = fs[d0][src].get((a, style))
                            new_l = fs[d1][src].get((b, style))
                            if not old_l or not new_l or old_l['atd']:
                                continue
                            sig = (style, a, b, d1.isoformat()[:7])
                            if sig in seen_moves:
                                continue
                            seen_moves.add(sig)
                            days = (new_l['etd'] - old_l['etd']).days if (old_l['etd'] and new_l['etd']) else None
                            moves.append({'date': d1.isoformat(), 'style': style, 'from': a, 'to': b,
                                          'from_etd': _iso(old_l['etd']), 'to_etd': _iso(new_l['etd']),
                                          'days': days, 'units': new_l['po_units'], 'po_name': new_l['po_name'] or old_l['po_name']})

        # ── production orders (customer PO level) ──
        pos = []
        line_by_ref = defaultdict(list)
        for r in line_rows:
            line_by_ref[r['ref']].append(r)
        for ref in sorted(set(prod_hist) | set(line_by_ref)):
            ph = prod_hist.get(ref, [])
            lr = line_by_ref.get(ref, [])
            p_last = ph[-1][1] if ph else None
            received = next((p['received'] for _, p in ph if p['received']), None)
            if not received:
                cands = [r['placed'] for r in lr if r['placed']]
                received = dt.date.fromisoformat(min(cands)) if cands else None
            first_etds = [dt.date.fromisoformat(r['first_etd']) for r in lr if r['first_etd']]
            first_etd = min(first_etds) if first_etds else next((p['etd'] for _, p in ph if p['etd']), None)
            open_etds = [dt.date.fromisoformat(r['latest_etd']) for r in lr if r['latest_etd'] and r['status'] in ('open', 'late')]
            all_etds = [dt.date.fromisoformat(r['latest_etd']) for r in lr if r['latest_etd']]
            latest_etd = max(open_etds) if open_etds else (max(all_etds) if all_etds else next((p['etd'] for _, p in reversed(ph) if p['etd']), None))
            units = sum(r['units'] for r in lr) or (p_last['total_units'] if p_last else 0)
            shipped_units = sum(r['ship_units'] for r in lr) or (p_last['units_shipped'] if p_last else 0)
            atds = [dt.date.fromisoformat(r['atd']) for r in lr if r['atd']]
            last_atd = max(atds) if atds else None
            n_lines = len(lr)
            n_done = sum(1 for r in lr if r['status'] in ('shipped', 'done'))
            if n_lines and n_done == n_lines:
                status = 'shipped' if atds else 'done'
            elif n_done:
                status = 'partial'
            elif latest_etd and latest_etd < today:
                status = 'late'
            else:
                status = 'open'
            slips = [r['slip_days'] for r in lr if r['slip_days'] is not None]
            # entry -> exit view of the PO, from the lines whose entry was observed; a PO that
            # was already on the ledger when the history began is flagged instead of dated
            m_lines = [r for r in lr if r['entry_date']]
            tr_all = [r for r in m_lines if r['tracked']]
            src_lines = tr_all or m_lines
            tr = [r for r in tr_all if r['shift_days'] is not None and r['outcome'] in ('completed', 'open')]
            entered = min([r['entry_date'] for r in src_lines] or [None])
            entry_etds = [r['entry_etd'] for r in src_lines if r['entry_etd']]
            final_etds = [r['final_etd'] for r in src_lines if r['final_etd']]
            n_open_l = sum(1 for r in lr if r['outcome'] == 'open')
            n_comp_l = sum(1 for r in lr if r['outcome'] == 'completed')
            n_rem_l = sum(1 for r in lr if r['outcome'] == 'removed')
            pos.append({
                'entered': entered, 'entry_etd': min(entry_etds) if entry_etds else None,
                'final_etd': max(final_etds) if final_etds else None,
                'shift_days': round(sum(r['shift_days'] for r in tr) / len(tr), 1) if tr else None,
                'shift_lines': len(tr), 'lines_open': n_open_l, 'lines_completed': n_comp_l, 'lines_removed': n_rem_l,
                'ledger_status': 'open' if n_open_l else ('off' if (n_comp_l or n_rem_l) else 'none'),
                'tracked': bool(tr_all), 'on_ledger_at_start': bool(m_lines) and not tr_all,
                'last_exit': max([r['exit_date'] for r in lr if r['exit_date']] or [None]),
                'ref': ref, 'po_name': (p_last or {}).get('po_name') or (lr[0]['po_name'] if lr else ''),
                'brand': (p_last or {}).get('brand') or (lr[0]['brand'] if lr else ''),
                'received': _iso(received), 'first_etd': _iso(first_etd), 'latest_etd': _iso(latest_etd), 'atd': _iso(last_atd),
                'lead_days': (first_etd - received).days if (received and first_etd) else None,
                'units': units, 'shipped_units': shipped_units,
                'lines': n_lines, 'lines_done': n_done,
                'etd_changes': sum(r['etd_changes'] for r in lr),
                'slip_days': median(slips) if slips else None, 'status': status,
                'first_seen': min([r['first_seen'] for r in lr] or [ph[0][0].isoformat() if ph else '']),
            })

        # ── volume carried: units on the ledger per copy (master preferred: it lists open work only) ──
        load_series = []
        for d in dates:
            if fs[d]['master']:
                units_open = sum(l['po_units'] for l in fs[d]['master'].values())
                n_pos = len({k[0] for k in fs[d]['master']})
            else:
                units_open, n_pos = 0, 0
                for ref, p in fs[d]['prods'].items():
                    if p['received'] and p['received'] <= d and (not p['etd'] or p['etd'] >= d - dt.timedelta(days=14)):
                        left = max(0, p['total_units'] - p['units_shipped']) if p['total_units'] else 0
                        if left > 0:
                            units_open += left; n_pos += 1
            load_series.append({'date': d.isoformat(), 'open_units': units_open, 'open_pos': n_pos,
                                'src': 'master' if fs[d]['master'] else 'factory'})
        by_date_load = {x['date']: x['open_units'] for x in load_series}
        for po in pos:
            po['load_at_receipt'] = None
            if po['received']:
                snap_d = next((d for d in dates if d.isoformat() >= po['received']), None)
                if snap_d and (snap_d - dt.date.fromisoformat(po['received'])).days <= 45:
                    po['load_at_receipt'] = by_date_load.get(snap_d.isoformat())

        # ── current state (the latest master copy, shared by every factory) ──
        if last_master:
            cur_lines = list(fs[last_master]['master'].values()) if last_master in fs else []
            current = {'as_of': last_master.isoformat(), 'pos': len({l['ref'] for l in cur_lines}),
                       'lines': len(cur_lines), 'units': sum(l['po_units'] for l in cur_lines),
                       'late_lines': sum(1 for l in cur_lines if l['etd'] and l['etd'] < today),
                       'late_units': sum(l['po_units'] for l in cur_lines if l['etd'] and l['etd'] < today)}
        else:
            open_rows = [r for r in line_rows if r['status'] in ('open', 'late')]
            current = {'as_of': dates[-1].isoformat(), 'pos': len({r['ref'] for r in open_rows}), 'lines': len(open_rows),
                       'units': sum(r['units'] for r in open_rows),
                       'late_lines': sum(1 for r in open_rows if r['status'] == 'late'),
                       'late_units': sum(r['units'] for r in open_rows if r['status'] == 'late')}

        # ── KPIs, whole history and recent window ──
        def kpis_for(rows, changes, mv, label):
            shipped_lines = [r for r in rows if r['status'] == 'shipped' and r['atd'] and r['observed'] and r['slip_days'] is not None]
            on_time = sum(1 for r in shipped_lines if r['slip_days'] <= 0)
            within7 = sum(1 for r in shipped_lines if r['slip_days'] <= 7)
            open_rows = [r for r in rows if r['status'] in ('open', 'late')]
            later = [c for c in changes if c['days'] > 0]
            earlier = [c for c in changes if c['days'] < 0]
            touched = {(c['ref'], c['style']) for c in changes}
            return {
                'window': label,
                'shipped_lines': len(shipped_lines),
                'on_time_pct': round(100.0 * on_time / len(shipped_lines), 1) if shipped_lines else None,
                'within_7_pct': round(100.0 * within7 / len(shipped_lines), 1) if shipped_lines else None,
                'median_slip_shipped': median([r['slip_days'] for r in shipped_lines]),
                'open_lines': len(open_rows),
                'open_slip_median': median([r['slip_days'] for r in open_rows]),
                'etd_changes': len(changes), 'etd_later': len(later), 'etd_earlier': len(earlier),
                'lines_touched': len(touched),
                'median_push_days': median([c['days'] for c in later]),
                'style_moves': len(mv),
            }
        # ── entry -> exit shift (the headline): tracked master-ledger lines only ──
        def shift_stats(rows):
            comp = [r for r in rows if r['tracked'] and r['outcome'] == 'completed' and r['shift_days'] is not None]
            opn = [r for r in rows if r['tracked'] and r['outcome'] == 'open' and r['shift_days'] is not None]
            rem = [r for r in rows if r['tracked'] and r['outcome'] == 'removed']
            n = len(comp)
            later_n = sum(1 for r in comp if r['shift_days'] > 0)
            return {
                'completed': {
                    'n': n, 'units': sum(r['units'] for r in comp),
                    'avg': round(sum(r['shift_days'] for r in comp) / n, 1) if n else None,
                    'median': median([r['shift_days'] for r in comp]),
                    'later_pct': round(100.0 * later_n / n, 1) if n else None,
                    'same_pct': round(100.0 * sum(1 for r in comp if r['shift_days'] == 0) / n, 1) if n else None,
                    'earlier_pct': round(100.0 * sum(1 for r in comp if r['shift_days'] < 0) / n, 1) if n else None,
                    'avg_changes': round(sum(r['etd_changes'] for r in comp) / n, 2) if n else None,
                    'avg_later_days': round(sum(r['shift_days'] for r in comp if r['shift_days'] > 0) / later_n, 1) if later_n else None,
                },
                'open': {
                    'n': len(opn), 'units': sum(r['units'] for r in opn),
                    'avg_so_far': round(sum(r['shift_days'] for r in opn) / len(opn), 1) if opn else None,
                    'later_pct': round(100.0 * sum(1 for r in opn if r['shift_days'] > 0) / len(opn), 1) if opn else None,
                },
                'removed': {'n': len(rem), 'units': sum(r['units'] for r in rem)},
            }
        cutoff = (today - dt.timedelta(days=recent_days)).isoformat()
        shift_all = shift_stats(line_rows)
        shift_recent = shift_stats([r for r in line_rows if r['outcome'] == 'open' or (r['exit_date'] and r['exit_date'] >= cutoff)])
        untracked_n = sum(1 for r in line_rows if r['src'] == 'master' and not r['tracked'])
        by_entry = defaultdict(lambda: {'entered': 0, 'units': 0, 'completed': 0, 'shifts': [], 'later': 0, 'open': 0, 'removed': 0})
        for r in line_rows:
            if not (r['tracked'] and r['entry_date']):
                continue
            b = by_entry[r['entry_date'][:7]]
            b['entered'] += 1; b['units'] += r['units']
            if r['outcome'] == 'completed' and r['shift_days'] is not None:
                b['completed'] += 1; b['shifts'].append(r['shift_days'])
                if r['shift_days'] > 0:
                    b['later'] += 1
            elif r['outcome'] == 'open':
                b['open'] += 1
            elif r['outcome'] == 'removed':
                b['removed'] += 1
        monthly_entry_rows = [{'month': m, 'entered': v['entered'], 'units': v['units'], 'completed': v['completed'], 'open': v['open'], 'removed': v['removed'],
                               'avg_shift': round(sum(v['shifts']) / len(v['shifts']), 1) if v['shifts'] else None,
                               'median_shift': median(v['shifts']),
                               'later_pct': round(100.0 * v['later'] / v['completed'], 1) if v['completed'] else None}
                              for m, v in sorted(by_entry.items())]
        recent_rows = [r for r in line_rows if (r['atd'] and r['atd'] >= cutoff) or r['status'] in ('open', 'late')]
        recent_changes = [c for c in etd_changes if c['date'] >= cutoff]
        recent_moves = [m for m in moves if m['date'] >= cutoff]
        kpis = {
            'snapshots': len(dates), 'master_days': len(m_dates), 'factory_copies': len(f_dates),
            'first_snapshot': dates[0].isoformat(), 'last_snapshot': dates[-1].isoformat(),
            'lines': len(line_rows), 'pos': len(pos),
            'late_open_pos': sum(1 for p in pos if p['status'] == 'late'),
            'current': current,
            'shift': shift_all, 'shift_recent': shift_recent, 'untracked_lines': untracked_n,
            'history_start': hist_start.isoformat() if hist_start else None,
            'all': kpis_for(line_rows, etd_changes, moves, 'all'),
            'recent': kpis_for(recent_rows, recent_changes, recent_moves, f'{recent_days}d'),
        }
        monthly = defaultdict(lambda: {'units_due': 0, 'lines': 0, 'shipped': 0, 'on_time': 0, 'slips': [], 'changes': 0})
        for r in line_rows:
            if not r['first_etd']:
                continue
            mm = monthly[r['first_etd'][:7]]
            mm['units_due'] += r['units']; mm['lines'] += 1; mm['changes'] += r['etd_changes']
            if r['status'] == 'shipped' and r['atd'] and r['observed'] and r['slip_days'] is not None:
                mm['shipped'] += 1; mm['slips'].append(r['slip_days'])
                if r['slip_days'] <= 0:
                    mm['on_time'] += 1
        monthly_rows = [{'month': m, 'units_due': v['units_due'], 'lines': v['lines'], 'shipped': v['shipped'],
                         'on_time_pct': round(100.0 * v['on_time'] / v['shipped'], 1) if v['shipped'] else None,
                         'median_slip': median(v['slips']), 'etd_changes': v['changes']}
                        for m, v in sorted(monthly.items())]
        loaded = [p for p in pos if p.get('load_at_receipt') and p['slip_days'] is not None]
        buckets = []
        if len(loaded) >= 6:
            loaded.sort(key=lambda p: p['load_at_receipt'])
            n = len(loaded)
            for label, part in (('Lighter load', loaded[: n // 3]), ('Medium load', loaded[n // 3: 2 * n // 3]), ('Heavier load', loaded[2 * n // 3:])):
                if part:
                    late = sum(1 for p in part if p['slip_days'] > 0)
                    buckets.append({'label': label, 'pos': len(part), 'load_min': part[0]['load_at_receipt'], 'load_max': part[-1]['load_at_receipt'],
                                    'late_pct': round(100.0 * late / len(part), 1), 'median_slip': median([p['slip_days'] for p in part])})
        out['factories'].append({
            'code': fac, 'name': FACTORY_NAMES.get(fac, fac), 'kpis': kpis, 'monthly': monthly_rows,
            'monthly_entry': monthly_entry_rows,
            'load_series': load_series[-200:], 'load_buckets': buckets,
            'pos': sorted(pos, key=lambda p: (p['entered'] or p['received'] or p['first_seen'] or ''), reverse=True),
            'moves': sorted(moves, key=lambda m: m['date'], reverse=True)[:300],
            'changes': sorted(etd_changes, key=lambda c: c['date'], reverse=True)[:400],
        })
        out['lines'].extend(line_rows)
    return out


# ─────────────────────────────────────────────────────────────────────────────
# Storage: S3 caches, the tracker's version history, Dropbox factory copies
# ─────────────────────────────────────────────────────────────────────────────
_deps = {}
_state_lock = threading.Lock()
_state = {'running': False, 'started': None, 'finished': None, 'progress': '', 'last_error': None,
          'last_run_day': None, 'counts': None, 'trigger': None}
_mem = {'scorecard': None, 'lines': None, 'tracker_days': None, 'tracker_days_at': 0.0}
_daily_thread_started = False


def _s3():
    return _deps['get_s3']()


def _bucket():
    return _deps['s3_bucket']


def _s3_get_json(key, version_id=None):
    kw = {'Bucket': _bucket(), 'Key': key}
    if version_id:
        kw['VersionId'] = version_id
    try:
        r = _s3().get_object(**kw)
        return json.loads(r['Body'].read().decode('utf-8'))
    except Exception as e:
        code = getattr(e, 'response', {}).get('Error', {}).get('Code', '') if hasattr(e, 'response') else ''
        if code in ('NoSuchKey', '404'):
            return None
        raise


def _s3_put_json(key, obj):
    _s3().put_object(Bucket=_bucket(), Key=key, Body=json.dumps(obj, default=str).encode('utf-8'),
                     ContentType='application/json')


def _s3_list(prefix):
    keys = []
    token = None
    while True:
        kw = {'Bucket': _bucket(), 'Prefix': prefix, 'MaxKeys': 1000}
        if token:
            kw['ContinuationToken'] = token
        r = _s3().list_objects_v2(**kw)
        keys.extend(o['Key'] for o in r.get('Contents', []))
        if not r.get('IsTruncated'):
            return keys
        token = r.get('NextContinuationToken')


def _dates_to_iso(d):
    return {k: (_iso(v) if k in DATE_FIELDS else v) for k, v in d.items()}


def _dates_from_iso(d):
    out = dict(d)
    for k in DATE_FIELDS:
        if k in out and isinstance(out[k], str):
            try:
                out[k] = dt.date.fromisoformat(out[k])
            except ValueError:
                out[k] = None
    return out


def _slim_parsed(parsed):
    return {'lines': [_dates_to_iso(l) for l in parsed['lines']], 'prods': [_dates_to_iso(p) for p in parsed['prods']]}


def _fat_parsed(slim):
    return {'lines': [_dates_from_iso(l) for l in slim.get('lines', [])], 'prods': [_dates_from_iso(p) for p in slim.get('prods', [])]}


_progress_written_at = 0.0


def _persist_state(force=False):
    """Mirror the build state to S3 (throttled) so every gunicorn worker, not just the
    one running the build, can answer /factory-scorecard/status truthfully."""
    global _progress_written_at
    now = time.time()
    if not force and now - _progress_written_at < 8:
        return
    _progress_written_at = now
    try:
        with _state_lock:
            snap = dict(_state)
        snap['persisted_at'] = dt.datetime.utcnow().isoformat() + 'Z'
        _s3_put_json(f'{SC_PREFIX}status.json', snap)
    except Exception as e:
        print(f'[Scorecard] status persist failed: {e}', flush=True)


def _set_progress(msg):
    with _state_lock:
        _state['progress'] = msg
    print(f'[Scorecard] {msg}', flush=True)
    _persist_state()


# ── the tracker's hourly copies (one per Eastern day) ──
def tracker_days(force=False):
    """{day_iso: {'version_id', 'modified'}} — the LAST version of each Eastern day."""
    now = time.time()
    if not force and _mem['tracker_days'] and now - _mem['tracker_days_at'] < 3600:
        return _mem['tracker_days']
    by_day = {}
    kw = {'Bucket': _bucket(), 'Prefix': SC_TRACKER_KEY, 'MaxKeys': 1000}
    while True:
        r = _s3().list_object_versions(**kw)
        for v in r.get('Versions', []):
            if v.get('Key') != SC_TRACKER_KEY:
                continue
            mod = v['LastModified']
            day = _et_day_of(mod).isoformat()
            cur = by_day.get(day)
            if not cur or mod > cur['modified']:
                by_day[day] = {'version_id': v.get('VersionId'), 'modified': mod}
        if not r.get('IsTruncated'):
            break
        kw['KeyMarker'] = r.get('NextKeyMarker')
        kw['VersionIdMarker'] = r.get('NextVersionIdMarker')
    _mem['tracker_days'] = by_day
    _mem['tracker_days_at'] = now
    return by_day


def master_day_lines(day, version_id, use_cache=True):
    """Parsed master-ledger lines for one day, cached under SC_PREFIX master/<day>.json.
    Today is never cached (the tracker keeps rewriting it during the day)."""
    key = f'{SC_PREFIX}master/{day}.json'
    is_today = (day == _et_today().isoformat())
    if use_cache and not is_today:
        cached = _s3_get_json(key)
        if cached and cached.get('v') == version_id:
            return _fat_parsed(cached)
    obj = _s3_get_json(SC_TRACKER_KEY, version_id=version_id)
    if not obj:
        return None
    parsed = parse_tracker(obj)
    if not is_today:
        slim = _slim_parsed(parsed); slim['v'] = version_id
        _s3_put_json(key, slim)
    return parsed


# ── the module's own daily copy of the parsed master ledger ──
def write_own_snapshot(day=None):
    load_master = _deps.get('load_master')
    if not load_master:
        return False
    day = day or _et_today()
    rows = load_master()
    if not rows:
        return False
    parsed = parse_master_rows(rows, day)
    if not parsed['lines']:
        return False
    slim = _slim_parsed(parsed); slim['rows'] = len(rows); slim['written'] = dt.datetime.utcnow().isoformat() + 'Z'
    _s3_put_json(f'{SC_PREFIX}ledger/{day.isoformat()}.json', slim)
    return True


def own_snapshot_days():
    out = {}
    for k in _s3_list(f'{SC_PREFIX}ledger/'):
        m = re.search(r'/(\d{4}-\d{2}-\d{2})\.json$', k)
        if m:
            out[m.group(1)] = k
    return out


# ── Dropbox: the factories' own dated ledger copies ──
def _dbx_headers():
    tok = _deps['get_dropbox_token']()
    if not tok:
        raise RuntimeError('no Dropbox token')
    return {'Authorization': f'Bearer {tok}', 'Content-Type': 'application/json'}


def factory_files():
    """Every *Ledger*.xlsx under the Ongoing Order folder -> [{path, name, rev, modified(date), code, size}]."""
    hdr = _dbx_headers()
    entries = []
    r = http_requests.post('https://api.dropboxapi.com/2/files/list_folder', headers=hdr,
                           json={'path': SC_DROPBOX_ROOT, 'recursive': True, 'limit': 2000}, timeout=60)
    r.raise_for_status()
    j = r.json()
    entries.extend(j.get('entries', []))
    while j.get('has_more'):
        r = http_requests.post('https://api.dropboxapi.com/2/files/list_folder/continue', headers=hdr,
                               json={'cursor': j['cursor']}, timeout=60)
        r.raise_for_status()
        j = r.json()
        entries.extend(j.get('entries', []))
    root_low = SC_DROPBOX_ROOT.lower().rstrip('/') + '/'
    out = []
    for e in entries:
        if e.get('.tag') != 'file':
            continue
        name = e.get('name') or ''
        low = name.lower()
        if 'ledger' not in low or not low.endswith('.xlsx') or name.startswith('~$'):
            continue
        rel = (e.get('path_lower') or '')[len(root_low):] if (e.get('path_lower') or '').startswith(root_low) else ''
        folder = rel.split('/')[0] if rel else ''
        code = next((c for n, c in FOLDER_TO_CODE.items() if n in folder), None)
        if not code:
            continue
        mod = e.get('client_modified') or e.get('server_modified') or ''
        try:
            mod_day = _et_day_of(mod)
        except Exception:
            continue
        pl = e.get('path_lower') or ''
        out.append({'path': pl, 'name': name, 'rev': e.get('rev') or '', 'modified': mod_day,
                    'code': code, 'size': e.get('size') or 0,
                    # the live workbook in a factory's folder root changes in place: never cached
                    'live': ('/archive/' not in pl and '/backup/' not in pl)})
    return out


def factory_file_parsed(entry, use_cache=True):
    """Parsed workbook for one Dropbox copy, cached under SC_PREFIX factory/<rev>.json."""
    key = f"{SC_PREFIX}factory/{entry['rev'] or re.sub(r'[^A-Za-z0-9]+', '_', entry['path'])}.json"
    if use_cache and not entry.get('live'):
        cached = _s3_get_json(key)
        if cached is not None:
            return _fat_parsed(cached) if cached.get('lines') else None
    hdr = {'Authorization': f"Bearer {_deps['get_dropbox_token']()}",
           'Dropbox-API-Arg': json.dumps({'path': entry['path']})}
    r = http_requests.post('https://content.dropboxapi.com/2/files/download', headers=hdr, timeout=120)
    if r.status_code != 200:
        raise RuntimeError(f"dropbox download {r.status_code} for {entry['name']}")
    try:
        parsed = parse_workbook(r.content, folder_code=entry['code'])
    except Exception as e:
        _s3_put_json(key, {'lines': [], 'prods': [], 'error': str(e)[:200], 'name': entry['name']})
        return None
    if not entry.get('live'):
        slim = _slim_parsed(parsed); slim['name'] = entry['name']; slim['modified'] = entry['modified'].isoformat()
        _s3_put_json(key, slim)
    return parsed if parsed['lines'] else None


# ─────────────────────────────────────────────────────────────────────────────
# Build
# ─────────────────────────────────────────────────────────────────────────────
def collect_snapshots(use_cache=True):
    snaps = []
    counts = {'master_days': 0, 'own_days': 0, 'factory_copies': 0, 'skipped_files': 0, 'errors': []}
    # master ledger: one copy per Eastern day from the tracker's version history
    days = tracker_days(force=not use_cache)
    own = own_snapshot_days()
    today = _et_today().isoformat()
    total = len(days)
    days_with_lines = set()
    for i, day in enumerate(sorted(days)):
        try:
            parsed = master_day_lines(day, days[day]['version_id'], use_cache=use_cache)
        except Exception as e:
            counts['errors'].append(f'master {day}: {str(e)[:120]}')
            parsed = None
        if parsed and parsed['lines']:
            snaps.append({'source': 'master', 'factory': None, 'date': dt.date.fromisoformat(day), **parsed})
            counts['master_days'] += 1
            days_with_lines.add(day)
        if i % 20 == 0:
            _set_progress(f'master ledger copies {i + 1}/{total}')
    # our own daily copies fill any day the tracker missed (or produced nothing)
    for day, key in sorted(own.items()):
        if day in days_with_lines or day == today:
            continue
        try:
            slim = _s3_get_json(key)
            if slim and slim.get('lines'):
                snaps.append({'source': 'master', 'factory': None, 'date': dt.date.fromisoformat(day), **_fat_parsed(slim)})
                counts['own_days'] += 1
        except Exception as e:
            counts['errors'].append(f'own {day}: {str(e)[:120]}')
    # the factories' own dated copies
    try:
        files = factory_files()
    except Exception as e:
        counts['errors'].append(f'dropbox list: {str(e)[:160]}')
        files = []
    for i, f in enumerate(files):
        try:
            parsed = factory_file_parsed(f, use_cache=use_cache)
        except Exception as e:
            counts['errors'].append(f"{f['name']}: {str(e)[:120]}")
            parsed = None
        if parsed:
            snaps.append({'source': 'factory', 'factory': f['code'], 'date': f['modified'], 'file': f['name'], **parsed})
            counts['factory_copies'] += 1
        else:
            counts['skipped_files'] += 1
        if i % 10 == 0:
            _set_progress(f'factory copies {i + 1}/{len(files)}')
    return snaps, counts


def rebuild(trigger='manual', use_cache=True):
    with _state_lock:
        if _state['running']:
            return False
        _state.update(running=True, started=dt.datetime.utcnow().isoformat() + 'Z', finished=None,
                      progress='starting', last_error=None, trigger=trigger)
    _persist_state(force=True)
    try:
        snaps, counts = collect_snapshots(use_cache=use_cache)
        _set_progress(f"building from {len(snaps)} copies")
        sc = build_scorecard(snaps, today=_et_today())
        lines = sc.pop('lines')
        sc['sources'] = counts
        sc['status_note'] = ('Master ledger copies from the S3 tracker history plus the factories\' own dated ledgers in Dropbox. '
                             'On-time figures use only lines whose first stated ETD was seen before the goods shipped and whose factory reports an actual ship date (ATD).')
        _s3_put_json(f'{SC_PREFIX}scorecard.json', sc)
        _s3_put_json(f'{SC_PREFIX}lines.json', {'generated_at': sc['generated_at'], 'lines': lines})
        _mem['scorecard'] = sc
        _mem['lines'] = {'generated_at': sc['generated_at'], 'lines': lines}
        with _state_lock:
            _state.update(running=False, finished=dt.datetime.utcnow().isoformat() + 'Z', progress='done',
                          last_run_day=_et_today().isoformat(), counts=counts, generated_at=sc['generated_at'])
            if trigger == 'daily':
                _state['last_daily_day'] = _et_today().isoformat()
        _persist_state(force=True)
        print(f"[Scorecard] rebuilt: {counts}", flush=True)
        return True
    except Exception as e:
        traceback.print_exc()
        with _state_lock:
            _state.update(running=False, finished=dt.datetime.utcnow().isoformat() + 'Z',
                          progress='failed', last_error=str(e)[:300])
        _persist_state(force=True)
        return False


def _load_blob(name):
    """scorecard.json / lines.json, memoised per worker but refreshed whenever the S3
    status shows a newer build (another worker, or the daily job, may have rebuilt)."""
    latest = None
    try:
        st = _s3_get_json(f'{SC_PREFIX}status.json') or {}
        latest = st.get('generated_at')
    except Exception:
        pass
    cached = _mem.get(name)
    if cached is not None and (not latest or cached.get('generated_at') == latest):
        return cached
    obj = _s3_get_json(f'{SC_PREFIX}{name}.json')
    if obj is not None:
        _mem[name] = obj
    return obj


def _daily_loop():
    """Once a day at SC_DAILY_HOUR_ET Eastern: write our own copy of the master ledger, then
    rebuild the scorecard (new tracker day + any new factory copies; everything else cached)."""
    while True:
        try:
            now = _et_now()
            target = now.replace(hour=SC_DAILY_HOUR_ET, minute=5, second=0, microsecond=0)
            if target <= now:
                target = target + dt.timedelta(days=1)
            time.sleep(max(60, (target - now).total_seconds()))
            today = _et_today().isoformat()
            # the day's own ledger copy is always refreshed, even when a manual rebuild ran earlier
            try:
                write_own_snapshot()
            except Exception as e:
                print(f'[Scorecard] own snapshot failed: {e}', flush=True)
            st = _s3_get_json(f'{SC_PREFIX}status.json') or {}
            if st.get('last_daily_day') == today:
                continue        # another worker already ran today's daily rebuild
            rebuild(trigger='daily')
        except Exception as e:
            print(f'[Scorecard] daily loop error: {e}', flush=True)
            time.sleep(600)


# ─────────────────────────────────────────────────────────────────────────────
# Routes
# ─────────────────────────────────────────────────────────────────────────────
def _authorized():
    """Machine key, or a Versa-Docs session whose profile is staff AND is_admin."""
    mk = (request.headers.get('X-Api-Key') or '').strip()
    key = _deps.get('machine_key') or ''
    if mk and key and hmac.compare_digest(mk, key):
        return True, None
    auth = request.headers.get('Authorization', '')
    token = auth[7:].strip() if auth.lower().startswith('bearer ') else ''
    if not token:
        return False, (jsonify({'error': 'Sign in to Versa Docs to view this data.'}), 401)
    ident = _deps['caller_identity'](token)
    if not ident:
        return False, (jsonify({'error': 'Sign in to Versa Docs to view this data.'}), 401)
    if ident.get('role') != 'staff' or not ident.get('is_admin'):
        return False, (jsonify({'error': 'The factory scorecard is for Versa admins only.'}), 403)
    return True, None


def register_scorecard_routes(app, get_s3, s3_bucket, get_dropbox_token, caller_identity, machine_key, load_master):
    global _daily_thread_started
    _deps.update(get_s3=get_s3, s3_bucket=s3_bucket, get_dropbox_token=get_dropbox_token,
                 caller_identity=caller_identity, machine_key=machine_key, load_master=load_master)

    @app.route('/factory-scorecard', methods=['GET', 'OPTIONS'])
    def factory_scorecard_get():
        if request.method == 'OPTIONS':
            return '', 204
        ok, err = _authorized()
        if not ok:
            return err
        try:
            sc = _load_blob('scorecard')
        except Exception as e:
            return jsonify({'error': f'scorecard unavailable: {e}'}), 500
        if sc is None:
            with _state_lock:
                st = dict(_state)
            return jsonify({'ready': False, 'status': st}), 202
        return jsonify(sc)

    @app.route('/factory-scorecard/lines', methods=['GET', 'OPTIONS'])
    def factory_scorecard_lines():
        if request.method == 'OPTIONS':
            return '', 204
        ok, err = _authorized()
        if not ok:
            return err
        try:
            ln = _load_blob('lines')
        except Exception as e:
            return jsonify({'error': f'lines unavailable: {e}'}), 500
        if ln is None:
            return jsonify({'ready': False}), 202
        return jsonify(ln)

    @app.route('/factory-scorecard/status', methods=['GET', 'OPTIONS'])
    def factory_scorecard_status():
        if request.method == 'OPTIONS':
            return '', 204
        ok, err = _authorized()
        if not ok:
            return err
        with _state_lock:
            st = dict(_state)
        if not st.get('running'):
            # The build may be running in ANOTHER gunicorn worker: the S3 copy is the
            # shared truth whenever this worker is idle.
            try:
                persisted = _s3_get_json(f'{SC_PREFIX}status.json')
                if persisted:
                    persisted['worker_idle'] = True
                    return jsonify(persisted)
            except Exception:
                pass
        return jsonify(st)

    @app.route('/factory-scorecard/rebuild', methods=['POST', 'OPTIONS'])
    def factory_scorecard_rebuild():
        if request.method == 'OPTIONS':
            return '', 204
        ok, err = _authorized()
        if not ok:
            return err
        with _state_lock:
            if _state['running']:
                return jsonify({'started': False, 'reason': 'already running', 'status': dict(_state)}), 409
        full = (request.args.get('full') or '').strip() in ('1', 'true', 'yes')
        snap = (request.args.get('snapshot') or '').strip() in ('1', 'true', 'yes')

        def _run():
            if snap:
                try:
                    write_own_snapshot()
                except Exception as e:
                    print(f'[Scorecard] own snapshot failed: {e}', flush=True)
            rebuild(trigger='manual-full' if full else 'manual', use_cache=not full)
        threading.Thread(target=_run, daemon=True, name='scorecard-rebuild').start()
        return jsonify({'started': True, 'full': full})

    if not _daily_thread_started:
        _daily_thread_started = True
        threading.Thread(target=_daily_loop, daemon=True, name='scorecard-daily').start()
    print('[Scorecard] routes registered (daily rebuild at %02d:05 ET)' % SC_DAILY_HOUR_ET, flush=True)
