"""
versa_docs_feed.py: production lines from Versa Docs, the online Style Ledger.

David, Nov 18 2026: the inventory platform follows Versa Docs for ALL incoming
production; the hourly ATS file is used for warehouse stock only.

How the lines arrive
    Versa Docs' cron asks this API to refresh (POST /production/versa-docs/refresh)
    while signed in as an office account. The API then reads the database function
    ledger_platform_feed with that same token, so what it keeps always comes from
    Versa Docs itself, never from the request body. The API sends the version it
    already has; an unchanged ledger answers with the version only.

Where they are kept
    One JSON file on the shared disk (VD_FEED_DIR, default /var/data/versa_docs),
    written atomically, so both workers and a restarted server read the same lines.

The switch
    The feed carries platform_on (Versa Docs app_settings ledger_platform_push).
    While it is false nothing on the platform changes. VD_PRODUCTION_SOURCE=off on
    the server forces it off whatever the feed says.

Pure helpers: no network here except fetch(), which is handed the HTTP client and
the caller's token. No secrets live in this module.
"""
import json
import os
import tempfile
from datetime import datetime

FEED_DIR = os.environ.get('VD_FEED_DIR', '/var/data/versa_docs')
FEED_FILE = 'production_feed.json'
MIN_LINES = 200        # a smaller feed is refused: a broken read must never empty the platform
MAX_DROP = 0.30        # so is one that lost more than 30% of the last good feed's lines or units


class FeedError(Exception):
    """code: short reason; detail: a little context for the log (never secrets)."""

    def __init__(self, code, detail=''):
        super().__init__(code)
        self.code = code
        self.detail = str(detail or '')[:300]


def forced_off():
    return str(os.environ.get('VD_PRODUCTION_SOURCE', '')).strip().lower() in ('off', 'dropbox', '0', 'false')


def feed_path(folder=None):
    return os.path.join(folder or FEED_DIR, FEED_FILE)


def fetch(http, base, anon, token, known=None, timeout=60):
    """Call ledger_platform_feed as the signed-in caller. Returns the decoded dict."""
    if not token:
        raise FeedError('no_token')
    try:
        r = http.post(f"{str(base).rstrip('/')}/rest/v1/rpc/ledger_platform_feed",
                      headers={'apikey': anon, 'Authorization': f'Bearer {token}',
                               'Content-Type': 'application/json'},
                      json={'p_known': known}, timeout=timeout)
    except Exception as e:
        raise FeedError('feed_unreachable', f'{type(e).__name__}: {e}')
    if r.status_code != 200:
        raise FeedError(f'feed_http_{r.status_code}', (r.text or '')[:300])
    try:
        data = r.json()
    except Exception:
        raise FeedError('feed_not_json')
    if not isinstance(data, dict) or not data.get('version'):
        raise FeedError('feed_shape')
    return data


def _num(v):
    if v is None or v == '':
        return 0
    try:
        f = float(v)
    except (TypeError, ValueError):
        return 0
    return int(round(f))


def units_of(line):
    return max(0, _num((line or {}).get('units')))


def summary(feed):
    lines = (feed or {}).get('lines') or []
    return {'version': (feed or {}).get('version'), 'platform_on': bool((feed or {}).get('platform_on')),
            'generated_at': (feed or {}).get('generated_at'), 'count': len(lines),
            'units': sum(units_of(l) for l in lines)}


def validate(feed, prev=None):
    """(ok, reason). Refuses a feed that is malformed, tiny, or far smaller than the last good one."""
    if not isinstance(feed, dict):
        return False, 'not_a_dict'
    lines = feed.get('lines')
    if not isinstance(lines, list):
        return False, 'no_lines'
    if len(lines) < MIN_LINES:
        return False, f'only_{len(lines)}_lines'
    for i, l in enumerate(lines):
        if not isinstance(l, dict) or not str(l.get('style') or '').strip():
            return False, f'bad_line_{i}'
    if prev and isinstance(prev.get('lines'), list) and prev['lines']:
        a, b = summary(prev), summary(feed)
        if b['count'] < a['count'] * (1 - MAX_DROP):
            return False, f"lines_dropped_{a['count']}_to_{b['count']}"
        if a['units'] and b['units'] < a['units'] * (1 - MAX_DROP):
            return False, f"units_dropped_{a['units']}_to_{b['units']}"
    return True, 'ok'


def save(feed, folder=None):
    """Write the feed atomically (temp file + rename) and return its path."""
    folder = folder or FEED_DIR
    os.makedirs(folder, exist_ok=True)
    body = dict(feed)
    body['saved_at'] = datetime.utcnow().isoformat() + 'Z'
    fd, tmp = tempfile.mkstemp(prefix='.feed-', suffix='.json', dir=folder)
    try:
        with os.fdopen(fd, 'w', encoding='utf-8') as f:
            json.dump(body, f, ensure_ascii=False, separators=(',', ':'))
        os.replace(tmp, feed_path(folder))
    except Exception:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise
    return feed_path(folder)


def file_stamp(folder=None):
    """(mtime_ns, size) of the saved feed, or None when there is none."""
    try:
        st = os.stat(feed_path(folder))
    except OSError:
        return None
    return (st.st_mtime_ns, st.st_size)


def load(folder=None):
    """The saved feed, or None (missing or unreadable)."""
    try:
        with open(feed_path(folder), encoding='utf-8') as f:
            data = json.load(f)
    except (OSError, ValueError):
        return None
    return data if isinstance(data, dict) and isinstance(data.get('lines'), list) else None


def _dt(v):
    if not v:
        return None
    try:
        return datetime.strptime(str(v)[:10], '%Y-%m-%d')
    except ValueError:
        return None


def ledger_tuples(feed):
    """Each line in the master Style Ledger's column order, A to I:
    Production#, PO Name, Style, Units, Brand, F ETD, G port arrival, H Shipment #, I Warehouse.
    F = the ledger's ETD when filled, else its Ex-Factory date (David's rule); G = its ETA column."""
    out = []
    for l in (feed or {}).get('lines') or []:
        out.append((l.get('production') or '', l.get('po_name') or '', str(l.get('style') or '').strip().upper(),
                    units_of(l), l.get('brand') or '', _dt(l.get('etd')), _dt(l.get('eta')),
                    l.get('shipment') or '', l.get('warehouse') or ''))
    return out


def incoming_by_style(feed):
    """{style: open units} over every line of the feed."""
    out = {}
    for l in (feed or {}).get('lines') or []:
        st = str(l.get('style') or '').strip().upper()
        if st:
            out[st] = out.get(st, 0) + units_of(l)
    return out


def brand_by_style(feed):
    """{style: the brand written on its line with the most units}."""
    best = {}
    for l in (feed or {}).get('lines') or []:
        st = str(l.get('style') or '').strip().upper()
        b = str(l.get('brand') or '').strip().upper()
        if not st or not b:
            continue
        u = units_of(l)
        if st not in best or u > best[st][1]:
            best[st] = (b, u)
    return {k: v[0] for k, v in best.items()}


def apply_incoming(items, by_style, brands, new_item, brand_fix=None):
    """Inventory items with Versa Docs as the only source of incoming production.

    For every item: incoming becomes the ledger's open units for its style (0 when the
    ledger has none) and Total ATS moves by the same amount, so warehouse stock,
    committed and allocated stay exactly as the hourly file had them. An item left with
    no warehouse stock, no incoming, nothing committed and nothing allocated is dropped
    (only the old incoming kept it on the platform). A style on the ledger that the
    hourly file does not list is added as an incoming-only item built by new_item(sku, brand).
    Returns (items, report). Pure: the inputs are never changed."""
    report = {'styles': 0, 'changed': 0, 'added': 0, 'dropped': 0, 'incoming_before': 0,
              'incoming_after': 0, 'added_styles': [], 'dropped_styles': []}
    seen = set()
    out = []
    for it in items or []:
        if not isinstance(it, dict):
            out.append(it)
            continue
        sku = str(it.get('sku') or '').strip().upper()
        old = _num(it.get('incoming'))
        report['incoming_before'] += old
        if sku in seen:
            # the hourly file had this style twice (merged rows with another brand key):
            # the ledger's units go on the first one only
            new = 0
        else:
            new = int(by_style.get(sku, 0))
            seen.add(sku)
        it = dict(it)
        if new != old:
            it['incoming'] = new
            it['total_ats'] = _num(it.get('total_ats')) - old + new
            report['changed'] += 1
        if (new <= 0 and _num(it.get('total_warehouse')) <= 0 and _num(it.get('committed')) == 0
                and _num(it.get('allocated')) == 0):
            report['dropped'] += 1
            if len(report['dropped_styles']) < 500:
                report['dropped_styles'].append(sku)
            continue
        report['incoming_after'] += new
        out.append(it)
    for sku in sorted(by_style):
        if sku in seen or by_style[sku] <= 0:
            continue
        b = brands.get(sku) or ''
        if brand_fix:
            b = brand_fix(sku, b)
        if not b:
            continue
        out.append(new_item(sku, b, int(by_style[sku])))
        report['added'] += 1
        report['incoming_after'] += int(by_style[sku])
        if len(report['added_styles']) < 500:
            report['added_styles'].append(sku)
    report['styles'] = len(out)
    return out, report
