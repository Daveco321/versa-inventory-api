"""
PRODUCTION SWATCH CARDS: the swatch cards in David's email, filed under their production POs
(David, Nov 9 2026: "once a week, look through my email and look for swatch cards associated
with specific production POs, and upload the swatch cards to this app ... recheck every Monday
night at 9pm").

Who does what
  * A Google Apps Script in David's own Google account (project "Versa Docs email notes", file
    SwatchCards.gs, next to the daily email notes) runs every Monday around 9 PM New York time.
    It lists the email with PDF attachments since the last run and, for each email, sends this
    module the attachment list (names, sizes) plus the subject and the start of the text.
  * This module decides which attachments are swatch cards and which production numbers each
    belongs to (plan), and the script then sends only those files (upload). Gmail is never
    changed: the script is read-only.
  * The phone app's Production Audit tab reads GET /production-swatch-cards.

How a card is recognized (the design team's naming, e.g. "AB X BRAND TF26901 =110226 fm Name=.pdf",
"CUSTOMER X BRAND RUGBY PC26901 =110226 rvd fm Name=.pdf", "CUSTOMER FOB CHINA NB26901, 02 =110226
fm Name=.pdf"):
  * a PDF, 10 KB to 40 MB;
  * the name says SWATCH, or reads like a card ("<customer> X <brand>" or the "=MMDDYY fm Name="
    stamp), or the email's own text (not the quoted part) mentions a swatch card;
  * not trims, labels, specs, tech packs, care labels, SOPs, price tags, invoices, packing
    lists and the like (unless the name says SWATCH);
  * at least one production number: in the file name ("NB26901, 02" = NB26901 + NB26902), or,
    for a card whose name has none, in the subject of an email whose text mentions a swatch card.
  The same file sent again (replies, forwards) is stored once; a revised card is a new file and
  shows as the newest card of its production.

Storage (bucket nauticaslimfit is publicly readable, listing is denied):
  production-swatch-cards/<digest>/files/<sha16>.pdf    the card (unguessable name; opened by URL)
  production-swatch-cards/<digest>/thumbs/<sha16>.jpg   its first page, 480 px wide
  production-swatch-cards/<digest>/index.json.enc       every card, every email already read, the
                                                        run log and the ingest settings (encrypted)
  <digest> is derived from the machine key, so nothing here can be guessed or listed.

Routes
  GET  /production-swatch-cards            staff session or machine key: the cards by production
  GET  /production-swatch-cards/status     machine key: runs, counts, settings
  POST /production-swatch-cards/config     machine key: {allowed_emails, allowed_aud, paused, ...}
  POST /production-swatch-cards/ingest     the Apps Script (Google ID token in x-versa-id-token for an
                                           allowed address and the pinned script audience) or the
                                           machine key; actions config / pending / plan / upload /
                                           seen / done
"""
import base64
import hashlib
import hmac
import io
import json
import os
import re
import threading
import time
import traceback
import urllib.parse
import datetime as dt

from flask import request, jsonify
import requests as http_requests

SC_PREFIX_ROOT = 'production-swatch-cards'
SC_MIN_BYTES = 10 * 1024
SC_MAX_BYTES = 40 * 1024 * 1024
SC_BACKFILL_SINCE = os.environ.get('SWATCH_BACKFILL_SINCE', '2025/11/01')
SC_WEEKLY_LOOKBACK_DAYS = 14          # first weekly run with no history looks back this far
SC_OVERLAP_SECONDS = 2 * 86400        # each run re-lists 2 days before the last one started
SC_MSG_KEEP_DAYS = 400
FACTORY_CODES = ('TF', 'NB', 'PC', 'DP', 'FR', 'NK')

_deps = {}
_idx_lock = threading.Lock()
_idx_cache = {'data': None, 'at': 0.0}
_IDX_TTL = 30
_token_cache = {}
_token_lock = threading.Lock()


# ─────────────────────────────────────────────────────────────────────────────
# Recognizing a card
# ─────────────────────────────────────────────────────────────────────────────
_PROD_RE = re.compile(r'(?<![A-Z0-9])([A-Z]{2})[ -]?(2[4-9]\d{3})(?!\d)((?:\s*[,&/+]\s*(?:[A-Z]{2})?\d{2,5}(?!\d))*)')
_SUFFIX_RE = re.compile(r'([A-Z]{2})?(\d{2,5})')
_NOT_CARD = re.compile(
    r'TRIM|LABEL|SPEC|TECH\s*PACK|\bT\s*/\s*P\b|\bTP\b|CARE|\bSOP\b|PRICE\s*TAG|HANG\s*TAG|POLY\s*BAG|PACKING|'
    r'PACKAGING|INVOICE|\bP\s*/?\s*L\b|\bB\s*/\s*L\b|\bBL\s*NO|BOOKING|INSPECTION|REPORT|\bTEST|CERTIF|\bUPC|'
    r'SIZE\s*CHART|MEASUREMENT|COMMUNICATION|GUIDELINE|ARTWORK|STICKER|CARTON|SHIPPING|COMMERCIAL|FORWARDER|'
    r'\bISF\b|CUSTOMS|QUOTE|QUOTATION|\bCOST|CONTRACT|AGREEMENT|STATEMENT|RECEIPT|MANIFEST|CHECKLIST|COMMENTS?\b',
    re.I)
_CARD_NAME = re.compile(r'SWATCH', re.I)
_HOUSE_X = re.compile(r'(?:^|[\s_])X(?:[\s_]|$)', re.I)                 # "<customer> X <brand>"
_STAMP = re.compile(r'=\s*\d{6}\b[^=]*?(?:\bfm\b|\brvd\b|\bfrom\b)', re.I)  # "=110226 fm Name="
_SAYS_CARD = re.compile(r'swatch\s*cards?', re.I)
_QUOTE_START = re.compile(r'\n\s*(?:From:|-----\s*Original Message|---- Replied Message|On .{0,120}\bwrote:|发件人|寄件者)', re.I)


def productions_in(text):
    """Production numbers in a file name or subject, shorthand included:
    'NB26901, 02' -> NB26901, NB26902; 'PC26901,02,03' -> PC26901..03; 'TF26901, TF26905' -> both."""
    s = str(text or '').upper()
    out = []
    for m in _PROD_RE.finditer(s):
        letters, digits, tail = m.group(1), m.group(2), m.group(3) or ''
        if letters not in FACTORY_CODES:
            continue
        base = letters + digits
        if base not in out:
            out.append(base)
        for t in _SUFFIX_RE.finditer(tail):
            pre, num = t.group(1), t.group(2)
            if pre and pre not in FACTORY_CODES:
                continue
            if len(num) == 5:
                ref = (pre or letters) + num
            elif len(num) in (2, 3) and not pre:
                ref = letters + digits[:5 - len(num)] + num
            else:
                continue
            if ref not in out:
                out.append(ref)
    return out


def own_text(text):
    """The email's own words: everything before the first quoted reply header."""
    t = str(text or '')
    m = _QUOTE_START.search(t)
    return t[:m.start()] if m else t


def plan_message(msg, known_atts=None):
    """msg = {id, subject, text, attachments: [{name, mime, size, att}]} -> (wanted, skipped).
    wanted: [{att, name, mime, size, prods, why}]. A PDF whose name reads like a card is taken;
    a PDF with a plain name only when the email's own text mentions a swatch card AND it is the
    one PDF in the email left after the exclusions (an email that also carries tech packs or
    trims never turns those into cards)."""
    known_atts = known_atts or {}
    subject = str(msg.get('subject') or '')
    says_card = bool(_SAYS_CARD.search(own_text(msg.get('text') or '')) or _SAYS_CARD.search(subject))
    subject_prods = productions_in(subject)
    named, plain, skipped = [], [], []
    for a in msg.get('attachments') or []:
        name = str(a.get('name') or '').strip()
        mime = str(a.get('mime') or '').lower()
        size = int(a.get('size') or 0)
        if not name:
            continue
        if not (name.lower().endswith('.pdf') or mime == 'application/pdf'):
            continue
        if size and (size < SC_MIN_BYTES or size > SC_MAX_BYTES):
            skipped.append({'name': name, 'why': 'size'})
            continue
        stem = name.rsplit('.', 1)[0]
        named_card = bool(_CARD_NAME.search(stem))
        if not named_card and _NOT_CARD.search(stem):
            skipped.append({'name': name, 'why': 'not a card'})
            continue
        item = {'att': a.get('att'), 'name': name, 'mime': mime or 'application/pdf', 'size': size,
                'prods': productions_in(stem)}
        if named_card or _HOUSE_X.search(stem) or _STAMP.search(stem):
            item['why'] = 'name'
            named.append(item)
        else:
            plain.append(item)
    picks = list(named)
    if not named and says_card and len(plain) == 1:
        plain[0]['why'] = 'email'
        picks.append(plain[0])
    else:
        skipped.extend({'name': i['name'], 'why': 'no card signal'} for i in plain)
    want = []
    for item in picks:
        if not item['prods']:
            if says_card and subject_prods and len(subject_prods) <= 6:
                item['prods'] = list(subject_prods)
                item['why'] += '+subject'
            else:
                skipped.append({'name': item['name'], 'why': 'no production #'})
                continue
        if f"{msg.get('id')}|{item['name']}|{item['size']}" in known_atts:
            skipped.append({'name': item['name'], 'why': 'already stored'})
            continue
        want.append(item)
    return want, skipped


# ─────────────────────────────────────────────────────────────────────────────
# Storage
# ─────────────────────────────────────────────────────────────────────────────
def _secret():
    return (_deps.get('machine_key') or '').strip()


def _prefix():
    digest = hashlib.sha256(f'versa-swatch-cards-prefix::{_secret()}'.encode()).hexdigest()[:32]
    return f'{SC_PREFIX_ROOT}/{digest}'


def _fernet():
    sec = _secret()
    if not sec:
        return None
    try:
        from cryptography.fernet import Fernet
    except Exception:
        return None
    raw = hashlib.sha256(f'versa-swatch-cards-at-rest::{sec}'.encode()).digest()
    return Fernet(base64.urlsafe_b64encode(raw))


def _s3():
    return _deps['get_s3']()


def _public_url(key):
    region = _deps.get('region') or 'us-east-2'
    return f"https://{_deps['s3_bucket']}.s3.{region}.amazonaws.com/{urllib.parse.quote(key)}"


def _empty_index():
    return {'v': 1, 'cards': {}, 'msgs': {}, 'atts': {}, 'runs': [], 'config': {}}


def load_index(fresh=False):
    now = time.time()
    with _idx_lock:
        if not fresh and _idx_cache['data'] is not None and now - _idx_cache['at'] < _IDX_TTL:
            return _idx_cache['data']
    f = _fernet()
    data = None
    if f is not None:
        try:
            r = _s3().get_object(Bucket=_deps['s3_bucket'], Key=f'{_prefix()}/index.json.enc')
            data = json.loads(f.decrypt(r['Body'].read()).decode('utf-8'))
        except Exception as e:
            code = ''
            try:
                code = e.response['Error']['Code']
            except Exception:
                pass
            if code not in ('NoSuchKey', '404', 'NotFound'):
                raise
    data = data or _empty_index()
    for k, v in _empty_index().items():
        data.setdefault(k, v)
    with _idx_lock:
        _idx_cache['data'], _idx_cache['at'] = data, now
    return data


def save_index(data):
    f = _fernet()
    if f is None:
        raise RuntimeError('swatch card storage is not configured (machine key or cryptography missing)')
    # forget emails read long ago (the weekly run only looks back about 9 days)
    cutoff = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=SC_MSG_KEEP_DAYS)).isoformat()
    data['msgs'] = {k: v for k, v in (data.get('msgs') or {}).items() if (v or {}).get('at', '') >= cutoff}
    data['runs'] = (data.get('runs') or [])[-30:]
    data['updated_at'] = dt.datetime.now(dt.timezone.utc).isoformat()
    body = f.encrypt(json.dumps(data, separators=(',', ':'), default=str).encode('utf-8'))
    _s3().put_object(Bucket=_deps['s3_bucket'], Key=f'{_prefix()}/index.json.enc', Body=body,
                     ContentType='application/octet-stream')
    with _idx_lock:
        _idx_cache['data'], _idx_cache['at'] = data, time.time()


def _thumbnail(pdf_bytes):
    """(jpeg bytes or None, page count or None) for page 1 at 480 px wide."""
    try:
        try:
            import pymupdf as fitz
        except ImportError:
            import fitz
        from PIL import Image
    except Exception:
        return None, None
    try:
        doc = fitz.open(stream=pdf_bytes, filetype='pdf')
        pages = doc.page_count
        if not pages:
            return None, 0
        page = doc.load_page(0)
        zoom = 480.0 / max(1.0, page.rect.width)
        pix = page.get_pixmap(matrix=fitz.Matrix(zoom, zoom), alpha=False)
        img = Image.frombytes('RGB', (pix.width, pix.height), pix.samples)
        out = io.BytesIO()
        img.save(out, format='JPEG', quality=82, optimize=True)
        doc.close()
        return out.getvalue(), pages
    except Exception as e:
        print(f'[SwatchCards] thumbnail failed: {e}', flush=True)
        return None, None


def store_card(msg, file_name, data_bytes, prods, why=''):
    """Store one card file (once per content) and file it under its productions."""
    sha = hashlib.sha256(data_bytes).hexdigest()
    sid = sha[:16]
    idx = load_index(fresh=True)
    cards = idx['cards']
    msg_date = str(msg.get('date') or '')
    att_key = f"{msg.get('id')}|{file_name}|{int(msg.get('size') or len(data_bytes))}"
    if sid in cards:
        c = cards[sid]
        c['prods'] = sorted(set(c.get('prods') or []) | set(prods))
        c['sightings'] = int(c.get('sightings') or 1) + 1
        # the card is dated by the first email that carried it
        if msg_date and (not c.get('date') or msg_date < c['date']):
            c.update(date=msg_date, msg=msg.get('id'), subject=str(msg.get('subject') or '')[:300],
                     **{'from': str(msg.get('from') or '')[:200]})
        idx['atts'][att_key] = sid
        save_index(idx)
        return {'stored': True, 'dup': True, 'id': sid, 'prods': c['prods']}
    key = f'{_prefix()}/files/{sid}.pdf'
    safe = re.sub(r'[\r\n"]', '', file_name)[:180] or f'{sid}.pdf'
    _s3().put_object(Bucket=_deps['s3_bucket'], Key=key, Body=data_bytes, ContentType='application/pdf',
                     ContentDisposition="inline; filename*=UTF-8''" + urllib.parse.quote(safe),
                     CacheControl='public, max-age=31536000, immutable')
    thumb, pages = _thumbnail(data_bytes)
    tkey = None
    if thumb:
        tkey = f'{_prefix()}/thumbs/{sid}.jpg'
        _s3().put_object(Bucket=_deps['s3_bucket'], Key=tkey, Body=thumb, ContentType='image/jpeg',
                         CacheControl='public, max-age=31536000, immutable')
    cards[sid] = {'id': sid, 'sha': sha, 'name': file_name, 'size': len(data_bytes), 'pages': pages,
                  'prods': sorted(set(prods)), 'date': msg_date, 'msg': msg.get('id'),
                  'subject': str(msg.get('subject') or '')[:300], 'from': str(msg.get('from') or '')[:200],
                  'key': key, 'thumb': tkey, 'why': why, 'sightings': 1,
                  'added': dt.datetime.now(dt.timezone.utc).isoformat()}
    idx['atts'][att_key] = sid
    save_index(idx)
    return {'stored': True, 'dup': False, 'id': sid, 'prods': cards[sid]['prods'], 'pages': pages}


# ─────────────────────────────────────────────────────────────────────────────
# Who may call
# ─────────────────────────────────────────────────────────────────────────────
def _is_machine():
    mk = (request.headers.get('X-Api-Key') or '').strip()
    key = _secret()
    return bool(mk and key and hmac.compare_digest(mk, key))


def _google_identity(tok):
    """{'email', 'aud'} for a valid Google ID token, else None (cached until shortly before expiry)."""
    h = hashlib.sha256(tok.encode('utf-8')).hexdigest()
    now = time.time()
    with _token_lock:
        hit = _token_cache.get(h)
        if hit and hit[0] > now:
            return hit[1]
    try:
        r = http_requests.get('https://oauth2.googleapis.com/tokeninfo', params={'id_token': tok}, timeout=10)
        if r.status_code != 200:
            return None
        c = r.json()
    except Exception:
        return None
    if c.get('iss') not in ('accounts.google.com', 'https://accounts.google.com'):
        return None
    if str(c.get('email_verified')).lower() != 'true':
        return None
    try:
        exp = float(c.get('exp') or 0)
    except (TypeError, ValueError):
        return None
    if exp <= now:
        return None
    ident = {'email': str(c.get('email') or '').strip().lower(), 'aud': str(c.get('aud') or '')}
    with _token_lock:
        if len(_token_cache) > 200:
            _token_cache.clear()
        _token_cache[h] = (min(exp - 30, now + 600), ident)
    return ident


def _ingest_allowed():
    if _is_machine():
        return True, 'machine'
    tok = (request.headers.get('x-versa-id-token') or '').strip()
    if not tok:
        return False, 'no credential'
    ident = _google_identity(tok)
    if not ident:
        return False, 'Google sign-in token not valid'
    cfg = load_index().get('config') or {}
    emails = [e.lower() for e in (cfg.get('allowed_emails') or [])]
    auds = list(cfg.get('allowed_aud') or [])
    if not emails or ident['email'] not in emails:
        return False, 'this Google account is not allowed'
    if not auds or ident['aud'] not in auds:
        return False, 'unknown script'
    return True, ident['email']


def _is_staff():
    try:
        ident = _deps['identity']()
    except Exception:
        ident = None
    return bool(ident and ident.get('tier') in ('staff', 'machine'))


# ─────────────────────────────────────────────────────────────────────────────
# Routes
# ─────────────────────────────────────────────────────────────────────────────
def _card_view(c):
    return {'id': c['id'], 'name': c.get('name'), 'prods': c.get('prods') or [], 'date': c.get('date'),
            'from': c.get('from'), 'subject': c.get('subject'), 'pages': c.get('pages'), 'size': c.get('size'),
            'url': _public_url(c['key']), 'thumb': _public_url(c['thumb']) if c.get('thumb') else None}


def _weekly_since(idx):
    runs = [r for r in (idx.get('runs') or []) if r.get('mode') == 'weekly' and not r.get('error') and not r.get('partial')]
    if runs:
        last = max(float(r.get('started_at') or 0) for r in runs) / 1000.0
        return int(last - SC_OVERLAP_SECONDS)
    return int(time.time() - SC_WEEKLY_LOOKBACK_DAYS * 86400)


def register_swatch_card_routes(app, *, get_s3, s3_bucket, region, machine_key, identity, held_factories=None):
    _deps.update(get_s3=get_s3, s3_bucket=s3_bucket, region=region, machine_key=machine_key,
                 identity=identity, held_factories=held_factories)

    @app.route('/production-swatch-cards', methods=['GET', 'OPTIONS'])
    def swatch_cards_get():
        if request.method == 'OPTIONS':
            return ('', 204)
        if not (_is_machine() or _is_staff()):
            return jsonify({'error': 'Sign in with your Versa office account to see swatch cards.'}), 401
        if _fernet() is None:
            return jsonify({'error': 'Swatch card storage is not configured on the server.'}), 503
        idx = load_index()
        held = set()
        try:
            held = set(_deps['held_factories']() or ()) if _deps.get('held_factories') else set()
        except Exception:
            held = set()
        cards, by_prod = [], {}
        for c in idx['cards'].values():
            prods = [p for p in (c.get('prods') or []) if p[:2] not in held]
            if not prods:
                continue
            v = _card_view(c)
            v['prods'] = prods
            cards.append(v)
            for p in prods:
                by_prod.setdefault(p, []).append(v['id'])
        order = {c['id']: (c.get('date') or '') for c in cards}
        for p in by_prod:
            by_prod[p].sort(key=lambda i: order.get(i, ''), reverse=True)
        cards.sort(key=lambda c: c.get('date') or '', reverse=True)
        runs = [r for r in (idx.get('runs') or []) if r.get('finished_at')]
        resp = jsonify({'cards': cards, 'by_prod': by_prod, 'updated_at': idx.get('updated_at'),
                        'last_run': runs[-1] if runs else None})
        resp.headers['Cache-Control'] = 'no-store'
        return resp

    @app.route('/production-swatch-cards/status', methods=['GET'])
    def swatch_cards_status():
        if not _is_machine():
            return jsonify({'error': 'machine key only'}), 401
        idx = load_index(fresh=True)
        cfg = dict(idx.get('config') or {})
        return jsonify({'configured': _fernet() is not None, 'cards': len(idx['cards']),
                        'productions': len({p for c in idx['cards'].values() for p in (c.get('prods') or [])}),
                        'emails_read': len(idx['msgs']), 'runs': (idx.get('runs') or [])[-10:],
                        'config': {'allowed_emails': len(cfg.get('allowed_emails') or []),
                                   'allowed_aud': len(cfg.get('allowed_aud') or []),
                                   'paused': bool(cfg.get('paused'))},
                        'weekly_since': _weekly_since(idx)})

    @app.route('/production-swatch-cards/config', methods=['POST'])
    def swatch_cards_config():
        if not _is_machine():
            return jsonify({'error': 'machine key only'}), 401
        body = request.get_json(silent=True) or {}
        idx = load_index(fresh=True)
        cfg = dict(idx.get('config') or {})
        if 'allowed_emails' in body:
            cfg['allowed_emails'] = sorted({str(e).strip().lower() for e in (body.get('allowed_emails') or []) if '@' in str(e)})
        if 'allowed_aud' in body:
            cfg['allowed_aud'] = sorted({str(a).strip() for a in (body.get('allowed_aud') or []) if str(a).strip()})
        if 'paused' in body:
            cfg['paused'] = bool(body.get('paused'))
        idx['config'] = cfg
        save_index(idx)
        return jsonify({'ok': True, 'allowed_emails': len(cfg.get('allowed_emails') or []),
                        'allowed_aud': len(cfg.get('allowed_aud') or []), 'paused': bool(cfg.get('paused'))})

    @app.route('/production-swatch-cards/ingest', methods=['POST', 'OPTIONS'])
    def swatch_cards_ingest():
        if request.method == 'OPTIONS':
            return ('', 204)
        ok, who = _ingest_allowed()
        if not ok:
            return jsonify({'error': who}), 401
        if _fernet() is None:
            return jsonify({'error': 'swatch card storage is not configured'}), 503
        body = request.get_json(silent=True) or {}
        action = str(body.get('action') or '')
        try:
            if action == 'config':
                idx = load_index(fresh=True)
                cfg = idx.get('config') or {}
                mode = 'backfill' if body.get('mode') == 'backfill' else 'weekly'
                if mode == 'backfill':
                    queries = [f'has:attachment filename:pdf swatch after:{SC_BACKFILL_SINCE}',
                               'has:attachment filename:pdf newer_than:45d']
                else:
                    queries = [f'has:attachment filename:pdf after:{_weekly_since(idx)}']
                return jsonify({'paused': bool(cfg.get('paused')), 'mode': mode, 'queries': queries,
                                'query': queries[0], 'max_list': 6000,
                                'run_id': f"{mode}-{int(time.time())}"})
            if action == 'pending':
                idx = load_index(fresh=True)
                ids = [str(i) for i in (body.get('ids') or [])][:2000]
                return jsonify({'ids': [i for i in ids if i not in idx['msgs']]})
            if action == 'plan':
                msg = body.get('message') or {}
                idx = load_index()
                want, skipped = plan_message(msg, idx.get('atts') or {})
                return jsonify({'want': want, 'skipped': skipped})
            if action == 'upload':
                msg = body.get('message') or {}
                f = body.get('file') or {}
                raw = str(f.get('data') or '')
                if not raw:
                    return jsonify({'error': 'no file data'}), 400
                data = base64.urlsafe_b64decode(raw + '=' * (-len(raw) % 4))
                if len(data) < SC_MIN_BYTES or len(data) > SC_MAX_BYTES or not data[:5] == b'%PDF-':
                    return jsonify({'error': 'not a PDF swatch card', 'size': len(data)}), 400
                prods = [p for p in (body.get('prods') or []) if re.match(r'^[A-Z]{2}\d{5}$', str(p))]
                if not prods:
                    prods = productions_in(f.get('name'))
                if not prods:
                    return jsonify({'error': 'no production number'}), 400
                msg = dict(msg, size=f.get('size') or len(data))
                res = store_card(msg, str(f.get('name') or 'swatch card.pdf'), data, prods, why=str(f.get('why') or ''))
                return jsonify(res)
            if action == 'seen':
                idx = load_index(fresh=True)
                mid = str(body.get('id') or '')
                if mid:
                    idx['msgs'][mid] = {'at': dt.datetime.now(dt.timezone.utc).isoformat()}
                    save_index(idx)
                return jsonify({'ok': True})
            if action == 'done':
                idx = load_index(fresh=True)
                run = dict(body.get('run') or {})
                run['by'] = who
                run['finished_at'] = dt.datetime.now(dt.timezone.utc).isoformat()
                run['cards_total'] = len(idx['cards'])
                idx['runs'] = (idx.get('runs') or []) + [run]
                save_index(idx)
                return jsonify({'ok': True})
        except Exception as e:
            traceback.print_exc()
            return jsonify({'error': f'{type(e).__name__}: {str(e)[:300]}'}), 500
        return jsonify({'error': f'unknown action {action!r}'}), 400
