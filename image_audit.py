"""Weekly "Missing Image and Mismatch Report" (David, Oct 15 2026).

Two tabs, nothing else on them:
  Missing Images        Brand | Style Number | Color Name | Category
  Image vs Color Check  Photo | Result | Brand | Style Numbers | Color Name on the Catalog |
                        What the Photo Shows | Why Flagged | Photo Link   (flagged rows only)

How a build works
  * Every inventory card (the same items /inventory serves) is resolved to the photo the
    DESKTOP CATALOG shows, by mirroring index.html resolveImage: platform override record,
    exact STYLE OVERRIDES file (.jpg/.JPG/.png/.PNG), the base style's STYLE OVERRIDES file
    for size/variant rows, then the shirt brand-folder / DROPBOX_SYNC tiers, the accessory and
    pants DROPBOX_SYNC keys, the server photo route for sportswear, and the swatch fallback.
    Presence comes from S3 folder listings, so no per-card HTTP probing is needed.
  * The color name is the catalog's (getStyleColorInfo): platform override color, then the S3
    color sheet (full SKU, base style, category-aware XX_NNN keys).
  * Each (photo, color name) pair is judged once by Claude and the verdict is cached in S3,
    keyed by the photo's ETag plus the color name. A weekly run only sends pairs whose photo
    or color changed. A small fingerprint per photo (cached the same way) finds one photo used
    under clashing color names.

Storage (bucket is public-read; nothing here is confidential): inventory/image_audit/
  verdicts.json, photos.json, status.json, report_<YYYY-MM-DD>.xlsx

Routes (machine key, or a staff admin session):
  GET  /export-image-audit             the workbook for today (ET); builds it when missing
  GET  /export-image-audit?format=summary
  GET  /admin/image-audit/status
  POST /admin/image-audit/build        start a build now (no email; email lives in open-orders)
  POST /admin/image-audit/verdicts     merge cached verdicts / photo fingerprints

The open-orders platform emails the workbook on Mondays (report key imageAudit). This module
builds it at 6:00 ET on Mondays (IMAGE_AUDIT_BUILD_HOUR_ET) on the leader worker, so the 10:00
send only downloads it.
"""
import base64
import difflib
import hashlib
import io
import json
import os
import re
import threading
import time
import traceback
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from urllib.parse import quote, unquote_plus

try:
    from zoneinfo import ZoneInfo
    ET = ZoneInfo('America/New_York')
except Exception:                       # pragma: no cover
    ET = None

PREFIX = 'inventory/image_audit/'
VERDICTS_KEY = PREFIX + 'verdicts.json'
PHOTOS_KEY = PREFIX + 'photos.json'
STATUS_KEY = PREFIX + 'status.json'
PHOTO_ROOT = 'ALL INVENTORY Photos/'
OVR_DIR = 'STYLE OVERRIDES/'
BRAND_DIR = 'PHOTOS INVENTORY/'
SYNC_DIR = 'DROPBOX_SYNC/'
SWATCH_DIR = 'SWATCH FALLBACK/'

MODEL = os.environ.get('IMAGE_AUDIT_MODEL', 'claude-opus-5-5')
MAX_NEW_PAIRS = int(os.environ.get('IMAGE_AUDIT_MAX_NEW', '600'))
VISION_ON = os.environ.get('IMAGE_AUDIT_VISION', 'on').strip().lower() not in ('off', '0', 'false', 'no')
MAX_BUILD_ATTEMPTS = 4                  # per day, for the Monday pre-build
BATCH = 5
VISION_WORKERS = 3
BUILD_HOUR_ET = int(os.environ.get('IMAGE_AUDIT_BUILD_HOUR_ET', '6'))   # hours of headroom before the 10:00 send
REQUEST_WAIT_S = 75                     # Render's gateway gives up near 100 s
HEARTBEAT_STALE_S = 10 * 60             # a 'building' status with no heartbeat this long is a dead build
STALE_BUILD_S = 3 * 3600                # outer bound for any one build
VISION_PX = 1024                        # long side sent to the model (fine stripes survive)
MAX_PIXELS = 60_000_000                 # refuse decompression bombs without touching Pillow's global
MAX_PHOTO_BYTES = 25 * 1024 * 1024
KEEP_REPORTS = 8

# ── catalog mirrors (index.html) ─────────────────────────────────────────────
# BRAND_IMAGE_PREFIX / SKU_BRAND_CODE_MAP / BRAND_MAPPING keys exactly as the desktop page
# defines them, so the brand correction and photo codes match what staff see.
BRAND_IMAGE_PREFIX = {"NAUTICA": "NA", "DKNY": "DK", "EB": "EB", "REEBOK": "RB", "VINCE": "VC", "BEN": "BE",
                      "USPA": "US", "CHAPS": "CH", "LUCKY": "LB", "JNY": "JN", "BEENE": "GB", "NICOLE": "NM",
                      "SHAQ": "SH", "TAYION": "TA", "STRAHAN": "MS", "VD": "VD", "VERSA": "VR", "CHEROKEE": "CK",
                      "AMERICA": "AC", "BLO": "BL", "BLACK": "BL", "DN": "D9", "KL": "KL", "RG": "RG", "NE": "NE",
                      "DH": "DH", "NW": "NW", "HC": "HC", "MP": "MP", "ZY": "ZY", "CE": "CE", "CL": "CL"}
SKU_BRAND_CODE_MAP = {}
for _k, _p in BRAND_IMAGE_PREFIX.items():
    SKU_BRAND_CODE_MAP[_p] = _k
SKU_BRAND_CODE_MAP.update({'DN': 'DN', 'VS': 'VERSA', 'CS': 'CHAPS', 'NT': 'NAUTICA'})
BRAND_MAPPING_KEYS = {"NAUTICA", "DKNY", "EB", "REEBOK", "VINCE", "BEN", "USPA", "CHAPS", "LUCKY", "JNY", "BEENE",
                      "NICOLE", "SHAQ", "TAYION", "STRAHAN", "VD", "VERSA", "CHEROKEE", "AMERICA", "BLO", "BLACK",
                      "DN", "KL", "NE", "RG", "DH", "NW", "HC", "MP", "ZY", "CE", "CL"}
# BRAND_MAPPING full_name on the desktop page, every key
BRAND_FULL = {'NAUTICA': 'Nautica', 'DKNY': 'DKNY', 'EB': 'Eddie Bauer', 'REEBOK': 'Reebok', 'VINCE': 'Vince Camuto',
              'BEN': 'Ben Sherman', 'USPA': 'U.S. Polo Assn.', 'CHAPS': 'Chaps', 'LUCKY': 'Lucky Brand',
              'JNY': 'Jones New York', 'BEENE': 'Geoffrey Beene', 'NICOLE': 'Nicole Miller',
              'SHAQ': "Shaquille O'Neal", 'TAYION': 'Tayion', 'STRAHAN': 'Michael Strahan', 'VD': 'Von Dutch',
              'VERSA': 'Versa', 'CHEROKEE': 'Cherokee', 'AMERICA': 'American Crew',
              'BLO': 'Bloomingdales Private Label', 'BLACK': 'Black Label', 'DN': 'Divine 9',
              'KL': 'Karl Lagerfeld Paris', 'NE': 'Neiman Marcus', 'RG': 'Robert Graham', 'DH': 'Daniel Hechter',
              'NW': 'Nine West', 'HC': 'Henri Christian', 'MP': 'Modern People', 'ZY': 'Zylos',
              'CE': 'Chuck English', 'CL': 'Christian Lacroix'}
# getBrandFolderUrl's folderMapping (spaces, not '+': these are S3 keys)
FOLDER = {'EB': 'EDDIE BAUER', 'USPA': 'US POLO', 'VINCE': 'VINCE CAMUTO', 'LUCKY': 'LUCKY BRAND',
          'BEN': 'BEN SHERMAN', 'BEENE': 'GEOFFREY BEENE', 'NICOLE': 'NICOLE MILLER',
          'AMERICA': 'AMERICAN CREW', 'TAYION': 'TAYON', 'VD': 'Von Dutch', 'KL': 'KARL LAGERFELD'}
YM = set('KN WT SD SF SB SL SN SV SJ SH SG SS BC BR BH BA CO TH PO PW PJ PH PL HE RB'.split())
SW_COLLARS = set('ZUMNOR')
SW_FABRICS = set('PH PJ PL PO PW TH HE RB'.split())
STD_STYLE = re.compile(r'^[A-Z0-9]{2}[A-Z]{2}[A-Z]{2}[A-Z0-9]{3}[A-Z]{2}[A-Z]?$')


def base_of(sku):
    return str(sku or '').split('-')[0].upper()


def has_pants_serial(b):
    return len(b) >= 10 and b[6] == 'P' and b[7].isdigit() and b[8].isdigit() and 'A' <= b[9] <= 'Z'


def is_young_men(sku):
    b = base_of(sku)
    return len(b) >= 6 and b[4:6] in YM


def is_sportswear(sku):
    b = base_of(sku)
    if not has_pants_serial(b):
        if len(b) >= 11 and b[-1] in SW_COLLARS:
            return True
        if len(b) >= 6 and b[4:6] in SW_FABRICS:
            return True
    return is_young_men(sku)


def is_blazer_or_vest(b):
    return bool(re.match(r'^[BV]\d\d$', b[6:9]))


def item_category(sku, brand):
    """getItemCategory."""
    b = base_of(sku)
    if has_pants_serial(b):
        return 'pants'
    if len(b) >= 11 and b[-1] in SW_COLLARS:
        return 'sportswear'
    if len(b) >= 6 and b[4:6] in SW_FABRICS:
        return 'sportswear'
    br = (brand or '').upper()
    if br == 'CHAPS' and b.startswith('CTH'):
        return 'accessories'
    if br == 'SHAQ' and len(b) >= 3 and 'T' in b[:3]:
        return 'accessories'
    return 'shirts'


def is_shirt_for_imaging(sku, brand):
    return item_category(sku, brand) == 'shirts' and not is_young_men(sku)


def category_label(sku, brand):
    cat = item_category(sku, brand)
    if cat == 'pants':
        return 'Pants'
    if cat == 'sportswear':
        return 'Sportswear'
    if cat == 'accessories':
        return 'Accessory'
    if is_blazer_or_vest(base_of(sku)):
        return 'Blazer / Vest'
    return 'Dress shirt' if not is_young_men(sku) else 'Young Men / Sportswear'


def sku_prefix(sku, brand):
    s = str(sku or '').upper()
    for code in (s[2:4], s[0:2]):
        if len(code) == 2 and brand and SKU_BRAND_CODE_MAP.get(code) == brand:
            return code
    return BRAND_IMAGE_PREFIX.get(brand) or (brand or '')[:2]


def _js_longest(nums):
    best = nums[0]                      # JS reduce: ties go to the later run
    for n in nums[1:]:
        best = best if len(best) > len(n) else n
    return best


def image_code(sku, brand):
    """extractImageCode (zero-padded to 3)."""
    prefix = sku_prefix(sku, brand)
    b = str(sku).split('-')[0]
    nums = re.findall(r'\d+', b)
    if nums:
        return f"{prefix}_{_js_longest(nums).rjust(3, '0')}"
    return f"{prefix}_{b}"


def color_fallback_keys(b, brand):
    """styleColorFallbackKeys."""
    b = str(b or '').upper()
    prefix = sku_prefix(b, brand)
    keys = []
    serial = b[6:9] if len(b) >= 9 else ''
    nums = re.findall(r'\d+', b)
    padded = _js_longest(nums).rjust(3, '0') if nums else None
    if re.match(r'^P\d\d$', serial) or has_pants_serial(b):
        keys.append(f'{prefix}_{serial}')
    elif is_blazer_or_vest(b):
        keys.append(f'{prefix}_{serial}')
    elif padded and is_sportswear(b):
        keys.append(f'{prefix}_SW_{padded}')
    if padded:
        keys.append(f'{prefix}_{padded}')
    return keys


def img_base_style_for(sku):
    s = str(sku or '').upper()
    i = s.find('-')
    if i < 0:
        return None
    b = s[:i]
    return b if (len(b) >= 8 and b != s) else None


_ABBR = {'BLK': 'Black', 'WHT': 'White', 'BLU': 'Blue', 'NVY': 'Navy', 'GRY': 'Grey', 'SRNTY': 'Serenity', 'TRQ': 'Turquoise'}


def format_color(raw):
    if not raw:
        return ''
    s = str(raw).strip()
    s = re.sub(r'\b([A-Za-z]+)\b',
               lambda m: _ABBR.get(m.group(1).upper()) or (m.group(1)[:1].upper() + m.group(1)[1:].lower()), s)
    return re.sub(r'\s{2,}', ' ', s).strip()


def get_override(ovr, sku):
    """getStyleOverride: exact key, then prefix keys ending in '-'."""
    s = str(sku or '').upper()
    if s in ovr:
        return ovr[s]
    for k, v in ovr.items():
        if k.endswith('-') and s.startswith(k[:-1]):
            return v
    return None


def color_name(ovr, cmap, sku, brand):
    """getStyleColorInfo().display -> (name, came_from_override)."""
    full = str(sku or '').upper().strip()
    b = full.split('-')[0]
    oc = (ovr.get(b) or ovr.get(full) or {}).get('color')
    if oc:
        return str(oc), True
    raw = cmap.get(full) if '-' in full else None
    if not raw:
        raw = cmap.get(b)
    if not raw:
        for k in color_fallback_keys(b, brand):
            raw = cmap.get(k)
            if raw:
                break
    if not raw:
        return '', False
    raw = str(raw)
    if '||' in raw:
        p = raw.split('||')
        return (format_color(p[0]) + ' ' + format_color(p[1])).strip(), False
    return format_color(raw), False


def correct_brand(item, ovr):
    """The brand the desktop files the card under (rebuildAppData's corrections)."""
    br = item.get('brand') or ''
    if br == 'NM':
        br = 'NICOLE'
    s = str(item.get('sku') or '').upper()
    if s.startswith('LUCK'):
        br = 'LUCKY'
    elif s.startswith('VP'):
        br = 'VERSA'
    elif len(s) >= 4:
        cb = SKU_BRAND_CODE_MAP.get(s[2:4])
        if cb and cb != br and br not in BRAND_MAPPING_KEYS:
            br = cb
    o = get_override(ovr, s)
    if o and o.get('brand') and o['brand'] != br:
        br = o['brand']
    return br


def group_key(sku):
    """One row per color on the Missing tab: size rows fold into their style, -V stays."""
    s = str(sku or '').upper()
    b = s.split('-')[0]
    if len(b) < 8:                      # genuinely dashed styles (BEN-22-116) stay whole
        return s
    b2 = re.sub(r'\s.*$', '', b)        # 'BLS19270 L' -> 'BLS19270'
    if len(b2) >= 6:
        b = b2
    return b + ('-V' if s.endswith('-V') else '')


# ── photo resolution against S3 listings ─────────────────────────────────────
def _hit(listing, name):
    v = listing.get(name)
    return v if (v and v[1] > 0) else None      # an empty file never loads in the browser


def resolve_photo(item, brand, ovr, L, dbx_codes, server_probe=None):
    """The photo the desktop card shows, as {'source', 'key'|'data'|'url'|'server', 'etag'}, or None.

    brand is the RAW feed brand (item brand_abbr): resolveImage looks the card up in
    inventoryData, whose rows keep the server's brand (NM, KLP, RB), not the corrected one.
    L = {'ovr': {...}, 'brand': {...}, 'sync': {...}, 'swatch': {...}} with names relative to
    each folder and values (etag, size). server_probe(sku, brand) -> bytes|None stands in for
    the catalog's /image/<sku>?brand= proxy rung."""
    sku = str(item.get('sku') or '').upper()
    o = get_override(ovr, sku)
    if not (o and o.get('image')):
        ob = img_base_style_for(sku)
        if ob:
            bo = get_override(ovr, ob)
            if bo and bo.get('image'):
                o = bo
    if o and o.get('image'):
        img = str(o['image'])
        if img.startswith('data:'):
            return {'source': 'override record', 'data': img}
        if img.startswith('http'):
            path = img.split('?')[0]
            in_ovr = '/STYLE+OVERRIDES/' in path or '/STYLE%20OVERRIDES/' in path
            name = unquote_plus(path.split('/STYLE+OVERRIDES/')[-1].split('/STYLE%20OVERRIDES/')[-1])
            h = _hit(L['ovr'], name)
            if h:
                return {'source': 'card photo', 'key': PHOTO_ROOT + OVR_DIR + name, 'etag': h[0]}
            if in_ovr:
                return None             # the file is gone: the card shows a broken image
            return {'source': 'override record', 'url': img}
    for e in ('.jpg', '.JPG', '.png', '.PNG'):
        h = _hit(L['ovr'], sku + e)
        if h:
            return {'source': 'card photo', 'key': PHOTO_ROOT + OVR_DIR + sku + e, 'etag': h[0]}
    ob = img_base_style_for(sku)
    if ob:
        for e in ('.jpg', '.JPG', '.png', '.PNG'):
            h = _hit(L['ovr'], ob + e)
            if h:
                return {'source': 'card photo', 'key': PHOTO_ROOT + OVR_DIR + ob + e, 'etag': h[0]}
    shirt = is_shirt_for_imaging(sku, brand)
    cat = item_category(sku, brand)
    cands = []
    if shirt:
        code = image_code(sku, brand)
        folder = FOLDER.get(brand, brand)
        cands += [('brand', f'{folder}/{code}.jpg'), ('brand', f'{folder}/{code}.png')]
        if code in dbx_codes:
            cands += [('sync', f'{code}.jpg'), ('sync', f'{code}.png')]
    if cat == 'accessories':
        # resolveImage builds _accSku and _accBase from the same full SKU, so only the
        # full SKU (dashes as underscores) is ever probed: CTH02-V never shows CTH02.jpg.
        code = sku.replace('-', '_')
        cands += [('sync', code + e) for e in ('.jpg', '.png', '.JPG', '.PNG')]
    if cat not in ('shirts', 'accessories'):
        if len(sku) >= 10 and sku[6] == 'P' and sku[7].isdigit() and sku[8].isdigit():
            pc = f"{sku_prefix(sku, brand)}_P{int(sku[7:9])}"
            if pc in dbx_codes:
                cands += [('sync', pc + '.jpg'), ('sync', pc + '.png')]
    for where, name in cands:
        h = _hit(L[where], name)
        if h:
            d = BRAND_DIR if where == 'brand' else SYNC_DIR
            return {'source': 'shared brand photo', 'key': PHOTO_ROOT + d + name, 'etag': h[0], 'code': image_code(sku, brand)}
    if server_probe and (is_sportswear(sku) or (shirt and image_code(sku, brand) in dbx_codes)):
        try:
            raw = server_probe(sku, brand)
        except Exception:
            raw = None
        if raw:
            return {'source': 'server photo', 'server': (sku, brand), 'etag': hashlib.md5(raw).hexdigest(),
                    'bytes': raw}
    # the desktop passes the FULL SKU here (size digits included), unlike the color lookup
    for k in color_fallback_keys(sku, brand):
        h = _hit(L['swatch'], k + '.jpg')
        if h:
            return {'source': 'swatch', 'key': PHOTO_ROOT + SWATCH_DIR + k + '.jpg', 'etag': h[0]}
    return None


# ── color-name semantics (one photo under clashing names) ───────────────────
_FAM = {
    'white': ['white', 'ivory', 'cream', 'ecru', 'snow', 'vanilla'],
    'black': ['black', 'jet', 'onyx', 'ebony'],
    'navy': ['navy', 'peacoat', 'midnight', 'indigo'],
    'blue': ['blue', 'sky', 'french', 'cobalt', 'royal', 'serenity', 'periwinkle', 'aqua', 'turquoise', 'denim',
             'chambray', 'powder', 'cerulean', 'sapphire', 'azure', 'steel'],
    'red': ['red', 'scarlet', 'crimson', 'chili', 'chilli', 'cherry', 'tomato'],
    'burgundy': ['burgundy', 'wine', 'maroon', 'merlot', 'oxblood'],
    'pink': ['pink', 'rose', 'blush', 'salmon', 'fuchsia', 'magenta', 'coral', 'peach'],
    'purple': ['purple', 'lilac', 'lavender', 'violet', 'plum', 'orchid', 'mauve', 'iris'],
    'green': ['green', 'sage', 'olive', 'mint', 'emerald', 'hunter', 'teal', 'army', 'forest', 'lime', 'erin'],
    'grey': ['grey', 'gray', 'charcoal', 'silver', 'slate', 'graphite', 'smoke', 'pewter', 'heather'],
    'brown': ['brown', 'chocolate', 'coffee', 'mocha', 'rust', 'cognac'],
    'khaki': ['khaki', 'tan', 'beige', 'sand', 'oat', 'stone', 'camel', 'taupe', 'bone', 'natural'],
    'yellow': ['yellow', 'gold', 'butter', 'buttermilk', 'mustard', 'lemon'],
    'orange': ['orange', 'tangerine', 'apricot', 'burnt'],
}
_PAT = {
    'stripe': ['stripe', 'stripes', 'pinstripe', 'pinstripes', 'striped', 'bengal', 'awning', 'hickory'],
    'check': ['check', 'checks', 'gingham', 'plaid', 'tartan', 'windowpane', 'graph', 'grid', 'tattersall',
              'houndstooth', 'buffalo'],
    'print': ['print', 'geo', 'dot', 'dots', 'paisley', 'medallion', 'floral', 'overprint', 'diamond', 'neat', 'foulard'],
    'solid': ['solid', 'sld'],
    'texture': ['dobby', 'herringbone', 'jacquard', 'basketweave', 'teardrop', 'wavy', 'twill', 'oxford',
                'birdseye', 'textured'],
}
_W2F = {w: f for f, ws in _FAM.items() for w in ws}
_W2P = {w: p for p, ws in _PAT.items() for w in ws}
_VOCAB = list(_W2F) + list(_W2P)


def _canon(w):
    if w in _W2F or w in _W2P:
        return w
    if len(w) >= 4:
        m = difflib.get_close_matches(w, _VOCAB, n=1, cutoff=0.8)
        if m:
            return m[0]
    return w


def _describe(name):
    s = str(name or '').lower()
    s = re.sub(r'/[^/]*\bhorse\b.*$', '', s)            # "... / Navy Horse" is the logo color
    s = re.sub(r'\b[\w ]*\bhorse\b', '', s)
    s = re.sub(r'w/\s*[\w ]*\b(icon|logo|embroidery|patch)\b.*$', '', s)
    s = s.replace('tear drop', 'teardrop').replace('teardrp', 'teardrop').replace('multi color', 'multicolor')
    words = [_canon(w) for w in re.findall(r'[a-z]+', s)]
    fam = {_W2F[w] for w in words if w in _W2F}
    pats = [_W2P[w] for w in words if w in _W2P]
    tex = sorted({w for w in words if _W2P.get(w) == 'texture'} - {'dobby'})
    if 'dobby' in words:                # any dobby counts as a solid (David, Aug 24 2026)
        pats = ['solid']
    pat = next((p for p in ('check', 'stripe', 'print') if p in pats), 'solid' if pats else None)
    return fam, pat, tex


def names_conflict(a, b):
    fa, pa, ta = _describe(a)
    fb, pb, tb = _describe(b)
    if fa and fb and not (fa & fb):
        return True
    if pa and pb and pa != pb:
        return True
    return bool(ta and tb and ta != tb)


def fingerprint(jpeg_bytes):
    """{'dh': 256-bit gradient hash hex, 'th': 8x8 RGB thumbnail hex} of a resized photo."""
    from PIL import Image
    im = Image.open(io.BytesIO(jpeg_bytes))
    g = im.convert('L').resize((17, 16), Image.LANCZOS)
    px = g.tobytes()
    bits = 0
    for r in range(16):
        for c in range(16):
            bits = (bits << 1) | (1 if px[r * 17 + c] > px[r * 17 + c + 1] else 0)
    th = im.convert('RGB').resize((8, 8), Image.BOX).tobytes()
    return {'dh': format(bits, '064x'), 'th': th.hex()}


def _same_photo(fa, fb):
    if bin(int(fa['dh'], 16) ^ int(fb['dh'], 16)).count('1') > 10:
        return False
    a, b = bytes.fromhex(fa['th']), bytes.fromhex(fb['th'])
    return sum(abs(x - y) for x, y in zip(a, b)) / 192.0 < 8


def photo_groups(prints):
    """{photo key: group id}: identical or near-identical photos share a group."""
    keys = sorted(prints)
    parent = {k: k for k in keys}

    def find(x):
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x
    hv = [(k, int(prints[k]['dh'], 16)) for k in keys]
    for i in range(len(hv)):
        for j in range(i + 1, len(hv)):
            if bin(hv[i][1] ^ hv[j][1]).count('1') <= 10 and _same_photo(prints[hv[i][0]], prints[hv[j][0]]):
                a, b = find(hv[i][0]), find(hv[j][0])
                if a != b:
                    parent[a] = b
    return {k: find(k) for k in keys}


# ── vision check ─────────────────────────────────────────────────────────────
SYSTEM_PROMPT = """You check product photos for Versa Group, a men's apparel company (dress shirts, sport shirts, polos, pants, blazers, ties, jewelry). Each photo comes with the color name the catalog shows for it. Decide whether the photo plausibly shows that color name.

How Versa names colors:
- "Grnd" means ground: the background color of a check, stripe or print. "White Grnd Navy Check" is a white background with a navy check.
- "Dobby" (and jacquard, twill, herringbone, oxford, end on end, textured) is a woven texture that looks solid or nearly solid. A dobby named with a stripe or check word may show a tonal stripe or check. Treat it as a solid of its color.
- "Solid"/"Sld" means one color with no visible pattern. A bare color like "White" or "Navy" also implies a solid.
- "Heather" is a mottled solid. "Chambray" is a slub, denim-look solid.
- Shade names (Serenity, Peacoat, Ensign, Cobalt, Sky, Tony Blue, Wine, Charcoal, Stone, Khaki, Oat, Sage, Lilac and so on) are shades. Be lenient on shade within one color family (navy vs dark blue, light blue vs sky blue, grey vs silver, burgundy vs wine).
- Names can list several colors ("Navy & Burgundy Check"). A name split by " / " is either ground / pattern or body / trim or logo color ("White Solid / Navy Horse").
- U.S. Polo Assn. names sometimes describe only the horse logo ("Peacoat Horse", "Multi Color Horse"). Then judge only what the name describes.

Verdicts:
- "mismatch": a clear contradiction a buyer would notice. The name says black but the item is white; the name says a solid but the photo shows an obvious check, stripe or print (or the reverse); the main color family is plainly different; or the named pattern colors are plainly absent.
- "unclear": you cannot judge (photo too small or too dark, a borderline shade), or the photo plainly shows a different kind of item than the color name describes (for example a colorblock polo name on a plain dress shirt photo). Say which in the reason.
- "match": consistent with the name, allowing for lighting, shade naming and texture words.
Fine patterns: if the name has a pinstripe, hairline, end on end, micro or mini check, gingham or tattersall and the item looks solid, the pattern may be too small to see. Answer "unclear", not "mismatch".
Severity for a mismatch: "high" for a different main color family or a solid-vs-pattern contradiction, "medium" for a wrong secondary or pattern color or a clearly different shade, "low" for a minor naming quibble. Use "none" when the verdict is not a mismatch.
photo_problem: "blank" when the image is empty or blank, "not_product" when it is only a logo, artwork or a sketch instead of the item, otherwise "none". With "blank" or "not_product" the verdict is "unclear". A photo marked "fabric swatch" is a flat fabric sample the catalog shows in place of the garment: judge its color and pattern against the name as usual and never call it not_product.
The item type given with each photo comes from the style code and can be wrong (jewelry, pants and blazers are sometimes filed as shirts). Never call a mismatch because of the item type.
seen_other_colors: the other visible colors, or an empty string when there are none.
Write short, plain US English. Spell it "color". Do not use dashes as punctuation."""

RESULT_SCHEMA = {
    'type': 'object',
    'properties': {'photos': {'type': 'array', 'items': {
        'type': 'object',
        'properties': {
            'id': {'type': 'string'},
            'seen_main_color': {'type': 'string'},
            'seen_other_colors': {'type': 'string'},
            'seen_pattern': {'type': 'string', 'enum': ['solid', 'texture', 'heather', 'stripe', 'check',
                                                        'print', 'multiple items', 'other']},
            'photo_problem': {'type': 'string', 'enum': ['none', 'blank', 'not_product']},
            'verdict': {'type': 'string', 'enum': ['match', 'mismatch', 'unclear']},
            'severity': {'type': 'string', 'enum': ['high', 'medium', 'low', 'none']},
            'reason': {'type': 'string'},
        },
        'required': ['id', 'seen_main_color', 'seen_other_colors', 'seen_pattern', 'photo_problem',
                     'verdict', 'severity', 'reason'],
        'additionalProperties': False}}},
    'required': ['photos'],
    'additionalProperties': False,
}


def us_copy(s):
    """Report copy rules: US 'color', no em or en dashes."""
    s = re.sub(r'([Cc])olour', r'\1olor', str(s or ''))
    s = re.sub(r'COLOUR', 'COLOR', s)
    s = re.sub('\\s*\u2014\\s*', ', ', s)
    return re.sub('\\s*\u2013\\s*', '-', s).strip()


_NO_OTHER = {'none', 'n/a', 'na', '-', 'null', 'no', 'nothing', 'no other colors', 'no other color',
             'no secondary color', 'no secondary colors'}


def seen_text(main, other, pattern):
    """'white, light blue (check)'; placeholder second colors ('none', 'n/a') are dropped."""
    other = str(other or '').strip()
    if other.lower().strip('. ') in _NO_OTHER:
        other = ''
    s = ', '.join(x for x in [str(main or '').strip(), other] if x)
    if pattern:
        s += f' ({pattern})'
    return us_copy(s)


def clean_seen(s):
    """Same cleanup for verdicts cached before seen_text existed."""
    return us_copy(re.sub(r',\s*(none|n/a|na|null|no other colou?rs?)\s*(?=\(|$)', ' ', str(s or ''),
                          flags=re.I).replace('  ', ' '))


FALLBACK_MODEL = os.environ.get('IMAGE_AUDIT_FALLBACK_MODEL', 'claude-opus-5')


def ask_model(client, content):
    """(response, model used). Plain messages.create, the call the platform's agent already makes
    with this server's SDK; the refusal fallback goes in as request fields so no SDK version can
    reject it. A 400/404 (fallback option or model not available to this key) retries without the
    option, then on the platform's agent model."""
    base = dict(max_tokens=8000, system=SYSTEM_PROMPT, messages=[{'role': 'user', 'content': content}],
                output_config={'effort': 'low', 'format': {'type': 'json_schema', 'schema': RESULT_SCHEMA}})
    c = client.with_options(timeout=150.0, max_retries=2)
    tries = [dict(model=MODEL, extra_headers={'anthropic-beta': 'server-side-fallback-2026-07-01'},
                  extra_body={'fallbacks': 'default'}),
             dict(model=MODEL)]
    if FALLBACK_MODEL and FALLBACK_MODEL != MODEL:
        tries.append(dict(model=FALLBACK_MODEL))
    last = None
    for t in tries:
        try:
            return c.messages.create(**base, **t), t['model']
        except Exception as e:
            if getattr(e, 'status_code', None) in (400, 404):
                last = e
                print(f"[image-audit] {t['model']} call rejected ({getattr(e, 'status_code', '')}): "
                      f"{str(e)[:300]}", flush=True)
                continue
            raise
    raise last


def vision_batch(client, batch, today):
    """Judge up to BATCH pairs in one request. Returns {pair id: verdict record}; pairs the
    model did not answer (refusal, error) are simply absent and retried next run."""
    content = []
    for i, p in enumerate(batch, 1):
        kind = ' | photo type: fabric swatch' if p.get('source') == 'swatch' else ''
        content.append({'type': 'text', 'text': f'Photo {i}: id={p["id"]} | catalog color name: "{p["color"]}" '
                                                f'| item type from the style code (may be wrong): {p["hint"]}{kind}'})
        content.append({'type': 'image', 'source': {'type': 'base64', 'media_type': 'image/jpeg',
                                                    'data': base64.standard_b64encode(p['jpeg']).decode()}})
    content.append({'type': 'text', 'text': f'Judge these {len(batch)} photos and return one entry per photo id.'})
    resp, used = ask_model(client, content)
    if getattr(resp, 'stop_reason', None) == 'refusal':
        print(f"[image-audit] vision batch declined ({getattr(resp, 'stop_details', None)})", flush=True)
        return {}
    text = ''.join(getattr(b, 'text', '') for b in resp.content if getattr(b, 'type', '') == 'text')
    data = json.loads(text)
    want = {p['id']: p for p in batch}
    out = {}
    for o in data.get('photos') or []:
        pid = str(o.get('id') or '')
        if pid not in want:
            continue
        v = o.get('verdict') if o.get('verdict') in ('match', 'mismatch', 'unclear') else 'unclear'
        sev = o.get('severity') if (v == 'mismatch' and o.get('severity') in ('high', 'medium', 'low')) else ''
        prob = o.get('photo_problem') if o.get('photo_problem') in ('blank', 'not_product') else ''
        if prob == 'not_product' and want[pid].get('source') == 'swatch':
            prob = ''                   # a swatch is what the catalog shows; keep the color verdict
        seen = seen_text(o.get('seen_main_color'), o.get('seen_other_colors'), o.get('seen_pattern'))
        out[pid] = {'etag': want[pid]['etag'], 'color': want[pid]['color'], 'verdict': v, 'severity': sev,
                    'kind': 'blank' if prob == 'blank' else ('not_product' if prob == 'not_product' else ''),
                    'seen': seen, 'reason': us_copy(o.get('reason', '')), 'checked': today, 'by': used}
    return out


RESULT_ORDER = ['Mismatch (high)', 'Blank photo', 'Mismatch (medium)', 'No color name',
                'Logo or sketch, not the item', 'Mismatch (low)', 'Check by eye', 'Same photo, different colors']
RESULT_FILL = {'Mismatch (high)': '#F8D7DA', 'Blank photo': '#F8D7DA', 'Mismatch (medium)': '#FDE8CF',
               'Mismatch (low)': '#FFF4CC', 'Logo or sketch, not the item': '#E8EEF9', 'Check by eye': '#E8EEF9',
               'No color name': '#F3E8FF', 'Same photo, different colors': '#F5F5F5'}


def result_label(rec):
    v, sev, kind = rec.get('verdict'), rec.get('severity'), rec.get('kind')
    if v == 'mismatch':
        return {'high': 'Mismatch (high)', 'medium': 'Mismatch (medium)', 'low': 'Mismatch (low)'}.get(sev, 'Mismatch (medium)')
    if v == 'unclear':
        if kind == 'blank':
            return 'Blank photo'
        if kind == 'not_product':
            return 'Logo or sketch, not the item'
        return 'Check by eye'
    return 'OK'


def pair_id(photo_key, color):
    return hashlib.md5((str(photo_key) + '|' + str(color).upper()).encode()).hexdigest()[:10]


# ── workbook ─────────────────────────────────────────────────────────────────
def write_workbook(missing_rows, flagged_rows):
    """xlsx bytes. missing_rows: dicts brand/style/color/category. flagged_rows: dicts
    result/brand/styles/color/seen/why/link/thumb (jpeg bytes or None)."""
    import xlsxwriter
    buf = io.BytesIO()
    wb = xlsxwriter.Workbook(buf, {'in_memory': True, 'strings_to_formulas': False, 'strings_to_urls': False})
    hdr = wb.add_format({'bold': True, 'font_color': '#FFFFFF', 'bg_color': '#1F2A44', 'valign': 'vcenter',
                         'text_wrap': True, 'border': 1, 'border_color': '#1F2A44'})
    cell = wb.add_format({'valign': 'top', 'bottom': 1, 'bottom_color': '#D0D5DD'})
    wrap = wb.add_format({'valign': 'top', 'text_wrap': True, 'bottom': 1, 'bottom_color': '#D0D5DD'})
    link = wb.add_format({'valign': 'top', 'font_color': '#1155CC', 'underline': 1, 'bottom': 1,
                          'bottom_color': '#D0D5DD'})
    res_fmt = {k: wb.add_format({'valign': 'top', 'bold': True, 'bg_color': v, 'bottom': 1, 'bottom_color': '#D0D5DD'})
               for k, v in RESULT_FILL.items()}

    ws = wb.add_worksheet('Missing Images')
    for c, (name, width) in enumerate([('Brand', 18), ('Style Number', 24), ('Color Name', 44), ('Category', 18)]):
        ws.write(0, c, name, hdr)
        ws.set_column(c, c, width)
    for r, row in enumerate(missing_rows, 1):
        ws.write_string(r, 0, row['brand'], cell)
        ws.write_string(r, 1, row['style'], cell)
        ws.write_string(r, 2, row['color'], wrap)
        ws.write_string(r, 3, row['category'], cell)
    ws.freeze_panes(1, 0)
    ws.autofilter(0, 0, max(len(missing_rows), 1), 3)

    ws2 = wb.add_worksheet('Image vs Color Check')
    cols = [('Photo', 15), ('Result', 18), ('Brand', 16), ('Style Numbers', 28), ('Color Name on the Catalog', 32),
            ('What the Photo Shows', 32), ('Why Flagged', 52), ('Photo Link', 12)]
    for c, (name, width) in enumerate(cols):
        ws2.write(0, c, name, hdr)
        ws2.set_column(c, c, width)
    THUMB = 96
    for r, row in enumerate(flagged_rows, 1):
        ws2.write_blank(r, 0, None, cell)
        ws2.write_string(r, 1, row['result'], res_fmt.get(row['result'], cell))
        ws2.write_string(r, 2, row['brand'], cell)
        ws2.write_string(r, 3, row['styles'], wrap)
        ws2.write_string(r, 4, row['color'], wrap)
        ws2.write_string(r, 5, row['seen'], wrap)
        ws2.write_string(r, 6, row['why'], wrap)
        if row.get('link'):
            ws2.write_url(r, 7, row['link'], link, 'Open photo')
        else:
            ws2.write_blank(r, 7, None, cell)
        if row.get('thumb'):
            try:
                from PIL import Image
                w, h = Image.open(io.BytesIO(row['thumb'])).size
                sc = THUMB / float(max(w, h))
                ws2.insert_image(r, 0, 'photo.jpg', {'image_data': io.BytesIO(row['thumb']), 'x_scale': sc,
                                                    'y_scale': sc, 'x_offset': 3, 'y_offset': 3,
                                                    'object_position': 1})
                ws2.set_row(r, THUMB * 0.78)
            except Exception:
                pass
    ws2.freeze_panes(1, 0)
    ws2.autofilter(0, 0, max(len(flagged_rows), 1), 7)
    wb.close()
    return buf.getvalue()


# ── registration ─────────────────────────────────────────────────────────────
def register_image_audit_routes(app, *, get_s3, s3_bucket, region, items, overrides, dropbox_codes,
                                color_map, server_photo, anthropic_client, identity, off_loop=None,
                                is_leader=None, start_scheduler=True, photo_hosts=()):
    """Wire the routes and the Monday pre-build. Hooks (all callables):
      items() -> list of inventory item dicts      overrides() -> {KEY: override dict}
      dropbox_codes() -> set of image codes         color_map() -> {KEY: raw color}
      server_photo(sku, brand) -> bytes|None        anthropic_client() -> anthropic.Anthropic
      identity() -> request identity dict           off_loop(fn, *a) -> run CPU work off the gevent loop
      is_leader() -> bool
    photo_hosts: extra host names (CloudFront) an override image URL may be fetched from."""
    from flask import jsonify, request, send_file

    public_base = f'https://{s3_bucket}.s3.{region}.amazonaws.com/'
    # Override 'image' values are staff-controlled: only our own photo hosts are ever fetched.
    fetch_hosts = {f'{s3_bucket}.s3.{region}.amazonaws.com', f'{s3_bucket}.s3.amazonaws.com'}
    fetch_hosts |= {str(h).lower() for h in (photo_hosts or ()) if h}
    run = off_loop or (lambda fn, *a: fn(*a))
    state = {'building': False, 'started': None, 'last_error': None, 'last_summary': None}
    lock = threading.Lock()
    hb = {'status': None}               # the 'building' status this worker keeps alive

    def now_et():
        return datetime.now(ET) if ET else datetime.utcnow()

    # S3 helpers
    def s3_get_json(key, default, strict=False):
        """default for a missing key. Any other failure returns default too, or raises when
        strict (a cache that cannot be read must never be rewritten as empty)."""
        try:
            obj = get_s3().get_object(Bucket=s3_bucket, Key=key)
            data = json.loads(obj['Body'].read().decode('utf-8'))
        except Exception as e:
            code = getattr(e, 'response', {}).get('Error', {}).get('Code', '') if hasattr(e, 'response') else ''
            if code in ('NoSuchKey', '404', 'NotFound'):
                return default
            if strict:
                raise
            print(f"[image-audit] read {key} failed: {e}", flush=True)
            return default
        if isinstance(default, dict) and not isinstance(data, dict):
            if strict:
                raise ValueError(f'{key} is not a JSON object')
            return default
        return data

    def s3_put(key, body, ctype):
        get_s3().put_object(Bucket=s3_bucket, Key=key, Body=body, ContentType=ctype)

    def s3_put_json(key, data):
        s3_put(key, json.dumps(data, separators=(',', ':')).encode('utf-8'), 'application/json')

    def merge_save(key, fresh, base=None):
        """Write base + what S3 holds now + fresh. A failed read raises, so a write can never
        shrink the file; base (the build's own copy) keeps the working set whole either way."""
        merged = dict(base or {})
        merged.update(s3_get_json(key, {}, strict=True))
        merged.update(fresh)
        s3_put_json(key, merged)
        return merged

    def read_status():
        st = s3_get_json(STATUS_KEY, {})
        return st if isinstance(st, dict) else {}

    def heartbeat():
        st = hb.get('status')
        if not st:
            return
        st['heartbeat_at'] = time.time()
        try:
            s3_put_json(STATUS_KEY, st)
        except Exception as e:
            print(f"[image-audit] heartbeat failed: {e}", flush=True)

    def list_dir(rel):
        out, token = {}, None
        prefix = PHOTO_ROOT + rel
        while True:
            kw = {'Bucket': s3_bucket, 'Prefix': prefix, 'MaxKeys': 1000}
            if token:
                kw['ContinuationToken'] = token
            resp = get_s3().list_objects_v2(**kw)
            for o in resp.get('Contents', []) or []:
                out[o['Key'][len(prefix):]] = (str(o.get('ETag', '')).strip('"'), int(o.get('Size') or 0))
            if not resp.get('IsTruncated'):
                break
            token = resp.get('NextContinuationToken')
        return out

    def report_key(day):
        return f'{PREFIX}report_{day}.xlsx'

    def host_ok(url):
        from urllib.parse import urlparse
        try:
            u = urlparse(str(url))
        except Exception:
            return False
        return u.scheme == 'https' and (u.hostname or '').lower() in fetch_hosts

    def photo_url(photo):
        if photo.get('key'):
            return public_base + quote(photo['key'], safe='/')
        if photo.get('url'):
            return photo['url'] if host_ok(photo['url']) else ''
        if photo.get('server'):
            sku, brand = photo['server']
            return f"https://versa-inventory-api.onrender.com/image/{quote(sku, safe='')}?brand={quote(brand)}"
        return ''

    def fetch_url(url):
        """Bytes of an override image URL on our own photo hosts, capped; None otherwise."""
        if not host_ok(url):
            return None
        import requests
        r = requests.get(url, timeout=(5, 20), allow_redirects=False, stream=True)
        try:
            if r.status_code != 200 or 'image' not in r.headers.get('Content-Type', '').lower():
                return None
            buf = io.BytesIO()
            for chunk in r.iter_content(65536):
                buf.write(chunk)
                if buf.tell() > MAX_PHOTO_BYTES:
                    return None
            return buf.getvalue()
        finally:
            r.close()

    blob_store = {}                     # the running build's blobs (see build)

    def raw_bytes(photo):
        if photo.get('bytes'):
            return photo['bytes']
        if photo.get('blob'):
            return blob_store.get(photo['blob'])
        if photo.get('data'):
            return base64.b64decode(photo['data'].split(',', 1)[1])
        if photo.get('key'):
            return get_s3().get_object(Bucket=s3_bucket, Key=photo['key'])['Body'].read()
        if photo.get('url'):
            return fetch_url(photo['url'])
        if photo.get('server'):
            return server_photo(*photo['server'])
        return None

    def to_jpeg(raw, size):
        from PIL import Image, ImageOps
        im = Image.open(io.BytesIO(raw))
        if im.size[0] * im.size[1] > MAX_PIXELS:
            raise ValueError(f'image too large ({im.size[0]}x{im.size[1]})')
        im.draft('RGB', (size * 2, size * 2))
        im = ImageOps.exif_transpose(im)
        if im.mode in ('RGBA', 'LA', 'P'):
            im = im.convert('RGBA')
            bg = Image.new('RGB', im.size, (255, 255, 255))
            bg.paste(im, mask=im.split()[-1])
            im = bg
        else:
            im = im.convert('RGB')
        im.thumbnail((size, size), Image.LANCZOS)
        out = io.BytesIO()
        im.save(out, 'JPEG', quality=85)
        return out.getvalue()

    def styles_of(cards):
        return sorted({group_key(c['sku']) for c in cards})

    # the build
    def build(day, log=print, beat=lambda: None):
        t0 = time.time()
        ovr = {str(k).upper(): v for k, v in (overrides() or {}).items() if isinstance(v, dict)}
        cmap = color_map() or {}
        dbx = set(dropbox_codes() or ())
        its = list(items() or [])
        # A half-loaded worker would mail a wrong report: fail instead (the attempt cap retries).
        if not its:
            raise RuntimeError('inventory is not loaded on this worker yet')
        if not cmap:
            raise RuntimeError('the S3 color sheet is not loaded on this worker yet')
        if not dbx:
            raise RuntimeError('the Dropbox photo index is not loaded on this worker yet')
        verdicts = s3_get_json(VERDICTS_KEY, {}, strict=True)
        prints = s3_get_json(PHOTOS_KEY, {}, strict=True)
        blobs = blob_store              # md5 -> one 1024 px JPEG per server or URL photo
        blobs.clear()
        L = {'ovr': list_dir(OVR_DIR), 'brand': list_dir(BRAND_DIR), 'sync': list_dir(SYNC_DIR),
             'swatch': list_dir(SWATCH_DIR)}
        log(f"[image-audit] {len(its)} cards; listings ovr={len(L['ovr'])} brand={len(L['brand'])} "
            f"sync={len(L['sync'])} swatch={len(L['swatch'])}")

        def one(it):
            brand = correct_brand(it, ovr)                  # the brand the card is filed under
            raw_brand = str(it.get('brand_abbr') or it.get('brand') or '').upper()
            photo = resolve_photo(it, raw_brand, ovr, L, dbx, server_probe=server_photo)
            if photo and photo.get('url') and not photo.get('etag'):
                # key a URL photo on its bytes, so a replaced file is judged again
                try:
                    b = fetch_url(photo['url'])
                except Exception:
                    b = None
                if b:
                    photo['bytes'], photo['etag'] = b, hashlib.md5(b).hexdigest()
            if photo and photo.get('bytes'):
                # keep one shrunken copy per distinct photo, not a full original per card
                k = photo['etag']
                if k not in blobs:
                    try:
                        blobs[k] = run(to_jpeg, photo['bytes'], VISION_PX)
                    except Exception:
                        blobs[k] = None
                photo.pop('bytes', None)
                photo['blob'] = k
            color, _from_ovr = color_name(ovr, cmap, it.get('sku'), brand)
            return {'sku': str(it.get('sku') or '').upper(), 'brand': brand, 'photo': photo, 'color': color,
                    'category': category_label(it.get('sku'), brand)}
        with ThreadPoolExecutor(8) as ex:        # only the server-photo and URL rungs do network work
            cards = list(ex.map(one, its))
        beat()

        # Missing tab: one row per color
        groups = {}
        for c in cards:
            if c['photo']:
                continue
            groups.setdefault(group_key(c['sku']), []).append(c)
        missing_rows = []
        for k, cs in groups.items():
            c0 = sorted(cs, key=lambda c: (len(c['sku']), c['sku']))[0]
            missing_rows.append({'brand': BRAND_FULL.get(c0['brand'], c0['brand']), 'style': k,
                                 'color': us_copy(c0['color']) or '(no color name)', 'category': c0['category']})
        missing_rows.sort(key=lambda r: (r['brand'].lower(), r['style']))

        # pairs
        pairs, no_color = {}, {}
        for c in cards:
            ph = c['photo']
            if not ph:
                continue
            pkey = ph.get('etag') or hashlib.md5((ph.get('data') or ph.get('url') or '').encode()).hexdigest()
            ph['pkey'] = pkey
            if not c['color']:
                no_color.setdefault((pkey, c['brand']), []).append(c)
                continue
            pid = pair_id(pkey, c['color'])
            p = pairs.setdefault(pid, {'id': pid, 'pkey': pkey, 'photo': ph, 'color': c['color'], 'cards': [],
                                       'hint': c['category'], 'source': ph.get('source')})
            p['cards'].append(c)

        new = [p for p in pairs.values() if not isinstance(verdicts.get(p['id']), dict)]
        new.sort(key=lambda p: p['cards'][0]['sku'])
        over_cap = max(0, len(new) - MAX_NEW_PAIRS)
        new = new[:MAX_NEW_PAIRS]

        # photos to fetch: new pairs (vision) and photos with no fingerprint yet
        jpeg640 = {}
        need = {}
        for p in new:
            need[p['pkey']] = p['photo']
        for p in pairs.values():
            if p['pkey'] not in prints:
                need[p['pkey']] = p['photo']

        def fetch(item):
            k, ph = item
            try:
                raw = raw_bytes(ph)
                return k, (run(to_jpeg, raw, 640) if raw else None)
            except Exception as e:
                log(f"[image-audit] photo {k} unreadable: {e}")
                return k, None
        new_prints = {}
        new_keys = {p['pkey'] for p in new}
        fetched = set()
        with ThreadPoolExecutor(8) as ex:
            for k, j in ex.map(fetch, list(need.items())):
                if j:
                    fetched.add(k)
                    if k in new_keys:
                        jpeg640[k] = j
                    try:
                        new_prints[k] = run(fingerprint, j)
                    except Exception:
                        pass
        if new_prints:
            prints = merge_save(PHOTOS_KEY, new_prints, base=prints)
        beat()
        log(f"[image-audit] {len(pairs)} photo/color pairs, {len(new)} new, {len(fetched)} photos fetched")

        # vision for new pairs, at VISION_PX so fine patterns survive
        todo = [p for p in new if p['pkey'] in jpeg640]
        unreadable = len(new) - len(todo)
        checked_new = 0
        client = None
        vision_errors = []
        if todo and VISION_ON:
            try:
                client = anthropic_client()
            except Exception as e:
                vision_errors.append(f'client: {e}'[:300])
                log(f"[image-audit] vision unavailable: {e}")
        if todo and client is not None:
            batches = [todo[i:i + BATCH] for i in range(0, len(todo), BATCH)]

            def judge(b):
                try:
                    items_ = []
                    for p in b:
                        try:
                            raw = raw_bytes(p['photo'])
                            j = run(to_jpeg, raw, VISION_PX) if raw else None
                        except Exception:
                            j = None
                        items_.append({'id': p['id'], 'color': p['color'], 'hint': p['hint'],
                                       'source': p['source'], 'etag': p['pkey'],
                                       'jpeg': j or jpeg640[p['pkey']]})
                    return vision_batch(client, items_, day)
                except Exception as e:
                    vision_errors.append(f'{type(e).__name__}: {e}'[:300])
                    log(f"[image-audit] vision batch failed: {e}")
                    return {}
            fresh = {}
            with ThreadPoolExecutor(VISION_WORKERS) as ex:
                for i, res in enumerate(ex.map(judge, batches), 1):
                    fresh.update(res)
                    if i % 10 == 0:
                        try:                                # keep progress if the worker restarts
                            merge_save(VERDICTS_KEY, fresh, base=verdicts)
                        except Exception as e:
                            log(f"[image-audit] progress save failed: {e}")
            if fresh:
                verdicts = merge_save(VERDICTS_KEY, fresh, base=verdicts)
        checked_new = sum(1 for p in todo if isinstance(verdicts.get(p['id']), dict))
        not_judged = len(todo) - checked_new
        beat()

        # one photo under clashing color names
        shown = {p['pkey'] for p in pairs.values() if p['pkey'] in prints}
        grp = run(photo_groups, {k: prints[k] for k in shown})
        by_grp = {}
        for p in pairs.values():
            if p['pkey'] in grp:
                by_grp.setdefault(grp[p['pkey']], []).append(p)
        shared = {}
        for plist in by_grp.values():
            if len(plist) < 2:
                continue
            for p in plist:
                others = [q for q in plist if q['id'] != p['id'] and names_conflict(p['color'], q['color'])]
                if others:
                    shared[p['id']] = others

        # flagged rows
        flagged = []
        for p in pairs.values():
            rec = verdicts.get(p['id'])
            if not isinstance(rec, dict):
                continue
            label = result_label(rec)
            why = rec.get('reason') or ''
            if label == 'OK' and p['id'] in shared:
                label = 'Same photo, different colors'
                why = ''
            if label == 'OK':
                continue
            if p['id'] in shared:
                also = '; '.join(f"{q['color']} ({', '.join(styles_of(q['cards'])[:3])})"
                                 for q in shared[p['id']][:4])
                why = (why + ' ' if why else '') + 'Same photo is also used for: ' + also
            c0 = p['cards'][0]
            flagged.append({'result': label, 'brand': BRAND_FULL.get(c0['brand'], c0['brand']),
                            'styles': ', '.join(styles_of(p['cards'])), 'color': us_copy(p['color']),
                            'seen': clean_seen(rec.get('seen')), 'why': us_copy(why),
                            'link': photo_url(p['photo']), 'pkey': p['pkey'], 'photo': p['photo']})
        for (pkey, brand), cs in no_color.items():
            flagged.append({'result': 'No color name', 'brand': BRAND_FULL.get(brand, brand),
                            'styles': ', '.join(styles_of(cs)), 'color': '(no color name)', 'seen': '',
                            'why': 'The card shows a photo but no color name.', 'link': photo_url(cs[0]['photo']),
                            'pkey': pkey, 'photo': cs[0]['photo']})
        order = {k: i for i, k in enumerate(RESULT_ORDER)}
        flagged.sort(key=lambda r: (order.get(r['result'], 99), r['brand'].lower(), r['styles']))

        # thumbnails for the flagged rows
        def thumb(row):
            j = jpeg640.get(row['pkey'])
            try:
                if not j:
                    raw = raw_bytes(row['photo'])
                    j = run(to_jpeg, raw, 640) if raw else None
                return run(to_jpeg, j, 192) if j else None
            except Exception:
                return None
        with ThreadPoolExecutor(8) as ex:
            for row, t in zip(flagged, ex.map(thumb, flagged)):
                row['thumb'] = t
        beat()

        xlsx = run(write_workbook, missing_rows, flagged)
        counts = {}
        for r in flagged:
            counts[r['result']] = counts.get(r['result'], 0) + 1
        summary = {'date': day, 'cards': len(cards), 'missing_colors': len(missing_rows),
                   'missing_cards': sum(1 for c in cards if not c['photo']), 'pairs': len(pairs),
                   'flagged': len(flagged), 'by_result': counts, 'checked_new': checked_new,
                   'unchecked': over_cap + unreadable + not_judged, 'over_cap': over_cap,
                   'unreadable': unreadable, 'not_judged': not_judged,
                   'vision_error': vision_errors[0] if vision_errors else '',
                   'seconds': round(time.time() - t0), 'built_at': now_et().isoformat(timespec='seconds')}
        return xlsx, summary

    def run_build(day, reason):
        with lock:
            if state['building']:
                return False
            state['building'], state['started'] = True, time.time()
        attempts = 1
        try:
            prev = read_status()
            attempts = int(prev.get('attempts') or 0) + 1 if prev.get('date') == day else 1
            st = {'state': 'building', 'date': day, 'started_at': time.time(), 'heartbeat_at': time.time(),
                  'reason': reason, 'attempts': attempts, 'pid': os.getpid()}
            hb['status'] = st
            s3_put_json(STATUS_KEY, st)

            def pulse():
                while hb.get('status') is st:
                    time.sleep(60)
                    if hb.get('status') is st:
                        heartbeat()
            threading.Thread(target=pulse, daemon=True, name='image-audit-heartbeat').start()
            xlsx, summary = build(day, beat=heartbeat)
            hb['status'] = None
            s3_put(report_key(day), xlsx, 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
            s3_put_json(STATUS_KEY, {'state': 'ready', 'date': day, 'summary': summary, 'finished_at': time.time(),
                                     'attempts': attempts})
            state['last_summary'], state['last_error'] = summary, None
            print(f"[image-audit] built {day}: {json.dumps(summary)}", flush=True)
            prune_reports(day)
            return True
        except Exception as e:
            state['last_error'] = f'{e}'
            print(f"[image-audit] build failed: {e}\n{traceback.format_exc()}", flush=True)
            try:
                s3_put_json(STATUS_KEY, {'state': 'error', 'date': day, 'error': str(e)[:500], 'finished_at': time.time(),
                                         'attempts': attempts})
            except Exception:
                pass
            return False
        finally:
            hb['status'] = None
            with lock:
                state['building'] = False

    def prune_reports(day):
        try:
            resp = get_s3().list_objects_v2(Bucket=s3_bucket, Prefix=PREFIX + 'report_', MaxKeys=1000)
            keys = sorted(o['Key'] for o in resp.get('Contents', []) or [])
            for k in keys[:-KEEP_REPORTS]:
                get_s3().delete_object(Bucket=s3_bucket, Key=k)
        except Exception as e:
            print(f"[image-audit] prune failed: {e}", flush=True)

    def start_build(day, reason):
        threading.Thread(target=run_build, args=(day, reason), daemon=True, name='image-audit-build').start()

    def remote_building(status, day):
        """Another worker is building today's report and is still alive (recent heartbeat)."""
        if status.get('state') != 'building' or status.get('date') != day:
            return False
        now = time.time()
        last = float(status.get('heartbeat_at') or status.get('started_at') or 0)
        return now - last < HEARTBEAT_STALE_S and now - float(status.get('started_at') or 0) < STALE_BUILD_S

    def allowed():
        ident = identity() or {}
        return ident.get('tier') == 'machine' or (ident.get('tier') == 'staff' and ident.get('is_admin') is True)

    def xlsx_response(day, summary):
        body = get_s3().get_object(Bucket=s3_bucket, Key=report_key(day))['Body'].read()
        resp = send_file(io.BytesIO(body), as_attachment=True,
                         mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                         download_name=f"Missing Image and Mismatch Report {day}.xlsx")
        resp.headers['X-Image-Audit-Summary'] = json.dumps(summary or {}, separators=(',', ':'))
        resp.headers['Access-Control-Expose-Headers'] = 'X-Image-Audit-Summary'
        return resp

    @app.route('/export-image-audit', methods=['GET', 'OPTIONS'])
    def export_image_audit():
        if request.method == 'OPTIONS':
            return '', 204
        if not allowed():
            return jsonify({'error': 'machine key or admin session required'}), 401
        day = now_et().date().isoformat()
        status = read_status()
        if (request.args.get('format') or '').lower() == 'summary':
            return jsonify(status)
        if status.get('state') == 'ready' and status.get('date') == day:
            return xlsx_response(day, status.get('summary'))
        if int(status.get('attempts') or 0) >= MAX_BUILD_ATTEMPTS and status.get('date') == day:
            return jsonify({'status': 'error', 'error': status.get('error') or 'build attempts used up'}), 503
        if not state['building'] and not remote_building(status, day):
            start_build(day, 'requested by /export-image-audit')
        deadline = time.time() + REQUEST_WAIT_S
        while time.time() < deadline:
            time.sleep(3)
            status = read_status()
            if status.get('state') == 'ready' and status.get('date') == day:
                return xlsx_response(day, status.get('summary'))
            if status.get('state') == 'error' and status.get('date') == day and not state['building']:
                return jsonify({'status': 'error', 'error': status.get('error')}), 503
        resp = jsonify({'status': 'building', 'date': day})
        resp.headers['Retry-After'] = '60'
        return resp, 503

    @app.route('/admin/image-audit/status', methods=['GET', 'OPTIONS'])
    def image_audit_status():
        if request.method == 'OPTIONS':
            return '', 204
        if not allowed():
            return jsonify({'error': 'machine key or admin session required'}), 401
        return jsonify({'s3': read_status(), 'worker': {
            'building': state['building'], 'started': state['started'], 'last_error': state['last_error'],
            'last_summary': state['last_summary'], 'model': MODEL}})

    @app.route('/admin/image-audit/build', methods=['POST', 'OPTIONS'])
    def image_audit_build():
        if request.method == 'OPTIONS':
            return '', 204
        if not allowed():
            return jsonify({'error': 'machine key or admin session required'}), 401
        day = now_et().date().isoformat()
        status = read_status()
        if state['building'] or remote_building(status, day):
            return jsonify({'started': False, 'reason': 'a build is already running', 'status': status}), 409
        start_build(day, 'manual')
        return jsonify({'started': True, 'date': day})

    @app.route('/admin/image-audit/verdicts', methods=['POST', 'OPTIONS'])
    def image_audit_verdicts():
        if request.method == 'OPTIONS':
            return '', 204
        if not allowed():
            return jsonify({'error': 'machine key or admin session required'}), 401
        body = request.get_json(silent=True)
        if not isinstance(body, dict):
            return jsonify({'error': 'body must be a JSON object'}), 400
        add_v = body.get('verdicts') or {}
        add_p = body.get('photos') or {}
        if not isinstance(add_v, dict) or not isinstance(add_p, dict):
            return jsonify({'error': 'verdicts and photos must be objects'}), 400
        good_v = {str(k)[:40]: {f: (str(v.get(f) or '')[:600]) for f in
                                ('etag', 'color', 'verdict', 'severity', 'kind', 'seen', 'reason', 'checked', 'by')}
                  for k, v in add_v.items()
                  if isinstance(v, dict) and v.get('verdict') in ('match', 'mismatch', 'unclear')}
        good_p = {str(k)[:64]: {'dh': v['dh'], 'th': v['th']} for k, v in add_p.items()
                  if isinstance(v, dict) and re.fullmatch(r'[0-9a-f]{64}', str(v.get('dh') or ''))
                  and re.fullmatch(r'[0-9a-f]{384}', str(v.get('th') or ''))}
        out = {'rejected': (len(add_v) - len(good_v)) + (len(add_p) - len(good_p))}
        try:
            if good_v:
                out['verdicts'] = len(merge_save(VERDICTS_KEY, good_v))
            if good_p:
                out['photos'] = len(merge_save(PHOTOS_KEY, good_p))
        except Exception as e:
            return jsonify({'error': f'cache unreadable, nothing written: {e}'}), 503
        return jsonify(out)

    def scheduler():
        time.sleep(180)
        while True:
            try:
                n = now_et()
                if n.weekday() == 0 and n.hour >= BUILD_HOUR_ET and (is_leader is None or is_leader()):
                    day = n.date().isoformat()
                    status = read_status()
                    # attempts count whatever the state, so a build that kills its worker stops too
                    done = status.get('date') == day and (
                        status.get('state') == 'ready' or int(status.get('attempts') or 0) >= MAX_BUILD_ATTEMPTS)
                    if not done and not state['building'] and not remote_building(status, day):
                        # an error today is retried at most every scheduler tick (5 min)
                        run_build(day, 'Monday pre-build')
            except Exception as e:
                print(f"[image-audit] scheduler error: {e}", flush=True)
            time.sleep(300)

    if start_scheduler:
        threading.Thread(target=scheduler, daemon=True, name='image-audit-scheduler').start()

    return {'build': build, 'run_build': run_build, 'state': state}
