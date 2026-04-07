"""
VERSA INVENTORY EXPORT API v3
- Dropbox as primary inventory source
- STYLE+OVERRIDES image lookup (matches frontend)
- Size-suffix stripping for correct image URLs
"""

# Gevent monkey-patch MUST happen before any other imports
# to avoid SSL recursion errors with boto3/urllib3
try:
    from gevent import monkey
    monkey.patch_all()
except ImportError:
    pass  # gevent not installed — run with sync workers

import os
import re
import json
import time
import threading
import concurrent.futures
from datetime import datetime
from io import BytesIO

import boto3
from botocore.exceptions import ClientError, NoCredentialsError
from flask import Flask, request, jsonify, send_file, Response, make_response
from flask_cors import CORS
import xlsxwriter
import requests as http_requests
import openpyxl
from PIL import Image as PilImage
from PIL import ImageOps
try:
    from reportlab.lib.pagesizes import letter
    from reportlab.lib.colors import Color, HexColor
    from reportlab.pdfgen import canvas as pdf_canvas
    from reportlab.lib.utils import ImageReader
    HAS_REPORTLAB = True
except ImportError:
    HAS_REPORTLAB = False
    print("⚠ reportlab not installed — PDF export will be unavailable. Add 'reportlab' to requirements.txt")

app = Flask(__name__)
CORS(app, resources={
    r"/*": {
        "origins": "*",
        "methods": ["GET", "POST", "DELETE", "OPTIONS"],
        "allow_headers": ["Content-Type"]
    }
})
app.config['MAX_CONTENT_LENGTH'] = 50 * 1024 * 1024  # 50MB

AWS_REGION       = os.environ.get('AWS_REGION', 'us-east-2')
S3_BUCKET        = os.environ.get('S3_BUCKET', 'nauticaslimfit')
S3_INVENTORY_KEY = os.environ.get('S3_INVENTORY_KEY', 'inventory/daily_inventory.xlsx')
S3_EXPORT_PREFIX = os.environ.get('S3_EXPORT_PREFIX', 'exports/').rstrip('/') + '/'
S3_PHOTOS_PREFIX = os.environ.get('S3_PHOTOS_PREFIX',
                                   'ALL+INVENTORY+Photos/PHOTOS+INVENTORY')

S3_OVERRIDES_KEY = os.environ.get('S3_OVERRIDES_KEY', 'inventory/style_overrides.json')
S3_MANUAL_ALLOC_KEY = os.environ.get('S3_MANUAL_ALLOC_KEY', 'inventory/manual_allocations.json')
S3_DEDUCTION_ASSIGN_KEY = os.environ.get('S3_DEDUCTION_ASSIGN_KEY', 'inventory/deduction_assignments.json')
S3_SAVED_CATALOGS_KEY = 'inventory/saved_catalogs.json'
S3_PREPACK_DEFAULTS_KEY = os.environ.get('S3_PREPACK_DEFAULTS_KEY', 'inventory/prepack_defaults.json')
S3_SUPPRESSION_OVERRIDES_KEY = os.environ.get('S3_SUPPRESSION_OVERRIDES_KEY', 'inventory/suppression_overrides.json')
S3_BANNER_RULES_KEY = os.environ.get('S3_BANNER_RULES_KEY', 'inventory/banner_rules.json')

S3_PHOTOS_URL = f"https://{S3_BUCKET}.s3.{AWS_REGION}.amazonaws.com/{S3_PHOTOS_PREFIX}"

# STYLE+OVERRIDES — primary image source (matches frontend logic)
S3_OVERRIDES_IMG_URL = f"https://{S3_BUCKET}.s3.{AWS_REGION}.amazonaws.com/ALL+INVENTORY+Photos/STYLE+OVERRIDES"
S3_DROPBOX_SYNC_PREFIX = 'ALL INVENTORY Photos/DROPBOX_SYNC'
S3_DROPBOX_SYNC_URL = f"https://{S3_BUCKET}.s3.{AWS_REGION}.amazonaws.com/ALL+INVENTORY+Photos/DROPBOX_SYNC"
CLOUDFRONT_URL = os.environ.get('CLOUDFRONT_URL', 'https://duv8yzuad6dsp.cloudfront.net')
# Derive CloudFront equivalents of S3 prefixes
CLOUDFRONT_OVERRIDES_URL = f"{CLOUDFRONT_URL}/ALL+INVENTORY+Photos/STYLE+OVERRIDES"
CLOUDFRONT_DROPBOX_SYNC_URL = f"{CLOUDFRONT_URL}/ALL+INVENTORY+Photos/DROPBOX_SYNC"
CLOUDFRONT_PHOTOS_URL = f"{CLOUDFRONT_URL}/{S3_PHOTOS_PREFIX}" 

# Dropbox direct download URL for hourly inventory file
# Dropbox shared link — will be converted to direct download at runtime
DROPBOX_URL = os.environ.get('DROPBOX_URL',
    'https://www.dropbox.com/scl/fi/de3nzb66mx41un0j69kyk/Inventory_ATS.xlsx?rlkey=ihoxu4s7gpb5ei14w2cl9dvyw&dl=1')

# Dropbox shared folder link for PHOTOS INVENTORY (daily image sync)
DROPBOX_PHOTOS_URL = os.environ.get('DROPBOX_PHOTOS_URL', '')
DROPBOX_PHOTOS_TOKEN = re.sub(r'\s+', '', os.environ.get('DROPBOX_PHOTOS_TOKEN', ''))
DROPBOX_PHOTOS_PATH = os.environ.get('DROPBOX_PHOTOS_PATH', '/Versa Share Files/PHOTOS INVENTORY')
DROPBOX_PHOTOS_SYNC_HOURS = int(os.environ.get('DROPBOX_PHOTOS_SYNC_HOURS', 8))

# Dropbox OAuth2 refresh token (never expires — auto-refreshes access tokens)
DROPBOX_APP_KEY = os.environ.get('DROPBOX_APP_KEY', '')
DROPBOX_APP_SECRET = os.environ.get('DROPBOX_APP_SECRET', '')
DROPBOX_REFRESH_TOKEN = os.environ.get('DROPBOX_REFRESH_TOKEN', '')

# Auto-refresh state
_dropbox_access_token = DROPBOX_PHOTOS_TOKEN  # fallback to legacy token
_dropbox_token_expires = 0

def get_dropbox_token():
    """Get a valid Dropbox access token, auto-refreshing if needed."""
    global _dropbox_access_token, _dropbox_token_expires
    
    # If we have refresh token config, use auto-refresh
    if DROPBOX_REFRESH_TOKEN and DROPBOX_APP_KEY and DROPBOX_APP_SECRET:
        if time.time() < _dropbox_token_expires - 300:  # 5 min buffer
            return _dropbox_access_token
        
        try:
            print("[Dropbox Auth] Refreshing access token...", flush=True)
            resp = http_requests.post('https://api.dropbox.com/oauth2/token', data={
                'grant_type': 'refresh_token',
                'refresh_token': DROPBOX_REFRESH_TOKEN,
                'client_id': DROPBOX_APP_KEY,
                'client_secret': DROPBOX_APP_SECRET,
            }, timeout=30)
            
            if resp.status_code == 200:
                data = resp.json()
                _dropbox_access_token = data['access_token']
                _dropbox_token_expires = time.time() + data.get('expires_in', 14400)
                print(f"[Dropbox Auth] ✓ Token refreshed, expires in {data.get('expires_in', '?')}s", flush=True)
                return _dropbox_access_token
            else:
                print(f"[Dropbox Auth] ✗ Refresh failed ({resp.status_code}): {resp.text[:200]}", flush=True)
        except Exception as e:
            print(f"[Dropbox Auth] ✗ Refresh error: {e}", flush=True)
    
    # Fallback to legacy static token
    return DROPBOX_PHOTOS_TOKEN

TARGET_W = 150
TARGET_H = 150
# Inward padding (px) so image bottom-right anchor stays inside the row.
# Without this, the twoCellAnchor 'to' cell bleeds into the next row and
# images don't hide when Excel filters — causing the "jumbled images" bug.
_IMG_CELL_PAD = 4
COL_WIDTH_UNITS = 22


def _padded_image_opts(img, pad=_IMG_CELL_PAD):
    """Shrink scale & shift offsets so the image stays strictly inside the cell.

    This ensures xlsxwriter's twoCellAnchor 'to' reference never bleeds into
    the next row, which is what causes images to jumble when filtering in Excel.
    The visual difference is ~2 px extra whitespace per side — imperceptible.
    """
    ratio = (TARGET_W - pad) / TARGET_W  # same for W and H (both 150)
    return {
        'image_data': img['image_data'],
        'x_scale':  img['x_scale']  * ratio,
        'y_scale':  img['y_scale']  * ratio,
        'x_offset': img['x_offset'] * ratio + pad / 2,
        'y_offset': img['y_offset'] * ratio + pad / 2,
        'object_position': 1,
        'url': img.get('url', ''),
    }

BRAND_IMAGE_PREFIX = {
    'NAUTICA': 'NA', 'DKNY': 'DK', 'EB': 'EB', 'REEBOK': 'RB', 'VINCE': 'VC',
    'BEN': 'BE', 'USPA': 'US', 'CHAPS': 'CH', 'LUCKY': 'LB', 'JNY': 'JN',
    'BEENE': 'GB', 'NICOLE': 'NM', 'SHAQ': 'SH', 'TAYION': 'TA', 'STRAHAN': 'MS',
    'VD': 'VD', 'VERSA': 'VR', 'CHEROKEE': 'CK', 'AMERICA': 'AC', 'BLO': 'BL', 'BLACK': 'BL', 'DN': 'D9',
    'KL': 'KL', 'RG': 'RG', 'NE': 'NE'
}

BRAND_FULL_NAMES = {
    'NAUTICA': 'Nautica', 'DKNY': 'DKNY', 'EB': 'Eddie Bauer', 'REEBOK': 'Reebok',
    'VINCE': 'Vince Camuto', 'BEN': 'Ben Sherman', 'USPA': 'U.S. Polo Assn.',
    'CHAPS': 'Chaps', 'LUCKY': 'Lucky Brand', 'JNY': 'Jones New York',
    'BEENE': 'Geoffrey Beene', 'NICOLE': 'Nicole Miller', 'SHAQ': "Shaquille O'Neal",
    'TAYION': 'Tayion', 'STRAHAN': 'Michael Strahan', 'VD': 'Von Dutch',
    'VERSA': 'Versa', 'CHEROKEE': 'Cherokee', 'AMERICA': 'American Crew', 'BLO': 'Bloomingdales', 'BLACK': 'Black Label', 'DN': 'Divine 9',
    'KL': 'Karl Lagerfeld Paris', 'RG': 'Robert Graham', 'NE': 'Neiman Marcus'
}

FOLDER_MAPPING = {
    'EB': 'EDDIE+BAUER', 'USPA': 'US+POLO', 'VINCE': 'VINCE+CAMUTO',
    'LUCKY': 'LUCKY+BRAND', 'BEN': 'BEN+SHERMAN', 'BEENE': 'GEOFFREY+BEENE',
    'NICOLE': 'NICOLE+MILLER', 'AMERICA': 'AMERICAN+CREW',
    'TAYION': 'TAYON', 'VD': 'Von+Dutch',
    'KL': 'KARL+LAGERFELD',
    'BLACK': 'Black'
}

STYLE_CONFIG = {
    'header_bg': '#ADD8E6', 'header_text': '#000000',
    'row_bg_odd': '#FFFFFF', 'row_bg_even': '#F0F4F8',
    'border_color': '#000000', 'font_name': 'Calibri'
}

_s3_client = None

def get_s3():
    global _s3_client
    if _s3_client is None:
        _s3_client = boto3.client('s3',
            aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID', ''),
            aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY', ''),
            region_name=AWS_REGION
        )
    return _s3_client

_inv_lock = threading.Lock()
_inventory = {
    'items': [],
    'brands': {},
    'etag': None,
    'last_sync': None,
    'item_count': 0,
    'source': None,  # 'dropbox' or 's3'
}

_img_lock = threading.Lock()
_img_cache = {}  # base_style → image result (shared across all size variants)

# --- Dropbox Photos Cache ---
_dropbox_photo_index = {}   # image_code (uppercase) → file_path on disk
_dropbox_thumb_cache = {}   # image_code → thumbnail bytes (for Excel exports)
_dropbox_photo_lock = threading.Lock()
_dropbox_photos_last_sync = 0

_export_lock = threading.Lock()
_exports = {
    'brands': {},
    'all_brands': None,
    'generating': False,
    'progress': '',
    'last_generated': None,
}

_overrides_lock = threading.Lock()
_style_overrides = {}
_overrides_loaded = False       # True once S3 load completes — prevents race on fresh workers
_overrides_last_saved = 0.0    # epoch timestamp of last POST save — used for version polling
_s3_overrides_etag = None      # ETag from S3 — used to detect cross-worker staleness
_manual_alloc_lock = threading.Lock()
_manual_allocations = []  # list of dicts: {id, sku, customer, po, qty, type, date, notes}
_deduction_assign_lock = threading.Lock()
_deduction_assignments = {}  # dict: { sku: 'warehouse' | 'overseas' }
_prepack_defaults_lock = threading.Lock()
_prepack_defaults = []  # list of dicts: {id, category, fit, label, master_qty, inner_qty, sizes}
_suppression_overrides_lock = threading.Lock()
_suppression_overrides = []  # list of SKU strings that should NOT be suppressed
_banner_rules_lock = threading.Lock()
_banner_rules = []  # list of banner rule dicts

def load_overrides_from_s3():
    """Load style overrides from S3 on startup"""
    global _style_overrides, _overrides_loaded, _s3_overrides_etag
    try:
        s3 = get_s3()
        resp = s3.get_object(Bucket=S3_BUCKET, Key=S3_OVERRIDES_KEY)
        data = json.loads(resp['Body'].read().decode('utf-8'))
        etag = resp.get('ETag', '').strip('"')
        with _overrides_lock:
            _style_overrides = data
            _s3_overrides_etag = etag
        _overrides_loaded = True
        print(f"  ✓ Loaded {len(_style_overrides)} style overrides from S3 (ETag: {etag[:8]})")
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            print("  No existing style overrides in S3 (will create on first save)")
        else:
            print(f"  ⚠ Could not load overrides from S3: {e}")
        _overrides_loaded = True  # mark loaded even on miss
    except Exception as e:
        print(f"  ⚠ Override load error: {e}")
        _overrides_loaded = True  # mark loaded on error

def save_overrides_to_s3():
    """Save style overrides to S3"""
    global _s3_overrides_etag
    try:
        s3 = get_s3()
        with _overrides_lock:
            data = json.dumps(_style_overrides)
        resp = s3.put_object(
            Bucket=S3_BUCKET,
            Key=S3_OVERRIDES_KEY,
            Body=data.encode('utf-8'),
            ContentType='application/json'
        )
        etag = resp.get('ETag', '').strip('"')
        with _overrides_lock:
            _s3_overrides_etag = etag
        print(f"  ✓ Saved {len(_style_overrides)} style overrides to S3 (ETag: {etag[:8]})")
        return True
    except Exception as e:
        print(f"  ✗ Failed to save overrides to S3: {e}")
        return False


def load_manual_allocations_from_s3():
    """Load manual allocation entries from S3 JSON on startup"""
    global _manual_allocations
    try:
        s3 = get_s3()
        resp = s3.get_object(Bucket=S3_BUCKET, Key=S3_MANUAL_ALLOC_KEY)
        data = json.loads(resp['Body'].read().decode('utf-8'))
        with _manual_alloc_lock:
            _manual_allocations = data if isinstance(data, list) else []
        print(f"  ✓ Loaded {len(_manual_allocations)} manual allocation entries from S3")
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            print("  No manual allocations in S3 yet (will create on first save)")
        else:
            print(f"  ⚠ Could not load manual allocations from S3: {e}")
    except Exception as e:
        print(f"  ⚠ Manual allocation load error: {e}")


def save_manual_allocations_to_s3():
    """Persist manual allocation entries to S3"""
    try:
        s3 = get_s3()
        with _manual_alloc_lock:
            data = json.dumps(_manual_allocations)
        s3.put_object(
            Bucket=S3_BUCKET,
            Key=S3_MANUAL_ALLOC_KEY,
            Body=data.encode('utf-8'),
            ContentType='application/json'
        )
        print(f"  ✓ Saved {len(_manual_allocations)} manual allocations to S3")
        return True
    except Exception as e:
        print(f"  ✗ Failed to save manual allocations to S3: {e}")
        return False


def load_deduction_assignments_from_s3():
    """Load deduction assignments (sku -> warehouse|overseas) from S3"""
    global _deduction_assignments
    try:
        s3 = get_s3()
        resp = s3.get_object(Bucket=S3_BUCKET, Key=S3_DEDUCTION_ASSIGN_KEY)
        data = json.loads(resp['Body'].read().decode('utf-8'))
        with _deduction_assign_lock:
            _deduction_assignments = data if isinstance(data, dict) else {}
        print(f"  ✓ Loaded {len(_deduction_assignments)} deduction assignments from S3")
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            print("  No deduction assignments in S3 yet")
        else:
            print(f"  ⚠ Could not load deduction assignments: {e}")
    except Exception as e:
        print(f"  ⚠ Deduction assignment load error: {e}")


def save_deduction_assignments_to_s3():
    """Save deduction assignments to S3"""
    try:
        s3 = get_s3()
        with _deduction_assign_lock:
            data = json.dumps(_deduction_assignments)
        s3.put_object(
            Bucket=S3_BUCKET,
            Key=S3_DEDUCTION_ASSIGN_KEY,
            Body=data.encode('utf-8'),
            ContentType='application/json'
        )
        print(f"  ✓ Saved {len(_deduction_assignments)} deduction assignments to S3")
        return True
    except Exception as e:
        print(f"  ✗ Failed to save deduction assignments: {e}")
        return False


def load_prepack_defaults_from_s3():
    """Load prepack default rules from S3"""
    global _prepack_defaults
    try:
        s3 = get_s3()
        resp = s3.get_object(Bucket=S3_BUCKET, Key=S3_PREPACK_DEFAULTS_KEY)
        data = json.loads(resp['Body'].read().decode('utf-8'))
        with _prepack_defaults_lock:
            _prepack_defaults = data if isinstance(data, list) else []
        print(f"  ✓ Loaded {len(_prepack_defaults)} prepack defaults from S3")
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            print("  No prepack defaults in S3 yet (will create on first save)")
        else:
            print(f"  ⚠ Could not load prepack defaults from S3: {e}")
    except Exception as e:
        print(f"  ⚠ Prepack defaults load error: {e}")


def save_prepack_defaults_to_s3():
    """Persist prepack default rules to S3"""
    try:
        s3 = get_s3()
        with _prepack_defaults_lock:
            data = json.dumps(_prepack_defaults)
        s3.put_object(
            Bucket=S3_BUCKET,
            Key=S3_PREPACK_DEFAULTS_KEY,
            Body=data.encode('utf-8'),
            ContentType='application/json'
        )
        print(f"  ✓ Saved {len(_prepack_defaults)} prepack defaults to S3")
        return True
    except Exception as e:
        print(f"  ✗ Failed to save prepack defaults: {e}")
        return False


def load_suppression_overrides_from_s3():
    global _suppression_overrides
    try:
        s3 = get_s3()
        resp = s3.get_object(Bucket=S3_BUCKET, Key=S3_SUPPRESSION_OVERRIDES_KEY)
        data = json.loads(resp['Body'].read().decode('utf-8'))
        with _suppression_overrides_lock:
            _suppression_overrides = data if isinstance(data, list) else []
        print(f"  ✓ Loaded {len(_suppression_overrides)} suppression overrides from S3")
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            print("  No suppression overrides in S3 yet")
        else:
            print(f"  ⚠ Could not load suppression overrides: {e}")
    except Exception as e:
        print(f"  ⚠ Suppression overrides load error: {e}")


def save_suppression_overrides_to_s3():
    try:
        s3 = get_s3()
        with _suppression_overrides_lock:
            data = json.dumps(_suppression_overrides)
        s3.put_object(Bucket=S3_BUCKET, Key=S3_SUPPRESSION_OVERRIDES_KEY, Body=data.encode('utf-8'), ContentType='application/json')
        print(f"  ✓ Saved {len(_suppression_overrides)} suppression overrides to S3")
        return True
    except Exception as e:
        print(f"  ✗ Failed to save suppression overrides: {e}")
        return False


def load_banner_rules_from_s3():
    global _banner_rules
    try:
        s3 = get_s3()
        resp = s3.get_object(Bucket=S3_BUCKET, Key=S3_BANNER_RULES_KEY)
        data = json.loads(resp['Body'].read().decode('utf-8'))
        with _banner_rules_lock:
            _banner_rules = data if isinstance(data, list) else []
        print(f"  ✓ Loaded {len(_banner_rules)} banner rules from S3")
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            print("  No banner rules in S3 yet")
        else:
            print(f"  ⚠ Could not load banner rules: {e}")
    except Exception as e:
        print(f"  ⚠ Banner rules load error: {e}")


def save_banner_rules_to_s3():
    try:
        s3 = get_s3()
        with _banner_rules_lock:
            data = json.dumps(_banner_rules)
        s3.put_object(Bucket=S3_BUCKET, Key=S3_BANNER_RULES_KEY, Body=data.encode('utf-8'), ContentType='application/json')
        print(f"  ✓ Saved {len(_banner_rules)} banner rules to S3")
        return True
    except Exception as e:
        print(f"  ✗ Failed to save banner rules: {e}")
        return False


S3_ALLOCATION_KEY = os.environ.get('S3_ALLOCATION_KEY', 'inventory/VIRTUAL WAREHOUSE ALLOCATION.csv')
# Production/Style Ledger — now Dropbox-only (S3 removed)
DROPBOX_PRODUCTION_FOLDER = os.environ.get('DROPBOX_PRODUCTION_FOLDER',
    '/Versa Share Files/David - Dropbox/Style Ledger')

# APO allocation file — Dropbox path
DROPBOX_APO_PATH = os.environ.get('DROPBOX_APO_PATH', '/EDI Team/Nuri/Python Macros/Inventory RAW/APO.csv')

# In-memory APO cache
_apo_data = []
_apo_lock = threading.Lock()
_apo_last_sync = 0


def load_allocation_from_s3():
    """Load allocation CSV from S3 and return as list of dicts"""
    try:
        s3 = get_s3()
        resp = s3.get_object(Bucket=S3_BUCKET, Key=S3_ALLOCATION_KEY)
        text = resp['Body'].read().decode('utf-8-sig')
        lines = text.strip().split('\n')
        if len(lines) < 2:
            return []
        headers = [h.strip() for h in lines[0].split(',')]
        po_idx = next((i for i, h in enumerate(headers) if 'po' in h.lower()), 0)
        cust_idx = next((i for i, h in enumerate(headers) if 'customer' in h.lower()), 1)
        sku_idx = next((i for i, h in enumerate(headers) if 'sku' in h.lower()), 2)
        qty_idx = next((i for i, h in enumerate(headers) if 'qty' in h.lower()), 3)

        results = []
        for line in lines[1:]:
            cols = [c.strip() for c in line.split(',')]
            if len(cols) <= sku_idx or not cols[sku_idx]:
                continue
            try:
                qty = int(cols[qty_idx]) if len(cols) > qty_idx else 0
            except ValueError:
                qty = 0
            results.append({
                'po': cols[po_idx] if len(cols) > po_idx else '',
                'customer': cols[cust_idx] if len(cols) > cust_idx else '',
                'sku': cols[sku_idx].upper(),
                'qty': qty
            })
        print(f"  ✓ Loaded {len(results)} allocation rows from S3")
        return results
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            print("  No allocation file found in S3")
        else:
            print(f"  ⚠ Could not load allocation from S3: {e}")
        return []
    except Exception as e:
        print(f"  ⚠ Allocation load error: {e}")
        return []


# Production data cache — loaded from Dropbox, refreshed hourly
_production_data = []
_production_last_sync = 0
_production_lock = threading.Lock()
_PRODUCTION_TTL = 3600  # 1 hour


def load_production_from_dropbox():
    """Load Style Ledger xlsx from Dropbox — picks the first .xlsx in the folder.
    Caches result in memory; returns cached data if under 1 hour old."""
    global _production_data, _production_last_sync

    # Return cache if fresh
    with _production_lock:
        if _production_data and (time.time() - _production_last_sync < _PRODUCTION_TTL):
            return _production_data

    token = get_dropbox_token()
    if not token:
        print("  ⚠ Production sync: no Dropbox token available")
        with _production_lock:
            return list(_production_data)  # return stale if any

    try:
        headers = {
            'Authorization': f'Bearer {token}',
            'Content-Type': 'application/json'
        }

        # List the Style Ledger folder — pick first .xlsx file found
        list_resp = http_requests.post(
            'https://api.dropboxapi.com/2/files/list_folder',
            headers=headers,
            json={'path': DROPBOX_PRODUCTION_FOLDER, 'recursive': False, 'limit': 50},
            timeout=20
        )
        if list_resp.status_code != 200:
            print(f"  ⚠ Production: could not list Dropbox folder ({list_resp.status_code}): {list_resp.text[:200]}")
            with _production_lock:
                return list(_production_data)

        entries = list_resp.json().get('entries', [])
        xlsx_files = [e for e in entries if e['.tag'] == 'file' and e['name'].lower().endswith('.xlsx')]
        if not xlsx_files:
            print(f"  ⚠ Production: no .xlsx file found in {DROPBOX_PRODUCTION_FOLDER}")
            with _production_lock:
                return list(_production_data)

        # Pick first xlsx (folder should only have one, but prefer any with "ledger" in name)
        xlsx_files.sort(key=lambda e: (0 if 'ledger' in e['name'].lower() else 1, e['name']))
        chosen = xlsx_files[0]
        print(f"  📋 Loading production data from Dropbox: {chosen['name']}", flush=True)

        # Download file bytes
        dl_resp = http_requests.post(
            'https://content.dropboxapi.com/2/files/download',
            headers={
                'Authorization': f'Bearer {token}',
                'Dropbox-API-Arg': json.dumps({'path': chosen['path_display']})
            },
            timeout=30
        )
        if dl_resp.status_code != 200:
            print(f"  ⚠ Production: download failed ({dl_resp.status_code})")
            with _production_lock:
                return list(_production_data)

        wb = openpyxl.load_workbook(BytesIO(dl_resp.content), read_only=True)
        ws = wb[wb.sheetnames[0]]
        results = []
        for row in ws.iter_rows(min_row=2, max_col=6, values_only=True):
            style = str(row[2] or '').strip().upper()
            if not style:
                continue
            etd = None
            if row[5]:
                if isinstance(row[5], datetime):
                    etd = row[5].strftime('%Y-%m-%d')
                else:
                    try:
                        etd = str(row[5])
                    except:
                        etd = None
            try:
                units = int(row[3] or 0)
            except (ValueError, TypeError):
                units = 0
            results.append({
                'production': str(row[0] or '').strip(),
                'poName': str(row[1] or '').strip(),
                'style': style,
                'units': units,
                'brand': str(row[4] or '').strip(),
                'etd': etd
            })
        wb.close()

        with _production_lock:
            _production_data = results
            _production_last_sync = time.time()

        print(f"  ✓ Loaded {len(results)} production rows from Dropbox ({chosen['name']})")
        return results

    except Exception as e:
        import traceback
        print(f"  ⚠ Production load error: {e}")
        traceback.print_exc()
        with _production_lock:
            return list(_production_data)  # return stale on error


def extract_image_code(sku, brand_abbr):
    """Extract image code from SKU — strips size suffix first"""
    prefix = BRAND_IMAGE_PREFIX.get(brand_abbr, brand_abbr[:2])
    # Strip size suffix (everything after first dash)
    base_sku = sku.split('-')[0]
    numbers = re.findall(r'\d+', str(base_sku))
    if numbers:
        main_number = max(numbers, key=len)
        return f"{prefix}_{main_number}"
    return f"{prefix}_{base_sku}"


def get_base_style(sku):
    """Get base style from SKU by stripping size suffix — matches frontend logic"""
    return sku.split('-')[0].upper()


# ── Python-side category + fit derivation (mirrors frontend logic) ────────
# Used by background/server-side exports to annotate items so _add_size_charts
# can match prepack default rules without needing the frontend to send _export_* fields.

_PY_SPORTSWEAR_COLLARS = set('ZUMNOR')
_PY_SPORTSWEAR_FABRICS = {'PH','PJ','PL','PO','PW','TH','HE'}
_PY_BT_FIT_CODES       = {'BT','BB','TT','SB','ST'}
_PY_YM_FABRIC_CODES    = {
    'YM','YB','YR','YG','YS','YT','YP','YA','YO','YE',
    '1Y','2Y','3Y','4Y','5Y','6Y','7Y','8Y','9Y',
    '10','11','12','13','14','15','16','17','18',
}

def _py_extract_fit_code(sku):
    """Extract 2-char fit code from base style — mirrors extractFitCode() in JS."""
    base = sku.split('-')[0].upper().rstrip('V').rstrip('-')
    if len(base) >= 3:
        candidate = base[-3:-1]   # 2nd & 3rd from end (last char = collar)
        known = {'SL','RF','BT','BB','TT','TF','MF','SS','SR','SB','ST',
                 'SE','SH','CE','CH','CR','SF','SC','RR'}
        if candidate in known:
            return candidate
    # fallback: dash suffix e.g. NONAU175-SL
    parts = sku.upper().split('-')
    if len(parts) > 1 and parts[-1] in {'SL','RF','BT','TF','MF','SS','SR'}:
        return parts[-1]
    return 'RF'

def _py_is_short_sleeve(sku):
    base = sku.split('-')[0].upper()
    fit = _py_extract_fit_code(sku)
    return fit in {'SS','SR','SB','ST'} or (len(base) >= 11 and base[-1] in _PY_SPORTSWEAR_COLLARS)

def _py_is_young_men(sku):
    base = sku.split('-')[0].upper()
    if len(base) >= 6:
        return base[4:6] in _PY_YM_FABRIC_CODES
    return False

def _py_is_big_tall(sku):
    base = sku.split('-')[0].upper()
    # Von Dutch special B&T prefixes: WBJ, BTC, BTS, WBK (after customer+brand code)
    if len(base) >= 7 and base[2:4] == 'VD':
        suffix = base[4:]
        if suffix.startswith(('WBJ', 'BTC', 'BTS', 'WBK')):
            return True
    if len(base) >= 11:
        return base[9:11] in _PY_BT_FIT_CODES
    return False

def _py_get_item_category(sku, brand_abbr):
    """Returns category string matching frontend getDetailedCategory() values."""
    base = sku.split('-')[0].upper()
    # Pants: position 6 = 'P', positions 7-8 = digits, position 9 = letter
    if (len(base) >= 10 and base[6] == 'P'
            and base[7].isdigit() and base[8].isdigit() and base[9].isalpha()):
        return 'pants'
    # Sportswear by collar code
    if len(base) >= 11 and base[-1] in _PY_SPORTSWEAR_COLLARS:
        return 'sportswear'
    # Sportswear by fabric code: polo/tee/henley fabrics
    if len(base) >= 6 and base[4:6] in _PY_SPORTSWEAR_FABRICS:
        return 'sportswear'
    # Accessories (Chaps ties, Shaq ties)
    brand_up = (brand_abbr or '').upper()
    if brand_up == 'CHAPS' and base.startswith('CTH'):
        return 'accessories'
    if brand_up == 'SHAQ' and len(base) >= 3 and 'T' in base[:3]:
        return 'accessories'
    # Young Men (fabric-code based)
    if _py_is_young_men(sku):
        return 'young_men'
    # Big & Tall
    if _py_is_big_tall(sku):
        return 'big_tall'
    # Shirts: split by sleeve
    return 'short_sleeve' if _py_is_short_sleeve(sku) else 'long_sleeve'

def _annotate_items_for_prepack(items):
    """Add _export_category, _export_fit, and _override_size_pack to raw inventory items (returns new list)."""
    result = []
    with _overrides_lock:
        overrides_snap = dict(_style_overrides)
    for item in items:
        sku        = item.get('sku', '')
        brand_abbr = item.get('brand_abbr', item.get('brand', ''))
        annotated  = dict(item)
        annotated.setdefault('_export_category', _py_get_item_category(sku, brand_abbr))
        annotated.setdefault('_export_fit',      _py_extract_fit_code(sku))
        annotated.setdefault('_export_customer',  sku[:2].upper() if len(sku) >= 2 else '')
        # Check for per-item size pack override (from Override Tool)
        if '_override_size_pack' not in annotated:
            sku_up = sku.upper()
            ov = overrides_snap.get(sku_up)
            if not ov:
                # Prefix match — keys ending with '-'
                for key in overrides_snap:
                    if key.endswith('-') and sku_up.startswith(key[:-1]):
                        ov = overrides_snap[key]
                        break
            if ov and ov.get('sizePack') and ov['sizePack'].get('sizes'):
                annotated['_override_size_pack'] = ov['sizePack']
            else:
                annotated['_override_size_pack'] = None
        result.append(annotated)
    return result


def get_image_url(item, s3_base_url):
    """Get brand-folder fallback URL"""
    brand_abbr = item.get('brand_abbr', item.get('brand', ''))
    folder_name = FOLDER_MAPPING.get(brand_abbr, brand_abbr)
    image_code = extract_image_code(item['sku'], brand_abbr)
    return f"{s3_base_url}/{folder_name}/{image_code}.jpg"


def get_style_override_url(sku):
    """Get STYLE+OVERRIDES URL — primary image source (matches frontend)"""
    base_style = get_base_style(sku)
    return f"{S3_OVERRIDES_IMG_URL}/{base_style}.jpg"


def _process_image_from_url(url, tw=TARGET_W, th=TARGET_H):
    """Download and resize an image from URL, trying .jpg/.png/.jpeg extensions"""
    if not (isinstance(url, str) and url.startswith('http')):
        return None

    headers = {'User-Agent': 'Mozilla/5.0'}
    base_url = url.rsplit('.', 1)[0]

    for ext in ['.jpg', '.png', '.jpeg']:
        try_url = base_url + ext
        try:
            resp = http_requests.get(try_url, headers=headers, timeout=5)
            if resp.status_code != 200:
                continue
            ct = resp.headers.get('Content-Type', '').lower()
            if 'image' not in ct:
                continue

            with PilImage.open(BytesIO(resp.content)) as im:
                im = ImageOps.exif_transpose(im)
                im.thumbnail((tw * 2, th * 2), PilImage.Resampling.LANCZOS)

                fmt = "PNG"
                if im.mode in ("RGBA", "LA") or (im.mode == "P" and "transparency" in im.info):
                    fmt = "PNG"
                else:
                    if im.mode != "RGB":
                        im = im.convert("RGB")
                    fmt = "JPEG"

                buf = BytesIO()
                im.save(buf, format=fmt, quality=85, optimize=True)
                raw = buf.getvalue()
                ow, oh = im.size

            wr = tw / ow
            hr = th / oh
            sf = min(wr, hr)
            return {
                'raw_bytes': raw,
                'x_scale': sf, 'y_scale': sf,
                'x_offset': (tw - ow * sf) / 2,
                'y_offset': (th - oh * sf) / 2,
                'url': try_url
            }
        except Exception:
            continue
    return None



def sync_dropbox_photos():
    """List files in Dropbox PHOTOS INVENTORY folder via API — just metadata, no downloading."""
    global _dropbox_photo_index, _dropbox_photos_last_sync
    token = get_dropbox_token()
    if not token:
        print("[Dropbox Photos] No Dropbox token configured, skipping", flush=True)
        return

    try:
        print(f"[Dropbox Photos] Listing files via API in: {DROPBOX_PHOTOS_PATH}", flush=True)
        headers = {
            'Authorization': f'Bearer {token}',
            'Content-Type': 'application/json'
        }

        new_index = {}  # image_code (uppercase) → dropbox_path
        cursor = None
        total_files = 0

        # Initial list_folder call
        payload = {
            'path': DROPBOX_PHOTOS_PATH,
            'recursive': True,
            'limit': 2000
        }
        resp = http_requests.post(
            'https://api.dropboxapi.com/2/files/list_folder',
            headers=headers, json=payload, timeout=60
        )

        if resp.status_code == 401:
            print(f"[Dropbox Photos] Auth failed (401) — forcing token refresh...", flush=True)
            # Force refresh by resetting expiry
            global _dropbox_token_expires
            _dropbox_token_expires = 0
            token = get_dropbox_token()
            if not token:
                print(f"[Dropbox Photos] Could not refresh token, giving up", flush=True)
                return
            headers['Authorization'] = f'Bearer {token}'
            resp = http_requests.post(
                'https://api.dropboxapi.com/2/files/list_folder',
                headers=headers, json=payload, timeout=60
            )
            if resp.status_code == 401:
                print(f"[Dropbox Photos] Still 401 after refresh — check credentials", flush=True)
                return

        # If path not found, try to discover it
        if resp.status_code == 409:
            print(f"[Dropbox Photos] Path '{DROPBOX_PHOTOS_PATH}' not found, searching root...", flush=True)
            # List root to find the right folder
            root_resp = http_requests.post(
                'https://api.dropboxapi.com/2/files/list_folder',
                headers=headers,
                json={'path': '', 'recursive': False, 'limit': 500},
                timeout=30
            )
            if root_resp.status_code == 200:
                root_data = root_resp.json()
                folders = [e['path_display'] for e in root_data.get('entries', []) if e['.tag'] == 'folder']
                print(f"[Dropbox Photos] Root folders: {folders}", flush=True)
                # Try to find PHOTOS INVENTORY anywhere
                for folder in folders:
                    sub_resp = http_requests.post(
                        'https://api.dropboxapi.com/2/files/list_folder',
                        headers=headers,
                        json={'path': folder, 'recursive': False, 'limit': 500},
                        timeout=30
                    )
                    if sub_resp.status_code == 200:
                        sub_data = sub_resp.json()
                        sub_folders = [e['path_display'] for e in sub_data.get('entries', []) if e['.tag'] == 'folder']
                        for sf in sub_folders:
                            if 'photo' in sf.lower() and 'inventory' in sf.lower():
                                print(f"[Dropbox Photos] Found photos folder: {sf}", flush=True)
                                # Re-do the listing with discovered path
                                payload['path'] = sf
                                resp = http_requests.post(
                                    'https://api.dropboxapi.com/2/files/list_folder',
                                    headers=headers, json=payload, timeout=60
                                )
                                break
                        if resp.status_code == 200:
                            break
            if resp.status_code != 200:
                print(f"[Dropbox Photos] Could not find photos folder. API response: {resp.text[:300]}", flush=True)
                return

        if resp.status_code != 200:
            print(f"[Dropbox Photos] API error: {resp.status_code} {resp.text[:200]}", flush=True)
            return

        data = resp.json()

        while True:
            for entry in data.get('entries', []):
                if entry['.tag'] != 'file':
                    continue
                name = entry['name']
                path_lower = entry['path_lower']
                lower = name.lower()

                # Only image files, skip 1x folder and macOS metadata
                if not lower.endswith(('.jpg', '.jpeg', '.png')):
                    continue
                if '/1x/' in path_lower or '__macosx' in path_lower:
                    continue

                name_no_ext = os.path.splitext(name)[0]
                # Clean: remove " copy", " Copy 2", etc.
                clean = re.sub(r'\s*copy\s*\d*$', '', name_no_ext, flags=re.IGNORECASE).strip()
                # Normalize separators: both - and _ → _
                clean = clean.replace('-', '_')
                key = clean.upper()

                # Store the Dropbox path (for on-demand download)
                # Prefer .jpg over .png if duplicates exist
                if key not in new_index or lower.endswith('.jpg'):
                    new_index[key] = entry['path_display']
                total_files += 1

            if not data.get('has_more'):
                break

            # Continue listing
            resp = http_requests.post(
                'https://api.dropboxapi.com/2/files/list_folder/continue',
                headers=headers,
                json={'cursor': data['cursor']},
                timeout=60
            )
            if resp.status_code != 200:
                print(f"[Dropbox Photos] Continue error: {resp.status_code}", flush=True)
                break
            data = resp.json()

        with _dropbox_photo_lock:
            _dropbox_photo_index = new_index
            _dropbox_photos_last_sync = time.time()

        # Clear image caches so they rebuild with Dropbox awareness
        _dropbox_img_cache.clear()
        _web_img_cache.clear()
        _img_cache.clear()

        print(f"[Dropbox Photos] ✓ Indexed {len(new_index)} unique images ({total_files} total files)", flush=True)

    except Exception as e:
        print(f"[Dropbox Photos] Error: {e}", flush=True)
        import traceback
        traceback.print_exc()


# Cache for downloaded Dropbox images — disk-based for persistence across requests
_dropbox_img_cache = {}  # image_code → (bytes, content_type) — small in-memory LRU for hot images
_dropbox_img_cache_lock = threading.Lock()
DROPBOX_DISK_CACHE = os.environ.get('DROPBOX_DISK_CACHE', '/var/data/dropbox_cache')
os.makedirs(DROPBOX_DISK_CACHE, exist_ok=True)



def _download_dropbox_file(dropbox_path):
    """Download a single file from Dropbox via API."""
    token = get_dropbox_token()
    if not token:
        return None, None
    try:
        headers = {
            'Authorization': f'Bearer {token}',
            'Dropbox-API-Arg': json.dumps({'path': dropbox_path})
        }
        resp = http_requests.post(
            'https://content.dropboxapi.com/2/files/download',
            headers=headers, timeout=30
        )
        if resp.status_code == 200:
            ct = resp.headers.get('Content-Type', 'application/octet-stream').lower()
            if 'image' not in ct:
                ext = dropbox_path.lower().rsplit('.', 1)[-1]
                ct = 'image/jpeg' if ext in ('jpg', 'jpeg') else 'image/png'
            return resp.content, ct
    except Exception as e:
        print(f"[Dropbox Photos] Download error for {dropbox_path}: {e}")
    return None, None


def _get_disk_cache_path(image_code):
    """Get the disk cache file path for an image code."""
    return os.path.join(DROPBOX_DISK_CACHE, image_code)


def get_dropbox_image_bytes(image_code):
    """Get raw image bytes from Dropbox (disk-cached). Returns (bytes, content_type) or (None, None)."""
    key = image_code.upper().replace('-', '_')

    # 1. Check in-memory hot cache
    with _dropbox_img_cache_lock:
        cached = _dropbox_img_cache.get(key, 'MISS')
    if cached is None:
        return None, None  # Previously failed
    if cached != 'MISS':
        return cached

    # 2. Check disk cache
    disk_path = _get_disk_cache_path(key)
    if os.path.exists(disk_path + '.jpg'):
        try:
            with open(disk_path + '.jpg', 'rb') as f:
                data = f.read()
            result = (data, 'image/jpeg')
            with _dropbox_img_cache_lock:
                if len(_dropbox_img_cache) > 200:
                    _dropbox_img_cache.clear()
                _dropbox_img_cache[key] = result
            return result
        except Exception:
            pass
    elif os.path.exists(disk_path + '.png'):
        try:
            with open(disk_path + '.png', 'rb') as f:
                data = f.read()
            result = (data, 'image/png')
            with _dropbox_img_cache_lock:
                if len(_dropbox_img_cache) > 200:
                    _dropbox_img_cache.clear()
                _dropbox_img_cache[key] = result
            return result
        except Exception:
            pass

    # 3. Check if this image exists in the index
    dropbox_path = _dropbox_photo_index.get(key)
    if not dropbox_path:
        return None, None

    # 4. Download on demand and save to disk
    data, ct = _download_dropbox_file(dropbox_path)
    if data:
        ext = '.png' if 'png' in ct else '.jpg'
        try:
            with open(disk_path + ext, 'wb') as f:
                f.write(data)
        except Exception:
            pass
        with _dropbox_img_cache_lock:
            if len(_dropbox_img_cache) > 200:
                _dropbox_img_cache.clear()
            _dropbox_img_cache[key] = (data, ct)
        return data, ct

    return None, None


def _upload_to_s3_sync(image_code, data, content_type):
    """Upload an image to S3 DROPBOX_SYNC folder for CDN delivery."""
    try:
        s3 = get_s3()
        ext = '.png' if 'png' in content_type else '.jpg'
        key = f"{S3_DROPBOX_SYNC_PREFIX}/{image_code}{ext}"
        s3.put_object(
            Bucket=S3_BUCKET,
            Key=key,
            Body=data,
            ContentType=content_type,
            CacheControl='public, max-age=86400',
        )
        # Invalidate CloudFront so new/updated image is instantly live
        cf_path = f"/ALL+INVENTORY+Photos/DROPBOX_SYNC/{image_code}{ext}"
        threading.Thread(target=_invalidate_cloudfront, args=([cf_path],), daemon=True).start()
        return True
    except Exception as e:
        print(f"[S3 Sync] Upload failed for {image_code}: {e}")
        return False


def prewarm_dropbox_cache():
    """Background job: download ALL Dropbox images to disk cache. Only one worker runs this."""
    # Use a file lock so only one worker pre-warms
    lock_file = '/tmp/dropbox_prewarm.lock'
    try:
        # Check for stale lock (older than 30 minutes)
        if os.path.exists(lock_file):
            lock_age = time.time() - os.path.getmtime(lock_file)
            if lock_age > 1800:  # 30 min stale threshold
                print(f"[Dropbox Pre-warm] Removing stale lock ({lock_age:.0f}s old)", flush=True)
                os.unlink(lock_file)
            else:
                print(f"[Dropbox Pre-warm] Another worker is already pre-warming, skipping", flush=True)
                return
        fd = os.open(lock_file, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        os.write(fd, str(os.getpid()).encode())
        os.close(fd)
    except FileExistsError:
        print(f"[Dropbox Pre-warm] Another worker is already pre-warming, skipping", flush=True)
        return

    try:
        if not _dropbox_photo_index:
            return

        # Count how many are already cached
        already_cached = 0
        to_download = []
        for key in _dropbox_photo_index:
            disk_path = _get_disk_cache_path(key)
            if os.path.exists(disk_path + '.jpg') or os.path.exists(disk_path + '.png'):
                already_cached += 1
            else:
                to_download.append(key)

        total = len(_dropbox_photo_index)
        print(f"[Dropbox Pre-warm] {already_cached}/{total} already cached on disk, {len(to_download)} to download...", flush=True)

        # Check which images already exist in S3 DROPBOX_SYNC
        s3_synced = set()
        try:
            s3 = get_s3()
            paginator = s3.get_paginator('list_objects_v2')
            for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=S3_DROPBOX_SYNC_PREFIX + '/'):
                for obj in page.get('Contents', []):
                    fname = obj['Key'].rsplit('/', 1)[-1]
                    code = os.path.splitext(fname)[0].upper()
                    s3_synced.add(code)
            print(f"[Dropbox Pre-warm] {len(s3_synced)} images already in S3 DROPBOX_SYNC", flush=True)
        except Exception as e:
            print(f"[Dropbox Pre-warm] Could not list S3 sync folder: {e}", flush=True)

        # If everything is on disk AND S3, nothing to do
        if not to_download and s3_synced.issuperset(_dropbox_photo_index.keys()):
            print(f"[Dropbox Pre-warm] ✓ All {total} images on disk and S3", flush=True)
            return

        downloaded = [0]
        failed = [0]
        s3_uploaded = [0]

        def _download_one(key):
            dropbox_path = _dropbox_photo_index.get(key)
            if not dropbox_path:
                return

            # Check if already on disk
            disk_path = _get_disk_cache_path(key)
            already_on_disk = os.path.exists(disk_path + '.jpg') or os.path.exists(disk_path + '.png')
            already_on_s3 = key in s3_synced

            # Skip if both disk and S3 are done
            if already_on_disk and already_on_s3:
                return

            # Read from disk if available, otherwise download from Dropbox
            data = None
            ct = None
            if already_on_disk:
                try:
                    ext = '.jpg' if os.path.exists(disk_path + '.jpg') else '.png'
                    with open(disk_path + ext, 'rb') as f:
                        data = f.read()
                    ct = 'image/jpeg' if ext == '.jpg' else 'image/png'
                except Exception:
                    pass

            if not data:
                data, ct = _download_dropbox_file(dropbox_path)
                if data:
                    ext = '.png' if 'png' in ct else '.jpg'
                    try:
                        with open(disk_path + ext, 'wb') as f:
                            f.write(data)
                        downloaded[0] += 1
                    except Exception:
                        failed[0] += 1
                        return
                else:
                    failed[0] += 1
                    return

            # Upload to S3 if not already there
            if not already_on_s3 and data:
                if _upload_to_s3_sync(key, data, ct):
                    s3_uploaded[0] += 1

            # Progress update every 200 images
            done = downloaded[0] + s3_uploaded[0]
            if done % 200 == 0 and done > 0:
                print(f"[Dropbox Pre-warm] Progress: {downloaded[0]} downloaded, {s3_uploaded[0]} synced to S3, {failed[0]} failed", flush=True)

        # Process ALL keys — some may be on disk but not S3 yet
        all_keys = list(_dropbox_photo_index.keys())

        # Use gevent pool — only 3 concurrent to limit memory
        try:
            from gevent.pool import Pool
            pool = Pool(size=3)
            pool.map(_download_one, all_keys)
        except ImportError:
            for key in all_keys:
                _download_one(key)

        print(f"[Dropbox Pre-warm] ✓ Done! {downloaded[0]} new downloads, {s3_uploaded[0]} synced to S3, {failed[0]} failed", flush=True)
    finally:
        try:
            os.unlink(lock_file)
        except Exception:
            pass


DROPBOX_THUMB_CACHE_DIR = os.path.join(os.path.dirname(DROPBOX_DISK_CACHE), 'dropbox_thumbs')
os.makedirs(DROPBOX_THUMB_CACHE_DIR, exist_ok=True)

def _get_thumb_disk_path(key):
    return os.path.join(DROPBOX_THUMB_CACHE_DIR, key + '.thumb')

def get_dropbox_thumbnail(image_code, tw=TARGET_W, th=TARGET_H):
    """Get resized thumbnail from Dropbox for Excel exports.
    Priority: in-memory → disk thumb cache → raw disk cache → Dropbox download.
    Thumbnails are persisted to disk so PIL processing only ever happens once."""
    import struct
    key = image_code.upper().replace('-', '_')

    # 1. In-memory hot cache (fastest)
    if key in _dropbox_thumb_cache:
        return _dropbox_thumb_cache[key]

    # Check index first — skip entirely for unknown images
    if key not in _dropbox_photo_index:
        return None

    def _cache_in_memory(result):
        """Add to in-memory cache with soft cap — disk is the real cache."""
        if len(_dropbox_thumb_cache) > 500:
            for old_key in list(_dropbox_thumb_cache.keys())[:100]:
                del _dropbox_thumb_cache[old_key]
        _dropbox_thumb_cache[key] = result
        return result

    # 2. Disk thumb cache — pre-processed, no PIL needed
    thumb_path = _get_thumb_disk_path(key)
    if os.path.exists(thumb_path):
        try:
            with open(thumb_path, 'rb') as f:
                meta = f.read(32)
                raw = f.read()
            x_scale, y_scale, x_offset, y_offset = struct.unpack('4d', meta)
            result = {
                'raw_bytes': raw,
                'x_scale': x_scale, 'y_scale': y_scale,
                'x_offset': x_offset, 'y_offset': y_offset,
                'url': f'dropbox://{key}'
            }
            return _cache_in_memory(result)
        except Exception:
            pass  # Corrupted — fall through to regenerate

    # 3. Get raw image bytes (disk or Dropbox API)
    raw_bytes, ct = get_dropbox_image_bytes(image_code)
    if not raw_bytes:
        return None

    # 4. PIL resize — happens at most once per image ever
    try:
        with PilImage.open(BytesIO(raw_bytes)) as im:
            im = ImageOps.exif_transpose(im)
            im.thumbnail((tw * 2, th * 2), PilImage.Resampling.LANCZOS)
            if im.mode in ("RGBA", "LA") or (im.mode == "P" and "transparency" in im.info):
                fmt = "PNG"
            else:
                if im.mode != "RGB":
                    im = im.convert("RGB")
                fmt = "JPEG"
            buf = BytesIO()
            im.save(buf, format=fmt, quality=85, optimize=True)
            processed = buf.getvalue()
            ow, oh = im.size
        wr = tw / ow
        hr = th / oh
        sf = min(wr, hr)
        x_off = (tw - ow * sf) / 2
        y_off = (th - oh * sf) / 2
        result = {
            'raw_bytes': processed,
            'x_scale': sf, 'y_scale': sf,
            'x_offset': x_off, 'y_offset': y_off,
            'url': f'dropbox://{key}'
        }
        # Persist to disk — next server restart skips PIL entirely
        try:
            with open(thumb_path, 'wb') as f:
                f.write(struct.pack('4d', sf, sf, x_off, y_off))
                f.write(processed)
        except Exception:
            pass
        return _cache_in_memory(result)
    except Exception:
        return None
def get_image_cached(item, s3_base_url):
    """
    Get image for an item, using cache keyed by base_style.
    Priority: base64 override → STYLE+OVERRIDES → Dropbox → brand folder fallback.
    All size variants of the same style share one cache entry.
    """
    sku = item.get('sku', '')
    base_style = get_base_style(sku)

    # Check cache first — all size variants share same image
    with _img_lock:
        if base_style in _img_cache:
            c = _img_cache[base_style]
            if c is None:
                return None  # Previously failed — skip
            return {
                'image_data': BytesIO(c['raw_bytes']),
                'x_scale': c['x_scale'], 'y_scale': c['y_scale'],
                'x_offset': c['x_offset'], 'y_offset': c['y_offset'],
                'object_position': 1, 'url': c['url']
            }

    result = None

    # 1. Platform base64 override (highest priority)
    override_data = _style_overrides.get(base_style)
    if override_data and isinstance(override_data, dict) and override_data.get('image'):
        try:
            import base64
            img_str = override_data['image']
            # Strip data URI prefix if present
            if ',' in img_str:
                img_str = img_str.split(',', 1)[1]
            raw = base64.b64decode(img_str)
            with PilImage.open(BytesIO(raw)) as im:
                im = ImageOps.exif_transpose(im)
                tw, th = TARGET_W, TARGET_H
                im.thumbnail((tw * 2, th * 2), PilImage.Resampling.LANCZOS)
                if im.mode in ("RGBA", "LA") or (im.mode == "P" and "transparency" in im.info):
                    fmt = "PNG"
                else:
                    if im.mode != "RGB":
                        im = im.convert("RGB")
                    fmt = "JPEG"
                buf = BytesIO()
                im.save(buf, format=fmt, quality=85, optimize=True)
                ow, oh = im.size
            wr = tw / ow
            hr = th / oh
            sf = min(wr, hr)
            result = {
                'raw_bytes': buf.getvalue(),
                'x_scale': sf, 'y_scale': sf,
                'x_offset': (tw - ow * sf) / 2,
                'y_offset': (th - oh * sf) / 2,
                'url': f'override://{base_style}'
            }
        except Exception:
            pass

    # 2. Try STYLE+OVERRIDES via CloudFront
    if not result:
        base_style_for_url = get_base_style(sku)
        override_url = f"{CLOUDFRONT_OVERRIDES_URL}/{base_style_for_url}.jpg"
        result = _process_image_from_url(override_url)

    # 3. Try Dropbox photos
    if not result:
        brand_abbr = item.get('brand_abbr', item.get('brand', ''))
        image_code = extract_image_code(sku, brand_abbr)
        result = get_dropbox_thumbnail(image_code)

    # 4. Fallback to CloudFront brand folder
    if not result:
        brand_abbr_cf = item.get('brand_abbr', item.get('brand', ''))
        folder_name_cf = FOLDER_MAPPING.get(brand_abbr_cf, brand_abbr_cf)
        image_code_cf = extract_image_code(sku, brand_abbr_cf)
        brand_url = f"{CLOUDFRONT_PHOTOS_URL}/{folder_name_cf}/{image_code_cf}.jpg"
        result = _process_image_from_url(brand_url)

    # Cache result (even None to avoid re-fetching failures)
    with _img_lock:
        _img_cache[base_style] = result

    if result:
        return {
            'image_data': BytesIO(result['raw_bytes']),
            'x_scale': result['x_scale'], 'y_scale': result['y_scale'],
            'x_offset': result['x_offset'], 'y_offset': result['y_offset'],
            'object_position': 1, 'url': result['url']
        }
    return None


def download_images_for_items(items, s3_base_url, use_cache=True):
    """Download images for all items using thread pool. Deduplicates by base_style."""
    results = {}

    # Deduplicate: only fetch once per unique base_style
    style_to_indices = {}  # base_style → list of item indices
    unique_items = {}      # base_style → first item with that style
    for i, item in enumerate(items):
        base_style = get_base_style(item.get('sku', ''))
        if base_style not in style_to_indices:
            style_to_indices[base_style] = []
            unique_items[base_style] = item
        style_to_indices[base_style].append(i)

    def _fetch(base_style_item):
        base_style, item = base_style_item
        try:
            img = get_image_cached(item, s3_base_url)
            return base_style, img
        except Exception:
            return base_style, None

    # Clear previously-failed cache entries so they get retried
    with _img_lock:
        failed_keys = [k for k, v in _img_cache.items() if v is None]
        for k in failed_keys:
            del _img_cache[k]

    unique_pairs = list(unique_items.items())
    print(f"    Fetching images: {len(unique_pairs)} unique styles for {len(items)} items")

    def _store_results(base_style, img):
        """Store fetched image data for all item indices sharing this base_style."""
        if not img:
            return
        for idx in style_to_indices.get(base_style, []):
            # Use the returned image data directly — don't re-read from cache
            try:
                raw = img.get('image_data')
                if raw:
                    raw.seek(0)
                    raw_bytes = raw.read()
                    results[idx] = {
                        'image_data': BytesIO(raw_bytes),
                        'x_scale': img.get('x_scale', 1), 'y_scale': img.get('y_scale', 1),
                        'x_offset': img.get('x_offset', 0), 'y_offset': img.get('y_offset', 0),
                        'object_position': 1, 'url': img.get('url', '')
                    }
            except Exception:
                pass

    # Use gevent pool if available (gevent monkey-patches break ThreadPoolExecutor)
    try:
        import gevent.pool
        pool = gevent.pool.Pool(size=30)
        fetch_results = pool.map(_fetch, unique_pairs)
        for result in fetch_results:
            try:
                base_style, img = result
                _store_results(base_style, img)
            except Exception:
                pass
    except ImportError:
        # Fallback to ThreadPoolExecutor if gevent not installed
        with concurrent.futures.ThreadPoolExecutor(max_workers=30) as pool:
            futures = {pool.submit(_fetch, pair): pair[0] for pair in unique_pairs}
            for f in concurrent.futures.as_completed(futures):
                try:
                    base_style, img = f.result()
                    _store_results(base_style, img)
                except Exception:
                    pass

    found = len(results)
    missed = len(items) - found
    print(f"    Image results: {found}/{len(items)} found, {missed} missing")
    return results


def _setup_worksheet(workbook, worksheet, has_color=False, view_mode='all',
                     is_order=False, incoming_only=False, catalog_mode=False):
    fmt_header = workbook.add_format({
        'bold': True, 'font_name': STYLE_CONFIG['font_name'], 'font_size': 11,
        'bg_color': STYLE_CONFIG['header_bg'], 'font_color': STYLE_CONFIG['header_text'],
        'border': 1, 'border_color': STYLE_CONFIG['border_color'],
        'align': 'center', 'valign': 'vcenter'
    })
    base = {
        'font_name': STYLE_CONFIG['font_name'], 'font_size': 10,
        'text_wrap': True, 'valign': 'vcenter', 'align': 'center',
        'border': 1, 'border_color': STYLE_CONFIG['border_color']
    }
    fmts = {
        'odd':  workbook.add_format({**base, 'bg_color': STYLE_CONFIG['row_bg_odd']}),
        'even': workbook.add_format({**base, 'bg_color': STYLE_CONFIG['row_bg_even']}),
        'num_odd':  workbook.add_format({**base, 'bg_color': STYLE_CONFIG['row_bg_odd'],  'num_format': '#,##0'}),
        'num_even': workbook.add_format({**base, 'bg_color': STYLE_CONFIG['row_bg_even'], 'num_format': '#,##0'}),
    }

    worksheet.hide_gridlines(2)
    worksheet.freeze_panes(1, 0)

    if catalog_mode:
        # ── Catalog exports: no committed/allocated, simplified layout ──
        if view_mode == 'incoming':
            headers = ['IMAGE', 'SKU', 'Brand']
            if has_color:
                headers.append('Color')
            headers.extend(['Fit', 'Fabrication'])
            if is_order:
                headers.append('Qty Selected')
            headers.extend(['Incoming', 'Overseas ATS', 'Ex-Factory', 'Arrival'])
        else:
            # Warehouse / All view: show warehouse names column instead of per-WH quantities
            headers = ['IMAGE', 'SKU', 'Brand']
            if has_color:
                headers.append('Color')
            headers.extend(['Fit', 'Fabrication'])
            if is_order:
                headers.append('Qty Selected')
            headers.extend(['Warehouse', 'Total ATS'])
            # All Inventory catalog view: include date columns for items with incoming stock
            if view_mode == 'all':
                headers.extend(['Ex-Factory', 'Arrival'])
    elif view_mode == 'incoming':
        # Admin overseas view: no warehouse columns, add dates
        headers = ['IMAGE', 'SKU', 'Brand']
        if has_color:
            headers.append('Color')
        headers.extend(['Fit', 'Fabrication'])
        if is_order:
            headers.append('Qty Selected')
        headers.extend(['Incoming', 'Committed', 'Allocated', 'Overseas ATS',
                        'Ex-Factory', 'Arrival'])
    else:
        # Admin standard / ATS / All view — full columns
        headers = ['IMAGE', 'SKU', 'Brand']
        if has_color:
            headers.append('Color')
        headers.extend(['Fit', 'Fabrication', 'Delivery'])
        if is_order:
            headers.append('Qty Selected')
        if not incoming_only:
            headers.extend(['JTW', 'TR', 'DCW', 'QA'])
        headers.append('Incoming')
        if not incoming_only:
            headers.append('Total Warehouse')
        headers.append('Total ATS')

    worksheet.set_row(0, 25)
    for c, h in enumerate(headers):
        worksheet.write(0, c, h, fmt_header)

    # Set column widths based on header names
    col_widths = {
        'IMAGE': COL_WIDTH_UNITS, 'SKU': 20, 'Brand': 20, 'Color': 18,
        'Fit': 12, 'Fabrication': 35, 'Delivery': 14, 'Qty Selected': 14,
        'JTW': 12, 'TR': 12, 'DCW': 12, 'QA': 12, 'Incoming': 12,
        'Total Warehouse': 14, 'Total ATS': 12, 'Overseas ATS': 14,
        'Committed': 12, 'Allocated': 12, 'Ex-Factory': 14, 'Arrival': 14,
        'Warehouse': 18,
    }
    for c, h in enumerate(headers):
        worksheet.set_column(c, c, col_widths.get(h, 12))

    worksheet.set_default_row(112.5)
    return fmts, headers


def _write_rows(workbook, worksheet, data, images, fmts, has_color=False,
                view_mode='all', headers=None):
    """Write data rows using headers list to determine column layout."""
    if not headers:
        headers = []

    # Map header names to data field getters
    FIELD_MAP = {
        'IMAGE': lambda item: '',
        'SKU': lambda item: item.get('sku', ''),
        'Brand': lambda item: item.get('brand_full', ''),
        'Color': lambda item: item.get('color', ''),
        'Fit': lambda item: item.get('fit', 'N/A'),
        'Fabrication': lambda item: item.get('fabrication', 'Standard Fabric'),
        'Delivery': lambda item: item.get('delivery', 'ATS'),
        'Qty Selected': lambda item: item.get('quantity_ordered', 0),
        'JTW': lambda item: item.get('jtw', 0),
        'TR': lambda item: item.get('tr', 0),
        'DCW': lambda item: item.get('dcw', 0),
        'QA': lambda item: item.get('qa', 0),
        'Incoming': lambda item: item.get('incoming', 0),
        'Total Warehouse': lambda item: item.get('total_warehouse', 0),
        'Total ATS': lambda item: item.get('total_ats', 0),
        'Overseas ATS': lambda item: item.get('total_ats', 0),
        'Committed': lambda item: item.get('committed', 0),
        'Allocated': lambda item: item.get('allocated', 0),
        'Ex-Factory': lambda item: item.get('ex_factory', ''),
        'Arrival': lambda item: item.get('arrival', ''),
        'Warehouse': lambda item: item.get('warehouse', ''),
    }

    # Determine which columns are numeric for formatting
    NUMERIC_HEADERS = {
        'Qty Selected', 'JTW', 'TR', 'DCW', 'QA', 'Incoming',
        'Total Warehouse', 'Total ATS', 'Overseas ATS',
        'Committed', 'Allocated'
    }

    for r, item in enumerate(data):
        row = r + 1
        even = r % 2 == 1
        cf = fmts['even'] if even else fmts['odd']
        nf = fmts['num_even'] if even else fmts['num_odd']

        for c, h in enumerate(headers):
            getter = FIELD_MAP.get(h)
            val = getter(item) if getter else ''
            fmt = nf if h in NUMERIC_HEADERS else cf
            worksheet.write(row, c, val, fmt)

        img = images.get(r)
        if img:
            try:
                worksheet.insert_image(row, 0, "img.png", _padded_image_opts(img))
            except Exception:
                worksheet.write(row, 0, "Error", cf)
        else:
            worksheet.write(row, 0, "No Image", cf)
    return len(data)


def _add_size_charts(workbook, worksheet, start, prepack_defaults=None, items=None):
    """
    Render prepack size scale grids vertically at the bottom of the worksheet.
    One grid per block of rows, stacked top-to-bottom.
    Only shows rules that match items on THIS tab — so each brand tab is independent.
    Falls back to hardcoded Slim/Regular if no rules matched.
    """
    t  = workbook.add_format({'bold':True, 'font_name':'Calibri', 'font_size':11,
                               'bg_color':'#FFFFFF', 'border':0, 'align':'left', 'valign':'vcenter'})
    s  = workbook.add_format({'bold':True, 'font_name':'Calibri', 'font_size':10,
                               'bg_color':'#FFFFFF', 'font_color':'#CC0000', 'border':0,
                               'align':'left', 'valign':'vcenter'})
    gh = workbook.add_format({'bold':True, 'font_name':'Calibri', 'font_size':10,
                               'border':1, 'align':'center', 'valign':'vcenter', 'bg_color':'#F3F4F6'})
    gd = workbook.add_format({'font_name':'Calibri', 'font_size':10,
                               'border':1, 'align':'center', 'valign':'vcenter', 'bg_color':'#FFFFFF'})

    # ── Determine which rules to show ─────────────────────────────────────
    packs_to_render = []

    if prepack_defaults and items:
        seen_keys = {}
        for item in items:
            cat = item.get('_export_category', '')
            fit = item.get('_export_fit', '')
            cust = item.get('_export_customer', '')
            sku = item.get('sku', '').upper()
            base = sku.split('-')[0]

            # PRIORITY A: Per-item Override Tool size pack
            override_pack = item.get('_override_size_pack')
            if override_pack and isinstance(override_pack, dict) and override_pack.get('sizes'):
                key = ('__override__', sku)
                if key not in seen_keys:
                    seen_keys[key] = {
                        'label': f"Override: {base}",
                        'master_qty': override_pack.get('master_qty', '?'),
                        'inner_qty':  override_pack.get('inner_qty', '?'),
                        'sizes':      override_pack.get('sizes', []),
                        '_is_override': True
                    }
                continue  # This item is handled — don't also add a prepack rule for it

            # PRIORITY B: SKU-specific assignment on any prepack rule
            sku_matched = None
            for rule in prepack_defaults:
                for s in (rule.get('skus') or []):
                    su = s.upper().strip()
                    if su and (su == sku or su == base or sku.startswith(su)):
                        sku_matched = rule
                        break
                if sku_matched:
                    break

            matched = sku_matched
            if not matched:
                # PRIORITY C: Score-based dimension matching (category, fit, customer, brand)
                brand_abbr = item.get('brand_abbr', item.get('brand', '')).upper()
                best_score = -1
                best_rule = None

                for r in prepack_defaults:
                    # Skip rules that have a SKU list — they should ONLY match via Priority B (exact SKU),
                    # never act as a catch-all through dimension scoring
                    r_skus = r.get('skus') or []
                    if isinstance(r_skus, list) and len([s for s in r_skus if s and s.strip()]) > 0:
                        continue

                    # Category must match or be 'any'
                    r_cat = r.get('category', 'any')
                    if r_cat != cat and r_cat != 'any':
                        continue

                    # Fits: if specified, item must match
                    r_fits = r.get('fits')
                    if isinstance(r_fits, list):
                        r_fits = [x for x in r_fits if x and x != 'any']
                    else:
                        legacy = r.get('fit', '')
                        r_fits = [legacy] if legacy and legacy != 'any' else []
                    if r_fits and fit not in r_fits:
                        continue

                    # Customers: if specified, item must match
                    r_custs = r.get('customers')
                    if isinstance(r_custs, list):
                        r_custs = [c.upper().strip() for c in r_custs if c and c.strip()]
                    else:
                        legacy_c = (r.get('customer') or '').strip()
                        r_custs = [legacy_c.upper()] if legacy_c else []
                    if r_custs and (not cust or cust.upper() not in r_custs):
                        continue

                    # Brands: if specified, item must match
                    r_brands = r.get('brands')
                    if isinstance(r_brands, list):
                        r_brands = [b.upper().strip() for b in r_brands if b and b.strip()]
                    else:
                        r_brands = []
                    if r_brands and (not brand_abbr or brand_abbr not in r_brands):
                        continue

                    # Score: +1 per specific dimension
                    score = 0
                    if r_cat != 'any':
                        score += 1
                    if r_fits:
                        score += 1
                    if r_custs:
                        score += 1
                    if r_brands:
                        score += 1

                    if score > best_score:
                        best_score = score
                        best_rule = r

                matched = best_rule

            if matched:
                key = matched.get('id', id(matched))
                if key not in seen_keys:
                    seen_keys[key] = matched

        # Render override grids first (sorted by SKU), then rule-based grids
        override_packs = [(k, v) for k, v in seen_keys.items() if k[0] == '__override__']
        rule_packs     = [(k, v) for k, v in seen_keys.items() if k[0] != '__override__']
        packs_to_render = [v for _, v in sorted(override_packs, key=lambda x: x[0][1])] + [v for _, v in rule_packs]

    elif items:
        # No prepack_defaults supplied — still check for item-level overrides
        seen_overrides = {}
        for item in items:
            override_pack = item.get('_override_size_pack')
            sku = item.get('sku', '').upper()
            base = sku.split('-')[0]
            if override_pack and isinstance(override_pack, dict) and override_pack.get('sizes'):
                if base not in seen_overrides:
                    seen_overrides[base] = {
                        'label': f"Override: {base}",
                        'master_qty': override_pack.get('master_qty', '?'),
                        'inner_qty':  override_pack.get('inner_qty', '?'),
                        'sizes':      override_pack.get('sizes', []),
                    }
        packs_to_render = list(seen_overrides.values())

    # ── Fallback: hardcoded Slim + Regular ────────────────────────────────
    if not packs_to_render:
        packs_to_render = [
            {'label': 'Slim Fit', 'master_qty': 36, 'inner_qty': '4 - 9pc Inners',
             'sizes': [['14-14.5 / 32-33', 4], ['15-15.5 / 32-33', 8], ['15-15.5 / 34-35', 4],
                       ['16-16.5 / 32-33', 4], ['16-16.5 / 34-35', 8], ['17-17.5 / 34-35', 8]]},
            {'label': 'Regular Fit', 'master_qty': 36, 'inner_qty': '4 - 9pc Inners',
             'sizes': [['15-15.5 / 32-33', 8], ['15-15.5 / 34-35', 8], ['16-16.5 / 32-33', 4],
                       ['16-16.5 / 34-35', 4], ['17-17.5 / 34-35', 4], ['17-17.5 / 36-37', 4],
                       ['18-18.5 / 36-37', 4]]},
        ]

    # ── Compute ratios helper ─────────────────────────────────────────────
    from math import gcd
    from functools import reduce
    def _compute_ratios(sizes):
        if not sizes:
            return []
        qtys = [q for _, q in sizes if q > 0]
        if not qtys:
            return [(n, 0) for n, _ in sizes]
        g = reduce(gcd, qtys)
        return [(n, q // g) for n, q in sizes] if g > 0 else sizes

    # ── Render each rule as a vertical block ──────────────────────────────
    r = start

    for rule in packs_to_render:
        sizes  = rule.get('sizes') or []
        # Ensure sizes is a list of [name, qty] pairs with numeric qty
        safe_sizes = []
        for sz in sizes:
            if isinstance(sz, (list, tuple)) and len(sz) >= 2:
                safe_sizes.append([str(sz[0]), int(sz[1]) if sz[1] else 0])
        sizes = safe_sizes
        if not sizes:
            continue  # Skip rules with no sizes defined
        master = rule.get('master_qty', '?')
        inner  = rule.get('inner_qty', '—')  # free-text string
        if isinstance(inner, (int, float)):
            inner = str(inner)  # legacy numeric → string
        label  = rule.get('label') or rule.get('category', '?')

        worksheet.set_row(r, 20)
        inner_part = '{}, '.format(inner) if inner and inner != '—' else ''
        worksheet.write(r, 0, '{} | {}{} pcs / box'.format(label, inner_part, master), t)
        r += 1

        worksheet.set_row(r, 16)
        worksheet.write(r, 0, '{} SIZE SCALE TO USE'.format(label.upper()), s)
        r += 1

        ratios = _compute_ratios(sizes)
        is_neck_sleeve = any('/' in str(sz[0]) for sz in sizes if sz)

        if is_neck_sleeve:
            # Parse neck/sleeve — e.g. "15-15.5 / 32-33" — using ratios
            neck_map   = {}   # neck -> {sleeve: ratio}
            neck_order = []
            slv_order  = []
            for (sz, _), (_, ratio) in zip(sizes, ratios):
                parts = [p.strip() for p in sz.split('/')]
                neck  = parts[0]
                slv   = parts[1] if len(parts) > 1 else ''
                if neck not in neck_map:
                    neck_map[neck] = {}
                    neck_order.append(neck)
                neck_map[neck][slv] = ratio
                if slv not in slv_order:
                    slv_order.append(slv)

            # Column header row: blank corner + neck sizes as columns
            worksheet.set_row(r, 22)
            worksheet.write(r, 0, '', gh)
            for ci, neck in enumerate(neck_order):
                worksheet.write(r, 1 + ci, neck, gh)
            r += 1

            # One data row per sleeve length
            for slv in slv_order:
                worksheet.set_row(r, 22)
                worksheet.write(r, 0, slv, gh)
                for ci, neck in enumerate(neck_order):
                    val = neck_map[neck].get(slv, '')
                    worksheet.write(r, 1 + ci, val if val != '' else '', gd)
                r += 1

        else:
            # Flat S/M/L — header row then ratio row
            worksheet.set_row(r, 22)
            worksheet.write(r, 0, 'Size', gh)
            for ci, (sz, _) in enumerate(ratios):
                worksheet.write(r, 1 + ci, sz, gh)
            r += 1

            worksheet.set_row(r, 22)
            worksheet.write(r, 0, 'Ratio', gh)
            for ci, (_, ratio) in enumerate(ratios):
                worksheet.write(r, 1 + ci, ratio, gd)
            r += 1

        # Blank gap row between grids
        worksheet.set_row(r, 10)
        r += 1


def build_brand_excel(brand_name, items, s3_base_url, view_mode='all', is_order=False,
                      catalog_mode=False, prepack_defaults=None):
    has_color = any(item.get('color') for item in items)

    # Auto-detect incoming_only: all items have zero warehouse stock
    # (only applies to non-incoming view modes — incoming view already omits warehouse)
    incoming_only = False
    if view_mode != 'incoming' and items and not catalog_mode:
        incoming_only = all(
            item.get('total_warehouse', 0) == 0
            for item in items
        )

    buf = BytesIO()
    wb = xlsxwriter.Workbook(buf, {'in_memory': True})
    wb.set_properties({'title': f'Versa - {brand_name}', 'author': 'Versa Inventory System'})
    ws = wb.add_worksheet(brand_name[:31])
    # Monkey-patch worksheet.write to catch string-as-format bugs
    _orig_ws_write = ws.write
    def _safe_ws_write(r, c, val=None, fmt=None, *args):
        if fmt is not None and isinstance(fmt, str):
            import traceback as _tb
            print(f"  🚨 STRING-AS-FORMAT at row={r} col={c} val={repr(val)[:80]} fmt={repr(fmt)[:80]}")
            _tb.print_stack()
            return  # skip the bad write
        if fmt is not None:
            _orig_ws_write(r, c, val, fmt, *args)
        elif val is not None:
            _orig_ws_write(r, c, val)
        else:
            _orig_ws_write(r, c)
    ws.write = _safe_ws_write
    print(f"  [build_brand_excel] Step 1: setup worksheet")
    fmts, headers = _setup_worksheet(wb, ws, has_color=has_color, view_mode=view_mode,
                                     is_order=is_order, incoming_only=incoming_only,
                                     catalog_mode=catalog_mode)
    print(f"  [build_brand_excel] Step 2: download images")
    imgs = download_images_for_items(items, s3_base_url, use_cache=True)
    print(f"  [build_brand_excel] Step 3: write {len(items)} rows, headers={headers}")
    n = _write_rows(wb, ws, items, imgs, fmts, has_color=has_color,
                    view_mode=view_mode, headers=headers)
    print(f"  [build_brand_excel] Step 4: add size charts (prepack_defaults={type(prepack_defaults).__name__}, len={len(prepack_defaults) if prepack_defaults else 0})")
    try:
        _add_size_charts(wb, ws, n + 2, prepack_defaults=prepack_defaults, items=items)
        print(f"  [build_brand_excel] Step 4: size charts OK")
    except Exception as e:
        import traceback as _tb
        _tb.print_exc()
        print(f"  ⚠ Size charts failed (non-fatal): {e}")
    print(f"  [build_brand_excel] Step 5: wb.close()")
    try:
        wb.close()
    except Exception as e:
        import traceback as _tb
        _tb.print_exc()
        print(f"  ⚠ wb.close() failed: {e}")
        # Try again without size charts — rebuild from scratch
        print(f"  [build_brand_excel] Retrying WITHOUT size charts...")
        buf = BytesIO()
        wb = xlsxwriter.Workbook(buf, {'in_memory': True})
        wb.set_properties({'title': f'Versa - {brand_name}', 'author': 'Versa Inventory System'})
        ws = wb.add_worksheet(brand_name[:31])
        fmts, headers = _setup_worksheet(wb, ws, has_color=has_color, view_mode=view_mode,
                                         is_order=is_order, incoming_only=incoming_only,
                                         catalog_mode=catalog_mode)
        imgs = download_images_for_items(items, s3_base_url, use_cache=True)
        _write_rows(wb, ws, items, imgs, fmts, has_color=has_color,
                    view_mode=view_mode, headers=headers)
        wb.close()
        print(f"  [build_brand_excel] Retry succeeded (no size charts)")
    return buf.getvalue()


def build_multi_brand_excel(brands_list, s3_base_url, catalog_mode=False, view_mode='all', prepack_defaults=None):
    for b in brands_list:
        sort_key = 'total_ats' if catalog_mode else 'total_warehouse'
        b['items'] = sorted(b['items'], key=lambda x: x.get(sort_key, 0), reverse=True)

    has_color = any(item.get('color') for b in brands_list for item in b['items'])

    all_items = []
    offsets = []
    off = 0
    for b in brands_list:
        offsets.append((off, len(b['items'])))
        all_items.extend(b['items'])
        off += len(b['items'])

    all_imgs = download_images_for_items(all_items, s3_base_url, use_cache=True)

    buf = BytesIO()
    wb = xlsxwriter.Workbook(buf, {'in_memory': True})
    wb.set_properties({'title': 'Versa Multi-Brand Export', 'author': 'Versa Inventory System'})

    for bi, brand in enumerate(brands_list):
        safe = re.sub(r'[\\/*?\[\]:]', '', brand['brand_name'])[:31] or f"Brand_{bi+1}"
        ws = wb.add_worksheet(safe)
        # Monkey-patch worksheet.write to catch string-as-format bugs
        _orig_ws_write = ws.write
        def _safe_ws_write(r, c, val=None, fmt=None, *args, _orig=_orig_ws_write):
            if fmt is not None and isinstance(fmt, str):
                print(f"  🚨 STRING-AS-FORMAT at row={r} col={c} val={repr(val)[:80]} fmt={repr(fmt)[:80]}")
                return  # skip the bad write
            if fmt is not None:
                _orig(r, c, val, fmt, *args)
            elif val is not None:
                _orig(r, c, val)
            else:
                _orig(r, c)
        ws.write = _safe_ws_write
        fmts, headers = _setup_worksheet(wb, ws, has_color=has_color,
                                         catalog_mode=catalog_mode, view_mode=view_mode)
        start, count = offsets[bi]
        local_imgs = {}
        for li in range(count):
            gi = start + li
            if gi in all_imgs:
                local_imgs[li] = all_imgs[gi]
        n = _write_rows(wb, ws, brand['items'], local_imgs, fmts,
                        has_color=has_color, headers=headers)
        try:
            _add_size_charts(wb, ws, n + 2, prepack_defaults=prepack_defaults, items=brand['items'])
        except Exception as e:
            print(f"  ⚠ Size charts failed for {brand.get('brand_name','?')} (non-fatal): {e}")

    try:
        wb.close()
    except Exception as e:
        import traceback as _tb
        _tb.print_exc()
        print(f"  ⚠ wb.close() failed in multi-brand: {e} — retrying WITHOUT size charts...")
        # Retry from scratch without size charts
        buf = BytesIO()
        wb = xlsxwriter.Workbook(buf, {'in_memory': True})
        wb.set_properties({'title': 'Versa Multi-Brand Export', 'author': 'Versa Inventory System'})
        for bi, brand in enumerate(brands_list):
            safe = re.sub(r'[\\/*?\[\]:]', '', brand['brand_name'])[:31] or f"Brand_{bi+1}"
            ws = wb.add_worksheet(safe)
            fmts, headers = _setup_worksheet(wb, ws, has_color=has_color,
                                             catalog_mode=catalog_mode, view_mode=view_mode)
            start, count = offsets[bi]
            local_imgs = {}
            for li in range(count):
                gi = start + li
                if gi in all_imgs:
                    local_imgs[li] = all_imgs[gi]
            _write_rows(wb, ws, brand['items'], local_imgs, fmts,
                        has_color=has_color, headers=headers)
        wb.close()
        print(f"  ✓ Multi-brand retry succeeded (no size charts)")
    return buf.getvalue()


# ============================================
# PDF TILE EXPORT — 2×2 grid with cached images
# ============================================
def build_brand_pdf(title, items, s3_base_url, subtitle='', show_qty=False):
    """Generate a PDF with 2×2 product tile grid, using server-cached images."""

    page_w, page_h = letter  # 612 × 792 points
    margin = 34  # ~12mm
    cols, rows_per_page = 2, 2
    gap_x, gap_y = 18, 18
    usable_w = page_w - margin * 2
    usable_h = page_h - margin * 2 - 28  # room for header
    tile_w = (usable_w - gap_x * (cols - 1)) / cols
    tile_h = (usable_h - gap_y * (rows_per_page - 1)) / rows_per_page
    img_h = tile_h * 0.65
    img_w = tile_w - 18
    tiles_per_page = cols * rows_per_page

    # Pre-fetch images using existing server cache (fast, parallel, disk-cached)
    imgs = download_images_for_items(items, s3_base_url, use_cache=True)

    buf = BytesIO()
    c = pdf_canvas.Canvas(buf, pagesize=letter)
    c.setTitle(title)

    total_pages = max(1, -(-len(items) // tiles_per_page))
    date_str = datetime.now().strftime('%m/%d/%Y')

    for page in range(total_pages):
        if page > 0:
            c.showPage()

        # ── Header ──
        c.setFont('Helvetica-Bold', 14)
        c.setFillColor(HexColor('#1e1e1e'))
        c.drawString(margin, page_h - margin + 4, title)
        c.setFont('Helvetica', 9)
        c.setFillColor(HexColor('#787878'))
        header_right = f"{date_str}  \u2022  {subtitle}  \u2022  Page {page + 1} of {total_pages}"
        c.drawRightString(page_w - margin, page_h - margin + 4, header_right)
        c.setStrokeColor(HexColor('#c8c8c8'))
        c.setLineWidth(0.5)
        c.line(margin, page_h - margin - 6, page_w - margin, page_h - margin - 6)

        start_y = page_h - margin - 10

        page_items = items[page * tiles_per_page:(page + 1) * tiles_per_page]

        for idx, item in enumerate(page_items):
            col_idx = idx % cols
            row_idx = idx // cols
            x = margin + col_idx * (tile_w + gap_x)
            y = start_y - row_idx * (tile_h + gap_y)

            # Tile background + border
            c.setStrokeColor(HexColor('#d2d2d2'))
            c.setLineWidth(0.5)
            c.setFillColor(HexColor('#ffffff'))
            c.roundRect(x, y - tile_h, tile_w, tile_h, 5, fill=1, stroke=1)

            # ── Image ──
            img_x = x + (tile_w - img_w) / 2
            img_y_top = y - 6  # 6pt padding from tile top
            img_y_bottom = img_y_top - img_h
            global_idx = page * tiles_per_page + idx
            img_data = imgs.get(global_idx)

            drew_image = False
            if img_data and img_data.get('image_data'):
                try:
                    img_data['image_data'].seek(0)
                    pil_img = PilImage.open(img_data['image_data'])
                    ow, oh = pil_img.size
                    ratio = ow / oh
                    draw_w, draw_h = img_w, img_h
                    if ratio > img_w / img_h:
                        draw_h = img_w / ratio
                    else:
                        draw_w = img_h * ratio
                    draw_x = img_x + (img_w - draw_w) / 2
                    draw_y = img_y_bottom + (img_h - draw_h) / 2

                    img_buf = BytesIO()
                    if pil_img.mode in ('RGBA', 'LA', 'P'):
                        pil_img = pil_img.convert('RGBA')
                        pil_img.save(img_buf, format='PNG')
                    else:
                        if pil_img.mode != 'RGB':
                            pil_img = pil_img.convert('RGB')
                        pil_img.save(img_buf, format='JPEG', quality=85)
                    img_buf.seek(0)
                    c.drawImage(ImageReader(img_buf), draw_x, draw_y, draw_w, draw_h,
                                preserveAspectRatio=True, mask='auto')
                    drew_image = True
                except Exception:
                    pass

            if not drew_image:
                c.setFillColor(HexColor('#f3f4f6'))
                c.rect(img_x, img_y_bottom, img_w, img_h, fill=1, stroke=0)
                c.setFillColor(HexColor('#a0a0a0'))
                c.setFont('Helvetica', 8)
                c.drawCentredString(x + tile_w / 2, img_y_bottom + img_h / 2, 'No Image')

            # ── Short Sleeve badge ──
            fit_str = item.get('fit', '')
            if 'short sleeve' in fit_str.lower():
                badge_text = 'SHORT SLEEVE'
                c.setFont('Helvetica-Bold', 6)
                bw = c.stringWidth(badge_text, 'Helvetica-Bold', 6) + 8
                bh = 10
                bx = img_x + 3
                by = img_y_bottom + 3
                c.setFillColor(Color(14/255, 165/255, 233/255, 0.9))
                c.roundRect(bx, by, bw, bh, 3, fill=1, stroke=0)
                c.setFillColor(HexColor('#ffffff'))
                c.drawString(bx + 4, by + 3, badge_text)

            # ── Text below image ──
            text_x = x + 9
            ty = img_y_bottom - 12
            line_step = 11

            # SKU
            c.setFont('Helvetica-Bold', 9)
            c.setFillColor(HexColor('#1e1e1e'))
            c.drawString(text_x, ty, str(item.get('sku', '')))
            ty -= line_step

            # Brand
            c.setFont('Helvetica', 7.5)
            c.setFillColor(HexColor('#505050'))
            c.drawString(text_x, ty, str(item.get('brand_full', item.get('brand_abbr', '')))[:35])
            ty -= line_step

            # Color
            color_str = item.get('color', '')
            if color_str:
                c.setFillColor(HexColor('#6d28d9'))
                c.setFont('Helvetica-Bold', 7)
                c.drawString(text_x, ty, color_str[:40])
                ty -= line_step

            # Fit + Fabric
            fabric = item.get('fabrication', '')
            fit = item.get('fit', '')
            fit_fab = f"{fit} \u2022 {fabric[:25] + '...' if len(fabric) > 25 else fabric}"
            c.setFont('Helvetica', 6.5)
            c.setFillColor(HexColor('#787878'))
            c.drawString(text_x, ty, fit_fab[:55])
            ty -= line_step

            # Qty / ATS
            if show_qty:
                qty = item.get('quantity_ordered', item.get('quantity', 0)) or 0
                c.setFont('Helvetica-Bold', 8)
                c.setFillColor(HexColor('#16a34a'))
                c.drawString(text_x, ty, f"Qty: {qty:,}")
                c.setFont('Helvetica', 7)
                c.setFillColor(HexColor('#646464'))
                c.drawRightString(x + tile_w - 9, ty, f"ATS: {item.get('total_ats', 0):,}")
            else:
                c.setFont('Helvetica-Bold', 8)
                c.setFillColor(HexColor('#16a34a'))
                c.drawString(text_x, ty, f"ATS: {item.get('total_ats', 0):,}")

            # Delivery
            delivery = item.get('delivery', '')
            if delivery and delivery not in ('ATS', ''):
                ty -= line_step
                c.setFont('Helvetica', 6.5)
                c.setFillColor(HexColor('#d97706'))
                c.drawString(text_x, ty, f"Delivery: {delivery}")

    c.save()
    return buf.getvalue()


def _col_val(row_dict, name):
    if name in row_dict:
        return row_dict[name]
    lo = name.lower()
    for k, v in row_dict.items():
        if k.lower() == lo:
            return v
    return None


def parse_inventory_excel(file_bytes):
    wb = openpyxl.load_workbook(BytesIO(file_bytes), read_only=True, data_only=True)
    ws = wb[wb.sheetnames[0]]

    rows_iter = ws.iter_rows(values_only=False)
    header_row = next(rows_iter)
    headers = [str(cell.value or '').strip() for cell in header_row]

    items = []
    for row in rows_iter:
        rd = {headers[i]: row[i].value for i in range(min(len(headers), len(row)))}

        sku = str(_col_val(rd, 'SKU') or '').strip()
        brand = str(_col_val(rd, 'Brand') or '').strip().upper()
        if not sku or sku == 'N/A' or not brand:
            continue

        jtw = int(_col_val(rd, 'JTW') or 0)
        tr  = int(_col_val(rd, 'TR')  or 0)
        dcw = int(_col_val(rd, 'DCW') or 0)
        qa  = int(_col_val(rd, 'QA') or _col_val(rd, 'Q/A') or _col_val(rd, 'Quality') or 0)
        committed = int(_col_val(rd, 'Committed') or 0)
        allocated = int(_col_val(rd, 'Allocated') or 0)
        incoming  = int(_col_val(rd, 'Incoming') or _col_val(rd, 'In Transit') or
                        _col_val(rd, 'InTransit') or _col_val(rd, 'In-Transit') or
                        _col_val(rd, 'On Order') or _col_val(rd, 'PO') or
                        _col_val(rd, 'Incoming Qty') or 0)

        total_ats_raw = _col_val(rd, 'Total ATS') or _col_val(rd, 'Total_ATS') or _col_val(rd, 'TotalATS') or 0
        total_ats = int(total_ats_raw)

        container = str(_col_val(rd, 'Container') or '').strip()
        receive_date = str(_col_val(rd, 'Receive Date') or _col_val(rd, 'ReceiveDate') or '').strip()
        lot_number = str(_col_val(rd, 'Lot Number') or _col_val(rd, 'LotNumber') or '').strip()

        brand_full = BRAND_FULL_NAMES.get(brand, brand)

        items.append({
            'sku': sku,
            'brand': brand,
            'brand_abbr': brand,
            'brand_full': brand_full,
            'name': f"{brand} {sku}",
            'jtw': jtw, 'tr': tr, 'dcw': dcw, 'qa': qa,
            'incoming': incoming,
            'committed': committed, 'allocated': allocated,
            'total_ats': total_ats,
            'total_warehouse': jtw + tr + dcw + qa,
            'container': container,
            'receive_date': receive_date,
            'lot_number': lot_number,
            'image': ''
        })

    wb.close()
    return items


def _group_by_brand(items):
    brands = {}
    for item in items:
        abbr = item['brand']
        if abbr not in brands:
            brands[abbr] = {'name': item['brand_full'], 'items': []}
        brands[abbr]['items'].append(item)
    return brands


# ============================================
# DROPBOX SYNC — primary inventory source
# ============================================
DROPBOX_INVENTORY_PATH = os.environ.get('DROPBOX_INVENTORY_PATH', '/Versa Share Files/Hourly ATS/Inventory_ATS.xlsx')

def sync_from_dropbox():
    """Fetch inventory directly via Dropbox API — uses OAuth, never expires, no shared link needed."""
    token = get_dropbox_token()
    if not token:
        print("  ⚠ No Dropbox token available, skipping Dropbox sync")
        return False

    print(f"  📂 Fetching inventory from Dropbox API: {DROPBOX_INVENTORY_PATH}")
    try:
        headers = {
            'Authorization': f'Bearer {token}',
            'Dropbox-API-Arg': json.dumps({'path': DROPBOX_INVENTORY_PATH})
        }
        resp = http_requests.post(
            'https://content.dropboxapi.com/2/files/download',
            headers=headers, timeout=60
        )

        # Auto-refresh token if expired
        if resp.status_code == 401:
            print("  ⚠ Dropbox auth failed (401) — forcing token refresh...")
            global _dropbox_token_expires
            _dropbox_token_expires = 0
            token = get_dropbox_token()
            if not token:
                print("  ⚠ Could not refresh token, giving up")
                return False
            headers['Authorization'] = f'Bearer {token}'
            resp = http_requests.post(
                'https://content.dropboxapi.com/2/files/download',
                headers=headers, timeout=60
            )

        if resp.status_code != 200:
            print(f"  ⚠ Dropbox API returned HTTP {resp.status_code}: {resp.text[:200]}")
            return False

        data = resp.content
        if len(data) < 1000:
            print(f"  ⚠ Dropbox file too small ({len(data)} bytes), likely error")
            return False

        print(f"  📂 Downloaded {len(data):,} bytes from Dropbox API")

        items = parse_inventory_excel(data)
        if not items:
            print("  ⚠ No valid rows parsed from Dropbox file")
            return False

        brands = _group_by_brand(items)

        with _inv_lock:
            _inventory['items'] = items
            _inventory['brands'] = brands
            _inventory['etag'] = 'dropbox'
            _inventory['last_sync'] = datetime.utcnow().isoformat() + 'Z'
            _inventory['item_count'] = len(items)
            _inventory['source'] = 'dropbox'

        print(f"  ✓ Dropbox sync: {len(items)} items across {len(brands)} brands")
        return True

    except Exception as e:
        print(f"  ⚠ Dropbox sync failed: {type(e).__name__}: {e}")
        return False


# ============================================
# S3 SYNC — fallback inventory source
# ============================================
def s3_read_inventory():
    try:
        s3 = get_s3()
        resp = s3.get_object(Bucket=S3_BUCKET, Key=S3_INVENTORY_KEY)
        data = resp['Body'].read()
        etag = resp.get('ETag', '')
        print(f"  Downloaded s3://{S3_BUCKET}/{S3_INVENTORY_KEY} ({len(data)} bytes)")
        return data, etag
    except ClientError as e:
        print(f"  S3 read failed: {e.response['Error']['Code']}")
        return None, None
    except NoCredentialsError:
        print("  S3 read failed: no AWS credentials configured")
        return None, None
    except Exception as e:
        print(f"  S3 read failed: {e}")
        return None, None


def s3_check_etag():
    try:
        s3 = get_s3()
        resp = s3.head_object(Bucket=S3_BUCKET, Key=S3_INVENTORY_KEY)
        return resp.get('ETag', '')
    except Exception:
        return None


def s3_upload_export(key, file_bytes):
    try:
        s3 = get_s3()
        s3.put_object(
            Bucket=S3_BUCKET, Key=key, Body=file_bytes,
            ContentType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        )
        print(f"    Uploaded s3://{S3_BUCKET}/{key}")
        return True
    except Exception as e:
        print(f"    Upload failed for {key}: {e}")
        return False


def sync_inventory():
    """Sync inventory: try Dropbox first, then S3 fallback"""
    # Try Dropbox API first
    if DROPBOX_REFRESH_TOKEN or DROPBOX_PHOTOS_TOKEN:
        if sync_from_dropbox():
            return True
        print("  Dropbox failed, falling back to S3...")

    # S3 fallback
    new_etag = s3_check_etag()
    with _inv_lock:
        if new_etag and new_etag == _inventory['etag'] and _inventory['items']:
            print("  Inventory unchanged (same ETag)")
            return False

    data, etag = s3_read_inventory()
    if data is None:
        return False

    try:
        items = parse_inventory_excel(data)
    except Exception as e:
        print(f"  Failed to parse inventory: {e}")
        return False

    brands = _group_by_brand(items)

    with _inv_lock:
        _inventory['items'] = items
        _inventory['brands'] = brands
        _inventory['etag'] = etag
        _inventory['last_sync'] = datetime.utcnow().isoformat() + 'Z'
        _inventory['item_count'] = len(items)
        _inventory['source'] = 's3'

    print(f"  Parsed {len(items)} items across {len(brands)} brands")
    return True


def generate_all_exports():
    with _export_lock:
        if _exports['generating']:
            return
        _exports['generating'] = True
        _exports['progress'] = 'starting...'

    try:
        with _inv_lock:
            brands = dict(_inventory['brands'])

        if not brands:
            print("  No inventory data for export generation")
            return

        date_str = datetime.utcnow().strftime('%Y-%m-%d')
        total = len(brands)

        print(f"\n{'='*60}")
        print(f"  Generating exports for {total} brands...")
        print(f"  Image strategy: STYLE+OVERRIDES first → brand folder fallback")
        print(f"{'='*60}")

        # Pre-cache images for ALL items (deduplicates by base_style)
        all_items = []
        for abbr, brand in brands.items():
            all_items.extend(brand['items'])

        print(f"  Pre-caching images for {len(all_items)} items...")
        download_images_for_items(all_items, S3_PHOTOS_URL, use_cache=True)
        with _img_lock:
            cached_count = sum(1 for v in _img_cache.values() if v is not None)
            failed_count = sum(1 for v in _img_cache.values() if v is None)
        print(f"  Image cache: {cached_count} found, {failed_count} not found\n")

        brands_list_for_multi = []
        done = 0

        sorted_brands = sorted(brands.items(),
            key=lambda x: sum(i.get('total_warehouse', 0) for i in x[1]['items']),
            reverse=True)

        for abbr, brand in sorted_brands:
            done += 1
            name = brand['name']
            with _export_lock:
                _exports['progress'] = f"{done}/{total}: {name}"

            print(f"  [{done}/{total}] {name} ({len(brand['items'])} items)")

            sorted_items = sorted(brand['items'], key=lambda x: x.get('total_warehouse', 0), reverse=True)

            try:
                annotated_items = _annotate_items_for_prepack(sorted_items)
                with _prepack_defaults_lock:
                    pd_snap = list(_prepack_defaults)
                xl_bytes = build_brand_excel(name, annotated_items, S3_PHOTOS_URL,
                                             prepack_defaults=pd_snap)

                with _export_lock:
                    _exports['brands'][abbr] = {
                        'bytes': xl_bytes,
                        'generated_at': datetime.utcnow().isoformat() + 'Z',
                        'name': name,
                        'items_count': len(sorted_items),
                        'size_bytes': len(xl_bytes),
                    }

                s3_key = f"{S3_EXPORT_PREFIX}{name.replace(' ', '_')}_{date_str}.xlsx"
                s3_upload_export(s3_key, xl_bytes)

                brands_list_for_multi.append({
                    'brand_name': name,
                    'items': annotated_items
                })
            except Exception as e:
                print(f"    Failed: {e}")

        if brands_list_for_multi:
            print(f"\n  [ALL] Multi-tab ({len(brands_list_for_multi)} brands)...")
            try:
                with _prepack_defaults_lock:
                    pd_snap = list(_prepack_defaults)
                multi_bytes = build_multi_brand_excel(brands_list_for_multi, S3_PHOTOS_URL,
                                                      prepack_defaults=pd_snap)
                with _export_lock:
                    _exports['all_brands'] = {
                        'bytes': multi_bytes,
                        'generated_at': datetime.utcnow().isoformat() + 'Z',
                        'brands_count': len(brands_list_for_multi),
                        'items_count': sum(len(b['items']) for b in brands_list_for_multi),
                        'size_bytes': len(multi_bytes),
                    }
                s3_upload_export(f"{S3_EXPORT_PREFIX}All_Brands_{date_str}.xlsx", multi_bytes)
            except Exception as e:
                print(f"    Failed: {e}")

        with _export_lock:
            _exports['generating'] = False
            _exports['progress'] = 'done'
            _exports['last_generated'] = datetime.utcnow().isoformat() + 'Z'

        print(f"\n{'='*60}")
        print(f"  Export generation complete! {done} brands")
        with _img_lock:
            print(f"  Image cache: {len(_img_cache)} unique styles cached")
        print(f"{'='*60}\n")

    except Exception as e:
        print(f"  Export generation error: {e}")
        with _export_lock:
            _exports['generating'] = False
            _exports['progress'] = f'error: {e}'


def trigger_background_generation():
    t = threading.Thread(target=generate_all_exports, daemon=True)
    t.start()


# ============================================
# ROUTES
# ============================================

@app.route('/', methods=['GET'])
def home():
    return jsonify({
        "service": "Versa Inventory Export API v3",
        "status": "running",
    })


@app.route('/health', methods=['GET'])
def health():
    with _inv_lock:
        inv_count = _inventory['item_count']
        last_sync = _inventory['last_sync']
        source = _inventory['source']
    with _export_lock:
        gen = _exports['generating']
        brands_ready = len(_exports['brands'])
        progress = _exports['progress']
    with _img_lock:
        img_count = len(_img_cache)
        img_found = sum(1 for v in _img_cache.values() if v is not None)

    return jsonify({
        "status": "healthy",
        "inventory_items": inv_count,
        "inventory_source": source,
        "last_sync": last_sync,
        "exports_generating": gen,
        "exports_ready": brands_ready,
        "generation_progress": progress,
        "images_cached": img_count,
        "images_found": img_found,
        "dropbox_photos_indexed": len(_dropbox_photo_index),
        "dropbox_photos_cached": len([f for f in os.listdir(DROPBOX_DISK_CACHE) if f.endswith(('.jpg', '.png'))]) if os.path.exists(DROPBOX_DISK_CACHE) else 0,
        "dropbox_photos_last_sync": _dropbox_photos_last_sync,
        "production_rows": len(_production_data),
        "production_last_sync": _production_last_sync,
        "production_folder": DROPBOX_PRODUCTION_FOLDER,
    })


@app.route('/sync', methods=['GET', 'OPTIONS'])
def sync():
    if request.method == 'OPTIONS':
        return '', 204

    updated = sync_inventory()

    with _inv_lock:
        items = list(_inventory['items'])
        last_sync = _inventory['last_sync']
        brand_count = len(_inventory['brands'])

    with _export_lock:
        has_exports = bool(_exports['brands'])
        is_generating = _exports['generating']

    if (updated or not has_exports) and not is_generating:
        print("  Triggering background export generation...")
        trigger_background_generation()

    return jsonify({
        "status": "ok",
        "updated": updated,
        "last_sync": last_sync,
        "item_count": len(items),
        "brand_count": brand_count,
        "inventory": items,
    })


@app.route('/inventory', methods=['GET', 'OPTIONS'])
def inventory():
    if request.method == 'OPTIONS':
        return '', 204

    # ── STALE-WORKER GUARD ──────────────────────────────────────────────────
    # Render runs multiple gunicorn workers, each with independent in-memory
    # _inventory. The admin's /sync call only updates whichever worker handles
    # it. Customers hitting a different worker would see stale data indefinitely.
    # Fix: if this worker's data is older than INVENTORY_MAX_AGE_SECONDS, force
    # a sync right now before responding — guarantees customers always get fresh data.
    INVENTORY_MAX_AGE_SECONDS = 300  # 5 minutes
    needs_sync = False
    with _inv_lock:
        last_sync_str = _inventory.get('last_sync')
        has_items = bool(_inventory['items'])

    if not has_items:
        needs_sync = True
    elif last_sync_str:
        try:
            last_sync_dt = datetime.fromisoformat(last_sync_str.replace('Z', '+00:00'))
            age = (datetime.now(last_sync_dt.tzinfo) - last_sync_dt).total_seconds()
            if age > INVENTORY_MAX_AGE_SECONDS:
                needs_sync = True
        except Exception:
            needs_sync = True

    if needs_sync:
        try:
            sync_inventory()
        except Exception as e:
            print(f"  [/inventory] Stale-worker sync failed: {e}")

    with _inv_lock:
        return jsonify({
            "status": "ok",
            "last_sync": _inventory['last_sync'],
            "item_count": _inventory['item_count'],
            "inventory": list(_inventory['items']),
        })


@app.route('/exports', methods=['GET', 'OPTIONS'])
def exports_manifest():
    if request.method == 'OPTIONS':
        return '', 204

    with _export_lock:
        brands = {}
        for abbr, info in _exports['brands'].items():
            brands[abbr] = {
                'name': info['name'],
                'items_count': info['items_count'],
                'size_bytes': info['size_bytes'],
                'generated_at': info['generated_at'],
            }
        all_b = None
        if _exports['all_brands']:
            a = _exports['all_brands']
            all_b = {
                'brands_count': a['brands_count'],
                'items_count': a['items_count'],
                'size_bytes': a['size_bytes'],
                'generated_at': a['generated_at'],
            }

        return jsonify({
            "generating": _exports['generating'],
            "progress": _exports['progress'],
            "last_generated": _exports['last_generated'],
            "brands": brands,
            "all_brands": all_b,
        })


@app.route('/download/brand/<abbr>', methods=['GET'])
def download_brand(abbr):
    abbr = abbr.upper()
    with _export_lock:
        info = _exports['brands'].get(abbr)
    if not info:
        return jsonify({"error": f"No pre-generated export for '{abbr}'"}), 404

    date_str = datetime.utcnow().strftime('%Y-%m-%d')
    filename = f"{info['name'].replace(' ', '_')}_{date_str}.xlsx"

    return send_file(
        BytesIO(info['bytes']),
        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        as_attachment=True, download_name=filename
    )


@app.route('/download/all', methods=['GET'])
def download_all():
    with _export_lock:
        info = _exports['all_brands']
    if not info:
        return jsonify({"error": "No pre-generated all-brands export"}), 404

    date_str = datetime.utcnow().strftime('%Y-%m-%d')
    return send_file(
        BytesIO(info['bytes']),
        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        as_attachment=True, download_name=f"All_Brands_{date_str}.xlsx"
    )


@app.route('/download/multi', methods=['GET'])
def download_multi_selected():
    brands_param = request.args.get('brands', '')
    if not brands_param:
        return jsonify({"error": "Missing 'brands' param"}), 400

    abbrs = [b.strip().upper() for b in brands_param.split(',') if b.strip()]
    if not abbrs:
        return jsonify({"error": "No valid brands"}), 400

    with _inv_lock:
        all_brands = dict(_inventory['brands'])

    brands_list = []
    for abbr in abbrs:
        if abbr in all_brands:
            sorted_items = sorted(all_brands[abbr]['items'],
                key=lambda x: x.get('total_warehouse', 0), reverse=True)
            brands_list.append({
                'brand_name': all_brands[abbr]['name'],
                'items': sorted_items
            })

    if not brands_list:
        return jsonify({"error": "No matching brands in inventory"}), 404

    brands_list.sort(
        key=lambda b: sum(i.get('total_warehouse', 0) for i in b['items']),
        reverse=True)

    # Annotate items with category/fit for prepack chart matching
    for b in brands_list:
        b['items'] = _annotate_items_for_prepack(b['items'])
    with _prepack_defaults_lock:
        pd_snap = list(_prepack_defaults)

    xl_bytes = build_multi_brand_excel(brands_list, S3_PHOTOS_URL, prepack_defaults=pd_snap)
    date_str = datetime.utcnow().strftime('%Y-%m-%d')

    return send_file(
        BytesIO(xl_bytes),
        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        as_attachment=True,
        download_name=f"Versa_{len(brands_list)}_Brands_{date_str}.xlsx"
    )


@app.route('/upload', methods=['POST', 'OPTIONS'])
def upload_inventory():
    if request.method == 'OPTIONS':
        return '', 204

    if 'file' not in request.files:
        return jsonify({"error": "No file. Use multipart form with 'file' field."}), 400

    file = request.files['file']
    data = file.read()
    if not data:
        return jsonify({"error": "Empty file"}), 400

    try:
        s3 = get_s3()
        s3.put_object(Bucket=S3_BUCKET, Key=S3_INVENTORY_KEY, Body=data,
                       ContentType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
    except Exception as e:
        return jsonify({"error": f"S3 upload failed: {e}"}), 500

    sync_inventory()
    with _inv_lock:
        count = _inventory['item_count']

    trigger_background_generation()

    return jsonify({"status": "ok", "message": f"Uploaded and synced. {count} items.", "item_count": count})


@app.route('/export-overseas-summary', methods=['POST', 'OPTIONS'])
def export_overseas_summary():
    if request.method == 'OPTIONS':
        return '', 204
    try:
        req = request.get_json()
        if not req or 'data' not in req:
            return jsonify({"error": "Missing 'data'"}), 400
        data = req['data']
        s3_url = req.get('s3_base_url', S3_PHOTOS_URL)
        fname = req.get('filename', 'Overseas_Summary')
        if not data:
            return jsonify({"error": "Empty data"}), 400

        xl_bytes = build_overseas_summary_excel(fname, data, s3_url)
        ts = datetime.now().strftime('%Y-%m-%d')
        return send_file(BytesIO(xl_bytes),
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            as_attachment=True, download_name=f"{fname}_{ts}.xlsx")
    except Exception as e:
        import traceback
        return jsonify({"error": str(e), "trace": traceback.format_exc()}), 500


def build_overseas_summary_excel(title, items, s3_base_url):
    """Build a formatted overseas summary Excel with images, date banners, brand separators."""
    buf = BytesIO()
    wb = xlsxwriter.Workbook(buf, {'in_memory': True})
    wb.set_properties({'title': f'Versa - {title}', 'author': 'Versa Inventory System'})
    ws = wb.add_worksheet('Overseas Summary')

    # ── Formats ──
    font = STYLE_CONFIG['font_name']
    fmt_header = wb.add_format({
        'bold': True, 'font_name': font, 'font_size': 11,
        'bg_color': '#1E293B', 'font_color': '#FFFFFF',
        'border': 1, 'border_color': '#1E293B',
        'align': 'center', 'valign': 'vcenter'
    })
    base = {
        'font_name': font, 'font_size': 10,
        'text_wrap': True, 'valign': 'vcenter', 'align': 'center',
        'border': 1, 'border_color': '#D1D5DB'
    }
    fmt_odd = wb.add_format({**base, 'bg_color': '#FFFFFF'})
    fmt_even = wb.add_format({**base, 'bg_color': '#F8FAFC'})
    fmt_num_odd = wb.add_format({**base, 'bg_color': '#FFFFFF', 'num_format': '#,##0'})
    fmt_num_even = wb.add_format({**base, 'bg_color': '#F8FAFC', 'num_format': '#,##0'})
    fmt_neg_odd = wb.add_format({**base, 'bg_color': '#FFFFFF', 'num_format': '#,##0', 'font_color': '#DC2626'})
    fmt_neg_even = wb.add_format({**base, 'bg_color': '#F8FAFC', 'num_format': '#,##0', 'font_color': '#DC2626'})
    fmt_ats_odd = wb.add_format({**base, 'bg_color': '#FFFFFF', 'num_format': '#,##0', 'bold': True, 'font_size': 11})
    fmt_ats_even = wb.add_format({**base, 'bg_color': '#F8FAFC', 'num_format': '#,##0', 'bold': True, 'font_size': 11})
    fmt_date_banner = wb.add_format({
        'bold': True, 'font_name': font, 'font_size': 12,
        'bg_color': '#FEF3C7', 'font_color': '#92400E',
        'border': 1, 'border_color': '#F59E0B',
        'align': 'left', 'valign': 'vcenter'
    })
    fmt_brand_sep = wb.add_format({
        'bold': True, 'font_name': font, 'font_size': 10,
        'bg_color': '#F1F5F9', 'font_color': '#475569',
        'border': 1, 'border_color': '#E2E8F0',
        'align': 'left', 'valign': 'vcenter'
    })
    fmt_sku_odd = wb.add_format({**base, 'bg_color': '#FFFFFF', 'bold': True, 'align': 'left'})
    fmt_sku_even = wb.add_format({**base, 'bg_color': '#F8FAFC', 'bold': True, 'align': 'left'})

    # ── Headers ──
    headers = ['IMAGE', 'SKU', 'Brand', 'Color', 'Fit', 'Fabrication',
               'Production', 'PO', 'Ex-Factory', 'Arrival',
               'Produced', 'Deducted', 'Flow ATS']
    col_widths = [COL_WIDTH_UNITS, 22, 12, 20, 12, 32, 22, 22, 14, 14, 12, 12, 12]

    ws.hide_gridlines(2)
    ws.freeze_panes(1, 0)
    ws.set_row(0, 28)
    for c, h in enumerate(headers):
        ws.write(0, c, h, fmt_header)
        ws.set_column(c, c, col_widths[c])

    # ── Download images ──
    img_items = [{'sku': d.get('sku', ''), 'brand_abbr': d.get('brand_abbr', ''),
                  'brand': d.get('brand_abbr', '')} for d in items]
    imgs = download_images_for_items(img_items, s3_base_url, use_cache=True)

    # ── Write rows with date banners & brand separators ──
    row = 1
    data_row_idx = 0  # for alternating colors
    last_date_key = None
    last_brand = None
    num_cols = len(headers)

    for i, item in enumerate(items):
        date_key = item.get('arrival', '') or item.get('ex_factory', '') or 'No Date'

        # Date group banner
        if date_key != last_date_key:
            etd = item.get('ex_factory', '—') or '—'
            arr = item.get('arrival', '—') or '—'
            ws.set_row(row, 26)
            ws.merge_range(row, 0, row, num_cols - 1,
                           f"  📅  Ex-Factory: {etd}  →  Arrival: {arr}", fmt_date_banner)
            row += 1
            last_date_key = date_key
            last_brand = None
            data_row_idx = 0

        # Brand separator
        brand = item.get('brand_full', '') or item.get('brand_abbr', '')
        brand_key = item.get('brand_abbr', '')
        if brand_key != last_brand:
            ws.set_row(row, 20)
            ws.merge_range(row, 0, row, num_cols - 1,
                           f"  ▸  {brand}", fmt_brand_sep)
            row += 1
            last_brand = brand_key
            data_row_idx = 0

        # Data row
        even = data_row_idx % 2 == 1
        cf = fmt_even if even else fmt_odd
        nf = fmt_num_even if even else fmt_num_odd
        sf = fmt_sku_even if even else fmt_sku_odd
        deducted_fmt = fmt_neg_even if even else fmt_neg_odd
        ats_fmt = fmt_ats_even if even else fmt_ats_odd

        ws.set_row(row, 112.5)

        # Image
        img = imgs.get(i)
        if img:
            try:
                ws.insert_image(row, 0, "img.png", _padded_image_opts(img))
            except Exception:
                ws.write(row, 0, "Error", cf)
        else:
            ws.write(row, 0, "No Image", cf)

        ws.write(row, 1, item.get('sku', ''), sf)
        ws.write(row, 2, item.get('brand_abbr', ''), cf)
        ws.write(row, 3, item.get('color', ''), cf)
        ws.write(row, 4, item.get('fit', ''), cf)
        ws.write(row, 5, item.get('fabrication', ''), cf)
        ws.write(row, 6, item.get('production', ''), cf)
        ws.write(row, 7, item.get('po', ''), cf)
        ws.write(row, 8, item.get('ex_factory', ''), cf)
        ws.write(row, 9, item.get('arrival', ''), cf)
        ws.write(row, 10, item.get('produced', 0), nf)
        deducted = item.get('deducted', 0)
        ws.write(row, 11, deducted, deducted_fmt if deducted else nf)
        ws.write(row, 12, item.get('flow_ats', 0), ats_fmt)

        row += 1
        data_row_idx += 1

    wb.close()
    return buf.getvalue()


@app.route('/export', methods=['POST', 'OPTIONS'])
def export_single():
    if request.method == 'OPTIONS':
        return '', 204
    try:
        req = request.get_json()
        if not req or 'data' not in req:
            return jsonify({"error": "Missing 'data'"}), 400
        data = req['data']
        s3_url = req.get('s3_base_url', S3_PHOTOS_URL)
        fname = req.get('filename', 'Export')
        view_mode = req.get('view_mode', 'all')
        is_order = req.get('is_order', False)
        catalog_mode = req.get('catalog_mode', False)
        prepack_defaults = req.get('prepack_defaults') or _prepack_defaults or []
        if not data:
            return jsonify({"error": "Empty data"}), 400

        print(f"[Export] {fname}: {len(data)} items, view={view_mode}, prepack_rules={len(prepack_defaults)}")

        xl_bytes = build_brand_excel(fname, data, s3_url, view_mode=view_mode,
                                     is_order=is_order, catalog_mode=catalog_mode,
                                     prepack_defaults=prepack_defaults)
        ts = datetime.now().strftime('%Y-%m-%d')
        return send_file(BytesIO(xl_bytes),
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            as_attachment=True, download_name=f"{fname}_{ts}.xlsx")
    except Exception as e:
        import traceback
        traceback.print_exc()
        print(f"[Export] FAILED for {fname}: {e}")
        return jsonify({"error": str(e), "trace": traceback.format_exc()}), 500


@app.route('/export-pdf', methods=['POST', 'OPTIONS'])
def export_pdf():
    if request.method == 'OPTIONS':
        return '', 204
    if not HAS_REPORTLAB:
        return jsonify({"error": "reportlab not installed on server. Add 'reportlab' to requirements.txt and redeploy."}), 500
    try:
        req = request.get_json()
        if not req or 'data' not in req:
            return jsonify({"error": "Missing 'data'"}), 400
        data = req['data']
        if not data:
            return jsonify({"error": "Empty data"}), 400
        s3_url = req.get('s3_base_url', S3_PHOTOS_URL)
        title = req.get('title', 'Export')
        subtitle = req.get('subtitle', f"{len(data)} styles")
        show_qty = req.get('show_qty', False)
        fname = req.get('filename', 'Export')

        pdf_bytes = build_brand_pdf(title, data, s3_url,
                                    subtitle=subtitle, show_qty=show_qty)
        ts = datetime.now().strftime('%Y-%m-%d')
        return send_file(BytesIO(pdf_bytes),
            mimetype='application/pdf',
            as_attachment=True, download_name=f"{fname}_{ts}.pdf")
    except Exception as e:
        import traceback
        return jsonify({"error": str(e), "trace": traceback.format_exc()}), 500


@app.route('/export-multi', methods=['POST', 'OPTIONS'])
def export_multi():
    if request.method == 'OPTIONS':
        return '', 204
    try:
        req = request.get_json()
        if not req or 'brands' not in req:
            return jsonify({"error": "Missing 'brands'"}), 400
        brands_data = req['brands']
        s3_url = req.get('s3_base_url', S3_PHOTOS_URL)
        fname = req.get('filename', 'Multi_Brand')
        catalog_mode = req.get('catalog_mode', False)
        view_mode = req.get('view_mode', 'all')
        prepack_defaults = req.get('prepack_defaults') or _prepack_defaults or []
        if not brands_data:
            return jsonify({"error": "Empty brands"}), 400

        xl_bytes = build_multi_brand_excel(brands_data, s3_url,
                                           catalog_mode=catalog_mode, view_mode=view_mode,
                                           prepack_defaults=prepack_defaults)
        ts = datetime.now().strftime('%Y-%m-%d')
        return send_file(BytesIO(xl_bytes),
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            as_attachment=True, download_name=f"{fname}_{ts}.xlsx")
    except Exception as e:
        import traceback
        return jsonify({"error": str(e), "trace": traceback.format_exc()}), 500


# ── Style Overrides ────────────────────────────────
@app.route('/overrides', methods=['GET', 'OPTIONS'])
def get_overrides():
    if request.method == 'OPTIONS':
        return '', 204
    # ALWAYS reload from S3 to prevent stale-worker data loss.
    # This endpoint is only called on page load and when version polling detects a change,
    # so the extra S3 read is infrequent and worth the consistency guarantee.
    load_overrides_from_s3()
    with _overrides_lock:
        return jsonify({"overrides": _style_overrides})

@app.route('/overrides/version', methods=['GET', 'OPTIONS'])
def get_overrides_version():
    """Lightweight version endpoint for frontend polling — uses S3 ETag as cross-worker source of truth."""
    if request.method == 'OPTIONS':
        return '', 204
    try:
        s3 = get_s3()
        head = s3.head_object(Bucket=S3_BUCKET, Key=S3_OVERRIDES_KEY)
        s3_etag = head['ETag'].strip('"')

        # If S3 ETag differs from our local copy, another worker saved — reload
        with _overrides_lock:
            local_etag = _s3_overrides_etag
        if local_etag != s3_etag:
            print(f"  [overrides/version] ETag mismatch (local={local_etag}, s3={s3_etag[:8]}) — reloading from S3", flush=True)
            load_overrides_from_s3()

        with _overrides_lock:
            count = len(_style_overrides)
        return jsonify({"version": s3_etag, "count": count, "last_saved": _overrides_last_saved})
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            return jsonify({"version": "none", "count": 0, "last_saved": 0})
        # Fallback to local hash on S3 errors
        import hashlib
        with _overrides_lock:
            count = len(_style_overrides)
            keys_str = ','.join(sorted(_style_overrides.keys()))
        fingerprint = hashlib.md5(f"{keys_str}:{_overrides_last_saved}".encode()).hexdigest()[:12]
        return jsonify({"version": fingerprint, "count": count, "last_saved": _overrides_last_saved})
    except Exception:
        import hashlib
        with _overrides_lock:
            count = len(_style_overrides)
            keys_str = ','.join(sorted(_style_overrides.keys()))
        fingerprint = hashlib.md5(f"{keys_str}:{_overrides_last_saved}".encode()).hexdigest()[:12]
        return jsonify({"version": fingerprint, "count": count, "last_saved": _overrides_last_saved})

def _backup_overrides_to_s3(overrides_dict):
    """Write a timestamped backup copy of overrides to S3. Fire-and-forget."""
    try:
        s3 = get_s3()
        ts = datetime.utcnow().strftime('%Y-%m-%d_%H-%M-%S')
        backup_key = f"inventory/overrides_backups/style_overrides_{ts}.json"
        s3.put_object(
            Bucket=S3_BUCKET,
            Key=backup_key,
            Body=json.dumps(overrides_dict).encode('utf-8'),
            ContentType='application/json'
        )
        print(f"  ✓ Backup saved: {backup_key}")
    except Exception as e:
        print(f"  ⚠ Backup failed (non-fatal): {e}")


@app.route('/overrides', methods=['POST'])
def save_overrides():
    try:
        req = request.get_json()
        if not req or 'overrides' not in req:
            return jsonify({"error": "Missing 'overrides' in request body"}), 400

        overrides = req['overrides']
        if not isinstance(overrides, dict):
            return jsonify({"error": "'overrides' must be an object"}), 400

        global _style_overrides
        with _overrides_lock:
            current = dict(_style_overrides)

        # SAFETY: merge incoming into current — never allow a smaller payload to wipe existing data
        # Incoming keys add/update; keys not present in incoming are preserved from S3 state
        merged = dict(current)
        merged.update(overrides)

        # If incoming count is suspiciously low vs current, only allow it if explicitly a delete op
        incoming_count = len(overrides)
        current_count = len(current)
        if incoming_count < current_count and not req.get('replace_all', False):
            # Merge only — do not shrink
            print(f"  ⚠ Incoming overrides ({incoming_count}) < current ({current_count}), merging (not replacing)")

        # Find which styles have new/changed images (for CloudFront invalidation)
        changed_styles = [
            style for style, data in overrides.items()
            if isinstance(data, dict) and data.get('image') and
               data.get('image') != (current.get(style) or {}).get('image')
        ]

        with _overrides_lock:
            _style_overrides = merged
        global _overrides_last_saved
        _overrides_last_saved = time.time()

        # Async backup before saving canonical file
        threading.Thread(target=_backup_overrides_to_s3, args=(merged,), daemon=True).start()

        success = save_overrides_to_s3()

        if success:
            # Invalidate CloudFront cache for changed override images — instant update
            if changed_styles:
                paths = [f"/ALL+INVENTORY+Photos/STYLE+OVERRIDES/{s}.jpg" for s in changed_styles]
                paths += [f"/ALL+INVENTORY+Photos/STYLE+OVERRIDES/{s}.png" for s in changed_styles]
                threading.Thread(target=_invalidate_cloudfront, args=(paths,), daemon=True).start()
            return jsonify({"success": True, "count": len(merged), "invalidated": len(changed_styles)})
        else:
            return jsonify({"error": "Failed to save to S3"}), 500
    except Exception as e:
        import traceback
        return jsonify({"error": str(e), "trace": traceback.format_exc()}), 500


@app.route('/overrides/backups', methods=['GET', 'OPTIONS'])
def list_overrides_backups():
    """List available override backup files in S3, newest first."""
    if request.method == 'OPTIONS':
        return '', 204
    try:
        s3 = get_s3()
        resp = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix='inventory/overrides_backups/')
        items = resp.get('Contents', [])
        backups = sorted(
            [{'key': o['Key'], 'size': o['Size'], 'last_modified': o['LastModified'].isoformat()} for o in items],
            key=lambda x: x['last_modified'], reverse=True
        )
        return jsonify({'backups': backups, 'count': len(backups)})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/overrides/backups/<path:backup_key>', methods=['GET', 'OPTIONS'])
def restore_overrides_backup(backup_key):
    """Download a specific backup file contents."""
    if request.method == 'OPTIONS':
        return '', 204
    try:
        s3 = get_s3()
        full_key = f"inventory/overrides_backups/{backup_key}"
        obj = s3.get_object(Bucket=S3_BUCKET, Key=full_key)
        data = json.loads(obj['Body'].read().decode('utf-8'))
        return jsonify({'overrides': data, 'count': len(data), 'key': full_key})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/allocations', methods=['GET', 'OPTIONS'])
def get_allocations():
    if request.method == 'OPTIONS':
        return '', 204
    # Merge S3 sheet entries with manual entries
    s3_data = load_allocation_from_s3()
    for row in s3_data:
        row['source'] = 's3'
    with _manual_alloc_lock:
        manual = list(_manual_allocations)
    for row in manual:
        row_copy = dict(row)
        row_copy['source'] = 'manual'
    merged = s3_data + [dict(r, source='manual') for r in manual]
    return jsonify({"allocations": merged})


@app.route('/allocations/version', methods=['GET', 'OPTIONS'])
def get_allocations_version():
    """Returns a lightweight version hash of manual allocations — for live sync polling"""
    if request.method == 'OPTIONS':
        return '', 204
    import hashlib
    # Read from S3 for multi-worker consistency
    try:
        s3 = get_s3()
        resp = s3.get_object(Bucket=S3_BUCKET, Key=S3_MANUAL_ALLOC_KEY)
        data = json.loads(resp['Body'].read().decode('utf-8'))
        allocs = data if isinstance(data, list) else []
        # Also update in-memory
        global _manual_allocations
        with _manual_alloc_lock:
            _manual_allocations = allocs
    except Exception:
        with _manual_alloc_lock:
            allocs = list(_manual_allocations)
    sorted_allocs = sorted(allocs, key=lambda x: x.get('id', ''))
    version = hashlib.md5(json.dumps(sorted_allocs, sort_keys=True).encode()).hexdigest()[:12]
    return jsonify({"version": version, "count": len(allocs)})


@app.route('/deduction-assignments', methods=['GET', 'OPTIONS'])
def get_deduction_assignments():
    if request.method == 'OPTIONS':
        return '', 204
    # Always reload from S3 for multi-worker consistency
    load_deduction_assignments_from_s3()
    with _deduction_assign_lock:
        data = dict(_deduction_assignments)
    return jsonify({"assignments": data})


@app.route('/deduction-assignments', methods=['POST'])
def save_deduction_assignments():
    try:
        req = request.get_json()
        if not req or 'assignments' not in req:
            return jsonify({"error": "Missing 'assignments'"}), 400
        assignments = req['assignments']
        if not isinstance(assignments, dict):
            return jsonify({"error": "'assignments' must be an object"}), 400
        global _deduction_assignments
        with _deduction_assign_lock:
            _deduction_assignments = assignments
        success = save_deduction_assignments_to_s3()
        if success:
            return jsonify({"success": True, "count": len(assignments)})
        return jsonify({"error": "Failed to save to S3"}), 500
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/prepack-defaults', methods=['GET', 'OPTIONS'])
def get_prepack_defaults():
    if request.method == 'OPTIONS':
        return '', 204
    # Always reload from S3 for multi-worker consistency
    load_prepack_defaults_from_s3()
    with _prepack_defaults_lock:
        return jsonify({"defaults": list(_prepack_defaults)})


@app.route('/prepack-defaults', methods=['POST'])
def save_prepack_defaults_route():
    try:
        global _prepack_defaults
        req = request.get_json()
        if not req or 'defaults' not in req:
            return jsonify({"error": "Missing 'defaults' in request body"}), 400
        defaults = req['defaults']
        if not isinstance(defaults, list):
            return jsonify({"error": "'defaults' must be an array"}), 400
        # SAFETY: refuse to save empty array if we already have rules
        # (prevents accidental wipe from failed client load)
        with _prepack_defaults_lock:
            current_count = len(_prepack_defaults)
        if len(defaults) == 0 and current_count > 0:
            print(f"  ⚠ BLOCKED: attempted to save 0 prepack defaults (server has {current_count})")
            return jsonify({"error": f"Refusing to delete all {current_count} rules. Use the UI to delete rules individually."}), 400
        with _prepack_defaults_lock:
            _prepack_defaults = defaults
        success = save_prepack_defaults_to_s3()
        if success:
            return jsonify({"ok": True, "count": len(defaults)})
        return jsonify({"error": "Failed to save to S3"}), 500
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/suppression-overrides', methods=['GET', 'OPTIONS'])
def get_suppression_overrides():
    if request.method == 'OPTIONS':
        return '', 204
    load_suppression_overrides_from_s3()
    with _suppression_overrides_lock:
        return jsonify({"overrides": list(_suppression_overrides)})


@app.route('/suppression-overrides', methods=['POST'])
def save_suppression_overrides_route():
    try:
        req = request.get_json()
        if not req or 'overrides' not in req:
            return jsonify({"error": "Missing 'overrides'"}), 400
        overrides = req['overrides']
        if not isinstance(overrides, list):
            return jsonify({"error": "'overrides' must be an array"}), 400
        global _suppression_overrides
        with _suppression_overrides_lock:
            _suppression_overrides = overrides
        success = save_suppression_overrides_to_s3()
        if success:
            return jsonify({"ok": True, "count": len(overrides)})
        return jsonify({"error": "Failed to save to S3"}), 500
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/banner-rules', methods=['GET', 'OPTIONS'])
def get_banner_rules():
    if request.method == 'OPTIONS':
        return '', 204
    load_banner_rules_from_s3()
    with _banner_rules_lock:
        return jsonify({"rules": list(_banner_rules)})


@app.route('/banner-rules', methods=['POST'])
def save_banner_rules_route():
    try:
        global _banner_rules
        req = request.get_json()
        if not req or 'rules' not in req:
            return jsonify({"error": "Missing 'rules'"}), 400
        rules = req['rules']
        if not isinstance(rules, list):
            return jsonify({"error": "'rules' must be an array"}), 400
        with _banner_rules_lock:
            _banner_rules = rules
        success = save_banner_rules_to_s3()
        if success:
            return jsonify({"ok": True, "count": len(rules)})
        return jsonify({"error": "Failed to save to S3"}), 500
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/manual-allocations', methods=['GET', 'OPTIONS'])
def get_manual_allocations():
    if request.method == 'OPTIONS':
        return '', 204
    # Always read from S3 for multi-worker consistency
    # (POST saves to S3, but other workers keep stale in-memory copies)
    try:
        s3 = get_s3()
        resp = s3.get_object(Bucket=S3_BUCKET, Key=S3_MANUAL_ALLOC_KEY)
        data = json.loads(resp['Body'].read().decode('utf-8'))
        fresh = data if isinstance(data, list) else []
        # Update in-memory copy so this worker stays current
        global _manual_allocations
        with _manual_alloc_lock:
            _manual_allocations = fresh
        return jsonify({"allocations": fresh})
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            return jsonify({"allocations": []})
        # Fallback to in-memory if S3 read fails
        with _manual_alloc_lock:
            data = list(_manual_allocations)
        return jsonify({"allocations": data})
    except Exception:
        with _manual_alloc_lock:
            data = list(_manual_allocations)
        return jsonify({"allocations": data})


@app.route('/manual-allocations', methods=['POST'])
def save_manual_allocations():
    try:
        req = request.get_json()
        if not req or 'allocations' not in req:
            return jsonify({"error": "Missing 'allocations' in request body"}), 400
        entries = req['allocations']
        if not isinstance(entries, list):
            return jsonify({"error": "'allocations' must be a list"}), 400
        global _manual_allocations
        with _manual_alloc_lock:
            _manual_allocations = entries
        success = save_manual_allocations_to_s3()
        if success:
            return jsonify({"success": True, "count": len(entries)})
        else:
            return jsonify({"error": "Failed to save to S3"}), 500
    except Exception as e:
        import traceback
        return jsonify({"error": str(e), "trace": traceback.format_exc()}), 500


@app.route('/allocations/delete-entries', methods=['POST', 'OPTIONS'])
def delete_allocation_entries():
    """Delete specific entries from S3 CSV and/or manual allocations.
    Accepts: { entries: [{sku, po, customer, qty, source}], delete_po: "PO#123" }
    - If delete_po is set, removes ALL entries (S3+manual) matching that PO.
    - If entries is set, removes each matching entry individually.
    """
    if request.method == 'OPTIONS':
        return '', 204
    try:
        req = request.get_json()
        if not req:
            return jsonify({"error": "Missing request body"}), 400

        delete_po = req.get('delete_po', '').strip()
        entries_to_delete = req.get('entries', [])
        deleted_s3 = 0
        deleted_manual = 0

        # ── Load current CSV from S3 ──
        s3 = get_s3()
        try:
            resp = s3.get_object(Bucket=S3_BUCKET, Key=S3_ALLOCATION_KEY)
            csv_text = resp['Body'].read().decode('utf-8-sig')
            csv_lines = csv_text.strip().split('\n')
        except ClientError:
            csv_lines = []

        if len(csv_lines) >= 2:
            header_line = csv_lines[0]
            headers = [h.strip() for h in header_line.split(',')]
            po_idx = next((i for i, h in enumerate(headers) if 'po' in h.lower()), 0)
            cust_idx = next((i for i, h in enumerate(headers) if 'customer' in h.lower()), 1)
            sku_idx = next((i for i, h in enumerate(headers) if 'sku' in h.lower()), 2)
            qty_idx = next((i for i, h in enumerate(headers) if 'qty' in h.lower()), 3)

            original_count = len(csv_lines) - 1  # minus header

            if delete_po:
                # Remove ALL rows matching this PO
                kept_lines = [header_line]
                for line in csv_lines[1:]:
                    cols = [c.strip() for c in line.split(',')]
                    row_po = cols[po_idx] if len(cols) > po_idx else ''
                    if row_po.strip().upper() == delete_po.upper():
                        deleted_s3 += 1
                    else:
                        kept_lines.append(line)
                csv_lines = kept_lines
            elif entries_to_delete:
                # Build set of entries to delete (sku+po+qty)
                delete_set = set()
                for e in entries_to_delete:
                    if e.get('source') == 's3':
                        key = (e.get('sku', '').upper(), e.get('po', '').strip(), str(e.get('qty', 0)))
                        delete_set.add(key)
                if delete_set:
                    kept_lines = [header_line]
                    for line in csv_lines[1:]:
                        cols = [c.strip() for c in line.split(',')]
                        sku = cols[sku_idx].upper() if len(cols) > sku_idx else ''
                        po = cols[po_idx] if len(cols) > po_idx else ''
                        qty = cols[qty_idx] if len(cols) > qty_idx else '0'
                        key = (sku, po, qty)
                        if key in delete_set:
                            deleted_s3 += 1
                            delete_set.discard(key)  # only remove first match
                        else:
                            kept_lines.append(line)
                    csv_lines = kept_lines

            # Write updated CSV back to S3
            if deleted_s3 > 0:
                new_csv = '\n'.join(csv_lines)
                s3.put_object(
                    Bucket=S3_BUCKET,
                    Key=S3_ALLOCATION_KEY,
                    Body=new_csv.encode('utf-8'),
                    ContentType='text/csv'
                )
                print(f"  ✓ Deleted {deleted_s3} S3 CSV entries, {len(csv_lines) - 1} remaining")

        # ── Also handle manual allocation deletions ──
        global _manual_allocations
        if delete_po:
            with _manual_alloc_lock:
                before = len(_manual_allocations)
                _manual_allocations = [r for r in _manual_allocations
                                       if (r.get('po', '').strip().upper() != delete_po.upper())]
                deleted_manual = before - len(_manual_allocations)
            if deleted_manual > 0:
                save_manual_allocations_to_s3()
        elif entries_to_delete:
            manual_ids = [e.get('id') for e in entries_to_delete if e.get('source') == 'manual' and e.get('id')]
            if manual_ids:
                id_set = set(manual_ids)
                with _manual_alloc_lock:
                    before = len(_manual_allocations)
                    _manual_allocations = [r for r in _manual_allocations if r.get('id') not in id_set]
                    deleted_manual = before - len(_manual_allocations)
                if deleted_manual > 0:
                    save_manual_allocations_to_s3()

        return jsonify({
            "success": True,
            "deleted_s3": deleted_s3,
            "deleted_manual": deleted_manual,
            "total_deleted": deleted_s3 + deleted_manual
        })
    except Exception as e:
        import traceback
        return jsonify({"error": str(e), "trace": traceback.format_exc()}), 500


@app.route('/production', methods=['GET', 'OPTIONS'])
def get_production():
    if request.method == 'OPTIONS':
        return '', 204
    data = load_production_from_dropbox()
    return jsonify({"production": data})


# ============================================
# APO ALLOCATION — Dropbox CSV (hourly sync)
# Path: /EDI Team/Nuri/Python Macros/APO.csv
# Columns: B=Customer, E=Style#
# ============================================

def load_apo_from_dropbox():
    """Download APO.csv from Dropbox, parse customer (col B) and style (col E),
    cache in memory. Returns list of {customer, style, qty} dicts."""
    global _apo_data, _apo_last_sync

    token = get_dropbox_token()
    if not token:
        print("  ⚠ APO sync: no Dropbox token available")
        return False

    ns_id = os.environ.get('DROPBOX_APO_NAMESPACE_ID', '')
    print(f"  📋 Fetching APO allocation file from Dropbox: {DROPBOX_APO_PATH} (namespace: {ns_id or 'none'})", flush=True)
    try:
        headers = {
            'Authorization': f'Bearer {token}',
            'Dropbox-API-Arg': json.dumps({'path': DROPBOX_APO_PATH})
        }
        if ns_id:
            headers['Dropbox-API-Path-Root'] = json.dumps({'.tag': 'namespace_id', 'namespace_id': ns_id})
        resp = http_requests.post(
            'https://content.dropboxapi.com/2/files/download',
            headers=headers, timeout=30
        )

        if resp.status_code == 401:
            print("  ⚠ APO sync: token expired, refreshing...")
            global _dropbox_token_expires
            _dropbox_token_expires = 0
            token = get_dropbox_token()
            if not token:
                return False
            headers['Authorization'] = f'Bearer {token}'
            resp = http_requests.post(
                'https://content.dropboxapi.com/2/files/download',
                headers=headers, timeout=30
            )

        if resp.status_code != 200:
            print(f"  ⚠ APO sync: HTTP {resp.status_code}: {resp.text[:200]}")
            return False

        text = resp.content.decode('utf-8-sig', errors='replace')
        import csv as _csv, io as _io
        reader = list(_csv.reader(_io.StringIO(text)))
        rows = [r for r in reader if any(c.strip() for c in r)]
        if len(rows) < 2:
            print(f"  ⚠ APO sync: file has fewer than 2 lines (got {len(rows)})")
            return False

        # Log raw headers and first 3 data rows so we can verify structure
        raw_headers = [h.strip() for h in rows[0]]
        print(f"  📋 APO headers ({len(raw_headers)} cols): {raw_headers[:10]}")
        for i, row in enumerate(rows[1:4]):
            print(f"  📋 APO row {i+1}: {row[:11]}")

        headers_lower = [h.lower() for h in raw_headers]

        # Find columns by header name, fall back to fixed positions (B=1, E=4, 0-indexed)
        def find_col(names, fallback):
            for n in names:
                for i, h in enumerate(headers_lower):
                    if n in h:
                        return i
            return fallback

        cust_idx  = find_col(['customer', 'cust', 'account'], 1)   # col B
        style_idx = find_col(['style', 'sku', 'item #', 'item#', 'style #', 'style#', 'part'], 4)  # col E
        qty_idx   = find_col(['qty', 'quantity', 'units', 'allocated', 'open qty', 'openqty'], -1)
        po_idx    = find_col(['po', 'order', 'po #', 'po#', 'order #'], -1)

        print(f"  📋 APO cols → cust:{cust_idx} style:{style_idx} qty:{qty_idx} po:{po_idx}")

        results = []
        for cols in rows[1:]:
            if len(cols) <= max(cust_idx, style_idx):
                continue
            customer = cols[cust_idx] if len(cols) > cust_idx else ''
            style_raw = cols[style_idx] if len(cols) > style_idx else ''
            # Strip size suffix (e.g. "ASU201SLS" or "TJNASU201SLS-2XL" → keep base)
            style = style_raw.upper().split('-')[0].strip()
            if not style:
                continue
            qty = 0
            if qty_idx >= 0 and len(cols) > qty_idx:
                try:
                    qty = int(float(cols[qty_idx].replace(',', '') or 0))
                except (ValueError, TypeError):
                    qty = 0
            po = cols[po_idx].strip() if po_idx >= 0 and len(cols) > po_idx else ''
            results.append({
                'customer': customer,
                'style': style,
                'qty': qty,
                'po': po
            })

        print(f"  📋 APO sample styles: {list(set(r['style'] for r in results[:10]))}")

        with _apo_lock:
            _apo_data = results
            _apo_last_sync = time.time()

        print(f"  ✓ APO sync: {len(results)} allocation rows loaded", flush=True)
        return True

    except Exception as e:
        print(f"  ⚠ APO sync failed: {type(e).__name__}: {e}")
        return False


@app.route('/apo/explore', methods=['GET', 'OPTIONS'])
def get_apo_explore():
    """Drill into Dropbox folder structure to find APO.csv exact path."""
    if request.method == 'OPTIONS':
        return '', 204
    token = get_dropbox_token()
    if not token:
        return jsonify({'error': 'No Dropbox token'}), 500

    def list_folder(path):
        try:
            r = http_requests.post(
                'https://api.dropboxapi.com/2/files/list_folder',
                headers={'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'},
                json={'path': path, 'recursive': False},
                timeout=15
            )
            if r.status_code == 200:
                return [{'name': e['name'], 'type': e['.tag']} for e in r.json().get('entries', [])]
            return {'error': r.status_code, 'body': r.text[:200]}
        except Exception as e:
            return {'error': str(e)}

    results = {
        'configured_path': DROPBOX_APO_PATH,
        'edi_team_root': list_folder('/EDI Team'),
    }

    # Drill into Nuri if it exists
    edi_entries = results['edi_team_root']
    if isinstance(edi_entries, list):
        nuri_match = next((e for e in edi_entries if 'nuri' in e['name'].lower()), None)
        if nuri_match:
            nuri_path = f"/EDI Team/{nuri_match['name']}"
            results['nuri_folder_name'] = nuri_match['name']
            results['nuri_contents'] = list_folder(nuri_path)

            # Drill into Python Macros if it exists
            nuri_entries = results['nuri_contents']
            if isinstance(nuri_entries, list):
                macro_match = next((e for e in nuri_entries if 'macro' in e['name'].lower() or 'python' in e['name'].lower()), None)
                if macro_match:
                    macro_path = f"{nuri_path}/{macro_match['name']}"
                    results['macros_folder_name'] = macro_match['name']
                    results['macros_contents'] = list_folder(macro_path)

    return jsonify(results)


@app.route('/apo/debug', methods=['GET', 'OPTIONS'])
def get_apo_debug():
    """Returns raw first rows of APO file for debugging column structure."""
    if request.method == 'OPTIONS':
        return '', 204
    token = get_dropbox_token()
    if not token:
        return jsonify({'error': 'No Dropbox token'}), 500
    try:
        # Try with namespace header if APO_NAMESPACE_ID env var is set
        ns_id = os.environ.get('DROPBOX_APO_NAMESPACE_ID', '')
        api_arg = {'path': DROPBOX_APO_PATH}
        req_headers = {
            'Authorization': f'Bearer {token}',
            'Dropbox-API-Arg': json.dumps(api_arg)
        }
        if ns_id:
            req_headers['Dropbox-API-Path-Root'] = json.dumps({'.tag': 'namespace_id', 'namespace_id': ns_id})

        resp = http_requests.post(
            'https://content.dropboxapi.com/2/files/download',
            headers=req_headers, timeout=30
        )
        if resp.status_code != 200:
            return jsonify({'error': f'HTTP {resp.status_code}', 'body': resp.text[:300], 'namespace_used': ns_id or 'none'}), 500
        text = resp.content.decode('utf-8-sig', errors='replace')
        lines = [l for l in text.strip().splitlines() if l.strip()]
        return jsonify({
            'path': DROPBOX_APO_PATH,
            'namespace_used': ns_id or 'none',
            'total_lines': len(lines),
            'headers': lines[0] if lines else '',
            'headers_split': [h.strip() for h in lines[0].split(',')] if lines else [],
            'sample_rows': [l for l in lines[1:6]]
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/apo', methods=['GET', 'OPTIONS'])
def get_apo():
    if request.method == 'OPTIONS':
        return '', 204
    with _apo_lock:
        data = list(_apo_data)
    last_sync = _apo_last_sync
    age_minutes = int((time.time() - last_sync) / 60) if last_sync else None
    return jsonify({
        'apo': data,
        'count': len(data),
        'last_sync': datetime.utcfromtimestamp(last_sync).isoformat() + 'Z' if last_sync else None,
        'age_minutes': age_minutes
    })


@app.route('/regenerate', methods=['POST', 'OPTIONS'])
def regenerate_exports():
    """Force re-sync inventory and regenerate all exports"""
    if request.method == 'OPTIONS':
        return '', 204

    with _export_lock:
        if _exports['generating']:
            return jsonify({"status": "already_generating", "progress": _exports['progress']})

    # Clear image cache to force re-download
    with _img_lock:
        _img_cache.clear()
    with _web_img_lock:
        _web_img_cache.clear()
    _dropbox_thumb_cache.clear()
    _dropbox_img_cache.clear()

    updated = sync_inventory()
    trigger_background_generation()

    with _inv_lock:
        count = _inventory['item_count']
        source = _inventory['source']

    return jsonify({
        "status": "regenerating",
        "inventory_source": source,
        "item_count": count,
        "message": f"Re-synced {count} items from {source}, regenerating exports..."
    })


###############################################################################
# IMAGE PROXY — Serve product images through the API
# Solves S3 browser-access issues (CORS, bucket policies, etc.)
# Serves original-resolution images (not resized like Excel thumbnails)
###############################################################################

_web_img_cache = {}   # base_style → (content_bytes, content_type)
_web_img_lock = threading.Lock()


def _fetch_raw_image(base_style, brand_abbr):
    """Download raw image bytes: base64 override → S3 override → Dropbox → S3 brand folder"""
    headers = {'User-Agent': 'Mozilla/5.0'}
    image_code = extract_image_code(base_style, brand_abbr)

    # 0. Platform base64 override (highest priority)
    override_data = _style_overrides.get(base_style)
    if override_data and isinstance(override_data, dict) and override_data.get('image'):
        try:
            import base64
            img_str = override_data['image']
            if ',' in img_str:
                img_str = img_str.split(',', 1)[1]
            raw = base64.b64decode(img_str)
            # Detect content type
            ct = 'image/png' if raw[:4] == b'\x89PNG' else 'image/jpeg'
            return raw, ct
        except Exception:
            pass

    # 1. Try STYLE+OVERRIDES via CloudFront (fast CDN)
    override_base = f"{CLOUDFRONT_OVERRIDES_URL}/{base_style}"
    for ext in ['.jpg', '.png', '.jpeg']:
        try:
            url = override_base + ext
            resp = http_requests.get(url, headers=headers, timeout=10)
            if resp.status_code == 200:
                ct = resp.headers.get('Content-Type', '').lower()
                if 'image' in ct:
                    return resp.content, ct
        except Exception:
            continue

    # 2. Try CloudFront DROPBOX_SYNC (fastest — edge cached)
    if image_code.upper() in _dropbox_photo_index:
        sync_base = f"{CLOUDFRONT_DROPBOX_SYNC_URL}/{image_code}"
        for ext in ['.jpg', '.png']:
            try:
                url = sync_base + ext
                resp = http_requests.get(url, headers=headers, timeout=10)
                if resp.status_code == 200:
                    ct = resp.headers.get('Content-Type', '').lower()
                    if 'image' in ct:
                        return resp.content, ct
            except Exception:
                continue

    # 3. Try Dropbox photos (disk cache → API download)
    dbx_bytes, dbx_ct = get_dropbox_image_bytes(image_code)
    if dbx_bytes:
        return dbx_bytes, dbx_ct

    # 4. Fallback to S3 brand folder
    folder_name = FOLDER_MAPPING.get(brand_abbr, brand_abbr)
    brand_base = f"{CLOUDFRONT_PHOTOS_URL}/{folder_name}/{image_code}"
    for ext in ['.jpg', '.png', '.jpeg']:
        try:
            url = brand_base + ext
            resp = http_requests.get(url, headers=headers, timeout=10)
            if resp.status_code == 200:
                ct = resp.headers.get('Content-Type', '').lower()
                if 'image' in ct:
                    return resp.content, ct
        except Exception:
            continue

    return None, None


@app.route('/image/<base_style>', methods=['GET', 'OPTIONS'])
def proxy_image(base_style):
    """Serve a product image by base style code, with server-side S3 caching."""
    if request.method == 'OPTIONS':
        return '', 204

    base_style = base_style.upper().split('.')[0]  # strip extension if present

    # Check web image cache first
    with _web_img_lock:
        cached = _web_img_cache.get(base_style, 'MISS')
    if cached is None:
        return '', 404  # Previously failed — skip
    if cached != 'MISS':
        resp = make_response(cached[0])
        resp.headers['Content-Type'] = cached[1]
        resp.headers['Cache-Control'] = 'public, max-age=86400'
        resp.headers['Access-Control-Allow-Origin'] = '*'
        return resp

    # Get brand from query param or look up from inventory
    brand_abbr = request.args.get('brand', '').upper()
    if not brand_abbr:
        with _inv_lock:
            for item in (_inventory.get('items') or []):
                if item.get('sku', '').split('-')[0].upper() == base_style:
                    brand_abbr = item.get('brand_abbr', item.get('brand', ''))
                    break

    # Fetch raw image from S3
    raw_bytes, content_type = _fetch_raw_image(base_style, brand_abbr)

    if raw_bytes:
        # Cache for future requests (limit cache to ~500 images to control memory)
        with _web_img_lock:
            if len(_web_img_cache) > 200:
                # Evict oldest ~50 entries
                keys = list(_web_img_cache.keys())[:50]
                for k in keys:
                    del _web_img_cache[k]
            _web_img_cache[base_style] = (raw_bytes, content_type)

        resp = make_response(raw_bytes)
        resp.headers['Content-Type'] = content_type
        resp.headers['Cache-Control'] = 'public, max-age=86400'
        resp.headers['Access-Control-Allow-Origin'] = '*'
        return resp

    # Cache the miss too (avoid re-fetching failures)
    with _web_img_lock:
        _web_img_cache[base_style] = None

    return '', 404


@app.route('/dropbox-photos', methods=['GET', 'OPTIONS'])
def dropbox_photo_list():
    """Return list of available Dropbox image codes for frontend."""
    if request.method == 'OPTIONS':
        return '', 204
    return jsonify({
        'codes': list(_dropbox_photo_index.keys()),
        'count': len(_dropbox_photo_index),
        'last_sync': _dropbox_photos_last_sync,
        's3_sync_url': S3_DROPBOX_SYNC_URL
    })


@app.route('/dropbox-photos/sync', methods=['POST', 'OPTIONS'])
def trigger_dropbox_photo_sync():
    """Manually trigger Dropbox photo sync + pre-warm."""
    if request.method == 'OPTIONS':
        return '', 204
    def _sync_and_warm():
        sync_dropbox_photos()
        prewarm_dropbox_cache()
    threading.Thread(target=_sync_and_warm, daemon=True).start()
    return jsonify({'status': 'sync_started', 'current_count': len(_dropbox_photo_index)})




# ============================================
# SAVED CATALOGS — S3 persistence
# ============================================

def _load_saved_catalogs():
    """Load saved catalogs from S3."""
    try:
        s3 = get_s3()
        resp = s3.get_object(Bucket=S3_BUCKET, Key=S3_SAVED_CATALOGS_KEY)
        return json.loads(resp['Body'].read().decode('utf-8'))
    except Exception as e:
        err_str = str(e)
        if 'NoSuchKey' in err_str or '404' in err_str or 'does not exist' in err_str.lower():
            return []
        print(f"[Saved Catalogs] Load error: {e}", flush=True)
        return []

def _save_saved_catalogs(catalogs):
    """Save catalogs list to S3 with timestamped backup."""
    try:
        s3 = get_s3()
        body = json.dumps(catalogs, indent=2)
        s3.put_object(
            Bucket=S3_BUCKET,
            Key=S3_SAVED_CATALOGS_KEY,
            Body=body,
            ContentType='application/json'
        )
        try:
            ts = datetime.utcnow().strftime('%Y-%m-%d_%H-%M-%S')
            s3.put_object(
                Bucket=S3_BUCKET,
                Key=f"inventory/catalogs_backups/saved_catalogs_{ts}.json",
                Body=body,
                ContentType='application/json'
            )
        except Exception as be:
            print(f"[Saved Catalogs] Backup failed (non-fatal): {be}", flush=True)
        return True
    except Exception as e:
        print(f"[Saved Catalogs] Save to S3 error: {e}", flush=True)
        return False

def _cors_json(data, status=200):
    """Return JSON response with explicit CORS headers."""
    resp = make_response(jsonify(data), status)
    resp.headers['Access-Control-Allow-Origin'] = '*'
    resp.headers['Access-Control-Allow-Methods'] = 'GET, POST, DELETE, OPTIONS'
    resp.headers['Access-Control-Allow-Headers'] = 'Content-Type'
    return resp

def _invalidate_cloudfront(paths):
    """Invalidate specific CloudFront paths so updates are instant.
    paths: list of strings like ['/ALL+INVENTORY+Photos/STYLE+OVERRIDES/NA5001.jpg']
    First 1000 invalidation paths/month are free."""
    try:
        distribution_id = os.environ.get('CLOUDFRONT_DISTRIBUTION_ID', '')
        if not distribution_id:
            return  # Silently skip if not configured
        cf = boto3.client('cloudfront',
            aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID', ''),
            aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY', ''),
            region_name='us-east-1'  # CloudFront is always us-east-1
        )
        cf.create_invalidation(
            DistributionId=distribution_id,
            InvalidationBatch={
                'Paths': {'Quantity': len(paths), 'Items': paths},
                'CallerReference': str(int(time.time()))
            }
        )
        print(f"[CloudFront] Invalidated {len(paths)} path(s): {paths}", flush=True)
    except Exception as e:
        print(f"[CloudFront] Invalidation failed (non-fatal): {e}", flush=True)

@app.route('/saved-catalogs', methods=['GET', 'POST', 'OPTIONS'])
def handle_saved_catalogs():
    if request.method == 'OPTIONS':
        resp = make_response('', 204)
        resp.headers['Access-Control-Allow-Origin'] = '*'
        resp.headers['Access-Control-Allow-Methods'] = 'GET, POST, DELETE, OPTIONS'
        resp.headers['Access-Control-Allow-Headers'] = 'Content-Type'
        return resp

    try:
        if request.method == 'GET':
            return _cors_json(_load_saved_catalogs())
        
        # POST
        data = request.get_json(force=True, silent=True)
        if not data or not data.get('name') or not data.get('url'):
            return _cors_json({'error': 'name and url required'}, 400)

        catalogs = _load_saved_catalogs()

        existing_idx = next((i for i, c in enumerate(catalogs) if c['name'].lower() == data['name'].lower()), -1)

        # Preserve existing slug if updating, otherwise use the one sent from frontend
        existing_slug = catalogs[existing_idx].get('slug', '') if existing_idx >= 0 else ''
        slug = existing_slug or data.get('slug', '')
        
        # Preserve existing folder if not explicitly provided
        existing_folder = catalogs[existing_idx].get('folder', '') if existing_idx >= 0 else ''
        folder = data.get('folder') if 'folder' in data else existing_folder

        entry = {
            'name': data['name'],
            'url': data['url'],
            'brands': data.get('brands', []),
            'slug': slug,
            'folder': folder,
            'savedAt': int(time.time() * 1000)
        }

        if existing_idx >= 0:
            catalogs[existing_idx] = entry
            action = 'updated'
        else:
            catalogs.insert(0, entry)
            action = 'created'

        if _save_saved_catalogs(catalogs):
            return _cors_json({'status': action, 'catalogs': catalogs})
        else:
            return _cors_json({'error': 'Failed to save to S3'}, 500)
    except Exception as e:
        print(f"[Saved Catalogs] Endpoint error: {e}", flush=True)
        import traceback; traceback.print_exc()
        return _cors_json({'error': str(e)}, 500)

@app.route('/saved-catalogs/reorder', methods=['POST', 'OPTIONS'])
def reorder_saved_catalogs():
    if request.method == 'OPTIONS':
        resp = make_response('', 204)
        resp.headers['Access-Control-Allow-Origin'] = '*'
        resp.headers['Access-Control-Allow-Methods'] = 'POST, OPTIONS'
        resp.headers['Access-Control-Allow-Headers'] = 'Content-Type'
        return resp
    try:
        data = request.get_json()
        order = data.get('order', [])  # list of original indices in new order
        catalogs = _load_saved_catalogs()
        if len(order) != len(catalogs):
            return _cors_json({'error': 'order length mismatch'}, 400)
        reordered = [catalogs[i] for i in order if 0 <= i < len(catalogs)]
        if len(reordered) != len(catalogs):
            return _cors_json({'error': 'invalid indices'}, 400)
        _save_saved_catalogs(reordered)
        return _cors_json({'status': 'reordered', 'catalogs': reordered})
    except Exception as e:
        print(f"[Saved Catalogs] Reorder error: {e}", flush=True)
        return _cors_json({'error': str(e)}, 500)


@app.route('/saved-catalogs/<int:idx>', methods=['DELETE', 'OPTIONS'])
def delete_saved_catalog(idx):
    if request.method == 'OPTIONS':
        resp = make_response('', 204)
        resp.headers['Access-Control-Allow-Origin'] = '*'
        resp.headers['Access-Control-Allow-Methods'] = 'GET, POST, DELETE, OPTIONS'
        resp.headers['Access-Control-Allow-Headers'] = 'Content-Type'
        return resp
    try:
        catalogs = _load_saved_catalogs()
        if idx < 0 or idx >= len(catalogs):
            return _cors_json({'error': 'invalid index'}, 404)
        removed = catalogs.pop(idx)
        _save_saved_catalogs(catalogs)
        return _cors_json({'status': 'deleted', 'name': removed['name'], 'catalogs': catalogs})
    except Exception as e:
        print(f"[Saved Catalogs] Delete error: {e}", flush=True)
        return _cors_json({'error': str(e)}, 500)


DROPBOX_RESYNC_INTERVAL = int(os.environ.get('DROPBOX_RESYNC_HOURS', 1)) * 3600  # Default: 1 hour

_worker_initialized = False

@app.before_request
def ensure_worker_initialized():
    """Trigger startup sync on the first request to this worker process.
    This is the only reliable way to initialize with gunicorn's fork model."""
    global _worker_initialized
    if not _worker_initialized:
        _worker_initialized = True
        print("\n  [before_request] First request — triggering startup sync...")
        threading.Thread(target=startup_sync, daemon=True).start()


def hourly_resync():
    """Background loop: re-sync from Dropbox and regenerate exports every hour"""
    while True:
        time.sleep(DROPBOX_RESYNC_INTERVAL)
        print(f"\n  ⏰ Hourly re-sync triggered...")

        # Periodic Dropbox photo sync + S3 upload
        if (DROPBOX_PHOTOS_TOKEN or DROPBOX_REFRESH_TOKEN) and (time.time() - _dropbox_photos_last_sync > DROPBOX_PHOTOS_SYNC_HOURS * 3600):
            print(f"  📷 Dropbox photos sync due (every {DROPBOX_PHOTOS_SYNC_HOURS}h)...")
            try:
                sync_dropbox_photos()
                prewarm_dropbox_cache()  # Downloads new images + uploads to S3
            except Exception as e:
                print(f"  ⚠ Dropbox photos sync failed: {e}")

        with _export_lock:
            if _exports['generating']:
                print("  ⏭ Skipping — export generation already in progress")
                continue

        try:
            updated = sync_inventory()
            if updated:
                with _inv_lock:
                    count = _inventory['item_count']
                    source = _inventory['source']
                print(f"  ✓ Re-synced {count} items from {source}, regenerating exports...")
                trigger_background_generation()
            else:
                print("  No changes detected, skipping export regeneration")
        except Exception as e:
            print(f"  ⚠ Hourly re-sync failed: {e}")

        # Refresh APO allocation file every hour
        try:
            load_apo_from_dropbox()
        except Exception as e:
            print(f"  ⚠ APO hourly refresh failed: {e}")

        # Refresh Style Ledger (production data) every hour
        try:
            # Force reload by resetting timestamp
            global _production_last_sync
            _production_last_sync = 0
            load_production_from_dropbox()
        except Exception as e:
            print(f"  ⚠ Production hourly refresh failed: {e}")


def startup_sync():
    print("\n" + "="*60)
    print("  VERSA INVENTORY EXPORT API v3 — Startup")
    print(f"  Dropbox URL configured: {'YES' if DROPBOX_URL else 'NO'}")
    _dbx_configured = bool(DROPBOX_PHOTOS_TOKEN or DROPBOX_REFRESH_TOKEN)
    print(f"  Dropbox Photos configured: {'YES (refresh token)' if DROPBOX_REFRESH_TOKEN else ('YES (static token)' if DROPBOX_PHOTOS_TOKEN else 'NO')}")
    print("="*60)

    load_overrides_from_s3()
    load_manual_allocations_from_s3()
    load_deduction_assignments_from_s3()
    load_prepack_defaults_from_s3()
    load_suppression_overrides_from_s3()
    load_banner_rules_from_s3()
    try:
        load_apo_from_dropbox()
    except Exception as e:
        print(f"  ⚠ APO startup load failed: {e}")

    # Load Style Ledger (production data) from Dropbox
    try:
        load_production_from_dropbox()
    except Exception as e:
        print(f"  ⚠ Production startup load failed: {e}")

    # Sync Dropbox photos index (before inventory so images are ready for export generation)
    if _dbx_configured:
        print("  → Syncing Dropbox photos index...", flush=True)
        sync_dropbox_photos()
        # Start background pre-warm of all images to disk
        if _dropbox_photo_index:
            import threading as _th
            _th.Thread(target=prewarm_dropbox_cache, daemon=True).start()

    # Sync inventory: Dropbox first, then S3 fallback
    try:
        updated = sync_inventory()
        with _inv_lock:
            count = _inventory['item_count']
            source = _inventory['source']
        if count > 0:
            print(f"  ✓ Startup: {count} items loaded from {source}")
            print(f"  → Generating exports (images + Excel)...")
            trigger_background_generation()
        else:
            print("  ⚠ Startup: no inventory data")
    except Exception as e:
        print(f"  Startup sync failed: {e}")

    # Start hourly re-sync loop
    if DROPBOX_URL:
        print(f"  ⏰ Hourly Dropbox re-sync enabled (every {DROPBOX_RESYNC_INTERVAL//3600}h)")
        hourly_resync()  # This runs forever in the same thread


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)

