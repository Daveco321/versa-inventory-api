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

app = Flask(__name__)
CORS(app, resources={
    r"/*": {
        "origins": "*",
        "methods": ["GET", "POST", "OPTIONS"],
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

S3_PHOTOS_URL = f"https://{S3_BUCKET}.s3.{AWS_REGION}.amazonaws.com/{S3_PHOTOS_PREFIX}"

# STYLE+OVERRIDES — primary image source (matches frontend logic)
S3_OVERRIDES_IMG_URL = f"https://{S3_BUCKET}.s3.{AWS_REGION}.amazonaws.com/ALL+INVENTORY+Photos/STYLE+OVERRIDES"

# Dropbox direct download URL for hourly inventory file
# Dropbox shared link — will be converted to direct download at runtime
DROPBOX_URL = os.environ.get('DROPBOX_URL',
    'https://www.dropbox.com/scl/fi/de3nzb66mx41un0j69kyk/Inventory_ATS.xlsx?rlkey=ihoxu4s7gpb5ei14w2cl9dvyw&dl=1')

TARGET_W = 150
TARGET_H = 150
COL_WIDTH_UNITS = 22

BRAND_IMAGE_PREFIX = {
    'NAUTICA': 'NA', 'DKNY': 'DK', 'EB': 'EB', 'REEBOK': 'RB', 'VINCE': 'VC',
    'BEN': 'BE', 'USPA': 'US', 'CHAPS': 'CH', 'LUCKY': 'LB', 'JNY': 'JN',
    'BEENE': 'GB', 'NICOLE': 'NM', 'SHAQ': 'SH', 'TAYION': 'TA', 'STRAHAN': 'MS',
    'VD': 'VD', 'VERSA': 'VR', 'CHEROKEE': 'CK', 'AMERICA': 'AC', 'BLO': 'BL', 'DN': 'D9',
    'KL': 'KL', 'NE': 'NE'
}

BRAND_FULL_NAMES = {
    'NAUTICA': 'Nautica', 'DKNY': 'DKNY', 'EB': 'Eddie Bauer', 'REEBOK': 'Reebok',
    'VINCE': 'Vince Camuto', 'BEN': 'Ben Sherman', 'USPA': 'U.S. Polo Assn.',
    'CHAPS': 'Chaps', 'LUCKY': 'Lucky Brand', 'JNY': 'Jones New York',
    'BEENE': 'Geoffrey Beene', 'NICOLE': 'Nicole Miller', 'SHAQ': "Shaquille O'Neal",
    'TAYION': 'Tayion', 'STRAHAN': 'Michael Strahan', 'VD': 'Von Dutch',
    'VERSA': 'Versa', 'CHEROKEE': 'Cherokee', 'AMERICA': 'American Crew', 'BLO': 'Bloomingdales', 'DN': 'Divine 9',
    'KL': 'Karl Lagerfeld Paris', 'NE': 'Neiman Marcus'
}

FOLDER_MAPPING = {
    'EB': 'EDDIE+BAUER', 'USPA': 'US+POLO', 'VINCE': 'VINCE+CAMUTO',
    'LUCKY': 'LUCKY+BRAND', 'BEN': 'BEN+SHERMAN', 'BEENE': 'GEOFFREY+BEENE',
    'NICOLE': 'NICOLE+MILLER', 'AMERICA': 'AMERICAN+CREW',
    'TAYION': 'TAYON', 'VD': 'Von+Dutch'
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

def load_overrides_from_s3():
    """Load style overrides from S3 on startup"""
    global _style_overrides
    try:
        s3 = get_s3()
        resp = s3.get_object(Bucket=S3_BUCKET, Key=S3_OVERRIDES_KEY)
        data = json.loads(resp['Body'].read().decode('utf-8'))
        with _overrides_lock:
            _style_overrides = data
        print(f"  ✓ Loaded {len(_style_overrides)} style overrides from S3")
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            print("  No existing style overrides in S3 (will create on first save)")
        else:
            print(f"  ⚠ Could not load overrides from S3: {e}")
    except Exception as e:
        print(f"  ⚠ Override load error: {e}")

def save_overrides_to_s3():
    """Save style overrides to S3"""
    try:
        s3 = get_s3()
        with _overrides_lock:
            data = json.dumps(_style_overrides)
        s3.put_object(
            Bucket=S3_BUCKET,
            Key=S3_OVERRIDES_KEY,
            Body=data.encode('utf-8'),
            ContentType='application/json'
        )
        print(f"  ✓ Saved {len(_style_overrides)} style overrides to S3")
        return True
    except Exception as e:
        print(f"  ✗ Failed to save overrides to S3: {e}")
        return False


S3_ALLOCATION_KEY = os.environ.get('S3_ALLOCATION_KEY', 'inventory/VIRTUAL WAREHOUSE ALLOCATION.csv')
S3_PRODUCTION_KEY = os.environ.get('S3_PRODUCTION_KEY', 'inventory/Style Ledger.xlsx')


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


def load_production_from_s3():
    """Load Style Ledger xlsx from S3 and return as list of dicts"""
    try:
        s3 = get_s3()
        resp = s3.get_object(Bucket=S3_BUCKET, Key=S3_PRODUCTION_KEY)
        data = resp['Body'].read()
        wb = openpyxl.load_workbook(BytesIO(data), read_only=True)
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
        print(f"  ✓ Loaded {len(results)} production rows from S3")
        return results
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            print("  No Style Ledger found in S3")
        else:
            print(f"  ⚠ Could not load production from S3: {e}")
        return []
    except Exception as e:
        print(f"  ⚠ Production load error: {e}")
        return []


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
            resp = http_requests.get(try_url, headers=headers, timeout=10)
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


def get_image_cached(item, s3_base_url):
    """
    Get image for an item, using cache keyed by base_style.
    Priority: STYLE+OVERRIDES → brand folder fallback.
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

    # Try STYLE+OVERRIDES first (primary — matches frontend)
    override_url = get_style_override_url(sku)
    result = _process_image_from_url(override_url)

    # Fallback to brand folder
    if not result:
        brand_url = get_image_url(item, s3_base_url)
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
        return base_style, get_image_cached(item, s3_base_url)

    unique_pairs = list(unique_items.items())
    print(f"    Fetching images: {len(unique_pairs)} unique styles for {len(items)} items")

    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as pool:
        futures = {pool.submit(_fetch, pair): pair[0] for pair in unique_pairs}
        for f in concurrent.futures.as_completed(futures):
            try:
                base_style, img = f.result()
                if img:
                    # Apply this image to ALL items with this base_style
                    for idx in style_to_indices.get(base_style, []):
                        # Each index needs its own BytesIO copy
                        cached = _img_cache.get(base_style)
                        if cached:
                            results[idx] = {
                                'image_data': BytesIO(cached['raw_bytes']),
                                'x_scale': cached['x_scale'], 'y_scale': cached['y_scale'],
                                'x_offset': cached['x_offset'], 'y_offset': cached['y_offset'],
                                'object_position': 1, 'url': cached['url']
                            }
            except Exception:
                pass

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
                worksheet.insert_image(row, 0, "img.png", {
                    'image_data': img['image_data'],
                    'x_scale': img['x_scale'], 'y_scale': img['y_scale'],
                    'x_offset': img['x_offset'], 'y_offset': img['y_offset'],
                    'object_position': 1, 'url': img.get('url', '')
                })
            except Exception:
                worksheet.write(row, 0, "Error", cf)
        else:
            worksheet.write(row, 0, "No Image", cf)
    return len(data)


def _add_size_charts(workbook, worksheet, start):
    t = workbook.add_format({'bold':True,'font_name':'Calibri','font_size':11,'bg_color':'#FFFFFF','border':0,'align':'left','valign':'vcenter'})
    s = workbook.add_format({'bold':True,'font_name':'Calibri','font_size':10,'bg_color':'#FFFFFF','font_color':'#FF0000','border':0,'align':'center','valign':'vcenter'})
    gh = workbook.add_format({'bold':True,'font_name':'Calibri','font_size':10,'border':1,'align':'center','valign':'vcenter','bg_color':'#FFFFFF'})
    gd = workbook.add_format({'font_name':'Calibri','font_size':10,'border':1,'align':'center','valign':'vcenter','bg_color':'#FFFFFF'})
    r = start
    for i in range(5): worksheet.set_row(r+i, [20,18,25,25,25][i])

    worksheet.write(r,0,'Slim Fit 9 pcs inner, 36 pcs / box (4 inners)',t)
    worksheet.merge_range(r+1,0,r+1,4,'9 PC. Slim Fit SIZE SCALE TO USE',s)
    for c,v in enumerate(['','14-14.5','15-15.5','16-16.5','17-17.5']): worksheet.write(r+2,c,v,gh)
    worksheet.write(r+3,0,'32/33',gh)
    for c,v in enumerate([1,2,1,''],1): worksheet.write(r+3,c,v,gd)
    worksheet.write(r+4,0,'34/35',gh)
    for c,v in enumerate(['',1,2,2],1): worksheet.write(r+4,c,v,gd)

    worksheet.write(r,7,'Regular Fit 9 pcs inner, 36 pcs / box (4 inners)',t)
    worksheet.merge_range(r+1,7,r+1,11,'9 PC. CLASSIC FIT & REGULAR FIT SIZE SCALE TO USE',s)
    for c,v in enumerate(['','15-15.5','16-16.5','17-17.5','18-18.5']): worksheet.write(r+2,7+c,v,gh)
    worksheet.write(r+3,7,'32/33',gh)
    for c,v in enumerate([1,2,1,''],1): worksheet.write(r+3,7+c,v,gd)
    worksheet.write(r+4,7,'34/35',gh)
    for c,v in enumerate(['',1,2,2],1): worksheet.write(r+4,7+c,v,gd)


def build_brand_excel(brand_name, items, s3_base_url, view_mode='all', is_order=False,
                      catalog_mode=False):
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
    fmts, headers = _setup_worksheet(wb, ws, has_color=has_color, view_mode=view_mode,
                                     is_order=is_order, incoming_only=incoming_only,
                                     catalog_mode=catalog_mode)
    imgs = download_images_for_items(items, s3_base_url, use_cache=True)
    n = _write_rows(wb, ws, items, imgs, fmts, has_color=has_color,
                    view_mode=view_mode, headers=headers)
    _add_size_charts(wb, ws, n + 2)
    wb.close()
    return buf.getvalue()


def build_multi_brand_excel(brands_list, s3_base_url, catalog_mode=False, view_mode='all'):
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
        _add_size_charts(wb, ws, n + 2)

    wb.close()
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
def sync_from_dropbox():
    """Fetch inventory directly from Dropbox shared link"""
    if not DROPBOX_URL:
        print("  No DROPBOX_URL configured, skipping Dropbox sync")
        return False

    # Convert any Dropbox URL format to direct download
    url = DROPBOX_URL.strip()
    # Remove st= param (session-specific, expires)
    url = re.sub(r'[&?]st=[^&]*', '', url)
    # Ensure dl=1 for direct download
    url = re.sub(r'[&?]dl=\d', '', url)
    url += ('&' if '?' in url else '?') + 'dl=1'

    print(f"  📂 Fetching inventory from Dropbox...")
    print(f"     URL: {url[:80]}...")
    try:
        # Use a session to follow redirects properly
        session = http_requests.Session()
        resp = session.get(url, timeout=60, headers={
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }, allow_redirects=True)

        print(f"     HTTP {resp.status_code}, Content-Type: {resp.headers.get('Content-Type', 'unknown')}")
        print(f"     Size: {len(resp.content)} bytes, Redirects: {len(resp.history)}")

        if resp.status_code != 200:
            print(f"  ⚠ Dropbox returned HTTP {resp.status_code}")
            return False

        # Verify we got an Excel file, not an HTML page
        ct = resp.headers.get('Content-Type', '').lower()
        if 'html' in ct:
            print(f"  ⚠ Dropbox returned HTML instead of Excel (likely auth/redirect issue)")
            print(f"     First 200 chars: {resp.text[:200]}")
            return False

        data = resp.content
        if len(data) < 1000:
            print(f"  ⚠ Dropbox file too small ({len(data)} bytes), likely error page")
            return False

        print(f"  📂 Downloaded {len(data):,} bytes from Dropbox")

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
    # Try Dropbox first
    if DROPBOX_URL:
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
                xl_bytes = build_brand_excel(name, sorted_items, S3_PHOTOS_URL)

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
                    'items': sorted_items
                })
            except Exception as e:
                print(f"    Failed: {e}")

        if brands_list_for_multi:
            print(f"\n  [ALL] Multi-tab ({len(brands_list_for_multi)} brands)...")
            try:
                multi_bytes = build_multi_brand_excel(brands_list_for_multi, S3_PHOTOS_URL)
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

    xl_bytes = build_multi_brand_excel(brands_list, S3_PHOTOS_URL)
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
        if not data:
            return jsonify({"error": "Empty data"}), 400

        xl_bytes = build_brand_excel(fname, data, s3_url, view_mode=view_mode,
                                     is_order=is_order, catalog_mode=catalog_mode)
        ts = datetime.now().strftime('%Y-%m-%d')
        return send_file(BytesIO(xl_bytes),
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            as_attachment=True, download_name=f"{fname}_{ts}.xlsx")
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
        if not brands_data:
            return jsonify({"error": "Empty brands"}), 400

        xl_bytes = build_multi_brand_excel(brands_data, s3_url,
                                           catalog_mode=catalog_mode, view_mode=view_mode)
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
    with _overrides_lock:
        return jsonify({"overrides": _style_overrides})

@app.route('/overrides', methods=['POST'])
def save_overrides():
    try:
        req = request.get_json()
        if not req or 'overrides' not in req:
            return jsonify({"error": "Missing 'overrides' in request body"}), 400

        overrides = req['overrides']
        if not isinstance(overrides, dict):
            return jsonify({"error": "'overrides' must be an object"}), 400

        with _overrides_lock:
            global _style_overrides
            _style_overrides = overrides

        success = save_overrides_to_s3()

        if success:
            return jsonify({"success": True, "count": len(overrides)})
        else:
            return jsonify({"error": "Failed to save to S3"}), 500
    except Exception as e:
        import traceback
        return jsonify({"error": str(e), "trace": traceback.format_exc()}), 500


@app.route('/allocations', methods=['GET', 'OPTIONS'])
def get_allocations():
    if request.method == 'OPTIONS':
        return '', 204
    data = load_allocation_from_s3()
    return jsonify({"allocations": data})


@app.route('/production', methods=['GET', 'OPTIONS'])
def get_production():
    if request.method == 'OPTIONS':
        return '', 204
    data = load_production_from_s3()
    return jsonify({"production": data})


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
    """Download raw image bytes from S3 (override → brand folder, .jpg → .png)"""
    headers = {'User-Agent': 'Mozilla/5.0'}

    # Try STYLE+OVERRIDES first
    override_base = f"{S3_OVERRIDES_IMG_URL}/{base_style}"
    # Then brand folder
    folder_name = FOLDER_MAPPING.get(brand_abbr, brand_abbr)
    image_code = extract_image_code(base_style, brand_abbr)
    brand_base = f"{S3_PHOTOS_URL}/{folder_name}/{image_code}"

    for base_url in [override_base, brand_base]:
        for ext in ['.jpg', '.png', '.jpeg']:
            try:
                url = base_url + ext
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
            if len(_web_img_cache) > 500:
                # Evict oldest ~100 entries
                keys = list(_web_img_cache.keys())[:100]
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


def startup_sync():
    print("\n" + "="*60)
    print("  VERSA INVENTORY EXPORT API v3 — Startup")
    print(f"  Dropbox URL configured: {'YES' if DROPBOX_URL else 'NO'}")
    print("="*60)

    load_overrides_from_s3()

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

