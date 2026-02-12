"""
VERSA INVENTORY EXPORT API v2
"""

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
from flask import Flask, request, jsonify, send_file, Response
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
app.config['MAX_CONTENT_LENGTH'] = 50 * 1024 * 1024  # 50MB for base64 images

AWS_REGION       = os.environ.get('AWS_REGION', 'us-east-2')
S3_BUCKET        = os.environ.get('S3_BUCKET', 'nauticaslimfit')
S3_INVENTORY_KEY = os.environ.get('S3_INVENTORY_KEY', 'inventory/daily_inventory.xlsx')
S3_EXPORT_PREFIX = os.environ.get('S3_EXPORT_PREFIX', 'exports/').rstrip('/') + '/'
S3_PHOTOS_PREFIX = os.environ.get('S3_PHOTOS_PREFIX',
                                   'ALL+INVENTORY+Photos/PHOTOS+INVENTORY')

S3_OVERRIDES_KEY = os.environ.get('S3_OVERRIDES_KEY', 'inventory/style_overrides.json')

S3_PHOTOS_URL = f"https://{S3_BUCKET}.s3.{AWS_REGION}.amazonaws.com/{S3_PHOTOS_PREFIX}"

TARGET_W = 150
TARGET_H = 150
COL_WIDTH_UNITS = 22

BRAND_IMAGE_PREFIX = {
    'NAUTICA': 'NA', 'DKNY': 'DK', 'EB': 'EB', 'REEBOK': 'RB', 'VINCE': 'VC',
    'BEN': 'BE', 'USPA': 'US', 'CHAPS': 'CH', 'LUCKY': 'LB', 'JNY': 'JN',
    'BEENE': 'GB', 'NICOLE': 'NM', 'SHAQ': 'SH', 'TAYION': 'TA', 'STRAHAN': 'MS',
    'VD': 'VD', 'VERSA': 'VR', 'AMERICA': 'AC', 'BLO': 'BL', 'DN': 'D9',
    'KL': 'KL', 'NE': 'NE'
}

BRAND_FULL_NAMES = {
    'NAUTICA': 'Nautica', 'DKNY': 'DKNY', 'EB': 'Eddie Bauer', 'REEBOK': 'Reebok',
    'VINCE': 'Vince Camuto', 'BEN': 'Ben Sherman', 'USPA': 'U.S. Polo Assn.',
    'CHAPS': 'Chaps', 'LUCKY': 'Lucky Brand', 'JNY': 'Jones New York',
    'BEENE': 'Geoffrey Beene', 'NICOLE': 'Nicole Miller', 'SHAQ': "Shaquille O'Neal",
    'TAYION': 'Tayion', 'STRAHAN': 'Michael Strahan', 'VD': 'Von Dutch',
    'VERSA': 'Versa', 'AMERICA': 'American Crew', 'BLO': 'Bloomingdales', 'DN': 'Divine 9',
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
}

_img_lock = threading.Lock()
_img_cache = {}

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
        # Find column indices (flexible matching)
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
    prefix = BRAND_IMAGE_PREFIX.get(brand_abbr, brand_abbr[:2])
    numbers = re.findall(r'\d+', str(sku))
    if numbers:
        main_number = max(numbers, key=len)
        return f"{prefix}_{main_number}"
    return f"{prefix}_{sku}"


def get_image_url(item, s3_base_url):
    brand_abbr = item.get('brand_abbr', item.get('brand', ''))
    folder_name = FOLDER_MAPPING.get(brand_abbr, brand_abbr)
    image_code = extract_image_code(item['sku'], brand_abbr)
    return f"{s3_base_url}/{folder_name}/{image_code}.jpg"


def _process_image_from_url(url, tw=TARGET_W, th=TARGET_H):
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


def get_image_cached(url):
    base = url.rsplit('.', 1)[0]

    with _img_lock:
        if base in _img_cache:
            c = _img_cache[base]
            return {
                'image_data': BytesIO(c['raw_bytes']),
                'x_scale': c['x_scale'], 'y_scale': c['y_scale'],
                'x_offset': c['x_offset'], 'y_offset': c['y_offset'],
                'object_position': 1, 'url': c['url']
            }

    result = _process_image_from_url(url)
    if result:
        with _img_lock:
            _img_cache[base] = result
        return {
            'image_data': BytesIO(result['raw_bytes']),
            'x_scale': result['x_scale'], 'y_scale': result['y_scale'],
            'x_offset': result['x_offset'], 'y_offset': result['y_offset'],
            'object_position': 1, 'url': result['url']
        }
    return None


def download_images_for_items(items, s3_base_url, use_cache=True):
    urls = [(i, get_image_url(item, s3_base_url)) for i, item in enumerate(items)]
    results = {}

    def _fetch(idx_url):
        idx, url = idx_url
        if use_cache:
            return idx, get_image_cached(url)
        else:
            r = _process_image_from_url(url)
            if r:
                return idx, {
                    'image_data': BytesIO(r['raw_bytes']),
                    'x_scale': r['x_scale'], 'y_scale': r['y_scale'],
                    'x_offset': r['x_offset'], 'y_offset': r['y_offset'],
                    'object_position': 1, 'url': r['url']
                }
            return idx, None

    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as pool:
        futures = {pool.submit(_fetch, pair): pair[0] for pair in urls}
        for f in concurrent.futures.as_completed(futures):
            try:
                idx, img = f.result()
                if img:
                    results[idx] = img
            except Exception:
                pass
    return results


def _setup_worksheet(workbook, worksheet, has_color=False):
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

    if has_color:
        headers = ['IMAGE', 'SKU', 'Brand', 'Color', 'Fit', 'Fabric Code', 'Fabrication',
                   'JTW', 'TR', 'DCW', 'QA', 'Incoming', 'Total Warehouse', 'Total ATS']
    else:
        headers = ['IMAGE', 'SKU', 'Brand', 'Fit', 'Fabric Code', 'Fabrication',
                   'JTW', 'TR', 'DCW', 'QA', 'Incoming', 'Total Warehouse', 'Total ATS']
    worksheet.set_row(0, 25)
    for c, h in enumerate(headers):
        worksheet.write(0, c, h, fmt_header)

    worksheet.set_column(0, 0, COL_WIDTH_UNITS)
    worksheet.set_column(1, 1, 20)
    worksheet.set_column(2, 2, 20)
    if has_color:
        worksheet.set_column(3, 3, 18)  # Color
        worksheet.set_column(4, 4, 12)  # Fit
        worksheet.set_column(5, 5, 12)  # Fabric Code
        worksheet.set_column(6, 6, 35)  # Fabrication
        worksheet.set_column(7, 13, 12) # Number columns (JTW through Total ATS)
    else:
        worksheet.set_column(3, 3, 12)
        worksheet.set_column(4, 4, 12)
        worksheet.set_column(5, 5, 35)
        worksheet.set_column(6, 12, 12) # Number columns (JTW through Total ATS)
    worksheet.set_default_row(112.5)
    return fmts


def _write_rows(workbook, worksheet, data, images, fmts, has_color=False):
    for r, item in enumerate(data):
        row = r + 1
        even = r % 2 == 1
        cf = fmts['even'] if even else fmts['odd']
        if has_color:
            vals = [
                '', item.get('sku',''), item.get('brand_full',''),
                item.get('color',''),
                item.get('fit','N/A'), item.get('fabric_code','N/A'),
                item.get('fabrication','Standard Fabric'),
                item.get('jtw',0), item.get('tr',0), item.get('dcw',0),
                item.get('qa',0), item.get('incoming',0),
                item.get('total_warehouse',0), item.get('total_ats',0)
            ]
            num_start = 7
            num_end = 13
        else:
            vals = [
                '', item.get('sku',''), item.get('brand_full',''),
                item.get('fit','N/A'), item.get('fabric_code','N/A'),
                item.get('fabrication','Standard Fabric'),
                item.get('jtw',0), item.get('tr',0), item.get('dcw',0),
                item.get('qa',0), item.get('incoming',0),
                item.get('total_warehouse',0), item.get('total_ats',0)
            ]
            num_start = 6
            num_end = 12
        for c, v in enumerate(vals):
            f = (fmts['num_even'] if even else fmts['num_odd']) if num_start <= c <= num_end else cf
            worksheet.write(row, c, v, f)

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


def build_brand_excel(brand_name, items, s3_base_url):
    # Detect if items have color data
    has_color = any(item.get('color') for item in items)

    buf = BytesIO()
    wb = xlsxwriter.Workbook(buf, {'in_memory': True})
    wb.set_properties({'title': f'Versa - {brand_name}', 'author': 'Versa Inventory System'})
    ws = wb.add_worksheet(brand_name[:31])
    fmts = _setup_worksheet(wb, ws, has_color=has_color)
    imgs = download_images_for_items(items, s3_base_url, use_cache=True)
    n = _write_rows(wb, ws, items, imgs, fmts, has_color=has_color)
    _add_size_charts(wb, ws, n + 2)
    wb.close()
    return buf.getvalue()


def build_multi_brand_excel(brands_list, s3_base_url):
    # Sort items within each brand by total_warehouse descending
    for b in brands_list:
        b['items'] = sorted(b['items'], key=lambda x: x.get('total_warehouse', 0), reverse=True)

    # Detect if any items have color data
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
        fmts = _setup_worksheet(wb, ws, has_color=has_color)
        start, count = offsets[bi]
        local_imgs = {}
        for li in range(count):
            gi = start + li
            if gi in all_imgs:
                local_imgs[li] = all_imgs[gi]
        n = _write_rows(wb, ws, brand['items'], local_imgs, fmts, has_color=has_color)
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

        # Additional context fields
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
        print(f"{'='*60}")

        all_items = []
        for abbr, brand in brands.items():
            all_items.extend(brand['items'])

        print(f"  Pre-caching images for {len(all_items)} items...")
        download_images_for_items(all_items, S3_PHOTOS_URL, use_cache=True)
        with _img_lock:
            cached_count = len(_img_cache)
        print(f"  Image cache: {cached_count} images\n")

        brands_list_for_multi = []
        done = 0

        # Sort brands by total warehouse inventory (highest first) for tab ordering
        sorted_brands = sorted(brands.items(),
            key=lambda x: sum(i.get('total_warehouse', 0) for i in x[1]['items']),
            reverse=True)

        for abbr, brand in sorted_brands:
            done += 1
            name = brand['name']
            with _export_lock:
                _exports['progress'] = f"{done}/{total}: {name}"

            print(f"  [{done}/{total}] {name} ({len(brand['items'])} items)")

            # Sort items within brand by total_warehouse descending
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
        print(f"{'='*60}\n")

    except Exception as e:
        print(f"  Export generation error: {e}")
        with _export_lock:
            _exports['generating'] = False
            _exports['progress'] = f'error: {e}'


def trigger_background_generation():
    t = threading.Thread(target=generate_all_exports, daemon=True)
    t.start()


@app.route('/', methods=['GET'])
def home():
    return jsonify({
        "service": "Versa Inventory Export API v2",
        "status": "running",
    })


@app.route('/health', methods=['GET'])
def health():
    with _inv_lock:
        inv_count = _inventory['item_count']
        last_sync = _inventory['last_sync']
    with _export_lock:
        gen = _exports['generating']
        brands_ready = len(_exports['brands'])
        progress = _exports['progress']
    with _img_lock:
        img_count = len(_img_cache)

    return jsonify({
        "status": "healthy",
        "inventory_items": inv_count,
        "last_sync": last_sync,
        "exports_generating": gen,
        "exports_ready": brands_ready,
        "generation_progress": progress,
        "images_cached": img_count,
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
            # Sort items within each brand by total_warehouse descending
            sorted_items = sorted(all_brands[abbr]['items'],
                key=lambda x: x.get('total_warehouse', 0), reverse=True)
            brands_list.append({
                'brand_name': all_brands[abbr]['name'],
                'items': sorted_items
            })

    if not brands_list:
        return jsonify({"error": "No matching brands in inventory"}), 404

    # Sort brands (tabs) by total warehouse inventory, highest first
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
        if not data:
            return jsonify({"error": "Empty data"}), 400

        xl_bytes = build_brand_excel(fname, data, s3_url)
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
        if not brands_data:
            return jsonify({"error": "Empty brands"}), 400

        xl_bytes = build_multi_brand_excel(brands_data, s3_url)
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


def startup_sync():
    time.sleep(3)
    print("\n  Running startup sync...")
    load_overrides_from_s3()
    try:
        updated = sync_inventory()
        with _inv_lock:
            count = _inventory['item_count']
        if count > 0:
            print(f"  Startup: {count} items loaded, generating exports...")
            trigger_background_generation()
        else:
            print("  Startup: no inventory data (upload Excel to S3 or use /upload)")
    except Exception as e:
        print(f"  Startup sync failed: {e}")

threading.Thread(target=startup_sync, daemon=True).start()


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)
