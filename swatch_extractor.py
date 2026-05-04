"""
SWATCH CARD EXTRACTOR — BACKEND (commit + history only)

Frontend handles the Anthropic vision call directly using AI_CONFIG.apiKey
(same pattern as the existing Versa AI chat). This module only handles the
two things that require server-side credentials:

  POST /api/swatch/commit   — APPEND-ONLY write to
                              s3://nauticaslimfit/Inventory+Colors+Data/style_color_map.xlsx
                              + audit log entry written
  GET  /api/swatch/history  — last N upload events

DESIGN GUARANTEE: This module NEVER edits or deletes existing rows.
The only write operation is `ws.append([sku, color])` after the last
existing row. See test_append_logic.py for byte-for-byte verification.

Wire-up in app.py (one line near the bottom, after `app = Flask(__name__)`
and after `get_s3()` / `S3_BUCKET` are defined):

    from swatch_extractor import register_swatch_routes
    register_swatch_routes(app, get_s3, S3_BUCKET)

Environment variables (all optional — sane defaults):
    S3_COLOR_MAP_KEY       — default "Inventory+Colors+Data/style_color_map.xlsx"
    S3_SWATCH_HISTORY_KEY  — default "inventory/swatch_upload_history.json"
"""

import os
import io
import json
import threading
from datetime import datetime, timezone

from flask import request, jsonify
import openpyxl
import requests as http_requests


# ─────────────────────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────────────────────
S3_COLOR_MAP_KEY      = os.environ.get('S3_COLOR_MAP_KEY',
                                       'Inventory+Colors+Data/style_color_map.xlsx')
S3_SWATCH_HISTORY_KEY = os.environ.get('S3_SWATCH_HISTORY_KEY',
                                       'inventory/swatch_upload_history.json')

# Anthropic API key — pulled from Render environment (NEVER hardcoded).
# The /api/ai-proxy endpoint uses this to forward requests on behalf of the
# browser, so the key never leaves the server.
ANTHROPIC_API_KEY     = os.environ.get('ANTHROPIC_API_KEY', '')
ANTHROPIC_API_URL     = 'https://api.anthropic.com/v1/messages'
ANTHROPIC_VERSION     = '2023-06-01'

# Sheet name the frontend looks for first (loadColorMapFromS3 in index.html)
COLOR_MAP_SHEET_NAME = 'Color Map'

# Serializes concurrent commits within a single worker. Cross-worker is rare
# enough that last-writer-wins is acceptable (audit log catches it anyway).
_commit_lock = threading.Lock()


# ─────────────────────────────────────────────────────────────────────────────
# COLOR MAP READ / APPEND
# ─────────────────────────────────────────────────────────────────────────────
def _download_color_map(get_s3, s3_bucket):
    """Download the master color map xlsx → openpyxl Workbook + ETag + size."""
    s3 = get_s3()
    resp = s3.get_object(Bucket=s3_bucket, Key=S3_COLOR_MAP_KEY)
    raw = resp['Body'].read()
    etag = resp.get('ETag', '').strip('"')
    wb = openpyxl.load_workbook(io.BytesIO(raw))
    return wb, etag, len(raw)


def _resolve_color_map_sheet(wb):
    """Match frontend logic: prefer 'Color Map' sheet, else first sheet."""
    if COLOR_MAP_SHEET_NAME in wb.sheetnames:
        return wb[COLOR_MAP_SHEET_NAME]
    return wb[wb.sheetnames[0]]


def _append_rows(ws, rows):
    """Append rows at the bottom. NEVER touches existing rows."""
    if ws.max_row < 1:
        ws.append(['Key', 'Color_Description'])
    for sku, color in rows:
        ws.append([sku, color])


def _upload_color_map(get_s3, s3_bucket, wb) -> str:
    buf = io.BytesIO()
    wb.save(buf)
    body = buf.getvalue()
    s3 = get_s3()
    resp = s3.put_object(
        Bucket=s3_bucket,
        Key=S3_COLOR_MAP_KEY,
        Body=body,
        ContentType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
    )
    new_etag = resp.get('ETag', '').strip('"')
    print(f"  [swatch] ✓ Uploaded color map to s3://{s3_bucket}/{S3_COLOR_MAP_KEY} "
          f"({len(body):,} bytes, ETag {new_etag[:8]})", flush=True)
    return new_etag


# ─────────────────────────────────────────────────────────────────────────────
# AUDIT LOG
# ─────────────────────────────────────────────────────────────────────────────
def _append_audit_entry(get_s3, s3_bucket, entry):
    """Append an entry to the JSON audit log. Best-effort; failure is logged
    but does not abort the commit."""
    s3 = get_s3()
    history = []
    try:
        resp = s3.get_object(Bucket=s3_bucket, Key=S3_SWATCH_HISTORY_KEY)
        history = json.loads(resp['Body'].read())
        if not isinstance(history, list):
            history = []
    except s3.exceptions.NoSuchKey:
        pass
    except Exception as e:
        print(f"  [swatch] ⚠ Could not read existing audit log: {e}", flush=True)

    history.append(entry)
    history = history[-1000:]  # cap log size

    try:
        s3.put_object(
            Bucket=s3_bucket,
            Key=S3_SWATCH_HISTORY_KEY,
            Body=json.dumps(history, indent=2).encode('utf-8'),
            ContentType='application/json',
        )
    except Exception as e:
        print(f"  [swatch] ⚠ Failed to write audit log: {e}", flush=True)


# ─────────────────────────────────────────────────────────────────────────────
# ROUTE REGISTRATION
# ─────────────────────────────────────────────────────────────────────────────
def register_swatch_routes(app, get_s3, s3_bucket):
    """Register swatch routes on a Flask app.

    Args:
        app: Flask application instance
        get_s3: callable returning a boto3 S3 client (your existing helper)
        s3_bucket: bucket name (e.g. S3_BUCKET)
    """

    @app.route('/api/swatch/commit', methods=['POST'])
    def swatch_commit():
        """Reviewed rows in → APPEND-ONLY to style_color_map.xlsx in S3.

        Body JSON:
        {
          "filename":      "JONES_SS_9_25-_R.pdf",
          "po_ref":        "NB26094",          // optional
          "delivery_date": "9/25 DELIVERY",    // optional
          "fabrication":   "95% POLYESTER...", // optional
          "rows": [
            {"sku": "JNAW008SSS", "color_description": "BLACK SOLID"},
            {"sku": "JNAW263SSP", "color_description": "NEW NAVY GRND||RED DASH PRINT"}
          ]
        }
        """
        body = request.get_json(silent=True) or {}
        rows = body.get('rows') or []
        if not rows:
            return jsonify({'error': 'No rows provided'}), 400

        # Sanitize: drop empties, strip whitespace, dedupe within payload only
        clean_rows = []
        seen_in_payload = set()
        for r in rows:
            sku = (r.get('sku') or '').strip()
            color = (r.get('color_description') or '').strip()
            if not sku or not color:
                continue
            sig = (sku.upper(), color)
            if sig in seen_in_payload:
                continue
            seen_in_payload.add(sig)
            clean_rows.append((sku, color))

        if not clean_rows:
            return jsonify({'error': 'No valid rows after cleaning'}), 400

        with _commit_lock:
            try:
                wb, prev_etag, _ = _download_color_map(get_s3, s3_bucket)
                ws = _resolve_color_map_sheet(wb)
                row_count_before = ws.max_row

                _append_rows(ws, clean_rows)
                new_etag = _upload_color_map(get_s3, s3_bucket, wb)

            except Exception as e:
                print(f"  [swatch] ✗ Commit failed: {e}", flush=True)
                return jsonify({'error': f'Commit failed: {e}'}), 500

        entry = {
            'timestamp':        datetime.now(timezone.utc).isoformat(),
            'filename':         body.get('filename', ''),
            'po_ref':           body.get('po_ref', ''),
            'delivery_date':    body.get('delivery_date', ''),
            'fabrication':      body.get('fabrication', ''),
            'rows_appended':    len(clean_rows),
            'prev_etag':        prev_etag,
            'new_etag':         new_etag,
            'row_count_before': row_count_before,
            'row_count_after':  row_count_before + len(clean_rows),
            'rows':             [{'sku': s, 'color': c} for s, c in clean_rows],
        }
        try:
            _append_audit_entry(get_s3, s3_bucket, entry)
        except Exception as e:
            print(f"  [swatch] ⚠ Audit log error (non-fatal): {e}", flush=True)

        return jsonify({
            'success':           True,
            'rows_appended':     len(clean_rows),
            'row_count_before':  row_count_before,
            'row_count_after':   row_count_before + len(clean_rows),
            'prev_etag':         prev_etag,
            'new_etag':          new_etag,
        })

    @app.route('/api/ai-proxy', methods=['POST'])
    def ai_proxy():
        """Server-side proxy for Anthropic API calls. The browser sends the
        same body it would send directly to api.anthropic.com; we attach the
        key from the Render environment and forward.

        This keeps ANTHROPIC_API_KEY out of index.html (which is in git) and
        out of the browser's network tab. If the key gets compromised, you
        rotate ONE env var instead of redeploying the frontend.
        """
        if not ANTHROPIC_API_KEY:
            return jsonify({
                'error': 'ANTHROPIC_API_KEY not configured on server. '
                         'Set it in Render → Environment → Environment Variables.'
            }), 503

        body = request.get_json(silent=True)
        if not body or not isinstance(body, dict):
            return jsonify({'error': 'Request body must be JSON'}), 400

        # Whitelist the fields we forward — protects against the browser
        # sneaking in fields we don't expect, and keeps the proxy minimal.
        allowed = {'model', 'messages', 'system', 'max_tokens',
                   'temperature', 'top_p', 'top_k', 'stop_sequences', 'tools'}
        forward = {k: v for k, v in body.items() if k in allowed}
        if 'model' not in forward or 'messages' not in forward:
            return jsonify({'error': 'Required fields: model, messages'}), 400
        forward.setdefault('max_tokens', 4096)

        try:
            resp = http_requests.post(
                ANTHROPIC_API_URL,
                headers={
                    'x-api-key': ANTHROPIC_API_KEY,
                    'anthropic-version': ANTHROPIC_VERSION,
                    'content-type': 'application/json',
                },
                json=forward,
                timeout=180,
            )
        except http_requests.exceptions.Timeout:
            return jsonify({'error': 'Anthropic API timed out'}), 504
        except Exception as e:
            print(f"  [ai-proxy] ✗ Upstream error: {e}", flush=True)
            return jsonify({'error': f'Upstream error: {e}'}), 502

        # Pass the upstream JSON + status straight through. On non-200,
        # Anthropic returns its own structured error which the frontend
        # handlers already know how to read.
        try:
            return jsonify(resp.json()), resp.status_code
        except ValueError:
            return jsonify({'error': f'Anthropic returned non-JSON ({resp.status_code})'}), 502

    @app.route('/api/swatch/history', methods=['GET'])
    def swatch_history():
        """Return last N audit log entries (most recent first)."""
        limit = int(request.args.get('limit', 25))
        try:
            s3 = get_s3()
            resp = s3.get_object(Bucket=s3_bucket, Key=S3_SWATCH_HISTORY_KEY)
            history = json.loads(resp['Body'].read())
            if not isinstance(history, list):
                history = []
        except Exception:
            history = []
        return jsonify({'history': history[-limit:][::-1]})

    print("  ✓ Swatch routes registered: /api/swatch/commit, /api/swatch/history, /api/ai-proxy",
          flush=True)
    if not ANTHROPIC_API_KEY:
        print("  ⚠ ANTHROPIC_API_KEY env var not set — /api/ai-proxy will return 503 until configured",
              flush=True)
