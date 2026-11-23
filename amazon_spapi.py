"""
amazon_spapi.py: a small client for Amazon's Selling Partner API (SP-API), used only
for Versa's own seller account. It is a private developer app with non-restricted
roles, so no buyer personal information is ever requested or stored.

Step 1 of the Amazon agent (read-only, David Nov 23 2026): live FBA stock, restock
recommendations, listings and sales for the daily Amazon brief. Shipments (Fulfillment
Inbound v2024-03-20) and listing writes come later and reuse call().

    SpApiConfig.from_env()      (config, reason): the three LWA values, or (None, reason)
    SpApiClient(config)         one client per job (not shared between threads)
        call(method, path, op, params=None, body=None)    one SP-API operation
        access_token(force=False)                         LWA refresh-token grant, cached
        inventory_summaries()                             every FBA inventory row, all pages
        run_report(report_type, start, end)               create, poll, download: bytes | None
        catalog_items(asins)                              title, brand, color, size, parent ASIN
    parse_tsv(bytes)            flat-file report bytes -> list of dicts (BOM and cp1252 safe)

SECRETS: the client id, client secret and refresh token come from SPAPI_LWA_CLIENT_ID,
SPAPI_LWA_CLIENT_SECRET and SPAPI_REFRESH_TOKEN. They are never logged, never stored and
never put in an exception: SpApiError carries Amazon's error code and message only, with
any secret scrubbed out. This file lives in a PUBLIC repository, so no credential, seller
id or account data belongs in it.

THROTTLING: every operation has a token bucket at Amazon's default rate and burst, and a
429 or 5xx answer is retried with exponential backoff plus jitter (Retry-After wins when
it asks for longer).
"""
import csv
import gzip
import io
import os
import random
import re
import time
from datetime import datetime, timezone
from urllib.parse import quote

ENDPOINT_NA = 'https://sellingpartnerapi-na.amazon.com'
TOKEN_URL = 'https://api.amazon.com/auth/o2/token'
MARKETPLACE_US = 'ATVPDKIKX0DER'
USER_AGENT = 'VersaAmazonAgent/1.0 (Language=Python)'

ENV_CLIENT_ID = 'SPAPI_LWA_CLIENT_ID'
ENV_CLIENT_SECRET = 'SPAPI_LWA_CLIENT_SECRET'
ENV_REFRESH_TOKEN = 'SPAPI_REFRESH_TOKEN'
ENV_ENDPOINT = 'SPAPI_ENDPOINT'          # optional: the sandbox host for a dry run

# Only Amazon's own SP-API hosts may receive an access token.
_ENDPOINT_RE = re.compile(r'^https://(sandbox\.)?sellingpartnerapi-(na|eu|fe)\.amazon\.com$')

# Report types Step 1 reads. All are tab-separated flat files and none needs a restricted role.
REPORT_RESTOCK = 'GET_RESTOCK_INVENTORY_RECOMMENDATIONS_REPORT'
REPORT_ALL_LISTINGS = 'GET_MERCHANT_LISTINGS_ALL_DATA'
REPORT_ORDERS_BY_DATE = 'GET_FLAT_FILE_ALL_ORDERS_DATA_BY_ORDER_DATE_GENERAL'

# (requests per second, burst) per operation: Amazon's default usage plans.
RATE_LIMITS = {
    'getInventorySummaries': (2.0, 2),
    'createReport': (0.0167, 15),
    'getReport': (2.0, 15),
    'getReportDocument': (0.0167, 15),
    'searchCatalogItems': (2.0, 2),
    'getOrderMetrics': (0.5, 15),
}
DEFAULT_RATE = (1.0, 1)
RETRY_STATUSES = (429, 500, 502, 503, 504)
CATALOG_BATCH = 20                        # searchCatalogItems takes at most 20 identifiers


class SpApiError(Exception):
    """A refusal from SP-API or Login with Amazon: operation, HTTP status, Amazon's error
    code and a short message. Never carries a credential."""

    def __init__(self, op, status, code, message=''):
        self.op = str(op or '')
        self.status = int(status or 0)
        self.code = str(code or 'error')
        self.message = str(message or '')[:300]
        text = f'{self.op}: HTTP {self.status} {self.code}'
        if self.message:
            text += f' ({self.message})'
        super().__init__(text)


class SpApiConfig:
    """The LWA app credentials plus where to send requests. repr() never shows a secret."""

    __slots__ = ('client_id', 'client_secret', 'refresh_token', 'endpoint', 'marketplace_id')

    def __init__(self, client_id, client_secret, refresh_token,
                 endpoint=ENDPOINT_NA, marketplace_id=MARKETPLACE_US):
        self.client_id = str(client_id or '').strip()
        self.client_secret = str(client_secret or '').strip()
        self.refresh_token = str(refresh_token or '').strip()
        endpoint = str(endpoint or '').strip().rstrip('/')
        self.endpoint = endpoint if _ENDPOINT_RE.match(endpoint) else ENDPOINT_NA
        self.marketplace_id = str(marketplace_id or MARKETPLACE_US).strip()

    def __repr__(self):
        return f'<SpApiConfig endpoint={self.endpoint} marketplace={self.marketplace_id} credentials=hidden>'

    @classmethod
    def from_env(cls, env=None):
        """(config, reason). config is None when an LWA value is missing; reason then names
        the missing variables (names only, never values)."""
        env = os.environ if env is None else env
        names = (ENV_CLIENT_ID, ENV_CLIENT_SECRET, ENV_REFRESH_TOKEN)
        vals = {n: str(env.get(n, '') or '').strip() for n in names}
        missing = [n for n in names if not vals[n]]
        if missing:
            return None, 'missing ' + ', '.join(missing)
        endpoint = str(env.get(ENV_ENDPOINT, '') or '').strip().rstrip('/') or ENDPOINT_NA
        reason = ''
        if not _ENDPOINT_RE.match(endpoint):
            reason = f'{ENV_ENDPOINT} ignored (not an SP-API host)'
            endpoint = ENDPOINT_NA
        return cls(vals[ENV_CLIENT_ID], vals[ENV_CLIENT_SECRET], vals[ENV_REFRESH_TOKEN],
                   endpoint=endpoint), reason


class _Bucket:
    """Token bucket: `rate` tokens a second, at most `burst` saved up."""

    def __init__(self, rate, burst, now):
        self.rate = float(rate)
        self.burst = float(burst)
        self.tokens = float(burst)
        self.at = float(now)

    def take(self, now):
        """Seconds to wait before this request may go (0 when a token is ready)."""
        now = float(now)
        self.tokens = min(self.burst, self.tokens + max(0.0, now - self.at) * self.rate)
        self.at = now
        if self.tokens >= 1.0:
            self.tokens -= 1.0
            return 0.0
        wait = (1.0 - self.tokens) / self.rate
        self.tokens = 0.0
        self.at = now + wait
        return wait


class SpApiClient:
    """SP-API calls for one job. Injectable session, sleep, clock and rng keep the tests offline."""

    def __init__(self, config, session=None, sleep=None, clock=None, rng=None,
                 max_retries=6, timeout=60):
        if config is None:
            raise ValueError('config required')
        self.config = config
        if session is None:
            import requests
            session = requests.Session()
        self._session = session
        self._sleep = sleep or time.sleep
        self._clock = clock or time.monotonic
        self._rng = rng or random.Random()
        self.max_retries = int(max_retries)
        self.timeout = timeout
        self._token = None
        self._token_until = 0.0
        self._buckets = {}
        self.calls = 0              # HTTP requests sent, for the sync log

    def __repr__(self):
        return f'<SpApiClient endpoint={self.config.endpoint}>'

    # ── plumbing ──
    def _scrub(self, text):
        s = str(text or '')
        for secret in (self.config.client_secret, self.config.refresh_token, self._token):
            if secret:
                s = s.replace(secret, '***')
        return s

    def _pace(self, op):
        b = self._buckets.get(op)
        if b is None:
            rate, burst = RATE_LIMITS.get(op, DEFAULT_RATE)
            b = self._buckets[op] = _Bucket(rate, burst, self._clock())
        wait = b.take(self._clock())
        if wait > 0:
            self._sleep(wait)

    def _backoff(self, attempt, resp=None):
        delay = min(60.0, 2.0 ** attempt) + self._rng.uniform(0.0, 0.5)
        retry_after = _header(resp, 'retry-after')
        if retry_after:
            try:
                delay = max(delay, min(120.0, float(retry_after)))
            except ValueError:
                pass
        self._sleep(delay)

    def _send(self, method, url, **kw):
        self.calls += 1
        return self._session.request(method, url, timeout=kw.pop('timeout', self.timeout), **kw)

    # ── Login with Amazon ──
    def access_token(self, force=False):
        """A cached LWA access token, renewed a minute before it expires."""
        now = self._clock()
        if not force and self._token and now < self._token_until:
            return self._token
        data = {'grant_type': 'refresh_token', 'refresh_token': self.config.refresh_token,
                'client_id': self.config.client_id, 'client_secret': self.config.client_secret}
        headers = {'user-agent': USER_AGENT, 'accept': 'application/json'}
        resp = None
        for attempt in range(self.max_retries + 1):
            resp = self._send('POST', TOKEN_URL, data=data, headers=headers)
            if resp.status_code in RETRY_STATUSES and attempt < self.max_retries:
                self._backoff(attempt, resp)
                continue
            break
        body = _json_dict(resp)
        token = body.get('access_token')
        if resp.status_code != 200 or not token:
            raise SpApiError('lwa_token', resp.status_code, body.get('error') or 'lwa_error',
                             self._scrub(body.get('error_description') or ''))
        self._token = str(token)
        try:
            ttl = float(body.get('expires_in') or 3600)
        except (TypeError, ValueError):
            ttl = 3600.0
        self._token_until = now + max(30.0, ttl - 60.0)
        return self._token

    # ── one operation ──
    def call(self, method, path, op, params=None, body=None):
        """Send one SP-API operation and return its JSON body (a dict).
        401/403: the access token is renewed once and the request repeated.
        429/5xx: retried with backoff, at most max_retries times."""
        url = self.config.endpoint + path
        refreshed = False
        attempt = 0
        while True:
            self._pace(op)
            headers = {'x-amz-access-token': self.access_token(), 'user-agent': USER_AGENT,
                       'accept': 'application/json'}
            if body is not None:
                headers['content-type'] = 'application/json'
            resp = self._send(method, url, params=params, json=body, headers=headers)
            status = resp.status_code
            if 200 <= status < 300:
                return _json_dict(resp)
            if status in (401, 403) and not refreshed:
                refreshed = True
                self.access_token(force=True)
                continue
            if status in RETRY_STATUSES and attempt < self.max_retries:
                self._backoff(attempt, resp)
                attempt += 1
                continue
            code, message = _first_error(resp)
            raise SpApiError(op, status, code, self._scrub(message))

    # ── FBA inventory ──
    def inventory_summaries(self, details=True, max_pages=400):
        """Every FBA inventory summary in the marketplace, all pages joined."""
        mp = self.config.marketplace_id
        base = {'details': 'true' if details else 'false', 'granularityType': 'Marketplace',
                'granularityId': mp, 'marketplaceIds': mp}
        out = []
        token = None
        for _ in range(int(max_pages)):
            params = dict(base)
            if token:
                params['nextToken'] = token
            body = self.call('GET', '/fba/inventory/v1/summaries', 'getInventorySummaries', params=params)
            out.extend((body.get('payload') or {}).get('inventorySummaries') or [])
            token = (body.get('pagination') or {}).get('nextToken')
            if not token:
                return out
        raise SpApiError('getInventorySummaries', 0, 'too_many_pages', f'stopped after {max_pages} pages')

    # ── reports ──
    def create_report(self, report_type, start=None, end=None, options=None):
        body = {'reportType': str(report_type), 'marketplaceIds': [self.config.marketplace_id]}
        if start is not None:
            body['dataStartTime'] = _iso(start)
        if end is not None:
            body['dataEndTime'] = _iso(end)
        if options:
            body['reportOptions'] = {str(k): str(v) for k, v in dict(options).items()}
        rid = self.call('POST', '/reports/2021-06-30/reports', 'createReport', body=body).get('reportId')
        if not rid:
            raise SpApiError('createReport', 0, 'no_report_id', str(report_type))
        return str(rid)

    def get_report(self, report_id):
        return self.call('GET', '/reports/2021-06-30/reports/' + _seg(report_id), 'getReport')

    def get_report_document(self, document_id):
        return self.call('GET', '/reports/2021-06-30/documents/' + _seg(document_id), 'getReportDocument')

    def download_document(self, document):
        """The report file behind a document's pre-signed URL, unzipped. No SP-API token
        is sent to that URL (it is a plain S3 link)."""
        url = str((document or {}).get('url') or '')
        if not url.startswith('https://'):
            raise SpApiError('downloadReport', 0, 'bad_url')
        resp = None
        for attempt in range(self.max_retries + 1):
            resp = self._send('GET', url, headers={'user-agent': USER_AGENT}, timeout=max(self.timeout, 120))
            if resp.status_code in RETRY_STATUSES and attempt < self.max_retries:
                self._backoff(attempt, resp)
                continue
            break
        if resp.status_code != 200:
            raise SpApiError('downloadReport', resp.status_code, 'download_failed')
        data = resp.content or b''
        if str(document.get('compressionAlgorithm') or '').upper() == 'GZIP':
            data = gzip.decompress(data)
        return data

    def run_report(self, report_type, start=None, end=None, options=None,
                   poll_every=20.0, timeout=1800.0):
        """Request a report and wait for it. Returns the file's bytes, or None when Amazon
        CANCELLED it (it does that when the range holds no data). FATAL or too slow raises."""
        report_id = self.create_report(report_type, start, end, options)
        deadline = self._clock() + float(timeout)
        while True:
            rep = self.get_report(report_id)
            status = str(rep.get('processingStatus') or '')
            if status == 'DONE':
                return self.download_document(self.get_report_document(rep.get('reportDocumentId')))
            if status == 'CANCELLED':
                return None
            if status == 'FATAL':
                raise SpApiError('getReport', 0, 'report_fatal', str(report_type))
            if self._clock() >= deadline:
                raise SpApiError('getReport', 0, 'report_timeout', str(report_type))
            self._sleep(float(poll_every))

    # ── catalog ──
    def catalog_items(self, asins, included=('relationships', 'summaries')):
        """{ASIN: {'parent', 'title', 'brand', 'color', 'size'}} for every ASIN Amazon
        returns. 'parent' is the variation parent ASIN, None for a standalone listing."""
        clean, seen = [], set()
        for a in asins or []:
            a = str(a or '').strip().upper()
            if a and a not in seen:
                seen.add(a)
                clean.append(a)
        mp = self.config.marketplace_id
        out = {}
        for i in range(0, len(clean), CATALOG_BATCH):
            chunk = clean[i:i + CATALOG_BATCH]
            body = self.call('GET', '/catalog/2022-04-01/items', 'searchCatalogItems', params={
                'identifiers': ','.join(chunk), 'identifiersType': 'ASIN', 'marketplaceIds': mp,
                'includedData': ','.join(included), 'pageSize': CATALOG_BATCH})
            for item in body.get('items') or []:
                asin = str(item.get('asin') or '').strip().upper()
                if asin:
                    out[asin] = _catalog_row(item, mp)
        return out


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────
def parse_tsv(data):
    """An Amazon flat-file report (tab separated, header row first) as a list of dicts.
    UTF-8 with or without BOM, falling back to cp1252. Quotes are kept as typed."""
    if data is None:
        return []
    if isinstance(data, (bytes, bytearray)):
        raw = bytes(data)
        try:
            text = raw.decode('utf-8-sig')
        except UnicodeDecodeError:
            text = raw.decode('cp1252', errors='replace')
    else:
        text = str(data)
    rows = list(csv.reader(io.StringIO(text), delimiter='\t', quoting=csv.QUOTE_NONE))
    if not rows:
        return []
    head = [h.strip() for h in rows[0]]
    out = []
    for r in rows[1:]:
        if not any(c.strip() for c in r):
            continue
        out.append({head[i]: (r[i].strip() if i < len(r) else '') for i in range(len(head)) if head[i]})
    return out


def _catalog_row(item, marketplace_id):
    parent = None
    for block in item.get('relationships') or []:
        if block.get('marketplaceId') not in (None, marketplace_id):
            continue
        for rel in block.get('relationships') or []:
            parents = rel.get('parentAsins') or []
            if str(rel.get('type') or '').upper() == 'VARIATION' and parents:
                parent = str(parents[0]).strip().upper()
                break
        if parent:
            break
    summary = {}
    for s in item.get('summaries') or []:
        if s.get('marketplaceId') in (None, marketplace_id):
            summary = s
            break
    return {'parent': parent, 'title': str(summary.get('itemName') or ''),
            'brand': str(summary.get('brand') or ''), 'color': str(summary.get('color') or ''),
            'size': str(summary.get('size') or '')}


def _json_dict(resp):
    try:
        body = resp.json()
    except Exception:
        return {}
    return body if isinstance(body, dict) else {}


def _first_error(resp):
    body = _json_dict(resp)
    errs = body.get('errors')
    if isinstance(errs, list) and errs and isinstance(errs[0], dict):
        e = errs[0]
        message = str(e.get('message') or '')
        details = str(e.get('details') or '')
        if details and details not in message:
            message = (message + ' ' + details).strip()
        return str(e.get('code') or f'http_{resp.status_code}'), message
    return f'http_{resp.status_code}', ''


def _header(resp, name):
    if resp is None:
        return None
    for k, v in (getattr(resp, 'headers', None) or {}).items():
        if str(k).lower() == name:
            return v
    return None


def _iso(value):
    if isinstance(value, datetime):
        if value.tzinfo is not None:
            value = value.astimezone(timezone.utc).replace(tzinfo=None)
        return value.strftime('%Y-%m-%dT%H:%M:%SZ')
    return str(value)


def _seg(value):
    return quote(str(value or ''), safe='')
