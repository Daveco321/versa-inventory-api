"""amazon_spapi.py: the SP-API client behind the Amazon agent (David, Nov 23 2026).

Offline: the HTTP session, the clock and sleep are fakes. Every credential below is a
made-up placeholder, and the tests check none of them ever reaches an error message."""
import gzip
import json
import os
import sys
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

import amazon_spapi as A  # noqa: E402

CLIENT_ID = 'amzn1.application-oa2-client.TESTONLY'
SECRET = 'test-client-secret-DO-NOT-LEAK'
REFRESH = 'Atzr|test-refresh-token-DO-NOT-LEAK'


class FakeResponse:
    def __init__(self, status=200, body=None, headers=None, content=None):
        self.status_code = status
        self._body = body
        self.headers = headers or {}
        if content is None and body is not None:
            content = json.dumps(body).encode('utf-8')
        self.content = content or b''

    def json(self):
        if self._body is None:
            raise ValueError('no json')
        return self._body


class FakeSession:
    """Answers through handler(method, url, kw) and records every request."""

    def __init__(self, handler):
        self.handler = handler
        self.requests = []

    def request(self, method, url, **kw):
        self.requests.append((method, url, kw))
        return self.handler(method, url, kw)


class FakeClock:
    def __init__(self):
        self.t = 1000.0
        self.slept = []

    def __call__(self):
        return self.t

    def sleep(self, d):
        self.slept.append(d)
        self.t += d


def token_ok(n=[0]):
    n[0] += 1
    return FakeResponse(200, {'access_token': f'Atza|access-{n[0]}', 'expires_in': 3600, 'token_type': 'bearer'})


def make_client(handler, max_retries=3):
    clock = FakeClock()
    session = FakeSession(handler)
    cfg = A.SpApiConfig(CLIENT_ID, SECRET, REFRESH)
    client = A.SpApiClient(cfg, session=session, sleep=clock.sleep, clock=clock,
                           rng=__import__('random').Random(7), max_retries=max_retries)
    return client, session, clock


def is_token(url):
    return url == A.TOKEN_URL


def requests_to(session, needle):
    return [r for r in session.requests if needle in r[1]]


class ConfigTests(unittest.TestCase):

    def test_missing_values_are_named_never_shown(self):
        cfg, reason = A.SpApiConfig.from_env({A.ENV_CLIENT_ID: CLIENT_ID})
        self.assertIsNone(cfg)
        self.assertIn(A.ENV_CLIENT_SECRET, reason)
        self.assertIn(A.ENV_REFRESH_TOKEN, reason)
        self.assertNotIn(CLIENT_ID, reason)

    def test_full_env_builds_a_config_whose_repr_hides_secrets(self):
        cfg, reason = A.SpApiConfig.from_env({A.ENV_CLIENT_ID: CLIENT_ID, A.ENV_CLIENT_SECRET: SECRET,
                                              A.ENV_REFRESH_TOKEN: REFRESH})
        self.assertEqual('', reason)
        self.assertEqual(A.ENDPOINT_NA, cfg.endpoint)
        self.assertEqual(A.MARKETPLACE_US, cfg.marketplace_id)
        for secret in (SECRET, REFRESH, CLIENT_ID):
            self.assertNotIn(secret, repr(cfg))

    def test_only_amazon_hosts_can_receive_a_token(self):
        env = {A.ENV_CLIENT_ID: CLIENT_ID, A.ENV_CLIENT_SECRET: SECRET, A.ENV_REFRESH_TOKEN: REFRESH,
               A.ENV_ENDPOINT: 'https://sellingpartnerapi-na.amazon.com.evil.example'}
        cfg, reason = A.SpApiConfig.from_env(env)
        self.assertEqual(A.ENDPOINT_NA, cfg.endpoint)
        self.assertIn('ignored', reason)
        env[A.ENV_ENDPOINT] = 'https://sandbox.sellingpartnerapi-na.amazon.com/'
        cfg, reason = A.SpApiConfig.from_env(env)
        self.assertEqual('https://sandbox.sellingpartnerapi-na.amazon.com', cfg.endpoint)
        self.assertEqual('', reason)


class TokenTests(unittest.TestCase):

    def test_token_is_cached_then_renewed_after_expiry(self):
        def handler(method, url, kw):
            if is_token(url):
                return token_ok()
            return FakeResponse(200, {'payload': {'inventorySummaries': []}})
        client, session, clock = make_client(handler)
        client.inventory_summaries()
        client.inventory_summaries()
        self.assertEqual(1, len(requests_to(session, 'auth/o2/token')))
        clock.t += 3600
        client.inventory_summaries()
        self.assertEqual(2, len(requests_to(session, 'auth/o2/token')))
        grant = requests_to(session, 'auth/o2/token')[0][2]['data']
        self.assertEqual('refresh_token', grant['grant_type'])

    def test_lwa_refusal_never_leaks_a_secret(self):
        def handler(method, url, kw):
            return FakeResponse(400, {'error': 'invalid_grant',
                                      'error_description': f'bad refresh_token {REFRESH} for {SECRET}'})
        client, _s, _c = make_client(handler)
        with self.assertRaises(A.SpApiError) as ctx:
            client.access_token()
        err = ctx.exception
        self.assertEqual('invalid_grant', err.code)
        self.assertEqual(400, err.status)
        for secret in (REFRESH, SECRET):
            self.assertNotIn(secret, str(err))
            self.assertNotIn(secret, err.message)


class CallTests(unittest.TestCase):

    def test_inventory_joins_every_page_with_the_marketplace_params(self):
        pages = {None: {'payload': {'inventorySummaries': [{'sellerSku': 'A-1'}]}, 'pagination': {'nextToken': 'T2'}},
                 'T2': {'payload': {'inventorySummaries': [{'sellerSku': 'A-2'}, {'sellerSku': 'A-3'}]}}}

        def handler(method, url, kw):
            if is_token(url):
                return token_ok()
            return FakeResponse(200, pages[(kw.get('params') or {}).get('nextToken')])
        client, session, _c = make_client(handler)
        rows = client.inventory_summaries()
        self.assertEqual(['A-1', 'A-2', 'A-3'], [r['sellerSku'] for r in rows])
        calls = requests_to(session, '/fba/inventory/v1/summaries')
        self.assertEqual(2, len(calls))
        first = calls[0][2]
        self.assertEqual(A.MARKETPLACE_US, first['params']['marketplaceIds'])
        self.assertEqual(A.MARKETPLACE_US, first['params']['granularityId'])
        self.assertEqual('true', first['params']['details'])
        self.assertTrue(first['headers']['x-amz-access-token'].startswith('Atza|'))
        self.assertEqual('T2', calls[1][2]['params']['nextToken'])
        sent = json.dumps([r[2].get('headers') for r in calls])
        self.assertNotIn(SECRET, sent)
        self.assertNotIn(REFRESH, sent)

    def test_throttled_call_backs_off_then_succeeds(self):
        answers = [FakeResponse(429, {'errors': [{'code': 'QuotaExceeded', 'message': 'You exceeded your quota'}]},
                                headers={'Retry-After': '3'}),
                   FakeResponse(200, {'payload': {'inventorySummaries': [{'sellerSku': 'X'}]}})]

        def handler(method, url, kw):
            return token_ok() if is_token(url) else answers.pop(0)
        client, _s, clock = make_client(handler)
        rows = client.inventory_summaries()
        self.assertEqual(['X'], [r['sellerSku'] for r in rows])
        self.assertTrue(any(s >= 3.0 for s in clock.slept), clock.slept)

    def test_throttling_that_never_ends_raises_after_the_retries(self):
        def handler(method, url, kw):
            if is_token(url):
                return token_ok()
            return FakeResponse(429, {'errors': [{'code': 'QuotaExceeded', 'message': 'slow down'}]})
        client, session, _c = make_client(handler, max_retries=2)
        with self.assertRaises(A.SpApiError) as ctx:
            client.inventory_summaries()
        self.assertEqual(429, ctx.exception.status)
        self.assertEqual('QuotaExceeded', ctx.exception.code)
        self.assertEqual(3, len(requests_to(session, '/fba/inventory/')))

    def test_a_refused_token_is_renewed_once(self):
        answers = [FakeResponse(403, {'errors': [{'code': 'Unauthorized', 'message': 'Access denied',
                                                   'details': 'The access token you provided has expired.'}]}),
                   FakeResponse(200, {'payload': {'inventorySummaries': []}})]

        def handler(method, url, kw):
            return token_ok() if is_token(url) else answers.pop(0)
        client, session, _c = make_client(handler)
        client.inventory_summaries()
        self.assertEqual(2, len(requests_to(session, 'auth/o2/token')))

    def test_a_missing_role_is_reported_with_amazons_words(self):
        def handler(method, url, kw):
            if is_token(url):
                return token_ok()
            return FakeResponse(403, {'errors': [{'code': 'Unauthorized',
                                                  'message': 'Access to requested resource is denied.'}]})
        client, session, _c = make_client(handler)
        with self.assertRaises(A.SpApiError) as ctx:
            client.inventory_summaries()
        self.assertEqual(403, ctx.exception.status)
        self.assertIn('denied', ctx.exception.message)
        self.assertEqual(2, len(requests_to(session, '/fba/inventory/')))   # one renewal, then stop

    def test_slow_operations_are_paced(self):
        def handler(method, url, kw):
            return token_ok() if is_token(url) else FakeResponse(200, {'items': []})
        client, _s, clock = make_client(handler)
        for _ in range(3):      # searchCatalogItems: 2 per second, burst 2
            client.call('GET', '/catalog/2022-04-01/items', 'searchCatalogItems', params={})
        self.assertEqual(1, len(clock.slept))
        self.assertAlmostEqual(0.5, clock.slept[0], places=3)


class ReportTests(unittest.TestCase):

    def report_handler(self, statuses, tsv=b'', compressed=True):
        statuses = list(statuses)

        def handler(method, url, kw):
            if is_token(url):
                return token_ok()
            if url.endswith('/reports/2021-06-30/reports') and method == 'POST':
                return FakeResponse(202, {'reportId': '5550001'})
            if '/reports/2021-06-30/reports/5550001' in url:
                st = statuses.pop(0)
                body = {'reportId': '5550001', 'processingStatus': st}
                if st == 'DONE':
                    body['reportDocumentId'] = 'amzn1.spdoc.1.4.na.TEST'
                return FakeResponse(200, body)
            if '/reports/2021-06-30/documents/' in url:
                doc = {'reportDocumentId': 'amzn1.spdoc.1.4.na.TEST', 'url': 'https://s3.example.test/report?sig=1'}
                if compressed:
                    doc['compressionAlgorithm'] = 'GZIP'
                return FakeResponse(200, doc)
            if url.startswith('https://s3.example.test/'):
                return FakeResponse(200, content=gzip.compress(tsv) if compressed else tsv)
            raise AssertionError(url)
        return handler

    def test_done_report_is_downloaded_unzipped_and_parsed(self):
        tsv = '﻿seller-sku\tasin1\titem-name\nAMNASU201SLS-M-FBA\tB0TEST0001\tNautica "Slim" Shirt\n'.encode('utf-8')
        client, session, clock = make_client(self.report_handler(['IN_QUEUE', 'IN_PROGRESS', 'DONE'], tsv))
        data = client.run_report(A.REPORT_ALL_LISTINGS, poll_every=15)
        rows = A.parse_tsv(data)
        self.assertEqual([{'seller-sku': 'AMNASU201SLS-M-FBA', 'asin1': 'B0TEST0001',
                           'item-name': 'Nautica "Slim" Shirt'}], rows)
        self.assertEqual([15.0, 15.0], [s for s in clock.slept if s == 15.0])
        create = [r for r in session.requests if r[0] == 'POST' and 'reports' in r[1]][0][2]
        self.assertEqual(A.REPORT_ALL_LISTINGS, create['json']['reportType'])
        self.assertEqual([A.MARKETPLACE_US], create['json']['marketplaceIds'])
        download = requests_to(session, 's3.example.test')[0][2]
        self.assertNotIn('x-amz-access-token', download['headers'])

    def test_report_dates_go_out_as_utc(self):
        from datetime import datetime, timedelta, timezone
        client, session, _c = make_client(self.report_handler(['DONE'], b'a\n1\n', compressed=False))
        et = timezone(timedelta(hours=-5))
        client.run_report(A.REPORT_ORDERS_BY_DATE, start=datetime(2026, 10, 24, 0, 0, tzinfo=et),
                          end=datetime(2026, 11, 23, 0, 0, tzinfo=et))
        body = [r for r in session.requests if r[0] == 'POST' and 'reports' in r[1]][0][2]['json']
        self.assertEqual('2026-10-24T05:00:00Z', body['dataStartTime'])
        self.assertEqual('2026-11-23T05:00:00Z', body['dataEndTime'])

    def test_cancelled_report_means_no_data(self):
        client, _s, _c = make_client(self.report_handler(['IN_QUEUE', 'CANCELLED']))
        self.assertIsNone(client.run_report(A.REPORT_RESTOCK))

    def test_fatal_report_raises(self):
        client, _s, _c = make_client(self.report_handler(['FATAL']))
        with self.assertRaises(A.SpApiError) as ctx:
            client.run_report(A.REPORT_RESTOCK)
        self.assertEqual('report_fatal', ctx.exception.code)

    def test_slow_report_times_out(self):
        client, _s, _c = make_client(self.report_handler(['IN_PROGRESS'] * 50))
        with self.assertRaises(A.SpApiError) as ctx:
            client.run_report(A.REPORT_RESTOCK, poll_every=60, timeout=300)
        self.assertEqual('report_timeout', ctx.exception.code)


class ParseTests(unittest.TestCase):

    def test_cp1252_files_and_blank_lines(self):
        raw = b'sku\tname\nAMUSCD179RFB-L\tShirt \x96 Regular\n\t\n'     # 0x96 = en dash in cp1252
        rows = A.parse_tsv(raw)
        self.assertEqual(1, len(rows))
        self.assertEqual('Shirt – Regular', rows[0]['name'])

    def test_short_rows_fill_blanks(self):
        rows = A.parse_tsv(b'a\tb\tc\n1\t2\n')
        self.assertEqual({'a': '1', 'b': '2', 'c': ''}, rows[0])

    def test_nothing_in_nothing_out(self):
        self.assertEqual([], A.parse_tsv(None))
        self.assertEqual([], A.parse_tsv(b''))


class CatalogTests(unittest.TestCase):

    def test_batches_of_twenty_and_parent_asins(self):
        asins = [f'B0TEST{n:04d}' for n in range(25)] + ['b0test0003', '']

        def handler(method, url, kw):
            if is_token(url):
                return token_ok()
            ids = kw['params']['identifiers'].split(',')
            items = []
            for a in ids:
                item = {'asin': a, 'summaries': [{'marketplaceId': A.MARKETPLACE_US, 'itemName': f'Shirt {a}',
                                                  'brand': 'Nautica', 'color': 'White', 'size': 'Medium'}]}
                if a != 'B0TEST0024':
                    item['relationships'] = [{'marketplaceId': A.MARKETPLACE_US, 'relationships': [
                        {'type': 'VARIATION', 'parentAsins': ['B0PARENT01']}]}]
                items.append(item)
            return FakeResponse(200, {'numberOfResults': len(items), 'items': items})
        client, session, _c = make_client(handler)
        out = client.catalog_items(asins)
        calls = requests_to(session, '/catalog/2022-04-01/items')
        self.assertEqual([20, 5], [len(c[2]['params']['identifiers'].split(',')) for c in calls])
        self.assertEqual(25, len(out))
        self.assertEqual('B0PARENT01', out['B0TEST0003']['parent'])
        self.assertIsNone(out['B0TEST0024']['parent'])
        self.assertEqual('Nautica', out['B0TEST0003']['brand'])
        self.assertEqual('relationships,summaries', calls[0][2]['params']['includedData'])


if __name__ == '__main__':
    unittest.main()
