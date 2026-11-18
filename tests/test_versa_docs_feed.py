"""Versa Docs production feed (Nov 18 2026): the online Style Ledger replaces the
Dropbox master for production rows and is the only source of incoming once its
switch is on; FOB landing lines become FOB Pickup lots with no arrival date.
Offline: sockets are blocked (tests/_support.py), the feed lives in a temp folder
and the Versa Docs call is a fake HTTP client. Every style, factory and number
here is synthetic (ZZ prefixes)."""
import json
import os
import shutil
import sys
import tempfile
import unittest
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _support import load_app  # noqa: E402


def line(prod, style, units, wh='TR', etd='2026-12-20', eta=None, brand='NAUTICA', po='ZZ TEST PO', ship=None):
    return {'prefix': prod[:2], 'production': prod, 'po_name': po, 'style': style, 'brand': brand,
            'po_units': units, 'ship_units': None, 'units': units, 'shipment': ship, 'etd': etd, 'etd_col': etd,
            'ex_factory': None, 'eta': eta, 'warehouse': wh, 'orange': False, 'sheet': 'Style_Ledger',
            'rowno': 2, 'row_id': 'r-' + prod + style}


def feed(lines, on=True, version='v1'):
    return {'source': 'versa-docs', 'generated_at': '2026-11-18T22:00:00Z', 'version': version,
            'platform_on': on, 'count': len(lines), 'lines': lines}


def item(sku, wh=0, incoming=0, committed=0, allocated=0, brand='NAUTICA'):
    return {'sku': sku, 'brand': brand, 'brand_abbr': brand, 'brand_full': brand, 'name': brand + ' ' + sku,
            'jtw': wh, 'tr': 0, 'dcw': 0, 'qa': 0, 'nj': 0, 'abfi': 0, 'incoming': incoming,
            'committed': committed, 'allocated': allocated, 'total_ats': wh + incoming + committed + allocated,
            'total_warehouse': wh, 'container': 'N/A', 'receive_date': '', 'lot_number': 'N/A', 'image': ''}


# 210 filler lines so a feed passes the 200-line floor
FILLER = [line('TF99%03d' % i, 'ZZNAFL%03dSLS' % i, 100) for i in range(210)]


class PureFeedTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.a = load_app()
        cls.f = sys.modules['versa_docs_feed']

    def test_validate(self):
        f = self.f
        good = feed(FILLER)
        self.assertEqual(f.validate(good), (True, 'ok'))
        self.assertFalse(f.validate(feed(FILLER[:50]))[0])                     # too small
        self.assertFalse(f.validate({'lines': 'x'})[0])
        bad = feed(FILLER + [{'style': ''}])
        self.assertFalse(f.validate(bad)[0])
        smaller = feed(FILLER[:140] + [line('TF99999', 'ZZNAFL999SLS', 100)] * 60)
        self.assertTrue(f.validate(smaller, good)[0])                           # same size
        shrunk = feed(FILLER[:205])
        shrunk['lines'] = [dict(l, units=10) for l in shrunk['lines']]
        ok, why = f.validate(shrunk, good)                                       # units fell 90%
        self.assertFalse(ok)
        self.assertIn('units_dropped', why)

    def test_tuples_and_rows(self):
        a, f = self.a, self.f
        fd = feed([line('TF99001', 'ZZNAPQ001SLD', 5004, wh='JT', etd='2026-12-20'),
                   line('TF99002', 'ZZNAPQ002SLD', 2016, wh='TR', etd='2026-12-01', eta='2027-01-10'),
                   line('TF99003', 'ZZNAPQ003SLD', 1200, wh='FOB', etd='2026-12-05', eta='2027-01-20'),
                   line('TF99004', 'ZZNAPQ004SLD', 300, wh=None, etd=None)])
        rows = [a._production_row_from_ledger(t) for t in f.ledger_tuples(fd)]
        r1, r2, r3, r4 = rows
        self.assertEqual((r1['production'], r1['style'], r1['units'], r1['warehouse']), ('TF99001', 'ZZNAPQ001SLD', 5004, 'JTW'))
        self.assertEqual((r1['etd'], r1['arrival'], r1['port_dated'], r1['fob_flag'], r1['fob_pickup']),
                         ('2026-12-20', None, False, False, False))
        # a port date works exactly as column G of the master: etd = G - 27, arrival = G + 10
        self.assertEqual((r2['etd'], r2['etd_raw'], r2['arrival'], r2['port_dated']),
                         ('2026-12-14', '2026-12-01', '2027-01-20', True))
        # FOB pickup: no working dates (so no screen works out an arrival), FOB-first in routing,
        # and the ledger's own ETD (not the port backdate) kept in etd_raw
        self.assertEqual((r3['etd'], r3['etd_raw'], r3['arrival'], r3['port_dated'], r3['fob_flag'], r3['fob_note'],
                          r3['fob_pickup'], r3['warehouse']), (None, '2026-12-05', None, False, True, 'FOB Pickup', True, 'FOB'))
        self.assertIsNone(a._apo_prod_arrival(r3, False))
        self.assertIsNotNone(a._apo_prod_arrival(r1, False))
        self.assertEqual((r4['etd'], r4['arrival'], r4['warehouse']), (None, None, ''))

    def test_fob_pickup_from_the_dropbox_master_too(self):
        a = self.a
        r = a._production_row_from_ledger(('TF99005', 'ZZ', 'ZZNAPQ005SLD', 600, 'NAUTICA', datetime(2026, 12, 9),
                                           None, None, 'fob'))
        self.assertEqual((r['etd'], r['etd_raw'], r['arrival'], r['fob_flag'], r['fob_note'], r['fob_pickup']),
                         (None, '2026-12-09', None, True, 'FOB Pickup', True))
        plain = a._production_row_from_ledger(('TF99006', 'ZZ', 'ZZNAPQ006SLD', 600, 'NAUTICA', datetime(2026, 12, 9),
                                               None, None, 'TR'))
        self.assertFalse(plain['fob_pickup'])
        self.assertFalse(plain['fob_flag'])

    def test_apply_incoming(self):
        a, f = self.a, self.f
        fd = feed([line('TF99001', 'ZZNAPQ001SLD', 5004), line('TF99002', 'ZZNAPQ001SLD', 996),
                   line('TF99003', 'ZZNAPQ003SLD', 1200),
                   line('TF99004', 'ZZDKPK777SLS', 900, brand='DKNY')])
        items = [item('ZZNAPQ001SLD', wh=50, incoming=3000, committed=-100),   # 3000 -> 6000
                 item('ZZNAPQ002SLD', incoming=800),                           # only incoming, gone from the ledger
                 item('ZZNAPQ002SLX', incoming=800, allocated=-200),           # gone too, but has an allocation
                 item('ZZNAPQ003SLD', wh=10, incoming=1200),                   # unchanged
                 item('ZZPLAIN001SLS', wh=40)]                                 # stock only
        before = json.dumps(items, sort_keys=True)
        out, rep = f.apply_incoming(items, f.incoming_by_style(fd), f.brand_by_style(fd), a._vd_new_item, a._vd_item_brand)
        self.assertEqual(json.dumps(items, sort_keys=True), before)            # inputs untouched
        by = {i['sku']: i for i in out}
        self.assertEqual((by['ZZNAPQ001SLD']['incoming'], by['ZZNAPQ001SLD']['total_ats']), (6000, 50 + 6000 - 100))
        self.assertNotIn('ZZNAPQ002SLD', by)
        self.assertEqual((by['ZZNAPQ002SLX']['incoming'], by['ZZNAPQ002SLX']['total_ats']), (0, -200))
        self.assertEqual((by['ZZNAPQ003SLD']['incoming'], by['ZZNAPQ003SLD']['total_ats']), (1200, 1210))
        self.assertEqual(by['ZZPLAIN001SLS']['total_ats'], 40)
        new = by['ZZDKPK777SLS']                                               # ledger-only style is added
        self.assertEqual((new['brand'], new['incoming'], new['total_ats'], new['total_warehouse']), ('DKNY', 900, 900, 0))
        self.assertEqual((rep['added'], rep['dropped'], rep['incoming_before'], rep['incoming_after']),
                         (1, 1, 3000 + 800 + 800 + 1200, 6000 + 1200 + 900))
        self.assertEqual(rep['dropped_styles'], ['ZZNAPQ002SLD'])

    def test_new_styles_join_the_brand_the_platform_already_uses(self):
        a, f = self.a, self.f
        items = [item('ZZRBPK001SLS', incoming=10, brand='RB'), item('ZZRBPK002SLS', wh=5, brand='RB')]
        fd = feed([line('TF99001', 'ZZRBPK009SLS', 700, brand='REEBOK'), line('TF99002', 'ZZNAPK009SLS', 50)])
        out, rep = f.apply_incoming(items, f.incoming_by_style(fd), f.brand_by_style(fd), a._vd_new_item,
                                    a._vd_brand_fix_for(items))
        by = {i['sku']: i for i in out}
        self.assertEqual(by['ZZRBPK009SLS']['brand'], 'RB')          # not a second REEBOK group
        self.assertEqual(by['ZZNAPK009SLS']['brand'], 'NAUTICA')     # no item with NA yet: the style number decides

    def test_placeholder_styles_and_unguessable_codes_are_not_added(self):
        a, f = self.a, self.f
        items = [item('ZZBLPK001SLS', incoming=10, brand='BLO'), item('ZZBLPK002SLS', wh=5, brand='BLO')]
        fd = feed([line('TF99001', 'TBD', 3000), line('TF99002', 'SEE PO', 100),
                   line('TF99003', 'ZZBLPK009SLS', 700, brand='BLACK')])
        out, rep = f.apply_incoming(items, f.incoming_by_style(fd), f.brand_by_style(fd), a._vd_new_item,
                                    a._vd_brand_fix_for(items), a._vd_style_ok)
        by = {i['sku']: i for i in out}
        self.assertNotIn('TBD', by)
        self.assertNotIn('SEE PO', by)
        self.assertEqual(by['ZZBLPK009SLS']['brand'], 'BLACK')

    def test_etd_text_markers_survive_the_feed(self):
        a, f = self.a, self.f
        l1 = line('TF99001', 'ZZNAPQ001SLD', 500, etd=None)
        l1.update(etd_col=None, etd_text='FOB AS READY', ex_factory='2026-12-01')
        l2 = line('TF99002', 'ZZNAPQ002SLD', 500, etd='2026-12-01')
        l2.update(etd_col=None, ex_factory='2026-12-01')
        r1, r2 = [a._production_row_from_ledger(t) for t in f.ledger_tuples(feed([l1, l2]))]
        self.assertEqual((r1['fob_flag'], r1['fob_note'], r1['etd']), (True, 'FOB AS READY', None))
        self.assertEqual((r2['fob_flag'], r2['etd']), (False, '2026-12-01'))

    def test_item_brand(self):
        a = self.a
        self.assertEqual(a._vd_item_brand('ZZNAPQ001SLD', 'whatever'), 'NAUTICA')     # the style number decides
        self.assertEqual(a._vd_item_brand('BUDKPK001SLS', 'DKNY'), 'DKNY')
        self.assertEqual(a._vd_item_brand('ODDSTYLE', 'Vince Camuto'), 'VINCE')
        self.assertEqual(a._vd_item_brand('ODDSTYLE', 'chaps'), 'CHAPS')


class FakeResp:
    def __init__(self, status, body):
        self.status_code = status
        self._body = body
        self.text = json.dumps(body)

    def json(self):
        return self._body


class FakeHttp:
    def __init__(self):
        self.calls = []
        self.answer = None

    def post(self, url, headers=None, json=None, timeout=None):
        self.calls.append({'url': url, 'headers': headers, 'json': json})
        return self.answer(json or {}) if callable(self.answer) else self.answer


class FeedRouteTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.a = load_app()
        cls.f = sys.modules['versa_docs_feed']
        cls.client = cls.a.app.test_client()

    def setUp(self):
        a = self.a
        self.dir = tempfile.mkdtemp(prefix='vdfeed-')
        self.f.FEED_DIR = self.dir
        self.http = FakeHttp()
        self.ident = {'tier': 'staff', 'is_admin': False, 'email': 'zz@example.test'}
        self._saved = (a.http_requests, a._request_identity, a._group_by_brand, a.load_factory_holds_from_s3,
                       a._VD_MIN_GAP, a.get_dropbox_token)
        a.http_requests = self.http
        a._request_identity = lambda: self.ident
        a._group_by_brand = lambda items: {}
        a.load_factory_holds_from_s3 = lambda force=False: frozenset()
        a._VD_MIN_GAP = 0
        a.get_dropbox_token = lambda: None                    # the Dropbox master is never read here
        os.environ.pop('VD_PRODUCTION_SOURCE', None)
        with a._vd_lock:
            a._vd_state.update(stamp=None, feed=None, version=None, platform_on=False, count=0, units=0,
                               rows_all=None, rows_version=None, by_style={}, brands={}, error='',
                               last_refresh=None, last_fetch_at=0.0, items_report=None)
        self.master = [a._production_row_from_ledger(('TF99901', 'ZZ', 'ZZNAPQ001SLD', 1111, 'NAUTICA',
                                                      datetime(2026, 12, 1), None, None, 'TR'))]
        with a._production_lock:
            a._production_data_all = list(self.master)
            a._production_data = list(self.master)
            a._production_last_sync = 10 ** 12
            a._production_rev = 'dropbox-rev'
        self.items = [item('ZZNAPQ001SLD', wh=50, incoming=1111), item('ZZPLAIN001SLS', wh=40)]
        with a._inv_lock:
            a._inventory['items'] = [dict(i) for i in self.items]
            a._inventory['items_raw'] = [dict(i) for i in self.items]
            a._inventory['item_count'] = len(self.items)
            a._inventory['hold_stamp'] = '|vd:off'
            a._inventory['hold_report'] = None

    def tearDown(self):
        a = self.a
        (a.http_requests, a._request_identity, a._group_by_brand, a.load_factory_holds_from_s3,
         a._VD_MIN_GAP, a.get_dropbox_token) = self._saved
        shutil.rmtree(self.dir, ignore_errors=True)
        with a._production_lock:
            a._production_data_all = []
            a._production_data = []
            a._production_last_sync = 0
            a._production_rev = None
        with a._inv_lock:
            a._inventory['items'] = []
            a._inventory['items_raw'] = None
        with a._vd_lock:
            a._vd_state.update(stamp=None, feed=None, version=None, platform_on=False, rows_all=None,
                               rows_version=None, by_style={}, brands={}, last_fetch_at=0.0)

    def refresh(self, token='eyJ.zz.zz'):
        return self.client.post('/production/versa-docs/refresh', headers={'Authorization': 'Bearer ' + token})

    def lines(self):
        return FILLER + [line('TF99001', 'ZZNAPQ001SLD', 6000, wh='JT'),
                         line('TF99002', 'ZZDKPK777SLS', 900, brand='DKNY', wh='FOB', etd='2026-12-05')]

    def test_refresh_needs_an_office_sign_in(self):
        self.ident = {'tier': 'factory', 'prefix': 'TF'}
        r = self.refresh()
        self.assertEqual(r.status_code, 401)
        self.assertEqual(self.http.calls, [])

    def test_switch_off_changes_nothing(self):
        a = self.a
        self.http.answer = FakeResp(200, feed(self.lines(), on=False, version='v-off'))
        r = self.refresh()
        self.assertEqual(r.status_code, 200)
        body = r.get_json()
        self.assertTrue(body['changed'])
        self.assertFalse(body['active'])
        # the caller's own token went to Versa Docs, with the publishable key
        call = self.http.calls[0]
        self.assertTrue(call['url'].endswith('/rest/v1/rpc/ledger_platform_feed'))
        self.assertEqual(call['headers']['Authorization'], 'Bearer eyJ.zz.zz')
        self.assertEqual(call['json'], {'p_known': None})
        self.assertEqual([p['production'] for p in a.load_production_from_dropbox()], ['TF99901'])
        a._hold_tick()
        with a._inv_lock:
            self.assertEqual({i['sku']: i['incoming'] for i in a._inventory['items']}, {'ZZNAPQ001SLD': 1111, 'ZZPLAIN001SLS': 0})

    def test_switch_on_takes_over_and_off_gives_back(self):
        a = self.a
        self.http.answer = FakeResp(200, feed(self.lines(), on=True, version='v-on'))
        r = self.refresh()
        self.assertEqual(r.status_code, 200)
        self.assertTrue(r.get_json()['active'])
        rows = a.load_production_from_dropbox()
        self.assertEqual(len(rows), 212)
        fob = next(p for p in rows if p['style'] == 'ZZDKPK777SLS')
        self.assertEqual((fob['fob_pickup'], fob['arrival'], fob['etd'], fob['etd_raw']), (True, None, None, '2026-12-05'))
        with a._inv_lock:
            by = {i['sku']: i for i in a._inventory['items']}
            stamp = a._inventory['hold_stamp']
        self.assertEqual((by['ZZNAPQ001SLD']['incoming'], by['ZZNAPQ001SLD']['total_ats']), (6000, 6050))
        self.assertEqual(by['ZZDKPK777SLS']['incoming'], 900)
        self.assertEqual(by['ZZPLAIN001SLS']['total_ats'], 40)
        self.assertEqual(stamp, '|vd:v-on')
        # the same version again: Versa Docs answers "unchanged", nothing is rewritten
        self.http.answer = lambda body: FakeResp(200, {'version': 'v-on', 'platform_on': True, 'unchanged': True})
        r = self.refresh()
        self.assertFalse(r.get_json()['changed'])
        self.assertEqual(self.http.calls[-1]['json'], {'p_known': 'v-on'})
        # the switch goes off in Versa Docs: the master is read again at once (the rows from
        # Versa Docs never count as a fresh cache) and the hourly file's incoming comes back
        asked = []
        a.get_dropbox_token = lambda: asked.append(1)
        self.http.answer = FakeResp(200, feed(self.lines(), on=False, version='v-off'))
        r = self.refresh()
        self.assertFalse(r.get_json()['active'])
        a.load_production_from_dropbox()
        self.assertTrue(asked)
        with a._inv_lock:
            self.assertEqual({i['sku']: i['incoming'] for i in a._inventory['items']}, {'ZZNAPQ001SLD': 1111, 'ZZPLAIN001SLS': 0})

    def test_other_worker_follows_the_file(self):
        a, f = self.a, self.f
        f.save(feed(self.lines(), on=True, version='v-disk'))       # as if the other worker saved it
        rows = a.load_production_from_dropbox()
        self.assertEqual(len(rows), 212)
        a._hold_tick()
        with a._inv_lock:
            self.assertEqual({i['sku']: i['incoming'] for i in a._inventory['items']}['ZZNAPQ001SLD'], 6000)
        self.assertTrue(a._vd_health()['active'])

    def test_server_override_forces_off(self):
        a, f = self.a, self.f
        f.save(feed(self.lines(), on=True, version='v-disk'))
        os.environ['VD_PRODUCTION_SOURCE'] = 'off'
        try:
            self.assertEqual([p['production'] for p in a.load_production_from_dropbox()], ['TF99901'])
            self.assertFalse(a._vd_health()['active'])
        finally:
            os.environ.pop('VD_PRODUCTION_SOURCE', None)

    def test_quick_second_refresh_does_not_freeze_the_worker(self):
        import threading
        a = self.a
        a._VD_MIN_GAP = 60
        self.http.answer = FakeResp(200, feed(self.lines(), on=False, version='v-off'))
        self.assertEqual(self.refresh().status_code, 200)
        box = {}
        t = threading.Thread(target=lambda: box.setdefault('r', self.refresh()))
        t.start()
        t.join(10)
        self.assertFalse(t.is_alive(), 'the second refresh hung')
        self.assertEqual(box['r'].get_json().get('skipped'), 'refreshed moments ago')
        self.assertTrue(a._vd_lock.acquire(timeout=2))
        a._vd_lock.release()

    def test_a_big_drop_is_taken_when_switching_off_or_once_it_lasts(self):
        a = self.a
        self.http.answer = FakeResp(200, feed(self.lines(), on=True, version='v-on'))
        self.refresh()
        shrunk = [dict(l, units=1) for l in self.lines()]
        a._VD_DROP_CONFIRM_SECONDS = 3600
        self.http.answer = FakeResp(200, feed(shrunk, on=True, version='v-drop'))
        self.assertEqual(self.refresh().status_code, 422)
        self.assertEqual(a._vd_health()['version'], 'v-on')
        a._VD_DROP_CONFIRM_SECONDS = 0
        self.assertEqual(self.refresh().status_code, 200)
        self.assertEqual(a._vd_health()['version'], 'v-drop')
        a._VD_DROP_CONFIRM_SECONDS = 20 * 60
        self.http.answer = FakeResp(200, feed(self.lines(), on=True, version='v-on2'))
        self.assertEqual(self.refresh().status_code, 200)
        self.http.answer = FakeResp(200, feed(shrunk, on=False, version='v-off-drop'))
        self.assertEqual(self.refresh().status_code, 200)
        h = a._vd_health()
        self.assertEqual((h['version'], h['active']), ('v-off-drop', False))
        self.assertIsNotNone(h['last_contact'])

    def test_bad_feeds_are_refused(self):
        a = self.a
        self.http.answer = FakeResp(200, feed(self.lines(), on=True, version='v-on'))
        self.refresh()
        # a tiny feed never replaces a good one
        self.http.answer = FakeResp(200, feed(self.lines()[:20], on=True, version='v-tiny'))
        r = self.refresh()
        self.assertEqual(r.status_code, 422)
        self.assertEqual(a._vd_health()['version'], 'v-on')
        # Versa Docs down: the last good feed stays
        self.http.answer = FakeResp(500, {'message': 'down'})
        r = self.refresh()
        self.assertEqual(r.status_code, 502)
        self.assertEqual(len(a.load_production_from_dropbox()), 212)



class DropboxCopyTests(unittest.TestCase):
    """POST /production/versa-docs/dropbox-sync: only known ledger names, only Versa Docs
    storage links, SHA-256 checked, one fixed Dropbox folder."""

    @classmethod
    def setUpClass(cls):
        cls.a = load_app()
        cls.client = cls.a.app.test_client()

    def setUp(self):
        import hashlib
        a = self.a
        self.data = b'PK\x03\x04' + b'zz-ledger' * 50
        self.sha = hashlib.sha256(self.data).hexdigest()
        self.uploads = []
        self.gets = []
        self.get_status = 200
        self.upload_status = 200
        test = self

        class Resp:
            def __init__(self, status, data):
                self.status_code, self._data = status, data

            def iter_content(self, n):
                for i in range(0, len(self._data), n):
                    yield self._data[i:i + n]

            def close(self):
                pass

        class Http:
            def get(self, url, timeout=None, allow_redirects=True, stream=False):
                test.gets.append({'url': url, 'allow_redirects': allow_redirects})
                return Resp(test.get_status, test.data)

            def post(self, url, headers=None, data=None, json=None, timeout=None):
                test.uploads.append({'url': url, 'arg': headers.get('Dropbox-API-Arg'), 'bytes': data})
                body = '{"error_summary": "missing_scope/"}' if test.upload_status != 200 else '{}'
                return type('R', (), {'status_code': test.upload_status, 'text': body})()

        self.ident = {'tier': 'staff', 'is_admin': False, 'email': 'zz@example.test'}
        self._saved = (a.http_requests, a._request_identity, a.get_dropbox_token)
        a.http_requests = Http()
        a._request_identity = lambda: self.ident
        a.get_dropbox_token = lambda: 'zz-token'
        self.url = a._vd_storage_prefixes()[0] + 'TF/FY1_Production_Style_Ledger.xlsx?token=zz'

    def tearDown(self):
        a = self.a
        a.http_requests, a._request_identity, a.get_dropbox_token = self._saved

    def post(self, files):
        return self.client.post('/production/versa-docs/dropbox-sync', json={'files': files})

    def test_office_sign_in_only(self):
        self.ident = {'tier': 'factory', 'prefix': 'TF'}
        self.assertEqual(self.post([{'name': 'FY1_Production_Style_Ledger.xlsx', 'url': self.url, 'sha256': self.sha}]).status_code, 401)
        self.assertEqual(self.uploads, [])

    def test_good_file_is_written_to_the_one_folder(self):
        a = self.a
        r = self.post([{'name': 'FY1_Production_Style_Ledger.xlsx', 'url': self.url, 'sha256': self.sha}])
        self.assertEqual(r.status_code, 200, r.get_data(as_text=True))
        self.assertEqual(len(self.uploads), 1)
        arg = json.loads(self.uploads[0]['arg'])
        self.assertEqual(arg['path'], a.VD_DROPBOX_FOLDER + '/FY1_Production_Style_Ledger.xlsx')
        self.assertEqual(arg['mode'], 'overwrite')
        self.assertEqual(self.uploads[0]['bytes'], self.data)

    def test_refusals(self):
        r = self.post([
            {'name': '../evil.xlsx', 'url': self.url, 'sha256': self.sha},
            {'name': 'NB_Production_Style_Ledger.xlsx', 'url': 'https://example.test/x.xlsx', 'sha256': self.sha},
            {'name': 'PC_Production_Style_Ledger.xlsx', 'url': self.url.replace('TF/FY1_', 'PC/PC_'), 'sha256': '0' * 64},
            {'name': 'Frank_Production_Style_Ledger.xlsx', 'url': self.url.replace('TF/FY1_', 'FR/Frank_'), 'sha256': 'nope'}])
        self.assertEqual(r.status_code, 502)
        errs = [f['error'] for f in r.get_json()['files']]
        self.assertEqual(errs, ['unknown file name', 'not a Versa Docs storage link', 'sha256 mismatch', 'sha256 missing'])
        self.assertEqual(self.uploads, [])

    def test_links_must_be_exactly_the_ledger_file(self):
        a = self.a
        host = a._vd_storage_prefixes()[0].split('/storage/')[0]
        bad = [
            host + '/storage/v1/object/sign/ledger-live/../packs/x.xlsx?token=zz',
            host + '/storage/v1/object/sign/ledger-live/%2e%2e/packs/x.xlsx?token=zz',
            host + '/storage/v1/object/sign/ledger-live/NB/FY1_Production_Style_Ledger.xlsx?token=zz',
            host + '/storage/v1/object/sign/ledger-live/TF/FY1_Production_Style_Ledger.xlsx',
            'https://evil.test/storage/v1/object/sign/ledger-live/TF/FY1_Production_Style_Ledger.xlsx?token=zz',
            'https://user@' + host.split('://')[1] + '/storage/v1/object/sign/ledger-live/TF/FY1_Production_Style_Ledger.xlsx?token=zz',
        ]
        for u in bad:
            self.assertFalse(a._vd_link_ok(u, 'FY1_Production_Style_Ledger.xlsx'), u)
        self.assertTrue(a._vd_link_ok(self.url, 'FY1_Production_Style_Ledger.xlsx'))
        r = self.post([{'name': 'FY1_Production_Style_Ledger.xlsx', 'url': bad[0], 'sha256': self.sha}])
        self.assertEqual(r.get_json()['files'][0]['error'], 'not a Versa Docs storage link')
        self.assertEqual(self.gets, [])

    def test_no_redirects_followed(self):
        self.get_status = 302
        r = self.post([{'name': 'FY1_Production_Style_Ledger.xlsx', 'url': self.url, 'sha256': self.sha}])
        self.assertEqual(r.get_json()['files'][0]['error'], 'download failed (302)')
        self.assertFalse(self.gets[0]['allow_redirects'])
        self.assertEqual(self.uploads, [])

    def test_no_write_access_is_reported(self):
        self.upload_status = 401
        r = self.post([{'name': 'FY1_Production_Style_Ledger.xlsx', 'url': self.url, 'sha256': self.sha}])
        self.assertEqual(r.status_code, 502)
        self.assertEqual(r.get_json()['files'][0]['error'], 'dropbox_no_write_access')


if __name__ == '__main__':
    unittest.main()
