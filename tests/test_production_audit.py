"""Production Audit + swatch cards (David, Nov 9 2026).

The phone app's Production Audit tab shows what changed on the Style Ledger per day, and the
swatch cards from David's email filed under their production POs. These tests run both modules
on a tiny Flask app with an in-memory S3, so nothing touches the network.

Everything here is synthetic: made-up style numbers (fabric ZZ, serial 9xx), production numbers
(TF26901...) and units.
"""
import base64
import datetime as dt
import io
import json
import os
import sys
import unittest
from unittest import mock

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from flask import Flask  # noqa: E402

import production_audit as pa  # noqa: E402
import production_swatch_cards as sc  # noqa: E402

KEY = 'test-machine-key-0123456789'


class FakeS3:
    """Just enough of boto3's S3 client: objects, versions of one key, prefix listing."""

    class _Err(Exception):
        def __init__(self, code):
            super().__init__(code)
            self.response = {'Error': {'Code': code}}

    def __init__(self):
        self.objects = {}            # key -> bytes
        self.versions = {}           # key -> [(modified, version_id, bytes)]

    def put_object(self, Bucket, Key, Body, **kw):
        self.objects[Key] = Body if isinstance(Body, bytes) else Body.encode('utf-8')

    def get_object(self, Bucket, Key, VersionId=None):
        if VersionId:
            for _m, vid, body in self.versions.get(Key, []):
                if vid == VersionId:
                    return {'Body': io.BytesIO(body)}
            raise self._Err('NoSuchVersion')
        if Key not in self.objects:
            raise self._Err('NoSuchKey')
        return {'Body': io.BytesIO(self.objects[Key])}

    def list_objects_v2(self, Bucket, Prefix, MaxKeys=1000, ContinuationToken=None):
        keys = sorted(k for k in self.objects if k.startswith(Prefix))
        return {'Contents': [{'Key': k} for k in keys], 'IsTruncated': False}

    def list_object_versions(self, Bucket, Prefix, MaxKeys=1000, **kw):
        out = []
        for key, vs in self.versions.items():
            if key.startswith(Prefix):
                out.extend({'Key': key, 'LastModified': m, 'VersionId': vid} for m, vid, _b in vs)
        return {'Versions': out, 'IsTruncated': False}

    def add_version(self, key, modified, obj):
        vid = f'v{len(self.versions.get(key, [])) + 1}'
        self.versions.setdefault(key, []).append((modified, vid, json.dumps(obj).encode('utf-8')))
        return vid


def _row(prod, style, units, etd='2026-12-15', ship='', wh='JTW', po='TEST PO', brand='NAUTICA',
         port=None):
    return {'production': prod, 'poName': po, 'style': style, 'units': units, 'brand': brand,
            'etd': etd, 'etd_raw': etd, 'arrival': port, 'port_dated': bool(port), 'fob_flag': False,
            'fob_note': '', 'shipmentNo': ship, 'warehouse': wh}


def _tracker(lines):
    return {'last_check': 'x', 'orders': {f'{p}::{s}': {'production': p, 'style': s, 'units': u, 'brand': 'NAUTICA',
                                                         'po_name': 'OLD NAME', 'etd': e, 'fell_off_date': None}
                                           for p, s, u, e in lines}}


def _base_lines(n=60):
    return [('TF26901', f'TJNAZZ{900 + i:03d}SLS', 100 + i, '12/15/2026') for i in range(n)]


class TestDiffEngine(unittest.TestCase):
    def setUp(self):
        pa._deps.clear()

    def test_rows_sum_per_production_and_style(self):
        rows = [_row('TF26901', 'TJNAZZ901SLS', 600, ship='26-901'), _row('tf26901 ', 'TJNAZZ901SLS', 400),
                _row('TF26901', 'TJNAZZ902SLS', 50, etd='2026-12-20', port='2027-01-31')]
        lines = pa.lines_from_rows(rows)
        self.assertEqual(lines['TF26901::TJNAZZ901SLS']['u'], 1000)
        self.assertEqual(lines['TF26901::TJNAZZ901SLS']['sh'], ['26-901'])
        self.assertEqual(lines['TF26901::TJNAZZ901SLS']['n'], 2)
        self.assertEqual(lines['TF26901::TJNAZZ902SLS']['g'], '2027-01-21')   # arrival - 10

    def test_column_f_text_and_dates_compare_by_day(self):
        self.assertEqual(pa.norm_etd('10/22/2026'), '2026-10-22')
        self.assertEqual(pa.norm_etd(' 2026/10/1'), '2026-10-01')
        self.assertEqual(pa.norm_etd('2026/10//15'), '2026-10-15')
        self.assertEqual(pa.norm_etd('tbd'), 'TBD')
        self.assertIsNone(pa.norm_etd(''))

    def test_new_removed_changed_and_moves(self):
        base = pa.own_snapshot('2026-11-09', [_row('TF26901', 'TJNAZZ901SLS', 1000),
                                              _row('TF26901', 'TJNAZZ902SLS', 500),
                                              _row('NB26901', 'TJNAZZ903SLS', 300)])
        cur = pa.own_snapshot('2026-11-10', [_row('TF26901', 'TJNAZZ901SLS', 1200, etd='2027-01-05', ship='26-901'),
                                             _row('TF26902', 'TJNAZZ902SLS', 500),
                                             _row('NB26901', 'TJNAZZ903SLS', 300, wh='TR'),
                                             _row('TF26903', 'TJNAZZ904SLS', 720)])
        events, pos = pa.diff_snapshots(base, cur)
        by = {(e['p'], e['s']): e for e in events}
        e = by[('TF26901', 'TJNAZZ901SLS')]
        self.assertEqual((e['k'], e['u0'], e['u1']), ('chg', 1000, 1200))
        self.assertEqual((e['e0'], e['e1']), (['2026-12-15'], ['2027-01-05']))
        self.assertEqual((e['sh0'], e['sh1']), ([], ['26-901']))
        rem = by[('TF26901', 'TJNAZZ902SLS')]
        self.assertEqual((rem['k'], rem['u0'], rem['mv']), ('rem', 500, ['TF26902']))
        new = by[('TF26902', 'TJNAZZ902SLS')]
        self.assertEqual((new['k'], new['u1'], new['mvf'], new.get('np')), ('new', 500, ['TF26901'], 1))
        self.assertEqual(by[('NB26901', 'TJNAZZ903SLS')]['w1'], 'TR')
        self.assertEqual(pos['TF26901']['u0'], 1500)
        self.assertEqual(pos['TF26901']['u1'], 1200)
        s = pa.summarize(events)
        self.assertEqual((s['new'], s['rem'], s['chg']), (2, 1, 2))
        self.assertEqual(s['up'], 200 + 500 + 720)
        self.assertEqual(s['down'], 500)

    def test_same_copy_has_no_changes(self):
        snap = pa.own_snapshot('2026-11-09', [_row('TF26901', f'TJNAZZ{900 + i}SLS', 10 * i) for i in range(30)])
        self.assertEqual(pa.diff_snapshots(snap, snap)[0], [])

    def test_tracker_days_never_compare_po_names_or_shipments(self):
        tr = {'day': '2026-11-08', 'source': 'tracker', 'fields': list(pa.TRACKER_FIELDS),
              'lines': pa.lines_from_tracker(_tracker([('TF26901', 'TJNAZZ901SLS', 100, '12/15/2026')]))}
        own = pa.own_snapshot('2026-11-09', [_row('TF26901', 'TJNAZZ901SLS', 100, ship='26-901', po='NEW NAME'),
                                             _row('TF26901', 'TJNAZZ901SLS', 0, etd='2026-12-20')])
        self.assertEqual(pa.diff_snapshots(tr, own)[0], [])   # a shared ex-factory date is no change

    def test_po_names_compare_as_a_set(self):
        a = pa.own_snapshot('2026-11-09', [_row('DP26901', 'TJDKZZ901SLS', 500, po='50279001'),
                                           _row('DP26901', 'TJDKZZ901SLS', 400, po='50279002')])
        b = pa.own_snapshot('2026-11-10', [_row('DP26901', 'TJDKZZ901SLS', 300, po='50279001'),
                                           _row('DP26901', 'TJDKZZ901SLS', 600, po='50279002')])
        self.assertEqual(pa.diff_snapshots(a, b)[0], [])


class _Env(unittest.TestCase):
    def setUp(self):
        pa._deps.clear()
        pa._mem.update(days={}, events={}, live=None, state=None, state_at=0.0)
        sc._deps.clear()
        sc._idx_cache.update(data=None, at=0.0)
        sc._token_cache.clear()
        self.s3 = FakeS3()
        self.rows = [_row(p, s, u) for p, s, u, _e in _base_lines()]
        self.ident = None
        self.held = set()
        self.app = Flask('t')
        pa.register_production_audit_routes(
            self.app, get_s3=lambda: self.s3, s3_bucket='b', machine_key=KEY, identity=lambda: self.ident,
            ledger_rows=lambda: (list(self.rows), 'rev1'), held_factories=lambda: self.held,
            start_capture=False)
        sc.register_swatch_card_routes(self.app, get_s3=lambda: self.s3, s3_bucket='b', region='us-east-2',
                                       machine_key=KEY, identity=lambda: self.ident,
                                       held_factories=lambda: self.held)
        self.c = self.app.test_client()


class TestCaptureAndRoutes(_Env):
    def _day(self, iso):
        return dt.date.fromisoformat(iso)

    def test_backfill_capture_and_range(self):
        # tracker copies: Nov 6 (60 lines), Nov 7 (a glitch copy of 3 lines, then a good one), Nov 8
        t = dt.datetime(2026, 11, 6, 22, 0, tzinfo=dt.timezone.utc)
        self.s3.add_version(pa.PA_TRACKER_KEY, t, _tracker(_base_lines()))
        good7 = _base_lines()
        good7[0] = ('TF26901', 'TJNAZZ900SLS', 999, '12/15/2026')
        self.s3.add_version(pa.PA_TRACKER_KEY, t + dt.timedelta(days=1), _tracker(good7))
        self.s3.add_version(pa.PA_TRACKER_KEY, t + dt.timedelta(days=1, hours=1), _tracker(good7[:3]))
        good8 = good7 + [('NB26901', 'TJNAZZ999SLS', 3600, '1/25/27')]
        self.s3.add_version(pa.PA_TRACKER_KEY, t + dt.timedelta(days=2), _tracker(good8))
        with mock.patch.object(pa, 'et_today', return_value=self._day('2026-11-09')):
            pa.backfill(start=self._day('2026-11-06'), end=self._day('2026-11-08'))
            self.assertEqual(pa._backfill['counts']['tracker_days'], 3)
            d7 = pa.events_for_day('2026-11-07')
            self.assertEqual([(e['s'], e['u0'], e['u1']) for e in d7['events']], [('TJNAZZ900SLS', 100, 999)])
            d8 = pa.events_for_day('2026-11-08')
            self.assertEqual([(e['k'], e['p'], e.get('np')) for e in d8['events']], [('new', 'NB26901', 1)])
            # today: the live ledger against Nov 8's copy (the ledger now has the NB line as 3,000 and lost TF 901)
            self.rows = [r for r in self.rows if r['style'] != 'TJNAZZ901SLS']
            self.rows[0]['units'] = 999
            self.rows.append(_row('NB26901', 'TJNAZZ999SLS', 3000, etd='2027-01-25'))
            self.assertTrue(pa.capture_once()['changed'])
            self.assertFalse(pa.capture_once()['changed'])          # same ledger, nothing new
            r = self.c.get('/production-audit?from=2026-11-07&to=2026-11-09', headers={'X-Api-Key': KEY})
            self.assertEqual(r.status_code, 200)
            j = r.get_json()
            self.assertEqual([d['d'] for d in j['days']], ['2026-11-07', '2026-11-08', '2026-11-09'])
            today = [e for e in j['events'] if e['d'] == '2026-11-09']
            kinds = sorted((e['k'], e['s']) for e in today)
            self.assertEqual(kinds, [('chg', 'TJNAZZ999SLS'), ('rem', 'TJNAZZ901SLS')])
            self.assertTrue(j['days'][-1]['live'])
            self.assertEqual(j['now']['NB26901']['u'], 3000)
            # a held factory disappears from the answer
            self.held = {'NB'}
            j = self.c.get('/production-audit?from=2026-11-07&to=2026-11-09', headers={'X-Api-Key': KEY}).get_json()
            self.assertFalse(any(e['f'] == 'NB' for e in j['events']))

    def test_nothing_stored_in_plain_text(self):
        with mock.patch.object(pa, 'et_today', return_value=self._day('2026-11-09')):
            pa.capture_once()
        stored = [k for k in self.s3.objects if k.startswith('production-audit/')]
        self.assertTrue(stored)
        for k in stored:
            self.assertNotIn(b'TJNAZZ', self.s3.objects[k])
            self.assertRegex(k, r'^production-audit/[0-9a-f]{32}/')

    def test_big_drop_waits_for_confirmation(self):
        self.rows = [_row(p, s, u) for p, s, u, _e in _base_lines(100)]
        with mock.patch.object(pa, 'et_today', return_value=self._day('2026-11-09')):
            pa.capture_once()
            self.rows = self.rows[:95]
            self.assertTrue(pa.capture_once()['changed'])            # 100 -> 95 is an ordinary edit
            self.rows = self.rows[:60]
            res = pa.capture_once()
            self.assertFalse(res['ok'])
            self.assertIn('waiting', res['why'])
            self.assertEqual(pa._state()['lines'], 95)
            # still that way half an hour later: it is real, and it is kept
            st = pa._state()
            st['drop_pending']['since'] -= pa.PA_DROP_CONFIRM_SECONDS + 1
            pa._save_state(st)
            self.assertTrue(pa.capture_once()['changed'])
            self.assertEqual(pa._state()['lines'], 60)

    def test_routes_need_staff_or_machine(self):
        self.assertEqual(self.c.get('/production-audit').status_code, 401)
        self.ident = {'tier': 'factory'}
        self.assertEqual(self.c.get('/production-audit').status_code, 401)
        self.assertEqual(self.c.get('/production-swatch-cards').status_code, 401)
        self.ident = {'tier': 'staff'}
        with mock.patch.object(pa, 'et_today', return_value=self._day('2026-11-09')):
            self.assertEqual(self.c.get('/production-audit?from=2026-11-01&to=2026-11-09').status_code, 200)
            self.assertEqual(self.c.get('/production-audit?from=2026-06-01&to=2026-11-09').status_code, 400)
        self.assertEqual(self.c.post('/production-audit/backfill', json={}).status_code, 401)


def _pdf(text='TF26901'):
    try:
        import pymupdf as fitz
    except ImportError:
        import fitz
    doc = fitz.open()
    page = doc.new_page(width=612, height=792)
    page.insert_text((72, 72), f'PO REF: {text}')
    for i in range(400):
        page.draw_rect(fitz.Rect(20 + (i % 20) * 28, 120 + (i // 20) * 30, 44 + (i % 20) * 28, 146 + (i // 20) * 30),
                       color=(i % 7 / 7, 0.3, 0.6), fill=(0.9, i % 5 / 5, 0.2))
    out = doc.tobytes(garbage=0, deflate=False)
    doc.close()
    return out


class TestSwatchCards(_Env):
    def test_names(self):
        self.assertEqual(sc.productions_in('CUSTOMER FOB CHINA NB26901, 02 =110226 fm Name=.pdf'), ['NB26901', 'NB26902'])
        self.assertEqual(sc.productions_in('AB X BRAND PC26901,02,03'), ['PC26901', 'PC26902', 'PC26903'])
        self.assertEqual(sc.productions_in('Factory invoice PC26-901'), [])
        msg = {'id': 'm1', 'subject': 'New PO = TF26901 = JAN 25', 'text': 'Please find PO with swatch card as attached.',
               'attachments': [{'name': 'AB X BRAND TF26901 =110226 fm Name=.pdf', 'size': 900000, 'att': 'a'},
                               {'name': 'BRAND TRIMS & LABELS_cmp.pdf', 'size': 900000, 'att': 'b'},
                               {'name': 'GB_RB901_cmp.pdf', 'size': 900000, 'att': 'c'},
                               {'name': 'AB x BRAND TTL 100 units TF26901.xlsx', 'size': 90000, 'att': 'd'}]}
        want, _skipped = sc.plan_message(msg)
        self.assertEqual([(w['att'], w['prods']) for w in want], [('a', ['TF26901'])])
        # a lone plain-named PDF in a "swatch card" email is the card, filed by the subject
        msg['attachments'] = [{'name': 'Scan 1.pdf', 'size': 900000, 'att': 'e'}]
        self.assertEqual([w['prods'] for w in sc.plan_message(msg)[0]], [['TF26901']])
        # ... but two plain PDFs (tech packs) are not
        msg['attachments'].append({'name': 'Scan 2.pdf', 'size': 900000, 'att': 'f'})
        self.assertEqual(sc.plan_message(msg)[0], [])

    def test_ingest_flow_dedupe_and_read(self):
        h = {'X-Api-Key': KEY}
        cfg = self.c.post('/production-swatch-cards/ingest', json={'action': 'config'}, headers=h).get_json()
        self.assertIn('filename:pdf', cfg['query'])
        msg = {'id': 'm1', 'date': '2026-11-02T10:00:00Z', 'from': 'design@example.com', 'subject': 'New PO TF26901',
               'text': 'swatch card attached',
               'attachments': [{'name': 'AB X BRAND TF26901 =110226 fm Name=.pdf', 'size': 0, 'att': 'a'}]}
        plan = self.c.post('/production-swatch-cards/ingest', json={'action': 'plan', 'message': msg}, headers=h).get_json()
        self.assertEqual(len(plan['want']), 1)
        pdf = _pdf()
        data = base64.urlsafe_b64encode(pdf).decode().rstrip('=')
        up = {'action': 'upload', 'message': msg, 'prods': ['TF26901'],
              'file': {'name': plan['want'][0]['name'], 'size': len(pdf), 'data': data}}
        r1 = self.c.post('/production-swatch-cards/ingest', json=up, headers=h).get_json()
        self.assertFalse(r1['dup'])
        self.assertEqual(r1['pages'], 1)
        msg2 = dict(msg, id='m2', date='2026-11-03T10:00:00Z')
        up2 = dict(up, message=msg2, prods=['TF26902'])
        r2 = self.c.post('/production-swatch-cards/ingest', json=up2, headers=h).get_json()
        self.assertTrue(r2['dup'])
        self.assertEqual(r2['prods'], ['TF26901', 'TF26902'])
        self.c.post('/production-swatch-cards/ingest', json={'action': 'seen', 'id': 'm1'}, headers=h)
        pend = self.c.post('/production-swatch-cards/ingest', json={'action': 'pending', 'ids': ['m1', 'm9']}, headers=h).get_json()
        self.assertEqual(pend['ids'], ['m9'])
        files = [k for k in self.s3.objects if k.endswith('.pdf')]
        thumbs = [k for k in self.s3.objects if k.endswith('.jpg')]
        self.assertEqual((len(files), len(thumbs)), (1, 1))
        self.assertNotIn(b'TF26901', self.s3.objects[[k for k in self.s3.objects if k.endswith('index.json.enc')][0]])
        self.ident = {'tier': 'staff'}
        j = self.c.get('/production-swatch-cards').get_json()
        self.assertEqual(j['by_prod']['TF26901'], [r1['id']])
        self.assertTrue(j['cards'][0]['url'].startswith('https://b.s3.us-east-2.amazonaws.com/production-swatch-cards/'))
        self.assertEqual(j['cards'][0]['date'], '2026-11-02T10:00:00Z')   # dated by the first email
        # not a PDF -> refused
        bad = dict(up, file={'name': 'x.pdf', 'size': 20000, 'data': base64.urlsafe_b64encode(b'A' * 20000).decode()})
        self.assertEqual(self.c.post('/production-swatch-cards/ingest', json=bad, headers=h).status_code, 400)

    def test_google_token_must_be_allowed_and_pinned(self):
        h = {'X-Api-Key': KEY}
        self.c.post('/production-swatch-cards/config', json={'allowed_emails': ['owner@example.com'],
                                                              'allowed_aud': ['script-aud']}, headers=h)
        ok = {'email': 'owner@example.com', 'aud': 'script-aud'}
        with mock.patch.object(sc, '_google_identity', return_value=ok):
            r = self.c.post('/production-swatch-cards/ingest', json={'action': 'config'},
                            headers={'x-versa-id-token': 'tok'})
            self.assertEqual(r.status_code, 200)
        for bad in ({'email': 'someone@example.com', 'aud': 'script-aud'}, {'email': 'owner@example.com', 'aud': 'other'}, None):
            with mock.patch.object(sc, '_google_identity', return_value=bad):
                r = self.c.post('/production-swatch-cards/ingest', json={'action': 'config'},
                                headers={'x-versa-id-token': 'tok'})
                self.assertEqual(r.status_code, 401)
        self.assertEqual(self.c.post('/production-swatch-cards/ingest', json={'action': 'config'}).status_code, 401)


if __name__ == '__main__':
    unittest.main()
