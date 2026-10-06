"""Substitute allocations held back from the Big 3 allocation reports (Oct 6 2026).

Synthetic only: made-up customer codes (Z...), PO numbers and styles. The pure
rules in apo_subs are tested directly; the app.py wiring (_apo_report_rows, the
dollar summary, /apo-report-rows and the workbook header) runs offline with the
open-orders reads replaced by fakes.

Run from tests/:  python -B -m unittest test_apo_subs
"""
import json
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import apo_subs as S  # noqa: E402

# Ordered style ZBUSXX410SLS on Burlington PO 451000016 was substituted with ZBUSXX410RFS.
ANN = {
    'ZBUSXX410SLS_451000016_0': {'check': True, 'notes': [],
                                 'sub': {'style': 'ZBUSXX410RFS', 'qty': 360, 'setBy': 'Tester',
                                         'setAt': '2026-09-09T14:00:00Z'}},
    # Two sub styles in one free-text field, one of them with a trailing note.
    'ZRODXX088SLP_52000535_0': {'sub': {'style': 'ZRODXX166SLP &ZRODXX088SLP - SECOND LOT LATER', 'qty': 7000}},
    # Dashed Ross-style PO text, legacy two-part key.
    'ZKHNXX920SLS_03-700465': {'sub': {'style': 'ZTMNXX921SLS', 'qty': 540}},
    # The PO already shipped (archive only).
    'ZBUDXX011SLS_451000905_0': {'sub': {'style': 'ZTMDXX011SLS', 'qty': 2500}},
    # Removed sub (null) and a sub with no usable style text.
    'ZBUXXX000SLS_451000001_0': {'sub': None},
    'ZBUXXX000SLS_451000002_0': {'sub': {'style': 'see notes', 'qty': 10}},
    # Not a Big 3 PO.
    'ZBUVXX022SLE_1500797_0': {'sub': {'style': 'ZTMVXX022SLE', 'qty': 180}},
}

POMAP = {
    '451000016': ('burlington', 'live'),
    '52000535': ('ross', 'live'),
    '3700465': ('ross', 'live'),
    '451000905': ('burlington', 'archive'),
    '1500797': ('zforman', 'live'),
}


def resolve(pod):
    hit = POMAP.get(pod)
    if hit:
        return hit
    if len(pod) >= 7:
        for k, v in POMAP.items():
            if S.po_match(pod, k):
                return v
    return (None, None)


def row(customer, po, style, qty):
    return {'customer': customer, 'po': po, 'style': style, 'qty': qty}


BURL_ROWS = [
    row('BURLINGTON', 'ZBURL SUB PO#4510000-16', 'ZBUSXX410RFS', 360),          # the sub (name + annotation)
    row('BURLINGTON', 'ZBURL PO#4510000-16 REPLACEMENT', 'ZBUSXX410RFS', 36),    # annotation only (no SUB word)
    row('BURLINGTON', 'ZBURL - ALLOCATION 9.24.26', 'ZBUSXX410RFS', 1800),       # legit: same style, other allocation
    row('BURLINGTON', 'ZBURL - 3.5.26 STYLES', 'ZTMDXX011SLS', 5400),            # legit bulk on a TM-prefixed style
    row('BURLINGTON', 'ZBURL - SUBMITTED Q2 BUY 20260101', 'ZRODXX160SLP', 1800),  # 'SUBMITTED' is not SUB
    row('BURLINGTON', 'ZBURL SUBS 451000905', 'ZTMDXX011SLS', 2500),             # SUBS word, archived PO
    row('BURLINGTON ', 'ZBURL - OVERSEAS BULK', 'ZROGXX022SLS', 18000),          # trailing-space customer
]


class Rules(unittest.TestCase):
    def test_customer_key(self):
        for name, key in (('BURLINGTON ', 'burlington'), ('BURL', 'burlington'), ('Ross', 'ross'),
                          ('ROSS', 'ross'), ('Ross Stores', 'ross'), ('TJX', 'tjx'), ('TJMA', 'tjx'),
                          ('MARS', 'tjx'), ('Marshalls', 'tjx'), ('Hamrick', 'hamrick'), (None, '')):
            self.assertEqual(S.customer_key(name), key, name)

    def test_po_digits_and_match(self):
        self.assertEqual(S.po_digits('4510000-16'), '451000016')
        self.assertEqual(S.po_digits('02-500422'), '2500422')
        self.assertEqual(S.po_digits('0077001001'), '77001001')
        self.assertTrue(S.po_match('451000016', '451000016'))
        self.assertTrue(S.po_match('4504000', '450400001'))          # name without the line suffix
        self.assertFalse(S.po_match('123456', '12345678'))           # too short for a prefix match
        self.assertFalse(S.po_match('', '451000016'))

    def test_po_refs_in_name(self):
        self.assertEqual(S.po_refs_in_name('ZBURL SUB PO#4510000-16'), ['451000016'])
        self.assertEqual(S.po_refs_in_name('ZROSS SUB PO#02-500422'), ['2500422'])
        self.assertEqual(S.po_refs_in_name('ZROSS SUB PO#52000334 NEED CONFIRM'), ['52000334'])
        self.assertEqual(S.po_refs_in_name('ZBURL - ALLOCATION 9.24.26'), [])
        self.assertEqual(S.po_refs_in_name('ZBURL - OVERSEAS BULK PROJECTION 20260101'), ['20260101'])
        self.assertEqual(S.po_refs_in_name('ZBURL 451000016 REPLACEMENT'), ['451000016'])
        self.assertEqual(S.po_refs_in_name(''), [])

    def test_parse_sub_styles(self):
        self.assertEqual(S.parse_sub_styles('ZBUUSYD311RFY, ZROUSYD314SLD,ZBUUSYD210RFD'),
                         ['ZBUUSYD311RFY', 'ZROUSYD314SLD', 'ZBUUSYD210RFD'])
        self.assertEqual(S.parse_sub_styles('ZTJDKPK170SLP - NOT IN STOCK YET CHECK LATER'),
                         ['ZTJDKPK170SLP'])
        self.assertEqual(S.parse_sub_styles('ZRODKPK166SLP &ZRODKPE088SLP'), ['ZRODKPK166SLP', 'ZRODKPE088SLP'])
        self.assertEqual(S.parse_sub_styles('ZBUUSOX270SLS - ZAMUSOX20SLSB'), ['ZBUUSOX270SLS', 'ZAMUSOX20SLSB'])
        self.assertEqual(S.parse_sub_styles('zbuuspc410rfs'), ['ZBUUSPC410RFS'])
        self.assertEqual(S.parse_sub_styles('see notes'), [])
        self.assertEqual(S.parse_sub_styles(None), [])

    def test_parse_annotation_key(self):
        self.assertEqual(S.parse_annotation_key('ZBUSXX410SLS_451000016_0'), ('ZBUSXX410SLS', '451000016'))
        self.assertEqual(S.parse_annotation_key('ZKHNXX920SLS_03-700465'), ('ZKHNXX920SLS', '03-700465'))
        self.assertEqual(S.parse_annotation_key('ZRONXX540SLP_6900562_1125'), ('ZRONXX540SLP', '6900562'))
        self.assertEqual(S.parse_annotation_key('NOKEY'), ('NOKEY', ''))

    def test_sub_refs(self):
        refs = S.sub_refs(ANN, resolve)
        by_key = {r['key']: r for r in refs}
        self.assertEqual(len(refs), 5)                         # null sub and no-style sub are skipped
        r = by_key['ZBUSXX410SLS_451000016_0']
        self.assertEqual((r['orig_style'], r['po_digits'], r['styles'], r['customer'], r['source'], r['qty']),
                         ('ZBUSXX410SLS', '451000016', ['ZBUSXX410RFS'], 'burlington', 'live', 360))
        self.assertEqual(by_key['ZRODXX088SLP_52000535_0']['styles'], ['ZRODXX166SLP', 'ZRODXX088SLP'])
        self.assertEqual(by_key['ZKHNXX920SLS_03-700465']['customer'], 'ross')
        self.assertEqual(by_key['ZBUDXX011SLS_451000905_0']['source'], 'archive')
        self.assertEqual(by_key['ZBUVXX022SLE_1500797_0']['customer'], 'zforman')

    def test_sub_refs_tolerates_bad_shapes(self):
        refs = S.sub_refs({'a': 'text', 'b': None, 'c': {'sub': 'ZBUSXX410RFS'}, 'd': {'sub': {'style': 'ZBUSXX410RFS'}}},
                          lambda pod: (_ for _ in ()).throw(RuntimeError('boom')))
        self.assertEqual(len(refs), 1)
        self.assertEqual(refs[0]['customer'], None)
        self.assertEqual(S.sub_refs(None, resolve), [])

    def test_split_hides_sub_rows_and_keeps_legit_ones(self):
        refs = S.sub_refs(ANN, resolve)
        kept, hidden, review = S.split_customer_rows(BURL_ROWS, 'BURLINGTON', refs)
        self.assertEqual([h['po'] for h in hidden],
                         ['ZBURL SUB PO#4510000-16', 'ZBURL PO#4510000-16 REPLACEMENT', 'ZBURL SUBS 451000905'])
        self.assertEqual([h['_sub_reason'] for h in hidden], ['name', 'annotation', 'name'])
        self.assertEqual((hidden[0]['_sub_orig'], hidden[0]['_sub_po']), ('ZBUSXX410SLS', '451000016'))
        self.assertEqual((hidden[1]['_sub_orig'], hidden[1]['_sub_po']), ('ZBUSXX410SLS', '451000016'))
        self.assertEqual((hidden[2]['_sub_orig'], hidden[2]['_sub_po']), ('ZBUDXX011SLS', '451000905'))
        self.assertEqual([k['po'] for k in kept],
                         ['ZBURL - ALLOCATION 9.24.26', 'ZBURL - 3.5.26 STYLES',
                          'ZBURL - SUBMITTED Q2 BUY 20260101', 'ZBURL - OVERSEAS BULK'])
        # The legit allocation on the live sub style is kept but flagged for the team.
        self.assertEqual([(r['po'], r['_sub_orig']) for r in review],
                         [('ZBURL - ALLOCATION 9.24.26', 'ZBUSXX410SLS')])
        # Archived-PO sub styles are not review noise.
        self.assertNotIn('ZBURL - 3.5.26 STYLES', [r['po'] for r in review])
        # Input rows are not mutated.
        self.assertNotIn('_sub_reason', BURL_ROWS[0])

    def test_other_customers_subs_never_touch_this_customer(self):
        refs = S.sub_refs(ANN, resolve)
        rows = [row('Ross', 'F26 DRESS PANTS 8.25', 'ZBUSXX410RFS', 1800),      # Burlington's sub style, Ross's row
                row('Ross', 'ZROSS SUB PO#52000535', 'ZRODXX166SLP', 2100),
                row('Ross', 'ZROSS SUB PO#52000535', 'ZRODXX088SLP', 5000),
                row('Ross', 'ZROSS PO#03-700465 SWAP', 'ZTMNXX921SLS', 540)]
        kept, hidden, review = S.split_customer_rows(rows, 'Ross', refs)
        self.assertEqual([k['po'] for k in kept], ['F26 DRESS PANTS 8.25'])
        self.assertEqual(len(hidden), 3)
        self.assertEqual(hidden[2]['_sub_reason'], 'annotation')
        self.assertEqual(review, [])

    def test_review_also_catches_a_misspelled_sub_by_po(self):
        # The Select Sub text says ...RFS but the allocation is ...SLE and is not named SUB:
        # not hidden (rule 2 needs the style), but the PO reference puts it on the review list.
        refs = S.sub_refs(ANN, resolve)
        rows = [row('BURLINGTON', 'ZBURL REPLACEMENT PO#4510000-16', 'ZBUSXX410SLE', 36),
                row('BURLINGTON', 'ZBURL - BULK 20260101', 'ZBUSXX410SLE', 1800)]
        kept, hidden, review = S.split_customer_rows(rows, 'BURLINGTON', refs)
        self.assertEqual(hidden, [])
        self.assertEqual(len(kept), 2)
        self.assertEqual([(r['po'], r['_review_reason'], r['_sub_orig'], r['_sub_po']) for r in review],
                         [('ZBURL REPLACEMENT PO#4510000-16', 'po', 'ZBUSXX410SLS', '451000016')])
        self.assertEqual(S.public_row(review[0])['reason'], 'po')

    def test_name_rule_alone_without_annotations(self):
        kept, hidden, review = S.split_customer_rows(BURL_ROWS, 'BURLINGTON', [])
        self.assertEqual([h['po'] for h in hidden], ['ZBURL SUB PO#4510000-16', 'ZBURL SUBS 451000905'])
        self.assertTrue(all(h['_sub_reason'] == 'name' and '_sub_orig' not in h for h in hidden))
        self.assertEqual(review, [])

    def test_sub_word_boundaries(self):
        for name in ('ZBURL SUB PO#1', 'sub po#1', 'ZROSS SUBS', 'SUBSTITUTE FOR PO 1', 'SUBSTITUTION'):
            self.assertTrue(S.SUB_WORD.search(name), name)
        for name in ('SUBMITTED', 'SUBTOTAL', 'ZSUBURBAN BUY', 'RESUB'):
            self.assertFalse(S.SUB_WORD.search(name), name)

    def test_summary_and_public_rows(self):
        refs = S.sub_refs(ANN, resolve)
        kept, hidden, review = S.split_customer_rows(BURL_ROWS, 'BURLINGTON', refs)
        s = S.summary(hidden, review, True, len(ANN))
        self.assertEqual((s['lines'], s['units'], s['styles'], s['subs_ok'], s['annotations']),
                         (3, 2896, ['ZBUSXX410RFS', 'ZTMDXX011SLS'], True, 7))
        self.assertEqual(s['rows'][0], {'po': 'ZBURL SUB PO#4510000-16', 'style': 'ZBUSXX410RFS', 'qty': 360,
                                        'reason': 'name', 'for_style': 'ZBUSXX410SLS', 'for_po': '451000016'})
        self.assertEqual((s['review'][0]['po'], s['review'][0]['reason']), ('ZBURL - ALLOCATION 9.24.26', 'style'))
        self.assertTrue(s['po_map_ok'])
        for r in s['rows'] + s['review']:
            self.assertFalse(any(k.startswith('_') for k in r))
        json.dumps(s)
        self.assertEqual(S.summary([], [], False)['subs_ok'], False)
        self.assertEqual(S.summary([], [], True, po_map_ok=False)['po_map_ok'], False)


# ── app.py wiring ────────────────────────────────────────────────────────────

class AppWiring(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from _support import load_app
        cls.a = load_app()
        cls.client = cls.a.app.test_client()

    def setUp(self):
        a = self.a
        self._saved = {k: getattr(a, k) for k in ('_fetch_sub_annotations', '_big3_po_customers',
                                                  '_apo_avg_price_maps', '_request_identity',
                                                  '_fetch_a2000_orders', '_fetch_po_history',
                                                  'build_ship_plan_excel', 'load_production_from_dropbox',
                                                  '_apo_color_map', 'http_requests')}
        self._caches = {'oo': dict(a._oo_orders_cache), 'po': dict(a._big3_po_cache), 'ann': dict(a._sub_ann_cache)}
        with a._apo_lock:
            self._apo_saved = list(a._apo_data)
            a._apo_data[:] = BURL_ROWS + [row('Ross', 'ZROSS SUB PO#52000535', 'ZRODXX166SLP', 2100),
                                          row('Ross', 'ZROSS - BULK', 'ZRONXX940SLP', 1500),
                                          row('TJX', 'ZTJX SUB PO#30-200813', 'ZTMNXX800SLY', 1800),
                                          row('TJX', 'TK MAXX ALLOCATION', 'ZTKNXX011SLS', 500)]
        self.ann_calls = 0

        def fake_ann():
            self.ann_calls += 1
            return ANN, True
        a._fetch_sub_annotations = fake_ann
        a._big3_po_customers = lambda: (dict(POMAP), True)
        a._apo_avg_price_maps = lambda: ({}, {})
        self.ident = {'tier': 'machine'}
        a._request_identity = lambda: self.ident

    def tearDown(self):
        a = self.a
        for k, v in self._saved.items():
            setattr(a, k, v)
        for name, saved in (('_oo_orders_cache', self._caches['oo']), ('_big3_po_cache', self._caches['po']),
                            ('_sub_ann_cache', self._caches['ann'])):
            getattr(a, name).clear()
            getattr(a, name).update(saved)
        with a._apo_lock:
            a._apo_data[:] = self._apo_saved

    def reset_po_caches(self):
        a = self.a
        a._oo_orders_cache.update(data=None, time=0.0, ok=False)
        a._big3_po_cache.update(map=None, time=0.0, ok=False, fail_until=0.0)

    def test_report_rows_split(self):
        rep = self.a._apo_report_rows('BURLINGTON')
        self.assertEqual([r['po'] for r in rep['hidden']],
                         ['ZBURL SUB PO#4510000-16', 'ZBURL PO#4510000-16 REPLACEMENT', 'ZBURL SUBS 451000905'])
        self.assertEqual(len(rep['rows']), 4)
        self.assertEqual([r['po'] for r in rep['review']], ['ZBURL - ALLOCATION 9.24.26'])
        self.assertTrue(rep['subs_ok'])
        self.assertTrue(rep['po_map_ok'])
        self.assertEqual(rep['annotations'], len(ANN))
        self.assertEqual(self.ann_calls, 1)                    # one Select Sub read per report build

    def test_po_map_health_when_the_live_book_is_down(self):
        # Live book unreachable and the archive failing: nothing is cached, the flag is False,
        # only the name rule hides, and the summaries carry po_map_ok False.
        a = self.a
        a._big3_po_customers = self._saved['_big3_po_customers']       # the real one
        self.reset_po_caches()
        a._fetch_a2000_orders = lambda: []                               # a failed fetch leaves ok False
        a._fetch_po_history = lambda code: (None, False)
        m, ok = a._big3_po_customers()
        self.assertEqual((m, ok), ({}, False))
        self.assertIsNone(a._big3_po_cache['map'])
        self.assertGreater(a._big3_po_cache['fail_until'], 0.0)
        rep = a._apo_report_rows('BURLINGTON')
        self.assertEqual((rep['subs_ok'], rep['po_map_ok']), (True, False))
        self.assertEqual([r['po'] for r in rep['hidden']], ['ZBURL SUB PO#4510000-16', 'ZBURL SUBS 451000905'])
        self.assertEqual(rep['review'], [])
        hs = a.build_apo_dollar_summary('BURLINGTON')['hidden_subs']
        self.assertEqual((hs['subs_ok'], hs['po_map_ok']), (True, False))
        r = self.client.get('/apo-report-rows?customer=BURLINGTON')
        self.assertFalse(r.get_json()['hidden_subs']['po_map_ok'])
        # A healthy follow-up after the backoff rebuilds the full map and caches it for real.
        a._big3_po_cache['fail_until'] = 0.0
        a._oo_orders_cache.update(data=[{'customer': 'BURL', 'orderNo': '451000016'}], time=a.time.time(), ok=True)
        a._fetch_a2000_orders = lambda: a._oo_orders_cache['data']
        a._fetch_po_history = lambda code: ({'pos': [{'orderNo': '451000905'}] if code == 'BURL' else []}, True)
        m, ok = a._big3_po_customers()
        self.assertTrue(ok)
        self.assertEqual(m['451000016'], ('burlington', 'live'))
        self.assertEqual(m['451000905'], ('burlington', 'archive'))
        self.assertEqual(a._big3_po_cache['map'], m)
        self.assertTrue(a._apo_report_rows('BURLINGTON')['po_map_ok'])

    def test_po_map_partial_archive_is_short_lived(self):
        a = self.a
        a._big3_po_customers = self._saved['_big3_po_customers']
        self.reset_po_caches()
        a._oo_orders_cache.update(data=[{'customer': 'ROSS', 'orderNo': '52000535'}], time=a.time.time(), ok=True)
        a._fetch_a2000_orders = lambda: a._oo_orders_cache['data']
        a._fetch_po_history = lambda code: ({'pos': [], 'ready': False}, False)    # archive rebuilding
        m, ok = a._big3_po_customers()
        self.assertEqual((m, ok), ({'52000535': ('ross', 'live')}, False))
        age = a.time.time() - a._big3_po_cache['time']
        self.assertTrue(530 < age < 600, age)                           # kept about 60 s, not 10 min
        self.assertEqual(a._big3_po_customers(), (m, False))            # served from cache meanwhile

    def test_annotation_cache_race_keeps_the_fresher_copy(self):
        a = self.a
        a._fetch_sub_annotations = self._saved['_fetch_sub_annotations']   # the real one
        a._sub_ann_cache.update(data={'old': {}}, time=0.0, ok=True, fail_until=0.0)   # expired entry
        fresh = {'ZK_1_0': {'sub': {'style': 'ZBUSXX410RFS'}}}

        class Racer:
            def get(self, *args, **kw):
                # a concurrent call stored a fresher copy while this fetch was in flight
                a._sub_ann_cache.update(data=fresh, time=a.time.time(), ok=True, fail_until=0.0)
                raise ConnectionError('synthetic')
        a.http_requests = Racer()
        self.assertEqual(a._fetch_sub_annotations(), (fresh, True))
        self.assertTrue(a._sub_ann_cache['ok'])
        self.assertEqual(a._sub_ann_cache['fail_until'], 0.0)

    def test_annotation_fetch_failure_backs_off(self):
        a = self.a
        a._fetch_sub_annotations = self._saved['_fetch_sub_annotations']
        a._sub_ann_cache.update(data=None, time=0.0, ok=False, fail_until=0.0)
        calls = []

        class Down:
            def get(self, *args, **kw):
                calls.append(1)
                raise ConnectionError('synthetic')
        a.http_requests = Down()
        self.assertEqual(a._fetch_sub_annotations(), ({}, False))
        self.assertEqual(a._fetch_sub_annotations(), ({}, False))       # inside the 60 s backoff: no new call
        self.assertEqual(len(calls), 1)

    def test_workbook_tabs_carry_only_kept_rows(self):
        # The xlsx forwarded to the buyer is built from the kept rows only: capture the tabs
        # handed to the renderer (no images, no network) and check SKUs and units.
        a = self.a
        captured = []
        a.build_ship_plan_excel = lambda tabs, url, headers=None: (captured.append(tabs) or b'PK\x03\x04synthetic')
        a.load_production_from_dropbox = lambda: []
        a._apo_color_map = lambda: {}
        a._fetch_a2000_orders = lambda: []
        r = self.client.get('/export-apo-brandcolor?customer=BURLINGTON')
        self.assertEqual(r.status_code, 200, r.data[:300])
        self.assertEqual(json.loads(r.headers['X-Apo-Hidden-Subs']),
                         {'lines': 3, 'units': 2896, 'styles': ['ZBUSXX410RFS', 'ZTMDXX011SLS'],
                          'subs_ok': True, 'po_map_ok': True})
        self.assertEqual(len(captured), 1)
        units = {}
        for tab in captured[0]:
            for it in tab['items']:
                units[it['sku']] = units.get(it['sku'], 0) + it['units_ship']
        self.assertEqual(units, {'ZBUSXX410RFS': 1800, 'ZTMDXX011SLS': 5400, 'ZRODXX160SLP': 1800, 'ZROGXX022SLS': 18000})
        text = json.dumps(captured[0])
        for never in ('SUB PO#', 'SUBS ', 'Held back', 'REPLACEMENT'):
            self.assertNotIn(never, text)

    def test_report_rows_customer_key_resolution_uses_feed_names(self):
        # Feed customer strings differ from the annotation side; both resolve to one key.
        rep = self.a._apo_report_rows('Ross')
        self.assertEqual([r['po'] for r in rep['hidden']], ['ZROSS SUB PO#52000535'])
        self.assertEqual([r['po'] for r in rep['rows']], ['ZROSS - BULK'])

    def test_exclude_tokens_still_apply(self):
        rep = self.a._apo_report_rows('TJX', ['TK'])
        self.assertEqual(rep['rows'], [])
        self.assertEqual([r['po'] for r in rep['hidden']], ['ZTJX SUB PO#30-200813'])

    def test_annotations_unavailable_keeps_the_name_rule(self):
        self.a._fetch_sub_annotations = lambda: ({}, False)
        rep = self.a._apo_report_rows('BURLINGTON')
        self.assertFalse(rep['subs_ok'])
        self.assertEqual([r['po'] for r in rep['hidden']], ['ZBURL SUB PO#4510000-16', 'ZBURL SUBS 451000905'])
        self.assertEqual(rep['review'], [])

    def test_po_map_is_only_built_when_a_sub_exists(self):
        self.a._fetch_sub_annotations = lambda: ({'k': {'check': True}}, True)
        self.a._big3_po_customers = lambda: (_ for _ in ()).throw(AssertionError('po map built for nothing'))
        rep = self.a._apo_report_rows('BURLINGTON')
        self.assertEqual([r['po'] for r in rep['hidden']], ['ZBURL SUB PO#4510000-16', 'ZBURL SUBS 451000905'])

    def test_dollar_summary_counts_only_kept_rows(self):
        s = self.a.build_apo_dollar_summary('BURLINGTON')
        self.assertEqual(s['total_units'], 1800 + 5400 + 1800 + 18000)
        hs = s['hidden_subs']
        self.assertEqual((hs['lines'], hs['units'], hs['subs_ok']), (3, 2896, True))
        self.assertEqual(hs['rows'][0]['style'], 'ZBUSXX410RFS')

    def test_route_machine_key_and_payload(self):
        r = self.client.get('/apo-report-rows?customer=BURLINGTON')
        self.assertEqual(r.status_code, 200, r.data[:200])
        d = r.get_json()
        self.assertEqual(d['count'], 4)
        self.assertEqual(d['units'], 1800 + 5400 + 1800 + 18000)
        self.assertEqual(sorted(d['rows'][0].keys()), ['customer', 'po', 'qty', 'style'])
        self.assertEqual([h['po'] for h in d['hidden_subs']['rows']],
                         ['ZBURL SUB PO#4510000-16', 'ZBURL PO#4510000-16 REPLACEMENT', 'ZBURL SUBS 451000905'])
        self.assertEqual(d['hidden_subs']['review'][0]['for_style'], 'ZBUSXX410SLS')
        self.assertFalse(any(S.SUB_WORD.search(x['po']) for x in d['rows']))   # SUBMITTED stays, SUB goes

    def test_route_needs_a_customer_and_a_credential(self):
        self.assertEqual(self.client.get('/apo-report-rows').status_code, 400)
        # The gate only enforces with AUTH_MODE=on (or the authz_preview=1 probe used here).
        self.ident = None
        self.assertEqual(self.client.get('/apo-report-rows?customer=BURLINGTON&authz_preview=1').status_code, 401)
        self.ident = {'tier': 'oo'}
        self.assertIn(self.client.get('/apo-report-rows?customer=BURLINGTON&authz_preview=1').status_code, (401, 403))
        self.ident = {'tier': 'machine'}
        self.assertEqual(self.client.get('/apo-report-rows?customer=BURLINGTON&authz_preview=1').status_code, 200)
        a = self.a
        self.assertIn('/apo-report-rows', a._AUTHZ_MACHINE_EXTRA)
        self.assertNotIn('/apo-report-rows', a._AUTHZ_CATALOG_READS)
        self.assertNotIn('/apo-report-rows', a._SCOPE_FILTERS)

    def test_workbook_route_reports_when_everything_was_a_sub(self):
        # TJX minus TK Maxx leaves only the substitute: a 404 that says so, with the header.
        r = self.client.get('/export-apo-brandcolor?customer=TJX&exclude_po=TK')
        self.assertEqual(r.status_code, 404)
        d = r.get_json()
        self.assertEqual(d['hidden_subs']['lines'], 1)
        self.assertEqual(json.loads(r.headers['X-Apo-Hidden-Subs']),
                         {'lines': 1, 'units': 1800, 'styles': ['ZTMNXX800SLY'], 'subs_ok': True, 'po_map_ok': True})

    def test_hidden_subs_header_shape(self):
        h = json.loads(self.a._hidden_subs_header({'lines': 2, 'units': 396, 'styles': ['ZA'], 'subs_ok': False,
                                                    'po_map_ok': False, 'rows': [{'po': 'secret'}]}))
        self.assertEqual(h, {'lines': 2, 'units': 396, 'styles': ['ZA'], 'subs_ok': False, 'po_map_ok': False})
        self.assertEqual(json.loads(self.a._hidden_subs_header(None)),
                         {'lines': 0, 'units': 0, 'styles': [], 'subs_ok': False, 'po_map_ok': True})


if __name__ == '__main__':
    unittest.main()
