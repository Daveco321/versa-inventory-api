"""Factory holds (Oct 1 2026): hide one factory's production everywhere on the
catalog, with an on/off switch. Offline: the app is imported with sockets
blocked (tests/_support.py) and S3 is replaced by an in-memory store. All
styles, factories and numbers here are synthetic (ZZ prefixes)."""
import json
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _support import load_app  # noqa: E402


def prod(ref, style, units, wh='JTW'):
    return {'production': ref, 'style': style, 'units': units, 'warehouse': wh, 'etd': '2027-01-15',
            'arrival': None, 'poName': 'ZZ TEST', 'brand': 'ZZ', 'fob_flag': False, 'fob_note': '',
            'shipmentNo': 'One', 'port_dated': False, 'etd_raw': '2027-01-15'}


def item(sku, wh=0, incoming=0, allocated=0):
    return {'sku': sku, 'brand': 'ZZ', 'brand_abbr': 'ZZ', 'brand_full': 'ZZ', 'name': 'ZZ ' + sku,
            'jtw': wh, 'tr': 0, 'dcw': 0, 'qa': 0, 'nj': 0, 'abfi': 0, 'incoming': incoming,
            'committed': 0, 'allocated': allocated, 'total_ats': wh + incoming + allocated,
            'total_warehouse': wh, 'container': 'N/A', 'receive_date': '', 'lot_number': 'N/A', 'image': ''}


LEDGER = [
    prod('NK99001', 'ZZNAPQ001SLD', 5004),            # held only
    prod('NK99001', 'ZZNAPQ002SLD', 2016),            # held only, no inventory row at all
    prod('NK99002', 'ZZNAPQ003SLD', 1000),            # held + visible production (mixed)
    prod('TF99001', 'ZZNAPQ003SLD', 3000),
    prod('NK99002', 'ZZNAPQ004SLD', 500),             # held, but warehouse stock exists
    prod('TF99002', 'ZZUSDP010SFS', 1836),            # other factory only
]
ITEMS = [
    item('ZZNAPQ001SLD', incoming=5004, allocated=-3000),   # like the Burlington allocation
    item('ZZNAPQ003SLD', incoming=4000),
    item('ZZNAPQ004SLD', wh=120, incoming=500),
    item('ZZUSDP010SFS', incoming=1836),
    item('ZZPLAIN001SLS', wh=50),
]


class FakeS3:
    """Just enough of boto3's client for the holds file."""

    def __init__(self):
        self.objects = {}

    def get_object(self, Bucket, Key):
        from botocore.exceptions import ClientError
        if Key not in self.objects:
            raise ClientError({'Error': {'Code': 'NoSuchKey', 'Message': 'missing'}}, 'GetObject')
        import io
        return {'Body': io.BytesIO(self.objects[Key])}

    def put_object(self, Bucket, Key, Body, ContentType=None):
        self.objects[Key] = Body


class PureHoldTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.a = load_app()

    def test_filter_production(self):
        a = self.a
        self.assertEqual(a._hold_filter_production(LEDGER, frozenset()), LEDGER)
        kept = a._hold_filter_production(LEDGER, frozenset({'NK'}))
        self.assertEqual([p['production'] for p in kept], ['TF99001', 'TF99002'])

    def test_items_follow_the_hold(self):
        a = self.a
        items, rep = a._apply_factory_hold_items(ITEMS, LEDGER, frozenset({'NK'}))
        by = {i['sku']: i for i in items}
        self.assertNotIn('ZZNAPQ001SLD', by)                       # only supply was held: gone
        self.assertEqual(by['ZZNAPQ003SLD']['incoming'], 3000)     # mixed: held units cut
        self.assertEqual(by['ZZNAPQ003SLD']['total_ats'], 3000)
        self.assertEqual(by['ZZNAPQ004SLD']['incoming'], 0)        # held production gone, stock stays
        self.assertEqual(by['ZZNAPQ004SLD']['total_ats'], 120)
        self.assertEqual(by['ZZUSDP010SFS']['incoming'], 1836)     # untouched
        self.assertEqual(by['ZZPLAIN001SLS']['total_ats'], 50)
        self.assertEqual(rep['styles_hidden'], 1)
        self.assertEqual(rep['hidden_styles'], ['ZZNAPQ001SLD'])
        self.assertEqual(rep['styles_cut'], 2)
        self.assertEqual(rep['units_cut'], 5004 + 1000 + 500)
        self.assertEqual(rep['held'], ['NK'])
        # the inputs are never mutated
        self.assertEqual(ITEMS[0]['incoming'], 5004)

    def test_no_hold_is_identity(self):
        a = self.a
        items, rep = a._apply_factory_hold_items(ITEMS, LEDGER, frozenset())
        self.assertEqual(items, ITEMS)
        self.assertEqual(rep['styles_hidden'], 0)

    def test_hold_codes(self):
        a = self.a
        self.assertEqual(a._hold_codes(['nk', ' TF ', 'bad!', '', None, 'ABC']), frozenset({'NK', 'TF'}))


class HoldRouteTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.a = load_app()
        cls.client = cls.a.app.test_client()

    def setUp(self):
        a = self.a
        self.s3 = FakeS3()
        self._saved = (a.get_s3, a._request_identity, a.load_production_from_dropbox, a._group_by_brand)
        a.get_s3 = lambda: self.s3
        a.load_production_from_dropbox = lambda: list(a._production_data)
        a._group_by_brand = lambda items: {}
        with a._production_lock:
            a._production_data_all = list(LEDGER)
            a._production_data = list(LEDGER)
            a._production_last_sync = 10 ** 12          # fresh, so no Dropbox call
        with a._inv_lock:
            a._inventory['items'] = [dict(i) for i in ITEMS]
            a._inventory['items_raw'] = [dict(i) for i in ITEMS]
            a._inventory['item_count'] = len(ITEMS)
            a._inventory['hold_stamp'] = ''
            a._inventory['hold_report'] = None
            a._inventory['last_sync'] = '2099-01-01T00:00:00Z'
        with a._factory_holds_lock:
            a._factory_holds.update(held=frozenset(), loaded_at=0.0, ok=False)
        self.ident = {'tier': 'staff', 'is_admin': True, 'email': 'zz@example.test'}
        a._request_identity = lambda: self.ident

    def tearDown(self):
        a = self.a
        a.get_s3, a._request_identity, a.load_production_from_dropbox, a._group_by_brand = self._saved
        with a._production_lock:
            a._production_data_all = []
            a._production_data = []
            a._production_last_sync = 0
        with a._inv_lock:
            a._inventory['items'] = []
            a._inventory['items_raw'] = None
            a._inventory['hold_stamp'] = None
            a._inventory['hold_report'] = None
        with a._factory_holds_lock:
            a._factory_holds.update(held=frozenset(), loaded_at=0.0)

    def post(self, held, note=''):
        return self.client.post('/admin/factory-holds', json={'held': held, 'note': note})

    def test_switch_on_then_off(self):
        a = self.a
        r = self.post(['NK'], 'quality hold')
        self.assertEqual(r.status_code, 200, r.get_data(as_text=True)[:300])
        d = r.get_json()
        self.assertEqual(d['held'], ['NK'])
        self.assertTrue(d['applied'])
        self.assertEqual(d['report']['hidden_styles'], ['ZZNAPQ001SLD'])
        self.assertEqual(d['saved']['updated_by'], 'zz@example.test')
        nk = next(f for f in d['factories'] if f['code'] == 'NK')
        self.assertTrue(nk['held'])
        self.assertEqual((nk['rows'], nk['units'], nk['styles']), (4, 5004 + 2016 + 1000 + 500, 4))
        saved = json.loads(self.s3.objects[a.S3_FACTORY_HOLDS_KEY])
        self.assertEqual(saved['held'], ['NK'])
        # every viewer path now lacks the held rows
        p = self.client.get('/production').get_json()['production']
        self.assertEqual([x['production'] for x in p], ['TF99001', 'TF99002'])
        inv = self.client.get('/inventory').get_json()['inventory']
        skus = {i['sku'] for i in inv}
        self.assertNotIn('ZZNAPQ001SLD', skus)
        self.assertEqual(next(i for i in inv if i['sku'] == 'ZZNAPQ003SLD')['incoming'], 3000)
        # a factory account still sees the whole ledger
        self.ident = {'tier': 'factory', 'prefix': 'NK'}
        p = self.client.get('/production').get_json()['production']
        self.assertEqual(len(p), len(LEDGER))
        self.ident = {'tier': 'staff', 'is_admin': True, 'email': 'zz@example.test'}
        # health reports it
        h = self.client.get('/health').get_json()['factory_holds']
        self.assertEqual(h['held'], ['NK'])
        self.assertEqual(h['production_rows_hidden'], 4)
        # off again: everything comes back
        r = self.post([])
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.get_json()['held'], [])
        p = self.client.get('/production').get_json()['production']
        self.assertEqual(len(p), len(LEDGER))
        inv = self.client.get('/inventory').get_json()['inventory']
        self.assertEqual(next(i for i in inv if i['sku'] == 'ZZNAPQ001SLD')['incoming'], 5004)

    def test_only_admins_or_the_machine_key_can_flip(self):
        self.ident = {'tier': 'staff', 'is_admin': False}
        self.assertEqual(self.post(['NK']).status_code, 403)
        self.assertEqual(self.client.get('/admin/factory-holds').status_code, 200)   # staff may look
        self.ident = {'tier': 'machine'}
        self.assertEqual(self.post(['NK']).status_code, 200)
        self.ident = {'tier': 'staff', 'is_admin': True}
        self.assertEqual(self.post(['XX']).status_code, 400)          # unknown code
        self.assertEqual(self.post('NK').status_code, 400)            # not a list

    def test_other_workers_follow_the_switch_file(self):
        """Another worker flips the file: this worker re-filters on its next tick."""
        a = self.a
        self.s3.objects[a.S3_FACTORY_HOLDS_KEY] = json.dumps({'held': ['NK']}).encode('utf-8')
        with a._factory_holds_lock:
            a._factory_holds['loaded_at'] = 0.0                      # cache expired
        inv = self.client.get('/inventory').get_json()['inventory']
        self.assertNotIn('ZZNAPQ001SLD', {i['sku'] for i in inv})
        self.assertEqual([x['production'] for x in self.client.get('/production').get_json()['production']],
                         ['TF99001', 'TF99002'])

    def test_sync_fails_closed_without_the_ledger(self):
        a = self.a
        self.s3.objects[a.S3_FACTORY_HOLDS_KEY] = json.dumps({'held': ['NK']}).encode('utf-8')
        with a._factory_holds_lock:
            a._factory_holds['loaded_at'] = 0.0
        with a._production_lock:
            a._production_data_all = []
            a._production_data = []
        items, rep = a._hold_filter_synced_items([dict(i) for i in ITEMS])
        self.assertIsNone(items)
        self.assertIsNone(rep)
        # and with no hold the items pass straight through
        self.s3.objects[a.S3_FACTORY_HOLDS_KEY] = json.dumps({'held': []}).encode('utf-8')
        with a._factory_holds_lock:
            a._factory_holds['loaded_at'] = 0.0
        items, rep = a._hold_filter_synced_items([dict(i) for i in ITEMS])
        self.assertEqual(len(items), len(ITEMS))


if __name__ == '__main__':
    unittest.main()
