"""The Amazon agent inside the API (Nov 23 2026): routes and who may call them, the gate entries,
the holds and prepack math on live-shaped feed rows, and the Amazon mailbox hand-off in
email_agent.py. Offline; synthetic styles and made-up addresses only."""
import os
import sys
import time
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _support import load_app  # noqa: E402
import email_agent as EA  # noqa: E402

APPROVER = 'owner@example.test'
BOX = 'agent-box@example.test'


class FakeAgent:
    def __init__(self):
        self.runs, self.updates = [], []

    def settings(self):
        return {'approvers': [APPROVER]}

    def status(self):
        return {'connected_to_amazon': False, 'ready': False, 'missing_settings': ['transcal_to']}

    def run(self, now=None, dry_run=False):
        self.runs.append(dry_run)
        return {'status': 'dry_run' if dry_run else 'proposed'}

    def update_settings(self, changes):
        self.updates.append(changes)
        return {'approvers': [APPROVER], **changes}


class RouteTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.a = load_app()

    def setUp(self):
        a = self.a
        self.saved = {n: getattr(a, n) for n in ('_request_identity', '_amazon_agent')}
        self.ident = {'tier': 'machine'}
        a._request_identity = lambda: self.ident
        self.agent = FakeAgent()
        a._amazon_agent = self.agent
        self.client = a.app.test_client()

    def tearDown(self):
        for n, v in self.saved.items():
            setattr(self.a, n, v)

    def test_registered(self):
        rules = {r.rule for r in self.a.app.url_map.iter_rules()}
        for path in ('/admin/amazon/status', '/admin/amazon/run', '/admin/amazon/settings'):
            self.assertIn(path, rules)
        self.assertIn('/admin/amazon/status', self.a._AUTHZ_MACHINE_EXTRA)

    def test_who_may_call(self):
        self.assertEqual(200, self.client.get('/admin/amazon/status').status_code)
        self.ident = {'tier': 'staff', 'email': APPROVER.upper()}
        self.assertEqual(200, self.client.get('/admin/amazon/status').status_code)
        self.ident = {'tier': 'staff', 'email': 'colleague@example.test'}
        self.assertEqual(403, self.client.get('/admin/amazon/status').status_code)
        self.ident = {'tier': 'factory', 'email': APPROVER}
        self.assertEqual(403, self.client.post('/admin/amazon/run', json={}).status_code)

    def test_status_carries_switches_not_amazon_data(self):
        body = self.client.get('/admin/amazon/status').get_json()
        self.assertFalse(body['enabled'])
        self.assertEqual(['transcal_to'], body['missing_settings'])
        self.assertIn('store', body)

    def test_run_is_a_dry_run_unless_asked(self):
        r = self.client.post('/admin/amazon/run', json={})
        self.assertEqual(202, r.status_code)
        self.assertTrue(r.get_json()['dry_run'])
        for _ in range(100):
            if self.a._amz_job['thread'] and not self.a._amz_job['thread'].is_alive():
                break
            time.sleep(0.02)
        self.assertEqual([True], self.agent.runs)
        self.client.post('/admin/amazon/run', json={'dry_run': False})
        for _ in range(100):
            if not self.a._amz_job['thread'].is_alive():
                break
            time.sleep(0.02)
        self.assertEqual([True, False], self.agent.runs)

    def test_settings(self):
        r = self.client.post('/admin/amazon/settings', json={'transcal_to': ['tickets@warehouse.example.test']})
        self.assertEqual(200, r.status_code)
        self.assertEqual([{'transcal_to': ['tickets@warehouse.example.test']}], self.agent.updates)

    def test_the_gate_lets_the_machine_key_through(self):
        a = self.a
        saved = a.AUTH_MODE
        try:
            a.AUTH_MODE = 'on'
            self.assertEqual(202, self.client.post('/admin/amazon/run', json={}).status_code)
            self.ident = None
            self.assertEqual(401, self.client.post('/admin/amazon/run', json={}).status_code)
        finally:
            a.AUTH_MODE = saved
            for _ in range(100):
                t = a._amz_job['thread']
                if not t or not t.is_alive():
                    break
                time.sleep(0.02)


class FeedMathTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.a = load_app()

    def setUp(self):
        a = self.a
        self.saved_items = a._inventory.get('items')
        self.saved_apo = list(a._apo_data)
        self.saved_fresh = a._fresh_prepack_defaults

    def tearDown(self):
        a = self.a
        a._inventory['items'] = self.saved_items
        a._apo_data[:] = self.saved_apo
        a._fresh_prepack_defaults = self.saved_fresh

    def test_holds_count_everyone_but_amazon(self):
        a = self.a
        a._inventory['items'] = [
            {'sku': 'AMZZSU901SLS', 'tr': 720, 'committed': -36, 'allocated': -100, 'brand_abbr': 'NAUTICA'},
            {'sku': 'AMZZSU901SLS-1515.53233', 'tr': 144, 'committed': 0, 'allocated': -72, 'brand_abbr': 'NAUTICA'},
            {'sku': 'ROZZSU901SLS', 'tr': 9999, 'committed': 0, 'allocated': 0, 'brand_abbr': 'NAUTICA'}]
        a._apo_data[:] = [{'customer': 'AMAZON', 'style': 'AMZZSU901SLS', 'qty': 80, 'po': 'X'},
                          {'customer': 'BURLINGTON', 'style': 'AMZZSU901SLS', 'qty': 20, 'po': 'Y'},
                          {'customer': 'AMAZON', 'style': 'AMZZSU901SLS-1515.53233', 'qty': 72, 'po': 'Z'}]
        rows = {r.sku: r for r in a._amz_stock_rows()}
        self.assertEqual({'AMZZSU901SLS', 'AMZZSU901SLS-1515.53233'}, set(rows))       # store accounts never
        self.assertEqual((720, 56), (rows['AMZZSU901SLS'].transcal, rows['AMZZSU901SLS'].hold))
        self.assertEqual(0, rows['AMZZSU901SLS-1515.53233'].hold)                        # all of it is Amazon's

    def test_prepack_comes_from_the_platform_rules(self):
        a = self.a
        a._inventory['items'] = [{'sku': 'AMZZSU901SLS', 'tr': 36, 'brand_abbr': 'NAUTICA'}]
        a._fresh_prepack_defaults = lambda: [{
            'id': 'seed-slim-ls', 'label': 'Slim Fit Long Sleeve', 'category': 'long_sleeve', 'fits': ['SL'],
            'master_qty': 36, 'sizes': [['14-14.5 / 32-33', 4], ['15-15.5 / 32-33', 8], ['15-15.5 / 34-35', 4],
                                        ['16-16.5 / 32-33', 4], ['16-16.5 / 34-35', 8], ['17-17.5 / 34-35', 8]]}]
        ratio = a._amz_prepack_resolver()('AMZZSU901SLS')
        self.assertEqual({'S': 4, 'M': 8, 'MT': 4, 'L': 4, 'LT': 8, 'XL': 8}, ratio)


class ReplyAgent(FakeAgent):
    def __init__(self):
        super().__init__()
        self.replies = []

    def settings(self):
        return {'approvers': [APPROVER], 'david': APPROVER}

    def handle_reply(self, sender, subject, text, now=None):
        self.replies.append((sender, subject, text))
        return {'status': 'sent'}


class MailboxTests(unittest.TestCase):
    """The Amazon mailbox rides email_agent.add_mailbox: wants() picks its mail, accept() claims
    it once and reads it on a thread, and only a verified sender reaches the agent."""

    @classmethod
    def setUpClass(cls):
        cls.a = load_app()

    def setUp(self):
        a = self.a
        self.saved_a = {n: getattr(a, n) for n in ('AMAZON_MAILBOX', '_amazon_agent', '_amz_send')}
        self.saved_ea = {n: getattr(EA, n) for n in ('_claim', '_fetch_received', '_sender_is_authentic',
                                                      '_is_machine_mail', 'RESEND_API_KEY', 'EMAIL_AGENT_REPLY_TO',
                                                      'EMAIL_AGENT_FROM')}
        a.AMAZON_MAILBOX = BOX
        self.agent = ReplyAgent()
        a._amazon_agent = self.agent
        self.notes = []
        a._amz_send = lambda to, subject, html, attachments, cc=None: self.notes.append((to, html)) or (True, 'ok')
        self.claimed = set()
        EA._claim = lambda email_id: not (email_id in self.claimed or self.claimed.add(email_id))
        EA._fetch_received = lambda email_id, data: {'text': 'YES\n\n> quoted', 'headers': {'h': 'v'}}
        self.authentic = (True, 'dmarc=pass')
        EA._sender_is_authentic = lambda addr, headers: self.authentic
        EA._is_machine_mail = lambda data, headers: None

    def tearDown(self):
        for n, v in self.saved_a.items():
            setattr(self.a, n, v)
        for n, v in self.saved_ea.items():
            setattr(EA, n, v)

    def event(self, to, email_id='e1'):
        return {'type': 'email.received', 'data': {'email_id': email_id, 'from': f'Owner <{APPROVER}>', 'to': to,
                                                    'subject': 'Re: Amazon shipment [ref A1123a]'}}

    def wait_threads(self):
        import threading
        for _ in range(200):
            if not any(t.name.startswith('amazon-reply-') for t in threading.enumerate()):
                return
            time.sleep(0.01)

    def test_only_mail_to_the_amazon_box_is_wanted(self):
        self.assertTrue(self.a._amz_wants(self.event([f'Versa Amazon <{BOX.upper()}>'])['data']))
        self.assertFalse(self.a._amz_wants(self.event(['inventory@example.test'])['data']))
        self.a.AMAZON_MAILBOX = ''
        self.assertFalse(self.a._amz_wants(self.event([BOX])['data']))

    def test_a_verified_reply_reaches_the_agent_once(self):
        self.assertEqual('queued', self.a._amz_accept(self.event([BOX])))
        self.assertEqual('duplicate', self.a._amz_accept(self.event([BOX])))
        self.wait_threads()
        self.assertEqual([(APPROVER, 'Re: Amazon shipment [ref A1123a]', 'YES\n\n> quoted')], self.agent.replies)

    def test_an_unverified_reply_does_nothing_and_david_hears_about_it(self):
        self.authentic = (False, 'dmarc=fail')
        self.a._amz_accept(self.event([BOX], 'e2'))
        self.wait_threads()
        self.assertEqual([], self.agent.replies)
        self.assertEqual(APPROVER, self.notes[0][0])
        self.assertIn('could not be verified', self.notes[0][1])

    def test_send_takes_a_list_its_own_from_and_its_own_reply_to(self):
        sent = {}

        class R:
            status_code = 200

            @staticmethod
            def json():
                return {'id': 'm1'}
        saved_post = EA.requests.post
        EA.RESEND_API_KEY = 'test-key'
        EA.EMAIL_AGENT_FROM = 'Versa Inventory <inventory-box@example.test>'
        EA.EMAIL_AGENT_REPLY_TO = 'inventory-box@example.test'
        EA.requests.post = lambda url, headers=None, json=None, timeout=None: sent.update(json) or R()
        try:
            ok, _ = EA._send_reply(['a@example.test', 'b@example.test'], 'S', '<p>x</p>', [],
                                   {'always_cc': [], 'reply_to': BOX}, cc=[], sender=f'Versa Amazon <{BOX}>')
            self.assertTrue(ok)
            self.assertEqual(['a@example.test', 'b@example.test'], sent['to'])
            self.assertEqual(f'Versa Amazon <{BOX}>', sent['from'])
            self.assertEqual(BOX, sent['reply_to'])                    # a YES comes back to the Amazon box
            sent.clear()
            EA._send_reply('c@example.test', 'S', '<p>x</p>', [], {'always_cc': [], 'reply_to': BOX}, cc=[])
            self.assertEqual('inventory-box@example.test', sent['reply_to'])   # the inventory mailbox is unchanged
        finally:
            EA.requests.post = saved_post


if __name__ == '__main__':
    unittest.main()
