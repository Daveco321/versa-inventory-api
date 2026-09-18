"""
Synthetic tests for the inbound email agent (email_agent.py) and its app.py wiring.

Covers the parts that decide whether a stranger can drive the agent: the Svix
signature on the webhook (including the published Svix test vector, so a change
here fails loudly instead of quietly accepting forged posts), the sender
allow-list, the refusal of any tier other than admin/staff, the auto-reply and
loop guards, the per-sender rate limit, once-only delivery, and the attachment
size cap. Then static checks that app.py still wires it up the way the design
assumes.

SYNTHETIC ONLY. Made-up addresses (all @example.test or @z-fake.test), made-up
ids, no network: requests and S3 are stubbed. Run from the repo root:
    python -B -m unittest tests.test_email_agent -v
"""
import ast
import base64
import hashlib
import hmac
import io
import json
import os
import sys
import time
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

import email_agent as EA  # noqa: E402

EA._HOOKS['machine_key'] = 'z-fake-machine-key-for-tests'


# ── Stubs ────────────────────────────────────────────────────────────────────
class FakeS3:
    """Enough S3 for the config object and the dedupe markers."""

    def __init__(self, objects=None):
        self.objects = dict(objects or {})
        self.puts = []
        self.deletes = []

    def get_object(self, Bucket=None, Key=None):
        if Key not in self.objects:
            raise Exception('NoSuchKey: ' + Key)
        return {'Body': io.BytesIO(self.objects[Key])}

    def head_object(self, Bucket=None, Key=None):
        if Key not in self.objects:
            raise Exception('404 Not Found')
        return {'ContentLength': len(self.objects[Key])}

    def put_object(self, Bucket=None, Key=None, Body=None, ContentType=None):
        self.objects[Key] = Body if isinstance(Body, bytes) else str(Body).encode()
        self.puts.append(Key)
        return {}

    def list_objects_v2(self, Bucket=None, Prefix='', MaxKeys=None, ContinuationToken=None):
        keys = sorted(k for k in self.objects if k.startswith(Prefix))
        return {'Contents': [{'Key': k} for k in keys], 'IsTruncated': False}

    def delete_object(self, Bucket=None, Key=None):
        self.objects.pop(Key, None)
        self.deletes.append(Key)
        return {}


class FakeResponse:
    def __init__(self, status_code=200, content=b'', payload=None):
        self.status_code = status_code
        self.content = content
        self.text = content.decode('utf-8', 'replace')
        self._payload = payload

    def json(self):
        return self._payload or {}


def use_s3(test, objects=None):
    """Point the module at a fake bucket and clear every cache it keeps."""
    EA._HOOKS['machine_key'] = 'z-fake-machine-key-for-tests'
    s3 = FakeS3(objects)
    EA._HOOKS['get_s3'] = lambda: s3
    EA._HOOKS['s3_bucket'] = 'z-fake-bucket'
    EA._cfg_cache['data'] = None
    EA._cfg_cache['at'] = 0.0
    EA._seen_ids.clear()
    EA._rate_hits.clear()
    test.addCleanup(EA._seen_ids.clear)
    test.addCleanup(EA._rate_hits.clear)
    return s3


def cfg_object(payload):
    """The stored allow-list, written the way the module writes it."""
    return {EA._config_key(): EA._seal(payload)}


# SES writes this on every message it receives; it is what proves a From address
# was not simply typed by whoever sent the mail.
PASS_HEADERS = {'Authentication-Results': ('mx.amazonses.com; spf=pass '
                                           'smtp.mailfrom=example.test; dkim=pass '
                                           'header.i=@example.test; dmarc=pass')}


# ── The webhook signature is the only credential on an open route ────────────
class SignatureTests(unittest.TestCase):
    SECRET = 'whsec_' + base64.b64encode(b'z-fake-signing-key-0123456789').decode()

    def sign(self, msg_id, ts, body, secret=None):
        key = base64.b64decode((secret or self.SECRET).split('_', 1)[1])
        signed = f'{msg_id}.{ts}.'.encode() + body
        return 'v1,' + base64.b64encode(hmac.new(key, signed, hashlib.sha256).digest()).decode()

    def test_svix_published_vector(self):
        """The documented Svix vector. Resend signs with Svix, so if this stops
        matching, real webhooks are being rejected (or worse, forgeries are
        being accepted) and the failure must not be silent."""
        secret = 'whsec_MfKQ9r8GKYqrTwjUPD8ILPZIo2LaLaSw'
        msg_id = 'msg_p5jXN8AQM9LWM0D4loKWxJek'
        ts = 1614265330
        body = b'{"test": 2432232314}'
        header = 'v1,g0hM9SsE+OTPJTGt/tmIKtSyZlE3uFJELVlNIOLJ1OE='
        # The vector is from 2021, so stand the clock next to it and run the
        # real verifier: this proves the shipped function accepts what Svix
        # actually signs, not just that two copies of the same maths agree.
        real_time = EA.time.time
        self.addCleanup(setattr, EA.time, 'time', real_time)
        EA.time.time = lambda: float(ts)
        ok, why = EA._verify_signature(secret, msg_id, ts, header, body)
        self.assertTrue(ok, why)
        self.assertFalse(EA._verify_signature(secret, msg_id, ts, header, body + b'!')[0])

    def test_good_signature_passes(self):
        body = b'{"type":"email.received"}'
        ts = int(time.time())
        ok, why = EA._verify_signature(self.SECRET, 'msg_1', ts, self.sign('msg_1', ts, body), body)
        self.assertTrue(ok, why)

    def test_multiple_signatures_one_valid(self):
        body = b'{"type":"email.received"}'
        ts = int(time.time())
        header = 'v1,AAAAinvalid= ' + self.sign('msg_1', ts, body)
        self.assertTrue(EA._verify_signature(self.SECRET, 'msg_1', ts, header, body)[0])

    def test_tampered_body_fails(self):
        body = b'{"type":"email.received"}'
        ts = int(time.time())
        sig = self.sign('msg_1', ts, body)
        ok, why = EA._verify_signature(self.SECRET, 'msg_1', ts, sig, body + b' ')
        self.assertFalse(ok)
        self.assertIn('mismatch', why)

    def test_other_id_or_timestamp_fails(self):
        body = b'{}'
        ts = int(time.time())
        sig = self.sign('msg_1', ts, body)
        self.assertFalse(EA._verify_signature(self.SECRET, 'msg_2', ts, sig, body)[0])
        self.assertFalse(EA._verify_signature(self.SECRET, 'msg_1', ts - 1, sig, body)[0])

    def test_stale_timestamp_fails(self):
        body = b'{}'
        ts = int(time.time()) - 3600
        ok, why = EA._verify_signature(self.SECRET, 'msg_1', ts, self.sign('msg_1', ts, body), body)
        self.assertFalse(ok)
        self.assertIn('tolerance', why)

    def test_unknown_version_tag_fails(self):
        body = b'{}'
        ts = int(time.time())
        sig = self.sign('msg_1', ts, body).replace('v1,', 'v0,')
        self.assertFalse(EA._verify_signature(self.SECRET, 'msg_1', ts, sig, body)[0])

    def test_no_secret_refuses_everything(self):
        body = b'{}'
        ts = int(time.time())
        ok, why = EA._verify_signature('', 'msg_1', ts, self.sign('msg_1', ts, body), body)
        self.assertFalse(ok)
        self.assertIn('missing', why)

    def test_missing_headers_refuse(self):
        self.assertFalse(EA._verify_signature(self.SECRET, None, None, None, b'{}')[0])


# ── Who is allowed to drive it ───────────────────────────────────────────────
class AllowListTests(unittest.TestCase):
    def test_address_beats_domain(self):
        use_s3(self, cfg_object({'senders': [{'email': 'boss@example.test', 'tier': 'admin'}],
                                 'domains': [{'domain': 'example.test', 'tier': 'staff'}]}))
        cfg = EA._load_config(force=True)
        self.assertEqual('admin', EA._identify_sender('boss@example.test', cfg)['tier'])
        self.assertEqual('staff', EA._identify_sender('rep@example.test', cfg)['tier'])

    def test_unknown_sender_is_none(self):
        use_s3(self, cfg_object({'senders': [{'email': 'boss@example.test', 'tier': 'admin'}]}))
        cfg = EA._load_config(force=True)
        self.assertIsNone(EA._identify_sender('stranger@z-fake.test', cfg))
        self.assertIsNone(EA._identify_sender('', cfg))

    def test_case_and_spacing_do_not_open_a_hole(self):
        use_s3(self, cfg_object({'senders': [{'email': '  BOSS@Example.TEST ', 'tier': 'admin'}]}))
        cfg = EA._load_config(force=True)
        self.assertIsNotNone(EA._identify_sender('Boss@EXAMPLE.test', cfg))

    def test_lookalike_domain_is_not_a_match(self):
        use_s3(self, cfg_object({'domains': [{'domain': 'example.test', 'tier': 'staff'}]}))
        cfg = EA._load_config(force=True)
        self.assertIsNone(EA._identify_sender('rep@notexample.test', cfg))
        self.assertIsNone(EA._identify_sender('rep@example.test.z-fake.test', cfg))

    def test_customer_tier_is_refused_and_reported(self):
        """The data tools have no customer view, so an external customer must
        not be able to reach them by being listed in the config."""
        use_s3(self, cfg_object({'senders': [{'email': 'buyer@z-fake.test', 'tier': 'customer'}]}))
        cfg = EA._load_config(force=True)
        self.assertEqual([], cfg['senders'])
        self.assertIsNone(EA._identify_sender('buyer@z-fake.test', cfg))
        self.assertEqual(1, len(cfg['_rejected']))
        self.assertIn('customer', cfg['_rejected'][0]['reason'])

    def test_unknown_tier_on_a_domain_is_refused(self):
        use_s3(self, cfg_object({'domains': [{'domain': 'z-fake.test', 'tier': 'everyone'}]}))
        cfg = EA._load_config(force=True)
        self.assertEqual([], cfg['domains'])
        self.assertEqual(1, len(cfg['_rejected']))

    def test_junk_rows_are_dropped_not_fatal(self):
        use_s3(self, cfg_object({'senders': ['not-a-dict', {'email': 'no-at-sign', 'tier': 'admin'},
                                             {'email': 'ok@example.test', 'tier': 'staff'}]}))
        cfg = EA._load_config(force=True)
        self.assertEqual(['ok@example.test'], [r['email'] for r in cfg['senders']])

    def test_missing_config_object_is_the_empty_allow_list(self):
        use_s3(self, {})
        cfg = EA._load_config(force=True)
        self.assertEqual([], cfg['senders'])
        self.assertEqual([], cfg['domains'])
        self.assertIsNone(EA._identify_sender('anyone@z-fake.test', cfg))


# ── Loops, robots and floods ─────────────────────────────────────────────────
class GuardTests(unittest.TestCase):
    def test_our_own_reply_is_not_answered_again(self):
        self.assertEqual('our own mail', EA._is_machine_mail({}, {'X-Versa-Agent': '1'}))

    def test_auto_replies_and_lists_are_skipped(self):
        self.assertIsNotNone(EA._is_machine_mail({}, {'Auto-Submitted': 'auto-replied'}))
        self.assertIsNotNone(EA._is_machine_mail({}, {'Precedence': 'bulk'}))
        self.assertIsNotNone(EA._is_machine_mail({}, {'List-Id': '<x.example.test>'}))
        self.assertIsNotNone(EA._is_machine_mail({}, {'X-Autoreply': 'yes'}))

    def test_auto_submitted_no_is_a_real_person(self):
        self.assertIsNone(EA._is_machine_mail({'from': 'rep@example.test'}, {'Auto-Submitted': 'no'}))

    def test_bounces_and_out_of_office_are_skipped(self):
        self.assertIsNotNone(EA._is_machine_mail({'from': 'MAILER-DAEMON@z-fake.test'}, {}))
        self.assertIsNotNone(EA._is_machine_mail({'from': 'a@b.test', 'subject': 'Out of office'}, {}))
        self.assertIsNotNone(EA._is_machine_mail({'from': 'a@b.test', 'subject': 'Undeliverable: x'}, {}))

    def test_a_normal_request_passes(self):
        self.assertIsNone(EA._is_machine_mail(
            {'from': 'Rep <rep@example.test>', 'subject': 'Nautica B&T deck please'}, {}))

    def test_rate_limit_per_sender(self):
        use_s3(self, {})
        for _ in range(3):
            self.assertTrue(EA._rate_ok('rep@example.test', 3))
        self.assertFalse(EA._rate_ok('rep@example.test', 3))
        self.assertTrue(EA._rate_ok('other@example.test', 3), 'the limit is per sender')

    def test_delivery_is_claimed_once(self):
        """Svix retries a delivery it thinks failed; the second one must not
        run the agent (or bill for it) again."""
        s3 = use_s3(self, {})
        self.assertTrue(EA._claim('em_1'))
        self.assertFalse(EA._claim('em_1'))
        self.assertIn(f'{EA._private_prefix()}/seen/em_1.json', s3.objects)

    def test_claim_survives_a_restart(self):
        s3 = use_s3(self, {})
        EA._claim('em_2')
        EA._seen_ids.clear()          # a new process, same bucket
        self.assertFalse(EA._claim('em_2'))
        self.assertIsNotNone(s3)


# ── Attachments ──────────────────────────────────────────────────────────────
class AttachmentTests(unittest.TestCase):
    def setUp(self):
        self.real_get = EA.requests.get
        self.addCleanup(setattr, EA.requests, 'get', self.real_get)

    def test_file_under_the_cap_is_attached_with_its_name(self):
        EA.requests.get = lambda url, timeout=None: FakeResponse(200, b'PDFBYTES')
        out, skipped = EA._collect_attachments(
            [{'url': 'https://z-fake.test/claude%20uploaded/Nautica%20B%26T%20Presentation.pdf'}], 15)
        self.assertEqual([], skipped)
        self.assertEqual('Nautica B&T Presentation.pdf', out[0]['filename'])
        self.assertEqual(b'PDFBYTES', base64.b64decode(out[0]['content']))

    def test_oversized_file_falls_back_to_the_link(self):
        EA.requests.get = lambda url, timeout=None: FakeResponse(200, b'x' * (2 * 1024 * 1024))
        out, skipped = EA._collect_attachments([{'url': 'https://z-fake.test/big.pdf'}], 1)
        self.assertEqual([], out)
        self.assertIn('over the cap', skipped[0]['reason'])

    def test_a_failed_download_does_not_sink_the_reply(self):
        EA.requests.get = lambda url, timeout=None: FakeResponse(404, b'no')
        out, skipped = EA._collect_attachments([{'url': 'https://z-fake.test/gone.pdf'}], 15)
        self.assertEqual([], out)
        self.assertIn('404', skipped[0]['reason'])

    def test_the_same_file_is_attached_once(self):
        EA.requests.get = lambda url, timeout=None: FakeResponse(200, b'ok')
        out, _ = EA._collect_attachments([{'url': 'https://z-fake.test/a.pdf'},
                                          {'url': 'https://z-fake.test/a.pdf'}], 15)
        self.assertEqual(1, len(out))


# ── Reading the message ──────────────────────────────────────────────────────
class BodyTests(unittest.TestCase):
    def test_html_only_mail_becomes_readable_text(self):
        txt = EA._plain_from_html(
            '<style>p{color:red}</style><p>Can I get a <b>Nautica</b> deck</p><br>thanks')
        self.assertIn('Nautica', txt)
        self.assertIn('thanks', txt)
        self.assertNotIn('color:red', txt)
        self.assertNotIn('<', txt)

    def test_entities_are_decoded(self):
        self.assertIn('B&T', EA._plain_from_html('<p>B&amp;T</p>'))


# ── Wiring in app.py ─────────────────────────────────────────────────────────
class WiringTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        with io.open(os.path.join(ROOT, 'app.py'), encoding='utf-8') as fh:
            cls.src = fh.read()
        cls.tree = ast.parse(cls.src)

    def test_webhook_path_is_open_and_registered(self):
        self.assertIn("'/inbound-email',", self.src)
        self.assertIn('from email_agent import register_email_routes', self.src)
        self.assertIn('register_email_routes(app,', self.src)

    def test_pnl_is_never_opened_by_this_change(self):
        """The P&L must stay off every open prefix and allow-list."""
        i = self.src.index('_AUTHZ_OPEN_PREFIXES = (')
        self.assertNotIn('/api/pnl', self.src[i:self.src.index(')', i)])

    def test_the_shared_loop_exists_and_the_chat_route_uses_it(self):
        names = [n.name for n in ast.walk(self.tree) if isinstance(n, ast.FunctionDef)]
        self.assertIn('_ai_agent_run', names)
        route = self.src[self.src.index('def api_ai_agent('):]
        route = route[:route.index('\ndef ')]
        self.assertIn('_ai_agent_run(client, convo, system, agent_tools', route)
        self.assertNotIn('messages.create(', route, 'the chat route must not keep its own loop')

    def test_email_surface_inherits_the_tool_rules_not_the_ui_rules(self):
        i = self.src.index('_AI_AGENT_TOOL_GUIDANCE_CORE = """')
        core = self.src[i:self.src.index('"""', i + 40)]
        self.assertIn('build_presentation', core)
        self.assertIn('CURATED SELECTIONS', core)
        self.assertNotIn('required JSON format', core)
        self.assertNotIn('chat bubble', core)
        self.assertIn('_AI_AGENT_TOOL_GUIDANCE = _AI_AGENT_TOOL_GUIDANCE_CORE + _AI_AGENT_PLATFORM_TAIL',
                      self.src)

    def test_admin_only_tools_are_still_gated_in_the_shared_loop(self):
        body = self.src[self.src.index('def _ai_agent_run('):]
        body = body[:body.index('\n@app.route')]
        self.assertIn('_AI_AGENT_ADMIN_TOOLS and not admin_ok', body)
        self.assertIn("artifacts.append(", body)


class PromptTests(unittest.TestCase):
    def test_the_email_prompt_names_the_sender_and_their_tier(self):
        EA._HOOKS['guidance_core'] = '<<CORE>>'
        sys_text = EA._build_system({'email': 'rep@example.test', 'name': 'Rep', 'tier': 'staff'}, {})
        self.assertIn('rep@example.test', sys_text)
        self.assertIn('<<CORE>>', sys_text)
        self.assertIn('not available to them', sys_text)
        self.assertIn('UNTRUSTED', sys_text)

    def test_an_admin_prompt_opens_the_history_tools(self):
        EA._HOOKS['guidance_core'] = ''
        sys_text = EA._build_system({'email': 'boss@example.test', 'name': '', 'tier': 'admin'}, {})
        self.assertIn('every tool is available', sys_text)


# ── The whole path, through the real Flask route ─────────────────────────────
class EndToEndTests(unittest.TestCase):
    """Posts a signed webhook at the live route and follows it to the reply."""

    SECRET = 'whsec_' + base64.b64encode(b'z-fake-e2e-signing-key-0000').decode()

    @classmethod
    def setUpClass(cls):
        sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
        from _support import load_app
        cls.app = load_app()
        cls.client = cls.app.app.test_client()

    def setUp(self):
        self.s3 = use_s3(self, cfg_object({
            'enabled': True,
            'senders': [{'email': 'boss@example.test', 'tier': 'admin', 'name': 'Boss'}],
            'notify': ['boss@example.test'],
        }))
        for name, value in (('RESEND_WEBHOOK_SECRET', self.SECRET),
                            ('RESEND_API_KEY', 'z-fake-resend-key'),
                            ('EMAIL_AGENT_FROM', 'Versa Inventory <ats@z-fake.test>')):
            self.addCleanup(setattr, EA, name, getattr(EA, name))
            setattr(EA, name, value)
        self.sent = []
        self.addCleanup(setattr, EA.requests, 'post', EA.requests.post)
        self.addCleanup(setattr, EA.requests, 'get', EA.requests.get)
        EA.requests.post = lambda url, headers=None, json=None, timeout=None: (
            self.sent.append(json) or FakeResponse(200, b'{}', {'id': 'sent_1'}))
        EA.requests.get = lambda url, headers=None, timeout=None: FakeResponse(200, b'PDF')

    def post(self, payload, secret=None, msg_id='msg_e2e'):
        body = json.dumps(payload).encode()
        ts = int(time.time())
        key = base64.b64decode((secret or self.SECRET).split('_', 1)[1])
        sig = 'v1,' + base64.b64encode(
            hmac.new(key, f'{msg_id}.{ts}.'.encode() + body, hashlib.sha256).digest()).decode()
        return self.client.post('/inbound-email', data=body,
                                content_type='application/json',
                                headers={'svix-id': msg_id, 'svix-timestamp': str(ts),
                                         'svix-signature': sig})

    @staticmethod
    def event(sender='boss@example.test', subject='Nautica deck please', email_id='em_e2e'):
        domain = sender.rsplit('@', 1)[-1]
        return {'type': 'email.received',
                'data': {'email_id': email_id, 'from': f'Boss <{sender}>', 'to': ['ats@z-fake.test'],
                         'subject': subject, 'text': 'Can I get a presentation of Nautica B&T in stock',
                         'headers': {'Authentication-Results': (
                             f'mx.amazonses.com; spf=pass smtp.mailfrom={domain}; '
                             f'dkim=pass header.i=@{domain}; dmarc=pass')}}}

    def stub_agent(self, answer='<p>Here it is.</p>', artifacts=None):
        self.addCleanup(EA._HOOKS.update, dict(EA._HOOKS))
        EA._HOOKS['agent_client'] = lambda: object()
        EA._HOOKS['model'] = lambda: 'z-fake-model'
        EA._HOOKS['guidance_core'] = '<<CORE>>'
        seen = {}

        def run(client, convo, system, tools, model, max_tokens, admin_ok, label=''):
            seen.update(convo=convo, system=system, tools=tools, admin_ok=admin_ok)
            return {'final_text': answer, 'tools_used': ['build_presentation'],
                    'artifacts': artifacts or [], 'usage': {}, 'elapsed_seconds': 1.0,
                    'iterations': 1}

        EA._HOOKS['agent_run'] = run
        return seen

    def test_unsigned_post_never_reaches_the_agent(self):
        calls = []
        self.addCleanup(setattr, EA, '_handle', EA._handle)
        EA._handle = lambda ev: calls.append(ev)
        r = self.client.post('/inbound-email', json=self.event())
        self.assertEqual(401, r.status_code)
        self.assertEqual([], calls)

    def test_wrong_secret_is_refused(self):
        calls = []
        self.addCleanup(setattr, EA, '_handle', EA._handle)
        EA._handle = lambda ev: calls.append(ev)
        other = 'whsec_' + base64.b64encode(b'z-fake-attacker-key-000000').decode()
        self.assertEqual(401, self.post(self.event(), secret=other).status_code)
        self.assertEqual([], calls)

    def test_signed_post_is_queued_once(self):
        calls = []
        self.addCleanup(setattr, EA, '_handle', EA._handle)
        EA._handle = lambda ev: calls.append(ev)
        self.assertEqual(200, self.post(self.event()).status_code)
        second = self.post(self.event(), msg_id='msg_retry')
        self.assertEqual(200, second.status_code)
        self.assertTrue(second.get_json().get('duplicate'))
        for _ in range(100):
            if calls:
                break
            time.sleep(0.01)
        self.assertEqual(1, len(calls), 'a retried delivery must not run the agent twice')

    def test_other_event_types_are_ignored(self):
        r = self.post({'type': 'email.delivered', 'data': {'email_id': 'em_x'}})
        self.assertEqual(200, r.status_code)
        self.assertEqual('email.delivered', r.get_json().get('ignored'))

    def test_an_allowed_sender_gets_the_answer_and_the_file(self):
        seen = self.stub_agent(artifacts=[{'url': 'https://z-fake.test/claude%20uploaded/Deck.pdf',
                                           'tool': 'build_presentation'}])
        EA._handle(self.event())
        self.assertEqual(1, len(self.sent), 'exactly one reply')
        msg = self.sent[0]
        self.assertEqual(['boss@example.test'], msg['to'])
        self.assertEqual('Re: Nautica deck please', msg['subject'])
        self.assertIn('Here it is.', msg['html'])
        self.assertEqual('Deck.pdf', msg['attachments'][0]['filename'])
        self.assertEqual('1', msg['headers']['X-Versa-Agent'])
        self.assertTrue(seen['admin_ok'], 'an admin sender keeps the history tools')
        self.assertIn('Nautica B&T in stock', seen['convo'][0]['content'])

    def test_a_staff_sender_loses_the_history_tools(self):
        use_s3(self, cfg_object({'senders': [{'email': 'rep@example.test', 'tier': 'staff'}]}))
        seen = self.stub_agent()
        EA._handle(self.event(sender='rep@example.test'))
        self.assertFalse(seen['admin_ok'])
        names = {t['name'] for t in seen['tools']}
        self.assertNotIn('past_orders_lookup', names)
        self.assertNotIn('sales_history_lookup', names)
        self.assertIn('build_presentation', names)

    def test_an_unknown_sender_gets_silence_and_david_gets_a_heads_up(self):
        self.stub_agent()
        EA._handle(self.event(sender='stranger@z-fake.test'))
        recipients = [addr for msg in self.sent for addr in msg['to']]
        self.assertNotIn('stranger@z-fake.test', recipients,
                         'never bounce to an address that may be forged')
        self.assertEqual(['boss@example.test'], recipients)

    def test_a_refusal_envelope_is_unwrapped_for_email(self):
        self.stub_agent(answer=json.dumps({'message': "I can't help with that request.",
                                           'actions': []}))
        EA._handle(self.event())
        self.assertIn("I can't help with that request.", self.sent[0]['html'])
        self.assertNotIn('actions', self.sent[0]['html'])

    def test_an_oversized_file_is_explained_not_dropped_silently(self):
        EA.requests.get = lambda url, headers=None, timeout=None: FakeResponse(
            200, b'x' * (3 * 1024 * 1024))
        use_s3(self, cfg_object({'senders': [{'email': 'boss@example.test', 'tier': 'admin'}],
                                 'attach_max_mb': 1}))
        self.stub_agent(answer='<p><a href="https://z-fake.test/Deck.pdf">Download</a></p>',
                        artifacts=[{'url': 'https://z-fake.test/Deck.pdf'}])
        EA._handle(self.event())
        html = self.sent[0]['html']
        self.assertNotIn('attachments', self.sent[0])
        self.assertIn('linked above rather than attached', html)
        self.assertIn('href', html)

    def test_the_reply_points_back_at_the_mailbox(self):
        """Replies are sent FROM a domain verified for sending, which is not
        the receive-only inbound domain. Without Reply-To, hitting Reply on an
        answer writes to an address nobody reads."""
        self.addCleanup(setattr, EA, 'EMAIL_AGENT_REPLY_TO', EA.EMAIL_AGENT_REPLY_TO)
        EA.EMAIL_AGENT_REPLY_TO = 'ats@z-fake-inbox.test'
        self.stub_agent()
        EA._handle(self.event())
        self.assertEqual('ats@z-fake-inbox.test', self.sent[0]['reply_to'])

    def test_no_reply_to_configured_leaves_the_field_off(self):
        self.addCleanup(setattr, EA, 'EMAIL_AGENT_REPLY_TO', EA.EMAIL_AGENT_REPLY_TO)
        EA.EMAIL_AGENT_REPLY_TO = ''
        self.stub_agent()
        EA._handle(self.event())
        self.assertNotIn('reply_to', self.sent[0])

    def test_a_run_is_logged_for_the_status_page(self):
        self.stub_agent()
        EA._handle(self.event(email_id='em_logged'))
        with EA._runs_lock:
            latest = EA._runs[0]
        self.assertEqual('answered', latest['status'])
        self.assertEqual('boss@example.test', latest['from'])
        self.assertTrue(any(k.startswith(EA._private_prefix()) and '/log/' in k
                                for k in self.s3.puts))

    def test_status_and_config_need_a_credential(self):
        self.assertEqual(401, self.client.get('/inbound-email/status').status_code)
        self.assertEqual(401, self.client.get('/inbound-email/config').status_code)
        self.assertEqual(401, self.client.post('/inbound-email/config',
                                               json={'enabled': False}).status_code)


# ── The chat route still answers the way the browser expects ─────────────────
class ChatRouteRegressionTests(unittest.TestCase):
    """The tool loop moved out of /api/ai-agent into _ai_agent_run. The page
    parses the reply envelope field by field, so the envelope is the contract."""

    class Block:
        def __init__(self, type_, **kw):
            self.type = type_
            for k, v in kw.items():
                setattr(self, k, v)

    class Usage:
        input_tokens = 11
        output_tokens = 22
        cache_read_input_tokens = 33
        cache_creation_input_tokens = 0

    class Resp:
        def __init__(self, content, stop_reason='end_turn'):
            self.content = content
            self.stop_reason = stop_reason
            self.usage = ChatRouteRegressionTests.Usage()

    @classmethod
    def setUpClass(cls):
        sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
        from _support import load_app
        cls.app = load_app()
        cls.client = cls.app.app.test_client()

    def stub_client(self, script):
        """script: a list of responses to hand back, one per model call."""
        calls = []
        turns = list(script)

        class Messages:
            def create(inner, **kw):
                calls.append(kw)
                return turns.pop(0)

        class Client:
            messages = Messages()

            def with_options(inner, **kw):
                return inner

        self.addCleanup(setattr, self.app, '_ai_agent_client', self.app._ai_agent_client)
        self.app._ai_agent_client = lambda: Client()
        return calls

    def test_a_plain_answer_keeps_the_whole_envelope(self):
        self.stub_client([self.Resp([self.Block('text', text='{"message":"1,200 units","actions":[]}')])])
        r = self.client.post('/api/ai-agent', json={'messages': [{'role': 'user', 'content': 'how many'}]})
        self.assertEqual(200, r.status_code)
        body = r.get_json()
        self.assertEqual('1,200 units', json.loads(body['content'][0]['text'])['message'])
        self.assertEqual('end_turn', body['stop_reason'])
        self.assertEqual(11, body['usage']['input_tokens'])
        self.assertEqual(33, body['usage']['cache_read_input_tokens'])
        self.assertEqual(1, body['agent']['iterations'])
        self.assertEqual([], body['agent']['tools'])
        self.assertIn('elapsed_seconds', body['agent'])
        self.assertIn('model', body)

    def test_a_tool_turn_runs_the_tool_and_reports_it(self):
        self.addCleanup(self.app._AI_AGENT_TOOL_FNS.update, dict(self.app._AI_AGENT_TOOL_FNS))
        self.app._AI_AGENT_TOOL_FNS['query_inventory'] = lambda params: {'total_ats': 4242}
        self.stub_client([
            self.Resp([self.Block('tool_use', id='tu_1', name='query_inventory', input={})]),
            self.Resp([self.Block('text', text='{"message":"4,242","actions":[]}')]),
        ])
        r = self.client.post('/api/ai-agent', json={'messages': [{'role': 'user', 'content': 'nautica'}]})
        body = r.get_json()
        self.assertEqual(['query_inventory'], body['agent']['tools'])
        self.assertEqual(2, body['agent']['iterations'])
        self.assertEqual(44, body['usage']['output_tokens'], 'usage adds up across both turns')

    def test_a_non_admin_never_gets_the_history_tools(self):
        calls = self.stub_client([self.Resp([self.Block('text', text='{}')])])
        self.client.post('/api/ai-agent', json={'messages': [{'role': 'user', 'content': 'past selling'}]})
        names = {t['name'] for t in calls[0]['tools']}
        for gated in self.app._AI_AGENT_ADMIN_TOOLS:
            self.assertNotIn(gated, names)

    def test_an_api_failure_is_a_502_not_a_traceback(self):
        class Boom:
            def with_options(inner, **kw):
                return inner

            class messages:
                @staticmethod
                def create(**kw):
                    raise RuntimeError('z-fake upstream failure')

        self.addCleanup(setattr, self.app, '_ai_agent_client', self.app._ai_agent_client)
        self.app._ai_agent_client = lambda: Boom()
        r = self.client.post('/api/ai-agent', json={'messages': [{'role': 'user', 'content': 'x'}]})
        self.assertEqual(502, r.status_code)
        self.assertIn('AI agent failed', r.get_json()['error'])

    def test_a_leading_assistant_turn_is_still_dropped(self):
        calls = self.stub_client([self.Resp([self.Block('text', text='{}')])])
        self.client.post('/api/ai-agent', json={'messages': [
            {'role': 'assistant', 'content': 'earlier'}, {'role': 'user', 'content': 'now'}]})
        self.assertEqual('user', calls[0]['messages'][0]['role'])

    def test_no_messages_is_still_a_400(self):
        self.stub_client([self.Resp([self.Block('text', text='{}')])])
        r = self.client.post('/api/ai-agent', json={'messages': []})
        self.assertEqual(400, r.status_code)

    def test_an_unconfigured_server_says_unavailable_before_anything_else(self):
        # The route builds the SDK client before it validates the body, the way
        # it always has: with no ANTHROPIC_API_KEY that is a 503, not a 400.
        r = self.client.post('/api/ai-agent', json={'messages': [{'role': 'user', 'content': 'x'}]})
        self.assertEqual(503, r.status_code)

    def test_the_runner_reports_built_files_to_its_caller(self):
        """The mailbox attaches whatever this list names."""
        self.addCleanup(self.app._AI_AGENT_TOOL_FNS.update, dict(self.app._AI_AGENT_TOOL_FNS))
        self.app._AI_AGENT_TOOL_FNS['build_presentation'] = lambda params: {
            'download_url': 'https://z-fake.test/Deck.pdf', 'format': 'PDF presentation (photo cards)'}
        self.stub_client([
            self.Resp([self.Block('tool_use', id='tu_1', name='build_presentation', input={})]),
            self.Resp([self.Block('text', text='done')]),
        ])

        class Client:
            def with_options(inner, **kw):
                return inner

        run = self.app._ai_agent_run(self.app._ai_agent_client(),
                                     [{'role': 'user', 'content': 'deck'}], [],
                                     self.app._AI_AGENT_TOOLS, 'z-fake-model', 1000, True)
        self.assertEqual([{'tool': 'build_presentation', 'url': 'https://z-fake.test/Deck.pdf',
                           'format': 'PDF presentation (photo cards)'}], run['artifacts'])
        self.assertEqual('done', run['final_text'])


# ── The From header is a claim, not proof ────────────────────────────────────
class SenderAuthenticityTests(unittest.TestCase):
    """An allow-list keyed on a From address is worthless if anyone can type
    that address. These are the checks that make the allow-list mean something."""

    def ar(self, results):
        return {'Authentication-Results': results}

    def test_dmarc_pass_is_enough(self):
        ok, why = EA._sender_is_authentic(
            'boss@example.test', self.ar('mx.amazonses.com; dmarc=pass'))
        self.assertTrue(ok, why)

    def test_aligned_dkim_is_enough(self):
        ok, why = EA._sender_is_authentic(
            'boss@example.test', self.ar('mx.amazonses.com; dkim=pass header.i=@example.test'))
        self.assertTrue(ok, why)

    def test_aligned_spf_is_enough(self):
        ok, why = EA._sender_is_authentic(
            'boss@example.test',
            self.ar('mx.amazonses.com; spf=pass smtp.mailfrom=bounce@example.test'))
        self.assertTrue(ok, why)

    def test_a_subdomain_still_aligns(self):
        ok, why = EA._sender_is_authentic(
            'boss@mail.example.test', self.ar('dkim=pass header.d=example.test'))
        self.assertTrue(ok, why)

    def test_someone_elses_dkim_does_not_vouch_for_this_sender(self):
        """The forgery that matters: valid DKIM for the attacker's own domain,
        From: rewritten to a trusted address."""
        ok, why = EA._sender_is_authentic(
            'boss@example.test', self.ar('dkim=pass header.i=@z-attacker.test; spf=pass '
                                         'smtp.mailfrom=z-attacker.test'))
        self.assertFalse(ok)
        self.assertIn('nothing authenticates', why)

    def test_a_failed_check_is_not_a_pass(self):
        self.assertFalse(EA._sender_is_authentic(
            'boss@example.test', self.ar('dkim=fail header.i=@example.test; spf=softfail'))[0])

    def test_no_header_at_all_is_refused(self):
        ok, why = EA._sender_is_authentic('boss@example.test', {})
        self.assertFalse(ok)
        self.assertIn('no Authentication-Results', why)

    def test_a_lookalike_domain_does_not_align(self):
        self.assertFalse(EA._sender_is_authentic(
            'boss@example.test', self.ar('dkim=pass header.i=@notexample.test'))[0])


class AuthenticationEnforcementTests(unittest.TestCase):
    """The gate, exercised through the worker."""

    def setUp(self):
        self.s3 = use_s3(self, cfg_object({
            'senders': [{'email': 'boss@example.test', 'tier': 'admin'}],
            'notify': ['boss@example.test'],
        }))
        for name, value in (('RESEND_API_KEY', 'z-fake'), ('EMAIL_AGENT_FROM', 'x@z-fake.test')):
            self.addCleanup(setattr, EA, name, getattr(EA, name))
            setattr(EA, name, value)
        self.sent = []
        self.addCleanup(setattr, EA.requests, 'post', EA.requests.post)
        EA.requests.post = lambda url, headers=None, json=None, timeout=None: (
            self.sent.append(json) or FakeResponse(200, b'{}', {'id': 'x'}))
        self.addCleanup(EA._HOOKS.update, dict(EA._HOOKS))
        EA._HOOKS['agent_client'] = lambda: object()
        EA._HOOKS['model'] = lambda: 'z-fake-model'
        EA._HOOKS['guidance_core'] = ''
        EA._HOOKS['tools'] = [{'name': 'query_inventory'}]
        EA._HOOKS['admin_tools'] = {'past_orders_lookup'}
        self.ran = []
        EA._HOOKS['agent_run'] = lambda *a, **k: (
            self.ran.append(1) or {'final_text': '<p>ok</p>', 'tools_used': [],
                                   'artifacts': [], 'usage': {}, 'elapsed_seconds': 1})

    def event(self, headers):
        return {'type': 'email.received',
                'data': {'email_id': f'em_{len(self.ran)}_{id(headers)}',
                         'from': 'Boss <boss@example.test>', 'subject': 'deck',
                         'text': 'nautica deck', 'headers': headers}}

    def test_a_spoofed_from_never_reaches_the_agent(self):
        EA._handle(self.event({'Authentication-Results': 'dkim=pass header.i=@z-attacker.test'}))
        self.assertEqual([], self.ran, 'the agent must not run for an unverified sender')
        recipients = [a for m in self.sent for a in m['to']]
        self.assertEqual(['boss@example.test'], recipients, 'only the heads-up, to David')
        self.assertIn('could not be verified', self.sent[0]['html'])

    def test_a_verified_sender_is_answered(self):
        EA._handle(self.event({'Authentication-Results': 'spf=pass smtp.mailfrom=example.test; '
                                                         'dkim=pass header.i=@example.test'}))
        self.assertEqual([1], self.ran)
        self.assertIn('ok', self.sent[0]['html'])

    def test_the_check_can_be_turned_off_deliberately(self):
        use_s3(self, cfg_object({'senders': [{'email': 'boss@example.test', 'tier': 'admin'}],
                                 'require_authentication': False}))
        EA._handle(self.event({}))
        self.assertEqual([1], self.ran)


class StorageProtectionTests(unittest.TestCase):
    """The allow-list was readable at a guessable public URL. It must not be."""

    def test_the_config_key_is_not_guessable(self):
        use_s3(self, {})
        key = EA._config_key()
        self.assertNotEqual('email-agent/config.json', key)
        self.assertTrue(key.startswith('email-agent/'))
        self.assertNotIn('config.json', key.split('/')[1])

    def test_the_key_depends_on_the_secret(self):
        EA._HOOKS['machine_key'] = 'z-fake-key-one'
        first = EA._config_key()
        EA._HOOKS['machine_key'] = 'z-fake-key-two'
        self.addCleanup(EA._HOOKS.update, {'machine_key': 'z-fake-machine-key-for-tests'})
        self.assertNotEqual(first, EA._config_key())

    def test_stored_bytes_do_not_contain_the_allow_list(self):
        s3 = use_s3(self, {})
        EA._save_config({'senders': [{'email': 'boss@example.test', 'tier': 'admin'}]})
        blob = s3.objects[EA._config_key()]
        self.assertNotIn(b'boss@example.test', blob, 'the allow-list is readable at rest')
        self.assertEqual([{'email': 'boss@example.test', 'tier': 'admin', 'name': ''}],
                         EA._load_config(force=True)['senders'], 'and still round-trips')

    def test_run_logs_are_sealed_too(self):
        s3 = use_s3(self, {})
        EA._record({'email_id': 'em_x', 'from': 'boss@example.test', 'subject': 'secret plans'})
        key = next(k for k in s3.objects if '/log/' in k)
        self.assertNotIn(b'boss@example.test', s3.objects[key])
        self.assertNotIn(b'secret plans', s3.objects[key])

    def test_the_old_public_copy_is_migrated_and_deleted(self):
        s3 = use_s3(self, {EA._LEGACY_CONFIG_KEY: json.dumps(
            {'senders': [{'email': 'boss@example.test', 'tier': 'admin'}]}).encode()})
        cfg = EA._load_config(force=True)
        self.assertEqual('boss@example.test', cfg['senders'][0]['email'], 'nothing lost')
        self.assertNotIn(EA._LEGACY_CONFIG_KEY, s3.objects, 'the public copy is gone')
        self.assertIn(EA._config_key(), s3.objects, 'and lives behind the private key')

    def test_the_first_days_public_objects_are_swept(self):
        """Run logs written before the fix named the sender and the subject."""
        s3 = use_s3(self, {
            'email-agent/log/2026-09-18/em_old.json': b'{"from": "boss@example.test"}',
            'email-agent/seen/em_old.json': b'{}',
            'inventory/keep-me.json': b'{}',
        })
        EA._swept.clear()
        self.addCleanup(EA._swept.clear)
        EA._sweep_legacy_objects()
        self.assertNotIn('email-agent/log/2026-09-18/em_old.json', s3.objects)
        self.assertNotIn('email-agent/seen/em_old.json', s3.objects)
        self.assertIn('inventory/keep-me.json', s3.objects, 'nothing else is touched')

    def test_the_sweep_leaves_the_private_objects_alone(self):
        s3 = use_s3(self, {})
        EA._claim('em_private')
        EA._record({'email_id': 'em_private', 'from': 'boss@example.test'})
        before = set(s3.objects)
        EA._swept.clear()
        self.addCleanup(EA._swept.clear)
        EA._sweep_legacy_objects()
        self.assertEqual(before, set(s3.objects))

    def test_the_sweep_runs_once(self):
        s3 = use_s3(self, {'email-agent/seen/em_old.json': b'{}'})
        EA._swept.clear()
        self.addCleanup(EA._swept.clear)
        EA._sweep_legacy_objects()
        s3.objects['email-agent/seen/em_later.json'] = b'{}'
        EA._sweep_legacy_objects()
        self.assertIn('email-agent/seen/em_later.json', s3.objects, 'second call is a no-op')

    def test_a_refused_list_does_not_raise(self):
        s3 = use_s3(self, {})

        def boom(**kw):
            raise Exception('AccessDenied')

        s3.list_objects_v2 = boom
        EA._swept.clear()
        self.addCleanup(EA._swept.clear)
        EA._sweep_legacy_objects()      # must not raise

    def test_dedupe_markers_are_private_too(self):
        s3 = use_s3(self, {})
        EA._claim('em_private')
        self.assertTrue(any(k.startswith(EA._private_prefix()) for k in s3.objects))
        self.assertNotIn('email-agent/seen/em_private.json', s3.objects)


if __name__ == '__main__':
    unittest.main(verbosity=2)
