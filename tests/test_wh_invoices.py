"""
Synthetic tests for the warehouse invoice mailbox (wh_invoices.py) and its wiring.

Covers the routing that keeps invoice mail away from the inventory assistant,
the two exact readers (the WMS invoice workbook and the monthly storage PDF),
every check, the sealed history, and a dry run end to end.

SYNTHETIC ONLY. Made-up vendors' numbers: rates, containers (valid check
digits, invented boxes), bank numbers, invoice numbers and addresses are all
invented. No network: S3 and Resend are stubbed and the AI reader is a fake
that returns fixed JSON. Run from the repo root:
    python -B -m unittest tests.test_wh_invoices -v
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
from datetime import datetime

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

import email_agent as EA  # noqa: E402
import wh_invoices as W  # noqa: E402


# ── Stubs ────────────────────────────────────────────────────────────────────
class FakeS3:
    def __init__(self):
        self.objects, self.etags, self.puts = {}, {}, []

    def get_object(self, Bucket=None, Key=None):
        if Key not in self.objects:
            raise Exception('NoSuchKey: ' + Key)
        return {'Body': io.BytesIO(self.objects[Key]), 'ETag': self.etags[Key]}

    def put_object(self, Bucket=None, Key=None, Body=None, ContentType=None, IfMatch=None, IfNoneMatch=None):
        if IfNoneMatch == '*' and Key in self.objects:
            raise Exception('An error occurred (PreconditionFailed) 412')
        if IfMatch and self.etags.get(Key) != IfMatch:
            raise Exception('An error occurred (PreconditionFailed) 412')
        self.objects[Key] = Body if isinstance(Body, bytes) else str(Body).encode()
        self.etags[Key] = '"%d"' % len(self.puts)
        self.puts.append(Key)
        return {}

    def head_object(self, Bucket=None, Key=None):
        if Key not in self.objects:
            raise Exception('404 Not Found')
        return {}

    def delete_object(self, Bucket=None, Key=None):
        self.objects.pop(Key, None)

    def list_objects_v2(self, Bucket=None, Prefix='', MaxKeys=1000, **kw):
        return {'Contents': [{'Key': k} for k in self.objects if k.startswith(Prefix)]}


def box(prefix, serial):
    """A container number with a valid ISO 6346 check digit."""
    for d in range(10):
        b = f'{prefix}{serial:06d}{d}'
        if W._iso6346_ok(b):
            return b
    raise AssertionError('no check digit')


BOX_A = box('ZZZU', 100001)
BOX_B = box('ZZZU', 100002)
BOX_C = box('ZZZU', 100003)


def setup_hooks(items=None):
    s3 = FakeS3()
    W._HOOKS.clear()
    W._HOOKS.update({'get_s3': lambda: s3, 's3_bucket': 'z-bucket', 'agent_client': lambda: None,
                     'machine_key': 'z-fake-machine-key-for-tests', 'caller_identity': None,
                     'inventory_items': lambda: list(items or []), 'base_style': lambda s: str(s).split('-')[0]})
    with W._cfg_lock:
        W._cfg_cache.update(at=0.0, data=None)
    return s3


def cfg():
    return W._load_config(force=True)


def ctx(ledger=None, items=None, email_id='z-email-1'):
    setup_hooks(items) if items is not None else None
    led = ledger or W._empty_ledger()
    return W._Ctx(cfg(), led, {'email_id': email_id})


def doc(**kw):
    d = W._new_doc('ai', 'z.pdf')
    d.update(kw)
    return d


def line(charge, qty, rate, amount, **kw):
    ln = {'section': '', 'charge': charge, 'desc': '', 'ref': '', 'date': '', 'qty': qty, 'unit': '',
          'rate': rate, 'amount': amount, 'free_qty': None, 'block': None}
    ln.update(kw)
    return ln


def run_checks(docs, c):
    return W._check_all(docs, c)


def codes(result, level=None):
    return [f['code'] for f in result['findings'] if level is None or f['level'] == level]


# ── Routing ──────────────────────────────────────────────────────────────────
class RoutingTests(unittest.TestCase):
    def setUp(self):
        setup_hooks()

    def test_mailbox_address_is_wanted(self):
        self.assertTrue(W.wants({'to': ['WHConfirmation@z-mail.test']}))
        self.assertTrue(W.wants({'to': ['someone@z-co.test'], 'received_for': ['whconfirmation@z-mail.test']}))
        self.assertTrue(W.wants({'to': [], 'cc': ['Invoices <whconfirmation@z-mail.test>']}))

    def test_other_addresses_are_not(self):
        self.assertFalse(W.wants({'to': ['ats@z-mail.test']}))
        self.assertFalse(W.wants({'to': ['whconfirmation-not@z-mail.test']}))
        self.assertFalse(W.wants({}))
        self.assertFalse(W.wants(None))

    def _app(self, accept):
        from flask import Flask
        app = Flask('z')
        # The email agent's module state is shared with its own tests: put it all back.
        hooks, boxes = dict(EA._HOOKS), list(EA._MAILBOXES)
        with EA._cfg_lock:
            cache = dict(EA._cfg_cache)

        def restore():
            EA._HOOKS.clear()
            EA._HOOKS.update(hooks)
            EA._MAILBOXES[:] = boxes
            with EA._cfg_lock:
                EA._cfg_cache.clear()
                EA._cfg_cache.update(cache)
        self.addCleanup(restore)
        EA._MAILBOXES[:] = []
        EA.add_mailbox('z-invoices', W.wants, accept)
        old_secret, old_sweeper = EA.RESEND_WEBHOOK_SECRET, EA._ensure_sweeper
        EA.RESEND_WEBHOOK_SECRET = 'whsec_' + base64.b64encode(b'z-secret-bytes-for-tests').decode()
        EA._ensure_sweeper = lambda: None
        self.addCleanup(lambda: setattr(EA, 'RESEND_WEBHOOK_SECRET', old_secret))
        self.addCleanup(lambda: setattr(EA, '_ensure_sweeper', old_sweeper))
        ats_s3 = FakeS3()
        EA.register_email_routes(app, get_s3=lambda: ats_s3, s3_bucket='z', agent_client=lambda: None,
                                 agent_run=lambda *a, **k: {}, tools=[], admin_tools=set(), guidance_core='',
                                 model=lambda: 'z', machine_key='z-machine')
        return app, ats_s3

    def _post(self, app, event):
        raw = json.dumps(event).encode()
        msg_id, ts = 'msg_z1', str(int(time.time()))
        key = base64.b64decode(EA.RESEND_WEBHOOK_SECRET.split('_', 1)[1])
        sig = base64.b64encode(hmac.new(key, f'{msg_id}.{ts}.'.encode() + raw, hashlib.sha256).digest()).decode()
        return app.test_client().post('/inbound-email', data=raw, headers={
            'svix-id': msg_id, 'svix-timestamp': ts, 'svix-signature': f'v1,{sig}',
            'Content-Type': 'application/json'})

    def test_webhook_hands_invoice_mail_to_the_mailbox_not_the_assistant(self):
        got = []
        app, ats_s3 = self._app(lambda ev: got.append(ev) or 'queued')
        r = self._post(app, {'type': 'email.received', 'data': {'email_id': 'z-1', 'to': ['whconfirmation@z.test'],
                                                                'from': 'a@z.test', 'subject': 'bill'}})
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.get_json().get('mailbox'), 'z-invoices')
        self.assertEqual(len(got), 1)
        self.assertFalse(any('seen' in k for k in ats_s3.objects), 'the assistant must not claim it')

    def test_assistant_mail_is_untouched(self):
        got = []
        app, ats_s3 = self._app(lambda ev: got.append(ev) or 'queued')
        r = self._post(app, {'type': 'email.received', 'data': {'email_id': 'z-2', 'to': ['ats@z.test'],
                                                                'from': 'a@z.test', 'subject': 'stock?'}})
        self.assertEqual(r.status_code, 200)
        self.assertEqual(got, [])
        self.assertNotEqual(r.get_json().get('mailbox'), 'z-invoices')

    def test_a_failing_mailbox_asks_for_a_retry_instead_of_falling_through(self):
        def boom(ev):
            raise RuntimeError('z')
        app, ats_s3 = self._app(boom)
        r = self._post(app, {'type': 'email.received', 'data': {'email_id': 'z-3', 'to': ['whconfirmation@z.test']}})
        self.assertEqual(r.status_code, 500)
        self.assertFalse(any('seen' in k for k in ats_s3.objects))

    def test_unsigned_post_never_reaches_the_mailbox(self):
        got = []
        app, _ = self._app(lambda ev: got.append(ev) or 'queued')
        r = app.test_client().post('/inbound-email', json={'type': 'email.received',
                                                           'data': {'email_id': 'z', 'to': ['whconfirmation@z.test']}})
        self.assertEqual(r.status_code, 401)
        self.assertEqual(got, [])

    def test_billing_systems_are_read_but_bounces_and_our_own_mail_are_not(self):
        bill = {'from': 'Zed Billing <no-reply@z-billing.test>', 'subject': 'Invoice ZT-1'}
        self.assertIsNone(W._skip_reason(bill, {'Auto-Submitted': 'auto-generated', 'Precedence': 'bulk'}))
        self.assertIsNotNone(W._skip_reason(bill, {'X-Versa-Agent': '1'}))
        self.assertIsNotNone(W._skip_reason(bill, {'Auto-Submitted': 'auto-replied'}))
        self.assertIsNotNone(W._skip_reason({'from': 'mailer-daemon@z.test', 'subject': 'x'}, {}))
        self.assertIsNotNone(W._skip_reason({'from': 'a@z.test', 'subject': 'Out of Office: back Monday'}, {}))

    def test_trust_for_a_bill_auto_forwarded_by_our_own_gmail(self):
        c = cfg()
        ses = ('amazonses.com; spf=pass (spfcheck: domain of z-co.test designates 192.0.2.1 as permitted sender) '
               'client-ip=192.0.2.1; envelope-from=owner+caf_=whconfirmation=z-mail.test@z-co.test; '
               'dkim=fail header.i=@z-vendor.test; dmarc=fail header.from=z-vendor.test')
        c = dict(c, internal_domains=['z-co.test'])
        hdr = {'Authentication-Results': ses, 'X-Forwarded-For': 'owner@z-co.test whconfirmation@z-mail.test'}
        self.assertEqual(W._trust('ar@z-vendor.test', hdr, c)[0], 'forwarded')
        no_fwd = {'Authentication-Results': ses}
        self.assertEqual(W._trust('ar@z-vendor.test', no_fwd, c)[0], 'unverified',
                         'a relay that does not say it forwarded for us earns nothing')
        stranger = {'Authentication-Results': ses.replace('z-co.test', 'z-other.test'),
                    'X-Forwarded-For': 'owner@z-co.test whconfirmation@z-mail.test'}
        self.assertEqual(W._trust('ar@z-vendor.test', stranger, c)[0], 'unverified',
                         'the forwarding envelope must authenticate as our own domain')
        signed = {'Authentication-Results': 'amazonses.com; dmarc=pass header.from=z-vendor.test'}
        self.assertEqual(W._trust('ar@z-vendor.test', signed, c)[0], 'verified')

    def test_report_goes_to_the_inventory_mailbox_admins_when_notify_is_empty(self):
        old = EA._load_config
        self.addCleanup(lambda: setattr(EA, '_load_config', old))
        EA._load_config = lambda force=False: {'notify': ['z-boss@z-co.test']}
        self.assertEqual(W._report_recipients_default(), ['z-boss@z-co.test'])
        EA._load_config = lambda force=False: {'notify': [], 'senders': [
            {'email': 'z-boss@z-co.test', 'tier': 'admin'}, {'email': 'z-staff@z-co.test', 'tier': 'staff'}]}
        self.assertEqual(W._report_recipients_default(), ['z-boss@z-co.test'])

    def test_gmail_confirmation_goes_to_the_teammate_who_asked(self):
        c = dict(cfg(), internal_domains=['z-co.test'])
        text = ('z-jane@z-co.test has requested to automatically forward mail to your email '
                'address whconfirmation@z-mail.test.')
        self.assertEqual(W._forwarding_requester(text, c), 'z-jane@z-co.test')
        outsider = text.replace('z-jane@z-co.test', 'z-jane@z-elsewhere.test')
        self.assertEqual(W._forwarding_requester(outsider, c), '', 'only our own people get the link')
        self.assertEqual(W._forwarding_requester('nothing here', c), '')

    def test_claim_is_once(self):
        self.assertTrue(W._claim('z-claim-1'))
        self.assertFalse(W._claim('z-claim-1'))
        W._seen_ids.clear()
        self.assertFalse(W._claim('z-claim-1'), 'the S3 marker survives a restart')


# ── Reader 1: the WMS invoice workbook ───────────────────────────────────────
def wms_workbook(number_cell=True):
    import openpyxl
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = 'Page 1'
    ws['B1'] = 'Printed 9/1/2026 10:00:00 AM MST'
    ws['D5'] = 'Remit Payment:'
    ws['D6'] = ' \nZed Warehouse LLC\n \n1 Test Way\nTesttown, CA 90000'
    ws['Q7'] = 'Invoice #'
    if number_cell:
        ws['Q9'] = 777001
    ws['C10'] = 'Bill To:'
    ws['Q11'] = 'Invoice Date:'
    ws['C12'] = ' \nVersa Group LLC\n \n320 W 37th St\nNew York, NY 10018'
    ws['Q13'] = datetime(2026, 8, 31)
    ws['Q14'] = 'Total Due :'
    ws['Q16'] = 'Terms:'
    ws['Q17'] = 'NET 30'
    rows = []
    # order 900001: labels + carton picks + piece picks (1.10 a piece, made up)
    rows.append(['Order', datetime(2026, 8, 25, 8, 0), 555001, 10, 'Labeling Fee', 'Label', 0.2, 2.0])
    rows.append([None, None, None, 5, 'Order Pick/Out Fee- Carton', 'Carton', 0.4, 2.0])
    rows.append([None, None, None, 12, 'Order Pick/Out Fee- Piece/Inner', 'Each', 1.1, 13.2])
    rows.append(('TOTAL', 'Order', '900001', 17.2))
    # order 900002: a transfer (BOL + pallets, no labels)
    rows.append(['Order', datetime(2026, 8, 26, 8, 0), 555002, 1, 'Bill of Lading Fee', 'Order', 7.0, 7.0])
    rows.append([None, None, None, 2, 'Pallet Out Fee', 'Pallet', 5.0, 10.0])
    rows.append(('TOTAL', 'Order', '900002', 17.0))
    # inbound containers: A and B normal, C with far too many pallets
    for b, pallets, cartons, when in ((BOX_A, 40, 1200, 27), (BOX_B, 38, 1140, 28), (BOX_C, 200, 1000, 29)):
        rows.append(['Receiving', datetime(2026, 8, when, 9, 0), 556000 + when, pallets, 'Pallet In Fee', 'Pallet', 5.0,
                     pallets * 5.0])
        rows.append([None, None, None, 1, "40' Container Devan", 'Container', 500.0, 500.0])
        rows.append([None, None, None, cartons, 'Sort & Seg (1-5 SKUs)', 'Carton', 0.1, round(cartons * 0.1, 2)])
        rows.append(('TOTAL', 'Inbound', b, round(pallets * 5.0 + 500.0 + cartons * 0.1, 2)))
    detail_total = round(sum(r[3] for r in rows if r[0] == 'TOTAL'), 2)
    ws['Q15'] = detail_total
    # Summary
    ws['C19'] = 'Summary'
    ws['C20'] = 'Order Charges'
    ws['R20'] = 34.2
    ws['C21'] = 'Labeling Fee : Label'
    ws['K21'], ws['O21'], ws['R21'] = 10, '$0.20', 2.0
    ws['C22'] = 'Order Pick/Out Fee- Carton : Carton'
    ws['K22'], ws['O22'], ws['R22'] = 5, '$0.40', 2.0
    ws['C23'] = 'Order Pick/Out Fee- Piece/Inner : Each'
    ws['K23'], ws['O23'], ws['R23'] = 12, '$1.10', 13.2
    ws['C24'] = 'Bill of Lading Fee : Order'
    ws['K24'], ws['O24'], ws['R24'] = 1, '$7.00', 7.0
    ws['C25'] = 'Pallet Out Fee : Pallet'
    ws['K25'], ws['O25'], ws['R25'] = 2, '$5.00', 10.0
    ws['C26'] = 'Inbound Charges'
    ws['R26'] = round(detail_total - 34.2, 2)
    ws['C27'] = 'Pallet In Fee : Pallet'
    ws['K27'], ws['O27'], ws['R27'] = 278, '$5.00', 1390.0
    ws['C28'] = "40' Container Devan : Container"
    ws['K28'], ws['O28'], ws['R28'] = 3, '$500.00', 1500.0
    ws['C29'] = 'Sort & Seg (1-5 SKUs) : Carton'
    ws['K29'], ws['O29'], ws['R29'] = 3340, '$0.10', 334.0
    ws['O30'], ws['R30'] = 'Total Charge(s):', detail_total
    ws['C31'] = 'Detail'
    ws['C32'], ws['E32'], ws['F32'], ws['G32'], ws['I32'] = 'Activity Type', 'Date', 'Transaction #', 'Fee Type', 'Quantity'
    ws['N32'], ws['P32'], ws['T32'], ws['U32'], ws['V32'] = 'SKU', 'Description', 'UOM ', 'Rate', 'Total'
    r = 33
    for row in rows:
        if row[0] == 'TOTAL':
            ws.cell(r, 10, f'Total Charge(s) for {row[1]} #  {row[2]} :')
            ws.cell(r, 22, row[3])
        else:
            act, when, txn, qty, fee, uom, rate, tot = row
            if act:
                ws.cell(r, 3, act)
                ws.cell(r, 5, when)
                ws.cell(r, 6, txn)
            ws.cell(r, 9, qty)
            ws.cell(r, 10, fee)
            ws.cell(r, 20, uom)
            ws.cell(r, 21, rate)
            ws.cell(r, 22, tot)
        r += 1
    return wb, detail_total


class WmsReaderTests(unittest.TestCase):
    def setUp(self):
        setup_hooks()

    def test_reads_every_block_and_adds_up(self):
        wb, total = wms_workbook()
        self.assertTrue(W._looks_wms_workbook(wb))
        d = W._parse_wms_workbook(wb, 'z-bill.xlsx')
        self.assertTrue(d['_selfcheck'])
        self.assertEqual(d['number'], '777001')
        self.assertEqual(d['date'], '2026-08-31')
        self.assertEqual(d['terms'], 'NET 30')
        self.assertEqual(d['vendor_name'], 'Zed Warehouse LLC')
        self.assertIn('Versa Group LLC', d['bill_to'])
        self.assertAlmostEqual(d['total'], total)
        kinds = [b['kind'] for b in d['blocks']]
        self.assertEqual(kinds.count('order'), 2)
        self.assertEqual(kinds.count('inbound'), 3)
        self.assertEqual(d['containers'], [BOX_A, BOX_B, BOX_C])
        self.assertEqual(len(d['summary']), 8)

    def test_blank_number_comes_from_the_file_name(self):
        wb, _ = wms_workbook(number_cell=False)
        d = W._parse_wms_workbook(wb, '777123 (1).xlsx')
        self.assertEqual(d['number'], '777123')
        self.assertTrue(any('file name' in n for n in d['notes']))

    def test_checks_flag_the_transfer_and_the_overpalletized_container(self):
        wb, _ = wms_workbook()
        d = W._parse_wms_workbook(wb, 'z.xlsx')
        old = W._book_lookup
        W._book_lookup = lambda orders: {'900001': {'customer': 'ZCUST', 'po': 'ZPO1', 'wh': ['TR'], 'open': 0,
                                                    'pick': 10, 'lines': 1, 'seen': '2026-08-25'},
                                         '_days_loaded': 3}
        self.addCleanup(lambda: setattr(W, '_book_lookup', old))
        res = run_checks([d], ctx())[0]
        found = {f['code']: f for f in res['findings']}
        self.assertIn('transfers', found)
        self.assertIn('900002', found['transfers']['title'])
        self.assertIn('pallets_high', found)
        self.assertIn(BOX_C, found['pallets_high']['title'])
        self.assertIn('pieces_over', found, '12 piece picks billed for 10 units')
        self.assertAlmostEqual(found['pieces_over']['amount'], 2.2)
        self.assertIn('summary', codes(res, 'ok'))
        self.assertIn('blocks', codes(res, 'ok'))
        self.assertNotIn('order_unmatched', found, 'a transfer is not also listed as unmatched')

    def test_summary_row_with_two_rates_is_not_an_error(self):
        wb, _ = wms_workbook()
        d = W._parse_wms_workbook(wb, 'z.xlsx')
        # one pallet-in line at a different rate; the summary prints one rate for the total
        i = next(i for i, ln in enumerate(d['lines']) if ln['charge'] == 'Pallet In Fee')
        d['lines'][i].update(rate=5.0)
        F = []
        W._chk_summary(d, F)
        self.assertEqual([f['level'] for f in F], ['ok'])

    def test_a_changed_line_is_caught(self):
        wb, _ = wms_workbook()
        d = W._parse_wms_workbook(wb, 'z.xlsx')
        d['lines'][0]['amount'] = 9.99
        F = []
        W._chk_line_math(d, F)
        W._chk_blocks(d, F)
        self.assertIn('line_math', [f['code'] for f in F if f['level'] == 'error'])
        self.assertIn('block_total', [f['code'] for f in F if f['level'] == 'error'])


# ── Reader 2: the monthly storage PDF ────────────────────────────────────────
def storage_pdf():
    pdf = W._pdf_open(b'') if False else None
    try:
        import pymupdf as fz
    except ImportError:
        import fitz as fz
    doc = fz.open()
    page = doc.new_page(width=612, height=792)
    y = [40]

    def put(x, text, size=9):
        page.insert_text((x, y[0]), text, fontsize=size)

    def nl(step=14):
        y[0] += step

    put(40, 'Bill To:'); nl()
    put(40, 'Original Brands LLC'); nl()
    put(40, '320 W 37th St'); nl()
    put(40, 'New York, NY 10018'); nl()
    put(40, 'INVOICE'); nl()
    put(40, 'Summary'); nl()
    put(40, 'Monthly WMS Subscription Fee : Monthly'); put(300, '1.00'); put(450, '$50.00'); put(520, '$50.00'); nl()
    put(40, 'As of 10/1/2026 : Pallet'); put(300, '6.00'); put(450, '$10.00'); put(520, '$60.00'); nl()
    put(40, 'Total Charge(s):'); put(520, '$110.00'); nl()
    put(40, 'Detail'); nl()
    put(40, 'SKU'); put(300, 'Quantity'); put(380, 'UOM'); put(450, 'Rate'); put(520, 'Total'); nl()
    put(40, 'As of 10/1/2026 : Pallet'); put(310, '6.00'); put(380, 'Pallet'); put(450, '$10.00'); put(520, '$60.00'); nl()
    put(40, 'ZZNASU001SLS-ALL'); put(310, '2.00'); put(380, 'Pallet'); put(450, '$10.00'); put(520, '$20.00'); nl()
    put(40, 'ZZNASU002RFS(ZZ-P36)36PK-'); put(310, '3.00'); put(380, 'Pallet'); put(450, '$10.00'); put(520, '$30.00'); nl(10)
    put(40, 'BIG'); nl()
    put(40, 'ZZNASU003SLS'); put(310, '1.00'); put(380, 'Pallet'); put(450, '$10.00'); put(520, '$10.00'); nl()
    put(40, 'Monthly WMS'); put(310, '1.00'); put(380, 'Monthly'); put(450, '$50.00'); put(520, '$50.00'); nl(10)
    put(40, 'Subscription Fee : Monthly'); nl()
    put(40, 'Total Charge(s) for Misc #  999 :'); put(520, '$110.00'); nl()
    put(40, 'Remit Payment:'); nl()
    put(40, 'ZED STORAGE'); nl()
    put(40, '1 Test Way'); nl()
    put(40, 'Company Bank Wire/ACH Details:'); nl()
    put(40, 'ZED STORAGE, Zed Bank, Aba: 000000123, Acct# 99990001'); nl()
    put(40, 'Invoice #'); nl()
    put(40, 'ZS-999'); nl()
    put(40, 'Invoice Date:'); nl()
    put(40, '10/01/2026'); nl()
    put(40, 'Total Due :'); nl()
    put(40, '$110.00'); nl()
    put(40, 'Terms:'); nl()
    put(40, 'NET 15'); nl()
    put(40, 'Monthly Storage Invoice:'); nl()
    return doc.tobytes()


class StorageReaderTests(unittest.TestCase):
    def setUp(self):
        setup_hooks()

    def test_reads_lines_wraps_header_and_bank(self):
        d = W._parse_storage_pdf(storage_pdf(), 'z-storage.pdf')
        self.assertIsNotNone(d)
        self.assertTrue(d['_selfcheck'], d['notes'])
        self.assertEqual(d['number'], 'ZS-999')
        self.assertEqual(d['date'], '2026-10-01')
        self.assertEqual(d['terms'], 'NET 15')
        self.assertEqual(d['amount_due'], 110.0)
        self.assertEqual(d['total'], 110.0)
        skus = [ln['ref'] for ln in d['lines'] if ln['charge'] == 'Storage']
        self.assertEqual(skus, ['ZZNASU001SLS-ALL', 'ZZNASU002RFS(ZZ-P36)36PK-BIG', 'ZZNASU003SLS'])
        other = [ln for ln in d['lines'] if ln['charge'] != 'Storage']
        self.assertEqual(other[0]['charge'], 'Monthly WMS Subscription Fee')
        self.assertEqual(d['banks'][0]['routing'], '000000123')
        self.assertEqual(d['banks'][0]['account'], '99990001')
        F = []
        W._chk_summary(d, F)
        W._chk_blocks(d, F)
        W._chk_totals(d, F)
        self.assertEqual({f['level'] for f in F}, {'ok'}, F)

    def test_storage_against_stock_and_last_month(self):
        items = [{'sku': 'ZZNASU001SLS', 'dcw': 0, 'tr': 50, 'jtw': 0, 'qa': 0},
                 {'sku': 'ZZNASU002RFS', 'dcw': 720, 'tr': 0, 'jtw': 0, 'qa': 0}]
        setup_hooks(items)
        d = W._parse_storage_pdf(storage_pdf(), 'z.pdf')
        led = W._empty_ledger()
        led['vendors']['dcw'] = {'storage_last': {'number': 'ZS-998', 'date': '2026-09-01', 'pallets': 3,
                                                  'skus': {'ZZNASU001SLS-ALL': 3}}}
        c = W._Ctx(cfg(), led, {'email_id': 'z'})
        d['vendor_key'], d['vendor_display'] = 'dcw', 'DCW'
        F = []
        W._chk_storage(d, F, c)
        by = {f['code']: f for f in F}
        self.assertIn('storage_zero', by)
        self.assertIn('ZZNASU001SLS-ALL', by['storage_zero']['detail'])
        self.assertAlmostEqual(by['storage_zero']['amount'], 20.0)
        self.assertIn('storage_jump', by, '6 pallets vs 3 last month')
        self.assertIn('storage_unmatched', by, 'ZZNASU003SLS is not in the feed')
        self.assertIn('storage_stock', [f['code'] for f in F if f['level'] == 'ok'])


# ── The checks ───────────────────────────────────────────────────────────────
class CheckTests(unittest.TestCase):
    def setUp(self):
        setup_hooks()

    def _inv(self, **kw):
        base = dict(type='invoice', vendor_name='Zed Trucking LLC', number='ZT-1', date='2026-10-01',
                    bill_to='Original Brands LLC\n320 W 37th St\nNew York, NY', total=100.0, amount_due=100.0,
                    lines=[line('Drayage Base', 1, 100.0, 100.0, ref=BOX_A)], containers=[BOX_A])
        base.update(kw)
        return doc(**base)

    def test_clean_bill_is_confirmed(self):
        res = run_checks([self._inv()], ctx())[0]
        self.assertEqual(res['verdict'], 'confirmed', res['findings'])

    def test_wrong_company_is_an_error(self):
        res = run_checks([self._inv(bill_to='ZZ Transports')], ctx())[0]
        self.assertIn('bill_to', codes(res, 'error'))
        self.assertEqual(res['verdict'], 'problem')

    def test_word_versa_does_not_match_inside_other_words(self):
        res = run_checks([self._inv(bill_to='Universal Freight')], ctx())[0]
        self.assertIn('bill_to', codes(res, 'error'))

    def test_lines_not_adding_to_the_total(self):
        res = run_checks([self._inv(total=120.0, amount_due=120.0)], ctx())[0]
        self.assertIn('lines_vs_total', codes(res, 'error'))

    def test_bank_change_is_flagged_and_first_sighting_recorded(self):
        first = run_checks([self._inv(banks=[{'bank': 'Zed Bank', 'routing': '000000123', 'account': '11110000'}])],
                           ctx())[0]
        self.assertIn('bank_new', codes(first, 'info'))
        led = W._empty_ledger()
        key = W._slug('Zed Trucking LLC')
        led['vendors'][key] = {'banks': [{'sha': W._bank_sha(key, '11110000'), 'last4': '0000'}]}
        same = run_checks([self._inv(banks=[{'bank': '', 'routing': '', 'account': '11110000'}])], ctx(led))[0]
        self.assertIn('bank', codes(same, 'ok'))
        changed = run_checks([self._inv(banks=[{'bank': '', 'routing': '', 'account': '22220000'}])], ctx(led))[0]
        self.assertIn('bank_changed', codes(changed, 'error'))
        self.assertNotIn('22220000', json.dumps(changed['findings']), 'account numbers are masked')

    def test_duplicates(self):
        d = self._inv()
        run_checks([d], ctx())
        led = W._empty_ledger()
        led['invoices'].append({'vendor': d['vendor_key'], 'number': 'ZT-1', 'total': 100.0, 'hash': d['hash'],
                                'email_id': 'older', 'checked_at': '2026-10-02T00:00:00'})
        again = run_checks([self._inv()], ctx(led))[0]
        self.assertIn('seen_before', codes(again, 'info'))
        led['invoices'][0]['hash'] = 'different'
        dup = run_checks([self._inv()], ctx(led))[0]
        self.assertIn('duplicate_number', codes(dup, 'error'))

    def test_container_billed_twice_for_the_same_service(self):
        d = self._inv(number='ZT-2')
        led = W._empty_ledger()
        led['invoices'].append({'vendor': W._slug('Zed Trucking LLC'), 'vendor_name': 'Zed Trucking', 'number': 'ZT-1',
                                'date': '2026-09-01', 'email_id': 'older',
                                'services': [[BOX_A, W._charge_key('', 'Drayage Base', ''), 100.0]]})
        res = run_checks([d], ctx(led))[0]
        self.assertIn('service_twice', codes(res, 'error'))

    def test_rate_against_the_last_bill_and_the_agreed_rate(self):
        key = W._slug('Zed Trucking LLC')
        ck = W._charge_key('', 'Drayage Base', '')
        led = W._empty_ledger()
        led['vendors'][key] = {'rates': {ck: {'last': 90.0, 'last_invoice': 'ZT-0', 'last_date': '2026-09-01',
                                               'hist': {'90.0000': 1}}}}
        res = run_checks([self._inv()], ctx(led))[0]
        up = [f for f in res['findings'] if f['code'] == 'rate_up']
        self.assertEqual(len(up), 1)
        self.assertAlmostEqual(up[0]['amount'], 10.0)
        c = ctx(led)
        c.cfg = dict(c.cfg, contract_rates={key: {ck: 100.0}})
        res2 = run_checks([self._inv()], c)[0]
        self.assertNotIn('rate_up', codes(res2))

    def test_flat_charges_compared_across_one_email(self):
        a = self._inv(number='ZT-5', lines=[line('Transload', None, None, 600.0, ref=BOX_A)], total=600.0,
                      amount_due=600.0)
        b = self._inv(number='ZT-6', lines=[line('Transload', None, None, 700.0, ref=BOX_B)], total=700.0,
                      amount_due=700.0, containers=[BOX_B])
        res = run_checks([a, b], ctx())
        self.assertIn('rate_batch', codes(res[1], 'question'))
        self.assertNotIn('rate_batch', codes(res[0]))

    def test_chassis_days_and_dwell(self):
        ev = [{'event': 'port_pickup', 'date': '2026-10-01', 'container': BOX_A},
              {'event': 'delivery', 'date': '2026-10-01', 'container': BOX_A},
              {'event': 'empty_return', 'date': '2026-10-10', 'container': BOX_A}]
        lines = [line('Drayage Base', 1, 100.0, 100.0, ref=BOX_A), line('Chassis- Port', 12, 10.0, 120.0, ref=BOX_A)]
        d = self._inv(lines=lines, total=220.0, amount_due=220.0, events=ev)
        res = run_checks([d], ctx())[0]
        by = {f['code']: f for f in res['findings']}
        self.assertEqual(by['chassis_days']['level'], 'error', '12 billed, out 10 days')
        self.assertAlmostEqual(by['chassis_days']['amount'], 20.0)
        self.assertEqual(by['dwell']['level'], 'question')

    def test_chassis_dates_from_a_proof_page(self):
        lines = [line('Drayage Base', 1, 100.0, 100.0, ref=BOX_A), line('Chassis- Port', 3, 10.0, 30.0, ref=BOX_A)]
        d = self._inv(lines=lines, total=130.0, amount_due=130.0,
                      events=[{'event': 'port_pickup', 'date': '2026-10-01', 'container': BOX_A}])
        proof = doc(type='proof', containers=[BOX_A],
                    events=[{'event': 'empty_return', 'date': '2026-10-03', 'container': BOX_A}])
        res = run_checks([d, proof], ctx())[0]
        self.assertIn('chassis_days', codes(res, 'ok'))

    def test_extra_charge_needs_proof_and_a_note_joins_it(self):
        lines = [line('Drayage Base', 1, 100.0, 100.0), line('Dry Run- Import', 1, 50.0, 50.0)]
        d = self._inv(lines=lines, total=150.0, amount_due=150.0,
                      observations=[{'item': 'Dry run charged', 'why': 'Terminal said a pool chassis was needed.',
                                     'amount': 50.0}])
        res = run_checks([d], ctx())[0]
        acc = [f for f in res['findings'] if f['code'] == 'accessorial']
        self.assertEqual(len(acc), 1)
        self.assertIn('pool chassis', acc[0]['detail'])
        self.assertNotIn('observation', codes(res))
        self.assertAlmostEqual(res['at_stake'], 50.0)

    def test_statement(self):
        st = doc(type='statement', vendor_name='Zed Storage LLC', date='2026-10-26', amount_due=300.0, total=300.0,
                 bill_to='VERSA GROUP, LLC', statement={
                     'date': '2026-10-26',
                     'lines': [{'date': '2026-06-30', 'number': 'ZS-1', 'due': '2026-07-27', 'amount': 100.0, 'balance': 100.0},
                               {'date': '2026-12-01', 'number': 'ZS-2', 'due': '2026-10-20', 'amount': 200.0, 'balance': 300.0}],
                     'aging': {'current': 0, 'days_1_30': 200.0, 'days_31_60': 0, 'days_61_90': 0, 'over_90': 100.0,
                               'total': 300.0}})
        led = W._empty_ledger()
        led['started'] = '2026-11-01'
        res = run_checks([st], ctx(led))[0]
        self.assertIn('stmt_balance', codes(res, 'ok'))
        self.assertIn('stmt_aging', codes(res, 'ok'))
        self.assertIn('stmt_before', codes(res, 'info'), 'ZS-1 predates the mailbox')
        self.assertIn('stmt_missing', codes(res, 'question'), 'ZS-2 is newer and was never received')
        st['statement']['lines'][1]['balance'] = 999.0
        self.assertIn('stmt_balance', codes(run_checks([st], ctx(led))[0], 'error'))

    def test_container_check_digit(self):
        self.assertTrue(W._iso6346_ok(BOX_A))
        bad = BOX_A[:-1] + str((int(BOX_A[-1]) + 1) % 10)
        self.assertFalse(W._iso6346_ok(bad))
        self.assertEqual(W._containers_in(f'Container: {BOX_A} and {BOX_B}'), [BOX_A, BOX_B])

    def test_dates(self):
        for s in ('2026-11-01', '11/01/2026', '11-1-26', '01-NOV-26', 'Nov 1, 2026', datetime(2026, 11, 1)):
            self.assertEqual(W._iso(s), '2026-11-01', s)
        self.assertEqual(W._iso('not a date'), '')


# ── Memory and storage ───────────────────────────────────────────────────────
class MemoryTests(unittest.TestCase):
    def test_history_is_sealed_and_private(self):
        s3 = setup_hooks()
        d = doc(type='invoice', vendor_name='Zed Trucking LLC', number='ZT-9', total=100.0,
                lines=[line('Drayage Base', 1, 100.0, 100.0, ref=BOX_A)], containers=[BOX_A],
                banks=[{'bank': 'Zed Bank', 'routing': '000000123', 'account': '55550000'}])
        res = run_checks([d], ctx())
        W._remember(res, {'email_id': 'z-mem', 'trust': 'internal'})
        key = W._ledger_key()
        self.assertTrue(key.startswith('wh-invoices/') and len(key.split('/')[1]) == 32)
        blob = s3.objects[key]
        self.assertNotIn(b'Zed Trucking', blob)
        self.assertNotIn(b'55550000', blob)
        led, _ = W._ledger_load()
        v = led['vendors'][W._slug('Zed Trucking LLC')]
        self.assertEqual(v['banks'][0]['last4'], '0000')
        self.assertNotIn('55550000', json.dumps(led), 'only a hash and the last four are kept')
        self.assertEqual(led['invoices'][-1]['number'], 'ZT-9')
        self.assertEqual(led['invoices'][-1]['services'][0][0], BOX_A)
        ck = W._charge_key('', 'Drayage Base', '')
        self.assertEqual(v['rates'][ck]['last'], 100.0)

    def test_unverified_senders_do_not_teach_rates_or_banks(self):
        setup_hooks()
        d = doc(type='invoice', vendor_name='Zed Trucking LLC', number='ZT-10', total=100.0,
                lines=[line('Drayage Base', 1, 100.0, 100.0)], banks=[{'bank': '', 'routing': '', 'account': '7777'}])
        W._remember(run_checks([d], ctx()), {'email_id': 'z-u', 'trust': 'unverified'}, learn=False)
        led, _ = W._ledger_load()
        v = led['vendors'][W._slug('Zed Trucking LLC')]
        self.assertEqual(v['rates'], {})
        self.assertEqual(v['banks'], [])
        self.assertEqual(led['invoices'][-1]['number'], 'ZT-10', 'still remembered for duplicate checks')

    def test_config_is_sealed(self):
        s3 = setup_hooks()
        c = cfg()
        c['report_to'] = ['z-owner@z-co.test']
        W._save_config(c)
        self.assertNotIn(b'z-owner', s3.objects[W._config_key()])
        self.assertEqual(cfg()['report_to'], ['z-owner@z-co.test'])

    def test_not_ready_without_a_machine_key(self):
        setup_hooks()
        W._HOOKS['machine_key'] = ''
        self.assertFalse(W._ready())
        self.assertEqual(W.accept({'data': {'email_id': 'z'}}), 'not ready')


# ── End to end, dry ──────────────────────────────────────────────────────────
class _Msg:
    def __init__(self, text):
        self.content = [type('B', (), {'type': 'text', 'text': text})()]
        self.stop_reason = 'end_turn'
        self.usage = type('U', (), {'input_tokens': 10, 'output_tokens': 5})()


class FakeAI:
    def __init__(self, answer):
        self.answer, self.calls = answer, []
        outer = self

        class _S:
            def __init__(self, kw):
                self.kw = kw

            def __enter__(self):
                return self

            def __exit__(self, *a):
                return False

            def get_final_message(self):
                outer.calls.append(self.kw)
                return _Msg(json.dumps(outer.answer))

        class _M:
            @staticmethod
            def stream(**kw):
                return _S(kw)

        self.messages = _M()
        self.beta = type('Beta', (), {'messages': _M()})()


def ai_doc(**kw):
    d = {'file_number': 1, 'pages': '1', 'doc_type': 'invoice', 'vendor_name': 'Zed Transfer LLC',
         'vendor_address': '', 'invoice_number': 'ZT-77', 'invoice_date': '2026-10-16', 'due_date': '2026-12-15',
         'terms': 'Net 60', 'bill_to': 'Original Brands LLC\n320 West 37th Street', 'customer_name': '',
         'remit_to': '', 'bank_details': [], 'assignment_notice': '', 'references': [], 'containers': [BOX_A],
         'line_items': [{'charge': 'Transload', 'description': '', 'reference': BOX_A, 'date': '', 'quantity': None,
                         'unit': '', 'rate': None, 'amount': 600.0, 'free_quantity': None}],
         'line_items_complete': True, 'subtotal': None, 'tax': None, 'total': 600.0, 'amount_due': 600.0,
         'statement_date': '', 'statement_lines': [],
         'aging': {k: None for k in ('current', 'days_1_30', 'days_31_60', 'days_61_90', 'over_90', 'total')},
         'events': [], 'observations': [], 'reading_notes': ''}
    d.update(kw)
    return d


class DryRunTests(unittest.TestCase):
    def test_a_workbook_and_a_scan_in_one_email(self):
        s3 = setup_hooks()
        fake = FakeAI({'documents': [ai_doc()]})
        W._HOOKS['agent_client'] = lambda: fake
        sent = []
        old_send = W._send
        W._send = lambda *a, **k: sent.append(a) or (True, 'z')
        self.addCleanup(lambda: setattr(W, '_send', old_send))
        old_book = W._book_lookup
        W._book_lookup = lambda orders: {'_days_loaded': 0}
        self.addCleanup(lambda: setattr(W, '_book_lookup', old_book))
        wb, _ = wms_workbook()
        buf = io.BytesIO()
        wb.save(buf)
        files = [{'name': 'z-bill.xlsx', 'ctype': '', 'bytes': buf.getvalue()},
                 {'name': 'z-scan.pdf', 'ctype': 'application/pdf', 'bytes': b'%PDF-1.4 z'}]
        out = W._handle({'data': {'from': 'z@z.test', 'subject': 'bills'}}, files=files, dry=True, remember=False,
                        test=True)
        self.assertNotIn('error', out, out.get('error'))
        self.assertEqual(len(out['documents']), 2)
        self.assertEqual(len(fake.calls), 1, 'only the scan goes to the AI reader')
        call = fake.calls[0]
        self.assertEqual(call['model'], W.WH_MODEL)
        self.assertEqual(call['extra_body']['output_config']['format']['type'], 'json_schema')
        sent_names = [c['text'] for c in call['messages'][0]['content'] if c['type'] == 'text']
        self.assertIn('File 1: z-scan.pdf', sent_names)
        self.assertNotIn('File 1: z-bill.xlsx', sent_names)
        self.assertTrue(out['subject'].startswith('[WH Check] TEST'))
        self.assertIn('<table', out['html'])
        self.assertNotIn('—', out['html'], 'no em dashes in the report')
        self.assertTrue(base64.b64decode(out['xlsx_b64']).startswith(b'PK'))
        self.assertEqual(sent, [], 'a dry run sends nothing')
        self.assertNotIn(W._ledger_key(), s3.objects, 'a test run remembers nothing')

    def test_schema_is_strict(self):
        def walk(node):
            if isinstance(node, dict):
                if node.get('type') == 'object':
                    self.assertIs(node.get('additionalProperties'), False)
                    self.assertEqual(sorted(node['required']), sorted(node['properties']))
                for v in node.values():
                    walk(v)
            elif isinstance(node, list):
                for v in node:
                    walk(v)
        walk(W._DOC_SCHEMA)


# ── Wiring in app.py ─────────────────────────────────────────────────────────
class WiringTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        with io.open(os.path.join(ROOT, 'app.py'), encoding='utf-8') as fh:
            cls.src = fh.read()
        ast.parse(cls.src)

    def test_routes_are_behind_inline_admin_auth(self):
        i = self.src.index('_AUTHZ_OPEN_PREFIXES = (')
        self.assertIn("'/wh-invoices'", self.src[i:self.src.index(')', self.src.index("'/catalog/'", i))])

    def test_registration_is_wrapped(self):
        i = self.src.index('import wh_invoices')
        self.assertIn('try:', self.src[i - 40:i])
        self.assertIn('wh_invoices.register_wh_routes(', self.src[i:i + 400])
        self.assertIn('[WHInvoices] NOT registered', self.src[i:i + 900])

    def test_no_rate_or_bank_number_in_the_module(self):
        with io.open(os.path.join(ROOT, 'wh_invoices.py'), encoding='utf-8') as fh:
            src = fh.read()
        import re
        self.assertEqual(re.findall(r'\b\d{9}\b', src), [], 'no routing-shaped numbers')
        self.assertNotRegex(src, r'\$\d+\.\d{2} per pallet')


if __name__ == '__main__':
    unittest.main()
