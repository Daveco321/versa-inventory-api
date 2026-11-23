"""amazon_agent.py: the daily run (plan, unconfirmed draft, email to David) and his replies
(YES, NO, changes). Everything is faked; synthetic styles and made-up addresses only."""
import base64
import io
import os
import sys
import unittest
from datetime import datetime, timedelta, timezone

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

import amazon_agent as A  # noqa: E402
import amazon_inbound as I  # noqa: E402
import amazon_planner as P  # noqa: E402

DAVID = 'owner@example.test'
STRANGER = 'someone@example.test'
TRANSCAL = 'tickets@warehouse.example.test'
SLIM = {'S': 4, 'M': 8, 'MT': 4, 'L': 4, 'LT': 8, 'XL': 8}
NOW = datetime(2026, 11, 23, 13, 0, tzinfo=timezone.utc)


def snapshot():
    listings, sold, fba = [], {}, {}
    for base, sales in (('AMZZSU901SLS', 40), ('AMZZSU902SLS', 30)):
        for s in SLIM:
            sku = f'{base}-{s}-FBA'
            listings.append([sku, 'B0ZZ' + s, 'Active', 'AMAZON_NA', 'Test shirt'])
            sold[sku] = sales
            fba[sku] = [5, 0, 0, 0]
    return {'listings': listings, 'sold': sold, 'fba': fba, 'problems': [], 'counts': {}}


class FakeInbound:
    ready_window_start = staticmethod(I.ready_window_start)
    ticket_shipments = staticmethod(I.ticket_shipments)

    def __init__(self, fail_create=False):
        self.created, self.cancelled, self.confirmed = [], [], []
        self.fail_create = fail_create

    def create_draft(self, client, name, source, cartons, box_spec, ready):
        if self.fail_create:
            raise I.InboundError('placement options', 'Amazon refused')
        self.created.append([c.warehouse_sku for c in cartons])
        n = len(self.created)
        return {'inbound_plan_id': f'wf{n}', 'placement_option_id': f'pl{n}', 'placement_fee': 0.0,
                'shipping_cost': 42.5, 'warnings': [], 'shipments': [
                    {'shipment_id': f'sh{n}', 'fc': 'SWF2', 'address': {'line1': '557 Tuckahoe Road', 'line2': '',
                                                                       'city': 'Yonkers', 'state': 'NY', 'zip': '10710'},
                     'address_source': 'label-verified', 'cartons': [[c.warehouse_sku, c.contents] for c in cartons],
                     'transport_option_id': f'to{n}', 'carrier': 'UPS', 'shipping_cost': 42.5}]}

    def confirm_draft(self, client, draft):
        self.confirmed.append(draft['inbound_plan_id'])
        return {'shipments': [{'shipment_id': draft['shipments'][0]['shipment_id'], 'confirmation_id': 'FBATESTZZZZ',
                               'fc': 'SWF2', 'address': draft['shipments'][0]['address'], 'address_source': 'label-verified',
                               'boxes': len(draft['shipments'][0]['cartons']), 'labels_pdf': b'%PDF fake'}], 'warnings': []}

    def cancel_draft(self, client, plan_id):
        self.cancelled.append(plan_id)


class Harness:
    def __init__(self, connected=True, configured=True, fail_create=False):
        self.store = {}
        self.sent = []
        self.inbound = FakeInbound(fail_create)
        self.agent = A.Agent({
            'client': (lambda: object()) if connected else (lambda: None),
            'store_get': lambda n: self.store.get(n), 'store_put': self.store.__setitem__,
            'stock_rows': lambda: [P.StockRow('AMZZSU901SLS', 360, 0), P.StockRow('AMZZSU902SLS', 360, 0)],
            'prepack_resolver': lambda: (lambda base: SLIM),
            'meta_for': lambda base: {'brand': 'Testbrand'},
            'send_email': self._send, 'pull': lambda client: snapshot(), 'inbound': self.inbound})
        if configured:
            self.agent.update_settings({'david': DAVID, 'approvers': [DAVID.upper()], 'transcal_to': [TRANSCAL],
                                        'source_address': {'name': 'Test Warehouse'}})

    def _send(self, to, subject, html, attachments, cc=None):
        self.sent.append({'to': to, 'subject': subject, 'html': html, 'files': [a['filename'] for a in attachments],
                          'attachments': attachments, 'cc': cc})
        return True, 'ok'

    def ref(self):
        return self.store['state']['current']


class RunTests(unittest.TestCase):

    def test_waits_for_amazon_and_settings(self):
        h = Harness(connected=False)
        self.assertEqual('waiting', h.agent.run(NOW)['status'])
        h = Harness(configured=False)
        out = h.agent.run(NOW)
        self.assertEqual('not_configured', out['status'])
        self.assertIn('transcal_to', out['detail'])
        self.assertEqual([], h.sent)

    def test_a_run_emails_david_the_plan_with_both_files_and_ships_nothing(self):
        h = Harness()
        out = h.agent.run(NOW)
        self.assertEqual('proposed', out['status'])
        self.assertEqual(1, len(h.sent))
        mail = h.sent[0]
        self.assertEqual(DAVID, mail['to'])
        self.assertIn('[ref A1123a]', mail['subject'])
        self.assertEqual(['Amazon SKU List 11.23.xlsx', 'Transcal Pick Ticket 11.23 - Amazon draft A1123a.xlsx'], mail['files'])
        self.assertIn('UPS (Amazon partnered) shipping: $42.50', mail['html'])
        self.assertIn('Carton size and weight sent to Amazon', mail['html'])
        self.assertEqual([], h.inbound.confirmed)
        ticket = base64.b64decode(mail['attachments'][1]['content'])
        import openpyxl
        ws = openpyxl.load_workbook(io.BytesIO(ticket))['Order Import Template']
        self.assertEqual('PENDING-SWF2-A', ws['A3'].value)
        # a second run while David has not answered sends nothing new
        self.assertEqual('waiting_for_david', h.agent.run(NOW + timedelta(hours=2))['status'])
        self.assertEqual(1, len(h.sent))

    def test_unanswered_proposal_expires_and_a_new_one_goes_out(self):
        h = Harness()
        h.agent.run(NOW)
        out = h.agent.run(NOW + timedelta(days=4))
        self.assertEqual('proposed', out['status'])
        self.assertEqual(['wf1'], h.inbound.cancelled)
        self.assertEqual('expired', h.store['state']['proposals']['A1123a']['status'])

    def test_amazon_refusing_the_draft_is_reported_and_nothing_waits(self):
        h = Harness(fail_create=True)
        out = h.agent.run(NOW)
        self.assertEqual('draft_failed', out['status'])
        self.assertIn('could not be set up', h.sent[0]['subject'])
        self.assertEqual(['Amazon SKU List 11.23.xlsx'], h.sent[0]['files'])
        self.assertIsNone(h.store['state']['current'])


class ReplyTests(unittest.TestCase):

    def setUp(self):
        self.h = Harness()
        self.h.agent.run(NOW)
        self.subject = 'Re: ' + self.h.sent[0]['subject']
        self.h.sent.clear()

    def test_yes_confirms_and_sends_transcal_the_ticket_and_labels(self):
        out = self.h.agent.handle_reply(DAVID, self.subject, 'Yes, send it\n\nOn Mon, Versa Amazon Agent wrote:\n> drop AMZZSU901SLS')
        self.assertEqual('sent', out['status'])
        self.assertEqual(['wf1'], self.h.inbound.confirmed)
        to_transcal, to_david = self.h.sent
        self.assertEqual([TRANSCAL], to_transcal['to'])
        self.assertTrue(to_transcal['subject'].startswith('Original Brands - Amazon Shipment'))
        self.assertEqual(['Transcal Pick Ticket 11.23 - Amazon (1 FBA).xlsx', 'FBATESTZZZZ box labels.pdf'], to_transcal['files'])
        self.assertIn('FBATESTZZZZ (SWF2)', to_david['html'])
        self.assertEqual('sent', self.h.store['state']['proposals']['A1123a']['status'])
        again = self.h.agent.handle_reply(DAVID, self.subject, 'yes')
        self.assertEqual('not_waiting', again['status'])                 # a second YES does nothing

    def test_only_an_approver_can_ship(self):
        out = self.h.agent.handle_reply(STRANGER, self.subject, 'YES')
        self.assertEqual('ignored', out['status'])
        self.assertEqual([], self.h.inbound.confirmed)
        self.assertEqual([], self.h.sent)

    def test_no_cancels_the_draft(self):
        out = self.h.agent.handle_reply(DAVID, self.subject, 'No thanks')
        self.assertEqual('cancelled', out['status'])
        self.assertEqual(['wf1'], self.h.inbound.cancelled)
        self.assertIn('Nothing was sent', self.h.sent[0]['html'])

    def test_drop_a_style_by_its_end_makes_a_new_draft(self):
        out = self.h.agent.handle_reply(DAVID, self.subject, 'Drop 901SLS please')
        self.assertEqual('proposed', out['status'])
        self.assertEqual(['wf1'], self.h.inbound.cancelled)
        self.assertTrue(all(not w.startswith('AMZZSU901SLS') for w in self.h.inbound.created[-1]))
        self.assertIn('dropped AMZZSU901SLS', self.h.sent[0]['html'])
        self.assertIn('[ref A1123b]', self.h.sent[0]['subject'])
        self.assertEqual('replaced', self.h.store['state']['proposals']['A1123a']['status'])

    def test_cap_a_style(self):
        out = self.h.agent.handle_reply(DAVID, self.subject, 'only 2 cartons of AMZZSU902SLS')
        self.assertEqual('proposed', out['status'])
        new = self.h.inbound.created[-1]
        self.assertEqual(2, sum(1 for w in new if w.startswith('AMZZSU902SLS')))
        self.assertTrue(any(w.startswith('AMZZSU901SLS') for w in new))

    def test_a_style_not_on_the_shipment_changes_nothing(self):
        out = self.h.agent.handle_reply(DAVID, self.subject, 'drop AMZZSU999SLS')
        self.assertEqual('not_on_shipment', out['status'])
        self.assertEqual([], self.h.inbound.cancelled)

    def test_anything_else_gets_a_short_note(self):
        out = self.h.agent.handle_reply(DAVID, self.subject, 'what is this?')
        self.assertEqual('not_understood', out['status'])
        self.assertIn('Reply YES', self.h.sent[0]['html'])

    def test_status_has_no_amazon_numbers(self):
        st = self.h.agent.status()
        self.assertTrue(st['ready'])
        self.assertEqual('awaiting_ok', st['current']['status'])
        self.assertNotIn('sold', repr(st))


if __name__ == '__main__':
    unittest.main()
