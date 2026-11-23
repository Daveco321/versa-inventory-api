"""amazon_inbound.py: the unconfirmed Amazon draft (plan, box contents, cheapest placement, UPS
quotes) and the confirm step after David's YES, against a fake Amazon that follows the
v2024-03-20 shapes. Synthetic SKUs, ids and addresses only."""
import os
import sys
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

import amazon_inbound as I  # noqa: E402
from amazon_pick_ticket import Carton  # noqa: E402

API = I.API
PREPACK = {'AMZZSU901SLS-S': 4, 'AMZZSU901SLS-M': 8, 'AMZZSU901SLS-MT': 4,
           'AMZZSU901SLS-L': 4, 'AMZZSU901SLS-LT': 8, 'AMZZSU901SLS-XL': 8}
SINGLE = {'AMZZSU903SLS - M': 36}                  # an Amazon SKU written with spaces stays exact
SOURCE = {'name': 'Test Warehouse', 'addressLine1': '1 Test Way', 'city': 'Testville',
          'stateOrProvinceCode': 'CA', 'postalCode': '90001', 'countryCode': 'US', 'phoneNumber': '5550100'}
BOX = {'length': 24, 'width': 18, 'height': 9, 'weight_lb': 25}


def cartons():
    return [Carton('AMZZSU901SLS', PREPACK) for _ in range(3)] + [Carton('AMZZSU903SLS-1515.53233', SINGLE) for _ in range(2)]


class Config:
    marketplace_id = 'ATVPDKIKX0DER'


class FakeAmazon:
    def __init__(self, fail_op=None, bad_split=False):
        self.config = Config()
        self.fail_op = fail_op
        self.bad_split = bad_split
        self.calls = []
        self.bodies = {}
        self.op_step = {}
        self.slept = 0.0
        self.t = 0.0
        self.cancelled = False

    def _sleep(self, s):
        self.slept += s
        self.t += s

    def _clock(self):
        return self.t

    def download_document(self, doc):
        assert doc['url'].startswith('https://labels.example.test/')
        return b'%PDF-1.4 fake labels'

    def _op(self, name):
        self.op_step[name] = 0
        return {'operationId': name}

    def call(self, method, path, op, params=None, body=None):
        self.calls.append((method, path, op))
        if body is not None:
            self.bodies[op] = body
        p = path.replace(API, '')
        if op == 'getInboundOperationStatus':
            name = p.rsplit('/', 1)[-1]
            self.op_step[name] = self.op_step.get(name, 0) + 1
            if name == self.fail_op:
                return {'operationStatus': 'FAILED', 'operationProblems': [
                    {'code': 'TestProblem', 'message': 'Amazon refused this step', 'severity': 'ERROR'}]}
            if self.op_step[name] == 1:
                return {'operationStatus': 'IN_PROGRESS', 'operationProblems': []}
            return {'operationStatus': 'SUCCESS', 'operationProblems': [
                {'code': 'W1', 'message': 'just a warning', 'severity': 'WARNING'}] if name == 'op-create' else []}
        if op == 'listPrepDetails':
            return {'mskuPrepDetails': [{'msku': m, 'prepCategory': 'TEXTILE', 'prepTypes': ['ITEM_POLYBAGGING']}
                                        for m in params['mskus']]}
        if op == 'createInboundPlan':
            return dict(self._op('op-create'), inboundPlanId='wfTESTPLAN')
        if op == 'cancelInboundPlan':
            self.cancelled = True
            return self._op('op-cancel')
        if op in ('generatePackingOptions', 'confirmPackingOption', 'setPackingInformation',
                  'generatePlacementOptions', 'generateTransportationOptions', 'confirmPlacementOption',
                  'confirmTransportationOptions'):
            return self._op({'generatePlacementOptions': 'op-place', 'setPackingInformation': 'op-pack'}.get(op, 'op-' + op))
        if op == 'listPackingOptions':
            return {'packingOptions': [{'packingOptionId': 'poTEST', 'packingGroups': ['pgTEST'], 'status': 'OFFERED',
                                        'fees': [], 'discounts': [], 'supportedConfigurations': [
                                            {'boxPackingMethods': ['BOX_CONTENT_PROVIDED'],
                                             'shippingRequirements': [{'solution': 'AMAZON_PARTNERED_CARRIER',
                                                                       'modes': ['GROUND_SMALL_PARCEL']}]}]}]}
        if op == 'listPackingGroupItems':
            return {'items': [{'msku': m} for m in list(PREPACK) + list(SINGLE)]}
        if op == 'listPlacementOptions':
            fee = lambda amt: [{'type': 'FEE', 'target': 'Placement Services', 'description': 'fee',
                                'value': {'code': 'USD', 'amount': amt}}]
            return {'placementOptions': [
                {'placementOptionId': 'plMINIMAL', 'shipmentIds': ['shONE'], 'status': 'OFFERED', 'fees': fee(61.2), 'discounts': []},
                {'placementOptionId': 'plSPLIT', 'shipmentIds': ['shA', 'shB'], 'status': 'OFFERED', 'fees': fee(0), 'discounts': []}]}
        if op == 'listShipmentItems':
            sid = p.split('/shipments/')[1].split('/')[0]
            if sid == 'shA':
                q = 36 if self.bad_split else 72
                return {'items': [{'msku': m, 'quantity': n * (2 if not self.bad_split else 1)} for m, n in PREPACK.items()]
                        + [{'msku': 'AMZZSU903SLS - M', 'quantity': q}]}
            return {'items': [{'msku': m, 'quantity': n} for m, n in PREPACK.items()]}
        if op == 'getShipment':
            sid = p.split('/shipments/')[1]
            dest = {'shA': {'warehouseId': 'SWF2', 'address': {}},
                    'shB': {'warehouseId': 'ZZT1', 'address': {'addressLine1': '9 Sample Rd', 'city': 'Exampleton',
                                                               'stateOrProvinceCode': 'NV', 'postalCode': '89001'}}}[sid]
            out = {'shipmentId': sid, 'destination': dest}
            if self.op_step.get('op-confirmPlacementOption'):
                out['shipmentConfirmationId'] = {'shA': 'FBATESTAAAA', 'shB': 'FBATESTBBBB'}[sid]
            return out
        if op == 'listTransportationOptions':
            opts = []
            for sid, base in (('shA', 40.0), ('shB', 20.0)):
                opts += [
                    {'transportationOptionId': f'to{sid}cheap', 'shipmentId': sid, 'shippingSolution': 'AMAZON_PARTNERED_CARRIER',
                     'shippingMode': 'GROUND_SMALL_PARCEL', 'carrier': {'name': 'UPS', 'alphaCode': 'UPSN'},
                     'quote': {'cost': {'code': 'USD', 'amount': base}}, 'preconditions': []},
                    {'transportationOptionId': f'to{sid}dear', 'shipmentId': sid, 'shippingSolution': 'AMAZON_PARTNERED_CARRIER',
                     'shippingMode': 'GROUND_SMALL_PARCEL', 'carrier': {'name': 'UPS', 'alphaCode': 'UPSN'},
                     'quote': {'cost': {'code': 'USD', 'amount': base * 3}}, 'preconditions': []},
                    {'transportationOptionId': f'to{sid}own', 'shipmentId': sid, 'shippingSolution': 'USE_YOUR_OWN_CARRIER',
                     'shippingMode': 'GROUND_SMALL_PARCEL', 'carrier': {'name': 'Other'}, 'preconditions': ['CONFIRMED_DELIVERY_WINDOW']}]
            return {'transportationOptions': opts}
        if op == 'listShipmentBoxes':
            sid = p.split('/shipments/')[1].split('/')[0]
            return {'boxes': [{'packageId': 'pk1', 'quantity': 4 if sid == 'shA' else 1}]}
        if op == 'getLabels':
            assert params['LabelType'] == 'UNIQUE' and params['PageType'] == I.DEFAULT_PAGE_TYPE
            return {'payload': {'DownloadURL': 'https://labels.example.test/' + path.split('/')[-2]}}
        raise AssertionError((method, path, op))


class DraftTests(unittest.TestCase):

    def test_draft_prices_everything_and_confirms_nothing(self):
        amz = FakeAmazon()
        d = I.create_draft(amz, 'Versa agent test', SOURCE, cartons(), BOX, '2026-11-24T16:00Z')
        confirms = [c for c in amz.calls if c[2] in ('confirmPlacementOption', 'confirmTransportationOptions')]
        self.assertEqual([], confirms)                                     # nothing committed at Amazon
        self.assertEqual(('wfTESTPLAN', 'plSPLIT', 0.0, 60.0), (d['inbound_plan_id'], d['placement_option_id'],
                                                                d['placement_fee'], d['shipping_cost']))
        plan_items = {x['msku']: x for x in amz.bodies['createInboundPlan']['items']}
        self.assertEqual(72, plan_items['AMZZSU903SLS - M']['quantity'])
        self.assertEqual(('NONE', 'SELLER'), (plan_items['AMZZSU901SLS-M']['labelOwner'], plan_items['AMZZSU901SLS-M']['prepOwner']))
        boxes = amz.bodies['setPackingInformation']['packageGroupings'][0]['boxes']
        self.assertEqual(sorted([3, 2]), sorted(b['quantity'] for b in boxes))       # identical cartons grouped
        self.assertEqual({'length': 24, 'width': 18, 'height': 9, 'unitOfMeasurement': 'IN'}, boxes[0]['dimensions'])
        by_fc = {s['fc']: s for s in d['shipments']}
        self.assertEqual(['SWF2', 'ZZT1'], sorted(by_fc))
        self.assertEqual(4, len(by_fc['SWF2']['cartons']))                # 2 prepacks + 2 single M
        self.assertEqual('toshAcheap', by_fc['SWF2']['transport_option_id'])           # cheapest partnered UPS
        self.assertEqual(('557 Tuckahoe Road', 'label-verified'), (by_fc['SWF2']['address']['line1'], by_fc['SWF2']['address_source']))
        self.assertEqual(('9 Sample Rd', 'amazon'), (by_fc['ZZT1']['address']['line1'], by_fc['ZZT1']['address_source']))
        self.assertIn('W1: just a warning', d['warnings'])

    def test_a_failed_step_names_amazons_reason_and_cancels_the_draft(self):
        amz = FakeAmazon(fail_op='op-place')
        with self.assertRaises(I.InboundError) as ctx:
            I.create_draft(amz, 'Versa agent test', SOURCE, cartons(), BOX, '2026-11-24T16:00Z')
        self.assertEqual('placement options', ctx.exception.step)
        self.assertIn('Amazon refused this step', ctx.exception.message)
        self.assertTrue(amz.cancelled)

    def test_cartons_amazon_did_not_place_whole_stop_the_draft(self):
        amz = FakeAmazon(bad_split=True)
        with self.assertRaises(I.InboundError) as ctx:
            I.create_draft(amz, 'Versa agent test', SOURCE, cartons(), BOX, '2026-11-24T16:00Z')
        self.assertEqual('placement', ctx.exception.step)
        self.assertTrue(amz.cancelled)


class ConfirmTests(unittest.TestCase):

    def test_confirm_gets_fba_ids_and_labels_then_the_ticket_uses_them(self):
        amz = FakeAmazon()
        d = I.create_draft(amz, 'Versa agent test', SOURCE, cartons(), BOX, '2026-11-24T16:00Z')
        draft_refs = [s.shipment_id for s in I.ticket_shipments(d)]
        self.assertEqual(['PENDING-SWF2', 'PENDING-ZZT1'], draft_refs)
        done = I.confirm_draft(amz, d)
        sel = amz.bodies['confirmTransportationOptions']['transportationSelections']
        self.assertEqual({'toshAcheap', 'toshBcheap'}, {s['transportationOptionId'] for s in sel})
        got = {s['fc']: s for s in done['shipments']}
        self.assertEqual(('FBATESTAAAA', 4, b'%PDF-1.4 fake labels'),
                         (got['SWF2']['confirmation_id'], got['SWF2']['boxes'], got['SWF2']['labels_pdf']))
        final = I.ticket_shipments(d, done)
        self.assertEqual(['FBATESTAAAA', 'FBATESTBBBB'], [s.shipment_id for s in final])
        self.assertEqual(144, final[0].expected_units)          # 2 prepack + 2 single-size cartons


class HelperTests(unittest.TestCase):

    def test_ready_window_skips_the_weekend(self):
        from datetime import datetime, timezone
        friday = datetime(2026, 11, 27, 15, 0, tzinfo=timezone.utc)
        self.assertEqual('2026-11-30T16:00Z', I.ready_window_start(friday))

    def test_unknown_fc_without_address_points_to_the_labels(self):
        addr, source = I.fc_address('ZZZ9', {})
        self.assertEqual('missing', source)
        self.assertIn('box labels', addr['line1'])


if __name__ == '__main__':
    unittest.main()
