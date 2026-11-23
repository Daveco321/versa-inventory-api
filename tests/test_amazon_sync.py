"""amazon_sync.py: the agent's read-only pull from Amazon (All Listings, FBA inventory, 30-day
orders) and the planner rows built from it. Fake client, synthetic SKUs only."""
import os
import sys
import unittest
from datetime import datetime, timezone

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

import amazon_spapi as S  # noqa: E402
import amazon_sync as Y  # noqa: E402

LISTINGS = ('item-name\tlisting-id\tseller-sku\tprice\tquantity\tasin1\tfulfillment-channel\tstatus\n'
            'Test Shirt M\t1\tAMZZSU901SLS-M-FBA\t30\t\tB0ZZ000001\tAMAZON_NA\tActive\n'
            'Test Shirt L\t2\tAMZZSU901SLS - L\t30\t\tB0ZZ000002\tAMAZON_NA\tActive\n'
            'Test Shirt M FBM\t3\tAMZZSU901SLS-M-FBM\t30\t5\tB0ZZ000003\tDEFAULT\tActive\n'
            'Old Shirt\t4\tAMZZSU902SLS-M\t30\t\tB0ZZ000004\tAMAZON_NA\tInactive\n').encode('utf-8')
ORDERS = ('amazon-order-id\torder-status\tsales-channel\tsku\titem-status\tquantity\tship-city\tship-postal-code\n'
          '111-1\tShipped\tAmazon.com\tAMZZSU901SLS-M-FBA\tShipped\t2\tTestville\t07001\n'
          '111-2\tPending\tAmazon.com\tAMZZSU901SLS-M-FBA\tUnshipped\t1\tTestville\t07001\n'
          '111-3\tCancelled\tAmazon.com\tAMZZSU901SLS-M-FBA\tCancelled\t5\tTestville\t07001\n'
          '111-4\tShipped\tAmazon.com\tAMZZSU901SLS-M-FBM\tShipped\t3\tTestville\t07001\n'
          '111-5\tShipped\tNon-Amazon\tAMZZSU901SLS - L\tShipped\t9\tTestville\t07001\n').encode('utf-8')
SUMMARIES = [
    {'sellerSku': 'AMZZSU901SLS-M-FBA', 'inventoryDetails': {
        'fulfillableQuantity': 40, 'inboundWorkingQuantity': 36, 'inboundShippedQuantity': 0,
        'inboundReceivingQuantity': 12, 'reservedQuantity': {'totalReservedQuantity': 3},
        'unfulfillableQuantity': {'totalUnfulfillableQuantity': 1}}},
    {'sellerSku': 'AMZZSU903SLS-XL', 'inventoryDetails': {'fulfillableQuantity': 7}},
]


class FakeClient:
    def __init__(self, fail=()):
        self.fail = set(fail)
        self.calls = 4
        self.order_range = None

    def run_report(self, report_type, start=None, end=None, options=None):
        if report_type in self.fail:
            raise S.SpApiError('createReport', 403, 'Unauthorized', 'Access to requested resource is denied.')
        if report_type == S.REPORT_ALL_LISTINGS:
            return LISTINGS
        if report_type == S.REPORT_ORDERS_BY_DATE:
            self.order_range = (start, end)
            return ORDERS
        raise AssertionError(report_type)

    def inventory_summaries(self):
        if 'fba' in self.fail:
            raise S.SpApiError('getInventorySummaries', 429, 'QuotaExceeded', 'slow down')
        return SUMMARIES


NOW = datetime(2026, 11, 23, 13, 0, tzinfo=timezone.utc)


class PullTests(unittest.TestCase):

    def test_snapshot_keeps_counts_not_orders(self):
        client = FakeClient()
        snap = Y.pull(client, now=NOW)
        self.assertEqual([], snap['problems'])
        self.assertEqual({'AMZZSU901SLS-M-FBA': 3, 'AMZZSU901SLS-M-FBM': 3}, snap['sold'])
        self.assertEqual([40, 48, 3, 1], snap['fba']['AMZZSU901SLS-M-FBA'])
        self.assertEqual('AMZZSU901SLS - L', snap['listings'][1][0])        # kept exactly as listed
        self.assertEqual(datetime(2026, 10, 24, 13, 0, tzinfo=timezone.utc), client.order_range[0])
        flat = repr(snap)
        for private in ('111-1', 'Testville', '07001'):
            self.assertNotIn(private, flat)                                  # no order-level data stored

    def test_one_failed_report_is_named_not_fatal(self):
        snap = Y.pull(FakeClient(fail={S.REPORT_ORDERS_BY_DATE, 'fba'}), now=NOW)
        self.assertEqual(2, len(snap['problems']))
        self.assertTrue(any('orders report' in p for p in snap['problems']))
        self.assertEqual(4, len(snap['listings']))
        self.assertEqual({}, snap['sold'])


class ListingTests(unittest.TestCase):

    def test_planner_rows(self):
        rows = {r.sku: r for r in Y.listings_from(Y.pull(FakeClient(), now=NOW))}
        m = rows['AMZZSU901SLS-M-FBA']
        self.assertEqual((40, 48, 3, True), (m.available, m.inbound, m.sold30, m.active))
        self.assertFalse(rows['AMZZSU902SLS-M'].active)                     # inactive listing
        self.assertFalse(rows['AMZZSU903SLS-XL'].active)                    # FBA stock but no listing row
        self.assertEqual(7, rows['AMZZSU903SLS-XL'].available)
        self.assertEqual(0, rows['AMZZSU901SLS - L'].sold30)                # the multi-channel order is not counted


if __name__ == '__main__':
    unittest.main()
