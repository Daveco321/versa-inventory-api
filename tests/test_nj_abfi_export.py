"""NJ / AE / AW / ABFI rules on customer exports (INV-10).

Run: python -B -m unittest discover -s tests -t .

Guards the Sep 16 2026 fix in two parts:

  1. Units of a production landing in a hidden warehouse are cut exactly ONCE
     from a customer export. The page (or the catalog feed) cuts them first, so
     _customer_export_scrub cutting them again subtracted the same units twice:
     a style with 1,044 sellable units exported as 72 and four styles vanished.
  2. Restricted warehouse stock never drives a customer's Total ATS below zero.

The ledger, the inventory rows and the export rows are all built here; no test
touches the network (tests._support blocks sockets before app.py is imported).
The style numbers are the ones the audit named, so a regression reports the same
styles David saw. Quantities are the audit's; nothing here carries a price, a
cost or a customer term.
"""
import time
import unittest
from contextlib import contextmanager

from tests._support import load_app

# style, landing warehouse (style ledger column I), units.
# TR lots are what a customer may be offered; NJ and ABFI lots are admin-only.
LEDGER = [
    ('TMKLCW031SLY', 'TR', 1044),
    ('TMKLCW031SLY', 'NJ', 324), ('TMKLCW031SLY', 'NJ', 324), ('TMKLCW031SLY', 'NJ', 324),
    # hidden units are larger than the visible lot: the second cut emptied these
    # four rows completely and the drop rule then deleted the style
    ('TMKLCJ043SLP', 'TR', 720), ('TMKLCJ043SLP', 'NJ', 1296),
    ('TMKLCM041SLS', 'TR', 720), ('TMKLCM041SLS', 'NJ', 1296),
    ('TMKLCW034SLS', 'TR', 720), ('TMKLCW034SLS', 'NJ', 1296),
    ('AMKLCW002SLB', 'TR', 576), ('AMKLCW002SLB', 'NJ', 648),
    ('TMEBPE017SLP', 'TR', 1206), ('TMEBPE017SLP', 'ABFI', 810),
    ('ZZCONTROL01SLS', 'TR', 500),       # nothing hidden: never touched
    ('ZZHIDDENONLY1SLS', 'NJ', 900),     # hidden only: never reaches a customer
]
HIDDEN_WH = {'NJ', 'AE', 'AW', 'ABFI'}
VISIBLE_UNITS = {}
HIDDEN_UNITS = {}
for _s, _w, _u in LEDGER:
    if _w in HIDDEN_WH:
        HIDDEN_UNITS[_s] = HIDDEN_UNITS.get(_s, 0) + _u
    else:
        VISIBLE_UNITS[_s] = VISIBLE_UNITS.get(_s, 0) + _u

# The four styles the customer line sheet dropped altogether.
VANISHED = ['TMKLCJ043SLP', 'TMKLCM041SLS', 'TMKLCW034SLS', 'AMKLCW002SLB']
# Every style that carries overseas supply in these fixtures.
OVERSEAS = sorted(set(VISIBLE_UNITS) | set(HIDDEN_UNITS))


def ledger_rows(with_refs=False):
    out = []
    for i, (style, wh, units) in enumerate(LEDGER):
        row = {'style': style, 'warehouse': wh, 'units': units,
               'production': '', 'etd': None, 'arrival': None}
        if with_refs:
            row['production'] = ('ZH%04d' % i) if wh in HIDDEN_WH else ('ZV%04d' % i)
            row['etd'] = '2026-10-01'
            row['arrival'] = '2026-11-15'
        out.append(row)
    return out


# What the page puts in the warehouse column when a style is in no warehouse a
# customer can be shipped from. The scrub reads it to tell "overseas only" apart
# from "in stock", so the fixtures have to use the real sentinel.
NO_WAREHOUSE = '—'


def export_row(sku, incoming, total_ats, warehouse=NO_WAREHOUSE, total_warehouse=0, **extra):
    """One row of a customer-format export payload, as the page posts it."""
    row = {'sku': sku, 'brand_abbr': 'ZZ', 'incoming': int(incoming),
           'total_ats': int(total_ats), 'total_warehouse': int(total_warehouse),
           'warehouse': warehouse, 'delivery': 'ATS' if total_warehouse else 'Overseas'}
    row.update(extra)
    return row


def raw_rows():
    """What the admin screen holds: visible plus hidden units, nothing cut yet."""
    return [export_row(s, VISIBLE_UNITS.get(s, 0) + HIDDEN_UNITS.get(s, 0),
                       VISIBLE_UNITS.get(s, 0) + HIDDEN_UNITS.get(s, 0)) for s in OVERSEAS]


def cut_rows():
    """What a current page posts: the hidden units are already gone. A style whose
    only supply is hidden never leaves the page at all."""
    return [export_row(s, VISIBLE_UNITS[s], VISIBLE_UNITS[s])
            for s in OVERSEAS if VISIBLE_UNITS.get(s, 0) > 0]


def by_sku(rows):
    return {str(r.get('sku')): r for r in rows if isinstance(r, dict)}


class CustomerExportScrub(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.app = load_app()

    @contextmanager
    def ledger(self, rows):
        """Swap the in-memory style ledger for the length of one check. An empty
        list means the ledger is unavailable; the retry clock is pushed forward so
        the helper cannot try to load it (the network is blocked in these tests)."""
        a = self.app
        keep_rows, keep_retry = a._production_data, a._ledger_retry_after
        a._production_data = list(rows)
        a._ledger_retry_after = time.time() + 300
        try:
            yield
        finally:
            a._production_data = keep_rows
            a._ledger_retry_after = keep_retry

    @contextmanager
    def request(self, catalog_scope=None):
        """An app context, so g works, optionally carrying a catalog-link scope."""
        with self.app.app.app_context():
            if catalog_scope is not None:
                self.app.g._catalog_scope = catalog_scope
            yield

    def scrub(self, rows, view_mode='all', nj=True, abfi=True, landing=None, scope=None,
              ledger=None):
        with self.ledger(ledger_rows() if ledger is None else ledger):
            with self.request(scope):
                return self.app._customer_export_scrub([dict(r) for r in rows],
                                                       view_mode, nj, abfi, landing)

    # ── 1. the hidden-landing cut runs exactly once ────────────────────────────

    def test_already_cut_payload_is_not_cut_again(self):
        """The headline case: the page cut 972 units of NJ-landing lots off
        TMKLCW031SLY and posted the 1,044 a customer can buy. It used to export 72."""
        out = by_sku(self.scrub(cut_rows(), landing=True))
        self.assertIn('TMKLCW031SLY', out)
        self.assertEqual(out['TMKLCW031SLY']['total_ats'], 1044)
        self.assertEqual(out['TMKLCW031SLY']['incoming'], 1044)
        for style, units in VISIBLE_UNITS.items():
            if units <= 0:
                continue
            self.assertIn(style, out, f'{style} was dropped by a second cut')
            self.assertEqual(out[style]['total_ats'], units, style)

    def test_uncut_payload_is_still_cut_once(self):
        """A build that does not cut client-side (or does not say so) is still
        scrubbed: the hidden units come off exactly once."""
        out = by_sku(self.scrub(raw_rows(), landing=None))
        self.assertEqual(out['TMKLCW031SLY']['total_ats'], 1044)
        self.assertEqual(out['TMKLCW031SLY']['incoming'], 1044)
        for style, units in VISIBLE_UNITS.items():
            if units > 0:
                self.assertEqual(out[style]['total_ats'], units, style)
        # a style whose only supply lands in NJ never survives
        self.assertNotIn('ZZHIDDENONLY1SLS', out)

    def test_the_four_dropped_styles_survive(self):
        """Hidden units larger than the visible lot: the second cut emptied the row
        and the drop rule deleted the style. All four must print their visible units."""
        out = by_sku(self.scrub(cut_rows(), landing=True))
        for style in VANISHED:
            self.assertIn(style, out, f'{style} vanished from the sheet')
            self.assertEqual(out[style]['total_ats'], VISIBLE_UNITS[style], style)
        self.assertEqual([out[s]['total_ats'] for s in VANISHED], [720, 720, 720, 576])

    def test_catalog_scope_needs_no_flag(self):
        """An anonymous catalog link posts rows that came from a feed the server
        already cut, so nothing is cut again even when the client says nothing."""
        out = by_sku(self.scrub(cut_rows(), landing=None, scope={'all': True}))
        self.assertEqual(out['TMKLCW031SLY']['total_ats'], 1044)
        for style in VANISHED:
            self.assertIn(style, out)

    def test_flag_is_ignored_without_a_ledger(self):
        """Ledger down: no row can be told apart, so the cut runs whatever the
        payload claims and the sheet keeps warehouse stock only."""
        rows = cut_rows() + [export_row('ZZWAREHOUSE1SLS', 0, 300, warehouse='TR', total_warehouse=300)]
        out = by_sku(self.scrub(rows, landing=True, ledger=[]))
        for style in VISIBLE_UNITS:
            self.assertNotIn(style, out, f'{style} kept overseas units with no ledger')
        self.assertIn('ZZWAREHOUSE1SLS', out)
        self.assertEqual(out['ZZWAREHOUSE1SLS']['total_ats'], 300)

    def test_warehouse_view_is_never_cut(self):
        """Warehouse ATS sheets carry no overseas units; the landing cut never runs."""
        rows = [export_row(s, 0, 400, warehouse='TR', total_warehouse=400) for s in OVERSEAS]
        for landing in (True, None):
            out = by_sku(self.scrub(rows, view_mode='ats', landing=landing))
            self.assertEqual(len(out), len(rows))
            for r in out.values():
                self.assertEqual(r['total_ats'], 400)

    def test_overseas_view_is_cut_only_when_the_ledger_is_down(self):
        """The overseas sheet is cut by the page; the server only steps in when the
        ledger is unavailable, and then it fails closed."""
        up = by_sku(self.scrub(cut_rows(), view_mode='incoming', landing=True))
        self.assertEqual(up['TMKLCW031SLY']['total_ats'], 1044)
        down = by_sku(self.scrub(cut_rows(), view_mode='incoming', landing=True, ledger=[]))
        self.assertEqual(down, {})

    def test_a_per_po_row_of_a_hidden_lot_is_dropped(self):
        """Per-delivery rows name their production. One landing in NJ never prints,
        flag or no flag."""
        led = ledger_rows(with_refs=True)
        hidden_ref = next(r['production'] for r in led
                          if r['style'] == 'TMKLCW031SLY' and r['warehouse'] == 'NJ')
        visible_ref = next(r['production'] for r in led
                           if r['style'] == 'TMKLCW031SLY' and r['warehouse'] == 'TR')
        rows = [export_row('TMKLCW031SLY', 324, 324, production=hidden_ref, _flow=True),
                export_row('TMKLCW031SLY', 1044, 1044, production=visible_ref, _flow=True)]
        for landing in (True, None):
            out = self.scrub(rows, landing=landing, ledger=led)
            refs = [str(r.get('production')) for r in out]
            self.assertEqual(refs, [visible_ref], f'landing_stripped={landing}')

    def test_rows_with_no_hidden_supply_are_untouched(self):
        """A style with no hidden lot keeps its units in every combination."""
        control = [export_row('ZZCONTROL01SLS', 500, 500)]
        for landing in (True, False, None):
            for scope in (None, {'all': True}):
                out = by_sku(self.scrub(control, landing=landing, scope=scope))
                self.assertEqual(out['ZZCONTROL01SLS']['total_ats'], 500)
                self.assertEqual(out['ZZCONTROL01SLS']['incoming'], 500)

    # ── 2. restricted stock never prints a negative ───────────────────────────

    def test_restricted_stock_never_takes_ats_below_zero(self):
        """Allocations had already consumed this style's ABFI units, so subtracting
        them again went below zero and the sheet printed a negative Total ATS."""
        row = export_row('ZZMIXED0001SLS', 0, 0, warehouse='DCW, ABFI',
                         total_warehouse=1908, nj=0, abfi=108)
        out = by_sku(self.scrub([row], landing=True))
        self.assertIn('ZZMIXED0001SLS', out)
        kept = out['ZZMIXED0001SLS']
        self.assertEqual(kept['total_ats'], 0)
        self.assertEqual(kept['total_warehouse'], 1800)
        self.assertEqual(kept['warehouse'], 'DCW')
        self.assertNotIn('nj', kept)
        self.assertNotIn('abfi', kept)

    def test_restricted_stock_is_still_subtracted(self):
        """The floor must not stop the subtraction where it has room to work."""
        row = export_row('ZZMIXED0002SLS', 0, 1000, warehouse='DCW, NJ',
                         total_warehouse=1000, nj=400, abfi=0)
        kept = by_sku(self.scrub([row], landing=True))['ZZMIXED0002SLS']
        self.assertEqual(kept['total_ats'], 600)
        self.assertEqual(kept['total_warehouse'], 600)
        self.assertEqual(kept['warehouse'], 'DCW')

    def test_a_restricted_only_row_is_still_dropped(self):
        """A row that exists only because of NJ or ABFI stock must never reach a
        customer, not even as a zero row. The floor must not keep it alive."""
        row = export_row('ZZNJONLY001SLS', 0, 108, warehouse='ABFI',
                         total_warehouse=108, nj=0, abfi=108)
        self.assertEqual(self.scrub([row], landing=True), [])

    def test_flag_does_not_disable_the_restricted_stock_strip(self):
        """hidden_landing_stripped speaks only for landings. Restricted STOCK on the
        row is still removed."""
        row = export_row('ZZMIXED0003SLS', 0, 500, warehouse='TR, NJ',
                         total_warehouse=500, nj=120, abfi=0)
        kept = by_sku(self.scrub([row], landing=True))['ZZMIXED0003SLS']
        self.assertEqual(kept['total_ats'], 380)
        self.assertEqual(kept['warehouse'], 'TR')


if __name__ == '__main__':
    unittest.main()
