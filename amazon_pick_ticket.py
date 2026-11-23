"""
amazon_pick_ticket.py: the Transcal pick ticket for Amazon FBA shipments, in Transcal's
order-import layout (templates/transcal_order_import.xlsx is the blank of their
'Transcal Pick Ticket 4.22.xlsx'). The Amazon agent attaches it to the email that asks
David to confirm a shipment, and sends it to Transcal only after his yes (David, Nov 23
2026: the agent never acts on its own).

A shipment is a list of cartons, each exactly what Transcal picks:

    Carton(warehouse_sku, contents)     contents = {Amazon SKU: units in this carton}
        prepack carton:      warehouse_sku is the style (AMNASU207SLS); contents span sizes
        single-size carton:  warehouse_sku is the sized row Transcal stocks
                             (AMNASU221SLS-1515.53233, AMNALC207SSS-M); one Amazon SKU inside
    Shipment(shipment_id, fc, address, cartons, expected_units=None)
    Address(line1, line2, city, state, zip)

    ticket_rows(shipments)                    -> the exact rows written, as dicts keyed A..Y
    build_ticket(shipments, template_bytes)   -> xlsx bytes
    ticket_filename(when, label)              -> 'Transcal Pick Ticket 11.23 - <label>.xlsx'

Rules (David, Jul-Sep 2026; the Sep 4 2026 ticket Transcal accepted is the reference):
    one row per warehouse SKU per shipment, SKUs alphabetical, a blank row between shipments
    A = B = '<shipment id>-<letter>', letters A, B, C ... in shipment order; C = 'UPS'
    K = the FC code; L/M/N/O/P = the FC address from the box labels, never the packing
        list's 'Ship to' (it named Hopewell Junction for SWF2 while the labels say Yonkers)
    X = the WAREHOUSE SKU, never the Amazon SKU; Y = total units
    I (Notes) on every row: the Amazon SKU(s) and how the cartons are packed
"""
import io
import os
from collections import namedtuple
from datetime import date, datetime

import amazon_skus as K

SHEET = 'Order Import Template'
FIRST_ROW = 3
CARRIER = 'UPS'
TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'templates', 'transcal_order_import.xlsx')
TEXT_COLUMNS = ('A', 'B', 'I', 'K', 'L', 'M', 'N', 'O', 'P', 'X')   # written as text ('@'): zips keep zeros

Address = namedtuple('Address', 'line1 line2 city state zip')


class TicketError(ValueError):
    """A shipment that cannot become a correct pick ticket. The message says why."""


class Carton:
    __slots__ = ('warehouse_sku', 'contents')

    def __init__(self, warehouse_sku, contents):
        self.warehouse_sku = K.norm_sku(warehouse_sku)
        self.contents = {}
        for sku, qty in dict(contents or {}).items():
            n = int(qty)
            if n <= 0:
                raise TicketError(f'carton {self.warehouse_sku}: {sku} has {qty} units')
            # Amazon SKUs stay exactly as Amazon lists them ('AMUSOX275SLB - XL' included):
            # the inbound plan must name the seller SKU character for character.
            key = str(sku).strip()
            self.contents[key] = self.contents.get(key, 0) + n
        if not self.warehouse_sku or not self.contents:
            raise TicketError('a carton needs a warehouse SKU and at least one Amazon SKU')

    @property
    def units(self):
        return sum(self.contents.values())

    @property
    def is_prepack(self):
        return not K.split_sku(self.warehouse_sku).size

    def __repr__(self):
        return f'<Carton {self.warehouse_sku} {self.units}u>'


class Shipment:
    __slots__ = ('shipment_id', 'fc', 'address', 'cartons', 'expected_units')

    def __init__(self, shipment_id, fc, address, cartons, expected_units=None):
        self.shipment_id = str(shipment_id or '').strip().upper()
        self.fc = str(fc or '').strip().upper()
        self.address = address if isinstance(address, Address) else Address(*address)
        self.cartons = list(cartons or [])
        self.expected_units = expected_units
        if not self.shipment_id or not self.fc:
            raise TicketError('a shipment needs its Amazon shipment id and FC code')
        a = self.address
        if not (a.line1 and a.city and a.state and a.zip):
            raise TicketError(f'{self.shipment_id}: the FC address is incomplete')
        if not self.cartons:
            raise TicketError(f'{self.shipment_id}: no cartons')


def _sized(amazon_sku):
    return K.split_sku(amazon_sku).size


def _ordered(skus):
    return sorted(skus, key=lambda s: (K.size_rank(_sized(s)), s))


def _ratio_label(carton):
    """'S4/M8/MT4/L4/LT8/XL8' from one prepack carton's contents."""
    parts = []
    for sku in _ordered(carton.contents):
        parts.append(f'{_sized(sku) or sku}{carton.contents[sku]}')
    return '/'.join(parts)


def _note(cartons):
    """The Notes text for one ticket row (all cartons of one warehouse SKU in one shipment).
    Wording matches the Sep 4 2026 ticket David approved."""
    n = len(cartons)
    first = cartons[0]
    per = first.units
    totals = {}
    for c in cartons:
        for sku, qty in c.contents.items():
            totals[sku] = totals.get(sku, 0) + qty
    if first.is_prepack:
        listing = ' / '.join(f'{sku} {totals[sku]}' for sku in _ordered(totals))
        same = all(c.contents == first.contents for c in cartons)
        packing = f'({_ratio_label(first)})' if same else '(mixed size breakdowns)'
        return (f'Amazon SKUs {listing} - warehouse SKU is the style, '
                f'packed in {per}pc prepack cartons {packing} - {n} cartons')
    if len(totals) == 1:
        sku = next(iter(totals))
        parts = K.split_sku(sku)
        size = parts.size or '?'
        differs = '' if parts.base + '-' + parts.size == cartons[0].warehouse_sku else ' - warehouse SKU differs'
        return f'Amazon SKU {sku} (size {size}){differs} - packed by size, {per}pc per carton ({n} cartons)'
    listing = ' / '.join(f'{sku} {totals[sku]}' for sku in _ordered(totals))
    return f'Amazon SKUs {listing} - warehouse SKU differs - packed by size, {per}pc per carton ({n} cartons)'


def _letter(i):
    if i >= 26:
        raise TicketError('more than 26 shipments on one ticket')
    return chr(ord('A') + i)


def ticket_rows(shipments):
    """The rows of the ticket, in order, with None between shipments (the blank row).
    Each row is a dict keyed by column letter."""
    out = []
    for i, shp in enumerate(shipments):
        ref = f'{shp.shipment_id}-{_letter(i)}'
        groups = {}
        for c in shp.cartons:
            groups.setdefault(c.warehouse_sku, []).append(c)
        total = 0
        if i:
            out.append(None)
        for wh_sku in sorted(groups):
            cartons = groups[wh_sku]
            qty = sum(c.units for c in cartons)
            total += qty
            a = shp.address
            out.append({'A': ref, 'B': ref, 'C': CARRIER, 'I': _note(cartons), 'K': shp.fc,
                        'L': a.line1, 'M': a.line2 or None, 'N': a.city, 'O': a.state, 'P': str(a.zip),
                        'X': wh_sku, 'Y': qty})
        if shp.expected_units is not None and int(shp.expected_units) != total:
            raise TicketError(f'{shp.shipment_id}: ticket has {total} units but Amazon expects {shp.expected_units}')
    return out


def build_ticket(shipments, template_bytes=None):
    """The pick ticket as xlsx bytes, written into Transcal's blank template."""
    import openpyxl
    from openpyxl.styles import Font

    rows = ticket_rows(shipments)
    if template_bytes is None:
        with open(TEMPLATE_PATH, 'rb') as fh:
            template_bytes = fh.read()
    wb = openpyxl.load_workbook(io.BytesIO(template_bytes))
    if SHEET not in wb.sheetnames:
        raise TicketError(f'template has no "{SHEET}" sheet')
    ws = wb[SHEET]
    if ws.max_row >= FIRST_ROW:
        ws.delete_rows(FIRST_ROW, ws.max_row - FIRST_ROW + 1)
    font = Font(name='Calibri', size=11)
    r = FIRST_ROW
    for row in rows:
        if row is not None:
            for col, value in row.items():
                if value is None:
                    continue
                cell = ws[f'{col}{r}']
                cell.value = value
                cell.font = font
                if col in TEXT_COLUMNS:
                    cell.number_format = '@'
        r += 1
    buf = io.BytesIO()
    wb.save(buf)
    return buf.getvalue()


def ticket_filename(when=None, label='Amazon'):
    """'Transcal Pick Ticket 11.23 - Amazon (3 FBA).xlsx' style, the name David uses."""
    d = when or date.today()
    if isinstance(d, datetime):
        d = d.date()
    safe = ''.join(ch for ch in str(label) if ch not in '\\/:*?"<>|').strip() or 'Amazon'
    return f'Transcal Pick Ticket {d.month}.{d.day} - {safe}.xlsx'
