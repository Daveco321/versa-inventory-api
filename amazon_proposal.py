"""
amazon_proposal.py: what David receives before any Amazon shipment happens (David, Nov 23
2026: "it should send me an email first confirming what it wants to do and attach the amazon
list of SKUs it wants to ship as well as the transcal pick ticket").

    sku_list_xlsx(plan, meta_for)          -> xlsx bytes: the Amazon SKU list attachment
    sku_list_filename(when)                -> 'Amazon SKU List 11.23.xlsx'
    proposal_email(plan, when, fcs=None)   -> (subject, html, text)

The SKU list follows David's own ship-tool export (Sep 4 2026: Warehouse SKU | Amazon SKU |
Brand | Color | Fit | Fabrication | Qty Selected | # of box) plus why each line is there:
units at Amazon now (incl. inbound), sold in the last 30 days and days left.
meta_for(base) -> {'brand', 'color', 'fit', 'fabrication'} comes from the platform.

Copy rules (David): plain words, short sentences, no em dashes.
"""
import html
import io
from datetime import date, datetime

COLUMNS = ('Warehouse SKU', 'Amazon SKU', 'Brand', 'Color', 'Fit', 'Fabrication', 'Qty Selected', '# of box',
           'At Amazon now', 'Sold last 30 days', 'Days left', 'Why')
WIDTHS = (26, 26, 14, 22, 18, 34, 12, 9, 13, 15, 10, 70)


def _d(when):
    d = when or date.today()
    return d.date() if isinstance(d, datetime) else d


def sku_list_filename(when=None):
    d = _d(when)
    return f'Amazon SKU List {d.month}.{d.day}.xlsx'


def sku_list_xlsx(plan, meta_for=None):
    """One row per warehouse SKU and Amazon SKU, styles in the order of the plan (most urgent
    first). The style's reason sits on its first row. A total row closes the sheet."""
    import openpyxl
    from openpyxl.styles import Alignment, Border, Font, PatternFill, Side

    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = 'Amazon SKUs'
    head_fill = PatternFill('solid', fgColor='ADD8E6')
    style_fill = PatternFill('solid', fgColor='F0F4F8')
    thin = Side(style='thin', color='BFBFBF')
    box = Border(left=thin, right=thin, top=thin, bottom=thin)
    bold = Font(name='Calibri', size=11, bold=True)
    plain = Font(name='Calibri', size=11)
    for i, (name, width) in enumerate(zip(COLUMNS, WIDTHS), start=1):
        c = ws.cell(row=1, column=i, value=name)
        c.font, c.fill, c.border = bold, head_fill, box
        c.alignment = Alignment(vertical='center', wrap_text=True)
        ws.column_dimensions[c.column_letter].width = width
    ws.freeze_panes = 'A2'
    r = 2
    total_units = total_boxes = 0
    counted_prepack = set()
    for st in plan.get('styles') or []:
        meta = (meta_for(st['base']) if meta_for else None) or {}
        first = True
        for ln in st['lines']:
            days = ln.get('days_left')
            values = (ln['warehouse_sku'], ln['amazon_sku'], meta.get('brand', ''), meta.get('color', ''),
                      meta.get('fit', ''), meta.get('fabrication', ''), ln['units'], ln['cartons'],
                      ln.get('at_amazon', 0), ln.get('sold30', 0),
                      ('' if days is None else days), st['why'] if first else '')
            for i, v in enumerate(values, start=1):
                c = ws.cell(row=r, column=i, value=v)
                c.font, c.border = plain, box
                c.alignment = Alignment(vertical='top', wrap_text=(i == len(values)))
                if first:
                    c.fill = style_fill
            total_units += ln['units']
            key = (st['base'], ln['warehouse_sku'])
            if ln['kind'] == 'single':
                total_boxes += ln['cartons']
            elif key not in counted_prepack:          # a prepack box holds every size: count it once
                counted_prepack.add(key)
                total_boxes += ln['cartons']
            first = False
            r += 1
    ws.cell(row=r, column=1, value='Total').font = bold
    ws.cell(row=r, column=7, value=total_units).font = bold
    ws.cell(row=r, column=8, value=total_boxes).font = bold
    buf = io.BytesIO()
    wb.save(buf)
    return buf.getvalue()


def proposal_email(plan, when=None, fcs=None, ref=None, extra=None):
    """(subject, html, text) for the confirmation email. fcs: optional list of
    (shipment id, FC code, cartons, units) from the unconfirmed Amazon draft. ref: the proposal
    reference a reply is matched on. extra: more plain sentences (costs, warnings)."""
    d = _d(when)
    t = plan.get('totals') or {}
    n_styles, n_cartons, n_units = t.get('styles', 0), t.get('cartons', 0), t.get('units', 0)
    subject = f'Amazon shipment for your OK: {n_cartons} cartons, {n_units:,} units ({d.month}/{d.day})'
    if ref:
        subject += f' [ref {ref}]'
    extra = [str(x) for x in (extra or []) if x]
    rows = []
    for st in plan.get('styles') or []:
        packs = []
        if st['prepack_cartons']:
            packs.append(f"{st['prepack_cartons']} prepack")
        if st['single_cartons']:
            packs.append(f"{st['single_cartons']} single-size")
        rows.append((st['base'], st['cartons'], st['units'], ' + '.join(packs), st['why']))
    lines = [f'The Amazon agent wants to send {n_cartons} cartons ({n_units:,} units, {n_styles} styles) '
             'from Transcal to Amazon. Nothing has been sent.', '']
    if fcs:
        lines.append('Amazon assigned these warehouses in the draft:')
        for sid, fc, ctn, units in fcs:
            lines.append(f'  {sid} to {fc}: {ctn} cartons, {units:,} units')
        lines.append('')
    for base, ctn, units, packs, why in rows:
        lines.append(f'{base}: {ctn} cartons ({packs}), {units:,} units. {why}')
    if extra:
        lines += [''] + extra
    lines += ['', 'Attached: the Amazon SKU list and the Transcal pick ticket.', '',
              'Reply YES to go ahead. To change it, reply in plain words, for example '
              '"drop AMNASU207SLS" or "only 5 cartons of AMUSCD179RFB".',
              'Nothing goes to Amazon or Transcal until you reply YES.']
    text = '\n'.join(lines)

    e = html.escape
    table = ''.join(
        f'<tr><td style="padding:6px 10px;border-bottom:1px solid #e5e7eb;font-weight:600">{e(base)}</td>'
        f'<td style="padding:6px 10px;border-bottom:1px solid #e5e7eb;text-align:right">{ctn}</td>'
        f'<td style="padding:6px 10px;border-bottom:1px solid #e5e7eb;text-align:right">{units:,}</td>'
        f'<td style="padding:6px 10px;border-bottom:1px solid #e5e7eb">{e(packs)}</td>'
        f'<td style="padding:6px 10px;border-bottom:1px solid #e5e7eb;color:#374151">{e(why)}</td></tr>'
        for base, ctn, units, packs, why in rows)
    fc_html = ''
    if fcs:
        fc_html = ('<p style="margin:12px 0 4px"><b>Amazon warehouses in the draft</b></p><ul style="margin:0">'
                   + ''.join(f'<li>{e(sid)} to {e(fc)}: {ctn} cartons, {units:,} units</li>'
                             for sid, fc, ctn, units in fcs) + '</ul>')
    body = (
        '<div style="font-family:Calibri,Arial,sans-serif;font-size:14px;color:#111827">'
        f'<p>The Amazon agent wants to send <b>{n_cartons} cartons ({n_units:,} units, {n_styles} styles)</b> '
        'from Transcal to Amazon. Nothing has been sent.</p>'
        f'{fc_html}'
        '<table style="border-collapse:collapse;margin:12px 0">'
        '<tr style="background:#ADD8E6"><th style="padding:6px 10px;text-align:left">Style</th>'
        '<th style="padding:6px 10px">Cartons</th><th style="padding:6px 10px">Units</th>'
        '<th style="padding:6px 10px;text-align:left">Packed as</th>'
        '<th style="padding:6px 10px;text-align:left">Why</th></tr>'
        f'{table}</table>'
        + ''.join(f'<p style="margin:4px 0">{e(x)}</p>' for x in extra) +
        '<p>Attached: the Amazon SKU list and the Transcal pick ticket.</p>'
        '<p><b>Reply YES to go ahead.</b> To change it, reply in plain words, for example '
        '"drop AMNASU207SLS" or "only 5 cartons of AMUSCD179RFB".<br>'
        'Nothing goes to Amazon or Transcal until you reply YES.</p></div>')
    return subject, body, text
