"""Substitute allocations held back from the Big 3 allocation reports.

David, Oct 6 2026: when a TJX / Ross / Burlington order line is substituted
(the Coverage Analytics "Select Sub" on the open-orders platform), the
replacement style is allocated to that customer in APO so the warehouse holds
it. Those allocations must never reach the customer's allocation report: the
buyer could order the substitute on top of what they already bought, or learn
that the goods are being substituted at all.

A row is a substitute allocation when EITHER holds:
  1. the APO allocation name says SUB (team convention '<CUST> SUB PO#<po>');
  2. the open-orders platform carries a "Select Sub" for a PO of this customer,
     the row's base style is one of the sub styles typed there, and the
     allocation name references that PO number.

Matching on the sub STYLE alone is deliberately NOT a rule: the Big 3 are
routinely allocated styles that carry another customer's prefix as ordinary
bulk allocations (Burlington holds thousands of units of Ross-prefixed styles
today), so a style-only match would hide legitimate allocations. Rows whose
base style is a live sub style but that were NOT hidden are returned as a
review list so the team can check them in the email body.

Pure functions, no I/O: app.py supplies the annotations, the PO -> customer
lookup and the APO rows.
"""
import re

# Allocation name says it is a substitute: SUB / SUBS / SUBSTITUTE / SUBSTITUTION.
SUB_WORD = re.compile(r'\bSUBS?\b|\bSUBSTITUT[A-Z]*', re.I)
# 'PO#4510000-16', 'PO 02-500422', 'po#450700001 NEED CONFIRM'
_PO_REF = re.compile(r'\bPO\s*#?\s*([0-9]+(?:-[0-9]{2,})*)', re.I)
# Fallback when a name carries a PO number without the 'PO' word: 7+ digits, dashes allowed.
_LONG_DIGITS = re.compile(r'(?<![0-9])[0-9]+(?:-[0-9]{2,})*(?![0-9])')
# A style token in the free-text sub field: 2 letters then 6+ letters/digits, at least one digit.
_SKU_TOKEN = re.compile(r'[A-Z]{2}[A-Z0-9]{6,}')

BIG3 = ('tjx', 'ross', 'burlington')
# Report customer key -> A2000 feed codes (open-orders /api/orders 'customer').
BIG3_A2000 = {'tjx': ('TJMA', 'MARS'), 'ross': ('ROSS',), 'burlington': ('BURL',)}
_CUSTOMER_ALIASES = {
    'burlington': 'burlington', 'burl': 'burlington', 'burlington stores': 'burlington',
    'ross': 'ross', 'ross stores': 'ross',
    'tjx': 'tjx', 'tjma': 'tjx', 'mars': 'tjx', 'tj maxx': 'tjx', 'tjmaxx': 'tjx',
    'tj max': 'tjx', 'marshalls': 'tjx',
}


def customer_key(name):
    """APO feed customer ('BURLINGTON ', 'Ross'), A2000 code ('BURL', 'TJMA') or full
    name ('Ross Stores') -> 'burlington' | 'ross' | 'tjx'; anything else lower-cased."""
    s = re.sub(r'\s+', ' ', str(name or '')).strip().lower()
    return _CUSTOMER_ALIASES.get(s, s)


def po_digits(s):
    """Digits only, leading zeros dropped (A2000 zero-pads some customers' POs;
    Burlington '4510000-16' and the feed's '451000016' are the same PO)."""
    return re.sub(r'\D', '', str(s or '')).lstrip('0')


def po_match(a, b):
    """Same PO: equal digit strings, or one is a 7+ digit prefix of the other
    (names that omit the line suffix: 'PO#4504000' vs feed '450400001')."""
    if not a or not b:
        return False
    if a == b:
        return True
    if min(len(a), len(b)) < 7:
        return False
    return a.startswith(b) or b.startswith(a)


def po_refs_in_name(name):
    """PO numbers referenced by an allocation name, as digit strings. 'PO#...'
    captures win; without the PO word any 7+ digit run counts."""
    text = str(name or '')
    refs = [po_digits(m.group(1)) for m in _PO_REF.finditer(text)]
    refs = [r for r in refs if r]
    if refs:
        return refs
    out = []
    for m in _LONG_DIGITS.finditer(text):
        d = po_digits(m.group(0))
        if len(d) >= 7:
            out.append(d)
    return out


def parse_sub_styles(text):
    """Style numbers typed into the Select Sub field (free text: commas, ' - ',
    '&', trailing notes). Upper-cased, de-duplicated, order kept."""
    out = []
    for tok in _SKU_TOKEN.findall(str(text or '').upper()):
        if any(c.isdigit() for c in tok) and tok not in out:
            out.append(tok)
    return out


def parse_annotation_key(key):
    """Coverage annotation key 'STYLE_PO_seq' (or legacy 'STYLE_PO') -> (style, po text)."""
    k = str(key or '')
    first = k.find('_')
    if first < 0:
        return k, ''
    last = k.rfind('_')
    if last > first:
        return k[:first], k[first + 1:last]
    return k[:first], k[first + 1:]


def sub_refs(annotations, resolve_customer):
    """Every Select Sub on the open-orders platform as a reference record.
    resolve_customer(po_digits) -> (customer_key, 'live' | 'archive') or (None, None).
    Records with no resolvable customer are kept (customer None) so the caller
    can still report them; they never match a customer's rows."""
    out = []
    for key, data in (annotations or {}).items():
        if not isinstance(data, dict):
            continue
        sub = data.get('sub')
        if not isinstance(sub, dict):
            continue
        styles = parse_sub_styles(sub.get('style'))
        if not styles:
            continue
        orig, po_text = parse_annotation_key(key)
        pod = po_digits(po_text)
        cust, source = (None, None)
        if pod:
            try:
                cust, source = resolve_customer(pod) or (None, None)
            except Exception:
                cust, source = (None, None)
        out.append({
            'key': key, 'orig_style': orig.upper(), 'po': po_text, 'po_digits': pod,
            'styles': styles, 'qty': sub.get('qty'), 'customer': cust, 'source': source,
            'set_by': sub.get('setBy') or '', 'set_at': sub.get('setAt') or '',
        })
    return out


def _base(style):
    return str(style or '').strip().upper().split('-')[0]


def split_customer_rows(rows, customer, refs):
    """rows = one customer's open APO allocations ({customer, po, style, qty}),
    already filtered for qty > 0 and the report's exclude tokens.
    Returns (kept, hidden, review):
      hidden rows gain '_sub_reason' ('name' | 'annotation') and, when an
      annotation explains them, '_sub_orig' / '_sub_po' (the ordered style and PO);
      review = kept rows that may still be substitutes, shown to the team only:
      the base style is a sub style of a LIVE Select Sub for this customer, or
      the allocation name references a PO that has a live Select Sub (the sub
      text is free text and is sometimes misspelled, so a style match alone
      would miss it). '_review_reason' says which ('style' | 'po')."""
    ck = customer_key(customer)
    mine = [r for r in (refs or []) if r.get('customer') == ck]
    kept, hidden, review = [], [], []
    for row in rows:
        name = str(row.get('po') or '')
        base = _base(row.get('style'))
        name_refs = po_refs_in_name(name)
        explain = None
        for ref in mine:
            if base in ref['styles'] and any(po_match(nr, ref['po_digits']) for nr in name_refs):
                explain = ref
                break
        says_sub = bool(SUB_WORD.search(name))
        if says_sub or explain:
            h = dict(row)
            h['_sub_reason'] = 'name' if says_sub else 'annotation'
            if explain is None:
                # A SUB-named row: still attach the ordered style when one annotation fits.
                for ref in mine:
                    if base in ref['styles'] and (
                            not name_refs or any(po_match(nr, ref['po_digits']) for nr in name_refs)):
                        explain = ref
                        break
            if explain:
                h['_sub_orig'] = explain['orig_style']
                h['_sub_po'] = explain['po']
            hidden.append(h)
            continue
        kept.append(row)
        for ref in mine:
            if ref.get('source') != 'live':
                continue
            by_style = base in ref['styles']
            by_po = any(po_match(nr, ref['po_digits']) for nr in name_refs)
            if by_style or by_po:
                rv = dict(row)
                rv['_sub_orig'] = ref['orig_style']
                rv['_sub_po'] = ref['po']
                rv['_review_reason'] = 'style' if by_style else 'po'
                review.append(rv)
                break
    return kept, hidden, review


def _qty(row):
    try:
        return int(row.get('qty') or 0)
    except Exception:
        return 0


def public_row(row):
    """A hidden/review row for a JSON answer: the allocation, style, units and
    the ordered style/PO when known. No internal keys leak into the payload."""
    out = {'po': row.get('po') or '', 'style': row.get('style') or '', 'qty': _qty(row)}
    if row.get('_sub_reason'):
        out['reason'] = row['_sub_reason']
    elif row.get('_review_reason'):
        out['reason'] = row['_review_reason']
    if row.get('_sub_orig'):
        out['for_style'] = row['_sub_orig']
    if row.get('_sub_po'):
        out['for_po'] = row['_sub_po']
    return out


def summary(hidden, review, subs_ok, annotations_count=0, po_map_ok=True):
    """subs_ok: the open-orders Select Sub list was read this run. po_map_ok: the
    PO -> customer lookup behind rule 2 was complete (live book and archive);
    False means Select Subs may have gone unmatched, so only rule 1 is certain."""
    return {
        'lines': len(hidden),
        'units': sum(_qty(r) for r in hidden),
        'styles': sorted({_base(r.get('style')) for r in hidden if _base(r.get('style'))}),
        'rows': [public_row(r) for r in hidden],
        'review': [public_row(r) for r in review],
        'subs_ok': bool(subs_ok),
        'po_map_ok': bool(po_map_ok),
        'annotations': int(annotations_count or 0),
    }
