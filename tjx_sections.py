"""Row order for TJX-logo catalog exports (David, Oct 15 2026).

Inside every brand the rows run colour family by colour family, each under a section band:
  White Solids, Black Solids, Navy & Blue Solids,
  Other Solids split by the nearest colour (Grey, Khaki & Brown, Green, Pink, Purple, Red,
  Orange, Yellow, then anything else),
  Fancies split into Checks, Stripes, Geos and Prints (the brand page's pattern words:
  index.html classifyColorPatterns; a pattern that fits several takes the first of
  check, stripe, geo; everything else is a print).
Inside a section the rows are grouped by fabrication (the style number's fabric code, so
two spellings of one fabric stay one group), the biggest fabrication first; inside a
fabrication the biggest style first. A style is placed ONCE, from its base row (else its
biggest row), so its -V partial cartons and every delivery stay right under it even when
their colour or fabrication text is spelled differently.
Pure: no app import, so it can be unit tested on synthetic rows."""
import re
from datetime import datetime

FAMILY_ORDER = ('White', 'Black', 'Navy Solid', 'Other Solid', 'Fancy')
FAMILY_LABELS = {'White': 'White Solids', 'Black': 'Black Solids', 'Navy Solid': 'Navy & Blue Solids',
                 'Other Solid': 'Other Solids', 'Fancy': 'Fancies'}

# Other solids: the first group whose words appear in the colour name
OTHER_SOLID_GROUPS = (
    ('Grey', r'gr[ae]y|silver|charcoal|slate|smoke|pewter|graphite|ash|steel|stone grey'),
    ('Khaki & Brown', r'khaki|tan|brown|camel|taupe|stone|sand|beige|buttermilk|oatmeal|mocha|coffee|bamboo|'
                      r'chocolate|cognac|caramel|tobacco|bark|mushroom|natural|ecru|almond|wheat|toffee|'
                      r'cream|ivory|bone|putty|chino|olive drab|peanut'),
    ('Green', r'green|olive|sage|mint|forest|hunter|emerald|lime|moss|army|jade|pistachio|kelly|'
              r'seafoam|spruce'),
    ('Pink', r'pink|rose|blush|salmon|coral|fuchsia|magenta|peach|flamingo'),
    ('Purple', r'purple|lavender|lavander|lilac|violet|plum|eggplant|orchid|mauve|grape|amethyst|wisteria|'
               r'iris|aubergine'),
    ('Red', r'red|burgundy|wine|maroon|crimson|cranberry|berry|merlot|brick|cardinal|raspberry|'
            r'oxblood|garnet|scarlet|cherry'),
    ('Orange', r'orange|tangerine|apricot|copper|amber|pumpkin|rust|terracotta|melon'),
    ('Yellow', r'yellow|butter|gold|mustard|lemon|canary|sunflower|maize|banana'),
)
_OTHER_RES = [(name, re.compile(r'\b(?:' + words + r')\b', re.I)) for name, words in OTHER_SOLID_GROUPS]
OTHER_SOLID_LAST = 'More Colors'

# Fancies: index.html classifyColorPatterns, plus the words the catalog's colour names use
# for checks (grid, graph, checker, 'Pld') and stripes (lines) that the brand page lacks
_STRIPE = re.compile(r'stripe|\blines\b', re.I)
_GEO = re.compile(r'\bgeo\b|\bgeometric\b|\btriangles?\b|\bdots?\b|\bdotted\b|\bpolka\b|\bpin\s*dots?\b|'
                  r'\bpindots?\b|\bcircles?\b|\bdiamonds?\b|\bsquares?\b|\bhexagons?\b|\boctagons?\b|'
                  r'\bovals?\b|\bchevrons?\b|\bmedallions?\b|\bfoulard\b|\bpaisley\b|\bneat\b|\bstars?\b', re.I)
_CHECK = re.compile(r'\bchecks?\b|\bchecked\b|\bcheckered\b|\bplaids?\b|\bwindowpanes?\b|\bgingham\b|'
                    r'\btattersall\b|\btartan\b|\bhoundstooth\b|\bmadras\b|\bgrids?\b|\bgraph\b|'
                    r'\bcheckers?\b|\bcheckerboard\b|\bpld\b', re.I)
FANCY_ORDER = ('Checks', 'Stripes', 'Geos', 'Prints')


def family_of(item):
    """The row's Color Family; an empty one counts as a fancy (classifyColor(''))."""
    f = str(item.get('tjx_color_family') or '').strip()
    return f if f in FAMILY_ORDER else 'Fancy'


def other_solid_group(color):
    c = str(color or '')
    for name, rx in _OTHER_RES:
        if rx.search(c):
            return name
    return OTHER_SOLID_LAST


def fancy_group(color):
    c = str(color or '')
    if _CHECK.search(c):
        return 'Checks'
    if _STRIPE.search(c):
        return 'Stripes'
    if _GEO.search(c):
        return 'Geos'
    return 'Prints'


def section_of(item):
    """(sort key, band label) for one export row."""
    fam = family_of(item)
    fi = FAMILY_ORDER.index(fam)
    if fam == 'Other Solid':
        g = other_solid_group(item.get('color'))
        gi = [n for n, _ in OTHER_SOLID_GROUPS].index(g) if g != OTHER_SOLID_LAST else len(OTHER_SOLID_GROUPS)
        return (fi, gi), f'{FAMILY_LABELS[fam]} · {g}'
    if fam == 'Fancy':
        g = fancy_group(item.get('color'))
        return (fi, FANCY_ORDER.index(g)), f'{FAMILY_LABELS[fam]} · {g}'
    return (fi, 0), FAMILY_LABELS[fam]


_TOKEN = re.compile(r'[a-z0-9.%]+')


def fabric_key(fabrication):
    """Near-identical fabrication spellings fold together: case, punctuation, word order and
    the words 'and'/'with' never split a group ('95% Polyester / 5% Spandex' ==
    '95% polyester, 5% spandex')."""
    toks = [t.strip('.') for t in _TOKEN.findall(str(fabrication or '').lower())]
    toks = [t for t in toks if t and t not in ('and', 'with', 'w')]
    # a percentage belongs to the fibre after it: 65% Polyester / 35% Cotton is not
    # 65% Cotton / 35% Polyester
    out, i = [], 0
    while i < len(toks):
        if toks[i].endswith('%') and i + 1 < len(toks):
            out.append(toks[i] + toks[i + 1])
            i += 2
        else:
            out.append(toks[i])
            i += 1
    return ' '.join(sorted(set(out)))


_MODERN = re.compile(r'^[A-Z0-9]{2}[A-Z]{4}[A-Z0-9]{3}[A-Z]{2,4}$')


def fabric_group(item):
    """The fabrication group: the style number's fabric code (chars 5-6) on modern style
    numbers, else the folded fabrication text."""
    base = style_of(item.get('sku')).upper()
    if _MODERN.match(base):
        return 'code:' + base[4:6]
    return 'text:' + fabric_key(item.get('fabrication'))


def style_of(sku):
    s = str(sku or '')
    return s[:-2] if s.endswith('-V') else s


def _num(v):
    return v if isinstance(v, (int, float)) and not isinstance(v, bool) else 0


def _arrival(item):
    for fmt in ('%b %d, %Y', '%Y-%m-%d'):
        try:
            return datetime.strptime(str(item.get('arrival') or '').strip(), fmt)
        except ValueError:
            pass
    return datetime.max


def order(items, brand_key, ats_field='total_ats'):
    """-> [(row index, brand key, section label), ...] in print order. Brands keep their
    first-appearance order; everything inside a brand follows the module rules."""
    first_brand, rep_row = {}, {}
    for i, it in enumerate(items):
        b = brand_key(it)
        first_brand.setdefault(b, i)
        st = (b, style_of(it.get('sku')))
        # the style's base row decides its place; with no base row, its biggest row
        is_base = not str(it.get('sku') or '').endswith('-V')
        cur = rep_row.get(st)
        if cur is None:
            rep_row[st] = i
        else:
            c_base = not str(items[cur].get('sku') or '').endswith('-V')
            if (is_base, _num(it.get(ats_field))) > (c_base, _num(items[cur].get(ats_field))):
                rep_row[st] = i
    place = {st: (section_of(items[i]), fabric_group(items[i])) for st, i in rep_row.items()}
    fab_tot, sty_tot = {}, {}
    for i, it in enumerate(items):
        b = brand_key(it)
        st = (b, style_of(it.get('sku')))
        (sk, _), fg = place[st]
        a = _num(it.get(ats_field))
        fab_tot[(b, sk, fg)] = fab_tot.get((b, sk, fg), 0) + a
        sty_tot[st] = sty_tot.get(st, 0) + a

    def key(i):
        it = items[i]
        b = brand_key(it)
        st = (b, style_of(it.get('sku')))
        (sk, _), fg = place[st]
        return (first_brand[b], sk, -fab_tot[(b, sk, fg)], fg, -sty_tot[st], st[1],
                str(it.get('sku') or '').endswith('-V'), _arrival(it), i)

    out = []
    for i in sorted(range(len(items)), key=key):
        st = (brand_key(items[i]), style_of(items[i].get('sku')))
        out.append((i, st[0], place[st][0][1]))
    return out
