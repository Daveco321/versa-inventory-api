"""
amazon_skus.py: Versa's Amazon SKU rules in one place, for the Amazon agent.

Everything here is a pure function of the SKU text (no feeds, no network), so the
planner, the pick ticket and the daily email all read a SKU the same way. The rules
are David's (Jun to Oct 2026) and match the AM FBA ship tool exactly:

    AM prefix         the first two letters are the customer/channel; AM = stock reserved
                      for Amazon. Other prefixes (RO, BU, TJ...) belong to store accounts
                      and are never Amazon supply, even on the same style.
    base vs size      BASE (AMNASU207SLS) = 36-pc prepack cartons in the warehouse;
                      BASE-SIZE (AMNASU207SLS-M) = single-size cartons of 36.
    -FBA / -FBM       Amazon listing markers, stripped to find the style and size.
    neck/sleeve       the warehouse sizes dress shirts by neck and sleeve
                      (AMNASU207SLS-1515.53233 = 15-15.5 neck / 32-33 sleeve); Amazon lists
                      the same goods by letter (M). Two ledger typos mean the same size:
                      1515.3435 (half missing) and 1515.553435 (half doubled) = 15-15.5/34-35.
    tall sleeves      34-35 and longer = the tall size (MT, LT, XLT); there is no XXLT
                      (stays XXL), and SLIM has no talls at the top: slim 17-17.5/34-35 is XL.
    -V                a leftover partial carton (under 36): never Amazon supply.

    norm_sku(s)              'AMUSOX275SLB - xl' -> 'AMUSOX275SLB-XL'
    split_sku(s)             -> SkuParts(base, size, marker) ; marker 'FBA' | 'FBM' | ''
    fit_code(base)           'AMNASU207SLS' -> 'SL'
    parse_neck(code)         '1515.553435' -> NeckSize(neck='1515.5', sleeve='3435', label)
    neck_to_alpha(code, fit) '1717.53435', 'SL' -> 'XL' ; same code with 'RF' -> 'XLT'
    alpha_size(size, fit)    letter sizes pass through, neck codes convert (None if unsure)
    is_amazon_stock(sku)     AM prefix and not a -V partial carton
    size_rank(size)          sort key: S, M, MT, L, LT, XL, XLT, XXL ...
"""
import re
from collections import namedtuple

CARTON_UNITS = 36                      # every Amazon carton is 36 pieces (prepack or single size)
AMAZON_PREFIX = 'AM'

SIZE_TOKENS = ('XS', 'S', 'M', 'L', 'XL', 'XXL', 'XXXL', '2XL', '3XL', '4XL', '5XL',
               'ST', 'MT', 'LT', 'XLT', 'XXLT', '2XLT', '3XLT', '1X', '2X', '3X')
# Alpha sizes (...-XL) or dress-shirt neck/sleeve codes (...-1616.53435).
_SIZE_RE = re.compile(r'-(?:(' + '|'.join(SIZE_TOKENS) + r')|(\d{4}(?:\.\d{1,6})?))$', re.I)
_MARKER_RE = re.compile(r'-(FBA|FBM)$', re.I)
_DASH_RE = re.compile(r'\s*-\s*')
# [CUSTOMER 2][BRAND 2][FABRIC 2][SERIAL 3][FIT 2][COLLAR 1]; fit = the letters after the digits
_FIT_RE = re.compile(r'(\d+)([A-Za-z]+)$')

NECK_TO_ALPHA = {'1414.5': 'S', '1515.5': 'M', '1616.5': 'L', '1717.5': 'XL', '1818.5': 'XXL', '1919.5': 'XXXL',
                 '1414': 'S', '1515': 'M', '1616': 'L', '1717': 'XL', '1818': 'XXL', '1919': 'XXXL'}
SLEEVE_IS_TALL = {'3233': False, '3435': True, '3536': True, '3637': True}

SIZE_ORDER = ('XS', 'S', 'ST', 'M', 'MT', 'L', 'LT', 'XL', 'XLT', 'XXL', '2XL', 'XXLT', '2XLT',
              'XXXL', '3XL', '3XLT', '4XL', '5XL', '1X', '2X', '3X')
_SIZE_RANK = {s: i for i, s in enumerate(SIZE_ORDER)}

SkuParts = namedtuple('SkuParts', 'base size marker')
NeckSize = namedtuple('NeckSize', 'neck sleeve label')


def norm_sku(sku):
    """Upper case, trimmed, with the spaces around dashes removed ('X - L' -> 'X-L')."""
    return _DASH_RE.sub('-', str(sku or '').strip()).upper()


def split_sku(sku):
    """(base, size, marker). marker is 'FBA', 'FBM' or ''. size is '' for a base (prepack) SKU,
    a letter size ('MT') or a neck/sleeve code ('1515.53233') exactly as written."""
    s = norm_sku(sku)
    marker = ''
    m = _MARKER_RE.search(s)
    if m:
        marker = m.group(1).upper()
        s = s[:m.start()]
    m = _SIZE_RE.search(s)
    if not m:
        return SkuParts(s, '', marker)
    return SkuParts(s[:m.start()], (m.group(1) or m.group(2)).upper(), marker)


def base_of(sku):
    return split_sku(sku).base


def fit_code(base):
    """The two fit letters after the serial: AMNASU207SLS -> 'SL', AMUSCD179RFB -> 'RF'."""
    m = _FIT_RE.search(str(base or ''))
    return m.group(2)[:2].upper() if m else ''


def is_partial_carton(sku):
    """A '-V' row: a leftover partial carton (fewer than 36 pieces)."""
    return norm_sku(sku).endswith('-V')


def is_amazon_stock(sku):
    """Stock that may go to Amazon: AM prefix, and not a -V partial carton."""
    s = norm_sku(sku)
    return s.startswith(AMAZON_PREFIX) and not is_partial_carton(s)


def is_neck_size(size):
    return bool(re.match(r'^\d', str(size or '')))


def parse_neck(code):
    """'1616.53435' -> NeckSize('1616.5', '3435', '16-16.5 / 34-35'). Handles the two ledger
    typos ('1515.553435' half doubled, '1515.3435' half missing). None when not a neck code."""
    s = str(code or '').strip()
    m = re.match(r'^(\d{2})(\d{2})\.55(\d{2})(\d{2})$', s) or re.match(r'^(\d{2})(\d{2})\.5(\d{2})(\d{2})$', s)
    if m:
        return NeckSize(f'{m.group(1)}{m.group(2)}.5', f'{m.group(3)}{m.group(4)}',
                        f'{m.group(1)}-{m.group(2)}.5 / {m.group(3)}-{m.group(4)}')
    m = re.match(r'^(\d{2})(\d{2})\.?(\d{2})(\d{2})$', s)
    if m:
        return NeckSize(f'{m.group(1)}{m.group(2)}', f'{m.group(3)}{m.group(4)}',
                        f'{m.group(1)}-{m.group(2)} / {m.group(3)}-{m.group(4)}')
    return None


def neck_to_alpha(code, fit=''):
    """Amazon letter size for a warehouse neck/sleeve code, or None when it cannot be
    mapped with confidence. fit matters only at the top of the slim range."""
    p = parse_neck(code)
    if p is None:
        return None
    letter = NECK_TO_ALPHA.get(p.neck)
    if letter is None:
        return None
    tall = SLEEVE_IS_TALL.get(p.sleeve)
    if tall is None:
        return None
    if not tall:
        return letter
    size = 'XXL' if letter == 'XXL' else letter + 'T'      # no XXLT in the Amazon size set
    if str(fit or '').upper() == 'SL' and size == 'XLT':     # slim has no talls at the top
        return 'XL'
    return size


def alpha_size(size, fit=''):
    """Letter sizes pass through; neck/sleeve codes convert; None when a code cannot map."""
    s = str(size or '').strip().upper()
    if not s:
        return ''
    if is_neck_size(s):
        return neck_to_alpha(s, fit)
    return s


def size_rank(size):
    """Sort key putting sizes in shelf order (S, M, MT, L, LT, XL, XLT, XXL); unknowns last."""
    s = str(size or '').upper()
    return (_SIZE_RANK.get(s, len(SIZE_ORDER)), s)


def alpha_index(warehouse_skus):
    """{(base, letter size): [exact warehouse SKUs]} for sized warehouse rows. The exact SKU
    is kept because a pick ticket must name the row Transcal actually has (typo forms included)."""
    out = {}
    for sku in warehouse_skus or ():
        parts = split_sku(sku)
        if not parts.size:
            continue
        letter = alpha_size(parts.size, fit_code(parts.base))
        if letter:
            out.setdefault((parts.base, letter), []).append(norm_sku(sku))
    return out
