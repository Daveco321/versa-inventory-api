"""Row decoration for the TJX catalog export, ported from the browser.

The TJX catalog page (customer_logo TJ or TM) decorates every exported row in
index.html: brand key and full name (rebuildAppData), Color, Fit, Fabrication,
Color Family and New Fabric (_tjxColorFamily / _tjxNewFabric), and the prepack
fields (_export_category, _export_fit, _export_customer, _override_size_pack).
This module is an exact port of those browser functions, so a server-built
workbook carries the same values the page would export.

The browser is the source of truth. Older ports in app.py (_apo_style_color,
_apo_classify_color, _apo_fabrication, _apo_fit_label ...) differ from it in
places, so nothing here is shared with them. Standard library only, and it
never imports app.py, so app.py can import it and the tests can run it alone.

JavaScript details that are reproduced on purpose:
  * truthiness: an override value of {} is truthy in JS (it blocks the prefix
    keys in getStyleOverride and wins in getStyleColorInfo), 0 and '' are not;
  * regexes: \\b, \\d and [A-Za-z] are ASCII only, \\s and trim() use the JS
    whitespace set (NBSP, U+FEFF, U+2028 ...), '.' stops at \\n \\r U+2028 U+2029;
  * a thrown error inside _tjxColorFamily / _tjxNewFabric yields '' for that
    cell, and getMatchingBanners evaluates every rule before the 'New Fabric'
    test, so one malformed rule blanks the column for every row.
"""
import re
import unicodedata
from functools import reduce

# ── JavaScript string semantics ─────────────────────────────────────────────

# JS WhiteSpace + LineTerminator: what \s matches and what trim() strips.
_JS_WS_CHARS = ('\t\n\x0b\x0c\r \u00a0\u1680\u2000\u2001\u2002\u2003\u2004\u2005\u2006'
                '\u2007\u2008\u2009\u200a\u2028\u2029\u202f\u205f\u3000\ufeff')
_S = '[\\t\\n\\x0b\\x0c\\r \\u00a0\\u1680\\u2000-\\u200a\\u2028\\u2029\\u202f\\u205f\\u3000\\ufeff]'
_DOT = '[^\\n\\r\\u2028\\u2029]'   # JS '.' without the s flag
_A = re.ASCII                       # JS \b and \w are ASCII only (no u flag)


def _js_trim(s):
    return s.strip(_JS_WS_CHARS)


def _truthy(v):
    """JavaScript truthiness: None/False/0/NaN/'' are falsy, every object
    (including an empty dict or list) is truthy."""
    if v is None or v is False:
        return False
    if isinstance(v, (int, float)):
        return v == v and v != 0
    if isinstance(v, str):
        return v != ''
    return True


def _js_or(*vals):
    """a || b || c: the first truthy value, else the last one."""
    for v in vals[:-1]:
        if _truthy(v):
            return v
    return vals[-1]


def _js_get(obj, key):
    """obj.key: None (null/undefined) throws like JS, primitives and arrays
    have no such property."""
    if obj is None:
        raise TypeError(f"Cannot read properties of null (reading '{key}')")
    if isinstance(obj, dict):
        return obj.get(key)
    return None


def _js_str_method(v):
    """A value about to have a string method called on it (toUpperCase, trim)."""
    if not isinstance(v, str):
        raise TypeError('string method called on a non-string')
    return v


def _js_num_str(v):
    if isinstance(v, bool):
        return 'true' if v else 'false'
    if isinstance(v, float):
        if v != v:
            return 'NaN'
        if v.is_integer() and abs(v) < 1e21:
            return str(int(v))
        return repr(v)
    return str(v)


def _js_string(v):
    """String(v) for the value types JSON can carry."""
    if v is None:
        return 'null'
    if isinstance(v, str):
        return v
    if isinstance(v, (bool, int, float)):
        return _js_num_str(v)
    if isinstance(v, list):
        return ','.join('' if x is None else _js_string(x) for x in v)
    if isinstance(v, dict):
        return '[object Object]'
    return str(v)


def _js_len(v):
    """v.length: arrays and strings only (undefined otherwise)."""
    if isinstance(v, (list, str)):
        return len(v)
    return None


def _js_some(v, fn):
    if isinstance(v, list):
        return any(fn(x) for x in v)
    raise TypeError('.some is not a function')


# ── Brand tables (index.html BRAND_IMAGE_PREFIX / BRAND_MAPPING / BRAND_ORDER) ──

BRAND_IMAGE_PREFIX = {
    'NAUTICA': 'NA', 'DKNY': 'DK', 'EB': 'EB', 'REEBOK': 'RB', 'VINCE': 'VC', 'BEN': 'BE',
    'USPA': 'US', 'CHAPS': 'CH', 'LUCKY': 'LB', 'JNY': 'JN', 'BEENE': 'GB', 'NICOLE': 'NM',
    'SHAQ': 'SH', 'TAYION': 'TA', 'STRAHAN': 'MS', 'VD': 'VD', 'VERSA': 'VR',
    'CHEROKEE': 'CK', 'AMERICA': 'AC', 'BLO': 'BL', 'BLACK': 'BL', 'DN': 'D9', 'KL': 'KL',
    'RG': 'RG', 'NE': 'NE', 'DH': 'DH', 'NW': 'NW', 'HC': 'HC', 'MP': 'MP', 'ZY': 'ZY',
    'CE': 'CE', 'CL': 'CL',
}

# BRAND_MAPPING[key].full_name (logos are not needed here).
BRAND_FULL_NAMES = {
    'NAUTICA': 'Nautica', 'DKNY': 'DKNY', 'EB': 'Eddie Bauer', 'REEBOK': 'Reebok',
    'VINCE': 'Vince Camuto', 'BEN': 'Ben Sherman', 'USPA': 'U.S. Polo Assn.',
    'CHAPS': 'Chaps', 'LUCKY': 'Lucky Brand', 'JNY': 'Jones New York',
    'BEENE': 'Geoffrey Beene', 'NICOLE': 'Nicole Miller', 'SHAQ': "Shaquille O'Neal",
    'TAYION': 'Tayion', 'STRAHAN': 'Michael Strahan', 'VD': 'Von Dutch', 'VERSA': 'Versa',
    'CHEROKEE': 'Cherokee', 'AMERICA': 'American Crew', 'BLO': 'Bloomingdales Private Label',
    'BLACK': 'Black Label', 'DN': 'Divine 9', 'KL': 'Karl Lagerfeld Paris',
    'NE': 'Neiman Marcus', 'RG': 'Robert Graham', 'DH': 'Daniel Hechter',
    'NW': 'Nine West', 'HC': 'Henri Christian', 'MP': 'Modern People', 'ZY': 'Zylos',
    'CE': 'Chuck English', 'CL': 'Christian Lacroix',
}

# Reverse lookup 2-char SKU brand code -> brand key. Built in the browser's
# order, so when two brands share a prefix the later one wins (BL -> BLACK).
SKU_BRAND_CODE_MAP = {}
for _bk, _px in BRAND_IMAGE_PREFIX.items():
    SKU_BRAND_CODE_MAP[_px] = _bk
SKU_BRAND_CODE_MAP['DN'] = 'DN'
SKU_BRAND_CODE_MAP['VS'] = 'VERSA'
SKU_BRAND_CODE_MAP['CS'] = 'CHAPS'
SKU_BRAND_CODE_MAP['NT'] = 'NAUTICA'   # Nautica overflow serials past 999

BRAND_ALIASES = {'NM': 'NICOLE'}       # ATS tab names that differ from the brand key

BRAND_ORDER = [
    'NAUTICA', 'DKNY', 'EB', 'VINCE',
    'KL', 'CHAPS', 'USPA', 'LUCKY',
    'BEN', 'BEENE', 'NE', 'JNY',
    'NICOLE', 'VD', 'REEBOK', 'SHAQ',
    'TAYION', 'STRAHAN', 'VERSA', 'AMERICA',
    'BLO', 'DN',
]

# ── Fit, collar and category code sets ──────────────────────────────────────

SHIRT_FIT_CODES = {
    'SL': 'Slim Fit Long Sleeve', 'RF': 'Regular Fit Long Sleeve',
    'BT': 'Big & Tall Long Sleeve', 'WB': 'Big & Tall (Von Dutch)',
    'BB': 'Big Long Sleeve', 'TT': 'Tall Long Sleeve', 'TF': 'Tailored Fit Long Sleeve',
    'MF': 'Modern Fit Long Sleeve', 'SS': 'Slim Fit Short Sleeve',
    'SR': 'Regular Fit Short Sleeve', 'SB': 'Short Sleeve Big', 'ST': 'Short Sleeve Tall',
    'TB': 'Short Sleeve Big & Tall', 'BR': 'Single Breasted Blazer',
    'DB': 'Double Breasted Blazer',
}

PANTS_FIT_CODES = {
    'SE': 'Slim Fit / Extended Button', 'SH': 'Slim Fit / Hook & Eye Closure',
    'SR': 'Slim Fit / Reg Button', 'CE': 'Classic Fit / Extended Button',
    'CH': 'Classic Fit / Hook & Eye Closure', 'CR': 'Classic Fit / Reg Button',
    'SF': 'Straight Fit / Reg Button', 'SC': 'Straight Fit / Hook & Eye Closure',
    'RR': 'Relaxed Fit / Reg Button',
    'TE': 'Big & Tall Fit / Extended Button', 'TH': 'Big & Tall Fit / Hook & Eye Closure',
    'TR': 'Big & Tall Fit / Reg Button',
    'BE': 'Bermuda Shorts Slim Fit / Extended Button',
    'BH': 'Bermuda Shorts Slim Fit / Hook & Eye Closure',
    'BA': 'Bermuda Shorts Slim Fit / Reg Button',
    'BD': 'Bermuda Shorts Straight Fit / Extended Button',
    'BC': 'Bermuda Shorts Straight Fit / Hook & Eye Closure',
    'BF': 'Bermuda Shorts Straight Fit / Reg Button',
    'BJ': 'Bermuda Shorts Big & Tall Fit / Extended Button',
    'BK': 'Bermuda Shorts Big & Tall Fit / Hook & Eye Closure',
    'BM': 'Bermuda Shorts Big & Tall Fit / Reg Button',
}

# Validation union (only membership is used here).
FIT_CODES = dict(PANTS_FIT_CODES)
FIT_CODES.update(SHIRT_FIT_CODES)

# fitCodeToLabel's switch (the short labels; anything else reads 'Slim Fit').
_FIT_SHORT_LABELS = {
    'SL': 'Slim Fit', 'RF': 'Regular Fit', 'TF': 'Tailored Fit', 'MF': 'Modern Fit',
    'BT': 'Big & Tall', 'WB': 'Big & Tall (Von Dutch)', 'BB': 'Big Fit', 'TT': 'Tall Fit',
    'SB': 'Short Sleeve Big', 'ST': 'Short Sleeve Tall', 'TB': 'Short Sleeve Big & Tall',
    'CF': 'Classic Fit', 'AF': 'Athletic Fit', 'SS': 'Slim Fit Short Sleeve',
    'SR': 'Regular Fit Short Sleeve', 'SE': 'Slim Fit Extended Button',
    'SH': 'Slim Fit Hook & Eye', 'CE': 'Classic Fit Extended Button',
    'CH': 'Classic Fit Hook & Eye', 'CR': 'Classic Fit Reg Button', 'SF': 'Straight Fit',
    'SC': 'Straight Fit Hook & Eye', 'RR': 'Relaxed Fit', 'BR': 'Single Breasted',
    'DB': 'Double Breasted',
}

SHORT_SLEEVE_CODES = frozenset(['SS', 'SR', 'SB', 'ST', 'TB'])
LONG_SLEEVE_FIT_CODES = frozenset(['SL', 'RF', 'TF', 'MF', 'BT', 'BB', 'TT', 'WB', 'BR', 'DB'])
YOUNG_MEN_FABRIC_CODES = frozenset(['KN', 'WT', 'SD', 'SF', 'SB', 'SL', 'SN', 'SV', 'SJ', 'SH',
                                    'SG', 'SS', 'BC', 'BR', 'BH', 'BA', 'CO', 'TH', 'PO', 'PW',
                                    'PJ', 'PH', 'PL', 'HE', 'RB'])
SPORTSWEAR_BOTTOM_CODES = frozenset(['BC', 'BR', 'BH', 'BA'])
BT_FIT_CODES = frozenset(['BT', 'BB', 'TT', 'SB', 'ST', 'TB', 'WB',
                          'TE', 'TH', 'TR', 'BJ', 'BK', 'BM'])
SPORTSWEAR_COLLARS = frozenset(['Z', 'U', 'M', 'N', 'O', 'R'])
SPORTSWEAR_FABRICS = frozenset(['PH', 'PJ', 'PL', 'PO', 'PW', 'TH', 'HE', 'RB'])
BUTTON_DOWN_COLLARS = frozenset(['B', 'D', 'H', 'L', 'W', 'X'])

# ── Fabric descriptions (index.html FABRIC_RULES, verbatim) ─────────────────

FABRIC_RULES = {
    "AW": "4 Way Stretch", "CA": "Catatonic 95% Polyester / 5% Spandex",
    "TD": "CVC Dobby 60% Polyester / 40% Cotton", "CH": "Chambray TC Stretch",
    "CS": "Cooling Stretch", "CV": "Cotton / Poly CVC",
    "DS": "4 Way Stretch Dobby 95% Polyester / 5% Spandex",
    "OX": "PINPOINT Oxford 65%/35% Poly/Cotton", "PP": "100% Polyester- 150D",
    "SA": "150D - Sateen 100% POLYESTER", "LN": "100% Slab Linen", "ST": "97% Cotton 3% spandex",
    "SW": "97% Cotton 3% Stretch Twill", "SU": "Stretch Supershirt (95% Polyester, 5% Spandex)",
    "TR": "Traverler Stretch", "TW": "4 way stretch twill",
    "TS": "TC Stretch (77% POLYESTER /20% COTTON /3%SPANDEX)",
    "WS": "4 Way Stretch (95%,5%) Sateen", "PC": "TC Poplin 65%/35% Poly/Cotton",
    "PT": "97% Poly 3% Stretch - 150D STRETCH", "VS": "Viscose (31%) Stretch",
    "VP": "50% Viscose 50%Polyester", "LP": "Linen Polyester/Spandex",
    "MR": "50% microfiber 50% rayon", "CT": "100% Cotton", "CP": "98% Cotton / 2% Spandex",
    "BP": "50% BAMBOO / 50% POLYESTER", "TC": "TC Stretch (52P,45C,3S %)",
    "SC": "60% Cotton, 38% Poly, 2% Spand",
    "BM": "30% Rayon made from Bamboo / 30% Microfiber / 36% Poly / 4% Spandex Twill",
    "VM": "62% Poly 35% Viscose made from Bamboo 3% Spendex",
    "SP": "52% Poly 45% COTTON 3% Spand CVC YARN DYE",
    "TP": "Solid Twill 21%Rayon/75.5%Poly/3.5%Spandex", "LC": "Linen 51% Cotton / 49% Poly",
    "CX": "97% Cotton / 3% Polyster", "WF": "96% Poly 4% Spandex waffle",
    "FT": "97% POLY/ 3% SPANDEX - FLAX TEXTURE",
    "CE": "88% Polyester/ 7% Cellulose/ 5% spandex - Tech", "PK": "100% Polyester - knit",
    "PD": "60% Cotton/ 40% polyester - Dobby",
    "PY": "50% cotton / 47% polyester/ 3% spandex - CVC OXFORD",
    "UP": "95% poly / 5%spandex ---Perforated", "NY": "78% Nylon / 22% Spandex - 165GSM",
    "CL": "35% Lyocell/35%Cotton/27% Nylon/3%Spandex", "PM": "50% Polyester / 50% Microfiber",
    "PX": "95% Polyester / 5% Spandex - Core", "CN": "71% Cotton / 27% Nylon / 2% Spandex",
    "MP": "74% Modal / 26% Polyester", "LE": "100% Linen",
    "PE": "96% POLYESTER / 4% SPANDEX - END ON END", "OC": "100% Cotton - OXFORD",
    "CD": "65% Polyester / 35% Cotton - Dobby", "CY": "100% Cotton - Yarn Dye",
    "CW": "100% Cotton - Twill", "CJ": "100% Cotton - Jacquard", "LT": "45% Cotton / 55% Linen",
    "DP": "95% POLYESTER / 5% SPANDEX - KNIT PERFORMANCE",
    "PR": "87% Polyamide / 13% Elastic - 149GSM - Rhone",
    "PS": "94% Polyester / 6% Spandex - 210GSM Knit", "CG": "100% Cotton - Poplin 105gsm",
    "PA": "88% Polyester / 12% Spandex 160GSM - Seamless Lux Knit",
    "PN": "88% Polyester / 12% Spandex 160GSM - Non-Seamless", "CF": "100% Cotton 50s 2 ply",
    "CB": "98% Cotton / 2% Spandex (Bloomingdale)", "KN": "KNITS", "WT": "WOVEN TOPS",
    "SD": "SWEATERS", "SF": "Flannel (Shacket)", "SB": "Trucker (Shacket)",
    "CO": "Corduroy (Overshirt)", "SL": "Twill (Shacket)",
    "SG": "CVC Twill 70% Cotton / 30% Polyester (Shacket)",
    "SN": "Denim Twill 75% Cotton / 15% Rayon / 10% Polyester (Shacket)",
    "SV": "Canvas 245GSM 97% Cotton / 3% Spandex (Shacket)",
    "SJ": "Jacquard Woven 210GSM 100% Cotton (Shacket)",
    "SH": "80% Cotton / 20% Polyester 325GSM (Shacket)",
    "SS": "TC 77% Polyester / 20% Cotton / 3% Spandex (Shacket)",
    "NS": "Tech Seersucker 53% Polyester / 40% Nylon / 7% Spandex",
    "ET": "Tech Seersucker 96% Polyester / 4% Elastane", "KP": "100% Polyester - Pique Knit",
    "LB": "60% Linen / 40% Cotton", "RB": "Rugby", "RP": "80% Polyester / 20% Rayon",
    "RS": "88% Polyester / 10% Rayon / 2% Spandex - 230GSM",
    "YD": "65% Polyester / 35% Cotton Yarn Dye", "KS": "Knit Sport Coat",
    "LA": "8% Lyocell / 88% Polyester / 4% Spandex 120GSM",
    "NP": "78% Nylon / 22% Spandex - 180GSM Premium Nylon",
    "PB": "100% Polyester - Imitation Cotton 130GSM",
    "PF": "92% Polyester / 8% Spandex 150GSM - Dobby",
    "PG": "73% Polyester / 5% Spandex / 22% Recycled Fiber 130GSM - Chambray End-on-End",
    "PH": "100% Polyester Polo - Mesh Sweater Knit", "PO": "100% Polyester Polo - Pique",
    "PJ": "100% Polyester Polo - Jersey", "PL": "100% Polyester Polo - Sweater Knit",
    "PU": "92% Polyester / 8% Spandex 150GSM - Lux Twisted Dobby",
    "PV": "94% Polyester / 6% Spandex - 210GSM", "PW": "100% Polyester Polo - Waffle",
    "PZ": "94% Polyester / 6% Spandex - 210GSM", "SE": "88% Polyester / 12% Spandex - 180GSM",
    "TB": "Polyester Rayon Blend", "WB": "Wool Polyester Rayon Blend", "TH": "TSHIRT",
    "HE": "HENLEY", "BC": "CARPENTERS (Bottoms)", "BR": "RIPSTOPS (Bottoms)",
    "BH": "HEAVY WEIGHT (Bottoms)", "BA": "PINSTRIPE (Bottoms)",
    "CK": "100% Cotton 100s 2 Ply (Kirkland)", "CM": "100% Cotton - Dobby for KLP",
    "CQ": "97% Cotton / 3% Spandex - Sateen 135GSM", "CR": "100% Cotton - Sateen 112GSM",
    "CU": "100% Cotton - Knit", "PQ": "65% Polyester / 35% Cotton - TC Twill",
    "CZ": "81% Cotton / 19% Polyester - 245GSM", "CC": "76% Cotton / 22% Nylon / 2% Spandex",
    "SK": "CVC Seersucker 130GSM 52% Cotton / 48% Polyester",
    "SM": "96% Polyester / 4% Spandex (Sport Jacket)"
}

# ── Banner rules (BANNER_RULES_SEED + _ensureBuiltinBanners) ────────────────

BANNER_RULES_SEED = [
    {'id': 'seed-ss', 'text': 'SHORT SLEEVE', 'bgColor': 'rgba(14,165,233,0.9)', 'textColor': '#fff',
     'position': 'bottom-left', 'visibility': 'both', 'category': 'short_sleeve',
     'fits': [], 'customers': [], 'brands': [], 'skus': []},
    {'id': 'seed-bt', 'text': 'BIG & TALL', 'bgColor': 'rgba(124,58,237,0.9)', 'textColor': '#fff',
     'position': 'bottom-left', 'visibility': 'both', 'category': 'big_tall',
     'fits': [], 'customers': [], 'brands': [], 'skus': []},
    {'id': 'seed-pants', 'text': 'PANTS', 'bgColor': 'rgba(107,114,128,0.9)', 'textColor': '#fff',
     'position': 'bottom-right', 'visibility': 'both', 'category': 'pants',
     'fits': [], 'customers': [], 'brands': [], 'skus': []},
    {'id': 'seed-sport', 'text': 'SPORTSWEAR', 'bgColor': 'rgba(234,88,12,0.9)', 'textColor': '#fff',
     'position': 'bottom-right', 'visibility': 'both', 'category': 'sportswear',
     'fits': [], 'customers': [], 'brands': [], 'skus': []},
    {'id': 'seed-acc-chaps', 'text': 'TIE & HANKY', 'bgColor': 'rgba(168,85,247,0.9)', 'textColor': '#fff',
     'position': 'bottom-right', 'visibility': 'both', 'category': 'accessories',
     'fits': [], 'customers': [], 'brands': ['CHAPS'], 'skus': []},
    {'id': 'seed-acc-shaq', 'text': 'TIE', 'bgColor': 'rgba(168,85,247,0.9)', 'textColor': '#fff',
     'position': 'bottom-right', 'visibility': 'both', 'category': 'accessories',
     'fits': [], 'customers': [], 'brands': ['SHAQ'], 'skus': []},
    {'id': 'seed-bd', 'text': 'BUTTON DOWN', 'bgColor': 'rgba(15,118,110,0.92)', 'textColor': '#fff',
     'position': 'bottom-right', 'visibility': 'both', 'category': 'button_down',
     'fits': [], 'customers': [], 'brands': [], 'skus': []},
]
BANNER_BUILTIN_IDS = ['seed-bd']


def merge_banner_rules(rules):
    """The list loadBannerRules leaves in bannerRules: the seeds when the
    server list is empty, then every built-in id that is missing appended."""
    if isinstance(rules, dict):
        rules = rules.get('rules')
    loaded = rules if isinstance(rules, list) else []
    merged = list(loaded) if loaded else [dict(r) for r in BANNER_RULES_SEED]
    for bid in BANNER_BUILTIN_IDS:
        if not any(_truthy(r) and _js_get(r, 'id') == bid for r in merged):
            seed = next((r for r in BANNER_RULES_SEED if r['id'] == bid), None)
            if seed:
                merged.append(dict(seed))
    return merged


def rule_fits(rule):
    """_ruleFits: `fits` array minus empty/'any', else the legacy `fit` string."""
    fits = _js_get(rule, 'fits')
    if isinstance(fits, list):
        return [f for f in fits if _truthy(f) and f != 'any']
    fit = _js_get(rule, 'fit')
    if _truthy(fit) and fit != 'any':
        return [fit]
    return []


def _nonblank(v):
    return _truthy(v) and _js_trim(_js_str_method(v)) != ''


def rule_customers(rule):
    """_ruleCustomers: `customers` array (non-blank), else the legacy `customer`
    string trimmed. Array items are kept untrimmed."""
    custs = _js_get(rule, 'customers')
    if isinstance(custs, list):
        return [c for c in custs if _nonblank(c)]
    cust = _js_get(rule, 'customer')
    if _truthy(cust) and _js_trim(_js_str_method(cust)):
        return [_js_trim(cust)]
    return []


def rule_brands(rule):
    brands = _js_get(rule, 'brands')
    return [b for b in brands if _nonblank(b)] if isinstance(brands, list) else []


def rule_fabrics(rule):
    fabs = _js_get(rule, 'fabrics')
    return [f for f in fabs if _nonblank(f)] if isinstance(fabs, list) else []


# ── Color map ───────────────────────────────────────────────────────────────

def color_map_from_records(records):
    """loadColorMapFromS3's loop over XLSX.utils.sheet_to_json rows: key =
    Key || Style_Number || Style_Num, trimmed and uppercased; the raw
    Color_Description is stored untouched when truthy. Later rows win."""
    cmap = {}
    for row in records:
        key = _js_or(row.get('Key'), row.get('Style_Number'), row.get('Style_Num'), '')
        key = _js_trim(_js_string(key)).upper()
        desc = row.get('Color_Description')
        if key and _truthy(desc):
            cmap[key] = desc
    return cmap


def color_map_from_rows(rows):
    """Same, from worksheet rows (first row = header, None = empty cell, as
    openpyxl's iter_rows(values_only=True) yields them). Header text is used
    as is and only the first column of a repeated name keeps the plain name,
    like sheet_to_json."""
    rows = iter(rows)
    try:
        header = next(rows)
    except StopIteration:
        return {}
    names = []
    seen = set()
    for h in header:
        n = None if h is None else _js_string(h)
        if n is not None and n in seen:
            n = None
        if n is not None:
            seen.add(n)
        names.append(n)
    wanted = ('Key', 'Style_Number', 'Style_Num', 'Color_Description')
    idx = {n: i for i, n in enumerate(names) if n in wanted}
    records = []
    for r in rows:
        rec = {}
        for n, i in idx.items():
            if i < len(r) and r[i] is not None:
                rec[n] = r[i]
        records.append(rec)
    return color_map_from_records(records)


def format_color_name(raw):
    """formatColorName: expand BLK/WHT/BLU/NVY/GRY/SRNTY/TRQ, capitalise every
    other ASCII word, collapse whitespace."""
    if not _truthy(raw):
        return ''
    s = _js_trim(raw)

    def _word(m):
        w = m.group(0)
        hit = _COLOR_ABBR.get(w.upper())
        if hit:
            return hit
        return w[:1].upper() + w[1:].lower()
    s = re.sub(r'\b([A-Za-z]+)\b', _word, s, flags=_A)
    s = re.sub(_S + '{2,}', ' ', s)
    return _js_trim(s)


_COLOR_ABBR = {'BLK': 'Black', 'WHT': 'White', 'BLU': 'Blue', 'NVY': 'Navy', 'GRY': 'Grey',
               'SRNTY': 'Serenity', 'TRQ': 'Turquoise'}

# ── classifyColor (the frontend version, index.html ~46133-46210) ───────────

_BLUE_FAMILY = re.compile(
    r'\bnavy\b|\bblue\b|\bindigo\b|\bserenity\b|\bperiwinkle\b|\bturq[ou]+ise\b|\baqua\b'
    r'|\bteal\b|\btanzine\b|\bcobalt\b|\bblueberry\b|\bseaspray\b|\bdeep sea\b|\bdenim\b'
    r'|\bcerulean\b|\bsapphire\b|\bazure\b|\bcyan\b', _A)
_NON_BLUE_WORDS = re.compile(
    r'\b(?:grey|gray|black|white|red|pink|green|brown|tan|khaki|olive|burgundy|wine|purple'
    r'|plum|orange|yellow|gold|silver|charcoal|ivory|cream|beige|camel|rust|coral|lilac'
    r'|lavender|mint|sage)\b', _A)
_DENIM = re.compile(r'\bdenim\b', _A)
_TRUE_BLUE = re.compile(r'\bnavy\b|\bblue\b|\bindigo\b', _A)
NAMED_SOLID_COLORS = {'tony blue': 'navy'}
_PART_SPLIT = re.compile(_S + '+/' + _S + '+')
_WS_RUN = re.compile(_S + '+')
_HAS_PRINT = re.compile(r'\bprint\b|\bprnt\b|\bgrnd\b|\bstripe\b|\bstripes\b|\bgeo\b|\bcheck\b', _A)
_DOBBY = re.compile(r'\bdobby\b', _A)
_WHITE_WORDS = re.compile(r'\bwhite\b|\bivory\b|\bcream\b', _A)
_BLACK_WORD = re.compile(r'\bblack\b', _A)
_SOLID_LEAD = re.compile('(' + _DOT + '*?)' + _S + r'*\bs(?:olid|ld)\b', _A)
_SOLID_WORD = re.compile(r'\bs(?:olid|ld)\b', _A)


def is_blue_lead(s):
    """_isBlueLead: a blue-family word, but 'denim' alone next to another
    colour word ('Denim Grey Solid') is a wash, not blue."""
    if not _BLUE_FAMILY.search(s):
        return False
    if _DENIM.search(s) and not _TRUE_BLUE.search(s) and _NON_BLUE_WORDS.search(s):
        return False
    return True


def classify_color(color_display):
    """classifyColor -> white / black / navy / other_solids / fancies."""
    if not _truthy(color_display):
        return 'fancies'
    c = _js_trim(_js_str_method(color_display)).lower()
    parts = _PART_SPLIT.split(c)
    if len(parts) > 1 and _js_trim(parts[0]):
        return classify_color(_js_trim(parts[0]))
    named = NAMED_SOLID_COLORS.get(_WS_RUN.sub(' ', c))
    if named:
        return named
    has_print = bool(_HAS_PRINT.search(c))
    if _DOBBY.search(c):
        if _WHITE_WORDS.search(c):
            return 'white'
        if _BLACK_WORD.search(c):
            return 'black'
        if is_blue_lead(c):
            return 'navy'
        return 'other_solids'
    lead = _SOLID_LEAD.match(c)
    if not has_print and lead and is_blue_lead(lead.group(1)):
        return 'navy'
    if not has_print and lead:
        lead_text = lead.group(1)
        if _WHITE_WORDS.search(lead_text):
            return 'white'
        if _BLACK_WORD.search(lead_text):
            return 'black'
        return 'other_solids'
    if not has_print and _SOLID_WORD.search(c):
        return 'other_solids'
    return 'fancies'


_COLOR_FAMILY = {'black': 'Black', 'white': 'White', 'navy': 'Navy Solid',
                 'other_solids': 'Other Solid'}

# ── SKU structure helpers (no override lookups) ─────────────────────────────

_DIGIT = re.compile('[0-9]')
_UPPER = re.compile('[A-Z]')
_P_SERIAL = re.compile('P[0-9][0-9]')
_BV_SERIAL = re.compile('[BV][0-9][0-9]')
_V_SERIAL = re.compile('V[0-9][0-9]')
_DIGIT_RUNS = re.compile('[0-9]+')


def _base_of(sku):
    """sku.split('-')[0].toUpperCase()"""
    return sku.split('-')[0].upper()


def has_pants_serial(base):
    """P##X serial at positions 6-9 marks dress pants for every brand."""
    return (len(base) >= 10 and base[6] == 'P' and bool(_DIGIT.fullmatch(base[7]))
            and bool(_DIGIT.fullmatch(base[8])) and bool(_UPPER.fullmatch(base[9])))


def is_young_men(sku):
    if not _truthy(sku):
        return False
    base = _base_of(sku)
    return len(base) >= 6 and base[4:6] in YOUNG_MEN_FABRIC_CODES


def is_sportswear(sku, brand_abbr=None):
    if not _truthy(sku):
        return False
    base = _base_of(sku)
    if not has_pants_serial(base):
        if len(base) >= 11 and base[-1] in SPORTSWEAR_COLLARS:
            return True
        if len(base) >= 6 and base[4:6] in SPORTSWEAR_FABRICS:
            return True
    return is_young_men(sku)


def is_pants(sku, brand_abbr=None):
    if not _truthy(sku):
        return False
    base = _base_of(sku)
    if has_pants_serial(base):
        return True
    return len(base) >= 6 and base[4:6] in SPORTSWEAR_BOTTOM_CODES


def is_blazer(sku):
    """Blazers and vests: serial B01-B99 / V01-V99 at positions 6-8."""
    if not _truthy(sku):
        return False
    return bool(_BV_SERIAL.fullmatch(_base_of(sku)[6:9]))


def is_vest(sku):
    if not _truthy(sku):
        return False
    return bool(_V_SERIAL.fullmatch(_base_of(sku)[6:9]))


def is_big_and_tall(sku):
    if not _truthy(sku):
        return False
    base = _base_of(sku)
    if len(base) >= 6 and base[2:4] == 'VD' and base[4:6] in ('WB', 'BT'):
        return True
    if len(base) < 11:
        return False
    return base[9:11] in BT_FIT_CODES


def extract_fit_code(sku):
    """extractFitCode: VD B&T code at 4-5, else the code before the collar,
    else the first dash part that is a fit code, else whatever sits there."""
    parts = sku.upper().split('-')
    base = parts[0]
    if len(base) >= 6 and base[2:4] == 'VD':
        vd_fit = base[4:6]
        if vd_fit in ('WB', 'BT'):
            return vd_fit
    if len(base) >= 3:
        fit = base[-3:-1]
        if fit in FIT_CODES:
            return fit
    for part in parts[1:]:
        part = _js_trim(part)
        if part in FIT_CODES:
            return part
    return base[-3:-1] if len(base) >= 3 else ''


def fit_code_to_label(code, sku=None):
    """fitCodeToLabel: pants read the full PANTS_FIT_CODES label, everything
    else the short switch label (default 'Slim Fit')."""
    if _truthy(sku) and is_pants(sku) and code in PANTS_FIT_CODES:
        return PANTS_FIT_CODES[code]
    return _FIT_SHORT_LABELS.get(code, 'Slim Fit')


_SLEEVE_WORDS = re.compile(_S + '*(Long|Short)' + _S + '*Sleeve' + _S + '*', re.ASCII | re.IGNORECASE)
_SHORT_SLEEVE_TEXT = re.compile('short' + _S + '*sleeve', re.ASCII | re.IGNORECASE)
_LONG_SLEEVE_TEXT = re.compile('long' + _S + '*sleeve', re.ASCII | re.IGNORECASE)


def sku_image_prefix(sku, brand_abbr):
    """skuImagePrefix: the SKU's own brand code when it maps to this brand
    (NT stays NT), else the brand's default prefix."""
    s = (sku or '').upper()
    for code in (s[2:4], s[0:2]):
        if len(code) == 2 and _truthy(brand_abbr) and SKU_BRAND_CODE_MAP.get(code) == brand_abbr:
            return code
    return _js_or(BRAND_IMAGE_PREFIX.get(brand_abbr), (brand_abbr or '')[:2])


def style_color_fallback_keys(base_sku, brand_abbr):
    """styleColorFallbackKeys: category namespace first (XX_P01 / XX_B01 /
    XX_V01 / XX_SW_036), then the legacy XX_NNN key. NNN is the longest digit
    run; on a tie the LAST run wins (reduce keeps c unless a is longer)."""
    b = (base_sku or '').upper()
    prefix = sku_image_prefix(b, brand_abbr)
    keys = []
    serial = b[6:9] if len(b) >= 9 else ''
    numbers = _DIGIT_RUNS.findall(b)
    padded = (reduce(lambda a, c: a if len(a) > len(c) else c, numbers).rjust(3, '0')
              if numbers else None)
    if _P_SERIAL.fullmatch(serial) or has_pants_serial(b):
        keys.append(f'{prefix}_{serial}')
    elif is_blazer(b) or is_vest(b):
        keys.append(f'{prefix}_{serial}')
    elif padded and is_sportswear(b, brand_abbr):
        keys.append(f'{prefix}_SW_{padded}')
    if padded:
        keys.append(f'{prefix}_{padded}')
    return keys


def format_fabric_name(raw):
    """formatFabricName: typo fixes, spacing around / % -, Title Case with
    CVC/TC/GSM kept upper. Two characters or fewer come back unchanged."""
    if not _truthy(raw) or len(raw) <= 2:
        return raw
    s = raw
    s = re.sub(r'Polyster\b', 'Polyester', s, flags=re.ASCII | re.IGNORECASE)
    s = re.sub(r'Spendex\b', 'Spandex', s, flags=re.ASCII | re.IGNORECASE)
    s = re.sub(r'Spand\b', 'Spandex', s, flags=re.ASCII | re.IGNORECASE)
    s = re.sub(r'Traverler\b', 'Traveler', s, flags=re.ASCII | re.IGNORECASE)
    s = re.sub(r'Cataonic\b', 'Cationic', s, flags=re.ASCII | re.IGNORECASE)
    s = re.sub('-{2,}', '-', s)
    s = s.replace('/', ' / ')
    s = re.sub('([0-9])' + _S + '*%' + _S + '*', '\\1% ', s)
    s = re.sub(_S + '*-' + _S + '*', ' - ', s)
    s = _js_trim(re.sub(_S + '{2,}', ' ', s))
    before = s     # the browser's callback reads s.indexOf(word) on the pre-replace text

    def _title(m):
        word = m.group(0)
        lower = word.lower()
        if lower in ('from', 'made', 'with') and before.find(word) > 0:
            return lower
        return word[:1].upper() + word[1:].lower()
    s = re.sub(r'\b([a-zA-Z]+)\b', _title, s, flags=_A)
    s = re.sub(r'\bCvc\b', 'CVC', s, flags=_A)
    s = re.sub(r'\bTc\b', 'TC', s, flags=_A)
    s = re.sub(r'\bGsm\b', 'GSM', s, flags=_A)
    return s


def parse_sku_fabric(sku):
    """The fabric part of parseSkuComponents: (code, formatted name), with the
    Chaps / Ben Sherman / USPA substitutions and the PP-on-pants description.
    The name is None when the browser's early return leaves it undefined."""
    if not _truthy(sku) or len(sku) < 2:
        return '', None
    base = _base_of(sku)
    if len(base) < 2:
        return '', None
    brand = base[2:4] if len(base) >= 4 else ''
    fabric = base[4:6] if len(base) >= 6 else ''
    name = _js_or(FABRIC_RULES.get(fabric), fabric)
    if brand == 'CH':
        if fabric == 'YD':
            name = '50% Microfiber / 50% Polyester Yarn Dye'
        if fabric == 'PT':
            name = '97% Polyester / 3% Spandex (150D STRETCH)'
    if brand == 'BE':
        if fabric == 'YD':
            name = '77% Poly / 20% Cotton / 3% Spandex'
    if brand == 'US':
        if fabric == 'CD':
            name = '65% Polyester / 35% Cotton'
    if fabric == 'PP' and len(base) >= 9 and base[6] == 'P' and _DIGIT.fullmatch(base[7]):
        name = '100% Polyester Woven Dress Pant'
    return fabric, format_fabric_name(name)


# ── ICU-like collation for localeCompare ────────────────────────────────────

# CLDR root order of the ASCII non-alphanumerics (whitespace first), then
# digits, then letters case-insensitively. Ties go to accents, then to case
# (lowercase first), as ICU's secondary and tertiary levels do.
_ICU_PUNCT = ' _-,;:!?.\'"()[]{}@*/\\&#%`^+<=>|~$'


def _icu_key(s):
    primary, secondary, tertiary = [], [], []
    for ch in s:
        decomposed = unicodedata.normalize('NFD', ch)
        base, marks = decomposed[0], decomposed[1:]
        i = _ICU_PUNCT.find(base)
        if base in _JS_WS_CHARS:
            primary.append(0)
        elif i >= 0:
            primary.append(1 + i)
        elif '0' <= base <= '9':
            primary.append(100 + ord(base))
        elif base.isalpha():
            primary.append(1000 + ord(base.lower()[0]))
        else:
            primary.append(100000 + ord(base))
        secondary.append(ord(marks[0]) if marks else 0)
        tertiary.append(1 if ch.isupper() else 0)
    return tuple(primary), tuple(secondary), tuple(tertiary)


# ── The decorator ───────────────────────────────────────────────────────────

class TjxDisplay:
    """Decorates TJX catalog export rows exactly like the browser does.

    overrides:          GET /overrides 'overrides' ({KEY: {color, fit, ...}};
                        keys ending '-' are prefix keys).
    color_map:          {KEY_UPPER: raw description} from style_color_map.xlsx.
    banner_rules:       GET /banner-rules 'rules'; the seed / built-in merge
                        of loadBannerRules is applied here.
    catalog_customers:  customer codes from the catalog link (empty for a
                        link without customer / customers).
    inventory_rows:     optional raw feed rows. getItemCategory reads the brand
                        from them only when it is called without a brand.
    """

    def __init__(self, overrides, color_map, banner_rules, catalog_customers=None,
                 inventory_rows=None):
        if isinstance(overrides, dict) and isinstance(overrides.get('overrides'), dict):
            overrides = overrides['overrides']
        self.overrides = overrides or {}
        self.color_map = color_map or {}
        self.banner_rules = merge_banner_rules(banner_rules)
        self.catalog_customers = list(catalog_customers or [])
        self._prefix_keys = [k for k in self.overrides if k.endswith('-')]
        self._inv_brand = {}
        for row in inventory_rows or []:
            base = _base_of(row['sku'])
            if base not in self._inv_brand:
                self._inv_brand[base] = _js_or(row.get('brand_abbr'), row.get('brand'), '')

    # ── overrides ──
    def get_style_override(self, sku):
        """getStyleOverride: exact key, then the first prefix key ('XYZ-')
        whose prefix starts the SKU."""
        if not _truthy(sku):
            return None
        su = sku.upper()
        hit = self.overrides.get(su)
        if _truthy(hit):
            return hit
        for key in self._prefix_keys:
            if su.startswith(key[:-1]):
                return self.overrides[key]
        return None

    def _override_field(self, sku, field):
        ov = self.get_style_override(sku)
        if not _truthy(ov):
            return None
        return _js_get(ov, field)

    # ── brand ──
    def brand_key(self, sku, feed_brand):
        """rebuildAppData step 4."""
        brand = feed_brand
        if _truthy(brand) and brand in BRAND_ALIASES:
            brand = BRAND_ALIASES[brand]
        if _truthy(sku):
            su = sku.upper()
            if su.startswith('LUCK'):
                brand = 'LUCKY'
            elif su.startswith('VP'):
                brand = 'VERSA'
            elif len(sku) >= 4:
                correct = SKU_BRAND_CODE_MAP.get(sku[2:4].upper())
                if correct and correct != brand and brand not in BRAND_FULL_NAMES:
                    brand = correct
        ov_brand = self._override_field(sku, 'brand')
        if _truthy(ov_brand) and ov_brand != brand:
            brand = ov_brand
        if brand is None:
            return 'undefined'   # brandGroups[undefined] -> the key 'undefined'
        return brand

    def brand_full(self, brand_key):
        return BRAND_FULL_NAMES.get(brand_key, brand_key)

    def brand_sort_key(self, brand_key):
        """sortBrandsByCustomOrder: BRAND_ORDER position first, then unlisted
        brands by full name (localeCompare)."""
        if brand_key in BRAND_ORDER:
            return (0, BRAND_ORDER.index(brand_key), (), (), ())
        return (1, 0) + _icu_key(self.brand_full(brand_key))

    # ── color ──
    def color_info(self, sku, brand_abbr):
        """getStyleColorInfo: override color, then the map by full SKU (sized
        SKUs only), base SKU, and the XX_NNN fallback keys. None when no hit."""
        full = _js_trim((sku or '').upper())
        base = full.split('-')[0]
        ov = _js_or(self.overrides.get(base), self.overrides.get(full), {})
        color = _js_get(ov, 'color')
        if _truthy(color):
            return {'display': color, 'ground': color, 'hasPrint': False}
        raw = self.color_map.get(full) if '-' in full else None
        if not _truthy(raw):
            raw = self.color_map.get(base)
        if not _truthy(raw):
            keys = style_color_fallback_keys(base, brand_abbr)
            if not keys:
                return None
            for k in keys:
                raw = self.color_map.get(k)
                if _truthy(raw):
                    break
        if not _truthy(raw):
            return None
        if '||' in raw:
            parts = raw.split('||')
            ground, printed = format_color_name(parts[0]), format_color_name(parts[1])
            return {'ground': ground, 'print': printed, 'display': ground + ' ' + printed,
                    'hasPrint': True}
        name = format_color_name(raw)
        return {'color': name, 'display': name, 'hasPrint': False}

    def color_display(self, sku, brand_abbr):
        info = self.color_info(sku, brand_abbr)
        return info['display'] if info else ''

    def color_family(self, sku, brand_abbr):
        """_tjxColorFamily."""
        try:
            info = self.color_info(sku, brand_abbr)
            bucket = classify_color(info['display'] if info else '')
            return _COLOR_FAMILY.get(bucket, 'Fancy')
        except Exception:
            return ''

    # ── fit / fabric ──
    def fit_label(self, sku):
        """getFitFromSKU."""
        fit = self._override_field(sku, 'fit')
        if _truthy(fit):
            return fit
        label = fit_code_to_label(extract_fit_code(sku), sku)
        if is_pants(sku):
            label = _js_trim(_WS_RUN.sub(' ', _SLEEVE_WORDS.sub(' ', label)))
        return label

    def fabric(self, sku):
        """getFabricFromSKU -> (code, description)."""
        ov = self.get_style_override(sku)
        if _truthy(ov) and _truthy(_js_get(ov, 'fabrication')):
            code, _name = parse_sku_fabric(sku)
            return _js_or(_js_get(ov, 'fabricCode'), code, '\u270e'), ov['fabrication']
        code, name = parse_sku_fabric(sku)
        if _truthy(code) and _truthy(name):
            return code, name
        return 'N/A', 'Standard Fabric'

    # ── categories ──
    def is_short_sleeve(self, sku):
        if is_pants(sku):
            return False
        fit = self._override_field(sku, 'fit')
        if _truthy(fit):
            return bool(_SHORT_SLEEVE_TEXT.search(_js_string(fit)))
        return extract_fit_code(sku) in SHORT_SLEEVE_CODES

    def is_long_sleeve_shirt(self, sku):
        if is_pants(sku):
            return False
        fit = self._override_field(sku, 'fit')
        if _truthy(fit):
            if _SHORT_SLEEVE_TEXT.search(_js_string(fit)):
                return False
            if _LONG_SLEEVE_TEXT.search(_js_string(fit)):
                return True
        if self.is_short_sleeve(sku):
            return False
        return extract_fit_code(sku) in LONG_SLEEVE_FIT_CODES

    def get_item_category(self, sku, brand_abbr):
        if not _truthy(sku):
            return 'shirts'
        base = _base_of(sku)
        if has_pants_serial(base):
            return 'pants'
        if len(base) >= 11 and base[-1] in SPORTSWEAR_COLLARS:
            return 'sportswear'
        if len(base) >= 6 and base[4:6] in SPORTSWEAR_FABRICS:
            return 'sportswear'
        brand = brand_abbr if _truthy(brand_abbr) else self._inv_brand.get(base, '')
        brand = _js_string(brand).upper()
        if brand == 'CHAPS' and base.startswith('CTH'):
            return 'accessories'
        if brand == 'SHAQ' and len(base) >= 3 and 'T' in base[:3]:
            return 'accessories'
        return 'shirts'

    def is_button_down(self, sku, brand_abbr):
        if not _truthy(sku):
            return False
        base = _base_of(_js_string(sku))
        if len(base) < 11:
            return False
        if base[-1] not in BUTTON_DOWN_COLLARS:
            return False
        if extract_fit_code(sku) not in FIT_CODES:
            return False
        if is_pants(sku) or is_sportswear(sku) or is_blazer(sku):
            return False
        return self.get_item_category(sku, brand_abbr) == 'shirts'

    def get_detailed_category(self, sku, brand_abbr):
        base = self.get_item_category(sku, brand_abbr)
        if base in ('pants', 'sportswear', 'accessories'):
            return base
        if is_blazer(sku):
            return 'blazers'
        if is_young_men(sku):
            return 'young_men'
        if is_big_and_tall(sku):
            return 'big_tall'
        return 'short_sleeve' if self.is_short_sleeve(sku) else 'long_sleeve'

    def matches_category(self, sku, brand_abbr, category, for_prepack=False):
        if not _truthy(category) or category in ('all', 'any'):
            return True
        if category == 'sportswear':
            return is_sportswear(sku, brand_abbr)
        if category == 'pants':
            return is_pants(sku, brand_abbr)
        if category == 'blazers':
            return is_blazer(sku)
        if category == 'vests':
            return is_vest(sku)
        if category == 'young_men':
            return is_young_men(sku)
        if category == 'short_sleeve':
            return self.is_short_sleeve(sku) and (for_prepack or not is_sportswear(sku, brand_abbr))
        if category == 'long_sleeve':
            return self.is_long_sleeve_shirt(sku) and (
                for_prepack or (not is_blazer(sku) and not is_sportswear(sku, brand_abbr)))
        if category == 'button_down':
            return self.is_button_down(sku, brand_abbr)
        return self.get_detailed_category(sku, brand_abbr) == category

    # ── banners ──
    def matching_banners(self, sku, brand_abbr, customer_code, ignore_visibility=False):
        """getMatchingBanners in catalog mode (CATALOG_MODE is true)."""
        rules = self.banner_rules
        if not rules:
            return []
        sku_u = _js_trim(sku.upper())
        base = sku_u.split('-')[0]
        cust_u = _js_trim(_js_str_method(_js_or(customer_code, '')).upper())
        brand_u = _js_trim(_js_str_method(_js_or(brand_abbr, '')).upper())
        self.get_detailed_category(sku, brand_abbr)   # evaluated (unused) by the browser too
        fit = extract_fit_code(sku)
        fab = base[4:6] if len(base) >= 6 else ''
        is_admin = False

        def _sku_hit(s):
            su = _js_trim(_js_str_method(s).upper())
            return bool(su) and (su == sku_u or su == base or sku_u.startswith(su))

        def _keep(r):
            vis = _js_or(_js_get(r, 'visibility'), 'both')
            if not ignore_visibility:
                if vis == 'admin' and not is_admin:
                    return False
                if vis == 'catalog' and is_admin:
                    return False
            r_skus = _js_or(_js_get(r, 'skus'), [])
            n = _js_len(r_skus)
            if n is not None and n > 0:
                return _js_some(r_skus, _sku_hit)
            also = _js_or(_js_get(r, 'alsoSkus'), [])
            n = _js_len(also)
            if n is not None and n > 0 and _js_some(also, _sku_hit):
                return True
            cat = _js_or(_js_get(r, 'category'), 'any')
            if cat != 'any' and not self.matches_category(sku, brand_abbr, cat, True):
                return False
            fits = rule_fits(r)
            if fits and fit not in fits:
                return False
            custs = rule_customers(r)
            if custs and (not cust_u or cust_u not in [c.upper() for c in custs]):
                return False
            brands = rule_brands(r)
            if brands and (not brand_u or brand_u not in [b.upper() for b in brands]):
                return False
            fabs = rule_fabrics(r)
            if fabs and (not fab or fab not in [f.upper() for f in fabs]):
                return False
            return True

        return [r for r in rules if _keep(r)]

    def new_fabric(self, sku, brand_abbr):
        """_tjxNewFabric: 'YES' when a matching banner reads 'New Fabric'."""
        try:
            banners = self.matching_banners(sku, brand_abbr, self.export_customer(sku), True)
            hit = any(_js_trim(_js_string(_js_or(_js_get(b, 'text'), ''))).lower() == 'new fabric'
                      for b in banners)
            return 'YES' if hit else ''
        except Exception:
            return ''

    # ── prepack fields ──
    def export_category(self, sku, brand_abbr):
        return self.get_detailed_category(sku, brand_abbr)

    def export_fit(self, sku):
        return extract_fit_code(sku)

    def export_customer(self, sku):
        """_currentPrepackCustomer."""
        if self.catalog_customers:
            return self.catalog_customers[0]
        return sku[:2].upper() if _truthy(sku) else ''

    def size_pack_override(self, sku):
        pack = self._override_field(sku, 'sizePack')
        return pack if _truthy(pack) else None
