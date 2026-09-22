"""Cost engine for the ADMIN-ONLY Versa Profit & Loss tool.

CONFIDENTIAL CONTEXT. This file holds no cost data. Every factory price comes from the cost
book (the encrypted store object built by the local parser) and every derived pricing
parameter (short-sleeve delta, fit premiums, grid precedence, pool order, program SKU map,
kit sizes, customer-group prefixes, FX base) comes from costbook['params']. The only numbers
here are public defaults (HTS duty rates, public freight benchmarks, royalty norms) in
DEFAULT_SETTINGS, which the UI marks as "Assumption" until David confirms them.

Pure and deterministic: no I/O, no clock reads, no network. The __main__ CLI at the bottom is
the only I/O and exists for private local runs.

Public API
==========
build_dataset(src, costbook, settings, overrides, now_iso, routing_module) -> dict
    src             the normalized sources of pnl.py (DESIGN 3.4): inventory{items,last_sync},
                    ledger{rows,last_sync}, apo{rows,last_sync}, manual_allocations[],
                    vw_allocations[], open_orders{orders,ok,fetched_at}, fob_customers[],
                    routing_inputs{}, sales_analytics (payload | {'building': True} | None),
                    today 'YYYY-MM-DD'. Optional: ledger_history{rows} (or a list) with past
                    ledger lines (production, poName, style, units) that restore received refs.
    costbook        DESIGN 5.1 (records + params). Records without 'pool' get one derived from
                    source_code, so the study table (prices_ALL.json) also works.
    settings        stored settings (DESIGN 5.2) or {}; merged over DEFAULT_SETTINGS.
    overrides       DESIGN 5.3 list (level L0).
    now_iso         build time stamp (UTC ISO), copied to builtAt.
    routing_module  pnl_routing (route_all is called once).
    Returns the DESIGN 5.4 dataset. Money is rounded to cents, unit costs to 4 decimals.
decode_sku(sku, params=None) -> dict | None     SKU attributes (None when undecodable)
CostIndex(costbook, settings, overrides, ledger_rows, today=None, history_rows=None)
    .resolve(style, factory, ref=None, poName=None, customer_group=None)
        -> {fobU, level, basis, evidence, rangeLo, rangeHi, factory, flags, fxShare, fxRef}   one factory's cost
    .style_quote(style) -> the combined cost of a style (contract C13) with its rule, grade, spread and
        costByFactory, or None when no factory quotes it directly
    .final(base, res) -> the resolution every table uses (contract C13 choke point)
price_fit(fit) -> the price column a fit uses (modern and tailored: regular)
landed(fobU, attrs, settings, regime, origin) -> {duty, freight, fees, landedU}   per unit
line_money(...) / stock_money(...)               DESIGN 5.6 and C11 row math (the client repeats it)
fx_what_if_unit(fobU, fxShare, fxRef, rate_new, rate_fallback=None) -> fobU'   the RMB what-if
fiber_class(fiber) -> 'natural' | 'synthetic'    the import multiplier's fiber class (C11)
fiber_group_of(content) -> 'cotton' | 'linen' | 'wool' | 'mmf' | None   the C11 predominant fiber rule
landed_multiplier(S, fiber) -> m | None          the multiplier a row uses (None = itemized math)
revenue_cost_pct(S) -> revCostPct                settings.revenueCosts item percents, summed in order
royalty_base(S) -> 'net' | 'revenue'             what royalty is a percent of
merge_settings(stored, params=None) -> effective settings
clean_params(params) -> (clean, missing, invalid) the cost book params, type checked
price_field_roles(costbook) -> {usd, base, cut, rmb, e1}   cost-book field names by role
DEFAULT_SETTINGS                                 public defaults only

Cost-book price fields are read by ROLE (contract C1): meta.priceFieldRoles, else the order of
meta.priceFields (usd, base, cut, rmb), else the role-neutral names. This file never spells a
cost-book field name that carries a workbook rate.

Price basis (contract C10, David, Sep 15 2026). Every record is priced at the Current USD printed
on its own sheet for its fit (the 'usd' role), at the sheet's own printed rate (the 'e1' role).
settings.fx.rate null (the default) keeps every price as printed. A saved rate R prices every
RMB-based (calculator) record at printed x (its sheet rate / R), which is its RMB amount / R. The
scaling happens on each candidate record, before medians, ranges, sibling and default levels, so
every level stays consistent. List prices and USD quotations never move. The 'base' role (prices
restated at params.fxBase) is only read to find a sheet rate a record does not carry.
Every row with an RMB-based part carries fxShare (the share of its unit cost that is RMB based)
and fxRef (the printed rate that part stands at, value weighted). The page's what-if:
    fobU' = r4(fobU x (1 - fxShare + fxShare x fxRef / rateNew)),   fob' = r2(qty x fobU')

Duty regimes (contract C2). Every lines, apo, inventory and production row carries dutyRegime:
    none  no import costs (FOB customer rows, and destinations mapped to 'none')
    ca    Canada: duty = fob x tariff.caMfnPct / 100; no US add-on, no specific duty, no MPF or
          HMF; freight = units x ocean, fees = units x inland (as for us)
    us    DESIGN 5.6 as written
settings.destinations maps an A2000 wh (lines), a ledger landing (production) or a stock
warehouse (inventory) to a regime. Unit costs round to 4 decimals first; fob = r2(units x fobU).
The `regime` argument of adders, landed, line_money and stock_money also accepts the old
fob_line boolean (true = none, false = us).

Cost model (contract C11, David, Sep 15 2026). settings.landed.mode 'itemized' (the default) keeps the
duty, freight and fees above. 'multiplier' costs every 'us' or 'ca' row with one import multiplier m
for the row's fiber class (fiber_class: natural = cotton, linen or wool; synthetic = everything else):
    duty = r2(fob x (m - 1)), freight = fees = 0, cogs (revenue rows) or landed (stock) = r2(fob + duty)
    per unit (landed()): duty = r4(fobU x (m - 1)), landedU = r4(fobU + duty)
In that mode the duty field means "Import (tariffs, freight and fees)" (dict.dutyLabel). A fiber class
with no multiplier keeps the itemized math on its rows, and the alert 'settings_incomplete' names it.
Every revenue row (lines, apo, shipped.byCustomer):
    royalty = r2(B x royaltyPct / 100)       B = net (settings.royalty.base 'net', the default) or rev
    revCost = r2(rev x revCostPct / 100)     revCostPct = revenue_cost_pct(S); set whenever rev is set
    contrib = r2(gp - royalty - revCost)     null when the row has no cost
Duty regime by customer (contract C12). A row with a customer (lines, apo, shipped history) takes, in
order: settings.regimeByCustomer[cust], 'none' for an FOB customer, settings.destinations[wh] (for an
allocation: its customer's open lines, C2), then 'us'. Stock rows follow destinations only.
Shipped history by customer (contract C14): shipped.byCustomer holds lifetime invoices per account and
style (history codes fold into their account), costed like order lines at today's style cost with the
account's chargeback rate and regime (no warehouse). shipped.range = {from, to, ingestedAt}.
Multi-factory cost of goods (contract C13). CostIndex.quotes(b) lists every direct factory quote for a base
style: list quotes (a Pinnacle-type list names the exact style on any ref, L1; a David Peng-type list prices
a ref and pattern and a KinYun-type list a ref, pattern and fit, L2, for refs of that factory that carry the
style on the ledger) and exact calculator matches (grid_exact: the style's own brand, fabric, fit, sleeve and
pattern at the style's fit column, L4a or L4b) under every factory that makes the style. Modern and tailored
fits use the regular fit price (price_fit). Sibling and fabric medians, relaxed matches, proxies and defaults
are not direct quotes. One value per factory (its one row: the lowest of its quotes, C15). CostIndex.combined(b): one factory
gives 'single'; several give 'average', unless max > min x (1 + wideSpreadPct / 100), then 'lowest'
(exactly at the threshold is 'average'). The grade is the worst grade among the values used; the level is
the used value's level ('single', 'lowest') or the best level among the inputs ('average'). fxShare and
fxRef are value weighted over the values used. CostIndex.final(b, res) is the one choke point every table
takes its cost through: a manual cost (L0) wins outright, a style with a direct quote takes its combined
cost on every row (the row keeps its factory), anything else keeps its ladder result ('fallback').
settings.costRule.mode 'cascade' switches the rule off (today's behaviour).
One right row (contract C15, David, Sep 15 2026). A style's cost on a factory sheet (calculator or price list)
comes from exactly one row, picked from the style number [customer][brand][fabric][serial][fit][pattern]: R1 the
style's own brand section only (brand sections are never combined; with no section on a sheet, its generic
all-brand rows or the customer group's own quotation; another brand's row only when nothing else prices the style:
level L4c, flag cross_brand, grade D, after the L5 proxies; a customer's brand-less quotation never serves another
group), R2 the fabric code (high confidence and the first code preferred), R0 a row whose text names the style is
that style's row, before the sleeve and pattern rules (its sleeve decides when the fit code is unknown; a pattern
that disagrees with the style number is flagged named_row_conflict), R3 the sleeve, R4 the pattern (solid for
solid, print for print; yarn dyed fabrics YD, SP and CY read yarn dyed; a row with no pattern, or whose pattern the
cost book only inferred, serves only when no pattern row fits; regular print over digital print unless the style
says digital, flag pattern_guess; never relaxed), R5 variant rows (brushed, perforated, zipper, tie, both side
print, camp collar) only for a style that shows the variant by its number; white, colour, cooling and cotton-origin
rows lose to a plain row; a row whose sheet text says its price is still needed loses to any confirmed row (flag
price_unconfirmed when it is the only one), R6 the fit column (modern and tailored: regular; unknown: regular, flag
fit_unknown), R7 the lowest of the rows still tied (then the lowest sheet row), flag ambiguous_rows, every
candidate listed. Never a median. Contract C13 then combines the factories. styles.costRow, costFlags and
costRowsSkipped name the row used, its C15 flags and the rows set aside (CostIndex._pick).

Contract additions (beyond DESIGN 5.4), all appended after the contract fields:
    lines: basis, dutyRegime, costRef, pieces, fxRef, revCost
    alloc: costRef, lotTier, pieces, fxShare, fxRef, revCost (the line's revenue costs for these units)
    apo: routing, ref, basis, ev, dutyRegime, costRef, coveredBy, fxRef, revCost
    inventory: basis, ev, dutyRegime, flags, fxRef
    production: basis, ev, dutyRegime, fxRef, ownFobU, ownFxShare, ownFxRef (the maker's own ladder cost for
            the line, L['res']: what that factory bills. fobU stays the style's cost after contract C13; the
            page's factory payments use ownFobU)
    styles: basis, ev, t12Net, dutyRegime, dedPct, atsFreeStock, atsFreeProd, fxShare, fxRef, openRevCost,
            costByFactory ([factory, source, refOrSheet, price4, level] per direct quote, by price),
            costRule (single, average, lowest, manual, fallback; 'cascade' in cascade mode), costSpread
            (r4(max / min - 1) over the per-factory values, 0 for one, null for none) (contract C13),
            costRow ([source, cell on that sheet, row label, fit column 'slim' | 'regular' | 'big_tall', price4 as
            the sheet prints it]; null when no row prices the style: for an average of factories, the lowest
            factory's row), costFlags (the C15 flags of that cost, possibly empty), costRowsSkipped ([cell as its
            record id 'SRC!CELL', because a row set aside can sit on another sheet, row label, price4, reason] per
            row set aside; reasons other_brand, other_pattern, other_sleeve, variant_not_this_style,
            lower_confidence, tie_not_lowest) (contract C15)
    row flags (contract C15): ambiguous_rows, cross_brand, fit_unknown, pattern_guess
    shipped.company: costedRev, revCost     shipped.byStyle: fobU, level, grade, origin, fxShare, fxRef, caShare
    shipped.byCustomer {fields, rows}, shipped.range (contract C14)
    inputs.fx: {mode, rate, basis, base, printed[], weighted, unrated} (the price basis in use)
    dict: basis{key: text}, flags{flag: label}, alertKinds{kind: {label, unit, valueLabel}},
          lotTiers{tier: {label, tone}}, priceBasis{basis: text}, regimes{regime: text},
          fiberClasses{class: text}, dutyLabel (the duty field's name under the cost model in use),
          costRules{rule: text} (contract C13)
    alerts: unit, valueLabel (C4); thin_contribution also lineCount; every alert refsTotal
    inputs: analytics_to, costbook_generated, routing{...counts}, lotTiers, overridesActive,
            analyticsProblem, analyticsRowsSkipped, costbookInvalid, costbookBadRecords
    totals.*: costedRev, costedNet, costedUnits, costedLines, grades, revCost, uncostedRevCost; lines
              totals count pieces. contrib = gp - royalty - (revCost - uncostedRevCost) on each part.
    evidence entries: kind, ref, style, brand, priceSheet
"""
import copy
import math
import re
import statistics
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta

__all__ = [
    'build_dataset', 'decode_sku', 'CostIndex', 'landed', 'line_money', 'stock_money', 'adders', 'fx_what_if_unit',
    'merge_settings', 'grade_of', 'base_of', 'fac_of', 'r2', 'r4', 'load_snapshot',
    'clean_params', 'price_field_roles', 'program_brand', 'friendly_date',
    'DEFAULT_SETTINGS', 'LEVELS', 'LEVEL_INFO', 'GRADE_INFO', 'ROUTING_INFO', 'CAT_GROUP',
    'CATEGORY_LABELS', 'FIBER_BY_FABRIC', 'FLAG_LABELS', 'ALERT_KINDS', 'LOT_TIER_LABELS',
    'PRICE_BASIS_LABELS', 'REGIME_LABELS', 'DUTY_REGIMES',
    'fiber_class', 'fiber_group_of', 'landed_multiplier', 'revenue_cost_pct', 'royalty_base',
    'FIBER_CLASSES', 'NATURAL_FIBERS', 'LANDED_MODES',
    'price_fit', 'PRICE_FIT', 'COST_RULES', 'COST_RULE_LABELS', 'COST_RULE_MODES',
]

# ── Levels, grades, routing labels (DESIGN 5.7; UI copy: no em or en dashes) ──
LEVELS = ('L0', 'L1', 'L2', 'L3', 'L4a', 'L4b', 'L4c', 'L4d', 'L5', 'L6', 'L7')
LEVEL_RANK = {lv: i for i, lv in enumerate(LEVELS)}
LEVEL_INFO = {
    'L0': ('Manual cost', 'A'),
    'L1': ('Factory list: ref and style', 'A'),
    'L2': ('Factory list: ref', 'A'),
    'L3': ('Factory list: sibling ref', 'B'),
    'L4a': ("Calculator: factory's own grid", 'B'),
    'L4b': ('Calculator: other grid', 'B'),
    'L4c': ('Calculator: other brand', 'D'),        # contract C15 R1: the last resort, grade D
    'L4d': ('Calculator: derived', 'B'),
    'L5': ('Proxy: same style or design on a list', 'C'),
    'L6': ('Default for category', 'C'),
    'L7': ('Needs cost', 'D'),
}
GRADE_INFO = {
    'A': {'label': 'Solid', 'desc': 'Factory list price or manual cost, and a clean supply match.'},
    'B': {'label': 'Good', 'desc': 'Calculator price or a sibling list price. Or a forced pull.'},
    'C': {'label': 'Rough', 'desc': 'Proxy or category default. Or a stale batch or a fallback supply match.'},
    'D': {'label': 'Missing', 'desc': "No usable cost, a cost from another brand's row, or no supply record. Needs a "
                                      'manual cost or a check.'},
}
ROUTING_INFO = {
    'R1': 'Clean placement',
    'R2': 'Forced pull',
    'R3': 'Stale or undated batch',
    'R4': 'FOB account from US warehouse',
    'R5': 'FIFO fallback',
    'R6': 'No supply record',
}
_GRADES = ('A', 'B', 'C', 'D')
_ROUTING_CAP = {'R1': 'A', 'R2': 'B', 'R4': 'B', 'R3': 'C', 'R5': 'C', 'R6': 'D'}
_ROUTING_RANK = {'R1': 1, 'R2': 2, 'R3': 3, 'R4': 4, 'R5': 5, 'R6': 6}

CATEGORY_LABELS = {'dress_shirt': 'Dress shirts', 'sport_shirt': 'Sport shirts', 'overshirt': 'Overshirts',
                   'knit_shirt': 'Knit shirts', 'polo': 'Polos', 'pants': 'Pants', 'blazer': 'Blazers',
                   'vest': 'Vests', 'other': 'Other'}
# DESIGN 5.6 cat_group
CAT_GROUP = {'dress_shirt': 'shirt', 'sport_shirt': 'shirt', 'overshirt': 'shirt', 'other': 'shirt',
             'knit_shirt': 'knit', 'polo': 'knit', 'pants': 'pants', 'blazer': 'blazer', 'vest': 'blazer'}

# ── Public SKU decode rules (index.html / app.py helpers). No cost data. ──
MOD_SKU_RE = re.compile(r'^[A-Z0-9]{2}[A-Z]{4}(?:\d{3}|[PBV]\d{2})[A-Z]{2,3}$')
_REF_RE = re.compile(r'^([A-Z]{2})\d{5}$')
BOTTOM_FABRICS = frozenset({'BC', 'BR', 'BH', 'BA'})
OVERSHIRT_FABRICS = frozenset({'SF', 'SB', 'CO', 'SL', 'SG', 'SN', 'SV', 'SJ', 'SH', 'SS'})
POLO_FABRICS = frozenset({'PO', 'PJ', 'PW', 'PH', 'PL', 'KP', 'RB', 'SD', 'TH', 'HE'})
KNIT_FABRICS = frozenset({'PK', 'DP', 'PS', 'PA', 'PN', 'SE', 'PV', 'PZ', 'CU', 'KN'})
POLO_COLLARS = frozenset('ZUMNOR')
SHORT_SLEEVE_FITS = frozenset({'SS', 'SR', 'SB', 'ST', 'TB'})
BIG_TALL_FITS = frozenset({'BT', 'BB', 'TT', 'WB', 'SB', 'ST', 'TB'})
SOLID_COLLARS = frozenset('SBCFJHWMZO')
PRINT_COLLARS = frozenset('PADEGKLXUNR')
FIT_CLASS = {'SLIM': 'slim', 'REGULAR': 'regular', 'BIG_TALL': 'bigTall', 'MODERN': 'modern', 'TAILORED': 'tailored'}
# The price column a fit uses (contract C13): SKU fit code MF decodes to MODERN and TF to TAILORED, and both
# take the regular fit price, in the calculator grids and in the KinYun-type fit columns.
PRICE_FIT = {'MODERN': 'REGULAR', 'TAILORED': 'REGULAR'}


def price_fit(fit):
    """The price column of a decoded fit (contract C13): modern and tailored use the regular fit price."""
    return PRICE_FIT.get(fit, fit)


# Fiber group by fabric code, from the public FABRIC_RULES text (index.html) and the fabric content of each
# fabrication (study section 4.4), under the predominant fiber rule of contract C11 (fiber_group_of): the
# largest single fiber share wins, and a tie between a natural and a synthetic fiber counts as natural. So CL
# (lyocell and cotton tie) and PY (cotton is the largest share) are cotton. A code whose name gives no fiber
# content counts as man-made fiber (synthetic), except CV (CVC: cotton is the chief fiber) and SF (flannel,
# cotton in the factory's own content). The broker's HTS lines must override this for duty.
# costbook.params.fiberByFabric may override it too. REVIEW docs/FABRIC_CLASS.md lists every code.
FIBER_BY_FABRIC = dict(
    [(c, 'cotton') for c in ('CB', 'CF', 'CG', 'CJ', 'CK', 'CL', 'CM', 'CN', 'CP', 'CQ', 'CR', 'CT', 'CU', 'CV',
                             'CW', 'CX', 'CY', 'LC', 'OC', 'PD', 'PY', 'SC', 'SF', 'SG', 'SH', 'SJ', 'SN', 'ST',
                             'SV', 'SW')]
    + [(c, 'linen') for c in ('LB', 'LE', 'LN', 'LT')])
_FIBERS = ('cotton', 'mmf', 'linen', 'wool')
# Fiber class of the import multiplier (contract C11): natural = cotton, linen or wool; synthetic =
# everything else (man-made fiber and anything unknown).
NATURAL_FIBERS = ('cotton', 'linen', 'wool')
FIBER_CLASSES = ('natural', 'synthetic')
FIBER_CLASS_LABELS = {'natural': 'Natural fibers (cotton, linen, wool)', 'synthetic': 'Synthetic fibers'}
_FIBER_CLASS_WORDS = {'natural': 'natural fibers', 'synthetic': 'synthetic fibers'}
# Words of a fabric content that name a natural fiber group (fiber_group_of). Every other fiber is
# synthetic: polyester, nylon, spandex, elastane, and also rayon, viscose, modal, lyocell and tencel.
_NATURAL_WORDS = {'cotton': 'cotton', 'linen': 'linen', 'flax': 'linen', 'wool': 'wool', 'merino': 'wool',
                  'lambswool': 'wool'}
_STRETCH_WORDS = frozenset({'spandex', 'elastane', 'lycra'})

# Public brand names by SKU brand code (display only).
BRAND_NAMES = {
    'NA': 'Nautica', 'DK': 'DKNY', 'CH': 'Chaps', 'CS': 'Chaps', 'US': 'U.S. Polo Assn.', 'JN': 'Jones New York',
    'KN': 'Jones New York', 'GB': 'Geoffrey Beene', 'VD': 'Von Dutch', 'VC': 'Vince Camuto', 'EB': 'Eddie Bauer',
    'BE': 'Ben Sherman', 'LB': 'Lucky Brand', 'KL': 'Karl Lagerfeld', 'NM': 'Nicole Miller', 'RB': 'Reebok',
    'TA': 'Tayion', 'SH': "Shaquille O'Neal", 'MS': 'Michael Strahan', 'NW': 'Nine West', 'RG': 'Robert Graham',
    'BL': "Bloomingdale's private label", 'NE': 'Neiman Marcus private label', 'VS': 'Versa', 'AC': 'America',
    'DH': 'DH', 'DN': 'DN', 'BLK': 'Black Label',
}
# SKU brand code BL is shared by Bloomingdale's private label (feed label BLO) and Black Label
# (feed label BLACK). Rows whose feed label says Black Label report the pseudo code BLK.
_BLACK_LABELS = frozenset({'BLACK', 'BLACK LABEL'})
# ATS feed brand label -> SKU brand code (legacy SKUs only: the L6 brand default).
BRAND_LABEL_CODE = {'NAUTICA': 'NA', 'VD': 'VD', 'CHAPS': 'CH', 'USPA': 'US', 'TAYION': 'TA', 'EB': 'EB',
                    'BEN': 'BE', 'LUCKY': 'LB', 'JNY': 'JN', 'BEENE': 'GB', 'SHAQ': 'SH', 'STRAHAN': 'MS',
                    'DKNY': 'DK', 'VINCE': 'VC', 'NM': 'NM', 'KLP': 'KL', 'RB': 'RB', 'AMERICA': 'AC',
                    'BLACK': 'BL', 'BLO': 'BL', 'NW': 'NW'}
# APO free-text customer -> A2000 codes whose open prices estimate APO revenue (study extra.py). The
# first code is the account the row is booked to; any other code is the same customer's second
# account and adds its price evidence. Names were tied to codes by style prefix, open orders and
# invoice history (Sep 2026 snapshot and live rows): COSTCO CANADA rows ship like COST1 (CC styles,
# ABFI Canada warehouses), COSTCO US like COST (CU styles, US warehouses), MACY rows are MA styles
# (Macy's; Backstage is MACY1 on MB styles), JCP styles sit in the history of both JCPenney codes,
# BEALL styles are on BEAL's open orders, HP rows are Half Price's own HP lines and WALM rows are WM
# (Walmart) styles. A name not listed here stays unmapped unless it is itself a known code.
_JCP = ['JCP01', 'JCP02']
_BEALLS = ['BEAL', 'BEAL1']
APO_CUSTOMER_CODES = {
    'ROSS': ['ROSS'], 'TJX': ['TJMA', 'MARS'], 'TKX': ['TJMA', 'MARS'], 'BURLINGTON': ['BURL'],
    'HALF PRICE': ['HALF'], 'HP': ['HALF'], 'BJS': ['BJS'], 'AMAZON': ['AMAZ'], 'WALMART': ['WALM1'],
    'WALM': ['WALM1'], 'HAMRICK': ['HAMR'], 'HAMRICKS': ['HAMR'], 'DD': ['DDS'], 'WALMART-PEERLESS': ['PEER'],
    'PROMODA': ['MULT'], 'MACY': ['MACY'], 'MACYS': ['MACY'], 'MACY BACKSTAGE': ['MACY1'],
    'MACYS BACKSTAGE': ['MACY1'], 'BELK': ['BELK'], 'BLOOMINGDALE': ['BLOO'], 'BLOOMINGDALES': ['BLOO'],
    'MEN WARHOUSE': ['MENS'], 'MENS WEARHOUSE': ['MENS'], 'FORMAN': ['FORM'], 'FORMAN MILLS': ['FORM'],
    'COSTCO': ['COST'], 'COSTCO US': ['COST'], 'COSTCO USA': ['COST'], 'COSTCO CANADA': ['COST1'],
    'JCP': _JCP, 'JCPENNEY': _JCP, 'JC PENNEY': _JCP, 'JC PENNY': _JCP, 'NORDSTROM': ['NORD'],
    'KOHL': ['KOHL'], 'KOHLS': ['KOHL'], 'BEALL': _BEALLS, 'BEALLS': _BEALLS, 'VETE': ['VETE'],
    'VETERANS': ['VETE']}
_APO_BIG3 = ('ROSS', 'BURL', 'MARS', 'TJMA')
# Invoice-history customer codes -> the account code that settings and the FOB list use.
HISTORY_CUSTOMER_ALIAS = {
    'TJMX': 'TJMA', 'TJMA1': 'TJMA', 'ROSS1': 'ROSS', 'MARS1': 'MARS', 'CENT': 'CENT1',
    'AMAZON_DROP': 'AMAZ', 'MACYSDROP': 'MACY', 'KOHLSDROP': 'KOHL', 'NORD_DROP': 'NORD',
    'BELKDROP': 'BELK', 'AAFESDROP': 'AAFE', 'TMWDROP': 'MENS'}
# APO price estimate: minimum invoiced units before a customer's own history is used.
_MIN_HIST_UNITS_STYLE = 50
_MIN_HIST_UNITS_BRAND = 500
WAREHOUSE_NAMES = {'TR': 'Trans-Cal', 'J2': 'JTW', 'JTW': 'JTW', 'DW': 'DCW Torrance', 'D2': 'DCW Carson',
                   'DCW': 'DCW', 'QA': 'Quetico AZ', 'QC': 'Quetico Chino', 'AE': 'ABFI Canada East',
                   'AW': 'ABFI Canada West', 'ABFI': 'ABFI Canada', 'NJ': 'Edison NJ', 'CH': 'Factory direct',
                   'FOB': 'FOB at the factory', 'CAN': 'Canada', 'WALM': 'Walmart direct import',
                   'WH': 'US warehouse pool'}
_STOCK_KEYS = (('jtw', 'JTW'), ('tr', 'TR'), ('dcw', 'DCW'), ('qa', 'QA'), ('nj', 'NJ'), ('abfi', 'ABFI'))
_ENGINE_FOB_FALLBACK = ('CENT1', 'GLOB', 'BFL', 'TJXAU', 'TJXUK', 'HALF', 'MULT', 'MULT1')
_LIST_FACTORY_PSEUDO = ('MIX', 'WH', 'UNKNOWN', 'N/A', 'BLANK', 'OTHER')

FLAG_LABELS = {
    'bulk': 'Bulk forecast line', 'kit': 'Kit priced per carton', 'late': 'Past cancel date',
    'fob_line': 'FOB or factory direct. No US duty or freight.', 'unsourced': 'Some units have no supply record',
    'partial_cost': 'Only part of the units have a cost', 'forced': 'Forced pull from a later batch',
    'fob_wh_fallback': 'FOB account served from the US warehouse', 'program_map': 'Program code decoded by the cost book map',
    'assumed': 'Attributes assumed. Confirm.', 'range': 'Several prices match.',
    'derived': 'Price derived from a related calculator row', 'default': 'Category default price',
    'price_conflict': 'The price file has two prices for the same text',
    'sheet_rate_far': 'This sheet prints an RMB rate far from the other sheets.',
    'ss_upper_bound': 'Long sleeve rate used. Upper bound.', 'malformed_sku': 'SKU could not be decoded',
    'not_in_feed': 'Style not in the ATS feed', 'gate_fail': 'Routing gate failed. FIFO used.',
    'assignment': 'Manual deduction assignment honored', 'suppressed': 'Batch looks arrived. Routing skips it.',
    'fob_flag': 'Batch has no firm date', 'double_booking': 'Possible double booking', 'blank_ref': 'Ref filled from the PO name',
    'no_ref': 'Ledger line has no ref', 'unpriced': 'No price estimate', 'price_proxy': 'Price estimated from the style',
    'blended_lot': 'Stock may span several receipts', 'legacy_brand_default': 'Legacy SKU. Brand default used.',
    'sibling': 'Price from a sibling ref of the same factory',
    'fabric_median': "A row of the factory's list for the same brand and fabric, not this style",
    'non_us_dest': 'Not a US import', 'no_units': 'No open units. The line value is left out.',
    'price_offprice': 'Price estimated from Ross, TJX and Burlington prices for the brand',
    'price_history': "Price from this customer's past invoices",
    'covered_by_bulk': "Same goods as this customer's bulk order. Count one of them, not both.",
    'supply_in_allocations': "Its supply is booked as this customer's allocations",
    'regime_from_orders': "No import costs, like this customer's open orders",
    'ref_after_stock': 'Stock arrived before this ref shipped. The cost is a style estimate.',
    'customer_quote': 'Factory quotation for this customer',
    'customer_regime': "Import costs follow this customer's setting",
    'cost_average': 'Average of several factory quotes',
    'cost_lowest': 'Lowest of several factory quotes. They differ a lot.',
    'fit_as_regular': 'Modern or tailored fit priced at the regular fit',
    # contract C15
    'ambiguous_rows': 'Several sheet rows fit. The lowest is used.',
    'cross_brand': "No sheet quotes this brand. Priced from another brand's row.",
    'fit_unknown': 'Fit unknown. The regular column is used.',
    'pattern_guess': 'Regular or digital print not known. The regular print row is used.',
    'price_unconfirmed': 'The sheet says this price is still needed. Treat it as unconfirmed.',
    'named_row_conflict': 'A sheet row names this style, but its pattern or sleeve differs from the style number. '
                          'Confirm which is right.',
}

# ── Contract C15 (David, Sep 15 2026): one right row per factory sheet ──
# A style's cost on a sheet comes from exactly one row, picked from the style number: the brand's section (R1), the
# fabric code (R2), the sleeve (R3), the pattern (R4), no special finish the style does not show (R5), the column for
# its fit (R6). Rows still tied: the lowest, flagged ambiguous_rows (R7). Never a median of rows.
C15_FLAGS = ('ambiguous_rows', 'cross_brand', 'fit_unknown', 'pattern_guess')
C15_REASONS = ('other_brand', 'other_pattern', 'other_sleeve', 'variant_not_this_style', 'lower_confidence',
               'tie_not_lowest')
# costRowsSkipped order: the rows closest to the one used first.
_REASON_ORDER = {r: i for i, r in enumerate(('tie_not_lowest', 'variant_not_this_style', 'lower_confidence',
                                             'other_pattern', 'other_sleeve', 'other_brand'))}
# Cost book variant tags. A hard variant row prices only a style that shows that variant. A relative variant row
# loses to a plain row, and competes with the others (R7) only when no plain row is left.
HARD_VARIANTS = frozenset({'perforated', 'brushed', 'zipper', 'with_tie', 'two_side_print', 'camp_collar'})
REL_VARIANTS = frozenset({'white', 'colour', 'usa_cotton', 'xinjiang_cotton', 'cooling'})
# Fabric codes whose public FABRIC_RULES text says Yarn Dye (YD, SP and CY). A style of these fabrics reads yarn dyed
# whatever its pattern letter (contract C15 R4 and R5).
YARN_DYE_FABRICS = frozenset({'YD', 'SP', 'CY'})
# Cost book record flags read by contract C15: a row whose text says its price is still needed (it loses to any
# confirmed row), and a row whose pattern the cost book only inferred (R4 reads it as a row with no pattern).
PROVISIONAL_FLAGS = frozenset({'PROVISIONAL_TEXT_NEED_PRICE'})
INFERRED_PATTERN_FLAGS = frozenset({'PATTERN_NOT_STATED_INFERRED_SOLID'})
# What the style number shows, from the public platform dictionaries: fabric UP is perforated (FABRIC_RULES);
# pattern letter A is a both side print, J and K a camp collar, Z and U a zip collar (SHIRT_COLLAR_CODES,
# SPORTSWEAR_COLLAR_CODES); customer JT is a shirt and tie set (CUSTOMER_CODES).
VARIANT_BY_FABRIC = {'UP': 'perforated'}
VARIANT_BY_COLLAR = {'A': 'two_side_print', 'J': 'camp_collar', 'K': 'camp_collar', 'Z': 'zipper', 'U': 'zipper'}
VARIANT_BY_CUSTOMER = {'JT': 'with_tie'}
_CONF_RANK = {'high': 0, 'medium': 1, 'low': 2}
# The sheet column a price fit reads (styles.costRow).
FIT_COLUMN = {'SLIM': 'slim', 'REGULAR': 'regular', 'BIG_TALL': 'big_tall'}

# ── Public defaults (DESIGN 5.2; r1_landed_cost.md). The UI marks unconfirmed blocks. ──
_OFFPRICE = ('ROSS', 'DDS', 'BURL', 'MARS', 'TJMA', 'WINN', 'BEAL', 'BEAL1', 'HAMR', 'CITI', 'VARI', 'GABE', 'FORM')
_DEPARTMENT = ('MACY', 'MACY1', 'JCP01', 'JCP02', 'KOHL', 'NORD', 'BELK', 'BLOO', 'BOSC', 'SEAR', 'SAKS')
_CLUB = ('COST', 'COST1', 'COST2', 'BJS', 'PRIC')
_WALMART = ('WALM1', 'PEER')
_LICENSED = ('NA', 'DK', 'CH', 'CS', 'US', 'JN', 'KN', 'GB', 'VD', 'VC', 'EB', 'BE', 'LB', 'KL', 'NM', 'RB',
             'TA', 'SH', 'MS', 'NW', 'RG')
_HOUSE = ('BL', 'NE', 'VS')
DEFAULT_SETTINGS = {
    'v': 1,
    # rate None = as printed on each sheet: every price is the sheet's Current USD at its own rate
    # (contract C10). A saved rate R prices every RMB-based record at its RMB amount / R.
    'fx': {'rate': None, 'asOf': None, 'basis': 'current_usd'},
    'gridPrecedence': None,
    'factories': {'TF': {'name': 'Topfind', 'origin': 'CN'}, 'NB': {'name': 'Yuxiu', 'origin': 'CN'},
                  'PC': {'name': 'Pinnacle', 'origin': 'CN'}, 'DP': {'name': 'David Peng', 'origin': 'CN'},
                  'FR': {'name': 'KinYun (Frank Zhang)', 'origin': 'CN'}, 'NK': {'name': 'Najmul', 'origin': 'BD'},
                  '_default': {'name': 'Unknown factory', 'origin': 'CN'}},
    'tariff': {'addonPct': {'CN': 20.0, 'BD': 10.0, 'VN': 12.5, 'IN': 10.0, 'PK': 10.0, 'OTHER': 10.0},
               'mfnPct': {'shirt_cotton': 19.7, 'shirt_mmf': 25.9, 'shirt_linen': 2.8, 'shirt_wool': 17.5,
                          'knit_cotton': 19.7, 'knit_mmf': 32.0, 'knit_linen': 32.0,
                          'pants_cotton': 16.6, 'pants_mmf': 27.9, 'pants_linen': 16.6,
                          'blazer_wool': 17.5, 'blazer_mmf': 27.3, 'blazer_cotton': 9.4, 'blazer_linen': 9.4},
               'mmfShirtSpecificPerUnit': 0.07, 'mpfPct': 0.3464, 'hmfPct': 0.125,
               # Canada (regime 'ca'): the public Canadian MFN rate for apparel. An assumption until
               # David confirms who is the importer of record into Canada.
               'caMfnPct': 18.0},
    'freight': {'oceanPerUnit': {'shirt': 0.285, 'knit': 0.211, 'pants': 0.356, 'blazer': 0.95},
                'inlandPerUnit': {'shirt': 0.092, 'knit': 0.068, 'pants': 0.114, 'blazer': 0.305}},
    # Black Label (BLK) follows the licensed default until David says otherwise.
    # base: royalty on net sales ('net', after chargebacks) or on revenue ('revenue') (contract C11).
    'royalty': {'defaultPct': 10.0, 'base': 'net',
                'byBrand': dict([(b, 10.0) for b in _LICENSED] + [(b, 0.0) for b in _HOUSE] + [('BLK', 10.0)])},
    # Duty regime by A2000 wh, ledger landing or stock warehouse (contract C2). Anything not listed is
    # a US import. CH = factory direct, FOB = FOB at the factory, WALM = Walmart imports it.
    'destinations': {'CH': 'none', 'FOB': 'none', 'WALM': 'none', 'AE': 'ca', 'AW': 'ca', 'CAN': 'ca', 'ABFI': 'ca'},
    # Import costs by A2000 customer code (contract C12): 'us', 'ca' or 'none'. It beats the FOB list and
    # the destinations. Empty by default.
    'regimeByCustomer': {},
    # Cost model (contract C11): 'itemized' = the duty, freight and fees above; 'multiplier' = the factory
    # cost times one import multiplier per fiber class (1 to 3). No multiplier until David sets one.
    'landed': {'mode': 'itemized', 'multiplier': {'natural': None, 'synthetic': None}},
    'deductions': {'byGroup': {'offprice': 1.0, 'department': 7.0, 'club': 2.5, 'walmart': 1.5, 'fob': 0.5, 'other': 2.0},
                   'byCustomer': {}},
    # Revenue costs (contract C11): each item is a percent of revenue (0 to 50), taken off contribution.
    # Zero until David sets them.
    'revenueCosts': {'items': [{'key': 'warehouse', 'name': 'Warehouse', 'pct': 0},
                               {'key': 'factoring', 'name': 'R&R factoring', 'pct': 0},
                               {'key': 'rent', 'name': 'Rent', 'pct': 0}]},
    'customerGroups': dict([(c, 'offprice') for c in _OFFPRICE] + [(c, 'department') for c in _DEPARTMENT]
                           + [(c, 'club') for c in _CLUB] + [(c, 'walmart') for c in _WALMART]),
    'fobCustomers': None,
    'opex': {'items': [{'name': 'Payroll', 'monthly': 0}, {'name': 'Rent and utilities', 'monthly': 0},
                       {'name': 'Warehouse and 3PL', 'monthly': 0}, {'name': 'Samples and design', 'monthly': 0},
                       {'name': 'Travel and shows', 'monthly': 0}, {'name': 'Insurance and professional', 'monthly': 0},
                       {'name': 'Other', 'monthly': 0}]},
    # gateFallback: 'fifo' places a SKU that fails the routing gate in the engine's own FIFO order
    # (R5, what the platform shows); 'engine' runs the engine without the gate.
    'routing': {'picksAsWarehouse': False, 'honorAssignments': True, 'gateFallback': 'fifo'},
    'bulk': {'includeInTotals': False},
    # How a style's unit cost combines the prices of every factory that quotes it (contract C13).
    'costRule': {'mode': 'combined', 'wideSpreadPct': 10},
    # A note the Customers and Statement views show next to shipped history (contract C14).
    'history': {'caveat': None},
    'confirmed': {'fx': False, 'grid': False, 'tariff': False, 'freight': False, 'royalty': False,
                  'deductions': False, 'opex': False, 'landed': False, 'revenueCosts': False},
    'updatedAt': None, 'updatedBy': None,
}
# Alert thresholds (not cost data).
THIN_CM = 0.05            # PO contribution margin under 5 percent
STALE_FEED_HOURS = 6      # ATS / APO older than this at build time
STALE_ANALYTICS_DAYS = 45
_EV_CAP = 30
_REF_CAP = 200

DUTY_REGIMES = ('us', 'ca', 'none')
REGIME_LABELS = {'us': 'US import', 'ca': 'Canada', 'none': 'Not a US import'}
# Alert kinds (contract C4): title, the unit its count is in, and what its value means. A unit of
# None means the count is not meaningful (one feed, one file); a valueLabel of None means no value.
ALERT_KINDS = {
    'below_cost': {'label': 'Lines sold below cost', 'unit': 'lines', 'valueLabel': 'Loss before royalty'},
    'thin_contribution': {'label': 'Thin profit POs', 'unit': 'POs', 'valueLabel': 'Revenue on these POs'},
    'needs_cost': {'label': 'Revenue with no cost', 'unit': 'lines', 'valueLabel': 'Revenue with no cost'},
    'bulk_needs_cost': {'label': 'Bulk forecast with no cost', 'unit': 'lines', 'valueLabel': 'Bulk revenue with no cost'},
    'grade_d': {'label': 'Grade D exposure', 'unit': 'lines', 'valueLabel': 'Revenue'},
    'double_booking': {'label': 'Possible double booking', 'unit': 'refs', 'valueLabel': 'At factory cost'},
    'late': {'label': 'Past cancel date', 'unit': 'lines', 'valueLabel': 'Revenue'},
    'unsourced': {'label': 'Units with no supply record', 'unit': 'lines', 'valueLabel': 'Revenue'},
    'apo_unpriced': {'label': 'Allocations with no price', 'unit': 'allocations', 'valueLabel': None},
    'non_us_dest': {'label': 'Canada and direct-import costs are assumed', 'unit': 'styles', 'valueLabel': None},
    'stale_input': {'label': 'Input may be out of date', 'unit': None, 'valueLabel': None},
    'costbook_params': {'label': 'Cost book has missing or invalid parameters', 'unit': 'settings', 'valueLabel': None},
    'settings_incomplete': {'label': 'Import multiplier not set', 'unit': 'settings', 'valueLabel': None},
    'assumption': {'label': 'Assumptions to confirm', 'unit': 'settings', 'valueLabel': None},
}
# Lot tier labels (contract C6). The engine keeps the codes; T5 is shown in the warning tone.
LOT_TIER_LABELS = {
    'T1': {'label': 'Lot matched to one ref', 'tone': 'gray'},
    'T1b': {'label': 'Lot matched to several refs (averaged)', 'tone': 'gray'},
    'T2': {'label': 'Lot date matches one PO', 'tone': 'gray'},
    'T3': {'label': 'Style made on one ref', 'tone': 'gray'},
    'T4': {'label': "Average across the style's refs", 'tone': 'gray'},
    'T5': {'label': 'Estimate. No production ref found', 'tone': 'warn'},
}
# APO price estimate sources (apo.priceBasis).
PRICE_BASIS_LABELS = {
    'customer_brand': "This customer's open prices for the brand",
    'customer_line': "This customer's own order for the style",
    'offprice_brand': 'Ross, TJX and Burlington open prices for the brand',
    'style_open': 'Open order prices for the style',
    'customer_invoice': "This customer's past invoices for the style",
    'style_t12': "The style's invoices in the last 12 months",
    'customer_brand_invoice': "This customer's past invoices for the brand",
    'none': 'No price estimate',
}
# Assumption blocks in the order the UI lists them, with their plain names (contract C4).
_ASSUMPTION_NAMES = (('fx', 'RMB rate'), ('grid', 'calculator grid order'), ('deductions', 'chargebacks'),
                     ('freight', 'freight and fees'), ('opex', 'payroll and monthly costs'), ('royalty', 'royalty'),
                     ('revenueCosts', 'revenue costs'), ('landed', 'import cost method'),
                     ('tariff', 'duty and tariffs'))
_DESTINATIONS_NAME = 'Canada and direct-import destinations'
# Cost model settings (contract C11) and the cost rule modes (contract C13). Bands, not Versa data.
LANDED_MODES = ('itemized', 'multiplier')
ROYALTY_BASES = ('net', 'revenue')
COST_RULE_MODES = ('combined', 'cascade')
# How a style's unit cost was set (styles.costRule, contract C13). 'cascade' marks cascade mode.
COST_RULES = ('single', 'average', 'lowest', 'manual', 'fallback', 'cascade')
COST_RULE_LABELS = {
    'single': 'One factory quote',
    'average': 'Average of the factory quotes',
    'lowest': 'Lowest factory quote. The quotes are far apart.',
    'manual': 'Manual cost',
    'fallback': 'No direct factory quote. The best estimate is used.',
    'cascade': 'Price ladder. Factory quotes are not combined.',
}
_DIRECT_LEVELS = ('L1', 'L2', 'L4a', 'L4b')      # levels a direct quote can have (C13)
_LIST_LEVELS = ('L1', 'L2', 'L3')                # a list factory's own list answers at these levels
_LOT_FLAGS = ('blended_lot', 'ref_after_stock')  # stock flags that stay on a combined cost
_SPREAD_TOL = 1e-9                               # float noise: exactly at the threshold stays 'average'
_MULT_BAND = (1.0, 3.0)          # a valid import multiplier
_REV_COST_MAX = 50.0             # a valid revenue cost item, percent of revenue
_SPREAD_MAX = 100.0              # costRule.wideSpreadPct
_CAVEAT_MAX = 500                # history.caveat characters
DUTY_LABELS = {'itemized': 'Duty and tariffs', 'multiplier': 'Import (tariffs, freight and fees)'}
# Cost-book params by plain name (alert text never shows a raw key).
_PARAM_NAMES = {'params': 'all parameters', 'fxBase': 'cost book RMB rate', 'ssDelta': 'short sleeve adjustment',
                'fitPremium': 'fit premiums', 'gridPrecedence': 'calculator grid order',
                'poolOrder': 'calculator sheet order', 'programMap': 'program code map', 'kitPcs': 'kit sizes',
                'customerGroupPrefix': 'customer prefixes', 'fiberByFabric': 'fiber by fabric',
                'fiberByStyle': 'fiber by style'}


# ── Small helpers ──
def r2(x):
    """Half-up to cents. The client uses floor(x * 100 + 0.5) / 100 too."""
    return None if x is None else math.floor(x * 100 + 0.5) / 100


def r4(x):
    return None if x is None else math.floor(x * 10000 + 0.5) / 10000


def _num(v, default=0.0):
    if isinstance(v, bool):
        return default
    try:
        f = float(v)
    except (TypeError, ValueError, OverflowError):
        return default
    return f if math.isfinite(f) else default


_MONTHS = ('Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec')


def friendly_date(v):
    """'Aug 21, 2026' from an ISO date, or None. Built by hand (no locale, no %-d on Windows)."""
    s = _d10(v)
    if not s:
        return None
    d = date.fromisoformat(s)
    return '%s %d, %d' % (_MONTHS[d.month - 1], d.day, d.year)


def _regime(v):
    """Duty regime from a regime string or the legacy fob_line flag (truthy = none)."""
    if isinstance(v, str):
        s = v.strip().lower()
        return s if s in DUTY_REGIMES else 'us'
    return 'none' if v else 'us'


def _str_list(v):
    return isinstance(v, (list, tuple)) and all(isinstance(x, str) for x in v)


def _pos(v):
    f = _num(v, 0.0)
    return f if f > 0 else None


def _int(v):
    try:
        return int(float(v))
    except (TypeError, ValueError, OverflowError):
        return 0


def _u(v):
    return str(v or '').strip().upper()


def _d10(v):
    """'YYYY-MM-DD' from an ISO-ish string or None."""
    s = str(v or '').strip()
    if len(s) >= 10 and s[4] == '-' and s[7] == '-':
        try:
            date.fromisoformat(s[:10])
            return s[:10]
        except ValueError:
            return None
    return None


def base_of(s):
    """Base style: size suffix dropped. Costco program codes keep their dash (CU-..., CC-...)."""
    s = str(s or '').strip().upper().split(' ')[0]
    if s.startswith('CU-') or s.startswith('CC-'):
        return s
    return s.split('-')[0]


def norm_vd(b):
    return b[:2] + 'VD' + b[4:] if len(b) > 4 and b[2:4] == 'DV' else b


def fac_of(ref):
    """Factory code from a production ref (two letters + five digits)."""
    ref = _u(ref)
    m = _REF_RE.match(ref)
    if m:
        return m.group(1)
    if not ref:
        return 'BLANK'
    if ref == 'N/A':
        return 'N/A'
    return 'OTHER'


def _worst_level(levels):
    ls = [lv for lv in levels if lv in LEVEL_RANK]
    return max(ls, key=LEVEL_RANK.get) if ls else None


def _worst_routing(labels):
    ls = [lb for lb in labels if lb in _ROUTING_RANK]
    return max(ls, key=_ROUTING_RANK.get) if ls else None


def grade_of(level, routing=None):
    """Combined grade (DESIGN 5.7): the worse of the level's grade and the routing cap.
    A = R1 with L0-L2; B = R1/R2 with L3-L4 (and R2/R4 with a list price); C = R3/R5 or L5-L6;
    D = R6 or L7. No routing (stock rows) means the level alone decides."""
    g = LEVEL_INFO.get(level, ('', 'D'))[1] if level else 'D'
    cap = _ROUTING_CAP.get(routing, 'A') if routing else 'A'
    return max(g, cap)


# ── SKU decode ──
def decode_sku(sku, params=None):
    """Attributes of a SKU or base style, or None when it cannot be decoded (legacy SKUs).
    params: costbook params (programMap, customerGroupPrefix, fiberByFabric, fiberByStyle). Program
    codes (for example Costco bulk codes) decode only through params.programMap. fiberByStyle
    (exact base style -> cotton, mmf, linen or wool, from the factory's fabric content) beats the
    fabric-code default. Wrong-typed params are ignored, never raised on."""
    b = base_of(sku)
    p = params if isinstance(params, dict) else {}
    gp = p.get('customerGroupPrefix') if isinstance(p.get('customerGroupPrefix'), dict) else {}
    dg = gp.get('_default')
    default_group = dg if isinstance(dg, str) and dg else 'OTHER'
    fibers = dict(FIBER_BY_FABRIC)
    fbf = p.get('fiberByFabric')
    if isinstance(fbf, dict):
        fibers.update({k: v for k, v in fbf.items() if isinstance(k, str) and v in _FIBERS})
    pmap = p.get('programMap') if isinstance(p.get('programMap'), dict) else {}
    pm = pmap.get(b)
    if isinstance(pm, dict) and not all(v is None or isinstance(v, str) for v in pm.values()):
        pm = None
    if isinstance(pm, dict):
        fit = pm.get('fit')
        d = {'base': b, 'program': True, 'modern': False, 'cust': b[:2], 'brand': pm.get('brand'),
             'fab': pm.get('fab'), 'serial': None, 'fitCode': None, 'collar': None,
             'cat': pm.get('cat') or 'dress_shirt', 'fit': fit, 'sleeve': pm.get('sleeve') or 'LS',
             'pat': pm.get('pat'), 'group': pm.get('group') or default_group, 'src': pm.get('src')}
    elif MOD_SKU_RE.match(b):
        cust, brand, fab, serial, fitc, collar = b[:2], b[2:4], b[4:6], b[6:9], b[9:11], b[11:]
        brand = {'NT': 'NA', 'DV': 'VD'}.get(brand, brand)
        if (serial[0] == 'P' and serial[1:].isdigit()) or fab in BOTTOM_FABRICS:
            cat = 'pants'
        elif (serial[0] == 'B' and serial[1:].isdigit()) or fitc in ('BR', 'DB') or fab == 'KS':
            cat = 'blazer'
        elif serial[0] == 'V' and serial[1:].isdigit():
            cat = 'vest'
        elif fab in OVERSHIRT_FABRICS:
            cat = 'overshirt'
        elif fab in POLO_FABRICS or (collar[:1] and collar[:1] in POLO_COLLARS):
            cat = 'polo'
        elif fab in KNIT_FABRICS:
            cat = 'knit_shirt'
        else:
            cat = 'dress_shirt'
        fit = ('SLIM' if fitc in ('SL', 'SS') else 'REGULAR' if fitc in ('RF', 'SR') else
               'BIG_TALL' if fitc in BIG_TALL_FITS else 'MODERN' if fitc == 'MF' else
               'TAILORED' if fitc == 'TF' else None)
        if cat == 'pants':
            fit = None
        sleeve = 'SS' if (fitc in SHORT_SLEEVE_FITS and cat != 'pants') else 'LS'
        c0 = collar[:1]
        pat = ('YARN_DYED' if fab in YARN_DYE_FABRICS or c0 == 'Y' else
               'SOLID' if c0 and c0 in SOLID_COLLARS else 'PRINT' if c0 and c0 in PRINT_COLLARS else None)
        grp = gp.get(cust)
        d = {'base': b, 'program': False, 'modern': True, 'cust': cust, 'brand': brand, 'fab': fab,
             'serial': serial, 'fitCode': fitc, 'collar': collar or None, 'cat': cat, 'fit': fit,
             'sleeve': sleeve, 'pat': pat, 'group': grp if isinstance(grp, str) and grp else default_group,
             'src': None}
    else:
        return None
    d['fitClass'] = FIT_CLASS.get(d['fit'])
    d['fiber'] = fibers.get(d['fab'], 'mmf') if d['fab'] else 'mmf'
    fbs = p.get('fiberByStyle')
    if isinstance(fbs, dict) and fbs.get(b) in _FIBERS:
        d['fiber'] = fbs[b]
    return d


def fiber_group_of(content):
    """Fiber group of a fabric content (contract C11): {fiber name: percent} or [(name, percent)] ->
    'cotton' | 'linen' | 'wool' | 'mmf', or None when nothing is readable. The largest single share
    wins. A tie between a natural and a synthetic fiber counts as natural. Spandex or elastane decides
    only when it is the largest share on its own. Rayon, viscose, modal, lyocell and tencel are
    synthetic, like every fiber that is not cotton, linen or wool."""
    pairs = content.items() if isinstance(content, dict) else content if isinstance(content, (list, tuple)) else ()
    shares = []
    for pair in pairs:
        if not (isinstance(pair, (list, tuple)) and len(pair) == 2 and isinstance(pair[0], str)):
            continue
        p = _fnum(pair[1])
        if p is None or p <= 0:
            continue
        words = re.findall(r'[a-z]+', pair[0].lower())
        group = next((_NATURAL_WORDS[w] for w in words if w in _NATURAL_WORDS), 'mmf')
        shares.append((p, group, any(w in _STRETCH_WORDS for w in words)))
    if not shares:
        return None
    top = max(s[0] for s in shares)
    lead = [s for s in shares if s[0] == top]
    lead = [s for s in lead if not s[2]] or lead       # a stretch fiber decides only on its own
    return next((g for g in NATURAL_FIBERS if any(s[1] == g for s in lead)), 'mmf')


def fiber_class(fiber):
    """'natural' (cotton, linen or wool) or 'synthetic' (everything else, unknown included), contract
    C11. fiber: a fiber group as rows carry it, or a fabric content (see fiber_group_of)."""
    if isinstance(fiber, (dict, list, tuple)):
        fiber = fiber_group_of(fiber)
    return 'natural' if isinstance(fiber, str) and fiber.strip().lower() in NATURAL_FIBERS else 'synthetic'


# ── Settings ──
def _deep_merge(base, over):
    out = copy.deepcopy(base)
    if not isinstance(over, dict):
        return out
    for k, v in over.items():
        if isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = _deep_merge(out[k], v)
        else:
            out[k] = copy.deepcopy(v)
    return out


# Settings maps where a null value means "use the default" (contract C3). In these four the default
# is the engine's fallback rule, so the key is dropped: royalty.byBrand -> royalty.defaultPct,
# deductions.byCustomer -> the customer's group rate, factories -> factories._default,
# tariff.addonPct -> tariff.addonPct.OTHER (those two keys themselves go back to their defaults).
_NULL_TO_RULE = (('royalty', 'byBrand'), ('deductions', 'byCustomer'), ('factories',), ('tariff', 'addonPct'))
# In these the default is the public default value for that key (dropped when there is none).
_NULL_TO_DEFAULT = (('deductions', 'byGroup'), ('customerGroups',), ('destinations',), ('tariff', 'mfnPct'),
                    ('freight', 'oceanPerUnit'), ('freight', 'inlandPerUnit'))
_NULL_KEEP = {('factories',): '_default', ('tariff', 'addonPct'): 'OTHER'}


def _at(d, path):
    for k in path:
        d = d.get(k) if isinstance(d, dict) else None
    return d


def merge_settings(stored, params=None):
    """Effective settings: stored values over DEFAULT_SETTINGS (dicts merge key by key, lists
    and scalars replace). fx.rate null (or any value that is not a positive number) stays null: every
    price as printed on its own sheet (contract C10). In the settings maps a null value means 'use the
    default' (contract C3; see _NULL_TO_RULE and _NULL_TO_DEFAULT). The C11 to C14 blocks (landed,
    royalty.base, revenueCosts, regimeByCustomer, costRule, history) follow the same rule: a null or a
    value the engine cannot use gives the public default, and a saved list replaces the default list."""
    s = _deep_merge(DEFAULT_SETTINGS, stored if isinstance(stored, dict) else {})
    fx = s.get('fx') if isinstance(s.get('fx'), dict) else {}
    s['fx'] = fx
    fx['rate'] = _pos(fx.get('rate'))
    if str(fx.get('basis') or '') not in ('current_usd', 'after_cut'):
        fx['basis'] = 'current_usd'
    for k in ('tariff', 'freight', 'royalty', 'deductions', 'routing', 'bulk', 'confirmed', 'factories',
              'destinations', 'regimeByCustomer', 'landed', 'revenueCosts', 'costRule', 'history'):
        if not isinstance(s.get(k), dict):
            s[k] = copy.deepcopy(DEFAULT_SETTINGS[k])
    for k in ('addonPct', 'mfnPct'):
        if not isinstance(s['tariff'].get(k), dict):
            s['tariff'][k] = copy.deepcopy(DEFAULT_SETTINGS['tariff'][k])
    for k in ('oceanPerUnit', 'inlandPerUnit'):
        if not isinstance(s['freight'].get(k), dict):
            s['freight'][k] = copy.deepcopy(DEFAULT_SETTINGS['freight'][k])
    if not isinstance(s['royalty'].get('byBrand'), dict):
        s['royalty']['byBrand'] = {}
    for k in ('byGroup', 'byCustomer'):
        if not isinstance(s['deductions'].get(k), dict):
            s['deductions'][k] = {}
    if not isinstance(s.get('customerGroups'), dict):
        s['customerGroups'] = copy.deepcopy(DEFAULT_SETTINGS['customerGroups'])
    for path in _NULL_TO_RULE + _NULL_TO_DEFAULT:
        m = _at(s, path)
        if not isinstance(m, dict):
            continue
        dflt = _at(DEFAULT_SETTINGS, path) or {}
        keep = _NULL_KEEP.get(path)
        for k in [k for k, v in m.items() if v is None]:
            if (path in _NULL_TO_DEFAULT or k == keep) and k in dflt:
                m[k] = copy.deepcopy(dflt[k])
            else:
                del m[k]
    # destinations: upper-case codes and known regimes only. An unknown regime on a default code
    # keeps the default; on any other code it is dropped (that code is then a US import).
    dd, dest = DEFAULT_SETTINGS['destinations'], {}
    for k, v in s['destinations'].items():
        if not isinstance(k, str) or not k.strip():
            continue
        code = k.strip().upper()
        reg = v.strip().lower() if isinstance(v, str) else None
        if reg in DUTY_REGIMES:
            dest[code] = reg
        elif code in dd and code not in dest:
            dest[code] = dd[code]
    s['destinations'] = dest
    if _fnum(s['tariff'].get('caMfnPct')) is None:
        s['tariff']['caMfnPct'] = DEFAULT_SETTINGS['tariff']['caMfnPct']
    # regimeByCustomer (C12): upper-case customer codes and known regimes only. Any other entry (null
    # included) is dropped, so that customer falls through to the FOB list and the destinations.
    rbc = {}
    for k, v in s['regimeByCustomer'].items():
        reg = v.strip().lower() if isinstance(v, str) else None
        if isinstance(k, str) and k.strip() and reg in DUTY_REGIMES:
            rbc[k.strip().upper()] = reg
    s['regimeByCustomer'] = rbc
    # landed (C11): a known mode; each multiplier a number from 1 to 3, else null (not set).
    L = s['landed']
    if L.get('mode') not in LANDED_MODES:
        L['mode'] = DEFAULT_SETTINGS['landed']['mode']
    mm = L.get('multiplier') if isinstance(L.get('multiplier'), dict) else {}
    L['multiplier'] = {c: _mult_ok(mm.get(c)) for c in FIBER_CLASSES}
    if s['royalty'].get('base') not in ROYALTY_BASES:
        s['royalty']['base'] = DEFAULT_SETTINGS['royalty']['base']
    # revenueCosts (C11): the saved list replaces the default list. A percent that is null or not a
    # number from 0 to 50 counts as 0 (the default), so a bad value switches that item off.
    items = s['revenueCosts'].get('items')
    if not isinstance(items, list):
        items = copy.deepcopy(DEFAULT_SETTINGS['revenueCosts']['items'])
    clean = []
    for it in items:
        if not isinstance(it, dict):
            continue
        key = it.get('key') if isinstance(it.get('key'), str) else ''
        p = _fnum(it.get('pct'))
        clean.append({'key': key, 'name': it.get('name') if isinstance(it.get('name'), str) else key,
                      'pct': it.get('pct') if p is not None and 0 <= p <= _REV_COST_MAX else 0})
    s['revenueCosts']['items'] = clean
    # costRule (C13) and history (C14).
    cr = s['costRule']
    if cr.get('mode') not in COST_RULE_MODES:
        cr['mode'] = DEFAULT_SETTINGS['costRule']['mode']
    w = _fnum(cr.get('wideSpreadPct'))
    if w is None or not 0 <= w <= _SPREAD_MAX:
        cr['wideSpreadPct'] = DEFAULT_SETTINGS['costRule']['wideSpreadPct']
    cav = s['history'].get('caveat')
    s['history']['caveat'] = (cav.strip()[:_CAVEAT_MAX] or None) if isinstance(cav, str) else None
    return s


def _factory_origin(S, fac):
    f = (S.get('factories') or {})
    rec = f.get(fac) if fac not in _LIST_FACTORY_PSEUDO else None
    rec = rec if isinstance(rec, dict) else (f.get('_default') or {})
    return rec.get('origin') or 'OTHER'


def _factory_name(S, fac):
    names = {'MIX': 'Mixed supply', 'WH': 'Warehouse stock', 'UNKNOWN': 'Unknown factory',
             'N/A': 'No ref on the ledger', 'BLANK': 'Blank ref', 'OTHER': 'Other ref'}
    if fac in names:
        return names[fac]
    rec = (S.get('factories') or {}).get(fac)
    return (rec or {}).get('name') or fac


# ── DESIGN 5.6 math (the client repeats it; keep the operation order) ──
def _ca_mfn(T):
    v = T.get('caMfnPct')
    return _num(DEFAULT_SETTINGS['tariff']['caMfnPct'] if v is None else v)


def _mult_ok(v):
    """An import multiplier (contract C11): a number from 1 to 3, else None (not set)."""
    f = _fnum(v)
    return f if f is not None and _MULT_BAND[0] <= f <= _MULT_BAND[1] else None


def landed_multiplier(S, fiber):
    """The import multiplier a row uses (contract C11): in multiplier mode, settings.landed.multiplier of
    the row's fiber class. None in itemized mode, or when that class has no multiplier: the row then
    keeps the itemized duty, freight and fees (and the build raises 'settings_incomplete')."""
    L = S.get('landed') if isinstance(S, dict) else None
    if not isinstance(L, dict) or L.get('mode') != 'multiplier' or not isinstance(L.get('multiplier'), dict):
        return None
    return _mult_ok(L['multiplier'].get(fiber_class(fiber or 'mmf')))


def revenue_cost_pct(S):
    """revCostPct (contract C11): settings.revenueCosts.items[].pct summed in list order, starting at 0.
    An item whose percent is not a number from 0 to 50 adds nothing. The page sums the same way."""
    rc = S.get('revenueCosts') if isinstance(S, dict) else None
    items = rc.get('items') if isinstance(rc, dict) else None
    t = 0.0
    for it in (items if isinstance(items, list) else ()):
        p = _fnum(it.get('pct')) if isinstance(it, dict) else None
        if p is not None and 0 <= p <= _REV_COST_MAX:
            t += p
    return t


def royalty_base(S):
    """'net' (royalty on net sales, the default) or 'revenue' (contract C11)."""
    R = S.get('royalty') if isinstance(S, dict) else None
    return 'revenue' if isinstance(R, dict) and R.get('base') == 'revenue' else 'net'


def adders(fob, qty, cat, fiber, origin, S, regime):
    """(duty, freight, fees) for a row: fob in dollars (already rounded), qty in pieces.
    regime: 'us', 'ca' or 'none' (contract C2); the old fob_line flag still works (true = none).
    Multiplier mode (contract C11): a 'us' or 'ca' row pays duty = r2(fob x (m - 1)) and no freight or
    fees, m being the multiplier of its fiber class. A class with no multiplier keeps the math below."""
    reg = _regime(regime)
    if reg == 'none' or fob is None:
        return 0.0, 0.0, 0.0
    m = landed_multiplier(S, fiber)
    if m is not None:
        return r2(fob * (m - 1)), 0.0, 0.0         # one import cost: tariffs, freight and fees
    g = CAT_GROUP.get(cat, 'shirt')
    fiber = fiber or 'mmf'
    T, F = S['tariff'], S['freight']
    if reg == 'ca':
        # Canada: Canadian MFN on FOB only. No US add-on, no US specific duty, no MPF or HMF.
        duty = r2(fob * _ca_mfn(T) / 100)
        freight = r2(qty * _num((F.get('oceanPerUnit') or {}).get(g)))
        fees = r2(qty * _num((F.get('inlandPerUnit') or {}).get(g)))
        return duty, freight, fees
    mfn_map, add_map = T.get('mfnPct') or {}, T.get('addonPct') or {}
    mfn = mfn_map.get(g + '_' + fiber)
    if mfn is None:
        mfn = mfn_map.get('shirt_mmf')
    mfn = _num(mfn)
    addon = add_map.get(origin) if origin is not None else None
    if addon is None:
        addon = add_map.get('OTHER')
    addon = _num(addon)
    spec = qty * _num(T.get('mmfShirtSpecificPerUnit')) if (g == 'shirt' and fiber == 'mmf') else 0
    ocean = _num((F.get('oceanPerUnit') or {}).get(g))
    inland = _num((F.get('inlandPerUnit') or {}).get(g))
    duty = r2(fob * (mfn + addon) / 100 + spec)
    freight = r2(qty * ocean)
    fees = r2(fob * (_num(T.get('mpfPct')) + _num(T.get('hmfPct'))) / 100 + qty * inland)
    return duty, freight, fees


def landed(fobU, attrs, settings, regime, origin):
    """Per-unit landed cost: {duty, freight, fees, landedU}. attrs: {'cat', 'fiber'}.
    regime: 'us', 'ca' or 'none' (the old fob_line flag still works)."""
    if fobU is None:
        return {'duty': None, 'freight': None, 'fees': None, 'landedU': None}
    S = settings if isinstance(settings, dict) and 'tariff' in settings else merge_settings(settings)
    reg = _regime(regime)
    if reg == 'none':
        return {'duty': 0.0, 'freight': 0.0, 'fees': 0.0, 'landedU': r4(fobU)}
    m = landed_multiplier(S, (attrs or {}).get('fiber'))
    if m is not None:
        # Multiplier mode (C11): the unit cost is rounded to 4 decimals first (C2), then the duty, so the
        # page gets the same landedU from the published fobU.
        u = r4(fobU)
        duty = r4(u * (m - 1))
        return {'duty': duty, 'freight': 0.0, 'fees': 0.0, 'landedU': r4(u + duty)}
    g = CAT_GROUP.get((attrs or {}).get('cat'), 'shirt')
    fiber = (attrs or {}).get('fiber') or 'mmf'
    T, F = S['tariff'], S['freight']
    if reg == 'ca':
        duty = fobU * _ca_mfn(T) / 100
        freight = _num(F['oceanPerUnit'].get(g))
        fees = _num(F['inlandPerUnit'].get(g))
        return {'duty': r4(duty), 'freight': r4(freight), 'fees': r4(fees), 'landedU': r4(fobU + duty + freight + fees)}
    mfn = T['mfnPct'].get(g + '_' + fiber)
    mfn = _num(T['mfnPct'].get('shirt_mmf') if mfn is None else mfn)
    addon = T['addonPct'].get(origin) if origin is not None else None
    addon = _num(T['addonPct'].get('OTHER') if addon is None else addon)
    spec = _num(T.get('mmfShirtSpecificPerUnit')) if (g == 'shirt' and fiber == 'mmf') else 0.0
    duty = fobU * (mfn + addon) / 100 + spec
    freight = _num(F['oceanPerUnit'].get(g))
    fees = fobU * (_num(T.get('mpfPct')) + _num(T.get('hmfPct'))) / 100 + _num(F['inlandPerUnit'].get(g))
    return {'duty': r4(duty), 'freight': r4(freight), 'fees': r4(fees), 'landedU': r4(fobU + duty + freight + fees)}


def _cust_group(cust, S, fob_set):
    g = (S.get('customerGroups') or {}).get(cust)
    if g:
        return g
    return 'fob' if cust in fob_set else 'other'


def _ded_pct(cust, S, group):
    bc, bg = S['deductions'].get('byCustomer') or {}, S['deductions'].get('byGroup') or {}
    if bc.get(cust) is not None and math.isfinite(_num(bc.get(cust), float('nan'))):
        return _num(bc[cust])
    if bg.get(group) is not None:
        return _num(bg[group])
    return _num(bg.get('other'))


def _roy_pct(brand, S):
    R = S['royalty']
    bb = R.get('byBrand') or {}
    v = bb.get(brand) if brand is not None else None
    if v is not None and math.isfinite(_num(v, float('nan'))):
        return _num(v)
    return _num(R.get('defaultPct'))


def line_money(rev, fob, qty, cat, fiber, origin, regime, ded_pct, roy_pct, S):
    """One revenue row (DESIGN 5.6, contract C11). rev None = unpriced (APO). fob None = uncosted (L7).
    regime: 'us', 'ca' or 'none' (the old fob_line flag still works). Royalty is a percent of net sales
    or of revenue (settings.royalty.base). revCost = r2(rev x revCostPct / 100) whenever rev is set,
    and contrib = r2(gp - royalty - revCost). The page repeats this operation order."""
    out = {'duty': None, 'freight': None, 'fees': None, 'cogs': None, 'deduct': None, 'net': None,
           'gp': None, 'royalty': None, 'revCost': None, 'contrib': None}
    if fob is not None:
        d, f, e = adders(fob, qty, cat, fiber, origin, S, regime)
        out.update(duty=d, freight=f, fees=e, cogs=r2(fob + d + f + e))
    if rev is not None:
        out['deduct'] = r2(rev * ded_pct / 100)
        out['net'] = r2(rev - out['deduct'])
        out['revCost'] = r2(rev * revenue_cost_pct(S) / 100)
        if fob is not None:
            out['gp'] = r2(out['net'] - out['cogs'])
            base = rev if royalty_base(S) == 'revenue' else out['net']
            out['royalty'] = r2(base * roy_pct / 100)
            out['contrib'] = r2(out['gp'] - out['royalty'] - out['revCost'])
    return out


def stock_money(fob, units, cat, fiber, origin, regime, S):
    """Inventory and production rows: (landed, landedU). regime as in adders."""
    if fob is None:
        return None, None
    d, f, e = adders(fob, units, cat, fiber, origin, S, regime)
    lnd = r2(fob + d + f + e)
    return lnd, (r4(lnd / units) if units > 0 else None)


def fx_what_if_unit(fobU, fx_share, fx_ref, rate_new, rate_fallback=None):
    """The page's RMB what-if on one published unit cost (DESIGN 5.6, contract C10):
        r4(fobU x (1 - fxShare + fxShare x fxRef / rateNew))
    fx_ref is the row's fxRef. Rows of datasets built before fxRef have none, so rate_fallback
    (settings.fx.rate, then params.fxBase) stands in. The unit cost comes back unchanged when the row
    has no RMB-based part or a rate is missing. The page repeats the same operation order."""
    u = _fnum(fobU)
    if u is None:
        return None
    s = _fnum(fx_share) or 0.0
    ref = _pos(fx_ref) or _pos(rate_fallback)
    rn = _pos(rate_new)
    if s <= 0 or not ref or not rn:
        return fobU
    return r4(u * (1 - s + s * ref / rn))


def _ref_mix(parts):
    """Value-weighted rate of RMB-based parts: [(dollar value of the RMB-based part, its rate)] -> rate
    or None. It is the rate at which the parts' RMB amounts equal their dollar value, so the page's
    what-if (fxShare x fxRef / rateNew) reprices every part at the new rate."""
    v = a = 0.0
    for x, e in parts:
        if x and e:
            v += x
            a += x * e
    return a / v if v else None


def _fxref_out(share, fxr):
    """fxRef as published: 4 decimals, only on rows with an RMB-based share."""
    return r4(fxr) if share and fxr else None

# ── Cost index: the cost-resolution cascade L0-L7 (port of study synth/cascade.py) ──
def _fnum(v):
    """A finite float from an int or float, else None (never raises: huge ints give None)."""
    if isinstance(v, bool) or not isinstance(v, (int, float)):
        return None
    try:
        f = float(v)
    except (OverflowError, ValueError):
        return None
    return f if math.isfinite(f) else None


# ── Cost-book price fields by role (contract C1) ──
_ROLE_ORDER = ('usd', 'base', 'cut', 'rmb')
_ROLE_DEFAULTS = {'usd': 'price_usd', 'base': 'price_usd_base', 'cut': 'price_usd_cut', 'rmb': 'rmb_price',
                  'e1': 'fx_sheet_e1'}


def price_field_roles(costbook):
    """{usd, base, cut, rmb, e1} -> the cost book's field name for that role. From
    meta.priceFieldRoles, else the order of meta.priceFields (usd, base, cut, rmb), else the
    role-neutral names. e1 (the sheet's printed rate) comes from meta.priceFieldRoles or its
    role-neutral name. A record without the usd field falls back to the base field, and one without
    the cut field to its price."""
    cb = costbook if isinstance(costbook, dict) else {}
    meta = cb.get('meta') if isinstance(cb.get('meta'), dict) else {}
    roles = dict(_ROLE_DEFAULTS)
    pr = meta.get('priceFieldRoles')
    if isinstance(pr, dict) and isinstance(pr.get('e1'), str) and pr.get('e1'):
        roles['e1'] = pr['e1']
    if isinstance(pr, dict) and any(isinstance(pr.get(k), str) and pr.get(k) for k in _ROLE_ORDER):
        for k in _ROLE_ORDER:
            v = pr.get(k)
            if isinstance(v, str) and v:
                roles[k] = v
            elif k in ('base', 'cut'):
                roles[k] = None
        return roles
    pf = meta.get('priceFields')
    names = []
    if isinstance(pf, dict):
        names = [k for k in pf if isinstance(k, str) and k]
    elif isinstance(pf, (list, tuple)):
        for x in pf:
            n = x if isinstance(x, str) else (x.get('field') or x.get('name')) if isinstance(x, dict) else None
            if isinstance(n, str) and n:
                names.append(n)
    if names:
        for i, k in enumerate(_ROLE_ORDER):
            if i < len(names):
                roles[k] = names[i]
            elif k in ('base', 'cut'):
                roles[k] = None
    return roles


# ── Cost-book params, type checked (engine backstop for stored or restored cost books) ──
_PARAM_KEYS = ('fxBase', 'ssDelta', 'fitPremium', 'gridPrecedence', 'poolOrder', 'programMap', 'kitPcs',
               'customerGroupPrefix')
_ADJ_MAX = 20.0          # per-unit dollar adjustments (fit premium, short sleeve delta): a wide sanity band
_KIT_MAX = 1000
_PROGRAM_FIELDS = ('brand', 'fab', 'fit', 'cat', 'sleeve', 'pat', 'group', 'src')


def _adj_ok(v):
    f = _fnum(v)
    return v is None or (f is not None and abs(f) <= _ADJ_MAX)


def clean_params(params):
    """(clean, missing, invalid) for costbook['params']. clean keeps only well-typed values, so the
    cascade can never raise on a bad param; missing lists absent required keys; invalid lists keys
    that are present but wrong (their rule is switched off, as if missing). Never raises."""
    if params is None:
        p, invalid = {}, []
    elif isinstance(params, dict):
        p, invalid = params, []
    else:
        p, invalid = {}, ['params']
    out = dict(p)
    missing = [k for k in _PARAM_KEYS if k not in p]

    def bad(k):
        if k not in invalid:
            invalid.append(k)
    if 'fxBase' in p:
        f = _fnum(p['fxBase'])
        if f is None or f <= 0:
            out.pop('fxBase', None)
            bad('fxBase')
    if 'ssDelta' in p and not _adj_ok(p['ssDelta']):
        out['ssDelta'] = None
        bad('ssDelta')
    if 'fitPremium' in p:
        fp = p['fitPremium']
        if not isinstance(fp, dict):
            out.pop('fitPremium', None)
            bad('fitPremium')
        else:
            clean = {}
            for k in ('regular', 'bigTall'):
                if _adj_ok(fp.get(k)):
                    clean[k] = fp.get(k)
                else:
                    clean[k] = None
                    bad('fitPremium')
            out['fitPremium'] = clean
    if 'gridPrecedence' in p:
        g = p['gridPrecedence']
        if not isinstance(g, dict):
            out.pop('gridPrecedence', None)
            bad('gridPrecedence')
        else:
            clean = {k: list(v) for k, v in g.items() if isinstance(k, str) and _str_list(v)}
            if len(clean) != len(g):
                bad('gridPrecedence')
            out['gridPrecedence'] = clean
    if 'poolOrder' in p:
        po = p['poolOrder']
        if not isinstance(po, dict):
            out.pop('poolOrder', None)
            bad('poolOrder')
        else:
            clean = {}
            for k, v in po.items():
                ok = isinstance(k, str)
                if ok and k in ('all', 'recordKinds', 'excludeFlags'):
                    ok = _str_list(v)
                elif ok and k == 'rule':
                    ok = v is None or isinstance(v, str)
                elif ok and k == 'polo':
                    ok = isinstance(v, dict) and all(_str_list(v.get(x) or []) for x in ('primary', 'fallback'))
                elif ok:
                    ok = (isinstance(v, dict) and _str_list(v.get('base') or [])
                          and isinstance(v.get('byGroup') or {}, dict)
                          and all(isinstance(gk, str) and _str_list(gv) for gk, gv in (v.get('byGroup') or {}).items()))
                if ok:
                    clean[k] = v
                else:
                    bad('poolOrder')
            out['poolOrder'] = clean
    if 'programMap' in p:
        pm = p['programMap']
        if not isinstance(pm, dict):
            out.pop('programMap', None)
            bad('programMap')
        else:
            clean = {k: v for k, v in pm.items() if isinstance(k, str) and isinstance(v, dict)
                     and all(v.get(f) is None or isinstance(v.get(f), str) for f in _PROGRAM_FIELDS)}
            if len(clean) != len(pm):
                bad('programMap')
            out['programMap'] = clean
    if 'kitPcs' in p:
        kp = p['kitPcs']
        if not isinstance(kp, dict):
            out.pop('kitPcs', None)
            bad('kitPcs')
        else:
            clean = {}
            for k, v in kp.items():
                f = _fnum(v)
                if isinstance(k, str) and f is not None and f == int(f) and 1 <= f <= _KIT_MAX:
                    if f > 1:
                        clean[k] = int(f)
                else:
                    bad('kitPcs')
            out['kitPcs'] = clean
    for key in ('customerGroupPrefix',):
        if key in p:
            v = p[key]
            if not isinstance(v, dict):
                out.pop(key, None)
                bad(key)
            else:
                clean = {k: x for k, x in v.items() if isinstance(k, str) and isinstance(x, str) and x}
                if len(clean) != len(v):
                    bad(key)
                out[key] = clean
    for key in ('fiberByFabric', 'fiberByStyle'):
        if key in p and p[key] is not None:
            v = p[key]
            if not isinstance(v, dict):
                out.pop(key, None)
                bad(key)
            else:
                clean = {k.strip().upper(): x for k, x in v.items() if isinstance(k, str) and x in _FIBERS}
                if len(clean) != len(v):
                    bad(key)
                out[key] = clean
    return out, missing, invalid


# Record fields the cascade uses as keys or in comparisons: text or null. List fields: text lists.
_REC_TEXT = ('source_code', 'record_kind', 'factory_code', 'production_ref', 'production_ref_resolved', 'style',
             'brand_code', 'category', 'fabric_code_confidence', 'sleeve', 'fit_class', 'pattern',
             'pattern_effective', 'pool')
_REC_LISTS = ('fabric_codes', 'flags', 'variant_tags', 'brand_codes', 'names_styles')


def _rec_ok(r):
    if not isinstance(r, dict) or not isinstance(r.get('id'), str) or not r['id']:
        return False
    if any(r.get(k) is not None and not isinstance(r.get(k), str) for k in _REC_TEXT):
        return False
    return all(r.get(k) is None or _str_list(r.get(k)) for k in _REC_LISTS)


def _rec_brands(r):
    """The SKU brand codes a record's brand section serves (contract C15 R1): the cost book's brand_codes, else
    its brand_code, else none."""
    bc = frozenset(_u(x) for x in (r.get('brand_codes') or ()) if x)
    return bc or (frozenset([_u(r['brand_code'])]) if r.get('brand_code') else frozenset())


def _brand_ok(brands, brand):
    """A list row serves a style's brand when it names that brand, or names none (contract C15 R1)."""
    return not brands or brand in brands


def program_brand(base):
    """Brand code of a program or legacy code (contract C7): the two characters after 'CU-'/'CC-'
    or after a two-letter customer prefix, when they are a known brand code. Else None."""
    s = str(base or '').strip().upper()
    if s.startswith('CU-') or s.startswith('CC-'):
        cand = s[3:5]
    elif len(s) >= 4 and s[:2].isalpha():
        cand = s[2:4]
    else:
        return None
    cand = {'NT': 'NA', 'DV': 'VD'}.get(cand, cand)
    return cand if cand in BRAND_NAMES and cand != 'BLK' else None


def _pool_of(r):
    """Calculator pool for cost books whose records carry no 'pool' (the study table)."""
    sc, g = r.get('source_code'), str(r.get('customer_group') or '').upper()
    if sc == 'NF-TJX':
        return 'NF-TJX:TJX'
    if sc == 'NF-OC':
        return 'NF-OC:' + {'KOHL': 'KOHL', 'BJS': 'BJS', 'AMAZON': 'AMAZON'}.get(g, 'NORHAM')
    if sc == 'NF-KNIT':
        return 'NF-KNIT:ANY'
    if sc == 'YW-SM':
        return 'YW-SM:' + ('COSTCO' if g.startswith('COSTCO') else g if g in ('WALMART', 'BJS') else 'ALFA')
    if sc == 'YW-REG':
        return 'YW-REG:REGULAR'
    if sc == 'YW-TC':
        return 'YW-TC:BJS' if 'BJ' in g else 'YW-TC:REGULAR'
    if sc == 'YW-HP':
        return 'YW-HP:HALF'
    return None


_POOL_LABEL = {'NF-TJX': 'NEW FACTORIES calculator, TJX sheet', 'NF-OC': 'NEW FACTORIES calculator, other customer sheet',
               'NF-KNIT': 'NEW FACTORIES calculator, knit sheet', 'YW-REG': 'YIWU calculator, regular customer sheet',
               'YW-TC': 'YIWU calculator, TC stretch sheet', 'YW-SM': 'YIWU calculator, supermarket sheet',
               'YW-HP': 'Half Price quotation'}


def _pool_label(pool):
    head, _, grp = str(pool or '').partition(':')
    lab = _POOL_LABEL.get(head, head or 'calculator')
    return lab + (' (%s block)' % grp.title() if head in ('NF-OC', 'YW-SM') and grp else '')


def _R(level, price, basis, ids=(), fac=None, alt=None, rng=None, fx=0.0, fxr=None, flags=(), row=None, skip=()):
    """A resolution. fx: the share of price that is RMB based (value share; a negative dollar step
    can put it a little above 1). fxr: the rate that RMB-based part stands at, or None. row: (record id, fit
    column) of the one row the price comes from (contract C15), or None. skip: ((record id, reason), ...), the
    rows the rule set aside. A price from another brand's row (flag cross_brand) caps the grade at D (gcap)."""
    flags = tuple(flags)
    out = {'level': level, 'price': price, 'alt': price if alt is None else alt, 'basis': basis,
           'ids': tuple(ids), 'fac': fac, 'rng': rng, 'fx': fx, 'fxr': fxr if fx else None, 'flags': flags,
           'row': row, 'skip': tuple(skip)}
    if 'cross_brand' in flags:
        out['gcap'] = 'D'
    return out


def _lowest(items):
    """Contract C15 R7 on list prices: [(price, record id)] -> (price, record id, flags, range, skip). The lowest
    price wins (then the lowest id); more than one row is flagged ambiguous_rows and the others are listed."""
    s = sorted(items, key=lambda x: (x[0], x[1]))
    if len(s) == 1:
        return s[0][0], s[0][1], (), None, ()
    return (s[0][0], s[0][1], ('ambiguous_rows',), (s[0][0], s[-1][0]),
            tuple((x[1], 'tie_not_lowest') for x in s[1:]))


def _wmedian(items):
    """Weighted median of [(value, weight, fxShare, fxRef)] -> (value, fxShare, fxRef)."""
    items = sorted(items, key=lambda t: t[:3])
    tot = sum(t[1] for t in items)
    acc = 0
    for v, w, fx, fxr in items:
        acc += w
        if acc >= tot / 2:
            return v, fx, fxr
    return None


class CostIndex:
    """The cost book indexed for the cascade. ledger_rows: the current style ledger (L5 list
    siblings and the L6 defaults are computed from it). self.ledger holds one prepared entry per
    ledger row (same index) with its resolution in 'res' (the row's own factory, the ladder). history_rows:
    optional past ledger lines; with the ledger they say which refs carry each style (contract C13)."""

    def __init__(self, costbook, settings=None, overrides=None, ledger_rows=None, today=None, history_rows=None):
        cb = costbook if isinstance(costbook, dict) else {}
        # Params are type checked here, so a stored or restored cost book with a bad param switches
        # that rule off (and the dataset says so) instead of failing the build.
        p, self.missing_params, self.invalid_params = clean_params(cb.get('params'))
        self.params = p
        self.S = S = merge_settings(settings, p)
        self.today = _d10(today)
        roles = price_field_roles(cb)
        self._f_usd, self._f_base, self._f_cut = roles['usd'], roles['base'], roles['cut']
        self._f_rmb, self._f_e1 = roles.get('rmb'), roles.get('e1')
        # Price basis (contract C10): the Current USD printed on each sheet. fx_rate None keeps every
        # price as printed; a saved rate reprices each RMB-based record at printed x (sheet rate / rate).
        # fx_base (the cost book's own base rate) only helps find a sheet rate a record does not carry.
        self.fx_base = _pos(p.get('fxBase'))
        self.fx_rate = _pos(S['fx'].get('rate'))
        self.after_cut = S['fx'].get('basis') == 'after_cut'
        self._rate_cache = {}
        self.ss_delta = _fnum(p.get('ssDelta'))
        fp = p.get('fitPremium') if isinstance(p.get('fitPremium'), dict) else {}
        self.reg_add, self.bt_add = _fnum(fp.get('regular')), _fnum(fp.get('bigTall'))
        grid = dict(p.get('gridPrecedence') or {})
        sg = S.get('gridPrecedence')
        if isinstance(sg, dict):
            grid.update({k: list(v) for k, v in sg.items() if isinstance(k, str) and _str_list(v)})
        self.grid = grid
        self.pool_cfg = p.get('poolOrder') if isinstance(p.get('poolOrder'), dict) else {}
        self.kit_pcs = {_u(k): _int(v) for k, v in (p.get('kitPcs') or {}).items() if _int(v) > 1}
        raw = cb.get('records') if isinstance(cb.get('records'), (list, tuple)) else []
        recs = [r for r in raw if _rec_ok(r)]
        self.bad_records = sum(1 for r in raw if isinstance(r, dict)) - len(recs)
        self.records = {r['id']: r for r in recs}
        # The rates the calculator sheets print, and the RMB-based records whose rate cannot be found
        # (those keep their printed price under a saved rate or a what-if).
        self.sheet_rates, self.unrated = Counter(), 0
        for r in recs:
            if r.get('record_kind') == 'calculator':
                e = self.rec_rate(r)
                if e:
                    self.sheet_rates[r4(e)] += 1
                elif self.rec_usd(r) is not None:
                    self.unrated += 1
        self.conflicts = {}
        self.conflict_of = defaultdict(list)
        for g in (cb.get('conflict_groups') if isinstance(cb.get('conflict_groups'), (list, tuple)) else []):
            if isinstance(g, dict) and isinstance(g.get('id'), str) and g['id']:
                self.conflicts[g['id']] = g
                for m in (g.get('members') if isinstance(g.get('members'), (list, tuple)) else []):
                    if isinstance(m, str):
                        self.conflict_of[m].append(g['id'])
        self._dec, self._cache = {}, {}
        self._build_lists(recs)
        self._build_calc(recs)
        self._build_overrides(overrides)
        self.list_style, self.list_design, self.defaults = defaultdict(list), defaultdict(list), {}
        self.ledger = self._prepare_ledger(ledger_rows or [])
        self._pass1()
        self._pass2()
        # Contract C13: the cost rule, and the refs that carry each style (the ledger, then its history).
        cr = S.get('costRule') if isinstance(S.get('costRule'), dict) else {}
        self.rule_mode = cr.get('mode') if cr.get('mode') in COST_RULE_MODES else 'combined'
        w = _fnum(cr.get('wideSpreadPct'))
        self.wide_pct = w if w is not None and 0 <= w <= _SPREAD_MAX else DEFAULT_SETTINGS['costRule']['wideSpreadPct']
        self.carry = self._carriers(history_rows)
        self._quotes, self._comb = {}, {}

    # ── decode / helpers ──
    def decode(self, b):
        if b not in self._dec:
            self._dec[b] = decode_sku(b, self.params)
        return self._dec[b]

    def rec_usd(self, r):
        """The Current USD printed on the record's own sheet (the 'usd' role)."""
        return _fnum(r.get(self._f_usd))

    def rec_rate(self, r):
        """The RMB rate printed on an RMB-based (calculator) record's own sheet (the 'e1' role), else
        None. A record that does not carry it: the rate its base-role price was restated from
        (params.fxBase x base / usd), else its RMB amount / its printed USD."""
        if r.get('record_kind') != 'calculator':
            return None
        rid = r.get('id')
        if rid in self._rate_cache:
            return self._rate_cache[rid]
        e = _fnum(r.get(self._f_e1)) if self._f_e1 else None
        if e is None or e <= 0:
            e = None
            u = self.rec_usd(r)
            if u is not None and u > 0:
                b = _fnum(r.get(self._f_base)) if self._f_base else None
                m = _fnum(r.get(self._f_rmb)) if self._f_rmb else None
                if b is not None and b > 0 and self.fx_base:
                    e = self.fx_base * b / u
                elif m is not None and m > 0:
                    e = m / u
        self._rate_cache[rid] = e
        return e

    def rec_price(self, r):
        """The price used for a record (contract C10): the Current USD printed on its sheet, for its
        fit (the 'usd' role; the 'base' role only when a record has no usd value). With a saved RMB
        rate, an RMB-based record prices at printed x (its sheet rate / the saved rate), which is its
        RMB amount at that rate. List prices and USD quotations never move."""
        p = self.rec_usd(r)
        if p is None and self._f_base:
            p = _fnum(r.get(self._f_base))
        if p is None:
            return None
        if self.fx_rate:
            e = self.rec_rate(r)
            if e:
                return p * e / self.fx_rate
        return p

    def sheet_price(self, r):
        """The price a record's sheet prints (the 'usd' role, else the 'base' role), whatever the saved rate or
        basis: the price styles.costRow and costRowsSkipped show (contract C15)."""
        p = self.rec_usd(r)
        return _fnum(r.get(self._f_base)) if p is None and self._f_base else p

    def rec_ref(self, r):
        """fxRef of a record: the rate its price stands at (the saved rate, else its sheet rate).
        None for a USD price (lists, quotations) or a record whose rate cannot be found."""
        e = self.rec_rate(r)
        return (self.fx_rate or e) if e else None

    def rec_cut(self, r):
        """The after-cut reading (the 'cut' role), or None."""
        return _fnum(r.get(self._f_cut)) if self._f_cut else None

    def eff(self, res):
        """(unit cost used, RMB-based share of it) for a resolution. Candidate prices already stand
        at the P&L's basis (printed, or the saved rate), so nothing is scaled here."""
        p = res.get('price')
        if p is None:
            return None, 0.0
        if self.after_cut:
            return res.get('alt', p), 0.0
        return p, res.get('fx') or 0.0

    def fx_ref(self, res):
        """fxRef of a resolution: the rate its RMB-based part stands at, or None."""
        if self.after_cut or res.get('price') is None or not res.get('fx'):
            return None
        return res.get('fxr')

    def eff_range(self, res):
        rng = res.get('rng')
        if not rng or res.get('price') is None:
            return None, None
        lo, hi = rng
        if self.after_cut:
            k = (res.get('alt') or 0) / res['price'] if res['price'] else 1.0
            return lo * k, hi * k
        return lo, hi

    def public(self, res):
        u, fx = self.eff(res)
        lo, hi = self.eff_range(res)
        share = r4(fx) if u is not None else 0.0
        return {'fobU': r4(u), 'level': res['level'], 'basis': res['basis'], 'evidence': list(res['ids']),
                'rangeLo': r4(lo), 'rangeHi': r4(hi), 'factory': res['fac'], 'flags': list(res['flags']),
                'fxShare': share, 'fxRef': _fxref_out(share, self.fx_ref(res))}

    def resolve(self, style, factory=None, ref=None, poName=None, customer_group=None, brand_label=None):
        """DESIGN 3.6: cost of one style from one factory (ref and poName when known)."""
        ref = _u(ref) or None
        fac = _u(factory) or fac_of(ref)
        return self.public(self.raw(base_of(style), fac, ref, poName, customer_group=customer_group,
                                    brand_label=brand_label))

    # ── indexes ──
    def _build_lists(self, recs):
        self.pc_exact, self.pc_style = {}, defaultdict(list)
        self.pc_design, self.pc_fcs = defaultdict(list), defaultdict(list)
        self.dp_ref, self.dp_rate = defaultdict(list), defaultdict(list)
        self.ky_ref, self.ky_rate = {}, defaultdict(list)
        self.pc_facs, self.dp_facs, self.ky_facs = set(), set(), set()
        self.dp_fabs, self.ky_fabs = set(), set()
        # A customer quotation that names one exact style with its price in a picture on the sheet (record kind
        # image_callout): that style's own quote, {style: (price, record id, factory)} (contract C15 R0).
        self.img_style = {}
        for r in recs:
            sc, pr = r.get('source_code'), self.rec_price(r)
            if pr is not None and r.get('record_kind') == 'image_callout' and _u(r.get('style')):
                self.img_style[norm_vd(_u(r.get('style')))] = (pr, r['id'], _u(r.get('factory_code')) or 'UNKNOWN')
                continue
            if pr is None or r.get('record_kind') != 'ref_price_list':
                continue
            if sc == 'PC':
                st = _u(r.get('style'))
                ref = _u(r.get('production_ref'))
                if not st:
                    continue
                self.pc_facs.add(_u(r.get('factory_code')) or fac_of(ref))
                self.pc_exact[(ref, st)] = (pr, r['id'])
                stn = norm_vd(st)
                self.pc_style[stn].append((pr, r['id'], ref))
                d = self.decode(stn)
                if d:
                    self.pc_design[stn[2:]].append((pr, r['id'], ref))
                    # brand, pattern and price fit of the listed style: the fabric rung takes one row of the style's
                    # own brand, pattern and fit column (contract C15 R1, R4, R6).
                    for code in (r.get('fabric_codes') or [d['fab']]):
                        self.pc_fcs[(code, d['cat'], d['sleeve'] if d['cat'] != 'pants' else '-')].append(
                            (pr, r['id'], d['brand'], d['pat'], price_fit(d['fit'])))
            elif sc == 'DP':
                ref = _u(r.get('production_ref_resolved'))
                self.dp_facs.add(_u(r.get('factory_code')) or fac_of(ref))
                self.dp_fabs.update(r.get('fabric_codes') or [])
                self.dp_ref[(ref, r.get('pattern'))].append((pr, r['id']))
                amazon = 'AMAZON' in str(r.get('customer_group') or '').upper()
                self.dp_rate[(r.get('pattern'), r.get('sleeve') or 'LS', amazon)].append((pr, r['id'], _rec_brands(r)))
            elif sc == 'KY':
                ref = _u(r.get('production_ref_resolved'))
                self.ky_facs.add(_u(r.get('factory_code')) or fac_of(ref))
                self.ky_fabs.update(r.get('fabric_codes') or [])
                self.ky_ref[(ref, r.get('pattern'), r.get('fit_class'))] = (pr, r['id'])
                amazon = 'AMAZON' in (str(r.get('customer_group') or '') + str(r.get('fabrication') or '')).upper()
                self.ky_rate[(r.get('pattern'), r.get('fit_class'), amazon)].append((pr, r['id'], _rec_brands(r)))
        self.dp_refs = {k[0] for k in self.dp_ref}
        self.ky_refs = {k[0] for k in self.ky_ref}
        # Contract C13: the Pinnacle-type list quotes of each style (DV brand letters fold to VD), one per ref.
        self.pc_by_style = defaultdict(list)
        for (ref, st), (pr, rid) in self.pc_exact.items():
            self.pc_by_style[norm_vd(st)].append((ref, pr, rid))

    def _carriers(self, history_rows):
        """{base: {factory: {ref: poName}}}: the production refs that carry each style on the current ledger,
        then on the ledger history when there is one (contract C13). Blank, N/A and other refs that name no
        factory are left out."""
        rows = [(L['b'], L['ref'], L['poName']) for L in self.ledger]
        for h in history_rows or ():
            if isinstance(h, dict):
                rows.append((base_of(h.get('style')), _u(h.get('production') or h.get('ref')),
                             str(h.get('poName') or h.get('po_name') or '').strip()))
        out = {}
        for b, ref, pn in rows:
            fac = fac_of(ref)
            if b and ref and fac not in _LIST_FACTORY_PSEUDO:
                out.setdefault(b, {}).setdefault(fac, {}).setdefault(ref, pn)
        return out

    def _build_calc(self, recs):
        cfg = self.pool_cfg
        kinds = set(cfg.get('recordKinds') or ('calculator', 'factory_quotation'))
        excl = set(cfg.get('excludeFlags') or ())
        # calc_a: the calculator rows of each pool (sheet) by every fabric code they carry. pool_brands: the SKU
        # brand codes each pool has a section for (contract C15 R1).
        self.calc_a, self.pool_brands = defaultdict(list), defaultdict(set)
        for r in recs:
            if r.get('record_kind') not in kinds or excl.intersection(r.get('flags') or ()):
                continue
            pr = self.rec_price(r)
            pool = r.get('pool') or _pool_of(r)
            if pr is None or not pool:
                continue
            alt = self.rec_cut(r)
            ref = self.rec_ref(r)
            codes = [c for c in (r.get('fabric_codes') or []) if c]
            prim = r.get('fabric_code_primary')
            brands = _rec_brands(r)
            # price: the printed Current USD, or its RMB amount at the saved rate (contract C10). rmb and
            # ref: an RMB-based record and the rate its price stands at (fxRef). Contract C15: brands (the SKU
            # brand codes its section serves), generic (an all-brand section), primary (its first fabric code),
            # pat (its pattern; a pattern the cost book only inferred reads as none, R4), tags (its variant tags),
            # names (the style numbers its text names), prov (its text says the price is still needed), row (its
            # sheet row, the last tie break).
            rfl = set(r.get('flags') or ())
            rn = r.get('row')
            c = {'id': r['id'], 'brand': r.get('brand_code'), 'brands': brands, 'generic': r.get('brand_scope') == 'all',
                 'codes': codes, 'primary': prim if prim in codes else (codes[0] if codes else None),
                 'conf': r.get('fabric_code_confidence'), 'cat': r.get('category'), 'fit': r.get('fit_class'),
                 'sleeve': r.get('sleeve'),
                 'pat': r.get('pattern') if rfl & INFERRED_PATTERN_FLAGS else r.get('pattern_effective'), 'price': pr,
                 'alt': pr if alt is None else alt, 'rmb': ref is not None, 'ref': ref,
                 'quote': r.get('record_kind') == 'factory_quotation',
                 'tags': frozenset(t for t in (r.get('variant_tags') or ()) if t),
                 'names': tuple(n for n in (_u(x) for x in (r.get('names_styles') or ())) if n),
                 'prov': bool(rfl & PROVISIONAL_FLAGS),
                 'row': rn if isinstance(rn, int) and not isinstance(rn, bool) else 10 ** 9,
                 'restated': any('SHEET_E1_IS' in str(f) for f in rfl),
                 'conflict': any(str(g).startswith('same_text') for g in self.conflict_of.get(r['id'], ()))}
            self.pool_brands[pool].update(brands)
            for code in dict.fromkeys(codes):
                self.calc_a[(pool, code)].append(c)

    def _build_overrides(self, overrides):
        self._ovr = {'ref_style': {}, 'style': {}, 'design': {}, 'ref': {}, 'fabric_category': {}}
        items = [o for o in (overrides or []) if isinstance(o, dict)]
        for o in sorted(items, key=lambda o: str(o.get('at') or '')):
            u = _fnum(o.get('fobU'))
            k = o.get('key') if isinstance(o.get('key'), dict) else {}
            sc = o.get('scope')
            if u is None or u <= 0 or sc not in self._ovr:
                continue
            eff = _d10(o.get('effective'))
            if eff and self.today and eff > self.today:
                continue
            ref, style, design = _u(k.get('ref')), base_of(k.get('style')), _u(k.get('design'))
            fab, cat = _u(k.get('fabric')), str(k.get('category') or '').strip().lower()
            key = {'ref_style': (ref, style) if ref and style else None, 'style': (style,) if style else None,
                   'design': (design,) if design else None, 'ref': (ref,) if ref else None,
                   'fabric_category': (fab, cat) if fab and cat else None}[sc]
            if key:
                self._ovr[sc][key] = o
        self.overrides_active = sum(len(v) for v in self._ovr.values())

    def _override(self, b, sku, ref, fac):
        refu = ref or ''
        for sc, key in (('ref_style', (refu, b)), ('style', (b,)),
                        ('design', (b[2:],) if sku and sku.get('modern') else None), ('ref', (refu,)),
                        ('fabric_category', (sku['fab'], sku['cat']) if sku else None)):
            if key is None:
                continue
            o = self._ovr[sc].get(key)
            if o:
                reason = str(o.get('reason') or '').strip()
                return _R('L0', float(o['fobU']), 'Manual cost.' + (' Reason: %s' % reason[:120] if reason else ''),
                          ('OVR:%s' % o.get('id'),), fac, fx=0.0)
        return None

    def _prepare_ledger(self, rows):
        pn_refs = defaultdict(set)
        for r in rows:
            ref = _u((r or {}).get('production'))
            if ref:
                pn_refs[str((r or {}).get('poName') or '').strip()].add(ref)
        out = []
        for i, r in enumerate(rows):
            r = r or {}
            ref, flag = _u(r.get('production')), None
            pn = str(r.get('poName') or '').strip()
            if not ref and len(pn_refs.get(pn, ())) == 1:
                ref, flag = next(iter(pn_refs[pn])), 'blank_ref'
            out.append({'i': i, 'ref': ref, 'fac': fac_of(ref), 'b': base_of(r.get('style')),
                        'style': _u(r.get('style')), 'units': _int(r.get('units')), 'poName': pn,
                        'etd': _d10(r.get('etd')), 'landing': _u(r.get('warehouse')), 'flag': flag, 'row': r,
                        'res': None})
        return out

    def _pass1(self):
        """Factory-list prices by style and design (L5 siblings)."""
        for L in self.ledger:
            res = self.raw(L['b'], L['fac'], L['ref'] or None, L['poName'], allow_default=False, use_ovr=False)
            if res['level'] in ('L1', 'L2'):
                e = (res['price'], res['ids'], L['ref'])
                self.list_style[L['b']].append(e)
                if MOD_SKU_RE.match(L['b']):
                    self.list_design[L['b'][2:]].append(e)
        for (ref, st), (pr, rid) in self.pc_exact.items():
            e = (pr, (rid,), ref)
            if e not in self.list_style[st]:
                self.list_style[st].append(e)
                if MOD_SKU_RE.match(st):
                    self.list_design[st[2:]].append(e)
        self._cache.clear()

    def _pass2(self):
        """Resolve the ledger, then the L6 defaults from its L1-L4 costs, then the rest."""
        for L in self.ledger:
            L['res'] = self.raw(L['b'], L['fac'], L['ref'] or None, L['poName'], allow_default=False)
        dd = defaultdict(list)
        for L in self.ledger:
            res = L['res']
            if res['level'] in ('L1', 'L2', 'L3', 'L4a', 'L4b', 'L4c', 'L4d') and L['units'] > 0:
                sku = self.decode(L['b'])
                if sku:
                    for k in ((sku['cat'], sku['fab']), (sku['cat'], sku['brand']), (sku['cat'],)):
                        dd[k].append((res['price'], L['units'], res['fx'], res.get('fxr')))
        self.defaults = {k: _wmedian(v) for k, v in dd.items()}
        self._cache.clear()
        for L in self.ledger:
            if L['res']['level'] == 'L7':
                L['res'] = self.raw(L['b'], L['fac'], L['ref'] or None, L['poName'])

    # ── calculator matching ──
    def pool_order(self, fac, grp, cat):
        cfg = self.pool_cfg
        if cat == 'polo':
            polo = cfg.get('polo') or {}
            return list(polo.get('primary') or []), list(polo.get('fallback') or [])
        fams = list(self.grid.get(fac) or self.grid.get('_default') or [])

        def fam_pools(fam):
            f = cfg.get(fam) or {}
            return list((f.get('byGroup') or {}).get(grp) or []) + list(f.get('base') or [])
        prim = list(dict.fromkeys(fam_pools(fams[0]))) if fams else []
        seen, fb = set(prim), []
        for pool in [x for fam in fams[1:] for x in fam_pools(fam)] + list(cfg.get('all') or []):
            if pool not in seen:
                seen.add(pool)
                fb.append(pool)
        return prim, fb

    # A brand-less factory quotation counts as brand matched in its own customer group's pool.
    # Private regression runs switch it off to compare with the study, which did not do this.
    QUOTE_BRAND_NEUTRAL = True

    def _own_pool(self, pool, grp):
        """True when the pool is this customer group's own block (for example the Half Price
        quotation pool for group HALF)."""
        return self.QUOTE_BRAND_NEUTRAL and bool(grp) and str(pool or '').partition(':')[2] == grp

    def style_hints(self, sku):
        """The variants a style shows by its number (contract C15 R5): its fabric code, pattern letter and customer
        code, read with the public platform dictionaries (VARIANT_BY_FABRIC, _COLLAR, _CUSTOMER)."""
        if not sku:
            return frozenset()
        return frozenset(v for v in (VARIANT_BY_FABRIC.get(sku.get('fab')),
                                     VARIANT_BY_COLLAR.get((sku.get('collar') or '')[:1]),
                                     VARIANT_BY_CUSTOMER.get(sku.get('cust'))) if v)

    @staticmethod
    def names_style(c, b):
        """True when a calculator row's text names base style b (contract C15 R0): a full style number names its
        design on any customer (the part after the customer code); a ten character number written without its fabric
        code ('ZZQA901WRS': customer, brand, serial, fit and pattern; the cost book flags the row for a check) names
        the style of that customer and brand; a number written after STYLE# names the serial, fit and pattern letters
        within the row's own brand section."""
        if not c['names'] or not MOD_SKU_RE.match(b or ''):
            return False
        return any((len(n) >= 11 and n[2:] == b[2:]) or (len(n) == 10 and n[:4] == b[:4] and n[4:] == b[6:])
                   or (len(n) < 10 and n == b[6:]) for n in c['names'])

    @staticmethod
    def fit_col(sku):
        """(price fit, known) of contract C15 R6: the slim, regular or big and tall column from the fit code; modern
        and tailored take regular (price_fit); an unknown or missing fit takes regular, with known False."""
        f = price_fit(sku.get('fit')) if sku else None
        return (f, True) if f in FIT_COLUMN else ('REGULAR', False)

    def _cv(self, c):
        """The value a calculator row counts at in R7: the price in use (the after-cut reading on that basis)."""
        return c['alt'] if self.after_cut else c['price']

    def _lrow(self, rid, sku):
        """(record id, fit column) of a list row for styles.costRow: the row's own fit column, else the style's."""
        r = self.records.get(rid) or {}
        return rid, FIT_COLUMN.get(r.get('fit_class')) or FIT_COLUMN[self.fit_col(sku)[0]]

    @staticmethod
    def _fit_note(sku):
        if not sku or sku.get('fit') in ('SLIM', 'REGULAR', 'BIG_TALL'):
            return ''
        if sku.get('fit') in ('MODERN', 'TAILORED'):
            return ' The %s fit takes the regular fit price.' % str(sku['fit']).lower()
        return ' The style number has no known fit, so the regular fit price is used.'

    def _pick(self, pools, sku, fc, sleeve, cross=False, strict=True):
        """Contract C15: the one row of the first pool (sheet) in pools that quotes the style, or None.
        -> {'pool', 'c': the row, 'tie': the rows R7 chose among (lowest first), 'skip': [(row, reason)], 'flags'}.
        Rows looked at: the pool's rows that carry the style's fabric code (R2), at the fit column fc (R6), in the
        style's garment class (polo or not). In order:
          R1  the style's own brand section. With no section for the brand on this sheet: its generic (all-brand)
              rows, or the customer group's own brand-less quotation. cross: every other brand's section (last
              resort); a customer's brand-less quotation serves there only in that customer group's own pool.
          R0  a row whose text names the style is that style's row, before the sleeve, pattern and variant rules
              (own brand only). A known fit code's sleeve decides between named rows; with an unknown fit code the
              named rows' own sleeves decide. A named row whose pattern (or, with a known fit, sleeve) differs from
              the style number still wins, flagged named_row_conflict.
          R3  the row's sleeve. A row with no sleeve is a long sleeve price, as the cost book reads it; a short
              sleeve style reaches it only through the short sleeve step (calc_match).
          R4  the row's pattern, when both are known (a pattern the cost book only inferred counts as none).
          R5  a hard variant row (brushed, perforated, zipper, tie, both side print, camp collar) only for a style
              that shows the variant, and a style that shows one takes that row.
          R2  strict (the exact rungs): the style's code comes first on the row, at high or medium confidence. Then
              the best confidence, then rows where the code comes first.
          R4  a pattern-specific row beats a row with no pattern. A regular print beats a digital print unless the
              style shows a digital print (flag pattern_guess).
          R5  a row whose text says its price is still needed loses to any confirmed row (reason lower_confidence;
              flag price_unconfirmed when only such rows are left). A plain row beats white, colour, cooling and
              cotton-origin rows and rows that name other styles.
          R7  the lowest (then the lowest sheet row, then the lowest id); more than one row left is flagged
              ambiguous_rows."""
        fab, b, sp = sku['fab'], sku['base'], sku.get('pat')
        polo, brand = sku['cat'] == 'polo', sku.get('brand')
        hints = self.style_hints(sku)
        known = self.fit_col(sku)[1]

        def tie_key(c):
            return self._cv(c), c['row'], c['id']
        for pool in pools:
            src = self.calc_a.get((pool, fab))
            if not src:
                continue
            rows = [c for c in src if c['cat'] != 'other' and polo == (c['cat'] == 'polo') and c['fit'] == fc]
            if not rows:
                continue
            # A factory quotation made for this customer group (it names no brand) in the group's own pool.
            own = self._own_pool(pool, sku.get('group'))
            if cross:
                def ok_b(c):
                    return (brand is None or brand not in c['brands']) and (own or not (c['quote'] and not c['brands']))
            elif brand is not None and brand in self.pool_brands.get(pool, ()):
                def ok_b(c):
                    return brand in c['brands']
            else:
                # No section for the brand on this sheet: an all-brand section, or the group's own quotation.
                def ok_b(c):
                    return c['generic'] or (own and c['quote'] and not c['brands'])
            skip, E, why = [], [], {}
            for c in rows:
                s_ok = (c['sleeve'] or 'LS') == sleeve
                p_ok = sp is None or c['pat'] is None or c['pat'] == sp
                if not ok_b(c):
                    if s_ok and p_ok:                  # other brands' rows for the same key only
                        skip.append((c, 'other_brand'))
                    continue
                why[c['id']] = ('other_sleeve' if not s_ok else 'other_pattern' if not p_ok else
                                'variant_not_this_style' if (c['tags'] & HARD_VARIANTS) - hints else None)
            named = [] if cross else [c for c in rows if c['id'] in why and self.names_style(c, b)]
            if named:
                return self._pick_named(pool, rows, named, why, skip, sleeve, sp, known, tie_key)
            for c in rows:
                if c['id'] in why:
                    if why[c['id']]:
                        skip.append((c, why[c['id']]))
                    else:
                        E.append(c)

            def narrow(keep, reason):
                ks = {c['id'] for c in keep}
                skip.extend((c, reason) for c in E if c['id'] not in ks)
                return keep
            hv = [c for c in E if c['tags'] & hints & HARD_VARIANTS]
            if hv:
                E = narrow(hv, 'variant_not_this_style')
            if strict:
                E = narrow([c for c in E if c['primary'] == fab and c['conf'] in ('high', 'medium')], 'lower_confidence')
            if not E:
                continue

            def rank(c):
                return _CONF_RANK.get(c['conf'], 3), 0 if c['primary'] == fab else 1
            r0 = min(rank(c) for c in E)
            E = narrow([c for c in E if rank(c) == r0], 'lower_confidence')
            flags = []
            if sp is not None and any(c['pat'] == sp for c in E):
                E = narrow([c for c in E if c['pat'] == sp], 'other_pattern')
            if sp == 'PRINT':
                dig = [c for c in E if 'digital_print' in c['tags']]
                if dig and len(dig) < len(E):
                    if 'digital_print' in hints:
                        E = narrow(dig, 'variant_not_this_style')
                    else:
                        E = narrow([c for c in E if 'digital_print' not in c['tags']], 'variant_not_this_style')
                        flags.append('pattern_guess')
            firm = [c for c in E if not c['prov']]
            if firm and len(firm) < len(E):
                E = narrow(firm, 'lower_confidence')
            elif not firm:
                flags.append('price_unconfirmed')
            rel = [c for c in E if c['tags'] & hints & REL_VARIANTS]
            plain = [c for c in E if not (c['tags'] & REL_VARIANTS) and not c['names']]
            if rel or plain:
                E = narrow(rel or plain, 'variant_not_this_style')
            E = sorted(E, key=tie_key)
            if len(E) > 1:
                flags.append('ambiguous_rows')
                skip.extend((c, 'tie_not_lowest') for c in E[1:])
            skip.sort(key=lambda t: (_REASON_ORDER[t[1]],) + tie_key(t[0]))
            return {'pool': pool, 'c': E[0], 'tie': E, 'skip': skip, 'flags': flags}
        return None

    def _pick_named(self, pool, rows, named, why, skip, sleeve, sp, known, tie_key):
        """Contract C15 R0 on one sheet: the rows whose text names the style. A known fit code's sleeve decides between
        them (a row with no sleeve serves); an unknown fit code lets their own sleeves decide. Then the style's
        pattern (a row with no pattern serves), a confirmed price over one the sheet says is still needed, and R7.
        A named row that disagrees with the style number's pattern or sleeve still wins, flagged named_row_conflict.
        Every other row of the brand's section is set aside with its own reason, else variant_not_this_style (a
        row names this style)."""
        flags, N = [], list(named)

        def narrow(keep, reason):
            ks = {c['id'] for c in keep}
            skip.extend((c, reason) for c in N if c['id'] not in ks)
            return keep
        if known:
            m = [c for c in N if c['sleeve'] in (None, sleeve)]
            if m:
                N = narrow(m, 'other_sleeve')
            else:
                flags.append('named_row_conflict')
        m = [c for c in N if sp is None or c['pat'] is None or c['pat'] == sp]
        if m:
            N = narrow(m, 'other_pattern')
        elif 'named_row_conflict' not in flags:
            flags.append('named_row_conflict')
        firm = [c for c in N if not c['prov']]
        if firm and len(firm) < len(N):
            N = narrow(firm, 'lower_confidence')
        elif not firm:
            flags.append('price_unconfirmed')
        N = sorted(N, key=tie_key)
        if len(N) > 1:
            flags.append('ambiguous_rows')
            skip.extend((c, 'tie_not_lowest') for c in N[1:])
        ids = {c['id'] for c in named}
        skip.extend((c, why[c['id']] or 'variant_not_this_style') for c in rows if c['id'] in why and c['id'] not in ids)
        skip.sort(key=lambda t: (_REASON_ORDER[t[1]],) + tie_key(t[0]))
        return {'pool': pool, 'c': N[0], 'tie': N, 'skip': skip, 'flags': flags, 'named': True}

    def calc_match(self, sku, fac, grp, cross=False):
        """-> (level, pick, adjustment, how, cross) or None (contract C15). Own brand: the exact rungs (L4a in the
        factory's own grid, L4b in the other; a style with no known fit takes the regular column at L4d), then the
        derived rungs (L4d): an alternate or low confidence fabric code, short sleeve from the long sleeve row plus
        the short sleeve step, regular or big and tall from the slim row plus the fit premium. cross: the same rungs
        on other brands' rows, every one at level L4c (flag cross_brand, grade D); _resolve tries it last. Each rung
        takes one row (_pick). A solid style never takes a print row, nor the reverse (R4): no rung relaxes it."""
        if sku['cat'] in ('pants', 'blazer', 'vest', 'overshirt') or not sku.get('fab'):
            return None
        prim, fb = self.pool_order(fac, grp, sku['cat'])
        allp = prim + fb
        fc, known = self.fit_col(sku)
        sl, ssd = sku['sleeve'], self.ss_delta
        how = 'exact' if fc == sku['fit'] else 'fit_regular' if known else 'fit_unknown'
        lvl_d = 'L4c' if cross else 'L4d'
        exact = ((('L4c', allp),) if cross else (('L4a', prim), ('L4b', fb)) if known else (('L4d', allp),))
        for lvl, pools in exact:
            s = self._pick(pools, sku, fc, sl, cross)
            if s:
                return lvl, s, 0.0, how, cross
        s = self._pick(allp, sku, fc, sl, cross, strict=False)
        if s:
            return lvl_d, s, 0.0, 'alt_code', cross
        if sl == 'SS' and ssd is not None:
            s = self._pick(allp, sku, fc, 'LS', cross, strict=False)
            if s:
                return lvl_d, s, ssd, 'ss_from_ls', cross
        add = self.bt_add if fc == 'BIG_TALL' else self.reg_add if fc == 'REGULAR' else None
        if add is not None:
            for s_ in ([sl, 'LS'] if sl == 'SS' else [sl]):
                if s_ != sl and ssd is None:
                    continue
                s = self._pick(allp, sku, 'SLIM', s_, cross, strict=False)
                if s:
                    return lvl_d, s, add + (ssd if s_ != sl else 0.0), 'fit_from_slim', cross
        return None

    def _calc_res(self, sku, fac, lvl, sel, adj, how, cross=False):
        """A calculator resolution (levels L4a to L4d; L4c for another brand's row) from the one row picked by
        contract C15 (sel, from _pick). The ladder and the direct calculator quotes of contract C13 both build it
        here, so their prices, RMB parts and flags agree. The price is the row's price plus a dollar step (adj: the
        short sleeve step or a fit premium, from the cost book params; it does not move with the rate). The
        evidence is the row used; the rows set aside ride along in skip. A tie (R7) keeps its price range."""
        c = sel['c']
        fit = sku.get('fit')
        known = self.fit_col(sku)[1]
        who = 'fabric' if cross else 'brand, fabric'
        text = {'exact': 'same %s, fit, sleeve and pattern' % who,
                'fit_regular': 'same %s, sleeve and pattern, %s fit at the regular fit price' % (who, str(fit).lower()),
                'fit_unknown': 'same %s, sleeve and pattern at the regular fit price, because the style number has '
                               'no known fit' % who,
                'alt_code': 'alternate or low confidence fabric code',
                'ss_from_ls': 'short sleeve derived from the long sleeve row',
                'fit_from_slim': 'fit derived from the slim row plus the fit premium'}[how]
        # The customer's own factory quotation, matched in its group pool although it names no brand.
        neutral = not cross and sku.get('brand') is not None and c['quote'] and not c['brands']
        if neutral and how == 'exact':
            text = "this customer's own quotation for the fabric, fit, sleeve and pattern"
        said = ["Another brand's row, because no sheet quotes this brand for the style"] if cross else []
        said.append(text)
        if sel.get('named'):
            said.append('The row text names this style, so it is the style\'s row')
        if 'named_row_conflict' in sel['flags']:
            said.append('That row\'s pattern or sleeve differs from the style number. Confirm which is right')
        if 'pattern_guess' in sel['flags']:
            said.append('The style number does not say digital print, so the regular print row is used')
        if 'price_unconfirmed' in sel['flags']:
            said.append('The sheet says this price is still needed, so it is unconfirmed')
        if 'ambiguous_rows' in sel['flags']:
            said.append('Several rows fit, so the lowest is used')
        flags = ['customer_quote'] if neutral else []
        if lvl == 'L4d' or (cross and how in ('alt_code', 'ss_from_ls', 'fit_from_slim')):
            flags.append('derived')
        if cross:
            flags.append('cross_brand')
        if fit in ('MODERN', 'TAILORED'):
            flags.append('fit_as_regular')
        if not known:
            flags.append('fit_unknown')
        flags.extend(sel['flags'])
        if sku['program']:
            flags.append('program_map')
            if 'ASSUM' in str(sku.get('src') or '').upper():
                flags.append('assumed')
        if c['conflict']:
            flags.append('price_conflict')
        if c['restated']:
            flags.append('sheet_rate_far')
        basis = '%s: %s.' % (_pool_label(sel['pool']), '. '.join(s[0].upper() + s[1:] for s in said))
        if sku['program']:
            basis += ' Program code decoded by the cost book map.'
        # The row's price already stands at the P&L's basis (printed, or the saved rate), so the RMB-based share
        # (value share) is exact.
        price = c['price'] + adj
        tie = sel['tie']
        return _R(lvl, price, basis, (c['id'],), fac, alt=c['alt'] + adj,
                  rng=(tie[0]['price'] + adj, max(x['price'] for x in tie) + adj) if len(tie) > 1 else None,
                  fx=c['price'] / price if c['rmb'] and price > 0 else 0.0, fxr=c['ref'] if c['rmb'] else None,
                  flags=flags, row=(c['id'], FIT_COLUMN.get(c['fit'], 'regular')),
                  skip=tuple((x['id'], why) for x, why in sel['skip']))

    # ── contract C13: every factory's direct quote, and one combined cost per style ──
    def grid_exact(self, sku, fac):
        """The exact calculator match of a style under one factory's grid order (contract C13): the style's own
        brand, fabric, fit, sleeve and pattern, at the style's fit column (modern and tailored at the regular
        column). -> (level, pick, 0.0, how) with level L4a (the factory's own grid) or L4b (the other grid), or
        None. It repeats the exact rungs of calc_match, so it picks the same one row (contract C15)."""
        if sku is None or sku['cat'] in ('pants', 'blazer', 'vest', 'overshirt') or not sku.get('fab'):
            return None
        fc = price_fit(sku['fit'])
        if fc not in FIT_COLUMN:
            return None
        prim, fb = self.pool_order(fac, sku['group'], sku['cat'])
        for lvl, pools in (('L4a', prim), ('L4b', fb)):
            s = self._pick(pools, sku, fc, sku['sleeve'])
            if s:
                return lvl, s, 0.0, ('exact' if fc == sku['fit'] else 'fit_regular')
        return None

    def _qv(self, q):
        """The value a quote or a factory value counts at: the price in use (the after-cut reading on that basis)."""
        return q['alt'] if self.after_cut else q['price']

    def _img_res(self, im, sku, fac):
        """Level L1 from a customer quotation that names the exact style, its price shown in a picture on the sheet
        (contract C15 R0: the row that names the style is its row). A USD price: no RMB part."""
        pr, rid = im[0], im[1]
        return _R('L1', pr, "This customer's quotation names the style. Its price is shown in a picture on the sheet.",
                  (rid,), fac, flags=('customer_quote',), row=self._lrow(rid, sku))

    def _pool_sheet(self, cands):
        sheets = sorted({str((self.records.get(c['id']) or {}).get('sheet') or '').strip() for c in cands} - {''})
        return ', '.join(sheets) or None

    @staticmethod
    def _names(names):
        """'A', 'A and B', 'A, B and C' (plain UI text)."""
        names = [n for n in names if n]
        return names[0] if len(names) == 1 else ', '.join(names[:-1]) + ' and ' + names[-1] if names else ''

    def quotes(self, b):
        """Every direct factory quote for base style b (contract C13), sorted by price. Each is a resolution with
        'fac' (the factory), 'src' (the list's source code, or the calculator sheet family) and 'where' (the ref,
        or the calculator sheet):
          - lists: a Pinnacle-type list names the exact style on any ref (L1); a David Peng-type list prices a
            ref and pattern, and a KinYun-type list a ref, pattern and fit (L2), for refs of that factory that
            carry the style on the ledger or its history;
          - calculators: the exact match (grid_exact) under each factory that makes the style. A list factory
            counts here only where its own list does not answer for the style (its ladder gives no L1 to L3).
            A style made by no known factory and on no list is matched under the default grid order (UNKNOWN).
        Sibling and fabric medians, relaxed or other-brand matches, proxies and defaults are not direct quotes."""
        if b in self._quotes:
            return self._quotes[b]
        sku = self.decode(b)
        carry = self.carry.get(b) or {}
        out = []
        for ref, pr, rid in self.pc_by_style.get(norm_vd(b), ()):
            r = self.records[rid]
            q = _R('L1', pr, 'Factory list, same ref and style.', (rid,), _u(r.get('factory_code')) or fac_of(ref),
                   row=self._lrow(rid, sku))
            out.append(dict(q, src=str(r.get('source_code') or ''), where=ref or None))
        im = self.img_style.get(norm_vd(b))
        if im:                                  # a customer quotation that names the style (its factory's one row)
            r = self.records[im[1]]
            out.append(dict(self._img_res(im, sku, im[2]), src=str(r.get('source_code') or ''),
                            where=str(r.get('sheet') or '').strip() or None))
        if sku:
            pat, fit = sku['pat'], price_fit(sku['fit'])
            for fac in sorted(carry):
                for ref in sorted(carry[fac]):
                    if ref in self.dp_refs and (ref, pat) in self.dp_ref:
                        p, rid, fl, rng, sk = _lowest(self.dp_ref[(ref, pat)])       # one row (contract C15 R7)
                        q = _R('L2', p, 'Factory list, same ref and pattern.' + (
                            ' Several rows fit, so the lowest is used.' if fl else ''), (rid,), fac, rng=rng, flags=fl,
                            row=self._lrow(rid, sku), skip=sk)
                    elif ref in self.ky_refs and (ref, pat, fit) in self.ky_ref:
                        pr, rid = self.ky_ref[(ref, pat, fit)]
                        mapped = fit != sku['fit']
                        q = _R('L2', pr, 'Factory list, same ref, pattern and fit.' + (
                            ' The %s fit takes the regular fit price.' % str(sku['fit']).lower() if mapped else ''),
                            (rid,), fac, flags=('fit_as_regular',) if mapped else (), row=self._lrow(rid, sku))
                    else:
                        continue
                    out.append(dict(q, src=str(self.records[q['ids'][0]].get('source_code') or ''), where=ref))
        lists = self.pc_facs | self.dp_facs | self.ky_facs
        grid = [fac for fac in sorted(carry)
                if (not im or fac != im[2])
                and (fac not in lists or not any(self.raw(b, fac, ref, pn, allow_default=False, use_ovr=False)['level']
                                                 in _LIST_LEVELS for ref, pn in sorted(carry[fac].items())))]
        if not carry and not out:
            grid = ['UNKNOWN']
        for fac in grid:
            m = self.grid_exact(sku, fac)
            if m:
                out.append(dict(self._calc_res(sku, fac, *m), src=str(m[1]['pool']).partition(':')[0],
                                where=self._pool_sheet([m[1]['c']])))
        out.sort(key=lambda q: (self._qv(q), q['fac'], q['src'], q['where'] or ''))
        self._quotes[b] = out
        return out

    def _fac_value(self, fac, qs):
        """One value per factory (contracts C13 and C15): its one row. A factory that quotes the style more than
        once (a list on several refs) counts at its lowest quote (R7, flag ambiguous_rows); the other quotes' rows
        are listed as set aside. Level, grade, RMB part and rate are the used quote's."""
        s = sorted(qs, key=lambda q: (self._qv(q), q['where'] or '', q['ids']))
        q0, n = s[0], len(s)
        vals = [q['price'] for q in s]
        flags, skip = list(q0['flags']), list(q0.get('skip') or ())
        if n > 1:
            if 'ambiguous_rows' not in flags:
                flags.append('ambiguous_rows')
            skip = [(q['row'][0], 'tie_not_lowest') for q in s[1:] if q.get('row')] + skip
        fx = q0['fx'] or 0.0
        return {'fac': fac, 'price': q0['price'], 'alt': q0['alt'], 'fx': fx, 'fxr': q0.get('fxr') if fx else None,
                'level': q0['level'], 'grade': LEVEL_INFO[q0['level']][1], 'n': n, 'ids': tuple(q0['ids']),
                'flags': tuple(flags), 'rng': (min(vals), max(vals)) if n > 1 else q0.get('rng'),
                'basis': q0['basis'] if n == 1 else '%s The lowest of its %d quotes.' % (q0['basis'], n),
                'row': q0.get('row'), 'skip': tuple(dict.fromkeys(skip))}

    def combined(self, b):
        """The combined cost of base style b (contract C13), or None when no factory quotes it directly:
        {'rule': 'single' | 'average' | 'lowest', 'res': its resolution, 'values': the per-factory values,
        'spread': r4(max / min - 1) over them (0 for one)}. One factory: its value. Two or more: their average,
        unless max > min x (1 + wideSpreadPct / 100), then the lowest. Exactly at the threshold is 'average'."""
        if b in self._comb:
            return self._comb[b]
        qs = self.quotes(b)
        out = None
        if qs:
            by = defaultdict(list)
            for q in qs:
                by[q['fac']].append(q)
            vals = sorted((self._fac_value(f, v) for f, v in by.items()), key=lambda v: (self._qv(v), v['fac']))
            lo, hi = self._qv(vals[0]), self._qv(vals[-1])
            if len(vals) == 1:
                rule, use = 'single', vals
            elif hi > lo * (1 + self.wide_pct / 100) + _SPREAD_TOL:
                rule, use = 'lowest', vals[:1]
            else:
                rule, use = 'average', vals
            spread = 0.0 if len(vals) == 1 else (r4(hi / lo - 1) if lo > 0 else None)
            out = {'rule': rule, 'res': self._comb_res(rule, use, vals), 'values': vals, 'spread': spread}
        self._comb[b] = out
        return out

    def _comb_res(self, rule, use, vals):
        """The resolution of a combined cost (contract C13). gcap: its grade, the worst grade among the values
        used. Level: the used value's level, or for 'average' the best level among the inputs. fxShare and fxRef
        are value weighted over the values used, so the page's RMB what-if stays exact."""
        pct = '%g' % self.wide_pct
        names = self._names([_factory_name(self.S, v['fac']) for v in vals])
        if rule == 'average':
            n = len(use)
            price = sum(v['price'] for v in use) / n
            alt = sum(v['alt'] for v in use) / n
            rv = sum(v['price'] * v['fx'] for v in use) / n
            fxr = _ref_mix((v['price'] * v['fx'], v['fxr']) for v in use)
            fx = rv / price if price > 0 and rv > 0 else 0.0
            level = min((v['level'] for v in use), key=LEVEL_RANK.get)
            grade = max(v['grade'] for v in use)
            flags = [f for v in use for f in v['flags'] if f != 'range'] + ['cost_average']
            rng = (min(v['price'] for v in use), max(v['price'] for v in use))
            basis = 'Average of %d factory quotes (%s). They are within %s percent of each other.' % (n, names, pct)
        else:
            v = use[0]
            price, alt, fx, fxr, level, grade = v['price'], v['alt'], v['fx'], v['fxr'], v['level'], v['grade']
            if rule == 'single':
                flags, rng = list(v['flags']), v['rng']
                basis = ('No factory is known for this style. %s' % v['basis'] if v['fac'] == 'UNKNOWN' else
                         '%s is the only factory that quotes this style. %s' % (_factory_name(self.S, v['fac']),
                                                                               v['basis']))
            else:
                flags = [f for f in v['flags'] if f != 'range'] + ['cost_lowest']
                rng = (min(x['price'] for x in vals), max(x['price'] for x in vals))
                basis = ('Lowest of %d factory quotes (%s), because they differ by more than %s percent. The lowest '
                         'is from %s. %s' % (len(vals), names, pct, _factory_name(self.S, v['fac']), v['basis']))
        # Contract C15: the style's sheet row is the used value's (for an average, the lowest factory's; Cost by
        # factory lists the others).
        res = _R(level, price, basis, tuple(dict.fromkeys(i for v in use for i in v['ids'])), None, alt=alt,
                 rng=rng, fx=fx, fxr=fxr, flags=tuple(dict.fromkeys(flags)), row=use[0].get('row'),
                 skip=use[0].get('skip') or ())
        res['gcap'], res['rule'] = grade, rule
        return res

    def final(self, b, res):
        """The cost every table uses for base style b: the one choke point of contract C13. A manual cost (L0)
        wins outright. In combined mode a style that some factory quotes directly takes its combined cost on
        every row; the row keeps its own factory, ref, lot tier and lot flags. Anything else keeps its ladder
        result (the 'fallback'). Cascade mode returns res unchanged (today's behaviour)."""
        if res is None or self.rule_mode != 'combined' or res.get('level') == 'L0':
            return res
        c = self.combined(b)
        if c is None:
            return res
        out = dict(c['res'], fac=res.get('fac'))
        for k in ('tier', 'ref'):
            if k in res:
                out[k] = res[k]
        lot = tuple(f for f in (res.get('flags') or ()) if f in _LOT_FLAGS)
        if lot:
            out['flags'] = tuple(dict.fromkeys(tuple(out['flags']) + lot))
        return out

    def cost_rule(self, b, level):
        """styles.costRule (contract C13) of a style whose cost stands at level."""
        if level == 'L0':
            return 'manual'
        if self.rule_mode != 'combined':
            return 'cascade'
        c = self.combined(b)
        return c['rule'] if c else 'fallback'

    def cost_by_factory(self, b):
        """styles.costByFactory (contract C13): [factory, source, ref or sheet, price, level] per direct quote."""
        return [[q['fac'], q['src'], q['where'], r4(self._qv(q)), q['level']] for q in self.quotes(b)]

    def cost_spread(self, b):
        """styles.costSpread (contract C13): r4(max / min - 1) over the per-factory values, 0 for one, None for none."""
        c = self.combined(b)
        return c['spread'] if c else None

    def style_quote(self, style):
        """The combined cost of a style (contract C13) as published: public fields plus rule, grade, spread and
        costByFactory. None when no factory quotes the style directly."""
        b = base_of(style)
        c = self.combined(b)
        if c is None:
            return None
        return dict(self.public(c['res']), rule=c['rule'], grade=c['res']['gcap'], spread=c['spread'],
                    costByFactory=self.cost_by_factory(b))

    # ── the cascade ──
    def raw(self, b, fac, ref=None, poName=None, allow_default=True, brand_label=None, customer_group=None,
            use_ovr=True):
        key = (b, fac, ref, (poName or '')[:2].upper(), allow_default, brand_label, customer_group, use_ovr)
        out = self._cache.get(key)
        if out is None:
            out = self._resolve(b, fac, ref, poName, allow_default, brand_label, customer_group, use_ovr)
            self._cache[key] = out
        return out

    def _resolve(self, b, fac, ref, poName, allow_default, brand_label, group, use_ovr):
        sku = self.decode(b)
        if sku is not None and group:
            sku = dict(sku, group=group)
        if use_ovr and self.overrides_active:
            o = self._override(b, sku, ref, fac)
            if o:
                return o
        # Contract C15: every rung takes one row of the style's own brand (R1), pattern (R4) and fit column (R6).
        # Rows still tied give the lowest, flagged ambiguous_rows (R7). Never a mean or a median of rows.
        many = ' Several rows fit, so the lowest is used.'
        if ref and (ref, b) in self.pc_exact:
            pr, rid = self.pc_exact[(ref, b)]
            return _R('L1', pr, 'Factory list, same ref and style.', (rid,), fac, row=self._lrow(rid, sku))
        im = self.img_style.get(norm_vd(b))
        if im and (fac == im[2] or fac in _LIST_FACTORY_PSEUDO):
            return self._img_res(im, sku, fac)
        if ref in self.dp_refs and sku is None:
            p, rid, fl, rng, sk = _lowest([x for k, v in self.dp_ref.items() if k[0] == ref for x in v])
            return _R('L2', p, 'Factory list, same ref. The SKU has no pattern letter, so every row of the ref fits.'
                      + (many if fl else ''), (rid,), fac, rng=rng, flags=('malformed_sku',) + fl,
                      row=self._lrow(rid, sku), skip=sk)
        if ref in self.dp_refs and sku and (ref, sku['pat']) in self.dp_ref:
            p, rid, fl, rng, sk = _lowest(self.dp_ref[(ref, sku['pat'])])
            return _R('L2', p, 'Factory list, same ref and pattern.' + (many if fl else ''), (rid,), fac, rng=rng,
                      flags=fl, row=self._lrow(rid, sku), skip=sk)
        brand = sku.get('brand') if sku else None
        fcl, known = self.fit_col(sku)
        ffl = () if not sku else ('fit_as_regular',) if sku['fit'] in ('MODERN', 'TAILORED') else () if known else (
            'fit_unknown',)
        if ref in self.ky_refs and sku and (ref, sku['pat'], fcl) in self.ky_ref:
            pr, rid = self.ky_ref[(ref, sku['pat'], fcl)]
            return _R('L2', pr, 'Factory list, same ref, pattern and fit.' + self._fit_note(sku), (rid,), fac,
                      flags=ffl, row=self._lrow(rid, sku))
        if sku and fac in self.pc_facs and not sku['program']:
            bn = norm_vd(b)
            for src, text in ((self.pc_style.get(bn, []), 'Same style on another ref of this factory list.'),
                              (self.pc_design.get(bn[2:], []), 'Same design on another ref of this factory list.')):
                c = [x for x in src if x[2] != ref]
                if c:
                    p, rid, fl, rng, sk = _lowest([(x[0], x[1]) for x in c])
                    return _R('L3', p, text + (many if fl else ''), (rid,), fac, rng=rng, flags=('sibling',) + fl,
                              row=self._lrow(rid, sku), skip=sk)
            # The listed styles of the same brand, fabric, category, sleeve and pattern (C15 R1, R4), at the style's
            # own fit (R6: a listed style's fit decides its price). With only slim styles listed, a regular or big and
            # tall style takes the slim price plus the fit premium (flag derived); any other fit falls through.
            c = [x for x in self.pc_fcs.get((sku['fab'], sku['cat'], sku['sleeve'] if sku['cat'] != 'pants' else '-'), [])
                 if x[2] == brand and (sku['pat'] is None or x[3] is None or x[3] == sku['pat'])]
            same = [x for x in c if x[4] == fcl]
            add = self.bt_add if fcl == 'BIG_TALL' else self.reg_add if fcl == 'REGULAR' else None
            slim = [x for x in c if x[4] == 'SLIM'] if not same and add is not None else []
            if same or slim:
                p, rid, fl, rng, sk = _lowest([(x[0], x[1]) for x in (same or slim)])
                if same:
                    return _R('L3', p, "This factory's list for the same brand, fabric, category, sleeve, pattern and "
                              'fit (not this style).' + (many if fl else ''), (rid,), fac, rng=rng,
                              flags=('fabric_median',) + ffl + fl, row=self._lrow(rid, sku), skip=sk)
                return _R('L3', p + add, "This factory's list for the same brand, fabric, category, sleeve and pattern "
                          '(not this style), at a slim style\'s price plus the fit premium.' + (many if fl else ''),
                          (rid,), fac, alt=p + add, rng=(rng[0] + add, rng[1] + add) if rng else None,
                          flags=('fabric_median', 'derived') + ffl + fl, row=self._lrow(rid, sku), skip=sk)
        if sku and fac in self.dp_facs and sku['fab'] in self.dp_fabs and sku['pat'] in ('SOLID', 'PRINT'):
            amazon = str(poName or '').upper().startswith('AM')
            for am in (amazon, not amazon):
                ps = [x[:2] for x in self.dp_rate.get((sku['pat'], sku['sleeve'], am), ()) if _brand_ok(x[2], brand)]
                if ps:
                    p, rid, fl, rng, sk = _lowest(ps)
                    return _R('L3', p, "This factory's list rate for the brand, pattern and sleeve. The ref is not on "
                              'its list.' + (many if fl else ''), (rid,), fac, rng=rng, flags=('sibling',) + fl,
                              row=self._lrow(rid, sku), skip=sk)
        if sku and fac in self.ky_facs and sku['fab'] in self.ky_fabs and sku['pat'] in ('SOLID', 'PRINT'):
            amazon = str(poName or '').upper().startswith('AM')
            for am in (amazon, not amazon):
                ps = [x[:2] for x in self.ky_rate.get((sku['pat'], fcl, am), ()) if _brand_ok(x[2], brand)]
                if ps:
                    p, rid, fl, rng, sk = _lowest(ps)
                    ss = sku['sleeve'] == 'SS'
                    return _R('L3', p, "This factory's list rate for the brand, pattern and fit. The ref is not on its "
                              'list.' + self._fit_note(sku) + (' Short sleeve at the long sleeve rate, so an upper bound.'
                                                               if ss else '') + (many if fl else ''),
                              (rid,), fac, rng=rng, flags=('sibling',) + (('ss_upper_bound',) if ss else ()) + ffl + fl,
                              row=self._lrow(rid, sku), skip=sk)
        if sku:
            m = self.calc_match(sku, fac, sku['group'])
            if m:
                return self._calc_res(sku, fac, *m)
            if not sku['program']:
                for items, text in (([x for x in self.list_style.get(b, []) if x[2] != ref],
                                     'Same style priced on a factory list (another ref).'),
                                    (self.list_design.get(b[2:], []), 'Same design priced on a factory list.')):
                    ps = list(dict.fromkeys((x[0], x[1][0]) for x in items if x[1]))
                    if ps:
                        p, rid, fl, rng, sk = _lowest(ps)
                        return _R('L5', p, text + (many if fl else ''), (rid,), fac, rng=rng, flags=fl,
                                  row=self._lrow(rid, sku), skip=sk)
            # Contract C15 R1: another brand's row only when nothing else prices the style (level L4c, grade D).
            m = self.calc_match(sku, fac, sku['group'], cross=True)
            if m:
                return self._calc_res(sku, fac, *m)
        if allow_default and self.defaults:
            if sku:
                for k, nm in (((sku['cat'], sku['fab']), 'category and fabric'),
                              ((sku['cat'], sku['brand']), 'category and brand'), ((sku['cat'],), 'category')):
                    if self.defaults.get(k):
                        v, fx, fxr = self.defaults[k]
                        return _R('L6', v, 'Default: weighted median of ledger costs for this %s.' % nm, (), fac,
                                  fx=fx, fxr=fxr, flags=('default',))
            elif brand_label:
                k = ('dress_shirt', BRAND_LABEL_CODE.get(brand_label, '?'))
                if self.defaults.get(k):
                    v, fx, fxr = self.defaults[k]
                    return _R('L6', v, 'Default for a legacy SKU: brand %s, dress shirt assumed.' % brand_label, (), fac,
                              fx=fx, fxr=fxr, flags=('default', 'legacy_brand_default'))
        return _R('L7', None, 'No price found. Needs a manual cost.', (), fac,
                  flags=('malformed_sku',) if sku is None else ())

# ── Warehouse cost per SKU: lot tiers (d2 section 4) over the ledger (+ optional history) ──
_LOT_JUNK = frozenset({'', 'N/A', 'NA', 'NAN', 'NONE', 'NULL', '.', '-', '0'})
_DATE_TXT_RE = re.compile(r'(?<!\d)(\d{1,2})[/.\-](\d{1,2})[/.\-](\d{4}|\d{2})(?!\d)')
_DATE6_RE = re.compile(r'(?<!\d)(\d{2})(\d{2})(\d{2})(?!\d)')
_LOT_TAG_RE = re.compile(r'(?<!\d)(\d{2})(\d{2})(\d{2})\s*$')


def _norm_name(s):
    return re.sub(r'[^A-Z0-9]', '', str(s or '').upper().replace('&', 'AND'))


def _valid_md(mm, dd):
    return 1 <= mm <= 12 and 1 <= dd <= 31


def _name_dates(s):
    s = str(s or '')
    out = set()
    for m in _DATE_TXT_RE.finditer(s):
        mm, dd, yy = int(m.group(1)), int(m.group(2)), int(m.group(3)) % 100
        if _valid_md(mm, dd):
            out.add((mm, dd, yy))
    for m in _DATE6_RE.finditer(s):
        mm, dd, yy = int(m.group(1)), int(m.group(2)), int(m.group(3))
        if _valid_md(mm, dd):
            out.add((mm, dd, yy))
    return out


def _lot_tag(lot):
    m = _LOT_TAG_RE.search(str(lot or ''))
    if not m:
        return None
    mm, dd, yy = int(m.group(1)), int(m.group(2)), int(m.group(3))
    return (mm, dd, yy) if _valid_md(mm, dd) else None


class _WarehouseCoster:
    """Warehouse cost of an exact SKU (d2 4.2-4.3):
    T1 lot label = a ledger PO name of the style (one ref; T1b: several, weighted);
    T2 the lot's MMDDYY tag is a date in the PO name of exactly one of the style's refs;
    T3 the style has one ref; T4 several refs, units-weighted; T5 no ref: calculator with the
    factory unknown. A T1/T2 match whose on-hand exceeds that ref's units becomes T4 (blended)."""

    def __init__(self, ci, history_rows=()):
        self.ci = ci
        self.by = defaultdict(dict)
        for L in ci.ledger:
            if L['ref']:
                self._add(L['b'], L['ref'], L['poName'], max(0, L['units']), 'cur', etd=L['etd'])
        for h in history_rows or ():
            if not isinstance(h, dict):
                continue
            ref = _u(h.get('production') or h.get('ref'))
            b = base_of(h.get('style'))
            if ref and b:
                u = h.get('units', h.get('po_units', h.get('ship_units')))
                self._add(b, ref, str(h.get('poName') or h.get('po_name') or '').strip(), max(0, _int(u)), 'hist')
        self.tiers = Counter()

    def _add(self, b, ref, pn, units, which, etd=None):
        e = self.by[b].get(ref)
        if e is None:
            e = self.by[b][ref] = {'cur': 0, 'hist': 0, 'names': set(), 'norms': set(), 'dates': None, 'pn': pn,
                                   'etd': None}
        e[which] += units
        if etd and (e['etd'] is None or etd < e['etd']):
            e['etd'] = etd
        if pn and pn not in e['names']:
            e['names'].add(pn)
            e['norms'].add(_norm_name(pn))
            e['dates'] = None
        if not e['pn'] and pn:
            e['pn'] = pn

    @staticmethod
    def _dates(e):
        if e['dates'] is None:
            e['dates'] = set().union(*[_name_dates(n) for n in e['names']]) if e['names'] else set()
        return e['dates']

    def cost(self, sku, onhand, lot, brand_label, rd=None):
        """rd: the SKU's receive date (YYYY-MM-DD) when known."""
        b = base_of(sku)
        refs = self.by.get(b)
        if not refs:
            res = self.ci.raw(b, 'UNKNOWN', brand_label=brand_label)
            self.tiers['T5'] += 1
            return dict(res, tier='T5', ref=None, fac='UNKNOWN')
        chosen, tier = None, None
        lot_s = str(lot or '').strip()
        if lot_s.upper() not in _LOT_JUNK:
            nl = _norm_name(lot_s)
            m = sorted(r for r, e in refs.items() if nl and nl in e['norms'])
            if m:
                chosen, tier = m, ('T1' if len(m) == 1 else 'T1b')
            else:
                tag = _lot_tag(lot_s)
                if tag:
                    m2 = sorted(r for r, e in refs.items() if tag in self._dates(e))
                    if len(m2) == 1:
                        chosen, tier = m2, 'T2'
        if chosen is None:
            chosen, tier = sorted(refs), ('T3' if len(refs) == 1 else 'T4')
        blended = False
        if tier in ('T1', 'T2') and len(refs) > 1:
            e = refs[chosen[0]]
            got = max(e['cur'], e['hist'])
            if 0 < got < onhand:
                chosen, tier, blended = sorted(refs), 'T4', True
        extra = []
        if tier == 'T3':
            # One ref for the style, but more on hand than that ref holds: earlier receipts are mixed in.
            e = refs[chosen[0]]
            got = max(e['cur'], e['hist'])
            if 0 < got < onhand:
                extra.append('blended_lot')
        if tier in ('T3', 'T4') and rd:
            etds = [refs[r]['etd'] for r in chosen if refs[r].get('etd')]
            if etds and len(etds) == len(chosen) and rd < min(etds):
                extra.append('ref_after_stock')
        self.tiers[tier] += 1
        return self._combine(b, chosen, refs, tier, blended, brand_label, extra)

    def _combine(self, b, chosen, refs, tier, blended, brand_label, extra=()):
        parts = []
        for ref in chosen:
            e = refs[ref]
            res = self.ci.raw(b, fac_of(ref), ref, e['pn'], brand_label=brand_label)
            parts.append((ref, max(e['cur'], e['hist']), res))
        priced = [x for x in parts if x[2]['price'] is not None]
        add = (('blended_lot',) if blended else ()) + tuple(extra)
        if len(parts) == 1 or not priced:
            ref, _, res = parts[0]
            out = dict(res, tier=tier, ref=ref if len(parts) == 1 else None, fac=fac_of(ref) if len(parts) == 1 else res['fac'])
            if add:
                out['flags'] = tuple(dict.fromkeys(tuple(res['flags']) + add))
            return out
        if sum(w for _, w, _ in priced) <= 0:
            priced = [(r, 1, res) for r, _, res in priced]
        tw = sum(w for _, w, _ in priced)
        price = sum(w * res['price'] for _, w, res in priced) / tw
        alt = sum(w * res['alt'] for _, w, res in priced) / tw
        amt = sum(w * res['price'] for _, w, res in priced)
        fx = (sum(w * res['price'] * (res['fx'] or 0) for _, w, res in priced) / amt) if amt else 0.0
        fxr = _ref_mix((w * res['price'] * (res['fx'] or 0), res.get('fxr')) for _, w, res in priced)
        lv = _worst_level(res['level'] for _, _, res in priced)
        ids = tuple(dict.fromkeys(i for _, _, res in priced for i in res['ids']))
        flags = set(f for _, _, res in priced for f in res['flags'])
        flags.update(add)
        if len(priced) < len(parts):
            flags.add('partial_cost')
        fw = Counter()
        for ref, w, _ in priced:
            fw[fac_of(ref)] += w
        top, tw2 = fw.most_common(1)[0], sum(fw.values())
        fac = top[0] if len(fw) == 1 or top[1] >= 0.6 * tw2 else 'MIX'
        worst = next(res for _, _, res in priced if res['level'] == lv)
        vals = [res['price'] for _, _, res in priced]
        top = max(priced, key=lambda x: x[1])[2]      # contract C15: the sheet row of the ref with the most units
        return {'level': lv, 'price': price, 'alt': alt, 'fx': fx, 'fxr': fxr if fx else None, 'ids': ids, 'fac': fac, 'ref': None,
                'rng': (min(vals), max(vals)), 'flags': tuple(sorted(flags)), 'tier': tier,
                'basis': 'Weighted average of the %d refs that made this style. Least certain part: %s'
                         % (len(priced), worst['basis']), 'row': top.get('row'), 'skip': tuple(top.get('skip') or ())}


# ── Dataset builder ──
def _rows(v):
    return [r for r in v if isinstance(r, dict)] if isinstance(v, (list, tuple)) else []


def _mdy_iso(s):
    s = str(s or '').strip()
    m = re.match(r'^(\d{1,2})[-/](\d{1,2})[-/](\d{4})$', s)
    if m:
        try:
            return date(int(m.group(3)), int(m.group(1)), int(m.group(2))).isoformat()
        except ValueError:
            return None
    return _d10(s)


def _table(fields, rows):
    return {'fields': list(fields), 'rows': [[r.get(f) for f in fields] for r in rows]}


LINE_FIELDS = ('id', 'ctrlNo', 'orderNo', 'cust', 'type', 'style', 'base', 'brand', 'cat', 'fab', 'fiber',
               'units', 'price', 'rev', 'start', 'cancel', 'late', 'fobLine', 'wh',
               'fobU', 'fob', 'duty', 'freight', 'fees', 'cogs', 'deduct', 'net', 'gp', 'royalty', 'contrib',
               'level', 'grade', 'routing', 'factory', 'ref', 'origin', 'fxShare', 'ev', 'flags', 'basis',
               'dutyRegime', 'costRef', 'pieces', 'fxRef', 'revCost')
ALLOC_FIELDS = ('line', 'units', 'kind', 'factory', 'ref', 'poName', 'landing', 'etd', 'arrival', 'fobU', 'level',
                'routing', 'forced', 'costRef', 'lotTier', 'pieces', 'fxShare', 'fxRef', 'revCost')
APO_FIELDS = ('id', 'cust', 'custName', 'po', 'style', 'base', 'brand', 'cat', 'fiber', 'units', 'estPrice',
              'priceBasis', 'rev', 'fobU', 'fob', 'duty', 'freight', 'fees', 'cogs', 'deduct', 'net', 'gp', 'royalty',
              'contrib', 'level', 'grade', 'factory', 'origin', 'fxShare', 'flags', 'routing', 'ref', 'basis', 'ev',
              'dutyRegime', 'costRef', 'coveredBy', 'fxRef', 'revCost')
INVENTORY_FIELDS = ('sku', 'base', 'brand', 'cat', 'fiber', 'wh', 'units', 'fobU', 'landedU', 'fob', 'landed', 'level',
                    'grade', 'factory', 'ref', 'lotTier', 'receiveDate', 'ageDays', 'origin', 'fxShare', 'basis', 'ev',
                    'dutyRegime', 'flags', 'fxRef')
PRODUCTION_FIELDS = ('ref', 'factory', 'poName', 'style', 'base', 'brand', 'cat', 'fiber', 'units', 'etd', 'arrival',
                     'landing', 'fobLanding', 'fobU', 'fob', 'landedU', 'landed', 'level', 'grade', 'claimed', 'free',
                     'origin', 'fxShare', 'flags', 'basis', 'ev', 'dutyRegime', 'fxRef', 'ownFobU', 'ownFxShare',
                     'ownFxRef')
STYLE_FIELDS = ('base', 'brand', 'cat', 'fab', 'fiber', 'fit', 'sleeve', 'pat', 'fobU', 'landedU', 'level', 'grade',
                'rangeLo', 'rangeHi', 'factories', 'onHand', 'onHandFob', 'onHandLanded', 'incoming', 'incomingFob',
                'ats', 'committed', 'allocated', 'openUnits', 'openRev', 'openGp', 'openContrib', 'apoUnits', 'apoRev',
                't12Units', 't12Rev', 't12Cogs', 't12Gp', 'lifeUnits', 'lifeRev', 'expPrice', 'atsPotentialGp', 'ladder',
                'basis', 'ev', 't12Net', 'dutyRegime', 'dedPct', 'atsFreeStock', 'atsFreeProd', 'fxShare', 'fxRef',
                'openRevCost', 'costByFactory', 'costRule', 'costSpread', 'costRow', 'costRowsSkipped', 'costFlags')
SHIPPED_COMPANY_FIELDS = ('month', 'units', 'rev', 'fob', 'duty', 'freight', 'fees', 'cogs', 'deduct', 'net', 'gp',
                          'royalty', 'contrib', 'costedShare', 'costedRev', 'revCost')
SHIPPED_STYLE_FIELDS = ('base', 'brand', 'cat', 'fiber', 'dedPct', 'fobShare', 'months', 'fobU', 'level', 'grade',
                        'origin', 'fxShare', 'fxRef', 'caShare')
# Contract C14: lifetime invoices per account and style, costed like order lines (pieces appended: kit
# programs are invoiced by the carton, like their order lines).
SHIPPED_CUSTOMER_FIELDS = ('cust', 'base', 'brand', 'cat', 'fiber', 'origin', 'units', 'rev', 'fobU', 'fob', 'duty',
                           'freight', 'fees', 'cogs', 'deduct', 'net', 'gp', 'royalty', 'revCost', 'contrib', 'level',
                           'grade', 'dutyRegime', 'fxShare', 'fxRef', 'pieces')
_COVERAGE_SETS = ('openBook', 'bulk', 'apo', 'inventory', 'production')


class _Build:
    def __init__(self, src, costbook, settings, overrides, now_iso, routing):
        self.src = src if isinstance(src, dict) else {}
        self.cb = costbook if isinstance(costbook, dict) else {}
        self.params = clean_params(self.cb.get('params'))[0]
        self.S = merge_settings(settings, self.params)
        self.overrides = overrides if isinstance(overrides, list) else []
        self.now_iso = now_iso if isinstance(now_iso, str) else None
        self.rt = routing
        self.today = _d10(self.src.get('today')) or _d10(self.now_iso) or '2000-01-01'
        self.today_d = date.fromisoformat(self.today)
        self.basis_keys = {}
        self.cov = {k: defaultdict(lambda: [0, 0.0, 0.0]) for k in _COVERAGE_SETS}
        self.gcov = {k: defaultdict(lambda: [0, 0.0]) for k in _COVERAGE_SETS}
        self.ev_used = set()
        self._gen = {}
        self.notes = []
        self.hist_rows = []            # shipped.byCustomer rows (contract C14)

    # ── helpers ──
    def bkey(self, text):
        k = self.basis_keys.get(text)
        if k is None:
            k = self.basis_keys[text] = 'b%d' % (len(self.basis_keys) + 1)
        return k

    def ev(self, ids):
        out = list(dict.fromkeys(ids))[:_EV_CAP]
        self.ev_used.update(out)
        return out

    def comp(self, units, res, fac, ref, kind, arow=None):
        """One costed component. Warehouse components are capped at grade B unless their lot tier
        is T1 or T2 (contract C5): only a lot match ties a carton to the ref whose price is used. A
        combined cost (contract C13) caps the grade at its own grade (gcap)."""
        u, fx = self.ci.eff(res)
        tier = res.get('tier') if kind == 'warehouse' else None
        cap = max('B' if kind == 'warehouse' and tier not in ('T1', 'T2') else 'A', res.get('gcap') or 'A')
        return {'u': units, 'p': u, 'fx': fx, 'fxr': self.ci.fx_ref(res), 'pb': res['price'], 'lv': res['level'], 'fac': fac, 'ref': ref,
                'ids': res['ids'], 'fl': res['flags'], 'basis': res['basis'], 'kind': kind, 'a': arow,
                'tier': tier, 'cap': cap, 'comb': bool(res.get('rule'))}

    def wh_fac(self, fac):
        return 'WH' if fac in ('UNKNOWN', None) else fac

    def generic(self, sku, b):
        """Style cost with no placement: the style's ledger mix, else its warehouse cost, else
        the calculator with the factory unknown. -> [(share, res, factory, ref, kind)]. Every res here
        has passed CostIndex.final (contract C13)."""
        key = (sku, b)
        if key in self._gen:
            return self._gen[key]
        lines = self.led_by_base.get(b) or []
        if lines:
            tot = sum(L['units'] for L in lines)
            out = [(L['units'] / tot, L['use'], L['fac'], L['ref'] or None, 'ledger') for L in lines]
        else:
            rows = [sku] if sku in self.wh else list(self.wh_by_base.get(b) or [])
            rows = [s for s in rows if self.wh[s]['price'] is not None]
            if rows:
                tot = sum(max(self.inv[s]['onhand'], 0) for s in rows)
                if tot <= 0:
                    w = self.wh[rows[0]]
                    out = [(1.0, w, self.wh_fac(w['fac']), w.get('ref'), 'warehouse')]
                else:
                    out = [(self.inv[s]['onhand'] / tot, self.wh[s], self.wh_fac(self.wh[s]['fac']), self.wh[s].get('ref'),
                            'warehouse') for s in rows if self.inv[s]['onhand'] > 0]
            else:
                res = self.ci.final(b, self.ci.raw(b, 'UNKNOWN'))
                out = [(1.0, res, 'UNKNOWN', None, 'calculator')]
        self._gen[key] = out
        return out

    def alloc_comps(self, sku, b, a):
        kind = a.get('kind')
        if kind == 'production' and a.get('ledgerIndex') is not None and 0 <= a['ledgerIndex'] < len(self.ci.ledger):
            L = self.ci.ledger[a['ledgerIndex']]
            return [self.comp(a['units'], L['use'], L['fac'], L['ref'] or None, 'production', a)]
        if kind == 'warehouse':
            w = self.wh.get(sku)
            if w is not None and w['price'] is not None:
                return [self.comp(a['units'], w, self.wh_fac(w['fac']), w.get('ref'), 'warehouse', a)]
            return [self.comp(a['units'] * sh, res, self.wh_fac(fac) if k == 'warehouse' else fac, ref, 'warehouse', a)
                    for sh, res, fac, ref, k in self.generic(sku, b)]
        return [self.comp(a['units'] * sh, res, fac, ref, 'unsourced', a) for sh, res, fac, ref, _ in self.generic(sku, b)]

    def agg(self, comps):
        """Unit cost and labels of a set of components (unit-weighted)."""
        tu = sum(c['u'] for c in comps)
        priced = bool(comps) and all(c['p'] is not None for c in comps)
        lv = _worst_level(c['lv'] for c in comps) or 'L7'
        flags = set(f for c in comps for f in c['fl'])
        unit = fx = fxr = None
        if priced and all(c.get('comb') for c in comps) and len({(c['p'], c['fx'], c.get('fxr')) for c in comps}) == 1:
            # Every component carries the same combined cost (contract C13): take it as it is, so float noise in a
            # weighted average can never move its 4-decimal rounding. One style, one cost on every row.
            unit, fx, fxr = comps[0]['p'], comps[0]['fx'], comps[0].get('fxr')
        elif priced:
            if tu > 0:
                amt = sum(c['u'] * c['p'] for c in comps)
                unit = amt / tu
                fx = (sum(c['u'] * c['p'] * c['fx'] for c in comps) / amt) if amt else 0.0
                fxr = _ref_mix((c['u'] * c['p'] * c['fx'], c.get('fxr')) for c in comps)
            else:
                unit = sum(c['p'] for c in comps) / len(comps)
                fx = sum(c['fx'] for c in comps) / len(comps)
                fxr = _ref_mix((c['p'] * c['fx'], c.get('fxr')) for c in comps)
        else:
            if any(c['p'] is not None for c in comps):
                flags.add('partial_cost')
            lv = 'L7'
        fw, rw, pw, ow = Counter(), Counter(), Counter(), Counter()
        for c in comps:
            w = c['u'] if tu > 0 else 1
            fw[c['fac']] += w
            if c['ref']:
                rw[c['ref']] += w
                if c.get('kind') == 'production':
                    pw[c['ref']] += w
            ow[c['fac']] += w * (c['p'] or 0) if priced else w
        tw = sum(fw.values())
        fac = None
        if fw:
            top = fw.most_common(1)[0]
            fac = top[0] if top[1] >= 0.6 * tw else 'MIX'
        # ref: the physical production ref (production pulls only). costRef: the ref whose price was
        # used, whatever the supply kind (contract C5).
        ref = pw.most_common(1)[0][0] if pw else None
        cost_ref = rw.most_common(1)[0][0] if rw else None
        ofac = ow.most_common(1)[0][0] if ow else None
        worst = next((c for c in comps if c['lv'] == lv), None)
        tiers = {c.get('tier') for c in comps if c.get('kind') == 'warehouse'}
        return {'unit': unit, 'fx': fx, 'fxr': fxr, 'level': lv, 'factory': fac, 'ref': ref, 'costRef': cost_ref,
                'origin': _factory_origin(self.S, ofac) if ofac else _factory_origin(self.S, '_default'),
                'flags': flags, 'ids': [i for c in comps for i in c['ids']], 'priced': priced,
                'cap': max([c.get('cap') or 'A' for c in comps] or ['A']),
                'lotTier': next(iter(tiers)) if len(tiers) == 1 else None,
                'basis': worst['basis'] if worst else 'No price found. Needs a manual cost.'}

    def feed_label(self, b):
        """The ATS feed's brand label for a base style (any of its SKUs), or None."""
        cache = self.__dict__.setdefault('_flab', {})
        if b not in cache:
            lab = None
            for s in (getattr(self, 'inv_by_base', {}) or {}).get(b) or ():
                lab = (self.inv.get(s) or {}).get('label')
                if lab:
                    break
            cache[b] = lab
        return cache[b]

    def base_label(self, b):
        """The feed's brand label for a base style (any of its SKUs), else the invoice history's."""
        return self.feed_label(b) or ((getattr(self, 'an', None) or {}).get(b) or {}).get('label')

    def brand_of(self, dec, sku=None, label=None, base=None):
        """Brand code of a row (contract C7): the decoded SKU brand (program map included); else the
        feed label; else the code's own brand letters (program and legacy codes); else ''. SKU brand
        BL with the feed label BLACK is Black Label (BLK), which carries its own royalty row. A style row
        (sku None: styles, shipped history) reads the style's feed label before the label passed in, so a
        Black Label style whose invoices say BLO stays Black Label (review finding R5-1)."""
        b = base or (dec['base'] if dec else (base_of(sku) if sku else ''))
        lab = _u(self.feed_label(b)) if (sku is None and b) else ''
        if not lab and label:
            lab = _u(label)
        if not lab and sku:
            lab = _u((getattr(self, 'inv', {}).get(_u(sku)) or {}).get('label'))
        if not lab and b:
            lab = _u(self.base_label(b))
        if dec and dec.get('brand'):
            br = dec['brand']
            return 'BLK' if br == 'BL' and lab in _BLACK_LABELS else br
        if lab in _BLACK_LABELS:
            return 'BLK'
        if lab and BRAND_LABEL_CODE.get(lab):
            return BRAND_LABEL_CODE[lab]
        return program_brand(b) or ''

    def dest_regime(self, code):
        """Duty regime of an A2000 wh, ledger landing or stock warehouse (settings.destinations)."""
        return self.S['destinations'].get(_u(code), 'us') if code else 'us'

    def cust_regime(self, cust, code=None):
        """Duty regime of a row with a customer (contract C12): settings.regimeByCustomer first, then
        'none' for an FOB customer, then settings.destinations for the wh or landing, then 'us'."""
        r = self.S['regimeByCustomer'].get(cust)
        if r in DUTY_REGIMES:
            return r
        if cust in self.fob_set:
            return 'none'
        return self.dest_regime(code)

    @staticmethod
    def hist_code(c):
        """Account code for an invoice-history customer code (drop-ship and legacy aliases)."""
        c = _u(c)
        return HISTORY_CUSTOMER_ALIAS.get(c, c)

    def cust_info(self, code):
        g = _cust_group(code, self.S, self.fob_set)
        return g, _ded_pct(code, self.S, g)

    # ── inputs ──
    def load_inputs(self):
        s = self.src
        self.inv_src = s.get('inventory') if isinstance(s.get('inventory'), dict) else {}
        self.led_src = s.get('ledger') if isinstance(s.get('ledger'), dict) else {}
        self.apo_src = s.get('apo') if isinstance(s.get('apo'), dict) else {}
        self.oo_src = s.get('open_orders') if isinstance(s.get('open_orders'), dict) else {}
        self.inv_items = _rows(self.inv_src.get('items'))
        self.ledger_rows = _rows(self.led_src.get('rows'))
        self.apo_rows = _rows(self.apo_src.get('rows'))
        self.orders = _rows(self.oo_src.get('orders'))
        self.manual = _rows(s.get('manual_allocations'))
        self.vw = _rows(s.get('vw_allocations'))
        live = [_u(c) for c in (s.get('fob_customers') or []) if _u(c)]
        fc = self.S.get('fobCustomers')
        if isinstance(fc, list):
            self.fob_set, self.fob_src = frozenset(_u(c) for c in fc if _u(c)), 'settings'
        elif live:
            self.fob_set, self.fob_src = frozenset(live), 'live'
        else:
            self.fob_set = frozenset(getattr(self.rt, 'ENGINE_FOB_CODES', None) or _ENGINE_FOB_FALLBACK)
            self.fob_src = 'engine'
        hist = s.get('ledger_history')
        self.history = _rows(hist.get('rows') if isinstance(hist, dict) else hist)
        sa = s.get('sales_analytics')
        self.sa = sa if isinstance(sa, dict) else None

    def route(self):
        ri = self.src.get('routing_inputs') if isinstance(self.src.get('routing_inputs'), dict) else {}
        R = self.S['routing']
        opts = {'picksAsWarehouse': R.get('picksAsWarehouse') is True, 'honorAssignments': R.get('honorAssignments') is not False,
                'gateFallback': 'engine' if R.get('gateFallback') == 'engine' else 'fifo'}
        self.R = self.rt.route_all(self.inv_items, self.ledger_rows, self.orders, self.apo_rows, self.vw, self.manual,
                                   ri, self.today, fob_codes=None, options=opts)
        self.keys = self.rt.line_keys(self.orders)

    def merge_inventory(self):
        inv = {}
        for it in self.inv_items:
            sku = _u(it.get('sku'))
            if not sku:
                continue
            m = inv.get(sku)
            if m is None:
                m = inv[sku] = {'sku': sku, 'b': base_of(sku), 'label': None, 'lot': None, 'rd': None, 'ats': 0,
                                'committed': 0, 'allocated': 0, 'incoming': 0, 'jtw': 0, 'tr': 0, 'dcw': 0, 'qa': 0,
                                'nj': 0, 'abfi': 0}
            # Raw sums, as pnl_routing.merge_inventory and the presentation stock cards do, so the
            # P&L shows and costs the same stock its own routing routes. A cell that nets negative
            # yields no stock row (build_inventory keeps positive cells only).
            for k, _ in _STOCK_KEYS:
                m[k] += _int(it.get(k))
            m['incoming'] += _int(it.get('incoming'))
            m['ats'] += _int(it.get('total_ats'))
            for k in ('committed', 'allocated'):
                v = _int(it.get(k))
                if abs(v) > abs(m[k]):
                    m[k] = v
            if not m['label'] and _u(it.get('brand')):
                m['label'] = _u(it.get('brand'))
            lot = str(it.get('lot_number') or '').strip()
            if not m['lot'] and lot.upper() not in _LOT_JUNK:
                m['lot'] = lot
            if not m['rd']:
                m['rd'] = _mdy_iso(it.get('receive_date'))
        for m in inv.values():
            m['onhand'] = sum(m[k] for k, _ in _STOCK_KEYS)
        self.inv = inv
        self.inv_by_base = defaultdict(list)
        for sku, m in inv.items():
            self.inv_by_base[m['b']].append(sku)

    def cost_supply(self):
        self.ci = CostIndex(self.cb, self.S, self.overrides, self.ledger_rows, today=self.today,
                            history_rows=self.history)
        self.whc = _WarehouseCoster(self.ci, self.history)
        # Contract C13: every table takes a style's cost through CostIndex.final, the one choke point. The
        # warehouse cost of a SKU and each ledger line's cost ('use') pass it here; generic() and style_cost()
        # pass it for the calculator fallback. L['res'] keeps the line's own factory ladder result.
        self.wh = {sku: self.ci.final(m['b'], self.whc.cost(sku, m['onhand'], m['lot'], m['label'], m['rd']))
                   for sku, m in self.inv.items()}
        for L in self.ci.ledger:
            L['use'] = self.ci.final(L['b'], L['res'])
        self.wh_by_base = defaultdict(list)
        for sku, m in self.inv.items():
            self.wh_by_base[m['b']].append(sku)
        self.led_by_base = defaultdict(list)
        for L in self.ci.ledger:
            if L['units'] > 0 and L['use']['price'] is not None:
                self.led_by_base[L['b']].append(L)

    # ── open order lines ──
    def build_lines(self):
        LA, LS = self.R.get('lineAlloc') or {}, self.R.get('lineSummary') or {}
        self.lines, self.alloc = [], []
        # (customer code, base) of every APO row: a bulk line on the same program base for the same
        # customer is the same goods booked twice (the BJ's kit program is the known case).
        apo_keys = set()
        for a in self.apo_rows:
            text = _u(a.get('customer'))
            for c in (APO_CUSTOMER_CODES.get(text) or [text]):
                apo_keys.add((c, base_of(a.get('style'))))
        self.bulk_keys = {}                                   # (cust, base) -> bulk line id
        self.cust_line = defaultdict(lambda: [0, 0.0])        # (cust, base) -> [pieces, revenue], any line type
        self.cust_regimes = defaultdict(set)                  # cust -> duty regimes of its open lines
        for i, o in enumerate(self.orders):
            key = self.keys[i]
            sku = _u(o.get('style'))
            b = base_of(sku)
            dec = self.ci.decode(b)
            q = max(0, _int(o.get('openQty')) + _int(o.get('pickQty')))
            pcs = self.ci.kit_pcs.get(b, 1)
            price = _num(o.get('salesPrice'))
            ov, pv = o.get('openValue'), o.get('pickValue')
            rev = r2(_num(ov) + _num(pv)) if (ov is not None or pv is not None) else r2(q * price)
            no_units = q <= 0
            if no_units:
                rev = 0.0          # no open units: a stale open or pick value adds no revenue
            typ = 'bulk' if (str(o.get('reportType') or '').lower() == 'bulk' or o.get('isPipeline') is True) else 'a2000'
            cust, wh = _u(o.get('customer')), _u(o.get('wh'))
            fob_cust = cust in self.fob_set
            fob_line = fob_cust or wh == 'CH'
            regime = self.cust_regime(cust, wh)        # C12: customer setting, FOB list, destinations
            start, cancel = _d10(o.get('startDate')), _d10(o.get('cancelDate'))
            late = 1 if cancel and cancel < self.today else 0
            summ = LS.get(key) or {}
            comps, arows = [], []
            for a in LA.get(key) or []:
                cs = self.alloc_comps(sku, b, a)
                comps += cs
                arows.append((a, cs))
            if q > 0 and not comps:
                comps = [self.comp(q * sh, res, fac, ref, 'unsourced') for sh, res, fac, ref, _ in self.generic(sku, b)]
            shown = comps if q > 0 else [self.comp(0, res, fac, ref, 'unsourced') for sh, res, fac, ref, _ in self.generic(sku, b)]
            ag = self.agg(shown)
            fobU = r4(ag['unit']) if ag['priced'] else None
            # Contract C2 rounding: the unit cost is rounded to 4 decimals first, then fob = r2(pieces x fobU).
            fob = None if fobU is None else (0.0 if q <= 0 else r2(q * pcs * fobU))
            cat = dec['cat'] if dec else 'other'
            fiber = dec['fiber'] if dec else 'mmf'
            brand = self.brand_of(dec, sku, base=b)
            group, ded = self.cust_info(cust)
            money = line_money(rev, fob, q * pcs, cat, fiber, ag['origin'], regime, ded, _roy_pct(brand, self.S), self.S)
            # d2 5.3: R6 only when nothing is placed; a partly unsourced line keeps the label of its
            # placed units and carries the 'unsourced' flag.
            routing = (_worst_routing(a.get('routing') for a, _ in arows if a.get('kind') != 'unsourced')
                       or ('R6' if q > 0 else None))
            level = ag['level'] if fobU is not None else 'L7'
            flags = set(ag['flags'])
            if typ == 'bulk':
                flags.add('bulk')
            if pcs > 1:
                flags.add('kit')
            if late:
                flags.add('late')
            if fob_line and regime == 'none':
                flags.add('fob_line')
            if regime != 'us' and not fob_line:
                flags.add('non_us_dest')
            if cust in self.S['regimeByCustomer']:
                flags.add('customer_regime')
            if no_units:
                flags.add('no_units')
            if typ == 'bulk' and (cust, b) in apo_keys:
                flags.add('supply_in_allocations')
            if _int(summ.get('unsourced')) > 0:
                flags.add('unsourced')
            if summ.get('status') in ('not_in_feed', 'no_style'):
                flags.add('not_in_feed')
            if summ.get('mode') == 'fifo':
                flags.add('gate_fail')
            if summ.get('mode') == 'assignment':
                flags.add('assignment')
            for a, _ in arows:
                if a.get('forced'):
                    flags.add('forced')
                if a.get('fobWhFallback'):
                    flags.add('fob_wh_fallback')
            row = {'id': key, 'ctrlNo': str(o.get('ctrlNo') or ''), 'orderNo': str(o.get('orderNo') or o.get('po') or ''),
                   'cust': cust, 'type': typ, 'style': sku, 'base': b, 'brand': brand, 'cat': cat,
                   'fab': dec['fab'] if dec else None, 'fiber': fiber, 'units': q, 'price': r4(price), 'rev': rev,
                   'start': start, 'cancel': cancel, 'late': late, 'fobLine': 1 if fob_line else 0, 'wh': wh or None,
                   'fobU': fobU, 'fob': fob, 'level': level, 'grade': max(grade_of(level, routing), ag['cap']),
                   'routing': routing, 'factory': ag['factory'], 'ref': ag['ref'], 'origin': ag['origin'],
                   'fxShare': r4(ag['fx']) if fobU is not None else 0.0, 'ev': self.ev(ag['ids']),
                   'flags': sorted(flags), 'basis': self.bkey(ag['basis']), 'dutyRegime': regime,
                   'costRef': ag['costRef'], 'pieces': q * pcs,
                   'fxRef': _fxref_out(r4(ag['fx']) if fobU is not None else 0.0, ag['fxr'])}
            row.update(money)
            row['_pcs'], row['_group'], row['_comps'] = pcs, group, comps
            self.lines.append(row)
            if q > 0:
                self.cust_regimes[cust].add(regime)
                cl = self.cust_line[(cust, b)]
                cl[0] += q * pcs
                cl[1] += rev
                if typ == 'bulk':
                    self.bulk_keys.setdefault((cust, b), key)
            cset = 'bulk' if typ == 'bulk' else 'openBook'
            if q > 0:
                # Coverage counts pieces, so a kit carton adds its pieces, like its cost does.
                for c in comps:
                    cell = self.cov[cset][c['lv']]
                    cell[0] += c['u'] * pcs
                    cell[1] += rev * c['u'] / q
                    cell[2] += c['u'] * pcs * (c['p'] or 0)
                g = self.gcov[cset][row['grade']]
                g[0] += q * pcs
                g[1] += rev
            for a, cs in arows:
                aa = self.agg(cs)
                ash = r4(aa['fx']) if aa['priced'] else 0.0
                prod = a.get('kind') == 'production'
                self.alloc.append({'line': key, 'units': a['units'], 'kind': a.get('kind'),
                                   'factory': (self.ci.ledger[a['ledgerIndex']]['fac'] if prod
                                               and a.get('ledgerIndex') is not None else aa['factory']),
                                   'ref': a.get('ref') if prod else None, 'poName': a.get('poName'),
                                   'landing': a.get('landing'), 'etd': a.get('etd'), 'arrival': a.get('arrival'),
                                   'fobU': r4(aa['unit']) if aa['priced'] else None, 'level': aa['level'],
                                   'routing': a.get('routing'), 'forced': 1 if a.get('forced') else 0,
                                   'costRef': aa['costRef'], 'lotTier': aa['lotTier'], 'pieces': a['units'] * pcs,
                                   'fxShare': ash, 'fxRef': _fxref_out(ash, aa['fxr']),
                                   # The line's revenue costs for these units, pro rata (C11).
                                   'revCost': (r2(money['revCost'] * a['units'] / q)
                                               if money['revCost'] is not None and q > 0 else None)})

    # ── invoice analytics (shipped lens) ──
    _AN_FIELDS = ('style', 'brand', 'qty', 'value', 'firstInv', 'lastInv', 'months', 'customers', 'color')

    def index_analytics(self):
        """Invoice analytics for the shipped lens. The lens is optional: a malformed row or a changed
        payload shape never fails the build. Unreadable fields are skipped and counted; anything
        unexpected leaves the lens 'invalid' and the rest of the dataset builds as usual."""
        self.an, self.months = {}, []
        self.an_skipped, self.an_problem = 0, None
        sa = self.sa
        self.sa_src = sa.get('source') if isinstance(sa, dict) and isinstance(sa.get('source'), dict) else {}
        if not sa:
            self.an_state = 'missing'
            return
        if sa.get('building') or (sa.get('ready') is False and not sa.get('styles')):
            self.an_state = 'building'
            return
        try:
            self._index_analytics(sa)
        except Exception as e:  # noqa: BLE001  (an optional lens must not take the P&L down)
            self.an, self.months = {}, []
            self.an_state, self.an_problem = 'invalid', type(e).__name__

    def _index_analytics(self, sa):
        fields = sa.get('fields')
        if not (isinstance(fields, (list, tuple)) and fields and all(isinstance(f, str) for f in fields)):
            fields = self._AN_FIELDS
        ix = {f: i for i, f in enumerate(fields)}
        styles = sa.get('styles')
        if not isinstance(styles, (list, tuple)):
            self.an_state, self.an_problem = 'invalid', 'styles'
            return

        def get(row, f):
            if isinstance(row, dict):
                return row.get(f)
            i = ix.get(f)
            return row[i] if isinstance(row, (list, tuple)) and i is not None and i < len(row) else None
        for row in styles:
            st = get(row, 'style')
            st = _u(st) if isinstance(st, (str, int)) and not isinstance(st, bool) else ''
            if not st:
                self.an_skipped += 1
                continue
            months, custs = get(row, 'months'), get(row, 'customers')
            if (months is not None and not isinstance(months, dict)) or (custs is not None and not isinstance(custs, dict)):
                self.an_skipped += 1          # the readable fields of the row still count
            e = self.an.setdefault(st, {'label': None, 'qty': 0.0, 'value': 0.0, 'months': defaultdict(lambda: [0.0, 0.0]),
                                        'customers': defaultdict(lambda: [0.0, 0.0])})
            lab = get(row, 'brand')
            e['label'] = e['label'] or (_u(lab) if isinstance(lab, str) and lab.strip() else None)
            e['qty'] += _num(get(row, 'qty'))
            e['value'] += _num(get(row, 'value'))
            for ym, v in (months.items() if isinstance(months, dict) else ()):
                if isinstance(v, (list, tuple)) and len(v) >= 2:
                    e['months'][str(ym)[:7]][0] += _num(v[0])
                    e['months'][str(ym)[:7]][1] += _num(v[1])
            for c, v in (custs.items() if isinstance(custs, dict) else ()):
                if isinstance(v, (list, tuple)) and len(v) >= 2:
                    e['customers'][_u(c)][0] += _num(v[0])
                    e['customers'][_u(c)][1] += _num(v[1])
        end = _d10(self.sa_src.get('to'))
        if end:
            y, m = int(end[:4]), int(end[5:7])
        else:
            y, m = self.today_d.year, self.today_d.month - 1
            if m == 0:
                y, m = y - 1, 12
        for k in range(11, -1, -1):
            yy, mm = y, m - k
            while mm <= 0:
                yy, mm = yy - 1, mm + 12
            self.months.append('%04d-%02d' % (yy, mm))
        self.an_state = 'ready'
        for e in self.an.values():
            e['t12'] = [sum(e['months'][ym][0] for ym in self.months if ym in e['months']),
                        sum(e['months'][ym][1] for ym in self.months if ym in e['months'])]

    # ── APO allocations ──
    def build_apo(self):
        # Every brand key here comes from brand_of, the resolver of the lines' brand field: the decoded
        # SKU, else the feed or invoice label of a legacy code (review numbers-engine F2). So a legacy
        # open line or invoice counts for its brand, and Black Label (BLK) stays apart from BL.
        # Sums are per piece, so a kit line cannot put a carton price into a brand average.
        cbp, bbp, spp = defaultdict(lambda: [0.0, 0.0]), defaultdict(lambda: [0.0, 0.0]), defaultdict(lambda: [0.0, 0.0])
        for row in self.lines:
            if row['type'] != 'a2000' or row['units'] <= 0:
                continue
            u, val = row['pieces'], row['units'] * row['price']
            spp[row['base']][0] += u
            spp[row['base']][1] += val
            br = row['brand'] or None
            if br:
                cbp[(row['cust'], br)][0] += u
                cbp[(row['cust'], br)][1] += val
                if row['cust'] in _APO_BIG3:
                    bbp[br][0] += u
                    bbp[br][1] += val
        # Lifetime invoice history by (account, base) and by (account, brand). Kit programs are
        # invoiced per carton, so they are left out.
        hist_style, hist_brand = defaultdict(lambda: [0.0, 0.0]), defaultdict(lambda: [0.0, 0.0])
        for st, e in self.an.items():
            hb = base_of(st)
            if hb in self.ci.kit_pcs:
                continue
            d = self.ci.decode(hb)
            # The lines' resolver first; a size-suffixed history key falls back to its own label.
            br = self.brand_of(d, hb, base=hb) or self.brand_of(d, hb, label=e.get('label'), base=hb) or None
            for c, v in e['customers'].items():
                if v[0] > 0 and v[1] > 0:
                    k = self.hist_code(c)
                    hist_style[(k, hb)][0] += v[0]
                    hist_style[(k, hb)][1] += v[1]
                    if br:
                        hist_brand[(k, br)][0] += v[0]
                        hist_brand[(k, br)][1] += v[1]
        self.apo = []
        AA = self.R.get('apoAlloc') or {}
        names = getattr(self.rt, 'ENGINE_CUSTOMER_NAMES', {}) or {}
        self.apo_names = {}
        for i, a in enumerate(self.apo_rows):
            sku = _u(a.get('style'))
            b = base_of(sku)
            dec = self.ci.decode(b)
            qty = max(0, _int(a.get('qty')))
            text = _u(a.get('customer'))
            codes = APO_CUSTOMER_CODES.get(text)
            if codes is None:
                codes = [text] if text in names or text in self.S['customerGroups'] else []
            cust = codes[0] if codes else (text or 'APO')
            if cust not in names:
                self.apo_names.setdefault(cust, str(a.get('customer') or 'APO').strip())
            brand = self.brand_of(dec, sku, base=b)
            grp = self.cust_info(cust)[0]
            br = brand or None            # the same resolver as the price maps above
            price, basis, pflags = None, 'none', set()
            # 1. This customer's open A2000 prices for the brand.
            if br:
                u = sum(cbp[(c, br)][0] for c in codes)
                if u > 0:
                    price, basis = sum(cbp[(c, br)][1] for c in codes) / u, 'customer_brand'
            # 2. This customer's own open line on the same style, bulk and kit lines included (per piece).
            if price is None:
                pu = sum(self.cust_line[(c, b)][0] for c in codes if (c, b) in self.cust_line)
                if pu > 0:
                    price, basis = sum(self.cust_line[(c, b)][1] for c in codes if (c, b) in self.cust_line) / pu, 'customer_line'
            # 3. Off-price customers: the Ross, TJX and Burlington open price for the brand.
            if price is None and br and grp == 'offprice' and bbp[br][0] > 0:
                price, basis = bbp[br][1] / bbp[br][0], 'offprice_brand'
                pflags.add('price_offprice')
            hist_ok = bool(codes) and b not in self.ci.kit_pcs
            # 4. This customer's own invoices for the same style.
            if price is None and hist_ok:
                hu = sum(hist_style[(c, b)][0] for c in codes if (c, b) in hist_style)
                if hu >= _MIN_HIST_UNITS_STYLE:
                    price, basis = sum(hist_style[(c, b)][1] for c in codes if (c, b) in hist_style) / hu, 'customer_invoice'
                    pflags.add('price_history')
            # 5. Open order prices for the style.
            if price is None and spp[b][0] > 0:
                price, basis = spp[b][1] / spp[b][0], 'style_open'
                pflags.add('price_proxy')
            # 6. The style's invoices in the last 12 months. Kit programs are invoiced per carton,
            # so they skip this rung, as the lifetime maps above do.
            if price is None and b not in self.ci.kit_pcs:
                e = self.an.get(b)
                if e and e.get('t12') and e['t12'][0] > 0 and e['t12'][1] > 0:
                    price, basis = e['t12'][1] / e['t12'][0], 'style_t12'
                    pflags.add('price_proxy')
            # 7. This customer's own invoices for the brand (a style with no price evidence of its own).
            if price is None and hist_ok and br:
                hu = sum(hist_brand[(c, br)][0] for c in codes if (c, br) in hist_brand)
                if hu >= _MIN_HIST_UNITS_BRAND:
                    price = sum(hist_brand[(c, br)][1] for c in codes if (c, br) in hist_brand) / hu
                    basis = 'customer_brand_invoice'
                    pflags.update(('price_history', 'price_proxy'))
            # 8. Last resort for other customers: the off-price price for the brand, flagged as such.
            if price is None and br and bbp[br][0] > 0:
                price, basis = bbp[br][1] / bbp[br][0], 'offprice_brand'
                pflags.update(('price_offprice', 'price_proxy'))
            if price is None:
                pflags.add('unpriced')
            est = r4(price) if price is not None else None
            rev = r2(qty * est) if est is not None else None
            comps, arows = [], AA.get(i) or []
            for al in arows:
                comps += self.alloc_comps(sku, b, al)
            if qty > 0 and not comps:
                comps = [self.comp(qty * sh, res, fac, ref, 'unsourced') for sh, res, fac, ref, _ in self.generic(sku, b)]
            shown = comps if qty > 0 else [self.comp(0, res, fac, ref, 'unsourced') for sh, res, fac, ref, _ in self.generic(sku, b)]
            ag = self.agg(shown)
            fobU = r4(ag['unit']) if ag['priced'] else None
            fob = None if fobU is None else (r2(qty * fobU) if qty > 0 else 0.0)
            cat, fiber = (dec['cat'], dec['fiber']) if dec else ('other', 'mmf')
            # Duty regime (C12, then C2): the customer's own setting (regimeByCustomer, for the booked
            # account or its other codes) comes first. Then an FOB customer imports nothing. Otherwise an
            # allocation follows the customer's open orders when they all share one non-US regime (for
            # example orders that all ship factory direct). Anything else is a US import.
            fob_line = cust in self.fob_set
            rbc = self.S['regimeByCustomer']
            set_by = next((c for c in [cust] + list(codes) if c in rbc), None)
            derived = False
            if set_by is not None:
                regime = rbc[set_by]
            elif fob_line:
                regime = 'none'
            else:
                regs = set().union(*[self.cust_regimes.get(c, set()) for c in codes]) if codes else set()
                regime = next(iter(regs)) if len(regs) == 1 else 'us'
                derived = regime != 'us'
            group, ded = self.cust_info(cust)
            money = line_money(rev, fob, qty, cat, fiber, ag['origin'], regime, ded, _roy_pct(brand, self.S), self.S)
            routing = _worst_routing(al.get('routing') for al in arows) or ('R6' if qty > 0 else None)
            level = ag['level'] if fobU is not None else 'L7'
            flags = set(ag['flags']) | pflags
            if fob_line and regime == 'none':
                flags.add('fob_line')
            if derived:
                flags.update(('non_us_dest', 'regime_from_orders'))
            elif regime != 'us' and not fob_line:
                flags.add('non_us_dest')
            if set_by is not None:
                flags.add('customer_regime')
            if qty <= 0:
                flags.add('no_units')
            if any(al.get('kind') == 'unsourced' for al in arows):
                flags.add('unsourced')
            covered = next((self.bulk_keys[(c, b)] for c in codes if (c, b) in self.bulk_keys), None)
            if covered:
                flags.add('covered_by_bulk')
            row = {'id': 'A%d' % (i + 1), 'cust': cust, 'custName': str(a.get('customer') or '').strip() or cust,
                   'po': str(a.get('po') or '').strip(), 'style': sku, 'base': b, 'brand': brand, 'cat': cat,
                   'fiber': fiber, 'units': qty, 'estPrice': est, 'priceBasis': basis, 'rev': rev, 'fobU': fobU,
                   'fob': fob, 'level': level, 'grade': max(grade_of(level, routing), ag['cap']),
                   'factory': ag['factory'], 'origin': ag['origin'], 'fxShare': r4(ag['fx']) if fobU is not None else 0.0,
                   'flags': sorted(flags), 'routing': routing, 'ref': ag['ref'], 'basis': self.bkey(ag['basis']),
                   'ev': self.ev(ag['ids']), 'dutyRegime': regime, 'costRef': ag['costRef'], 'coveredBy': covered,
                   'fxRef': _fxref_out(r4(ag['fx']) if fobU is not None else 0.0, ag['fxr'])}
            row.update(money)
            row['_comps'] = comps
            self.apo.append(row)
            if qty > 0:
                for c in comps:
                    cell = self.cov['apo'][c['lv']]
                    cell[0] += c['u']
                    cell[1] += (rev or 0.0) * c['u'] / qty
                    cell[2] += c['u'] * (c['p'] or 0)
                g = self.gcov['apo'][row['grade']]
                g[0] += qty
                g[1] += rev or 0.0

    # ── stock rows ──
    def build_inventory(self):
        self.inventory = []
        for sku, m in self.inv.items():
            w = self.wh[sku]
            dec = self.ci.decode(m['b'])
            unit, fx = self.ci.eff(w)
            cat, fiber = (dec['cat'], dec['fiber']) if dec else ('other', 'mmf')
            brand = self.brand_of(dec, sku, m['label'])
            origin = _factory_origin(self.S, w['fac'])
            level = w['level'] if unit is not None else 'L7'
            age = None
            if m['rd']:
                try:
                    age = (self.today_d - date.fromisoformat(m['rd'])).days
                except ValueError:
                    age = None
            ev = self.ev(w['ids'])
            bk = self.bkey(w['basis'])
            fobU = r4(unit)
            share = r4(fx) if unit is not None else 0.0
            fxref = _fxref_out(share, self.ci.fx_ref(w))
            # C5: stock is tied to the ref whose price it uses only by a lot match (T1, T2). Anything
            # else is a style estimate, so it cannot be graded better than B. A combined cost (C13) also
            # caps the grade at its own grade.
            grade = max(grade_of(level), 'A' if w.get('tier') in ('T1', 'T2') else 'B', w.get('gcap') or 'A')
            for k, code in _STOCK_KEYS:
                units = m[k]
                if units <= 0:
                    continue
                regime = self.dest_regime(code)
                fob = r2(units * fobU) if fobU is not None else None
                lnd, lndU = stock_money(fob, units, cat, fiber, origin, regime, self.S)
                flags = set(w.get('flags') or ())
                if regime != 'us':
                    flags.add('non_us_dest')
                row = {'sku': sku, 'base': m['b'], 'brand': brand, 'cat': cat, 'fiber': fiber, 'wh': code, 'units': units,
                       'fobU': fobU, 'landedU': lndU, 'fob': fob, 'landed': lnd, 'level': level,
                       'grade': grade, 'factory': w['fac'], 'ref': w.get('ref'), 'lotTier': w.get('tier'),
                       'receiveDate': m['rd'], 'ageDays': age, 'origin': origin,
                       'fxShare': share, 'basis': bk, 'ev': ev,
                       'dutyRegime': regime, 'flags': sorted(flags), 'fxRef': fxref}
                self.inventory.append(row)
                cell = self.cov['inventory'][level]
                cell[0] += units
                cell[1] += fob or 0.0
                cell[2] += fob or 0.0
                g = self.gcov['inventory'][row['grade']]
                g[0] += units
                g[1] += fob or 0.0

    def double_bookings(self):
        """Refs of different factories that carry the same (style, units) lines."""
        by_ref = defaultdict(Counter)
        for L in self.ci.ledger:
            if L['ref'] and _REF_RE.match(L['ref']) and L['units'] > 0:
                by_ref[L['ref']][(L['style'], L['units'])] += 1
        inv = defaultdict(set)
        for ref, c in by_ref.items():
            for k in c:
                inv[k].add(ref)
        shared = Counter()
        for k, refs in inv.items():
            if len(refs) < 2:
                continue
            rs = sorted(refs)
            for x in range(len(rs)):
                for y in range(x + 1, len(rs)):
                    a, b = rs[x], rs[y]
                    if fac_of(a) != fac_of(b):
                        shared[(a, b)] += min(by_ref[a][k], by_ref[b][k])
        out = []
        for (a, b), n in sorted(shared.items()):
            na, nb = sum(by_ref[a].values()), sum(by_ref[b].values())
            small = min(na, nb)
            if n == small and (n >= 3 or (n >= 2 and na == nb)):
                out.append({'refs': [a, b], 'lines': n, 'kind': 'identical' if na == nb else 'contained',
                            'smaller': a if na <= nb else b})
        self.dbl = out
        self.dbl_refs = {r for d in out for r in d['refs']}

    def arrival_of(self, row, b):
        if row.get('port_dated') and _d10(row.get('arrival')):
            return _d10(row.get('arrival'))
        etd = _d10(row.get('etd'))
        if not etd:
            return None
        pants = self.rt.is_pants(b) if hasattr(self.rt, 'is_pants') else (b[6:7] == 'P' or b[4:6] in BOTTOM_FABRICS)
        days = getattr(self.rt, 'TRANSIT_DAYS_PANTS', 55) if pants else getattr(self.rt, 'TRANSIT_DAYS', 45)
        return (date.fromisoformat(etd) + timedelta(days=days)).isoformat()

    def build_production(self):
        self.production = []
        LU = self.R.get('ledgerUse') or {}
        for L in self.ci.ledger:
            r, res, units = L['row'], L['use'], L['units']      # 'use': the style's cost after contract C13
            dec = self.ci.decode(L['b'])
            unit, fx = self.ci.eff(res)
            cat, fiber = (dec['cat'], dec['fiber']) if dec else ('other', 'mmf')
            origin = _factory_origin(self.S, L['fac'])
            fob_landing = L['landing'] == 'FOB'
            regime = self.dest_regime(L['landing'])
            fobU = r4(unit)
            fob = r2(units * fobU) if fobU is not None else None
            lnd, lndU = stock_money(fob, units, cat, fiber, origin, regime, self.S)
            # The maker's own price for this line (L['res'], its own ladder before contract C13): what that factory
            # bills. The page's factory payments use it; cost of goods keeps the style's cost (fobU).
            own_u, own_fx = self.ci.eff(L['res'])
            own_share = r4(own_fx) if own_u is not None else 0.0
            lu = LU.get(L['i']) or {}
            claimed = _int(lu.get('claimed'))
            level = res['level'] if unit is not None else 'L7'
            flags = set(res['flags'])
            if regime != 'us' and not fob_landing:
                flags.add('non_us_dest')
            if lu.get('suppressed'):
                flags.add('suppressed')
            if r.get('fob_flag'):
                flags.add('fob_flag')
            if L['flag']:
                flags.add(L['flag'])
            if not L['ref'] or L['ref'] == 'N/A':
                flags.add('no_ref')
            if lu.get('status') == 'not_in_feed':
                flags.add('not_in_feed')
            if L['ref'] in self.dbl_refs:
                flags.add('double_booking')
            row = {'ref': L['ref'] or None, 'factory': L['fac'], 'poName': L['poName'] or None, 'style': L['style'],
                   'base': L['b'], 'brand': self.brand_of(dec, L['style'], r.get('brand')), 'cat': cat, 'fiber': fiber,
                   'units': units, 'etd': L['etd'], 'arrival': self.arrival_of(r, L['b']), 'landing': L['landing'] or None,
                   'fobLanding': 1 if fob_landing else 0, 'fobU': fobU, 'fob': fob, 'landedU': lndU, 'landed': lnd,
                   'level': level, 'grade': max(grade_of(level), res.get('gcap') or 'A'), 'claimed': claimed,
                   'free': max(0, units - claimed),
                   'origin': origin, 'fxShare': r4(fx) if unit is not None else 0.0, 'flags': sorted(flags),
                   'basis': self.bkey(res['basis']), 'ev': self.ev(res['ids']), 'dutyRegime': regime,
                   'fxRef': _fxref_out(r4(fx) if unit is not None else 0.0, self.ci.fx_ref(res)),
                   'ownFobU': r4(own_u), 'ownFxShare': own_share,
                   'ownFxRef': _fxref_out(own_share, self.ci.fx_ref(L['res']))}
            self.production.append(row)
            cell = self.cov['production'][level]
            cell[0] += units
            cell[1] += fob or 0.0
            cell[2] += fob or 0.0
            g = self.gcov['production'][row['grade']]
            g[0] += units
            g[1] += fob or 0.0

    # ── style cost (today's cost of a base style; styles table and shipped lens) ──
    def style_cost(self, b, label=None):
        key = ('style', b, label)
        if key in self._gen:
            return self._gen[key]
        parts = self.generic(b, b)
        if len(parts) == 1 and parts[0][1]['level'] == 'L7' and label and self.ci.decode(b) is None:
            res = self.ci.final(b, self.ci.raw(b, 'UNKNOWN', brand_label=label))
            parts = [(1.0, res, 'UNKNOWN', None, 'calculator')]
        comps = [self.comp(sh, res, fac, ref, k) for sh, res, fac, ref, k in parts]
        ag = self.agg(comps)
        units = [c['p'] for c in comps if c['p'] is not None]
        rng = [self.ci.eff_range(res) for _, res, _, _, _ in parts]
        lo = min([x[0] for x in rng if x[0] is not None] + units) if units else None
        hi = max([x[1] for x in rng if x[1] is not None] + units) if units else None
        # Contract C15: the style's sheet row is the one behind the largest share of its cost (every share when the
        # style takes one combined cost).
        best = None
        for sh, res, _, _, _ in parts:
            if res.get('row') and (best is None or sh > best[0]):
                best = (sh, res)
        # costFlags: the C15 flags of the cost behind that row (styles.costFlags).
        out = dict(ag, lo=lo, hi=hi, facs=sorted({c['fac'] for c in comps if c['fac']}),
                   row=best[1]['row'] if best else None, skip=tuple(best[1].get('skip') or ()) if best else (),
                   costFlags=[f for f in C15_FLAGS if best and f in best[1]['flags']])
        self._gen[key] = out
        return out

    @staticmethod
    def _row_label(r):
        return ' '.join(str(r.get('fabrication') or r.get('style') or '').split())

    def cost_row(self, sc):
        """styles.costRow (contract C15): [source, cell on that sheet, row label, fit column, price4] of the one sheet
        row behind the style's cost. price4 is the price the sheet prints. Its C15 flags go to styles.costFlags. A
        price shown in a picture has no cell: its cell reads as the record id's part after the source. None when no
        row prices the style (a manual cost, a default, no cost)."""
        row = sc.get('row')
        r = self.ci.records.get(row[0]) if row else None
        if not r:
            return None
        self.ev_used.add(r['id'])
        cell = r['id'].partition('!')[2] if r.get('record_kind') == 'image_callout' else (r.get('cell') or
                                                                                         r['id'].partition('!')[2])
        return [str(r.get('source_code') or ''), str(cell), self._row_label(r), row[1], r4(self.ci.sheet_price(r))]

    def cost_skip(self, sc):
        """styles.costRowsSkipped (contract C15): [cell as its record id, row label, price4, reason] per row the rule
        set aside on the sheet of the row used, closest first. Every cell listed is in the evidence table."""
        out = []
        for rid, why in sc.get('skip') or ():
            r = self.ci.records.get(rid)
            if r:
                self.ev_used.add(rid)
                out.append([rid, self._row_label(r), r4(self.ci.sheet_price(r)), why])
        return out

    def build_styles(self):
        inv_b = defaultdict(lambda: [0, 0.0, 0.0])
        reg_b = defaultdict(set)          # base -> duty regimes of its stock and production rows
        for r in self.inventory:
            e = inv_b[r['base']]
            e[0] += r['units']
            e[1] += r['fob'] or 0.0
            e[2] += r['landed'] or 0.0
            reg_b[r['base']].add(r['dutyRegime'])
        prod_b = defaultdict(lambda: [0, 0.0, set(), 0])
        for r in self.production:
            e = prod_b[r['base']]
            # A suppressed batch already sits in warehouse stock (routing skipped it), so it is not
            # incoming or free supply here; totals.production reports suppressed units apart.
            if 'suppressed' not in r['flags']:
                e[0] += r['units']
                e[1] += r['fob'] or 0.0
                e[3] += r['free']
            e[2].add(r['factory'])
            if r['units'] > 0:
                reg_b[r['base']].add(r['dutyRegime'])
        feed_b = defaultdict(lambda: [0, 0, 0, None])
        for sku, m in self.inv.items():
            e = feed_b[m['b']]
            e[0] += m['ats']
            e[1] += abs(m['committed'])
            e[2] += abs(m['allocated'])
            e[3] = e[3] or m['label']
        open_b = defaultdict(lambda: {'u': 0, 'rev': 0.0, 'ded': 0.0, 'gp': 0.0, 'contrib': 0.0, 'rc': 0.0,
                                      'lad': defaultdict(lambda: [0, 0.0])})
        for r in self.lines:
            if r['type'] != 'a2000' or r['units'] <= 0:
                continue
            e = open_b[r['base']]
            e['u'] += r['units']
            e['rev'] += r['rev']
            e['ded'] += r['deduct'] or 0.0
            e['gp'] += r['gp'] or 0.0
            e['contrib'] += r['contrib'] or 0.0
            e['rc'] += r['revCost'] or 0.0
            e['lad'][r['cust']][0] += r['units']
            e['lad'][r['cust']][1] += r['rev']
        apo_b = defaultdict(lambda: [0, 0.0, 0.0])
        for r in self.apo:
            apo_b[r['base']][0] += r['units']
            apo_b[r['base']][1] += r['rev'] or 0.0
            apo_b[r['base']][2] += r['deduct'] or 0.0
        bases = set(k for k, v in inv_b.items() if v[0] > 0) | set(prod_b) | set(open_b) | set(apo_b)
        bases |= {r['base'] for r in self.lines}
        bases |= {k for k, v in feed_b.items() if v[1] or v[2]}
        bases |= {k for k, e in self.an.items() if e.get('t12') and e['t12'][0] > 0}
        self.styles, self.shipped_cost = [], {}
        for b in sorted(bases):
            dec = self.ci.decode(b)
            an = self.an.get(b) or {}
            label = feed_b[b][3] if b in feed_b else an.get('label')
            sc = self.style_cost(b, label)
            cat, fiber = (dec['cat'], dec['fiber']) if dec else ('other', 'mmf')
            unit = sc['unit'] if sc['priced'] else None
            level = sc['level'] if unit is not None else 'L7'
            # The style lands one way when all its stock and production do (for example a Canada-only
            # program); otherwise its landed cost is a US import.
            regs = reg_b.get(b) or set()
            regime = next(iter(regs)) if len(regs) == 1 else 'us'
            lu = landed(unit, {'cat': cat, 'fiber': fiber}, self.S, regime, sc['origin'])['landedU']
            brand = self.brand_of(dec, None, label, base=b)
            ob, ib, pb, fb, ab = open_b.get(b), inv_b.get(b), prod_b.get(b), feed_b.get(b), apo_b.get(b)
            t12 = an.get('t12') or [0.0, 0.0]
            sh = self.shipped_style_money(b, an, unit, cat, fiber, sc['origin'], brand)
            # Expected price and the deduction percent of the same customer mix (contract C8).
            # Open lines and invoice history count kit cartons, so a kit base divides by its pieces
            # per carton (build_lines' convention); the landed cost it meets below is per piece.
            pcs = self.ci.kit_pcs.get(b, 1)
            exp = ded = None
            if ob and ob['u'] > 0:
                exp = ob['rev'] / (ob['u'] * pcs)
                ded = 100 * ob['ded'] / ob['rev'] if ob['rev'] else None
            elif t12[0] > 0 and t12[1] > 0:
                exp = t12[1] / (t12[0] * pcs)
                ded = self.mix_of(an)[0]
            elif an.get('qty', 0) > 0 and an.get('value', 0) > 0:
                exp = an['value'] / (an['qty'] * pcs)
                ded = self.mix_of(an)[0]
            elif ab and ab[0] > 0 and ab[1] > 0:
                exp = ab[1] / ab[0]          # allocation revenue is built from per-piece prices
                ded = 100 * ab[2] / ab[1]
            if exp is not None and ded is None:
                ded = _ded_pct('', self.S, 'other')
            exp4, ded4 = r4(exp), r4(ded)
            ats = fb[0] if fb else 0
            free_prod = pb[3] if pb else 0
            # Potential gross profit of the free units at the expected price, after deductions. The
            # published rounded inputs are used, so the client gets the same cents.
            pot = (r2(max(0, ats) * (exp4 * (1 - ded4 / 100) - lu)) if exp4 is not None and lu is not None else None)
            facs = set(sc['facs']) | (pb[2] if pb else set())
            row = {'base': b, 'brand': brand, 'cat': cat, 'fab': dec['fab'] if dec else None,
                   'fiber': fiber, 'fit': dec['fitClass'] if dec else None, 'sleeve': dec['sleeve'] if dec else None,
                   'pat': dec['pat'] if dec else None, 'fobU': r4(unit), 'landedU': lu, 'level': level,
                   'grade': max(grade_of(level), sc['cap']), 'rangeLo': r4(sc['lo']) if unit is not None else None,
                   'rangeHi': r4(sc['hi']) if unit is not None else None,
                   'factories': sorted(f for f in facs if f), 'onHand': ib[0] if ib else 0,
                   'onHandFob': r2(ib[1]) if ib else 0.0, 'onHandLanded': r2(ib[2]) if ib else 0.0,
                   'incoming': pb[0] if pb else 0, 'incomingFob': r2(pb[1]) if pb else 0.0, 'ats': ats,
                   'committed': fb[1] if fb else 0, 'allocated': fb[2] if fb else 0,
                   'openUnits': ob['u'] if ob else 0, 'openRev': r2(ob['rev']) if ob else 0.0,
                   'openGp': r2(ob['gp']) if ob else 0.0, 'openContrib': r2(ob['contrib']) if ob else 0.0,
                   'apoUnits': ab[0] if ab else 0, 'apoRev': r2(ab[1]) if ab else 0.0,
                   't12Units': int(round(t12[0])), 't12Rev': r2(t12[1]), 't12Cogs': sh['cogs'], 't12Gp': sh['gp'],
                   'lifeUnits': int(round(an.get('qty', 0))), 'lifeRev': r2(an.get('value', 0.0)),
                   'expPrice': exp4, 'atsPotentialGp': pot,
                   'ladder': sorted(([c, v[0], r4(v[1] / v[0])] for c, v in ob['lad'].items() if v[0] > 0),
                                    key=lambda x: (-x[2], x[0])) if ob else [],
                   'basis': self.bkey(sc['basis']), 'ev': self.ev(sc['ids']), 't12Net': sh['net'],
                   'dutyRegime': regime, 'dedPct': ded4, 'atsFreeStock': max(0, ats - free_prod),
                   'atsFreeProd': free_prod, 'fxShare': r4(sc['fx']) if unit is not None else 0.0,
                   'fxRef': _fxref_out(r4(sc['fx']) if unit is not None else 0.0, sc.get('fxr')),
                   'openRevCost': r2(ob['rc']) if ob else 0.0,
                   # Contract C13: every direct factory quote, how the cost was set, how far the quotes spread.
                   'costByFactory': self.ci.cost_by_factory(b), 'costRule': self.ci.cost_rule(b, level),
                   'costSpread': self.ci.cost_spread(b),
                   # Contract C15: the one sheet row behind the cost, and the rows the rule set aside.
                   'costRow': self.cost_row(sc) if unit is not None else None,
                   'costRowsSkipped': self.cost_skip(sc) if unit is not None else [],
                   'costFlags': list(sc.get('costFlags') or ()) if unit is not None else []}
            self.styles.append(row)

    # ── shipped lens (invoiced units at today's style cost) ──
    def mix_of(self, an):
        """(deduction percent, share with no import costs, share landing in Canada) from the style's
        lifetime customer mix, by invoice value. History codes (drop-ship accounts, legacy suffixes) count
        as the account they belong to, with its chargeback rate and its duty regime (contract C12:
        regimeByCustomer, then the FOB list, else a US import; invoices carry no warehouse)."""
        cust = an.get('customers') or {}
        tv = sum(v[1] for v in cust.values() if v[1] > 0)
        if tv <= 0:
            return _ded_pct('', self.S, 'other'), 0.0, 0.0
        ded = sum(v[1] * self.cust_info(self.hist_code(c))[1] for c, v in cust.items() if v[1] > 0) / tv
        regs = {c: self.cust_regime(self.hist_code(c)) for c in cust}
        fobs = sum(v[1] for c, v in cust.items() if v[1] > 0 and regs[c] == 'none') / tv
        cas = sum(v[1] for c, v in cust.items() if v[1] > 0 and regs[c] == 'ca') / tv
        return ded, fobs, cas

    def shipped_style_money(self, b, an, unit, cat, fiber, origin, brand):
        out = {'cogs': 0.0 if unit is not None else None, 'gp': None, 'net': 0.0, 'rows': {}}
        if not an or not self.months:
            if unit is None:
                out['cogs'] = None
            return out
        ded, fobs, cas = self.mix_of(an)
        roy = _roy_pct(brand, self.S)
        on_rev = royalty_base(self.S) == 'revenue'
        rcp = revenue_cost_pct(self.S)
        unit4 = r4(unit)
        # Kit programs are invoiced per carton while the style cost is per piece, so the factory
        # cost and every adder count pieces, the same convention as shipped_by_customer.
        pcs = self.ci.kit_pcs.get(b, 1)
        rev = cogs = dsum = rsum = 0.0
        for ym in self.months:
            e = an['months'].get(ym)
            if not e or (not e[0] and not e[1]):
                continue
            u, v = e[0], e[1]
            qty = u * pcs
            rev += v
            dd = v * ded / 100
            dsum += dd
            rec = {'u': u, 'v': v, 'ded': dd, 'rc': v * rcp / 100}
            if unit is not None:
                fob = r2(qty * unit4)
                d, f, x = adders(fob, qty, cat, fiber, origin, self.S, 'us')
                k = 1 - fobs
                if cas:
                    # The Canada share (C12) pays the Canadian adders on the same factory cost.
                    k -= cas
                    dc, fc, xc = adders(fob, qty, cat, fiber, origin, self.S, 'ca')
                    rec.update(fob=fob, duty=d * k + dc * cas, freight=f * k + fc * cas, fees=x * k + xc * cas)
                    cogs += fob + (d + f + x) * k + (dc + fc + xc) * cas
                else:
                    rec.update(fob=fob, duty=d * k, freight=f * k, fees=x * k)
                    cogs += fob + (d + f + x) * k
                rec['roy'] = (v if on_rev else v - dd) * roy / 100
                rsum += rec['roy']
            out['rows'][ym] = rec
        out['net'] = r2(rev - dsum)
        out['ded'], out['fobShare'], out['caShare'] = ded, fobs, cas
        if unit is not None:
            out['cogs'] = r2(cogs)
            out['gp'] = r2(out['net'] - out['cogs'])
        return out

    def build_shipped(self):
        if self.an_state != 'ready':
            note = {'building': 'Shipped history is still loading. Refresh in a few minutes.',
                    'invalid': 'Shipped history could not be read. The rest of the P&L is not affected.'}.get(
                        self.an_state, 'Shipped history is not available on this server yet.')
            self.shipped = {'months': [], 'company': _table(SHIPPED_COMPANY_FIELDS, []),
                            'byStyle': _table(SHIPPED_STYLE_FIELDS, []), 'note': note, 'state': self.an_state,
                            'byCustomer': _table(SHIPPED_CUSTOMER_FIELDS, []), 'range': self.shipped_range()}
            return
        comp = {ym: defaultdict(float) for ym in self.months}
        by_style = []
        for st in sorted(self.an):
            an = self.an[st]
            if not any(an['months'].get(ym) and (an['months'][ym][0] or an['months'][ym][1]) for ym in self.months):
                continue
            dec = self.ci.decode(st)
            label = an.get('label')
            brand = self.brand_of(dec, None, label, base=st)
            sc = self.style_cost(st, label)
            unit = sc['unit'] if sc['priced'] else None
            cat, fiber = (dec['cat'], dec['fiber']) if dec else ('other', 'mmf')
            sm = self.shipped_style_money(st, an, unit, cat, fiber, sc['origin'], brand)
            level = sc['level'] if unit is not None else 'L7'
            by_style.append({'base': st, 'brand': brand, 'cat': cat, 'fiber': fiber, 'dedPct': r4(sm.get('ded', 0.0)),
                             'fobShare': r4(sm.get('fobShare', 0.0)),
                             'months': {ym: [int(round(r['u'])), r2(r['v'])] for ym, r in sm['rows'].items()},
                             'fobU': r4(unit), 'level': level, 'grade': max(grade_of(level), sc['cap']),
                             'origin': sc['origin'], 'fxShare': r4(sc['fx']) if unit is not None else 0.0,
                             'fxRef': _fxref_out(r4(sc['fx']) if unit is not None else 0.0, sc.get('fxr')),
                             'caShare': r4(sm.get('caShare', 0.0))})
            for ym, r in sm['rows'].items():
                c = comp[ym]
                c['units'] += r['u']
                c['rev'] += r['v']
                c['deduct'] += r['ded']
                if unit is not None:
                    c['costedRev'] += r['v']
                    c['costedDed'] += r['ded']
                    c['revCost'] += r['rc']           # like royalty: styles that have a cost only
                    for k in ('fob', 'duty', 'freight', 'fees', 'roy'):
                        c[k] += r[k]
        rows = []
        for ym in self.months:
            c = comp[ym]
            fob, duty, freight, fees = r2(c['fob']), r2(c['duty']), r2(c['freight']), r2(c['fees'])
            cogs = r2(fob + duty + freight + fees)
            net = r2(c['rev'] - c['deduct'])
            gp = r2((c['costedRev'] - c['costedDed']) - cogs)
            roy = r2(c['roy'])
            rc = r2(c['revCost'])
            rows.append({'month': ym, 'units': int(round(c['units'])), 'rev': r2(c['rev']), 'fob': fob, 'duty': duty,
                         'freight': freight, 'fees': fees, 'cogs': cogs, 'deduct': r2(c['deduct']), 'net': net, 'gp': gp,
                         'royalty': roy, 'contrib': r2(gp - roy - rc),
                         'costedShare': round(c['costedRev'] / c['rev'], 3) if c['rev'] else 0.0,
                         'costedRev': r2(c['costedRev']), 'revCost': rc})
        ob_to = self._div_to('OB') or _d10(self.sa_src.get('to'))
        ds_to = self._div_to('DS')
        head = ('Invoices run through %s. ' % friendly_date(ob_to)) if ob_to else ''
        if ob_to and ds_to and ds_to < ob_to:
            # Dropship invoices arrive on their own schedule; when they stop earlier, say so.
            head += 'Dropship invoices run through %s. ' % friendly_date(ds_to)
        note = (head
                + 'Gross profit, royalty, revenue costs and profit count only styles that have a cost. '
                  'The Revenue with a cost row shows that share.')
        self.hist_rows = self.shipped_by_customer()
        self.shipped = {'months': list(self.months), 'company': _table(SHIPPED_COMPANY_FIELDS, rows),
                        'byStyle': _table(SHIPPED_STYLE_FIELDS, by_style), 'note': note, 'state': 'ready',
                        'byCustomer': _table(SHIPPED_CUSTOMER_FIELDS, self.hist_rows), 'range': self.shipped_range()}

    def shipped_range(self):
        """shipped.range (contract C14): the invoice history's own dates, from sales_analytics.source.
        byDiv carries the feed's per-division dates (OB wholesale, DS dropship) when it sends them."""
        s = self.sa_src if isinstance(getattr(self, 'sa_src', None), dict) else {}
        return {'from': s.get('from'), 'to': s.get('to'), 'ingestedAt': s.get('ingestedAt'),
                'byDiv': s.get('byDiv') if isinstance(s.get('byDiv'), dict) else None}

    def _div_to(self, div):
        """The invoice history's end date for one division (source.byDiv), else None."""
        s = self.sa_src if isinstance(getattr(self, 'sa_src', None), dict) else {}
        bd = s.get('byDiv') if isinstance(s.get('byDiv'), dict) else {}
        e = bd.get(div) if isinstance(bd.get(div), dict) else {}
        return _d10(e.get('to'))

    def shipped_by_customer(self):
        """shipped.byCustomer (contract C14): lifetime invoices per account and style. History codes fold
        into their account (hist_code). Each row is costed like an order line (C11) at the style's cost
        of today, with the account's chargeback rate and duty regime (C12, no warehouse). Only rows with
        units. A style with no cost keeps every amount after cost empty. The style key is the history's
        own key, as in shipped.byStyle."""
        fold = defaultdict(lambda: [0.0, 0.0])
        for st, an in self.an.items():
            for c, v in an['customers'].items():
                cell = fold[(self.hist_code(c), st)]
                cell[0] += v[0]
                cell[1] += v[1]
        out = []
        for cust, st in sorted(fold):
            u, val = fold[(cust, st)]
            units = int(round(u))
            if units <= 0:
                continue
            an = self.an[st]
            dec = self.ci.decode(st)
            label = an.get('label')
            sc = self.style_cost(st, label)
            unit = sc['unit'] if sc['priced'] else None
            fobU = r4(unit)
            cat, fiber = (dec['cat'], dec['fiber']) if dec else ('other', 'mmf')
            brand = self.brand_of(dec, None, label, base=st)
            qty = units * self.ci.kit_pcs.get(st, 1)
            rev = r2(val)
            fob = r2(qty * fobU) if fobU is not None else None
            regime = self.cust_regime(cust)
            level = sc['level'] if unit is not None else 'L7'
            share = r4(sc['fx']) if unit is not None else 0.0
            row = {'cust': cust, 'base': st, 'brand': brand, 'cat': cat, 'fiber': fiber, 'origin': sc['origin'],
                   'units': units, 'rev': rev, 'fobU': fobU, 'fob': fob, 'level': level,
                   'grade': max(grade_of(level), sc['cap']), 'dutyRegime': regime, 'fxShare': share,
                   'fxRef': _fxref_out(share, sc.get('fxr')), 'pieces': qty}
            row.update(line_money(rev, fob, qty, cat, fiber, sc['origin'], regime, self.cust_info(cust)[1],
                                  _roy_pct(brand, self.S), self.S))
            out.append(row)
        return out

    # ── summaries ──
    def coverage(self):
        out = {}
        for k in _COVERAGE_SETS:
            out[k] = {lv: [int(round(v[0])), r2(v[1]), r2(v[2])] for lv, v in sorted(self.cov[k].items(),
                                                                                   key=lambda x: LEVEL_RANK[x[0]])}
        out['grades'] = {k: {g: [int(round(v[0])), r2(v[1])] for g, v in sorted(self.gcov[k].items())}
                         for k in _COVERAGE_SETS}
        return out

    def unresolved(self):
        out = []

        def reason(dec, kind):
            if dec is None:
                return 'The code cannot be decoded and the cost book has no map for it.'
            if dec['cat'] in ('vest',):
                return 'No list price, and the calculators do not price vests.'
            return 'No list price and no calculator row for these attributes.'
        for r in self.lines:
            if r['level'] == 'L7' and r['units'] > 0:
                out.append({'kind': 'line', 'key': r['id'], 'style': r['style'], 'units': r['units'], 'value': r['rev'],
                            'reason': reason(self.ci.decode(r['base']), 'line')})
        for r in self.apo:
            if r['level'] == 'L7' and r['units'] > 0:
                out.append({'kind': 'apo', 'key': r['id'], 'style': r['style'], 'units': r['units'],
                            'value': r['rev'] or 0.0, 'reason': reason(self.ci.decode(r['base']), 'apo')})
        for r in self.production:
            if r['level'] == 'L7' and r['units'] > 0:
                out.append({'kind': 'production', 'key': '%s|%s' % (r['ref'] or '', r['style']), 'style': r['style'],
                            'units': r['units'], 'value': 0.0, 'reason': reason(self.ci.decode(r['base']), 'production')})
        seen = Counter()
        for r in self.inventory:
            if r['level'] == 'L7':
                seen[r['sku']] += r['units']
        for sku, u in seen.items():
            out.append({'kind': 'inventory', 'key': sku, 'style': sku, 'units': u, 'value': 0.0,
                        'reason': reason(self.ci.decode(base_of(sku)), 'inventory')})
        out.sort(key=lambda x: (-(x['value'] or 0), -x['units'], x['kind'], x['key']))
        return out

    def conflicts(self):
        touch = defaultdict(lambda: [0, 0.0, 0, 0])
        for rows, i in ((self.lines, 0), (self.production, 2), (self.inventory, 3)):
            for r in rows:
                gids = {g for rid in r['ev'] for g in self.ci.conflict_of.get(rid, ())}
                for g in gids:
                    touch[g][i] += r['units']
                    if i == 0:
                        touch[g][1] += r['rev'] or 0.0
        out = []
        for gid, t in touch.items():
            g = self.ci.conflicts.get(gid) or {}
            out.append({'id': gid, 'kind': g.get('kind'), 'members': list(g.get('members') or []),
                        'spread': g.get('spread'), 'median': g.get('median'), 'units': t[2] + t[3],
                        'lineUnits': t[0], 'lineRev': r2(t[1]), 'productionUnits': t[2], 'inventoryUnits': t[3]})
            self.ev_used.update(g.get('members') or [])
        out.sort(key=lambda x: (-(x['units'] + x['lineUnits']), x['id']))
        return out

    def assumption_blocks(self):
        """[(block key, plain name)] of the unconfirmed assumption blocks, in the UI's order (C4).
        Canada and direct-import destinations belong to the duty block (confirmed.tariff). With both
        import multipliers set (C11) no row uses duty and tariffs or freight and fees, so those two
        blocks are not listed. The destinations still are."""
        conf = self.S.get('confirmed') or {}
        unused = ('tariff', 'freight') if self.mult_complete() else ()
        out = []
        for k, name in _ASSUMPTION_NAMES:
            if conf.get(k) is not True:
                if k not in unused:
                    out.append((k, name))
                if k == 'tariff':
                    out.append(('destinations', _DESTINATIONS_NAME))
        return out

    def mult_complete(self):
        """True in multiplier mode with both multipliers set (C11)."""
        L = self.S['landed']
        return L.get('mode') == 'multiplier' and all(L['multiplier'].get(c) is not None for c in FIBER_CLASSES)

    def mult_gaps(self):
        """Multiplier mode with an empty multiplier (C11) -> ([fiber classes the rows need], [styles]).
        Those rows keep the itemized duty, freight and fees."""
        L = self.S['landed']
        if L.get('mode') != 'multiplier':
            return [], []
        missing = {c for c in FIBER_CLASSES if L['multiplier'].get(c) is None}
        need, bases = set(), set()
        if missing:
            for rows in (self.lines, self.apo, self.inventory, self.production, self.hist_rows):
                for r in rows:
                    c = fiber_class(r.get('fiber'))
                    if (c in missing and r.get('dutyRegime') in ('us', 'ca') and r.get('fob') is not None
                            and (r.get('units') or 0) > 0):
                        need.add(c)
                        bases.add(r['base'])
            for r in self.styles:
                c = fiber_class(r.get('fiber'))
                if c in missing and r.get('dutyRegime') in ('us', 'ca') and r.get('fobU') is not None:
                    need.add(c)
                    bases.add(r['base'])
        return [c for c in FIBER_CLASSES if c in need], sorted(bases)

    def alerts(self):
        """Needs-attention list (contract C4). Every alert carries unit (what count counts, or None)
        and valueLabel (what value means, or None when there is no value), plus refsTotal (how many
        refs there are before the list is cut at _REF_CAP)."""
        A = []
        a2 = [r for r in self.lines if r['type'] == 'a2000']
        inc_bulk = self.S['bulk'].get('includeInTotals') is True

        def add(kind, sev, title, detail, rows=None, value=None, count=None, refs=None, aid=None, total=None, **extra):
            meta = ALERT_KINDS.get(kind) or {}
            if refs is None:
                ids = [r['id'] for r in (rows or [])]
                refs, total = {'lines': ids[:_REF_CAP]}, len(ids)
            elif total is None:
                total = sum(len(v) for v in refs.values() if isinstance(v, list))
            a = {'id': aid or 'al_%s' % kind, 'kind': kind, 'severity': sev, 'title': title, 'detail': detail,
                 'count': len(rows) if count is None and rows is not None else (count or 0),
                 'value': r2(value) if value is not None else None, 'unit': meta.get('unit'),
                 'valueLabel': meta.get('valueLabel') if value is not None else None, 'refs': refs, 'refsTotal': total}
            a.update(extra)
            A.append(a)
        below = sorted([r for r in a2 if r['gp'] is not None and r['gp'] < 0], key=lambda r: r['gp'])
        if below:
            add('below_cost', 'high', 'Lines sold below cost', 'These open lines lose money before royalty. Check the price or the cost.',
                below, -sum(r['gp'] for r in below))
        po = defaultdict(list)
        for r in a2:
            if r['gp'] is not None and r['units'] > 0:
                po[(r['cust'], r['orderNo'])].append(r)
        thin = []
        for k, rs in po.items():
            net = sum(r['net'] for r in rs)
            if net > 0 and sum(r['contrib'] for r in rs) / net < THIN_CM:
                thin.append(rs)
        if thin:
            thin.sort(key=lambda rs: sum(r['contrib'] for r in rs) / sum(r['net'] for r in rs))
            ids = [r['id'] for rs in thin for r in rs]
            add('thin_contribution', 'medium', 'Thin profit POs',
                'Purchase orders whose profit after royalty, chargebacks and revenue costs is under %d percent '
                'of net sales.' % round(THIN_CM * 100),
                None, sum(r['rev'] for rs in thin for r in rs), count=len(thin),
                refs={'lines': ids[:_REF_CAP]}, total=len(ids), lineCount=len(ids))
        # Needs a cost: firm A2000 lines (high). Bulk forecast lines get their own alert, which is only
        # informational while bulk stays out of the totals.
        l7 = [r for r in a2 if r['level'] == 'L7' and r['units'] > 0]
        if l7:
            add('needs_cost', 'high', 'Revenue with no cost',
                'These open order lines have no usable cost yet. Set a manual cost in the Cost Book tab.',
                sorted(l7, key=lambda r: -r['rev']), sum(r['rev'] for r in l7))
        bl7 = [r for r in self.lines if r['type'] == 'bulk' and r['level'] == 'L7' and r['units'] > 0]
        if bl7:
            add('bulk_needs_cost', 'high' if inc_bulk else 'info', 'Bulk forecast with no cost',
                ('These bulk forecast lines are in the totals but have no usable cost yet. Set a manual cost in the Cost Book tab.'
                 if inc_bulk else
                 'These bulk forecast lines have no usable cost yet. They are not firm orders and are left out of the totals. '
                 'Set a manual cost in the Cost Book tab before you include them.'),
                sorted(bl7, key=lambda r: -r['rev']), sum(r['rev'] for r in bl7))
        gd = [r for r in a2 if r['grade'] == 'D' and r['units'] > 0]
        if gd:
            add('grade_d', 'medium', 'Grade D exposure', 'Open revenue whose cost or supply match is missing or unreliable.',
                sorted(gd, key=lambda r: -r['rev']), sum(r['rev'] for r in gd))
        if self.dbl:
            fob_by_ref = defaultdict(float)
            for r in self.production:
                if r['ref']:
                    fob_by_ref[r['ref']] += r['fob'] or 0.0
            add('double_booking', 'medium', 'Possible double booking',
                'Two refs from different factories carry the same styles and units. Confirm which one is live.',
                None, sum(fob_by_ref[d['smaller']] for d in self.dbl), count=len(self.dbl_refs),
                refs={'refs': sorted(self.dbl_refs)}, pairs=len(self.dbl))
        late = [r for r in a2 if r['late'] and r['units'] > 0]
        if late:
            add('late', 'medium', 'Past cancel date', 'Open lines whose cancel date has passed.', late, sum(r['rev'] for r in late))
        uns = [r for r in a2 if 'unsourced' in r['flags']]
        if uns:
            add('unsourced', 'info', 'Units with no supply record', "Some units found no warehouse stock or batch. They are costed at the style's average cost.",
                uns, sum(r['rev'] for r in uns))
        unp = [r for r in self.apo if 'unpriced' in r['flags']]
        if unp:
            bases = sorted({r['base'] for r in unp})
            add('apo_unpriced', 'info', 'Allocations with no price',
                'No open order or invoice price exists for these allocations. They are left out of allocation revenue.',
                count=len(unp), refs={'styles': bases[:_REF_CAP]}, total=len(bases))
        if (self.S.get('confirmed') or {}).get('tariff') is not True:
            nus = sorted({r['base'] for rows in (self.lines, self.apo, self.inventory, self.production) for r in rows
                          if 'non_us_dest' in (r.get('flags') or ()) and r['units'] > 0})
            if nus:
                detail = ('Some goods land in Canada or are imported by the customer. Canada uses the same import '
                          'multiplier as the US. Direct imports carry no import costs. Confirm this on the Assumptions tab.'
                          if self.S['landed'].get('mode') == 'multiplier' else
                          'Some goods land in Canada or are imported by the customer. Canada uses an assumed Canadian '
                          'duty rate with no US add-on or fees. Direct imports carry no duty or freight. Confirm this on '
                          'the Assumptions tab.')
                add('non_us_dest', 'info', 'Canada and direct-import costs are assumed', detail,
                    count=len(nus), refs={'styles': nus[:_REF_CAP]}, total=len(nus))
        # stale inputs (one feed each: no count unit, no value)
        now = self._parse_now()
        for key, label, stamp in (('ats', 'ATS feed', self.inv_src.get('last_sync')), ('apo', 'Allocations feed', self.apo_src.get('last_sync'))):
            t = self._parse_ts(stamp)
            if now and t and (now - t).total_seconds() > STALE_FEED_HOURS * 3600:
                add('stale_input', 'medium', '%s is out of date' % label,
                    'The %s was last updated %d hours before this build.' % (label, (now - t).total_seconds() // 3600),
                    count=1, refs={}, aid='al_stale_%s' % key)
        if self.oo_src.get('ok') is not True:
            add('stale_input', 'high', 'Open orders may be stale',
                'The open orders feed reported a problem. Numbers may miss the latest changes.',
                count=1, refs={}, aid='al_stale_orders')
        if self.an_state == 'invalid':
            add('stale_input', 'medium', 'Shipped history could not be read',
                'The invoice history arrived in a shape the P&L cannot read. The Statement shows booked months only. '
                'The rest of the P&L is not affected.', count=1, refs={}, aid='al_stale_analytics')
        elif self.an_state != 'ready':
            add('stale_input', 'info', 'Shipped history not loaded',
                'The invoice history is not available yet. The Statement shows booked months only.',
                count=1, refs={}, aid='al_stale_analytics')
        else:
            to = self._div_to('OB') or _d10(self.sa_src.get('to'))
            ds_to = self._div_to('DS')

            def _hist_old(d):
                return bool(d) and (self.today_d - date.fromisoformat(d)).days > STALE_ANALYTICS_DAYS
            hist_parts = []
            if _hist_old(to):
                hist_parts.append('The invoice history ends on %s.' % friendly_date(to))
            if _hist_old(ds_to) and ds_to != to:
                hist_parts.append('Dropship invoice history ends on %s.' % friendly_date(ds_to))
            if hist_parts:
                add('stale_input', 'info', 'Shipped history is old', ' '.join(hist_parts),
                    count=1, refs={}, aid='al_stale_analytics')
            if self.an_skipped:
                add('stale_input', 'info', 'Shipped history partly unreadable',
                    '%d invoice history rows could not be read in full. Their readable parts are used.' % self.an_skipped,
                    count=1, refs={}, aid='al_analytics_rows')
        bad = list(self.ci.missing_params) + [k for k in self.ci.invalid_params if k not in self.ci.missing_params]
        if bad or self.ci.bad_records:
            parts = []
            if bad:
                parts.append('Some pricing rules are switched off because their parameters are missing or invalid: %s.'
                             % ', '.join(_PARAM_NAMES.get(k, 'other parameters') for k in bad))
            if self.ci.unrated:
                parts.append('%d calculator %s no sheet rate. A saved RMB rate or a what-if cannot reprice %s.'
                             % (self.ci.unrated, 'price carries' if self.ci.unrated == 1 else 'prices carry',
                                'it' if self.ci.unrated == 1 else 'them'))
            if self.ci.bad_records:
                parts.append('%d price rows could not be read and are skipped.' % self.ci.bad_records)
            parts.append('Rebuild the cost book.')
            add('costbook_params', 'high',
                ALERT_KINDS['costbook_params']['label'] if bad else 'Cost book has unreadable price rows',
                ' '.join(parts), count=len(bad), refs={'params': bad}, aid='al_costbook_params',
                missing=list(self.ci.missing_params), invalid=list(self.ci.invalid_params),
                badRecords=self.ci.bad_records)
        need, gap_styles = self.mult_gaps()
        if need:
            one = len(need) == 1
            add('settings_incomplete', 'high', ALERT_KINDS['settings_incomplete']['label'],
                'Import costs are set to use a multiplier on the factory cost, but the %s for %s %s empty. '
                'Those goods use the itemized duty, freight and fees instead. Set %s on the Assumptions tab.'
                % ('multiplier' if one else 'multipliers', ' and '.join(_FIBER_CLASS_WORDS[c] for c in need),
                   'is' if one else 'are', 'it' if one else 'them'),
                count=len(need), refs={'styles': gap_styles[:_REF_CAP]}, total=len(gap_styles),
                aid='al_settings_incomplete', missing=list(need))
        blocks = self.assumption_blocks()
        if blocks:
            add('assumption', 'info', 'Assumptions to confirm',
                'These settings still use public defaults: %s.' % ', '.join(n for _, n in blocks),
                count=len(blocks), refs={'blocks': [k for k, _ in blocks]}, aid='al_assumptions')
        return A

    def _parse_now(self):
        return self._parse_ts(self.now_iso)

    @staticmethod
    def _parse_ts(s):
        s = str(s or '').strip()
        if not s:
            return None
        try:
            t = datetime.fromisoformat(s.replace('Z', '+00:00'))
        except ValueError:
            return None
        if t.tzinfo is not None:
            t = t.replace(tzinfo=None) - (t.utcoffset() or timedelta(0))
        return t

    def evidence(self):
        ovr = {'OVR:%s' % o.get('id'): o for o in self.overrides if isinstance(o, dict)}
        out = {}
        for rid in sorted(self.ev_used):
            if rid in ovr:
                o = ovr[rid]
                k = o.get('key') or {}
                out[rid] = {'src': 'Manual override', 'sheet': None, 'cell': None, 'row': None, 'factory': None,
                            'scope': o.get('scope'), 'fabrication': None, 'fabric': k.get('fabric') or None, 'fit': None,
                            'sleeve': None, 'pattern': None, 'customerGroup': None, 'priceUsd': r4(_num(o.get('fobU'))),
                            'rmb': None, 'fxSheet': None, 'flags': [], 'kind': 'override', 'ref': k.get('ref') or None,
                            'style': k.get('style') or None, 'brand': None, 'priceSheet': None,
                            'reason': o.get('reason'), 'by': o.get('by'), 'at': o.get('at'), 'effective': o.get('effective')}
                continue
            r = self.ci.records.get(rid)
            if not r:
                continue
            # priceUsd: the price the P&L uses for this record (printed, at the saved rate, or the after-cut
            # reading). priceSheet: the Current USD the sheet prints. fxSheet: the sheet's own rate.
            p = self.ci.rec_price(r)
            cut = self.ci.rec_cut(r) if (self.ci.after_cut and r.get('record_kind') == 'calculator') else None
            used = cut if cut is not None else p
            f_rmb = self.ci._f_rmb
            out[rid] = {'src': r.get('origin_file') or r.get('source_code'), 'sheet': r.get('sheet'), 'cell': r.get('cell'),
                        'row': r.get('row'), 'factory': r.get('factory_code'), 'scope': r.get('scope'),
                        'fabrication': r.get('fabrication'), 'fabric': r.get('fabric_code_primary'),
                        'fit': r.get('fit_class'), 'sleeve': r.get('sleeve'), 'pattern': r.get('pattern_effective'),
                        'customerGroup': r.get('customer_group'), 'priceUsd': r4(used),
                        'rmb': _fnum(r.get(f_rmb)) if f_rmb else None,
                        'fxSheet': r4(self.ci.rec_rate(r)), 'flags': list(r.get('flags') or []),
                        'kind': r.get('record_kind'), 'ref': r.get('production_ref_resolved') or r.get('production_ref'),
                        'style': r.get('style'), 'brand': r.get('brand'), 'priceSheet': self.ci.rec_usd(r)}
        return out

    def totals(self):
        inc_bulk = self.S['bulk'].get('includeInTotals') is True

        def pcs(r):
            """Pieces of a row: kit lines are sold by the carton, so their units are cartons."""
            return r['pieces'] if r.get('pieces') is not None else r['units']

        cost_fields = ('fob', 'duty', 'freight', 'fees', 'cogs')

        def tot(rows):
            # Unit totals count pieces (F11), so a kit carton adds its pieces like its cost does.
            # An allocation with no price estimate (rev None) keeps its cost out of the cost fields, as
            # the page's _pnlAgg does (review numbers-reconcile R8). Its cost goes to unpricedFob and
            # unpricedCogs, so net - cogs - uncostedNet = gp for every dataset.
            priced = [r for r in rows if r.get('rev') is not None]
            unpriced = [r for r in rows if r.get('rev') is None]
            t = {'units': sum(pcs(r) for r in rows), 'lines': len(rows),
                 'kitCartons': sum(r['units'] for r in rows if 'kit' in (r.get('flags') or ()))}
            for f in ('rev', 'deduct', 'net', 'fob', 'duty', 'freight', 'fees', 'cogs', 'gp', 'royalty', 'contrib',
                      'revCost'):
                t[f] = r2(sum((r.get(f) or 0.0) for r in (priced if f in cost_fields else rows)))
            costed = [r for r in priced if r.get('fob') is not None and r.get('level') != 'L7']
            t['costedRev'] = r2(sum((r.get('rev') or 0.0) for r in costed))
            t['costedNet'] = r2(sum((r.get('net') or 0.0) for r in costed))
            t['costedUnits'] = sum(pcs(r) for r in costed)
            t['costedLines'] = len(costed)
            nogp = [r for r in rows if r.get('gp') is None]          # no cost or no price: no gross profit
            t['uncostedRev'] = r2(sum((r.get('rev') or 0.0) for r in nogp))
            t['uncostedNet'] = r2(sum((r.get('net') or 0.0) for r in nogp))
            t['uncostedUnits'] = sum(pcs(r) for r in nogp)
            # Revenue costs of rows with no gross profit (C11): contrib = gp - royalty - (revCost - this).
            t['uncostedRevCost'] = r2(sum((r.get('revCost') or 0.0) for r in nogp))
            t['unpricedLines'] = len(unpriced)
            t['unpricedUnits'] = sum(pcs(r) for r in unpriced)
            t['unpricedFob'] = r2(sum((r.get('fob') or 0.0) for r in unpriced))
            t['unpricedCogs'] = r2(sum((r.get('cogs') or 0.0) for r in unpriced))
            g = defaultdict(lambda: [0, 0.0])
            for r in rows:
                g[r['grade']][0] += pcs(r)
                g[r['grade']][1] += r.get('rev') or 0.0
            t['grades'] = {k: [v[0], r2(v[1])] for k, v in sorted(g.items())}
            return t
        a2 = [r for r in self.lines if r['type'] == 'a2000']
        bk = [r for r in self.lines if r['type'] == 'bulk']
        ob = tot(a2 + bk if inc_bulk else a2)
        ob['includesBulk'] = inc_bulk

        def stock(rows, extra=None):
            t = {'units': sum(r['units'] for r in rows), 'fob': r2(sum(r['fob'] or 0.0 for r in rows)),
                 'landed': r2(sum(r['landed'] or 0.0 for r in rows)),
                 'costedUnits': sum(r['units'] for r in rows if r['fob'] is not None)}
            g = defaultdict(lambda: [0, 0.0])
            for r in rows:
                g[r['grade']][0] += r['units']
                g[r['grade']][1] += r['fob'] or 0.0
            t['grades'] = {k: [v[0], r2(v[1])] for k, v in sorted(g.items())}
            return t
        inv = stock(self.inventory)
        bywh = defaultdict(lambda: [0, 0.0, 0.0])
        for r in self.inventory:
            e = bywh[r['wh']]
            e[0] += r['units']
            e[1] += r['fob'] or 0.0
            e[2] += r['landed'] or 0.0
        inv['byWh'] = {k: [v[0], r2(v[1]), r2(v[2])] for k, v in sorted(bywh.items())}
        prod = stock(self.production)
        prod['claimed'] = sum(r['claimed'] for r in self.production)
        prod['free'] = sum(r['free'] for r in self.production)
        sup = [r for r in self.production if 'suppressed' in r['flags']]
        prod['suppressedUnits'] = sum(r['units'] for r in sup)
        prod['suppressedFob'] = r2(sum(r['fob'] or 0.0 for r in sup))
        prod['fobLandingUnits'] = sum(r['units'] for r in self.production if r['fobLanding'])
        apo = tot(self.apo)
        # Allocations that are the same goods as a bulk order of the same customer: subtract them from
        # the allocation totals when the bulk forecast is shown too, so nothing is counted twice.
        cov = [r for r in self.apo if r.get('coveredBy')]
        apo['coveredByBulk'] = {'lines': len(cov), 'units': sum(r['units'] for r in cov)}
        for f in ('rev', 'deduct', 'net', 'fob', 'cogs', 'gp', 'royalty', 'contrib', 'revCost'):
            apo['coveredByBulk'][f] = r2(sum((r.get(f) or 0.0) for r in cov
                                             if f not in cost_fields or r.get('rev') is not None))
        return {'openBook': ob, 'bulk': tot(bk), 'apo': apo, 'inventory': inv, 'production': prod}

    def dictionary(self):
        names = dict(getattr(self.rt, 'ENGINE_CUSTOMER_NAMES', {}) or {})
        for o in self.orders:
            c = _u(o.get('customer'))
            if c and o.get('customerFull') and c not in names:
                names[c] = str(o['customerFull']).strip()
        names.update({k: v for k, v in self.apo_names.items() if k not in names})
        codes = ({r['cust'] for r in self.lines} | {r['cust'] for r in self.apo} | set(self.fob_set)
                 | {r['cust'] for r in self.hist_rows})
        customers = {}
        for c in sorted(x for x in codes if x):
            g = _cust_group(c, self.S, self.fob_set)
            customers[c] = {'name': names.get(c) or c, 'group': g, 'fob': c in self.fob_set}
        brands = set()
        for rows in (self.lines, self.apo, self.inventory, self.production, self.styles):
            brands.update(r['brand'] for r in rows if r.get('brand'))
        list_facs = self.ci.pc_facs | self.ci.dp_facs | self.ci.ky_facs
        facs = set(k for k in (self.S.get('factories') or {}) if k != '_default')
        for rows in (self.lines, self.apo, self.inventory, self.production):
            facs.update(r['factory'] for r in rows if r.get('factory'))
        for r in self.alloc:
            if r.get('factory'):
                facs.add(r['factory'])
        factories = {}
        for f in sorted(facs):
            factories[f] = {'name': _factory_name(self.S, f), 'origin': _factory_origin(self.S, f),
                            'source': 'none' if f in _LIST_FACTORY_PSEUDO else ('list' if f in list_facs else 'calculator')}
        return {'customers': customers, 'brands': {b: BRAND_NAMES.get(b, b) for b in sorted(brands)},
                'factories': factories, 'warehouses': dict(WAREHOUSE_NAMES),
                'levels': {lv: {'label': LEVEL_INFO[lv][0], 'grade': LEVEL_INFO[lv][1]} for lv in LEVELS},
                'grades': copy.deepcopy(GRADE_INFO), 'routing': {k: {'label': v} for k, v in ROUTING_INFO.items()},
                'categories': dict(CATEGORY_LABELS), 'basis': {v: k for k, v in self.basis_keys.items()},
                'flags': dict(FLAG_LABELS), 'alertKinds': copy.deepcopy(ALERT_KINDS),
                'lotTiers': copy.deepcopy(LOT_TIER_LABELS), 'priceBasis': dict(PRICE_BASIS_LABELS),
                'regimes': dict(REGIME_LABELS), 'fiberClasses': dict(FIBER_CLASS_LABELS),
                'dutyLabel': DUTY_LABELS.get(self.S['landed'].get('mode'), DUTY_LABELS['itemized']),
                'costRules': dict(COST_RULE_LABELS)}

    # ── price basis (contract C10) ──
    def fx_inputs(self):
        """inputs.fx: the price basis in use. mode 'printed' (each sheet's Current USD at its own rate),
        'rate' (every RMB-based price at the saved rate) or 'after_cut'. printed: the rates the
        calculator sheets print. weighted: the unit-weighted printed rate of the RMB-based costs
        (see weighted_rate). base: the cost book's own base rate (params.fxBase), for older pages."""
        ci = self.ci
        mode = 'after_cut' if ci.after_cut else ('rate' if ci.fx_rate else 'printed')
        return {'mode': mode, 'rate': ci.fx_rate, 'basis': self.S['fx'].get('basis'), 'base': ci.fx_base,
                'printed': sorted(ci.sheet_rates), 'weighted': self.weighted_rate(), 'unrated': ci.unrated}

    def weighted_rate(self):
        """The unit-weighted rate of the RMB-based costs, from the published rows: the sum of
        pieces x fxShare x fxRef over the sum of pieces x fxShare. The A2000 open book first, else
        production, else stock. None when no row has an RMB-based part."""
        sets = ((self.lines, lambda r: r['pieces'] if r['type'] == 'a2000' and r['units'] > 0 else 0),
                (self.production, lambda r: r['units']), (self.inventory, lambda r: r['units']))
        for rows, qty in sets:
            w = a = 0.0
            for r in rows:
                s, e, n = r.get('fxShare') or 0.0, r.get('fxRef'), qty(r)
                if s > 0 and e and n > 0:
                    w += n * s
                    a += n * s * e
            if w > 0:
                return r4(a / w)
        return None

    @staticmethod
    def _rate_txt(x):
        return ('%.4f' % x).rstrip('0').rstrip('.')

    def fx_note(self, fxin):
        pr = fxin['printed']
        span = ''
        if pr:
            span = self._rate_txt(pr[0]) if len(pr) == 1 else '%s to %s' % (self._rate_txt(pr[0]), self._rate_txt(pr[-1]))
        if fxin['mode'] == 'after_cut':
            return 'Calculator prices use the after-cut reading. They do not move with the rate.'
        if fxin['mode'] == 'rate':
            fx = self.S['fx']
            as_of = (friendly_date(fx['asOf']) or str(fx['asOf'])) if fx.get('asOf') else None
            return ('Every RMB-based calculator price uses %s RMB per US dollar%s.'
                    % (self._rate_txt(fxin['rate']), (' as of %s' % as_of) if as_of else '')
                    + (' The sheets print %s.' % span if span else ''))
        return ("Calculator prices are the Current USD printed on each sheet, for the style's fit."
                + (' The sheets print %s RMB per US dollar.' % span if span else ''))

    def run(self):
        self.load_inputs()
        self.route()
        self.merge_inventory()
        self.cost_supply()
        self.index_analytics()
        self.build_lines()
        self.build_apo()
        self.build_inventory()
        self.double_bookings()
        self.build_production()
        self.build_styles()
        self.build_shipped()
        coverage = self.coverage()
        unresolved = self.unresolved()
        conflicts = self.conflicts()
        alerts = self.alerts()
        evidence = self.evidence()
        totals = self.totals()
        st = self.R.get('stats') or {}
        ls = st.get('lines') or {}
        S = self.S
        fxin = self.fx_inputs()
        mult = S['landed'].get('mode') == 'multiplier'
        notes = [
            self.fx_note(fxin),
            ('Factory prices are FOB. Import costs (tariffs, freight and fees) are a multiplier on the factory cost: '
             'one for natural fibers and one for synthetic fibers. Royalty, chargebacks and revenue costs use the '
             'Assumptions tab.' if mult else
             'Factory prices are FOB. Duty, freight, fees, royalty, chargebacks and revenue costs use the Assumptions tab.'),
            ('Goods that land in Canada use the same import multiplier. Goods the customer imports carry no import costs.'
             if mult else
             'Goods that land in Canada use an assumed Canadian duty rate. Goods the customer imports carry no US duty, freight or fees.'),
            'Supply follows the smart routing engine. Units it cannot place are costed at the style average.',
            "Shipped history is costed at today's costs, not the costs of the time.",
        ]
        if royalty_base(S) == 'revenue':
            notes.append('Royalty is a percent of revenue, before chargebacks.')
        rc_names = [str(it.get('name') or it.get('key') or '').strip() for it in S['revenueCosts']['items']
                    if (_fnum(it.get('pct')) or 0) > 0]
        if any(rc_names):
            notes.append('Profit also takes off revenue costs, each a percent of revenue: %s.'
                         % ', '.join(n for n in rc_names if n))
        if self.ci.rule_mode == 'combined':
            notes.append('When several factories quote a style, its cost is the average of their quotes, or the lowest '
                         'quote when they differ by more than %s percent. Every row of the style uses that one cost.'
                         % ('%g' % self.ci.wide_pct))
        notes.append("Each factory's price for a style comes from one sheet row, picked from the style number: its "
                     "brand's section, fabric, sleeve, pattern and fit column, and no special finish the style does not "
                     'have. Rows are never averaged. Where several rows still fit, the lowest is used and flagged.')
        if not S['bulk'].get('includeInTotals'):
            notes.append('Bulk lines are a forecast. They are shown apart and left out of the totals.')
        if self.ci.unrated:
            notes.append('%d calculator %s no sheet rate. %s as printed.'
                         % (self.ci.unrated, 'price carries' if self.ci.unrated == 1 else 'prices carry',
                            'It stays' if self.ci.unrated == 1 else 'They stay'))
        if self.whc.tiers:
            notes.append('Warehouse stock is costed by lot where the ledger allows it, else by the refs that made the style.')
        blocks = self.assumption_blocks()
        if blocks:
            notes.append('Unconfirmed assumptions: %s.' % ', '.join(n for _, n in blocks))
        inputs = {
            'ats_sync': self.inv_src.get('last_sync'), 'ledger_sync': self.led_src.get('last_sync'),
            'ledger_rows': len(self.ledger_rows), 'orders_ok': self.oo_src.get('ok') is True,
            'orders_fetched_at': self.oo_src.get('fetched_at'), 'apo_sync': self.apo_src.get('last_sync'),
            'analytics_ingested': self.sa_src.get('ingestedAt') if self.sa else None,
            'analytics_to': self.sa_src.get('to') if self.sa else None,
            'analyticsProblem': self.an_problem, 'analyticsRowsSkipped': self.an_skipped,
            'costbookInvalid': list(self.ci.invalid_params), 'costbookBadRecords': self.ci.bad_records,
            'costbook_etag': None, 'settings_etag': None, 'overrides_etag': None,
            'costbook_generated': self.cb.get('generatedAt'),
            'fob_source': self.fob_src,
            'routing': {'skuStatus': st.get('skuStatus'), 'skuMode': st.get('skuMode'),
                        'clock': st.get('clock'), 'assignmentsHonored': st.get('assignmentsHonored'),
                        'suppressedLots': st.get('suppressedLots'), 'lineUnits': ls.get('units'),
                        'placedUnits': ls.get('placed'), 'unsourcedUnits': ls.get('unsourced'),
                        'forcedUnits': ls.get('forcedUnits'), 'options': st.get('options')},
            'lotTiers': dict(self.whc.tiers), 'overridesActive': self.ci.overrides_active,
            'fx': fxin,
        }
        return {
            'v': 1, 'builtAt': self.now_iso, 'asOf': self.today, 'inputs': inputs, 'settings': S,
            'dict': self.dictionary(),
            'lines': _table(LINE_FIELDS, self.lines), 'alloc': _table(ALLOC_FIELDS, self.alloc),
            'apo': _table(APO_FIELDS, self.apo), 'inventory': _table(INVENTORY_FIELDS, self.inventory),
            'production': _table(PRODUCTION_FIELDS, self.production), 'styles': _table(STYLE_FIELDS, self.styles),
            'shipped': self.shipped, 'coverage': coverage, 'unresolved': unresolved, 'conflicts': conflicts,
            'alerts': alerts, 'evidence': evidence, 'totals': totals, 'notes': notes,
        }


def build_dataset(src, costbook, settings, overrides, now_iso, routing_module=None):
    """Build the DESIGN 5.4 dataset. Raises the routing module's RoutingInputError when the
    ledger or the ATS feed is empty (pnl.py checks both before calling)."""
    if routing_module is None:
        import pnl_routing as routing_module
    return _Build(src, costbook, settings, overrides, now_iso, routing_module).run()


# ── Private local runs only (the server never calls these) ──
def _utc_to_et(t):
    """Naive UTC datetime -> naive US Eastern wall clock (US DST rule)."""
    def nth_sunday(y, m, n):
        d = date(y, m, 1)
        return d + timedelta(days=(6 - d.weekday()) % 7 + 7 * (n - 1))
    start = datetime.combine(nth_sunday(t.year, 3, 2), datetime.min.time()) + timedelta(hours=7)
    end = datetime.combine(nth_sunday(t.year, 11, 1), datetime.min.time()) + timedelta(hours=6)
    return t - timedelta(hours=4 if start <= t < end else 5)


def load_snapshot(sp_dir, today=None, now=None, ledger_history=None):
    """Sources (DESIGN 3.4 shapes) from a local snapshot folder: live/inventory.json,
    live/production.json, live/apo.json, live/allocations.json, oo_orders.json and, when present,
    oo_analytics.json and dicts.json. The clock defaults to the ATS sync time in US Eastern."""
    import json
    import os

    def rd(*parts, required=True):
        p = os.path.join(sp_dir, *parts)
        if not os.path.exists(p):
            if required:
                raise FileNotFoundError(p)
            return None
        with open(p, encoding='utf-8') as f:
            return json.load(f)
    inv = rd('live', 'inventory.json')
    prod = rd('live', 'production.json')
    apo = rd('live', 'apo.json')
    alloc = rd('live', 'allocations.json', required=False) or {}
    oo = rd('oo_orders.json')
    an = rd('oo_analytics.json', required=False)
    dicts = (rd('dicts.json', required=False) or {}).get('dicts') or {}
    rows = alloc.get('allocations') or []
    et = None
    t = _Build._parse_ts(inv.get('last_sync'))
    if t:
        et = _utc_to_et(t)
    today = today or (et.date().isoformat() if et else None)
    now = now or (et.isoformat(timespec='seconds') if et else None)
    return {
        'inventory': {'items': inv.get('inventory') or [], 'last_sync': inv.get('last_sync')},
        'ledger': {'rows': prod.get('production') or [], 'last_sync': prod.get('last_sync')},
        'apo': {'rows': apo.get('apo') or [], 'last_sync': apo.get('last_sync')},
        'manual_allocations': [r for r in rows if r.get('source') == 'manual'],
        'vw_allocations': [r for r in rows if r.get('source') == 's3'],
        'open_orders': {'orders': oo.get('orders') or [], 'ok': not oo.get('errors'), 'fetched_at': oo.get('timestamp')},
        'fob_customers': list(dicts.get('FOB_CUSTOMER_CODES') or []),
        'routing_inputs': {'now': now} if now else {},
        'sales_analytics': an,
        'today': today,
        'ledger_history': {'rows': list(ledger_history or [])},
    }


def main(argv=None):
    import argparse
    import json
    import sys
    import time
    ap = argparse.ArgumentParser(description='Build the P&L dataset from a local snapshot (CONFIDENTIAL output; local only).')
    ap.add_argument('--snapshot', required=True, help='folder with live/*.json, oo_orders.json, oo_analytics.json')
    ap.add_argument('--costbook', required=True, help='cost book JSON (local file)')
    ap.add_argument('--settings', help='settings JSON (optional)')
    ap.add_argument('--overrides', help='overrides JSON list (optional)')
    ap.add_argument('--ledger-history', help='JSON list of past ledger lines (optional)')
    ap.add_argument('--today', help='YYYY-MM-DD (default: the ATS sync date in US Eastern)')
    ap.add_argument('--now', help='YYYY-MM-DDTHH:MM:SS US Eastern (routing suppression clock)')
    ap.add_argument('--out', required=True, help='dataset JSON to write (keep it local)')
    a = ap.parse_args(argv)

    def rd(p, default):
        if not p:
            return default
        with open(p, encoding='utf-8') as f:
            return json.load(f)
    hist = rd(a.ledger_history, [])
    src = load_snapshot(a.snapshot, today=a.today, now=a.now, ledger_history=hist.get('rows') if isinstance(hist, dict) else hist)
    cb, st, ov = rd(a.costbook, {}), rd(a.settings, {}), rd(a.overrides, [])
    import pnl_routing
    t0 = time.perf_counter()
    ds = build_dataset(src, cb, st, ov, datetime.utcnow().replace(microsecond=0).isoformat() + 'Z', pnl_routing)
    dt = time.perf_counter() - t0
    body = json.dumps(ds, separators=(',', ':'), allow_nan=False)
    with open(a.out, 'w', encoding='utf-8') as f:
        f.write(body)
    counts = {k: len(ds[k]['rows']) for k in ('lines', 'alloc', 'apo', 'inventory', 'production', 'styles')}
    print('built in %.2fs, %d bytes, rows %s, alerts %d' % (dt, len(body), counts, len(ds['alerts'])), file=sys.stderr)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
