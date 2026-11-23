"""
amazon_planner.py: decides what the Amazon agent proposes to send from Transcal to Amazon
FBA, in whole 36-piece cartons. It only proposes: David confirms every shipment by email
before anything is created at Amazon or sent to Transcal (David, Nov 23 2026).

Pure: every input is passed in, so the same code runs on live SP-API data, an MRP export
or a test fixture.

    plan_shipment(listings, stock, prepack_for, settings=None) -> dict
        listings     [Listing(sku, asin, parent, available, inbound, sold30, active=True)]
                     one per Amazon seller SKU (FBA and FBM twins included)
        stock        [StockRow(sku, transcal, hold)] AM warehouse rows; transcal = units
                     available at Transcal, hold = units promised to anyone but Amazon
                     (open orders in A2000 plus other customers' APO allocations)
        prepack_for  callable(base) -> {letter size: units in one 36-pc carton} or None
                     (the platform's prepack rules; see ratio_from_rule)
    ratio_from_rule(rule, fit) -> ({size: units per 36}, '') or (None, reason)

How a style is judged (David's team model, the Sep 2026 Q4 replens):
    velocity     units sold in the last 30 days across the style's Amazon SKUs, FBM twins included
    tier         HOT >= 300 sold -> target 1,512 at Amazon; STRONG 100+ -> 1,008;
                 MODERATE 30+ -> 720; SLOW 10+ -> 504; under 10 -> nothing is sent
    at Amazon    available + inbound (reserved is ignored)
    skipped      240+ days of cover at Amazon, or less than one carton short of the target
    size targets the target split 70% by each size's own sales, 30% by the prepack ratio
    cartons      prepack cartons first, enough for the neediest size (sending extra of the
                 other sizes is fine), then single-size cartons for sizes still short.
                 Never more than Transcal has free. Every carton is 36 pieces.
    never sent   -V partial cartons, store accounts' stock (non-AM), AMNASU201WH1/WH2/WH3,
                 sizes without an active Amazon listing
"""
import math
import re
from collections import namedtuple

import amazon_skus as K
from amazon_pick_ticket import Carton

Listing = namedtuple('Listing', 'sku asin parent available inbound sold30 active')
Listing.__new__.__defaults__ = (True,)
StockRow = namedtuple('StockRow', 'sku transcal hold')

DEFAULTS = {
    'tiers': ((300, 1512, 'HOT'), (100, 1008, 'STRONG'), (30, 720, 'MODERATE'), (10, 504, 'SLOW')),
    'cover_skip_days': 240,
    'velocity_weight': 0.7,
    'excluded_bases': ('AMNASU201WH1', 'AMNASU201WH2', 'AMNASU201WH3'),
}

_WORD_SIZE = {'SMALL': 'S', 'MEDIUM': 'M', 'LARGE': 'L', 'X-LARGE': 'XL', 'XLARGE': 'XL',
              'XX-LARGE': 'XXL', 'XXLARGE': 'XXL', 'XXX-LARGE': 'XXXL'}
_LETTERS = set(K.SIZE_TOKENS)


def rule_size(label, fit=''):
    """A prepack rule's size label as an Amazon letter size, or None.
    '15-15.5 / 34-35' (by fit), '15-15.5 (M)', 'S (14-14.5)', 'Medium', 'XL'."""
    t = str(label or '').strip().upper()
    m = re.match(r'^(\d{2})-(\d{2})(?:\.5)?\s*/\s*(\d{2})-(\d{2})$', t)
    if m:
        return K.neck_to_alpha(f'{m.group(1)}{m.group(2)}.5{m.group(3)}{m.group(4)}', fit)
    m = re.search(r'\(([A-Z0-9]{1,4})\)', t)
    if m and m.group(1) in _LETTERS:
        return m.group(1)
    m = re.match(r'^([A-Z0-9]{1,4})\s*\(', t)
    if m and m.group(1) in _LETTERS:
        return m.group(1)
    if t in _WORD_SIZE:
        return _WORD_SIZE[t]
    if t in _LETTERS:
        return t
    return None


def ratio_from_rule(rule, fit=''):
    """({size: units in one 36-pc carton}, '') from a prepack rule or an Override Tool size
    pack, or (None, reason). Rules written per inner (9 = 1/3/3/2) are scaled to 36."""
    if not rule or not rule.get('sizes'):
        return None, 'no prepack rule'
    pairs = []
    for entry in rule.get('sizes') or []:
        try:
            label, qty = entry[0], int(entry[1])
        except (TypeError, ValueError, IndexError):
            return None, 'prepack rule has an unreadable size line'
        size = rule_size(label, fit)
        if size is None:
            return None, f'prepack size "{label}" has no Amazon letter size'
        if qty > 0:
            pairs.append((size, qty))
    per = sum(q for _s, q in pairs)
    try:
        master = int(rule.get('master_qty') or K.CARTON_UNITS)
    except (TypeError, ValueError):
        master = K.CARTON_UNITS
    if per <= 0:
        return None, 'prepack rule has no sizes'
    if master != K.CARTON_UNITS or master % per:
        return None, f'prepack rule is not a {K.CARTON_UNITS}-piece carton'
    ratio = {}
    for size, q in pairs:
        ratio[size] = ratio.get(size, 0) + q * (master // per)
    return ratio, ''


def _pick_listing(listings):
    """The listing to send a size to: never the FBM twin or an inactive listing; the best
    seller first, then the -FBA program SKU, then the SKU text (stable)."""
    ok = [x for x in listings if x.active and K.split_sku(x.sku).marker != 'FBM']
    if not ok:
        return None
    return sorted(ok, key=lambda x: (-int(x.sold30 or 0), K.split_sku(x.sku).marker != 'FBA', x.sku))[0]


def _days(units, sold30):
    return None if not sold30 else round(units / (sold30 / 30.0))


def _tier(velocity, tiers):
    for floor, target, name in tiers:
        if velocity >= floor:
            return target, name
    return None, None


def plan_shipment(listings, stock, prepack_for, settings=None):
    cfg = dict(DEFAULTS)
    cfg.update(settings or {})
    excluded = {K.norm_sku(b) for b in cfg['excluded_bases']}
    w = float(cfg['velocity_weight'])

    # Amazon side, by style and letter size
    by_base = {}
    for x in listings:
        parts = K.split_sku(x.sku)
        if not parts.base.startswith(K.AMAZON_PREFIX):
            continue
        size = K.alpha_size(parts.size, K.fit_code(parts.base)) if parts.size else ''
        if size is None:
            continue
        by_base.setdefault(parts.base, {}).setdefault(size, []).append(x)

    # Transcal side: free units per prepack pool and per single-size row
    pool, singles, unreadable = {}, {}, []
    for r in stock:
        sku = K.norm_sku(r.sku)
        if not K.is_amazon_stock(sku):
            continue
        free = max(0, int(r.transcal or 0) - max(0, int(r.hold or 0)))
        parts = K.split_sku(sku)
        if not parts.size:
            pool[parts.base] = pool.get(parts.base, 0) + free
            continue
        size = K.alpha_size(parts.size, K.fit_code(parts.base))
        if not size:
            unreadable.append(sku)
            continue
        singles.setdefault(parts.base, {}).setdefault(size, []).append([sku, free])

    styles, cartons, skipped = [], [], []
    bases = sorted(set(pool) | set(singles))
    for base in bases:
        free_total = pool.get(base, 0) + sum(f for rows in singles.get(base, {}).values() for _s, f in rows)
        if free_total < K.CARTON_UNITS:
            continue                                   # nothing whole to send: not worth a line
        if base in excluded:
            skipped.append({'base': base, 'reason': 'kept out of Amazon shipments (WH1/WH2/WH3 rule)'})
            continue
        sizes = by_base.get(base)
        if not sizes:
            skipped.append({'base': base, 'reason': 'not listed on Amazon'})
            continue
        fit = K.fit_code(base)
        sold = {s: sum(int(x.sold30 or 0) for x in xs) for s, xs in sizes.items()}
        at_amz = {s: sum(int(x.available or 0) + int(x.inbound or 0) for x in xs
                         if K.split_sku(x.sku).marker != 'FBM') for s, xs in sizes.items()}
        velocity = sum(sold.values())
        at_amazon = sum(at_amz.values())
        target, tier = _tier(velocity, cfg['tiers'])
        if not target:
            skipped.append({'base': base, 'reason': f'sold only {velocity} in 30 days'})
            continue
        days = _days(at_amazon, velocity)
        if days is not None and days >= cfg['cover_skip_days']:
            skipped.append({'base': base, 'reason': f'Amazon has {at_amazon} (about {days} days)'})
            continue
        gap = target - at_amazon
        if gap < K.CARTON_UNITS:
            skipped.append({'base': base, 'reason': f'Amazon has {at_amazon} of the {target} target'})
            continue

        ratio = prepack_for(base)
        ratio = dict(ratio) if ratio else None
        all_sizes = set(s for s in sizes if s) | set(ratio or {})
        share = {}
        for s in all_sizes:
            by_sales = sold.get(s, 0) / velocity if velocity else 0.0
            by_ratio = (ratio.get(s, 0) / float(K.CARTON_UNITS)) if ratio else by_sales
            share[s] = w * by_sales + (1.0 - w) * by_ratio
        total_share = sum(share.values()) or 1.0
        need = {s: max(0.0, target * share[s] / total_share - at_amz.get(s, 0)) for s in all_sizes}
        target_size = {s: listing for s, listing in ((s, _pick_listing(sizes.get(s, []))) for s in all_sizes) if listing}
        notes = []

        # 1) prepack cartons from the style's pool
        k = 0
        pool_cartons = pool.get(base, 0) // K.CARTON_UNITS
        if ratio and pool_cartons:
            missing = sorted((s for s in ratio if s not in target_size), key=K.size_rank)
            if missing:
                notes.append('prepack not sent: no active Amazon listing for size ' + ', '.join(missing))
            else:
                wants = [math.ceil(need[s] / ratio[s]) for s in ratio if need.get(s, 0) > 0 and ratio[s] > 0]
                k = min(pool_cartons, max(wants) if wants else 0)
        elif pool_cartons and not ratio:
            notes.append('prepack not sent: no prepack rule matches this style')
        sent = {s: k * ratio.get(s, 0) for s in all_sizes} if (k and ratio) else {s: 0 for s in all_sizes}
        style_cartons = []
        lines = []
        if k:
            contents = {target_size[s].sku: ratio[s] for s in sorted(ratio, key=K.size_rank)}
            style_cartons += [Carton(base, contents) for _ in range(k)]
            for s in sorted(ratio, key=K.size_rank):
                lines.append({'warehouse_sku': base, 'amazon_sku': target_size[s].sku, 'size': s,
                              'kind': 'prepack', 'units': k * ratio[s], 'cartons': k})

        # 2) single-size cartons for sizes still short
        for s in sorted(all_sizes, key=K.size_rank):
            short = need.get(s, 0) - sent.get(s, 0)
            if short <= 0:
                continue
            rows = sorted(singles.get(base, {}).get(s, []), key=lambda r: (-r[1], r[0]))
            available = sum(f // K.CARTON_UNITS for _sku, f in rows)
            if not available:
                continue
            listing = target_size.get(s)
            if listing is None:
                notes.append(f'size {s} not sent: no active Amazon listing')
                continue
            want = min(int(math.ceil(short / K.CARTON_UNITS)), available)
            for row in rows:
                if want <= 0:
                    break
                n = min(want, row[1] // K.CARTON_UNITS)
                if n <= 0:
                    continue
                row[1] -= n * K.CARTON_UNITS
                want -= n
                sent[s] = sent.get(s, 0) + n * K.CARTON_UNITS
                style_cartons += [Carton(row[0], {listing.sku: K.CARTON_UNITS}) for _ in range(n)]
                lines.append({'warehouse_sku': row[0], 'amazon_sku': listing.sku, 'size': s,
                              'kind': 'single', 'units': n * K.CARTON_UNITS, 'cartons': n})

        if not style_cartons:
            skipped.append({'base': base, 'reason': '; '.join(notes) or 'the sizes Amazon needs are not free at Transcal'})
            continue
        for ln in lines:
            s = ln['size']
            ln['at_amazon'] = at_amz.get(s, 0)
            ln['sold30'] = sold.get(s, 0)
            ln['days_left'] = _days(at_amz.get(s, 0), sold.get(s, 0))
        units = sum(c.units for c in style_cartons)
        styles.append({'base': base, 'fit': fit, 'tier': tier, 'target': target, 'velocity': velocity,
                       'at_amazon': at_amazon, 'days_left': days, 'gap': gap,
                       'prepack_cartons': k, 'single_cartons': len(style_cartons) - k,
                       'cartons': len(style_cartons), 'units': units, 'lines': lines, 'notes': notes,
                       'why': why_text(velocity, at_amazon, days, tier, target)})
        cartons += style_cartons

    styles.sort(key=lambda st: (st['days_left'] if st['days_left'] is not None else 10 ** 6, st['base']))
    return {'styles': styles, 'cartons': cartons, 'skipped': skipped, 'unreadable_sizes': unreadable,
            'totals': {'styles': len(styles), 'cartons': len(cartons), 'units': sum(c.units for c in cartons)}}


def why_text(velocity, at_amazon, days, tier, target):
    """One plain sentence for the email: why this style is on the shipment."""
    left = 'none left' if not at_amazon else (f'about {days} days left' if days is not None else 'no recent sales')
    return (f'Sold {velocity:,} in the last 30 days. Amazon has {at_amazon:,} including inbound ({left}). '
            f'Target for a {tier.lower()} seller is {target:,}.')
