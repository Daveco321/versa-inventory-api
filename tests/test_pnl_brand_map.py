"""Synthetic tests for the invoice brand map in the P&L engine (David, Oct 7 2026).

Rule: a style on past invoices takes the brand A2000 gave it (the open-orders brandMap that rides
on the sales analytics payload); a new style follows its style number. The map only decides when
the code's own letters do not name a known brand. No map means every brand exactly as before.

No real data: every style, brand letter pair and customer below is invented.
Run: python -m unittest discover -s tests -p "test_pnl_brand_map.py"
"""
import copy
import json
import os
import sys
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))
sys.path.insert(0, HERE)
import pnl_engine as E  # noqa: E402
import test_pnl_engine as T  # noqa: E402   (fixtures only: src, build, rows, by)

# Synthetic codes. Legacy word codes are not modern codes, so only program_brand reads their letters.
WORD_GUESS = 'QQNAWORD'        # letters 3-4 say NA: today's positional guess
WORD_PLAIN = 'ZZPLAIN7'        # letters 3-4 name no brand: blank today
DASH_HIST = 'ZZDASH-NAVY'      # a legacy dash code as the invoice history keys it
DASH_LINE = 'ZZDASH-NAVY-L'    # the same style with a size, on an open line
WORD_BLACK = 'ZZBKWORD'        # map key BLACK
WORD_BLO = 'ZZBOWORD'          # map key BLO, feed label BLACK
MOD_KNOWN = 'ZZNAQF611SLS'     # modern, letters NA (a known brand)
MOD_UNKNOWN = 'ZZLUQF612SLS'   # modern, letters LU (no such brand code)

MAP = {WORD_GUESS: 'DKNY', WORD_PLAIN: 'ARCHITECT', DASH_HIST: 'NW', WORD_BLACK: 'BLACK',
       WORD_BLO: 'BLO', MOD_KNOWN: 'CHAPS', MOD_UNKNOWN: 'LUCKY'}


def hist_row(style, units=10, value=100.0, label='SYN'):
    return [style, label, units, value, '2025-06-01', '2026-01-20',
            {'2026-01': [units, value, 0, 0.0]}, {'ROSS': [units, value]}, 'RED']


def src_map(bases=None, labels=None):
    """The shared fixture plus synthetic history, one legacy open line, one Black Label feed SKU and,
    when bases is not None, a brandMap on the analytics payload."""
    s = T.src()
    for st in (WORD_GUESS, WORD_PLAIN, DASH_HIST, WORD_BLACK, MOD_KNOWN, MOD_UNKNOWN):
        s['sales_analytics']['styles'].append(hist_row(st))
    s['sales_analytics']['styles'].append(hist_row(WORD_BLO, label='BLO'))
    s['inventory']['items'] = s['inventory']['items'] + [T.inv(WORD_BLO + '-M', tr=30, brand='BLACK')]
    s['open_orders']['orders'] = s['open_orders']['orders'] + [T.order('9', DASH_LINE, 12, 10.0, cust='KOHL')]
    if bases is not None:
        s['sales_analytics']['brandMap'] = {'v': 1, 'labels': dict(labels or {}), 'bases': dict(bases)}
    return s


def shipped_brands(ds):
    return {r['base']: r['brand'] for r in T.rows({'b': ds['shipped']['byStyle']}, 'b')}


def customer_brands(ds):
    return {r['base']: r['brand'] for r in T.rows({'b': ds['shipped']['byCustomer']}, 'b')}


def style_brands(ds):
    return {r['base']: r['brand'] for r in T.rows(ds, 'styles')}


class BrandMapRule(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.before = T.build(src_map())
        cls.after = T.build(src_map(MAP))

    def test_codes_are_what_the_tests_assume(self):
        for code in (WORD_GUESS, WORD_PLAIN, DASH_HIST, DASH_LINE, WORD_BLACK, WORD_BLO):
            self.assertIsNone(E.decode_sku(code, T.PARAMS), code)
        self.assertEqual(E.decode_sku(MOD_KNOWN, T.PARAMS)['brand'], 'NA')
        self.assertEqual(E.decode_sku(MOD_UNKNOWN, T.PARAMS)['brand'], 'LU')
        self.assertNotIn('LU', E.BRAND_NAMES)

    def test_without_a_map_the_old_guesses_stand(self):
        sh = shipped_brands(self.before)
        self.assertEqual(sh[WORD_GUESS], 'NA')          # the letters' guess
        self.assertEqual(sh[WORD_PLAIN], '')
        self.assertEqual(sh[DASH_HIST], '')
        self.assertEqual(sh[MOD_UNKNOWN], 'LU')
        self.assertEqual(sh[MOD_KNOWN], 'NA')
        self.assertNotIn('brandMap', self.before['inputs'])

    def test_history_styles_take_the_map_brand(self):
        for brands in (shipped_brands(self.after), customer_brands(self.after), style_brands(self.after)):
            self.assertEqual(brands[WORD_GUESS], 'DK')
            self.assertEqual(brands[WORD_PLAIN], 'AR')
            self.assertEqual(brands[DASH_HIST], 'NW')
            self.assertEqual(brands[MOD_UNKNOWN], 'LB')  # letters that name no brand: the map decides
        self.assertEqual(self.after['dict']['brands']['AR'], 'Architect')
        self.assertEqual(self.after['inputs']['brandMap'], {'v': 1, 'bases': len(MAP)})

    def test_a_modern_code_keeps_its_own_brand(self):
        for brands in (shipped_brands(self.after), customer_brands(self.after), style_brands(self.after)):
            self.assertEqual(brands[MOD_KNOWN], 'NA')    # the map says CHAPS: a new style follows its number

    def test_black_label_keeps_its_pseudo_code(self):
        self.assertEqual(shipped_brands(self.after)[WORD_BLACK], 'BLK')
        # Map key BLO, feed label BLACK: Black Label, as for a decoded BL (review finding R5-1).
        self.assertEqual(style_brands(self.after)[WORD_BLO], 'BLK')
        inv = {r['sku']: r for r in T.rows(self.after, 'inventory')}
        self.assertEqual(inv[WORD_BLO + '-M']['brand'], 'BLK')

    def test_a_modern_bl_code_reads_black_label_from_the_map(self):
        """BL is both Bloomingdale's and Black Label: the map key BLACK says Black Label, like the feed label."""
        def brand(bases):
            s = src_map(bases)
            s['sales_analytics']['styles'].append(hist_row('ZZBLQF613SLS', label='BLO'))
            return shipped_brands(T.build(s))['ZZBLQF613SLS']
        self.assertEqual(brand(None), 'BL')
        self.assertEqual(brand({'ZZBLQF613SLS': 'BLACK'}), 'BLK')
        self.assertEqual(brand({'ZZBLQF613SLS': 'BLO'}), 'BL')
        self.assertEqual(brand({'ZZBLQF613SLS': 'NAUTICA'}), 'BL')       # a known code keeps its letters

    def test_an_open_line_on_a_mapped_dash_code(self):
        before = T.by(self.before, 'lines', 'id')['9|' + DASH_LINE]
        after = T.by(self.after, 'lines', 'id')['9|' + DASH_LINE]
        self.assertEqual((before['base'], before['brand']), ('ZZDASH', ''))
        self.assertEqual((after['base'], after['brand']), ('ZZDASH', 'NW'))   # by the history's own base

    def test_a_mapped_dash_history_key_prices_its_brand_on_the_apo(self):
        """The APO price maps read the history key itself in the map (its base drops the dash parts)."""
        def run(bases):
            s = src_map(bases)
            s['sales_analytics']['styles'].append(
                ['ZZDASH-RED', 'SYN', 600, 8700.0, '2024-03-01', '2025-06-10', {'2025-06': [600, 8700.0, 0, 0.0]},
                 {'MENS': [600, 8700.0]}, 'RED'])
            s['apo']['rows'] = s['apo']['rows'] + [
                {'style': 'MWNWQF303SLS', 'qty': 24, 'customer': 'MEN WARHOUSE', 'po': 'SYNTH MW NW'}]
            return {r['po']: r for r in T.rows(T.build(s), 'apo')}['SYNTH MW NW']
        mw = run(dict(MAP, **{'ZZDASH-RED': 'NW'}))
        self.assertEqual((mw['cust'], mw['brand'], mw['priceBasis'], mw['estPrice']),
                         ('MENS', 'NW', 'customer_brand_invoice', E.r4(8700.0 / 600)))
        self.assertNotEqual(run(None)['priceBasis'], 'customer_brand_invoice')   # no map: no brand evidence

    def test_the_map_beats_a_feed_label(self):
        s = src_map(MAP)
        s['inventory']['items'] = s['inventory']['items'] + [T.inv(WORD_GUESS + '-M', tr=20, brand='NAUTICA')]
        inv = {r['sku']: r for r in T.rows(T.build(s), 'inventory')}
        self.assertEqual(inv[WORD_GUESS + '-M']['brand'], 'DK')
        s['sales_analytics'].pop('brandMap')
        inv = {r['sku']: r for r in T.rows(T.build(s), 'inventory')}
        self.assertEqual(inv[WORD_GUESS + '-M']['brand'], 'NA')                # the feed label, as before

    def test_a_key_without_a_code_changes_nothing(self):
        ds = T.build(src_map({WORD_GUESS: 'SOMETHING NEW', WORD_PLAIN: ''}))
        sh = shipped_brands(ds)
        self.assertEqual((sh[WORD_GUESS], sh[WORD_PLAIN]), ('NA', ''))

    def test_costs_do_not_move(self):
        """The map names brands only: unit costs, levels and grades are the same with and without it."""
        keep = ('base', 'fobU', 'landedU', 'level', 'grade')
        for t in ('styles',):
            a = [{k: r.get(k) for k in keep} for r in T.rows(self.before, t)]
            b = [{k: r.get(k) for k in keep} for r in T.rows(self.after, t)]
            self.assertEqual(a, b)
        a = [(r['base'], r['fobU'], r['level']) for r in T.rows({'b': self.before['shipped']['byStyle']}, 'b')]
        b = [(r['base'], r['fobU'], r['level']) for r in T.rows({'b': self.after['shipped']['byStyle']}, 'b')]
        self.assertEqual(a, b)


class Corrections(unittest.TestCase):
    """brandMap.fixes (Oct 7 2026): a style number made with the wrong brand letters. The map's brand
    wins even over the decoded brand, for that style only."""

    def test_a_fix_beats_the_decoded_brand(self):
        s = src_map(MAP)
        s['sales_analytics']['brandMap']['fixes'] = {MOD_KNOWN: 'CHAPS'}
        ds = T.build(s)
        for brands in (shipped_brands(ds), customer_brands(ds), style_brands(ds)):
            self.assertEqual(brands[MOD_KNOWN], 'CH')     # its letters say NA; the correction says CHAPS
        self.assertEqual(customer_brands(ds)[WORD_GUESS], 'DK')   # other styles keep the plain map rule

    def test_reader(self):
        self.assertEqual(E.brand_map_bases({'brandMap': {'fixes': {' zzq ': ' nicole '}}}, 'fixes'),
                         {'ZZQ': 'NICOLE'})
        self.assertEqual(E.brand_map_bases({'brandMap': {'bases': {}}}, 'fixes'), {})


class NoMapIsUnchanged(unittest.TestCase):
    def dump(self, s):
        return json.dumps(T.build(s), sort_keys=True, allow_nan=False)

    def test_an_empty_or_absent_map_builds_the_same_bytes(self):
        plain = self.dump(src_map())
        for bm in ({'v': 1, 'labels': {}, 'bases': {}}, {'v': 1}, {'bases': 'oops'}, None, 'oops', []):
            s = src_map()
            s['sales_analytics']['brandMap'] = copy.deepcopy(bm)
            self.assertEqual(self.dump(s), plain, repr(bm))

    def test_a_map_of_other_styles_only_adds_its_input_stamp(self):
        plain = T.build(src_map())
        other = T.build(src_map({'YYOTHER1': 'NAUTICA'}))
        self.assertEqual(other['inputs'].pop('brandMap'), {'v': 1, 'bases': 1})
        self.assertEqual(json.dumps(other, sort_keys=True), json.dumps(plain, sort_keys=True))

    def test_a_building_payload_has_no_map(self):
        s = T.src(analytics=False)
        self.assertEqual(json.dumps(T.build(s), sort_keys=True), json.dumps(T.build(copy.deepcopy(s)), sort_keys=True))
        self.assertEqual(E.brand_map_bases({'building': True}), {})


class BrandTables(unittest.TestCase):
    MAP_KEYS = ('NAUTICA', 'JNY', 'NW', 'LUCKY', 'BEN', 'CHAPS', 'USPA', 'DKNY', 'BEENE', 'EB', 'VD', 'SHAQ',
                'VINCE', 'TAYION', 'REEBOK', 'KL', 'STRAHAN', 'HC', 'VERSA', 'CL', 'BLO', 'DN', 'AMERICA', 'NE',
                'NICOLE', 'ARCHITECT', 'PRESWICK', 'BJ', 'BUFFALO', 'ADRIENNE')

    def test_every_map_key_has_a_named_code(self):
        for k in self.MAP_KEYS:
            self.assertIn(E.MAP_KEY_CODE.get(k), E.BRAND_NAMES, k)
        self.assertIn('BLACK', E._BLACK_LABELS)                       # BLACK -> BLK, not a table entry
        want = {'NICOLE': 'NM', 'KL': 'KL', 'REEBOK': 'RB', 'VERSA': 'VS', 'HC': 'HC', 'CL': 'CL', 'DN': 'DN',
                'NE': 'NE', 'ARCHITECT': 'AR', 'PRESWICK': 'PM', 'BJ': 'BJ', 'BUFFALO': 'BD', 'ADRIENNE': 'AV',
                'STRAHAN': 'MS', 'JNY': 'JN', 'NW': 'NW', 'LUCKY': 'LB'}
        self.assertEqual({k: E.MAP_KEY_CODE[k] for k in want}, want)
        names = {'HC': 'Henri Christian', 'CL': 'Christian Lacroix', 'AR': 'Architect', 'PM': 'Preswick & Moore',
                 'BJ': "Berkley Jensen (BJ's)", 'BD': 'Buffalo David Bitton', 'AV': 'Adrienne Vittadini'}
        self.assertEqual({k: E.BRAND_NAMES[k] for k in names}, names)
        self.assertEqual(E.BRAND_NAMES['CS'], 'Chaps')                 # modern CS codes are Chaps

    def test_the_feed_label_table_is_unchanged(self):
        self.assertEqual(E.BRAND_LABEL_CODE, {
            'NAUTICA': 'NA', 'VD': 'VD', 'CHAPS': 'CH', 'USPA': 'US', 'TAYION': 'TA', 'EB': 'EB', 'BEN': 'BE',
            'LUCKY': 'LB', 'JNY': 'JN', 'BEENE': 'GB', 'SHAQ': 'SH', 'STRAHAN': 'MS', 'DKNY': 'DK', 'VINCE': 'VC',
            'NM': 'NM', 'KLP': 'KL', 'RB': 'RB', 'AMERICA': 'AC', 'BLACK': 'BL', 'BLO': 'BL', 'NW': 'NW'})

    def test_letters_never_name_a_map_only_brand(self):
        self.assertEqual(E.program_brand(WORD_GUESS), 'NA')
        for code in ('QQARWORD', 'QQPMWORD', 'QQBJWORD', 'QQBDWORD', 'QQAVWORD', 'QQHCWORD', 'QQCLWORD'):
            self.assertIsNone(E.program_brand(code), code)

    def test_new_codes_take_the_default_royalty_like_a_blank_brand(self):
        S = E.merge_settings({})
        blank = E._roy_pct('', S)
        self.assertEqual(blank, S['royalty']['defaultPct'])
        for code in ('AR', 'PM', 'BJ', 'BD', 'AV', 'HC', 'CL'):
            self.assertEqual(E._roy_pct(code, S), blank, code)
        S2 = E.merge_settings({'royalty': {'defaultPct': 7.5}})
        self.assertEqual(E._roy_pct('AR', S2), E._roy_pct('', S2))

    def test_map_reader(self):
        self.assertEqual(E.brand_map_bases({'brandMap': {'bases': {' zzx1 ': ' nautica ', 'ZZX2': 5, 3: 'X',
                                                                   'ZZX3': ''}}}), {'ZZX1': 'NAUTICA'})
        for sa in (None, {}, {'brandMap': None}, {'brandMap': {'bases': []}}, []):
            self.assertEqual(E.brand_map_bases(sa), {})


class DatasetStamps(unittest.TestCase):
    """A brandMap that first appears (an open-orders deploy) changes the source stamps, so the next
    check rebuilds the dataset even though the invoice history itself did not change."""

    def test_brand_map_changes_the_stamps(self):
        try:
            import pnl
        except Exception as e:  # pragma: no cover (flask missing)
            self.skipTest('pnl not importable: %s' % type(e).__name__)
        plain = pnl._stamps(pnl._normalize_sources(src_map()))
        again = pnl._stamps(pnl._normalize_sources(src_map()))
        with_map = pnl._stamps(pnl._normalize_sources(src_map(MAP)))
        same_map = pnl._stamps(pnl._normalize_sources(src_map(dict(MAP))))
        other = pnl._stamps(pnl._normalize_sources(src_map({WORD_GUESS: 'CHAPS'})))
        self.assertEqual(plain, again)
        self.assertNotEqual(plain, with_map)
        self.assertEqual(with_map, same_map)
        self.assertNotEqual(with_map, other)
        norm = pnl._normalize_sources(src_map(MAP))
        self.assertEqual(norm['sales_analytics']['brandMap']['bases'], MAP)


if __name__ == '__main__':
    unittest.main()
