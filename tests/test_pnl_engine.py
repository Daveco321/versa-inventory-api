"""Synthetic unit tests for pnl_engine (the P&L cost engine).

No real data: every factory code, ref, style, brand, fabric code and price below is invented.
Prices are sentinel values (7.7777, 4.4444 and similar) so a leak would be easy to spot.
Run: python -m unittest discover -s tests -p "test_pnl_engine.py"
"""
import copy
import json
import math
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import pnl_engine as E  # noqa: E402
import pnl_routing as R  # noqa: E402

FXB = 7.0
R2 = 7.9191          # a second synthetic sheet rate (contract C10)
PARAMS = {
    'fxBase': FXB, 'ssDelta': -0.1111, 'fitPremium': {'regular': 0.2222, 'bigTall': 0.5555},
    'gridPrecedence': {'TT': ['GY', 'GN'], '_default': ['GN', 'GY']},
    'poolOrder': {'all': ['GN:BASE', 'GN:CLUB', 'GY:BASE', 'GY:CLUB', 'GK:ANY'],
                  'polo': {'primary': ['GK:ANY'], 'fallback': ['GY:BASE']},
                  'GY': {'byGroup': {'CLUB': ['GY:CLUB']}, 'base': ['GY:BASE']},
                  'GN': {'byGroup': {'CLUB': ['GN:CLUB']}, 'base': ['GN:BASE']},
                  'recordKinds': ['calculator', 'factory_quotation'], 'excludeFlags': ['SKIP_ME']},
    'programMap': {'ZZKIT01': {'brand': 'QA', 'fab': 'QF', 'fit': 'SLIM', 'sleeve': 'LS', 'pat': None, 'group': 'OTHER',
                               'cat': 'dress_shirt', 'src': 'synthetic kit (fit assumed slim)'}},
    'kitPcs': {'ZZKIT01': 12},
    'customerGroupPrefix': {'CL': 'CLUB', '_default': 'OTHER'},
}


# Cost-book price fields are read by role (contract C1). The fixtures use role-neutral synthetic
# names and declare them in meta.priceFieldRoles, as the parser does. e1 is the sheet's printed RMB
# rate (contract C10). Calculator fixtures print at FXB unless a test says otherwise, so their
# printed (usd) and restated (base) prices are equal.
F_BASE, F_CUT, F_E1 = 'price_usd_base', 'price_usd_cut', 'fx_sheet_e1'
ROLES = {'usd': 'price_usd', 'base': F_BASE, 'cut': F_CUT, 'rmb': 'rmb_price', 'e1': F_E1}


def rec(rid, sc, kind, price, **kw):
    r = {'id': rid, 'source_code': sc, 'record_kind': kind, 'price_usd': price, F_BASE: price,
         F_CUT: None, 'factory_code': None, 'production_ref': None,
         'production_ref_resolved': None, 'style': None, 'brand_code': None, 'customer_group': None,
         'category': 'dress_shirt', 'fabric_codes': [], 'fabric_code_confidence': 'high', 'sleeve': None,
         'fit_class': None, 'pattern': None, 'pattern_effective': None, 'flags': [], 'pool': None,
         'scope': 'fabrication', 'origin_file': 'SYNTH.xlsx', 'sheet': 'S', 'cell': rid.split('!')[1], 'row': 1,
         'fabrication': 'synthetic', 'brand': 'Synthetic', 'rmb_price': None, F_E1: None}
    r.update(kw)
    if 'pattern' in kw and 'pattern_effective' not in kw:
        r['pattern_effective'] = kw['pattern']
    return r


def calc(rid, pool, price, brand, fab, fit, pat=None, sleeve=None, alt=None, e1=FXB, **kw):
    """A calculator record printed at price (Current USD) on a sheet whose rate is e1. Its base-role
    price is the printed price restated at FXB, as the parser writes it."""
    kw[F_CUT] = alt
    kw.setdefault(F_BASE, round(price * e1 / FXB, 4))
    kw[F_E1] = e1
    return rec(rid, 'SYN-' + pool.split(':')[0], 'calculator', price, pool=pool, brand_code=brand, fabric_codes=fab,
               fit_class=fit, pattern=pat, pattern_effective=pat, sleeve=sleeve, factory_code='UNKNOWN', **kw)


RECORDS = [
    # AA: a per-ref, per-style list (Pinnacle-type)
    rec('AA!J2', 'PC', 'ref_price_list', 7.7777, factory_code='AA', production_ref='AA26001', style='ROQAQF101SLS',
        fabric_codes=['QF'], scope='ref_style'),
    rec('AA!J3', 'PC', 'ref_price_list', 6.6666, factory_code='AA', production_ref='AA26002', style='TMQAQF102SLS',
        fabric_codes=['QF'], scope='ref_style'),
    # BB: a per-ref list keyed by pattern (David Peng-type)
    rec('BB!H2', 'DP', 'ref_price_list', 3.3333, factory_code='BB', production_ref_resolved='BB26001', pattern='SOLID',
        sleeve='LS', customer_group='ROSS', fabric_codes=['QK'], scope='ref'),
    rec('BB!H3', 'DP', 'ref_price_list', 3.8787, factory_code='BB', production_ref_resolved='BB26001', pattern='PRINT',
        sleeve='LS', customer_group='ROSS', fabric_codes=['QK'], scope='ref'),
    rec('BB!H4', 'DP', 'ref_price_list', 3.4444, factory_code='BB', production_ref_resolved='BB26002', pattern='SOLID',
        sleeve='LS', customer_group='AMAZON', fabric_codes=['QK'], scope='ref'),
    # CC: a per-ref list keyed by pattern and fit (KinYun-type)
    rec('CC!F3', 'KY', 'ref_price_list', 2.2222, factory_code='CC', production_ref_resolved='CC26001', pattern='SOLID',
        fit_class='SLIM', customer_group='ROSS', fabric_codes=['QS'], scope='ref'),
    rec('CC!F4', 'KY', 'ref_price_list', 2.7222, factory_code='CC', production_ref_resolved='CC26001', pattern='PRINT',
        fit_class='SLIM', customer_group='ROSS', fabric_codes=['QS'], scope='ref'),
    # calculator grids
    calc('GN!F1', 'GN:BASE', 4.4444, 'QA', ['QF'], 'SLIM', 'SOLID', alt=4.5454),
    calc('GN!K1', 'GN:BASE', 4.6666, 'QA', ['QF'], 'REGULAR', 'SOLID'),
    calc('GN!F2', 'GN:BASE', 5.0505, 'QA', ['QH'], 'SLIM', 'SOLID'),
    calc('GN!F3', 'GN:BASE', 4.2424, 'QA', ['QX', 'QM'], 'SLIM', 'SOLID'),
    calc('GN!F4', 'GN:BASE', 3.9393, 'QA', ['QJ'], 'SLIM', 'SOLID'),
    calc('GN!F5', 'GN:BASE', 8.8888, 'QA', ['QF'], 'SLIM', 'SOLID', flags=['SKIP_ME']),
    calc('GN!F6', 'GN:BASE', 4.3434, 'DK', ['QF'], 'SLIM', 'SOLID'),
    calc('GY!F1', 'GY:BASE', 4.1717, 'QA', ['QF'], 'SLIM', 'SOLID'),
    calc('GK!F1', 'GK:ANY', 3.1313, 'QA', ['PO'], 'SLIM', None, category='polo'),
    rec('HP!D1', 'SYN-HP', 'factory_quotation', 5.5151, pool='GY:BASE', brand_code='QA', fabric_codes=['QU'],
        fit_class='SLIM', pattern='SOLID', factory_code='TT'),
]
COSTBOOK = {'v': 1, 'generatedAt': '2026-01-01T00:00:00Z', 'params': PARAMS, 'records': RECORDS,
            'conflict_groups': [{'id': 'same_text_price_conflict#x1', 'kind': 'same_text_price_conflict',
                                 'members': ['GY!F1'], 'spread': 0.0, 'median': 4.1717}],
            'meta': {'priceFieldRoles': dict(ROLES), 'priceFields': {n: {} for k, n in ROLES.items() if k != 'e1'}}}


def led(ref, style, units, po='SYNTH PO', etd='2026-06-01', landing='TR'):
    return {'production': ref, 'poName': po, 'style': style, 'units': units, 'etd': etd, 'arrival': None,
            'port_dated': False, 'fob_flag': False, 'warehouse': landing, 'shipmentNo': ''}


def ci(ledger=(), settings=None, overrides=None, costbook=None, today='2026-03-02'):
    return E.CostIndex(costbook or COSTBOOK, settings or {}, overrides or [], list(ledger), today=today)


class DecodeSku(unittest.TestCase):
    def test_modern_shirt(self):
        d = E.decode_sku('ROQAQF101SLS-15-32', PARAMS)
        self.assertEqual((d['base'], d['cust'], d['brand'], d['fab'], d['cat'], d['fit'], d['sleeve'], d['pat']),
                         ('ROQAQF101SLS', 'RO', 'QA', 'QF', 'dress_shirt', 'SLIM', 'LS', 'SOLID'))
        self.assertEqual(d['fitClass'], 'slim')
        self.assertEqual(d['group'], 'OTHER')

    def test_brand_folds(self):
        self.assertEqual(E.decode_sku('RONTQF101SLS')['brand'], 'NA')
        self.assertEqual(E.decode_sku('RODVQF101SLS')['brand'], 'VD')

    def test_categories(self):
        cases = {'ROQAQFP02SRS': 'pants', 'ROQABC101SLS': 'pants', 'ROQAQFB05DBV': 'blazer', 'ROQAQF101DBS': 'blazer',
                 'ROQAKS101SLS': 'blazer', 'ROQAQFV01SLS': 'vest', 'ROQASF101SLS': 'overshirt', 'ROQAPO101SLS': 'polo',
                 'ROQAQF101SLZ': 'polo', 'ROQAPK101SLS': 'knit_shirt', 'ROQAQF101SLP': 'dress_shirt'}
        for sku, cat in cases.items():
            self.assertEqual(E.decode_sku(sku)['cat'], cat, sku)
        self.assertIsNone(E.decode_sku('ROQAQFP02SRS')['fit'])

    def test_fit_sleeve_pattern(self):
        f = {s: E.decode_sku(s) for s in ('ROQAQF101RFS', 'ROQAQF101BTS', 'ROQAQF101MFS', 'ROQAQF101TFS', 'ROQAQF101SSP',
                                           'ROQAQF101SRY', 'ROQAYD101SLS', 'ROQAQF101SL')}
        self.assertEqual(f['ROQAQF101RFS']['fit'], 'REGULAR')
        self.assertEqual(f['ROQAQF101BTS']['fitClass'], 'bigTall')
        self.assertEqual(f['ROQAQF101MFS']['fit'], 'MODERN')
        self.assertEqual(f['ROQAQF101TFS']['fit'], 'TAILORED')
        self.assertEqual((f['ROQAQF101SSP']['fit'], f['ROQAQF101SSP']['sleeve'], f['ROQAQF101SSP']['pat']), ('SLIM', 'SS', 'PRINT'))
        self.assertEqual((f['ROQAQF101SRY']['fit'], f['ROQAQF101SRY']['sleeve'], f['ROQAQF101SRY']['pat']), ('REGULAR', 'SS', 'YARN_DYED'))
        self.assertEqual(f['ROQAYD101SLS']['pat'], 'YARN_DYED')
        self.assertIsNone(f['ROQAQF101SL']['pat'])

    def test_prefix_group_program_legacy_fiber(self):
        self.assertEqual(E.decode_sku('CLQAQF101SLS', PARAMS)['group'], 'CLUB')
        self.assertEqual(E.decode_sku('1PQAQF101SLS-L', PARAMS)['cust'], '1P')
        p = E.decode_sku('ZZKIT01', PARAMS)
        self.assertTrue(p['program'])
        self.assertEqual((p['brand'], p['fab'], p['fit']), ('QA', 'QF', 'SLIM'))
        self.assertIsNone(E.decode_sku('ZZKIT01'))            # no params: program codes do not decode
        self.assertIsNone(E.decode_sku('LEGACY-123'))
        self.assertEqual(E.base_of('CU-QAPROG2'), 'CU-QAPROG2')
        self.assertEqual(E.decode_sku('ROQACT101SLS')['fiber'], 'cotton')
        self.assertEqual(E.decode_sku('ROQALN101SLS')['fiber'], 'linen')
        self.assertEqual(E.decode_sku('ROQAQF101SLS')['fiber'], 'mmf')
        self.assertEqual(E.decode_sku('ROQAQF101SLS', dict(PARAMS, fiberByFabric={'QF': 'wool'}))['fiber'], 'wool')


class Cascade(unittest.TestCase):
    def test_l1_list_ref_style(self):
        r = ci().resolve('ROQAQF101SLS', 'AA', 'AA26001')
        self.assertEqual((r['level'], r['fobU'], r['evidence'], r['fxShare']), ('L1', 7.7777, ['AA!J2'], 0.0))

    def test_l2_ref_pattern_and_malformed(self):
        c = ci()
        self.assertEqual(c.resolve('ROQAQK105SLS', 'BB', 'BB26001')['fobU'], 3.3333)
        self.assertEqual(c.resolve('ROQAQK105SLP', 'BB', 'BB26001')['fobU'], 3.8787)
        m = c.resolve('ZZBAD', 'BB', 'BB26001')
        self.assertEqual((m['level'], m['fobU'], m['rangeLo'], m['rangeHi']), ('L2', round((3.3333 + 3.8787) / 2, 4), 3.3333, 3.8787))
        self.assertIn('malformed_sku', m['flags'])

    def test_l2_ref_pattern_fit(self):
        r = ci().resolve('ROQAQS110SLS', 'CC', 'CC26001')
        self.assertEqual((r['level'], r['fobU']), ('L2', 2.2222))

    def test_l3_siblings(self):
        c = ci()
        self.assertEqual(c.resolve('ROQAQF101SLS', 'AA', 'AA26009')['fobU'], 7.7777)       # same style, other ref
        d = c.resolve('BUQAQF102SLS', 'AA', 'AA26009')
        self.assertEqual((d['level'], d['fobU']), ('L3', 6.6666))                         # same design
        f = c.resolve('ROQAQF999SLS', 'AA', 'AA26009')
        self.assertEqual((f['level'], f['fobU']), ('L3', round((7.7777 + 6.6666) / 2, 4)))
        self.assertIn('fabric_median', f['flags'])
        self.assertEqual(c.resolve('ROQAQK105SLS', 'BB', 'BB26099', poName='RO-SYNTH')['fobU'], 3.3333)
        self.assertEqual(c.resolve('ROQAQK105SLS', 'BB', 'BB26099', poName='AM-SYNTH')['fobU'], 3.4444)
        k = c.resolve('ROQAQS111SSP', 'CC', 'CC26009')
        self.assertEqual((k['level'], k['fobU']), ('L3', 2.7222))
        self.assertIn('ss_upper_bound', k['flags'])

    def test_l4_grids(self):
        c = ci()
        a = c.resolve('ROQAQF201SLS', 'TT', 'TT26001')
        self.assertEqual((a['level'], a['fobU'], a['fxShare']), ('L4a', 4.1717, 1.0))      # TT prefers GY
        self.assertIn('price_conflict', a['flags'])
        n = c.resolve('ROQAQF201SLS', 'NN', 'NN26001')
        self.assertEqual((n['level'], n['fobU']), ('L4a', 4.4444))                         # default grid GN
        b = c.resolve('ROQAQH201SLS', 'TT', 'TT26001')
        self.assertEqual((b['level'], b['fobU']), ('L4b', 5.0505))
        o = c.resolve('ROQZQF201SLS', 'TT', 'TT26001')
        self.assertEqual((o['level'], o['fobU']), ('L4c', 4.1717))
        q = c.resolve('ROQAQU201SLS', 'TT', 'TT26001')
        self.assertEqual((q['level'], q['fobU'], q['fxShare']), ('L4a', 5.5151, 0.0))       # USD quote: no FX share
        self.assertEqual(c.resolve('ROQAPO201SLS', 'TT')['level'], 'L4a')                  # polo pools

    def test_l4d_derivations(self):
        c = ci()
        alt = c.resolve('ROQAQM201SLS', 'NN')
        self.assertEqual((alt['level'], alt['fobU']), ('L4d', 4.2424))
        ss = c.resolve('ROQAQJ301SSS', 'NN')
        self.assertEqual((ss['level'], ss['fobU']), ('L4d', round(3.9393 - 0.1111, 4)))
        fit = c.resolve('ROQAQJ401RFS', 'NN')
        self.assertEqual((fit['level'], fit['fobU']), ('L4d', round(3.9393 + 0.2222, 4)))
        bt = c.resolve('ROQAQJ402BTS', 'NN')
        self.assertEqual(bt['fobU'], round(3.9393 + 0.5555, 4))
        mf = c.resolve('ROQAQF501MFS', 'NN')
        self.assertEqual((mf['level'], mf['fobU']), ('L4d', 4.6666))
        pat = c.resolve('ROQAQJ601SLP', 'NN')
        self.assertEqual((pat['level'], pat['fobU']), ('L4d', 3.9393))
        self.assertIn('derived', pat['flags'])

    def test_exclude_flag_and_missing_pool(self):
        self.assertEqual(ci().resolve('ROQAQF201SLS', 'NN')['fobU'], 4.4444)   # the SKIP_ME row never matches
        self.assertEqual(E._pool_of({'source_code': 'NF-TJX'}), 'NF-TJX:TJX')
        self.assertEqual(E._pool_of({'source_code': 'NF-OC', 'customer_group': 'KOHL'}), 'NF-OC:KOHL')

    def test_l5_l6_l7(self):
        ledger = [led('BB26001', 'ROQAQK701SLS', 500), led('NN26001', 'ROQAQF801SLS', 100),
                  led('TT26001', 'ROQAQF802SLS', 300), led('NN26002', 'RODKQF803SLS', 200)]
        c = ci(ledger)
        l5 = c.resolve('ROQAQK701SLS', 'TT', 'TT26009')
        self.assertEqual((l5['level'], l5['fobU']), ('L5', 3.3333))
        l6 = c.resolve('ROQAQV901SLS', 'TT')
        self.assertEqual((l6['level'], l6['fobU']), ('L6', 3.3333))      # weighted median: the 500-unit L2 line
        self.assertIn('category and brand', l6['basis'])
        self.assertIn('default', l6['flags'])
        leg = c.resolve('LEGACY99', 'UNKNOWN', brand_label='DKNY')
        self.assertEqual((leg['level'], leg['fobU']), ('L6', 4.3434))
        self.assertEqual(ci().resolve('ROQAQV901SLS', 'TT')['level'], 'L7')
        self.assertIsNone(ci().resolve('ROQAQV901SLS', 'TT')['fobU'])

    def test_blank_ref_takes_the_po_names_ref(self):
        c = ci([led('BB26001', 'ROQAQK701SLS', 500, po='SYNTH PO 1'), led('', 'ROQAQK702SLS', 200, po='SYNTH PO 1')])
        self.assertEqual((c.ledger[1]['ref'], c.ledger[1]['flag'], c.ledger[1]['res']['level']), ('BB26001', 'blank_ref', 'L2'))

    def test_l0_overrides(self):
        base = {'reason': 'synthetic', 'effective': '', 'by': 'x@example.com'}
        ov = [dict(base, id='o1', scope='ref_style', key={'ref': 'AA26001', 'style': 'ROQAQF101SLS'}, fobU=9.1111, at='2026-01-01'),
              dict(base, id='o2', scope='style', key={'style': 'ROQAQF201SLS'}, fobU=9.2222, at='2026-01-01'),
              dict(base, id='o3', scope='ref', key={'ref': 'TT26001'}, fobU=9.3333, at='2026-01-01'),
              dict(base, id='o4', scope='design', key={'design': 'QAQF301SLS'}, fobU=9.4444, at='2026-01-01'),
              dict(base, id='o5', scope='fabric_category', key={'fabric': 'QH', 'category': 'dress_shirt'}, fobU=9.5555, at='2026-01-01'),
              dict(base, id='o6', scope='style', key={'style': 'ROQAQF401SLS'}, fobU=9.6666, at='2026-01-01', effective='2099-01-01'),
              dict(base, id='o7', scope='style', key={'style': 'ROQAQF201SLS'}, fobU=9.7777, at='2026-02-01')]
        c = ci(overrides=ov)
        r = c.resolve('ROQAQF101SLS', 'AA', 'AA26001')
        self.assertEqual((r['level'], r['fobU'], r['evidence'], r['fxShare']), ('L0', 9.1111, ['OVR:o1'], 0.0))
        self.assertEqual(c.resolve('ROQAQF201SLS', 'TT', 'TT26001')['fobU'], 9.7777)   # style beats ref; latest wins
        self.assertEqual(c.resolve('ROQAQF999SLS', 'TT', 'TT26001')['fobU'], 9.3333)   # ref
        self.assertEqual(c.resolve('TMQAQF301SLS', 'TT')['fobU'], 9.4444)              # design
        self.assertEqual(c.resolve('ROQAQH777SLS', 'NN')['fobU'], 9.5555)              # fabric and category
        self.assertEqual(c.resolve('ROQAQF401SLS', 'NN')['level'], 'L4a')             # not yet effective

    def test_fx_rate_and_after_cut(self):
        c = ci(settings={'fx': {'rate': 7.7}})
        self.assertEqual(c.resolve('ROQAQF201SLS', 'NN')['fobU'], round(4.4444 * FXB / 7.7, 4))
        self.assertEqual(c.resolve('ROQAQF101SLS', 'AA', 'AA26001')['fobU'], 7.7777)   # lists are USD
        self.assertEqual(c.resolve('ROQAQU201SLS', 'TT')['fobU'], 5.5151)               # USD quote
        a = ci(settings={'fx': {'basis': 'after_cut'}}).resolve('ROQAQF201SLS', 'NN')
        self.assertEqual((a['fobU'], a['fxShare'], a['fxRef']), (4.5454, 0.0, None))
        self.assertIsNone(E.merge_settings({}, PARAMS)['fx']['rate'])         # null = as printed on each sheet
        self.assertIsNone(E.merge_settings({'fx': {'rate': 'x'}}, PARAMS)['fx']['rate'])
        self.assertEqual(E.merge_settings({'fx': {'rate': 7.7}}, PARAMS)['fx']['rate'], 7.7)

    def test_grid_precedence_setting(self):
        c = ci(settings={'gridPrecedence': {'TT': ['GN', 'GY']}})
        self.assertEqual(c.resolve('ROQAQF201SLS', 'TT')['fobU'], 4.4444)


class LandedMath(unittest.TestCase):
    S = E.merge_settings({}, PARAMS)

    def test_rounding_half_up(self):
        self.assertEqual(E.r2(0.125), 0.13)
        self.assertEqual(E.r2(-0.125), -0.12)
        self.assertEqual(E.r4(1.00005), 1.0001)
        self.assertIsNone(E.r2(None))

    def test_adders_by_group(self):
        S = self.S
        self.assertEqual(E.adders(100.0, 10, 'dress_shirt', 'mmf', 'CN', S, False),
                         (E.r2(100.0 * (25.9 + 20.0) / 100 + 10 * 0.07), E.r2(10 * 0.285), E.r2(100.0 * (0.3464 + 0.125) / 100 + 10 * 0.092)))
        self.assertEqual(E.adders(100.0, 10, 'overshirt', 'cotton', 'CN', S, False)[0], E.r2(100.0 * (19.7 + 20.0) / 100))
        self.assertEqual(E.adders(100.0, 10, 'polo', 'mmf', 'BD', S, False)[:2], (E.r2(100.0 * (32.0 + 10.0) / 100), E.r2(10 * 0.211)))
        self.assertEqual(E.adders(100.0, 10, 'pants', 'cotton', 'CN', S, False)[0], E.r2(100.0 * (16.6 + 20.0) / 100))
        self.assertEqual(E.adders(100.0, 10, 'vest', 'wool', 'CN', S, False)[:2], (E.r2(100.0 * (17.5 + 20.0) / 100), E.r2(10 * 0.95)))
        self.assertEqual(E.adders(100.0, 10, 'dress_shirt', 'mmf', 'CN', S, True), (0.0, 0.0, 0.0))

    def test_fallbacks(self):
        S = self.S
        silk = E.adders(100.0, 10, 'knit_shirt', 'silk', 'CN', S, False)[0]
        self.assertEqual(silk, E.r2(100.0 * (25.9 + 20.0) / 100))                    # missing key -> shirt_mmf
        self.assertEqual(E.adders(100.0, 1, 'dress_shirt', 'cotton', None, S, False)[0], E.r2(100.0 * (19.7 + 10.0) / 100))
        self.assertEqual(E.adders(100.0, 1, 'dress_shirt', 'cotton', 'ZZ', S, False)[0], E.r2(100.0 * (19.7 + 10.0) / 100))
        self.assertEqual(E.adders(100.0, 10, 'dress_shirt', None, 'CN', S, False)[0], E.r2(100.0 * (25.9 + 20.0) / 100 + 0.7))

    def test_landed_per_unit_and_stock(self):
        S = self.S
        x = E.landed(2.0, {'cat': 'dress_shirt', 'fiber': 'mmf'}, S, False, 'CN')
        self.assertEqual(x['landedU'], E.r4(2.0 + 2.0 * 45.9 / 100 + 0.07 + 0.285 + 2.0 * 0.4714 / 100 + 0.092))
        self.assertEqual(E.landed(2.0, {'cat': 'polo'}, S, True, 'CN')['landedU'], 2.0)
        lnd, lu = E.stock_money(100.0, 10, 'dress_shirt', 'mmf', 'CN', False, S)
        d, f, e = E.adders(100.0, 10, 'dress_shirt', 'mmf', 'CN', S, False)
        self.assertEqual((lnd, lu), (E.r2(100.0 + d + f + e), E.r4(E.r2(100.0 + d + f + e) / 10)))

    def test_line_money(self):
        S = self.S
        m = E.line_money(1000.0, 400.0, 100, 'dress_shirt', 'mmf', 'CN', False, 7.0, 10.0, S)
        d, f, e = E.adders(400.0, 100, 'dress_shirt', 'mmf', 'CN', S, False)
        cogs = E.r2(400.0 + d + f + e)
        ded = E.r2(1000.0 * 7.0 / 100)
        net = E.r2(1000.0 - ded)
        gp = E.r2(net - cogs)
        roy = E.r2(net * 10.0 / 100)
        self.assertEqual((m['cogs'], m['deduct'], m['net'], m['gp'], m['royalty'], m['contrib']),
                         (cogs, ded, net, gp, roy, E.r2(gp - roy)))
        u = E.line_money(1000.0, None, 100, 'dress_shirt', 'mmf', 'CN', False, 7.0, 10.0, S)
        self.assertEqual((u['cogs'], u['gp'], u['net']), (None, None, net))
        n = E.line_money(None, 400.0, 100, 'dress_shirt', 'mmf', 'CN', False, 7.0, 10.0, S)
        self.assertEqual((n['net'], n['gp'], n['cogs']), (None, None, cogs))

    def test_deductions_and_royalty(self):
        S = E.merge_settings({'deductions': {'byCustomer': {'ROSS': 3.0}}, 'customerGroups': {'ZZC1': 'club'}})
        fob = frozenset({'FOBX'})
        self.assertEqual(E._ded_pct('ROSS', S, E._cust_group('ROSS', S, fob)), 3.0)        # byCustomer wins
        self.assertEqual(E._ded_pct('ZZC1', S, E._cust_group('ZZC1', S, fob)), 2.5)        # group club
        self.assertEqual(E._cust_group('FOBX', S, fob), 'fob')
        self.assertEqual(E._ded_pct('ZZZ9', S, E._cust_group('ZZZ9', S, fob)), 2.0)        # other
        self.assertEqual(E._roy_pct('NA', S), 10.0)
        self.assertEqual(E._roy_pct('VS', S), 0.0)
        self.assertEqual(E._roy_pct('QQ', S), 10.0)
        self.assertEqual(E._roy_pct(None, S), 10.0)

    def test_grades(self):
        cases = [('L1', 'R1', 'A'), ('L0', None, 'A'), ('L1', 'R2', 'B'), ('L4a', 'R1', 'B'), ('L3', 'R4', 'B'),
                 ('L4d', 'R3', 'C'), ('L5', 'R1', 'C'), ('L2', 'R5', 'C'), ('L2', 'R6', 'D'), ('L7', 'R1', 'D')]
        for lv, rt, g in cases:
            self.assertEqual(E.grade_of(lv, rt), g, (lv, rt))


# ── dataset ──
TODAY = '2026-03-02'


def inv(sku, tr=0, committed=0, allocated=0, incoming=0, brand='SYN', lot='N/A'):
    return {'sku': sku, 'brand': brand, 'jtw': 0, 'tr': tr, 'dcw': 0, 'qa': 0, 'nj': 0, 'abfi': 0, 'incoming': incoming,
            'committed': committed, 'allocated': allocated, 'total_ats': tr + incoming + committed + allocated,
            'lot_number': lot, 'receive_date': '01-15-2026', 'total_warehouse': tr}


def order(ctrl, sku, qty, price, cust='ROSS', start='2026-05-01', cancel=None, wh='TR', bulk=False):
    return {'ctrlNo': ctrl, 'orderNo': 'PO' + ctrl, 'style': sku, 'customer': cust, 'customerFull': 'Customer ' + cust,
            'openQty': qty, 'pickQty': 0, 'salesPrice': price, 'openValue': round(qty * price, 2), 'pickValue': 0.0,
            'startDate': start + 'T00:00:00', 'cancelDate': (cancel or start) + 'T00:00:00', 'wh': wh,
            'reportType': 'bulk' if bulk else 'a2000', 'isPipeline': bulk}


def src(orders_ok=True, analytics=True):
    items = [inv('ROQAQF201SLS', tr=500, committed=-400, allocated=-100, incoming=1000),
             inv('ROQAQF203SLS', tr=50, committed=-50), inv('ROQAQH204SLS', incoming=600, committed=-600),
             inv('ROQAQF205SLS', incoming=300)]
    ledger = [led('TT26001', 'ROQAQF201SLS', 1000, etd='2026-03-20'),
              led('TT26002', 'ROQAQH204SLS', 300, etd='2026-03-15'), led('TT26002', 'ROQAQF205SLS', 150),
              led('NN26002', 'ROQAQH204SLS', 300, etd='2026-03-15'), led('NN26002', 'ROQAQF205SLS', 150)]
    orders = [order('1', 'ROQAQF201SLS', 300, 9.99), order('2', 'ROQAQF201SLS', 100, 1.00, cust='KOHL'),
              order('3', 'ROQAQF203SLS', 50, 12.5, cust='FOBX', wh='CH'),
              order('4', 'ROQAQH204SLS', 600, 11.0, start='2026-02-01', cancel='2026-02-20'),
              order('5', 'CUQZZZ26', 1000, 15.0, cust='COST', bulk=True),
              order('6', 'ZZKIT01', 10, 99.0, cust='BJS', bulk=True)]
    apo = [{'style': 'ROQAQF201SLS', 'qty': 100, 'customer': 'ROSS', 'po': 'SYNTH ALLOC'},
           {'style': 'ROQAQF205SLS', 'qty': 40, 'customer': 'SOMEONE NEW', 'po': 'SYNTH ALLOC 2'}]
    sa = {'fields': ['style', 'brand', 'qty', 'value', 'firstInv', 'lastInv', 'months', 'customers', 'color'],
          'source': {'from': '2025-01-01', 'to': '2026-02-20', 'ingestedAt': '2026-02-21T00:00:00'}, 'v': 2,
          'styles': [['ROQAQF201SLS', 'SYN', 50, 500.0, '2025-01-05', '2026-02-10',
                      {'2026-01': [30, 300.0, 0, 0.0], '2026-02': [20, 200.0, 0, 0.0], '2024-01': [5, 50.0, 0, 0.0]},
                      {'ROSS': [40, 400.0], 'FOBX': [10, 100.0]}, 'BLUE']]} if analytics else {'building': True}
    return {'inventory': {'items': items, 'last_sync': '2026-03-01T12:00:00Z'},
            'ledger': {'rows': ledger, 'last_sync': '2026-03-02T10:00:00Z'},
            'apo': {'rows': apo, 'last_sync': '2026-03-02T09:00:00Z'},
            'manual_allocations': [], 'vw_allocations': [],
            'open_orders': {'orders': orders, 'ok': orders_ok, 'fetched_at': '2026-03-02T11:00:00Z'},
            'fob_customers': ['FOBX'], 'routing_inputs': {'now': '2026-03-02T08:00:00'}, 'sales_analytics': sa,
            'today': TODAY}


def rows(ds, t):
    return [dict(zip(ds[t]['fields'], r)) for r in ds[t]['rows']]


def client_recalc(r, S, dct, table):
    """Python copy of the frontend's DESIGN 5.6 recompute (pnl.core.js _pnlRecalc, scen null), with
    the duty regime read from the row (contract C2)."""
    kit = 'kit' in (r.get('flags') or [])
    qty = round(r['fob'] / r['fobU']) if kit and r['fobU'] and r['fob'] else r['units']
    d, f, e = E.adders(r['fob'], qty, r['cat'], r['fiber'], r['origin'], S, r['dutyRegime'])
    cogs = E.r2(r['fob'] + d + f + e)
    grp = dct['customers'][r['cust']]['group']
    ded = E.r2(r['rev'] * E._ded_pct(r['cust'], S, grp) / 100)
    net = E.r2(r['rev'] - ded)
    gp = E.r2(net - cogs)
    roy = E.r2(net * E._roy_pct(r['brand'], S) / 100)
    return {'duty': d, 'freight': f, 'fees': e, 'cogs': cogs, 'deduct': ded, 'net': net, 'gp': gp, 'royalty': roy,
            'contrib': E.r2(gp - roy)}


def client_recalc_fx(r, S, dct, rate):
    """Python copy of the page's RMB what-if (pnl.core.js _pnlRecalc with scen {fxRate}): the unit cost
    first, fobU' = r4(fobU x (1 - fxShare + fxShare x fxRef / rate)), then fob' = r2(qty x fobU') and
    DESIGN 5.6. Rows without fxRef scale from settings.fx.rate."""
    kit = 'kit' in (r.get('flags') or [])
    qty = math.floor(r['fob'] / r['fobU'] + 0.5) if kit and r['fobU'] and r['fob'] else r['units']
    u = E.fx_what_if_unit(r['fobU'], r['fxShare'], r.get('fxRef'), rate, S['fx'].get('rate'))
    fob = E.r2(qty * u) if (u != r['fobU'] and qty > 0) else r['fob']
    grp = dct['customers'][r['cust']]['group']
    m = E.line_money(r['rev'], fob, qty, r['cat'], r['fiber'], r['origin'], r['dutyRegime'],
                     E._ded_pct(r['cust'], S, grp), E._roy_pct(r['brand'], S), S)
    return dict(m, fobU=u, fob=fob, qty=qty)


class Dataset(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.ds = E.build_dataset(src(), COSTBOOK, {}, [], '2026-03-02T12:00:00Z', R)

    def test_shape(self):
        ds = self.ds
        for k in ('v', 'builtAt', 'asOf', 'inputs', 'settings', 'dict', 'lines', 'alloc', 'apo', 'inventory', 'production',
                  'styles', 'shipped', 'coverage', 'unresolved', 'conflicts', 'alerts', 'evidence', 'totals', 'notes'):
            self.assertIn(k, ds)
        design = {
            'lines': 'id ctrlNo orderNo cust type style base brand cat fab fiber units price rev start cancel late fobLine wh fobU fob duty freight fees cogs deduct net gp royalty contrib level grade routing factory ref origin fxShare ev flags',
            'alloc': 'line units kind factory ref poName landing etd arrival fobU level routing forced',
            'apo': 'id cust custName po style base brand cat fiber units estPrice priceBasis rev fobU fob duty freight fees cogs deduct net gp royalty contrib level grade factory origin fxShare flags',
            'inventory': 'sku base brand cat fiber wh units fobU landedU fob landed level grade factory ref lotTier receiveDate ageDays origin fxShare',
            'production': 'ref factory poName style base brand cat fiber units etd arrival landing fobLanding fobU fob landedU landed level grade claimed free origin fxShare flags',
            'styles': 'base brand cat fab fiber fit sleeve pat fobU landedU level grade rangeLo rangeHi factories onHand onHandFob onHandLanded incoming incomingFob ats committed allocated openUnits openRev openGp openContrib apoUnits apoRev t12Units t12Rev t12Cogs t12Gp lifeUnits lifeRev expPrice atsPotentialGp ladder'}
        for t, f in design.items():
            want = f.split()
            self.assertEqual(ds[t]['fields'][:len(want)], want, t)           # contract fields first, in order
            for r in ds[t]['rows']:
                self.assertEqual(len(r), len(ds[t]['fields']))
        for k in ('months', 'company', 'byStyle', 'note'):
            self.assertIn(k, ds['shipped'])
        for t in ('openBook', 'bulk', 'apo', 'inventory', 'production'):
            self.assertIn(t, ds['coverage'])
            self.assertIn(t, ds['coverage']['grades'])
            self.assertIn(t, ds['totals'])
        for k in ('customers', 'brands', 'factories', 'warehouses', 'levels', 'grades', 'routing', 'categories'):
            self.assertIn(k, ds['dict'])
        json.dumps(ds, allow_nan=False)

    def test_rounding(self):
        money = {'rev', 'fob', 'duty', 'freight', 'fees', 'cogs', 'deduct', 'net', 'gp', 'royalty', 'contrib', 'landed'}
        unit = {'fobU', 'landedU', 'fxShare', 'fxRef', 'price', 'estPrice'}
        for t in ('lines', 'apo', 'inventory', 'production', 'alloc'):
            for r in rows(self.ds, t):
                for k, v in r.items():
                    if isinstance(v, float) and k in money:
                        self.assertAlmostEqual(v * 100, round(v * 100), places=6, msg=(t, k))
                    if isinstance(v, float) and k in unit:
                        self.assertAlmostEqual(v * 10000, round(v * 10000), places=5, msg=(t, k))

    def test_lines_and_alloc(self):
        L = {r['ctrlNo']: r for r in rows(self.ds, 'lines')}
        self.assertEqual(len(L), 6)
        self.assertEqual(L['1']['id'], '1|ROQAQF201SLS')
        self.assertEqual(L['1']['rev'], 2997.0)
        self.assertEqual(L['3']['fobLine'], 1)
        self.assertEqual((L['3']['duty'], L['3']['freight'], L['3']['fees']), (0.0, 0.0, 0.0))
        self.assertEqual(L['4']['late'], 1)
        self.assertEqual(L['5']['type'], 'bulk')
        self.assertEqual((L['5']['level'], L['5']['grade'], L['5']['routing'], L['5']['fob']), ('L7', 'D', 'R6', None))
        k = L['6']
        self.assertIn('kit', k['flags'])
        self.assertEqual(k['fob'], E.r2(10 * 12 * k['fobU']))
        self.assertEqual(k['freight'], E.r2(10 * 12 * 0.285))
        ids = {r['id'] for r in rows(self.ds, 'lines')}
        by_line = {}
        for a in rows(self.ds, 'alloc'):
            self.assertIn(a['line'], ids)
            by_line[a['line']] = by_line.get(a['line'], 0) + a['units']
        for r in rows(self.ds, 'lines'):
            self.assertEqual(by_line.get(r['id'], 0), r['units'])            # every unit is attributed

    def test_client_math_matches(self):
        ds = self.ds
        for t in ('lines', 'apo'):
            for r in rows(ds, t):
                if r['fob'] is None or r['rev'] is None:
                    continue
                c = client_recalc(r, ds['settings'], ds['dict'], t)
                for k, v in c.items():
                    self.assertEqual(r[k], v, (t, r.get('id'), k))
        for t in ('inventory', 'production'):
            for r in rows(ds, t):
                if r['fob'] is None:
                    continue
                lnd, lu = E.stock_money(r['fob'], r['units'], r['cat'], r['fiber'], r['origin'], r['dutyRegime'],
                                        ds['settings'])
                self.assertEqual((r['landed'], r['landedU']), (lnd, lu))

    def test_alerts(self):
        kinds = {a['kind']: a for a in self.ds['alerts']}
        for k in ('below_cost', 'bulk_needs_cost', 'double_booking', 'late', 'assumption'):
            self.assertIn(k, kinds)
        self.assertIn('2|ROQAQF201SLS', kinds['below_cost']['refs']['lines'])
        self.assertEqual(set(kinds['double_booking']['refs']['refs']), {'TT26002', 'NN26002'})
        # C4: double booking counts the refs it lists; the pair count rides along.
        self.assertEqual((kinds['double_booking']['count'], kinds['double_booking']['unit'], kinds['double_booking']['pairs']),
                         (2, 'refs', 1))
        # D5: the only uncosted line is bulk forecast, so it is an info alert of its own, not needs_cost.
        self.assertNotIn('needs_cost', kinds)
        self.assertEqual((kinds['bulk_needs_cost']['severity'], kinds['bulk_needs_cost']['refs']['lines']),
                         ('info', ['5|CUQZZZ26']))
        inc = E.build_dataset(src(), COSTBOOK, {'bulk': {'includeInTotals': True}}, [], '2026-03-02T12:00:00Z', R)
        self.assertEqual({a['kind']: a for a in inc['alerts']}['bulk_needs_cost']['severity'], 'high')
        stale = E.build_dataset(src(orders_ok=False), COSTBOOK, {}, [], '2026-03-02T12:00:00Z', R)
        self.assertIn('al_stale_orders', {a['id'] for a in stale['alerts']})
        self.assertIn('al_stale_ats', {a['id'] for a in self.ds['alerts']})   # ATS synced over 6 hours before the build
        for a in self.ds['alerts']:
            self.assertNotRegex(a['title'] + a['detail'], '[\u2013\u2014]')

    def test_evidence_and_coverage(self):
        ds = self.ds
        used = set()
        for t in ('lines', 'apo', 'inventory', 'production', 'styles'):
            for r in rows(ds, t):
                used.update(r['ev'])
        self.assertTrue(used.issubset(set(ds['evidence'])))
        self.assertNotIn('GN!F5', ds['evidence'])
        ob = ds['totals']['openBook']
        self.assertEqual(sum(v[0] for v in ds['coverage']['openBook'].values()), ob['units'])
        self.assertAlmostEqual(sum(v[1] for v in ds['coverage']['openBook'].values()), ob['rev'], places=1)
        self.assertEqual(ds['totals']['bulk']['lines'], 2)
        self.assertFalse(ds['totals']['openBook']['includesBulk'])

    def test_apo_and_shipped(self):
        A = rows(self.ds, 'apo')
        self.assertEqual(A[0]['priceBasis'], 'customer_brand')
        self.assertEqual(A[0]['estPrice'], round((300 * 9.99 + 600 * 11.0) / 900, 4))   # ROSS x brand QA, both lines
        self.assertEqual(A[1]['cust'], 'SOMEONE NEW')
        self.assertIn('SOMEONE NEW', self.ds['dict']['customers'])
        sh = self.ds['shipped']
        self.assertEqual(len(sh['months']), 12)
        self.assertEqual(sh['months'][-1], '2026-02')
        by = rows({'b': sh['byStyle']}, 'b')
        self.assertEqual(by[0]['months'], {'2026-01': [30, 300.0], '2026-02': [20, 200.0]})
        self.assertEqual(by[0]['fobShare'], 0.2)
        comp = rows({'c': sh['company']}, 'c')
        self.assertEqual(sum(c['units'] for c in comp), 50)
        b2 = E.build_dataset(src(analytics=False), COSTBOOK, {}, [], '2026-03-02T12:00:00Z', R)
        self.assertEqual((b2['shipped']['state'], b2['shipped']['months']), ('building', []))

    def test_bulk_in_totals_and_determinism(self):
        inc = E.build_dataset(src(), COSTBOOK, {'bulk': {'includeInTotals': True}}, [], '2026-03-02T12:00:00Z', R)
        self.assertEqual(inc['totals']['openBook']['lines'], 6)
        again = E.build_dataset(src(), COSTBOOK, {}, [], '2026-03-02T12:00:00Z', R)
        self.assertEqual(json.dumps(again, sort_keys=True), json.dumps(self.ds, sort_keys=True))

    def test_inventory_lot_tiers_and_production(self):
        I = {r['sku']: r for r in rows(self.ds, 'inventory')}
        self.assertEqual(I['ROQAQF201SLS']['lotTier'], 'T3')
        self.assertEqual(I['ROQAQF201SLS']['ref'], 'TT26001')
        self.assertEqual(I['ROQAQF203SLS']['lotTier'], 'T5')
        P = rows(self.ds, 'production')
        self.assertTrue(all('double_booking' in r['flags'] for r in P if r['ref'] in ('TT26002', 'NN26002')))
        self.assertEqual(sum(r['claimed'] + r['free'] for r in P), sum(r['units'] for r in P))

    def test_settings_defaults_are_public_only(self):
        d = E.DEFAULT_SETTINGS
        self.assertIsNone(d['fx']['rate'])                   # the rate comes from the cost book
        self.assertEqual(set(d['confirmed']), {'fx', 'grid', 'tariff', 'freight', 'royalty', 'deductions', 'opex'})
        self.assertFalse(any(d['confirmed'].values()))
        self.assertEqual(d['routing'], {'picksAsWarehouse': False, 'honorAssignments': True, 'gateFallback': 'fifo'})

    def test_gate_fallback_setting_reaches_routing(self):
        self.assertEqual(self.ds['inputs']['routing']['options']['gateFallback'], 'fifo')
        eng = E.build_dataset(src(), COSTBOOK, {'routing': {'gateFallback': 'engine'}}, [], '2026-03-02T12:00:00Z', R)
        self.assertEqual(eng['inputs']['routing']['options']['gateFallback'], 'engine')
        odd = E.build_dataset(src(), COSTBOOK, {'routing': {'gateFallback': 'bogus'}}, [], '2026-03-02T12:00:00Z', R)
        self.assertEqual(odd['inputs']['routing']['options']['gateFallback'], 'fifo')


# ── review fixes: contracts C1 to C8 and the numbers findings (synthetic data only) ──
NOW = '2026-03-02T12:00:00Z'


def build(s=None, cb=None, settings=None, overrides=None):
    return E.build_dataset(s or src(), cb or COSTBOOK, settings or {}, overrides or [], NOW, R)


def by(ds, t, key):
    return {r[key]: r for r in rows(ds, t)}


def kinds_of(ds):
    return {a['kind']: a for a in ds['alerts']}


class PriceRolesC1(unittest.TestCase):
    """Cost-book price fields are found by role, never by a name spelled in the engine. The usd role
    is the price used (contract C10); the base role and the RMB amount only help find a sheet rate
    that a record does not carry; the cut role is the after-cut reading."""
    PRINTED, RATE = 3.4343, R2                # GN!F1: printed on a sheet at R2, restated at FXB below
    BASE = round(PRINTED * RATE / FXB, 6)

    def book(self, base_name, cut_name, meta, e1_name=None, rmb_name=None):
        recs = []
        for r in copy.deepcopy(RECORDS):
            p, c = r.pop(F_BASE), r.pop(F_CUT)
            e = r.pop(F_E1)
            if r['id'] == 'GN!F1':
                r['price_usd'], p, e = self.PRINTED, self.BASE, self.RATE
                if rmb_name:
                    r[rmb_name] = self.PRINTED * self.RATE
            if base_name:
                r[base_name] = p
            if cut_name:
                r[cut_name] = c
            if e1_name:
                r[e1_name] = e
            recs.append(r)
        cb = {k: v for k, v in COSTBOOK.items() if k != 'meta'}
        cb['records'] = recs
        if meta is not None:
            cb['meta'] = meta
        return cb

    def check(self, cb, used=PRINTED, cut=4.5454, ref=RATE):
        r = ci(costbook=cb).resolve('ROQAQF201SLS', 'NN')
        self.assertEqual((r['fobU'], r['fxRef']), (used, ref))
        self.assertEqual(ci(costbook=cb, settings={'fx': {'basis': 'after_cut'}}).resolve('ROQAQF201SLS', 'NN')['fobU'], cut)
        at = ci(costbook=cb, settings={'fx': {'rate': FXB}}).resolve('ROQAQF201SLS', 'NN')
        self.assertEqual(at['fobU'], E.r4(used * ref / FXB) if ref else used)   # the RMB amount at the saved rate

    def test_roles_from_meta(self):
        roles = {'usd': 'price_usd', 'base': 'price_usd_alpha', 'cut': 'price_usd_beta', 'rmb': 'rmb_price',
                 'e1': 'sheet_rate_gamma'}
        self.check(self.book('price_usd_alpha', 'price_usd_beta', {'priceFieldRoles': roles}, e1_name='sheet_rate_gamma'))

    def test_roles_from_price_fields_order(self):
        pf = {'price_usd': {}, 'price_usd_alpha': {}, 'price_usd_beta': {}, 'rmb_price': {}}
        self.check(self.book('price_usd_alpha', 'price_usd_beta', {'priceFields': pf}, e1_name=F_E1))
        self.check(self.book('price_usd_alpha', 'price_usd_beta', {'priceFields': list(pf)}, e1_name=F_E1))

    def test_no_meta_uses_role_neutral_names_then_price_usd(self):
        self.check(self.book(F_BASE, F_CUT, None, e1_name=F_E1))
        # a study table: price_usd only, so no sheet rate can be found and the price never moves
        self.check(self.book(None, None, None), used=self.PRINTED, cut=self.PRINTED, ref=None)

    def test_sheet_rate_fallbacks(self):
        # no sheet-rate field: the rate the base-role price was restated from (FXB x base / usd) ...
        self.check(self.book(F_BASE, F_CUT, None))
        # ... else the RMB amount over the printed price
        self.check(self.book(None, F_CUT, None, rmb_name='rmb_price'))
        c = ci(costbook=self.book(F_BASE, F_CUT, None, e1_name=F_E1))
        self.assertEqual((dict(c.sheet_rates), c.unrated), ({FXB: 8, R2: 1}, 0))

    def test_engine_source_spells_no_rate_bearing_field(self):
        with open(E.__file__, encoding='utf-8') as f:
            txt = f.read()
        self.assertNotRegex(txt, r'price_usd_\w*\d')
        self.assertNotRegex(txt, r'(?i)fx6|after_cut_at')


class ParamsBackstopF2(unittest.TestCase):
    """A stored or restored cost book with a bad param never fails the build; the alert names it."""
    BAD = [('gridPrecedence', 5), ('gridPrecedence', 'abc'), ('kitPcs', [1, 2]), ('programMap', 'x'),
           ('programMap', {'ZZKIT01': {'fab': ['QF']}}), ('customerGroupPrefix', ['a']), ('customerGroupPrefix', 'x'),
           ('customerGroupPrefix', {'CL': ['CLUB']}), ('poolOrder', 5), ('poolOrder', {'all': 5}),
           ('fitPremium', {'regular': 500, 'bigTall': 0.5555}), ('fitPremium', 'x'), ('ssDelta', 10 ** 400),
           ('fxBase', 'abc'), ('fxBase', 0), ('fxBase', -3), ('fiberByStyle', ['x']), ('kitPcs', {'ZZKIT01': -36})]

    def test_bad_params_never_fail_the_build(self):
        for key, val in self.BAD:
            with self.subTest(key=key, val=repr(val)[:24]):
                ds = build(cb=dict(COSTBOOK, params=dict(PARAMS, **{key: val})))
                al = {a['id']: a for a in ds['alerts']}['al_costbook_params']
                self.assertEqual((al['severity'], al['title'], al['unit']),
                                 ('high', 'Cost book has missing or invalid parameters', 'settings'))
                self.assertIn(key, al['refs']['params'])
                self.assertIn(key, al['invalid'])
                self.assertNotIn(key, al['detail'])                 # plain names only, never raw keys
                json.dumps(ds, allow_nan=False)

    def test_params_not_an_object(self):
        ds = build(cb=dict(COSTBOOK, params=[1, 2]))
        al = {a['id']: a for a in ds['alerts']}['al_costbook_params']
        self.assertIn('params', al['invalid'])
        self.assertIn('gridPrecedence', al['missing'])

    def test_overflow_is_not_a_crash(self):
        self.assertIsNone(E._fnum(10 ** 400))
        self.assertEqual(E._num(10 ** 400), 0.0)
        self.assertEqual(E._num(10 ** 400, 5.5555), 5.5555)
        clean, missing, invalid = E.clean_params(dict(PARAMS, ssDelta=10 ** 400))
        self.assertEqual((clean['ssDelta'], missing, invalid), (None, [], ['ssDelta']))

    def test_bad_base_rate_with_a_saved_rate_is_named(self):
        # A calculator price with no sheet rate, and no base rate to find it from, cannot be repriced.
        recs = copy.deepcopy(RECORDS)
        for r in recs:
            if r['id'] == 'GN!F1':
                r[F_E1] = None
        ds = build(cb=dict(COSTBOOK, params=dict(PARAMS, fxBase=0), records=recs), settings={'fx': {'rate': 7.7}})
        al = {a['id']: a for a in ds['alerts']}['al_costbook_params']
        self.assertIn('saved RMB rate', al['detail'])
        self.assertIn('1 calculator price carries no sheet rate', al['detail'])
        self.assertIn('1 calculator price carries no sheet rate. It stays as printed.', ds['notes'])
        self.assertEqual(ds['inputs']['fx']['unrated'], 1)
        r = ci(costbook=dict(COSTBOOK, params=dict(PARAMS, fxBase=0), records=recs), settings={'fx': {'rate': 7.7}})
        self.assertEqual(r.resolve('ROQAQF201SLS', 'NN')['fobU'], 4.4444)      # stays as printed
        self.assertNotRegex(al['detail'], '[–—]')

    def test_unreadable_record_is_skipped(self):
        bad = rec('BAD!Z1', 'SYN-GN', 'calculator', 4.4444, pool='GN:BASE', pattern=['SOLID'])
        ds = build(cb=dict(COSTBOOK, records=RECORDS + [bad]))
        al = {a['id']: a for a in ds['alerts']}['al_costbook_params']
        self.assertEqual((al['title'], al['badRecords'], ds['inputs']['costbookBadRecords']),
                         ('Cost book has unreadable price rows', 1, 1))

    def test_good_params_raise_nothing(self):
        self.assertNotIn('al_costbook_params', {a['id'] for a in build()['alerts']})


def src_dest():
    """src() plus a Canada line (wh AE), a factory-direct Peerless line and allocation, ABFI stock,
    and Walmart-import and Canada landings on the ledger."""
    s = src()
    it = inv('ROQAQF211SLS')
    it.update(abfi=120, total_ats=120, total_warehouse=120)
    s['inventory']['items'] = s['inventory']['items'] + [it]
    s['ledger']['rows'] = s['ledger']['rows'] + [led('TT26011', 'ROQAQF212SLS', 400, po='WA SYNTH PO', landing='WALM'),
                                                 led('TT26012', 'ROQAQF213SLS', 300, po='WN SYNTH PO', landing='CAN')]
    s['open_orders']['orders'] = s['open_orders']['orders'] + [order('8', 'ROQAQF211SLS', 60, 9.0, cust='ZZCA', wh='AE'),
                                                               order('9', 'ROQAQF201SLS', 20, 8.8888, cust='PEER', wh='CH')]
    s['apo']['rows'] = s['apo']['rows'] + [{'style': 'ROQAQF201SLS', 'qty': 12, 'customer': 'WALMART-PEERLESS',
                                            'po': 'SYNTH PEER'}]
    return s


class RegimesC2(unittest.TestCase):
    S = E.merge_settings({}, PARAMS)

    def test_adders_and_landed_by_regime(self):
        S = self.S
        self.assertEqual(E.adders(100.0, 10, 'dress_shirt', 'mmf', 'CN', S, 'ca'),
                         (E.r2(100.0 * 18.0 / 100), E.r2(10 * 0.285), E.r2(10 * 0.092)))
        self.assertEqual(E.adders(100.0, 10, 'dress_shirt', 'mmf', 'CN', S, 'none'), (0.0, 0.0, 0.0))
        self.assertEqual(E.adders(100.0, 10, 'dress_shirt', 'mmf', 'CN', S, 'us'),
                         E.adders(100.0, 10, 'dress_shirt', 'mmf', 'CN', S, False))
        self.assertEqual(E.adders(100.0, 10, 'dress_shirt', 'mmf', 'CN', S, True), (0.0, 0.0, 0.0))
        x = E.landed(2.0, {'cat': 'polo', 'fiber': 'cotton'}, S, 'ca', 'CN')
        self.assertEqual(x['landedU'], E.r4(2.0 + 2.0 * 18.0 / 100 + 0.211 + 0.068))
        self.assertEqual(E.landed(2.0, {'cat': 'polo'}, S, 'none', 'CN')['landedU'], 2.0)
        S2 = E.merge_settings({'tariff': {'caMfnPct': 12.5}}, PARAMS)
        self.assertEqual(E.adders(100.0, 1, 'pants', 'cotton', 'CN', S2, 'ca')[0], 12.5)

    def test_rows_carry_their_regime(self):
        ds = build(src_dest())
        L = by(ds, 'lines', 'id')
        ca = L['8|ROQAQF211SLS']
        self.assertEqual((ca['dutyRegime'], ca['fobLine']), ('ca', 0))
        self.assertIn('non_us_dest', ca['flags'])
        self.assertEqual((ca['duty'], ca['freight'], ca['fees']),
                         (E.r2(ca['fob'] * 18.0 / 100), E.r2(ca['units'] * 0.285), E.r2(ca['units'] * 0.092)))
        ch = L['9|ROQAQF201SLS']
        self.assertEqual((ch['dutyRegime'], ch['fobLine'], ch['duty'], ch['freight'], ch['fees']), ('none', 1, 0.0, 0.0, 0.0))
        self.assertIn('fob_line', ch['flags'])
        self.assertNotIn('non_us_dest', ch['flags'])
        fx = L['3|ROQAQF203SLS']                                  # FOB customer
        self.assertEqual((fx['dutyRegime'], fx['fobLine']), ('none', 1))
        self.assertEqual(L['1|ROQAQF201SLS']['dutyRegime'], 'us')
        peer = [r for r in rows(ds, 'apo') if r['cust'] == 'PEER'][0]
        self.assertEqual((peer['dutyRegime'], peer['duty']), ('none', 0.0))
        self.assertTrue({'non_us_dest', 'regime_from_orders'} <= set(peer['flags']))
        ab = [r for r in rows(ds, 'inventory') if r['sku'] == 'ROQAQF211SLS' and r['wh'] == 'ABFI'][0]
        self.assertEqual(ab['dutyRegime'], 'ca')
        self.assertIn('non_us_dest', ab['flags'])
        self.assertEqual((ab['landed'], ab['landedU']),
                         E.stock_money(ab['fob'], ab['units'], ab['cat'], ab['fiber'], ab['origin'], 'ca', ds['settings']))
        P = by(ds, 'production', 'ref')
        self.assertEqual((P['TT26011']['dutyRegime'], P['TT26011']['landed'], P['TT26011']['fobLanding']),
                         ('none', P['TT26011']['fob'], 0))
        self.assertIn('non_us_dest', P['TT26011']['flags'])
        self.assertEqual(P['TT26012']['dutyRegime'], 'ca')
        al = kinds_of(ds)['non_us_dest']
        self.assertEqual((al['severity'], al['unit']), ('info', 'styles'))
        self.assertIn('ROQAQF211SLS', al['refs']['styles'])
        for t in ('lines', 'apo'):                                # the client recompute still matches
            for r in rows(ds, t):
                if r['fob'] is None or r['rev'] is None:
                    continue
                for k, v in client_recalc(r, ds['settings'], ds['dict'], t).items():
                    self.assertEqual(r[k], v, (t, r.get('id'), k))

    def test_destinations_setting(self):
        us = by(build(src_dest(), settings={'destinations': {'AE': 'us'}}), 'lines', 'id')['8|ROQAQF211SLS']
        self.assertEqual(us['dutyRegime'], 'us')
        self.assertNotIn('non_us_dest', us['flags'])
        back = by(build(src_dest(), settings={'destinations': {'AE': None}}), 'lines', 'id')['8|ROQAQF211SLS']
        self.assertEqual(back['dutyRegime'], 'ca')
        conf = build(src_dest(), settings={'confirmed': {'tariff': True}})
        self.assertNotIn('non_us_dest', kinds_of(conf))

    def test_rounding_fob_is_units_times_rounded_unit_cost(self):
        ds = build(src_dest())
        for t in ('lines', 'apo', 'inventory', 'production'):
            for r in rows(ds, t):
                q = r.get('pieces') if t == 'lines' else r['units']
                if r['fob'] is not None and r['fobU'] is not None and q > 0:
                    self.assertEqual(r['fob'], E.r2(q * r['fobU']), (t, r.get('id') or r.get('sku') or r.get('ref')))


def src_lots():
    s = src()
    s['ledger']['rows'] = s['ledger']['rows'] + [
        led('AA26001', 'ROQAQF101SLS', 50, po='SYNTH AA PO 1', etd='2026-04-10'),
        led('AA26002', 'TMQAQF102SLS', 500, po='SYNTH AA PO 2', etd='2026-01-05')]
    # The feed shows the order's 100 units as committed and the ledger's 50 as incoming, so the routing
    # gate passes and the line routes cleanly.
    s['inventory']['items'] = s['inventory']['items'] + [inv('ROQAQF101SLS', tr=300, incoming=50, committed=-100),
                                                         inv('TMQAQF102SLS', tr=40, lot='SYNTH AA PO 2')]
    s['open_orders']['orders'] = s['open_orders']['orders'] + [order('10', 'ROQAQF101SLS', 100, 12.0)]
    return s


class GradesC5(unittest.TestCase):
    def test_style_level_stock_is_capped_at_b(self):
        ds = build(src_lots())
        inv_rows = {r['sku']: r for r in rows(ds, 'inventory')}
        t3 = inv_rows['ROQAQF101SLS']
        self.assertEqual((t3['lotTier'], t3['level'], t3['grade'], t3['ref']), ('T3', 'L1', 'B', 'AA26001'))
        self.assertTrue({'blended_lot', 'ref_after_stock'} <= set(t3['flags']))
        t1 = inv_rows['TMQAQF102SLS']
        self.assertEqual((t1['lotTier'], t1['level'], t1['grade']), ('T1', 'L1', 'A'))

    def test_warehouse_supply_names_its_cost_ref_not_a_lot(self):
        ds = build(src_lots())
        line = by(ds, 'lines', 'id')['10|ROQAQF101SLS']
        wh = [a for a in rows(ds, 'alloc') if a['line'] == '10|ROQAQF101SLS' and a['kind'] == 'warehouse']
        self.assertTrue(wh)
        self.assertEqual((wh[0]['ref'], wh[0]['costRef'], wh[0]['lotTier']), (None, 'AA26001', 'T3'))
        self.assertEqual((line['ref'], line['costRef'], line['grade']), (None, 'AA26001', 'B'))
        for a in rows(ds, 'alloc'):
            if a['kind'] != 'production':
                self.assertIsNone(a['ref'])


class InventoryC8(unittest.TestCase):
    def test_potential_gp_after_deductions_and_free_units(self):
        ds = build()
        st = by(ds, 'styles', 'base')['ROQAQF201SLS']
        ls = [r for r in rows(ds, 'lines') if r['base'] == 'ROQAQF201SLS' and r['type'] == 'a2000']
        self.assertEqual(st['dedPct'], E.r4(100 * sum(r['deduct'] for r in ls) / sum(r['rev'] for r in ls)))
        free = sum(r['free'] for r in rows(ds, 'production') if r['base'] == 'ROQAQF201SLS')
        self.assertEqual((st['atsFreeProd'], st['atsFreeStock']), (free, max(0, st['ats'] - free)))
        for s in rows(ds, 'styles'):             # the client gets the same cents from the published fields
            if s['atsPotentialGp'] is not None:
                self.assertEqual(s['atsPotentialGp'],
                                 E.r2(max(0, s['ats']) * (s['expPrice'] * (1 - s['dedPct'] / 100) - s['landedU'])))


class FiberF5(unittest.TestCase):
    def test_fiber_by_style_beats_the_fabric_default(self):
        self.assertEqual(E.decode_sku('ROQAPO101SLS', PARAMS)['fiber'], 'mmf')
        p = dict(PARAMS, fiberByStyle={'ROQAPO101SLS': 'cotton'})
        self.assertEqual(E.decode_sku('ROQAPO101SLS-L', p)['fiber'], 'cotton')
        self.assertEqual(E.decode_sku('ROQAPO102SLS', p)['fiber'], 'mmf')
        self.assertEqual(E.decode_sku('ROQAPO101SLS', dict(PARAMS, fiberByStyle={'ROQAPO101SLS': 'silk'}))['fiber'], 'mmf')
        self.assertEqual(E.decode_sku('ROQAPO101SLS', dict(PARAMS, fiberByStyle=['x']))['fiber'], 'mmf')


class HalfPriceQuoteF6(unittest.TestCase):
    QUOTE = rec('HQ!D2', 'SYN-HQ', 'factory_quotation', 6.1616, pool='GY:CLUB', brand_code=None, fabric_codes=['QU'],
                fit_class='SLIM', pattern='SOLID', factory_code='TT')

    def test_group_quote_counts_as_brand_matched_in_its_own_pool(self):
        cb = dict(COSTBOOK, records=RECORDS + [self.QUOTE])
        c = E.CostIndex(cb, {}, [], [], today=TODAY)
        r = c.resolve('CLQZQU201SLS', 'TT')
        self.assertEqual((r['level'], r['fobU'], r['fxShare']), ('L4a', 6.1616, 0.0))
        self.assertIn('customer_quote', r['flags'])
        o = c.resolve('ROQZQU201SLS', 'TT')                     # another group: not its quote
        self.assertEqual((o['level'], o['fobU']), ('L4c', 5.5151))

        class Legacy(E.CostIndex):
            QUOTE_BRAND_NEUTRAL = False
        self.assertEqual(Legacy(cb, {}, [], [], today=TODAY).resolve('CLQZQU201SLS', 'TT')['level'], 'L4c')


class BlackLabelF7(unittest.TestCase):
    def test_black_label_by_feed_label(self):
        s = src()
        s['inventory']['items'] = s['inventory']['items'] + [inv('ROBLQF301SLS', tr=10, brand='BLACK'),
                                                             inv('ROBLQF302SLS', tr=10, brand='BLO')]
        ds = build(s)
        b = {r['sku']: r['brand'] for r in rows(ds, 'inventory')}
        self.assertEqual((b['ROBLQF301SLS'], b['ROBLQF302SLS']), ('BLK', 'BL'))
        self.assertEqual(ds['dict']['brands']['BLK'], 'Black Label')
        self.assertEqual((E._roy_pct('BLK', ds['settings']), E._roy_pct('BL', ds['settings'])), (10.0, 0.0))


class HistoryAliasF8(unittest.TestCase):
    def shipped_style(self, customers, fob=('FOBX',)):
        s = src()
        s['fob_customers'] = list(fob)
        s['sales_analytics']['styles'][0][7] = customers
        return by({'b': build(s)['shipped']['byStyle']}, 'b', 'base')['ROQAQF201SLS']

    def test_history_codes_count_as_their_account(self):
        self.assertEqual(E._Build.hist_code('amazon_drop'), 'AMAZ')
        self.assertEqual(self.shipped_style({'TJMX': [10, 100.0]})['dedPct'], 1.0)          # off-price
        self.assertEqual(self.shipped_style({'KOHLSDROP': [10, 100.0]})['dedPct'], 7.0)     # department
        self.assertEqual(self.shipped_style({'CENT': [10, 100.0]}, fob=('CENT1',))['fobShare'], 1.0)


class ApoPricingF2(unittest.TestCase):
    def src_apo(self):
        s = src()
        s['apo']['rows'] = s['apo']['rows'] + [
            {'style': 'ZZKIT01-1', 'qty': 24, 'customer': 'BJS', 'po': 'SYNTH KIT'},
            {'style': 'ROQAQF207SLS', 'qty': 30, 'customer': 'AMAZON', 'po': 'SYNTH AMZ'},
            {'style': 'ROQAQF201SLS', 'qty': 5, 'customer': 'MEN WARHOUSE', 'po': 'SYNTH MW'},
            {'style': 'ROQAQF209SLS', 'qty': 20, 'customer': 'AMAZON', 'po': 'SYNTH AMZ 2'}]
        s['sales_analytics']['styles'].append(['ROQAQF207SLS', 'SYN', 60, 1200.0, '2025-03-01', '2026-01-10',
                                               {'2026-01': [60, 1200.0, 0, 0.0]}, {'AMAZ': [60, 1200.0]}, 'RED'])
        s['sales_analytics']['styles'].append(['ROQAQF208SLS', 'SYN', 600, 7200.0, '2025-03-01', '2026-01-10',
                                               {'2026-01': [600, 7200.0, 0, 0.0]}, {'AMAZ': [600, 7200.0]}, 'RED'])
        return s

    def test_brand_history_comes_after_style_evidence(self):
        ds = build(self.src_apo())
        A = {r['style']: r for r in rows(ds, 'apo')}
        br = A['ROQAQF209SLS']            # no price evidence of its own: the customer's brand history
        self.assertEqual((br['cust'], br['priceBasis'], br['estPrice']),
                         ('AMAZ', 'customer_brand_invoice', E.r4((1200.0 + 7200.0) / 660)))
        self.assertTrue({'price_history', 'price_proxy'} <= set(br['flags']))
        self.assertIn('customer_brand_invoice', ds['dict']['priceBasis'])

    def test_price_cascade(self):
        ds = build(self.src_apo())
        A = {r['style']: r for r in rows(ds, 'apo')}
        kit = A['ZZKIT01-1']
        self.assertEqual((kit['priceBasis'], kit['estPrice'], kit['coveredBy']), ('customer_line', E.r4(99.0 / 12), '6|ZZKIT01'))
        self.assertIn('covered_by_bulk', kit['flags'])
        self.assertIn('supply_in_allocations', by(ds, 'lines', 'id')['6|ZZKIT01']['flags'])
        cov = ds['totals']['apo']['coveredByBulk']
        self.assertEqual((cov['lines'], cov['rev']), (1, kit['rev']))
        amz = A['ROQAQF207SLS']
        self.assertEqual((amz['cust'], amz['priceBasis'], amz['estPrice']), ('AMAZ', 'customer_invoice', 20.0))
        self.assertIn('price_history', amz['flags'])
        mw = A['ROQAQF201SLS'] if A['ROQAQF201SLS']['cust'] == 'MENS' else [r for r in rows(ds, 'apo') if r['cust'] == 'MENS'][0]
        self.assertEqual((mw['cust'], mw['custName'], mw['priceBasis']), ('MENS', 'MEN WARHOUSE', 'style_open'))
        self.assertEqual(ds['dict']['customers']['MENS']['name'], 'MEN WARHOUSE')

    def test_offprice_price_only_for_offprice_or_flagged(self):
        A = rows(build(), 'apo')
        self.assertEqual(A[0]['priceBasis'], 'customer_brand')              # ROSS: its own open price
        other = A[1]                                                         # an unmapped customer, group other
        self.assertEqual(other['priceBasis'], 'offprice_brand')
        self.assertTrue({'price_offprice', 'price_proxy'} <= set(other['flags']))
        self.assertIn('offprice_brand', build()['dict']['priceBasis'])


class ApoTotalsR8(unittest.TestCase):
    """The server's APO totals keep an unpriced allocation's cost apart, as the page does."""

    def test_unpriced_cost_stays_out_of_the_cost_totals(self):
        s = src()
        s['apo']['rows'] = s['apo']['rows'] + [
            {'style': 'ROGBQF201SLS', 'qty': 25, 'customer': 'SOMEONE NEW', 'po': 'SYNTH UNPRICED'}]
        ds = build(s)
        A = rows(ds, 'apo')
        unp = [r for r in A if r['rev'] is None]
        self.assertEqual([r['style'] for r in unp], ['ROGBQF201SLS'])
        u = unp[0]
        self.assertIn('unpriced', u['flags'])
        self.assertIsNotNone(u['cogs'])                          # it has a cost, but no price
        t = ds['totals']['apo']
        priced = [r for r in A if r['rev'] is not None]
        for f in ('fob', 'duty', 'freight', 'fees', 'cogs'):
            self.assertAlmostEqual(t[f], sum(r[f] or 0 for r in priced), places=2, msg=f)
        self.assertEqual((t['unpricedLines'], t['unpricedUnits']), (1, 25))
        self.assertEqual((t['unpricedFob'], t['unpricedCogs']), (u['fob'], u['cogs']))
        self.assertAlmostEqual(t['uncostedNet'], sum(r['net'] or 0 for r in A if r['gp'] is None), places=2)
        self.assertAlmostEqual(t['net'] - t['cogs'] - t['uncostedNet'], t['gp'], places=2)
        self.assertEqual(t['costedUnits'] + t['uncostedUnits'], t['units'])
        ob = ds['totals']['openBook']
        L = [r for r in rows(ds, 'lines') if r['type'] == 'a2000']
        self.assertAlmostEqual(ob['cogs'], sum(r['cogs'] or 0 for r in L), places=2)   # lines always carry a price
        self.assertEqual((ob['unpricedLines'], ob['unpricedUnits'], ob['unpricedCogs']), (0, 0, 0.0))
        self.assertAlmostEqual(ob['net'] - ob['cogs'] - ob['uncostedNet'], ob['gp'], places=2)


class ApoBrandResolverF2(unittest.TestCase):
    """The APO price maps take the brand from the lines' resolver, so legacy codes count (F2 residual)."""
    LEGACY_LINE, LEGACY_HIST = 'ZZLEGACY77', 'ZZLEGACY88'

    def src_legacy(self):
        s = src()
        s['inventory']['items'] = s['inventory']['items'] + [inv(self.LEGACY_LINE, tr=40, brand='NAUTICA')]
        s['open_orders']['orders'] = s['open_orders']['orders'] + [order('7', self.LEGACY_LINE, 20, 17.5, cust='AMAZ')]
        s['sales_analytics']['styles'].append([self.LEGACY_HIST, 'NAUTICA', 600, 8700.0, '2024-03-01', '2025-06-10',
                                               {'2025-06': [600, 8700.0, 0, 0.0]}, {'MENS': [600, 8700.0]}, 'RED'])
        s['apo']['rows'] = s['apo']['rows'] + [
            {'style': 'RONAQF301SLS', 'qty': 12, 'customer': 'AMAZON', 'po': 'SYNTH AMZ NA'},
            {'style': 'MWNAQF302SLS', 'qty': 24, 'customer': 'MEN WARHOUSE', 'po': 'SYNTH MW NA'}]
        return s

    def test_legacy_lines_and_invoices_count_for_their_brand(self):
        for code in (self.LEGACY_LINE, self.LEGACY_HIST):
            self.assertIsNone(E.decode_sku(code, PARAMS))          # decode_sku alone sees no brand
        ds = build(self.src_legacy())
        self.assertEqual(by(ds, 'lines', 'id')['7|' + self.LEGACY_LINE]['brand'], 'NA')    # the lines' resolver
        A = {r['po']: r for r in rows(ds, 'apo')}
        amz = A['SYNTH AMZ NA']
        self.assertEqual((amz['cust'], amz['brand'], amz['priceBasis'], amz['estPrice']),
                         ('AMAZ', 'NA', 'customer_brand', 17.5))    # Amazon's own open price for the brand
        mw = A['SYNTH MW NA']
        self.assertEqual((mw['cust'], mw['priceBasis'], mw['estPrice']),
                         ('MENS', 'customer_brand_invoice', E.r4(8700.0 / 600)))
        self.assertTrue({'price_history', 'price_proxy'} <= set(mw['flags']))
        self.assertNotIn('price_offprice', mw['flags'])


class ApoCustomerNames(unittest.TestCase):
    """Free-text APO names seen in live rows map to their A2000 account where the data is clear."""

    def test_names_map_to_their_account(self):
        M = E.APO_CUSTOMER_CODES
        self.assertEqual(M['COSTCO CANADA'], ['COST1'])
        for name in ('COSTCO', 'COSTCO US', 'COSTCO USA'):
            self.assertEqual(M[name], ['COST'], name)
        self.assertEqual((M['JCP'][0], M['JCPENNEY'][0]), ('JCP01', 'JCP01'))
        self.assertEqual((M['NORDSTROM'], M['MACY'], M['MACY BACKSTAGE'], M['MEN WARHOUSE']),
                         (['NORD'], ['MACY'], ['MACY1'], ['MENS']))
        self.assertEqual((M['HP'], M['WALM'], M['BEALL'][0]), (['HALF'], ['WALM1'], 'BEAL'))
        self.assertNotIn('PETER', M)                                # not clear from the data: left for David
        for name, codes in M.items():
            self.assertEqual(name, name.strip().upper(), name)      # rows are matched upper case
            self.assertTrue(codes and all(isinstance(c, str) and c == c.upper() for c in codes), name)

    def test_mapped_names_take_the_account_price_and_regime(self):
        s = src()
        s['open_orders']['orders'] = s['open_orders']['orders'] + [order('8', 'CCQAQF402SLS', 30, 8.3817, cust='COST1', wh='AW')]
        s['sales_analytics']['styles'].append(['JCQAQF501SLS', 'SYN', 80, 960.0, '2025-03-01', '2026-01-10',
                                               {'2026-01': [80, 960.0, 0, 0.0]}, {'JCP02': [80, 960.0]}, 'RED'])
        s['apo']['rows'] = s['apo']['rows'] + [
            {'style': 'CCQAQF401SLS', 'qty': 10, 'customer': 'Costco Canada', 'po': 'SYNTH CC'},
            {'style': 'JCQAQF501SLS', 'qty': 60, 'customer': 'JCP', 'po': 'SYNTH JCP'},
            {'style': 'NOQAQF601SLS', 'qty': 5, 'customer': 'NORDSTROM', 'po': 'SYNTH NO'},
            {'style': 'PHQAQF701SLS', 'qty': 5, 'customer': 'PETER', 'po': 'SYNTH PH'}]
        ds = build(s)
        A = {r['po']: r for r in rows(ds, 'apo')}
        cc = A['SYNTH CC']
        self.assertEqual((cc['cust'], cc['custName'], cc['priceBasis'], cc['estPrice']),
                         ('COST1', 'Costco Canada', 'customer_brand', 8.3817))
        self.assertEqual(cc['dutyRegime'], 'ca')                    # COST1's open lines land in Canada
        self.assertTrue({'regime_from_orders', 'non_us_dest'} <= set(cc['flags']))
        self.assertEqual(ds['dict']['customers']['COST1']['group'], 'club')
        jcp = A['SYNTH JCP']                                        # booked to JCP01, JCP02 invoices count
        self.assertEqual((jcp['cust'], jcp['priceBasis'], jcp['estPrice']), ('JCP01', 'customer_invoice', 12.0))
        self.assertEqual(A['SYNTH NO']['cust'], 'NORD')
        self.assertEqual(A['SYNTH PH']['cust'], 'PETER')            # unmapped: kept as typed


class AnalyticsRobustF2(unittest.TestCase):
    def variant(self, mut):
        s = src()
        mut(s['sales_analytics'])
        return build(s)

    def test_malformed_payloads_never_fail_the_build(self):
        control = build()
        row = ['ROQAQF203SLS', 'SYN', 5, 50.0, None, None]
        cases = {
            'months_list': lambda sa: sa['styles'].append(row + [[['2026-01', [5, 50.0]]], {}, None]),
            'customers_list': lambda sa: sa['styles'].append(row + [{}, [['ROSS', [5, 50.0]]], None]),
            'source_text': lambda sa: sa.update(source='oops'),
            'source_list': lambda sa: sa.update(source=[1, 2]),
            'fields_number': lambda sa: sa.update(fields=5),
            'styles_object': lambda sa: sa.update(styles={'a': 1}),
            'bare_row': lambda sa: sa['styles'].append('ROQAQF203SLS'),
            'style_object': lambda sa: sa['styles'].append([{'x': 1}, 'SYN', 5, 50.0, None, None, {}, {}, None]),
        }
        for name, mut in cases.items():
            with self.subTest(name):
                ds = self.variant(mut)
                self.assertEqual(ds['lines'], control['lines'])
                self.assertEqual(ds['inventory'], control['inventory'])
                json.dumps(ds, allow_nan=False)
        bad = self.variant(cases['styles_object'])
        self.assertEqual(bad['shipped']['state'], 'invalid')
        self.assertEqual({a['id']: a for a in bad['alerts']}['al_stale_analytics']['title'], 'Shipped history could not be read')
        part = self.variant(cases['months_list'])
        self.assertEqual((part['shipped']['state'], part['inputs']['analyticsRowsSkipped']), ('ready', 1))
        self.assertIn('al_analytics_rows', {a['id'] for a in part['alerts']})


class SettingsNullsC3(unittest.TestCase):
    def test_null_means_use_the_default(self):
        S = E.merge_settings({'royalty': {'defaultPct': 7.7777, 'byBrand': {'GB': None}},
                              'tariff': {'addonPct': {'VN': None, 'OTHER': None}, 'mfnPct': {'shirt_cotton': None}},
                              'factories': {'NK': None, '_default': None},
                              'freight': {'oceanPerUnit': {'shirt': None}},
                              'destinations': {'AE': None, 'ZZ': None},
                              'customerGroups': {'ROSS': None, 'ZZNEW': None},
                              'deductions': {'byCustomer': {'ROSS': None}, 'byGroup': {'department': None}}})
        # Maps whose default is a rule: the key goes, so the rule applies.
        self.assertEqual((E._roy_pct('GB', S), E._roy_pct('DK', S)), (7.7777, 10.0))
        self.assertNotIn('GB', S['royalty']['byBrand'])
        self.assertNotIn('VN', S['tariff']['addonPct'])
        self.assertEqual(S['tariff']['addonPct']['OTHER'], 10.0)
        self.assertEqual(E.adders(100.0, 10, 'dress_shirt', 'cotton', 'VN', S, 'us')[0], E.r2(100.0 * (19.7 + 10.0) / 100))
        self.assertNotIn('NK', S['factories'])
        self.assertEqual(E._factory_origin(S, 'NK'), 'CN')
        self.assertEqual(S['factories']['_default'], E.DEFAULT_SETTINGS['factories']['_default'])
        self.assertNotIn('ROSS', S['deductions']['byCustomer'])
        # Maps whose default is a value: the public default comes back.
        self.assertEqual(S['tariff']['mfnPct']['shirt_cotton'], 19.7)
        self.assertEqual(S['freight']['oceanPerUnit']['shirt'], 0.285)
        self.assertEqual(S['destinations']['AE'], 'ca')
        self.assertNotIn('ZZ', S['destinations'])
        self.assertEqual(S['customerGroups']['ROSS'], 'offprice')
        self.assertNotIn('ZZNEW', S['customerGroups'])
        self.assertEqual(S['deductions']['byGroup']['department'], 7.0)

    def test_destinations_are_normalized(self):
        S = E.merge_settings({'destinations': {'ae': 'US', 'xx': 'mars', 'CAN': 'moon'}})
        self.assertEqual(S['destinations']['AE'], 'us')
        self.assertNotIn('XX', S['destinations'])
        self.assertEqual(S['destinations']['CAN'], 'ca')        # a bad value on a default code keeps the default
        self.assertEqual(E.merge_settings({'tariff': {'caMfnPct': None}})['tariff']['caMfnPct'], 18.0)

    def test_new_defaults(self):
        d = E.DEFAULT_SETTINGS
        self.assertEqual(d['destinations'], {'CH': 'none', 'FOB': 'none', 'WALM': 'none', 'AE': 'ca', 'AW': 'ca',
                                             'CAN': 'ca', 'ABFI': 'ca'})
        self.assertEqual((d['tariff']['caMfnPct'], d['royalty']['byBrand']['BLK']), (18.0, 10.0))


class NoUnitsF10(unittest.TestCase):
    def test_a_line_with_no_units_adds_no_revenue(self):
        s = src()
        a, b = order('11', 'ROQAQF203SLS', 0, 12.5), order('12', 'ROQAQF203SLS', -12, 12.5)
        a['openValue'], b['openValue'] = 120.0, 150.0
        s['open_orders']['orders'] = s['open_orders']['orders'] + [a, b]
        L = by(build(s), 'lines', 'id')
        for k in ('11|ROQAQF203SLS', '12|ROQAQF203SLS'):
            self.assertEqual((L[k]['units'], L[k]['rev'], L[k]['gp'] or 0.0), (0, 0.0, 0.0))
            self.assertIn('no_units', L[k]['flags'])


class AlertsC4(unittest.TestCase):
    def test_every_alert_says_what_it_counts(self):
        ds = build()
        units = {'lines', 'POs', 'refs', 'allocations', 'settings', 'styles'}
        for a in ds['alerts']:
            for k in ('unit', 'valueLabel', 'refsTotal', 'count', 'value'):
                self.assertIn(k, a)
            if a['kind'] == 'stale_input':
                self.assertIsNone(a['unit'])
            else:
                self.assertIn(a['unit'], units)
            if a['value'] is None:
                self.assertIsNone(a['valueLabel'])
            self.assertIn(a['kind'], ds['dict']['alertKinds'])
            self.assertNotRegex(a['title'] + a['detail'], '[–—]')
        k = kinds_of(ds)
        self.assertEqual((k['thin_contribution']['unit'], k['thin_contribution']['lineCount']),
                         ('POs', k['thin_contribution']['refsTotal']))
        self.assertEqual(k['assumption']['detail'],
                         'These settings still use public defaults: RMB rate, calculator grid order, customer deductions, '
                         'freight and fees, operating expenses, royalty, duty and tariffs, '
                         'Canada and direct-import destinations.')
        self.assertIn('Unconfirmed assumptions: RMB rate, calculator grid order', ' '.join(ds['notes']))
        for n in ds['notes'] + [ds['shipped']['note']]:
            self.assertNotRegex(n, '[–—]')

    def test_needs_cost_is_for_firm_lines_only(self):
        s = src()
        s['open_orders']['orders'] = s['open_orders']['orders'] + [order('7', 'ZZLEGACY9', 10, 9.0)]
        k = kinds_of(build(s))
        self.assertEqual((k['needs_cost']['severity'], k['needs_cost']['refs']['lines']), ('high', ['7|ZZLEGACY9']))
        self.assertEqual(k['bulk_needs_cost']['refs']['lines'], ['5|CUQZZZ26'])

    def test_shipped_note_is_plain_text(self):
        note = build()['shipped']['note']
        self.assertIn('Feb 20, 2026', note)
        self.assertNotRegex(note, r'\d{4}-\d{2}-\d{2}')
        self.assertNotRegex(note, r'\b[a-z]+[A-Z]\w*\b')          # no field names

    def test_dict_labels_every_flag_and_tier(self):
        ds = build(src_dest())
        for t in ('lines', 'apo', 'inventory', 'production'):
            for r in rows(ds, t):
                for f in r['flags']:
                    self.assertIn(f, ds['dict']['flags'], (t, f))
        self.assertEqual(set(ds['dict']['lotTiers']), {'T1', 'T1b', 'T2', 'T3', 'T4', 'T5'})
        self.assertEqual(ds['dict']['lotTiers']['T5']['tone'], 'warn')
        self.assertEqual(set(ds['dict']['regimes']), {'us', 'ca', 'none'})
        for labels in (ds['dict']['flags'], {k: v['label'] for k, v in ds['dict']['alertKinds'].items()}):
            for v in labels.values():
                self.assertNotRegex(v, '[–—]')


class BrandsC7(unittest.TestCase):
    def test_program_and_legacy_codes(self):
        cases = {'CUCHSYN26': 'CH', 'CU-NASYN': 'NA', 'CC-NASYN1': 'NA', 'CCEBSYN25': 'EB', 'BJNASYN27': 'NA',
                 'CUNTSYN26': 'NA', '1234567': None, 'CUQZZZ26': None, 'AB': None}
        for code, want in cases.items():
            self.assertEqual(E.program_brand(code), want, code)

    def test_rows_without_a_brand_carry_empty_text(self):
        self.assertEqual(by(build(), 'lines', 'id')['5|CUQZZZ26']['brand'], '')
        s = src()
        s['open_orders']['orders'] = s['open_orders']['orders'] + [order('12', 'CUDKSYN26', 100, 11.0, cust='COST', bulk=True)]
        self.assertEqual(by(build(s), 'lines', 'id')['12|CUDKSYN26']['brand'], 'DK')


class KitPiecesF11(unittest.TestCase):
    def test_kit_lines_count_pieces(self):
        ds = build()
        k = by(ds, 'lines', 'id')['6|ZZKIT01']
        self.assertEqual((k['units'], k['pieces']), (10, 10 * 12))
        al = [a for a in rows(ds, 'alloc') if a['line'] == '6|ZZKIT01']
        self.assertEqual((sum(a['units'] for a in al), sum(a['pieces'] for a in al)), (10, 10 * 12))
        tb = ds['totals']['bulk']
        self.assertEqual((tb['units'], tb['kitCartons']), (1000 + 120, 10))
        self.assertEqual(sum(v[0] for v in ds['coverage']['bulk'].values()), 1000 + 120)


# ── price basis (contract C10): the Current USD printed on each sheet, repriced only at a saved rate ──
def basis_book(extra=(), change=None):
    """COSTBOOK with extra records and per-id field changes."""
    recs = copy.deepcopy(RECORDS)
    for r in recs:
        if change and r['id'] in change:
            r.update(change[r['id']])
    return dict(COSTBOOK, records=recs + [copy.deepcopy(x) for x in extra])


# Fabric QZ: three candidates on sheets at two rates. Fabric QY: two candidates (an even count).
PA, PB, PC = 4.0404, 4.2323, 5.0505
MIXED = [calc('GN!Z1', 'GN:BASE', PA, 'QA', ['QZ'], 'SLIM', 'SOLID', e1=R2),
         calc('GN!Z2', 'GN:BASE', PB, 'QA', ['QZ'], 'SLIM', 'SOLID', e1=FXB),
         calc('GN!Z3', 'GN:BASE', PC, 'QA', ['QZ'], 'SLIM', 'SOLID', e1=FXB),
         calc('GN!Y1', 'GN:BASE', PA, 'QA', ['QY'], 'SLIM', 'SOLID', e1=FXB),
         calc('GN!Y2', 'GN:BASE', PC, 'QA', ['QY'], 'SLIM', 'SOLID', e1=R2)]
P1 = 3.4343                                                   # GN!F1 printed on a sheet at R2
AT_R2 = {'GN!F1': {'price_usd': P1, F_BASE: round(P1 * R2 / FXB, 6), F_E1: R2}}


class PriceBasisC10(unittest.TestCase):
    def test_printed_current_usd_is_used(self):
        c = ci(costbook=basis_book(change=AT_R2))
        r = c.resolve('ROQAQF201SLS', 'NN')
        self.assertEqual((r['level'], r['fobU'], r['fxShare'], r['fxRef']), ('L4a', P1, 1.0, R2))
        lst = c.resolve('ROQAQF101SLS', 'AA', 'AA26001')
        self.assertEqual((lst['fobU'], lst['fxShare'], lst['fxRef']), (7.7777, 0.0, None))     # lists are USD
        q = c.resolve('ROQAQU201SLS', 'TT')
        self.assertEqual((q['fobU'], q['fxShare'], q['fxRef']), (5.5151, 0.0, None))           # USD quotation

    def test_saved_rate_prices_the_rmb_amount(self):
        c = ci(costbook=basis_book(change=AT_R2), settings={'fx': {'rate': 7.7}})
        r = c.resolve('ROQAQF201SLS', 'NN')
        self.assertEqual((r['fobU'], r['fxRef']), (E.r4(P1 * R2 / 7.7), 7.7))
        self.assertEqual(c.resolve('ROQAQF201SLS', 'TT')['fobU'], E.r4(4.1717 * FXB / 7.7))   # a sheet at FXB
        self.assertEqual(c.resolve('ROQAQF101SLS', 'AA', 'AA26001')['fobU'], 7.7777)
        self.assertEqual(c.resolve('ROQAQU201SLS', 'TT')['fobU'], 5.5151)

    def test_candidates_are_repriced_before_the_median_and_range(self):
        cb = basis_book(MIXED)
        p = ci(costbook=cb).resolve('ROQAQZ201SLS', 'NN')          # printed PA (at R2), PB and PC (at FXB)
        self.assertEqual((p['fobU'], p['rangeLo'], p['rangeHi'], p['fxRef']), (PB, PA, PC, FXB))
        # At FXB the record printed at R2 costs more than PB and becomes the median. Scaling the printed
        # median instead would keep PB.
        at = ci(costbook=cb, settings={'fx': {'rate': FXB}}).resolve('ROQAQZ201SLS', 'NN')
        self.assertEqual((at['fobU'], at['rangeLo'], at['rangeHi'], at['fxRef']), (E.r4(PA * R2 / FXB), PB, PC, FXB))
        at2 = ci(costbook=cb, settings={'fx': {'rate': R2}}).resolve('ROQAQZ201SLS', 'NN')
        self.assertEqual((at2['fobU'], at2['rangeLo'], at2['rangeHi']), (PA, E.r4(PB * FXB / R2), E.r4(PC * FXB / R2)))

    def test_fx_ref_is_value_weighted_and_the_what_if_matches(self):
        cb = basis_book(MIXED)
        p = ci(costbook=cb).resolve('ROQAQY201SLS', 'NN')          # even count: the mean of PA (at FXB) and PC (at R2)
        med, ref = (PA + PC) / 2, (PA * FXB / 2 + PC * R2 / 2) / ((PA + PC) / 2)
        self.assertEqual((p['fobU'], p['fxShare'], p['fxRef']), (E.r4(med), 1.0, E.r4(ref)))
        at = ci(costbook=cb, settings={'fx': {'rate': 7.7}}).resolve('ROQAQY201SLS', 'NN')
        self.assertEqual(at['fobU'], E.r4((PA * FXB / 7.7 + PC * R2 / 7.7) / 2))
        # fxRef is published at 4 decimals, so the page's what-if can land 0.0001 from a rebuild.
        self.assertAlmostEqual(E.fx_what_if_unit(p['fobU'], p['fxShare'], p['fxRef'], 7.7), at['fobU'], delta=0.00015)
        self.assertAlmostEqual(med * ref / 7.7, (PA * FXB / 7.7 + PC * R2 / 7.7) / 2, places=12)   # exact before rounding

    def test_dollar_steps_do_not_move(self):
        # Regular fit from the slim row plus the fit premium (a dollar step): only the row moves.
        p = ci().resolve('ROQAQJ401RFS', 'NN')
        self.assertEqual((p['fobU'], p['fxShare'], p['fxRef']),
                         (E.r4(3.9393 + 0.2222), E.r4(3.9393 / (3.9393 + 0.2222)), FXB))
        at = ci(settings={'fx': {'rate': 7.7}}).resolve('ROQAQJ401RFS', 'NN')
        self.assertEqual(at['fobU'], E.r4(3.9393 * FXB / 7.7 + 0.2222))
        self.assertAlmostEqual(E.fx_what_if_unit(p['fobU'], p['fxShare'], p['fxRef'], 7.7), at['fobU'], delta=0.00015)
        # Short sleeve from long sleeve (a negative step): the share is a little above 1 and still exact.
        s = ci().resolve('ROQAQJ301SSS', 'NN')
        self.assertGreater(s['fxShare'], 1.0)
        at = ci(settings={'fx': {'rate': 7.7}}).resolve('ROQAQJ301SSS', 'NN')
        self.assertAlmostEqual(E.fx_what_if_unit(s['fobU'], s['fxShare'], s['fxRef'], 7.7), at['fobU'], delta=0.00015)

    def test_defaults_and_lists_follow_the_saved_rate(self):
        ledger = [led('NN26001', 'ROQAQF801SLS', 500), led('TT26001', 'ROQAQF802SLS', 100)]
        p = ci(ledger).resolve('ROQAQV901SLS', 'TT')      # L6 by category and brand: the 500-unit L4a line
        self.assertEqual((p['level'], p['fobU'], p['fxShare'], p['fxRef']), ('L6', 4.4444, 1.0, FXB))
        c2 = ci(ledger, settings={'fx': {'rate': R2}})
        at = c2.resolve('ROQAQV901SLS', 'TT')
        self.assertEqual((at['level'], at['fobU'], at['fxRef']), ('L6', E.r4(4.4444 * FXB / R2), R2))
        self.assertEqual(c2.resolve('ROQAQF101SLS', 'AA', 'AA26009')['fobU'], 7.7777)   # L3 sibling list price

    def test_rows_carry_fx_ref_and_the_dataset_says_which_basis(self):
        ds = build()
        self.assertIsNone(ds['settings']['fx']['rate'])
        fx = ds['inputs']['fx']
        self.assertEqual((fx['mode'], fx['rate'], fx['printed'], fx['weighted'], fx['unrated']), ('printed', None, [FXB], FXB, 0))
        for t in ('lines', 'alloc', 'apo', 'inventory', 'production', 'styles'):
            self.assertIn('fxShare', ds[t]['fields'], t)
            self.assertIn('fxRef', ds[t]['fields'], t)
            n = 0
            for r in rows(ds, t):
                if r['fxShare']:
                    n += 1
                    self.assertEqual(r['fxRef'], FXB, (t, r))
                else:
                    self.assertIsNone(r['fxRef'], (t, r))
            self.assertGreater(n, 0, t)
        sh = rows({'b': ds['shipped']['byStyle']}, 'b')
        self.assertTrue(sh and all((r['fxRef'] == FXB) if r['fxShare'] else r['fxRef'] is None for r in sh))
        self.assertIn("Calculator prices are the Current USD printed on each sheet, for the style's fit. "
                      'The sheets print 7 RMB per US dollar.', ds['notes'])
        at = build(settings={'fx': {'rate': 7.7}})
        self.assertEqual((at['settings']['fx']['rate'], at['inputs']['fx']['mode'], at['inputs']['fx']['weighted']),
                         (7.7, 'rate', 7.7))
        self.assertTrue(all(r['fxRef'] in (None, 7.7) for r in rows(at, 'lines')))
        self.assertIn('Every RMB-based calculator price uses 7.7 RMB per US dollar. The sheets print 7.', at['notes'])
        cut = build(settings={'fx': {'basis': 'after_cut'}})
        self.assertEqual((cut['inputs']['fx']['mode'], cut['inputs']['fx']['weighted']), ('after_cut', None))
        self.assertTrue(all(r['fxShare'] == 0.0 and r['fxRef'] is None for r in rows(cut, 'lines')))
        for n in ds['notes'] + at['notes'] + cut['notes']:
            self.assertNotRegex(n, '[–—]')

    def test_evidence_shows_the_price_used_and_the_sheet_rate(self):
        s = src()
        s['open_orders']['orders'] = s['open_orders']['orders'] + [order('13', 'ROQAQF207SLS', 60, 9.0, cust='KOHL')]
        for settings, used in (({}, P1), ({'fx': {'rate': 7.7}}, E.r4(P1 * R2 / 7.7))):
            ev = build(s, cb=basis_book(change=AT_R2), settings=settings)['evidence']['GN!F1']
            self.assertEqual((ev['priceUsd'], ev['priceSheet'], ev['fxSheet']), (used, P1, R2))

    def test_what_if_on_the_printed_dataset_matches_a_rebuild(self):
        # The page's what-if starts from 4-decimal published values, so unit costs agree to rounding.
        base, at = build(), build(settings={'fx': {'rate': 7.7}})
        A, n = by(at, 'lines', 'id'), 0
        for r in rows(base, 'lines'):
            if r['fobU'] is None:
                continue
            u = E.fx_what_if_unit(r['fobU'], r['fxShare'], r['fxRef'], 7.7)
            self.assertAlmostEqual(u, A[r['id']]['fobU'], delta=0.0002, msg=r['id'])
            n += r['fxShare'] > 0
        self.assertGreater(n, 0)

    def test_client_formula(self):
        self.assertEqual(E.fx_what_if_unit(4.4444, 1.0, 7.0, 7.7), E.r4(4.4444 * (1 - 1.0 + 1.0 * 7.0 / 7.7)))
        self.assertEqual(E.fx_what_if_unit(4.4444, 0.0, None, 7.7), 4.4444)                  # no RMB-based part
        self.assertEqual(E.fx_what_if_unit(4.4444, 0.5, None, 7.7, 7.0), E.r4(4.4444 * (0.5 + 0.5 * 7.0 / 7.7)))
        self.assertEqual(E.fx_what_if_unit(4.4444, 0.5, None, 7.7), 4.4444)                  # nothing to scale from
        self.assertIsNone(E.fx_what_if_unit(None, 1.0, 7.0, 7.7))
        ds = build()
        moved = 0
        for t in ('lines', 'apo'):
            for r in rows(ds, t):
                if r['fob'] is None or r['rev'] is None:
                    continue
                c = client_recalc_fx(r, ds['settings'], ds['dict'], 7.7)
                self.assertEqual(c['fob'], E.r2(c['qty'] * c['fobU']) if r['fxShare'] else r['fob'], (t, r['id']))
                self.assertEqual(c['cogs'], E.r2(c['fob'] + c['duty'] + c['freight'] + c['fees']))
                self.assertEqual((c['deduct'], c['net']), (r['deduct'], r['net']))              # revenue side unchanged
                if r['fxShare'] and r['fob'] > 0:
                    self.assertLess(c['fob'], r['fob'])                                         # a weaker RMB: cheaper
                    moved += 1
                    if 'kit' in r['flags']:
                        self.assertEqual(c['qty'], r['pieces'])
        self.assertGreater(moved, 0)


if __name__ == '__main__':
    unittest.main()
