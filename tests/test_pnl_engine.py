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
        # Contract C15: no pattern letter, so every row of the ref fits. One row, the lowest (R7), never their mean.
        self.assertEqual((m['level'], m['fobU'], m['rangeLo'], m['rangeHi'], m['evidence']),
                         ('L2', 3.3333, 3.3333, 3.8787, ['BB!H2']))
        self.assertTrue({'malformed_sku', 'ambiguous_rows'} <= set(m['flags']))

    def test_l2_ref_pattern_fit(self):
        r = ci().resolve('ROQAQS110SLS', 'CC', 'CC26001')
        self.assertEqual((r['level'], r['fobU']), ('L2', 2.2222))

    def test_l3_siblings(self):
        c = ci()
        self.assertEqual(c.resolve('ROQAQF101SLS', 'AA', 'AA26009')['fobU'], 7.7777)       # same style, other ref
        d = c.resolve('BUQAQF102SLS', 'AA', 'AA26009')
        self.assertEqual((d['level'], d['fobU']), ('L3', 6.6666))                         # same design
        f = c.resolve('ROQAQF999SLS', 'AA', 'AA26009')
        self.assertEqual((f['level'], f['fobU']), ('L3', 6.6666))       # one row of the brand's list, the lowest (C15)
        self.assertTrue({'fabric_median', 'ambiguous_rows'} <= set(f['flags']))
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
        mf = c.resolve('ROQAQF501MFS', 'NN')         # contract C15 R6: the regular column, an exact match
        self.assertEqual((mf['level'], mf['fobU']), ('L4a', 4.6666))
        self.assertIn('fit_as_regular', mf['flags'])
        pat = c.resolve('ROQAQJ601SLP', 'NN')        # R4: a print style never takes the solid row (no relaxed rung)
        self.assertEqual((pat['level'], pat['fobU']), ('L7', None))

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


def rc_pct(S):
    """The page's revenue cost percent (contract C11): the item percents summed in list order from 0."""
    t = 0.0
    for it in S['revenueCosts']['items']:
        t += it['pct']
    return t


def client_recalc(r, S, dct, table):
    """Python copy of the frontend's DESIGN 5.6 recompute (pnl.core.js _pnlRecalc, scen null), with
    the duty regime read from the row (contract C2), and the royalty base, revenue costs and
    contribution of contract C11."""
    kit = 'kit' in (r.get('flags') or [])
    qty = round(r['fob'] / r['fobU']) if kit and r['fobU'] and r['fob'] else r['units']
    d, f, e = E.adders(r['fob'], qty, r['cat'], r['fiber'], r['origin'], S, r['dutyRegime'])
    cogs = E.r2(r['fob'] + d + f + e)
    grp = dct['customers'][r['cust']]['group']
    ded = E.r2(r['rev'] * E._ded_pct(r['cust'], S, grp) / 100)
    net = E.r2(r['rev'] - ded)
    gp = E.r2(net - cogs)
    roy = E.r2((r['rev'] if S['royalty'].get('base') == 'revenue' else net) * E._roy_pct(r['brand'], S) / 100)
    rc = E.r2(r['rev'] * rc_pct(S) / 100)
    return {'duty': d, 'freight': f, 'fees': e, 'cogs': cogs, 'deduct': ded, 'net': net, 'gp': gp, 'royalty': roy,
            'revCost': rc, 'contrib': E.r2(gp - roy - rc)}


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
        self.assertEqual(set(d['confirmed']), {'fx', 'grid', 'tariff', 'freight', 'royalty', 'deductions', 'opex',
                                               'landed', 'revenueCosts'})       # C11 added the last two
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

    def test_history_takes_the_feed_label_first(self):
        # Invoices can name a Black Label style with another label (BLO). Shipped history takes the style's feed
        # label first, as lines and the styles table do, so its history pays Black Label's royalty. A style with
        # invoices only keeps its invoice label.
        s = src()
        s['inventory']['items'] = s['inventory']['items'] + [inv('ROBLQF301SLS', tr=10, brand='BLACK')]
        for st in ('ROBLQF301SLS', 'ROBLQF309SLS'):
            s['sales_analytics']['styles'].append([st, 'BLO', 12, 131.3131, '2025-06-01', '2026-02-10',
                                                   {'2026-01': [12, 131.3131, 0, 0.0]}, {'ROSS': [12, 131.3131]}, 'NAVY'])
        ds = build(s, settings={'royalty': {'byBrand': {'BLK': 2.25, 'BL': 5.4321}}})      # sentinel rates only
        H = {r['base']: r for r in rows({'b': ds['shipped']['byCustomer']}, 'b') if r['cust'] == 'ROSS'}
        Y = by({'b': ds['shipped']['byStyle']}, 'b', 'base')
        blk, bl = H['ROBLQF301SLS'], H['ROBLQF309SLS']
        self.assertEqual((blk['brand'], Y['ROBLQF301SLS']['brand'], by(ds, 'styles', 'base')['ROBLQF301SLS']['brand']),
                         ('BLK', 'BLK', 'BLK'))
        self.assertIsNotNone(blk['fob'])
        self.assertEqual(blk['royalty'], E.r2(blk['net'] * 2.25 / 100))              # Black Label's own rate
        self.assertNotEqual(blk['royalty'], E.r2(blk['net'] * 5.4321 / 100))
        self.assertEqual((bl['brand'], Y['ROBLQF309SLS']['brand']), ('BL', 'BL'))
        self.assertIsNotNone(bl['fob'])
        self.assertEqual(bl['royalty'], E.r2(bl['net'] * 5.4321 / 100))
        self.assertGreater(bl['royalty'], 0)


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
        # C11: deductions are called chargebacks, and revenue costs and the import cost method are blocks.
        self.assertEqual(k['assumption']['detail'],
                         'These settings still use public defaults: RMB rate, calculator grid order, chargebacks, '
                         'freight and fees, payroll and monthly costs, royalty, revenue costs, import cost method, '
                         'duty and tariffs, Canada and direct-import destinations.')
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

    def test_candidates_are_repriced_before_the_lowest_is_picked(self):
        # Contract C15 R7: rows still tied give the lowest, at the P&L's basis. Each row is repriced first.
        cb = basis_book(MIXED)
        p = ci(costbook=cb).resolve('ROQAQZ201SLS', 'NN')          # printed PA (at R2), PB and PC (at FXB)
        self.assertEqual((p['fobU'], p['rangeLo'], p['rangeHi'], p['fxRef'], p['evidence']), (PA, PA, PC, R2, ['GN!Z1']))
        self.assertIn('ambiguous_rows', p['flags'])
        # At FXB the row printed at R2 costs more than PB, so PB becomes the lowest. Scaling the printed pick instead
        # would keep PA.
        at = ci(costbook=cb, settings={'fx': {'rate': FXB}}).resolve('ROQAQZ201SLS', 'NN')
        self.assertEqual((at['fobU'], at['rangeLo'], at['rangeHi'], at['fxRef'], at['evidence']), (PB, PB, PC, FXB, ['GN!Z2']))
        at2 = ci(costbook=cb, settings={'fx': {'rate': R2}}).resolve('ROQAQZ201SLS', 'NN')
        self.assertEqual((at2['fobU'], at2['rangeLo'], at2['rangeHi']), (E.r4(PB * FXB / R2), E.r4(PB * FXB / R2),
                                                                         E.r4(PC * FXB / R2)))

    def test_one_row_keeps_its_own_rate_and_the_what_if_matches(self):
        cb = basis_book(MIXED)
        p = ci(costbook=cb).resolve('ROQAQY201SLS', 'NN')          # PA (at FXB) and PC (at R2): the lowest, PA
        self.assertEqual((p['fobU'], p['fxShare'], p['fxRef']), (PA, 1.0, FXB))
        at = ci(costbook=cb, settings={'fx': {'rate': 7.7}}).resolve('ROQAQY201SLS', 'NN')
        self.assertEqual(at['fobU'], E.r4(PA * FXB / 7.7))
        self.assertAlmostEqual(E.fx_what_if_unit(p['fobU'], p['fxShare'], p['fxRef'], 7.7), at['fobU'], delta=0.00015)

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


# ── contracts C11, C12 and C14 (Sep 15): cost model, duty regime by customer, history by customer ──
# Sentinel values only: import multipliers 1.37 and 1.53, royalty 6.5, revenue costs 2.25, 1.75 and 0.8.
MULTS = {'natural': 1.37, 'synthetic': 1.53}
REV_ITEMS = [{'key': 'warehouse', 'name': 'Warehouse', 'pct': 2.25},
             {'key': 'factoring', 'name': 'Factoring', 'pct': 1.75},
             {'key': 'rent', 'name': 'Rent', 'pct': 0.8}]
C11 = {'landed': {'mode': 'multiplier', 'multiplier': dict(MULTS)}, 'royalty': {'defaultPct': 6.5},
       'revenueCosts': {'items': REV_ITEMS}}
BY_CUSTOMER_FIELDS = ['cust', 'base', 'brand', 'cat', 'fiber', 'origin', 'units', 'rev', 'fobU', 'fob', 'duty',
                      'freight', 'fees', 'cogs', 'deduct', 'net', 'gp', 'royalty', 'revCost', 'contrib', 'level',
                      'grade', 'dutyRegime', 'fxShare', 'fxRef']


class FiberClassC11(unittest.TestCase):
    def test_fiber_groups_map_to_two_classes(self):
        for f in ('cotton', 'linen', 'wool', ' Cotton '):
            self.assertEqual(E.fiber_class(f), 'natural', f)
        for f in ('mmf', 'silk', '', None, 'rayon'):
            self.assertEqual(E.fiber_class(f), 'synthetic', f)

    def test_predominant_fiber(self):
        cases = [({'Cotton': 50, 'Polyester': 45, 'Spandex': 5}, 'cotton'),     # the largest share wins
                 ({'polyester': 60, 'cotton': 40}, 'mmf'),
                 ({'cotton': 50, 'polyester': 50}, 'cotton'),                     # a tie counts as natural
                 ({'rayon': 55, 'linen': 45}, 'mmf'),                             # rayon is synthetic
                 ({'viscose': 50, 'linen': 50}, 'linen'),
                 ({'lyocell': 70, 'wool': 30}, 'mmf'),
                 ({'modal': 40, 'tencel': 30, 'cotton': 30}, 'mmf'),
                 ({'spandex': 60, 'cotton': 40}, 'mmf'),                          # spandex decides only on its own
                 ({'elastane': 50, 'wool': 50}, 'wool'),
                 ({'spandex': 50, 'polyester': 50}, 'mmf'),
                 ([('organic cotton', 70), ('recycled polyester', 30)], 'cotton'),
                 ({'merino wool': 80, 'nylon': 20}, 'wool'),
                 ({}, None), ({'cotton': 'x'}, None), ('cotton', None)]
        for content, want in cases:
            self.assertEqual(E.fiber_group_of(content), want, content)
        self.assertEqual(E.fiber_class({'Cotton': 50, 'Polyester': 45, 'Spandex': 5}), 'natural')
        self.assertEqual(E.fiber_class({'polyester': 95, 'spandex': 5}), 'synthetic')


class CostModelC11(unittest.TestCase):
    S = E.merge_settings(C11, PARAMS)
    S0 = E.merge_settings({}, PARAMS)

    def test_multiplier_adders_by_fiber_class_and_regime(self):
        S = self.S
        nat, syn = (E.r2(100.0 * (1.37 - 1)), 0.0, 0.0), (E.r2(100.0 * (1.53 - 1)), 0.0, 0.0)
        self.assertEqual(E.adders(100.0, 10, 'dress_shirt', 'cotton', 'CN', S, 'us'), nat)
        self.assertEqual(E.adders(100.0, 10, 'dress_shirt', 'mmf', 'CN', S, 'us'), syn)
        self.assertEqual(E.adders(100.0, 10, 'polo', 'mmf', 'BD', S, 'ca'), syn)          # Canada: the same multiplier
        self.assertEqual(E.adders(100.0, 10, 'pants', 'linen', 'CN', S, 'ca'), nat)
        self.assertEqual(E.adders(100.0, 10, 'dress_shirt', 'cotton', 'CN', S, 'none'), (0.0, 0.0, 0.0))
        self.assertEqual(E.adders(100.0, 10, 'dress_shirt', None, 'CN', S, 'us'), syn)     # unknown fiber: synthetic
        duty = E.r2(123.45 * (1.37 - 1))
        self.assertEqual(E.stock_money(123.45, 10, 'blazer', 'wool', 'CN', 'us', S),
                         (E.r2(123.45 + duty), E.r4(E.r2(123.45 + duty) / 10)))
        du = E.r4(2.2222 * (1.53 - 1))
        self.assertEqual(E.landed(2.2222, {'cat': 'polo', 'fiber': 'mmf'}, S, 'ca', 'CN'),
                         {'duty': du, 'freight': 0.0, 'fees': 0.0, 'landedU': E.r4(2.2222 + du)})
        self.assertEqual(E.landed(2.2222, {'cat': 'polo', 'fiber': 'mmf'}, S, 'none', 'CN')['landedU'], 2.2222)
        raw = 2.22224999                                          # an unrounded style cost: rounded first (C2)
        du = E.r4(E.r4(raw) * (1.53 - 1))
        self.assertEqual(E.landed(raw, {'cat': 'polo', 'fiber': 'mmf'}, S, 'us', 'CN')['landedU'], E.r4(E.r4(raw) + du))
        self.assertEqual(E.adders(100.0, 10, 'dress_shirt', 'mmf', 'CN', self.S0, 'us'),          # itemized: today's math
                         (E.r2(100.0 * (25.9 + 20.0) / 100 + 10 * 0.07), E.r2(10 * 0.285),
                          E.r2(100.0 * (0.3464 + 0.125) / 100 + 10 * 0.092)))

    def test_royalty_base_revenue_costs_and_contribution(self):
        S = self.S
        m = E.line_money(1000.0, 400.0, 100, 'dress_shirt', 'cotton', 'CN', 'us', 7.0, 6.5, S)
        duty = E.r2(400.0 * (1.37 - 1))
        cogs = E.r2(400.0 + duty)
        net = E.r2(1000.0 - E.r2(1000.0 * 7.0 / 100))
        gp = E.r2(net - cogs)
        roy = E.r2(net * 6.5 / 100)
        rc = E.r2(1000.0 * (2.25 + 1.75 + 0.8) / 100)
        self.assertEqual(E.revenue_cost_pct(S), 2.25 + 1.75 + 0.8)
        self.assertEqual((m['duty'], m['freight'], m['fees'], m['cogs'], m['net'], m['gp'], m['royalty'], m['revCost'],
                          m['contrib']), (duty, 0.0, 0.0, cogs, net, gp, roy, rc, E.r2(gp - roy - rc)))
        R = E.merge_settings(dict(C11, royalty={'defaultPct': 6.5, 'base': 'revenue'}), PARAMS)
        self.assertEqual(E.royalty_base(R), 'revenue')
        r = E.line_money(1000.0, 400.0, 100, 'dress_shirt', 'cotton', 'CN', 'us', 7.0, 6.5, R)
        self.assertEqual((r['royalty'], r['contrib']), (E.r2(1000.0 * 6.5 / 100), E.r2(gp - E.r2(1000.0 * 6.5 / 100) - rc)))
        u = E.line_money(1000.0, None, 100, 'dress_shirt', 'cotton', 'CN', 'us', 7.0, 6.5, S)          # no cost
        self.assertEqual((u['net'], u['revCost'], u['royalty'], u['contrib']), (net, rc, None, None))
        n = E.line_money(None, 400.0, 100, 'dress_shirt', 'cotton', 'CN', 'us', 7.0, 6.5, S)           # no price
        self.assertEqual((n['cogs'], n['revCost'], n['contrib']), (cogs, None, None))

    def test_defaults_keep_todays_money(self):
        S0 = self.S0
        self.assertEqual(S0['landed'], {'mode': 'itemized', 'multiplier': {'natural': None, 'synthetic': None}})
        self.assertEqual((E.revenue_cost_pct(S0), E.royalty_base(S0), S0['regimeByCustomer']), (0.0, 'net', {}))
        m = E.line_money(1000.0, 400.0, 100, 'dress_shirt', 'mmf', 'CN', 'us', 7.0, 10.0, S0)
        self.assertEqual((m['revCost'], m['contrib']), (0.0, E.r2(m['gp'] - m['royalty'])))
        base = build(src_dest())
        same = build(src_dest(), settings={'landed': {'mode': 'itemized', 'multiplier': dict(MULTS)},
                                           'royalty': {'base': 'net'}, 'regimeByCustomer': {},
                                           'revenueCosts': {'items': [dict(it, pct=0) for it in REV_ITEMS]}})
        for t in ('lines', 'alloc', 'apo', 'inventory', 'production', 'styles'):
            self.assertEqual(base[t], same[t], t)
        for k in ('company', 'byStyle', 'byCustomer'):
            self.assertEqual(base['shipped'][k], same['shipped'][k], k)
        self.assertEqual(base['totals'], same['totals'])

    def test_missing_multiplier_keeps_itemized_math_and_alerts(self):
        half = {'landed': {'mode': 'multiplier', 'multiplier': {'natural': 1.37, 'synthetic': None}}}
        S = E.merge_settings(half, PARAMS)
        self.assertEqual(E.adders(100.0, 10, 'dress_shirt', 'mmf', 'CN', S, 'us'),
                         E.adders(100.0, 10, 'dress_shirt', 'mmf', 'CN', self.S0, 'us'))
        self.assertEqual(E.adders(100.0, 10, 'dress_shirt', 'cotton', 'CN', S, 'us'), (E.r2(100.0 * (1.37 - 1)), 0.0, 0.0))
        ds, base = build(settings=half), build()
        self.assertEqual(by(ds, 'lines', 'id')['1|ROQAQF201SLS']['duty'], by(base, 'lines', 'id')['1|ROQAQF201SLS']['duty'])
        a = kinds_of(ds)['settings_incomplete']
        self.assertEqual((a['severity'], a['unit'], a['valueLabel'], a['value'], a['count'], a['missing']),
                         ('high', 'settings', None, None, 1, ['synthetic']))
        self.assertIn('ROQAQF201SLS', a['refs']['styles'])
        self.assertIn('synthetic fibers', a['detail'])
        self.assertNotRegex(a['title'] + a['detail'], '[–—]')
        self.assertIn('settings_incomplete', ds['dict']['alertKinds'])
        self.assertNotIn('settings_incomplete', kinds_of(build(settings=C11)))           # both multipliers set
        self.assertNotIn('settings_incomplete', kinds_of(base))                           # itemized mode
        none_set = kinds_of(build(settings={'landed': {'mode': 'multiplier'}}))['settings_incomplete']
        self.assertEqual(none_set['missing'], ['synthetic'])      # only the classes the rows need are named

    def test_dataset_in_multiplier_mode(self):
        ds = build(src_dest(), settings=C11)
        S = ds['settings']
        seen = set()
        for t in ('lines', 'apo'):
            for r in rows(ds, t):
                if r['fob'] is None:
                    continue
                if r['dutyRegime'] == 'none':
                    self.assertEqual((r['duty'], r['freight'], r['fees']), (0.0, 0.0, 0.0))
                else:
                    d = E.r2(r['fob'] * (MULTS[E.fiber_class(r['fiber'])] - 1))
                    self.assertEqual((r['duty'], r['freight'], r['fees'], r['cogs']), (d, 0.0, 0.0, E.r2(r['fob'] + d)),
                                     (t, r['id']))
                    seen.add(r['dutyRegime'])
                if r['rev'] is None:
                    continue
                self.assertEqual(r['revCost'], E.r2(r['rev'] * rc_pct(S) / 100))
                self.assertEqual(r['contrib'], E.r2(r['gp'] - r['royalty'] - r['revCost']))
                for k, v in client_recalc(r, S, ds['dict'], t).items():
                    self.assertEqual(r[k], v, (t, r['id'], k))
        self.assertEqual(seen, {'us', 'ca'})
        for t in ('inventory', 'production'):
            for r in rows(ds, t):
                if r['fob'] is not None:
                    d = 0.0 if r['dutyRegime'] == 'none' else E.r2(r['fob'] * (MULTS[E.fiber_class(r['fiber'])] - 1))
                    self.assertEqual(r['landed'], E.r2(r['fob'] + d), (t, r.get('sku') or r.get('ref')))
        for s in rows(ds, 'styles'):
            if s['fobU'] is not None and s['dutyRegime'] != 'none':
                du = E.r4(s['fobU'] * (MULTS[E.fiber_class(s['fiber'])] - 1))
                self.assertEqual(s['landedU'], E.r4(s['fobU'] + du), s['base'])
        self.assertEqual((ds['dict']['dutyLabel'], build()['dict']['dutyLabel']),
                         ('Import (tariffs, freight and fees)', 'Duty and tariffs'))
        self.assertIn('Import costs (tariffs, freight and fees)', ' '.join(ds['notes']))
        for n in ds['notes']:
            self.assertNotRegex(n, '[–—]')

    def test_summaries_carry_revenue_costs(self):
        ds = build(settings=C11)
        L = rows(ds, 'lines')
        a2 = [r for r in L if r['type'] == 'a2000']
        ob = ds['totals']['openBook']
        self.assertEqual(ob['revCost'], E.r2(sum(r['revCost'] or 0.0 for r in a2)))
        for part in ('openBook', 'bulk', 'apo'):
            t = ds['totals'][part]
            self.assertAlmostEqual(t['contrib'], t['gp'] - t['royalty'] - (t['revCost'] - t['uncostedRevCost']), places=2)
        self.assertIn('revCost', ds['totals']['apo']['coveredByBulk'])
        byid = {r['id']: r for r in L}
        for a in rows(ds, 'alloc'):
            ln = byid[a['line']]
            want = E.r2(ln['revCost'] * a['units'] / ln['units']) if ln['revCost'] is not None and ln['units'] > 0 else None
            self.assertEqual(a['revCost'], want)
        st = by(ds, 'styles', 'base')['ROQAQF201SLS']
        mine = [r for r in a2 if r['base'] == 'ROQAQF201SLS']
        self.assertEqual(st['openRevCost'], E.r2(sum(r['revCost'] for r in mine)))
        self.assertEqual(st['openContrib'], E.r2(sum(r['contrib'] or 0.0 for r in mine)))
        comp = rows({'c': ds['shipped']['company']}, 'c')
        by_st = rows({'b': ds['shipped']['byStyle']}, 'b')
        for c in comp:
            if c['rev']:
                self.assertEqual(c['contrib'], E.r2(c['gp'] - c['royalty'] - c['revCost']), c['month'])
                want = sum(b['months'][c['month']][1] * rc_pct(ds['settings']) / 100 for b in by_st
                           if c['month'] in b['months'] and b['fobU'] is not None)
                self.assertAlmostEqual(c['revCost'], want, places=2)
        rv = build(settings=dict(C11, royalty={'defaultPct': 6.5, 'base': 'revenue'}))
        jan = [c for c in rows({'c': rv['shipped']['company']}, 'c') if c['month'] == '2026-01'][0]
        self.assertEqual(jan['royalty'], E.r2(300.0 * 6.5 / 100))                        # on revenue, before chargebacks
        self.assertIn('Royalty is a percent of revenue, before chargebacks.', rv['notes'])

    def test_assumptions_follow_the_cost_model(self):
        blocks = kinds_of(build(settings=C11))['assumption']['refs']['blocks']
        self.assertTrue({'landed', 'revenueCosts', 'destinations'} <= set(blocks))
        self.assertFalse({'tariff', 'freight'} & set(blocks))           # the multipliers replace them
        itemized = kinds_of(build())['assumption']['refs']['blocks']
        self.assertTrue({'tariff', 'freight', 'landed', 'revenueCosts'} <= set(itemized))
        conf = build(settings=dict(C11, confirmed={'landed': True, 'revenueCosts': True}))
        self.assertFalse({'landed', 'revenueCosts'} & set(kinds_of(conf)['assumption']['refs']['blocks']))

    def test_settings_merge_backstop(self):
        S = E.merge_settings({'landed': {'mode': 'bogus', 'multiplier': {'natural': 0.5, 'synthetic': '1.53', 'x': 2}},
                              'royalty': {'base': 'gross'},
                              'revenueCosts': {'items': [{'key': 'a', 'name': 'A', 'pct': None}, {'key': 'b', 'pct': 99},
                                                         'junk', {'key': 'c', 'pct': 2.25}]},
                              'costRule': {'mode': 'median', 'wideSpreadPct': -1},
                              'history': {'caveat': '  Synthetic caveat.  '},
                              'regimeByCustomer': {'zzpeer': 'US', 'zz': 'mars', 'yy': None, ' ': 'us'}})
        self.assertEqual(S['landed'], {'mode': 'itemized', 'multiplier': {'natural': None, 'synthetic': None}})
        self.assertEqual(S['royalty']['base'], 'net')
        self.assertEqual(S['revenueCosts']['items'], [{'key': 'a', 'name': 'A', 'pct': 0}, {'key': 'b', 'name': 'b', 'pct': 0},
                                                      {'key': 'c', 'name': 'c', 'pct': 2.25}])
        self.assertEqual(E.revenue_cost_pct(S), 2.25)
        self.assertEqual((S['costRule'], S['history']), ({'mode': 'combined', 'wideSpreadPct': 10},
                                                         {'caveat': 'Synthetic caveat.'}))
        self.assertEqual(S['regimeByCustomer'], {'ZZPEER': 'us'})
        N = E.merge_settings({'landed': None, 'revenueCosts': None, 'regimeByCustomer': None, 'costRule': None,
                              'history': None, 'royalty': {'base': None}})
        for k in ('landed', 'revenueCosts', 'regimeByCustomer', 'costRule', 'history'):
            self.assertEqual(N[k], E.DEFAULT_SETTINGS[k], k)
        self.assertEqual(N['royalty']['base'], 'net')
        P = E.merge_settings({'landed': {'mode': 'multiplier', 'multiplier': {'natural': 1.37}}, 'revenueCosts': {'items': []}})
        self.assertEqual(P['landed']['multiplier'], {'natural': 1.37, 'synthetic': None})
        self.assertEqual((P['revenueCosts']['items'], E.revenue_cost_pct(P)), ([], 0.0))   # a saved list replaces the default

    def test_new_defaults_are_public(self):
        d = E.DEFAULT_SETTINGS
        self.assertEqual(d['landed'], {'mode': 'itemized', 'multiplier': {'natural': None, 'synthetic': None}})
        self.assertEqual(d['royalty']['base'], 'net')
        self.assertEqual([(i['key'], i['name'], i['pct']) for i in d['revenueCosts']['items']],
                         [('warehouse', 'Warehouse', 0), ('factoring', 'R&R factoring', 0), ('rent', 'Rent', 0)])
        self.assertEqual((d['regimeByCustomer'], d['costRule'], d['history']),
                         ({}, {'mode': 'combined', 'wideSpreadPct': 10}, {'caveat': None}))
        self.assertEqual((d['confirmed']['landed'], d['confirmed']['revenueCosts']), (False, False))
        ds = build()
        for k in ('landed', 'revenueCosts', 'regimeByCustomer', 'costRule', 'history'):
            self.assertEqual(ds['settings'][k], d[k], k)                    # echoed in the dataset


class RegimeByCustomerC12(unittest.TestCase):
    def test_customer_setting_comes_first(self):
        ds = build(src_dest(), settings=dict(C11, regimeByCustomer={'peer': 'us', 'FOBX': 'ca', 'ZZCA': 'us'},
                                             destinations={'WALM': 'us'}))
        L = by(ds, 'lines', 'id')
        ch = L['9|ROQAQF201SLS']                               # a Peerless-like account on factory direct (CH)
        self.assertEqual((ch['dutyRegime'], ch['fobLine']), ('us', 1))       # still in the FOB segment filter
        self.assertEqual((ch['duty'], ch['freight'], ch['fees']), (E.r2(ch['fob'] * (1.53 - 1)), 0.0, 0.0))
        self.assertGreater(ch['duty'], 0)
        self.assertIn('customer_regime', ch['flags'])
        self.assertFalse({'fob_line', 'non_us_dest'} & set(ch['flags']))
        fx = L['3|ROQAQF203SLS']                               # an FOB customer set to Canada
        self.assertEqual((fx['dutyRegime'], fx['duty']), ('ca', E.r2(fx['fob'] * (1.53 - 1))))
        self.assertEqual(L['8|ROQAQF211SLS']['dutyRegime'], 'us')             # beats the AE destination
        self.assertEqual(L['1|ROQAQF201SLS']['dutyRegime'], 'us')
        self.assertNotIn('customer_regime', L['1|ROQAQF201SLS']['flags'])
        peer = [r for r in rows(ds, 'apo') if r['cust'] == 'PEER'][0]
        self.assertEqual(peer['dutyRegime'], 'us')
        self.assertIn('customer_regime', peer['flags'])
        self.assertNotIn('regime_from_orders', peer['flags'])
        self.assertEqual(by(ds, 'production', 'ref')['TT26011']['dutyRegime'], 'us')   # stock rows: destinations only
        self.assertEqual([r['dutyRegime'] for r in rows(ds, 'inventory') if r['wh'] == 'ABFI'], ['ca'])
        for t in ('lines', 'apo'):
            for r in rows(ds, t):
                if r['fob'] is not None and r['rev'] is not None:
                    for k, v in client_recalc(r, ds['settings'], ds['dict'], t).items():
                        self.assertEqual(r[k], v, (t, r['id'], k))
        for f in ds['dict']['flags']:
            self.assertNotRegex(ds['dict']['flags'][f], '[–—]')
        base = by(build(src_dest()), 'lines', 'id')                           # no setting: as before
        self.assertEqual((base['9|ROQAQF201SLS']['dutyRegime'], base['3|ROQAQF203SLS']['dutyRegime']), ('none', 'none'))

    def test_shipped_history_follows_the_customer_setting(self):
        def shipped(settings):
            ds = build(settings=settings)
            return by({'b': ds['shipped']['byStyle']}, 'b', 'base')['ROQAQF201SLS'], rows({'c': ds['shipped']['company']}, 'c')
        st, _ = shipped({})
        self.assertEqual((st['fobShare'], st['caShare']), (0.2, 0.0))          # FOBX is an FOB customer
        st, _ = shipped({'regimeByCustomer': {'FOBX': 'us'}})
        self.assertEqual((st['fobShare'], st['caShare']), (0.0, 0.0))
        st, comp = shipped(dict(C11, regimeByCustomer={'ROSS': 'ca'}))
        self.assertEqual((st['fobShare'], st['caShare']), (0.2, 0.8))
        S = E.merge_settings(C11, PARAMS)
        fob = E.r2(30 * st['fobU'])
        du = E.adders(fob, 30, st['cat'], st['fiber'], st['origin'], S, 'us')[0]
        dc = E.adders(fob, 30, st['cat'], st['fiber'], st['origin'], S, 'ca')[0]
        jan = [c for c in comp if c['month'] == '2026-01'][0]
        self.assertEqual(jan['duty'], E.r2(du * (1 - 0.2 - 0.8) + dc * 0.8))


class ShippedByCustomerC14(unittest.TestCase):
    @staticmethod
    def src_hist():
        s = src()
        s['sales_analytics']['styles'][0][7] = {'KOHLSDROP': [4, 40.0], 'KOHL': [6, 66.0], 'FOBX': [10, 100.0],
                                                'AMAZON_DROP': [2, 30.0], 'ROSS': [0, 0.0], 'TJMX': [-3, -30.0]}
        s['sales_analytics']['styles'].append(['ZZLEGACY9', 'SYN', 7, 70.0, '2025-01-05', '2026-02-10',
                                               {'2026-01': [7, 70.0, 0, 0.0]}, {'ROSS1': [7, 70.0]}, 'RED'])
        return s

    def test_rows_fold_history_codes_and_use_line_math(self):
        ds = build(self.src_hist(), settings=C11)
        bc = ds['shipped']['byCustomer']
        self.assertEqual(bc['fields'][:len(BY_CUSTOMER_FIELDS)], BY_CUSTOMER_FIELDS)
        R = {(r['cust'], r['base']): r for r in rows({'b': bc}, 'b')}
        self.assertEqual(set(R), {('KOHL', 'ROQAQF201SLS'), ('FOBX', 'ROQAQF201SLS'), ('AMAZ', 'ROQAQF201SLS'),
                                  ('ROSS', 'ZZLEGACY9')})                     # units > 0 only
        S = ds['settings']
        k = R[('KOHL', 'ROQAQF201SLS')]                                        # KOHLSDROP folds into KOHL
        self.assertEqual((k['units'], k['rev'], k['pieces'], k['dutyRegime']), (10, 106.0, 10, 'us'))
        st = by({'b': ds['shipped']['byStyle']}, 'b', 'base')['ROQAQF201SLS']
        self.assertEqual((k['fobU'], k['level'], k['grade'], k['origin'], k['fxShare']),
                         (st['fobU'], st['level'], st['grade'], st['origin'], st['fxShare']))
        want = E.line_money(106.0, E.r2(10 * k['fobU']), 10, k['cat'], k['fiber'], k['origin'], 'us',
                            E._ded_pct('KOHL', S, 'department'), E._roy_pct(k['brand'], S), S)
        for f, v in want.items():
            self.assertEqual(k[f], v, f)
        self.assertEqual((k['duty'], k['freight']), (E.r2(k['fob'] * (1.53 - 1)), 0.0))
        f = R[('FOBX', 'ROQAQF201SLS')]
        self.assertEqual((f['dutyRegime'], f['duty'], f['freight'], f['fees']), ('none', 0.0, 0.0, 0.0))
        u = R[('ROSS', 'ZZLEGACY9')]                                           # ROSS1 folds into ROSS; no cost
        self.assertEqual((u['fob'], u['cogs'], u['gp'], u['royalty'], u['contrib'], u['level']),
                         (None, None, None, None, None, 'L7'))
        self.assertEqual((u['deduct'], u['revCost']), (E.r2(70.0 * 1.0 / 100), E.r2(70.0 * rc_pct(S) / 100)))
        self.assertIn('AMAZ', ds['dict']['customers'])                         # a history-only account is named
        self.assertEqual(ds['shipped']['range'], {'from': '2025-01-01', 'to': '2026-02-20',
                                                  'ingestedAt': '2026-02-21T00:00:00', 'byDiv': None})
        ca = {(r['cust'], r['base']): r for r in rows({'b': build(self.src_hist(), settings=dict(
            C11, regimeByCustomer={'KOHL': 'ca'}))['shipped']['byCustomer']}, 'b')}
        self.assertEqual(ca[('KOHL', 'ROQAQF201SLS')]['dutyRegime'], 'ca')

    def test_history_not_loaded(self):
        b2 = build(src(analytics=False))
        self.assertEqual((b2['shipped']['byCustomer']['rows'], b2['shipped']['byCustomer']['fields'][:3]),
                         ([], ['cust', 'base', 'brand']))
        self.assertEqual(b2['shipped']['range'], {'from': None, 'to': None, 'ingestedAt': None, 'byDiv': None})


# ── contract C13 (Sep 15): one cost per style, from every factory's direct quote ──
# Sentinel prices only. Grid order: factory TT reads grid GY first; every other factory reads grid GN first.
LO, AT10, OVER10 = 2.8282, 3.11102, 3.1111            # AT10 is exactly LO x 1.1; OVER10 is just above it
C13_EXTRA = [
    rec('AA!J8', 'PC', 'ref_price_list', 7.1111, factory_code='AA', production_ref='AA26003', style='ROQAQF101SLS',
        fabric_codes=['QF'], scope='ref_style'),
    rec('AA!J5', 'PC', 'ref_price_list', 6.1616, factory_code='AA', production_ref='AA26004', style='ROQAQG701SLS',
        fabric_codes=['QG'], scope='ref_style'),
    rec('AA!J6', 'PC', 'ref_price_list', 3.3434, factory_code='AA', production_ref='AA26005', style='ROQAQE702SLS',
        fabric_codes=['QE'], scope='ref_style'),
    rec('AA!J7', 'PC', 'ref_price_list', 5.3535, factory_code='AA', production_ref='AA26006', style='ROQAQD703SLS',
        fabric_codes=['QD'], scope='ref_style'),
    rec('CC!G3', 'KY', 'ref_price_list', 2.9393, factory_code='CC', production_ref_resolved='CC26001', pattern='SOLID',
        fit_class='REGULAR', customer_group='ROSS', fabric_codes=['QS'], scope='ref'),
    calc('GN!T1', 'GN:BASE', LO, 'QA', ['QT'], 'SLIM', 'SOLID'), calc('GY!T1', 'GY:BASE', AT10, 'QA', ['QT'], 'SLIM', 'SOLID'),
    calc('GN!W1', 'GN:BASE', LO, 'QA', ['QW'], 'SLIM', 'SOLID'), calc('GY!W1', 'GY:BASE', OVER10, 'QA', ['QW'], 'SLIM', 'SOLID'),
    calc('GN!G1', 'GN:BASE', 6.4646, 'QA', ['QG'], 'SLIM', 'SOLID'),
    calc('GN!E1', 'GN:BASE', 4.3434, 'QA', ['QE'], 'SLIM', 'SOLID'),
    calc('GN!D1', 'GN:BASE', 3.5353, 'QA', ['QD'], 'SLIM', 'SOLID'),
    calc('GN!R1', 'GN:BASE', 4.2323, 'QA', ['QR'], 'SLIM', 'SOLID', e1=R2),
    calc('GY!R1', 'GY:BASE', 4.5454, 'QA', ['QR'], 'SLIM', 'SOLID', e1=FXB)]
C13_BOOK = basis_book(C13_EXTRA)
CASCADE = {'costRule': {'mode': 'cascade'}}


def c13(ledger=(), settings=None, overrides=None):
    return ci(ledger, settings, overrides, costbook=C13_BOOK)


def src_c13():
    """src() with ROQAQF201SLS also made on a second factory's ref (NN26009), a tailored style on a KinYun-type
    ref, a style on a list factory's unlisted ref (a fallback) and a style on a list and a grid factory."""
    s = src()
    s['ledger']['rows'] = s['ledger']['rows'] + [
        led('NN26009', 'ROQAQF201SLS', 400, etd='2026-04-01'), led('CC26001', 'ROQAQS803TFS', 200),
        led('AA26009', 'ROQAQF605SLS', 120), led('AA26004', 'ROQAQG701SLS', 150), led('NN26003', 'ROQAQG701SLS', 150)]
    return s


def style_rows(ds, base):
    """(table, row) for every row of one base style: lines, apo, stock, production, styles, the line's
    allocations and both shipped tables."""
    out = [(t, r) for t in ('lines', 'apo', 'inventory', 'production', 'styles') for r in rows(ds, t) if r['base'] == base]
    ids = {r['id'] for t, r in out if t == 'lines'}
    out += [('alloc', r) for r in rows(ds, 'alloc') if r['line'] in ids]
    out += [('byStyle', r) for r in rows({'b': ds['shipped']['byStyle']}, 'b') if r['base'] == base]
    out += [('byCustomer', r) for r in rows({'b': ds['shipped']['byCustomer']}, 'b') if r['base'] == base]
    return out


class CombinedCostC13(unittest.TestCase):
    def test_single_quotes(self):
        c = c13([led('NN26001', 'ROQAQF601SLS', 100), led('BB26001', 'ROQAQK105SLS', 50),
                 led('BB26099', 'ROQAQK106SLS', 50), led('CC26009', 'ROQAQF804SLS', 50)])
        g = c.style_quote('ROQAQF601SLS')                         # one factory, priced by its own grid
        self.assertEqual((g['rule'], g['fobU'], g['level'], g['grade'], g['spread'], g['fxShare'], g['fxRef']),
                         ('single', 4.4444, 'L4a', 'B', 0.0, 1.0, FXB))
        self.assertEqual(g['costByFactory'], [['NN', 'GN', 'S', 4.4444, 'L4a']])
        p = c.style_quote('TMQAQF102SLS')                         # a list names the exact style (no ledger ref needed)
        self.assertEqual((p['rule'], p['fobU'], p['level'], p['grade'], p['fxShare'], p['fxRef']),
                         ('single', 6.6666, 'L1', 'A', 0.0, None))
        self.assertEqual(p['costByFactory'], [['AA', 'PC', 'AA26002', 6.6666, 'L1']])
        m = c.style_quote('ROQAQF101SLS')                         # one factory on two refs: its lowest quote (C15 R7)
        self.assertEqual((m['rule'], m['fobU'], m['spread'], m['rangeLo'], m['rangeHi'], m['evidence']),
                         ('single', 7.1111, 0.0, 7.1111, 7.7777, ['AA!J8']))
        self.assertIn('ambiguous_rows', m['flags'])
        self.assertEqual(c.combined('ROQAQF101SLS')['res']['skip'], (('AA!J2', 'tie_not_lowest'),))
        self.assertEqual([q[2] for q in m['costByFactory']], ['AA26003', 'AA26001'])          # sorted by price
        d = c.style_quote('ROQAQK105SLS')                         # ref and pattern list, for a ref carrying the style
        self.assertEqual((d['rule'], d['fobU'], d['level'], d['costByFactory']),
                         ('single', 3.3333, 'L2', [['BB', 'DP', 'BB26001', 3.3333, 'L2']]))
        self.assertIsNone(c.style_quote('ROQAQK106SLS'))          # ref not on the list: a sibling rate is no quote
        k = c.style_quote('ROQAQF804SLS')                         # a list factory whose list does not answer: its grid
        self.assertEqual(k['costByFactory'], [['CC', 'GN', 'S', 4.4444, 'L4a']])
        u = c.style_quote('ROQAQF609SLS')                         # no factory known: the default grid order
        self.assertEqual((u['rule'], u['fobU'], u['costByFactory']), ('single', 4.4444, [['UNKNOWN', 'GN', 'S', 4.4444, 'L4a']]))
        self.assertTrue(c.combined('ROQAQF609SLS')['res']['basis'].startswith('No factory is known for this style.'))

    def test_average_threshold_and_lowest(self):
        def both(st):
            return [led('TT26001', st, 100), led('NN26002', st, 100)]
        c = c13(both('ROQAQF602SLS') + both('ROQAQT603SLS') + both('ROQAQW604SLS'))
        a = c.style_quote('ROQAQF602SLS')
        self.assertEqual((a['rule'], a['fobU'], a['level'], a['grade'], a['spread']),
                         ('average', E.r4((4.1717 + 4.4444) / 2), 'L4a', 'B', E.r4(4.4444 / 4.1717 - 1)))
        self.assertEqual(a['costByFactory'], [['TT', 'GY', 'S', 4.1717, 'L4a'], ['NN', 'GN', 'S', 4.4444, 'L4a']])
        self.assertTrue({'cost_average', 'price_conflict'} <= set(a['flags']))
        self.assertEqual((a['rangeLo'], a['rangeHi'], sorted(a['evidence'])), (4.1717, 4.4444, ['GN!F1', 'GY!F1']))
        t = c.style_quote('ROQAQT603SLS')                         # exactly 10 percent apart: still the average
        self.assertEqual((t['rule'], t['fobU'], t['spread']), ('average', E.r4((LO + AT10) / 2), 0.1))
        w = c.style_quote('ROQAQW604SLS')                         # just above: the lowest, with its own evidence
        self.assertEqual((w['rule'], w['fobU'], w['level'], w['rangeLo'], w['rangeHi'], w['evidence']),
                         ('lowest', LO, 'L4a', LO, OVER10, ['GN!W1']))
        self.assertIn('cost_lowest', w['flags'])
        self.assertIn('Lowest of 2 factory quotes', c.combined('ROQAQW604SLS')['res']['basis'])
        narrow = c13(both('ROQAQF602SLS'), settings={'costRule': {'wideSpreadPct': 5}}).style_quote('ROQAQF602SLS')
        self.assertEqual((narrow['rule'], narrow['fobU']), ('lowest', 4.1717))
        wide = c13(both('ROQAQW604SLS'), settings={'costRule': {'wideSpreadPct': 12.5}}).style_quote('ROQAQW604SLS')
        self.assertEqual(wide['rule'], 'average')
        for text in (c.combined(s)['res']['basis'] for s in ('ROQAQF602SLS', 'ROQAQW604SLS')):
            self.assertNotRegex(text, '[–—]')

    def test_grade_rule(self):
        c = c13([led('AA26004', 'ROQAQG701SLS', 100), led('NN26003', 'ROQAQG701SLS', 100),
                 led('AA26005', 'ROQAQE702SLS', 100), led('NN26003', 'ROQAQE702SLS', 100),
                 led('AA26006', 'ROQAQD703SLS', 100), led('NN26003', 'ROQAQD703SLS', 100)])
        a = c.style_quote('ROQAQG701SLS')        # a list price (grade A) and a calculator price (grade B), averaged
        self.assertEqual((a['rule'], a['level'], a['grade'], a['fobU']),
                         ('average', 'L1', 'B', E.r4((6.1616 + 6.4646) / 2)))           # best level, worst grade
        lo_list = c.style_quote('ROQAQE702SLS')  # far apart, the list is lower: its level and grade
        self.assertEqual((lo_list['rule'], lo_list['level'], lo_list['grade'], lo_list['fobU']), ('lowest', 'L1', 'A', 3.3434))
        lo_grid = c.style_quote('ROQAQD703SLS')  # far apart, the calculator is lower: its level and grade
        self.assertEqual((lo_grid['rule'], lo_grid['level'], lo_grid['grade'], lo_grid['fobU']), ('lowest', 'L4a', 'B', 3.5353))
        ds = build(src_c13(), cb=C13_BOOK)
        for r in rows(ds, 'production'):
            if r['base'] == 'ROQAQG701SLS':                       # both rows carry the combined grade
                self.assertEqual((r['level'], r['grade'], r['fobU']), ('L1', 'B', a['fobU']), r['ref'])

    def test_value_weighted_fx_ref_and_the_what_if(self):
        both = [led('TT26001', 'ROQAQR705SLS', 100), led('NN26002', 'ROQAQR705SLS', 100)]
        tt, nn = 4.5454, 4.2323                   # TT's grid prints at FXB, NN's at R2
        q = c13(both).style_quote('ROQAQR705SLS')
        self.assertEqual((q['rule'], q['fobU'], q['fxShare'], q['fxRef']),
                         ('average', E.r4((tt + nn) / 2), 1.0, E.r4((tt * FXB + nn * R2) / (tt + nn))))
        at = c13(both, settings={'fx': {'rate': 7.7}}).style_quote('ROQAQR705SLS')
        exact = (tt * FXB / 7.7 + nn * R2 / 7.7) / 2
        self.assertEqual((at['rule'], at['fobU'], at['fxRef']), ('average', E.r4(exact), 7.7))
        self.assertAlmostEqual(E.fx_what_if_unit(q['fobU'], q['fxShare'], q['fxRef'], 7.7), at['fobU'], delta=0.00015)
        med, ref = (tt + nn) / 2, (tt * FXB + nn * R2) / (tt + nn)
        self.assertAlmostEqual(med * ref / 7.7, exact, places=12)          # exact before the 4-decimal rounding

    def test_manual_cost_wins_outright(self):
        base = {'reason': 'synthetic', 'effective': '', 'by': 'x@example.com', 'at': '2026-01-01'}
        ov = [dict(base, id='o1', scope='style', key={'style': 'ROQAQF201SLS'}, fobU=9.2222),
              dict(base, id='o2', scope='ref', key={'ref': 'NN26003'}, fobU=9.3333)]
        ds = build(src_c13(), cb=C13_BOOK, overrides=ov)
        seen = style_rows(ds, 'ROQAQF201SLS')
        self.assertTrue(seen)
        for t, r in seen:
            self.assertEqual((r['fobU'], r['level']), (9.2222, 'L0'), t)
        st = by(ds, 'styles', 'base')['ROQAQF201SLS']
        self.assertEqual((st['costRule'], len(st['costByFactory'])), ('manual', 2))        # the quotes are still listed
        P = {r['ref']: r for r in rows(ds, 'production') if r['base'] == 'ROQAQG701SLS'}
        self.assertEqual((P['NN26003']['fobU'], P['NN26003']['level']), (9.3333, 'L0'))      # a ref override: its rows only
        self.assertEqual((P['AA26004']['level'], P['AA26004']['fobU']), ('L1', E.r4((6.1616 + 6.4646) / 2)))

    def test_fallback_keeps_the_ladder(self):
        ds = build(src_c13(), cb=C13_BOOK)
        st = by(ds, 'styles', 'base')['ROQAQF605SLS']
        lad = c13(src_c13()['ledger']['rows']).resolve('ROQAQF605SLS', 'AA', 'AA26009')
        self.assertEqual((st['costRule'], st['fobU'], st['level'], st['costByFactory'], st['costSpread']),
                         ('fallback', lad['fobU'], 'L3', [], None))
        self.assertIn('fabric_median', lad['flags'])

    def test_modern_and_tailored_take_the_regular_price(self):
        self.assertEqual([E.price_fit(f) for f in ('MODERN', 'TAILORED', 'SLIM', 'REGULAR', 'BIG_TALL', None)],
                         ['REGULAR', 'REGULAR', 'SLIM', 'REGULAR', 'BIG_TALL', None])
        mf, tf = E.decode_sku('ROQAQF801MFS'), E.decode_sku('ROQAQF802TFS')
        self.assertEqual((mf['fitCode'], mf['fit'], tf['fitCode'], tf['fit']), ('MF', 'MODERN', 'TF', 'TAILORED'))
        c = c13([led('NN26004', 'ROQAQF801MFS', 100), led('NN26005', 'ROQAQF802TFS', 100), led('CC26001', 'ROQAQS803TFS', 100)])
        for st in ('ROQAQF801MFS', 'ROQAQF802TFS'):
            q = c.style_quote(st)                                   # the grid's regular column, an exact match
            self.assertEqual((q['rule'], q['fobU'], q['level'], q['costByFactory']),
                             ('single', 4.6666, 'L4a', [['NN', 'GN', 'S', 4.6666, 'L4a']]), st)
            self.assertIn('fit_as_regular', q['flags'])
            self.assertEqual(c.resolve(st, 'NN')['level'], 'L4a')    # contract C15 R6: the ladder reads the same column
        k = c.style_quote('ROQAQS803TFS')                           # the KinYun-type regular fit column
        self.assertEqual((k['rule'], k['fobU'], k['level'], k['costByFactory']),
                         ('single', 2.9393, 'L2', [['CC', 'KY', 'CC26001', 2.9393, 'L2']]))
        self.assertIn('fit_as_regular', k['flags'])
        lad = c.resolve('ROQAQS803TFS', 'CC', 'CC26001')
        self.assertEqual((lad['level'], lad['fobU']), ('L2', 2.9393))  # the ladder reads the regular rate too (C15 R6)
        self.assertIn('fit_as_regular', lad['flags'])
        now, old = build(src_c13(), cb=C13_BOOK), build(src_c13(), cb=C13_BOOK, settings=CASCADE)
        self.assertEqual((by(now, 'production', 'style')['ROQAQS803TFS']['fobU'],
                          by(old, 'production', 'style')['ROQAQS803TFS']['fobU']), (2.9393, 2.9393))

    def test_one_cost_for_a_style_everywhere(self):
        ds = build(src_c13(), cb=C13_BOOK)
        want = E.r4((4.1717 + 4.4444) / 2)
        seen = style_rows(ds, 'ROQAQF201SLS')
        self.assertEqual({t for t, _ in seen}, {'lines', 'alloc', 'apo', 'inventory', 'production', 'styles', 'byStyle',
                                                'byCustomer'})
        for t, r in seen:
            self.assertEqual((r['fobU'], r['level']), (want, 'L4a'), t)
            if 'grade' in r:
                self.assertNotEqual(r['grade'], 'A', t)
        P = [r for r in rows(ds, 'production') if r['base'] == 'ROQAQF201SLS']
        self.assertEqual(sorted(r['factory'] for r in P), ['NN', 'TT'])           # each row keeps its factory
        st = by(ds, 'styles', 'base')['ROQAQF201SLS']
        self.assertEqual((st['costRule'], st['costSpread'], st['factories']),
                         ('average', E.r4(4.4444 / 4.1717 - 1), ['NN', 'TT']))
        self.assertEqual(ds['styles']['fields'][-7:], ['costByFactory', 'costRule', 'costSpread', 'costRow',
                                                       'costRowsSkipped', 'costFlags', 'kitPcs'])
        self.assertEqual(set(ds['dict']['costRules']), set(E.COST_RULES))
        for t in ('lines', 'apo'):                                               # the page recompute still matches
            for r in rows(ds, t):
                if r['fob'] is not None and r['rev'] is not None:
                    for k, v in client_recalc(r, ds['settings'], ds['dict'], t).items():
                        self.assertEqual(r[k], v, (t, r['id'], k))
        for t in ('lines', 'apo', 'inventory', 'production'):
            for r in rows(ds, t):
                for f in r['flags']:
                    self.assertIn(f, ds['dict']['flags'], (t, f))
        for v in list(ds['dict']['costRules'].values()) + [ds['dict']['flags'][f] for f in ('cost_average', 'cost_lowest',
                                                                                            'fit_as_regular')]:
            self.assertNotRegex(v, '[–—]')

    def test_production_keeps_each_factorys_own_price(self):
        # Cost of goods takes the style's combined cost, but a factory bills its own price. Each production row carries
        # the maker's own ladder cost (ownFobU, with its fxShare and fxRef) for the page's factory payments.
        ds = build(src_c13(), cb=C13_BOOK)
        self.assertEqual(ds['production']['fields'][-3:], ['ownFobU', 'ownFxShare', 'ownFxRef'])
        c = c13(src_c13()['ledger']['rows'])
        P = rows(ds, 'production')
        for r in P:
            own = c.resolve(r['style'], r['factory'], r['ref'], r['poName'])
            self.assertEqual((r['ownFobU'], r['ownFxShare']), (own['fobU'], own['fxShare']), r['ref'])
        mine = {r['factory']: r for r in P if r['base'] == 'ROQAQF201SLS'}
        want = E.r4((4.1717 + 4.4444) / 2)
        self.assertEqual((mine['TT']['fobU'], mine['NN']['fobU']), (want, want))            # one cost of goods
        self.assertEqual((mine['TT']['ownFobU'], mine['NN']['ownFobU']), (4.1717, 4.4444))  # each factory's own price
        self.assertEqual((mine['TT']['ownFxShare'], mine['TT']['ownFxRef']), (1.0, FXB))
        lst = [r for r in P if r['ref'] == 'AA26004'][0]                                      # a list price: no RMB part
        self.assertEqual((lst['ownFobU'], lst['ownFxShare'], lst['ownFxRef']), (6.1616, 0.0, None))
        at = {r['factory']: r for r in rows(build(src_c13(), cb=C13_BOOK, settings={'fx': {'rate': 7.7}}), 'production')
              if r['base'] == 'ROQAQF201SLS'}                                                # the RMB what-if moves it like fobU
        self.assertAlmostEqual(E.fx_what_if_unit(4.1717, 1.0, FXB, 7.7), at['TT']['ownFobU'], delta=0.00015)
        for r in rows(build(src_c13(), cb=C13_BOOK, settings=CASCADE), 'production'):   # cascade: the same price
            self.assertEqual(r['ownFobU'], r['fobU'], r['ref'])

    def test_cascade_mode_equals_the_old_result(self):
        keep = E.CostIndex.final
        try:
            E.CostIndex.final = lambda self, b, res: res              # the engine without the C13 choke point
            before = build(src_c13(), cb=C13_BOOK)
        finally:
            E.CostIndex.final = keep
        cas = build(src_c13(), cb=C13_BOOK, settings=CASCADE)
        for k in ('lines', 'alloc', 'apo', 'inventory', 'production', 'shipped', 'coverage', 'unresolved', 'conflicts',
                  'evidence', 'totals', 'alerts', 'dict'):
            self.assertEqual(cas[k], before[k], k)
        i = cas['styles']['fields'].index('costRule')
        self.assertEqual([r[:i] + r[i + 1:] for r in cas['styles']['rows']],
                         [r[:i] + r[i + 1:] for r in before['styles']['rows']])
        self.assertTrue(all(r[i] in ('cascade', 'manual') for r in cas['styles']['rows']))
        c = c13(src_c13()['ledger']['rows'])
        for r in rows(cas, 'production'):                          # each row at its own factory's ladder cost
            self.assertEqual(r['fobU'], c.resolve(r['style'], r['factory'], r['ref'], r['poName'])['fobU'], r['ref'])
        comb = {(r['ref'], r['style']): r['fobU'] for r in rows(build(src_c13(), cb=C13_BOOK), 'production')}
        self.assertNotEqual(comb[('TT26001', 'ROQAQF201SLS')], by(cas, 'production', 'ref')['TT26001']['fobU'])


# ── contract C15 (Sep 15): one right row per factory sheet. Sentinel prices only. ──
def c15rec(rid, price, fab, brand='QA', pat='SOLID', sleeve='LS', fit='SLIM', conf='high', tags=(), label='SAMPLE ROW',
           pool='GN:BASE', **kw):
    return calc(rid, pool, price, brand, fab, fit, pat, sleeve, fabric_code_confidence=conf, variant_tags=list(tags),
                fabrication=label, row=int(''.join(ch for ch in rid.split('!')[1] if ch.isdigit())), **kw)


C15_RECS = [
    # Fabric VQ, brand QA, grid GN: the slim, regular and big and tall columns of one plain solid row, and its variants.
    c15rec('GN!F101', 3.3131, ['VQ'], label='SAMPLE PLAIN SOLID'),
    c15rec('GN!K101', 3.5217, ['VQ'], fit='REGULAR', label='SAMPLE PLAIN SOLID'),
    c15rec('GN!P101', 4.1287, ['VQ'], fit='BIG_TALL', label='SAMPLE PLAIN SOLID'),
    c15rec('GN!F102', 3.7171, ['VQ'], conf='medium', tags=['brushed'], label='SAMPLE BRUSHED SOLID'),
    c15rec('GN!F103', 3.9191, ['UP', 'VQ'], tags=['perforated'], label='SAMPLE PERFORATED SOLID'),
    c15rec('GN!F104', 3.5353, ['VQ'], pat='PRINT', tags=['regular_print'], label='SAMPLE REGULAR PRINT'),
    c15rec('GN!F105', 3.8629, ['VQ'], pat='PRINT', tags=['digital_print'], label='SAMPLE DIGITAL PRINT'),
    c15rec('GN!F106', 3.1515, ['VQ'], sleeve='SS', label='SAMPLE SHORT SLEEVE SOLID'),
    c15rec('GN!F107', 3.0101, ['VZ', 'VQ'], conf='low', label='SAMPLE OTHER FABRIC'),
    c15rec('GN!F108', 2.9292, ['VQ'], brand='QB', label='SAMPLE OTHER BRAND SOLID'),
    # VT: two plain rows (a tie), no sleeve stated. VR: a digital print row only.
    c15rec('GN!F109', 3.6363, ['VT'], sleeve=None, label='SAMPLE TIE A'),
    c15rec('GN!F110', 3.4242, ['VT'], sleeve=None, label='SAMPLE TIE B'),
    c15rec('GN!F111', 3.6868, ['VR'], pat='PRINT', tags=['digital_print'], label='SAMPLE DIGITAL PRINT ONLY'),
    # VC: a plain row, a white row and a colour row. VD: white and colour rows only.
    c15rec('GN!F112', 3.5555, ['VC'], label='SAMPLE PLAIN'),
    c15rec('GN!F113', 3.2323, ['VC'], tags=['white'], label='SAMPLE WHITE'),
    c15rec('GN!F114', 3.3434, ['VC'], tags=['colour'], label='SAMPLE COLOUR'),
    c15rec('GN!F115', 3.2626, ['VD'], tags=['white'], label='SAMPLE WHITE'),
    c15rec('GN!F116', 3.3737, ['VD'], tags=['colour'], label='SAMPLE COLOUR'),
    # VN: a row whose text names one style (no pattern stated), and a plain solid row.
    c15rec('GN!F117', 4.4573, ['VN'], pat=None, names_styles=['301PSS'], label='SAMPLE ROW STYLE#301PSS'),
    c15rec('GN!K117', 4.6161, ['VN'], pat=None, fit='REGULAR', names_styles=['301PSS'], label='SAMPLE ROW STYLE#301PSS'),
    c15rec('GN!F118', 3.9797, ['VN'], label='SAMPLE PLAIN SOLID'),
    c15rec('GN!K118', 4.1919, ['VN'], fit='REGULAR', label='SAMPLE PLAIN SOLID'),
    # Grid GY club block: an all-brand row for fabric VG, and a QA row of another fabric (QA has a section there).
    c15rec('GY!F119', 4.1414, ['VG'], brand=None, brand_scope='all', label='SAMPLE ALL BRANDS', pool='GY:CLUB'),
    c15rec('GY!F120', 4.2929, ['VH'], label='SAMPLE QA ROW', pool='GY:CLUB'),
    # VK: a section that serves two brand codes. PO: a zip polo next to the plain polo GK!F1.
    c15rec('GN!F121', 3.7979, ['VK'], brand_codes=['QA', 'QE'], label='SAMPLE TWO CODE SECTION'),
    c15rec('GK!F122', 2.8989, ['PO'], category='polo', tags=['zipper'], label='SAMPLE ZIP POLO', pool='GK:ANY'),
    # A list names style ROQDVQ301SLS on a ref of factory AA: a proxy (L5) for any other factory.
    rec('AA!J9', 'PC', 'ref_price_list', 5.2525, factory_code='AA', production_ref='AA26007', style='ROQDVQ301SLS',
        fabric_codes=['VQ'], scope='ref_style'),
]
C15_BOOK = basis_book(C15_RECS)


def pick15(style, fac='NN', costbook=None):
    """The internal resolution (with its C15 row and the rows set aside) of one style at one factory."""
    return ci(costbook=costbook or C15_BOOK).raw(style, fac)


class OneRightRowC15(unittest.TestCase):
    def test_a_plain_row_beats_a_brushed_row(self):
        # R5: brushed and perforated rows price only a style that shows them. Never a median with them.
        r = pick15('ROQAVQ201SLS')
        self.assertEqual((r['level'], r['price'], r['ids'], r['row'], r['rng']),
                         ('L4a', 3.3131, ('GN!F101',), ('GN!F101', 'slim'), None))
        skip = dict(r['skip'])
        self.assertEqual((skip['GN!F102'], skip['GN!F103']), ('variant_not_this_style', 'variant_not_this_style'))
        self.assertEqual((skip['GN!F104'], skip['GN!F106'], skip['GN!F107'], skip['GN!F108']),
                         ('other_pattern', 'other_sleeve', 'lower_confidence', 'other_brand'))
        self.assertNotIn('ambiguous_rows', r['flags'])
        self.assertEqual(r['skip'][0], ('GN!F102', 'variant_not_this_style'))        # the closest rows come first
        up = pick15('ROQAUP202SLS')                                                  # fabric UP: a perforated style
        self.assertEqual((up['price'], up['ids']), (3.9191, ('GN!F103',)))

    def test_solid_and_print_rows(self):
        # R4: solid rows for solid styles, print rows for print styles. A regular print beats a digital print when
        # the style number does not say digital (flag pattern_guess). A digital print row alone serves as it is.
        p = pick15('ROQAVQ230SLP')
        self.assertEqual((p['price'], p['ids']), (3.5353, ('GN!F104',)))
        self.assertIn('pattern_guess', p['flags'])
        skip = dict(p['skip'])
        self.assertEqual((skip['GN!F105'], skip['GN!F101']), ('variant_not_this_style', 'other_pattern'))
        d = pick15('ROQAVR231SLP')
        self.assertEqual((d['price'], d['flags']), (3.6868, ()))
        self.assertEqual(pick15('ROQAVR232SLS')['level'], 'L7')        # a solid style: no print row, nothing relaxed

    def test_brand_sections_are_never_combined(self):
        # R1: a style takes its own brand's row, even when another brand's row is cheaper. The other brand takes its own.
        a, b = pick15('ROQAVQ201SLS'), pick15('ROQBVQ201SLS')
        self.assertEqual((a['price'], b['price'], b['ids']), (3.3131, 2.9292, ('GN!F108',)))
        self.assertEqual(dict(b['skip'])['GN!F101'], 'other_brand')
        e = pick15('ROQEVK201SLS')                     # a section serving two brand codes (the cost book's brand_codes)
        self.assertEqual((e['level'], e['price']), ('L4a', 3.7979))

    def test_generic_section_when_the_brand_has_none(self):
        # R1: with no section for the brand on a sheet, that sheet's all-brand rows serve.
        g = pick15('ROQCVG201SLS', 'TT')
        self.assertEqual((g['level'], g['price'], g['ids']), ('L4b', 4.1414, ('GY!F119',)))
        self.assertNotIn('cross_brand', g['flags'])

    def test_another_brand_only_as_the_last_resort(self):
        # R1: no sheet quotes brand QD, so another brand's row prices it, after the proxies: level L4c, grade D.
        x = pick15('ROQDVQ201SLS')
        self.assertEqual((x['level'], x['price'], x['gcap']), ('L4c', 2.9292, 'D'))
        self.assertTrue({'cross_brand', 'ambiguous_rows'} <= set(x['flags']))    # QA's and QB's rows tie: the lowest
        self.assertEqual(dict(x['skip'])['GN!F101'], 'tie_not_lowest')
        self.assertEqual(E.grade_of('L4c'), 'D')
        p = pick15('ROQDVQ301SLS')                     # the same style on a factory list: the proxy comes first
        self.assertEqual((p['level'], p['price']), ('L5', 5.2525))

    def test_fit_columns(self):
        # R6: the slim, regular and big and tall columns by the fit code. Modern and tailored use regular. An unknown
        # fit code uses regular, flagged fit_unknown.
        want = {'ROQAVQ201SLS': ('L4a', 3.3131, 'slim', ()), 'ROQAVQ211RFS': ('L4a', 3.5217, 'regular', ()),
                'ROQAVQ212BTS': ('L4a', 4.1287, 'big_tall', ()),
                'ROQAVQ213MFS': ('L4a', 3.5217, 'regular', ('fit_as_regular',)),
                'ROQAVQ214TFS': ('L4a', 3.5217, 'regular', ('fit_as_regular',)),
                'ROQAVQ215PSS': ('L4d', 3.5217, 'regular', ('fit_unknown',))}
        for st, (lv, p, col, fl) in want.items():
            r = pick15(st)
            self.assertEqual((r['level'], r['price'], r['row'][1]), (lv, p, col), st)
            for f in fl:
                self.assertIn(f, r['flags'], st)
        self.assertIsNone(E.decode_sku('ROQAVQ215PSS')['fit'])

    def test_ties_take_the_lowest_and_list_every_row(self):
        # R7: two plain rows fit: the lowest, flagged ambiguous_rows, the other listed. The range keeps both.
        t = pick15('ROQAVT201SLS')
        self.assertEqual((t['price'], t['ids'], t['rng'], t['skip']),
                         (3.4242, ('GN!F110',), (3.4242, 3.6363), (('GN!F109', 'tie_not_lowest'),)))
        self.assertIn('ambiguous_rows', t['flags'])
        # R3: a row with no sleeve is a long sleeve price, so a short sleeve style takes it with the short sleeve step.
        s = pick15('ROQAVT221SSS')
        self.assertEqual((s['level'], E.r4(s['price']), s['ids']), ('L4d', E.r4(3.4242 - 0.1111), ('GN!F110',)))

    def test_sleeve_row_for_a_short_sleeve_style(self):
        s = pick15('ROQAVQ220SSS')
        self.assertEqual((s['level'], s['price']), ('L4a', 3.1515))
        self.assertEqual(dict(s['skip'])['GN!F101'], 'other_sleeve')

    def test_fabric_code_confidence(self):
        # R2: a row that carries the code at low confidence gives way on the exact rung. It prices a style only when no
        # better row fits (a derived match).
        w = pick15('ROQAVZ201SLS')
        self.assertEqual((w['level'], w['price']), ('L4d', 3.0101))
        self.assertIn('derived', w['flags'])

    def test_a_row_that_names_the_style(self):
        # R5: a row whose text names the style is that style's row, before the pattern rule. For other styles it is one
        # row among the rest, so a plain solid row beats it.
        n = pick15('ROQAVN301PSS')
        self.assertEqual((n['level'], n['price'], n['ids']), ('L4d', 4.6161, ('GN!K117',)))     # fit PS: regular column
        self.assertEqual(dict(n['skip'])['GN!K118'], 'variant_not_this_style')
        o = pick15('ROQAVN302SLS')
        self.assertEqual((o['price'], dict(o['skip'])['GN!F117']), (3.9797, 'other_pattern'))

    def test_white_and_colour_rows(self):
        # R5: a plain row beats white and colour rows. With no plain row, the lowest of them, flagged.
        v = pick15('ROQAVC201SLS')
        self.assertEqual((v['price'], dict(v['skip'])['GN!F113']), (3.5555, 'variant_not_this_style'))
        d = pick15('ROQAVD201SLS')
        self.assertEqual((d['price'], d['skip']), (3.2626, (('GN!F116', 'tie_not_lowest'),)))
        self.assertIn('ambiguous_rows', d['flags'])

    def test_zip_polo(self):
        # R5: the zip collar letter shows the variant, so a zip polo takes the zipper row and a plain polo does not.
        z, p = pick15('ROQAPO401SLZ', 'TT'), pick15('ROQAPO402SLS', 'TT')
        self.assertEqual((z['price'], p['price']), (2.8989, 3.1313))
        self.assertEqual(dict(p['skip'])['GK!F122'], 'variant_not_this_style')

    def test_list_rates_keep_to_the_brand(self):
        # R1 on a price list: a factory's list rate for one brand never prices another brand's style.
        cb = basis_book(change={'CC!F3': {'brand_code': 'QK'}, 'CC!F4': {'brand_code': 'QK'}})
        c = ci(costbook=cb)
        self.assertEqual(c.resolve('ROQKQS111SSP', 'CC', 'CC26009')['fobU'], 2.7222)     # its own brand's rate
        self.assertEqual(c.resolve('ROQAQS111SSP', 'CC', 'CC26009')['level'], 'L7')      # no rate, no grid row for QA

    def test_dataset_names_the_row_and_the_rows_set_aside(self):
        s = src()
        s['open_orders']['orders'] = s['open_orders']['orders'] + [order('21', 'ROQAVQ201SLS', 10, 9.0),
                                                                   order('22', 'ROQDVQ201SLS', 10, 9.0)]
        ds = build(s, cb=C15_BOOK)
        S = by(ds, 'styles', 'base')
        a = S['ROQAVQ201SLS']
        self.assertEqual(a['costRow'], ['SYN-GN', 'F101', 'SAMPLE PLAIN SOLID', 'slim', 3.3131])
        self.assertEqual((a['fobU'], a['ev']), (3.3131, ['GN!F101']))
        sk = {x[0]: x for x in a['costRowsSkipped']}
        self.assertEqual(sk['GN!F102'], ['GN!F102', 'SAMPLE BRUSHED SOLID', 3.7171, 'variant_not_this_style'])
        self.assertTrue(all(x[3] in E.C15_REASONS for x in a['costRowsSkipped']))
        self.assertTrue({'GN!F101'} | set(sk) <= set(ds['evidence']))            # every cell named is in the evidence
        x = S['ROQDVQ201SLS']
        # costRow is the contract's five items; its C15 flags are styles.costFlags.
        self.assertEqual((x['level'], x['grade'], x['costRow'][:2], len(x['costRow']), x['costFlags']),
                         ('L4c', 'D', ['SYN-GN', 'F108'], 5, ['ambiguous_rows', 'cross_brand']))
        self.assertEqual(a['costFlags'], [])
        line = [r for r in rows(ds, 'lines') if r['base'] == 'ROQDVQ201SLS'][0]
        self.assertEqual((line['level'], line['grade']), ('L4c', 'D'))
        self.assertTrue({'cross_brand', 'ambiguous_rows'} <= set(line['flags']))
        for f in E.C15_FLAGS:
            self.assertIn(f, ds['dict']['flags'])
            self.assertNotRegex(ds['dict']['flags'][f], '[–—]')
        self.assertEqual(ds['dict']['levels']['L4c']['grade'], 'D')
        for st in rows(ds, 'styles'):                                              # the page's shape check
            if st['costRow'] is not None:
                self.assertEqual(len(st['costRow']), 5)
                self.assertIn(st['costRow'][3], ('slim', 'regular', 'big_tall'))
                self.assertIsInstance(st['costRow'][4], float)
            self.assertTrue(set(st['costFlags']) <= set(E.C15_FLAGS))
        self.assertTrue(any(n.startswith("Each factory's price for a style comes from one sheet row") for n in ds['notes']))


# The C15 fix run (Sep 15): named rows first, yarn dyed CY, inferred patterns, cooling, provisional prices, customer
# quotes in the other-brand rung, a quote in a picture, a ten character style number, the list fabric rung's fit, the
# order of the derived rungs and the tie break. Sentinel prices only.
def _fx(r, **kw):
    return dict(r, **kw)


FIX_RECS = [
    # CY (FABRIC_RULES: Yarn Dye): a yarn dyed row and a solid row.
    c15rec('GN!F140', 4.2323, ['CY'], pat='YARN_DYED', label='SAMPLE YARN DYED'),
    c15rec('GN!F141', 3.9111, ['CY'], label='SAMPLE CY SOLID'),
    # WC: rows that name styles, with other sleeves and patterns, and a plain long sleeve print row.
    c15rec('GN!K142', 3.4444, ['WC'], pat='PRINT', sleeve='SS', fit='REGULAR', names_styles=['401WSP'], label='SAMPLE SS ROW STYLE#401WSP'),
    c15rec('GN!K143', 3.9999, ['WC'], pat='PRINT', fit='REGULAR', label='SAMPLE PLAIN PRINT'),
    c15rec('GN!K144', 3.7777, ['WC'], fit='REGULAR', names_styles=['402WSP'], label='SAMPLE SOLID ROW STYLE#402WSP'),
    c15rec('GN!F145', 3.1212, ['WC'], pat='PRINT', sleeve='SS', names_styles=['403SLP'], label='SAMPLE SS ROW STYLE#403SLP'),
    c15rec('GN!F146', 3.6161, ['WC'], pat='PRINT', names_styles=['403SLP'], label='SAMPLE LS ROW STYLE#403SLP'),
    c15rec('GN!K148', 3.8888, ['WC'], pat='PRINT', fit='REGULAR', names_styles=['404PSP'], label='SAMPLE LS ROW STYLE#404PSP'),
    c15rec('GN!K149', 3.5959, ['WC'], pat='PRINT', sleeve='SS', fit='REGULAR', names_styles=['404PSP'], label='SAMPLE SS ROW STYLE#404PSP'),
    # WD: a row whose pattern the cost book only inferred (the sheet states none).
    _fx(c15rec('GN!F150', 4.0404, ['WD'], pat=None, label='SAMPLE WD ROW'), pattern_effective='SOLID',
        flags=['PATTERN_NOT_STATED_INFERRED_SOLID']),
    # WG: a row naming a style without its fabric code, and a cheaper plain row.
    c15rec('GN!K151', 4.4545, ['WG'], fit='REGULAR', names_styles=['ROQA801WRS'], label='SAMPLE ROW ROQA801WRS'),
    c15rec('GN!K152', 4.0101, ['WG'], fit='REGULAR', label='SAMPLE PLAIN SOLID'),
    # WF: a plain row and a cooling row. WH: a white row whose price is still needed and a colour row. WJ: only such a row.
    c15rec('GN!F153', 3.5050, ['WF'], label='SAMPLE PLAIN SOLID'),
    c15rec('GN!F154', 3.4040, ['WF'], tags=['cooling'], label='SAMPLE SOLID COOLING'),
    c15rec('GN!F155', 3.3030, ['WH'], tags=['white'], flags=['PROVISIONAL_TEXT_NEED_PRICE'], label='SAMPLE NEED WHITE PRICE'),
    c15rec('GN!F156', 3.6464, ['WH'], tags=['colour'], label='SAMPLE SOLID COLOUR'),
    c15rec('GN!F157', 3.7070, ['WJ'], flags=['PROVISIONAL_TEXT_NEED_PRICE'], label='SAMPLE NEED PRICE'),
    # WN: a long sleeve row in grid GN and a short sleeve row in grid GY that carries WN as its second code.
    c15rec('GN!F158', 4.2020, ['WN'], label='SAMPLE LS SOLID'),
    c15rec('GY!F159', 3.8080, ['WP', 'WN'], sleeve='SS', label='SAMPLE SS SECOND CODE', pool='GY:BASE'),
    # WQ: two rows at the same price.
    c15rec('GN!F160', 3.9090, ['WQ'], label='SAMPLE TWIN LOW ROW'),
    c15rec('GN!F99', 3.9090, ['WQ'], label='SAMPLE TWIN HIGH ROW'),
    # WK: the club group's own brand-less quotation.
    rec('HQ!D161', 'SYN-HQ', 'factory_quotation', 5.6161, pool='GN:CLUB', brand_code=None, fabric_codes=['WK'],
        fit_class='SLIM', pattern='SOLID', sleeve='LS', factory_code='TT', row=161),
    # WE: a customer quotation that names one style, its price in a picture.
    rec('HQ!IMG-ROQAWE601WSJ', 'SYN-HQ', 'image_callout', 6.1234, style='ROQAWE601WSJ', factory_code='TT',
        fabric_codes=['WE'], brand_code='QA', customer_group='CLUB', flags=['IMAGE_ONLY_PRICE_NOT_IN_A_CELL']),
    # WL and WM: list styles of factory AA, a slim one and a regular one.
    rec('AA!J12', 'PC', 'ref_price_list', 7.1313, factory_code='AA', production_ref='AA26011', style='ROQAWL011SLS',
        fabric_codes=['WL'], scope='ref_style'),
    rec('AA!J13', 'PC', 'ref_price_list', 7.4242, factory_code='AA', production_ref='AA26013', style='ROQAWM021RFS',
        fabric_codes=['WM'], scope='ref_style'),
]
FIX_BOOK = basis_book(C15_RECS + FIX_RECS)


def fix15(style, fac='NN', ref=None):
    return ci(costbook=FIX_BOOK).raw(style, fac, ref)


class OneRightRowFixes(unittest.TestCase):
    def test_cy_is_yarn_dyed_whatever_the_letter(self):
        # The platform's FABRIC_RULES says CY is a yarn dye, like YD and SP, so a CY style takes the yarn dyed row.
        self.assertEqual((E.decode_sku('ROQACY201SLB')['pat'], E.decode_sku('ROQACY202SLD')['pat']), ('YARN_DYED', 'YARN_DYED'))
        r = fix15('ROQACY201SLB')
        self.assertEqual((r['level'], r['price'], r['ids']), ('L4a', 4.2323, ('GN!F140',)))
        self.assertEqual(dict(r['skip'])['GN!F141'], 'other_pattern')

    def test_a_row_that_names_the_style_comes_before_sleeve_and_pattern(self):
        # R0: with an unknown fit code the named row's own sleeve decides.
        r = fix15('ROQAWC401WSP')
        self.assertEqual((r['level'], r['price'], r['ids']), ('L4d', 3.4444, ('GN!K142',)))
        self.assertEqual(dict(r['skip'])['GN!K143'], 'variant_not_this_style')
        self.assertNotIn('named_row_conflict', r['flags'])
        # A named row whose pattern differs from the style number still wins, flagged for a check.
        c = fix15('ROQAWC402WSP')
        self.assertEqual((c['price'], c['ids']), (3.7777, ('GN!K144',)))
        self.assertIn('named_row_conflict', c['flags'])
        self.assertIn('Confirm which is right', c['basis'])
        # A known fit code's sleeve decides between two named rows.
        k = fix15('ROQAWC403SLP')
        self.assertEqual((k['level'], k['price'], dict(k['skip'])['GN!F145']), ('L4a', 3.6161, 'other_sleeve'))
        # An unknown fit code named by a long and a short sleeve row: both fit, the lowest, flagged.
        t = fix15('ROQAWC404PSP')
        self.assertEqual((t['price'], dict(t['skip'])['GN!K148']), (3.5959, 'tie_not_lowest'))
        self.assertIn('ambiguous_rows', t['flags'])

    def test_a_pattern_the_cost_book_inferred_serves_both(self):
        # R4: the sheet states no pattern, so the row serves a print style when the section has no print row.
        self.assertEqual((fix15('ROQAWD501SLP')['price'], fix15('ROQAWD502SLS')['price']), (4.0404, 4.0404))

    def test_a_number_without_its_fabric_code_names_the_style(self):
        n = fix15('ROQAWG801WRS')
        self.assertEqual((n['price'], dict(n['skip'])['GN!K152']), (4.4545, 'variant_not_this_style'))
        self.assertEqual(fix15('TMQAWG801WRS')['price'], 4.0101)            # another customer: not named

    def test_cooling_is_a_finish_that_loses_to_a_plain_row(self):
        r = fix15('ROQAWF701SLS')
        self.assertEqual((r['price'], dict(r['skip'])['GN!F154']), (3.5050, 'variant_not_this_style'))
        self.assertNotIn('ambiguous_rows', r['flags'])

    def test_a_price_still_needed_loses_to_a_confirmed_row(self):
        r = fix15('ROQAWH901SLS')
        self.assertEqual((r['price'], dict(r['skip'])['GN!F155']), (3.6464, 'lower_confidence'))
        self.assertNotIn('ambiguous_rows', r['flags'])
        u = fix15('ROQAWJ902SLS')                                         # the only row: used, flagged
        self.assertEqual(u['price'], 3.7070)
        self.assertIn('price_unconfirmed', u['flags'])
        self.assertIn('price_unconfirmed', E.FLAG_LABELS)

    def test_the_derived_rungs_try_every_sheet_before_the_next_step(self):
        # L4d order: the alternate fabric code on every sheet first, then the short sleeve step, then the fit premium.
        r = fix15('ROQAWN951SSS')
        self.assertEqual((r['level'], r['price'], r['ids']), ('L4d', 3.8080, ('GY!F159',)))

    def test_equal_prices_take_the_lowest_sheet_row(self):
        r = fix15('ROQAWQ961SLS')
        self.assertEqual((r['ids'], dict(r['skip'])['GN!F160']), (('GN!F99',), 'tie_not_lowest'))

    def test_a_customer_quotation_never_serves_another_group(self):
        own = fix15('CLQDWK171SLS')
        self.assertEqual((own['level'], own['price']), ('L4a', 5.6161))
        self.assertIn('customer_quote', own['flags'])
        self.assertEqual(fix15('ROQDWK172SLS')['level'], 'L7')      # no other brand's section quotes WK

    def test_a_quote_in_a_picture_names_its_style(self):
        c = ci(costbook=FIX_BOOK)
        r = c.resolve('ROQAWE601WSJ', 'UNKNOWN')
        self.assertEqual((r['level'], r['fobU'], r['evidence']), ('L1', 6.1234, ['HQ!IMG-ROQAWE601WSJ']))
        q = c.quotes('ROQAWE601WSJ')
        self.assertEqual([(x['fac'], x['level'], x['price']) for x in q], [('TT', 'L1', 6.1234)])
        self.assertEqual(c.resolve('ROQAWE602WSJ', 'UNKNOWN')['level'], 'L7')

    def test_the_list_fabric_rung_keeps_to_the_fit(self):
        # R6 on a price list: a regular style takes a slim style's price plus the fit premium (derived); a slim style
        # with only a regular style listed falls through.
        c = ci(costbook=FIX_BOOK)
        r = c.raw('ROQAWL012RFS', 'AA', 'AA26012')
        self.assertEqual((r['level'], E.r4(r['price'])), ('L3', E.r4(7.1313 + 0.2222)))
        self.assertTrue({'fabric_median', 'derived'} <= set(r['flags']))
        self.assertEqual(c.raw('ROQAWL013SLS', 'AA', 'AA26014')['price'], 7.1313)
        self.assertNotEqual(c.raw('ROQAWM022SLS', 'AA', 'AA26015')['level'], 'L3')


class FiberMapC11(unittest.TestCase):
    def test_fabric_codes_follow_the_predominant_fiber(self):
        # The fabric names below are the public FABRIC_RULES text of the platform.
        cases = {'PY': {'cotton': 50, 'polyester': 47, 'spandex': 3}, 'CL': {'lyocell': 35, 'cotton': 35, 'nylon': 27,
                 'spandex': 3}, 'LC': {'cotton': 51, 'poly': 49}, 'LT': {'cotton': 45, 'linen': 55},
                 'SN': {'cotton': 75, 'rayon': 15, 'polyester': 10}, 'OX': {'poly': 65, 'cotton': 35},
                 'SP': {'poly': 52, 'cotton': 45, 'spand': 3}, 'MR': {'microfiber': 50, 'rayon': 50},
                 'VP': {'viscose': 50, 'polyester': 50}, 'TD': {'polyester': 60, 'cotton': 40}}
        for code, content in cases.items():
            self.assertEqual(E.FIBER_BY_FABRIC.get(code, 'mmf'), E.fiber_group_of(content), code)
        self.assertEqual((E.decode_sku('ROQAPY101SLS')['fiber'], E.decode_sku('ROQACL101SLS')['fiber']), ('cotton', 'cotton'))
        self.assertEqual(E.fiber_class(E.decode_sku('ROQAPY101SLS')['fiber']), 'natural')


# ── Sep 22 defect fixes: kit money, suppressed supply, raw stock cells, PO-aware KinYun rate,
# per-division shipped dates. Sentinel numbers only. ──
def src_kit_hist():
    """src() plus invoice history for the kit program (10 cartons of 12 pieces, one customer)."""
    s = src()
    s['sales_analytics']['styles'].append(['ZZKIT01', 'SYN', 10, 1200.0, '2025-06-01', '2026-02-10',
                                           {'2026-01': [10, 1200.0, 0, 0.0]}, {'BJS': [10, 1200.0]}, 'NAVY'])
    return s


class KitShippedMoney(unittest.TestCase):
    """Invoice-history units of a kit program are cartons; its cost is per piece. The shipped lens
    costs the pieces, exactly as shipped.byCustomer does."""

    def test_shipped_cogs_count_kit_pieces(self):
        ds = build(src_kit_hist())
        st = by(ds, 'styles', 'base')['ZZKIT01']
        bc = {(r['cust'], r['base']): r for r in rows({'b': ds['shipped']['byCustomer']}, 'b')}
        row = bc[('BJS', 'ZZKIT01')]
        self.assertEqual((row['units'], row['pieces']), (10, 120))
        # The one month is inside the trailing 12, so the style's t12 cost is the customer row's cost.
        self.assertEqual(st['t12Cogs'], row['cogs'])
        self.assertEqual(st['t12Gp'], row['gp'])
        fob = E.r2(120 * st['fobU'])
        d, f, x = E.adders(fob, 120, 'dress_shirt', 'mmf', row['origin'], ds['settings'], 'us')
        self.assertEqual(st['t12Cogs'], E.r2(fob + d + f + x))
        Y = by({'b': ds['shipped']['byStyle']}, 'b', 'base')['ZZKIT01']
        self.assertEqual(Y['months'], {'2026-01': [10, 1200.0]})          # months stay invoiced cartons

    def test_kit_expected_price_is_per_piece(self):
        st = by(build(src_kit_hist()), 'styles', 'base')['ZZKIT01']
        self.assertEqual(st['expPrice'], E.r4(1200.0 / (10 * 12)))        # history branch
        s2 = src_kit_hist()
        s2['inventory']['items'] = s2['inventory']['items'] + [inv('ZZKIT01', tr=240)]
        s2['open_orders']['orders'] = s2['open_orders']['orders'] + [order('13', 'ZZKIT01', 5, 108.0, cust='BJS')]
        st2 = by(build(s2), 'styles', 'base')['ZZKIT01']
        self.assertEqual(st2['expPrice'], E.r4(5 * 108.0 / (5 * 12)))     # open-lines branch
        # Pinned independently: the feed's ats counts CARTONS, exp/landed are per piece, so the
        # dollar figure scales by the 12 pieces per carton (kitPcs is published for the client).
        self.assertEqual(st2['kitPcs'], 12)
        self.assertEqual(st2['atsPotentialGp'],
                         E.r2(max(0, st2['ats']) * 12 * (st2['expPrice'] * (1 - st2['dedPct'] / 100) - st2['landedU'])))
        self.assertGreater(st2['atsPotentialGp'], 0)                      # the margin is real, not rounding noise
        st3 = by(build(src()), 'styles', 'base').get('ROQAQF221SLS')
        if st3 is not None:
            self.assertIsNone(st3.get('kitPcs'))                          # non-kit styles publish no factor

    def test_kit_apo_price_skips_the_carton_t12(self):
        s = src_kit_hist()
        s['apo']['rows'] = s['apo']['rows'] + [{'style': 'ZZKIT01', 'qty': 24, 'customer': 'MEN WARHOUSE',
                                               'po': 'SYNTH KIT T12'}]
        a = [r for r in rows(build(s), 'apo') if r['po'] == 'SYNTH KIT T12'][0]
        self.assertNotEqual(a['priceBasis'], 'style_t12')                 # a carton price, never per piece
        self.assertEqual((a['priceBasis'], a['estPrice']), ('offprice_brand', E.r4((300 * 9.99 + 600 * 11.0) / 900)))


class SuppressedSupplyStyles(unittest.TestCase):
    def test_suppressed_batches_are_not_incoming(self):
        s = src()
        # Arrival lands on the build day and the warehouse holds the same units: routing suppresses it.
        s['inventory']['items'] = s['inventory']['items'] + [inv('ROQAQF221SLS', tr=200)]
        s['ledger']['rows'] = s['ledger']['rows'] + [led('TT26021', 'ROQAQF221SLS', 200, etd='2026-01-16')]
        ds = build(s)
        p = by(ds, 'production', 'ref')['TT26021']
        self.assertIn('suppressed', p['flags'])
        st = by(ds, 'styles', 'base')['ROQAQF221SLS']
        self.assertEqual((st['incoming'], st['incomingFob'], st['atsFreeProd']), (0, 0.0, 0))
        self.assertEqual((st['onHand'], st['atsFreeStock']), (200, st['ats']))
        self.assertIn('TT', st['factories'])                              # the maker is still named
        self.assertEqual(ds['totals']['production']['suppressedUnits'], 200)   # totals keep them apart


class RawStockCells(unittest.TestCase):
    def test_duplicate_feed_rows_sum_raw(self):
        s = src()
        s['inventory']['items'] = s['inventory']['items'] + [inv('ROQAQF223SLS', tr=100), inv('ROQAQF223SLS', tr=-40),
                                                             inv('ROQAQF224SLS', tr=-15)]
        ds = build(s)
        I = [r for r in rows(ds, 'inventory') if r['sku'] == 'ROQAQF223SLS']
        self.assertEqual([(r['wh'], r['units']) for r in I], [('TR', 60)])     # not 100: the negative row counts
        self.assertEqual(by(ds, 'styles', 'base')['ROQAQF223SLS']['onHand'], 60)
        self.assertEqual([r for r in rows(ds, 'inventory') if r['sku'] == 'ROQAQF224SLS'], [])


class KinYunPoAware(unittest.TestCase):
    AMZ = rec('CC!F9', 'KY', 'ref_price_list', 2.4444, factory_code='CC', production_ref_resolved='CC26002',
              pattern='SOLID', fit_class='SLIM', customer_group='AMAZON', fabric_codes=['QS'], scope='ref')

    def test_rate_rung_reads_the_po_prefix(self):
        c = ci(costbook=basis_book([self.AMZ]))
        am = c.resolve('ROQAQS110SLS', 'CC', 'CC26099', poName='AM SYNTH PO')
        self.assertEqual((am['level'], am['fobU']), ('L3', 2.4444))       # the Amazon rate for an AM PO
        other = c.resolve('ROQAQS110SLS', 'CC', 'CC26099', poName='SYNTH PO')
        self.assertEqual((other['level'], other['fobU']), ('L3', 2.2222))
        self.assertEqual(c.resolve('ROQAQS110SLS', 'CC', 'CC26099')['fobU'], 2.2222)


class ShippedDivDates(unittest.TestCase):
    def test_range_carries_by_div_and_the_note_names_both_dates(self):
        s = src()
        bd = {'OB': {'from': '2025-01-01', 'to': '2026-02-20', 'rows': 500},
              'DS': {'from': '2025-01-01', 'to': '2026-02-10', 'rows': 200}}
        s['sales_analytics']['source']['byDiv'] = bd
        ds = build(s)
        self.assertEqual(ds['shipped']['range']['byDiv'], bd)
        note = ds['shipped']['note']
        self.assertIn('Invoices run through Feb 20, 2026.', note)
        self.assertIn('Dropship invoices run through Feb 10, 2026.', note)
        self.assertNotRegex(note, r'\d{4}-\d{2}-\d{2}')
        s2 = src()
        s2['sales_analytics']['source']['byDiv'] = {'OB': {'to': '2026-02-20'}, 'DS': {'to': '2026-02-20'}}
        self.assertNotIn('Dropship', build(s2)['shipped']['note'])        # same date: one sentence

    def test_stale_alert_considers_the_dropship_date(self):
        s = src()
        s['sales_analytics']['source']['byDiv'] = {'OB': {'to': '2026-02-20'}, 'DS': {'to': '2026-01-10'}}
        al = {a['id']: a for a in build(s)['alerts']}
        self.assertIn('al_stale_analytics', al)
        self.assertEqual(al['al_stale_analytics']['title'], 'Shipped history is old')
        self.assertIn('Dropship invoice history ends on Jan 10, 2026.', al['al_stale_analytics']['detail'])
        self.assertNotIn('The invoice history ends', al['al_stale_analytics']['detail'])
        self.assertNotIn('al_stale_analytics', {a['id'] for a in build()['alerts']})   # fresh dates: no alert


if __name__ == '__main__':
    unittest.main()
