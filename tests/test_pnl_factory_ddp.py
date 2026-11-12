"""Synthetic tests for factory delivered terms (Nov 12 2026): settings factories.<code>.ddpBrands lists the brands
a factory delivers duty paid (DDP), so the P&L adds no import costs on that factory's goods of those brands,
whatever row prices them. Synthetic factory codes, brands and sentinel prices only (the fixtures of
test_pnl_engine). Run: python -m unittest discover -s tests -p "test_pnl_factory_ddp.py"
"""
import copy
import os
import sys
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))
sys.path.insert(0, HERE)
import pnl  # noqa: E402
import pnl_engine as E  # noqa: E402
import test_pnl_engine as T  # noqa: E402   (fixtures only)

TT_QA = {'factories': {'TT': {'name': 'Synth Factory T', 'origin': 'CN', 'ddpBrands': ['QA']}}}
AA_QA = {'factories': {'AA': {'name': 'Synth Factory A', 'origin': 'CN', 'ddpBrands': ['QA']}}}


def history_only(style, qty=60, value=600.0):
    """src() plus invoice history for a style with no stock, ledger line or open order."""
    s = T.src()
    sa = copy.deepcopy(s['sales_analytics'])
    sa['styles'].append([style, 'SYN', qty, value, '2026-01-05', '2026-02-10',
                         {'2026-01': [qty // 2, value / 2, 0, 0.0], '2026-02': [qty - qty // 2, value / 2, 0, 0.0]},
                         {'ROSS': [qty, value]}, 'BLUE'])
    s['sales_analytics'] = sa
    return s


class FactoryBrandsFromSettings(unittest.TestCase):
    def test_parse(self):
        S = E.merge_settings({'factories': {
            'AA': {'name': 'x', 'origin': 'CN', 'ddpBrands': ['qa', ' DV ', 'NT', '', 5, None]},
            'TT': {'name': 'y', 'origin': 'CN'},                       # no ddpBrands
            'BB': {'name': 'z', 'origin': 'CN', 'ddpBrands': 'QA'},    # not a list
            'CC': {'name': 'w', 'origin': 'CN', 'ddpBrands': []},      # empty
            'WH': {'ddpBrands': ['QA']}, 'UNKNOWN': {'ddpBrands': ['QA']}, 'MIX': {'ddpBrands': ['QA']},
            '_default': {'name': 'Unknown factory', 'origin': 'CN', 'ddpBrands': ['QA']}}})
        self.assertEqual(E.ddp_factory_brands(S), {'AA': frozenset({'QA', 'VD', 'NA'})})

    def test_off_by_default(self):
        self.assertEqual(E.ddp_factory_brands(E.merge_settings({})), {})
        self.assertEqual(E.ddp_factory_brands({}), {})
        self.assertEqual(E.ddp_factory_brands(None), {})
        for fac, rec in E.DEFAULT_SETTINGS['factories'].items():
            self.assertNotIn('ddpBrands', rec, fac)                     # the public defaults name no brand

    def test_settings_validation_keeps_and_checks_the_list(self):
        clean, problems, dropped = pnl.validate_settings(
            {'factories': {'aa': {'name': 'Synth', 'origin': 'cn', 'ddpBrands': ['qa', 'vd']}}})
        self.assertEqual(problems, [])
        self.assertEqual(clean['factories'], {'AA': {'name': 'Synth', 'origin': 'CN', 'ddpBrands': ['QA', 'VD']}})
        self.assertEqual(dropped, [])
        for bad, issue in (('QA', 'not_a_list'), (['Q A'], 'invalid_code'), ([7], 'invalid_code'),
                           (['QA'] * 41, 'too_long')):
            clean, problems, _ = pnl.validate_settings({'factories': {'AA': {'name': 'S', 'ddpBrands': bad}}})
            self.assertIsNone(clean, bad)
            self.assertIn(issue, {p['issue'] for p in problems}, bad)


class FactoryDeliveredResolutions(unittest.TestCase):
    def test_a_grid_price_for_goods_of_a_delivering_factory(self):
        base = T.ci().resolve('ROQAQF201SLS', 'TT', 'TT26001')
        self.assertNotIn('ddp', base['flags'])
        c = T.ci(settings=TT_QA)
        r = c.resolve('ROQAQF201SLS', 'TT', 'TT26001')
        self.assertEqual((r['level'], r['fobU'], r['flags']), (base['level'], base['fobU'], base['flags'] + ['ddp']))
        self.assertTrue(r['basis'].endswith('Synth Factory T delivers QA duty paid (DDP), so no import costs are added.'),
                        r['basis'])
        self.assertEqual(r['basis'].count('duty paid'), 1)
        # The same goods from another factory, and another brand from the same factory, keep their import costs.
        self.assertNotIn('ddp', c.resolve('ROQAQF201SLS', 'NN')['flags'])
        dk = c.resolve('RODKQF201SLS', 'TT')
        self.assertNotIn('ddp', dk['flags'])
        self.assertNotIn('ddp', c.resolve('ROQAQF201SLS', 'UNKNOWN')['flags'])

    def test_a_style_two_factories_make(self):
        # ROQAQH204SLS is made on TT26002 and NN26002: its one combined cost averages both factories' quotes. The
        # delivering factory's lot is delivered on its own row; the other lot and any row with no known factory keep
        # the import costs, flagged ddp_mixed.
        c = T.ci(ledger=T.src()['ledger']['rows'], settings=TT_QA)
        tt = c.final('ROQAQH204SLS', c.raw('ROQAQH204SLS', 'TT', 'TT26002'))
        nn = c.final('ROQAQH204SLS', c.raw('ROQAQH204SLS', 'NN', 'NN26002'))
        anywhere = c.final('ROQAQH204SLS', c.raw('ROQAQH204SLS', 'UNKNOWN'))
        self.assertEqual(tt['price'], nn['price'])
        self.assertEqual(c.combined('ROQAQH204SLS')['rule'], 'average')
        self.assertIn('ddp', tt['flags'])
        self.assertNotIn('ddp_mixed', tt['flags'])
        for r in (nn, anywhere):
            self.assertIn('ddp_mixed', r['flags'])
            self.assertNotIn('ddp', r['flags'])

    def test_a_list_quote_and_the_combined_cost(self):
        self.assertEqual([q['flags'] for q in T.ci().quotes('ROQAQF101SLS')], [()])
        c = T.ci(settings=AA_QA)
        qs = c.quotes('ROQAQF101SLS')
        self.assertEqual([(q['fac'], q['level'], q['price'], q['flags']) for q in qs], [('AA', 'L1', 7.7777, ('ddp',))])
        lst = c.resolve('ROQAQF101SLS', 'AA', 'AA26001')
        self.assertEqual((lst['level'], lst['fobU'], lst['flags']), ('L1', 7.7777, ['ddp']))
        # A row whose own factory is not known (stock, history) takes the combined cost, delivered too.
        anywhere = c.final('ROQAQF101SLS', c.raw('ROQAQF101SLS', 'UNKNOWN'))
        self.assertEqual((anywhere['level'], anywhere['price'], anywhere['flags']), ('L1', 7.7777, ('ddp',)))
        self.assertIn('Synth Factory A delivers QA duty paid (DDP)', anywhere['basis'])
        self.assertEqual(c.style_quote('ROQAQF101SLS')['flags'], ['ddp'])

    def test_brand_letters_fold(self):
        c = T.ci(settings={'factories': {'TT': {'ddpBrands': ['VD']}}})
        self.assertEqual((c.brand_code('RODVQF201SLS'), c.brand_code('ROVDQF201SLS'), c.brand_code('ROQAQF201SLS')),
                         ('VD', 'VD', 'QA'))
        for st in ('RODVQF201SLS', 'ROVDQF201SLS'):
            r = c.raw(st, 'TT')                       # another brand's grid row (L4c) prices it: still delivered
            self.assertEqual(r['level'], 'L4c', st)
            self.assertIn('ddp', r['flags'], st)
        self.assertNotIn('ddp', c.raw('ROQAQF201SLS', 'TT')['flags'])

    def test_a_manual_cost_keeps_its_own_terms(self):
        ov = [T.ovr('f1', 'style', {'style': 'ROQAQF201SLS'}, 9.4444)]          # FOB, the default
        r = T.ci(settings=TT_QA, overrides=ov).resolve('ROQAQF201SLS', 'TT', 'TT26001')
        self.assertEqual((r['level'], r['fobU'], r['flags']), ('L0', 9.4444, []))
        ov = [T.ovr('f2', 'style', {'style': 'ROQAQF201SLS'}, 9.4444, terms='DDP')]
        r = T.ci(settings=TT_QA, overrides=ov).resolve('ROQAQF201SLS', 'TT', 'TT26001')
        self.assertEqual((r['level'], r['flags']), ('L0', ['ddp']))
        self.assertNotIn('duty paid', r['basis'])


class FactoryDeliveredDataset(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.before = T.build()
        cls.after = T.build(settings=TT_QA)

    def test_production_rows(self):
        P = {(r['ref'], r['base']): r for r in T.rows(self.after, 'production')}
        B = {(r['ref'], r['base']): r for r in T.rows(self.before, 'production')}
        for k in (('TT26001', 'ROQAQF201SLS'), ('TT26002', 'ROQAQH204SLS'), ('TT26002', 'ROQAQF205SLS')):
            self.assertEqual((P[k]['dutyRegime'], P[k]['landed'], P[k]['landedU']), ('none', P[k]['fob'], P[k]['fobU']), k)
            self.assertIn('ddp', P[k]['flags'])
            self.assertEqual((P[k]['fobU'], P[k]['fob']), (B[k]['fobU'], B[k]['fob']), k)   # the factory cost is unchanged
            self.assertGreater(B[k]['landed'], B[k]['fob'], k)
        for k in (('NN26002', 'ROQAQH204SLS'), ('NN26002', 'ROQAQF205SLS')):
            self.assertEqual(P[k]['dutyRegime'], 'us', k)
            self.assertNotIn('ddp', P[k]['flags'])
            self.assertEqual(P[k]['landed'], B[k]['landed'], k)

    def test_lines_and_styles(self):
        L = T.by(self.after, 'lines', 'id')['1|ROQAQF201SLS']
        self.assertEqual((L['dutyRegime'], L['duty'], L['freight'], L['fees'], L['cogs']), ('none', 0.0, 0.0, 0.0, L['fob']))
        self.assertIn('ddp', L['flags'])
        for k, v in T.client_recalc(L, self.after['settings'], self.after['dict'], 'lines').items():
            self.assertEqual(L[k], v, k)
        # A line placed half on the delivering factory and half on another stays an import, flagged ddp_mixed.
        M = T.by(self.after, 'lines', 'id')['4|ROQAQH204SLS']
        self.assertEqual(M['dutyRegime'], 'us')
        self.assertIn('ddp_mixed', M['flags'])
        self.assertNotIn('ddp', M['flags'])
        st, st0 = T.by(self.after, 'styles', 'base')['ROQAQF201SLS'], T.by(self.before, 'styles', 'base')['ROQAQF201SLS']
        self.assertEqual((st['fobU'], st['landedU'], st['dutyRegime']), (st0['fobU'], st0['fobU'], 'none'))
        self.assertIn('ddp', st['costFlags'])
        self.assertGreater(st0['landedU'], st0['fobU'])

    def test_shipped_history(self):
        cust = {(x['cust'], x['base']): x for x in self._table(self.after['shipped']['byCustomer'])}
        r = cust[('ROSS', 'ROQAQF201SLS')]
        self.assertEqual((r['dutyRegime'], r['duty'], r['freight'], r['fees'], r['cogs']), ('none', 0.0, 0.0, 0.0, r['fob']))
        r0 = {(x['cust'], x['base']): x for x in self._table(self.before['shipped']['byCustomer'])}[('ROSS', 'ROQAQF201SLS')]
        self.assertGreater(r0['duty'], 0)
        bs = {x['base']: x for x in self._table(self.after['shipped']['byStyle'])}['ROQAQF201SLS']
        self.assertEqual(bs['fobShare'], 1.0)
        # The Styles table costs the same history the same way (t12Cogs = factory cost only).
        st = T.by(self.after, 'styles', 'base')['ROQAQF201SLS']
        fob = E.r2(sum(E.r2(u * bs['pcs'] * E.r4(st['fobU'])) for ym, (u, v) in bs['months'].items()))
        self.assertEqual(st['t12Cogs'], fob)

    @staticmethod
    def _table(t):
        return [dict(zip(t['fields'], r)) for r in t['rows']]


class DeliveredStyleWithoutStockOrProduction(unittest.TestCase):
    """A style that only sold (no stock, ledger line or open order): its Styles row lands with no import costs when
    its cost is delivered (Nov 12 2026; before, such a row always read as a US import)."""

    def test_factory_terms(self):
        s = history_only('ROQAQF101SLS')
        st0 = T.by(T.build(s), 'styles', 'base')['ROQAQF101SLS']
        self.assertGreater(st0['landedU'], st0['fobU'])
        ds = T.build(s, settings=AA_QA)
        st = T.by(ds, 'styles', 'base')['ROQAQF101SLS']
        self.assertEqual((st['fobU'], st['landedU'], st['dutyRegime'], st['level']), (7.7777, 7.7777, 'none', 'L1'))
        self.assertIn('ddp', st['costFlags'])
        self.assertEqual((st['onHand'], st['incoming'], st['openUnits']), (0, 0, 0))
        self.assertEqual(st['t12Cogs'], E.r2(E.r2(30 * 7.7777) + E.r2(30 * 7.7777)))
        r = {(x['cust'], x['base']): x for x in FactoryDeliveredDataset._table(ds['shipped']['byCustomer'])}[
            ('ROSS', 'ROQAQF101SLS')]
        self.assertEqual((r['dutyRegime'], r['duty'], r['freight'], r['fees']), ('none', 0.0, 0.0, 0.0))

    def test_manual_ddp_terms(self):
        ov = [T.ovr('h1', 'style', {'style': 'ROQAQF101SLS'}, 9.5555, terms='DDP')]
        st = T.by(T.build(history_only('ROQAQF101SLS'), overrides=ov), 'styles', 'base')['ROQAQF101SLS']
        self.assertEqual((st['fobU'], st['landedU'], st['dutyRegime'], st['costRule']), (9.5555, 9.5555, 'none', 'manual'))
        fob = [T.ovr('h2', 'style', {'style': 'ROQAQF101SLS'}, 9.5555)]
        st = T.by(T.build(history_only('ROQAQF101SLS'), overrides=fob), 'styles', 'base')['ROQAQF101SLS']
        self.assertEqual(st['dutyRegime'], 'us')
        self.assertGreater(st['landedU'], st['fobU'])


class NothingElseMoves(unittest.TestCase):
    def test_settings_without_brands_give_the_same_dataset(self):
        named = {'factories': {'TT': {'name': 'Synth Factory T', 'origin': 'CN'}}}
        for st in ({}, named, {'factories': {'TT': {'name': 'Synth Factory T', 'origin': 'CN', 'ddpBrands': []}}}):
            a, b = T.build(settings=st), T.build(settings=named)
            for t in ('lines', 'apo', 'inventory', 'production', 'styles'):
                self.assertEqual(a[t], b[t], (t, st))

    def test_a_brand_the_factory_does_not_make_changes_nothing(self):
        a = T.build(settings={'factories': {'TT': {'name': 'Synth Factory T', 'origin': 'CN', 'ddpBrands': ['ZQ']}}})
        b = T.build(settings={'factories': {'TT': {'name': 'Synth Factory T', 'origin': 'CN'}}})
        for t in ('lines', 'apo', 'inventory', 'production', 'styles'):
            self.assertEqual(a[t], b[t], t)
        self.assertEqual(a['shipped']['byCustomer'], b['shipped']['byCustomer'])


if __name__ == '__main__':
    unittest.main()
