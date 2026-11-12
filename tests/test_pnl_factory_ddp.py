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
        # ROQAQH204SLS is made on TT26002 and NN26002 (300 units each on the ledger): its one combined cost averages
        # both factories' quotes. The delivering factory's lot is delivered on its own row; the other lot keeps the
        # import costs (ddp_mixed). A row with no known factory pays import on the other factory's share of the
        # units, half (ddp_part, David Nov 12 2026: split by share).
        c = T.ci(ledger=T.src()['ledger']['rows'], settings=TT_QA)
        tt = c.final('ROQAQH204SLS', c.raw('ROQAQH204SLS', 'TT', 'TT26002'))
        nn = c.final('ROQAQH204SLS', c.raw('ROQAQH204SLS', 'NN', 'NN26002'))
        anywhere = c.final('ROQAQH204SLS', c.raw('ROQAQH204SLS', 'UNKNOWN'))
        self.assertEqual(tt['price'], nn['price'])
        self.assertEqual(c.combined('ROQAQH204SLS')['rule'], 'average')
        self.assertIn('ddp', tt['flags'])
        self.assertNotIn('ddp_mixed', tt['flags'])
        self.assertIn('ddp_mixed', nn['flags'])
        self.assertNotIn('ddp', nn['flags'])
        self.assertEqual((c.ddp_share('ROQAQH204SLS'), c.part_share('UNKNOWN', 'ROQAQH204SLS')), (0.5, 0.5))
        self.assertEqual(anywhere['impShare'], 0.5)
        self.assertIn('ddp_part', anywhere['flags'])
        for f in ('ddp', 'ddp_mixed'):
            self.assertNotIn(f, anywhere['flags'])
        self.assertIn('Synth Factory T made 50% of this style\'s units on record and delivers duty paid (DDP), so '
                      'import costs are added on the other 50% only.', anywhere['basis'])

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

    def test_a_manual_cost_on_goods_the_factory_delivers(self):
        # Everything from a delivering factory is delivered, a typed (manual) cost included; the same manual cost
        # on another factory's goods keeps its own terms.
        ov = [T.ovr('f1', 'style', {'style': 'ROQAQF201SLS'}, 9.4444)]          # FOB, the default
        c = T.ci(settings=TT_QA, overrides=ov)
        r = c.resolve('ROQAQF201SLS', 'TT', 'TT26001')
        self.assertEqual((r['level'], r['fobU'], r['flags']), ('L0', 9.4444, ['ddp']))
        self.assertIn('Synth Factory T delivers QA duty paid (DDP)', r['basis'])
        self.assertEqual(c.resolve('ROQAQF201SLS', 'NN')['flags'], [])
        self.assertEqual(T.ci(overrides=ov).resolve('ROQAQF201SLS', 'TT', 'TT26001')['flags'], [])   # no setting
        ov = [T.ovr('f2', 'style', {'style': 'ROQAQF201SLS'}, 9.4444, terms='DDP')]
        r = T.ci(settings=TT_QA, overrides=ov).resolve('ROQAQF201SLS', 'TT', 'TT26001')
        self.assertEqual((r['level'], r['flags']), ('L0', ['ddp']))
        self.assertNotIn('duty paid', r['basis'])                  # already delivered by its own terms


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
        # A line placed half on the delivering factory's lot and half on another pays import on that half only.
        M = T.by(self.after, 'lines', 'id')['4|ROQAQH204SLS']
        M0 = T.by(self.before, 'lines', 'id')['4|ROQAQH204SLS']
        self.assertEqual(M['dutyRegime'], 'us')
        self.assertIn('ddp_part', M['flags'])
        for f in ('ddp', 'ddp_mixed'):
            self.assertNotIn(f, M['flags'])
        self.assertEqual((M['fob'], M['duty'], M['freight'], M['fees']),
                         (M0['fob'], E.r2(M0['duty'] * 0.5), E.r2(M0['freight'] * 0.5), E.r2(M0['fees'] * 0.5)))
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


TT_ALL = {'factories': {'TT': {'name': 'Synth Factory T', 'origin': 'CN', 'ddp': True}}}
RECORD = [{'production': 'TT26009', 'style': 'ROQAQF301SLS', 'factory': 'TT'}]


def ci_rec(settings, record_rows=None, ledger=()):
    return E.CostIndex(T.COSTBOOK, settings, [], list(ledger), today=T.TODAY, record_rows=record_rows)


class EverythingFromAFactory(unittest.TestCase):
    """factories.<code>.ddp true: the factory delivers everything it makes duty paid, whatever the brand."""

    def test_parse_and_validate(self):
        S = E.merge_settings({'factories': {'TT': {'name': 'x', 'ddp': True, 'ddpBrands': ['QA']},
                                            'NN': {'name': 'y', 'ddp': False, 'ddpBrands': ['QB']},
                                            'AA': {'name': 'z', 'ddp': 'yes'}}})
        self.assertEqual(E.ddp_factory_brands(S), {'TT': E.ALL_BRANDS, 'NN': frozenset({'QB'})})
        clean, problems, dropped = pnl.validate_settings({'factories': {'TT': {'name': 'S', 'ddp': True}}})
        self.assertEqual((problems, dropped, clean['factories']), ([], [], {'TT': {'name': 'S', 'ddp': True}}))
        clean, problems, _ = pnl.validate_settings({'factories': {'TT': {'name': 'S', 'ddp': 'yes'}}})
        self.assertIsNone(clean)
        self.assertIn('not_a_boolean', {p['issue'] for p in problems})

    def test_every_brand(self):
        c = T.ci(settings=TT_ALL)
        for st in ('ROQAQF201SLS', 'RODKQF201SLS', 'RODVQF201SLS'):
            r = c.raw(st, 'TT')
            self.assertNotEqual(r['level'], 'L7', st)
            self.assertIn('ddp', r['flags'], st)
            self.assertTrue(r['basis'].endswith('Synth Factory T delivers duty paid (DDP), so no import costs are added.'),
                            r['basis'])
        self.assertNotIn('ddp', c.raw('RODKQF201SLS', 'NN')['flags'])


class FactoryRecord(unittest.TestCase):
    """Which factory made each style outlives the ledger (the factory record), so stock and sold goods of a
    delivering factory stay delivered after their lines leave the ledger."""

    def test_rows_from_the_scorecard_history(self):
        lines = [{'ref': 'tt26009', 'style': 'roqaqf301sls', 'factory': 'TT', 'outcome': 'completed', 'units': 100},
                 {'ref': 'TT26010', 'style': 'ROQAQF302SLS', 'factory': 'TT', 'outcome': 'open', 'units': 50},
                 {'ref': 'TT26011', 'style': 'ROQAQF303SLS', 'factory': 'TT', 'outcome': 'removed', 'units': 50},
                 {'ref': 'TT26012', 'style': 'ROQAQF304SLS', 'factory': 'TT', 'outcome': 'reissued', 'units': 50},
                 {'ref': 'TT26013', 'style': 'ROQAQF305SLS', 'factory': 'TT', 'outcome': 'moved', 'units': 50},
                 {'ref': 'TT25001', 'style': 'ROQAQF306SLS', 'factory': 'TT', 'outcome': None, 'units': 0},
                 {'ref': 'TT25002', 'style': 'ROQAQF307SLS', 'factory': 'TT', 'outcome': None, 'units': 12},
                 {'ref': '', 'style': 'ROQAQF308SLS', 'outcome': 'open'}, 'junk', None]
        self.assertEqual(E.factory_record_rows(lines), [
            {'production': 'TT26009', 'style': 'ROQAQF301SLS', 'factory': 'TT', 'units': 100},
            {'production': 'TT26010', 'style': 'ROQAQF302SLS', 'factory': 'TT', 'units': 50},
            {'production': 'TT25002', 'style': 'ROQAQF307SLS', 'factory': 'TT', 'units': 12}])
        for bad in (None, 'x', {}, 5):
            self.assertEqual(E.factory_record_rows(bad), [])

    def test_made_by(self):
        c = ci_rec(TT_ALL, RECORD, T.src()['ledger']['rows'])
        self.assertEqual(c.made_by['ROQAQF301SLS'], frozenset({'TT'}))                  # the record
        self.assertEqual(c.made_by['ROQAQF201SLS'], frozenset({'TT'}))                  # the ledger
        self.assertEqual(c.made_by['ROQAQH204SLS'], frozenset({'TT', 'NN'}))
        self.assertEqual(c.made_by['ROQAQF101SLS'], frozenset({'AA'}))                  # the factory list
        self.assertNotIn('ROQAQF999SLS', c.made_by)

    def test_stock_and_history_with_no_known_factory(self):
        c = ci_rec(TT_ALL, RECORD, T.src()['ledger']['rows'])
        r = c.final('ROQAQF301SLS', c.raw('ROQAQF301SLS', 'UNKNOWN'))
        self.assertIn('ddp', r['flags'])
        self.assertIn('Made by Synth Factory T, which delivers duty paid (DDP), so no import costs are added.', r['basis'])
        self.assertIn('ddp', c.final('ROQAQF201SLS', c.raw('ROQAQF201SLS', 'UNKNOWN'))['flags'])
        two = c.final('ROQAQH204SLS', c.raw('ROQAQH204SLS', 'UNKNOWN'))              # TT and NN made it
        self.assertNotIn('ddp', two['flags'])
        self.assertNotIn('ddp', ci_rec(TT_ALL).raw('ROQAQF301SLS', 'UNKNOWN')['flags'])    # no record: not known
        self.assertNotIn('ddp', ci_rec({}, RECORD).raw('ROQAQF301SLS', 'UNKNOWN')['flags'])  # no setting

    def test_dataset(self):
        def run(record):
            s = T.src()
            s['inventory']['items'] = s['inventory']['items'] + [T.inv('ROQAQF301SLS-M', tr=100)]
            if record is not None:
                s['factory_record'] = {'rows': record, 'generatedAt': '2026-03-01T06:00:00Z'}
            return T.build(s, settings=TT_ALL)
        ds = run(RECORD)
        inv = T.by(ds, 'inventory', 'sku')['ROQAQF301SLS-M']
        self.assertEqual((inv['dutyRegime'], inv['landed'], inv['landedU']), ('none', inv['fob'], inv['fobU']))
        self.assertIn('ddp', inv['flags'])
        st = T.by(ds, 'styles', 'base')['ROQAQF301SLS']
        self.assertEqual((st['landedU'], st['dutyRegime']), (st['fobU'], 'none'))
        self.assertEqual((ds['inputs']['factory_record_rows'], ds['inputs']['factory_record_at']),
                         (1, '2026-03-01T06:00:00Z'))
        no = run(None)
        inv0 = T.by(no, 'inventory', 'sku')['ROQAQF301SLS-M']
        self.assertEqual((inv0['dutyRegime'], inv0['fob']), ('us', inv['fob']))
        self.assertGreater(inv0['landed'], inv0['fob'])
        self.assertEqual((no['inputs']['factory_record_rows'], no['inputs']['factory_record_at']), (0, None))


class LedgerLinesWithNoFactory(unittest.TestCase):
    """Nov 12 2026 regression: the live ledger holds lines whose ref names no factory (N/A, blank). The passes that
    run while the cost index is built resolve them, so the factory record must exist before them."""

    def test_builds_with_na_and_blank_refs(self):
        ledger = T.src()['ledger']['rows'] + [T.led('N/A', 'ROQAQF301SLS', 40), T.led('', 'ROQAQF302SLS', 20)]
        for settings in (TT_ALL, TT_QA, {}):
            c = ci_rec(settings, RECORD, ledger)
            r = c.final('ROQAQF301SLS', c.raw('ROQAQF301SLS', 'N/A'))
            self.assertEqual('ddp' in r['flags'], bool(settings), settings)         # on record as TT's QA goods
            self.assertNotIn('ddp', c.raw('ROQAQF302SLS', 'BLANK')['flags'])        # no factory on record
        s = T.src()
        s['ledger']['rows'] = ledger
        s['factory_record'] = {'rows': RECORD, 'generatedAt': '2026-03-01T06:00:00Z'}
        for settings in (TT_ALL, TT_QA, {}):
            ds = T.build(s, settings=settings)
            P = {(r['ref'], r['base']): r for r in T.rows(ds, 'production')}
            self.assertEqual(P[('N/A', 'ROQAQF301SLS')]['dutyRegime'], 'none' if settings else 'us', settings)


SPLIT = [{'production': 'TT26009', 'style': 'ROQAQF301SLS', 'factory': 'TT', 'units': 300},
         {'production': 'NN26009', 'style': 'ROQAQF301SLS', 'factory': 'NN', 'units': 100}]


class SplitByShare(unittest.TestCase):
    """David, Nov 12 2026: a style made partly by a factory that delivers duty paid pays import, where the goods cannot
    be tied to one factory (untied stock, sales, the style's own landed cost), on the other factories' share of its
    units on record. Here TT made 300 of 400 units, so those rows pay a quarter of the import."""

    def test_money_helpers(self):
        S = E.merge_settings({})
        full = E.adders(100.0, 10, 'dress_shirt', 'mmf', 'CN', S, 'us')
        for imp in (None, 1, 1.0, 2):
            self.assertEqual(E.adders(100.0, 10, 'dress_shirt', 'mmf', 'CN', S, 'us', imp), full, imp)
        self.assertEqual(E.adders(100.0, 10, 'dress_shirt', 'mmf', 'CN', S, 'us', 0.25), tuple(E.r2(x * 0.25) for x in full))
        self.assertEqual(E.adders(100.0, 10, 'dress_shirt', 'mmf', 'CN', S, 'us', 0), (0.0, 0.0, 0.0))
        self.assertEqual(E.stock_money(100.0, 10, 'dress_shirt', 'mmf', 'CN', 'us', S, 0.25)[0],
                         E.r2(100.0 + sum(E.r2(x * 0.25) for x in full)))
        m = E.line_money(200.0, 100.0, 10, 'dress_shirt', 'mmf', 'CN', 'us', 0, 0, S, 0.25)
        self.assertEqual((m['duty'], m['freight'], m['fees']), tuple(E.r2(x * 0.25) for x in full))
        L = E.landed(10.0, {'cat': 'dress_shirt', 'fiber': 'mmf'}, S, 'us', 'CN')
        L4 = E.landed(10.0, {'cat': 'dress_shirt', 'fiber': 'mmf'}, S, 'us', 'CN', imp=0.25)
        self.assertEqual(L4['duty'], E.r4(L['duty'] * 0.25))
        self.assertEqual(L4['landedU'], E.r4(10.0 + L4['duty'] + L4['freight'] + L4['fees']))
        self.assertEqual(E.landed(10.0, {'cat': 'dress_shirt', 'fiber': 'mmf'}, S, 'us', 'CN', imp=None), L)

    def test_shares(self):
        c = ci_rec(TT_ALL, SPLIT)
        self.assertEqual(c.made_units['ROQAQF301SLS'], {'TT': 300.0, 'NN': 100.0})
        self.assertEqual(c.ddp_share('ROQAQF301SLS'), 0.75)
        self.assertEqual(c.part_share('UNKNOWN', 'ROQAQF301SLS'), 0.25)
        self.assertEqual(c.part_share('WH', 'ROQAQF301SLS'), 0.25)
        self.assertIsNone(c.part_share('TT', 'ROQAQF301SLS'))           # a known factory is all or nothing
        self.assertIsNone(c.part_share('UNKNOWN', 'ROQAQF999SLS'))      # nothing on record
        self.assertFalse(c.delivers('UNKNOWN', 'ROQAQF301SLS'))
        self.assertIsNone(ci_rec({}, SPLIT).part_share('UNKNOWN', 'ROQAQF301SLS'))   # no factory delivers
        r = c.final('ROQAQF301SLS', c.raw('ROQAQF301SLS', 'UNKNOWN'))
        self.assertEqual((r['impShare'], 'ddp_part' in r['flags'], 'ddp' in r['flags']), (0.25, True, False))
        self.assertIn('Synth Factory T made 75% of this style', r['basis'])

    def _ds(self, settings, record, overrides=None):
        s = history_only('ROQAQF301SLS')
        s['inventory']['items'] = s['inventory']['items'] + [T.inv('ROQAQF301SLS-M', tr=100)]
        s['factory_record'] = {'rows': record, 'generatedAt': '2026-03-01T06:00:00Z'}
        return T.build(s, settings=settings, overrides=overrides)

    def test_stock_sales_and_the_style_row(self):
        full, ds = self._ds({}, SPLIT), self._ds(TT_ALL, SPLIT)
        inv0, inv = T.by(full, 'inventory', 'sku')['ROQAQF301SLS-M'], T.by(ds, 'inventory', 'sku')['ROQAQF301SLS-M']
        d, f, e = E.adders(inv['fob'], inv['units'], inv['cat'], inv['fiber'], inv['origin'], ds['settings'], 'us')
        self.assertEqual((inv['dutyRegime'], inv['fob']), ('us', inv0['fob']))
        self.assertEqual(inv['landed'], E.r2(inv['fob'] + E.r2(d * 0.25) + E.r2(f * 0.25) + E.r2(e * 0.25)))
        self.assertEqual(inv0['landed'], E.r2(inv0['fob'] + d + f + e))
        self.assertIn('ddp_part', inv['flags'])
        tab = lambda d_, t: [dict(zip(d_['shipped'][t]['fields'], r)) for r in d_['shipped'][t]['rows']]
        bc = {(x['cust'], x['base']): x for x in tab(ds, 'byCustomer')}[('ROSS', 'ROQAQF301SLS')]
        bc0 = {(x['cust'], x['base']): x for x in tab(full, 'byCustomer')}[('ROSS', 'ROQAQF301SLS')]
        self.assertEqual((bc['dutyRegime'], bc['fob']), ('us', bc0['fob']))
        self.assertEqual((bc['duty'], bc['freight'], bc['fees']),
                         (E.r2(bc0['duty'] * 0.25), E.r2(bc0['freight'] * 0.25), E.r2(bc0['fees'] * 0.25)))
        bs = {x['base']: x for x in tab(ds, 'byStyle')}['ROQAQF301SLS']
        self.assertEqual((bs['fobShare'], bs['caShare']), (0.75, 0.0))       # the page's own math gives a quarter
        st, st0 = T.by(ds, 'styles', 'base')['ROQAQF301SLS'], T.by(full, 'styles', 'base')['ROQAQF301SLS']
        self.assertEqual(st['landedU'], E.landed(st['fobU'], {'cat': st['cat'], 'fiber': st['fiber']}, ds['settings'], 'us',
                                                 'CN', imp=0.25)['landedU'])
        self.assertLess(st['t12Cogs'], st0['t12Cogs'])
        self.assertGreater(st['t12Cogs'], E.r2(30 * st['fobU']) + E.r2(30 * st['fobU']))

    def test_own_price_terms_still_win(self):
        # A style whose own cost is a delivered price (a DDP manual cost) stays delivered on every row.
        ov = [T.ovr('s1', 'style', {'style': 'ROQAQF301SLS'}, 9.6666, terms='DDP')]
        ds = self._ds(TT_ALL, SPLIT, overrides=ov)
        inv = T.by(ds, 'inventory', 'sku')['ROQAQF301SLS-M']
        self.assertEqual((inv['dutyRegime'], inv['landed']), ('none', inv['fob']))
        bc = {(x['cust'], x['base']): x for x in [dict(zip(ds['shipped']['byCustomer']['fields'], r))
                                                   for r in ds['shipped']['byCustomer']['rows']]}[('ROSS', 'ROQAQF301SLS')]
        self.assertEqual((bc['dutyRegime'], bc['duty']), ('none', 0.0))

    def test_a_ledger_line_with_no_factory(self):
        s = T.src()
        s['ledger']['rows'] = s['ledger']['rows'] + [T.led('N/A', 'ROQAQF301SLS', 40)]
        s['factory_record'] = {'rows': SPLIT, 'generatedAt': '2026-03-01T06:00:00Z'}
        P = {(r['ref'], r['base']): r for r in T.rows(T.build(s, settings=TT_ALL), 'production')}[('N/A', 'ROQAQF301SLS')]
        d, f, e = E.adders(P['fob'], P['units'], P['cat'], P['fiber'], P['origin'], E.merge_settings(TT_ALL), 'us')
        self.assertEqual((P['dutyRegime'], P['landed']),
                         ('us', E.r2(P['fob'] + E.r2(d * 0.25) + E.r2(f * 0.25) + E.r2(e * 0.25))))
        self.assertIn('ddp_part', P['flags'])

    def test_no_setting_no_change(self):
        a, b = self._ds({}, SPLIT), self._ds({}, [])
        for t in ('lines', 'apo', 'inventory', 'production', 'styles'):
            self.assertEqual(a[t], b[t], t)
        self.assertEqual(a['shipped']['byCustomer'], b['shipped']['byCustomer'])


class ServiceWiring(unittest.TestCase):
    def test_sources_and_the_rebuild_stamp(self):
        raw = {'factory_record': {'rows': [{'production': ' tt26009 ', 'style': 'roqaqf301sls', 'factory': 'tt', 'x': 1},
                                           {'production': '', 'style': 'ROQAQF302SLS'}, 'junk'],
                                  'generatedAt': '2026-03-01T06:00:00Z'}}
        norm = pnl._normalize_sources(raw)
        self.assertEqual(norm['factory_record'], {'rows': [{'production': 'TT26009', 'style': 'ROQAQF301SLS',
                                                            'factory': 'TT', 'units': 0}], 'generatedAt': '2026-03-01T06:00:00Z'})
        self.assertEqual(pnl._normalize_sources({})['factory_record'], {'rows': [], 'generatedAt': None})
        self.assertIn('factory_record', pnl.SOURCE_KEYS)
        a = pnl._stamps(pnl._normalize_sources({}))
        b = pnl._stamps(norm)
        self.assertNotEqual(a, b)                                       # a new record rebuilds the P&L
        self.assertEqual(b, pnl._stamps(pnl._normalize_sources(raw)))


class ScorecardLines(unittest.TestCase):
    def test_made_lines_keeps_five_fields_and_reads_once_per_build(self):
        import factory_scorecard as F
        reads = []
        store = {'status': {'generated_at': 'G1'},
                 'lines': {'generated_at': 'G1', 'lines': [{'ref': 'TT26009', 'style': 'ROQAQF301SLS', 'factory': 'TT',
                                                            'outcome': 'open', 'units': 5, 'changes': [1, 2, 3]}]}}

        def fake(key, version_id=None):
            reads.append(key)
            return store['status'] if key.endswith('status.json') else store['lines'] if key.endswith('lines.json') else None
        saved = (F._s3_get_json, dict(F._made_memo), F._mem.get('lines'))
        try:
            F._s3_get_json = fake
            F._made_memo.update(generated_at=None, lines=None)
            F._mem['lines'] = None
            got = F.made_lines()
            self.assertEqual(got, {'generated_at': 'G1', 'lines': [{'ref': 'TT26009', 'style': 'ROQAQF301SLS',
                                                                    'factory': 'TT', 'outcome': 'open', 'units': 5}]})
            F.made_lines()
            self.assertEqual(sum(1 for k in reads if k.endswith('lines.json')), 1)       # memo per build
            store['status'] = {'generated_at': 'G2'}
            store['lines'] = {'generated_at': 'G2', 'lines': []}
            self.assertEqual(F.made_lines(), {'generated_at': 'G2', 'lines': []})       # a new build is read
            store['lines'] = None
            store['status'] = {'generated_at': 'G3'}
            F._made_memo.update(generated_at=None, lines=None)
            self.assertIsNone(F.made_lines())                                           # nothing built yet
        finally:
            F._s3_get_json = saved[0]
            F._made_memo.clear()
            F._made_memo.update(saved[1])
            F._mem['lines'] = saved[2]


if __name__ == '__main__':
    unittest.main()
