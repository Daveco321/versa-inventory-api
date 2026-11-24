"""fx_years (Nov 24 2026): each year's average yuan per US dollar for Inventory Analytics.

Offline: every ECB answer here is a made-up CSV served by a fake http_get, so no test touches the network.
The numbers in these CSVs are synthetic (8.0 and 9.0 yuan per euro, 1.0 dollar per euro and the like)."""
import calendar
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import fx_years  # noqa: E402

HDR = 'KEY,FREQ,CURRENCY,CURRENCY_DENOM,EXR_TYPE,EXR_SUFFIX,TIME_PERIOD,OBS_VALUE,OBS_STATUS'


def _csv(rows):
    return '\n'.join([HDR] + ['EXR.D.%s.EUR.SP00.A,D,%s,EUR,SP00,A,%s,%s,A' % (c, c, d, v) for c, d, v in rows])


def _ts(y, m, d):
    return float(calendar.timegm((y, m, d, 12, 0, 0, 0, 0, 0)))


class _Resp:
    def __init__(self, status, text):
        self.status_code = status
        self.text = text


class _Http:
    def __init__(self, status=200, text=''):
        self.status, self.text, self.calls = status, text, []

    def __call__(self, url, params=None, timeout=None):
        self.calls.append((url, dict(params or {}), timeout))
        if isinstance(self.text, Exception):
            raise self.text
        return _Resp(self.status, self.text)


class TestParse(unittest.TestCase):
    def test_a_day_counts_only_with_both_currencies(self):
        text = _csv([('CNY', '2031-01-02', '8.0'), ('USD', '2031-01-02', '1.0'),     # 8.0 yuan per dollar
                     ('CNY', '2031-01-03', '9.0'), ('USD', '2031-01-03', '1.0'),     # 9.0
                     ('CNY', '2031-01-04', '9.9'),                                   # no dollar rate: skipped
                     ('USD', '2031-01-05', '1.1'),                                   # no yuan rate: skipped
                     ('CNY', '2031-01-06', ''), ('USD', '2031-01-06', '1.0'),        # blank value: skipped
                     ('CNY', '2031-01-07', 'x'), ('USD', '2031-01-07', '1.0'),       # not a number: skipped
                     ('GBP', '2031-01-08', '0.9')])                                  # another currency
        got = fx_years.parse_ecb_csv(text)
        self.assertEqual(got, {'2031': {'rate': 8.5, 'days': 2, 'through': '2031-01-03'}})

    def test_years_apart_and_cross_rate(self):
        text = _csv([('CNY', '2031-12-31', '8.4'), ('USD', '2031-12-31', '1.2'),     # 7.0
                     ('CNY', '2032-01-02', '9.0'), ('USD', '2032-01-02', '1.5')])    # 6.0
        got = fx_years.parse_ecb_csv(text)
        self.assertEqual(got['2031']['rate'], 7.0)
        self.assertEqual(got['2032'], {'rate': 6.0, 'days': 1, 'through': '2032-01-02'})

    def test_an_impossible_year_is_dropped(self):
        text = _csv([('CNY', '2031-01-02', '80.0'), ('USD', '2031-01-02', '1.0')])
        self.assertEqual(fx_years.parse_ecb_csv(text), {})
        self.assertEqual(fx_years.parse_ecb_csv(''), {})
        self.assertEqual(fx_years.parse_ecb_csv(None), {})


class TestGet(unittest.TestCase):
    def setUp(self):
        fx_years._reset_for_tests()
        self._quiet = sys.stdout
        sys.stdout = open(os.devnull, 'w')

    def tearDown(self):
        sys.stdout.close()
        sys.stdout = self._quiet
        fx_years._reset_for_tests()

    def test_fixed_years_and_the_seed_before_any_fetch(self):
        http = _Http(500, '')
        last_fixed = max(int(y) for y in fx_years.FIXED)
        now = _ts(last_fixed + 1, 6, 1)
        out = fx_years.get(now=now, http_get=http, spawn=False)
        self.assertTrue(out['ready'])
        for y, v in fx_years.FIXED.items():
            self.assertEqual(out['rates'][y], v)
        seed_year = str(last_fixed + 1)
        if seed_year in fx_years.SEED:
            self.assertEqual(out['rates'][seed_year], fx_years.SEED[seed_year]['rate'])
        self.assertEqual(out['live'], [])                 # the fetch failed
        self.assertEqual(len(http.calls), 1)
        url, params, timeout = http.calls[0]
        self.assertEqual(url, fx_years.ECB_URL)
        self.assertEqual(params['startPeriod'], '%d-01-01' % (last_fixed + 1))
        self.assertEqual(params['format'], 'csvdata')
        self.assertTrue(timeout)

    def test_a_fetch_replaces_the_seed_and_waits_for_its_ttl(self):
        y = max(int(k) for k in fx_years.FIXED) + 1
        http = _Http(200, _csv([('CNY', '%d-02-02' % y, '9.0'), ('USD', '%d-02-02' % y, '1.25')]))   # 7.2
        now = _ts(y, 3, 1)
        out = fx_years.get(now=now, http_get=http, spawn=False)
        self.assertEqual(out['rates'][str(y)], 7.2)
        self.assertEqual(out['through'][str(y)], '%d-02-02' % y)
        self.assertEqual(out['live'], [str(y)])
        fx_years.get(now=now + 60, http_get=http, spawn=False)                  # fresh: no second request
        self.assertEqual(len(http.calls), 1)
        with fx_years._lock:
            fx_years._state['at'] = now - fx_years.FETCH_TTL - 1            # stale now
        fx_years.get(now=now, http_get=http, spawn=False)
        self.assertEqual(len(http.calls), 2)

    def test_a_failure_backs_off_and_never_raises(self):
        y = max(int(k) for k in fx_years.FIXED) + 1
        http = _Http(200, RuntimeError('down'))
        now = _ts(y, 3, 1)
        out = fx_years.get(now=now, http_get=http, spawn=False)
        self.assertTrue(out['ready'])                                           # fixed years still serve
        self.assertEqual(len(http.calls), 1)
        fx_years.get(now=now + 10, http_get=http, spawn=False)                  # backing off
        self.assertEqual(len(http.calls), 1)
        http.text = _csv([('CNY', '%d-02-02' % y, '8.0'), ('USD', '%d-02-02' % y, '1.0')])
        with fx_years._lock:
            fx_years._state['fail_until'] = 0.0
        out = fx_years.get(now=now + 20, http_get=http, spawn=False)
        self.assertEqual(out['rates'][str(y)], 8.0)

    def test_two_open_years_in_one_request(self):
        last = max(int(k) for k in fx_years.FIXED)
        y1, y2 = last + 1, last + 2
        http = _Http(200, _csv([('CNY', '%d-05-05' % y1, '7.0'), ('USD', '%d-05-05' % y1, '1.0'),
                                ('CNY', '%d-01-05' % y2, '6.5'), ('USD', '%d-01-05' % y2, '1.0')]))
        out = fx_years.get(now=_ts(y2, 1, 10), http_get=http, spawn=False)
        self.assertEqual(http.calls[0][1]['startPeriod'], '%d-01-01' % y1)
        self.assertEqual(http.calls[0][1]['endPeriod'], '%d-12-31' % y2)
        self.assertEqual(out['rates'][str(y1)], 7.0)
        self.assertEqual(out['rates'][str(y2)], 6.5)
        self.assertEqual(sorted(out['live']), [str(y1), str(y2)])

    def test_no_open_year_means_no_request(self):
        http = _Http(200, '')
        last = max(int(k) for k in fx_years.FIXED)
        out = fx_years.get(now=_ts(last, 7, 1), http_get=http, spawn=False)
        self.assertEqual(http.calls, [])
        self.assertEqual(out['live'], [])
        self.assertEqual(fx_years.open_years(_ts(last, 7, 1)), [])
        self.assertEqual(fx_years.open_years(_ts(last + 2, 1, 1)), [str(last + 1), str(last + 2)])

    def test_the_fixed_table_is_sane(self):
        for y, v in fx_years.FIXED.items():
            self.assertTrue(fx_years.SANE[0] < v < fx_years.SANE[1], y)
        for y, v in fx_years.SEED.items():
            self.assertNotIn(y, fx_years.FIXED)
            self.assertTrue(fx_years.SANE[0] < v['rate'] < fx_years.SANE[1], y)


if __name__ == '__main__':
    unittest.main()
