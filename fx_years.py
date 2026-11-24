"""Yearly average yuan per US dollar for the admin Inventory Analytics (Nov 24 2026).

Why (Charlesy, through David, Nov 24 2026): "the average annual conversion calculated into the numbers". The
calculator price sheets fix each price in yuan and print the dollar figure at the rate of the day the sheet was
saved, so every sale was costed at that one rate, whatever year it shipped in. With each year's own average rate the
page restates the yuan part of a cost for the year the units sold in (the P&L dataset's fxShare and fxRef say how
much of a cost is yuan based and at which printed rate).

Public market data only, never a cost: the European Central Bank's daily euro reference rates. Yuan per dollar on a
day = (yuan per euro) / (dollars per euro); a year's rate is the mean over the days the ECB published both. Completed
years are fixed below (computed from the ECB series on Nov 24 2026). Any later year is fetched in the background at
most once per FETCH_TTL and kept in memory; until a fetch lands, SEED serves. get() never blocks and never raises.
"""
import csv
import io
import threading
import time

ECB_URL = 'https://data-api.ecb.europa.eu/service/data/EXR/D.CNY+USD.EUR.SP00.A'
SOURCE = ('European Central Bank euro reference rates (yuan per euro divided by dollars per euro), '
          'the mean of the days published in each year')

FIXED = {'2019': 6.9114, '2020': 6.9003, '2021': 6.4491, '2022': 6.7347, '2023': 7.0846, '2024': 7.1956,
         '2025': 7.1874}
FIXED_AS_OF = '2026-11-24'
SEED = {'2026': {'rate': 6.7969, 'days': 230, 'through': '2026-11-24'}}

FETCH_TTL = 12 * 3600.0
FAIL_BACKOFF = 3600.0
TIMEOUT = (5, 20)
SANE = (4.0, 12.0)          # a yearly rate outside this band is a bad read, never served

_lock = threading.Lock()
_state = {'years': {}, 'at': 0.0, 'fail_until': 0.0, 'refreshing': False}


def parse_ecb_csv(text):
    """{year: {'rate', 'days', 'through'}} from the ECB csvdata of the daily CNY and USD series (both per euro).
    A day counts only when both were published; a year's rate is the mean of its days, 4 decimals."""
    per_day = {}
    for row in csv.DictReader(io.StringIO(text or '')):
        cur = str(row.get('CURRENCY') or '').strip()
        day = str(row.get('TIME_PERIOD') or '').strip()
        raw = str(row.get('OBS_VALUE') or '').strip()
        if cur not in ('CNY', 'USD') or len(day) != 10 or not raw:
            continue
        try:
            v = float(raw)
        except ValueError:
            continue
        if v > 0:
            per_day.setdefault(day, {})[cur] = v
    sums = {}
    for day in sorted(per_day):
        d = per_day[day]
        if 'CNY' in d and 'USD' in d:
            s = sums.setdefault(day[:4], [0.0, 0, ''])
            s[0] += d['CNY'] / d['USD']
            s[1] += 1
            s[2] = day
    out = {}
    for y, (tot, n, last) in sums.items():
        rate = round(tot / n, 4)
        if SANE[0] < rate < SANE[1]:
            out[y] = {'rate': rate, 'days': n, 'through': last}
    return out


def open_years(now=None):
    """The years after the last fixed one, up to the current UTC year: the ones fetched live."""
    now = time.time() if now is None else now
    last = max(int(y) for y in FIXED)
    cur = time.gmtime(now).tm_year
    return [str(y) for y in range(last + 1, cur + 1)]


def _fetch(years, http_get=None):
    """One ECB request covering `years`; what it returns lands in memory. Runs off the request thread."""
    try:
        if http_get is None:
            import requests
            http_get = requests.get
        resp = http_get(ECB_URL, params={'startPeriod': '%s-01-01' % min(years),
                                         'endPeriod': '%s-12-31' % max(years), 'format': 'csvdata'},
                        timeout=TIMEOUT)
        if getattr(resp, 'status_code', None) != 200:
            raise RuntimeError('status')
        got = {y: v for y, v in parse_ecb_csv(resp.text).items() if y in years}
        if not got:
            raise ValueError('no days')
        with _lock:
            _state['years'].update(got)
            _state['at'] = time.time()
            _state['fail_until'] = 0.0
        return got
    except Exception as e:
        print('[FX] ECB rate fetch failed: %s' % type(e).__name__, flush=True)
        with _lock:
            _state['fail_until'] = time.time() + FAIL_BACKOFF
        return None
    finally:
        with _lock:
            _state['refreshing'] = False


def get(now=None, http_get=None, spawn=True):
    """The yearly rates block of /api/pnl/analytics:
        {'ready': True, 'rates': {year: yuan per dollar}, 'through': {year: last day counted},
         'live': [years read from the ECB by this worker], 'source': SOURCE, 'fixedAsOf': FIXED_AS_OF}
    Completed years come from FIXED; later years from the last ECB fetch, else SEED. A due refresh (never
    fetched, or older than FETCH_TTL, and not backing off after a failure) starts one background fetch;
    spawn=False runs it inline (tests)."""
    now = time.time() if now is None else now
    years = open_years(now)
    start = False
    with _lock:
        if (years and not _state['refreshing'] and now >= _state['fail_until']
                and now - _state['at'] >= FETCH_TTL):
            _state['refreshing'] = True
            start = True
    if start:
        if spawn:
            try:
                threading.Thread(target=_fetch, args=(years, http_get), daemon=True, name='fx-years').start()
            except Exception:
                with _lock:
                    _state['refreshing'] = False
                    _state['fail_until'] = now + FAIL_BACKOFF
        else:
            _fetch(years, http_get)
    with _lock:
        held = dict(_state['years'])
    rates = dict(FIXED)
    through = {y: y + '-12-31' for y in FIXED}
    live = []
    for y in years:
        v = held.get(y) or SEED.get(y)
        if v:
            rates[y] = v['rate']
            through[y] = v['through']
            if y in held:
                live.append(y)
    return {'ready': True, 'rates': rates, 'through': through, 'live': live, 'source': SOURCE,
            'fixedAsOf': FIXED_AS_OF}


def _reset_for_tests():
    with _lock:
        _state.update(years={}, at=0.0, fail_until=0.0, refreshing=False)
