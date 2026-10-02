"""One worker per instance does the once-per-instance chores (Oct 2 2026).

Four gunicorn workers each uploaded every export to S3 on every run (about
75 MB a run, 1,270 GB of billed bandwidth in September 2026) and each ran the
daily scorecard rebuild at the same minute. Offline: S3 is a recorder and the
leader lock is exercised with a fake fcntl."""
import io
import os
import sys
import types
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _support import load_app  # noqa: E402

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


class UploadOnce(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.app = load_app()

    def setUp(self):
        a = self.app
        self._saved = (a.s3_upload_export, a.worker_leader.is_leader)
        self.sent = []
        a.s3_upload_export = lambda key, b: self.sent.append(key) or True
        a._export_uploaded_inputs.clear()

    def tearDown(self):
        self.app.s3_upload_export, self.app.worker_leader.is_leader = self._saved
        self.app._export_uploaded_inputs.clear()

    def test_a_worker_that_is_not_the_leader_never_uploads(self):
        self.app.worker_leader.is_leader = lambda: False
        self.assertFalse(self.app._upload_export_once('exports/Nautica_2026-10-02.xlsx', b'x', (1, 'a', 'b', 'c')))
        self.assertEqual(self.sent, [])

    def test_the_leader_uploads_once_per_set_of_inputs(self):
        self.app.worker_leader.is_leader = lambda: True
        k = 'exports/Nautica_2026-10-02.xlsx'
        self.assertTrue(self.app._upload_export_once(k, b'x', (1, 'a', 'b', 'c')))
        self.assertFalse(self.app._upload_export_once(k, b'x', (1, 'a', 'b', 'c')))   # same data again
        self.assertTrue(self.app._upload_export_once(k, b'y', (2, 'a', 'b', 'c')))    # inventory changed
        self.assertEqual(self.sent, [k, k])

    def test_a_new_day_is_a_new_key_so_it_still_gets_its_copy(self):
        self.app.worker_leader.is_leader = lambda: True
        inputs = (1, 'a', 'b', 'c')
        self.app._upload_export_once('exports/Nautica_2026-10-02.xlsx', b'x', inputs)
        self.app._upload_export_once('exports/Nautica_2026-10-03.xlsx', b'x', inputs)
        self.assertEqual(len(self.sent), 2)

    def test_a_failed_upload_is_retried_next_run(self):
        self.app.worker_leader.is_leader = lambda: True
        self.app.s3_upload_export = lambda key, b: self.sent.append(key) or False
        k = 'exports/DKNY_2026-10-02.xlsx'
        self.app._upload_export_once(k, b'x', (1, 'a', 'b', 'c'))
        self.app._upload_export_once(k, b'x', (1, 'a', 'b', 'c'))
        self.assertEqual(self.sent, [k, k])

    def test_export_run_goes_through_the_gate(self):
        src = io.open(os.path.join(ROOT, 'app.py'), encoding='utf-8').read()
        i = src.index('def generate_all_exports')
        body = src[i:i + 9000]
        self.assertNotIn('s3_upload_export(', body)
        self.assertEqual(body.count('_upload_export_once('), 2)


class LeaderLock(unittest.TestCase):
    """The real lock, with fcntl faked so this runs on Windows too."""

    def _fresh(self, held_by_other=False):
        import importlib
        import worker_leader as wl
        wl = importlib.reload(wl)
        calls = []

        def flock(fd, op):
            calls.append(op)
            if held_by_other:
                raise BlockingIOError('held')
        fake = types.SimpleNamespace(flock=flock, LOCK_EX=2, LOCK_NB=4)
        sys.modules['fcntl'] = fake
        self.addCleanup(lambda: sys.modules.pop('fcntl', None))
        tmp = os.path.join(os.path.dirname(os.path.abspath(__file__)), '_leader_test.lock')
        wl.LOCK_PATH = tmp
        self.addCleanup(lambda: os.path.exists(tmp) and os.remove(tmp))
        return wl, calls

    def test_first_worker_takes_the_lock_and_keeps_it(self):
        wl, calls = self._fresh()
        self.assertTrue(wl.is_leader())
        self.assertTrue(wl.is_leader())
        self.assertEqual(len(calls), 1, 'the lock is taken once and held, not re-taken per call')
        wl._fh.close()

    def test_a_second_worker_is_not_the_leader_and_asks_again_later(self):
        wl, calls = self._fresh(held_by_other=True)
        self.assertFalse(wl.is_leader())
        self.assertFalse(wl.is_leader())
        self.assertEqual(len(calls), 2, 'keeps trying, so it takes over if the leader dies')

    def test_no_writable_lock_file_falls_back_to_old_behaviour(self):
        wl, _ = self._fresh()
        wl.LOCK_PATH = os.path.join(ROOT, 'no-such-dir', 'x', 'leader.lock')
        self.assertTrue(wl.is_leader())


class ScorecardDaily(unittest.TestCase):
    def test_daily_job_runs_only_in_the_leader(self):
        src = io.open(os.path.join(ROOT, 'factory_scorecard.py'), encoding='utf-8').read()
        i = src.index('def _daily_loop')
        body = src[i:i + 2500]
        lead = body.index('worker_leader.is_leader()')
        self.assertLess(lead, body.index('write_own_snapshot()'))
        self.assertLess(lead, body.index("rebuild(trigger='daily')"))


if __name__ == '__main__':
    unittest.main()
