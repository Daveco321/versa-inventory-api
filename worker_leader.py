"""One gunicorn worker per instance does the once-per-instance chores.

The service runs several gunicorn workers on one Render instance, and every
worker used to run the same background jobs: each rebuilt and uploaded the
28 brand exports to S3 (about 75 MB per run, billed as outbound bandwidth
because the bucket is in us-east-2 and Render is in Oregon), and each ran the
daily factory scorecard rebuild at the same minute. On Oct 2 2026 that was
1,270 GB of bandwidth in September and the memory-limit restarts David kept
getting emails about.

is_leader() hands one worker an exclusive, non-blocking flock on a file in
/tmp (shared by every worker on the instance) and keeps it for the life of
that process. The OS drops the lock when the process exits, so if the leader
dies another worker takes over the next time it asks. On platforms without
fcntl (local Windows runs, the offline tests) there is only one process, so
it is always the leader.
"""
import os
import threading

LOCK_PATH = os.environ.get('LEADER_LOCK_PATH', '/tmp/versa-inventory-api.leader.lock')

_lock = threading.Lock()
_fh = None


def is_leader():
    global _fh
    with _lock:
        if _fh is not None:
            return True
        try:
            import fcntl
        except ImportError:
            return True
        try:
            fh = open(LOCK_PATH, 'a+')
        except OSError:
            # Cannot coordinate at all (no writable /tmp): behave as before
            # rather than letting NO worker run the jobs.
            return True
        try:
            fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            fh.close()
            return False        # another worker is the leader
        except OSError:
            fh.close()
            return True         # locking unsupported here: same fallback as above
        _fh = fh
        return True
