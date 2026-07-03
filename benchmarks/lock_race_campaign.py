"""Instrumented lock-race attribution campaign (plan Item 1).

Multi-process contenders hammer one S3Lock key. Ground truth for mutual exclusion
is a local overlap ledger (atomic O_APPEND writes of ENTER/EXIT + monotonic ns).
Every listing poll made by any lock is logged raw to per-process JSONL (via a
wrapper around DistributedLock._list_objects) so any violation is attributable.

Usage:
  python lock_race_campaign.py <out_dir> [--minutes 15] [--procs 4] [--churn 0]
                               [--load-threads 0] [--config <s3_config.toml path>]

Conditions are composed by the caller (fresh vs churned prefix, load vs idle).
Exit code 1 if any mutual-exclusion violation is detected.
"""
import argparse
import json
import multiprocessing as mp
import os
import pathlib
import re
import threading
import time
import uuid

from s3func import s3
from s3func import locking


def load_cfg(path):
    txt = pathlib.Path(path).read_text()
    return dict(re.findall(r"^(\w+)\s*=\s*['\"](.+?)['\"]", txt, re.M))


def make_session(cfg, bucket):
    return s3.S3Session(cfg['access_key_id'], cfg['access_key'], bucket,
                        endpoint_url=cfg['endpoint_url'])


def instrument_polls(jsonl_path):
    """Wrap DistributedLock._list_objects to log every poll's raw entries."""
    original = locking.DistributedLock._list_objects
    f = open(jsonl_path, 'a', buffering=1)

    def logged(session, obj_lock_key, lock_id=None):
        t0 = time.monotonic_ns()
        try:
            res = original(session, obj_lock_key, lock_id)
            f.write(json.dumps({
                't_ns': t0, 'pid': os.getpid(), 'prefix': obj_lock_key,
                'entries': [{'key': e.get('key'),
                             'lock_type': e.get('lock_type'),
                             'ts': str(e.get('upload_timestamp'))} for e in res],
            }) + '\n')
            return res
        except Exception as e:
            f.write(json.dumps({'t_ns': t0, 'pid': os.getpid(),
                                'prefix': obj_lock_key, 'error': repr(e)}) + '\n')
            raise

    locking.DistributedLock._list_objects = staticmethod(logged)


def ledger_append(ledger_path, line):
    fd = os.open(ledger_path, os.O_WRONLY | os.O_APPEND | os.O_CREAT, 0o644)
    try:
        os.write(fd, line.encode())
    finally:
        os.close(fd)


def contender(cfg_path, bucket, lock_key, ledger_path, jsonl_dir, stop_ns, hold_s):
    cfg = load_cfg(cfg_path)
    instrument_polls(pathlib.Path(jsonl_dir) / f'polls-{os.getpid()}.jsonl')
    session = make_session(cfg, bucket)
    n_acq = 0
    while time.monotonic_ns() < stop_ns:
        lock = session.lock(lock_key)
        try:
            got = lock.acquire(timeout=30)
        except Exception as e:
            ledger_append(ledger_path, f'{os.getpid()} ERROR {time.monotonic_ns()} {e!r}\n')
            continue
        if got:
            ledger_append(ledger_path, f'{os.getpid()} ENTER {time.monotonic_ns()}\n')
            time.sleep(hold_s)
            ledger_append(ledger_path, f'{os.getpid()} EXIT {time.monotonic_ns()}\n')
            lock.release()
            n_acq += 1
    ledger_append(ledger_path, f'{os.getpid()} DONE {time.monotonic_ns()} n={n_acq}\n')


def load_worker(cfg_path, bucket, prefix, stop_evt):
    cfg = load_cfg(cfg_path)
    session = make_session(cfg, bucket)
    i = 0
    while not stop_evt.is_set():
        try:
            session.put_object(f'{prefix}/load-{i % 25}', b'x' * 512)
        except Exception:
            time.sleep(0.1)
        i += 1


def churn(cfg_path, bucket, lock_key, cycles):
    cfg = load_cfg(cfg_path)
    session = make_session(cfg, bucket)
    for _ in range(cycles):
        lock = session.lock(lock_key)
        assert lock.acquire(timeout=30)
        lock.release()


def verify_ledger(ledger_path):
    """Detect overlapping ENTER/EXIT intervals across processes."""
    intervals, opens, errors = [], {}, 0
    for line in pathlib.Path(ledger_path).read_text().splitlines():
        parts = line.split()
        if parts[1] == 'ENTER':
            opens[parts[0]] = int(parts[2])
        elif parts[1] == 'EXIT':
            intervals.append((opens.pop(parts[0]), int(parts[2]), parts[0]))
        elif parts[1] == 'ERROR':
            errors += 1
    intervals.sort()
    violations = []
    for (s1, e1, p1), (s2, e2, p2) in zip(intervals, intervals[1:]):
        if s2 < e1 and p1 != p2:
            violations.append((p1, p2, s2, e1))
    return len(intervals), errors, violations


if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    ap.add_argument('out_dir')
    ap.add_argument('--minutes', type=float, default=15)
    ap.add_argument('--procs', type=int, default=4)
    ap.add_argument('--churn', type=int, default=0)
    ap.add_argument('--load-threads', type=int, default=0)
    ap.add_argument('--hold', type=float, default=0.05)
    ap.add_argument('--bucket', default='achelous')
    ap.add_argument('--config', default='/home/mike/git/ebooklet/ebooklet/tests/s3_config.toml')
    args = ap.parse_args()

    out = pathlib.Path(args.out_dir)
    out.mkdir(parents=True, exist_ok=True)
    run = uuid.uuid4().hex[:8]
    lock_key = f'lock-campaign-{run}/target'
    ledger = out / f'ledger-{run}.log'
    print(f'run={run} lock_key={lock_key} minutes={args.minutes} procs={args.procs} '
          f'churn={args.churn} load={args.load_threads}', flush=True)

    if args.churn:
        t0 = time.monotonic()
        churn(args.config, args.bucket, lock_key, args.churn)
        print(f'churn: {args.churn} acquire/release cycles in {time.monotonic()-t0:.0f}s', flush=True)

    stop_evt = threading.Event()
    loaders = [threading.Thread(target=load_worker,
                                args=(args.config, args.bucket, f'lock-campaign-{run}-load', stop_evt))
               for _ in range(args.load_threads)]
    for t in loaders:
        t.start()

    stop_ns = time.monotonic_ns() + int(args.minutes * 60 * 1e9)
    procs = [mp.Process(target=contender,
                        args=(args.config, args.bucket, lock_key, str(ledger), str(out), stop_ns, args.hold))
             for _ in range(args.procs)]
    for p in procs:
        p.start()
    for p in procs:
        p.join()
    stop_evt.set()
    for t in loaders:
        t.join()

    n, errors, violations = verify_ledger(ledger)
    print(f'RESULT: acquisitions={n} errors={errors} violations={len(violations)}', flush=True)
    for v in violations:
        print(f'  VIOLATION: pids {v[0]}/{v[1]} overlap {v[3]-v[2]} ns around t={v[2]}', flush=True)

    ## cleanup remote
    cfg = load_cfg(args.config)
    session = make_session(cfg, args.bucket)
    for prefix in (f'lock-campaign-{run}', f'lock-campaign-{run}-load'):
        objs = list(session.list_object_versions(prefix=prefix).iter_objects())
        if objs:
            session.delete_objects([{'key': o['key'], **({'version_id': o['version_id']} if o.get('version_id') else {})} for o in objs])
    left = len(list(session.list_object_versions(prefix=f'lock-campaign-{run}').iter_objects()))
    print(f'cleanup leftovers: {left}', flush=True)
    raise SystemExit(1 if violations else 0)
