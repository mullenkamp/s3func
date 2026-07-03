import pytest
import os, pathlib
import uuid
from time import sleep
import concurrent.futures

try:
    import tomllib as toml
except ImportError:
    import tomli as toml
from s3func import s3, b2

#################################################
### Parameters

script_path = pathlib.Path(os.path.realpath(os.path.dirname(__file__)))

# Legacy credentials for B2-native lock tests
try:
    with open(script_path.joinpath('s3_config.toml'), "rb") as f:
        conn_config = toml.load(f)['connection_config']
    legacy_endpoint_url = conn_config['endpoint_url']
    legacy_access_key_id = conn_config['aws_access_key_id']
    legacy_access_key = conn_config['aws_secret_access_key']
except:
    legacy_endpoint_url = os.environ.get('endpoint_url')
    legacy_access_key_id = os.environ.get('aws_access_key_id')
    legacy_access_key = os.environ.get('aws_secret_access_key')

legacy_bucket = 'achelous'


################################################
### Helper functions


def worker_lock(session, key, results, timeout=120):
    """Worker function for concurrency test."""
    lock = session.lock(key)
    acquired = lock.acquire(blocking=True, timeout=timeout)
    if acquired:
        results.append(True)
        sleep(0.1)
        lock.release()
    else:
        results.append(False)


################################################
### S3-compatible lock tests (parameterized)


def test_s3_lock(s3_session):
    """Test lock acquire, release, and context manager."""
    obj_key = uuid.uuid4().hex

    lock = s3_session.lock(obj_key)

    other_locks = lock.other_locks()
    assert isinstance(other_locks, dict)

    if other_locks:
        lock.break_other_locks()

    assert not lock.locked()
    assert lock.acquire()
    assert lock.locked()

    lock.release()
    assert not lock.locked()

    with lock:
        assert lock.locked()

    assert not lock.locked()


def test_s3_lock_concurrency(s3_session):
    """Verify multiple threads can acquire the lock sequentially."""
    concurrency_key = uuid.uuid4().hex + '.concurrent'
    results = []

    initial_lock = s3_session.lock(concurrency_key)
    if initial_lock.other_locks():
        initial_lock.break_other_locks()

    num_workers = 2
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
        futures = [executor.submit(worker_lock, s3_session, concurrency_key, results) for _ in range(num_workers)]
        concurrent.futures.wait(futures)

    assert sum(results) == num_workers


################################################
### B2-native lock tests (not parameterized)


def test_B2Lock():
    """Test B2 native lock."""
    if not legacy_access_key_id:
        pytest.skip('Legacy B2 credentials not available')

    b2_session = b2.B2Session(legacy_access_key_id, legacy_access_key)
    obj_key = uuid.uuid4().hex

    lock = b2_session.lock(obj_key)

    other_locks = lock.other_locks()
    assert isinstance(other_locks, dict)

    if other_locks:
        lock.break_other_locks()

    assert not lock.locked()
    assert lock.acquire()
    assert lock.locked()

    lock.release()
    assert not lock.locked()

    with lock:
        assert lock.locked()

    assert not lock.locked()


def test_B2Lock_concurrency():
    """Verify B2 lock concurrency."""
    if not legacy_access_key_id:
        pytest.skip('Legacy B2 credentials not available')

    concurrency_key = uuid.uuid4().hex + '.concurrent'
    results = []

    b2_session = b2.B2Session(legacy_access_key_id, legacy_access_key)

    initial_lock = b2_session.lock(concurrency_key)
    if initial_lock.other_locks():
        initial_lock.break_other_locks()

    num_workers = 4
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
        futures = [executor.submit(worker_lock, b2_session, concurrency_key, results) for _ in range(num_workers)]
        concurrent.futures.wait(futures)

    assert sum(results) == num_workers


def test_s3_lock_shared(s3_session):
    """Two shared holders coexist; an exclusive contender is blocked until both release."""
    obj_key = uuid.uuid4().hex
    r1 = s3_session.lock(obj_key)
    r2 = s3_session.lock(obj_key)
    w = s3_session.lock(obj_key)
    try:
        assert r1.acquire(exclusive=False) is True
        assert r2.acquire(exclusive=False) is True
        assert w.acquire(blocking=False) is False
        r1.release()
        r2.release()
        assert w.acquire(timeout=60) is True
    finally:
        r1.release()
        r2.release()
        w.release()
        assert w.locked() is False
