import pytest
import os, pathlib
import uuid
import io
import sys
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

try:
    with open(script_path.joinpath('s3_config.toml'), "rb") as f:
        conn_config = toml.load(f)['connection_config']
    endpoint_url = conn_config['endpoint_url']
    access_key_id = conn_config['aws_access_key_id']
    access_key = conn_config['aws_secret_access_key']
except:
    endpoint_url = os.environ.get('endpoint_url')
    access_key_id = os.environ.get('aws_access_key_id')
    access_key = os.environ.get('aws_secret_access_key')

bucket = 'achelous'
obj_key = uuid.uuid4().hex

s3_session = s3.S3Session(access_key_id, access_key, bucket, endpoint_url=endpoint_url)
b2_session = b2.B2Session(access_key_id, access_key)

################################################
### Tests

def test_S3Lock():
    """

    """
    s3lock = s3_session.s3lock(obj_key)

    other_locks = s3lock.other_locks()

    assert isinstance(other_locks, dict)

    if other_locks:
        _ = s3lock.break_other_locks()

    assert not s3lock.locked()

    assert s3lock.acquire()

    assert s3lock.locked()

    s3lock.release()

    assert not s3lock.locked()

    with s3lock:
        assert s3lock.locked()

    assert not s3lock.locked()


def worker_s3_lock(key, results):
    """
    Worker function for concurrency test.
    """
    s3lock = s3_session.s3lock(key)
    # Attempt to acquire lock with blocking and timeout
    acquired = s3lock.acquire(blocking=True, timeout=30)
    if acquired:
        results.append(True)
        sleep(1)  # Hold the lock for a bit
        s3lock.release()
    else:
        results.append(False)


def test_S3Lock_concurrency():
    """
    Suggestion 1: True Concurrency Verification.
    Verifies that multiple threads can acquire the lock sequentially.
    """
    concurrency_key = uuid.uuid4().hex + '.concurrent'
    results = []
    
    # Clean up any existing locks
    initial_lock = s3_session.s3lock(concurrency_key)
    if initial_lock.other_locks():
        initial_lock.break_other_locks()

    num_workers = 3
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
        # Submit multiple workers at once
        futures = [executor.submit(worker_s3_lock, concurrency_key, results) for _ in range(num_workers)]
        concurrent.futures.wait(futures)

    # All workers should have successfully acquired the lock sequentially
    print(f"S3 Concurrency Results: {results}")
    assert sum(results) == num_workers


def s3lock_loop():
    """

    """
    s3lock = s3_session.s3lock(obj_key)

    for i in range(100):
        print(i)
        with s3lock:
            mod_date = s3lock._timestamp
            others = s3lock.other_locks()
            if others:
                for other_one, obj in others.items():
                    if 1 in obj:
                        if obj[1] < mod_date:
                            print(('Other mod date was earlier.'))
                            # raise ValueError('Other mod date was earlier.')


def test_B2Lock():
    """

    """
    s3lock = b2_session.b2lock(obj_key)

    other_locks = s3lock.other_locks()

    assert isinstance(other_locks, dict)

    if other_locks:
        _ = s3lock.break_other_locks()

    assert not s3lock.locked()

    assert s3lock.acquire()

    assert s3lock.locked()

    s3lock.release()

    assert not s3lock.locked()

    with s3lock:
        assert s3lock.locked()

    assert not s3lock.locked()


def worker_b2_lock(key, results):
    """
    Worker function for concurrency test.
    """
    lock = b2_session.b2lock(key)
    # Attempt to acquire lock with blocking and timeout
    acquired = lock.acquire(blocking=True, timeout=30)
    if acquired:
        results.append(True)
        sleep(1)  # Hold the lock for a bit
        lock.release()
    else:
        results.append(False)


def test_B2Lock_concurrency():
    """
    Suggestion 1: True Concurrency Verification.
    Verifies that multiple threads can acquire the lock sequentially.
    """
    concurrency_key = uuid.uuid4().hex + '.concurrent'
    results = []
    
    # Clean up any existing locks
    initial_lock = b2_session.b2lock(concurrency_key)
    if initial_lock.other_locks():
        initial_lock.break_other_locks()

    num_workers = 3
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
        # Submit multiple workers at once
        futures = [executor.submit(worker_b2_lock, concurrency_key, results) for _ in range(num_workers)]
        concurrent.futures.wait(futures)

    # All workers should have successfully acquired the lock sequentially
    print(f"B2 Concurrency Results: {results}")
    assert sum(results) == num_workers
