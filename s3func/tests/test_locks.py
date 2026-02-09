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


################################################
### Helper functions


def worker_lock(session, key, results):
    """
    Worker function for concurrency test.
    """
    lock = session.lock(key)
    # Attempt to acquire lock with blocking and timeout
    acquired = lock.acquire(blocking=True, timeout=60)
    if acquired:
        results.append(True)
        sleep(1)  # Hold the lock for a bit
        lock.release()
    else:
        results.append(False)


################################################
### Tests


def test_B2Lock():
    """

    """
    b2_session = b2.B2Session(access_key_id, access_key)

    lock = b2_session.lock(obj_key)

    other_locks = lock.other_locks()

    assert isinstance(other_locks, dict)

    if other_locks:
        _ = lock.break_other_locks()

    assert not lock.locked()

    assert lock.acquire()

    # print(lock._list_objects(b2_session, lock._obj_lock_key))
    # print(list(b2_session.list_objects().iter_objects()))

    # res = []
    # for objs in b2_session.list_objects().iter_objects():
    #     key = objs['key']
    #     if 'lock' in key:
    #         res.append(objs)

    # print(res)

    assert lock.locked()

    lock.release()

    assert not lock.locked()

    with lock:
        assert lock.locked()

    assert not lock.locked()


def test_B2Lock_concurrency():
    """
    Verifies that multiple threads can acquire the lock sequentially.
    """
    concurrency_key = uuid.uuid4().hex + '.concurrent'
    results = []
    
    # Clean up any existing locks
    b2_session = b2.B2Session(access_key_id, access_key)

    initial_lock = b2_session.lock(concurrency_key)
    if initial_lock.other_locks():
        initial_lock.break_other_locks()

    num_workers = 4
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
        # Submit multiple workers at once
        futures = [executor.submit(worker_lock, b2_session, concurrency_key, results) for _ in range(num_workers)]
        concurrent.futures.wait(futures)

    # All workers should have successfully acquired the lock sequentially
    print(f"B2 Concurrency Results: {results}")
    assert sum(results) == num_workers



def test_S3Lock():
    """

    """
    s3_session = s3.S3Session(access_key_id, access_key, bucket, endpoint_url=endpoint_url)

    lock = s3_session.lock(obj_key)

    other_locks = lock.other_locks()

    assert isinstance(other_locks, dict)

    if other_locks:
        _ = lock.break_other_locks()

    assert not lock.locked()

    assert lock.acquire()

    assert lock.locked()

    lock.release()

    assert not lock.locked()

    with lock:
        assert lock.locked()

    assert not lock.locked()


def test_S3Lock_concurrency():
    """
    Verifies that multiple threads can acquire the lock sequentially.
    """
    concurrency_key = uuid.uuid4().hex + '.concurrent'
    results = []
    
    # Clean up any existing locks
    s3_session = s3.S3Session(access_key_id, access_key, bucket, endpoint_url=endpoint_url)

    initial_lock = s3_session.lock(concurrency_key)
    if initial_lock.other_locks():
        initial_lock.break_other_locks()

    num_workers = 4
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
        # Submit multiple workers at once
        futures = [executor.submit(worker_lock, s3_session, concurrency_key, results) for _ in range(num_workers)]
        concurrent.futures.wait(futures)

    # All workers should have successfully acquired the lock sequentially
    print(f"S3 Concurrency Results: {results}")
    assert sum(results) == num_workers


# def s3lock_loop():
#     """

#     """
#     s3_session = s3.S3Session(access_key_id, access_key, bucket, endpoint_url=endpoint_url)
#     lock = s3_session.lock(obj_key)

#     for i in range(100):
#         print(i)
#         with lock:
#             mod_date = lock._timestamp
#             others = lock.other_locks()
#             if others:
#                 for other_one, obj in others.items():
#                     if 1 in obj:
#                         if obj[1] < mod_date:
#                             print(('Other mod date was earlier.'))
#                             # raise ValueError('Other mod date was earlier.')



