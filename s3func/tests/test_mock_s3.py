import pytest
import io
import uuid
import boto3
import concurrent.futures
from time import sleep
from moto.server import ThreadedMotoServer
from s3func import s3


@pytest.fixture(scope="module")
def s3_server():
    # Use a random port to avoid conflicts if possible, but 5000 is usually fine for tests
    server = ThreadedMotoServer(port=5000)
    server.start()
    yield "http://localhost:5000"
    server.stop()


@pytest.fixture
def s3_session(s3_server):
    access_key_id = "testing"
    access_key = "testing"
    bucket_name = "test-bucket"
    endpoint_url = s3_server

    # Create the bucket in the mock server using boto3
    s3_client = boto3.client(
        "s3", endpoint_url=endpoint_url, aws_access_key_id=access_key_id, aws_secret_access_key=access_key
    )
    s3_client.create_bucket(Bucket=bucket_name)
    # Enable versioning
    s3_client.put_bucket_versioning(Bucket=bucket_name, VersioningConfiguration={'Status': 'Enabled'})

    session = s3.S3Session(access_key_id, access_key, bucket_name, endpoint_url=endpoint_url)
    return session


def test_mock_s3_put_get_object(s3_session):
    key = "test-key"
    data = b"hello world"

    # Put object
    resp = s3_session.put_object(key, data)
    assert resp.metadata['status'] == 200

    # Get object
    resp = s3_session.get_object(key)
    assert resp.metadata['status'] == 200
    assert resp.stream.read() == data


def test_mock_s3_list_objects(s3_session):
    key = f"list-test-key-{uuid.uuid4().hex}"
    data = b"list data"
    s3_session.put_object(key, data)

    found = False
    for obj in s3_session.list_objects().iter_objects():
        if obj['key'] == key:
            found = True
            break
    assert found


def test_mock_s3_list_object_versions(s3_session):
    key = f"version-test-key-{uuid.uuid4().hex}"
    s3_session.put_object(key, b"v1")
    s3_session.put_object(key, b"v2")

    versions = []
    for obj in s3_session.list_object_versions(prefix=key).iter_objects():
        if obj['key'] == key:
            versions.append(obj['version_id'])

    assert len(versions) >= 2


def test_mock_s3_delete_objects(s3_session):
    key = f"delete-test-key-{uuid.uuid4().hex}"
    s3_session.put_object(key, b"delete me")

    # Check it exists
    found = False
    for obj in s3_session.list_objects().iter_objects():
        if obj['key'] == key:
            found = True
            break
    assert found

    # Delete
    s3_session.delete_objects([{'key': key}])

    # Check it's gone (v2 listing might still show delete markers if we use list_object_versions, but list_objects should not show it)
    found = False
    for obj in s3_session.list_objects().iter_objects():
        if obj['key'] == key:
            found = True
            break
    assert not found


def test_mock_s3_copy_object(s3_session):
    src_key = "src-key"
    dest_key = "dest-key"
    data = b"copy data"
    s3_session.put_object(src_key, data)

    s3_session.copy_object(src_key, dest_key)

    resp = s3_session.get_object(dest_key)
    assert resp.stream.read() == data


def test_mock_s3_head_object(s3_session):
    key = "head-key"
    s3_session.put_object(key, b"head data")

    resp = s3_session.head_object(key)
    assert resp.metadata['status'] == 200
    assert 'version_id' in resp.metadata


def test_mock_s3_lock(s3_session):
    lock_key = f"lock-{uuid.uuid4().hex}"
    lock = s3_session.lock(lock_key)

    assert not lock.locked()
    assert lock.acquire()
    assert lock.locked()
    lock.release()
    assert not lock.locked()

    with lock:
        assert lock.locked()
    assert not lock.locked()


def worker_lock(session, key, results):
    lock = session.lock(key)
    acquired = lock.acquire(blocking=True, timeout=10)
    if acquired:
        results.append(True)
        sleep(0.1)
        lock.release()
    else:
        results.append(False)


def test_mock_s3_lock_concurrency(s3_session):
    concurrency_key = f"concurrency-{uuid.uuid4().hex}"
    results = []
    num_workers = 3

    with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
        futures = [executor.submit(worker_lock, s3_session, concurrency_key, results) for _ in range(num_workers)]
        concurrent.futures.wait(futures)

    assert sum(results) == num_workers
