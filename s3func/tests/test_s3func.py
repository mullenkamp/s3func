import pytest
import os, pathlib
import uuid
import io
import sys
from time import sleep
from timeit import default_timer
from threading import current_thread
import concurrent.futures
import datetime

try:
    import tomllib as toml
except ImportError:
    import tomli as toml
from s3func import s3, http_url, b2

#################################################
### Parameters

script_path = pathlib.Path(os.path.realpath(os.path.dirname(__file__)))

file_name = 'stns_data.blt'

# Legacy credentials for B2-native and HTTP tests
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
legacy_base_url = 'https://b2.tethys-ts.xyz/file/' + legacy_bucket + '/'


################################################
### S3-compatible provider tests (parameterized)


def test_s3_put_get_object(s3_session, s3_test_key):
    """Upload bytes, then download and verify."""
    with io.open(script_path.joinpath(file_name), 'rb') as f:
        obj = f.read()

    resp = s3_session.put_object(s3_test_key, obj)
    assert resp.metadata['status'] == 200

    resp2 = s3_session.get_object(s3_test_key)
    data = resp2.data if resp2.data else resp2.stream.read()

    assert len(data) == len(obj)


def test_s3_put_file_object(s3_session, s3_test_key):
    """Upload with file-like object, compare etag with bytes upload."""
    with io.open(script_path.joinpath(file_name), 'rb') as f:
        obj = f.read()

    resp1 = s3_session.put_object(s3_test_key, obj)
    assert resp1.metadata['status'] == 200
    etag1 = resp1.metadata['etag']

    resp2 = s3_session.put_object(s3_test_key, io.open(script_path.joinpath(file_name), 'rb'))
    assert resp2.metadata['status'] == 200
    etag2 = resp2.metadata['etag']

    assert etag1 == etag2


def test_s3_list_objects(s3_session, s3_test_key):
    """List objects and verify our test key is present."""
    found_key = False
    for js in s3_session.list_objects().iter_objects():
        if js['key'] == s3_test_key:
            found_key = True
            break

    assert found_key


def test_s3_list_object_versions(s3_session, s3_test_key):
    """List object versions and verify our test key is present (skip if unsupported)."""
    import urllib3

    found_key = False
    try:
        for js in s3_session.list_object_versions().iter_objects():
            if js['key'] == s3_test_key:
                found_key = True
                break
    except urllib3.exceptions.HTTPError as e:
        if '501' in str(e):
            pytest.skip('Provider does not support list_object_versions')
        raise

    assert found_key


def test_s3_head_object(s3_session, s3_test_key):
    """Head request returns metadata."""
    response = s3_session.head_object(s3_test_key)
    assert response.metadata['status'] // 100 == 2


def test_s3_copy_object(s3_session, s3_test_key):
    """Copy object and verify etags match."""
    copy_key = s3_test_key + '.copy'

    resp1 = s3_session.copy_object(s3_test_key, copy_key)
    assert resp1.metadata['status'] == 200

    # Clean up copy
    s3_session.delete_object(copy_key)


def test_s3_delete_objects(s3_session, s3_test_key):
    """Delete test objects (with purge) and verify removal."""
    s3_session.delete_objects([s3_test_key])

    found_key = False
    for js in s3_session.list_objects().iter_objects():
        if js['key'] == s3_test_key:
            found_key = True
            break

    assert not found_key


################################################
### HTTP tests (B2 public URL, not parameterized)


def test_http_url_get_object():
    """GET via public HTTP URL."""
    if not legacy_access_key_id:
        pytest.skip('Legacy B2 credentials not available')

    # Need an object on B2 to test against — use a fresh key
    obj_key = uuid.uuid4().hex
    b2_s3 = s3.S3Session(legacy_access_key_id, legacy_access_key, legacy_bucket, endpoint_url=legacy_endpoint_url)

    with io.open(script_path.joinpath(file_name), 'rb') as f:
        obj = f.read()
    b2_s3.put_object(obj_key, obj)

    try:
        url = legacy_base_url + obj_key
        session = http_url.HttpSession()

        stream1 = session.get_object(url)
        data1 = stream1.stream.read()

        new_url = http_url.join_url_key(obj_key, legacy_base_url)
        stream2 = session.get_object(new_url)
        data2 = stream2.stream.read()

        assert data1 == data2
    finally:
        # Cleanup
        obj_keys = []
        for js in b2_s3.list_object_versions().iter_objects():
            if js['key'] == obj_key:
                obj_keys.append({'key': js['key'], 'version_id': js['version_id']})
        if obj_keys:
            b2_s3.delete_objects(obj_keys)


def test_http_url_head_object():
    """HEAD via public HTTP URL."""
    if not legacy_access_key_id:
        pytest.skip('Legacy B2 credentials not available')

    obj_key = uuid.uuid4().hex
    b2_s3 = s3.S3Session(legacy_access_key_id, legacy_access_key, legacy_bucket, endpoint_url=legacy_endpoint_url)

    with io.open(script_path.joinpath(file_name), 'rb') as f:
        obj = f.read()
    b2_s3.put_object(obj_key, obj)

    try:
        url = legacy_base_url + obj_key
        session = http_url.HttpSession()
        response = session.head_object(url)
        assert response.metadata['status'] // 100 == 2
    finally:
        obj_keys = []
        for js in b2_s3.list_object_versions().iter_objects():
            if js['key'] == obj_key:
                obj_keys.append({'key': js['key'], 'version_id': js['version_id']})
        if obj_keys:
            b2_s3.delete_objects(obj_keys)


################################################
### B2-native API tests (not parameterized)


def test_b2_put_object():
    """Upload via B2 native API."""
    if not legacy_access_key_id:
        pytest.skip('Legacy B2 credentials not available')

    obj_key = uuid.uuid4().hex
    b2_session = b2.B2Session(legacy_access_key_id, legacy_access_key)

    with io.open(script_path.joinpath(file_name), 'rb') as f:
        obj = f.read()

    resp1 = b2_session.put_object(obj_key, obj)
    assert resp1.metadata['status'] == 200
    resp1_sha1 = resp1.metadata['content_sha1']

    resp2 = b2_session.put_object(obj_key, io.open(script_path.joinpath(file_name), 'rb'))
    assert resp2.metadata['status'] == 200
    resp2_sha1 = resp2.metadata['content_sha1']

    assert resp1_sha1 == resp2_sha1

    # Cleanup
    for js in b2_session.list_object_versions().iter_objects():
        if js['key'] == obj_key:
            b2_session.delete_object(js['key'], js['version_id'])


def test_b2_list_objects():
    """List objects via B2 native API."""
    if not legacy_access_key_id:
        pytest.skip('Legacy B2 credentials not available')

    b2_session = b2.B2Session(legacy_access_key_id, legacy_access_key)

    count = 0
    for js in b2_session.list_objects().iter_objects():
        count += 1
        if count > 5:
            break

    assert count > 0


def test_b2_head_object():
    """Head request via B2 native API."""
    if not legacy_access_key_id:
        pytest.skip('Legacy B2 credentials not available')

    obj_key = uuid.uuid4().hex
    b2_session = b2.B2Session(legacy_access_key_id, legacy_access_key)

    with io.open(script_path.joinpath(file_name), 'rb') as f:
        obj = f.read()
    b2_session.put_object(obj_key, obj)

    try:
        response = b2_session.head_object(obj_key)
        assert 'version_id' in response.metadata
    finally:
        for js in b2_session.list_object_versions().iter_objects():
            if js['key'] == obj_key:
                b2_session.delete_object(js['key'], js['version_id'])
