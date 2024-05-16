import pytest
import os, pathlib
import uuid
import io
try:
    import tomllib as toml
except ImportError:
    import tomli as toml
from s3func import *

#################################################
### Parameters

script_path = pathlib.Path(os.path.realpath(os.path.dirname(__file__)))

try:
    with open(script_path.joinpath('s3_config.toml'), "rb") as f:
        conn_config = toml.load(f)['connection_config']
except:
    conn_config = {
        'service_name': 's3',
        'endpoint_url': os.environ['endpoint_url'],
        'aws_access_key_id': os.environ['aws_access_key_id'],
        'aws_secret_access_key': os.environ['aws_secret_access_key'],
        }


bucket = 'achelous'
flag = "w"
buffer_size = 524288
read_timeout = 60
threads = 10
object_lock = False
file_name = 'stns_data.blt'
obj_key = uuid.uuid4().hex
base_url = 'https://b2.tethys-ts.xyz/file/' + bucket + '/'
url = base_url +  obj_key

s3 = s3_client(conn_config)


################################################
### Pytest stuff


@pytest.hookimpl(tryfirst=True, hookwrapper=True)
def pytest_runtest_makereport(item, call):
    # execute all other hooks to obtain the report object
    outcome = yield
    rep = outcome.get_result()

    # set a report attribute for each phase of a call, which can
    # be "setup", "call", "teardown"

    setattr(item, "rep_" + rep.when, rep)


@pytest.fixture
def get_logs(request):
    yield

    if request.node.rep_call.failed:
        # Add code here to cleanup failure scenario
        print("executing test failed")

        obj_keys = []
        for js in list_object_versions(s3, bucket):
            if js['Key'] == obj_key:
                obj_keys.append({'Key': js['Key'], 'VersionId': js['VersionId']})

        if obj_keys:
            delete_objects(s3, bucket, obj_keys)

    # elif request.node.rep_call.passed:
    #     # Add code here to cleanup success scenario
    #     print("executing test success")


################################################
### Tests


# @pytest.mark.parametrize(
#     "a,b,result",
#     [
#         (0, 0, 0),
#         (1, 1, 2),
#         (3, 2, 5),
#     ],
# )
# def test_add(a: int, b: int, result: int):
#     assert add(a, b) == result


def test_put_object():
    """

    """
    ### Upload with bytes
    with io.open(script_path.joinpath(file_name), 'rb') as f:
        obj = f.read()

    resp1 = put_object(s3, bucket, obj_key, obj)

    meta = resp1.metadata
    if meta['status'] != 200:
        raise ValueError('Upload failed')

    resp1_etag = meta['etag']

    ## Upload with a file-obj
    resp2 = put_object(s3, bucket, obj_key, io.open(script_path.joinpath(file_name), 'rb'))

    meta = resp2.metadata
    if meta['status'] != 200:
        raise ValueError('Upload failed')

    resp2_etag = meta['etag']

    assert resp1_etag == resp2_etag


def test_list_objects():
    """

    """
    count = 0
    found_key = False
    for i, js in enumerate(list_objects(s3, bucket)):
        count += 1
        if js['Key'] == obj_key:
            found_key = True

    assert found_key


def test_list_object_versions():
    """

    """
    count = 0
    found_key = False
    for i, js in enumerate(list_object_versions(s3, bucket)):
        count += 1
        if js['Key'] == obj_key:
            found_key = True

    assert found_key


def test_get_object():
    """

    """
    stream1 = get_object(obj_key, bucket, s3)
    data1 = stream1.read()

    stream2 = get_object(obj_key, bucket, connection_config=conn_config)
    data2 = stream2.read()

    assert data1 == data2


def test_url_to_stream():
    """

    """
    stream1 = url_to_stream(url)
    data1 = stream1.read()

    stream2 = base_url_to_stream(obj_key, base_url)
    data2 = stream2.read()

    assert data1 == data2


def test_head_object():
    """

    """
    response = head_object(obj_key, bucket, s3)

    assert 'version_id' in response.metadata


def test_url_to_headers():
    """

    """
    response = url_to_headers(url)

    assert 'version_id' in response.metadata


def test_legal_hold():
    """

    """
    hold = get_object_legal_hold(s3, bucket, obj_key)
    if hold:
        raise ValueError("There's a hold, but there shouldn't be.")

    put_object_legal_hold(s3, bucket, obj_key, True)

    hold = get_object_legal_hold(s3, bucket, obj_key)
    if not hold:
        raise ValueError("There isn't a hold, but there should be.")

    put_object_legal_hold(s3, bucket, obj_key, False)

    hold = get_object_legal_hold(s3, bucket, obj_key)
    if hold:
        raise ValueError("There's a hold, but there shouldn't be.")

    resp2 = put_object(s3, bucket, obj_key, open(script_path.joinpath(file_name), 'rb'), object_legal_hold=True)

    hold = get_object_legal_hold(s3, bucket, obj_key)
    if not hold:
        raise ValueError("There isn't a hold, but there should be.")

    put_object_legal_hold(s3, bucket, obj_key, False)

    hold = get_object_legal_hold(s3, bucket, obj_key)
    if hold:
        raise ValueError("There's a hold, but there shouldn't be.")

    assert True


def test_delete_objects():
    """

    """
    obj_keys = []
    for js in list_object_versions(s3, bucket):
        if js['Key'] == obj_key:
            obj_keys.append({'Key': js['Key'], 'VersionId': js['VersionId']})

    delete_objects(s3, bucket, obj_keys)

    found_key = False
    for i, js in enumerate(list_object_versions(s3, bucket)):
        if js['Key'] == obj_key:
            found_key = True

    assert not found_key









































































