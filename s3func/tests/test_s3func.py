import pytest
import os, pathlib
import uuid
import io
import sys
from time import sleep
from timeit import default_timer
import datetime
try:
    import tomllib as toml
except ImportError:
    import tomli as toml
from s3func import s3, b2, http_url, utils


#################################################
### Parameters

script_path = pathlib.Path(os.path.realpath(os.path.dirname(__file__)))
package_path = str(script_path.parent)

# if package_path not in sys.path:
#     sys.path.insert(0, package_path)
# import s3, http_url # For running without a package

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
# obj_key = 'manual_test_key'
base_url = 'https://b2.tethys-ts.xyz/file/' + bucket + '/'
url = base_url +  obj_key

s3_client = s3.client(conn_config)


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
        for js in s3.list_object_versions(s3_client, bucket):
            if js['Key'] == obj_key:
                obj_keys.append({'Key': js['Key'], 'VersionId': js['VersionId']})

        if obj_keys:
            s3.delete_objects(s3_client, bucket, obj_keys)

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


def test_s3_put_object():
    """

    """
    ### Upload with bytes
    with io.open(script_path.joinpath(file_name), 'rb') as f:
        obj = f.read()

    resp1 = s3.put_object(s3_client, bucket, obj_key, obj)

    meta = resp1.metadata
    if meta['status'] != 200:
        raise ValueError('Upload failed')

    resp1_etag = meta['etag']

    ## Upload with a file-obj
    resp2 = s3.put_object(s3_client, bucket, obj_key, io.open(script_path.joinpath(file_name), 'rb'))

    meta = resp2.metadata
    if meta['status'] != 200:
        raise ValueError('Upload failed')

    resp2_etag = meta['etag']

    assert resp1_etag == resp2_etag


def test_s3_list_objects():
    """

    """
    count = 0
    found_key = False
    resp = s3.list_objects(s3_client, bucket)
    for i, js in enumerate(resp.metadata['contents']):
        count += 1
        if js['key'] == obj_key:
            found_key = True

    assert found_key


def test_s3_list_object_versions():
    """

    """
    count = 0
    found_key = False
    resp = s3.list_object_versions(s3_client, bucket)
    for i, js in enumerate(resp.metadata['versions']):
        count += 1
        if js['key'] == obj_key:
            found_key = True

    assert found_key


def test_s3_get_object():
    """

    """
    stream1 = s3.get_object(obj_key, bucket, s3_client)
    data1 = stream1.stream.read()

    stream2 = s3.get_object(obj_key, bucket, connection_config=conn_config)
    data2 = stream2.stream.read()

    assert data1 == data2


def test_http_url_get_object():
    """

    """
    stream1 = http_url.get_object(url)
    data1 = stream1.stream.read()

    new_url = http_url.join_url_obj_key(obj_key, base_url)

    stream2 = http_url.get_object(new_url)
    data2 = stream2.stream.read()

    assert data1 == data2


def test_s3_head_object():
    """

    """
    response = s3.head_object(obj_key, bucket, s3_client)

    assert 'version_id' in response.metadata


def test_http_url_head_object():
    """

    """
    response = http_url.head_object(url)

    assert 'version_id' in response.metadata


def test_legal_hold():
    """

    """
    hold = s3.get_object_legal_hold(s3_client, bucket, obj_key)
    if hold.status != 404:
        raise ValueError("There's a hold, but there shouldn't be.")

    put_hold = s3.put_object_legal_hold(s3_client, bucket, obj_key, True)
    if put_hold.status != 200:
        raise ValueError("Creating a hold failed.")

    hold = s3.get_object_legal_hold(s3_client, bucket, obj_key)
    if not hold.metadata['legal_hold']:
        raise ValueError("There isn't a hold, but there should be.")

    put_hold = s3.put_object_legal_hold(s3_client, bucket, obj_key, False)
    if put_hold.status != 200:
        raise ValueError("Removing a hold failed.")

    hold = s3.get_object_legal_hold(s3_client, bucket, obj_key)
    if hold.metadata['legal_hold']:
        raise ValueError("There's a hold, but there shouldn't be.")

    _ = s3.put_object(s3_client, bucket, obj_key, open(script_path.joinpath(file_name), 'rb'), object_legal_hold=True)

    hold = s3.get_object_legal_hold(s3_client, bucket, obj_key)
    if not hold.metadata['legal_hold']:
        raise ValueError("There isn't a hold, but there should be.")

    put_hold = s3.put_object_legal_hold(s3_client, bucket, obj_key, False)
    if put_hold.status != 200:
        raise ValueError("Removing a hold failed.")

    hold = s3.get_object_legal_hold(s3_client, bucket, obj_key)
    if hold.metadata['legal_hold']:
        raise ValueError("There's a hold, but there shouldn't be.")

    assert True


def test_delete_objects():
    """

    """
    obj_keys = []
    resp = s3.list_object_versions(s3_client, bucket)
    for js in resp.metadata['versions']:
        if js['key'] == obj_key:
            obj_keys.append({'key': js['key'], 'version_id': js['version_id']})

    s3.delete_objects(s3_client, bucket, obj_keys)

    found_key = False
    resp = s3.list_object_versions(s3_client, bucket)
    for i, js in enumerate(resp.metadata['versions']):
        if js['key'] == obj_key:
            found_key = True

    assert not found_key


def test_S3Lock():
    """

    """
    s3lock = s3.S3Lock(s3_client, bucket, obj_key)

    other_locks = s3lock.other_locks()

    assert isinstance(other_locks, dict)

    if other_locks:
        _ = s3lock.break_other_locks()

    assert not s3lock.locked()

    assert s3lock.aquire()

    assert s3lock.locked()

    s3lock.release()

    assert not s3lock.locked()

    with s3lock:
        assert s3lock.locked()

    assert not s3lock.locked()


def s3lock_loop():
    """

    """
    s3lock = s3.S3Lock(s3_client, bucket, obj_key)

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


# def resp_delay_test(n):
#     """

#     """
#     s3lock = s3.S3Lock(s3_client, bucket, obj_key)

#     to_mod_time = []
#     to_header_time = []
#     to_return_time = []

#     for i in range(n):
#         print(i)
#         start_time = datetime.datetime.now(datetime.timezone.utc)
#         resp = s3.put_object(s3lock._s3_client, s3lock._bucket, s3lock._obj_lock_key, b'1')
#         return_time = datetime.datetime.now(datetime.timezone.utc)
#         header_time = datetime.datetime.strptime(resp.headers['ResponseMetadata']['HTTPHeaders']['date'], '%a, %d %b %Y %H:%M:%S %Z').replace(tzinfo=datetime.timezone.utc)
#         mod_time = resp.metadata['last_modified']
#         _ = s3.delete_object(s3lock._s3_client, s3lock._bucket, s3lock._obj_lock_key, resp.metadata['version_id'])

#         to_mod_time.append((mod_time - start_time).total_seconds())
#         to_header_time.append((header_time - start_time).total_seconds())
#         to_return_time.append((return_time - start_time).total_seconds())

#     # mod_time_mean = to_mod_time/n
#     # header_time_mean = to_header_time/n
#     # return_time_mean = to_return_time/n

#     return to_mod_time, to_header_time, to_return_time



# def _save_test_results(to_mod_time, to_header_time, to_return_time):
#     """

#     """
#     df1 = pd.DataFrame(zip(to_mod_time, to_header_time, to_return_time), columns=['to_mod_time', 'to_header_time', 'to_return_time'])
#     df1.to_csv('put_object_timings.csv')


















































