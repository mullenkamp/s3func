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

# import b2sdk.v2 as b2
# from b2sdk._internal.session import B2Session

#################################################
### Parameters

script_path = pathlib.Path(os.path.realpath(os.path.dirname(__file__)))
package_path = str(script_path.parent)

# if package_path not in sys.path:
#     sys.path.insert(0, package_path)
# import s3, http_url, b2 # For running without a package

try:
    with open(script_path.joinpath('s3_config.toml'), "rb") as f:
        conn_config = toml.load(f)['connection_config']
    endpoint_url = conn_config['endpoint_url']
    access_key_id = conn_config['aws_access_key_id']
    access_key = conn_config['aws_secret_access_key']
except:
    endpoint_url = os.environ['endpoint_url']
    access_key_id = os.environ['aws_access_key_id']
    access_key = os.environ['aws_secret_access_key']

bucket = 'achelous'
bucket_id = 'e063bcbc0d6523df74ed0e1d'
flag = "w"
buffer_size = 524288
read_timeout = 60
threads = 10
object_lock = False
file_name = 'stns_data.blt'
obj_key = uuid.uuid4().hex
# obj_key = 'gwrc_flow_sensor_sites.gpkg'
# obj_key = 'manual_test_key'
base_url = 'https://b2.tethys-ts.xyz/file/' + bucket + '/'
url = base_url +  obj_key

# s3_client = s3.client(conn_config)
s3_session = s3.S3Session(access_key_id, access_key, bucket, endpoint_url=endpoint_url)
http_session = http_url.HttpSession()
b2_session = b2.B2Session(access_key_id, access_key)

## B2
# auth_file_path = script_path.joinpath('auth_file.sqlite')


# info = b2.InMemoryAccountInfo()

# b2_api = b2.B2Api(info, cache=b2.InMemoryCache())
# # Auth here
# bucket = b2_api.get_bucket_by_id('e063bcbc0d6523df74ed0e1d')


# session = B2Session(info)

# sqlite_info = b2.SqliteAccountInfo(auth_file_path)

# b2_api = b2.B2Api(sqlite_info)

# session = B2Session(sqlite_info)

# conn = sqlite_info._get_connection()
# cur = conn.cursor()
# res = cur.execute("SELECT download_url FROM account")
# res.fetchone()
# res = cur.execute("UPDATE account SET download_url='https://b2.tethys-ts.xyz'")
# conn.commit()





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
        for js in s3_session.list_object_versions():
            if js['Key'] == obj_key:
                obj_keys.append({'Key': js['Key'], 'VersionId': js['VersionId']})

        if obj_keys:
            s3_session.delete_objects(obj_keys)

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

    resp1 = s3_session.put_object(obj_key, obj)

    meta = resp1.metadata
    if meta['status'] != 200:
        raise ValueError('Upload failed')

    resp1_etag = meta['etag']

    ## Upload with a file-obj
    resp2 = s3_session.put_object(obj_key, io.open(script_path.joinpath(file_name), 'rb'))

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
    resp = s3_session.list_objects()
    for i, js in enumerate(resp.iter_objects()):
        count += 1
        if js['key'] == obj_key:
            found_key = True

    assert found_key


def test_s3_list_object_versions():
    """

    """
    count = 0
    found_key = False
    resp = s3_session.list_object_versions()
    for i, js in enumerate(resp.iter_objects()):
        count += 1
        if js['key'] == obj_key:
            found_key = True

    assert found_key


def test_s3_get_object():
    """

    """
    stream1 = s3_session.get_object(obj_key)
    data1 = stream1.stream.read()

    # stream2 = s3_session.get_object(obj_key)
    # data2 = stream2.stream.read()

    assert len(data1) > 10000


def test_http_url_get_object():
    """

    """
    stream1 = http_session.get_object(url)
    data1 = stream1.stream.read()

    new_url = http_url.join_url_key(obj_key, base_url)

    stream2 = http_session.get_object(new_url)
    data2 = stream2.stream.read()

    assert data1 == data2


def test_s3_head_object():
    """

    """
    response = s3_session.head_object(obj_key)

    assert 'version_id' in response.metadata


def test_http_url_head_object():
    """

    """
    response = http_session.head_object(url)

    assert 'version_id' in response.metadata


def test_s3_legal_hold():
    """

    """
    hold = s3_session.get_object_legal_hold(obj_key)
    if hold.status != 404:
        raise ValueError("There's a hold, but there shouldn't be.")

    put_hold = s3_session.put_object_legal_hold(obj_key, True)
    if put_hold.status != 200:
        raise ValueError("Creating a hold failed.")

    hold = s3_session.get_object_legal_hold(obj_key)
    if not hold.metadata['legal_hold']:
        raise ValueError("There isn't a hold, but there should be.")

    put_hold = s3_session.put_object_legal_hold(obj_key, False)
    if put_hold.status != 200:
        raise ValueError("Removing a hold failed.")

    hold = s3_session.get_object_legal_hold(obj_key)
    if hold.metadata['legal_hold']:
        raise ValueError("There's a hold, but there shouldn't be.")

    _ = s3_session.put_object(obj_key, open(script_path.joinpath(file_name), 'rb'), object_legal_hold=True)

    hold = s3_session.get_object_legal_hold(obj_key)
    if not hold.metadata['legal_hold']:
        raise ValueError("There isn't a hold, but there should be.")

    put_hold = s3_session.put_object_legal_hold(obj_key, False)
    if put_hold.status != 200:
        raise ValueError("Removing a hold failed.")

    hold = s3_session.get_object_legal_hold(obj_key)
    if hold.metadata['legal_hold']:
        raise ValueError("There's a hold, but there shouldn't be.")

    assert True


def test_s3_delete_objects():
    """

    """
    obj_keys = []
    resp = s3_session.list_object_versions()
    for js in resp.iter_objects():
        if js['key'] == obj_key:
            obj_keys.append({'key': js['key'], 'version_id': js['version_id']})

    s3_session.delete_objects(obj_keys)

    found_key = False
    resp = s3_session.list_object_versions()
    for i, js in enumerate(resp.iter_objects()):
        if js['key'] == obj_key:
            found_key = True

    assert not found_key


def test_S3Lock():
    """

    """
    s3lock = s3_session.s3lock(obj_key)

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


####################################################
### B2


def test_b2_put_object():
    """

    """
    ### Upload with bytes
    with io.open(script_path.joinpath(file_name), 'rb') as f:
        obj = f.read()

    resp1 = b2_session.put_object(obj_key, obj)

    meta = resp1.metadata
    if meta['status'] != 200:
        raise ValueError('Upload failed')

    resp1_sha1 = meta['content_sha1']

    ## Upload with a file-obj
    resp2 = b2_session.put_object(obj_key, io.open(script_path.joinpath(file_name), 'rb'))

    meta = resp2.metadata
    if meta['status'] != 200:
        raise ValueError('Upload failed')

    resp2_sha1 = meta['content_sha1']

    assert resp1_sha1 == resp2_sha1


def test_b2_list_objects():
    """

    """
    count = 0
    found_key = False
    resp = b2_session.list_objects()
    for i, js in enumerate(resp.iter_objects()):
        count += 1
        if js['key'] == obj_key:
            found_key = True

    assert found_key


def test_b2_list_object_versions():
    """

    """
    count = 0
    found_key = False
    resp = b2_session.list_object_versions()
    for i, js in enumerate(resp.iter_objects()):
        count += 1
        if js['key'] == obj_key:
            found_key = True

    assert found_key


def test_b2_get_object():
    """

    """
    stream1 = b2_session.get_object(obj_key)
    data1 = stream1.stream.read()

    # stream2 = b2_session.get_object(obj_key)
    # data2 = stream2.stream.read()

    assert len(data1) > 10000


def test_b2_head_object():
    """

    """
    response = b2_session.head_object(obj_key)

    assert 'version_id' in response.metadata


def test_b2_delete_objects():
    """

    """
    # obj_keys = []
    resp = b2_session.list_object_versions()
    for js in resp.iter_objects():
        if js['key'] == obj_key:
            # obj_keys.append({'key': js['key'], 'version_id': js['version_id']})

            b2_session.delete_object(js['key'], js['version_id'])

    found_key = False
    resp = b2_session.list_object_versions()
    for i, js in enumerate(resp.iter_objects()):
        if js['key'] == obj_key:
            found_key = True

    assert not found_key


def test_B2Lock():
    """

    """
    s3lock = b2_session.b2lock(obj_key)

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


















































