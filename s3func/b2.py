#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on May 13 08:04:38 2024

@author: mike
"""
import io
from typing import List, Union
# import requests
import urllib.parse
# from requests import Session
# from requests.adapters import HTTPAdapter
import hashlib
import urllib3
import orjson
import copy
import uuid
from time import sleep
from timeit import default_timer
import datetime
from threading import current_thread
# import b2sdk.v2 as b2
# from b2sdk._internal.session import B2Session

from . import http_url
# import http_url

from . import utils
# import utils

#######################################################
### Parameters

# key_patterns = {
#     'b2': '{base_url}/{bucket}/{key}',
#     'contabo': '{base_url}:{bucket}/{key}',
#     }

# multipart_size = 2**28

auth_url = 'https://api.backblazeb2.com/b2api/v3/b2_authorize_account'

get_upload_url_str = '/b2api/v3/b2_get_upload_url'
download_file_by_id_str = '/b2api/v3/b2_download_file_by_id'

available_capabilities = ( "listKeys", "writeKeys", "deleteKeys", "listAllBucketNames", "listBuckets", "readBuckets", "writeBuckets", "deleteBuckets", "readBucketRetentions", "writeBucketRetentions", "readBucketEncryption", "writeBucketEncryption", "writeBucketNotifications", "listFiles", "readFiles", "shareFiles", "writeFiles", "deleteFiles", "readBucketNotifications", "readFileLegalHolds", "writeFileLegalHolds", "readFileRetentions", "writeFileRetentions", "bypassGovernance" )

md5_locks = {
    'shared': 'cfcd208495d565ef66e7dff9f98764da',
    'exclusive': 'c4ca4238a0b923820dcc509a6f75849b'
    }

# info = b2.InMemoryAccountInfo()

# b2_api = b2.B2Api(info)

# session = B2Session(info)

# sqlite_info = b2.SqliteAccountInfo()

#######################################################
### Functions


# def client(connection_config: utils.B2ConnectionConfig, max_pool_connections: int = 10, download_url: HttpUrl=None):
#     """
#     Creates a B2Api class instance associated with a B2 account.

#     Parameters
#     ----------
#     connection_config : dict
#         A dictionary of the connection info necessary to establish an B2 connection. It should contain application_key_id and application_key which are equivelant to the B2 aws_access_key_id and aws_secret_access_key.
#     max_pool_connections : int
#         The number of simultaneous connections for the B2 connection.

#     Returns
#     -------
#     botocore.client.BaseClient
#     """
#     ## Validate config
#     _ = utils.B2ConnectionConfig(**connection_config)

#     info = b2.InMemoryAccountInfo()
#     b2_api = b2.B2Api(info,
#                       cache=b2.InMemoryCache(),
#                       max_upload_workers=max_pool_connections,
#                       max_copy_workers=max_pool_connections,
#                       max_download_workers=max_pool_connections,
#                       save_to_buffer_size=524288)

#     config = copy.deepcopy(connection_config)

#     b2_api.authorize_account("production", config['application_key_id'], config['application_key'])

#     if download_url is not None:
#         info._download_url = download_url

#     return b2_api


def get_authorization(application_key_id, application_key, session):
    """

    """
    headers = urllib3.make_headers(basic_auth=f'{application_key_id}:{application_key}')

    response = session.request('get', auth_url, headers=headers)
    resp = utils.HttpResponse(response, False)

    return resp


#######################################################
### Other classes


class B2Lock:
    """

    """
    def __init__(self, access_key_id: str, access_key: str, bucket: str, key: str, lock_id: str=None, **b2_session_kwargs):
        """
        This class contains a locking mechanism by utilizing B2 objects. It has implementations for both shared and exclusive (the default) locks. It follows the same locking API as python thread locks (https://docs.python.org/3/library/threading.html#lock-objects), but with some extra methods for managing "deadlocks". The required B2 permissions are ListObjects, WriteObjects, and DeleteObjects.

        This initialized class can be used as a context manager exactly like the thread locks. It can also be pickled, which means it can be used in multiprocessing.

        Parameters
        ----------
        access_key_id : str
            The access key id also known as application_key_id.
        access_key : str
            The access key also known as application_key.
        bucket : str
            The bucket to be used when performing B2 operations.
        key : str
            The base object key that will be given a lock. The extension ".lock" plus a unique object id will be appended to the key, so the user is welcome to reference an existing object without worry that it will be overwritten.
        lock_id : str or None
            The unique ID used for the lock object. None will create a new ID. Retaining the lock_id will allow the user to use the lock later.
        b2_session_kwargs :
            Other kwargs passed to B2Session.
        """
        self._b2_session_kwargs = dict(access_key_id=access_key_id, access_key=access_key, bucket=bucket)
        self._b2_session_kwargs.update(b2_session_kwargs)

        session = B2Session(**self._b2_session_kwargs)

        obj_lock_key = key + '.lock.'
        objs = self._list_objects(session, obj_lock_key)

        version_ids = {0: '', 1: ''}
        timestamp = None

        if lock_id is None:
            self.lock_id = uuid.uuid4().hex[:13]
        else:
            self.lock_id = lock_id
            if objs:
                for obj in objs:
                    key = obj['key']
                    if lock_id in key:
                        seq = int(key[-1])
                        version_ids[seq] = obj['version_id']
                        if seq == 1:
                            timestamp = obj['last_modified']

        self._version_ids = version_ids
        self._obj_lock_key_len = len(obj_lock_key)

        self._timestamp = timestamp

        self._obj_lock_key = obj_lock_key
        self._key = key


    @staticmethod
    def _list_objects(session, obj_lock_key, lock_id=None):
        """

        """
        if lock_id is not None:
            key = obj_lock_key + lock_id
        else:
            key = obj_lock_key
        objs = session.list_object_versions(prefix=key)
        if objs.status in (401, 403):
            raise urllib3.exceptions.HTTPError(str(objs.error)[1:-1])

        res = []
        for l in objs.iter_objects():
            if l['content_md5'] == md5_locks['exclusive']:
                l['lock_type'] = 'exclusive'
            elif l['content_md5'] == md5_locks['shared']:
                l['lock_type'] = 'shared'
            else:
                raise ValueError('This lock file was created by something else...')
            res.append(l)

        return res


    @staticmethod
    def _check_older_timestamp(timestamp_other, timestamp, lock_id, lock_id_other):
        """

        """
        if timestamp_other == timestamp:
            if lock_id_other < lock_id:
                return True
        if timestamp_other < timestamp:
            return True

        return False


    def _check_for_older_objs(self, objs, all_locks=False):
        """

        """
        res = {}
        for lock_id_other, obj in objs.items():
            if not all_locks:
                if obj['lock_type'] == 'shared':
                    continue
            if 1 not in obj:
                if self._check_older_timestamp(obj[0], self._timestamp, self.lock_id, lock_id_other):
                    res[lock_id_other] = obj
            elif self._check_older_timestamp(obj[1], self._timestamp, self.lock_id, lock_id_other):
                res[lock_id_other] = obj

        return res


    def _delete_lock_object(self, session, seq):
        """

        """
        obj_name = self._obj_lock_key + f'{self.lock_id}-{seq}'
        _ = session.delete_object(obj_name, self._version_ids[seq])
        self._version_ids[seq] = ''
        self._timestamp = None


    def _delete_lock_objects(self, session):
        """

        """
        for seq in (0, 1):
            self._delete_lock_object(session, seq)


    def _put_lock_objects(self, session, body):
        """

        """
        for seq in (0, 1):
            obj_name = self._obj_lock_key + f'{self.lock_id}-{seq}'
            timestamp = datetime.datetime.now(datetime.timezone.utc)
            resp = session.put_object(obj_name, body, last_modified=timestamp)
            if ('version_id' in resp.metadata) and (resp.status == 200):
                self._version_ids[seq] = resp.metadata['version_id']
                self._timestamp = timestamp
            else:
                if seq == 1:
                    self._delete_lock_objects(session)
                else:
                    self._delete_lock_object(session, seq)
                raise urllib3.exceptions.HTTPError(str(resp.error)[1:-1])


    def _other_locks_timestamps(self, session):
        """
        Method to list all of the other locks' timestamps (and lock type).

        Returns
        -------
        list of dict
        """
        objs = self._list_objects(session, self._obj_lock_key)

        other_locks = {}

        if objs:
            for l in objs:
                lock_id, seq = l['key'][self._obj_lock_key_len:].split('-')
                if lock_id != self.lock_id:
                    if lock_id in other_locks:
                        other_locks[lock_id].update({int(seq): l['last_modified']})
                    else:
                        other_locks[lock_id] = {int(seq): l['last_modified'],
                                               'lock_type': l['lock_type'],
                                               }
        return other_locks


    def other_locks(self):
        """
        Method that finds all of the other locks and returns a summary dict by lock id.

        Returns
        -------
        dict
        """
        session = B2Session(**self._b2_session_kwargs)
        objs = self._list_objects(session, self._obj_lock_key)

        other_locks = {}

        if objs:
            for l in objs:
                lock_id, seq = l['key'][self._obj_lock_key_len:].split('-')
                if lock_id != self.lock_id:
                    other_locks[lock_id] = {'last_modified': l['last_modified'],
                                           'lock_type': l['lock_type'],
                                           'owner': l['owner'],
                                           }

        return other_locks


    def break_other_locks(self, timestamp: str | datetime.datetime=None):
        """
        Removes all other locks that are on the object older than specified timestamp. This is only meant to be used in deadlock circumstances.

        Parameters
        ----------
        timestamp : str or datetime.datetime
            All locks older than the timestamp will be removed. The default is now.

        Returns
        -------
        list of dict of the removed keys/versions
        """
        if timestamp is None:
           timestamp = datetime.datetime.now(datetime.timezone.utc)
        elif isinstance(timestamp, str):
            timestamp = datetime.datetime.fromisoformat(timestamp).astimezone(datetime.timezone.utc)
        else:
            raise TypeError('timestamp must be either an ISO datetime string or a datetime object.')

        session = B2Session(**self._b2_session_kwargs)
        objs = self._list_objects(session, self._obj_lock_key)

        keys = []
        if objs:
            for l in objs:
                if l['last_modified'] < timestamp:
                    _ = session.delete_object(l['key'], l['version_id'])
                    keys.append(l)

        self._version_ids = {0: '', 1: ''}
        self._timestamp = None

        return keys


    def locked(self):
        """
        Checks to see if there's a lock on the object. This will return True is there is a shared or exclusive lock.

        Returns
        -------
        bool
        """
        session = B2Session(**self._b2_session_kwargs)
        objs = self._list_objects(session, self._obj_lock_key)
        if objs:
            return True
        else:
            return False


    def aquire(self, blocking=True, timeout=-1, exclusive=True):
        """
        Acquire a lock, blocking or non-blocking.

        When invoked with the blocking argument set to True (the default), block until the lock is unlocked, then set it to locked and return True.

        When invoked with the blocking argument set to False, do not block. If a call with blocking set to True would block, return False immediately; otherwise, set the lock to locked and return True.

        When invoked with the timeout argument set to a positive value, block for at most the number of seconds specified by timeout and as long as the lock cannot be acquired. A timeout argument of -1 specifies an unbounded wait. It is forbidden to specify a timeout when blocking is False.

        When the exclusive argument is True (the default), an exclusive lock is made. If False, then a shared lock is made. These are equivalent to the exclusive and shared locks in the linux flock command.

        The return value is True if the lock is acquired successfully, False if not (for example if the timeout expired).

        Returns
        -------
        bool
        """
        if self._timestamp is None:
            session = B2Session(**self._b2_session_kwargs)
            if exclusive:
                body = b'1'
            else:
                body = b'0'
            self._put_lock_objects(session, body)
            objs = self._other_locks_timestamps(session)
            objs2 = self._check_for_older_objs(objs, exclusive)

            if objs2:
                start_time = default_timer()

                while blocking:
                    sleep(2)
                    objs = self._other_locks_timestamps(session)
                    objs2 = self._check_for_older_objs(objs, exclusive)
                    if len(objs2) == 0:
                        return True
                    else:
                        if timeout > 0:
                            duration = default_timer() - start_time
                            if duration > timeout:
                                break

                ## If the user makes it non-blocking or the timer runs out, the object version needs to be removed
                self._delete_lock_objects(session)

                return False
            else:
                return True
        else:
            return True


    def release(self):
        """
        Release the lock. It can only release the lock that was created via this instance. Returns nothing.
        """
        if self._timestamp is not None:
            session = B2Session(**self._b2_session_kwargs)
            self._delete_lock_objects(session)

    def __enter__(self):
        self.aquire()

    def __exit__(self, *args):
        self.release()

#######################################################
### Main class


class B2Session:
    """

    """
    def __init__(self, access_key_id: str=None, access_key: str=None, bucket: str=None, max_pool_connections: int = 10, max_attempts: int=3, read_timeout: int=120, download_url: str=None, stream=True):
        """
        Establishes an B2 client connection with a B2 account. If connection_config is None, then only get_object and head_object methods are available.

        Parameters
        ----------
        access_key_id : str or None
            The access key id also known as application_key_id.
        access_key : str or None
            The access key also known as application_key.
        bucket : str or None
            The bucket to be used when performing B2 operations. If None, then the application_key_id must be associated with only one bucket as this info can be obtained from the initial API request. If it's a str and the application_key_id is not specific to a signle bucket, then the listBuckets capability must be associated with the application_key_id.
        max_pool_connections : int
            The number of simultaneous connections for the B2 connection.
        max_attempts: int
            The number of retries if the connection fails.
        read_timeout: int
            The read timeout in seconds.
        download_url : str or None
            An alternative download_url when downloading data. If None, the download_url will be retrieved from the initial b2 request. It should NOT include the file/ at the end of the url.
        stream : bool
            Should the connection stay open for streaming or should all the data/content be loaded during the initial request.
        """
        b2_session = http_url.session(max_pool_connections, max_attempts, read_timeout)

        if isinstance(download_url, str):
            if not utils.is_url(download_url):
                raise TypeError(f'{download_url} is not a proper http url.')

        if isinstance(access_key_id, str) and isinstance(access_key, str):
            conn_config = utils.build_conn_config(access_key_id, access_key, 'b2')

            resp = get_authorization(conn_config['application_key_id'], conn_config['application_key'], b2_session)
            if resp.status // 100 != 2:
                raise urllib3.exceptions.HTTPError(f'{resp.error}')

            data = orjson.loads(resp.data)

            storage_api = data['apiInfo']['storageApi']
            if 'bucketId' in storage_api:
                bucket_id = storage_api['bucketId']
                bucket = storage_api['bucketName']
            elif isinstance(bucket, str):
                # TODO run the list_buckets request to determine the bucket_id associated with the bucket.
                pass
            else:
                raise ValueError('Bucket access error. See the docstrings for the bucket parameter.')

            api_url = storage_api['apiUrl']
            if download_url is None:
                download_url = storage_api['downloadUrl']
            auth_token = data['authorizationToken']

            self.bucket_id = bucket_id
            self.api_url = api_url
            self.auth_token = auth_token
            self.account_id = data['accountId']
            self._auth_data = data

        elif (bucket is None) or (download_url is None):
            raise ValueError('If access_key_id and access_key is None, then bucket and download_url must be assigned.')

        self._session = b2_session
        self.bucket = bucket
        self.download_url = download_url
        self._upload_url_data = {}
        self._stream = stream
        self._access_key_id = access_key_id
        self._access_key = access_key
        self._max_attempts = max_attempts
        self._read_timeout = read_timeout


    def create_app_key(self, capabilities: List[str], key_name: str, duration: int=None, bucket_id: str=None, prefix: str=None):
        """

        """
        if hasattr(self, 'auth_token'):
            headers = {'Authorization': self.auth_token}
        else:
            raise ValueError('connection_config must be initialised.')

        for cap in capabilities:
            if cap not in available_capabilities:
                raise ValueError(f'{cap} is not in {available_capabilities}.')

        fields = {
            'accountId': self.account_id,
            'capabilities': capabilities,
            'keyName': key_name}

        if isinstance(duration, int):
            fields['validDurationInSeconds'] = duration

        if isinstance(bucket_id, str):
            fields['bucketId'] = bucket_id

        if isinstance(prefix, str):
            fields['namePrefix'] = prefix

        url = urllib.parse.urljoin(self.api_url, '/b2api/v3/b2_create_key')

        resp = self._session.request('post', url, json=fields, headers=headers)
        b2resp = utils.B2Response(resp, self._stream)

        return b2resp


    def list_buckets(self):
        """

        """
        if hasattr(self, 'auth_token'):
            headers = {'Authorization': self.auth_token}
        else:
            raise ValueError('connection_config must be initialised.')

        url = urllib.parse.urljoin(self.api_url, '/b2api/v3/b2_list_buckets')
        url += f'?accountId={self.account_id}'

        resp = self._session.request('get', url, headers=headers)
        b2resp = utils.B2Response(resp, self._stream)

        return b2resp


    def get_object(self, key: str, version_id: str=None):
        """
        Method to get an object/file from a B2 bucket.

        Parameters
        ----------
        key : str
            The object/file name in the B2 bucket.
        version_id : str
            The B2 version/file id associated with the object.

        Returns
        -------
        B2Response
        """
        if hasattr(self, 'auth_token'):
            headers = {'Authorization': self.auth_token}
        else:
            headers = None

        ## Get the object
        if isinstance(version_id, str):
            url = urllib.parse.urljoin(self.download_url, download_file_by_id_str)
            url += f'?fileId={version_id}'
        else:
            if key.startswith('/'):
                key = key[1:]
            url = urllib.parse.urljoin(self.download_url, 'file/' + self.bucket + '/' + key)

        resp = self._session.request('get', url, headers=headers, preload_content=not self._stream)
        b2resp = utils.B2Response(resp, self._stream)

        return b2resp


    def head_object(self, key: str, version_id: str=None):
        """
        Method to get the headers/metadata of an object (without getting the data) from a B2 bucket.

        Parameters
        ----------
        key : str
            The object/file name in the B2 bucket.
        version_id : str
            The B2 version/file id associated with the object.

        Returns
        -------
        B2Response
        """
        if hasattr(self, 'auth_token'):
            headers = {'Authorization': self.auth_token}
        else:
            headers = None

        ## Get the object
        if isinstance(version_id, str):
            url = urllib.parse.urljoin(self.download_url, download_file_by_id_str)
            url += f'?fileId={version_id}'
        else:
            if key.startswith('/'):
                key = key[1:]
            url = urllib.parse.urljoin(self.download_url, 'file/' + self.bucket + '/' + key)

        resp = self._session.request('head', url, headers=headers, preload_content=not self._stream)
        b2resp = utils.B2Response(resp, self._stream)

        return b2resp


    def _get_upload_url(self):
        """

        """
        if hasattr(self, 'auth_token'):
            headers = {'Authorization': self.auth_token}
        else:
            raise ValueError('connection_config must be initialised.')

        url = urllib.parse.urljoin(self.api_url, get_upload_url_str)
        url += f'?bucketId={self.bucket_id}'

        resp = self._session.request('get', url, headers=headers, preload_content=False)
        data = orjson.loads(resp.data)
        if resp.status != 200:
            raise urllib3.exceptions.HTTPError(f'{data}')

        thread_name = current_thread().name

        self._upload_url_data[thread_name] = {'upload_url': data['uploadUrl'],
                                              'auth_token': data['authorizationToken']
                                              }

    def put_object(self, key: str, obj: Union[bytes, io.BufferedIOBase], metadata: dict={}, content_type: str=None, last_modified: datetime.datetime=None):
        """
        Method to upload data to a B2 bucket.

        Parameters
        ----------
        key : str
            The key name for the uploaded object.
        obj : bytes or io.BufferedIOBase
            The file object to be uploaded.
        metadata : dict or None
            A dict of the user metadata that should be saved along with the object.
        content_type : str
            The http content type to associate the object with.
        last_modified : datetime.datetime
            The last modified date associated with the object

        Returns
        -------
        B2Response
        """
        if not hasattr(self, 'auth_token'):
            raise ValueError('connection_config must be initialised.')

        key = urllib.parse.quote(key)

        ## Get upload url
        thread_name = current_thread().name
        if thread_name not in self._upload_url_data:
            self._get_upload_url()

        upload_url_data = self._upload_url_data[thread_name]
        upload_url = upload_url_data['upload_url']

        headers = {'Authorization': upload_url_data['auth_token'],
                   'X-Bz-File-Name': key}

        if isinstance(obj, bytes):
            headers['Content-Length'] = len(obj)
            headers['X-Bz-Content-Sha1'] = hashlib.sha1(obj).hexdigest()
        else:
            if obj.seekable():
                obj.seek(0, 2)
                headers['Content-Length'] = obj.tell()
                obj.seek(0, 0)

            # else:
            #     raise TypeError('obj must be seekable.')

            headers['X-Bz-Content-Sha1'] = 'do_not_verify'

        if isinstance(content_type, str):
            headers['Content-Type'] = content_type
        else:
            headers['Content-Type'] = 'b2/x-auto'

        ## User metadata - must be less than 2 kb
        user_meta = {}
        if isinstance(last_modified, datetime.datetime):
            user_meta['X-Bz-Info-src_last_modified_millis'] = str(int(last_modified.astimezone(datetime.timezone.utc).timestamp() * 1000))

        if metadata:
            for key, value in metadata.items():
                if isinstance(key, str) and isinstance(value, str):
                    user_meta['X-Bz-Info-' + key] = value
                else:
                    raise TypeError('metadata keys and values must be strings.')

        # Check for size and add to headers
        size = 0
        for key, val in user_meta.items():
            size += len(key.encode())
            size += len(val.encode())
            headers[key] = val

        if size > 2048:
            raise ValueError('metadata size is {size} bytes, but it must be under 2048 bytes.')

        # TODO : In python version 3.11, the file_digest function can input a file object

        counter = 0
        while True:
            resp = self._session.request('post', upload_url, body=obj, headers=headers)
            if resp.status == 200:
                break
            elif resp.status not in (401, 503):
                error = orjson.loads(resp.data)
                raise urllib3.exceptions.HTTPError(f'{error}')
            elif counter == 5:
                error = orjson.loads(resp.data)
                raise urllib3.exceptions.HTTPError(f'{error}')

            self._get_upload_url()
            upload_url_data = self._upload_url_data[thread_name]
            upload_url = upload_url_data['upload_url']
            headers['Authorization'] = upload_url_data['auth_token']
            upload_url = upload_url_data['upload_url']

        b2resp = utils.B2Response(resp, self._stream)
        b2resp.metadata.update(utils.get_metadata_from_b2_put_object(resp))

        return b2resp


    def list_objects(self, prefix: str=None, start_after: str=None, delimiter: str=None, max_keys: int=10000):
        """
        B2 method to list object/file names.

        Parameters
        ----------
        prefix: str
            Limits the response to keys that begin with the specified prefix.
        start_after : str
            The B2 key to start after.
        delimiter : str
            A delimiter is a character you use to group keys.
        max_keys : int
            Sets the maximum number of keys returned in the response. By default, the action returns up to 1,000 key names. The response might contain fewer keys but will never contain more.

        Returns
        -------
        B2ListResponse
        """
        if hasattr(self, 'auth_token'):
            headers = {'Authorization': self.auth_token}
        else:
            raise ValueError('connection_config must be initialised.')

        params = utils.build_b2_query_params(self.bucket_id, start_after=start_after, prefix=prefix, delimiter=delimiter, max_keys=max_keys)

        resp = utils.B2ListResponse('/b2api/v3/b2_list_file_names', self._session, self.api_url, headers, params)

        return resp


    def list_object_versions(self, prefix: str=None, start_after: str=None, delimiter: str=None, max_keys: int=None):
        """
        B2 method to list object/file versions.

        Parameters
        ----------
        prefix : str
            Limits the response to keys that begin with the specified prefix.
        start_after : str
            The B2 key to start after.
        delimiter : str or None
            A delimiter is a character you use to group keys.
        max_keys : int
            Sets the maximum number of keys returned in the response. By default, the action returns up to 1,000 key names. The response might contain fewer keys but will never contain more.

        Returns
        -------
        B2ListResponse
        """
        if hasattr(self, 'auth_token'):
            headers = {'Authorization': self.auth_token}
        else:
            raise ValueError('connection_config must be initialised.')

        params = utils.build_b2_query_params(self.bucket_id, start_after=start_after, prefix=prefix, delimiter=delimiter, max_keys=max_keys)

        resp = utils.B2ListResponse('/b2api/v3/b2_list_file_versions', self._session, self.api_url, headers, params)

        return resp


    def delete_object(self, key: str, version_id: str):
        """
        Delete a single object/version.

        Parameters
        ----------
        key : str
            The object key in the B2 bucket.
        version_id : str
            The B2 version id associated with the object.

        Returns
        -------
        B2Response
        """
        if hasattr(self, 'auth_token'):
            headers = {'Authorization': self.auth_token}
        else:
            raise ValueError('connection_config must be initialised.')

        params = utils.build_b2_query_params(key=key, version_id=version_id)

        url = urllib.parse.urljoin(self.api_url, '/b2api/v3/b2_delete_file_version')
        resp = self._session.request('post', url, headers=headers, json=params)
        b2resp = utils.B2Response(resp, self._stream)

        return b2resp


    # def delete_objects(self, keys: List[dict]):
    #     """
    #     keys must be a list of dictionaries. The dicts must have the keys named Key and VersionId derived from the list_object_versions function. This function will automatically separate the list into 1000 count list chunks (required by the delete_objects request).

    #     Returns
    #     -------
    #     None
    #     """
    #     for keys in utils.chunks(keys, 1000):
    #         keys2 = []
    #         for key in keys:
    #             if 'key' in key:
    #                 key['Key'] = key.pop('key')
    #             if 'Key' not in key:
    #                 raise ValueError('"key" must be passed in the list of dict.')
    #             if 'version_id' in key:
    #                 key['VersionId'] = key.pop('version_id')
    #             if 'VersionId' not in key:
    #                 raise ValueError('"version_id" must be passed in the list of dict.')
    #             keys2.append(key)

    #         _ = self._client.delete_objects(Bucket=self.bucket, Delete={'Objects': keys2, 'Quiet': True})



########################################################
### B2 Locks and holds


    # def get_object_legal_hold(self, key: str, version_id: str=None):
    #     """
    #     Method to get the staus of a legal hold of an object. The user must have b2:GetObjectLegalHold or b2:readFileLegalHolds permissions for this request.

    #     Parameters
    #     ----------
    #     key : str
    #         The key name for the uploaded object.
    #     version_id : str
    #         The B2 version id associated with the object.

    #     Returns
    #     -------
    #     B2Response
    #     """
    #     params = utils.build_b2_params(self.bucket, key=key, version_id=version_id)

    #     b2resp = utils.B2Response(self._client, 'get_object_legal_hold', **params)

    #     return b2resp


    # def put_object_legal_hold(self, key: str, lock: bool=False, version_id: str=None):
    #     """
    #     Method to put or remove a legal hold on an object. The user must have b2:PutObjectLegalHold or b2:writeFileLegalHolds permissions for this request.

    #     Parameters
    #     ----------
    #     key : str
    #         The key name for the uploaded object.
    #     lock : bool
    #         Should a lock be added to the object?
    #     version_id : str
    #         The B2 version id associated with the object.

    #     Returns
    #     -------
    #     None
    #     """
    #     if lock:
    #         hold = {'Status': 'ON'}
    #     else:
    #         hold = {'Status': 'OFF'}

    #     params = utils.build_b2_params(self.bucket, key=key, version_id=version_id)
    #     params['LegalHold'] = hold

    #     b2resp = utils.B2Response(self._client, 'put_object_legal_hold', **params)

    #     return b2resp


    # def get_object_lock_configuration(self):
    #     """
    #     Function to whther a bucket is configured to have object locks. The user must have b2:GetBucketObjectLockConfiguration or b2:readBucketRetentions permissions for this request.

    #     Returns
    #     -------
    #     B2Reponse
    #     """
    #     b2resp = utils.B2Response(self._client, 'get_object_lock_configuration', Bucket=self.bucket)

    #     return b2resp


    # def put_object_lock_configuration(self, lock: bool=False):
    #     """
    #     Function to enable or disable object locks for a bucket. The user must have b2:PutBucketObjectLockConfiguration or b2:writeBucketRetentions permissions for this request.

    #     Parameters
    #     ----------
    #     lock : bool
    #         Should a lock be enabled for the bucket?

    #     Returns
    #     -------
    #     boto3 response
    #     """
    #     if lock:
    #         hold = {'ObjectLockEnabled': 'Enable'}
    #     else:
    #         hold = {'ObjectLockEnabled': 'Disable'}

    #     # resp = b2.put_object_lock_configuration(Bucket=bucket, ObjectLockConfiguration=hold)
    #     b2resp = utils.B2Response(self._client, 'put_object_lock_configuration', Bucket=self.bucket, ObjectLockConfiguration=hold)

    #     return b2resp


    def b2lock(self, key: str):
        """
        This class contains a locking mechanism by utilizing B2 objects. It has implementations for both shared and exclusive (the default) locks. It follows the same locking API as python thread locks (https://docs.python.org/3/library/threading.html#lock-objects), but with some extra methods for managing "deadlocks". The required B2 permissions are ListObjects, WriteObjects, and DeleteObjects.

        This initialized class can be used as a context manager exactly like thread locks.

        Parameters
        ----------
        key : str
            The base object key that will be given a lock. The extension ".lock" plus a unique lock id will be appended to the key, so the user is welcome to reference an existing object without worry that it will be overwritten or deleted.
        """
        return B2Lock(self._access_key_id, self._access_key, self.bucket, key, max_attempts=self._max_attempts, read_timeout=self._read_timeout)










































































