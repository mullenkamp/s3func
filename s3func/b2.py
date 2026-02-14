#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on May 13 08:04:38 2024

@author: mike
"""
import io
from typing import List, Union
import urllib.parse
import hashlib
import urllib3
import orjson
import copy
import uuid
from time import sleep
from timeit import default_timer
import datetime
import weakref
from threading import current_thread
import concurrent.futures

# import b2sdk.v2 as b2
# from b2sdk._internal.session import B2Session

from . import http_url, utils, response, locking

# import http_url, utils, response, locking

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

available_capabilities = (
    "listKeys",
    "writeKeys",
    "deleteKeys",
    "listAllBucketNames",
    "listBuckets",
    "readBuckets",
    "writeBuckets",
    "deleteBuckets",
    "readBucketRetentions",
    "writeBucketRetentions",
    "readBucketEncryption",
    "writeBucketEncryption",
    "writeBucketNotifications",
    "listFiles",
    "readFiles",
    "shareFiles",
    "writeFiles",
    "deleteFiles",
    "readBucketNotifications",
    "readFileLegalHolds",
    "writeFileLegalHolds",
    "readFileRetentions",
    "writeFileRetentions",
    "bypassGovernance",
)

md5_locks = {'shared': 'cfcd208495d565ef66e7dff9f98764da', 'exclusive': 'c4ca4238a0b923820dcc509a6f75849b'}

# info = b2.InMemoryAccountInfo()

# b2_api = b2.B2Api(info)

# session = B2Session(info)

# sqlite_info = b2.SqliteAccountInfo()

#######################################################
### Functions


_b2_auth_cache = {}


def get_authorization(application_key_id, application_key, session):
    """
    Get authorization with simple global caching.
    """
    cache_key = (application_key_id, application_key)
    if cache_key in _b2_auth_cache:
        # Check if it's "old"? 1 hour should be very safe
        data, ts = _b2_auth_cache[cache_key]
        if (default_timer() - ts) < 3600:

            class FakeResponse:
                def __init__(self, data):
                    self.status = 200
                    self.data = data
                    self.error = None

            return FakeResponse(orjson.dumps(data))

    headers = urllib3.make_headers(basic_auth=f'{application_key_id}:{application_key}')
    resp = session.request('get', auth_url, headers=headers)
    resp = response.HttpResponse(resp, False)

    if resp.status == 200:
        _b2_auth_cache[cache_key] = (orjson.loads(resp.data), default_timer())

    return resp


def release_b2_lock(obj_lock_key, lock_id, version_ids, b2_session_kwargs):
    """
    Made for the creation of finalize objects to ensure that the lock is released if something goes wrong.
    """
    session = B2Session(**b2_session_kwargs)
    for seq, version_id in version_ids.items():
        if version_id:
            obj_name = obj_lock_key + f'{lock_id}-{seq}'
            try:
                _ = session.delete_object(obj_name, version_id)
            except:
                pass


#######################################################
### Other classes


# class B2Lock(locking.DistributedLock):
#     """
#     B2 implementation of DistributedLock.
#     """
#     def __init__(self, access_key_id: str, access_key: str, bucket: str, key: str, lock_id: str=None, **b2_session_kwargs):
#         super().__init__(key, lock_id)
#         self._b2_session_kwargs = dict(access_key_id=access_key_id, access_key=access_key, bucket=bucket)
#         self._b2_session_kwargs.update(b2_session_kwargs)

#         self._obj_lock_key = key + '.lock.'
#         self._obj_lock_key_len = len(self._obj_lock_key)

#         # If lock_id was provided, check if it already exists to recover state
#         if lock_id:
#             session = B2Session(**self._b2_session_kwargs)
#             objs = self._list_objects(session, self._obj_lock_key, lock_id)
#             for obj in objs:
#                 obj_key_name = obj['key']
#                 if lock_id in obj_key_name:
#                     seq = int(obj_key_name[-1])
#                     self._version_ids[seq] = obj['version_id']
#                     if seq == 1:
#                         self._timestamp = obj.get('last_modified', obj['upload_timestamp'])
#             if self._timestamp:
#                 self._finalizer = weakref.finalize(self, release_b2_lock, self._obj_lock_key, self.lock_id, self._version_ids, self._b2_session_kwargs)

#     @staticmethod
#     def _list_objects(session, obj_lock_key, lock_id=None):
#         key = obj_lock_key + (lock_id if lock_id else "")
#         objs = session.list_objects(prefix=key)
#         if objs.status in (401, 403):
#             raise urllib3.exceptions.HTTPError(str(objs.error))

#         res = []
#         for l in objs.iter_objects():
#             if l['content_md5'] == md5_locks['exclusive']:
#                 l['lock_type'] = 'exclusive'
#             elif l['content_md5'] == md5_locks['shared']:
#                 l['lock_type'] = 'shared'
#             else:
#                 continue
#             res.append(l)
#         return res

#     def _do_put_lock_objects(self, exclusive):
#         session = B2Session(**self._b2_session_kwargs)
#         body = b'1' if exclusive else b'0'

#         # Consistent local timestamp for B2 last_modified metadata
#         local_ts = datetime.datetime.now(datetime.timezone.utc)

#         for seq in (0, 1):
#             obj_name = self._obj_lock_key + f'{self.lock_id}-{seq}'
#             resp = session.put_object(obj_name, body, last_modified=local_ts)
#             if resp.status == 200:
#                 self._version_ids[seq] = resp.metadata.get('version_id')
#                 # Use server-side upload_timestamp for ordering to avoid clock skew issues
#                 if seq == 0:
#                     self._timestamp = resp.metadata['upload_timestamp']
#             else:
#                 release_b2_lock(self._obj_lock_key, self.lock_id, self._version_ids, self._b2_session_kwargs)
#                 raise urllib3.exceptions.HTTPError(f"Failed to put lock object: {resp.error}")

#         self._finalizer = weakref.finalize(self, release_b2_lock, self._obj_lock_key, self.lock_id, self._version_ids, self._b2_session_kwargs)

#     def _do_list_other_locks(self):
#         session = B2Session(**self._b2_session_kwargs)
#         objs = self._list_objects(session, self._obj_lock_key)
#         other_locks = {}
#         for l in objs:
#             parts = l['key'][self._obj_lock_key_len:].split('-')
#             if len(parts) != 2: continue
#             lock_id, seq = parts
#             if lock_id != self.lock_id:
#                 timestamp = l.get('last_modified', l['upload_timestamp'])
#                 if lock_id not in other_locks:
#                     other_locks[lock_id] = {'lock_type': l['lock_type']}
#                 other_locks[lock_id][int(seq)] = timestamp
#         return other_locks

#     def other_locks(self):
#         session = B2Session(**self._b2_session_kwargs)
#         objs = self._list_objects(session, self._obj_lock_key)
#         other_locks = {}
#         for l in objs:
#             parts = l['key'][self._obj_lock_key_len:].split('-')
#             if len(parts) != 2: continue
#             lock_id, seq = parts
#             if lock_id != self.lock_id:
#                 timestamp = l.get('last_modified', l['upload_timestamp'])
#                 other_locks[lock_id] = {
#                     'last_modified': timestamp,
#                     'lock_type': l['lock_type'],
#                     'owner': l.get('owner'),
#                 }
#         return other_locks

#     def break_other_locks(self, timestamp: str | datetime.datetime=None):
#         if timestamp is None:
#            timestamp = datetime.datetime.now(datetime.timezone.utc)
#         elif isinstance(timestamp, str):
#             timestamp = datetime.datetime.fromisoformat(timestamp).astimezone(datetime.timezone.utc)

#         session = B2Session(**self._b2_session_kwargs)
#         objs = self._list_objects(session, self._obj_lock_key)
#         keys = []
#         for l in objs:
#             timestamp_obj = l.get('last_modified', l['upload_timestamp'])
#             if timestamp_obj <= timestamp:
#                 _ = session.delete_object(l['key'], l['version_id'])
#                 keys.append(l)
#         return keys

#     def locked(self):
#         session = B2Session(**self._b2_session_kwargs)
#         return len(self._list_objects(session, self._obj_lock_key)) > 0


#######################################################
### Main class


class B2Session:
    """ """

    def __init__(
        self,
        access_key_id: str = None,
        access_key: str = None,
        bucket: str = None,
        max_connections: int = 10,
        max_attempts: int = 3,
        read_timeout: int = 120,
        download_url: str = None,
        stream=True,
    ):
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
        max_connections : int
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
        b2_session = http_url.session(max_connections, max_attempts, read_timeout)

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
            bucket_id = None
            if 'bucketId' in storage_api:
                bucket_id = storage_api['bucketId']
                bucket = storage_api['bucketName']
            elif isinstance(bucket, str):
                # Resolve bucket name to bucket ID
                self._session = b2_session
                self.api_url = storage_api['apiUrl']
                self.auth_token = data['authorizationToken']
                self.account_id = data['accountId']

                buckets_resp = self.list_buckets()
                if buckets_resp.status == 200:
                    buckets_data = orjson.loads(buckets_resp.data)
                    for b in buckets_data.get('buckets', []):
                        if b['bucketName'] == bucket:
                            bucket_id = b['bucketId']
                            break
                if bucket_id is None:
                    raise ValueError(f"Could not find bucket {bucket} or resolve its ID.")
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

    def create_app_key(
        self, capabilities: List[str], key_name: str, duration: int = None, bucket_id: str = None, prefix: str = None
    ):
        """ """
        if hasattr(self, 'auth_token'):
            headers = {'Authorization': self.auth_token}
        else:
            raise ValueError('connection_config must be initialised.')

        for cap in capabilities:
            if cap not in available_capabilities:
                raise ValueError(f'{cap} is not in {available_capabilities}.')

        fields = {'accountId': self.account_id, 'capabilities': capabilities, 'keyName': key_name}

        if isinstance(duration, int):
            fields['validDurationInSeconds'] = duration

        if isinstance(bucket_id, str):
            fields['bucketId'] = bucket_id

        if isinstance(prefix, str):
            fields['namePrefix'] = prefix

        url = urllib.parse.urljoin(self.api_url, '/b2api/v3/b2_create_key')

        resp = self._session.request('post', url, json=fields, headers=headers)
        b2resp = response.B2Response(resp, self._stream)

        return b2resp

    def list_buckets(self):
        """ """
        if hasattr(self, 'auth_token'):
            headers = {'Authorization': self.auth_token}
        else:
            raise ValueError('connection_config must be initialised.')

        url = urllib.parse.urljoin(self.api_url, '/b2api/v3/b2_list_buckets')
        url += f'?accountId={self.account_id}'

        resp = self._session.request('get', url, headers=headers)
        b2resp = response.B2Response(resp, self._stream)

        return b2resp

    def get_object(self, key: str, version_id: str = None):
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
        b2resp = response.B2Response(resp, self._stream)

        return b2resp

    def head_object(self, key: str, version_id: str = None):
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
        b2resp = response.B2Response(resp, self._stream)

        return b2resp

    def _get_upload_url(self):
        """ """
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

        self._upload_url_data[thread_name] = {'upload_url': data['uploadUrl'], 'auth_token': data['authorizationToken']}

    def put_object(
        self,
        key: str,
        obj: Union[bytes, io.BufferedIOBase],
        metadata: dict = {},
        content_type: str = None,
        last_modified: datetime.datetime = None,
    ):
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

        headers = {'Authorization': upload_url_data['auth_token'], 'X-Bz-File-Name': key}

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
            user_meta['X-Bz-Info-src_last_modified_millis'] = str(
                int(last_modified.astimezone(datetime.timezone.utc).timestamp() * 1000)
            )

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
            headers['Authorization'] = upload_url_data['auth_token']
            upload_url = upload_url_data['upload_url']
            counter += 1

        b2resp = response.B2Response(resp, self._stream)
        b2resp.metadata.update(utils.get_metadata_from_b2_put_object(resp))

        return b2resp

    def list_objects(self, prefix: str = None, start_after: str = None, delimiter: str = None, max_keys: int = 10000):
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

        params = utils.build_b2_query_params(
            self.bucket_id, start_after=start_after, prefix=prefix, delimiter=delimiter, max_keys=max_keys
        )

        resp = response.B2ListResponse('/b2api/v3/b2_list_file_names', self._session, self.api_url, headers, params)

        return resp

    def list_object_versions(
        self, prefix: str = None, start_after: str = None, delimiter: str = None, max_keys: int = None
    ):
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

        params = utils.build_b2_query_params(
            self.bucket_id, start_after=start_after, prefix=prefix, delimiter=delimiter, max_keys=max_keys
        )

        resp = response.B2ListResponse('/b2api/v3/b2_list_file_versions', self._session, self.api_url, headers, params)

        return resp

    def delete_object(self, key: str, version_id: str = None):
        """
        Delete a single object/version. If version_id is None, it will delete all versions.

        Parameters
        ----------
        key : str
            The object key in the B2 bucket.
        version_id : str or None
            The B2 version id associated with the object.

        Returns
        -------
        B2Response
        """
        if hasattr(self, 'auth_token'):
            headers = {'Authorization': self.auth_token}
        else:
            raise ValueError('connection_config must be initialised.')

        if version_id is None:
            resp = self.list_object_versions(key)
            for resp_meta in resp.iter_objects():
                key = resp_meta['key']
                version_id = resp_meta['key']

                params = utils.build_b2_query_params(key=key, version_id=version_id)

                url = urllib.parse.urljoin(self.api_url, '/b2api/v3/b2_delete_file_version')
                resp = self._session.request('post', url, headers=headers, json=params)

                if resp.status != 200:
                    error = orjson.loads(resp.data)
                    raise urllib3.exceptions.HTTPError(f'{error}')

                b2resp = response.B2Response(resp, self._stream)

        else:
            params = utils.build_b2_query_params(key=key, version_id=version_id)

            url = urllib.parse.urljoin(self.api_url, '/b2api/v3/b2_delete_file_version')
            resp = self._session.request('post', url, headers=headers, json=params)

            if resp.status != 200:
                error = orjson.loads(resp.data)
                raise urllib3.exceptions.HTTPError(f'{error}')

            b2resp = response.B2Response(resp, self._stream)

        return b2resp

    def delete_objects(self, keys: List[dict]):
        """
        keys must be a list of dictionaries. The dicts must have the keys named key and version_id derived from the list_object_versions method. The B2 API has no bulk deletion call, so the deletes will be performed serially.

        Returns
        -------
        None
        """
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            futures = []
            for key_dict in keys:
                future = executor.submit(self.delete_object, key_dict['key'], key_dict['version_id'])
                futures.append(future)
            concurrent.futures.wait(futures)

    def copy_object(
        self,
        dest_key: str,
        source_version_id: str,
        dest_bucket_id: str | None = None,
        metadata: dict = {},
        content_type: str = None,
    ):
        """
        Copy an object within B2. The source and destination must use the same credentials.

        Parameters
        ----------
        dest_key : str
            The destination key
        source_version_id : str
            The specific version id of the source object. Required for B2 instead of the source_key.
        source_bucket : str or None
            The source bucket. If None, then it uses the initialised bucket.
        dest_bucket_id: str or None
            The destimation bucket id. If None, then it uses the initialised bucket.
        metadata : dist
            The metadata for the destination object. If no metadata is provided, then the metadata is copied from the source.

        Returns
        -------
        B2Response
        """
        headers = {'fileName': urllib.parse.quote(dest_key), 'sourceFileId': source_version_id}
        if isinstance(dest_bucket_id, str):
            headers['destinationBucketId'] = dest_bucket_id

        ## Get upload url
        thread_name = current_thread().name
        if thread_name not in self._upload_url_data:
            self._get_upload_url()

        upload_url_data = self._upload_url_data[thread_name]
        # upload_url = upload_url_data['upload_url']

        headers['Authorization'] = upload_url_data['auth_token']

        if isinstance(content_type, str):
            headers['Content-Type'] = content_type
        else:
            headers['Content-Type'] = 'b2/x-auto'

        ## User metadata - must be less than 2 kb
        user_meta = {}
        if metadata:
            headers['MetadataDirective'] = 'REPLACE'
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

        url = urllib.parse.urljoin(self.api_url, '/b2api/v4/b2_copy_file')

        counter = 0
        while True:
            resp = self._session.request('post', url, headers=headers)
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
            headers['Authorization'] = upload_url_data['auth_token']
            counter += 1

        b2resp = response.B2Response(resp, self._stream)
        b2resp.metadata.update(utils.get_metadata_from_b2_put_object(resp))

        return b2resp

    def lock(self, key: str, lock_id: str = None):
        """
        This class contains a locking mechanism by utilizing B2 objects. It has implementations for both shared and exclusive (the default) locks. It follows the same locking API as python thread locks (https://docs.python.org/3/library/threading.html#lock-objects), but with some extra methods for managing "deadlocks". The required B2 permissions are ListObjects, WriteObjects, and DeleteObjects.

        This initialized class can be used as a context manager exactly like thread locks.

        Parameters
        ----------
        key : str
            The base object key that will be given a lock. The extension ".lock" plus a unique lock id will be appended to the key, so the user is welcome to reference an existing object without worry that it will be overwritten or deleted.
        lock_id: str or None
            Reuse an existing lock ID. Defaults to none which will create a new ID.

        Returns
        -------
        DistributedLock
        """
        return locking.B2Lock(
            self._access_key_id,
            self._access_key,
            self.bucket,
            key,
            lock_id,
            download_url=self.download_url,
            max_attempts=self._max_attempts,
            read_timeout=self._read_timeout,
        )
