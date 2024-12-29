#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on May 13 08:04:38 2024

@author: mike
"""
import io
# import os
# from pydantic import HttpUrl
from typing import List, Union
import boto3
import botocore
import copy
# import requests
# import urllib.parse
# from urllib3.util import Retry, Timeout
# import datetime
import hashlib
# from requests import Session
# from requests.adapters import HTTPAdapter
import urllib3
import uuid
from time import sleep
from timeit import default_timer
import datetime
import weakref

# from . import http_url
# import http_url

from . import utils
# import utils

#######################################################
### Parameters


md5_locks = {
    'shared': 'cfcd208495d565ef66e7dff9f98764da',
    'exclusive': 'c4ca4238a0b923820dcc509a6f75849b'
    }


#######################################################
### Functions


def client(access_key_id: str, access_key: str, endpoint_url: str=None, max_pool_connections: int = 10, max_attempts: int = 3, retry_mode: str='adaptive', timeout: int=120):
    """
    Creates a botocore.client.BaseClient associated with an S3 account. This can use the legacy connect (signature_version s3) and the current version.

    Parameters
    ----------
    connection_config : dict
        A dictionary of the connection info necessary to establish an S3 connection. It should contain service_name, endpoint_url, aws_access_key_id, and aws_secret_access_key. aws_access_key_id can also be access_key_id or application_key_id. aws_secret_access_key can also be secret_access_key or application_key.
    max_pool_connections : int
        The number of simultaneous connections for the S3 connection.
    max_attempts: int
        The number of max attempts passed to the "retries" option in the S3 config.
    retry_mode: str
        The retry mode passed to the "retries" option in the S3 config.
    timeout: int
        The timeout in seconds passed to the "retries" option in the S3 config.

    Returns
    -------
    botocore.client.BaseClient
    """
    ## Validate config
    conn_config = utils.build_conn_config(access_key_id, access_key, 's3', endpoint_url)

    # if 'config' in conn_config:
    #     config0 = conn_config.pop('config')
    #     config0.update({'max_pool_connections': max_pool_connections, 'retries': {'mode': retry_mode, 'max_attempts': max_attempts}, 'read_timeout': timeout})
    #     config1 = boto3.session.Config(**config0)

    #     s3_config1 = conn_config.copy()
    #     s3_config1.update({'config': config1})

    #     s3 = boto3.client(**s3_config1)
    # else:
    #     conn_config.update({'config': botocore.config.Config(max_pool_connections=max_pool_connections, retries={'mode': retry_mode, 'max_attempts': max_attempts}, read_timeout=timeout)})
    #     s3 = boto3.client(**conn_config)

    conn_config.update({'config': botocore.config.Config(max_pool_connections=max_pool_connections, retries={'mode': retry_mode, 'max_attempts': max_attempts}, read_timeout=timeout)})
    s3 = boto3.client(**conn_config)

    return s3


def release_s3_lock(obj_lock_key, lock_id, version_ids, s3_session_kwargs):
    """
    Made for the creation of finalize objects to ensure that the lock is released if something goes wrong.
    """
    del_dict = [{'Key': obj_lock_key + f'{lock_id}-{seq}', 'VersionId': version_id} for seq, version_id in version_ids.items() if version_id is not None]
    if del_dict:
        session = S3Session(**s3_session_kwargs)
        _ = session.delete_objects(del_dict)


#######################################################
### Other classes


# class S3UserMetadata:
#     """

#     """
#     def __init__(self,



class S3Lock:
    """

    """
    def __init__(self, access_key_id: str, access_key: str, bucket: str, key: str, endpoint_url: str=None, lock_id: str=None, **s3_session_kwargs):
        """
        This class contains a locking mechanism by utilizing S3 objects. It has implementations for both shared and exclusive (the default) locks. It follows the same locking API as python thread locks (https://docs.python.org/3/library/threading.html#lock-objects), but with some extra methods for managing "deadlocks". The required S3 permissions are ListObjects, WriteObjects, and DeleteObjects.

        This initialized class can be used as a context manager exactly like the thread locks. It can also be pickled, which means it can be used in multiprocessing.

        Parameters
        ----------
        access_key_id : str
            The access key id also known as aws_access_key_id.
        access_key : str
            The access key also known as aws_secret_access_key.
        bucket : str
            The bucket to be used when performing S3 operations.
        endpoint_url : str
            The nedpoint http(s) url for the s3 service.
        key : str
            The base object key that will be given a lock. The extension ".lock" plus a unique object id will be appended to the key, so the user is welcome to reference an existing object without worry that it will be overwritten.
        lock_id : str or None
            The unique ID used for the lock object. None will create a new ID. Retaining the lock_id will allow the user to use the lock later.
        s3_session_kwargs :
            Other kwargs passed to S3Session.
        """
        # self.bucket = bucket
        # self._access_key_id = access_key_id
        # self._access_key = access_key
        # self._endpoint_url = endpoint_url
        self._s3_session_kwargs = dict(access_key_id=access_key_id, access_key=access_key, bucket=bucket, endpoint_url=endpoint_url)
        self._s3_session_kwargs.update(s3_session_kwargs)

        session = S3Session(**self._s3_session_kwargs)

        obj_lock_key = key + '.lock.'
        objs = self._list_objects(session, obj_lock_key)

        version_ids = {0: None, 1: None}
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
                            timestamp = obj['upload_timestamp']

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
            if l['etag'] == md5_locks['exclusive']:
                l['lock_type'] = 'exclusive'
            elif l['etag'] == md5_locks['shared']:
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


    # def _delete_lock_object(self, session, seq):
    #     """

    #     """
    #     obj_name = self._obj_lock_key + f'{self.lock_id}-{seq}'
    #     _ = session.delete_object(obj_name, self._version_ids[seq])
    #     self._version_ids[seq] = None
    #     self._timestamp = None


    # def _delete_lock_objects(self, session):
    #     """

    #     """
    #     del_dict = [{'Key': self._obj_lock_key + f'{self.lock_id}-{seq}', 'VersionId': self._version_ids[seq]} for seq in (0, 1)]
    #     _ = session.delete_objects(del_dict)
    #     self._version_ids = {0: None, 1: None}
    #     self._timestamp = None


    def _put_lock_objects(self, session, body):
        """

        """
        for seq in (0, 1):
            obj_name = self._obj_lock_key + f'{self.lock_id}-{seq}'
            resp = session.put_object(obj_name, body)
            if ('version_id' in resp.metadata) and (resp.status == 200):
                self._version_ids[seq] = resp.metadata['version_id']
                self._timestamp = resp.metadata['upload_timestamp']
            else:
                # if seq == 1:
                #     self._delete_lock_objects(session)
                # else:
                #     self._delete_lock_object(session, seq)
                release_s3_lock(self._obj_lock_key, self.lock_id, self._version_ids, self._s3_session_kwargs)
                # self._version_ids = {0: None, 1: None}
                # self._timestamp = None
                raise urllib3.exceptions.HTTPError(str(resp.error)[1:-1])

        ## Create finalizer object
        self._finalizer = weakref.finalize(self, release_s3_lock, self._obj_lock_key, self.lock_id, self._version_ids, self._s3_session_kwargs)


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
                        other_locks[lock_id].update({int(seq): l['upload_timestamp']})
                    else:
                        other_locks[lock_id] = {int(seq): l['upload_timestamp'],
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
        session = S3Session(**self._s3_session_kwargs)
        objs = self._list_objects(session, self._obj_lock_key)

        other_locks = {}

        if objs:
            for l in objs:
                lock_id, seq = l['key'][self._obj_lock_key_len:].split('-')
                if lock_id != self.lock_id:
                    other_locks[lock_id] = {'upload_timestamp': l['upload_timestamp'],
                                           'lock_type': l['lock_type'],
                                           'owner': l['owner'],
                                           }
        return other_locks


    def break_other_locks(self, timestamp: str | datetime.datetime=None):
        """
        Removes all locks that are on the object older than specified timestamp. This is only meant to be used in deadlock circumstances.

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

        session = S3Session(**self._s3_session_kwargs)
        objs = self._list_objects(session, self._obj_lock_key)

        keys = []
        if objs:
            for l in objs:
                # lock_id, seq = l['key'][self._obj_lock_key_len:].split('-')
                # if lock_id != self.lock_id:
                if l['upload_timestamp'] < timestamp:
                    keys.append({'Key': l['key'], 'VersionId': l['version_id']})

            session.delete_objects(keys)

        self._version_ids = {0: None, 1: None}
        self._timestamp = None

        return keys


    def locked(self):
        """
        Checks to see if there's a lock on the object. This will return True if there is a shared or exclusive lock.

        Returns
        -------
        bool
        """
        session = S3Session(**self._s3_session_kwargs)
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
            session = S3Session(**self._s3_session_kwargs)
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
                self._finalizer()
                self._version_ids = {0: None, 1: None}
                self._timestamp = None

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
            self._finalizer()
            self._version_ids = {0: None, 1: None}
            self._timestamp = None

    def __enter__(self):
        self.aquire()

    def __exit__(self, *args):
        self.release()

#######################################################
### Main class


class S3Session:
    """

    """
    def __init__(self, access_key_id: str, access_key: str, bucket: str, endpoint_url: str=None, max_pool_connections: int = 10, max_attempts: int = 3, retry_mode: str='adaptive', read_timeout: int=120, stream=True):
        """
        Establishes an S3 client connection with an S3 account. This can use the legacy connect (signature_version s3) and the current version.

        Parameters
        ----------
        access_key_id : str
            The access key id also known as aws_access_key_id.
        access_key : str
            The access key also known as aws_secret_access_key.
        bucket : str
            The bucket to be used when performing S3 operations.
        endpoint_url : str
            The nedpoint http(s) url for the s3 service.
        max_pool_connections : int
            The number of simultaneous connections for the S3 connection.
        max_attempts: int
            The number of max attempts passed to the "retries" option in the S3 config.
        retry_mode: str
            The retry mode passed to the "retries" option in the S3 config.
        read_timeout: int
            The read timeout in seconds passed to the "retries" option in the S3 config.
        stream : bool
            Should the connection stay open for streaming or should all the data/content be loaded during the initial request.
        """
        s3 = client(access_key_id, access_key, endpoint_url, max_pool_connections, max_attempts, retry_mode, read_timeout)

        self.client = s3
        self.bucket = bucket
        self._stream = stream
        self._endpoint_url = endpoint_url
        self._access_key_id = access_key_id
        self._access_key = access_key
        self._max_attempts = max_attempts
        self._retry_mode = retry_mode
        self._read_timeout = read_timeout


    def get_object(self, key: str, version_id: str=None, range_start: int=None, range_end: int=None):
        """
        Method to get an object from an S3 bucket.

        Parameters
        ----------
        key : str
            The object key in the S3 bucket.
        version_id : str
            The S3 version id associated with the object.
        range_start: int
            The byte range start for the file.
        range_end: int
            The byte range end for the file.
        chunk_size: int
            The amount of bytes to download as once.

        Returns
        -------
        S3Response
        """
        ## Get the object
        params = utils.build_s3_params(self.bucket, key=key, version_id=version_id, range_start=range_start, range_end=range_end)

        s3resp = utils.S3Response(self.client, 'get_object', self._stream, **params)

        return s3resp


    def head_object(self, key: str, version_id: str=None):
        """
        Method to get the headers/metadata of an object from an S3 bucket.

        Parameters
        ----------
        key : str
            The object key in the S3 bucket.
        version_id : str
            The S3 version id associated with the object.

        Returns
        -------
        S3Response
        """
        params = utils.build_s3_params(self.bucket, key=key, version_id=version_id)

        s3resp = utils.S3Response(self.client, 'head_object', self._stream, **params)

        return s3resp


    def put_object(self, key: str, obj: Union[bytes, io.BufferedIOBase], metadata: dict={}, content_type: str=None, object_legal_hold: bool=False):
        """
        Method to upload data to an S3 bucket.

        Parameters
        ----------
        key : str
            The key name for the uploaded object.
        obj : bytes, io.BytesIO, or io.BufferedIOBase
            The file object to be uploaded.
        metadata : dict or None
            A dict of the user metadata that should be saved along with the object.
        content_type : str
            The http content type to associate the object with.
        object_legal_hold : bool
            Should the object be uploaded with a legal hold?

        Returns
        -------
        S3Response
        """
        # TODO : In python version 3.11, the file_digest function can input a file object

        if isinstance(obj, (bytes, bytearray)) and ('content-md5' not in metadata):
            metadata['content-md5'] = hashlib.md5(obj).hexdigest()

        # Check for metadata size
        size = 0
        for meta_key, meta_val in metadata.items():
            if isinstance(meta_key, str) and isinstance(meta_val, str):
                size += len(meta_key.encode())
                size += len(meta_val.encode())
            else:
                raise TypeError('metadata keys and values must be strings.')

        if size > 2048:
            raise ValueError('metadata size is {size} bytes, but it must be under 2048 bytes.')

        params = utils.build_s3_params(self.bucket, key=key, metadata=metadata, content_type=content_type, object_legal_hold=object_legal_hold)
        params['Body'] = obj

        s3resp = utils.S3Response(self.client, 'put_object', self._stream, **params)
        s3resp.metadata.update(metadata)

        return s3resp


    def list_objects(self, prefix: str=None, start_after: str=None, delimiter: str=None, max_keys: int=None):
        """
        Wrapper S3 method around the list_objects_v2 client function.

        Parameters
        ----------
            Limits the response to keys that begin with the specified prefix.
        start_after : str
            The S3 key to start after.
        delimiter : str
            A delimiter is a character you use to group keys.
        max_keys : int
            Sets the maximum number of keys returned in the response. By default, the action returns up to 1,000 key names. The response might contain fewer keys but will never contain more.

        Returns
        -------
        S3ListResponse
        """
        params = utils.build_s3_params(self.bucket, start_after=start_after, prefix=prefix, delimiter=delimiter, max_keys=max_keys)

        resp = utils.S3ListResponse(self.client, 'list_objects_v2', **params)

        return resp


    def list_object_versions(self, prefix: str=None, start_after: str=None, delimiter: str=None, max_keys: int=None):
        """
        Wrapper S3 method around the list_object_versions client function.

        Parameters
        ----------
        prefix : str
            Limits the response to keys that begin with the specified prefix.
        start_after : str
            The S3 key to start after.
        delimiter : str or None
            A delimiter is a character you use to group keys.
        max_keys : int
            Sets the maximum number of keys returned in the response. By default, the action returns up to 1,000 key names. The response might contain fewer keys but will never contain more.

        Returns
        -------
        S3ListResponse
        """
        params = utils.build_s3_params(self.bucket, key_marker=start_after, prefix=prefix, delimiter=delimiter, max_keys=max_keys)

        resp = utils.S3ListResponse(self.client, 'list_object_versions', **params)

        return resp


    def delete_object(self, key: str, version_id: str=None):
        """
        Delete a single object/version.

        Parameters
        ----------
        key : str
            The object key in the S3 bucket.
        version_id : str
            The S3 version id associated with the object.

        Returns
        -------
        S3Response
        """
        params = utils.build_s3_params(self.bucket, key=key, version_id=version_id)

        s3resp = utils.S3Response(self.client, 'delete_object', self._stream, **params)

        return s3resp


    def delete_objects(self, keys: List[dict]):
        """
        keys must be a list of dictionaries. The dicts must have the keys named Key and VersionId derived from the list_object_versions function. This function will automatically separate the list into 1000 count list chunks (required by the delete_objects request).

        Returns
        -------
        None
        """
        for keys in utils.chunks(keys, 1000):
            keys2 = []
            for key in keys:
                if 'key' in key:
                    key['Key'] = key.pop('key')
                if 'Key' not in key:
                    raise ValueError('"key" must be passed in the list of dict.')
                if 'version_id' in key:
                    key['VersionId'] = key.pop('version_id')
                if 'VersionId' not in key:
                    raise ValueError('"version_id" must be passed in the list of dict.')
                keys2.append(key)

            _ = self.client.delete_objects(Bucket=self.bucket, Delete={'Objects': keys2, 'Quiet': True})



########################################################
### S3 Locks and holds


    def get_object_legal_hold(self, key: str, version_id: str=None):
        """
        Method to get the staus of a legal hold of an object. The user must have s3:GetObjectLegalHold or b2:readFileLegalHolds permissions for this request.

        Parameters
        ----------
        key : str
            The key name for the uploaded object.
        version_id : str
            The S3 version id associated with the object.

        Returns
        -------
        S3Response
        """
        params = utils.build_s3_params(self.bucket, key=key, version_id=version_id)

        s3resp = utils.S3Response(self.client, 'get_object_legal_hold', self._stream, **params)

        return s3resp


    def put_object_legal_hold(self, key: str, lock: bool=False, version_id: str=None):
        """
        Method to put or remove a legal hold on an object. The user must have s3:PutObjectLegalHold or b2:writeFileLegalHolds permissions for this request.

        Parameters
        ----------
        key : str
            The key name for the uploaded object.
        lock : bool
            Should a lock be added to the object?
        version_id : str
            The S3 version id associated with the object.

        Returns
        -------
        None
        """
        if lock:
            hold = {'Status': 'ON'}
        else:
            hold = {'Status': 'OFF'}

        params = utils.build_s3_params(self.bucket, key=key, version_id=version_id)
        params['LegalHold'] = hold

        s3resp = utils.S3Response(self.client, 'put_object_legal_hold', self._stream, **params)

        return s3resp


    def get_object_lock_configuration(self):
        """
        Function to whther a bucket is configured to have object locks. The user must have s3:GetBucketObjectLockConfiguration or b2:readBucketRetentions permissions for this request.

        Returns
        -------
        S3Reponse
        """
        s3resp = utils.S3Response(self.client, 'get_object_lock_configuration', self._stream, Bucket=self.bucket)

        return s3resp


    def put_object_lock_configuration(self, lock: bool=False):
        """
        Function to enable or disable object locks for a bucket. The user must have s3:PutBucketObjectLockConfiguration or b2:writeBucketRetentions permissions for this request.

        Parameters
        ----------
        lock : bool
            Should a lock be enabled for the bucket?

        Returns
        -------
        boto3 response
        """
        if lock:
            hold = {'ObjectLockEnabled': 'Enable'}
        else:
            hold = {'ObjectLockEnabled': 'Disable'}

        # resp = s3.put_object_lock_configuration(Bucket=bucket, ObjectLockConfiguration=hold)
        s3resp = utils.S3Response(self.client, 'put_object_lock_configuration', self._stream, Bucket=self.bucket, ObjectLockConfiguration=hold)

        return s3resp


    def s3lock(self, key: str):
        """
        This class contains a locking mechanism by utilizing S3 objects. It has implementations for both shared and exclusive (the default) locks. It follows the same locking API as python thread locks (https://docs.python.org/3/library/threading.html#lock-objects), but with some extra methods for managing "deadlocks". The required S3 permissions are ListObjects, WriteObjects, and DeleteObjects.

        This initialized class can be used as a context manager exactly like thread locks.

        Parameters
        ----------
        key : str
            The base object key that will be given a lock. The extension ".lock" plus a unique lock id will be appended to the key, so the user is welcome to reference an existing object without worry that it will be overwritten or deleted.
        """
        return S3Lock(self._access_key_id, self._access_key, self.bucket, key, self._endpoint_url, max_attempts=self._max_attempts, retry_mode=self._retry_mode, read_timeout=self._read_timeout)








































