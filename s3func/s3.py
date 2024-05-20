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
# import uuid
from time import sleep
from timeit import default_timer

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
### Main functions


def client(connection_config: utils.ConnectionConfig, max_pool_connections: int = 10, max_attempts: int = 3, retry_mode: str='adaptive', read_timeout: int=120):
    """
    Function to establish a client connection with an S3 account. This can use the legacy connect (signature_version s3) and the current version.

    Parameters
    ----------
    connection_config : dict
        A dictionary of the connection info necessary to establish an S3 connection. It should contain service_name, endpoint_url, aws_access_key_id, and aws_secret_access_key.
    max_pool_connections : int
        The number of simultaneous connections for the S3 connection.
    max_attempts: int
        The number of max attempts passed to the "retries" option in the S3 config.
    retry_mode: str
        The retry mode passed to the "retries" option in the S3 config.
    read_timeout: int
        The read timeout in seconds passed to the "retries" option in the S3 config.

    Returns
    -------
    S3 client object
    """
    ## Validate config
    _ = utils.ConnectionConfig(**connection_config)

    s3_config = copy.deepcopy(connection_config)

    if 'config' in s3_config:
        config0 = s3_config.pop('config')
        config0.update({'max_pool_connections': max_pool_connections, 'retries': {'mode': retry_mode, 'max_attempts': max_attempts}, 'read_timeout': read_timeout})
        config1 = boto3.session.Config(**config0)

        s3_config1 = s3_config.copy()
        s3_config1.update({'config': config1})

        s3 = boto3.client(**s3_config1)
    else:
        s3_config.update({'config': botocore.config.Config(max_pool_connections=max_pool_connections, retries={'mode': retry_mode, 'max_attempts': max_attempts}, read_timeout=read_timeout)})
        s3 = boto3.client(**s3_config)

    return s3


def get_object(obj_key: str, bucket: str, s3_client: botocore.client.BaseClient = None, version_id: str=None, range_start: int=None, range_end: int=None, chunk_size: int=524288, **s3_client_kwargs):
    """
    Function to get an object from an S3 bucket. Either s3 or connection_config must be used. This function will return a file object of the object in the S3 location. This file object does not contain any data until data is read from it, which ensures large files are not completely read into memory.

    Parameters
    ----------
    obj_key : str
        The object key in the S3 bucket.
    bucket : str
        The bucket name.
    s3_client : botocore.client.BaseClient
        An S3 client object created via the s3_client function.
    version_id : str
        The S3 version id associated with the object.
    range_start: int
        The byte range start for the file.
    range_end: int
        The byte range end for the file.
    chunk_size: int
        The amount of bytes to download as once.
    s3_client_kwargs:
        kwargs to the s3_client function if the s3 parameter was not passed.

    Returns
    -------
    read-only file obj
    """
    ## Get the object
    if s3_client is None:
        s3_client = client(**s3_client_kwargs)

    params = utils.build_s3_params(bucket, obj_key=obj_key, version_id=version_id, range_start=range_start, range_end=range_end)

    s3resp = utils.S3Response(s3_client, 'get_object', **params)

    return s3resp


# def get_object_combo(obj_key: str, bucket: str, s3: botocore.client.BaseClient = None, session: urllib3.poolmanager.PoolManager=None, base_url: HttpUrl=None, version_id: str=None, range_start: int=None, range_end: int=None, chunk_size: int=524288, **kwargs):
#     """
#     Combo function to get an object from an S3 bucket either using the S3 get_object function or the base_url_to_stream function. One of s3, connection_config, or base_url must be used. This function will return a file object of the object in the S3 (or url) location. This file object does not contain any data until data is read from it, which ensures large files are not completely read into memory.

#     Parameters
#     ----------
#     obj_key : str
#         The object key in the S3 bucket.
#     bucket : str
#         The bucket name.
#     s3 : botocore.client.BaseClient
#         An S3 client object created via the s3_client function.
#     base_url : HttpUrl
#         The url path up to the obj_key.
#     version_id : str
#         The S3 version id associated with the object.
#     range_start: int
#         The byte range start for the file.
#     range_end: int
#         The byte range end for the file.
#     chunk_size: int
#         The amount of bytes to download as once.
#     kwargs:
#         Either the s3_client_kwargs or the url_session_kwargs depending on the input.

#     Returns
#     -------
#     read-only file obj
#     """
#     ## Get the object
#     if isinstance(base_url, str) and (version_id is None):
#         stream = http_url.base_url_to_stream(obj_key, base_url, session, range_start, range_end, chunk_size, **kwargs)

#     elif isinstance(s3, botocore.client.BaseClient):
#         stream = get_object(obj_key, bucket, s3, version_id, range_start, range_end, chunk_size, **kwargs)

#     else:
#         raise TypeError('One of s3, connection_config, or public_url needs to be correctly defined.')

#     return stream


def head_object(obj_key: str, bucket: str, s3_client: botocore.client.BaseClient = None, version_id: str=None, **s3_client_kwargs):
    """
    Function to get an object from an S3 bucket. Either s3 or connection_config must be used. This function will return a file object of the object in the S3 location. This file object does not contain any data until data is read from it, which ensures large files are not completely read into memory.

    Parameters
    ----------
    obj_key : str
        The object key in the S3 bucket.
    bucket : str
        The bucket name.
    s3_client : botocore.client.BaseClient
        An S3 client object created via the s3_client function.
    version_id : str
        The S3 version id associated with the object.
    s3_client_kwargs:
        kwargs to the s3_client function if the s3 parameter was not passed.

    Returns
    -------
    read-only file obj
    """
    ## Get the object
    if s3_client is None:
        s3_client = client(**s3_client_kwargs)

    params = utils.build_s3_params(bucket, obj_key=obj_key, version_id=version_id)

    s3resp = utils.S3Response(s3_client, 'head_object', **params)

    return s3resp


def put_object(s3_client: botocore.client.BaseClient, bucket: str, obj_key: str, obj: Union[bytes, io.BufferedIOBase], metadata: dict={}, content_type: str=None, object_legal_hold: bool=False):
    """
    Function to upload data to an S3 bucket. This function will iteratively write the input file_obj in chunks ensuring that little memory is needed writing the object.

    Parameters
    ----------
    s3_client : boto3.client
        A boto3 client object
    bucket : str
        The S3 bucket.
    obj_key : str
        The key name for the uploaded object.
    obj : bytes, io.BytesIO, or io.BufferedIOBase
        The file object to be uploaded.
    metadata : dict or None
        A dict of the metadata that should be saved along with the object.
    content_type : str
        The http content type to associate the object with.
    object_legal_hold : bool
        Should the object be uploaded with a legal hold?

    Returns
    -------
    None
    """
    # TODO : In python version 3.11, the file_digest function can input a file object
    if isinstance(obj, (bytes, bytearray)) and ('content-md5' not in metadata):
        metadata['content-md5'] = hashlib.md5(obj).hexdigest()
    params = utils.build_s3_params(bucket, obj_key=obj_key, metadata=metadata, content_type=content_type, object_legal_hold=object_legal_hold)
    params['Body'] = obj

    s3resp = utils.S3Response(s3_client, 'put_object', **params)
    s3resp.metadata.update(metadata)

    return s3resp


#####################################
### Other S3 operations


def list_objects(s3_client: botocore.client.BaseClient, bucket: str, prefix: str=None, start_after: str=None, delimiter: str=None, max_keys: int=None):
    """
    Wrapper S3 function around the list_objects_v2 base function.

    Parameters
    ----------
    s3_client : boto3.client
        A boto3 client object
    bucket : str
        The S3 bucket.
    prefix : str
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
    params = utils.build_s3_params(bucket, start_after=start_after, prefix=prefix, delimiter=delimiter, max_keys=max_keys)

    resp = utils.S3ListResponse(s3_client, 'list_objects_v2', **params)

    return resp


def list_object_versions(s3_client: botocore.client.BaseClient, bucket: str, prefix: str=None, start_after: str=None, delimiter: str=None, max_keys: int=None):
    """
    Wrapper S3 function around the list_object_versions base function.

    Parameters
    ----------
    s3_client : boto3.client
        A boto3 client object
    bucket : str
        The S3 bucket.
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
    params = utils.build_s3_params(bucket, key_marker=start_after, prefix=prefix, delimiter=delimiter, max_keys=max_keys)

    resp = utils.S3ListResponse(s3_client, 'list_object_versions', **params)

    return resp


def delete_object(s3_client: botocore.client.BaseClient, bucket: str, obj_key: str, version_id: str=None):
    """
    obj_keys must be a list of dictionaries. The dicts must have the keys named Key and VersionId derived from the list_object_versions function. This function will automatically separate the list into 1000 count list chunks (required by the delete_objects request).

    Returns
    -------
    None
    """
    params = utils.build_s3_params(bucket, obj_key=obj_key, version_id=version_id)

    s3resp = utils.S3Response(s3_client, 'delete_object', **params)

    return s3resp


def delete_objects(s3_client: botocore.client.BaseClient, bucket: str, obj_keys: List[dict]):
    """
    obj_keys must be a list of dictionaries. The dicts must have the keys named Key and VersionId derived from the list_object_versions function. This function will automatically separate the list into 1000 count list chunks (required by the delete_objects request).

    Returns
    -------
    None
    """
    for keys in utils.chunks(obj_keys, 1000):
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

        _ = s3_client.delete_objects(Bucket=bucket, Delete={'Objects': keys2, 'Quiet': True})


########################################################
### S3 Locks and holds


def get_object_legal_hold(s3_client: botocore.client.BaseClient, bucket: str, obj_key: str, version_id: str=None):
    """
    Function to get the staus of a legal hold of an object. The user must have s3:GetObjectLegalHold or b2:readFileLegalHolds permissions for this request.

    Parameters
    ----------
    s3_client : boto3.client
        A boto3 client object
    bucket : str
        The S3 bucket.
    obj_key : str
        The key name for the uploaded object.

    Returns
    -------
    S3Response
    """
    params = utils.build_s3_params(bucket, obj_key=obj_key, version_id=version_id)

    s3resp = utils.S3Response(s3_client, 'get_object_legal_hold', **params)

    return s3resp


def put_object_legal_hold(s3_client: botocore.client.BaseClient, bucket: str, obj_key: str, lock: bool=False, version_id: str=None):
    """
    Function to put or remove a legal hold on an object. The user must have s3:PutObjectLegalHold or b2:writeFileLegalHolds permissions for this request.

    Parameters
    ----------
    s3_client : boto3.client
        A boto3 client object
    bucket : str
        The S3 bucket.
    obj_key : str
        The key name for the uploaded object.
    lock : bool
        Should a lock be added to the object?

    Returns
    -------
    None
    """
    if lock:
        hold = {'Status': 'ON'}
    else:
        hold = {'Status': 'OFF'}

    params = utils.build_s3_params(bucket, obj_key=obj_key, version_id=version_id)
    params['LegalHold'] = hold

    s3resp = utils.S3Response(s3_client, 'put_object_legal_hold', **params)

    return s3resp


def get_object_lock_configuration(s3_client: botocore.client.BaseClient, bucket: str):
    """
    Function to whther a bucket is configured to have object locks. The user must have s3:GetBucketObjectLockConfiguration or b2:readBucketRetentions permissions for this request.

    Parameters
    ----------
    s3_client : boto3.client
        A boto3 client object
    bucket : str
        The S3 bucket.

    Returns
    -------
    S3Reponse
    """
    s3resp = utils.S3Response(s3_client, 'get_object_lock_configuration', Bucket=bucket)

    return s3resp


def put_object_lock_configuration(s3_client: botocore.client.BaseClient, bucket: str, lock: bool=False):
    """
    Function to enable or disable object locks for a bucket. The user must have s3:PutBucketObjectLockConfiguration or b2:writeBucketRetentions permissions for this request.

    Parameters
    ----------
    s3_client : boto3.client
        A boto3 client object
    bucket : str
        The S3 bucket.
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
    s3resp = utils.S3Response(s3_client, 'put_object_lock_configuration', Bucket=bucket, ObjectLockConfiguration=hold)

    return s3resp


class S3Lock:
    """

    """
    def __init__(self, s3_client: botocore.client.BaseClient, bucket: str, obj_key: str, version_id: str=None):
        """
        This class contains a locking mechanism by utilizing S3 objects and associated versions. It has implementations for both shared and exclusive (the default) locks. It follows the same locking API as python thread locks (https://docs.python.org/3/library/threading.html#lock-objects), but with some extra methods for managing "deadlocks". Object versioning MUST be activated in the S3 bucket for this to work. The required S3 permissions are ListObjects, WriteObjects, and DeleteObjects.

        This initialized class can be used as a context manager exactly like the thread locks.

        Parameters
        ----------
        s3_client : botocore.client.BaseClient
            An initialised boto3/botocore Client. The user is recommended to use the client function in this package.
        bucket : str
            The S3 bucket
        obj_key : str
            The base object key that will be given a lock. The extension ".lock" will be appended to the key, so the user is welcome to reference an existing object without worry that it will be overwritten.
        version_id : str
            Open a previously locked object version. This will allow the user to continue a lock from a previous instance (e.g. if that instance crashed). This will raise a KeyError if the object version doesn't exist.
        """
        obj_lock_key = obj_key + '.lock'
        objs = self._list_object_versions(s3_client, bucket, obj_lock_key)

        if isinstance(version_id, str):
            objs = [l for l in objs if l['version_id'] == version_id]
            if objs:
                self._version_id = version_id
                self._last_modified = objs[0]['last_modified']
            else:
                raise KeyError(f'No object version with {version_id}.')
        else:
            self._version_id = ''
            self._last_modified = None

        self._s3_client = s3_client
        self._bucket = bucket
        self._obj_lock_key = obj_lock_key
        self._obj_key = obj_key


    @staticmethod
    def _list_object_versions(s3_client, bucket, obj_lock_key):
        """

        """
        objs = list_object_versions(s3_client, bucket, prefix=obj_lock_key)
        if objs.status in (401, 403):
            raise urllib3.exceptions.HTTPError(str(objs.error)[1:-1])

        meta = objs.metadata
        res = []
        if 'versions' in meta:
            for l in meta['versions']:
                if l['etag'] == md5_locks['exclusive']:
                    l['lock_type'] = 'exclusive'
                elif l['etag'] == md5_locks['shared']:
                    l['lock_type'] = 'shared'
                else:
                    raise ValueError('This lock file was created by something else...')
                res.append(l)

        return res


    def other_locks(self):
        """
        Method to list all of the other locks that might also be on the object.

        Returns
        -------
        list of dict
        """
        objs = self._list_object_versions(self._s3_client, self._bucket, self._obj_lock_key)

        if objs:
            return [l for l in objs if l['version_id'] != self._version_id]
        else:
            return []


    def break_other_locks(self):
        """
        Removes all other locks that are on the object. This is only meant to be used in deadlock circumstances.

        Returns
        -------
        list of dict of the removed keys/versions
        """
        objs = self._list_object_versions(self._s3_client, self._bucket, self._obj_lock_key)

        obj_keys = []
        if objs:
            for l in objs:
                obj_keys.append({'Key': l['key'], 'VersionId': l['version_id']})

            delete_objects(self._s3_client, self._bucket, obj_keys)

        return obj_keys


    def locked(self):
        """
        Checks to see if there's a lock on the object. 

        Returns
        -------
        bool
        """
        objs = self._list_object_versions(self._s3_client, self._bucket, self._obj_lock_key)
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
        if self._last_modified is None:
            if exclusive:
                body = b'1'
            else:
                body = b'0'
            resp = put_object(self._s3_client, self._bucket, self._obj_lock_key, body)
            if resp.status != 200:
                raise urllib3.exceptions.HTTPError(str(resp.error)[1:-1])
            self._version_id = resp.metadata['version_id']
            self._last_modified = resp.metadata['last_modified']

            objs = self.other_locks()

            objs2 = [l for l in objs if (l['last_modified'] < self._last_modified) and (l['lock_type'] == 'exclusive')]

            if objs2:
                start_time = default_timer()

                while blocking:
                    sleep(2)
                    objs = self.other_locks()
                    objs2 = [l for l in objs if (l['last_modified'] < self._last_modified) and (l['lock_type'] == 'exclusive')]
                    if len(objs2) == 0:
                        return True
                    else:
                        if timeout > 0:
                            duration = default_timer() - start_time
                            if duration > timeout:
                                break

                return False
            else:
                return True
        else:
            return True


    def release(self):
        """
        Release the lock. It can only release the lock that was created via this instance. Returns nothing.
        """
        if self._last_modified is not None:
            _ = delete_object(self._s3_client, self._bucket, self._obj_lock_key, self._version_id)
            self._version_id = ''
            self._last_modified = None

    def __enter__(self):
        self.aquire()

    def __exit__(self, *args):
        self.release()
















































































