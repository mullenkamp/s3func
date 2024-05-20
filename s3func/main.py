#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on May 13 08:04:38 2024

@author: mike
"""
import io
# import os
from pydantic import HttpUrl
from typing import List, Union
import boto3
import botocore
import copy
# import requests
import urllib.parse
from urllib3.util import Retry, Timeout
import datetime
import hashlib
# from requests import Session
# from requests.adapters import HTTPAdapter
import urllib3
import uuid
from time import sleep
from timeit import default_timer

# from . import utils
import utils

#######################################################
### Parameters

# key_patterns = {
#     'b2': '{base_url}/{bucket}/{obj_key}',
#     'contabo': '{base_url}:{bucket}/{obj_key}',
#     }

# multipart_size = 2**28

b2_auth_url = 'https://api.backblazeb2.com/b2api/v3/b2_authorize_account'

available_capabilities = [ "listKeys", "writeKeys", "deleteKeys", "listAllBucketNames", "listBuckets", "readBuckets", "writeBuckets", "deleteBuckets", "readBucketRetentions", "writeBucketRetentions", "readBucketEncryption", "writeBucketEncryption", "writeBucketNotifications", "listFiles", "readFiles", "shareFiles", "writeFiles", "deleteFiles", "readBucketNotifications", "readFileLegalHolds", "writeFileLegalHolds", "readFileRetentions", "writeFileRetentions", "bypassGovernance" ]

##################################################
### S3 Client and url session


def s3_client(connection_config: utils.ConnectionConfig, max_pool_connections: int = 10, max_attempts: int = 3, retry_mode: str='adaptive', read_timeout: int=120):
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


# def url_session(max_pool_connections: int = 30, max_attempts: int=3, read_timeout: int=120):
#     """
#     Function to setup a requests url session for url downloads

#     Parameters
#     ----------
#     max_pool_connections : int
#         The number of simultaneous connections for the S3 connection.
#     max_attempts: int
#         The number of retries if the connection fails.
#     read_timeout: int
#         The read timeout in seconds.

#     Returns
#     -------
#     Session object
#     """
#     s = Session()
#     retries1 = Retry(
#         total=max_attempts,
#         backoff_factor=1,
#     )
#     s.mount('https://', TimeoutHTTPAdapter(timeout=read_timeout, max_retries=retries1, pool_connections=max_pool_connections, pool_maxsize=max_pool_connections))

#     return s


def url_session(max_pool_connections: int = 10, max_attempts: int=3, read_timeout: int=120):
    """
    Function to setup a urllib3 pool manager for url downloads.

    Parameters
    ----------
    max_pool_connections : int
        The number of simultaneous connections for the S3 connection.
    max_attempts: int
        The number of retries if the connection fails.
    read_timeout: int
        The read timeout in seconds.

    Returns
    -------
    Pool Manager object
    """
    timeout = urllib3.util.Timeout(read_timeout)
    retries = Retry(
        total=max_attempts,
        backoff_factor=1,
        )
    http = urllib3.PoolManager(num_pools=max_pool_connections, timeout=timeout, retries=retries)

    return http



#######################################################
### Main functions


# def url_to_stream(url: HttpUrl, session: requests.sessions.Session=None, range_start: int=None, range_end: int=None, chunk_size: int=524288, **url_session_kwargs):
#     """
#     requests version
#     """
#     if session is None:
#         session = url_session(**url_session_kwargs)

#     headers = build_url_headers(range_start=range_start, range_end=range_end)

#     response = session.get(url, headers=headers, stream=True)
#     stream = ResponseStream(response.iter_content(chunk_size))

#     return stream


def url_to_stream(url: HttpUrl, session: urllib3.poolmanager.PoolManager=None, range_start: int=None, range_end: int=None, chunk_size: int=524288, **url_session_kwargs):
    """

    """
    if session is None:
        session = url_session(**url_session_kwargs)

    headers = utils.build_url_headers(range_start=range_start, range_end=range_end)

    response = session.request('get', url, headers=headers, preload_content=False)
    resp = utils.HttpResponse(response)

    return resp


def base_url_to_stream(obj_key: str, base_url: HttpUrl, session: urllib3.poolmanager.PoolManager=None, range_start: int=None, range_end: int=None, chunk_size: int=524288, **url_session_kwargs):
    """

    """
    if not base_url.endswith('/'):
        base_url += '/'
    url = urllib.parse.urljoin(base_url, obj_key)
    response = url_to_stream(url, session, range_start, range_end, chunk_size, **url_session_kwargs)

    return response


def get_object(obj_key: str, bucket: str, s3: botocore.client.BaseClient = None, version_id: str=None, range_start: int=None, range_end: int=None, chunk_size: int=524288, **s3_client_kwargs):
    """
    Function to get an object from an S3 bucket. Either s3 or connection_config must be used. This function will return a file object of the object in the S3 location. This file object does not contain any data until data is read from it, which ensures large files are not completely read into memory.

    Parameters
    ----------
    obj_key : str
        The object key in the S3 bucket.
    bucket : str
        The bucket name.
    s3 : botocore.client.BaseClient
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
    if s3 is None:
        s3 = s3_client(**s3_client_kwargs)

    params = utils.build_s3_params(bucket, obj_key=obj_key, version_id=version_id, range_start=range_start, range_end=range_end)

    s3resp = utils.S3Response(s3, 'get_object', **params)

    return s3resp


def get_object_combo(obj_key: str, bucket: str, s3: botocore.client.BaseClient = None, session: urllib3.poolmanager.PoolManager=None, base_url: HttpUrl=None, version_id: str=None, range_start: int=None, range_end: int=None, chunk_size: int=524288, **kwargs):
    """
    Combo function to get an object from an S3 bucket either using the S3 get_object function or the base_url_to_stream function. One of s3, connection_config, or base_url must be used. This function will return a file object of the object in the S3 (or url) location. This file object does not contain any data until data is read from it, which ensures large files are not completely read into memory.

    Parameters
    ----------
    obj_key : str
        The object key in the S3 bucket.
    bucket : str
        The bucket name.
    s3 : botocore.client.BaseClient
        An S3 client object created via the s3_client function.
    base_url : HttpUrl
        The url path up to the obj_key.
    version_id : str
        The S3 version id associated with the object.
    range_start: int
        The byte range start for the file.
    range_end: int
        The byte range end for the file.
    chunk_size: int
        The amount of bytes to download as once.
    kwargs:
        Either the s3_client_kwargs or the url_session_kwargs depending on the input.

    Returns
    -------
    read-only file obj
    """
    ## Get the object
    if isinstance(base_url, str) and (version_id is None):
        stream = base_url_to_stream(obj_key, base_url, session, range_start, range_end, chunk_size, **kwargs)

    elif isinstance(s3, botocore.client.BaseClient):
        stream = get_object(obj_key, bucket, s3, version_id, range_start, range_end, chunk_size, **kwargs)

    else:
        raise TypeError('One of s3, connection_config, or public_url needs to be correctly defined.')

    return stream


def url_to_headers(url: HttpUrl, session: urllib3.poolmanager.PoolManager=None, **url_session_kwargs):
    """

    """
    if session is None:
        session = url_session(**url_session_kwargs)

    response = session.request('head', url)
    resp = utils.HttpResponse(response)

    return resp


def base_url_to_headers(obj_key: str, base_url: HttpUrl, session: urllib3.poolmanager.PoolManager=None, **url_session_kwargs):
    """

    """
    if not base_url.endswith('/'):
        base_url += '/'
    url = urllib.parse.urljoin(base_url, obj_key)
    response = url_to_headers(url, session, **url_session_kwargs)

    return response


def head_object(obj_key: str, bucket: str, s3: botocore.client.BaseClient = None, version_id: str=None, **s3_client_kwargs):
    """
    Function to get an object from an S3 bucket. Either s3 or connection_config must be used. This function will return a file object of the object in the S3 location. This file object does not contain any data until data is read from it, which ensures large files are not completely read into memory.

    Parameters
    ----------
    obj_key : str
        The object key in the S3 bucket.
    bucket : str
        The bucket name.
    s3 : botocore.client.BaseClient
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
    if s3 is None:
        s3 = s3_client(**s3_client_kwargs)

    params = utils.build_s3_params(bucket, obj_key=obj_key, version_id=version_id)

    s3resp = utils.S3Response(s3, 'head_object', **params)

    return s3resp


def put_object(s3: botocore.client.BaseClient, bucket: str, obj_key: str, obj: Union[bytes, io.BufferedIOBase], metadata: dict={}, content_type: str=None, object_legal_hold: bool=False):
    """
    Function to upload data to an S3 bucket. This function will iteratively write the input file_obj in chunks ensuring that little memory is needed writing the object.

    Parameters
    ----------
    s3 : boto3.client
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

    s3resp = utils.S3Response(s3, 'put_object', **params)
    s3resp.metadata.update(metadata)

    return s3resp


#####################################
### Other S3 operations


def list_objects(s3: botocore.client.BaseClient, bucket: str, prefix: str=None, start_after: str=None, delimiter: str=None, max_keys: int=None):
    """
    Wrapper S3 function around the list_objects_v2 base function.

    Parameters
    ----------
    s3 : boto3.client
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

    resp = utils.S3ListResponse(s3, 'list_objects_v2', **params)

    return resp


def list_object_versions(s3: botocore.client.BaseClient, bucket: str, prefix: str=None, start_after: str=None, delimiter: str=None, max_keys: int=None):
    """
    Wrapper S3 function around the list_object_versions base function.

    Parameters
    ----------
    s3 : boto3.client
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

    resp = utils.S3ListResponse(s3, 'list_object_versions', **params)

    return resp


def delete_object(s3: botocore.client.BaseClient, bucket: str, obj_key: str, version_id: str=None):
    """
    obj_keys must be a list of dictionaries. The dicts must have the keys named Key and VersionId derived from the list_object_versions function. This function will automatically separate the list into 1000 count list chunks (required by the delete_objects request).

    Returns
    -------
    None
    """
    params = utils.build_s3_params(bucket, obj_key=obj_key, version_id=version_id)

    s3resp = utils.S3Response(s3, 'delete_object', **params)

    return s3resp


def delete_objects(s3: botocore.client.BaseClient, bucket: str, obj_keys: List[dict]):
    """
    obj_keys must be a list of dictionaries. The dicts must have the keys named Key and VersionId derived from the list_object_versions function. This function will automatically separate the list into 1000 count list chunks (required by the delete_objects request).

    Returns
    -------
    None
    """
    for keys in utils.chunks(obj_keys, 1000):
        _ = s3.delete_objects(Bucket=bucket, Delete={'Objects': keys, 'Quiet': True})


########################################################
### S3 Locks and holds


def get_object_legal_hold(s3: botocore.client.BaseClient, bucket: str, obj_key: str, version_id: str=None):
    """
    Function to get the staus of a legal hold of an object. The user must have s3:GetObjectLegalHold or b2:readFileLegalHolds permissions for this request.

    Parameters
    ----------
    s3 : boto3.client
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

    s3resp = utils.S3Response(s3, 'get_object_legal_hold', **params)

    return s3resp


def put_object_legal_hold(s3: botocore.client.BaseClient, bucket: str, obj_key: str, lock: bool=False, version_id: str=None):
    """
    Function to put or remove a legal hold on an object. The user must have s3:PutObjectLegalHold or b2:writeFileLegalHolds permissions for this request.

    Parameters
    ----------
    s3 : boto3.client
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

    s3resp = utils.S3Response(s3, 'put_object_legal_hold', **params)

    return s3resp


def get_object_lock_configuration(s3: botocore.client.BaseClient, bucket: str):
    """
    Function to whther a bucket is configured to have object locks. The user must have s3:GetBucketObjectLockConfiguration or b2:readBucketRetentions permissions for this request.

    Parameters
    ----------
    s3 : boto3.client
        A boto3 client object
    bucket : str
        The S3 bucket.

    Returns
    -------
    S3Reponse
    """
    s3resp = utils.S3Response(s3, 'get_object_lock_configuration', Bucket=bucket)

    return s3resp


def put_object_lock_configuration(s3: botocore.client.BaseClient, bucket: str, lock: bool=False):
    """
    Function to enable or disable object locks for a bucket. The user must have s3:PutBucketObjectLockConfiguration or b2:writeBucketRetentions permissions for this request.

    Parameters
    ----------
    s3 : boto3.client
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
    s3resp = utils.S3Response(s3, 'put_object_lock_configuration', Bucket=bucket, ObjectLockConfiguration=hold)

    return s3resp


class S3Lock:
    """

    """
    def __init__(self, s3: botocore.client.BaseClient, bucket: str, obj_key: str):
        """

        """
        obj_lock_key = obj_key + '.lock'
        _ = self._list_object_versions(s3, bucket, obj_lock_key)

        self._s3 = s3
        self._bucket = bucket
        self._obj_lock_key = obj_lock_key
        self._obj_key = obj_key
        self._version_id = ''
        self._last_modified = None
        self._uuid = uuid.uuid4().bytes


    @staticmethod
    def _list_object_versions(s3, bucket, obj_lock_key):
        """

        """
        objs = list_object_versions(s3, bucket, prefix=obj_lock_key)
        if objs.status in (401, 403):
            raise urllib3.exceptions.HTTPError(str(objs.error)[1:-1])

        return objs


    def other_locks(self):
        """

        """
        objs = self._list_object_versions(self._s3, self._bucket, self._obj_lock_key)

        meta = objs.metadata
        if 'versions' in meta:
            return [l for l in meta['versions'] if l['version_id'] != self._version_id]
        else:
            return []


    def break_other_locks(self):
        """

        """
        objs = self._list_object_versions(self._s3, self._bucket, self._obj_lock_key)

        meta = objs.metadata
        obj_keys = []
        if 'versions' in meta:
            for l in meta['versions']:
                obj_keys.append({'Key': l['key'], 'VersionId': l['version_id']})

            delete_objects(self._s3, self._bucket, obj_keys)

        return obj_keys


    def locked(self):
        """

        """
        objs = self._list_object_versions(self._s3, self._bucket, self._obj_lock_key)
        meta = objs.metadata
        if 'versions' in meta:
            return True
        else:
            return False


    def aquire(self, blocking=True, timeout=-1):
        """

        """
        if self._last_modified is None:
            resp = put_object(self._s3, self._bucket, self._obj_lock_key, self._uuid)
            if resp.status != 200:
                raise urllib3.exceptions.HTTPError(str(resp.error)[1:-1])
            self._version_id = resp.metadata['version_id']
            self._last_modified = resp.metadata['last_modified']

            objs = self.other_locks()

            objs2 = [l for l in objs if l['last_modified'] < self._last_modified]

            if objs2:
                start_time = default_timer()

                while blocking:
                    sleep(2)
                    objs = self.other_locks()
                    objs2 = [l for l in objs if l['last_modified'] < self._last_modified]
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

        """
        if self._last_modified is not None:
            _ = delete_object(self._s3, self._bucket, self._obj_lock_key, self._version_id)
            self._version_id = ''
            self._last_modified = None





#########################################################
### Backblaze


def get_authorization_b2(username, password, session=None, **url_session_kwargs):
    """

    """
    if session is None:
        session = url_session(**url_session_kwargs)

    headers = urllib3.make_headers(basic_auth=f'{username}:{password}')

    response = session.request('get', b2_auth_url, headers=headers)
    resp = utils.HttpResponse(response)

    return resp


def create_app_key_b2(auth_dict: dict, capabilities: List[str], key_name: str, duration: int=None, bucket_id: str=None, prefix: str=None, session=None, **url_session_kwargs):
    """

    """
    account_id = auth_dict['accountId']
    api_url = auth_dict['apiInfo']['storageApi']['apiUrl']
    auth_token = auth_dict['authorizationToken']

    fields = {
        'accountId': account_id,
        'capabilities': capabilities,
        'keyName': key_name}

    if isinstance(duration, int):
        fields['validDurationInSeconds'] = duration

    if isinstance(bucket_id, str):
        fields['bucketId'] = bucket_id

    if isinstance(prefix, str):
        fields['namePrefix'] = prefix

    url = urllib.parse.urljoin(api_url, '/b2api/v3/b2_create_key')

    if session is None:
        session = url_session(**url_session_kwargs)

    response = session.request('post', url, json=fields, headers={'Authorization': auth_token})
    resp = utils.HttpResponse(response)

    return resp


def list_buckets_b2(auth_dict: dict, session=None, **url_session_kwargs):
    """

    """
    account_id = auth_dict['accountId']
    api_url = auth_dict['apiInfo']['storageApi']['apiUrl']
    auth_token = auth_dict['authorizationToken']

    fields = {
        'accountId': account_id,
        }

    url = urllib.parse.urljoin(api_url, '/b2api/v3/b2_list_buckets')

    if session is None:
        session = url_session(**url_session_kwargs)

    response = session.request('post', url, json=fields, headers={'Authorization': auth_token})
    resp = utils.HttpResponse(response)

    return resp












































































