#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on May 13 08:04:38 2024

@author: mike
"""
from typing import List, Union
# import requests
import urllib.parse
# from requests import Session
# from requests.adapters import HTTPAdapter
import urllib3
# import b2sdk.v2 as b2
# from b2sdk._internal.session import B2Session

# from . import http_url
# import http_url

# from . import utils
# import utils

#######################################################
### Parameters

# key_patterns = {
#     'b2': '{base_url}/{bucket}/{obj_key}',
#     'contabo': '{base_url}:{bucket}/{obj_key}',
#     }

# multipart_size = 2**28

b2_auth_url = 'https://api.backblazeb2.com/b2api/v3/b2_authorize_account'

available_capabilities = [ "listKeys", "writeKeys", "deleteKeys", "listAllBucketNames", "listBuckets", "readBuckets", "writeBuckets", "deleteBuckets", "readBucketRetentions", "writeBucketRetentions", "readBucketEncryption", "writeBucketEncryption", "writeBucketNotifications", "listFiles", "readFiles", "shareFiles", "writeFiles", "deleteFiles", "readBucketNotifications", "readFileLegalHolds", "writeFileLegalHolds", "readFileRetentions", "writeFileRetentions", "bypassGovernance" ]

md5_locks = {
    'shared': 'cfcd208495d565ef66e7dff9f98764da',
    'exclusive': 'c4ca4238a0b923820dcc509a6f75849b'
    }


#########################################################
### Backblaze

info = b2.InMemoryAccountInfo()

b2_api = b2.B2Api(info)

session = B2Session(info)

sqlite_info = b2.SqliteAccountInfo()














def get_authorization(username, password, url_session=None, **url_session_kwargs):
    """

    """
    if url_session is None:
        url_session = http_url.session(**url_session_kwargs)

    headers = urllib3.make_headers(basic_auth=f'{username}:{password}')

    response = url_session.request('get', b2_auth_url, headers=headers)
    resp = utils.HttpResponse(response)

    return resp


def create_app_key(auth_dict: dict, capabilities: List[str], key_name: str, duration: int=None, bucket_id: str=None, prefix: str=None, url_session=None, **url_session_kwargs):
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

    if url_session is None:
        url_session = http_url.session(**url_session_kwargs)

    response = url_session.request('post', url, json=fields, headers={'Authorization': auth_token})
    resp = utils.HttpResponse(response)

    return resp


def list_buckets(auth_dict: dict, url_session=None, **url_session_kwargs):
    """

    """
    account_id = auth_dict['accountId']
    api_url = auth_dict['apiInfo']['storageApi']['apiUrl']
    auth_token = auth_dict['authorizationToken']

    fields = {
        'accountId': account_id,
        }

    url = urllib.parse.urljoin(api_url, '/b2api/v3/b2_list_buckets')

    if url_session is None:
        url_session = http_url.session(**url_session_kwargs)

    response = url_session.request('post', url, json=fields, headers={'Authorization': auth_token})
    resp = utils.HttpResponse(response)

    return resp


def list_objects(auth_dict: dict, bucket: str, prefix: str=None, start_after: str=None, delimiter: str=None, max_keys: int=10000, url_session=None, **url_session_kwargs):
    """
    b2_list_file_names
    """
    auth_token = auth_dict['authorizationToken']

    query_params = utils.build_b2_query_params(bucket=bucket, start_after=start_after, prefix=prefix, delimiter=delimiter, max_keys=max_keys)



def put_object():
    """

    """






































































