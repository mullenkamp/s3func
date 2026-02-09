#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Oct  8 11:02:46 2022

@author: mike
"""
# import io
# import os
# import pandas as pd
import orjson
# import botocore
from typing import Optional, Annotated
# import msgspec
import datetime
import xml.etree.ElementTree as ET
# import re
from urllib.parse import urlparse

#######################################################
### Parameters

# key_patterns = {
#     'b2': '{base_url}/{bucket}/{obj_key}',
#     'contabo': '{base_url}:{bucket}/{obj_key}',
#     }

b2_field_mappings = {
    'accountId': 'owner',
    'action': 'action',
    'bucketId': 'bucket_id',
    'contentLength': 'content_length',
    'contentMd5': 'content_md5',
    'contentSha1': 'content_sha1',
    'contentType': 'content_type',
    'fileId': 'version_id',
    'fileName': 'key',
    'fileRetention': 'object_retention',
    'uploadTimestamp': 'upload_timestamp'
    }

# url_regex = "^(https?://)?[a-z0-9]+?[\.a-z0-9]+\.[a-z]+?[\.a-z]+(\/[a-zA-Z0-9#]+\/?)*$"
# url_pattern = re.compile(url_regex)


##################################################
### msgspec classes


# class ValClass(msgspec.Struct):
#     """

#     """
#     def _validate(self) -> None:
#         msgspec.convert(msgspec.to_builtins(self), type=self.__class__)


# class S3ConnectionConfig(ValClass, omit_defaults=True):
#     service_name: str
#     aws_access_key_id: str
#     aws_secret_access_key: str
#     endpoint_url: Annotated[str, msgspec.Meta(pattern=url_regex)]=None


# class B2ConnectionConfig(ValClass):
#     application_key_id: str
#     application_key: str



# @define
# class TestClass:
#     service_name: str
#     aws_access_key_id: str
#     aws_secret_access_key: str
#     endpoint_url: str=None

#######################################################
### Helper Functions


def is_url(url):
    try:
        result = urlparse(url)
        return all([result.scheme, result.netloc])
    except AttributeError:
        return False


def build_conn_config(access_key_id, access_key, service_name, endpoint_url=None):
    """

    """
    service_name = service_name.lower()
    conn_config = {}
    if service_name == 's3':
        if isinstance(endpoint_url, str):
            if not is_url(endpoint_url):
                raise TypeError(f'{endpoint_url} is not a proper http url.')
            conn_config['endpoint_url'] = endpoint_url

        conn_config['aws_access_key_id'] = access_key_id
        conn_config['aws_secret_access_key'] = access_key
        conn_config['service_name'] = service_name

    elif service_name == 'b2':
        conn_config['application_key_id'] = access_key_id
        conn_config['application_key'] = access_key

    else:
        raise ValueError('service_name must be either s3 or b2.')

    return conn_config


def build_s3_params(bucket: str, key: str=None, start_after: str=None, prefix: str=None, delimiter: str=None, max_keys: int=None, key_marker: str=None, range_start: int=None, range_end: int=None, metadata: dict={}, content_type: str=None, version_id: str=None):
    """
    Builds parameters for S3 urllib3 requests (headers and query params).
    """
    query_params = {}
    headers = {}

    # Query Parameters
    if start_after:
        query_params['start-after'] = start_after
    if prefix:
        query_params['prefix'] = prefix
    if delimiter:
        query_params['delimiter'] = delimiter
    if max_keys:
        query_params['max-keys'] = str(max_keys)
    if key_marker:
        query_params['key-marker'] = key_marker
    if version_id:
        query_params['versionId'] = version_id

    # Headers
    if metadata:
        for k, v in metadata.items():
            headers[f'x-amz-meta-{k}'] = v
    if content_type:
        headers['Content-Type'] = content_type
    
    # Range
    if (range_start is not None) or (range_end is not None):
        range_dict = {}
        if range_start is not None:
            range_dict['start'] = str(range_start)
        else:
            range_dict['start'] = ''

        if range_end is not None:
            range_dict['end'] = str(range_end)
        else:
            range_dict['end'] = ''

        headers['Range'] = 'bytes={start}-{end}'.format(**range_dict)

    return query_params, headers


def build_url_headers(range_start: int=None, range_end: int=None):
    """

    """
    params = {}

    # Range
    if (range_start is not None) or (range_end is not None):
        range_dict = {}
        if range_start is not None:
            range_dict['start'] = str(range_start)
        else:
            range_dict['start'] = ''

        if range_end is not None:
            range_dict['end'] = str(range_end)
        else:
            range_dict['end'] = ''

        range1 = 'bytes={start}-{end}'.format(**range_dict)

        params['Range'] = range1

    return params


def build_b2_query_params(bucket: str=None, key: str=None, start_after: str=None, prefix: str=None, delimiter: str=None, max_keys: int=None, key_marker: str=None, range_start: int=None, range_end: int=None, metadata: dict={}, content_type: str=None, version_id: str=None):
    """

    """
    params = {}
    if bucket:
        params['bucketId'] = bucket
    if start_after:
        params['startFileName'] = start_after
    if key:
        params['fileName'] = key
    if prefix:
        params['prefix'] = prefix
    if delimiter:
        params['delimiter'] = delimiter
    if max_keys:
        params['maxFileCount'] = max_keys
    # if key_marker:
    #     params['KeyMarker'] = key_marker
    if version_id:
        params['fileId'] = version_id

    # Range
    # if (range_start is not None) or (range_end is not None):
    #     range_dict = {}
    #     if range_start is not None:
    #         range_dict['start'] = str(range_start)
    #     else:
    #         range_dict['start'] = ''

    #     if range_end is not None:
    #         range_dict['end'] = str(range_end)
    #     else:
    #         range_dict['end'] = ''

    #     range1 = 'bytes={start}-{end}'.format(**range_dict)

    #     params['range'] = range1

    return params


def chunks(lst, n_items):
    """
    Yield successive n-sized chunks from list.
    """
    lst_len = len(lst)
    n = lst_len//n_items

    pos = 0
    for i in range(0, n):
        yield lst[pos:pos + n_items]
        pos += n_items

    remainder = lst_len%n_items
    if remainder > 0:
        yield lst[pos:pos + remainder]


def add_metadata_from_urllib3(response):
    """
    Function to create metadata from the http headers/response.
    """
    headers = response.headers
    metadata = {'status': response.status}

    for key, value in headers.items():
        key = key.lower()
        if key == 'content-length':
            metadata['content_length'] = int(value)
        elif key == 'x-bz-file-name':
            metadata['key'] = value
        elif key == 'x-bz-file-id':
            metadata['version_id'] = value
            if '_u' in value:
                metadata['upload_timestamp'] = datetime.datetime.fromtimestamp(int(value.split('_u')[1]) * 0.001, datetime.timezone.utc)
        elif key == 'x-bz-upload-timestamp':
            metadata['upload_timestamp'] = datetime.datetime.fromtimestamp(int(value) * 0.001, datetime.timezone.utc)
        elif 'x-bz-info-' in key:
            new_key = key.split('x-bz-info-')[1]
            metadata[new_key] = value
        elif 'x-amz-meta-' in key:
            new_key = key.split('x-amz-meta-')[1]
            metadata[new_key] = value

    return metadata


def add_metadata_from_s3_xml(response, method=None):
    """
    Function to create metadata from the s3 headers and XML response.
    """
    # Parse Headers first (common to all)
    metadata = add_metadata_from_urllib3(response)
    if 'x-amz-version-id' in response.headers:
        metadata['version_id'] = response.headers['x-amz-version-id']
        
    # Timestamp extraction
    # Try Last-Modified, then Date, then current time as fallback (though risky for locks)
    if 'last-modified' in response.headers:
         try:
             dt = datetime.datetime.strptime(response.headers['last-modified'], '%a, %d %b %Y %H:%M:%S %Z')
             metadata['upload_timestamp'] = dt.replace(tzinfo=datetime.timezone.utc)
         except ValueError:
             pass
             
    if 'upload_timestamp' not in metadata and 'date' in response.headers:
         try:
             dt = datetime.datetime.strptime(response.headers['date'], '%a, %d %b %Y %H:%M:%S %Z')
             metadata['upload_timestamp'] = dt.replace(tzinfo=datetime.timezone.utc)
         except ValueError:
             pass

    if 'etag' in response.headers:
        metadata['etag'] = response.headers['etag'].strip('"')
    
    # Parse XML Body if error or specific methods
    if (response.status // 100) != 2:
        try:
            root = ET.fromstring(response.data)
            # AWS Error XML usually has Code and Message
            error = {}
            for child in root:
                error[child.tag] = child.text
            return metadata, error
        except ET.ParseError:
            return metadata, {'Code': 'ParseError', 'Message': 'Could not parse XML error response: ' + str(response.data)}
            
    # Success XML parsing for specific methods
    # Some S3 responses (like CopyObject) return data in XML
    # Only parse if content-type indicates XML to avoid consuming streams of non-XML data
    content_type = response.headers.get('content-type', '')
    if 'xml' in content_type and response.data and len(response.data) > 0:
        try:
            root = ET.fromstring(response.data)
            tag = root.tag.split('}')[-1]
            
            if tag == 'CopyObjectResult':
                # Flatten CopyObjectResult into metadata
                for child in root:
                    tag_name = child.tag.split('}')[-1]
                    if tag_name == 'ETag':
                        metadata['etag'] = child.text.strip('"')
                    elif tag_name == 'LastModified':
                         try:
                             dt = datetime.datetime.strptime(child.text, '%Y-%m-%dT%H:%M:%S.%fZ')
                             metadata['upload_timestamp'] = dt.replace(tzinfo=datetime.timezone.utc)
                         except ValueError:
                             pass

        except ET.ParseError:
            pass # Not XML or don't care

    return metadata, None

def get_metadata_from_b2_put_object(response):
    """
    Function to create metadata from the b2 put_object response body.
    """
    data = orjson.loads(response.data)

    meta = {}
    for key, val in data.items():
        if key in b2_field_mappings:
            if key == 'contentSha1':
                if 'unverified:' in val:
                    val = val.split('unverified:')[1]
            meta[b2_field_mappings[key]] = val

    if 'upload_timestamp' in meta:
        meta['upload_timestamp'] = datetime.datetime.fromtimestamp(meta['upload_timestamp'] * 0.001, datetime.timezone.utc)

    return meta

























