#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Oct  8 11:02:46 2022

@author: mike
"""
# import io
# import os
# import pandas as pd
import urllib3
import botocore
import boto3
from pydantic import BaseModel, HttpUrl
from urllib3.util import Retry, Timeout
import datetime
import copy

#######################################################
### Parameters

# key_patterns = {
#     'b2': '{base_url}/{bucket}/{obj_key}',
#     'contabo': '{base_url}:{bucket}/{obj_key}',
#     }


##################################################
### pydantic classes


class ConnectionConfig(BaseModel):
    service_name: str
    endpoint_url: HttpUrl
    aws_access_key_id: str
    aws_secret_access_key: str



#######################################################
### Helper Functions


def build_s3_params(bucket: str, obj_key: str=None, start_after: str=None, prefix: str=None, delimiter: str=None, max_keys: int=None, key_marker: str=None, object_legal_hold: bool=False, range_start: int=None, range_end: int=None, metadata: dict={}, content_type: str=None, version_id: str=None):
    """

    """
    params = {'Bucket': bucket}
    if start_after:
        params['StartAfter'] = start_after
    if obj_key:
        params['Key'] = obj_key
    if prefix:
        params['Prefix'] = prefix
    if delimiter:
        params['Delimiter'] = delimiter
    if max_keys:
        params['MaxKeys'] = max_keys
    if key_marker:
        params['KeyMarker'] = key_marker
    if object_legal_hold: # This is for the put_object request
        params['ObjectLockLegalHoldStatus'] = 'ON'
    if metadata:
        params['Metadata'] = metadata
    if content_type:
        params['ContentType'] = content_type
    if version_id:
        params['VersionId'] = version_id

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
    # if 'Content-Length' in headers:
    #     metadata['content_length'] = int(headers['Content-Length'])
    # if 'x-bz-file-id' in headers:
    #     metadata['version_id'] = headers['x-bz-file-id']
    # if 'X-Bz-Upload-Timestamp' in headers:
    #     metadata['last_modified'] = datetime.datetime.fromtimestamp(int(headers['X-Bz-Upload-Timestamp']) * 0.001, datetime.timezone.utc)
    # elif 'x-bz-file-id' in headers:
    #     metadata['last_modified'] = datetime.datetime.fromtimestamp(int(headers['x-bz-file-id'].split('_u')[1]) * 0.001, datetime.timezone.utc)

    for key, value in headers.items():
        if key == 'Content-Length':
            metadata['content_length'] = int(headers['Content-Length'])
        elif key == 'x-bz-file-id':
            metadata['version_id'] = headers['x-bz-file-id']
            metadata['last_modified'] = datetime.datetime.fromtimestamp(int(headers['x-bz-file-id'].split('_u')[1]) * 0.001, datetime.timezone.utc)
        elif key == 'X-Bz-Upload-Timestamp':
            metadata['last_modified'] = datetime.datetime.fromtimestamp(int(headers['X-Bz-Upload-Timestamp']) * 0.001, datetime.timezone.utc)
        elif 'x-bz-info-' in key:
            new_key = key.split('x-bz-info-')[1]
            metadata[new_key] = value

    return metadata


def add_metadata_from_s3(response):
    """
    Function to create metadata from the s3 headers/response.
    """
    # headers = response.headers
    if 'Metadata' in response:
        metadata = response.pop('Metadata')
    else:
        metadata = {}

    if 'ETag' in response:
        metadata['etag'] = response['ETag'].strip('"')
    if 'VersionId' in response:
        metadata['version_id'] = response['VersionId']
        metadata['last_modified'] = datetime.datetime.fromtimestamp(int(metadata['version_id'].split('_u')[1]) * 0.001, datetime.timezone.utc)
    if 'ContentLength' in response:
        metadata['content_length'] = response['ContentLength']
    if 'HTTPStatusCode' in response['ResponseMetadata']:
        metadata['status'] = response['ResponseMetadata']['HTTPStatusCode']

    if 'LegalHold' in response:
        if 'Status' in response['LegalHold']:
            status = response['LegalHold']['Status']

            if status == 'ON':
                metadata['legal_hold'] = True
            else:
                metadata['legal_hold'] = False

    return metadata





# class ResponseStream(object):
#     """
#     In many applications, you'd like to access a requests response as a file-like object, simply having .read(), .seek(), and .tell() as normal. Especially when you only want to partially download a file, it'd be extra convenient if you could use a normal file interface for it, loading as needed.

# This is a wrapper class for doing that. Only bytes you request will be loaded - see the example in the gist itself.

# https://gist.github.com/obskyr/b9d4b4223e7eaf4eedcd9defabb34f13
#     """
#     def __init__(self, request_iterator):
#         self._bytes = io.BytesIO()
#         self._iterator = request_iterator


#     def iter_content(self, chunk_size=None):
#         return self._iterator

#     def _load_all(self):
#         self._bytes.seek(0, io.SEEK_END)
#         for chunk in self._iterator:
#             self._bytes.write(chunk)

#     def _load_until(self, goal_position):
#         current_position = self._bytes.seek(0, io.SEEK_END)
#         while current_position < goal_position:
#             try:
#                 current_position += self._bytes.write(next(self._iterator))
#             except StopIteration:
#                 break

#     def tell(self):
#         return self._bytes.tell()

#     def read(self, size=None):
#         left_off_at = self._bytes.tell()
#         if size is None:
#             self._load_all()
#         else:
#             goal_position = left_off_at + size
#             self._load_until(goal_position)

#         self._bytes.seek(left_off_at)
#         return self._bytes.read(size)

#     def seek(self, position, whence=io.SEEK_SET):
#         if whence ==io.SEEK_END:
#             self._load_all()
#         else:
#             self._bytes.seek(position, whence)


# class TimeoutHTTPAdapter(HTTPAdapter):
#     def __init__(self, *args, **kwargs):
#         if "timeout" in kwargs:
#             self.timeout = kwargs["timeout"]
#             del kwargs["timeout"]
#         super().__init__(*args, **kwargs)

#     def send(self, request, **kwargs):
#         timeout = kwargs.get("timeout")
#         if timeout is None and hasattr(self, 'timeout'):
#             kwargs["timeout"] = self.timeout
#         return super().send(request, **kwargs)


class S3ListResponse:
    """

    """
    def __init__(self, s3_client, method, **kwargs):
        """

        """
        error = {}

        func = getattr(s3_client, method)

        try:
            resp = func(**kwargs)
            status = resp['ResponseMetadata']['HTTPStatusCode']

            contents = []
            versions = []
            del_markers = []
            while True:
                if 'Versions' in resp:
                    for js in resp['Versions']:
                        versions.append({
                            'etag': js['ETag'].strip('"'),
                            'size': js['Size'],
                            'key': js['Key'],
                            'version_id': js['VersionId'],
                            'is_latest': js['IsLatest'],
                            'last_modified': js['LastModified'],
                            'owner': js['Owner']['ID'],
                            })
                    if 'DeleteMarkers' in resp:
                        for js in resp['DeleteMarkers']:
                            del_markers.append({
                                'key': js['Key'],
                                'version_id': js['VersionId'],
                                'is_latest': js['IsLatest'],
                                'last_modified': js['LastModified'],
                                'owner': js['Owner']['ID'],
                                })
                    if 'NextKeyMarker' in resp:
                        kwargs['KeyMarker'] = resp['NextKeyMarker']
                    else:
                        break

                elif 'Contents' in resp:
                    for js in resp['Contents']:
                        contents.append({
                            'etag': js['ETag'].strip('"'),
                            'size': js['Size'],
                            'key': js['Key'],
                            'last_modified': js['LastModified'],
                            })
                    if 'NextContinuationToken' in resp:
                        kwargs['ContinuationToken'] = resp['NextContinuationToken']
                    else:
                        break
                else:
                    break

                resp = func(**kwargs)

            metadata = {'status': status}
            if contents:
                metadata['contents'] = contents
            if versions:
                metadata['versions'] = versions
            if del_markers:
                metadata['delete_markers'] = del_markers

        except s3_client.exceptions.ClientError as err:
            resp = err.response.copy()
            status = resp['ResponseMetadata']['HTTPStatusCode']
            metadata = {'status': status}
            error = {'status': status}
            error.update({key.lower(): val for key, val in resp['Error'].items()})

        self.headers = {'ResponseMetadata': resp['ResponseMetadata']}
        self.metadata = metadata
        self.stream = None
        self.error = error
        self.status = status


class S3Response:
    """

    """
    def __init__(self, s3_client, method, **kwargs):
        """

        """
        stream = None
        error = {}

        func = getattr(s3_client, method)

        try:
            resp = func(**kwargs)
            metadata = add_metadata_from_s3(resp)
            status = resp['ResponseMetadata']['HTTPStatusCode']
            metadata['status'] = status

            if 'Body' in resp:
                if isinstance(resp['Body'], botocore.response.StreamingBody):
                    stream = resp.pop('Body')
                else:
                    del resp['Body']
        except s3_client.exceptions.ClientError as err:
            resp = err.response.copy()
            status = resp['ResponseMetadata']['HTTPStatusCode']
            metadata = {'status': status}
            error = {'status': status}
            error.update({key.lower(): val for key, val in resp['Error'].items()})

        self.headers = resp
        self.metadata = metadata
        self.stream = stream
        self.error = error
        self.status = status


class HttpResponse:
    """

    """
    def __init__(self, response: urllib3.HTTPResponse):
        """

        """
        stream = None
        error = {}
        metadata = add_metadata_from_urllib3(response)

        if (response.status // 100) != 2:
            error = response.json()
        else:
            stream = response

        self.headers = dict(response.headers)
        self.metadata = metadata
        self.stream = stream
        self.error = error
        self.status = response.status








# try:
#     resp = func(Bucket=bucket, Key=obj_key)

# except s3.exceptions.ClientError as err:
#     error = err



















