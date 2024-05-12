#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Oct  8 11:02:46 2022

@author: mike
"""
# import io
# import os
# import pandas as pd
from pydantic import BaseModel, HttpUrl

#######################################################
### Parameters

# key_patterns = {
#     'b2': '{base_url}/{bucket}/{obj_key}',
#     'contabo': '{base_url}:{bucket}/{obj_key}',
#     }


#######################################################
### Helper Functions

class ConnectionConfig(BaseModel):
    service_name: str
    endpoint_url: HttpUrl
    aws_access_key_id: str
    aws_secret_access_key: str


def build_s3_params(bucket: str, obj_key: str=None, start_after: str=None, prefix: str=None, delimiter: str=None, max_keys: int=None, key_marker: str=None, object_legal_hold: bool=False, range_start: int=None, range_end: int=None, metadata: dict=None, content_type: str=None, version_id: str=None):
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
    if object_legal_hold:
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


































