#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on May 13 08:04:38 2024

@author: mike
"""
from pydantic import HttpUrl
from typing import List, Union
# import requests
import urllib.parse
from urllib3.util import Retry, Timeout
# from requests import Session
# from requests.adapters import HTTPAdapter
import urllib3

from . import utils
# import utils

#######################################################
### Parameters



##################################################
### url session


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


def session(max_pool_connections: int = 10, max_attempts: int=3, read_timeout: int=120):
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


def join_url_obj_key(obj_key: str, base_url: HttpUrl):
    """

    """
    if not base_url.endswith('/'):
        base_url += '/'
    url = urllib.parse.urljoin(base_url, obj_key)

    return url


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


def get_object(url: HttpUrl, url_session: urllib3.poolmanager.PoolManager=None, range_start: int=None, range_end: int=None, chunk_size: int=524288, **url_session_kwargs):
    """

    """
    if url_session is None:
        url_session = session(**url_session_kwargs)

    headers = utils.build_url_headers(range_start=range_start, range_end=range_end)

    response = url_session.request('get', url, headers=headers, preload_content=False)
    resp = utils.HttpResponse(response)

    return resp


# def base_url_to_stream(obj_key: str, base_url: HttpUrl, url_session: urllib3.poolmanager.PoolManager=None, range_start: int=None, range_end: int=None, chunk_size: int=524288, **url_session_kwargs):
#     """

#     """
#     if not base_url.endswith('/'):
#         base_url += '/'
#     url = urllib.parse.urljoin(base_url, obj_key)
#     response = url_to_stream(url, url_session, range_start, range_end, chunk_size, **url_session_kwargs)

#     return response


def head_object(url: HttpUrl, url_session: urllib3.poolmanager.PoolManager=None, **url_session_kwargs):
    """

    """
    if url_session is None:
        url_session = session(**url_session_kwargs)

    response = url_session.request('head', url)
    resp = utils.HttpResponse(response)

    return resp


# def base_url_to_headers(obj_key: str, base_url: HttpUrl, session: urllib3.poolmanager.PoolManager=None, **url_session_kwargs):
#     """

#     """
#     if not base_url.endswith('/'):
#         base_url += '/'
#     url = urllib.parse.urljoin(base_url, obj_key)
#     response = url_to_headers(url, session, **url_session_kwargs)

#     return response







































































