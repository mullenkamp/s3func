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
### Functions


def session(max_pool_connections: int = 10, max_attempts: int=3, timeout: int=120):
    """
    Function to setup a urllib3 pool manager for url downloads.

    Parameters
    ----------
    max_pool_connections : int
        The number of simultaneous connections for the S3 connection.
    max_attempts: int
        The number of retries if the connection fails.
    timeout: int
        The timeout in seconds.

    Returns
    -------
    Pool Manager object
    """
    timeout = urllib3.util.Timeout(timeout)
    retries = Retry(
        total=max_attempts,
        backoff_factor=1,
        )
    http = urllib3.PoolManager(num_pools=max_pool_connections, timeout=timeout, retries=retries)

    return http


def join_url_key(key: str, base_url: HttpUrl):
    """

    """
    if not base_url.endswith('/'):
        base_url += '/'
    url = urllib.parse.urljoin(base_url, key)

    return url


#######################################################
### Main class


class HttpSession:
    """

    """
    def __init__(self, max_pool_connections: int = 10, max_attempts: int=3, read_timeout: int=120):
        """
        Class using a urllib3 pool manager for url requests.

        Parameters
        ----------
        max_pool_connections : int
            The number of simultaneous connections for the S3 connection.
        max_attempts: int
            The number of retries if the connection fails.
        read_timeout: int
            The read timeout in seconds.

        """
        http_session = session(max_pool_connections, max_attempts, read_timeout)

        self._session = http_session
        # self.buffer_size = buffer_size


    def get_object(self, url: HttpUrl, range_start: int=None, range_end: int=None):
        """
        Use a GET request to download the body and headers of a url.

        Parameters
        ----------
        url : HttpUrl
            The URL to do the GET request on.
        range_start: int
            The byte range start for the file.
        range_end: int
            The byte range end for the file.
        chunk_size: int
            The amount of bytes to download as once.

        Returns
        -------
        HttpResponse
        """
        headers = utils.build_url_headers(range_start=range_start, range_end=range_end)

        response = self._session.request('get', url, headers=headers, preload_content=False)
        resp = utils.HttpResponse(response)

        return resp


    def head_object(self, url: HttpUrl):
        """
        Use a HEAD request to download the headers of a url.

        Parameters
        ----------
        url : HttpUrl
            The URL to do the HEAD request on.

        Returns
        -------
        HttpResponse
        """
        response = self._session.request('head', url)
        resp = utils.HttpResponse(response)

        return resp






































































