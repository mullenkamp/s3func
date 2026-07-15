#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on May 13 08:04:38 2024

@author: mike
"""
from typing import List, Union

# import requests
import urllib.parse
from urllib3.util import Retry, Timeout
import urllib3

from . import utils, response

# import utils, response

#######################################################
### Parameters


##################################################
### Functions


def session(max_connections: int = 10, max_attempts: int = 3, timeout: int = 120, connect_timeout: int = 30):
    """
    Function to setup a urllib3 pool manager for url downloads.

    Parameters
    ----------
    max_connections : int
        The number of simultaneous connections for the S3 connection.
    max_attempts: int
        The number of retries for connection errors and transient HTTP
        statuses (429, 500, 502, 503, 504). N retries = N+1 attempts.
    timeout: int
        The READ timeout in seconds: the maximum time a single read of the
        response may sit without data (an idle timeout), NOT a cap on total
        request duration.
    connect_timeout: int
        Seconds allowed for connection establishment. NOTE: urllib3 leaves
        the socket on this timeout during the request-SEND phase, so it also
        caps each body-send operation - which is why large bytes bodies are
        streamed (see utils.stream_body_threshold): streamed bodies send in
        16KiB chunks, making this a per-chunk idle bound during upload.

    Returns
    -------
    Pool Manager object
    """
    ## Timeout semantics, hard-won (2026-07-15 transport review):
    ## - A bare Timeout(x) sets total=x - a hard wall on WHOLE requests.
    ##   Never do that: multi-minute uploads/downloads are legitimate.
    ## - read= applies per response-read: slow-but-progressing GETs never
    ##   spuriously die; a stalled socket dies within `timeout`.
    ## - connect= ALSO governs each socket send op during the request body
    ##   phase. A monolithic bytes body = ONE send op spanning the whole
    ##   body, so it must be streamed (put_object wraps large bytes in
    ##   BytesIO); then a progressing upload never times out and a genuinely
    ##   stalled one dies within `connect_timeout`.
    timeout = urllib3.util.Timeout(connect=connect_timeout, read=timeout)
    retries = Retry(
        total=max_attempts,
        backoff_factor=1,
        # Transient statuses are retried with exponential backoff (Retry-After
        # honored when present - note urllib3 obeys it uncapped, so a
        # misconfigured endpoint sending a huge Retry-After can block up to
        # max_attempts * that value). POST is deliberately NOT in the retried
        # methods (urllib3 default): the S3 multi-object-delete and the
        # B2-native upload paths are POSTs, and b2.put_object has its own
        # manual retry loop that must not compound with this one.
        # 520-524 are the Cloudflare-style edge errors ("web server returned
        # an unknown error"/timeouts) that B2's fronting infrastructure emits
        # transiently - a bare 522 failed an ebooklet CI run un-retried
        # (2026-07-15); all are server-side transient conditions, safe to
        # retry on idempotent methods.
        status_forcelist=(429, 500, 502, 503, 504, 520, 521, 522, 523, 524),
        # Callers dispatch on resp.status (locking lock-PUT check, delete_objects
        # per-chunk failure handling, list iterators) and never expect a raise:
        # when retries exhaust, the LAST response must be returned, not
        # MaxRetryError. A status-retried PUT body is fine here because all
        # internal PUT bodies are bytes (non-seekable file bodies would not be
        # rewound - pre-existing hazard shared with connection-error retries).
        raise_on_status=False,
    )
    http = urllib3.PoolManager(maxsize=max_connections, timeout=timeout, retries=retries, block=True)

    return http


def join_url_key(key: str, base_url: str):
    """ """
    if not base_url.endswith('/'):
        base_url += '/'
    url = urllib.parse.urljoin(base_url, key)

    return url


#######################################################
### Main class


class HttpSession:
    """ """

    def __init__(self, max_connections: int = 10, max_attempts: int = 3, read_timeout: int = 120, stream=True):
        """
        Class using a urllib3 pool manager for url requests.

        Parameters
        ----------
        max_connections : int
            The number of simultaneous connections for the S3 connection.
        max_attempts: int
            The number of retries for connection errors and transient HTTP
            statuses (429, 500, 502, 503, 504). N retries = N+1 attempts.
        read_timeout: int
            The read timeout in seconds.
        stream : bool
            Should the connection stay open for streaming or should all the data/content be loaded during the initial request.

        """
        http_session = session(max_connections, max_attempts, read_timeout)

        self._session = http_session
        self._stream = stream
        # self.buffer_size = buffer_size

    def get_object(self, url: str, range_start: int = None, range_end: int = None):
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

        if not utils.is_url(url):
            raise TypeError(f'{url} is not a proper http url.')

        resp = self._session.request('get', url, headers=headers, preload_content=not self._stream)
        resp = response.HttpResponse(resp, self._stream)

        return resp

    def head_object(self, url: str):
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
        if not utils.is_url(url):
            raise TypeError(f'{url} is not a proper http url.')

        resp = self._session.request('head', url)
        resp = response.HttpResponse(resp, self._stream)

        return resp
