#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Regression tests for the 0.9.2 retry policy: transient HTTP statuses
(429/500/502/503/504) are retried with backoff on idempotent methods, exhausted
retries RETURN the final response (never raise), and POSTs are never
status-retried.

Runs against a local scripted HTTP server - no credentials, no network.
"""
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest
from urllib3.util import Retry

from s3func import s3, http_url

##############################################
### Local flaky server

_ERROR_BODY = (
    b'<?xml version="1.0" encoding="UTF-8"?>'
    b'<Error><Code>InternalError</Code><Message>scripted test error</Message></Error>'
)


class _ScriptedHandler(BaseHTTPRequestHandler):
    """Responds with a scripted sequence of statuses; the last entry repeats."""

    protocol_version = 'HTTP/1.1'
    script = [(200, {})]        # [(status, extra_headers), ...] - class-level, set per test
    requests_log = []           # [(method, path), ...] - class-level, reset per test

    def _respond(self):
        length = int(self.headers.get('Content-Length', 0))
        if length:
            self.rfile.read(length)

        idx = len(type(self).requests_log)
        type(self).requests_log.append((self.command, self.path))
        status, extra_headers = self.script[min(idx, len(self.script) - 1)]

        body = _ERROR_BODY if status >= 400 else b'ok'
        self.send_response(status)
        for name, value in extra_headers.items():
            self.send_header(name, value)
        self.send_header('Content-Length', str(len(body)))
        self.send_header('Content-Type', 'application/xml' if status >= 400 else 'application/octet-stream')
        self.end_headers()
        if self.command != 'HEAD':
            self.wfile.write(body)

    do_GET = do_PUT = do_POST = do_HEAD = do_DELETE = _respond

    def log_message(self, format, *args):
        pass


class _QuietServer(ThreadingHTTPServer):
    def handle_error(self, request, client_address):
        # urllib3 drops connections between retry attempts - the resulting
        # broken-pipe tracebacks in the server thread are expected noise.
        pass


class _ServerHandle:
    def __init__(self, server):
        self._server = server
        host, port = server.server_address
        self.url = f'http://{host}:{port}'

    def set_script(self, script):
        _ScriptedHandler.script = script
        _ScriptedHandler.requests_log = []

    @property
    def requests(self):
        return list(_ScriptedHandler.requests_log)


@pytest.fixture()
def flaky_server():
    server = _QuietServer(('127.0.0.1', 0), _ScriptedHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    handle = _ServerHandle(server)
    try:
        yield handle
    finally:
        server.shutdown()
        server.server_close()


@pytest.fixture()
def zero_backoff(monkeypatch):
    """
    Disable backoff sleeps. MUST patch at CLASS level: urllib3 rebuilds the
    Retry via .new() on every increment, so an instance-level patch is lost
    after the first retry (and the tests would silently sleep 0+2+4s).
    """
    monkeypatch.setattr(Retry, 'get_backoff_time', lambda self: 0.0)


def _make_session(url, max_attempts=3):
    return s3.S3Session('test-id', 'test-key', 'test-bucket', endpoint_url=url, max_attempts=max_attempts)


##############################################
### Tests


def test_retry_config():
    http = http_url.session(max_attempts=3)
    retries = http.connection_pool_kw['retries']
    assert retries.total == 3
    assert set(retries.status_forcelist) == {429, 500, 502, 503, 504, 520, 521, 522, 523, 524}
    assert retries.raise_on_status is False
    assert retries.allowed_methods == Retry.DEFAULT_ALLOWED_METHODS
    assert 'POST' not in retries.allowed_methods
    assert retries.respect_retry_after_header is True


def test_get_cloudflare_52x_retried_until_success(flaky_server, zero_backoff):
    """The Cloudflare-style edge errors B2's fronting infrastructure emits
    (a bare 522 failed an ebooklet CI run un-retried, 2026-07-15)."""
    flaky_server.set_script([(522, {}), (520, {}), (200, {})])
    session = _make_session(flaky_server.url)

    resp = session.get_object('some_key')

    assert resp.status == 200
    assert len(flaky_server.requests) == 3


def test_get_5xx_retried_until_success(flaky_server, zero_backoff):
    flaky_server.set_script([(500, {}), (503, {}), (200, {})])
    session = _make_session(flaky_server.url)

    resp = session.get_object('some_key')

    assert resp.status == 200
    assert len(flaky_server.requests) == 3
    assert all(method == 'GET' for method, _path in flaky_server.requests)


def test_put_5xx_retried_until_success(flaky_server, zero_backoff):
    flaky_server.set_script([(500, {}), (200, {})])
    session = _make_session(flaky_server.url)

    resp = session.put_object('some_key', b'data')

    assert resp.status == 200
    assert len(flaky_server.requests) == 2
    assert all(method == 'PUT' for method, _path in flaky_server.requests)


def test_persistent_5xx_returns_response_not_exception(flaky_server, zero_backoff):
    # The raise_on_status=False contract: exhausted retries hand back the final
    # response so callers (lock PUT check, delete_objects, list iterators) can
    # dispatch on resp.status - a raise here would bypass their error handling.
    flaky_server.set_script([(500, {})])
    session = _make_session(flaky_server.url, max_attempts=2)

    resp = session.get_object('some_key')

    assert resp.status == 500
    assert resp.error
    # total=N means N retries = N+1 requests (documented off-by-one)
    assert len(flaky_server.requests) == 3


def test_post_not_status_retried(flaky_server, zero_backoff):
    # Multi-object-delete is a POST; POST must never be status-retried (B2's
    # native POSTs are non-idempotent, and b2.put_object has its own manual
    # retry loop that must not compound).
    flaky_server.set_script([(500, {})])
    session = _make_session(flaky_server.url)

    with pytest.raises(Exception):
        # delete_objects raises its own HTTPError on a failed chunk - that's
        # its documented contract; what matters here is the request count.
        session.delete_objects(['a', 'b'], purge=False)

    post_requests = [m for m, _p in flaky_server.requests if m == 'POST']
    assert len(post_requests) == 1


def test_429_retry_after_honored(flaky_server):
    # No zero_backoff: Retry-After bypasses get_backoff_time entirely.
    flaky_server.set_script([(429, {'Retry-After': '1'}), (200, {})])
    session = _make_session(flaky_server.url)

    start = time.perf_counter()
    resp = session.get_object('some_key')
    elapsed = time.perf_counter() - start

    assert resp.status == 200
    assert len(flaky_server.requests) == 2
    assert elapsed >= 1.0
