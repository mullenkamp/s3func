#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Tripwire for the 0.9.4 large-upload streaming fix: a slow-but-PROGRESSING
upload whose transmission time exceeds the socket timeout must succeed.

Pre-0.9.4, put_object handed urllib3 a monolithic `bytes` body, which is
transmitted as ONE sendall() whose deadline is fixed at entry and never
extended by progress - the upload died at exactly the timeout with
TimeoutError('The write operation timed out') on a healthy connection.
Streaming the body (BytesIO -> 16KiB chunks) makes the timeout per-chunk.

Runs against a local throttled sink - no credentials, no network, no TLS
(the deadline mechanism is not TLS-specific).
"""
import io
import socket
import threading
import time

from s3func import S3Session, utils

##############################################
### Throttled sink

_DRAIN_CHUNK = 65536
_DRAIN_SLEEP = 0.01          # ~6.4 MB/s drain rate


class _ThrottledSink:
    """Accepts ONE HTTP request, drains its body at a throttled rate through a
    small receive buffer, then replies 200. The small RCVBUF caps the TCP
    window, so the client cannot dump the whole body into kernel buffers -
    its send path genuinely has to wait on the drain, which is what pushes a
    monolithic-bytes sendall past the send wall."""

    def __init__(self):
        self._srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._srv.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 65536)
        self._srv.bind(('127.0.0.1', 0))
        self._srv.listen(1)
        self.port = self._srv.getsockname()[1]
        self.bytes_received = 0
        self._thread = threading.Thread(target=self._serve, daemon=True)
        self._thread.start()

    def _serve(self):
        conn, _ = self._srv.accept()
        conn.settimeout(60)
        try:
            buf = b''
            while b'\r\n\r\n' not in buf:
                data = conn.recv(4096)
                if not data:
                    return
                buf += data
            head, _, rest = buf.partition(b'\r\n\r\n')
            content_length = 0
            for line in head.split(b'\r\n'):
                if line.lower().startswith(b'content-length:'):
                    content_length = int(line.split(b':', 1)[1])
            self.bytes_received = len(rest)
            remaining = content_length - len(rest)
            while remaining > 0:
                chunk = conn.recv(min(_DRAIN_CHUNK, remaining))
                if not chunk:
                    return
                remaining -= len(chunk)
                self.bytes_received += len(chunk)
                time.sleep(_DRAIN_SLEEP)
            conn.sendall(b'HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n')
        finally:
            conn.close()
            self._srv.close()


##############################################
### Tests


def test_large_upload_survives_send_wall():
    """The core invariant: send progress must extend the upload's life.
    The body is sized past what the kernel can locally queue (tcp_wmem max),
    so the sender must genuinely outlive the 2s connect/send wall - the
    pre-0.9.4 monolithic path dies at ~2.0s here."""
    wmem_max = 4 * 2**20
    try:
        with open('/proc/sys/net/ipv4/tcp_wmem') as f:
            wmem_max = int(f.read().split()[2])
    except OSError:
        pass
    body = b'x' * (wmem_max + 24 * 2**20)

    sink = _ThrottledSink()
    session = S3Session('test-key-id', 'test-secret', 'testbucket',
                        endpoint_url=f'http://127.0.0.1:{sink.port}',
                        max_attempts=0, read_timeout=60, connect_timeout=2)
    t0 = time.monotonic()
    resp = session.put_object('tripwire-key', body)
    elapsed = time.monotonic() - t0

    assert resp.status == 200
    assert sink.bytes_received == len(body), 'the sink did not receive the full body'
    ## Regime proof: the send outlived the wall, i.e. this test really was in
    ## the territory where the monolithic path times out.
    assert elapsed > 2.0


def test_bytes_wrap_threshold():
    """Bodies over utils.stream_body_threshold reach the transport as a
    seekable stream with the SigV4 payload hash precomputed from the bytes;
    small bodies stay bytes (byte-identical legacy path)."""
    captured = {}

    class _CaptureSession(S3Session):
        def request(self, method, url, headers=None, fields=None, body=None, preload_content=None):
            captured['body'] = body
            captured['headers'] = dict(headers or {})

            class _Resp:
                status = 200
                headers = {}
                data = b''

            return _Resp()

    s = _CaptureSession('k', 's', 'b', endpoint_url='http://127.0.0.1:1/')

    s.put_object('small', b'tiny')
    assert isinstance(captured['body'], bytes)
    assert 'x-amz-content-sha256' not in captured['headers']

    big = b'y' * (utils.stream_body_threshold + 1)
    s.put_object('big', big)
    assert isinstance(captured['body'], io.BytesIO)
    assert captured['body'].read() == big
    assert 'x-amz-content-sha256' in captured['headers']
    assert captured['headers']['Content-Length'] == str(len(big))
