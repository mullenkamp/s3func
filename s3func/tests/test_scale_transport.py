#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Scale-tier live transport regression (deselected by default; run with
`uv run pytest -m scale` at release cadence — NEVER in CI).

T2 from the 2026-07-15 transport review: the deterministic live reproduction
of the send-wall defect, link-speed independent — instead of needing a slow
link and a huge body, it shrinks the wall (connect_timeout=5) and sizes the
body from a calibration PUT so transmission MUST outlive the wall. On any
pre-0.9.4 stack this dies at ~5s with TimeoutError('The write operation
timed out'); on the streamed-body stack it succeeds.

Transfer budget: <= ~250 MB per run. All objects deleted in finally.
"""
import io
import os
import pathlib
import time

import pytest

try:
    import tomllib as toml
except ImportError:
    import tomli as toml

from s3func import S3Session

pytestmark = pytest.mark.scale

script_path = pathlib.Path(os.path.realpath(os.path.dirname(__file__)))

try:
    with io.open(script_path.joinpath('s3_config.toml'), 'rb') as f:
        _cc = toml.load(f)['connection_config']
    endpoint_url = _cc['endpoint_url']
    access_key_id = _cc['aws_access_key_id']
    access_key = _cc['aws_secret_access_key']
    _HAVE_CREDS = True
except Exception:
    _HAVE_CREDS = os.environ.get('aws_secret_access_key') is not None
    if _HAVE_CREDS:
        endpoint_url = os.environ['endpoint_url']
        access_key_id = os.environ['aws_access_key_id']
        access_key = os.environ['aws_secret_access_key']

bucket = 'achelous'
_PREFIX = 'scale-transport-'


@pytest.mark.skipif(not _HAVE_CREDS, reason='no s3 credentials available')
def test_send_wall_live():
    """A single PUT sized to legitimately outlive a 5s send wall must succeed
    (and the elapsed-time assert proves the test really was in the failing
    regime, not on a link so fast the wall was never at risk)."""
    session = S3Session(access_key_id, access_key, bucket, endpoint_url,
                        max_attempts=0, read_timeout=60, connect_timeout=5)
    keys = []
    try:
        ## Calibrate: single-stream throughput varies hour to hour - never
        ## hardcode a rate (transport review, NOTE-6).
        cal = os.urandom(16 * 2**20)
        t0 = time.monotonic()
        resp = session.put_object(_PREFIX + 'cal', cal)
        keys.append(_PREFIX + 'cal')
        assert resp.status == 200
        rate = len(cal) / (time.monotonic() - t0)

        ## Size the body for ~15x the wall at the calibrated rate, bounded.
        size = int(min(max(15 * 5 * rate, 48 * 2**20), 200 * 2**20))
        body = os.urandom(size)
        t0 = time.monotonic()
        resp = session.put_object(_PREFIX + 'wall', body)
        keys.append(_PREFIX + 'wall')
        elapsed = time.monotonic() - t0

        assert resp.status == 200, f'PUT failed: {resp.status}'
        assert elapsed > 5.0, (
            f'{size / 2**20:.0f}MiB completed in {elapsed:.1f}s - the link is too fast '
            'for this wall; the regime proof failed (raise the size bound)'
        )
    finally:
        for key in keys:
            session.delete_object(key)
        leftover = [o['key'] for o in session.list_objects(prefix=_PREFIX).iter_objects()]
        assert not leftover, f'test objects not cleaned up: {leftover}'
