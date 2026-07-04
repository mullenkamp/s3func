"""
Unit tests (no network) for the multi-object-delete response checking (0.9.1).

Regression context: delete_objects used to fire the batched POST and discard the
response entirely - urllib3 does not raise on HTTP status, and Quiet=true mode
still returns per-key <Error> elements, so failed deletes reported success.
"""
from types import SimpleNamespace

import pytest
import urllib3

from s3func import S3Session
from s3func.response import parse_delete_errors


#################################################
### parse_delete_errors (pure)

NS = 'http://s3.amazonaws.com/doc/2006-03-01/'

BODY_NAMESPACED_TWO_ERRORS = f'''<?xml version="1.0" encoding="UTF-8"?>
<DeleteResult xmlns="{NS}">
  <Error><Key>k1</Key><Code>AccessDenied</Code><Message>denied</Message></Error>
  <Error><Key>k2</Key><Code>InternalError</Code><Message>oops</Message></Error>
</DeleteResult>'''.encode()

BODY_PLAIN_ONE_ERROR = b'''<DeleteResult>
  <Deleted><Key>k0</Key></Deleted>
  <Error><Key>k1</Key><Code>NoSuchVersion</Code><Message>gone</Message></Error>
</DeleteResult>'''

BODY_ALL_SUCCESS = f'<DeleteResult xmlns="{NS}"></DeleteResult>'.encode()


def test_parse_namespaced_errors():
    errors = parse_delete_errors(BODY_NAMESPACED_TWO_ERRORS)
    assert errors == [
        {'key': 'k1', 'code': 'AccessDenied', 'message': 'denied'},
        {'key': 'k2', 'code': 'InternalError', 'message': 'oops'},
    ]


def test_parse_plain_error_ignores_deleted_entries():
    errors = parse_delete_errors(BODY_PLAIN_ONE_ERROR)
    assert errors == [{'key': 'k1', 'code': 'NoSuchVersion', 'message': 'gone'}]


def test_parse_success_bodies():
    assert parse_delete_errors(BODY_ALL_SUCCESS) == []
    assert parse_delete_errors(b'') == []       # some providers send no body in quiet mode
    assert parse_delete_errors(None) == []
    assert parse_delete_errors(b'  \n') == []


def test_parse_malformed_raises():
    with pytest.raises(urllib3.exceptions.HTTPError):
        parse_delete_errors(b'<DeleteResult><unclosed')


#################################################
### delete_objects failure paths (stubbed request)


def make_session():
    return S3Session('fake-id', 'fake-key', 'fake-bucket', endpoint_url='https://s3.example.com')


def test_delete_objects_all_success(monkeypatch):
    session = make_session()
    calls = []

    def fake_request(method, url, headers=None, fields=None, body=None, preload_content=None):
        calls.append(body)
        return SimpleNamespace(status=200, data=BODY_ALL_SUCCESS)

    monkeypatch.setattr(session, 'request', fake_request)

    assert session.delete_objects(['a', 'b'], purge=False) is None
    assert len(calls) == 1


def test_delete_objects_per_key_errors_raise(monkeypatch):
    session = make_session()

    def fake_request(method, url, headers=None, fields=None, body=None, preload_content=None):
        return SimpleNamespace(status=200, data=BODY_NAMESPACED_TWO_ERRORS)

    monkeypatch.setattr(session, 'request', fake_request)

    with pytest.raises(urllib3.exceptions.HTTPError, match=r'2 key\(s\).*k1.*AccessDenied.*k2') as exc_info:
        session.delete_objects(['k0', 'k1', 'k2'], purge=False)
    assert 'k0' not in str(exc_info.value)  # only FAILED keys are listed


def test_delete_objects_http_failure_attempts_all_chunks(monkeypatch):
    """A failed chunk must not stop later chunks (best-effort), but must raise at the end."""
    session = make_session()
    keys = [f'key{i:04d}' for i in range(1001)]  # 2 chunks: 1000 + 1
    responses = iter([
        SimpleNamespace(status=500, data=b'throttled'),
        SimpleNamespace(status=200, data=BODY_ALL_SUCCESS),
    ])
    calls = []

    def fake_request(method, url, headers=None, fields=None, body=None, preload_content=None):
        calls.append(body)
        return next(responses)

    monkeypatch.setattr(session, 'request', fake_request)

    with pytest.raises(urllib3.exceptions.HTTPError, match=r'1000 key\(s\)'):
        session.delete_objects(keys, purge=False)
    assert len(calls) == 2  # the second chunk was still attempted


def test_delete_objects_unparseable_200_marks_chunk_failed_and_continues(monkeypatch):
    """A 200 with an unparseable body means the chunk's outcome is unknown - it must
    be recorded as failed WITHOUT aborting the remaining chunks."""
    session = make_session()
    keys = [f'key{i:04d}' for i in range(1001)]
    responses = iter([
        SimpleNamespace(status=200, data=b'not xml at all'),
        SimpleNamespace(status=200, data=BODY_ALL_SUCCESS),
    ])
    calls = []

    def fake_request(method, url, headers=None, fields=None, body=None, preload_content=None):
        calls.append(body)
        return next(responses)

    monkeypatch.setattr(session, 'request', fake_request)

    with pytest.raises(urllib3.exceptions.HTTPError, match=r'1000 key\(s\).*unparseable-response'):
        session.delete_objects(keys, purge=False)
    assert len(calls) == 2
