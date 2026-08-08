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


#################################################
### Version resolution for keys= + purge=True (0.9.6)
###
### Regression context: resolution used to cost ONE listing request PER KEY
### (0.279 s/call live => ~17.6 min for a 3,789-object delete). It now derives
### a common prefix and does ONE bulk listing, filtering to the exact requested
### key set in memory. These tests pin the request COUNT as well as the
### resulting delete set: a correctness regression here is silent version
### retention, which looks identical to success from the caller's side.


class FakeNamespace:
    """An in-memory versioned bucket that answers list/delete over the wire shape.

    Records every request so tests can assert on COUNT, not just outcome, and
    paginates so the resolver's marker handling is exercised.
    """

    def __init__(self, objects, page_size=1000, last_modified='2026-08-08T03:43:24.000Z'):
        ## objects: {key: [(version_id, is_delete_marker), ...]}
        self.objects = objects
        self.page_size = page_size
        self.last_modified = last_modified
        self.gets = []
        self.posts = []
        ## {get_index: exception} - a transient failure needs the RETRY to
        ## succeed, otherwise the fallback raises too and the silent-downgrade
        ## path is never reached.
        self.get_errors = {}
        self.status_on_get = {}

    def _records(self):
        for key in sorted(self.objects):
            for version_id, is_marker in self.objects[key]:
                yield key, version_id, is_marker

    def request(self, method, url, headers=None, fields=None, body=None, preload_content=None):
        fields = dict(fields or {})
        if method == 'GET':
            idx = len(self.gets)
            self.gets.append(fields)
            if idx in self.get_errors:
                raise self.get_errors[idx]
            if idx in self.status_on_get:
                return SimpleNamespace(status=self.status_on_get[idx], data=b'')
            if 'versions' in fields:
                return SimpleNamespace(status=200, data=self._versions_page(fields))
            return SimpleNamespace(status=200, data=self._objects_page(fields))
        self.posts.append(body)
        return SimpleNamespace(status=200, data=BODY_ALL_SUCCESS)

    def _select(self, fields):
        prefix = fields.get('prefix', '')
        marker = fields.get('key-marker') or fields.get('continuation-token') or ''
        return [r for r in self._records() if r[0].startswith(prefix) and r[0] > marker]

    def _versions_page(self, fields):
        rows = self._select(fields)
        page, rest = rows[: self.page_size], rows[self.page_size :]
        ## S3 groups the page's Versions ahead of its DeleteMarkers; the resolver
        ## must not assume key order across yields because of it.
        parts = []
        for key, version_id, is_marker in page:
            if is_marker:
                continue
            parts.append(
                f'<Version><Key>{key}</Key><VersionId>{version_id}</VersionId>'
                f'<IsLatest>false</IsLatest><Size>1</Size>'
                f'<LastModified>{self.last_modified}</LastModified></Version>'
            )
        for key, version_id, is_marker in page:
            if not is_marker:
                continue
            parts.append(
                f'<DeleteMarker><Key>{key}</Key><VersionId>{version_id}</VersionId>'
                f'<IsLatest>false</IsLatest>'
                f'<LastModified>{self.last_modified}</LastModified></DeleteMarker>'
            )
        trunc = 'true' if rest else 'false'
        tail = ''
        if rest:
            tail = f'<NextKeyMarker>{page[-1][0]}</NextKeyMarker><NextVersionIdMarker>{page[-1][1]}</NextVersionIdMarker>'
        return (
            f'<ListVersionsResult xmlns="{NS}">{"".join(parts)}'
            f'<IsTruncated>{trunc}</IsTruncated>{tail}</ListVersionsResult>'
        ).encode()

    def _objects_page(self, fields):
        rows = [r for r in self._select(fields) if not r[2]]
        parts = ''.join(
            f'<Contents><Key>{key}</Key><Size>1</Size>'
            f'<LastModified>{self.last_modified}</LastModified></Contents>'
            for key, _v, _m in rows
        )
        return (
            f'<ListBucketResult xmlns="{NS}">{parts}'
            f'<IsTruncated>false</IsTruncated></ListBucketResult>'
        ).encode()


def deleted_entries(body):
    """Parse a multi-delete POST body into a {(key, version_id)} set."""
    import xml.etree.ElementTree as ET

    root = ET.fromstring(body)
    out = set()
    for obj in root.findall(f'{{{NS}}}Object') or root.findall('Object'):
        key_el = obj.find(f'{{{NS}}}Key')
        ver_el = obj.find(f'{{{NS}}}VersionId')
        out.add((key_el.text, ver_el.text if ver_el is not None else None))
    return out


def bind(fake):
    session = make_session()
    session.request = fake.request
    return session


def two_versions(keys):
    return {k: [(f'{k}-v1', False), (f'{k}-v2', False)] for k in keys}


### --- the headline: one bulk listing, exact subset ------------------------------


def test_purge_resolution_uses_one_bulk_listing():
    """N keys must cost ONE listing, not N. Fails on 0.9.5 (5 GETs)."""
    wanted = ['db/a', 'db/b', 'db/c', 'db/d', 'db/e']
    fake = FakeNamespace({**two_versions(wanted), 'db/zz': [('zz-v1', False)]})

    bind(fake).delete_objects(keys=list(wanted), purge=True)

    assert len(fake.gets) == 1, f'expected one bulk listing, got {len(fake.gets)}'
    assert len(fake.posts) == 1
    assert fake.gets[0].get('prefix') == 'db/'


def test_unrequested_keys_under_the_prefix_are_never_deleted():
    """The invariant that forbids a plain prefix= delete: the bulk listing sees
    db/zz, and the resolver must drop it."""
    wanted = ['db/a', 'db/b']
    fake = FakeNamespace({**two_versions(wanted), 'db/zz': [('zz-v1', False)]})

    bind(fake).delete_objects(keys=list(wanted), purge=True)

    assert {k for k, _v in deleted_entries(fake.posts[0])} == set(wanted)


def test_strict_prefix_sibling_is_spared():
    """ebooklet group ids are non-zero-padded decimals: deleting '1' must not
    touch '10'. This is the hazard that rules out a prefix= delete."""
    fake = FakeNamespace({
        'db/1': [('1-v1', False), ('1-v2', False)],
        'db/10': [('10-v1', False)],
        'db/12': [('12-v1', False)],
    })

    bind(fake).delete_objects(keys=['db/1', 'db/12'], purge=True)

    assert {k for k, _v in deleted_entries(fake.posts[0])} == {'db/1', 'db/12'}


def test_all_versions_and_delete_markers_are_purged():
    fake = FakeNamespace({
        'db/a': [('a-v1', False), ('a-v2', False), ('a-v3', False)],
        'db/b': [('b-v9', True), ('b-v8', False)],
    })

    bind(fake).delete_objects(keys=['db/a', 'db/b'], purge=True)

    assert deleted_entries(fake.posts[0]) == {
        ('db/a', 'a-v1'), ('db/a', 'a-v2'), ('db/a', 'a-v3'),
        ('db/b', 'b-v9'), ('db/b', 'b-v8'),
    }


def test_versions_spanning_a_truncated_page_are_all_collected():
    """The classic pagination-drops-versions bug. delete_objects cannot inject
    max-keys, so this can only be exercised at the stub tier."""
    fake = FakeNamespace(two_versions(['db/a', 'db/b', 'db/c']), page_size=2)

    bind(fake).delete_objects(keys=['db/a', 'db/b', 'db/c'], purge=True)

    assert len(fake.gets) == 3, 'expected 3 pages of 2 records'
    assert len(deleted_entries(fake.posts[0])) == 6


### --- the guard -----------------------------------------------------------------


def test_guard_aborts_to_per_key_and_keeps_the_same_delete_set():
    """A common prefix that over-lists must abort back to per-key, discard the
    partial, and still produce the exact right delete set."""
    noise = {f'db/noise{i:04d}': [(f'n{i}', False)] for i in range(60)}
    ## 'db/a' has versions on BOTH sides of the abort point: if a partial result
    ## were reused instead of discarded, one of them would be lost.
    objects = {**noise, 'db/a': [('a-v1', False)], 'db/zzz': [('z-v1', False)]}
    fake = FakeNamespace(objects, page_size=10)

    bind(fake).delete_objects(keys=['db/a', 'db/zzz'], purge=True)

    assert deleted_entries(fake.posts[0]) == {('db/a', 'a-v1'), ('db/zzz', 'z-v1')}
    ## bulk aborts once pages_fetched exceeds n_unresolved (2), then 2 per-key
    assert len(fake.gets) == 5, f'expected 3 bulk pages + 2 per-key, got {len(fake.gets)}'


def test_no_common_prefix_never_lists_the_whole_bucket():
    """commonprefix('') is dropped by build_s3_params, so a bulk attempt would
    scan the bucket from the root. Every listing must carry a prefix."""
    fake = FakeNamespace({'aaa': [('a1', False)], 'zzz': [('z1', False)]})

    bind(fake).delete_objects(keys=['aaa', 'zzz'], purge=True)

    assert all(f.get('prefix') for f in fake.gets), f'un-prefixed listing: {fake.gets}'


### --- failures must be loud -----------------------------------------------------


def test_transient_transport_error_with_501_in_the_url_raises():
    """A dead-transport error whose str() embeds a URL containing '501' must not
    be mistaken for a provider capability signal. Fails on 0.9.5, which falls
    back to the unversioned listing and silently drops the version ids."""
    fake = FakeNamespace({'db/501.abc': [('v1', False), ('v2', False)],
                          'db/501.def': [('v3', False)]})
    pool = urllib3.HTTPSConnectionPool('s3.example.com')
    fake.get_errors[0] = urllib3.exceptions.MaxRetryError(
        pool, 'https://s3.example.com/bucket?prefix=db%2F501.abc&versions=', reason=Exception('boom')
    )

    with pytest.raises(urllib3.exceptions.HTTPError):
        bind(fake).delete_objects(keys=['db/501.abc', 'db/501.def'], purge=True)

    assert not fake.posts, 'no delete may be issued when resolution failed'


def test_genuine_501_falls_back_to_the_unversioned_listing():
    """A real capability 501 keeps the documented fallback: versionless deletes,
    which on an unversioned provider are complete deletes."""
    fake = FakeNamespace(two_versions(['db/a', 'db/b']))
    fake.status_on_get[0] = 501

    bind(fake).delete_objects(keys=['db/a', 'db/b'], purge=True)

    assert deleted_entries(fake.posts[0]) == {('db/a', None), ('db/b', None)}


def test_listing_server_error_raises_and_issues_no_delete():
    """Fails on 0.9.5, which degrades to a versionless delete and returns None."""
    fake = FakeNamespace(two_versions(['db/a', 'db/b']))
    fake.status_on_get[0] = 500

    with pytest.raises(urllib3.exceptions.HTTPError):
        bind(fake).delete_objects(keys=['db/a', 'db/b'], purge=True)

    assert not fake.posts


@pytest.mark.parametrize(
    'stamp', ['2026-08-08T03:43:24.000Z', '2026-08-08T03:43:24Z', '2026-08-08T03:43:24+00:00']
)
def test_last_modified_variants_are_parsed_not_fatal(stamp):
    """Providers differ on fractional seconds and on 'Z' vs a numeric offset.
    0.9.5 hard-coded the fractional form, so the other two raised ValueError -
    which the bare `except Exception` then swallowed on the keys= path, SILENTLY
    DOWNGRADING the purge. Parse all three; the delete must be unaffected."""
    fake = FakeNamespace(two_versions(['db/a', 'db/b']), last_modified=stamp)

    bind(fake).delete_objects(keys=['db/a', 'db/b'], purge=True)

    assert len(deleted_entries(fake.posts[0])) == 4


def test_unparsable_last_modified_surfaces_as_httperror():
    """A timestamp we genuinely cannot read must still fail as an HTTPError
    subclass, never as a bare ValueError: ebooklet's phase-D GC catches HTTPError
    and nothing else, and it runs AFTER the commit and AFTER the journal is
    cleared - a ValueError there crashes a push that already succeeded."""
    fake = FakeNamespace(two_versions(['db/a', 'db/b']), last_modified='last tuesday')

    with pytest.raises(urllib3.exceptions.HTTPError):
        bind(fake).delete_objects(keys=['db/a', 'db/b'], purge=True)


### --- missing keys, degenerate input --------------------------------------------


def test_missing_key_emits_no_versionless_delete():
    """0.9.5 emits a bare <Object><Key> for an unresolvable key, which on a
    versioned bucket creates a delete marker for an object that never existed."""
    fake = FakeNamespace(two_versions(['db/a']))

    bind(fake).delete_objects(keys=['db/a', 'db/ghost'], purge=True)

    assert deleted_entries(fake.posts[0]) == {('db/a', 'db/a-v1'), ('db/a', 'db/a-v2')}


def test_all_keys_missing_issues_no_delete_at_all():
    fake = FakeNamespace({'db/other': [('o1', False)]})

    assert bind(fake).delete_objects(keys=['db/x', 'db/y'], purge=True) is None
    assert not fake.posts


def test_empty_key_list_makes_no_requests():
    fake = FakeNamespace(two_versions(['db/a']))

    assert bind(fake).delete_objects(keys=[], purge=True) is None
    assert not fake.gets and not fake.posts


def test_single_key_does_exactly_one_listing():
    fake = FakeNamespace(two_versions(['db/a']))

    bind(fake).delete_objects(keys=['db/a'], purge=True)

    assert len(fake.gets) == 1 and len(fake.posts) == 1


def test_explicit_version_ids_are_not_expanded():
    """A key supplied with a version_id must be deleted at THAT version only,
    and must not widen the resolution scope for the bare keys alongside it."""
    fake = FakeNamespace({
        'db/a': [('a-v1', False), ('a-v2', False)],
        'db/b': [('b-v1', False)],
    })

    bind(fake).delete_objects(keys=[{'key': 'db/a', 'version_id': 'a-v1'}, 'db/b'], purge=True)

    assert deleted_entries(fake.posts[0]) == {('db/a', 'a-v1'), ('db/b', 'b-v1')}
    assert fake.gets[0].get('prefix') == 'db/b', 'resolution scope must derive from unresolved keys only'


def test_duplicate_delete_entries_are_deduped():
    """A bare key mixed with the same key at an explicit version must not emit
    that version twice - a second delete of one version can return NoSuchVersion."""
    fake = FakeNamespace({'db/a': [('a-v1', False)]})

    bind(fake).delete_objects(keys=['db/a', {'key': 'db/a', 'version_id': 'a-v1'}], purge=True)

    body = fake.posts[0]
    assert body.count(b'<Object>') == 1, f'duplicate entries emitted: {body!r}'


def test_malformed_key_is_rejected_without_listing_the_bucket():
    """A None key makes commonprefix raise TypeError. On 0.9.5 the same input
    costs a whole-bucket listing per bad key before failing at the XML build."""
    fake = FakeNamespace(two_versions(['db/a']))

    with pytest.raises((ValueError, TypeError)):
        bind(fake).delete_objects(keys=['db/a', None], purge=True)

    assert not fake.gets, 'no request may be issued for a malformed key list'
    assert not fake.posts
