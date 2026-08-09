# s3func

<p align="center">
    <em>Simple functions for working with S3-compatible object storage</em>
</p>

[![build](https://github.com/mullenkamp/s3func/workflows/Build/badge.svg)](https://github.com/mullenkamp/s3func/actions)
[![codecov](https://codecov.io/gh/mullenkamp/s3func/branch/master/graph/badge.svg)](https://codecov.io/gh/mullenkamp/s3func)
[![PyPI version](https://badge.fury.io/py/s3func.svg)](https://badge.fury.io/py/s3func)

---

`s3func` is a lightweight Python library providing a simplified interface for interacting with S3-compatible object storage services (AWS S3, Backblaze B2, MEGA S4, and others). It removes the `boto3` dependency in favor of a fast, `urllib3`-based client with custom SigV4 signing.

## Key Features

- **Zero Boto3 Dependency**: Minimal overhead and faster imports.
- **Provider-Agnostic**: One `S3Session` for AWS S3 and any S3-compatible provider (Backblaze B2, MEGA S4, ...), with provider quirks handled portably (strict RFC3986 signing of paths and queries).
- **Distributed Locking**: Verified shared/exclusive locking on plain object storage - no CAS required (see [docs/locking.md](docs/locking.md)).
- **Streaming Support**: Efficiently stream large objects.
- **Automatic Retries**: urllib3 retries with exponential backoff for connection errors and transient HTTP statuses (429/5xx); `Retry-After` honored; non-idempotent POSTs are never status-retried.
- **Slow-link-safe transfers** (0.9.4): timeouts are true *idle* bounds, never total-request deadlines — large uploads are streamed in chunks (bytes bodies over 1 MiB are wrapped automatically), so a slow-but-progressing multi-minute transfer never times out while a genuinely stalled socket still dies within `read_timeout`/`connect_timeout`.

## Installation

```bash
pip install s3func
```

## Usage Examples

### S3 Operations

```python
from s3func import S3Session

# Initialize session for AWS S3
session = S3Session(
    access_key_id='YOUR_ACCESS_KEY',
    access_key='YOUR_SECRET_KEY',
    bucket='my-bucket',
    region='us-east-1'
)

# Also works with other S3-compatible providers (Contabo, Wasabi, DigitalOcean, etc.)
# by providing an endpoint_url.
session = S3Session(
    access_key_id='YOUR_ACCESS_KEY',
    access_key='YOUR_SECRET_KEY',
    bucket='my-bucket',
    endpoint_url='https://eu2.contabostorage.com'
)

# Upload an object
session.put_object('hello.txt', b'Hello, S3!')

# Download an object
resp = session.get_object('hello.txt')
print(resp.data.decode())

# List objects
for obj in session.list_objects(prefix='logs/').iter_objects():
    print(obj['key'], obj['content_length'])
```

### Custom Metadata

You can easily read and write custom metadata headers.

```python
# Upload with metadata
session.put_object(
    'data.csv', 
    b'col1,col2\n1,2', 
    metadata={'processed': 'false', 'source': 'sensor-1'}
)

# Read metadata
resp = session.head_object('data.csv')
print(resp.metadata['processed']) # 'false'
```

### Deleting Objects

`delete_objects` removes many objects in one batched request. By default it *purges*: every
version of each object is removed, not just the current one.

```python
# Delete specific objects. Keys are matched EXACTLY, never as prefixes -
# deleting 'group/1' does not touch 'group/10'.
session.delete_objects(keys=['old/a.bin', 'old/b.bin'])

# Delete everything under a prefix
session.delete_objects(prefix='old/')

# Keep the version history: on a versioned bucket this only adds a delete
# marker, and every previous version is retained (and still billed for).
session.delete_objects(keys=['old/a.bin'], purge=False)

# Already know the version ids? Pass dicts and no lookup is done at all.
session.delete_objects(keys=[{'key': 'old/a.bin', 'version_id': '...'}])

# A single object, optionally one specific version
session.delete_object('old/a.bin')
session.delete_object('old/a.bin', version_id='...')
```

`keys` and `prefix` are mutually exclusive, and exactly one is required. Under `purge=True`
the version ids for bare string keys are resolved with a *single* bulk listing rather than one
request per key.

Deletion is the one place where failures are raised rather than returned: every chunk is
attempted first, so a partial failure still deletes everything it can, and the resulting
`urllib3.exceptions.HTTPError` names the keys that failed. A purge that cannot read the
versions it is meant to remove also raises — it will not quietly fall back to a versionless
delete, which would report success while leaving the whole version history in place.

### Distributed Locking

`s3func` provides a powerful distributed lock that mimics Python's `threading.Lock` API.

```python
# Using S3 Lock via context manager
with session.lock('process-1'):
    # This block is protected by a distributed lock
    print("Doing some exclusive work...")

# Explicit acquire/release with timeout
lock = session.lock('my-resource')
if lock.acquire(blocking=True, timeout=10):
    try:
        # Perform operation
        pass
    finally:
        lock.release()
```

## Errors and Exceptions

`s3func` splits failures into two deliberate categories, and the split follows the shape of the
operation rather than the kind of error.

**Single-object methods return a response; they do not raise on HTTP status.** `get_object`,
`put_object`, `head_object`, `copy_object`, `delete_object` and the object-lock config methods
all hand back a response whose `.status` and `.error` you inspect. A 404 or a 403 is a value,
not an exception.

```python
resp = session.get_object('maybe-missing.bin')
if resp.status // 100 != 2:
    print('failed:', resp.status, resp.error)
```

**Aggregate operations raise, because there is no single response to hand back.** Listings and
multi-object deletes span many requests, so a failure part-way through cannot be reported as a
status code:

| operation | raises |
|---|---|
| `list_objects(...).iter_objects()`, `list_object_versions(...).iter_objects()` | `S3ListError` — a non-2xx page, an unparsable body or field, or a truncated listing the server gave no way to continue |
| `delete_objects(...)` | `HTTPError` naming the keys that failed; every chunk is attempted first. Also `S3ListError` if a purge cannot resolve versions |
| lock acquire/release (`session.lock(...)`) | `HTTPError` on a failed ticket write, `ValueError` if a foreign object is found in the lock namespace |

**Transport failures raise from any call**, single-object ones included, once retries are
exhausted — `MaxRetryError`, `ProtocolError`, timeouts and so on.

The unifying rule: **every failure that reaches you from a request is a subclass of
`urllib3.exceptions.HTTPError`**, including `S3ListError` and all urllib3 transport errors. One
`except` clause catches the lot:

```python
import urllib3

try:
    for obj in session.list_object_versions(prefix='data/').iter_objects():
        ...
    session.delete_objects(keys=doomed)
except urllib3.exceptions.HTTPError as err:
    ...  # transport failure, bad listing, or a partly-failed delete
```

`S3ListError` carries the numeric HTTP `.status` (`None` when the failure was in parsing), so
branch on that rather than on the text of a message — urllib3's transport errors embed the
request URL in their `str()`, which makes substring tests match keys and prefixes by accident.

```python
from s3func import S3ListError

try:
    records = list(session.list_object_versions(prefix=key).iter_objects())
except S3ListError as err:
    if err.status == 501:
        ...  # this provider has no versioning support (MEGA S4 answers 501 here)
    raise
```

**Invalid arguments raise before anything is sent** — `ValueError` or `TypeError` for
non-string or oversized metadata, for passing both `keys` and `prefix` to `delete_objects`, for
a malformed key entry, or for a bad endpoint URL.

## Performance Tips

- **Streaming**: Set `stream=True` in the session (default) or individual requests to handle large files without loading them entirely into memory.
- **Retries**: All sessions share one `urllib3` Retry policy (`max_attempts` retries, exponential backoff, `Retry-After` honored): connection errors and transient statuses — 429, 500, 502, 503, 504, plus the Cloudflare-style edge errors 520-524 that B2's fronting infrastructure emits — are retried on idempotent methods. POST requests (S3 multi-object-delete, B2-native uploads) are never status-retried. When *status* retries are exhausted the final response is **returned**, not raised, so callers can inspect `resp.status`/`resp.error`; a *connection* failure that exhausts its retries **does** raise. See [Errors and Exceptions](#errors-and-exceptions).

Changes between releases are tracked in [CHANGELOG.md](CHANGELOG.md).

## How Distributed Locking Works

Full walk-through with diagrams: [docs/locking.md](docs/locking.md).

The lock is a Lamport-bakery-style election over plain object storage (no
compare-and-swap needed):

1.  **Acquisition**: A worker writes two small ticket objects (`seq-0` and `seq-1`).
2.  **Self-visibility gate (0.9.0)**: it polls the listing until its OWN ticket is
    visible - a listing that cannot show your own writes cannot be trusted to show
    competitors (raises after `visibility_timeout`, default 30s).
3.  **Election**: it lists all tickets and yields to older ones (`seq-1` timestamp;
    lexicographic `lock_id` breaks ties). Shared tickets yield only to older
    exclusive tickets.
4.  **Confirming re-list (0.9.0)**: winning requires a second clear listing taken
    `settle_delay` (default 1.0s) later - a violation now needs two *independent*
    stale listings.
5.  **Own-ticket invariant (0.9.0)**: every decisive listing must still contain the
    worker's own ticket; if another client deleted it (e.g. `break_other_locks`),
    acquisition raises instead of "winning" without a ticket. Recovering a ticket
    via `lock_id=` restores the ticket only - `acquire()` re-runs the election.
6.  **Holder re-verification (0.9.3)**: `lock.verify()` re-checks that a holder
    still holds the lock (a fresh listing must show both of its ticket objects) -
    call it at critical boundaries so a broken holder aborts instead of writing
    without mutual exclusion. `break_other_locks()` is age-gated by default
    (only tickets older than 2 hours are broken; the caller's own never are).
7.  **Auto-Cleanup**: `weakref.finalize` deletes ticket objects even if the process
    exits unexpectedly (best effort).

**Guarantee and residual window**: on storage with strongly consistent listings the
election is safe. On eventually-consistent listings the hardening reduces the
failure mode to *two consecutive independently-stale listings* (measured on B2:
80/80 listings were first-poll consistent - see `benchmarks/results_visibility_lag.md`).
No provider we tested currently offers atomic conditional writes
(`benchmarks/conditional_write_probe.py` is the qualification gate for adding a
true CAS lock per provider; MEGA S4 accepts the headers but is not atomic under
concurrency). Tune via `session.lock(key, settle_delay=..., visibility_timeout=...)`.

## Development

### Setup environment

We use [uv](https://docs.astral.sh/uv/) to manage the development environment and production build. 

```bash
uv sync --all-extras --dev
```

### Running Tests

```bash
uv run pytest
```

## License

This project is licensed under the terms of the Apache Software License 2.0.
