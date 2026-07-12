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

## Performance Tips

- **Streaming**: Set `stream=True` in the session (default) or individual requests to handle large files without loading them entirely into memory.
- **Retries**: All sessions share one `urllib3` Retry policy (`max_attempts` retries, exponential backoff): connection errors and transient statuses (429, 500, 502, 503, 504) are retried on idempotent methods; when retries exhaust, the final response is returned (never an exception) so callers can inspect `resp.status`/`resp.error`. POST requests (S3 multi-object-delete, B2-native uploads) are never status-retried.

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
