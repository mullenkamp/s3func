# s3func

<p align="center">
    <em>Simple functions for working with S3 and B2</em>
</p>

[![build](https://github.com/mullenkamp/s3func/workflows/Build/badge.svg)](https://github.com/mullenkamp/s3func/actions)
[![codecov](https://codecov.io/gh/mullenkamp/s3func/branch/master/graph/badge.svg)](https://codecov.io/gh/mullenkamp/s3func)
[![PyPI version](https://badge.fury.io/py/s3func.svg)](https://badge.fury.io/py/s3func)

---

`s3func` is a lightweight Python library providing a simplified interface for interacting with S3-compatible object storage services and Backblaze B2. It removes the `boto3` dependency in favor of a fast, `urllib3`-based client with custom SigV4 signing.

## Key Features

- **Zero Boto3 Dependency**: Minimal overhead and faster imports.
- **S3 & B2 Support**: Unified interface for both AWS S3 (and compatible) and Backblaze B2 native API.
- **Distributed Locking**: Robust shared and exclusive locking mechanism using object storage.
- **Streaming Support**: Efficiently stream large objects.
- **Automatic Retries**: Built-in adaptive retry logic for transient network issues.

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

### Backblaze B2 Operations

`s3func` uses the B2 native API for better performance and reliability on Backblaze.

```python
from s3func import B2Session

# Initialize B2 session
session = B2Session(
    access_key_id='YOUR_APPLICATION_KEY_ID',
    access_key='YOUR_APPLICATION_KEY',
    bucket='my-b2-bucket'
)

# Put object
session.put_object('data.json', b'{"status": "ok"}', content_type='application/json')
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
- **B2 Authorization Caching**: `s3func` automatically caches B2 authorization tokens globally for up to 1 hour, significantly reducing latency for concurrent operations.
- **Adaptive Retries**: The library uses `urllib3` retry logic configured for high-concurrency environments to handle rate limiting and network blips automatically.

## How Distributed Locking Works

The locking mechanism uses a "two-object sequential" protocol to ensure safety even in eventually consistent systems:

1.  **Acquisition**: A worker writes two small objects (`seq-0` and `seq-1`) to the storage provider.
2.  **Verification**: It then lists all existing lock objects for that resource. 
3.  **Conflict Resolution**: A worker "wins" the lock if its `seq-1` timestamp is the earliest among all candidates. If timestamps are identical, the lexicographical order of a unique `lock_id` acts as a tie-breaker. 
4.  **Auto-Cleanup**: Uses `weakref.finalize` to ensure lock objects are deleted even if the process exits unexpectedly (best effort).

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
