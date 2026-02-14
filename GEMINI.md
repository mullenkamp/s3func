# s3func

`s3func` is a Python library providing a simplified interface for interacting with S3-compatible object storage services (like AWS S3 and Backblaze B2). It wraps `boto3` to offer a more intuitive `S3Session` class and implements a distributed locking mechanism via `S3Lock`.

## Project Structure

*   `s3func/`: Source code directory.
    *   `s3.py`: Core S3 functionality (`S3Session`) and locking (`S3Lock`).
    *   `b2.py`: Backblaze B2 specific implementations.
    *   `http_url.py`: Utilities for handling HTTP URLs.
    *   `utils.py`: Helper functions and classes (e.g., `S3Response`).
    *   `tests/`: Test suite (using `pytest`).
*   `pyproject.toml`: Project configuration, dependencies, and build settings (Hatchling).
*   `uv.lock`: Dependency lock file managed by `uv`.

## Environment Setup

The project uses [uv](https://docs.astral.sh/uv/) for dependency management.

### Using uv (Recommended)

1.  **Install uv:**
    ```bash
    curl -LsSf https://astral.sh/uv/install.sh | sh
    ```

2.  **Sync dependencies:**
    ```bash
    uv sync --all-extras --dev
    ```

### Using pip

If you prefer standard `pip`:

1.  **Create a virtual environment:**
    ```bash
    python -m venv .venv
    source .venv/bin/activate
    ```

2.  **Install dependencies:**
    ```bash
    pip install .[dev]
    ```

## Development

### Running Tests

Tests are written using `pytest`.

```bash
# Run all tests
uv run pytest

# Or with pip/venv active
pytest
```

**Note:** Tests may require AWS credentials (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`) and an `ENDPOINT_URL` to be set in the environment if running integration tests against a real S3 endpoint.

## Key Components

### S3Session

Located in `s3func/s3.py`, `S3Session` is the main entry point. It simplifies common operations:

*   `get_object(key, ...)`
*   `put_object(key, obj, ...)`
*   `list_objects(prefix, ...)`
*   `copy_object(source_key, dest_key, ...)`

### S3Lock

Also in `s3func/s3.py`, `S3Lock` implements a distributed lock using S3 objects. It mimics the Python `threading.Lock` API:

*   `acquire(blocking=True, ...)`
*   `release()`
*   Context manager support: `with session.s3lock('my-lock-key'): ...`

## Build

The project uses `hatchling` as the build backend.

```bash
# Build the package
uv build
```