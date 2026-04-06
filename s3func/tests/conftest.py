"""
Shared fixtures for s3func tests.

Provider configs are loaded from s3_config.toml (local dev) and/or
prefixed environment variables (CI). Each S3-compatible provider gets
its own parameterized test run.

To add a new provider:
1. Add a [providers.<name>] section to s3_config.toml, OR
2. Set env vars: <name>_endpoint_url, <name>_access_key_id, <name>_access_key, <name>_bucket
"""
import os
import pathlib
import uuid

import pytest

try:
    import tomllib as toml
except ImportError:
    import tomli as toml

from s3func import s3

script_path = pathlib.Path(os.path.realpath(os.path.dirname(__file__)))

# Known provider prefixes to check in environment variables
_ENV_PREFIXES = ['b2', 'mega']


def _load_providers():
    """Load S3-compatible provider configs from toml and/or environment variables."""
    providers = {}

    # Load from toml
    toml_path = script_path / 's3_config.toml'
    if toml_path.exists():
        with open(toml_path, 'rb') as f:
            config = toml.load(f)
        for name, conf in config.get('providers', {}).items():
            providers[name] = conf

    # Load from environment variables (may override toml)
    for prefix in _ENV_PREFIXES:
        key_id = os.environ.get(f'{prefix}_access_key_id')
        key = os.environ.get(f'{prefix}_access_key')
        bucket = os.environ.get(f'{prefix}_bucket')
        if key_id and key and bucket:
            providers[prefix] = {
                'endpoint_url': os.environ.get(f'{prefix}_endpoint_url'),
                'access_key_id': key_id,
                'access_key': key,
                'bucket': bucket,
            }

    return providers


provider_configs = _load_providers()


@pytest.fixture(params=list(provider_configs.keys()) or ['no_providers'], scope='module')
def s3_provider(request):
    """Yield (name, config_dict) for each available S3-compatible provider."""
    if request.param == 'no_providers':
        pytest.skip('No S3 provider credentials available')
    return request.param, provider_configs[request.param]


@pytest.fixture(scope='module')
def s3_session(s3_provider):
    """Create an S3Session for the current provider."""
    name, config = s3_provider
    return s3.S3Session(
        config['access_key_id'],
        config['access_key'],
        config['bucket'],
        endpoint_url=config.get('endpoint_url'),
    )


@pytest.fixture(scope='module')
def s3_test_key():
    """Unique object key for this test run."""
    return uuid.uuid4().hex
