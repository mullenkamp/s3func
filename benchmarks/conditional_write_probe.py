"""MEGA S4 conditional-write probe: verify the spec's If-None-Match / If-Match
PutObject support behaves atomically enough to build a CAS lock on.

Phases:
1. If-None-Match:* create on a fresh key        -> expect 200
2. If-None-Match:* create on the existing key   -> expect 412, body preserved
3. Race: N parallel conditional creates on one fresh key, R rounds
                                                -> expect EXACTLY ONE 200 per round,
                                                   final body == winner's body
4. If-Match ETag CAS update: correct ETag -> 200; stale ETag -> 412, body preserved

All keys under an isolated throwaway prefix; cleanup verified by listing.
"""
import concurrent.futures
import tomllib
import uuid

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

CFG_PATH = '/home/mike/git/wrf-repos/wrf-runs/projects/sst/multi-year/v50_12km_cu_16_nudging/parameters.toml'
with open(CFG_PATH, 'rb') as f:
    ro = tomllib.load(f)['remote']['output']

BUCKET = ro['path'].strip('/').split('/')[0]          # 'sst'
PREFIX = 'envlib-condput-probe-' + uuid.uuid4().hex[:8]
print(f'endpoint: {ro["endpoint"]}  bucket: {BUCKET}  prefix: {PREFIX}')

s3 = boto3.client(
    's3',
    aws_access_key_id=ro['access_key_id'],
    aws_secret_access_key=ro['secret_access_key'],
    endpoint_url=ro['endpoint'],
    config=Config(retries={'max_attempts': 2}, max_pool_connections=20),
)


def cond_create(key, body):
    """If-None-Match:* PUT. Returns ('ok', etag) or ('412'|error-code, None)."""
    try:
        resp = s3.put_object(Bucket=BUCKET, Key=key, Body=body, IfNoneMatch='*')
        return 'ok', resp.get('ETag')
    except ClientError as e:
        code = e.response['Error'].get('Code')
        status = e.response['ResponseMetadata'].get('HTTPStatusCode')
        return f'{code}/{status}', None


try:
    ## Phase 1: conditional create on fresh key
    k1 = f'{PREFIX}/phase1'
    r, _ = cond_create(k1, b'first')
    print(f'phase1 conditional create on fresh key -> {r}')
    assert r == 'ok', r

    ## Phase 2: conditional create on EXISTING key
    r, _ = cond_create(k1, b'second')
    body = s3.get_object(Bucket=BUCKET, Key=k1)['Body'].read()
    print(f'phase2 conditional create on existing key -> {r}; body={body!r}')
    assert '412' in r or 'PreconditionFailed' in r, r
    assert body == b'first', body

    ## Phase 3: the race - N parallel conditional creates, R rounds
    N, R = 8, 5
    all_ok = True
    for rnd in range(R):
        key = f'{PREFIX}/race{rnd}'
        bodies = [f'writer-{i}'.encode() for i in range(N)]
        with concurrent.futures.ThreadPoolExecutor(max_workers=N) as ex:
            results = list(ex.map(lambda b: cond_create(key, b), bodies))
        winners = [i for i, (r, _) in enumerate(results) if r == 'ok']
        losers = [r for r, _ in results if r != 'ok']
        final = s3.get_object(Bucket=BUCKET, Key=key)['Body'].read()
        consistent = (len(winners) == 1 and final == bodies[winners[0]]) if winners else False
        print(f'phase3 round {rnd}: winners={len(winners)} loser-codes={set(losers)} '
              f'final-matches-winner={consistent}')
        all_ok = all_ok and consistent
    assert all_ok, 'race rounds must each have exactly one winner whose body persisted'

    ## Phase 4: If-Match ETag CAS update
    k4 = f'{PREFIX}/phase4'
    etag0 = s3.put_object(Bucket=BUCKET, Key=k4, Body=b'v0')['ETag']
    resp = s3.put_object(Bucket=BUCKET, Key=k4, Body=b'v1', IfMatch=etag0)
    etag1 = resp['ETag']
    print(f'phase4a If-Match with CURRENT etag -> ok (new etag differs: {etag1 != etag0})')
    try:
        s3.put_object(Bucket=BUCKET, Key=k4, Body=b'v2-stale', IfMatch=etag0)  # stale!
        stale_result = 'ok (BAD - stale ETag accepted!)'
    except ClientError as e:
        stale_result = f"{e.response['Error'].get('Code')}/{e.response['ResponseMetadata'].get('HTTPStatusCode')}"
    body4 = s3.get_object(Bucket=BUCKET, Key=k4)['Body'].read()
    print(f'phase4b If-Match with STALE etag -> {stale_result}; body={body4!r}')
    assert 'ok' not in stale_result and body4 == b'v1'

    print('\nALL PHASES PASSED: S4 conditional writes behave atomically (CAS-lock viable)')

finally:
    listed = s3.list_objects_v2(Bucket=BUCKET, Prefix=PREFIX)
    keys = [o['Key'] for o in listed.get('Contents', [])]
    if keys:
        s3.delete_objects(Bucket=BUCKET, Delete={'Objects': [{'Key': k} for k in keys]})
    left = s3.list_objects_v2(Bucket=BUCKET, Prefix=PREFIX).get('KeyCount', 0)
    print(f'cleanup: deleted {len(keys)} probe objects; leftovers: {left}')
