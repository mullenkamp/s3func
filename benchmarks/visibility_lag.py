"""B2 visibility-lag characterization: how long after a write completes until it
is visible to the discovery read the lock depends on?

Conditions:
  A. S3-compat: PUT -> ListObjectsV2(prefix)     [the bakery's read primitive]
  B. same, under background write load           [conditions of the observed race]
  C. native:  b2_upload_file -> b2_list_file_versions(name)  [version-ordered lock idea]
  D. same, under background write load

Writer and observer use SEPARATE connections (mimics cross-process). Poll schedule
is adaptive (immediate, then 25ms..6.4s backoff) so class-C costs stay trivial.
Reports polls-needed and wall-clock lag distributions per condition.
"""
import base64
import concurrent.futures
import hashlib
import pathlib
import re
import statistics
import threading
import time
import urllib.parse
import uuid

import boto3
import requests
from botocore.config import Config

CFG = pathlib.Path('/home/mike/git/ebooklet/ebooklet/tests/s3_config.toml')
cfg = dict(re.findall(r"^(\w+)\s*=\s*['\"](.+?)['\"]", CFG.read_text(), re.M))
BUCKET = 'achelous'
RUN = uuid.uuid4().hex[:8]
TRIALS = 20
POLL_SCHEDULE = [0, 0.025, 0.05, 0.1, 0.2, 0.4, 0.8, 1.6, 3.2, 6.4]

def s3_client():
    return boto3.client('s3', aws_access_key_id=cfg['access_key_id'],
                        aws_secret_access_key=cfg['access_key'],
                        endpoint_url=cfg['endpoint_url'],
                        config=Config(retries={'max_attempts': 2}, max_pool_connections=20))

writer_s3 = s3_client()
observer_s3 = s3_client()   # separate connection pool = separate client

## ---- native B2 auth (same application key works for both APIs) ----
auth = requests.get('https://api.backblazeb2.com/b2api/v2/b2_authorize_account',
                    headers={'Authorization': 'Basic ' + base64.b64encode(
                        f"{cfg['access_key_id']}:{cfg['access_key']}".encode()).decode()})
auth.raise_for_status()
A = auth.json()
API, TOKEN, ACCT = A['apiUrl'], A['authorizationToken'], A['accountId']

allowed_bucket_id = (A.get('allowed') or {}).get('bucketId')
if allowed_bucket_id:
    BUCKET_ID = allowed_bucket_id
else:
    r = requests.post(f'{API}/b2api/v2/b2_list_buckets',
                      json={'accountId': ACCT, 'bucketName': BUCKET},
                      headers={'Authorization': TOKEN})
    r.raise_for_status()
    BUCKET_ID = r.json()['buckets'][0]['bucketId']

## second auth session for the OBSERVER (separate token/connection)
auth2 = requests.get('https://api.backblazeb2.com/b2api/v2/b2_authorize_account',
                     headers={'Authorization': 'Basic ' + base64.b64encode(
                         f"{cfg['access_key_id']}:{cfg['access_key']}".encode()).decode()})
auth2.raise_for_status()
API2, TOKEN2 = auth2.json()['apiUrl'], auth2.json()['authorizationToken']
obs_session = requests.Session()

def native_upload(name, body):
    r = requests.post(f'{API}/b2api/v2/b2_get_upload_url', json={'bucketId': BUCKET_ID},
                      headers={'Authorization': TOKEN})
    r.raise_for_status()
    up = r.json()
    r = requests.post(up['uploadUrl'], data=body, headers={
        'Authorization': up['authorizationToken'],
        'X-Bz-File-Name': urllib.parse.quote(name),
        'Content-Type': 'b2/x-auto',
        'X-Bz-Content-Sha1': hashlib.sha1(body).hexdigest(),
    })
    r.raise_for_status()
    return r.json()['fileId']

def native_list_has(name, file_id):
    r = obs_session.post(f'{API2}/b2api/v2/b2_list_file_versions',
                         json={'bucketId': BUCKET_ID, 'startFileName': name,
                               'prefix': name, 'maxFileCount': 20},
                         headers={'Authorization': TOKEN2})
    r.raise_for_status()
    return any(f['fileId'] == file_id for f in r.json()['files'])

def s3_list_has(key):
    resp = observer_s3.list_objects_v2(Bucket=BUCKET, Prefix=key, MaxKeys=5)
    return any(o['Key'] == key for o in resp.get('Contents', []))

def measure(write_fn, check_fn):
    """Returns (polls_needed, wall_lag_seconds, gave_up)."""
    write_fn()
    t0 = time.monotonic()
    for i, delay in enumerate(POLL_SCHEDULE):
        if delay:
            time.sleep(delay)
        if check_fn():
            return i + 1, time.monotonic() - t0, False
    return len(POLL_SCHEDULE), time.monotonic() - t0, True

## ---- background load machinery ----
stop_load = threading.Event()

def load_worker(n):
    c = s3_client()
    i = 0
    while not stop_load.is_set():
        k = f'lag-load-{RUN}/{n}-{i % 25}'
        try:
            c.put_object(Bucket=BUCKET, Key=k, Body=b'x' * 512)
        except Exception:
            time.sleep(0.1)
        i += 1

def run_condition(label, write_make, check_make, loaded):
    if loaded:
        stop_load.clear()
        pool = concurrent.futures.ThreadPoolExecutor(max_workers=6)
        futs = [pool.submit(load_worker, n) for n in range(6)]
        time.sleep(2)   # let load ramp
    results = []
    for t in range(TRIALS):
        wf, cf = write_make(t), check_make
        results.append(measure(wf, lambda cf=cf, t=t: cf(t)))
    if loaded:
        stop_load.set()
        pool.shutdown(wait=True)
    polls = [r[0] for r in results]
    lags = [r[1] for r in results]
    gaveups = sum(1 for r in results if r[2])
    multi = sum(1 for p in polls if p > 1)
    print(f'{label}: first-poll-visible {TRIALS - multi}/{TRIALS}, >1 poll: {multi}, '
          f'gave up: {gaveups}; lag p50={statistics.median(lags)*1000:.0f}ms '
          f'max={max(lags)*1000:.0f}ms; polls max={max(polls)}')
    return results

try:
    ## Condition A/B: S3-compat LIST
    s3_keys = {}
    def s3_write_make(t):
        key = f'lag-probe-{RUN}/s3-{time.monotonic_ns()}'
        s3_keys[t] = key
        return lambda: writer_s3.put_object(Bucket=BUCKET, Key=key, Body=b'ticket')
    def s3_check(t):
        return s3_list_has(s3_keys[t])

    run_condition('A. S3 LIST, idle  ', s3_write_make, s3_check, loaded=False)
    s3_keys.clear()
    run_condition('B. S3 LIST, loaded', s3_write_make, s3_check, loaded=True)

    ## Condition C/D: native version list (same-name versions, like the lock idea)
    NAT_NAME = f'lag-probe-{RUN}/native-lockname'
    fids = {}
    def nat_write_make(t):
        def w():
            fids[t] = native_upload(NAT_NAME, f'ticket-{t}'.encode())
        return w
    def nat_check(t):
        return native_list_has(NAT_NAME, fids[t])

    run_condition('C. native versions, idle  ', nat_write_make, nat_check, loaded=False)
    fids.clear()
    run_condition('D. native versions, loaded', nat_write_make, nat_check, loaded=True)

finally:
    stop_load.set()
    ## cleanup: S3-compat objects (probe + load) by prefix; native versions individually
    for prefix in (f'lag-probe-{RUN}', f'lag-load-{RUN}'):
        while True:
            listed = writer_s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix)
            keys = [o['Key'] for o in listed.get('Contents', [])]
            if not keys:
                break
            writer_s3.delete_objects(Bucket=BUCKET, Delete={'Objects': [{'Key': k} for k in keys]})
    ## native versions of the lock name need per-version deletion
    r = requests.post(f'{API}/b2api/v2/b2_list_file_versions',
                      json={'bucketId': BUCKET_ID, 'startFileName': f'lag-probe-{RUN}/',
                            'prefix': f'lag-probe-{RUN}/', 'maxFileCount': 200},
                      headers={'Authorization': TOKEN})
    for fobj in r.json().get('files', []):
        requests.post(f'{API}/b2api/v2/b2_delete_file_version',
                      json={'fileName': fobj['fileName'], 'fileId': fobj['fileId']},
                      headers={'Authorization': TOKEN})
    left = writer_s3.list_objects_v2(Bucket=BUCKET, Prefix=f'lag-probe-{RUN}').get('KeyCount', 0)
    left2 = writer_s3.list_objects_v2(Bucket=BUCKET, Prefix=f'lag-load-{RUN}').get('KeyCount', 0)
    print(f'cleanup leftovers: probe={left} load={left2}')
