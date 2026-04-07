#!/usr/bin/env python3
"""
Benchmark: s3func vs rclone for concurrent S3 upload/download performance.

Measures wall-clock time to transfer 10 files simultaneously at various sizes.
s3func uses ThreadPoolExecutor; rclone uses its built-in --transfers flag.

Usage:
    python benchmarks/bench_s3func_vs_rclone.py --provider b2
    python benchmarks/bench_s3func_vs_rclone.py --provider mega --iterations 5 --sizes 1MB,10MB
    python benchmarks/bench_s3func_vs_rclone.py --provider b2 --output-csv results.csv
"""
import argparse
import atexit
import concurrent.futures
import csv
import os
import shutil
import subprocess
import sys
import tempfile
import time
import uuid

try:
    import tomllib as toml
except ImportError:
    import tomli as toml

# Add project root to path so s3func is importable
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from s3func import S3Session


SIZES = {
    '1MB': 1 << 20,
    '10MB': 10 << 20,
    '100MB': 100 << 20,
}

NUM_FILES = 10
STREAM_CHUNK_SIZE = 1 << 20  # 1MB chunks for stream-to-disk downloads

DEFAULT_CONFIG = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    's3func', 'tests', 's3_config.toml',
)


def parse_args():
    parser = argparse.ArgumentParser(description='Benchmark s3func vs rclone for concurrent S3 transfers')
    parser.add_argument('--provider', required=True, help='Provider name from s3_config.toml (e.g. b2, mega)')
    parser.add_argument('--config', default=DEFAULT_CONFIG, help='Path to s3_config.toml')
    parser.add_argument('--iterations', type=int, default=3, help='Number of iterations per benchmark (default: 3)')
    parser.add_argument('--sizes', default='1MB,10MB,100MB', help='Comma-separated file sizes (default: 1MB,10MB,100MB)')
    parser.add_argument('--rclone-path', default='rclone', help='Path to rclone binary (default: rclone)')
    parser.add_argument('--output-csv', default=None, help='Write raw results to CSV file')
    return parser.parse_args()


def load_provider_config(config_path, provider_name):
    with open(config_path, 'rb') as f:
        config = toml.load(f)

    providers = config.get('providers', {})
    if provider_name not in providers:
        available = ', '.join(providers.keys()) or '(none)'
        print(f"Error: provider '{provider_name}' not found. Available: {available}")
        sys.exit(1)

    return providers[provider_name]


def generate_test_files(sizes, tmpdir):
    """Generate NUM_FILES random files per size in separate subdirectories."""
    files = {}
    for label, size_bytes in sizes.items():
        size_dir = os.path.join(tmpdir, label)
        os.makedirs(size_dir)
        paths = []
        for i in range(NUM_FILES):
            path = os.path.join(size_dir, f'file_{i}.bin')
            with open(path, 'wb') as f:
                f.write(os.urandom(size_bytes))
            paths.append(path)
        files[label] = {'dir': size_dir, 'paths': paths, 'size_bytes': size_bytes}
    return files


def build_rclone_base_args(config, rclone_path):
    return [
        rclone_path,
        '--s3-provider', 'Other',
        '--s3-access-key-id', config['access_key_id'],
        '--s3-secret-access-key', config['access_key'],
        '--s3-endpoint', config['endpoint_url'],
        '--s3-region', 'us-east-1',
        '--no-check-dest',
        '--s3-no-check-bucket',
        '--ignore-checksum',
        '--transfers', str(NUM_FILES),
    ]


# -- s3func benchmarks --

def bench_s3func_upload(session, keys_and_data):
    """Upload NUM_FILES concurrently via ThreadPoolExecutor. Returns elapsed seconds."""
    def _upload(key, data):
        resp = session.put_object(key, data)
        if resp.status != 200:
            raise RuntimeError(f's3func upload failed for {key}: status {resp.status}')

    start = time.perf_counter()
    with concurrent.futures.ThreadPoolExecutor(max_workers=NUM_FILES) as pool:
        futures = [pool.submit(_upload, key, data) for key, data in keys_and_data]
        concurrent.futures.wait(futures)
        for fut in futures:
            fut.result()  # raise any exceptions
    return time.perf_counter() - start


def bench_s3func_download_memory(session_no_stream, keys, expected_size):
    """Download NUM_FILES concurrently into memory (stream=False). Returns elapsed seconds."""
    def _download(key):
        resp = session_no_stream.get_object(key)
        data = resp.data
        if len(data) != expected_size:
            raise RuntimeError(f's3func download size mismatch for {key}: {len(data)} != {expected_size}')

    start = time.perf_counter()
    with concurrent.futures.ThreadPoolExecutor(max_workers=NUM_FILES) as pool:
        futures = [pool.submit(_download, key) for key in keys]
        concurrent.futures.wait(futures)
        for fut in futures:
            fut.result()
    return time.perf_counter() - start


def bench_s3func_download_stream(session_stream, keys, expected_size, dest_dir):
    """Download NUM_FILES concurrently, streaming to disk (stream=True). Returns elapsed seconds."""
    def _download(key, dest_path):
        resp = session_stream.get_object(key)
        written = 0
        with open(dest_path, 'wb') as f:
            while True:
                chunk = resp.stream.read(STREAM_CHUNK_SIZE)
                if not chunk:
                    break
                f.write(chunk)
                written += len(chunk)
        resp.stream.release_conn()
        if written != expected_size:
            raise RuntimeError(f's3func stream download size mismatch for {key}: {written} != {expected_size}')

    start = time.perf_counter()
    with concurrent.futures.ThreadPoolExecutor(max_workers=NUM_FILES) as pool:
        futures = []
        for i, key in enumerate(keys):
            dest = os.path.join(dest_dir, f'dl_{i}.bin')
            futures.append(pool.submit(_download, key, dest))
        concurrent.futures.wait(futures)
        for fut in futures:
            fut.result()
    return time.perf_counter() - start


# -- rclone benchmarks --

def bench_rclone_upload(base_args, bucket, prefix, local_dir):
    """Upload a directory of files via rclone copy. Returns elapsed seconds."""
    args = base_args + ['copy', local_dir, f':s3:{bucket}/{prefix}']
    start = time.perf_counter()
    result = subprocess.run(args, capture_output=True, text=True)
    elapsed = time.perf_counter() - start
    if result.returncode != 0:
        raise RuntimeError(f'rclone upload failed: {result.stderr}')
    return elapsed


def bench_rclone_download(base_args, bucket, prefix, dest_dir):
    """Download a prefix of files via rclone copy. Returns elapsed seconds."""
    args = base_args + ['copy', f':s3:{bucket}/{prefix}', dest_dir]
    start = time.perf_counter()
    result = subprocess.run(args, capture_output=True, text=True)
    elapsed = time.perf_counter() - start
    if result.returncode != 0:
        raise RuntimeError(f'rclone download failed: {result.stderr}')
    return elapsed


# -- Cleanup --

def cleanup_s3_objects(session, prefix):
    """Delete all test objects from S3 in bulk."""
    try:
        resp = session.list_objects(prefix=prefix)
        keys = [obj['key'] for obj in resp.iter_objects()]
        if keys:
            session.delete_objects(keys)
    except Exception as e:
        print(f'  Warning: cleanup failed: {e}')


# -- Results formatting --

def format_size(size_bytes):
    if size_bytes < 1024:
        return f'{size_bytes} B'
    elif size_bytes < 1 << 20:
        return f'{size_bytes // 1024} KB'
    else:
        return f'{size_bytes // (1 << 20)} MB'


def throughput_mbps(total_bytes, elapsed_seconds):
    if elapsed_seconds <= 0:
        return float('inf')
    return (total_bytes / (1 << 20)) / elapsed_seconds


def print_results(results, provider_name, endpoint, bucket, iterations):
    print()
    print(f'=== S3 Benchmark: s3func vs rclone ({NUM_FILES} concurrent files) ===')
    print(f'Provider: {provider_name} | Endpoint: {endpoint}')
    print(f'Bucket: {bucket} | Iterations: {iterations} | Files per batch: {NUM_FILES}')

    # Group results by operation and size
    upload_results = {}
    download_results = {}
    for r in results:
        key = (r['operation'], r['size_label'])
        bucket_dict = upload_results if r['operation'] == 'upload' else download_results
        if key not in bucket_dict:
            bucket_dict[key] = {}
        tool = r['tool']
        if tool not in bucket_dict[key]:
            bucket_dict[key][tool] = []
        bucket_dict[key][tool].append(r)

    # Upload table
    print()
    print(f'UPLOAD ({NUM_FILES} files simultaneously)')
    print(f'{"Size/file":<10} {"Total":<10} {"s3func (s)":<12} {"rclone (s)":<12} {"s3func (MB/s)":<15} {"rclone (MB/s)":<15} {"Ratio":<8}')
    print('-' * 82)

    for size_label in [s for s in SIZES if ('upload', s) in upload_results]:
        data = upload_results[('upload', size_label)]
        size_bytes = SIZES[size_label]
        total_bytes = size_bytes * NUM_FILES

        s3func_times = [r['elapsed'] for r in data.get('s3func', [])]
        rclone_times = [r['elapsed'] for r in data.get('rclone', [])]

        s3func_avg = sum(s3func_times) / len(s3func_times) if s3func_times else 0
        rclone_avg = sum(rclone_times) / len(rclone_times) if rclone_times else 0

        s3func_tp = throughput_mbps(total_bytes, s3func_avg)
        rclone_tp = throughput_mbps(total_bytes, rclone_avg)

        ratio = rclone_avg / s3func_avg if s3func_avg > 0 else 0

        print(f'{size_label:<10} {format_size(total_bytes):<10} {s3func_avg:<12.3f} {rclone_avg:<12.3f} {s3func_tp:<15.2f} {rclone_tp:<15.2f} {ratio:<8.2f}x')

    # Download table
    print()
    print(f'DOWNLOAD ({NUM_FILES} files simultaneously)')
    print(f'{"Size/file":<10} {"Total":<10} {"s3func mem(s)":<15} {"s3func disk(s)":<16} {"rclone (s)":<12} {"s3func mem":<12} {"s3func disk":<13} {"rclone":<10}')
    print(f'{"":10} {"":10} {"":15} {"":16} {"":12} {"(MB/s)":12} {"(MB/s)":13} {"(MB/s)":10}')
    print('-' * 98)

    for size_label in [s for s in SIZES if ('download', s) in download_results]:
        data = download_results[('download', size_label)]
        size_bytes = SIZES[size_label]
        total_bytes = size_bytes * NUM_FILES

        mem_times = [r['elapsed'] for r in data.get('s3func_mem', [])]
        disk_times = [r['elapsed'] for r in data.get('s3func_disk', [])]
        rclone_times = [r['elapsed'] for r in data.get('rclone', [])]

        mem_avg = sum(mem_times) / len(mem_times) if mem_times else 0
        disk_avg = sum(disk_times) / len(disk_times) if disk_times else 0
        rclone_avg = sum(rclone_times) / len(rclone_times) if rclone_times else 0

        mem_tp = throughput_mbps(total_bytes, mem_avg)
        disk_tp = throughput_mbps(total_bytes, disk_avg)
        rclone_tp = throughput_mbps(total_bytes, rclone_avg)

        print(f'{size_label:<10} {format_size(total_bytes):<10} {mem_avg:<15.3f} {disk_avg:<16.3f} {rclone_avg:<12.3f} {mem_tp:<12.2f} {disk_tp:<13.2f} {rclone_tp:<10.2f}')

    print()
    print('Notes:')
    print('  - s3func uploads from memory; rclone reads from disk')
    print('  - s3func mem = download to memory (stream=False)')
    print('  - s3func disk = download streaming to disk (stream=True)')
    print('  - rclone downloads write to disk')
    print('  - Ratio > 1 means s3func is faster')


def write_csv(results, path):
    with open(path, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=['tool', 'operation', 'size_label', 'size_bytes', 'total_bytes', 'iteration', 'elapsed', 'throughput_mbps'])
        writer.writeheader()
        for r in results:
            writer.writerow(r)
    print(f'CSV results written to {path}')


# -- Main --

def main():
    args = parse_args()

    # Parse sizes
    size_labels = [s.strip() for s in args.sizes.split(',')]
    for s in size_labels:
        if s not in SIZES:
            print(f"Error: unknown size '{s}'. Available: {', '.join(SIZES.keys())}")
            sys.exit(1)
    sizes = {s: SIZES[s] for s in size_labels}

    # Check rclone
    if not shutil.which(args.rclone_path):
        print(f"Error: rclone not found at '{args.rclone_path}'")
        sys.exit(1)

    # Load config
    config = load_provider_config(args.config, args.provider)

    # Create temp directory
    tmpdir = tempfile.mkdtemp(prefix='s3bench_')
    atexit.register(shutil.rmtree, tmpdir, ignore_errors=True)

    print(f'Generating test files ({NUM_FILES} files per size)...')
    files = generate_test_files(sizes, tmpdir)

    # Create sessions
    session = S3Session(
        config['access_key_id'], config['access_key'], config['bucket'],
        endpoint_url=config.get('endpoint_url'), stream=False,
    )
    session_stream = S3Session(
        config['access_key_id'], config['access_key'], config['bucket'],
        endpoint_url=config.get('endpoint_url'), stream=True,
    )

    rclone_base = build_rclone_base_args(config, args.rclone_path)
    run_id = uuid.uuid4().hex[:8]
    run_prefix = f'benchmark/{run_id}/'
    results = []

    try:
        for size_label in size_labels:
            info = files[size_label]
            size_bytes = info['size_bytes']
            total_bytes = size_bytes * NUM_FILES
            print(f'\nBenchmarking {size_label} ({NUM_FILES} x {size_label} = {format_size(total_bytes)} total)...')

            # Pre-read file data for s3func
            file_data = []
            for path in info['paths']:
                with open(path, 'rb') as f:
                    file_data.append(f.read())

            # --- Uploads ---
            for iteration in range(args.iterations):
                # s3func upload
                s3func_prefix = f'benchmark/{run_id}/s3func/{size_label}/{iteration}'
                keys_and_data = []
                for i in range(NUM_FILES):
                    key = f'{s3func_prefix}/file_{i}.bin'
                    keys_and_data.append((key, file_data[i]))

                elapsed = bench_s3func_upload(session, keys_and_data)
                tp = throughput_mbps(total_bytes, elapsed)
                results.append({
                    'tool': 's3func', 'operation': 'upload', 'size_label': size_label,
                    'size_bytes': size_bytes, 'total_bytes': total_bytes,
                    'iteration': iteration, 'elapsed': elapsed, 'throughput_mbps': tp,
                })
                print(f'  Upload s3func  iter {iteration}: {elapsed:.3f}s ({tp:.2f} MB/s)')

                # rclone upload
                rclone_prefix = f'benchmark/{run_id}/rclone/{size_label}/{iteration}'
                elapsed = bench_rclone_upload(rclone_base, config['bucket'], rclone_prefix, info['dir'])
                tp = throughput_mbps(total_bytes, elapsed)
                results.append({
                    'tool': 'rclone', 'operation': 'upload', 'size_label': size_label,
                    'size_bytes': size_bytes, 'total_bytes': total_bytes,
                    'iteration': iteration, 'elapsed': elapsed, 'throughput_mbps': tp,
                })
                print(f'  Upload rclone  iter {iteration}: {elapsed:.3f}s ({tp:.2f} MB/s)')

            # --- Downloads ---
            # Use the first iteration's uploads as download source
            s3func_dl_prefix = f'benchmark/{run_id}/s3func/{size_label}/0'
            rclone_dl_prefix = f'benchmark/{run_id}/rclone/{size_label}/0'
            s3func_dl_keys = [f'{s3func_dl_prefix}/file_{i}.bin' for i in range(NUM_FILES)]

            for iteration in range(args.iterations):
                # s3func download (memory)
                elapsed = bench_s3func_download_memory(session, s3func_dl_keys, size_bytes)
                tp = throughput_mbps(total_bytes, elapsed)
                results.append({
                    'tool': 's3func_mem', 'operation': 'download', 'size_label': size_label,
                    'size_bytes': size_bytes, 'total_bytes': total_bytes,
                    'iteration': iteration, 'elapsed': elapsed, 'throughput_mbps': tp,
                })
                print(f'  DL s3func mem  iter {iteration}: {elapsed:.3f}s ({tp:.2f} MB/s)')

                # s3func download (stream to disk)
                dl_dir = os.path.join(tmpdir, f'dl_stream_{size_label}_{iteration}')
                os.makedirs(dl_dir, exist_ok=True)
                elapsed = bench_s3func_download_stream(session_stream, s3func_dl_keys, size_bytes, dl_dir)
                tp = throughput_mbps(total_bytes, elapsed)
                results.append({
                    'tool': 's3func_disk', 'operation': 'download', 'size_label': size_label,
                    'size_bytes': size_bytes, 'total_bytes': total_bytes,
                    'iteration': iteration, 'elapsed': elapsed, 'throughput_mbps': tp,
                })
                print(f'  DL s3func disk iter {iteration}: {elapsed:.3f}s ({tp:.2f} MB/s)')

                # rclone download
                dl_dir = os.path.join(tmpdir, f'dl_rclone_{size_label}_{iteration}')
                os.makedirs(dl_dir, exist_ok=True)
                elapsed = bench_rclone_download(rclone_base, config['bucket'], rclone_dl_prefix, dl_dir)
                tp = throughput_mbps(total_bytes, elapsed)
                results.append({
                    'tool': 'rclone', 'operation': 'download', 'size_label': size_label,
                    'size_bytes': size_bytes, 'total_bytes': total_bytes,
                    'iteration': iteration, 'elapsed': elapsed, 'throughput_mbps': tp,
                })
                print(f'  DL rclone      iter {iteration}: {elapsed:.3f}s ({tp:.2f} MB/s)')

    finally:
        print(f'\nCleaning up test objects under {run_prefix}...')
        cleanup_s3_objects(session, run_prefix)

    print_results(results, args.provider, config.get('endpoint_url', 'AWS'), config['bucket'], args.iterations)

    if args.output_csv:
        write_csv(results, args.output_csv)


if __name__ == '__main__':
    main()
