# CLAUDE.md

Guidance for Claude Code when working in this repository.

## Project Overview

s3func is a lightweight, boto3-free Python client for S3-compatible object
storage (AWS S3, Backblaze B2, MEGA S4, ...) built on urllib3 with custom SigV4
signing, plus a verified distributed lock built on plain object storage.
Downstream consumer: **ebooklet** (uses `S3Session` + `S3Lock`) → cfdb → envlib.

## Development Commands

Uses **uv**.

```bash
uv sync
uv run pytest s3func/tests/test_lock_model.py s3func/tests/test_mock_s3.py  # fast, no network
uv run pytest s3func/tests/                                                # FULL suite - hits LIVE buckets
```

- **Live tests cost real requests/money and need credentials**:
  `s3func/tests/s3_config.toml` (git-ignored) with `[connection_config]`
  (legacy B2-native tests) and `[providers.b2]` / `[providers.mega]` sections —
  the conftest parameterizes S3-compatible tests across every configured provider.
- `benchmarks/` scripts run LIVE and deliberately (lock race campaign,
  conditional-write qualification probe, visibility-lag measurement) — never as
  part of routine testing. Results recorded in `benchmarks/results_visibility_lag.md`.
- After changing `locking.py` or `s3.py`/`signer.py`, run the ebooklet suite as
  a downstream regression: `cd ~/git/ebooklet && uv run --with ~/git/s3func pytest ebooklet/tests/`.
  **CAUTION**: `uv run --with <local dir>` can serve a STALE cached build of the
  directory (observed 2026-07-05: a cached booklet 0.12.0 masqueraded as the
  working tree and produced phantom failures). Before trusting such a run,
  verify inside the overlay (`uv run --with ~/git/s3func python -c "import s3func;
  print(s3func.__version__)"`) or `uv cache clean s3func` first.

## Architecture

- `s3.py` — `S3Session` (all providers). Object URLs MUST go through
  `_object_url()` (percent-encodes keys once). `delete_objects` raises
  `HTTPError` listing the failed keys if ANY delete fails (all chunks are
  attempted first) — the multi-delete response must never be ignored: Quiet
  mode still returns per-key `<Error>` elements, and urllib3 does not raise
  on HTTP status.
- `signer.py` — SigV4. **Invariant: wire encoding == canonical encoding**,
  strict RFC3986 for both paths and query strings (some providers, e.g. MEGA,
  do not canonicalize a wire `+` as a space). `add_auth(_now=...)` is a test seam.
- `response.py` — response wrappers + XML/JSON list parsers. ListVersions
  yields delete markers explicitly with `delete_marker: True`.
- `locking.py` — the bakery-style `DistributedLock`/`S3Lock`. Full design +
  diagrams: **`docs/locking.md`**. Non-negotiable invariants (each one is a
  live-reproduced failure mode, see the doc's table):
  - election decisions go through the pure `evaluate_election` over a parsed
    listing snapshot — never re-implement the comparison inline;
  - ALL election timestamps (including one's own) come from the LISTING, never
    from put-responses (split-brain otherwise);
  - a ticket existing is not the lock: only a won election sets `_acquired`
    (recovery via `lock_id=` re-runs the election);
  - every decisive listing must contain the contender's own ticket
    (self-visibility gate + own-ticket invariant);
  - `SESSION_REGISTRY` / callable-as-service is the injection seam for tests
    and future providers; a CAS lock may only be added for a provider that
    passes `benchmarks/conditional_write_probe.py` (none currently do).
- `b2.py` + `B2Lock` — B2 native API, **deprecated since 0.9.0, removal in
  1.0**. Do not extend; B2 works fully through `S3Session`.
- `http_url.py` — plain-HTTP read-only session.

## Transport invariants (2026-07-15 review — do not regress these)

- **Never hand urllib3 a monolithic `bytes` body larger than `utils.stream_body_threshold` (1MiB).** A bytes body is transmitted as ONE socket `sendall()` whose deadline is fixed at entry and never extended by progress — any upload slower than `len(body)/connect_timeout` dies spuriously mid-transfer on a healthy connection, then retries from byte 0 (this deterministically broke every concurrent large-group ebooklet push; dual-blind-review-confirmed, live A/B proven). `put_object` (S3 and B2-native) wraps large bytes in `io.BytesIO` so urllib3 streams 16KiB chunks: the timeout becomes per-chunk idle, retries rewind via `seek(0)`, and the payload hash is still computed from the bytes in one pass. Do not "simplify" the wrap away.
- **`session()` must construct `Timeout(connect=..., read=...)` — never `Timeout(x)`**, whose first positional arg is `total=` (a hard wall on whole requests; this was the 0.9.3 bug, mislabeled as a read timeout). `read=` is a per-response-read idle bound; `connect=` also governs each body-send chunk (an urllib3 semantic — the send phase runs on the connect timeout).
- The offline send-wall tripwire (`tests/test_streaming_upload.py`, throttled loopback sink) guards both invariants in the normal suite; `tests/test_scale_transport.py` (`pytest -m scale`, never CI) reproduces the failing regime live. Full mechanism + evidence: the ebooklet repo's `planning/transport-review-*.md`.

## Testing Philosophy

`test_lock_model.py` carries the lock's correctness: a `FakeSession` with
per-observer staleness injection, regressions for every root-caused bug, and an
exhaustive small-model check over contender interleavings. Keep it green and
extend it FIRST when touching lock logic; live tests validate providers, not
the algorithm.

## Provider Quirks (verified live)

- B2: S3-compat LIST timestamps are second-granularity (ties are routine);
  conditional writes rejected (`NotImplemented`).
- MEGA S4: conditional writes accepted but NOT atomic under concurrency;
  multi-object-delete hangs on keys containing `$` (use single deletes);
  wire `+` in queries is not treated as a space.
