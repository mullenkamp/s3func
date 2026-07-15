# Changelog

Notable changes to s3func. The format loosely follows [Keep a Changelog](https://keepachangelog.com/);
s3func does not promise SemVer — minor versions may change behavior.
Entries for 0.8.5 and earlier were reconstructed from commit history after the fact.

## 0.9.5 (2026-07-16)

### Fixed
- **Cloudflare-style edge errors (520-524) are now retried** on idempotent
  methods, alongside the existing 429/5xx set. B2's fronting infrastructure
  emits them transiently; a bare 522 ('The response produced nonsense
  content') failed an ebooklet CI run un-retried on 2026-07-15.

### Changed
- **CI matrix restored to parallel** (reverting 0.9.4's `max-parallel: 1`,
  also reverted in ebooklet). The serialization was misdiagnosis: with the
  failure logs finally in hand, none of the matrix failures were cross-job
  races (all remote keys are per-run uuids) — they were an un-retried 522
  (fixed above), a PyPI-propagation race during a release, and an intra-test
  timing flake in the DEPRECATED B2-native lock's own 4-worker contention
  test (1 of 4 workers exceeded its 120s acquire timeout; that test leaves
  with the module's planned removal). Parallel matrix jobs also mirror
  production reality: many concurrent ingestion processes against one
  account is the normal envlib workload.

## 0.9.4 (2026-07-15)

The no-spurious-timeout release: two fixes that together make slow-but-
progressing transfers (in either direction) immune to timeouts while genuinely
stalled sockets still die promptly. Found by the ebooklet 0.10.1 pre-release
end-to-end push test; mechanism established by a dual-blind transport review
(both reviewers converged on the same diagnosis and the same fix by
independent live experiments — reports in the ebooklet repo's `planning/`).
Neither fix is sufficient alone: the timeout split without body streaming
made concurrent uploads strictly WORSE (it tightened the effective send wall
from 60s to 30s).

### Fixed
- **Large uploads no longer race a hidden whole-body deadline.** `put_object`
  (S3 and B2-native) passed `bytes` bodies straight to urllib3, which
  transmits a bytes body as ONE socket `sendall()` — and CPython fixes that
  call's deadline at entry from the socket timeout, never extending it on
  progress. Any PUT whose transmission time exceeded the socket timeout died
  with `TimeoutError('The write operation timed out')` on a perfectly healthy
  connection, then retried from byte 0 (concurrency only divides per-stream
  throughput, so ebooklet's newly-concurrent pushes hit this wall
  constantly — e.g. 10 streams on a ~10MB/s uplink fail every group over
  ~30MB, deterministically). Bytes bodies over 1MiB (`utils.stream_body_threshold`)
  are now wrapped in `io.BytesIO`: urllib3 streams them in 16KiB chunks, the
  timeout becomes per-chunk (a true idle bound), retries still rewind via
  `seek(0)`, and the payload hash (SigV4 SHA-256 / B2 SHA-1) is still computed
  from the original bytes in one pass. Verified live A/B: 10×78MB raw-bytes
  PUTs = 0/10; identical bodies via BytesIO = 10/10 with zero retries, even
  on streams that legitimately took 275s. RAM cost: none — CPython's
  `BytesIO(bytes)` is copy-on-write (measured: +0 MB on wrap and on chunked
  reads; a copy happens only if the stream is written to, which the upload
  path never does).
- **The session timeout is now a true READ (idle) timeout, not a total-request
  deadline.** `session()` built `urllib3.util.Timeout(timeout)`, whose first
  positional argument is `total=` — so every request had to COMPLETE within
  `read_timeout` seconds (default 120; ebooklet passes 60), despite the
  parameter being named and documented as a read timeout everywhere. This
  also applied to large ranged GETs on slow downlinks (consumers pulling
  ~100MB group objects). Now `Timeout(connect=connect_timeout, read=timeout)`.
- **B2-native `put_object` rewinds file-like bodies before every retry
  attempt.** Its manual 401/503 retry loop re-POSTs the body; a stream
  consumed by the previous attempt (reachable since the large-bytes BytesIO
  wrap above) would have retried as an empty body against a non-zero
  Content-Length. (The B2-native module remains deprecated — removal at 1.0;
  the S3-compatible endpoint is the supported path.)
- **CI: the test matrix now runs one Python version at a time**
  (`max-parallel: 1`, also applied to ebooklet). The live suites talk to real
  S3 providers with shared credentials — concurrent matrix jobs raced each
  other (lock elections, native-B2 auth) and failed spuriously.

### Added
- **`connect_timeout` parameter** on `session()` and `S3Session` (default 30):
  bounds connection establishment, and — an urllib3 semantic worth knowing —
  each body-send chunk during uploads.

## 0.9.3 (2026-07-12)

### Changed
- **`break_other_locks()` is age-gated by default**: a default-argument call now
  deletes only tickets uploaded more than `locking.default_break_age` seconds
  (2 hours) ago. Previously the default cutoff was "now" — it deleted EVERY
  ticket under the lock prefix, including a live writer's (silently ending its
  mutual exclusion mid-session) and the caller's own. Pass an explicit
  `timestamp` to override the gate (e.g. now, to break everything).
- **`break_other_locks()` never deletes the caller's own tickets** (the name now
  tells the truth). A crashed-and-reopened process gets a fresh `lock_id`, so
  its stale previous ticket remains breakable as "other".

### Added
- **`DistributedLock.verify()`**: re-verify that THIS instance still holds the
  lock — True only if the election was won and a fresh listing still shows both
  of the instance's ticket objects. Lets a holder cheaply re-check mutual
  exclusion at critical boundaries (ebooklet calls it at push start and before
  the commit PUT); a broken holder aborts safely instead of writing without the
  lock. Under listing staleness the failure direction is a spurious abort.

## 0.9.2 (2026-07-09)

### Fixed
- **Transient HTTP error responses (429, 500, 502, 503, 504) are now retried with
  exponential backoff** on idempotent methods (GET/PUT/DELETE/HEAD). Previously only
  *connection* errors were retried — an error *response* (e.g. Backblaze's
  `500 InternalError: internal incident`, observed live on lock PUTs) was returned to
  the caller un-retried. One shared urllib3 Retry policy covers all three session
  classes (S3/B2/HTTP).

### Changed
- When status retries exhaust, the **final response is returned** (never an
  exception), preserving the existing caller contract of dispatching on
  `resp.status`/`resp.error`. Note this also changes the persistent
  429/503-with-`Retry-After` path, which previously raised `MaxRetryError` on
  exhaustion.
- `Retry-After` headers are now honored on 5xx responses too, uncapped — a
  misconfigured endpoint sending a huge `Retry-After` can block a request thread for
  up to `max_attempts` × that value.
- POST requests (the S3 multi-object-delete, all B2-native upload/copy/delete calls)
  are deliberately **never** status-retried; `B2Session.put_object` keeps its own
  manual retry loop.
- `retry_mode` on `S3Session` is deprecated (it was never consumed); kept for
  signature compatibility, removal at 1.0.
- README retry claims corrected to match the actual policy.

## 0.9.1 (2026-07-05)

### Fixed
- `delete_objects` now checks every chunk's response: non-2xx marks the whole chunk
  failed; per-key `<Error>` elements are parsed out of 200 responses; after
  attempting all chunks an `HTTPError` listing the failed keys is raised. Previously
  the batched delete POST's response was ignored entirely, so failures silently
  no-oped.

## 0.9.0 (2026-07-03)

### Fixed
- **The bakery lock's election logic was rewritten** after an instrumented race
  campaign reproduced historical mutual-exclusion violations at scale (57/985
  acquisitions on the old code; 0/311 on the new): a pure election core
  (all-competitor scan, single-source timestamps from the listing, deterministic id
  tiebreak) replaced per-iteration accumulation, plus a self-visibility gate,
  confirming re-list, own-ticket invariant, and recovery re-election.
- SigV4 signing of object keys with special characters (path encoding + strict
  RFC3986 query encoding) — previously keys like `temperature!0.0.40` failed
  signature validation on B2, and `+`-in-query broke MEGA.

### Changed
- Provider/session registry for the lock machinery; the native-B2 lock path is
  deprecated in favor of the S3-compatible endpoint.

## 0.8.5 / 0.8.4 (2026-04-07)

### Fixed
- `delete_objects` fixes across providers.

## 0.8.3 (2026-04-06)

### Fixed
- Compatibility fixes for MEGA S4.

## 0.8.2 (2026-02-15)

### Changed
- Locking updates.

## 0.8.0 / 0.8.1 (2026-02-08 – 2026-02-09)

### Changed
- API consistency pass.

## 0.7.x (2024-09 – 2025-07)

- 0.7.3: fixed max connections for urllib3. 0.7.1: API polish. 0.7.0: changed
  `list_objects` output.

## 0.6.0 and earlier

Pre-changelog history (0.3.x–0.6.0, 2024): the S3/B2 session classes, S3Lock/B2Lock,
metadata parsing, and the response wrapper. See `git log` for details.
