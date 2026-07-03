# Visibility-lag and conditional-write results (2026-07-02/03)

## List-after-write visibility (B2, bucket achelous, from NZ; 20 trials/condition)

| Condition | first-poll visible | lag p50 | lag max |
|---|---|---|---|
| S3-compat LIST, idle | 20/20 | 155 ms | 479 ms |
| S3-compat LIST, loaded | 20/20 | 152 ms | 169 ms |
| B2-native version list, idle | 20/20 | 303 ms | 492 ms |
| B2-native version list, loaded | 20/20 | 219 ms | 226 ms |

Interpretation: typical-case list-after-write is consistent at first poll (~1 RTT).
The native API offers no visibility or latency advantage (deprecated in 0.9.0).
80 trials cannot exclude rare (<~1%/poll) tail staleness - the 0.9.0 lock
hardening (self-visibility gate + confirming re-list) targets exactly that tail.

## Conditional writes (CAS qualification probe: conditional_write_probe.py)

| Provider | If-None-Match PUT | Atomic under concurrency? |
|---|---|---|
| Backblaze B2 (S3-compat) | rejected: NotImplemented | n/a |
| Backblaze B2 (native API) | no such capability (spec-checked) | n/a |
| MEGA S4 (ca-west-1) | accepted; sequential semantics correct (412 + preserved) | **NO** - 8 simultaneous If-None-Match:* creates ALL returned 200 in 4/5 rounds |

A provider must pass ALL probe phases (including the parallel-winners race)
before a CAS lock implementation is added for it in s3func's provider registry.

## Provider quirks found during 0.9.0 verification

- **MEGA S4: multi-object-delete (POST DeleteObjects) hangs indefinitely on keys
  containing `$`** (request never answered; singular DELETE works fine for the
  same keys). Reproduced 2026-07-03. Worth reporting to MEGA alongside the
  conditional-write atomicity issue.
- **MEGA S4 does not canonicalize a wire `+` in query strings as a space** -
  s3func 0.9.0 emits strict RFC3986 (%20) query encoding so wire and SigV4
  canonical forms are identical on every provider.
- **B2 S3-compat LIST returns second-granularity LastModified** (milliseconds
  zeroed) - under contention all tickets tie, which is why the lock election
  derives every timestamp (including one's own) from the listing and breaks
  ties lexicographically by lock id.
