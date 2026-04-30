# Coldkeep Benchmarking

v1.7 introduces repository-level benchmarks to measure performance without
changing correctness guarantees.

## Running benchmarks

```bash
# Quick run against a local Postgres instance
coldkeep benchmark run --dataset small

# JSON output (suitable for tooling and baseline comparison)
coldkeep benchmark run --dataset small --output json

# Compare against a recorded baseline
coldkeep benchmark run --dataset small --output json --compare benchmark-baseline.json

# Available presets: small | medium | large
coldkeep benchmark run --dataset medium --repeat 3

# Override store-folder benchmark worker concurrency for experiments
coldkeep benchmark run --dataset small --workers 1
coldkeep benchmark run --dataset small --workers 4
```

Required environment for deterministic benchmark runs:

```bash
export DB_HOST=127.0.0.1
export DB_PORT=5432
export DB_USER=coldkeep
export DB_PASSWORD=coldkeep
export DB_NAME=coldkeep
export DB_SSLMODE=disable
export COLDKEEP_KEY=0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef
```

## Current baseline

The baseline file `benchmark-baseline.json` at the repository root was recorded
from the v1.6 codebase before v1.7 performance optimizations were applied.
It covers all eight scenarios on the **small** dataset and serves as the
regression reference for CI.

To regenerate the baseline after an intentional performance change:

```bash
coldkeep benchmark run --dataset small --output json > benchmark-baseline.json
```

## Scenarios

| Name | What it measures |
| --- | --- |
| `store-large-file` | Throughput for a single large sequential file |
| `store-many-small-files` | Throughput across many small files |
| `store-mixed-dataset` | Throughput across a mixed random/repeated dataset |
| `restore-large-file` | Restore throughput for a single large file |
| `restore-many-files` | Restore throughput across many small files |
| `snapshot-creation` | Time to create a snapshot over a populated store |
| `gc-after-churn` | GC run time after repeated store/delete churn |
| `stats-inspect` | Stats and inspect read-only query latency |

## Determinism validation

`coldkeep benchmark run` performs a determinism check that verifies:

1. **Same chunk graph** — two independent runs of the same dataset produce the
   same chunk count and logical-file hash set in the DB.
2. **Same snapshot content** — snapshot membership is stable across runs.
3. **Same restored-tree hashes** — `store → restore → SHA-256(bytes)` produces
   an identical `relative-path → digest` map across isolated runs, proving that
   user-visible restore output is byte-for-bit stable.

## Comparison thresholds

| Context | Flag | Threshold | Meaning |
| --- | --- | --- | --- |
| Local / dev | `--threshold 20` (default) | 20% | Fail if any scenario is >20% slower or throughput drops >20% |
| CI | `--threshold 100` | 100% | Fail only if a scenario becomes **more than 2× slower** (disaster detection) |

The 20% default is intentionally tight so developers notice regressions during active work.
The 100% CI threshold tolerates normal run-to-run variance on short-duration small-dataset
runs (snapshot and GC timings are DB/filesystem sensitive) while still catching catastrophic
regressions.

## CI policy

CI runs the **small** benchmark dataset on every push:

1. **Always runs the benchmark** and captures a `benchmark-baseline.json` artifact.
2. **Runs a second pass with `--compare ... --threshold 100`** to catch disasters
   (any scenario becoming >2× slower fails the job).
3. Does **not** enforce tight micro-performance numbers — normal timing variance is expected.

Run the `--compare` flag with the default `--threshold 20` locally to investigate
potential regressions before opening a PR.

## Phase 4 implementation order

The recommended execution order for Phase 4 performance work is:

1. Profile Phase 1 benchmarks.
2. Add internal prepared chunk representation.
3. Extract prepare-file-chunks phase.
4. Extract commit-prepared-chunks phase.
5. Add deterministic index validation.
6. Reduce obvious buffer copies.
7. Reuse hashers locally.
8. Optimize repeated small-file overhead only where safe.
9. Add preparation determinism tests.
10. Run full tests.
11. Run benchmark compare.
12. Document performance result in benchmark docs.

## Latest local compare result (2026-04-29)

Environment: local Postgres + deterministic mode + compare against
benchmark-baseline.json (v1.6 baseline).

Command set executed:

```bash
coldkeep benchmark run --dataset small --workers 1 --output json --compare benchmark-baseline.json --threshold 20
coldkeep benchmark run --dataset small --workers 4 --output json --compare benchmark-baseline.json --threshold 20
```

Observed compare outcome:

- `workers=1`: compare failed threshold with regressions in
   `store-large-file`, `store-mixed-dataset`, `restore-large-file`, and
   `snapshot-creation`.
- `workers=4`: compare failed threshold with regressions in
   `store-large-file` and `restore-large-file`; `store-mixed-dataset` and
   `store-many-small-files` improved relative to single-worker runs.

Current note: medium preset runs are environment-sensitive and can fail under
local contention/transient transaction-abort conditions; run medium compare in
a clean benchmark window after stabilizing local DB/containers state.

## Phase 4 carry-over

Phase 4 introduced prepare/commit separation for correctness and future pipeline
work. This currently adds measurable two-pass overhead on large-file store
workloads because the file hash and chunk preparation are still performed
separately (one full read pass for hashing, one for chunking).

This is accepted as temporary v1.7 performance debt and must remain visible in
benchmark comparisons until reduced or explicitly re-baselined. The baseline
file (`benchmark-baseline.json`) intentionally reflects the pre-Phase-4 numbers
so that the overhead stays visible.

**Status:** Phase 4 COMPLETE WITH TRACKED PERFORMANCE DEBT  
**Debt:** two-pass file hash + chunk preparation overhead  
**Target:** Phase 5 / single-pass store preparation optimization

## Phase 5

Phase 5 removes the Phase 4 two-pass store overhead by computing the logical
file hash during chunk preparation. The prepare/commit boundary remains intact:
preparation is CPU/read-side only, while commit remains sequential and ordered.

Latest local stabilized check (repeat=3 medians per dataset/worker, Postgres,
deterministic mode) still shows regression pressure versus the v1.6 small
baseline reference:

- store-large-file: still regressed
- store-mixed-dataset: still regressed
- store-many-small-files: still regressed

Notes:

- medium workers=4 improved versus medium workers=1 on mixed and large-file
   throughput, but this does not fully clear baseline regression signals.
- small workers=4 showed only slight movement and remains below the recorded
   baseline on key store scenarios.
