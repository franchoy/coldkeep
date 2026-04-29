# Coldkeep Benchmarking

v1.7 introduces repository-level benchmarks to measure performance without
changing correctness guarantees.

## Running benchmarks

```
# Quick run against a local Postgres instance
coldkeep benchmark run --dataset small

# JSON output (suitable for tooling and baseline comparison)
coldkeep benchmark run --dataset small --output json

# Compare against a recorded baseline
coldkeep benchmark run --dataset small --output json --compare benchmark-baseline.json

# Available presets: small | medium | large
coldkeep benchmark run --dataset medium --repeat 3
```

## Current baseline

The baseline file `benchmark-baseline.json` at the repository root was recorded
from the v1.6 codebase before v1.7 performance optimizations were applied.
It covers all eight scenarios on the **small** dataset and serves as the
regression reference for CI.

To regenerate the baseline after an intentional performance change:

```
coldkeep benchmark run --dataset small --output json > benchmark-baseline.json
```

## Scenarios

| Name | What it measures |
|---|---|
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
|---|---|---|---|
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
