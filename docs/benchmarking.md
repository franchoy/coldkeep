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

## CI policy

CI runs the **small** benchmark dataset on every push to confirm that the
benchmark infrastructure itself works and to capture a timing artifact.
The goal is to detect major regressions and build failures, not to enforce
precise micro-performance numbers.  Run-to-run variance is expected; the
`--compare` flag is a developer tool for deliberate regression investigation,
not a CI gate.
