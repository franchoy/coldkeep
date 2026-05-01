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

## Phase 7 benchmark matrix (2026-05-01)

Executed matrix:

```bash
coldkeep benchmark run --dataset small --workers 1 --output json
coldkeep benchmark run --dataset small --workers 4 --output json
coldkeep benchmark run --dataset medium --workers 1 --output json
coldkeep benchmark run --dataset medium --workers 4 --output json
```

Focused read-path scenarios from this run:

- `snapshot-creation`
   - small w1: 672ms
   - small w4: 631ms
   - medium w1: 69901ms
   - medium w4: 43955ms
- `gc-after-churn`
   - small w1: 2462ms
   - small w4: 1733ms
   - medium w1: 30225ms
   - medium w4: 22860ms
- `stats-inspect`
   - small w1: 903ms
   - small w4: 635ms
   - medium w1: 73280ms
   - medium w4: 49338ms

Write-path impact check (Step 8) for index-cost guardrails:

- Compared to the v1.6 small baseline, single-worker store timings regressed for
   `store-large-file` and `store-mixed-dataset` in this local run.
- At workers=4, `store-many-small-files` and `store-mixed-dataset` improved,
   while `store-large-file` remained slower than baseline.
- No new Phase 7 candidate snapshot index was added, so these write-path
   results are not attributable to a newly introduced snapshot index.

Decision recorded for Phase 7:

- Keep candidate snapshot indexes out of schema until EXPLAIN plus benchmark
   evidence demonstrates real read-path gain with acceptable write-path cost.

### Phase 7 - Benchmark Results (revision)

#### Improvements

- GC after churn improved significantly (up to ~34% on small, ~24% on medium).
- stats/inspect improved significantly (up to ~40% on small, ~32% on medium).
- Worker scaling remains strong across all measured scenarios.

#### Snapshot creation behavior

Snapshot creation regressed versus the v1.6 small baseline:

- ~29% slower on small dataset (`workers=1`).
- ~21% slower on small dataset (`workers=4`).

However, snapshot creation still scales with workers in medium runs:

- ~37% faster from `workers=1` to `workers=4` on medium.

#### Interpretation

This behavior is treated as a workload-shift effect rather than a correctness or
stability failure. The likely contributors are:

- Increased metadata/query work after Phase 6-7 changes.
- More explicit, safer query shapes.
- No snapshot-specific batching optimization yet.

Phase 7 status is COMPLETE with one explicit condition:

- Snapshot-creation regression is documented and accepted as temporary
   performance debt pending later phases.

#### Baseline policy

Do not regenerate `benchmark-baseline.json` yet.

Keep the current baseline until snapshot behavior is either optimized or
explicitly accepted long-term after later phase work (Phase 8/9), so the
regression remains visible.

## Phase 4 carry-over

Phase 4 introduced prepare/commit separation for correctness and future pipeline
work. The temporary two-pass store overhead identified in Phase 4 was resolved
in Phase 5 by moving logical-file hashing into the preparation pass.

**Status:** Phase 4 carry-over debt addressed in Phase 5  
**Resolved item:** two-pass file hash + chunk preparation overhead

## Phase 5

Phase 5 removes the Phase 4 two-pass store overhead by computing the logical
file hash during chunk preparation. The prepare/commit boundary remains intact:
preparation is CPU/read-side only, while commit remains sequential and ordered.

Latest local benchmark compare against the v1.6 small baseline (Postgres,
deterministic mode, threshold=20) now reports these Phase 5 store outcomes:

- store-large-file: improved
- store-mixed-dataset: improved
- store-many-small-files: improved

Notes:

- official small compare commands (workers=1 and workers=4) are currently green
   against `benchmark-baseline.json` at threshold 20.
- focused repeat harness runs still show medium-profile variability and should
   be treated as directional diagnostics, not baseline-gate replacements.

### Phase 5 guardrails (do not change)

Phase 5 optimization work must not alter core compatibility or commit-safety
contracts. Specifically, do not:

- parallelize chunks inside a file
- change chunk boundaries
- change hash algorithms
- change logical file identity
- change DB schema
- change container format
- change rollback/fsync semantics
- batch commits across files
- weaken prepare/commit separation

## Phase 6 -- Restore read-path optimization

Phase 6 optimizes restore-side metadata loading and I/O behavior while preserving
pin/unpin safety, deterministic chunk ordering, and byte-identical restore output.

No storage format, schema, GC, snapshot, or chunker behavior changes are introduced.

The restore flow has unmissable safety checkpoints that must remain; optimizations
can only reduce overhead **between** them, not eliminate them.

### Restore flow (current)

```text
1. STAGE: Resolve restore target
   - Input: restoration target (file ID or path)
   - Output: RestoreDescriptor with logical_file_id, path, metadata flags

2. STAGE: Pin chunks (protect from GC)
   - Query: SELECT ... FROM file_chunk ... WHERE logical_file_id = ? ORDER BY chunk_order
   - Action: UPDATE chunk SET pin_count = pin_count + 1 WHERE id = ?
   - Guarantee: ✓ Pin **before** performing any read or restore work
   - Guarantee: ✓ Ordered query ensures proper chunk visibility and sequence

3. STAGE: Load logical file metadata
   - Query: SELECT original_name, file_hash FROM logical_file WHERE id = ?
   - Output: expected file hash (for integrity check at end)

4. STAGE: Load ordered chunk recipe
   - Query: SELECT ... FROM file_chunk [WITH chunks/blocks] ORDER BY chunk_order
   - Output: restoreChunkRow list (container location, offsets, hashes, codec)

5. STAGE: For each chunk (ordered iteration)
   - Validate: chunk_order is monotonically contiguous
   - Locate: container file + block offset
   - Read: io operations to fetch compressed block from container
   - Decode: decompress/decrypt block using codec + nonce + key
   - Verify: SHA-256: computed hash == expected chunk hash
   - Append: plaintext bytes to temporary output file
   - Update: running file hash (SHA-256)

6. STAGE: Finalize and commit to destination
   - Fsync: temporary output file to ensure durability
   - Rename: atomic replace of temporary file with target path
   - Fsync: directory metadata to ensure rename is durable
   - Guarantee: ✓ Final hash == expected file hash (catch corruption early)

7. STAGE: Unpin chunks (allow GC)
   - Action: UPDATE chunk SET pin_count = pin_count - 1 WHERE id = ?
   - Guarantee: ✓ Unpin **after** restore completes or fails (via defer)
   - Guarantee: ✓ Even on error, chunks are unpinned for cleanup

8. STAGE: Apply physical metadata (optional)
   - Set: file mode, mtime, uid, gid if metadata is present and not skipped
```

### Design principles (not to be violated)

**One ordered chunk recipe per file (O(1) DB queries):**

The restore flow loads all chunk metadata for a file in a **single ordered query**
during STAGE 2-4. This design principle is performance-critical and must be
preserved across all optimizations:

- The query joins `file_chunk`, `chunk`, `blocks`, and optionally `container`
- Result is sorted by `chunk_order ASC` (deterministic order guarantee)
- All chunk metadata is pre-fetched: offsets, sizes, hashes, codecs, container locations
- STAGE 5b (chunk-by-chunk loop) reads from pre-loaded rows with **zero additional DB queries**
- This ensures O(n) file I/O + CPU work per file, and O(1) DB trips per file

**Do NOT refactor this into:**

- per-chunk lookup patterns (would increase DB queries to O(n) per file)
- lazy-load or streaming patterns (loses tuple prefetching, adds per-chunk latency)
- separate queries for offsets vs hashes vs codecs (violates cache locality)

This constraint is automatically preserved by keeping the current query structure;
Phase 6 optimizations can only affect the computation/verification path, not the
recipe loading strategy.

### Optimization scope (Phase 6)

Optimizations that respect the above flow and safety guarantees:

1. **Batch pin/unpin updates** — combine multiple chunk pins into one SQL statement
   while preserving transactional semantics and exact pin_count accuracy.
2. **Output buffering** — add write buffering to reduce syscalls, conditioned on
   final fsync + rename guarantees remaining unchanged.
3. **Container locality metrics** — measure open/close churn to guide future access
   patterns without changing sequential read semantics.
4. **Decode-path micro-benchmarks** — add Go benchmarks to isolate codec/hash
   overhead from end-to-end benchmark noise.

### Optimization scope (NOT Phase 6)

Optimizations that **cannot** be applied in Phase 6 without explicit safety re-review:

- Parallelize restore writes yet
- Change restore output ordering
- Skip hash verification if it is currently performed
- Weaken `pin_count` semantics or reduce restore pin/unpin safety
- Cache container readers globally
- Let GC delete unpinned chunks during restore
- Change snapshot restore semantics
- Introduce a schema migration unless benchmark evidence proves it necessary
- Alter file path reconstruction behavior
- Defer fsync/rename (breaks durability on crash)
- Batch unpin before restore completes (loses fail-safe cleanup semantics)

### Suggested implementation order

The recommended execution order for Phase 6 restore work is:

1. Profile restore benchmark scenarios.
2. Document current restore flow.
3. Add internal `restoreRecipe` / `restoreChunk`.
4. Load ordered recipe once per file.
5. Add defensive chunk-order validation.
6. Add restore-local container reader cache.
7. Add buffered output writes.
8. Remove unnecessary byte copies.
9. Preserve and test pin/unpin behavior.
10. Add restore determinism tests.
11. Run full test suite.
12. Run benchmark compare.
13. Update docs.

### Phase 6 completion checklist

Use this checklist before considering Phase 6 complete.

#### Profiling

- restore baseline captured before changes
- restore-large-file reviewed
- restore-many-files reviewed
- mixed workload coverage reviewed and documented
- query count/open count hotspots identified

#### Architecture

- internal restore recipe type added
- ordered chunk recipe loaded once per file where feasible
- defensive chunk-order validation added
- restore-local reader cache added if beneficial
- buffered output writes added if beneficial
- no global restore cache introduced

#### Correctness

- restored bytes unchanged
- restored tree hash unchanged
- logical file hash validation still passes
- snapshot restore behavior unchanged
- partial/filter restore behavior unchanged
- restore after GC still works
- empty-file restore unchanged

#### Safety

- chunks pinned before restore reads
- chunks unpinned after successful restore
- chunks unpinned after failed restore
- no stale pins after error
- GC cannot remove chunks during restore
- no fsync semantics weakened if restore had them
- no container lifecycle changes

#### Performance

- repeated per-chunk DB lookups reduced or confirmed absent
- repeated container open/close reduced or confirmed absent
- unnecessary byte copies reduced
- restore-large-file improved or documented neutral
- restore-many-files improved or documented neutral
- no major regression in store/snapshot/GC scenarios

#### Tests

- restore recipe ordering test added
- reader cache lifecycle test added if cache is implemented
- pin/unpin failure test added
- restore twice produces identical tree hash
- snapshot restore determinism test passes
- restore after GC test passes
- full adversarial suite passes
- go test ./... passes

#### Documentation

- Phase 6 benchmark note added
- before/after restore results documented
- any neutral result documented honestly
- no claim of unsafe restore parallelism added

### Phase 6 Step 12 benchmark matrix

The full Step 12 matrix completed successfully:

- small, workers=1
- small, workers=4
- medium, workers=1
- medium, workers=4

For the medium workers=4 run, `COLDKEEP_DB_OPERATION_TIMEOUT_MS=1800000`
was used to avoid timeout noise during the benchmark window.

#### Restore results

Small dataset:

- restore-large-file improved by ~19.7% duration / ~24.6% throughput
- restore-many-files improved by ~18.1% duration / ~22.1% throughput

Medium dataset:

- restore-large-file was effectively neutral: +0.53% duration
- restore-many-files was effectively neutral: +0.68% duration

#### Mixed workload note

There is currently no `restore-mixed-dataset` benchmark scenario.
The closest available mixed scenario is `store-mixed-dataset`.

#### Interpretation

Phase 6 improved restore behavior clearly on the small dataset and remained
neutral on the medium dataset. No major restore regression was observed.

### Phase 6 remaining checks

Before closing Phase 6, confirm:

- restore recipe/order tests pass
- reader cache lifecycle tests pass, if cache was added
- pin/unpin success and failure tests pass
- restore twice produces identical tree hash
- snapshot restore determinism still passes
- restore after GC still passes
- full adversarial suite passes
- go test ./... passes

Once those are green, Phase 6 is complete.

## Phase 7 Priority 3 -- Restore/store indexes

Decision: do not add new restore/store indexes.

Rationale: current schema coverage already includes the needed access paths:

- `file_chunk(logical_file_id, chunk_order)`
- `logical_file(file_hash, total_size)`
- `chunk(chunk_hash, size)`

## Phase 8 -- Conservative I/O Optimization

Phase 8 optimizes I/O behavior while preserving crash-safety guarantees.
The ordering remains:

`write bytes -> flush/fsync -> publish metadata`

No storage format, schema, GC, snapshot, or restore semantics changed.

### Safe optimizations list (Phase 8 candidates)

| Candidate | Status |
| --- | --- |
| operation-scoped I/O metrics | implemented |
| prepared statement reuse for snapshot rows | implemented |
| transaction-local snapshot batching | implemented |
| writer-scoped container handle reuse | implemented |
| buffered writes with flush-before-fsync | implemented |
| remove duplicate stat/path normalization | implemented |
| restore reader cleanup | implemented |
| remove redundant copies | partial (ongoing) |

### Dangerous optimizations to avoid (Phase 8)

Do not apply these in Phase 8:

- async container writes
- metadata publish before fsync
- global file descriptor cache
- cross-worker shared writer
- batch fsync across independent logical commits
- change container layout
- change chunk/block layout
- skip verification
- parallel restore output writes
- defer rollback cleanup

Some of these may be valid in a future engine model, but they are out-of-scope
for the current v1.x crash-safety and compatibility model.

### Phase 8 benchmark protocol

After each meaningful change:

```bash
go run ./cmd/coldkeep benchmark run --dataset small --workers 1 --output json
go run ./cmd/coldkeep benchmark run --dataset small --workers 4 --output json
```

End-of-phase matrix:

```bash
go run ./cmd/coldkeep benchmark run --dataset medium --workers 1 --output json
go run ./cmd/coldkeep benchmark run --dataset medium --workers 4 --output json
```

Recorded outputs for Step 12:

- `.benchmarks/step12/small_w1.json`
- `.benchmarks/step12/small_w4.json`
- `.benchmarks/step12/medium_w1.json`
- `.benchmarks/step12/medium_w4.json`

### Suggested implementation order (Phase 8)

1. Capture focused I/O benchmark baseline.
2. Add operation-scoped I/O metrics.
3. Inspect append/fsync/open-close paths.
4. Add writer-scoped handle reuse if not already present.
5. Add safe buffered writes if beneficial.
6. Remove redundant fsyncs only inside one append operation.
7. Optimize snapshot metadata batching/prepared statements.
8. Remove duplicate stat/path normalization.
9. Validate restore cache lifecycle.
10. Add fault-path tests.
11. Run adversarial/recovery tests.
12. Run benchmark matrix.
13. Document accepted/rejected optimizations.

### Metrics before/after (focus scenarios)

Before = `benchmark-baseline.json` (v1.6 small baseline)
After = Phase 8 Step 12 outputs (small workers 1 and 4)

| Scenario | Baseline small w1 (ms) | Phase 8 small w1 (ms) | Delta vs baseline | Phase 8 small w4 (ms) | Delta vs baseline |
| --- | ---: | ---: | ---: | ---: | ---: |
| `snapshot-creation` | 520 | 861 | +66% | 423 | -19% |
| `store-large-file` | 1608 | 3029 | +88% | 2999 | +87% |
| `store-mixed-dataset` | 399 | 741 | +86% | 310 | -22% |
| `gc-after-churn` | 2624 | 3405 | +30% | 2193 | -16% |
| `restore-large-file` | 1790 | 3244 | +81% | 3211 | +79% |

Medium matrix (absolute results, end-of-phase):

| Scenario | Medium w1 (ms) | Medium w4 (ms) |
| --- | ---: | ---: |
| `snapshot-creation` | 67876 | 36573 |
| `store-large-file` | 33076 | 34291 |
| `store-mixed-dataset` | 65002 | 38402 |
| `gc-after-churn` | 28733 | 21991 |
| `restore-large-file` | 37120 | 37416 |

### Optimizations accepted

- Step 8: store-path metadata flow cleanup (`preparedFile.PhysicalMetadata` carry path)
- Step 9: restore reader-cache close aggregation (`errors.Join`), buffered-writer finalization guard, and dead API parameter cleanup
- Step 10: failure-path coverage for write/flush/fsync/rollback/snapshot-batch/close-on-error invariants
- Step 11: crash-safety recovery chain validation (recovery -> verify -> restore -> GC)
- Step 12: benchmark-after-change cadence (small) plus end matrix (medium)

### Optimizations rejected

- Restore parallelism in Phase 8 (risk to deterministic ordering and crash boundary behavior in v1.x)
- Global/cross-operation restore reader cache (lifetime and quarantine/GC coupling risk)
- Any throughput shortcut that weakens fsync/rollback durability boundaries

### Remaining debt

- `store-large-file` and `restore-large-file` remain slower than the v1.6 small baseline in this environment.
- Additional restore-path SQL efficiency work (for example unpin batching) is still pending.
- Restore parallelism remains deferred to a later release with explicit ordering and memory-budget design.
- Re-evaluate `store-large-file` degradation under an isolated benchmark window to separate architectural cost from local environment noise.

This priority is an explicit no-op to avoid redundant index churn.

## Phase 7 Priority 4 -- GC index proposal

Decision: do not add `chunk(container_id)`.

Rationale: `container_id` is stored on `blocks`, not on `chunk`, so a
`chunk(container_id)` index is invalid for this schema.

GC behavior remains unchanged unless query-plan evidence shows a real hotspot.
