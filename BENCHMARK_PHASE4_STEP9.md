# Phase 4 Step 9 - Benchmark Results

Status: archived historical benchmark report (Phase 4 evidence record).

This file is a phase-specific benchmark report.
If you are new to the project, read [README.md](README.md) first and then
[docs/benchmarking.md](docs/benchmarking.md) for the canonical benchmark model
and current release framing.

## Executive Summary

Benchmarked the prepared/commit chunk path optimization (Phase 4 Step 9) using the small dataset on April 29, 2026.

The Phase 4 optimization separates file storage into two phases:

- **Preparation Phase**: Materializes all chunk metadata deterministically (hashing, sizing) before DB mutations
- **Commit Phase**: Processes pre-computed chunks sequentially and deterministically (no re-hashing, no re-allocation)

## Benchmark Configuration

**Environment**: Linux, in-process SQLite
**Datasets**: Small (verified; Medium requires DB_HOST configuration)
**Workers**: 1 and 4

## Key Store Operation Results

### Small Dataset - Single Worker (1 worker)

| Operation | Duration (ms) | Throughput (MB/s) | Files | Total Bytes |
| --------- | ------------- | ----------------- | ----- | ----------- |
| **store-large-file** | 2,301 | 6.95 | 1 | 16 MB |
| **store-mixed-dataset** | 571 | 4.33 | 20 | 2.5 MB |
| **store-many-small-files** | 1,295 | 0.075 | 100 | 100 KB |

### Small Dataset - Multi-Worker (4 workers)

| Operation | Duration (ms) | Throughput (MB/s) | Files | Total Bytes |
| --------- | ------------- | ----------------- | ----- | ----------- |
| **store-large-file** | 2,292 | 6.98 | 1 | 16 MB |
| **store-mixed-dataset** | 292 | 8.47 | 20 | 2.5 MB |
| **store-many-small-files** | 514 | 0.19 | 100 | 100 KB |

## Performance Analysis

### Large File Storage

- **Single worker**: 6.95 MB/s
- **Multi-worker (4x)**: 6.98 MB/s
- **Impact**: Minimal change, as expected for single large file (chunking overhead limited by sequential I/O)

### Mixed Dataset

- **Single worker**: 4.33 MB/s
- **Multi-worker (4x)**: 8.47 MB/s
- **Improvement**: +1.96x throughput with 4 workers (modest scaling efficiency)
- **Analysis**: Preparation phase parallelization benefit visible in mixed 20-file workload

### Many Small Files

- **Single worker**: 0.075 MB/s (1,295 ms for 100 KB)
- **Multi-worker (4x)**: 0.19 MB/s (514 ms for 100 KB)
- **Improvement**: +2.52x throughput with 4 workers
- **Analysis**: Per-file preparation overhead reduced; 4-worker parallelism effective for high-cardinality workloads

## Design Benefits (Realized in Results)

1. **Preparation Separation**: CPU-side work (chunking, hashing, data materialization) happens before DB mutations
2. **No Re-hashing**: Commit phase uses precomputed hashes; no redundant SHA256 computation
3. **No Re-allocation**: Chunk data payloads are captured immutably during preparation; commit phase avoids re-reading
4. **Worker Efficiency**: Cloned LocalWriter per worker + singleton SimulatedWriter enables realistic concurrent packing with thread-safe access

## Observed Metrics

**Store Operation Scaling**:

- Large file: Serial I/O dominates; chunking overhead minimal
- Mixed workload: 1.96x speedup suggests ~50% efficiency in 4-worker setup (expected for mixed sizes + DB transactions)
- Many small files: 2.52x speedup shows good parallelization for high-cardinality, low per-file computation

## Notes on Other Operations

As expected, restore/GC/snapshot improvements were not the focus of Phase 4:

- **Restore**: Primarily read-path bound; preparation optimization has no impact
- **GC**: Metadata queries and reference counting dominate; no chunk preparation benefit
- **Snapshot**: Query-focused; minimal store operation dependency

## Caveats

1. **Medium Dataset**: Requires `DB_HOST`, `DB_PORT`, `DB_USER` environment variables for determinism validation
2. **In-Process SQLite**: Results are for single-instance in-memory DB (PostgreSQL may show different profile)
3. **Repeat Count**: Small dataset runs single repeat (suitable for optimization validation, not production stress testing)

## Conclusion

Phase 4 Step 9 implementation is complete with comprehensive test coverage:

- ✅ 10 unit tests validating chunk preparation determinism, indexes, sizes, hashes, versions, and final file hash
- ✅ 1 integration test validating store graph equivalence and round-trip restore correctness
- ✅ Benchmark results show expected multi-worker scaling (modest to meaningful improvement for prepared/commit path)

The optimization correctly implements CPU-side preparation before DB mutations, enabling better parallelization efficiency in multi-worker scenarios while maintaining deterministic behavior.

## Step 13 - Optimization Decision Register (Keep/Reject/Defer)

This section locks explicit decisions for the optimization pass so choices are not re-litigated later.

### Keep

| Optimization | Decision | Why |
| ------------ | -------- | --- |
| Step 8: Store path single-stat flow with `preparedFile.PhysicalMetadata` as the carry path | **keep** | Removes redundant metadata-building path, keeps one-file/one-stat semantics, no behavior regression, and no stale cross-file cache. |
| Step 9: Restore reader cache cleanup hardening (`errors.Join` in cache close) | **keep** | Better failure visibility with no behavior change in success path. |
| Step 9: Restore finalization guard (`bufw = nil` after explicit flush) | **keep** | Prevents deferred flush against a closed handle on error/finalization paths; low complexity and safer close semantics. |
| Step 9: Remove unused restore cache `ctx` parameter | **keep** | Neutral performance and lower API noise; no semantic risk. |
| Step 10: Error-path and rollback validation tests | **keep** | Directly enforces durability and publish-boundary invariants; reduces regression risk in high-impact failure paths. |
| Step 11: Crash-safety validation (fault-injection + recovery/verify/restore/GC chain) | **keep** | Confirms failure isolation and post-recovery correctness for real operational flows. |
| Step 12: Benchmark cadence (`small` after meaningful changes, full `medium` matrix at end) | **keep** | Improves optimization governance and catches regressions earlier than end-only benchmarking. |

### Reject (Documented to Avoid Re-Litigation)

| Idea | Decision | Why Rejected |
| ---- | -------- | ------------ |
| Introduce restore parallelism in this pass | **reject** | High correctness risk for ordered replay and verification boundaries in current v1.x scope; not needed for this safety-first pass. |
| Add cross-operation/global reader cache for restore | **reject** | Handle lifetime and quarantine/GC interactions add operational risk; restore-local cache already captures the low-risk gain. |
| Relax fsync/rollback durability checks for throughput gains | **reject** | Violates the core durability invariant: no metadata may publish bytes that are not durably written. |

### Defer (Valuable, but Not v1.x Scope)

| Idea | Decision | Target Window | Why Deferred |
| ---- | -------- | ------------- | ------------ |
| Restore parallelism with deterministic ordered writer boundary | **defer** | v1.8/v1.10+ | Needs dedicated design for deterministic merge/order guarantees, memory budgeting, and stronger failure-model testing. |
| Restore unpin batch update optimization (`unpinRestoreChunksWithContext`) | **defer** | v1.8+ | Valuable SQL efficiency improvement, but current per-chunk correctness is stable and already validated. |
| Snapshot metadata batch insert path specialization beyond current prepared statement reuse | **defer** | v1.10+ | Potential throughput upside, but requires broader DB-backend contract and migration-safe perf validation. |

### Locked Invariant (Applies to All Keep/Reject/Defer Decisions)

Failure before durable write publish must never leave live metadata pointing at bytes that are not durably present.

## Phase 9 Step 6 - v1.7 vs v1.6 Baseline Comparison

Comparison source of truth:

- Baseline: `benchmark-baseline.json` (v1.6, small dataset, workers=1)
- Final baseline-comparable view: `benchmarks/v1.7/final-small-w1.json` (v1.7, small dataset, workers=1)
- Final scaled-operating view: `benchmarks/v1.7/final-small-w4.json` (v1.7, small dataset, workers=4)

No baseline rebasing was performed in this step.

For release review, keep both views in scope:

- `workers=1` is the strict apples-to-apples baseline comparison and remains slower in all 8 scenarios.
- `workers=4` better reflects the intended v1.7 operating model and wins 6 of 8 scenarios on the same small baseline.

### Delta Table - Baseline-Comparable View (`small`, workers=1)

| Scenario | Baseline (ms) | v1.7 Final (ms) | Delta vs Baseline |
| -------- | ------------: | --------------: | ----------------: |
| `store-large-file` | 2251 | 10483 | +366% |
| `store-many-small-files` | 1290 | 4902 | +280% |
| `store-mixed-dataset` | 563 | 2513 | +346% |
| `restore-large-file` | 2329 | 10641 | +357% |
| `restore-many-files` | 5016 | 15298 | +205% |
| `snapshot-creation` | 642 | 2984 | +365% |
| `gc-after-churn` | 2376 | 7314 | +208% |
| `stats-inspect` | 885 | 3998 | +352% |

### Delta Table - Scaled Release View (`small`, workers=4)

| Scenario | Baseline (ms) | v1.7 Final (ms) | Delta vs Baseline |
| -------- | ------------: | --------------: | ----------------: |
| `store-large-file` | 2251 | 2271 | +1% |
| `store-many-small-files` | 1290 | 496 | -62% |
| `store-mixed-dataset` | 563 | 275 | -51% |
| `restore-large-file` | 2329 | 2368 | +2% |
| `restore-many-files` | 5016 | 4621 | -8% |
| `snapshot-creation` | 642 | 365 | -43% |
| `gc-after-churn` | 2376 | 1634 | -31% |
| `stats-inspect` | 885 | 613 | -31% |

### Classification

Baseline-comparable (`small`, workers=1) improved:

- none in the baseline-comparable (`small`, workers=1) release comparison

Baseline-comparable (`small`, workers=1) neutral:

- none in the baseline-comparable (`small`, workers=1) release comparison

Baseline-comparable (`small`, workers=1) regressed:

- `store-large-file`
- `store-many-small-files`
- `store-mixed-dataset`
- `restore-large-file`
- `restore-many-files`
- `snapshot-creation`
- `gc-after-churn`
- `stats-inspect`

Scaled release view (`small`, workers=4) improved:

- `store-many-small-files`
- `store-mixed-dataset`
- `restore-many-files`
- `snapshot-creation`
- `gc-after-churn`
- `stats-inspect`

Scaled release view (`small`, workers=4) neutral:

- none

Scaled release view (`small`, workers=4) regressed:

- `store-large-file`
- `restore-large-file`

Accepted neutral:

- none

Accepted remaining debt:

- The strict baseline-comparable (`small`, workers=1) view remains fully regressed and is documented rather than hidden.
- The intended v1.7 operating model (`small`, workers=4) still carries two small remaining regressions: `store-large-file` (+1%) and `restore-large-file` (+2%).
- Rationale: this release intentionally preserves conservative durability, rollback, recovery, and deterministic behavior boundaries while deferring higher-risk throughput work (for example restore parallelism and deeper SQL batching).

### Release-policy note

Regressions are explicitly documented and not hidden by rebasing the baseline in this step. The added `workers=4` table is supplementary release framing, not a baseline substitution. Any baseline refresh should be an intentional, explicitly called out release decision.
