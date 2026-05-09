# Coldkeep Benchmarking

v1.7 introduces repository-level benchmarks to measure performance without
changing correctness guarantees.

Release framing for v1.7:

- Performance is improved through controlled execution and conservative tuning.
- v1.7 is not a fully concurrent daemon design.
- No storage format change is introduced.
- No schema-breaking change is introduced.
- Restore determinism, GC safety, and snapshot semantics remain preserved.
- Migration note: none required for v1.7.

## Performance model change in v1.7

v1.7 introduces a deterministic, scalable execution model.

Single-worker performance may be lower than v1.6 in some scenarios due to
additional correctness boundaries and explicit execution structure.

However, multi-worker execution shows significant improvements across
real-world workloads and is the recommended operating mode for performance
evaluation in v1.7.

## Running benchmarks

Phase 8 benchmark execution is script-only for v1.8 release hardening.

The `coldkeep benchmark` command is available in the shipped CLI for ad-hoc
performance evaluation; it supports `benchmark run` (repeatable workload tests
with configurable worker count) and `benchmark chunkers` (chunk-algorithm
comparison). Use the retained helper scripts under `scripts/run_phase8_*.sh`
and `scripts/compare_phase8_*.py` for large-scale decision-grade matrix runs
requiring strict isolation and metrics collection.

Support level summary:

- `coldkeep benchmark run|chunkers`: supported CLI surface for local/ad-hoc
   benchmarking and regression inspection.
- `scripts/run_phase8_*.sh` + `scripts/compare_phase8_*.py`: release
   decision-grade harness for the v1.8 block-size decision record.
- Historical phase reports (`BENCHMARK_PHASE4_STEP9.md`,
   `BENCHMARK_PHASE8_BLOCK_SIZE_DECISION.md`): archived evidence docs, not live
   implementation specs.

```bash
# Inspect/resume the packed block-size matrix
scripts/run_phase8_blocksize_matrix.sh --list-missing

# Resume or execute the matrix
scripts/run_phase8_blocksize_matrix.sh

# Summarize completed artifacts
python3 scripts/summarize_phase8_blocksize.py --input-dir tmp/bench_phase8

# Focused sequence runners remain available for single-slice experiments
scripts/run_phase8_store_sequence.sh <BLOCK_MB> <DATASET_PATH> <RUN_ID>
scripts/run_phase8_restore_sequence.sh <BLOCK_MB> <DATASET_PATH> <RUN_ID>
scripts/run_phase8_dedup_sequence.sh <BLOCK_MB> <DATASET_ROOT> <RUN_ID>
scripts/run_phase8_gc_sequence.sh <BLOCK_MB> <DATASET_ROOT> <RUN_ID>

# Compare focused result documents
python3 scripts/compare_phase8_dedup_results.py <result_1m.json> <result_2m.json>
python3 scripts/compare_phase8_gc_results.py <result_1m.json> <result_2m.json>
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

Optional contention-tuning environment variables (recommended for shared CI runners):

```bash
# Increase lock retry resilience under transient PostgreSQL row-lock contention.
export COLDKEEP_CONTAINER_LOCK_RETRY_ATTEMPTS=12
export COLDKEEP_CONTAINER_LOCK_RETRY_BASE_WAIT_MS=15
export COLDKEEP_CONTAINER_LOCK_RETRY_MAX_WAIT_MS=900
```

Defaults if unset: attempts=10, base wait=10ms, max wait=500ms.
Bounds are enforced: attempts [1,64], base wait [1ms,2000ms], max wait [base,5000ms].

### Benchmark output examples

Human table output now includes configured vs effectively used workers:

```text
Benchmark run (small preset, repeat=1)
Execution: workers=4 pipeline_depth=1 deterministic=true

CASE                    TIME   MB/s  W_CFG  W_USED  FILES
store-large-file        2.9s   110   4      4       1
store-many-small-files  4.8s   97    4      4       1200
snapshot-creation       0.6s   0     4      1       0
```

JSON output exposes both per-case worker usage and an aggregate
`execution_stats` block:

```json
{
   "status": "ok",
   "command": "benchmark",
   "data": {
      "dataset": "small",
      "execution": {
         "store_folder_workers": 4,
         "pipeline_depth": 1,
         "deterministic": true
      },
      "execution_stats": {
         "workers_used": 4,
         "total_files": 2400,
         "total_bytes": 123456789
      },
      "rows": [
         {
            "case": "store-many-small-files",
            "execution_stats": {
               "workers_used": 4,
               "total_files": 1200
            }
         }
      ]
   }
}
```

## Current baseline

The repository now maintains two official v1.9 baseline artifacts for the
recommended packed production family (`aes-gcm` encryption):

- `benchmarks/v1.9/baselines/benchmark-baseline-v1.9-packed-aes-gcm-none-small-w1-r1.json`
  - Baseline A (uncompressed): `packed + aes-gcm + none`
  - Purpose: protect v1.8 behavior and detect non-compression regressions.
- `benchmarks/v1.9/baselines/benchmark-baseline-v1.9-packed-aes-gcm-zstd-small-w1-r1.json`
  - Baseline B (compressed): `packed + aes-gcm + zstd`
  - Purpose: measure compression tradeoffs and detect compression regressions.

Comparability contract for these baselines:

- same dataset preset (`small`)
- same repeat count (`1`)
- same execution profile (`workers=1`, deterministic mode)
- same benchmark case set
- same logical totals (`total_files`, `total_bytes`)

The machine-readable manifest is stored at:

- `benchmarks/v1.9/baselines/baseline-manifest-v1.9.json`

It records file checksums, comparability validation, and per-case compressed vs
uncompressed deltas.

To regenerate v1.9 baseline artifacts after intentional benchmark changes:

```bash
COLDKEEP_CODEC=aes-gcm COLDKEEP_COMPRESSION=none \
  coldkeep benchmark run --dataset small --repeat 1 --workers 1 --output json \
  > benchmarks/v1.9/baselines/benchmark-baseline-v1.9-packed-aes-gcm-none-small-w1-r1.json

COLDKEEP_CODEC=aes-gcm COLDKEEP_COMPRESSION=zstd \
  coldkeep benchmark run --dataset small --repeat 1 --workers 1 --output json \
  > benchmarks/v1.9/baselines/benchmark-baseline-v1.9-packed-aes-gcm-zstd-small-w1-r1.json
```

To detect regressions against each baseline:

```bash
# Uncompressed production path regression check
COLDKEEP_CODEC=aes-gcm COLDKEEP_COMPRESSION=none \
  coldkeep benchmark run --dataset small --repeat 1 --workers 1 \
  --compare benchmarks/v1.9/baselines/benchmark-baseline-v1.9-packed-aes-gcm-none-small-w1-r1.json \
  --threshold 20

# Compressed production path regression check
COLDKEEP_CODEC=aes-gcm COLDKEEP_COMPRESSION=zstd \
  coldkeep benchmark run --dataset small --repeat 1 --workers 1 \
  --compare benchmarks/v1.9/baselines/benchmark-baseline-v1.9-packed-aes-gcm-zstd-small-w1-r1.json \
  --threshold 20
```

For one-command capture + validation, use:

```bash
scripts/run_v19_baseline_pair.sh
```

Legacy reference baselines at repository root (`benchmark-baseline*.json`) are
retained for historical v1.6/v1.7 context.

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

## Regression Thresholds (v1.9)

Benchmarks are now actionable through defined regression thresholds. Thresholds are
mode-specific (uncompressed vs. compressed) and case-specific, balancing detection
sensitivity with normal run-to-run variance.

**Official policy:** See [benchmarks/v1.9/regression-thresholds.yaml](../benchmarks/v1.9/regression-thresholds.yaml)
for the authoritative threshold definition.

### Uncompressed mode (packed + aes-gcm + none)

**Strict thresholds:**

| Metric | Default | Rationale |
| --- | --- | --- |
| Store throughput regression | > 5% | Production baseline; strict monitoring required |
| Restore throughput regression | > 5% | Critical read path; regressions must be justified |
| Metadata operation regression | > 3% | snapshot-creation, gc-after-churn, stats-inspect |
| Memory increase | > 10% | Not yet enforced via CLI but monitored |

Any regression exceeding these thresholds **fails CI** and must be investigated or reverted.

### Compressed mode (packed + aes-gcm + zstd)

**Stage 1 (v1.9–v1.10): Warning thresholds only**

Compressed thresholds account for natural compression overhead observed in v1.9 baseline:
- `store-large-file`: 13.8% duration overhead typical
- `snapshot-creation`: 27.2% duration overhead typical
- `stats-inspect`: 16.7% duration overhead typical

Warning thresholds are set ~5–10% above baseline to detect real regressions:

| Operation | Store/Restore | Metadata | Verify |
| --- | --- | --- | --- |
| **Throughput warning** | 15–25% | 25–35% | 15% |
| **Duration warning** | 15–25% | 25–35% | 15% |

Compressed-mode regressions **log warnings** but do not fail CI in v1.9. This permits
natural variance while capturing trends for future hardening phases.

**Per-case thresholds (compressed):**

| Case | Duration Warning | Throughput Warning |
| --- | --- | --- |
| store-large-file | 25% | 25% |
| store-many-small-files | 20% | 20% |
| store-mixed-dataset | 25% | 25% |
| restore-large-file | 15% | 15% |
| restore-many-files | 15% | 15% |
| snapshot-creation | 35% | 35% |
| gc-after-churn | 15% | 15% |
| stats-inspect | 25% | 25% |
| verify-system-deep | 15% | 15% |

**Future plan (v1.11+):**
- Analyze compression benefit vs. performance cost
- Convert warnings to hard-fail thresholds if justified
- Require cost-benefit analysis for any compression-mode exception

### Local development workflow

Use `--compare` with thresholds matching your change context:

```bash
# Against uncompressed baseline (recommended for most work)
COLDKEEP_CODEC=aes-gcm COLDKEEP_COMPRESSION=none \
  ./coldkeep benchmark run --dataset small --repeat 1 --workers 1 \
  --compare benchmarks/v1.9/baselines/benchmark-baseline-v1.9-packed-aes-gcm-none-small-w1-r1.json \
  --threshold 5

# Against compressed baseline (if working on compression features)
COLDKEEP_CODEC=aes-gcm COLDKEEP_COMPRESSION=zstd \
  ./coldkeep benchmark run --dataset small --repeat 1 --workers 1 \
  --compare benchmarks/v1.9/baselines/benchmark-baseline-v1.9-packed-aes-gcm-zstd-small-w1-r1.json \
  --threshold 20
```

The `--compare` flag uses single-run thresholds; for production gates involving multiple
runs, use the helper script:

```bash
# Regenerate baselines and validate against current code
scripts/run_v19_baseline_pair.sh --threshold 5
```

### CI policy

CI applies thresholds as follows:

1. **Uncompressed mode (hard fail):** CI runs benchmark against
   `benchmark-baseline-v1.9-packed-aes-gcm-none-*` at threshold 5%. Any regression
   exceeding 5% fails the CI run and blocks merge.

2. **Compressed mode (warnings only):** CI runs benchmark against
   `benchmark-baseline-v1.9-packed-aes-gcm-zstd-*` at threshold 20%. Violations log
   warnings but do not fail CI in v1.9.

3. **Measurement variance:** Small-dataset single-run variance is expected (±2%);
   real regressions typically exceed 3–5% and are reliably detected.

### Updating thresholds

When to update thresholds:

1. **Performance improvement:** If you improve performance (e.g., new optimization),
   the baseline becomes a new regression floor; regenerate baselines:
   ```bash
   scripts/run_v19_baseline_pair.sh --threshold 100
   ```

2. **Justifiable slowdown:** If a change introduces acceptable slowdown (e.g., new
   feature, safety boundary), document the cost-benefit analysis and increase the
   per-case threshold if warranted. Update `regression-thresholds.yaml` with rationale.

3. **Phase transition:** Moving from v1.10 to v1.11 to apply compressed hard-fail
   thresholds requires analysis of compression impact trends and explicit approval
   from the release engineering team.

## Step 6.7 — Deterministic Restore Across Matrix (v1.9)

Core roadmap guarantee: **Compression must not affect deterministic restore.**

### Validation scope

Step 6.7 validates that the same file produces byte-identical restores regardless of:

- **Compression modes:** uncompressed (`none`) vs. compressed (`zstd`)
- **Encryption codecs:** unencrypted (`plain`) vs. encrypted (`aes-gcm`)
- **Repository state:** baseline, after GC, after snapshot operations
- **Repeated operations:** multiple runs, different execution paths

### Test matrix

| Compression | Encryption | Baseline | After GC | After Snapshots | Repeated Runs |
| --- | --- | --- | --- | --- | --- |
| none | plain | ✓ | ✓ | ✓ | ✓ |
| none | aes-gcm | ✓ | ✓ | ✓ | ✓ |
| zstd | plain | ✓ | ✓ | ✓ | ✓ |
| zstd | aes-gcm | ✓ | ✓ | ✓ | ✓ |

### Validation checklist

- ✓ **Byte-identical restore everywhere:** All modes produce exact byte-for-byte identical output
- ✓ **Repeated restores identical:** Multiple restores of the same file produce identical hashes
- ✓ **Restore after GC identical:** Restores after garbage collection produce identical output
- ✓ **Restore after snapshots identical:** Restores after snapshot creation/deletion produce identical output
- ✓ **Same input → same logical output independent of compression/encryption:** Compression is metadata-only; it never affects restored bytes

### Implementation

Deterministic restore matrix validation runs in tests/adversarial/:

```bash
# Run matrix tests (requires COLDKEEP_TEST_DB=1 and database setup)
COLDKEEP_TEST_DB=1 go test ./tests/adversarial -run "TestStep67" -v

# Test names:
# - TestStep67DeterministicRestoreCompressionMatrix: Core matrix (4 modes × 8 scenarios)  
# - TestStep67CrossModeDeterminism: Cross-mode consistency (identical restores across all 4 modes)
```

### Guarantees for operators

1. **Compression is metadata-only:** Choosing `COLDKEEP_COMPRESSION=zstd` does not affect restored file content; it only affects storage efficiency.
2. **Repository config changes don't affect existing files:** Changing the repository-level compression setting does not re-compress or alter existing stored files.
3. **Read-only operations are always safe:** Snapshots, GC, and other maintenance never modify file content during restore.
4. **Deterministic output is cryptographically validated:** Each restore operation verifies content hashes and encryption state; silent corruption is not possible.

## Step 6.8 — Revalidate Dedup Semantics (v1.9)

Core roadmap guarantee: **Compression must not reduce dedup effectiveness.**

### Validation scope

Step 6.8 validates that deduplication behavior remains identical regardless of compression mode.
The logical dedup graph (chunk identities, file-chunk relationships, reuse patterns) must be
identical between uncompressed and compressed modes. Only physical storage representation changes.

Step 6.8 validates:

- **Chunk identities unchanged:** The same file content produces the same chunk hash
- **Dedup graph unchanged:** File-chunk relationships are identical across compression modes
- **No duplicate chunk storage introduced:** Identical data is deduplicated identically regardless of compression
- **Restore unchanged:** Files restored from either mode produce identical content

### Test matrix

| Compression | Encryption | Duplicate Files | Partial Overlap | Modified Versions |
| --- | --- | --- | --- | --- |
| none | plain | ✓ | ✓ | ✓ |
| none | aes-gcm | ✓ | ✓ | ✓ |
| zstd | plain | ✓ | ✓ | ✓ |
| zstd | aes-gcm | ✓ | ✓ | ✓ |

### Validation checklist

- ✓ **Chunk count identical:** Exact same number of unique chunks across compression modes
- ✓ **Chunk hashes identical:** All chunk identifiers match byte-for-byte
- ✓ **File-chunk relationships identical:** Mapping of files to chunks is the same
- ✓ **Dedup ratio identical:** Total stored size / unique chunk size yields identical ratio
- ✓ **File references identical:** Number of logical file references is the same
- ✓ **Restores identical:** Files restored from either mode produce byte-identical content

### Implementation

Dedup semantics matrix validation runs in tests/adversarial/:

```bash
# Run dedup semantics tests (requires COLDKEEP_TEST_DB=1 and database setup)
COLDKEEP_TEST_DB=1 go test ./tests/adversarial -run "TestStep68" -v

# Test names:
# - TestStep68DedupSemanticCompressionIndependence: Core matrix (2 encryptions × 2 compressions)
# - TestStep68CrossCompressionDedupConsistency: Full 4-mode consistency (all mode combinations)

# Example with extended output
COLDKEEP_TEST_DB=1 go test ./tests/adversarial -run "TestStep68DedupSemanticCompressionIndependence" -v -race
```

Test payloads include:

- **Duplicate files:** Identical content stored multiple times (should deduplicate completely)
- **Partially overlapping:** Files with common prefix/suffix (should chunk-deduplicate section boundaries)
- **Modified versions:** Content with incremental changes (should deduplicate unchanged sections)
- **Different content:** Unique files (baseline for dedup ratio calculation)

### Guarantees for operators

1. **Dedup effectiveness is independent of compression:** Enabling `COLDKEEP_COMPRESSION=zstd` does not reduce dedup effectiveness; files using identical content still share chunks.
2. **Logical dedup graph is invariant:** The set of unique chunks and their relationships is identical regardless of compression setting.
3. **Only physical storage representation changes:** Compressed chunks occupy less disk space but represent the same logical dedup structure.
4. **Cross-compression queries are safe:** Repositories with mixed compression modes (if supported by migration) maintain consistent dedup semantics.

## Step 6.9 — Revalidate GC Safety Across Matrix (v1.9)

Core roadmap guarantee: **GC must remain transform-agnostic and safe.**

### Validation scope

Step 6.9 validates garbage collection safety across repository classes where block
transforms and metadata paths vary:

- **Compressed repositories:** `zstd` storage path
- **Uncompressed repositories:** `none` storage path
- **Mixed repositories:** both `none` and `zstd` data in one repository
- **Encrypted repositories:** `aes-gcm` with compression path coverage
- **Legacy repositories:** legacy metadata shape (no packed block metadata rows)

Each class validates:

- **Live block preservation:** GC must not delete reachable data
- **Orphan deletion:** unreachable chunks/blocks must be reclaimable
- **Restore after GC:** live files restore with original hashes
- **Verify after GC:** repository verification remains healthy

### Test matrix

| Repository Class | Compression Path | Encryption | Legacy Shape | Live Preserve | Orphan Remove | Restore After GC | Verify After GC |
| --- | --- | --- | --- | --- | --- | --- | --- |
| compressed | zstd | plain | no | ✓ | ✓ | ✓ | ✓ |
| uncompressed | none | plain | no | ✓ | ✓ | ✓ | ✓ |
| mixed | none + zstd | plain | no | ✓ | ✓ | ✓ | ✓ |
| encrypted | zstd | aes-gcm | no | ✓ | ✓ | ✓ | ✓ |
| legacy | none | plain | yes | ✓ | ✓ | ✓ | ✓ |

### Validation checklist

- ✓ **Live compressed blocks preserved:** compressed live data remains reachable after GC
- ✓ **Orphaned compressed blocks removable:** unreachable compressed blocks are reclaimed
- ✓ **Restore after GC correct:** restored live files match pre-GC SHA-256
- ✓ **Verify after GC correct:** `verify system` passes after GC in all matrix classes

### Implementation

GC safety matrix validation runs in tests/adversarial/:

```bash
# Run GC safety matrix tests (requires COLDKEEP_TEST_DB=1 and database setup)
COLDKEEP_TEST_DB=1 go test ./tests/adversarial -run "TestStep69" -v

# Test names:
# - TestStep69GCSafetyAcrossMatrix: compressed/uncompressed/mixed/encrypted/legacy classes
```

Implementation details:

- Performs GC dry-run and real-run in each repository class.
- Asserts orphan chunk IDs are removed post-GC.
- Asserts live chunk IDs remain present post-GC.
- Restores live files and validates SHA-256 hashes.
- Runs `verify system` post-GC (`VerifyStandard`) as final safety gate.

## Step 6.10 — Validate Mixed Repository Stability (v1.9)

Core roadmap guarantee: **Mixed repositories are normal behavior and must remain stable.**

### Validation scope

Step 6.10 validates repository stability through an end-to-end evolution path:

- **v1.8 blocks path:** legacy metadata/read path
- **Phase 5 uncompressed path:** packed metadata with `compression=none`
- **zstd path:** packed metadata with `compression=zstd`
- **encryption mode transitions:** `plain` and `aes-gcm` blocks in one repository
- **store-if-smaller fallback path:** zstd-configured writes that safely store as `none` when compression expands payloads

After evolution, validation executes:

- **restore everything**
- **verify everything**
- **GC everything**
- **stats everything**

### Validation checklist

- ✓ **Mixed repositories stable:** legacy + packed + encrypted + compressed data co-exist safely
- ✓ **Per-block metadata fully sufficient:** packed block metadata is complete and read-safe
- ✓ **Repository defaults never required for reads:** restores remain correct after changing repository compression defaults

### Implementation

Mixed repository stability validation runs in tests/adversarial/:

```bash
# Run Step 6.10 (requires COLDKEEP_TEST_DB=1 and database setup)
COLDKEEP_TEST_DB=1 go test ./tests/adversarial -run "TestStep610" -v

# Test names:
# - TestStep610MixedRepositoryStability
```

Key assertions include:

- Old chunks remain on legacy read path while new chunks use packed refs.
- Compression codec distribution contains both `none` and `zstd` in mixed states.
- Store-if-smaller fallback is observed for incompressible payloads under zstd defaults.
- Full restore hash checks pass before and after repository-default changes.
- Verify and GC continue to pass in mixed repository states.
- Stats report both legacy and packed block populations.

## CI policy

CI now separates correctness checks from benchmark measurement:

1. The correctness matrix runs independently from benchmarks and covers the supported codec combinations.
2. The benchmark matrix runs the small dataset only for the recommended packed `aes-gcm` production modes with `COLDKEEP_COMPRESSION=none` and `COLDKEEP_COMPRESSION=zstd`.
3. Benchmark outputs are captured as artifacts for inspection. Threshold-based regression comparison is enforced via `--compare` with mode-specific thresholds; violations are reported per the v1.9 regression thresholds policy above.

See [benchmarks/v1.9/regression-thresholds.yaml](../benchmarks/v1.9/regression-thresholds.yaml)
and CI workflow for authoritative threshold application.

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

#### Restore interpretation

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

For the Phase 8 packed block-size decision matrix, use the resumable helper:

```bash
# Show what is already complete vs missing/incomplete
DB_HOST=127.0.0.1 DB_PORT=5432 DB_USER=coldkeep DB_PASSWORD=coldkeep DB_NAME=coldkeep DB_SSLMODE=disable \
scripts/run_phase8_blocksize_matrix.sh --list-missing

# Resume only missing or incomplete runs (builds once, writes JSON atomically)
DB_HOST=127.0.0.1 DB_PORT=5432 DB_USER=coldkeep DB_PASSWORD=coldkeep DB_NAME=coldkeep DB_SSLMODE=disable \
scripts/run_phase8_blocksize_matrix.sh

# Generate an aggregated markdown/json summary from the collected JSON files
python3 scripts/summarize_phase8_blocksize.py \
   --input-dir tmp/bench_phase8 \
   --output tmp/bench_phase8/summary.md \
   --json-output tmp/bench_phase8/summary.json
```

The helper skips only outputs that already contain a valid `"status":"ok"`
payload, so interrupted Codespace sessions can resume without restarting the
whole matrix.

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
