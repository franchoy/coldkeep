# Phase 8 - Benchmark and Block Size Decision (v1.8)

Status: decision framework locked.

## Objective

Choose the final v1.8 default packed block size using measured evidence, not
single-run intuition.

Primary decision set:

- 1 MiB (current default)
- 2 MiB (primary candidate)

Optional exploratory set:

- 3 MiB (experimental only)

Current implementation knob:

- `COLDKEEP_BLOCK_TARGET_SIZE_MB` controls packed target size at write time.
- Current code default remains 1 MiB.

Important behavior contract:

- The env override affects new writes only.
- Existing blocks remain self-describing via per-block metadata (for example
   `storage_blocks.plaintext_size`).
- Readers must not assume a global configured block size.

## 1. Locked Benchmark Question

The question is not "which size is fastest in one test".

The locked question is:

Which packed block size gives the best overall operating balance for Coldkeep,
while preserving safety invariants and predictable operator behavior?

The decision must optimize for system-level behavior, not one benchmark number.

## 2. Candidate Sizes (Locked)

Use the following candidates:

- Candidate A: 1 MiB
- Candidate B: 2 MiB
- Candidate C (optional exploratory): 3 MiB

FastCDC parameters currently in use:

- `MinChunkSize = 32 * 1024`
- `AvgChunkSize = 64 * 1024`
- `MaxChunkSize = 128 * 1024`

Expected average chunks per block (using `AvgChunkSize = 64 KiB`):

- 1 MiB -> ~16 chunks/block
- 2 MiB -> ~32 chunks/block
- 3 MiB -> ~48 chunks/block

Initial expectations (hypotheses to validate with data):

- 1 MiB: safest balance
- 2 MiB: likely best throughput and future compression candidate
- 3 MiB: possible upside, but higher over-read and GC-retained-space risk

## 3. Required Decision Dimensions

Each candidate (1, 2, optional 3 MiB) must be compared across all dimensions:

1. Store throughput
2. Restore throughput
3. Block reads on restore
4. Read/decrypt amplification
5. GC retained dead space
6. Verify cost
7. Compression-readiness impact (future v1.9+)
8. Memory and latency impact (tail behavior, not only average)

No candidate can be accepted without complete evidence for all dimensions.

## 4. Measurement Contract (What Counts as Evidence)

For each candidate size:

1. Run benchmark presets `small` and `medium`.
2. Run with `workers=1` and `workers=4`.
3. Collect at least 3 repeats per matrix point.
4. Record median and p95 for timing-sensitive metrics.
5. Record restore block-read count and bytes read (amplification proxy).
6. Record GC simulation and real GC outcomes for retained dead bytes.
7. Record verify wall time on the same repository state.

Minimum matrix:

- sizes: 1, 2 MiB (required), 3 MiB (optional exploratory)
- datasets: small, medium
- workers: 1, 4
- repeats: >= 3

## 5. Fixed Variables (Decision-Grade Requirement)

For decision-grade benchmarking, block size is the only independent variable.

Keep all of the following fixed across candidate runs:

- FastCDC configuration
- container max size (`64 MiB`)
- database backend
- storage directory type
- encryption/key settings
- CPU parallelism
- dataset
- CLI command sequence
- GC settings
- verify settings
- machine
- filesystem

Any run matrix that changes one of the above between candidates is not valid
for block-size selection.

Invalid comparisons (must not be used for the decision):

- `1 MiB` on sqlite vs `2 MiB` on postgres
- `1 MiB` encrypted vs `2 MiB` plain
- `1 MiB` warm cache vs `2 MiB` cold cache

These comparisons mix confounders and invalidate attribution.

## 6. Repository Isolation (Fresh Repo Per Candidate)

Never benchmark `1 MiB` and `2 MiB` in the same repository.

Required repository layout per run set:

- `repo_1m/`
- `repo_2m/`
- `repo_3m_optional/`

Each candidate run must start from a fresh repository state for that candidate
and must not reuse prior repositories from a different block-size run.

Why this is mandatory:

- Mixed block sizes contaminate attribution metrics.
- GC outcomes become ambiguous when packed blocks were written under different
   target-size policies.
- Restore behavior becomes noisy because physical block composition differs by
   prior-run history, not only by current candidate policy.

## 7. Dataset Matrix (A-F)

Multiple datasets are mandatory because block-size trade-offs differ by
workload.

### Dataset A - Large Sequential File

Purpose:

- Measure sequential store/restore throughput and block-read reduction behavior.

Shape:

- 1 file sized 1-4 GiB.

Content options:

- random-ish binary
- moderately compressible generated content

Note:

- v1.8 does not enable compression for packed-block write path, but including
   a compressibility-oriented variant is useful for v1.9 readiness signals.

### Dataset B - Many Small Files

Purpose:

- Measure small-file packing efficiency and restore locality effects.

Shape:

- 10,000-100,000 files
- file sizes 1 KiB-64 KiB
- directory depth 3-5 levels

Expected trade-off signal:

- larger target blocks may reduce block count
- smaller target blocks may reduce selective-restore over-read

### Dataset C - Mixed Realistic Folder (Priority)

Purpose:

- Simulate real cold-backup behavior under mixed content classes.

Shape (single source tree with mix):

- small text and config files
- medium photos and documents
- large archive, video, or database dump files
- duplicate subtrees

Priority:

- This is the highest-priority dataset for final default decision weight.

### Dataset D - Dedup-Heavy Workload

Purpose:

- Ensure dedup effectiveness remains intact and block packing does not repack
   existing duplicate content unnecessarily.

Shape:

- `folder_v1/`
- `folder_v2/` with 80-95% shared content vs `folder_v1/`

Measurement rule:

- Record second-ingestion metrics separately from first-ingestion metrics.

### Dataset E - Selective Restore and Over-Read

Purpose:

- Quantify selective-restore penalty from larger packed blocks.

Shape:

- Store many small files, then run selective restores:
   - restore one file
   - restore 100 random files
   - restore one nested directory

Interpretation:

- This dataset is a primary guardrail against hidden read-amplification cost.

### Dataset F - GC Partially-Live Packed Blocks

Purpose:

- Measure retained dead space caused by whole-block GC atomicity.

Shape:

- Store 10,000 small files
- delete 50% randomly
- run GC

Required outputs:

- bytes reclaimable
- bytes retained due to partially-live blocks

Decision weight:

- This dataset is critical for the 1 MiB vs 2 MiB choice.

## 8. Benchmark Run Matrix (Locked)

Minimum required matrix:

- Block sizes: `1 MiB`, `2 MiB`
- Datasets: `A`, `B`, `C`, `D`, `E`, `F`
- Runs per test: `3`

Recommended full matrix:

- `1 MiB`
   - Dataset A (large file) x3
   - Dataset B (many small files) x3
   - Dataset C (mixed folder) x3
   - Dataset D (dedup-heavy) x3
   - Dataset E (selective restore) x3
   - Dataset F (GC partially-live) x3
- `2 MiB`
   - Dataset A (large file) x3
   - Dataset B (many small files) x3
   - Dataset C (mixed folder) x3
   - Dataset D (dedup-heavy) x3
   - Dataset E (selective restore) x3
   - Dataset F (GC partially-live) x3

Optional exploratory matrix:

- `3 MiB`
   - same full A-F matrix, or
   - focused C/E/F matrix when runtime budget is constrained

Why `3` runs per test:

- First run can be noisier due to initialization and environment variance.
- Decision-quality comparison needs central tendency, not one sample.

Aggregation rule:

- Use median as the primary comparison number.
- Mean may be reported as secondary context.

## 9. Cache State Policy (Cold vs Warm)

Cache state affects restore and verify measurements and must be handled
explicitly.

Two allowed modes:

### Practical mode (default)

- Run each test `3` times and use median.
- This is sufficient for most decision-grade comparisons when strict cache
   control is unavailable.

### Strict mode (when safe and available)

- Before restore/verify runs, drop OS page cache.
- Linux example:

```bash
sync
echo 3 | sudo tee /proc/sys/vm/drop_caches
```

Use strict mode only when it is operationally safe for the benchmark host.

Fallback disclosure requirement:

- If strict cache control is not available, report explicitly:
   - `cache state not controlled; results use repeated median runs`

## 10. Benchmark Command Harness (Locked)

Use one consistent shell harness shape for all matrix runs.

Template shape:

```bash
#!/usr/bin/env bash
set -euo pipefail

BLOCK_MB="$1"
DATASET="$2"
RUN_ID="$3"

export COLDKEEP_BLOCK_TARGET_SIZE_MB="$BLOCK_MB"
export COLDKEEP_STORAGE_DIR="/tmp/coldkeep-bench-${BLOCK_MB}m-${DATASET}-${RUN_ID}"
export COLDKEEP_TEST_DB=1
export DB_HOST=127.0.0.1
export DB_PORT=5432
export DB_USER=coldkeep
export DB_PASSWORD=coldkeep
export DB_NAME="coldkeep_bench_${BLOCK_MB}_${DATASET}_${RUN_ID}"
export DB_SSLMODE=disable
export COLDKEEP_KEY=00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff

# init db/repo
# store dataset
# stats
# restore dataset
# verify
# gc dry-run / gc if dataset requires
```

Rules:

- Do not reuse DBs between runs.
- Do not reuse storage directories between runs.
- Keep command sequence fixed for all candidates.

Implementation note:

- Canonical matrix runner lives at `scripts/run_phase8_blocksize_matrix.sh` and
   follows these isolation rules by generating per-run `DB_NAME` and
   `COLDKEEP_STORAGE_DIR` from block size, dataset, and run id.

## 10.1 Store Benchmark Sequence (Locked)

For each `(block_size, dataset, run_id)` tuple, execute this exact order:

1. Fresh repo context:
   - unique `DB_NAME`
   - unique `COLDKEEP_STORAGE_DIR`
   - remove storage dir before the run
2. Store dataset:
   - use `store` for single-file datasets
   - use `store-folder` for directory datasets
3. Record store elapsed time (wall clock)
4. Collect stats (`coldkeep stats --output json`)
5. Run verify (`coldkeep verify system --standard`)
6. Record verify elapsed time (wall clock)

Mandatory success checks for every run:

- Store command exits `0`
- Verify command exits `0`
- `storage_blocks_count > 0`
- For multi-chunk datasets: `avg_chunks_per_block > 1`
- Packed block integrity in DB: every `storage_blocks.block_hash` is present

Harness implementation:

- Canonical sequence runner: `scripts/run_phase8_store_sequence.sh`
- Output artifacts:
   - `tmp/bench_phase8_store_sequence/*-stats.json`
   - `tmp/bench_phase8_store_sequence/*-result.json`

## 10.2 Restore Benchmark Sequence (Locked)

For each stored repo tuple `(block_size, dataset, run_id)`, execute:

1. Restore full dataset
2. Compare per-file hashes and tree hash against original dataset
3. Record elapsed time (wall clock)
4. Collect block read/cache metrics if available

Selective restore cases (required):

- single small file
- 100 random small files
- one subdirectory

Expected checks for full + selective cases:

- restored bytes match original bytes
- read amplification is measured
- no hash mismatch

Implementation note:

- Canonical restore sequence runner: `scripts/run_phase8_restore_sequence.sh`
- Uses `restore --stored-path ... --mode prefix --destination <root>` for stable
   path reconstruction and deterministic file-by-file comparisons.
- Captures IO counters via `COLDKEEP_IO_COUNTERS_FILE` and computes:
   `read_amplification = bytes_read / restored_bytes`.

Output artifacts:

- `tmp/bench_phase8_restore_sequence/*-restore-result.json`
- `tmp/bench_phase8_restore_sequence/*-list.json`
- `tmp/bench_phase8_restore_sequence/*-selection.json`

## 10.3 Dedup Benchmark Sequence (Locked)

Dataset D must run this exact sequence:

1. Store `folder_v1`
2. Record baseline chunk/block counts
3. Store `folder_v2` (mostly duplicate content)
4. Record incremental new chunks/blocks
5. Restore both folders and validate hashes/tree hashes
6. Run `verify system --standard`

Expected checks:

- Second store creates far fewer chunks/blocks than baseline
- Existing chunks are not repacked
- Restore validation passes for both folders

Definitions:

- `new_chunks_v2 = chunks_after_v2 - chunks_after_v1`
- `new_blocks_v2 = blocks_after_v2 - blocks_after_v1`
- Dedup incremental ratios:
   - `chunk_incremental_ratio = new_chunks_v2 / chunks_after_v1`
   - `block_incremental_ratio = new_blocks_v2 / blocks_after_v1`

Repack guard:

- Snapshot `chunk_block_refs` mapping after `folder_v1`
- After `folder_v2`, require unchanged mapping for all pre-existing chunks

Canonical scripts:

- Sequence runner: `scripts/run_phase8_dedup_sequence.sh`
- Cross-size comparator (1 MiB vs 2 MiB): `scripts/compare_phase8_dedup_results.py`

Cross-size decision requirement:

- Dedup effectiveness should be roughly equivalent between `1 MiB` and `2 MiB`.
- Chunk identity must remain stable across block-size candidates.
- If dedup deltas are significant, mark run as `investigate=true` and inspect:
   - dataset composition drift,
   - cache state drift,
   - unexpected write-path differences unrelated to chunking.

Output artifacts:

- `tmp/bench_phase8_dedup_sequence/*-dedup-result.json`
- `tmp/bench_phase8_dedup_sequence/*-selection.json`
- `tmp/bench_phase8_dedup_sequence/*-chunk-map-before-v2.tsv`
- `tmp/bench_phase8_dedup_sequence/*-chunk-map-after-v2.tsv`

## 10.4 GC Benchmark Sequence (Locked)

Dataset F must run this exact sequence:

1. Store all small files from `DATASET_F_ROOT/files/`
2. Remove a random subset (~30% by default)
3. Run `simulate gc --output json` — record:
   - `logically_reclaimable_bytes`
   - `physically_reclaimable_bytes`
   - `retained_dead_bytes_due_to_packed_blocks`
4. Run `gc` (live)
5. Run `verify system --standard`
6. Restore remaining (non-removed) files and validate hashes

Primary comparison metric between 1 MiB and 2 MiB:

- `retained_dead_bytes_due_to_packed_blocks`

Expected outcome:

- 2 MiB blocks may retain more dead space because each packed block contains more
  chunks; a partially-live 2 MiB block wastes more bytes than a partially-live 1 MiB block.
- This is one of the strongest reasons to stay with 1 MiB if throughput difference is small.
- If retained dead bytes differ significantly, 1 MiB is preferred on storage efficiency grounds.

Canonical scripts:

- Sequence runner: `scripts/run_phase8_gc_sequence.sh`
- Cross-size comparator (1 MiB vs 2 MiB): `scripts/compare_phase8_gc_results.py`

Output artifacts:

- `tmp/bench_phase8_gc_sequence/*-gc-result.json`
- `tmp/bench_phase8_gc_sequence/*-simulate-gc.json`
- `tmp/bench_phase8_gc_sequence/*-removal.json`

## 11. Metrics to Collect (Locked)

### Store Metrics

Collect:

- elapsed time
- input bytes
- store throughput (MB/s)
- number of chunks
- number of `storage_blocks`
- average chunks per block
- average block plaintext size
- average block stored size
- block fill ratio
- container count
- DB rows inserted
- duplicate chunk count

Formulas:

- `store_throughput = input_bytes / elapsed_seconds`
- `avg_chunks_per_block = chunk_block_refs_count / storage_blocks_count`
- `block_fill_ratio = avg_block_plaintext_payload_size / target_block_size`

### Restore Metrics

Collect:

- elapsed time
- restored bytes
- restore throughput (MB/s)
- number of block reads
- bytes read from containers
- bytes restored
- read amplification
- block cache hits
- block cache misses
- decrypt and decode count

Formulas:

- `restore_throughput = restored_bytes / elapsed_seconds`
- `read_amplification = bytes_read_from_containers / bytes_restored`
- `cache_hit_ratio = hits / (hits + misses)`

Interpretation:

- For full restore, read amplification should be close to `1`.
- For selective restore, read amplification may be significantly higher.

### Verify Metrics

Collect:

- elapsed time
- blocks verified
- chunks verified
- bytes read
- block hash mismatches (must be `0`)
- chunk hash mismatches (must be `0`)

Comparison requirement:

- Compare verify wall time directly between `1 MiB` and `2 MiB`.

### GC Metrics

Collect:

- elapsed time
- live chunks
- dead chunks
- live blocks
- dead blocks
- blocks deleted
- bytes reclaimable
- bytes retained due to partially-live packed blocks
- container cleanup count

Most important Phase 8 GC metric:

- `retained_dead_bytes_due_to_packed_blocks`

This metric captures the real retained-space cost of larger block sizes.

### Optional Compression-Simulation Metrics

Even before v1.9, run offline simulation on decoded plaintext packed blocks:

- compress each block payload with zstd
- record compressed size and timing

Collect:

- estimated compression ratio
- estimated stored bytes
- compression time
- decompression time

Expected trend to validate (not assume):

- `2 MiB` may compress slightly better than `1 MiB`
- `3 MiB` may show diminishing additional benefit

## 12. Benchmark Instrumentation Path (Locked)

Selected approach combines Option A and Option B:

- Option A: extend `stats` output with block-layout metrics so Phase 9 and
   operator reports can reuse one canonical source.
- Option B: expose an internal collector helper (`CollectBlockStats`) so test
   and benchmark code can read the same metrics deterministically.

At minimum, instrumentation must expose:

- `storage_blocks_count`
- `chunk_block_refs_count`
- `avg_chunks_per_block`
- `avg_block_plaintext_size`
- `avg_block_stored_size`
- `avg_block_fill_ratio`
- `legacy_block_count`
- `packed_block_count`
- `codec_distribution`

Option C (manual SQL snippets) remains useful for ad-hoc debugging but is not
the canonical reporting path for decision-grade benchmark output.

## 13. Safety and Correctness Gates (Must Stay True)

These are hard gates for all candidates:

1. Packed blocks are reclaimed only as whole physical units.
2. No chunk inside a packed block is treated as independently reclaimable
   physical storage.
3. Mixed repositories (legacy + packed) remain valid.
4. Snapshot-retained packed chunks continue to protect the whole block.
5. Verify passes with no new integrity regressions.

If a candidate violates any gate, it is disqualified regardless of speed.

## 14. Decision Rule (Locked)

This section is locked. Do not revise after the benchmark run begins.

### Default rule

Keep `1 MiB` unless `2 MiB` shows clear benefit.

### Switch to 2 MiB only if ALL of the following are true

1. `2 MiB` improves large-file or mixed store **or** restore throughput by
   **≥ 10%** (median across the run matrix) compared to `1 MiB`.
2. `2 MiB` does **not** increase selective-restore read amplification to an
   unacceptable level (decision: any increase > 20% over `1 MiB` disqualifies).
3. `2 MiB` does **not** significantly increase `retained_dead_bytes_due_to_packed_blocks`
   after GC (decision: an increase > 20% over `1 MiB` disqualifies).
4. `2 MiB` verify time remains acceptable (no regression > 15% vs `1 MiB`).
5. Memory and latency impact is within acceptable bounds for expected operators.

All five conditions must hold simultaneously. A single disqualifier retains
`1 MiB` as the default.

### Tie-breaking and mixed results

If results are mixed, close, or inconclusive across any dimension:

- Choose `1 MiB`.

### Rationale

`1 MiB` is safer, more granular, produces lower over-read on selective restore,
carries lower GC retention risk (partially-live blocks waste fewer bytes), and
has a longer operational track record. The burden of proof is on `2 MiB` to
demonstrate clear, consistent benefit that outweighs these structural advantages.

## 15. 3 MiB Policy (Experimental Only)

3 MiB may be evaluated, but it is not a default candidate unless evidence is
overwhelmingly positive.

"Overwhelmingly positive" means all are true:

1. It materially outperforms both 1 MiB and 2 MiB on store and restore
   throughput across the matrix.
2. It does not introduce meaningful GC retained-dead-space penalty.
3. It does not increase verify cost or p95 latency materially.
4. It does not create concerning memory amplification for normal operators.

If any of the above is not met, 3 MiB remains experimental and is not selected
as v1.8 default.

## 16. Required Final Output

Phase 8 must end with a concise decision record containing:

1. Selected default (`1 MiB` or `2 MiB` for v1.8).
2. One-paragraph rationale tied to measured evidence.
3. Matrix table summary with medians and p95s.
4. Explicit statement on 3 MiB status (`experimental` or `promoted`).
5. Operator-facing note about tradeoffs (throughput vs amplification vs GC
   retention profile).

## 17. Result Table (Fill After Runs Complete)

Record median values from the full benchmark matrix below. The Δ column is
expressed as `(2 MiB − 1 MiB) / 1 MiB` in percentage points; positive Δ
favours 2 MiB for throughput metrics and disfavours 2 MiB for cost metrics
(amplification, retained dead bytes, verify time). Leave cells blank until the
corresponding sequence harness has completed all runs.

| Dataset            | Metric                    | 1 MiB (median) | 2 MiB (median) | Δ    | Decision hint |
|:-------------------|:--------------------------|---------------:|---------------:|-----:|:--------------|
| Large file         | Store MB/s                |                |                |      |               |
| Large file         | Restore MB/s              |                |                |      |               |
| Small files        | Store MB/s                |                |                |      |               |
| Small files        | Restore MB/s              |                |                |      |               |
| Selective restore  | Read amplification (×)    |                |                |      |               |
| GC partial-live    | Retained dead bytes (MiB) |                |                |      |               |
| Verify             | Wall-clock time (s)       |                |                |      |               |

**Decision hint key**

- `+2 MiB` — metric favours 2 MiB by more than the minimum required threshold
  (see section 14).
- `+1 MiB` — metric favours 1 MiB or 2 MiB improvement is below threshold.
- `=` — within noise (< 3 %).

---

### Final Decision Record

> **Fill this section only after all runs are complete and the table above is
> populated. Do not fill speculatively.**

```
DefaultBlockSize = X MiB

Rationale:
  <One paragraph tying the selected value to specific rows in the table above.
   Reference the section 14 thresholds that were or were not met. State whether
   3 MiB remains experimental or was promoted.>

3 MiB status: experimental | promoted

Operator note:
  <Brief statement on the throughput / read-amplification / GC-retention
   tradeoff that operators should be aware of when overriding the default via
   COLDKEEP_BLOCK_TARGET_SIZE_MB.>
```

## 18. Investigation Triggers

The following observations **must halt the benchmark and trigger an
implementation investigation** before any decision is recorded. Do not proceed
to the decision record while any trigger condition is unresolved.

### 18.1 Correctness triggers (stop immediately)

| Observation | Why it matters |
|:------------|:---------------|
| 1 MiB and 2 MiB produce different restored file hashes for the same source tree | Block size must not affect restore correctness; differing hashes indicate a data-path bug. |
| `verify system --standard` passes but a restore hash differs | Verifier and restore path disagree on block content; one or both are wrong. |
| Any chunk appears in `chunk_block_refs` after v2 store pointing to a different block than before v2 (repack detected) | Existing chunks must never be repacked; a detected repack breaks the no-mutation invariant. |

Any correctness trigger causes **Phase 8 to stop**. Return to implementation
and fix the defect before re-running.

### 18.2 Anomaly triggers (investigate before deciding)

| Observation | Likely cause / what to check |
|:------------|:-----------------------------|
| 2 MiB changes the chunk-incremental ratio by > 10 percentage points relative to 1 MiB for the same dataset | Dedup effectiveness should not be sensitive to block size; check packing logic and boundary alignment. |
| Read amplification exceeds 3× for a normal full-folder restore (non-selective) | Suggests over-reading of packed blocks; check block fan-out and IO path. |
| `retained_dead_bytes_due_to_packed_blocks` (simulate gc) is higher than total size of removed files | More dead space retained than removed; check partial-live block accounting. |
| GC deletes more blocks than expected given removal fraction | Could indicate cascade eviction of live-chunk blocks; audit `chunk_block_refs` integrity after gc. |
| `block_hash` verification wall-clock time is disproportionate relative to store time (e.g. > 2× store time) | Unexpected fan-out or re-read pattern in the verifier; profile IO during verify. |
| Packed block fill ratio < 50 % for large-file or mixed datasets | Blocks are not being filled to target; investigate packing/flush thresholds. |

Anomaly triggers do not automatically stop Phase 8, but the root cause must be
understood and documented before the decision record is written. If the
investigation reveals a defect, treat it as a correctness trigger and stop.

## 19. Phase 8 Deliverables

Phase 8 is complete only when every item in this checklist is satisfied and
committed to the repository.

### 19.1 Benchmark infrastructure

- [ ] **Harness scripts** — all four sequence scripts present and syntax-clean:
  - `scripts/run_phase8_store_sequence.sh`
  - `scripts/run_phase8_restore_sequence.sh`
  - `scripts/run_phase8_dedup_sequence.sh`
  - `scripts/run_phase8_gc_sequence.sh`
- [ ] **Comparator scripts** present and import-clean:
  - `scripts/compare_phase8_dedup_results.py`
  - `scripts/compare_phase8_gc_results.py`
  - `scripts/summarize_phase8_blocksize.py`
- [ ] Each harness enforces **per-run DB + storage isolation** (fresh `DB_NAME`
  and `COLDKEEP_STORAGE_DIR` per invocation).

### 19.2 Datasets

- [ ] **Repeatable dataset generator** or committed seed files exist for every
  dataset class used in the matrix (small-files, large-file, mixed,
  dedup/Dataset D, partial-live/Dataset F).
- [ ] Dataset generation is deterministic given the same seed; the seed value
  is recorded alongside results.
- [ ] Dataset sizes and file-count targets match the values documented in
  section 10 of this file.

### 19.3 Metrics collection

- [ ] All result artefacts are present under `tmp/bench_phase8_*/` for every
  `(block_size, dataset, run_id)` triple in the matrix.
- [ ] `*-stats.json` files contain `block_layout` fields
  (`storage_blocks_count`, `avg_chunks_per_block`, `avg_block_fill_ratio`).
- [ ] IO counter JSONL files are present for restore runs
  (`COLDKEEP_IO_COUNTERS_FILE`).
- [ ] `*-simulate-gc.json` files contain
  `data.gc.summary.retained_dead_bytes_due_to_packed_blocks`.
- [ ] Medians and p95s have been extracted and entered into the result table
  in section 17.

### 19.4 Decision

- [ ] Section 17 result table is fully populated with median values.
- [ ] Section 17 Final Decision Record is filled in with:
  - `DefaultBlockSize` set to the chosen value.
  - One-paragraph rationale referencing specific table rows and section 14
    thresholds.
  - 3 MiB status declared (`experimental` or `promoted`).
  - Operator note written.
- [ ] No section 18 investigation trigger was left unresolved when the decision
  was recorded.

### 19.5 Code changes

- [ ] The chosen default block size is set as the compiled-in default in the
  relevant Go source file (not only via env override).
- [ ] `COLDKEEP_BLOCK_TARGET_SIZE_MB` env override is **retained** in the
  codebase for operator use and future testing; it is documented in
  `README.md` or operator documentation.
- [ ] `go test ./... -run TestDoesNotExist -count=1` passes with no compile
  errors after the default is updated.

### 19.6 Repository hygiene

- [ ] All harness scripts and comparator scripts are committed.
- [ ] Result artefacts under `tmp/` are **not** committed (covered by
  `.gitignore`); only summary JSON or the filled section 17 table is committed.
- [ ] This file (`BENCHMARK_PHASE8_BLOCK_SIZE_DECISION.md`) reflects the final
  decision and is committed as part of the v1.8 release record.


## 20. Phase 8 Final Checklist

All items must be checked before Phase 8 is closed and the v1.8 default is
committed. Items are grouped by concern; check them in order.

### 20.1 Implementation prerequisites

- [ ] `COLDKEEP_BLOCK_TARGET_SIZE_MB` (or equivalent internal override) exists
  and is honoured at runtime.
- [ ] Default block size starts as **1 MiB** in source before any benchmark
  runs begin.
- [ ] Reader never assumes the currently-configured block size; block size is
  derived solely from per-block metadata.
- [ ] Existing blocks are self-describing through their stored metadata (no
  external side-file required to read them back).

### 20.2 Dataset coverage

- [ ] Dataset A — large sequential file benchmark completed.
- [ ] Dataset B — many-small-files benchmark completed.
- [ ] Dataset C — mixed-folder benchmark completed.
- [ ] Dataset D — dedup-heavy benchmark completed.
- [ ] Dataset E — selective-restore benchmark completed.
- [ ] Dataset F — GC partial-live benchmark completed.
- [ ] Fresh DB + storage used for **each** block-size benchmark run.

### 20.3 Metrics collected

**Throughput**
- [ ] Store throughput collected for 1 MiB and 2 MiB (all applicable datasets).
- [ ] Restore throughput collected for 1 MiB and 2 MiB (all applicable
  datasets).
- [ ] Verify wall-clock time collected for 1 MiB and 2 MiB.

**GC**
- [ ] GC reclaim bytes collected for 1 MiB and 2 MiB.
- [ ] `retained_dead_bytes_due_to_packed_blocks` collected for 1 MiB and 2 MiB
  (Dataset F).

**Block layout**
- [ ] Block count collected.
- [ ] Average chunks per block collected.
- [ ] Average block size collected.
- [ ] Block fill ratio collected.

**IO**
- [ ] Read amplification collected (bytes read / bytes restored).
- [ ] Block cache hit/miss collected if instrumentation is available; explicitly
  deferred and noted if not.

**Dedup**
- [ ] Dedup effectiveness compared between 1 MiB and 2 MiB (chunk-incremental
  ratio and block-incremental ratio, Dataset D).
- [ ] Optional compression simulation completed, **or** explicitly deferred with
  a written note explaining why.

### 20.4 Decision and documentation

- [ ] Results table (section 17) fully populated with medians.
- [ ] Final default block size decision documented in section 17 Final Decision
  Record.
- [ ] No section 18 investigation trigger left unresolved at decision time.

### 20.5 Code and test sign-off

- [ ] Default block size set in source to the value selected by the decision.
- [ ] `COLDKEEP_BLOCK_TARGET_SIZE_MB` env override retained in codebase for
  operator use and future testing.
- [ ] Full test suite passes after the default is updated
  (`go test ./... -count=1`).
- [ ] Race detector test passes after the default is updated
  (`go test -race ./... -count=1`).
- [ ] DB-backed compatibility tests still pass after the default is updated.
- [ ] No correctness regressions found (all restore hashes match sources across
  all tested configurations).
