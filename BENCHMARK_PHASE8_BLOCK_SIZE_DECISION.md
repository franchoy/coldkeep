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

## 8. Metrics to Collect (Locked)

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

## 9. Safety and Correctness Gates (Must Stay True)

These are hard gates for all candidates:

1. Packed blocks are reclaimed only as whole physical units.
2. No chunk inside a packed block is treated as independently reclaimable
   physical storage.
3. Mixed repositories (legacy + packed) remain valid.
4. Snapshot-retained packed chunks continue to protect the whole block.
5. Verify passes with no new integrity regressions.

If a candidate violates any gate, it is disqualified regardless of speed.

## 10. Decision Rule

Primary decision is 1 MiB vs 2 MiB.

Choose 2 MiB only if it is clearly better on overall balance, meaning:

1. Throughput improvement is consistent across store and restore workloads.
2. GC retained dead-space impact is acceptable and explicitly quantified.
3. Verify cost and tail latency do not regress materially.
4. Memory pressure increase is bounded and acceptable for expected operators.

Otherwise, keep 1 MiB as default.

## 11. 3 MiB Policy (Experimental Only)

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

## 12. Required Final Output

Phase 8 must end with a concise decision record containing:

1. Selected default (`1 MiB` or `2 MiB` for v1.8).
2. One-paragraph rationale tied to measured evidence.
3. Matrix table summary with medians and p95s.
4. Explicit statement on 3 MiB status (`experimental` or `promoted`).
5. Operator-facing note about tradeoffs (throughput vs amplification vs GC
   retention profile).
