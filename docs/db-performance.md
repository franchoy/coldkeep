# DB Performance

Historical note: this document captures v1.7-era DB performance work.
For current v1.9 contract/baseline framing, see `docs/benchmarking.md` and
`docs/internal/benchmark_baselines_v1_9.md`.

v1.7 DB performance work is intentionally conservative: controlled execution,
query-shape cleanup, and measured validation rather than daemon-style
concurrency expansion.

Compatibility guardrails for this work:

- no storage format change
- no schema-breaking change
- restore determinism preserved
- GC safety preserved
- snapshot semantics preserved

## Step 3 recommendation

Proceed with:

- Run EXPLAIN/EXPLAIN ANALYZE for the snapshot candidate queries.
- Add only the snapshot indexes that are actually used.
- Refactor stats N+1 query loops.
- Benchmark snapshot/stats/inspect paths.
- Check store write-path impact from added indexes.

## Phase 7 query review

Restore and store metadata paths already have appropriate index coverage:

- `file_chunk(logical_file_id, chunk_order)`
- `logical_file(file_hash, total_size)`
- `chunk(chunk_hash, size)`

GC is primarily block/container driven; `chunk(container_id)` is not applicable because
`container_id` belongs to `blocks` in the current schema.

Primary Phase 7 candidates are `snapshot_file` indexes for lineage/diff/delete-preview
queries and stats query-shape cleanup to remove N+1 patterns.

You are on the right track: targeted indexes + query-shape cleanup, not index sprawl.

## Step 3 validation (2026-05-01)

EXPLAIN ANALYZE was run for the snapshot query shapes used by listing, lineage,
delete-preview (NOT EXISTS), and diff summary logic.

Validation rule:

- Keep an index only if the planner uses it and measured execution improves.
- Reject an index if plans/timings do not show a real benefit.

Candidate results:

- Candidate A: `snapshot_file(snapshot_id, logical_file_id)` -> rejected.
  Plans continued to use `snapshot_id` scans from existing indexes; this new
  composite index was not selected for the tested query shapes.
- Candidate B: `snapshot_file(path_id, logical_file_id, snapshot_id)` -> rejected.
  Delete-preview still used a hash anti-join with a broad `snapshot_file`
  scan on the anti-join side; no stable planner adoption of this composite index
  was observed in the validated query set.

Decision:

- Do not add either candidate index to schema at this time.
- Keep focusing on query-shape cleanup and re-test indexes only when plans
  demonstrate clear usage and measurable end-to-end gains.

## Phase 7 - DB Optimization (2026-05-01)

Phase 7 optimization work targeted `snapshot_file` read/query shapes and stats
aggregation performance.

Added indexes:

- None.

Rejected indexes:

- `idx_snapshot_file_snapshot_logical` (`snapshot_file(snapshot_id, logical_file_id)`):
  no stable planner adoption or measurable end-to-end gain in validated query
  shapes.
- `idx_snapshot_file_path_logical_snapshot`
  (`snapshot_file(path_id, logical_file_id, snapshot_id)`): no measurable gain
  in delete-preview and related anti-join shapes.

Stats improvements:

- Removed N+1 per-file chunk-size lookups.
- Replaced per-snapshot loops with set-based `GROUP BY`/set-join queries.
- Consolidated repeated join work through batched aggregation paths.

Result summary:

- `stats-inspect` improved versus the v1.6 small baseline
  (workers=1: 1060ms -> 903ms, workers=4: 1060ms -> 635ms).
- `gc-after-churn` improved versus baseline
  (workers=1: 2624ms -> 2462ms, workers=4: 2624ms -> 1733ms).
- `snapshot-creation` regressed versus baseline
  (workers=1: 520ms -> 672ms, workers=4: 520ms -> 631ms).
- Store-path impact is mixed (some store scenarios improved under workers=4,
  while `store-large-file` regressed in the measured local run); no new
  snapshot candidate index was introduced in this phase.
