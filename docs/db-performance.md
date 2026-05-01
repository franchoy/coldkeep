# DB Performance

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
