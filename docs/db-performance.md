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
