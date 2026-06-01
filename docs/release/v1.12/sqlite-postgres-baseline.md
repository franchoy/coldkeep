# v1.12 SQLite/PostgreSQL Compatibility Baseline

Purpose: make backend compatibility explicit from the start of v1.12.

## Direction

SQLite is the intended future default for local repositories. PostgreSQL remains a supported
external/advanced catalog backend and stays continuously tested.

## Phase 0 rule

- Do not change the default backend in Phase 0.
- Do not introduce SQLite-only assumptions.
- Do not remove PostgreSQL tests.
- Do not make engine contracts depend on one backend's SQL behavior.

## Known dialect observation (Phase 0)

The existing verify-file path (`internal/maintenance/verify_command.go`) uses a PostgreSQL-style
`$1` positional placeholder. SQLite accepts `$1`-style numbered placeholders, so this is currently
compatible, but it is an example of a SQL dialect boundary that the catalog facade must isolate
behind adapters rather than spread across orchestration code. This is recorded so Phase 3/4 work
does not assume a single placeholder style.

## Compatibility areas to track

- schema bootstrap;
- migrations;
- transaction behavior;
- placeholder syntax (`?` vs `$1`);
- `RETURNING` support;
- path normalization;
- timestamp behavior;
- schema introspection;
- snapshot graph queries;
- reachability queries;
- packed/legacy metadata queries;
- GC plan queries;
- restore plan queries;
- verify queries.

## Minimum future dual-backend tests

- store;
- restore;
- snapshot create/list/restore/delete;
- GC dry-run/live plan behavior;
- verify;
- inspect/stats;
- packed/legacy parity;
- catalog contract tests;
- engine request/result equivalence tests.

## Toolchain note

`go.mod` declares `go 1.25` with `toolchain go1.25.10`. Environments that cannot download this
toolchain cannot build or run the suite locally; in that case validation relies on CI. This is a
build-environment fact, not a backend compatibility issue, but it is recorded here because it affects
how compatibility evidence is gathered.

## Phase 4 catalog compatibility baseline

Implemented `internal/catalog` methods are now tested across SQLite and PostgreSQL through a
dual-backend contract harness (`internal/catalog/backend_contract_test.go`). The SQLite backend runs
unconditionally on an in-memory database. The PostgreSQL backend follows the existing project
convention: it is gated by `COLDKEEP_TEST_DB` and skips with an explicit reason when unset, reads the
DSN from the `DB_*` environment variables, provisions a uniquely named throwaway database, applies the
schema via `db.EnsurePostgresSchema` with `COLDKEEP_DB_AUTO_BOOTSTRAP=true`, and drops the database on
cleanup. CI provides a `postgres:16` service and sets `COLDKEEP_TEST_DB=1`, so the PostgreSQL path runs
in CI.

Methods covered across both backends:

- `FindLogicalFile` (missing → `(nil, nil)`; exact field values);
- `FindPhysicalFilesForLogicalFile` (empty for unknown id; path ordering; nullable `mtime`; boolean
  `is_metadata_complete`);
- `FindSnapshot` (missing → `(nil, nil)`; type/label/parent; timestamp parse);
- `ListSnapshots` (newest-first ordering; `Type`, `LabelSubstring` (LIKE), `Since`/`Until`, `Limit`);
- `LoadReachabilityRoots` (current vs snapshot set separation; set de-duplication);
- deferred methods (`LoadSnapshotGraph`, `LoadChunkPlacements`, `LoadRestorePlanMetadata`,
  `LoadGCPlanMetadata`) consistently return `ErrNotImplemented`.

### Catalog SQL dialect rules

- **Placeholders.** Catalog queries use `$1`-style positional placeholders, accepted by both lib/pq
  (PostgreSQL) and go-sqlite3 (SQLite). Verified by the dual-backend tests, not assumed.
- **Timestamps.** Timestamp fixtures use fixed UTC values (`time.Date(..., time.UTC)`), never
  `time.Now()`, so comparisons are deterministic. Timestamp **bind parameters** (e.g. `ListSnapshots`
  `Since`/`Until`) must bind `time.Time` directly and must **not** be pre-formatted to an RFC3339
  string. go-sqlite3 stores timestamps with a space-separated layout, so a pre-formatted `T`-separated
  string sorts before all stored values and silently returns no rows. Binding `time.Time` lets each
  driver serialize the value consistently with how `created_at` is stored. This bug was found and
  fixed in Phase 4 (`internal/catalog/snapshots.go`).
- **Booleans.** `physical_file.is_metadata_complete` is `INTEGER` (0/1) on SQLite and `BOOLEAN` on
  PostgreSQL. Bind and scan Go `bool`; never bind integer literals for the column.
- **Nullable fields.** Nullable columns (`mode`, `mtime`) are scanned through nullable scan types
  (`sql.NullInt64`, `sql.NullTime`) internally and exposed as neutral exported types (`int`,
  `*time.Time`). A NULL `mtime` maps to a nil `*time.Time` on both backends.
- **Ordering.** Every list method specifies an explicit `ORDER BY`; result order is never left to the
  backend default.
- **Text matching.** Label filtering uses `LIKE` with `%substring%`, tested on both backends.
- **Reachability sets.** Reachability is returned as sets keyed by logical file id, so duplicate
  source rows do not produce duplicate roots, identically on both backends.

Any backend-specific behavior must be isolated behind a small documented helper rather than ad-hoc
per-query SQL branching. As of Phase 4 the only dialect-sensitive point found is the timestamp bind
rule above; it is handled by binding `time.Time` (no branching required).

