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
