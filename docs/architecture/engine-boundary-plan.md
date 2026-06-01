# Engine Boundary Plan

This document records the engine/catalog boundary direction across releases. It is a planning
document; it does not freeze contracts.

## Boundary rules (from v1.11)

```
cmd/coldkeep          may import internal/engine
internal/engine       may import domain packages
domain packages       must not import internal/engine
internal/engine       must not import cmd/coldkeep
```

These rules are enforced by `internal/engine/dependency_guard_test.go`.

## v1.11 state

v1.11 introduced the behavior-preserving engine facade. `Engine` exposes `Stats`, `Inspect`, and
`Verify`. Only `stats` is routed through the engine from `cmd/coldkeep`. Mutating operations exist
only as inactive candidate contracts. No business logic was moved.

## v1.12 state (Phase 5 — Snapshot Orchestration Migration, complete)

Phase 5 added 4 read-side snapshot methods to the `Engine` interface:
`SnapshotList`, `SnapshotShow`, `SnapshotStats`, `SnapshotDiff`.

All 4 are implemented on `DefaultEngine` and routed from `cmd/coldkeep` via the phase var seam.
Mutating snapshot operations (`create`, `delete`, `restore`) remain direct-wired and are deferred
to a later phase.

`Engine` interface methods as of v1.12 Phase 5:
- `Stats`
- `Inspect`
- `Verify`
- `SnapshotList`
- `SnapshotShow`
- `SnapshotStats`
- `SnapshotDiff`

## v1.12 state (Phase 6 — GC Plan and Reachability Migration, complete)

Phase 6 added `GarbageCollect` to the `Engine` interface and routed both dry-run and live GC
through `DefaultEngine.GarbageCollect` -> `maintenance.RunGCWithDB`.

Execution parity is preserved across backends with an intentional backend rule:

- PostgreSQL: dry-run and live supported.
- SQLite: dry-run supported; live refused by design.

Reachability catalogization is deferred: `LoadGCPlanMetadata` remains `ErrNotImplemented`.

`Engine` interface methods as of v1.12 Phase 6:
- `Stats`
- `Inspect`
- `Verify`
- `SnapshotList`
- `SnapshotShow`
- `SnapshotStats`
- `SnapshotDiff`
- `GarbageCollect`

## v1.12 state (Phase 7 — Restore Plan Migration, complete scoped routing)

Phase 7 adds `Restore` to the active `Engine` interface and routes restore-by-ID live and dry-run execution
through engine orchestration while preserving existing CLI batch reporting and output contracts.

Scope activated in Phase 7:

- `Restore` file-ID mode (`RestoreModeFileIDs`) active on `DefaultEngine`.

Scope intentionally deferred in Phase 7:

- stored-path restore routing through engine (`RestoreModeStoredPath`) remains deferred.
- snapshot restore routing through engine remains deferred.
- catalog restore-plan API (`LoadRestorePlanMetadata`) remains `ErrNotImplemented`.

`Engine` interface methods as of v1.12 Phase 7:
- `Stats`
- `Inspect`
- `Verify`
- `SnapshotList`
- `SnapshotShow`
- `SnapshotStats`
- `SnapshotDiff`
- `GarbageCollect`
- `Restore`

## v1.12 state (Phase 8 — Store/CDC/Placement Coordination, complete scoped routing)

Phase 8 adds `Store` to the active `Engine` interface and routes single-file store execution through
engine orchestration while preserving existing CLI output and JSON contracts.

Scope activated in Phase 8:

- `Store` single-file mode (`Recursive=false`) active on `DefaultEngine`.

Scope intentionally deferred in Phase 8:

- `store-folder` routing through engine remains deferred.
- catalog placement API (`LoadChunkPlacements`) remains `ErrNotImplemented`.

`Engine` interface methods as of v1.12 Phase 8:
- `Stats`
- `Inspect`
- `Verify`
- `SnapshotList`
- `SnapshotShow`
- `SnapshotStats`
- `SnapshotDiff`
- `GarbageCollect`
- `Restore`
- `Store`

## v1.12 state (Phase 9 — Remove/Repair/Recovery Migration, complete scoped routing)

Phase 9 adds `Remove` to the active `Engine` interface and routes remove-by-ID execution through
engine orchestration while preserving existing CLI batch/human/JSON/exit behavior.

Scope activated in Phase 9:

- `Remove` file-ID mode (`RemoveModeFileIDs`) active on `DefaultEngine`.

Scope intentionally deferred in Phase 9:

- stored-path remove modes (`RemoveModeStoredPath`, `RemoveModeStoredPaths`) remain deferred.
- repair remains on direct maintenance routing.
- recovery remains on direct startup/doctor recovery routing.

`Engine` interface methods as of v1.12 Phase 9:
- `Stats`
- `Inspect`
- `Verify`
- `SnapshotList`
- `SnapshotShow`
- `SnapshotStats`
- `SnapshotDiff`
- `GarbageCollect`
- `Restore`
- `Store`
- `Remove`

## v1.12 state (Phase 10 — CLI Thin Wrapper Burn-down, complete scoped routing)

Phase 10 performs a conservative thin-wrapper reduction for verify command orchestration without
activating any deferred high-risk command routes.

Scope activated in Phase 10:

- CLI verify execution is routed via an engine-backed seam (`verifyCommandPhase`) that delegates to
	`Engine.Verify`.
- verify summary collection in CLI now consumes an injected DB handle from storage context instead
	of opening a separate global DB connection path.
- dependency-direction guard adds explicit prohibition of direct `cmd/coldkeep` -> `internal/catalog`
	imports.

Scope intentionally unchanged in Phase 10:

- no new `Engine` methods are activated (active method count remains 11).
- stored-path remove/restore, repair, recovery, and folder-store routing remain deferred.

`Engine` interface methods as of v1.12 Phase 10:
- `Stats`
- `Inspect`
- `Verify`
- `SnapshotList`
- `SnapshotShow`
- `SnapshotStats`
- `SnapshotDiff`
- `GarbageCollect`
- `Restore`
- `Store`
- `Remove`

## v1.12 state (Phase 11 — Engine/Catalog Invariant Test Matrix, proof complete)

Phase 11 is a proof phase: no new routing activation, no backend/schema/storage-format changes,
and no broad refactor.

Scope completed in Phase 11:

- consolidated routed operation -> invariant -> test evidence in
	`docs/release/v1.12/invariant-test-matrix.md`;
- added routed text parity tests for verify/store/restore/remove paths to complement existing JSON
	routing guards;
- confirmed dependency-direction guardrails remain enforced:
	- `cmd/coldkeep` must not import `internal/catalog` directly,
	- `internal/catalog` must not import `internal/engine` or CLI packages;
- confirmed catalog backend-neutral contract coverage remains in place for implemented methods and
	deferred methods continue to return `ErrNotImplemented`.

Scope intentionally unchanged in Phase 11:

- no new `Engine` methods activated (active method count remains 11);
- deferred high-risk routes remain deferred (stored-path restore/remove modes, snapshot restore,
	folder store, repair, recovery).

`Engine` interface methods as of v1.12 Phase 11:
- `Stats`
- `Inspect`
- `Verify`
- `SnapshotList`
- `SnapshotShow`
- `SnapshotStats`
- `SnapshotDiff`
- `GarbageCollect`
- `Restore`
- `Store`
- `Remove`

## v1.12 Migration Rule

v1.12 moves orchestration behind the engine and metadata planning behind catalog APIs.

The migration order is:

1. read-side parity (`inspect`, `verify system`);
2. request/result contract expansion;
3. catalog facade skeleton (`internal/catalog`);
4. catalog backend compatibility baseline (SQLite/PostgreSQL);
5. snapshot orchestration;
6. GC planning/reachability;
7. restore planning;
8. store/CDC/placement;
9. remove/repair/recovery;
10. CLI thin-wrapper burn-down.

### No-command-routing-without-complete-contract rule

No command is routed through the engine unless its request/result contract can represent the existing
command behavior. Routing a command behind a thin contract that drops existing behavior is forbidden.

### Catalog facade migration principle

When migration begins:

```
cmd/coldkeep          must not import internal/catalog directly
internal/engine       may import internal/catalog
internal/catalog      must not import internal/engine
internal/catalog      must not import cmd/coldkeep
```

The catalog facade owns logical identity, physical mapping, snapshot graph, reachability, GC
eligibility, restore-plan inputs, placement, and verification expectations. Storage owns payload
bytes and block/container representation. The engine owns operation coordination, validation, and
safety-boundary enforcement.

## Required invariants (must not be weakened)

```
GC must never delete reachable data.
Restore must never write outside the intended destination.
Verify must fail closed on inconsistent catalog/storage state.
Recovery must not legitimize corrupt mappings.
Packed and legacy storage behavior must remain aligned.
CLI parsing must not be the only place where correctness invariants live.
Engine/catalog APIs must not weaken existing safety guarantees.
```
