# v1.12 Release Phase List

This sequence converts the high-level v1.12 plan into the release train. The behavior-preserving rule
is not refinable: no command is routed through the engine unless its request/result contract can
represent the existing command behavior.

## Phase 0 — Scope Lock, Baseline, and Risk Inventory

Planning, inventory, guardrails, and release checklist. No code behavior change.

## Phase 1 — Engine DB Ownership and Read-Side Parity

Fix engine dependency ownership (`Verify` must use `Config.DB`, not global DB discovery) and safely
route/validate read-side commands. Route `inspect` and `verify system` through the engine only after
parity tests exist.

## Phase 2 — Engine Contract Expansion

Replace thin placeholder request/result candidates with realistic contracts that can preserve real
command behavior (store, store-folder, restore by ID, restore by stored path, remove, snapshot
create/list/files/stats/diff/restore/delete, GC dry-run/live, repair, recovery, config). Results
stay renderer-neutral.

**Status: complete.** `internal/engine/candidates.go` contracts expanded with shared neutral types
(`OperationWarning`, `BatchSummary`, `ExecutionMode`, `SnapshotQuery`) and mode/destination enums;
recovery re-modeled from an incorrect restore-like placeholder to a corrective report. No interface
methods added and no command routed (contract preparation only). Neutrality and representability
proven by `internal/engine/contracts_test.go`. `CK-112-R002` fixed. Entry criteria for Phase 3: the
catalog facade skeleton can assume these contracts as the engine-side shape it must eventually feed.

## Phase 3 — Catalog Facade Skeleton

Introduce `internal/catalog` interfaces with wrapper-only adapters over existing DB/query code. No
behavior change, no SQL dialect change. Dependency rule: engine may import catalog; catalog must not
import engine; CLI must not import catalog directly once migration begins.

**Status: complete.** `internal/catalog` package created with the `Catalog` aggregate interface
composed of eight per-responsibility sub-interfaces (`LogicalFileCatalog`, `PhysicalFileCatalog`,
`SnapshotCatalog`, `SnapshotGraphCatalog`, `ReachabilityCatalog`, `PlacementCatalog`,
`RestorePlanCatalog`, `GCPlanCatalog`). Four interfaces have real wrappers backed by live SQL
(`FindLogicalFile`, `FindPhysicalFilesForLogicalFile`, `FindSnapshot`, `ListSnapshots`,
`LoadReachabilityRoots`). Four interfaces are deferred skeletons returning `ErrNotImplemented`
(`LoadSnapshotGraph` → Phase 5/6, `LoadChunkPlacements` → Phase 7/8, `LoadRestorePlanMetadata` →
Phase 7, `LoadGCPlanMetadata` → Phase 6). Dependency direction enforced by
`TestCatalogDependencyDirection`; contract neutrality enforced by `TestCatalogExportedTypesAreNeutral`;
behavioral correctness proven by 9 SQLite-backed tests in `service_test.go`. `engine.Config`
unchanged. Entry criteria for Phase 4: backend-neutral catalog contract tests cover SQLite and
PostgreSQL behavioral differences.

## Phase 4 — SQLite/PostgreSQL Catalog Compatibility Baseline

Add backend-neutral catalog contract tests (SQLite + PostgreSQL where feasible) and document SQL
dialect boundaries (placeholder syntax, `RETURNING`, transaction behavior, introspection, timestamp
and path normalization).

**Status: complete.** Dual-backend contract harness added in
`internal/catalog/backend_contract_test.go`: SQLite runs unconditionally; PostgreSQL is gated by
`COLDKEEP_TEST_DB` (CI provides a `postgres:16` service and sets it) and skips with an explicit reason
locally. One backend-neutral fixture (`seedCatalogFixture`) seeds both backends via `$1` placeholders
with Go-typed args (bool, `time.Time`). Cross-backend tests cover `FindLogicalFile`,
`FindPhysicalFilesForLogicalFile` (ordering, nullable `mtime`, boolean `is_metadata_complete`),
`FindSnapshot`, `ListSnapshots` (ordering, `Type`/`LabelSubstring`/`Since`/`Until`/`Limit`),
`LoadReachabilityRoots` (set separation + de-dup), and the deferred `ErrNotImplemented` methods. The
baseline uncovered a real timestamp-bind incompatibility in `ListSnapshots` (pre-formatted RFC3339
string vs go-sqlite3 space-separated storage); fixed narrowly by binding `time.Time` directly. Dialect
rules documented in `sqlite-postgres-baseline.md`; `CK-112-R003` fixed. Entry criteria for Phase 5:
snapshot read-side orchestration (list/stats/files/diff) can be routed through the engine using
catalog reads proven equivalent across backends.

## Phase 5 — Snapshot Orchestration Migration

Route snapshot list/stats/files/diff through engine first, then create/delete/restore after tests.
Keep snapshot graph/reachability access behind catalog where possible. Preserve immutability and
retention semantics.

**Status: complete.** `snapshot list`, `snapshot files`, `snapshot stats`, and `snapshot diff`
(including the summary fast-path) are now routed through the engine. Four phase variables
(`listSnapshotsPhase`, `getSnapshotPhase`, `snapshotStatsPhase`, `diffSnapshotsPhase`,
`diffSnapshotSummaryPhase`) in `cmd/coldkeep/main.go` are now engine-backed closures; the previous
direct calls to the snapshot package are replaced. `SnapshotList`, `SnapshotShow`, `SnapshotStats`,
and `SnapshotDiff` added to the `Engine` interface and implemented on `DefaultEngine`. The
`snapshotMetaToSnapshot` helper bridges `engine.SnapshotMeta` → `snapshot.Snapshot` for rendering.
`ParentSnapshotID` correctness bug fixed: `SnapshotStatsResult` now carries the real parent snapshot
ID. Snapshot create/delete/restore deferred to Phase 9/7 respectively. Parity proven by
`internal/engine/snapshot_engine_test.go` and CLI routing tests. Entry criteria for Phase 6:
GC dry-run and live GC can be routed through engine using a DB-aware wrapper.

## Phase 6 — GC Plan and Reachability Migration

Move GC dry-run/live orchestration behind engine and reachability/deletion-plan inputs behind catalog
APIs. Represent packed and legacy roots consistently. Preserve: GC must never delete reachable data.

**Status: complete.** `RunGCWithDB(ctx, dbconn, dryRun, containersDir)` added to
`internal/maintenance/gc.go`; `RunGCWithContainersDirResult` refactored to a thin wrapper that opens
the DB and delegates. `GarbageCollect` added to the `Engine` interface and implemented on
`DefaultEngine` — maps `GarbageCollectRequest.DryRun` through `RunGCWithDB`. `runGCPhase` in
`cmd/coldkeep/main.go` replaced with an engine-backed closure (same signature preserved). Live GC
continues to be refused on the SQLite backend; dry-run is supported on both. `LoadGCPlanMetadata`
remains `ErrNotImplemented` (reachability catalog API deferred). Parity proven by
`internal/engine/gc_engine_test.go` (3 SQLite tests) and `cmd/coldkeep/gc_engine_routing_test.go`
(2 CLI routing tests). Entry criteria for Phase 7: restore plan catalog API allows engine-routed
restore by ID and by stored path.

## Phase 7 — Restore Plan Migration

Introduce a restore-plan catalog API. Support ID restore and stored-path restore. Preserve overwrite,
prefix, override, and original destination modes, plus path traversal and symlink safety. Add
engine-level tests for "restore must not write outside destination."

**Status: complete (scoped).** `Restore` added to the active `Engine` interface and implemented on
`DefaultEngine` for `file_ids` mode. CLI restore-by-ID live and dry-run execution now route through
an engine-backed seam (`restoreByIDPhase`), while preserving batch execution/reporting and existing
human/JSON/exit-code behavior. Stored-path restore and snapshot restore remain on direct paths and are
explicitly deferred pending full destination-mode parity coverage through engine. `LoadRestorePlanMetadata`
remains `ErrNotImplemented` (catalog restore-plan API deferred). Parity evidence: engine tests in
`internal/engine/restore_engine_test.go` and CLI routing test in
`cmd/coldkeep/restore_engine_routing_test.go`.

## Phase 8 — Store/CDC/Placement Coordination Migration

Route `store` and `store-folder` through engine. Keep CDC/chunker selection, compression, and
encryption behavior identical. Move placement lookup/recording behind catalog APIs where feasible.
Preserve cross-version chunk reuse.

## Phase 9 — Remove/Repair/Recovery Migration

Route remove by ID/stored-path, repair, and recovery (where safe) through engine. Recovery must not
legitimize corrupt mappings; repair must not hide broken catalog/storage state. Preserve batch and
dry-run semantics.

## Phase 10 — CLI Thin Wrapper Burn-down

Reduce `cmd/coldkeep` to: parse args, validate CLI syntax, build engine requests, call engine, render
output. Remove duplicated orchestration. Keep CLI validation as user-facing protection while
duplicating core safety validation in engine/catalog.

## Phase 11 — Engine/Catalog Invariant Test Matrix

Prove no-data-loss, deterministic restore, GC safety, verification failure-closed behavior, snapshot
retention, packed/legacy parity, and SQLite/PostgreSQL compatibility at the engine/catalog boundary.
Add catalog contract tests (reachability, placement, restore-plan, GC-plan, snapshot graph) and
dependency-direction checks for catalog.

## Phase 12 — Release Candidate Gate

Run the full local suite, race suite, integration/adversarial suites, PostgreSQL compatibility suite,
critical coverage checks, and release checklist. Compare CLI JSON/human output parity for migrated
commands. Document known limitations and accepted risks.
