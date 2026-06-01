# v1.12 CLI / Business Logic Coupling Inventory

Purpose: identify what must move from CLI to engine/catalog. This is the most important Phase 0
artifact. Cells marked `TBD` are filled in as each migration phase begins; the `Initial findings`
section below records what Phase 0 inventory already confirmed.

## Command map

| Command | Current orchestration owner | Direct DB access | Direct storage-context access | Direct filesystem access | Rendering mixed with behavior | Target owner | v1.12 phase |
|---|---|---|---|---|---|---|---|
| stats | engine (routed) | No (via engine) | Yes (loads context for engine) | TBD | TBD | engine | Phase 1 |
| inspect | CLI → observability.Service | No (engine path uses Config.DB) | TBD | TBD | TBD | engine | Phase 1 (routing deferred) |
| verify | CLI → maintenance (global DB) | Engine path now uses Config.DB; CLI path still global | Yes | TBD | TBD | engine | Phase 1 (DB ownership fixed; routing deferred) |
| snapshot create | CLI/snapshot | TBD | TBD | TBD | TBD | engine + catalog | Phase 5 |
| snapshot list | CLI/snapshot | TBD | TBD | TBD | TBD | engine + catalog | Phase 5 |
| snapshot files | CLI/snapshot | TBD | TBD | TBD | TBD | engine + catalog | Phase 5 |
| snapshot stats | CLI/snapshot | TBD | TBD | TBD | TBD | engine + catalog | Phase 5 |
| snapshot diff | CLI/snapshot | TBD | TBD | TBD | TBD | engine + catalog | Phase 5 |
| snapshot restore | CLI/snapshot/storage | TBD | TBD | TBD | TBD | engine + catalog | Phase 5/7 |
| snapshot delete | CLI/snapshot | TBD | TBD | TBD | TBD | engine + catalog | Phase 5 |
| gc | CLI/maintenance | TBD | TBD | TBD | TBD | engine + catalog | Phase 6 |
| restore | CLI/storage | TBD | TBD | TBD | TBD | engine + catalog | Phase 7 |
| store | CLI/storage | TBD | TBD | TBD | TBD | engine + catalog | Phase 8 |
| store-folder | CLI/storage | TBD | TBD | TBD | TBD | engine + catalog | Phase 8 |
| remove | CLI/storage | TBD | TBD | TBD | TBD | engine + catalog | Phase 9 |
| repair | CLI/maintenance | TBD | TBD | TBD | TBD | engine | Phase 9 |
| recovery | startup/recovery | TBD | TBD | TBD | TBD | engine/recovery | Phase 9 |
| config | CLI/storage config | TBD | TBD | TBD | TBD | deferred or engine | TBD |

## Initial findings (Phase 0 inventory)

These are confirmed by static inspection of `cmd/coldkeep/main.go` at the v1.11.0 baseline. Line
numbers are indicative and must be re-confirmed at the start of each migration phase.

- `db.ConnectDB` is called directly from `cmd/coldkeep/main.go` in 3 places (approx. lines 841, 2588,
  2641).
- `verify` reopens the DB inside `maintenance.VerifyCommandWithContainersDir`
  (`internal/maintenance/verify_command.go`) via `db.ConnectDB()`, ignoring any engine-owned DB.
- Storage context is loaded via the `loadDefaultStorageContextPhase` variable (approx. line 193),
  which wraps `storage.LoadDefaultStorageContext`; numerous commands call it directly.
- No direct `QueryContext`/`QueryRowContext`/`ExecContext`/`BeginTx`/`sql.Tx` calls were found in
  `main.go` production paths (these appear in test files only).

## Phase 1 update (v1.12.1)

- Engine `Verify` DB ownership fixed (CK-112-R001). `DefaultEngine.Verify` delegates to the new
  `maintenance.VerifyCommandWithDBAndContainersDir`, which accepts a caller-provided `*sql.DB`. The
  legacy `maintenance.VerifyCommandWithContainersDir` remains as a thin wrapper that opens the global
  DB and delegates to the DB-aware path, so the existing CLI `verify` command is unchanged.
- `Stats` and `Inspect` reviewed and confirmed to already use the injected `Config.DB` via
  `observability.Service`; no global reopen.
- CLI routing of `inspect` and `verify system` through the engine is **deferred** to keep the Phase 1
  diff minimal and risk-free. No CLI behavior, JSON shape, or exit code changed in Phase 1.

## Phase 2 update (v1.12)

- Engine operation contracts were expanded (`CK-112-R002` fixed). The thin v1.11 placeholders in
  `internal/engine/candidates.go` now represent real command behavior for store, restore, remove, gc,
  snapshot (create/list/show/stats/diff/delete/restore), repair, and recovery, plus shared
  renderer-/backend-neutral types. See `engine-baseline.md` for the full contract table and deferrals.
- This is contract/design work only: the active `Engine` interface is unchanged, no command was
  routed, and no CLI/JSON/exit-code/storage/schema/backend behavior changed. The command map above is
  therefore unchanged; phase targets per command still apply.

## Phase 3 update (v1.12)

- `internal/catalog` package created. The `Catalog` aggregate interface composes eight
  per-responsibility sub-interfaces. `Service` satisfies all of them.
- Real SQL wrappers: `FindLogicalFile`, `FindPhysicalFilesForLogicalFile`, `FindSnapshot`,
  `ListSnapshots`, `LoadReachabilityRoots`. Queries use `$1` positional placeholders (backend-neutral).
- Deferred skeletons returning `ErrNotImplemented`: `LoadSnapshotGraph` (Phase 5/6),
  `LoadChunkPlacements` (Phase 7/8), `LoadRestorePlanMetadata` (Phase 7), `LoadGCPlanMetadata`
  (Phase 6).
- No command was routed. `engine.Config` unchanged. No CLI/JSON/exit-code/storage/schema/backend
  behavior changed.

## Phase 5 update (v1.12)

- `snapshot list`, `snapshot files`, `snapshot stats`, and `snapshot diff` (including summary
  fast-path) routed through the engine. The `gc` command remains on the direct path pending Phase 6.
- Four phase vars changed from direct `snapshot`/`retention` package calls to engine-backed closures:
  `listSnapshotsPhase`, `getSnapshotPhase`, `snapshotStatsPhase`, `diffSnapshotsPhase`,
  `diffSnapshotSummaryPhase`.
- `Engine` interface expanded from 3 to 7 active methods: `SnapshotList`, `SnapshotShow`,
  `SnapshotStats`, `SnapshotDiff` added.
- Direct DB access for snapshot reads removed from the CLI path (engine owns the DB for these).
- Snapshot create, delete, and restore remain CLI/snapshot package calls (deferred to Phase 9/7).
- No CLI/JSON/exit-code/storage/schema/backend behavior changed.

## Phase 6 update (v1.12)

- `gc` (dry-run and live) routed through the engine. `runGCPhase` in `cmd/coldkeep/main.go` changed
  from a direct `maintenance.RunGCWithContainersDirResult` reference to an engine-backed closure.
- `RunGCWithDB(ctx, dbconn, dryRun, containersDir)` added to `internal/maintenance/gc.go`;
  `RunGCWithContainersDirResult` refactored to open DB and delegate (thin wrapper preserved for
  backward compatibility in tests requiring `COLDKEEP_TEST_DB`).
- `Engine` interface expanded from 7 to 8 active methods: `GarbageCollect` added.
- `DefaultEngine.GarbageCollect` maps `GarbageCollectRequest.DryRun` → `RunGCWithDB` and translates
  `GCResult` fields to `GarbageCollectResult`.
- Live GC continues to be refused on the SQLite backend. Dry-run is supported on both.
- `LoadGCPlanMetadata` remains `ErrNotImplemented` (reachability catalog API deferred to Phase 11+).
- No CLI/JSON/exit-code/storage/schema/backend behavior changed.

## Phase 7 update (v1.12)

- `restore` by logical file ID (live and dry-run execution paths) is routed through the engine.
- `Engine` interface expanded from 8 to 9 active methods: `Restore` added.
- `DefaultEngine.Restore` is active for `RestoreModeFileIDs`; `RestoreModeStoredPath` returns
  `ErrNotImplemented` (explicit deferral).
- CLI now uses an engine-backed seam for restore-by-ID execution (`restoreByIDPhase`) in both
  live and dry-run modes, while preserving existing batch reporting, JSON envelope shape,
  and exit-code semantics.
- Stored-path restore (`--stored-path`) remains direct-wired to storage and is deferred until
  full destination-mode parity through engine is proven.
- `LoadRestorePlanMetadata` remains `ErrNotImplemented` (catalog restore-plan boundary deferred).

## Phase 8 update (v1.12)

- `store` single-file mode is routed through the engine.
- `Engine` interface expanded from 9 to 10 active methods: `Store` added.
- `DefaultEngine.Store` is active for non-recursive single-file requests; recursive folder mode
  returns `ErrNotImplemented` (explicit deferral in this phase).
- CLI `store` now uses an engine-backed seam (`storeByFilePhase`) while preserving text/JSON output
  shape and exit-code behavior.
- `store-folder` remains direct-wired to storage and is deferred until worker/folder orchestration
  parity through engine is fully proven.
- `LoadChunkPlacements` remains `ErrNotImplemented` (catalog placement boundary deferred).

## Phase 9 update (v1.12)

- `remove` by logical file ID (live and dry-run execution paths) is routed through the engine.
- `Engine` interface expanded from 10 to 11 active methods: `Remove` added.
- `DefaultEngine.Remove` is active for `RemoveModeFileIDs`; stored-path modes
  (`RemoveModeStoredPath`, `RemoveModeStoredPaths`) return `ErrNotImplemented`
  (explicit deferral in this phase).
- CLI remove-by-ID now uses an engine-backed seam (`removeByIDPhase`) while preserving existing
  batch report shape, JSON envelope fields, human output, and exit-code behavior.
- Stored-path remove (`--stored-path`) and stored-path batch remove (`--stored-paths`) remain
  direct-wired to storage and are deferred until dedicated parity coverage is added.
- Repair and recovery routing remain deferred in this phase to preserve current corrective and
  startup fail-safe semantics without broad orchestration changes.

## Phase 10 update (v1.12)

- `verify` execution now routes through an engine-backed seam (`verifyCommandPhase`) that invokes
  `Engine.Verify` using injected DB ownership from `loadDefaultStorageContextPhase`.
- verify summary collection remains CLI-rendered but no longer opens a second global DB connection;
  summary queries now run against the same injected DB handle used for verify execution.
- Human output, JSON envelope/fields, usage validation, and exit-code behavior for `verify`
  remain unchanged.
- Dependency guardrail strengthened: `cmd/coldkeep` is now explicitly blocked from importing
  `internal/catalog` directly by test.
- Existing routed scopes remain unchanged; no new high-risk command migration was activated.

## Phase 11 update (v1.12)

- Phase 11 is proof-only: no new command routing and no broad coupling refactor.
- Consolidated routed operation -> invariant -> test mapping added in
  `docs/release/v1.12/invariant-test-matrix.md`.
- Added routed text parity evidence for existing engine-mediated commands:
  - verify: `TestVerifySystemEngineRoutingText`
  - store single-file: `TestStoreByFileEngineRoutingText`
  - restore by ID live: `TestRestoreByIDEngineRoutingText`
  - remove by ID live: `TestRemoveByIDEngineRoutingText`
- Existing dependency-direction coupling guards remain active:
  - CLI must not import catalog directly (`internal/engine/dependency_guard_test.go`)
  - catalog must not import engine/CLI (`internal/catalog/dependency_test.go`)
- Existing routed scope and explicit deferrals remain unchanged.

## Direct DB access patterns

Search targets:

- `db.ConnectDB`
- `QueryContext`
- `QueryRowContext`
- `ExecContext`
- `BeginTx`
- `sql.DB`
- `sql.Tx`

## Direct storage context patterns

Search targets:

- `OpenStorageContext`
- `LoadDefaultStorageContext`
- `storage.Context`
- `containersDir` / `container.ContainersDir`
- repository config loading

## Rendering mixed with behavior

Search targets:

- `fmt.Print`
- `fmt.Println`
- `log.Print`
- `os.Stdout`
- `os.Stderr`
- clirender calls inside logic-heavy code

## Validation duplicated outside engine

Search targets:

- path validation
- worker validation
- limit validation
- snapshot ID validation
- stored-path validation
- output mode validation
