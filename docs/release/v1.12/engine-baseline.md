# v1.11 Engine Baseline

This document records what v1.11 actually delivered, so v1.12 does not over-assume. v1.11 introduced
the facade; v1.12 must not pretend the placeholder contracts are already sufficient.

## Existing package

- `internal/engine` exists.

Files:

- `internal/engine/engine.go` — `Engine` interface.
- `internal/engine/types.go` — active request/result types.
- `internal/engine/default_engine.go` — `DefaultEngine`, `Config`, `New`.
- `internal/engine/candidates.go` — inactive mutating candidates.
- `internal/engine/engine_test.go` — engine tests.
- `internal/engine/dependency_guard_test.go` — dependency-direction guard.
- `internal/engine/backend_compat_test.go` — backend-neutrality tests.

## Existing active engine methods

The `Engine` interface exposes only:

- `Stats(ctx, StatsRequest) (StatsResult, error)`
- `Inspect(ctx, InspectRequest) (InspectResult, error)`
- `Verify(ctx, VerifyRequest) (VerifyResult, error)`

`DefaultEngine` implements these as wrapper-only delegations:

- `Stats` → `observability.Service.Stats`
- `Inspect` → `observability.Service.Inspect` (with `validateInspectRequest`)
- `Verify` → `maintenance.VerifyCommandWithContainersDir` (with `validateVerifyRequest`)

## Config

```go
type Config struct {
    DB           *sql.DB // caller owns connection lifetime
    ContainerDir string  // defaults to container.ContainersDir if empty
}
```

`New(cfg Config)` requires `cfg.DB != nil` and constructs an `observability.Service` from it.

## Confirmed CLI routing

- `stats` is routed through the engine (`runObservabilityStatsPhase` in `cmd/coldkeep/main.go`).
- `inspect` is NOT routed; it calls `observability.Service.Inspect` directly.
- `verify` is NOT routed; it calls `maintenance.VerifyCommandWithContainersDir` directly.
- All mutating commands (store, store-folder, restore, remove, gc, snapshot, repair, recovery) call
  domain packages directly; none are routed through the engine.

## Existing candidate contracts

Active (v1.11):

| Type | Fields |
| --- | --- |
| `StatsRequest` | `IncludeContainers bool`, `Trace observability.TraceOptions` |
| `StatsResult` | `Raw *observability.StatsResult` |
| `InspectRequest` | `Entity observability.EntityType`, `EntityID string`, `Options observability.InspectOptions` |
| `InspectResult` | `Raw *observability.InspectResult` |
| `VerifyRequest` | `Level string`, `Target string`, `FileID int` |
| `VerifyResult` | (empty) |

Inactive mutating candidates (`candidates.go`, explicitly "not part of the active v1.11 Engine
interface"): `StoreRequest`/`StoreResult`, `RestoreRequest`/`RestoreResult`,
`RemoveRequest`/`RemoveResult`, `SnapshotCreateRequest`/`SnapshotCreateResult`,
`SnapshotRestoreRequest`/`SnapshotRestoreResult`, `SnapshotDeleteRequest`/`SnapshotDeleteResult`,
`GarbageCollectRequest`/`GarbageCollectResult`, `RepairRequest`/`RepairResult`,
`RecoverRequest`/`RecoverResult`.

## Phase 2 update (v1.12) — Engine contract expansion

Phase 2 expanded the thin v1.11 placeholders in `candidates.go` into realistic, renderer-neutral,
backend-neutral contracts that can represent the existing CLI behavior before any routing happens.
The active `Engine` interface is unchanged (still `Stats`, `Inspect`, `Verify`); no method was added
and no command was routed. This is type/contract + test + doc work only.

### Engine contract rules

- Engine requests represent operation intent, not CLI syntax.
- Engine results represent operation outcomes, not human or JSON rendering.
- CLI owns argument parsing and rendering only.
- Engine owns operation validation that protects correctness.
- Catalog/storage-specific details must be represented without leaking SQL dialects.
- Contracts must be rich enough to preserve existing CLI behavior before routing.
- Contracts must remain backend-neutral (no `database/sql`, driver, or dialect fields) and
  renderer-neutral (no `io.Writer`, `cobra`, stdout/stderr, or interface fields).

### Shared operation-neutral types (new)

- `OperationWarning{Code, Message, Detail}` — structured warnings instead of stderr text.
- `ExecutionMode` (`sequential`/`parallel`), `BatchItemStatus` (`ok`/`failed`/`skipped`),
  `BatchSummary{OK, Failed, Skipped}` — batch outcome representation.
- `SnapshotQuery{Path, Prefix, Pattern, Regex, MinSize, MaxSize, ModifiedAfter, ModifiedBefore,
  Limit}` — shared snapshot file-selection filters.

### Expanded operation contracts

| Operation | Modes now representable | Notes |
| --- | --- | --- |
| Store / store-folder | single file, recursive folder, codec, workers | `StoreResult` carries hash, stored path, already-stored, chunk created/reused, byte sizes, warnings. |
| Restore | file-ID batch; stored-path with original/prefix/override destination; overwrite/strict/no-metadata; dry-run/fail-fast/workers/limit | Per-item `RestoreItemResult` + `BatchSummary`; `RestoreMode` and `RestoreDestinationMode` enums. |
| Remove | file-ID batch, single stored-path, stored-paths batch; dry-run/fail-fast | Per-item `RemoveItemResult` with `RemainingRefCount`/`Removed`; `RemoveMode` enum. |
| GC | dry-run/live, workers | Result carries affected containers, container filenames, snapshot/current/shared retention breakdown (packed + legacy neutral), bytes reclaimed. |
| Snapshot create | full vs partial, `--id`/`--label`/`--from` | `SnapshotType` enum; result carries type/paths/files-inserted/parent. |
| Snapshot list | type/label/since/until/limit/tree | `SnapshotListResult` with `SnapshotMeta` rows and tree lines. |
| Snapshot show (files) | query filters, limit | `SnapshotShowResult` with `SnapshotFile` entries + matched/total counts. |
| Snapshot stats | all-snapshots or per-snapshot; reuse metrics | `HasReuse` gates reuse/new/ratio. |
| Snapshot diff | summary fast-path, added/removed/modified filter, query | `SnapshotDiffSummary` + optional `SnapshotDiffEntry` list. |
| Snapshot delete | force vs dry-run; lineage/impact | Result carries parent, children, total/unique/shared files, warnings. |
| Snapshot restore | partial paths, destination modes, overwrite/strict/no-metadata, query | Shares `RestoreDestinationMode`. |
| Repair | single `ref-counts`/`chunk-live-ref-counts`, batch, fail-fast | `RepairTarget` enum; per-target scanned/updated/orphan rows. |
| Recovery | corrective dry-run; quarantine/abort/sealing report | Re-modeled from the previous (incorrect) restore-like placeholder. |

### Intentionally deferred

- `RestoreRequest.InputPath` and `RemoveRequest.InputPath` / `RepairRequest.InputPath`: whether batch
  input-file parsing remains a CLI-level concern or moves into the engine is decided in the relevant
  migration phase (Restore Phase 7, Remove/Repair Phase 9). The field is retained so the contract can
  represent the existing command; it does not commit to engine-side file parsing.
- No `OperationError` taxonomy was introduced; engine `error` returns remain the error channel.
  A structured error taxonomy is out of scope for v1.12 Phase 2.
- No interface methods or implementations were added (contract preparation only). Activation/routing
  happens in later phases per `release-phase-list.md`.

### Contract tests added

- `internal/engine/contracts_test.go`:
  - `TestCandidateContractFieldTypesAreNeutral` — recursive field-type walk rejecting any field whose
    type comes from a package other than the engine package, `time`, or built-ins (catches `io.Writer`,
    `*sql.DB`, `*cobra.Command`, interfaces, channels, funcs even if named innocuously).
  - Per-operation representability tests proving restore (all modes), remove (all modes), GC
    (dry-run/live + retention), snapshot create/list/show/stats/diff/delete/restore, repair
    (targets + batch), recovery (corrective report), and store (file + folder) are expressible.
- `internal/engine/candidates_test.go` and `backend_compat_test.go` neutrality lists extended to cover
  all new types.
- `TestEngineActiveInterfaceRemainsReadOriented` still passes (interface unchanged).

### Risk

- `CK-112-R002` (thin mutating candidate contracts) is marked **fixed** with evidence: the major
  operation modes are now representable, deferrals are documented above, and construction/neutrality
  tests pass. Activation remains gated by later phases.


## Dependency guard

`internal/engine/dependency_guard_test.go` (`TestEngineDependencyDirection`) enforces:

1. `internal/engine` must not import `cmd/coldkeep` (transitively).
2. Non-engine `internal/*` packages must not import `internal/engine`.
3. `cmd/coldkeep` may import `internal/engine` (allowed).

## Baseline concern (resolved in Phase 1 / v1.12.1)

`DefaultEngine.Verify` previously passed `Config.ContainerDir` to
`maintenance.VerifyCommandWithContainersDir`, which opened its own DB connection via `db.ConnectDB()`
and ignored `Config.DB`. The active `Verify` method now delegates to
`maintenance.VerifyCommandWithDBAndContainersDir(Config.DB, ...)`, honoring the engine-owned DB.
Regression coverage lives in `internal/engine/verify_db_ownership_test.go`. Tracked as `CK-112-R001`
(fixed).

`Stats` and `Inspect` were reviewed for the same risk and are clean: both delegate through
`observability.Service`, which uses the injected `Config.DB` (`maintenance.RunStatsResultWithDB`,
`maintenance.CollectBlockStats`). Neither reopens a global DB connection.

## Phase 4 update (v1.12) — Dual-backend catalog baseline

Dual-backend contract tests added in `internal/catalog/backend_contract_test.go`. SQLite runs
unconditionally; PostgreSQL gated by `COLDKEEP_TEST_DB`. A timestamp-bind incompatibility in
`ListSnapshots` was uncovered and fixed (bind `time.Time` directly). Dialect rules documented in
`sqlite-postgres-baseline.md`. `CK-112-R003` fixed.

## Phase 5 update (v1.12) — Snapshot read-side routing

The `Engine` interface now has 7 active methods:

| Method | Status |
|---|---|
| `Stats(ctx, StatsRequest) (StatsResult, error)` | active since v1.11 |
| `Inspect(ctx, InspectRequest) (InspectResult, error)` | active since v1.11 |
| `Verify(ctx, VerifyRequest) (VerifyResult, error)` | active since v1.11; DB ownership fixed v1.12.1 |
| `SnapshotList(ctx, SnapshotListRequest) (SnapshotListResult, error)` | added Phase 5 |
| `SnapshotShow(ctx, SnapshotShowRequest) (SnapshotShowResult, error)` | added Phase 5 |
| `SnapshotStats(ctx, SnapshotStatsRequest) (SnapshotStatsResult, error)` | added Phase 5 |
| `SnapshotDiff(ctx, SnapshotDiffRequest) (SnapshotDiffResult, error)` | added Phase 5 |

All new methods use `e.config.DB` (no global reopen). CLI routing via engine-backed closures in
`cmd/coldkeep/main.go`. Snapshot create/delete/restore deferred. `ParentSnapshotID` correctness
bug fixed in `SnapshotStatsResult`.

## Phase 6 update (v1.12) — GC routing

The `Engine` interface now has 8 active methods:

| Method | Status |
|---|---|
| `Stats(ctx, StatsRequest) (StatsResult, error)` | active since v1.11 |
| `Inspect(ctx, InspectRequest) (InspectResult, error)` | active since v1.11 |
| `Verify(ctx, VerifyRequest) (VerifyResult, error)` | active since v1.11; DB ownership fixed v1.12.1 |
| `SnapshotList(ctx, SnapshotListRequest) (SnapshotListResult, error)` | added Phase 5 |
| `SnapshotShow(ctx, SnapshotShowRequest) (SnapshotShowResult, error)` | added Phase 5 |
| `SnapshotStats(ctx, SnapshotStatsRequest) (SnapshotStatsResult, error)` | added Phase 5 |
| `SnapshotDiff(ctx, SnapshotDiffRequest) (SnapshotDiffResult, error)` | added Phase 5 |
| `GarbageCollect(ctx, GarbageCollectRequest) (GarbageCollectResult, error)` | added Phase 6 |

`DefaultEngine.GarbageCollect` delegates to `maintenance.RunGCWithDB` (new DB-aware entry point).
Live GC is refused on SQLite; dry-run is supported on both. `LoadGCPlanMetadata` remains
`ErrNotImplemented` (reachability catalog API deferred).

## Phase 7 update (v1.12) — Restore-by-ID routing

The `Engine` interface now has 9 active methods:

| Method | Status |
|---|---|
| `Stats(ctx, StatsRequest) (StatsResult, error)` | active since v1.11 |
| `Inspect(ctx, InspectRequest) (InspectResult, error)` | active since v1.11 |
| `Verify(ctx, VerifyRequest) (VerifyResult, error)` | active since v1.11; DB ownership fixed v1.12.1 |
| `SnapshotList(ctx, SnapshotListRequest) (SnapshotListResult, error)` | added Phase 5 |
| `SnapshotShow(ctx, SnapshotShowRequest) (SnapshotShowResult, error)` | added Phase 5 |
| `SnapshotStats(ctx, SnapshotStatsRequest) (SnapshotStatsResult, error)` | added Phase 5 |
| `SnapshotDiff(ctx, SnapshotDiffRequest) (SnapshotDiffResult, error)` | added Phase 5 |
| `GarbageCollect(ctx, GarbageCollectRequest) (GarbageCollectResult, error)` | added Phase 6 |
| `Restore(ctx, RestoreRequest) (RestoreResult, error)` | added Phase 7 |

`DefaultEngine.Restore` is active for `RestoreModeFileIDs` (live and dry-run). It uses the
injected `Config.DB` and engine-provided container directory (`Config.ContainerDir` fallback
preserved through `storage.StorageContext.EffectiveContainerDir`). Stored-path restore remains
deferred in engine (`ErrNotImplemented`) pending full destination-mode parity validation.

CLI routing scope in Phase 7 is intentionally narrow: restore-by-ID live and dry-run execution are
routed through an engine-backed seam; stored-path and snapshot restore remain direct paths.

## v1.12 implication

Do not activate mutating commands through the engine until request/result contracts are expanded
enough to preserve real command behavior. The inactive v1.11 candidates are placeholders, not final
contracts. For example, `RestoreRequest` lacks stored-path mode, overwrite semantics, destination
mode, worker/limit behavior, and safety validation. v1.12 expands contracts operation by operation.
