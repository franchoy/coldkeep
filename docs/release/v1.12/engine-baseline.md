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

## Dependency guard

`internal/engine/dependency_guard_test.go` (`TestEngineDependencyDirection`) enforces:

1. `internal/engine` must not import `cmd/coldkeep` (transitively).
2. Non-engine `internal/*` packages must not import `internal/engine`.
3. `cmd/coldkeep` may import `internal/engine` (allowed).

## Baseline concern (deferred to Phase 1)

`DefaultEngine.Verify` passes `Config.ContainerDir` to
`maintenance.VerifyCommandWithContainersDir`, which opens its own DB connection via `db.ConnectDB()`
and ignores `Config.DB`. The active `Verify` method must use the engine-provided DB before `verify`
is routed through the engine. Tracked as `CK-112-R001`.

## v1.12 implication

Do not activate mutating commands through the engine until request/result contracts are expanded
enough to preserve real command behavior. The inactive v1.11 candidates are placeholders, not final
contracts. For example, `RestoreRequest` lacks stored-path mode, overwrite semantics, destination
mode, worker/limit behavior, and safety validation; `GarbageCollectRequest` lacks richer plan/result
semantics. v1.12 should expand contracts operation by operation.
