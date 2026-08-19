# Engine Boundary Plan

This document records the engine/catalog boundary direction across releases.
It is a current-state architectural planning document rather than a frozen
public contract.

## Boundary rules

```
cmd/coldkeep          may import internal/engine
internal/engine       may import domain packages
domain packages       must not import internal/engine
internal/engine       must not import cmd/coldkeep
```

These rules are enforced by `internal/engine/dependency_guard_test.go`.

## Current active engine boundary

The active engine interface now owns these routed operations:

- `Stats`
- `Inspect`
- `Verify`
- `SnapshotList`
- `SnapshotShow`
- `SnapshotStats`
- `SnapshotDiff`
- `SnapshotCreate`
- `SnapshotDelete`
- `SnapshotRestore`
- `GarbageCollect`
- `Store`
- `Restore`
- `RestoreStoredPath`
- `Remove`
- `RemoveStoredPaths`

The active restore/remove topology established in v1.13.8 is:

- by-ID restore:
  `CLI -> Engine.Restore -> storage restore by logical file ID`
- stored-path restore:
  `CLI -> Engine.RestoreStoredPath -> storage.RestoreFileByStoredPathWithStorageContextResultOptions`
- by-ID remove:
  `CLI -> Engine.Remove -> storage.RemoveFileWithDBResult`
- stored-path remove:
  `CLI -> Engine.RemoveStoredPaths -> storage.RemoveFileByStoredPathWithStorageContextResult`

Method selection now owns restore/remove addressing semantics. No active
restore/remove request still uses an addressing-mode enum.

## Ownership model

- CLI owns:
  - flag and positional parsing
  - command-shape validation
  - batch-input file reading
  - human and JSON rendering
  - performance spans
  - compatibility projection
- Engine owns:
  - typed requests and results
  - request-level validation
  - addressing semantics through method selection
  - stored-path target preparation
  - dry-run/live dispatch
  - fail-fast behavior
  - operation result meaning
- Storage/domain owns:
  - catalog lookup and mutation
  - destination/path safety
  - overwrite behavior
  - metadata application
  - transactions
  - ref-count transitions
  - snapshot-retention enforcement
  - payload reconstruction
  - temporary chunk pinning
- GC alone owns physical payload reclamation.

## Active snapshot-mutation boundary

v1.13.9 activated snapshot mutation at the production CLI boundary:

- `CLI -> Engine.SnapshotCreate -> snapshot domain`
- `CLI -> Engine.SnapshotDelete -> snapshot domain`
- `CLI -> Engine.SnapshotRestore -> restore/domain seams`

Snapshot create is atomic, snapshot delete is metadata-only and preserves
retained content, and snapshot restore retains explicit destination and
selection semantics. This activation does not make every snapshot read-side
workflow or the daemon/API contract complete.

## Current mandatory completion boundaries

- `Engine.Store` remains single-file only; v1.13.12 must add a distinct
  engine-owned folder/recursive store operation and remove the misleading
  unsupported recursive request surface.
- Snapshot list/show/stats/diff remain active but provisional mixed read-side
  seams. v1.13.12 must complete their engine ownership and neutral contracts.
- Repair, recovery, startup recovery, and doctor remain CLI/domain-owned
  corrective work. v1.13.12 must activate explicit headless engine operations
  and move correctness sequencing behind them.
- Snapshot graph, placement, restore-plan, and GC-plan catalog surfaces remain
  unimplemented. v1.13.12 must implement and adopt all four in production.

These items are mandatory v1.x architecture work. They are not v2.x
deferrals. SQLite-default productization and broader distributed coordination
remain the separate v2.x boundaries.

## Migration rule

No command is routed through the engine unless its request/result contract can
represent the existing command behavior without dropping safety or compatibility
semantics.

When orchestration moves behind the engine:

```
cmd/coldkeep          must not import internal/catalog directly
internal/engine       may import internal/catalog
internal/catalog      must not import internal/engine
internal/catalog      must not import cmd/coldkeep
```

The catalog facade owns logical identity, physical mapping, snapshot graph,
reachability, GC eligibility, restore-plan inputs, placement, and verification
expectations. Storage owns payload bytes and block/container representation.
The engine owns operation coordination, validation, and safety-boundary
enforcement.

## Required invariants

```
GC must never delete reachable data.
Restore must never write outside the intended destination.
Verify must fail closed on inconsistent catalog/storage state.
Recovery must not legitimize corrupt mappings.
Packed and legacy storage behavior must remain aligned.
CLI parsing must not be the only place where correctness invariants live.
Engine/catalog APIs must not weaken existing safety guarantees.
```
