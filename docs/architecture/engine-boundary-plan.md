# Engine Boundary Plan

This document records the current Engine/Catalog/CLI architecture and the
remaining product-boundary direction. It is a current-state architectural
document rather than a frozen public contract or a historical release report.

## Boundary rules

```
cmd/coldkeep          may import internal/engine
internal/engine       may import domain packages
domain packages       must not import internal/engine
internal/engine       must not import cmd/coldkeep
internal/application  composes engine, database, and storage resources
internal/catalog      must not import internal/engine or cmd/coldkeep
```

These rules are enforced by the Engine, application-composition, and thin-CLI
dependency guards.

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
- `PlanGarbageCollection`
- `Store`
- `StoreFolder`
- `ListFiles`
- `SearchFiles`
- `GetConfiguration`
- `SetConfiguration`
- `Repair`
- `Recover`
- `Doctor`
- `Restore`
- `RestoreStoredPath`
- `Remove`
- `RemoveStoredPaths`

This is the complete 25-operation v1 Engine surface. Each operation has a
neutral request/result contract, a stable typed Engine error boundary, an
implementation, and a production route through the application composition
root.

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
- Catalog owns:
  - logical identity and physical mapping decisions
  - current-file and repository-configuration metadata
  - snapshot graph and reachability roots
  - authoritative packed/legacy placement
  - restore-plan and GC-plan metadata
- Storage/domain owns:
  - destination/path safety
  - overwrite behavior
  - metadata application
  - transactions
  - ref-count transitions
  - snapshot-retention enforcement
  - payload reconstruction
  - temporary chunk pinning
- GC alone owns physical payload reclamation.

## Snapshot boundary

v1.13.9 activated snapshot mutation at the production CLI boundary:

- `CLI -> Engine.SnapshotCreate -> snapshot domain`
- `CLI -> Engine.SnapshotDelete -> snapshot domain`
- `CLI -> Engine.SnapshotRestore -> restore/domain seams`

Snapshot create is atomic, snapshot delete is metadata-only and preserves
retained content, and snapshot restore retains explicit destination and
selection semantics. Snapshot list/show/stats/diff and create/delete/restore
are Engine-owned for v1. A future daemon/API transport remains v2 scope and is
not implied by this local headless boundary.

## Current v1 completion state

- Engine architecture is complete for v1.
- Catalog architecture is complete for the frozen v1 contract: all thirteen
  methods are neutral, the graph/placement/restore-plan/GC-plan authorities are
  implemented and adopted where required, and all five formerly affected
  aggregate methods expose stable typed, cause-preserving Catalog errors.
- The production CLI is thin for v1: parsing, input ingestion, rendering,
  prompts, compatibility projection, outer coordination, and composition stay
  outside the headless Engine; production operation execution stays behind it.
- Application composition remains intentionally narrow and is not an open v1
  blocker.

This does not claim that Catalog owns every conceivable future persistence
operation. It states completion only against the frozen v1 responsibility
interfaces and production-adoption contract.

## Restore publication boundary

Every planner resolves an exact file destination before storage mutation. The
common secure installer retains trusted parent and temporary-object identity,
publishes no-overwrite restores atomically without replacement, and performs
intentional overwrite through the platform's replacing primitive. Metadata is
applied after publication through retained object identity; strict metadata
failure may therefore return an error after correct bytes are visible.

The native Linux, macOS, and Windows behavior is proven within the frozen
same-host/local-filesystem threat bound. Replacing a checked path with a
symlink or reparse point cannot redirect create, publish, metadata, or cleanup.
Arbitrary hostile same-user relocation of an already-open Unix directory is
outside that guarantee.

## Verification placement boundary

File-deep verification consumes `Catalog.LoadChunkPlacements` as the placement
authority. It checks every authoritative `LEGACY_ONLY`, `PACKED_ONLY`, and
`MIXED` recipe entry and fails closed on missing, conflicting, incomplete, or
corrupt placement. Storage/codec support remains limited to the existing
repository format contract.

## Backend and coordination boundary

SQLite and PostgreSQL are supported v1 backends within their documented
capability bounds. Snapshot-label filtering has one backend-neutral, narrow
ASCII case-insensitive substring contract. PostgreSQL compatibility is
retained; making SQLite the default repository-local product experience is v2
productization, while centralized PostgreSQL server product mode is v3.

Participating v1 operations, including valid `simulate gc`, use cooperative
same-process/same-host/local-filesystem coordination with native Linux, macOS,
and Windows behavior. Isolated `simulate store` and `simulate store-folder`
remain bypasses. PostgreSQL advisory-session ownership is an additional
backend barrier where applicable, not a cross-host repository lock. NFS, SMB,
NAS, cross-machine, and distributed coordination remain v3 scope.

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

The Catalog facade owns logical identity, physical mapping, snapshot graph,
reachability, GC eligibility, restore-plan inputs, placement, and verification
expectations. Storage owns payload bytes and block/container representation.
The Engine owns operation coordination, validation, and safety-boundary
enforcement.

## Product handoff boundary

V2 owns the SQLite-default repository-local product, local daemon/mutation
owner, queue and maintenance barriers, daemon-backed CLI, local API/UI,
scheduling, catalog transfer/backup, and local recovery workflows while
retaining PostgreSQL compatibility. V3 owns network exposure, NAS/multiple
roots, NFS/SMB semantics, centralized PostgreSQL product mode, multi-user/auth,
cloud/object storage, replication/sync, and distributed coordination.

All currently identified and frozen v1.13.13 blocker roots are technically
closed, and Phase 13 approved formal v1.x normative completion. Merge, tag,
publication, and v2 implementation remain separate later-phase decisions.

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
