# v1.12 Catalog Boundary Map

Purpose: define what the catalog facade will own before creating it (Phase 3). This document is a
boundary declaration only; no `internal/catalog` package is created in Phase 0.

## Principle

The database is a catalog contract, not merely persistence.

## Catalog owns

- logical identity;
- physical file mapping;
- snapshot graph;
- reachability;
- GC eligibility;
- restore-plan metadata;
- chunk/block/container placement;
- verification expectations;
- migration-visible metadata compatibility.

## Storage owns

- payload bytes;
- sealed containers;
- packed blocks;
- compression representation;
- encryption representation;
- filesystem persistence mechanics.

## Engine owns

- operation entry points;
- request validation;
- mutation ordering;
- catalog/storage coordination;
- deterministic operation results;
- stable operation-level errors;
- safety boundary enforcement.

## Verify/recovery owns

- reconciliation between catalog truth and storage reality;
- detection of missing/corrupt payloads;
- quarantine semantics;
- proof that restore and GC decisions remain safe.

## Packed and legacy representation

The catalog facade must represent both packed and legacy storage roots explicitly. GC reachability
and restore planning must not silently assume one representation. Packed/legacy parity is a v1.12
invariant.

## GC-plan boundaries

- Restore-plan inputs (logical → physical resolution, stored-path resolution, destination/overwrite
  decisions) become catalog-backed in Phase 7. Safety validation (no write outside destination,
  traversal/symlink safety) must be enforced at engine/catalog level, not only in the CLI.
- GC-plan inputs (reachability, deletion eligibility) become catalog-backed in Phase 6. The invariant
  "GC must never delete reachable data" must be tested at the engine/catalog boundary.

## Phase 3 status

`internal/catalog` package created. Implemented boundaries:

- **Logical identity** — `FindLogicalFile` wraps `logical_file` SELECT.
- **Physical file mapping** — `FindPhysicalFilesForLogicalFile` wraps `physical_file` SELECT.
- **Snapshot metadata** — `FindSnapshot` and `ListSnapshots` wrap `snapshot` SELECT with filter.
- **Reachability** — `LoadReachabilityRoots` queries `physical_file` (current) and `snapshot_file`
  (snapshot-protected) with `SELECT DISTINCT logical_file_id`.

Deferred boundaries (return `ErrNotImplemented`):

- **Snapshot graph** — `LoadSnapshotGraph` deferred to Phase 5/6.
- **Chunk/block placement** — `LoadChunkPlacements` deferred to Phase 7/8; must unify packed
  (`storage_blocks`/`chunk_block_refs`) and legacy (`blocks`) roots.
- **Restore-plan metadata** — `LoadRestorePlanMetadata` deferred to Phase 7.
- **GC-plan metadata** — `LoadGCPlanMetadata` deferred to Phase 6.

## Phase 4 status

Implemented boundaries are now proven equivalent across SQLite and PostgreSQL by the dual-backend
contract harness (`internal/catalog/backend_contract_test.go`). One backend-sensitive point was found
and isolated: timestamp filter binds in `ListSnapshots` must pass `time.Time` (not a pre-formatted
RFC3339 string) so go-sqlite3 and lib/pq compare consistently. This does not move the boundary; it
confirms the catalog owns timestamp comparison semantics and must keep them backend-neutral. See
`sqlite-postgres-baseline.md` for the full dialect rules.

## Phase 6 status

GC routing is complete, but reachability/deletion-plan catalog boundaries are intentionally deferred.
The current GC orchestration (`maintenance.RunGCWithDB`) computes reachability and deletion
eligibility directly via SQL — it is not routed through `LoadGCPlanMetadata`. The engine
(`DefaultEngine.GarbageCollect`) wraps this DB-aware function. The catalog boundary for GC is
therefore: engine → maintenance.RunGCWithDB (not yet: engine → catalog.GCPlanCatalog → maintenance).

`LoadGCPlanMetadata` remains `ErrNotImplemented`. Activating the reachability catalog API for GC
is deferred to a future phase because:

1. GC correctness is preserved (the reachability query inside `RunGCWithDB` is unchanged).
2. The packed/legacy root unification required for `LoadGCPlanMetadata` shares work with
   `LoadChunkPlacements` (Phase 7/8); doing it separately now adds duplicate complexity.
3. The safety invariant "GC must never delete reachable data" is tested at the engine level
   (`TestGCDryRunThroughEngineEmptyDB`, `TestGCLiveRefusedOnSQLite`).

## Phase 7 status

Restore-by-ID routing is complete at the engine boundary, but restore-plan catalog boundaries are
intentionally deferred.

- `Engine.Restore` is active for `RestoreModeFileIDs`.
- Stored-path restore and snapshot restore are not routed through engine restore planning in this
  phase.
- `LoadRestorePlanMetadata` remains `ErrNotImplemented`.

Current boundary shape for restore in Phase 7:

- restore-by-ID (live and dry-run): CLI batch orchestration -> engine.Restore -> storage restore pipeline.
- stored-path restore: CLI -> storage direct path (destination-mode safety unchanged).

Deferral rationale:

1. Destination-mode parity (`original`/`prefix`/`override`) and strict/no-metadata semantics for
  stored-path restore require full route-equivalence tests before safe activation behind engine.
2. Existing traversal/symlink safeguards are already enforced in `pathsafe` + storage/snapshot
  planning paths; deferral avoids a broad refactor in a correctness-critical phase.
3. This keeps the Phase 7 diff narrow while moving restore orchestration onto the engine for the
  highest-volume path (file-ID restore).

## Phase 8 status

Single-file store routing is complete at the engine boundary, but placement catalog boundaries are
intentionally deferred.

- `Engine.Store` is active for non-recursive single-file mode.
- `store-folder` is not routed through engine store orchestration in this phase.
- `LoadChunkPlacements` remains `ErrNotImplemented`.

Current boundary shape for store in Phase 8:

- single-file store: CLI -> engine.Store -> storage store pipeline.
- folder store: CLI -> storage direct path.

Deferral rationale:

1. Folder worker/orchestration parity requires dedicated route-equivalence tests before activation.
2. Placement catalogization (`LoadChunkPlacements`) shares packed/legacy unification concerns with
  other deferred metadata boundaries; this phase keeps orchestration migration narrow.
3. Store correctness is preserved by delegating to existing storage-context-aware store functions
  without rewriting chunking, packing, or placement logic.

## Phase 9 status

Remove-by-ID routing is complete at the engine boundary, while other destructive/corrective paths
are intentionally deferred.

- `Engine.Remove` is active for `RemoveModeFileIDs`.
- remove-by-ID path now uses engine orchestration and preserves existing storage-layer invariants.
- stored-path remove modes remain on CLI -> storage direct paths in this phase.
- repair and recovery remain on direct maintenance/recovery paths in this phase.

Current boundary shape for Phase 9 operations:

- remove-by-ID: CLI batch orchestration -> engine.Remove -> storage remove pipeline.
- remove stored-path modes: CLI -> storage direct path.
- repair: CLI -> maintenance direct path.
- recovery: startup/doctor -> recovery direct path.

Deferral rationale:

1. Stored-path remove modes have distinct target parsing/addressing flows and need dedicated parity
  guards before safe engine activation.
2. Repair and recovery are correctness-critical corrective paths; routing is deferred until wrappers
  can be activated with tiny diffs and explicit equivalence tests for fail-safe semantics.
3. This phase keeps destructive-operation migration narrow while preserving snapshot-retention and
  reference-count safety behavior through existing storage invariants.

