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
