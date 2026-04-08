# coldkeep Architecture

This document contains the internal architecture model for coldkeep.
It complements README.md, which is intentionally newcomer-first.
This document is intended for contributors and advanced users who need to understand system invariants and internal behavior.

## System Overview

coldkeep is a correctness-first, local-first, content-addressed storage engine.

The architecture composes:

- a logical file model
- content-addressed chunk identity
- physical block placement metadata
- append-only container files on disk
- lifecycle-aware recovery and verification paths

Correctness has two explicit layers:

- v1.0 storage correctness: deterministic restore, integrity, recovery, GC safety
- v1.1 interface correctness: batch CLI contract stability and deterministic orchestration

## Data Model

Core entities:

- logical_file: user-visible logical file (name, size, file hash, lifecycle state)
- chunk: content-addressed chunk identity (chunk hash, size, reference/pin counters, lifecycle state)
- file_chunk: ordered mapping between logical files and their chunks
- blocks: physical placement and codec metadata for each chunk
- container: physical append-only container file on disk

Storage pipeline:

```text
logical_file -> file_chunk -> chunk -> blocks -> container
```

## Container and Append Model

Containers are stored under:

```text
storage/containers/
```

Write model:

- append-only writes
- active container receives new blocks until sealed/rotated
- deterministic placement behavior is driven by ordered write path and metadata contracts

This model simplifies crash recovery by avoiding in-place mutation of already-written container payloads.

## Lifecycle Model

Lifecycle states:

- logical_file: PROCESSING -> COMPLETED -> ABORTED
- chunk: PROCESSING -> COMPLETED -> ABORTED

Lifecycle intent:

- PROCESSING: in-flight work
- COMPLETED: visible/eligible state after correctness conditions are satisfied
- ABORTED: interrupted/invalid in-flight state that must not be treated as committed data

Contributor note:

- The authoritative append lifecycle state machine is documented in internal/storage/store.go.
- Writer comments should point to that state machine instead of duplicating lifecycle logic.

## Core Invariants

These invariants should hold across store, restore, GC, recovery, and verification:

- every COMPLETED chunk has exactly one valid block record
- every block record references a valid container (or an explicitly quarantined/missing container state)
- every COMPLETED logical file has a complete, contiguous, ordered file_chunk graph
- live_ref_count > 0 protects a chunk from GC deletion
- pin_count > 0 protects a chunk from concurrent deletion while restore-like operations are active
- committed metadata implies referenced bytes are already durable on disk

## Validity and Restorability Model

A logical file is considered valid/restorable when:

- logical_file status is COMPLETED
- all referenced chunks are COMPLETED
- at least one referenced block is readable (loss-minimizing recovery can preserve metadata survivability)

Important nuance:

- metadata survivability is not restore success
- restore still succeeds only when full reconstruction passes final end-to-end file hash validation

Logical files with no restorable chunks are omitted from list/search visibility.

## Recovery Model (Corrective)

Recovery is corrective and state-changing by design.

Startup recovery responsibilities include:

- marking stale PROCESSING rows as ABORTED
- quarantining inconsistent/damaged container states where required
- preserving logical-file metadata when at least one referenced chunk remains restorable

Loss-minimizing behavior:

- logical files are only fully lost when all referenced chunks are unrecoverable
- partial internal reconstruction is never exposed as a successful restore artifact

No partially written or inconsistent state is exposed as valid user-visible data.

## Restore Model (Atomic and Hash-Gated)

Restore path behavior:

- reconstruct into a temporary file
- fsync + close temporary file
- atomically rename into destination
- fsync parent directory for durability
- validate final reconstructed file hash against stored file hash

Consequences:

- destination replacement is atomic at the visible path boundary
- incomplete or hash-mismatched reconstruction fails explicitly
- partial/corrupt output is not accepted as success

## GC Model (Reference Safe)

GC can reclaim only unreachable data:

- chunk is reclaimable only when live_ref_count == 0 and pin_count == 0
- container is deletable only when all resident chunks are reclaimable

This ensures reachable restore data is never deleted by GC.

## Verification Model

Verification levels:

- standard: metadata integrity checks
- full: metadata + container structure/consistency checks
- deep: full payload read and hash validation

Deep verification explicitly detects:

- payload tampering
- invalid offsets/bounds
- trailing unaccounted bytes after last valid block
- codec/authentication mismatches (for encrypted codec flows)

Operational note:

- verification phase is read-only
- CLI commands may still run startup recovery before verify begins

## Command Mutation Model

Mutation semantics by command family:

- store/store-folder: mutate metadata and physical data
- remove: mutate metadata and may make data GC-eligible
- gc: mutate metadata and containers (unless dry-run)
- startup recovery: corrective metadata mutation
- doctor: corrective (runs recovery before verify)
- verify: observational phase assumes recovered state

## Trust Boundary and Assumptions

Guarantees hold within the documented operating assumptions:

- database is not externally modified behind coldkeep
- container files are not manually altered
- filesystem honors write + fsync semantics
- PostgreSQL deployment provides expected transactional, locking, and advisory-lock behavior
- schema/bootstrap state matches the release migration expectations

## Interface Correctness Layer (v1.1)

Beyond storage-core correctness, v1.1 adds interface correctness for batch CLI orchestration:

- deterministic per-item ordering and reporting
- isolated execution semantics
- stable machine-readable status/summary/results envelopes
- automation-safe process exit behavior

These contracts are validated by targeted adversarial orchestration tests and tracked in VALIDATION_MATRIX.md under G9.

## Evolution Note: v1.2 physical_file Layer

v1.2 is planned to introduce a physical_file to logical_file relationship.
This is expected to extend the external model (path/folder restore semantics) while preserving core architecture pillars:

- chunk identity model
- container/block model
- lifecycle and recovery philosophy
- storage correctness guarantees G1-G8
- interface correctness direction from G9 onward

This means architecture documentation should evolve by extension, not by rewrite.
