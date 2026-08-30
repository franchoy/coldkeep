# coldkeep Architecture

This document contains the internal architecture model for coldkeep.
It complements [README.md](README.md), which is intentionally newcomer-first.
This document is intended for contributors and advanced users who need to understand system invariants and internal behavior.

Read [README.md](README.md) first if you need installation, quickstart, CLI examples, or the operator-facing contract summary.

Companion documents:

- [VALIDATION_MATRIX.md](VALIDATION_MATRIX.md) for guarantee-to-evidence mapping
- [COMPATIBILITY.md](COMPATIBILITY.md) for version-compatibility and chunker-evolution contract
- [CONTRIBUTING.md](CONTRIBUTING.md) for contributor workflow and local CI guidance
- [PRE_RELEASE_CHECKLIST.md](PRE_RELEASE_CHECKLIST.md) for release-gate execution
- [SECURITY.md](SECURITY.md) for the threat model and security limits
- [docs/PATH_IDENTITY.md](docs/PATH_IDENTITY.md) for current-state path identity policy
- [docs/BLOCK_ABSTRACTION_V18.md](docs/BLOCK_ABSTRACTION_V18.md) for v1.8 packed-block design lock constants and invariants
- [docs/storage_transform_semantics.md](docs/storage_transform_semantics.md) for the canonical logical/compressed/physical payload and hash semantics used by the v1.9 transform-aware metadata model

## System Overview

coldkeep is a correctness-first, local-first, content-addressed storage engine.

The architecture composes:

- a reusable headless Engine with neutral requests, results, and typed errors
- a backend-neutral Catalog for logical metadata and planning authority
- a thin local CLI for parsing, composition, coordination, and projection
- a logical file model
- content-addressed chunk identity
- physical block placement metadata
- append-only container files on disk
- lifecycle-aware recovery and verification paths

The v1 responsibility split is:

```text
CLI -> application composition -> Engine -> Catalog/domain/storage
```

Engine owns operation validation and orchestration. Catalog owns logical
identity, mapping, snapshot graph, reachability, authoritative placement,
restore-plan metadata, and GC-plan metadata. Storage owns payload bytes and
block/container representation. The 25-operation Engine, the frozen thirteen-
method Catalog contract (including stable typed errors), and the thin CLI are
complete for v1. Application composition remains intentionally narrow.

Correctness has ten explicit layers:

- v1.0 storage correctness: deterministic restore, integrity, recovery, GC safety
- v1.1 interface correctness: batch CLI contract stability and deterministic orchestration
- v1.2 physical-graph coherence: audited physical roots, explicit repair, invariant taxonomy, batch maintenance semantics
- v1.3 snapshot-based retention: immutable point-in-time captures, snapshot-protected GC, reachability audits
- v1.4 snapshot clarity hardening: lineage metadata is explicit and non-dependency by contract
- v1.5 chunker-evolution compatibility clarity: mixed-version repositories are first-class, write-default policy is explicit and new-writes-only
- v1.6 observability and simulation contract hardening: read-only introspection, exact GC simulation parity, tooling-safe trace channel behavior
- v1.7 deterministic performance foundation: controlled execution, benchmark-backed validation, and release-readiness safety proof without storage-format or schema-breaking changes
- v1.8 packed block abstraction: multiple chunks per physical storage block, AES-GCM packed-block integration, and release hardening while preserving all existing correctness guarantees
- v1.9 transform architecture freeze: block-level compression semantics, metadata-driven read path, explicit verify stages, and frozen storage contracts for engine extraction

Migration philosophy:

- coldkeep prefers non-destructive evolution over automatic optimization.

## Deep Design View

This deep version of the architecture captures five linked aspects:

- chunking system design (CDC + content-addressed recipes),
- chunker versioning model,
- explicit store and restore execution flow,
- invariant families that must remain true across lifecycle phases,
- supporting diagrams for system and flow understanding.

### Correctness Layers

This diagram is a mental anchor for how guarantees compose across layers.

```text
+------------------------------------------------------------+
| Transform Architecture Freeze (v1.9)                       |
|------------------------------------------------------------|
| Packed multi-chunk storage blocks (storage_blocks +        |
|   chunk_block_refs)                                        |
| Compression-before-encryption write ordering               |
| Logical/compressed/physical hash semantics                 |
| Explicit staged verification pipeline                      |
| Metadata-driven mixed-repository read path                 |
+------------------------------------------------------------+
    ^
    | extends snapshot retention layer
    |
+------------------------------------------------------------+
| Snapshot-Based Retention (v1.3 introduced, v1.4 clarified)|
|------------------------------------------------------------|
| Immutable point-in-time captures                           |
| Snapshots are self-contained (no parent content dependency)|
| Snapshot-protected GC: union of current-state              |
|   (physical_file) and snapshot (snapshot_file) roots      |
| Reachability integrity audits                              |
| Stats retention visibility                                 |
+------------------------------------------------------------+
    ^
    | extends physical graph layer
    |
+------------------------------------------------------------+
| Physical Graph Coherence (v1.2 - G10..G13)                |
|------------------------------------------------------------|
| Audited physical_file root graph                           |
| Explicit repair boundary (repair ref-counts)               |
| GC pre-flight integrity gate                               |
| Invariant error taxonomy + batch maintenance reporting     |
+------------------------------------------------------------+
    ^
    | extends interface layer
    |
+------------------------------------------------------------+
| Interface Correctness (v1.1 - G9)                         |
|------------------------------------------------------------|
| Deterministic batch CLI behavior                           |
| Stable JSON contracts                                      |
| Automation-safe execution semantics                        |
+------------------------------------------------------------+
    ^
    | requires storage guarantees
    |
+------------------------------------------------------------+
| Storage Correctness (v1.0 - G1..G8)                       |
|------------------------------------------------------------|
| Deterministic restore                                      |
| Content-addressed integrity                                |
| Crash-safe lifecycle and recovery                          |
| Reference-safe GC                                          |
+------------------------------------------------------------+
    ^
    | implemented by
    |
+------------------------------------------------------------+
| Physical Storage Model                                     |
|------------------------------------------------------------|
| logical_file -> chunk -> blocks/storage_blocks -> container|
| Append-only containers + transactional DB                  |
+------------------------------------------------------------+
```

## Data Model

Core entities:

- logical_file: user-visible logical file (name, size, file hash, lifecycle state)
- chunk: content-addressed chunk identity (chunk hash, size, reference/pin counters, lifecycle state)
- file_chunk: ordered mapping between logical files and their chunks
- blocks: physical placement and codec metadata for each chunk (legacy single-chunk layout)
- storage_blocks: v1.8+ packed physical block (container placement, block hash, codec, sizes, transform metadata)
- chunk_block_refs: v1.8+ per-chunk placement inside a packed storage block
- container: physical append-only container file on disk

Storage pipeline:

```text
logical_file -> file_chunk -> chunk -> blocks / storage_blocks+chunk_block_refs -> container
```

For transform-aware packed-block metadata semantics, including the canonical
definitions of logical payload, compressed payload, physical payload, and their
associated hashes and sizes, see [docs/storage_transform_semantics.md](docs/storage_transform_semantics.md).

## Chunking Model

coldkeep uses content-defined chunking (CDC).

Key properties:

- boundaries depend on input data characteristics,
- chunker versions implement boundary-strategy differences,
- persisted state is a chunked reconstruction recipe (`file_chunk -> chunk -> blocks`), not raw file-blob storage.

Example:

```text
File A (v1):
    [chunk1][chunk2][chunk3]

File B (v2):
    [chunk4][chunk5]
```

Even if content overlaps, chunk layout may differ across versions because boundary strategy differs.

## Chunker Versioning

Versioning model:

- each committed logical file records one `chunker_version` provenance label,
- repositories may contain mixed-version logical-file history,
- the effective chunker version is selected at store/write time,
- restore is recipe replay and does not require executing the active runtime chunker.

This separation is intentional: write-time chunker evolution changes future layout
behavior while restore compatibility remains metadata-driven.

## Store Flow (Write Path)

The store path is deterministic, transactional, and append-oriented.

High-level flow:

1. Select active write chunker version from repository configuration.
2. Chunk input bytes according to that version's CDC boundary strategy.
3. Resolve/reuse-or-create chunk identities under repository integrity rules.
4. Append physical block payloads to active container files as needed.
5. Persist logical recipe mapping (`logical_file`, `file_chunk`, `chunk`, `blocks`) transactionally.
6. Commit only when metadata and durable bytes satisfy completion invariants.

Non-obvious safety gate (important for future maintenance):

- completed-file/chunk reuse is never accepted on content hash alone;
- store runs structural and (mode-dependent) semantic replay validation before returning `AlreadyStored=true`;
- if a claimed completed candidate fails validation, store marks it aborted, cleans stale recipe links, and reclaims to rebuild a fresh canonical recipe;
- semantic reuse mode is controlled by `COLDKEEP_REUSE_SEMANTIC_VALIDATION` (`off`, `suspicious`, `always`; default `suspicious`).

This avoids hidden "magic reuse" behavior: reuse is explicit, gated, and fail-closed when integrity signals disagree.

Store flow diagram:

```text
input bytes
    |
    v
select active chunker version
    |
    v
chunking (CDC)
    |
    v
chunk identity resolve (reuse/new)
    |
    +--> append block bytes to container (if new payload)
    |
    v
persist recipe metadata (logical_file/file_chunk/chunk/blocks)
    |
    v
transaction commit -> COMPLETED state visibility
```

## Restore Flow (Read Path)

Restore is recipe-driven replay, not re-chunking.

High-level flow:

1. Load completed logical-file recipe metadata.
2. Validate metadata sanity (including chunker-version field shape/sanity policy).
3. Resolve ordered `file_chunk -> chunk -> blocks` graph.
4. Stream/decode referenced block bytes into a temporary output.
5. Verify reconstructed content hash against stored logical-file hash.
6. Publish the exact destination atomically through retained parent/object
   identity, using non-replacing or intentional-overwrite semantics.

Restore flow diagram:

```text
logical file id/path
    |
    v
load persisted recipe metadata
    |
    v
ordered chunk/block replay
    |
    v
reconstruct temp file
    |
    v
final hash verification
    |
    v
secure atomic publication to exact destination
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

- The authoritative append lifecycle state machine is documented in [internal/storage/store.go](internal/storage/store.go).
- Writer comments should point to that state machine instead of duplicating lifecycle logic.

## Core Invariants

These invariants should hold across store, restore, GC, recovery, and verification:

- every COMPLETED chunk has exactly one valid block record
- every block record references a valid container (or an explicitly quarantined/missing container state)
- every COMPLETED logical file has a complete, contiguous, ordered file_chunk graph
- live_ref_count > 0 protects a chunk from GC deletion
- pin_count > 0 protects a chunk from concurrent deletion while restore-like operations are active
- committed metadata implies referenced bytes are already durable on disk
- G14 snapshot-retained content is GC-safe: any logical file reachable from either current state (`physical_file`) or retained snapshot history (`snapshot_file`) must be treated as live and must not be reclaimed; GC computes a `ReachabilitySummary` before the container sweep and applies it as an additional safety net (`containerHasRetainedChunks`) independent of `live_ref_count`
- G15 snapshot deletion is metadata-only: deleting a snapshot removes only `snapshot` and `snapshot_file` rows; it may reduce logical reachability and make content eligible for a future GC pass, but it must not directly delete logical content
- Snapshot lineage (`snapshot.parent_id`) is informational metadata only: parent/child links support analysis and visualization, but restore reads only the selected snapshot and never requires parent snapshot content.
- G17 verify/doctor snapshot awareness: system verify audits persisted snapshot reachability integrity (`snapshot_file` -> `logical_file` existence, logical lifecycle validity, retained non-empty files with missing chunk graph), and doctor reporting surfaces snapshot-retention audit counters so snapshot-driven integrity/GC blockers are explicit to operators

## Snapshot Lineage (v1.4)

Snapshots may reference a parent snapshot via `parent_id`.

This relationship is:

- informational only
- not used for reconstruction
- not required for restore
- safe to break (deleting a parent does not affect child snapshot usability)

This design preserves:

- snapshot independence
- simple garbage collection behavior
- deterministic restore

Metadata growth note:

- each snapshot stores a full metadata view of captured files
- metadata size grows over time as snapshots accumulate
- this tradeoff is intentional to keep restore behavior simple, self-contained, and safety-first

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

### Guarantee 1: Chunker-Version-Independent Restore

Restore correctness is intentionally decoupled from write-time chunker evolution.

Contract:

- restore reconstructs bytes from persisted metadata references (`file_chunk`, `chunk`, `blocks`), not from re-chunking input with the current chunker.
- write-time chunker selection affects future storage shape and dedup behavior, but not replayability of already persisted logical files.
- chunker version is retained as metadata for auditability and observability.

Non-guarantees:

- cross-version chunk boundary identity is not guaranteed.
- cross-version dedup ratio identity is not guaranteed.

### Guarantee 2: Snapshot Stability Under Chunker Evolution

Snapshot stability is based on metadata-level logical-file references, not on the active chunker algorithm.

Contract:

- snapshot membership is persisted via `snapshot_file` links to logical files.
- committed logical files are immutable reconstruction recipes.
- restore of snapshot content replays persisted logical-file chunk graphs and does not re-chunk data with the current default chunker.
- therefore, chunker evolution for new writes does not invalidate previously created snapshots.

### Guarantee 3: No Automatic Data Migration

Write-path evolution is explicit and command-driven, not background-mutating.

Contract:

- coldkeep does not perform automatic re-chunking of committed logical files.
- coldkeep does not run background migration that rewrites persisted chunk/block mappings.
- stored payload representation is changed only by explicit operator-initiated commands that write new data.

This preserves auditability and avoids implicit state drift caused by unattended migrations.

Non-guarantee note:

- coldkeep does not provide automatic background optimization or re-chunking of existing committed data.

### Guarantee 4: Chunker Evolution Safety in Mixed-Version Repositories

Chunker evolution is designed for coexistence rather than repository bifurcation.

Contract:

- each committed logical file has one chunker-version provenance label.
- repository history may contain logical files written under multiple chunker versions.
- fresh v1.5+ repositories initialize write default to `v2-fastcdc`; upgrade paths preserve prior write default (`v1-simple-rolling` unless explicitly changed).
- chunks may be reused across chunker versions if their content is identical.
- chunk.chunker_version is origin metadata for the chunk row, not a reuse constraint for later logical files.

This supports long-lived repositories where chunker defaults change over time without breaking compatibility expectations.

Non-guarantee note:

- coexistence safety does not imply guaranteed cross-version dedup efficiency; version transitions may temporarily reduce observed reuse.

Documentation boundary note:

- treat restore correctness and snapshot stability as guarantees,
- treat cross-version reuse permission as a guarantee when content identity matches,
- and treat reuse ratios, chunk counts, and boundary alignment as implementation details unless explicitly promoted to contract language.

### Guarantee 5: Deterministic Chunking Per Version

Determinism is defined within each chunker version contract, not across versions.

Contract:

- for the same chunker version and identical input bytes, chunk boundaries and chunk sequence are deterministic.
- deterministic behavior is evaluated per version because algorithms intentionally differ across versions.
- boundary differences between versions are expected and do not violate restore correctness or compatibility guarantees.

Non-guarantee note:

- stable chunk boundaries across different chunker versions are not part of the compatibility contract.

### Guarantee 6: Forward-Compatible Chunker Metadata Handling

Forward compatibility is achieved by recipe-driven restore and metadata-sanity gates.

Contract:

- restore does not execute chunker algorithms to reconstruct stored data; it replays persisted chunk bytes and mappings.
- well-formed but unknown chunker-version labels are tolerated as informational metadata.
- malformed or empty chunker-version metadata is rejected as repository integrity failure.

This allows future chunker-version labels to coexist with restore correctness while preserving strict metadata sanity checks.

Restore path behavior:

- reconstruct into a temporary file
- fsync + close temporary file
- retain the trusted parent and temporary-object identity
- publish `overwrite=false` atomically without replacing a final-window entrant
- publish `overwrite=true` as an intentional atomic replacement
- reject lower-layer reinterpretation of an exact file destination as a directory
- apply metadata after publication through the retained published object
- fsync parent directory for durability
- validate final reconstructed file hash against stored file hash

Consequences:

- exact destination publication is atomic at the visible path boundary
- replacement of a checked parent path by a symlink/reparse point cannot
  redirect creation, publication, metadata, or cleanup within the frozen bound
- incomplete or hash-mismatched reconstruction fails explicitly
- partial/corrupt output is not accepted as success
- strict metadata failure can return an error after correct bytes are visible;
  metadata failure does not roll back published content

The native contract is proven on Linux, macOS, and Windows for cooperative
same-host/local-filesystem use. It does not claim protection against arbitrary
hostile same-user relocation of an already-open Unix directory object.

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

File-deep verification obtains the ordered authoritative placement union from
Catalog and checks every legacy-only, packed-only, and mixed recipe entry.
Missing, incomplete, conflicting, malformed, or corrupt placement fails closed;
packed chunks are not omitted. This guarantee remains bounded to the existing
repository storage and codec contract.

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

## Backend and Repository Coordination Model

SQLite and PostgreSQL are supported v1 backends within explicitly documented
capability bounds. Shared snapshot-label filtering uses narrow ASCII case-
insensitive substring semantics on both. PostgreSQL compatibility remains part
of v1 and is retained through v2. Making SQLite the default repository-local
product experience is v2 productization; centralized PostgreSQL server product
mode is v3.

The production Coordinator provides cooperative same-process/same-host/local-
filesystem ownership for participating operations on Linux, macOS, and
Windows. Valid `simulate gc` participates because it opens the shared
application and plans against the live repository. Isolated `simulate store`
and `simulate store-folder`, benchmarks, init, help, version, and invalid
commands remain bounded bypasses. PostgreSQL advisory-session ownership is an
additional backend barrier where applicable.

No NFS, SMB, NAS, cross-machine, or distributed-locking guarantee is made.
Those product and coordination semantics remain v3 scope.

## Trust Boundary and Assumptions

Guarantees hold within the documented operating assumptions:

- database is not externally modified behind coldkeep
- container files are not manually altered
- filesystem honors write + fsync semantics
- PostgreSQL deployment provides expected transactional, locking, and advisory-lock behavior
- Missing PostgreSQL schema requires manual schema application or `COLDKEEP_DB_AUTO_BOOTSTRAP=true`. Existing older schemas are auto-upgraded to the required v16 schema at startup.

## Interface Correctness Layer (v1.1)

Beyond storage-core correctness, v1.1 adds interface correctness for batch CLI orchestration:

- deterministic per-item ordering and reporting
- isolated execution semantics
- stable machine-readable status/summary/results envelopes
- automation-safe process exit behavior

These contracts are validated by targeted adversarial orchestration tests and tracked in [VALIDATION_MATRIX.md](VALIDATION_MATRIX.md) under G9.

## Evolution Note: v1.2 physical_file Layer

v1.2 introduces the `physical_file` to `logical_file` relationship.
This extends the external model (path/folder restore semantics, explicit current-state roots, repair boundaries, and GC pre-flight integrity gating) while preserving core architecture pillars:

- chunk identity model
- container/block model
- lifecycle and recovery philosophy
- storage correctness guarantees G1-G8
- interface correctness direction from G9 onward

### Current batch invariant strategy

For v1.2 batch operations (`restore`, `remove`, `repair --batch`), invariant preservation is enforced per item rather than once at batch end.

- This is the intentional safety-first design for the current release.
- It keeps failure isolation simple and deterministic.
- It ensures every successful item leaves the system in a verified-consistent state before the next item executes.

Future performance work may introduce optional post-batch invariant enforcement or batched SQL primitives, but that is explicitly deferred beyond v1.2.

### Path Identity Policy

For v1.2 physical path identity rules (canonicalization strategy, case behavior, and rationale), see:

- [docs/PATH_IDENTITY.md](docs/PATH_IDENTITY.md)

### Current restore/remove boundary

Coldkeep now exposes four active restore/remove engine methods:

- `Engine.Restore` for by-ID restore only
- `Engine.RestoreStoredPath` for exactly one current `physical_file.path`
- `Engine.Remove` for by-ID logical deletion only
- `Engine.RemoveStoredPaths` for one or more current `physical_file.path`
  unlinks

The production layering is:

```text
CLI -> Engine -> Storage
```

The CLI owns parsing, command-shape validation, input-file reading, renderer
projection, and performance spans. The engine owns typed requests, validation,
method-selected addressing semantics, batch preparation, and batch result
meaning. Storage owns catalog lookup and mutation, overwrite behavior,
path-safety enforcement, payload reconstruction, transactions, ref-count
transitions, snapshot-retention enforcement, and temporary chunk pinning.

Restore and remove are intentionally distinct:

- by-ID restore reconstructs one or more logical files under a destination root
- stored-path restore reconstructs exactly one current mapping using original,
  prefix, or override destination semantics
- by-ID remove deletes logical identity and logical content associations
- stored-path remove unlinks current mappings and updates `logical_file.ref_count`

Restore does not persistently mutate logical identity, physical mappings,
logical ref counts, snapshots, file-chunk ownership, or chunk live-reference
ownership. It may temporarily mutate `chunk.pin_count` during payload
reconstruction and must restore pin state on success and failure.

Neither remove path directly deletes payload bytes, container files, or block
files. GC alone owns physical payload reclamation.

Both live remove forms refuse snapshot-retained logical files with
`SNAPSHOT_RETAINED_DELETE_BLOCKED`.

### Invariant-Driven Concurrency Safety

A key design pattern in v1.2 remove operations is the use of **invariant-driven safety nets** to handle edge cases:

**Pattern:**
When cascading through physical_file mappings for removal, each step verifies the invariant:

```text
logical_file.ref_count == COUNT(physical_file rows for that logical_file)
```

**Why this matters:**
The cascade reads all paths in a transaction snapshot, then iterates to delete each one. A concurrent INSERT could theoretically add a new mapping after the SELECT but before all DELETEs complete. This is **safe by design** because:

1. **Invariant check enforces correctness**: Each removal verifies the ref_count matches the actual row count
2. **Isolation prevents corruption**: Transaction isolation prevents the snapshot from being corrupted by concurrent writes
3. **Safety net catches edge cases**: If a concurrent operation somehow violated expectations, the invariant check would detect it and abort, preventing silent corruption

This is superior to "best-effort" deletion without verification. It ensures we fail **loud** rather than **silent**.

**Future expansions:**
As future performance-oriented evolutions are considered, this invariant-driven pattern should remain the foundation, with verification potentially pushed to the end of the batch operation rather than per-item.

### Phase 5: Audited Physical Graph Coherence

Phase 4 made the v1.2 physical layer correct on the write path. Phase 5 extends that into a read-side audited guarantee.

Standard verify now audits the current-state physical graph for:

- orphan `physical_file` rows whose `logical_file_id` points nowhere
- `logical_file.ref_count` drift relative to `COUNT(physical_file rows)`
- impossible negative `logical_file.ref_count` states

This changes the trust model from “store/remove maintain the invariant when they run” to “the system can prove the invariant still holds now”.

Doctor remains recovery-first, but its verify phase now includes these cheap metadata audits. Automatic repair inside doctor is still intentionally deferred.

The explicit repair boundary is now defined:

- `verify`: detect only
- `doctor`: recover + detect, but no physical-layer auto-repair
- `repair ref-counts`: explicit operator command that recomputes `logical_file.ref_count` from `physical_file` rows

This preserves a clear source of truth: current-state `physical_file` rows win for ref-count reconstruction, while orphan `physical_file` rows remain a hard integrity failure that must be investigated rather than silently rewritten.

This phase also wires GC to the audited physical roots: `RunGCWithContainersDirResult` runs `CheckPhysicalFileGraphIntegrity` as a pre-flight immediately after acquiring the advisory lock. If any integrity issue is detected (orphan `physical_file` rows, `ref_count` mismatches, or negative ref counts), GC is refused with an actionable error directing the operator to run `repair ref-counts` first. This prevents GC from treating live blocks as unreferenced due to drift in `logical_file.ref_count`.

#### Phase 5 ready definition

Phase 5 is complete when `coldkeep simulate gc` can exactly predict what real GC would consider reclaimable while remaining strictly read-only.

That readiness bar is defined by all of the following:

- simulation and real GC share the same reachability path for current-state and snapshot-retained roots
- hypothetical `--delete-snapshot <id>` simulation excludes that snapshot only from simulated roots and never deletes snapshot metadata
- current live files still protect chunks during simulation
- other retained snapshots still protect chunks during simulation
- reclaimability reporting distinguishes logical reclaimability from immediately recoverable physical disk space
- `coldkeep simulate gc` does not call sweep/delete execution
- `coldkeep simulate gc` performs zero DB writes and zero filesystem writes
- CLI text and JSON outputs are both rendered from the same simulation result model

Operationally, this means Phase 5 is not "estimate GC impact"; it is "compute the same reclaimability decision boundary as real GC, plus hypothetical snapshot-root removal, without changing repository state".

### Phase 6 — GC root model formalization

Phase 6 formalizes the GC trust model to explicitly operate under the v1.2 audited-root model (Option A — conservative path):

1. **Advisory lock** — singleton enforcement (existing).
2. **Pre-flight gate** — `CheckPhysicalFileGraphIntegrity` must pass before any deletion decision. Applies equally to real GC and dry-run GC. A drifted dry-run graph produces misleading "what would be deleted" output and is therefore also refused.
3. **Chunk liveness evaluation** — `chunk.live_ref_count` and `chunk.pin_count` remain the immediate deletion criterion. This is correct and safe because steps 1–2 guarantee the physical-root graph is coherent and chunk ref counts are trustworthy inputs.

#### GC root model invariant chain

```text
physical_file rows (audited coherent)
    → logical_file (ref_count authoritative after repair)
        → file_chunk → chunk (live_ref_count/pin_count evaluated per container)
            → blocks → container (eligible for deletion only if all chunks have zero liveness)
```

#### Phase 6 test coverage

- `TestRunGCRefusesOnOrphanPhysicalFileRows` — orphan rows trigger refusal
- `TestRunGCRefusesOnNegativeLogicalRefCounts` — negative ref counts trigger refusal
- `TestRunGCDryRunRefusesOnDriftedGraph` — dry-run respects the pre-flight gate
- `TestRunGCSucceedsAfterRepairLogicalRefCounts` — repair unblocks GC (unit)
- `TestRepairThenVerifyThenGCSmoke` — full operator recovery loop (integration): store → corrupt → verify fails → doctor fails → repair succeeds → verify passes → gc dry-run passes → gc passes → restore matches

#### v1.13.16 snapshot-only current-root correction

Current recipe liveness is distinct from retained reachability. A completed
logical recipe contributes each `file_chunk` occurrence to
`chunk.live_ref_count` exactly once when it has at least one current
`physical_file` mapping. Additional mappings do not multiply that contribution;
the last unlink deactivates it, and the first reattachment reactivates it.
Snapshot membership and restore pins do not alter `live_ref_count`.

Snapshot-only recipes remain GC roots through `snapshot_file`. Live GC removes
a rootless completed recipe before sweeping chunks only when it has no current
mapping, no snapshot membership, and no pinned recipe chunk. Dry-run models the
same eligibility without mutation. By-ID logical deletion remains blocked for
snapshot-retained content, while stored-path unlink is permitted. The
v1.13.16 Phase 6R2 certification proves byte-identical snapshot restore after
GC, unreachable-control reclamation, current-live preservation, and pin-aware
cleanup on SQLite and PostgreSQL plain/AES-GCM.

### Phase 7 — Operator ergonomics and observability hardening

Phase 7 adds an internal invariant taxonomy layer to make failures easier to consume in text output, JSON output, tests, and logs without changing command boundaries.

- Added `internal/invariants` typed errors with stable codes.
- Physical graph verify failures now carry machine-readable codes:
  - `PHYSICAL_GRAPH_ORPHAN`
  - `PHYSICAL_GRAPH_REFCOUNT_MISMATCH`
  - `PHYSICAL_GRAPH_NEGATIVE_REFCOUNT`
  - `PHYSICAL_GRAPH_INTEGRITY` (multi-issue aggregate)
- GC refusal on drifted roots now carries `GC_REFUSED_INTEGRITY`.
- Repair refusal on orphan rows now carries `REPAIR_REFUSED_ORPHAN_ROWS`.

CLI error payloads now include optional advisory metadata when an invariant code is present:

- JSON mode: `invariant_code`, `recommended_action`
- Text mode: `INVARIANT_CODE: ...` and `Recommended action: ...`

This improves operator guidance while keeping doctor detect-only for physical-layer drift and preserving the explicit repair boundary.

### Phase 8 — Observability and simulation tooling contract

Phase 8 formalizes the operator/tooling command contract for read-only observability.

Command surfaces in scope:

- `coldkeep stats`
- `coldkeep stats --json`
- `coldkeep inspect <entity> <id>`
- `coldkeep inspect ... --relations`
- `coldkeep inspect ... --reverse`
- `coldkeep inspect ... --deep --limit N`
- `coldkeep simulate gc`
- `coldkeep simulate gc --delete-snapshot <id>`
- `coldkeep simulate gc --containers`
- `--trace` / `--trace-json` diagnostics

Inspect entity support currently includes `file` (alias `logical-file`), `chunk`, `container`, and `snapshot`.

Phase 8 guarantees:

- observability commands are read-only (`stats`, `inspect`, `simulate gc`)
- GC simulation is exact relative to GC reclaimability decisions under the same integrity gates
- simulation does not mutate repository state (no DB writes, no filesystem writes)
- JSON output is intended for tooling/automation pipelines
- deep inspect traversals can be large and should be bounded with `--limit N`
- trace diagnostics are emitted on stderr (`--trace`, `--trace-json`) so stdout payloads remain stable for piping and automation

### Stored-path remove dry-run

Single `remove --stored-path` intentionally remains a live-only command shape.

Batch `remove --stored-paths` supports dry-run through the dedicated engine
boundary, but that dry-run remains a lookup/planning path rather than a full
proof of live success. Snapshot-retention refusal, transaction-time failures,
and other live invariant failures are still enforced only during execution.

This means architecture documentation should evolve by extension, not by rewrite.

When release-facing behavior changes, keep this document aligned with [README.md](README.md), [VALIDATION_MATRIX.md](VALIDATION_MATRIX.md), and [PRE_RELEASE_CHECKLIST.md](PRE_RELEASE_CHECKLIST.md) so operator-facing semantics and internal-model semantics do not drift apart.
