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

## System Overview

coldkeep is a correctness-first, local-first, content-addressed storage engine.

The architecture composes:

- a logical file model
- content-addressed chunk identity
- physical block placement metadata
- append-only container files on disk
- lifecycle-aware recovery and verification paths

Correctness has five explicit layers:

- v1.0 storage correctness: deterministic restore, integrity, recovery, GC safety
- v1.1 interface correctness: batch CLI contract stability and deterministic orchestration
- v1.2 physical-graph coherence: audited physical roots, explicit repair, invariant taxonomy, batch maintenance semantics
- v1.3 snapshot-based retention: immutable point-in-time captures, snapshot-protected GC, reachability audits
- v1.4 snapshot clarity hardening: lineage metadata is explicit and non-dependency by contract

### Correctness Layers

This diagram is a mental anchor for how guarantees compose across layers.

```text
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
| logical_file -> chunk -> blocks -> container               |
| Append-only containers + transactional DB                  |
+------------------------------------------------------------+
```

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
- content-identical chunks may be reused across version eras under repository safety rules.
- deduplication safety is defined by content identity and integrity checks, not by chunker label alone.

This supports long-lived repositories where chunker defaults change over time without breaking compatibility expectations.

Non-guarantee note:

- coexistence safety does not imply guaranteed cross-version dedup efficiency; version transitions may temporarily reduce observed reuse.

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

### Remove Semantics Consistency

v1.2 introduces a critical consistency guarantee between two remove entry points:

**remove-by-stored-path (new)** and **remove-by-ID (legacy)** are now semantically symmetric:

- Both cascade through all physical_file mappings before cleanup
- Both maintain the invariant: `logical_file.ref_count == COUNT(physical_file rows)`
- Both prevent orphan physical_file rows from pointing to deleted logical_file

**Implementation:**

- `remove-by-stored-path`: Removes one physical_file mapping, decrements logical_file.ref_count
- `remove-by-ID`: Cascades through ALL physical_file mappings (using remove-by-stored-path primitive), then deletes logical_file and file_chunk records

**Data Integrity:**
The cascade design ensures that:

1. No physical_file rows can exist after their logical_file is deleted
2. At each step of removal, ref_count correctly reflects the number of remaining physical mappings
3. References to deleted logical_file are impossible by construction

**Migration Path:**
This architecture enables future phases (v1.3+) to redefine higher-level remove commands entirely in terms of the physical_file → logical_file → chunk model without breaking storage guarantees.

### Invariant-Driven Concurrency Safety

A key design pattern in v1.2 remove operations is the use of **invariant-driven safety nets** to handle edge cases:

**Pattern:**
When cascading through physical_file mappings for removal, each step verifies the invariant:
```
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

### Phase 6 — GC root model formalization

Phase 6 formalizes the GC trust model to explicitly operate under the v1.2 audited-root model (Option A — conservative path):

1. **Advisory lock** — singleton enforcement (existing).
2. **Pre-flight gate** — `CheckPhysicalFileGraphIntegrity` must pass before any deletion decision. Applies equally to real GC and dry-run GC. A drifted dry-run graph produces misleading "what would be deleted" output and is therefore also refused.
3. **Chunk liveness evaluation** — `chunk.live_ref_count` and `chunk.pin_count` remain the immediate deletion criterion. This is correct and safe because steps 1–2 guarantee the physical-root graph is coherent and chunk ref counts are trustworthy inputs.

#### GC root model invariant chain

```
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

### Dry-run Support (Deferred beyond v1.2)

v1.2 intentionally does **not** support `--dry-run` with `remove --stored-path`.

**Rationale:**

- Dry-run requires a rollback-safe preview of transactional changes
- The remove transaction is tightly coupled to the transactional remove-by-ID cascade path
- Exposing dry-run now would require significant refactoring of the cascade logic
- The overhead of implementing dry-run correctly (separate read-only simulation) is not justified for the initial release

**Post-v1.2 plan:**
Dry-run support for `remove --stored-path` will be added when:

1. The remove transaction primitive is refactored for independent preview semantics
2. Integration tests validate that dry-run output accurately mirrors execute behavior
3. Documentation clarifies the dry-run contract (what is previewed, what is guaranteed, what is advisory)

Users can currently force-verify remove safety via explicit `verify` before `remove`, which provides correctness assurance without dry-run.

This means architecture documentation should evolve by extension, not by rewrite.

When release-facing behavior changes, keep this document aligned with [README.md](README.md), [VALIDATION_MATRIX.md](VALIDATION_MATRIX.md), and [PRE_RELEASE_CHECKLIST.md](PRE_RELEASE_CHECKLIST.md) so operator-facing semantics and internal-model semantics do not drift apart.
