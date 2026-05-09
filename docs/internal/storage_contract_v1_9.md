# Storage Contract v1.9 (Internal)

Status: Frozen
Date: 2026-05-09
Scope: Canonical internal storage behavior contract for v1.x continuity

## Purpose

This document is the single internal contract for v1.9 storage semantics.

It exists so future work can reorganize architecture without redesigning
storage behavior:

- v1.10 engine APIs: extraction and interface shaping
- v1.11 logic migration: implementation movement across packages
- v1.12 contract stabilization: explicit long-horizon compatibility hardening

This contract is behavior-authoritative. Implementation may move; semantics may
not.

## 1. Transform Ordering (Frozen)

Write path order is immutable:

1. logical encode
2. logical hash
3. compression
4. compressed hash
5. encryption
6. physical hash
7. persist

Read/verify order is strict inverse:

1. read payload
2. physical hash verify (if present)
3. decrypt (if encrypted)
4. compressed hash verify (if present)
5. decompress (if compressed)
6. logical hash verify
7. decode logical block

Constraints:

- stage order must not be reordered in v1.x
- verify fails at the first failing stage with stage-specific semantics
- missing legacy hash columns are handled as legacy-compatibility skips

## 2. Hash Semantics (Frozen)

Hash roles are fixed:

- block_hash: canonical logical block identity
- compressed_hash: compressed layer integrity checkpoint
- physical_hash: persisted payload integrity checkpoint
- chunk_hash: chunk identity for dedup graph semantics

Identity authority:

- only block_hash and chunk/file graph metadata define logical identity
- compressed_hash and physical_hash are integrity-only
- compressed_hash and physical_hash must not drive dedup identity
- compressed_hash and physical_hash must not drive snapshot identity
- compressed_hash and physical_hash must not drive GC liveness semantics

## 3. Compatibility Guarantees (Frozen)

Compatibility model:

- mixed repositories are first-class supported steady-state
- legacy-readable historical formats remain restorable
- compatibility does not require homogeneous repository state
- defaults/config changes affect future writes only

Explicit non-guarantees:

- no automatic historical rewrite
- no automatic historical recompression
- no eager background migration of historical block layout

## 4. Repository Defaults vs Block Reality (Frozen)

Repository defaults are write-policy only.

Defaults apply to newly written blocks at write time.

Defaults are non-authoritative for read/verify behavior.

Changing defaults must not reinterpret historical rows.

## 5. Block Metadata Authority (Frozen)

Per-block metadata is authoritative for interpretation.

Read and verify must use persisted per-block fields (codec, compression_codec,
hash fields, sizes, placement metadata) to determine behavior for each block.

Historical metadata is immutable interpretation truth.

## 6. Compression Semantics (Frozen)

Compression contract:

- scope: block-level only
- ordering: compression before encryption
- policy: store-if-smaller
- read behavior: decompression is controlled by block metadata only

Mixed compressed and uncompressed blocks in the same repository are expected and
fully supported.

## 7. Verify Semantics (Frozen)

Verify contract:

- staged integrity checks are mandatory and ordered
- physical layer validation (if present) precedes decrypt/decompress
- compressed layer validation (if present) precedes logical validation
- logical hash validation is required for logical correctness
- decode and chunk mapping validation happen after transform integrity checks

Failure semantics:

- fail closed on corruption/tamper/decode mismatch
- stage-specific failure meaning must remain stable for operators and tooling

## 8. Mixed Repository Semantics (Frozen)

One repository may contain, simultaneously:

- legacy-single and packed-multi blocks
- none and aes-gcm encryption modes
- none and zstd compression modes

Contract:

- this is normal steady-state, not a migration edge case
- read and verify operate block-by-block from metadata
- no repository-wide homogeneous assumptions are allowed

## 9. Read-Path Negotiation Semantics (Frozen)

Negotiation is per-block and capability-resolved:

1. read persisted block metadata
2. resolve needed handlers from registered runtime capabilities
3. execute strict inverse transform pipeline for that block
4. validate integrity at each stage

Constraints:

- repository defaults are not part of read-path negotiation
- unknown/unsupported per-block modes fail explicitly
- no default-based fallback may reinterpret stored metadata

## 10. Contract Lock and Change Policy

This v1.9 contract is locked for v1.x architectural reorganization.

Permitted in v1.10-v1.12:

- API extraction/refactoring
- package movement
- internal boundary cleanup
- performance optimizations that preserve behavior

Not permitted in v1.x without explicit major-contract process:

- transform stage reorder
- hash role reassignment
- metadata-authority weakening
- compatibility-scope narrowing
- mixed-repository behavior regression

Any semantic change requires:

1. explicit contract update
2. ADR update
3. full verification test evidence
4. operator-facing documentation update

## 11. Foundation Mapping

This contract is the behavioral foundation for:

- v1.10 engine APIs: behavior-preserving interface extraction
- v1.11 logic migration: implementation relocation under stable semantics
- v1.12 contract stabilization: consolidated long-term contract hardening

## 12. Normative References

- docs/STORAGE_SEMANTICS_v1.9.md
- docs/STORAGE_SEMANTICS_v1.9_ENGINE_QUICK_REFERENCE.md
- docs/internal/transform_ordering_contract_v1.9.md
- docs/internal/hash_layer_semantics_v1.9.md
- docs/internal/compression_semantics_v1.9.md
- docs/internal/storage_compatibility_matrix.md
- docs/adr/ADR-0001-transform-ordering-contract-v1.9.md
- docs/adr/ADR-0002-hash-layer-semantics-v1.9.md
- docs/adr/ADR-0003-compression-semantics-v1.9.md
- docs/adr/ADR-0004-legacy-compatibility-guarantees-v1.9.md
- docs/adr/ADR-0005-mixed-repository-semantics-v1.9.md
- docs/adr/ADR-0006-read-path-negotiation-semantics-v1.9.md
