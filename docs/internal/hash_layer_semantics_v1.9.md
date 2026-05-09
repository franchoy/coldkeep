# Hash Layer Semantics v1.9 (Internal)

Status: Frozen
Date: 2026-05-09

## Canonical Hash Roles

- block_hash
  - canonical logical block identity
  - restore/logical correctness authority

- compressed_hash
  - transform-stage integrity checkpoint
  - validates pre-encryption compressed payload bytes

- physical_hash
  - persisted payload integrity checkpoint
  - validates exact stored payload bytes

## Identity Constraints

Only block_hash can participate in logical identity semantics.

compressed_hash and physical_hash are integrity checkpoints only and must not
be used to drive:

- dedup identity
- GC identity/liveness decisions
- snapshot identity
- restore graph construction

## Implementation Notes

- Verify/read pipeline uses physical_hash and compressed_hash only for staged
  integrity checks.
- Dedup remains content/graph identity driven by logical data and chunk/file
  identity metadata, not transform-layer hashes.
