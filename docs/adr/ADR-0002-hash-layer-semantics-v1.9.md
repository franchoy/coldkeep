# ADR-0002: Hash Layer Semantics (v1.9)

- Status: Accepted
- Date: 2026-05-09
- Scope: Storage identity and integrity semantics

## Context

Coldkeep persists three block-related hash values:

- block_hash
- compressed_hash
- physical_hash

Without explicit role boundaries, non-canonical hashes could leak into identity
or lifecycle decisions and break retrocompatibility across mixed repositories.

## Decision

The three-hash model is frozen as canonical:

1. block_hash
   - Canonical logical block identity.
   - Participates in logical correctness.
   - Participates in restore identity semantics.

2. compressed_hash
   - Transform-stage integrity checkpoint only.
   - Validates post-compression, pre-encryption payload integrity.

3. physical_hash
   - Persisted payload integrity checkpoint only.
   - Validates exact on-disk payload bytes.

## Prohibited Uses

compressed_hash and physical_hash must never define or influence:

- dedup identity
- GC identity/liveness semantics
- snapshot identity
- restore graph semantics

## Consequences

- Integrity and identity concerns remain cleanly separated.
- Mixed repositories remain stable even as defaults/config evolve.
- Corruption checks can fail at compressed/physical layers without changing
  logical identity semantics.
