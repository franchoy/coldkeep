# ADR-0003: Compression Semantics (v1.9)

- Status: Accepted
- Date: 2026-05-09
- Scope: Storage transform and compatibility semantics

## Context

Coldkeep supports optional compression with mixed repositories where historical
and newly-written blocks can coexist. Engine extraction requires immutable
compression behavior with no interpretation ambiguity.

## Decision

Compression semantics are frozen for v1.9:

1. Scope
   - Compression is block-level only.
   - Compression is never chunk-level or file-level policy authority.

2. Timing
   - Compression always executes before encryption on writes.
   - Reads reverse in strict inverse order (decrypt then decompress).

3. Policy
   - Store-if-smaller is canonical behavior.
   - If compression expands payload, block is persisted as uncompressed.

4. Read behavior
   - Decompression is controlled by per-block metadata (`compression_codec`).
   - Repository defaults do not override historical block interpretation.

## Consequences

- Mixed compressed/uncompressed blocks are valid steady-state.
- Compression default changes only affect future writes.
- Restore/verify behavior remains deterministic from persisted metadata.
- Reordering or replacing the canonical compression policy is a major
  compatibility change and must not be done silently in v1.x.
