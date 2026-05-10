# ADR-0001: Transform Ordering Contract (v1.9)

- Status: Accepted
- Date: 2026-05-09
- Scope: Repository storage semantics

## Context

Coldkeep v1.9 stores blocks with optional compression and optional encryption,
plus hash checkpoints across logical, compressed, and physical layers.

Engine extraction work for v1.10 requires one immutable interpretation contract
for all existing and future blocks. If transform order is ambiguous or mutable,
mixed repositories become unsafe and retrocompatibility cost increases sharply.

## Decision

Coldkeep freezes transform ordering as stable repository semantics.

### Write path (frozen)

1. logical encode
2. logical hash
3. compression
4. compressed hash
5. encryption
6. physical hash
7. persist

### Read path (frozen strict inverse)

1. read payload
2. physical hash verify
3. decrypt
4. compressed hash verify
5. decompress
6. logical hash verify
7. decode logical block

Repository defaults are write policy for newly written blocks only.
Per-block metadata remains authoritative for reads forever.

## Consequences

- Mixed repositories remain valid across default/config changes.
- Historical blocks keep their original interpretation forever.
- Verify and restore can fail fast by stage with deterministic semantics.
- Any future stage reordering is a major contract change and must not be done
  silently in v1.x.

## Implementation Notes

- Write-path sequencing is implemented in storage transform and persist flows.
- Read-path sequencing is implemented in verify/read pipeline stages.
- Stage-order tests must assert precedence (physical before compressed before
  logical) and write-path boundary hashing behavior.
