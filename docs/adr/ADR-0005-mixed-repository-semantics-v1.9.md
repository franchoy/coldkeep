# ADR-0005: Mixed Repository Semantics (v1.9)

- Status: Accepted
- Date: 2026-05-09
- Scope: Read/verify compatibility semantics

## Context

Coldkeep repositories naturally accumulate historical and new block layouts and
transform metadata over time. Treating mixed repositories as exceptional leads
to hidden homogeneous assumptions and compatibility regressions.

## Decision

Mixed repositories are first-class, normal behavior.

Valid steady-state examples include simultaneous presence of blocks with:

- legacy-single + no compression + no encryption
- packed-multi + no compression + aes-gcm
- packed-multi + zstd + aes-gcm

Read/verify contract:

- per-block metadata is authoritative
- repository defaults are non-authoritative for reads
- homogeneous repository assumptions are invalid

## Consequences

- mixed repositories are supported without migration pressure
- compatibility and runtime defaults remain cleanly separated
- future engine extraction can rely on metadata-driven per-block interpretation
