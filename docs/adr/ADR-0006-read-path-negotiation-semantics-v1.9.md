# ADR-0006: Read-Path Negotiation Semantics (v1.9)

- Status: Accepted
- Date: 2026-05-09
- Scope: Restore/verify interpretation contract

## Context

Coldkeep repositories are mixed by design: legacy and packed blocks, multiple
compression modes, and multiple encryption modes can coexist in one repository.

Engine extraction and long-term compatibility require one immutable negotiation
rule for read/verify behavior. Any dependency on repository defaults or
homogeneous assumptions introduces replay ambiguity and retrocompatibility risk.

## Decision

Read-path negotiation is frozen as metadata-driven and capability-resolved.

For each block, restore/verify must:

1. read that block's persisted metadata (`codec`, `compression_codec`, hashes)
2. resolve required transform handlers from registered runtime capabilities
3. run strict inverse transform stages for that block
4. validate integrity at each stage using that block's persisted hashes

Repository defaults are write policy for future blocks only and are
non-authoritative for reads.

## Consequences

- mixed repositories are first-class steady-state for restore/verify
- old and new blocks can be read together without migration prerequisites
- unknown or unsupported per-block modes fail explicitly at stage boundaries
- deterministic replay semantics stay stable across default/config changes

## Implementation Notes

- verify pipeline: `internal/verify/verify_block_pipeline.go`
- storage reader bridge: `internal/storage/storage_block_reader.go`
- mixed negotiation test evidence:
  - `internal/verify/verify_block_pipeline_test.go`
    (`TestVerifyStoredBlockMixedRepositoryMetadataNegotiationStep78`)
