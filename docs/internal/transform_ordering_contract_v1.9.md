# Transform Ordering Contract v1.9 (Internal)

Status: Frozen
Date: 2026-05-09

This document is the internal implementation map for the frozen transform
ordering contract.

## Contract

Write path:

logical encode
-> logical hash
-> compression
-> compressed hash
-> encryption
-> physical hash
-> persist

Read path:

read payload
-> physical hash verify
-> decrypt
-> compressed hash verify
-> decompress
-> logical hash verify
-> decode logical block

## Code Mapping

- Write encode/hash boundary: internal/storage/store.go (buildAndEncodePackedBlock)
- Write compression/hash/encryption/hash boundary: internal/storage/store.go (applyPackedBlockTransforms)
- Write persistence boundary: internal/storage/store.go (persistPackedBlockPayload)
- Read verification/decrypt/decompress/hash/decode stages: internal/verify/verify_block_pipeline.go (VerifyStoredBlock)

## Semantic Constraints

- Repository defaults are write policy only.
- Reads are metadata-driven only.
- Existing block metadata is immutable historical truth.
- Reordering stages is a major contract change.
