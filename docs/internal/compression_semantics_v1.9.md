# Compression Semantics v1.9 (Internal)

Status: Frozen
Date: 2026-05-09

## Canonical Rules

- Scope: block-level only.
- Timing: compress before encrypt.
- Policy: store-if-smaller.
- Reads: per-block metadata controls decompression.

## Compatibility Implications

- Compression defaults are write policy for new blocks only.
- Historical blocks are interpreted from persisted `compression_codec` metadata.
- Mixed repositories (compressed and uncompressed blocks) are expected.

## Code Mapping

- Write-time compression policy and ordering:
  - internal/storage/store.go (`applyPackedBlockTransforms`)
- Read-time decompression routing:
  - internal/verify/verify_block_pipeline.go (`VerifyStoredBlock`)
- Operator-facing semantics:
  - docs/STORAGE_SEMANTICS_v1.9.md
  - docs/storage_transform_semantics.md
