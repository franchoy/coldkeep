# Pre-release Notes

This document is a compatibility-friendly pointer to `PRE_RELEASE_CHECKLIST.md`,
which remains the canonical release-gate document.

## v1.8 release position

- v1.8 introduces packed storage blocks (multiple chunks per physical block).
- v1.8 completes AES-GCM packed-block integration (per-chunk encryption tracked via companion `blocks` rows).
- v1.8 reads v1.7 repositories without forced rewrite.
- Mixed repositories containing legacy v1.7 data and new v1.8 packed blocks are valid steady-state.
- v1.8 introduces no schema-breaking change; the `storage_blocks` and `chunk_block_refs` tables are additive.
- restore determinism is preserved.
- GC safety is preserved.
- snapshot semantics are preserved.

For the full release gate, validation sequence, and operator checklist, use
`PRE_RELEASE_CHECKLIST.md`.
