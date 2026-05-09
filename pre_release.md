# Pre-release Notes

This document is a compatibility-friendly pointer to `PRE_RELEASE_CHECKLIST.md`,
which remains the canonical release-gate document.

## v1.9 release position

- v1.9 formalizes transform-based storage semantics (logical/compressed/physical payload layers).
- v1.9 adds block-level compression with store-if-smaller policy and metadata-driven reads.
- v1.9 preserves AES-GCM packed-block integration.
- v1.9 reads v1.7/v1.8 repositories without forced rewrite.
- Mixed repositories containing legacy v1.7/v1.8 data and new v1.9 compressed/encrypted blocks are valid steady-state.
- v1.9 introduces no schema-breaking rewrite; storage metadata migrations remain additive.
- restore determinism is preserved.
- GC safety is preserved.
- snapshot semantics are preserved.

For the full release gate, validation sequence, and operator checklist, use
`PRE_RELEASE_CHECKLIST.md`.
