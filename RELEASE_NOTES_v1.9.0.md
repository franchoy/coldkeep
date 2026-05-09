# v1.9.0 - Transform Storage Architecture Freeze

v1.9.0 evolves Coldkeep from single-stage persistence semantics into a formal
transform-based storage architecture while preserving deterministic restore,
logical deduplication, repository retrocompatibility, and integrity guarantees.

This release freezes storage semantics required for v1.10 engine extraction.

## Release Highlights

- Formalized transform pipeline contract (write and inverse read ordering).
- Added block-level compression support with store-if-smaller policy.
- Preserved AES-GCM packed-block encryption behavior.
- Locked logical/compressed/physical hash semantics.
- Added explicit verify failure stages across payload/decode/reference layers.
- Confirmed metadata-driven mixed-repository read path as a first-class behavior.
- Added explicit repository capabilities model for supported/observed storage features.

## Storage Semantics (Frozen in v1.9)

Write path ordering:

1. logical encode
2. logical hash
3. compression
4. compressed hash
5. encryption
6. physical hash
7. persistence

Read path ordering (inverse):

1. read persisted payload
2. physical hash verify
3. decrypt
4. compressed hash verify
5. decompress
6. logical hash verify
7. decode logical block

Contract notes:

- Per-block metadata is authoritative for read/verify behavior.
- Repository defaults affect future writes only.
- No automatic rewrite or recompression of historical blocks.
- Mixed repositories (legacy + packed + compressed + encrypted) are supported.

## Compatibility

- v1.9 reads v1.7 and v1.8 repositories.
- Existing payload bytes are not rewritten during upgrade.
- Legacy rows with missing `compressed_hash` / `physical_hash` remain readable and verifiable under compatibility rules.
- v1.7 is not guaranteed to read repositories containing v1.8/v1.9 packed metadata.

## Verification and Integrity

v1.9 verification explicitly stages failures by layer:

- `physical_payload`
- `decrypt`
- `compressed_hash`
- `decompress`
- `logical_hash`
- `block_decode`
- `chunk_refs`
- `snapshots`

This preserves fail-closed integrity behavior while improving operator diagnostics.

## Performance and Baselines

v1.9 freezes benchmark baseline artifacts and thresholds under `benchmarks/v1.9/`.

- Uncompressed and compressed baseline pairs are tracked.
- Regression thresholds are codified and CI-consumable.
- Baseline refresh requires an explicit decision and documented rationale.

## Validation Summary

Release-gate validation for v1.9 includes:

- compatibility suites
- adversarial corruption suites
- restore determinism matrix
- GC safety matrix
- mixed-repository stability checks
- benchmark baseline and threshold freeze checks
- storage contract/ADR freeze checks

All required v1.9 release gates are green.

## v1.10 Readiness

v1.10 can focus on architecture extraction without storage redesign because v1.9
freezes:

- transform ordering semantics
- hash-layer semantics
- metadata-driven read behavior
- mixed-repository compatibility behavior
- verification-stage semantics

## References

- `docs/STORAGE_SEMANTICS_v1.9.md`
- `docs/STORAGE_SEMANTICS_v1.9_ENGINE_QUICK_REFERENCE.md`
- `docs/adr/ADR-0001-transform-ordering-contract-v1.9.md`
- `docs/adr/ADR-0002-hash-layer-semantics-v1.9.md`
- `docs/adr/ADR-0003-compression-semantics-v1.9.md`
- `docs/adr/ADR-0004-legacy-compatibility-guarantees-v1.9.md`
- `docs/adr/ADR-0005-mixed-repository-semantics-v1.9.md`
- `docs/adr/ADR-0006-read-path-negotiation-semantics-v1.9.md`
