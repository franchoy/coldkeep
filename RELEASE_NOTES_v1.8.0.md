# v1.8.0 - Packed Block Foundation and Release Hardening

v1.8.0 introduces packed storage blocks for new writes and finalizes the
release-hardening gates needed for stable operator rollout.

This release preserves Coldkeep core guarantees: deterministic restore,
integrity verification, GC safety, and snapshot correctness.

## Scope and positioning

- Packed block abstraction is now active for new writes.
- Compiled-in default packed block target size is locked to 1 MiB.
- `COLDKEEP_BLOCK_TARGET_SIZE_MB` remains available for advanced,
  write-time tuning and benchmarking.
- `COLDKEEP_PACKED_BLOCK_SIZE_MIB` is retained as a legacy fallback only when
  `COLDKEEP_BLOCK_TARGET_SIZE_MB` is unset.
- Existing v1.7 data remains readable; migration is schema-level (v12), not a
  historical payload rewrite.
- Missing PostgreSQL schema requires manual schema application or
  `COLDKEEP_DB_AUTO_BOOTSTRAP=true`; existing older schemas auto-upgrade to v12
  at startup.

## Operator-visible highlights

- `storage_blocks` + `chunk_block_refs` are now first-class for packed writes.
- `coldkeep benchmark` remains available for ad-hoc/local performance checks.
- Phase 8 benchmark decision is finalized: 1 MiB remains the default for v1.8.
- AES-GCM packed-block write/verify flows are covered as release gates.

## Decision and safety framing

- Block-size decision evidence is recorded in
  `BENCHMARK_PHASE8_BLOCK_SIZE_DECISION.md`.
- Mixed repositories (legacy + packed) remain valid and supported.
- v1.7 compatibility remains read-compatible for historical data; v1.7 is not
  guaranteed to read repositories containing v1.8 packed-block data.

## Validation framing

- CI parity checks and release gates are documented in
  `PRE_RELEASE_CHECKLIST.md`.
- Benchmark docs have explicit support-level language to distinguish:
  - supported CLI benchmark commands (`coldkeep benchmark run|chunkers`), and
  - release decision-grade matrix scripts under `scripts/run_phase8_*.sh`.
