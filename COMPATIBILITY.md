# Compatibility Contract

This document defines what coldkeep guarantees across versions, especially for
chunker evolution and restore behavior.

It complements:

- README.md for the operator-facing guarantee summary
- ARCHITECTURE.md for implementation model and invariants
- VALIDATION_MATRIX.md for evidence mapping

## Scope

This contract is about:

- restore correctness across chunker versions
- chunker evolution expectations
- explicit non-guarantees to reduce ambiguity

## Guarantee 1: Restore Correctness Across Chunker Versions

Contract:

- Any stored logical file can be restored byte-identically regardless of the current default chunker.
- Restore replays persisted references (`file_chunk` -> `chunk` -> `blocks`) and validates final file hash.
- Stored chunker version is metadata (provenance/observability), not a restore execution dependency.

Practical consequence:

- Changing repository default chunker affects future writes, not the ability to restore historical data.

## Guarantee 2: Snapshot Stability Across Future Versions

Contract:

- Snapshots remain valid across future coldkeep versions within the v1.x compatibility policy.
- Snapshot membership references logical files through persisted snapshot metadata (`snapshot`, `snapshot_file`).
- Logical files are immutable reconstruction recipes (`file_chunk` -> `chunk` -> `blocks`) once committed.
- Chunker evolution for future writes does not invalidate previously captured snapshots.

Practical consequence:

- A snapshot created before a chunker change remains restorable after the chunker change.
- Snapshot restore remains metadata replay from the selected snapshot scope; it does not require re-chunking with the current default chunker.

## Chunker Evolution Model

Current chunker versions include:

- v1 simple rolling
- v2 FastCDC

Evolution expectations:

- New chunker versions may change boundaries and dedup behavior for new writes.
- Existing logical files remain restorable because restore is metadata replay, not re-chunking.
- Mixed-version repositories are expected and supported.

## Explicit Non-Guarantees

coldkeep does not guarantee:

- identical chunk boundaries across chunker versions
- identical dedup ratios across chunker versions
- identical chunk counts across implementations
- write-path performance parity across chunker versions

These are intentionally not compatibility requirements.

## What Users Should Expect Across Versions

Users can expect:

- stable restore correctness for previously stored logical files
- stable CLI/JSON contracts within v1.x compatibility policy
- observable chunker metadata for diagnostics

Users should not assume:

- that switching chunkers preserves dedup metrics for future writes
- that benchmark percentages are exact constants across machines

## Operational Guidance

When evaluating chunker evolution:

- compare reuse percentage and boundary stability trends, not exact chunk counts
- include shifted-data scenarios in validation
- keep deterministic, seed-driven benchmark inputs

See internal/chunk/benchmark for current benchmark/validation harness.
