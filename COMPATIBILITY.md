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
- explicit migration behavior boundaries
- explicit non-guarantees to reduce ambiguity

Migration philosophy:

- coldkeep prefers non-destructive evolution over automatic optimization.

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

## Guarantee 3: No Automatic Data Migration

Contract:

- coldkeep does not automatically rewrite already stored logical-file payload mappings.
- no automatic re-chunking is performed in the background when chunker defaults evolve.
- no background migration process silently transforms persisted data layouts.
- no silent data transformation is applied to stored content without explicit operator command intent.

Practical consequence:

- changing default chunker affects only future writes.
- historical data remains as-written until an explicit, user-invoked workflow rewrites it.

## Guarantee 4: Chunker Evolution Safety

Contract:

- multiple chunker versions can coexist safely in the same repository.
- each committed logical file has one chunker-version label for provenance.
- chunks may be reused across chunker versions if their content is identical.
- chunk.chunker_version is origin metadata for the chunk row, not a reuse constraint.

Practical consequence:

- mixed-version repositories are expected steady-state, not an exceptional mode.
- changing default chunker does not require repository split or compatibility migration.

Non-implication:

- coexistence safety does not imply guaranteed cross-version dedup efficiency.
- cross-version reuse is opportunistic under content identity and integrity checks, not a guaranteed efficiency ratio.

## Guarantee 5: Deterministic Chunking Per Version

Contract:

- for a given chunker version and identical input bytes, chunking is deterministic.
- same input yields the same chunk sequence for that version.
- different chunker versions may produce different chunk boundaries for the same input.

Practical consequence:

- determinism guarantees reproducible behavior within a version.
- cross-version boundary drift is expected and is not a compatibility failure.

## Guarantee 6: Forward-Compatible Restore Metadata

Contract:

- unknown future chunker-version metadata does not block restore, as long as metadata is well-formed.
- restore replays persisted chunk bytes and mappings; it does not invoke chunker logic to reconstruct stored files.
- unknown chunker-version values are treated as informational provenance metadata, not as a restore precondition.

Practical consequence:

- repositories containing newer chunker-version labels remain restorable by recipe replay semantics.
- malformed or empty chunker-version metadata remains a hard error because metadata sanity is part of integrity checks.

## Versioning Rules

Chunker evolution model:

Current chunker versions include:

- v1 simple rolling
- v2 FastCDC

Evolution expectations:

- New chunker versions may change boundaries and dedup behavior for new writes.
- Existing logical files remain restorable because restore is metadata replay, not re-chunking.
- Mixed-version repositories are expected and supported.
- Dedup identity remains content-based under repository integrity/safety constraints, even when logical-file recipe versions differ.

## Explicit Non-Guarantees

### Non-Guarantee 1: Cross-Version Dedup Efficiency

coldkeep does not guarantee dedup efficiency between different chunker versions.

Clarifications:

- chunks may be reused across versions when identical content produces the same chunk identity under repository integrity policy,
- but cross-version reuse efficiency is not guaranteed,
- and chunker upgrades may reduce dedup temporarily until new write populations stabilize.

### Non-Guarantee 2: Stable Chunk Boundaries Across Versions

coldkeep does not guarantee stable chunk boundaries across chunker versions.

Clarifications:

- different chunker versions may produce different chunk layouts for the same input,
- boundary drift across versions is expected,
- and this does not violate restore correctness or compatibility guarantees.

### Non-Guarantee 3: Automatic Optimization

coldkeep does not automatically optimize or re-chunk existing data.

Clarifications:

- existing stored data is not background-rewritten for optimization,
- no automatic re-chunk pass is performed after chunker changes,
- and optimization/rewrite behavior requires explicit operator-invoked commands.

coldkeep does not guarantee:

- identical chunk boundaries across chunker versions
- identical dedup ratios across chunker versions
- identical chunk counts across implementations
- write-path performance parity across chunker versions
- automatic in-place migration of historical data after chunker changes
- acceptance of malformed chunker-version metadata

These are intentionally not compatibility requirements.

## What Users Should Expect Across Versions

Users can expect:

- stable restore correctness for previously stored logical files
- stable CLI/JSON contracts within v1.x compatibility policy
- observable chunker metadata for diagnostics
- stats visibility for chunk and logical-file version distributions in mixed-version repositories

Users should not assume:

- that switching chunkers preserves dedup metrics for future writes
- that benchmark percentages are exact constants across machines

## Upgrade Behavior

Configuration command:

- `coldkeep config set default-chunker <version>` updates repository write-default policy.

Existing repositories:

- retain their persisted chunker-version history,
- are not automatically rewritten or re-chunked by upgrade,
- preserve prior repository write default on upgrade (`v1-simple-rolling` for legacy repositories unless explicitly changed),
- and use configured/default chunker policy only for new writes.

New repositories:

- initialize with the current default chunker policy (`v2-fastcdc` in v1.5+),
- while older upgraded repositories keep their existing write-default policy.

## Future-Proofing Notes

- Unknown future chunker-version labels are intentionally tolerated for restore when metadata is well-formed.
- Compatibility is recipe-driven: persisted reconstruction metadata is the long-term replay contract.
- Guarantees are intended to be additive across minor releases; new behavior should not weaken existing restore/snapshot safety guarantees.
- If a behavior changes from guarantee to non-guarantee (or the reverse), this file is the source of truth and must be updated in the same change.
- Mixed-version repositories are a first-class operating mode, not a migration edge case.

## Common Mistakes to Avoid

- Overpromising cross-version deduplication outcomes.
- Underexplaining chunker versioning semantics between write-time policy and restore-time replay.
- Mixing stable compatibility guarantees with implementation details that can evolve.

Authoring rules:

- Guarantees must describe stable externally observable behavior.
- Implementation notes must be marked as non-guarantees unless they are contractually frozen.
- Cross-version dedup metrics, chunk counts, and boundary alignment must never be presented as guaranteed outcomes.

## Operational Guidance

When evaluating chunker evolution:

- compare reuse percentage and boundary stability trends, not exact chunk counts
- include shifted-data scenarios in validation
- keep deterministic, seed-driven benchmark inputs

See internal/chunk/benchmark for current benchmark/validation harness.
