# Validation Matrix

This document is the authoritative mapping between public guarantees (README)
and enforced evidence (tests and verify checks).

Use it as the detailed companion to `README.md`:

- read `README.md` first for the high-level contract
- use this document when you need to answer "how do we know this guarantee is enforced?"
- use `PRE_RELEASE_CHECKLIST.md` when you need to execute the release gate locally

This file is intentionally dense. If you are brand new, skim the guarantee names
first and only drill into evidence rows relevant to the behavior you are changing.

## Guarantee ID Stability

Guarantee IDs (G1–G17+) are part of the public validation contract.

- IDs are stable across v0.10, v1.0, v1.1, v1.2, v1.3, v1.4, v1.5, v1.6, v1.7, v1.8, and v1.9
- v1.5 (chunker-evolution), v1.6 (observability/simulation), v1.7 (deterministic performance foundation), v1.8 (packed block abstraction + AES-GCM integration), and v1.9 (transform/verification semantics freeze) do not introduce new guarantee IDs
- Guarantees may be reworded, but IDs must not change
- New guarantees must use new IDs (G18, G19, ...)

This prevents future "renumbering drift".

Guarantees below map to the strongest applicable evidence in the current tree.
That evidence is intentionally heterogeneous: unit, package integration,
adversarial, cross-platform, required hosted CI, manual/local, documentation-
only, and deferred boundaries are distinguished instead of being collapsed
into one `covered` label. A test's existence does not by itself establish that
its execution is required and fail-closed in hosted CI.

This document originated from the v0.9/v0.10 trust-validation work and is now
the maintained v1.x guarantee-to-evidence contract: v1.0 storage-core
guarantees (G1-G8), v1.1 interface-correctness extensions (G9), v1.2 physical-file
graph coherence guarantees (G10-G13), and v1.3 snapshot-retention guarantees (G14-G17),
with v1.4 clarifying lineage semantics, v1.5 adding chunker-evolution compatibility
contract clarity, v1.6 adding observability and simulation contract hardening,
v1.7 adding controlled-execution performance validation language, v1.8 adding
packed block abstraction and AES-GCM packed-block integration, v1.9 freezing
transform/verification semantics, and v1.13.11 adding bounded coordination,
container, decompression, JSON-fidelity, SQL-mutation, and required-CI proof —
none of which introduce new guarantee IDs.

## Scope

- Target: single-node trust model for v1.0 core plus maintained v1.x
  interface, observability, block-abstraction, same-host coordination, and
  integrity-hardening contracts
- Surface: existing `verify` and `doctor` contracts (no new top-level validate command)
- Goal: each guarantee maps to automated evidence (verify checks, tests, or both)

Evidence is mapped by behavior, not by test filename prefix. In particular,
some older adversarial files keep their original campaign naming even when the
guarantee they now best evidence sits under a different matrix row.

Reading note:

- `Primary verify evidence` names the main runtime verification surface, not every internal helper involved
- `Primary test evidence` highlights the most representative automated coverage, not an exhaustive list of all related tests
- `covered` means the guarantee is intentionally mapped to concrete automated evidence in the current tree
- `required CI` means hosted workflow policy requires the relevant job or named
  pass event; broad job success and named-event proof are recorded separately

## Guarantees to Evidence

| ID | Guarantee | Primary verify evidence | Primary test evidence | Status |
| --- | --- | --- | --- | --- |
| G1 | Deterministic, byte-identical restore | Deep restore path validates chunk hash and final file hash | `TestRepeatRestoreDeterminism`, `TestSameInputSameChunkGraph`, `TestStoreRemoveGCRestartStoreConvergesChunkGraph` | covered |
| G2 | Repeat store does not drift chunk graph | Reuse and graph checks in store path, plus verify full/system checks | `TestRepeatedStorePreservesChunkGraphDeterminism` | covered |
| G3 | No exposure of partially written or inconsistent data | Recovery + verify model excludes/processes invalid lifecycle states, including standard verify enforcement that each COMPLETED chunk has exactly one blocks row, rollback-safe sealing-marker transitions, quarantine of damaged active containers without harming unrelated live data, ghost-byte sealing-container quarantine with preserved live data, and strict-recovery resynchronization of already-quarantined orphan container size drift instead of surfacing stale metadata as healthy state | `TestStartupRecoverySimulation`, `TestDoctorAbortsProcessingLogicalFilesFromRecoverableState`, `TestVerifyStandard/detects completed chunk missing block row`, `TestStoreSealingMarkerUpdateFailureAbortsSafelyAndRecovers`, `TestStartupRecoveryQuarantinesDamagedActiveContainerAndPreservesOtherLiveData`, `TestStartupRecoveryQuarantinesGhostByteSealingContainerAndPreservesOtherLiveData`, `TestStartupRecoveryResyncsPreexistingQuarantinedOrphanConflictState`, `TestAdversarialG2PreexistingQuarantinedOrphanSizeDriftResyncsAndPreservesHealthyRestore` | covered |
| G4 | GC is reference-safe: no reachable chunk is ever deleted | GC liveness checks use `live_ref_count OR pin_count`; verify post-GC integrity | `TestStoreGCRestore`, `TestGCRestorePinRaceContainerNotDeleted`, `TestStoreLifecycleSeededRandomizedOperationOrder` | covered |
| G5 | Exact, atomic restore publication (within same-host local-filesystem semantics) | Restore reconstructs into a temporary object under a retained trusted parent; no-overwrite uses atomic non-replacing publication, intentional overwrite uses atomic replacement, and metadata targets the retained published object | Phase 9 native/fault/race proof: exact-directory rejection, final-window occupation, overwrite/no-overwrite, link fallback/fail-closed behavior, parent replacement, retained-object metadata, cleanup, and all restore selectors on Linux/macOS/Windows | covered; arbitrary hostile relocation of an already-open Unix directory is outside the frozen guarantee |
| G6 | Safe cooperative same-process and same-host storage operations | Verify catches graph/reference corruption; transactional claims/retries protect writes; the production Coordinator adds an exclusive, fail-fast outer Lease for participating same-host repository operations, including valid `simulate gc` | In-process: `TestConcurrentStoreSameFile`, `TestConcurrentStoreSameChunk`, `TestConcurrentStoreFolderStress`, `TestRepeatedJitteredStoreGCRestoreInterleaving`, `TestRepeatedJitteredStoreGCRestoreRemoveInterleaving`. Native/Coordinator: `TestNativeLockContentionAndReacquire`, `TestWindowsNativeLockContentionAndReacquire`, `TestProductionCoordinatorIntegratedNativeLifecycle`. Linux process proof includes independent contention, killed-holder release, live-GC exclusion, and Phase 9 simulate-GC holder/reacquisition proof | covered; native Linux/macOS/Windows lifecycle and PostgreSQL route are proven. Cross-host, NAS, NFS/SMB, and distributed coordination are not claimed |
| G7 | Deep corruption detection (payload/offset/tail and authoritative placement) | Verify deep validates decoded payload hashes and container continuity and consumes Catalog placement authority for every legacy-only, packed-only, and mixed file recipe entry | Existing transform/AES-GCM tests plus Phase 9 dual-backend healthy/fault legacy, packed, mixed, companion, malformed-range, corruption, and truthful-count proof | covered; missing/conflicting/incomplete placement fails closed and packed chunks are not omitted |
| G8 | Corrective health gate contract stability | Doctor phase model and JSON/exit-code contract tests | `TestDoctorCommand`, `TestDoctorJSONContractConsistency`, `TestDoctorJSONFailureShortPathSingleMachineReadablePayload`, `TestDoctorRepeatedRecoverableStateConvergesAndPreservesLiveData` | covered |

## Post-v1.0 Extension Guarantees (v1.1+)

These rows track guarantees added after the v1.0 baseline. They are intentionally
separate from the frozen v1.0 core matrix (G1-G8).
This extends the correctness model from storage invariants to interaction semantics,
physical-file graph coherence, and snapshot-based retention.

| ID | Guarantee | Primary verify evidence | Primary test evidence | Status |
| --- | --- | --- | --- | --- |
| G9 | Interface correctness for batch CLI orchestration: isolated execution, deterministic ordering, and truthful machine-readable reporting | CLI batch contract checks (per-item status + summary + exit behavior) | `TestAdversarialG9BatchSemanticsOrchestration` (partial failure isolation, dry-run parity, duplicate explosion, fail-fast control-flow, mixed `--input` chaos) | covered |
| G10 | Current-state physical mapping graph coherence is audited in standard verify | Standard verify checks orphan `physical_file` rows, `logical_file.ref_count` mismatches, and negative `logical_file.ref_count` states before deeper storage checks | `TestVerifySystemStandardPassesOnConsistentPhysicalGraph`, `TestVerifySystemStandardDetectsOrphanPhysicalFileRows`, `TestVerifySystemStandardDetectsLogicalRefCountMismatch`, `TestVerifySystemStandardDetectsNegativeLogicalRefCount`, `TestDoctorSurfacesPhysicalMappingIntegrityFailures` | covered |
| G11 | GC only executes on an audited coherent physical-root graph | GC refuses (real and dry-run) when physical_file graph has any integrity issue; operator recovery path: repair ref-counts → GC succeeds | `TestRunGCRefusesOnOrphanPhysicalFileRows`, `TestRunGCRefusesOnNegativeLogicalRefCounts`, `TestRunGCRefusesOnPhysicalIntegrityIssues`, `TestRunGCDryRunRefusesOnDriftedGraph`, `TestRunGCSucceedsAfterRepairLogicalRefCounts`, `TestRepairThenVerifyThenGCSmoke` | covered |
| G12 | Invariant failures expose stable machine-readable classification and operator guidance | Invariant-related errors include stable `invariant_code`; CLI error payloads include optional `recommended_action` without changing core failure class/exit code semantics | `TestCodeExtractsTypedInvariantCode`, `TestVerifySystemStandardDetectsOrphanPhysicalFileRows`, `TestVerifySystemStandardDetectsLogicalRefCountMismatch`, `TestRepairLogicalRefCountsResultWithDBRefusesOrphanPhysicalRows`, `TestRunGCRefusesOnPhysicalIntegrityIssues`, `TestDoctorJSONFailureIncludesInvariantCodeAndActionWhenAvailable`, `TestDoctorTextFailureIncludesInvariantCodeAndActionWhenAvailable` | covered |
| G13 | Batch maintenance commands expose deterministic execution semantics and invariant-aware per-item reporting | Batch restore/remove/repair payloads clearly separate overall `status` from per-item result `status`, explicitly report `execution_mode` as `continue_on_error` or `fail_fast`, preserve deterministic ordering, and include `invariant_code` / `recommended_action` on invariant-related item failures when available | `TestEmitBatchCommandReportJSONSchema`, `TestEmitBatchCommandReportJSONIncludesNonFailureMessages`, `TestRunRepairCommandBatchJSONPartialFailure`, `TestRunRepairCommandBatchInvariantFailureUsesVerifyExitAndMetadata`, `TestBatchFlagsEndToEnd` | covered |
| G14 | Snapshot-retained content is GC-safe | Logical-file liveness is computed from the union of current-state roots (`physical_file`) and snapshot roots (`snapshot_file`); delete-by-ID is refused when the logical file is snapshot-retained; GC pre-flight computes `ReachabilitySummary` and skips containers whose chunks are reachable from any retained logical file | `TestListRetainedLogicalFileIDs`, `TestIsLogicalFileReferencedBySnapshot`, `TestComputeReachabilitySummary`, `TestRemoveFailsWhenLogicalFileIsRetainedBySnapshot`, `TestRunGCDoesNotDeleteSnapshotRetainedContainer`, `TestRunGCDryRunDoesNotCountSnapshotRetainedContainerAsReclaimable`, `TestAdversarialG14SnapshotRetainedGCGuardUnderChurn` | covered |
| G15 | Snapshot deletion only changes metadata and future GC eligibility | Deleting a snapshot removes only `snapshot` and `snapshot_file` rows; logical content remains intact immediately after delete, while the retained-logical set is recomputed so newly unreferenced content becomes eligible only for a later GC pass | `TestDeleteSnapshotRemovesSnapshotRowsOnly`, `TestAdversarialG17RetentionRootTransitionChurn` | covered |
| G16 | Stats expose snapshot-retention pressure to operators | Global stats report how many logical files and bytes are retained only by current state, only by snapshots, shared by both, and retained by snapshots overall so operators can explain why GC is not reclaiming content | `TestRunStatsResultIncludesSnapshotRetentionVisibility`, `TestRunStatsCommandJSONIncludesSnapshotRetention`, `TestStatsCommandHuman`, `TestAdversarialG16SnapshotQueryContractChaos` | covered |
| G17 | Verify and doctor audit persisted snapshot reachability integrity | Standard verify rejects persisted snapshot anomalies (`snapshot_file` orphan logical references, invalid referenced logical lifecycle states, and retained non-empty files with missing chunk graph) using stable invariant codes; doctor text report surfaces snapshot-retention integrity counters alongside physical mapping counters | `TestVerifySystemStandardPassesWithConsistentSnapshotReachability`, `TestVerifySystemStandardDetectsOrphanSnapshotLogicalReference`, `TestVerifySystemStandardDetectsSnapshotInvalidLifecycleState`, `TestVerifySystemStandardDetectsSnapshotRetainedMissingChunkGraph`, `TestFormatDoctorTextReportGoldenHealthy`, `TestFormatDoctorTextReportGoldenDegraded`, `TestAdversarialG15CorruptedSnapshotMetadataDetectionConservativeGC` | covered |

## Open Work Tracking

Use this section for branch-specific additions that are not yet fully covered.

| Item | Target evidence | Owner | Status |
| --- | --- | --- | --- |
| Long-run randomized fault loop expansion | Stress-tier seeded randomized lifecycle loop (`TestStoreLifecycleSeededRandomizedOperationOrder`) plus dedicated long-run soak (`TestRandomizedLongRunLifecycleSoak`) and repeated CI long-run passes | TBD | completed |
| Multi-process contention (non-goal for v1.0 baseline) | Phase 12 native runtime plus Phase 13 Linux independent-process contention, killed-holder release, live-GC exclusion, and PostgreSQL advisory-session evidence | Phases 12–13; named required-CI preservation in Phase 18 | completed within the documented platform boundary |
| Atomic restore exact/no-overwrite/confinement proof | Verify exact targets are never reinterpreted, no-overwrite is atomically non-replacing, intentional overwrite remains atomic, parent replacement cannot redirect mutation, retained-object metadata preserves identity, and cleanup remains confined | v1.13.13 Phase 9 `P7-RST1-001`–`006` and `P7-RST2-001`–`006` on Linux/macOS/Windows | completed within the frozen same-host/local-filesystem threat bound |
| Dry-run support for `remove --stored-path` (deferred beyond v1.2) | Active `Engine.RemoveStoredPaths` dry-run path, production CLI routing, deterministic result reporting, and non-mutation regressions including `TestRemoveStoredPathsDryRunPlansExistingMapping`, `TestRemoveStoredPathsDryRunDoesNotMutateCatalog`, and `TestRemoveStoredPathsDryRunPreservesSnapshotRetentionParityGap` | Completed in v1.13.8 | completed |
| Batch delete optimization for remove cascade (v1.4+ optimization) | Current v1.2 implementation uses O(N) per-path delete + invariant check; optimize to batch DELETE + single post-batch invariant check; add micro-benchmarks comparing per-path vs batch semantics; ensure no correctness regression | v1.4 performance enhancement | deferred |
| Optional post-batch invariant enforcement strategy | Current v1.2 batch operations preserve invariants per item; future performance-oriented mode may allow post-batch invariant validation while keeping deterministic error semantics | v1.4+ performance track | deferred |
| Structured logging for invariant violations (deferred beyond v1.2) | Add optional structured event emission for invariant failures such as `INVARIANT_VIOLATION logical_file_ref_count_mismatch`; cover via CLI/logging contract tests without weakening hard-fail behavior | Post-v1.2 observability track | deferred |
| Batch repair scope extensions | Extend `repair --batch` beyond `ref-counts` target to additional scopes (`repair all`, `repair --scope physical-graph`) while preserving deterministic reporting semantics | Post-v1.2 repair roadmap | deferred |
| Automatic physical-layer repair inside doctor | Keep verify/doctor detect-only for `physical_file` drift even though explicit `repair ref-counts` exists; preserve operator intent and avoid hidden metadata mutation during health checks | Post-v1.2 repair strategy track | deferred |
| GC dry-run physical integrity bypass flag | Allow `--force` to skip `CheckPhysicalFileGraphIntegrity` pre-flight for advanced operator scenarios | Future operator tooling sprint | deferred |

## v1.13.11+ Hardening and Required Evidence

These validation groups extend the existing G1-G17 mapping without assigning
new guarantee IDs. They record the difference between automated test coverage
and fail-closed hosted execution proof.

| Validation group | Contract | Representative evidence | Evidence classification | Hosted requirement and boundary |
| --- | --- | --- | --- | --- |
| Storage and recovery (G1-G5) | Deterministic restore, fail-closed recovery, reference-safe GC, exact destination meaning, atomic non-replacing publication, and retained-parent confinement | Existing unit/integration/adversarial suites plus v1.13.13 Phase 9 `P7-RST1-*` and `P7-RST2-*` proof | unit, integration, adversarial, native cross-platform, race/fault, required CI | Linux/macOS/Windows same-host/local-filesystem semantics only; the documented open-Unix-directory relocation limit remains |
| Coordination (G6) | Same-process/same-host protection, supported native primitives, production Coordinator lifecycle, representative Linux process semantics, and coordinated live `simulate gc` | Phase 12/13 native and process tests plus v1.13.13 Phase 9 `P7-GCC-*` proof | integration, adversarial, cross-platform, dual-backend, required CI | Native Linux/macOS/Windows and SQLite/PostgreSQL routes. No cross-host, NAS, NFS/SMB, distributed, or network-filesystem proof |
| Corruption and health (G7-G8) | Deep transform-aware corruption detection, complete authoritative legacy/packed/mixed file placement verification, and stable doctor/verify contracts | Existing package/integration/adversarial suites plus v1.13.13 Phase 9 `P7-VFY-*` dual-backend proof | unit, integration, adversarial, dual-backend, required CI | Every authoritative placement is visited; support is not broadened beyond existing formats/codecs |
| CLI and physical graph (G9-G13) | Deterministic batch behavior, physical-graph audit, GC refusal, and invariant-aware reporting | Existing command, maintenance, integration, and adversarial suites | unit, integration, adversarial, required CI | Required jobs execute the covered packages; no batch optimization, hidden repair, or bypass behavior is claimed |
| Snapshot retention (G14-G17) | Snapshot roots remain GC-safe and auditable | G14-G17 package/integration tests and explicit adversarial selector | unit, integration, adversarial, required CI | Required adversarial execution retains the G14-G17 family; scope is snapshot-retention correctness only |
| Container integrity (Phase 14) | Validate outer ranges before allocation/I/O; reject header overlap; require header/catalog/physical maximum consistency; preserve persisted maximum; use overflow-safe append; detect short header writes; preserve v0/v1 compatibility | `TestValidateContainerRangeBoundaries`, `TestFileContainerReadAtRejectsInvalidRangeBeforeAllocation`, `TestOpenExistingContainerRejectsHeaderCatalogMaxSizeMismatch`, `TestFileContainerAppendRejectsOverflowAsContainerFull`, `TestStorageBlockReaderUsesCatalogContainerMaxSize` | unit, integration, required CI | Broad required matrix; this is container range/header proof, not decompression proof |
| Bounded decompression (Phase 15) | Enforce exact expected size and the absolute 4 MiB decompression ceiling before decoder/allocation; bound zstd output and decoder memory/window; preserve identity exactness | `TestNoneDecompressRequiresExactExpectedSize`, `TestZstdDecompressRejectsOutputBeyondExpectedSize`, `TestZstdDecompressBoundsConcatenatedFramesAcrossAggregateOutput`, Restore/Verify/reuse bound regressions | unit, integration, adversarial, required CI | Broad required matrix; 4 MiB is a decompression ceiling, not a container maximum |
| JSON integer fidelity (Phase 16) | Preserve exact integer tokens recursively with `UseNumber` and strict EOF for stats, inspect, and simulate-GC | `TestToObjectMapPreservesExactJSONNumbers`, `TestRunStatsCommandJSONPreservesExactLargeIntegers` | unit, integration, required CI | Integers remain JSON numbers and output shape is unchanged; downstream JavaScript precision is not claimed |
| SQL mutation cardinality (Phase 17) | Audit 70 production mutations; harden 20 required-row sites; retain 18 zero-safe and 32 already-safe sites; validate affected rows without imposing blanket exact-one semantics | `TestMutationRowsAffectedContractAcrossBackends`, required container/storage/recovery/repair/remove/GC rollback regressions | unit, dual-backend integration, rollback/adversarial, named required CI | The PostgreSQL cardinality event is required. No claim that every mutation must affect exactly one row |
| CI execution proof (Phase 18) | Reject missing, malformed, skipped, or non-passing selected backend/storage/recovery/coordination events | CI JSON parsers plus `scripts/audit_ci_enforcement.sh` and its regression suite | required CI | Named pass-event proof supplements, rather than replaces, broad job success |
| Benchmarks | Require valid four-profile candidate integrity while treating hosted timing as informational | `benchmark_gate.py integrity`; hosted-advisory comparator and exit verification | required CI integrity, advisory timing, deferred hard performance enforcement | Integrity/evaluator failures block; valid timing threshold crossings do not. Hard timing enforcement is deferred to controlled infrastructure |

## Exit Criteria

1. Every guarantee row remains mapped to at least one automated test and/or verify check.
2. Quality, correctness-matrix, integration-stress, integration-long-run,
   adversarial, legacy-compatibility, smoke, cross-platform, benchmark-integrity,
   and benchmark-timing-advisory evaluation all complete successfully; required
   named pass events are present with no matching required skip.
3. Contract-sensitive checks (doctor and verify JSON shape, exit codes, failure typing) stay stable.
