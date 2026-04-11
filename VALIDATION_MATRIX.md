
# v1.0 Validation Matrix

This document is the authoritative mapping between public guarantees (README)
and enforced evidence (tests and verify checks).

## Guarantee ID Stability

Guarantee IDs (G1–G8) are part of the public validation contract.

- IDs are stable across v0.10 and v1.0
- Guarantees may be reworded, but IDs must not change
- New guarantees must use new IDs (G9, G10, ...)

This prevents future “renumbering drift”.

All guarantees below are enforced through integration tests and verified under repeated GC / restart / restore cycles.

This document originated from the v0.9/v0.10 trust-validation work and is now
the maintained v1.x guarantee-to-evidence contract: v1.0 storage-core
guarantees (G1-G8) plus post-v1.0 interface-correctness extensions (G9+).

## Scope

- Target: single-node trust model for v1.0 core plus v1.1+ interface contracts
- Surface: existing `verify` and `doctor` contracts (no new top-level validate command)
- Goal: each guarantee maps to automated evidence (verify checks, tests, or both)

## Guarantees to Evidence

| ID | Guarantee | Primary verify evidence | Primary test evidence | Status |
| --- | --- | --- | --- | --- |
| G1 | Deterministic, byte-identical restore | Deep restore path validates chunk hash and final file hash | `TestRepeatRestoreDeterminism`, `TestSameInputSameChunkGraph`, `TestStoreRemoveGCRestartStoreConvergesChunkGraph` | covered |
| G2 | Repeat store does not drift chunk graph | Reuse and graph checks in store path, plus verify full/system checks | `TestRepeatedStorePreservesChunkGraphDeterminism` | covered |
| G3 | No exposure of partially written or inconsistent data | Recovery + verify model excludes/processes invalid lifecycle states, including standard verify enforcement that each COMPLETED chunk has exactly one blocks row, rollback-safe sealing-marker transitions, quarantine of damaged active containers without harming unrelated live data, and ghost-byte sealing-container quarantine with preserved live data | `TestStartupRecoverySimulation`, `TestDoctorAbortsProcessingLogicalFilesFromRecoverableState`, `TestVerifyStandard/detects completed chunk missing block row`, `TestStoreSealingMarkerUpdateFailureAbortsSafelyAndRecovers`, `TestStartupRecoveryQuarantinesDamagedActiveContainerAndPreservesOtherLiveData`, `TestStartupRecoveryQuarantinesGhostByteSealingContainerAndPreservesOtherLiveData` | covered |
| G4 | GC is reference-safe: no reachable chunk is ever deleted | GC liveness checks use `live_ref_count OR pin_count`; verify post-GC integrity | `TestStoreGCRestore`, `TestGCRestorePinRaceContainerNotDeleted`, `TestStoreLifecycleSeededRandomizedOperationOrder` | covered |
| G5 | Atomic restore replacement (within single-node local filesystem semantics) | Restore path writes temp + fsync + atomic rename | `TestRestoreFailurePreservesExistingOutput` (explicit atomicity and cleanup), `TestRestoreAtomicityWithTestHook`, `TestRestoreAtomicityWithCorruption`, `TestStoreGCRestore`, `TestSampleDatasetEndToEnd` | covered |
| G6 | Safe in-process concurrent storage operations | Verify catches graph/reference corruption; transactional claims/retries in write path | `TestConcurrentStoreSameFile`, `TestConcurrentStoreSameChunk`, `TestConcurrentStoreFolderStress`, `TestRepeatedJitteredStoreGCRestoreInterleaving`, `TestRepeatedJitteredStoreGCRestoreRemoveInterleaving` (all in-process, shared DB and store path, dedup races, stress) | covered (multi-process contention and external crash overlap not covered; see open work) |
| G7 | Deep corruption detection (payload/offset/tail) | Verify deep validates decoded payload hashes and container continuity, including authenticated AES-GCM decode failures on tampered ciphertext, tampered nonce metadata, wrong-key mismatch, and malformed key configuration (invalid length and invalid encoding) | `TestVerifySystemDeepDetectsChunkDataCorruption`, `TestVerifySystemDeepDetectsAESGCMTamperedCiphertext`, `TestVerifySystemDeepDetectsAESGCMNonceMetadataTampering`, `TestVerifySystemDeepDetectsAESGCMWrongKeyMismatch`, `TestVerifySystemDeepDetectsAESGCMInvalidKeyConfiguration`, `TestVerifySystemDeepDetectsAESGCMInvalidHexKeyConfiguration`, `TestVerifySystemDeepDetectsTrailingBytesAfterLastBlock`, `TestVerifySystemDeepAggregatesChunkErrors` | covered |
| G8 | Corrective health gate contract stability | Doctor phase model and JSON/exit-code contract tests | `TestDoctorCommand`, `TestDoctorJSONContractConsistency`, `TestDoctorJSONFailureShortPathSingleMachineReadablePayload`, `TestDoctorRepeatedRecoverableStateConvergesAndPreservesLiveData` | covered |

## Post-v1.0 Extension Guarantees (v1.1+)

These rows track guarantees added after the v1.0 baseline. They are intentionally
separate from the frozen v1.0 core matrix (G1-G8).
This extends the correctness model from storage invariants to interaction semantics.

| ID | Guarantee | Primary verify evidence | Primary test evidence | Status |
| --- | --- | --- | --- | --- |
| G9 | Interface correctness for batch CLI orchestration: isolated execution, deterministic ordering, and truthful machine-readable reporting | CLI batch contract checks (per-item status + summary + exit behavior) | `TestAdversarialG9BatchSemanticsOrchestration` (partial failure isolation, dry-run parity, duplicate explosion, fail-fast control-flow, mixed `--input` chaos) | covered |

## Open Work Tracking

Use this section for branch-specific additions that are not yet fully covered.

| Item | Target evidence | Owner | Status |
| --- | --- | --- | --- |
| Long-run randomized fault loop expansion | Stress-tier seeded randomized lifecycle loop (`TestStoreLifecycleSeededRandomizedOperationOrder`) plus dedicated long-run soak (`TestRandomizedLongRunLifecycleSoak`) and repeated CI long-run passes | TBD | completed |
| Multi-process contention (non-goal for v1.0 baseline) | Separate post-v1.0 track | TBD | deferred |
| Atomic restore explicit failure-mode and atomicity | Simulate restore failures before/after rename; verify original output file is preserved, no partial/corrupt final file is visible, and temp files are cleaned up; assert destination file is byte-identical and no temp files remain after failure | `TestRestoreFailurePreservesExistingOutput`, `TestRestoreAtomicityWithTestHook`, `TestRestoreAtomicityWithCorruption`, `TestRestoreFailureDoesNotCorruptDestination` | completed |
| Dry-run support for `remove --stored-path` (v1.2 deferral) | Extend remove tx primitive to support rollback-safe preview mode; implement in CLI with `--dry-run` flag; add integration tests validating preview output matches dry-run semantics | Phase 5 planned feature | deferred |
| Batch delete optimization for remove cascade (v1.4+ optimization) | Current v1.2 implementation uses O(N) per-path delete + invariant check; optimize to batch DELETE + single post-batch invariant check; add micro-benchmarks comparing per-path vs batch semantics; ensure no correctness regression | v1.4 performance enhancement | deferred |
| Structured logging for invariant violations (Phase 5+) | Add optional structured event emission for invariant failures such as `INVARIANT_VIOLATION logical_file_ref_count_mismatch`; cover via CLI/logging contract tests without weakening hard-fail behavior | Phase 5 observability enhancement | deferred |

## Exit Criteria

1. Every guarantee row remains mapped to at least one automated test and/or verify check.
2. Quality, integration-correctness, integration-stress, integration-long-run, and smoke all pass.
3. Contract-sensitive checks (doctor and verify JSON shape, exit codes, failure typing) stay stable.
