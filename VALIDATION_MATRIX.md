# v1.0 Validation Matrix

This document tracks v0.9 guarantees and the evidence used to validate them
during the v0.10 trust-validation phase.

## Scope

- Target: single-node trust proof before v1.0
- Surface: existing `verify` and `doctor` contracts (no new top-level validate command)
- Goal: each guarantee maps to automated evidence (verify checks, tests, or both)

## Guarantees to Evidence

| Guarantee | Primary verify evidence | Primary test evidence | Status |
| --- | --- | --- | --- |
| Deterministic, byte-identical restore | Deep restore path validates chunk hash and final file hash | `TestRepeatRestoreDeterminism`, `TestSameInputSameChunkGraph`, `TestStoreRemoveGCRestartStoreConvergesChunkGraph` | covered |
| Repeat store does not drift chunk graph | Reuse and graph checks in store path, plus verify full/system checks | `TestRepeatedStorePreservesChunkGraphDeterminism` | covered |
| No exposure of partially written or inconsistent data | Recovery + verify model excludes/processes invalid lifecycle states, including standard verify enforcement that each COMPLETED chunk has exactly one blocks row, rollback-safe sealing-marker transitions, quarantine of damaged active containers without harming unrelated live data, and ghost-byte sealing-container quarantine with preserved live data | `TestStartupRecoverySimulation`, `TestDoctorAbortsProcessingLogicalFilesFromRecoverableState`, `TestVerifyStandard/detects completed chunk missing block row`, `TestStoreSealingMarkerUpdateFailureAbortsSafelyAndRecovers`, `TestStartupRecoveryQuarantinesDamagedActiveContainerAndPreservesOtherLiveData`, `TestStartupRecoveryQuarantinesGhostByteSealingContainerAndPreservesOtherLiveData` | covered |
| Non-destructive garbage collection | GC liveness checks use `live_ref_count OR pin_count`; verify post-GC integrity | `TestStoreGCRestore`, `TestGCRestorePinRaceContainerNotDeleted`, `TestStoreLifecycleSeededRandomizedOperationOrder` | covered |
| Atomic restore replacement on the single-node local filesystem model | Restore path writes temp + fsync + atomic rename | `TestStoreGCRestore`, `TestSampleDatasetEndToEnd` (restore correctness only; does not directly test atomicity or failure-mode durability) | partially covered |
| Atomic restore replacement (within single-node local filesystem semantics) | Restore path writes temp + fsync + atomic rename | `TestRestoreFailurePreservesExistingOutput` (explicit atomicity and cleanup), `TestRestoreAtomicityWithTestHook`, `TestRestoreAtomicityWithCorruption`, `TestStoreGCRestore`, `TestSampleDatasetEndToEnd` | covered |
| Safe in-process concurrent storage operations | Verify catches graph/reference corruption; transactional claims/retries in write path | `TestConcurrentStoreSameFile`, `TestConcurrentStoreSameChunk`, `TestConcurrentStoreFolderStress`, `TestRepeatedJitteredStoreGCRestoreInterleaving`, `TestRepeatedJitteredStoreGCRestoreRemoveInterleaving` (all in-process, shared DB and store path, dedup races, stress) | covered (multi-process contention and external crash overlap not covered; see open work) |
| Deep corruption detection (payload/offset/tail) | Verify deep validates decoded payload hashes and container continuity, including authenticated AES-GCM decode failures on tampered ciphertext, tampered nonce metadata, wrong-key mismatch, and malformed key configuration (invalid length and invalid encoding) | `TestVerifySystemDeepDetectsChunkDataCorruption`, `TestVerifySystemDeepDetectsAESGCMTamperedCiphertext`, `TestVerifySystemDeepDetectsAESGCMNonceMetadataTampering`, `TestVerifySystemDeepDetectsAESGCMWrongKeyMismatch`, `TestVerifySystemDeepDetectsAESGCMInvalidKeyConfiguration`, `TestVerifySystemDeepDetectsAESGCMInvalidHexKeyConfiguration`, `TestVerifySystemDeepDetectsTrailingBytesAfterLastBlock`, `TestVerifySystemDeepAggregatesChunkErrors` | covered |
| Corrective health gate contract stability | Doctor phase model and JSON/exit-code contract tests | `TestDoctorCommand`, `TestDoctorJSONContractConsistency`, `TestDoctorJSONFailureShortPathSingleMachineReadablePayload`, `TestDoctorRepeatedRecoverableStateConvergesAndPreservesLiveData` | covered |

## Open Work Tracking

Use this section for branch-specific additions that are not yet fully covered.

| Item | Target evidence | Owner | Status |
| --- | --- | --- | --- |
| Long-run randomized fault loop expansion | Stress-tier seeded randomized lifecycle loop (`TestStoreLifecycleSeededRandomizedOperationOrder`) plus dedicated long-run soak (`TestRandomizedLongRunLifecycleSoak`) and repeated CI long-run passes | TBD | completed |
| Multi-process contention (non-goal for v1.0 baseline) | Separate post-v1.0 track | TBD | deferred |

| Atomic restore failure-mode tests | Simulate restore failures before/after rename; verify original output file is preserved, no partial/corrupt final file is visible, and temp files are cleaned up | `TestRestoreFailurePreservesExistingOutput`, `TestRestoreAtomicityWithTestHook`, `TestRestoreAtomicityWithCorruption` | completed |
| Explicit atomicity: restore failure before rename leaves destination file untouched | Simulate restore failure after temp file is written but before rename; assert destination file is byte-identical and no temp files remain (temp file is cleaned up and never becomes visible as final output) | `TestRestoreFailureDoesNotCorruptDestination` | completed |

## Exit Criteria

1. Every guarantee row remains mapped to at least one automated test and/or verify check.
2. Quality, integration-correctness, integration-stress, integration-long-run, and smoke all pass.
3. Contract-sensitive checks (doctor and verify JSON shape, exit codes, failure typing) stay stable.
