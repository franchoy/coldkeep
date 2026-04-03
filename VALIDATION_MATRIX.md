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
| No exposure of partially written or inconsistent data | Recovery + verify model excludes/processes invalid lifecycle states, including standard verify enforcement that each COMPLETED chunk has exactly one blocks row, rollback-safe sealing-marker transitions, and quarantine of damaged active containers without harming unrelated live data | `TestStartupRecoverySimulation`, `TestDoctorAbortsProcessingLogicalFilesFromRecoverableState`, `TestVerifyStandard/detects completed chunk missing block row`, `TestStoreSealingMarkerUpdateFailureAbortsSafelyAndRecovers`, `TestStartupRecoveryQuarantinesDamagedActiveContainerAndPreservesOtherLiveData` | covered |
| Non-destructive garbage collection | GC liveness checks use `live_ref_count OR pin_count`; verify post-GC integrity | `TestStoreGCRestore`, `TestGCRestorePinRaceContainerNotDeleted`, `TestStoreLifecycleSeededRandomizedOperationOrder` | covered |
| Atomic restore operations | Restore path writes temp + fsync + atomic rename | `TestStoreGCRestore`, `TestSampleDatasetEndToEnd` | covered |
| Safe concurrent storage operations | Verify catches graph/reference corruption; transactional claims/retries in write path | `TestConcurrentStoreSameFile`, `TestConcurrentStoreSameChunk`, `TestConcurrentStoreFolderStress`, `TestRepeatedJitteredStoreGCRestoreInterleaving`, `TestRepeatedJitteredStoreGCRestoreRemoveInterleaving` | covered |
| Deep corruption detection (payload/offset/tail) | Verify deep validates decoded payload hashes and container continuity | `TestVerifySystemDeepDetectsChunkDataCorruption`, `TestVerifySystemDeepDetectsTrailingBytesAfterLastBlock`, `TestVerifySystemDeepAggregatesChunkErrors` | covered |
| Corrective health gate contract stability | Doctor phase model and JSON/exit-code contract tests | `TestDoctorCommand`, `TestDoctorJSONContractConsistency`, `TestDoctorJSONFailureShortPathSingleMachineReadablePayload`, `TestDoctorRepeatedRecoverableStateConvergesAndPreservesLiveData` | covered |

## Open Work Tracking

Use this section for branch-specific additions that are not yet fully covered.

| Item | Target evidence | Owner | Status |
| --- | --- | --- | --- |
| Long-run randomized fault loop expansion | Stress-tier seeded randomized lifecycle loop (`TestStoreLifecycleSeededRandomizedOperationOrder`) plus dedicated long-run soak (`TestRandomizedLongRunLifecycleSoak`) and repeated CI long-run passes | TBD | completed |
| Multi-process contention (non-goal for v1.0 baseline) | Separate post-v1.0 track | TBD | deferred |

## Exit Criteria

1. Every guarantee row remains mapped to at least one automated test and/or verify check.
2. Quality, integration-correctness, integration-stress, integration-long-run, and smoke all pass.
3. Contract-sensitive checks (doctor and verify JSON shape, exit codes, failure typing) stay stable.
