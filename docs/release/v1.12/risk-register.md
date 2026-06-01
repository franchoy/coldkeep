# v1.12 Risk Register

Severity logic (consistent with the prior stabilization train):

- **S0** — catastrophic: silent data corruption, reachable data deletion, false verify success,
  snapshot retention violation, or path traversal outside the intended root.
- **S1** — critical: crash-consistency failures, serious JSON automation breakage, unsafe filename
  trust, and migration parity failures.

| ID | Risk | Severity | Area | Phase target | Status | Notes |
|---|---|---|---|---|---|---|
| CK-112-R001 | Engine `Verify` does not honor engine-owned DB | S1 | engine/verify | Phase 1 | fixed | Fixed in v1.12.1: `DefaultEngine.Verify` now delegates to `maintenance.VerifyCommandWithDBAndContainersDir(Config.DB, ...)` instead of the global path that called `db.ConnectDB()`. Regression tests: `internal/engine/verify_db_ownership_test.go`. |
| CK-112-R002 | Existing candidate mutating request/result structs are too thin | S1 | engine contracts | Phase 2 | fixed | Fixed in Phase 2: `internal/engine/candidates.go` contracts expanded to represent real command behavior (store/restore/remove/gc/snapshot create-list-show-stats-diff-delete-restore/repair/recovery), with shared neutral types (`OperationWarning`, `BatchSummary`, `ExecutionMode`, `SnapshotQuery`) and enums for restore/remove modes and destination modes. Recovery re-modeled from an incorrect restore-like placeholder to a corrective report. Renderer/backend neutrality enforced by `TestCandidateContractFieldTypesAreNeutral`; representability proven by per-operation construction tests in `internal/engine/contracts_test.go`. Deferrals (batch `InputPath` ownership, no error taxonomy, no interface methods/routing) documented in `engine-baseline.md`. No routing performed; activation gated by later phases. |
| CK-112-R003 | Catalog facade may accidentally become SQLite-specific | S1 | catalog/db | Phase 4 | open | Add backend contract tests; isolate dialect differences behind adapters. |
| CK-112-R004 | CLI behavior drift during routing | S1 | CLI/engine | all migration phases | open | Require parity tests (human output, exit codes, errors). |
| CK-112-R005 | JSON output drift during routing | S1 | CLI/rendering | all migration phases | open | Require JSON envelope/shape tests. |
| CK-112-R006 | Restore safety validation could remain CLI-only | S0/S1 | restore/catalog | Phase 7 | open | Engine/catalog must enforce "no write outside destination" and traversal/symlink safety too. |
| CK-112-R007 | GC reachability may be split between old/new paths | S0/S1 | GC/catalog | Phase 6 | open | Catalog contract must represent packed and legacy roots; GC must never delete reachable data. |

## Notes

- Risks discovered during a phase are added here, not silently fixed out of scope.
- A risk is only closed with evidence (test, parity proof, or documented accepted deferral).
- No release-tracker row is closed without evidence.
