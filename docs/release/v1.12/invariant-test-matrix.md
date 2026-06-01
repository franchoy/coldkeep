# v1.12 Engine/Catalog Invariant Test Matrix

## Purpose

This document maps every v1.12 routed operation to the core correctness invariant it protects and
references the tests that prove the invariant at the engine, CLI, and catalog boundaries.

Phase 11 is a proof phase, not a migration phase.

## Routed operation matrix

| Operation | Routed in phase | Critical invariant | Test file | Test name(s) | Status |
|---|---:|---|---|---|---|
| Stats | baseline (active before v1.12 routing train) | Human/JSON contract stability and CLI parity | `cmd/coldkeep/main_test.go` | `TestStatsCommandHuman`, `TestStatsCommandJSON`, `TestRunStatsCommandJSONContract`, `TestStatsCLIHuman`, `TestStatsCLIJSON`, `TestBackwardCompatibilityStatsCLI` | covered |
| Inspect | baseline (active before v1.12 routing train) | Human/JSON contract stability and CLI parity | `cmd/coldkeep/main_test.go` | `TestRunInspectCommandFileTextShowsChunkerAndChunkSummary`, `TestRunInspectCommandJSONContractByEntity`, `TestInspectCLIHuman`, `TestInspectCLIJSON`, `TestBackwardCompatibilityInspectFileCLI` | covered |
| Verify (system/file) | Phase 10 thin-wrapper routing | Uses injected DB dependency and must fail closed (no false pass) while preserving human/JSON output | `internal/engine/verify_db_ownership_test.go`, `cmd/coldkeep/verify_engine_routing_test.go` | `TestDefaultEngineVerifyUsesConfiguredDB`, `TestDefaultEngineVerifyFileUsesConfiguredDB`, `TestVerifySystemEngineRoutingJSON`, `TestVerifySystemEngineRoutingText`, `TestVerifyFileEngineRoutingJSON` | covered |
| Snapshot list | Phase 5 | Read-side routing and JSON contract stability | `internal/engine/snapshot_engine_test.go`, `cmd/coldkeep/snapshot_engine_routing_test.go` | `TestSnapshotListRoutesThroughEngine`, `TestSnapshotListTypeFilter`, `TestSnapshotListLabelAndMeta`, `TestSnapshotListEngineRoutingJSON` | covered |
| Snapshot show | Phase 5 | Not-found behavior and snapshot/file metadata parity | `internal/engine/snapshot_engine_test.go`, `cmd/coldkeep/snapshot_engine_routing_test.go` | `TestSnapshotShowNotFound`, `TestSnapshotShowReturnsMetaAndFiles`, `TestSnapshotShowEngineRoutingJSON` | covered |
| Snapshot stats | Phase 5 | Aggregate/stat lineage output stability | `internal/engine/snapshot_engine_test.go`, `cmd/coldkeep/snapshot_engine_routing_test.go`, `cmd/coldkeep/main_test.go` | `TestSnapshotStatsBasic`, `TestSnapshotStatsEngineRoutingJSON`, `TestRunSnapshotCommandStatsTextShowsLineageBreakdownWhenParentExists`, `TestRunSnapshotCommandStatsJSONIncludesLineageFieldsWhenParentExists` | covered |
| Snapshot diff | Phase 5 | Diff summary/entry parity and query-filter behavior stability | `internal/engine/snapshot_engine_test.go`, `cmd/coldkeep/snapshot_engine_routing_test.go`, `cmd/coldkeep/main_test.go` | `TestSnapshotDiffSummaryFastPath`, `TestSnapshotDiffFullReturnsEntries`, `TestSnapshotDiffEngineRoutingJSON`, `TestSnapshotDiffSummaryEngineRoutingJSON`, `TestRunSnapshotCommandDiffForwardsAndFormatsJSON`, `TestRunSnapshotCommandDiffTextShowsMatchedAndTotalCounts` | covered |
| GC dry/live routed scope | Phase 6 | GC must never delete reachable data; live GC backend guardrail preserved | `internal/engine/gc_engine_test.go`, `cmd/coldkeep/gc_engine_routing_test.go` | `TestGCDryRunThroughEngineEmptyDB`, `TestGCDryRunEchoesFields`, `TestGCLiveRefusedOnSQLite`, `TestGCDryRunEngineRoutingJSON`, `TestGCDryRunEngineRoutingHuman` | covered |
| Restore by ID live | Phase 7 | Destination safety and routed live behavior parity | `internal/engine/restore_engine_test.go`, `cmd/coldkeep/restore_engine_routing_test.go` | `TestRestoreFailFastStopsOnFirstFailure`, `TestRestoreByIDEngineRoutingJSON`, `TestRestoreByIDEngineRoutingText` | covered |
| Restore by ID dry-run | Phase 7 | Dry-run must not write/mutate state | `internal/engine/restore_engine_test.go`, `cmd/coldkeep/restore_engine_routing_test.go` | `TestRestoreDryRunByIDThroughEngine`, `TestRestoreByIDDryRunEngineRoutingJSON` | covered |
| Store single file | Phase 8 | Store path must use injected context and preserve CLI output contract | `internal/engine/store_engine_test.go`, `cmd/coldkeep/store_engine_routing_test.go` | `TestStoreByFileThroughEngine`, `TestStoreRequiresInjectedStoreContext`, `TestStoreByFileEngineRoutingJSON`, `TestStoreByFileEngineRoutingText` | covered |
| Remove by ID live | Phase 9 | Removal must fail closed when snapshot retention forbids deletion; routed parity preserved | `internal/engine/remove_engine_test.go`, `cmd/coldkeep/remove_engine_routing_test.go` | `TestRemoveByIDThroughEngine`, `TestRemoveByIDRetainedSnapshotFailsClosed`, `TestRemoveByIDEngineRoutingJSON`, `TestRemoveByIDEngineRoutingText` | covered |
| Remove by ID dry-run | Phase 9 | Dry-run must not mutate state | `internal/engine/remove_engine_test.go`, `cmd/coldkeep/remove_engine_routing_test.go` | `TestRemoveByIDDryRunThroughEngine`, `TestRemoveByIDDryRunEngineRoutingJSON` | covered |

## Dependency ownership and direction evidence

- Engine dependency ownership and boundary guardrails:
  - `internal/engine/verify_db_ownership_test.go`
  - `internal/engine/store_engine_test.go` (`TestStoreRequiresInjectedStoreContext`)
  - `internal/engine/dependency_guard_test.go` (`TestEngineDependencyDirection`)
- Catalog boundary direction guardrails:
  - `internal/catalog/dependency_test.go` (`TestCatalogDependencyDirection`)
- CLI must not import catalog directly:
  - enforced by `internal/engine/dependency_guard_test.go` (`TestEngineDependencyDirection`)

## Catalog backend-neutrality and deferred-method evidence

- SQLite/PostgreSQL compatibility for implemented catalog methods:
  - `internal/catalog/backend_contract_test.go` (`TestCatalogContractFindLogicalFileAcrossBackends`, `TestCatalogContractFindPhysicalFilesAcrossBackends`, `TestCatalogContractFindSnapshotAcrossBackends`, `TestCatalogContractListSnapshotsAcrossBackends`, `TestCatalogContractLoadReachabilityRootsAcrossBackends`)
- Deferred methods remain explicit and fail closed with `ErrNotImplemented`:
  - `internal/catalog/backend_contract_test.go` (`TestCatalogContractDeferredMethodsAcrossBackends`)
  - `internal/catalog/service_test.go` (`TestServiceDeferredMethodsReturnErrNotImplemented`)
- Catalog exported types remain backend/renderer neutral:
  - `internal/catalog/neutrality_test.go` (`TestCatalogExportedTypesAreNeutral`)

## Deferred operations (intentionally unchanged in Phase 11)

| Operation | Reason deferred | Future phase |
|---|---|---|
| Recursive/folder store | Worker/folder orchestration parity proof needed before activation | v1.13+ |
| Stored-path restore | Destination/path-mode semantics need dedicated parity proof | v1.13+ |
| Snapshot restore migration to engine | Restore-plan safety boundary still deferred | v1.13+ |
| Stored-path remove / stored-paths remove | Distinct destructive path-addressing semantics need dedicated parity proof | v1.13+ |
| Repair migration to engine | Corrective semantics must remain fail-safe and non-masking | v1.13+ |
| Recovery migration to engine | Startup/quarantine fail-safe semantics must remain explicit | v1.13+ |

## Residual risk notes after Phase 11

- Phase 11 adds no new routing and no schema/storage/backend changes.
- Risk posture is improved by explicit operation-to-test mapping and added routed text-parity checks.
- Open risks remain only for intentionally deferred operations.

## Phase 12 release gate entry criteria

- Every routed v1.12 operation has a mapped invariant and at least one proving test.
- Dependency direction guardrails remain enforced (`cmd/coldkeep` not importing `internal/catalog`; catalog not importing engine/CLI).
- Catalog backend neutrality remains proven across SQLite/PostgreSQL for implemented methods.
- Deferred methods remain explicit (`ErrNotImplemented`) and deferred operations stay deferred.
- Full mandatory validation gate is green in required order:
  1. `gofmt -w` + `gofmt -l`
  2. `golangci-lint run ./...`
  3. `go vet ./...`
  4. `go test -count=1 ./...`
  5. `go test -race -count=1 ./...`
