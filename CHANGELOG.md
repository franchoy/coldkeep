# Changelog

All notable changes to this project will be documented in this file.

This project follows a lightweight, prototype-friendly versioning
approach.

Version numbers indicate conceptual milestones rather than
production stability.

v1.4 clarifies snapshot lineage semantics and release-gate guidance.

For the current operator-facing contract, see [README.md](README.md).
For guarantee-to-evidence mapping, see [VALIDATION_MATRIX.md](VALIDATION_MATRIX.md).
For release-gate execution, see [PRE_RELEASE_CHECKLIST.md](PRE_RELEASE_CHECKLIST.md).

------------------------------------------------------------------------

## [1.4.0] - 2026-04-19

Snapshot clarity and release hardening milestone.

v1.4 keeps the v1.3 retention model and makes the operator contract explicit:
snapshots are always self-contained; lineage metadata never creates restore
dependencies.

### Clarified

- **Self-contained snapshot contract** — snapshot content is independent of
  parent snapshot content at restore time.
- **`--from` lineage semantics** — `--from` records metadata for lineage
  analysis only; it does not create dependency chains.
- **Lineage tree interpretation** — `snapshot list --tree` is a metadata view,
  not an execution dependency graph.
- **Delete dry-run impact wording** — delete preview explicitly describes
  metadata/reference impact rather than guaranteed disk-space reclamation.
- **Release-gate guidance** — pre-release docs emphasize parent-delete +
  child-restore checks to validate lineage independence.

### Scope alignment (v1.4)

- No behavioral regression against the v1.3 snapshot retention safety model.
- Lineage remains informational only by design.
- Snapshot delete remains metadata-only.

------------------------------------------------------------------------

## [1.3.0] - 2026-04-18

Snapshot-layer and retention contract establishment.

v1.0 established storage correctness.
v1.1 established interface correctness.
v1.2 established physical-graph coherence.
v1.3 establishes snapshot-based retention as a correctness layer: the system
captures immutable point-in-time views, protects snapshot-retained content from
GC deletion, and audits persisted snapshot reachability as part of standard
verification and health reporting.

### Added (1.3.0)

- **`snapshot` and `snapshot_file` tables** — new schema objects (schema version 7) for immutable point-in-time captures
- **`snapshot create [--id ID] [--label LABEL] [--from PARENT_ID] [paths…]`** — full snapshot (all current files) or partial (filtered paths/prefixes); `--from` stores lineage metadata only and is currently full-to-full only
- **`snapshot list [--type full|partial] [--limit N] [--since DATE]`** — list snapshots with filtering and ordering
- **`snapshot show <snapshotID> [--limit N] [query filters…]`** — inspect snapshot contents with query support
- **`snapshot stats [snapshotID]`** — report snapshot retention pressure and metadata
- **`snapshot restore <snapshotID> [paths…] [--mode original|prefix|override] [--destination DIR]`** — restore full or partial from snapshot
- **`snapshot diff <base-ID> <target-ID> [--filter added|removed|modified] [query filters…]`** — classify changes between snapshots
- **`snapshot delete <snapshotID> --force`** — remove snapshot metadata only (logical content preserved)
- **Snapshot query semantics** — unified SnapshotQuery across show/restore/diff with exact path, prefix, glob pattern, regex, size window, and modified-time window criteria (ANDed)
- **Snapshot-aware retention model** — logical files are retained by union of current-state physical mappings and snapshot references; GC eligibility changes only after snapshot delete
- **Stats retention visibility** — global and per-snapshot stats expose retained-only-by-current, retained-only-by-snapshot, shared-by-both, and total snapshot-retained metrics
- **Verify snapshot reachability** — standard verify checks for orphan snapshot_file rows, invalid lifecycle states, and missing chunk graphs in snapshot-retained files
- **Doctor snapshot-retention context** — text and JSON reports surface snapshot-retention integrity counters alongside physical mapping counters
- **G14–G17 guarantees** — snapshot-retained GC safety, snapshot deletion metadata-only semantics, stats retention visibility, and verify/doctor snapshot reachability audits

### Scope alignment (v1.3)

- Snapshot command surface is in scope: `snapshot create`, `snapshot restore`,
  `snapshot list`, `snapshot show`, `snapshot stats`, `snapshot diff`,
  `snapshot delete --force`.
- Snapshot query semantics are part of the contract surface across
  `snapshot show`, `snapshot restore`, and `snapshot diff`:
  exact path, prefix, glob pattern, regex, size window, and modified-time
  window criteria with AND semantics.
- `snapshot diff` filtering semantics are explicit and stable:
  `--filter added|removed|modified` reduces returned entries and summary counts
  to the selected classification.
- Snapshot delete semantics are explicit and stable:
  delete removes snapshot metadata (`snapshot` + `snapshot_file`) only;
  underlying logical files/blocks are not directly removed by snapshot delete.

### Snapshot-retention / GC contract alignment

- Snapshot-retained content remains GC-ineligible until the retaining snapshot
  is deleted.
- Removing current-state mappings alone does not make snapshot-retained content
  collectible.
- GC eligibility transition is expected only after snapshot delete.

### Validation and release-gate alignment

- [VALIDATION_MATRIX.md](VALIDATION_MATRIX.md) is expected to list and cover G14-G17 for v1.3.
- v1.3 release gating explicitly includes:
  - test surface coverage (package/integration/adversarial/smoke)
  - documentation/release checklist consistency ([README.md](README.md) + [VALIDATION_MATRIX.md](VALIDATION_MATRIX.md))
  - manual snapshot/retention lifecycle gate in [PRE_RELEASE_CHECKLIST.md](PRE_RELEASE_CHECKLIST.md)
- Manual snapshot lifecycle gate examples are aligned to CLI contracts:
  use `snapshot create --id <snapshotID>`, use positional snapshot IDs for
  `snapshot restore`/`snapshot diff`/`snapshot delete`, and use
  `remove --stored-path` for current-state mapping removal.

### Post-v1.3 hardening backlog (non-blocking)

- Add fuzz coverage for snapshot query combinator cases (`regex` + `pattern` +
  `prefix`) as a future hardening task.

------------------------------------------------------------------------

## [1.2.0] - 2026-04-11

Physical-file layer and operator semantics milestone.

v1.0 established storage correctness.
v1.1 established interface correctness.
v1.2 establishes physical-graph coherence: the system now knows and audits
*where* each logical file lives, can repair drifted reference counts explicitly,
and refuses GC when the physical root graph is inconsistent.

### Added (1.2.0)

- **`physical_file` table** — new schema object (schema version 6) that maps each
  current-state filesystem path to its owning `logical_file`. Managed by the
  `physical_file_repository` layer.
- **`remove --stored-path <path>`** — removes one physical-path mapping and
  decrements `logical_file.ref_count`. Emits `remaining_ref_count` in JSON output.
- **`remove --stored-paths <path> [path …]`** — batch stored-path remove with
  the same deterministic batch contract as ID-based batch remove (input-order
  preservation, deduplication, fail-fast, dry-run support absent for
  stored-path per documented deferral).
- **`restore --stored-path <path>`** — restore a file by its stored path, with
  optional `--mode` (`original` / `prefix` / `override`) and `--destination`.
- **`repair ref-counts [--output json]`** — explicit operator command that
  recomputes `logical_file.ref_count` from `physical_file` rows. Reports
  `updated_logical_files` and `scanned_logical_files`.
- **`repair --batch [--input <file>] [--fail-fast]`** — batch maintenance layer
  for `repair` operations (currently `ref-counts`), following the same batch
  contract as `restore` and `remove`.
- **Physical graph audit in `verify system`** — standard verify now checks:
  - orphan `physical_file` rows whose `logical_file_id` points nowhere
  - `logical_file.ref_count` drift relative to `COUNT(physical_file rows)`
  - impossible negative `logical_file.ref_count` states
  - Failures carry machine-readable `invariant_code` and `recommended_action`.
- **Audited GC root gate** — `gc` (including dry-run) runs
  `CheckPhysicalFileGraphIntegrity` as a mandatory pre-flight after acquiring the
  advisory lock. A drifted root graph refuses GC with `GC_REFUSED_INTEGRITY`.
- **Typed invariant taxonomy** (`internal/invariants`) — stable, machine-readable
  error codes for physical graph failures: `PHYSICAL_GRAPH_ORPHAN`,
  `PHYSICAL_GRAPH_REFCOUNT_MISMATCH`, `PHYSICAL_GRAPH_NEGATIVE_REFCOUNT`,
  `GC_REFUSED_INTEGRITY`, `REPAIR_REFUSED_ORPHAN_ROWS`.
- **`invariant_code` + `recommended_action` in JSON error payloads** — all
  physical-graph verify failures and GC/repair refusals now include advisory
  metadata in both JSON (`invariant_code`, `recommended_action`) and text output.
- **`stored_path` field in store JSON output** — `coldkeep store --output json`
  now includes `stored_path` alongside `file_id` and `path`.
- **`docs/PATH_IDENTITY.md`** — canonical reference for path identity policy
  (canonicalization, case-sensitivity, symlink behavior) in the v1.2
  `physical_file` layer.
- **G10–G13 guarantees** — post-v1.0 extensions tracked in `VALIDATION_MATRIX.md`:
  - G10: physical graph audit (orphan/ref-count coherence)
  - G11: audited GC root gate (pre-flight on drifted graph)
  - G12: stable invariant error taxonomy (machine-readable codes)
  - G13: batch maintenance reporting semantics (`execution_mode`,
    per-item `success`/`failed`/`skipped`, `recommended_action` on refusal)
- Schema migration to version 6 (`physical_file` table, index on
  `physical_file(logical_file_id)`).

### Changed (1.2.0)

- `list --output json` and `search --output json` now include `stored_path` per
  file entry when a physical mapping exists.
- `doctor` verify phase now includes the physical graph audit; a drifted graph
  causes `doctor` to exit with `PHYSICAL_GRAPH_REFCOUNT_MISMATCH`.
- `repair` extended with `ref-counts` sub-command and batch wrapper; existing
  `repair --batch` tests cover mixed-input ordering and fail-fast semantics.
- Clarified batch `restore`/`remove` CLI help: JSON `status` values are
  `ok`, `partial_failure`, `error` (overall) and `success`, `failed`,
  `skipped`, `planned` (per-item). Exit `0` when no item fails, `1` when any
  item fails, `2` for pre-execution usage/validation errors.

### Fixed (1.2.0)

- `strings.TrimLeft` path separator cutset in `restore.go` corrected to handle
  both `/` and `\` reliably.
- Unused helper types in `physical_file_repository.go` removed (lint/staticcheck
  cleanliness).

### Technical debt noted (post-v1.2)

- `BuildPlan` and `ExecutePlan` are deprecated transitional helpers and are
  candidates for removal beyond v1.2 or isolation into a dedicated legacy file.
- Dry-run support for `remove --stored-path` is deferred beyond v1.2 (rationale
  documented in [ARCHITECTURE.md](ARCHITECTURE.md) under "Dry-run Support").

### Notes (1.2.0)

- `remove --stored-path` unlinks one physical mapping at a time; the logical
  file and its data remain restorable as long as any physical mapping still
  exists. Removing all mappings does not automatically trigger GC: the logical
  file row persists with `ref_count=0` until GC runs.
- `repair ref-counts` is an explicit operator command, not an automatic
  background repair. `doctor` detects drift but does not auto-repair.
- The explicit repair boundary: `verify` detects, `doctor` recovers + detects,
  `repair ref-counts` is the only write-path for fixing drift.
- On-disk format and internal structures may evolve in future versions.

### Guarantees (1.2.0)

- Introduces G10: physical graph audit (orphan/ref-count coherence in `verify system`)
- Introduces G11: audited GC root gate (pre-flight refusal on drifted graph)
- Introduces G12: stable invariant error taxonomy (machine-readable codes)
- Introduces G13: batch maintenance reporting semantics (execution_mode, per-item isolation)

------------------------------------------------------------------------

## [1.1.0] - 2026-04-07

Interface-correctness milestone (CLI + automation layer).

v1.0 established correctness of storage internals.
v1.1 establishes correctness of interaction semantics for CLI and automation.

This release introduces a unified batch execution layer for `restore` and
`remove`, focused on deterministic behavior, structured observability, and
correctness under real-world mixed-input scenarios.

### Added (1.1.0)

- Multi-target support for `restore` and `remove` commands (multiple IDs in a
  single invocation)
- Input file ingestion via `--input <file>` for batch operations
- Dry-run mode (`--dry-run`) providing full execution planning without mutation
- Structured per-item batch reporting with explicit status classification:
  `success`, `failed`, `skipped`, and `planned`
- Preservation of raw invalid inputs via `raw_value` in JSON output (no implicit
  `id=0` fallback)
- Fail-fast execution mode (`--fail-fast`) to stop processing on first execution
  failure
- Comprehensive adversarial integration test suite for batch semantics,
  including mixed input, duplicate handling, ordering guarantees, and dry-run
  parity

### Changed (1.1.0)

- Unified CLI → batch → reporting pipeline so all inputs (valid, invalid,
  duplicates) are processed through a single deterministic execution model
- Strict input-order preservation guaranteed across parsing → planning →
  execution → reporting, including invalid and duplicate entries
- Introduced explicit batch status contract:
  - `ok`: all items succeeded
  - `partial_failure`: some items failed
  - `error`: all items failed or no execution possible
- JSON output contract improved:
  - failure items expose `error`
  - non-failure items (`success`, `skipped`, `planned`) expose `message`
- All-invalid input now produces a full structured batch report instead of a
  generic CLI error
- Dry-run output aligned with real execution semantics (same ordering, same
  targets, same output paths; only status differs)
- Duplicate targets are executed once and reported as `skipped` with explicit
  reason
- Exit-code behavior aligned with batch semantics: non-zero exit when any item
  fails while still emitting full structured results

### Notes (1.1.0)

- Batch operations are best-effort by default: failures do not prevent execution
  of other valid targets unless `--fail-fast` is used
- This release establishes a deterministic and automation-friendly CLI contract
  for batch workflows, aligned with coldkeep’s correctness-first design
- In guarantee terms, this release introduces an interface-correctness layer
  (G9): deterministic orchestration, machine-readable contract stability, and
  automation-safe partial-failure behavior

### Guarantees (1.1.0)

- Introduces G9: interface correctness for batch CLI orchestration

------------------------------------------------------------------------

## [1.0.0] - 2026-04-06

First stable correctness milestone.

This release marks the transition from experimental validation to a
correctness-defined baseline. The storage model, lifecycle semantics, and CLI
contracts are considered stable within the documented trust boundary.

v1.0 consolidates the guarantees defined in v0.9 and validated in v0.10,
establishing coldkeep as a correctness-first storage engine with deterministic
restore, verifiable integrity, and safe garbage collection.

### Added (1.0.0)

- Formalized v1.0 trust model consolidating:
  - storage correctness guarantees
  - verification semantics
  - recovery behavior expectations
- Established `doctor` as the primary operator-facing health and recovery command
- Defined CLI behavior and output contracts as stable for v1.x evolution

### Changed (1.0.0)

- Promoted validation and adversarial testing results (v0.10) to baseline
  correctness guarantees
- Frozen CLI surface and operational semantics as a v1.0 contract
- Clarified system trust boundaries and non-goals for the v1.x line

### Notes (1.0.0)

- v1.0 defines a correctness and operational baseline, not long-term
  compatibility guarantees
- On-disk format and internal structures may evolve in future versions
- coldkeep remains a research-oriented project, but with a stable and validated
  correctness model for local-first usage

------------------------------------------------------------------------

## [0.10.0] - Pre-v1.0 Validation Phase

Validation and adversarial testing phase leading into v1.0.

This release focuses on validating coldkeep's correctness guarantees under stress,
failure, and adversarial lifecycle interleavings.

The core architecture, storage model, and CLI contracts are considered stable.
This phase is dedicated to actively attempting to break system invariants and
eliminate remaining correctness risks before the v1.0 milestone.

### Reuse Integrity Hardening

- Added semantic integrity validation for completed logical-file reuse, controlled by
  `COLDKEEP_REUSE_SEMANTIC_VALIDATION` (`off` / `suspicious` / `always`; default `suspicious`)
- Hardened structural reuse acceptance: completed-file reuse now requires contiguous
  file-chunk graph integrity, completed referenced chunks, valid block metadata,
  non-quarantined containers, and on-disk container file presence before
  returning `AlreadyStored=true`
- Hardened completed-chunk reuse validation: chunk reuse now enforces block-row
  cardinality and validates container presence/quarantine state plus
  block offset/size bounds against container metadata and physical file size
- Added integration regression proving semantically corrupted completed-file reuse
  is refused and rebuild/retry paths are triggered

### Atomicity & Lifecycle Safety

- Added atomic logical-file completion boundary: the final transition to
  `COMPLETED` now verifies full chunk linkage and contiguous ordering in the same
  transaction
- Hardened rollback error handling: rollback failures are now surfaced and
  escalated instead of silently ignored, with failed append paths retiring or
  quarantining unsafe containers
- Added in-process container quarantine behavior for active/just-sealed
  container failures so unsafe containers are withdrawn immediately, not only on
  next startup recovery

### Lifecycle Hardening

- Hardened startup sealing recovery: containers in `sealing=TRUE` state are now
  quarantined (not auto-sealed) when physical file size differs from DB
  `current_size`, preventing ghost-byte containers from being promoted as healthy
- Added integration regression for append-rollback ghost-byte state: startup
  recovery quarantines the sealing container and GC (dry-run + real) skips it
- Fixed SQLite compatibility in remove path tests by falling back from
  `SELECT ... FOR UPDATE` to plain `SELECT` when the dialect does not support
  row-lock syntax

### Added (0.10.0)

- Added integration coverage for non-strict startup recovery on suspicious
  orphan-container conflicts (`COLDKEEP_STRICT_RECOVERY=false`)
- Added adversarial integration coverage for restore pin + remove + GC
  interleavings
- Added integration regression for deep verification trailing-byte detection
  after the last completed block payload
- Added `VALIDATION_MATRIX.md` to map v0.9 guarantees to concrete verify checks
  and integration evidence during the v0.10 trust-validation phase
- Added lifecycle determinism integration regression
  `TestStoreRemoveGCRestartStoreConvergesChunkGraph` to assert store/remove/GC/
  restart cycles converge to a stable chunk graph and restorable output

### Changed (0.10.0)

- Clarified verification operational contract as a recovered-state checker,
  not a live online-consistency checker during in-flight writes
- Documented `VALIDATION_MATRIX.md` in README as the maintained v0.10/v1.0
  guarantee-to-evidence contract, audited locally and enforced in CI
- Tightened validation-matrix auditing so README v0.9 guarantee summary bullets
  are counted and checked against the maintained validation contract surface
- Clarified verification mode trade-offs (`standard`, `full`, `deep`) with
  explicit cost/coverage guidance
- Elevated `coldkeep doctor` in docs/help/smoke as the recommended
  operator-facing health-check and release-gate command
- Froze `doctor` default mode as a v1.0 product contract: no-flag `doctor`
  remains the fast `standard` health gate; `--full` and `--deep` are explicit
  stronger/slow-path escalations
- Documented `doctor` as a corrective health command that may update
  metadata through recovery before running verification
- Documented startup recovery strictness as intentional fail-fast behavior,
  with a non-strict override for restart-race scenarios
- Documented contiguous offset validation as an explicit current-format
  invariant (append-only contiguous layout per container)
- Froze and documented `doctor --output json` failure contract: failures emit
  only generic CLI error JSON on `stderr` (no partial doctor report payload)
- Strengthened GC emptiness invariants and stats accounting to treat chunk
  liveness as `live_ref_count OR pin_count`, preserving restore safety under
  remove/GC interleavings

### Verification & Recovery Semantics

- Standard verification now enforces pinned-chunk integrity:
  `pin_count > 0` chunks must remain `COMPLETED` and retain block metadata
- Standard verification now also enforces completed-chunk block cardinality:
  every `COMPLETED` chunk must have exactly one `blocks` row
- Deep verification now fails when container tails contain trailing
  unaccounted bytes beyond the last completed block
- PostgreSQL startup schema guard now explicitly requires
  `schema_version >= 5`; optional first-run bootstrap remains available via
  `COLDKEEP_DB_AUTO_BOOTSTRAP=true`

### v1.0 Trust Model

- Consolidated operator trust model documentation: startup recovery is the normal
  lifecycle entry point (not exceptional maintenance), `doctor` is the recommended
  health gate and is corrective (not read-only), `verify` assumes recovered state,
  strict recovery is the production baseline, and semantic reuse validation trades
  read/CPU cost for stronger inline integrity confidence
- Made `coldkeep doctor` a named first-class v1.0 command: it is explicitly
  corrective (may abort dangling writes and clear stale sealing markers before
  verifying), is the recommended pre-ingestion, post-startup, and pre-release gate,
  and its default mode (`--standard`) is a frozen v1.0 product contract

### Tests

- Added integration regression
  `TestVerifySystemDeepDetectsAESGCMTamperedCiphertext`: stores a file with
  the `aes-gcm` codec, flips on-disk ciphertext bytes, and asserts both deep
  verify and restore reject the tampered payload
- Added integration regression
  `TestVerifySystemDeepDetectsAESGCMNonceMetadataTampering`: stores a file
  with the `aes-gcm` codec, mutates `blocks.nonce` metadata in DB, and asserts
  both deep verify and restore reject the tampered authenticated context
- Added integration regression
  `TestVerifySystemDeepDetectsAESGCMWrongKeyMismatch`: stores a file with the
  `aes-gcm` codec, verifies baseline success under the original key, then
  switches to a different valid key and asserts both deep verify and restore
  reject the mismatched-key read path
- Added integration regression
  `TestVerifySystemDeepDetectsAESGCMInvalidKeyConfiguration`: stores a file
  with the `aes-gcm` codec, then sets malformed `COLDKEEP_KEY` configuration
  and asserts both deep verify and restore fail under invalid key setup
- Added integration regression
  `TestVerifySystemDeepDetectsAESGCMInvalidHexKeyConfiguration`: stores a
  file with the `aes-gcm` codec, then sets non-hex `COLDKEEP_KEY`
  configuration and asserts both deep verify and restore fail under invalid key
  encoding
- Tightened plain-codec deep verification regressions so they store with
  explicit `plain` codec selection instead of relying on the process default,
  and `TestVerifySystemDeepDetectsChunkDataCorruption` now asserts both deep
  verify aggregate failure and restore `chunk hash mismatch` semantics
- Hardened verification fixtures and nearby full-mode regressions against
  default-codec drift: `setupStoredFileForVerification`, `TestVerifyFull`, and
  `TestVerifySystemFullDetectsNonContiguousOffsets` now store with explicit
  `plain` codec selection so DB-backed runs do not depend on process defaults
- Tightened file-level verification regressions to assert returned error
  contracts instead of generic failure only, including deep chunk corruption,
  full container truncation/missing-file cases, and standard missing-metadata /
  broken-order cases
- Tightened remaining system-level verification regressions to assert returned
  error contracts, including deep container-content mismatch aggregation and
  full-mode non-contiguous offset detection
- Tightened the remaining local verify-block regressions in `TestVerifyFull`
  and `TestVerifySystemDeepDetectsTrailingBytesAfterLastBlock` so malformed
  completed chunks, missing container files, and deep trailing-byte failures
  assert returned error-contract substrings instead of generic failure only
- Tightened `TestVerifySystemDeepAggregatesChunkErrors` to assert the returned
  aggregated deep-verification error count substring instead of generic
  failure-only behavior
- Normalized adjacent schema, seal, recovery, remove, and store failure-injection
  regressions to use shared returned-error substring assertions instead of
  manual non-nil checks, and fixed `TestStoreSealingMarkerUpdateFailureAbortsSafelyAndRecovers`
  to reach the intended rotation/sealing-marker failure path under explicit
  container sizing
- Added integration regression `TestDoctorAbortsProcessingLogicalFilesFromRecoverableState`:
  injects a dangling PROCESSING logical file, runs doctor, asserts recovery aborted
  it (`aborted_logical_files >= 1`), and confirms the PROCESSING row is now ABORTED
  and subsequent verify passes
- Added integration regression
  `TestStoreSealingMarkerUpdateFailureAbortsSafelyAndRecovers`: injects a DB
  failure on `container.sealing` transition, asserts store fails and marks file
  ABORTED without lingering `sealing=TRUE` rows, then verifies clean retry,
  restore hash equality, and full verify success
- Added stress-tier seeded randomized lifecycle regression
  `TestStoreLifecycleSeededRandomizedOperationOrder`: runs deterministic-random
  operation ordering across store/verify/gc/restore/remove loops with per-step
  integrity assertions
- Added stress-tier repeated jittered interleaving regression
  `TestRepeatedJitteredStoreGCRestoreInterleaving`: runs multi-round
  store/restore/gc interleavings with deterministic randomized start offsets and
  asserts restore correctness plus post-run invariant stability
- Added stress-tier four-way repeated interleaving regression
  `TestRepeatedJitteredStoreGCRestoreRemoveInterleaving`: runs multi-round
  store/gc/restore/remove interleavings with deterministic randomized start
  offsets and asserts victim removal plus restore/invariant stability
- Added dedicated long-run randomized soak regression
  `TestRandomizedLongRunLifecycleSoak`: runs repeated store/verify/gc/restore/
  recovery/remove cycles under deterministic randomization and is included in
  the CI long-run gate alongside `TestStoreGCVerifyRestoreDeleteLoopStability`
- Added repeated doctor convergence regression
  `TestDoctorRepeatedRecoverableStateConvergesAndPreservesLiveData`: injects
  recoverable PROCESSING logical-file and chunk rows during live workload,
  runs `doctor`, and asserts corrective counters plus preserved restore/verify
  behavior for valid data across repeated rounds
- Added recovery preservation regression
  `TestStartupRecoveryQuarantinesDamagedActiveContainerAndPreservesOtherLiveData`:
  quarantines a truncated active container, proves unrelated live data remains
  restorable, and verifies new writes avoid the quarantined container
- Added ghost-byte recovery preservation regression
  `TestStartupRecoveryQuarantinesGhostByteSealingContainerAndPreservesOtherLiveData`:
  quarantines a sealing container with ghost bytes, proves unrelated live data
  remains restorable, and verifies new writes avoid the quarantined container
- Added integration assertions for GC/restore pinning under remove/GC/restore
  interleavings
- Added integration assertion that non-strict recovery continues on suspicious
  orphan conflict states instead of aborting startup
- Added command-layer test that pins doctor JSON failure behavior to generic
  CLI error payload shape (no `command`/`data` fields)
- Added integration coverage for atomic logical-file completion and contiguous
  file_chunk ordering under both single-file and concurrent multi-chunk ingestion

------------------------------------------------------------------------

## [0.9.0] - 2026-03-31

Release hardening and delivery-gate enforcement.

This version focuses on making regressions materially harder to merge or tag by
strengthening the CI gate that enforces storage correctness guarantees.

It also formalizes the v0.9 storage guarantees model so operators and automation
can reason explicitly about validity, restore safety, and recovery behavior.

### Added (0.9.0)

- Added a formal v0.9 storage guarantees definition in project documentation
- Defined explicit validity rules for restorable logical files (`COMPLETED`
  lifecycle and readable referenced blocks)
- Defined restore atomicity and durability guarantees (temp write, fsync,
  atomic rename, parent directory fsync)
- Defined non-destructive GC guarantees and trust-boundary assumptions
- Documented explicit v0.9 non-guarantees (format compatibility and
  multi-node/distributed consistency are not guaranteed pre-v1)

### Changed (0.9.0)

- Hardened GitHub Actions CI with workflow concurrency cancellation and job timeouts
- Extended CI execution to release-style tags matching `v*`
- Upgraded integration correctness runs to use the Go race detector
- Updated smoke CI runs to reset database and storage state between phases so
  the main samples run and edge-case run are isolated and deterministic
- Documented the required GitHub ruleset / branch-protection policy around the
  aggregate `CI Required Gate`
- Added a maintainer audit script to verify the local workflow gate and the
  expected GitHub repository protection policy
- Clarified and tightened user-facing behavior expectations for data integrity,
  crash recovery, concurrency, and verification semantics

### Notes (0.9.0)

- The repository now exposes a single aggregate status check, `CI Required Gate`,
  intended to be configured as the mandatory required check in GitHub.
- GitHub branch protection and tag protection remain repository settings; they
  cannot be fully enforced from source files alone.
- v0.9 guarantees are correctness-oriented and apply within the documented trust
  boundary (database/filesystem are not externally modified and fsync semantics
  are respected).

------------------------------------------------------------------------

## [0.8.0] - 2026-03-30

Simulation and CLI stabilization release.

This version focuses on making coldkeep easier to evaluate, safer to operate,
and more predictable to automate. It introduces dry-run simulation, structured
JSON output for CLI commands, richer operation result models, and stronger
verification and integration coverage.

### Added (0.8.0)

- `simulate store` and `simulate store-folder` commands
- Simulated storage backend for dry-run ingestion without writing container data
- Structured JSON output mode for CLI commands via `--output json`
- Structured result models for store, restore, remove, gc, and stats operations
- Startup recovery JSON event output
- CLI contract tests for JSON schema and exit-code classification
- Retry and fragmentation metrics in stats output
- Explicit read-only and writable container open helpers
- Additional deep verification coverage for file and system integrity
- Expanded integration test coverage for:
  - concurrent store stress
  - aborted file/chunk retry recovery
  - container rotation and sealing
  - verify standard/full/deep behavior
  - corruption and truncation detection
  - shared-chunk safety
  - startup recovery behavior

### Changed (0.8.0)

- Stabilized CLI command behavior and output handling
- Improved exit-code classification using typed CLI errors
- Refined simulation reporting to reflect realistic container usage
- Updated store path to return structured metadata for CLI and JSON responses
- Updated restore path to return structured metadata for CLI and JSON responses
- Updated remove and gc paths to return structured operation summaries
- Improved verification logic with clearer offset continuity and payload validation
- Replaced ambiguous container open boolean usage with explicit helper functions
- Improved retry handling around already-existing block metadata during chunk store
- Improved search filter validation at CLI level for numeric size arguments

### Fixed (0.8.0)

- Fixed restore path to use explicit read-only container access
- Fixed several CLI usage/error paths to classify correctly as usage failures
- Fixed simulation command argument handling and error reporting
- Fixed deep verification consistency around transformer reuse and offset checks
- Fixed container counting in simulation output to better match completed stored data
- Fixed fragile retry behavior that previously depended on string-matching some storage errors

### Notes (0.8.0)

- `simulate` reuses the real chunking, block encoding, and metadata pipeline, but
  does not persist container payloads to physical storage.
- coldkeep remains an experimental project and is not production ready.
- On-disk format and CLI details may continue to evolve before v1.0, but v0.8
  establishes the intended CLI contract and evaluation workflow.

------------------------------------------------------------------------

## [0.7.0] - 2026-03-28

Block Abstraction & Encryption Foundations.

This release introduces a major evolution of the storage engine by decoupling
logical data (chunks) from physical storage (blocks), and adding a pluggable
encoding layer with support for encryption.

### Added (0.7.0)

- Block abstraction layer separating logical chunks from physical storage
- New `blocks` table storing codec, offsets, sizes, and encryption metadata
- Pluggable block transformer interface for encoding/decoding
- AES-GCM encryption support (`aes-gcm` codec)
- Per-block random nonce storage for encryption
- CLI support for codec selection (`--codec`)
- Environment-based configuration (`COLDKEEP_CODEC`, `COLDKEEP_KEY`)
- `init` command to generate encryption keys and bootstrap local setup
- Dual-mode CI testing (plain + encrypted storage paths)
- Improved CLI structure and help output

### Changed (0.7.0)

- Storage engine now writes encoded blocks instead of raw chunk records
- Restore pipeline now decodes blocks before reconstructing files
- Verification system operates on decoded block payloads
- Stats now report physical storage using block sizes (`stored_size`)
- Garbage collection operates on blocks and uses `live_ref_count OR pin_count` as deletion invariant
- CLI command handling refactored for extensibility and clarity

### Removed (0.7.0)

- Legacy chunk physical fields (`chunk.container_id`, `chunk.chunk_offset`)
- Chunk record header format (`ChunkRecordHeaderSize` and related logic)
- Direct chunk-to-container storage model

### Security (0.7.0)

- Data at rest can now be encrypted using AES-GCM
- Encryption keys are externalized via environment variables (not stored in DB or repo)
- `.env` files are created with restricted permissions (0600)
- Fail-fast behavior when encryption is requested but no key is provided

### Notes (0.7.0)

- This is a foundational release for future features such as key rotation,
  multi-key support, and advanced encoding strategies.
- Existing data stored with the `plain` codec remains fully compatible.
- Coldkeep remains an experimental research project and is not production ready.
- On-disk formats may continue to evolve before v1.0.

------------------------------------------------------------------------

## [0.6.0] - 2026-03-22

Storage model evolution and container API redesign.

### Added (0.6.0)

- New container abstraction with Append / ReadAt / Sync / Close API
- Container sealing with full-file hash verification
- Multi-layer verification system (standard, full, deep)
- Stress tests for concurrency, retries, and rotation

### Changed (0.6.0)

- Refactored storage pipeline to use container interface
- Simplified container header format
- Improved concurrency handling with row-level locking and retry logic
- Container rotation behavior under concurrent workloads

### Removed (0.6.0)

- Whole-container compression flag
- Whole-container encryption flag

### Notes (0.6.0)

- On-disk format is still evolving and may change before v1.0
- Container size limit is enforced on a best-effort basis under concurrency

------------------------------------------------------------------------

## [0.5.0] - 2026-03-21

Deterministic restore guarantees for stored files and dataset-level workflows.

### Added (0.5.0)

- End-to-end integration tests using real fixture datasets:
  - `samples`
  - `samples_edge_cases`

- Full workflow validation for fixture datasets:
  - store folder
  - verify system (full)
  - GC (dry run + real run)
  - restart / recovery
  - restore all stored logical files
  - hash comparison against original inputs

### Scope (0.5.0)

- This release validates deterministic restore at the logical-file level:
  - stable stored logical files under deduplication
  - byte-identical restore outputs verified by SHA-256
  - deterministic behavior across GC and restart

- It does **not** yet define a first-class “restore folder tree layout exactly”
  contract, as current tests restore logical files individually.

### Notes (0.5.0)

- Whole-container compression remains readable for backward compatibility,
  but is no longer used for new writes.
- Compression removal and storage-format cleanup are deferred to a later release.
- Some scenario tests overlap between generated datasets and fixture datasets;
  both are currently retained to maximize confidence.

------------------------------------------------------------------------

## [0.4.0] - 2026-03-19

Integrity Verification Layer

This release introduces a complete integrity verification system for coldkeep,
covering metadata consistency, container structure validation, and full
end-to-end data integrity checks.

The system is designed in three verification levels:

- Standard: metadata integrity checks
- Full: metadata + container structure and hash validation
- Deep: full physical verification by reading container data and recomputing chunk hashes

### Added (0.4.0)

- `verify system` command with three verification levels (standard, full, deep)
- `verify file <id>` command with per-file verification (standard, full, deep)
- Deep verification logic that reads container data and validates chunk hashes
- Record-level validation (header hash + stored size + data hash)
- Container-wide integrity verification across all sealed containers
- Comprehensive integration tests for verification (positive and corruption scenarios)

### Improved (0.4.0)

- Verification coverage across file, chunk, and container layers
- Error reporting with aggregated verification failures
- Internal consistency checks for chunk offsets, sizes, and container bounds

### Notes (0.4.0)

- Deep verification performs full disk reads and may be slow on large datasets
- Whole-container compression is still present but will be removed in a future release in favor of block-level compression

coldkeep remains an experimental research project and is not production ready.
The on-disk format may change before v1.0.

------------------------------------------------------------------------

## [0.3.0] - 2026-03-15

Safe garbage collection foundation.

### Added (0.3.0)

- Repository verification command (`coldkeep verify`)
- Verification levels: standard, full, deep
- Reference count validation
- Container integrity verification
- Chunk offset validation
- Deep data verification (hash validation)

### Improved (0.3.0)

- Garbage collection safety via transactional re-checks
- Advisory lock preventing concurrent GC runs
- `gc --dry-run` simulation mode

### Testing (0.3.0)

- Integration tests for GC safety
- Verification corruption detection tests

------------------------------------------------------------------------

## [0.2.0]- 2026-03-11

Crash-consistency foundation for the storage engine

### Added (0.2.0)

- Logical file lifecycle management
- Chunk lifecycle management
- Retry handling for interrupted operations
- Startup recovery system
- Container quarantine mechanism
- Extended storage statistics
- Smoke test improvements
- Durable container writes with fsync to guarantee on-disk persistence

### Improved (0.2.0)

- Concurrent file ingestion
- Garbage collection safety
- Operational observability

### Notes (0.2.0)

This version introduces the core reliability model for the storage
engine.

The on-disk format and APIs may still change in future releases.

### Known Limitations (0.2.0)

- Basic crash recovery exists, but full end-to-end crash consistency across
  filesystem and database layers is still evolving.
- No encryption at rest or in transit.
- No authentication or authorization model.
- Whole-container compression is not suitable for efficient random-access
  restores.
- Concurrency behavior has not been heavily stress-tested under high parallel
  workloads.
- No background integrity verification or automatic container scrubbing.
- On-disk storage format may change before v1.0.

------------------------------------------------------------------------

## [0.1.0] - 2026-02-24

Initial public research prototype (POC).

### Added (0.1.0)

- Content-addressed chunking using SHA-256
- File-level SHA-256 deduplication guard
- Chunk reference counting
- Container packing on disk with deterministic append logic
- PostgreSQL-backed metadata schema
- CLI commands:
  - `store`
  - `store-folder`
  - `restore`
  - `remove`
  - `gc`
  - `stats`
  - `list`
- Docker Compose setup (Postgres + app)
- Integration test scaffolding (environment-gated)
- Basic CI pipeline (build, vet, tests)
- Open-source project files:
  - LICENSE (Apache-2.0)
  - SECURITY.md
  - CONTRIBUTING.md
  - CODE_OF_CONDUCT.md
  - README.md

### Design Characteristics (0.1.0)

- Per-file transactional metadata
- `SELECT ... FOR UPDATE SKIP LOCKED` container selection
- Deterministic chunk ordering for restore correctness
- Chunk-level and full-file integrity verification on restore

### Known Limitations (0.1.0)

- Not crash-consistent; filesystem and database state may diverge on
    failure.
- No encryption at rest or in transit.
- No authentication or authorization model.
- Whole-container compression is not suitable for efficient
    random-access restores.
- Concurrency guarantees are minimal and not heavily stress-tested.
- No background integrity verification process.

------------------------------------------------------------------------

Future versions may introduce structural changes to container layout,
metadata integrity, or security properties. Backward compatibility
guarantees are not defined for the prototype stage.
