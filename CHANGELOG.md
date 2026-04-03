# Changelog

All notable changes to this project will be documented in this file.

This project follows a lightweight, prototype-friendly versioning
approach.

Version numbers indicate conceptual milestones rather than
production stability.

------------------------------------------------------------------------

## [0.10.0] - Unreleased

Validation and adversarial testing phase before v1.0.

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

### Added

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

### Changed

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

### Added
- Added a formal v0.9 storage guarantees definition in project documentation
- Defined explicit validity rules for restorable logical files (`COMPLETED`
  lifecycle and readable referenced blocks)
- Defined restore atomicity and durability guarantees (temp write, fsync,
  atomic rename, parent directory fsync)
- Defined non-destructive GC guarantees and trust-boundary assumptions
- Documented explicit v0.9 non-guarantees (format compatibility and
  multi-node/distributed consistency are not guaranteed pre-v1)

### Changed
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

### Notes
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

### Added
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

### Changed
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

### Fixed
- Fixed restore path to use explicit read-only container access
- Fixed several CLI usage/error paths to classify correctly as usage failures
- Fixed simulation command argument handling and error reporting
- Fixed deep verification consistency around transformer reuse and offset checks
- Fixed container counting in simulation output to better match completed stored data
- Fixed fragile retry behavior that previously depended on string-matching some storage errors

### Notes
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

### Added

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

### Changed

- Storage engine now writes encoded blocks instead of raw chunk records
- Restore pipeline now decodes blocks before reconstructing files
- Verification system operates on decoded block payloads
- Stats now report physical storage using block sizes (`stored_size`)
- Garbage collection operates on blocks and uses `live_ref_count OR pin_count` as deletion invariant
- CLI command handling refactored for extensibility and clarity

### Removed

- Legacy chunk physical fields (`chunk.container_id`, `chunk.chunk_offset`)
- Chunk record header format (`ChunkRecordHeaderSize` and related logic)
- Direct chunk-to-container storage model

### Security

- Data at rest can now be encrypted using AES-GCM
- Encryption keys are externalized via environment variables (not stored in DB or repo)
- `.env` files are created with restricted permissions (0600)
- Fail-fast behavior when encryption is requested but no key is provided

### Notes

- This is a foundational release for future features such as key rotation,
  multi-key support, and advanced encoding strategies.
- Existing data stored with the `plain` codec remains fully compatible.
- Coldkeep remains an experimental research project and is not production ready.
- On-disk formats may continue to evolve before v1.0.

------------------------------------------------------------------------

## [0.6.0] - 2026-03-22

Storage model evolution and container API redesign.

### Added

* New container abstraction with Append / ReadAt / Sync / Close API
* Container sealing with full-file hash verification
* Multi-layer verification system (standard, full, deep)
* Stress tests for concurrency, retries, and rotation

### Changed

* Refactored storage pipeline to use container interface
* Simplified container header format
* Improved concurrency handling with row-level locking and retry logic
* Container rotation behavior under concurrent workloads

### Removed

* Whole-container compression flag
* Whole-container encryption flag

### Notes

* On-disk format is still evolving and may change before v1.0
* Container size limit is enforced on a best-effort basis under concurrency

------------------------------------------------------------------------

## [0.5.0] - 2026-03-21

Deterministic restore guarantees for stored files and dataset-level workflows.

### Added

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

### Scope

- This release validates deterministic restore at the logical-file level:
  - stable stored logical files under deduplication
  - byte-identical restore outputs verified by SHA-256
  - deterministic behavior across GC and restart

- It does **not** yet define a first-class “restore folder tree layout exactly”
  contract, as current tests restore logical files individually.

### Notes

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

### Added

- `verify system` command with three verification levels (standard, full, deep)
- `verify file <id>` command with per-file verification (standard, full, deep)
- Deep verification logic that reads container data and validates chunk hashes
- Record-level validation (header hash + stored size + data hash)
- Container-wide integrity verification across all sealed containers
- Comprehensive integration tests for verification (positive and corruption scenarios)

### Improved

- Verification coverage across file, chunk, and container layers
- Error reporting with aggregated verification failures
- Internal consistency checks for chunk offsets, sizes, and container bounds

### Notes

- Deep verification performs full disk reads and may be slow on large datasets
- Whole-container compression is still present but will be removed in a future release in favor of block-level compression

coldkeep remains an experimental research project and is not production ready.
The on-disk format may change before v1.0.

------------------------------------------------------------------------

## [0.3.0] - 2026-03-15

Safe garbage collection foundation.

### Added

- Repository verification command (`coldkeep verify`)
- Verification levels: standard, full, deep
- Reference count validation
- Container integrity verification
- Chunk offset validation
- Deep data verification (hash validation)

### Improved

- Garbage collection safety via transactional re-checks
- Advisory lock preventing concurrent GC runs
- `gc --dry-run` simulation mode

### Testing

- Integration tests for GC safety
- Verification corruption detection tests

------------------------------------------------------------------------

## [0.2.0]- 2026-03-11

Crash-consistency foundation for the storage engine

### Added

- Logical file lifecycle management
- Chunk lifecycle management
- Retry handling for interrupted operations
- Startup recovery system
- Container quarantine mechanism
- Extended storage statistics
- Smoke test improvements
- Durable container writes with fsync to guarantee on-disk persistence

### Improved

- Concurrent file ingestion
- Garbage collection safety
- Operational observability

### Notes

This version introduces the core reliability model for the storage
engine.

The on-disk format and APIs may still change in future releases.

### Known Limitations

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

### Added

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

### Design Characteristics

- Per-file transactional metadata
- `SELECT ... FOR UPDATE SKIP LOCKED` container selection
- Deterministic chunk ordering for restore correctness
- Chunk-level and full-file integrity verification on restore

### Known Limitations

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
