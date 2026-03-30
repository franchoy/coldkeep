# Changelog

All notable changes to this project will be documented in this file.

This project follows a lightweight, prototype-friendly versioning
approach.

Version numbers indicate conceptual milestones rather than
production stability.

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
- Garbage collection operates on blocks and uses `ref_count` as deletion invariant
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