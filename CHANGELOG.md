# Changelog

All notable changes to this project will be documented in this file.

This project follows a lightweight, prototype-friendly versioning
approach.

Version numbers indicate conceptual milestones rather than
production stability.

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

-   Logical file lifecycle management
-   Chunk lifecycle management
-   Retry handling for interrupted operations
-   Startup recovery system
-   Container quarantine mechanism
-   Extended storage statistics
-   Smoke test improvements
-   Durable container writes with fsync to guarantee on-disk persistence

### Improved

-   Concurrent file ingestion
-   Garbage collection safety
-   Operational observability

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

-   Content-addressed chunking using SHA-256
-   File-level SHA-256 deduplication guard
-   Chunk reference counting
-   Container packing on disk with deterministic append logic
-   PostgreSQL-backed metadata schema
-   CLI commands:
    -   `store`
    -   `store-folder`
    -   `restore`
    -   `remove`
    -   `gc`
    -   `stats`
    -   `list`
-   Docker Compose setup (Postgres + app)
-   Integration test scaffolding (environment-gated)
-   Basic CI pipeline (build, vet, tests)
-   Open-source project files:
    -   LICENSE (Apache-2.0)
    -   SECURITY.md
    -   CONTRIBUTING.md
    -   CODE_OF_CONDUCT.md
    -   README.md

### Design Characteristics

-   Per-file transactional metadata
-   `SELECT ... FOR UPDATE SKIP LOCKED` container selection
-   Deterministic chunk ordering for restore correctness
-   Chunk-level and full-file integrity verification on restore

### Known Limitations

-   Not crash-consistent; filesystem and database state may diverge on
    failure.
-   No encryption at rest or in transit.
-   No authentication or authorization model.
-   Whole-container compression is not suitable for efficient
    random-access restores.
-   Concurrency guarantees are minimal and not heavily stress-tested.
-   No background integrity verification process.

------------------------------------------------------------------------

Future versions may introduce structural changes to container layout,
metadata integrity, or security properties. Backward compatibility
guarantees are not defined for the prototype stage.