# Changelog

All notable changes to this project will be documented in this file.

This project follows a lightweight, research-friendly versioning
approach. Version numbers indicate conceptual milestones rather than
production stability.

------------------------------------------------------------------------

## \[0.1.0\] - 2026-02-24

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