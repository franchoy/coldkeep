# Changelog

All notable changes to this project will be documented in this file.

This project follows a lightweight, prototype-friendly approach to versioning.

## [0.1.0] - 2026-02-24

Initial public research prototype (POC).

### Added
- Content-addressed chunking with SHA-256 deduplication
- Container packing on disk with Postgres metadata
- CLI commands: store, store-folder, restore, remove, gc, stats
- Docker Compose setup for Postgres + app
- Basic CI (build, vet, tests)
- Open-source project files: LICENSE, SECURITY.md, CONTRIBUTING.md, CODE_OF_CONDUCT.md

### Notes
- Not crash-consistent; filesystem and DB state may diverge on failures.
- Whole-container compression is not suitable for random access restores.
