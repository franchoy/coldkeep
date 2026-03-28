# coldkeep

![CI](https://github.com/franchoy/coldkeep/actions/workflows/ci.yml/badge.svg)
![Go Version](https://img.shields.io/badge/go-1.23+-blue)
![License](https://img.shields.io/badge/license-Apache%202.0-blue)
![Status](https://img.shields.io/badge/status-research%20experimental-orange)
![Release](https://img.shields.io/github/v/release/franchoy/coldkeep?include_prereleases)

> **Status:** Experimental research project.\
> **Not production-ready. Do not use for real or sensitive data.**  
> On-disk format and APIs may change before v1.0.

coldkeep is a content-addressed storage engine for cold data.

It splits files into content-defined chunks, deduplicates them using SHA-256,
and stores them in append-only container files with database-backed metadata.

coldkeep guarantees deterministic, byte-identical restore of stored data,
validated by end-to-end hashing and resilient across garbage collection
and restart/recovery.

The system prioritizes correctness, determinism, and recoverability
over performance or feature completeness.

------------------------------------------------------------------------

# What it does (today)

- Store files or folders by splitting them into content-defined chunks.
- Deduplicate chunks using SHA-256 (content-addressed storage).
- Pack chunks into container files up to a configurable maximum size.
- Restore files by reconstructing them from stored chunks.
- Guarantee byte-identical restore outputs (verified by SHA-256).
- Use an append-only container model for deterministic and safe writes.
- Remove logical files (decrementing chunk reference counts).
- Run garbage collection to remove unreferenced chunks safely.
- Recover from interrupted operations on startup.
- Provide storage statistics and container health information.
- Perform multi-level integrity verification (metadata, container structure, and full data integrity).

------------------------------------------------------------------------

## Verification

coldkeep provides a multi-level integrity verification system to ensure
consistency and detect corruption across metadata and stored data.

### Levels

- **Standard**
  - Validates metadata integrity
  - Checks reference counts, chunk ordering, and orphan records

- **Full**
  - Includes all standard checks
  - Verifies container files exist and match recorded sizes
  - Validates container hashes and chunk-to-container consistency

- **Deep**
  - Includes all full checks
  - Reads container data and recomputes chunk hashes
  - Detects physical data corruption at the byte level

### Usage

Verify the entire system:

```bash
coldkeep verify system --level standard
coldkeep verify system --level full
coldkeep verify system --level deep
```

Verify a specific file:

```bash
coldkeep verify file <file_id> --level standard
coldkeep verify file <file_id> --level full
coldkeep verify file <file_id> --level deep
```

### Notes

Deep verification performs full reads of container files and may be slow.  
Recommended for periodic integrity audits rather than frequent execution.

------------------------------------------------------------------------

# Deterministic restore guarantees (v0.5)

Coldkeep v0.5 guarantees deterministic restore at the logical file level.

Current integration coverage validates:

- deterministic stored logical files under deduplication
- byte-identical restore outputs verified by SHA-256
- consistent restore behavior after GC and restart/recovery
- deterministic results across representative and edge-case datasets

This guarantee applies to logical files and dataset-level workflows.

Coldkeep does not yet define a first-class contract for restoring full
directory trees with exact original layout. Current tests restore logical
files individually rather than reconstructing folder structure.

------------------------------------------------------------------------

# Design sketch

Core tables:

- **logical_file**  
  User-visible file entry (name, size, file_hash).

- **chunk**  
  Logical identity of a content-addressed chunk (chunk_hash, size, ref_count).

- **blocks**  
  Physical placement and codec metadata for each chunk (codec, block_offset, stored_size, container_id).

- **file_chunk**  
  Ordered mapping between logical files and chunks.

- **container**  
  Physical container file storing raw block payloads.

Containers are stored on disk under:

storage/containers/

Containers follow an append-only write model.

Storage pipeline:

    logical_file -> file_chunk -> chunk -> blocks -> container

Lifecycle states:

- logical_file: PROCESSING → COMPLETED → ABORTED
- chunk: PROCESSING → COMPLETED → ABORTED

These states allow coldkeep to detect interrupted operations and
recover safely on startup.

------------------------------------------------------------------------

# Project structure

    coldkeep/
    │
    ├─ cmd/
    │   └─ coldkeep/          # CLI entrypoint
    │
    ├─ internal/
    │   ├─ chunk/             # chunking logic
    │   ├─ container/         # container format + management
    │   ├─ db/                # database helpers
    │   ├─ listing/           # file listing operations
    │   ├─ maintenance/       # gc, stats, verify_command
    │   ├─ recovery/          # system recovery logic
    │   ├─ storage/           # store / restore / remove pipeline
    │   ├─ utils_env/         # env helpers
    │   ├─ utils_print/       # print helpers
    │   └─ verify/            # verification logic
    │
    ├─ tests/                 # integration tests
    ├─ scripts/               # smoke / development scripts
    ├─ db/                    # database schema
    │
    ├─ docker-compose.yml
    ├─ go.mod
    └─ README.md

------------------------------------------------------------------------

# Quickstart

A small `samples/` folder is included for testing.

------------------------------------------------------------------------

# 🐳 Local development (With Docker)

Start services:

``` bash
docker compose up -d --build
```

Store a sample file:

``` bash
docker compose run --rm -v "$PWD/samples:/samples" app store /samples/hello.txt
```

Store the sample folder:

``` bash
docker compose run --rm -v "$PWD/samples:/samples" app store-folder /samples
```

List stored files:

``` bash
docker compose run --rm app list
```

Restore a file:

``` bash
docker compose run --rm app restore 1 _out.bin
```

Show stats:

``` bash
docker compose run --rm app stats
```

Run garbage collection:

``` bash
docker compose run --rm app gc
```

------------------------------------------------------------------------

# 💻 Local development (without Docker)

Start Postgres (example):

``` bash
docker compose up -d postgres
```

Build the CLI:

``` bash
go build -o coldkeep ./cmd/coldkeep
```

Store the sample folder:

``` bash
./coldkeep store-folder samples
```

List stored files:

``` bash
./coldkeep list
```

Restore a file:

``` bash
./coldkeep restore 1 restored.bin
```

Show stats:

``` bash
./coldkeep stats
```

Run GC:

``` bash
./coldkeep gc
```

------------------------------------------------------------------------

# Configuration

Database configuration is read from environment variables\
(see `docker-compose.yml` for defaults).

Storage is written to:

./storage/

During development you can safely delete this directory.

Additional environment variables used in development:

- COLDKEEP_STORAGE_DIR
- COLDKEEP_SAMPLES_DIR

------------------------------------------------------------------------

# Known limitations

## Crash recovery

coldkeep includes a crash recovery model based on lifecycle states.

Operations use explicit states (`PROCESSING`, `COMPLETED`, `ABORTED`)
to detect and handle interrupted operations safely.

On startup the system:

- marks stale `PROCESSING` rows as `ABORTED`
- prevents incomplete chunks from being reused
- allows safe retries of interrupted operations

This model ensures that partially written data does not corrupt
the logical state of the system.

In v0.6, the append-only container model further simplifies recovery
by eliminating in-place mutations of container data.

However, the system is still experimental and full transactional
guarantees across filesystem and database layers are not yet complete.

Use only with **disposable test data**.


## Container compression

Whole-container compression has been removed in v0.6.

Future versions may introduce block-level compression.

## Concurrency & integrity

Concurrency support has been significantly improved in v0.6,
including locking and retry mechanisms.

However, it is still evolving and not yet optimized for
extreme parallel workloads.

------------------------------------------------------------------------

# Security

See `SECURITY.md`.

This is an experimental research project 

Do not use for real or sensitive data

------------------------------------------------------------------------

# Development

## Build

go build -o coldkeep ./cmd/coldkeep

## Tests

Run all tests:

go test ./...

Integration tests live under:

tests/

and require a running PostgreSQL instance.

------------------------------------------------------------------------

## Smoke test

`scripts/smoke.sh` runs a full end-to-end workflow : using the `samples/` directory.

store → stats → list → restore → dedup check.

### Local

``` bash
docker compose up -d postgres
go build -o coldkeep ./cmd/coldkeep
bash scripts/smoke.sh
```

### Docker

``` bash
docker compose up -d postgres

docker compose run --rm \
  -e COLDKEEP_SAMPLES_DIR=/samples \
  -e COLDKEEP_STORAGE_DIR=/tmp/coldkeep-storage \
  -v "$PWD/samples:/samples" \
  --entrypoint bash \
  app scripts/smoke.sh
```

------------------------------------------------------------------------

## Roadmap

Coldkeep follows a risk-reduction approach, where each release removes a
class of failure until the system becomes fully trustworthy.

- **v0.2 — Crash Consistency Foundation**  
  Eliminate DB ↔ filesystem divergence and ensure safe recovery after crashes.

- **v0.3 — Safe Garbage Collection**  
  Guarantee that GC cannot remove referenced data.

- **v0.4 — Integrity & Verification Layer**  
  Enable full-system verification and corruption detection.

- **v0.5 — Deterministic Restore Guarantees**  
  Ensure byte-identical, reproducible restore across GC and restart.

- **v0.6 — Storage Model Evolution**  
  Introduce an append-only container model, remove legacy compression,
  and improve concurrency coordination as a foundation for future
  block abstraction and encryption.

- **v0.7 — Block Abstraction & Encryption Foundations**  
  Introduce block-level structure to enable partial reads, compression,
  and encryption in a controlled and extensible way.

- **v0.8 — Simulation & CLI Stabilization**  
  Add simulation capabilities for storage planning and define a stable
  command-line interface.

- **v0.9 — Internal Hardening**  
  Improve reliability, simplify internals, and finalize implementation details.

- **v1.0 — Storage Engine Stable**  
  Coldkeep becomes a trustworthy storage engine for real cold backups.

------------------------------------------------------------------------

# Contributing

Contributions and discussion are welcome.

See `CONTRIBUTING.md`.

------------------------------------------------------------------------

# License

Apache-2.0. See `LICENSE`
