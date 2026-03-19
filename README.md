# coldkeep

![CI](https://github.com/franchoy/coldkeep/actions/workflows/ci.yml/badge.svg)
![Go Version](https://img.shields.io/badge/go-1.23+-blue)
![License](https://img.shields.io/badge/license-Apache%202.0-blue)
![Status](https://img.shields.io/badge/status-research%20experimental-orange)
![Release](https://img.shields.io/github/v/release/franchoy/coldkeep?include_prereleases)



> **Status:** Experimental research projec.\
> **Not production-ready. Do not use for real or sensitive data.**

coldkeep is an experimental **local-first content-addressed file storage engine with verifiable integrity**
written in Go.

Files are split into **content-addressed chunks**, packed into
**container files on disk**, and tracked through **PostgreSQL
metadata**.

This repository exists primarily for **learning, experimentation, and
design exploration** --- not for operational backup or production
storage.

------------------------------------------------------------------------

# What it does (today)

-   Store a file (or folder) by splitting it into chunks.
-   Deduplicate chunks using SHA-256.
-   Pack chunks into container files up to a maximum size.
-   Restore a file by reconstructing it from stored chunks.
-   Remove logical files (decrements chunk reference counts).
-   Run garbage collection to remove unreferenced chunks.
-   Recover safely from interrupted operations on startup.
-   Display storage statistics and container health information.
-   Multi-level integrity verification (metadata, container structure, and full data integrity)

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

Verify an specific file

```bash
coldkeep verify file <file_id> --level standard
coldkeep verify file <file_id> --level full
coldkeep verify file <file_id> --level deep
```

### Notes

Deep verification performs full reads of container files and may be slow

Recommended for periodic integrity audits rather than frequent execution

------------------------------------------------------------------------

# Design sketch

Core tables:

-   **logical_file**\
    User-visible file entry (name, size, file_hash).

-   **chunk**\
    Content-addressed chunk (chunk_hash, size, ref_count, container_id,
    offset).

-   **file_chunk**\
    Ordered mapping between logical files and chunks.

-   **container**\
    Physical container file storing chunk data.

Containers are stored on disk under:

    storage/containers/

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
    │   ├─ chunk/               # chunking and compression logic
    │   ├─ container/           # container format + container management
    │   ├─ db/                  # database connection helpers
    │   ├─ listing/             # file listing operations
    │   ├─ maintenance/         # gc, stats, and verify_command
    │   ├─ recovery             # system recovery logic
    │   ├─ storage/             # store / restore / remove pipeline
    │   ├─ utils_compresion/    # small compresion helper utilities
    │   ├─ utils_env/           # small env helper utilities
    │   ├─ utils_print/         # small print helper utilities
    │   └─ verify/              # verify logc for system or file
    │
    ├─ tests/                 # integration tests
    ├─ scripts/               # smoke / development scripts
    ├─ db/                    # database schema
    │
    ├─ docker-compose.yml
    ├─ go.mod
    └─ README.md

`internal/` packages implement the storage engine.\
`cmd/` contains the CLI entrypoint.

------------------------------------------------------------------------

# Quickstart

A small `samples/` folder is included for testing.

------------------------------------------------------------------------

# 🐳 With Docker

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

- `COLDKEEP_STORAGE_DIR`
- `COLDKEEP_SAMPLES_DIR`

------------------------------------------------------------------------

# Known limitations

## Crash recovery

coldkeep now includes a basic crash recovery model.

Operations use lifecycle states (`PROCESSING`, `COMPLETED`, `ABORTED`)
to detect interrupted operations.

On startup the system:

- marks stale `PROCESSING` rows as `ABORTED`
- prevents incomplete chunks from being reused
- allows safe retries of interrupted operations

However, the system is still experimental and full transactional
guarantees across filesystem and database layers are not yet complete.

Use only with **disposable test data**.

------------------------------------------------------------------------

## Container compression

Container-level compression currently compresses the whole container.

This makes random access difficult because compressed streams are not
seekable.

Default for this prototype: **no compression**.

------------------------------------------------------------------------

## Concurrency & integrity

Concurrency support is still evolving.

Basic protections exist to avoid duplicate chunk ingestion and to
coordinate concurrent writers, but the system has not yet been
stress-tested for heavy parallel workloads.

-   Concurrent store/remove/gc operations are not a focus for v0.
-   Concurrent operations may leave unused bytes in containers.

Future versions will improve:

-   crash recovery
-   concurrency coordination
-   container lifecycle safety

------------------------------------------------------------------------

# Security

See `SECURITY.md`.

This project is a prototype and should not be used to protect sensitive
data.

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

Short-term goals:

- framed container format with random-access compression
- improved container lifecycle management
- stronger crash recovery guarantees
- improved concurrency coordination
- richer operational statistics

Longer-term ideas:

- cloud storage backends
- container compaction
- optional encryption
- background integrity verification

------------------------------------------------------------------------

# Contributing

Contributions and discussion are welcome.

See `CONTRIBUTING.md`.

------------------------------------------------------------------------

# License

Apache-2.0. See `LICENSE`.