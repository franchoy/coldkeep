# coldkeep

![CI](https://github.com/franchoy/coldkeep/actions/workflows/ci.yml/badge.svg)
![Go Version](https://img.shields.io/badge/go-1.23+-blue)
![License](https://img.shields.io/badge/license-Apache%202.0-blue)
![Status](https://img.shields.io/badge/status-research%20experimental-orange)
![Release](https://img.shields.io/github/v/release/franchoy/coldkeep?include_prereleases)



> **Status:** Experimental research projec.\
> **Not production-ready. Do not use for real or sensitive data.**

coldkeep is an experimental **local-first content-addressed file storage engine**
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
    │   ├─ container/         # container format + container management
    │   ├─ chunk/             # chunking and compression logic
    │   ├─ db/                # database connection helpers
    │   ├─ storage/           # store / restore / remove pipeline
    │   ├─ maintenance/       # gc and stats
    │   ├─ listing/           # file listing operations
    │   └─ utils/             # small helper utilities
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

    go build ./cmd/coldkeep

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