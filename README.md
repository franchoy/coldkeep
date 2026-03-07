# coldkeep (POC)

![CI](https://github.com/franchoy/coldkeep/actions/workflows/ci.yml/badge.svg)
![Go Version](https://img.shields.io/badge/go-1.23+-blue)
![License](https://img.shields.io/badge/license-Apache%202.0-blue)
![Status](https://img.shields.io/badge/status-research%20prototype-orange)

> **Status:** Research prototype / proof-of-concept.\
> **Not production-ready. Do not use for real or sensitive data.**

coldkeep is an experimental **local-first content-addressed file storage
prototype** written in Go.

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
-   Display basic storage statistics.

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

------------------------------------------------------------------------

# Known limitations

## Crash consistency

coldkeep is **not crash-consistent yet**.

Some operations combine filesystem writes with database transactions.\
Filesystem operations cannot be rolled back if a transaction fails.

Possible consequences:

-   orphan container files
-   temporary disagreement between DB and disk
-   partially applied container writes

Use only with **disposable test data**.

------------------------------------------------------------------------

## Container compression

Container-level compression currently compresses the whole container.

This makes random access difficult because compressed streams are not
seekable.

Default for this prototype: **no compression**.

------------------------------------------------------------------------

## Concurrency & integrity

Concurrency guarantees are minimal.

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

`scripts/smoke.sh` provides a quick end-to-end test using the `samples/`
directory.

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

# Roadmap ideas

-   framed container format with random-access compression
-   improved crash consistency
-   safer concurrent operations
-   experimental cloud storage backends
-   richer CLI and statistics

------------------------------------------------------------------------

# Contributing

Contributions and discussion are welcome.

See `CONTRIBUTING.md`.

------------------------------------------------------------------------

# License

Apache-2.0. See `LICENSE`.