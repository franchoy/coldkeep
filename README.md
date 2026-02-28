# coldkeep (POC)

![CI](https://github.com/franchoy/coldkeep/actions/workflows/ci.yml/badge.svg)
![Go Version](https://img.shields.io/badge/go-1.23+-blue)
![License](https://img.shields.io/badge/license-Apache%202.0-blue)
![Status](https://img.shields.io/badge/status-research%20prototype-orange)

> **Status:** Research prototype / proof‑of‑concept.\
> **Not production‑ready. Do not use for real or sensitive data.**

coldkeep is an experimental local-first content‑addressed file storage
prototype written in Go.\
It stores files as **chunks** inside **container** files on disk, and
tracks metadata in Postgres.

This repo is meant for learning, experimentation, and discussion --- not
for operational backup/storage use.

------------------------------------------------------------------------

## What it does (today)

-   Store a file (or folder) by splitting it into chunks.
-   Deduplicate chunks using SHA‑256.
-   Pack chunks into container files up to a max size.
-   Restore a file by reconstructing it from chunks.
-   Remove a logical file (decrements chunk ref counts).
-   Run GC to delete unreferenced chunks/containers.
-   Basic stats.

------------------------------------------------------------------------

## Design sketch

-   **logical_file**: user-facing file entry (name, size, file_hash)
-   **chunk**: content-addressed chunk (hash, size, ref_count,
    container_id, offset)
-   **file_chunk**: mapping from logical_file → ordered chunk list
-   **container**: physical file on disk that stores chunk data

Containers are plain files under:

    storage/containers/

------------------------------------------------------------------------

## Quickstart (Using the included `samples/` folder)

This repository includes a small `samples/` directory for testing.

### 🐳 With Docker

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

Restore a file (replace ID as needed):

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

### 💻 Local (without Docker)

Start Postgres (example via Docker):

``` bash
docker compose up -d postgres
```

Build:

``` bash
cd app
go build -o ../coldkeep .
cd ..
```

Store the sample folder:

``` bash
./coldkeep store-folder samples
```

List files:

``` bash
./coldkeep list
```

Restore a file:

``` bash
./coldkeep restore 1 ./restored.bin
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

## Configuration

The app reads DB connection info from environment variables\
(see `docker-compose.yml` for defaults).

Storage goes to:

    ./storage/

You can safely delete the entire `storage/` directory during testing.

------------------------------------------------------------------------

## Known limitations (important)

### Crash consistency

coldkeep is **not crash-consistent**. Some operations combine filesystem
writes with DB transactions, but filesystem changes cannot be rolled
back if a DB transaction fails.

Possible effects:

-   Orphan container files on disk
-   Temporary disagreement between DB and disk
-   Partially applied container sealing/compression

Use only with disposable test data.

------------------------------------------------------------------------

### Whole-container compression

If container compression is enabled (gzip/zstd), restores may become
very slow because compressed streams are not seekable.

Default for this POC is **no container compression**.

------------------------------------------------------------------------

### Concurrency & integrity

-   Concurrency guarantees are minimal.
-   Heavy concurrent store/remove/gc is not a goal for v0.
-   Concurrent operations may leave orphan bytes inside containers.

------------------------------------------------------------------------

## Security

See `SECURITY.md`.

This project is a prototype and should not be used to protect sensitive
information.

------------------------------------------------------------------------

## Development

### Tests

-   Unit tests: `go test ./...`
-   Integration tests require Postgres (see tests for details).

### Smoke script

`scripts/smoke.sh` provides a quick end‑to‑end test using the `samples/`
folder.

#### Local (without Docker)

docker compose up -d postgres
go build -o coldkeep ./app
bash scripts/smoke.sh

#### Docker

docker compose up -d postgres
docker compose run --rm  
  -e COLDKEEP_SAMPLES_DIR=/samples \
  -e COLDKEEP_STORAGE_DIR=/tmp/coldkeep-storage \
  -v "$PWD/samples:/samples" \
  --entrypoint bash \
  app scripts/smoke.sh

------------------------------------------------------------------------

## Roadmap ideas

-   Framed container format for random access with compression.
-   Stronger crash consistency.
-   Safer concurrent operations.
-   Cloud backends experiments.
-   CLI improvements and richer stats.

------------------------------------------------------------------------

## Contributing

Contributions and discussion are welcome.\
See `CONTRIBUTING.md`.

------------------------------------------------------------------------

## License

Apache-2.0. See `LICENSE`.