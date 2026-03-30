# coldkeep

![CI](https://github.com/franchoy/coldkeep/actions/workflows/ci.yml/badge.svg)
![Go Version](https://img.shields.io/badge/go-1.23+-blue)
![License](https://img.shields.io/badge/license-Apache%202.0-blue)
![Status](https://img.shields.io/badge/status-research%20experimental-orange)
![Release](https://img.shields.io/github/v/release/franchoy/coldkeep?include_prereleases)

> **Status:** Experimental research project.  
> **Not production-ready. Do not use for real or sensitive data.**  
> On-disk format and APIs may change before v1.0.

coldkeep is a content-addressed storage engine for cold data.

It splits files into content-defined chunks, deduplicates them using SHA-256,
and stores them as encoded blocks in append-only container files with database-backed metadata.

coldkeep guarantees deterministic, byte-identical restore of stored data,
validated by end-to-end hashing and resilient across garbage collection
and system restart/recovery.

> coldkeep is designed as a correctness-first storage engine, prioritizing
> determinism and recoverability over performance and feature completeness.

---

## Why coldkeep?

coldkeep is designed for correctness-first cold storage.

Unlike traditional backup tools, it provides:

- Deterministic, byte-identical restore guarantees
- Content-addressed deduplication with strong integrity validation
- Explicit lifecycle management for safe recovery
- Multi-level verification (metadata, container, and full data integrity)
- Simulation capabilities to evaluate storage impact before committing data

The goal is not maximum performance, but maximum confidence in stored data.

---

## What it does (today)

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
- Simulate storage operations without writing data to disk (v0.8).
- Provide structured JSON output for all CLI commands for automation and scripting (v0.8).

---

## ✨ What’s new in v0.8

- `simulate` command for dry-run storage analysis
- Simulation reuses the real chunking, deduplication, and metadata pipeline for accurate results
- JSON output mode (`--output json`) across CLI commands
- Structured result models for store, restore, remove, gc, stats
- Typed CLI error handling and stable exit codes
- Startup recovery JSON reporting
- Improved CLI validation and consistent command behavior
- Enhanced verification consistency and deep-check correctness
- Improved retry handling in storage pipeline
- Stronger integration test coverage and CLI contract tests

---

## 🔍 Simulation (v0.8)

coldkeep now supports simulation to evaluate storage impact without writing data.

### Example

```bash
coldkeep simulate store-folder ./data
coldkeep simulate store file.txt --output json
```

### What simulation does

- Uses real chunking and deduplication
- Executes the full metadata pipeline in an isolated temporary database
- Simulates container packing behavior
- Produces realistic statistics:
  - logical size (input data)
  - stored size (deduplicated physical storage)
  - deduplication ratio
  - container count

### What simulation does NOT do

- Does not write container files
- Does not persist data
- Does not modify real storage

Simulation is designed as a **decision tool** for evaluating coldkeep before adoption,
providing realistic estimates of storage efficiency and expected container usage.

---

## CLI Output (v0.8)

coldkeep supports structured output for automation.

### Output modes

- `text` (default)
- `json`

### Example

```bash
coldkeep stats --output json
coldkeep list --output json
coldkeep simulate store-folder ./data --output json
```

### Notes

- JSON output is considered stable starting in v0.8 and is intended for long-term compatibility.
- CLI exit codes are now consistent and machine-friendly
- Errors are classified into usage, verification, and runtime categories
- Machine-readable JSON output is written to stdout, while diagnostic and recovery messages are written to stderr.

---

## ✨ What’s new in v0.7

- Block abstraction layer (logical vs physical separation)
- Pluggable codecs (plain + aes-gcm)
- AES-GCM encryption
- CLI codec selection
- `init` command for key setup
- CI coverage for both modes

---

## 🔐 Encryption model

coldkeep supports pluggable block encoding via codecs:

| Codec | Description |
| --- | --- |
| `plain` | No encoding (raw data) |
| `aes-gcm` | AES-256-GCM authenticated encryption |

Key properties:

- Encryption is applied at the **block level**
- Hashing is always performed on **plaintext**
- Each block uses a **random nonce**
- Encryption keys are **externalized via environment variables**

> The system fails fast if encryption is requested and no key is provided.
>
> Encryption is applied after chunking and before storage, ensuring
> deduplication operates on plaintext while data at rest remains protected.

---

## 🔑 Initialization (Encryption Setup)

Before storing encrypted data, generate a key:

```bash
coldkeep init
```

This will:

- generate a secure 256-bit encryption key
- print it to the console
- create a `.env` file if it does not already exist

Example:

```bash
COLDKEEP_KEY=...
COLDKEEP_CODEC=aes-gcm
```

Load it into your shell:

```bash
export $(cat .env | xargs)
```

### Docker:

The `/app` mount ensures the `.env` file created by `init` persists on the host.

```bash
docker compose run --rm -v "$PWD:/app" app init
```

> ⚠️ Data encrypted with a key cannot be recovered without it.  
> There is currently no key rotation or recovery mechanism.

---

## ⚙️ Codec selection

```bash
coldkeep store --codec plain file.txt
coldkeep store --codec aes-gcm file.txt
```

### Codec notes

- `aes-gcm` requires `COLDKEEP_KEY`
- The CLI flag overrides environment configuration
- The default codec is `aes-gcm`
- Using `plain` stores data unencrypted and prints a warning

---

## 🚀 Quickstart

A small `samples/` folder is included for testing and experimentation.

### Quick start (local, no Docker)

```bash
# 1. Generate encryption key and write .env
coldkeep init
```

> **Security note:** If you lose the key, data cannot be recovered.  
> Never commit `.env` to version control.

```bash
# 2. Load the key into your shell
export $(cat .env | xargs)
```

```bash
# 3. Store a file
coldkeep store file.txt
```

#### Optional: simulate storage impact before storing

```bash
coldkeep simulate store file.txt
```

> Simulation does not write any data and can be safely used before storing files.

---

### Quick start (Docker)

The `/app` mount ensures the `.env` file created by `init` persists on the host.

```bash
# 1. Start services
docker compose up -d --build
```

```bash
# 2. Generate encryption key (required before storing data)
docker compose run --rm -v "$PWD:/app" app init
```

> **Important:** This creates a `.env` file with your encryption key.  
> You must pass this file to subsequent commands using `--env-file`.

> **Security note:** If you lose the key, data cannot be recovered.

```bash
# 3. Store a file (pass the key from the generated .env)
docker compose run --rm \
  --env-file .env \
  -v "$PWD/samples:/samples" \
  app store /samples/hello.txt
```

#### Optional: simulate storage impact before storing

```bash
docker compose run --rm \
  -v "$PWD/samples:/samples" \
  app simulate store /samples/hello.txt
```

> Simulation does not write any data and can be safely used before storing files.

---

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

Verification results can be exported in JSON format using `--output json`.

### Notes

Deep verification performs full reads of container files and may be slow,
especially for large datasets.
Recommended for periodic integrity audits rather than frequent execution.



---

## Deterministic restore guarantees (v0.5)

coldkeep guarantees deterministic restore at the logical file level (introduced in v0.5).

Current integration coverage validates:

- deterministic stored logical files under deduplication
- byte-identical restore outputs verified by SHA-256
- consistent restore behavior after GC and restart/recovery
- deterministic results across representative and edge-case datasets

This guarantee applies to logical files and dataset-level workflows.

Coldkeep does not yet define a first-class contract for restoring full
directory trees with exact original layout. 
Current support focuses on logical file reconstruction, with tests restoring
files individually rather than reconstructing full directory structure.

---

## Container lifecycle and restore boundaries

coldkeep uses an append-only container model where data is written
sequentially and containers are sealed once they reach their maximum size.

At any given time, there may be an **active (unsealed) container**
receiving new data.

### Restore behavior

Restore operations may read data from both:

- sealed containers
- the active unsealed container

This ensures that recently stored data can be restored immediately,
without waiting for container rotation or sealing.

### Verification behavior

Verification distinguishes between container states:

- **Standard / Full verification**
  - Focus on metadata and structural consistency
  - Do not require all containers to be sealed

- **Deep verification**
  - Reads and validates actual stored data
  - Assumes the system has completed startup recovery.

### Why this distinction exists

This design allows:

- immediate restore availability after store operations
- safe append-only writes without blocking on container sealing
- strong integrity guarantees through explicit verification steps

In short:

- **Restore prioritizes availability**
- **Verification enforces correctness**

---

## Design sketch

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

Containers follow an append-only write model, ensuring deterministic writes and simplifying recovery.

Storage pipeline:

```text
logical_file -> file_chunk -> chunk -> blocks -> container
```

Lifecycle states:

- logical_file: PROCESSING → COMPLETED → ABORTED
- chunk: PROCESSING → COMPLETED → ABORTED

These states allow coldkeep to detect interrupted operations and
recover safely on startup.

---

## Project structure

```text
coldkeep/
│
├─ cmd/
│   └─ coldkeep/          # CLI entrypoint
│
├─ internal/
│   ├─ blocks/            # block encoding (plain, aes-gcm)
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
```

---

## Development

### 🐳 Local development (with Docker)

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

---

### 💻 Local development (without Docker)

Start Postgres (example):

``` bash
docker compose up -d postgres
```

Build the CLI first using the build command below.

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

---

### Build

``` bash
go build -o coldkeep ./cmd/coldkeep
```

### Tests

Run all tests:

``` bash
go test ./...
```

Integration tests live under:

tests/

and require a running PostgreSQL instance.

---

### Smoke test

`scripts/smoke.sh` runs a full end-to-end workflow using the `samples/` directory.

store -> stats -> list -> restore -> dedup check.

#### Local

``` bash
docker compose up -d postgres
go build -o coldkeep ./cmd/coldkeep
bash scripts/smoke.sh
```

#### Docker

``` bash
docker compose up -d postgres

docker compose run --rm \
  -e COLDKEEP_SAMPLES_DIR=/samples \
  -e COLDKEEP_STORAGE_DIR=/tmp/coldkeep-storage \
  -v "$PWD/samples:/samples" \
  --entrypoint bash \
  app scripts/smoke.sh
```

---

## Configuration

Database configuration is read from environment variables\
(see `docker-compose.yml` for defaults).

Storage is written to:

./storage/

During development you can safely delete this directory.

Additional environment variables used in development:

- COLDKEEP_STORAGE_DIR
- COLDKEEP_SAMPLES_DIR

---

## Known limitations

### Crash recovery

coldkeep includes a crash recovery model based on lifecycle states.

Operations use explicit states (`PROCESSING`, `COMPLETED`, `ABORTED`)
to detect and handle interrupted operations safely.

On startup the system:

- marks stale `PROCESSING` rows as `ABORTED`
- prevents incomplete chunks from being reused
- allows safe retries of interrupted operations

This model ensures that partially written data does not corrupt
the logical state of the system.

The append-only container model simplifies recovery by eliminating
in-place mutations of container data.

However, the system is still experimental and full transactional
guarantees across filesystem and database layers are not yet complete.

Use only with **disposable test data**.

### Container compression

Whole-container compression has been removed in v0.6.

Future versions may introduce block-level compression.

### Concurrency & integrity

Concurrency support has been significantly improved, including locking and retry mechanisms.

However, it is still evolving and not yet optimized for
extreme parallel workloads.

---

## Security

See `SECURITY.md`.

This is an experimental research project with evolving on-disk formats.

Do not use for real or sensitive data.

---

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
  Add simulation capabilities for storage planning and establish a stable,
  automation-friendly command-line interface (introduced in v0.8).

- **v0.9 — Internal Hardening**  
  Improve reliability, simplify internals, and finalize implementation details.

- **v1.0 — Storage Engine Stable**  
  Coldkeep becomes a trustworthy storage engine for real cold backups.

---

## Contributing

Contributions and discussion are welcome.

See `CONTRIBUTING.md`.

---

## License

Apache-2.0. See `LICENSE`
