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

coldkeep is designed to guarantee deterministic, byte-identical restore of stored data,
validated by end-to-end hashing and resilient across garbage collection
and system restart/recovery under defined operating conditions.

> coldkeep is designed as a correctness-first storage engine, prioritizing
> determinism and recoverability over performance and feature completeness.

For v0.9, every change intended for mainline or release delivery is expected to
pass the full GitHub Actions pipeline before merge or tag publication. The repo
contains a synthetic required check named `CI Required Gate` that aggregates the
quality, integration, and smoke jobs across both codecs.

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
- Run `coldkeep doctor` as the recommended operator-facing health check that bundles recovery, verification, and schema sanity checks.
- Simulate storage operations without writing data to disk (v0.8).
- Provide structured JSON output for all CLI commands for automation and scripting (v0.8).

### Command mutation model

- `store` / `store-folder` mutate stored data and metadata.
- `remove` mutates metadata and may make unreferenced physical data eligible for collection.
- `gc` mutates metadata and container files unless `--dry-run` is used.
- startup `recovery` is corrective and mutates recoverable metadata/state.
- `doctor` is corrective and may mutate metadata because it runs recovery before verify.
- `verify` is read-only during its verification phase.

CLI note: `coldkeep verify ...` still runs automatic startup recovery before the
verification phase, so the overall command may correct stale recoverable state
before the read-only verification checks begin.

---

## 🛡️ Storage Guarantees (v0.9)

coldkeep is designed as a **correctness-first storage engine**.  
This section defines the guarantees provided by the system as of **v0.9**.

### Summary

coldkeep v0.9 guarantees:

- deterministic, byte-identical restore
- no exposure of partially written or inconsistent data
- non-destructive garbage collection
- atomic restore operations
- safe concurrent storage operations

### Core invariants

The system relies on a small set of invariants that should stay true across store,
restore, verification, garbage collection, and recovery:

- every `COMPLETED` chunk has exactly one valid block record
- every block record references a valid container
- every `COMPLETED` logical file has a complete, contiguous, ordered `file_chunk` graph
- `live_ref_count > 0` protects a chunk from garbage collection
- `pin_count > 0` protects a chunk from concurrent deletion during restore-like operations
- committed metadata implies the referenced bytes are already durable on disk

### Core validity model

A logical file is considered **valid and restorable** only when:

- its status is `COMPLETED`
- all referenced chunks are `COMPLETED`
- all referenced blocks exist and are readable

Only files in this state are returned by `list` and `search` and are eligible for restore.

---

### Data integrity

coldkeep guarantees **end-to-end data integrity**:

- Every chunk is validated using SHA-256 during restore
- The final restored file hash must match the original stored hash
- Any mismatch causes the restore operation to fail

> A restored file is either **bit-for-bit identical**, or the operation fails.

---

### Crash consistency

coldkeep guarantees **safe recovery after crashes or interruptions**:

- Writes use lifecycle states: `PROCESSING → COMPLETED → ABORTED`
- On startup:
  - incomplete operations are marked as `ABORTED`
  - inconsistent containers may be quarantined

> No partially written or inconsistent data is exposed as valid.

### Contributor note (append lifecycle contract)

For storage-writer lifecycle semantics, use the single authoritative state machine in
[`internal/storage/store.go`](internal/storage/store.go) (see "Append lifecycle state machine").
Implementation comments in writer files are intentionally brief pointers to that source of truth.

---

### Restore safety

Restore operations are **atomic and verified**:

- Data is written to a temporary file
- The file is fsynced and closed
- It is atomically renamed into place
- The parent directory is fsynced for durability

> A restore either produces a complete valid file, or no file at all.

---

### Garbage collection safety

Garbage collection is **non-destructive**:

- Only chunks with no live file references and no active restore pins are removed
- Containers are deleted only when all their chunks are unreferenced
- Referenced data is never removed

> GC cannot delete data required to restore a valid logical file.

---

### Concurrency model

coldkeep supports **safe concurrent operations**:

- Logical files and chunks are claimed via database constraints
- Duplicate work is avoided via hash-based deduplication
- Concurrent operations coordinate via retry and backoff

> Concurrent storage operations do not corrupt data or create inconsistent state.

---

### Verification model

coldkeep provides multiple verification levels:

- `standard`: metadata integrity
- `full`: container structure and metadata consistency
- `deep`: full data read and hash verification

Verification assumes:

- all `COMPLETED` chunks are valid
- corrupted or missing containers are quarantined

The verification checks themselves are read-only. When using the CLI, startup
recovery still runs before `verify`, so corrective metadata changes can happen
before the read-only verification phase starts.

> The system can detect corruption explicitly when verification is run.

---

### Simulation behavior

The `simulate` command provides **accurate metadata-level estimation**:

- No data is written to storage
- Real chunking and deduplication logic is executed
- Container packing math follows the real write path closely enough to estimate size and layout

> Simulation reflects real storage behavior without side effects.

Use simulated mode for:

- planning
- estimation
- tests
- workflow validation

Simulated mode is intended for planning, estimation, tests, and workflow validation — not as proof of real durability.
Simulated mode is not proof of physical durability guarantees.

Simulation is still a confidence tool, not a correctness proof for the PostgreSQL-backed runtime:

- Simulated mode uses an isolated SQLite database
- It does not exercise PostgreSQL advisory locks
- It does not prove row-lock or lock-wait behavior under contention
- It is not strong evidence for GC exclusivity or retry behavior under real concurrent load

> These guarantees describe system behavior under controlled conditions.
> coldkeep remains experimental and is not yet recommended for production use.

---

### Non-guarantees (v0.9)

coldkeep does **not** guarantee:

- Backward compatibility of on-disk format
- Stability of internal schemas before v1.0
- Protection against manual modification of storage or database
- Distributed or multi-node consistency

---

### Trust boundary

Guarantees hold only if:

- the database is not externally modified
- container files are not manually altered
- the filesystem honors write and fsync semantics

---

## ✨ What’s new in v0.10

- Reuse integrity hardening for `COMPLETED` logical files:
  - semantic validation mode via `COLDKEEP_REUSE_SEMANTIC_VALIDATION` (`off` / `suspicious` / `always`; default: `suspicious`)
  - `suspicious`: semantic re-read/re-hash only when risk signals are present (for example retry history or mutable container references)
  - `always`: semantic re-read/re-hash for every reuse candidate (strongest inline gate, highest read/CPU cost)
  - structural graph validation remains mandatory in all modes (contiguous file-chunk links, completed chunks, valid block/container references, on-disk container presence)
- Startup sealing recovery hardening: containers left in `sealing=TRUE` are quarantined (not auto-sealed) when physical size and DB `current_size` diverge, preventing ghost-byte promotion as healthy
- Restore safety hardening under interleavings:
  - GC/delete liveness checks consistently honor `live_ref_count OR pin_count`
  - restore pinning behavior is covered by adversarial integration tests for restore/remove/GC races
- Deep verification hardening: deep verify now fails on trailing unaccounted bytes after the last completed block payload in a container
- Verification contract clarity: verification remains a recovered-state checker (run after recovery; not an online in-flight-write consistency checker)
- Completion-boundary hardening: logical-file completion verifies chunk linkage and contiguous ordering at the completion boundary before exposing the file as `COMPLETED`

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

### Example simulation commands

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
- Does not validate PostgreSQL-specific locking, advisory lock, or contention semantics

Use simulation for planning, estimation, tests, and workflow validation only.
It is not proof of physical durability.

Simulation is designed as a **decision tool** for evaluating coldkeep before adoption,
providing realistic estimates of storage efficiency and expected container usage.

### Operational timeouts

Database operations now run with bounded connection and statement limits by default. The main tuning knobs are:

- `COLDKEEP_DB_CONNECT_TIMEOUT_MS`
- `COLDKEEP_DB_OPERATION_TIMEOUT_MS`
- `COLDKEEP_DB_STATEMENT_TIMEOUT_MS`
- `COLDKEEP_DB_LOCK_TIMEOUT_MS`
- `COLDKEEP_DB_IDLE_IN_TX_TIMEOUT_MS`
- `COLDKEEP_DB_MAX_OPEN_CONNS`
- `COLDKEEP_DB_MAX_IDLE_CONNS`
- `COLDKEEP_DB_CONN_MAX_LIFETIME_MS`
- `COLDKEEP_DB_CONN_MAX_IDLE_TIME_MS`

These limits are intended to keep CLI commands from hanging indefinitely on dead connections, blocked sessions, or stalled lock acquisition.

Current defaults:

- `COLDKEEP_DB_CONNECT_TIMEOUT_MS=5000`
- `COLDKEEP_DB_OPERATION_TIMEOUT_MS=300000`
- `COLDKEEP_DB_STATEMENT_TIMEOUT_MS=30000`
- `COLDKEEP_DB_LOCK_TIMEOUT_MS=5000`
- `COLDKEEP_DB_IDLE_IN_TX_TIMEOUT_MS=60000`
- `COLDKEEP_DB_MAX_OPEN_CONNS=25`
- `COLDKEEP_DB_MAX_IDLE_CONNS=5`
- `COLDKEEP_DB_CONN_MAX_LIFETIME_MS=1800000`
- `COLDKEEP_DB_CONN_MAX_IDLE_TIME_MS=300000`

### Recovery strictness

- `COLDKEEP_STRICT_RECOVERY` (default: `true`) — controls how startup recovery handles suspicious orphan container conflicts.
  - `true` (default): Recovery aborts startup with an error on suspicious orphan container state.
    This is intentional and is the trust-first behavior.
  - `false`: Suspicious conflicts are downgraded to warnings and recovery continues.
    This relaxed mode is intended for messy environments and known duplicate-retrier/restart-race scenarios (for example during rolling restarts or replaying a partially-applied recovery).
- Production guidance: keep strict mode enabled (`COLDKEEP_STRICT_RECOVERY=true`).
- Operational expectation: strict mode can fail startup by design when state is suspicious; treat that as a safety signal, investigate, and recover explicitly.

### Semantic reuse validation

- `COLDKEEP_REUSE_SEMANTIC_VALIDATION` (default: `suspicious`) controls whether coldkeep performs an operational semantic validation pass before reusing a `COMPLETED` logical file row.
- Operationally, semantic validation can pin chunk rows, read/decode payload data from container files, and recompute chunk/file hashes.
- Modes:
  - `off`: run structural graph checks only. Fastest mode. Skips semantic payload re-validation, so corruption is more likely to be detected later by explicit `verify` runs.
  - `suspicious` (default): run semantic validation only when risk signals are present (for example file/chunk retry history or mutable container references). This is the recommended balance for normal production throughput.
  - `always`: run semantic validation for every completed-file reuse candidate. Strongest inline integrity gate, but highest IO/CPU cost and can noticeably increase store latency on reuse-heavy workloads.
- If ingestion performance drops unexpectedly after enabling this feature, check whether `COLDKEEP_REUSE_SEMANTIC_VALIDATION=always` is set.

### v1.0 trust model

This section describes how coldkeep's subsystems compose into a coherent
operator trust model.

- **Startup recovery is part of normal safe operation.**
  It runs automatically before every substantive command (`store`, `store-folder`,
  `restore`, `remove`, `gc`, `stats`, `list`, `search`, `verify`) and resolves
  in-flight write state from prior sessions. It is not an exceptional maintenance
  step — it is the expected lifecycle entry point. Startup recovery is itself a
  corrective, state-changing step.

- **`doctor` is the recommended operator health command and is corrective, not read-only.**
  It runs recovery + verify + schema/version sanity in one command and may
  update metadata (abort dangling writes, clear stale sealing markers) before
  running verification. Use it after startup, before first ingestion in a new
  environment, and as the standard pre-release gate.

- **`verify` assumes recovered state.**
  Verification (`verify standard/full/deep`) is layered integrity checking
  scoped to metadata and payload consistency. It is not a live online-consistency
  checker during in-flight writes and does not resolve incomplete write state
  itself — recovery must have run first. The verification phase itself is
  read-only.

- **Strict recovery is recommended in production.**
  `COLDKEEP_STRICT_RECOVERY=true` (the default) aborts startup on suspicious
  orphan container state. Treat strict-mode failures as intentional safety signals
  that require investigation, not as operational noise to suppress.

- **Semantic reuse validation trades performance for stronger reuse confidence.**
  `COLDKEEP_REUSE_SEMANTIC_VALIDATION` (default `suspicious`) controls whether
  coldkeep re-reads and re-hashes payload before accepting a completed-file reuse
  shortcut:
  - `off`: graph-only structural checks — fastest, relies on `verify` runs for
    corruption detection.
  - `suspicious` (default): semantic checks only when risk signals are present
    (retry history, mutable container references) — recommended balance.
  - `always`: semantic checks for every reuse candidate — strongest inline gate,
    highest read/CPU cost.

- **`pin_count` protects restore safety** by preventing GC/remove from reclaiming
  data while a restore operation holds active pins.

**Production baseline:** strict recovery (`COLDKEEP_STRICT_RECOVERY=true`) +
`doctor --standard` as the pre-ingestion readiness gate.

---

## CLI Output (v0.8)

coldkeep supports structured output for automation.

### Output modes

- `text` (default)
- `json`

### Example JSON output commands

```bash
coldkeep stats --output json
coldkeep list --output json
coldkeep list --limit 50 --offset 100 --output json
coldkeep simulate store-folder ./data --output json
```

### Output notes

- JSON output is considered stable starting in v0.8 and is intended for long-term compatibility.
- CLI exit codes are now consistent and machine-friendly
- Errors are classified into usage, system/runtime, verification, and recovery categories
- In `--output json` mode, each command invocation emits exactly one canonical JSON payload: success payloads go to stdout, and error payloads go to stderr with a non-zero exit code.
- Machine-readable JSON output is written to stdout, while diagnostic and recovery messages are written to stderr.

### Frozen CLI exit codes (public contract)

The CLI exit-code mapping is frozen for v1.0 compatibility and should not change
without a major-version bump.

| Exit code | Class | Meaning |
| --- | --- | --- |
| `0` | `SUCCESS` | Command completed successfully |
| `2` | `USAGE` | Validation / user input error (invalid args, unknown command/flag, invalid ID/level) |
| `1` | `GENERAL` | System/runtime error (I/O, DB, internal command failures not covered below) |
| `3` | `VERIFY` | Verification failure |
| `4` | `RECOVERY` | Recovery-phase failure |

For JSON errors (`--output json`), `error_class` matches this table (`USAGE`,
`GENERAL`, `VERIFY`, `RECOVERY`) and `exit_code` contains the numeric code.
In particular, `coldkeep doctor --output json` recovery-phase failures are
guaranteed to return `error_class=RECOVERY` with exit code `4`.

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

### Docker

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

#### Optional: simulate storage impact before storing (local)

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

#### Optional: simulate storage impact before storing (Docker)

```bash
docker compose run --rm \
  -v "$PWD/samples:/samples" \
  app simulate store /samples/hello.txt
```

> Simulation does not write any data and can be safely used before storing files.

---

## Doctor (recommended health check)

`coldkeep doctor` is the recommended operator-facing health command and a first-class
v1.0 CLI primitive. Treat it as the first command to run after startup and as the
standard pre-release and pre-ingestion readiness gate.

After significant operations, run `coldkeep doctor` to validate system health.

> **Doctor is corrective, not read-only.** It runs recovery before running
> verification, and may update database metadata — aborting dangling in-flight
> writes, clearing stale sealing markers — before the verification phase executes.
> Running doctor on a fresh deployment or after an unclean shutdown is safe and
> intended: it resolves recoverable state and then confirms integrity.

It runs the checks in this order:

1. Corrective recovery phase (may abort dangling writes and resolve stale state)
2. System verification (`standard` by default, or `full` / `deep`)
3. Schema/version sanity query

Doctor intentionally runs its own internal recovery phase and does not use the generic startup recovery event path used by commands such as `store`, `restore`, and `verify`.

### Doctor usage

```bash
coldkeep doctor
coldkeep doctor --full
coldkeep doctor --deep --output json
```

### Text output example

```text
Doctor health report
  Overall status:      ok
  Verify level:        full
  Phase 1 - Recovery:  ok
  Phase 2 - Verify:    ok
  Phase 3 - Schema:    ok (version=5)
  Note: Recovery phase may have modified metadata
  Recovery summary: aborted_logical_files=0 aborted_chunks=0 quarantined_missing_containers=0 quarantined_corrupt_tail_containers=0 quarantined_orphan_containers=0
  Recommended next step: none
```

### JSON output example

```json
{
  "status": "ok",
  "command": "doctor",
  "data": {
    "recovery": {
      "aborted_logical_files": 0,
      "aborted_chunks": 0,
      "quarantined_missing": 0,
      "quarantined_corrupt_tail": 0,
      "quarantined_orphan": 0,
      "skipped_dir_entries": 0,
      "checked_container_record": 12,
      "checked_disk_files": 12,
      "sealing_completed": 0,
      "sealing_quarantined": 0
    },
    "verify_level": "standard",
    "schema_version": 5,
    "recovery_status": "ok",
    "verify_status": "ok",
    "schema_status": "ok"
  }
}
```

### JSON failure contract (frozen)

For `doctor --output json`, failure handling is intentionally delegated to the
generic CLI error path.

- On failure, doctor emits exactly one generic error JSON payload on `stderr`
  and exits non-zero.
- Doctor does not emit a doctor-shaped `command=data` success payload on
  failure.
- Doctor does not emit partial phase data (`recovery`, `verify`,
  `schema_version`) on failure.
- Failure phase detail is conveyed only in the generic `message` field
  (for example: `doctor verify phase failed: ...`).

This contract is covered by integration and command-layer tests and is intended
to remain stable for automation.

### Frozen v1.0 Doctor Contract

The following product decisions are explicitly frozen for v1.0:

- Default doctor verify level remains `standard`.
- Success JSON shape remains exactly the current `status + command + data` envelope,
  with the existing doctor `data` fields unchanged.
- Failure JSON remains owned by the generic CLI error contract (not doctor-specific).
- Doctor is corrective, not read-only: recovery runs before verify and may mutate
  metadata to resolve recoverable state.

### Operational guidance

- Run `doctor` after startup, before first ingestion in a new environment.
- Treat `doctor` as the canonical operator gate command in automation/smoke/release checks.
- After significant operations, run `coldkeep doctor` to validate system health.
- Frozen v1.0 product decision: `doctor` is the fast health gate and defaults to `--standard`.
- Frozen v1.0 text contract: doctor text mode always prints `Note: Recovery phase may have modified metadata`.
- Prefer `doctor --standard` for frequent checks.
- Use `doctor --full` for stronger structural assurance.
- Reserve `doctor --deep` for periodic audits due to higher I/O cost.

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

### Verification usage

Verify the entire system:

```bash
coldkeep verify system --standard
coldkeep verify system --full
coldkeep verify system --deep
```

Verify a specific file:

```bash
coldkeep verify file <file_id> --standard
coldkeep verify file <file_id> --full
coldkeep verify file <file_id> --deep
```

Verification results can be exported in JSON format using `--output json`.

CLI defaults/parsing rules:

- Verify defaults to `standard` when no level is provided.
- Verify level can be passed as flags (`--standard|--full|--deep`) or as positional level tokens in canonical forms:
  - `coldkeep verify system <standard|full|deep>`
  - `coldkeep verify file <file_id> <standard|full|deep>`
- Do not pass both a verify-level flag and positional level token in the same invocation.

### Verification notes

Verification is a recovered-state checker, not a general online consistency checker.
Running verification while writes are in-flight can produce transient false positives.
Operationally, run `verify` after startup recovery has completed and when ingestion
work is idle or quiesced.

The `verify` checks themselves do not mutate stored state. In the CLI, any
state change observed before verification comes from the automatic startup
recovery step, not from the verification phase.

Mode guidance:

- `standard`: fast metadata sanity checks
- `full`: stronger structural checks, moderate I/O
- `deep`: strongest content proof, highest I/O and runtime cost

Deep verification performs full reads of container files and may be slow,
especially for large datasets.
Recommended for periodic integrity audits rather than frequent execution.

---

## Deterministic restore

Deterministic restore was introduced in v0.5 and is now part of the
core storage guarantees defined in the Storage Guarantees section above.

Integration tests validate:

- byte-identical restore outputs
- consistency across GC and recovery
- deterministic behavior across datasets

### Local test flow for newcomers

For a reproducible local run that mirrors DB-backed integration expectations:

1. Start Postgres:

```bash
docker compose up -d postgres
```

1. Set environment variables:

```bash
export COLDKEEP_TEST_DB=1
export COLDKEEP_CODEC=plain
export DB_HOST=127.0.0.1
export DB_PORT=5432
export DB_USER=coldkeep
export DB_PASSWORD=coldkeep
export DB_NAME=coldkeep
export DB_SSLMODE=disable
```

1. Run correctness tier first:

```bash
go test ./tests -short -count=1 -v -timeout 20m
```

1. Run full integration and then full repository tests:

```bash
go test ./tests -count=1 -v -timeout 20m
go test ./... -count=1 -timeout 25m
```

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
  Logical identity of a content-addressed chunk (chunk_hash, size, live_ref_count, pin_count).

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

---

## CI Gate Policy (v0.9)

The repository-side workflow now enforces a single final status named `CI Required Gate`.
That gate fails if any upstream quality, integration, or smoke job fails or is skipped.

To make that gate non-bypassable for pull requests, merges, and release preparation,
GitHub repository settings must also enforce it.

Recommended GitHub ruleset / branch protection configuration:

- Require pull requests before merging to `main`
- Require status checks before merging
- Mark `CI Required Gate` as a required status check
- Require merge queue and keep `merge_group` enabled in the workflow
- Apply the same required check to `release/**` and `hotfix/**`
- Restrict direct pushes to protected branches
- Restrict tag creation for release tags such as `v*` to trusted maintainers or automation

Recommended rule names to keep policy auditing deterministic:

- `Protect mainline branches`
- `Protect release tags`

Without those GitHub-side protections, no workflow file can fully prevent an
administrator or an unrestricted direct push from bypassing CI.

Maintainers can audit the current setup with:

```bash
scripts/audit_ci_enforcement.sh --local-only
scripts/audit_ci_enforcement.sh --repo franchoy/coldkeep
```

The remote audit requires GitHub CLI authentication with repository admin access.

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
./coldkeep list --limit 50 --offset 100
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

By default, smoke uses an isolated temporary storage directory and cleans it up
on exit. This avoids permission/stale-state interference from repository-local
`storage/containers` during repeated local runs. To persist smoke storage
artifacts, set `COLDKEEP_STORAGE_DIR` explicitly.

Smoke also enables quiet healthy startup recovery output by default
(`COLDKEEP_QUIET_HEALTHY_STARTUP_RECOVERY=1`), so repeated healthy startup
recovery internals do not flood logs. Set
`COLDKEEP_QUIET_HEALTHY_STARTUP_RECOVERY=0` (or `false`) to force full startup
recovery log replay during smoke.

#### Local

``` bash
docker compose up -d postgres
go build -o coldkeep ./cmd/coldkeep
bash scripts/smoke.sh
```

For repeatable local reruns against an existing DB, enable smoke reset mode:

``` bash
COLDKEEP_TEST_DB=1 COLDKEEP_SMOKE_RESET_DB=1 \
DB_HOST=127.0.0.1 DB_PORT=5432 DB_USER=coldkeep DB_PASSWORD=coldkeep DB_NAME=coldkeep \
bash scripts/smoke.sh
```

This option truncates smoke tables and clears `COLDKEEP_STORAGE_DIR` before running.

#### Smoke in Docker

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

On PostgreSQL connections, coldkeep validates that `schema_version` exists and
is up to date (`>= 5`) during startup.

- Default: schema must already be initialized (apply `db/schema_postgres.sql`).
- Optional bootstrap: set `COLDKEEP_DB_AUTO_BOOTSTRAP=true` to auto-apply the
  embedded PostgreSQL schema if `schema_version` is missing.

### First-run PostgreSQL behavior

Startup uses a strict default for correctness.

- If `schema_version` is missing and `COLDKEEP_DB_AUTO_BOOTSTRAP` is not set to
  `true`, coldkeep exits with an error.
- The error is expected on a brand-new PostgreSQL database that has not been
  initialized yet.

You can choose one of two setup modes:

1. Explicit setup (default)

``` bash
psql -U coldkeep -d coldkeep -f db/schema_postgres.sql
./coldkeep stats
```

1. Self-bootstrap on first run

``` bash
export COLDKEEP_DB_AUTO_BOOTSTRAP=true
./coldkeep stats
```

If you hit the first-run failure, use either of the two modes above and rerun
the command.

### Troubleshooting: first run fails on PostgreSQL

If startup fails with:

``` text
postgres schema is not initialized (missing schema_version table); apply db/schema_postgres.sql or set COLDKEEP_DB_AUTO_BOOTSTRAP=true
```

it means the target PostgreSQL database is reachable, but its schema has not
been initialized yet.

Fix with one of these options:

1. Explicit init (recommended default)

``` bash
psql -U coldkeep -d coldkeep -f db/schema_postgres.sql
```

1. Enable auto-bootstrap for first run

``` bash
export COLDKEEP_DB_AUTO_BOOTSTRAP=true
```

Storage is written to:

./storage/

During development you can safely delete this directory.

Additional environment variables used in development:

- COLDKEEP_STORAGE_DIR
- COLDKEEP_SAMPLES_DIR
- COLDKEEP_QUIET_HEALTHY_STARTUP_RECOVERY

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

- **v0.10 — Trust Validation Phase**  
  Validate guarantees under adversarial conditions and eliminate remaining correctness risks.

- **v1.0 — Storage Engine Stable**  
  Coldkeep becomes a trustworthy storage engine for real cold backups.

---

## Contributing

Contributions and discussion are welcome.

See `CONTRIBUTING.md`.

---

## License

Apache-2.0. See `LICENSE`
