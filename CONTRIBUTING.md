# Contributing to coldkeep

Thank you for your interest in contributing to coldkeep.

coldkeep V0 is an experimental deduplicated storage engine prototype. We
welcome improvements, bug reports, architectural discussions, and
thoughtful critiques.

------------------------------------------------------------------------

## Project Philosophy (V0)

coldkeep V0 prioritizes:

-   Correctness over performance
-   Deterministic storage behavior
-   Transactional metadata safety
-   Clear and readable code
-   Simplicity over abstraction

This is a research prototype. Stability and conceptual clarity are more
important than feature velocity.

Major architectural changes should be discussed before implementation.

------------------------------------------------------------------------

## Development Requirements

-   Go 1.23+ (or the version specified in go.mod)
-   PostgreSQL 14+
-   Docker (recommended for reproducibility)

------------------------------------------------------------------------

## Running with Docker (Recommended)

Start PostgreSQL:

``` bash
docker compose up -d postgres
```

Build the application image:

``` bash
docker compose build
```

Run commands:

``` bash
docker compose run --rm app stats
docker compose run --rm app store-folder samples
```

------------------------------------------------------------------------

## Running Without Docker

Set environment variables:

``` bash
export DB_HOST=localhost
export DB_PORT=5432
export DB_USER=coldkeep
export DB_PASSWORD=coldkeep
export DB_NAME=coldkeep
```

Initialize the schema:

``` bash
psql -U coldkeep -d coldkeep -f db/schema_postgres.sql
```

For first run, you can also enable one-time bootstrap behavior instead of
manually applying the schema:

``` bash
export COLDKEEP_DB_AUTO_BOOTSTRAP=true
```

Without manual schema initialization or this env var, coldkeep will fail fast
on startup if `schema_version` is missing.

Build:

``` bash
go build -o coldkeep ./cmd/coldkeep
```

Run:

``` bash
./coldkeep store-folder samples
./coldkeep stats
```

------------------------------------------------------------------------

## Code Style & Quality

-   Code must pass `gofmt`
-   Code must pass `go vet`
-   Keep functions small and readable
-   Avoid premature optimization
-   Prefer clarity over cleverness
-   Avoid hidden or implicit concurrency patterns

Before submitting:

``` bash
gofmt -w .
go vet ./...
go test ./...
```

------------------------------------------------------------------------

## Concurrency & Storage Rules

coldkeep uses:

-   Per-file PostgreSQL transactions
-   `SELECT ... FOR UPDATE SKIP LOCKED`
-   Deterministic container append logic

When modifying storage logic:

-   Do not remove transactional boundaries
-   Do not introduce partial metadata commits
-   Do not bypass container locking semantics
-   Do not introduce non-deterministic container writes
-   Preserve chunk ordering guarantees

Canonical append lifecycle contract:

-   Use the state machine comment in
    [`internal/storage/store.go`](internal/storage/store.go) ("Append lifecycle state machine")
    as the single source of truth for append/rollback/commit-ack/failure handling.
    Writer implementation comments should remain pointers to this contract.

If unsure, open a discussion before implementing changes.

------------------------------------------------------------------------

## Testing Guidelines

-   Unit tests should not require Docker
-   Integration tests must be explicitly gated by environment variables
-   Avoid flaky timing-based tests
-   Prefer deterministic inputs

For v0.9, changes that affect storage, restore, verification, recovery, GC,
or CLI contracts are expected to pass the full GitHub Actions pipeline:

-   quality
-   integration
-   smoke
-   the aggregate `CI Required Gate`

If adding storage logic, include at least one restore verification test.

### Quick Start: Local Test Runs For New Contributors

Use this sequence to run tests with the same DB-backed setup used by integration checks.

1. Start Postgres:

``` bash
docker compose up -d postgres
```

1. Export integration environment once in your shell:

``` bash
export COLDKEEP_TEST_DB=1
export COLDKEEP_CODEC=plain
export DB_HOST=127.0.0.1
export DB_PORT=5432
export DB_USER=coldkeep
export DB_PASSWORD=coldkeep
export DB_NAME=coldkeep
export DB_SSLMODE=disable
```

1. Run correctness tier first (fast signal, stress tests skipped):

``` bash
go test ./tests -short -count=1 -v -timeout 20m
```

1. Run full integration suite (includes stress-tier coverage):

``` bash
go test ./tests -count=1 -v -timeout 20m
```

1. Run full repository suite before opening a PR:

``` bash
go test ./... -count=1 -timeout 25m
```

Optional focused run while working on doctor behavior:

``` bash
go test ./tests -run 'TestDoctor(Command|JSONContractConsistency|FailureJSONContractAndStreams)$' -count=1 -v
```

Recommended focused runs for v0.10 lifecycle/reuse hardening changes:

``` bash
go test ./tests -run 'TestReuseRefusesSemanticallyCorruptedCompletedFile|TestGCRestorePinRaceContainerNotDeleted|TestVerifySystemDeepDetectsTrailingBytesAfterLastBlock|TestStartupRecoveryQuarantinesSealingContainerWithGhostBytesAndGCSkipsIt' -count=1 -v
```

If you modify startup recovery, reuse validation, verify semantics, restore
pinning, or GC locking behavior, include at least one targeted regression test
for the changed contract in your PR.

If your shell does not keep exported variables between commands, prefix each test command with the environment variables directly.

------------------------------------------------------------------------

## Submitting Changes

1.  Fork the repository
2.  Create a feature branch
3.  Keep commits small and focused
4.  Run formatting and tests
5.  Open a Pull Request

Pull requests should include:

-   A clear description of the change
-   Why it is needed
-   Tradeoffs introduced
-   Any schema impact (if applicable)

Repository maintainers should protect `main`, `release/**`, and `hotfix/**`
with required status checks and require the exact final job name
`CI Required Gate`. That enforcement is configured in GitHub repository
settings or rulesets, not in the source tree.

To keep this auditable, use the GitHub rule names `Protect mainline branches`
and `Protect release tags`, then verify them with:

``` bash
scripts/audit_ci_enforcement.sh --repo franchoy/coldkeep
```

Breaking changes must include migration notes.

------------------------------------------------------------------------

## Reporting Bugs

When opening an issue, include:

-   coldkeep version (commit hash if possible)
-   Exact command used
-   Full console output
-   Steps to reproduce
-   Expected vs. actual behavior

If the issue involves potential corruption or security impact, see
SECURITY.md before filing publicly.

------------------------------------------------------------------------

## Architectural Discussions

For significant changes (e.g., encryption, container format redesign,
background compaction, resumable uploads):

Please open a design discussion issue first.

coldkeep is evolving toward:

-   Block-based container layout
-   Authenticated metadata
-   Encryption support
-   Background verification / compaction
-   Cloud backend experimentation

------------------------------------------------------------------------

## Not in Scope for V0

The following are unlikely to be accepted in V0:

-   Large framework rewrites
-   ORM introduction
-   Non-deterministic storage logic
-   Breaking schema changes without migration strategy
-   Heavy dependency additions without strong justification

------------------------------------------------------------------------

Thank you for contributing to coldkeep.