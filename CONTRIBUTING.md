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

Build:

``` bash
go build -o coldkeep ./app
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

If unsure, open a discussion before implementing changes.

------------------------------------------------------------------------

## Testing Guidelines

-   Unit tests should not require Docker
-   Integration tests must be explicitly gated by environment variables
-   Avoid flaky timing-based tests
-   Prefer deterministic inputs

If adding storage logic, include at least one restore verification test.

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