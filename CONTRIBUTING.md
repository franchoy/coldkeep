# Contributing to coldkeep

Thank you for your interest in contributing to coldkeep.

coldkeep V0 is an experimental deduplicated storage engine prototype.
We welcome improvements, bug reports, and architectural discussions.

---

## Project Goals (V0)

coldkeep V0 focuses on:

- Correctness over performance
- Transactional metadata safety
- Deterministic container layout
- Safe concurrent folder ingestion
- Clear and readable code

V0 is intentionally simple. Major architectural changes should be discussed before implementation.

---

## Development Environment

### Requirements

- Go 1.23+
- PostgreSQL 14+
- Docker (recommended)

---

## Running Locally (Docker Recommended)

Start PostgreSQL:

    docker compose up -d postgres

Build coldkeep:

    docker compose build

Run commands:

    docker compose run --rm app ./coldkeep stats
    docker compose run --rm app ./coldkeep store-folder /input

---

## Running Without Docker

Set environment variables:

    export DB_HOST=localhost
    export DB_PORT=5432
    export DB_USER=coldkeep
    export DB_PASSWORD=coldkeep
    export DB_NAME=coldkeep

Initialize schema:

    psql -U coldkeep -d coldkeep -f db/init.sql

Build:

    go build -o coldkeep ./app

---

## Code Style

- Code must pass gofmt
- Keep functions small and readable
- Avoid premature optimization
- Prefer clarity over cleverness
- No hidden concurrency patterns

Run:

    gofmt -w .
    go vet ./...

---

## Concurrency Rules

coldkeep uses:

- Per-file database transactions
- SELECT ... FOR UPDATE SKIP LOCKED
- Container rotation logic

When modifying storage logic:

- Do not remove transactional boundaries
- Do not introduce partial metadata commits
- Do not bypass container locking semantics
- Keep container writes deterministic

If unsure, open a discussion before implementing.

---

## Submitting Changes

1. Fork the repository
2. Create a feature branch
3. Keep commits small and focused
4. Run formatting tools
5. Open a Pull Request

Pull requests should include:

- Clear description of the change
- Why it is needed
- Any tradeoffs introduced

---

## Reporting Bugs

Open an issue with:

- coldkeep version
- Exact command used
- Console output
- Steps to reproduce

If the issue involves data corruption or security, see SECURITY.md.

---

## Architectural Changes

For significant changes (e.g., encryption, container format change, resumable uploads):

Please open a design discussion issue first.

coldkeep is evolving toward:

- Block-based layout
- Authenticated metadata
- Encryption
- Background compaction

---

## What We Are Not Accepting (V0)

- Large framework rewrites
- ORM introduction
- Breaking schema changes without migration plan
- Non-deterministic storage logic

---

Thank you for contributing to coldkeep.
