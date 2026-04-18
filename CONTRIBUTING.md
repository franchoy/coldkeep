# Contributing to coldkeep

Thank you for your interest in contributing to coldkeep.

coldkeep v1.x is a correctness-first storage engine.

The project currently has two explicit correctness layers:

- storage correctness (v1.0 core)
- interaction correctness for CLI/automation contracts (v1.1 layer)

The project originated as a research prototype, and continues to
prioritize correctness, determinism, and clarity over feature velocity.
We welcome improvements, bug reports, architectural discussions, and
thoughtful critiques.

## New Contributor Reading Order

If you are new to coldkeep, read docs in this order:

1. [`README.md`](README.md) for project scope, quickstart, and command basics.
2. [`ARCHITECTURE.md`](ARCHITECTURE.md) for internals: data model, invariants, lifecycle, recovery, and trust assumptions.
3. [`VALIDATION_MATRIX.md`](VALIDATION_MATRIX.md) for guarantee-to-evidence mapping (G1-G9).
4. [`PRE_RELEASE_CHECKLIST.md`](PRE_RELEASE_CHECKLIST.md) for CI-parity and release readiness workflow.

This order minimizes cognitive load while preserving full access to the correctness model.

------------------------------------------------------------------------

## Project Philosophy

coldkeep v1.x prioritizes:

-   Correctness over performance
-   Deterministic storage behavior
-   Deterministic interaction behavior for automation workflows
-   Transactional metadata safety
-   Clear and readable code
-   Simplicity over abstraction

Stability and conceptual clarity remain more important than feature velocity.

Major architectural changes should be discussed before implementation.

## v1.x Philosophy: Correctness Over Feature Velocity

The v0.10 validation phase established a contribution posture that remains important across v1.x.
Contributions should prioritize:

-   strengthening storage invariants and correctness guarantees
-   preserving stable machine-readable CLI contracts for automation
-   preserving G9 interface correctness guarantees for CLI and automation
-   improving stress and adversarial validation coverage, including long-run tests
-   identifying and reproducing edge-case failures
-   improving observability and operator-facing behavior

New storage features or architectural changes are discouraged unless they address
a demonstrated correctness issue.

Changes that alter storage invariants or lifecycle semantics must include
adversarial or long-run test coverage demonstrating correctness.

This focus preserves system reliability as the primary delivery criterion.

------------------------------------------------------------------------

## Development Requirements

-   Go 1.23+ (or the version specified in go.mod)
-   PostgreSQL 14+
-   Docker (recommended for reproducibility)

------------------------------------------------------------------------

## Running with Docker (Recommended)

Start PostgreSQL:

``` bash
docker compose up -d coldkeep_postgres
```

Build the application image:

``` bash
docker compose build
```

Run commands:

``` bash
docker compose run --rm coldkeep stats
docker compose run --rm coldkeep store-folder samples
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

For v1.x, changes that affect storage, restore, verification, recovery, GC,
batch orchestration, or CLI contracts are expected to pass the full GitHub Actions pipeline:

-   quality
-   integration-correctness
-   integration-stress
-   smoke
-   the aggregate `CI Required Gate`

The repository also has a separate `integration-long-run` soak job for extended
stability coverage. It is intentionally isolated from the required gate so it
can be enabled, tuned, or temporarily disabled without changing the standard
correctness/stress merge path.

If adding storage logic, include at least one restore verification test.

Maintainers preparing a release should also run the
[`PRE_RELEASE_CHECKLIST.md`](PRE_RELEASE_CHECKLIST.md) flow.

### New Contributor Path: Before You Open a PR

Most CI failures for first-time contributors come from only running a subset of checks locally.
Use the flow below to mirror the required GitHub CI jobs before opening a PR.

1. Start PostgreSQL and clean local test artifacts:

``` bash
docker compose up -d coldkeep_postgres
bash scripts/clean_test_storage.sh
```

2. Export shared test environment:

``` bash
export COLDKEEP_TEST_DB=1
export COLDKEEP_DB_AUTO_BOOTSTRAP=true
export DB_HOST=127.0.0.1
export DB_PORT=5432
export DB_USER=coldkeep
export DB_PASSWORD=coldkeep
export DB_NAME=coldkeep
export DB_SSLMODE=disable
export COLDKEEP_KEY=0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef
```

`COLDKEEP_CODEC` is intentionally not exported globally here because the local CI
simulation loop sets it per run (`plain` then `aes-gcm`).
`COLDKEEP_KEY` is only required when codec is `aes-gcm`; it is ignored by `plain`.

If you do not want auto-bootstrap, apply `db/schema_postgres.sql` manually first.

3. Run the quality job equivalent (same intent as CI `quality`):

Run this block from a clean working tree when possible.
If `go mod tidy && git diff --exit-code` fails while you have local edits,
that indicates uncommitted diff in your workspace, not necessarily a test failure.

``` bash
go mod tidy && git diff --exit-code

unformatted=$(gofmt -l $(git ls-files '*.go'))
if [ -n "$unformatted" ]; then
    echo "Unformatted Go files detected:"
    echo "$unformatted"
    exit 1
fi

bash -n scripts/*.sh
bash scripts/validate_validation_matrix.sh

# Requires golangci-lint in PATH (same linter family as CI)
golangci-lint run ./...

go vet ./...

COLDKEEP_CODEC=plain go test -race -count=1 ./cmd/... ./internal/...
COLDKEEP_CODEC=aes-gcm COLDKEEP_KEY="$COLDKEEP_KEY" go test -race -count=1 ./cmd/... ./internal/...

go build ./...
bash scripts/audit_ci_enforcement.sh --local-only
go build -o coldkeep ./cmd/coldkeep
```

4. Run full required CI matrix locally (all required gate jobs, both codecs):

``` bash
for codec in plain aes-gcm; do
    echo "=== Codec: ${codec} ==="
    export COLDKEEP_CODEC="$codec"

    # integration-correctness
    go test -race -count=1 -short ./tests/integration/...

    # integration-stress
    go test -race -count=1 ./tests/integration/...

    # integration-long-run
    COLDKEEP_LONG_RUN=1 go test -race -count=1 ./tests/integration/... -run 'TestStoreGCVerifyRestoreDeleteLoopStability|TestRandomizedLongRunLifecycleSoak'

    # adversarial
    COLDKEEP_LONG_RUN=1 go test -race -count=1 ./tests/adversarial/...

    # smoke
    # New contributors: if smoke setup fails, see README "Smoke Validation (Two Approaches)"
    # for both Docker-runner and host-runner workflows.
    COLDKEEP_SMOKE_RESET_DB=1 \
    COLDKEEP_SCHEMA_PATH=db/schema_postgres.sql \
    COLDKEEP_STORAGE_DIR="$PWD/.ci-storage/${codec}" \
    COLDKEEP_SMOKE_SCHEMA_MESSAGE_GATE=1 \
    PATH="$PWD:$PATH" \
    bash scripts/smoke.sh
done
```

This is the closest local approximation of what must pass for `CI Required Gate`.

If you prefer to run smoke outside this loop (or need troubleshooting), use the
dual guidance in [`README.md`](README.md) under "Smoke Validation (Two Approaches)".

5. Optional but recommended full sweep before pushing:

``` bash
go test ./... -count=1 -timeout 25m
```

Optional focused run while working on doctor behavior:

``` bash
go test ./tests -run 'TestDoctor(Command|JSONContractConsistency|FailureJSONContractAndStreams)$' -count=1 -v
```

Recommended focused runs for lifecycle/reuse hardening changes:

``` bash
go test ./tests -run 'TestReuseRefusesSemanticallyCorruptedCompletedFile|TestGCRestorePinRaceContainerNotDeleted|TestVerifySystemDeepDetectsTrailingBytesAfterLastBlock|TestStartupRecoveryQuarantinesSealingContainerWithGhostBytesAndGCSkipsIt' -count=1 -v
```

If you modify startup recovery, semantic reuse validation, verify semantics, restore
pinning, or GC locking behavior, include at least one targeted regression test
for the changed contract in your PR.

If your shell does not keep exported variables between commands, prefix each command with the needed environment variables.

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

coldkeep continues to evolve in areas such as:

-   Block-layout refinements and format hardening
-   Authenticated metadata
-   Key rotation and encryption lifecycle hardening
-   Background verification / compaction
-   Cloud backend experimentation

------------------------------------------------------------------------

## Not in Scope for v1.x

The following are unlikely to be accepted in v1.x:

-   Large framework rewrites
-   ORM introduction
-   Non-deterministic storage logic
-   Breaking schema changes without migration strategy
-   Heavy dependency additions without strong justification

------------------------------------------------------------------------

Thank you for contributing to coldkeep.