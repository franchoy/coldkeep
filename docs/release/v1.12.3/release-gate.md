# v1.12.3 - Release Train Closure Hygiene

## Release Summary

- Release name: `v1.12.3 - Release Train Closure Hygiene`
- Branch: `release/v1.12.3`
- Base branch: `main`
- Commit range: `main..release/v1.12.3`
- Included phases:
  - Phase 0 - Scope Lock
  - Phase 1 - v1.12 Release Train Status Cleanup
  - Phase 2 - Generated Validation Artifact Cleanup
  - Phase 3 - Final Hygiene Release Gate

## Scope Outcome

Behavior changes: none.

Documentation changes:

- Added the v1.12.3 release-tracking baseline.
- Corrected stale `docs/release/v1.12/*` release-train statuses to reflect the released v1.12
  state.
- Preserved intentionally deferred v1.12 operations without reclassifying them as complete.
- Added the v1.13 handoff note for engine contracts, catalog contracts, error taxonomy,
  dependency-direction checks, and engine-level invariants.
- Recorded final release-gate evidence in this document.

Generated artifact cleanup:

- Removed tracked generated root validation outputs:
  - `benchmark-none-w1.json`
  - `benchmark-none-w4.json`
  - `benchmark-zstd-w1.json`
  - `benchmark-zstd-w4.json`
  - `regression-report-none-w1.json`
  - `regression-report-none-w4.json`
  - `regression-report-zstd-w1.json`
  - `regression-report-zstd-w4.json`
- Preserved tracked benchmark baselines:
  - `benchmark-baseline.json`
  - `benchmark-baseline-w4.json`
  - `benchmark-baseline-committed.json`
- Added `.gitignore` protection for generated local validation outputs:
  - `benchmark-none-*.json`
  - `benchmark-zstd-*.json`
  - `regression-report-*.json`
  - `artifacts/`

## Compatibility Statement

v1.12.3 is a hygiene-only patch release. It introduces no Go code, test, script, CI, parser, CLI,
JSON, exit-code, schema, repository-format, storage-format, default-backend, engine, catalog, or
migration behavior changes.

## Validation Commands

The final hygiene release gate was run in this order:

```bash
git status -sb
git log --oneline main..release/v1.12.3
git ls-files benchmark-none-w1.json benchmark-none-w4.json benchmark-zstd-w1.json benchmark-zstd-w4.json regression-report-none-w1.json regression-report-none-w4.json regression-report-zstd-w1.json regression-report-zstd-w4.json
git ls-files benchmark-baseline.json benchmark-baseline-w4.json benchmark-baseline-committed.json
gofmt -l $(git ls-files '*.go')
golangci-lint run ./...
go vet ./...
go test -count=1 ./...
go test -race -count=1 ./...
git diff --check
git status -sb
```

## Validation Results

1. `git status -sb` -> pass (`## release/v1.12.3...origin/release/v1.12.3`)
2. `git log --oneline main..release/v1.12.3` -> pass
   - `65ccfab v1.12.3 phase 2: ignore generated validation artifacts`
   - `7d994fa v1.12.3 phase 1: close v1.12 release train docs`
   - `c00f0c9 v1.12.3 phase 0: add release train closure baseline`
3. Generated artifact tracking check -> pass (no listed generated root artifacts are still tracked)
4. Benchmark baseline tracking check -> pass
   - `benchmark-baseline.json`
   - `benchmark-baseline-w4.json`
   - `benchmark-baseline-committed.json`
5. `gofmt -l $(git ls-files '*.go')` -> pass (no output)
6. `golangci-lint run ./...` -> pass (`0 issues.`)
7. `go vet ./...` -> pass
8. `go test -count=1 ./...` -> pass
9. `go test -race -count=1 ./...` -> pass
10. `git diff --check` -> pass
11. `git status -sb` -> pass (`## release/v1.12.3...origin/release/v1.12.3`)

## Residual Risks

- No product-scope residual risk remains open within the approved v1.12.3 hygiene scope.
- Standard merge-path risk remains external to this branch: PR review and `main` CI must still be
  green before merge and tag creation.

## Decisions

Merge readiness decision: ready to open a PR.

PR readiness decision: ready, because the final hygiene release gate is green and the branch
remains hygiene-only.

Release tag recommendation: recommend tag `v1.12.3` after PR approval and green `main` CI.

v1.12.x closure decision: the v1.12.x release train is ready to close after v1.12.3 if PR review
and `main` CI are green.
