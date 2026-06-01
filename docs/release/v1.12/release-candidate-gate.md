# v1.12 Release Candidate Gate

## Status

Status: pending (in progress)
Branch: release/v1.12
Base: main
Date: 2026-06-01

Gate invariant: no release claim without full validation evidence.

## Commit range

```bash
git log --oneline main..release/v1.12
```

Latest confirmed head:

- 6374815 phase11: add invariant matrix and parity proof updates

## Routed operations (final v1.12 scope)

- stats
- inspect
- verify (system/file)
- snapshot list/show/stats/diff
- gc routed scope (dry-run/live with SQLite live refusal guard)
- restore by ID (live/dry-run)
- store single-file
- remove by ID (live/dry-run)

## Deferred operations (intentionally unchanged)

- recursive/folder store
- stored-path restore
- snapshot restore
- stored-path remove / stored-paths remove
- repair routing
- recovery routing
- full restore-plan / placement / gc-plan catalog activation where deferred

## Risk summary

Current release-train risk posture from risk register:

- CK-112-R001 fixed
- CK-112-R002 fixed
- CK-112-R003 fixed
- CK-112-R004 open (partial, deferred-operation scope)
- CK-112-R005 open (partial, deferred-operation scope)
- CK-112-R006 mitigated (partial, deferred-operation scope)
- CK-112-R007 mitigated

No evidence of a newly introduced S0/S1 blocker in routed scope from completed Phase 12 checks so far.

## Validation evidence

### Mandatory Phase 12 gate (required order)

1. `git status -sb` -> pass (clean)
2. `git log --oneline main..release/v1.12` -> pass (expected phase commits present)
3. `gofmt -w $(git ls-files '*.go')` -> pass
4. `gofmt -l $(git ls-files '*.go')` -> pass (empty)
5. `golangci-lint run ./...` -> pass (`0 issues.`)
6. `go vet ./...` -> pass
7. `go test -count=1 ./...` -> pass
8. `go test -race -count=1 ./...` -> pass
9. `git diff --check` -> pass
10. `git status -sb` -> pass (clean)

### PRE_RELEASE_CHECKLIST execution status

- Step 1 PostgreSQL startup/env: pass (compose service healthy).
- Step 2 quality-equivalent block: pass after installing missing `shellcheck` prerequisite.
  - Added environment prerequisite on this host: `shellcheck` installed via apt.
  - Included: smart-quote check, shell syntax check, shellcheck, matrix validation, row-writer scope check, lint, vet, race command blocks, builds, CI enforcement audit.
- Step 3 full CI matrix: in progress.
  - Initial attempt failed smoke due missing exported DB env in that command context and stale DB volume auth mismatch.
  - Corrective actions completed: DB volume reset, explicit DB env in command, `postgresql-client` (`psql`) and `jq` installed, host DB auth validated (`current_user=coldkeep`, `current_database=coldkeep`).
  - Re-run is ongoing in smaller deterministic commands to capture complete evidence cleanly.

## Accepted residual risks

Accepted residual risks are limited to intentionally deferred operations documented in:

- docs/release/v1.12/invariant-test-matrix.md
- docs/release/v1.12/risk-register.md

No deferred operation is being reclassified as complete in Phase 12.

## PR readiness decision

Decision: not ready yet (pending).

Conditions still required before go/no-go:

- complete Step 3 full CI matrix evidence (both codecs, smoke, benchmark regression checks)
- complete remaining pre-release checklist sections required by Phase 12
- confirm final branch clean status after all checklist runs
- finalize this gate document with full pass/fail table

## Notes

This document is intentionally evidence-first and does not claim release readiness until all required checklist and validation evidence is complete and green.
