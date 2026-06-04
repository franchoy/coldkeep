# Coldkeep v1.12.1 Final Release Gate

Release name: v1.12.1 - Post-Migration CLI Contract Hardening
Recommended tag: `v1.12.1`
Branch: `release/v1.12.1`
Base branch: `main`
Gate date: 2026-06-04

## Commit Range

Range: `main..release/v1.12.1`

```text
cc8591b v1.12.1 phase 5: record static analysis cleanup closure
d4ce8ea v1.12.1 phase 4: accept selected json shorthand flags
32714d1 v1.12.1 phase 3: honor selected explicit false boolean flags
340980e v1.12.1 phase 2: reject selected empty flag values
2009553 v1.12.1 phase 1: reject selected extra positional args
d0e7909 v1.12.1 phase 0: add patch release scope baseline
```

## Included Phases

- Phase 0 - Scope Lock and Release Baseline.
- Phase 1 - Extra Positional Argument Rejection.
- Phase 2 - Empty Filter / Empty Flag Rejection.
- Phase 3 - Boolean Flag Value Semantics.
- Phase 4 - JSON Shorthand Consistency.
- Phase 5 - Safe Codacy / Static Analysis Cleanup.
- Phase 6 - Final Patch Release Gate.

## Behavior Changes

v1.12.1 is a patch-level CLI contract hardening release. Valid command behavior is intended to
remain unchanged.

Selected malformed commands now fail earlier:

- Extra positional arguments for selected commands.
- Empty or whitespace-only selected filter and flag values.
- Empty or whitespace-only `--stored-path`.
- Empty or whitespace-only `snapshot create --id`.

Phase 3 and Phase 4 were contract-lock phases: no production code changed in those phases. JSON
envelope shape is unchanged. Exit-code classes are unchanged except that selected malformed
commands now correctly return usage failure.

## Compatibility Statement

No changes were made to:

- Schema.
- Repository format.
- Storage format.
- Default backend.
- Engine/catalog architecture.
- Packed or legacy storage semantics.
- Daemon, API, UI, NAS, cloud, or multi-user behavior.

## Validation Commands

Final validation was run in this order:

```bash
git status -sb
git log --oneline main..release/v1.12.1
gofmt -w $(git ls-files '*.go')
gofmt -l $(git ls-files '*.go')
golangci-lint run ./...
go vet ./...
go test -count=1 ./...
go test -race -count=1 ./...
git diff --check
git status -sb
```

## Validation Results

- `git status -sb`: clean at gate start.
- `git log --oneline main..release/v1.12.1`: six phase commits listed.
- `gofmt -w $(git ls-files '*.go')`: completed with no resulting changes.
- `gofmt -l $(git ls-files '*.go')`: empty output.
- `golangci-lint run ./...`: passed with `0 issues`.
- `go vet ./...`: passed.
- `go test -count=1 ./...`: passed.
- `go test -race -count=1 ./...`: passed.
- `git diff --check`: passed.
- `git status -sb`: clean after validation.

## Residual Risks

- v1.12.1 intentionally covers only selected CLI validation and automation contract cases, not a
  complete CLI parser audit.
- JSON side-channel cleanup for store, restore, simulate, and benchmark remains deferred.
- Benchmark validation hardening remains deferred.
- Snapshot tag normalization remains deferred.
- Broad parser rewrite and architecture cleanup remain out of scope.
- New engine migrations and deferred operation migrations remain out of scope.

## Merge Readiness Decision

Decision: ready to open a PR for final review and merge.

This decision is based on a green final release gate, docs-only Phase 6 changes, no production code
changes in the final phase, and explicit residual-risk documentation.

## Release Tag Recommendation

Recommended tag after merge: `v1.12.1`
