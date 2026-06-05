# Coldkeep v1.12.2 Final Patch Release Gate

Release name: v1.12.2 - CLI Validation Follow-up
Recommended tag: `v1.12.2`
Branch: `release/v1.12.2`
Base branch: `main`
Gate date: 2026-06-05

## Commit Range

Range: `main..release/v1.12.2`

```text
2b04b3e v1.12.2 phase 3: add empty-value parser-path coverage
74cfcbe v1.12.2 phase 2: align search extension parser validation
0c11a11 v1.12.2 phase 1: fix v1.12.1 release status docs
a1665ee v1.12.2 phase 0: add CLI validation follow-up baseline
```

## Included Phases

- Phase 0 - Scope Lock.
- Phase 1 - v1.12.1 Release Documentation Status Cleanup.
- Phase 2 - `search --extension` Parser Alignment.
- Phase 3 - Empty-Value Parser-Path Regression Coverage.
- Phase 4 - Final Patch Release Gate.

## Behavior Changes

v1.12.2 is a hygiene-only CLI validation follow-up patch.

- v1.12.1 release documentation status now reflects that v1.12.1 was completed, merged, and
  tagged.
- `search --extension` is treated as a value-taking flag by the parser so empty and blank values
  are rejected through the real parser path.
- `search --extension .txt` remains unsupported; extension search was not implemented.
- Parser-path regression coverage now proves selected v1.12.1 empty-value validation cases through
  `parseCommandLine` before command dispatch.

No new CLI validation families were added.

## Compatibility Statement

No changes were made to:

- Search matching semantics.
- Extension matching, case normalization, or leading-dot normalization.
- JSON output behavior.
- Exit-code classes.
- Benchmark behavior.
- Snapshot tag normalization.
- Schema.
- Repository format.
- Storage format.
- Default backend.
- Engine, catalog, or storage architecture.
- Daemon, API, UI, NAS, cloud, or v1.13 work.

## Validation Commands

Final validation was run in this order:

```bash
git status -sb
git log --oneline main..release/v1.12.2
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
- `git log --oneline main..release/v1.12.2`: four phase commits listed.
- `gofmt -w $(git ls-files '*.go')`: completed with no resulting changes.
- `gofmt -l $(git ls-files '*.go')`: empty output.
- `golangci-lint run ./...`: passed with `0 issues`.
- `go vet ./...`: passed.
- `go test -count=1 ./...`: passed.
- `go test -race -count=1 ./...`: passed.
- `git diff --check`: passed.
- `git status -sb`: clean after validation.

## Residual Risks

- v1.12.2 intentionally remains narrow and does not implement extension search.
- Broader parser redesign remains deferred.
- Benchmark validation hardening remains deferred.
- Snapshot tag normalization remains deferred.
- JSON side-channel cleanup remains deferred.
- Engine/catalog/storage migrations and v1.13 architecture work remain deferred.

## Merge Readiness Decision

Decision: ready to open a PR for final review and merge.

This decision is based on a green final patch release gate, no production code changes in Phase 4,
explicit compatibility documentation, and closure evidence for all v1.12.2 risks.

## Release Tag Recommendation

Recommended tag after merge: `v1.12.2`
