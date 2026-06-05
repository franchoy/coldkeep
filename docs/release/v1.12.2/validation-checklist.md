# v1.12.2 Validation Checklist

## Phase-Level Validation

- Phase 0: run `git diff --check`; run markdown lint if available.
- Phase 1: run `git diff --check`; run markdown lint if available. Closure requires confirming
  no Go files, root README, version files, CLI behavior, parser behavior, or tests changed.
- Phase 2: run focused parser-path tests covering `search --extension` empty values and any existing relevant CLI/parser tests.
  Closure requires confirming `search --extension .txt` remains unsupported and
  `search --extension` without a value is rejected as a missing value.
- Phase 3: run the new selected empty-value parser-path regression tests and existing relevant CLI/parser tests.
  Closure requires confirming selected empty-value cases parse through `parseCommandLine` before
  command validation rejects them.
- Phase 4: run `git diff --check`; run markdown lint if available. Closure requires confirming
  `PRE_RELEASE_CHECKLIST.md` has Profile A / B / C, current CI local mirrors, reusable unchecked
  sign-off boxes, and no code/test/script/CI behavior changes.
- Phase 5: run Profile A from `PRE_RELEASE_CHECKLIST.md`, then run the direct full release gate.
  Closure evidence is recorded in `docs/release/v1.12.2/release-gate.md`.

## Full Release Gate

Mandatory final gate:

```bash
gofmt -w $(git ls-files '*.go')
gofmt -l $(git ls-files '*.go')
golangci-lint run ./...
go vet ./...
go test -count=1 ./...
go test -race -count=1 ./...
git diff --check
git status -sb
```

The release is ready for PR review after the Phase 5 gate is green and the result is recorded.

## No-Go Checks

- No new CLI validation families.
- No broad parser rewrite.
- No benchmark validation hardening.
- No snapshot tag normalization.
- No JSON side-channel cleanup.
- No engine, catalog, storage, schema, repository-format, or backend changes.
- No recursive or folder store migration.
- No stored-path restore, snapshot restore, stored-path remove, repair, or recovery migration.
- No daemon, API, UI, NAS, or cloud work.
- No v1.13 implementation work.
- No unrelated Codacy cleanup.

## Behavior Compatibility Checks

- `search --extension` empty-value validation follows the real parser path.
- Unsupported-flag semantics remain unchanged unless a phase explicitly records a compatibility-safe adjustment.
- Parser-path tests assert user-visible CLI behavior rather than private implementation details.
- No storage, catalog, engine, repository format, schema, or backend behavior changes are introduced.

## Docs Checks

- v1.12.1 documentation status cleanup is factual and does not overclaim release readiness.
- v1.12.2 docs remain hygiene-only and actionable.
- All v1.12.2 phases stay on `release/v1.12.2` until the final release gate is green.
- Phase documents preserve the distinction between planning, implementation, validation, and release/tag readiness.
