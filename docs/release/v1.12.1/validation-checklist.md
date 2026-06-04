# Coldkeep v1.12.1 Validation Checklist

Release name: v1.12.1 - Post-Migration CLI Contract Hardening

All phases stay on `release/v1.12.1` until the final release gate is green.

## Phase-Level Validation

For every implementation phase:

- Confirm the phase objective and excluded scope before editing code.
- Add focused tests for every command behavior changed.
- Include accepted and rejected CLI forms in tests.
- Check routed-command parity when a routed command is touched.
- Run the focused package tests for changed code.
- Run the standard test suite before closing the phase.
- Update the risk register when a phase changes user-facing behavior.

Phase 1 selected command checks:

- `coldkeep init extra` is rejected.
- `coldkeep version extra` is rejected.
- `coldkeep help extra` is rejected.
- `coldkeep verify system extra` is rejected.
- `coldkeep snapshot stats <snapshotID> extra` is rejected while `snapshot stats <snapshotID>`
  remains valid.
- `coldkeep repair ref-counts extra` is rejected.
- Search, simulate, benchmark, snapshot create, and snapshot delete are not changed in Phase 1.

Phase 2 selected empty-value checks:

- `coldkeep search --name ""` and `coldkeep search --name "   "` are rejected.
- `coldkeep search --path ""` and `coldkeep search --path "   "` are rejected.
- `coldkeep search --extension ""` and `coldkeep search --extension "   "` are rejected.
- `coldkeep snapshot list --path ""` and `coldkeep snapshot list --path "   "` are rejected.
- `coldkeep remove --stored-path ""` and `coldkeep remove --stored-path "   "` are rejected
  before ID remove handling.
- `coldkeep restore --stored-path ""` and `coldkeep restore --stored-path "   "` are rejected
  before ID restore handling.
- `coldkeep snapshot create --id ""` and `coldkeep snapshot create --id "   "` are rejected
  instead of generating an implicit snapshot ID.
- Valid non-empty command forms remain unchanged.
- Snapshot tag normalization and benchmark behavior are not changed in Phase 2.

Phase 3 selected boolean checks:

- `coldkeep list --reverse=false` does not enable reverse behavior; `--reverse` remains unsupported
  for `list`.
- `coldkeep snapshot list --reverse=false` does not enable reverse behavior; `--reverse` remains
  unsupported for `snapshot list`.
- `coldkeep snapshot delete <id> --force=false` is rejected and does not perform forced delete.
- `coldkeep snapshot delete <id> --dry-run=false` is rejected and does not perform dry-run preview.
- Bare `--force` still performs snapshot delete.
- Bare `--dry-run` still performs snapshot delete preview.
- `--force=true` still performs snapshot delete.
- `--dry-run=true` still performs snapshot delete preview.
- Unrelated boolean flags are not changed in Phase 3.

Phase 4 selected JSON shorthand checks:

- `coldkeep list --json` resolves JSON mode like `coldkeep list --output json`.
- `coldkeep search --json` resolves JSON mode like `coldkeep search --output json`.
- `coldkeep remove --json` resolves JSON mode like `coldkeep remove --output json`.
- `coldkeep gc --json` resolves JSON mode like `coldkeep gc --output json`.
- `coldkeep config get <key> --json` resolves JSON mode like
  `coldkeep config get <key> --output json`.
- `coldkeep snapshot stats <snapshotID> --json` resolves JSON mode like
  `coldkeep snapshot stats <snapshotID> --output json`.
- JSON envelope shape remains unchanged for selected command payloads.
- Unsupported `benchmark --json` remains rejected.
- Store, restore, simulate, and benchmark JSON side-channel cleanup is not changed in Phase 4.

Phase 5 static-analysis closure checks:

- `golangci-lint run ./...` reports zero issues.
- `go vet ./...` passes.
- If both are green, Phase 5 remains docs-only.
- Do not perform production cleanup for style-only or speculative Codacy findings.
- Do not perform parser, storage, restore, GC, verify, engine, schema, storage-format, or
  default-backend refactors in Phase 5.
- Defer any non-blocking cleanup to a later release or a dedicated cleanup phase.

Phase 6 final release gate status:

- `git status -sb` passed and was clean at gate start.
- `git log --oneline main..release/v1.12.1` listed the six phase commits.
- `gofmt -w $(git ls-files '*.go')` completed with no resulting changes.
- `gofmt -l $(git ls-files '*.go')` was empty.
- `golangci-lint run ./...` passed with `0 issues`.
- `go vet ./...` passed.
- `go test -count=1 ./...` passed.
- `go test -race -count=1 ./...` passed.
- `git diff --check` passed.
- `git status -sb` passed and was clean after validation.
- `docs/release/v1.12.1/release-gate.md` records the final gate evidence.

Phase 0 validation:

- `git diff --check`
- Optional markdown lint when available.
- Confirm no Go files changed.

## Full Release Gate

The mandatory final gate is:

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

The release is not ready until every command above passes and the resulting status contains only
expected release changes.

## No-Go Checks

Do not proceed to final release if any of these are present:

- New engine migrations.
- Recursive or folder store migration.
- Stored-path restore migration.
- Snapshot restore migration.
- Stored-path remove migration.
- Repair or recovery migration.
- Schema changes.
- Repository format changes.
- Storage format changes.
- Default backend changes.
- SQLite-first default switch.
- Daemon, API, UI, NAS, or cloud work.
- Broad parser rewrite.
- Broad `main.go` rewrite.
- Style-only Codacy chasing.
- Architecture refactors.
- Version updates before the final release phase.
- Root README changes without explicit approval.

## Behavior Compatibility Checks

- Valid command invocations from v1.12.0 continue to work.
- New validation failures are limited to ignored positional arguments, empty values, invalid boolean
  values, or documented malformed `--json` forms.
- Error messages are clear enough for CLI users and automation logs.
- JSON output for valid automation flows remains stable.
- Boolean flag behavior is predictable for implicit true and explicit true/false forms.
- Routed and non-routed command paths agree for the same user-facing contract.

## Docs Checks

- Phase notes identify objective, scope, exclusions, tests, and acceptance criteria.
- Risk register entries are updated when behavior risk changes.
- Release notes do not claim readiness before Phase 6 is green.
- Documentation stays specific to v1.12.1 patch hardening.
- Phase 0 remains docs-only: no Go files, root README, or version files changed.
