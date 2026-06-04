# Coldkeep v1.12.1 Phase List

Release name: v1.12.1 - Post-Migration CLI Contract Hardening

All phases stay on `release/v1.12.1` until the final release gate is green.

## Phase 0 - Scope Lock and Release Baseline

Objective: establish the patch-release baseline and scope guardrails before implementation starts.

Included scope:

- Create the v1.12.1 release planning documents.
- Record scope, non-goals, validation expectations, and initial risks.
- Confirm the release is docs-only at Phase 0.

Excluded scope:

- Go code changes.
- Version file updates.
- Root README updates.
- PR creation or release readiness claims.

Expected tests:

- `git diff --check`
- Optional markdown lint when available.

Acceptance criteria:

- Required v1.12.1 planning documents exist under `docs/release/v1.12.1/`.
- No Go files changed.
- Scope and non-goals are explicit enough to block architecture drift.

## Phase 1 - Extra Positional Argument Rejection

Status: Complete

Objective: reject positional arguments that are currently ignored by user-facing commands.

Included scope:

- Reject extra positional arguments for the selected Phase 1 command batch:
  `init extra`, `version extra`, `help extra`, `verify system extra`,
  `snapshot stats <snapshotID> extra`, and `repair ref-counts extra`.
- Preserve the existing valid `snapshot stats <snapshotID>` contract.
- Preserve the existing valid `verify system <fast|standard|full|deep>` compatibility form while
  rejecting non-level extra tokens.
- Add focused validation tests covering accepted and rejected argument forms.

Excluded scope:

- Broad parser rewrite.
- Large `main.go` restructuring.
- Engine migration work.
- Changes to valid command output.

Expected tests:

- Focused CLI tests for each command touched.
- Routed-command parity tests where command routing is involved.
- Full standard test suite before phase closure.

Acceptance criteria:

- Extra positional arguments fail with a clear error.
- Existing valid invocations continue to pass.
- No out-of-scope architecture changes are introduced.
- Phase 1 does not expand into search, simulate, benchmark, snapshot create, or snapshot delete.

## Phase 2 - Empty Filter / Empty Flag Rejection

Status: Complete

Objective: reject empty filter and empty flag values when the value would be ambiguous, unsafe, or
previously accepted only by accident.

Included scope:

- Reject empty and whitespace-only values for the selected Phase 2 command batch:
  `search --name`, `search --path`, `search --extension`, `snapshot list --path`,
  `remove --stored-path`, `restore --stored-path`, and `snapshot create --id`.
- Prevent empty `--stored-path` from falling through to ID-based remove or restore modes.
- Prevent explicit empty `snapshot create --id` from silently generating an implicit snapshot ID.
- Add tests for empty, blank, and valid values where needed.

Excluded scope:

- New filter syntax.
- Schema or storage changes.
- Behavior changes for valid non-empty values.
- Broad CLI option redesign.
- Snapshot tag normalization.
- Benchmark JSON or NaN behavior.
- New non-empty `search --path`, `search --extension`, or `snapshot list --path` behavior.

Expected tests:

- Focused CLI tests for each affected flag or filter.
- Regression tests confirming valid values still work.
- Full standard test suite before phase closure.

Acceptance criteria:

- Empty values are rejected consistently where documented by the phase.
- Valid values retain previous behavior.
- Any intentional compatibility impact is recorded in the risk register.
- Phase 2 does not expand into snapshot tag normalization, benchmark behavior, schema work, storage
  format work, or parser rewrites.

## Phase 3 - Boolean Flag Value Semantics

Status: Complete

Objective: make boolean flag handling consistent for explicit values, implicit values, and invalid
values.

Included scope:

- Lock the selected Phase 3 boolean cases:
  `list --reverse=false`, `snapshot list --reverse=false`,
  `snapshot delete <id> --force=false`, and `snapshot delete <id> --dry-run=false`.
- Preserve existing bare and explicit-true behavior for selected supported snapshot delete flags.
- Preserve existing unsupported behavior for `list --reverse` and `snapshot list --reverse`; Phase 3
  does not add a reverse option to those commands.
- Add targeted regression tests for selected explicit-false and compatibility forms.

Excluded scope:

- Renaming flags.
- Removing existing valid boolean forms without evidence and risk review.
- Parser replacement or broad command rewrite.
- New reverse behavior for `list` or `snapshot list`.
- Changes to unrelated boolean flags.

Expected tests:

- Focused boolean flag tests for each changed command.
- Automation compatibility checks for accepted forms.
- Full standard test suite before phase closure.

Acceptance criteria:

- Boolean flags have predictable semantics across touched commands.
- Invalid boolean values fail clearly.
- Existing accepted automation forms remain compatible unless explicitly documented.
- Explicit false is not interpreted as true for the selected Phase 3 cases.
- Phase 3 does not expand into a broad parser rewrite or unrelated boolean cleanup.

## Phase 4 - JSON Shorthand Consistency

Status: Complete

Objective: make `--json` shorthand behavior consistent across user-facing commands.

Included scope:

- Lock selected Phase 4 shorthand cases:
  `list --json`, `search --json`, `remove --json`, `gc --json`,
  `config get <key> --json`, and `snapshot stats <snapshotID> --json`.
- Preserve existing `--output json` behavior and JSON envelope shapes.
- Add selected parity tests proving `--json` resolves to the same JSON mode as `--output json`.
- Keep unsupported `benchmark --json` rejected.

Excluded scope:

- JSON schema redesign.
- Output field additions unrelated to shorthand behavior.
- API, daemon, or UI output work.
- Store, restore, simulate, or benchmark JSON side-channel cleanup.
- Adding JSON support to commands that do not already support it.
- Broad output-mode rewrite.

Expected tests:

- Focused `--json` CLI tests for each command touched.
- Routed-command parity tests.
- Full standard test suite before phase closure.

Acceptance criteria:

- `--json` shorthand selects JSON output consistently.
- Existing valid JSON automation output remains stable.
- Any output-contract risk has mitigation or closure evidence.
- Phase 4 does not change JSON envelope shape, human output, schema, storage format, or backend
  defaults.

## Phase 5 - Safe Codacy / Static Analysis Cleanup

Status: Complete

Objective: address only behavior-preserving static-analysis findings that are safe inside a patch
release.

Included scope:

- Review current static-analysis state after Phases 1-4.
- Confirm `golangci-lint run ./...` reports zero issues locally.
- Confirm `go vet ./...` passes locally.
- Record that no production code cleanup was required for Phase 5.
- Defer any future Codacy or maintainability cleanup unless it is behavior-preserving and
  release-blocking.

Excluded scope:

- Style-only Codacy chasing.
- Broad rewrites.
- Architecture refactors.
- Risky cleanup without behavior evidence.
- Production code changes when static analysis is already green.
- Parser, storage, restore, GC, verify, or engine refactors.

Expected tests:

- `golangci-lint run ./...`
- `go vet ./...`
- Full standard test suite before phase closure.
- Race test for `./cmd/coldkeep/...`.

Acceptance criteria:

- Phase 5 remains docs-only when static analysis is already green.
- No production code cleanup is performed without a tiny, behavior-preserving, release-blocking
  finding.
- No correctness regression is introduced.
- Deferred cleanup is documented rather than forced into the patch release.

## Phase 6 - Final Patch Release Gate

Objective: confirm v1.12.1 is ready for final release review, tagging, and merge.

Included scope:

- Run the full release gate.
- Confirm the risk register has required closure evidence.
- Confirm no out-of-scope architecture, storage, schema, or backend changes are present.
- Prepare final release notes and tag only after the gate is green.

Excluded scope:

- New feature work.
- New migrations.
- Late Codacy churn without release-blocking justification.
- PR creation before the gate is green.

Expected tests:

- `gofmt -w $(git ls-files '*.go')`
- `gofmt -l $(git ls-files '*.go')`
- `golangci-lint run ./...`
- `go vet ./...`
- `go test -count=1 ./...`
- `go test -race -count=1 ./...`
- `git diff --check`
- `git status -sb`

Acceptance criteria:

- Full release gate is green.
- No Go formatting drift remains.
- Risk register closure evidence is complete.
- v1.12.1 readiness is claimed only after the final gate passes.
