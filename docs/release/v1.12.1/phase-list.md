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

Objective: reject positional arguments that are currently ignored by user-facing commands.

Included scope:

- Identify commands that silently ignore unexpected positional arguments.
- Add focused validation for extra arguments at CLI boundaries.
- Add small tests covering accepted and rejected argument forms.

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

## Phase 2 - Empty Filter / Empty Flag Rejection

Objective: reject empty filter and empty flag values when the value would be ambiguous, unsafe, or
previously accepted only by accident.

Included scope:

- Identify flags and filters where `""` has no valid semantic meaning.
- Reject empty values with focused validation messages.
- Add tests for empty, missing, and valid values.

Excluded scope:

- New filter syntax.
- Schema or storage changes.
- Behavior changes for valid non-empty values.
- Broad CLI option redesign.

Expected tests:

- Focused CLI tests for each affected flag or filter.
- Regression tests confirming valid values still work.
- Full standard test suite before phase closure.

Acceptance criteria:

- Empty values are rejected consistently where documented by the phase.
- Valid values retain previous behavior.
- Any intentional compatibility impact is recorded in the risk register.

## Phase 3 - Boolean Flag Value Semantics

Objective: make boolean flag handling consistent for explicit values, implicit values, and invalid
values.

Included scope:

- Audit boolean flags for inconsistent value acceptance.
- Normalize rejection of invalid boolean values.
- Add tests for implicit true, explicit true/false, and invalid values.

Excluded scope:

- Renaming flags.
- Removing existing valid boolean forms without evidence and risk review.
- Parser replacement or broad command rewrite.

Expected tests:

- Focused boolean flag tests for each changed command.
- Automation compatibility checks for accepted forms.
- Full standard test suite before phase closure.

Acceptance criteria:

- Boolean flags have predictable semantics across touched commands.
- Invalid boolean values fail clearly.
- Existing accepted automation forms remain compatible unless explicitly documented.

## Phase 4 - JSON Shorthand Consistency

Objective: make `--json` shorthand behavior consistent across user-facing commands.

Included scope:

- Audit `--json` handling in routed and non-routed commands.
- Fix inconsistencies where shorthand behavior differs without reason.
- Add tests verifying JSON output mode selection and rejected malformed forms.

Excluded scope:

- JSON schema redesign.
- Output field additions unrelated to shorthand behavior.
- API, daemon, or UI output work.

Expected tests:

- Focused `--json` CLI tests for each command touched.
- Routed-command parity tests.
- Full standard test suite before phase closure.

Acceptance criteria:

- `--json` shorthand selects JSON output consistently.
- Existing valid JSON automation output remains stable.
- Any output-contract risk has mitigation or closure evidence.

## Phase 5 - Safe Codacy / Static Analysis Cleanup

Objective: address only behavior-preserving static-analysis findings that are safe inside a patch
release.

Included scope:

- Fix small, localized Codacy or lint findings when the behavior is unchanged.
- Add tests when cleanup touches logic or control flow.
- Defer findings that require style churn, refactors, or architecture movement.

Excluded scope:

- Style-only Codacy chasing.
- Broad rewrites.
- Architecture refactors.
- Risky cleanup without behavior evidence.

Expected tests:

- Focused tests for any logic-adjacent cleanup.
- `golangci-lint run ./...`
- Full standard test suite before phase closure.

Acceptance criteria:

- Cleanup is small, behavior-preserving, and justified.
- No correctness regression is introduced.
- Deferred findings are documented rather than forced into the patch release.

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
