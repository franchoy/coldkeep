# v1.12.3 Phase List

## Phase 0 - Scope Lock

Objective: establish the v1.12.3 hygiene-only release baseline.

Included scope:

- Create the v1.12.3 release planning documents.
- Record the post-v1.12.2 audit findings that motivate the patch.
- Define non-goals, risks, validation policy, and final gate expectations.

Excluded scope:

- Go code changes.
- Test changes.
- Root README or version file changes.
- Artifact removal.
- PR creation.
- Any v1.13 implementation work.

Expected validation:

- `git diff --check`
- Optional markdown lint if available.

Acceptance criteria:

- Work is on `release/v1.12.3`.
- Only `docs/release/v1.12.3/*` files are created.
- No Go files, tests, root README, version files, or artifacts are changed.
- Scope is explicitly hygiene-only.
- Stale v1.12 release-train docs and generated validation artifacts are listed as cleanup targets.
- Final hygiene release gate is documented.
- Phase 0 commit is created after validation passes.

## Phase 1 - v1.12 Release Train Status Cleanup

Status: Complete

Objective: correct stale v1.12 release-train status wording and clarify handoff state.

Included scope:

- Update v1.12 release-train docs that still say Planning, In progress, or pending after release
  closure.
- Clarify that the v1.12 release train is complete.
- Clarify that v1.13 is the next architecture-stabilization step.

Excluded scope:

- Product behavior changes.
- New release-note claims unrelated to v1.12 final state.
- Root README or version file changes unless explicitly approved in a later phase.
- Any Go or test changes.

Expected validation:

- `git diff --check`
- Optional markdown lint if available.

Acceptance criteria:

- Stale v1.12 release-train status wording is corrected.
- Documentation does not overclaim v1.12.3 readiness or hide deferred work.
- v1.13 is described as the next step without starting implementation work.
- No source-code or test changes are introduced.
- Closure evidence: `docs/release/v1.12/README.md`, `phase-0-scope-baseline.md`, and
  `release-candidate-gate.md` now reflect the released v1.12 state, preserve deferred-operation
  language, and point to v1.13 as the next stabilization step.

## Phase 2 - Generated Validation Artifact Cleanup

Objective: remove misleading generated root validation outputs and prevent them from reappearing as
tracked release evidence.

Included scope:

- Determine which root benchmark and regression validation outputs are generated and safe to clean.
- Remove only approved generated outputs if they are tracked.
- Add ignore rules for generated local validation outputs if needed.

Excluded scope:

- Removing committed benchmark baselines unless they are explicitly proven generated and obsolete.
- Benchmark threshold or baseline migration work.
- Any Go, test, parser, CLI, or schema changes.

Expected validation:

- `git diff --check`
- `git ls-files benchmark-none-w1.json benchmark-none-w4.json benchmark-zstd-w1.json benchmark-zstd-w4.json regression-report-none-w1.json regression-report-none-w4.json regression-report-zstd-w1.json regression-report-zstd-w4.json`

Acceptance criteria:

- Only approved generated validation outputs are removed or ignored.
- Intentional committed benchmark baselines remain intact.
- Ignore rules do not hide meaningful committed release evidence.
- No product behavior or test changes are introduced.

## Phase 3 - Final Hygiene Release Gate

Objective: verify and record v1.12.3 hygiene-release readiness.

Included scope:

- Run the mandatory final hygiene release gate.
- Record validation outcomes.
- Confirm no out-of-scope behavior, schema, storage, backend, engine, or v1.13 work entered the
  release.

Excluded scope:

- New feature work.
- v1.13 implementation.
- Release tagging before the gate is green and documented.

Expected validation:

- `git status -sb`
- `git log --oneline main..release/v1.12.3`
- `gofmt -l $(git ls-files '*.go')`
- `golangci-lint run ./...`
- `go vet ./...`
- `go test -count=1 ./...`
- `go test -race -count=1 ./...`
- `git diff --check`
- `git status -sb`

Acceptance criteria:

- Mandatory final gate is green or every failure is explicitly documented and resolved before
  readiness is claimed.
- Scope risks are closed or explicitly accepted.
- v1.12.3 readiness is only claimed after the gate is green and recorded.
- Final confirmation shows the release stayed hygiene-only and on `release/v1.12.3`.
