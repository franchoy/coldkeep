# v1.12.2 Phase List

## Phase 0 - Scope Lock

Objective: establish the v1.12.2 hygiene-only release baseline.

Included scope:

- Create the v1.12.2 release planning documents.
- Record the v1.12.1 audit findings that motivate this patch.
- Define non-goals, risks, validation policy, and final gate expectations.

Excluded scope:

- Go code changes.
- Root README or version file changes.
- PR creation.
- Any v1.13 implementation work.

Expected tests:

- `git diff --check`
- Optional markdown lint if available.

Acceptance criteria:

- Work is on `release/v1.12.2`.
- Only `docs/release/v1.12.2/*` files are created.
- No Go files, root README, or version files are changed.
- Scope is explicitly hygiene-only.
- Final release gate is documented.
- Phase 0 commit is created after validation passes.

## Phase 1 - v1.12.1 Release Documentation Status Cleanup

Status: Complete

Objective: correct stale v1.12.1 documentation status wording.

Included scope:

- Update v1.12.1 release docs that still say Planning / Phase 0 where that wording overstates incompleteness.
- Keep wording factual and limited to release-state cleanup.

Excluded scope:

- Root README changes.
- Version file changes.
- New release-note claims unrelated to v1.12.1 final state.
- Any code changes.

Expected tests:

- `git diff --check`
- Optional markdown lint if available.

Acceptance criteria:

- Stale v1.12.1 status wording is corrected.
- Documentation does not overclaim v1.12.2 readiness.
- No source-code behavior changes are introduced.
- Closure evidence: `docs/release/v1.12.1/README.md` now says `Status: Complete / Released`,
  records release tag `v1.12.1`, and notes that the final release gate passed before merge.

## Phase 2 - `search --extension` Parser Alignment

Status: Complete

Objective: align `search --extension` behavior with the real parser path and v1.12.1 empty-value validation claims.

Included scope:

- Inspect the active parser path for `search --extension`.
- Make the smallest behavior change needed for empty-value validation consistency.
- Preserve existing unsupported-flag semantics unless the phase documents a required compatibility-safe adjustment.

Excluded scope:

- New CLI validation families.
- Broad parser refactors.
- Search engine, catalog, or storage behavior changes.
- JSON side-channel cleanup.

Expected tests:

- Focused parser-path test for `search --extension` empty values.
- Existing relevant CLI/parser tests.

Acceptance criteria:

- `search --extension` rejects empty values through the real parser path as claimed by v1.12.1 validation docs.
- Unsupported-flag behavior is unchanged or explicitly documented as compatible.
- No engine, catalog, storage, schema, or backend changes are made.
- Closure evidence: `extension` is now a parser value flag, real parser-path tests cover empty,
  blank, non-empty unsupported, and missing-value forms, and search still does not allow
  `--extension` as a supported filter.

## Phase 3 - Empty-Value Parser-Path Regression Coverage

Status: Complete

Objective: add parser-path regression coverage for selected v1.12.1 empty-value validation cases.

Included scope:

- Add focused tests for representative empty-value cases that exercise the real parser path.
- Prefer behavior-level assertions over internal implementation details.

Excluded scope:

- Exhaustive validation matrix expansion.
- Benchmark validation hardening.
- Parser architecture rewrite.
- Unrelated Codacy cleanup.

Expected tests:

- New parser-path regression tests.
- Existing targeted CLI/parser test package.

Acceptance criteria:

- Selected v1.12.1 empty-value validation cases are covered through the parser path users actually invoke.
- Tests remain stable if internal helper names or minor parser implementation details change.
- Coverage does not introduce new validation scope outside the release plan.
- Closure evidence: added parser-path regression coverage for selected empty and blank values on
  search `--name`, search `--path`, snapshot list `--path`, remove `--stored-path`, restore
  `--stored-path`, and snapshot create `--id`; no production code changed.

## Phase 4 - Pre-release Checklist Modernization

Status: Complete

Objective: modernize `PRE_RELEASE_CHECKLIST.md` so it is safe and clear as a local pre-PR CI-parity path while preserving deeper release-tag/manual gates.

Included scope:

- Add validation profiles for pre-PR CI parity, full release-tag/manual validation, and historical/special-release templates.
- Align local checklist commands with current `.github/workflows/ci.yml`, including legacy compatibility and local cross-platform approximation.
- Clarify smoke, critical coverage, benchmark interpretation, required tools, expected runtime, and reusable sign-off behavior.
- Keep historical and manual release gates available without making them normal pre-PR blockers.

Excluded scope:

- Go code changes.
- Test changes.
- Script changes.
- CI behavior changes.
- v1.13 implementation work.

Expected tests:

- `git diff --check`
- Optional markdown lint if available.

Acceptance criteria:

- `PRE_RELEASE_CHECKLIST.md` has clear Profile A / B / C validation paths.
- Current CI legacy-compatibility has a local mirror command.
- Current CI cross-platform has a documented local approximation and does not overclaim macOS/Windows proof.
- Final sign-off boxes are reusable and unchecked.
- No Go, test, script, or CI behavior changes are made.

## Phase 5 - Final Patch Release Gate

Objective: verify and record v1.12.2 patch-release readiness.

Included scope:

- Run the mandatory final release gate.
- Record validation outcomes.
- Confirm no out-of-scope architecture, storage, schema, or backend changes entered the patch.

Excluded scope:

- New feature work.
- v1.13 implementation.
- Release tagging before the gate is green and documented.

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

- Mandatory final gate is green or every failure is explicitly documented and resolved before readiness is claimed.
- Scope risks are closed or explicitly accepted.
- v1.12.2 readiness is only claimed after the gate is green and recorded.
- Closure evidence: `docs/release/v1.12.2/release-gate.md` records the green post-Phase 4 final
  gate, compatibility statement, residual risks, merge readiness decision, and tag recommendation.
