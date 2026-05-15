# v1.10 Release Gates

Status: Complete
Owner phase: Phase 9 — Release Gate Definition

## Purpose

This document defines the release gates for the Coldkeep v1.10 stabilization train.

A release gate is a condition that must be satisfied before a v1.10.x release is tagged from `main`.

# Phase 9 Completion Statement

The v1.10 release gates have been defined.

## Phase 9 completed

- v1.10.0 baseline release gates
- general v1.10.x release gates
- documentation gates
- issue-tracking gates
- S0/S1 gates
- local validation gates
- CI gates
- Codacy gates
- branch/PR/merge/tag policy
- release-note gates
- optional gate summary inventory

## Phase 9 did not

- change CI workflows
- add required CI jobs
- enable Codacy blocking
- add coverage thresholds
- implement remediation
- change production code
- change tests
- change scripts
- change dependencies

## Philosophy

Coldkeep is a correctness-first storage system.

Release gates must prioritize:

- data safety
- deterministic restore
- verification correctness
- GC safety
- snapshot retention
- restore/recovery safety
- validation strictness
- machine-readable output correctness
- CI reliability
- known-risk traceability

Release gates must not prioritize:

- style-only warnings
- generic maintainability scoring
- broad aesthetic refactors
- raw scanner count
- architectural extraction during v1.10
- superficial global coverage percentages

## Core Rule

A v1.10.x release may be tagged only after:

1. scoped work for that v1.10.x is complete,
2. local pre-release validation is green or exceptions are documented,
3. CI is green after merge,
4. release notes are prepared,
5. known risks are updated,
6. the code is merged into `main`,
7. the tag is created from `main`.

No v1.10.x release is tagged from an unmerged feature branch.

---

# v1.10.0 Release Gates

v1.10.0 is complete only when all baseline/freeze phases are complete.

## Phase completion gates

- [ ] Phase 0 — Freeze Declaration & Scope Lock complete
- [ ] Phase 1 — Release Documentation Skeleton complete
- [ ] Phase 2 — Source Inventory & Evidence Freeze complete
- [ ] Phase 3 — Severity Model Adoption complete
- [ ] Phase 4 — Issue Tracking Schema Freeze complete
- [ ] Phase 5 — Codacy Baseline Import complete
- [ ] Phase 6 — External Audit Import complete
- [ ] Phase 7 — Remediation Matrix Construction complete
- [ ] Phase 8 — CI Baseline Capture complete
- [ ] Phase 9 — Release Gate Definition complete
- [ ] Phase 10 — Dependency & Toolchain Vulnerability Plan complete
- [ ] Phase 11 — Codacy Policy Baseline complete
- [ ] Phase 12 — Initial S0/S1 Candidate Review complete
- [ ] Phase 13 — v1.10.0 Checklist & Release Candidate complete

## Scope gates

- [ ] No production behavior changes unless explicitly documented as baseline-only
- [ ] No remediation implementation started
- [ ] No engine extraction started
- [ ] No new product features added
- [ ] No repository format changes
- [ ] No CI enforcement behavior changed
- [ ] No Codacy blocking introduced

## Evidence gates

- [ ] Roadmap evidence frozen or explicitly referenced
- [ ] Codacy JSON evidence frozen
- [ ] External audit evidence frozen
- [ ] Release-chain plan frozen
- [ ] CI proposal frozen
- [ ] Evidence manifest complete
- [ ] Evidence checksums recorded

## Tracking gates

- [ ] Severity/status/domain/risk labels defined
- [ ] Issue tracking schema frozen
- [ ] Codacy findings imported or explicitly excluded
- [ ] External audit findings imported or explicitly excluded
- [ ] Remediation matrix created
- [ ] Raw issue rows linked to matrix rows
- [ ] Accepted-risk log initialized
- [ ] Deferred-issue log initialized
- [ ] Suppressed-finding log initialized

## CI baseline gates

- [ ] Current workflows inventoried
- [ ] Current local validation commands recorded
- [ ] Current release gate baseline recorded
- [ ] Known CI gaps recorded
- [ ] Codacy enforcement state recorded
- [ ] Future CI evolution mapped to v1.10.x

## Risk gates

- [ ] S0/S1 candidate feed prepared
- [ ] Initial S0/S1 review completed in Phase 12
- [ ] No known S0/S1 candidate remains unknown
- [ ] Every known S0/S1 candidate has a target, status, and rationale

## Release gates

- [ ] Local pre-release checklist run
- [ ] Local CI simulation run
- [ ] Failures fixed or exceptions documented
- [ ] PR opened from v1.10.0 feature branch
- [ ] PR CI green
- [ ] PR merged into `main`
- [ ] `v1.10.0` tag created from `main`
- [ ] Release notes published

---

# General v1.10.x Release Gates

Every v1.10.x release after v1.10.0 must satisfy the following gates.

## Scope gate

Each v1.10.x release must have a narrow declared scope.

Examples:

| Release | Scope |
|---|---|
| v1.10.1 | CLI correctness and contract stabilization |
| v1.10.2 | Validation and security hardening |
| v1.10.3 | Packed storage and metadata integrity |
| v1.10.4 | GC correctness and reachability |
| v1.10.5 | Restore and recovery safety |
| v1.10.6 | CI evolution phase 1 and Codacy passive integration |
| v1.10.7 | Critical-path coverage gates |
| v1.10.8 | Filesystem abstraction groundwork |
| v1.10.9 | Filesystem fault injection phase 1 |
| v1.10.10 | Cross-platform validation |
| v1.10.11 | Stabilization and regression burn-down |
| v1.10.12 | Engine-boundary preparation without behavior change |

## Required gates for every v1.10.x

- [ ] Release scope declared before implementation
- [ ] Matrix rows selected for the release
- [ ] Included issues listed
- [ ] Excluded issues listed with reason
- [ ] Behavior changes documented
- [ ] Regression tests added or rationale documented
- [ ] CI gates added if required by matrix rows
- [ ] Accepted/deferred/suppressed logs updated
- [ ] Known issues updated
- [ ] Changelog updated
- [ ] Local pre-release checklist green or exception documented
- [ ] Local CI simulation green or exception documented
- [ ] PR opened
- [ ] PR CI green
- [ ] PR merged into `main`
- [ ] Release tag created from `main`
- [ ] Release notes published

## Forbidden for every v1.10.x

- [ ] No engine extraction
- [ ] No unrelated product features
- [ ] No broad architecture rewrite
- [ ] No style-only scanner cleanup as release blocker
- [ ] No raw Codacy count release gate
- [ ] No untracked S0/S1 decision


# Documentation Gates

Every v1.10.x release must update the relevant documentation.

## Required documentation updates

| Document | Required when |
|---|---|
| `CHANGELOG.md` | Every release |
| `docs/release/v1.10/phase-status.md` | v1.10.0 phases |
| `docs/release/v1.10/v1.10.0-checklist.md` | v1.10.0 only |
| `docs/release/v1.10/release-gates.md` | When release-gate policy changes |
| `docs/release/v1.10/remediation-matrix.csv` | When matrix rows change status/closure |
| `docs/release/v1.10/issue-tracker.csv` | When raw issue statuses change |
| `docs/release/v1.10/accepted-risks.md` | When risk is accepted |
| `docs/release/v1.10/deferred-issues.md` | When issue is deferred |
| `docs/release/v1.10/suppressed-findings.md` | When finding is suppressed |
| `docs/release/v1.10/known-s0-s1.md` | When S0/S1 status changes |
| `README.md` | Only if user-visible project status changes |

## Documentation quality gate

Release documentation must not claim that an issue is fixed, accepted, deferred, or suppressed unless the corresponding tracker/matrix/risk document also records that decision.

## Changelog gate

Each release changelog entry must include:

- release scope
- included work
- excluded work
- behavior changes
- validation performed
- known risks or accepted/deferred issues
- next release target

---

# Issue-Tracking Gates

## Raw issue tracker gate

Before release:

- [ ] Every new finding is recorded in `issue-tracker.csv`
- [ ] Every real issue has `release_target`
- [ ] Every real issue has `severity`
- [ ] Every real issue has `domain`
- [ ] Every real issue has `status`
- [ ] Every real issue has `decision`
- [ ] Every non-duplicate/non-suppressed issue is linked to a matrix row
- [ ] No S0/S1 issue remains `open`

## Remediation matrix gate

Before release:

- [ ] Every included matrix row has clear acceptance criteria
- [ ] Every fixed matrix row has closure proof
- [ ] Every deferred matrix row has rationale
- [ ] Every accepted matrix row has rationale
- [ ] Every suppressed scanner matrix row has rationale
- [ ] Matrix status matches linked issue statuses
- [ ] Matrix release targets remain accurate

## Closure proof gate

A matrix row or issue may be closed only with at least one of:

- commit SHA
- regression test
- adversarial test
- integration test
- CI gate
- validation command
- documentation path
- accepted-risk record
- deferred-issue record
- suppression record

## Unknown issue gate

A release may not ship with newly discovered but unrecorded issues.

If discovered during validation, the issue must be:

- fixed in scope,
- added to tracker and deferred,
- added to tracker and accepted,
- added to tracker and suppressed with rationale,
- or added to tracker and assigned to a later release.

---

# S0/S1 Release Gates

## S0 gate

An S0 issue blocks release unless one of the following is true:

- it is fixed with closure proof,
- it is proven impossible with evidence,
- it is accepted with explicit high-visibility rationale,
- it is outside the shipped/released behavior and deferred with explicit rationale.

S0 acceptance or deferral should be exceptional.

## S1 gate

An S1 issue must not remain unknown or untriaged.

An S1 issue may remain open only if:

- it has a target release,
- it has an owner or owner placeholder,
- risk during deferral is documented,
- it does not invalidate the current release scope,
- it is recorded in known issues if user-visible.

## v1.10.0 special rule

v1.10.0 is a baseline release.

It may release with known S0/S1 candidates only if Phase 12 has completed and every candidate has:

- matrix ID or issue ID,
- severity decision,
- domain,
- release target,
- status,
- rationale,
- required regression/CI expectation where applicable.

## Engine extraction gate

No v1.11 engine-boundary work may begin while known S0/S1 issues remain untriaged.

---

# Local Validation Gates

Before opening a v1.10.x release PR, run the local validation baseline unless explicitly skipped with reason.

## Baseline commands

```bash
go test ./...
go test -race ./...
```

## Project release commands

Run all applicable current release commands recorded in ci-baseline.md.

Examples may include:

```bash
scripts/smoke.sh
scripts/audit_ci_enforcement.sh
```

Add project-specific commands here as they become formal gates in Phase 13 or later releases.

## Result policy

Each validation command must be recorded as one of:

- pass
- fail
- skipped
- not-present
- environment-blocked

Failures must not be hidden.

If a failure is unrelated to release readiness, document:

- command,
- failure,
- reason it is non-blocking,
- linked issue/matrix row if applicable,
- follow-up release target.

---

# CI Gates

## PR CI gate

Before merge:

- [ ] Required GitHub Actions workflows pass
- [ ] No required job is skipped unexpectedly
- [ ] No required artifact is missing unexpectedly
- [ ] No flaky failure is ignored without an issue/matrix row
- [ ] If CI fails due to environment/external cause, exception is documented

## Main branch gate

Before tag:

- [ ] `main` contains the merged release PR
- [ ] CI is green on `main`, or documented platform/environment exception exists
- [ ] Release tag is created from the validated `main` commit

## No new hard gate without baseline

A new CI hard gate may not be introduced unless:

- baseline behavior is known,
- false-positive risk is understood,
- failure messages are actionable,
- it maps to a matrix row or release-gate requirement,
- it does not block on style-only noise.

This is especially important for Codacy, coverage thresholds, mutation testing, and cross-platform expansion.

---

# Codacy Gates

## v1.10.0 Codacy gate

For v1.10.0:

- [ ] Codacy evidence is frozen
- [ ] Codacy findings are imported
- [ ] Codacy findings are mapped to matrix rows
- [ ] Dependency/toolchain candidates are identified
- [ ] Production security candidates are identified
- [ ] Style/test-noise candidates remain tracked
- [ ] Codacy is not blocking release on raw count
- [ ] Codacy is not blocking release on style-only findings

## Later v1.10.x Codacy gate

Before Codacy becomes a hard gate:

- [ ] Codacy policy is defined
- [ ] Suppression rationale format is defined
- [ ] False-positive classes are documented
- [ ] Production security classes are reviewed
- [ ] Dependency CVE handling is defined
- [ ] Style-only findings remain non-blocking
- [ ] Test-only scanner noise is handled with rationale
- [ ] Blocking rules map to correctness/security risk, not maintainability score

## Codacy must not block on

- markdownlint style-only findings
- naming preferences
- broad abstraction preferences
- generic maintainability score
- raw issue count
- test-only complexity unless it hides invariant quality

---

# Branch, PR, Merge, and Tag Policy

## Branch policy

Each v1.10.x release gets one feature branch.

Example:

```text
feature/v1.10.0-baseline-freeze-declaration
feature/v1.10.1-cli-contracts
feature/v1.10.2-validation-security
```

## Phase policy

Internal phases do not get release tags.

A release is tagged only after all phases for that v1.10.x are complete.

## PR policy

Open a PR only after:

- scoped work is complete,
- local pre-release checklist has run,
- local CI simulation has run,
- documentation is updated,
- known exceptions are documented.

## Merge policy

Merge only when:

- PR review is complete,
- required CI is green,
- release scope is still respected,
- no untracked S0/S1 issue is known.

## Tag policy

Tag only after merge to main.

Correct:

```text
feature branch → PR → green CI → merge to main → tag v1.10.x from main
```

Incorrect:

```text
feature branch → tag v1.10.x before merge
phase branch → tag v1.10.x-phase-N
```

## Next-branch policy

Do not start the next v1.10.x branch until:

- previous v1.10.x is merged,
- main is green,
- previous v1.10.x is tagged/released if applicable.

---

# Release Notes Gate

Every v1.10.x release note must include:

## Required sections

- Summary
- Scope
- Included work
- Excluded work
- Behavior changes
- Validation performed
- Known issues
- Accepted risks
- Deferred issues
- Suppressed findings, if any
- Compatibility notes
- Next release target

## v1.10.0 release-note special case

v1.10.0 release notes must clearly state:

- this is a baseline/freeze release,
- no remediation fixes are included unless explicitly documented,
- Codacy and external audit findings are imported,
- remediation matrix is created,
- CI baseline and release gates are recorded,
- actual remediation begins in v1.10.1.

## Release-note anti-patterns

Do not claim:

- all Codacy issues are fixed,
- all audit issues are fixed,
- CI has been hardened if only baseline was captured,
- engine extraction has started,
- style warnings are release blockers,
- future v1.10.x work is already complete.
