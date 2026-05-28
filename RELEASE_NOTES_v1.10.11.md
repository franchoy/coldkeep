# v1.10.11 — Stabilization & Regression Burn-down

## Overview

v1.10.11 is a stabilization and regression burn-down release.

It closes the v1.10 reliability freeze by consolidating evidence, reviewing
remaining high-risk items, executing local regression validation, auditing
flaky/CI stability, and documenting known issues that must remain visible after
v1.10.

## Highlights

- Remaining S0/S1 risk inventory refreshed.
- Accepted, deferred, suppressed, and fixed risk dispositions documented.
- Full v1.10.11 regression matrix defined and executed.
- 18 required local validation rows passed.
- Flaky-test and CI stability audit completed.
- Known-issues-after-v1.10 handoff created.
- Local release-candidate gate passed.

## Validation

Local validation result:

```text
18 required rows passed
0 failed
0 blocked
0 required rows skipped
0 flaky candidates
0 local release blockers
```

CI validation (Phase 9 pending):

- PR CI gate: pending
- Cross-platform OS matrix: pending
- PostgreSQL integration CI: pending
- Long-run CI: pending
- Benchmark regression CI: pending
- Codacy review: pending
- Merge/main CI: pending

## Known issues

Known issues and carried-forward items are documented in:

```text
docs/release/v1.10/v1.10.11-known-issues-after-v1.10.md
docs/release/v1.10/v1.10.11-known-issues-after-v1.10.csv
```

These include accepted risks, deferred risks, suppressed findings, accepted
observations, and CI-pending evidence. Zero release blockers.

## Compatibility

v1.10.11 is behavior-preserving.

It does not change repository format, container format, CLI behavior, JSON
contracts, scripts, CI configuration, dependencies, engine boundaries, or catalog
architecture.

It reads v1.7/v1.8/v1.9/v1.10 repositories without forced rewrite.

## What changed

No runtime behavior changed in this release.

Changes are documentation and release-evidence only:

- v1.10.11 stabilization scope and review contract.
- S0/S1 risk inventory refresh.
- Risk disposition audit (accepted, deferred, suppressed, fixed).
- Regression matrix definition and execution.
- Flaky-test and CI stability audit.
- Known-issues-after-v1.10 handoff.
- Release-candidate gate.

## Release boundary

This release does not include:

- engine extraction;
- catalog abstraction;
- new product features;
- code, test, CI, script, or dependency changes.

Engine extraction and catalog abstraction remain future v1.10.12 / v1.11+ work.
