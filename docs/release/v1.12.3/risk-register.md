# v1.12.3 Risk Register

## CK-1123-R001 - Hygiene release accidentally changes product behavior

Severity: High

Status: Open

Mitigation: Keep scope limited to release-train documentation cleanup, generated validation
artifact cleanup, ignore-rule updates if needed, and final gate documentation. Reject any Go,
parser, CLI, JSON, exit-code, schema, storage, backend, or engine change from this release.

Evidence required for closure: Final diff shows no product behavior changes and no Go files or
tests changed.

Phase 1 evidence: changes were limited to `docs/release/v1.12/*` release-state wording and
`docs/release/v1.12.3/*` tracking docs. No Go files, tests, parser behavior, CLI behavior, JSON
behavior, exit-code behavior, schema behavior, storage behavior, backend behavior, or engine
behavior changed.

## CK-1123-R002 - v1.12 status cleanup overclaims completion or hides deferred work

Severity: Medium

Status: Open

Mitigation: Use factual release-state wording, preserve deferred-work references where they still
matter, and distinguish completed v1.12 release-train work from planned v1.13 stabilization work.

Evidence required for closure: Updated v1.12 docs clearly mark release-train completion without
claiming v1.12.3 readiness early or erasing deferred items that still belong to later work.

Phase 1 evidence: `docs/release/v1.12/README.md` now says `Status: Complete / Released`,
`phase-0-scope-baseline.md` now says `Status: Complete`, and `release-candidate-gate.md` now says
`Decision: Ready / released for the scoped v1.12 train`. Deferred operations remain explicitly
documented and v1.13 is identified as the next stabilization step.

## CK-1123-R003 - Generated artifact cleanup removes intentional benchmark baselines

Severity: High

Status: Open

Mitigation: Separate generated root validation outputs from intentional committed baselines before
removing anything. Treat baseline migration or deletion as out of scope unless explicitly proven
safe and reviewed later.

Evidence required for closure: Cleanup evidence shows only approved generated outputs were removed
and benchmark baseline files were left intact unless explicitly proven generated and obsolete.

Phase 2 evidence: the tracked generated root files `benchmark-none-w1.json`,
`benchmark-none-w4.json`, `benchmark-zstd-w1.json`, `benchmark-zstd-w4.json`,
`regression-report-none-w1.json`, `regression-report-none-w4.json`,
`regression-report-zstd-w1.json`, and `regression-report-zstd-w4.json` were removed. The tracked
baseline files `benchmark-baseline.json`, `benchmark-baseline-w4.json`, and
`benchmark-baseline-committed.json` were preserved.

## CK-1123-R004 - Ignore rules hide meaningful committed release evidence

Severity: Medium

Status: Open

Mitigation: Add ignore rules only for generated local validation outputs that should not be
tracked. Review ignore patterns against existing committed release evidence and benchmark baselines
before merging.

Evidence required for closure: Ignore-rule diff is narrowly scoped and does not match meaningful
committed release evidence, benchmark baselines, or archival docs.

Phase 2 evidence: `.gitignore` now ignores `benchmark-none-*.json`, `benchmark-zstd-*.json`,
`regression-report-*.json`, and `artifacts/` only. The new patterns do not match
`benchmark-baseline.json`, `benchmark-baseline-w4.json`, or `benchmark-baseline-committed.json`.

## CK-1123-R005 - v1.12.3 drifts into v1.13 implementation work

Severity: High

Status: Open

Mitigation: Keep engine, catalog, storage, schema, repository-format, backend, parser, CLI, JSON,
test, and migration work explicitly out of scope. Stop any change that starts solving v1.13
implementation instead of handoff hygiene.

Evidence required for closure: Final diff contains only approved v1.12.3 hygiene changes and no
v1.13 implementation, migration, schema, repository-format, storage, backend, engine, parser, or
CLI work.

Phase 1 evidence: the update adds a v1.13 handoff note only as release-state documentation. No
v1.13 implementation, migration, schema, repository-format, storage, backend, engine, parser, or
CLI work was started.
