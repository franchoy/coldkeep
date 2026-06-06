# v1.12.3 Risk Register

## CK-1123-R001 - Hygiene release accidentally changes product behavior

Severity: High

Status: Open

Mitigation: Keep scope limited to release-train documentation cleanup, generated validation
artifact cleanup, ignore-rule updates if needed, and final gate documentation. Reject any Go,
parser, CLI, JSON, exit-code, schema, storage, backend, or engine change from this release.

Evidence required for closure: Final diff shows no product behavior changes and no Go files or
tests changed.

## CK-1123-R002 - v1.12 status cleanup overclaims completion or hides deferred work

Severity: Medium

Status: Open

Mitigation: Use factual release-state wording, preserve deferred-work references where they still
matter, and distinguish completed v1.12 release-train work from planned v1.13 stabilization work.

Evidence required for closure: Updated v1.12 docs clearly mark release-train completion without
claiming v1.12.3 readiness early or erasing deferred items that still belong to later work.

## CK-1123-R003 - Generated artifact cleanup removes intentional benchmark baselines

Severity: High

Status: Open

Mitigation: Separate generated root validation outputs from intentional committed baselines before
removing anything. Treat baseline migration or deletion as out of scope unless explicitly proven
safe and reviewed later.

Evidence required for closure: Cleanup evidence shows only approved generated outputs were removed
and benchmark baseline files were left intact unless explicitly proven generated and obsolete.

## CK-1123-R004 - Ignore rules hide meaningful committed release evidence

Severity: Medium

Status: Open

Mitigation: Add ignore rules only for generated local validation outputs that should not be
tracked. Review ignore patterns against existing committed release evidence and benchmark baselines
before merging.

Evidence required for closure: Ignore-rule diff is narrowly scoped and does not match meaningful
committed release evidence, benchmark baselines, or archival docs.

## CK-1123-R005 - v1.12.3 drifts into v1.13 implementation work

Severity: High

Status: Open

Mitigation: Keep engine, catalog, storage, schema, repository-format, backend, parser, CLI, JSON,
test, and migration work explicitly out of scope. Stop any change that starts solving v1.13
implementation instead of handoff hygiene.

Evidence required for closure: Final diff contains only approved v1.12.3 hygiene changes and no
v1.13 implementation, migration, schema, repository-format, storage, backend, engine, parser, or
CLI work.
