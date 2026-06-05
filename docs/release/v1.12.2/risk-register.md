# v1.12.2 Risk Register

## CK-1122-R001 - Hygiene release expands into new CLI validation families

Severity: High

Status: Open

Mitigation: Keep scope limited to stale v1.12.1 docs, `search --extension` parser alignment, selected empty-value parser-path regression tests, and final gate documentation.

Evidence required for closure: Final diff shows no new validation family work beyond the approved parser-alignment and selected empty-value test scope.

## CK-1122-R002 - `search --extension` parser alignment changes unsupported-flag semantics unexpectedly

Severity: Medium

Status: Open

Mitigation: Inspect and test the real parser path before changing behavior. Keep unsupported-flag handling unchanged unless the phase documents a compatibility-safe reason.

Evidence required for closure: Focused parser-path tests and review evidence show empty-value behavior is aligned without unintended unsupported-flag changes.

## CK-1122-R003 - Parser-path tests accidentally encode implementation details too tightly

Severity: Medium

Status: Open

Mitigation: Assert user-visible parser behavior and error outcomes rather than helper names, internal call order, or private implementation structure.

Evidence required for closure: New tests remain behavior-level and would survive internal parser refactoring that preserves CLI behavior.

## CK-1122-R004 - Docs cleanup overclaims release readiness

Severity: Medium

Status: Open

Mitigation: Use factual status wording. Do not claim v1.12.2 readiness until the final patch release gate is green and recorded.

Evidence required for closure: Updated docs distinguish planning, completed v1.12.1 cleanup, and v1.12.2 readiness conditions clearly.

## CK-1122-R005 - Patch release drifts into v1.13 architecture work

Severity: High

Status: Open

Mitigation: Keep engine, catalog, storage, schema, repository format, backend, daemon, API, UI, NAS, cloud, and migration work explicitly out of scope.

Evidence required for closure: Final diff contains no v1.13 architecture, migration, schema, repository-format, or backend changes.
