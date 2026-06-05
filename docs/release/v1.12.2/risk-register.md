# v1.12.2 Risk Register

## CK-1122-R001 - Hygiene release expands into new CLI validation families

Severity: High

Status: Open

Mitigation: Keep scope limited to stale v1.12.1 docs, `search --extension` parser alignment, selected empty-value parser-path regression tests, and final gate documentation.

Evidence required for closure: Final diff shows no new validation family work beyond the approved parser-alignment and selected empty-value test scope.

Phase 1 evidence: docs-only cleanup changed v1.12.1 release status wording and v1.12.2 tracking
docs only; no CLI validation family was added.

Phase 2 evidence: the only parser-scope behavior change was registering `extension` as a
value-taking flag so existing empty-value validation can run through the real parser path.
No new CLI validation family or search feature was added.

## CK-1122-R002 - `search --extension` parser alignment changes unsupported-flag semantics unexpectedly

Severity: Medium

Status: Open

Mitigation: Inspect and test the real parser path before changing behavior. Keep unsupported-flag handling unchanged unless the phase documents a compatibility-safe reason.

Evidence required for closure: Focused parser-path tests and review evidence show empty-value behavior is aligned without unintended unsupported-flag changes.

Phase 2 evidence: parser-path coverage asserts `search --extension .txt` still fails with
`unknown flag(s) for search: extension`; non-empty extension search was not made supported.

## CK-1122-R003 - Parser-path tests accidentally encode implementation details too tightly

Severity: Medium

Status: Open

Mitigation: Assert user-visible parser behavior and error outcomes rather than helper names, internal call order, or private implementation structure.

Evidence required for closure: New tests remain behavior-level and would survive internal parser refactoring that preserves CLI behavior.

Phase 2 evidence: tests invoke `parseCommandLine` with user-facing `coldkeep search` arguments
before dispatching to `runSearchCommand`, and assert public usage errors rather than private helper
details.

## CK-1122-R004 - Docs cleanup overclaims release readiness

Severity: Medium

Status: Open

Mitigation: Use factual status wording. Do not claim v1.12.2 readiness until the final patch release gate is green and recorded.

Evidence required for closure: Updated docs distinguish planning, completed v1.12.1 cleanup, and v1.12.2 readiness conditions clearly.

Phase 1 evidence: `docs/release/v1.12.1/README.md` now says `Status: Complete / Released`,
records tag `v1.12.1`, and limits the completion note to the completed v1.12.1 patch release.
The v1.12.2 docs still require the final v1.12.2 gate before readiness is claimed.

## CK-1122-R005 - Patch release drifts into v1.13 architecture work

Severity: High

Status: Open

Mitigation: Keep engine, catalog, storage, schema, repository format, backend, daemon, API, UI, NAS, cloud, and migration work explicitly out of scope.

Evidence required for closure: Final diff contains no v1.13 architecture, migration, schema, repository-format, or backend changes.

Phase 1 evidence: docs-only status cleanup did not touch engine, catalog, storage, schema,
repository format, backend, daemon, API, UI, NAS, cloud, or migration files.

Phase 2 evidence: implementation touched only CLI parser registration, parser-path CLI tests, and
v1.12.2 release-tracking docs; no v1.13 architecture or migration files changed.
