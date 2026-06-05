# v1.12.2 - CLI Validation Follow-up

## Purpose

v1.12.2 is a tiny hygiene-only patch release before v1.13. Its purpose is to close documentation and CLI parser-validation gaps found after the v1.12.1 release audit without expanding the release into new validation families or architecture work.

## Background

The strict v1.12.1 audit found no storage, engine, or catalog correctness issue. It did identify two follow-up hygiene items worth closing before v1.13:

1. The v1.12.1 release documentation still says Planning / Phase 0 in places where release-state wording should reflect the completed patch release.
2. `search --extension` validation and test coverage should be aligned with the real parser path used by the CLI.

## Scope

- Fix stale v1.12.1 release documentation status.
- Align `search --extension` parser behavior with v1.12.1 empty-value validation claims.
- Add parser-path regression tests for selected v1.12.1 empty-value validation cases.
- Run and record the final patch release gate.

All v1.12.2 phases stay on the same branch, `release/v1.12.2`, until the final release gate is green.

## Non-goals

- New CLI validation families.
- Broad parser rewrite.
- Benchmark validation hardening.
- Snapshot tag normalization.
- JSON side-channel cleanup.
- Engine, catalog, or storage migration.
- Recursive or folder store migration.
- Stored-path restore migration.
- Snapshot restore migration.
- Stored-path remove migration.
- Repair or recovery migration.
- Schema changes.
- Repository or storage format changes.
- Default backend changes.
- Daemon, API, UI, NAS, or cloud work.
- v1.13 implementation work.
- Unrelated Codacy cleanup.

## Safety Principles

- Treat this as a documentation and parser-path hygiene release only.
- Preserve existing command semantics unless a phase explicitly documents the compatibility expectation.
- Keep implementation changes narrow and covered by parser-path tests.
- Do not change storage, catalog, repository format, schema, or backend behavior.
- Do not claim v1.12.2 release readiness until the final patch release gate is green and recorded.

## Branch Workflow

- Create and keep work on `release/v1.12.2`, branched from current `main` after v1.12.1 is merged and tagged.
- Keep all v1.12.2 phases on this same branch until the final release gate is green.
- Do not open a PR during Phase 0.
- Do not update version files during Phase 0.
- Do not edit the root README during Phase 0.

## Validation Policy

Each phase must run validation proportional to its changes. Phase 0 is documentation-only and requires `git diff --check`; markdown lint may be run if available. Parser behavior phases require focused CLI/parser tests, followed by the final release gate before readiness is claimed.

## Final Release and Tag Policy

v1.12.2 readiness may only be claimed after the final patch release gate passes and results are documented. The release tag must not be created until all phase acceptance criteria are satisfied, scope risks are closed or explicitly accepted, and the branch is ready for the normal release process.
