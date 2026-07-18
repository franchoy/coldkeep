# v1.13 - Engine Stabilization Baseline & Contract Inventory

## Purpose

v1.13 starts after v1.12.3 completion. Its purpose is to stabilize the engine contract surface,
stabilize the catalog contract surface, harden dependency direction, clarify engine-level
correctness invariant ownership, prepare for SQLite-first local portability, and preserve
PostgreSQL compatibility while the project moves toward future v2.x local-first productization.

v1.13.0 is the baseline release for that work. It is documentation and inventory only, and it
must not change runtime behavior.

## v1.13 Goals

- Engine contract stabilization.
- Catalog contract stabilization.
- Dependency-direction hardening.
- Engine-level invariant ownership clarification.
- SQLite-first local portability preparation.
- PostgreSQL compatibility preservation.

## v1.13.0 Baseline Policy

- v1.13.0 is baseline and inventory only.
- v1.13.0 must not change runtime behavior.
- v1.13.0 does not implement v2.x work.
- v1.13.0 prepares the project for v2.x local-first productization without implementing daemon,
  API, UI, protocol, or product-surface changes.

## Release Boundaries

Do not change during v1.13.0:

- Go code.
- Tests.
- Schema or migrations.
- CLI behavior.
- JSON output.
- Exit codes.
- Storage format.
- Repository format.
- Default backend behavior.
- Engine routing.
- Catalog implementation.
- CI behavior.
- Scripts.

Do not implement during v1.13.0:

- New engine operations.
- New catalog methods.
- SQLite default switch.
- PostgreSQL removal.
- Daemon, API, UI, or v2 work.
- Broad refactors.

## Release Artifacts

The v1.13.0 baseline is defined by:

- `docs/release/v1.13/v1.13.0-scope.md`
- `docs/release/v1.13/v1.13.0-phase-list.md`
- `docs/release/v1.13/v1.13.0-validation-checklist.md`

All v1.13.0 phases stay on `release/v1.13.0` until the full release gate is green.

## Current Release State

- `v1.13.9` is complete, merged, tagged, published, and operationally closed.
- Phase 24 is complete; PR #104 merged, `v1.13.9` was tagged and released,
  and release/tag CI passed.
- `release/v1.13.9` was deleted locally and remotely. No Phase 25 was
  required, and no mandatory v1.x runtime remediation remained.
- `v1.13.10 — v1.x Closure Integrity and CI Runtime Hygiene` is ready for
  release after completing all eight phases and the local pre-release gate.
- `v1.13.10-release-train-reconciliation.md` is the canonical disposition of
  retired historical v1.13.10–v1.13.13 allocations.
- `v1.13.10-engine-contract-documentation-truthfulness.md` records the active
  Engine contract boundary and its intentional limitations.
- `v1.13.10-release-state-validator-contract.md` freezes the lifecycle,
  evidence, parsing, CKRS rule, output, fixture, and CI integration contract.
- `v1.13.10-release-state-validator-implementation.md` records its
  deterministic implementation, isolated tests, and blocking CI enforcement.
- `v1.13.10-github-actions-node24-artifact-migration.md` records the five-step
  upload-artifact v7 migration and semantic-preservation evidence.
- `v1.13.10-v1x-closure-summary-and-v2.0-handoff-freeze.md` freezes the final
  v1.x baseline, explicit v2.0 inputs, and v2/v3 scope boundary.
- The canonical v1.13.10 release gate is passed awaiting publication, and one
  pull request from `release/v1.13.10` to `main` is authorized. External PR,
  merge, tag, and publication evidence remains unavailable.
