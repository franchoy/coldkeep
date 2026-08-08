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
- `v1.13.10 — v1.x Closure Integrity and CI Runtime Hygiene` is released and
  operationally closed. It is a valid closure-integrity and CI-runtime-hygiene
  baseline, not the final v1.x release.
- A post-release roadmap-to-code audit superseded the narrower final-v1.x
  conclusion and restored v1.13.11–v1.13.13 for remaining must-before-v2 work.
- `v1.13.11 — Safety and Backend Compatibility Gate Closure` is the single
  active release. Phases 0–11 are complete; Phase 11 closure evidence records
  accepted exact-head CI and CodeQL. The diagnostic benchmark-gate bootstrap is recorded in
  [v1.13.11-phase11-benchmark-gate-integrity-remediation.md](v1.13.11-phase11-benchmark-gate-integrity-remediation.md).
  Its
  exclusive-only coordination contract
  defines canonical container-namespace identity, a recovery-safe control
  directory, stable errors, diagnostic owner metadata, lease lifecycle, and
  command policy without implementing an OS lock or changing runtime behavior.
  BKC-016 remains deferred until Phase 12 and Phase 13 implement and prove
  native coordination; Phase 12 is Next but not started or authorized, and
  Phase 13 is not started.
  Phase 10 implementation `ad82c959` passed exact-head CI run `30148670910`,
  including all five PostgreSQL events in plain job `89655223183` and required
  gate `89656972706`. A first benchmark variance was resolved by successful
  same-head rerun `89656813012` without any benchmark accommodation. BKC-003
  and BKC-015 are backend-specific — proven within their documented bounds.
  Phase 9 exact-head
  CI run `30114444798` at `848e579b` proved scoped active, uncontended Engine
  mutation and GC dry-run parity, including all five required PostgreSQL events
  in plain job `89551564893` and required-gate job `89555865026`. Phase 8 exact-head CI run `30109561344` at `bcae3576` proved
  its scoped snapshot selector and tree-presentation contracts, including both
  required PostgreSQL selector events and the aggregate required gate. Phase 7 exact-head CI run `29993172886` at `313d0069`
  proved the scoped engine read-side contracts across SQLite and PostgreSQL,
  including the SQLite deep-verification single-connection correction. Phase 6
  exact-head CI run `29983479388` proved the scoped
  implemented catalog contracts across SQLite and PostgreSQL, including
  deterministic snapshot ordering. Selected schema/bootstrap/migration
  contracts and the G6 fail-closed remediation also have exact-head CI
  evidence, while broad backend parity remains intentionally unclaimed.
  v1.13.11 is not ready for
  release; the latest released version remains v1.13.10. Its canonical trackers are
  `v1.13.11-phase0-post-release-closure-correction-and-baseline.md`,
  `v1.13.11-scope.md`, `v1.13.11-phase-list.md`,
  `v1.13.11-validation-checklist.md`, and `v1.13.11-release-gate.md`.
- The updated `v1.13.x-release-train.md` is the authoritative current plan;
  final v1.x completion is gated by v1.13.11–v1.13.13. v2.0 implementation has
  not started.
- `v1.13.10-engine-contract-documentation-truthfulness.md` records the current
  Engine contract boundary and its intentional limitations.
- `v1.13.10-release-state-validator-contract.md` freezes the lifecycle,
  evidence, parsing, CKRS rule, output, fixture, and CI integration contract.
- `v1.13.10-release-state-validator-implementation.md` records its
  deterministic implementation, isolated tests, and blocking CI enforcement.
- `v1.13.10-github-actions-node24-artifact-migration.md` records the five-step
  upload-artifact v7 migration and semantic-preservation evidence.
- `v1.13.10-v1x-closure-summary-and-v2.0-handoff-freeze.md` freezes the final
  v1.x baseline, explicit v2.0 inputs, and v2/v3 scope boundary.
- v1.13.10's public release, tag, merge, tag-CI, and deleted-release-branch
  evidence is recorded separately from its historical pre-release gate narrative.
