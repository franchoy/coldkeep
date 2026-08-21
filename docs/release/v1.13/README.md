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
- `v1.13.11 — Safety and Backend Compatibility Gate Closure` is complete,
  merged as `507859daccf25594142c61e5ab8209a751fb579a`, annotated-tagged, and
  published. Phases 0–20 are complete. Phase 12 implements the
  repository-wide exclusive fail-fast Lease and proves native runtime plus the
  production Coordinator lifecycle on Linux, macOS, and Windows. Phase 13
  preserves strengthened G6 integrity coverage and proves deterministic Linux
  independent-process contention, killed-holder release with immediate
  reacquisition, real live-GC cross-process exclusion, and dedicated
  PostgreSQL advisory-session ownership. Its platform and same-host proof
  limits are recorded in
  [v1.13.11-phase13-closure.md](v1.13.11-phase13-closure.md). Phase 14 now
  validates outer ranges before allocation/read, enforces supported-header,
  catalog-maximum, and physical-size consistency, and preserves persisted
  maxima across packed reads and recovery without changing format bytes. Its
  evidence is recorded in
  [v1.13.11-phase14-container-range-header-consistency.md](v1.13.11-phase14-container-range-header-consistency.md).
  Phase 15 now validates exact decompression expectations before allocation,
  caps complete CKBL output and zstd decoder memory/window resources at 4 MiB,
  and applies the same intrinsic contract to identity and zstd codecs. Restore,
  system Verify, and Store semantic reuse inherit the shared bounded path. Its
  evidence is recorded in
  [v1.13.11-phase15-bounded-decompression.md](v1.13.11-phase15-bounded-decompression.md).
  Phase 16 now preserves exact integer tokens throughout the stable v1.7
  stats, inspect, and simulate-GC JSON envelope path without changing JSON
  number types, schemas, APIs, storage, coordination, or error behavior. Its
  evidence is recorded in
  [v1.13.11-phase16-json-integer-fidelity.md](v1.13.11-phase16-json-integer-fidelity.md).
  Phase 17 now inventories all 70 non-DDL production mutations and hardens the
  20 required-row gaps without changing valid zero-row cleanup, recovery, CAS,
  upsert, bulk, or GC semantics. SQLite/PostgreSQL parity and rollback are
  proven, including no physical GC deletion after a missed metadata delete.
  Its evidence is recorded in
  [v1.13.11-phase17-fail-closed-sql-mutations.md](v1.13.11-phase17-fail-closed-sql-mutations.md).
  Phase 18 now makes selected existing PostgreSQL mutation-cardinality,
  storage/recovery, and Linux coordination execution proof fail closed in
  required CI without changing product semantics or job topology. Its evidence
  is recorded in
  [v1.13.11-phase18-required-backend-coordination-ci.md](v1.13.11-phase18-required-backend-coordination-ci.md).
  Phase 19 reconciles the validation matrix, backend claim matrix, reusable
  release checklist, active trackers, Phase 18 closure chronology, proof
  boundaries, deferred items, and evidence links. Its documentation-only
  evidence is recorded in
  [v1.13.11-phase19-validation-evidence-reconciliation.md](v1.13.11-phase19-validation-evidence-reconciliation.md).
  Phase 20 freezes the prospective exact-head gate contract and pre-release
  state. The immutable commit containing that record must pass candidate-head
  CI, Required Gate, CodeQL, and the complete clean local Profile A gate before
  one pull request to `main` is authorized. BKC-016 remains
  `Deferred — documented`. Phase 12 closure remains recorded in
  [v1.13.11-phase12-closure.md](v1.13.11-phase12-closure.md). The diagnostic
  benchmark-gate bootstrap remains recorded in
  [v1.13.11-phase11-benchmark-gate-integrity-remediation.md](v1.13.11-phase11-benchmark-gate-integrity-remediation.md).
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
  Its final exact-head evidence, PR #106 merge, main validation, annotated tag,
  tag validation, stable publication, and release-branch deletion are
  reconciled as completed release operations. Its canonical trackers are
  `v1.13.11-phase0-post-release-closure-correction-and-baseline.md`,
  `v1.13.11-scope.md`, `v1.13.11-phase-list.md`,
  `v1.13.11-validation-checklist.md`, and `v1.13.11-release-gate.md`.
- `v1.13.12 — Engine and Catalog Completion` is released and operationally
  closed. Final candidate `33aa1a563b1e6f7b09a86326c6bbd06d7b106e58`
  completed the catalog planning APIs, production engine ownership, neutral
  contracts, and enforceable thin-CLI boundary without changing storage
  semantics. PR #107 merged it as
  `7505d7000faef452caeb4c01784f0510960e7240`; PR #108 produced final `main`
  `fd396cd0c8cf43662881211b8e6b2877eb9a8010`. Annotated tag `v1.13.12`, tag
  validation, stable publication, and both temporary-branch deletions are
  complete. Historical Phase 0–21 evidence remains frozen.
- `v1.13.13 — Final v1.x and v2 Handoff Gate` is active on
  `release/v1.13.13`. Phase 5 is Complete. Phase 6 is Next. Its authoritative
  mode is `AUDIT_PLUS_REMEDIATION`, with `RESTORE_INSTALL_CONTRACT` unresolved
  entering the train. v1.x remains open, SQLite-default productization remains
  v2.x scope, PostgreSQL compatibility remains required, and v2.x
  implementation is not authorized.
- Its active trackers are [scope](v1.13.13-scope.md),
  [phase list](v1.13.13-phase-list.md), and
  [validation checklist](v1.13.13-validation-checklist.md).
- Phase 1 freezes current authority and requirements in the
  [authoritative roadmap and supersession audit](v1.13.13-phase1-authoritative-roadmap-and-supersession-audit.md),
  [roadmap authority matrix](v1.13.13-roadmap-authority-matrix.md), and
  [v1.x normative requirement matrix](v1.13.13-v1x-normative-requirements.md).
- Phase 2 independently records complete Engine and Catalog planning
  ownership, a complete thin-CLI boundary, narrow application composition,
  zero promised production stubs, and zero architecture blockers in the
  [Engine/Catalog ownership audit](v1.13.13-phase2-engine-catalog-ownership-audit.md),
  [Engine operation matrix](v1.13.13-engine-operation-ownership-matrix.md),
  [Catalog adoption matrix](v1.13.13-catalog-contract-adoption-matrix.md), and
  [CLI boundary exception matrix](v1.13.13-cli-boundary-exception-matrix.md).
  Catalog architecture remains `PARTIAL` under `P2-ARCH-001` because five
  aggregate methods lack the stable typed-error boundary.
  Phase 3 independently confirms `RESTORE_INSTALL_CONTRACT` as a blocker for
  Phase 6 disposition and later design/remediation.
- Phase 3 independently records `V1_CORRECTNESS: BLOCKED` with three distinct
  root findings: the confirmed restore installation contract, a concurrent
  destination-parent symlink replacement confinement gap, and packed/mixed
  file-deep verification omission. Its exhaustive evidence is the
  [correctness and invariant audit](v1.13.13-phase3-correctness-and-invariant-audit.md),
  [correctness invariant matrix](v1.13.13-correctness-invariant-matrix.md),
  [restore correctness matrix](v1.13.13-restore-correctness-matrix.md), and
  [safety subsystem matrix](v1.13.13-safety-subsystem-audit-matrix.md).
  Phase 3 makes no remediation, backend-equivalence, coordination, or v1
  closure claim.
- Phase 4 independently records SQLite and PostgreSQL support within explicit
  bounds, derives current schema version 16, keeps SQLite-default
  productization in v2 and centralized PostgreSQL product mode in v3, and
  classifies SQLite live GC as `EXPLICIT_V2_PRODUCTIZATION_BOUND`. Its result
  is `DUAL_BACKEND_CONTRACT: BLOCKED` under one new root, `P4-BE-001`, because
  production snapshot-label filtering has backend-dependent case semantics.
  Its exhaustive evidence is the
  [SQLite/PostgreSQL backend audit](v1.13.13-phase4-sqlite-postgresql-backend-audit.md),
  [backend capability matrix](v1.13.13-backend-capability-matrix.md),
  [backend contract evidence matrix](v1.13.13-backend-contract-evidence-matrix.md),
  and [schema/migration parity matrix](v1.13.13-schema-migration-parity-matrix.md).
  All Phase 2/3 findings remain unchanged; Phase 4 makes no coordination,
  remediation, correctness-closure, or v1 closure claim.
- Phase 5 independently records `REPOSITORY_COORDINATION: BLOCKED` under one
  new root, `P5-COORD-001`: the live-repository `simulate gc` route bypasses
  the outer repository lease and can reach schema work plus GC planning
  outside exclusive ownership. The same-process registry, native Unix/Windows
  lifecycle, bounded process-death evidence, owner diagnostics, ordinary
  operation coverage, live-GC exclusion, and PostgreSQL dedicated advisory
  session remain proven within explicit bounds. Evidence is the
  [repository coordination audit](v1.13.13-phase5-repository-coordination-audit.md),
  [coordination contract matrix](v1.13.13-repository-coordination-contract-matrix.md),
  [operation coverage matrix](v1.13.13-coordination-operation-coverage-matrix.md),
  and [platform evidence matrix](v1.13.13-coordination-platform-evidence-matrix.md).
  Network/NAS/distributed coordination remains post-v1; all Phase 2–4
  findings remain unchanged and no remediation is authorized.
- The updated `v1.13.x-release-train.md` is the authoritative current plan.
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
