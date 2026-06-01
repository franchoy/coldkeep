# v1.12 Release Phase List

This sequence converts the high-level v1.12 plan into the release train. The behavior-preserving rule
is not refinable: no command is routed through the engine unless its request/result contract can
represent the existing command behavior.

## Phase 0 — Scope Lock, Baseline, and Risk Inventory

Planning, inventory, guardrails, and release checklist. No code behavior change.

## Phase 1 — Engine DB Ownership and Read-Side Parity

Fix engine dependency ownership (`Verify` must use `Config.DB`, not global DB discovery) and safely
route/validate read-side commands. Route `inspect` and `verify system` through the engine only after
parity tests exist.

## Phase 2 — Engine Contract Expansion

Replace thin placeholder request/result candidates with realistic contracts that can preserve real
command behavior (store, store-folder, restore by ID, restore by stored path, remove, snapshot
create/list/files/stats/diff/restore/delete, GC dry-run/live, repair, recovery, config). Results
stay renderer-neutral.

## Phase 3 — Catalog Facade Skeleton

Introduce `internal/catalog` interfaces with wrapper-only adapters over existing DB/query code. No
behavior change, no SQL dialect change. Dependency rule: engine may import catalog; catalog must not
import engine; CLI must not import catalog directly once migration begins.

## Phase 4 — SQLite/PostgreSQL Catalog Compatibility Baseline

Add backend-neutral catalog contract tests (SQLite + PostgreSQL where feasible) and document SQL
dialect boundaries (placeholder syntax, `RETURNING`, transaction behavior, introspection, timestamp
and path normalization).

## Phase 5 — Snapshot Orchestration Migration

Route snapshot list/stats/files/diff through engine first, then create/delete/restore after tests.
Keep snapshot graph/reachability access behind catalog where possible. Preserve immutability and
retention semantics.

## Phase 6 — GC Plan and Reachability Migration

Move GC dry-run/live orchestration behind engine and reachability/deletion-plan inputs behind catalog
APIs. Represent packed and legacy roots consistently. Preserve: GC must never delete reachable data.

## Phase 7 — Restore Plan Migration

Introduce a restore-plan catalog API. Support ID restore and stored-path restore. Preserve overwrite,
prefix, override, and original destination modes, plus path traversal and symlink safety. Add
engine-level tests for "restore must not write outside destination."

## Phase 8 — Store/CDC/Placement Coordination Migration

Route `store` and `store-folder` through engine. Keep CDC/chunker selection, compression, and
encryption behavior identical. Move placement lookup/recording behind catalog APIs where feasible.
Preserve cross-version chunk reuse.

## Phase 9 — Remove/Repair/Recovery Migration

Route remove by ID/stored-path, repair, and recovery (where safe) through engine. Recovery must not
legitimize corrupt mappings; repair must not hide broken catalog/storage state. Preserve batch and
dry-run semantics.

## Phase 10 — CLI Thin Wrapper Burn-down

Reduce `cmd/coldkeep` to: parse args, validate CLI syntax, build engine requests, call engine, render
output. Remove duplicated orchestration. Keep CLI validation as user-facing protection while
duplicating core safety validation in engine/catalog.

## Phase 11 — Engine/Catalog Invariant Test Matrix

Prove no-data-loss, deterministic restore, GC safety, verification failure-closed behavior, snapshot
retention, packed/legacy parity, and SQLite/PostgreSQL compatibility at the engine/catalog boundary.
Add catalog contract tests (reachability, placement, restore-plan, GC-plan, snapshot graph) and
dependency-direction checks for catalog.

## Phase 12 — Release Candidate Gate

Run the full local suite, race suite, integration/adversarial suites, PostgreSQL compatibility suite,
critical coverage checks, and release checklist. Compare CLI JSON/human output parity for migrated
commands. Document known limitations and accepted risks.
