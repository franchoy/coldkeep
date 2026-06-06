# v1.12 Phase 0 — Scope Lock, Baseline, and Risk Inventory

Release: v1.12 — Orchestration Migration and Catalog Facade Preparation
Status: Complete

## Objective

Create the v1.12 operational baseline before any logic migration.

This phase must answer:

> What exactly is v1.12 allowed to change, what is forbidden, what is already true after v1.11, and
> what code paths must move during the release train?

## Core invariant

v1.12 must not change user-visible behavior while moving orchestration behind the engine/catalog
boundary.

Specifically:

- no CLI behavior drift;
- no JSON shape drift;
- no exit-code drift;
- no repository format change;
- no storage format change;
- no default DB backend switch;
- no loss of PostgreSQL compatibility;
- no SQLite-only assumptions in engine/catalog contracts;
- no weakening of restore, verify, snapshot, or GC invariants.

## Included

- Release scope.
- Phase list.
- v1.11 baseline inventory.
- Coupling inventory.
- Direct DB access inventory (search method + initial findings).
- Direct storage-context access inventory (search method + initial findings).
- Existing engine contract inventory.
- Catalog responsibility map.
- SQLite/PostgreSQL baseline.
- Risk register.
- Validation checklist template.
- README/status correction.
- v1.12 Copilot phase prompt.

## Excluded

- No command routing changes.
- No new catalog package.
- No mutation migration.
- No behavior change.
- No default DB change.
- No storage/repository format change.
- No schema/migration change.
- No daemon/API/UI/NAS/cloud work.
- No v2.x features.
- No unrelated Codacy/style cleanup.

If a bug is found during inventory, it is recorded in the risk register. It is not fixed in Phase 0
unless it blocks the baseline itself.

## Bug discovered during Phase 0 inventory (not fixed here)

`DefaultEngine.Verify` (`internal/engine/default_engine.go`) passes `Config.ContainerDir` but
delegates to `maintenance.VerifyCommandWithContainersDir` (`internal/maintenance/verify_command.go`),
which calls `db.ConnectDB()` itself and ignores the caller-provided `Config.DB`. The active engine
`Verify` method therefore does not fully honor the engine-owned DB. This is recorded as risk
`CK-112-R001` and is deferred to Phase 1. It is tolerable in v1.11 because `verify` is not routed
through the engine yet.

## Exit criteria

Phase 0 is complete when:

1. `docs/release/v1.12/` exists.
2. v1.12 scope and non-goals are explicit.
3. v1.11 baseline is documented honestly.
4. every major command has a migration target phase.
5. known v1.12 risks are recorded.
6. catalog/storage/engine responsibility boundaries are documented.
7. SQLite/PostgreSQL compatibility expectations are documented.
8. README no longer points to v1.11 as future work.
9. no command behavior changed.
10. no architecture package was added except docs/prompts.
11. Phase 1 has clear entry criteria.

## Phase 1 entry criteria

- This Phase 0 baseline is merged.
- Risk `CK-112-R001` (engine `Verify` DB ownership) is the first code target.
- Read-side routing parity for `inspect` and `verify system` requires parity tests before routing.

## Closure note

This Phase 0 baseline was completed as the release-train scope lock for the shipped v1.12 work.
Its documented deferred risks and entry criteria remain part of the historical release record and
do not imply that every deferred operation was completed inside v1.12.
