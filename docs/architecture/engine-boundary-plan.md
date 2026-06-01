# Engine Boundary Plan

This document records the engine/catalog boundary direction across releases. It is a planning
document; it does not freeze contracts.

## Boundary rules (from v1.11)

```
cmd/coldkeep          may import internal/engine
internal/engine       may import domain packages
domain packages       must not import internal/engine
internal/engine       must not import cmd/coldkeep
```

These rules are enforced by `internal/engine/dependency_guard_test.go`.

## v1.11 state

v1.11 introduced the behavior-preserving engine facade. `Engine` exposes `Stats`, `Inspect`, and
`Verify`. Only `stats` is routed through the engine from `cmd/coldkeep`. Mutating operations exist
only as inactive candidate contracts. No business logic was moved.

## v1.12 state (Phase 5 — Snapshot Orchestration Migration, complete)

Phase 5 added 4 read-side snapshot methods to the `Engine` interface:
`SnapshotList`, `SnapshotShow`, `SnapshotStats`, `SnapshotDiff`.

All 4 are implemented on `DefaultEngine` and routed from `cmd/coldkeep` via the phase var seam.
Mutating snapshot operations (`create`, `delete`, `restore`) remain direct-wired and are deferred
to a later phase.

`Engine` interface methods as of v1.12 Phase 5:
- `Stats`
- `Inspect`
- `Verify`
- `SnapshotList`
- `SnapshotShow`
- `SnapshotStats`
- `SnapshotDiff`

## v1.12 Migration Rule

v1.12 moves orchestration behind the engine and metadata planning behind catalog APIs.

The migration order is:

1. read-side parity (`inspect`, `verify system`);
2. request/result contract expansion;
3. catalog facade skeleton (`internal/catalog`);
4. catalog backend compatibility baseline (SQLite/PostgreSQL);
5. snapshot orchestration;
6. GC planning/reachability;
7. restore planning;
8. store/CDC/placement;
9. remove/repair/recovery;
10. CLI thin-wrapper burn-down.

### No-command-routing-without-complete-contract rule

No command is routed through the engine unless its request/result contract can represent the existing
command behavior. Routing a command behind a thin contract that drops existing behavior is forbidden.

### Catalog facade migration principle

When migration begins:

```
cmd/coldkeep          must not import internal/catalog directly
internal/engine       may import internal/catalog
internal/catalog      must not import internal/engine
internal/catalog      must not import cmd/coldkeep
```

The catalog facade owns logical identity, physical mapping, snapshot graph, reachability, GC
eligibility, restore-plan inputs, placement, and verification expectations. Storage owns payload
bytes and block/container representation. The engine owns operation coordination, validation, and
safety-boundary enforcement.

## Required invariants (must not be weakened)

```
GC must never delete reachable data.
Restore must never write outside the intended destination.
Verify must fail closed on inconsistent catalog/storage state.
Recovery must not legitimize corrupt mappings.
Packed and legacy storage behavior must remain aligned.
CLI parsing must not be the only place where correctness invariants live.
Engine/catalog APIs must not weaken existing safety guarantees.
```
