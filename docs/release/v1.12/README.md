# Coldkeep v1.12 Release Train

Status: Complete / Released
Base release: v1.11.0
Target: Full logic migration, catalog facade, and SQLite/PostgreSQL compatibility preparation

## Goal

Move business orchestration into the engine so the CLI becomes a thin wrapper, and introduce a
catalog/metadata facade for graph, placement, restore-plan, and GC-plan decisions.

## Non-goals

- No daemon.
- No API.
- No UI.
- No NAS/cloud.
- No multi-user.
- No repository format change.
- No storage format change.
- No default database backend switch.
- No user-visible CLI behavior change unless explicitly documented and approved.

## Core rule

Behavior first, architecture second.

The engine/catalog migration must preserve v1.10/v1.11 behavior. The roadmap explicitly warns that
engine extraction must be behavior-preserving first and must not hide unstable behavior behind a
clean API.

## Completion principle

v1.12 was not considered complete merely because packages existed.

It was considered complete only when the scoped operation orchestration was routed through
engine/catalog APIs without behavior drift. The released v1.12 train completed that scoped
orchestration migration and centralized metadata access for the approved operations; intentionally
deferred operations remained outside the routed scope and were not reclassified as complete.

## Documents in this directory

| Document | Purpose |
| --- | --- |
| `README.md` | Overview of the v1.12 release train (this file). |
| `phase-0-scope-baseline.md` | Phase 0 scope lock, baseline, and risk inventory. |
| `release-phase-list.md` | The full v1.12 phase list (Phase 0–12). |
| `coupling-inventory.md` | CLI / business-logic coupling inventory and search targets. |
| `engine-baseline.md` | Honest record of what v1.11 actually delivered. |
| `catalog-boundary-map.md` | Catalog / storage / engine / verify ownership boundaries. |
| `sqlite-postgres-baseline.md` | SQLite/PostgreSQL compatibility baseline and dialect watchlist. |
| `risk-register.md` | Known v1.12 migration risks with severity and phase targets. |
| `validation-checklist.md` | Per-phase validation checklist template. |

## Roadmap context

```
v1.11.0  — Behavior-preserving engine facade (complete)
v1.12    — Orchestration migration and catalog facade (complete / released)
v1.13    — Engine contract stabilization, catalog contract stabilization, error taxonomy,
           dependency-direction checks, and engine-level invariants
v2.x     — Local-first productization: daemon, API, CLI thin client, UI, scheduling
v3.x     — Network, NAS, cloud, multi-user
```

## Handoff

The v1.12 release train is complete and released through v1.12.0, v1.12.1, and v1.12.2.
Deferred operations remain documented in the v1.12 release evidence and are not being rewritten as
completed here. The next architecture-stabilization step is v1.13, which should focus on engine
contracts, catalog contracts, error taxonomy, dependency-direction checks, and engine-level
invariants.
