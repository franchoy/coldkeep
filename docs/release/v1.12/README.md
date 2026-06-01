# Coldkeep v1.12 Release Train

Status: Planning / Phase 0
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

v1.12 is not considered complete because packages exist.

It is complete only when operation orchestration is routed through engine/catalog APIs without
behavior drift. v1.12 is explicitly about moving orchestration into the engine and centralizing
metadata access, not just creating empty architecture packages.

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
v1.12    — Orchestration migration and catalog facade (this train)
v1.13    — Engine contract stabilization and SQLite/PostgreSQL compatibility gates
v2.x     — Local-first productization: daemon, API, CLI thin client, UI, scheduling
v3.x     — Network, NAS, cloud, multi-user
```
