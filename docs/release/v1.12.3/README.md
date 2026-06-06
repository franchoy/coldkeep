# v1.12.3 - Release Train Closure Hygiene

## Purpose

v1.12.3 is a tiny release-handoff hygiene patch before v1.13. Its purpose is to close stale
v1.12 release-train documentation and generated validation artifact hygiene gaps found after the
v1.12.2 audit without changing product behavior.

## Background

A strict audit of `main` after v1.12.2 found no storage, schema, backend, or engine correctness
blocker. It did identify two release-handoff hygiene issues worth resolving before v1.13:

1. Main v1.12 release-train documentation still contains stale Planning / In progress / pending
   wording that no longer reflects the completed v1.12 release train.
2. Generated local benchmark and regression validation outputs appear at the repository root and,
   if tracked, may be misleading unless they are cleaned up or explicitly ignored.

## Scope

- Fix stale v1.12 release-train documentation status.
- Clarify that the v1.12 release train is complete and that v1.13 is the next
  architecture-stabilization step.
- Remove generated root benchmark and regression validation outputs if they are tracked.
- Add ignore rules for generated local validation outputs if needed.
- Run and record the final hygiene release gate.

All v1.12.3 phases stay on the same branch, `release/v1.12.3`, until the final release gate is
green.

## Non-goals

- Go code changes.
- Test changes.
- Parser behavior changes.
- CLI behavior changes.
- JSON behavior changes.
- Exit-code behavior changes.
- Schema changes.
- Repository or storage format changes.
- Default backend changes.
- Engine, catalog, or storage migration.
- v1.13 implementation work.
- Codacy cleanup.
- Benchmark threshold changes.
- Benchmark baseline migration unless explicitly reviewed in a later release.
- Removing committed baseline files unless they are explicitly proven generated and obsolete.

## Safety Principles

- Treat this as release-handoff hygiene only, not product behavior work.
- Keep documentation factual and limited to release-state cleanup.
- Distinguish generated validation outputs from intentional committed benchmark baselines before
  removing or ignoring anything.
- Do not change storage, schema, backend, engine, parser, CLI, JSON, exit-code, or repository
  behavior.
- Do not claim v1.12.3 readiness until the final hygiene release gate is green and recorded.

## Branch Workflow

- Create and keep work on `release/v1.12.3`, branched from current `main` after v1.12.2 is merged
  and tagged.
- Keep all v1.12.3 phases on this same branch until the final release gate is green.
- Do not open a PR during Phase 0.
- Do not update version files during Phase 0.
- Do not edit the root README during Phase 0.
- Do not remove artifacts during Phase 0.

## Validation Policy

Each phase must run validation proportional to its changes. Phase 0 is documentation-only and
requires `git diff --check`; markdown lint may be run if available. Later phases must prove that
documentation cleanup stays factual, artifact cleanup only touches approved generated outputs, and
the full hygiene release gate is green before readiness is claimed.

## Final Release and Tag Policy

v1.12.3 readiness may only be claimed after the final hygiene release gate passes and results are
documented. The release tag must not be created until all phase acceptance criteria are satisfied,
scope risks are closed or explicitly accepted, and the branch is ready for the normal release
process.
