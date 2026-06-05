# Coldkeep v1.12.1

Release name: v1.12.1 - Post-Migration CLI Contract Hardening

Status: Complete / Released
Base release: v1.12.0
Release type: Patch-level hardening release
Branch: `release/v1.12.1`
Release tag: `v1.12.1`

Completion note: v1.12.1 was completed as a patch release for post-migration CLI contract
hardening. The full final release gate passed before merge, and the release was tagged as
`v1.12.1`.

## Purpose

v1.12.1 hardens user-facing command validation and automation contracts after the v1.12
engine/catalog boundary migration. The release is intentionally narrow: it improves CLI contract
clarity without starting another architecture migration train.

## Scope

- CLI validation hardening.
- Rejection of ignored positional arguments.
- Rejection of empty-value flags and filters where an empty value would be ambiguous or unsafe.
- Consistent boolean flag value semantics.
- Consistent `--json` shorthand behavior.
- Small parity tests for routed commands.
- Safe Codacy and static-analysis cleanup only when behavior-preserving.
- Documentation and risk tracking for the patch release.

## Non-goals

- No new engine migrations.
- No recursive or folder store migration.
- No stored-path restore migration.
- No snapshot restore migration.
- No stored-path remove migration.
- No repair or recovery migration.
- No schema changes.
- No repository format changes.
- No storage format changes.
- No default backend changes.
- No SQLite-first default switch.
- No daemon, API, UI, NAS, or cloud work.
- No broad parser rewrite.
- No broad `main.go` rewrite.
- No style-only Codacy chasing.
- No architecture refactors.

## Safety Principles

- Treat v1.12.1 as a patch release, not a new migration train.
- Prefer explicit CLI failures over silently ignored user input.
- Preserve command output for existing valid invocations unless the phase explicitly targets that
  contract.
- Keep each change small enough to validate with focused tests and the full release gate.
- Do not combine validation fixes with unrelated cleanup.
- Keep all v1.12.1 phases on `release/v1.12.1` until the final release gate is green.

## Branch Workflow

The release branch should be created from `main` after v1.12.0 is merged and tagged:

```bash
git switch main
git pull
git tag --list v1.12.0
git switch -c release/v1.12.1
```

All v1.12.1 phases stay on the same branch until the final release gate is green. Phase commits
should remain scoped to the active phase. Phase 0 is docs-only and must not modify Go code, root
README content, or version files.

## Validation Policy

Every implementation phase must include focused command validation coverage for the changed
behavior and must preserve valid existing automation contracts. New rejection behavior must be
documented in the phase notes or tests, and routed-command parity must be checked when a routed CLI
path is affected.

Phase 0 validation is limited to:

```bash
git diff --check
```

Markdown lint may be run when available.

## Final Release and Tag Policy

Do not claim v1.12.1 readiness until Phase 6 is complete and the full release gate is green. The
final release commit and tag should be created only after the release gate passes, no out-of-scope
changes are present, and the risk register has closure evidence for all tracked release-blocking
risks.
