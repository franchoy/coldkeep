# Coldkeep Repository Instructions

Coldkeep is correctness-first. The primary invariant is: never lose user data.

## Active authority

<!-- coldkeep-current-state:start -->
```text
SOURCE_VERSION: 2.0.0-dev.0
RELEASE_FAMILY: v2.0
RELEASE_STATE: development
CURRENT_PHASE: 2.04
CURRENT_AUTHORITY: docs/release/current.json
LATEST_STABLE: 1.13.19
V2_IMPLEMENTATION: ACTIVE
PRODUCT_GATES: INCOMPLETE
```
<!-- coldkeep-current-state:end -->

Use the current charter in `docs/release/v2.0/README.md` and the explicitly
adopted `coldkeep_phase3c1_build_2026-11-09.md`. The current unit is Phase 3C1:
exact schema20 generation model, checkpointed 19-to-20 upgrade and recovery
continuity. Owner adoption on 10 November 2026 authorizes bounded implementation,
qualification, sequential commits, independent outgoing review and guarded
fast-forward of existing `release/v2.0`, then actual-head hosted observation.
Owner adoption on 11 November 2026 additionally authorizes bounded Go 1.26.11
security maintenance for GO-2026-6859, verified exact toolchain/image pins,
fresh affected qualification, independent review and another guarded update.
Frozen v1 contracts and historical evidence retain their original toolchains.
PR #129 stays draft. The [CI bridge](docs/release/v2.0/ci-acceptance-bridge.md)
records closure at accepted `3aa9cae7b15037e6f51c22311e2b2ee0c0178b86`.
Preserve all predecessor worktrees, evidence and historical result files.
Parent 2.04 and Phase 3C remain ACTIVE; Phase 3C2 OUTSTANDING; implementation
ACTIVE; product gates INCOMPLETE. Ordinary schema18/19/20 writers, reclamation,
capture/publication and IMPORT_READY maintenance/activation remain refused.
No merge, tag, release, protection change, deployment or live switch is granted.

## Correctness rules

- GC must never delete reachable data.
- Restore must not write outside its intended destination.
- Verify must fail closed on inconsistent catalog or storage state.
- Recovery must not legitimize corrupt mappings.
- Packed and legacy storage behavior must remain aligned.
- Destructive, storage, GC, restore, and verify changes require their applicable
  regression-contract evidence before closure.

## Validation and handoffs

Use the current owning unit's validation profile in `docs/release/v2.0/README.md`,
`docs/release/v2.0/ci-followup-result.md` and `PRE_RELEASE_CHECKLIST.md`.
The Phase 3C1 BUILD requires fresh affected complete codec routes with staged real
fixtures, the dedicated recovery requirements, integrated native/browser/Docker
proofs, source/identity negative controls and applicable static checks. The measured
source checkpoints and fresh/reused evidence boundaries are recorded in the
follow-up result. Missing, skipped or pending required evidence is not a
pass. Historical publication's docs-only profile must not substitute for this
implementation qualification.
The baseline commands:

- `python3 scripts/validate_release_state.py --state auto --json`
- `python3 scripts/validate_governance.py`
- `python3 -m unittest discover -s scripts -p 'test_*.py' -v`
- `bash scripts/audit_ci_enforcement.sh --local-only`

Use Go 1.26.11 with `GOTOOLCHAIN=local`; language floor remains Go 1.25.
Inspect TestMain and use a dedicated disposable PostgreSQL server and storage.
Never run tests against a user repository/catalog. Preserve PostgreSQL and
SQLite compatibility within their existing qualified bounds.

Future phase/step instructions for this user specify recommended Codex model,
reasoning effort, execution scope, rationale and completion criteria. This is
personal execution guidance, never a paid-model contributor requirement or CI
gate. Do not change account settings or permanent global configuration.

Respect each phase's `PLAN` or `BUILD` mode. Stop on scope expansion, unexpected
dependency movement or newly discovered private security impact. The adopted
v2 identity transition is authorized, not unexpected release drift. Future
product gates remain incomplete; this unit grants no later action authority.
