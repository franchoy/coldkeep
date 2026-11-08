# Coldkeep Repository Instructions

Coldkeep is correctness-first. The primary invariant is: never lose user data.

## Active authority

<!-- coldkeep-current-state:start -->
```text
SOURCE_VERSION: 2.0.0-dev.0
RELEASE_FAMILY: v2.0
RELEASE_STATE: development
CURRENT_PHASE: 2.03
CURRENT_AUTHORITY: docs/release/current.json
LATEST_STABLE: 1.13.19
V2_IMPLEMENTATION: ACTIVE
PRODUCT_GATES: INCOMPLETE
```
<!-- coldkeep-current-state:end -->

Active v2.0 Phase 1D / action 2.03 is authorized in BUILD mode by the
8 November 2026 constrained-import instruction and `docs/release/v2.0/README.md`.
The accepted Phase 1C C1–C3 follow-up is preserved. Scope: constrained fresh-target
import, actual installation-loss recovery and fresh recovery after real live GC.
Local-profile action 2.03 is COMPLETE after the F1–F2 correction and qualification
under `coldkeep_phase1d_followup_codex_2026-11-08.md`; see
`docs/release/v2.0/phase1d-followup-result.md`. The submitted Phase 1D report
remains byte-identical historical evidence. Product gates stay INCOMPLETE,
implementation ACTIVE, and action 2.04 must not begin. No daemon/UI,
schema migration, real-repository adoption or release publication is authorized.
Preserve predecessor worktrees and frozen v1 records. Local commits are authorized.
No push, PR publication, merge, tag, release or deployment is authorized.
Dependency/toolchain versions remain pinned. Preserve user changes and data.

## Correctness rules

- GC must never delete reachable data.
- Restore must not write outside its intended destination.
- Verify must fail closed on inconsistent catalog or storage state.
- Recovery must not legitimize corrupt mappings.
- Packed and legacy storage behavior must remain aligned.
- Destructive, storage, GC, restore, and verify changes require their applicable
  regression-contract evidence before closure.

## Validation and handoffs

Use the bounded Phase 1D validation profile in `docs/release/v2.0/README.md`
and `PRE_RELEASE_CHECKLIST.md`. Run focused checks before applicable broader
gates. Missing hosted or platform evidence is not a pass. The baseline commands:

- `python3 scripts/validate_release_state.py --state auto --json`
- `python3 scripts/validate_governance.py`
- `python3 -m unittest discover -s scripts -p 'test_*.py' -v`
- `bash scripts/audit_ci_enforcement.sh --local-only`

Use Go 1.26.10 with `GOTOOLCHAIN=local`; language floor remains Go 1.25.
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
