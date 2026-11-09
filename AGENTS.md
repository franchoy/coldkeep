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

The v2.0 Phase 3B follow-up R1 BUILD authorized by
`coldkeep_phase3b_followup_codex_2026-11-09.md` is COMPLETE, awaiting independent
review. R1 lossless operator root selection is corrected and qualified locally.
See `docs/release/v2.0/phase3b-followup-result.md`; preserve the submitted
`docs/release/v2.0/phase3b-result.md` byte-for-byte. Only a qualified follow-up
can discharge R1; submitted Phase 3B cannot. Phase 3C stays OUTSTANDING.
Preserve accepted reports, predecessor worktrees, frozen v1 and all pins.
Parent 2.04 stays ACTIVE; implementation ACTIVE; product gates INCOMPLETE.
Ordinary schema18/19 writers and IMPORT_READY maintenance/activation refuse.
Phase 3C and live switching remain separate. Local commits and owned disposable
fixtures are authorized. No push, publication, merge, tag or release.

## Correctness rules

- GC must never delete reachable data.
- Restore must not write outside its intended destination.
- Verify must fail closed on inconsistent catalog or storage state.
- Recovery must not legitimize corrupt mappings.
- Packed and legacy storage behavior must remain aligned.
- Destructive, storage, GC, restore, and verify changes require their applicable
  regression-contract evidence before closure.

## Validation and handoffs

Use the bounded Phase 3B follow-up R1 validation profile in `docs/release/v2.0/README.md`
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
