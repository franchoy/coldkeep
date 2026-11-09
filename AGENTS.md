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
authorized owning BUILD. The current unit is the bounded hosted-CI follow-up
(M1 installation, E1 cancellation, E2 composition, C1 fixtures and Q1 proof
credentials/cleanup). On 9 November 2026 the owner adopted its launch prompt,
including a guarded fast-forward update of existing `release/v2.0` after local
qualification and independent outgoing review. Initial publication is accepted
at `ac9382c9e620b17b43800c2a5cf0065e9e75446d`; existing PR #129 stays draft.
See `docs/release/v2.0/ci-followup-result.md` for measured checkpoint evidence
and `docs/release/v2.0/next-slice.md` for the remaining handoff. Hosted results
at the correction's final public head are PENDING and require separate actual
push/PR observation; local qualification is not hosted success.
The accepted R1 report and all earlier local-only records remain historical,
unchanged evidence. This document records the adopted scope, not new authority.
Preserve accepted reports, predecessor worktrees, frozen v1 and all pins.
Parent 2.04 stays ACTIVE; implementation ACTIVE; product gates INCOMPLETE.
Phase 3C stays OUTSTANDING. Ordinary schema18/19 writers and IMPORT_READY
maintenance/activation refuse. No merge, tag, release, protection change,
deployment or live installation switch is granted by this workflow.

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
The CI follow-up requires both complete codec package routes with staged real
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
