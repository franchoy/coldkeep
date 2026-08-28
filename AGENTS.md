# Coldkeep Repository Instructions

Coldkeep is correctness-first. The primary invariant is: never lose user data.

## Active authority

- `v1.13.15` is published stable and is the final planned v1.x release.
- v1.x planned feature and architecture work is closed and frozen. A future
  v1 maintenance train requires a newly discovered critical correctness or
  security defect, a separate plan, and explicit authorization.
- `v1.13.14` is immutable historical release state. Do not edit its release
  evidence or mutate its tag or GitHub release.
- Do not implement v2. V2 planning review is authorized, but implementation
  requires a separate plan and explicit authorization.
- Do not introduce SQLite-first product defaults or perform broad refactors
  without the separately authorized future phase that owns them.
- The terminal v1.13.15 closure bundle, release state, scope, source/test
  allowlist, and release gate under `docs/release/v1.13/` are binding.
- Respect each phase's `PLAN` or `BUILD` mode and stop at its authorization
  boundary.

## Correctness rules

- GC must never delete reachable data.
- Restore must not write outside its intended destination.
- Verify must fail closed on inconsistent catalog or storage state.
- Recovery must not legitimize corrupt mappings.
- Packed and legacy storage behavior must remain aligned.
- Destructive, storage, GC, restore, and verify changes require their applicable
  regression-contract evidence before closure.

## Validation

Use the canonical commands in `PRE_RELEASE_CHECKLIST.md` and the active
v1.13.15 validation checklist. At minimum, run focused tests for the changed
area before the broader applicable gate. Do not represent unavailable hosted
evidence as passing.

The baseline repository-governance commands are:

- `python3 scripts/validate_release_state.py --state development --json`
- `python3 scripts/validate_governance.py`
- `python3 -m unittest discover -s scripts -p 'test_*.py' -v`
- `bash scripts/audit_ci_enforcement.sh --local-only`

The frozen v1 release-critical execution contract uses Go 1.26.7 with
`GOTOOLCHAIN=local`; the module language floor remains Go 1.25.

    V1_X: CLOSED_AND_FROZEN
    V1_13_15: PUBLISHED_STABLE
    V1_13_15_IS_FINAL_PLANNED_V1_RELEASE: YES
    V1_PLANNED_FEATURE_WORK: NONE
    V2_PLANNING_REVIEW: AUTHORIZED
    V2_IMPLEMENTATION: NOT_STARTED
    V2_IMPLEMENTATION_AUTHORIZATION: REQUIRES_SEPARATE_PLAN

Stop and return to Plan mode on scope expansion, unexpected dependency
movement, release-identity drift, or newly discovered private security impact.
