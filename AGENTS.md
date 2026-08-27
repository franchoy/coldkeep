# Coldkeep Repository Instructions

Coldkeep is correctness-first. The primary invariant is: never lose user data.

## Active authority

- `v1.13.15` is the active final v1.x closure train.
- `v1.13.14` is immutable historical release state. Do not edit its release
  evidence or mutate its tag or GitHub release.
- Do not implement v2, introduce SQLite-first product defaults, or perform
  broad refactors during v1.13.15.
- The active phase list, scope, source/test allowlist, and release gate under
  `docs/release/v1.13/` are binding.
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

Stop and return to Plan mode on scope expansion, unexpected dependency
movement, release-identity drift, or newly discovered private security impact.
