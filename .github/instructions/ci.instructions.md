# Coldkeep CI Instructions

<!-- coldkeep-current-state:start -->
```text
SOURCE_VERSION: 1.13.16
PHASE_12: COMPLETE
PHASE_13: COMPLETE
CK-V11316-007: CLOSED
FINDINGS_CONFIRMED: 15
FINDINGS_CLOSED: 15/15
V1_X_TECHNICAL_CORRECTNESS: ESTABLISHED
V1_X_FULL_CLOSURE: NOT_ESTABLISHED
```
<!-- coldkeep-current-state:end -->

Coldkeep CI exists to protect correctness, determinism, and release confidence.

Coldkeep is a correctness-first cold storage engine. The primary invariant is: never lose user data.

## CI Priority Order

When proposing or reviewing CI changes, prioritize:

1. Data-loss prevention.
2. Restore determinism.
3. GC safety.
4. Verification integrity.
5. Crash/recovery behavior.
6. Packed/legacy storage parity.
7. SQLite/PostgreSQL compatibility.
8. CLI and JSON contract stability.
9. Reproducible release gates.
10. Actionable security/dependency findings.

Do not prioritize style-only checks above correctness evidence.

## Preferred CI Improvements

Prefer CI improvements that strengthen:

- race detection;
- adversarial tests;
- snapshot lifecycle validation;
- restore/verify/GC invariants;
- packed and legacy parity;
- deterministic CLI/JSON contracts;
- migration/bootstrap behavior;
- dependency vulnerability visibility;
- critical-path coverage;
- release checklist reproducibility.

## Avoid Noisy Gates

Do not recommend CI gates that block releases only for:

- naming preferences;
- style-only lint;
- generic maintainability scoring;
- architecture opinions;
- broad complexity complaints without correctness risk;
- duplicated explicit invariant logic;
- test-only formatting churn.

Codacy is signal, not authority.

## Codacy Boundary

Codacy may be used for:

- PR annotations;
- trend visibility;
- dependency/security surfacing;
- unchecked-error visibility;
- duplicate-code visibility in critical paths;
- maintainability trend tracking.

Codacy must not be treated as:

- architecture authority;
- correctness authority;
- invariant authority;
- a reason for broad refactors during the active v1.13.16 maintenance train;
- a blocker for style-only findings.

## Coverage Boundary

Prefer critical-path coverage over global coverage.

Critical-path coverage should focus on:

- storage writes and reads;
- restore and snapshot restore;
- verify and integrity checks;
- GC reachability and deletion planning;
- repair/recovery behavior;
- catalog mutation and interpretation;
- migration/bootstrap behavior.

Do not add global coverage gates unless explicitly approved.

## v1.13.16 Maintenance Boundary

v1.13.16 is the active exceptional critical-maintenance train. Phase 12
technical correctness is established, Phase 13 is complete, CK-V11316-007 is
closed, and all 15 findings are closed in the tracked candidate. Phase 14 is
Next but remains non-executable pending fresh hosted certification and final
read-only Phase 13C recertification of this exact head. v1.13.15 remains
published stable and immutable. Use development-state validation on
`release/v1.13.16`, follow the 20-phase plan and exact phase mode, and do not
use CI work to introduce:

- v2 implementation or SQLite-first product-default behavior;
- public API, schema, storage-format, or repository-format changes;
- product features;
- broad refactors;
- unassigned dependency or toolchain movement;
- required gates outside the active phase allowlist.

Treat v1.13.14 and v1.13.15 release evidence as immutable historical state.
Do not perform out-of-phase repair, dependency movement, or schema/format
change. V2 planning review is authorized; v2 implementation requires a
separate plan. Stop on release identity drift or newly discovered private
security impact.

## Required Review Questions

Before proposing a CI change, answer:

1. Which correctness invariant does this protect?
2. Which command or workflow validates it?
3. Is it release-blocking or advisory?
4. Could it create noisy failures?
5. Does it preserve PostgreSQL compatibility?
6. Does it avoid style-only blocking?
7. Does it fit the current release phase?
