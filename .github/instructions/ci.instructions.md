# Coldkeep CI Instructions

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
- a reason for broad refactors during the active v1.13.15 closure train;
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

## v1.13.15 Release Boundary

v1.13.15 is published stable and planned v1 work is closed and frozen. Do not
use CI work to introduce:

- v2 implementation or SQLite-first product-default behavior;
- public API, schema, storage-format, or repository-format changes;
- product features;
- broad refactors;
- unassigned dependency or toolchain movement;
- required gates outside the active phase allowlist.

Treat v1.13.14 release evidence as immutable historical state. Preserve the
v1.13.15 immutable product/release identity. Future v1 maintenance requires a
new critical correctness or security defect and a separate authorized plan.
V2 planning review is authorized; v2 implementation requires a separate plan.
Stop on release identity drift or newly discovered private security impact.

## Required Review Questions

Before proposing a CI change, answer:

1. Which correctness invariant does this protect?
2. Which command or workflow validates it?
3. Is it release-blocking or advisory?
4. Could it create noisy failures?
5. Does it preserve PostgreSQL compatibility?
6. Does it avoid style-only blocking?
7. Does it fit the current release phase?
