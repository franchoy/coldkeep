# Coldkeep CI Instructions

<!-- coldkeep-current-state:start -->
```text
SOURCE_VERSION: 2.0.0-dev.0
RELEASE_FAMILY: v2.0
RELEASE_STATE: development
CURRENT_PHASE: 2.01
CURRENT_AUTHORITY: docs/release/current.json
LATEST_STABLE: 1.13.19
V2_IMPLEMENTATION: ACTIVE
PRODUCT_GATES: INCOMPLETE
```
<!-- coldkeep-current-state:end -->

Active v2.0 Phase 1A / action 2.01 is authorized in BUILD mode by the accepted
7 November 2026 instruction and `docs/release/v2.0/README.md`;
`docs/release/current.json` is current authority. Scope: baseline/compatibility
ledger, version domains, executable fixtures, product contracts, profile
statuses, result glossary and applicable evidence. Latest stable remains
v1.13.19; frozen `docs/release/v1*/` records retain their historical bytes.
Do not implement passive opening, daemon/UI, storage/schema, capture, migration
or recovery-export/import features in 2.01. Actions 2.02 and 2.03 are separate.
No push, PR publication, merge, tag, release or deployment is authorized.
Dependency/toolchain versions remain pinned. Preserve user changes and data.


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

## Frozen v1.13.19 Boundary

v1.13.19 is published, stable, immutable, and latest at D19
`d3581602aee2599547564322fe083baae1d52063`, annotated A19
`60b6878114f936b295c415605e8af31def2aba5a`, and source-only GitHub Release
`431395975`. Natural tag CI and CodeQL passed on attempt 1, public readback
passed, and independent owner acceptance was granted. V1.x product/runtime
development is complete and frozen. The completed exact 34-path recovery
delta changed only the reviewed fixture isolation, version/install anchors,
finite validators/tests, current authority, and recovery records. It preserves
Go 1.26.10, compress v1.20.2, every product/dependency byte, all 13 technical
tuples, and frozen v1.13.17/v1.13.18 documents.

v1.13.18 remains a genuine public-tag certification failure with no GitHub
Release. Do not rerun or relabel its failed gate, move A18, infer a writer for
the workspace fixture reappearance, or substitute its passing CodeQL/install
dimensions for v1.13.19 evidence. The completed v1.13.19 10-phase plan and
exact 34-path boundary remain historical authority. Do not use CI work to
introduce:

- v2 work beyond adopted action 2.01 or SQLite-first product-default behavior;
- public API, schema, storage-format, or repository-format changes;
- product features;
- broad refactors;
- unassigned dependency or toolchain movement;
- unplanned gates or lifecycle work without separate authority.

Treat v1.13.14 and v1.13.15 release evidence, and the v1.13.16
failed-publication disposition, as immutable historical state.
Do not perform out-of-phase repair, dependency movement, or schema/format
change. V2 action 2.01 is authorized under the active charter. Stop on release identity drift or newly discovered private
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
