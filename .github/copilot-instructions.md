# Coldkeep Copilot Instructions

<!-- coldkeep-current-state:start -->
```text
SOURCE_VERSION: 1.13.17
RECOVERY_ROUTE: C_SUCCESSOR_VERSION
CURRENT_PHASE: NONE_CLOSURE_CANDIDATE
TRACKED_PUBLICATION_MATERIAL: FROZEN
V1_13_15_STATE: PUBLISHED_STABLE_IMMUTABLE
V1_13_16_STATE: PUBLIC_TAG_FAILED_CERTIFICATION_NO_GITHUB_RELEASE
V1_13_17_STATE: CLOSURE_CANDIDATE_PENDING_TERMINAL_AUDIT
CK-V11316-007: CLOSED_AT_V1.13.16_SOURCE_SCOPE
FINDINGS_CONFIRMED: 15
FINDINGS_CLOSED: 15/15
V1_X_TECHNICAL_CORRECTNESS: ESTABLISHED
V1_X_FULL_CLOSURE: NOT_ESTABLISHED
```
<!-- coldkeep-current-state:end -->

Coldkeep is a correctness-first cold storage engine. The primary invariant is: never lose user data.

Correctness, determinism, crash safety, GC safety, restore safety, verification integrity, and compatibility are more important than style, abstraction, or brevity.

v1.13.17 is published at immutable M17/A17 and GitHub Release `400460267`.
The tracked descendant is an unpublished local closure candidate, limited to
the attached closure-push context repair and governance reconciliation.
The earlier marker `v1.13.17 is the active Route C recovery successor` is
retained only as historical pre-publication wording; it is not current state.
Phases 0-9 serialize as Complete for review, while external terminal
effectiveness and full v1.x closure remain unestablished. This state does not
certify or authorize later operations. v1.13.16 is the
immutable failed-publication predecessor: its
public tag failed required certification, no GitHub Release exists, and no
publication or closure is authorized. v1.13.15 remains
published stable and immutable as the final planned v1.x release; planned v1
feature and architecture work remains closed and frozen.

For future work:

- treat v1.13.14 as immutable historical release state;
- do not implement v2 or SQLite-first product-default behavior;
- do not change public APIs, schema, storage format, or repository format;
- do not perform broad refactors;
- keep fixes narrow and separately planned;
- preserve existing CLI, JSON, and exit-code behavior unless the task explicitly changes it.

For correctness bugs:

1. identify the invariant;
2. add or update a regression test where practical;
3. make the smallest safe fix;
4. run targeted tests;
5. document behavior impact.

GC must never delete reachable data.
Restore must not write outside the intended destination.
Verify must fail closed on inconsistent catalog/storage state.
Recovery must not legitimize corrupt mappings.
Packed and legacy storage behavior must remain aligned.

SQLite and PostgreSQL engine/catalog compatibility is complete v1.x scope.
SQLite-first local productization belongs to v2.x.
Do not remove PostgreSQL compatibility.
Do not introduce SQLite-only assumptions into engine or catalog contracts.

The root `AGENTS.md` and v1.13.17 10-phase controls are authoritative. Follow
the exact phase mode and allowlist; do not perform an out-of-phase repair.
Stop on scope expansion, unexpected dependency movement, release-identity
drift, or newly discovered private security impact.

V2 planning review is authorized. V2 implementation has not started and
requires a separate plan and explicit authorization. Do not introduce broad
refactors, dependency movement, schema/format changes, or unplanned product
work during v1.13.17.

Codacy is signal, not authority.
Do not chase style-only or generic maintainability warnings at the expense of correctness.
