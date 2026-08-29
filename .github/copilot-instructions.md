# Coldkeep Copilot Instructions

Coldkeep is a correctness-first cold storage engine. The primary invariant is: never lose user data.

Correctness, determinism, crash safety, GC safety, restore safety, verification integrity, and compatibility are more important than style, abstraction, or brevity.

v1.13.16 is the active exceptional critical-maintenance train. Seven findings
are Open and none is fixed or closed. v1.13.15 remains published stable and
immutable as the final planned v1.x release; planned v1 feature and
architecture work remains closed and frozen.

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

The root `AGENTS.md` and v1.13.16 20-phase controls are authoritative. Follow
the exact phase mode and allowlist; do not perform an out-of-phase repair.
Stop on scope expansion, unexpected dependency movement, release-identity
drift, or newly discovered private security impact.

V2 planning review is authorized. V2 implementation has not started and
requires a separate plan and explicit authorization. Do not introduce broad
refactors, dependency movement, schema/format changes, or unplanned product
work during v1.13.16.

Codacy is signal, not authority.
Do not chase style-only or generic maintainability warnings at the expense of correctness.
