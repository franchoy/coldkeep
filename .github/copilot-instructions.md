# Coldkeep Copilot Instructions

Coldkeep is a correctness-first cold storage engine. The primary invariant is: never lose user data.

Correctness, determinism, crash safety, GC safety, restore safety, verification integrity, and compatibility are more important than style, abstraction, or brevity.

The phrase “active v1.13.15 final v1.x closure train” is retained as a
governance compatibility sentinel only; it no longer describes an active
implementation train. v1.13.15 is published stable, and planned v1 work is
closed and frozen.

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

The root `AGENTS.md` and terminal v1.13.15 closure controls are authoritative.
Stop on scope expansion, unexpected dependency movement, release-identity
drift, or newly discovered private security impact.

V2 planning review is authorized. V2 implementation has not started and
requires a separate plan and explicit authorization. Future v1 maintenance is
limited to a newly discovered critical correctness or security defect under a
separate plan.

Codacy is signal, not authority.
Do not chase style-only or generic maintainability warnings at the expense of correctness.
