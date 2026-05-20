# Coldkeep Copilot Instructions

Coldkeep is a correctness-first cold storage engine. The primary invariant is: never lose user data.

Correctness, determinism, crash safety, GC safety, restore safety, verification integrity, and compatibility are more important than style, abstraction, or brevity.

During v1.10.x:

- do not implement engine extraction;
- do not implement catalog abstraction;
- do not change the default database backend;
- do not introduce product features;
- do not perform broad refactors;
- keep fixes narrow and phase-scoped;
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

Coldkeep is moving toward SQLite-first local repositories while keeping PostgreSQL compatibility tested.
Do not remove PostgreSQL compatibility.
Do not introduce SQLite-only assumptions into engine or catalog contracts.

Codacy is signal, not authority.
Do not chase style-only or generic maintainability warnings at the expense of correctness.