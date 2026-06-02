# Coldkeep v1.12 Phase Prompt

You are working on Coldkeep v1.12.

v1.12 goal:
Move business orchestration into the engine so the CLI becomes a thin wrapper, and introduce
catalog/metadata APIs for graph, placement, restore-plan, GC-plan, snapshot graph, reachability, and
storage placement decisions.

Rules:

- preserve CLI behavior;
- preserve JSON shape;
- preserve exit codes;
- preserve repository/storage format;
- do not change the default database backend;
- do not remove PostgreSQL compatibility;
- do not introduce SQLite-only assumptions;
- do not implement daemon/API/UI/NAS/cloud/multi-user work;
- do not perform unrelated cleanup;
- keep changes phase-scoped.

Before editing:

1. Identify phase goal.
2. Identify affected invariant.
3. List included files.
4. List out-of-scope files.
5. Identify behavior parity tests.
6. Identify SQLite/PostgreSQL impact.
7. Identify packed/legacy impact.
8. Identify risk-register updates.

During implementation:

- make the smallest safe change;
- prefer behavior-preserving adapters before rewrites;
- keep CLI as parser/request-builder/renderer;
- move orchestration to engine only when request/result contracts are complete;
- move metadata decisions behind catalog APIs only with contract tests.

Final response:

- invariant protected;
- behavior impact;
- files changed;
- tests run;
- SQLite impact;
- PostgreSQL impact;
- packed/legacy impact;
- remaining risk;
- phase checklist result.
