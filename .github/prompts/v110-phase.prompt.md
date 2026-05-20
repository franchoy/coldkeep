# Coldkeep v1.10 Phase Prompt

You are working on a Coldkeep v1.10.x stabilization phase.

Before editing:

1. Identify the phase goal.
2. Identify the core invariant.
3. List expected files.
4. List out-of-scope files.
5. Identify required tests.
6. Identify docs/checklists that may need updates.

Rules:

- no broad refactor;
- no engine extraction;
- no catalog abstraction;
- no default DB behavior change;
- no product feature work;
- no unrelated cleanup;
- tests before fixes where practical;
- preserve CLI/JSON contracts unless explicitly changing them;
- do not close tracker/matrix rows unless explicitly asked.

Implementation:

- make the smallest safe change;
- prefer regression or invariant tests;
- avoid changing unrelated packages.

Final response:

- invariant protected;
- files changed;
- tests run;
- behavior change;
- compatibility impact;
- remaining risk.