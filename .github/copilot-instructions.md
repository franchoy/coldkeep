# Coldkeep Copilot Instructions

<!-- coldkeep-current-state:start -->
```text
SOURCE_VERSION: 2.0.0-dev.0
RELEASE_FAMILY: v2.0
RELEASE_STATE: development
CURRENT_PHASE: 2.03
CURRENT_AUTHORITY: docs/release/current.json
LATEST_STABLE: 1.13.19
V2_IMPLEMENTATION: ACTIVE
PRODUCT_GATES: INCOMPLETE
```
<!-- coldkeep-current-state:end -->

Active v2.0 Phase 1B / action 2.03 is authorized in BUILD mode by the accepted
8 November 2026 passive/inventory instruction and `docs/release/v2.0/README.md`.
`docs/release/current.json` remains current authority; parent action 2.03 stays
ACTIVE. Scope: passive existing-repository opening and a quiesced full-catalog
recovery inventory, plus the accepted CI-audit correction. Phase 1A / 2.01 is
accepted complete and preserved. Latest stable remains v1.13.19; frozen
`docs/release/v1*/` records retain their historical bytes. No package copy,
completion/import protocol, daemon/UI, schema, capture or migration belongs to
this unit. Product gates and full action 2.03 remain incomplete.
No push, PR publication, merge, tag, release or deployment is authorized.
Dependency/toolchain versions remain pinned. Preserve user changes and data.


Coldkeep is a correctness-first cold storage engine. The primary invariant is: never lose user data.

Correctness, determinism, crash safety, GC safety, restore safety, verification integrity, and compatibility are more important than style, abstraction, or brevity.

v1.13.19 is published, stable, immutable, and latest at D19
`d3581602aee2599547564322fe083baae1d52063`, annotated A19
`60b6878114f936b295c415605e8af31def2aba5a`, and source-only GitHub Release
`431395975`. Natural tag CI and CodeQL passed on attempt 1, public readback
passed, and independent owner acceptance was granted. V1.x product/runtime
development is complete and frozen. The completed 34-path recovery boundary
preserves all product/dependency bytes, 13 technical tuples, and frozen
v1.13.17/v1.13.18 documents. v1.13.18
remains a public-tag certification NONPASS with no GitHub Release; do not move
A18, erase its failed run, conflate the workspace NONPASS with the isolated
`/tmp` PASS, or infer a writer. v1.13.17 remains published and immutable at
M17/A17 and Release `400460267`; v1.13.16 remains an immutable failed-
publication predecessor; v1.13.15 remains published stable and immutable.

For future work:

- treat v1.13.14 as immutable historical release state;
- keep v2 work within the adopted Phase 1B passive/inventory unit; no SQLite-first product-default behavior;
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

The root `AGENTS.md` and `docs/release/current.json` are current
authority; v1 closure is frozen historical evidence. The completed 10-phase controls remain historical evidence. Do not
start new v1 product work or mutate a tag or Release without separate explicit
authority. Stop on scope expansion, unexpected dependency movement,
release-identity drift, or newly discovered private security impact.


Codacy is signal, not authority.
Do not chase style-only or generic maintainability warnings at the expense of correctness.
