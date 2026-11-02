# Coldkeep Copilot Instructions

<!-- coldkeep-current-state:start -->
```text
SOURCE_VERSION: 1.13.19
RELEASE_ROUTE: FINAL_V1_SOURCE_SNAPSHOT_RECOVERY
CURRENT_PHASE: NONE_POST_RELEASE_CLOSED
RELEASE_STATE: POST_RELEASE_CLOSED
TRACKED_PUBLICATION_MATERIAL: FROZEN
V1_13_15_STATE: PUBLISHED_STABLE_IMMUTABLE
V1_13_16_STATE: PUBLIC_TAG_FAILED_CERTIFICATION_NO_GITHUB_RELEASE
V1_13_17_STATE: PUBLISHED_IMMUTABLE_TERMINAL_EFFECTIVE_QUALIFIED
V1_13_18_STATE: PUBLIC_TAG_FAILED_CERTIFICATION_NO_GITHUB_RELEASE
V1_13_19_STATE: PUBLISHED_STABLE_IMMUTABLE_LATEST
CK-V11316-007: CLOSED_AT_V1.13.16_SOURCE_SCOPE
FINDINGS_CONFIRMED: 15
FINDINGS_CLOSED: 15/15
V1_X_TECHNICAL_CORRECTNESS: ESTABLISHED
V1_X_FULL_CLOSURE: COMPLETE_EFFECTIVE
V1_13_19_PUBLICATION: COMPLETE
V1_X: CLOSED_AND_FROZEN
V2_IMPLEMENTATION: NOT_STARTED
```
<!-- coldkeep-current-state:end -->

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

The root `AGENTS.md` and v1.13.19 post-publication closure are current
authority. The completed 10-phase controls remain historical evidence. Do not
start new v1 product work or mutate a tag or Release without separate explicit
authority. Stop on scope expansion, unexpected dependency movement,
release-identity drift, or newly discovered private security impact.

V2 planning review is authorized. V2 implementation has not started and
requires a separate plan and explicit authorization. Do not introduce broad
refactors, dependency movement, schema/format changes, or unplanned product
work. Preserve every v1.13.17 and v1.13.18 release document and all retained
historical evidence unchanged.

Codacy is signal, not authority.
Do not chase style-only or generic maintainability warnings at the expense of correctness.
