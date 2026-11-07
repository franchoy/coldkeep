# Coldkeep Critical-Path Coverage Prompt

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


You are helping improve Coldkeep test coverage.

Coldkeep is a correctness-first cold storage engine. The primary invariant is: never lose user data.

## Goal

Improve coverage where it protects correctness-critical behavior.

Do not chase global coverage percentages. Do not add shallow tests only to improve metrics.

## Coverage Priority Order

Prioritize coverage for:

1. GC reachability and deletion safety.
2. Restore and snapshot restore determinism.
3. Verify and integrity failure detection.
4. Crash/recovery behavior.
5. Repair behavior and fail-closed semantics.
6. Packed/legacy storage parity.
7. Catalog mutation and interpretation.
8. Snapshot lifecycle and retention behavior.
9. CLI/JSON contract stability.
10. SQLite/PostgreSQL compatibility.

## Before proposing tests

Identify:

1. The invariant being protected.
2. The affected command, package, or workflow.
3. The existing tests that already cover the path.
4. The missing branch, edge case, or failure mode.
5. Whether the test should be:
   - unit,
   - integration,
   - adversarial,
   - regression,
   - race/concurrency,
   - cross-backend,
   - CLI contract,
   - documentation/process validation.
6. Whether the test belongs in the current release phase.

## Good Coverage Targets

Prefer tests for:

- live vs dry-run GC parity;
- snapshot-retained data;
- shared/deduplicated block reachability;
- restore path sanitization;
- symlink and traversal rejection;
- malformed packed metadata;
- missing/truncated physical containers;
- verify fail-closed behavior;
- repair refusing to legitimize corrupt mappings;
- interrupted writes and temp/rename cleanup;
- CLI JSON shape and exit codes;
- PostgreSQL and SQLite catalog parity where applicable;
- release-checklist validation and documentation consistency.

## Avoid Bad Coverage

Avoid:

- tests that only execute code without assertions;
- tests that duplicate existing behavior without new invariant coverage;
- broad fixture churn;
- style-only tests;
- brittle timing tests;
- tests that require unnecessary external services;
- global coverage thresholds without critical-path justification;
- changing behavior only to make tests easy.

## Required Output

When proposing coverage, provide:

- invariant protected;
- current coverage;
- gap;
- proposed test name;
- test type;
- expected failure before fix, if regression;
- expected command to run;
- whether it is release-blocking or advisory;
- scope risks.

## Frozen v1.13.19 boundaries

v1.13.19 is published, stable, immutable, and latest at D19
`d3581602aee2599547564322fe083baae1d52063`, annotated A19
`60b6878114f936b295c415605e8af31def2aba5a`, and source-only GitHub Release
`431395975`. Natural tag CI and CodeQL passed on attempt 1, public readback
passed, and owner acceptance was granted. V1.x product/runtime development is
complete and frozen. The completed finite source recovery made no
product-coverage expansion. Its reviewed fixture fix and strict lifecycle
tests preserve production
CKRS017, A18/A17, all product/dependency bytes, 13 technical tuples, and frozen
v1.13.17/v1.13.18 documents. v1.13.18 remains a public-tag certification
NONPASS with no GitHub Release; its passing bounded evidence does not certify
the v1.13.19 certificate. The completed 10-phase plan and exact 34-path
boundary remain historical evidence:

- keep v2 work within adopted action 2.01; no SQLite-first product-default behavior;
- do not change public APIs, schemas, storage formats, or repository formats;
- do not introduce product features;
- do not perform broad refactors;
- do not use coverage work to force style-only cleanup;
- do not remove PostgreSQL compatibility;
- keep changes within the separately authorized plan and allowlist.

V2 action 2.01 is authorized under the active charter. Work beyond 2.01
and SQLite-first product defaults require their later owning phase.
