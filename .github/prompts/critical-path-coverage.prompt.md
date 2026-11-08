# Coldkeep Critical-Path Coverage Prompt

<!-- coldkeep-current-state:start -->
```text
SOURCE_VERSION: 2.0.0-dev.0
RELEASE_FAMILY: v2.0
RELEASE_STATE: development
CURRENT_PHASE: 2.02
CURRENT_AUTHORITY: docs/release/current.json
LATEST_STABLE: 1.13.19
V2_IMPLEMENTATION: ACTIVE
PRODUCT_GATES: INCOMPLETE
```
<!-- coldkeep-current-state:end -->

Active v2.0 Phase 2 / action 2.02 is authorized in BUILD mode by
`coldkeep_phase2_codex_2026-11-08.md` and `docs/release/v2.0/README.md`.
Scope: one owned internal fixture daemon, embedded typed UI/API, real historical
restoration and Linux x86-64 CGO SQLite feasibility alongside PostgreSQL Docker.
Accepted 2.01 and local-profile 2.03 COMPLETE reports remain unchanged.
Implementation stays ACTIVE; product gates stay INCOMPLETE. Action 2.04 must not
begin. No schema/payload/recovery-format change or real-repository adoption.
Only the mattn SQLite driver and its checksums may change among Go dependencies;
Go 1.26.10 and unrelated pins remain fixed. Local commits and owned disposable
fixtures are authorized. Preserve predecessor worktrees and frozen v1 records.
No push, PR publication, merge, tag, release or deployment is authorized.
Preserve user changes and data.


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

- keep v2 work within the adopted Phase 2 delivery/feasibility unit; no SQLite-first product-default behavior;
- do not change public APIs, schemas, storage formats, or repository formats;
- do not introduce product features;
- do not perform broad refactors;
- do not use coverage work to force style-only cleanup;
- do not remove PostgreSQL compatibility;
- keep changes within the separately authorized plan and allowlist.

V2 Phase 2 delivery/feasibility work is authorized under the active charter. Work beyond this unit
and SQLite-first product defaults require their later owning phase.
