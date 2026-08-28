# Coldkeep Critical-Path Coverage Prompt

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

## Frozen v1.x boundaries

v1.13.15 is published stable and planned v1 coverage work is closed. For any
separately authorized future maintenance:

- do not implement v2 or SQLite-first product-default behavior;
- do not change public APIs, schemas, storage formats, or repository formats;
- do not introduce product features;
- do not perform broad refactors;
- do not use coverage work to force style-only cleanup;
- do not remove PostgreSQL compatibility;
- keep changes within the separately authorized plan and allowlist.

V2 planning review is authorized, but v2 implementation and SQLite-first
product defaults require a separate plan.
