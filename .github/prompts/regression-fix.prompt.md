# Coldkeep Regression Fix Prompt

You are fixing a Coldkeep regression or correctness bug.

Coldkeep is a correctness-first cold storage engine. The primary invariant is: never lose user data.

## Goal

Fix the reported bug with the smallest safe change, preserve existing behavior unless the bug requires a behavior change, and add regression coverage so the issue cannot silently return.

## Before editing

Identify and state:

1. The reported bug.
2. The affected command, package, or workflow.
3. The violated invariant.
4. Whether the issue affects:
   - data loss risk,
   - GC safety,
   - restore determinism,
   - verification integrity,
   - recovery safety,
   - CLI contract,
   - JSON contract,
   - SQLite/PostgreSQL compatibility,
   - CI/release integrity.
5. The expected correct behavior.
6. The smallest test that should fail before the fix.

## Rules

- Do not perform broad refactors.
- Do not introduce unrelated cleanup.
- Do not change public behavior unless required to fix the bug.
- Do not change CLI output, JSON shape, or exit-code behavior unless the bug is specifically about those contracts.
- v1.13.16 is the active exceptional critical-maintenance train with seven
  Open findings and none fixed; follow its 20-phase plan and exact phase mode.
- v1.13.15 remains published stable and immutable; planned v1 feature and
  architecture work remains closed and frozen.
- Do not implement v2 or SQLite-first product-default behavior without a
  separate implementation plan.
- Do not change public APIs, schemas, storage formats, or repository formats.
- Keep changes within the active phase's exact plan and allowlist; do not make
  an out-of-phase repair.
- Do not remove PostgreSQL compatibility.
- Do not introduce SQLite-only assumptions into engine or catalog contracts.
- Do not close issue-tracker or remediation-matrix rows unless explicitly asked.

## Preferred workflow

1. Locate the smallest responsible code path.
2. Add or update a regression test where practical.
3. Confirm the test fails on the current behavior, if feasible.
4. Apply the smallest safe fix.
5. Run the targeted test.
6. Run the smallest relevant package test.
7. Recommend broader tests only when needed for release confidence.

## Test expectations

For CLI regressions, prefer tests that prove:

- invalid positional arguments are rejected;
- empty values are rejected;
- boolean flags behave consistently;
- duplicate unsafe singleton flags are rejected;
- `--json` and `--output json` behavior is consistent;
- no human output leaks into strict JSON paths;
- validation happens before repository/storage initialization where applicable.

For restore/recovery regressions, prefer tests that prove:

- traversal is rejected;
- symlinks do not redirect writes outside the destination;
- overwrite behavior is explicit;
- interrupted operations leave deterministic recoverable state;
- temp/rename cleanup is safe;
- stored-path restore modes behave consistently.

For GC regressions, prefer tests that prove:

- reachable data is never reclaimable;
- snapshot roots are respected;
- packed and legacy reachability agree;
- dry-run and live plans are equivalent;
- repair does not legitimize invalid mappings.

For verify regressions, prefer tests that prove:

- corrupted data fails verification;
- missing storage fails closed;
- packed and legacy verification coverage is aligned;
- false success is impossible for the reported case.

For catalog/database regressions, prefer tests that prove:

- SQLite behavior is correct;
- PostgreSQL behavior remains compatible where feasible;
- SQL dialect assumptions stay behind adapters;
- migrations remain deterministic.

## Output required after editing

Provide a concise summary with:

- Bug fixed:
- Invariant protected:
- Behavior change:
- Files changed:
- Tests added or updated:
- Tests run:
- Compatibility impact:
- Remaining risk or deferred work:

## Do not claim success unless

- the regression test exists or a clear reason is given for why direct regression coverage was not practical;
- the targeted test passes;
- the fix is limited to the reported issue;
- no unrelated behavior changes were introduced.
