# Coldkeep Regression Fix Prompt

<!-- coldkeep-current-state:start -->
```text
SOURCE_VERSION: 2.0.0-dev.0
RELEASE_FAMILY: v2.0
RELEASE_STATE: development
CURRENT_PHASE: 2.04
CURRENT_AUTHORITY: docs/release/current.json
LATEST_STABLE: 1.13.19
V2_IMPLEMENTATION: ACTIVE
PRODUCT_GATES: INCOMPLETE
```
<!-- coldkeep-current-state:end -->

Use `docs/release/v2.0/README.md` and the current explicitly authorized BUILD
for execution scope. The owner adopted the bounded M1/E1/E2/C1/Q1 CI follow-up
on 9 November 2026, including a guarded fast-forward of existing `release/v2.0`
after local qualification and independent review. PR #129 remains draft.
`docs/release/v2.0/ci-followup-result.md` is the shared current evidence record;
corrected-head hosted checks remain PENDING until both push and PR observations
bind the actual final public head. `development-visibility.md` records the adopted
scope; documentation alone grants no additional authority. Accepted R1 and
prior publication records retain their historical source/evidence boundaries.
Accepted 2.01, bounded 2.02 and local-profile 2.03 remain COMPLETE. Parent 2.04
and implementation stay ACTIVE; product gates INCOMPLETE; Phase 3C OUTSTANDING.
Preserve phase results, predecessor worktrees, frozen v1 and every dependency
pin. Ordinary schema18/19 writers and IMPORT_READY activation remain refused.
No merge, tag, release, protection change, deployment or live switch is granted.
Preserve user changes and data.

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
- v1.13.19 is published, stable, immutable, and latest at D19
  `d3581602aee2599547564322fe083baae1d52063`, annotated A19
  `60b6878114f936b295c415605e8af31def2aba5a`, and source-only GitHub Release
  `431395975`. Natural tag CI and CodeQL passed on attempt 1, public readback
  passed, and owner acceptance was granted. V1.x product/runtime development
  is complete and frozen. The completed exact 34-path recovery boundary
  preserves every product, dependency, technical, and frozen historical byte.
- v1.13.18 remains a public-tag certification NONPASS with no GitHub Release.
  Do not move A18, erase the failed run, conflate the workspace NONPASS with
  the isolated `/tmp` PASS, or infer a writer for the reappearing fixture.
- v1.13.17 is published at immutable M17/A17 and GitHub Release `400460267`.
  The tracked descendant is a non-authorizing terminal-disposition declaration.
  Phase 9M established source-scoped qualified external closure at C; protected-
  main serialization and effectiveness readback remain separate. This state
  does not certify or authorize later operations. v1.13.16 is the
  immutable failed-publication predecessor. The successor's completed 10-phase
  plan remains historical evidence.
- v1.13.15 remains published stable and immutable; planned v1 feature and
  architecture work remains closed and frozen.
- Keep v2 implementation within the current explicitly authorized unit;
  SQLite-first defaults require their later owning phase.
- Do not change public APIs, schemas, storage formats, or repository formats.
- New v1 product/runtime work requires separate explicit authority; keep any
  authorized repair within its exact plan and allowlist.
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
