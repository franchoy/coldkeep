# Coldkeep I/O Performance

This document tracks I/O-focused performance work and the corresponding safety
contracts.

## Phase 8 -- Conservative I/O Optimization

Phase 8 optimizes I/O behavior while preserving crash-safety guarantees.
The ordering remains:

`write bytes -> flush/fsync -> publish metadata`

No storage format, schema, GC, snapshot, or restore semantics changed.

### Safe optimizations list (Phase 8 candidates)

The following candidates are considered safe under the Phase 8 durability
contract when implemented conservatively.

| Candidate | Status |
| --- | --- |
| operation-scoped I/O metrics | implemented |
| prepared statement reuse for snapshot rows | implemented |
| transaction-local snapshot batching | implemented |
| writer-scoped container handle reuse | implemented |
| buffered writes with flush-before-fsync | implemented |
| remove duplicate stat/path normalization | implemented |
| restore reader cleanup | implemented |
| remove redundant copies | partial (ongoing) |

Notes:

- `remove redundant copies` remains partial because copy-elimination opportunities
  must continue to be evaluated case-by-case against readability and error-path
  safety.
- Any future candidate must preserve the publish boundary invariant:
  `write bytes -> flush/fsync -> publish metadata`.

### Dangerous optimizations to avoid (Phase 8)

Do not apply these in Phase 8:

- async container writes
- metadata publish before fsync
- global file descriptor cache
- cross-worker shared writer
- batch fsync across independent logical commits
- change container layout
- change chunk/block layout
- skip verification
- parallel restore output writes
- defer rollback cleanup

These may be valid in a future engine model, but they are intentionally
out-of-scope for the current crash-safety and compatibility contract.

### Scope summary

Phase 8 focused on conservative, low-risk cleanup in store/restore paths:

- remove redundant or duplicated per-file I/O work where safe
- tighten error-path cleanup and rollback behavior
- keep durability boundaries explicit and test-validated
- benchmark continuously after meaningful changes

### Suggested implementation order (Phase 8)

1. Capture focused I/O benchmark baseline.
2. Add operation-scoped I/O metrics.
3. Inspect append/fsync/open-close paths.
4. Add writer-scoped handle reuse if not already present.
5. Add safe buffered writes if beneficial.
6. Remove redundant fsyncs only inside one append operation.
7. Optimize snapshot metadata batching/prepared statements.
8. Remove duplicate stat/path normalization.
9. Validate restore cache lifecycle.
10. Add fault-path tests.
11. Run adversarial/recovery tests.
12. Run benchmark matrix.
13. Document accepted/rejected optimizations.

### Metrics before/after (focus scenarios)

Before = `benchmark-baseline.json` (v1.6 small baseline)
After = Phase 8 Step 12 outputs from `.benchmarks/step12/*.json`

| Scenario | Baseline small w1 (ms) | Phase 8 small w1 (ms) | Delta vs baseline | Phase 8 small w4 (ms) | Delta vs baseline |
| --- | ---: | ---: | ---: | ---: | ---: |
| `snapshot-creation` | 520 | 861 | +66% | 423 | -19% |
| `store-large-file` | 1608 | 3029 | +88% | 2999 | +87% |
| `store-mixed-dataset` | 399 | 741 | +86% | 310 | -22% |
| `gc-after-churn` | 2624 | 3405 | +30% | 2193 | -16% |
| `restore-large-file` | 1790 | 3244 | +81% | 3211 | +79% |

Medium matrix (end-of-phase absolute numbers):

| Scenario | Medium w1 (ms) | Medium w4 (ms) |
| --- | ---: | ---: |
| `snapshot-creation` | 67876 | 36573 |
| `store-large-file` | 33076 | 34291 |
| `store-mixed-dataset` | 65002 | 38402 |
| `gc-after-churn` | 28733 | 21991 |
| `restore-large-file` | 37120 | 37416 |

### Optimizations accepted

- Store-path stat/metadata flow cleanup (single-file metadata path carried in `preparedFile`)
- Restore-path minor cleanup (`errors.Join` close aggregation, safer buffered-writer finalization)
- Error-path and rollback validation expansion for pre-publish failure boundaries
- Crash-safety fault-injection + recovery chain validation
- Benchmark-after-change cadence (small runs per meaningful step, medium matrix at phase end)

### Optimizations rejected

- Restore parallelism inside Phase 8
- Global restore reader cache shared across operations
- Any optimization that weakens fsync/rollback durability boundaries

Reason: correctness and crash-safety boundaries outweigh marginal throughput gains in
this phase.

### Remaining debt

- `store-large-file` remains slower than the v1.6 small baseline in this local profile.
- `restore-large-file` remains slower than the v1.6 small baseline in this local profile.
- Restore unpin batching and related SQL efficiency work remain deferred.
- Deterministic restore parallelism is deferred to a later release with explicit design
  for ordering, memory budget, and failure semantics.

### Phase 9 scope freeze (v1.7 release readiness)

Phase 9 is validation and polish only. Do not add new optimizations unless they
fix a release blocker.

Allowed:

- tests
- docs
- small correctness fixes
- benchmark reporting polish
- release notes
- minor cleanup

Avoid:

- new worker behavior
- new DB indexes
- new I/O batching model
- new CLI contract changes
- new storage/schema changes

### Interpretation

Phase 8 intentionally prioritized durability and safety clarity over aggressive
throughput improvements. The accepted changes are keep-worthy because they are
low risk and improve maintainability/testability of failure boundaries.

Performance-sensitive follow-up work should continue in v1.8/v1.10+ under the same
publish-boundary invariant.

### Phase 8 completion checklist status

Status legend:

- PASS: complete and evidenced
- PARTIAL: complete with explicit debt or scoped exception

#### Baseline

- PASS: focused I/O benchmark baseline captured
- PASS: store-large-file reviewed
- PASS: store-mixed-dataset reviewed
- PASS: snapshot-creation reviewed
- PASS: gc-after-churn reviewed
- PASS: restore-large-file reviewed

Evidence: benchmark outputs and extracted focus set in .benchmarks/step12 and
the metrics tables above.

#### Instrumentation

- PASS: operation-scoped I/O metrics added
- PASS: container opens counted
- PASS: appends counted
- PASS: fsyncs counted
- PASS: bytes read/written counted
- PASS: metrics exposed in benchmark JSON

Evidence: execution_stats and execution_stats.io fields in benchmark JSON.

#### Write path

- PASS: container write buffering reviewed
- PASS: flush-before-fsync preserved
- PASS: fsync-before-metadata-publish preserved
- PASS: redundant fsyncs reviewed and only removed in safe scope
- PASS: writer-scoped handle reuse reviewed/implemented
- PASS: no global writer/cache introduced

#### Snapshot path

- PASS: snapshot creation write/query pattern reviewed
- PASS: batch insert or prepared statement reuse implemented
- PASS: snapshot creation remains atomic
- PASS: no partial snapshot visibility
- PASS: snapshot immutability unchanged

Evidence: snapshot batch failure rollback coverage and existing snapshot contract tests.

#### Store path

- PASS: repeated stat/path normalization reviewed
- PASS: file metadata reused safely
- PASS: no stale filesystem state cached across operations
- PASS: preparedFile flow remains intact
- PASS: commit remains sequential

#### Restore path

- PASS: reader cache lifecycle validated
- PASS: repeated open/close avoided where supported
- PASS: no restore ordering changes
- PASS: no restore verification changes
- PASS: restored bytes unchanged in covered regression/integration paths

#### Safety

- PASS: write failure before metadata publish tested
- PARTIAL: flush failure before metadata publish tested if injectable
- PASS: fsync failure before metadata publish tested
- PASS: rollback after partial append tested
- PASS: snapshot batch failure tested
- PASS: container handles close on error
- PASS: recovery/verify tests pass
- PASS: GC after failure still safe

Notes:

- Flush-failure injection is partial because the write path exposes append/sync
  failure hooks directly; there is no distinct low-level flush seam on the
  container writer interface in the current model.

#### Performance

- PASS: benchmark run after each major optimization (small set)
- PASS: final full benchmark matrix run
- PASS: snapshot-creation regression documented
- PASS: store-large-file regression documented
- PASS: store-mixed-dataset variability documented
- PARTIAL: restore-large-file neutral or improved
- PASS: GC after churn neutral or improved in workers=4 profile
- PASS: no new major regressions introduced outside documented debt items

Notes:

- restore-large-file is marked PARTIAL because it remains slower vs v1.6 small
  baseline in the recorded local profile and is explicitly tracked as debt.

#### Tests

- PASS: I/O behavior tests added
- PASS: fault-injection tests added where feasible
- PASS: snapshot tests pass
- PASS: restore determinism tests pass
- PASS: GC tests pass
- PASS: adversarial tests pass
- PASS: go test ./... passes

#### Documentation

- PASS: docs/io-performance.md added/updated
- PASS: docs/benchmarking.md updated
- PASS: accepted optimizations documented
- PASS: rejected/deferred optimizations documented
- PASS: crash-safety ordering documented
- PASS: no unsafe performance claims added
