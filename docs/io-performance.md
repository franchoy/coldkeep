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

### Interpretation

Phase 8 intentionally prioritized durability and safety clarity over aggressive
throughput improvements. The accepted changes are keep-worthy because they are
low risk and improve maintainability/testability of failure boundaries.

Performance-sensitive follow-up work should continue in v1.8/v1.10+ under the same
publish-boundary invariant.
