# v1.7.0 - Deterministic Performance Foundation

v1.7.0 improves Coldkeep performance through benchmark-driven, deterministic execution work. It adds a reusable benchmark suite, explicit execution options, hardened store-folder worker behavior, optimized store preparation, restore read-path improvements, DB query-shape cleanup, and conservative I/O optimization.

The release preserves Coldkeep's core guarantees: deterministic restore, GC safety, snapshot correctness, and crash-safety assumptions.

## Scope and positioning

- This release focuses on controlled execution and measurement-backed tuning.
- It is not a fully concurrent daemon release.
- It introduces no storage-format change.
- It introduces no schema-breaking change.

## Operator-visible highlights

- Benchmarking command surface with reusable scenarios and machine-readable outputs.
- Execution controls in benchmark paths to support deterministic comparisons.
- Store-folder worker-path hardening and safer operational behavior under load.
- Store preparation and restore read-path optimizations for better runtime characteristics.
- DB query-shape cleanup and conservative I/O refinements.

## Validation framing

- Determinism and safety gates remain central release criteria.
- Benchmark comparisons are reported explicitly, including regressions when present.
- CLI compatibility is preserved for existing operator workflows.
