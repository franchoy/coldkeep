# Coldkeep v1.10 Freeze Declaration

Status: Active  
Applies to: v1.10.x release train  
Declared in: v1.10.0  
Next architectural milestone: v1.11 engine boundary introduction

## 1. Declaration

Coldkeep v1.10 is a reliability freeze, CI hardening, and correctness burn-down train.

The purpose of v1.10 is to make the current v1.9.x codebase safe, audited, classified, regression-protected, and suitable as the base for v1.11 engine-boundary work.

v1.10 is not a feature-expansion release.

## 2. Primary Objective

The v1.10 train exists to convert Coldkeep from functionally complete into trust-complete.

The train prioritizes:

- data safety
- deterministic restore
- GC safety
- verification correctness
- packed-storage consistency
- restore and recovery hardening
- validation correctness
- CLI contract stability
- JSON output determinism
- CI reliability
- audit traceability

## 3. Frozen Scope

During v1.10, the following are frozen unless directly required to fix correctness, validation, recovery, security, or CI integrity:

- user-facing feature expansion
- storage format expansion
- engine extraction
- broad architectural restructuring
- major public API changes
- cosmetic refactors
- style-only cleanup as release-blocking work

## 4. Change Classification During v1.10

| Change type | Allowed in v1.10.0? | Allowed later in v1.10.x? | Notes |
|---|---:|---:|---|
| Documentation for freeze/baseline | Yes | Yes | Main purpose of v1.10.0 |
| Issue tracker/remediation matrix | Yes | Yes | Phase 1+ |
| Regression tests | Yes, if baseline-safe | Yes | Prefer with fixes |
| CLI correctness fixes | No, unless blocking baseline | Yes, v1.10.1 | Main remediation starts later |
| Validation/security fixes | No, unless urgent | Yes, v1.10.2 | Includes path/security hardening |
| Packed storage fixes | No | Yes, v1.10.3 | Later phase |
| GC correctness fixes | No | Yes, v1.10.4 | Later phase |
| Restore/recovery fixes | No | Yes, v1.10.5 | Later phase |
| CI passive documentation | Yes | Yes | Active enforcement later |
| Codacy passive setup | Maybe | Yes, v1.10.6 | Do not block style |
| Critical-path coverage gates | No | Yes, v1.10.7 | Later phase |
| Filesystem abstraction | No | Yes, v1.10.8 | Later phase |
| Fault injection | No | Yes, v1.10.9 | Later phase |
| Engine extraction | No | No | Deferred to v1.11 |
| New user-facing features | No | No | Out of train scope |
| Broad refactor | No | No, unless required by fix | Avoid churn |
| Style-only cleanup | No | Only opportunistic | Must not block release |

Practical decision rule:

When in doubt, the change is out of scope unless it prevents data loss, corruption, unsafe restore, false verification, unsafe deletion, validation bypass, or CI false success.

## 5. Explicitly Allowed Work

The following work is allowed in v1.10:

- bug fixes
- correctness fixes
- validation fixes
- path normalization hardening
- restore/recovery safety fixes
- GC reachability and refcount fixes
- verification coverage fixes
- packed-storage parity fixes
- CLI contract fixes
- JSON output contract fixes
- dependency and toolchain security fixes
- regression tests
- adversarial tests
- CI baseline capture
- CI correctness gates
- Codacy passive integration
- documented suppressions
- accepted-risk documentation
- deferred-issue documentation
- release-gate documentation

## 6. Explicitly Forbidden Work

The following work must not be performed in v1.10.0:

- engine package extraction
- CLI/business-logic separation for architectural reasons alone
- new repository format work unless required for data safety
- new product commands
- new storage backends
- new cloud features
- new UI features
- new encryption/compression features
- broad rewrites
- style-only Codacy burn-down
- CodeRabbit or aggressive AI review gate introduction

## 7. Exception Rule

A frozen change may be allowed only if it satisfies all of the following:

1. It fixes or prevents a correctness, data-safety, validation, recovery, security, or CI-integrity issue.
2. It is linked to a tracked v1.10 issue.
3. It includes regression coverage or a documented reason why regression coverage is not practical.
4. It does not start engine extraction.
5. It does not introduce unrelated feature behavior.

## 8. Release Discipline

Every v1.10.x release must:

- select a narrow domain
- include only related fixes
- document included and excluded issues
- keep CI green
- avoid large stabilization branches
- record accepted and deferred risks
- avoid Codacy style-only blocking
- prioritize correctness over maintainability scoring

## 9. Exit Condition For The v1.10 Train

The v1.10 train is complete when:

- all known S0/S1 issues are fixed or explicitly accepted/deferred with rationale
- no scanner finding remains unknown
- packed and legacy storage semantics are aligned across verify, stats, inspect, restore, GC, and simulation
- JSON-mode output is deterministic and automation-safe
- CI gates cover critical correctness paths
- engine extraction can begin in v1.11 without preserving unstable behavior behind a new API
