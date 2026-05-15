# v1.10 CI Evolution

Status: Complete  
Owner phase: Phase 8 and later

## Purpose

This document records the v1.10 CI hardening direction.

Phase 8 captures the plan.

Implementation occurs in later v1.10.x releases.

## Priority 1 — Filesystem Fault Injection

Target releases:

- v1.10.8 — filesystem abstraction groundwork
- v1.10.9 — filesystem fault injection phase 1

Fault classes to model:

- ENOSPC
- EIO
- fsync failure
- partial writes
- delayed flush
- interrupted atomic rename

Validation requirements:

- no silent corruption
- no orphaned references
- no broken restore paths
- deterministic recovery behavior

## Priority 2 — Critical-Path Coverage Gates

Target release:

- v1.10.7

Coverage should focus on correctness-critical paths, not global coverage.

Candidate critical areas:

- chunk storage
- snapshot lifecycle
- GC engine
- restore pipeline
- metadata graph logic
- transaction coordination
- verification layer

Initial target policy:

- introduce soft reporting first
- promote selected thresholds to hard gates only after baseline stability
- avoid blocking on non-critical utilities

## Priority 3 — Mutation Testing

Target release:

- v1.10.11 or later unless cheap to pilot earlier

Recommended targets:

- reference counting
- GC eligibility logic
- snapshot retention logic
- integrity verification
- deterministic ordering

Policy:

- do not run on every PR initially
- prefer scheduled or pre-release validation

## Priority 4 — Cross-Platform Validation

Target release:

- v1.10.10

Platforms:

- Ubuntu
- macOS
- Windows

Validation focus:

- path normalization
- permissions handling
- symlink behavior
- newline stability
- timestamp consistency
- deterministic restore

## Priority 5 — Advanced Chaos Scheduling

Target release:

- v1.10.11 or later

Potential chaos dimensions:

- snapshot timing
- GC timing
- restore timing
- lock contention
- transaction ordering

## Codacy Evolution

Target release:

- v1.10.6

Codacy should be introduced as constrained observability:

- maintainability tracking
- duplication visibility
- security surfacing
- dependency vulnerability visibility
- PR annotations
- trend reporting

Codacy should not become:

- architecture authority
- correctness authority
- invariant authority
- style-only release blocker

This matches the CI proposal's recommendation to improve deeper storage-engine validation rather than adding more stylistic tooling.

### Validation Checklist for Step 8.11

- [x] `ci-evolution.md` status is Complete
- [x] Filesystem fault injection priority documented (v1.10.8-v1.10.9, ENOSPC/EIO/fsync/partial-write/rename)
- [x] Critical-path coverage priority documented (v1.10.7, soft reporting first)
- [x] Mutation testing policy documented (v1.10.11+, scheduled/pre-release)
- [x] Cross-platform validation priority documented (v1.10.10, Ubuntu/macOS/Windows)
- [x] Advanced chaos scheduling documented (v1.10.11+, timing/contention)
- [x] Codacy evolution policy documented (v1.10.6, observability not authority)
- [x] No CI behavior changed (Phase 8 captures plan only; implementation later)
