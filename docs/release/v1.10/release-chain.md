# Coldkeep v1.10 Release Chain

Target series: v1.10.x  
Purpose: Stabilization, correctness hardening, CI evolution, and engine-readiness preparation before v1.11.

## Strategy

The v1.10 series is a reliability freeze.

The goal is not to add features. The goal is to remove ambiguity and known risk from the v1.9.x feature-complete codebase.

Every known issue must become one of:

- fixed
- accepted with rationale
- deferred with rationale
- suppressed with rationale
- duplicate of another tracked issue
- converted into a regression test or CI invariant

## Global Rules

- No engine extraction during v1.10.
- No new product features.
- No broad aesthetic refactors.
- No style-only scanner work as release blocker.
- S0/S1 issues must not remain unknown.
- CI must remain green throughout the train.
- Correctness overrides maintainability scoring.

## Release Train

### v1.10.0 — Baseline & Freeze Declaration

Scope:

- declare feature freeze
- commit Codacy baseline
- commit external audit inventory
- create remediation matrix
- define severity/status/domain labels
- record current CI state and release gates

### v1.10.1 — CLI Correctness & Contract Stabilization

Scope:

- reject ignored trailing positional arguments
- normalize boolean flag behavior
- reject empty values for value flags
- unify `--json` and `--output json`
- fix mixed JSON/human output paths
- reject unsafe duplicate singleton flags

### v1.10.2 — Validation & Security Hardening

Scope:

- centralize path normalization
- reject traversal and Windows-drive snapshot paths
- validate container filenames before filesystem joins
- harden restore destination handling
- fix unsafe temp/rename/overwrite patterns
- upgrade Go/toolchain dependencies where needed

### v1.10.3 — Packed Storage & Metadata Integrity

Scope:

- align packed and legacy stats
- align inspect/graph APIs with packed storage
- ensure verify covers packed refs and storage blocks
- validate packed metadata invariants
- add hybrid repository regression tests

### v1.10.4 — GC Correctness & Reachability

Scope:

- fix packed block reachability
- align GC roots with completed/current logical files
- prevent repair from legitimizing bad mappings
- validate deletion plans against snapshots and pins
- add dry-run/live-plan equivalence tests

### v1.10.5 — Restore & Recovery Safety

Scope:

- unify normal restore and snapshot restore sanitization
- harden stored-path restore modes
- fix TOCTOU overwrite and symlink risks
- validate recovery/quarantine filename handling
- add interrupted restore/recovery regression tests

### v1.10.6 — CI Evolution Phase 1 & Codacy Passive Integration

Scope:

- enable Codacy passive repository analysis
- define scanner suppression policy
- classify false positives
- add actionable security/dependency visibility without style blocking

### v1.10.7 — Critical-Path Coverage Gates

Scope:

- define critical packages
- measure invariant-path coverage
- add soft thresholds first
- promote selected thresholds to hard gates
- avoid global coverage blocking

### v1.10.8 — Filesystem Abstraction Groundwork

Scope:

- introduce filesystem abstraction only where needed
- preserve behavior
- prepare injectable operation hooks
- add equivalence tests

### v1.10.9 — Filesystem Fault Injection Phase 1

Scope:

- simulate ENOSPC
- simulate fsync failure
- simulate partial writes
- simulate interrupted rename
- verify no silent corruption and deterministic recovery

### v1.10.10 — Cross-Platform Validation

Scope:

- validate Ubuntu, macOS, Windows where feasible
- focus on path normalization, symlinks, permissions, timestamps, restore determinism
- document platform-specific limitations

### v1.10.11 — Stabilization & Regression Burn-down

Scope:

- resolve remaining S0/S1 issues
- audit suppressions and accepted risks
- run long-run/adversarial/regression suites
- eliminate flaky tests

### v1.10.12 — Engine Boundary Preparation

Scope:

- identify engine-facing contracts
- locate CLI/business-logic coupling
- define no-behavior-change migration rules
- prepare v1.11 transition checklist

No engine extraction happens in v1.10.12.
