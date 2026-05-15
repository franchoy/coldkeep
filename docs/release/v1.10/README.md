# Coldkeep v1.10 Release Train

Status: In progress  
Release train: v1.10.x  
Current release package: v1.10.0 — Baseline & Freeze Declaration

## Purpose

Coldkeep v1.10 is a reliability freeze, CI hardening, and correctness burn-down train.

The purpose of this train is to make the v1.9.x functionally complete system safe enough to serve as the foundation for v1.11 engine-boundary work.

v1.10 focuses on:

- correctness burn-down
- Codacy baseline classification
- external audit inventory
- remediation matrix creation
- CLI contract stabilization
- JSON output determinism
- validation hardening
- packed-storage consistency
- GC correctness
- restore and recovery safety
- CI hardening
- release-gate discipline

## Non-goals

v1.10 does not include:

- new product features
- engine extraction
- broad architectural rewrites
- new storage backends
- new cloud features
- new UI features
- style-only Codacy burn-down as a release blocker

## v1.10.0 Scope

v1.10.0 establishes the baseline and freeze declaration for the release train.

v1.10.0 includes:

- release documentation structure
- evidence inventory
- Codacy baseline import
- external audit import
- severity/status/domain label definitions
- issue tracking schema
- remediation matrix
- CI baseline
- release gates
- toolchain vulnerability plan
- Codacy policy baseline
- initial S0/S1 review

v1.10.0 does not implement the actual remediation fixes. Remediation begins in v1.10.1.

## Release Train

| Release | Scope |
|---|---|
| v1.10.0 | Baseline & Freeze Declaration |
| v1.10.1 | CLI Correctness & Contract Stabilization |
| v1.10.2 | Validation & Security Hardening |
| v1.10.3 | Packed Storage & Metadata Integrity |
| v1.10.4 | GC Correctness & Reachability |
| v1.10.5 | Restore & Recovery Safety |
| v1.10.6 | CI Evolution Phase 1 & Codacy Passive Integration |
| v1.10.7 | Critical-Path Coverage Gates |
| v1.10.8 | Filesystem Abstraction Groundwork |
| v1.10.9 | Filesystem Fault Injection Phase 1 |
| v1.10.10 | Cross-Platform Validation |
| v1.10.11 | Stabilization & Regression Burn-down |
| v1.10.12 | Engine Boundary Preparation |

## Directory Map

| File | Purpose |
|---|---|
| `freeze-declaration.md` | Formal v1.10 freeze and scope lock |
| `release-chain.md` | v1.10.x release train overview |
| `phase-status.md` | Phase-by-phase completion tracking |
| `v1.10.0-checklist.md` | Completion checklist for v1.10.0 |
| `issue-triage-schema.md` | Canonical issue tracking schema |
| `labels.md` | Severity/status/domain/risk labels |
| `issue-tracker.csv` | Raw imported issue inventory |
| `remediation-matrix.csv` | Deduplicated root-invariant matrix |
| `accepted-risks.md` | Risks accepted with rationale |
| `deferred-issues.md` | Real issues intentionally deferred |
| `suppressed-findings.md` | Scanner findings suppressed with rationale |
| `known-s0-s1.md` | Initial catastrophic/critical issue inventory |
| `codacy-baseline.md` | Codacy baseline summary and classification |
| `external-audit-inventory.md` | Third-party audit inventory |
| `ci-baseline.md` | Current CI state before v1.10 evolution |
| `ci-evolution.md` | CI hardening plan for v1.10 |
| `release-gates.md` | Required checks before release |
| `toolchain-vulnerability-plan.md` | Go/toolchain/dependency vulnerability plan |
| `codacy-policy.md` | How Codacy is used during v1.10 |
| `evidence/MANIFEST.md` | Frozen source evidence manifest |
| `templates/` | Reusable templates for later phases |

## Completion Rule

A v1.10.x release is tagged only after:

- all internal phases for that v1.10.x are complete
- local pre-release checklist passes
- local CI simulation passes or documented exceptions exist
- PR is merged into main
- release notes are prepared

## Release / Tag Policy

- `v1.10.0-freeze` is the internal freeze marker.
- `v1.10.0` is reserved for the full v1.10.0 baseline release package after the release-train work is complete.
- Later `v1.10.x` tags should only be created when the relevant phase scope is complete and reviewed.
- Phase 1 begins from this directory structure; later phases populate the inventory, classification, and gates.
