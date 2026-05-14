# Coldkeep v1.10 Release Train

Status: Reliability freeze / stabilization train.

Coldkeep v1.10 exists to harden the v1.9.x feature-complete codebase before engine-boundary work begins in v1.11.

## Purpose

The v1.10 train focuses on:

- correctness burn-down
- CI hardening
- Codacy and audit baseline classification
- restore/recovery safety
- GC correctness
- packed-storage consistency
- validation and CLI contract stabilization
- release-gate discipline

## Non-goals

The v1.10 train does not include:

- new product features
- engine extraction
- storage-engine rewrite
- broad aesthetic refactors
- style-only Codacy cleanup as a release blocker

## Release Train Boundaries

| Release | Scope |
|---|---|
| v1.10.0 | Baseline & freeze declaration |
| v1.10.1 | CLI correctness & contract stabilization |
| v1.10.2 | Validation & security hardening |
| v1.10.3 | Packed storage & metadata integrity |
| v1.10.4 | GC correctness & reachability |
| v1.10.5 | Restore & recovery safety |
| v1.10.6 | CI evolution phase 1 & Codacy passive integration |
| v1.10.7 | Critical-path coverage gates |
| v1.10.8 | Filesystem abstraction groundwork |
| v1.10.9 | Filesystem fault injection phase 1 |
| v1.10.10 | Cross-platform validation |
| v1.10.11 | Stabilization & regression burn-down |
| v1.10.12 | Engine-boundary preparation without behavior change |

v1.10.0 is not expected to fix the imported issue backlog. Its job is to freeze scope and establish the baseline machinery used by the later releases.

## Authority

This directory is the release-control source for the v1.10 stabilization train.
