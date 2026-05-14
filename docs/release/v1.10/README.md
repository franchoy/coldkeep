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

## Authority

This directory is the release-control source for the v1.10 stabilization train.
