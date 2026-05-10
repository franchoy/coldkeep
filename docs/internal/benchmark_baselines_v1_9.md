# Benchmark Baselines v1.9 (Internal)

Status: Frozen
Date: 2026-05-09
Scope: Official benchmark reference point for post-v1.9 work

## Purpose

This document freezes the official v1.9 benchmark baselines and the policy used
to compare future results against them.

The goal is to ensure that v1.10+ architectural work measures regressions
against one stable reference point instead of silently moving the floor.

These baselines are the reference for:

- v1.10 engine extraction
- v1.11 logic migration
- v1.12 contract stabilization

## Official Baseline Artifacts

The official v1.9 baseline set is:

- compression modes: `none`, `zstd`
- worker profiles: `w1`, `w4`
- contract shape: `none/zstd × w1/w4`

Official baseline JSON files:

- `benchmarks/v1.9/baselines/benchmark-baseline-v1.9-packed-aes-gcm-none-small-w1-r1.json`
- `benchmarks/v1.9/baselines/benchmark-baseline-v1.9-packed-aes-gcm-zstd-small-w1-r1.json`
- `benchmarks/v1.9/baselines/benchmark-baseline-v1.9-packed-aes-gcm-none-small-w4-r1.json`
- `benchmarks/v1.9/baselines/benchmark-baseline-v1.9-packed-aes-gcm-zstd-small-w4-r1.json`

Machine-readable manifests:

- `benchmarks/v1.9/baselines/baseline-manifest-v1.9.json` (CI-active profile pointer; currently `w4`)
- `benchmarks/v1.9/baselines/baseline-manifest-v1.9-small-w1-r1.json` (frozen `w1` profile)
- `benchmarks/v1.9/baselines/baseline-manifest-v1.9-small-w4-r1.json` (frozen `w4` profile)

The authoritative regression-threshold policy is:

- `benchmarks/v1.9/regression-thresholds.yaml`

## Corpora Contract

Benchmark corpora are part of the baseline contract, not incidental test data.

Authoritative corpus definitions are documented in:

- `docs/BENCHMARK_CORPORA.md`

Frozen corpus expectations:

- deterministic content generation
- stable corpus versions
- same dataset preset across baseline comparisons
- same logical totals across all official baseline profiles

## Methodology Contract

The official v1.9 baselines were captured under this locked methodology:

- dataset preset: `small`
- repeat count: `1`
- worker count: `1` and `4` (each pair compared within same worker profile)
- deterministic benchmark mode enabled
- same benchmark case set across all baseline profiles

Each baseline manifest must continue to prove:

- same dataset
- same repeat
- same execution profile
- same logical totals
- same case set

## Regression Threshold Contract

Thresholds are frozen in `benchmarks/v1.9/regression-thresholds.yaml`.

Policy summary:

- uncompressed baseline: strict thresholds; intended to fail CI on real regressions
- compressed baseline: warning-oriented thresholds in v1.9/v1.10; intended to
  detect compression regressions without treating normal compression overhead as
  a failure by default

Threshold changes require:

1. explicit rationale
2. baseline policy update
3. regenerated or reaffirmed baseline artifacts
4. release engineering approval

## Versioning Rules

The baseline JSON files, manifest, and threshold policy are versioned artifacts.

Requirements:

- baseline file paths in manifests must remain repository-relative
- baseline JSON content must remain committed to the repository
- threshold policy must remain committed and human-reviewable
- legacy root-level baseline files remain historical context only; they are not
  the official v1.9 reference set

## Freeze Statement

These v1.9 baseline artifacts are the official reference point for future
regression detection until an explicit baseline-refresh decision supersedes
them.

Architecture may change in v1.10+, but benchmark comparison authority remains
anchored to this v1.9 baseline set.