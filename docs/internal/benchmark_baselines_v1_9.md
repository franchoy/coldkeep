# Benchmark Baselines v1.9 (Internal)

Status: Frozen historical single-observation advisory evidence
Date: 2026-05-09
Scope: Historical/informational evidence; no Phase 11 performance authority

## Purpose

This document freezes the historical v1.9 benchmark artifacts and records the
policy that originally compared later results against them.

The Phase 11 benchmark-gate investigation found that these files do not contain
distribution or resolved-environment provenance and that the active and
worker-specific manifests contain stale hashes. Their measurement content and
3%/5% thresholds remain frozen historical policy, but they must not be treated
as schema-v2 aggregate evidence or reinterpreted as paired-ratio thresholds.
Required CI may read them only as `historical_v1.9_absolute` advisory inputs
after validating their established legacy shape. A threshold crossing is
`BENCHMARK_TIMING_WARNING`, not a hard performance failure or pass.

The artifacts continue to document earlier decisions. They do not select a
Phase 11 production reference or hard performance endpoint. Missing or malformed
baseline evidence remains an advisory-evaluation integrity error and fails the
job; advisory authority does not weaken evidence validation.

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

The historical v1.9 regression-threshold policy is:

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

## Historical Regression Threshold Contract

Thresholds are frozen in `benchmarks/v1.9/regression-thresholds.yaml` for
historical advisory interpretation only. They have no paired-gate or hard
performance authority.

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

These v1.9 artifacts remain immutable historical evidence. Required CI cites
them only under the hosted timing advisory policy and preserves their content,
manifests, hashes, and thresholds byte-for-byte. They cannot be reused as paired
samples. Future hard performance authority requires separately authorized
controlled infrastructure, qualification, reference governance, and numeric
threshold policy.
