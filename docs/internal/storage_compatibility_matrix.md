# Storage Compatibility Matrix

This document defines the Phase 6 compatibility matrix for the compression-aware
packed-block storage model.

It separates three concerns that must not be conflated:

- correctness and restore guarantees
- supported production configurations
- benchmark coverage

Correctness coverage is broader than benchmark coverage. A mode may be fully
supported and legacy-readable without being benchmarked in every pairing.

## Dimensions

Coldkeep storage currently has three independent dimensions:

| Dimension | Modes |
| --- | --- |
| Encryption | `none`, `aes-gcm` |
| Compression | `none`, `zstd` |
| Packing | `legacy-single`, `packed-multi` |

These dimensions combine into a storage matrix. The matrix is a compatibility
model, not a benchmark obligation.

## Classification Rules

### Fully Supported

These are the recommended production write modes for the v1.9 compression
feature set:

| Packing | Encryption | Compression | Status |
| --- | --- | --- | --- |
| `packed-multi` | `aes-gcm` | `none` | Fully supported |
| `packed-multi` | `aes-gcm` | `zstd` | Fully supported, recommended when compression is desired |

Notes:

- These are the primary packed-block production modes.
- Compression is a real write policy, but it remains block-local and
  metadata-driven.
- Existing data is never rewritten automatically when the repository default
  changes.

### Supported Compatibility Modes

These modes are safe and readable, but they are not the primary production
recommendation for new deployments:

| Packing | Encryption | Compression | Status |
| --- | --- | --- | --- |
| `legacy-single` | `none` | `none` | Supported, legacy-readable |
| `packed-multi` | `none` | `none` | Supported |
| `packed-multi` | `none` | `zstd` | Supported |

Notes:

- These modes must continue to restore correctly.
- Legacy repositories remain restorable even after compression is introduced.
- Mixed repositories containing both legacy and packed rows are expected
  steady-state.
- Mixed repositories are first-class supported mode; homogeneous-state-only
  assumptions are invalid.

### Legacy Readability Guarantee

All historical readable formats remain restorable.

That means:

- old repositories created before compression was introduced remain readable
- mixed repositories remain safe
- repository defaults only affect future writes
- no automatic recompression or migration is performed in the background

Explicitly not guaranteed:

- automatic rewrite of historical repositories
- eager migration of historical block layouts
- automatic recompression of historical blocks

Migration semantics are additive and readability-first.

## Benchmark Matrix

Benchmarking is intentionally narrower than correctness coverage.

The benchmark matrix should focus on representative modes that reveal the major
performance characteristics of the storage model:

| Packing | Encryption | Compression | Benchmark role |
| --- | --- | --- | --- |
| `packed-multi` | `aes-gcm` | `none` | Baseline packed production path |
| `packed-multi` | `aes-gcm` | `zstd` | Primary compression production path |
| `packed-multi` | `none` | `none` | Compression-free packed baseline |
| `packed-multi` | `none` | `zstd` | Compression-only packed path |
| `legacy-single` | `none` | `none` | Restore/verify legacy baseline only |

Benchmark guidance:

- benchmark representative modes, not every possible cross-product equally
- compare compression impact against the relevant baseline for the same packing
  and encryption family
- do not treat benchmark omission as a correctness gap
- official v1.9 baseline pair for packed production is:
  - Baseline A: `packed-multi + aes-gcm + none`
  - Baseline B: `packed-multi + aes-gcm + zstd`
  - both must run against the same dataset preset and execution profile for
    valid comparison

## Operator Expectations

- New writes follow the current repository policy.
- Existing blocks keep their stored metadata and are never rewritten
  automatically.
- Reads and verify use per-block metadata, not the current repository default.
- Read-path negotiation resolves transforms from per-block metadata and runtime
  capability registries only.
- Restore must remain deterministic across supported and legacy-readable modes.

## Summary

Compatibility matrix summary:

- supported: packed `aes-gcm` with `none` and `zstd`
- supported compatibility modes: legacy single and packed `none`/`zstd`
- legacy-readable: all historical readable formats remain restorable
- benchmarked: representative modes only, not exhaustive cross-product testing
