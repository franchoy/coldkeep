# v1.10 Labels

Status: Complete  
Owner phase: Phase 3 - Severity Model Adoption

## Purpose

This document defines the canonical labels used during the Coldkeep v1.10 stabilization train.

These labels apply to:

- Codacy findings
- external audit findings
- manual review findings
- CI gaps
- release-gate failures
- accepted risks
- deferred issues
- suppressed findings
- remediation matrix rows

Coldkeep severity is based on project risk, not raw scanner severity.

Scanner severity is preserved as source metadata, but the release decision uses the Coldkeep S0-S4 model.

## Severity Assignment Principle

Severity is assigned by the worst credible impact if the issue is real and reachable.

The main questions are:

1. Can this lose user data?
2. Can this corrupt stored data?
3. Can this make verification falsely pass?
4. Can this make GC delete retained data?
5. Can this restore outside the intended destination?
6. Can this break deterministic restore?
7. Can this make automation believe a failed operation succeeded?
8. Can this hide a CI/release failure?
9. Can this create an unsafe migration or recovery state?
10. Is it only style, readability, documentation, or scanner noise?

## Label Families

- severity labels
- status labels
- domain labels
- risk labels
- release-target labels
- decision labels
- code-area labels
- CI/release labels

## Severity Labels

### `severity/S0-catastrophic`

Release-blocking.

An S0 issue is a catastrophic correctness, data-safety, or security issue that can invalidate Coldkeep's core promise.

S0 includes any credible risk of:

- silent data corruption
- reachable data deletion
- GC deleting retained data
- snapshot retention violation
- restore writing outside the intended root
- false verification success on corrupted/missing data
- unrecoverable restore failure for valid retained data
- recovery flow destroying or hiding valid data
- migration causing data loss
- repository state becoming unrecoverable

#### S0 release rule

An S0 issue must not remain open for a release.

It must be one of:

- fixed
- proven impossible with evidence
- explicitly accepted only if unreachable and documented
- deferred only if the affected release is not shipping the risky behavior

S0 deferral should be rare.

### `severity/S1-critical`

Critical. Must be fixed or explicitly resolved before engine extraction and normally before the next stabilization release in the same domain.

S1 includes:

- crash-consistency failures
- incorrect refcount repair
- stale liveness causing unsafe deletion or unsafe retention
- unsafe container filename trust
- serious JSON automation breakage for correctness-critical commands
- migration parity failures
- restore/recovery partial-failure hazards
- CI/release gate false success hiding correctness failure
- dependency/toolchain vulnerability without upgrade or mitigation plan

#### S1 release rule

An S1 issue may remain open only if:

- it is assigned to a specific v1.10.x release
- the risk during deferral is documented
- the release notes or known-issues file records it if user-visible
- it does not invalidate the current release's scope

### `severity/S2-major`

Major. Should be fixed during v1.10 but may be batched.

S2 includes:

- inaccurate stats
- misleading verify summaries
- inconsistent CLI validation that can cause wrong operation but not data loss
- benchmark comparison false success
- CI tooling defects that reduce confidence
- packed/legacy observability mismatch without known data-loss path
- documentation mismatch that can cause unsafe usage

#### S2 release rule

An S2 issue can be deferred with rationale and target release.

### `severity/S3-minor`

Minor. Fix opportunistically or when touching related code.

S3 includes:

- misleading error text
- small UX inconsistency
- low-risk duplicate flags
- test-only scanner findings
- non-critical script validation gaps
- minor docs mismatch
- low-risk complexity in tests

#### S3 release rule

S3 issues should not block v1.10.0 unless they affect release documentation or audit traceability.

### `severity/S4-optional`

Optional cleanup.

S4 includes:

- style-only issues
- generic maintainability warnings
- non-risky complexity warnings
- naming preferences
- future refactor ideas
- markdownlint formatting noise
- low-value scanner noise

#### S4 release rule

S4 issues do not block release.

They may be fixed only when they are close to touched code or improve release clarity without causing churn.

## Severity Examples

| Finding | Likely severity | Reason |
|---|---:|---|
| GC can delete a packed block retained by a snapshot | S0 | Reachable retained data deletion |
| Verify can pass while packed block bytes are corrupted | S0 | False verification success |
| Restore stored path allows `../` traversal | S0 | Write outside destination root |
| Recovery quarantine trusts unsafe filenames | S1 | Recovery/filesystem safety risk |
| `--json` emits mixed human and JSON output for restore/verify/GC | S1/S2 | Can break automation; severity depends command criticality |
| Empty `remove --path ""` broadens deletion scope | S1/S2 | Could remove wrong logical files depending behavior |
| Benchmark compare ignores missing baseline cases | S2 | CI/performance false success |
| Test uses `0o755` temporary directory | S3/S4 or suppressed | Usually test-only scanner noise |
| Markdown list missing blank line | S4 | Style only |
| Large test function exceeds Lizard limit | S4 | Test readability, not product risk |

## Status Labels

### `status/open`

Imported or recorded, but not yet classified.

Allowed during import phases only.

A release should not ship with important findings left as `status/open`.

### `status/triaged`

Classified with severity, domain, target release, and root invariant.

No fix decision has been completed yet.

### `status/fixed`

Resolved by code, tests, documentation, CI, or release-process change.

A fixed issue must include closure proof.

Closure proof may be:

- commit SHA
- regression test name
- CI job name
- documentation path
- validation command
- manual verification note

### `status/accepted`

Real issue or risk intentionally accepted.

Requires:

- severity
- rationale
- impact
- mitigation
- owner/date
- review condition
- reopen condition

Accepted does not mean ignored.

### `status/deferred`

Real issue intentionally moved to a later release.

Requires:

- original target release
- new target release
- rationale
- risk during deferral
- required follow-up

### `status/suppressed`

Scanner finding intentionally suppressed.

Allowed for:

- false positive
- test-only noise
- documented intentional pattern
- style-only non-blocking finding
- unreachable pattern

Requires:

- tool/rule
- file/scope
- reason
- safety explanation
- review condition

### `status/duplicate`

Finding is a duplicate of another tracked issue or matrix row.

Requires:

- duplicate target ID
- reason for duplicate classification

### `status/needs-regression`

Issue requires a regression test before closure.

This can be combined with another status in the notes or represented as a boolean field in CSV.

### `status/needs-ci-gate`

Issue requires CI enforcement before closure.

This can be combined with another status in the notes or represented as a boolean field in CSV.

### `status/blocked`

Cannot proceed without a decision, dependency, reproduction, or design clarification.

Blocked items require:

- blocker description
- owner
- next decision needed

## Status Transition Model

Typical flow:

```text
open
	-> triaged
			-> fixed
			-> accepted
			-> deferred
			-> suppressed
			-> duplicate
			-> blocked
```

Import phases may create open rows.
Triage phases should move rows out of open.

No S0/S1 issue should remain open after v1.10.0.

## Domain Labels

### Core domains

| Label | Meaning |
|---|---|
| `domain/cli` | CLI argument parsing, arity, flags, help, command contract |
| `domain/json` | JSON output, output mode inference, mixed streams, machine contract |
| `domain/validation` | Input validation, empty values, numeric ranges, duplicate flags |
| `domain/security` | Security hardening, dependency vulnerabilities, unsafe patterns |
| `domain/filesystem` | Path joins, permissions, symlink behavior, fsync, rename, traversal |
| `domain/storage` | Core logical/chunk/container storage behavior |
| `domain/packed-storage` | Packed blocks, storage blocks, chunk/block metadata, hybrid repos |
| `domain/gc` | GC roots, reachability, deletion planning, dry-run/live equivalence |
| `domain/refcount` | Refcount repair, liveness, mapping legitimacy |
| `domain/restore` | Normal restore, stored-path restore, snapshot restore output |
| `domain/recovery` | Quarantine, orphan recovery, interrupted operations |
| `domain/verify` | Integrity verification, hash checks, false pass/fail behavior |
| `domain/snapshot` | Snapshot create/list/delete/restore, retention, lineage |
| `domain/migration` | Schema/data migration, backward compatibility, parity |
| `domain/benchmark` | Benchmark run/compare/report scripts and validation |
| `domain/ci` | GitHub Actions, local CI simulation, release gates |
| `domain/codacy` | Codacy import, suppression policy, scanner classification |
| `domain/docs` | README, changelog, roadmap, release documentation |
| `domain/dependencies` | Go/toolchain/modules/CVEs |
| `domain/concurrency` | Race safety, locks, concurrent store/GC/restore behavior |
| `domain/observability` | Stats, inspect, graph visibility, simulation reporting |
| `domain/tooling` | Scripts and helper tools outside core runtime |

## Multi-Domain Rule

If an issue spans multiple domains, assign the primary domain by the release that should fix it.

Examples:

- `restore --stored-path ""` falls through into ID mode:
	- primary: `domain/cli` if parser contract fix
	- secondary note: `domain/restore`

- Packed block missing from verify:
	- primary: `domain/packed-storage`
	- secondary note: `domain/verify`

- GC simulation misses snapshot-retained packed block:
	- primary: `domain/gc`
	- secondary note: `domain/packed-storage`

The CSV should contain one primary `domain`. Secondary domains may be recorded in `notes`.

## Risk Labels

### Data and correctness risks

| Label | Meaning |
|---|---|
| `risk/data-loss` | Can delete, lose, or make retained data unreachable |
| `risk/data-corruption` | Can corrupt stored or restored bytes |
| `risk/false-verify-success` | Can report integrity success incorrectly |
| `risk/false-verify-failure` | Can report corruption incorrectly |
| `risk/gc-safety` | Can make GC delete live data or retain wrong data |
| `risk/snapshot-retention` | Can violate snapshot immutability or retention |
| `risk/determinism` | Can break deterministic restore/output/order |
| `risk/recovery` | Can break recovery or quarantine safety |
| `risk/migration` | Can break upgrade/downgrade/schema/data parity |

### Security and filesystem risks

| Label | Meaning |
|---|---|
| `risk/path-traversal` | Can escape intended filesystem root |
| `risk/symlink` | Unsafe symlink handling |
| `risk/toctou` | Time-of-check/time-of-use race |
| `risk/permissions` | Unsafe file or directory permissions |
| `risk/container-filename-trust` | Unsafe trust of DB/container filenames before filesystem joins |
| `risk/dependency-cve` | Dependency or toolchain vulnerability |
| `risk/command-execution` | Unsafe command execution or argument handling |
| `risk/sql-injection` | Unsafe SQL construction risk or scanner finding |

### Automation and CI risks

| Label | Meaning |
|---|---|
| `risk/json-contract` | JSON output is invalid, mixed, duplicated, or misleading |
| `risk/ci-false-success` | CI/release tooling can pass despite failure |
| `risk/benchmark-false-success` | Benchmark comparison can pass invalid/missing data |
| `risk/scanner-noise` | Static-analysis finding likely non-actionable |
| `risk/release-process` | Release checklist/gate can miss important state |
| `risk/docs-misleading` | Documentation can lead user/operator to unsafe behavior |

## Risk Field Mapping

The CSV uses these separate boolean/rating fields:

- `breaking_risk`
- `data_loss_risk`
- `security_risk`
- `determinism_risk`
- `recovery_risk`

Allowed values:

```text
none
low
medium
high
```

The label names above are more specific and may be recorded in notes or `root_invariant`.

## Release Target Labels

| Label | Meaning |
|---|---|
| `target/v1.10.0` | Baseline/freeze, inventory, classification, release gates |
| `target/v1.10.1` | CLI correctness and contract stabilization |
| `target/v1.10.2` | Validation and security hardening |
| `target/v1.10.3` | Packed storage and metadata integrity |
| `target/v1.10.4` | GC correctness and reachability |
| `target/v1.10.5` | Restore and recovery safety |
| `target/v1.10.6` | CI evolution phase 1 and Codacy passive integration |
| `target/v1.10.7` | Critical-path coverage gates |
| `target/v1.10.8` | Filesystem abstraction groundwork |
| `target/v1.10.9` | Filesystem fault injection phase 1 |
| `target/v1.10.10` | Cross-platform validation |
| `target/v1.10.11` | Stabilization and regression burn-down |
| `target/v1.10.12` | Engine-boundary preparation without extraction |
| `target/v1.11+` | Deferred to engine-boundary or later architectural work |
| `target/backlog` | Tracked but not assigned |
| `target/none` | Suppressed or duplicate, no remediation target |

## Target Assignment Rule

Every non-suppressed, non-duplicate issue must have a target release.

For v1.10.0:

- target assignment is required
- implementation is not required
- unresolved target decisions must use `status/blocked`

## Decision Labels

| Decision | Meaning |
|---|---|
| `decision/fix` | The issue should be fixed in code, tests, docs, or CI |
| `decision/suppress` | Scanner finding is intentionally suppressed |
| `decision/accept` | Real risk is accepted with rationale |
| `decision/defer` | Real issue is moved to a later target |
| `decision/duplicate` | Issue is covered by another issue/matrix row |
| `decision/investigate` | More investigation needed before final decision |
| `decision/not-applicable` | Finding does not apply to Coldkeep's context |

## Decision Rationale Rule

The following decisions require written rationale:

- `decision/suppress`
- `decision/accept`
- `decision/defer`
- `decision/not-applicable`

The rationale must be specific. Generic statements like "false positive" or "not important" are not sufficient.

## Code-Area Labels

| Label | Meaning |
|---|---|
| `area/production` | Runtime code shipped as part of Coldkeep behavior |
| `area/test` | Unit/integration/adversarial/benchmark tests |
| `area/script` | Shell/Python/helper scripts |
| `area/ci` | GitHub Actions or CI enforcement logic |
| `area/docs` | Documentation only |
| `area/docker` | Dockerfile/container packaging |
| `area/benchmark` | Benchmark infrastructure or benchmark datasets |
| `area/generated` | Generated file or machine-produced artifact |

## Code-Area Severity Rule

Production findings are not automatically severe, but they receive stricter review.

Test-only findings are not automatically safe, but they may be suppressed or downgraded if:

- they cannot affect runtime behavior
- they do not mask CI failure
- they do not teach unsafe patterns copied into production
- they do not invalidate adversarial tests

## Critical-Path Labels

| Label | Meaning |
|---|---|
| `critical/chunk-storage` | Chunk write/read/hash/reference behavior |
| `critical/container-storage` | Container/block persistence and lookup |
| `critical/packed-storage` | Packed block metadata and block/chunk references |
| `critical/snapshot-lifecycle` | Snapshot create/delete/list/restore retention behavior |
| `critical/gc` | Reachability, roots, deletion plan, live/dead classification |
| `critical/restore` | Restore pipeline, ordering, overwrite behavior, destination safety |
| `critical/verify` | Integrity checks and corruption detection |
| `critical/recovery` | Quarantine, repair, interrupted operation handling |
| `critical/metadata-graph` | Graph relationships, logical/physical/chunk/block references |
| `critical/transactions` | DB/filesystem consistency, commit boundaries, rollback behavior |
| `critical/cli-automation` | JSON output and CLI contracts for automation-critical commands |
| `critical/ci-release-gates` | CI and release checks that determine shippability |

## Critical-Path Rule

Critical-path issues require stronger closure proof.

At least one of the following should usually be present:

- regression test
- adversarial test
- integration test
- CI gate
- explicit manual verification command
- formal accepted-risk rationale

## CSV Usage

### `issue-tracker.csv`

The following columns must use the labels or values from this document:

| CSV column | Expected value |
|---|---|
| `release_target` | `v1.10.0`, `v1.10.1`, etc. |
| `status` | `open`, `triaged`, `fixed`, `accepted`, `deferred`, `suppressed`, `duplicate`, `blocked` |
| `severity` | `S0`, `S1`, `S2`, `S3`, `S4` |
| `domain` | primary domain without `domain/` prefix |
| `breaking_risk` | `none`, `low`, `medium`, `high` |
| `data_loss_risk` | `none`, `low`, `medium`, `high` |
| `security_risk` | `none`, `low`, `medium`, `high` |
| `determinism_risk` | `none`, `low`, `medium`, `high` |
| `recovery_risk` | `none`, `low`, `medium`, `high` |
| `requires_regression_test` | `true` or `false` |
| `requires_ci_gate` | `true` or `false` |
| `decision` | `fix`, `suppress`, `accept`, `defer`, `duplicate`, `investigate`, `not-applicable` |

### `remediation-matrix.csv`

The matrix groups many issue rows into one root invariant.

The following columns must use this model:

| CSV column | Expected value |
|---|---|
| `release_target` | `v1.10.x` |
| `status` | same status model |
| `severity` | highest severity among grouped issues unless justified |
| `domain` | primary remediation domain |
| `data_loss_risk` | highest credible grouped risk |
| `security_risk` | highest credible grouped risk |
| `determinism_risk` | highest credible grouped risk |
| `recovery_risk` | highest credible grouped risk |

## CSV Prefix Rule

In Markdown documents, labels may be written with prefixes:

```text
severity/S0-catastrophic
domain/gc
risk/data-loss
```

In CSV files, use compact values:

```text
S0
gc
high
```

## S0/S1 Blocking Rules

### v1.10.0 Rule

v1.10.0 is a baseline release. It does not need to fix every known S0/S1 issue.

However, v1.10.0 must not ship with unknown S0/S1 candidates.

Every known S0/S1 candidate must be:

- recorded
- assigned a severity
- assigned a domain
- assigned a target release
- assigned a status
- linked to evidence or rationale

### v1.10.x Remediation Rule

For remediation releases v1.10.1 and later:

- no S0 in that release's domain may remain unresolved
- no S1 in that release's domain may remain untriaged
- deferring S1 requires explicit rationale
- accepting S0/S1 requires explicit rationale and review
- any fixed S0/S1 requires closure proof

### Engine Extraction Rule

No engine extraction may begin in v1.11 while known S0/S1 issues remain untriaged.

If any S0/S1 remains open, it must be explicitly accepted or deferred with rationale before engine-boundary work starts.

## Suppression Rules

A scanner finding may be suppressed when one of the following is true:

- false positive
- test-only pattern with no runtime impact
- style-only finding
- intentionally explicit invariant-heavy code
- scanner cannot understand safe dynamic construction
- finding is covered by another tracked issue
- finding is not relevant to Coldkeep's threat model

A suppression must include:

- tool
- rule ID
- file/scope
- finding ID if available
- reason
- why it is safe
- review condition
- related issue or matrix row if applicable

## Suppression Anti-Pattern

Do not suppress with only:

```text
false positive
not relevant
won't fix
scanner noise
```

These are insufficient without rationale.

Codacy remains constrained and observability-focused; style-only findings may be non-blocking, but suppression still requires explicit, reviewable rationale.

## Accepted Risk Rules

An accepted risk is a real issue or limitation that the project intentionally allows to remain.

Accepted risks require:

- issue ID
- severity
- domain
- reason for acceptance
- impact
- mitigation
- owner/date
- review condition
- reopen condition

Accepted risk is not the same as suppression.

Suppression means the finding is not actionable as reported.
Acceptance means the risk is real but intentionally tolerated.

## Deferred Issue Rules

A deferred issue is real and actionable but moved to a later release.

Deferred issues require:

- issue ID
- current target
- new target
- reason for deferral
- risk during deferral
- mitigation
- owner/date
- follow-up condition

S0 deferral requires exceptional justification.
S1 deferral requires explicit target release and risk rationale.
