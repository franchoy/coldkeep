# v1.10 Codacy Policy

Status: Complete  
Owner phase: Phase 11 — Codacy Policy Baseline

# Phase 11 Completion Statement

The v1.10 Codacy policy baseline has been defined.

Phase 11 completed:

- Codacy allowed-use policy
- Codacy forbidden-use policy
- Codacy severity mapping policy
- Codacy blocking/non-blocking policy
- suppression policy
- production security review policy
- test/docs/style handling policy
- dependency/CVE handling link to Phase 10
- Codacy finding lifecycle
- v1.10.6 passive integration target
- optional policy and suppression-candidate summaries

Phase 11 did not:

- enable Codacy blocking
- change CI workflows
- suppress findings without rationale
- fix Codacy findings
- change production code
- change tests
- change scripts
- change dependencies

## Purpose

This document defines how Codacy is used during the Coldkeep v1.10 stabilization train.

Codacy is useful for surfacing:

- dependency vulnerabilities
- production security candidates
- resource/lifecycle warnings
- complexity hotspots
- duplication hotspots
- style and documentation findings
- test-only scanner noise that needs classification

Codacy is not the release authority.

Coldkeep's release authority remains:

- correctness invariants
- data safety
- deterministic restore
- verification integrity
- GC safety
- restore/recovery safety
- validation strictness
- CI reliability
- documented accepted/deferred/suppressed risk decisions

## Core Policy

Codacy findings must be imported, classified, and tracked.

Codacy findings must not be blindly fixed, blindly suppressed, or blindly used as release blockers.

Every Codacy finding must eventually become one of:

- fixed
- accepted with rationale
- deferred with rationale
- suppressed with rationale
- duplicate of another tracked finding
- not applicable with evidence
- converted into a regression test or CI invariant

# Allowed Codacy Uses

Codacy may be used during v1.10 for the following purposes.

## 1. Security surfacing

Codacy may identify candidate security issues such as:

- unsafe dynamic paths
- SQL construction warnings
- command execution warnings
- unsafe file permissions
- unsafe file access patterns
- weak randomness warnings
- Docker/container hardening warnings

These findings require Coldkeep-specific review before enforcement.

## 2. Dependency vulnerability visibility

Codacy/Trivy dependency findings may identify:

- Go stdlib CVEs
- module vulnerabilities
- toolchain upgrade recommendations

These findings feed:

- `toolchain-vulnerability-plan.md`
- `issue-tracker.csv`
- `remediation-matrix.csv`
- v1.10.2 validation/security hardening

## 3. Complexity hotspot tracking

Codacy/Lizard complexity findings may identify code that is hard to reason about.

Complexity findings are useful when they affect:

- critical restore paths
- GC reachability
- verification logic
- packed-storage metadata handling
- recovery/quarantine safety
- transaction boundaries
- release/benchmark scripts that can mask failures

Complexity findings are not automatically bugs.

## 4. Duplication visibility

Codacy may identify duplicated logic that could hide inconsistent invariant enforcement.

Duplication is most relevant when it appears in:

- validation paths
- restore paths
- GC paths
- verify paths
- snapshot handling
- benchmark/release gates

## 5. PR annotation context

Codacy annotations may help reviewers notice risky patterns.

Annotations should be treated as review prompts, not automatic commands.

## 6. Trend reporting

Codacy may be used to track whether risk classes are improving over time.

Trend reporting must not override correctness-first release decisions.

# Forbidden Codacy Uses

Codacy must not be used as:

- architectural authority
- correctness authority
- invariant authority
- automatic refactoring authority
- style-only release blocker
- raw issue count release blocker
- generic maintainability-score release blocker
- substitute for adversarial tests
- substitute for storage-invariant reasoning
- substitute for manual review of S0/S1 risks

## Forbidden Release Decisions

Do not block a v1.10.x release only because:

- markdownlint count is non-zero
- Lizard complexity count is non-zero
- test-only warnings are non-zero
- generic maintainability score is below a target
- raw Codacy issue count is high
- Codacy suggests abstraction/naming/style changes

## Forbidden Fix Pattern

Do not perform broad refactors only to satisfy Codacy during v1.10.

A Codacy-driven change is allowed only when it:

- fixes a tracked correctness/security/CI/release risk,
- maps to a matrix row,
- includes validation or rationale,
- does not start engine extraction,
- does not introduce unrelated product behavior.

# Severity Mapping Policy

Codacy severity is preserved as source metadata.

Coldkeep severity is assigned independently using the S0-S4 model in `labels.md`.

## Rule

A Codacy `Error`, `Warning`, or `Info` does not automatically map to S0, S1, S2, S3, or S4.

Coldkeep severity depends on:

- reachability
- production vs test/docs area
- data-loss risk
- corruption risk
- false verification risk
- GC safety risk
- restore/recovery risk
- security risk
- CI false-success risk
- whether the finding affects a critical path

## Examples

| Codacy finding | Coldkeep interpretation |
|---|---|
| Trivy Go stdlib CVE in `go.mod` | Usually S1/S2 until upgraded, mitigated, or proven not applicable |
| Production dynamic path warning in restore/GC/recovery | Review as possible S1/S2; may escalate if traversal/data-loss reachable |
| SQL warning in test fixture | Likely S3/S4 or suppress with rationale |
| `math/rand` in deterministic benchmark/test | Likely S3/S4 or suppress with rationale |
| Markdown formatting warning | S4, non-blocking |
| High test complexity | S3/S4 unless it weakens invariant coverage |
| High production complexity in GC/restore/verify | S2/S3, possibly S1 if it hides known correctness ambiguity |

# Codacy Blocking Policy

## v1.10.0 rule

Codacy is not a blocking gate for v1.10.0.

For v1.10.0, Codacy gates are:

- evidence frozen
- findings imported
- findings classified
- findings matrix-linked
- dependency candidates identified
- production security candidates identified
- style/test noise tracked
- no raw-count blocking

## v1.10.6 passive integration rule

v1.10.6 may introduce Codacy passive integration.

Passive means:

- Codacy can annotate PRs
- Codacy can report trends
- Codacy can surface candidate risks
- Codacy can inform review
- Codacy does not fail release on style-only findings
- Codacy does not fail release on raw count

## Future hard-blocking candidates

Only the following Codacy classes may become hard-blocking later, and only after policy and baseline are stable:

- high-confidence production security findings
- dependency CVEs without plan
- critical-path unchecked errors
- critical-path resource leaks
- CI/release script findings that can mask failure
- production filesystem/path findings with unsafe trust boundary
- new S0/S1 candidate findings without triage

## Permanently non-blocking classes

The following must not become release blockers by themselves:

- markdownlint style-only findings
- naming preferences
- line length or formatting preferences
- generic maintainability score
- raw issue count
- test complexity when tests remain understandable and effective
- intentionally explicit invariant-heavy code
- duplicate code where deduplication would obscure safety logic

# Suppression Policy

A Codacy finding may be suppressed only with rationale.

## Valid suppression reasons

A finding may be suppressed when it is:

- false positive
- test-only scanner noise
- docs/style-only non-risk
- deterministic test/benchmark pattern
- intentionally explicit invariant-heavy logic
- intentionally duplicated safety logic
- unreachable in Coldkeep's execution model
- covered by a broader matrix row
- not applicable to Coldkeep's threat model

## Invalid suppression reasons

The following are not sufficient:

- "false positive"
- "scanner noise"
- "not important"
- "won't fix"
- "style"
- "test only"

These may be true, but they need explanation.

## Required suppression record

Every suppression must include:

- suppression ID
- CK issue ID
- matrix ID if applicable
- Codacy source ID
- tool
- rule ID
- file/scope
- source area: production/test/docs/script/CI/docker
- reason
- why this is safe
- review condition
- reopen condition
- owner/date

## Suppression review rule

Suppressed findings must be revisited when:

- the affected file moves from test to production,
- the affected code becomes critical path,
- the scanner rule changes,
- the threat model changes,
- the finding appears in a new production context,
- a related S0/S1 issue is discovered.

# Production Security Review Policy

Production security findings require explicit review.

## Production security candidate classes

Examples include:

- dynamic filesystem path findings
- SQL construction findings
- command execution findings
- unsafe file permission findings
- weak randomness findings in production code
- Docker root-user findings
- dependency CVEs
- resource/lifecycle findings in critical paths

## Review questions

For each production security candidate, answer:

1. Is the code reachable in normal Coldkeep operation?
2. Is the input user-controlled, database-controlled, or internal-only?
3. Is the path rooted and normalized?
4. Is the SQL value parameterized or is the dynamic part constrained?
5. Is the command executable trusted and are arguments controlled?
6. Does the finding affect restore, GC, verify, recovery, packed storage, or migration?
7. Could it cause data loss, traversal, false verification, or CI false success?
8. Is there already a matrix row for the root invariant?
9. Is a regression/adversarial test required?
10. Should the finding block a later release?

## Required outcome

A production security candidate must become one of:

- fixed,
- assigned to a remediation release,
- accepted with rationale,
- deferred with rationale,
- suppressed as false positive with evidence,
- not applicable with evidence.

# Test, Docs, and Style Handling Policy

## Test-only findings

Test-only findings may be downgraded, suppressed, or moved to backlog when:

- they cannot affect runtime behavior,
- they do not mask CI failure,
- they do not weaken adversarial tests,
- they do not teach unsafe patterns likely to be copied into production,
- they do not reduce confidence in correctness-critical validation.

Test-only findings may still matter if they:

- make tests misleading,
- hide failures,
- generate malformed artifacts,
- allow benchmark false success,
- weaken adversarial coverage,
- affect release scripts or CI gates.

## Documentation style findings

Documentation style findings are normally S4.

They should not block release unless they make release instructions unsafe or misleading.

## Complexity findings in tests

Test complexity is not automatically bad.

A large adversarial test may be acceptable if it preserves scenario readability and invariant coverage.

Test complexity should be fixed only when it improves clarity without weakening coverage.

## Production complexity findings

Production complexity should be reviewed by risk.

Complexity in critical paths is more important than complexity in simple utilities.

Refactoring is allowed only when it reduces correctness risk and has regression coverage.

# Dependency and CVE Handling

Dependency/toolchain findings are handled through:

docs/release/v1.10/toolchain-vulnerability-plan.md

## Rule

A dependency/CVE Codacy finding must not be suppressed merely because v1.10.0 is a baseline release.

It must be one of:

- fixed by upgrade,
- assigned to v1.10.2,
- proven not applicable,
- accepted with rationale,
- deferred with rationale.

## v1.10.0 decision

For v1.10.0, dependency/toolchain findings are tracked and assigned according to Phase 10.

The expected default target is:

v1.10.2 - Validation & Security Hardening

## Closure rule

A dependency/CVE finding may close only when:

- Go/toolchain/module version is upgraded and validation is green,
- finding is proven not applicable with evidence,
- risk is accepted with full accepted-risk record,
- target is deferred with full deferred-issue record.

# Codacy Finding Lifecycle

Codacy findings move through the same lifecycle as all v1.10 issues.

## Typical lifecycle

imported/open
	-> triaged
			-> fixed
			-> suppressed
			-> accepted
			-> deferred
			-> duplicate
			-> not-applicable

## v1.10.0 expected state

By v1.10.0 release:

- Codacy evidence is frozen
- Codacy rows are imported
- Codacy rows are matrix-linked
- Codacy rows have first-pass severity/domain/target
- Codacy policy is defined
- Codacy blocking is not enabled

Codacy findings do not need to be fixed in v1.10.0.

## v1.10.6 expected state

By v1.10.6:

- Codacy passive integration may be enabled
- suppression candidates should be reviewed
- production security candidates should be routed
- dependency handling should already be planned or in progress
- style-only and test-noise classes should be documented
- hard-blocking rules should still be conservative
