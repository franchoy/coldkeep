# v1.10 Codacy Baseline

Status: Complete  
Owner phase: Phase 5 - Codacy Baseline Import

## Purpose

This document records the Codacy baseline imported for the Coldkeep v1.10 stabilization train.

Codacy is used during v1.10 for:

- security surfacing
- dependency vulnerability visibility
- complexity hotspot tracking
- duplication visibility
- unchecked/resource-pattern visibility
- PR annotation context
- trend reporting

Codacy is not used as:

- architectural authority
- correctness authority
- invariant authority
- style-only release blocker
- substitute for adversarial tests
- substitute for storage-engine reasoning

Coldkeep severity is assigned separately from Codacy severity.

## Phase 5 Scope

Phase 5 imports and classifies the Codacy baseline.

Phase 5 does not:

- fix Codacy findings
- suppress findings permanently
- refactor complexity hotspots
- change production code
- change tests
- change CI enforcement
- block v1.10.0 on style-only issues

Actual remediation begins in later v1.10.x releases.

## Phase 5 Completion Statement

The Codacy baseline has been imported into `issue-tracker.csv`.

Phase 5 completed:

- raw Codacy JSON parse
- Codacy issue row import
- source metadata preservation
- first-pass bucket classification
- first-pass release target assignment
- first-pass area/domain assignment
- generated summary counts
- dependency/toolchain candidate identification
- production security candidate identification
- likely non-blocking scanner-noise identification

Phase 5 did not:

- fix findings
- suppress findings permanently
- create remediation matrix rows
- change production code
- change CI enforcement
- change release gates

## Classification Buckets

Every Codacy finding is imported into `issue-tracker.csv` and assigned one primary bucket.

## Buckets

| Bucket | Meaning | Typical target |
|---|---|---|
| `dependency-cve` | Toolchain/module vulnerability, usually from Trivy | v1.10.2 or Phase 10 toolchain plan |
| `production-security-candidate` | Security finding in runtime production code | v1.10.2 or related domain |
| `production-correctness-candidate` | Finding may reflect correctness risk, not just style | Domain-specific v1.10.x |
| `test-security-noise` | Security-style finding in tests with no runtime impact | suppress/accept after review |
| `test-complexity-hotspot` | Large/complex test function | S3/S4, normally non-blocking |
| `production-complexity-hotspot` | Large/complex production function | S2/S3 depending critical path |
| `docs-style` | Markdown/style-only finding | S4, non-blocking |
| `docker-hardening` | Docker/container packaging hardening | v1.10.2 or backlog |
| `script-tooling-risk` | Shell/Python/tooling issue that can mask CI or release failure | v1.10.6 or v1.10.11 |
| `scanner-false-positive-candidate` | Likely scanner mismatch requiring rationale | suppressed later with rationale |

## Bucket Assignment Rule

Bucket assignment is not final closure.

A finding may be bucketed as `test-security-noise` or `scanner-false-positive-candidate`, but it is not considered suppressed until a suppression rationale is recorded in `suppressed-findings.md` or the issue row itself contains sufficient rationale.

Phase 5 may use `decision=investigate` for findings that need human review.

## Imported Baseline

## Source

Frozen evidence file:

```text
docs/release/v1.10/evidence/coldkeep_codacy_all_issues.baseline.json
```

Imported into:

```text
docs/release/v1.10/issue-tracker.csv
```

Optional generated summary:

```text
docs/release/v1.10/codacy-summary.csv
```

## Import Status

Status: Complete

The Codacy JSON was imported into `issue-tracker.csv` using the Phase 4 schema.

Each imported row preserves:

- Codacy issue ID
- file path
- line number
- scanner rule
- scanner category
- scanner subcategory where present
- scanner severity
- scanner tool
- source commit where present
- message/title

## Count Summary

| Dimension | Value | Count |
|---|---|---:|
| total | Codacy findings imported | 1443 |
| source_tool | Lizard | 755 |
| source_tool | Opengrep | 504 |
| source_tool | Trivy | 46 |
| source_tool | markdownlint | 118 |
| source_category | Complexity | 755 |
| source_category | Security | 542 |
| source_category | CodeStyle | 110 |
| source_severity | Error | 220 |
| source_severity | Warning | 794 |
| source_severity | Info | 126 |
| area | production | 385 |
| area | test | 860 |
| area | docs | 127 |
| area | script | 28 |
| area | docker | 2 |

Full generated counts are recorded in `codacy-summary.csv`.

## Dependency / Toolchain Candidates

The following imported Codacy rows are dependency/toolchain candidates and must feed Phase 10 - Dependency & Toolchain Vulnerability Plan.

```text
CK-110-0012 | CVE-2025-58185 | go.mod:3
CK-110-0058 | CVE-2026-27139 | go.mod:3
CK-110-0097 | CVE-2025-58189 | go.mod:3
CK-110-0103 | CVE-2025-61724 | go.mod:3
CK-110-0112 | CVE-2025-58186 | go.mod:3
CK-110-0121 | CVE-2025-47906 | go.mod:3
CK-110-0147 | CVE-2026-39836 | go.mod:3
CK-110-0154 | CVE-2025-61725 | go.mod:3
CK-110-0181 | CVE-2025-61726 | go.mod:3
CK-110-0250 | CVE-2026-39823 | go.mod:3
CK-110-0268 | CVE-2025-61727 | go.mod:3
CK-110-0270 | CVE-2026-27142 | go.mod:3
CK-110-0285 | CVE-2026-39820 | go.mod:3
CK-110-0309 | CVE-2025-22871 | go.mod:3
CK-110-0311 | CVE-2024-45341 | go.mod:3
CK-110-0336 | CVE-2025-22873 | go.mod:3
CK-110-0345 | CVE-2026-32288 | go.mod:3
CK-110-0365 | CVE-2024-34155 | go.mod:3
CK-110-0385 | CVE-2025-4673 | go.mod:3
CK-110-0472 | CVE-2025-47912 | go.mod:3
CK-110-0487 | CVE-2026-39826 | go.mod:3
CK-110-0494 | CVE-2025-61723 | go.mod:3
CK-110-0516 | CVE-2025-58183 | go.mod:3
CK-110-0656 | CVE-2026-39825 | go.mod:3
CK-110-0701 | CVE-2026-32289 | go.mod:3
CK-110-0725 | CVE-2025-61729 | go.mod:3
CK-110-0734 | CVE-2025-61728 | go.mod:3
CK-110-0906 | CVE-2024-34158 | go.mod:3
CK-110-0941 | CVE-2024-45336 | go.mod:3
CK-110-1001 | CVE-2025-61730 | go.mod:3
CK-110-1014 | CVE-2024-34156 | go.mod:3
CK-110-1028 | CVE-2026-32282 | go.mod:3
CK-110-1039 | CVE-2026-33814 | go.mod:3
CK-110-1040 | CVE-2026-32281 | go.mod:3
CK-110-1055 | CVE-2026-33811 | go.mod:3
CK-110-1065 | CVE-2025-58187 | go.mod:3
CK-110-1116 | CVE-2025-22866 | go.mod:3
CK-110-1169 | CVE-2025-68121 | go.mod:3
CK-110-1236 | CVE-2025-0913 | go.mod:3
CK-110-1237 | CVE-2025-58188 | go.mod:3
CK-110-1284 | CVE-2026-32283 | go.mod:3
CK-110-1311 | CVE-2025-47907 | go.mod:3
CK-110-1329 | CVE-2026-42499 | go.mod:3
CK-110-1338 | CVE-2026-25679 | go.mod:3
CK-110-1352 | CVE-2026-32280 | go.mod:3
CK-110-1412 | CVE-2025-22870 | go.mod:3
```

These are not fixed in Phase 5.

Phase 10 will decide whether to:

- upgrade Go/toolchain immediately
- defer to v1.10.2
- document a temporary accepted risk
- mark findings as not applicable after verification

## Validation checklist

- [ ] Dependency/toolchain candidate count generated
- [ ] Candidate CK IDs recorded in `codacy-baseline.md`
- [ ] Phase 10 is identified as owner
- [ ] No toolchain upgrade performed in Phase 5
- [ ] No dependency finding is ignored

## Validation checklist

- [ ] Dependency CVE bucket exists
- [ ] Production security candidate bucket exists
- [ ] Production correctness candidate bucket exists
- [ ] Test security noise bucket exists
- [ ] Test complexity hotspot bucket exists
- [ ] Production complexity hotspot bucket exists
- [ ] Docs style bucket exists
- [ ] Docker hardening bucket exists
- [ ] Script/tooling risk bucket exists
- [ ] Scanner false-positive candidate bucket exists
- [ ] Bucket assignment is not treated as final suppression

## First-Pass Interpretation

The Codacy baseline contains mixed signal types.

## High-priority signal

The following classes require careful review:

- dependency/toolchain CVEs
- production security findings
- production filesystem/path findings
- production SQL/dynamic-query findings
- production command-execution findings
- production resource/lifecycle findings
- CI/script findings that could mask release failures

## Medium-priority signal

The following classes should be tracked but not blindly refactored:

- production complexity hotspots
- critical-path duplication
- benchmark/tooling validation risks
- Docker hardening findings

## Low-priority or likely non-blocking signal

The following classes should not block v1.10.0 by themselves:

- markdownlint style findings
- test-only complexity warnings
- test-only file permission warnings
- test-only dynamic file reads
- deterministic benchmark/test `math/rand` usage when not security-sensitive

## Important Rule

A finding being low-priority does not mean it disappears.

It must still become one of:

- fixed
- suppressed with rationale
- accepted with rationale
- deferred with rationale
- duplicate of a tracked issue
- converted into a CI/test invariant

## Validation checklist

- [ ] High-priority Codacy signal classes listed
- [ ] Medium-priority signal classes listed
- [ ] Low-priority/non-blocking classes listed
- [ ] Low-priority findings are still tracked
- [ ] No style-only finding is made release-blocking

## Production Security Candidate Review Queue

Production/security candidate rows generated from `issue-tracker.csv`: 136.

The following Codacy classes require careful review before constrained enforcement:

- production SQL/dynamic query findings
- production filesystem/path findings
- production command execution findings
- production file permission findings
- dependency/toolchain CVEs
- Docker root-user/container hardening findings

These are imported into `issue-tracker.csv` and should be grouped into root invariants in Phase 7.

These findings are not automatically treated as true positives.

These findings are not automatically suppressed.

## Validation checklist

- [ ] Production security candidate count generated
- [ ] Candidate classes listed
- [ ] They are not automatically treated as true positives
- [ ] They are not automatically suppressed
- [ ] Phase 7 root-invariant grouping is identified

## Likely Non-Blocking Findings

Test/docs likely non-blocking candidates generated from `issue-tracker.csv`: 997.

The baseline includes findings that are likely non-blocking for v1.10.0:

- markdownlint style findings in documentation
- Lizard complexity warnings in tests
- test-only file permission warnings
- test-only dynamic file read warnings
- deterministic benchmark/test randomness warnings

These findings remain tracked.

They may later become:

- suppressed with rationale
- accepted with rationale
- deferred to backlog
- fixed opportunistically

They must not block v1.10.0 merely because of scanner count.

## Validation checklist

- [ ] Test/docs likely non-blocking count generated
- [ ] Non-blocking classes listed
- [ ] Findings remain tracked
- [ ] No permanent suppression yet unless rationale is added
- [ ] Scanner count is not treated as release blocker
