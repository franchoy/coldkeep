# v1.10 Remediation Matrix Summary

Status: Complete  
Owner phase: Phase 7 — Remediation Matrix Construction

## Purpose

The remediation matrix is the work-package plan for the v1.10 stabilization train.

It groups raw findings from:

- Codacy
- external audit
- manual review
- CI proposal
- later release-gate review

into root-invariant remediation rows.

The matrix is not a scanner-count report.

The release plan follows matrix rows, not raw issue count.

## Source Inputs

Raw findings are stored in:

```text
docs/release/v1.10/issue-tracker.csv
```

Matrix rows are stored in:

```text
docs/release/v1.10/remediation-matrix.csv
```

## Matrix Rule

Every non-suppressed, non-duplicate raw issue must eventually map to one matrix row.

A matrix row may represent:

- one raw finding
- many repeated symptoms
- one scanner class
- one behavioral invariant
- one release-gate gap
- one documentation/risk decision

## Validation checklist

```markdown
- [ ] Matrix purpose is documented
- [ ] It says matrix rows are work packages
- [ ] It says raw issue count is not the plan
- [ ] It says every real raw issue must map to a matrix row
- [ ] It lists input and output files
```

# Grouping Strategy

## Group by root invariant

The matrix groups findings by the invariant that must hold after remediation.

Examples:

| Raw symptoms | Root invariant |
|---|---|
| Commands ignore trailing args | Commands must reject unexpected positional arguments before stateful work |
| `--reverse=false` still reverses | Boolean flags with explicit false must not be interpreted as true |
| Empty filters become broad searches | Explicit empty filters must be rejected before query execution |
| Duplicate benchmark case names overwrite rows | Benchmark comparison inputs must enforce unique case identity |
| NaN/Infinity enters JSON/CSV reports | Numeric inputs and metrics must reject non-finite values |
| JSON shorthand rejected inconsistently | JSON mode must be accepted consistently for supported commands |
| Codacy test-only permission warnings | Test-only permission findings need documented suppression or accepted-risk rationale |
| Codacy Go stdlib CVEs | Supported Go toolchain must not carry known unplanned CVEs |

## Do not group only by scanner rule

A scanner rule may appear in unrelated contexts.

For example:

- SQL-injection scanner findings in production code require different review from SQL-injection findings in tests.
- File-permission findings in tests differ from runtime filesystem permission findings.
- Complexity findings in critical restore logic differ from complexity findings in long adversarial tests.

## Do not over-deduplicate

Do not merge findings if they require different release targets.

Examples:

- CLI extra-arg rejection belongs to v1.10.1.
- Restore stored-path empty fallback belongs to v1.10.5.
- Benchmark numeric finiteness belongs to v1.10.6.
- Toolchain CVEs belong to Phase 10 / v1.10.2.

## Validation checklist

```markdown
- [ ] Root-invariant grouping rule documented
- [ ] Scanner-rule grouping warning documented
- [ ] Over-deduplication warning documented
- [ ] Examples cover CLI, validation, benchmark, Codacy, toolchain
```

# Generated Summary

Full generated counts are recorded in:

```text
docs/release/v1.10/remediation-matrix-summary.csv
```

Minimum dimensions recorded:

- release target
- severity
- domain
- status
- data-loss risk
- security risk
- determinism risk
- recovery risk
- CI gate requirement

## Validation checklist

```markdown
- [ ] remediation-matrix-summary.csv created
- [ ] Counts include release target
- [ ] Counts include severity
- [ ] Counts include domain
- [ ] Counts include status
- [ ] Counts include risk fields
- [ ] Counts include CI-gate requirement
- [ ] Summary markdown references the CSV
```

# Phase 7 Completion Statement

The v1.10 remediation matrix has been constructed.

Phase 7 completed:

- raw Codacy and external-audit findings grouped by root invariant
- remediation matrix rows created
- raw issue rows linked to matrix IDs
- first-pass release target mapping reviewed
- first-pass severity/domain/risk grouping reviewed
- accepted/deferred/suppressed decision logs initialized
- generated matrix summary counts

Phase 7 did not:

- fix findings
- close findings
- implement remediation
- change production code
- change tests
- change scripts
- change CI enforcement
- finalize S0/S1 candidate review
- define CI baseline

Phase 8 will record current CI state and validation gates.

## Validation checklist

```markdown
- [ ] Summary status is Complete
- [ ] Completion statement exists
- [ ] Completed work is listed
- [ ] Non-goals are listed
- [ ] It points to Phase 8
```

# Phase 12 Candidate Feed

The following matrix rows should be reviewed during Phase 12 - Initial S0/S1 Candidate Review.

```text
Potential Phase 12 S0/S1 matrix candidates: 45

CK-110-M001: S1 backlog migration :: Review dynamic SQL scanner findings
CK-110-M002: S1 backlog snapshot :: Review dynamic SQL scanner findings
CK-110-M003: S1 backlog snapshot :: Filesystem permission scanner findings must be reviewed by runtime area and threat model
CK-110-M004: S1 backlog storage :: Review dynamic SQL scanner findings
CK-110-M005: S1 backlog storage :: Review dynamic filesystem path scanner findings
CK-110-M006: S1 backlog storage :: Filesystem permission scanner findings must be reviewed by runtime area and threat model
CK-110-M007: S1 backlog storage :: Randomness scanner findings must distinguish deterministic tests/benchmarks from cryptographic use
CK-110-M008: S1 v1.10.1 json :: Stabilize JSON shorthand and machine output contract
CK-110-M009: S1 v1.10.2 dependencies :: Resolve Go toolchain vulnerability baseline
CK-110-M010: S1 v1.10.2 filesystem :: Review dynamic filesystem path scanner findings
CK-110-M011: S1 v1.10.2 filesystem :: Filesystem permission scanner findings must be reviewed by runtime area and threat model
CK-110-M012: S1 v1.10.2 security :: Review dynamic SQL scanner findings
CK-110-M013: S1 v1.10.2 validation :: Reject empty or whitespace-only filters
CK-110-M014: S1 v1.10.4 gc :: Review dynamic SQL scanner findings
CK-110-M015: S1 v1.10.5 restore :: Review dynamic filesystem path scanner findings
CK-110-M016: S1 v1.10.5 restore :: Filesystem permission scanner findings must be reviewed by runtime area and threat model
CK-110-M017: S1 v1.10.5 restore :: Restore mode value flags must reject empty values before fallback behavior
CK-110-M018: S1 v1.10.6 benchmark :: Command execution findings must be reviewed for trusted executable and argument boundaries
CK-110-M019: S1 v1.10.6 benchmark :: Review dynamic filesystem path scanner findings
CK-110-M020: S1 v1.10.6 benchmark :: Filesystem permission scanner findings must be reviewed by runtime area and threat model
CK-110-M021: S1 v1.10.6 benchmark :: Randomness scanner findings must distinguish deterministic tests/benchmarks from cryptographic use
CK-110-M022: S1 v1.10.6 ci :: Codacy finding must be classified before enforcement
CK-110-M023: S1 v1.10.6 codacy :: Command execution findings must be reviewed for trusted executable and argument boundaries
CK-110-M026: S2 backlog storage :: Review production complexity hotspots
CK-110-M027: S2 backlog verify :: Review production complexity hotspots
CK-110-M035: S2 v1.10.3 packed-storage :: Review production complexity hotspots
CK-110-M036: S2 v1.10.4 gc :: Review production complexity hotspots
CK-110-M037: S2 v1.10.5 recovery :: Review production complexity hotspots
CK-110-M038: S2 v1.10.5 restore :: Review production complexity hotspots
CK-110-M054: S3 backlog verify :: Test complexity findings must be reviewed without weakening invariant coverage
CK-110-M055: S3 backlog verify :: Classify test-only scanner findings
CK-110-M060: S3 v1.10.3 packed-storage :: Test complexity findings must be reviewed without weakening invariant coverage
CK-110-M061: S3 v1.10.3 packed-storage :: Classify test-only scanner findings
CK-110-M062: S3 v1.10.4 gc :: Test complexity findings must be reviewed without weakening invariant coverage
CK-110-M063: S3 v1.10.4 gc :: Classify test-only scanner findings
CK-110-M064: S3 v1.10.5 recovery :: Test complexity findings must be reviewed without weakening invariant coverage
CK-110-M065: S3 v1.10.5 recovery :: Classify test-only scanner findings
CK-110-M066: S3 v1.10.5 restore :: Test complexity findings must be reviewed without weakening invariant coverage
CK-110-M067: S3 v1.10.5 restore :: Classify test-only scanner findings
CK-110-M075: S4 v1.10.4 gc :: Codacy finding must be classified before enforcement
CK-110-M079: S3 backlog gc :: Review removal and maintenance backlog behavior gaps
CK-110-M081: S3 backlog packed-storage :: Review packed-storage parity and reachability gaps
CK-110-M082: S3 backlog restore :: Review restore and recovery backlog behavior gaps
CK-110-M085: S3 backlog verify :: Review verify and inspect behavior gaps
CK-110-M087: S2 v1.10.5 restore :: Reject restore mode-specific flags outside valid mode
```

Phase 7 does not finalize the S0/S1 list.

Phase 12 will decide whether each candidate is:

- confirmed S0/S1
- downgraded after evidence review
- accepted with rationale
- deferred with rationale
- converted into a required regression test or CI gate

## Validation checklist

```markdown
- [ ] Potential S0/S1 matrix candidates generated
- [ ] Candidate matrix IDs recorded or referenced
- [ ] Phase 12 is identified as owner
- [ ] No final S0/S1 decision made in Phase 7
```