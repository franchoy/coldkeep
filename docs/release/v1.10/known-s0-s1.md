# v1.10 Known S0/S1 Candidate Issues

Status: Complete  
Owner phase: Phase 12 — Initial S0/S1 Candidate Review

# Phase 12 Completion Statement

The initial v1.10 S0/S1 candidate review has been completed.

Phase 12 completed:

- matrix-based S0/S1 candidate extraction
- manual high-risk family cross-check
- final severity review for each candidate
- confirmed S0/S1 candidate listing
- downgraded/not-S0-S1 candidate listing
- matrix row severity/rationale updates
- issue tracker severity/rationale updates
- accepted/deferred S0/S1 decision log updates where applicable
- release-gate update for Phase 12 review

Phase 12 did not:

- implement fixes
- change production code
- change tests
- change scripts
- change CI workflows
- change dependencies
- tag a release

v1.10.0 may proceed to Phase 13 only if no candidate remains pending review.

## Purpose

This document records the initial S0/S1 candidate review for the Coldkeep v1.10 stabilization train.

Phase 12 does not implement remediation.

Phase 12 exists to ensure that no known catastrophic or critical candidate remains unknown before v1.10.0 is released.

## v1.10.0 Rule

v1.10.0 is a baseline/freeze release.

It may ship with known S0/S1 candidates only if every candidate has:

- issue ID or matrix ID,
- severity decision,
- domain,
- release target,
- status,
- rationale,
- required regression or CI expectation where applicable,
- clear follow-up release.

## Not Allowed

v1.10.0 must not ship with:

- unknown S0/S1 candidates,
- unreviewed high-risk restore/path findings,
- unreviewed GC/reachability deletion findings,
- unreviewed false-verification candidates,
- unreviewed dependency/toolchain vulnerability findings,
- unreviewed production security candidates,
- unreviewed CI false-success findings that can hide release failure.

# Severity Criteria Used In This Review

## S0 — Catastrophic

An issue is S0 if it can credibly cause or hide:

- silent data corruption,
- reachable data deletion,
- GC deleting retained data,
- snapshot retention violation,
- restore writing outside the intended root,
- false verification success on corrupted or missing data,
- unrecoverable restore failure for valid retained data,
- recovery flow destroying or hiding valid data,
- migration causing data loss,
- repository state becoming unrecoverable.

S0 normally blocks release unless fixed, proven impossible, accepted with exceptional rationale, or outside the shipped behavior with explicit deferral.

## S1 — Critical

An issue is S1 if it can credibly cause or hide:

- crash-consistency failure,
- incorrect refcount repair,
- stale liveness causing unsafe retention or deletion,
- unsafe container filename trust,
- serious JSON automation breakage for correctness-critical commands,
- migration parity failure,
- restore/recovery partial-failure hazard,
- CI/release false success hiding correctness failure,
- dependency/toolchain vulnerability without plan.

S1 must not remain unknown or untriaged.


# Review Summary

Total candidates reviewed: 72

## Counts

- S1: 28
- S3: 25
- S2: 17
- S4: 2

## Confirmed S0/S1 Candidates

| Matrix ID | Final severity | Target | Domain | Title | Rationale | Follow-up |
|---|---|---|---|---|---|---|
| `CK-110-M001` | S1 | backlog | migration | Review dynamic SQL scanner findings | Candidate remains critical for migration because available evidence indicates a trust-boundary, restore/recovery, or correctness/CI hazard; not yet proven catastrophic (S0), but requires tracked mitigation before closure. | backlog must include targeted regression coverage, explicit acceptance criteria, and closure proof for matrix row CK-110-M001. |
| `CK-110-M002` | S1 | backlog | snapshot | Review dynamic SQL scanner findings | Candidate remains critical for snapshot because available evidence indicates a trust-boundary, restore/recovery, or correctness/CI hazard; not yet proven catastrophic (S0), but requires tracked mitigation before closure. | backlog must include targeted regression coverage, explicit acceptance criteria, and closure proof for matrix row CK-110-M002. |
| `CK-110-M003` | S1 | backlog | snapshot | Filesystem permission scanner findings must be reviewed by runtime area and threat model | Candidate remains critical for snapshot because available evidence indicates a trust-boundary, restore/recovery, or correctness/CI hazard; not yet proven catastrophic (S0), but requires tracked mitigation before closure. | backlog must include targeted regression coverage, explicit acceptance criteria, and closure proof for matrix row CK-110-M003. |
| `CK-110-M004` | S1 | backlog | storage | Review dynamic SQL scanner findings | Candidate remains critical for storage because available evidence indicates a trust-boundary, restore/recovery, or correctness/CI hazard; not yet proven catastrophic (S0), but requires tracked mitigation before closure. | backlog must include targeted regression coverage, explicit acceptance criteria, and closure proof for matrix row CK-110-M004. |
| `CK-110-M005` | S1 | backlog | storage | Review dynamic filesystem path scanner findings | Candidate remains critical for storage because available evidence indicates a trust-boundary, restore/recovery, or correctness/CI hazard; not yet proven catastrophic (S0), but requires tracked mitigation before closure. | backlog must include targeted regression coverage, explicit acceptance criteria, and closure proof for matrix row CK-110-M005. |
| `CK-110-M006` | S1 | backlog | storage | Filesystem permission scanner findings must be reviewed by runtime area and threat model | Candidate remains critical for storage because available evidence indicates a trust-boundary, restore/recovery, or correctness/CI hazard; not yet proven catastrophic (S0), but requires tracked mitigation before closure. | backlog must include targeted regression coverage, explicit acceptance criteria, and closure proof for matrix row CK-110-M006. |
| `CK-110-M007` | S1 | backlog | storage | Randomness scanner findings must distinguish deterministic tests/benchmarks from cryptographic u | Candidate remains critical for storage because available evidence indicates a trust-boundary, restore/recovery, or correctness/CI hazard; not yet proven catastrophic (S0), but requires tracked mitigation before closure. | backlog must include targeted regression coverage, explicit acceptance criteria, and closure proof for matrix row CK-110-M007. |
| `CK-110-M008` | S1 | v1.10.1 | json | Stabilize JSON shorthand and machine output contract | Candidate remains critical for json because available evidence indicates a trust-boundary, restore/recovery, or correctness/CI hazard; not yet proven catastrophic (S0), but requires tracked mitigation before closure. | v1.10.1 must include targeted regression coverage, explicit acceptance criteria, and closure proof for matrix row CK-110-M008. |
| `CK-110-M009` | S1 | v1.10.2 | dependencies | Resolve Go toolchain vulnerability baseline | Candidate remains critical for dependencies because available evidence indicates a trust-boundary, restore/recovery, or correctness/CI hazard; not yet proven catastrophic (S0), but requires tracked mitigation before closure. | v1.10.2 must include targeted regression coverage, explicit acceptance criteria, and closure proof for matrix row CK-110-M009. |
| `CK-110-M010` | S1 | v1.10.2 | filesystem | Review dynamic filesystem path scanner findings | Candidate remains critical for filesystem because available evidence indicates a trust-boundary, restore/recovery, or correctness/CI hazard; not yet proven catastrophic (S0), but requires tracked mitigation before closure. | v1.10.2 must include targeted regression coverage, explicit acceptance criteria, and closure proof for matrix row CK-110-M010. |
| `CK-110-M011` | S1 | v1.10.2 | filesystem | Filesystem permission scanner findings must be reviewed by runtime area and threat model | Candidate remains critical for filesystem because available evidence indicates a trust-boundary, restore/recovery, or correctness/CI hazard; not yet proven catastrophic (S0), but requires tracked mitigation before closure. | v1.10.2 must include targeted regression coverage, explicit acceptance criteria, and closure proof for matrix row CK-110-M011. |
| `CK-110-M012` | S1 | v1.10.2 | security | Review dynamic SQL scanner findings | Candidate remains critical for security because available evidence indicates a trust-boundary, restore/recovery, or correctness/CI hazard; not yet proven catastrophic (S0), but requires tracked mitigation before closure. | v1.10.2 must include targeted regression coverage, explicit acceptance criteria, and closure proof for matrix row CK-110-M012. |
| `CK-110-M013` | S1 | v1.10.2 | validation | Reject empty or whitespace-only filters | Candidate remains critical for validation because available evidence indicates a trust-boundary, restore/recovery, or correctness/CI hazard; not yet proven catastrophic (S0), but requires tracked mitigation before closure. | v1.10.2 must include targeted regression coverage, explicit acceptance criteria, and closure proof for matrix row CK-110-M013. |
| `CK-110-M014` | S1 | v1.10.4 | gc | Review dynamic SQL scanner findings | Candidate remains critical for gc because available evidence indicates a trust-boundary, restore/recovery, or correctness/CI hazard; not yet proven catastrophic (S0), but requires tracked mitigation before closure. | v1.10.4 must include targeted regression coverage, explicit acceptance criteria, and closure proof for matrix row CK-110-M014. |
| `CK-110-M015` | S1 | v1.10.5 | restore | Review dynamic filesystem path scanner findings | Candidate remains critical for restore because available evidence indicates a trust-boundary, restore/recovery, or correctness/CI hazard; not yet proven catastrophic (S0), but requires tracked mitigation before closure. | v1.10.5 must include targeted regression coverage, explicit acceptance criteria, and closure proof for matrix row CK-110-M015. |
| `CK-110-M016` | S1 | v1.10.5 | restore | Filesystem permission scanner findings must be reviewed by runtime area and threat model | Candidate remains critical for restore because available evidence indicates a trust-boundary, restore/recovery, or correctness/CI hazard; not yet proven catastrophic (S0), but requires tracked mitigation before closure. | v1.10.5 must include targeted regression coverage, explicit acceptance criteria, and closure proof for matrix row CK-110-M016. |
| `CK-110-M017` | S1 | v1.10.5 | restore | Restore mode value flags must reject empty values before fallback behavior | Candidate remains critical for restore because available evidence indicates a trust-boundary, restore/recovery, or correctness/CI hazard; not yet proven catastrophic (S0), but requires tracked mitigation before closure. | v1.10.5 must include targeted regression coverage, explicit acceptance criteria, and closure proof for matrix row CK-110-M017. |
| `CK-110-M018` | S1 | v1.10.6 | benchmark | Command execution findings must be reviewed for trusted executable and argument boundaries | Candidate remains critical for benchmark because available evidence indicates a trust-boundary, restore/recovery, or correctness/CI hazard; not yet proven catastrophic (S0), but requires tracked mitigation before closure. | v1.10.6 must include targeted regression coverage, explicit acceptance criteria, and closure proof for matrix row CK-110-M018. |
| `CK-110-M019` | S1 | v1.10.6 | benchmark | Review dynamic filesystem path scanner findings | Candidate remains critical for benchmark because available evidence indicates a trust-boundary, restore/recovery, or correctness/CI hazard; not yet proven catastrophic (S0), but requires tracked mitigation before closure. | v1.10.6 must include targeted regression coverage, explicit acceptance criteria, and closure proof for matrix row CK-110-M019. |
| `CK-110-M020` | S1 | v1.10.6 | benchmark | Filesystem permission scanner findings must be reviewed by runtime area and threat model | Candidate remains critical for benchmark because available evidence indicates a trust-boundary, restore/recovery, or correctness/CI hazard; not yet proven catastrophic (S0), but requires tracked mitigation before closure. | v1.10.6 must include targeted regression coverage, explicit acceptance criteria, and closure proof for matrix row CK-110-M020. |
| `CK-110-M021` | S1 | v1.10.6 | benchmark | Randomness scanner findings must distinguish deterministic tests/benchmarks from cryptographic u | Candidate remains critical for benchmark because available evidence indicates a trust-boundary, restore/recovery, or correctness/CI hazard; not yet proven catastrophic (S0), but requires tracked mitigation before closure. | v1.10.6 must include targeted regression coverage, explicit acceptance criteria, and closure proof for matrix row CK-110-M021. |
| `CK-110-M022` | S1 | v1.10.6 | ci | Codacy finding must be classified before enforcement | Candidate remains critical for ci because available evidence indicates a trust-boundary, restore/recovery, or correctness/CI hazard; not yet proven catastrophic (S0), but requires tracked mitigation before closure. | v1.10.6 must include targeted regression coverage, explicit acceptance criteria, and closure proof for matrix row CK-110-M022. |
| `CK-110-M023` | S1 | v1.10.6 | codacy | Command execution findings must be reviewed for trusted executable and argument boundaries | Candidate remains critical for codacy because available evidence indicates a trust-boundary, restore/recovery, or correctness/CI hazard; not yet proven catastrophic (S0), but requires tracked mitigation before closure. | v1.10.6 must include targeted regression coverage, explicit acceptance criteria, and closure proof for matrix row CK-110-M023. |
| `CK-110-M037` | S1 | v1.10.5 | recovery | Review production complexity hotspots | Candidate remains critical for recovery because available evidence indicates a trust-boundary, restore/recovery, or correctness/CI hazard; not yet proven catastrophic (S0), but requires tracked mitigation before closure. | v1.10.5 must include targeted regression coverage, explicit acceptance criteria, and closure proof for matrix row CK-110-M037. |
| `CK-110-M038` | S1 | v1.10.5 | restore | Review production complexity hotspots | Candidate remains critical for restore because available evidence indicates a trust-boundary, restore/recovery, or correctness/CI hazard; not yet proven catastrophic (S0), but requires tracked mitigation before closure. | v1.10.5 must include targeted regression coverage, explicit acceptance criteria, and closure proof for matrix row CK-110-M038. |
| `CK-110-M043` | S1 | v1.10.6 | ci | Harden release and benchmark script output validation | Candidate remains critical for ci because available evidence indicates a trust-boundary, restore/recovery, or correctness/CI hazard; not yet proven catastrophic (S0), but requires tracked mitigation before closure. | v1.10.6 must include targeted regression coverage, explicit acceptance criteria, and closure proof for matrix row CK-110-M043. |
| `CK-110-M045` | S1 | v1.10.6 | tooling | Release and benchmark scripts must not mask failures or mishandle unsafe inputs | Candidate remains critical for tooling because available evidence indicates a trust-boundary, restore/recovery, or correctness/CI hazard; not yet proven catastrophic (S0), but requires tracked mitigation before closure. | v1.10.6 must include targeted regression coverage, explicit acceptance criteria, and closure proof for matrix row CK-110-M045. |
| `CK-110-M087` | S1 | v1.10.5 | restore | Reject restore mode-specific flags outside valid mode | Candidate remains critical for restore because available evidence indicates a trust-boundary, restore/recovery, or correctness/CI hazard; not yet proven catastrophic (S0), but requires tracked mitigation before closure. | v1.10.5 must include targeted regression coverage, explicit acceptance criteria, and closure proof for matrix row CK-110-M087. |

## Downgraded / Not-S0-S1 Candidates

| Matrix ID | Final severity | Target | Domain | Title | Rationale | Follow-up |
|---|---|---|---|---|---|---|
| `CK-110-M024` | S2 | backlog | migration | Review production complexity hotspots | Candidate is real and non-trivial, but current evidence supports major risk (S2) rather than critical S1; risk remains tracked with release-targeted remediation. | backlog should implement scoped hardening/tests and retain matrix linkage until acceptance criteria pass for CK-110-M024. |
| `CK-110-M025` | S2 | backlog | snapshot | Review production complexity hotspots | Candidate is real and non-trivial, but current evidence supports major risk (S2) rather than critical S1; risk remains tracked with release-targeted remediation. | backlog should implement scoped hardening/tests and retain matrix linkage until acceptance criteria pass for CK-110-M025. |
| `CK-110-M026` | S2 | backlog | storage | Review production complexity hotspots | Candidate is real and non-trivial, but current evidence supports major risk (S2) rather than critical S1; risk remains tracked with release-targeted remediation. | backlog should implement scoped hardening/tests and retain matrix linkage until acceptance criteria pass for CK-110-M026. |
| `CK-110-M027` | S2 | backlog | verify | Review production complexity hotspots | Candidate is real and non-trivial, but current evidence supports major risk (S2) rather than critical S1; risk remains tracked with release-targeted remediation. | backlog should implement scoped hardening/tests and retain matrix linkage until acceptance criteria pass for CK-110-M027. |
| `CK-110-M031` | S2 | v1.10.2 | filesystem | Review production complexity hotspots | Candidate is real and non-trivial, but current evidence supports major risk (S2) rather than critical S1; risk remains tracked with release-targeted remediation. | v1.10.2 should implement scoped hardening/tests and retain matrix linkage until acceptance criteria pass for CK-110-M031. |
| `CK-110-M032` | S2 | v1.10.2 | snapshot | Snapshot tag input and filtering must use consistent normalization | Candidate is real and non-trivial, but current evidence supports major risk (S2) rather than critical S1; risk remains tracked with release-targeted remediation. | v1.10.2 should implement scoped hardening/tests and retain matrix linkage until acceptance criteria pass for CK-110-M032. |
| `CK-110-M033` | S2 | v1.10.2 | validation | CLI validation must happen before repository initialization when possible | Candidate is real and non-trivial, but current evidence supports major risk (S2) rather than critical S1; risk remains tracked with release-targeted remediation. | v1.10.2 should implement scoped hardening/tests and retain matrix linkage until acceptance criteria pass for CK-110-M033. |
| `CK-110-M034` | S2 | v1.10.2 | validation | Reject invalid numeric and non-finite values | Candidate is real and non-trivial, but current evidence supports major risk (S2) rather than critical S1; risk remains tracked with release-targeted remediation. | v1.10.2 should implement scoped hardening/tests and retain matrix linkage until acceptance criteria pass for CK-110-M034. |
| `CK-110-M035` | S2 | v1.10.3 | packed-storage | Review production complexity hotspots | Candidate is real and non-trivial, but current evidence supports major risk (S2) rather than critical S1; risk remains tracked with release-targeted remediation. | v1.10.3 should implement scoped hardening/tests and retain matrix linkage until acceptance criteria pass for CK-110-M035. |
| `CK-110-M036` | S2 | v1.10.4 | gc | Review production complexity hotspots | Candidate is real and non-trivial, but current evidence supports major risk (S2) rather than critical S1; risk remains tracked with release-targeted remediation. | v1.10.4 should implement scoped hardening/tests and retain matrix linkage until acceptance criteria pass for CK-110-M036. |
| `CK-110-M039` | S2 | v1.10.6 | benchmark | Reject duplicate benchmark case identities | Candidate is real and non-trivial, but current evidence supports major risk (S2) rather than critical S1; risk remains tracked with release-targeted remediation. | v1.10.6 should implement scoped hardening/tests and retain matrix linkage until acceptance criteria pass for CK-110-M039. |
| `CK-110-M040` | S2 | v1.10.6 | benchmark | Benchmark comparison must reject self-comparison and invalid input pairing | Candidate is real and non-trivial, but current evidence supports major risk (S2) rather than critical S1; risk remains tracked with release-targeted remediation. | v1.10.6 should implement scoped hardening/tests and retain matrix linkage until acceptance criteria pass for CK-110-M040. |
| `CK-110-M041` | S2 | v1.10.6 | benchmark | Validate benchmark report type and envelope | Candidate is real and non-trivial, but current evidence supports major risk (S2) rather than critical S1; risk remains tracked with release-targeted remediation. | v1.10.6 should implement scoped hardening/tests and retain matrix linkage until acceptance criteria pass for CK-110-M041. |
| `CK-110-M042` | S2 | v1.10.6 | benchmark | Reject invalid numeric and non-finite values | Candidate is real and non-trivial, but current evidence supports major risk (S2) rather than critical S1; risk remains tracked with release-targeted remediation. | v1.10.6 should implement scoped hardening/tests and retain matrix linkage until acceptance criteria pass for CK-110-M042. |
| `CK-110-M044` | S2 | v1.10.6 | codacy | Review production complexity hotspots | Candidate is real and non-trivial, but current evidence supports major risk (S2) rather than critical S1; risk remains tracked with release-targeted remediation. | v1.10.6 should implement scoped hardening/tests and retain matrix linkage until acceptance criteria pass for CK-110-M044. |
| `CK-110-M046` | S2 | v1.10.6 | tooling | Harden release and benchmark script output validation | Candidate is real and non-trivial, but current evidence supports major risk (S2) rather than critical S1; risk remains tracked with release-targeted remediation. | v1.10.6 should implement scoped hardening/tests and retain matrix linkage until acceptance criteria pass for CK-110-M046. |
| `CK-110-M047` | S3 | backlog | migration | Test complexity findings must be reviewed without weakening invariant coverage | Candidate is meaningful for quality/safety posture but currently appears moderate (S3) with no direct evidence of critical-path catastrophic impact. | Track in backlog; keep documented rationale and promote only if new evidence shows critical-path risk for CK-110-M047. |
| `CK-110-M048` | S3 | backlog | migration | Classify test-only scanner findings | Candidate is meaningful for quality/safety posture but currently appears moderate (S3) with no direct evidence of critical-path catastrophic impact. | Track in backlog; keep documented rationale and promote only if new evidence shows critical-path risk for CK-110-M048. |
| `CK-110-M051` | S3 | backlog | storage | Test complexity findings must be reviewed without weakening invariant coverage | Candidate is meaningful for quality/safety posture but currently appears moderate (S3) with no direct evidence of critical-path catastrophic impact. | Track in backlog; keep documented rationale and promote only if new evidence shows critical-path risk for CK-110-M051. |
| `CK-110-M052` | S3 | backlog | storage | Classify test-only scanner findings | Candidate is meaningful for quality/safety posture but currently appears moderate (S3) with no direct evidence of critical-path catastrophic impact. | Track in backlog; keep documented rationale and promote only if new evidence shows critical-path risk for CK-110-M052. |
| `CK-110-M054` | S3 | backlog | verify | Test complexity findings must be reviewed without weakening invariant coverage | Candidate is meaningful for quality/safety posture but currently appears moderate (S3) with no direct evidence of critical-path catastrophic impact. | Track in backlog; keep documented rationale and promote only if new evidence shows critical-path risk for CK-110-M054. |
| `CK-110-M055` | S3 | backlog | verify | Classify test-only scanner findings | Candidate is meaningful for quality/safety posture but currently appears moderate (S3) with no direct evidence of critical-path catastrophic impact. | Track in backlog; keep documented rationale and promote only if new evidence shows critical-path risk for CK-110-M055. |
| `CK-110-M056` | S3 | v1.10.2 | filesystem | Test complexity findings must be reviewed without weakening invariant coverage | Candidate is meaningful for quality/safety posture but currently appears moderate (S3) with no direct evidence of critical-path catastrophic impact. | Track in v1.10.2; keep documented rationale and promote only if new evidence shows critical-path risk for CK-110-M056. |
| `CK-110-M057` | S3 | v1.10.2 | filesystem | Classify test-only scanner findings | Candidate is meaningful for quality/safety posture but currently appears moderate (S3) with no direct evidence of critical-path catastrophic impact. | Track in v1.10.2; keep documented rationale and promote only if new evidence shows critical-path risk for CK-110-M057. |
| `CK-110-M058` | S3 | v1.10.2 | security | Container packaging should avoid unnecessary root execution where applicable | Candidate is meaningful for quality/safety posture but currently appears moderate (S3) with no direct evidence of critical-path catastrophic impact. | Track in v1.10.2; keep documented rationale and promote only if new evidence shows critical-path risk for CK-110-M058. |
| `CK-110-M059` | S3 | v1.10.2 | security | Classify test-only scanner findings | Candidate is meaningful for quality/safety posture but currently appears moderate (S3) with no direct evidence of critical-path catastrophic impact. | Track in v1.10.2; keep documented rationale and promote only if new evidence shows critical-path risk for CK-110-M059. |
| `CK-110-M060` | S3 | v1.10.3 | packed-storage | Test complexity findings must be reviewed without weakening invariant coverage | Candidate is meaningful for quality/safety posture but currently appears moderate (S3) with no direct evidence of critical-path catastrophic impact. | Track in v1.10.3; keep documented rationale and promote only if new evidence shows critical-path risk for CK-110-M060. |
| `CK-110-M061` | S3 | v1.10.3 | packed-storage | Classify test-only scanner findings | Candidate is meaningful for quality/safety posture but currently appears moderate (S3) with no direct evidence of critical-path catastrophic impact. | Track in v1.10.3; keep documented rationale and promote only if new evidence shows critical-path risk for CK-110-M061. |
| `CK-110-M062` | S3 | v1.10.4 | gc | Test complexity findings must be reviewed without weakening invariant coverage | Candidate is meaningful for quality/safety posture but currently appears moderate (S3) with no direct evidence of critical-path catastrophic impact. | Track in v1.10.4; keep documented rationale and promote only if new evidence shows critical-path risk for CK-110-M062. |
| `CK-110-M063` | S3 | v1.10.4 | gc | Classify test-only scanner findings | Candidate is meaningful for quality/safety posture but currently appears moderate (S3) with no direct evidence of critical-path catastrophic impact. | Track in v1.10.4; keep documented rationale and promote only if new evidence shows critical-path risk for CK-110-M063. |
| `CK-110-M064` | S3 | v1.10.5 | recovery | Test complexity findings must be reviewed without weakening invariant coverage | Candidate is meaningful for quality/safety posture but currently appears moderate (S3) with no direct evidence of critical-path catastrophic impact. | Track in v1.10.5; keep documented rationale and promote only if new evidence shows critical-path risk for CK-110-M064. |
| `CK-110-M065` | S3 | v1.10.5 | recovery | Classify test-only scanner findings | Candidate is meaningful for quality/safety posture but currently appears moderate (S3) with no direct evidence of critical-path catastrophic impact. | Track in v1.10.5; keep documented rationale and promote only if new evidence shows critical-path risk for CK-110-M065. |
| `CK-110-M066` | S3 | v1.10.5 | restore | Test complexity findings must be reviewed without weakening invariant coverage | Candidate is meaningful for quality/safety posture but currently appears moderate (S3) with no direct evidence of critical-path catastrophic impact. | Track in v1.10.5; keep documented rationale and promote only if new evidence shows critical-path risk for CK-110-M066. |
| `CK-110-M067` | S3 | v1.10.5 | restore | Classify test-only scanner findings | Candidate is meaningful for quality/safety posture but currently appears moderate (S3) with no direct evidence of critical-path catastrophic impact. | Track in v1.10.5; keep documented rationale and promote only if new evidence shows critical-path risk for CK-110-M067. |
| `CK-110-M074` | S4 | v1.10.2 | filesystem | Classify documentation style findings | Candidate appears low-risk/non-blocking in current context; retain traceability and avoid treating it as release-blocking without new evidence. | Keep tracked in v1.10.2 and re-evaluate if domain usage or threat context changes for CK-110-M074. |
| `CK-110-M075` | S4 | v1.10.4 | gc | Codacy finding must be classified before enforcement | Candidate appears low-risk/non-blocking in current context; retain traceability and avoid treating it as release-blocking without new evidence. | Keep tracked in v1.10.4 and re-evaluate if domain usage or threat context changes for CK-110-M075. |
| `CK-110-M077` | S3 | backlog | benchmark | Review benchmark backlog correctness gaps | Candidate is meaningful for quality/safety posture but currently appears moderate (S3) with no direct evidence of critical-path catastrophic impact. | Track in backlog; keep documented rationale and promote only if new evidence shows critical-path risk for CK-110-M077. |
| `CK-110-M079` | S3 | backlog | gc | Review removal and maintenance backlog behavior gaps | Candidate is meaningful for quality/safety posture but currently appears moderate (S3) with no direct evidence of critical-path catastrophic impact. | Track in backlog; keep documented rationale and promote only if new evidence shows critical-path risk for CK-110-M079. |
| `CK-110-M081` | S3 | backlog | packed-storage | Review packed-storage parity and reachability gaps | Candidate is meaningful for quality/safety posture but currently appears moderate (S3) with no direct evidence of critical-path catastrophic impact. | Track in backlog; keep documented rationale and promote only if new evidence shows critical-path risk for CK-110-M081. |
| `CK-110-M082` | S3 | backlog | restore | Review restore and recovery backlog behavior gaps | Candidate is meaningful for quality/safety posture but currently appears moderate (S3) with no direct evidence of critical-path catastrophic impact. | Track in backlog; keep documented rationale and promote only if new evidence shows critical-path risk for CK-110-M082. |
| `CK-110-M083` | S3 | backlog | tooling | Review script and tooling backlog behavior gaps | Candidate is meaningful for quality/safety posture but currently appears moderate (S3) with no direct evidence of critical-path catastrophic impact. | Track in backlog; keep documented rationale and promote only if new evidence shows critical-path risk for CK-110-M083. |
| `CK-110-M084` | S3 | backlog | snapshot | Review snapshot behavior parity gaps | Candidate is meaningful for quality/safety posture but currently appears moderate (S3) with no direct evidence of critical-path catastrophic impact. | Track in backlog; keep documented rationale and promote only if new evidence shows critical-path risk for CK-110-M084. |
| `CK-110-M085` | S3 | backlog | verify | Review verify and inspect behavior gaps | Candidate is meaningful for quality/safety posture but currently appears moderate (S3) with no direct evidence of critical-path catastrophic impact. | Track in backlog; keep documented rationale and promote only if new evidence shows critical-path risk for CK-110-M085. |
| `CK-110-M088` | S2 | v1.10.6 | tooling | Harden script JSON emission contract | Candidate is real and non-trivial, but current evidence supports major risk (S2) rather than critical S1; risk remains tracked with release-targeted remediation. | v1.10.6 should implement scoped hardening/tests and retain matrix linkage until acceptance criteria pass for CK-110-M088. |

# Manual High-Risk Family Cross-Check

The following high-risk families were manually searched in the remediation matrix:

- path traversal
- unsafe container filename trust
- restore overwrite / symlink / TOCTOU
- GC reachability / retained data
- refcount / liveness
- false verification / corruption
- migration parity / data loss
- dependency CVE
- JSON automation false success
- benchmark / CI false success

Result:

```text
All searched high-risk families were reviewed against the matrix and compared with s0-s1-candidate-summary.csv.

Direct high-risk rows (restore/recovery/gc/refcount/verify/migration/dependencies/ci and traversal-style findings) are represented in the candidate summary.

Term-match rows intentionally not in the candidate summary are non-S0/S1 matrix items (mostly CLI contract hardening, test/docs/tooling backlog classification, and low-risk documentation/style rows) with no medium/high security_risk, no medium/high recovery_risk, and no current S0/S1 severity. These remain tracked in remediation-matrix.csv and are not treated as unknown risk.

The phrase "container filename" had no direct matrix-term hits; related container hardening risk remains tracked under CK-110-M058.
```