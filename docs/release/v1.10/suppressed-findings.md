# v1.10 Suppressed Findings

Status: Active  
Owner phase: Phase 7 and later

## Purpose

Records scanner findings suppressed as false positives, test-only noise, intentional patterns, or non-blocking style findings.

Suppression requires rationale.

## Required Fields

Every suppression must include:

- suppression ID
- tool
- rule ID
- finding ID or matrix ID
- file/scope
- reason
- why this is safe
- review condition
- related issue or matrix row

## Current Suppressions

No permanent suppressions recorded during Phase 7.

Phase 7 may create matrix rows that are likely suppression candidates, but final suppression requires rationale.

# Codacy Suppression Format

Codacy suppressions must use this format.

## CK-110-SUP-XXX - Short title

Suppression ID: CK-110-SUP-XXX  
Linked issue ID: CK-110-XXXX  
Linked matrix ID: CK-110-MXXX  
Codacy source ID: TODO  
Tool: TODO  
Rule ID: TODO  
File/scope: TODO  
Area: production | test | docs | script | ci | docker  
Decision date: TODO  
Owner: TODO  

### Finding

TODO.

### Decision

suppressed

### Reason

TODO.

### Why This Is Safe

TODO.

### Review Condition

TODO.

### Reopen Condition

TODO.

### Related Release

TODO.

## Phase 11 Note

No permanent Codacy suppressions are created by Phase 11 unless each has a complete rationale.

Phase 11 defines the policy. Later phases may create actual suppression records.
