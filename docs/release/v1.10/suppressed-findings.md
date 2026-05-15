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
