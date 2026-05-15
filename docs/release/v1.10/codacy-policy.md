# v1.10 Codacy Policy

Status: Pending  
Owner phase: Phase 11 - Codacy Policy Baseline

## Purpose

Defines how Codacy is used during v1.10.

Codacy should provide:

- passive repository analysis
- PR annotations
- security surfacing
- dependency vulnerability visibility
- maintainability trend visibility

Codacy should not block v1.10 on:

- style-only findings
- naming preferences
- generic maintainability score
- abstraction preferences
- intentionally explicit invariant-heavy logic

The CI proposal recommends adopting Codacy in a constrained, observability-focused role, not as an architectural or correctness authority.
