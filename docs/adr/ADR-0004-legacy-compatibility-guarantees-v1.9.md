# ADR-0004: Legacy Compatibility Guarantees (v1.9)

- Status: Accepted
- Date: 2026-05-09
- Scope: Repository upgrade and migration semantics

## Context

Coldkeep supports repositories created by historical v1.x releases. Upgrade
behavior must be explicit to prevent assumptions that startup migrations imply
automatic payload rewrites or recompression.

## Decision

Legacy compatibility guarantees are frozen for v1.9:

1. Mandatory
   - historical repositories remain readable/restorable
   - mixed repositories remain valid steady-state

2. Optional / not guaranteed
   - automatic rewrite of historical repositories
   - automatic recompression of historical repositories
   - eager/background migration of historical layouts

3. Migration semantics
   - startup migrations are compatibility/readability migrations
   - migration defaults affect future writes only
   - payload/layout rewrites, if ever introduced, must be explicit operator
     actions and documented as opt-in workflows

## Consequences

- operators can upgrade without surprise rewrite behavior
- compatibility and optimization concerns remain cleanly separated
- future engine work can rely on stable non-destructive upgrade semantics
