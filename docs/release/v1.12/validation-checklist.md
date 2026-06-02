# v1.12 Phase Validation Checklist Template

Every v1.12 phase must complete this checklist before merge.

## Scope

- [ ] Phase objective is documented.
- [ ] Included work is listed.
- [ ] Excluded work is listed.
- [ ] No unrelated cleanup is included.
- [ ] No v2.x work is included.
- [ ] No repository/storage format change is included unless explicitly approved.

## Behavior

- [ ] CLI behavior unchanged or explicitly documented.
- [ ] JSON behavior unchanged or explicitly documented.
- [ ] Exit-code behavior unchanged or explicitly documented.
- [ ] Error semantics unchanged or explicitly documented.
- [ ] Human output unchanged or explicitly documented.

## Engine/catalog

- [ ] Engine dependency ownership is preserved.
- [ ] No correctness invariant lives only in CLI parsing.
- [ ] Request/result contracts include all required behavior.
- [ ] Catalog responsibilities are explicit where touched.
- [ ] Packed and legacy storage are both considered.

## Backend compatibility

- [ ] SQLite impact reviewed.
- [ ] PostgreSQL impact reviewed.
- [ ] No SQLite-only assumption introduced.
- [ ] No PostgreSQL compatibility removed.
- [ ] SQL dialect assumptions documented or isolated.

## Tests

- [ ] Targeted tests added or updated.
- [ ] Existing package tests pass.
- [ ] CLI parity tests pass where command routing changed.
- [ ] JSON parser tests pass where JSON output is touched.
- [ ] SQLite tests pass where catalog/db behavior is touched.
- [ ] PostgreSQL tests pass or are documented as not applicable.
- [ ] Race/adversarial/integration tests run if the phase touches mutation, restore, GC, verify, or recovery.

## Documentation

- [ ] Release phase document updated.
- [ ] Risk register updated.
- [ ] Coupling inventory updated.
- [ ] README/changelog updated if user-facing status changed.
- [ ] Accepted/deferred risks documented.

## Final gate

- [ ] No new S0/S1 risk is unknown.
- [ ] CI is green.
- [ ] Review summary states behavior impact.
- [ ] Review summary states remaining risk.
