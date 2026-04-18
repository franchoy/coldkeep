## Title
v1.3: Snapshot layer + snapshot-aware retention (Phases 1-7)

## Summary
Describe the snapshot system changes introduced in this PR.

- What snapshot capabilities are added/changed (`create`, `restore`, `list`, `show`, `stats`, `diff`, `delete`)
- How retention behavior is affected
- Any CLI/documentation/test updates relevant for reviewers

## Key Invariants
Confirm and summarize the invariants preserved by this PR.

- [ ] Snapshot immutability is preserved
- [ ] Snapshot rows are treated as GC roots
- [ ] Retention model remains explicit and consistent

## Lifecycle Semantics Impact (Required)
This PR changes lifecycle semantics: content may be retained by snapshots even after removal from current state.

Explain reviewer-relevant implications:

- Current-state removal vs snapshot-retained content
- When GC eligibility changes
- Operator/CLI behavior expectations

## Validation Evidence
List the key evidence that supports correctness.

- Unit/package tests:
- Integration tests:
- Adversarial tests:
- Smoke/manual lifecycle gate:

## Reviewer Notes
Call out any risk areas, migration concerns, or rollout notes.
