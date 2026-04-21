# Title

scope: short summary

Use this template fully for release-sensitive or correctness-sensitive PRs.
If a section is not relevant, mark it `N/A` rather than deleting reviewer context silently.

If this is your first small PR and it does not change correctness-sensitive
behavior, a shorter PR description is fine, but keep the validation evidence
section explicit.

## Summary

Describe the release-facing changes introduced in this PR.

- What snapshot, recovery, or operator-surface behavior changed
- How retention or quarantine behavior is affected
- Any CLI, documentation, or test updates relevant for reviewers

## Key Invariants

Confirm and summarize the invariants preserved by this PR.

- [ ] Snapshot immutability is preserved
- [ ] Snapshot rows are treated as GC roots
- [ ] Retention model remains explicit and consistent
- [ ] Recovery remains conservative and metadata-consistent

## Lifecycle Semantics Impact (Required)

This PR may change lifecycle semantics: content may be retained by snapshots even after removal from current state, and recovery may resynchronize quarantined metadata to match on-disk state.

Explain reviewer-relevant implications:

- Current-state removal vs snapshot-retained content
- When GC eligibility changes
- Recovery and quarantine behavior expectations
- Operator/CLI behavior expectations

## Validation Evidence

List the key evidence that supports correctness.

- Unit/package tests:
- Integration tests:
- Adversarial tests:
- Smoke/manual lifecycle gate:
- Release checklist steps executed locally:

## Reviewer Notes

Call out any risk areas, migration concerns, or rollout notes.
