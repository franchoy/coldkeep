# Coldkeep Release PR Summary Prompt

Prepare a Coldkeep pull request summary for a release-phase or stabilization PR.

Coldkeep is a correctness-first cold storage engine. The summary must focus on invariants, behavior, compatibility, and validation evidence.

## Goal

Produce a clear PR summary that helps reviewers understand:

1. what invariant was protected,
2. what changed,
3. what did not change,
4. what tests prove it,
5. what risks remain.

Avoid generic summaries such as “fixed bugs” or “updated tests.”

## Required summary format

Use this structure:

```markdown
## Summary

<One short paragraph explaining the purpose of the PR.>

## Invariant protected

- <Primary invariant protected by this PR.>
- <Secondary invariant, if any.>

## Scope

### Included

- <Included change 1>
- <Included change 2>

### Explicitly out of scope

- <Out-of-scope item 1>
- <Out-of-scope item 2>

## Behavior changes

- <User-visible or machine-visible behavior change.>
- <Say “No intended user-visible behavior change” if applicable.>

## Compatibility impact

- CLI:
- JSON output:
- SQLite:
- PostgreSQL:
- Packed storage:
- Legacy storage:
- Existing repositories:

## Tests added or updated

- <Test name or test file>
- <Test name or test file>

## Tests run

```bash
<command>
<command>
```
## Remaining risks

## Rules

- Be specific.
- Mention the release phase if applicable.
- Mention the affected invariant, not only the affected file.
- Do not claim a tracker row was closed unless the PR actually updates it.
- Do not claim full release readiness unless the full release gate was run.
- Do not hide behavior changes.
- Do not describe style-only cleanup as correctness hardening.
- Do not call Codacy findings fixed unless the related finding is actually removed, suppressed with rationale, or documented.
- Do not claim PostgreSQL compatibility unless PostgreSQL-relevant tests were run or the change is clearly backend-neutral.
- Do not claim SQLite portability improvement unless the change directly affects local repository portability.

## Good examples of invariant wording

- GC live mode must not delete snapshot-reachable packed blocks.
- Restore must reject stored paths that escape the destination directory.
- JSON output must remain machine-consumable without human side-channel output.
- CLI validation must reject malformed commands before repository initialization.
- Verification must fail closed when packed metadata references missing blocks.
- SQLite-first changes must not remove PostgreSQL compatibility.

## Bad examples

Avoid vague wording such as:

- Improved code quality.
- Cleaned up logic.
- Made behavior better.
- Refactored storage code.
- Fixed edge cases.
- Updated tests.

Replace them with invariant-focused wording.

## Final review

Before returning the summary, check:

- Does the summary distinguish included and out-of-scope work?
- Does it mention tests actually run?
- Does it avoid claiming broader validation than was performed?
- Does it identify behavior and compatibility impact?
- Does it make remaining risk visible?