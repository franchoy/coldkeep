# Coldkeep Repository Instructions

Coldkeep is correctness-first. The primary invariant is: never lose user data.

## Active authority

<!-- coldkeep-current-state:start -->
```text
SOURCE_VERSION: 2.0.0-dev.0
RELEASE_FAMILY: v2.0
RELEASE_STATE: development
CURRENT_PHASE: 2.04
CURRENT_AUTHORITY: docs/release/current.json
LATEST_STABLE: 1.13.19
V2_IMPLEMENTATION: ACTIVE
PRODUCT_GATES: INCOMPLETE
```
<!-- coldkeep-current-state:end -->

Use the current charter in `docs/release/v2.0/README.md` and the owner-adopted
`coldkeep_phase3c2_build_2026-11-12.md`. The current unit is Phase 3C2:
bounded installed reads, exact DTOs, snapshot pagination, optional physical
read-index preparation and integrated local-profile action 2.04 qualification.
The [accepted C1 bridge](docs/release/v2.0/phase3c1-acceptance-bridge.md) binds
C1 acceptance and actual hosted outcomes at `06bde38380dd3a42be5c180beb99bf1aa509b8ac`.
Preserve every original predecessor report, worktree, ref and evidence directory.
Owner adoption authorizes sequential local commits, independent review of the
exact outgoing source/evidence, guarded fast-forward of existing `release/v2.0`,
bounded current-status body refresh of draft PR #129 and both actual-head hosted
events. Do not request the same authorization again. PR #129 stays draft.
The C2-G1 follow-up is locally qualified in phase3c2-followup-result.md and
its source-bound qualification. Exact successor review, hosted observation and
final evidence/cleanup remain conditions of acceptance. Original C2 reports
are preserved as hashed predecessors.
C2/3C/local-profile 2.04 are COMPLETE on the integrated local evidence recorded
in the owning result. Exact outgoing review and both actual-head hosted events
remain additional conditions of final C2 acceptance. Implementation stays ACTIVE
and product gates INCOMPLETE.
Ordinary schema18/19/20 writers, reclamation, capture/publication and IMPORT_READY
maintenance/activation remain refused. No merge, tag, release, protection change,
deployment, live switch or action 2.05 implementation is granted.

## Correctness rules

- GC must never delete reachable data.
- Restore must not write outside its intended destination.
- Verify must fail closed on inconsistent catalog or storage state.
- Recovery must not legitimize corrupt mappings.
- Packed and legacy storage behavior must remain aligned.
- Destructive, storage, GC, restore, and verify changes require their applicable
  regression-contract evidence before closure.

## Validation and handoffs

Use the current owning unit's validation profile in `docs/release/v2.0/README.md`,
`docs/release/v2.0/ci-followup-result.md` and `PRE_RELEASE_CHECKLIST.md`.
The Phase 3C2 build mode (`BUILD`) requires fresh affected complete codec routes
with staged real fixtures, the dedicated recovery requirements, integrated
native/browser/Docker proofs, source/identity negative controls and applicable
static checks. The measured source checkpoints and fresh/reused evidence
boundaries are recorded in the follow-up result. Missing, skipped or pending
required evidence is not a pass. Historical publication's docs-only profile must
not substitute for this implementation qualification.
The baseline commands:

- `python3 scripts/validate_release_state.py --state auto --json`
- `python3 scripts/validate_governance.py`
- `python3 -m unittest discover -s scripts -p 'test_*.py' -v`
- `bash scripts/audit_ci_enforcement.sh --local-only`

Use Go 1.26.11 with `GOTOOLCHAIN=local`; language floor remains Go 1.25.
Inspect TestMain and use a dedicated disposable PostgreSQL server and storage.
Never run tests against a user repository/catalog. Preserve PostgreSQL and
SQLite compatibility within their existing qualified bounds.

Future phase/step instructions for this user specify recommended Codex model,
reasoning effort, execution scope, rationale and completion criteria. This is
personal execution guidance, never a paid-model contributor requirement or CI
gate. Do not change account settings or permanent global configuration.

Respect each phase's planning mode (`PLAN`) or build mode (`BUILD`). Stop on
scope expansion, unexpected dependency movement or newly discovered private
security impact. The adopted v2 identity transition is authorized, not
unexpected release drift. Future product gates remain incomplete; this unit
grants no later action authority.
