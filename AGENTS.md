# Coldkeep Repository Instructions

Coldkeep is correctness-first. The primary invariant is: never lose user data.

## Active authority

<!-- coldkeep-current-state:start -->
```text
SOURCE_VERSION: 1.13.18
RELEASE_ROUTE: FINAL_V1_SOURCE_SNAPSHOT
CURRENT_PHASE: 4_NEXT
TRACKED_PUBLICATION_MATERIAL: FROZEN
V1_13_15_STATE: PUBLISHED_STABLE_IMMUTABLE
V1_13_16_STATE: PUBLIC_TAG_FAILED_CERTIFICATION_NO_GITHUB_RELEASE
V1_13_17_STATE: PUBLISHED_IMMUTABLE_TERMINAL_EFFECTIVE_QUALIFIED
V1_13_18_STATE: ACTIVE_FINAL_SOURCE_SNAPSHOT_CANDIDATE
CK-V11316-007: CLOSED_AT_V1.13.16_SOURCE_SCOPE
FINDINGS_CONFIRMED: 15
FINDINGS_CLOSED: 15/15
V1_X_TECHNICAL_CORRECTNESS: ESTABLISHED
V1_X_FULL_CLOSURE: ESTABLISHED_AT_M_PROTECTED_MAIN_EFFECTIVE_QUALIFIED
V1_13_18_PUBLICATION: NOT_PUBLISHED
V2_IMPLEMENTATION: NOT_STARTED
```
<!-- coldkeep-current-state:end -->

- `v1.13.18` is the active final v1.x source-snapshot candidate. Phases 0-3 are
  complete and Phase 4 hosted certification is the next pending phase. The
  current corrective successor still requires fresh independent exact-head
  source review before any operational adoption or remote authorization. The
  tracked state is
  structural and non-authorizing; it claims no hosted certification, merge,
  tag, publication, cleanup, or owner acceptance.
- `v1.13.17` is published at immutable M17/A17 and GitHub Release `400460267`.
  Phase 9M and TS-READBACK-01 remain source-scoped qualified historical facts,
  including their recorded failed-attempt qualifications.
- `v1.13.16` is the immutable failed-publication predecessor: its public
  annotated tag failed required certification, no GitHub Release exists, and
  publication and Phase 19 remain unauthorized.
- `v1.13.15` remains published stable, immutable, and the final planned v1.x
  release. Planned v1 feature and architecture work stays closed and frozen.
- `v1.13.14` is immutable historical release state. Do not edit its release
  evidence or mutate its tag or GitHub release.
- Do not implement v2. V2 planning review is authorized, but implementation
  requires a separate plan and explicit authorization.
- Do not introduce SQLite-first product defaults or perform broad refactors
  without the separately authorized future phase that owns them.
- The v1.13.18 scope, 10-phase list, validation checklist, exact 40-path
  cumulative allowlist, release state, snapshot reference, and release gate under
  `docs/release/v1.13/` are binding current authority.
- Respect each phase's `PLAN` or `BUILD` mode and stop at its authorization
  boundary.
- The `immutable-transition-v1` projections and predecessor disposition are
  structural only. They do not certify hosted operations or authorize a push,
  PR, merge, tag, publication, closure, or v2 implementation.
- The Go 1.26.10 and Agentlinter correction may modify only the exact 33 paths
  named by its adopted manifest, producing the exact 40-path cumulative delta
  from M, and must preserve every `v1.13.17-*` release document byte; stop and
  obtain separate explicit owner authorization before changing any additional
  path. The four historical release branches and all their worktrees remain
  untouched. No push, PR, merge, tag, Release, workflow dispatch, cleanup, or
  v2 work is authorized.

## Correctness rules

- GC must never delete reachable data.
- Restore must not write outside its intended destination.
- Verify must fail closed on inconsistent catalog or storage state.
- Recovery must not legitimize corrupt mappings.
- Packed and legacy storage behavior must remain aligned.
- Destructive, storage, GC, restore, and verify changes require their applicable
  regression-contract evidence before closure.

## Validation

Use the canonical commands in `PRE_RELEASE_CHECKLIST.md` and the active
v1.13.18 validation checklist. At minimum, run focused tests for the changed
area before the broader applicable gate. Do not represent unavailable hosted
evidence as passing.

The baseline repository-governance commands are:

- `python3 scripts/validate_release_state.py --state auto --json`
- `python3 scripts/validate_governance.py`
- `python3 -m unittest discover -s scripts -p 'test_*.py' -v`
- `bash scripts/audit_ci_enforcement.sh --local-only`

The frozen v1 release-critical execution contract uses Go 1.26.10 with
`GOTOOLCHAIN=local`; the module language floor remains Go 1.25.

Within the lifecycle summary below, the phase marker identifies the first
not-yet-completed lifecycle phase; it is descriptive and does not authorize work.

    V1_13_15: PUBLISHED_STABLE_HISTORICAL_PRODUCT_BASELINE
    V1_13_15_IS_FINAL_PLANNED_V1_RELEASE: YES
    V1_13_16: PUBLIC_TAG_FAILED_CERTIFICATION_NO_GITHUB_RELEASE
    V1_13_17: PUBLISHED_IMMUTABLE_TERMINAL_EFFECTIVE_QUALIFIED
    V1_13_18: ACTIVE_FINAL_SOURCE_SNAPSHOT_CANDIDATE
    RELEASE_STATE: DEVELOPMENT
    PHASE_3: COMPLETE
    PHASE_4: NEXT
    FINDINGS_CONFIRMED: 15
    FINDINGS_CLOSED: 15/15
    CK_V11316_007: CLOSED
    V1_X_TECHNICAL_CORRECTNESS: ESTABLISHED
    V1_X_FULL_CLOSURE: ESTABLISHED_AT_M_PROTECTED_MAIN_EFFECTIVE_QUALIFIED
    V1_13_18_PUBLICATION: NOT_PUBLISHED
    V2_PLANNING_REVIEW: AUTHORIZED
    V2_IMPLEMENTATION: NOT_STARTED
    V2_IMPLEMENTATION_AUTHORIZATION: REQUIRES_SEPARATE_PLAN

Stop and return to Plan mode on scope expansion, unexpected dependency
movement, release-identity drift, or newly discovered private security impact.
