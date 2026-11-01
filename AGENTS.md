# Coldkeep Repository Instructions

Coldkeep is correctness-first. The primary invariant is: never lose user data.

## Active authority

<!-- coldkeep-current-state:start -->
```text
SOURCE_VERSION: 1.13.19
RELEASE_ROUTE: FINAL_V1_SOURCE_SNAPSHOT_RECOVERY
CURRENT_PHASE: 3_NEXT
TRACKED_PUBLICATION_MATERIAL: FROZEN
V1_13_15_STATE: PUBLISHED_STABLE_IMMUTABLE
V1_13_16_STATE: PUBLIC_TAG_FAILED_CERTIFICATION_NO_GITHUB_RELEASE
V1_13_17_STATE: PUBLISHED_IMMUTABLE_TERMINAL_EFFECTIVE_QUALIFIED
V1_13_18_STATE: PUBLIC_TAG_FAILED_CERTIFICATION_NO_GITHUB_RELEASE
V1_13_19_STATE: ACTIVE_TAG_CERTIFICATION_RECOVERY_CANDIDATE
CK-V11316-007: CLOSED_AT_V1.13.16_SOURCE_SCOPE
FINDINGS_CONFIRMED: 15
FINDINGS_CLOSED: 15/15
V1_X_TECHNICAL_CORRECTNESS: ESTABLISHED
V1_X_FULL_CLOSURE: ESTABLISHED_AT_M_PROTECTED_MAIN_EFFECTIVE_QUALIFIED
V1_13_19_PUBLICATION: NOT_PUBLISHED
V2_IMPLEMENTATION: NOT_STARTED
```
<!-- coldkeep-current-state:end -->

- `v1.13.19` is the active finite tag-certification recovery candidate based
  on exact M18. Phases 0-2 are Complete and Phase 3 independent local
  acceptance is Next. Its only runtime-visible change is application version
  1.13.19. The reviewed fixture repair removes only the active snapshot tag
  from a newly created disposable test clone; production CKRS017 remains
  strict. The exact 34-path M18-relative delta, 13 inherited technical tuples,
  and frozen v1.13.17/v1.13.18 documents are binding.
- `v1.13.18` remains an immutable public annotated tag at A18/M18 whose
  required natural tag CI failed. Passing CodeQL, installations and other
  bounded dimensions do not certify it, and no v1.13.18 GitHub Release exists.
  Preserve the failed run, the shared-workspace 281-test NONPASS, the isolated
  `/tmp` PASS, and the unidentified writer qualification. Do not move or
  recreate A18.
- Bounded Route A direct verification remains accepted on K
  `c69d678349edae59b1ee1d582fc2dbfd372ec133`, tree C2
  `804f61238191e6dd93a3e85b27f291c95e84b14e`, under evidence SHA-256
  `52b0e04990d07f130a85cf6c8dd7ba81eeff834a863e6542df1a298d865b27ed`.
  Its seven-row scope is not rerun or broadened by v1.13.19.
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
- The v1.13.19 scope, 10-phase list, validation checklist, exact 34-path
  recovery allowlist, release state, snapshot reference, predecessor
  disposition, and release gate under `docs/release/v1.13/` are binding current
  authority. All v1.13.18 documents remain frozen historical bytes.
- Respect each phase's `PLAN` or `BUILD` mode and stop at its authorization
  boundary.
- The `immutable-transition-v1` projections and predecessor disposition are
  structural only. They do not certify hosted operations or authorize a push,
  PR, merge, tag, publication, closure, or v2 implementation.
- The candidate must descend from exact M18 through a merge-free range and
  produce the complete 34-row delta. Every other M18 path, all 13 technical
  modes/blobs, all 24 frozen v1.13.17/v1.13.18 documents, compress v1.20.2,
  module sums, persisted fixtures, and product behavior remain unchanged.
  Stop and obtain separate owner authorization before changing another path or
  technical byte. Historical branches/worktrees remain untouched. This local
  phase authorizes no canonical adoption, push, PR, merge, tag, Release,
  workflow action, cleanup, or v2 work.

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
v1.13.19 validation checklist. At minimum, run focused tests for the changed
area before the broader applicable gate. Do not represent unavailable hosted
evidence as passing.

The baseline repository-governance commands are:

- `python3 scripts/validate_release_state.py --state auto --json`
- `python3 scripts/validate_governance.py`
- `python3 -m unittest discover -s scripts -p 'test_*.py' -v`
- `bash scripts/audit_ci_enforcement.sh --local-only`

The frozen v1 release-critical execution contract uses Go 1.26.10 with
`GOTOOLCHAIN=local`; the module language floor remains Go 1.25.

NEXT: the first not-yet-completed lifecycle phase in the summary below.
This phase marker is descriptive and does not authorize work.

    V1_13_15: PUBLISHED_STABLE_HISTORICAL_PRODUCT_BASELINE
    V1_13_15_IS_FINAL_PLANNED_V1_RELEASE: YES
    V1_13_16: PUBLIC_TAG_FAILED_CERTIFICATION_NO_GITHUB_RELEASE
    V1_13_17: PUBLISHED_IMMUTABLE_TERMINAL_EFFECTIVE_QUALIFIED
    V1_13_18: PUBLIC_TAG_FAILED_CERTIFICATION_NO_GITHUB_RELEASE
    V1_13_19: ACTIVE_TAG_CERTIFICATION_RECOVERY_CANDIDATE
    RELEASE_STATE: PRE_RELEASE
    PHASE_3: COMPLETE
    PHASE_4: NOT_STARTED
    PHASE_5: NOT_STARTED
    FINDINGS_CONFIRMED: 15
    FINDINGS_CLOSED: 15/15
    CK_V11316_007: CLOSED
    V1_X_TECHNICAL_CORRECTNESS: ESTABLISHED
    V1_X_FULL_CLOSURE: ESTABLISHED_AT_M_PROTECTED_MAIN_EFFECTIVE_QUALIFIED
    V1_13_19_PUBLICATION: NOT_PUBLISHED
    V2_PLANNING_REVIEW: AUTHORIZED
    V2_IMPLEMENTATION: NOT_STARTED
    V2_IMPLEMENTATION_AUTHORIZATION: REQUIRES_SEPARATE_PLAN

Stop and return to Plan mode on scope expansion, unexpected dependency
movement, release-identity drift, or newly discovered private security impact.
