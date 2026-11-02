# Coldkeep Repository Instructions

Coldkeep is correctness-first. The primary invariant is: never lose user data.

## Active authority

<!-- coldkeep-current-state:start -->
```text
SOURCE_VERSION: 1.13.19
RELEASE_ROUTE: FINAL_V1_SOURCE_SNAPSHOT_RECOVERY
CURRENT_PHASE: NONE_POST_RELEASE_CLOSED
RELEASE_STATE: POST_RELEASE_CLOSED
TRACKED_PUBLICATION_MATERIAL: FROZEN
V1_13_15_STATE: PUBLISHED_STABLE_IMMUTABLE
V1_13_16_STATE: PUBLIC_TAG_FAILED_CERTIFICATION_NO_GITHUB_RELEASE
V1_13_17_STATE: PUBLISHED_IMMUTABLE_TERMINAL_EFFECTIVE_QUALIFIED
V1_13_18_STATE: PUBLIC_TAG_FAILED_CERTIFICATION_NO_GITHUB_RELEASE
V1_13_19_STATE: PUBLISHED_STABLE_IMMUTABLE_LATEST
CK-V11316-007: CLOSED_AT_V1.13.16_SOURCE_SCOPE
FINDINGS_CONFIRMED: 15
FINDINGS_CLOSED: 15/15
V1_X_TECHNICAL_CORRECTNESS: ESTABLISHED
V1_X_FULL_CLOSURE: COMPLETE_EFFECTIVE
V1_13_19_PUBLICATION: COMPLETE
V1_X: CLOSED_AND_FROZEN
V2_IMPLEMENTATION: NOT_STARTED
```
<!-- coldkeep-current-state:end -->

- `v1.13.19` is published, stable, immutable, and latest. Protected integration
  completed at frozen D19; annotated A19 directly targets D19, and immutable
  source-only GitHub Release `431395975` is the latest Release. The canonical
  closure record binds the accepted natural CI and CodeQL runs; both passed on
  attempt 1. Public archive/module/executable readback passed and independent
  owner acceptance was granted. V1.x is complete and frozen.
- `v1.13.18` remains an immutable public annotated tag at A18/M18 whose
  required natural tag CI failed. Passing CodeQL, installations and other
  bounded dimensions do not certify it, and no v1.13.18 GitHub Release exists.
  Preserve the failed run, the shared-workspace 281-test NONPASS, the isolated
  `/tmp` PASS (successful test result), and the unidentified writer
  qualification. Do not move or
  recreate A18.
- Bounded Route A direct verification remains accepted on K
  `c69d678349edae59b1ee1d582fc2dbfd372ec133`, tree C2
  `804f61238191e6dd93a3e85b27f291c95e84b14e`, under evidence
  SHA-256 (SHA: Secure Hash Algorithm)
  `52b0e04990d07f130a85cf6c8dd7ba81eeff834a863e6542df1a298d865b27ed`.
  Its seven-row scope is not rerun or broadened by v1.13.19.
- `v1.13.17` is published at immutable M17/A17 and GitHub Release `400460267`.
  Phase 9M and TS-READBACK-01 remain source-scoped qualified historical facts,
  including their recorded failed-attempt qualifications.
- `v1.13.16` is the immutable failed-publication predecessor: its public
  annotated tag failed required certification, no GitHub Release exists, and
  publication and Phase 19 remain unauthorized.
- `v1.13.15` remains published, immutable, and the final planned v1.x product
  baseline. Planned v1 feature and architecture work stays closed and frozen.
- `v1.13.14` is immutable historical release state. Do not edit its release
  evidence or mutate its tag or GitHub release.
- Do not implement v2. V2 planning review is authorized, but implementation
  requires a separate plan and explicit authorization.
- Do not introduce SQLite-first product defaults or perform broad refactors
  without the separately authorized future phase that owns them.
- The v1.13.19 post-publication closure record is current lifecycle authority.
  The scope, phase list, validation checklist, recovery allowlist, release
  state, snapshot reference, predecessor disposition, and release gate under
  `docs/release/v1.13/` remain immutable historical evidence of the completed
  train. All v1.13.17/v1.13.18 documents remain frozen historical bytes.
- No v1 product/runtime phase remains. Future critical v1 maintenance, tag or
  Release mutation, workflow intervention, and all v2 implementation require
  separate explicit authority. Preserve all product/dependency bytes, formats,
  schemas, persisted fixtures, and public behavior unless that later authority
  expressly changes them.
- Planning mode (`PLAN`) and build mode (`BUILD`) are lifecycle modes. Respect
  each phase's `PLAN` or `BUILD` mode in any separately authorized future
  maintenance or v2 plan; this closure creates no active phase.

## Correctness rules

- GC must never delete reachable data.
- Restore must not write outside its intended destination.
- Verify must fail closed on inconsistent catalog or storage state.
- Recovery must not legitimize corrupt mappings.
- Packed and legacy storage behavior must remain aligned.
- Destructive, storage, GC, restore, and verify changes require their applicable
  regression-contract evidence before closure.

## Validation

Use the canonical commands in `PRE_RELEASE_CHECKLIST.md` and the applicable
post-release governance contract. At minimum, run focused tests for the changed
area before the broader applicable gate. Do not represent unavailable hosted
evidence as passing.

The baseline repository-governance commands are:

- `python3 scripts/validate_release_state.py --state auto --json`
- `python3 scripts/validate_governance.py`
- `python3 -m unittest discover -s scripts -p 'test_*.py' -v`
- `bash scripts/audit_ci_enforcement.sh --local-only`

The frozen v1 release-critical execution contract uses Go 1.26.10 with
`GOTOOLCHAIN=local`; the module language floor remains Go 1.25.

No v1.x lifecycle phase remains. `CURRENT_PHASE: NONE_POST_RELEASE_CLOSED`
does not authorize v2 or new v1 product work.

Current v1.13.19 lifecycle summary

    V1_13_15: PUBLISHED_STABLE_HISTORICAL_PRODUCT_BASELINE
    V1_13_15_IS_FINAL_PLANNED_V1_RELEASE: YES
    V1_13_16: PUBLIC_TAG_FAILED_CERTIFICATION_NO_GITHUB_RELEASE
    V1_13_17: PUBLISHED_IMMUTABLE_TERMINAL_EFFECTIVE_QUALIFIED
    V1_13_18: PUBLIC_TAG_FAILED_CERTIFICATION_NO_GITHUB_RELEASE
    V1_13_19: PUBLISHED_STABLE_IMMUTABLE_LATEST
    RELEASE_STATE: POST_RELEASE_CLOSED
    CURRENT_PHASE: NONE_POST_RELEASE_CLOSED
    FINDINGS_CONFIRMED: 15
    FINDINGS_CLOSED: 15/15
    CK_V11316_007: CLOSED
    V1_X_TECHNICAL_CORRECTNESS: ESTABLISHED
    V1_X_FULL_CLOSURE: COMPLETE_EFFECTIVE
    V1_13_19_PUBLICATION: COMPLETE
    V1_X: CLOSED_AND_FROZEN
    V2_PLANNING_REVIEW: AUTHORIZED
    V2_IMPLEMENTATION: NOT_STARTED
    V2_IMPLEMENTATION_AUTHORIZATION: REQUIRES_SEPARATE_PLAN

Stop and return to Plan mode on scope expansion, unexpected dependency
movement, release-identity drift, or newly discovered private security impact.
