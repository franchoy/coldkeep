# v1.13 - Engine Stabilization Baseline & Contract Inventory

## Purpose

v1.13 starts after v1.12.3 completion. Its purpose is to stabilize the engine contract surface,
stabilize the catalog contract surface, harden dependency direction, clarify engine-level
correctness invariant ownership, prepare for SQLite-first local portability, and preserve
PostgreSQL compatibility while the project moves toward future v2.x local-first productization.

## Current roadmap authority

v1.x completed the frozen Engine/Catalog correctness work and bounded
SQLite/PostgreSQL backend compatibility. SQLite-first repository-local default
product behavior belongs to v2.x. Older v1.x documents that implied the
default-product switch must ship before v1 closure are historical and
superseded on that timing point; their completed-release facts remain intact.

v1.13.0 is the baseline release for that work. It is documentation and inventory only, and it
must not change runtime behavior.

## v1.13 Goals

- Engine contract stabilization.
- Catalog contract stabilization.
- Dependency-direction hardening.
- Engine-level invariant ownership clarification.
- SQLite-first local portability preparation.
- PostgreSQL compatibility preservation.

## v1.13.0 Baseline Policy

- v1.13.0 is baseline and inventory only.
- v1.13.0 must not change runtime behavior.
- v1.13.0 does not implement v2.x work.
- v1.13.0 prepares the project for v2.x local-first productization without implementing daemon,
  API, UI, protocol, or product-surface changes.

## Release Boundaries

Do not change during v1.13.0:

- Go code.
- Tests.
- Schema or migrations.
- CLI behavior.
- JSON output.
- Exit codes.
- Storage format.
- Repository format.
- Default backend behavior.
- Engine routing.
- Catalog implementation.
- CI behavior.
- Scripts.

Do not implement during v1.13.0:

- New engine operations.
- New catalog methods.
- SQLite default switch.
- PostgreSQL removal.
- Daemon, API, UI, or v2 work.
- Broad refactors.

## Release Artifacts

The v1.13.0 baseline is defined by:

- `docs/release/v1.13/v1.13.0-scope.md`
- `docs/release/v1.13/v1.13.0-phase-list.md`
- `docs/release/v1.13/v1.13.0-validation-checklist.md`

All v1.13.0 phases stay on `release/v1.13.0` until the full release gate is green.

## Current Release State

### Current Phase 9 certified state

v1.13.16 remains the active exceptional critical-maintenance source train.
Phase 8 implementation and certification are complete at executable/evidence
head `f884ecba169f8eee908e9c8078d01d056cb1a0bd`. Its linear authority chain is
`1967df264919a908680db4bc2a3dbf1fa5f673da`,
`800cfc81ca453a9aa800da57819da0a58118b3e2`,
`55d976177d0a9e25446726ad018d52a0b180293a`, and `f884ecba...`.
The implementation retains the complete 8/8 red/control baseline, the exact
five production and nine test paths, the external `graph_test` split, and an
acyclic production graph.

Branch PostgreSQL certification passed 12/12 and detached PostgreSQL
certification passed 12/12. The detached R7 profiles were the original
authorized serialized execution completing after a premature terminal report;
they were not a retry, and the R8 replacement execution was not performed.
After final evidence capture, the Phase 8-owned PostgreSQL, PyYAML, detached
worktree, and covered cache/temp resources were removed. No post-hosted commit
occurred before this separately authorized current-authority reconciliation.

Fresh push-triggered CI `33802189644` attempt 1 passed 36/36 and CodeQL
`33802189741` attempt 1 passed 4/4 at `f884ecba...`. All 40 exact-head checks
completed successfully, the critical-coverage artifact was produced, direct
repository and release-branch open CodeQL alert enumeration each returned
zero, and direct Codacy truth was `NOT_EXPOSED`.

Phase 9P externally froze the simulation duplicate-reuse and stored-path
contract. Phase 9 retained authority
`c31cbd45236c3ce5e569b3b34dd621ba5b6e0f3d`, simulation fix
`ecede474f3df0027c198dff165addcf1065519cc`, and stored-path fix
`99017d1ae84e1ac8bac51c4c7b1f06a7f92d93bc`. The initial Build passed the
functional contract and stopped at exact golangci-lint 2.9.0 on only two
obsolete private wrappers. Append-only Phase 9R1 recovery deleted only those
proven-unused wrappers in `7b7f89afe43783b0e61ce10e240cd699d23252c8`;
no history rewrite or product-behavior change occurred. Candidate evidence is
`4e65ea22395c0839dc4ce6d95fd48b617a32ffe4`.

Branch and detached certification passed under Go 1.26.7, including four
simulation codec/compression profiles, SQLite/PostgreSQL graph parity,
PostgreSQL real Store reuse in `off`, `suspicious`, and `always`, PostgreSQL
stored-path preservation, affected ordinary/race and Phase 6/8/3 preservation,
full Go/race/vet, exact lint with zero issues, three independent
critical-coverage reports, and release/governance/Python/CI gates. Fresh
push-triggered CI `33919209386` passed 36/36 and CodeQL `33919209398` passed
4/4 at the candidate head. All 40 exact-head checks succeeded, direct
repository and release-branch open alerts were zero, the critical-coverage
artifact was present, direct Codacy truth was `NOT_EXPOSED`, and no automatic
retry occurred.

CK-V11316-005 and CK-V11316-006 are closed certified under the
[Phase 9 contract](v1.13.16-phase9-simulation-reuse-and-stored-path-contract.md).
CK-V11316-001 through CK-V11316-006 and CK-V11316-008 through CK-V11316-011
are closed certified; CK-V11316-007 remains open. Findings are 13 confirmed
and 10/13 closed. Phase 9 is Complete and Phase 10 remains Next. Phase 10R1
successfully corrected CK-V11316-012's refcount fixture and proved the final
13/13 plain and AES concurrency selectors with zero skips, then the fresh
complete matrix stopped because the required G1 unrelated-container proof
self-skipped. [Phase 10R2 recovery authority](v1.13.16-phase10-integrated-local-validation.md)
creates CK-V11316-013 as `OPEN_RECOVERY_AUTHORIZED` and authorizes only the
exact G1 CLI environment/non-skippable fixture correction. Product packing,
container, storage, and restore behavior remain unchanged. Complete Phase 10
certification must restart again from the committed R2 correction; no push or
Phase 11 execution is authorized. v1.x technical correctness closure remains
withheld.

### Historical Phase 6 chronology

The chronology below preserves the pre-closure recovery states and is
superseded where its pending wording conflicts with the current Phase 6R10
authority above.

v1.13.16 — Snapshot Retention Integrity, Observability Truth, and Final v1.x
Closure is the active exceptional critical-maintenance source train. Phases 0
through 5 are Complete. Phase 6 is Next and locally certified pending exact-
head hosted gates; Phase 6R2 is the locally certified snapshot-only lifecycle
correction, and Phase 6R3 is locally certified pending exact-head hosted
recertification. Phase 6R7 passed corrected detached certification, including
all 12 retained PostgreSQL selectors, and its accepted head was pushed once.
[Phase 6R9](v1.13.16-phase6r9-critical-coverage-throughput-recovery.md)
completed its one-file throughput isolation and passed branch-worktree and
clean committed-tree certification. Detached certification stopped only in
the unchanged benchmark signal-cleanup regression. [Phase 6R9R2](v1.13.16-phase6r9r2-benchmark-signal-test-recovery.md)
implemented the bounded one-file test-harness recovery for CK-V11316-011 and
passed the complete branch-worktree matrix. Clean committed-tree, detached,
and hosted certification remain pending.
Phase 7 is not started. Eleven findings are confirmed and one is closed at
this candidate state.
Phase 4 completed exact-head certification at
`6c11bc6245b301873c598fe784a4df3cbc5ba809`: CI `33303282081` attempt 2 passed
36/36 jobs, reused CodeQL `33303282125` passed 4/4, and open alerts remain zero.
CK-V11316-001 and CK-V11316-008 are implemented and locally certified pending
exact-head hosted closure. Phase 5 product
commit `cc4186b1c095bae06ee92e12048c1262d53e843d` implements fail-closed
selector accounting and atomic rollback and has passed clean-tree, SQLite, and
PostgreSQL plain/AES-GCM certification. Evidence commit
`4ba598fb57a3594d094ed3297c3fb5549dbc401f` passed exact-head CI run
`33307923059` with 36/36 jobs including Required Gate and CodeQL run
`33307923075` with 4/4 jobs and zero open alerts. CK-V11316-002 is closed
certified. Technical correctness closure is withheld.

Phase 6R2 sealed product
`e8cc9b3650ab9548ce46e7df18e9c6f3483e2574` and permanent test
`854a3187adc66dcc04d7e95f41f8925cdcd608bf` passed clean detached full-Go,
vet, SQLite, PostgreSQL plain/AES-GCM, race, release-state, governance, Python,
and CI-enforcement certification. Exact-head CI, Required Gate, CodeQL,
zero-open-alert confirmation, and Codacy truth remain pending.

Exact-head CI `33331967865` at that evidence commit passed quality,
cross-platform validation, critical coverage, legacy compatibility, benchmark
integrity, and timing advisories, while both PostgreSQL correctness profiles
failed on the same two stale migration-compatibility expectations. CodeQL
`33331967855` passed 4/4 with zero open alerts. Phase 6R3 authority
`a72602ed988978476bd500c130d428a8c4471cfe` authorized only
`tests/integration/v18_migration_compatibility_integration_test.go` to reconcile
stored-path unlink and current/pin/snapshot root-union assertions. Test commit
`81df6a497a2ae07ecba177fb645e1b4bf556fa15` completed that reconciliation
with zero product runtime changes and passed exact-affected and complete
PostgreSQL plain/AES-GCM short/race profiles plus the clean detached full local
gate. Exact-head hosted certification remains pending; Phase 7 is not
authorized.

Phase 6R5 is the authorized final gate reconciliation after exact-head CI
`33334609160` at `ce7271a068aea1656d7239c5e282752928ed2e31` exposed a stale
stored-path lifecycle in smoke and stdout batch-item parsing plus incomplete
conservative-state proof in G15. CodeQL `33334609155` passed 4/4 with zero open
alerts. No product runtime defect is established: only `scripts/smoke.sh` and
the G14–G17 adversarial test may change. Phase 6 is withheld pending this
reconciliation and exact-head hosted certification; Phase 7 is not authorized.

Phase 6R5 authority `2870ef1e8504e8ac42640d41c05fff6b8fd4b008` and
test `d7deb4097d681751f7373ac7f11ef6bb8bb18790` completed the bounded
reconciliation. Plain/AES-GCM smoke, focused and complete adversarial,
PostgreSQL correctness, migration compatibility, race, and the detached full
local gate passed with zero product runtime changes. Phase 6 is now a candidate
pending exact-head hosted certification; the prior finding state was `1/8`, and
Phase 7 is not authorized.

Final CI `33365683050` at `bd45a0e611d3dacb726490c3beb5fac5cd598f34`
then failed the plain quality gate because
`TestVerifySystemDeepPackedSQLiteSingleConnection` applied its 250 ms
verification timeout to packed-fixture Store setup. The same boundary had
failed under coverage in CI `33303282081` attempt 1. Phase 6R6 classified the
repetition as `COMBINATION`: primarily fixture-isolation drift, with a
contributing test-timeout-policy defect; no product or CI-infrastructure defect
is established. [Phase 6R7](v1.13.16-phase6r7-single-connection-verify-test-recovery.md)
authorized only `internal/verify/verify_system_single_connection_test.go` to
separate default-timeout fixture construction from successful and deliberately
blocked 250 ms verification children. Authority
`ce815419c0e72d341da6892eec01662de734292f` and test
`7399b38b22c2f19fe0e12e1c0a8ec06cadc7ebce` completed that one-file
recovery. The 100/50/50 targeted repetitions, exact local `golangci-lint`
v2.9.0, full Go/race/vet and 215 Python tests passed. An isolated PostgreSQL
16.15 service pinned by digest and bound only to dynamic localhost port 32768
then passed the complete plain/AES-GCM Phase 6R5 preservation matrix; the
existing port-5432 service was unchanged. CK-V11316-009 is implemented pending
exact-head hosted certification, findings remain `1/9` closed, CI
`33365683050` was not retried, Phase 6 remains withheld, and Phase 7 remains
unauthorized.

Evidence commit `96f19cbd333b0120ab1e18a65c6309eb6d9a2210` was created after
the branch-worktree and isolated PostgreSQL matrices passed but before detached
certification completed. Its earlier locally-certified wording is superseded:
the later detached attempt passed lint, targeted ordinary/race/atomic coverage,
and full `internal/verify` ordinary/race, then stopped in
`TestEngineDependencyDirection`. Phase 6R7R3 proved a linked-worktree VCS
stamping environment defect; `GOFLAGS=-buildvcs=false` is authorized only as a
source-neutral detached validation policy. Corrected detached execution passed,
including all 12 retained PostgreSQL selectors, and head
`f58be125eee09c6c1cfb76b911ada18e7f232a69` was pushed exactly once. CI
`33407340364` passed 35 of 36 jobs, failing only critical coverage; CodeQL
`33407340517` passed 4 of 4 with zero alerts. CK-V11316-010 classifies that
failure as primary gate composition, secondary measurement stability, with
coverage instrumentation contributing. It is not a product or CI
infrastructure regression. Phase 6R9 authority `6be80e391...` and test
`9439c433...` completed the one-file implementation. Ordinary 20x, race 10x,
atomic 10x, full Phase 1 coverage, three critical reports, full repository
ordinary/race, exact lint, and 215 Python tests passed. CK-V11316-010 is
implemented pending exact-head hosted certification; committed and detached
certification remain pending.

The earlier Phase 1 pointer, "Phase 2 is Next", the Phase 4 Complete / Phase 5
Next authorization pointer, and the Phase 5 pre-implementation RED pointer are
historical and superseded by the current Phase 6 authority above.

v1.13.15 remains the published stable immutable historical product baseline,
release `378768097`. Annotated tag object
`38b48ef60e1cd8cc9a6966bfaa1fda074fdf6f12` peels to immutable product
baseline `6a2417e8189631b018779c2fd24fc559ed761f3f`. Tag CI `33210164827`,
tag-native installation on Linux, macOS, and Windows, CodeQL `33210164772`,
zero open CodeQL alerts, and the release attestation passed.

The canonical publication body remains `v1.13.15-release-body.md`, exactly
1,815 bytes at SHA-256
`477796fc1c44151ddc77825559c48876c49ab742540586a190abc2c878eea357`.
The release was published at `2026-08-28T21:17:21Z` with zero custom assets and
a verified immutable-release attestation. Its Phases 0–10 and
`post-release-closed` state remain historical release truth.

v1.13.14 remains unchanged immutable published historical state. Planned v1
feature and architecture work remains closed and frozen. V2 planning review is
authorized; v2 implementation has not started and requires a separate plan.

## Historical v1.13.14 current-state narrative

`v1.13.13 — Final v1.x and v2 Handoff Gate` is published and operationally
closed. `v1.13.14 — Final v1.x Correctness Remediation and Closure
Certification` is published and operationally closed. Its annotated tag object
`a996b25b562de69749f41c3af56626aeb5d44e33` peels to certified final main
`caac44d459609f89f2c971cb7b07a8678bd52d2c`; all Phases 0–26 are complete
subject to the Phase 26 self-effective final-main and cleanup proof.
Phase 4 closed `CK-V1-AUD-001` with execution-derived verification results and
Phase 5 closed `CK-V1-AUD-002` with complete caller-context and cancellation
ownership. Phase 6 closed `CK-V1-AUD-007` by propagating required single-file
Store finalization failures without changing writer-owned quarantine.
Phase 7 closed `CK-V1-AUD-008` with joined rollback cleanup failures, retained
poisoned container identity, durable quarantine ordering, and fail-closed
append refusal. Phase 8 closed `CK-V1-AUD-006` and `CK-V1-AUD-009` by
preserving snapshot restore overwrite and giving Show, Diff, and Restore one
private Engine selector normalizer. Phase 9 closed `CK-V1-AUD-010` and
`CK-V1-AUD-012` by applying SnapshotDiff Limit after query/change filtering
with truthful raw/matched/summary counts and carrying persisted membership
through Catalog snapshot projections into `SnapshotMeta.FileCount`. At the
Phase 9 boundary, `8/17` findings were closed and all 4 process gates remained
open. Phase 10 froze the
GC execution scheduler, active-container accounting, physical-byte,
partial-result, filesystem-failure, and overflow contracts without
implementing or closing `CK-V1-AUD-003` or `CK-V1-AUD-004`. Phase 11 then
implemented the frozen bounded scheduler and accurate physical-byte contract,
including active accounting, surfaced filesystem/overflow errors, and truthful
partial results, and closed those two findings. `10/17` findings are closed and
all 4 process gates remain open at the Phase 11 boundary. Phase 12 then closed
`CK-V1-AUD-011`, `CK-V1-AUD-013`, and `CK-V1-AUD-014` by applying one private
Engine ContainerDir default resolver, correcting the Store codec contract text
to the unchanged runtime precedence, and sharing Store's exact packed-block
target resolver with silent Stats consumption. `13/17` findings are closed and
all 4 process gates remain open at the Phase 12 boundary. Phase 13 then closed
`CK-V1-AUD-015`, `CK-V1-AUD-016`, and `CK-V1-AUD-017` by aligning the root
README and top-level help with the existing search syntax, five-code exit and
JSON automation contract, and init compression behavior. `16/17` findings are
closed. Phase 14 completed all four process gates with isolated CLI test
storage, tracked-source evidence validation, an exact-SHA benchmark-evidence
lifecycle, and branch-relative release linearity while preserving the Phase 0
non-fast-forward ruleset. `CK-V1-AUD-005` remains partial. Phase 15 then reconciled
all 21 frozen rows: 16 closed findings retain integrated regression PASS, all
four process gates retain integrated PASS, and AUD-005 remains lifecycle-
deferred to Phases 18/26 rather than falsely closed. Phase 16 then proved all
10 assigned rows across the exact applicable SQLite/PostgreSQL, plain/AES-GCM,
none/zstd, and legacy/packed/mixed matrix: 20/20 backend, 16/16 codec, 16/16
compression, and 21/21 layout cells passed without product or test changes.
Phase 17 then proved 17/17 deep rows across cancellation/deadline, race,
deterministic fault, adversarial, and exact-head native execution: 10/10
cancellation, 17/17 race, 12/12 fault, 17/17 adversarial, and 15/15 native
cells passed without product or test changes. Phase 18 then reconciled every
current candidate-truth surface without changing product, test, script, or
workflow code. Sixteen of 17 findings remain closed, all 4 process gates and
all 21 integrated rows remain complete, and the compatibility and runtime-
stress matrices remain complete. `CK-V1-AUD-005` remains lifecycle-partial:
its Phase 18 candidate-truth leg is complete and its final publication-truth
leg remains pending Phase 26. Phase 19 approved candidate-side technical
corrective completion for entry into the final release lifecycle without
certifying release readiness, publication, or final closure. Phase 20 then
recorded the final corrective handoff and froze the 1,397-entry candidate core
without changing it. Phase 21 independently passed the complete local exact-
candidate gate, and Phase 22 passed the exact-head hosted CI, CodeQL, native-
platform, benchmark, security, and live governance gates for that candidate.
PR #112 subsequently exposed a GATE-004 PR execution-context defect. Phase 23R
repairs the audit to validate release history against the authoritative
same-repository PR head while leaving product validation on GitHub's synthetic
merge, and refreezes the 1,397-entry core at SHA-256
`58d2909d1fe490e3ce246c48b42d02ff8ea256877fec05f1c8c2972f83248023`.
The refreeze passed real PR-context acceptance, the repeated independent Phase
21 local gate, and repeated Phase 22 hosted certification. GATE-004 is reclosed
at `4/4`. PR #112 merged immutable Phase23R candidate
`eef722121aad571a6b2394bde67ea3f08ab768e4` normally as
`7962275ffe24f9ca719d5ced543f71f89a1286f4`. Phase 24 conditionally completes
the recognized Phase 0–24 topology through documentation-only reconciliation;
its authority depends on the protected reconciliation merge and exact
final-main proof. No v1.13.14 tag or stable GitHub release exists. Phase 25
publication and Phase 26 final reconciliation remain separately authorized and
unstarted. v1.x normative scope and Phase23R corrective recertification are
complete after 17 confirmed findings and 4 process gates. The
[Phase 1 scope freeze](v1.13.14-phase1-confirmed-finding-rejection-and-scope-freeze.md)
is the current classification authority, and the
[Phase 2 design freeze](v1.13.14-phase2-remediation-design-and-regression-contract-freeze.md)
is the remediation and regression-contract authority, and the
[Phase 3 seam record](v1.13.14-phase3-deterministic-remediation-test-seams.md)
is the test-infrastructure readiness authority, and the
[Phase 4 closure record](v1.13.14-phase4-execution-derived-verification-results.md)
is the `CK-V1-AUD-001` implementation and closure authority, and the
[Phase 5 closure record](v1.13.14-phase5-complete-caller-context-and-cancellation-ownership.md)
is the `CK-V1-AUD-002` implementation and closure authority, and the
[Phase 6 closure record](v1.13.14-phase6-single-file-store-finalization-error-propagation.md)
is the `CK-V1-AUD-007` implementation and closure authority, and the
[Phase 7 closure record](v1.13.14-phase7-rollback-close-failure-and-quarantine-safety.md)
is the `CK-V1-AUD-008` implementation and closure authority, and the
[Phase 8 closure record](v1.13.14-phase8-snapshot-restore-overwrite-and-shared-selector-contract.md)
is the `CK-V1-AUD-006` and `CK-V1-AUD-009` implementation and closure
authority, and the
[Phase 9 closure record](v1.13.14-phase9-snapshot-diff-limit-and-snapshot-metadata-file-counts.md)
is the `CK-V1-AUD-010` and `CK-V1-AUD-012` implementation and closure
authority, and the
[Phase 10 design-freeze record](v1.13.14-phase10-gc-execution-and-byte-accounting-design-freeze.md)
is the binding `CK-V1-AUD-003` and `CK-V1-AUD-004` execution-design authority,
and the
[Phase 11 closure record](v1.13.14-phase11-gc-workers-and-accurate-reclaimed-bytes.md)
is their implementation and closure authority, and the
[Phase 12 closure record](v1.13.14-phase12-configuration-defaults-and-observability-parity.md)
is the `CK-V1-AUD-011`, `CK-V1-AUD-013`, and `CK-V1-AUD-014` implementation
and closure authority, and the
[Phase 13 closure record](v1.13.14-phase13-cli-help-and-automation-contracts.md)
is the `CK-V1-AUD-015`, `CK-V1-AUD-016`, and `CK-V1-AUD-017` implementation
and closure authority, and the
[Phase 14 release-gate hardening record](v1.13.14-phase14-release-gate-reproducibility-hardening.md)
is the `CK-V1-GATE-001` through `CK-V1-GATE-004` closure authority, and the
[Phase 15 integrated closure record](v1.13.14-phase15-finding-by-finding-integrated-regression-closure.md)
and [21-row matrix](v1.13.14-integrated-regression-closure-matrix.md) are the
current finding-by-finding integration authority, and the
[Phase 16 compatibility proof](v1.13.14-phase16-backend-codec-compression-storage-layout-proof.md)
and [compatibility matrix](v1.13.14-compatibility-proof-matrix.md) are the
backend, codec, compression, and storage-layout authority, and the
[Phase 17 runtime-stress proof](v1.13.14-phase17-cancellation-race-fault-adversarial-cross-platform-proof.md)
and [adversarial runtime matrix](v1.13.14-adversarial-runtime-proof-matrix.md)
are the cancellation, race, fault, adversarial, and native-platform authority,
and the
[Phase 18 current-candidate reconciliation](v1.13.14-phase18-current-documentation-and-release-state-reconciliation.md)
is the current candidate-truth authority, and the
[Phase 19 formal corrective closure decision](v1.13.14-phase19-formal-final-v1x-corrective-closure-decision.md)
is the authority for entry into the final release lifecycle.
The [Phase 20 handoff record](v1.13.14-phase20-final-closure-handoff-and-immutable-candidate-freeze.md)
and [candidate-core manifest](v1.13.14-immutable-candidate-core-manifest.txt)
are the final corrective handoff and immutable candidate-core authorities.
The [Phase 21 local gate](v1.13.14-phase21-independent-full-local-exact-candidate-release-gate.md)
and [Phase 22 hosted gate](v1.13.14-phase22-hosted-exact-head-security-quality-and-governance-gate.md)
are the combined local and hosted release-readiness authorities. The
[Phase 26 final reconciliation](v1.13.14-phase26-post-publication-truth-reconciliation-and-final-cleanup.md)
is the current publication, AUD-005 closure, `17/17` finding, and final v1.x
operational-closure authority. V2 implementation has not started.
The v1.13.13 publication authority is the
[Phase 16 record](v1.13.13-phase16-tag-ci-stable-publication-and-cleanup.md).


- `v1.13.9` is complete, merged, tagged, published, and operationally closed.
- Phase 24 is complete; PR #104 merged, `v1.13.9` was tagged and released,
  and release/tag CI passed.
- `release/v1.13.9` was deleted locally and remotely. No Phase 25 was
  required, and no mandatory v1.x runtime remediation remained.
- `v1.13.10 — v1.x Closure Integrity and CI Runtime Hygiene` is released and
  operationally closed. It is a valid closure-integrity and CI-runtime-hygiene
  baseline, not the final v1.x release.
- A post-release roadmap-to-code audit superseded the narrower final-v1.x
  conclusion and restored v1.13.11–v1.13.13 for remaining must-before-v2 work.
- `v1.13.11 — Safety and Backend Compatibility Gate Closure` is complete,
  merged as `507859daccf25594142c61e5ab8209a751fb579a`, annotated-tagged, and
  published. Phases 0–20 are complete. Phase 12 implements the
  repository-wide exclusive fail-fast Lease and proves native runtime plus the
  production Coordinator lifecycle on Linux, macOS, and Windows. Phase 13
  preserves strengthened G6 integrity coverage and proves deterministic Linux
  independent-process contention, killed-holder release with immediate
  reacquisition, real live-GC cross-process exclusion, and dedicated
  PostgreSQL advisory-session ownership. Its platform and same-host proof
  limits are recorded in
  [v1.13.11-phase13-closure.md](v1.13.11-phase13-closure.md). Phase 14 now
  validates outer ranges before allocation/read, enforces supported-header,
  catalog-maximum, and physical-size consistency, and preserves persisted
  maxima across packed reads and recovery without changing format bytes. Its
  evidence is recorded in
  [v1.13.11-phase14-container-range-header-consistency.md](v1.13.11-phase14-container-range-header-consistency.md).
  Phase 15 now validates exact decompression expectations before allocation,
  caps complete CKBL output and zstd decoder memory/window resources at 4 MiB,
  and applies the same intrinsic contract to identity and zstd codecs. Restore,
  system Verify, and Store semantic reuse inherit the shared bounded path. Its
  evidence is recorded in
  [v1.13.11-phase15-bounded-decompression.md](v1.13.11-phase15-bounded-decompression.md).
  Phase 16 now preserves exact integer tokens throughout the stable v1.7
  stats, inspect, and simulate-GC JSON envelope path without changing JSON
  number types, schemas, APIs, storage, coordination, or error behavior. Its
  evidence is recorded in
  [v1.13.11-phase16-json-integer-fidelity.md](v1.13.11-phase16-json-integer-fidelity.md).
  Phase 17 now inventories all 70 non-DDL production mutations and hardens the
  20 required-row gaps without changing valid zero-row cleanup, recovery, CAS,
  upsert, bulk, or GC semantics. SQLite/PostgreSQL parity and rollback are
  proven, including no physical GC deletion after a missed metadata delete.
  Its evidence is recorded in
  [v1.13.11-phase17-fail-closed-sql-mutations.md](v1.13.11-phase17-fail-closed-sql-mutations.md).
  Phase 18 now makes selected existing PostgreSQL mutation-cardinality,
  storage/recovery, and Linux coordination execution proof fail closed in
  required CI without changing product semantics or job topology. Its evidence
  is recorded in
  [v1.13.11-phase18-required-backend-coordination-ci.md](v1.13.11-phase18-required-backend-coordination-ci.md).
  Phase 19 reconciles the validation matrix, backend claim matrix, reusable
  release checklist, active trackers, Phase 18 closure chronology, proof
  boundaries, deferred items, and evidence links. Its documentation-only
  evidence is recorded in
  [v1.13.11-phase19-validation-evidence-reconciliation.md](v1.13.11-phase19-validation-evidence-reconciliation.md).
  Phase 20 freezes the prospective exact-head gate contract and pre-release
  state. The immutable commit containing that record must pass candidate-head
  CI, Required Gate, CodeQL, and the complete clean local Profile A gate before
  one pull request to `main` is authorized. BKC-016 remains
  `Deferred — documented`. Phase 12 closure remains recorded in
  [v1.13.11-phase12-closure.md](v1.13.11-phase12-closure.md). The diagnostic
  benchmark-gate bootstrap remains recorded in
  [v1.13.11-phase11-benchmark-gate-integrity-remediation.md](v1.13.11-phase11-benchmark-gate-integrity-remediation.md).
  Phase 10 implementation `ad82c959` passed exact-head CI run `30148670910`,
  including all five PostgreSQL events in plain job `89655223183` and required
  gate `89656972706`. A first benchmark variance was resolved by successful
  same-head rerun `89656813012` without any benchmark accommodation. BKC-003
  and BKC-015 are backend-specific — proven within their documented bounds.
  Phase 9 exact-head
  CI run `30114444798` at `848e579b` proved scoped active, uncontended Engine
  mutation and GC dry-run parity, including all five required PostgreSQL events
  in plain job `89551564893` and required-gate job `89555865026`. Phase 8 exact-head CI run `30109561344` at `bcae3576` proved
  its scoped snapshot selector and tree-presentation contracts, including both
  required PostgreSQL selector events and the aggregate required gate. Phase 7 exact-head CI run `29993172886` at `313d0069`
  proved the scoped engine read-side contracts across SQLite and PostgreSQL,
  including the SQLite deep-verification single-connection correction. Phase 6
  exact-head CI run `29983479388` proved the scoped
  implemented catalog contracts across SQLite and PostgreSQL, including
  deterministic snapshot ordering. Selected schema/bootstrap/migration
  contracts and the G6 fail-closed remediation also have exact-head CI
  evidence, while broad backend parity remains intentionally unclaimed.
  Its final exact-head evidence, PR #106 merge, main validation, annotated tag,
  tag validation, stable publication, and release-branch deletion are
  reconciled as completed release operations. Its canonical trackers are
  `v1.13.11-phase0-post-release-closure-correction-and-baseline.md`,
  `v1.13.11-scope.md`, `v1.13.11-phase-list.md`,
  `v1.13.11-validation-checklist.md`, and `v1.13.11-release-gate.md`.
- `v1.13.12 — Engine and Catalog Completion` is released and operationally
  closed. Final candidate `33aa1a563b1e6f7b09a86326c6bbd06d7b106e58`
  completed the catalog planning APIs, production engine ownership, neutral
  contracts, and enforceable thin-CLI boundary without changing storage
  semantics. PR #107 merged it as
  `7505d7000faef452caeb4c01784f0510960e7240`; PR #108 produced final `main`
  `fd396cd0c8cf43662881211b8e6b2877eb9a8010`. Annotated tag `v1.13.12`, tag
  validation, stable publication, and both temporary-branch deletions are
  complete. Historical Phase 0–21 evidence remains frozen.
- `v1.13.13 — Final v1.x and v2 Handoff Gate` is published and operationally
  closed. Its annotated tag object
  `5d534a6b4a9d44ab7303d9b76575338e20eee36d` peels to final main
  `f3b75dc0fbee44e4fe91eb2df1df724f9426640e`; tag-triggered CI and stable
  publication passed. See the [Phase 16 publication record](v1.13.13-phase16-tag-ci-stable-publication-and-cleanup.md).

  ```text
  V1_13_13_STATE: RELEASED_AND_OPERATIONALLY_CLOSED
  PHASE_16: COMPLETE
  TAG: CREATED_AND_VALIDATED
  GITHUB_RELEASE: PUBLISHED_STABLE
  ```
- Its current trackers are [scope](v1.13.13-scope.md),
  [phase list](v1.13.13-phase-list.md), and
  [validation checklist](v1.13.13-validation-checklist.md).
- Phase 1 freezes current authority and requirements in the
  [authoritative roadmap and supersession audit](v1.13.13-phase1-authoritative-roadmap-and-supersession-audit.md),
  [roadmap authority matrix](v1.13.13-roadmap-authority-matrix.md), and
  [v1.x normative requirement matrix](v1.13.13-v1x-normative-requirements.md).
- Phase 2 independently records complete Engine and Catalog planning
  ownership, a complete thin-CLI boundary, narrow application composition,
  zero promised production stubs, and zero architecture blockers in the
  [Engine/Catalog ownership audit](v1.13.13-phase2-engine-catalog-ownership-audit.md),
  [Engine operation matrix](v1.13.13-engine-operation-ownership-matrix.md),
  [Catalog adoption matrix](v1.13.13-catalog-contract-adoption-matrix.md), and
  [CLI boundary exception matrix](v1.13.13-cli-boundary-exception-matrix.md).
  At that Phase 2 snapshot, Catalog architecture was `PARTIAL` under
  `P2-ARCH-001` because five aggregate methods lacked the stable typed-error
  boundary. Phase 9 later proved that root `CLOSED`; current Catalog
  architecture is complete for the frozen v1 contract.
  Phase 3 independently confirms `RESTORE_INSTALL_CONTRACT` as a blocker for
  Phase 6 disposition and later design/remediation.
- Phase 3 independently records `V1_CORRECTNESS: BLOCKED` with three distinct
  root findings: the confirmed restore installation contract, a concurrent
  destination-parent symlink replacement confinement gap, and packed/mixed
  file-deep verification omission. Its exhaustive evidence is the
  [correctness and invariant audit](v1.13.13-phase3-correctness-and-invariant-audit.md),
  [correctness invariant matrix](v1.13.13-correctness-invariant-matrix.md),
  [restore correctness matrix](v1.13.13-restore-correctness-matrix.md), and
  [safety subsystem matrix](v1.13.13-safety-subsystem-audit-matrix.md).
  Phase 3 makes no remediation, backend-equivalence, coordination, or v1
  closure claim.
- Phase 4 independently records SQLite and PostgreSQL support within explicit
  bounds, derives current schema version 16, keeps SQLite-default
  productization in v2 and centralized PostgreSQL product mode in v3, and
  classifies SQLite live GC as `EXPLICIT_V2_PRODUCTIZATION_BOUND`. Its result
  is `DUAL_BACKEND_CONTRACT: BLOCKED` under one new root, `P4-BE-001`, because
  production snapshot-label filtering has backend-dependent case semantics.
  Its exhaustive evidence is the
  [SQLite/PostgreSQL backend audit](v1.13.13-phase4-sqlite-postgresql-backend-audit.md),
  [backend capability matrix](v1.13.13-backend-capability-matrix.md),
  [backend contract evidence matrix](v1.13.13-backend-contract-evidence-matrix.md),
  and [schema/migration parity matrix](v1.13.13-schema-migration-parity-matrix.md).
  All Phase 2/3 findings remain unchanged; Phase 4 makes no coordination,
  remediation, correctness-closure, or v1 closure claim.
- Phase 5 independently records `REPOSITORY_COORDINATION: BLOCKED` under one
  new root, `P5-COORD-001`: the live-repository `simulate gc` route bypasses
  the outer repository lease and can reach schema work plus GC planning
  outside exclusive ownership. The same-process registry, native Unix/Windows
  lifecycle, bounded process-death evidence, owner diagnostics, ordinary
  operation coverage, live-GC exclusion, and PostgreSQL dedicated advisory
  session remain proven within explicit bounds. Evidence is the
  [repository coordination audit](v1.13.13-phase5-repository-coordination-audit.md),
  [coordination contract matrix](v1.13.13-repository-coordination-contract-matrix.md),
  [operation coverage matrix](v1.13.13-coordination-operation-coverage-matrix.md),
  and [platform evidence matrix](v1.13.13-coordination-platform-evidence-matrix.md).
  Network/NAS/distributed coordination remains post-v1; all Phase 2–4
  findings remain unchanged and no remediation is authorized.
- Phase 6 independently freezes six final v1.x blocker roots in five
  remediation workstreams. It promotes the partial Catalog typed-error gap to
  a closure blocker under mandatory `V1R-CAT-003`, keeps the two restore
  invariants distinct, freezes Coldkeep's prior narrow ASCII case-insensitive
  snapshot-label substring behavior across SQLite/PostgreSQL, and preserves
  all other Phase 2–5 roots. V2/v3 deferrals and non-blocking debt are frozen;
  no remediation is implemented. Evidence is the
  [deferred-item and blocker freeze](v1.13.13-phase6-deferred-item-and-blocker-freeze.md),
  [final blocker matrix](v1.13.13-final-blocker-classification-matrix.md),
  [remediation workstream freeze](v1.13.13-remediation-workstream-freeze.md),
  and [deferred-item matrix](v1.13.13-deferred-item-classification-matrix.md).
  Phases 7–9 are generalized for design, minimum remediation, and complete
  cross-platform/backend proof; v1.x remains blocked.
- Phase 7 freezes one implementation-ready design for every workstream, one
  deterministic proof contract for every blocker, a seven-commit Phase 8
  order, and an exact source/test allowlist. Catalog errors translate once at
  five Service boundaries; label matching uses one backend-neutral predicate;
  file-deep verify consumes authoritative Catalog placements; and valid
  `simulate gc` joins the existing outer lease lifecycle. Restore becomes
  exact-only at the storage boundary and uses retained parent/object identity
  plus fail-closed native atomic publication while preserving post-publication
  metadata semantics. Pinned `x/sys v0.38.0` supplies the required Linux,
  Darwin, and Windows surfaces without a new dependency. Evidence is the
  [Phase 7 design freeze](v1.13.13-phase7-remediation-design-and-test-contract-freeze.md),
  [implementation design matrix](v1.13.13-remediation-implementation-design-matrix.md),
  [regression contract matrix](v1.13.13-remediation-regression-contract-matrix.md),
  and [Phase 8 source/test allowlist](v1.13.13-phase8-source-test-allowlist.md).
  Phase 8 implemented all seven frozen boundaries with zero allowlist
  violations. Evidence is the
  [Phase 8 remediation report](v1.13.13-phase8-minimum-v1x-blocker-remediation.md)
  and [blocker implementation status](v1.13.13-phase8-blocker-implementation-status.md).
  Phase 9 independently closes all six roots after complete source, negative,
  SQLite/PostgreSQL, Linux/macOS/Windows, race, fault, adversarial, and
  exact-head hosted proof. Evidence is the
  [Phase 9 remediation proof](v1.13.13-phase9-remediation-regression-and-cross-platform-backend-proof.md),
  [blocker closure matrix](v1.13.13-phase9-blocker-closure-matrix.md), and
  [proof execution matrix](v1.13.13-phase9-proof-execution-matrix.md).
  Phase 10 then reconciled current architecture and release prose, closed
  `P2-DOC-001`, and recorded the current truth in the
  [Phase 10 reconciliation](v1.13.13-phase10-documentation-and-current-state-reconciliation.md),
  [current-state truth matrix](v1.13.13-current-state-truth-matrix.md), and
  [stale-claim disposition matrix](v1.13.13-stale-claim-disposition-matrix.md).
  Phase 11 then passed the complete frozen-candidate local gate with all 44
  mandatory local rows executed, local PostgreSQL proof, both codecs, G1–G17,
  remediation rechecks, cross-builds, smoke, long-run, and four-profile hard
  benchmark integrity. Evidence is the
  [independent full local release gate](v1.13.13-phase11-independent-full-local-release-gate.md)
  and [local execution matrix](v1.13.13-phase11-local-gate-execution-matrix.md).
  Phase 12 then passed exact-candidate CI, Required Gate, PostgreSQL, both
  codecs, native Linux/macOS/Windows, stress, long-run, adversarial, benchmark
  integrity, CodeQL, Codacy, and effective protection policy. Evidence is the
  [hosted gate](v1.13.13-phase12-hosted-exact-head-security-and-quality-gate.md),
  [hosted inventory](v1.13.13-phase12-hosted-check-inventory.md), and
  [security-quality matrix](v1.13.13-phase12-security-quality-evidence-matrix.md).
  Phase 13 then approved formal v1.x normative closure after reconciling all
  49 requirements, the six closed blockers, safe deferrals, residual bounds,
  and current governance. Evidence is the
  [formal closure decision](v1.13.13-phase13-formal-v1x-closure-decision.md),
  [normative closure matrix](v1.13.13-v1x-normative-closure-matrix.md), and
  [closure evidence index](v1.13.13-v1x-closure-evidence-index.md). Phase 14
  then froze the final v1.x record and v2/v3 ownership boundary in the
  [handoff record](v1.13.13-phase14-v1x-closure-and-v2-handoff-record.md),
  [v2 scope matrix](v1.13.13-v2-handoff-scope-matrix.md), and
  [compact closure state](v1.13.13-v1x-final-closure-state.md). The later
  [re-ratification decision](v1.13.13-phase13-formal-v1x-closure-reratification.md),
  [49-row matrix](v1.13.13-v1x-normative-reratification-matrix.md), and
  [evidence index](v1.13.13-v1x-closure-reratification-evidence-index.md)
  prove the quality-remediation delta preserved closure. The dedicated
  [first Phase 14 re-freeze record](v1.13.13-phase14-v1x-closure-v2-handoff-refreeze.md)
  remains the blocked-attempt authority. The
  [test-harness correction and replacement re-freeze](v1.13.13-phase14-test-harness-correction-and-replacement-refreeze.md)
  defines the new Phase 15 candidate and its exact-head effectiveness
  condition; tag, publication, and v2 implementation have not occurred.
- The updated `v1.13.x-release-train.md` is the authoritative current plan.
- `v1.13.10-engine-contract-documentation-truthfulness.md` records the current
  Engine contract boundary and its intentional limitations.
- `v1.13.10-release-state-validator-contract.md` freezes the lifecycle,
  evidence, parsing, CKRS rule, output, fixture, and CI integration contract.
- `v1.13.10-release-state-validator-implementation.md` records its
  deterministic implementation, isolated tests, and blocking CI enforcement.
- `v1.13.10-github-actions-node24-artifact-migration.md` records the five-step
  upload-artifact v7 migration and semantic-preservation evidence.
- `v1.13.10-v1x-closure-summary-and-v2.0-handoff-freeze.md` freezes the final
  v1.x baseline, explicit v2.0 inputs, and v2/v3 scope boundary.
- v1.13.10's public release, tag, merge, tag-CI, and deleted-release-branch
  evidence is recorded separately from its historical pre-release gate narrative.
