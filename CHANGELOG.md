# Changelog

All notable changes to this project will be documented in this file.

This project follows a lightweight, prototype-friendly versioning
approach.

Version numbers indicate conceptual milestones rather than
production stability.

v1.9 formalizes transform-based storage semantics with block-level compression,
explicit verification stages, and frozen engine-extraction contracts.

For the current operator-facing contract, see [README.md](README.md).
For guarantee-to-evidence mapping, see [VALIDATION_MATRIX.md](VALIDATION_MATRIX.md).
For release-gate execution, see [PRE_RELEASE_CHECKLIST.md](PRE_RELEASE_CHECKLIST.md).

Use this file for milestone history and release deltas. If you are new to the
project, do not start here; start with [README.md](README.md).

------------------------------------------------------------------------

## v1.13.15 - Unreleased — Final v1.x Security, Reproducibility, and Operational Closure

- Persisted the final 15-finding/four-invariant closure contract and the
  canonical v1.13.14 publication-identity baseline on a release branch created
  from exact main `5337f955467e67e4cce9bcdae5904a4b2c6af670`.
- Activated source identity `1.13.15`, current root agent authority, and the
  bounded Phase 0–10 lifecycle. Phase 1 Windows security and rename-boundary
  remediation is next.
- Retained `go 1.25` as the language floor while freezing Go 1.26.7 as the
  certified release toolchain; upgraded only `x/sys` to v0.44.0; added blocking
  ordinary-output govulncheck v1.7.0 on Linux and Windows; and bounded native
  Windows rename construction at the UTF-16 limit.
- Added an unprivileged, socket-free development container with development-
  only PostgreSQL; pinned registry-verified official builder, runtime, and
  PostgreSQL image indexes; and passed source installation on Linux, macOS,
  and Windows plus product-image smokes on Linux amd64 and arm64.
- Distribution remains source-only. No schema, storage-format, public API, or
  v2 product implementation change has landed through Phase 3.

## v1.13.14 - 2026-08-26 — Final v1.x Correctness Remediation and Closure Certification

- Published the stable v1.13.14 release from annotated tag object
  `a996b25b562de69749f41c3af56626aeb5d44e33`, peeling to certified final main
  `caac44d459609f89f2c971cb7b07a8678bd52d2c`. Tag CI run `33002555210`
  passed after its single authorized failed-job retry. Phase 26 adds generic
  strict-descendant `post-release-closed` validation, reconciles current
  publication truth, closes `CK-V1-AUD-005`, and completes all `17/17`
  findings without changing the published artifact, source version, product,
  workflow, governance, schema, format, dependency, or v2 implementation.
- Merged immutable Phase23R candidate
  `eef722121aad571a6b2394bde67ea3f08ab768e4` through protected PR #112 as
  normal merge `7962275ffe24f9ca719d5ced543f71f89a1286f4`. Phase 24 reconciles
  merged-not-tagged lifecycle documentation and adds the conditional canonical
  release gate while preserving the 1,397-entry core and SHA-256
  `58d2909d1fe490e3ce246c48b42d02ff8ea256877fec05f1c8c2972f83248023`.
  This pre-publication state remains historical authority for the release
  candidate and is superseded for current lifecycle truth by Phases 25–26.
- Began the owner-authorized Phase 23R recovery after PR #112 proved that the
  release-linearity audit inspected GitHub's synthetic pull-request merge
  instead of the authoritative same-repository release head. The validator now
  accepts `--candidate-ref`, release PR identity fails closed, and deterministic
  regressions preserve synthetic-merge product validation while selecting the
  exact PR head for lineage. The repaired 1,397-entry core is refrozen at
  SHA-256 `58d2909d1fe490e3ce246c48b42d02ff8ea256877fec05f1c8c2972f83248023`.
  The repair head then passed real PR-context quality and Required Gate proof,
  reclosed GATE-004, passed the repeated independent Phase 21 local gate, and
  passed repeated Phase 22 hosted certification. State D is effective after
  final-head hosted acceptance. Historical Phase 2/14/15/20–22 evidence is
  unchanged. This historical pre-merge state is superseded by the protected
  Phase 23 merge and conditional Phase 24 reconciliation above.
- Reconciled the authoritative v1.13.13 publication identity: annotated tag,
  tag-triggered CI, stable GitHub release, and actual temporary-branch state.
- Activated source identity `1.13.14` and `release/v1.13.14` as the active
  corrective-revalidation release.
- Froze the 17 confirmed findings and 4 release-process gates without changing
  runtime behavior, schema, storage, dependencies, workflows, or v2 scope.
- Corrected release-branch governance after GitHub rejected inherited
  authoritative-main merge history: retained non-fast-forward protection,
  removed only the incompatible whole-history linearity rule, and assigned a
  branch-relative merge-history validator to Phase 14.
- Completed the Phase 1 source audit and froze all 17 findings and 4 process
  gates with zero rows closed, together with definitive rejection,
  root-cause-deduplication, compatibility, closure-proof, and owner-phase
  registers.
- Completed the documentation-only Phase 2 remediation-design and regression-
  contract freeze for all 21 rows. The implementation, regression,
  compatibility, and source/test matrices contain no unresolved decisions;
  zero findings and zero process gates are closed.
- Completed Phase 3 deterministic remediation test infrastructure: added an
  invocation-scoped verification-stage observer, private GC filesystem and
  dispatch options, per-`StorageContext` restore hooks, context-aware private
  Doctor callbacks, and deterministic `faultfs` truncate injection. The seams
  are nil by default, race-safe, and behavior-preserving; no finding or process
  gate was closed.
- Closed `CK-V1-AUD-001` in Phase 4 by replacing post-verification catalog
  recounts with a race-safe, invocation-local ledger of successful legacy and
  packed verification stages. Preserved the public `VerifyResult`, level,
  target, error, schema, storage, JSON, and CLI contracts; verification failure
  still returns a zero result. This Phase 4 milestone closed the first of 17
  findings while all 4 process gates remained open.
- Closed `CK-V1-AUD-002` in Phase 5 by preserving caller cancellation and
  deadlines through single-file Store, both Remove identities, both Restore
  identities, Verify, and Doctor Recovery/Schema/Verify/Audit. Sequential
  batches now stop before new dispatch and return truthful partial results;
  bounded safety cleanup remains independent and cleanup failures are joined.
  Phase 4 verification accounting and the still-open Phase 6 finalization-error
  contract are unchanged. Two of 17 findings are closed, all 4 process gates
  remain open, and Phase 6 is Next.
- Closed `CK-V1-AUD-007` and its historical root-equivalent `CK-110-1728` in
  Phase 6 by moving all single-file Store wrappers to one exactly-once
  finalization boundary. Finalization failures now propagate, preceding Store
  failures are joined, Engine returns no success result, and Phase 5
  cancellation/deadline chains remain discoverable. LocalWriter retains sole
  ownership of physical quarantine, StoreFolder and rollback behavior are
  unchanged, three of 17 findings are closed, all 4 process gates remain open,
  and Phase 7 is Next.
- Closed `CK-V1-AUD-008` in Phase 7 by making rollback truncate and close
  failures jointly visible and retaining the exact affected container identity
  in a private poisoned writer state until durable quarantine succeeds.
  Unresolved quarantine now blocks further appends; successful quarantine
  clears poison only after the DB row is non-selectable. Full-container path
  rollback retains the pending container ID, normal rollback remains
  quarantine-free, and Phase 5/6 contracts remain unchanged. Four of 17
  findings are closed, all 4 process gates remain open, and Phase 8 is Next.
- Closed `CK-V1-AUD-006` and `CK-V1-AUD-009` in Phase 8 by preserving
  `SnapshotRestoreRequest.Overwrite` through the Engine adapter and routing
  SnapshotShow, SnapshotDiff, and SnapshotRestore through one private selector
  normalizer. Existing lower atomic overwrite ownership, explicit restore-path
  accounting, public shapes, CLI/JSON contracts, schema, and storage formats
  remain unchanged. Six of 17 findings are closed, all 4 process gates remain
  open, and Phase 9 is Next.
- Closed `CK-V1-AUD-010` and `CK-V1-AUD-012` in Phase 9. SnapshotDiff now
  rejects negative Limit, preserves raw Total and pre-limit Matched/Summary
  populations, sorts by stored path, and caps only detailed Entries. Existing
  Catalog find/list/graph queries now carry persisted snapshot membership into
  list/show/graph/tree `SnapshotMeta.FileCount` without N+1 queries, schema,
  public-shape, JSON, or CLI changes. Eight of 17 findings are closed, all 4
  process gates remain open, and Phase 10 is Next.
- Completed the documentation-only Phase 10 GC execution and byte-accounting
  design freeze. `CK-V1-AUD-003` and `CK-V1-AUD-004` remain open with verified
  active-container accounting, transaction-concurrency, partial-result,
  deterministic scheduling, physical-byte, filesystem-failure, and overflow
  contracts. Eight of 17 findings remain closed, all 4 process gates remain
  open, and Phase 11 is the sole Next phase; implementation has not started.
- Closed `CK-V1-AUD-003` and `CK-V1-AUD-004` in Phase 11. Engine now validates
  and forwards GC Workers; maintenance executes materialized sealed and active
  plans through a bounded no-backlog coordinator with plan-ordered results,
  stop-new-dispatch semantics, and truthful partial results. Reclaimed bytes
  come from one safe-path physical `Stat` per affected unit; active live
  removals are counted, filesystem failures are surfaced, failed Remove earns
  zero credit, and checked overflow preserves the exact prefix plus all
  successful count/filename evidence. Ten of 17 findings are closed, all 4
  process gates remain open, and Phase 12 is the sole Next phase.
- Closed `CK-V1-AUD-011`, `CK-V1-AUD-013`, and `CK-V1-AUD-014` in Phase 12.
  One private Engine resolver now applies the documented ContainerDir default
  consistently to Verify, GC, Recover, ordinary Restore, and RestoreStoredPath
  while preserving explicit values exactly. `StoreRequest.Codec` now documents
  the unchanged explicit request, `COLDKEEP_CODEC`, then `aes-gcm` precedence.
  Store and Stats now share one Store-compatible packed-block target resolver;
  Store warning behavior is unchanged and Stats remains silent. Thirteen of 17
  findings are closed, all 4 process gates remain open, and Phase 13 is the
  sole Next phase.
- Closed `CK-V1-AUD-015`, `CK-V1-AUD-016`, and `CK-V1-AUD-017` in Phase 13.
  The root README now uses the accepted `search --name` form; README and top
  help expose the existing five-code exit contract, invariant-over-execution-
  over-validation batch precedence, and distinct JSON error-field roles; and
  init help documents the existing none/zstd compression forms, block-level
  pre-encryption ordering, new-writes-only scope, and zstd level range. Runtime
  parsing, classification, JSON, search, init, and compression behavior are
  unchanged. Sixteen of 17 findings are closed, all 4 process gates remain
  open, `CK-V1-AUD-005` remains partial, and Phase 14 is the sole Next phase.
- Completed all four release-process gates in Phase 14. CLI schema-gate
  subprocesses now receive unique absolute TempDir storage roots; snapshot
  evidence names are validated only through tracked Git Go source; benchmark
  evidence uses external transient generation, exhaustive checksummed
  manifests, same-filesystem atomic exact-SHA promotion, signal cleanup, and
  explicit ignored inventory; and release branches now have a repository-side
  merge-base-relative linearity validator alongside the unchanged Phase 0
  non-fast-forward ruleset. Product runtime, workflow topology, governance,
  benchmark thresholds, schema, storage formats, and dependencies are
  unchanged. Findings remain `16/17`, `CK-V1-AUD-005` remains partial, process
  gates are `4/4`, and Phase 15 is the sole Next phase.
- Completed Phase 15 finding-by-finding integrated regression closure. All 20
  currently executable frozen primary regressions pass independently, the
  AUD-005 lifecycle audit remains consistent and deliberately deferred to
  Phases 18/26, and all 21 numbered rows reconcile without production or test
  code changes. Findings remain `16/17`, process gates remain `4/4`,
  `CK-V1-AUD-005` remains partial, and Phase 16 is the sole Next phase.
- Completed Phase 16 backend, codec, compression, and storage-layout proof.
  All 10 owner rows and all 73 derived non-Cartesian compatibility cells pass:
  20/20 backend, 16/16 codec, 16/16 compression, and 21/21 layout. SQLite,
  PostgreSQL, plain, AES-GCM, none, zstd, legacy, packed, and mixed evidence is
  direct and complete; no product or test code changed. Findings remain
  `16/17`, process gates remain `4/4`, `CK-V1-AUD-005` remains partial, and
  Phase 17 is the sole Next phase.
- Completed Phase 17 cancellation, deadline, race, deterministic-fault,
  adversarial, and native cross-platform proof. All 17 deep rows and every
  derived runtime cell pass: 10/10 cancellation, 17/17 race, 12/12 fault,
  17/17 adversarial, and 15/15 native. The complete owner/process/PostgreSQL
  race matrix and both G1–G17 codec profiles passed without production or test
  changes, and repository-local runtime storage remained absent. Findings
  remain `16/17`, process gates remain `4/4`, `CK-V1-AUD-005` remains partial,
  and Phase 18 is the sole Next phase.
- Completed the documentation-only Phase 18 current-candidate reconciliation.
  Root README, the v1.13 index, the active changelog entry, and current
  v1.13.14 release controls now agree that Phases 0–18 are complete, Phase 19
  is next, findings remain `16/17`, process gates remain `4/4`, and all 21
  integrated rows plus the compatibility and runtime-stress matrices are
  complete. `CK-V1-AUD-005` remains lifecycle-partial: candidate truth is
  reconciled and final publication truth remains pending Phase 26.
  `v1.13.14` remains active and unreleased with no tag or stable GitHub
  release; corrective revalidation remains in progress and no product, test,
  script, workflow, schema, storage, dependency, governance, or benchmark
  threshold changed.
- Completed the documentation-only Phase 19 formal corrective closure
  decision. All A–L inputs are satisfied, including `CK-V1-AUD-005` as
  `SATISFIED_NON_BLOCKING_LIFECYCLE_PENDING`; all 17 findings, 4 gates, 21
  integrated rows, and 8 frozen rejections remain accounted for. Candidate-side
  technical corrective completion is complete and entry into the final release
  lifecycle is approved, while findings remain `16/17`, corrective
  revalidation remains in progress, and release-readiness certification stays
  pending Phases 21/22. Phase 20 is now the sole Next phase under separate
  authorization. No merge, tag, release, publication, final closure, product,
  test, script, workflow, schema, storage, dependency, governance, or benchmark
  threshold change occurred.
- Completed the documentation-only Phase 20 final corrective handoff and
  immutable candidate-core freeze. The canonical 1,397-entry candidate-core
  manifest has SHA-256
  `984221fc5dc0e9d2d76d97cfb2c334bd7a52733682d818cb839d7c5caacac4da`;
  only the explicitly lifecycle-mutable release documentation is excluded.
  Phases 0–20 are complete and Phase 21 is the sole Next phase under separate
  authorization, pending exact-head Phase 20 hosted acceptance. Findings remain
  `16/17`, `CK-V1-AUD-005` remains open partial with Phase 26 publication truth
  pending, release-readiness certification remains pending Phases 21/22, and
  no product, test, script, workflow, schema, storage, dependency, governance,
  benchmark threshold, merge, tag, release, publication, or final closure
  changed.
- Completed Phase 21's independent full local exact-candidate release gate.
  Exact Go 1.25.10 and golangci-lint 2.6.2, isolated PostgreSQL, both codec
  matrices, full uncached and race sweeps, G1–G17, smoke, legacy, hard
  benchmark integrity, operator, snapshot, and retention gates passed. The
  1,397-entry candidate core remained byte-for-byte unchanged at SHA-256
  `984221fc5dc0e9d2d76d97cfb2c334bd7a52733682d818cb839d7c5caacac4da`.
- Completed Phase 22's hosted exact-head release gate against Phase 21 evidence
  commit `1fe768a45eec1117d3c808581080e75545812617`. CI run `32949760025`, all 25
  required jobs, Required Gate `98123359910`, native Linux/macOS/Windows,
  CodeQL run `32949759949`, Aggregate `98118569421`, hosted benchmark
  artifacts, release linearity, and live governance passed. Local and hosted
  readiness are now `CERTIFIED_PHASES_21_22`; Phases 0–22 are complete and
  Phase 23 is the sole Next phase under separate authorization. No merge, tag,
  publication, final closure, AUD-005 closure, product, test, script, workflow,
  schema, storage, dependency, benchmark-threshold, or governance change
  occurred.

## v1.13.13 - 2026-08-23 — Final v1.x and v2 Handoff Gate

- Reconciled canonical v1.13.12 lifecycle truth after its final-main merge,
  annotated tag, tag validation, stable publication, and temporary-branch
  cleanup, while preserving the historical Phase 0–21 evidence as frozen.
- Completed Phase 16 publication: annotated tag object
  `5d534a6b4a9d44ab7303d9b76575338e20eee36d` peels to
  `f3b75dc0fbee44e4fe91eb2df1df724f9426640e`; tag CI run `32657349955` and
  its Required Gate passed, and the stable GitHub release was published.
- Activated release identity 1.13.13 on `release/v1.13.13` and froze the
  Phase 0–16 train in `AUDIT_PLUS_REMEDIATION` mode. Phase 0 is complete and
  Phase 1 is next.
- Recorded `RESTORE_INSTALL_CONTRACT` as the unresolved blocker family entering
  v1.13.13. Phase 0 changes no restore behavior and authorizes no v2 work.
- Froze the Phase 1 authority hierarchy and supersession rules, with document-
  level and stable-ID normative requirement matrices for later audits.
- Classified SQLite-default portable productization as v2 while retaining
  SQLite backend and PostgreSQL compatibility requirements through v1.
- Froze local same-host coordination as the v1 boundary, local daemon/product
  work as v2, and network/NAS/distributed expansion as v3.
- Audited all 25 active Engine operations and all 13 methods across ten Catalog
  responsibility interfaces, separating ten production-adopted methods from
  three library-only facade methods without using hosted PostgreSQL execution
  as a substitute for source/routing evidence.
- Froze the complete 36-row CLI lower-layer exception inventory: every entry
  is bounded parsing, composition, projection, compatibility, benchmark, or
  simulation support, with zero architectural bypasses.
- Recorded complete Engine and thin-CLI architecture, narrow application
  composition, zero promised production stubs, and zero Phase 2 architecture
  blockers. Catalog planning adoption is complete, while aggregate Catalog
  architecture is partial because five methods lack its stable typed-error
  boundary (`P2-ARCH-001`). At Phase 2, correctness, backend, coordination,
  restore, and v1 closure decisions remained later-phase work.
- Completed the source-first Phase 3 audit of every frozen correctness
  requirement and every production restore route. The audit confirms
  `RESTORE_INSTALL_CONTRACT` and records two additional root blockers:
  concurrent destination-parent symlink replacement and packed/mixed
  file-deep verification omission. No finding was remediated.
- Revalidated GC reachability and G6 coherent planning, snapshot safety,
  recovery/repair, container bounds, bounded decompression, exact numeric
  fidelity, SQL mutation cardinality, and scoped concurrent GC planning as
  proven within explicit backend/platform/coordination bounds. Backend
  equivalence remains Phase 4 and coordination ownership remains Phase 5.
- Completed the source-first Phase 4 SQLite/PostgreSQL audit and mechanically
  derived current schema version 16 from migration and fresh-schema sources.
- Preserved SQLite and PostgreSQL as supported v1 backends within explicit
  bounds, kept SQLite-default productization in v2 and centralized PostgreSQL
  product mode in v3, and classified SQLite live GC as an authority-backed
  explicit v2 productization bound rather than inferring it from daemon scope.
- Recorded `DUAL_BACKEND_CONTRACT: BLOCKED` under one new root,
  `P4-BE-001`: production snapshot-label filtering uses backend-native `LIKE`,
  producing different case matching on SQLite and PostgreSQL. No finding was
  remediated, and all inherited Phase 2/3 findings remain unchanged.
- Completed the source-first Phase 5 repository coordination audit across
  identity, control namespace, process reservation, native Unix/Windows locks,
  owner diagnostics, operation coverage, process death, live GC, and the
  PostgreSQL dedicated advisory session.
- Preserved same-host/local-filesystem coordination as the v1 boundary and
  network/NFS/SMB/NAS/cloud/distributed coordination as post-v1 scope, without
  interpreting PostgreSQL advisory ownership as a cross-host repository lock.
- Recorded `REPOSITORY_COORDINATION: BLOCKED` under one new root,
  `P5-COORD-001`: the live-repository `simulate gc` route bypasses the outer
  lease and can reach schema work plus GC planning outside exclusive
  ownership. No finding was remediated, and all Phase 2–4 findings remain
  unchanged for Phase 6 disposition.
- Completed Phase 6 by independently classifying six final v1.x blocker roots
  and freezing five remediation workstreams. The previously partial Catalog
  typed-error gap is a closure blocker under mandatory `V1R-CAT-003`; the two
  restore safety roots remain distinct within one destination workstream.
- Froze narrow ASCII case-insensitive snapshot-label substring matching as the
  shared SQLite/PostgreSQL contract from Coldkeep's pre-Catalog query behavior,
  rather than from an accidental SQLite default.
- Froze v2 local-daemon/SQLite productization, v3 network/distributed
  expansion, documentation debt, optional maintenance, and historical
  supersession without implementing remediation or authorizing v2 work.
- Generalized Phases 7–9 to remediation design/test-contract freeze, minimum
  v1.x blocker remediation, and complete cross-platform/backend regression
  proof. v1.x remains blocked pending closure of all six roots.
- Completed the documentation-only Phase 7 design freeze for all five
  remediation workstreams and all six independent blocker proof contracts.
- Froze Catalog error translation at the five Service boundaries, one shared
  SQLite/PostgreSQL snapshot-label predicate, Catalog-authoritative packed
  file-deep verification, and outer-lease ownership for valid `simulate gc`.
- Froze exact-only restore routing and retained parent/object installation,
  including atomic fail-closed Linux/Darwin/Windows publication and preserved
  post-publication strict-metadata behavior. Verified the required native
  bindings in pinned `golang.org/x/sys v0.38.0`; no dependency was added.
- Froze the seven-commit Phase 8 order, source/test allowlist, Build-mode stop
  conditions, and Phase 9 closure matrix. All six blocker roots remain
  `OPEN_FROZEN`; Phase 8 is next and no remediation has begun.
- Implemented the seven frozen Phase 8 boundaries across Catalog typed errors,
  backend-neutral snapshot-label matching, authoritative packed file-deep
  verification, `simulate gc` repository coordination, and restore routing,
  native atomic publication, and retained-object metadata.
- Added fail-closed Linux/Darwin/Windows restore installation without a new
  dependency, preserving exact destinations, atomic no-overwrite, intentional
  overwrite, confined cleanup, and post-publication strict-metadata behavior.
- Passed the complete local Phase 8 package, race, G5/G7 adversarial, vet,
  lint, and cross-platform compile validation with zero allowlist violations.
  All six blockers are `IMPLEMENTED_PENDING_PROOF`; none is `CLOSED`, and
  Phase 9 remains responsible for complete platform/backend closure proof.
- Completed Phase 9 by reconciling all 34 frozen regression IDs and closing
  all six blocker roots independently through source, negative, local,
  SQLite/PostgreSQL, Linux/macOS/Windows, race, fault, adversarial, and
  exact-head hosted evidence.
- Added test-only proof for complete Catalog cancellation/deadline identity,
  exhaustive dual-backend packed placement failure, independent-process
  `simulate gc` ownership, native retained-parent/publication behavior, and
  PostgreSQL integration routes; production Go behavior is unchanged.
- Recorded the Phase 9 evidence, blocker-closure matrix, and proof-execution
  matrix. Phase 10 documentation/current-state reconciliation is next; final
  v1.x closure, merge, tag, publication, and v2 implementation remain
  unauthorized.
- Completed the documentation-only Phase 10 current-state reconciliation.
  Engine is complete for v1, Catalog is complete for the frozen v1 contract,
  the CLI is thin for v1, and `P2-DOC-001` is closed.
- Reconciled exact restore publication and confinement, complete authoritative
  legacy/packed/mixed file-deep verification, backend-neutral snapshot-label
  behavior, and coordinated live `simulate gc` within the proven platform and
  threat bounds.
- Preserved SQLite-default local productization in v2, network/NAS/distributed
  product scope in v3, and all Phase 0–9 historical evidence. All currently
  identified frozen blocker roots are technically closed, but formal v1.x
  closure and the v1.13.13 release remain pending later phases.
- Advanced Phase 11 — Independent Full Local Release Gate — to Next without
  changing production code, tests, schemas, migrations, dependencies, CI,
  validator implementation, or version.
- Completed Phase 11 on immutable starting candidate
  `ab6aa49f54b34ed8a4610058a16b63ce4eab7d29`: all 44 mandatory local rows
  passed with zero failures and zero unexpected skips, including full/race,
  both codecs, local PostgreSQL, G1–G17, restore/verify/Catalog/coordination
  rechecks, smoke, legacy/snapshot, long-run/refcount, cross-build, and all four
  hard benchmark-integrity profiles.
- Recorded the Phase 11 independent local-gate evidence and execution matrix,
  removed all gate-generated repository artifacts, preserved all seven closed
  findings and zero known open v1 blockers, and advanced Phase 12 — Hosted
  Exact-Head Security and Quality Gate — to Next. Formal v1.x closure remains
  pending a later phase; no production, test, schema, migration, dependency,
  CI implementation, validator, or version change is included.
- Completed Phase 12 on exact candidate
  `123976440053264362c2b93041ade43aed3788cf`: all 29 GitHub checks, CI Required
  Gate, PostgreSQL/backend proof, both codecs, native Linux/macOS/Windows,
  stress, long-run, adversarial, hard benchmark integrity, smoke, legacy,
  coverage reporting, all CodeQL analyses, and CodeQL Aggregate passed.
- Reconciled Codacy exact-head quality with `analyzed=true`, up-to-standards,
  and zero added regular or potential issues; verified zero open release-branch
  CodeQL alerts and effective required-check/branch rules with no bypass.
- Added the hosted gate report, complete check inventory, and security-quality
  matrix, and advanced Phase 13 — Formal v1.x Closure Decision — to Next in
  `PLAN` mode. Formal closure, merge, tag, publication, and v2 implementation
  remain unauthorized.
- Completed Phase 13 by reconciling all 49 frozen Phase 1 rows: all 45
  mandatory v1 obligations are satisfied, the four explicit non-v1 boundary
  rows remain truthful, all six blocker roots and `P2-DOC-001` remain closed,
  and no late blocker or current contradiction remains.
- Approved formal v1.x normative completion and added the Phase 13 decision,
  49-row closure matrix, and evidence index. v1.13.13 remains an unreleased
  release candidate; Phase 14 handoff, Phase 15 merge, Phase 16 tag/publication,
  and all v2 implementation remain pending.
- Completed Phase 14 by freezing the final v1.x closure record, authoritative
  local-first v2 handoff scope, inherited v1 guarantees, explicit v3 boundary,
  optional maintenance, and zero inherited v1 blockers.
- Added the Phase 14 handoff record, v2 scope matrix, and compact Phase 15/16
  closure reference. Advanced Phase 15 — Merge and Post-Merge Reconciliation —
  to Next without implementation, test, configuration, v2 branch, merge, tag,
  publication, GitHub-governance, or Codacy-configuration changes.
- Completed the Phase 15 pre-merge quality remediation at
  `02647536ac618f8f2df8297863d05357fe15eb54`, preserving all six technical
  blocker closures while reducing candidate-added regular and potential
  Codacy findings to zero.
- Re-ratified the 49/49 formal v1.x closure against that remediation head and
  added the decision, normative matrix, and evidence index. Historical Phases
  13 and 14 remain Complete; that decision required a Phase 14 handoff
  re-freeze before Phase 15 could receive a replacement immutable candidate.
- Added the dedicated Phase 14 recovery-path re-freeze record without changing
  the historical Phase 14 evidence or any v2/v3 ownership. The commit
  containing that record defined candidate attempt
  `1668a7490048144f4dea0fd795f4779b5e7108b5`, whose push gate passed but PR
  gate failed closed on nondeterministic fixed-byte AES-GCM test corruption.
- Corrected only that test mechanism in
  `bd84206a1de5fc5568e83abcfae1e382f44c2ba1`: the test now targets the first
  ciphertext byte and XORs it with `0x01`, guaranteeing corruption without
  changing product behavior, test intent, or assertions.
- Added the replacement Phase 14 re-freeze record. The commit containing that
  record defines the new Phase 15 candidate, whose freeze becomes effective
  only after full exact-head hosted proof.

```text
QUALITY_REMEDIATION_HEAD: 02647536ac618f8f2df8297863d05357fe15eb54
FORMAL_V1_X_CLOSURE_RERATIFICATION: APPROVED
PRIOR_PHASE_14_REFREEZE_ATTEMPT: 1668a7490048144f4dea0fd795f4779b5e7108b5
PRIOR_ATTEMPT_RESULT: BLOCKED_TEST_HARNESS_NONDETERMINISM
CORRECTIVE_TEST_COMMIT: bd84206a1de5fc5568e83abcfae1e382f44c2ba1
PRODUCT_BEHAVIOR_CHANGED: NO
PHASE_14_REFREEZE: REPLACEMENT_CANDIDATE_IDENTITY_DEFINED_BY_THIS_COMMIT
PHASE_15_RELEASE_CANDIDATE: REPLACEMENT_PHASE_14_REFREEZE_COMMIT
CANDIDATE_FREEZE_EFFECTIVE: CONDITIONAL_ON_EXACT_HEAD_PROOF
PHASE_15_MERGE: NOT_AUTHORIZED_BY_PHASE_14
PHASE_15: NEXT_OPERATION_AFTER_EFFECTIVE_FREEZE
MERGE_AUTHORIZED: NO
```

------------------------------------------------------------------------

## v1.13.12 - 2026-08-20 — Engine and Catalog Completion

- Started the mandatory architecture-completion release from exact v1.13.11
  merge commit `507859daccf25594142c61e5ab8209a751fb579a`.
- Activated version 1.13.12 and froze the production ownership, compatibility,
  catalog-adoption, and thin-CLI acceptance baseline.
- Added the stable backend-neutral engine error taxonomy, deterministic
  translation helpers, invariant/cancellation preservation, and exhaustive
  active-contract neutrality coverage with only the four explicit Phase 3
  observability DTO debts temporarily allowlisted.
- Replaced Stats and Inspect observability-backed contracts with complete
  engine-owned DTOs, exact tagged dynamic values, ordered trace events, and a
  complete Verify summary while preserving the existing renderer output
  through compatibility projection.
- Completed the catalog graph, placement, restore-plan, and GC-plan contracts
  on SQLite and PostgreSQL and adopted each in its production path. Completed
  engine ownership for folder store, list/search, configuration, snapshots,
  inspect/stats/verify, repair, recovery/startup recovery, Doctor, and live GC
  planning behind an application composition boundary and enforced thin-CLI
  dependency guards.
- Completed the isolated compatibility and adversarial regression phase. Its
  full PostgreSQL matrix caught and corrected Doctor session-open exit/message
  projection drift; the restarted plain matrix, AES-GCM matrix, G1–G17
  adversarial suite, coordination/advisory-session proofs, legacy fixture, and
  post-correction full/race suites pass. Phase 21 passed for exact candidate
  `33aa1a563b1e6f7b09a86326c6bbd06d7b106e58`: local Profile A and hosted
  exact-head validation both passed. PR #107 merged that candidate into `main`
  as `7505d7000faef452caeb4c01784f0510960e7240`; post-merge reconciliation PR
  #108 produced final `main` `fd396cd0c8cf43662881211b8e6b2877eb9a8010`.
  Annotated tag `v1.13.12` (tag object
  `f5772657721b6d45e659b4a87e6546988b5bbd86`) peels to that final commit, tag
  validation passed, the stable GitHub release was published, and the release
  and post-merge reconciliation branches were deleted. v1.13.12 is released
  and operationally closed without closing v1.x.

------------------------------------------------------------------------

## v1.13.11 - 2026-08-19 — Safety and Backend Compatibility Gate Closure

- Published the stable v1.13.11 release from merge commit
  `507859daccf25594142c61e5ab8209a751fb579a` after exact-head candidate,
  hosted PR, main, tag, and release validation. This completes v1.13.11 but
  does not close v1.x; v1.13.12 and v1.13.13 remain mandatory.

- Completed the Phase 20 pre-release state transition and froze the exact-head
  candidate contract. The commit containing that contract must pass
  candidate-head CI, Required Gate, CodeQL, and the complete clean local
  Profile A gate before one pull request to `main` is authorized.
- Kept benchmark integrity hard-required, timing advisory, and BKC-016
  `Deferred — documented`. Merge, tag, publication, and release-branch
  deletion remain separate later operations.

### Phase 11 benchmark gate diagnostic bootstrap

- Added a fixed, release-gate-only `ci-stable-v1` calibration fixture, strict
  single-envelope schema-v2 reports, per-case database isolation, and an
  external sampler with median/MAD statistics and fail-closed evidence checks.
- Added a manual-only, read-only calibration/baseline-capture workflow pinned
  to Go 1.25.12, Ubuntu 24.04, and the reviewed PostgreSQL 16 image digest.
- Required CI still uses the historical benchmark gate. No baseline,
  threshold, repository runtime, or Phase 12 change is included; Phase 11
  remains blocked on the predetermined calibration.

### Phase 11 repository coordination contract implemented

- Added the internal exclusive-only repository coordination contract, stable
  error sentinels, non-mutating canonical container-namespace identity,
  recovery-safe `.coldkeep-control` namespace, versioned diagnostic owner
  metadata, and explicit lease lifecycle helper.
- Added fake-based contract tests for identity aliases, operation policy,
  owner metadata, cancellation/deadlines, release errors, nested acquisition,
  and independent repositories. Added a pure CLI policy seam that classifies
  participating commands without acquiring a lock.
- BKC-016 remains `Deferred — documented`: native Linux/macOS/Windows locking,
  CLI acquisition, subprocess contention, crash release, live-GC barriers, and
  PostgreSQL advisory session ownership remain Phases 12–13 work. No workflow,
  schema, Engine behavior, OS lock, or advisory-lock behavior changed.

### Phase 10 transaction and row-lock contracts complete

- Added shared SQLite/PostgreSQL transaction contracts for backend detection,
  commit/rollback, read-own-writes, constraint rollback, affected rows,
  PostgreSQL `FOR UPDATE`, `NOWAIT`, `SKIP LOCKED`, server-observed blocked-lock
  cancellation, and SQLite's intentional clause-omission boundary.
- Added a production-helper integration contract for container NOWAIT
  contention/savepoint recovery and deterministic SKIP LOCKED allocation.
- Extended the existing plain-codec internal-package run with
  `./internal/container` and five exact PostgreSQL pass-event requirements.
  Exact-head CI run `30148670910` at `ad82c959` passed all five events in plain
  job `89655223183` and required gate `89656972706`. The initial uncompressed
  benchmark variance was resolved by successful same-head rerun `89656813012`;
  no benchmark accommodation or production code change was made.

### Phase 9 engine mutation parity complete

- Added five shared SQLite/PostgreSQL Engine mutation contracts covering
  single-file Store, by-ID and stored-path Remove/Restore, snapshot
  create/delete/restore, deterministic failures and partial batches, semantic
  repository/container fingerprints, and GC dry-run planning/non-mutation.
- Exact-head CI run `30114444798` at `848e579b` passed quality, all five new
  `/postgres` events in plain correctness job `89551564893`, and aggregate
  required-gate job `89555865026`. BKC-012/013 are equivalently proven only
  for the documented active, uncontended mutation and GC dry-run contracts.
- Extended only the existing plain-codec internal-package JSON event parser and
  matching CI audit. No production code, workflow job, package invocation,
  codec leg, schema, storage format, or lock behavior changed.

### Phase 8 snapshot selector determinism closure complete

- Completed Phase 8 exact-head CI evidence in run `30109561344` at `bcae3576`:
  quality, both correctness codecs, required PostgreSQL selector events,
  adversarial, stress, long-run, smoke, compatibility, benchmark,
  cross-platform, and `CI Required Gate` all passed. BKC-011 is equivalently
  proven only for the scoped snapshot list/show/stats/diff selector and
  tree-presentation contracts.
- Implemented Phase 8 snapshot selector contracts for deterministic equal-time
  list ordering, file-query filtering, invalid direct-engine regex rejection,
  pre-cancelled selection, and read non-mutation. CLI diff now preserves
  repeated path/prefix query selectors rather than narrowing an unordered map
  into one engine path. The direct-engine invalid-regex silent-ignore defect is
  corrected by fallible query conversion and propagation through show/diff.

- Recorded the Phase 0 post-release correction that restored the v1.13.11–v1.13.13 release train.
- Activated executable and reusable release-checklist identity to `1.13.11`.
- Completed the Phase 2 backend compatibility claim matrix, distinguishing
  separate evidence from proven parity and recording required-CI gaps.
- Completed the Phase 3 reusable dual-backend test harness with file-backed
  SQLite fixtures, optional isolated PostgreSQL scratch databases, strict
  cleanup reporting, and catalog-suite adoption.
- Implemented Phase 4 required-CI activation for PostgreSQL-gated internal
  package contracts in the plain correctness-matrix codec leg, with JSON
  execution-proof enforcement; run `29729981751` confirmed it. Phase 5
  schema/bootstrap/migration parity is Next; no backend parity claim is added.
- Closed Phase 5 schema/bootstrap/migration contract evidence: required
  PostgreSQL SCH execution, canonical lint/vet, and selected schema contracts
  are recorded without claiming broad schema parity.
- Recorded deterministic G6 shared packed-block corruption reproduction and
  added fail-closed protection that refuses partial rebuild of a shared
  immutable block while preserving single-member cleanup.
- Recorded final green exact-head CI after one authorized same-SHA retry of a
  transient workers=4 uncompressed benchmark anomaly; no benchmark baseline,
  workflow, or configuration changed between attempts.
- Completed exact-head catalog contract evidence in CI run `29983479388` at
  `db12c3d2`: all six PostgreSQL catalog contract selectors passed, including
  CAT-004 after its deterministic `created_at DESC, id DESC` ordering fix, and
  the aggregate required gate succeeded. Phase 6 is complete.
- Completed Phase 7 exact-head engine read-side evidence in CI run
  `29993172886` at `313d0069`: all four required PostgreSQL engine selectors,
  quality, both correctness legs, adversarial validation, and the aggregate
  required gate passed.
- Corrected deep verification for a single-connection SQLite handle by fully
  materializing and closing eligible-container rows before querying packed
  blocks. The bounded regression retains `MaxOpenConns(1)` and the production
  packed-storage writer; byte-level verification is unchanged.
- BKC-010 is now equivalently proven for the tested Stats, Inspect, Verify,
  context/error, and non-mutation contracts. BKC-011 remains separate evidence
  because selector/query behavior is Phase 8 work, which is Next.

------------------------------------------------------------------------

## v1.13.10 - 2026-07-19 — v1.x Closure Integrity and CI Runtime Hygiene

- Closed v1.13.9 post-release documentation truth, release-train
  reconciliation, engine-contract ownership documentation, and the v1.x/v2.0
  handoff freeze without adding runtime features.
- Added the deterministic release-state validator, its isolated fixture suite,
  blocking CI enforcement, and CI-audit coverage; maintained the Node 24
  artifact runtime with `actions/upload-artifact@v7`.
- Corrected two PostgreSQL release-gate test fixtures in `43ae85f` without
  changing runtime, schema, migration, storage, repository, or backend
  behavior.
- Corrected benchmark scratch-database cleanup in `eb38a58` after the first
  documentation candidate exposed an operational release-gate failure. The
  bounded benchmark-infrastructure change preserves workloads, thresholds,
  baselines, schemas, migrations, and normal runtime behavior.
- Corrected the subsequent package-interaction test isolation in `53b66dda`:
  benchmark lifecycle tests now assert cleanup of their own exact scratch
  database names rather than a cluster-global set. No production behavior
  changed, and the complete local pre-release gate passed on that remediation
  commit with no residual benchmark scratch databases.
- Public GitHub evidence confirms stable release `Coldkeep v1.13.10 — v1.x
  Closure Integrity and CI Runtime Hygiene`, published July 19, 2026 at 18:01;
  tag `v1.13.10` targets `423c57815580c39bee4f79ecd81570e9cfa9d273`, the merge
  of PR #105. Tag-triggered CI run #502 succeeded with 19 jobs in 18m26s, and
  `release/v1.13.10` is absent from the public branch list. The local GitHub
  CLI token was invalid; public GitHub pages supplied the independent evidence.
- This remains a valid released closure-integrity and CI-runtime-hygiene
  baseline. Its prior final-v1.x conclusion was superseded after release by a
  roadmap-to-code audit that found remaining must-before-v2 work; the active
  release train is now v1.13.11–v1.13.13.

------------------------------------------------------------------------

## v1.13.9 - 2026-07-18 — Snapshot Mutation Boundary: Create / Delete / Restore

- Closed the inherited correctness and CI baseline, including trusted restore
  destinations, stored-path batch compatibility, and required cross-platform
  enforcement.
- Activated snapshot create, delete, and restore at the engine boundary and
  routed the production CLI through those engine methods while preserving
  established CLI contracts.
- Stabilized snapshot mutation request/result ownership, completed routing and
  boundary hardening, and recorded the repair/recovery boundary decision.
- Hardened SQLite/PostgreSQL compatibility evidence; completed CLI thin-wrapper
  and coupling review; and completed the v1.x/v2.0 handoff review.
- Released with verdict `READY WITH NON-BLOCKING DEFERRALS`; no mandatory v1.x
  runtime remediation remained.
- Deferred active `StoreRequest` narrowing, read-side coupling decisions, and
  repair/recovery activation design to early v2.0; deferred daemon, API, UI,
  and broader productization/architectural work to later v2.x.

------------------------------------------------------------------------

## v1.13.8 - 2026-06-28 — Restore / Remove Contract Split and Stored-Path Boundary

- Preserved valid zero-reference logical-file state across SQLite reopen and
  PostgreSQL schema rerun without altering schema version `16`.
- Separated by-ID restore from stored-path restore and by-ID logical removal
  from stored-path mapping removal.
- Added dedicated engine-owned stored-path restore and remove boundaries and
  routed the production CLI through them without changing observable behavior.
- Hardened restore destination safety and expanded remove, reachability,
  verification, GC, and backend invariants.
- Preserved PostgreSQL as the current normal local runtime backend and did not
  add a normal SQLite local runtime mode.

------------------------------------------------------------------------

## v1.13.7 - 2026-06-20 — SQLite-First Repository Portability Baseline

- Established the future SQLite-first repository portability baseline without
  switching the normal runtime backend.
- Defined the future repository-root SQLite catalog placement contract and
  added direct catalog-plus-payload relocation evidence.
- Corrected physical-file migration backfill behavior for already-mapped
  logical files while preserving PostgreSQL compatibility.
- Kept PostgreSQL as the normal local runtime backend and did not add a normal
  SQLite local runtime mode.
- Preserved CLI behavior, JSON output, exit codes, storage format, and
  repository format behavior.
- Identified a later zero-reference migration edge case post-release and
  assigned it to `v1.13.8`.

------------------------------------------------------------------------

## v1.13.6 - 2026-06-15 — Catalog Contract Expansion and Backend Parity Review

- Expanded the active catalog-contract inventory and classified the backend
  parity surface across SQLite and PostgreSQL.
- Reviewed snapshot membership, logical/physical mapping, placement,
  restore-plan, reachability, and GC eligibility contracts.
- Kept the release documentation-led: no new runtime behavior, schema, or
  storage-format changes were introduced.
- Preserved PostgreSQL compatibility while keeping SQLite-first direction in a
  preparatory state.
- Recorded that existing evidence was sufficient without adding a new Phase 8
  guard suite.

------------------------------------------------------------------------

## v1.13.5 - 2026-06-14 — Release Record Closure and Engine-Level Invariant Ownership

- Closed the remaining release-record gaps from the prior release before
  continuing engine-boundary stabilization.
- Reviewed which critical invariants were still CLI-owned versus protected in
  lower layers.
- Added narrow engine-level invariant ownership evidence without widening the
  active command surface.
- Preserved observable CLI behavior, JSON behavior, backend behavior, and
  repository/storage formats.
- Kept snapshot mutation and repair/recovery routing decisions out of scope.

------------------------------------------------------------------------

## v1.13.4 - 2026-06-13 — Read-Side Dependency Direction and Result Ownership Review

- Reviewed read-side ownership across engine, observability, snapshot, renderer,
  and CLI seams.
- Clarified result ownership and dependency direction for active read-side
  surfaces without claiming broader routing completion.
- Preserved current human output, JSON output, exit behavior, and backend
  semantics.
- Refreshed compatibility evidence while keeping snapshot mutation and broader
  routing changes out of scope.
- Recorded remaining provisional seams honestly instead of overclaiming unified
  ownership.

------------------------------------------------------------------------

## v1.13.3 - 2026-06-12 — Read-Side Contract Cleanup

- Cleaned up the active read-side contract surface around stats, inspect,
  snapshot show, and snapshot diff.
- Reduced misleading wrapper-shaped seams while keeping public behavior stable.
- Preserved current query behavior, renderer expectations, and backend
  compatibility posture.
- Kept mutating operations and snapshot mutation routing out of scope.
- Established a clearer base for later read-side ownership review work.

------------------------------------------------------------------------

## v1.13.2 - 2026-06-08 — Engine Error Taxonomy Baseline

- Established the initial engine error taxonomy baseline for active and
  near-active engine operations.
- Clarified unsupported-operation versus invariant-failure versus passthrough
  error meaning.
- Preserved active routing, JSON behavior, exit codes, and backend behavior.
- Avoided broad routing expansion while preparing later contract cleanup.
- Improved the precision needed for future daemon/API-safe contract shaping.

------------------------------------------------------------------------

## v1.13.1 - 2026-06-07 — Explicit Deferred-Contract Boundaries

- Converted implicit deferred seams into explicit unsupported or
  candidate-only boundaries.
- Clarified which engine/catalog paths were active versus intentionally
  incomplete.
- Preserved runtime behavior while preventing partial routes from overclaiming
  completeness.
- Kept new engine operations, new catalog implementations, and snapshot
  mutation routing out of scope.
- Established the explicit deferral baseline for the rest of the `v1.13.x`
  stabilization train.

------------------------------------------------------------------------

## v1.13.0 - 2026-06-06 — Engine Stabilization Baseline & Contract Inventory

- Established the `v1.13.x` engine-stabilization baseline as an inventory-only
  release.
- Recorded the active engine, catalog, dependency-direction, and invariant
  ownership seams without changing runtime behavior.
- Defined the release-train proposal that drives the subsequent `v1.13.x`
  sequence.
- Preserved backend behavior, storage format, repository format, JSON behavior,
  and exit-code behavior.
- Kept daemon, API, UI, and implementation work out of scope.

------------------------------------------------------------------------

## v1.12.3 - 2026-06-06 — Release Train Closure Hygiene

- Closed the `v1.12.x` release train with documentation and release-control
  hygiene updates.
- Reconciled the final release-state records and preserved the single-branch,
  single-PR release workflow.
- Prepared the transition into `v1.13.0` without changing product behavior.
- Preserved backend behavior, engine routing, storage/repository formats, and
  schema behavior.

------------------------------------------------------------------------

## v1.12.2 - 2026-06-05 — CLI Validation Follow-up

- Followed up on CLI validation seams after the main migration work landed.
- Tightened validation ownership and parity expectations without changing the
  active command set.
- Preserved CLI behavior, JSON output, exit codes, backend behavior, and
  storage/repository formats.
- Kept broad engine-boundary expansion and schema/storage changes out of scope.

------------------------------------------------------------------------

## v1.12.1 - 2026-06-04 — Post-Migration CLI Contract Hardening

- Hardened CLI contracts after the main engine/catalog migration baseline.
- Clarified post-migration validation and contract-preservation behavior.
- Preserved runtime semantics, backend behavior, and repository/storage
  formats.
- Kept broader architectural expansion out of scope while stabilizing the
  migrated surfaces.

------------------------------------------------------------------------

## v1.12.0 - 2026-06-02 — Engine/Catalog Boundary Migration Baseline

- Established the engine/catalog boundary migration baseline for the `v1.12.x`
  line.
- Migrated selected orchestration seams while preserving behavior first.
- Fixed database-ownership and boundary-shaping issues needed for the next
  release steps.
- Preserved CLI behavior, JSON output, exit codes, storage format, and
  repository format behavior.
- Left later validation hardening and closure hygiene to `v1.12.1`-`v1.12.3`.

------------------------------------------------------------------------

## v1.11.0 - 2026-05-31 — Behavior-Preserving Engine Facade Baseline

Introduces the behavior-preserving engine facade baseline.

No observable CLI, JSON, exit-code, storage, snapshot, GC, restore, verify,
or schema behavior changed.

Changes:

- Added `internal/engine` package with `Engine` interface (`Stats`, `Inspect`,
  `Verify`). `DefaultEngine` wraps existing domain packages; no business logic
  was moved.
- Added inactive mutating operation candidates for Phase 3 (v1.12+). Not wired.
- `runObservabilityStatsPhase` in `cmd/coldkeep` now delegates through the
  engine facade. Observable output is identical.
- Dependency guard test enforces the engine/domain package direction contract.
- Test: normalized `COLDKEEP_CODEC` per codec in G6 adversarial suite,
  fixing subtest isolation when CI sets `COLDKEEP_CODEC`.
- Updated version string to `1.11.0`.

Explicit non-goals: does not route additional CLI commands through engine,
does not lift business logic, does not change DB backend, no new product features.

------------------------------------------------------------------------

## v1.10.16 - 2026-05-31 — Final v1.10.x Evidence Closure

- Updated reported version metadata to `1.10.16`.
- Closed stale v1.10.15 post-publication release evidence.
- Marked v1.10.15 Phase 6 as released / complete.
- Recorded v1.10.15 PR, CI, merge, tag, and GitHub release evidence.
- Declared v1.10.x complete after v1.10.16.

No runtime behavior, storage behavior, restore behavior, verify behavior, GC
behavior, snapshot behavior, schema behavior, migration behavior, JSON
behavior, CI, scripts, dependencies, engine behavior, or catalog behavior
changed.

v1.11 may begin as behavior-preserving engine facade work under the existing
v1.10.14 handoff rules.

------------------------------------------------------------------------

## v1.10.15 - 2026-05-31 — Release Metadata Reconciliation

Metadata-only corrective release. Reconciles repository metadata after
the v1.10.14 release and closes v1.10.x definitively before v1.11 starts.

Changes:

- Updated reported version to `1.10.15`.
- Reconciled v1.10.14 Phase 7 release evidence (PR #74, merge commit
  19e5471, CI run 26696932318, tag v1.10.14, published 2026-05-30T23:05:39Z)
  into repository documents; Phase 7 was completed externally at publication
  time but remained marked pending inside the repository.
- Added CHANGELOG entries for v1.10.14 and v1.10.15.
- Documented the v1.10 release-notes source-of-truth convention.
- Documented `COLDKEEP_STRICT_RECOVERY` operator setting in README.
- Fixed pre-existing test infrastructure bugs found during pre-release
  checklist execution: `gc_step11_test.go` and `gc_test.go` packed-block
  fixtures missing `physical_hash` column; `tests/utils/common.go`
  `ResetDB` TRUNCATE list missing `snapshot`, `snapshot_file`,
  `snapshot_path`, and `physical_file` tables.

No storage behavior, restore behavior, verify behavior, GC behavior,
snapshot behavior, schema behavior, migration behavior, CLI behavior
beyond the reported version output, JSON behavior, CI, scripts, or
dependencies changed.

------------------------------------------------------------------------

## v1.10.14 - 2026-05-30 — Release Evidence Closure and v1.11 Handoff

Documentation and release-control release only. No runtime behavior,
source code, tests, CI, scripts, or dependencies changed.

Changes:

- Closed stale v1.10.12 release evidence.
- Recorded v1.10.13 final release evidence.
- Updated README roadmap state for v1.10.x closure.
- Updated CHANGELOG through v1.10.13.
- Clarified the v1.10 issue tracker source of truth.
- Documented Codacy false-positive handling and the RAC_* SQL pattern
  decision (non-applicable to Coldkeep).
- Created the v1.11 handoff gate with wrapper-first and parity-before-
  lifting rules.
- Created the v1.10.14 final release gate.

No runtime behavior, CLI behavior, JSON behavior, storage format,
repository format, schema behavior, migration behavior, engine work,
or catalog work changed.

------------------------------------------------------------------------

## v1.10.13 - 2026-05-30 — Post-Release Correctness Hardening

Fixes-only release closing 10 correctness gaps found in the adversarial
post-release audit of v1.10.12. No behavior, CLI, JSON, storage format,
or repository format changes beyond the specific invariants hardened below.

Highlights:

- V1: checkContainersFileExistence extended to cover storage_blocks-referenced
  containers (packed verify gap: missing container no longer silently skipped)
- V2: verifyPhysicalPayloadStage and verifyCompressedPayloadStage fail-closed
  on NULL hash for non-legacy packed blocks (NULL hash no longer bypasses
  payload verification stages)
- G4: GC sealed container scan adds sealing=FALSE guard (containers actively
  being sealed are now correctly excluded from GC eligibility)
- G2: FK violation on container delete returns actionable GC diagnostic instead
  of raw DB error
- V3: UNIQUE(container_id, container_offset) added to storage_blocks with
  migration preflight duplicate check (schema version 16)
- S1: snapshotSourceQuery adds lf.status=COMPLETED filter (incomplete logical
  files are excluded from snapshot capture)
- S2: snapshotAncestorCycleExists added; snapshot parent creation rejects
  cyclic parent chains (A→B→A), not only self-references
- X1: ApplySQLiteSessionPragmas now also sets PRAGMA foreign_keys=ON (FK
  constraints enforced on all SQLite connections)
- G1 (modified): SQLite live GC now fails-closed with a clear error; SQLite
  dry-run GC is allowed (GC singleton invariant preserved on SQLite)
- C2: runRemoveCommand passes perf spans to all emitBatchCommandReport("remove")
  call sites; remove --output json now includes perf_spans array
- C1 excluded: duplicate singleton flag guard already hard-rejects in v1.10.12

Regression tests added for each fix.

Upgrade note: schema migration to version 16 runs automatically on first open and aborts if
duplicate `(container_id, container_offset)` pairs exist in `storage_blocks`.

------------------------------------------------------------------------

## v1.10.12 — Engine Boundary Readiness

- Documented CLI/business-logic coupling.
- Documented direct DB, filesystem, and storage-context access.
- Documented operation contract candidates.
- Documented invariant ownership and no-behavior-change migration rules.
- Prepared v1.11 transition checklist.

------------------------------------------------------------------------

## v1.10.11 — Stabilization & Regression Burn-down

- Completed v1.10 stabilization and regression burn-down before behavior freeze.
- Closed remaining S0/S1 issues; performed suppression, accepted-risk, and deferred-risk audit.
- Validated long-run, adversarial, and regression suites.
- Documented final v1.10 behavior freeze evidence and known issues after v1.10.

------------------------------------------------------------------------

## v1.10.10 — Cross-Platform Validation

- Validated path normalization, restore determinism, and symlink policy across Linux, macOS, and Windows.
- Captured cross-platform CI evidence in release documentation.

------------------------------------------------------------------------

## v1.10.9 — Filesystem Fault Injection Phase 1

- Introduced deterministic scripted fault filesystem for test-only use.
- Added fault injection for ENOSPC, write failure, partial write, fsync failure, rename, and remove.
- Proved correctness-critical paths fail safely under deterministic fault injection without silent corruption.

------------------------------------------------------------------------

## v1.10.8 — Filesystem Abstraction Groundwork

- Inventoried filesystem operations and classified critical seams.
- Introduced behavior-preserving filesystem abstraction seams.
- Added equivalence tests proving OS-backed behavior parity.

------------------------------------------------------------------------

## v1.10.7 — Critical-Path Coverage Gates

- Established critical-path coverage visibility for correctness-critical packages.
- Added invariant-to-coverage mapping and soft threshold policy.
- Added coverage baseline capture and regression prevention gate design.

------------------------------------------------------------------------

## v1.10.6 - 2026-05-22 —  CI / Codacy / Copilot Workflow Hardening

- Added CI-specific Copilot instructions.
- Added critical-path coverage prompt.
- Defined Codacy passive-mode policy.
- Defined scanner suppression / acceptance policy.
- Defined CI/Codacy release gate boundaries.
- Reviewed CI workflow delta options and kept CI workflow behavior unchanged.
- Recorded local validation and workflow consistency evidence.
- Confirmed `go vet ./...`, `go test ./... -count=1`, and `go test -race ./... -count=1` passed.
- No product behavior, CI workflow behavior, Codacy configuration, dependency, storage, restore, GC, verify, repair, CLI, or JSON contract changes intended.

------------------------------------------------------------------------

## v1.10.5 - 2026-05-20 — Release Validation Debt Hardening

Fixes-only release that consolidates validation debt from v1.10.x development: gosec baseline triage, Codacy finding workflow hardening, full-suite timeout investigation, local-vs-CI validation boundary clarification, regression matrix, tracker/matrix closure accounting, and full local PRE_RELEASE_CHECKLIST.md execution.

Highlights:

- gosec baseline triage completed: 114 findings classified, 0 direct closures, 2 inherited from v1.10.4
- Codacy finding workflow hardened: PR workflow, evidence format, and merge-blocking rules defined
- full-suite timeout at `TestConcurrentStoreMultiChunkFilesAtomicCompletion` investigated and classified as documented boundary
- local-vs-CI validation boundary clarified: Phase 5 policy for local failure classification and CI authority
- validation regression matrix constructed: Phase 8 matrix covering Phases 1–7 evidence
- tracker/matrix closure accounting completed: 2 inherited closure rows from v1.10.4, 13 linked issues covered
- full local PRE_RELEASE_CHECKLIST.md execution recorded: all active sections executed or classified
- no production behavior, test behavior, CI workflow, or dependency changes introduced in release-candidate documentation phases

------------------------------------------------------------------------

## v1.10.4 - 2026-05-18 — GC Reachability & Deletion Correctness

Fixes-only release focused on GC reachability/deletion correctness evidence consolidation, tracker/matrix closure finalization for selected v1.10.4 rows, and release-candidate documentation readiness.

Highlights:

- evidence chain consolidated across Phases 1-9 for included v1.10.4 scope
- closure outcomes finalized: CK-110-M014 fixed, CK-110-M036 fixed, CK-110-M079 fixed (35 linked issue rows closed)
- local validation boundary recorded (`go vet`, targeted package tests, targeted race tests, targeted GC/snapshot/retention/repair/report/parity/concurrency suite all pass)
- known local full-suite timeout at `TestConcurrentStoreMultiChunkFilesAtomicCompletion` remains documented
- no production behavior, test behavior, CI workflow, or dependency changes introduced in release-candidate documentation phases

------------------------------------------------------------------------

## v1.10.3 - 2026-05-17 — Packed Storage Metadata Integrity

Fixes-only release focused on packed-storage metadata integrity evidence, tracker/matrix closure accounting for selected v1.10.3 rows, and release-candidate documentation readiness.

Highlights:

- packed metadata invariant and trust-boundary evidence consolidated across Phases 1-7
- corruption regression matrix validation and compatibility guardrail evidence confirmed
- closure outcomes finalized: CK-110-M035 accepted, CK-110-M060 deferred, CK-110-M061 deferred
- local validation and CI-equivalent simulation recorded (targeted packed tests, `go vet`, `go test`, `go test -race`, `govulncheck`)
- no production behavior, test behavior, CI workflow, or dependency changes introduced in this release phase

------------------------------------------------------------------------

## v1.10.2 - 2026-05-16 — Validation & Security Hardening

Hardening release focused on path-safety centralization, container filename trust-boundary enforcement, snapshot/stored-path traversal rejection, restore destination and symlink safety, temp/rename cleanup safeguards, environment/config parsing validation, PostgreSQL DSN escaping, toolchain CVE remediation, and benchmark script argument validation for security-sensitive paths.

------------------------------------------------------------------------

## v1.10.1 - 2026-05-16 — CLI Correctness & Contract Stabilization

Stabilization release focused on strict CLI validation, deterministic malformed-input behavior, JSON output contracts, pre-stateful validation ordering, and closure of selected v1.10.1 tracker/matrix rows.

------------------------------------------------------------------------

## v1.10.0 - 2026-05-15 — Baseline & Freeze Declaration

### Status

Reliability freeze and stabilization baseline.

### Summary

v1.10.0 starts the Reliability Freeze, CI Hardening, and Correctness Burn-down release train.

This release establishes the baseline, evidence inventory, issue tracking model, remediation matrix, CI baseline, release gates, Codacy policy, toolchain vulnerability plan, and initial S0/S1 candidate review for the v1.10 stabilization series.

### Added

- v1.10 feature freeze declaration
- v1.10 release documentation workspace
- frozen baseline evidence under `docs/release/v1.10/evidence/`
- evidence manifest and SHA256 checksums
- S0-S4 severity model, status lifecycle, domain labels, risk labels, release-target labels, and decision labels
- frozen raw issue tracker and remediation matrix schemas
- Codacy baseline findings imported into `issue-tracker.csv`
- external audit findings imported into `issue-tracker.csv`
- root-invariant remediation matrix construction
- current CI and local validation baseline capture
- v1.10 release gate definition
- Go/toolchain vulnerability handling plan
- Codacy usage, blocking, and suppression policy
- initial S0/S1 candidate review completion
- v1.10.0 release-candidate validation record

### Changed

- No production behavior changes.
- No repository format changes.
- No CI enforcement changes.
- No Codacy hard-blocking changes.

### Notes

v1.10.0 is a baseline/freeze release.

It does not remediate the imported findings. Remediation begins in v1.10.1.

------------------------------------------------------------------------

## [1.9.0] - 2026-05-09

### Added (v1.9 transform architecture freeze)

- Block-level compression support with metadata-driven read behavior (`none`, `zstd`) and store-if-smaller policy.
- Explicit transform/hash semantics documentation: logical, compressed, and physical payload layers.
- Staged verification model with explicit failure-stage classification (physical, decrypt, compressed hash, decompress, logical hash, decode, chunk refs, snapshots).
- Repository capability modeling (`internal/repository/capabilities`) for explicit supported vs observed storage features.
- Frozen v1.9 storage semantics references for engine extraction (`docs/STORAGE_SEMANTICS_v1.9.md`, engine quick reference, ADR set).

### Changed (v1.9 transform architecture freeze)

- Read path and verify behavior are formally metadata-driven and mixed-repository safe.
- Compression defaults are write-policy only; existing historical blocks are never auto-rewritten.
- Benchmark baseline policy is frozen under `benchmarks/v1.9/` with explicit regression-threshold contracts.

### Compatibility (v1.9 transform architecture freeze)

- v1.9 reads v1.7 and v1.8 repositories without forced rewrite or recompression.
- Mixed repositories (legacy + packed + compressed + encrypted) remain supported steady-state.
- v1.7 is not guaranteed to read repositories containing newer v1.8/v1.9 packed metadata.
- Missing PostgreSQL schema requires manual schema application or `COLDKEEP_DB_AUTO_BOOTSTRAP=true`; older schemas auto-upgrade to required v15 at startup.

### Not included (v1.9 transform architecture freeze)

- No automatic background rewrite/recompression tooling.
- No storage redesign beyond frozen v1.9 semantics (v1.10 is extraction-focused).

------------------------------------------------------------------------

## [1.8.0] - 2026-05-07

### Added (v1.8 packed-block transition)

- v1.8 packed block abstraction.
- Multiple chunks per storage block (`storage_blocks` + `chunk_block_refs`).
- Mandatory block hash validation for packed-block integrity.
- Packed-block-aware `verify`, `gc`, and `restore` behavior.
- AES-GCM packed-block integration: `COLDKEEP_CODEC=aes-gcm` now works end-to-end with packed writes; per-chunk nonce and codec metadata are tracked in companion `blocks` rows.
- Block-layout stats and benchmarking documentation updates.

### Changed (v1.8 packed-block transition)

- New writes use the packed block layout.
- Default packed block target size is 1 MiB.
- `COLDKEEP_BLOCK_TARGET_SIZE_MB` remains available as an advanced/operator write-time tuning override.
- `COLDKEEP_CODEC=aes-gcm` now fully applies to packed-block writes: `storage_blocks.codec = "aes-gcm"`, stored bytes = 12-byte nonce prefix + AES-GCM ciphertext of the encoded block; the read path decrypts transparently.

### Compatibility (v1.8 packed-block transition)

- v1.8 reads v1.7 repositories.
- Mixed legacy/packed repositories are supported.
- v1.7 is not guaranteed to read v1.8 repositories.
- Existing v1.7 data is not automatically rewritten during upgrade.
- Missing PostgreSQL schema requires manual schema application or
  `COLDKEEP_DB_AUTO_BOOTSTRAP=true`. Existing older schemas are auto-upgraded
  to the required v12 schema at startup.

### Not included (v1.8 packed-block transition)

- Block-level compression is not included; it is planned for v1.9.

------------------------------------------------------------------------

## [1.7.0] - 2026-05-02

Deterministic Performance Foundation milestone.

v1.7 improves performance through controlled execution and conservative
measurement-guided cleanup. It is not a fully concurrent daemon release.
Restore determinism, GC safety, and snapshot semantics remain preserved, and
this milestone does not introduce a storage-format change or schema-breaking
change.

### Release highlights (v1.7)

- **Execution options** — benchmark runs now expose and document execution
  controls such as configured workers, effectively used workers, pipeline
  depth, and deterministic mode so comparisons stay explicit and repeatable.
- **Store-folder worker hardening** — `store-folder` worker handling was
  tightened for safer multi-worker operation, including explicit worker-path
  support and clearer behavior under parallel load.
- **Store prepare/commit split** — store execution now separates deterministic
  chunk preparation from the commit phase so CPU-side work happens before DB
  mutation and publish boundaries remain explicit.
- **Single-pass preparation** — prepared store paths now carry the metadata
  and chunk payload information needed for commit without redundant re-hashing,
  re-reading, or duplicate metadata construction.
- **Restore recipe/cache/buffer work** — restore-path cleanup kept replay
  recipe ordering explicit, hardened restore-local reader-cache lifecycle, and
  made buffered-writer finalization/close behavior safer on success and error
  paths.
- **Stats/inspect query cleanup** — read-only observability paths were cleaned
  up to remove N+1 aggregation patterns and replace them with more set-based
  query shapes where measured gains were demonstrated.
- **Conservative I/O counters/buffering** — v1.7 adds measured I/O-path
  cleanup with operation-scoped counters, conservative buffering, and explicit
  durability boundaries rather than high-risk write-path weakening.

### Added (v1.7)

- **Benchmark infrastructure** — `coldkeep benchmark run` with eight built-in
  scenarios (store, restore, snapshot, GC, stats), deterministic dataset
  generation, and JSON/table output.
- **Restore-tree determinism check** — `validateBenchmarkDeterminism` now
  verifies that `store → restore → SHA-256(bytes)` produces an identical
  `relative-path → digest` map across independent runs, proving user-visible
  restore output is byte-for-bit stable.
- **v1.6 baseline recorded** — `benchmark-baseline.json` captures the eight
  scenario results on the small dataset before v1.7 optimizations.
- **CI benchmark job** — runs the small dataset on every push; result uploaded
  as a `benchmark-baseline` artifact for trend tracking.
- **Benchmark documentation** — see [docs/benchmarking.md](docs/benchmarking.md).

### Compatibility notes (v1.7)

- No storage format change.
- No schema-breaking change.
- Migration required: none.
- Restore remains deterministic and byte-identical.
- GC safety model remains conservative and reference-safe.
- Snapshot creation, retention, diff, and restore semantics remain unchanged.

------------------------------------------------------------------------

## [1.6.0] - 2026-04-28

Observability and simulation contract hardening milestone.

v1.6 formalizes read-only observability commands, exact GC simulation behavior,
and trace output channel conventions for both human operators and tooling.

Validation note: release sign-off included v1.6 observability/simulation
checklist execution, CI-parity quality checks, full tests/integration runs, and
smoke matrix verification across plain and aes-gcm codecs.

### Release highlights (1.6.0)

- **Read-only observability command surface** — `coldkeep stats`,
  `coldkeep inspect <entity> <id>`, and `coldkeep simulate gc` are documented
  as read-only operations.
- **Exact GC simulation parity** — `simulate gc` reflects the same reclaimability
  decisions as GC preflight/liveness evaluation without executing deletion.
  It uses the shared GC planning layer (`gc.BuildPlan`) to preview actual GC
  reclaimability semantics (including fully-dead active containers), not legacy
  `gc --dry-run` behavior.
- **Known v1.7 optimization follow-up** — stats graph byte aggregation currently
  uses per-chunk lookup (`sumChunkSizesByID`) and is slated for batched
  optimization in v1.7.
- **Tooling-oriented output contracts** — JSON output and trace channels are
  documented for automation use, including `--trace-json` JSONL diagnostics.
- **Stable diagnostics channel routing** — trace diagnostics (`--trace`,
  `--trace-json`) are emitted on stderr so stdout payloads remain stable for
  piping and tooling.
- **Operator safety wording** — docs now explicitly state that simulation does
  not mutate database state or filesystem state.

### Added (1.6.0)

- **Observability command documentation** in [README.md](README.md):
  - `coldkeep stats`
  - `coldkeep stats --json`
  - `coldkeep inspect <entity> <id>`
  - `coldkeep inspect ... --relations`
  - `coldkeep inspect ... --reverse`
  - `coldkeep inspect ... --deep --limit N`
  - inspect entity coverage: `file` (`logical-file` alias), `chunk`, `container`, `snapshot`
  - `coldkeep simulate gc`
  - `coldkeep simulate gc --delete-snapshot <id>`
  - `coldkeep simulate gc --containers`
  - `--trace` / `--trace-json`
- **Observability guarantees section** documenting:
  - read-only command behavior
  - exact simulation semantics
  - explicit non-mutation during simulation
  - JSON output intended for tooling
  - deep inspect sizing guidance (`--limit`)

### Changed (1.6.0)

- Updated [PRE_RELEASE_CHECKLIST.md](PRE_RELEASE_CHECKLIST.md) documentation
  checklist to include v1.6 observability/trace contract checks and release-note
  alignment criteria.
- Updated [README.md](README.md) roadmap note to reflect v1.6 completion status
  and post-v1.6 focus areas.
- Updated [ARCHITECTURE.md](ARCHITECTURE.md) contract framing to formalize
  Phase 8 observability/simulation guarantees and stderr trace-channel behavior.

### Scope alignment (v1.6)

- Observability commands are contractually read-only.
- GC simulation is exact and non-mutating.
- Trace diagnostics are emitted on stderr to preserve stdout stability.
- JSON output is designed for machine tooling and automation pipelines.
- Deep inspect traversal may be large; bounded output via `--limit` is the
  recommended operator pattern.

------------------------------------------------------------------------

## [1.5.0] - 2026-04-25

Compatibility-contract and chunker-evolution clarity milestone.

v1.5 introduces CDC evolution through chunker versioning, FastCDC support,
explicit chunker configuration, observability, and benchmark validation while
preserving compatibility with existing repositories.

### Release highlights (1.5.0)

- **CDC evolution with compatibility preserved** — repositories can safely
  contain mixed `v1-simple-rolling` and `v2-fastcdc` write history, while
  restore remains metadata-replay and byte-identical for persisted data.
- **FastCDC as the new-repo default write policy** — fresh v1.5+ repositories
  initialize with `v2-fastcdc`; upgraded repositories preserve their prior
  default unless explicitly changed by the operator.
- **Explicit chunker policy controls** — `coldkeep config set default-chunker`
  is now clearly documented and CLI-described as affecting only future writes,
  with no automatic re-chunking of historical data.
- **Observability for mixed-version operation** — docs and contracts now make
  chunker provenance and version-distribution visibility explicit through
  existing stats/verify/doctor surfaces.
- **Benchmark validation guidance for chunker decisions** — chunker benchmark
  commands and interpretation notes are documented so boundary/reuse behavior
  can be evaluated with deterministic inputs before policy changes.

### Added (1.5.0)

- **Compatibility contract document** — added [COMPATIBILITY.md](COMPATIBILITY.md) as the
  canonical source for guarantees, non-guarantees, versioning rules, and
  upgrade behavior.
- **Explicit guarantee set (G1-G6)** — restore correctness across chunker
  versions, snapshot stability, no automatic data migration, mixed-version
  coexistence safety, deterministic chunking per version, and forward-compatible
  restore metadata handling.
- **Explicit non-guarantee set** — cross-version dedup efficiency, stable
  cross-version boundaries, and automatic optimization/re-chunking are
  intentionally not contractual guarantees.
- **Common mistakes guidance** — added contract-authoring guardrails to avoid
  overpromising implementation outcomes and to keep guarantees separate from
  implementation details.

### Changed (1.5.0)

- **README high-level contract summary** — updated [README.md](README.md) with
  concise operator-facing sections for chunking model, chunker versions,
  config behavior, and high-level safety guarantees.
- **Architecture deep contract model** — updated [ARCHITECTURE.md](ARCHITECTURE.md)
  with detailed chunking/versioning model, guarantee deep-dive, and explicit
  boundary guidance between stable guarantees and evolving implementation
  behavior.
- **CLI help safety wording** — `config set default-chunker` help now states:
  "Affects only new stored data. Existing data is not modified."

### Scope alignment (v1.5)

- Restore remains recipe-driven; runtime chunker selection is not used to
  reconstruct already persisted files.
- Fresh v1.5+ repositories default new writes to `v2-fastcdc`; upgraded
  repositories preserve prior write default (`v1-simple-rolling` unless
  explicitly changed).
- Chunker default changes affect only future writes.
- Mixed-version repositories are first-class and expected.
- Cross-version chunk reuse is opportunistic under content identity and
  integrity constraints; reuse efficiency is intentionally non-guaranteed.
- Documentation contract language is aligned with implementation behavior and
  test coverage.

------------------------------------------------------------------------

## [1.4.1] - 2026-04-21

Recovery and release-readiness hardening patch.

v1.4.1 does not change the v1.4 snapshot model. It hardens recovery behavior,
snapshot capture safety under churn, and release-validation ergonomics.

### Recovery hardening

- Strict recovery now converges safely on a known edge case: preexisting
  quarantined orphan container rows with stale size metadata are resynchronized
  instead of forcing strict-recovery abort.
- Quarantine metadata synchronization now aligns with physical file size when
  containers are quarantined directly.
- Strict recovery remains conservative and correctness-first, while reducing
  unnecessary blocking for unrelated healthy data during restart.

### Snapshot safety under churn

- Snapshot source enumeration on PostgreSQL now uses stronger locking during
  snapshot capture.
- This reduces metadata-race risk during heavy create/remove churn.
- The v1.4 contract is preserved: snapshots remain self-contained and
  restore-safe.

### Validation and adversarial coverage

- Added adversarial coverage for preexisting quarantined-orphan
  size-drift resynchronization behavior.
- Updated validation-matrix evidence mapping so release evidence is tied more
  explicitly to behavior contracts.

### Documentation and release-readiness guidance

- Pre-release guidance was clarified around prerequisites, storage cleanup, and
  DB/storage alignment.
- Contributor guidance now better separates first-contribution workflow from
  full release-validation workflow.
- Validation docs/scripts were aligned with clearer matrix title/wording.
- Changelog, architecture, and overview docs were cross-linked more
  consistently.

### Local validation scope (v1.4.1)

- quality-equivalent checks
- validation-matrix audit
- CI-enforcement local audit
- package tests across plain and `aes-gcm`
- smoke validation across both codecs
- adversarial coverage reruns during hardening work

### Correctness impact and non-changes

Preserved from v1.4:

- snapshots remain self-contained
- restore remains deterministic and byte-identical
- snapshot deletion remains metadata-only
- GC safety model remains conservative
- recovery remains corrective and correctness-first

Not changed in v1.4.1:

- no feature-surface expansion
- no snapshot-lineage semantic change
- no restore dependency model change
- no retention/GC contract relaxation

------------------------------------------------------------------------

## [1.4.0] - 2026-04-19

Snapshot clarity and release hardening milestone.

v1.4 keeps the v1.3 retention model and makes the operator contract explicit:
snapshots are always self-contained; lineage metadata never creates restore
dependencies.

### Clarified

- **Self-contained snapshot contract** — snapshot content is independent of
  parent snapshot content at restore time.
- **`--from` lineage semantics** — `--from` records metadata for lineage
  analysis only; it does not create dependency chains.
- **Lineage tree interpretation** — `snapshot list --tree` is a metadata view,
  not an execution dependency graph.
- **Delete dry-run impact wording** — delete preview explicitly describes
  metadata/reference impact rather than guaranteed disk-space reclamation.
- **Release-gate guidance** — pre-release docs emphasize parent-delete +
  child-restore checks to validate lineage independence.

### Scope alignment (v1.4)

- No behavioral regression against the v1.3 snapshot retention safety model.
- Lineage remains informational only by design.
- Snapshot delete remains metadata-only.

------------------------------------------------------------------------

## [1.3.0] - 2026-04-18

Snapshot-layer and retention contract establishment.

v1.0 established storage correctness.
v1.1 established interface correctness.
v1.2 established physical-graph coherence.
v1.3 establishes snapshot-based retention as a correctness layer: the system
captures immutable point-in-time views, protects snapshot-retained content from
GC deletion, and audits persisted snapshot reachability as part of standard
verification and health reporting.

### Added (1.3.0)

- **`snapshot` and `snapshot_file` tables** — new schema objects (schema version 7) for immutable point-in-time captures
- **`snapshot create [--id ID] [--label LABEL] [--from PARENT_ID] [paths…]`** — full snapshot (all current files) or partial (filtered paths/prefixes); `--from` stores lineage metadata only and is currently full-to-full only
- **`snapshot list [--type full|partial] [--limit N] [--since DATE]`** — list snapshots with filtering and ordering
- **`snapshot show <snapshotID> [--limit N] [query filters…]`** — inspect snapshot contents with query support
- **`snapshot stats [snapshotID]`** — report snapshot retention pressure and metadata
- **`snapshot restore <snapshotID> [paths…] [--mode original|prefix|override] [--destination DIR]`** — restore full or partial from snapshot
- **`snapshot diff <base-ID> <target-ID> [--filter added|removed|modified] [query filters…]`** — classify changes between snapshots
- **`snapshot delete <snapshotID> --force`** — remove snapshot metadata only (logical content preserved)
- **Snapshot query semantics** — unified SnapshotQuery across show/restore/diff with exact path, prefix, glob pattern, regex, size window, and modified-time window criteria (ANDed)
- **Snapshot-aware retention model** — logical files are retained by union of current-state physical mappings and snapshot references; GC eligibility changes only after snapshot delete
- **Stats retention visibility** — global and per-snapshot stats expose retained-only-by-current, retained-only-by-snapshot, shared-by-both, and total snapshot-retained metrics
- **Verify snapshot reachability** — standard verify checks for orphan snapshot_file rows, invalid lifecycle states, and missing chunk graphs in snapshot-retained files
- **Doctor snapshot-retention context** — text and JSON reports surface snapshot-retention integrity counters alongside physical mapping counters
- **G14–G17 guarantees** — snapshot-retained GC safety, snapshot deletion metadata-only semantics, stats retention visibility, and verify/doctor snapshot reachability audits

### Scope alignment (v1.3)

- Snapshot command surface is in scope: `snapshot create`, `snapshot restore`,
  `snapshot list`, `snapshot show`, `snapshot stats`, `snapshot diff`,
  `snapshot delete --force`.
- Snapshot query semantics are part of the contract surface across
  `snapshot show`, `snapshot restore`, and `snapshot diff`:
  exact path, prefix, glob pattern, regex, size window, and modified-time
  window criteria with AND semantics.
- `snapshot diff` filtering semantics are explicit and stable:
  `--filter added|removed|modified` reduces returned entries and summary counts
  to the selected classification.
- Snapshot delete semantics are explicit and stable:
  delete removes snapshot metadata (`snapshot` + `snapshot_file`) only;
  underlying logical files/blocks are not directly removed by snapshot delete.

### Snapshot-retention / GC contract alignment

- Snapshot-retained content remains GC-ineligible until the retaining snapshot
  is deleted.
- Removing current-state mappings alone does not make snapshot-retained content
  collectible.
- GC eligibility transition is expected only after snapshot delete.

### Validation and release-gate alignment

- [VALIDATION_MATRIX.md](VALIDATION_MATRIX.md) is expected to list and cover G14-G17 for v1.3.
- v1.3 release gating explicitly includes:
  - test surface coverage (package/integration/adversarial/smoke)
  - documentation/release checklist consistency ([README.md](README.md) + [VALIDATION_MATRIX.md](VALIDATION_MATRIX.md))
  - manual snapshot/retention lifecycle gate in [PRE_RELEASE_CHECKLIST.md](PRE_RELEASE_CHECKLIST.md)
- Manual snapshot lifecycle gate examples are aligned to CLI contracts:
  use `snapshot create --id <snapshotID>`, use positional snapshot IDs for
  `snapshot restore`/`snapshot diff`/`snapshot delete`, and use
  `remove --stored-path` for current-state mapping removal.

### Post-v1.3 hardening backlog (non-blocking)

- Add fuzz coverage for snapshot query combinator cases (`regex` + `pattern` +
  `prefix`) as a future hardening task.

------------------------------------------------------------------------

## [1.2.0] - 2026-04-11

Physical-file layer and operator semantics milestone.

v1.0 established storage correctness.
v1.1 established interface correctness.
v1.2 establishes physical-graph coherence: the system now knows and audits
*where* each logical file lives, can repair drifted reference counts explicitly,
and refuses GC when the physical root graph is inconsistent.

### Added (1.2.0)

- **`physical_file` table** — new schema object (schema version 6) that maps each
  current-state filesystem path to its owning `logical_file`. Managed by the
  `physical_file_repository` layer.
- **`remove --stored-path <path>`** — removes one physical-path mapping and
  decrements `logical_file.ref_count`. Emits `remaining_ref_count` in JSON output.
- **`remove --stored-paths <path> [path …]`** — batch stored-path remove with
  the same deterministic batch contract as ID-based batch remove (input-order
  preservation, deduplication, fail-fast, dry-run support absent for
  stored-path per documented deferral).
- **`restore --stored-path <path>`** — restore a file by its stored path, with
  optional `--mode` (`original` / `prefix` / `override`) and `--destination`.
- **`repair ref-counts [--output json]`** — explicit operator command that
  recomputes `logical_file.ref_count` from `physical_file` rows. Reports
  `updated_logical_files` and `scanned_logical_files`.
- **`repair --batch [--input <file>] [--fail-fast]`** — batch maintenance layer
  for `repair` operations (currently `ref-counts`), following the same batch
  contract as `restore` and `remove`.
- **Physical graph audit in `verify system`** — standard verify now checks:
  - orphan `physical_file` rows whose `logical_file_id` points nowhere
  - `logical_file.ref_count` drift relative to `COUNT(physical_file rows)`
  - impossible negative `logical_file.ref_count` states
  - Failures carry machine-readable `invariant_code` and `recommended_action`.
- **Audited GC root gate** — `gc` (including dry-run) runs
  `CheckPhysicalFileGraphIntegrity` as a mandatory pre-flight after acquiring the
  advisory lock. A drifted root graph refuses GC with `GC_REFUSED_INTEGRITY`.
- **Typed invariant taxonomy** (`internal/invariants`) — stable, machine-readable
  error codes for physical graph failures: `PHYSICAL_GRAPH_ORPHAN`,
  `PHYSICAL_GRAPH_REFCOUNT_MISMATCH`, `PHYSICAL_GRAPH_NEGATIVE_REFCOUNT`,
  `GC_REFUSED_INTEGRITY`, `REPAIR_REFUSED_ORPHAN_ROWS`.
- **`invariant_code` + `recommended_action` in JSON error payloads** — all
  physical-graph verify failures and GC/repair refusals now include advisory
  metadata in both JSON (`invariant_code`, `recommended_action`) and text output.
- **`stored_path` field in store JSON output** — `coldkeep store --output json`
  now includes `stored_path` alongside `file_id` and `path`.
- **`docs/PATH_IDENTITY.md`** — canonical reference for path identity policy
  (canonicalization, case-sensitivity, symlink behavior) in the v1.2
  `physical_file` layer.
- **G10–G13 guarantees** — post-v1.0 extensions tracked in `VALIDATION_MATRIX.md`:
  - G10: physical graph audit (orphan/ref-count coherence)
  - G11: audited GC root gate (pre-flight on drifted graph)
  - G12: stable invariant error taxonomy (machine-readable codes)
  - G13: batch maintenance reporting semantics (`execution_mode`,
    per-item `success`/`failed`/`skipped`, `recommended_action` on refusal)
- Schema migration to version 6 (`physical_file` table, index on
  `physical_file(logical_file_id)`).

### Changed (1.2.0)

- `list --output json` and `search --output json` now include `stored_path` per
  file entry when a physical mapping exists.
- `doctor` verify phase now includes the physical graph audit; a drifted graph
  causes `doctor` to exit with `PHYSICAL_GRAPH_REFCOUNT_MISMATCH`.
- `repair` extended with `ref-counts` sub-command and batch wrapper; existing
  `repair --batch` tests cover mixed-input ordering and fail-fast semantics.
- Clarified batch `restore`/`remove` CLI help: JSON `status` values are
  `ok`, `partial_failure`, `error` (overall) and `success`, `failed`,
  `skipped`, `planned` (per-item). Exit `0` when no item fails, `1` when any
  item fails, `2` for pre-execution usage/validation errors.

### Fixed (1.2.0)

- `strings.TrimLeft` path separator cutset in `restore.go` corrected to handle
  both `/` and `\` reliably.
- Unused helper types in `physical_file_repository.go` removed (lint/staticcheck
  cleanliness).

### Technical debt noted (post-v1.2)

- `BuildPlan` and `ExecutePlan` are deprecated transitional helpers and are
  candidates for removal beyond v1.2 or isolation into a dedicated legacy file.
- Dry-run support for `remove --stored-path` is deferred beyond v1.2 (rationale
  documented in [ARCHITECTURE.md](ARCHITECTURE.md) under "Dry-run Support").

### Notes (1.2.0)

- `remove --stored-path` unlinks one physical mapping at a time; the logical
  file and its data remain restorable as long as any physical mapping still
  exists. Removing all mappings does not automatically trigger GC: the logical
  file row persists with `ref_count=0` until GC runs.
- `repair ref-counts` is an explicit operator command, not an automatic
  background repair. `doctor` detects drift but does not auto-repair.
- The explicit repair boundary: `verify` detects, `doctor` recovers + detects,
  `repair ref-counts` is the only write-path for fixing drift.
- On-disk format and internal structures may evolve in future versions.

### Guarantees (1.2.0)

- Introduces G10: physical graph audit (orphan/ref-count coherence in `verify system`)
- Introduces G11: audited GC root gate (pre-flight refusal on drifted graph)
- Introduces G12: stable invariant error taxonomy (machine-readable codes)
- Introduces G13: batch maintenance reporting semantics (execution_mode, per-item isolation)

------------------------------------------------------------------------

## [1.1.0] - 2026-04-07

Interface-correctness milestone (CLI + automation layer).

v1.0 established correctness of storage internals.
v1.1 establishes correctness of interaction semantics for CLI and automation.

This release introduces a unified batch execution layer for `restore` and
`remove`, focused on deterministic behavior, structured observability, and
correctness under real-world mixed-input scenarios.

### Added (1.1.0)

- Multi-target support for `restore` and `remove` commands (multiple IDs in a
  single invocation)
- Input file ingestion via `--input <file>` for batch operations
- Dry-run mode (`--dry-run`) providing full execution planning without mutation
- Structured per-item batch reporting with explicit status classification:
  `success`, `failed`, `skipped`, and `planned`
- Preservation of raw invalid inputs via `raw_value` in JSON output (no implicit
  `id=0` fallback)
- Fail-fast execution mode (`--fail-fast`) to stop processing on first execution
  failure
- Comprehensive adversarial integration test suite for batch semantics,
  including mixed input, duplicate handling, ordering guarantees, and dry-run
  parity

### Changed (1.1.0)

- Unified CLI → batch → reporting pipeline so all inputs (valid, invalid,
  duplicates) are processed through a single deterministic execution model
- Strict input-order preservation guaranteed across parsing → planning →
  execution → reporting, including invalid and duplicate entries
- Introduced explicit batch status contract:
  - `ok`: all items succeeded
  - `partial_failure`: some items failed
  - `error`: all items failed or no execution possible
- JSON output contract improved:
  - failure items expose `error`
  - non-failure items (`success`, `skipped`, `planned`) expose `message`
- All-invalid input now produces a full structured batch report instead of a
  generic CLI error
- Dry-run output aligned with real execution semantics (same ordering, same
  targets, same output paths; only status differs)
- Duplicate targets are executed once and reported as `skipped` with explicit
  reason
- Exit-code behavior aligned with batch semantics: non-zero exit when any item
  fails while still emitting full structured results

### Notes (1.1.0)

- Batch operations are best-effort by default: failures do not prevent execution
  of other valid targets unless `--fail-fast` is used
- This release establishes a deterministic and automation-friendly CLI contract
  for batch workflows, aligned with coldkeep’s correctness-first design
- In guarantee terms, this release introduces an interface-correctness layer
  (G9): deterministic orchestration, machine-readable contract stability, and
  automation-safe partial-failure behavior

### Guarantees (1.1.0)

- Introduces G9: interface correctness for batch CLI orchestration

------------------------------------------------------------------------

## [1.0.0] - 2026-04-06

First stable correctness milestone.

This release marks the transition from experimental validation to a
correctness-defined baseline. The storage model, lifecycle semantics, and CLI
contracts are considered stable within the documented trust boundary.

v1.0 consolidates the guarantees defined in v0.9 and validated in v0.10,
establishing coldkeep as a correctness-first storage engine with deterministic
restore, verifiable integrity, and safe garbage collection.

### Added (1.0.0)

- Formalized v1.0 trust model consolidating:
  - storage correctness guarantees
  - verification semantics
  - recovery behavior expectations
- Established `doctor` as the primary operator-facing health and recovery command
- Defined CLI behavior and output contracts as stable for v1.x evolution

### Changed (1.0.0)

- Promoted validation and adversarial testing results (v0.10) to baseline
  correctness guarantees
- Frozen CLI surface and operational semantics as a v1.0 contract
- Clarified system trust boundaries and non-goals for the v1.x line

### Notes (1.0.0)

- v1.0 defines a correctness and operational baseline, not long-term
  compatibility guarantees
- On-disk format and internal structures may evolve in future versions
- coldkeep remains a research-oriented project, but with a stable and validated
  correctness model for local-first usage

------------------------------------------------------------------------

## [0.10.0] - Pre-v1.0 Validation Phase

Validation and adversarial testing phase leading into v1.0.

This release focuses on validating coldkeep's correctness guarantees under stress,
failure, and adversarial lifecycle interleavings.

The core architecture, storage model, and CLI contracts are considered stable.
This phase is dedicated to actively attempting to break system invariants and
eliminate remaining correctness risks before the v1.0 milestone.

### Reuse Integrity Hardening

- Added semantic integrity validation for completed logical-file reuse, controlled by
  `COLDKEEP_REUSE_SEMANTIC_VALIDATION` (`off` / `suspicious` / `always`; default `suspicious`)
- Hardened structural reuse acceptance: completed-file reuse now requires contiguous
  file-chunk graph integrity, completed referenced chunks, valid block metadata,
  non-quarantined containers, and on-disk container file presence before
  returning `AlreadyStored=true`
- Hardened completed-chunk reuse validation: chunk reuse now enforces block-row
  cardinality and validates container presence/quarantine state plus
  block offset/size bounds against container metadata and physical file size
- Added integration regression proving semantically corrupted completed-file reuse
  is refused and rebuild/retry paths are triggered

### Atomicity & Lifecycle Safety

- Added atomic logical-file completion boundary: the final transition to
  `COMPLETED` now verifies full chunk linkage and contiguous ordering in the same
  transaction
- Hardened rollback error handling: rollback failures are now surfaced and
  escalated instead of silently ignored, with failed append paths retiring or
  quarantining unsafe containers
- Added in-process container quarantine behavior for active/just-sealed
  container failures so unsafe containers are withdrawn immediately, not only on
  next startup recovery

### Lifecycle Hardening

- Hardened startup sealing recovery: containers in `sealing=TRUE` state are now
  quarantined (not auto-sealed) when physical file size differs from DB
  `current_size`, preventing ghost-byte containers from being promoted as healthy
- Added integration regression for append-rollback ghost-byte state: startup
  recovery quarantines the sealing container and GC (dry-run + real) skips it
- Fixed SQLite compatibility in remove path tests by falling back from
  `SELECT ... FOR UPDATE` to plain `SELECT` when the dialect does not support
  row-lock syntax

### Added (0.10.0)

- Added integration coverage for non-strict startup recovery on suspicious
  orphan-container conflicts (`COLDKEEP_STRICT_RECOVERY=false`)
- Added adversarial integration coverage for restore pin + remove + GC
  interleavings
- Added integration regression for deep verification trailing-byte detection
  after the last completed block payload
- Added `VALIDATION_MATRIX.md` to map v0.9 guarantees to concrete verify checks
  and integration evidence during the v0.10 trust-validation phase
- Added lifecycle determinism integration regression
  `TestStoreRemoveGCRestartStoreConvergesChunkGraph` to assert store/remove/GC/
  restart cycles converge to a stable chunk graph and restorable output

### Changed (0.10.0)

- Clarified verification operational contract as a recovered-state checker,
  not a live online-consistency checker during in-flight writes
- Documented `VALIDATION_MATRIX.md` in README as the maintained v0.10/v1.0
  guarantee-to-evidence contract, audited locally and enforced in CI
- Tightened validation-matrix auditing so README v0.9 guarantee summary bullets
  are counted and checked against the maintained validation contract surface
- Clarified verification mode trade-offs (`standard`, `full`, `deep`) with
  explicit cost/coverage guidance
- Elevated `coldkeep doctor` in docs/help/smoke as the recommended
  operator-facing health-check and release-gate command
- Froze `doctor` default mode as a v1.0 product contract: no-flag `doctor`
  remains the fast `standard` health gate; `--full` and `--deep` are explicit
  stronger/slow-path escalations
- Documented `doctor` as a corrective health command that may update
  metadata through recovery before running verification
- Documented startup recovery strictness as intentional fail-fast behavior,
  with a non-strict override for restart-race scenarios
- Documented contiguous offset validation as an explicit current-format
  invariant (append-only contiguous layout per container)
- Froze and documented `doctor --output json` failure contract: failures emit
  only generic CLI error JSON on `stderr` (no partial doctor report payload)
- Strengthened GC emptiness invariants and stats accounting to treat chunk
  liveness as `live_ref_count OR pin_count`, preserving restore safety under
  remove/GC interleavings

### Verification & Recovery Semantics

- Standard verification now enforces pinned-chunk integrity:
  `pin_count > 0` chunks must remain `COMPLETED` and retain block metadata
- Standard verification now also enforces completed-chunk block cardinality:
  every `COMPLETED` chunk must have exactly one `blocks` row
- Deep verification now fails when container tails contain trailing
  unaccounted bytes beyond the last completed block
- PostgreSQL startup schema guard now explicitly requires
  `schema_version >= 5`; optional first-run bootstrap remains available via
  `COLDKEEP_DB_AUTO_BOOTSTRAP=true`

### v1.0 Trust Model

- Consolidated operator trust model documentation: startup recovery is the normal
  lifecycle entry point (not exceptional maintenance), `doctor` is the recommended
  health gate and is corrective (not read-only), `verify` assumes recovered state,
  strict recovery is the production baseline, and semantic reuse validation trades
  read/CPU cost for stronger inline integrity confidence
- Made `coldkeep doctor` a named first-class v1.0 command: it is explicitly
  corrective (may abort dangling writes and clear stale sealing markers before
  verifying), is the recommended pre-ingestion, post-startup, and pre-release gate,
  and its default mode (`--standard`) is a frozen v1.0 product contract

### Tests

- Added integration regression
  `TestVerifySystemDeepDetectsAESGCMTamperedCiphertext`: stores a file with
  the `aes-gcm` codec, flips on-disk ciphertext bytes, and asserts both deep
  verify and restore reject the tampered payload
- Added integration regression
  `TestVerifySystemDeepDetectsAESGCMNonceMetadataTampering`: stores a file
  with the `aes-gcm` codec, mutates `blocks.nonce` metadata in DB, and asserts
  both deep verify and restore reject the tampered authenticated context
- Added integration regression
  `TestVerifySystemDeepDetectsAESGCMWrongKeyMismatch`: stores a file with the
  `aes-gcm` codec, verifies baseline success under the original key, then
  switches to a different valid key and asserts both deep verify and restore
  reject the mismatched-key read path
- Added integration regression
  `TestVerifySystemDeepDetectsAESGCMInvalidKeyConfiguration`: stores a file
  with the `aes-gcm` codec, then sets malformed `COLDKEEP_KEY` configuration
  and asserts both deep verify and restore fail under invalid key setup
- Added integration regression
  `TestVerifySystemDeepDetectsAESGCMInvalidHexKeyConfiguration`: stores a
  file with the `aes-gcm` codec, then sets non-hex `COLDKEEP_KEY`
  configuration and asserts both deep verify and restore fail under invalid key
  encoding
- Tightened plain-codec deep verification regressions so they store with
  explicit `plain` codec selection instead of relying on the process default,
  and `TestVerifySystemDeepDetectsChunkDataCorruption` now asserts both deep
  verify aggregate failure and restore `chunk hash mismatch` semantics
- Hardened verification fixtures and nearby full-mode regressions against
  default-codec drift: `setupStoredFileForVerification`, `TestVerifyFull`, and
  `TestVerifySystemFullDetectsNonContiguousOffsets` now store with explicit
  `plain` codec selection so DB-backed runs do not depend on process defaults
- Tightened file-level verification regressions to assert returned error
  contracts instead of generic failure only, including deep chunk corruption,
  full container truncation/missing-file cases, and standard missing-metadata /
  broken-order cases
- Tightened remaining system-level verification regressions to assert returned
  error contracts, including deep container-content mismatch aggregation and
  full-mode non-contiguous offset detection
- Tightened the remaining local verify-block regressions in `TestVerifyFull`
  and `TestVerifySystemDeepDetectsTrailingBytesAfterLastBlock` so malformed
  completed chunks, missing container files, and deep trailing-byte failures
  assert returned error-contract substrings instead of generic failure only
- Tightened `TestVerifySystemDeepAggregatesChunkErrors` to assert the returned
  aggregated deep-verification error count substring instead of generic
  failure-only behavior
- Normalized adjacent schema, seal, recovery, remove, and store failure-injection
  regressions to use shared returned-error substring assertions instead of
  manual non-nil checks, and fixed `TestStoreSealingMarkerUpdateFailureAbortsSafelyAndRecovers`
  to reach the intended rotation/sealing-marker failure path under explicit
  container sizing
- Added integration regression `TestDoctorAbortsProcessingLogicalFilesFromRecoverableState`:
  injects a dangling PROCESSING logical file, runs doctor, asserts recovery aborted
  it (`aborted_logical_files >= 1`), and confirms the PROCESSING row is now ABORTED
  and subsequent verify passes
- Added integration regression
  `TestStoreSealingMarkerUpdateFailureAbortsSafelyAndRecovers`: injects a DB
  failure on `container.sealing` transition, asserts store fails and marks file
  ABORTED without lingering `sealing=TRUE` rows, then verifies clean retry,
  restore hash equality, and full verify success
- Added stress-tier seeded randomized lifecycle regression
  `TestStoreLifecycleSeededRandomizedOperationOrder`: runs deterministic-random
  operation ordering across store/verify/gc/restore/remove loops with per-step
  integrity assertions
- Added stress-tier repeated jittered interleaving regression
  `TestRepeatedJitteredStoreGCRestoreInterleaving`: runs multi-round
  store/restore/gc interleavings with deterministic randomized start offsets and
  asserts restore correctness plus post-run invariant stability
- Added stress-tier four-way repeated interleaving regression
  `TestRepeatedJitteredStoreGCRestoreRemoveInterleaving`: runs multi-round
  store/gc/restore/remove interleavings with deterministic randomized start
  offsets and asserts victim removal plus restore/invariant stability
- Added dedicated long-run randomized soak regression
  `TestRandomizedLongRunLifecycleSoak`: runs repeated store/verify/gc/restore/
  recovery/remove cycles under deterministic randomization and is included in
  the CI long-run gate alongside `TestStoreGCVerifyRestoreDeleteLoopStability`
- Added repeated doctor convergence regression
  `TestDoctorRepeatedRecoverableStateConvergesAndPreservesLiveData`: injects
  recoverable PROCESSING logical-file and chunk rows during live workload,
  runs `doctor`, and asserts corrective counters plus preserved restore/verify
  behavior for valid data across repeated rounds
- Added recovery preservation regression
  `TestStartupRecoveryQuarantinesDamagedActiveContainerAndPreservesOtherLiveData`:
  quarantines a truncated active container, proves unrelated live data remains
  restorable, and verifies new writes avoid the quarantined container
- Added ghost-byte recovery preservation regression
  `TestStartupRecoveryQuarantinesGhostByteSealingContainerAndPreservesOtherLiveData`:
  quarantines a sealing container with ghost bytes, proves unrelated live data
  remains restorable, and verifies new writes avoid the quarantined container
- Added integration assertions for GC/restore pinning under remove/GC/restore
  interleavings
- Added integration assertion that non-strict recovery continues on suspicious
  orphan conflict states instead of aborting startup
- Added command-layer test that pins doctor JSON failure behavior to generic
  CLI error payload shape (no `command`/`data` fields)
- Added integration coverage for atomic logical-file completion and contiguous
  file_chunk ordering under both single-file and concurrent multi-chunk ingestion

------------------------------------------------------------------------

## [0.9.0] - 2026-03-31

Release hardening and delivery-gate enforcement.

This version focuses on making regressions materially harder to merge or tag by
strengthening the CI gate that enforces storage correctness guarantees.

It also formalizes the v0.9 storage guarantees model so operators and automation
can reason explicitly about validity, restore safety, and recovery behavior.

### Added (0.9.0)

- Added a formal v0.9 storage guarantees definition in project documentation
- Defined explicit validity rules for restorable logical files (`COMPLETED`
  lifecycle and readable referenced blocks)
- Defined restore atomicity and durability guarantees (temp write, fsync,
  atomic rename, parent directory fsync)
- Defined non-destructive GC guarantees and trust-boundary assumptions
- Documented explicit v0.9 non-guarantees (format compatibility and
  multi-node/distributed consistency are not guaranteed pre-v1)

### Changed (0.9.0)

- Hardened GitHub Actions CI with workflow concurrency cancellation and job timeouts
- Extended CI execution to release-style tags matching `v*`
- Upgraded integration correctness runs to use the Go race detector
- Updated smoke CI runs to reset database and storage state between phases so
  the main samples run and edge-case run are isolated and deterministic
- Documented the required GitHub ruleset / branch-protection policy around the
  aggregate `CI Required Gate`
- Added a maintainer audit script to verify the local workflow gate and the
  expected GitHub repository protection policy
- Clarified and tightened user-facing behavior expectations for data integrity,
  crash recovery, concurrency, and verification semantics

### Notes (0.9.0)

- The repository now exposes a single aggregate status check, `CI Required Gate`,
  intended to be configured as the mandatory required check in GitHub.
- GitHub branch protection and tag protection remain repository settings; they
  cannot be fully enforced from source files alone.
- v0.9 guarantees are correctness-oriented and apply within the documented trust
  boundary (database/filesystem are not externally modified and fsync semantics
  are respected).

------------------------------------------------------------------------

## [0.8.0] - 2026-03-30

Simulation and CLI stabilization release.

This version focuses on making coldkeep easier to evaluate, safer to operate,
and more predictable to automate. It introduces dry-run simulation, structured
JSON output for CLI commands, richer operation result models, and stronger
verification and integration coverage.

### Added (0.8.0)

- `simulate store` and `simulate store-folder` commands
- Simulated storage backend for dry-run ingestion without writing container data
- Structured JSON output mode for CLI commands via `--output json`
- Structured result models for store, restore, remove, gc, and stats operations
- Startup recovery JSON event output
- CLI contract tests for JSON schema and exit-code classification
- Retry and fragmentation metrics in stats output
- Explicit read-only and writable container open helpers
- Additional deep verification coverage for file and system integrity
- Expanded integration test coverage for:
  - concurrent store stress
  - aborted file/chunk retry recovery
  - container rotation and sealing
  - verify standard/full/deep behavior
  - corruption and truncation detection
  - shared-chunk safety
  - startup recovery behavior

### Changed (0.8.0)

- Stabilized CLI command behavior and output handling
- Improved exit-code classification using typed CLI errors
- Refined simulation reporting to reflect realistic container usage
- Updated store path to return structured metadata for CLI and JSON responses
- Updated restore path to return structured metadata for CLI and JSON responses
- Updated remove and gc paths to return structured operation summaries
- Improved verification logic with clearer offset continuity and payload validation
- Replaced ambiguous container open boolean usage with explicit helper functions
- Improved retry handling around already-existing block metadata during chunk store
- Improved search filter validation at CLI level for numeric size arguments

### Fixed (0.8.0)

- Fixed restore path to use explicit read-only container access
- Fixed several CLI usage/error paths to classify correctly as usage failures
- Fixed simulation command argument handling and error reporting
- Fixed deep verification consistency around transformer reuse and offset checks
- Fixed container counting in simulation output to better match completed stored data
- Fixed fragile retry behavior that previously depended on string-matching some storage errors

### Notes (0.8.0)

- `simulate` reuses the real chunking, block encoding, and metadata pipeline, but
  does not persist container payloads to physical storage.
- coldkeep remains an experimental project and is not production ready.
- On-disk format and CLI details may continue to evolve before v1.0, but v0.8
  establishes the intended CLI contract and evaluation workflow.

------------------------------------------------------------------------

## [0.7.0] - 2026-03-28

Block Abstraction & Encryption Foundations.

This release introduces a major evolution of the storage engine by decoupling
logical data (chunks) from physical storage (blocks), and adding a pluggable
encoding layer with support for encryption.

### Added (0.7.0)

- Block abstraction layer separating logical chunks from physical storage
- New `blocks` table storing codec, offsets, sizes, and encryption metadata
- Pluggable block transformer interface for encoding/decoding
- AES-GCM encryption support (`aes-gcm` codec)
- Per-block random nonce storage for encryption
- CLI support for codec selection (`--codec`)
- Environment-based configuration (`COLDKEEP_CODEC`, `COLDKEEP_KEY`)
- `init` command to generate encryption keys and bootstrap local setup
- Dual-mode CI testing (plain + encrypted storage paths)
- Improved CLI structure and help output

### Changed (0.7.0)

- Storage engine now writes encoded blocks instead of raw chunk records
- Restore pipeline now decodes blocks before reconstructing files
- Verification system operates on decoded block payloads
- Stats now report physical storage using block sizes (`stored_size`)
- Garbage collection operates on blocks and uses `live_ref_count OR pin_count` as deletion invariant
- CLI command handling refactored for extensibility and clarity

### Removed (0.7.0)

- Legacy chunk physical fields (`chunk.container_id`, `chunk.chunk_offset`)
- Chunk record header format (`ChunkRecordHeaderSize` and related logic)
- Direct chunk-to-container storage model

### Security (0.7.0)

- Data at rest can now be encrypted using AES-GCM
- Encryption keys are externalized via environment variables (not stored in DB or repo)
- `.env` files are created with restricted permissions (0600)
- Fail-fast behavior when encryption is requested but no key is provided

### Notes (0.7.0)

- This is a foundational release for future features such as key rotation,
  multi-key support, and advanced encoding strategies.
- Existing data stored with the `plain` codec remains fully compatible.
- Coldkeep remains an experimental research project and is not production ready.
- On-disk formats may continue to evolve before v1.0.

------------------------------------------------------------------------

## [0.6.0] - 2026-03-22

Storage model evolution and container API redesign.

### Added (0.6.0)

- New container abstraction with Append / ReadAt / Sync / Close API
- Container sealing with full-file hash verification
- Multi-layer verification system (standard, full, deep)
- Stress tests for concurrency, retries, and rotation

### Changed (0.6.0)

- Refactored storage pipeline to use container interface
- Simplified container header format
- Improved concurrency handling with row-level locking and retry logic
- Container rotation behavior under concurrent workloads

### Removed (0.6.0)

- Whole-container compression flag
- Whole-container encryption flag

### Notes (0.6.0)

- On-disk format is still evolving and may change before v1.0
- Container size limit is enforced on a best-effort basis under concurrency

------------------------------------------------------------------------

## [0.5.0] - 2026-03-21

Deterministic restore guarantees for stored files and dataset-level workflows.

### Added (0.5.0)

- End-to-end integration tests using real fixture datasets:
  - `samples`
  - `samples_edge_cases`

- Full workflow validation for fixture datasets:
  - store folder
  - verify system (full)
  - GC (dry run + real run)
  - restart / recovery
  - restore all stored logical files
  - hash comparison against original inputs

### Scope (0.5.0)

- This release validates deterministic restore at the logical-file level:
  - stable stored logical files under deduplication
  - byte-identical restore outputs verified by SHA-256
  - deterministic behavior across GC and restart

- It does **not** yet define a first-class “restore folder tree layout exactly”
  contract, as current tests restore logical files individually.

### Notes (0.5.0)

- Whole-container compression remains readable for backward compatibility,
  but is no longer used for new writes.
- Compression removal and storage-format cleanup are deferred to a later release.
- Some scenario tests overlap between generated datasets and fixture datasets;
  both are currently retained to maximize confidence.

------------------------------------------------------------------------

## [0.4.0] - 2026-03-19

Integrity Verification Layer

This release introduces a complete integrity verification system for coldkeep,
covering metadata consistency, container structure validation, and full
end-to-end data integrity checks.

The system is designed in three verification levels:

- Standard: metadata integrity checks
- Full: metadata + container structure and hash validation
- Deep: full physical verification by reading container data and recomputing chunk hashes

### Added (0.4.0)

- `verify system` command with three verification levels (standard, full, deep)
- `verify file <id>` command with per-file verification (standard, full, deep)
- Deep verification logic that reads container data and validates chunk hashes
- Record-level validation (header hash + stored size + data hash)
- Container-wide integrity verification across all sealed containers
- Comprehensive integration tests for verification (positive and corruption scenarios)

### Improved (0.4.0)

- Verification coverage across file, chunk, and container layers
- Error reporting with aggregated verification failures
- Internal consistency checks for chunk offsets, sizes, and container bounds

### Notes (0.4.0)

- Deep verification performs full disk reads and may be slow on large datasets
- Whole-container compression is still present but will be removed in a future release in favor of block-level compression

coldkeep remains an experimental research project and is not production ready.
The on-disk format may change before v1.0.

------------------------------------------------------------------------

## [0.3.0] - 2026-03-15

Safe garbage collection foundation.

### Added (0.3.0)

- Repository verification command (`coldkeep verify`)
- Verification levels: standard, full, deep
- Reference count validation
- Container integrity verification
- Chunk offset validation
- Deep data verification (hash validation)

### Improved (0.3.0)

- Garbage collection safety via transactional re-checks
- Advisory lock preventing concurrent GC runs
- `gc --dry-run` simulation mode

### Testing (0.3.0)

- Integration tests for GC safety
- Verification corruption detection tests

------------------------------------------------------------------------

## [0.2.0]- 2026-03-11

Crash-consistency foundation for the storage engine

### Added (0.2.0)

- Logical file lifecycle management
- Chunk lifecycle management
- Retry handling for interrupted operations
- Startup recovery system
- Container quarantine mechanism
- Extended storage statistics
- Smoke test improvements
- Durable container writes with fsync to guarantee on-disk persistence

### Improved (0.2.0)

- Concurrent file ingestion
- Garbage collection safety
- Operational observability

### Notes (0.2.0)

This version introduces the core reliability model for the storage
engine.

The on-disk format and APIs may still change in future releases.

### Known Limitations (0.2.0)

- Basic crash recovery exists, but full end-to-end crash consistency across
  filesystem and database layers is still evolving.
- No encryption at rest or in transit.
- No authentication or authorization model.
- Whole-container compression is not suitable for efficient random-access
  restores.
- Concurrency behavior has not been heavily stress-tested under high parallel
  workloads.
- No background integrity verification or automatic container scrubbing.
- On-disk storage format may change before v1.0.

------------------------------------------------------------------------

## [0.1.0] - 2026-02-24

Initial public research prototype (POC).

### Added (0.1.0)

- Content-addressed chunking using SHA-256
- File-level SHA-256 deduplication guard
- Chunk reference counting
- Container packing on disk with deterministic append logic
- PostgreSQL-backed metadata schema
- CLI commands:
  - `store`
  - `store-folder`
  - `restore`
  - `remove`
  - `gc`
  - `stats`
  - `list`
- Docker Compose setup (Postgres + app)
- Integration test scaffolding (environment-gated)
- Basic CI pipeline (build, vet, tests)
- Open-source project files:
  - LICENSE (Apache-2.0)
  - SECURITY.md
  - CONTRIBUTING.md
  - CODE_OF_CONDUCT.md
  - README.md

### Design Characteristics (0.1.0)

- Per-file transactional metadata
- `SELECT ... FOR UPDATE SKIP LOCKED` container selection
- Deterministic chunk ordering for restore correctness
- Chunk-level and full-file integrity verification on restore

### Known Limitations (0.1.0)

- Not crash-consistent; filesystem and database state may diverge on
    failure.
- No encryption at rest or in transit.
- No authentication or authorization model.
- Whole-container compression is not suitable for efficient
    random-access restores.
- Concurrency guarantees are minimal and not heavily stress-tested.
- No background integrity verification process.

------------------------------------------------------------------------

Future versions may introduce structural changes to container layout,
metadata integrity, or security properties. Backward compatibility
guarantees are not defined for the prototype stage.
