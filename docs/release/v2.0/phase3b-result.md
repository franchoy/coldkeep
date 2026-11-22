# Phase 3B physical binding BUILD result — 9 November 2026

```text
Phase 3B unit: COMPLETE, awaiting independent review
Parent action 2.04: ACTIVE
Implementation: ACTIVE
Product gates: INCOMPLETE
Physical binding qualification: PASS
Fresh recovery qualification: PASS
Fresh integrated qualification: PASS
Ordinary schema18/19 writers: REFUSED
IMPORT_READY maintenance/activation: REFUSED
Phase 3C: OUTSTANDING
```

This is the bounded unit authorized by `coldkeep_phase3b_codex_2026-11-09.md`
and the user's full BUILD request, including conditional completion in this run.
Independent acceptance is still required. No release, publication, push, merge,
tag, production daemon, capture enablement or live installation switch occurred.

## Source, inputs and environment

Accepted source: HEAD `cf70de06d2ab2da6f64fa7b13fa9610af07e32f2`, tree
`40297ae58e050d9f1ef36fee5b62954011a28d91`. All 1,754 tracked hashes/modes and
143 supplied comparison copies matched the clean accepted checkout.
Launch SHA-256 `9430c75eadca1c8e806daf0f49a1a6df884b52818e707d1571db55275d6cbf63`
matched the external expectation. Its 171 members/170 checksum entries and the
embedded accepted archive's 239 members/238 entries had exact coverage and
valid CRCs. The embedded archive matched SHA-256
`829c99e4be041b8dc8efe921a14aa5a335184ba32ba988ba975b108a82fcc88a`.

Clean implementation checkpoint: HEAD `11ce46d2906a557692a4ecbe20a440c35066506a`,
tree `5e5c0f178571d43a472df8a3e19781900a9d5639`, branch
`release/v2.0-phase3b`, worktree
`/home/franchoy/vscode/coldkeep-worktrees/v2.0-phase3b`.
Final implementation/test checkpoint: HEAD `d04702b778f18fd14d32baa37c6fc0842f80a5c8`,
tree `6d32a423e2ded3c4c00e13580d469cd601c32e59`. Two intermediate corrections
connected independent plaintext/encrypted restoration to the actual offline
relocation, then made status refuse readiness while partial prepared intent
staging remains. All required current runtime, Python, static and integrated
gates were refreshed on this clean final implementation checkpoint. Earlier
passing runs remain auditable history. `final-qualification-delta.json` records
the changes and their qualification; the full engine exception remains explicit.
The final documentation commit changes only the charter, handoff, instructions,
contract clarification and this report. `final-identity.json` records the exact
final HEAD/tree; `final-documentation-delta.json` proves no production, test or
runner change after qualification. Both patch forms are independently replayed
to that final tree, including modes, in fresh repositories.

Requested settings: GPT-6 Astra / Extra High throughout. Actual observable model
and effort: unknown; no settings change is claimed. Actual pinned executables
were Go 1.26.10 (`GOTOOLCHAIN=local`, `GOWORK=off`, CGO enabled for qualification),
golangci-lint 2.9.0, PostgreSQL server/client 16.15, Node 24.22.2/npm 11.19.1,
and Docker Engine 29.9.2. The retained digest-pinned PostgreSQL16 image and all
dependency/frontend/image pins are unchanged. Modest Go parallelism and small
fixtures were used; heavy database, browser and Docker gates ran sequentially.

## Implemented contract

[Physical binding](physical-binding.md) defines the private maintenance facade,
operator commands, exact schemas and state machine. The ordinary writer ceiling
remains 17. Explicit checkpoint-protected migration adds schema19 without
changing the six schema18 identity relations or their 53 exact constraints.
Six binding/source tables add 41 constraints: 94 total, each bounded to 4,096
metadata bytes, with a 385,024-byte aggregate. Schema18 retains its 217,088-byte
envelope. Relation/name/kind/status ownership, complete seen-set, raw metadata
checks and server-side boolean definition comparison remain mandatory.

Root revisions, heads and unique operations retain plan/request digests and the
checked recovery generation/manifest. Configured source IDs remain distinct from
the disabled unknown-scope legacy source. Real sources remain capture-disabled;
rebind and disable preserve prior mappings and history. Every operation advances
the root configuration revision and creates a fresh bounded pre-operation copy.
Schema17/package1/receipt1 and schema18/package2/receipt2 remain exact; schema19
uses package3/receipt3 and the explicit physical-binding-v1/recovery-only reader.
Portable history never grants current local writer authority.

Installation flock, canonical existing-root lease and database advisory reservation
are acquired in that order and retained through checkpoint, commit, marker then
descriptor publication, reconciliation and physical cleanup. Observed lease loss
is terminal. Committed errors report reconciliation-required. Reconciliation
checks actual SQL operation/revision, prior descriptor, checkpoint and source
result; it never infers rollback from a failed response. Local records contain
no passwords or encryption keys. Status can report installation/root state
without catalog access and cannot reconstruct a deleted descriptor. Both passive
and checked status report reconciliation-required for durable intent or partial
prepared-intent staging; neither reports maintenance readiness in those states.

Limits are marker 16 KiB, descriptor 64 KiB, intent 128 KiB, one root, one pending
intent and 64 configured sources. Native paths use reversible base64, including
invalid UTF-8 bytes; the smaller 3,072-byte path cap keeps base64 within the
unchanged 4,096-byte catalog field budget. Existing row/object/receipt/deadline
limits and lower caller limits remain unchanged. The aggregate test-run timeout
is ten minutes; production operation ceilings were not enlarged.

Evidence is `linux-local-unique-mount-session-v1`: opened-handle filesystem,
device/inode and `STATX_MNT_ID_UNIQUE`, with boot and namespace observations.
Linux 6.8+ support and the returned UNIQUE mask are required; no weaker fallback.
This run observed ext4 payloads and separate tmpfs/bind-mount installation roles.
The admission allowlist also recognizes XFS/overlayfs; those filesystem types
were not separately qualified here. A same-object offline rename preserves
identity and native lock objects. Explicit reconnect after Docker recreation
advances incarnation/revision only after full validation and checkpoint.
Hardware independence and clone resistance are not claimed.

## Expected versus observed qualification

Every row below passed within the stated local profile. Stable names are enforced
by the ordinary blocking runner; raw events and exact mandatory-set accounting
are included in the evidence archive.

| Required family / expectation | Observed production-path proof |
| --- | --- |
| Initial binding and 18→19 migration; IDs/history/recipes retained | `TestBindingMigrationAndRecovery`; checked pre-operation generation, exact profile 19, one tuple, idempotent same operation and v3 import |
| Marker/descriptor malformed or unsafe input refuses | `TestBindingControlAdmission` and `TestBindingUnsafeControlOwner`; missing/future/truncated/oversized/duplicate/trailing, symlink/FIFO/hardlink, owner/mode and stale identity/revision cases |
| Wrong tuple, key, DB, profile, ambient settings and stale plans refuse | `TestBindingEarlyRefusals`, `TestBindingIntentConflicts`, `TestBindingCommittedIntentConflicts`; actual catalog fingerprints unchanged on refusal |
| Missing, empty, unrelated or copied roots never initialize | `TestBindingSubstitutedRoots`; transient-write monitoring for empty substitution, copied marker refusal, unavailable catalog and weaker evidence |
| Supported evidence succeeds; changed sessions require explicit reconnect | `TestBindingDockerProfile`; recreation refused automatic admission, then reviewed reconnect advanced revision/incarnation |
| Offline same-object relocation preserves lock and history | `TestBindingSourcesAndOfflineRelocation` plus strengthened `TestBindingIndependentRecovery`; drained rename, preserved objects/IDs, fresh export and independent plaintext/encrypted history restoration |
| Active root/control/lock substitution invalidates ownership permanently | `TestBindingActiveSubstitution` and `TestExistingLeaseLossNeverRevives`; no schema19 commit or revived handles |
| Real source lifecycle stays disabled, distinct and revisioned | `TestBindingSourcesAndOfflineRelocation`; two sources, same-object and changed-source rebind, fresh-scan classification, disable retains history |
| Missing/empty/read-only sources never publish emptiness or markers | `TestBindingSourceCapacityAndAvailability`; read-only availability, missing/substituted mismatch and transient-write monitoring |
| Exact path and finite capacity boundaries | `TestBindingExactPathsAndBudgets`, `TestBindingNativeByteRootMapping`; below/at/above byte caps, actual invalid-UTF8 root through CLI; actual source counts 63/64 accepted and 65 refused |
| Same/different-process and different-root/same-DB exclusion | `TestBindingReservationsThroughPublication`; contenders excluded through postcommit publication and cleanup |
| Crash reconciliation is idempotent and truthful | `TestBindingCrashCuts` and `TestBindingProcessTermination`; injected SQL/publication cuts and actual SIGKILL before/after intent, commit and both file publications |
| Cleanup joins precede ownership release | `TestBindingCleanupJoins`, `TestBindingExportCleanup`; blocked rollback/driver closure, cancellation, child and credential cleanup, propagated errors and lower deadlines |
| File write/sync/rename/directory-sync/close failures do not complete | `TestBindingPublicationFailures`; committed failures retain reconciliation evidence and same-request completion succeeds |
| Exact schema19 and metadata admission | `TestBindingSchemaAdmission`; columns/types/nullability, missing/relocated/duplicate/wrong-kind/status/definition constraints, compressed/oversized metadata and malformed lineage/head refusal; all A1–A2 cases retained |
| New profile cannot bypass export/import/restore admission | `TestBindingRecoveryAdmissionBoundaries`; no Complete/READY/pins/output on malformed/future/mixed/wrong-repository profiles, with valid positive control and preserved receipt |
| Actual selected Docker profile | Non-root fixed `--mount` roles, read-only source, separate installation, 256 MiB memory/64 MiB temp; actual missing bind source refuses without host creation; empty replacement refused |
| Untouched old and current writers/readers | `TestBindingOldWritersAndReaders`; D19/accepted/pre3B/current schema17 positives and 18/19 writer fences with bootstrap enabled; pre3B reader v1/v2 positives and v3 refusal before pins/output; current v1/v2/v3 |
| Schema19 independent plaintext/encrypted recovery | `TestBindingIndependentRecovery`; original DB/root/source/descriptor unavailable in recovery namespace, independent final file reopens/hashes/metadata and all twelve identity/binding relations compared |
| Every retained READY artifact refuses maintenance/activation | All six binding/source actions tested against actual v1/v2/v3 receipts; malformed artifacts also refuse; receipts preserved; recovery-only restore remains available |
| Ordinary runner remains blocking | Twenty mutation tests cover missing/skipped new and retained events and nonzero Go child with otherwise complete events |
| Preservation and scoped authority | Predecessor/frozen/report/fixture/pin checks and 36 authority tests; completing this unit cannot complete 3C, parent 2.04 or product gates |

## Fresh gates and explicit reuse

The final fresh ordinary recovery race gate passed **667 test events**: recoveryexport
541, coordination 99 and operator 27. All **491 required events** passed (341
accepted + 150 added), with zero missing, failed or skipped events. Final duration was
479.624 seconds including reader staging; the first full gate also
passed in 476.537 seconds. Both raw streams are retained. The fresh post-live-GC proof remains
schema17; schema19 ordinary GC is still fenced.

The unchanged current product oracle passed all 23 expectations: 12 restored,
9 refused and 2 retained known limitations. Full Python: **586 tests,
PASS**, including authority and runner mutations. Focused reruns also passed:
36 authority, 20 recovery-runner mutations, 6 integrated-runner mutations, 6 TCP
mutations and 3 Go CI-enforcement tests. These overlapping totals are not summed.

The full fresh Phase 2 proof passed 85 required native/web tests and 15 critical
events, plus 38 PostgreSQL tests / 3 critical events, nine TCP classifier controls,
native and Docker browser restoration with independent hashes, prepared embedded
assets, CLI-only install, no-CGO refusal and ownership/cleanup checks. Build,
vet, pinned lint, formatting/whitespace, tidy-diff, row-writer/source guards,
validation matrix, shellcheck, current authority/governance, explicit historical-v1
validators and local CI audit passed. Workflow enforcement intervals were unchanged.

Affected fresh regressions passed 146 database events, 312 selected shared-layer
events and 35 restore/pin/history events. The whole engine package remains
**FAILED/UNQUALIFIED**: 805 passed events, eight failed events in
`TestEngineBatchCancellationStopsNewDispatchWithPartialResult` and
`TestEngineDependencyDirection`, plus its helper skip. The untouched accepted
checkout reproduced the identical eight failures with the same configured flags.
No architecture guard or failing test was weakened.

The four disclosed nonqualifying skips remain: `TestRestoreNativeDarwinTempPathWithoutEvalSymlinks`,
`TestStoreInterleavingRetryClaimSignalsCASBoundary`,
`TestStoreFolderWorkersFourWithConcurrentGCPlanNoCorruption`, and
`TestPhase3R13ReadQueryRestoreCWDChild`. No mandatory recovery or integrated
case skipped. No new qualifying skip is accepted.

Only the historical D19 fixed-oracle result is reused: original HEAD
`d3581602aee2599547564322fe083baae1d52063`, tree
`96b2840cb9da9714705bb6c2998f46b7ed3e78ee`, original command/log digests and
unchanged fixture/test/runner boundaries in `reuse-ledger.json`. It does not
replace fresh old-binary schema19 refusal. All required current runtime gates
were rerun; historical unrelated broad results are not used to qualify this unit.

## Corrections, cleanup and remaining limits

Development diagnostics are retained in `commands.jsonl` and raw logs. Early
harness corrections covered nested-root contention expectations, rollback-cut
placement and private Docker installation permissions. The Docker test then
found a real matcher weakness: ordinary mount IDs and namespace inodes were
reused after recreation. Requiring boot-scoped unique mount IDs corrected it;
the fresh full race gate exercises the corrected production rule. Portability
checks moved strict JSON parsing to common code and Linux-only options to their
proper file; unused imports and one lint suggestion were corrected before the
clean qualifying checkpoint. Postcommit intent checking was strengthened before
that checkpoint and passed the added conflict cases. Final matrix review found
that the independent restore proof needed to follow the actual offline move;
the eight-line test-only correction and full recovery rerun closed that gap.
A later review found that partial prepared staging blocked apply but could leave
status reporting readiness. Status now refuses it, the named mandatory regression
passed, and all final gates were refreshed after that production correction.

The first broader regression attempt lacked `COLDKEEP_TEST_DB=1`; the next lacked
`COLDKEEP_DB_AUTO_BOOTSTRAP=true` for its disposable scratch catalogs. Those runs
are explicitly nonqualifying. Corrected `*-configured` and final `*-final` regression commands recorded both
flags and the maintenance database and reran all affected families. Their final
accounting has only the four disclosed skips and inherited engine failures.

Before owned server disposal, only postgres/template0/template1 remained, with
zero fixture roles, other sessions, subscriptions or replication slots. Both sequentially used owned
server identities and their credentials were removed after the zero-state check; integrated proof containers/networks/
image tags cleaned successfully. Go cleanup disposed only controlled fixtures,
including intentionally failed/uncertain checkpoint scenarios, after retaining
nonsecret events/observations. Generated build trees/readers are excluded from
the archive and removed under recorded ownership. No pre-existing resource was
pruned or reset.

All 36 previous worktree registrations (including 9 absent), 133 refs, 13 accepted
reports, all 849 frozen files and retained fixture/tool/dependency pins are
preserved. Frozen aggregate remains
`64fb69cb875c4bdf96c10cd759275b7232849c2f8083b0b406f11d7b18b23c7a`.
The bounded server used a 2 GiB memory limit and 1.5 GiB data tmpfs; a point-in-time
sample observed 139.9 MiB, not a measured peak. Binding Docker fixtures used 256 MiB
memory and 64 MiB tmpfs. Physical failure-domain independence is unknown.

Process termination does not establish general power-loss durability. Privileged
mount/unmount, real reboot, cross-filesystem transfer, arbitrary replacement-root
activation, broader filesystem/platform distribution, external LAN/routed IPv6,
and production readiness remain unqualified. Invalid or partial control artifacts
refuse; there is no automatic reconstruction from a copied catalog/marker.
All IMPORT_READY maintenance/activation remains deferred. Phase 3C separately owns
generation-owned membership, exact entry names, policy provenance and indexed/
paginated lookup. Ordinary writer admission and ready-target activation require
their later ownership/adoption qualification.

The compact archive contains this report, exact identities and full source
hash/mode inventories, command/raw-log ledger, mandatory events, controls,
preservation/cleanup, both independently replayed patch forms, and exact internal
SHA256SUMS coverage. Standalone report/identity/patch equality and the external
ZIP checksum are verified by `package-verification.json`.
