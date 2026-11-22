# Phase 3A identity, adoption and recovery result

Date: 8 November 2026. Mode: BUILD. Owning instruction:
`coldkeep_phase3a_codex_2026-11-08.md`.

Phase 3A unit: COMPLETE.
Parent action 2.04: ACTIVE.
Implementation: ACTIVE. Product gates: INCOMPLETE.
Identity/adoption/recovery qualification: PASS.

Completion is bounded to Linux x86-64/PostgreSQL 16 in the cooperative local
manual-postgresql-docker profile. Accepted 2.01, local 2.03 and corrected 2.02
remain COMPLETE. Physical binding and capture membership are not qualified.
No live installation was switched and nothing was pushed, published, merged,
tagged or released. No subagent was used.

## Source and execution identity

Requested model: GPT-6 Astra. Requested effort: Extra High. Actual model: unknown.
Actual effort: unknown. Neither setting is exposed by this execution interface;
the request is recorded separately and no switch is claimed.

Accepted HEAD `1229563116f71067c74801516bcd64c92e7b6ed2`, tree
`883f1a4a27db1c5eb747c76e5d77bb4379ef5f78`, was clean. All 1,740 source hashes,
55 selected launch source copies and accepted record copies matched. The launch
ZIP SHA-256 is `ec6c02b356ff03ad0074bb0409c269b3312aceafc150d15d3cb84de82faf2024`;
all 90 checksum entries and complete 91-member coverage passed. Accepted evidence
SHA-256 is `130029ee9eb89ea2ffe3a2e4c4f90cc43b0e50694d2d0b7fe9c500f0a394da10`;
all 274 checksums and complete 275-member coverage passed. Both archives passed
CRC and safe-member checks. No earlier start bundle was substituted.

The separate worktree is `/home/franchoy/vscode/coldkeep-worktrees/v2.0-phase3a`,
branch `release/v2.0-phase3a`. The clean implementation checkpoint is
`6f257f6fd0391412ec661dabea52013168f7e6eb`, tree
`d9fb1b017b17ceecfdf0b9de6dfb546c39cccd55`. Fresh qualification ran against that
checkpoint. The final commit changes only this report, charter and handoff.
The accompanying `final-identity.json` binds exact final HEAD/tree and ordered
commits without placing a self-referential commit ID inside the committed report.

The launch plans and accepted source records were read before production edits;
`implementation-contract.md` records the pre-edit design. Version 2.0.0-dev.0,
module github.com/franchoy/coldkeep/v2, latest stable 1.13.19, toolchain,
dependencies, frontend, image pins and fixture expectations remain unchanged.

## Implemented contract

Explicit `coldkeep-recovery adopt-preview` and `adopt-apply` implement the
[operator and compatibility contract](identity-adoption.md). Preview is passive;
apply requires an explicit catalog/root, expected schema 17, typed operation ID,
reviewed plan digest and fresh checkpoint destination. No ordinary open,
bootstrap environment option or daemon start triggers adoption.

Six additive tables hold one repository, logical root, adopter device, disabled
synthetic unknown legacy source, committed adoption operation and every historical
snapshot's legacy mapping. Typed IDs use a type prefix and 128 CSPRNG bits encoded
as 32 lowercase hexadecimal digits. They are names, not credentials or physical
ownership evidence. Canonical encodings, uniqueness, cardinality, foreign keys,
disabled/unbound state and provenance are database constrained and independently
validated during recovery. Snapshot IDs, original history and payload recipes
remain unchanged. The device is the adopter, not an invented historical capturer.

One existing canonical-root maintenance lease and a catalog reservation span
inspection, fresh schema17 export, joined SQL/driver/child/credential cleanup,
completion publication and independent checking, serializable writable migration,
commit reconciliation, post-validation and physical cleanup. A transaction rechecks
the supported state under schema-version locking. All identity records and the
17-to-18 transition commit atomically. Same request/plan reconciles the same IDs;
a conflicting request refuses. A post-commit acknowledgement fault is reconciled,
not called a rollback. The completed pre17 generation remains the rollback route
into a fresh owned target; no downgrade or automatic installation replacement exists.

Any retained IMPORT_READY authority refuses adoption before mutation. Replacing
a receipt cannot revoke authority already read by an old reader. Existing valid
schema17/v1 receipts remain readable by the untouched old operator and the new
operator, including after a refused adoption attempt.

| Catalog/profile | General writers | Qualified recovery |
| --- | --- | --- |
| PostgreSQL 17 | Existing admission and behavior | v1 package/receipt, exact 19 tables and old reader capability |
| PostgreSQL 18 | Identity-binding-required refusal | v2 package/receipt, exact 25 tables, repository ID and explicit identity capability |
| SQLite 17 | Existing behavior and native proof | No identity adoption; explicit refusal without writes |
| Future, malformed or mixed profiles | Refusal | No completion/readiness on semantic admission failure |

Current ordinary database, startup-recovery/storage and raw-handle engine
construction refuse PostgreSQL18 before durable mutation. Actual untouched D19
and accepted pre3A production writers also refuse, including auto-bootstrap.
Schema18 restores validate the current full catalog and receipt under ownership
before the existing temporary pins. Recovered logical identities remain unbound.
Low-level caller-owned SQL/payload primitives do not constitute independent
ordinary admission APIs. Arbitrary external SQL writers and cross-host owners
are outside this cooperative local profile.

## Expected versus observed

All named Phase 3A requirements below ran fresh under the race detector. The
full exact requirement list and run/pass observations are in `mandatory-events.json`
and `recovery/go.jsonl`; success and expected refusal are distinct outcomes.

| Family / executable requirement | Expected | Observed |
| --- | --- | --- |
| Passive/explicit: TestAdoptionRoundTrip, TestAdoptionOperatorAndDatabaseReservation | Preview unchanged; explicit operator adopts | PASS; real commands preview/apply/reconcile |
| Identity: TestAdoptionIdentityConstraints, TestAdoptionRoundTrip | Canonical unique/FK lineage, explicit unknown provenance, unchanged legacy history/recipes | PASS; actual constraint violations refuse and snapshots map completely |
| Repeat/conflict: TestAdoptionRoundTrip, TestAdoptionOperatorAndDatabaseReservation | Same IDs on retry; conflicting request and distinct-root contender refuse | PASS; actual process/database reservation controls |
| Checkpoint: TestAdoptionEarlyRefusals, TestAdoptionCheckpointWrongSource, TestAdoptionStalePlanAndBounds | Failed/incomplete/corrupt/wrong-source package, missing key or stale plan cannot advance | EXPECTED REFUSALS; schema/catalog/payload/control preservation checked |
| Continuous ownership: TestAdoptionOwnershipContinuity | Real second process excluded through checkpoint, migration and cleanup | PASS at eight barriers |
| Transaction faults: TestAdoptionTransactionFaults | Pre-DDL, DDL, data and pre-commit faults roll back; lost acknowledgement reconciles | PASS; schema17 without identities or consistent committed18 |
| Writer fences: TestAdoptionWriterFences, TestAdoptionEngineAdmission | D19/accepted/current controls write17; all refuse18 before changes | PASS; full logical catalog/schema/payload/control comparisons |
| Old authority: TestAdoptionOldReceiptCompatibility | Retained v1 import/restore works; adoption cannot bypass it | PASS; untouched accepted importer/reader and new reader; receipt unchanged |
| Backend: TestAdoptionEarlyRefusals/sqlite, TestAdoptionStalePlanAndBounds/sqlite-file | SQLite adoption refuses before writes | EXPECTED REFUSALS; actual SQLite file unchanged; ordinary proof passes |
| Profiles: TestAdoptionProfileRefusals, TestAdoptionImportProfiles | Future/mixed/false labels, missing/extra schema, wrong types, unknown capability, orphan/conflicting/bound IDs, missing constraints/lineage and malformed receipts refuse | EXPECTED REFUSALS; actual restored-catalog semantic check before readiness |
| Recovery: TestAdoptionHistoricalIsolation | Pre17 and post18 packages independently import/restore plaintext and encrypted fixed history | PASS in isolated containers without original DB/root/installation; 3 plaintext and 8 encrypted files per route, independent hashes/metadata and exact post18 lineage |
| Lifetime: TestAdoptionCleanupOwnership, TestAdoptionMigrationCleanup, TestAdoptionExportCleanup | Blocked rollback, physical driver close, child and credential cleanup retain lease and prevent false success | PASS including cancellation and non-cancellation barriers |
| Bounds: TestAdoptionStalePlanAndBounds, TestAdoptionProfileRefusals plus retained receipt suite | Request/text/row/aggregate/deadline/receipt budgets enforced | EXPECTED REFUSALS; no limit enlargement; 32 MiB + 64 KiB receipt and separate 1 MiB config ceiling retained |
| Supplemental engine audit | Determine whether broad failures are introduced | UNRESOLVED inherited two test families, reproduced exactly on accepted source; not reported as a passing engine package |

## Fresh gates and explicit reuse

Fresh blocking recovery: **488 test/subtest passes**, zero failures, skips or
missing events. **313 named requirements** include all **244 accepted requirements**
and 69 additions. Full-catalog closure, migrated column order, repair/retirement
references, RLS/visibility, subscriptions, credential placement, bounded decoding,
joined cleanup and post-live-GC independent recovery remain in the fresh run.
The ordinary protected CI runner includes these requirements. Mutation tests
demonstrate nonzero Go exit, missing event and skipped event fail the gate for
new adoption and retained recovery cases. Hosted CI was not run.

Fresh integrated Phase 2 proof: all **85 Go cases / 15 critical outputs**, **38
PostgreSQL cases / three outputs**, **nine TCP controls**, native and Docker
browser journeys, actual fixed historical restores, CGO SQLite LocalWriter
store/close/reopen/restore, packaging, CLI installation and ownership checks passed.
P1 destination admission, P2 TCP classification and P3 complete-byte bounds remain.
The direct daemon sibling target timed out within its declared connect interval;
it is not reported as an explicit refusal. Published-port targets explicitly
refused. External physical LAN and routed IPv6 qualification are not claimed.

Fresh current product oracle: **23 matched expectations**, comprising 12 restores,
nine refusals and two known baseline limits. Those limits are not restoration
successes. Fresh full Python: **568 tests passed**, plus **30 focused authority
tests** and targeted runner/CI mutations. Build, vet, pinned lint (zero issues),
tidy diff, gofmt/whitespace, shell syntax/shellcheck, matrix, row-writer and quote
guards, local CI audit and current/historical validators passed.

Fresh applicable regressions: database/schema package **146 passes**; selected
catalog/container/storage/snapshot/verification **312 passes**, three existing
skips; restore/pinning/history **35 passes**, no skips. The broad supplemental
engine run had **805 passes**, one subprocess-helper skip and eight failing
test/subtest events across two pre-existing families. Both families reproduced
on the untouched accepted HEAD with identical failures: the call-count-based
stepped cancellation test cancels before its expected first success, and the
existing dependency guard rejects the accepted proofdaemon-to-engine import.
Their unchanged source/commands/raw output are bound in
`qualification-summary.json` and `logs/accepted-engine-counterexample.log`.
They are outside the bounded adoption gate and remain unresolved; no full engine
package pass or whole-product qualification is claimed.

The four explicit skips are TestRestoreNativeDarwinTempPathWithoutEvalSymlinks,
TestStoreInterleavingRetryClaimSignalsCASBoundary,
TestStoreFolderWorkersFourWithConcurrentGCPlanNoCorruption and
TestPhase3R13ReadQueryRestoreCWDChild (subprocess helper). None counts as a pass.
No required recovery, new adoption or integrated case skipped.

Only the historical D19 fixed 23-case oracle is REUSED, not fresh. All six original
artifact hashes, original command/exit/context, exact D19 commit/tree, 1,631
original source blobs and unchanged fixture/runner/dependency boundary were
reverified. `reuse-ledger.json` retains those bindings. Historical broad/recovery
results are not substitutes for this run. D19 and accepted incompatible-writer
executions are fresh, with all original production blobs verified before/after
building the external harness; provenance is in `recovery/phase3a-readers.json`.

All qualifying commands, timestamps, clean checkpoint identity, exit status and
raw-output digests are in `commands.jsonl`. Principal commands were:

```text
bash scripts/run_recovery_export_tests.sh <fresh recovery evidence>
python3 scripts/run_phase2_proof.py --output <fresh integrated evidence>
python3 scripts/run_product_fixtures.py --reader current --output <fresh oracle>
go test -race -count=1 -p=1 -timeout=6m -json ./internal/db ./internal/engine
python3 -m unittest discover -s scripts -p 'test_*.py' -v
go build ./...
go vet ./...
golangci-lint run ./...
go mod tidy -diff
bash scripts/audit_ci_enforcement.sh --local-only
```

## Failures, corrections and limitations

Entry comparison initially used the wrong accepted next-slice archive path.
All 1,740 source hashes had passed, but shell sequencing created the new worktree
after that comparison error. The lookup was corrected and all copies were verified
before source edits. Preservation capture then excluded only the new worktree/ref;
all 33 prior accepted registrations were independently compared, with the accepted
followup bringing the preserved predecessor total to 34. This ordering deviation
is retained in `entry-source-verification.json`, not hidden as a successful first run.

Development qualification found a missing explicit fixture timestamp, a test
Destination/config type mismatch and lint issues; these were corrected before the
clean checkpoint. A disposable-server readiness race was resolved by checking the
actual test database after initialization. A reuse helper initially assumed original
D19 artifacts were nested in the accepted compact archive; their recorded original
paths and exact digests were used instead. Development outputs and clean reruns
remain separate. The supplemental inherited engine failures are not corrections
and remain disclosed above.

During parallel validation, two ancillary command snapshots observed a temporary
untracked directory owned by the full Python suite. Tracked source was unchanged,
but those shellcheck/focused-authority observations are not counted as clean
qualification; both checks were rerun with clean status. The raw snapshots remain.

Observed pinned tools: Go 1.26.10 with GOTOOLCHAIN=local/GOWORK=off, Node 24.22.2,
npm 11.19.1, golangci-lint 2.9.0 and PostgreSQL16 clients/image. Native build used
CGO; its SHA-256 is `7873558eba9a0d6e35ff6838b7e71e5d54420aa1e1ac7884276a333dfc053ac5`.
Its clean asset manifest binds the tested commit/tree. Environment, executable
digests, native linkage, container image identity and browser 158.0.8127.18 are
retained. Observing this host/runtime does not establish a glibc minimum or native
distribution support. Deterministic faults do not prove power-loss durability.
Package hashes do not authenticate an untrusted dump; trusted operator custody
and separate required keys remain essential. Logical lineage and isolated restore
do not prove current volume identity or physical failure-domain independence.

## Preservation, cleanup and handoff

All 34 predecessor registrations (including nine absent-but-registered paths),
their HEAD/tree/status, 131 prior refs, 1,740 accepted source hashes, old accepted
reports and 849 frozen files were preserved. Frozen aggregate remains
`64fb69cb875c4bdf96c10cd759275b7232849c2f8083b0b406f11d7b18b23c7a`.
Pre-existing Docker container/network/image identities and tag associations remain.
Only owned disposable server/fixtures, credentials, reader overlays and generated
binaries/assets were removed. No client sessions or unexpected databases remained
on the owned server before removal. Raw nonsecret evidence is retained.

The final report, identity, source inventory, full changed text sources, incremental
review.patch and ordered.patch are delivered in the checksummed compact evidence
archive. Independent scratch replays of both patch forms must reach the final
tree; `patch-replay.json` records that check. Archive verification checks every
member and standalone-copy equality. Private credentials, payload/catalog dumps,
generated binaries, caches and node_modules are excluded.

Phase 3B owns root markers, expected-volume evidence/confidence, source/root
relocation and revisioned rebind, and missing/substituted empty-mount refusal.
Phase 3C owns generation-owned typed membership, exact-name encoding/display
separation, policy provenance and measured indexed/paginated lookups. Full 2.04
can complete only after all three packages and integrated action-level acceptance.
This run implements neither later unit. The next handoff also carries the two
inherited supplemental engine test failures for their owning maintenance scope.
