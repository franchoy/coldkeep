# Phase 1C follow-up C1–C3 result

**Phase 1C follow-up C1–C3 COMPLETE; parent 2.03 ACTIVE; product gates INCOMPLETE.**
Application identity remains **2.0.0-dev.0**, implementation ACTIVE. This is the
bounded local correction result, subject to review. The
[submitted Phase 1C result](phase1c-result.md) remains byte-identical. Constrained
import, original-installation-loss restoration and post-live-GC survival remain
in the [next handoff](next-slice.md); none was begun here. The current
[package/operator contract](recovery-package.md) describes the corrected bounds
and exporter prerequisites.

The operative authority was the user's explicit invocation of
`coldkeep_phase1c_followup_codex_2026-11-08.md`, not instructions inferred from
archived evidence. Requested settings: **GPT-6 Astra / Extra High**
(`gpt-6-astra`, `xhigh`), BUILD. Actual model and effort were not observable and
are recorded as **unknown**. No global/account settings changed; no subagents
were used.

The launch ZIP SHA-256 is
`1943d250043a7895b0c39590ff8669e7a0d2873d5b0ec00a5f931cce6f24a845`.
All 32 outer checksum entries and all 105 submitted archive checksum entries
(106 files including its manifest) verified. The submitted archive digest is
`a5cce618c7e0ea1b73be01fe794ef6dae94a952d7ce9be646ed1b6dfb2dd0581`;
its already-incorporated review patch was not reapplied. Archived recording and
cleanup helpers were not executed.

| Identity | Value |
| --- | --- |
| Verified submitted base HEAD | `c470be5879bdada70a41a73db30287b68b1e4a47` |
| Verified submitted base tree | `c8b8bd1422a3be45d299e4176d85557850c39862` |
| Separate candidate worktree | `/home/franchoy/vscode/coldkeep-worktrees/v2.0-phase1c-followup` |
| Candidate branch | `release/v2.0-phase1c-followup` |
| Clean tested implementation checkpoint | `cfac52dd73739dbaccb21ad17b7561df23f1ead3` |
| Tested checkpoint tree | `780df5347d84c0d2e56ad6eea4a017753b9d45ad` |
| Fresh external evidence | `/home/franchoy/.local/state/coldkeep-release-evidence/v2.0/phase1c-followup-20261108` |

The final report-only commit follows that checkpoint. Its exact identity,
ordered commits, verified documentation-only delta, incremental patch digest
and independent index replay results are in external `final-identity.json`.
The committed report does not contain its own commit hash.

The complete supplied proposal was inspected and applied against verified file
identities. Proposal SHA-256:
`dc484609773616e9a544b963a404597937091d2f3e0b29b62063c003994919ef`.
Both production files, the credential/subscription regressions, runner and
package contract match the proposal bytes. Gofmt did not alter it. Pinned lint
reported one QF1011 finding in the new parser test: replacing
`var parseContext context.Context = ctx` with `parseContext := ctx` preserves
the same inferred interface type and behavior. That is the only proposal
adaptation; subsequent lint and checkpoint tests passed. Additional changes
are the charter, handoff and this new report.

C1 counts top-level members and exclusions during token traversal, before
typed slice allocation. The caller's member limit and independent 20,000-entry
exclusion ceiling remain separate. Unicode simple-fold aliases and duplicate
aliases use the same equivalence as pinned Go's `encoding/json`; context is
checked during traversal and around typed decoding. Existing depth, string,
key, unknown-field, type, trailing-input and semantic checks remain.

C2 canonicalizes and validates the existing temporary parent before creating
anything. Equal/descendant source or generation parents are refused; common
parents with sibling trees remain valid. `os.Root` anchors directory creation,
passfile creation and owned removal. Modes remain 0700/0600, escaping and
cleanup errors remain enforced, and production still uses fixed `/tmp`. The
private parent seam is for disposable tests. Accepted driver/socket cleanup,
source-lease ordering and child joining are unchanged.

C3 checks publicly visible `pg_subscription.subdbid` against the current
database OID and refuses current-database subscriptions, including disabled
ones, before snapshot export and dump preparation. It does not read
`subconninfo` or need extra reader privileges. A subscription in another
database does not block export. Fixtures use `connect=false, slot_name=NONE`
and remove subscriptions before their owned databases.

| Scenario | Expected and observed on submitted source/probe | Corrected checkpoint |
| --- | --- | --- |
| C1 lowered member limit | Exact submitted production, unchanged new tests: assertion failed because two members were decoded at limit one | `ErrLimit`, typed slices nil: PASS |
| C1 exclusion ceiling | Exact submitted production: assertion failed because 20,001 exclusions and one member were decoded before refusal | Pre-allocation refusal: PASS |
| C1 independent limits, aliases, strictness, cancellation | Targeted correction checks | All five JSON parents and named subtests: PASS |
| C2 source/destination equal, descendant and alias parents | Instrumented submitted allocate-first algorithm: all six unsafe cases allocated and generated protected-tree events; aliases were admitted | All six refuse with no allocation handle or protected-tree create/remove events: PASS |
| C2 valid sibling parent / outside alias | Instrumented algorithm: sibling case passed; outside-alias case failed canonical placement (seven failing children, one passing) | Both succeed with canonical placement, private modes, escaping and cleanup: PASS |
| C3 restricted reader / ordinary database owner | Exact submitted production, unchanged new test: both refusal assertions failed, `error=<nil> snapshotExported=true` | Both return `ErrUnsupported` before snapshot/dump and leave no accepted completion: PASS |
| C3 other-database subscription | Exact submitted production positive control passed | Export and package check pass; other subscription unchanged: PASS |

Raw qualifying red runs are `red-c1-exact-submitted`,
`red-c2-instrumented-old-algorithm` and `red-c3-isolated-exact-submitted` in
`logs/`. C2 is explicitly an instrumented old-algorithm reproduction, not an
unchanged-production run. Its exact diff and supplied mutation are in
`probes/`; neither mutation nor supplemental observation test entered the
candidate. The real global `/tmp` layout was never changed.

A supplemental, bounded submitted-source observation reproduced the C3 data
loss: export reported Complete/Published, the package checker reported byte
completion, and the metadata archive omitted `CREATE SUBSCRIPTION`. A matching
owned-cut `pg_dump` exited zero with the 83-byte warning
`pg_dump: warning: subscriptions not dumped because current user is not a superuser`.
Only bounded nonsecret output was recorded. That observation test's PASS means
the submitted defect was confirmed, not that submitted behavior was accepted.

Two initial PostgreSQL runs (`red-c3-exact-submitted`, `green-targeted`) ran
concurrently against the same disposable cluster. Their cluster-wide role
fingerprints interfered, causing preservation assertions to fail. Those runs
are retained as **nonqualifying orchestration failures**, not defect proof or
passing evidence. Sequential reruns used unchanged tests and produced the
qualifying red/green results above. No fixture or preservation assertion was
weakened. The initial lint finding and its one-line correction are also kept.

All following checkpoint results are **new executions** bound to the clean
implementation commit in `commands.jsonl` (commands, UTC, exits and source
identities accompany full raw logs):

| Validation | Measured result |
| --- | --- |
| Combined recovery/coordination runner, race/count-one/4-minute profile | 309 run/pass pairs: 211 recoveryexport, 98 coordination; two package passes, zero failures/skips/missing |
| Named runner requirements | All 103 previous plus all 32 supplied new events enforced; 135 required named events |
| Missing-new-event probe | Removing the database-owner subscription event from the actual stream makes the checker exit 1 and name the missing event; synthetic harness proof, not another runtime test |
| Unchanged current product oracle, race | All 23 expected outcomes: 12 RESTORED, 9 REFUSED, 2 KNOWN_LIMIT; fixture/oracle bytes unchanged |
| Build, vet, pinned lint, module tidy diff | PASS; zero lint issues and no dependency changes |
| Authority/governance and focused authority tests | PASS; 24 focused tests newly executed |
| Local CI enforcement audit and relevant mutation tests | PASS; recovery-copy and product-fixture removal/masking checks retained |
| Go formatting, shell syntax/ShellCheck, whitespace, validation matrix, versioned row writers, smart quotes | PASS |
| Source, frozen-history and worktree preservation | PASS |

The combined runner newly re-executed retained plaintext/encrypted round trips,
same-snapshot and closure checks, source preservation, child/cleanup barriers,
publication failures and completed-package corruption checks, including the
accepted B1–B3 regressions. There was no second full passive replay to inflate
coverage. Final authority/governance diagnostics and preservation are captured
again on the clean report commit; no production or test changes follow the
tested checkpoint.

Evidence explicitly **reused**, not rerun here, is identified with original
commands, source identities, raw bytes and hashes in `reused-evidence.json`:

- Submitted full Python run: 542 tests at
  `ece9602f0b2ee5b5dbd579c87557c082c9ffcc8a`, tree
  `edde9e86c840874d9342ea80aa211c97d22c20ae`. All 22 `scripts/*.py` files remain
  byte-identical. Current validators and the 24 focused tests above are new.
- Unchanged broad catalog/container/storage/verify/recovery/db evidence:
  1,277 passes and 11 skips at `2e31dde54bccc31189ffc96561feacd31d27904f`.
  Six compression-gated cases were separately covered by the retained 12-event
  run at `1f46389e99f8f41a8886c35c853635ed66af694f`. The five other skips remain
  skips; they are not promoted to passes.
- Accepted Phase 1A D19 baseline execution and 23-case fixture results. Only
  the current-reader oracle was newly executed here.

Runtime: Linux x86_64; installed Go **1.26.10**, `GOTOOLCHAIN=local`,
`GOWORK=off`, `CGO_ENABLED=1`; pinned lib/pq **v1.11.2** and golangci-lint
**2.9.0**. Disposable PostgreSQL server **16.13** used only owned tmpfs data on
127.0.0.1:55486. Explicit `/usr/lib/postgresql/16/bin/pg_dump` and `pg_restore`
were both **16.15**. These are observed major-matched versions, not a claim of
identical patch releases. Binary/source paths, image digest and source hashes
are in `environment.json`. Pinned `encoding/json/fold.go`, `database/sql` and
`os.Root` source identities were recorded; accepted B1 lifecycle source remains
unchanged.

Preservation checks cover 1,670 unchanged tracked paths and all 849 frozen v1
files, aggregate SHA-256
`64fb69cb875c4bdf96c10cd759275b7232849c2f8083b0b406f11d7b18b23c7a`.
All 20 pre-existing worktrees retain their entry HEAD/tree/status, including
submitted Phase 1C and accepted predecessors. Nine already-absent temporary
worktree registrations were recorded and preserved, not pruned. Schemas,
migrations, payload/package formats, module pins, fixtures/oracle, current
authority and B1 source/tests remain byte-identical. The submitted report hash
remains `dda45a641813da473370bcf412c176b40fcb0fd96da99ea9b538a94cb0db0d90`.
`LOCAL_ACTION_2_03_AUTHORITY_ONLY` remains a structural label, not a product or
recovery certificate. Existing instruction mirrors need no authority change.

Cleanup passed. Before server removal, queries found zero fixture databases,
roles, subscriptions, subscription workers, replication slots and owned
sessions. The verified owned tmpfs container and both disposable probe
worktrees were removed; 84 working fixture files (19,361,351 bytes) were removed.
No private export credential directories remained. The operational credential
scan found zero matches in 77 evidence files, and the private credential file
was removed. The candidate, predecessor worktrees and raw evidence remain.

The external delivery contains `scenario-results.json`, `review.patch`,
ordered `commits/` patches, `final-identity.json`, raw logs/events, proposal and
probe receipts, reuse provenance, preservation, cleanup, final diagnostics and
`coldkeep-phase1c-followup-evidence.zip` with complete internal checksums and an
external ZIP digest. Both incremental and ordered patch forms are independently
replayed through fresh Git indexes and required to match the final tree.
No databases, payload copies, operational credentials or raw encryption keys
belong in the evidence archive; any redaction is accounted for separately.

Byte completion and test-only metadata restoration still do not prove product
recovery. After review, the next separate BUILD unit should use **GPT-6 Astra /
Extra High** for constrained import and recovery from the completed generation
plus independently held keys/readers, with the original installation unavailable
and after later live GC. Key/reader custody, independent storage and historical
reconstruction remain explicit obligations. Parent 2.03 and product gates stay
open; 2.04 remains blocked pending integrated recovery proof.
