# Phase 1B follow-up result — 8 November 2026

**B1–B3 follow-up unit: COMPLETE as a local review candidate. Parent action
2.03: ACTIVE. Product gates: INCOMPLETE.** No recovery package, independent
restoration or publication is claimed. Accepted Phase 1A / 2.01 remains unchanged.

The [submitted Phase 1B report](phase1b-result.md) remains byte-identical as
execution history. Its old completion claim preceded the review findings;
this result qualifies its lifetime, migrated-layout and catalog-visibility
claims. The submitted tests did not fail retroactively. New counterexamples
and corrected executions are distinguished below.

## Entry and source binding

The operative instruction was `coldkeep_phase1b_followup_codex_2026-11-08.md`
inside the supplied correction ZIP. The ZIP SHA-256 is
`fa909e5300ba1bc48cec56b9b5afde3808bcb064119bee872989b2749582417d`;
all 23 manifest entries and ZIP integrity verified. The checkout at
`/home/franchoy/vscode/coldkeep-worktrees/v2.0-phase1b` was clean at expected
HEAD `f6ef2456fd8ec90b68f246c45ca053fd66b6a39d`, tree
`f4dd6e4ed2e0b1e783769911d43d1e878857bed2`.

Work used the separate `release/v2.0-phase1b-followup` branch/worktree at
`/home/franchoy/vscode/coldkeep-worktrees/v2.0-phase1b-followup`.
The submitted worktree, accepted Phase 1A at
`cc2c175549b9a185de8c79943ae5855a4aa8d44f` and main at
`e8a501d6edbc32cd440562065f1a851d8afdc03d` retain their entry identities and
clean status. No reset, push, merge, tag, release or deployment occurred.

Clean implementation checkpoint:
`1f53d77dfc120e26feed31a44c1612eb94798a74`, tree
`e12c5ba2c348f9028dfe1d61e76320c2ed2b795a`.
The final focused suite and applicable gates ran against this checkpoint.
The subsequent completion commit adds only this report. External
`final-identity.json` records the final commit/tree, ordered local commits,
clean status and documentation-only delta without a self-referential identity.

Requested settings: GPT-6 Astra / Extra High (`gpt-6-astra`, `xhigh`).
Actual session model/effort were not observable or switchable through available
controls; they are recorded as unknown. No global/account settings changed.

## B1–B3 dispositions

| Finding | Disposition and implementation |
|---|---|
| B1 — actual cleanup before lease release | PASS. One physical driver connection is opened under ownership and supplied once to database/sql. Reconnection is refused. Cleanup seals the connector, closes the pool, and waits for the actual driver Close completion channel, which follows transaction release. Automatic rollback and driver-close errors are retained. Owned sockets and cancellation callbacks are closed/joined before releasing the lease. |
| B2 — supported migrated layouts | PASS. The supplied runtime comparison patch is applied unchanged: exact column names and supported types are matched independently of physical order. The checked-in v7 migration produces schema 17 with `snapshot_file.path_id` last; history and whole-object closure are admitted without changing the source during inspection. Missing/renamed/extra/wrong-type fields still refuse. |
| B3 — complete catalog visibility | PASS. The supplied `row_security=off` session setting and verification are retained alongside B1. Restricted policy-filtered reads refuse before consumption. An already-authorized owner sees the complete catalog. Role, membership, policy, ACL, ownership, RLS, schema, catalog and payload/control fingerprints remain unchanged across inspection. |

B1 is specific to installed Go 1.26.10 and pq v1.11.2. Local source hashes match
all four exact dependency files recorded in the review. Go's transaction and
connection done bits precede actual cleanup; neither sentinel errors nor
`DB.Close` are treated as a join. pq's connection cancellation watcher also
lacks a completion join. The adapter therefore omits that watcher for BEGIN,
while keeping database/sql's cancellable transaction and automatic rollback.
Queries internally use short-lived prepared statements, whose pinned pq
cancellation path joins its cancel request when the query/rows finish. Every
statement is closed with its rows. No prepared statement capability escapes.

The primary socket receives an I/O deadline five seconds after operation
cancellation, covering stalled connection startup, BEGIN and cancellation
transport failure. Cancel-request sockets keep pq's bounded dial/deadline.
Deadlines interrupt I/O; only actual driver/socket closure and joined callbacks
end ownership. Cleanup may outlast the operation work deadline. Resolvers and
the synchronous consumer must still honor cancellation. This is a local
cooperative ownership contract, without a remote-server fencing or network-
partition availability claim. Real PostgreSQL cancellation tests observe zero
inventory sessions after return; mock wire peers separately test stalled I/O.

Both catalog proposals passed application checks. B2 production bytes match the
proposal exactly. B3's production edits were reconciled with B1. Proposed tests
retain their assertions, with added observations, a consumer error return in
place of `Fatal` inside the restricted callback, and stronger preservation
fingerprints. No dependency, schema or migration SQL changed.

## New scenarios and outcomes

| Scenario | Submitted implementation / counterexample | Corrected outcome |
|---|---|---|
| Automatic rollback held in flight | Actual inspection returned incomplete before rollback/driver close finished | Inspection cannot return; passive and legacy acquisition remain excluded. After rollback, driver Close is held and exclusion persists. Actual close permits incomplete return and reacquisition. |
| Automatic `ErrBadConn` close held in flight | Actual inspection returned before driver close finished | Ownership and operation remain held until actual close completes; automatic rollback error is retained. |
| Cleanup errors | Automatic rollback error was lost | Explicit and automatic rollback errors plus driver-close errors survive; result remains incomplete. Existing cleanup-error injection also prevents an otherwise completed inventory. |
| Supported migrated column order | Refused `snapshot_file` columns | Checked-in migration ends at 17 with `id snapshot_id logical_file_id size mode mtime path_id`; inherited history and one whole object admitted. |
| Missing/renamed/extra/wrong-type columns | Existing and new refusal controls | Refused; consumer not called. Fresh schema still succeeds. |
| Policy-filtered catalog | Restricted reader reached the consumer with a filtered graph | Control sees zero containers with normal RLS; inspection refuses with `Complete=false` and no consumption. |
| Already-authorized owner | Control sees one container | Full seven logical files, 35 chunks and one whole object retained; policies and privileges unchanged. |
| Real PostgreSQL active query cancellation/deadline | Newly added execution | Query observed in `PgSleep` before cancellation; incomplete result, joined cancellation, zero inventory sessions after return. |
| Stalled startup / BEGIN | Newly added pinned-driver wire-peer tests | Cancellation bounds blocked I/O; actual socket closes and observer joins; incomplete result. These are controlled wire peers, not an additional real-server qualification. |
| Partial open, query/read-only refusal, consumer failure, row/consumer cancellation, successful closure | Retained and expanded scenarios | All pass; `runPreserved` now also checks zero inventory sessions after every inspection. Server write refusal is asserted as PostgreSQL SQLSTATE 25006, with undefined-table 42P01 as the query-failure control. |

The final B1 regressions were also run against the submitted `inventory.go`
cleanup, adding only a private connector-injection seam. Both blocked-cleanup
cases fail by observing early return; automatic rollback-error preservation
fails too. The unchanged corrected tests then pass. This is production-path
regression evidence, separate from the standalone standard-library probes.
Those probes were newly executed: their first two PASS results mean the unsafe
assumptions reproduced, not Coldkeep acceptance.

An initial fake connector accidentally also implemented `io.Closer`; Go called
it from `DB.Close`, causing two timeout failures in the first B1 baseline run.
The connector and connection were separated, and the final baseline run above
records the intended early-return failures. Both runs remain in raw evidence.
The initial catalog failures and intermediate passing checks also remain.

## Validation, preservation and limits

New final execution used Linux x86-64, installed Go 1.26.10 with
`GOTOOLCHAIN=local`, `GOWORK=off`, CGO=1, pq v1.11.2 and golangci-lint 2.9.0.
Disposable PostgreSQL 16.13 used loopback port 55484 and tmpfs storage, image
`postgres@sha256:2586e2a95d1c9b31cb2967feb562948f7d364854453d703039b6efa45fe48417`.
Each fixture created/dropped a unique catalog and reader role. No user catalog,
repository or production key was used. Final cleanup checks and container
removal are recorded outside the source tree.

| Gate | Evidence disposition |
|---|---|
| Final focused race suite | NEW PASS: 179 distinct run/pass pairs, 87 recoveryexport and 92 coordination; two package passes; zero skips/failures/missing events. All corrective top-level tests and their bounded subcases are required by the existing blocking runner. |
| Current fixed product oracle | NEW PASS: 23/23, including 12 RESTORED, nine REFUSED and two KNOWN_LIMIT, under race detection at the clean implementation checkpoint. |
| Build, vet, pinned lint, tidy diff | NEW PASS; zero lint issues and no module drift. |
| Authority/governance and focused authority regressions | NEW PASS; 24 cases, including scope/publication refusals. Parent 2.03 ACTIVE and `LOCAL_ACTION_2_03_PASSIVE_INVENTORY_ONLY` unchanged. |
| Local CI audit and product-fixture failure-mask mutations | NEW PASS. Existing workflow/failure propagation retained. |
| Formatting, ShellCheck, shell syntax, smart quotes, validation matrix, row-writer guard, diff whitespace | NEW PASS. Final preservation checks parse every shell file separately. |
| Complete Python suite | REUSED: submitted 542 passing cases. Python implementation/tests are unchanged; original receipt is implementation-stage and is not relabeled as a final-source run. |
| Container/catalog/storage/verify/recovery/db regressions | REUSED: 1,277 passes at `2e31dde54bccc31189ffc96561feacd31d27904f`, with 11 recorded skips. The six compression-gated cases were subsequently exercised in 12 passing events at `1f46389e99f8f41a8886c35c853635ed66af694f`; five other skips remain skips. Those packages are unchanged. |
| Exact D19 fixture reader | REUSED: accepted Phase 1A baseline execution and unchanged 23-case oracle. No new historical-reader execution claimed. |
| Preservation | NEW PASS: accepted fixture/contract/schema/pin bytes, submitted report, predecessor worktrees and all unchanged tracked files. All 849 frozen v1 files retain digest `64fb69cb875c4bdf96c10cd759275b7232849c2f8083b0b406f11d7b18b23c7a`. |

Full release/stress/platform/hosted qualification is not rerun or claimed.
Native Windows/Darwin runtime remains unqualified; previous coordination
cross-compilation is reused only as historical evidence. Passive SQLite is
not implemented. The [API contract](passive-inventory.md) retains its bounds,
local PostgreSQL 16 profile, explicit configuration, missing-lock preparation
refusal, full-catalog scope and diagnostic-only output.

## Artifacts and next handoff

Evidence root:
`/home/franchoy/.local/state/coldkeep-release-evidence/v2.0/phase1b-followup-20261108`.
It contains `final-identity.json`, `review.patch` from the submitted HEAD,
ordered local commit patches, `scenario-results.json`, commands/exits with
source bindings, initial failures, final raw events, dependency provenance,
input verification, preservation, cleanup and explicit reused-evidence records.
`coldkeep-phase1b-followup-evidence.zip` has an internal checksum manifest and
an external SHA-256 receipt. Disposable product fixture copies are removed;
the compact bundle retains their raw events and outcome/hash records.

The [next handoff](next-slice.md) retains explicit coordination preparation,
a fresh owned full-catalog cut and whole-object copying, PostgreSQL dump/restore
compatibility, staged completion/interruption/corruption refusal, independent
key/reader custody, constrained fresh-destination import, restoration with the
original installation unavailable, and survival after later live GC. These
integrated 2.03 obligations still precede schema additions. Next recommendation:
GPT-6 Astra / Extra High for the separately authorized recovery-copy/completion
BUILD unit, followed by constrained import and isolated restoration. None of
that work starts here.
