# Phase 1D follow-up result — 8 November 2026

F1: CORRECTED AND QUALIFIED
F2: CORRECTED AND QUALIFIED
Local-profile action 2.03: COMPLETE
Profile: Linux/manual PostgreSQL 16
Product gates: INCOMPLETE
Implementation: ACTIVE

The conditional completion authority in `coldkeep_phase1d_followup_codex_2026-11-08.md`
is exercised after the submitted-source counterexamples, corrected production
paths and retained local gates passed. Action 2.03 remained ACTIVE in the correction
checkpoint. The submitted [Phase 1D report](phase1d-result.md) and all predecessors
remain byte-identical historical evidence. Neither 2.02 nor 2.04 began. This result
does not authorize adoption, publication or complete-product claims.

## Identity and review artifacts

- Submitted HEAD: `c26f608cdc9e14d62597862ff81c138978f95ab0`.
- Submitted tree: `3ffa87b30b8bc9b4b15b2ae9775d47aad6d9be92`.
- Clean qualifying checkpoint: `d0dbf79c13a8ba0eaa39cde0b68ce0f602cbe895`.
- Checkpoint tree: `bdedfef0578ef8604804a39c8a4fe2c5cc5b0c26`.
- Checkpoint subject: `fix(recovery): align receipt capacity and honor owned inventory deadlines`.
- Branch: `release/v2.0-phase1d-followup`; worktree:
  `/home/franchoy/vscode/coldkeep-worktrees/v2.0-phase1d-followup`.
- Final HEAD/tree and ordered local commits are in external `final-identity.json`;
  the report cannot embed its own containing commit without self-reference.
  The final commit changes only current report/authority/charter/handoff prose.
  Runtime, tests, runner, dependencies and authority implementation retain the
  qualifying checkpoint bytes. Version/module/latest stable remain unchanged.

Evidence root: `/home/franchoy/.local/state/coldkeep-release-evidence/v2.0/phase1d-followup-20261108`.
All relative evidence references below are beneath this root and the ZIP root.

- [Final identity](/home/franchoy/.local/state/coldkeep-release-evidence/v2.0/phase1d-followup-20261108/final-identity.json)
- [Incremental review patch](/home/franchoy/.local/state/coldkeep-release-evidence/v2.0/phase1d-followup-20261108/review.patch) and [ordered commits patch](/home/franchoy/.local/state/coldkeep-release-evidence/v2.0/phase1d-followup-20261108/ordered.patch)
- [Compact evidence ZIP](/home/franchoy/.local/state/coldkeep-release-evidence/v2.0/phase1d-followup-20261108/coldkeep-phase1d-followup-evidence.zip)
- [Exact commands, source binding and log digests](/home/franchoy/.local/state/coldkeep-release-evidence/v2.0/phase1d-followup-20261108/commands.jsonl)
- [Verified results](/home/franchoy/.local/state/coldkeep-release-evidence/v2.0/phase1d-followup-20261108/verified-results.json) and [artifact replay/equality](/home/franchoy/.local/state/coldkeep-release-evidence/v2.0/phase1d-followup-20261108/artifact-verification.json)

Requested settings: GPT-6 Astra (`gpt-6-astra`), Extra High (`xhigh`). Actual model
and effort are not observable and remain null in `settings.json`. No global
settings changed and no subagents were used.

The follow-up ZIP matches SHA-256
`633dd040c9a486e3a0cb5ad10a6d1e5b8d74ddae48e20a60e4b964d4720aac1b`.
All 33 outer and 128 nested checksum entries were verified, including complete
coverage and standalone/nested report, identity and patch equality. Selected
source snapshots and clean submitted HEAD/tree were verified before creating
separate correction/probe worktrees. Archived execution/cleanup scripts were
read only; the submitted patch was not reapplied. See `launch-verification.json`.

## Submitted failures and production corrections

The supplied Go files were initially uncompiled/unexecuted proposals. The Python
size witness remains a data-only calculation, never a valid package or runtime
proof. Exact Go overlays were copied without adaptation to an owned detached
checkout of submitted HEAD. `probe-overlay.json` records their hashes and zero
production drift; `probe-overlay.patch`, `input/probes/` and both red logs retain
the source/command binding. Both probes compiled and reached their intended
assertions with race enabled:

- F1: `TestRecoveryReaderReceiptWithinAdvertisedScale` failed on a 1,943,667-byte,
  6,000-object receipt with `unexpected EOF`. The actual `restore` consumer also
  failed decoding before its deliberate ambient-setting refusal. These pure
  tests establish the reader cap; they do not claim an authenticated package.
- F2: `TestRecoveryImportHonorsInventoryDuration` lowered only inventory duration
  to one nanosecond. Submitted import returned `Published:true Ready:true` with
  no error, failing the deadline expectation. Source/package fingerprints and
  owned-session checks ran through the existing preservation helper.

F1 now uses `recoveryexport.ReadImportReceipt` from the actual restore command.
The shared finite byte ceiling is **33,619,968 bytes (32 MiB + 64 KiB)** and the
object ceiling is **20,000**. The object array is already serialized inside the
unchanged 32 MiB inventory diagnostic. The extra reserve covers all other
receipt fields: target root and host can each be 4,096 bytes, expanding at most
sixfold; database/role are at most 63 bytes, key reference 128, and remaining
labels/digests/integers are fixed or bounded. A worst-case escaping regression
measures 50,760 bytes of non-object overhead, below 65,536. Rows, directory entries
and the 20,001 archive-plus-object member limit independently bound the count.
No package capacity, schema, receipt shape or persisted encoding changed.

The anchored reader requires regular single-link input, checks actual bytes
through physical EOF with cancellation, then performs token traversal before
allocating typed Objects. Alias-aware counting includes `OBJECTS` and Unicode
`objectſ`; folded duplicates also refuse. Existing depth/string/key budgets,
strict unknown fields, truncation and trailing-input refusal remain. Exact byte
and object boundaries pass; genuine one-over boundaries refuse with the typed
slice unpopulated. Tests cover cancellation before/during traversal and unsafe
input forms. The separate operator-config cap remains 1 MiB.

The producer checks object count and validates the serialized receipt through the
same consumer contract **before writing the pending marker**. Injection at the
real importer boundary demonstrates byte/count overflow leaves neither pending
nor ready receipt and returns no publication; normal receipt publication and
production readback pass. These over-budget injections exercise the explicit
boundary; upstream profile limits mechanically keep ordinary admitted receipts
within it. The green helper regression was adapted to the dedicated production
reader; the actual restore-consumer test and integrated operator test also pass.

F2 adds the inventory timeout at `inspectOwnedCut` after input validation and
before active work. An immediate context check refuses already-expired work.
The parent/global deadline cannot be extended. The nanosecond import now returns
an error wrapping `context.DeadlineExceeded`, publishes nothing, preserves source
and package and leaves zero owned sessions. Deterministic connector observations
cover earlier-parent, lower-inventory and expired-parent cases. All accepted
rollback/driver/socket, child, credential and late-publication regressions pass;
`catalog_session.go` and the cleanup machinery are byte-identical. Expiration
ends work permission; it does not release ownership before actual cleanup joins
or promise a hard wall-clock return from kernel I/O.

## Actual supported-scale recovery

`TestRecoveryImportReceiptScaleRestore` exports a real valid 3,624-row catalog
with 3,602 independently copied objects. Its 3,600 additional header-only
containers have real version/max-size/CRC bytes and catalog rows. The other two
objects retain nonempty snapshot history. All existing limits and timeouts are
unchanged. The actual staged operator command imports into a fresh ordinary-owner
destination, then restores selected history with the source installation
unavailable in its namespace. The resulting full receipt is **1,160,272 bytes**;
all objects, including final ID 4599, are retained.

Three restored files were independently reread by the parent against the fixed
old-writer oracle, including the nonempty `history/Case.txt`:

`expected = observed = 8950abfda7b727630760dd35bcf5c3daa7631aff223a90f7728c0d2521dde10c`

The scale generation is `cf82adcae137112a8d4cb65c3a38ce08`, manifest SHA-256
`51f5e1c0a43e00200b78e963023ad6ba210dc1d7700fac64f68d2e50ee3c117d`.
`acceptance/receipt-scale.json` records full import/restore results, expected and
observed hashes, target/custody/isolation facts and reader identity. Mode 0640 and
mtime `2025-01-02T03:04:05Z` match separately. Source/package fingerprints remain
unchanged; pins and remaining owned sessions are zero. The staged reader SHA-256
is `de0a5e7896b9e1ea13e899fea44f4194a60a4413e9df2c2e78fd92c8f595929f`.

## Fresh validation and explicit reuse

All qualifying commands ran against the clean checkpoint and exited zero.
`commands.jsonl` binds exact argv, HEAD/tree/status, times, exit and raw-log hash.

| Check | Result |
| --- | --- |
| Recovery/export/import/coordination and command package, race/count=1 | 418 passing events: 293 recoveryexport, 98 coordination, 27 command; zero failures/skips |
| Required events | All 244 present with one run/pass each; all 206 original requirements retained, 38 added |
| Current fixed product oracle, race | 23 matched: 12 RESTORED, 9 REFUSED, 2 KNOWN_LIMIT |
| Authority and runner Python | Fresh 25 authority and four real-runner tests passed |
| Reader-specific runner mutations | Complete coverage passes; missing reader event, reader skip and nonzero Go exit refuse |
| CI enforcement | Existing six recovery-step mutations and local audit passed |
| Build/vet/pinned lint | Passed; Go 1.26.10 and golangci-lint 2.9.0 built with Go 1.26.10 |
| Formatting/shell/guards/dependencies | Passed; no dependency drift; runner shellcheck, shell syntax, matrix, row-writer, smart-quote and whitespace checks passed |
| Current and explicit historical authority/governance | Passed; final diagnostics freshly bind final clean documentation commit |

Original-loss plaintext (three files), encrypted/mixed (eight) and fresh recovery
after real non-dry live GC (eight) all ran again. GC reclaimed both exported
source objects, **465,863 bytes**, with zero old recipes remaining; the unchanged
pre-GC generation was freshly imported and its eight old files reconstructed.
Together with scale recovery there are 22 independently reread final-file
observations. Full raw records remain in `acceptance/`; they are not substituted
by the submitted review summaries. Physical independence remains **unknown**:
same-host functional isolation does not qualify host/disk-loss survival.

`reused-evidence.json` explicitly retains the submitted **547-test Python suite**
and **35 affected storage/restore/snapshot/engine/maintenance events**, with
original commands/checkpoints/raw-log digests and unchanged source/dependency
checks. Python and authority implementation did not change; the changed shell
runner's dependencies have fresh focused and reader-specific mutation coverage.
Storage/snapshot/engine/maintenance source is unchanged, while changed recovery
paths have fresh integration coverage. These are reused results, not new runs.
The exact D19 oracle and earlier 1,277-pass/11-skip broad and 12-pass compression
records also remain historical. Six compression skips have historical coverage;
five others remain unqualified (Darwin path behavior, CAS interleaving,
concurrent workers/GC-plan store, two unsupported-codec verification cases).
See `reused/historical-skip-coverage.json`. No unrelated full release/platform/
stress certification was repeated or claimed.

Observed Linux amd64; Go 1.26.10 with GOTOOLCHAIN=local, GOWORK=off and race CGO=1;
static staged reader CGO=0. Host PostgreSQL tools are 16.15, disposable servers
and isolated tools 16.13. The unchanged PostgreSQL 16 image digest is
`sha256:2586e2a95d1c9b31cb2967feb562948f7d364854453d703039b6efa45fe48417`.
`environment.json` records actual versions and tool hashes. This is local,
not hosted or full-platform evidence. The historical exhausted-storage case
remains accurately scoped to connection refusal, not a pg_restore WAL experiment.

Development failures are retained: an unused import after moving receipt encoding,
a default PATH linter built with Go 1.25 (replaced by the existing pinned binary),
and a redundant test type annotation caught by pinned lint. The initial archive
comparison used a nonexistent source/report path before correcting to its actual
root entry; complete verification preceded worktree creation. No qualifying run
failed and no assertions were weakened to make defect probes pass.

## Preservation, cleanup and handoff

All 849 frozen files retain aggregate SHA-256
`64fb69cb875c4bdf96c10cd759275b7232849c2f8083b0b406f11d7b18b23c7a`.
Schema/dependency/fixture/oracle bytes, accepted cleanup/visibility/layout tests,
submitted report and all predecessor reports remain unchanged. All 22 existing
predecessor worktrees, nine already-absent registrations and preexisting branches
retain their recorded identity/status. No pruning, reset, push, merge, tag,
release, deployment or publication occurred. See `preservation-final.json`.

The owned server was identified by exact ID/label before removal. Only its
postgres/template0/template1 databases and setup role remained; other client
sessions, subscriptions, prepared transactions and replication slots were zero.
All 11 unrelated container IDs were preserved. Recovery fixture containers and
children had joined. The owned probe checkout was removed only after verifying
unchanged production and exact overlay hashes; its overlay and red evidence are
retained. Only the recorded private credential/tool directory and disposable
product working copies were removed. Source fixtures remain untouched.
`cleanup-fixture.json` and `credential-cleanup.json` record these boundaries.

The compact evidence excludes credentials, keys, binaries, working payload/catalog
copies and nested historical archives. `secret-scan.json` records actual-secret
checks without retaining secret values. `SHA256SUMS` covers all regular ZIP
members except itself; standalone report/identity/patches equal their archived
copies. Incremental and ordered patch replay reproduce the exact final tree.

The [handoff](next-slice.md) retains 2.02, 2.04–2.13, 2.15, 2.17 and separate
publication authority as outstanding. `LOCAL_ACTION_2_03_AUTHORITY_ONLY` is still
structural authority, not runtime certification; no validator implementation or
parallel authority ledger was introduced. No later action or mandatory additional
closure-only build is authorized by this result.
