# Phase 3A follow-up result

Date: 9 November 2026. Mode: BUILD.
Owner: `coldkeep_phase3a_followup_codex_2026-11-08.md`.
Phase 3A follow-up A1–A2: COMPLETE, awaiting independent review.
Parent action 2.04: ACTIVE. Implementation: ACTIVE. Product gates: INCOMPLETE.

A1 exact relation-bound constraint set: PASS.
A2 bounded metadata before deparsing/transfer: PASS.
Fresh recovery qualification: PASS.

Both submitted defects were reproduced against unchanged production source in an
owned probe worktree. The correction qualified in a separate worktree. Completion
uses the owning BUILD's conditional authorization; it clears only the A1–A2
obligation. The submitted report remains immutable. No Phase 3B/3C work began.

## Identity, inputs and preservation

| Identity | HEAD | Tree |
|---|---|---|
| Submitted Phase 3A start | `8dad297a812201f7a0e55e7d2858b3fc7e6fa59b` | `12c62b2a8313efa874b57e37b9dd8558fff6fc69` |
| Clean qualified implementation | `4f3f6aa260e91eac13c8ce5469c8b18073b507dc` | `f622506b07f99b5e27f087103901cf01c0db10fc` |

The final documentation-only commit and exact final identity are recorded outside
the commit in `final-identity.json`. Its delta from the qualified checkpoint is
restricted to this report, `README.md` and `next-slice.md`. No runtime evidence is
represented as having run at a different revision. Branch:
`release/v2.0-phase3a-followup`; worktree:
`/home/franchoy/vscode/coldkeep-worktrees/v2.0-phase3a-followup`.

Requested settings throughout: GPT-6 Astra / Extra High. Observable actual model
and effort: **unknown**; this interface supplies no trustworthy active-model or
effort telemetry. No switch or global configuration change is claimed.

Verified the launch ZIP SHA-256
`afdaf0a3e8c06dc7025c41ab012835d1e4f4324a884aaea71887ae1aeba30784`,
safe unique paths, CRC and all 96 internal checksums covering its 97 members.
Read the README, review, owning follow-up BUILD and launch identity before edits.
The proposed design/tests were inspected and adapted; their bundled presence was
never treated as executed evidence. The separately submitted evidence ZIP is
2,572,305 bytes, SHA-256
`002b53b39ab0d9cbbb37fbbcfed3ff3483670b637361ddd0320bd1ef4721676c`;
all 364 checksums and 365 members verified. All 1,751 submitted checkout files
matched its source ledger; all 57 selected source copies matched too.

Inventories preceded either new worktree and all test-resource creation. Subsequent
checks preserved 35 old worktree registrations (including 9 already absent), 132
old refs, 1,751 submitted and 1,740 accepted pre-3A source hashes, all 12 predecessor
result reports, and the 849 frozen historical files. Their aggregate SHA-256 stays
`64fb69cb875c4bdf96c10cd759275b7232849c2f8083b0b406f11d7b18b23c7a`.
The submitted `phase3a-result.md` remains byte-identical, SHA-256
`bb50da22c6638bb38351d01979f26c2c27e8c091fdb2a6e1d864f214feb3fe23`.
All 11 pre-existing containers, 7 networks and 10 image inventory entries remain.
No prune, predecessor reset, ref rewrite or historical-report edit occurred.

## Reproduced submitted failures

The detached probe retained the submitted HEAD/tree and changed no tracked source;
only the adapted test overlay was added. `probe-before` ran before production edits.
An additional `probe-confirmed-before` run supplied direct complete probe-file
hash inventories before and after execution. This confirmation occurred later;
it does not change the chronology of the original reproduction. Both runs failed
the same seven negative regression leaves for the intended defects while their
valid controls passed. Raw outputs and both bindings are retained.

| Submitted counterexample | Actual unchanged-source observation |
|---|---|
| A1.1: source repository FK moved onto device under its original name | 53 constraints and 53 unique expected names remained; source FK absent, device carried both FKs; inspection consumed the malformed profile. |
| A1.2: missing source FK padded by an allowed CHECK name on device | 53 constraints, 52 distinct names; inspection consumed the malformed profile. |
| A1 representative export/import | Malformed export reported complete; actual restored malformed import reached readiness. Negative assertions failed for those results. |
| A2 1 MiB repeated literal, expected name | Definition 1,048,637 bytes; stored `conbin` 48,393, logical 4,195,189, compression `pglz`; oversized definition crossed the driver before refusal. |
| A2 1 MiB literal, unexpected name | Definition 1,048,637 bytes; stored 48,391, logical 4,195,191, `pglz`; oversized definition crossed the driver. |
| A2 measured 16 KiB literal, expected name | Definition 16,445 bytes; stored 1,133, logical 66,418, `pglz`; stored bytes fit the proposed 4,096 ceiling while logical bytes did not. |

The driver observer wraps real pq preparation, statement query and `Rows.Next`;
it records values before production `Scan` and string comparison. Fixture-only
queries compute definition lengths/digests on the server, returning bounded
observations. These oracle queries deliberately inspect the large definition;
they are distinct from the measured production admission path. Logical catalog,
relation-bound constraint digests, payload/control trees and applicable receipts
are checked before/after; incidental PostgreSQL statistics/WAL are not treated as
immutable repository data.

## Correction and metadata envelope

Production changes are confined to `internal/recoveryexport/profile.go` and
`profile_constraints.go`. The exact 53 canonical definitions are unchanged.
Authority is an explicit `(public relation, constraint name)` map carrying kind
and canonical definition. A seen-key set rejects repeated/unexpected keys and
requires every expected key; neither count nor a name prefix grants authority.
The trusted namespace join and recorded relation OID bind each admitted entry.
Validated, immediate, local, non-inherited canonical status is required, including
the canonical CHECK `NO INHERIT` policy. Existing column and row-semantic checks
remain in place.

Admission now has two stages:

1. Read bounded fixed metadata for at most 54 records (53 plus one overflow
   observation), without sorting/deparsing definition text. Check the complete
   relation/name/kind/status set and raw input representation first.
2. For each of the 53 admitted OIDs, repeat the raw/status/relation guard in a
   row-dependent SQL `CASE`, then compare `pg_get_constraintdef` to the fixed
   canonical definition on the server. Only a boolean crosses the driver.

The independent metadata ceiling is **4,096 raw variable-input bytes per
constraint, 217,088 aggregate for 53 admitted constraints**. It does not borrow
or raise catalog-row limits. The sum includes `conbin`, `conkey`, `confkey`,
`conpfeqop`, `conppeqop`, `conffeqop`, `confdelsetcols` and `conexclop`, using
`COALESCE(pg_column_size(original_datum)::bigint+4,0)`. The extra header allowance
is conservative. Every field must have NULL `pg_column_compression`; compressed
metadata is unsupported, even when its stored length is small. Names use
PostgreSQL's bounded `name` type; definitions and attacker literals are absent
from application diagnostics and returned values.

The representation proof uses the pinned PostgreSQL 16.15 sources, not a cast or
deparsed string as its input-size guard. `pg_column_size` passes the original
Datum to `toast_datum_size`, which reads an external value's recorded stored
length rather than its pointer length. `pg_column_compression` checks inline or
external compression metadata without expanding the value. Rejecting compression
therefore allows the stored-length bound to cover supported uncompressed inline
and external representations. The row-dependent `CASE` places these guards before
the deparser's `TextDatumGetCString` / `stringToNode` path. Primary references:
[TOAST](https://www.postgresql.org/docs/16/storage-toast.html),
[CASE evaluation](https://www.postgresql.org/docs/16/functions-conditional.html),
[varlena.c](https://raw.githubusercontent.com/postgres/postgres/REL_16_15/src/backend/utils/adt/varlena.c),
[detoast.c](https://raw.githubusercontent.com/postgres/postgres/REL_16_15/src/backend/access/common/detoast.c),
[toast_compression.c](https://raw.githubusercontent.com/postgres/postgres/REL_16_15/src/backend/access/common/toast_compression.c)
and [ruleutils.c](https://raw.githubusercontent.com/postgres/postgres/REL_16_15/src/backend/utils/adt/ruleutils.c).
Retrieved source digests are in `postgresql-source-provenance.json`.

Canonical migration/restoration actually measured 53 entries, maximum 558 raw
bytes, aggregate 13,929 / 13,953 bytes respectively, with zero compressed entries.
These controls establish support for the observed canonical representations;
they do not claim every possible TOAST layout was experimentally constructed.
The proof bounds admitted metadata and driver transfer, not all backend allocator
activity, catalog scanning cost or arbitrary extension behavior.

## Expected versus observed corrected boundaries

| Case | Required and observed result |
|---|---|
| Both A1 substitutions | `ErrUnsupported`, no consumer call, no completed result, logical source unchanged. |
| Missing, extra, wrong kind, wrong definition, NOT VALID, deferrable, NO INHERIT, 80 extra records | All refused; malformed set/status cases deparsed zero definitions. Wrong canonical definition refused after bounded comparison. |
| All three oversized A2 witnesses | One preflight query, at most 54 observed rows, zero production definition queries, largest returned value 41 bytes. |
| Valid canonical schema 18 | Accepted; 54 queries / 106 rows, largest returned value 41 bytes. |
| Migrated schema 17 then real adoption, and real dump/import restoration | Canonical schema 18 admitted; schema-17 route and required current readers retained. |
| Raw guard at 4,095 / 4,096 / 4,097 bytes | Actual PostgreSQL expression admitted / admitted / refused. Test-only PLAIN temp columns exercise this boundary without mutating system catalogs. |
| Lower caller row/catalog budgets | Existing `ErrLimit` before new metadata queries; limits not weakened. |
| A1 and A2 exports | Early refusal without completion marker; source state preserved. |
| Malformed restored import | Real export and restore occur first; a test-only restored-stage hook changes owned destination constraints before the real validator. No bypass of corrected exporter; no `IMPORT_READY`, pending ready receipt, publication or before-ready event. |
| Actual staged operator restore | Valid control restores known history. Malformed constraint profile then refuses before output or pin UPDATE; existing receipt and all logical source/destination state unchanged. A fixture-only nontransactional sequence/trigger detects even rolled-back pin attempts; valid control proves the detector works. |
| Cancellation at preflight and definition rows | Attributable `context.Canceled`, no consumer/completion; blocked physical rollback/close retain in-process and second-process ownership until cleanup finishes; no leaked sessions or source changes. |

The probe wrapper was adapted for pq's actual prepared-statement route; it was not
assumed that a connection-level query wrapper would see these values. Tests reuse
existing fixture, adoption, export/import, operator and cleanup helpers.

## Fresh qualification and explicit reuse

All fresh qualifying commands ran on the clean implementation checkpoint above,
with Go 1.26.10 (`GOTOOLCHAIN=local`, `GOWORK=off`, `CGO_ENABLED=1`). The actual
Go executable path and SHA-256 were recorded. PostgreSQL server: 16.15 Debian,
image `postgres:16.15-bookworm@sha256:bb3e1a57e5407e0a5280b4211980a5e537f4abd234a87014ac979849a78dd825`;
local dump/restore clients: 16.15 Ubuntu. Pinned lint: 2.9.0 built with Go 1.26.10.
Host/tool identities are in `environment.json`.

| Fresh gate | Outcome |
|---|---|
| Ordinary recovery/adoption/coordination/operator runner with race detector | PASS: 516 test/subtest events (391 recoveryexport, 98 coordination, 27 command), zero failures/skips. All 313 retained mandatory requirements plus 28 added events = 341 ran and passed. The additions include 5 group parents and 23 semantic leaves, not 28 independent scenarios. |
| Original-loss and post-live-GC recovery; current staged readers; supported 17/18 cases | PASS within the full runner; no substitution of the original 488-pass baseline. |
| Fixed current product oracle | PASS: all 23 expectations, interpreted as 12 restores, 9 refusals and 2 known limits. Known limits are not successful restores. |
| Full Python suite | PASS: 577 tests. |
| Focused authority / ordinary-runner mutations | PASS: 31 / 16 tests. Missing new A1 or A2 requirement, skipped requirement and nonzero Go child exit fail the real runner; retained controls remain. |
| Build, vet, pinned lint, tidy-diff, gofmt, whitespace, shell syntax/ShellCheck | PASS. |
| Validation matrix, versioned row writers, smart quotes, current authority/governance, explicit historical-v1 authority/governance | PASS. |
| Local CI enforcement audit and applicable CI/proof/TCP mutations | PASS; workflow unchanged. |

`commands.jsonl` binds each exact command, cwd, HEAD/tree, clean status, outcome,
duration and raw-log digest. `mandatory-events.json` enumerates all 341 run/pass
pairs. `qualification-summary.json` retains bounded measurements;
`validation-ledger.json` separates fresh qualification from expected failures and
development runs. Principal invocations include the ordinary
`bash scripts/run_recovery_export_tests.sh`,
`python3 scripts/run_product_fixtures.py --reader current --output <owned-output>`,
`python3 -m unittest discover -s scripts -p 'test_*.py' -v`, `go build ./...`,
`go vet ./...`, pinned `golangci-lint run ./...`, `go mod tidy -diff`,
`bash scripts/audit_ci_enforcement.sh --local-only` and explicit historical-v1
validators against the unchanged historical root. The command ledger carries
the concrete paths and full arguments, including runner evidence destinations.

Explicit reuse is limited to unchanged boundaries. `reuse-ledger.json` binds
503 relevant production/test/runner/fixture/dependency files, original command
records and log digests; 92 original nonsecret text artifacts are retained.
The original tested checkpoint was `6f257f6fd0391412ec661dabea52013168f7e6eb`;
its submitted-final delta was documentation only. Current `go list` evidence
shows unchanged production import/dependency graphs for 59 packages and no
recoveryexport dependency in the 75 local dependency/test records examined for
reused packages. No shared API, constructor or lifetime boundary changed.

Reused outcomes: Phase 2 native/browser/Docker proof (85 cases / 15 critical
outputs; PostgreSQL 38 / 3; 9 TCP controls; packaging and ownership), database
146 passes, selected storage/snapshot/container/verification lower layers 312
passes with 3 disclosed skips, restore/pinning/history 35 passes, and historical
D19's 23 fixed expectations with original source/artifact provenance. These were
not rerun or represented as fresh follow-up execution. The corrected recovery
profile is qualified only by the fresh full run.

The broad engine package remains **FAILED / UNQUALIFIED**. Preserved original
evidence has 805 passed events, 8 failed events and 1 helper skip. The two inherited
failed families are `TestEngineBatchCancellationStopsNewDispatchWithPartialResult`
and `TestEngineDependencyDirection`; the accepted pre-3A counterexample remains
bound to the same diagnostics. Neither was fixed, deleted, skipped or relabeled
green. Their unchanged dependencies permit reuse of that inherited classification.

Four existing nonqualifying skips remain explicit:
`TestRestoreNativeDarwinTempPathWithoutEvalSymlinks`,
`TestStoreInterleavingRetryClaimSignalsCASBoundary`,
`TestStoreFolderWorkersFourWithConcurrentGCPlanNoCorruption` and
`TestPhase3R13ReadQueryRestoreCWDChild`. They add no required-case skip here.

Initial development failures are retained: the boundary fixture first combined
parameterized TRUNCATE/INSERT in one prepared command, rejected by pq; splitting
the test statements corrected it. Lint found an unused initial test variable,
which was removed. Both corrections preceded the clean checkpoint. The evidence
summarizer was also adjusted to recognize the unittest OK summary before buffered
fixture stdout; the 577-test command itself passed without rerunning or changing
its output. No unresolved fresh qualifying failure is hidden.

## Cleanup, delivery and remaining work

The separately labelled owned PostgreSQL container was removed only after exact
ID/image/label verification and an inventory showing only postgres/template0/
template1 databases, zero test roles and zero other database sessions. The probe's
1,751 tracked files were reverified, its exact overlay preserved, then only that
owned worktree was removed. Generated credentials, private tooling and 84 owned
product working-fixture files were removed. Nonsecret raw evidence remains.
All public evidence existing at cleanup was scanned for the generated database
credential without recording its contents or digest. Detailed dispositions are
in `cleanup.json` and preservation inventories.

Standalone evidence directory:
`/home/franchoy/.local/state/coldkeep-release-evidence/v2.0/phase3a-followup-20261109`.
Delivery includes this report, final identity and source hashes, exact final
documentation delta, incremental `review.patch`, ordered commit patches,
independent temporary-index replay records including tree/file-mode equality,
and `coldkeep-phase3a-followup-evidence.zip` with complete internal `SHA256SUMS`
and external `.zip.sha256`. Archive verification checks coverage, safe members,
CRC and equality with standalone report/identity/patches. These are local host
paths, not an assertion that artifacts have been attached to ChatGPT.

No schema, persisted payload/package/receipt format, dependency, fixture oracle,
protected workflow interval or ordinary schema-17/SQLite behavior changed.
Compressed constraint metadata is deliberately unsupported. Trusted SQL-dump
semantics are unchanged; this is not an arbitrary hostile-SQL sandbox, power-loss
proof, platform expansion or hosted CI qualification. Physical binding remains
unqualified; ordinary schema-18 writers still refuse. After independent acceptance,
Phase 3B owns physical markers, expected-volume evidence and rebind/relocation;
Phase 3C still owns typed membership, exact names, policy provenance and indexed
lookup. Full 2.04 needs later integrated acceptance. No push, merge, tag, release,
publication, live installation switch or Phase 3B/3C execution occurred.
