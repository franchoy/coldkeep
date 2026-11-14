# Phase 1D integrated result — 8 November 2026

Phase 1D: COMPLETE
Local-profile action 2.03: COMPLETE
Profile: Linux/manual PostgreSQL 16
Product gates: INCOMPLETE
Implementation: ACTIVE

The conditional authority in `coldkeep_phase1d_codex_2026-11-08.md` is exercised
after D1–D12 passed on a clean implementation checkpoint. Current phase stays
2.03; 2.04 has not begun. This result qualifies constrained import and functional
recovery in the declared local profile. It grants no publication, real-repository
adoption, native distribution or complete-product claim.

## Identity and evidence navigation

- Accepted base HEAD: `d96edea6abb1c2d55152e2020286a08ad959368e`.
- Accepted base tree: `8c30e47284668c86dae8aad55945458111eaed06`.
- Implementation checkpoint HEAD: `b0b3ce832ff559c67ccf99f37ce434ef17dc61dd`.
- Implementation checkpoint tree: `294476de1ffc1348a595418ee92a5f87e1d9c765`.
- Checkpoint subject: `feat(recovery): import trusted generations into fresh owned targets and prove isolated recovery`.
- Branch: `release/v2.0-phase1d`; worktree:
  `/home/franchoy/vscode/coldkeep-worktrees/v2.0-phase1d`.
- Final HEAD/tree and ordered commits are recorded in the linked external
  `final-identity.json`. Embedding this report's containing commit identity in
  itself would create a self-reference. That identity binds the exact report
  hash, final clean diagnostics, incremental patch and replay result.
- The final commit changes result/authority/provider prose only. Runtime code,
  tests, runner, CI and authority implementation retain checkpoint bytes.
- Version/module remain `2.0.0-dev.0` / `github.com/franchoy/coldkeep/v2`;
  latest stable remains `1.13.19`. No schema, dependency or image pin moved.

Evidence root: `/home/franchoy/.local/state/coldkeep-release-evidence/v2.0/phase1d-20261108`.
Relative evidence paths below refer to this root and the ZIP root. Main links:
[identity](/home/franchoy/.local/state/coldkeep-release-evidence/v2.0/phase1d-20261108/final-identity.json),
[commands with exit codes and source binding](/home/franchoy/.local/state/coldkeep-release-evidence/v2.0/phase1d-20261108/commands.jsonl),
[verified results](/home/franchoy/.local/state/coldkeep-release-evidence/v2.0/phase1d-20261108/verified-results.json),
[incremental patch](/home/franchoy/.local/state/coldkeep-release-evidence/v2.0/phase1d-20261108/review.patch),
[compact evidence ZIP](/home/franchoy/.local/state/coldkeep-release-evidence/v2.0/phase1d-20261108/coldkeep-phase1d-evidence.zip).
`SHA256SUMS` covers every regular bundled member except itself. Standalone report,
identity and patch equal their bundled copies; both incremental and ordered
patch replay must reproduce the final tree. See `artifact-verification.json`.

Requested settings: GPT-6 Astra (`gpt-6-astra`), Extra High (`xhigh`). Actual
model and effort are not observable; both are recorded as null. No global
settings changed and no subagents were used (`settings.json`).

The outer launch manifest covered 26 files and the accepted archive manifest
105 files, with complete coverage. Accepted archive SHA-256:
`5ac1ad2126653ceeb6f340251eb6e8acac37502bd8001b2394177a3f7629df70`;
accepted patch SHA-256:
`7f7c16b34c02018c0eef4aabe5f59eabae6e1a1258b690c81dff5b66430f1e63`.
Standalone/nested identity, report and patch agree; current contract snapshots
match the accepted checkout. Historical archive scripts were not executed and
its patch was not reapplied. See `launch-verification.json` and `input/`.

## Implemented contract

`recoveryexport.Import` is callable production code with a distinct writable
`Destination`, explicit generation/manifest SHA-256 consent, trusted operator
backup flag, exclusive target assertion, finite limits, selected tools and
separately resolved key material. `cmd/coldkeep-recovery` supplies narrow
check/import/restore invocations. The [operator guide](recovery-package.md)
contains provisioning, configuration, invocation, custody and cleanup details.
No source configuration, source database credentials or original root is an
import/recovery input.

The operator provisions a pristine template0 PostgreSQL 16 database, matching
encoding/libc locale/collation/ctype/default tablespace. The ordinary owner lacks
superuser, replication, bypass-RLS, role/database creation and all memberships
(a conservative restriction also excluding indirect elevated membership).
Unexpected namespaces/objects/settings, subscriptions and populated/system/
template targets refuse. The root must be previously absent below a canonical
parent, separate from the package. A dedicated database connection holds a
cooperative database-scoped advisory reservation; the importer-created target
holds the existing canonical-root lease without renaming. Freshness is rechecked
under ownership. External administrators remain outside cooperative fencing.

The accepted completed-package checker runs before SQL/target mutation. Consumed
bytes are copied through anchored regular single-link handles, hashed, rechecked
and stored privately; payloads are independent copies at validated SourceNames.
Traversal, collisions and the reserved control namespace refuse. The private
archive is rehashed and expanded SQL streamed to discard with a 512 MiB ceiling.
Existing limits remain: 1 GiB object, 4 GiB objects, 64 MiB archive, 8 MiB manifest,
20,000 inventory rows/entries, 16 MiB catalog, 8 MiB decoded block and two-minute
operation duration. See the guide for the separate parser/member/total ceilings.
Copy bytes plus a 16 MiB reserve are capacity-checked. An explicit database
capacity budget of at least 64 MiB is required; compressed size cannot predict
heap/index/temp/WAL needs and this is not a general SQL resource sandbox.

One `pg_restore --no-owner --no-acl --exit-on-error --single-transaction
--no-password` child restores full metadata using explicit host/port/user/database
and a minimal writable environment. Archive trust is required because backup SQL
can execute code; checksums and the ordinary role do not sanitize it. Ambient
PG/libpq settings, including resolver-introduced changes, refuse. Credentials are
private external files with joined cleanup, never argv or evidence inputs.
The passive opener retains its unchanged read-only child settings.

The private owned-target inventory seam reuses full schema/graph/payload admission
under the retained lease and established rollback/driver cleanup joins. Admission
compares exact object IDs, safe names, lengths, digests, encoded formats, counts,
catalog version, selection, reader and key requirements. Full historical, pin,
repair and retired closure survives. Original root observations remain provenance;
the target has its own observed root hash/OID/role, with no invented stable IDs.
The advertised actual reader supports separately held 32-byte AES-256 keys;
missing, wrong and unsupported-length combinations refuse before readiness.

There is no cross-database/filesystem atomicity claim. The private control area
contains a durable logical `IMPORT_READY` receipt only after copying, metadata
commit, semantic/key admission, child/active-session/credential cleanup and
required syncs. Designated ownership handles remain through publication and are
released before success. Pre-publication failures leave incomplete targets with
no usable handoff. Publication/durability/release uncertainty returns
`published-or-uncertain`, Ready false, preserving the target for reconciliation.
Fresh named targets are required for retry; there is no drop, overwrite, resume,
repair, schema initialization or migration on behalf of the importer.

## D1–D12 expected versus observed

All named runtime rows below have actual run and pass events in
[the race log](/home/franchoy/.local/state/coldkeep-release-evidence/v2.0/phase1d-20261108/acceptance/go.jsonl); [event verification](/home/franchoy/.local/state/coldkeep-release-evidence/v2.0/phase1d-20261108/acceptance/acceptance-events.json)
reports 380 passes, zero failures/skips/missing events. The runner retains all
135 accepted requirements and adds 71 (206 required named events total).

| ID | Expected and observed | Named evidence |
| --- | --- | --- |
| D1 | Invalid/missing/incomplete/corrupt/unsupported or untrusted packages refuse before destination SQL/mutation; observed PASS. | `TestRecoveryImportTrustAndPackage` and its 8 required cases |
| D2 | Fresh ordinary target succeeds; populated/system/privileged/membership/locale/alias targets refuse; both target dimensions contend deterministically; PASS. | `TestRecoveryImportTargetRefusals`, `TestRecoveryImportContention`, `TestRecoveryImportFreshnessUnderReservation` |
| D3 | Full 19-table metadata plus sequence/index/constraint/function/trigger definitions round-trip; nextval and FK behavior exercised under ordinary owner; PASS. | `TestRecoveryImportRoundTrip` |
| D4 | Recomputed valid checksums around inconsistent names/IDs/formats/counts/reader/key/header or missing/extra objects refuse; PASS. | `TestRecoveryImportSemanticAdmission` and its 11 cases |
| D5 | Unsafe names, handle substitutions, links, limits, short writes/ENOSPC/close/fsync/archive drift/cancellation preserve input and prevent premature readiness; PASS. | `TestRecoveryImportCopyAndBounds`, semantic/name cases, retained checker/parser cases; finite DB exhaustion in plaintext recovery |
| D6 | Blocked transaction/actual driver/child/credential cleanup retains ownership; late failures distinguish uncertainty; success leaves zero sessions/processes; PASS. | `TestRecoveryImportCleanupJoins`, `TestRecoveryImportCredentialJoin`, `TestRecoveryImportChildJoin`, `TestRecoveryImportFailures`; isolated receipts and cleanup receipt |
| D7 | Plain old history restored without original installation access; 3 final files match independently expected hashes/mode/mtime; PASS. | `TestRecoveryOriginalLoss/plaintext=true`; `acceptance/original-loss-plain-true.json` |
| D8 | Mixed/encrypted old history restored using independently staged key/reader; 8 files match; missing/wrong/unsupported combinations refuse; PASS. | `TestRecoveryOriginalLoss/plaintext=false`, target refusal cases; `acceptance/original-loss-plain-false.json` |
| D9 | Whole objects needed by snapshot-only, pin-only, repair and retired mappings remain in full restored closure; no fabricated file meaning for pin-only chunks; PASS. | `TestRecoveryImportFullHistoricalClosure`, retained `TestRecoveryHistoricalClosure` |
| D10 | Production non-dry GC removes historical recipes and reclaims required source objects; same unchanged generation freshly imports into another target and restores 8 files; PASS. | `TestRecoveryPostLiveGC`; `acceptance/live-gc.json`, `acceptance/post-live-gc.json` |
| D11 | Child exit, missing event and skip reject; blocking CI step and existing requirements preserved; fixed 23-case oracle matches; PASS. | 4 `test_recovery_runner` tests; 6 `TestAuditCIEnforcementRequiresRecoveryCopy` mutations; current product logs |
| D12 | ACTIVE/COMPLETE support remains local structural authority; missing result, unknown phase and complete-product claims refuse; only satisfied 2.03 obligation removed; PASS. | `test_v2_authority`, fresh full Python suite and final authority/governance diagnostics |

## Actual restoration and custody

The fixed external oracle was recorded before export and never mounted as reader
input. Selected snapshot `independent-history` has zero current-file rows. Its
old `history/Case.txt` content differs from the later current version; normal
RemoveStoredPaths removes current roots. Packed/plain/zstd/AES and legacy paths
are represented. Plain recovery selects IDs 1, 2, 100; encrypted/mixed and post-GC
recovery select all eight IDs below. Each restored file was reread from final
output and SHA-256 checked independently of the reader's success report.

| ID | Path | Expected SHA-256 = observed SHA-256 | Plain | Encrypted/mixed | Post-GC |
| --- | --- | --- | --- | --- | --- |
| 1 | `history/Empty` | `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855` | PASS | PASS | PASS |
| 2 | `history/Case.txt` | `8950abfda7b727630760dd35bcf5c3daa7631aff223a90f7728c0d2521dde10c` | PASS | PASS | PASS |
| 3 | `history/case.txt` | `6b1d4bfe25f0144686cade6a6f77f222e6ef929b949a9d688e418d56dd3af609` | — | PASS | PASS |
| 4 | `history/é.txt` | `4d18e266c17e7efc1617632a2dca08f75923711c9ef814cea1265df797286f09` | — | PASS | PASS |
| 5 | `history/file-5.bin` | `3c7762a35e19754d73dfe53ef5d929c51974c32368bd59a6d725fa5da5777be3` | — | PASS | PASS |
| 6 | `history/file-6.bin` | `9617b1c61f17f60cc0f63c915fca166a8335af0c173e1efe25211cc351e656a9` | — | PASS | PASS |
| 7 | `history/file-7.bin` | `a9abaef479fd8a58bda0527e7f927d37233017152ed9db1760817800c3c41236` | — | PASS | PASS |
| 100 | `history/legacy.bin` | `0d599ec22b3b271ce0f24bc3d293fcb20a7776fff1119fa8ec4908c9e437bfb4` | PASS | PASS | PASS |

All 19 final file observations have mode `0640`, mtime `2025-01-02T03:04:05Z`;
content and metadata checks are separate. Empty content, case-distinct names and
a supported UTF-8 name survive on this Linux filesystem. Full selection IDs,
paths, sizes, mode/mtime, target roots and exact receipts are in the three JSON
files. Pins are zero before and after each reader; remaining owned sessions zero.

The source DB is unreachable from the recovery network namespace; original live
payload, checkout/configuration and original key paths are absent. Negative
catalog and filesystem access probes run before import/restore. The separate
PostgreSQL destination container supplies the network namespace. A fresh reader
process sees only read-only package, independently staged custody, separately
staged executable and fresh destination mounts, with an explicit empty/minimal
environment. Source credentials are never passed. The encrypted key is staged
before export and parent fixture key memory cleared before recovery. Key bytes
are absent from manifests/logs/argv/evidence. The staged static reader SHA-256 is
`468a180ac7357759733fc12bb7d0e515ecd9479cee47368b20d21bd9f16be76d`.

Actual reconstruction uses explicit imported DB/root via StorageContext, existing
snapshot selection and existing storage restore. The adapter has no writer or
ordinary schema/bootstrap constructor. The unchanged snapshot wrapper checks
cancellation between files; the rehearsal bounds the entire owned reader child
with `timeout --kill-after=5s 120s`. It does not claim per-file context propagation.
A hard kill can leave transient pins and requires reconciliation, as documented.

A second fixture exported generation `d4541c54a8ae7d09bfb8163ca23448d2`
(manifest `4799c3f21c67a28cd970ee5a05373c0892673b193c16461dbcf2f867965c1671`).
Normal snapshot deletion and `GarbageCollect(DryRun:false)` left zero recipes and
reclaimed both exported source objects (packed container and `legacy.bin`),
**465,863 bytes**. Package fingerprints were unchanged. That same generation was
freshly imported into a new isolated database/root and all eight selected old
files restored; no preexisting imported destination or cached source content was
used. Original-loss generations and their manifest hashes are separately recorded
in `verified-results.json`.

Byte completeness, semantic readiness, demonstrated restoration, key/reader
custody and failure-domain independence remain separate facts. Physical
independence is **unknown**: same-host containers demonstrate functional isolation,
not host/disk-loss survival. Package custody fields were not rewritten.

## Qualification and environment

All qualifying runtime commands ran on the clean checkpoint above. Exact argv,
HEAD/tree, start/end time, exit code and raw-log digest are in `commands.jsonl`.
Every qualifying command exited zero. Final documentation/authority diagnostics
are freshly bound to the final clean commit, separately from runtime qualification.

| Check | Result | Evidence |
| --- | --- | --- |
| Recovery/export/import/coordination, race, count=1 | 380 passed; 206 required events; no skip/fail/missing | `acceptance/go.jsonl`, `logs/qualifying-recovery.log` |
| Affected storage/snapshot/engine/maintenance, race | 35 passed; no skip/fail | `logs/qualifying-affected.log` |
| Current-reader fixed oracle, race | 23 matched: 12 RESTORED, 9 REFUSED, 2 KNOWN_LIMIT | `product-current/execution.json`, `fixture-results.json`, `go-test.jsonl` |
| Fresh full Python suite | 547 passed in 282.373 seconds | `logs/qualifying-python.log` |
| Focused authority / runner enforcement | 25 authority and 4 runner tests passed; also in full suite | `logs/focused-authority.log`, `logs/runner-mutations.log`; final focused logs |
| CI enforcement mutations | all 6 passed (including absent recovery image); local audit passed | `logs/qualifying-ci-mutations.log`, `qualifying-ci-audit.log` |
| Build / vet / pinned lint / tidy diff | passed; no dependency drift | `logs/qualifying-build.log`, `qualifying-vet.log`, `qualifying-lint.log`, `qualifying-tidy.log` |
| Formatting / shell / guards | 603 Go files formatted, 25 shell files parse, changed runner shellcheck, matrix/row-writer/smart-quote/whitespace guards pass | `logs/qualifying-format.log` and corresponding qualifying logs |
| Current / explicit historical authority and governance | passed; historical route remains v1.13.19 post-release-closed | final diagnostics and `logs/historical-authority.log`, `historical-governance.log` |

Observed Linux amd64, Go 1.26.10, GOTOOLCHAIN=local, GOWORK=off, CGO=1 for
race tests (staged reader CGO=0); golangci-lint 2.9.0 built with Go 1.26.10.
Host PostgreSQL clients `/usr/lib/postgresql/16/bin/pg_dump` and `pg_restore` are
16.15; the isolated recovery image clients and disposable servers are 16.13
Debian. Tool hashes/versions are recorded in `environment.json` and
`logs/final-recovery-tool-identity.log`. Major versions match;
patch versions are deliberately reported separately. Test image:
`postgres@sha256:2586e2a95d1c9b31cb2967feb562948f7d364854453d703039b6efa45fe48417`.
CI's existing pinned PostgreSQL 16 image remains unchanged and is now explicitly
passed to the recovery reader stage; this is local evidence, not hosted CI.

Destination PostgreSQL has a finite 768 MiB tmpfs, 1 GiB memory limit; reader
container 512 MiB memory and 64 MiB private tmpfs. The plaintext scenario actually
fills PostgreSQL storage to ENOSPC and observes connection refusal, with no ready
handoff, then retries using a fresh database/root after owned filler removal.
This is an observed exhausted-storage connection failure, not a claimed
pg_restore WAL/heap exhaustion experiment. Import write/sync/close error tests
cover their precise injected failure boundaries. Expanded SQL, stderr/stdout and
child lifetimes are separately bounded. No generic hard SQL memory/WAL sandbox
or crash/power-loss certification is claimed.

## Development failures, reuse and retained limits

No qualifying run failed. Earlier development/setup failures are retained in
raw logs and command records:

- Initial metadata assertion expected a populated Inventory catalog map; corrected
  to the actually validated full table shape and semantic metadata checks.
- Isolated fixture startup initially observed the bootstrap socket before the
  final TCP server; readiness now waits on TCP. Container default PG variables
  correctly caused ambient-environment refusal; reader now uses env-i. Mixed
  stderr/JSON parsing was fixed by separating child output streams.
- A hardlink test initially linked the original package member and altered its
  link count; the test now uses an independent copied fixture. Historical fixture
  setup initially omitted matching live_ref_count updates; passive admission
  refused, and the fixture setup was corrected without weakening validation.
- Lint found a redundant type annotation; it was removed before checkpoint.
- Initial server readiness returned early once. Launch verification initially
  looked for the wrong nested manifest/report paths, then verified the actual
  SHA256SUMS and source report. No source edits preceded completed verification.

Retained evidence in `reused-evidence.json` is explicitly historical, not new
execution: unchanged-source broad regressions (1,277 passes, 11 skips) from
`2e31dde54bccc31189ffc96561feacd31d27904f`, and 12 compression passes from
`1f46389e99f8f41a8886c35c853635ed66af694f`, whose six compression cases cover six
historical skips. Five historical skips remain unqualified here: native Darwin
path behavior, CAS interleaving, concurrent workers/GC-plan store case, and the
two unsupported-storage-codec verify cases. Exact names/logs/source trees are
retained in `historical-skip-coverage.json` and `reused-evidence.json`.
The affected packages/dependency pins were byte-identical to those original
sources. Fresh affected restore/history tests and integration tests are separate.
Accepted D19 raw execution/results retain the exact fixed oracle; the previous
542-test Python result is not reused. The 547-test run is fresh.

The two fixed product KNOWN_LIMIT outcomes remain legacy backslash-to-separator
normalization (not lossless Unix capture), and content restoration despite future
catalog block-version metadata when the encoded CKBL remains v1 (verification
required), as specified by the unchanged oracle. This unit does not qualify native platforms, SQLite passive recovery,
empty-directory completeness beyond retained contracts, arbitrary SQL uploads,
stable key/repository identity, generic resume/repair, external-admin fencing,
physical host/disk independence, or complete-product/release gates.

## Preservation, cleanup and handoff

[Preservation](/home/franchoy/.local/state/coldkeep-release-evidence/v2.0/phase1d-20261108/preservation-final.json) verifies the 849 frozen files:
`64fb69cb875c4bdf96c10cd759275b7232849c2f8083b0b406f11d7b18b23c7a`, protected
fixtures/oracles/schema/dependencies/accepted cleanup and visibility/layout tests,
and all previous result reports. All 21 existing predecessor worktrees, nine
already-absent registrations and preexisting branches retain their status and
identity. No worktree pruning/reset, push, PR, merge, tag, release or deployment
occurred. The accepted checkout remains at its exact original commit/tree.

Only recorded disposable resources were cleaned. Before removing the owned test
server, it contained only postgres/template0/template1 databases and its setup
admin role; other client sessions, subscriptions, replication slots and prepared
transactions were all zero. Isolated recovery containers, reader children,
runner binaries and private child credentials were already gone. The recorded
server ID/label was verified before removal; all 11 unrelated container IDs
were preserved. Private orchestration credentials are removed after final
non-DB diagnostics; exact cleanup is in `cleanup-fixture.json` and
`credential-cleanup.json`. Disposable copied product working fixtures are excluded
and cleaned with an exact recorded-path receipt; fixed source fixtures remain.

`secret-scan.json` records scans against actual disposable credentials and secret
material, with no secrets retained. Evidence excludes private files, binaries,
working payload/catalog copies and nested historical archives. Raw selected logs,
input provenance, preserved reused evidence and command records remain. No raw
key/password value is redacted into a misleading success claim.

The [handoff](next-slice.md) leaves 2.02, 2.04–2.13, 2.15, 2.17 and separate
publication authorization outstanding. Review this result and its artifacts;
there is no mandatory closure-only build and no authority to begin 2.04.
