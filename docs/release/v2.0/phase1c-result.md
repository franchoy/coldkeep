# Phase 1C recovery-copy result — 8 November 2026

**Phase 1C unit COMPLETE; parent 2.03 ACTIVE; product gates INCOMPLETE.**
This is a local copy/completion candidate. Application identity remains
**2.0.0-dev.0**. Constrained import, original-installation-loss restoration and
post-live-GC survival remain the next integrated recovery unit. No product
qualification, real-repository adoption or publication is claimed.

## Entry, source and settings

The launch ZIP SHA-256 is
`cc697499e81ff2ea313eb6a55f6d8b7aa49c75a7ec7a17d6476375db2c22b895`.
All 17 launch checksum entries and ZIP integrity passed; the accepted evidence
ZIP retained SHA-256
`a1edc5f47b01007eb5996acd6b7eb34a650ef597931f23c1fd8774fd8b554147`
and all 86 internal checksum entries passed. Archived helpers were not executed
and the accepted patch was not reapplied. The user-designated README and Phase
1C BUILD instruction supplied operative scope; accepted evidence/planning
materials supplied provenance and contracts, not separate action authorization.

Separate worktree `/home/franchoy/vscode/coldkeep-worktrees/v2.0-phase1c`, branch
`release/v2.0-phase1c`, started clean at accepted HEAD
`a6d12760359e5e75d1021863581f3cbc6ddf5f75`, tree
`5b5db15ceffa867ea743397595794ed99f0cd90b`.

All final runtime/gate executions bind the clean implementation checkpoint
`ece9602f0b2ee5b5dbd579c87557c082c9ffcc8a`, tree
`edde9e86c840874d9342ea80aa211c97d22c20ae`.
The later result commit changes this report only. External `final-identity.json`
records that final commit/tree, clean status, ordered local commits and patch
replay; this committed report deliberately contains no self-referential identity.

Requested settings: **GPT-6 Astra / Extra High (`gpt-6-astra`, `xhigh`)**, BUILD.
Actual model/reasoning settings are **unknown / unobservable** through this
session. No global/account settings changed and no model choice is inferred from
the request. No subagents were used.

## Implemented behavior

- `coordination.PrepareExisting` is explicitly mutating, separate from passive
  Inspect/Export. It requires an existing root and the established process/native
  lock domain, preserves lock inode/bytes and diagnostic owner metadata, refuses
  unsafe artifacts and excludes competing participating owners. Operators must
  first stop legacy writers/maintenance; it is not lost-live-lock repair.
- A private owned-cut callback shares accepted B1–B3 lifecycle/schema/visibility
  guarantees. Export acquires a fresh read-only repeatable-read full-catalog cut,
  exports its PostgreSQL snapshot and dumps all metadata/data with a matched
  PostgreSQL 16 custom-format client. No inventory JSON authorizes later work.
- Explicit child arguments/environment prevent ambient PG target redirection and
  scalar DSN interpretation. Private passfiles remain outside source/package
  trees and are removed after joined children. Archive/stderr/inspection output
  are bounded. The qualified profile refuses unsupported database prerequisites.
- Independent regular files copy whole objects, retaining packed/compressed,
  encrypted/plaintext, legacy, retired, journal, pinned and catalog-orphan closure.
  Anchored fresh directories, disjoint canonical paths, size/capacity/time limits,
  exact hashes, close/sync propagation and destination rereads precede finalization.
- The versioned manifest distinguishes application, catalog, payload and package
  versions. It declares keys/readers but includes no raw secrets. Custody and
  destination independence are unknown. The bounded checker needs no live source,
  key or SQL execution and refuses incomplete/corrupt/unsafe/unsupported material.
- Source readers, child/pipe workers, automatic or explicit rollback, physical
  driver close and socket cleanup finish before lease release. Only afterward can
  atomic Linux no-replace marker rename publish completion. Late acknowledgement
  or sync errors retain `Published=true` and require reconciliation with Check.
- The blocking CI companion runner requires explicit scenario events and rejects
  failures/skips/missing events. Existing passive and product gates remain. The
  passive runner excludes the separately opted-in Phase 1C test family. Protected
  workflow wrapper intervals and aggregate failure propagation are preserved.

The [versioned package/operator contract](recovery-package.md) gives API inputs,
preparation, restart/cleanup policy, exact limits, trust/locale/role requirements
and publication semantics. `lifecycle-review.md` in external evidence records the
new pinned Go/os-exec inspection and unchanged accepted B1 session source hashes.
No dependency, schema SQL, migration or payload-format change occurred.

## New acceptance scenarios

All rows below are **NEW executions** at the clean implementation checkpoint.
Raw commands/source receipts and test events are external, with exact scenario
selectors in `scenario-results.json`. Test events include parent/subtest events;
they are not 277 independent product scenarios.

| Scenario | Expected and observed |
| --- | --- |
| Plaintext/encrypted export | PASS: complete generation, exact hashes/lengths and independent file inodes; packed and retained legacy payloads; three members including archive; key required only for encrypted case. Plain/encrypted metadata had 21/174 rows across all 19 tables. |
| Historical/repair/retired/pin closure | PASS: five whole objects plus archive; snapshot-only zero-current recipe, pin-only chunk, all repair tables, retired packed/legacy extents and catalog orphan survive metadata round trip. |
| Same snapshot | PASS: external ordinary-table commit after snapshot is absent from restored archive. No sequence/DDL mutation; deliberate external fixture change is separately accounted. |
| Metadata round trip | PASS: fresh database loaded by a non-superuser using `--no-owner --no-acl --exit-on-error`; every supported table, sequence definition/value, index, constraint, function and trigger compares; next ID and FK behavior pass. No bootstrap/migration/sequence repair. |
| Preparation and preservation | PASS: missing root/unprepared/unsafe artifacts refuse; explicit/repeated preparation preserves existing artifacts and permits later passive export. Subprocess legacy/passive contention passes. Source catalog/payload/control fingerprints and transient mutation watch pass. |
| Dump/copy ownership and child lifetime | PASS: held dump/copy retains legacy/passive exclusion; actual live child cancellation joins/reaps it and retains lease through the join barrier; no surviving owned PostgreSQL sessions. |
| Late cleanup | PASS: deterministic barriers before actual driver rollback and close retain lease/no marker; driver-close and automatic rollback errors prevent completion. Retained B1–B3 regressions also pass. |
| Interruptions/publication | PASS: stage failure/cancellation after archive, during copy and before publication leaves no accepted completion. Post-publication acknowledgement/sync failure truthfully preserves a valid generation. |
| Data/filesystem failures | PASS: missing/changed source, destination digest mismatch, truncated archive, ENOSPC, short write, close and sync errors refuse; original error classes propagate. |
| Destinations/parser bounds | PASS: source containment/aliases, existing/relative destinations, small archive/manifest/total/count/time limits and integer overflow refuse without changing unrelated material. |
| Tools/configuration | PASS: missing/wrong/mismatched tools, nonzero/empty/oversized child output, expired snapshot, missing privileges, RLS, unsupported schema and hostile database/user values refuse. Controlled 15/17 server-version responses exercise the production guard over a real PG16 connection; no PG15/17 server qualification claimed. Ambient PG injections do not redirect children. |
| Independent checker/corruption | PASS after original fixture database/root removal; missing/altered marker/member/manifest, wrong length/digest, generation mismatch, future format, duplicate keys/members, traversal/absolute names, symlink/FIFO/hardlink/extra/nested/deep/oversized material refuse. |

## Validation and evidence disposition

Local Linux x86-64 used installed Go **1.26.10**, `GOTOOLCHAIN=local`, `GOWORK=off`,
CGO=1, pq **v1.11.2**, golangci-lint **2.9.0**. Actual clients were
`/usr/lib/postgresql/16/bin/pg_dump` and `pg_restore`, both **16.15**.
Disposable PostgreSQL **16.13** listened only at `127.0.0.1:55485` with tmpfs data,
image `postgres@sha256:2586e2a95d1c9b31cb2967feb562948f7d364854453d703039b6efa45fe48417`.
Tool/server/image observations and source hashes are in `environment.json`.

| Gate | Outcome / provenance |
| --- | --- |
| Recovery/coordination acceptance | NEW PASS: `-race -count=1`; 277 passed events, 179 recoveryexport and 98 coordination, two package passes; zero failures/skips/missing events. |
| Retained passive runner | NEW PASS: 185 events; run without Phase 1C opt-in; zero failures/skips/missing events. These overlap the acceptance suite, not additional independent scenarios. |
| Current product oracle | NEW PASS: unchanged 23 cases; 12 RESTORED, nine REFUSED, two KNOWN_LIMIT under race detection. |
| Full Python suite | NEW PASS: 542 tests. No reuse claimed because authority implementation changes. |
| Authority/governance | NEW PASS, including 24 focused explicit-phase/refusal tests; structural label is `LOCAL_ACTION_2_03_AUTHORITY_ONLY`. Parent completion, unknown phase, contradictory mirrors and publication claims still refuse. |
| Local CI audit/mutations | NEW PASS: recovery/product gate masking/removal mutations and protected CK-015 wrapper identity/boundary checks. Three separately labeled synthetic runner probes retain Go exit 23 and reject missing/skip events. Hosted CI was not run. |
| Build/vet/lint/tidy | NEW PASS; zero lint issues, no module drift. |
| Formatting/guards | NEW PASS: every tracked Go file formatted; every tracked shell file parses; changed-shell ShellCheck, smart quotes, validation matrix, row-writer and diff whitespace pass. |
| Unaffected runtime regressions | REUSED: 1,277 passes at `2e31dde54bccc31189ffc96561feacd31d27904f`, with 11 recorded skips. Six compression-gated cases later produced 12 passing events at `1f46389e99f8f41a8886c35c853635ed66af694f`; five other skips remain skips. Catalog/container/storage/verify/recovery/db implementations and dependencies remain unchanged. |
| Exact D19 | REUSED accepted Phase 1A execution and unchanged 23-case oracle. No new historical-reader run claimed. |

Initial implementation-stage failures are retained, not relabeled as passes:
wrong version API name was corrected to `version.String()`; a manifest initially
counted only nonempty catalog maps (13) instead of all 19 tables and refused
staged verification; the combined history fixture initially retained an invalid
live-reference count and correctly refused. Three static-lint suggestions were
fixed, the new audit's condition regex escaping was corrected, and the injected
server-version test was adapted to the pinned prepared-statement driver seam.
Corrected affected checks and the complete final suite passed at the clean
checkpoint. No historical oracle or expected product outcome was weakened.

## Preservation, cleanup and review artifacts

NEW preservation proof compares all 1,651 unchanged tracked paths, accepted
Phase 0/1A/1B/follow-up records, fixed fixtures/product contract, schema/format and
module pins. All **849 frozen v1 files** retain digest
`64fb69cb875c4bdf96c10cd759275b7232849c2f8083b0b406f11d7b18b23c7a`.
The main, Phase 0, Phase 1A, submitted Phase 1B and accepted follow-up worktrees
retain their exact entry HEAD/tree/clean status. `current.json` is byte-unchanged.

Cleanup verified zero fixture databases, reader roles and owned sessions before
removing this run's labeled tmpfs container. All private passfiles were gone;
84 owned working-fixture files (19,361,351 bytes) were removed. Raw receipts remain.
No user catalog/repository/key or unrelated resource was used or removed.

External evidence root:
`/home/franchoy/.local/state/coldkeep-release-evidence/v2.0/phase1c-20261108`.
It contains final identity, incremental `review.patch`, ordered commit patches,
independent-index replay proof, raw acceptance/product events, scenario/package
receipts, commands/source identities, initial failure logs, explicit reuse,
preservation and cleanup. The compact ZIP contains checksums and excludes
passwords, key bytes, disposable catalogs/payloads and archived launch helpers.
Any redacted evidence copy is explicitly identified with original/copy hashes;
raw external originals remain preserved. No push/merge/tag/release/deployment.

Remaining limitations are explicit in the [operator contract](recovery-package.md)
and [next handoff](next-slice.md): local Linux/PostgreSQL16 only, hard bounds,
trusted operator SQL, unknown independent custody/failure domain, no product
import or isolated historical file restoration/post-GC proof, and no full
release/stress/platform/hosted or arbitrary hardware power-loss qualification.
