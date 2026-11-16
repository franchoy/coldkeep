# Phase 1B passive/inventory result — 8 November 2026

**Phase 1B passive existing-repository opening and quiesced full-catalog
inventory unit: COMPLETE. Parent action 2.03: ACTIVE / INCOMPLETE.** Product
gates remain incomplete. This is a local review candidate, not a recovery
package or independent-protection claim.

**Missing coordination material remains a limitation:** passive opening refuses
an absent root, control directory or persistent lock. It never prepares them.
Legacy repositories without a prepared lock need the explicit preparation
decision carried into the [next handoff](next-slice.md).

## Entry, identity and preservation

The supplied ZIP SHA-256 is
`8e7bdbbc36acfa65703677f1ec9367bb89b0136f7d80b333d4df9ad0d4ec5a82`.
All 16 entries in its `SHA256SUMS` verified. The user's operative instruction
was `coldkeep_phase1b_2_03_passive_inventory_codex_2026-11-08.md`; other bundle
documents supplied evidence and context. The carry-forward patch verified as
`fb6432081768bfa071a143aef78c1754e93c2915d031f0268814918dcac5af52` and applied
without reconciliation changes in local commit
`d63e6bf09ef2a204fa5f0402ebf3b6b9e5e7857a`.

The supplied working directory contained the actual main checkout at
`/home/franchoy/vscode/coldkeep/coldkeep`, clean at
`e8a501d6edbc32cd440562065f1a851d8afdc03d`. The accepted Phase 1A worktree
was separately verified clean at the exact expected HEAD
`cc2c175549b9a185de8c79943ae5855a4aa8d44f` and tree
`c62d1d17780bb3037c4eee5e15106ebd6650d2aa`. Both are preserved. Work proceeded
from that accepted commit on `release/v2.0-phase1b` in
`/home/franchoy/vscode/coldkeep-worktrees/v2.0-phase1b`.

Implementation checkpoint `2e31dde54bccc31189ffc96561feacd31d27904f`, tree
`bc86a74923e7048078871ce24f1d8398f04c4e57`, contains the complete unit.
Final runtime checkpoint `1f46389e99f8f41a8886c35c853635ed66af694f`, tree
`1b352436bb17dd71a531281c64658ffcfc18e085`, adds resolver-time ambient-input
refusal and its tests. Focused acceptance, current product fixtures, lint, vet,
build, module and source guards were rerun on that clean checkpoint. The later
completion commit changes only this report and its charter link. External
`final-identity.json` records final HEAD/tree, ordered commits, clean status,
tested identities and the exact documentation delta, without self-reference.

The Phase 1A report, compatibility ledger, product contract, retained fixture
expectations/harness/assets, schemas, container format implementation, go.mod
and go.sum remain byte-identical to acceptance. All 849 frozen v1 release files
retain digest `64fb69cb875c4bdf96c10cd759275b7232849c2f8083b0b406f11d7b18b23c7a`.
Latest stable remains 1.13.19; application version remains 2.0.0-dev.0. The
existing authority selects phase 2.03 ACTIVE and
`LOCAL_ACTION_2_03_PASSIVE_INVENTORY_ONLY`; completing 2.03 or publishing remains
outside that scope.

Requested execution was GPT-6 Astra / Extra High (`gpt-6-astra`, `xhigh`). Actual
model and reasoning settings are not observable in the available session
metadata, so they are recorded as unknown. No account/global settings changed.

## Implemented boundary

The [API contract](passive-inventory.md) defines explicit configuration, mode,
supported inputs, lifetime, checks, refusals and limits. `Inspect` resolves one
catalog/root/key/profile tuple, acquires an open-existing lease in the legacy
process/native-lock domain, opens one PostgreSQL read-only repeatable snapshot,
enumerates the full schema-17 catalog and validates every retained physical
reference. It consumes the inventory synchronously while owned, closes all
readers/transaction/database, then releases ownership. Any partial, cancelled,
lost-lease, lost-snapshot, consumer or cleanup failure is incomplete. Resolvers
run once; an introduced alternate libpq host or credential source refuses.

Plaintext/encrypted legacy and packed input, none/zstd compression, snapshot-only
history, physical-file history, pins, zero-current recipes, published repair
journals, retired extents and catalog-listed orphan containers are covered.
Copy units are whole containers. Interrupted/inconsistent states refuse;
uncataloged entries are observed as excluded without pruning or traversal.
Empty files do not acquire invented payload objects. No source identity is
invented, so a byte-identical copied legacy root is not distinguishable as an
original installation by an ID v1 never stored.

Checks include metadata/schema requirements, graph closure, encoded headers,
extents, whole-container SHA-256, available block/chunk hashes, bounded decoding
and authentication. There is no reconstructed-file verification claim. Retained
inventory is diagnostic material after the cut closes; later copying must
establish/revalidate a fresh owned cut. Ownership excludes participating local
writers/maintenance, not arbitrary external SQL/filesystem changes or undrained
workers inside a future daemon.

## Actual environment and bounds

Executed on Linux x86-64, Go 1.26.10 (`GOTOOLCHAIN=local`, CGO=1), GCC 13.3.0,
Python 3.12.3 and Docker 29.9.2. The filesystem tool reported `ext2/ext3`.
Disposable PostgreSQL was **16.13**, image digest
`postgres@sha256:2586e2a95d1c9b31cb2967feb562948f7d364854453d703039b6efa45fe48417`,
on loopback port 55483 with tmpfs database storage. Each new inventory fixture
used a separate disposable database and SELECT-only reader role. Fixture setup
explicitly prepares coordination before the passive baseline. No user catalog,
repository or key was used. The disposable container is removed at completion.
The unchanged hosted PostgreSQL pin is a different observation, not a locally
executed hosted result. Windows/macOS coordination only cross-compiled here.

Supported passive backend is PostgreSQL 16, profile
`manual-postgresql-docker`, explicit local plaintext transport. Existing SQLite
fixtures still execute; no passive SQLite/WAL adapter or backup support claim
was added. Packed inputs are immutable retained historical artifacts; legacy
inputs use declared historical builder recipes. No historical encoding changed.

Hard ceilings: 20,000 rows; 16 MiB catalog bytes; 4,096 bytes per text/bytea field;
512 schema columns; 8 MiB per stored/decoded block; 256 MiB cumulative decoded
work; 1 GiB per object / 4 GiB total objects; 20,000 directory entries; 32 MiB
diagnostic JSON; two-minute operation deadline. Callers can lower limits.
Queries bound iteration with a remaining-row limit and one overflow sentinel;
field guards run before server JSON serialization. Hashing uses 64 KiB reads;
decode is one bounded block at a time. The callback must honor cancellation.

## Expected versus observed acceptance

Raw event details and the bounded inventory example are in `final-focused/`;
`scenario-results.json` indexes test events and scenario observations.

| Scenario | Expected | Observed |
|---|---|---|
| Compatible complete catalog | Include current/history/snapshot/pin relationships and whole object | PASS: 168 rows, 33,779 catalog bytes, one 465,751-byte object; diagnostic-only complete result |
| Legacy/packed and plaintext/AES-GCM | Correct readers/key requirement; plaintext succeeds without a key | PASS, including mixed recipes; no key replacement |
| Published repair / retired closure | Retain all journal and retired physical references | PASS: 181 rows, four whole objects |
| Pin-only chunk and catalog orphan | Preserve roots outside current-file view and unreferenced container | PASS: 165 rows, 35 chunks, two whole objects |
| Missing/ambiguous configuration or unprepared lock | Refuse without initialization/fallback | PASS, including missing database/root/control/lock and resolver-introduced host/passfile |
| Future schema/catalog block requirement and actual encoded future header | Refuse each independently | PASS; retained old-reader format-metadata KNOWN_LIMIT remains unchanged |
| Missing/wrong key, incompatible/missing/truncated/unsafe payload, inconsistent graph/interrupted state | Distinct bounded refusal, no admissible partial result | PASS across all declared refusal cases |
| Passive source preservation | No schema/catalog/pin/payload/control mutation, including transient writes | PASS: before/after semantic/byte comparisons, Linux mutation events, positive create-undo detector and constructor/call-boundary guard |
| Same coordination domain | Legacy/passive exclusion in-process and across processes; canonical aliases | PASS; existing owner bytes and persistent lock retained |
| Snapshot and resources | Consistent read-only snapshot through consumer; close before lease release | PASS: write attempts refused, external SQL does not change snapshot, failure at every declared stage, cancellation, dead transaction, no surviving session and lost lease never complete |
| Actual bounded enumeration | Admit bounded large history; refuse during enumeration at lower capacity | PASS: 6,000 added history memberships, 12,168 rows, one object, 927,203 catalog bytes; 500-row limit stops after 500 delivered rows in 214 ms |
| Oversized metadata/decode/output and cancellation | Incomplete bounded result | PASS; observed allocation delta for the large-history test was 40,803,464 bytes including preservation harness, not a peak-heap guarantee |

The complete focused runner recorded **159 passing test/subtest events, zero
skips, failures or missing required events**, under the race detector.

## Applicable gates and honest limits

| Gate | Result and source binding |
|---|---|
| Complete Python governance suite | PASS, 542 tests during implementation; final explicit authority regression suite PASS, 24 tests; validator code unchanged afterwards |
| Active authority/governance and explicit historical-v1 route | PASS; historical route used preserved main; active final result remains development-only with 2.03 obligations |
| Carry-forward mutation test, including `|| true` / `; true` | PASS on clean implementation checkpoint |
| Local CI enforcement audit | PASS on final runtime checkpoint; blocking focused step added outside protected historical wrapper interval |
| Current product oracle | PASS, all 23 declared outcomes, final runtime checkpoint, race detector; accepted D19 result reused without re-execution |
| Container/catalog/storage/verify/recovery/db regressions | PASS, 1,277 test/subtest events at clean implementation checkpoint; 11 declared skips, detailed below |
| Previously skipped compression-gated regressions | PASS, 12 test/subtest events with explicit zstd gate on final runtime checkpoint |
| Build, vet, tidy diff, pinned golangci-lint 2.9.0 | PASS on final runtime checkpoint, zero lint issues and no dependency drift |
| Gofmt, shell syntax/ShellCheck, smart quotes, validation matrix, row-writer scope, diff whitespace | PASS |
| Windows and Darwin coordination compilation | PASS on final runtime checkpoint; native runtime tests NOT RUN |
| Source/fixture/frozen-history preservation and predecessor worktrees | PASS; final identity and preservation records carry exact hashes |

The 11 broad-regression skips were one Darwin-only test, six compression-gated
tests, two SQLite concurrency limitations and two invalid-codec cases prevented
by schema CHECK constraints. The six compression tests were then explicitly
enabled and passed. The other five remain recorded skips, not passes. Existing
regression source is identical between its tested checkpoint and final runtime
checkpoint; only the new inventory configuration seam changed afterwards.

Initial failures are retained rather than overwritten: system Go was too old
until the installed pinned toolchain was selected; existing PostgreSQL fixture
tests needed their explicit test-bootstrap flag; the global linter was too old,
then pinned lint found one De Morgan simplification, which was fixed. The first
new CI step placement broke the frozen CK-015 source interval; moving it after
the existing PostgreSQL step restored the interval. A broad Go audit run begun
before that correction reported three baseline audit failures and no other
reported failures; all three tests and their mutations passed on rerun. This
does not relabel the original broad run as passing. A wrong-column-type test
fixture needed its PostgreSQL default dropped before casting; the corrected
refusal passed. The final lint wrapper initially required its external evidence
directory to exist; preparing it produced a passing run. Final configuration
review added refusal of ambient changes introduced by credential/key resolvers,
then reran the focused suite and affected quality gates.

Hosted CI, full release profiles, stress/performance qualification, native
platform runtime, recovery copy/completion/import and original-installation-loss
restoration were NOT RUN or implemented for this unit. No publication or full
product qualification is claimed.

## Review artifacts and next unit

Evidence root:
`/home/franchoy/.local/state/coldkeep-release-evidence/v2.0/phase1b-20261108`.
It contains `final-identity.json`, `review.patch`, ordered `commits/`, the compact
`coldkeep-phase1b-evidence.zip`, `validation-index.json`, raw command/event logs,
input checksums, inventory example and final preservation records. The bundle
has an internal SHA-256 inventory and an external ZIP digest. Its command ledger
redacts only the disposable server password; no production secrets or key bytes
are included. Early commands before source capture was added are identified as
implementation-stage runs rather than falsely bound to the final tree.

The [next handoff](next-slice.md) carries explicit legacy coordination
preparation, fresh owned full-catalog/whole-object copying, PostgreSQL dump and
restore requirements, any later supported SQLite backup decision, staged
completion and corruption/interruption refusal, independent key/reader custody,
constrained fresh-destination import, isolated restoration with the original
installation unavailable, and survival after later live GC. Those integrated
action 2.03 results precede schema additions. Action 2.02, daemon worker drainage
and future epochs/jobs retain their existing owners. This run starts none of
that remaining work.
