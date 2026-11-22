# Phase 3C2 owning result

Phase 3C2: COMPLETE
Phase 3C: COMPLETE
Local-profile action 2.04: COMPLETE
Implementation: ACTIVE
Product gates: INCOMPLETE
Profile: installed Linux/PostgreSQL16 schema20
Governing BUILD: coldkeep_phase3c2_build_2026-11-12.md
Accepted C1: 06bde38380dd3a42be5c180beb99bf1aa509b8ac
Qualification HEAD: efb26b340b9ee113f4af4ef8367502b0c2bedd18
Qualification tree: 8eb302363766275d45efa2fe8b745123258ce1ad
Qualification SHA-256: aa8527c9295fd8f2d094804a489e0d79aa7b9656a65bc82a51f6a56e26698444
Ordinary schema18/19/20 writers and reclamation: REFUSED
IMPORT_READY maintenance/activation: REFUSED
Action 2.05: NOT IMPLEMENTED
Hosted: PENDING

Local integrated qualification is complete. Final C2 acceptance additionally
requires independent review of the exact outgoing source/evidence and both
actual-head hosted events, including separate findings and latest Codacy.
The final external receipt records those outcomes without rewriting this
point-in-time source result. The [C1 bridge](phase3c1-acceptance-bridge.md) and
all original predecessor reports retain their accepted bytes.

Requested settings: GPT-6 Astra / Extra High. Actual parent model variant and
effort: unknown (not exposed). Independent review was configured through the
collaboration tool as `gpt-6-astra` / `xhigh`; actual backend variant/effort are
not independently exposed. These are execution guidance, never CI prerequisites.

## Implemented boundary

The [read contract](generation-read.md) and [qualification matrix](phase3c2-qualification-matrix.md)
define five typed version-1 model operations, byte-exact DTOs and one owned
repeatable-read snapshot. The view retains the canonical lease, installed
physical/control evidence and a separate non-data alias-reservation connection.
Callback return closes method admission, cancels and joins outstanding work,
revalidates identity and finishes physical cleanup before ownership release.
Trusted callbacks must cooperate; there is no forced-stop/hard-return promise.

Generation order is opaque ID order under COLLATE C; child order uses raw
BYTEA components and distinct root/nonroot SQL. Cursors bind the view, exact
scope, fixed query/order, upper bound and last identity. They are neither
authorization nor publication receipts; arbitrary well-formed last-key edits
are not all detected. Pages issue their own indexed SQL, with no OFFSET,
Inventory slicing, repeated full admission or optional filters.

Page default is 50, maximum 100, limit+1 at most 101. Request/cursor/response
ceilings are 16 KiB/4 KiB/1 MiB. `response_limit` can only lower the response
ceiling; oversized results return `ErrResponseLimit` without successful data.
Native POSIX bytes use canonical base64; int64 values use exact decimal strings,
including values above 2^53. Null and zero remain distinct. Existing component,
path, depth and policy limits remain 255 bytes/3072 bytes/128/4096 bytes/32 rules.
Pinned Node exercises the actual JSON consumer boundary.

Explicit optional-index preparation validates the exact nonunique B-tree,
creates/checks a fresh recovery checkpoint before DDL, uses one owned finite
transaction, and verifies unchanged logical catalog/payload/physical identity.
Unknown commit responses reconcile only exact-present state; absence needs a
new checkpoint destination and conflicting definitions refuse. Schema20,
package4 and recovery profiles17–19/SQLite17 remain unchanged. Untouched C1
at 06bde383 is the tenth required staged role; all nine earlier roles remain.

The first C2 commit fixes the inherited sleep-container fixture: bounded tmpfs
overrides PostgreSQL's declared data VOLUME, effective configuration/mountinfo
is checked, and exact-ID/task-owned cleanup uses anonymous-volume removal as
a fallback. Success/early-failure and unexpected-mount/foreign-owner/cleanup
controls are blocking. No volume is removed by inferred age, name or emptiness.

## Qualification and exact reuse

Runtime qualification used clean HEAD `45c76bdce69ab9199c14ded0c29f419f0b2b2e8f`,
tree `b6fa1cc9adf5fce4109d510ba7ae575b5e054442`, exact Go1.26.11,
GOTOOLCHAIN=local, pinned PostgreSQL16/clients and Node24.22.2. AC was online.
No heavy complete routes ran concurrently. Required package/child ceilings
remain 15 minutes dedicated, 35 minutes complete and 2400 seconds whole-child.

| Route | Independently checked result |
| --- | --- |
| Recovery, race | 866 required test/subtest passes + four package passes; observed 1040 + four; no skips; 729.636 seconds including staging. |
| Passive, race | 55 required test/subtest passes + two package passes; observed 471 + two; no skips; 374.758 seconds including staging. |
| Complete plain, race | 890 required test/subtest passes + four package passes; observed 4866 + 46 package passes. |
| Complete AES-GCM, race | Same 890 + four required and 4866 + 46 observed passes. |
| Current fixed oracle | All 23 fixed content/refusal/known-limit outcomes matched, source preserved. |
| Fresh untouched D19 oracle | All 23 outcomes matched; original source blobs and modes verified. |
| Full Python | 671 tests, no failures/skips. |
| Go CI enforcement | 54 required top-level tests, 397 tests/subtests and package passed; no failures/skips. |
| Static and security | Build, vet, pinned lint2.9.0, tidy-diff, dependency/row-writer boundaries, active/historical validators, contract/type checks, CI audit/mutations, formatting/shell syntax and reachable-vulnerability scan passed. |
| Integrated Phase2 | Fresh native/browser/Docker/package/install/TCP/PG proof; required semantic events, prepared source/assets and resource cleanup independently checked. |

Every accepted 722 recovery and 49 passive requirement remains. Each complete
codec route has exactly the same 142 inherited optional/platform/no-test skips
as the trusted C1 archive; these are not passes. Mandatory packages have no
skips, and actual hosted PostgreSQL gates remain independently required.
Required C2 read/index/DTO/cursor/lifetime and fixture cases are included in
the final race routes, not qualified by combining development partial runs.

The qualification HEAD above adds only `phase3c2_authority.py` and its tests
to the runtime checkpoint: 1826 other tracked paths have identical bytes/modes.
The correction binds identified executable/fixture/config/schema inputs and
tracked additions. Fresh ten-role staging was verified before/after the
source-equivalence comparison; the original runtime routes retain their
whole-child staging verification.
Separate normalized old/new operator builds are byte-identical. Normal staged
binaries retain actual source/path/VCS provenance and are not claimed identical.
Those probes support reuse, not replacement of original qualification binaries.
Phase2, full Python/Go CI, both oracles, static and vulnerability checks ran fresh
at the authority successor. The four completed runtime routes are explicitly
reused with their original identities. A later result/docs successor requires
unchanged execution-input digest and fresh appropriate authority/static checks.

`phase3c2-qualification.json` binds execution inputs with SHA-256
`61fd3a2b23e2230a6029d19ec031570aeffabc55f73fb220053aff1c00c46e09`.
It is a structural authority record, not self-certifying runtime evidence.
External `local-qualification-ledger.json` binds commands, source identities,
raw reports and independent recomputation to all 18 obligations.

Failed development attempts remain in `attempt-ledger.md` and raw logs. These
include the initial battery recovery timeout and narrow SQL guard correction,
synthetic runner prerequisite failures, two authority input-binding corrections,
and a Go CI command missing the accepted explicit 25-minute timeout. That run
failed at the default ten minutes. A second full attempt refused the runner's
TMPDIR outside the lint harness's required `/tmp` boundary. The complete rerun
used a uniquely owned `/tmp` directory and C1's already accepted 25-minute
budget, preserving both failures. No timeout/partial run is a pass. Failed recovery
inner cleanup was unqualified; exact owned outer tmpfs disposal is recorded.

## Measurements and limits

Opening still performs full bounded catalog/graph/payload/key admission and
inventory serialization: O(total admitted catalog/payload), at most 20000 rows,
16 MiB catalog and existing two-minute admission ceiling. It is not constant
time/memory. Raw EXPLAIN ANALYZE/BUFFERS and allocation evidence is retained.

| Inserted selected + distractors | Total admitted rows / catalog bytes | Opening ms / allocated bytes | Largest plan-node work / sorts | Largest measured Read ms / response bytes |
| --- | --- | --- | --- | --- |
| 50 + 50 | 498 / 128000 | 126.608 / 8723912 | 102 / 3 | 7.195 / 22792 |
| 500 + 500 | 3198 / 866000 | 310.290 / 19921288 | 101 / 1 | 11.847 / 44897 |
| 2500 + 2500 | 15198 / 4146000 | 1128.001 / 69005344 | 101 / 0 | 11.909 / 44897 |

There are 48 measured calls. Limit-one allocations remain about 100–120 KiB
across these sizes. The measured shallow calls use 2–5 data queries plus two
inherited pg_locks ownership-reservation checks per Read. EXPLAIN covers data
queries only; elapsed/allocation figures include reservation checks. Global
pg_locks work is not an indexed-work claim. Deep selectors perform bounded
per-component lookups up to 128; the shallow query count is not universal.
Timing is context, not a latency guarantee. No planner setting forces scans.

Exact generation/nonroot-child cardinalities 0/1/100/101/200/201 and separate
root-query scaling are covered. Maximum-name tests are separate from aggregate
scale. The oversized-component case deliberately removes a schema constraint
and proves admission refusal; it is not isolated execution proof of the page's
CASE cap. Four indexed continuity receipt filenames encode the plaintext bool,
not index absence; indexed/unindexed coverage is established by named Go events.

## Preservation, publication and next work

The external evidence retains launch/C1 checksums/manifests, complete source and
tool provenance, expected events, fresh/reused ledger, failed attempts, query
plans, cleanup, independent review, both patch forms/replays and final archive
identity. Only exactly owned resources are disposed; baseline Docker resources,
predecessor registrations/refs/bytes/modes/status and earlier evidence remain.
Unattributed temporary resources are never covered by a universal cleanup claim.

The adopted publication is one guarded fast-forward of existing release/v2.0
and bounded current-status refresh of draft PR #129. Main, stable v1.13.19,
PR title/base/draft and unrelated work remain unchanged. No merge, tag, release,
deployment or live switch is authorized. The [next handoff](next-slice.md) is
action 2.05 runtime/daemon ownership planning/build preparation under its own
adoption, recommended Astra/Extra High; C2 implements none of that action.

This is bounded installed Linux/PostgreSQL16 metadata browsing with exact DTOs
and recovery continuity. It creates no committed capture history, product
capture/publication or user-facing generation restore. Windows native-byte
runtime, broader native delivery, media authenticity/failure-domain independence,
real reboot/unmount, cross-filesystem transfer and schema20 live-GC survival
remain unqualified.
