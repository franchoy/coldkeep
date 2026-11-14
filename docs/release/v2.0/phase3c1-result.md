# Phase 3C1 local qualification

Phase 3C1 unit: LOCALLY QUALIFIED. Hosted acceptance: PENDING.
Parent action 2.04: ACTIVE. Phase 3C: ACTIVE. Phase 3C2: OUTSTANDING.
Implementation: ACTIVE. Product gates: INCOMPLETE.

The implementation starts from accepted HEAD
`3aa9cae7b15037e6f51c22311e2b2ee0c0178b86` (tree
`adc4d66e98030259d55a8856934d33e0d657f491`). The frozen implementation/test
checkpoint is `a64aad02a362be260e5ca3fa40895365dfa21435`, tree
`d492f0ec42cc7a540475952a0bf11a4c8414a34f`. This report is a documentation-only
successor. The final external evidence must record its exact final commit/tree, independent
outgoing approval, patch replay, guarded publication and actual-head hosted
acceptance. Those final actions remain pending when this source report is created. Under BUILD section 7, that receipt may close C1 without another
report-only source commit. Until then this source deliberately retains a
conservative C1 obligation; it cannot clear C2 or parent action 2.04.

The [model and upgrade contract](generation-model.md) defines schema 20,
recovery package 4, IMPORT_READY v4 and the distinct recovery-only reader.
The checkpointed 19→20 operation preserves historical snapshots, physical IDs,
marker/descriptor bytes and binding revisions. It installs empty structures;
no historical generation, scan, policy or publication is invented.

Generation, entry and policy IDs are namespaced 128-bit values. Typed membership
preserves raw POSIX components and immutable source/binding/policy provenance.
Successful file observations reference completed recipes; directory metadata,
source-root metadata and negative outcomes remain distinct. Names are bounded
at 255 raw bytes, 3072 reconstructed path bytes and 128 components. Policy descriptors
are bounded at 4096 canonical bytes and 32 ordered rules. Existing 20,000-row,
16-MiB aggregate and 4096-byte variable-field catalog limits remain in force.

The actual PostgreSQL 16.15 maximum component fixture inserted an uncompressed
255-byte component with a 367-byte composite ROW representation on 8192-byte
blocks. The maximum canonical policy fixture stored 4096 raw bytes; its complete
catalog encoding used 54,266 JSON bytes across 198 rows. These are selected-profile
measurements, not general cross-platform or unbounded-capacity claims.

Fresh race-enabled qualification retained every accepted 580 recovery test and
three package identities and all 49 passive tests and two package identities.
The current recovery manifest requires 722 tests and four packages. Q1-R2
observer controls and the total-object case remain mandatory. Counts below are
test/subtest events, not counts of independent designs.

| Route | Required test identities | Actual test/package passes | Unrelated test/package skips | Command wall time |
| --- | --- | --- | --- | --- |
| Dedicated passive | 49 + 2 packages | 465 / 2 | 0 / 0 | 451.38 s |
| Dedicated recovery | 722 + 4 packages | 896 / 4 | 0 / 0 | 588.95 s |
| Complete packages, plaintext | 722 + 4 packages | 4722 / 46 | 139 / 3 | 777.08 s |
| Complete packages, AES-GCM | 722 + 4 packages | 4722 / 46 | 139 / 3 | 832.98 s |

The complete codec routes use the frozen checkpoint above and separate fresh,
owned PostgreSQL 16.15 fixtures with the pinned image, 1-GiB tmpfs, 1.5-GiB memory
and 4-CPU limits. Required cases never skip. Broad-route unrelated skips are
retained in raw events and the external ledger; they are not represented as
newly qualified platforms. All four passing routes report matching before/after
resource inventories, no owned sessions, unchanged staged readers and completed
reader cleanup. Dedicated scopes retain 15-minute Go package bounds. Complete codec scopes
allow 35 minutes per Go package within the unchanged 2400-second whole-child
supervisor; other packages consume that same aggregate budget.

The passive route is reused from its valid source-bound checkpoint run. The three recovery/plaintext/AES-GCM routes ran fresh during the powered continuation at that exact same frozen source. It includes
the second-stage constraint-admission batching correction described below, the
stale-checkpoint test correction, packages-only runtime budget and exact guard,
and the strengthened real-deadline cleanup test. Earlier passing dedicated or
plaintext runs remain historical evidence. Exact paths, hashes, modes, commands
and output digests are in the external source-equivalence ledger.

The preserved Python authority/enforcement suite passed 658 tests in 828.773 seconds
at the frozen checkpoint. The current 23-case product oracle passed at
that same checkpoint and is reused unchanged.
The Go enforcement package passed in 859.043 seconds at this corrected
checkpoint; its execution inputs are bound separately. Build, vet, module
integrity, pinned golangci-lint 2.9.0 built with Go 1.26.10, repository guards and
Windows compile qualification passed; the final documentation checkpoint gets
fresh static/governance checks before outgoing approval. No new Windows runtime
or native-byte behavior is claimed by the compile result.

The integrated Phase 2 proof is reused from its successful run at
`a64aad02a362be260e5ca3fa40895365dfa21435`: 85 required native tests, 38 required
PostgreSQL cases, browser restores, packaging, credential/TCP controls and owned
Docker cleanup. A separate owned detached worktree held prepared assets, with
exact source and prepared-asset bindings retained. This report is the only later
source change; it does not alter proof or build inputs.
Both final hosted events must rerun their ordinary complete routes.

The C1 cases cover before/during/after-commit interruption, uncertain response,
exact retry, stale/wrong identities and checkpoints, old-recognized staged and
installed intent fences, alias competition, cancellation/deadline and joined
rollback/driver/child/credential cleanup. The independent checker verifies the
owned checkpoint's bytes/profile and manifest-member digests; it is not a
second restored semantic read of catalog.dump. The immutable upgrade intent
pins that fresh generation and the current catalog plan. The cooperative-owner
boundary is explicit in the contract.

Untouched D19/pre3C and current writers are refused on schema 20, including real
caller-supplied-handle Store/GC/Repair/SetConfig attempts. Positive schema 17
controls establish that the old operations really mutate when allowed. The
schema 20 statement fence stops mutations before default/sequence effects;
restricted recovery pin changes require a validated owned session. Catalog,
payload and sequence fingerprints are checked. This is a cooperative old-tool
compatibility fence, not a security boundary against a malicious database owner.

Plaintext and encrypted schema 20 export/import preserve generation-only recipe
100 with zero current-file/snapshot references, exact lineage and private reader
content. Isolated staged-operator historical restores compare reread content and
metadata; pins return to zero and original paths are inaccessible. This is
same-host functional isolation; physical independence remains unknown. Legacy
counter meanings are unchanged. All schema 20 ordinary mutation, reclamation and
repair remain fenced, and every IMPORT_READY target remains recovery-only.
Generation-only preservation does not establish schema 20 live-GC survival.

Development failures and corrections remain in the evidence. The first full
Python run failed a stale fixed-total structural assertion; the correction
retains the exact inherited identities/digest while allowing only C1 additions.
The first recovery run failed the source-capability guard; exact file/function
allowances and two failing mutation controls qualify the narrow new seams.
Two complete plaintext attempts hit the 15-minute recovery-package timeout
without earlier assertion failures, including a controlled fresh-server retry.
They left one and two owned databases/roles respectively; both failed cleanup
receipts and exact-ID fixture disposal are retained. The source-capacity case
took 271.76 and 272.21 seconds, compared with 82.90 in dedicated qualification.
Freshness did not resolve the slowdown; its cause remains unestablished.
Independent review supported the documented packages-only 35-minute budget
inside the existing 40-minute supervisor. Both complete codec commands, required
events and cleanup had to pass; no partial run or projection qualified. No
mandatory identity or product/resource ceiling was relaxed.

The first run within that larger package budget completed with one failing
deadline subtest (and enclosing test/package failure events), while all resource
inventories matched. The test began its two-second deadline before checkpoint
work and never observed migration rollback within its five-second wait. The
source-supported early-expiry explanation is not presented as a measured cause.
The corrected fixture uses a ten-second real deadline beyond that unchanged
five-second admission wait, requires a live context at rollback entry, then
requires DeadlineExceeded, owner exclusion and non-return while rollback and
physical close remain held. All four race variants passed; both complete
codecs rerun the corrected test. No production deadline changed. The Python
rerun also caught the stale CI guard timeout literal; exact 35m command equality
and an unlimited-timeout mutation refusal now pass, with the full suite rerun.

The AES fixture setup initially stopped before tests because socket readiness
observed the temporary PostgreSQL initialization server before its restart.
Recovery verified the original owned container, final TCP readiness and pristine
database inventory before creating the two intended test databases. The failed
setup, raw startup log and recovery are retained separately from test results.

The subsequent AES route at the preceding checkpoint timed out in the final
row-security case after 2100.137 seconds in the recovery package. The upgrade
operator case had already passed. No earlier assertion failed, but the required
row-security parent/package pass was absent and owned resource inventories
differed. That failed route is retained and its exact fixture was disposed.

A separate instrumented 64-source diagnostic measured 37,260 individual
constraint-definition queries and 275.737 seconds. PostgreSQL recorded no JIT;
reducing GOMAXPROCS to four did not improve the result (280.073 seconds) and was
not adopted. The helper now batches only its second-stage expected-definition
comparisons after the unchanged full name/kind/status/raw-size admission.
Typed parameters, unique constraint OIDs, same-row size guards and boolean-only
output preserve refusal before unsafe deparsing. Missing rows, excess counts,
definition mismatches, cancellation and cleanup failures still refuse.

Focused race qualification passed the admission, lowered-limit, compressed/raw
definition, cancellation and held-cleanup controls. Against an unchanged real
catalog, each of the 53 expected definitions was independently perturbed and
refused. The canonical comparison used one query with all 53 result rows.
The instrumented capacity case then used 399 comparison queries and completed
in 164.812 seconds. These are bounded diagnostic observations, not a general
performance guarantee. Diagnostic settings were not used for the four passing
qualification routes, which used fresh default PostgreSQL fixtures. All affected
qualification was repeated after this production correction; no pre-correction
pass substitutes for it. The external diagnostic receipt also records the
ignored ambient public-key typo in the unsuccessful four-processor timing
experiment and the premature static invocation that refused a dirty tree before
running checks.

Two fresh dedicated recovery attempts at that checkpoint hit the unchanged
15-minute package limit: the first in TestExecutionBounds (778 test/subtest
passes and three package passes), and the isolated retry in
TestRecoveryGenerationAdmissionBounds (580 test/subtest passes and three
package passes). Neither had an earlier assertion failure. Later required tests
were missing and owned database/role inventories differed. Both exact fixtures
were disposed. The isolated retry ran after all other suites had finished, so
ongoing overlap is not a sufficient explanation. Schema 17 cases that bypass
the changed validator also slowed down. Host observations then showed AC power
disconnected, a discharging 13–14% battery, a power-saving profile and sampled
frequencies mostly near 1.4 GHz. This may contribute, but no historical or paired
power/frequency baseline proves causation. Further budget changes were deferred
while requesting power handling. These failed routes remain separate from any
subsequent successful qualification; source/commands identify every attempt.

The 11 November continuation observed AC already connected and a charging
battery (initially 67%); it proceeded without renewed confirmation. One ordinary
fresh pinned fixture ran a race-enabled ten-minute diagnostic selection with
explicit plaintext/empty ambient key, count=1, p=1 and default GOMAXPROCS=16.
The source-capacity case passed in 50.89 seconds and the schema 17 ambient-isolation
control in 0.41 seconds. Reader hashes and before/after inventories matched;
owned fixture disposal was verified. Independent review accepted one unchanged
15-minute dedicated attempt from these observations. No harness, product deadline,
scheduler or resource-capacity change was made. The three complete routes ran
sequentially on distinct owned fixtures; no failed partial runs were combined.
The agent did not change host power policy. The final AES observation showed
performance governor/energy preference, whereas its before observation showed
powersave/power. The precise transition time and cause are unknown; both
observations and a clarification receipt are retained. The preserved seven a64 successes retain exact source/test/harness/
requirement/dependency bindings, including the prepared Phase 2 asset manifest.
They are reused evidence, not fresh executions in this continuation. The powered
comparison does not establish a controlled AC/battery causal result.

Requested settings from the adopted BUILD: GPT-6 Astra / Extra High.
Observable session identity: Codex based on GPT-6. Exact model variant and actual
reasoning effort: unknown / unknown. The separate outgoing reviewer is an
independent agent explicitly requested with GPT-6 Astra / Extra High; exact actual variant/effort are
also unknown. No parent model switch is claimed.

The accepted checkout, predecessor worktrees/evidence, main and stable
v1.13.19 are preserved. Keep PR #129 draft with auto-merge unset. The start ZIP's
computed SHA-256 is recorded; no external start-ZIP sidecar was supplied.
The resume packet independently confirms that computed start digest matches the issued bundle; the original missing-sidecar note is preserved. Both the resume packet and incomplete checkpoint archive passed complete CRC/path/manifest verification.
Its complete internal manifest and the existing accepted evidence archive
SHA-256 `4b1dbcd4e68ab2a3ee2e5a833699f344c62cf2dd55ab32d0146cfe75204e0484`
were verified before editing. The [C2 handoff](next-slice.md) owns measured
indexed reads, keyset pagination and integrated closure. Capture, publication,
ordinary writer activation, durable jobs, generation-based user restore and
product UI remain with their later owners.
