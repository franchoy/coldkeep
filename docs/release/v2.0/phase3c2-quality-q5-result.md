# C2-Q5 proof and caller-context test qualification

Phase 3C2: COMPLETE
Phase 3C: COMPLETE
Local-profile action 2.04: COMPLETE
Quality unit C2-Q5: COMPLETE
Final Phase 3C2 acceptance: WITHHELD_CODACY
Implementation: ACTIVE
Product gates: INCOMPLETE
Profile: installed Linux/PostgreSQL16 schema20
Governing BUILD: coldkeep_phase3c2_quality_q5_build_2026-11-15.md
Accepted C1: 06bde38380dd3a42be5c180beb99bf1aa509b8ac
Qualification HEAD: d5a08db8c8689ea5d8f71e41a90f07f3270eeff8
Qualification tree: 6a7e3d47041e16097ef64c02206d77c42d48064b
Qualification SHA-256: 8d6dfb604511a0d6a820309e564ca8a401c5bc1e34b7410cb72de074ad046645
Ordinary schema18/19/20 writers and reclamation: REFUSED
IMPORT_READY maintenance/activation: REFUSED
Action 2.05: NOT IMPLEMENTED

All 27 local obligations passed independent source and runtime review on
2026-11-15: seven FRESH, eight RETAINED and twelve COMPOSED. This result completes
local C2-Q5 qualification. Actual-root completed-owner checks, exact outgoing
review, guarded publication and actual-head hosted/native acceptance remain
later gates. PR #129 remains open and draft; no terminal result is claimed here.

The implementation extracts only test fixture/assertion helpers in
internal/application/proof_test.go and
internal/engine/caller_context_propagation_test.go. Independent review preserves
all proof requests/results, six constructor refusals, real SQLite ownership,
caller contexts, ordered short-circuit predicates, live atomic observations and
all 30 batch-cancellation cases. Production Go, other Go tests, workflows,
checked-in runners, dependencies, schemas and tool/image pins remain exact.
Pinned Lizard 1.24.0 covers all 83 bodies in both complete changed files: file
NLOC 214/454, maximum function NLOC 39/26 and maximum CCN 8. All 24 protected
command/generation/coordination metric files retain their required bounds.

Both complete affected packages ran freshly and sequentially under Go 1.26.11,
GOTOOLCHAIN=local, CGO, race/count1/p1 and the unchanged 35-minute per-package /
2,400-second aggregate limits. Each codec passed 847 test/subtest identities
(application 10, engine 837), all 57 target-file identities, the 30-case batch
matrix, 53 selected PostgreSQL identities and both package terminals. Plain ran
from 19:21:11.723444 to 19:21:31.652093 UTC; AES-GCM from 19:22:17.370018 to
19:22:38.267120 UTC, all on 2026-11-15. Explicit positively owned PostgreSQL16
fixtures used the unchanged ordinary backend helpers. The sole ordinary helper
skip, TestPhase3R13ReadQueryRestoreCWDChild, remains separate; its two passing
parent cases executed the real child contract. No required case was skipped.

Fresh support includes the complete 691-identity Python projection, all 22
focused authority tests, 17 event-enforcement identities and 397 Go-CI identities.
The exact identity sets and ordered lifecycles were reviewed, alongside pinned
formatting/build/vet/lint, source/DTO/dependency/CI guards, documentation checks,
metrics and the current pinned govulncheck v1.7.0 scan, which reports no
vulnerabilities. The documentation diagnostics remain preserved. Authority
retains all 26 prior checks and adds proof-caller-context-metrics-equivalence;
all 20 prior test obligations survive their declared fixture/count adaptations.
Q4 record/marker/BUILD fallback and missing-only 27th-check/hash-rebound controls
refuse, with acceptance restored only by restoring the complete valid fixture.

The non-owning Python projection changed only the two Q5 owner documents in an
independent checkout with exact staged entries and modes. Its synthetic fields
never supplied runtime evidence or completed the genuine owner. The genuine
owner stayed incomplete at the implementation checkpoint and throughout both
independent local approvals. The completed ledger then preceded this separate
four-document finalization; all 896 execution inputs retain SHA-256
`25cd238de1f8e87f560d19d8c8dd29270a3728a8ad27c1df81e9837a46e7ef43`. The full checkpoint inventory has 1,855 tracked files; this
finalization preserves all 1,851 other files and all modes. Sixteen historical
C2 records, including the accepted Q4 pair, and twelve earlier records remain
immutable.

Retained evidence is bound to Q4 implementation
`870094af4dcd5705f3a0de31bcd67cc967cd6e8d`, tree
`8ec834272d6550dff132c584869033d83d301040`, and its historical 896-input SHA-256
`fad695e431e32ea7fb206787e47d144e8d48ac7165bd90e2aafb505f83ea9ea3`.
Q4 was published and accepted at `00c00df04c6689fcdcb0813b6601da760ddda8bd`.
The Q4 raw archive SHA-256 is
`0335bb3306915b19b71222dbbb5fbc65e8f4307e32b2a028bccb7db49a802446`.
The ledger preserves all original receipts, timestamps, canonical locations and
modes: 539 raw dependencies, 26 accepted row pointers, 52 historical context
pointers and 22 original execution receipts were independently verified.

Each codec row combines 847 fresh affected-package passes with 4,112 retained
unaffected-package passes. All 794 old application/engine passes per codec are
excluded from reuse. The original Q4 plain command ran from
2026-11-15T13:20:04.082827+00:00 to 2026-11-15T13:35:31.218222+00:00; AES from
2026-11-15T13:35:31.684004+00:00 to 2026-11-15T13:53:00.876028+00:00. These remain
historical commands, not fresh Q5 all-package runs. Their retained subset keeps
108 optional skips, 44 passing package terminals and three no-test packages
per codec separate from qualified test identities.

Other retained routes preserve Q4 focused 289, recovery 1,080 and passive 490
passing identities, fixed current/D19 oracles, completed Phase2 native 85 /
PostgreSQL 38, continuity/query/consumer proof, native coordination, real staged
admission and the repaired missing/null reader controls. Their original times
and raw observations remain in each ledger row. In particular, the Phase2
receipt remains 2026-11-15T15:07:00.055891+00:00 through
2026-11-15T15:07:55.210436+00:00; the repaired missing/null pair remains at
16:41:25.153168–16:42:17.954771 UTC that day. Reuse depends on unchanged
production, unaffected tests, fixtures, runners, reader provenance, tool/image
pins and contracts, mapped per row. The two changed test packages receive fresh
runtime; authority Python receives fresh support checks. No old whole-input
digest or remote installation is represented as current Q5 execution.

Failed attempts remain nonqualifying. The first fresh plain attempt stopped
before a Go child because the external recorder observed the temporary
PostgreSQL socket server; the reviewed correction requires final TCP readiness.
The second plain attempt omitted the existing disposable-fixture bootstrap
selector: 24 PostgreSQL openings failed, producing 48 failed parent/subtest
identities and 23 unrun required identities. Its passing target subset did not
qualify the route. Restoring the existing COLDKEEP_DB_AUTO_BOOTSTRAP=true fixture
selector in a fresh external recorder preserved the command, expected cases,
source, assertions, limits and ownership checks; the complete third plain and
AES routes then passed. Original source-inventory ordering/import and recorder
serialization failures, reviewer locator/counter/receipt-schema/byte-adapter
corrections and failed controls remain bound to old/new bytes, preserved outputs
and independent correction reviews. No failed result was reclassified as PASS.

Owned PostgreSQL/private/temporary resources have per-route disposal records.
The projection and two temporary tooling roots were inventoried and removed.
Preservation covers the 51 predecessor worktrees, 71,343 tracked observations,
2,670 accepted successor evidence files and exact recorded Docker state; only
the owned Q5 branch/worktree were added. The local scan found no matches for
five known generated values. That point-in-time result is limited to those
values; final delivery requires its own scan. Inaccessible/raced processes and
private untracked live storage are not claimed inspected. Historical optional
skip/no-test, compile-only/native-selection, TCP timeout/refusal, query
reservation, benchmark timing/integrity and unknown physical failure-domain
independence limits remain unchanged.

The evidence root is
`/home/franchoy/vscode/coldkeep/phase3c2-quality-q5-evidence-2026-11-15-egtfRso1`.
The genuine ledger is qualification/check-ledger.json, SHA-256
`b0ae9ec51a55717587618891fd13de590b29d1bf3f2fa6bfdc56d36f856b2164`, completed at
2026-11-15T19:53:15.997757+00:00. Independent source approval is
reviews/source/local27-review.json, SHA-256
`4418352441eff09027694d1f3660cdcaaa5c29f6a0dead6459bc85cd5ff92657`, at
19:45:47.984117 UTC; runtime approval is reviews/runtime/local27-review.json,
SHA-256 `6dd4dc61e568d25097d1fc50ea3f855d3e8f1dbe236e5fd297dc07c221975642`, at
19:50:38.767841 UTC. Both precede the genuine ledger and this finalization.
Both independent correction-chain approvals are bound in that ledger.

After actual-root completed-owner checks and independent exact outgoing review,
the adopted BUILD permits one guarded fast-forward from accepted Q4
`00c00df04c6689fcdcb0813b6601da760ddda8bd`. Both actual-head CI events, both CodeQL
workflows and separate findings, full hosted Python/staged/native routes and
complete native comparison remain required before the guarded PR current-status
update. All five Q5 subjects must disappear without unexplained replacements;
account for all 42 other Added subjects, retain all 23 prior absences and keep
six historical Fixed records separate. Overall Codacy may remain failed for
those other subjects. Published-head native disappearance is a later gate, not
an inferred consequence of the local 27th check. The denied optional collection
remains unused and complete global rule coverage remains UNKNOWN. No extra
corrective publication, merge, release/tag, installation switch, ordinary-writer
activation or live maintenance/reclamation is authorized.

Requested implementation and independent review: GPT-6 Astra / Extra High.
Observable backend model/effort telemetry remains UNKNOWN; requested selectors
do not establish actual backend settings.
