# Phase 3B follow-up R1 result — 9 November 2026

```text
Phase 3B follow-up R1: COMPLETE, awaiting independent review
R1 lossless operator root selection: PASS
Submitted-source decoder and operator counterexamples: REPRODUCED
Fresh recovery/operator qualification: PASS
Parent action 2.04: ACTIVE
Implementation: ACTIVE
Product gates: INCOMPLETE
Phase 3C: OUTSTANDING
Ordinary schema18/19 writers: REFUSED
IMPORT_READY maintenance/activation: REFUSED
Publication: NOT AUTHORIZED
Governing BUILD: coldkeep_phase3b_followup_codex_2026-11-09.md
```

This closes the bounded local R1 / P3B-PATH-1 correction for independent review.
The original Phase 3B report and all accepted predecessors remain unchanged.
No Phase 3C implementation, push, PR, merge, tag, release, ordinary-writer
admission or live installation switch occurred. The proposed public development
branch/draft PR workflow remains a separate next adjustment after review.

## Exact source and authenticated inputs

Submitted HEAD `cd5f9f9ac96b963f46fe6c24b7e38e0c2b525e69`, tree
`9848d2a14478b13dce15212fb8602238331282c4`: clean, all 1,774 tracked hashes,
Git blobs and modes verified, and all 53 selected comparison copies matched.
Launch ZIP SHA-256
`ebef1b996111f8ba1eda209e5c86408d1488e5115ea69a4f9755ddc6e8d8e581`;
78 members / 77 complete checksums. Nested submitted ZIP SHA-256
`b855025bfde810786a94ef8bdfe068d231e4672a1f03d362779d06aaf4a416da`;
462 members / 461 complete checksums. Standalone submitted records matched.

Qualified clean implementation HEAD `cfa9ecebe18dec7d0cbd81fe10c31a63aa079bbc`,
tree `19b529915e5a39fe05da96d7bf011b50e8a09b73`, on
`release/v2.0-phase3b-followup` at
`/home/franchoy/vscode/coldkeep-worktrees/v2.0-phase3b-followup`.
The separate completion commit changes only documentation/instructions;
`final-identity.json` records its exact HEAD/tree and clean status.
`final-documentation-delta.json` proves no production/test/runner change after
qualification. Incremental and ordered commit patches independently replay to
the final tree including modes; every final tracked file is inventoried.

Requested model/effort: GPT-6 Astra / Extra High throughout. Actual parent model
and effort are not observable and remain unknown. Delegated review/implementation
requests explicitly selected `gpt-6-astra` / `xhigh`; no account/model switch is
claimed. Tools were pinned Go 1.26.10 with `GOTOOLCHAIN=local`, `GOWORK=off`,
CGO enabled for race qualification, golangci-lint 2.9.0, PostgreSQL16.15 clients
and the retained digest-pinned PostgreSQL16 image. Tool, module, dependency,
frontend, schema, package, receipt and control-format pins remain unchanged.

## Correction and representation contract

Only `cmd/coldkeep-recovery/main_linux.go` changes production behavior. It reads
at most the existing 1 MiB config ceiling plus one sentinel byte, closes the
input, rejects excess bytes, and checks raw UTF-8 and escaped UTF-16 fidelity
before typed decoding. A bounded linear scan distinguishes escaped quotes and
backslashes, scalar escapes and immediately adjacent high/low surrogate pairs.
The existing JSON decoder retains syntax, type, unknown-field and trailing-input
checks. Malformed Unicode refuses before credential/key access, root or
installation admission, checkpoint/control creation or database work.

Literal U+FFFD and `\ufffd` remain the same legitimate name. Valid supplementary
pairs, literal backslash-plus-`udcff`, whitespace, case and non-ASCII bytes remain
exact. Base64 still selects native `ff` without Unicode replacement. The retained
policy treats nonempty plain and base64 values as ambiguous and refuses both;
empty/absent/null string fields contribute no selection. This clarifies existing
behavior, without forcing valid plain configs to base64. The config ceiling is
separate from unchanged receipt/control limits. See [contract](physical-binding.md).

R1 was marked ACTIVE before implementation. The structural validator retains
`2.04-phase3b-followup-r1` even when the original Phase 3B report is present.
Only the bounded follow-up's complete, unique status fields clear R1; they cannot
clear 3C, complete 2.04/product gates or authorize publication. This remains
structural authority, not an independent runtime acceptance receipt.

## Expected versus observed R1 matrix

The submitted probe kept all 1,774 production/dependency/existing-test files and
modes unchanged; only three recorded test overlays were added. Both submitted
and corrected operator probes staged the actual binary using the unchanged
ordinary-runner staging prefix, with explicit focused children. No miniature
operator or duplicate test-only parser was substituted.

| Input or boundary | Submitted observation | Corrected observation |
| --- | --- | --- |
| Raw native byte `ff` in plain JSON root | Decoder admitted U+FFFD; real preview selected genuine replacement-character directory | Early Unicode refusal for preview and apply |
| Lone low `\udcff` and high `\ud800` | Both decoder negatives failed as expected; both real previews exactly matched valid U+FFFD plan and physical evidence | Both preview/apply paths refuse before credentials or writes |
| Genuine literal/escaped U+FFFD | Literal/escaped decoder controls and literal-U+FFFD real preview succeeded | Actual preview/apply/check succeed; literal and escaped spelling select identical bytes/identity |
| Separate native-byte `ff` sibling | Otherwise valid schema18 sibling preview succeeds with distinct root/control/lock identities within the recorded device/mount context | Base64 preview/apply/check succeed; exact SQL path bytes and logical identity retained |
| Valid pairs, backslash-plus-`u`, whitespace/case/non-ASCII | All supplied decoder controls pass | Exact decoder bytes and actual preview/apply/check pass for all six representation cases |
| Invalid UTF-8 variants, mismatched/reversed/separated surrogate pairs | Additional corrected regression scope | Predecode refusal, including malformed keys/credential-path text; typed destination remains untouched |
| Config byte ceiling | Existing limit retained | Exactly 1 MiB value/padded configs decode; excess value/padding/trailing bytes refuse before typed allocation |
| Syntax and selection | Retained contracts | Unknown/type/truncated/malformed/trailing input and ambiguous or invalid base64 selection refuse |
| Historical reader control | Supported existing recovery route | Actual staged encrypted schema19 import/restore at escaped U+FFFD root rereads eight fixed historical files and metadata; zero pins/sessions |

The three successful malformed submitted previews are the qualifying operator
counterexamples. No malformed apply was attempted on submitted production.
The corrected six negative preview/apply cases use normally valid credentials,
request and plan from the genuine-U+FFFD positive preview. That exact plan is
then successfully applied with valid input, excluding stale-plan attribution.

Recursive inotify watches observed zero transient writes in payload/control,
installation, native sibling and checkpoint-parent trees. Independent logical
catalog/tree fingerprints, physical lock/root identity and explicit absent
checkpoint/marker/descriptor/intent assertions also passed. Credential IN_OPEN/
IN_ACCESS watches observed zero accesses, and operation-owned sessions were zero.
A positive create/write/remove and credential-read control proved the observers
could detect transient activity. Input bytes are recorded as hex; root selections
are base64 with physical evidence. Config records contain credential file paths,
never their contents. Negative records identify the positive comparison root,
not a root selected by the refusing command.

## Fresh qualification and precise reuse

Fresh full recovery/coordination/operator race qualification passed **742** test
and subtest run/pass pairs: recoveryexport 560, coordination 99, operator 83.
All **566 mandatory events** passed: **491 retained + 75 new** (56 decoder/config,
19 staged operator). Zero missing, failed or skipped events. Duration including
reader staging: **507.885 seconds**. The retained native-byte, untouched
old/current-reader, schema19 isolated recovery and schema17 post-live-GC controls
all executed in this ordinary blocking gate.

The unchanged current oracle passed **23 outcomes: 12 restores, 9 expected
refusals and 2 retained known limits**. Full Python passed **607 tests**.
Focused authority **41**, recovery-runner mutation **36**, integrated-runner **6**,
TCP mutation **6** and Go CI-enforcement **3** also passed; overlapping totals
are not summed. New missing/skipped-event and nonzero-Go-child mutations cover
both config and staged operator requirements. Build, vet, pinned lint, tidy-diff,
format/whitespace, shell syntax/shellcheck, row-writer/source guards, validation
matrix, current/historical authority and governance, and local CI audit passed.

The first current-oracle run was interrupted by an evidence-helper cleanup
ordering mistake: its disposable working directory was removed during a SQLite
upsert, yielding a read-only-database error. This was not a qualifying pass.
The exact failed stream/helper are retained in `product-current/` and
`tooling-notes.json`. Server cleanup and product disposal were separated, and
`product-final/` passed on unchanged clean source. No gate was weakened.

The original full Phase2/native/browser/Docker/TCP proof and broader database,
storage/restore/pinning regressions are **reused, not fresh**, from clean
`d04702b778f18fd14d32baa37c6fc0842f80a5c8` /
`6d32a423e2ded3c4c00e13580d469cd601c32e59`. Four original command records and
91 artifacts retain exact log digests. A transitive production/test closure,
recursive fixtures/embedded assets, runner/frontend/deployment inputs and pins
bind **598 unchanged files**, exact scoped membership and Git/physical modes;
every selector resolves to real files. Historical D19 retains its original
checkpoint, six evidence artifacts and unchanged oracle boundary. No new prepared
daemon binary is claimed by this reuse.

The reused failed `TestEngineDependencyDirection` scans the whole production
import graph. A separate comparison proves all internal package edges unchanged;
only standard-library `bytes` and `unicode/utf8` imports were added to the operator.
This does not turn that failed test into a pass. Both inherited engine families,
including `TestEngineBatchCancellationStopsNewDispatchWithPartialResult`, remain
FAILED/UNQUALIFIED. The four retained nonqualifying skips are
`TestRestoreNativeDarwinTempPathWithoutEvalSymlinks`,
`TestStoreInterleavingRetryClaimSignalsCASBoundary`,
`TestStoreFolderWorkersFourWithConcurrentGCPlanNoCorruption` and
`TestPhase3R13ReadQueryRestoreCWDChild`. Fresh required recovery events have no skips.

## Preservation, cleanup and residual limits

All **37 preexisting worktree registrations**, including **9 absent paths**, and
**134 existing refs** were preserved without pruning/reset. The original submitted
candidate, **15 prior result records**, all **849 frozen v1 files** (aggregate
`64fb69cb875c4bdf96c10cd759275b7232849c2f8083b0b406f11d7b18b23c7a`) and protected
production/dependency/fixture scopes remain byte-identical. The new ledger protects
actual `db/adoption_postgres.sql`, correcting the old nonexistent
`db/identity_postgres.sql` selector; each protected path/prefix must match tracked
files. The original archive/report is not rewritten.

One owned disposable PostgreSQL server used 2 GiB RAM, 1536 MiB tmpfs and four
CPUs; Go used `-p=2` and GOMAXPROCS=4. Heavy gates ran sequentially. Before server
removal, only postgres/template databases remained: zero fixture roles, other
sessions, subscriptions and replication slots. Owned credentials, probe checkout,
reader staging and disposable fixtures were removed after their respective runs.
Original Docker container/network/image identities were preserved; only the
correction branch/worktree remains added. `resource-observation.json` is a
point-in-time observation, not a peak-memory or general-scale claim.

The Linux/session-bound, same-host physical-binding limits remain. No hardware
independence, clone resistance, broader platform, live installation or stable
product qualification is added. Review N1 remains: a failed `tx.Commit()` can
leave outcome unknown while the conservative status says committed/reconciliation
required. R1 does not redesign that state machine. Partial/prepared intent staging
continues to refuse; universal automatic repair/reconstruction is not claimed.

Evidence directory:
`/home/franchoy/.local/state/coldkeep-release-evidence/v2.0/phase3b-followup-20261109`.
Review archive: `coldkeep-phase3b-followup-evidence.zip`, with complete internal
`SHA256SUMS`, external ZIP checksum, standalone result/identity/patch equality,
source hashes/modes, exact command logs and two independent patch replays.
