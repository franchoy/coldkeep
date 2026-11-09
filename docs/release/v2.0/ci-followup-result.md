# Hosted-CI follow-up: local qualification checkpoint

The bounded M1/E1/E2/C1/Q1 corrections are implemented and primarily qualified
at `4282b4360056ad91aa7ab79d46b1803f6c946e5d`, tree
`28c55e5d6e773e077d75ec19659674e101cc0c33`. The final implementation checkpoint,
`4efa9e38a10d026f809a70a47f93dc882b2ae47f`, tree
`3e18cb617b2df9724eccec3c60bd919a0c072f5e`, changes only the dedicated recovery
package timeout from 10 to 15 minutes. Its fresh qualification is recorded below.
This record is prepared before
the authorized guarded update of the existing development branch. Hosted
acceptance is **PENDING** here. The final external evidence must identify the
actual pushed head and both completed event runs; this document does not
claim their outcome in advance.

The owner explicitly authorized the full bounded follow-up and guarded push
on 9 November 2026, adopting `coldkeep_v2_ci_launch_prompt_2026-11-09.txt`
and `coldkeep_v2_ci_followup_codex_2026-11-09.md`. Accepted publication is
`ac9382c9e620b17b43800c2a5cf0065e9e75446d`, tree
`a4dd032b18d42f5b5f53896a9af01cd271aa3b2d`, on
[draft PR #129](https://github.com/franchoy/coldkeep/pull/129).
The separate correction worktree preserves the original accepted worktree,
all prior accepted commits/reports and the stable main line.

## Corrections and limitations

| Area | Implemented correction | Qualified boundary |
| --- | --- | --- |
| E1 | Real cancelable contexts and a private per-engine completed-item boundary replace Err-call counting. | All three public batch APIs, both fail-fast modes, before/after/extra-read/during/uncanceled controls. Removing both effective dispatch stops makes 18 cases fail. Production cancellation checks remain. |
| E2 | `internal/application` composes the proof service using borrowed daemon-owned handles. | No second pool or discovery/close ownership; explicit root, exact path/name/size and cancellation controls; unchanged architecture guard and fresh integrated proof. |
| C1 | Both full command/internal package routes stage verified readers and real PostgreSQL prerequisites for the entire command. | Required plain/AES-GCM matrix, independent dedicated recovery gate, exact 566 accepted test identities plus three package passes, all downstream/aggregate dependencies retained. |
| M1 | Non-tag candidates install through a fresh external consumer pinned to the resolved full public commit. | Independent Git origin, module version/sums, pre/post graph, replacement/exclusion refusal, compiler and installed binary checks; same implementation on Linux/macOS/Windows. Local Linux evidence uses the already public accepted source; corrected-source hosted evidence remains pending. |
| Q1 | Removes the two password env files and password values from all three JSON configurations; supplies scoped child environments. | `PGPASSWORD` for fixture/daemon, `DB_PASSWORD` for the retained CLI, `POSTGRES_PASSWORD` for initialization; real authenticated acceptance/refusal, precedence, secret observations and owned cleanup faults. |

Standalone `go install github.com/franchoy/coldkeep/v2/cmd/coldkeep@SHA`
remains an observed unsupported development route because the pinned Go
installer separately queries `latest`. The isolated-consumer method proves
remote exact-source consumption; it does not resolve that standalone behavior.
Semantic-tag installation retains its separate Unix/PowerShell checks. No tag,
module/version change, replacement dependency or stable-install change is used.

Environment delivery is not encryption. Relevant processes, privileged observers
and Docker container-environment metadata can expose fixture credentials.
Owned proof child trees are supervised and their cleanup is checked. Unresolved
local child ownership retains private working files and fails; a failed Docker
cleanup or residual readback also prevents success. No guarantee is made after
SIGKILL, host loss or unreachable Docker. No CodeQL suppression or dismissal is authorized.

## Measured local evidence

| Fresh campaign | Source | Observed result | Elapsed |
| --- | --- | --- | --- |
| Complete plain command/internal packages | `4282b4360` | 4,568 test/subtest passes, 45 package passes; all 566 + 3 requirements | 777.735 s |
| Complete AES-GCM command/internal packages | `4282b4360` | 4,568 test/subtest passes, 45 package passes; all 566 + 3 requirements | 695.440 s |
| Dedicated recovery, original limit | `4282b4360` | 742 test/subtest passes, three package passes; zero skips/failures | 507.586 s |
| Dedicated recovery, measured 15-minute limit | `4efa9e38a` | 742 test/subtest passes, three package passes; all 566 + 3 requirements, zero skips/failures | 507.847 s |
| Current fixed oracle | `4282b4360`, then `4efa9e38a` | All 23 expectations match in both fresh runs | 2.947 s / 3.045 s |
| Historical D19 fixed oracle | D19 `d3581602a`, invoked at `4282b4360` | All 23 expectations match; 1,631 original source blobs verified | 7.214 s |
| Complete Python suite | `4efa9e38a` | 635 tests, OK | 564.248 s |
| Integrated native/browser/packaging/PostgreSQL Docker proof | `4282b4360` | PASS, including all 85 required native tests and cleanup | 61.102 s |

Each broad codec command has 139 unrelated skipped tests plus three packages
without test files. The engine package passes in both routes; its 31 unrelated
skipped tests are part of those disclosed totals. Neither broad route skips a
required recovery identity. Each oracle has 12 RESTORED, nine REFUSED and two
KNOWN_LIMIT outcomes, matching the unchanged expectations; this is not a claim
that every fixture restores. Both owned campaign servers and all staged readers
were removed with verified resource preservation and zero generated-password
matches in the retained evidence.

The required recovery set is compared by identity, not only count. Its canonical
SHA-256 is `8eb9e04d2061e1c8659cb89a38e43648c0c241d2349900d0bf749503f9f19062`.
All completed campaigns contain every required event with no required skip or
failure. The external evidence enumerates every unrelated skip identity and
reported reason; these remain unqualified rather than being reported as passes.

The final full Python suite passes 635 tests. Fresh native/browser/packaging/
PostgreSQL Docker qualification passes in 61.102 seconds, including 85 required
native tests, real credential controls and all TCP/CLI checks. Docker cleanup,
local process cleanup and private-directory removal are complete. Generated
password observations find zero matches across 63 precleanup and 60 retained
files. Five real cleanup fault replays preserve initial uncertainty/failure
outcomes, then prove owned-resource removal and unrelated-resource preservation.

Fresh affected runner/audit mutations, formatting, shell syntax/ShellCheck,
module tidy and whitespace checks pass. The Go audit/candidate-lint mutation
campaign passed at `4282b4360` in 714.513 seconds. Build, vet, pinned lint,
dependency verification, current/historical authority, governance, validation
matrix, row-writer and CI audit pass freshly at `4efa9e38a`.

The timeout adjustment follows a measured 511.595-second recovery package in
the plain broad route: a 600-second limit left 88.405 seconds of headroom.
The finite 900-second package limit keeps the existing 25-minute hosted
correctness job limit and every required event. Both original hosted recovery
steps were skipped after an earlier failure, so there is no prior hosted
recovery timing. Final hosted fit still requires observation.

The exact `4282b4360` to `4efa9e38a` boundary has one changed line, no added or
removed files, no mode changes and 1,789 identical remaining tracked files.
The broad package command and common runner path are identical; only the
selected recovery command's timeout differs. Broad plain/AES-GCM, integrated
Phase 2, D19 and Go audit-mutation evidence are explicitly reused with their
original command/log hashes and this full path/mode comparison. All Go,
Phase 2, frontend, packaging, dependency, workflow and validator inputs are
unchanged. Dedicated recovery, current oracle and Python are fresh at the new
checkpoint. Final documentation changes must be checked separately before
publication and preserve the implementation.

Two real local Linux consumer installs of accepted public `ac9382c9e...` pass,
including poisoned ambient Go configuration and an untouched fake local
candidate. Real Go parser controls reject grouped replacements/exclusions.
Verifier and mocked-orchestration controls reject graph upgrades/replacements,
missing or malformed identity, wrong origin, Go/CLI/module versions or sums,
and missing binaries. They also exercise CGO metadata and native-command failure
checks. These controls supplement the real installs; none of these local runs
qualifies unpublished corrected source.

Original hosted failures are reused with their accepted source/job/log hashes.
Both initial CI runs had 12 successful, six failed and eight skipped jobs;
CodeQL analyzers succeeded but four findings remained open. Fresh correction
failures are retained: initial toolchain/browser setup mismatches, a SQLite
fixture wrapper missing optional `ExecContext` forwarding, an unsupported
`Path.open(opener=...)` call in the private-file helper, an effective GOENV
assertion, and a `/proc` observation race. The initial cancellation-mutation
result parser also had an overly specific postcondition; its raw expected
nonzero result was retained and correctly classified. Each attempt remains
attributed to its original source; the corresponding corrected runs pass.
Independent reviews identified and closed wrapper bypass, explicit-target,
detached-child/drain and TCP cleanup propagation gaps before publication.

Tool identities: Go 1.26.10 (`GOTOOLCHAIN=local`), Node 24.22.2, npm 11.19.1,
golangci-lint 2.9.0, PostgreSQL clients 16.15 and the unchanged pinned
PostgreSQL image. Requested model/effort: GPT-6 Astra / Extra High. Actual
runtime model/effort are not independently observable; no account settings changed.

## Hosted acceptance and next handoff

The guarded update must prove ancestry and the exact expected old remote ref,
then advance only `refs/heads/release/v2.0`. PR #129 must remain open/draft
against unchanged main with automatic merge disabled. Preserve its existing
historical description; send no PR comments or reviewer requests.

Both push and pull-request CI must qualify the actual new head, including all
six remote platform/event installations, both codec legs per event, every
applicable downstream job and CI Required Gate. Record the synthetic PR merge
identity separately. Both CodeQL analyzers, the separate findings check and
the latest Codacy result require actual readback. Four original alerts must
clear through code changes; skipped, pending, canceled and unknown are not green.

Final external deliverables must bind the public head/tree, complete hosted
inventories, failure attribution, fresh/reused evidence, preservation/cleanup,
independent review and both incremental/ordered patches reconstructed to the
final tree including modes. The compact archive must exclude credentials, raw
working databases, private caches and disposable binaries; exact checksums and
standalone equality must be verified before delivery. See [next handoff](next-slice.md).

Parent **2.04 ACTIVE**, implementation **ACTIVE**, product gates **INCOMPLETE**,
Phase 3C **OUTSTANDING**. Ordinary schema18/19 writers and every IMPORT_READY
target's maintenance/activation remain refused. No merge, ready transition,
release, tag, protection/default-branch change, live installation switch or
Phase 3C work is part of this correction.
