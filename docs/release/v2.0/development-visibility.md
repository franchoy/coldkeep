# V2.0 development visibility

## Prepared scope and approval

The prepared workflow makes existing v2.0 development reviewable on GitHub:
one `release/v2.0` branch in `franchoy/coldkeep`, preserving accepted commit
ancestry, and one **draft** pull request from that branch to `main`.

**This document does not authorize publication.** Independent acceptance of the
bounded Phase 3B R1 correction is recorded below. Before the remote step, record
explicit user approval of this one-branch/one-draft-PR scope. Approval was not
supplied when this proposal was prepared; any actual approval is recorded
separately. A completion report, uploaded evidence or a request to discuss
workflow does not provide that approval. Local preparation
may finish first; with approval already supplied, the owning BUILD can complete
publication and hosted observation in the same run.

The approved scope, when granted, is limited to the prepared source and docs
commit. It grants no push of other local branches, tags or evidence archives,
no rewriting accepted commits, no merge to `main`, no release, no protection
change and no deployment or live installation switch. Later remote updates
need their owning instruction's authority. Do not use `--all` or `--mirror`.

Historical local-only BUILD results, including
[R1's `Publication: NOT AUTHORIZED`](phase3b-followup-result.md), retain their
original meaning and bytes. Record the later approval and actual remote results
separately. Keep `docs/release/current.json`, all machine-readable state blocks,
phase results, version/module identity, dependencies and frozen v1 records
unchanged. The validator's `separate-publication-authorization` obligation
remains outstanding; this workflow neither clears it nor certifies product,
merge, tag or release readiness.

## Foundation and current progress

The foundation is Phase 3B R1 HEAD
`815f812a5027dad4f03c88ae30b97c77ad01b8e0`, tree
`6dc1296efe72083014d34febe1e9f57944489419`. Add the reviewed documentation
change as a new commit. Preserve the existing implementation, correction and
result commits; retain predecessor worktrees for reproducibility.

### Independent acceptance — 9 November 2026

Independent review **ACCEPTED the bounded Phase 3B R1 correction** at that
HEAD/tree. The review inspected source, regression-test semantics, both patch
forms, original-source counterexamples, raw qualification logs, reuse/preservation
records and archive integrity. It reconstructed the submitted, qualifying and
final trees and checked all 1,777 final source files and modes. No further R1
correction was required by that review.

The reviewed `coldkeep-phase3b-followup-evidence.zip` has 297 members and 296
verified checksum entries, with SHA-256
`6ab6515dd8d6179f820b220bbc527a21a66b2540d4b827a37d9eb1a8d0ecde91`.
The review did not run uploaded helpers, candidate binaries/tests, PostgreSQL
or Docker workloads, and did not inspect the user's live local installation.
Runtime and host-cleanup statements remain independently inspected recorded
evidence, rather than fresh execution by this reviewer. Acceptance does not
qualify the inherited engine failures or authorize publication, ordinary writers,
ready-target activation, broader platforms or a product release.

| Work | Qualified scope | Remaining boundary |
| --- | --- | --- |
| 2.01 | Baseline/compatibility fixtures and product contracts | No product qualification implied. |
| 2.02 | Internal delivery and selected Linux feasibility proof, including P1–P3 corrections | Broader native/product support remains open. |
| 2.03 | Linux/manual PostgreSQL recovery profile, including F1–F2 corrections | Physical failure-domain independence is not established. |
| 2.04 Phase 3A | Explicit identity adoption and exact recovery profiles, including A1–A2 | Parent 2.04 remains ACTIVE. |
| 2.04 Phase 3B | Bounded physical binding and schema19 recovery; R1 lossless config selection corrected locally | Independent R1 review accepted the correction on 9 November 2026; Phase 3C remains OUTSTANDING. |
| V2.0 product | Development version `2.0.0-dev.0` | Product gates INCOMPLETE; later product/support/release gates remain open. |

The governing results are linked from the [charter](README.md) and
[next handoff](next-slice.md). They distinguish fresh runs, evidence reuse and
unqualified outcomes. R1 recorded 742 recovery test/subtest passes covering
566 mandatory requirements, 607 Python tests and all 23 fixed-oracle outcomes.
That bounded evidence is not an assertion that the whole repository or hosted
suite is green.

Two inherited engine families remain failed/unqualified:

- `TestEngineBatchCancellationStopsNewDispatchWithPartialResult`;
- `TestEngineDependencyDirection`.

Retain the four disclosed nonqualifying skips and all platform/network limits
in the owning reports. Ordinary schema18/19 writers remain refused; every
IMPORT_READY target refuses binding/source maintenance and activation. Later
ownership/adoption work must qualify those transitions separately.

## Preserved workflow and validation

Before publication, inventory local refs/worktrees and verify the clean R1
HEAD/tree, predecessor ancestry, current remote `main`, target-branch absence
and the exact outgoing commit list. Inspect all outgoing content for accidental
credentials or private operational data. Preserve local evidence paths and
reports without publishing external raw evidence directories.

Create an isolated development worktree at the accepted R1 commit, apply only
the reviewed docs patch and make a new documentation commit. Preserve all
original evidence commits. Verify changed-path allowlists, file modes, tree
identity, unchanged machine authority and frozen history. If actual checkout,
remote state, ancestry or the reviewed scope differs, resolve the discrepancy
before the remote step; do not silently incorporate unrelated commits.

This is a docs-only validation profile. Run current release-state/governance
checks, the focused v2 authority tests, local CI-enforcement audit and whitespace
checks. Record explicit historical-v1 checks where required by the owning BUILD.
There is no need to repeat disposable recovery/runtime proofs solely to change
these documents. Their prior outcomes and failures remain attributed to their
original source and commands. Do not add tests that merely restate prose.

The remote creation must be conditional on the destination branch still being
absent, including at the server's update boundary. A prior absence check alone
is insufficient. If the branch already exists, stop that creation rather than
advance or overwrite it. Publish only the exact prepared commit, then verify
the remote branch's object ID before creating the approved draft PR. Reconcile
a partial success by readback; do not create duplicate PRs or retry blindly.

## Hosted checks and build boundaries

Existing CI and CodeQL workflows already cover `push` on `release/**` and PRs
whose base is `main`. Initial development publication needs no workflow or
protection change. Push and PR runs may both occur and are separate evidence.
The existing remote candidate-install gate uses the public commit SHA; a v2
tag is unnecessary for that check.

Observe actual hosted run/check URLs, source identities and outcomes. Failed,
skipped, pending and unavailable remain distinct. The broad engine suites may
expose the known inherited failures; new hosted failures need attribution.
Keep the draft open and record those findings. Do not disable tests, weaken
`CI Required Gate`, change protections or claim a green gate to create visibility.
Codacy findings, if reported for the PR, are follow-up evidence rather than a
claim that analysis has already passed.

The CLI remains independently buildable/installable. The internal daemon proof
needs the [prepared embedded-asset build](phase2-build.md), with the existing
pinned Go/Node/npm toolchains. A source build alone does not qualify a runnable
daemon package; do not commit generated assets to make a status badge pass.
No broader native or product support is established by opening the draft.

Future phase PRs targeting `release/v2.0` require a separately reviewed expansion
of the CI/CodeQL pull-request base filters. The initial draft targets `main`,
so that expansion is deferred. Keep small coherent commits and reviewable phase
boundaries without treating each implementation action as a released version.

## Execution guidance

For this bounded docs, Git and publication workflow, recommend **GPT-6 Astra /
High**. If hosted results require substantive CI, architecture or correctness
work, prepare a separate bounded correction with **GPT-6 Astra / Extra High**.
Record requested settings separately from observable settings; use unknown
when actual model or effort is unavailable. These are personal execution
recommendations, not contributor requirements or CI gates.

Completion of an approved publication run means the exact branch and draft PR
are visible, their identities/URLs are recorded, hosted states are truthfully
observed, and `main`, tags, releases, installations and predecessor worktrees
remain preserved. It does not require concealing or repairing all hosted
findings inside this docs-only unit, and does not complete action 2.04.
