# Coldkeep v2.0 active charter

Current workflow: **bounded hosted-CI follow-up**, governed by the adopted
`coldkeep_v2_ci_followup_codex_2026-11-09.md` BUILD and the
[development contract](development-visibility.md). The owner explicitly adopted
its implementation and guarded fast-forward scope on 9 November 2026.
Initial publication is accepted at
`ac9382c9e620b17b43800c2a5cf0065e9e75446d`; continue the existing `release/v2.0`
branch and draft PR #129 after required local qualification and independent
outgoing review. No new PR or renewed initial-publication approval is needed.

Current result: [CI follow-up](ci-followup-result.md).
Current handoff: [remaining qualification and deferred Phase 3C](next-slice.md).
The measured code checkpoint is `4282b4360056ad91aa7ab79d46b1803f6c946e5d`,
tree `28c55e5d6e773e077d75ec19659674e101cc0c33`.
M1 remote consumption, E1 cancellation, E2 application composition, C1 fixture
staging and Q1 credential/cleanup corrections are implemented. Fresh integrated
native/browser/packaging/PostgreSQL proof passed. Plain complete packages passed
with all 566 required tests plus three package passes; their 139 unrelated test
skips and three packages without test files remain explicitly recorded.
Both codec routes, dedicated recovery and the unchanged current/D19 23-case
oracles pass locally. The recovery timeout-only follow-up at
`4efa9e38a10d026f809a70a47f93dc882b2ae47f` and its final qualification are
recorded separately in the result. Hosted corrected-head checks are PENDING
and require actual push and PR observations after publication.
These local results do not certify unrun gates or a product release.

[Current authority](../current.json) remains **2.04 ACTIVE**, implementation
ACTIVE and product gates INCOMPLETE. Phase 3C stays OUTSTANDING and needs its
own authorized BUILD. Ordinary schema18/19 writers remain refused; every
IMPORT_READY target refuses binding/source maintenance and activation.

The preceding **Phase 3B follow-up R1** is COMPLETE and independently accepted
on 9 November 2026. Its [accepted result](phase3b-followup-result.md),
[preserved submission](phase3b-result.md), [physical binding contract](physical-binding.md)
and earlier Phase 3A/A1–A2 records remain unchanged. R1 acceptance is bound to
HEAD `815f812a5027dad4f03c88ae30b97c77ad01b8e0`, tree
`6dc1296efe72083014d34febe1e9f57944489419`; its submitted predecessor was
`cd5f9f9ac96b963f46fe6c24b7e38e0c2b525e69`, tree
`9848d2a14478b13dce15212fb8602238331282c4`.
The R1 report's original failures, skips and reused evidence remain historical
facts; the current follow-up records fresh corrections separately.

Requested settings: GPT-6 Astra / Extra High; actual settings are recorded only
where observable. Version 2.0.0-dev.0, `/v2`, latest stable 1.13.19 and the
manual-postgresql-docker profile remain unchanged.
LOCAL_ACTION_2_04_PHASE3A_AUTHORITY_ONLY is structural authority only.
The adopted branch-update authorization grants no merge, ready-for-review
transition, tag, release, protection/default-branch change, deployment or live
installation switch. `separate-publication-authorization` remains a validator
obligation; a development push does not certify product-release gates.

## Selected first stable profile

**v2.0.0 at action 2.17:** manual local PostgreSQL Docker product. Required
work is 2.01–2.13, 2.15 and 2.17; IDs are work identifiers, not one release each.
One owner, host, repository per daemon and local payload root. A shared Go
application service serves embedded browser UI, authenticated local API and
CLI operations. Capture/archive preserves originals; exact history,
verification, restore, independent recovery export and supported v1.13.19
adoption are required. Ordinary files/directories include empty directories.
Retention starts keep-all. No hosted account/service dependency.

Linux x86-64 Docker Engine is the first qualification target. Ubuntu 24.04
and ext4 are candidate reference choices, **not demonstrated support**.
Investigate Linux native feasibility early; qualify native delivery later.
SQLite Docker, conversion, scheduling, native packages and optional actions
are not selected prerequisites for v2.0. Existing repositories retain their
backend and encryption. Capture is best-effort per-file observation, not an
atomic application snapshot. Existing crash-consistency and plaintext metadata
limitations remain; keys are independently required for encrypted recovery.

## Accepted action 2.01 scope and validation (historical)

Complete the [baseline/compatibility and version-domain ledger](baseline-compatibility.md),
[fixture manifest/harness](../../../testdata/product-fixtures/README.md),
[product contract, profile table and result glossary](product-contract.md), and
applicable evidence. Preserve the completed Phase 0 record, explicit historical-v1
route, `/v2` module, 2.0.0-dev.0 identity and corrected preservation guards.
The accepted entry is `eb9ddebfe7d6d2c24f7b84052b08b1facfe953cb`; it does not
replace the original frozen-v1 historical base in current.json.
No passive-opening, daemon/UI, schema, storage, capture, migration or recovery
export/import implementation is authorized in 2.01. No user repository is
migrated. Actions 2.02 and 2.03 are separate runs. Push, PR, merge,
tags, releases and deployment are outside this local candidate run.

Use pinned Go 1.26.10 with `GOTOOLCHAIN=local`, language floor 1.25.0 and unchanged
dependencies. Required local evidence:

- `python3 scripts/validate_release_state.py --state auto --json`
- `python3 scripts/validate_governance.py`
- `python3 -m unittest discover -s scripts -p 'test_*.py' -v`
- `bash scripts/audit_ci_enforcement.sh --local-only`
- Focused explicit-phase authority regressions; unknown/conflicting scopes and
  release/publication claims fail. Phase 0 tests stay bound to phase 0.
- `python3 scripts/run_product_fixtures.py --reader baseline --output <new-dir>`
  and `--reader current` with a separate new directory. Exact D19 and current
  readers must execute every fixture and match its restore/refusal/known-limit
  oracle; skipped or missing cases are not a pass. Retain raw results.
- Build, new fixture race checks, affected historical-reader regressions and CI
  gate mutation tests. Ordinary Linux CI runs current fixtures; no product code
  or dependency changed. Reuse prior unrelated runtime evidence explicitly.
- `go vet ./...`, formatting, shell syntax, validation-matrix and row-writer
  guards. The new harness uses copied SQLite catalogs/payloads without TestMain;
  other database checks require disposable environments, never a user catalog.
- Frozen v1 path/byte digest and scope inspection; no dependency drift.

This bounded profile adapts PRE_RELEASE_CHECKLIST applicability for action 2.01.
Existing hosted gates remain enforced. Full release, platform, stress,
benchmark and publication qualification stays with its owning action; an
unrun gate is never a pass. Exit requires these applicable checks, coherent
identity/authority/evidence, all original 2.01 deliverables, preserved baseline
bytes and a concrete next 2.03 slice. Completion does not qualify the product.

Historical validation uses `--authority historical-v1 --repo-root <frozen-v1-checkout>`
on both validators. The root must contain its historical source and authority;
archived state blocks in this v2 checkout are not current authorities.

## Version and distribution policy

Initial application identity is `2.0.0-dev.0`; base `2.0.0`, prerelease `dev.0`,
channel `dev`. Dev, alpha, beta, rc and stable remain distinct across parsers,
paths and artifact/tag identity checks. VCS revision, modified/unknown state,
Go toolchain and module build provenance are separate JSON version fields.
Missing VCS metadata was observed in the reported Phase 0 build environment;
this observation does not establish a general Go worktree limitation. When
unknown, modification state remains `null` and revision is omitted. The original
run verified clean/dirty VCS stamping in an ordinary disposable clone; prepared
package revision binding remains 2.02 work. An application version change
does not change API/catalog/payload/recovery formats. Binary names stay
`coldkeep` and future `coldkeepd`.

Prepared daemon binaries/images will contain the embedded UI. Source builds
must document frontend-then-Go steps, pinned frontend tools/lockfile, asset
digest and notices. Production needs no Node server. CLI `go install` remains
supported where its import graph builds independently. Current local source:
`GOTOOLCHAIN=local go install ./cmd/coldkeep`. A future published v2 module uses
`github.com/franchoy/coldkeep/v2/cmd/coldkeep@<published-v2-version>`; no such tag
is created here. Full daemon `go install` may be advertised only when the tagged
module contains generated assets and clean-module installation is qualified.
Action 2.02 proves packaging; Phase 0 only records this policy.

## Amendment register (original graph retained)

F01 authority and F02 identity/evidence Phase 0 portions are complete. Action
2.01 supplies the baseline ledger, fixed oracle and product-result contracts.
F14 distribution policy and the bounded internal prepared-build proof are recorded
in the Phase 2 result; public distribution qualification remains future work.
The contracts below retain all later obligations; this is not a product acceptance receipt.

| Finding | Owner | Contract / remaining evidence |
|---|---|---|
| F01 | Phase 0 / 2.01 | Explicit authority/frozen-v1 dispatch and baseline fixture evidence; later phase transitions remain bounded. |
| F02 | Phase 0 / 2.01–2.02 | `/v2`, prerelease/provenance and evidence identities; qualified public assets remain future. |
| F03 | 2.04–2.05, 2.08 | Private catalog-independent installation descriptor and bounded setup-intent journal; recover same create/adopt intent, authenticated unavailable status, no second history catalog. |
| F04 | 2.03–2.05, 2.15 | Typed inspect/open-existing, create-new, migrate and owned-runtime modes; passive open never initializes/repairs/falls back. One validated catalog/root/key/profile tuple; close resources before releasing lease. |
| F05 | 2.04 | Preserve `.coldkeep-control/repository.lock`; versioned reserved control layout, no top-level payload-like descriptors/SQLite sidecars. Separate portable identity from local credentials. Adopt legacy absence once after recovery; adopted missing descriptor fails. |
| F06 | 2.10–2.11 | Ingest the exact opened regular-file handle or verified bounded private staging; never reopen a validated path; report substitution/instability. |
| F07 | 2.07, 2.10–2.12, 2.17 | Bound actual chunk/recipe buffers, queries, staging and admission; stable database keyset pages. Preserve algorithms; measure before support claims. No new 2.18→2.17 dependency. |
| F08 | 2.04, 2.10, 2.12 | Reversible exact native names end-to-end, opaque entry IDs, no trimming/folding/replacement; retain containment; explicit destination mapping/rejection. |
| F09 | 2.04, 2.11 | Common reachability for published/pending/reader claims across GC, repair, verify, accounting and recovery; directories are metadata, never fake file recipes. |
| F10 | 2.03, 2.15 | Quiesced full catalog plus complete closure, whole containers when needed, independent byte copies, bound manifest/readers/keys. Inconsistent forensic copies are not verified recovery sets. Resume only the same cut or restart. |
| F11 | 2.08 | HttpOnly session plus per-tab in-memory proof from one-use private bootstrap; sensitive reads/writes need both. Exact Host/full Origin, CSRF, deny CORS, practical CSP; stolen loopback cookie alone grants nothing. |
| F12 | 2.02, 2.13, 2.17, 2.21 | Docker mount-role map, read-only sources, non-root identity, bounded temp, `create_host_path: false`, qualified Engine with localhost fix, IPv4/IPv6 checks, no ordinary DB host port. Fixed mounts and controlled recreation after uncertain reattach. |
| F13 | 2.07, 2.15; 2.16 conversion | Irreversible token-retirement frontier, clock uncertainty and random admission epoch after recovery/rollback; preserve compatible restart receipts. Old tokens never admit duplicate intent. |
| F14 | 2.02, 2.17 | Bounded prepared embedded-UI build, offline browser, API routing, missing/stale assets and identity proof observed in 2.02; full public distribution qualification remains incomplete. |

Measured bounds belong to their actions: file/name/entry/memory/query/staging
limits (2.07, 2.10–2.12, 2.17); shutdown/mount envelope (2.02, 2.05, 2.17);
SQLite runtime/lifecycle (2.14); native support (2.22–2.26). No values are
invented here. Original detailed action specifications and dependencies remain
authoritative for future work, supplemented by this adopted review.

## Inputs and execution preference

Planning edition 1.1 ZIP SHA-256:
`edaaa4b04e5c571b03d883a6af1c71cf95f40dfbedaccf9aadc1ca81eb6c231e`.
Phase 0 charter SHA-256:
`b3d30fcc47c501c021fbacc4c4f752eb29f2fb3fc1cf35891b30403445926806`.
Preimplementation review SHA-256:
`ea7605c6b3e49cf98c8bf760233fb338694a354a89f43eda11db94611cb17626`.
The unchanged supplied bundle remains in the user's Downloads; original
specifications 2.01–2.03, shared release/compatibility/recovery contracts and
F01–F14 informed this charter. Inspected code archive SHA-256
`84888b7e47d4e571511edc91dfbeab5705e6d6a6b308769aea703eb465d85414`
is an archive identity, never a Git revision.

Future handoffs for this user specify recommended Codex model, reasoning
effort, execution scope, short rationale and completion criteria. Phase 1A historical
recommendation: GPT-6 Astra / Extra High because historical readers, fixture
oracles and compatibility promises must agree. Actual parent model/effort are
not observable. This personal
preference creates no contributor subscription requirement, CI gate or global
configuration change. See [next-slice.md](next-slice.md) and the local result.
