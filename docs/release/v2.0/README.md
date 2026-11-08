# Coldkeep v2.0 active charter

Current unit: **Phase 2 follow-up / action 2.02 BUILD**, authorized by
`coldkeep_phase2_followup_codex_2026-11-08.md` (8 November 2026 follow-up bundle).
[Current authority](../current.json) records **2.02 ACTIVE**, implementation
ACTIVE and product gates INCOMPLETE. Application **2.0.0-dev.0**, latest stable
**1.13.19**, /v2 and profile manual-postgresql-docker remain unchanged.

Accepted base: `e25da48003f30aeacb900131be28c3e092f85283`, tree
`bad2400116d4dbb4fd50446acd48034c82ffb2d8`. Accepted 2.01 and local-profile
2.03 COMPLETE remain in their unchanged reports, including the
[Phase 1D follow-up](phase1d-followup-result.md). Preserve all predecessors.

## Phase 2 scope and validation

Prove an embedded React/TypeScript/Vite UI and typed /api/v1 in one owned Go
fixture process; authenticated history-to-restore through the existing engine;
and real Linux x86-64 CGO SQLite persistence alongside CGO-disabled PostgreSQL
Docker. This is an internal delivery and feasibility proof. It does not qualify
native support, SQLite lifecycle/defaults, durable jobs or production authentication.
No 2.04, schema/payload/recovery-format change, real repository adoption or publication.
All Go dependency pins, checksums and Go 1.26.10 remain fixed. Frontend tools/dependencies are pinned separately.

Required clean-checkpoint validation:

- Go 1.26.10, GOTOOLCHAIN=local, GOWORK=off, pinned lint; build, vet, formatting,
  shell checks, CI audits/mutations, matrix, row-writer/source and dependency guards.
- Owned-runtime/API/security/admission tests and blocking ordinary Linux proof gate;
  packaged browser history-to-restore, independent reread, browser-close ownership,
  competing process and shutdown joins; no Node runtime or external assets.
- Prepared-build identity, missing/stale assets and independent CLI go install.
- Real SQLite store/close/reopen/restore and copied historical reader, named no-CGO
  refusal; PostgreSQL CLI and daemon Docker, mount/non-root/temp/exposure proof.
- Fresh unchanged 23-case product oracle; source-bound reuse of unchanged affected
  SQLite/reader/store checks.
- Source-bound reuse of accepted recovery/coordination/command race gate, all 244 requirements;
  original-loss, post-live-GC and large-receipt scenarios remain blocking.
- Fresh full Python suite and focused authority tests; explicit 0/2.01/2.03 fixtures,
  historical-v1 validators against the actual frozen checkout and 849-file digest.
- Source-bound raw commands, identities, initial failures, predecessor preservation,
  owned cleanup, local commits, replay-verified patches and checksummed evidence.

The submitted [Phase 2 result](phase2-result.md) is preserved. Its local completion
is held pending P1 restore-destination admission, P2 TCP reachability classification
and P3 complete-value byte bounds. The follow-up requires fresh counterexamples,
corrected race tests, integrated native/browser/Docker proof, current oracle,
Python/authority and build/static checks. Unchanged recovery, broad SQLite/retained
history and D19 evidence may be reused only with exact source/command/log bindings.
The follow-up result will record conditional local completion after all criteria
pass; implementation stays ACTIVE and product gates INCOMPLETE. No 2.04 work.
Actual model/effort are unknown unless directly observable.

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
