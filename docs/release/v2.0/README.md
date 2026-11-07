# Coldkeep v2.0 active charter

Adopted 7 November 2026 (Europe/Madrid), under instruction edition 1.0.
V2 implementation is active under the bounded Phase 0 BUILD authorization.
The single machine-readable phase/state authority is
[../current.json](../current.json). Phase 0 is complete as a local development
candidate; every product gate remains incomplete. See [the result](phase0-result.md).
Latest published stable is **v1.13.19**, independently recorded in the frozen
[v1 closure](../v1.13/v1.13.19-post-publication-closure.md).

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

## Phase 0 scope and validation

Establish active authority and matching provider instructions; explicit
family/version/state dispatch with a preserved historical-v1 route; root Go
module `github.com/franchoy/coldkeep/v2`; complete prerelease identity; current
package/evidence consumers; and the next bounded fixture/recovery handoff.
No daemon, schema, storage, capture, migration or recovery-export implementation
is authorized in this phase. No user repository is migrated. Push, PR, merge,
tags, releases and deployment are outside this local candidate run.

Use pinned Go 1.26.10 with `GOTOOLCHAIN=local`, language floor 1.25.0 and unchanged
dependencies. Required local evidence:

- `python3 scripts/validate_release_state.py --state auto --json`
- `python3 scripts/validate_governance.py`
- `python3 -m unittest discover -s scripts -p 'test_*.py' -v`
- `bash scripts/audit_ci_enforcement.sh --local-only`
- Strict version/dispatch negative cases, historical-v1 fixtures, complete
  module build/test compilation, version/CLI/architecture checks, actual `/v2`
  required-test events and rejection of missing/skipped/legacy package evidence.
- `go vet ./...`, formatting, shell syntax, validation-matrix and row-writer
  guards. Run affected Go regressions in disposable PostgreSQL/storage, never
  a user catalog. Compile tests with `go test -c`, not TestMain execution.
- Frozen v1 path/byte digest and scope inspection; no dependency drift.

This bounded profile adapts PRE_RELEASE_CHECKLIST applicability for Phase 0.
Existing hosted gates remain enforced. Full release, platform, stress,
benchmark and publication qualification stays with its owning action; an
unrun gate is never a pass. Exit requires these applicable checks, coherent
identity/authority/evidence, historical preservation and a concrete next slice.

Historical validation uses `--authority historical-v1 --repo-root <frozen-v1-checkout>`
on both validators. The root must contain its historical source and authority;
archived state blocks in this v2 checkout are not current authorities.

## Version and distribution policy

Initial application identity is `2.0.0-dev.0`; base `2.0.0`, prerelease `dev.0`,
channel `dev`. Dev, alpha, beta, rc and stable remain distinct across parsers,
paths and artifact/tag identity checks. VCS revision, modified/unknown state,
Go toolchain and module build provenance are separate JSON version fields.
Build provenance is explicitly unknown when the Go build omits VCS settings
(including this pinned toolchain in a Git worktree with a `.git` file). An
ordinary disposable clone is used to verify clean/dirty VCS stamping; packaging
at 2.02 must bind its source revision explicitly. An application version change
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

Only F01 authority and F02 identity/evidence Phase 0 portions are implemented.
F14 distribution policy is decided; its build proof remains future work.
All other contracts below are future obligations, not acceptance receipts.

| Finding | Owner | Contract / remaining evidence |
|---|---|---|
| F01 | Phase 0 / 2.01 | Explicit authority and frozen-v1 dispatch; baseline fixtures remain 2.01 work. |
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
| F14 | 2.02, 2.17 | Prepared embedded-UI assets and frontend-then-Go source policy; clean distribution builds, offline UI, API routing, missing asset failures and identity proof remain incomplete. |

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
effort, execution scope, short rationale and completion criteria. Phase 0
recommendation: GPT-6 Astra / Extra High for cross-cutting governance and
identity review. Actual parent model/effort are not observable. This personal
preference creates no contributor subscription requirement, CI gate or global
configuration change. See [next-slice.md](next-slice.md) and the local result.
