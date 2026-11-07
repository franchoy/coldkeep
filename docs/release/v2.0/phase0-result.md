# Phase 0 local candidate result

Prepared 7 November 2026, Europe/Madrid. Instruction edition 1.0.

**Result: COMPLETE — applicable Phase 0 checks passed.** All future v2 product
gates remain incomplete. No publication or later feature work is included in
this result.

## Checkout and execution

Requested model/effort: **GPT-6 Astra / Extra High**. Actual parent model and
reasoning effort are not observable through this session; no substitution is
claimed and no account/global model setting was changed.

- Actual launch checkout: `/home/franchoy/vscode/coldkeep/coldkeep`, clean
  `main` at `e8a501d6edbc32cd440562065f1a851d8afdc03d`; local `origin/main`
  matched. This local revision was the base; no fetch was necessary.
- Candidate: `/home/franchoy/vscode/coldkeep-worktrees/v2.0-phase0`, branch
  `release/v2.0-phase0`. Original checkout and unrelated work remain unchanged.
- The inspected archive was located and independently hashed:
  `84888b7e47d4e571511edc91dfbeab5705e6d6a6b308769aea703eb465d85414`,
  11,943,914 bytes. **Every tracked launch file matched the archive bytes**;
  there was no relevant launch drift. Archive identity is separate from Git.
- Stable remains v1.13.19: D19 `d3581602aee2599547564322fe083baae1d52063`,
  annotated A19 `60b6878114f936b295c415605e8af31def2aba5a`, immutable Release
  `431395975`, from the repository's frozen publication/closure evidence.
- Implementation checkpoints: `9cd2250ff` (authority/module/version),
  `bd4eb812f` and `2c435f6dd` (historical fixture isolation), and `951f6893b`
  (immutable diagnostic-fixture bytes). The completion record is the final local
  commit on this branch. `final-identity.json` in the evidence directory binds
  final HEAD, all local commit identifiers and clean working-tree status;
  `phase0.patch` contains the complete reviewable change from the launch base.

## Delivered behavior

[The charter](README.md) and [current.json](../current.json) select the manual
PostgreSQL Docker v2.0.0 target at 2.17, required actions, keep-all retention,
qualification candidates, distribution policy, F01–F14 owners and execution
preference. Provider mirrors and current documentation agree.

Both validators share explicit family/version/state selection. Missing, mixed,
unknown or contradictory authority fails; v2 patch 18/19 cannot select v1.
Frozen-v1 validation remains explicit on a historical checkout. Phase completion
never certifies a product release or authorizes a tag. Correctness rules,
provider consistency and frozen historical path/byte integrity stay enforced.

The root Go module and current import/package consumers use
`github.com/franchoy/coldkeep/v2`. This includes executable historical regression
tests, architecture guards, CI, installation checks and required-event profiles.
Four approved proof-wrapper hashes changed only for `/v2` package identity;
exact-body, structural and failure-propagation checks remain active.

Application identity is `2.0.0-dev.0`. Parsers preserve complete prereleases and
reject malformed/truncated identities. Version JSON adds base, prerelease,
channel and separate build provenance. Clean and dirty ordinary-clone builds
report their actual revision/modification state while keeping the same version.
Missing VCS metadata was observed in the reported Phase 0 build environment;
the execution narrative does not establish a general Go worktree limitation.
Those builds truthfully report `modified: null` and omit the revision. A normal
disposable clone proved source installation/stamping. Prepared-package revision
binding remains 2.02 work.

## Validation

Evidence directory:
`/home/franchoy/.local/state/coldkeep-release-evidence/v2.0/phase0-20261107`.
Logs retain initial failures as well as corrected results. Exact profile
selectors and script-test group commands are in their JSON result files.

Environment: Go 1.26.10 with `GOTOOLCHAIN=local`, Python 3.12.3, GCC 13.3.0,
Docker Engine 29.9.2, pinned PostgreSQL 16.15 image. Runtime tests used a dedicated
container, tmpfs database, random loopback port and disposable storage. The
container was removed after runtime tests; no user repository/catalog was used.

| Command / evidence | Result |
|---|---|
| `python3 scripts/validate_release_state.py --state auto --json` | PASS, v2 development only |
| `python3 scripts/validate_governance.py` | PASS |
| Both validators with `--authority historical-v1 --repo-root <original-launch-checkout>` | PASS, preserved v1 closure |
| `python3 -m unittest discover -s scripts -p 'test_*.py' -v` | PASS, 525 tests in 361.389 seconds |
| `python3 -m unittest discover -s scripts -p 'test_v2_authority.py' -v` | PASS, 10 tests; independent reviewer also passed them |
| `bash scripts/audit_ci_enforcement.sh --local-only` | PASS |
| `go build ./...`; `go test -c -o <artifact> <package>` for each package | PASS, 51 packages compiled without running TestMain |
| Plain and AES-GCM `go test -race -count=1 ./cmd/... ./internal/...` | PASS, 41 package results per codec |
| `go test -race -count=1 ./internal/engine -run '^TestEngineDependencyDirection$'` | PASS after package-observation guard fix |
| All 51 `./scripts` tests in three exhaustive, disjoint `-run` groups with `-count=1 -timeout=25m` | PASS, 17 tests per group |
| Actual `go test -race -count=1 -p=1 -parallel=1 -json` events + required-event checker | PASS for all five profiles: CK014 verify, CK015 SQLite/PostgreSQL, integration plain/AES-GCM |
| `go vet ./...`; pinned `run_candidate_lint_gate.sh` with golangci-lint 2.9.0 | PASS, zero lint issues |
| `go mod tidy`; formatting; shell syntax; ShellCheck of changed audit script; smart-quotes, validation-matrix, row-writer and diff guards | PASS; no dependency changes |
| Local CLI installation, human/JSON identity, ordinary-clone clean/dirty provenance | PASS |
| Clean-commit diagnostic capture and frozen-history/scope audit | PASS; complete source attribution |

Resolved validation issues: historical tests initially inherited live v2 version
or tracked authority; all three historical fixture inventories now exclude the
current selector and the v19 diagnostic reads immutable D19 bytes. A helper
incorrectly discarded Git output and was corrected. The original whole
`go test -count=1 ./scripts` invocation timed out at its default 600 seconds;
all 51 tests were rerun successfully in isolated groups with a declared limit.
The installed linter was 2.6.2; the pinned 2.9.0 was built in a temporary tools
directory and passed. Existing-destination diagnostic refusal was retained;
clean evidence used a fresh output path.

**Not run / not claimed:** hosted CI/CodeQL, remote published-candidate install,
full platform/product/long-run/benchmark release qualification, daemon/UI,
migration/adoption/recovery-export acceptance and public release procedures.
They require publication or later owning action/profile work and are outside
this bounded local phase. No unavailable prerequisite is hidden as a pass.

## Preservation and focused review

Frozen `docs/release/v1*/` and historical receipt/probe fixtures have no diff.
`go.sum`, toolchain/language and dependency pins are unchanged. Of the Go files,
340 changed only their module import prefix. Other Go changes are version/JSON
provenance and the focused version/architecture/audit tests. Catalog schemas,
payloads, content keys, algorithms and user-data behavior are unchanged.
Compatibility effects are the intended major Go import path change, development
version identity and additive version JSON fields. This is not v2 adoption or
runtime qualification.

The separate read-only reviewer found a stale engine module-root guard that
could silently inspect no packages, and a conflicting provider prohibition.
Both were fixed: the guard now requires observing its target packages, and
provider contradiction/invariant checks prevent recurrence. Final review found
no remaining material issue; it did not substitute for runtime test evidence.

## Next bounded work

The updated [next-slice.md](next-slice.md) selects a separate Phase 1A run to
complete **all of 2.01 only**: baseline/compatibility inventory, version domains,
representative fixtures, product contract, profile statuses and result glossary.
Recommended model/effort: **GPT-6 Astra / Extra High**. Passive inspection and
integrated recovery/restoration remain subsequent bounded 2.03 work; full
recovery proof is still required before schema additions. Packaging/Linux
feasibility remains separate 2.02 work after baseline fixtures.

## Later review correction — 7 November 2026

The [Phase 0 follow-up](phase0-followup-result.md) repairs the historical-directory
and complete-literal version guards, clarifies fixture outcomes and separates
the next action from later recovery work. Its execution base and final identity
are recorded separately. The original archive, frozen-v1 anchors, commits and
execution evidence above remain unchanged. The provenance wording above is
qualified by this later review, without claiming the earlier runtime checks
were rerun.
