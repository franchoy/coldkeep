# Phase 1A / action 2.01 result — 8 November 2026

**Status: COMPLETE — action 2.01.** Product gates remain **INCOMPLETE**. This local
run implements only action 2.01, not 2.02 packaging or 2.03 passive opening,
recovery export/import or platform qualification.

## Identity and authority

Requested model/effort: GPT-6 Astra / Extra High (`gpt-6-astra`, `xhigh`). Actual
model/effort are not observable in the available session metadata. No account
settings or global configuration changed.

The actual accepted Phase 0 checkout was clean at
`eb9ddebfe7d6d2c24f7b84052b08b1facfe953cb`, tree
`100982d74c7b6c8a3e0e1a259966c5fd6ffb787c`, on `release/v2.0-phase0`.
A separate branch `release/v2.0-phase1a` and worktree
`/home/franchoy/vscode/coldkeep-worktrees/v2.0-phase1a` were created there.
No subsequent work was present to reconcile. Both the accepted Phase 0 worktree
and original main checkout are preserved. The accepted 17 focused / 532 Python
follow-up checks were entry evidence, not rerun as an entry ceremony.

Implementation checkpoint: `46cac4e72c95a7acd60dd792ef45ade9adfdca44`, tree
`02385ad48a5a6bd578daf255db20c56c0601a488`. The final identity and ordered local
commits are in external `final-identity.json`, avoiding a self-referential
commit hash in this document. The review patch starts at accepted `eb9ddeb`,
not at the original v1 historical base.

The existing `docs/release/current.json` selects exactly `phase: "2.01"`, with
`phase_status: "COMPLETE"`.
All seven active mirrors agree. Both validators report
`LOCAL_ACTION_2_01_AUTHORITY_ONLY`; Phase 0 tests remain explicitly phase 0.
Unknown/conflicting scopes, release states and tag certification still fail.
The corrected historical traversal and version-literal guards are unchanged.
Original historical base `e8a501d6edbc32cd440562065f1a851d8afdc03d`, latest
stable 1.13.19, application 2.0.0-dev.0, `/v2` module and dependency/toolchain
pins remain unchanged. Completion cannot authorize publication.

## Deliverables

- [Baseline/compatibility ledger and version domains](baseline-compatibility.md):
  archive versus Git identities, all 25 engine methods and CLI/application
  seams, schema fence, legacy/packed readers, transforms and named limits.
- [Fixed fixture manifest](../../../testdata/product-fixtures/manifest.json),
  [harness guide](../../../testdata/product-fixtures/README.md),
  [Go harness](../../../tests/productfixtures/fixtures_test.go) and
  [evidence runner](../../../scripts/run_product_fixtures.py).
- [Expected versus observed fixture table](fixture-results.md): exact reader
  identities, historical content hashes, metadata/refusal and key references.
- [Profile checklist, capture/import policy and glossary](product-contract.md):
  selected/unqualified/deferred profiles, independent result dimensions and
  five worked examples. Original action 2.01 and shared contracts are retained.
- [Next bounded 2.03 handoff](next-slice.md): inputs, outputs, refusal behavior,
  lifetime/closure evidence and recommended Astra / Extra High settings.

All 23 expectations passed against exact D19 and the current checkpoint under
`-race`: 12 restores, 9 refusals and 2 known baseline limits. Restored hashes
and selected Linux mode/mtime matched. The baseline's 1,631 original Git blobs
were unchanged before/after. Test-only overlays adapted the harness import
prefix; they did not alter any baseline production source.

The environment was Linux x86-64, Go 1.26.10, CGO and a C compiler. Schema-17
SQLite catalogs/payloads were disposable copies. The fixture key reference is
the retained public dummy key, never a user key; reports contain references
only. No PostgreSQL server was required or started for this harness. Source
catalog meaning and payload bytes were preserved after every repository case;
restore pin/unpin activity remains distinct from future passive opening.

## Validation and observations

Evidence root:
`/home/franchoy/.local/state/coldkeep-release-evidence/v2.0/phase1a-20261107`.
The directory name follows the 7 November launch bundle; work finished on
8 November in Europe/Madrid. `validation-index.json` records exact commands,
source revisions/trees or changed-file hashes, environments, exit statuses,
log paths and reused/not-run evidence. The compact ZIP includes raw logs,
fixture reports, final authority/diagnostic/preservation and SHA-256 inventory.

| Applicable check | Observed result |
|---|---|
| Focused explicit-phase authority tests | PASS, 22 tests |
| Fixture evidence-consumer negative tests | PASS, 3 tests |
| Complete Python suite for changed authority/CI evidence | PASS, 540 tests |
| Current release-state and governance validators | PASS, development authority only |
| Explicit historical-v1 route, both validators | PASS on preserved original main checkout |
| Current/baseline fixture runner, Go race detector | PASS, all 23 each; no skips |
| Four selected historical storage-reader regressions, `-race` | PASS |
| CI local audit and two focused Go audit tests (three gate mutations) | PASS after correction |
| `go build ./...`, `go vet ./...` | PASS |
| Pinned golangci-lint 2.9.0 gate | PASS, zero issues |
| Shell syntax/ShellCheck, matrix, row-writer, smart-quote guards | PASS |
| Gofmt, Markdown link inspection and `git diff --check` | PASS |
| Frozen-history/source-asset/pin/product-code preservation | PASS |

Initial fixture probes exposed three draft-expectation failures: truncation
uses a bounded-read error, PROCESSING logical files appear not found through
the restore adapter, and catalog block-format version 2 alone does not stop
restoration of encoded v1 data. The exact failures remain in
`fixtures-initial.log`; no historical payload or restored-content hash changed.
The last observation remains a KNOWN_LIMIT, separately from verifier refusal
and actual future encoded-header refusal. Backslash name normalization is the
other known limit. 2.03/2.15 must inspect compatibility explicitly; a successful
restore is not a full admission verdict.

The initial CI audit also refused the new upload because diagnostic v4 is
reserved for exactly one step; a partially applied edit was caught on the next
run. The final fixture upload uses existing ordinary v7 with its exact audit
count updated. ShellCheck's literal-variable notice received a targeted
annotation. Initial failures are retained under `ci-initial/` and
`ci-partial-edit/`; corrected audit, negative tests and ShellCheck pass.

Accepted Phase 0 runtime evidence is reused only for unchanged code: both-codec
unit race suites, PostgreSQL/SQLite CK-015 proofs, CK-014 and integration
correctness. Prior records are identified and hashed in the index. These are
not new executions. No hosted CI, product/platform release matrix, stress,
benchmark, native installation, daemon/UI, recovery package, migration,
power-loss or original-installation-loss qualification is claimed here.

## Preservation and exit

All 849 frozen v1 release files retain digest
`64fb69cb875c4bdf96c10cd759275b7232849c2f8083b0b406f11d7b18b23c7a`.
The original archive still hashes to
`84888b7e47d4e571511edc91dfbeab5705e6d6a6b308769aea703eb465d85414`.
All three retained fixture assets are byte-identical to entry. Product Go
source under `internal`/`cmd`, go.mod/go.sum, container definitions and both
Phase 0 result records are unchanged. New Go code is tests only; changes outside
documentation concern narrow authority selection and fixture/CI validation.
No schema, stored payload, key behavior, defaults or application version changed.

Remaining action 2.01 work: none. The final completion commit changes only
documentation and phase status; tested implementation bytes remain identical
to the checkpoint. Final identity, diagnostic, patch and checksum inventory
bind the delivered clean local candidate. Future product gates remain incomplete.

The next separate 2.03 unit produces passive-open and quiesced full-catalog /
referenced-closure inventory proof. Its completion alone is insufficient:
package staging/publication, constrained import and original-installation-loss
restoration must still finish 2.03 before schema additions. Separate 2.02
packaging/Linux-native feasibility is available after these fixed fixtures.
Neither implementation starts here. No push, PR, merge, tag, release,
deployment or user-data migration was performed.
