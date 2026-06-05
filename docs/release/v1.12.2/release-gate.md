# Coldkeep v1.12.2 Final Patch Release Gate

Release name: v1.12.2 - CLI Validation Follow-up
Recommended tag: `v1.12.2`
Branch: `release/v1.12.2`
Base branch: `main`
Gate date: 2026-06-05

## Commit Range

Range: `main..release/v1.12.2`

```text
f1cd349 v1.12.2 phase 4: modernize pre-release checklist
4b1872d v1.12.2 phase 4: record final patch release gate
2b04b3e v1.12.2 phase 3: add empty-value parser-path coverage
74cfcbe v1.12.2 phase 2: align search extension parser validation
0c11a11 v1.12.2 phase 1: fix v1.12.1 release status docs
a1665ee v1.12.2 phase 0: add CLI validation follow-up baseline
```

## Included Phases

- Phase 0 - Scope Lock.
- Phase 1 - v1.12.1 Release Documentation Status Cleanup.
- Phase 2 - `search --extension` Parser Alignment.
- Phase 3 - Empty-Value Parser-Path Regression Coverage.
- Superseded pre-modernization gate evidence commit.
- Phase 4 - Pre-release Checklist Modernization.
- Phase 5 - Final Patch Release Gate.

## Behavior Changes

v1.12.2 is a narrow CLI validation follow-up patch.

- v1.12.1 release documentation status now reflects that v1.12.1 was completed, merged, and
  tagged.
- `search --extension` is treated as a value-taking flag by the parser so empty and blank values
  are rejected through the real parser path.
- `search --extension .txt` remains unsupported; extension search was not implemented.
- Parser-path regression coverage now proves selected v1.12.1 empty-value validation cases through
  `parseCommandLine` before command dispatch.

No new CLI validation families were added.

## Documentation and Checklist Changes

Phase 4 modernized `PRE_RELEASE_CHECKLIST.md` before the final gate:

- Added Profile A / B / C validation paths.
- Identified Profile A as the standard pre-PR CI-parity gate.
- Added required local tools and broad runtime guidance.
- Added a local mirror for the current CI `legacy-compatibility` job.
- Added a local approximation for the current CI `cross-platform` job while clearly stating that
  GitHub Actions must still prove macOS and Windows behavior.
- Clarified CI-parity smoke versus stronger manual schema-path smoke.
- Clarified that critical coverage is informational unless enforced by `ci-required`.
- Made reusable final sign-off boxes unchecked.

## Compatibility Statement

No changes were made to:

- Search matching semantics.
- Extension matching, case normalization, or leading-dot normalization.
- JSON output behavior.
- Exit-code classes.
- Benchmark behavior.
- Snapshot tag normalization.
- Schema.
- Repository format.
- Storage format.
- Default backend.
- Engine, catalog, or storage architecture.
- Daemon, API, UI, NAS, cloud, or v1.13 work.

## Profile A Pre-PR Checklist Result

Profile A from `PRE_RELEASE_CHECKLIST.md` was run before this final release gate:

- Section 1: PostgreSQL was running and accepting connections.
- Section 2: quality-equivalent checks passed.
- Section 3: required CI matrix local equivalents passed, including correctness, stress,
  long-run, adversarial, smoke, benchmark execution, legacy compatibility, and local
  cross-platform approximation.
- Final local checks passed: `git diff --check` and `git status -sb`.

Tooling note: the CI-pinned `golangci-lint v2.6.2` was installed and used successfully during
Profile A Section 2 before `scripts/clean_test_storage.sh` removed `/tmp/coldkeep-tools`. When
reinstalled for the direct release gate under the local `go1.26.1` toolchain, it panicked with
`file requires newer Go version go1.26 (application built with go1.25)`. The local installed
`golangci-lint v2.11.3` then ran successfully with `0 issues`. CI remains authoritative for the
pinned linter on the GitHub Go 1.25.x runner.

## Validation Commands

Profile A ran:

```bash
docker compose up -d coldkeep_postgres
docker compose exec -T coldkeep_postgres pg_isready -U coldkeep
bash scripts/clean_test_storage.sh
go mod tidy
git diff --exit-code
gofmt -l $(git ls-files '*.go')
bash -n scripts/*.sh
bash scripts/check_smart_quotes.sh
shellcheck $(find scripts/ -maxdepth 1 -name '*.sh' ! -name 'critical_coverage.sh')
scripts/validate_validation_matrix.sh
bash scripts/check_versioned_row_writers.sh
golangci-lint run ./...
go vet ./...
COLDKEEP_CODEC=plain go test -race -count=1 ./cmd/... ./internal/...
COLDKEEP_CODEC=aes-gcm COLDKEEP_KEY="$COLDKEEP_KEY" go test -race -count=1 ./cmd/... ./internal/...
go test -race -count=1 ./internal/chunk/benchmark -run 'TestFastCDCBetterThanV1_SmallModifications|TestFastCDCBetterThanV1_ShiftedData|TestChunkerDeterminism|TestChunkerDeterminism_RunDatasetTwice|TestChunkCoverage|TestDefaultDatasetsDeterministicAcrossCalls'
go build ./...
scripts/audit_ci_enforcement.sh --local-only
go build -o coldkeep ./cmd/coldkeep
```

Profile A Section 3 ran the CI local equivalents for both `plain` and `aes-gcm` codecs:

```bash
go test -race -count=1 -short ./tests/integration/...
go test -race -count=1 ./tests/integration/...
COLDKEEP_LONG_RUN=1 go test -race -count=1 ./tests/integration/... -run 'TestStoreGCVerifyRestoreDeleteLoopStability|TestRandomizedLongRunLifecycleSoak|TestSnapshotRetentionChurnLongRun'
go test -race -count=1 ./tests/integration/... -run 'TestRefCountContainmentStressMatrix'
COLDKEEP_LONG_RUN=1 go test -race -count=1 ./tests/adversarial/...
go test -race -count=1 ./tests/adversarial/... -run 'TestAdversarialG14|TestAdversarialG15|TestAdversarialG16|TestAdversarialG17'
scripts/smoke.sh
```

Profile A benchmark and supplemental local gates ran:

```bash
./coldkeep benchmark run --dataset small --workers 1 --output json
./coldkeep benchmark run --dataset small --workers 4 --output json
COLDKEEP_COMPRESSION=zstd ./coldkeep benchmark run --dataset small --workers 1 --output json
COLDKEEP_COMPRESSION=zstd ./coldkeep benchmark run --dataset small --workers 4 --output json
python3 scripts/validate_regression_thresholds.py check benchmark-none-w1.json --mode uncompressed
python3 scripts/validate_regression_thresholds.py check benchmark-none-w4.json --mode uncompressed
python3 scripts/validate_regression_thresholds.py check benchmark-zstd-w1.json --mode compressed
python3 scripts/validate_regression_thresholds.py check benchmark-zstd-w4.json --mode compressed
go test -race -count=1 ./tests/integration/... -run 'TestPhase2PostMigrationStoreRestoreSnapshotRegressionIntegration'
go test ./internal/pathsafe/... -count=1
go test ./internal/storage -run "RestoreCrossPlatform|RestoreSeam" -count=1
git diff --check
git status -sb
```

Final direct release validation was run in this order:

```bash
git status -sb
git log --oneline main..release/v1.12.2
gofmt -w $(git ls-files '*.go')
gofmt -l $(git ls-files '*.go')
golangci-lint run ./...
go vet ./...
go test -count=1 ./...
go test -race -count=1 ./...
git diff --check
git status -sb
```

## Validation Results

- Profile A Section 1: PostgreSQL was running and `pg_isready` passed.
- Profile A Section 2: quality-equivalent checks passed.
- Profile A Section 3 correctness, stress, long-run, refcount, adversarial, smoke, legacy
  compatibility, and local cross-platform approximation checks passed.
- Profile A benchmark execution completed for all four combinations.
- `benchmark-none-w1` threshold check passed.
- `benchmark-none-w4` produced a local workers=4 HARD FAIL.
- `benchmark-zstd-w1` threshold check passed.
- `benchmark-zstd-w4` threshold check passed.
- Profile A final `git diff --check`: passed.
- Profile A final `git status -sb`: clean after generated benchmark artifacts were restored.
- Direct `git status -sb`: clean at gate start.
- Direct `git log --oneline main..release/v1.12.2`: six v1.12.2 commits listed.
- Direct `gofmt -w $(git ls-files '*.go')`: completed with no resulting changes.
- Direct `gofmt -l $(git ls-files '*.go')`: empty output.
- Direct pinned `golangci-lint v2.6.2`: local toolchain panic under Go 1.26.1; see tooling note.
- Direct local `golangci-lint v2.11.3`: passed with `0 issues`.
- Direct `go vet ./...`: passed.
- Direct `go test -count=1 ./...`: passed.
- Direct `go test -race -count=1 ./...`: passed.
- Direct `git diff --check`: passed.
- Direct `git status -sb`: clean after validation.

## Benchmark Local Interpretation

The only benchmark threshold failure was `benchmark-none-w4`, with 10 local workers=4 regressions
reported against tight v1.9 GitHub Actions baselines. `benchmark-none-w1`, `benchmark-zstd-w1`,
and `benchmark-zstd-w4` passed.

Per `PRE_RELEASE_CHECKLIST.md`, workers=1 failures require investigation as potential real
regressions, while workers=4 local failures may be environment variance if CI passes. This local
workers=4 failure is recorded as local variance evidence and is not treated as a release blocker
by itself.

## Residual Risks

- v1.12.2 intentionally remains narrow and does not implement extension search.
- Broader parser redesign remains deferred.
- Benchmark validation hardening remains deferred.
- Snapshot tag normalization remains deferred.
- JSON side-channel cleanup remains deferred.
- Engine/catalog/storage migrations and v1.13 architecture work remain deferred.
- GitHub Actions remains authoritative for macOS/Windows cross-platform validation and the
  CI-pinned linter environment.

## Merge Readiness Decision

Decision: ready to open a PR for final review and merge.

This decision is based on Profile A pre-PR validation, direct final release-gate validation,
documentation of local benchmark variance, and explicit compatibility/residual-risk evidence.

## PR Readiness Decision

Decision: ready to open the v1.12.2 release PR.

Open the PR only after this Phase 5 evidence commit is pushed.

## Release Tag Recommendation

Recommended tag after merge: `v1.12.2`
