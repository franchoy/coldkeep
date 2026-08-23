# Pre-release Checklist

Use this checklist before opening a correctness-sensitive PR or cutting a release tag.

Audience:

- Maintainers preparing a release or a release candidate
- Contributors validating a broad correctness-sensitive change end-to-end

If you only need day-to-day contributor setup, start with `README.md` and `CONTRIBUTING.md` instead. This document is intentionally heavier and more exhaustive.

Newcomer rule of thumb: if you are not preparing a release, a release candidate,
or a broad correctness-sensitive validation pass, this is probably not the first
document you want.

Execution model (step-by-step):

- Run sections in order. Do not mark a section complete until its "Expected"/"Confirm" checks pass.
- Capture evidence as you go (command output snippets, failing/success states, and any remediation notes).
- If a step fails, fix the issue and re-run that step before moving forward.
- For releases that include snapshot/retention scope, treat sections 15-17 as required release gates after sections 1-13.

## Validation profile decision guide

Choose the lightest profile that honestly matches the work being validated.

### Profile A - Pre-PR CI-parity gate

Use this before opening a PR. It is the standard local "full-system green"
path intended to minimize GitHub CI failures before review.

Run:

- Section 1: local PostgreSQL and CI-compatible environment setup.
- Section 2: quality-equivalent checks.
- Section 3: required CI matrix local equivalents, including Phase 18 named
  backend/storage/recovery/coordination selectors, smoke, hard benchmark
  integrity, timing advisory evaluation, legacy compatibility, and the local
  cross-platform approximation.
- Final local checks: `git diff --check` and `git status -sb`.

Profile A is green only when:

- quality checks pass;
- required CI matrix local equivalents pass;
- smoke passes;
- all four benchmark-integrity profiles pass and all four hosted-timing-style
  observations produce a valid advisory classification; historical timing
  threshold crossings alone do not fail the gate;
- `git diff --check` passes;
- the working tree is clean or only intentional committed changes remain.

Profile A is a strong local Linux validation path, but it does not replace
GitHub Actions. In particular, local Linux cannot fully prove the macOS and
Windows legs of the cross-platform job.

### Profile B - Full release-tag/manual gate

Use this before final release tagging, or when the release manager asks for
manual release evidence. It includes Profile A plus the release-tag/manual
sections:

- Section 4 when extra integration confidence is useful.
- Sections 5-11 for manual CLI/operator contract checks.
- Sections 15-17 for snapshot/retention release validation when the release
  includes snapshot/retention scope or the release manager promotes the gate.
- Section 18 for final sign-off evidence.

Record completed release evidence in a release-specific document. Do not mark
the reusable checklist template as completed.

### Profile C - Historical/special-release templates

Sections 12-14 are archived templates for historical v1.5/v1.6 release tracks.
Use them as reference material only unless the release manager explicitly
promotes a section into the active gate for a special release.

## Required local tools

Install these before claiming CI parity locally:

- Go 1.25.x, or the version required by `go.mod` / `toolchain`.
- `docker compose`.
- PostgreSQL client tools, including `psql`.
- `jq`.
- `shellcheck`.
- `golangci-lint` v2.6.2 for exact CI parity. Newer local versions may report
  findings that GitHub CI does not yet enforce.
- Python 3.
- A bash-compatible shell.

Missing local tools should be fixed before claiming Profile A CI parity. If a
tool is unavailable, record the gap and do not treat the profile as green.

## Runtime guidance

These estimates are broad and environment-dependent:

- Quality gate: often several minutes; longer on cold caches or slower CPUs.
- Full CI-parity matrix: often tens of minutes because it includes race tests,
  integration suites, smoke, adversarial tests, and benchmarks.
- Benchmark timing observations: sensitive to local CPU scheduling and virtualization,
  especially workers=4.
- Full release-tag/manual gate: can take substantially longer because it adds
  manual CLI/operator checks and optional release-specific gates.

## Checklist status interpretation

Current release-gate sections:

- Profile A pre-PR CI parity: sections 1-3 plus final local checks.
- Profile B release-tag/manual gate: Profile A plus the release-manager-selected
  manual sections.
- Profile C historical templates: sections 12-14 only when explicitly promoted.

Historical v1.9 note:

- Active v1.9 blockers are the current release-gate sections (1-11, 15-18).
- Historical template sections (12-14) are archived reference material only.
- Unchecked boxes in sections 12-14 are intentional historical state and are not v1.9 blockers unless a release manager explicitly promotes one into the active v1.9 gate.

## Release freeze policy

Before running the technical release checks below, freeze implementation scope.

Current goal: make the release boring in production: no surprises, no hidden
development paths, and every operator-facing behavior documented while
preserving deterministic restore, GC safety, snapshot correctness, and stable
CLI behavior.

Historical v1.9 positioning note, retained as context for older release
evidence:

- v1.9 keeps packed-block storage metadata for new writes.
- v1.9 adds block-level compression with store-if-smaller semantics.
- v1.9 reads v1.7/v1.8 repositories without forced rewrite.
- v1.9 default packed block target remains 1 MiB.
- v1.9 keeps mixed legacy/packed/compressed repositories as valid steady-state.
- restore determinism is preserved.
- GC safety is preserved.
- snapshot semantics are preserved.

At this point, do not add new optimizations unless they fix a release blocker.

Allowed during this gate:

- tests
- docs
- small correctness fixes
- benchmark reporting polish
- release notes
- minor cleanup

Required before final tag creation:

- release notes file for the target version exists (for v1.9: `RELEASE_NOTES_v1.9.0.md`),
- benchmark command support level is documented clearly (`coldkeep benchmark` CLI vs phase harness scripts),
- historical phase benchmark reports are marked as archived evidence, not live implementation spec.

Avoid during this gate:

- new worker behavior
- new DB indexes
- new I/O batching model
- new CLI contract changes
- new storage/schema changes

Expected:

- every PR/change in the release window is classifiable as one of the allowed categories above,
- and no release-gate step introduces net new product surface.

Suggested preflight before Step 1:

- run all commands in this checklist from repository root (`/workspaces/coldkeep` in the dev container),
- `docker compose` available locally
- `psql` available locally if you will run host-side smoke or schema checks
- `jq` available locally if you will run host-side smoke output checks
- `golangci-lint` available locally if you want full quality-job parity
- no important artifacts stored under `./storage`, `.ci-storage`, or `/tmp/coldkeep*`
- clean Python bytecode artifacts before packaging release ZIPs:

```bash
find . -type d -name '__pycache__' -prune -exec rm -rf {} +
find . -type f -name '*.pyc' -delete
```

- verify no Python bytecode artifacts remain before packaging:

```bash
find . -type d -name '__pycache__' -o -type f -name '*.pyc'
```

## Prerequisite: PostgreSQL assumptions and operator surface

Review this before starting Step 1.

Operator expectation surface for supported PostgreSQL deployments:

- Schema/bootstrap: coldkeep expects the tracked schema/migration version managed by this release. Missing PostgreSQL schema requires manual schema application or `COLDKEEP_DB_AUTO_BOOTSTRAP=true`. Existing older schemas are auto-upgraded to the required v16 schema at startup.
- Locking behavior: coldkeep expects normal PostgreSQL row/table lock semantics and transactional guarantees under default supported isolation behavior.
- Advisory locks: maintenance and coordination flows rely on PostgreSQL advisory locking primitives being available and functioning correctly.

## 1) Start PostgreSQL and set CI-compatible environment

```bash
docker compose up -d coldkeep_postgres

export COLDKEEP_TEST_DB=1
export COLDKEEP_DB_AUTO_BOOTSTRAP=true
export COLDKEEP_SCHEMA_PATH=db/schema_postgres.sql
export DB_HOST=127.0.0.1
export DB_PORT=5432
export DB_USER=coldkeep
export DB_PASSWORD=coldkeep
export DB_NAME=coldkeep
export DB_SSLMODE=disable
export COLDKEEP_KEY=0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef
export COLDKEEP_STORAGE_DIR="$PWD/.ci-storage/manual-checks"
```

`COLDKEEP_CODEC` is intentionally not exported globally here because step 3
sets it per loop iteration (`plain` then `aes-gcm`).
`COLDKEEP_KEY` is only required when codec is `aes-gcm`; it is ignored by `plain`.

If `docker compose up -d coldkeep_postgres` fails because port `5432` is already
allocated, stop or reuse the existing local PostgreSQL container before
continuing. The remaining steps assume a reachable PostgreSQL instance on the
host/port above.

Tip: prefer `docker compose exec coldkeep_postgres ...` over `docker exec <container-name> ...`
in manual steps below. Compose service addressing is stable across different
project naming schemes; hard-coded container names are not.

## 2) Run quality-equivalent checks (CI quality job parity)

Run this block from a clean working tree when possible.
If `go mod tidy && git diff --exit-code` fails while you have local edits,
that indicates uncommitted diff in your workspace, not necessarily a test failure.

```bash
bash scripts/clean_test_storage.sh

go mod tidy && git diff --exit-code

unformatted=$(gofmt -l $(git ls-files '*.go'))
if [ -n "$unformatted" ]; then
  echo "Unformatted Go files detected:"
  echo "$unformatted"
  exit 1
fi

bash -n scripts/*.sh
bash scripts/check_smart_quotes.sh
if command -v shellcheck >/dev/null 2>&1; then
  # CI excludes critical_coverage.sh (ignore_names: critical_coverage.sh in
  # ludeeus/action-shellcheck). Match that exclusion here to avoid local
  # failures that CI would not surface.
  shellcheck $(find scripts/ -maxdepth 1 -name '*.sh' ! -name 'critical_coverage.sh')
else
  echo "shellcheck not found. Install it to match CI parity (e.g., apt install shellcheck or brew install shellcheck)."
  exit 1
fi
scripts/validate_validation_matrix.sh
bash scripts/check_versioned_row_writers.sh
# CI pins golangci-lint at v2.6.2 (golangci/golangci-lint-action@v9 version: v2.6.2).
# A newer local version may surface findings that CI would not flag, causing
# false parity failures. For exact parity, install the pinned version:
#   curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh \
#     | sh -s -- -b $(go env GOPATH)/bin v2.6.2
golangci-lint run ./...
go vet ./...

COLDKEEP_CODEC=plain go test -race -count=1 ./cmd/... ./internal/...
COLDKEEP_CODEC=aes-gcm COLDKEEP_KEY="$COLDKEEP_KEY" go test -race -count=1 ./cmd/... ./internal/...
go test -race -count=1 ./internal/chunk/benchmark -run 'TestFastCDCBetterThanV1_SmallModifications|TestFastCDCBetterThanV1_ShiftedData|TestChunkerDeterminism|TestChunkerDeterminism_RunDatasetTwice|TestChunkCoverage|TestDefaultDatasetsDeterministicAcrossCalls'

go build ./...
scripts/audit_ci_enforcement.sh --local-only

# The fixture suite is valid from any branch. The real repository check is
# release-lifecycle aware: run it from the active release branch, a main
# candidate, or an annotated tag checkout. Neither command performs network calls.
python3 scripts/test_validate_release_state.py
python3 scripts/validate_release_state.py --state auto

go build -o coldkeep ./cmd/coldkeep

expected_version="1.13.13"

human_version=$(./coldkeep version)
if [ "$human_version" != "coldkeep version $expected_version" ]; then
  echo "version mismatch: human output=$human_version expected=coldkeep version $expected_version"
  exit 1
fi

json_version=$(
  ./coldkeep version --output json |
    jq -er '
      select(
        .status == "ok" and
        .command == "version" and
        (.data.version | type) == "string"
      ) |
      .data.version
    '
)

if [ "$json_version" != "$expected_version" ]; then
  echo "version mismatch: JSON version=$json_version expected=$expected_version"
  exit 1
fi
```

Expected: local quality checks match CI intent and produce no diff or lint/format failures.

Expected: the built CLI reports exactly 1.13.13 in both human and JSON modes.
A version mismatch blocks Profile A and release approval.

Note: `scripts/clean_test_storage.sh` removes `./storage`, `.ci-storage`, and
`/tmp/coldkeep*`. Do not keep one-off repro scripts or evidence you care about
under those paths while running this checklist.

Transition note for newcomers: Step 1 exports manual CLI variables used again in
steps 5-11. Step 3 intentionally unsets/overrides some of them to mirror CI.
Do not skip `unset` lines in step 3.

## 3) Run full required CI matrix locally (all gate jobs, both codecs)

```bash
unset COLDKEEP_STORAGE_DIR

for codec in plain aes-gcm; do
  echo "=== Codec: ${codec} ==="
  export COLDKEEP_CODEC="$codec"

  # integration-correctness
  go test -race -count=1 -short ./tests/integration/...

  # integration-stress
  go test -race -count=1 ./tests/integration/...

  # integration-long-run
  COLDKEEP_LONG_RUN=1 go test -race -count=1 ./tests/integration/... -run 'TestStoreGCVerifyRestoreDeleteLoopStability|TestRandomizedLongRunLifecycleSoak|TestSnapshotRetentionChurnLongRun'

  # integration-refcount-containment (v1.8 Option A hold gate: 25-iteration matrix by default)
  go test -race -count=1 ./tests/integration/... -run 'TestRefCountContainmentStressMatrix'

  # For v1.8 release hold: 1000-iteration stress matrix (validates chunk refcount repair under extreme load)
  # COLDKEEP_REFCOUNT_STRESS_ITERS=1000 go test -race -count=1 ./tests/integration/... -run 'TestRefCountContainmentStressMatrix'

  # adversarial
  unset COLDKEEP_STORAGE_DIR
  COLDKEEP_LONG_RUN=1 go test -race -count=1 ./tests/adversarial/...
  go test -race -count=1 ./tests/adversarial/... -run 'TestAdversarialG14|TestAdversarialG15|TestAdversarialG16|TestAdversarialG17'

  # smoke
  COLDKEEP_SMOKE_RESET_DB=1 \
  COLDKEEP_STORAGE_DIR="$PWD/.ci-storage/${codec}" \
  COLDKEEP_SMOKE_SCHEMA_MESSAGE_GATE=1 \
  PATH="$PWD:$PATH" \
  scripts/smoke.sh
done

# Step 3 loop leaves COLDKEEP_CODEC set to the last codec (aes-gcm).
# Reset it before the benchmark block and manual CLI checks in later steps.
unset COLDKEEP_CODEC

# Phase 18 named execution proof. Every selected test below must report PASS;
# a matching SKIP is a release-gate failure even when `go test` exits zero.
COLDKEEP_CODEC=plain go test -v -race -count=1 ./internal/db \
  -run '^TestMutationRowsAffectedContractAcrossBackends/postgres$'

for codec in plain aes-gcm; do
  COLDKEEP_CODEC="$codec" go test -v -race -count=1 ./tests/integration/... \
    -run '^TestRoundTripStoreRestore$'
done

COLDKEEP_CODEC=plain go test -v -race -count=1 ./tests/integration/... \
  -run '^(TestRemoveWithSharedChunksRefCount|TestStartupRecoveryResyncsPreexistingQuarantinedOrphanConflictState)$'

for codec in plain aes-gcm; do
  COLDKEEP_CODEC="$codec" COLDKEEP_LONG_RUN=1 go test -v -race -count=1 \
    ./tests/adversarial/... \
    -run '^(TestAdversarialG6IndependentProcessRepositoryContention|TestAdversarialG6KilledLeaseHolderReleasesRepository|TestAdversarialG6LiveGCExcludesIndependentStoreProcess)$'
done

COLDKEEP_CODEC=plain go test -v -race -count=1 ./internal/maintenance \
  -run '^(TestGCAdvisoryLockUsesDedicatedSessionAndReleases|TestRunGCReleasesAdvisoryLockAfterOperationFailure|TestRunGCAdvisoryCleanupFailureReturnsErrorAndDiscardsSession|TestRunGCLiveRefusesSingleConnectionPool)$'

# benchmark-integrity and benchmark-timing-advisory (CI-equivalent policy)
# CI always sets COLDKEEP_CODEC=aes-gcm and applies the fixed lock-retry
# settings below. Candidate integrity is hard-required. Historical timing is
# informational when the observation and evaluator are valid; threshold
# crossings must remain visible but do not fail the gate.
export COLDKEEP_CODEC=aes-gcm
export COLDKEEP_CONTAINER_LOCK_RETRY_ATTEMPTS=12
export COLDKEEP_CONTAINER_LOCK_RETRY_BASE_WAIT_MS=15
export COLDKEEP_CONTAINER_LOCK_RETRY_MAX_WAIT_MS=900

candidate_sha=$(git rev-parse HEAD)
go_version=$(go version)
postgres_version=$(psql --version)
postgres_digest=sha256:33f923b05f64ca54ac4401c01126a6b92afe839a0aa0a52bc5aeb5cc958e5f20

while read -r profile compression workers dataset; do
  output_dir="benchmark-integrity-evidence/${profile}/integrity"
  test ! -e "$output_dir"
  COLDKEEP_COMPRESSION="$compression" \
    python3 scripts/benchmark_gate.py integrity \
      --binary ./coldkeep \
      --output-dir "$output_dir" \
      --compression "$compression" \
      --workers "$workers" \
      --dataset "$dataset" \
      --command-timeout-seconds 600 \
      --source-commit "$candidate_sha" \
      --go-version "$go_version" \
      --postgres-version "$postgres_version" \
      --database-image-digest "$postgres_digest"
  (cd "$output_dir" && sha256sum --check checksums.sha256)
done <<'EOF'
none-w1 none 1 ci-paired-w1-v2
none-w4 none 4 ci-paired-w4-v2
zstd-w1 zstd 1 ci-paired-w1-v2
zstd-w4 zstd 4 ci-paired-w4-v2
EOF

while read -r profile compression workers mode baseline; do
  evidence_dir="benchmark-timing-evidence/${profile}"
  mkdir -p "$evidence_dir"
  COLDKEEP_COMPRESSION="$compression" \
    ./coldkeep benchmark run \
      --dataset small \
      --workers "$workers" \
      --repeat 1 \
      --output json \
      | tee "${evidence_dir}/benchmark.json"

  set +e
  python3 scripts/validate_regression_thresholds.py check \
    "${evidence_dir}/benchmark.json" \
    --baseline "benchmarks/v1.9/baselines/${baseline}" \
    --mode "$mode" \
    --policy hosted-advisory \
    --json-report "${evidence_dir}/timing-advisory.json"
  comparator_exit=$?
  set -e

  test -s "${evidence_dir}/timing-advisory.json"
  python3 scripts/validate_regression_thresholds.py verify-advisory-exit \
    --report "${evidence_dir}/timing-advisory.json" \
    --observed-exit-code "$comparator_exit"
  case "$comparator_exit" in
    0|10|11|12) ;;
    *) echo "invalid timing-advisory exit: $comparator_exit" >&2; exit 2 ;;
  esac
  (
    cd "$evidence_dir"
    actual_inventory=$(find . -maxdepth 1 -type f ! -name checksums.sha256 -printf '%f\n' | sort)
    test "$actual_inventory" = $'benchmark.json\ntiming-advisory.json'
    sha256sum benchmark.json timing-advisory.json > checksums.sha256
    sha256sum --check checksums.sha256
  )
done <<'EOF'
none-w1 none 1 uncompressed benchmark-baseline-v1.9-packed-aes-gcm-none-small-w1-r1.json
none-w4 none 4 uncompressed benchmark-baseline-v1.9-packed-aes-gcm-none-small-w4-r1.json
zstd-w1 zstd 1 compressed benchmark-baseline-v1.9-packed-aes-gcm-zstd-small-w1-r1.json
zstd-w4 zstd 4 compressed benchmark-baseline-v1.9-packed-aes-gcm-zstd-small-w4-r1.json
EOF

unset COLDKEEP_CODEC COLDKEEP_COMPRESSION COLDKEEP_CONTAINER_LOCK_RETRY_ATTEMPTS \
      COLDKEEP_CONTAINER_LOCK_RETRY_BASE_WAIT_MS COLDKEEP_CONTAINER_LOCK_RETRY_MAX_WAIT_MS
```

Run the current `legacy-compatibility` job locally. This mirrors the CI job's
plain-codec PostgreSQL assumptions:

```bash
export COLDKEEP_TEST_DB=1
export COLDKEEP_DB_AUTO_BOOTSTRAP=true
export COLDKEEP_CODEC=plain
export COLDKEEP_KEY=0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef
export DB_HOST=127.0.0.1
export DB_PORT=5432
export DB_USER=coldkeep
export DB_PASSWORD=coldkeep
export DB_NAME=coldkeep
export DB_SSLMODE=disable

go test -race -count=1 ./tests/integration/... -run 'TestPhase2PostMigrationStoreRestoreSnapshotRegressionIntegration'
```

Run the local approximation for the current `cross-platform` job:

```bash
go test ./internal/pathsafe/... -count=1
go test ./internal/storage -run "RestoreCrossPlatform|RestoreSeam" -count=1
```

This catches the same package and test families locally. A local failure is a
blocker for the local gate, but a successful Linux run does not replace the
GitHub Actions macOS and Windows jobs. Full OS-specific path behavior is only
proved by the GitHub `cross-platform` matrix.

Important — local regression check interpretation:
The v1.9 baselines were generated on dedicated GitHub Actions runners (`ubuntu-latest`).
Dev container and developer hardware environments introduce scheduling variance,
especially under workers=4 where the script thresholds are tight (3–5% for
uncompressed mode). Workers=1 results are generally stable locally; workers=4
results will often produce warnings or HARD FAILs on under-powered or virtualized
hardware even when the code itself is correct.

Triage rule:

- `workers=1` failures: investigate as potential real regressions.
- `workers=4` failures: compare against CI results. If CI passes, treat local
  failure as environment variance (not a code defect) and proceed.
- CI is the authoritative regression gate. A local w4 HARD FAIL is not a
  release blocker on its own.
- Cross-platform local approximation failures are blockers locally, but a
  successful local Linux run does not replace GitHub macOS/Windows validation.

Why `unset COLDKEEP_STORAGE_DIR` first: step 1 exports a manual-check storage path
for later CLI validation. Leaving that variable set during integration/adversarial
test runs can force unrelated tests onto a shared storage directory and produce
false failures that do not reflect CI behavior.

Smoke parity note: the smoke block above mirrors CI as closely as practical. It
does not set `COLDKEEP_SCHEMA_PATH`, because the current CI smoke job relies on
the default schema path plus `COLDKEEP_SMOKE_RESET_DB=1`,
`COLDKEEP_SMOKE_SCHEMA_MESSAGE_GATE=1`, and a per-codec storage directory.

Stronger manual schema-path smoke remains useful for release-tag/manual
validation when you specifically want to exercise explicit schema-path
operator setup:

```bash
COLDKEEP_SMOKE_RESET_DB=1 \
COLDKEEP_SCHEMA_PATH=db/schema_postgres.sql \
COLDKEEP_STORAGE_DIR="$PWD/.ci-storage/manual-schema-smoke" \
COLDKEEP_SMOKE_SCHEMA_MESSAGE_GATE=1 \
PATH="$PWD:$PATH" \
scripts/smoke.sh
```

Expected: this mirrors the current `ci-required` upstream jobs (`quality`,
`correctness-matrix`, `integration-stress`, `integration-long-run`,
`adversarial`, `smoke`, `legacy-compatibility`, `benchmark-integrity`,
`benchmark-timing-advisory`, and `cross-platform`) across their documented
codec/profile matrices. The local cross-platform commands are an approximation;
GitHub Actions must still prove native macOS and Windows runtime.

Generate the critical coverage report (mirrors the CI `critical-coverage-report` job;
informational, not enforced by `ci-required` in the current workflow, but useful
for release review):

```bash
mkdir -p artifacts
scripts/critical_coverage.sh --report --csv-output artifacts/critical-coverage-report.csv
```

Expected: report generates without error. Review the CSV for any Tier 1 packages
with zero or near-zero coverage before proceeding to tag.

For the snapshot contract gate, run the focused integration suite after the matrix loop:

```bash
scripts/run_snapshot_release_gate.sh --count 1
```

For the Phase 7 released-v1.7 compatibility proof, run the strict gate below
only when the release manager marks legacy binary compatibility as relevant for
the release-tag gate. It must use a real released v1.7 coldkeep binary, not a
local rebuild.

```bash
COLDKEEP_V17_BIN=/absolute/path/to/released/coldkeep-v1.7 \
scripts/run_phase7_v17_binary_gate.sh --count 1
```

Expected: gate fails immediately if `COLDKEEP_V17_BIN` is missing/non-executable,
and passes only when `TestPhase7BuildFixtureWithActualV17BinaryIntegration`
executes successfully against the released v1.7 binary.

After step 3, confirm `COLDKEEP_CODEC` is unset before manual CLI checks below.
The benchmark block above unsets it explicitly; verify with `echo $COLDKEEP_CODEC`.

## 4) Run integration umbrella suite (optional extra confidence, not a release gate)

This step is intentionally non-blocking for release sign-off.
Use it to catch broader regressions outside the required CI-equivalent gate set in steps 2-3.

```bash
unset COLDKEEP_STORAGE_DIR
export COLDKEEP_CODEC=plain

go test -p 1 ./tests/... -count=1 -v -timeout 20m
```

Run this only while the PostgreSQL service from step 1 is still reachable.
The `-p 1` package-level serialization avoids cross-package interference when
multiple test packages share the same PostgreSQL instance.

New maintainer note: if this step fails while steps 2-3 passed, treat it as extra investigation work, not an automatic release blocker. The required release gate remains the CI-parity flow above.

If step 4 fails and steps 2-3 passed, capture the failure as investigation work and continue release gating from step 5. If steps 2-3 also failed, fix and re-run steps 2-3 first.

## 5) Run doctor

Before steps 5-11, confirm your local CLI environment points at storage that
matches the database state you want to inspect. Step 2 deletes
`$PWD/.ci-storage/manual-checks`, and steps 3-4 mutate the shared `coldkeep`
database. If you continue from those steps without resetting `DB_NAME` and
`COLDKEEP_STORAGE_DIR`, doctor/stats/verify may legitimately report missing
containers from an earlier storage path rather than a product defect.

Step 5A (recommended newcomer-safe reset before steps 5-11):

```bash
export DB_NAME=coldkeep_manual
export COLDKEEP_STORAGE_DIR="$PWD/.ci-storage/manual-checks"
rm -rf "$COLDKEEP_STORAGE_DIR"
mkdir -p "$COLDKEEP_STORAGE_DIR"
docker compose exec -T coldkeep_postgres psql -U coldkeep -d postgres -c "DROP DATABASE IF EXISTS coldkeep_manual;"
docker compose exec -T coldkeep_postgres psql -U coldkeep -d postgres -c "CREATE DATABASE coldkeep_manual;"

# Bootstrap the fresh manual-check database once.
export COLDKEEP_DB_AUTO_BOOTSTRAP=true
./coldkeep stats >/dev/null
```

Use this reset whenever you want steps 5-11 to validate the CLI against a fresh,
known-good manual sandbox instead of the DB/storage state left behind by the CI-parity loop.

Step 5B (run doctor):

```bash
unset COLDKEEP_CODEC
./coldkeep doctor
./coldkeep doctor --output json
```

Expected: both succeed and JSON output is machine-readable.

## 6) Validate guarantee matrix

```bash
scripts/validate_validation_matrix.sh
```

Expected: required v1.0 core guarantee rows (G1-G8), post-v1.0 extension rows (G9+), and exit criteria are present in `VALIDATION_MATRIX.md`.

## 7) Test bootstrap on and off

Bootstrap ON (clean schema bootstrap path):

```bash
unset COLDKEEP_CODEC
export DB_NAME=coldkeep_bootstrap_on_probe
docker compose exec -T coldkeep_postgres psql -U coldkeep -d postgres -c "DROP DATABASE IF EXISTS coldkeep_bootstrap_on_probe;"
docker compose exec -T coldkeep_postgres psql -U coldkeep -d postgres -c "CREATE DATABASE coldkeep_bootstrap_on_probe;"
export COLDKEEP_DB_AUTO_BOOTSTRAP=true
./coldkeep stats
```

Bootstrap OFF (fail-fast when schema is missing):

```bash
export DB_NAME=coldkeep_bootstrap_off_probe
docker compose exec -T coldkeep_postgres psql -U coldkeep -d postgres -c "DROP DATABASE IF EXISTS coldkeep_bootstrap_off_probe;"
docker compose exec -T coldkeep_postgres psql -U coldkeep -d postgres -c "CREATE DATABASE coldkeep_bootstrap_off_probe;"
unset COLDKEEP_DB_AUTO_BOOTSTRAP
# Point to a fresh DB without schema and confirm command fails fast.
./coldkeep stats
```

Expected: bootstrap on creates/validates schema path successfully; bootstrap off fails fast on missing schema.
Expected bootstrap-off failure shape: non-zero exit plus missing-schema diagnostics (for example `schema_version`/relation-not-found style errors from PostgreSQL).

## 8) Test clean install path

Warning: destructive operation ahead.
`docker compose down -v` removes PostgreSQL volumes and deletes data for all
databases in this compose project, including manual sandboxes created in
earlier steps.

From a clean machine/container flow:

```bash
docker compose down -v
docker compose up -d coldkeep_postgres
docker compose build
docker compose run --rm coldkeep stats
docker compose run --rm coldkeep doctor
```

Expected: no manual local state is required beyond documented setup, and basic commands succeed.

If another local PostgreSQL container already binds host port `5432`, this step
will fail until that container is stopped or reconfigured.

Important: `docker compose down -v` destroys the PostgreSQL volume used by any
earlier manual sandbox database (for example `coldkeep_manual`). After Step 8,
rerun the newcomer-safe reset block from Step 5 before continuing with Steps 9-11.

## 9) Verify CLI contract stability

Run core command paths in JSON mode and validate both success and failure envelopes.

```bash
unset COLDKEEP_CODEC
./coldkeep doctor --output json
./coldkeep verify system --standard --output json
./coldkeep verify system --invalid-level --output json
```

Confirm:

- Success output keeps the expected top-level envelope fields (`status`, `command`) and command-specific data fields
- Error output keeps the expected generic error envelope shape (`error_class`, `exit_code`, `message`)
- Exit codes remain stable per v1.0 contract (`0` success, `2` usage, `1` general, `3` verify, `4` recovery)

Expected: no drift in CLI JSON structure, error classification, or frozen exit-code mapping.

## 10) Verify batch CLI contract stability (v1.1)

These checks validate G9 (interface correctness guarantee).

Run targeted tests that lock the primary batch parser/preparation path, execution/reporting path, and integration behavior:

```bash
go test ./cmd/coldkeep -run 'TestPrintBatchHumanReportSymbolsAndAlignment|TestPrintBatchHumanReportDryRunPlannedNoIcon|TestEmitBatchCommandReportJSONSchema|TestRunRemoveCommandAllInvalidTargetsEmitsBatchJSONReport|TestRunRestoreCommandAllInvalidTargetsEmitsBatchJSONReport|TestBatchFailureExitCodeClassification|TestClassifyExitCodeNoValidFileIDsIsUsage'
go test ./internal/batch -run 'TestLoadRawTargets|TestPrepareTargetsPreservesInputOrder|TestHasExecutableTargets|TestExecutePreparedPreservesInputOrderAndFailFast|TestExecutePreparedFailFastStopsOnlyOnExecutionFailure'
go test ./tests/integration -run TestBatchFlagsEndToEnd
```

Optional transitional API guardrails (legacy-facing, keep while transition remains supported):

```bash
go test ./internal/batch -run 'TestResolveTargets|TestDeduplicateTargets'
```

Manual spot-checks (text mode):

```bash
./coldkeep restore 12 ./out --dry-run
./coldkeep remove 12 999 13
# --fail-fast behavior is meaningful only when multiple IDs/targets are present.
```

Confirm:

- Human symbols remain stable: `✔` success, `✖` failed, `↷` skipped, no icon for planned dry-run rows
- ID column remains aligned (`id=%-6d` style)
- JSON batch envelope remains `status + command + dry_run + summary + results`
- Failed item JSON uses `error` field (not `message`)
- `--fail-fast` stops further execution but still emits partial report
- Empty effective ID set returns `no valid file IDs after parsing input` with usage exit code `2`
- Restore overwrite default is safe (requires `--overwrite` to replace files)

## 11) Verify v1.2 physical-file contract (new in v1.2)

These checks validate G10–G13 (physical graph audit, audited GC root, invariant taxonomy, batch maintenance semantics).

Run targeted physical-graph and repair integration tests:

```bash
go test ./tests/integration -run 'TestRepairThenVerifyThenGCSmoke|TestBatchFlagsEndToEnd'
```

Manual spot-checks against a populated DB (run after step 1 and step 3):

```bash
export COLDKEEP_CODEC=plain

# store two files and capture one stored_path from JSON output
hello_json=$(./coldkeep store samples/hello.txt --output json)
stored_path=$(printf '%s\n' "$hello_json" | jq -r '.data.stored_path')
./coldkeep store samples/lorem.txt --output json

# confirm stored_path is present in the store payload
printf '%s\n' "$hello_json" | jq -r '.data.stored_path'

# verify system: must include physical graph audit on success
./coldkeep verify system --standard --output json

# repair ref-counts: must report updated_logical_files
./coldkeep repair ref-counts --output json

# corrupt logical_file.ref_count and confirm verify detects it
# (manual DB update + verify — covers GC_REFUSED_INTEGRITY and PHYSICAL_GRAPH_REFCOUNT_MISMATCH)
first_file_id=$(docker compose exec -T coldkeep_postgres psql -U coldkeep -d "$DB_NAME" -At -c "SELECT id FROM logical_file ORDER BY id LIMIT 1")
docker compose exec -T coldkeep_postgres psql -U coldkeep -d "$DB_NAME" -c "UPDATE logical_file SET ref_count = ref_count + 1 WHERE id = ${first_file_id};"
./coldkeep gc --output json
./coldkeep verify system --standard --output json

# repair the intentional drift and confirm verify/GC recover
./coldkeep repair ref-counts --output json
./coldkeep verify system --standard --output json
./coldkeep gc --dry-run --output json

# stored-path remove: confirm remaining_ref_count in JSON output
./coldkeep remove --stored-path "$stored_path" --output json

# restore-by-file-id remains the supported restore contract surface.
# (restore-by-stored-path is not currently part of the CLI contract.)

# confirm repair ref-counts --batch executes and emits per-item results
./coldkeep repair ref-counts --batch --output json

# repair chunk-live-ref-counts: must report updated_chunks and scanned_chunks
./coldkeep repair chunk-live-ref-counts --output json

# confirm repair chunk-live-ref-counts --batch executes and emits per-item results
./coldkeep repair chunk-live-ref-counts --batch --output json
```

Confirm:

- `store --output json` contains `stored_path` field in `data`
- `verify system --standard --output json` succeeds with no `invariant_code` in payload
- `repair ref-counts --output json` success payload contains `updated_logical_files` and `scanned_logical_files`
- `repair chunk-live-ref-counts --output json` success payload contains `updated_chunks` and `scanned_chunks` (v1.8 new)
- `remove --stored-path --output json` success payload contains `remaining_ref_count`
- After stored-path removal, `verify system --standard --output json` still passes when graph invariants are healthy
- `repair ref-counts --batch --output json` emits `execution_mode` field and per-item results array
- `repair chunk-live-ref-counts --batch --output json` emits `execution_mode` field and per-item results array (v1.8 new)
- GC correctly refuses when ref_count drift is present: `error_class=GENERAL`, `invariant_code=GC_REFUSED_INTEGRITY`
- `repair ref-counts` unblocks subsequent GC and verify
- `repair chunk-live-ref-counts` unblocks GC and verify when chunk.live_ref_count mismatch is the blocker (v1.8 new)
- `remove --stored-path` with `--dry-run` is intentionally rejected today (usage exit code `2`); this is deferred by design

## Historical Template Sections (v1.5/v1.6)

Sections 12-14 are retained as historical release templates for prior release
tracks (v1.5/v1.6). Unchecked boxes in these sections are intentional and do
not represent unfinished blockers for the current v1.9 release.

Historical status marker:

- Sections 12-14 are archived reference templates only.
- They are explicitly non-gating for v1.9 final sign-off.
- Keep checklist boxes unchanged in these sections to preserve historical parity.

For v1.9 final tagging, use the active release-gate flow in earlier sections
plus the snapshot sign-off sections that follow.

## 12) Historical Template (Archived, Non-gating) - v1.5 CDC / chunker-evolution contract

Use this section for releases that include chunker-evolution behavior, default
chunker policy changes, or compatibility-contract updates.

### A. Schema / migration

- [ ] v1.4.1 -> v1.5 migration succeeds
- [ ] Historical `logical_file` rows are backfilled with `v1-simple-rolling`
- [ ] Historical `chunk` rows are backfilled with `v1-simple-rolling`
- [ ] Upgraded repositories preserve prior default chunker (`v1-simple-rolling` unless explicitly changed)
- [ ] Fresh v1.5 repositories initialize default chunker to `v2-fastcdc`

Suggested evidence commands:

```bash
go test ./internal/db -run 'TestRunMigrationsSucceedsOnSQLiteInMemory|TestRunMigrationsPreservesExistingRepositoryDefaultChunker|TestRunMigrationsBackfillsChunkerVersionForLegacyLogicalFileAndChunkRows|TestRunMigrationsMigratesLegacySnapshotV7ToV8WithoutDataLoss' -count=1
```

### B. Store / restore compatibility

- [ ] v1 store works
- [ ] v2 store works
- [ ] Mixed-version repository store works
- [ ] Restore is byte-identical for v1-written files
- [ ] Restore is byte-identical for v2-written files
- [ ] Restore behavior does not depend on active write chunker

Suggested evidence commands:

```bash
go test ./internal/storage -run 'TestV2ChunkerStoreSucceeds|TestV2ChunkerRestoreByteIdentical|TestStoreWithFreshDefaultV2PersistsLogicalVersion|TestStoreAfterSwitchToV2PersistsLogicalVersion|TestRestoreIgnoresConfiguredRuntimeChunker' -count=1
go test ./tests/integration -run 'TestReadPathRestoreAfterMigrationIntegration|TestReadPathSnapshotRestoreAfterMigrationIntegration' -count=1
```

### C. Snapshot behavior across versions

- [ ] Snapshot create works
- [ ] Snapshot restore works
- [ ] Snapshot diff works
- [ ] Snapshot delete works
- [ ] GC after snapshot delete remains safe
- [ ] Snapshot lifecycle behaves correctly in mixed-version repositories

Suggested evidence commands:

```bash
go test ./internal/snapshot -run 'TestCreateSnapshotFullCopiesAllPhysicalFiles|TestRestoreSnapshotCompatibleWithVersionedLogicalMetadata|TestDiffSnapshotsSummarySQLCountsAddedRemovedModified|TestDeleteSnapshotRemovesSnapshotRowsOnly' -count=1
go test ./tests/integration -run 'TestSnapshotCreateLifecycleIntegration|TestSnapshotCrossFeatureInteractionIntegration|TestPhase7SnapshotRetentionLifecycleCLIIntegration' -count=1
```

### D. Dedup / chunk identity semantics

- [ ] Chunk lookup remains content-based
- [ ] `chunker_version` is not part of dedup identity
- [ ] Reused chunks are not overwritten during cross-version reuse
- [ ] No duplicate chunks are created solely because chunker versions differ

Suggested evidence commands:

```bash
go test ./internal/storage -run 'TestCrossVersionChunkReuseIsAllowed|TestCrossVersionDedupCompatibility|TestStoreFileReusedChunkAllowsCrossVersionReuse|TestStoreAllowsCrossVersionChunkReuseWithoutOverwritingOriginMetadata' -count=1
```

### E. Config behavior

- [ ] `config get default-chunker` works
- [ ] `config set default-chunker v1-simple-rolling` works
- [ ] `config set default-chunker v2-fastcdc` works
- [ ] Invalid chunker values are rejected with usage-class error
- [ ] Default chunker config changes affect only new writes

Suggested evidence commands:

```bash
go test ./cmd/coldkeep -run 'TestRunConfigCommandSetAndGetDefaultChunker|TestRunConfigCommandSetRejectsUnknownVersion|TestRunConfigCommandSetDoesNotModifyExistingData|TestPrintHelpConfigDefaultChunkerSafetyNote' -count=1
go test ./internal/storage -run 'TestGetDefaultChunkerVersionFallsBackToV1WhenUnset|TestSetDefaultChunkerVersionRoundTrip|TestSetDefaultChunkerVersionRejectsUnregisteredVersion' -count=1
```

### F. Observability

- [ ] `stats` reports active write chunker
- [ ] `stats` reports chunk counts by chunker version
- [ ] `stats` reports chunk bytes by chunker version
- [ ] `stats` reports logical file counts by chunker version
- [ ] Mixed-version repositories are clearly represented in stats output

Suggested evidence commands:

```bash
go test ./internal/maintenance -run 'TestRunStatsResultIncludesChunkCountsByVersion|TestRunStatsResultPureV1RepositoryReportsOnlyV1|TestRunStatsResultPureV2RepositoryReportsOnlyV2|TestRunStatsResultMixedRepositoryReportsBothVersions|TestRunStatsResultVersionTotalsMatchDatabaseReality' -count=1
go test ./cmd/coldkeep -run 'TestRunStatsCommandJSONIncludesSnapshotRetention|TestStatsCommandHuman|TestStatsCommandHelpIncludesJSONTraceAndDeterminism' -count=1
```

### G. Benchmark and determinism validation

- [ ] v1 chunker baseline behavior remains stable
- [ ] v2 deterministic behavior tests pass
- [ ] Chunk coverage tests pass
- [ ] Shifted-data benchmark assertions pass
- [ ] Benchmark validations pass repeatedly (non-flaky)

Suggested evidence commands:

```bash
go test ./internal/chunk/benchmark -run 'TestFastCDCBetterThanV1_SmallModifications|TestFastCDCBetterThanV1_ShiftedData|TestChunkerDeterminism|TestChunkerDeterminism_RunDatasetTwice|TestChunkCoverage' -count=3
go test ./internal/chunk -run 'TestBothChunkersDeterministic|TestBothChunkersReconstructFullCoverage|TestV1OutputUnchanged' -count=1
go test ./internal/chunk/fastcdc -run 'TestDeterministicChunkBoundariesAndData' -count=1
```

## 13) Historical Template (Archived, Non-gating) - v1.6 observability / simulation contract

Release-ready definition:

v1.6 is ready when Coldkeep can explain repository state, inspect storage relationships,
and exactly simulate GC impact through deterministic, read-only commands, with clean
human output and stable JSON output.

### A. Repository-state explanation (`stats`)

- [ ] `coldkeep stats` explains repository state in human-readable form
- [ ] `coldkeep stats --json` emits stable tooling-oriented JSON
- [ ] Container detail remains opt-in via `--containers`
- [ ] Output remains deterministic for identical repository state and flags

Suggested evidence commands:

```bash
go test ./internal/observability -run 'TestStatsIncludesRepositoryAndChunkMetrics|TestStatsContainersOptional|TestStatsDeterministicOrdering|TestStatsResultCarriesChunkerBreakdown' -count=1
go test ./cmd/coldkeep -run 'TestRunStatsCommandJSONContract|TestRunStatsCommandJSONIncludesSnapshotRetention|TestStatsCommandHuman|TestStatsCommandHelpIncludesJSONTraceAndDeterminism' -count=1
```

### B. Storage-relationship inspection (`inspect`)

- [ ] `coldkeep inspect <entity> <id>` supports `file`, `logical-file`, `snapshot`, `chunk`, and `container`
- [ ] `--relations`, `--reverse`, and `--deep` behavior is documented and tested
- [ ] `--limit` bounds deep traversal output
- [ ] Human and JSON output remain deterministic for identical inputs

Suggested evidence commands:

```bash
go test ./internal/observability -run 'TestInspectLogicalFileIncludesChunkRelations|TestInspectChunkIncludesIncomingFileReferences|TestInspectSnapshotIncludesRetainedFileCounts|TestInspectDeterministicOrdering' -count=1
go test ./cmd/coldkeep -run 'TestRunInspectCommandJSONContractByEntity|TestRunInspectCommandLogicalFileAliasRoutesToEntityFile|TestRunInspectCommandRejectsInvalidUsage|TestRunInspectCommandHelpIncludesJSONTraceAndDeterminism' -count=1
```

### C. Exact GC simulation (`simulate gc`)

- [ ] `coldkeep simulate gc` is read-only
- [ ] `coldkeep simulate gc --delete-snapshot <id>` reflects post-delete reclaimability exactly
- [ ] `coldkeep simulate gc --containers` reports per-container detail when requested
- [ ] Simulation matches GC reclaimability decisions under the same integrity gates

Suggested evidence commands:

```bash
go test ./internal/observability -run 'TestSimulateGCMatchesBuildPlan|TestSimulateGCDeleteSnapshotAffectsReclaimability|TestSimulateGCDeterministicOrdering|TestSimulateGCDoesNotMutateState' -count=1
go test ./cmd/coldkeep -run 'TestRunSimulateGCCommandJSONContract|TestRunSimulateGCCommandTextMatchesGolden|TestRunSimulateGCCommandRejectsInvalidUsage|TestPrintSimulateGCHelpIncludesReadOnlyGuarantee' -count=1
```

### D. Output-channel and contract checks

- [ ] `--json` remains suitable for automation
- [ ] `--trace` and `--trace-json` emit diagnostics to stderr only
- [ ] Human output is understandable and JSON output keeps stable envelope structure
- [ ] `meta.version` is treated as the CLI JSON contract version (additive fields remain compatible without version bump; bump only on breaking JSON contract changes)
- [ ] Observability commands perform zero repository mutations

### E. Documentation and release artifacts

- [ ] `README.md` documents `stats`, `inspect`, and `simulate gc` observability surfaces
- [ ] `README.md` includes JSON/trace contract guidance (`--json`, `--trace`, `--trace-json`)
- [ ] `README.md` explicitly states read-only / non-mutation guarantees for observability commands
- [ ] `README.md` warns that deep inspect output can be large and recommends `--limit`
- [ ] `ARCHITECTURE.md` and `COMPATIBILITY.md` remain aligned with current behavior
- [ ] Release notes are drafted and aligned with behavior

Suggested quick checks:

```bash
rg -n 'coldkeep stats|coldkeep inspect <entity> <id>|coldkeep simulate gc|--trace-json|read-only|exact simulation|does not mutate' README.md
rg -n 'deep inspect output can be large|--deep --limit N|JSON output is intended for tooling' README.md
rg -n 'Release highlights \(1\.6\.0\)|observability|simulate gc|trace-json' CHANGELOG.md
```

### F. Final CI commands (historical template note)

For this historical v1.6 template, prefer the CI-parity gate flow already defined in sections 2-3 for modern release validation. Use the commands below only as additional exploratory reruns, not as current release blockers.

- [ ] `go test ./...` (exploratory historical sweep)
- [ ] `go test -race ./...` (exploratory historical sweep)
- [ ] `go vet ./...` passes
- [ ] Integration suite passes (`go test ./tests/integration/...`)

## 14) Historical Template (Archived, Non-gating) - Sign-off

- [ ] Quality parity checks passed
- [ ] Full local CI matrix simulation passed (both codecs)
- [ ] Smoke passed
- [ ] Integration suite passed
- [ ] v1.6 observability / simulation checklist passed
- Note: Step 4 integration umbrella suite is optional (non-gating) and was triaged separately.

## 15) Snapshot sign-off checklist (Phases 1-7)

Use this as the final snapshot gate before tagging a release.

This is a Profile B release-tag/manual gate. It is required for releases or
changes that affect snapshot/retention behavior, and optional for ordinary
pre-PR Profile A validation unless the release manager promotes it.

### Phase 1 - schema / invariants

- [ ] `snapshot` and `snapshot_file` tables exist in both SQLite and PostgreSQL paths
- [ ] Unique `(snapshot_id, path_id)` constraint exists (`snapshot_path.path` remains globally unique)
- [ ] Migration version is correct and idempotent
- [ ] Path normalization rules are centralized and tested
- [ ] No regression to pre-snapshot schema behavior

### Phase 2 - snapshot creation

- [ ] Full snapshot copies all current `physical_file` rows
- [ ] Partial snapshot supports exact paths and directory prefixes
- [ ] Exact missing path causes rollback
- [ ] Empty directory prefix is allowed and deterministic
- [ ] Duplicate inputs are deduplicated
- [ ] Normalized slash-path semantics are enforced

### Phase 3 - snapshot restore

- [ ] Restore reads from `snapshot_file`, not current state
- [ ] Full snapshot restore works
- [ ] Partial restore exact/path-prefix semantics match snapshot create semantics
- [ ] Overwrite rules are preflight-validated
- [ ] Metadata handling is correct for normal, `--no-metadata`, and `--strict`
- [ ] Restore planning is side-effect free until execution
- [ ] Destination modes behave consistently

### Phase 4 - snapshot visibility / lifecycle

- [ ] Snapshot list works with filtering and ordering
- [ ] Snapshot show returns metadata plus file list
- [ ] Snapshot stats works globally and per snapshot
- [ ] Snapshot lineage is documented and tested as metadata-only (not restore dependency)
- [ ] `snapshot delete --force` only removes snapshot metadata
- [ ] Delete does not directly delete retained content

### Phase 5 - snapshot diff

- [ ] Diff classification is path-based and logical-ID-based
- [ ] Added/removed/modified semantics are correct
- [ ] Unchanged content is omitted
- [ ] Output ordering is deterministic
- [ ] Summary matches returned diff entries
- [ ] JSON/text contracts are stable

### Phase 6 - snapshot query/filtering

- [ ] Single `SnapshotQuery` abstraction is used across show/restore/diff
- [ ] Exact/prefix/glob/regex/size/time filters all validate correctly
- [ ] Query criteria are ANDed
- [ ] Filtered counts match returned collections
- [ ] Slash-path glob behavior is documented and implemented consistently
- [ ] Diff query filtering is applied after classification
- [ ] Diff size/mtime semantics are documented and stable

### Phase 7 - snapshot-aware retention / GC

- [ ] Retained logical roots are computed from `physical_file` union `snapshot_file`
- [ ] Snapshot-only retained content is GC-safe
- [ ] Deleting a snapshot changes only future GC eligibility; eligibility changes only when all retaining snapshots are removed
- [ ] Child snapshot remains restorable after deleting its lineage parent
- [ ] Stats expose snapshot retention pressure
- [ ] Verify audits persisted snapshot reachability anomalies
- [ ] Doctor/reporting surfaces snapshot-retention integrity context
- [ ] G14-G17 are reflected in `VALIDATION_MATRIX.md` as covered

### C. Test surface checklist

Package tests:

- [ ] `internal/snapshot` covers create / restore / diff / query behavior
- [ ] `internal/retention` covers current-only / snapshot-only / shared retention
- [ ] `internal/maintenance` covers snapshot-retained container protection
- [ ] `internal/verify` covers snapshot reachability anomalies
- [ ] Stats/reporting tests include snapshot retention visibility

Integration tests:

- [ ] Snapshot lifecycle end-to-end works
- [ ] Filtered snapshot show returns correct matched counts
- [ ] Filtered snapshot diff summary matches returned entries
- [ ] Snapshot-retained content blocks GC until all retaining snapshots are deleted
- [ ] Long-run snapshot churn test remains green

Adversarial tests:

- [ ] G14 snapshot-retained GC guard
- [ ] G15 corrupted snapshot metadata detection with conservative GC
- [ ] G16 snapshot query contract chaos
- [ ] G17 retention root transition churn
- [ ] Older G1-G13 adversarial tests still pass

Smoke:

- [ ] Smoke includes snapshot lifecycle gate
- [ ] Smoke resets snapshot tables too
- [ ] Smoke exercises:
- [ ] `snapshot create`
- [ ] `snapshot show`
- [ ] `snapshot restore`
- [ ] `snapshot diff`
- [ ] `snapshot delete`
- [ ] GC dry-run before/after delete

### D. Documentation / release checklist

README:

- [ ] Snapshot status line matches actual feature set
- [ ] Snapshot command examples are accurate
- [ ] Query semantics are documented
- [ ] Diff filtering semantics are documented
- [ ] Delete semantics are documented

PR template / reviewer context:

- [ ] `.github/pull_request_template.md` exists and matches current release impact language
- [ ] Release PR uses the template and includes lifecycle-semantics impact note

VALIDATION_MATRIX:

- [ ] G14-G17 are listed and covered
- [ ] Evidence names match actual tests
- [ ] No stale "covered" claims remain

Quick evidence-name consistency check (G14-G17):

```bash
for t in \
  TestListRetainedLogicalFileIDs \
  TestIsLogicalFileReferencedBySnapshot \
  TestComputeReachabilitySummary \
  TestRemoveFailsWhenLogicalFileIsRetainedBySnapshot \
  TestRunGCDoesNotDeleteSnapshotRetainedContainer \
  TestRunGCDryRunDoesNotCountSnapshotRetainedContainerAsReclaimable \
  TestAdversarialG14SnapshotRetainedGCGuardUnderChurn \
  TestDeleteSnapshotRemovesSnapshotRowsOnly \
  TestAdversarialG17RetentionRootTransitionChurn \
  TestRunStatsResultIncludesSnapshotRetentionVisibility \
  TestRunStatsCommandJSONIncludesSnapshotRetention \
  TestAdversarialG16SnapshotQueryContractChaos \
  TestVerifySystemStandardPassesWithConsistentSnapshotReachability \
  TestVerifySystemStandardDetectsOrphanSnapshotLogicalReference \
  TestVerifySystemStandardDetectsSnapshotInvalidLifecycleState \
  TestVerifySystemStandardDetectsSnapshotRetainedMissingChunkGraph \
  TestFormatDoctorTextReportGoldenHealthy \
  TestFormatDoctorTextReportGoldenDegraded \
  TestAdversarialG15CorruptedSnapshotMetadataDetectionConservativeGC
do
  grep -R --line-number --include='*.go' "func ${t}(" . >/dev/null || {
    echo "missing evidence: ${t}";
    exit 1;
  }
done
echo "G14-G17 evidence names: OK"
```

## 16) Snapshot CLI/contract checklist

Commands in scope:

- [ ] `snapshot create`
- [ ] `snapshot restore`
- [ ] `snapshot list`
- [ ] `snapshot show`
- [ ] `snapshot stats`
- [ ] `snapshot delete`
- [ ] `snapshot diff`

For each command above, confirm:

- [ ] Text mode output is understandable
- [ ] JSON output keeps stable envelope structure
- [ ] `command`/action fields are correct
- [ ] Error classification follows frozen CLI behavior
- [ ] Filtered counts and returned arrays remain consistent

Additional CLI validation and policy checks:

- [ ] `snapshot diff --filter added|removed|modified` works as specified
- [ ] `--path`, `--prefix`, `--pattern`, `--regex`, `--min-size`, `--max-size`, `--modified-after`, and `--modified-before` validate at CLI level
- [ ] Invalid regex/pattern/time/size ranges fail as usage errors (exit code `2`)
- [ ] `snapshot delete` requires at least one of `--force` or `--dry-run`; when both are provided, `--dry-run` takes precedence (read-only)

## 17) Verify snapshot / retention contract (manual gate)

Run this manual lifecycle gate after core CI/test gates pass.

This is a Profile B manual gate. It is required for release-tag validation or
snapshot/retention-impacting changes, not for every small PR.

Naming note for this gate: `pre-gc-gate` is the `snapshot_id` system identifier.
Create it with `--id`, then pass it positionally to `snapshot restore`,
`snapshot diff`, and `snapshot delete`. If you also set `--label`, treat it as
metadata only (never as a command target).

```bash
# Prefer a single retaining snapshot for this gate unless you intentionally want
# to test multi-snapshot retention. If multiple snapshots retain the same logical
# file, GC eligibility will not change until all retaining snapshots are deleted.

# create snapshot
./coldkeep snapshot create --id pre-gc-gate --output json

# confirm current-path removal is blocked while the logical file is retained by a snapshot
./coldkeep remove --stored-path <stored-path-from-store-output> --output json

# confirm GC dry-run reports snapshot-retained logical files
./coldkeep gc --dry-run --output json

# restore from snapshot
./coldkeep snapshot restore pre-gc-gate --mode prefix --destination ./out --output json

# diff two snapshots
./coldkeep snapshot diff pre-gc-gate <second-snapshot-id> --output json

# delete snapshot
./coldkeep snapshot delete pre-gc-gate --force --output json

# confirm GC eligibility changes only after all retaining snapshots are deleted
./coldkeep gc --dry-run --output json
```

Confirm:

- [ ] Snapshot create succeeds
- [ ] Removing current mapping is refused while the logical file is snapshot-retained
- [ ] GC dry-run reports snapshot-retained logical files before snapshot delete
- [ ] Snapshot restore succeeds from retained snapshot data
- [ ] Snapshot diff works and output is consistent with returned entries
- [ ] Snapshot delete succeeds only with `--force`
- [ ] GC eligibility changes only after all retaining snapshots are deleted

## 18) Final global sign-off

- [ ] Doctor checks passed
- [ ] Validation matrix audit passed
- [ ] Bootstrap on/off behavior verified
- [ ] Clean install path verified
- [ ] CLI contract stability verified
- [ ] Batch CLI contract stability verified
- [ ] v1.2 physical-file contract verified (G10–G13)
- [ ] v1.6 observability / simulation contract verified (non-gating per historical template sections 12-14)
- [ ] Snapshot phase checklist verified (Phases 1-7)
- [ ] Snapshot C. test surface checklist verified
- [ ] Snapshot D. documentation/release checklist verified
- [ ] Snapshot/retention manual gate verified
- [ ] Release PR description follows `.github/pull_request_template.md`
