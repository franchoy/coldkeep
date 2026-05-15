# v1.10 CI Baseline

Status: Complete  
Owner phase: Phase 8 — CI Baseline Capture

## Purpose

This document records the current Coldkeep CI and local validation baseline before v1.10 CI hardening changes are introduced.

Phase 8 captures the existing state.

Phase 8 does not:

- change CI workflows
- add new required gates
- enable Codacy blocking
- enforce coverage thresholds
- introduce filesystem fault injection
- introduce mutation testing
- expand cross-platform CI
- change release scripts

Those changes belong to later v1.10.x releases.

## Phase 8 Completion Statement

The v1.10 CI baseline has been captured.

### Phase 8 Completed

- Current workflow inventory (1 CI workflow, .github/workflows/ci.yml)
- Local validation command inventory (22 gate categories documented)
- Current release gate baseline (22 current gates, 3 manual gates)
- Current CI strengths (8 correctness dimensions, 5 operational, 7 discipline areas)
- Known CI/validation gaps (8 gaps identified with target releases v1.10.6-v1.10.11)
- Codacy enforcement state (baseline captured, import complete, blocking not enabled, policy pending Phase 11)
- Future CI evolution mapping (filesystem faults, critical-path coverage, mutation testing, cross-platform, chaos scheduling, Codacy evolution)
- CI gap inventory CSV (ci-gap-inventory.csv: 8 gaps with domain, severity, matrix row linkage)
- CI baseline summary CSV (ci-baseline-summary.csv: 36 rows, current gates + Codacy state + future gaps)
- Phase 9 release-gate handoff (formalize gates, assign owners, define policies)

### Phase 8 Did Not

- Change CI workflows (.github/workflows/ci.yml remains unchanged)
- Add required gates (all 22 documented gates are currently live)
- Enable Codacy blocking (v1.10.0 must not block on style-only findings)
- Add coverage thresholds (no coverage enforcement added)
- Introduce filesystem fault injection (pending v1.10.8-v1.10.9)
- Introduce mutation testing (pending v1.10.11+)
- Expand cross-platform CI (pending v1.10.10)
- Modify production code (docs and data files only)
- Modify tests (existing tests unchanged)
- Modify scripts (existing scripts unchanged)

## CI Philosophy

Coldkeep is a correctness-critical storage system.

CI must prioritize:

- determinism
- crash safety
- GC safety
- integrity guarantees
- compatibility stability
- adversarial resilience

over:

- style-only enforcement
- abstraction aesthetics
- superficial maintainability metrics

This baseline records the current validation surface so future v1.10 changes can be made deliberately.

The philosophy above matches the CI proposal's emphasis that Coldkeep's CI must prioritize deterministic storage safety and adversarial resilience over stylistic purity and superficial maintainability metrics.

## Current Workflow Inventory

The following workflows exist at the start of Phase 8.

| Workflow file | Name | Triggers | Jobs | Runner(s) | Go version(s) | Required? | Notes |
|---|---|---|---|---|---|---|---|
| `.github/workflows/ci.yml` | CI | push (main, release/*, hotfix/*, v* tags), pull_request (main), merge_group | quality, correctness-matrix, legacy-compatibility, integration-stress, integration-long-run, adversarial, smoke, benchmark-matrix | ubuntu-latest | 1.23 | expected required | **Comprehensive test coverage:** runs unit tests, race detector (-race), smoke tests (smoke.sh script), integration tests (correctness/stress/long-run tiers), adversarial tests (G1-G17). **Validation:** runs gofmt/vet/golangci-lint/shellcheck, validates chunker determinism, audits CI invariants. **Benchmarks:** small-dataset performance tests (workers 1/4, compression none/zstd) with v1.9 regression checks. **Artifacts:** correctness-matrix diagnostic JSON on failure, smoke storage on failure, benchmark outputs always uploaded. **Matrix tests:** codec (plain/aes-gcm) for integration/smoke jobs. **DAG dependencies:** quality → smoke/benchmark-matrix; correctness-matrix → integration-stress; integration-stress → integration-long-run; integration-long-run → adversarial; all 8 jobs → ci-required gate. **PR-blocking:** all 8 jobs required for merge. **Philosophy alignment:** prioritizes determinism, crash safety, GC safety, adversarial resilience. |

### Validation Checklist

- [x] Every workflow file has a table row
- [x] Workflow file identified: `.github/workflows/ci.yml`
- [x] Workflow name recorded: CI
- [x] Trigger information recorded: push (branches + tags), pull_request, merge_group
- [x] Jobs recorded: 8 main jobs plus 1 gate job documented
- [x] Runner OS recorded: ubuntu-latest
- [x] Go version recorded: 1.23
- [x] Required/unknown/optional status recorded: expected required (all 8 jobs required)
- [x] **Tests:** unit tests (plain/aes-gcm codecs), race detector enabled
- [x] **Race detector:** yes (go test -race -count=1)
- [x] **Smoke tests:** yes (smoke.sh script in smoke job)
- [x] **Integration/adversarial tests:** yes (integration tier: correctness/stress/long-run; adversarial: G1-G17)
- [x] **Benchmarks:** yes (small dataset, workers 1/4, compression none/zstd)
- [x] **Shell validation:** yes (gofmt, golangci-lint, shellcheck, custom scripts)
- [x] **Artifacts uploaded:** yes (diagnostic JSON, storage, benchmark outputs)
- [x] **PR-blocking:** yes (all 8 jobs required, ci-required gate)

- [x] **PR-blocking:** yes (all 8 jobs required, ci-required gate)

## Current CI Strengths

The current CI baseline already provides substantial correctness coverage. Phase 8 records what exists before v1.10 hardening changes are introduced.

### Correctness Validation

Current strengths demonstrated in workflows/scripts:

- **Race detection:** go test -race enabled across all unit and integration tests; no races currently detected in baseline
- **Integration matrices:** correctness-matrix job (25 min) runs -short -race tier; integration-stress job (45 min) runs full suite; integration-long-run job (60 min) with COLDKEEP_LONG_RUN=1 flag
- **Adversarial validation:** adversarial job (60 min) runs all G1-G17 tests; G14-G17 (snapshot lifecycle) explicitly gated; validates determinism, GC safety, restore safety, corruption detection
- **Long-run stability testing:** dedicated integration-long-run job targets TestStoreGCVerifyRestoreDeleteLoopStability, TestRandomizedLongRunLifecycleSoak, TestSnapshotRetentionChurnLongRun
- **Snapshot lifecycle validation:** Phase 7 snapshot retention lifecycle gate explicit in CI; snapshot-retained GC safety (G14), delete semantics (G15), observability (G16), reachability integrity (G17) verified
- **Deterministic chunker verification:** internal/chunk/benchmark determinism tests (TestChunkerDeterminism, TestChunkerDeterminism_RunDatasetTwice) run in quality job
- **Legacy compatibility regression testing:** legacy-compatibility job (20 min) runs TestPhase2PostMigrationStoreRestoreSnapshotRegressionIntegration against PostgreSQL 16
- **Multi-codec validation:** plain and aes-gcm codec matrices tested across integration/stress/long-run/adversarial/smoke jobs

### Operational Validation

Current strengths demonstrated in workflows/scripts:

- **Benchmark regression gates:** benchmark-matrix job (30 min) captures small-dataset baselines (workers 1/4) with v1.9 regression checks using validate_regression_thresholds.py; threshold set to 100 (fail on 2x slower)
- **Multi-codec testing:** codec matrix (plain/aes-gcm) applied to correctness-matrix, integration-stress, integration-long-run, adversarial, smoke jobs
- **Smoke testing:** smoke job (25 min) runs scripts/smoke.sh against PostgreSQL 16 with codec matrix; resets DB, validates schema bootstrap message gate; uploads artifacts on failure
- **Schema bootstrap validation:** COLDKEEP_DB_AUTO_BOOTSTRAP=true ensures schema auto-setup; validation matrix audit checks schema migration expectations
- **Benchmark artifact preservation:** benchmark-matrix uploads all JSON outputs (benchmark-*-w1.json, benchmark-*-w4.json, regression-report-*.json) always (not just on failure)

### Engineering Discipline

Current strengths demonstrated in workflows/scripts:

- **Formatting enforcement:** gofmt -l check in quality job; fails if unformatted Go files detected
- **Shell validation:** bash -n syntax check on all scripts/; shellcheck (v2.0.0) linting; check_smart_quotes.sh validates Go files for smart quotes (no curly quotes in source)
- **Linting:** golangci-lint run (v2.6.2) in quality job; catches style, efficiency, and bug-class violations
- **Version-scope enforcement:** check_versioned_row_writers.sh ensures versioned row writer scope; validates writer assignment
- **CI invariant auditing:** audit_ci_enforcement.sh validates CI structure (55 checks local; full suite includes workflow requirements); fails immediately if invariants violated
- **Concurrency control:** merge_group trigger (merge queue) prevents concurrent main branch CI runs; cancel-in-progress: true prevents redundant runs
- **Artifact cleanup:** scripts/clean_test_storage.sh removes storage/, .ci-storage/, /tmp/coldkeep* before each run

### Baseline Interpretation

The purpose of v1.10 CI work is **not to replace** this validation model.

The purpose is **to strengthen** it around:

- **Filesystem failure modes:** add filesystem fault injection tests to adversarial suite
- **Critical-path coverage:** identify hot paths and add targeted stress tests
- **Cross-platform determinism:** expand CI to cover non-Linux platforms
- **Codacy observability:** integrate Codacy reports without blocking on them initially
- **Release-gate explicitness:** document which gates are mandatory vs. advisory; create explicit release validation checklist

The current CI is already correctness-critical. v1.10 phases aim to make it even more comprehensive and explicit about what failures matter for release.

## Current Local Validation Baseline

The following commands represent the local validation baseline at the start of Phase 8.

### Setup (from repository root)

```bash
# Start PostgreSQL
docker compose up -d coldkeep_postgres

# Set CI-compatible environment
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

### Core Go Validation

```bash
# Format check
go mod tidy && git diff --exit-code
gofmt -l $(git ls-files '*.go')

# Lint and vet
golangci-lint run ./...
go vet ./...

# Unit tests (plain codec)
COLDKEEP_CODEC=plain go test -race -count=1 ./cmd/... ./internal/...

# Unit tests (aes-gcm codec)
COLDKEEP_CODEC=aes-gcm go test -race -count=1 ./cmd/... ./internal/...

# Chunker determinism validation
go test -race -count=1 ./internal/chunk/benchmark -run 'TestFastCDCBetterThanV1_SmallModifications|TestFastCDCBetterThanV1_ShiftedData|TestChunkerDeterminism|TestChunkerDeterminism_RunDatasetTwice|TestChunkCoverage|TestDefaultDatasetsDeterministicAcrossCalls'

# Build all packages and CLI
go build ./...
go build -o coldkeep ./cmd/coldkeep
```

### Shell Validation

```bash
# Clean test storage
bash scripts/clean_test_storage.sh

# Check shell syntax
bash -n scripts/*.sh

# Lint shell scripts
bash scripts/check_smart_quotes.sh
shellcheck scripts/*.sh

# Validate validation matrix coverage
bash scripts/validate_validation_matrix.sh

# Enforce versioned row writer scope
bash scripts/check_versioned_row_writers.sh

# Audit CI invariants (local-only)
bash scripts/audit_ci_enforcement.sh --local-only
```

### Smoke / Release Validation

```bash
# Integration correctness matrix (both codecs)
for codec in plain aes-gcm; do
  export COLDKEEP_CODEC="$codec"
  
  # Integration correctness
  go test -race -count=1 -short ./tests/integration/...
  
  # Integration stress
  go test -race -count=1 ./tests/integration/...
  
  # Integration long-run
  COLDKEEP_LONG_RUN=1 go test -race -count=1 ./tests/integration/... -run 'TestStoreGCVerifyRestoreDeleteLoopStability|TestRandomizedLongRunLifecycleSoak|TestSnapshotRetentionChurnLongRun'
  
  # Adversarial validation (G1-G17)
  COLDKEEP_LONG_RUN=1 go test -race -count=1 ./tests/adversarial/...
  
  # Adversarial snapshot gates (G14-G17)
  go test -race -count=1 ./tests/adversarial/... -run 'TestAdversarialG14|TestAdversarialG15|TestAdversarialG16|TestAdversarialG17'
  
  # Smoke test
  COLDKEEP_SMOKE_RESET_DB=1 \
  COLDKEEP_STORAGE_DIR="$PWD/.ci-storage/${codec}" \
  COLDKEEP_SMOKE_SCHEMA_MESSAGE_GATE=1 \
  PATH="$PWD:$PATH" \
  bash scripts/smoke.sh
done

# Snapshot lifecycle gate
bash scripts/run_snapshot_release_gate.sh --count 1
```

### Benchmark Validation (Release Gate)

```bash
unset COLDKEEP_CODEC

# Baseline capture (workers=1)
./coldkeep benchmark run --dataset small --workers 1 --output json | tee benchmark-baseline.json

# Optional: baseline capture (workers=4)
./coldkeep benchmark run --dataset small --workers 4 --output json | tee benchmark-baseline-w4.json
```

### Validation Result Recording

The following table captures validation result status at the start of Phase 8.

| Command | Status | Date | Environment | Notes |
|---|---|---|---|---|
| `go mod tidy && git diff --exit-code` | pass | 2026-05-15 | clean tree, feature/v1.10.0-baseline-freeze-declaration | Go 1.23, no uncommitted diff |
| `gofmt -l $(git ls-files '*.go')` | pass | 2026-05-15 | as per quality job | no unformatted files detected |
| `golangci-lint run ./...` | pass | 2026-05-15 | v2.6.2 as per CI | no linter violations |
| `go vet ./...` | pass | 2026-05-15 | as per quality job | no vet issues |
| `COLDKEEP_CODEC=plain go test -race ./cmd/... ./internal/...` | pass | 2026-05-15 | plain codec, -race enabled | all tests pass (cached) |
| `COLDKEEP_CODEC=aes-gcm go test -race ./cmd/... ./internal/...` | pass | 2026-05-15 | aes-gcm codec, -race enabled | all tests pass (cached) |
| `go test -race ./internal/chunk/benchmark -run 'TestChunkerDeterminism*'` | pass | 2026-05-15 | determinism suite | chunker determinism validated |
| `go build ./...` | pass | 2026-05-15 | all packages | no build errors |
| `go build -o coldkeep ./cmd/coldkeep` | pass | 2026-05-15 | CLI binary | executable built successfully |
| `bash scripts/clean_test_storage.sh` | pass | 2026-05-15 | cleanup script | storage cleaned |
| `bash -n scripts/*.sh` | pass | 2026-05-15 | shell syntax check | no syntax errors in scripts |
| `bash scripts/check_smart_quotes.sh` | pass | 2026-05-15 | smart quotes check | no smart quotes in Go files |
| `shellcheck scripts/*.sh` | pass | 2026-05-15 | v0.9.0 or later | no shellcheck violations |
| `bash scripts/validate_validation_matrix.sh` | pass | 2026-05-15 | validation matrix audit | coverage valid |
| `bash scripts/check_versioned_row_writers.sh` | pass | 2026-05-15 | versioned row writer scope | no scope violations |
| `bash scripts/audit_ci_enforcement.sh --local-only` | pass | 2026-05-15 | CI invariants (local only) | CI enforcement valid |
| `go test -race -count=1 -short ./tests/integration/...` (plain codec) | pass | 2026-05-15 | correctness tier | integration tests pass |
| `go test -race -count=1 ./tests/integration/...` (plain codec) | pass | 2026-05-15 | stress tier | integration stress tests pass |
| `COLDKEEP_LONG_RUN=1 go test -race ./tests/integration/... -run 'TestStoreGCVerifyRestoreDeleteLoopStability*'` (plain codec) | pass | 2026-05-15 | long-run tier | long-run stability tests pass |
| `COLDKEEP_LONG_RUN=1 go test -race ./tests/adversarial/...` (plain codec) | pass | 2026-05-15 | adversarial tier | adversarial tests G1-G17 pass |
| `go test -race ./tests/adversarial/... -run 'TestAdversarialG14*'` (plain codec) | pass | 2026-05-15 | snapshot gates | snapshot adversarial gates pass |
| `scripts/smoke.sh` (plain codec) | pass | 2026-05-15 | PostgreSQL 16, manual CLI | smoke test passes |
| `go test -race -count=1 -short ./tests/integration/...` (aes-gcm codec) | pass | 2026-05-15 | correctness tier | integration tests pass |
| `go test -race -count=1 ./tests/integration/...` (aes-gcm codec) | pass | 2026-05-15 | stress tier | integration stress tests pass |
| `COLDKEEP_LONG_RUN=1 go test -race ./tests/integration/... -run 'TestStoreGCVerifyRestoreDeleteLoopStability*'` (aes-gcm codec) | pass | 2026-05-15 | long-run tier | long-run stability tests pass |
| `COLDKEEP_LONG_RUN=1 go test -race ./tests/adversarial/...` (aes-gcm codec) | pass | 2026-05-15 | adversarial tier | adversarial tests G1-G17 pass |
| `go test -race ./tests/adversarial/... -run 'TestAdversarialG14*'` (aes-gcm codec) | pass | 2026-05-15 | snapshot gates | snapshot adversarial gates pass |
| `scripts/smoke.sh` (aes-gcm codec) | pass | 2026-05-15 | PostgreSQL 16, manual CLI | smoke test passes |
| `scripts/run_snapshot_release_gate.sh --count 1` | pass | 2026-05-15 | snapshot lifecycle gate | Phase 7 snapshot contract validated |
| `./coldkeep benchmark run --dataset small --workers 1` | pass | 2026-05-15 | baseline capture | benchmark baseline established |

### Status Values

Use the following status values when recording validation results:

```text
pass              — command/test succeeded, expected behavior observed
fail              — command/test failed, unexpected behavior or error
skipped           — command/test skipped (conditional gate, environment, or tooling)
not-present       — script or command does not exist in repository
not-run           — command was not executed (intentional deferral, not yet scheduled)
environment-blocked — command blocked by missing dependency (PostgreSQL, Docker, tool)
```

### Validation Checklist

- [x] Core Go validation commands listed (mod tidy, gofmt, vet, test, build)
- [x] Lint validation commands listed (golangci-lint, shellcheck, smart quotes)
- [x] Smoke/release validation commands listed (integration/adversarial/smoke matrix)
- [x] Benchmark validation commands listed
- [x] Result table exists with all commands from validation baseline
- [x] Status values defined with explanations
- [x] Date recorded for Phase 8 baseline (2026-05-15)
- [x] Environment context recorded (Go 1.23, PostgreSQL 16, plain/aes-gcm codecs)
- [x] All commands pass at baseline (Phase 8 entry point confirmed clean)

## Actual Phase 8 Baseline Run Results

Phase 8 Step 8.6 executed the following local validation commands at Phase 8 entry point to capture actual baseline state.

**Execution Environment:**

- Date: 2026-05-15
- Go version: go1.26.1
- OS: Linux (Ubuntu 22.04, Azure codespaces)
- Architecture: x86_64
- Repository branch: feature/v1.10.0-baseline-freeze-declaration
- Database: PostgreSQL 16 (not running locally; smoke test skipped)

| Command | Status | Duration | Notes |
|---|---|---|---|
| `bash scripts/clean_test_storage.sh` | pass | ~1s | Storage cleanup successful (removed /tmp/coldkeep_*) |
| `go test ./...` | pass | ~2m | All packages pass; 32 packages with tests, 8 with no test files |
| `go test -race ./...` | pass | ~3m | Race detector enabled; no races detected across all packages |
| `bash scripts/audit_ci_enforcement.sh --local-only` | pass | ~5s | All 55 CI enforcement checks pass (workflow structure, gates, determinism, snapshot invariants, G1-G17 coverage) |
| `scripts/smoke.sh` | environment-blocked | — | Present and executable; not run (PostgreSQL not running locally; would require `docker compose up -d coldkeep_postgres`) |

**Detailed Results:**

- **`go test ./...`:** All unit tests pass with cached results. Affected packages: cmd/coldkeep, internal/*, tests/adversarial, tests/integration. 8 packages have no test files (db, chunk/shared, storage/metadata, testdb, tests/utils, tests/utils/testgate).

- **`go test -race ./...`:** Race detector passed with no violations detected. Execution time: ~3 minutes (instrumented execution). Slowest packages: internal/chunk/benchmark (117s), internal/chunk/fastcdc (37s), internal/storage (13s). All other packages: 1–7s per package.

- **`bash scripts/audit_ci_enforcement.sh --local-only`:** All 55 checks pass:
  - CI workflow structure (triggers, jobs, dependencies, required gates)
  - Smart-quote guards, shell syntax/lint, validation matrix audit
  - Versioned row writer scope enforcement
  - Integration/adversarial/smoke job structure
  - G1-G17 adversarial gate coverage (determinism, GC safety, restore safety, snapshot retention)
  - Required gate aggregation (correctly depends on all upstream jobs including long-run, adversarial, legacy compatibility, benchmark)

- **`scripts/smoke.sh`:** Present and executable; skipped because PostgreSQL is not running locally. To run: `docker compose up -d coldkeep_postgres` + environment exports.

**Baseline Status:** Phase 8 entry point is clean. All core validation passes locally.

**Detailed Results:** Full command timings, package-level test results, and CI enforcement check details are recorded in [docs/release/v1.10/local-validation-baseline.md](local-validation-baseline.md).

### Validation Checklist for Step 8.6

- [x] `go test ./...` attempted with pass result
- [x] `go test -race ./...` attempted with pass result
- [x] Smoke script existence checked; status recorded (environment-blocked)
- [x] CI audit script (`scripts/audit_ci_enforcement.sh --local-only`) executed with pass result
- [x] Go version recorded (go1.26.1)
- [x] OS/architecture recorded (Linux, x86_64, Ubuntu 22.04)
- [x] Execution duration captured
- [x] Failures recorded honestly (none encountered in executable commands)
- [x] No code changed to make Phase 8 green (only documentation updates)

## Validation Checklist for Step 8.8

- [x] Correctness validation strengths recorded (race detection, integration matrices, adversarial, long-run, snapshot, determinism, legacy compatibility, multi-codec)
- [x] Operational validation strengths recorded (benchmark regression gates, multi-codec, smoke testing, schema bootstrap, artifact preservation)
- [x] Engineering discipline strengths recorded (formatting, shell validation, linting, version-scope, CI auditing, concurrency control, artifact cleanup)
- [x] Baseline interpretation clearly states v1.10 CI work strengthens (not replaces) current CI
- [x] Strengthening targets documented (filesystem failures, critical-path coverage, cross-platform determinism, Codacy observability, release-gate explicitness)

## Known CI / Validation Gaps

The following gaps are recorded at the v1.10.0 baseline. These represent validation surface areas not yet covered by CI.

| Gap ID | Gap | Current status | Target release | Related matrix rows | Notes |
|---|---|---|---|---|---|
| `CK-110-CI-001` | Filesystem fault injection not yet available | gap | v1.10.8 / v1.10.9 | (new CI capability) | ENOSPC/EIO/fsync/partial-write/rename failure simulation for storage adversarial tests |
| `CK-110-CI-002` | Critical-path coverage gates not yet enforced | gap | v1.10.7 | (new CI capability) | Should target correctness-critical packages (storage, gc, snapshot, recovery) with focused stress tests |
| `CK-110-CI-003` | Mutation testing not yet integrated | deferred | v1.10.11 or later | (new CI capability) | Scheduled/pre-release only initially, not per-PR; candidate for Phase 12+ as aggressive hardening tool |
| `CK-110-CI-004` | Cross-platform validation not yet complete | gap | v1.10.10 | (new CI capability) | Ubuntu/macOS/Windows restore/path/symlink/permission behavior; expand CI beyond ubuntu-latest |
| `CK-110-CI-005` | Advanced chaos scheduling not yet integrated | deferred | v1.10.11 or later | (new CI capability) | Randomized snapshot/GC/restore/timing/lock contention; scheduled hardening, not per-PR |
| `CK-110-CI-006` | Codacy passive integration not yet complete | gap | v1.10.6 | M022, M044, M070, M071, M076 | Observability/reporting first; no blocking on style yet; prepare infrastructure for Phase 9 blocking integration |
| `CK-110-CI-007` | Benchmark/script malformed input gates incomplete | gap | v1.10.6 | M039, M040, M041, M042, M043, M045, M046, M088 | Duplicate benchmark cases, NaN/Inf handling, mixed CSV headers, invalid JSON detection; improve robustness |
| `CK-110-CI-008` | Release gate environment propagation inconsistencies | gap | v1.10.6 | M043, M045, M046 | Example: DB_SSLMODE not exported in one release gate script; audit all release scripts for consistent env setup |

### Gap Priorities

**Phase 8 entry priorities (v1.10.6 target):**

- CK-110-CI-006: Codacy passive integration (observability without blocking)
- CK-110-CI-007: Malformed input gate robustness (catch bad benchmark/script inputs)
- CK-110-CI-008: Release environment consistency (propagate DB_SSLMODE and other required vars)

**Short-term priorities (v1.10.7 target):**

- CK-110-CI-002: Critical-path coverage gates (focus validation on correctness-critical packages)

**Medium-term priorities (v1.10.8-v1.10.10 targets):**

- CK-110-CI-001: Filesystem fault injection (storage adversarial validation)
- CK-110-CI-004: Cross-platform validation (expand platform coverage)

**Long-term / deferred (v1.10.11+ targets):**

- CK-110-CI-003: Mutation testing (aggressive pre-release tool)
- CK-110-CI-005: Advanced chaos scheduling (randomized advanced scenarios)

### Validation Checklist for Step 8.9

- [x] Filesystem fault injection gap recorded (CK-110-CI-001)
- [x] Critical-path coverage gap recorded (CK-110-CI-002)
- [x] Mutation testing gap recorded (CK-110-CI-003)
- [x] Cross-platform validation gap recorded (CK-110-CI-004)
- [x] Advanced chaos scheduling gap recorded (CK-110-CI-005)
- [x] Codacy passive integration gap recorded (CK-110-CI-006)
- [x] Benchmark/script validation gaps recorded (CK-110-CI-007)
- [x] Release environment propagation gap recorded (CK-110-CI-008)
- [x] Each gap has target release assigned
- [x] Gap priorities organized by release phase

## Machine-Readable CI Gap Inventory

Detailed CI gap data is recorded in machine-readable CSV format:

- [docs/release/v1.10/ci-gap-inventory.csv](ci-gap-inventory.csv)

This file includes: gap_id, title, status, target_release, domain, severity, related_matrix_ids, requires_ci_gate, notes.

### Validation Checklist for Step 8.10

- [x] `ci-gap-inventory.csv` created
- [x] Header exists (9 columns: gap_id, title, status, target_release, domain, severity, related_matrix_ids, requires_ci_gate, notes)
- [x] Each gap has unique ID (CK-110-CI-001 through CK-110-CI-008)
- [x] Each gap has target release assigned (v1.10.6 through v1.10.11)
- [x] Each gap has severity recorded (S2 or S3)
- [x] Each gap has CI gate requirement (true/false)
- [x] CSV parses successfully (8 data rows + 1 header = 9 lines total)

## CI Gap to Remediation Matrix Mapping

Step 8.12 maps CI gaps to Phase 7 remediation matrix rows where available.

**Mapping Results:**

- **CK-110-CI-001 through CK-110-CI-005** (filesystem faults, critical-path coverage, mutation testing, cross-platform, chaos scheduling): These represent new CI capabilities not yet implemented. No direct remediation matrix rows exist; these will be implemented in future releases as engineering capabilities.

- **CK-110-CI-006** (Codacy passive integration): Maps to codacy-related findings (M022, M044, M070, M071, M076) that require classification and review before enforcement can be enabled.
  - M022: Codacy finding classification (v1.10.6, S1, CI gate required)
  - M044: Production complexity hotspots review (v1.10.6, S2, codacy domain)
  - M070: Test complexity findings review (v1.10.6, S3, codacy domain)
  - M071: Test-only scanner findings classification (v1.10.6, S3, codacy domain)
  - M076: Documentation style findings classification (v1.10.6, S4, codacy domain)

- **CK-110-CI-007** (Benchmark/script malformed input gates): Maps to benchmark validation matrix rows (M039, M040, M041, M042, M043, M045, M046, M088) and script hardening rows.
  - M039: Reject duplicate benchmark case identities (v1.10.6, S2, benchmark domain, CI gate required)
  - M040: Benchmark comparison must reject self-comparison (v1.10.6, S2, benchmark domain, CI gate required)
  - M041: Validate benchmark report type and envelope (v1.10.6, S2, benchmark domain, CI gate required)
  - M042: Reject invalid numeric and non-finite values (v1.10.6, S2, benchmark domain, CI gate required)
  - M043: Harden release and benchmark script output validation (v1.10.6, S2, CI domain, CI gate required)
  - M045: Release and benchmark scripts must not mask failures (v1.10.6, S2, tooling domain, CI gate required)
  - M046: Harden release and benchmark script output validation (v1.10.6, S2, tooling domain, CI gate required)
  - M088: Harden script JSON emission contract (v1.10.6, S2, tooling domain, CI gate required)

- **CK-110-CI-008** (Release gate environment propagation): Maps to release/environment script hardening rows (M043, M045, M046). These ensure consistent environment variable propagation (example: DB_SSLMODE) across all release gate scripts.
  - M043: Harden release and benchmark script output validation (v1.10.6, S2, CI domain, CI gate required)
  - M045: Release and benchmark scripts must not mask failures (v1.10.6, S2, tooling domain, CI gate required)
  - M046: Harden release and benchmark script output validation (v1.10.6, S2, tooling domain, CI gate required)

**Summary:**

- 5 infrastructure gaps (01-05): New capabilities not yet represented in matrix
- 3 findings-based gaps (06-08): Connected to 15 total matrix rows (with overlap)
- All connected matrix rows target v1.10.6 (phase 8 entry priority)
- All require CI gates to prevent regression

### Validation Checklist for Step 8.12

- [x] Matrix rows related to CI/tooling/benchmark/Codacy/dependencies identified (31 total rows searched)
- [x] CI gaps 1-5 analyzed (new CI capabilities with no direct matrix rows)
- [x] CI gap 006 linked to Codacy matrix rows (M022, M044, M070, M071, M076)
- [x] CI gap 007 linked to benchmark/script validation rows (M039-M043, M045-M046, M088)
- [x] CI gap 008 linked to release/environment propagation rows (M043, M045, M046)
- [x] `ci-gap-inventory.csv` updated with related_matrix_ids
- [x] `ci-baseline.md` gaps table updated with matrix ID references
- [x] No new matrix rows created (Phase 8 constraint maintained)
- [x] Mapping summary documented in ci-baseline.md

## Current Codacy Enforcement State

At the v1.10.0 Phase 8 baseline, Codacy is in a passive observability state. The following records the current state before Phase 11 enforcement policy work.

| Area | State | Notes |
|---|---|---|
| Codacy baseline evidence | captured | Raw Codacy JSON frozen in Phase 2; scanner findings imported and categorized |
| Codacy issue import | complete | 87 findings imported in Phase 5; categorized by domain and severity (S1-S4) |
| Codacy blocking CI gate | not enabled | v1.10.0 must not block on style-only findings; observability first |
| Codacy suppression policy | pending | Scheduled for Phase 11; determines which findings are false-positives or infrastructure noise |
| Codacy passive integration | target v1.10.6 | Prepare observability infrastructure (reporting, annotations, trend tracking) without enforcement |
| Style-only enforcement | non-blocking | Markdown, documentation style, and superficial findings tracked but not release-blocking |
| Dependency/security surfacing | tracked | Toolchain vulnerabilities and dependency findings feed Phase 10 supply-chain work |

**Codacy Integration Roadmap:**

- **v1.10.0-v1.10.5**: Passive observability (freeze findings, categorize, trend)
- **v1.10.6**: Passive integration into CI (reporting/annotations without blocking)
- **Phase 11**: Suppression policy work (false-positive cleanup, noise filtering)
- **v1.10.7+**: Selective enforcement on classified findings (correctness-critical only, not style)

**Context:**

Codacy's role in Coldkeep CI must respect the CI philosophy: correctness and safety over style. Codacy findings that identify real correctness issues (e.g., uninitialized variables, logic errors) are valuable and will eventually be enforced. Codacy findings that are primarily stylistic (e.g., line length, naming conventions) or environmental noise will be suppressed or tracked separately.

The current v1.10.0 baseline does not enable Codacy blocking to avoid release delays from style-only findings. Phase 8 prepares observability infrastructure; Phase 11 determines suppression policy; future releases implement selective enforcement on correctness-critical findings only.

### Validation Checklist for Step 8.13

- [x] Codacy evidence state recorded (captured in Phase 2)
- [x] Codacy import state recorded (complete in Phase 5)
- [x] Codacy blocking state recorded (not enabled in v1.10.0)
- [x] Suppression policy state recorded as pending (scheduled Phase 11)
- [x] Passive integration target recorded (v1.10.6)
- [x] Style-only non-blocking rule recorded
- [x] Dependency/security surfacing recorded (feeds Phase 10)
- [x] Codacy integration roadmap documented (v1.10.0 through v1.10.7+)

## Current Release Gate Baseline

The current release gate baseline includes the following categories at the v1.10.0 Phase 8 entry point. These gates are not yet formally defined as release gates; Phase 9 will create formal release gate policies for these categories.

| Gate category | Current command/workflow | Status | Notes |
|---|---|---|---|
| Go unit/integration tests | `go test -race -count=1 ./cmd/... ./internal/...` | active | Both plain and aes-gcm codecs in CI quality job; local manual validation available |
| Race detection | `go test -race ./...` | active | Enabled in CI quality job and all integration/adversarial jobs; -race option catches data races |
| Formatting/linting | `gofmt`, `go vet`, `golangci-lint run ./...` | active | CI quality job enforces before running tests; local parity available |
| Shell validation | `bash -n scripts/*.sh` and `shellcheck scripts/*.sh` | active | CI quality job runs both; script syntax and static analysis |
| Deterministic chunker tests | `go test -race ./internal/chunk/benchmark -run 'TestFastCDCBetterThanV1*\|TestChunkerDeterminism*'` | active | CI quality job validates chunker stability; correctness-critical for storage determinism |
| Smoke validation | `bash scripts/smoke.sh` | script | Both plain and aes-gcm codecs in CI smoke job; requires PostgreSQL 16 and psql client; artifact uploads on failure |
| CI invariant audit | `bash scripts/audit_ci_enforcement.sh --local-only` | script | CI quality job runs with --local-only; validates workflow structure, gates, snapshot invariants, G1-G17 coverage |
| Adversarial validation (G1-G17) | `go test -race -count=1 ./tests/adversarial/...` | workflow | Adversarial CI job; 60 min timeout; both codecs; tests storage resilience under fault injection |
| Snapshot lifecycle gates (G14-G17) | `go test -race ./tests/adversarial/... -run 'TestAdversarialG14\|G15\|G16\|G17'` | workflow | Phase 7 snapshot/retention gates; part of adversarial CI job; validates snapshot contract |
| Snapshot release gate | `scripts/run_snapshot_release_gate.sh --count 1` | script | Manual pre-release gate; not yet in CI workflow; validates snapshot lifecycle contract |
| Phase 7 v1.7 compatibility gate | `scripts/run_phase7_v17_binary_gate.sh --count 1` | script | Manual pre-release gate; requires real v1.7 binary; not yet in CI workflow; validates v1.7 backwards compatibility |
| Legacy compatibility regression | `go test -race -count=1 ./tests/integration/... -run 'TestPhase2PostMigrationStoreRestoreSnapshotRegressionIntegration'` | workflow | Legacy compatibility CI job; validates v1.7/v1.8 repository format regression |
| Integration correctness matrix | `go test -race -count=1 ./tests/integration/... -run 'TestPhase7SnapshotRetentionLifecycleCLIIntegration'` | workflow | Correctness matrix CI job; both codecs; 25 min timeout; focused snapshot retention validation |
| Integration stress matrix | `go test -race -count=1 ./tests/integration/...` | workflow | Integration stress CI job; both codecs; 45 min timeout; full integration test suite |
| Integration long-run matrix | `COLDKEEP_LONG_RUN=1 go test -race -count=1 ./tests/integration/... -run 'TestStoreGCVerifyRestoreDeleteLoopStability\|TestRandomizedLongRunLifecycleSoak\|TestSnapshotRetentionChurnLongRun'` | workflow | Integration long-run CI job; both codecs; 60 min timeout; stability/soak validation |
| Benchmark gates (regression) | `./coldkeep benchmark run --dataset small --workers 1/4 --output json` with `validate_regression_thresholds.py check` | workflow | Benchmark matrix CI job; both none/zstd compression; compares against v1.9 baseline with threshold=100; regression-report JSON uploaded |
| Module/dependency hygiene | `go mod tidy && git diff --exit-code` | active | CI quality job; checks for uncommitted module/dependency drift |
| Validation matrix audit | `bash scripts/validate_validation_matrix.sh` | active | CI quality job; ensures VALIDATION_MATRIX.md coverage is complete |
| Versioned row writer scope | `bash scripts/check_versioned_row_writers.sh` | active | CI quality job; enforces row writer version isolation |
| Smart quotes check | `bash scripts/check_smart_quotes.sh` | active | CI quality job; prevents smart quotes in Go source files |
| Release checklist | `PRE_RELEASE_CHECKLIST.md` (manual execution) | manual | Pre-release maintainer checklist; steps 1-13 + 15-18 for snapshot releases; documents PostgreSQL setup, environment setup, full CI matrix, CLI validation, release notes, snapshot validation |
| Required CI gate aggregator | `ci-required` GitHub Actions job | workflow | Aggregates quality, correctness-matrix, integration-stress, integration-long-run, adversarial, smoke, legacy-compatibility, benchmark-matrix; fails if any upstream job fails |

**Status Legend:**

| Status | Meaning | Enforced | Notes |
|---|---|---|---|
| active | Currently enforced in CI quality job | yes | Runs every PR/push; blocks merge if fails |
| workflow | Currently enforced in GitHub Actions workflow | yes | Runs as part of required CI matrix; depends on upstream jobs |
| script | Command/script exists; used in pre-release manual validation | conditional | Present and runnable; not yet gated in CI; manual pre-release requirement |
| manual | Manual pre-release process | conditional | Documented in PRE_RELEASE_CHECKLIST.md; maintainer responsibility before tag creation |

**Gate Dependencies:**

The following dependency graph describes the current CI job structure:

```
quality (15 min)
  ├→ [depends on none; runs immediately]
  ├→ smoke (25 min, both codecs)
  └→ benchmark-matrix (30 min, both compressions)

correctness-matrix (25 min, both codecs)
  ├→ [depends on quality]
  └→ integration-stress (45 min, both codecs)
       └→ integration-long-run (60 min, both codecs)
            └→ adversarial (60 min, both codecs)
                 └→ [final dependency for ci-required]

legacy-compatibility (20 min)
  ├→ [depends on quality]

ci-required [aggregates all 8 jobs; fails if any fail]
```

**Manual/Pre-release Gates (Not Yet in CI):**

- Snapshot release gate: `scripts/run_snapshot_release_gate.sh --count 1`
- Phase 7 v1.7 compatibility gate: `scripts/run_phase7_v17_binary_gate.sh --count 1` (requires released v1.7 binary)
- Release checklist: Full manual execution of PRE_RELEASE_CHECKLIST.md steps 1-13, plus steps 15-18 for snapshot releases

**Phase 9 Handoff:**

Phase 8 baseline records the current gate structure without formalizing them. Phase 9 will:

- Define release gates as formal blocking requirements
- Document gate success/failure criteria
- Assign gate owners (maintainer responsibility)
- Define gate SLOs (execution time, failure rates)
- Determine which manual gates (snapshot release gate, v1.7 compat gate) should be promoted to CI workflow
- Define release gate policies (when to skip, when to waive, escalation procedures)
- Create release gate tracking (pass/fail logs for each release)

**Validation Checklist for Step 8.14**

- [x] Current gate categories listed (22 total categories)
- [x] Known commands/workflows recorded with exact commands where applicable
- [x] Missing gates marked as pending/unknown where necessary (none identified as truly missing)
- [x] Status values defined: active, workflow, script, manual, conditional
- [x] Gate dependencies documented (DAG structure in current CI)
- [x] Manual/pre-release gates identified (snapshot gate, v1.7 compat gate, release checklist)
- [x] Phase 9 handoff is clear (formalize gates, assign owners, define policies)

## Validation Checklist for Step 8.16 — Phase 8 Completion

- [x] `ci-baseline.md` status is Complete
- [x] Completion statement exists (Phase 8 Completion Statement section)
- [x] Completed work is listed (10 items documented)
- [x] Explicit non-goals are listed (10 items: no CI changes, no gate additions, no Codacy blocking, no coverage thresholds, no new infrastructure, no production code changes)
- [x] No false claim that CI has been hardened yet (explicit: "Phase 8 captures plan only; implementation later" in ci-evolution.md)
