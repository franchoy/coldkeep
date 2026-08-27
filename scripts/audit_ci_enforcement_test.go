package scripts_test

import (
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/franchoy/coldkeep/internal/pathsafe"
)

func TestAuditCIEnforcementLocalWorkflowRequiresCrossPlatformInNeeds(t *testing.T) {
	workflow := readRepoFile(t, filepath.Join(".github", "workflows", "ci.yml"))
	codeqlWorkflow := readRepoFile(t, filepath.Join(".github", "workflows", "codeql.yml"))
	mutated := strings.Replace(
		workflow,
		", cross-platform, vulnerability, source-install",
		", vulnerability, source-install",
		1,
	)
	if mutated == workflow {
		t.Fatal("required-gate cross-platform dependency fixture was not removed")
	}

	stderr := runAuditLocalOnly(t, mutated, codeqlWorkflow, true)
	if !strings.Contains(stderr, "required gate depends on security and Phase 3 reproducibility jobs") {
		t.Fatalf("expected missing cross-platform dependency error, got:\n%s", stderr)
	}
}

func TestAuditCIEnforcementRequiresCandidateLintParity(t *testing.T) {
	workflow := readRepoFile(t, filepath.Join(".github", "workflows", "ci.yml"))
	codeqlWorkflow := readRepoFile(t, filepath.Join(".github", "workflows", "codeql.yml"))

	t.Run("hosted version pin", func(t *testing.T) {
		mutated := strings.Replace(workflow, "version: v2.9.0", "version: v2.9.1", 1)
		if mutated == workflow {
			t.Fatal("hosted linter version fixture not found")
		}
		stderr := runAuditLocalOnly(t, mutated, codeqlWorkflow, true)
		if !strings.Contains(stderr, "hosted quality pins golangci-lint v2.9.0") {
			t.Fatalf("expected hosted linter pin failure, got:\n%s", stderr)
		}
	})

	t.Run("pipeline status capture", func(t *testing.T) {
		gate := readRepoFile(t, filepath.Join("scripts", "run_candidate_lint_gate.sh"))
		mutated := strings.Replace(gate, "pipeline_status=(\"${PIPESTATUS[@]}\")", "pipeline_status=(0 0)", 1)
		if mutated == gate {
			t.Fatal("candidate lint pipeline fixture not found")
		}
		gatePath := filepath.Join(t.TempDir(), "run_candidate_lint_gate.sh")
		if err := os.WriteFile(gatePath, []byte(mutated), 0o700); err != nil {
			t.Fatalf("write candidate lint gate fixture: %v", err)
		}
		t.Setenv("COLDKEEP_CANDIDATE_LINT_GATE_FILE", gatePath)
		stderr := runAuditLocalOnly(t, workflow, codeqlWorkflow, true)
		if !strings.Contains(stderr, "candidate lint gate captures lint and tee pipeline statuses") {
			t.Fatalf("expected pipeline status audit failure, got:\n%s", stderr)
		}
	})
}

func TestCandidateLintGateRejectsFindingsWhenLinterReturnsSuccess(t *testing.T) {
	fakeLinter := writeFakeCandidateLinter(t)
	evidenceDir := t.TempDir()
	cmd := exec.Command("bash", "scripts/run_candidate_lint_gate.sh", "run", evidenceDir)
	cmd.Dir = repoRoot(t)
	cmd.Env = append(os.Environ(),
		"COLDKEEP_GOLANGCI_LINT_BIN="+fakeLinter,
		"FAKE_LINT_OUTPUT=cmd/example.go:7:3: synthetic actionable finding (unused)",
		"FAKE_LINT_EXIT=0",
	)
	output, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("expected actionable finding to fail closed, got success:\n%s", output)
	}
	if strings.Contains(string(output), "LOCAL_CANDIDATE_LINT=PASS") {
		t.Fatalf("finding-bearing run emitted PASS:\n%s", output)
	}
	status, readErr := os.ReadFile(filepath.Join(evidenceDir, "golangci-lint.status"))
	if readErr != nil {
		t.Fatalf("read lint status: %v", readErr)
	}
	if string(status) != "FAIL\n" {
		t.Fatalf("finding-bearing run status = %q, want FAIL", status)
	}

	verify := exec.Command("bash", "scripts/run_candidate_lint_gate.sh", "verify", evidenceDir)
	verify.Dir = repoRoot(t)
	if verifyOutput, verifyErr := verify.CombinedOutput(); verifyErr == nil {
		t.Fatalf("expected finding-bearing evidence verification to fail, got:\n%s", verifyOutput)
	}
}

func TestCandidateLintGateAcceptsCleanPinnedLinter(t *testing.T) {
	fakeLinter := writeFakeCandidateLinter(t)
	if !strings.Contains(fakeLinter, " ") {
		t.Fatalf("fake linter path %q does not exercise whitespace handling", fakeLinter)
	}
	evidenceDir := t.TempDir()
	callLog := filepath.Join(t.TempDir(), "calls.log")
	cmd := exec.Command("bash", "scripts/run_candidate_lint_gate.sh", "run", evidenceDir)
	cmd.Dir = repoRoot(t)
	cmd.Env = append(os.Environ(),
		"COLDKEEP_GOLANGCI_LINT_BIN="+fakeLinter,
		"FAKE_LINT_CALL_LOG="+callLog,
		"FAKE_LINT_OUTPUT=",
		"FAKE_LINT_EXIT=0",
	)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("expected clean pinned linter to pass, got err=%v output:\n%s", err, output)
	}
	if !strings.Contains(string(output), "LOCAL_CANDIDATE_LINT=PASS") {
		t.Fatalf("clean run did not emit PASS:\n%s", output)
	}
	calls, err := os.ReadFile(callLog)
	if err != nil {
		t.Fatalf("read fake linter call log: %v", err)
	}
	for _, invocation := range []string{"version\n", "config path\n"} {
		if !strings.Contains(string(calls), invocation) {
			t.Fatalf("whitespace-path fake linter did not receive %q; calls:\n%s", strings.TrimSpace(invocation), calls)
		}
	}
}

func TestAuditCIEnforcementLocalWorkflowRequiresNativeCoordinationRuntime(t *testing.T) {
	workflow := readRepoFile(t, filepath.Join(".github", "workflows", "ci.yml"))
	codeqlWorkflow := readRepoFile(t, filepath.Join(".github", "workflows", "codeql.yml"))

	for name, test := range map[string]struct {
		old         string
		replacement string
		wantMessage string
	}{
		"missing step": {
			old: "      - name: Run native coordination runtime tests\n" +
				"        run: go test -v -count=1 -run '^(TestNativeLock|TestWindowsNativeLock|TestProductionCoordinator)' ./internal/coordination\n\n",
			wantMessage: "cross-platform native coordination runtime step",
		},
		"altered command": {
			old:         "go test -v -count=1 -run '^(TestNativeLock|TestWindowsNativeLock|TestProductionCoordinator)' ./internal/coordination",
			replacement: "go test -v -count=1 ./internal/coordination",
			wantMessage: "cross-platform native coordination command covers native backends and production Coordinator",
		},
	} {
		t.Run(name, func(t *testing.T) {
			mutated := strings.Replace(workflow, test.old, test.replacement, 1)
			if mutated == workflow {
				t.Fatalf("workflow fixture did not contain %q", test.old)
			}
			stderr := runAuditLocalOnly(t, mutated, codeqlWorkflow, true)
			if !strings.Contains(stderr, test.wantMessage) {
				t.Fatalf("expected %q, got:\n%s", test.wantMessage, stderr)
			}
		})
	}
}

func TestAuditCIEnforcementRequiresCertifiedToolchainEverywhere(t *testing.T) {
	workflow := readRepoFile(t, filepath.Join(".github", "workflows", "ci.yml"))
	codeqlWorkflow := readRepoFile(t, filepath.Join(".github", "workflows", "codeql.yml"))

	mutated := strings.Replace(workflow, "go-version: '1.26.7'", "go-version: '1.26.x'", 1)
	if mutated == workflow {
		t.Fatal("certified CI toolchain fixture not found")
	}
	stderr := runAuditLocalOnly(t, mutated, codeqlWorkflow, true)
	if !strings.Contains(stderr, "every required CI setup-go step must pin Go 1.26.7 exactly") {
		t.Fatalf("expected exact CI toolchain failure, got:\n%s", stderr)
	}
}

func TestAuditCIEnforcementRequiresBlockingOrdinaryGovulncheck(t *testing.T) {
	workflow := readRepoFile(t, filepath.Join(".github", "workflows", "ci.yml"))
	codeqlWorkflow := readRepoFile(t, filepath.Join(".github", "workflows", "codeql.yml"))

	mutated := strings.Replace(
		workflow,
		"go run golang.org/x/vuln/cmd/govulncheck@v1.7.0 ./...",
		"go run golang.org/x/vuln/cmd/govulncheck@v1.7.0 -json ./...",
		1,
	)
	if mutated == workflow {
		t.Fatal("govulncheck command fixture not found")
	}
	stderr := runAuditLocalOnly(t, mutated, codeqlWorkflow, true)
	if !strings.Contains(stderr, "reachable-vulnerability scan must remain blocking and use ordinary output semantics") {
		t.Fatalf("expected ordinary-output govulncheck failure, got:\n%s", stderr)
	}
}

func TestAuditCIEnforcementRequiresNativeWindowsRenameBoundary(t *testing.T) {
	workflow := readRepoFile(t, filepath.Join(".github", "workflows", "ci.yml"))
	codeqlWorkflow := readRepoFile(t, filepath.Join(".github", "workflows", "codeql.yml"))
	step := "      - name: Run Windows secure-rename boundary tests\n" +
		"        if: runner.os == 'Windows'\n" +
		"        run: go test -v -count=1 -run '^TestWindowsRenameBuffer' ./internal/fsx/secureinstall\n\n"
	mutated := strings.Replace(workflow, step, "", 1)
	if mutated == workflow {
		t.Fatal("Windows secure-rename boundary fixture not found")
	}
	stderr := runAuditLocalOnly(t, mutated, codeqlWorkflow, true)
	if !strings.Contains(stderr, "native Windows secure-rename boundary step") {
		t.Fatalf("expected Windows boundary-gate failure, got:\n%s", stderr)
	}
}

func TestAuditCIEnforcementRequiresPinnedBenchmarkCalibrationToolchain(t *testing.T) {
	workflow := readRepoFile(t, filepath.Join(".github", "workflows", "ci.yml"))
	codeqlWorkflow := readRepoFile(t, filepath.Join(".github", "workflows", "codeql.yml"))
	baselineWorkflow := readRepoFile(t, filepath.Join(".github", "workflows", "benchmark-baseline.yml"))
	baselineWorkflow = strings.Replace(baselineWorkflow, "go-version: '1.26.7'", "go-version: '1.26.x'", 1)

	stderr := runAuditLocalOnlyWithBaseline(t, workflow, codeqlWorkflow, baselineWorkflow, true)
	if !strings.Contains(stderr, "benchmark calibration pins the certified Go patch") {
		t.Fatalf("expected benchmark calibration toolchain error, got:\n%s", stderr)
	}
}

func TestAuditCIEnforcementRejectsUnsafeBenchmarkCalibrationWorkflow(t *testing.T) {
	workflow := readRepoFile(t, filepath.Join(".github", "workflows", "ci.yml"))
	codeqlWorkflow := readRepoFile(t, filepath.Join(".github", "workflows", "codeql.yml"))
	baselineWorkflow := readRepoFile(t, filepath.Join(".github", "workflows", "benchmark-baseline.yml"))
	tests := []struct {
		name        string
		mutate      func(string) string
		wantMessage string
	}{
		{
			name: "main-only authorization removed",
			mutate: func(value string) string {
				return strings.Replace(
					value,
					`if [[ "${TRUSTED_REF}" != "refs/heads/main" ]]; then`,
					`if [[ "${TRUSTED_REF}" != "refs/heads/release/v1.13.11" ]]; then`,
					1,
				)
			},
			wantMessage: "benchmark authorization is fail-closed on refs/heads/main",
		},
		{
			name: "source SHA equality removed",
			mutate: func(value string) string {
				return strings.Replace(
					value,
					`if [[ "${SOURCE_SHA}" != "${TRUSTED_SHA}" ]]; then`,
					`if [[ -z "${SOURCE_SHA}" ]]; then`,
					1,
				)
			},
			wantMessage: "benchmark source_sha must equal trusted github.sha",
		},
		{
			name: "checkout source made operator-controlled",
			mutate: func(value string) string {
				return strings.Replace(value, "ref: ${{ github.sha }}", "ref: ${{ inputs.source_sha }}", 1)
			},
			wantMessage: "benchmark checkout cannot use inputs.source_sha",
		},
		{
			name: "persisted credentials defaulted",
			mutate: func(value string) string {
				return strings.Replace(value, "          persist-credentials: false\n", "", 1)
			},
			wantMessage: "benchmark checkouts must disable persisted credentials",
		},
		{
			name: "setup-go cache re-enabled",
			mutate: func(value string) string {
				return strings.Replace(value, "          cache: false", "          cache: true", 1)
			},
			wantMessage: "benchmark setup-go caching must be disabled",
		},
		{
			name: "sample harness redirected",
			mutate: func(value string) string {
				return strings.Replace(
					value,
					"python3 scripts/benchmark_gate.py sample",
					"python3 governed-source/scripts/benchmark_gate.py sample",
					1,
				)
			},
			wantMessage: "benchmark sample harness runs from trusted checkout",
		},
		{
			name: "calibration harness redirected",
			mutate: func(value string) string {
				return strings.Replace(
					value,
					"python3 scripts/benchmark_gate.py calibrate",
					"python3 governed-source/scripts/benchmark_gate.py calibrate",
					1,
				)
			},
			wantMessage: "benchmark calibration harness runs from trusted checkout",
		},
		{
			name: "runner temp artifact isolation removed",
			mutate: func(value string) string {
				return strings.Replace(
					value,
					"path: ${{ runner.temp }}/benchmark-calibration-input",
					"path: downloaded",
					1,
				)
			},
			wantMessage: "benchmark calibration artifacts use runner.temp",
		},
		{
			name: "artifact source provenance check removed",
			mutate: func(value string) string {
				return strings.Replace(value, "              if actual != expected:\n", "", 1)
			},
			wantMessage: "benchmark calibration requires artifact provenance to match github.sha",
		},
		{
			name: "authorization failure suppressed",
			mutate: func(value string) string {
				return strings.Replace(value, "          set -euo pipefail", "          set -euo pipefail\n          set +e", 1)
			},
			wantMessage: "benchmark source validation must not use broad failure suppression",
		},
		{
			name: "automatic schedule",
			mutate: func(value string) string {
				return strings.Replace(value, "  workflow_dispatch:", "  schedule:\n  workflow_dispatch:", 1)
			},
			wantMessage: "benchmark calibration workflow must remain manual-only",
		},
		{
			name: "write permission",
			mutate: func(value string) string {
				return strings.Replace(value, "  contents: read", "  contents: write", 1)
			},
			wantMessage: "benchmark calibration workflow must not receive write permission",
		},
		{
			name: "adaptive sample count",
			mutate: func(value string) string {
				return strings.Replace(value, "          sample_count=10", "          sample_count=11", 1)
			},
			wantMessage: "benchmark calibration fixes ten measured samples",
		},
		{
			name: "fixture drift",
			mutate: func(value string) string {
				return strings.Replace(value, "            --dataset ci-stable-v1", "            --dataset small", 1)
			},
			wantMessage: "benchmark calibration fixes the fixture identity",
		},
		{
			name: "push step",
			mutate: func(value string) string {
				return value + "\n# git push origin HEAD\n"
			},
			wantMessage: "benchmark calibration workflow must remain artifact-only",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mutated := tt.mutate(baselineWorkflow)
			if mutated == baselineWorkflow {
				t.Fatal("benchmark baseline workflow mutation target not found")
			}
			stderr := runAuditLocalOnlyWithBaseline(
				t,
				workflow,
				codeqlWorkflow,
				mutated,
				true,
			)
			if !strings.Contains(stderr, tt.wantMessage) {
				t.Fatalf("expected %q, got:\n%s", tt.wantMessage, stderr)
			}
		})
	}
}

func TestAuditCIEnforcementRejectsBenchmarkGovernanceMutations(t *testing.T) {
	workflow := readRepoFile(t, filepath.Join(".github", "workflows", "ci.yml"))
	codeqlWorkflow := readRepoFile(t, filepath.Join(".github", "workflows", "codeql.yml"))
	tests := []struct {
		name        string
		old         string
		replacement string
		message     string
	}{
		{
			name: "missing integrity profile", old: "          - profile: none-w4\n",
			replacement: "", message: "integrity matrix must contain profile none-w4 exactly once",
		},
		{
			name: "fixture drift", old: "dataset: ci-paired-w4-v2",
			replacement: "dataset: ci-paired-w4-v1", message: "bounded workers=4 fixture",
		},
		{
			name: "timeout drift", old: "--command-timeout-seconds 600",
			replacement: "--command-timeout-seconds 601", message: "600-second command timeout",
		},
		{
			name: "integrity downgrade", old: "python3 scripts/benchmark_gate.py integrity",
			replacement: "python3 scripts/validate_regression_thresholds.py check", message: "hard candidate-only interface",
		},
		{
			name: "advisory made legacy", old: "--policy hosted-advisory",
			replacement: "--policy legacy", message: "informational authority",
		},
		{
			name: "exit verifier removed", old: "verify-advisory-exit",
			replacement: "verify-removed-exit", message: "exact classification and exit code",
		},
		{
			name: "machine report not required", old: "          [[ -s \"${report}\" ]]\n",
			replacement: "", message: "requires a machine-readable report",
		},
		{
			name: "advisory exit allowlist widened", old: "            0|10|11|12)\n",
			replacement: "            0|2|10|11|12)\n", message: "narrowly accepts valid informational exit codes",
		},
		{
			name: "evaluator failure made successful", old: "            2)\n              exit 2\n",
			replacement: "            2)\n              exit 0\n", message: "must return failure for evaluator exit code 2",
		},
		{
			name: "timing checksum verification removed", old: "          sha256sum --check checksums.sha256\n",
			replacement: "", message: "timing artifact verifies checksums",
		},
		{
			name: "broad suppression", old: "          report=\"${evidence_dir}/timing-advisory.json\"\n          set +e\n",
			replacement: "          report=\"${evidence_dir}/timing-advisory.json\"\n          continue-on-error: true\n          set +e\n", message: "broad failure suppression",
		},
		{
			name: "integrity missing artifact allowed", old: "          if-no-files-found: error\n\n  benchmark-timing-advisory:",
			replacement: "          if-no-files-found: ignore\n\n  benchmark-timing-advisory:", message: "integrity artifact rejects missing evidence",
		},
		{
			name: "advisory upload not always", old: "      - name: Upload benchmark timing advisory evidence\n        if: ${{ always() }}",
			replacement: "      - name: Upload benchmark timing advisory evidence\n        if: ${{ success() }}", message: "timing artifact upload always runs",
		},
		{
			name: "required dependency removed", old: "benchmark-integrity, benchmark-timing-advisory, cross-platform, vulnerability",
			replacement: "benchmark-timing-advisory, cross-platform, vulnerability", message: "required gate depends on security and Phase 3 reproducibility jobs",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mutated := strings.Replace(workflow, tt.old, tt.replacement, 1)
			if mutated == workflow {
				t.Fatalf("mutation target not found: %q", tt.old)
			}
			output := runAuditLocalOnly(t, mutated, codeqlWorkflow, true)
			if !strings.Contains(output, tt.message) {
				t.Fatalf("expected %q, got:\n%s", tt.message, output)
			}
		})
	}
}

func TestAuditCIEnforcementRequiresSeparatedTimingAndIntegrityContracts(t *testing.T) {
	validator := readRepoFile(t, filepath.Join("scripts", "validate_regression_thresholds.py"))
	tests := []struct {
		name        string
		old         string
		replacement string
		message     string
	}{
		{
			name:        "diagnostic state made required",
			old:         `TIMING_ROW_OPTIONAL_FIELDS = {"diagnostic_final_state"}`,
			replacement: `TIMING_ROW_OPTIONAL_FIELDS = {}`,
			message:     "historical timing treats diagnostic final state as optional",
		},
		{
			name:        "optional state no longer validated",
			old:         `not legacy and "diagnostic_final_state" in row`,
			replacement: `not legacy and False`,
			message:     "optional timing diagnostic final state is validated when present",
		},
		{
			name:        "hard state imported into timing",
			old:         `TIMING_ROW_OPTIONAL_FIELDS = {"diagnostic_final_state"}`,
			replacement: "TIMING_ROW_OPTIONAL_FIELDS = {\"diagnostic_final_state\"}\nbenchmark_contract.hard_final_state({})",
			message:     "historical timing advisory must not require hard diagnostic final state",
		},
		{
			name:        "evaluator exit remapped",
			old:         `"BENCHMARK_TIMING_EVALUATION_FAILURE": 2`,
			replacement: `"BENCHMARK_TIMING_EVALUATION_FAILURE": 12`,
			message:     "timing evaluator failure maps exactly to exit code 2",
		},
		{
			name:        "omitempty counter removed",
			old:         `"container_append_count", "fsync_count", "container_open_count",`,
			replacement: `"fsync_count", "container_open_count",`,
			message:     "timing validator models Go omitempty field container_append_count",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mutated := strings.Replace(validator, tt.old, tt.replacement, 1)
			if mutated == validator {
				t.Fatalf("mutation target not found: %q", tt.old)
			}
			output := runAuditFixtureWithTimingValidator(
				t,
				readRepoFile(t, filepath.Join(".github", "workflows", "ci.yml")),
				readRepoFile(t, filepath.Join(".github", "workflows", "codeql.yml")),
				readRepoFile(t, filepath.Join(".github", "workflows", "benchmark-baseline.yml")),
				mutated,
				true,
				false,
				false,
			)
			if !strings.Contains(output, tt.message) {
				t.Fatalf("expected %q, got:\n%s", tt.message, output)
			}
		})
	}
}

func TestAuditCIEnforcementRejectsPrematureRequiredBenchmarkGateSwitch(t *testing.T) {
	workflow := readRepoFile(t, filepath.Join(".github", "workflows", "ci.yml"))
	workflow = strings.Replace(
		workflow,
		"python3 scripts/validate_regression_thresholds.py check",
		"python3 scripts/benchmark_gate.py compare",
		1,
	)
	stderr := runAuditLocalOnly(
		t,
		workflow,
		readRepoFile(t, filepath.Join(".github", "workflows", "codeql.yml")),
		true,
	)
	if !strings.Contains(stderr, "unauthorized benchmark sampler, comparator, or paired gate") {
		t.Fatalf("expected premature gate-switch error, got:\n%s", stderr)
	}
}

func TestAuditCIEnforcementRejectsPrematurePairedBenchmarkGateSwitch(t *testing.T) {
	workflow := readRepoFile(t, filepath.Join(".github", "workflows", "ci.yml"))
	workflow = strings.Replace(
		workflow,
		"python3 scripts/validate_regression_thresholds.py check",
		"python3 scripts/paired_benchmark_gate.py sample",
		1,
	)
	stderr := runAuditLocalOnly(
		t,
		workflow,
		readRepoFile(t, filepath.Join(".github", "workflows", "codeql.yml")),
		true,
	)
	if !strings.Contains(stderr, "unauthorized benchmark sampler, comparator, or paired gate") {
		t.Fatalf("expected premature paired gate-switch error, got:\n%s", stderr)
	}
}

func TestAuditCIEnforcementRejectsPrematurePairedBenchmarkDependency(t *testing.T) {
	workflow := readRepoFile(t, filepath.Join(".github", "workflows", "ci.yml"))
	workflow = strings.Replace(
		workflow,
		"needs: [quality, correctness-matrix, integration-stress, integration-long-run, adversarial, smoke, legacy-compatibility, benchmark-integrity, benchmark-timing-advisory, cross-platform, vulnerability]",
		"needs: [quality, correctness-matrix, integration-stress, integration-long-run, adversarial, smoke, legacy-compatibility, benchmark-integrity, benchmark-timing-advisory, benchmark-paired-decision, cross-platform, vulnerability]",
		1,
	)
	stderr := runAuditLocalOnly(
		t,
		workflow,
		readRepoFile(t, filepath.Join(".github", "workflows", "codeql.yml")),
		true,
	)
	if !strings.Contains(stderr, "required CI contains a premature paired benchmark job or dependency") {
		t.Fatalf("expected premature paired dependency error, got:\n%s", stderr)
	}
}

func TestAuditCIEnforcementRejectsPrematurePairedGovernanceFiles(t *testing.T) {
	workflow := readRepoFile(t, filepath.Join(".github", "workflows", "ci.yml"))
	codeqlWorkflow := readRepoFile(t, filepath.Join(".github", "workflows", "codeql.yml"))
	baselineWorkflow := readRepoFile(t, filepath.Join(".github", "workflows", "benchmark-baseline.yml"))
	for _, test := range []struct {
		name      string
		reference bool
		threshold bool
		message   string
	}{
		{name: "reference", reference: true, message: "paired reference manifest exists before governance authorization"},
		{name: "threshold", threshold: true, message: "paired threshold policy exists before threshold authorization"},
	} {
		t.Run(test.name, func(t *testing.T) {
			stderr := runAuditFixture(
				t,
				workflow,
				codeqlWorkflow,
				baselineWorkflow,
				true,
				test.reference,
				test.threshold,
			)
			if !strings.Contains(stderr, test.message) {
				t.Fatalf("expected %q, got:\n%s", test.message, stderr)
			}
		})
	}
}

func TestAuditCIEnforcementPairedLauncherConfidentialityAndLifecycle(t *testing.T) {
	compliant := `name: Temporary Paired Diagnostic
jobs:
  sample:
    timeout-minutes: 45
    strategy:
      matrix:
        include:
          - profile: none-w1
            dataset: ci-paired-w1-v2
          - profile: none-w4
            dataset: ci-paired-w4-v2
          - profile: zstd-w1
            dataset: ci-paired-w1-v2
          - profile: zstd-w4
            dataset: ci-paired-w4-v2
    steps:
      - name: Mask runner roots
        run: |
          set +x
          echo "::add-mask::$GITHUB_WORKSPACE"
          echo "::add-mask::$RUNNER_TEMP"
          echo "::add-mask::$HOME"
          echo '/github/workspace /github/runner_temp'
      - name: Sample
        run: |
          set +x
          token="$(openssl rand -hex 12)"
          echo "::add-mask::${token}"
          sensitive_root="$(mktemp -d "${RUNNER_TEMP}/paired.XXXXXXXX")"
          echo "::add-mask::${sensitive_root}"
          profile_parent="${GITHUB_WORKSPACE}/paired-evidence/${{ matrix.profile }}"
          profile_output="${profile_parent}/artifact"
          mkdir -p "${profile_parent}"
          test ! -e "${profile_output}"
          python3 scripts/paired_benchmark_gate.py sample \
            --dataset "${{ matrix.dataset }}" \
            --pairs 10 \
            --command-timeout-seconds 600 \
            --output-dir "${profile_output}"
      - name: Upload profile
        if: ${{ always() }}
        uses: actions/upload-artifact@v7
        with:
          path: paired-evidence/${{ matrix.profile }}/artifact
  decision:
    timeout-minutes: 10
    steps:
      - name: Decide
        run: |
          set +x
          decision_parent="${GITHUB_WORKSPACE}/paired-decision"
          decision_output="${decision_parent}/decision"
          mkdir -p "${decision_parent}"
          test ! -e "${decision_output}"
          python3 scripts/paired_benchmark_gate.py decision \
            --mode diagnostic \
            --output-dir "${decision_output}"
      - name: Upload decision
        if: ${{ always() }}
        uses: actions/upload-artifact@v7
        with:
          path: paired-decision/decision
`
	tests := []struct {
		name        string
		mutate      func(string) string
		wantFailure bool
		message     string
	}{
		{name: "parent-only creation", mutate: func(value string) string { return value }},
		{name: "distinct child per profile", mutate: func(value string) string { return value }},
		{name: "nonexistent decision child", mutate: func(value string) string { return value }},
		{name: "exact harness-owned upload", mutate: func(value string) string { return value }},
		{name: "platform aliases", mutate: func(value string) string { return value }},
		{name: "generated values masked", mutate: func(value string) string { return value }},
		{name: "no governance authority", mutate: func(value string) string { return value }},
		{
			name: "pre-created sample output",
			mutate: func(value string) string {
				return strings.Replace(value, "          test ! -e \"${profile_output}\"", "          touch \"${profile_output}\"\n          test ! -e \"${profile_output}\"", 1)
			},
			wantFailure: true,
			message:     "sample output must not be created before harness invocation",
		},
		{
			name: "pre-created decision output",
			mutate: func(value string) string {
				return strings.Replace(value, "          test ! -e \"${decision_output}\"", "          touch \"${decision_output}\"\n          test ! -e \"${decision_output}\"", 1)
			},
			wantFailure: true,
			message:     "decision output must not be created before harness invocation",
		},
		{
			name: "mkdir sample output",
			mutate: func(value string) string {
				return strings.Replace(value, "          test ! -e \"${profile_output}\"", "          mkdir -p \"${profile_output}\"\n          test ! -e \"${profile_output}\"", 1)
			},
			wantFailure: true,
			message:     "sample output must not be created before harness invocation",
		},
		{
			name: "mkdir decision output",
			mutate: func(value string) string {
				return strings.Replace(value, "          test ! -e \"${decision_output}\"", "          mkdir -p \"${decision_output}\"\n          test ! -e \"${decision_output}\"", 1)
			},
			wantFailure: true,
			message:     "decision output must not be created before harness invocation",
		},
		{
			name: "install output",
			mutate: func(value string) string {
				return strings.Replace(value, "          test ! -e \"${profile_output}\"", "          install -d \"${profile_output}\"\n          test ! -e \"${profile_output}\"", 1)
			},
			wantFailure: true,
			message:     "sample output must not be created before harness invocation",
		},
		{
			name: "checkout into output",
			mutate: func(value string) string {
				needle := "      - name: Sample\n"
				checkout := "      - name: Unsafe checkout\n        uses: actions/checkout@v6\n        with:\n          path: paired-evidence/${{ matrix.profile }}/artifact\n"
				return strings.Replace(value, needle, checkout+needle, 1)
			},
			wantFailure: true,
			message:     "sample output must not be an actions/checkout destination",
		},
		{
			name: "extract into output",
			mutate: func(value string) string {
				return strings.Replace(value, "          test ! -e \"${profile_output}\"", "          tar -xf evidence.tar -C \"${profile_output}\"\n          test ! -e \"${profile_output}\"", 1)
			},
			wantFailure: true,
			message:     "sample output must not be populated, checked out, extracted, or recreated",
		},
		{
			name: "missing nonexistence assertion",
			mutate: func(value string) string {
				return strings.Replace(value, "          test ! -e \"${profile_output}\"\n", "", 1)
			},
			wantFailure: true,
			message:     "sample output requires an exact nonexistence assertion",
		},
		{
			name: "upload mismatch",
			mutate: func(value string) string {
				return strings.Replace(value, "          path: paired-evidence/${{ matrix.profile }}/artifact", "          path: paired-evidence/${{ matrix.profile }}/different", 1)
			},
			wantFailure: true,
			message:     "sample upload path must equal the harness-owned output path",
		},
		{
			name: "shared matrix output",
			mutate: func(value string) string {
				return strings.Replace(value, "profile_parent=\"${GITHUB_WORKSPACE}/paired-evidence/${{ matrix.profile }}\"", "profile_parent=\"${GITHUB_WORKSPACE}/paired-evidence/shared\"", 1)
			},
			wantFailure: true,
			message:     "sample output must be distinct for every matrix profile",
		},
		{
			name: "workspace root output",
			mutate: func(value string) string {
				return strings.Replace(value, "profile_output=\"${profile_parent}/artifact\"", "profile_output=\"${GITHUB_WORKSPACE}\"", 1)
			},
			wantFailure: true,
			message:     "sample output must be a nonexistent child below a contained parent",
		},
		{
			name: "traversal output",
			mutate: func(value string) string {
				return strings.Replace(value, "profile_output=\"${profile_parent}/artifact\"", "profile_output=\"${profile_parent}/../artifact\"", 1)
			},
			wantFailure: true,
			message:     "sample output must not use traversal",
		},
		{
			name: "symlink output",
			mutate: func(value string) string {
				return strings.Replace(value, "          test ! -e \"${profile_output}\"", "          ln -s elsewhere \"${profile_output}\"\n          test ! -e \"${profile_output}\"", 1)
			},
			wantFailure: true,
			message:     "sample output must not be populated, checked out, extracted, or recreated",
		},
		{
			name: "delete and recreate output",
			mutate: func(value string) string {
				return strings.Replace(value, "          test ! -e \"${profile_output}\"", "          rm -rf \"${profile_output}\"\n          mkdir -p \"${profile_output}\"\n          test ! -e \"${profile_output}\"", 1)
			},
			wantFailure: true,
			message:     "sample output must not be created before harness invocation",
		},
		{
			name: "yaml env exposure",
			mutate: func(value string) string {
				return value + "        env:\n          DB_PASSWORD: exposed\n"
			},
			wantFailure: true,
			message:     "prohibited values through YAML env",
		},
		{
			name: "pre-mask service",
			mutate: func(value string) string {
				return strings.Replace(value, "    steps:\n", "    services:\n      postgres:\n        image: postgres:16\n    steps:\n", 1)
			},
			wantFailure: true,
			message:     "must provision its isolated container after masking",
		},
		{
			name: "outer timeout gap",
			mutate: func(value string) string {
				return strings.Replace(value, "timeout-minutes: 45", "timeout-minutes: 40", 1)
			},
			wantFailure: true,
			message:     "outer timeout is 45 minutes",
		},
		{
			name:        "runtime persistence",
			mutate:      func(value string) string { return value + "# GITHUB_ENV\n" },
			wantFailure: true,
			message:     "persists or traces sensitive runtime values",
		},
		{
			name: "generated path printed before masking",
			mutate: func(value string) string {
				return strings.Replace(value, "          echo \"::add-mask::${sensitive_root}\"", "          echo \"${sensitive_root}\"", 1)
			},
			wantFailure: true,
			message:     "generated dynamic paths and identifiers must be masked before printing",
		},
		{
			name: "production authority",
			mutate: func(value string) string {
				return strings.Replace(value, "--mode diagnostic", "--mode production", 1)
			},
			wantFailure: true,
			message:     "paired launcher must remain diagnostic-only",
		},
		{
			name: "threshold authority",
			mutate: func(value string) string {
				return value + "\n# threshold-policy-v1.13.json\n"
			},
			wantFailure: true,
			message:     "must not create manifest or threshold authority",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			output := runPairedLauncherAudit(t, tt.mutate(compliant), tt.wantFailure)
			if tt.message != "" && !strings.Contains(output, tt.message) {
				t.Fatalf("expected %q, got:\n%s", tt.message, output)
			}
		})
	}
}

func TestAuditCIEnforcementLocalWorkflowRequiresCrossPlatformSuccessAssertion(t *testing.T) {
	workflow := readRepoFile(t, filepath.Join(".github", "workflows", "ci.yml"))
	codeqlWorkflow := readRepoFile(t, filepath.Join(".github", "workflows", "codeql.yml"))
	workflow = strings.Replace(
		workflow,
		`             [ "${BENCHMARK_TIMING_ADVISORY_RESULT}" != "success" ] || \
             [ "${CROSS_PLATFORM_RESULT}" != "success" ] || \
             [ "${VULNERABILITY_RESULT}" != "success" ]; then`,
		`             [ "${BENCHMARK_TIMING_ADVISORY_RESULT}" != "success" ] || \
             [ "${VULNERABILITY_RESULT}" != "success" ]; then`,
		1,
	)

	stderr := runAuditLocalOnly(t, workflow, codeqlWorkflow, true)
	if !strings.Contains(stderr, "required gate rejects skipped cross-platform job") {
		t.Fatalf("expected missing cross-platform success assertion error, got:\n%s", stderr)
	}
}

func TestAuditCIEnforcementLocalWorkflowRequiresReleaseBranchPushTrigger(t *testing.T) {
	workflow := readRepoFile(t, filepath.Join(".github", "workflows", "ci.yml"))
	codeqlWorkflow := readRepoFile(t, filepath.Join(".github", "workflows", "codeql.yml"))
	workflow = strings.Replace(workflow, "      - release/**\n", "", 1)

	stderr := runAuditLocalOnly(t, workflow, codeqlWorkflow, true)
	if !strings.Contains(stderr, "CI push branch includes release/**") {
		t.Fatalf("expected missing CI release branch trigger error, got:\n%s", stderr)
	}
}

func TestAuditCIEnforcementLocalWorkflowRequiresWorkflowDispatch(t *testing.T) {
	workflow := readRepoFile(t, filepath.Join(".github", "workflows", "ci.yml"))
	codeqlWorkflow := readRepoFile(t, filepath.Join(".github", "workflows", "codeql.yml"))
	workflow = strings.Replace(workflow, "  workflow_dispatch:\n", "", 1)

	stderr := runAuditLocalOnly(t, workflow, codeqlWorkflow, true)
	if !strings.Contains(stderr, "CI workflow_dispatch trigger") {
		t.Fatalf("expected missing CI workflow_dispatch error, got:\n%s", stderr)
	}
}

func TestAuditCIEnforcementLocalCodeQLRequiresReleaseBranchPushTrigger(t *testing.T) {
	workflow := readRepoFile(t, filepath.Join(".github", "workflows", "ci.yml"))
	codeqlWorkflow := readRepoFile(t, filepath.Join(".github", "workflows", "codeql.yml"))
	codeqlWorkflow = strings.Replace(codeqlWorkflow, "      - release/**\n", "", 1)

	stderr := runAuditLocalOnly(t, workflow, codeqlWorkflow, true)
	if !strings.Contains(stderr, "CodeQL push branch includes release/**") {
		t.Fatalf("expected missing CodeQL release branch trigger error, got:\n%s", stderr)
	}
}

func TestAuditCIEnforcementLocalCodeQLRequiresWorkflowDispatch(t *testing.T) {
	workflow := readRepoFile(t, filepath.Join(".github", "workflows", "ci.yml"))
	codeqlWorkflow := readRepoFile(t, filepath.Join(".github", "workflows", "codeql.yml"))
	codeqlWorkflow = strings.Replace(codeqlWorkflow, "  workflow_dispatch:\n", "", 1)

	stderr := runAuditLocalOnly(t, workflow, codeqlWorkflow, true)
	if !strings.Contains(stderr, "CodeQL workflow_dispatch trigger") {
		t.Fatalf("expected missing CodeQL workflow_dispatch error, got:\n%s", stderr)
	}
}

func TestAuditCIEnforcementLocalWorkflowPassesCurrentConfiguration(t *testing.T) {
	workflow := readRepoFile(t, filepath.Join(".github", "workflows", "ci.yml"))
	codeqlWorkflow := readRepoFile(t, filepath.Join(".github", "workflows", "codeql.yml"))
	output := runAuditLocalOnly(t, workflow, codeqlWorkflow, false)
	if !strings.Contains(output, "[audit] PASSED") {
		t.Fatalf("expected audit pass output, got:\n%s", output)
	}
}

func TestAuditCIEnforcementSelectsAuthoritativeReleasePRHead(t *testing.T) {
	workflow := readRepoFile(t, filepath.Join(".github", "workflows", "ci.yml"))
	codeqlWorkflow := readRepoFile(t, filepath.Join(".github", "workflows", "codeql.yml"))
	releaseSHA := strings.TrimSpace(runAuditTestCommand(t, "git", "rev-parse", "HEAD"))

	t.Run("same repository main pull request passes exact head", func(t *testing.T) {
		probe, logPath := writeReleaseLinearityProbe(t)
		eventPath := writeReleasePullRequestEvent(t, "main", "release/v1.13.14", "franchoy/coldkeep", releaseSHA)
		setReleasePullRequestAuditEnvironment(t, probe, logPath, eventPath, "release/v1.13.14", "franchoy/coldkeep")

		output := runAuditLocalOnly(t, workflow, codeqlWorkflow, false)
		if !strings.Contains(output, "authoritative same-repository head "+releaseSHA) {
			t.Fatalf("authoritative PR identity proof omitted:\n%s", output)
		}
		calls, err := os.ReadFile(logPath)
		if err != nil {
			t.Fatalf("read release-linearity probe log: %v", err)
		}
		if !strings.Contains(string(calls), "--candidate-ref "+releaseSHA) {
			t.Fatalf("audit did not pass authoritative PR SHA to validator:\n%s", calls)
		}
	})

	tests := []struct {
		name       string
		baseRef    string
		headRef    string
		headRepo   string
		headSHA    string
		eventBody  string
		wantPhrase string
	}{
		{name: "wrong base", baseRef: "develop", headRef: "release/v1.13.14", headRepo: "franchoy/coldkeep", headSHA: releaseSHA, wantPhrase: "event identity is malformed"},
		{name: "wrong repository", baseRef: "main", headRef: "release/v1.13.14", headRepo: "fork/coldkeep", headSHA: releaseSHA, wantPhrase: "event identity is malformed"},
		{name: "mismatched head ref", baseRef: "main", headRef: "release/other", headRepo: "franchoy/coldkeep", headSHA: releaseSHA, wantPhrase: "event identity is malformed"},
		{name: "invalid SHA", baseRef: "main", headRef: "release/v1.13.14", headRepo: "franchoy/coldkeep", headSHA: "not-a-sha", wantPhrase: "event identity is malformed"},
		{name: "uppercase SHA", baseRef: "main", headRef: "release/v1.13.14", headRepo: "franchoy/coldkeep", headSHA: strings.ToUpper(releaseSHA), wantPhrase: "event identity is malformed"},
		{name: "missing SHA", eventBody: `{"pull_request":{"base":{"ref":"main"},"head":{"ref":"release/v1.13.14","repo":{"full_name":"franchoy/coldkeep"}}}}`, wantPhrase: "event identity is malformed"},
		{name: "non-string SHA", eventBody: `{"pull_request":{"base":{"ref":"main"},"head":{"ref":"release/v1.13.14","repo":{"full_name":"franchoy/coldkeep"},"sha":23}}}`, wantPhrase: "event identity is malformed"},
		{name: "unresolvable SHA", baseRef: "main", headRef: "release/v1.13.14", headRepo: "franchoy/coldkeep", headSHA: strings.Repeat("0", 40), wantPhrase: "does not resolve to a local commit"},
		{name: "malformed JSON", eventBody: "{", wantPhrase: "event identity is malformed"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			probe, logPath := writeReleaseLinearityProbe(t)
			eventPath := filepath.Join(t.TempDir(), "event.json")
			body := test.eventBody
			if body == "" {
				body = releasePullRequestEvent(test.baseRef, test.headRef, test.headRepo, test.headSHA)
			}
			if err := os.WriteFile(eventPath, []byte(body), 0o600); err != nil {
				t.Fatalf("write PR event fixture: %v", err)
			}
			setReleasePullRequestAuditEnvironment(t, probe, logPath, eventPath, "release/v1.13.14", "franchoy/coldkeep")
			output := runAuditLocalOnly(t, workflow, codeqlWorkflow, true)
			if !strings.Contains(output, test.wantPhrase) {
				t.Fatalf("expected %q, got:\n%s", test.wantPhrase, output)
			}
			if calls, err := os.ReadFile(logPath); err == nil && strings.TrimSpace(string(calls)) != "" {
				t.Fatalf("validator ran after invalid PR identity:\n%s", calls)
			}
		})
	}

	t.Run("missing event path fails closed", func(t *testing.T) {
		probe, logPath := writeReleaseLinearityProbe(t)
		setReleasePullRequestAuditEnvironment(t, probe, logPath, filepath.Join(t.TempDir(), "missing.json"), "release/v1.13.14", "franchoy/coldkeep")
		output := runAuditLocalOnly(t, workflow, codeqlWorkflow, true)
		if !strings.Contains(output, "requires a readable GITHUB_EVENT_PATH") {
			t.Fatalf("missing-event diagnostic omitted:\n%s", output)
		}
	})

	t.Run("missing repository fails closed", func(t *testing.T) {
		probe, logPath := writeReleaseLinearityProbe(t)
		eventPath := writeReleasePullRequestEvent(t, "main", "release/v1.13.14", "franchoy/coldkeep", releaseSHA)
		setReleasePullRequestAuditEnvironment(t, probe, logPath, eventPath, "release/v1.13.14", "")
		output := runAuditLocalOnly(t, workflow, codeqlWorkflow, true)
		if !strings.Contains(output, "requires GITHUB_REPOSITORY") {
			t.Fatalf("missing-repository diagnostic omitted:\n%s", output)
		}
	})

	t.Run("wrong event name fails closed", func(t *testing.T) {
		probe, logPath := writeReleaseLinearityProbe(t)
		eventPath := writeReleasePullRequestEvent(t, "main", "release/v1.13.14", "franchoy/coldkeep", releaseSHA)
		setReleasePullRequestAuditEnvironment(t, probe, logPath, eventPath, "release/v1.13.14", "franchoy/coldkeep")
		t.Setenv("GITHUB_EVENT_NAME", "push")
		output := runAuditLocalOnly(t, workflow, codeqlWorkflow, true)
		if !strings.Contains(output, "requires GITHUB_EVENT_NAME=pull_request") {
			t.Fatalf("wrong-event diagnostic omitted:\n%s", output)
		}
	})

	t.Run("ordinary release push uses HEAD", func(t *testing.T) {
		probe, logPath := writeReleaseLinearityProbe(t)
		t.Setenv("COLDKEEP_RELEASE_LINEARITY_VALIDATOR_FILE", probe)
		t.Setenv("PHASE23R_LINEAGE_PROBE_LOG", logPath)
		t.Setenv("GITHUB_EVENT_NAME", "push")
		t.Setenv("GITHUB_HEAD_REF", "")
		output := runAuditLocalOnly(t, workflow, codeqlWorkflow, false)
		if !strings.Contains(output, "[audit] PASSED") {
			t.Fatalf("ordinary release push audit failed:\n%s", output)
		}
		calls, err := os.ReadFile(logPath)
		if err != nil || !strings.Contains(string(calls), "--candidate-ref HEAD") {
			t.Fatalf("ordinary release push did not validate HEAD: err=%v calls=%q", err, calls)
		}
	})

	t.Run("release tag uses HEAD", func(t *testing.T) {
		probe, logPath := writeReleaseLinearityProbe(t)
		t.Setenv("COLDKEEP_RELEASE_LINEARITY_VALIDATOR_FILE", probe)
		t.Setenv("PHASE23R_LINEAGE_PROBE_LOG", logPath)
		t.Setenv("GITHUB_EVENT_NAME", "push")
		t.Setenv("GITHUB_HEAD_REF", "")
		t.Setenv("GITHUB_REF_TYPE", "tag")
		t.Setenv("GITHUB_REF_NAME", "v1.13.14")
		output := runAuditLocalOnly(t, workflow, codeqlWorkflow, false)
		if !strings.Contains(output, "[audit] PASSED") {
			t.Fatalf("release tag audit failed:\n%s", output)
		}
		calls, err := os.ReadFile(logPath)
		if err != nil || !strings.Contains(string(calls), "--candidate-ref HEAD") {
			t.Fatalf("release tag did not validate HEAD: err=%v calls=%q", err, calls)
		}
	})

	t.Run("non release pull request skips lineage", func(t *testing.T) {
		probe, logPath := writeReleaseLinearityProbe(t)
		eventPath := writeReleasePullRequestEvent(t, "main", "topic", "franchoy/coldkeep", releaseSHA)
		setReleasePullRequestAuditEnvironment(t, probe, logPath, eventPath, "topic", "franchoy/coldkeep")
		output := runAuditLocalOnly(t, workflow, codeqlWorkflow, false)
		if !strings.Contains(output, "real release-linearity check not required for context topic") {
			t.Fatalf("non-release PR skip proof omitted:\n%s", output)
		}
		if calls, err := os.ReadFile(logPath); err == nil && strings.TrimSpace(string(calls)) != "" {
			t.Fatalf("validator ran for non-release PR:\n%s", calls)
		}
	})
}

func writeReleaseLinearityProbe(t *testing.T) (string, string) {
	t.Helper()
	root := t.TempDir()
	probe := filepath.Join(root, "validate_release_linearity.sh")
	logPath := filepath.Join(root, "calls.log")
	source := `#!/usr/bin/env bash
set -euo pipefail
# merge-base "$candidate_commit" refs/remotes/origin/main
# rev-list --merges "${base}..${candidate_commit}"
printf '%s\n' "$*" >> "$PHASE23R_LINEAGE_PROBE_LOG"
`
	if err := os.WriteFile(probe, []byte(source), 0o700); err != nil {
		t.Fatalf("write release-linearity probe: %v", err)
	}
	return probe, logPath
}

func releasePullRequestEvent(baseRef, headRef, headRepo, headSHA string) string {
	return fmt.Sprintf(`{"pull_request":{"base":{"ref":%q},"head":{"ref":%q,"repo":{"full_name":%q},"sha":%q}}}`+"\n", baseRef, headRef, headRepo, headSHA)
}

func writeReleasePullRequestEvent(t *testing.T, baseRef, headRef, headRepo, headSHA string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "event.json")
	if err := os.WriteFile(path, []byte(releasePullRequestEvent(baseRef, headRef, headRepo, headSHA)), 0o600); err != nil {
		t.Fatalf("write release PR event fixture: %v", err)
	}
	return path
}

func setReleasePullRequestAuditEnvironment(t *testing.T, probe, logPath, eventPath, headRef, repository string) {
	t.Helper()
	t.Setenv("COLDKEEP_RELEASE_LINEARITY_VALIDATOR_FILE", probe)
	t.Setenv("PHASE23R_LINEAGE_PROBE_LOG", logPath)
	t.Setenv("GITHUB_EVENT_NAME", "pull_request")
	t.Setenv("GITHUB_EVENT_PATH", eventPath)
	t.Setenv("GITHUB_HEAD_REF", headRef)
	t.Setenv("GITHUB_REPOSITORY", repository)
}

func runAuditTestCommand(t *testing.T, name string, args ...string) string {
	t.Helper()
	cmd := exec.Command(name, args...)
	cmd.Dir = repoRoot(t)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("run %s %v: %v\n%s", name, args, err, output)
	}
	return string(output)
}

func TestAuditCIEnforcementRequiresPhase14ReleaseGateTooling(t *testing.T) {
	workflow := readRepoFile(t, filepath.Join(".github", "workflows", "ci.yml"))
	codeqlWorkflow := readRepoFile(t, filepath.Join(".github", "workflows", "codeql.yml"))

	for _, test := range []struct {
		name        string
		environment string
		message     string
	}{
		{name: "tracked source validator", environment: "COLDKEEP_SNAPSHOT_EVIDENCE_VALIDATOR_FILE", message: "tracked-source snapshot evidence validator must be an executable"},
		{name: "release linearity validator", environment: "COLDKEEP_RELEASE_LINEARITY_VALIDATOR_FILE", message: "branch-relative release-linearity validator must be an executable"},
		{name: "benchmark lifecycle", environment: "COLDKEEP_RELEASE_BENCHMARK_EVIDENCE_FILE", message: "release benchmark evidence lifecycle validator must be an executable"},
		{name: "benchmark runner", environment: "COLDKEEP_RELEASE_BENCHMARK_RUNNER_FILE", message: "external-transient release benchmark evidence runner must be an executable"},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Setenv(test.environment, filepath.Join(t.TempDir(), "missing"))
			output := runAuditLocalOnly(t, workflow, codeqlWorkflow, true)
			if !strings.Contains(output, test.message) {
				t.Fatalf("expected %q, got:\n%s", test.message, output)
			}
		})
	}
}

func TestAuditCIEnforcementRejectsPhase18RequiredProofMutations(t *testing.T) {
	workflow := readRepoFile(t, filepath.Join(".github", "workflows", "ci.yml"))
	codeqlWorkflow := readRepoFile(t, filepath.Join(".github", "workflows", "codeql.yml"))
	tests := []struct {
		name        string
		anchor      string
		old         string
		replacement string
		wantMessage string
		sourcePath  string
		sourceEnv   string
	}{
		{
			name:        "SQLite plain package command",
			anchor:      "      - name: Test packages (plain codec)\n",
			old:         "go test -race -count=1 ./cmd/... ./internal/...",
			replacement: "go test -race -count=1 ./internal/...",
			wantMessage: "SQLite quality plain package command",
		},
		{
			name:        "SQLite AES-GCM package command",
			anchor:      "      - name: Test packages (aes-gcm codec)\n",
			old:         "go test -race -count=1 ./cmd/... ./internal/...",
			replacement: "go test -race -count=1 ./cmd/...",
			wantMessage: "SQLite quality AES-GCM package command",
		},
		{
			name:        "Phase 17 PostgreSQL mutation marker",
			anchor:      "      - name: Run required PostgreSQL internal package contracts\n",
			old:         "TestMutationRowsAffectedContractAcrossBackends/postgres",
			replacement: "TestMutationRowsAffectedContractAcrossBackends/sqlite",
			wantMessage: "PostgreSQL internal package contracts prove Phase 17 mutation-cardinality execution",
		},
		{
			name:        "v1.13.12 PostgreSQL catalog planning marker",
			anchor:      "      - name: Run required PostgreSQL internal package contracts\n",
			old:         "TestCatalogContractRestorePlansAcrossBackends/postgres",
			replacement: "TestCatalogContractRestorePlansAcrossBackends/sqlite",
			wantMessage: "PostgreSQL internal package contracts prove catalog restore-plan execution",
		},
		{
			name:        "v1.13.12 PostgreSQL engine Doctor marker",
			anchor:      "      - name: Run required PostgreSQL internal package contracts\n",
			old:         "TestEngineDoctorAcrossBackends/postgres",
			replacement: "TestEngineDoctorAcrossBackends/sqlite",
			wantMessage: "PostgreSQL internal package contracts prove engine Doctor execution",
		},
		{
			name:        "Unix native contention source",
			old:         "func TestNativeLockContentionAndReacquire",
			replacement: "func removedNativeLockContentionAndReacquire",
			wantMessage: "Unix native coordination source retains contention runtime test",
			sourcePath:  filepath.Join("internal", "coordination", "native_lock_unix_test.go"),
			sourceEnv:   "COLDKEEP_NATIVE_UNIX_TEST_FILE",
		},
		{
			name:        "Windows native contention source",
			old:         "func TestWindowsNativeLockContentionAndReacquire",
			replacement: "func removedWindowsNativeLockContentionAndReacquire",
			wantMessage: "Windows native coordination source retains contention runtime test",
			sourcePath:  filepath.Join("internal", "coordination", "native_lock_windows_test.go"),
			sourceEnv:   "COLDKEEP_NATIVE_WINDOWS_TEST_FILE",
		},
		{
			name:        "production Coordinator source",
			old:         "func TestProductionCoordinatorsShareProcessRegistryAndProtectSuccessor",
			replacement: "func removedProductionCoordinatorsShareProcessRegistryAndProtectSuccessor",
			wantMessage: "production Coordinator source retains registry and successor runtime test",
			sourcePath:  filepath.Join("internal", "coordination", "coordinator_native_test.go"),
			sourceEnv:   "COLDKEEP_COORDINATOR_NATIVE_TEST_FILE",
		},
		{
			name:        "correctness DB gate",
			anchor:      "      - name: Run integration tests (correctness tier)\n",
			old:         "COLDKEEP_TEST_DB: 1",
			replacement: "COLDKEEP_TEST_DB: 0",
			wantMessage: "integration correctness execution proof enables DB gate",
		},
		{
			name:        "correctness JSON command",
			anchor:      "      - name: Run integration tests (correctness tier)\n",
			old:         "go test -race -count=1 -short -json ./tests/integration/...",
			replacement: "go test -race -count=1 -short ./tests/integration/...",
			wantMessage: "integration correctness execution proof uses JSON evidence",
		},
		{
			name:        "storage round-trip marker",
			anchor:      "      - name: Run integration tests (correctness tier)\n",
			old:         "TestRoundTripStoreRestore",
			replacement: "RemovedRoundTripMarker",
			wantMessage: "required PostgreSQL storage round-trip execution proof",
		},
		{
			name:        "storage remove marker",
			anchor:      "      - name: Run integration tests (correctness tier)\n",
			old:         "TestRemoveWithSharedChunksRefCount",
			replacement: "RemovedSharedChunkMarker",
			wantMessage: "required PostgreSQL storage remove execution proof",
		},
		{
			name:        "startup recovery marker",
			anchor:      "      - name: Run integration tests (correctness tier)\n",
			old:         "TestStartupRecoveryResyncsPreexistingQuarantinedOrphanConflictState",
			replacement: "TestStartupRecoveryMarkerRemoved",
			wantMessage: "required PostgreSQL recovery execution proof",
		},
		{
			name:        "correctness plain codec scope",
			anchor:      "      - name: Run integration tests (correctness tier)\n",
			old:         "if codec == \"plain\":",
			replacement: "if codec == \"unused\":",
			wantMessage: "integration correctness execution proof scopes recovery and remove markers to plain codec",
		},
		{
			name:        "correctness package binding",
			anchor:      "      - name: Run integration tests (correctness tier)\n",
			old:         "github.com/franchoy/coldkeep/tests/integration",
			replacement: "github.com/franchoy/coldkeep/tests/adversarial",
			wantMessage: "integration correctness execution proof binds the integration package",
		},
		{
			name:        "correctness malformed JSON rejection",
			anchor:      "      - name: Run integration tests (correctness tier)\n",
			old:         "json.loads(raw_line)",
			replacement: "{}",
			wantMessage: "integration correctness execution proof rejects malformed JSON",
		},
		{
			name:        "correctness empty JSON rejection",
			anchor:      "      - name: Run integration tests (correctness tier)\n",
			old:         "if not events:",
			replacement: "if False:",
			wantMessage: "integration correctness execution proof rejects empty JSON",
		},
		{
			name:        "correctness skip rejection",
			anchor:      "      - name: Run integration tests (correctness tier)\n",
			old:         "event.get(\"Action\") == \"skip\"",
			replacement: "False",
			wantMessage: "integration correctness execution proof rejects required skips",
		},
		{
			name:        "correctness pass requirement",
			anchor:      "      - name: Run integration tests (correctness tier)\n",
			old:         "event.get(\"Action\") == \"pass\"",
			replacement: "event.get(\"Action\") == \"output\"",
			wantMessage: "integration correctness execution proof requires pass events",
		},
		{
			name:        "correctness parser diagnostic",
			anchor:      "      - name: Run integration tests (correctness tier)\n",
			old:         "print(\"required execution-proof failure:\", file=sys.stderr)",
			replacement: "print(\"execution proof failed\", file=sys.stderr)",
			wantMessage: "integration correctness execution-proof parser",
		},
		{
			name:        "correctness test status",
			anchor:      "      - name: Run integration tests (correctness tier)\n",
			old:         "status=${PIPESTATUS[0]}",
			replacement: "status=0",
			wantMessage: "integration correctness execution proof preserves test status",
		},
		{
			name:        "correctness parser status",
			anchor:      "      - name: Run integration tests (correctness tier)\n",
			old:         "status=$?",
			replacement: "status=0",
			wantMessage: "integration correctness execution proof propagates parser status",
		},
		{
			name:        "correctness blocking exit",
			anchor:      "      - name: Run integration tests (correctness tier)\n",
			old:         "exit \"$status\"",
			replacement: "exit 0",
			wantMessage: "integration correctness execution proof remains blocking",
		},
		{
			name:        "correctness broad failure suppression",
			anchor:      "      - name: Run integration tests (correctness tier)\n",
			old:         "        env:\n",
			replacement: "        continue-on-error: true\n        env:\n",
			wantMessage: "integration correctness execution-proof step must not suppress broad failures",
		},
		{
			name:        "adversarial Linux runner",
			anchor:      "  adversarial:\n",
			old:         "runs-on: ubuntu-latest",
			replacement: "runs-on: macos-latest",
			wantMessage: "adversarial coordination proof runs on Linux",
		},
		{
			name:        "adversarial PostgreSQL service",
			anchor:      "  adversarial:\n",
			old:         "image: postgres:16",
			replacement: "image: postgres:15",
			wantMessage: "adversarial job pins postgres service image",
		},
		{
			name:        "adversarial DB gate",
			anchor:      "      - name: Run adversarial validation (G1–G17)\n",
			old:         "COLDKEEP_TEST_DB: 1",
			replacement: "COLDKEEP_TEST_DB: 0",
			wantMessage: "adversarial coordination proof enables DB gate",
		},
		{
			name:        "adversarial long-run gate",
			anchor:      "      - name: Run adversarial validation (G1–G17)\n",
			old:         "COLDKEEP_LONG_RUN: 1",
			replacement: "COLDKEEP_LONG_RUN: 0",
			wantMessage: "adversarial coordination proof enables long-run gate",
		},
		{
			name:        "adversarial JSON command",
			anchor:      "      - name: Run adversarial validation (G1–G17)\n",
			old:         "go test -race -count=1 -json ./tests/adversarial/...",
			replacement: "go test -race -count=1 ./tests/adversarial/...",
			wantMessage: "adversarial coordination proof uses JSON execution evidence",
		},
		{
			name:        "independent-process plain marker",
			anchor:      "      - name: Run adversarial validation (G1–G17)\n",
			old:         "TestAdversarialG6IndependentProcessRepositoryContention/plain",
			replacement: "TestAdversarialG6IndependentProcessRepositoryContention/removed",
			wantMessage: "independent-process plain execution proof",
		},
		{
			name:        "independent-process AES-GCM marker",
			anchor:      "      - name: Run adversarial validation (G1–G17)\n",
			old:         "TestAdversarialG6IndependentProcessRepositoryContention/aes-gcm",
			replacement: "TestAdversarialG6IndependentProcessRepositoryContention/removed",
			wantMessage: "independent-process AES-GCM execution proof",
		},
		{
			name:        "killed-holder marker",
			anchor:      "      - name: Run adversarial validation (G1–G17)\n",
			old:         "TestAdversarialG6KilledLeaseHolderReleasesRepository",
			replacement: "TestAdversarialG6KilledHolderMarkerRemoved",
			wantMessage: "killed-holder execution proof",
		},
		{
			name:        "live-GC marker",
			anchor:      "      - name: Run adversarial validation (G1–G17)\n",
			old:         "TestAdversarialG6LiveGCExcludesIndependentStoreProcess",
			replacement: "TestAdversarialG6LiveGCMarkerRemoved",
			wantMessage: "live-GC execution proof",
		},
		{
			name:        "adversarial package binding",
			anchor:      "      - name: Run adversarial validation (G1–G17)\n",
			old:         "github.com/franchoy/coldkeep/tests/adversarial",
			replacement: "github.com/franchoy/coldkeep/tests/integration",
			wantMessage: "adversarial coordination execution proof binds the adversarial package",
		},
		{
			name:        "adversarial malformed JSON rejection",
			anchor:      "      - name: Run adversarial validation (G1–G17)\n",
			old:         "json.loads(raw_line)",
			replacement: "{}",
			wantMessage: "adversarial coordination execution proof rejects malformed JSON",
		},
		{
			name:        "adversarial empty JSON rejection",
			anchor:      "      - name: Run adversarial validation (G1–G17)\n",
			old:         "if not events:",
			replacement: "if False:",
			wantMessage: "adversarial coordination execution proof rejects empty JSON",
		},
		{
			name:        "adversarial skip rejection",
			anchor:      "      - name: Run adversarial validation (G1–G17)\n",
			old:         "event.get(\"Action\") == \"skip\"",
			replacement: "False",
			wantMessage: "adversarial coordination execution proof rejects required skips",
		},
		{
			name:        "adversarial pass requirement",
			anchor:      "      - name: Run adversarial validation (G1–G17)\n",
			old:         "event.get(\"Action\") == \"pass\"",
			replacement: "event.get(\"Action\") == \"output\"",
			wantMessage: "adversarial coordination execution proof requires pass events",
		},
		{
			name:        "adversarial parser diagnostic",
			anchor:      "      - name: Run adversarial validation (G1–G17)\n",
			old:         "print(\"required execution-proof failure:\", file=sys.stderr)",
			replacement: "print(\"execution proof failed\", file=sys.stderr)",
			wantMessage: "adversarial coordination execution-proof parser",
		},
		{
			name:        "adversarial test status",
			anchor:      "      - name: Run adversarial validation (G1–G17)\n",
			old:         "status=${PIPESTATUS[0]}",
			replacement: "status=0",
			wantMessage: "adversarial coordination proof preserves test status",
		},
		{
			name:        "adversarial parser status",
			anchor:      "      - name: Run adversarial validation (G1–G17)\n",
			old:         "status=$?",
			replacement: "status=0",
			wantMessage: "adversarial coordination proof propagates parser status",
		},
		{
			name:        "adversarial blocking exit",
			anchor:      "      - name: Run adversarial validation (G1–G17)\n",
			old:         "exit \"$status\"",
			replacement: "exit 0",
			wantMessage: "adversarial coordination proof remains blocking",
		},
		{
			name:        "adversarial broad failure suppression",
			anchor:      "      - name: Run adversarial validation (G1–G17)\n",
			old:         "        id: adversarial_g1_g17\n",
			replacement: "        id: adversarial_g1_g17\n        continue-on-error: true\n",
			wantMessage: "adversarial coordination execution-proof step must not suppress broad failures",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if test.sourcePath != "" {
				source := readRepoFile(t, test.sourcePath)
				mutated := strings.Replace(source, test.old, test.replacement, 1)
				if mutated == source {
					t.Fatalf("source fixture %s did not contain %q", test.sourcePath, test.old)
				}
				stderr := runAuditLocalOnlyWithSourceFixture(t, workflow, codeqlWorkflow, test.sourceEnv, mutated)
				if !strings.Contains(stderr, test.wantMessage) {
					t.Fatalf("expected %q, got:\n%s", test.wantMessage, stderr)
				}
				return
			}
			anchorIndex := strings.Index(workflow, test.anchor)
			if anchorIndex < 0 {
				t.Fatalf("workflow fixture did not contain anchor %q", test.anchor)
			}
			targetOffset := strings.Index(workflow[anchorIndex:], test.old)
			if targetOffset < 0 {
				t.Fatalf("workflow fixture did not contain %q after anchor %q", test.old, test.anchor)
			}
			targetIndex := anchorIndex + targetOffset
			mutated := workflow[:targetIndex] + test.replacement + workflow[targetIndex+len(test.old):]
			stderr := runAuditLocalOnly(t, mutated, codeqlWorkflow, true)
			if !strings.Contains(stderr, test.wantMessage) {
				t.Fatalf("expected %q, got:\n%s", test.wantMessage, stderr)
			}
		})
	}
}

func TestAuditCIEnforcementLocalWorkflowRequiresDeterministicG6PostgresCommand(t *testing.T) {
	workflow := readRepoFile(t, filepath.Join(".github", "workflows", "ci.yml"))
	codeqlWorkflow := readRepoFile(t, filepath.Join(".github", "workflows", "codeql.yml"))
	workflow = strings.Replace(
		workflow,
		"          go test -v -race -count=1 ./tests/adversarial/... \\\n            -run '^TestAdversarialG6DeterministicStoreInterleavingPostgres$'\n",
		"",
		1,
	)

	stderr := runAuditLocalOnly(t, workflow, codeqlWorkflow, true)
	if !strings.Contains(stderr, "deterministic G6 PostgreSQL regression targets adversarial package explicitly") {
		t.Fatalf("expected missing deterministic G6 postgres command error, got:\n%s", stderr)
	}
}

func TestAuditCIEnforcementLocalWorkflowRequiresDeterministicG6DBGate(t *testing.T) {
	workflow := readRepoFile(t, filepath.Join(".github", "workflows", "ci.yml"))
	codeqlWorkflow := readRepoFile(t, filepath.Join(".github", "workflows", "codeql.yml"))
	workflow = strings.Replace(
		workflow,
		"      - name: Run deterministic G6 PostgreSQL interleaving regression\n        env:\n          COLDKEEP_TEST_DB: 1\n",
		"      - name: Run deterministic G6 PostgreSQL interleaving regression\n        env:\n",
		1,
	)

	stderr := runAuditLocalOnly(t, workflow, codeqlWorkflow, true)
	if !strings.Contains(stderr, "deterministic G6 PostgreSQL regression enables DB gate") {
		t.Fatalf("expected missing deterministic G6 postgres DB gate error, got:\n%s", stderr)
	}
}

func runAuditLocalOnlyWithSourceFixture(
	t *testing.T,
	workflow string,
	codeqlWorkflow string,
	sourceEnv string,
	source string,
) string {
	t.Helper()
	sourcePath := filepath.Join(t.TempDir(), "coordination_test.go")
	if err := os.WriteFile(sourcePath, []byte(source), 0o600); err != nil {
		t.Fatalf("write coordination source fixture: %v", err)
	}
	t.Setenv(sourceEnv, sourcePath)
	return runAuditLocalOnly(t, workflow, codeqlWorkflow, true)
}

func runAuditLocalOnly(t *testing.T, workflow string, codeqlWorkflow string, wantFailure bool) string {
	t.Helper()
	return runAuditLocalOnlyWithBaseline(
		t,
		workflow,
		codeqlWorkflow,
		readRepoFile(t, filepath.Join(".github", "workflows", "benchmark-baseline.yml")),
		wantFailure,
	)
}

func runPairedLauncherAudit(t *testing.T, launcher string, wantFailure bool) string {
	t.Helper()
	launcherPath := filepath.Join(t.TempDir(), "paired.yml")
	if err := os.WriteFile(launcherPath, []byte(launcher), 0o600); err != nil {
		t.Fatalf("write paired launcher fixture: %v", err)
	}
	cmd := exec.Command(
		"bash",
		"scripts/audit_ci_enforcement.sh",
		"--local-only",
		"--paired-launcher",
		launcherPath,
	)
	cmd.Dir = repoRoot(t)
	output, err := cmd.CombinedOutput()
	if wantFailure {
		if err == nil {
			t.Fatalf("expected paired launcher audit failure, got success:\n%s", output)
		}
		return string(output)
	}
	if err != nil {
		t.Fatalf("expected paired launcher audit success, got err=%v output:\n%s", err, output)
	}
	return string(output)
}

func runAuditLocalOnlyWithBaseline(
	t *testing.T,
	workflow string,
	codeqlWorkflow string,
	baselineWorkflow string,
	wantFailure bool,
) string {
	t.Helper()
	return runAuditFixture(
		t,
		workflow,
		codeqlWorkflow,
		baselineWorkflow,
		wantFailure,
		false,
		false,
	)
}

func runAuditFixture(
	t *testing.T,
	workflow string,
	codeqlWorkflow string,
	baselineWorkflow string,
	wantFailure bool,
	createPairedReference bool,
	createPairedThreshold bool,
) string {
	t.Helper()
	return runAuditFixtureWithTimingValidator(
		t,
		workflow,
		codeqlWorkflow,
		baselineWorkflow,
		readRepoFile(t, filepath.Join("scripts", "validate_regression_thresholds.py")),
		wantFailure,
		createPairedReference,
		createPairedThreshold,
	)
}

func runAuditFixtureWithTimingValidator(
	t *testing.T,
	workflow string,
	codeqlWorkflow string,
	baselineWorkflow string,
	timingValidator string,
	wantFailure bool,
	createPairedReference bool,
	createPairedThreshold bool,
) string {
	t.Helper()

	tmpDir := t.TempDir()
	workflowPath := filepath.Join(tmpDir, "ci.yml")
	codeqlWorkflowPath := filepath.Join(tmpDir, "codeql.yml")
	baselineWorkflowPath := filepath.Join(tmpDir, "benchmark-baseline.yml")
	timingValidatorPath := filepath.Join(tmpDir, "validate_regression_thresholds.py")
	matrixPath := filepath.Join(tmpDir, "VALIDATION_MATRIX.md")
	pairedReferencePath := filepath.Join(tmpDir, "reference-v1.13.json")
	pairedThresholdPath := filepath.Join(tmpDir, "threshold-policy-v1.13.json")

	if err := os.WriteFile(workflowPath, []byte(workflow), 0o600); err != nil {
		t.Fatalf("write workflow fixture: %v", err)
	}
	if err := os.WriteFile(codeqlWorkflowPath, []byte(codeqlWorkflow), 0o600); err != nil {
		t.Fatalf("write codeql workflow fixture: %v", err)
	}
	if err := os.WriteFile(baselineWorkflowPath, []byte(baselineWorkflow), 0o600); err != nil {
		t.Fatalf("write benchmark baseline workflow fixture: %v", err)
	}
	if err := os.WriteFile(timingValidatorPath, []byte(timingValidator), 0o600); err != nil {
		t.Fatalf("write timing validator fixture: %v", err)
	}
	if err := os.WriteFile(matrixPath, []byte(readRepoFile(t, "VALIDATION_MATRIX.md")), 0o600); err != nil {
		t.Fatalf("write validation matrix fixture: %v", err)
	}
	if createPairedReference {
		if err := os.WriteFile(pairedReferencePath, []byte("{}\n"), 0o600); err != nil {
			t.Fatalf("write paired reference fixture: %v", err)
		}
	}
	if createPairedThreshold {
		if err := os.WriteFile(pairedThresholdPath, []byte("{}\n"), 0o600); err != nil {
			t.Fatalf("write paired threshold fixture: %v", err)
		}
	}

	cmd := exec.Command("bash", "scripts/audit_ci_enforcement.sh", "--local-only")
	cmd.Dir = repoRoot(t)
	cmd.Env = append(os.Environ(),
		"COLDKEEP_CI_WORKFLOW_FILE="+workflowPath,
		"COLDKEEP_CODEQL_WORKFLOW_FILE="+codeqlWorkflowPath,
		"COLDKEEP_BENCHMARK_BASELINE_WORKFLOW_FILE="+baselineWorkflowPath,
		"COLDKEEP_TIMING_VALIDATOR_FILE="+timingValidatorPath,
		"COLDKEEP_VALIDATION_MATRIX_FILE="+matrixPath,
		"COLDKEEP_PAIRED_REFERENCE_MANIFEST_FILE="+pairedReferencePath,
		"COLDKEEP_PAIRED_THRESHOLD_POLICY_FILE="+pairedThresholdPath,
	)
	output, err := cmd.CombinedOutput()
	if wantFailure {
		if err == nil {
			t.Fatalf("expected audit failure, got success:\n%s", string(output))
		}
		return string(output)
	}
	if err != nil {
		t.Fatalf("expected audit success, got err=%v output:\n%s", err, string(output))
	}
	return string(output)
}

func readRepoFile(t *testing.T, relPath string) string {
	t.Helper()

	path, err := pathsafe.SafeJoin(repoRoot(t), filepath.ToSlash(relPath))
	if err != nil {
		t.Fatalf("resolve %s: %v", relPath, err)
	}
	relPathFromRoot, err := filepath.Rel(repoRoot(t), path)
	if err != nil {
		t.Fatalf("rel %s: %v", relPath, err)
	}
	content, err := fs.ReadFile(os.DirFS(repoRoot(t)), filepath.ToSlash(relPathFromRoot))
	if err != nil {
		t.Fatalf("read %s: %v", relPath, err)
	}
	return string(content)
}

func writeFakeCandidateLinter(t *testing.T) string {
	t.Helper()
	toolDir := filepath.Join(t.TempDir(), "candidate lint tools")
	if err := os.MkdirAll(toolDir, 0o700); err != nil {
		t.Fatalf("create fake candidate linter directory: %v", err)
	}
	path := filepath.Join(toolDir, "golangci-lint")
	content := `#!/usr/bin/env bash
set -euo pipefail
if [[ -n "${FAKE_LINT_CALL_LOG:-}" ]]; then
  printf '%s\n' "$*" >> "${FAKE_LINT_CALL_LOG}"
fi
case "${1:-}" in
  version)
    echo "golangci-lint has version 2.9.0 built with test"
    ;;
  config)
    case "${2:-}" in
      path)
        echo ".golangci.yml"
        ;;
      verify)
        ;;
      *)
        exit 2
        ;;
    esac
    ;;
  run)
    if [[ -n "${FAKE_LINT_OUTPUT:-}" ]]; then
      printf '%s\n' "${FAKE_LINT_OUTPUT}"
    fi
    exit "${FAKE_LINT_EXIT:-0}"
    ;;
  *)
    exit 2
    ;;
esac
`
	if err := os.WriteFile(path, []byte(content), 0o700); err != nil {
		t.Fatalf("write fake candidate linter: %v", err)
	}
	return path
}

func repoRoot(t *testing.T) string {
	t.Helper()

	root, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	return filepath.Dir(root)
}
