package scripts_test

import (
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
	workflow = strings.Replace(
		workflow,
		"needs: [quality, correctness-matrix, integration-stress, integration-long-run, adversarial, smoke, legacy-compatibility, benchmark-integrity, benchmark-timing-advisory, cross-platform]",
		"needs: [quality, correctness-matrix, integration-stress, integration-long-run, adversarial, smoke, legacy-compatibility, benchmark-integrity, benchmark-timing-advisory]",
		1,
	)

	stderr := runAuditLocalOnly(t, workflow, codeqlWorkflow, true)
	if !strings.Contains(stderr, "required gate depends separately on benchmark integrity") {
		t.Fatalf("expected missing cross-platform dependency error, got:\n%s", stderr)
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

func TestAuditCIEnforcementRequiresPinnedBenchmarkCalibrationToolchain(t *testing.T) {
	workflow := readRepoFile(t, filepath.Join(".github", "workflows", "ci.yml"))
	codeqlWorkflow := readRepoFile(t, filepath.Join(".github", "workflows", "codeql.yml"))
	baselineWorkflow := readRepoFile(t, filepath.Join(".github", "workflows", "benchmark-baseline.yml"))
	baselineWorkflow = strings.Replace(baselineWorkflow, "go-version: '1.25.12'", "go-version: '1.25.x'", 1)

	stderr := runAuditLocalOnlyWithBaseline(t, workflow, codeqlWorkflow, baselineWorkflow, true)
	if !strings.Contains(stderr, "benchmark calibration pins the Go patch") {
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
			stderr := runAuditLocalOnlyWithBaseline(
				t,
				workflow,
				codeqlWorkflow,
				tt.mutate(baselineWorkflow),
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
			name: "required dependency removed", old: "benchmark-integrity, benchmark-timing-advisory, cross-platform",
			replacement: "benchmark-timing-advisory, cross-platform", message: "depends separately on benchmark integrity",
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
		"needs: [quality, correctness-matrix, integration-stress, integration-long-run, adversarial, smoke, legacy-compatibility, benchmark-integrity, benchmark-timing-advisory, cross-platform]",
		"needs: [quality, correctness-matrix, integration-stress, integration-long-run, adversarial, smoke, legacy-compatibility, benchmark-integrity, benchmark-timing-advisory, benchmark-paired-decision, cross-platform]",
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
             [ "${CROSS_PLATFORM_RESULT}" != "success" ]; then`,
		`             [ "${BENCHMARK_TIMING_ADVISORY_RESULT}" != "success" ]; then`,
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

func repoRoot(t *testing.T) string {
	t.Helper()

	root, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	return filepath.Dir(root)
}
