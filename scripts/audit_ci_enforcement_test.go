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

	tmpDir := t.TempDir()
	workflowPath := filepath.Join(tmpDir, "ci.yml")
	codeqlWorkflowPath := filepath.Join(tmpDir, "codeql.yml")
	baselineWorkflowPath := filepath.Join(tmpDir, "benchmark-baseline.yml")
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
