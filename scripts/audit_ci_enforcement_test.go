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
		"needs: [quality, correctness-matrix, integration-stress, integration-long-run, adversarial, smoke, legacy-compatibility, benchmark-matrix, cross-platform]",
		"needs: [quality, correctness-matrix, integration-stress, integration-long-run, adversarial, smoke, legacy-compatibility, benchmark-matrix]",
		1,
	)

	stderr := runAuditLocalOnly(t, workflow, codeqlWorkflow, true)
	if !strings.Contains(stderr, "required gate depends on all upstream jobs") {
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
	if !strings.Contains(stderr, "required CI switched to a replacement benchmark gate") {
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
	if !strings.Contains(stderr, "required CI switched to a replacement benchmark gate") {
		t.Fatalf("expected premature paired gate-switch error, got:\n%s", stderr)
	}
}

func TestAuditCIEnforcementRejectsPrematurePairedBenchmarkDependency(t *testing.T) {
	workflow := readRepoFile(t, filepath.Join(".github", "workflows", "ci.yml"))
	workflow = strings.Replace(
		workflow,
		"needs: [quality, correctness-matrix, integration-stress, integration-long-run, adversarial, smoke, legacy-compatibility, benchmark-matrix, cross-platform]",
		"needs: [quality, correctness-matrix, integration-stress, integration-long-run, adversarial, smoke, legacy-compatibility, benchmark-matrix, benchmark-paired-decision, cross-platform]",
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
    steps:
      - name: Mask runner roots
        run: |
          set +x
          echo "::add-mask::$GITHUB_WORKSPACE"
          echo "::add-mask::$RUNNER_TEMP"
          echo "::add-mask::$HOME"
      - name: Sample
        run: |
          python3 scripts/paired_benchmark_gate.py sample --dataset ci-paired-w1-v2 --pairs 10 --command-timeout-seconds 600
          python3 scripts/paired_benchmark_gate.py sample --dataset ci-paired-w4-v2 --pairs 10 --command-timeout-seconds 600
      - name: Decide
        run: python3 scripts/paired_benchmark_gate.py decision
      - name: Upload
        if: ${{ always() }}
        uses: actions/upload-artifact@v7
`
	tests := []struct {
		name        string
		mutate      func(string) string
		wantFailure bool
		message     string
	}{
		{name: "compliant", mutate: func(value string) string { return value }},
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
		`             [ "${BENCHMARK_RESULT}" != "success" ] || \
             [ "${CROSS_PLATFORM_RESULT}" != "success" ]; then`,
		`             [ "${BENCHMARK_RESULT}" != "success" ]; then`,
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
