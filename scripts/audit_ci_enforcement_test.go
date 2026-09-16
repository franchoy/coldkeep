package scripts_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

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
	if !strings.Contains(stderr, "required gate depends on security and hosted reproducibility jobs") {
		t.Fatalf("expected missing cross-platform dependency error, got:\n%s", stderr)
	}
}

func TestAuditCIEnforcementRequiresRemoteExactCandidateInstallation(t *testing.T) {
	workflow := readRepoFile(t, filepath.Join(".github", "workflows", "ci.yml"))
	codeqlWorkflow := readRepoFile(t, filepath.Join(".github", "workflows", "codeql.yml"))

	tests := []struct {
		name        string
		mutate      func(string) string
		wantMessage string
	}{
		{
			name: "required-gate dependency",
			mutate: func(value string) string {
				return strings.Replace(value, "source-install, remote-candidate-install, product-container", "source-install, product-container", 1)
			},
			wantMessage: "required gate depends on security and hosted reproducibility jobs",
		},
		{
			name: "semantic tag selection",
			mutate: func(value string) string {
				return strings.Replace(
					value,
					"github.ref_type == 'tag' && github.ref_name || github.event.pull_request.head.sha || github.sha",
					"github.event.pull_request.head.sha || github.sha",
					1,
				)
			},
			wantMessage: "remote candidate installation selects semantic tags and exact non-tag revisions",
		},
		{
			name: "explicit tag event detection",
			mutate: func(value string) string {
				return strings.Replace(value, "EVENT_REF_TYPE: ${{ github.ref_type }}", "EVENT_REF_TYPE: branch", 1)
			},
			wantMessage: "remote candidate installation detects tag events explicitly",
		},
		{
			name: "event ref name",
			mutate: func(value string) string {
				return strings.Replace(value, "EVENT_REF_NAME: ${{ github.ref_name }}", "EVENT_REF_NAME: fixed", 1)
			},
			wantMessage: "remote candidate installation retains the triggering ref name",
		},
		{
			name: "public tag resolution",
			mutate: func(value string) string {
				return strings.ReplaceAll(value, "git ls-remote", "git remote")
			},
			wantMessage: "Unix remote installation resolves public tag and peeled refs independently",
		},
		{
			name: "peeled tag proof",
			mutate: func(value string) string {
				return strings.Replace(value, `peeled_ref="${tag_ref}^{}"`, `peeled_ref="${tag_ref}"`, 1)
			},
			wantMessage: "Unix remote installation requires a peeled public tag ref",
		},
		{
			name: "peeled tag origin authority",
			mutate: func(value string) string {
				return strings.Replace(value, `EXPECTED_ORIGIN_SHA="${PUBLIC_TAG_PEELED_COMMIT_SHA}"`, `EXPECTED_ORIGIN_SHA="${resolved_hash}"`, 1)
			},
			wantMessage: "Unix tag origin authority is the public peeled commit",
		},
		{
			name: "Unix origin equality",
			mutate: func(value string) string {
				return strings.Replace(value, `          test "${resolved_hash}" = "${EXPECTED_ORIGIN_SHA}"
`, "", 1)
			},
			wantMessage: "Unix module origin equals the expected source commit",
		},
		{
			name: "Windows origin equality",
			mutate: func(value string) string {
				return strings.Replace(value, `          if ($moduleInfo.Origin.Hash -ne $env:EXPECTED_ORIGIN_SHA) { throw 'candidate origin mismatch' }
`, "", 1)
			},
			wantMessage: "Windows module origin equals the expected source commit",
		},
		{
			name: "tag semantic version equality",
			mutate: func(value string) string {
				return strings.Replace(value, `            test "${resolved_version}" = "${EXPECTED_VERSION}"
`, "", 1)
			},
			wantMessage: "Unix tag path proves the resolved semantic version",
		},
		{
			name: "go list selected query",
			mutate: func(value string) string {
				return strings.Replace(value, `go list -m -json "${module}@${CANDIDATE_QUERY}"`, `go list -m -json "${module}@${TRIGGER_SHA}"`, 1)
			},
			wantMessage: "Unix module resolution uses the selected candidate query",
		},
		{
			name: "go install selected query",
			mutate: func(value string) string {
				return strings.Replace(value, `go install "${module}/cmd/coldkeep@${CANDIDATE_QUERY}"`, `go install "${module}/cmd/coldkeep@${TRIGGER_SHA}"`, 1)
			},
			wantMessage: "Unix public installation uses the selected candidate query",
		},
		{
			name: "checkout contamination",
			mutate: func(value string) string {
				return strings.Replace(
					value,
					"    steps:\n      - name: Setup Go\n        uses: actions/setup-go@v6\n        with:\n          go-version: '1.26.7'\n          check-latest: false\n          cache: false",
					"    steps:\n      - uses: actions/checkout@v6\n      - name: Setup Go\n        uses: actions/setup-go@v6\n        with:\n          go-version: '1.26.7'\n          check-latest: false\n          cache: false",
					1,
				)
			},
			wantMessage: "remote candidate installation must run outside a checkout and remain blocking",
		},
		{
			name: "continue on error",
			mutate: func(value string) string {
				return strings.Replace(value, "    timeout-minutes: 25\n\n    env:\n      EVENT_REF_TYPE:", "    timeout-minutes: 25\n    continue-on-error: true\n\n    env:\n      EVENT_REF_TYPE:", 1)
			},
			wantMessage: "remote candidate installation must run outside a checkout and remain blocking",
		},
		{
			name: "broad failure suppression",
			mutate: func(value string) string {
				return strings.Replace(value, "          cc --version\n\n          install_root=", "          cc --version || true\n\n          install_root=", 1)
			},
			wantMessage: "remote candidate installation must run outside a checkout and remain blocking",
		},
		{
			name: "required-gate result",
			mutate: func(value string) string {
				return strings.Replace(value, "             [ \"${REMOTE_CANDIDATE_INSTALL_RESULT}\" != \"success\" ] || \\\n", "", 1)
			},
			wantMessage: "required gate rejects skipped remote-candidate-install job",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mutated := tt.mutate(workflow)
			if mutated == workflow {
				t.Fatal("remote exact-candidate fixture was not mutated")
			}
			stderr := runAuditLocalOnly(t, mutated, codeqlWorkflow, true)
			if !strings.Contains(stderr, tt.wantMessage) {
				t.Fatalf("expected %q, got:\n%s", tt.wantMessage, stderr)
			}
		})
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
			name: "database provenance binding removed",
			mutate: func(value string) string {
				return strings.Replace(value, "--database-provenance \"${provenance_root}/database-provenance.before.json\"", "--database-provenance removed.json", 1)
			},
			wantMessage: "benchmark calibration binds sampling to retained database provenance",
		},
		{
			name: "post database observation removed",
			mutate: func(value string) string {
				index := strings.LastIndex(value, "database-provenance collect")
				if index < 0 {
					return value
				}
				return value[:index] + "database-provenance validate" + value[index+len("database-provenance collect"):]
			},
			wantMessage: "benchmark calibration must retain exactly two pre/post database observations",
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
			name: "integrity provenance binding removed", old: "--database-provenance \"${output_parent}/database-provenance.before.json\"",
			replacement: "--database-provenance removed.json", message: "integrity matrix binds execution to retained database provenance",
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
			replacement: "benchmark-timing-advisory, cross-platform, vulnerability", message: "required gate depends on security and hosted reproducibility jobs",
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
	mutated := strings.Replace(
		workflow,
		"benchmark-integrity, benchmark-timing-advisory, cross-platform",
		"benchmark-integrity, benchmark-timing-advisory, benchmark-paired-decision, cross-platform",
		1,
	)
	if mutated == workflow {
		t.Fatal("required-gate paired-dependency fixture was not inserted")
	}
	stderr := runAuditLocalOnly(
		t,
		mutated,
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
	mutated := strings.Replace(
		workflow,
		"             [ \"${CROSS_PLATFORM_RESULT}\" != \"success\" ] || \\\n",
		"",
		1,
	)
	if mutated == workflow {
		t.Fatal("required-gate cross-platform success assertion fixture was not removed")
	}

	stderr := runAuditLocalOnly(t, mutated, codeqlWorkflow, true)
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

type codeQLContractMutation struct {
	name        string
	old         string
	replacement string
	wantMessage string
}

func TestAuditCIEnforcementRequiresTagCodeQLContract(t *testing.T) {
	runCodeQLContractMutations(t, []codeQLContractMutation{
		{name: "tag trigger", old: "      - v*\n", wantMessage: "CodeQL push tags include v*"},
		{name: "main trigger", old: "      - main\n", wantMessage: "CodeQL push branch retains main"},
		{name: "release trigger", old: "      - release/**\n", wantMessage: "CodeQL push branch includes release/**"},
		{name: "actions language", old: "          - language: actions\n            build-mode: none\n", wantMessage: "CodeQL retains actions analysis"},
		{name: "Go language", old: "          - language: go\n            build-mode: autobuild\n", wantMessage: "CodeQL retains Go analysis"},
		{name: "Python language", old: "          - language: python\n            build-mode: none\n", wantMessage: "CodeQL retains Python analysis"},
		{name: "aggregate dependency", old: "    needs: [analyze]\n", wantMessage: "CodeQL aggregate depends on the language matrix"},
		{
			name:        "aggregate blocking",
			old:         "  aggregate:\n    name: CodeQL Aggregate\n    runs-on: ubuntu-latest\n",
			replacement: "  aggregate:\n    name: CodeQL Aggregate\n    runs-on: ubuntu-latest\n    continue-on-error: true\n",
			wantMessage: "CodeQL aggregate must remain blocking",
		},
	})
}

func TestAuditCIEnforcementRejectsScopedCodeQLTriggerConfusion(t *testing.T) {
	runCodeQLContractMutations(t, []codeQLContractMutation{
		{
			name: "main under wrong push mapping",
			old:  "    branches:\n      - main\n      - release/**\n    tags:\n      - v*\n",
			replacement: "    branches:\n      - release/**\n    tags:\n      - v*\n" +
				"      - main\n",
			wantMessage: "CodeQL push branch retains main",
		},
		{
			name: "release under pull request trigger",
			old: "    branches:\n      - main\n      - release/**\n    tags:\n      - v*\n" +
				"  pull_request:\n    branches:\n      - main\n",
			replacement: "    branches:\n      - main\n    tags:\n      - v*\n" +
				"  pull_request:\n    branches:\n      - main\n      - release/**\n",
			wantMessage: "CodeQL push branch includes release/**",
		},
		{
			name: "tag under wrong push mapping",
			old:  "    branches:\n      - main\n      - release/**\n    tags:\n      - v*\n",
			replacement: "    branches:\n      - main\n      - release/**\n      - v*\n" +
				"    tags:\n",
			wantMessage: "CodeQL push tags include v*",
		},
		{
			name:        "branches mapping",
			old:         "    branches:\n      - main\n      - release/**\n",
			wantMessage: "CodeQL push branch retains main",
		},
		{
			name:        "tags mapping",
			old:         "    tags:\n      - v*\n",
			wantMessage: "CodeQL push tags include v*",
		},
	})
}

func runCodeQLContractMutations(t *testing.T, tests []codeQLContractMutation) {
	t.Helper()
	workflow := readRepoFile(t, filepath.Join(".github", "workflows", "ci.yml"))
	codeqlWorkflow := readRepoFile(t, filepath.Join(".github", "workflows", "codeql.yml"))

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mutated := strings.Replace(codeqlWorkflow, tt.old, tt.replacement, 1)
			if mutated == codeqlWorkflow {
				t.Fatal("CodeQL tag-certification fixture was not mutated")
			}
			stderr := runAuditLocalOnly(t, workflow, mutated, true)
			if !strings.Contains(stderr, tt.wantMessage) {
				t.Fatalf("expected %q, got:\n%s", tt.wantMessage, stderr)
			}
		})
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
		auditRoot := cloneAuditRepositoryOnBranch(t, "release/v1.13.14")
		t.Setenv("COLDKEEP_AUDIT_TEST_REPO_ROOT", auditRoot)
		t.Setenv("COLDKEEP_RELEASE_LINEARITY_VALIDATOR_FILE", probe)
		t.Setenv("PHASE23R_LINEAGE_PROBE_LOG", logPath)
		t.Setenv("GITHUB_EVENT_NAME", "push")
		t.Setenv("GITHUB_HEAD_REF", "")
		t.Setenv("GITHUB_REF_TYPE", "branch")
		t.Setenv("GITHUB_REF_NAME", "release/v1.13.14")
		output := runAuditLocalOnly(t, workflow, codeqlWorkflow, false)
		if !strings.Contains(output, "[audit] PASSED") {
			t.Fatalf("ordinary release push audit failed:\n%s", output)
		}
		calls, err := os.ReadFile(logPath)
		if err != nil || !strings.Contains(string(calls), "--candidate-ref HEAD") {
			t.Fatalf("ordinary release push did not validate HEAD: err=%v calls=%q", err, calls)
		}
	})

	t.Run("recovery branch push skips release lineage", func(t *testing.T) {
		probe, logPath := writeReleaseLinearityProbe(t)
		branch := "recovery/v1.13.15-phase8-release-state"
		auditRoot := cloneAuditRepositoryOnBranch(t, branch)
		t.Setenv("COLDKEEP_AUDIT_TEST_REPO_ROOT", auditRoot)
		t.Setenv("COLDKEEP_RELEASE_LINEARITY_VALIDATOR_FILE", probe)
		t.Setenv("PHASE23R_LINEAGE_PROBE_LOG", logPath)
		t.Setenv("GITHUB_EVENT_NAME", "push")
		t.Setenv("GITHUB_HEAD_REF", "")
		t.Setenv("GITHUB_REF_TYPE", "branch")
		t.Setenv("GITHUB_REF_NAME", branch)
		output := runAuditLocalOnly(t, workflow, codeqlWorkflow, false)
		if !strings.Contains(output, "real release-linearity check not required for context "+branch) {
			t.Fatalf("recovery branch context proof omitted:\n%s", output)
		}
		if calls, err := os.ReadFile(logPath); err == nil && strings.TrimSpace(string(calls)) != "" {
			t.Fatalf("release-linearity validator ran for recovery branch:\n%s", calls)
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
	t.Setenv("GITHUB_REF_TYPE", "branch")
	t.Setenv("GITHUB_REF_NAME", "115/merge")
}

func cloneAuditRepositoryOnBranch(t *testing.T, branch string) string {
	t.Helper()
	root := filepath.Join(t.TempDir(), "repository")
	clone := exec.Command("git", "clone", "--no-hardlinks", "--quiet", repoRoot(t), root)
	if output, err := clone.CombinedOutput(); err != nil {
		t.Fatalf("clone audit fixture repository: %v\n%s", err, output)
	}
	checkout := exec.Command("git", "-C", root, "checkout", "--quiet", "-B", branch)
	if output, err := checkout.CombinedOutput(); err != nil {
		t.Fatalf("create audit fixture branch %s: %v\n%s", branch, err, output)
	}
	auditPath := filepath.Join(root, "scripts", "audit_ci_enforcement.sh")
	if err := os.WriteFile(auditPath, []byte(readRepoFile(t, filepath.Join("scripts", "audit_ci_enforcement.sh"))), 0o755); err != nil {
		t.Fatalf("install current CI audit in cloned repository fixture: %v", err)
	}
	return root
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
			old:         "TestRoundTripStoreRestore",
			replacement: "RemovedRoundTripMarker",
			wantMessage: "required-event profiles preserve storage round-trip proof",
			sourcePath:  filepath.Join("scripts", "check_required_test_events.py"),
			sourceEnv:   "COLDKEEP_REQUIRED_TEST_EVENTS_FILE",
		},
		{
			name:        "storage remove marker",
			old:         "TestRemoveWithSharedChunksRefCount",
			replacement: "RemovedSharedChunkMarker",
			wantMessage: "required-event profiles preserve remove proof",
			sourcePath:  filepath.Join("scripts", "check_required_test_events.py"),
			sourceEnv:   "COLDKEEP_REQUIRED_TEST_EVENTS_FILE",
		},
		{
			name:        "startup recovery marker",
			old:         "TestStartupRecoveryResyncsPreexistingQuarantinedOrphanConflictState",
			replacement: "TestStartupRecoveryMarkerRemoved",
			wantMessage: "required-event profiles preserve recovery proof",
			sourcePath:  filepath.Join("scripts", "check_required_test_events.py"),
			sourceEnv:   "COLDKEEP_REQUIRED_TEST_EVENTS_FILE",
		},
		{
			name:        "correctness profile selection",
			anchor:      "      - name: Run integration tests (correctness tier)\n",
			old:         "integration-correctness-${{ matrix.codec }}",
			replacement: "integration-correctness-wrong",
			wantMessage: "integration correctness selects codec-specific profile",
		},
		{
			name:        "correctness package binding",
			old:         "github.com/franchoy/coldkeep/tests/integration",
			replacement: "github.com/franchoy/coldkeep/tests/adversarial",
			wantMessage: "required-event profiles bind integration package",
			sourcePath:  filepath.Join("scripts", "check_required_test_events.py"),
			sourceEnv:   "COLDKEEP_REQUIRED_TEST_EVENTS_FILE",
		},
		{
			name:        "correctness stderr separation",
			anchor:      "      - name: Run integration tests (correctness tier)\n",
			old:         "2>\"$stderr_file\" | tee \"$json_file\"",
			replacement: "| tee \"$json_file\"",
			wantMessage: "integration correctness keeps stderr separate from JSON evidence",
		},
		{
			name:        "correctness pipeline status snapshot",
			anchor:      "      - name: Run integration tests (correctness tier)\n",
			old:         "pipeline_status=(\"${PIPESTATUS[@]}\")",
			replacement: "pipeline_status=(0 0)",
			wantMessage: "integration correctness snapshots complete pipeline status",
		},
		{
			name:        "correctness Go status",
			anchor:      "      - name: Run integration tests (correctness tier)\n",
			old:         "go_status=${pipeline_status[0]}",
			replacement: "go_status=0",
			wantMessage: "integration correctness preserves Go status",
		},
		{
			name:        "correctness capture status",
			anchor:      "      - name: Run integration tests (correctness tier)\n",
			old:         "capture_status=${pipeline_status[1]}",
			replacement: "capture_status=0",
			wantMessage: "integration correctness preserves evidence-capture status",
		},
		{
			name:        "correctness checker invocation",
			anchor:      "      - name: Run integration tests (correctness tier)\n",
			old:         "python3 scripts/check_required_test_events.py",
			replacement: "python3 scripts/removed_required_test_events.py",
			wantMessage: "integration correctness invokes required-event checker",
		},
		{
			name:        "correctness checker status",
			anchor:      "      - name: Run integration tests (correctness tier)\n",
			old:         "checker_status=$?",
			replacement: "checker_status=0",
			wantMessage: "integration correctness preserves checker status",
		},
		{
			name:        "correctness capture failure propagation",
			anchor:      "      - name: Run integration tests (correctness tier)\n",
			old:         "elif [ \"$capture_status\" -ne 0 ]; then",
			replacement: "elif false; then",
			wantMessage: "integration correctness propagates evidence-capture failure",
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

func TestAuditCIEnforcementRequiresCK014RequiredProofWiring(t *testing.T) {
	workflow := readRepoFile(t, filepath.Join(".github", "workflows", "ci.yml"))
	codeqlWorkflow := readRepoFile(t, filepath.Join(".github", "workflows", "codeql.yml"))
	tests := []struct {
		name        string
		old         string
		replacement string
		wantMessage string
	}{
		{
			name:        "missing internal checker step",
			old:         "      - name: Run CK-014 internal verification proofs\n",
			replacement: "      - name: Removed CK-014 internal verification proofs\n",
			wantMessage: "missing CK-014 internal verification proof step",
		},
		{
			name:        "internal proof wrong codec leg",
			old:         "      - name: Run CK-014 internal verification proofs\n        if: ${{ matrix.codec == 'plain' }}\n",
			replacement: "      - name: Run CK-014 internal verification proofs\n        if: ${{ matrix.codec == 'aes-gcm' }}\n",
			wantMessage: "CK-014 internal proofs run only in plain correctness leg",
		},
		{
			name:        "internal proof missing aggregation selector",
			old:         "TestVerifySystemDeepCollectsTwoInjectedDownstreamPhysicalFaults",
			replacement: "RemovedTwoFaultAggregationProof",
			wantMessage: "CK-014 downstream aggregation proof selector",
		},
		{
			name:        "internal capture failure suppressed",
			old:         "elif [ \"$capture_status\" -ne 0 ]; then\n            status=$capture_status",
			replacement: "elif false; then\n            status=0",
			wantMessage: "CK-014 internal proofs propagate evidence-capture failure",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
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

func TestAuditCIEnforcementRequiresCK015NamedProofWiring(t *testing.T) {
	workflow := readRepoFile(t, filepath.Join(".github", "workflows", "ci.yml"))
	codeqlWorkflow := readRepoFile(t, filepath.Join(".github", "workflows", "codeql.yml"))
	checklist := readRepoFile(t, "PRE_RELEASE_CHECKLIST.md")
	checker := readRepoFile(t, filepath.Join("scripts", "check_required_test_events.py"))

	controlOutput := withCK015CoreAuditCase(t, "00-control", func() string {
		return runAuditLocalOnlyWithChecklistFixture(t, workflow, codeqlWorkflow, checklist, false)
	})
	for _, slot := range []string{"hosted-sqlite", "hosted-postgres", "local-sqlite", "local-postgres"} {
		if !strings.Contains(controlOutput, "matches frozen approved wrapper body ("+slot+")") {
			t.Fatalf("unchanged control did not prove %s identity:\n%s", slot, controlOutput)
		}
	}

	tests := []struct {
		name        string
		source      string
		anchor      string
		old         string
		replacement string
		wantMessage string
	}{
		{
			name: "hosted SQLite step removed", source: "workflow",
			old:         "      - name: Run CK-015 SQLite initial-lookup proofs\n",
			replacement: "      - name: Removed CK-015 SQLite initial-lookup proofs\n",
			wantMessage: "missing CK-015 SQLite named-proof step",
		},
		{
			name: "hosted PostgreSQL step removed", source: "workflow",
			old:         "      - name: Run CK-015 PostgreSQL lookup and preservation proofs\n",
			replacement: "      - name: Removed CK-015 PostgreSQL lookup and preservation proofs\n",
			wantMessage: "missing CK-015 PostgreSQL named-proof step",
		},
		{
			name: "hosted SQLite wrong leg", source: "workflow",
			anchor: "      - name: Run CK-015 SQLite initial-lookup proofs\n",
			old:    "if: ${{ matrix.codec == 'plain' }}", replacement: "if: ${{ matrix.codec == 'aes-gcm' }}",
			wantMessage: "CK-015 SQLite proof runs only in plain correctness leg",
		},
		{
			name: "hosted PostgreSQL wrong package", source: "workflow",
			anchor: "      - name: Run CK-015 PostgreSQL lookup and preservation proofs\n",
			old:    "./internal/storage", replacement: "./internal/verify",
			wantMessage: "CK-015 PostgreSQL hosted wrapper uses exact race/count/serialization/package flags",
		},
		{
			name: "hosted SQLite selector removed", source: "workflow",
			anchor: "      - name: Run CK-015 SQLite initial-lookup proofs\n",
			old:    "|TestCKV11316015InitialLookupPartialScanErrorStopsStoreBeforeFallbackSQLite", replacement: "",
			wantMessage: "CK-015 SQLite partial-Scan selector",
		},
		{
			name: "hosted PostgreSQL selector removed", source: "workflow",
			anchor: "      - name: Run CK-015 PostgreSQL lookup and preservation proofs\n",
			old:    "|TestCKV11316015PostgresRepairCompetitorWinsChunkLockBeforePublication", replacement: "",
			wantMessage: "CK-015 PostgreSQL competitor-lock selector",
		},
		{
			name: "hosted SQLite wrong profile", source: "workflow",
			anchor: "      - name: Run CK-015 SQLite initial-lookup proofs\n",
			old:    "profile=ck015-initial-lookup-sqlite", replacement: "profile=ck015-initial-lookup-postgres",
			wantMessage: "CK-015 SQLite proof freezes checker profile",
		},
		{
			name: "hosted PostgreSQL maintenance missing", source: "workflow",
			anchor: "      - name: Run CK-015 PostgreSQL lookup and preservation proofs\n",
			old:    "          COLDKEEP_TEST_DB_MAINTENANCE: postgres\n", replacement: "",
			wantMessage: "CK-015 PostgreSQL proof uses maintenance database",
		},
		{
			name: "hosted PostgreSQL bootstrap missing", source: "workflow",
			anchor: "      - name: Run CK-015 PostgreSQL lookup and preservation proofs\n",
			old:    "          COLDKEEP_DB_AUTO_BOOTSTRAP: true\n", replacement: "",
			wantMessage: "CK-015 PostgreSQL proof enables bootstrap",
		},
		{
			name: "hosted PostgreSQL connection missing", source: "workflow",
			anchor: "      - name: Run CK-015 PostgreSQL lookup and preservation proofs\n",
			old:    "          DB_SSLMODE: disable\n", replacement: "",
			wantMessage: "CK-015 PostgreSQL proof sets DB SSL mode",
		},
		{
			name: "hosted SQLite stale protection removed", source: "workflow",
			anchor: "      - name: Run CK-015 SQLite initial-lookup proofs\n",
			old:    "refusing to reuse CK-015 SQLite evidence path", replacement: "allowing stale CK-015 SQLite evidence path",
			wantMessage: "CK-015 SQLite hosted wrapper refuses stale invocation evidence",
		},
		{
			name: "hosted PostgreSQL stderr merged", source: "workflow",
			anchor: "      - name: Run CK-015 PostgreSQL lookup and preservation proofs\n",
			old:    "2>\"$go_stderr_file\" | tee \"$json_file\"", replacement: "2>&1 | tee \"$json_file\"",
			wantMessage: "CK-015 PostgreSQL hosted wrapper keeps Go stderr separate from JSON",
		},
		{
			name: "hosted SQLite pipeline forged", source: "workflow",
			anchor: "      - name: Run CK-015 SQLite initial-lookup proofs\n",
			old:    "pipeline_status=(\"${PIPESTATUS[@]}\")", replacement: "pipeline_status=(0 0)",
			wantMessage: "CK-015 SQLite hosted wrapper snapshots complete pipeline status immediately",
		},
		{
			name: "hosted SQLite Go status forced", source: "workflow",
			anchor: "      - name: Run CK-015 SQLite initial-lookup proofs\n",
			old:    "go_status=${pipeline_status[0]}", replacement: "go_status=0",
			wantMessage: "CK-015 SQLite hosted wrapper preserves Go status",
		},
		{
			name: "hosted PostgreSQL capture status forced", source: "workflow",
			anchor: "      - name: Run CK-015 PostgreSQL lookup and preservation proofs\n",
			old:    "capture_status=${pipeline_status[1]}", replacement: "capture_status=0",
			wantMessage: "CK-015 PostgreSQL hosted wrapper preserves capture status",
		},
		{
			name: "hosted SQLite checker status forced", source: "workflow",
			anchor: "      - name: Run CK-015 SQLite initial-lookup proofs\n",
			old:    "checker_status=$?", replacement: "checker_status=0",
			wantMessage: "CK-015 SQLite hosted wrapper preserves checker status",
		},
		{
			name: "hosted PostgreSQL status write suppressed", source: "workflow",
			anchor: "      - name: Run CK-015 PostgreSQL lookup and preservation proofs\n",
			old:    "status_record_write_status=$?", replacement: "status_record_write_status=0",
			wantMessage: "CK-015 PostgreSQL hosted wrapper preserves status-record write status",
		},
		{
			name: "hosted SQLite readability predicate weakened", source: "workflow",
			anchor: "      - name: Run CK-015 SQLite initial-lookup proofs\n",
			old:    "[ ! -f \"$target\" ] || [ ! -r \"$target\" ]", replacement: "[ ! -f \"$target\" ]",
			wantMessage: "CK-015 SQLite hosted wrapper requires every invocation record to be readable and regular",
		},
		{
			name: "artifact not always retained", source: "workflow",
			anchor: "      - name: Upload correctness-matrix execution evidence\n",
			old:    "if: ${{ always() }}", replacement: "if: failure()",
			wantMessage: "correctness-matrix evidence upload always runs",
		},
		{
			name: "artifact path only JSON", source: "workflow",
			anchor: "      - name: Upload correctness-matrix execution evidence\n",
			old:    "integration-diag/*", replacement: "integration-diag/*.json",
			wantMessage: "correctness-matrix evidence upload retains every integration diagnostic record",
		},
		{
			name: "artifact missing files ignored", source: "workflow",
			anchor: "      - name: Upload correctness-matrix execution evidence\n",
			old:    "if-no-files-found: error", replacement: "if-no-files-found: ignore",
			wantMessage: "correctness-matrix evidence upload fails when no records exist",
		},
		{
			name: "required dependency removed", source: "workflow",
			anchor: "  ci-required:\n",
			old:    "needs: [quality, correctness-matrix,", replacement: "needs: [quality,",
			wantMessage: "required gate depends on security and hosted reproducibility jobs",
		},
		{
			name: "required result capture removed", source: "workflow",
			anchor: "  ci-required:\n",
			old:    "CORRECTNESS_MATRIX_RESULT: ${{ needs['correctness-matrix'].result }}", replacement: "CORRECTNESS_MATRIX_RESULT: missing",
			wantMessage: "required gate captures correctness-matrix result",
		},
		{
			name: "checker SQLite profile removed", source: "checker",
			old: "\"ck015-initial-lookup-sqlite\"", replacement: "\"ck015-initial-lookup-sqlite-removed\"",
			wantMessage: "required-event profiles contain exact CK-015 SQLite profile once",
		},
		{
			name: "checker routing child removed", source: "checker",
			old: "TestCKV11316015InitialLookupSupportedStatusRoutingSQLite/processing", replacement: "RemovedRoutingChild",
			wantMessage: "CK-015 SQLite profile requires processing routing child",
		},
		{
			name: "local SQLite wrapper removed independently", source: "checklist",
			old: "# CK-V11316-015 SQLite initial-lookup named proof.", replacement: "# Removed CK-V11316-015 SQLite initial-lookup named proof.",
			wantMessage: "missing local Profile A CK-015 SQLite wrapper",
		},
		{
			name: "local PostgreSQL wrapper removed independently", source: "checklist",
			old: "# CK-V11316-015 PostgreSQL lookup and preservation named proof.", replacement: "# Removed CK-V11316-015 PostgreSQL lookup and preservation named proof.",
			wantMessage: "missing local Profile A CK-015 PostgreSQL wrapper",
		},
		{
			name: "local SQLite wrong package", source: "checklist",
			anchor: "# CK-V11316-015 SQLite initial-lookup named proof.\n",
			old:    "./internal/storage", replacement: "./internal/verify",
			wantMessage: "local CK-015 SQLite wrapper uses exact package, environment, and flags",
		},
		{
			name: "local SQLite unanchored selector", source: "checklist",
			anchor: "# CK-V11316-015 SQLite initial-lookup named proof.\n",
			old:    "selector='^(", replacement: "selector='(",
			wantMessage: "local CK-015 SQLite wrapper freezes anchored selector",
		},
		{
			name: "local SQLite wrong profile", source: "checklist",
			anchor: "# CK-V11316-015 SQLite initial-lookup named proof.\n",
			old:    "profile=ck015-initial-lookup-sqlite", replacement: "profile=ck015-initial-lookup-postgres",
			wantMessage: "local CK-015 SQLite wrapper selects exact profile",
		},
		{
			name: "local SQLite race removed", source: "checklist",
			anchor: "# CK-V11316-015 SQLite initial-lookup named proof.\n",
			old:    "go test -race -count=1 -p=1", replacement: "go test -count=1 -p=1",
			wantMessage: "local CK-015 SQLite wrapper uses exact package, environment, and flags",
		},
		{
			name: "local SQLite stale protection removed", source: "checklist",
			anchor: "# CK-V11316-015 SQLite initial-lookup named proof.\n",
			old:    "refusing to reuse CK-015 SQLite evidence path", replacement: "allowing CK-015 SQLite evidence reuse",
			wantMessage: "local CK-015 SQLite wrapper refuses stale invocation evidence",
		},
		{
			name: "local SQLite stderr merged", source: "checklist",
			anchor: "# CK-V11316-015 SQLite initial-lookup named proof.\n",
			old:    "2>\"$go_stderr_file\" | tee \"$json_file\"", replacement: "2>&1 | tee \"$json_file\"",
			wantMessage: "local CK-015 SQLite wrapper keeps Go stderr separate from JSON",
		},
		{
			name: "local SQLite pipeline forged", source: "checklist",
			anchor: "# CK-V11316-015 SQLite initial-lookup named proof.\n",
			old:    "pipeline_status=(\"${PIPESTATUS[@]}\")", replacement: "pipeline_status=(0 0)",
			wantMessage: "local CK-015 SQLite wrapper snapshots complete pipeline status immediately",
		},
		{
			name: "local SQLite Go status forced", source: "checklist",
			anchor: "# CK-V11316-015 SQLite initial-lookup named proof.\n",
			old:    "go_status=${pipeline_status[0]}", replacement: "go_status=0",
			wantMessage: "local CK-015 SQLite wrapper preserves Go status",
		},
		{
			name: "local SQLite capture status forced", source: "checklist",
			anchor: "# CK-V11316-015 SQLite initial-lookup named proof.\n",
			old:    "capture_status=${pipeline_status[1]}", replacement: "capture_status=0",
			wantMessage: "local CK-015 SQLite wrapper preserves capture status",
		},
		{
			name: "local SQLite checker status forced", source: "checklist",
			anchor: "# CK-V11316-015 SQLite initial-lookup named proof.\n",
			old:    "checker_status=$?", replacement: "checker_status=0",
			wantMessage: "local CK-015 SQLite wrapper preserves checker status",
		},
		{
			name: "local SQLite status write forced", source: "checklist",
			anchor: "# CK-V11316-015 SQLite initial-lookup named proof.\n",
			old:    "status_record_write_status=$?", replacement: "status_record_write_status=0",
			wantMessage: "local CK-015 SQLite wrapper preserves status-record write status",
		},
		{
			name: "local SQLite readability predicate weakened", source: "checklist",
			anchor: "# CK-V11316-015 SQLite initial-lookup named proof.\n",
			old:    "[ ! -f \"$target\" ] || [ ! -r \"$target\" ]", replacement: "[ ! -f \"$target\" ]",
			wantMessage: "local CK-015 SQLite wrapper requires every invocation record to be readable and regular",
		},
		{
			name: "local PostgreSQL wrong package", source: "checklist",
			anchor: "# CK-V11316-015 PostgreSQL lookup and preservation named proof.\n",
			old:    "./internal/storage", replacement: "./internal/verify",
			wantMessage: "local CK-015 PostgreSQL wrapper uses exact race/count/serialization/package flags",
		},
		{
			name: "local PostgreSQL unanchored selector", source: "checklist",
			anchor: "# CK-V11316-015 PostgreSQL lookup and preservation named proof.\n",
			old:    "selector='^(", replacement: "selector='(",
			wantMessage: "local CK-015 PostgreSQL wrapper freezes anchored selector",
		},
		{
			name: "local PostgreSQL wrong profile", source: "checklist",
			anchor: "# CK-V11316-015 PostgreSQL lookup and preservation named proof.\n",
			old:    "profile=ck015-initial-lookup-postgres", replacement: "profile=ck015-initial-lookup-sqlite",
			wantMessage: "local CK-015 PostgreSQL wrapper selects exact profile",
		},
		{
			name: "local PostgreSQL race removed", source: "checklist",
			anchor: "# CK-V11316-015 PostgreSQL lookup and preservation named proof.\n",
			old:    "go test -race -count=1 -p=1", replacement: "go test -count=1 -p=1",
			wantMessage: "local CK-015 PostgreSQL wrapper uses exact race/count/serialization/package flags",
		},
		{
			name: "local PostgreSQL maintenance removed", source: "checklist",
			anchor: "# CK-V11316-015 PostgreSQL lookup and preservation named proof.\n",
			old:    "COLDKEEP_TEST_DB_MAINTENANCE=postgres \\\n", replacement: "",
			wantMessage: "local CK-015 PostgreSQL wrapper uses maintenance database",
		},
		{
			name: "local PostgreSQL bootstrap removed", source: "checklist",
			anchor: "# CK-V11316-015 PostgreSQL lookup and preservation named proof.\n",
			old:    "COLDKEEP_DB_AUTO_BOOTSTRAP=true \\\n", replacement: "",
			wantMessage: "local CK-015 PostgreSQL wrapper enables bootstrap",
		},
		{
			name: "local PostgreSQL connection removed", source: "checklist",
			anchor: "# CK-V11316-015 PostgreSQL lookup and preservation named proof.\n",
			old:    "DB_SSLMODE=\"$DB_SSLMODE\" \\\n", replacement: "",
			wantMessage: "local CK-015 PostgreSQL wrapper preserves DB SSL mode",
		},
		{
			name: "local PostgreSQL stale protection removed", source: "checklist",
			anchor: "# CK-V11316-015 PostgreSQL lookup and preservation named proof.\n",
			old:    "refusing to reuse CK-015 PostgreSQL evidence path", replacement: "allowing CK-015 PostgreSQL evidence reuse",
			wantMessage: "local CK-015 PostgreSQL wrapper refuses stale invocation evidence",
		},
		{
			name: "local PostgreSQL stderr merged", source: "checklist",
			anchor: "# CK-V11316-015 PostgreSQL lookup and preservation named proof.\n",
			old:    "2>\"$go_stderr_file\" | tee \"$json_file\"", replacement: "2>&1 | tee \"$json_file\"",
			wantMessage: "local CK-015 PostgreSQL wrapper keeps Go stderr separate from JSON",
		},
		{
			name: "local PostgreSQL pipeline forged", source: "checklist",
			anchor: "# CK-V11316-015 PostgreSQL lookup and preservation named proof.\n",
			old:    "pipeline_status=(\"${PIPESTATUS[@]}\")", replacement: "pipeline_status=(0 0)",
			wantMessage: "local CK-015 PostgreSQL wrapper snapshots complete pipeline status immediately",
		},
		{
			name: "local PostgreSQL Go status forced", source: "checklist",
			anchor: "# CK-V11316-015 PostgreSQL lookup and preservation named proof.\n",
			old:    "go_status=${pipeline_status[0]}", replacement: "go_status=0",
			wantMessage: "local CK-015 PostgreSQL wrapper preserves Go status",
		},
		{
			name: "local PostgreSQL capture status forced", source: "checklist",
			anchor: "# CK-V11316-015 PostgreSQL lookup and preservation named proof.\n",
			old:    "capture_status=${pipeline_status[1]}", replacement: "capture_status=0",
			wantMessage: "local CK-015 PostgreSQL wrapper preserves capture status",
		},
		{
			name: "local PostgreSQL checker status forced", source: "checklist",
			anchor: "# CK-V11316-015 PostgreSQL lookup and preservation named proof.\n",
			old:    "checker_status=$?", replacement: "checker_status=0",
			wantMessage: "local CK-015 PostgreSQL wrapper preserves checker status",
		},
		{
			name: "local PostgreSQL status write forced", source: "checklist",
			anchor: "# CK-V11316-015 PostgreSQL lookup and preservation named proof.\n",
			old:    "status_record_write_status=$?", replacement: "status_record_write_status=0",
			wantMessage: "local CK-015 PostgreSQL wrapper preserves status-record write status",
		},
		{
			name: "local PostgreSQL readability predicate weakened", source: "checklist",
			anchor: "# CK-V11316-015 PostgreSQL lookup and preservation named proof.\n",
			old:    "[ ! -f \"$target\" ] || [ ! -r \"$target\" ]", replacement: "[ ! -f \"$target\" ]",
			wantMessage: "local CK-015 PostgreSQL wrapper requires every invocation record to be readable and regular",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var source string
			switch test.source {
			case "workflow":
				source = workflow
			case "checklist":
				source = checklist
			case "checker":
				source = checker
			default:
				t.Fatalf("unknown source %q", test.source)
			}
			anchorIndex := 0
			if test.anchor != "" {
				anchorIndex = strings.Index(source, test.anchor)
				if anchorIndex < 0 {
					t.Fatalf("source fixture did not contain anchor %q", test.anchor)
				}
			}
			targetOffset := strings.Index(source[anchorIndex:], test.old)
			if targetOffset < 0 {
				t.Fatalf("source fixture did not contain %q after anchor %q", test.old, test.anchor)
			}
			targetIndex := anchorIndex + targetOffset
			mutated := source[:targetIndex] + test.replacement + source[targetIndex+len(test.old):]

			var stderr string
			switch test.source {
			case "workflow":
				stderr = runAuditLocalOnlyWithChecklistFixture(t, mutated, codeqlWorkflow, checklist, true)
			case "checklist":
				stderr = runAuditLocalOnlyWithChecklistFixture(t, workflow, codeqlWorkflow, mutated, true)
			case "checker":
				stderr = runAuditLocalOnlyWithSourceFixture(t, workflow, codeqlWorkflow, "COLDKEEP_REQUIRED_TEST_EVENTS_FILE", mutated)
			}
			if !strings.Contains(stderr, test.wantMessage) {
				t.Fatalf("expected %q, got:\n%s", test.wantMessage, stderr)
			}
		})
	}
}

func TestAuditCIEnforcementRequiresCK015ABCDMutationMatrix(t *testing.T) {
	workflow := readRepoFile(t, filepath.Join(".github", "workflows", "ci.yml"))
	codeqlWorkflow := readRepoFile(t, filepath.Join(".github", "workflows", "codeql.yml"))
	checklist := readRepoFile(t, "PRE_RELEASE_CHECKLIST.md")
	type wrapperFixture struct {
		name, id, source, anchor, indent string
	}
	wrappers := []wrapperFixture{
		{"hosted SQLite", "hosted-sqlite", "workflow", "      - name: Run CK-015 SQLite initial-lookup proofs\n", "          "},
		{"hosted PostgreSQL", "hosted-postgres", "workflow", "      - name: Run CK-015 PostgreSQL lookup and preservation proofs\n", "          "},
		{"local SQLite", "local-sqlite", "checklist", "# CK-V11316-015 SQLite initial-lookup named proof.\n", ""},
		{"local PostgreSQL", "local-postgres", "checklist", "# CK-V11316-015 PostgreSQL lookup and preservation named proof.\n", ""},
	}

	for _, wrapper := range wrappers {
		wrapper := wrapper
		mutations := []struct {
			name, id, old, replacement, wantMessage string
		}{
			{
				"A completeness omits Go stderr",
				"a",
				"evidence_status=0\n" + wrapper.indent + "for target in \"$json_file\" \"$go_stderr_file\" \"$checker_stdout_file\" \"$checker_stderr_file\" \"$status_file\" \"$metadata_file\"; do",
				"evidence_status=0\n" + wrapper.indent + "for target in \"$json_file\" \"$checker_stdout_file\" \"$checker_stderr_file\" \"$status_file\" \"$metadata_file\"; do",
				"active completeness gate must enumerate exactly six invocation records",
			},
			{
				"B Go failure returns zero",
				"b",
				"if [ \"$go_status\" -ne 0 ]; then\n" + wrapper.indent + "  status=$go_status",
				"if [ \"$go_status\" -ne 0 ]; then\n" + wrapper.indent + "  status=0",
				"failure precedence must propagate Go, capture, checker, status-write, and evidence statuses",
			},
			{
				"C delayed PIPESTATUS snapshot",
				"c",
				"pipeline_status=(\"${PIPESTATUS[@]}\")",
				"printf 'fixture diagnostic\\n' | cat\n" + wrapper.indent + "pipeline_status=(\"${PIPESTATUS[@]}\")",
				"PIPESTATUS snapshot must immediately follow the Go/tee pipeline",
			},
			{
				"D stale check disabled",
				"d",
				"if [ -e \"$target\" ]; then",
				"if false; then",
				"active stale-target gate must enumerate six records and reject existing targets",
			},
		}
		for _, mutation := range mutations {
			mutation := mutation
			t.Run(wrapper.name+"/"+mutation.name, func(t *testing.T) {
				source := workflow
				if wrapper.source == "checklist" {
					source = checklist
				}
				anchorIndex := strings.Index(source, wrapper.anchor)
				if anchorIndex < 0 {
					t.Fatalf("source fixture did not contain anchor %q", wrapper.anchor)
				}
				targetOffset := strings.Index(source[anchorIndex:], mutation.old)
				if targetOffset < 0 {
					t.Fatalf("source fixture did not contain mutation target %q", mutation.old)
				}
				targetIndex := anchorIndex + targetOffset
				mutated := source[:targetIndex] + mutation.replacement + source[targetIndex+len(mutation.old):]
				stderr := withCK015CoreAuditCase(t, "ad-"+wrapper.id+"-"+mutation.id, func() string {
					if wrapper.source == "workflow" {
						return runAuditLocalOnlyWithChecklistFixture(t, mutated, codeqlWorkflow, checklist, true)
					}
					return runAuditLocalOnlyWithChecklistFixture(t, workflow, codeqlWorkflow, mutated, true)
				})
				if !strings.Contains(stderr, mutation.wantMessage) {
					t.Fatalf("expected %q, got:\n%s", mutation.wantMessage, stderr)
				}
				if !strings.Contains(stderr, "does not match frozen approved wrapper body") {
					t.Fatalf("expected frozen wrapper identity diagnostic, got:\n%s", stderr)
				}
			})
		}
	}
}

func TestAuditCIEnforcementRequiresCK015ActiveBoundedGrammar(t *testing.T) {
	workflow := readRepoFile(t, filepath.Join(".github", "workflows", "ci.yml"))
	codeqlWorkflow := readRepoFile(t, filepath.Join(".github", "workflows", "codeql.yml"))
	checklist := readRepoFile(t, "PRE_RELEASE_CHECKLIST.md")

	tests := []struct {
		name        string
		source      string
		anchor      string
		old         string
		replacement string
		wantMessage string
	}{
		{
			name: "hosted SQLite critical region in inactive branch", source: "workflow",
			anchor:      "      - name: Run CK-015 SQLite initial-lookup proofs\n",
			old:         "          go test -race -count=1 -p=1 -parallel=1 -json ./internal/storage \\\n            -run \"$selector\" 2>\"$go_stderr_file\" | tee \"$json_file\"\n          pipeline_status=(\"${PIPESTATUS[@]}\")\n          go_status=${pipeline_status[0]}\n          capture_status=${pipeline_status[1]}",
			replacement: "          if false; then\n            go test -race -count=1 -p=1 -parallel=1 -json ./internal/storage \\\n              -run \"$selector\" 2>\"$go_stderr_file\" | tee \"$json_file\"\n            pipeline_status=(\"${PIPESTATUS[@]}\")\n            go_status=${pipeline_status[0]}\n            capture_status=${pipeline_status[1]}\n          fi",
			wantMessage: "critical proof regions must not be hidden in an inactive branch",
		},
		{
			name: "hosted PostgreSQL stale gate in quoted decoy", source: "workflow",
			anchor:      "      - name: Run CK-015 PostgreSQL lookup and preservation proofs\n",
			old:         "          for target in \"$json_file\" \"$go_stderr_file\" \"$checker_stdout_file\" \"$checker_stderr_file\" \"$status_file\" \"$metadata_file\"; do\n            if [ -e \"$target\" ]; then\n              echo \"refusing to reuse CK-015 PostgreSQL evidence path: $target\" >&2\n              exit 1\n            fi\n          done",
			replacement: "          : '\n          for target in \"$json_file\" \"$go_stderr_file\" \"$checker_stdout_file\" \"$checker_stderr_file\" \"$status_file\" \"$metadata_file\"; do\n            if [ -e \"$target\" ]; then\n              echo \"refusing to reuse CK-015 PostgreSQL evidence path: $target\" >&2\n              exit 1\n            fi\n          done\n          '",
			wantMessage: "active completeness gate must enumerate exactly six invocation records",
		},
		{
			name: "hosted SQLite duplicate checker decoy", source: "workflow",
			anchor:      "      - name: Run CK-015 SQLite initial-lookup proofs\n",
			old:         "          python3 scripts/check_required_test_events.py \\\n            --profile \"$profile\" \\\n            --events \"$json_file\" \\\n            >\"$checker_stdout_file\" 2>\"$checker_stderr_file\"",
			replacement: "          python3 scripts/check_required_test_events.py \\\n            --profile \"$profile\" \\\n            --events \"$json_file\" \\\n            >\"$checker_stdout_file\" 2>\"$checker_stderr_file\"\n          python3 scripts/check_required_test_events.py \\\n            --profile \"$profile\" \\\n            --events \"$json_file\" \\\n            >\"$checker_stdout_file\" 2>\"$checker_stderr_file\"",
			wantMessage: "required-event checker invocation must occur exactly once at active top level",
		},
		{
			name: "hosted PostgreSQL critical regions reordered", source: "workflow",
			anchor:      "      - name: Run CK-015 PostgreSQL lookup and preservation proofs\n",
			old:         "          printf '%s\\n' \\\n            \"go_status=$go_status\" \\\n            \"capture_status=$capture_status\" \\\n            \"checker_status=$checker_status\" \\\n            'status_record_write_status=0' >\"$status_file\"\n          status_record_write_status=$?\n\n          evidence_status=0",
			replacement: "          evidence_status=0\n          printf '%s\\n' \\\n            \"go_status=$go_status\" \\\n            \"capture_status=$capture_status\" \\\n            \"checker_status=$checker_status\" \\\n            'status_record_write_status=0' >\"$status_file\"\n          status_record_write_status=$?",
			wantMessage: "critical proof regions must remain unique and in execution order",
		},
		{
			name: "local SQLite stale gate in never-called function", source: "checklist",
			anchor:      "# CK-V11316-015 SQLite initial-lookup named proof.\n",
			old:         "for target in \"$json_file\" \"$go_stderr_file\" \"$checker_stdout_file\" \"$checker_stderr_file\" \"$status_file\" \"$metadata_file\"; do\n  if [ -e \"$target\" ]; then\n    echo \"refusing to reuse CK-015 SQLite evidence path: $target\" >&2\n    exit 1\n  fi\ndone",
			replacement: "ck015_decoy() {\n  for target in \"$json_file\" \"$go_stderr_file\" \"$checker_stdout_file\" \"$checker_stderr_file\" \"$status_file\" \"$metadata_file\"; do\n    if [ -e \"$target\" ]; then\n      echo \"refusing to reuse CK-015 SQLite evidence path: $target\" >&2\n      exit 1\n    fi\n  done\n}",
			wantMessage: "critical proof regions must not be hidden in a function",
		},
		{
			name: "local PostgreSQL duplicate evidence initialization", source: "checklist",
			anchor: "# CK-V11316-015 PostgreSQL lookup and preservation named proof.\n",
			old:    "evidence_status=0", replacement: "evidence_status=0\nevidence_status=0",
			wantMessage: "evidence status initialization must occur exactly once at active top level",
		},
		{
			name: "local SQLite status reset after precedence", source: "checklist",
			anchor:      "# CK-V11316-015 SQLite initial-lookup named proof.\n",
			old:         "fi\nset -e\nif [ \"$status\" -ne 0 ]; then",
			replacement: "fi\nstatus=0\nset -e\nif [ \"$status\" -ne 0 ]; then",
			wantMessage: "computed failure status must not be reset after the precedence chain",
		},
		{
			name: "local PostgreSQL failure return weakened", source: "checklist",
			anchor:      "# CK-V11316-015 PostgreSQL lookup and preservation named proof.\n",
			old:         "  exit \"$status\"\nfi",
			replacement: "  return 0\nfi",
			wantMessage: "local wrapper must restore errexit, exit on failure, and fall through on success",
		},
	}

	for index, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var source string
			switch test.source {
			case "workflow":
				source = workflow
			case "checklist":
				source = checklist
			default:
				t.Fatalf("unknown source %q", test.source)
			}
			anchorIndex := strings.Index(source, test.anchor)
			if anchorIndex < 0 {
				t.Fatalf("source fixture did not contain anchor %q", test.anchor)
			}
			targetOffset := strings.Index(source[anchorIndex:], test.old)
			if targetOffset < 0 {
				t.Fatalf("source fixture did not contain mutation target after anchor: %q", test.old)
			}
			targetIndex := anchorIndex + targetOffset
			mutated := source[:targetIndex] + test.replacement + source[targetIndex+len(test.old):]

			stderr := withCK015CoreAuditCase(t, fmt.Sprintf("struct-%02d", index+1), func() string {
				if test.source == "workflow" {
					return runAuditLocalOnlyWithChecklistFixture(t, mutated, codeqlWorkflow, checklist, true)
				}
				return runAuditLocalOnlyWithChecklistFixture(t, workflow, codeqlWorkflow, mutated, true)
			})
			if !strings.Contains(stderr, test.wantMessage) {
				t.Fatalf("expected %q, got:\n%s", test.wantMessage, stderr)
			}
			if !strings.Contains(stderr, "does not match frozen approved wrapper body") {
				t.Fatalf("expected frozen wrapper identity diagnostic, got:\n%s", stderr)
			}
		})
	}
}

func TestAuditCIEnforcementRequiresCK015StatusLifetimeMutationMatrix(t *testing.T) {
	workflow := readRepoFile(t, filepath.Join(".github", "workflows", "ci.yml"))
	codeqlWorkflow := readRepoFile(t, filepath.Join(".github", "workflows", "codeql.yml"))
	checklist := readRepoFile(t, "PRE_RELEASE_CHECKLIST.md")
	type wrapperFixture struct {
		name, id, source, anchor, indent string
	}
	wrappers := []wrapperFixture{
		{"hosted SQLite", "hosted-sqlite", "workflow", "      - name: Run CK-015 SQLite initial-lookup proofs\n", "          "},
		{"hosted PostgreSQL", "hosted-postgres", "workflow", "      - name: Run CK-015 PostgreSQL lookup and preservation proofs\n", "          "},
		{"local SQLite", "local-sqlite", "checklist", "# CK-V11316-015 SQLite initial-lookup named proof.\n", ""},
		{"local PostgreSQL", "local-postgres", "checklist", "# CK-V11316-015 PostgreSQL lookup and preservation named proof.\n", ""},
	}
	mutations := []struct{ name, id, line string }{
		{"E evidence status reset", "e", `evidence_status="0"`},
		{"F Go status reset", "f", `go_status="0"`},
	}
	for _, wrapper := range wrappers {
		wrapper := wrapper
		for _, mutation := range mutations {
			mutation := mutation
			t.Run(wrapper.name+"/"+mutation.name, func(t *testing.T) {
				source := workflow
				if wrapper.source == "checklist" {
					source = checklist
				}
				anchor := strings.Index(source, wrapper.anchor)
				if anchor < 0 {
					t.Fatalf("missing wrapper anchor %q", wrapper.anchor)
				}
				needle := wrapper.indent + "cat \"$checker_stderr_file\" >&2\n"
				offset := strings.Index(source[anchor:], needle)
				if offset < 0 {
					t.Fatalf("missing insertion point after %q", wrapper.anchor)
				}
				position := anchor + offset + len(needle)
				mutated := source[:position] + "\n" + wrapper.indent + mutation.line + "\n" + source[position:]
				output := withCK015CoreAuditCase(t, "ef-"+wrapper.id+"-"+mutation.id, func() string {
					if wrapper.source == "workflow" {
						return runAuditLocalOnlyWithChecklistFixture(t, mutated, codeqlWorkflow, checklist, true)
					}
					return runAuditLocalOnlyWithChecklistFixture(t, workflow, codeqlWorkflow, mutated, true)
				})
				if !strings.Contains(output, "does not match frozen approved wrapper body") {
					t.Fatalf("status-lifetime mutation lacked identity diagnostic:\n%s", output)
				}
				if !strings.Contains(output, "[audit] FAILED:") {
					t.Fatalf("status-lifetime mutation lacked terminal audit failure:\n%s", output)
				}
			})
		}
	}
	if root := os.Getenv("COLDKEEP_CK015_NEGATIVE_EVIDENCE_DIR"); root != "" {
		verifyCK015CoreEvidenceInventory(t, root)
	}
}

func ck015CoreCaseIDs() []string {
	ids := []string{"00-control"}
	for _, wrapper := range []string{"hosted-sqlite", "hosted-postgres", "local-sqlite", "local-postgres"} {
		for _, mutation := range []string{"a", "b", "c", "d"} {
			ids = append(ids, "ad-"+wrapper+"-"+mutation)
		}
	}
	for index := 1; index <= 8; index++ {
		ids = append(ids, fmt.Sprintf("struct-%02d", index))
	}
	for _, wrapper := range []string{"hosted-sqlite", "hosted-postgres", "local-sqlite", "local-postgres"} {
		for _, mutation := range []string{"e", "f"} {
			ids = append(ids, "ef-"+wrapper+"-"+mutation)
		}
	}
	return ids
}

func verifyCK015CoreEvidenceInventory(t *testing.T, root string) {
	t.Helper()
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatalf("read CK-015 core evidence root: %v", err)
	}
	expected := map[string]bool{}
	for _, id := range ck015CoreCaseIDs() {
		expected[id] = true
	}
	if len(expected) != 33 {
		t.Fatalf("internal core inventory has %d IDs, want 33", len(expected))
	}
	if len(entries) != len(expected) {
		t.Fatalf("core evidence contains %d cases, want %d", len(entries), len(expected))
	}
	for _, entry := range entries {
		if !entry.IsDir() || !expected[entry.Name()] {
			t.Fatalf("unexpected core evidence entry %q", entry.Name())
		}
		caseRoot := filepath.Join(root, entry.Name())
		for _, required := range []string{
			"argv.json", "cwd", "metadata.json", "selected-environment.json", "fixture.diff",
			"input-hashes-before.json", "input-hashes-after.json", "started-utc", "ended-utc",
			"stdout", "stderr", "status.json", "terminal-observation.json",
		} {
			if info, statErr := os.Stat(filepath.Join(caseRoot, required)); statErr != nil || !info.Mode().IsRegular() {
				t.Fatalf("case %q missing regular %s: %v", entry.Name(), required, statErr)
			}
		}
		var status struct {
			Started  bool `json:"started"`
			Exited   bool `json:"exited"`
			ExitCode int  `json:"exit_code"`
		}
		statusBytes, readErr := os.ReadFile(filepath.Join(caseRoot, "status.json"))
		if readErr != nil || json.Unmarshal(statusBytes, &status) != nil {
			t.Fatalf("read status for %q: %v", entry.Name(), readErr)
		}
		var terminal struct {
			Pass  bool `json:"pass_marker"`
			Fail  bool `json:"fail_marker"`
			Error bool `json:"error_marker"`
		}
		terminalBytes, readErr := os.ReadFile(filepath.Join(caseRoot, "terminal-observation.json"))
		if readErr != nil || json.Unmarshal(terminalBytes, &terminal) != nil {
			t.Fatalf("read terminal for %q: %v", entry.Name(), readErr)
		}
		if !status.Started || !status.Exited {
			t.Fatalf("case %q did not start and exit", entry.Name())
		}
		if entry.Name() == "00-control" {
			if status.ExitCode != 0 || !terminal.Pass || terminal.Fail || terminal.Error {
				t.Fatalf("positive control %q has invalid status/terminal", entry.Name())
			}
		} else if status.ExitCode == 0 || terminal.Pass || !terminal.Fail || !terminal.Error {
			t.Fatalf("negative case %q has invalid status/terminal", entry.Name())
		}
	}
}

func TestAuditCIEnforcementRequiresCK015ApprovedWrapperIdentityHelper(t *testing.T) {
	workflow := readRepoFile(t, filepath.Join(".github", "workflows", "ci.yml"))
	checklist := readRepoFile(t, "PRE_RELEASE_CHECKLIST.md")
	fixtures := []struct{ slot, content string }{
		{"hosted-sqlite", extractCK015ApprovedSection(t, workflow, "      - name: Run CK-015 SQLite initial-lookup proofs", "      - name: Run CK-015 PostgreSQL lookup and preservation proofs")},
		{"hosted-postgres", extractCK015ApprovedSection(t, workflow, "      - name: Run CK-015 PostgreSQL lookup and preservation proofs", "      - name: Run required PostgreSQL internal package contracts")},
		{"local-sqlite", extractCK015ApprovedSection(t, checklist, "# CK-V11316-015 SQLite initial-lookup named proof.", "# CK-V11316-015 PostgreSQL lookup and preservation named proof.")},
		{"local-postgres", extractCK015ApprovedSection(t, checklist, "# CK-V11316-015 PostgreSQL lookup and preservation named proof.", "# Step 3 loop leaves COLDKEEP_CODEC set to the last codec (aes-gcm).")},
	}
	for _, fixture := range fixtures {
		fixture := fixture
		t.Run(fixture.slot, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "approved-body")
			if err := os.WriteFile(path, []byte(fixture.content), 0o600); err != nil {
				t.Fatalf("write approved body: %v", err)
			}
			output, err := runCK015IdentityProbe(t, fixture.slot, path)
			if err != nil || !strings.Contains(output, "matches frozen approved wrapper body ("+fixture.slot+")") {
				t.Fatalf("approved %s identity failed: %v\n%s", fixture.slot, err, output)
			}
		})
	}
	for _, slot := range []string{"wrong-slot", ""} {
		t.Run("reject-slot-"+slot, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "body")
			if err := os.WriteFile(path, []byte(fixtures[0].content), 0o600); err != nil {
				t.Fatalf("write probe body: %v", err)
			}
			output, err := runCK015IdentityProbe(t, slot, path)
			if err == nil || !strings.Contains(output, "unknown CK-015 approved wrapper slot") {
				t.Fatalf("invalid slot %q was not rejected: %v\n%s", slot, err, output)
			}
		})
	}
}

func runCK015IdentityProbe(t *testing.T, slot, path string) (string, error) {
	t.Helper()
	cmd := exec.Command("bash", "scripts/audit_ci_enforcement.sh", "--ck015-identity-probe", slot, path)
	cmd.Dir = repoRoot(t)
	output, err := cmd.CombinedOutput()
	return string(output), err
}

func extractCK015ApprovedSection(t *testing.T, source, startAnchor, endAnchor string) string {
	t.Helper()
	startMarker := startAnchor + "\n"
	endMarker := endAnchor + "\n"
	if strings.Count(source, startMarker) != 1 || strings.Count(source, endMarker) != 1 {
		t.Fatalf("approved boundaries are not unique: %q -> %q", startAnchor, endAnchor)
	}
	start := strings.Index(source, startMarker)
	end := strings.Index(source, endMarker)
	if start < 0 || end <= start {
		t.Fatalf("approved boundaries are not ordered: %q -> %q", startAnchor, endAnchor)
	}
	return strings.TrimRight(source[start:end], "\n")
}

func TestAuditCIEnforcementRequiresCK015WholeBodyIdentityOnlyMutations(t *testing.T) {
	workflow := readRepoFile(t, filepath.Join(".github", "workflows", "ci.yml"))
	codeqlWorkflow := readRepoFile(t, filepath.Join(".github", "workflows", "codeql.yml"))
	checklist := readRepoFile(t, "PRE_RELEASE_CHECKLIST.md")
	tests := []struct {
		name, insertion string
	}{
		{"assignment", "          CK015_IDENTITY_ONLY=1\n"},
		{"declaration", "          readonly CK015_IDENTITY_ONLY=1\n"},
		{"compound", "          : && :\n"},
		{"no-op", "          :\n"},
		{"comment", "          # identity-only fixture\n"},
		{"whitespace", "          \n"},
	}
	end := "      - name: Run CK-015 PostgreSQL lookup and preservation proofs\n"
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			mutated := strings.Replace(workflow, end, test.insertion+end, 1)
			if mutated == workflow {
				t.Fatal("identity-only mutation was not applied")
			}
			output := runAuditLocalOnlyWithChecklistFixture(t, mutated, codeqlWorkflow, checklist, true)
			if !strings.Contains(output, "CK-015 SQLite hosted wrapper does not match frozen approved wrapper body") {
				t.Fatalf("identity-only mutation was not rejected by frozen identity:\n%s", output)
			}
		})
	}
	t.Run("insertion near final return", func(t *testing.T) {
		needle := "          exit \"$status\"\n\n" + end
		replacement := "          : # identity-only insertion near final return\n          exit \"$status\"\n\n" + end
		mutated := strings.Replace(workflow, needle, replacement, 1)
		if mutated == workflow {
			t.Fatal("final-return identity mutation was not applied")
		}
		output := runAuditLocalOnlyWithChecklistFixture(t, mutated, codeqlWorkflow, checklist, true)
		if !strings.Contains(output, "does not match frozen approved wrapper body") {
			t.Fatalf("final-return mutation was not rejected by frozen identity:\n%s", output)
		}
	})
}

func TestAuditCIEnforcementRequiresCK015ApprovedBoundaries(t *testing.T) {
	workflow := readRepoFile(t, filepath.Join(".github", "workflows", "ci.yml"))
	codeqlWorkflow := readRepoFile(t, filepath.Join(".github", "workflows", "codeql.yml"))
	checklist := readRepoFile(t, "PRE_RELEASE_CHECKLIST.md")
	start := "      - name: Run CK-015 SQLite initial-lookup proofs\n"
	end := "      - name: Run CK-015 PostgreSQL lookup and preservation proofs\n"
	tests := []struct {
		name, source, want string
	}{
		{"missing", strings.Replace(workflow, start, "      - name: Missing CK-015 SQLite initial-lookup proofs\n", 1), "start boundary must occur exactly once"},
		{"duplicated", strings.Replace(workflow, end, start+end, 1), "start boundary must occur exactly once"},
		{"reordered", strings.Replace(strings.Replace(workflow, end, "", 1), start, end+start, 1), "boundaries must be correctly ordered"},
		{"carriage-return", strings.Replace(workflow, start, strings.TrimSuffix(start, "\n")+"\r\n", 1), "source contains a CR byte"},
		{"nul", strings.Replace(workflow, start, start+"      # malformed\x00boundary\n", 1), "source contains a NUL byte"},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			output := runAuditLocalOnlyWithChecklistFixture(t, test.source, codeqlWorkflow, checklist, true)
			if !strings.Contains(output, test.want) {
				t.Fatalf("expected boundary diagnostic %q:\n%s", test.want, output)
			}
		})
	}
}

func TestAuditCIEnforcementRequiresCK015EvidenceRootExclusivity(t *testing.T) {
	root := t.TempDir()
	if err := os.Chmod(root, 0o700); err != nil {
		t.Fatalf("make evidence root private: %v", err)
	}
	caseID := "stale-case"
	caseRoot, err := allocateCK015CoreEvidenceCase(root, caseID)
	if err != nil {
		t.Fatalf("first exclusive allocation failed: %v", err)
	}
	marker := filepath.Join(caseRoot, "must-remain")
	if err := os.WriteFile(marker, []byte("preserved\n"), 0o600); err != nil {
		t.Fatalf("write stale-root marker: %v", err)
	}
	if _, err := allocateCK015CoreEvidenceCase(root, caseID); err == nil {
		t.Fatal("stale core evidence case root was silently reused")
	}
	if content, err := os.ReadFile(marker); err != nil || string(content) != "preserved\n" {
		t.Fatalf("stale-root refusal altered prior evidence: %v %q", err, content)
	}
}

func TestAuditCIEnforcementRequiresCK015SyntheticWrapperBehavior(t *testing.T) {
	workflow := readRepoFile(t, filepath.Join(".github", "workflows", "ci.yml"))
	checklist := readRepoFile(t, "PRE_RELEASE_CHECKLIST.md")
	wrapperCases := []struct {
		name    string
		kind    string
		profile string
		body    string
	}{
		{
			name:    "hosted-sqlite",
			kind:    "hosted",
			profile: "ck015-initial-lookup-sqlite",
			body:    extractCK015HostedRunBody(t, workflow, "Run CK-015 SQLite initial-lookup proofs", "Run CK-015 PostgreSQL lookup and preservation proofs"),
		},
		{
			name:    "hosted-postgres",
			kind:    "hosted",
			profile: "ck015-initial-lookup-postgres",
			body:    extractCK015HostedRunBody(t, workflow, "Run CK-015 PostgreSQL lookup and preservation proofs", "Run required PostgreSQL internal package contracts"),
		},
		{
			name:    "local-sqlite",
			kind:    "local",
			profile: "ck015-initial-lookup-sqlite",
			body:    extractCK015LocalBody(t, checklist, "# CK-V11316-015 SQLite initial-lookup named proof.", "# CK-V11316-015 PostgreSQL lookup and preservation named proof."),
		},
		{
			name:    "local-postgres",
			kind:    "local",
			profile: "ck015-initial-lookup-postgres",
			body:    extractCK015LocalBody(t, checklist, "# CK-V11316-015 PostgreSQL lookup and preservation named proof.", "# Step 3 loop leaves COLDKEEP_CODEC"),
		},
	}

	for _, wrapperCase := range wrapperCases {
		wrapperCase := wrapperCase
		for _, fault := range []bool{false, true} {
			fault := fault
			caseName := "success"
			if fault {
				caseName = "missing-go-stderr"
			}
			t.Run(wrapperCase.name+"/"+caseName, func(t *testing.T) {
				runCK015SyntheticWrapperCase(t, wrapperCase.name, wrapperCase.kind, wrapperCase.profile, wrapperCase.body, fault)
			})
		}
	}
}

func extractCK015HostedRunBody(t *testing.T, workflow, stepName, nextStepName string) string {
	t.Helper()
	startMarker := "      - name: " + stepName + "\n"
	start := strings.Index(workflow, startMarker)
	if start < 0 {
		t.Fatalf("missing hosted wrapper step %q", stepName)
	}
	endMarker := "      - name: " + nextStepName + "\n"
	endOffset := strings.Index(workflow[start+len(startMarker):], endMarker)
	if endOffset < 0 {
		t.Fatalf("missing hosted wrapper terminator %q", nextStepName)
	}
	step := workflow[start : start+len(startMarker)+endOffset]
	runMarker := "        run: |\n"
	runOffset := strings.Index(step, runMarker)
	if runOffset < 0 {
		t.Fatalf("hosted wrapper %q has no run body", stepName)
	}
	lines := strings.Split(step[runOffset+len(runMarker):], "\n")
	for index, line := range lines {
		if strings.HasPrefix(line, "          ") {
			lines[index] = strings.TrimPrefix(line, "          ")
		}
	}
	return strings.TrimSpace(strings.Join(lines, "\n")) + "\n"
}

func extractCK015LocalBody(t *testing.T, checklist, startMarker, endMarker string) string {
	t.Helper()
	start := strings.Index(checklist, startMarker)
	if start < 0 {
		t.Fatalf("missing local wrapper marker %q", startMarker)
	}
	endOffset := strings.Index(checklist[start+len(startMarker):], endMarker)
	if endOffset < 0 {
		t.Fatalf("missing local wrapper terminator %q", endMarker)
	}
	return strings.TrimSpace(checklist[start:start+len(startMarker)+endOffset]) + "\n"
}

func runCK015SyntheticWrapperCase(t *testing.T, name, kind, profile, body string, fault bool) {
	t.Helper()
	caseRoot := t.TempDir()
	stubDir := filepath.Join(caseRoot, "stubs")
	if err := os.Mkdir(stubDir, 0o700); err != nil {
		t.Fatalf("create stub directory: %v", err)
	}
	writeExecutableFixture(t, filepath.Join(stubDir, "go"), `#!/usr/bin/env bash
set -eu
if [ "$#" -eq 1 ] && [ "$1" = version ]; then
  printf '%s\n' 'go version go1.26.7 linux/amd64'
  exit 0
fi
if [ "$#" -ne 9 ] || [ "$1" != test ] || [ "$2" != -race ] || [ "$3" != -count=1 ] || [ "$4" != -p=1 ] || [ "$5" != -parallel=1 ] || [ "$6" != -json ] || [ "$7" != ./internal/storage ] || [ "$8" != -run ]; then
  printf 'unexpected synthetic go invocation:' >&2
  printf ' <%s>' "$@" >&2
  printf '\n' >&2
  exit 97
fi
if [ "$9" != "$CK015_EXPECTED_SELECTOR" ]; then
  printf 'unexpected CK-015 selector: %s\n' "$9" >&2
  exit 98
fi
package=github.com/franchoy/coldkeep/internal/storage
printf '{"Action":"start","Package":"%s"}\n' "$package"
case "$CK015_EXPECTED_PROFILE" in
  ck015-initial-lookup-sqlite)
    test_names='TestCKV11316015InitialLookupOperationalErrorStopsStoreBeforeFallbackSQLite
TestCKV11316015InitialLookupPartialScanErrorStopsStoreBeforeFallbackSQLite
TestCKV11316015InitialLookupErrNoRowsPreservesNewObjectStoreSQLite
TestCKV11316015InitialLookupSupportedStatusRoutingSQLite
TestCKV11316015InitialLookupSupportedStatusRoutingSQLite/completed
TestCKV11316015InitialLookupSupportedStatusRoutingSQLite/aborted
TestCKV11316015InitialLookupSupportedStatusRoutingSQLite/processing'
    ;;
  ck015-initial-lookup-postgres)
    test_names='TestCKV11316015PostgresInitialLookupOperationalErrorStopsStoreBeforeFallback
TestCKV11316015PostgresOpenLocalStorageRepairAndRecovery
TestCKV11316015PostgresRepairPublisherLocksChunkBeforeAuthorityMutation
TestCKV11316015PostgresRepairCompetitorWinsChunkLockBeforePublication
TestCKV11316015PostgresSharedChunkHealingBetweenValidationAndPlanReclassifies'
    ;;
  *) printf 'unexpected CK-015 profile: %s\n' "$CK015_EXPECTED_PROFILE" >&2; exit 99 ;;
esac
for test_name in $test_names
do
  printf '{"Action":"run","Package":"%s","Test":"%s"}\n' "$package" "$test_name"
  printf '{"Action":"pass","Package":"%s","Test":"%s"}\n' "$package" "$test_name"
done
printf '{"Action":"pass","Package":"%s"}\n' "$package"
`)
	writeExecutableFixture(t, filepath.Join(stubDir, "python3"), `#!/usr/bin/env bash
set +e
if [ "$#" -lt 1 ] || [ "$1" != scripts/check_required_test_events.py ]; then
  printf 'unexpected synthetic python3 invocation\n' >&2
  exit 96
fi
/usr/bin/python3 "$@"
checker_status=$?
if [ "$checker_status" -eq 0 ] && [ "${CK015_DELETE_GO_STDERR:-0}" -eq 1 ]; then
  events_file=
  previous=
  for argument in "$@"; do
    if [ "$previous" = --events ]; then
      events_file=$argument
      break
    fi
    previous=$argument
  done
  if [ -z "$events_file" ]; then
    printf 'synthetic python3 stub did not receive --events\n' >&2
    exit 95
  fi
  /bin/rm -f -- "${events_file%.json}.go.stderr"
fi
exit "$checker_status"
`)
	writeExecutableFixture(t, filepath.Join(stubDir, "git"), `#!/usr/bin/env bash
set -eu
if [ "$#" -eq 2 ] && [ "$1" = rev-parse ] && [ "$2" = HEAD ]; then
  printf '%s\n' '0123456789abcdef0123456789abcdef01234567'
  exit 0
fi
printf 'unexpected synthetic git invocation\n' >&2
exit 94
`)

	diagDir := filepath.Join(caseRoot, "integration-diag")
	profileRoot := filepath.Join(caseRoot, "profile-a")
	if kind == "local" {
		diagDir = filepath.Join(profileRoot, "required-test-events")
	}
	if err := os.MkdirAll(diagDir, 0o700); err != nil {
		t.Fatalf("create diagnostic directory: %v", err)
	}
	unrelatedPath := filepath.Join(diagDir, "unrelated-diagnostic.txt")
	if err := os.WriteFile(unrelatedPath, []byte("must remain\n"), 0o600); err != nil {
		t.Fatalf("write unrelated evidence: %v", err)
	}
	continuationPath := filepath.Join(caseRoot, "continued")
	script := "#!/usr/bin/env bash\nset -euo pipefail\n" + body
	if kind == "local" {
		script += "printf 'continued\\n' >\"$CK015_CONTINUATION_FILE\"\n"
	}
	scriptPath := filepath.Join(caseRoot, "wrapper.sh")
	writeExecutableFixture(t, scriptPath, script)

	cmd := exec.Command("/bin/bash", scriptPath)
	cmd.Dir = repoRoot(t)
	deleteValue := "0"
	if fault {
		deleteValue = "1"
	}
	cmd.Env = append(os.Environ(),
		"PATH="+stubDir+":"+os.Getenv("PATH"),
		"GOTOOLCHAIN=local",
		"RUNNER_TEMP="+caseRoot,
		"GITHUB_SHA=0123456789abcdef0123456789abcdef01234567",
		"GITHUB_RUN_ID=39001",
		"GITHUB_RUN_ATTEMPT=1",
		"GITHUB_JOB=synthetic-"+name,
		"GITHUB_REPOSITORY=franchoy/coldkeep",
		"GITHUB_REF=refs/heads/release/v1.13.16",
		"COLDKEEP_PROFILE_A_EVIDENCE_DIR="+profileRoot,
		"CK015_CONTINUATION_FILE="+continuationPath,
		"CK015_DELETE_GO_STDERR="+deleteValue,
		"CK015_EXPECTED_PROFILE="+profile,
		"CK015_EXPECTED_SELECTOR="+ck015SyntheticSelector(t, profile),
		"DB_HOST=127.0.0.1",
		"DB_PORT=5432",
		"DB_USER=coldkeep",
		"DB_PASSWORD=coldkeep",
		"DB_NAME=coldkeep",
		"DB_SSLMODE=disable",
	)
	stdoutPath := filepath.Join(caseRoot, "stdout")
	stderrPath := filepath.Join(caseRoot, "stderr")
	stdoutFile, openErr := os.OpenFile(stdoutPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if openErr != nil {
		t.Fatalf("create streaming stdout record: %v", openErr)
	}
	stderrFile, openErr := os.OpenFile(stderrPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if openErr != nil {
		_ = stdoutFile.Close()
		t.Fatalf("create streaming stderr record: %v", openErr)
	}
	cmd.Stdout = stdoutFile
	cmd.Stderr = stderrFile
	cmd.Stdin = strings.NewReader("")
	started := time.Now().UTC()
	err := cmd.Run()
	ended := time.Now().UTC()
	for name, file := range map[string]*os.File{"stdout": stdoutFile, "stderr": stderrFile} {
		if syncErr := file.Sync(); syncErr != nil {
			t.Fatalf("sync streaming %s record: %v", name, syncErr)
		}
		if closeErr := file.Close(); closeErr != nil {
			t.Fatalf("close streaming %s record: %v", name, closeErr)
		}
	}
	stdoutBytes, readErr := os.ReadFile(stdoutPath)
	if readErr != nil {
		t.Fatalf("read streaming stdout record: %v", readErr)
	}
	stderrBytes, readErr := os.ReadFile(stderrPath)
	if readErr != nil {
		t.Fatalf("read streaming stderr record: %v", readErr)
	}
	output := string(stdoutBytes) + string(stderrBytes)
	exitCode := 0
	if err != nil {
		if exit, ok := err.(*exec.ExitError); ok {
			exitCode = exit.ExitCode()
		} else {
			exitCode = -1
		}
	}
	if writeErr := writeCK015EvidenceJSON(filepath.Join(caseRoot, "child-observation.json"), map[string]any{
		"started_utc":           started.Format(time.RFC3339Nano),
		"ended_utc":             ended.Format(time.RFC3339Nano),
		"started":               exitCode != -1,
		"exited":                exitCode != -1,
		"exit_code":             exitCode,
		"pass_marker":           strings.Contains(output, "required execution-proof pass:"),
		"missing_record_marker": strings.Contains(output, "missing or unreadable CK-015"),
	}); writeErr != nil {
		t.Fatalf("retain synthetic child observation: %v", writeErr)
	}
	if exportRoot := os.Getenv("COLDKEEP_CK015_BEHAVIOR_EVIDENCE_DIR"); exportRoot != "" {
		exportName := name + "-success"
		if fault {
			exportName = name + "-missing-go-stderr"
		}
		copyFixtureTree(t, caseRoot, filepath.Join(exportRoot, exportName))
	}
	if fault && err == nil {
		t.Fatalf("missing-record wrapper unexpectedly succeeded:\n%s", output)
	}
	if !fault && err != nil {
		t.Fatalf("success wrapper failed: %v\n%s", err, output)
	}

	statusMatches, globErr := filepath.Glob(filepath.Join(diagDir, "*.status"))
	if globErr != nil || len(statusMatches) != 1 {
		t.Fatalf("expected one status record, matches=%v err=%v output=%s", statusMatches, globErr, output)
	}
	prefix := strings.TrimSuffix(statusMatches[0], ".status")
	records := []string{
		prefix + ".json",
		prefix + ".go.stderr",
		prefix + ".checker.stdout",
		prefix + ".checker.stderr",
		prefix + ".status",
		prefix + ".metadata",
	}
	for index, record := range records {
		_, statErr := os.Stat(record)
		if fault && index == 1 {
			if !os.IsNotExist(statErr) {
				t.Fatalf("fault case retained deleted Go stderr record %q: %v", record, statErr)
			}
			continue
		}
		if statErr != nil {
			t.Fatalf("required synthetic record missing %q: %v", record, statErr)
		}
	}
	statusRecord := readFixturePath(t, prefix+".status")
	for _, expected := range []string{"go_status=0", "capture_status=0", "checker_status=0", "status_record_write_status=0"} {
		if !strings.Contains(statusRecord, expected+"\n") {
			t.Fatalf("status record lacks %q:\n%s", expected, statusRecord)
		}
	}
	if !strings.Contains(readFixturePath(t, prefix+".checker.stdout"), "required execution-proof pass: profile="+profile) {
		t.Fatalf("genuine checker success was not retained")
	}
	if _, statErr := os.Stat(unrelatedPath); statErr != nil {
		t.Fatalf("unrelated diagnostic evidence was not retained: %v", statErr)
	}
	if fault && !strings.Contains(output, "missing or unreadable CK-015") {
		t.Fatalf("fault did not identify missing invocation record:\n%s", output)
	}
	if kind == "local" {
		_, continuationErr := os.Stat(continuationPath)
		if fault && !os.IsNotExist(continuationErr) {
			t.Fatalf("local fault reached continuation sentinel: %v", continuationErr)
		}
		if !fault && continuationErr != nil {
			t.Fatalf("local success did not reach continuation sentinel: %v", continuationErr)
		}
	}

	outcome := "success"
	if fault {
		outcome = "missing-go-stderr-rejected"
	}
	injectionAction := "none"
	if fault {
		injectionAction = "deleted-after-genuine-checker-success:" + prefix + ".go.stderr"
	}
	if writeErr := os.WriteFile(filepath.Join(caseRoot, "injection-trace.txt"), []byte(injectionAction+"\n"), 0o600); writeErr != nil {
		t.Fatalf("retain injection trace: %v", writeErr)
	}
	var inventory strings.Builder
	for _, record := range records {
		state := "present"
		if _, statErr := os.Stat(record); os.IsNotExist(statErr) {
			state = "missing"
		}
		fmt.Fprintf(&inventory, "%s\t%s\n", state, filepath.Base(record))
	}
	if writeErr := os.WriteFile(filepath.Join(caseRoot, "record-inventory.txt"), []byte(inventory.String()), 0o600); writeErr != nil {
		t.Fatalf("retain record inventory: %v", writeErr)
	}
	result := fmt.Sprintf("wrapper=%s\nkind=%s\nprofile=%s\nfault=%t\noutcome=%s\nother_records=5\nstatus_values=0,0,0,0\nunrelated_evidence=retained\n", name, kind, profile, fault, outcome)
	if writeErr := os.WriteFile(filepath.Join(caseRoot, "result.txt"), []byte(result), 0o600); writeErr != nil {
		t.Fatalf("retain synthetic result: %v", writeErr)
	}
}

func ck015SyntheticSelector(t *testing.T, profile string) string {
	t.Helper()
	switch profile {
	case "ck015-initial-lookup-sqlite":
		return "^(TestCKV11316015InitialLookupOperationalErrorStopsStoreBeforeFallbackSQLite|TestCKV11316015InitialLookupPartialScanErrorStopsStoreBeforeFallbackSQLite|TestCKV11316015InitialLookupErrNoRowsPreservesNewObjectStoreSQLite|TestCKV11316015InitialLookupSupportedStatusRoutingSQLite)$"
	case "ck015-initial-lookup-postgres":
		return "^(TestCKV11316015PostgresInitialLookupOperationalErrorStopsStoreBeforeFallback|TestCKV11316015PostgresOpenLocalStorageRepairAndRecovery|TestCKV11316015PostgresRepairPublisherLocksChunkBeforeAuthorityMutation|TestCKV11316015PostgresRepairCompetitorWinsChunkLockBeforePublication|TestCKV11316015PostgresSharedChunkHealingBetweenValidationAndPlanReclassifies)$"
	default:
		t.Fatalf("unknown synthetic CK-015 profile %q", profile)
		return ""
	}
}

func writeExecutableFixture(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o700); err != nil {
		t.Fatalf("write executable fixture %q: %v", path, err)
	}
}

func readFixturePath(t *testing.T, path string) string {
	t.Helper()
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read fixture path %q: %v", path, err)
	}
	return string(content)
}

func copyFixtureTree(t *testing.T, sourceRoot, destinationRoot string) {
	t.Helper()
	if _, err := os.Stat(destinationRoot); !os.IsNotExist(err) {
		t.Fatalf("refusing to replace synthetic evidence destination %q", destinationRoot)
	}
	if err := filepath.WalkDir(sourceRoot, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		relative, err := filepath.Rel(sourceRoot, path)
		if err != nil {
			return err
		}
		destination := filepath.Join(destinationRoot, relative)
		if entry.IsDir() {
			return os.MkdirAll(destination, 0o700)
		}
		content, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		return os.WriteFile(destination, content, 0o600)
	}); err != nil {
		t.Fatalf("export synthetic evidence: %v", err)
	}
}

func TestAuditCIEnforcementRequiresCK014ProfileAParity(t *testing.T) {
	workflow := readRepoFile(t, filepath.Join(".github", "workflows", "ci.yml"))
	codeqlWorkflow := readRepoFile(t, filepath.Join(".github", "workflows", "codeql.yml"))
	checklist := readRepoFile(t, "PRE_RELEASE_CHECKLIST.md")
	tests := []struct {
		name        string
		old         string
		replacement string
		wantMessage string
	}{
		{
			name:        "missing codec profile",
			old:         "--profile \"integration-correctness-${codec}\"",
			replacement: "--profile \"integration-correctness-removed\"",
			wantMessage: "local Profile A selects codec-specific required-event profile",
		},
		{
			name:        "missing internal profile",
			old:         "--profile ck014-internal-verify",
			replacement: "--profile removed-internal-verify",
			wantMessage: "local Profile A invokes CK-014 internal required-event profile",
		},
		{
			name:        "pipeline status not snapshotted",
			old:         "pipeline_status=(\"${PIPESTATUS[@]}\")",
			replacement: "pipeline_status=(0 0)",
			wantMessage: "local Profile A snapshots complete pipeline status",
		},
		{
			name:        "capture failure suppressed",
			old:         "elif [ \"$capture_status\" -ne 0 ]; then",
			replacement: "elif false; then",
			wantMessage: "local Profile A propagates evidence-capture failure",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			mutated := strings.Replace(checklist, test.old, test.replacement, 1)
			if mutated == checklist {
				t.Fatalf("checklist fixture did not contain %q", test.old)
			}
			stderr := runAuditLocalOnlyWithChecklistFixture(t, workflow, codeqlWorkflow, mutated, true)
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

func TestAuditCIEnforcementRequiresExactFullAdversarialTimeoutParity(t *testing.T) {
	workflow := readRepoFile(t, filepath.Join(".github", "workflows", "ci.yml"))
	codeqlWorkflow := readRepoFile(t, filepath.Join(".github", "workflows", "codeql.yml"))
	checklist := readRepoFile(t, "PRE_RELEASE_CHECKLIST.md")

	const hostedWithoutTimeout = "go test -race -count=1 -json ./tests/adversarial/..."
	const hostedWithTimeout = hostedWithoutTimeout + " -timeout 20m"
	const localWithoutTimeout = "COLDKEEP_LONG_RUN=1 go test -race -count=1 ./tests/adversarial/..."
	const localWithTimeout = localWithoutTimeout + " -timeout 20m"

	validWorkflow := workflow
	if !strings.Contains(validWorkflow, hostedWithTimeout) {
		validWorkflow = strings.Replace(validWorkflow, hostedWithoutTimeout, hostedWithTimeout, 1)
		if validWorkflow == workflow {
			t.Fatalf("workflow fixture did not contain %q", hostedWithoutTimeout)
		}
	}
	validChecklist := checklist
	if !strings.Contains(validChecklist, localWithTimeout) {
		validChecklist = strings.Replace(validChecklist, localWithoutTimeout, localWithTimeout, 1)
		if validChecklist == checklist {
			t.Fatalf("checklist fixture did not contain %q", localWithoutTimeout)
		}
	}

	runAuditLocalOnlyWithChecklistFixture(
		t,
		validWorkflow,
		codeqlWorkflow,
		validChecklist,
		false,
	)

	tests := []struct {
		name        string
		workflow    string
		checklist   string
		wantMessage string
	}{
		{
			name:        "hosted timeout omitted",
			workflow:    strings.Replace(validWorkflow, hostedWithTimeout, hostedWithoutTimeout, 1),
			checklist:   validChecklist,
			wantMessage: "hosted full long-run adversarial package uses exact 20-minute timeout",
		},
		{
			name:        "hosted timeout disabled",
			workflow:    strings.Replace(validWorkflow, hostedWithTimeout, hostedWithoutTimeout+" -timeout 0", 1),
			checklist:   validChecklist,
			wantMessage: "hosted full long-run adversarial package uses exact 20-minute timeout",
		},
		{
			name:        "hosted timeout wrong",
			workflow:    strings.Replace(validWorkflow, hostedWithTimeout, hostedWithoutTimeout+" -timeout 30m", 1),
			checklist:   validChecklist,
			wantMessage: "hosted full long-run adversarial package uses exact 20-minute timeout",
		},
		{
			name:        "local timeout omitted",
			workflow:    validWorkflow,
			checklist:   strings.Replace(validChecklist, localWithTimeout, localWithoutTimeout, 1),
			wantMessage: "local Profile A full long-run adversarial package uses exact 20-minute timeout",
		},
		{
			name:        "local timeout disabled",
			workflow:    validWorkflow,
			checklist:   strings.Replace(validChecklist, localWithTimeout, localWithoutTimeout+" -timeout 0", 1),
			wantMessage: "local Profile A full long-run adversarial package uses exact 20-minute timeout",
		},
		{
			name:        "local and hosted disagree",
			workflow:    validWorkflow,
			checklist:   strings.Replace(validChecklist, localWithTimeout, localWithoutTimeout+" -timeout 15m", 1),
			wantMessage: "local Profile A full long-run adversarial package uses exact 20-minute timeout",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			stderr := runAuditLocalOnlyWithChecklistFixture(
				t,
				test.workflow,
				codeqlWorkflow,
				test.checklist,
				true,
			)
			if !strings.Contains(stderr, test.wantMessage) {
				t.Fatalf("expected %q, got:\n%s", test.wantMessage, stderr)
			}
		})
	}
}

func withCK015CoreAuditCase(t *testing.T, caseID string, run func() string) string {
	t.Helper()
	if os.Getenv("COLDKEEP_CK015_NEGATIVE_EVIDENCE_DIR") == "" {
		return run()
	}
	const name = "COLDKEEP_CK015_CORE_CASE_ID"
	previous, existed := os.LookupEnv(name)
	if err := os.Setenv(name, caseID); err != nil {
		t.Fatalf("set core audit case ID: %v", err)
	}
	defer func() {
		var err error
		if existed {
			err = os.Setenv(name, previous)
		} else {
			err = os.Unsetenv(name)
		}
		if err != nil {
			t.Errorf("restore core audit case ID: %v", err)
		}
	}()
	return run()
}

func allocateCK015CoreEvidenceCase(root, caseID string) (string, error) {
	if root == "" || caseID == "" || filepath.Base(caseID) != caseID || caseID == "." || caseID == ".." {
		return "", fmt.Errorf("invalid CK-015 core evidence root/case ID")
	}
	info, err := os.Lstat(root)
	if err != nil {
		return "", fmt.Errorf("inspect CK-015 core evidence root: %w", err)
	}
	if !info.IsDir() || info.Mode()&0o077 != 0 {
		return "", fmt.Errorf("CK-015 core evidence root must be a private directory")
	}
	caseRoot := filepath.Join(root, caseID)
	if err := os.Mkdir(caseRoot, 0o700); err != nil {
		return "", fmt.Errorf("create exclusive CK-015 core evidence case %q: %w", caseID, err)
	}
	return caseRoot, nil
}

func writeCK015EvidenceFile(path string, content []byte) error {
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	if _, err = file.Write(content); err == nil {
		err = file.Sync()
	}
	if closeErr := file.Close(); err == nil {
		err = closeErr
	}
	return err
}

func writeCK015EvidenceJSON(path string, value any) error {
	content, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	content = append(content, '\n')
	return writeCK015EvidenceFile(path, content)
}

func ck015FileSHA256(path string) (string, error) {
	content, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(content)
	return hex.EncodeToString(digest[:]), nil
}

func runCK015AuditWithEvidence(
	t *testing.T,
	cmd *exec.Cmd,
	caseID string,
	wantFailure bool,
	consumed map[string]string,
) (string, error) {
	t.Helper()
	root := os.Getenv("COLDKEEP_CK015_NEGATIVE_EVIDENCE_DIR")
	if root == "" {
		t.Fatalf("core case %q requires COLDKEEP_CK015_NEGATIVE_EVIDENCE_DIR", caseID)
	}
	caseRoot, err := allocateCK015CoreEvidenceCase(root, caseID)
	if err != nil {
		t.Fatalf("allocate core audit evidence: %v", err)
	}
	inputsRoot := filepath.Join(caseRoot, "inputs")
	baselineRoot := filepath.Join(caseRoot, "baseline")
	if err := os.Mkdir(inputsRoot, 0o700); err != nil {
		t.Fatalf("create core input evidence: %v", err)
	}
	if err := os.Mkdir(baselineRoot, 0o700); err != nil {
		t.Fatalf("create core baseline evidence: %v", err)
	}

	inputHashes := make(map[string]string, len(consumed))
	pathMap := make(map[string]string, len(consumed))
	for name, sourcePath := range consumed {
		content, readErr := os.ReadFile(sourcePath)
		if readErr != nil {
			t.Fatalf("read consumed %s: %v", name, readErr)
		}
		destination := filepath.Join(inputsRoot, name)
		if writeErr := writeCK015EvidenceFile(destination, content); writeErr != nil {
			t.Fatalf("retain consumed %s: %v", name, writeErr)
		}
		digest := sha256.Sum256(content)
		inputHashes[name] = hex.EncodeToString(digest[:])
		pathMap[sourcePath] = destination
	}
	for name, repoPath := range map[string]string{
		"ci.yml":                   filepath.Join(repoRoot(t), ".github", "workflows", "ci.yml"),
		"PRE_RELEASE_CHECKLIST.md": filepath.Join(repoRoot(t), "PRE_RELEASE_CHECKLIST.md"),
	} {
		content, readErr := os.ReadFile(repoPath)
		if readErr != nil {
			t.Fatalf("read baseline %s: %v", name, readErr)
		}
		if writeErr := writeCK015EvidenceFile(filepath.Join(baselineRoot, name), content); writeErr != nil {
			t.Fatalf("retain baseline %s: %v", name, writeErr)
		}
	}
	diff := exec.Command("git", "diff", "--no-index", "--", baselineRoot, inputsRoot)
	diff.Dir = cmd.Dir
	diffOutput, diffErr := diff.CombinedOutput()
	if diffErr != nil {
		if exit, ok := diffErr.(*exec.ExitError); !ok || exit.ExitCode() != 1 {
			t.Fatalf("create exact fixture diff: %v\n%s", diffErr, diffOutput)
		}
	}
	if writeErr := writeCK015EvidenceFile(filepath.Join(caseRoot, "fixture.diff"), diffOutput); writeErr != nil {
		t.Fatalf("retain fixture diff: %v", writeErr)
	}

	head := strings.TrimSpace(runAuditTestCommand(t, "git", "rev-parse", "HEAD"))
	sourceHashes := map[string]string{}
	for _, relative := range []string{"scripts/audit_ci_enforcement.sh", "scripts/check_required_test_events.py"} {
		digest, hashErr := ck015FileSHA256(filepath.Join(repoRoot(t), relative))
		if hashErr != nil {
			t.Fatalf("hash source %s: %v", relative, hashErr)
		}
		sourceHashes[relative] = digest
	}
	metadata := map[string]any{
		"case_id": caseID, "test_name": t.Name(), "expected_failure": wantFailure,
		"candidate_head": head, "candidate_state": "pre-commit-or-exact-commit-as-observed",
		"source_hashes": sourceHashes, "consumed_input_hashes": inputHashes,
		"consumed_path_to_copy":      pathMap,
		"cleared_environment_policy": "external focused runner clears unrelated COLDKEEP/GITHUB/DB settings",
	}
	if writeErr := writeCK015EvidenceJSON(filepath.Join(caseRoot, "metadata.json"), metadata); writeErr != nil {
		t.Fatalf("retain core metadata: %v", writeErr)
	}
	if writeErr := writeCK015EvidenceJSON(filepath.Join(caseRoot, "argv.json"), cmd.Args); writeErr != nil {
		t.Fatalf("retain core argv: %v", writeErr)
	}
	if writeErr := writeCK015EvidenceFile(filepath.Join(caseRoot, "cwd"), []byte(cmd.Dir+"\n")); writeErr != nil {
		t.Fatalf("retain core cwd: %v", writeErr)
	}
	selectedEnvironment := map[string]string{}
	for _, binding := range cmd.Env {
		if strings.HasPrefix(binding, "COLDKEEP_") || strings.HasPrefix(binding, "GITHUB_") || strings.HasPrefix(binding, "PYTHONDONTWRITEBYTECODE=") {
			name, value, _ := strings.Cut(binding, "=")
			selectedEnvironment[name] = value
		}
	}
	if writeErr := writeCK015EvidenceJSON(filepath.Join(caseRoot, "selected-environment.json"), selectedEnvironment); writeErr != nil {
		t.Fatalf("retain selected environment: %v", writeErr)
	}
	if writeErr := writeCK015EvidenceJSON(filepath.Join(caseRoot, "input-hashes-before.json"), inputHashes); writeErr != nil {
		t.Fatalf("retain before hashes: %v", writeErr)
	}

	stdoutFile, err := os.OpenFile(filepath.Join(caseRoot, "stdout"), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		t.Fatalf("open core stdout: %v", err)
	}
	stderrFile, err := os.OpenFile(filepath.Join(caseRoot, "stderr"), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		_ = stdoutFile.Close()
		t.Fatalf("open core stderr: %v", err)
	}
	var stdoutBuffer, stderrBuffer bytes.Buffer
	cmd.Stdout = io.MultiWriter(stdoutFile, &stdoutBuffer)
	cmd.Stderr = io.MultiWriter(stderrFile, &stderrBuffer)
	cmd.Stdin = strings.NewReader("")
	started := time.Now().UTC()
	if writeErr := writeCK015EvidenceFile(filepath.Join(caseRoot, "started-utc"), []byte(started.Format(time.RFC3339Nano)+"\n")); writeErr != nil {
		t.Fatalf("retain child start: %v", writeErr)
	}
	runErr := cmd.Run()
	ended := time.Now().UTC()
	for name, file := range map[string]*os.File{"stdout": stdoutFile, "stderr": stderrFile} {
		if syncErr := file.Sync(); syncErr != nil {
			t.Fatalf("sync core %s: %v", name, syncErr)
		}
		if closeErr := file.Close(); closeErr != nil {
			t.Fatalf("close core %s: %v", name, closeErr)
		}
	}
	if writeErr := writeCK015EvidenceFile(filepath.Join(caseRoot, "ended-utc"), []byte(ended.Format(time.RFC3339Nano)+"\n")); writeErr != nil {
		t.Fatalf("retain child end: %v", writeErr)
	}
	exitCode := 0
	startedChild := true
	exited := true
	launchError := ""
	if runErr != nil {
		if exit, ok := runErr.(*exec.ExitError); ok {
			exitCode = exit.ExitCode()
		} else {
			startedChild = false
			exited = false
			exitCode = -1
			launchError = runErr.Error()
		}
	}
	status := map[string]any{"started": startedChild, "exited": exited, "exit_code": exitCode, "launch_error": launchError}
	if writeErr := writeCK015EvidenceJSON(filepath.Join(caseRoot, "status.json"), status); writeErr != nil {
		t.Fatalf("retain child status: %v", writeErr)
	}
	combined := stdoutBuffer.String() + stderrBuffer.String()
	terminal := map[string]any{
		"pass_marker":    strings.Contains(combined, "[audit] PASSED:"),
		"fail_marker":    strings.Contains(combined, "[audit] FAILED:"),
		"error_marker":   strings.Contains(combined, "[audit] ERROR:"),
		"classification": map[bool]string{true: "intended-rejection", false: "positive-control"}[wantFailure],
	}
	if writeErr := writeCK015EvidenceJSON(filepath.Join(caseRoot, "terminal-observation.json"), terminal); writeErr != nil {
		t.Fatalf("retain terminal observation: %v", writeErr)
	}
	afterHashes := make(map[string]string, len(consumed))
	for name, sourcePath := range consumed {
		digest, hashErr := ck015FileSHA256(sourcePath)
		if hashErr != nil {
			t.Fatalf("hash consumed input after child %s: %v", name, hashErr)
		}
		afterHashes[name] = digest
	}
	if writeErr := writeCK015EvidenceJSON(filepath.Join(caseRoot, "input-hashes-after.json"), afterHashes); writeErr != nil {
		t.Fatalf("retain after hashes: %v", writeErr)
	}
	if !mapsEqualString(inputHashes, afterHashes) {
		t.Fatalf("core audit child changed its consumed inputs")
	}
	return combined, runErr
}

func mapsEqualString(left, right map[string]string) bool {
	if len(left) != len(right) {
		return false
	}
	for key, value := range left {
		if right[key] != value {
			return false
		}
	}
	return true
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

func runAuditLocalOnlyWithChecklistFixture(
	t *testing.T,
	workflow string,
	codeqlWorkflow string,
	checklist string,
	wantFailure bool,
) string {
	t.Helper()
	checklistPath := filepath.Join(t.TempDir(), "PRE_RELEASE_CHECKLIST.md")
	if err := os.WriteFile(checklistPath, []byte(checklist), 0o600); err != nil {
		t.Fatalf("write pre-release checklist fixture: %v", err)
	}
	t.Setenv("COLDKEEP_PRE_RELEASE_CHECKLIST_FILE", checklistPath)
	return runAuditLocalOnly(t, workflow, codeqlWorkflow, wantFailure)
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

	auditRoot := repoRoot(t)
	if configured := os.Getenv("COLDKEEP_AUDIT_TEST_REPO_ROOT"); configured != "" {
		auditRoot = configured
	}
	preReleaseChecklistPath := os.Getenv("COLDKEEP_PRE_RELEASE_CHECKLIST_FILE")
	if preReleaseChecklistPath == "" {
		preReleaseChecklistPath = filepath.Join(repoRoot(t), "PRE_RELEASE_CHECKLIST.md")
	}
	requiredTestEventsPath := os.Getenv("COLDKEEP_REQUIRED_TEST_EVENTS_FILE")
	if requiredTestEventsPath == "" {
		requiredTestEventsPath = filepath.Join(repoRoot(t), "scripts", "check_required_test_events.py")
	}
	cmd := exec.Command("bash", "scripts/audit_ci_enforcement.sh", "--local-only")
	cmd.Dir = auditRoot
	cmd.Env = append(os.Environ(),
		"COLDKEEP_CI_WORKFLOW_FILE="+workflowPath,
		"COLDKEEP_CODEQL_WORKFLOW_FILE="+codeqlWorkflowPath,
		"COLDKEEP_BENCHMARK_BASELINE_WORKFLOW_FILE="+baselineWorkflowPath,
		"COLDKEEP_TIMING_VALIDATOR_FILE="+timingValidatorPath,
		"COLDKEEP_VALIDATION_MATRIX_FILE="+matrixPath,
		"COLDKEEP_PRE_RELEASE_CHECKLIST_FILE="+preReleaseChecklistPath,
		"COLDKEEP_REQUIRED_TEST_EVENTS_FILE="+requiredTestEventsPath,
		"COLDKEEP_PAIRED_REFERENCE_MANIFEST_FILE="+pairedReferencePath,
		"COLDKEEP_PAIRED_THRESHOLD_POLICY_FILE="+pairedThresholdPath,
	)
	var output []byte
	var err error
	if caseID := os.Getenv("COLDKEEP_CK015_CORE_CASE_ID"); caseID != "" {
		var recorded string
		recorded, err = runCK015AuditWithEvidence(t, cmd, caseID, wantFailure, map[string]string{
			"ci.yml":                            workflowPath,
			"codeql.yml":                        codeqlWorkflowPath,
			"benchmark-baseline.yml":            baselineWorkflowPath,
			"validate_regression_thresholds.py": timingValidatorPath,
			"VALIDATION_MATRIX.md":              matrixPath,
			"PRE_RELEASE_CHECKLIST.md":          preReleaseChecklistPath,
			"check_required_test_events.py":     requiredTestEventsPath,
		})
		output = []byte(recorded)
	} else {
		output, err = cmd.CombinedOutput()
	}
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
