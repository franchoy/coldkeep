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

func runAuditLocalOnly(t *testing.T, workflow string, codeqlWorkflow string, wantFailure bool) string {
	t.Helper()

	tmpDir := t.TempDir()
	workflowPath := filepath.Join(tmpDir, "ci.yml")
	codeqlWorkflowPath := filepath.Join(tmpDir, "codeql.yml")
	matrixPath := filepath.Join(tmpDir, "VALIDATION_MATRIX.md")

	if err := os.WriteFile(workflowPath, []byte(workflow), 0o600); err != nil {
		t.Fatalf("write workflow fixture: %v", err)
	}
	if err := os.WriteFile(codeqlWorkflowPath, []byte(codeqlWorkflow), 0o600); err != nil {
		t.Fatalf("write codeql workflow fixture: %v", err)
	}
	if err := os.WriteFile(matrixPath, []byte(readRepoFile(t, "VALIDATION_MATRIX.md")), 0o600); err != nil {
		t.Fatalf("write validation matrix fixture: %v", err)
	}

	cmd := exec.Command("bash", "scripts/audit_ci_enforcement.sh", "--local-only")
	cmd.Dir = repoRoot(t)
	cmd.Env = append(os.Environ(),
		"COLDKEEP_CI_WORKFLOW_FILE="+workflowPath,
		"COLDKEEP_CODEQL_WORKFLOW_FILE="+codeqlWorkflowPath,
		"COLDKEEP_VALIDATION_MATRIX_FILE="+matrixPath,
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
