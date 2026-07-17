package testutils

import (
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type fakeSQLExecDB struct {
	queries []string
	err     error
}

func (f *fakeSQLExecDB) Exec(query string, args ...any) (sql.Result, error) {
	f.queries = append(f.queries, query)
	return nil, f.err
}

func TestShouldPreserveFailedIsolatedPostgresDB(t *testing.T) {
	t.Setenv(preserveFailureStateEnv, "1")
	if !shouldPreserveFailedIsolatedPostgresDB(1) {
		t.Fatal("expected failing exit code to preserve isolated DB when enabled")
	}
	if shouldPreserveFailedIsolatedPostgresDB(0) {
		t.Fatal("expected successful exit code not to preserve isolated DB")
	}
}

func TestFinalizeIsolatedPostgresDBPreservesOnlyOnFailure(t *testing.T) {
	diagDir := t.TempDir()
	t.Setenv(preserveFailureStateEnv, "1")
	t.Setenv(diagnosticDirEnv, diagDir)

	adminDB := &fakeSQLExecDB{}
	exitCode := finalizeIsolatedPostgresDB(adminDB, "adversarial", "coldkeep_adversarial_1", 1)
	if exitCode != 1 {
		t.Fatalf("expected failing exit code to be preserved, got %d", exitCode)
	}
	if len(adminDB.queries) != 0 {
		t.Fatalf("expected no cleanup queries for preserved DB, got %d", len(adminDB.queries))
	}

	entries, err := os.ReadDir(diagDir)
	if err != nil {
		t.Fatalf("read diagnostic dir: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected one preserved DB manifest, got %d", len(entries))
	}
}

func TestFinalizeIsolatedPostgresDBDropsSuccessfulDatabase(t *testing.T) {
	diagDir := t.TempDir()
	t.Setenv(preserveFailureStateEnv, "1")
	t.Setenv(diagnosticDirEnv, diagDir)

	adminDB := &fakeSQLExecDB{}
	exitCode := finalizeIsolatedPostgresDB(adminDB, "integration", "coldkeep_integration_1", 0)
	if exitCode != 0 {
		t.Fatalf("expected success exit code, got %d", exitCode)
	}
	if len(adminDB.queries) != 2 {
		t.Fatalf("expected terminate and drop queries for successful DB, got %d", len(adminDB.queries))
	}
	entries, err := os.ReadDir(diagDir)
	if err != nil {
		t.Fatalf("read diagnostic dir: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("expected no diagnostic manifest for successful DB, got %d file(s)", len(entries))
	}
}

func TestFinalizeIsolatedPostgresDBManifestWarningDoesNotChangeFailureExitCode(t *testing.T) {
	filePath := filepath.Join(t.TempDir(), "not-a-dir")
	if err := os.WriteFile(filePath, []byte("x"), 0o600); err != nil {
		t.Fatalf("write diagnostic dir fixture: %v", err)
	}
	t.Setenv(preserveFailureStateEnv, "1")
	t.Setenv(diagnosticDirEnv, filePath)

	adminDB := &fakeSQLExecDB{}
	exitCode := finalizeIsolatedPostgresDB(adminDB, "adversarial", "coldkeep_adversarial_1", 1)
	if exitCode != 1 {
		t.Fatalf("expected original failure exit code, got %d", exitCode)
	}
	if len(adminDB.queries) != 0 {
		t.Fatalf("expected no cleanup queries for preserved failure DB, got %d", len(adminDB.queries))
	}
}

func TestFinalizeIsolatedPostgresDBDropsUnrelatedHelperDatabaseWhenG6ManifestExists(t *testing.T) {
	diagDir := t.TempDir()
	t.Setenv(preserveFailureStateEnv, "1")
	t.Setenv(diagnosticDirEnv, diagDir)

	if _, err := WriteDiagnosticJSON("g6-failure", map[string]any{
		"isolated_database_name": "coldkeep_adversarial_g6_123",
	}); err != nil {
		t.Fatalf("write G6 manifest fixture: %v", err)
	}

	adminDB := &fakeSQLExecDB{}
	exitCode := finalizeIsolatedPostgresDB(adminDB, "adversarial", "coldkeep_adversarial_1", 1)
	if exitCode != 1 {
		t.Fatalf("expected failure exit code to remain unchanged, got %d", exitCode)
	}
	if len(adminDB.queries) != 2 {
		t.Fatalf("expected unrelated helper DB to be dropped, got %d query calls", len(adminDB.queries))
	}
}

func TestTrustedIsolatedPostgresIdentifierRejectsUnexpectedNames(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic for invalid isolated DB identifier")
		}
	}()
	_ = trustedIsolatedPostgresIdentifier("bad-name!")
}

func TestSanitizeDiagnosticNameRemovesUnsafeCharacters(t *testing.T) {
	got := sanitizeDiagnosticName(" g6 / same file ")
	if strings.ContainsAny(got, " /") {
		t.Fatalf("expected sanitized diagnostic name, got %q", got)
	}
}
