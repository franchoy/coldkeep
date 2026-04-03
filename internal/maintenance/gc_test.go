package maintenance

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	dbschema "github.com/franchoy/coldkeep/db"
	"github.com/franchoy/coldkeep/internal/container"
	"github.com/franchoy/coldkeep/internal/db"
)

func requireDB(t *testing.T) {
	t.Helper()
	if os.Getenv("COLDKEEP_TEST_DB") == "" {
		t.Skip("Set COLDKEEP_TEST_DB=1 to run DB-backed maintenance tests")
	}
}

func applySchema(t *testing.T, dbconn *sql.DB) {
	t.Helper()

	var logicalFileTable sql.NullString
	if err := dbconn.QueryRow(`SELECT to_regclass('public.logical_file')`).Scan(&logicalFileTable); err == nil && logicalFileTable.Valid {
		return
	}

	if strings.TrimSpace(dbschema.PostgresSchema) == "" {
		t.Fatalf("embedded postgres schema is empty")
	}

	if _, err := dbconn.Exec(dbschema.PostgresSchema); err != nil && !isDuplicateSchemaError(err) {
		t.Fatalf("apply schema: %v", err)
	}
}

func isDuplicateSchemaError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, sql.ErrNoRows) {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "already exists") || strings.Contains(msg, "42710")
}

func resetDB(t *testing.T, dbconn *sql.DB) {
	t.Helper()
	_, err := dbconn.Exec(`
		TRUNCATE TABLE
			file_chunk,
			chunk,
			logical_file,
			container
		RESTART IDENTITY CASCADE
	`)
	if err != nil {
		t.Fatalf("truncate tables: %v", err)
	}
}

func TestRunGCWithAdvisoryUnlockFailureStillSucceeds(t *testing.T) {
	requireDB(t)

	dbconn, err := db.ConnectDB()
	if err != nil {
		t.Fatalf("connect db: %v", err)
	}
	defer dbconn.Close()

	applySchema(t, dbconn)
	resetDB(t, dbconn)

	containersDir := t.TempDir()
	originalContainersDir := container.ContainersDir
	t.Cleanup(func() {
		container.ContainersDir = originalContainersDir
	})
	container.ContainersDir = containersDir

	filename := "gc-unlock-failure.bin"
	containerPath := filepath.Join(containersDir, filename)
	if err := os.WriteFile(containerPath, []byte("gc unlock failure test"), 0o644); err != nil {
		t.Fatalf("write container file: %v", err)
	}

	if _, err := dbconn.Exec(
		`INSERT INTO container (filename, current_size, max_size, sealed, quarantine)
		 VALUES ($1, $2, $3, TRUE, FALSE)`,
		filename,
		int64(len("gc unlock failure test")),
		container.GetContainerMaxSize(),
	); err != nil {
		t.Fatalf("insert container row: %v", err)
	}

	originalUnlock := gcAdvisoryUnlock
	gcAdvisoryUnlock = func(_ context.Context, _ *sql.DB) error {
		return errors.New("forced advisory unlock failure")
	}
	t.Cleanup(func() {
		gcAdvisoryUnlock = originalUnlock
	})

	result, err := RunGCWithContainersDirResult(false, containersDir)
	if err != nil {
		t.Fatalf("gc should succeed despite advisory unlock failure: %v", err)
	}
	if result.AffectedContainers != 1 {
		t.Fatalf("expected one affected container, got %d", result.AffectedContainers)
	}

	var remaining int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM container`).Scan(&remaining); err != nil {
		t.Fatalf("count container rows: %v", err)
	}
	if remaining != 0 {
		t.Fatalf("expected container row to be deleted, got %d", remaining)
	}
}
