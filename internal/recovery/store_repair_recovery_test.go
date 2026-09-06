package recovery

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	"github.com/franchoy/coldkeep/internal/container"
)

func TestStoreRepairRecoveryCleansOnlyAbandonedPrePublicationState(t *testing.T) {
	dbconn := openRecoveryTestDB(t)
	defer func() { _ = dbconn.Close() }()
	containersDir := t.TempDir()

	logicalID := insertRecoveryRepairLogicalFile(t, dbconn, "abandoned")
	var attemptID int64
	if err := dbconn.QueryRow(`
		INSERT INTO store_repair_attempt
		 (logical_file_id, source_file_hash, source_total_size, recipe_fingerprint, status)
		VALUES ($1, 'source', 1, 'fingerprint', 'READY') RETURNING id`, logicalID,
	).Scan(&attemptID); err != nil {
		t.Fatalf("insert abandoned repair attempt: %v", err)
	}
	filename := "container_repair_abandoned.bin"
	path := filepath.Join(containersDir, filename)
	if err := os.WriteFile(path, make([]byte, container.ContainerHdrLen), 0o600); err != nil {
		t.Fatalf("write abandoned repair container: %v", err)
	}
	var containerID int64
	if err := dbconn.QueryRow(`
		INSERT INTO container (filename, sealed, quarantine, current_size, max_size)
		VALUES ($1, TRUE, TRUE, $2, $2 + 1) RETURNING id`, filename, container.ContainerHdrLen,
	).Scan(&containerID); err != nil {
		t.Fatalf("insert abandoned repair container: %v", err)
	}
	if _, err := dbconn.Exec(`
		INSERT INTO store_repair_container
		 (attempt_id, container_id, physical_size, physical_hash, status)
		VALUES ($1,$2,$3,'hash','DURABLE')`, attemptID, containerID, container.ContainerHdrLen,
	); err != nil {
		t.Fatalf("link abandoned repair container: %v", err)
	}

	if err := recoverAbandonedStoreRepairsWithContext(context.Background(), dbconn, containersDir); err != nil {
		t.Fatalf("recover abandoned repair: %v", err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("abandoned staging file still exists: %v", err)
	}
	var status string
	if err := dbconn.QueryRow(`SELECT status FROM store_repair_attempt WHERE id = $1`, attemptID).Scan(&status); err != nil {
		t.Fatalf("read recovered attempt: %v", err)
	}
	if status != "ABORTED" {
		t.Fatalf("recovered attempt status=%q, want ABORTED", status)
	}
	if err := recoverAbandonedStoreRepairsWithContext(context.Background(), dbconn, containersDir); err != nil {
		t.Fatalf("abandoned repair recovery must be idempotent: %v", err)
	}
}

func TestStoreRepairRecoveryPreservesPublishedState(t *testing.T) {
	dbconn := openRecoveryTestDB(t)
	defer func() { _ = dbconn.Close() }()
	containersDir := t.TempDir()
	logicalID := insertRecoveryRepairLogicalFile(t, dbconn, "published")
	var attemptID int64
	if err := dbconn.QueryRow(`
		INSERT INTO store_repair_attempt
		 (logical_file_id, source_file_hash, source_total_size, recipe_fingerprint, status)
		VALUES ($1, 'source', 1, 'fingerprint', 'PUBLISHED') RETURNING id`, logicalID,
	).Scan(&attemptID); err != nil {
		t.Fatalf("insert published repair attempt: %v", err)
	}
	filename := "container_repair_published.bin"
	path := filepath.Join(containersDir, filename)
	if err := os.WriteFile(path, make([]byte, container.ContainerHdrLen), 0o600); err != nil {
		t.Fatalf("write published repair container: %v", err)
	}
	var containerID int64
	if err := dbconn.QueryRow(`
		INSERT INTO container (filename, sealed, quarantine, current_size, max_size)
		VALUES ($1, TRUE, FALSE, $2, $2 + 1) RETURNING id`, filename, container.ContainerHdrLen,
	).Scan(&containerID); err != nil {
		t.Fatalf("insert published repair container: %v", err)
	}
	if _, err := dbconn.Exec(`
		INSERT INTO store_repair_container
		 (attempt_id, container_id, physical_size, physical_hash, status)
		VALUES ($1,$2,$3,'hash','PUBLISHED')`, attemptID, containerID, container.ContainerHdrLen,
	); err != nil {
		t.Fatalf("link published repair container: %v", err)
	}

	if err := recoverAbandonedStoreRepairsWithContext(context.Background(), dbconn, containersDir); err != nil {
		t.Fatalf("recover with published repair present: %v", err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("published repair file was removed: %v", err)
	}
	var status string
	if err := dbconn.QueryRow(`SELECT status FROM store_repair_attempt WHERE id = $1`, attemptID).Scan(&status); err != nil {
		t.Fatalf("read published attempt: %v", err)
	}
	if status != "PUBLISHED" {
		t.Fatalf("published attempt status changed to %q", status)
	}
}

func insertRecoveryRepairLogicalFile(t *testing.T, dbconn interface {
	QueryRow(string, ...any) *sql.Row
}, suffix string) int64 {
	t.Helper()
	var logicalID int64
	if err := dbconn.QueryRow(`
		INSERT INTO logical_file (original_name, total_size, file_hash, status, ref_count, chunker_version)
		VALUES ($1, 1, $2, 'COMPLETED', 0, 'v1-simple-rolling') RETURNING id`, suffix, "repair-"+suffix,
	).Scan(&logicalID); err != nil {
		t.Fatalf("insert repair recovery logical file: %v", err)
	}
	return logicalID
}
