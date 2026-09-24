package container

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/franchoy/coldkeep/internal/db"
	"github.com/franchoy/coldkeep/internal/fsx"
	_ "github.com/mattn/go-sqlite3"
)

func TestRepairStagingContainerIsDurableAtFinalPathBeforeReady(t *testing.T) {
	dbconn, attemptID := newRepairStagingTestDB(t)
	dir := t.TempDir()
	payload := []byte("durable-repair-payload")
	staged, err := CreateRepairStagingContainer(context.Background(), dbconn, attemptID, dir, payload, 1<<20)
	if err != nil {
		t.Fatalf("create repair staging container: %v", err)
	}
	if filepath.Dir(staged.Path) != dir || filepath.Base(staged.Path) != staged.Filename {
		t.Fatalf("staged path is not its final container path: %+v", staged)
	}
	got, err := os.ReadFile(staged.Path)
	if err != nil {
		t.Fatalf("read durable repair staging file: %v", err)
	}
	if string(got[ContainerHdrLen:]) != string(payload) {
		t.Fatalf("staged payload differs: got=%q want=%q", got[ContainerHdrLen:], payload)
	}
	var sealed, quarantine bool
	var status string
	if err := dbconn.QueryRow(`
		SELECT c.sealed, c.quarantine, rc.status
		FROM container c JOIN store_repair_container rc ON rc.container_id = c.id
		WHERE c.id = $1`, staged.ID).Scan(&sealed, &quarantine, &status); err != nil {
		t.Fatalf("load durable staging metadata: %v", err)
	}
	if !sealed || !quarantine || status != "DURABLE" {
		t.Fatalf("staged metadata=(sealed=%t quarantine=%t status=%s)", sealed, quarantine, status)
	}
}

func TestRepairStagingSyncFailureNeverCreatesDurableAuthority(t *testing.T) {
	dbconn, attemptID := newRepairStagingTestDB(t)
	dir := t.TempDir()
	wantErr := errors.New("injected file sync failure")
	originalOpen := repairStagingOpenFile
	repairStagingOpenFile = func(path string, flag int, perm os.FileMode) (fsx.File, error) {
		file, err := os.OpenFile(path, flag, perm)
		if err != nil {
			return nil, err
		}
		return &syncFailRepairFile{File: file, err: wantErr}, nil
	}
	t.Cleanup(func() { repairStagingOpenFile = originalOpen })

	_, err := CreateRepairStagingContainer(context.Background(), dbconn, attemptID, dir, []byte("payload"), 1<<20)
	if !errors.Is(err, wantErr) {
		t.Fatalf("staging error=%v, want injected sync failure", err)
	}
	var durable int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM store_repair_container WHERE attempt_id = $1 AND status = 'DURABLE'`, attemptID).Scan(&durable); err != nil {
		t.Fatalf("count durable staging rows: %v", err)
	}
	if durable != 0 {
		t.Fatalf("sync-failed staging exposed %d durable rows", durable)
	}
}

type syncFailRepairFile struct {
	*os.File
	err error
}

func (f *syncFailRepairFile) Sync() error { return f.err }

func newRepairStagingTestDB(t *testing.T) (*sql.DB, int64) {
	t.Helper()
	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	dbconn.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = dbconn.Close() })
	if err := db.RunMigrations(dbconn); err != nil {
		t.Fatalf("run migrations: %v", err)
	}
	var logicalID int64
	if err := dbconn.QueryRow(`
		INSERT INTO logical_file (original_name, total_size, file_hash, status, ref_count, chunker_version)
		VALUES ('repair', 1, 'repair-hash', 'COMPLETED', 0, 'v2-fastcdc') RETURNING id`,
	).Scan(&logicalID); err != nil {
		t.Fatalf("insert logical file: %v", err)
	}
	var attemptID int64
	if err := dbconn.QueryRow(`
		INSERT INTO store_repair_attempt
		 (logical_file_id, source_file_hash, source_total_size, recipe_fingerprint, status)
		VALUES ($1, 'repair-hash', 1, 'fingerprint', 'PREPARING') RETURNING id`, logicalID,
	).Scan(&attemptID); err != nil {
		t.Fatalf("insert repair attempt: %v", err)
	}
	return dbconn, attemptID
}
