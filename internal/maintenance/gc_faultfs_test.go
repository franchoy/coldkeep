package maintenance

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/franchoy/coldkeep/internal/container"
	"github.com/franchoy/coldkeep/internal/db"
	"github.com/franchoy/coldkeep/internal/fsx"
	"github.com/franchoy/coldkeep/internal/fsx/faultfs"
	_ "github.com/mattn/go-sqlite3"
)

func setupGCDeleteFaultFixture(t *testing.T) (*sql.DB, string, int64, string) {
	t.Helper()
	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	t.Cleanup(func() { _ = dbconn.Close() })

	if err := db.RunMigrations(dbconn); err != nil {
		t.Fatalf("run migrations: %v", err)
	}

	containersDir := t.TempDir()
	originalContainersDir := container.ContainersDir
	t.Cleanup(func() { container.ContainersDir = originalContainersDir })
	container.ContainersDir = containersDir

	filename := "gc-delete-fault.bin"
	containerPath := filepath.Join(containersDir, filename)
	payload := []byte("gc-delete-fault-payload")
	if err := os.WriteFile(containerPath, payload, 0o600); err != nil {
		t.Fatalf("write container file: %v", err)
	}

	var containerID int64
	if err := dbconn.QueryRow(
		`INSERT INTO container (filename, current_size, max_size, sealed, quarantine)
		 VALUES ($1, $2, $3, FALSE, FALSE)
		 RETURNING id`,
		filename,
		int64(len(payload)),
		container.GetContainerMaxSize(),
	).Scan(&containerID); err != nil {
		t.Fatalf("insert container row: %v", err)
	}

	var chunkID int64
	if err := dbconn.QueryRow(
		`INSERT INTO chunk (chunk_hash, size, status, live_ref_count, pin_count, chunker_version)
		 VALUES ($1, $2, 'COMPLETED', 0, 0, 'v2-fastcdc')
		 RETURNING id`,
		"gc-delete-fault-chunk",
		int64(len(payload)),
	).Scan(&chunkID); err != nil {
		t.Fatalf("insert chunk row: %v", err)
	}

	if _, err := dbconn.Exec(
		`INSERT INTO blocks (chunk_id, codec, format_version, plaintext_size, stored_size, container_id, block_offset)
		 VALUES ($1, 'plain', 1, $2, $3, $4, 0)`,
		chunkID,
		int64(len(payload)),
		int64(len(payload)),
		containerID,
	); err != nil {
		t.Fatalf("insert block row: %v", err)
	}

	return dbconn, containersDir, containerID, containerPath
}

func TestGCDeleteFaultFSRemoveFailureIsReturned(t *testing.T) {
	t.Parallel()

	dbconn, containersDir, containerID, containerPath := setupGCDeleteFaultFixture(t)

	script := faultfs.NewScript(faultfs.Fault{Op: faultfs.OpRemove, Err: faultfs.ErrFaultRemove})
	outcome, physicalBytes, err := sweepDeadActiveContainerResult(
		context.Background(),
		dbconn,
		containersDir,
		map[int64]struct{}{},
		livePhysicalUnits{LegacyLiveContainerIDs: map[int64]struct{}{}, PackedLiveBlockIDs: map[int64]struct{}{}},
		faultfs.New(fsx.Default(), script),
		containerID,
		filepath.Base(containerPath),
	)
	if !errors.Is(err, faultfs.ErrFaultRemove) {
		t.Fatalf("cleanup error = %v, want errors.Is(ErrFaultRemove)", err)
	}
	if outcome != sealedContainerSkipped || physicalBytes != 0 {
		t.Fatalf("remove-failed unit outcome=%v bytes=%d, want skipped/0", outcome, physicalBytes)
	}
	if got := script.CallCount(faultfs.OpRemove); got != 1 {
		t.Fatalf("remove call count = %d, want 1", got)
	}
	if _, err := os.Stat(containerPath); err != nil {
		t.Fatalf("expected container file to remain after remove failure, stat err=%v", err)
	}
	var remaining int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM container WHERE id = $1`, containerID).Scan(&remaining); err != nil {
		t.Fatalf("count container rows: %v", err)
	}
	if remaining != 0 {
		t.Fatalf("expected container row to be deleted despite remove failure, got %d", remaining)
	}
}
