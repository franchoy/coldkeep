package recovery

import (
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	"github.com/franchoy/coldkeep/internal/container"
	"github.com/franchoy/coldkeep/internal/db"
	_ "github.com/mattn/go-sqlite3"
)

func TestQuarantineOrphanContainersAcceptsDirectQuarantineAfterSizeSync(t *testing.T) {
	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	defer func() { _ = dbconn.Close() }()
	dbconn.SetMaxOpenConns(1)
	dbconn.SetMaxIdleConns(1)

	if err := db.RunMigrations(dbconn); err != nil {
		t.Fatalf("run migrations: %v", err)
	}

	containersDir := t.TempDir()
	filename := "strict-recovery-orphan.bin"
	path := filepath.Join(containersDir, filename)

	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("create container file: %v", err)
	}
	if _, err := f.Write([]byte("stale-quarantine-row-physical-file")); err != nil {
		_ = f.Close()
		t.Fatalf("write container file: %v", err)
	}
	if _, err := f.Write([]byte("payload-that-makes-the-file-larger-than-the-row")); err != nil {
		_ = f.Close()
		t.Fatalf("append payload: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close container file: %v", err)
	}

	res, err := dbconn.Exec(`
		INSERT INTO container (filename, current_size, max_size, sealed, sealing, quarantine)
		VALUES (?, ?, ?, FALSE, TRUE, FALSE)
	`, filename, container.ContainerHdrLen, container.ContainerHdrLen+256)
	if err != nil {
		t.Fatalf("insert container row: %v", err)
	}
	containerID, err := res.LastInsertId()
	if err != nil {
		t.Fatalf("last insert id: %v", err)
	}

	if err := container.QuarantineContainerInDir(dbconn, containerID, containersDir); err != nil {
		t.Fatalf("direct quarantine: %v", err)
	}

	var quarantine int
	var currentSize int64
	var maxSize int64
	if err := dbconn.QueryRow(`SELECT quarantine, current_size, max_size FROM container WHERE id = ?`, containerID).Scan(&quarantine, &currentSize, &maxSize); err != nil {
		t.Fatalf("query direct quarantine row: %v", err)
	}
	if quarantine != 1 {
		t.Fatalf("expected direct quarantine to mark row quarantined, got %d", quarantine)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat container file: %v", err)
	}
	if currentSize != info.Size() || maxSize != info.Size() {
		t.Fatalf("expected direct quarantine size sync to %d, got current=%d max=%d", info.Size(), currentSize, maxSize)
	}

	stats := &recoveryStats{}
	if err := quarantineOrphanContainers(dbconn, containersDir, stats); err != nil {
		t.Fatalf("strict orphan recovery should accept reconciled quarantine row, got: %v", err)
	}
	if stats.quarantinedOrphan != 0 {
		t.Fatalf("expected no new orphan rows, got %d", stats.quarantinedOrphan)
	}
}

func TestQuarantineOrphanContainersResyncsExistingQuarantineSizeDrift(t *testing.T) {
	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	defer func() { _ = dbconn.Close() }()
	dbconn.SetMaxOpenConns(1)
	dbconn.SetMaxIdleConns(1)

	if err := db.RunMigrations(dbconn); err != nil {
		t.Fatalf("run migrations: %v", err)
	}

	containersDir := t.TempDir()
	filename := "quarantine-size-drift.bin"
	path := filepath.Join(containersDir, filename)
	content := []byte("quarantined-file-grew-after-row-was-marked")
	if err := os.WriteFile(path, content, 0o644); err != nil {
		t.Fatalf("write container file: %v", err)
	}

	res, err := dbconn.Exec(`
		INSERT INTO container (filename, current_size, max_size, sealed, sealing, quarantine)
		VALUES (?, ?, ?, FALSE, FALSE, TRUE)
	`, filename, int64(len(content))-5, int64(len(content))-5)
	if err != nil {
		t.Fatalf("insert stale quarantine row: %v", err)
	}
	containerID, err := res.LastInsertId()
	if err != nil {
		t.Fatalf("last insert id: %v", err)
	}

	stats := &recoveryStats{}
	if err := quarantineOrphanContainers(dbconn, containersDir, stats); err != nil {
		t.Fatalf("resync quarantined orphan row: %v", err)
	}

	var quarantine int
	var currentSize int64
	var maxSize int64
	if err := dbconn.QueryRow(`SELECT quarantine, current_size, max_size FROM container WHERE id = ?`, containerID).Scan(&quarantine, &currentSize, &maxSize); err != nil {
		t.Fatalf("query resynced container row: %v", err)
	}
	if quarantine != 1 {
		t.Fatalf("expected row to remain quarantined, got %d", quarantine)
	}
	if currentSize != int64(len(content)) || maxSize != int64(len(content)) {
		t.Fatalf("expected resynced sizes=%d, got current=%d max=%d", len(content), currentSize, maxSize)
	}
	if stats.quarantinedOrphan != 0 {
		t.Fatalf("expected no new orphan rows, got %d", stats.quarantinedOrphan)
	}
}
