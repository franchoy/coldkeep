package recovery

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/franchoy/coldkeep/internal/container"
	"github.com/franchoy/coldkeep/internal/db"
	"github.com/franchoy/coldkeep/internal/fsx"
	_ "github.com/mattn/go-sqlite3"
)

func TestRecoverSealingContainerRequiresExactAuthoritativeOccupancy(t *testing.T) {
	tests := []struct {
		name               string
		payloadSize        int64
		seed               func(*testing.T, *sql.DB, int64, int64)
		wantSealed         int
		wantQuarantined    int
		wantSealingCleared int
	}{
		{name: "empty exact payload", payloadSize: 0, wantSealed: 1, wantSealingCleared: 1},
		{name: "unowned payload", payloadSize: 32, wantQuarantined: 1, wantSealingCleared: 1},
		{name: "exact active legacy", payloadSize: 32, seed: func(t *testing.T, dbconn *sql.DB, containerID, payloadSize int64) {
			insertRecoveryLegacyExtent(t, dbconn, containerID, container.ContainerHdrLen, payloadSize, "exact-active")
		}, wantSealed: 1, wantSealingCleared: 1},
		{name: "legacy gap", payloadSize: 32, seed: func(t *testing.T, dbconn *sql.DB, containerID, _ int64) {
			insertRecoveryLegacyExtent(t, dbconn, containerID, container.ContainerHdrLen, 15, "gap-left")
			insertRecoveryLegacyExtent(t, dbconn, containerID, container.ContainerHdrLen+16, 16, "gap-right")
		}, wantQuarantined: 1, wantSealingCleared: 1},
		{name: "legacy overlap", payloadSize: 32, seed: func(t *testing.T, dbconn *sql.DB, containerID, _ int64) {
			insertRecoveryLegacyExtent(t, dbconn, containerID, container.ContainerHdrLen, 20, "overlap-left")
			insertRecoveryLegacyExtent(t, dbconn, containerID, container.ContainerHdrLen+16, 16, "overlap-right")
		}, wantQuarantined: 1, wantSealingCleared: 1},
		{name: "trailing unowned byte", payloadSize: 32, seed: func(t *testing.T, dbconn *sql.DB, containerID, payloadSize int64) {
			insertRecoveryLegacyExtent(t, dbconn, containerID, container.ContainerHdrLen, payloadSize-1, "trailing")
		}, wantQuarantined: 1, wantSealingCleared: 1},
		{name: "invalid extent before payload", payloadSize: 32, seed: func(t *testing.T, dbconn *sql.DB, containerID, payloadSize int64) {
			insertRecoveryLegacyExtent(t, dbconn, containerID, 0, payloadSize, "invalid-bound")
		}, wantQuarantined: 1, wantSealingCleared: 1},
		{name: "packed companion excluded", payloadSize: 32, seed: insertRecoveryPackedExtentWithCompanion, wantSealed: 1, wantSealingCleared: 1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dbconn, containersDir, containerID, filename, currentSize := newRecoverySealingFixture(t, test.payloadSize)
			if test.seed != nil {
				test.seed(t, dbconn, containerID, test.payloadSize)
			}
			stats := &recoveryStats{}
			if err := recoverOneSealingContainer(context.Background(), dbconn, containerID, filename, currentSize, containersDir, fsx.Default(), stats); err != nil {
				t.Fatalf("recover sealing container: %v", err)
			}
			var sealed, sealing, quarantined int
			if err := dbconn.QueryRow(`SELECT sealed, sealing, quarantine FROM container WHERE id = ?`, containerID).Scan(&sealed, &sealing, &quarantined); err != nil {
				t.Fatalf("query recovered sealing state: %v", err)
			}
			if sealed != test.wantSealed || quarantined != test.wantQuarantined || (sealing == 0) != (test.wantSealingCleared == 1) {
				t.Fatalf("state sealed=%d sealing=%d quarantine=%d, want sealed=%d sealing cleared=%d quarantine=%d",
					sealed, sealing, quarantined, test.wantSealed, test.wantSealingCleared, test.wantQuarantined)
			}
		})
	}
}

func TestRecoverSealingContainerOccupancyReadFailureDoesNotMutateState(t *testing.T) {
	dbconn, containersDir, containerID, filename, currentSize := newRecoverySealingFixture(t, 32)
	if _, err := dbconn.Exec(`DROP TABLE storage_blocks`); err != nil {
		t.Fatalf("drop occupancy table: %v", err)
	}
	stats := &recoveryStats{}
	if err := recoverOneSealingContainer(context.Background(), dbconn, containerID, filename, currentSize, containersDir, fsx.Default(), stats); err == nil {
		t.Fatal("recovery unexpectedly ignored occupancy read failure")
	}
	var sealed, sealing, quarantined int
	if err := dbconn.QueryRow(`SELECT sealed, sealing, quarantine FROM container WHERE id = ?`, containerID).Scan(&sealed, &sealing, &quarantined); err != nil {
		t.Fatalf("query state after occupancy read failure: %v", err)
	}
	if sealed != 0 || sealing != 1 || quarantined != 0 {
		t.Fatalf("occupancy read failure mutated state: sealed=%d sealing=%d quarantine=%d", sealed, sealing, quarantined)
	}
}

func newRecoverySealingFixture(t *testing.T, payloadSize int64) (*sql.DB, string, int64, string, int64) {
	t.Helper()
	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sealing recovery db: %v", err)
	}
	t.Cleanup(func() { _ = dbconn.Close() })
	dbconn.SetMaxOpenConns(1)
	dbconn.SetMaxIdleConns(1)
	if err := db.RunMigrations(dbconn); err != nil {
		t.Fatalf("run sealing recovery migrations: %v", err)
	}
	containersDir := t.TempDir()
	filename := "sealing.bin"
	currentSize := container.ContainerHdrLen + payloadSize
	if err := os.WriteFile(filepath.Join(containersDir, filename), make([]byte, currentSize), 0o600); err != nil {
		t.Fatalf("write sealing fixture: %v", err)
	}
	result, err := dbconn.Exec(`
		INSERT INTO container (filename, current_size, max_size, sealed, sealing, quarantine)
		VALUES (?, ?, ?, 0, 1, 0)`, filename, currentSize, currentSize+128)
	if err != nil {
		t.Fatalf("insert sealing container: %v", err)
	}
	containerID, err := result.LastInsertId()
	if err != nil {
		t.Fatalf("read sealing container ID: %v", err)
	}
	return dbconn, containersDir, containerID, filename, currentSize
}

func insertRecoveryLegacyExtent(t *testing.T, dbconn *sql.DB, containerID, offset, size int64, label string) {
	t.Helper()
	result, err := dbconn.Exec(`
		INSERT INTO chunk (chunk_hash, size, status, live_ref_count, chunker_version)
		VALUES (?, ?, 'COMPLETED', 1, 'v1-simple-rolling')`, label, size)
	if err != nil {
		t.Fatalf("insert recovery legacy chunk: %v", err)
	}
	chunkID, err := result.LastInsertId()
	if err != nil {
		t.Fatalf("read recovery legacy chunk ID: %v", err)
	}
	if _, err := dbconn.Exec(`
		INSERT INTO blocks (chunk_id, codec, format_version, plaintext_size, stored_size, nonce, container_id, block_offset)
		VALUES (?, 'plain', 1, ?, ?, X'', ?, ?)`, chunkID, size, size, containerID, offset); err != nil {
		t.Fatalf("insert recovery legacy extent: %v", err)
	}
}

func insertRecoveryPackedExtentWithCompanion(t *testing.T, dbconn *sql.DB, containerID, payloadSize int64) {
	t.Helper()
	result, err := dbconn.Exec(`
		INSERT INTO chunk (chunk_hash, size, status, live_ref_count, chunker_version)
		VALUES ('packed-companion', ?, 'COMPLETED', 1, 'v1-simple-rolling')`, payloadSize)
	if err != nil {
		t.Fatalf("insert recovery packed chunk: %v", err)
	}
	chunkID, err := result.LastInsertId()
	if err != nil {
		t.Fatalf("read recovery packed chunk ID: %v", err)
	}
	result, err = dbconn.Exec(`
		INSERT INTO storage_blocks
		(format_version, codec, plaintext_size, compression_codec, compressed_size,
		 stored_size, container_id, container_offset, block_hash, compression_ratio,
		 payload_hash, compressed_hash, physical_hash)
		VALUES (1, 'none', ?, 'none', ?, ?, ?, ?, X'01', 1.0, '01', X'01', X'01')`,
		payloadSize, payloadSize, payloadSize, containerID, container.ContainerHdrLen)
	if err != nil {
		t.Fatalf("insert recovery packed block: %v", err)
	}
	blockID, err := result.LastInsertId()
	if err != nil {
		t.Fatalf("read recovery packed block ID: %v", err)
	}
	if _, err := dbconn.Exec(`INSERT INTO chunk_block_refs (chunk_id, block_id, offset_in_block, size_in_block) VALUES (?, ?, 0, ?)`, chunkID, blockID, payloadSize); err != nil {
		t.Fatalf("insert recovery packed membership: %v", err)
	}
	if _, err := dbconn.Exec(`
		INSERT INTO blocks (chunk_id, codec, format_version, plaintext_size, stored_size, nonce, container_id, block_offset)
		VALUES (?, 'plain', 1, ?, ?, X'', ?, ?)`, chunkID, payloadSize, payloadSize, containerID, container.ContainerHdrLen); err != nil {
		t.Fatalf("insert recovery packed companion: %v", err)
	}
}

func TestQuarantineOrphanContainersAcceptsDirectQuarantineAfterCurrentSizeSync(t *testing.T) {
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
	if currentSize != info.Size() || maxSize != container.ContainerHdrLen+256 {
		t.Fatalf("expected current size sync to %d and preserved maximum %d, got current=%d max=%d", info.Size(), container.ContainerHdrLen+256, currentSize, maxSize)
	}

	stats := &recoveryStats{}
	if err := quarantineOrphanContainers(dbconn, containersDir, stats); err != nil {
		t.Fatalf("strict orphan recovery should accept reconciled quarantine row, got: %v", err)
	}
	if stats.quarantinedOrphan != 0 {
		t.Fatalf("expected no new orphan rows, got %d", stats.quarantinedOrphan)
	}
}

func TestQuarantineOrphanContainersResyncsCurrentSizeAndPreservesMaximum(t *testing.T) {
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
	`, filename, int64(len(content))-5, int64(len(content))+64)
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
	if currentSize != int64(len(content)) || maxSize != int64(len(content))+64 {
		t.Fatalf("expected current size=%d and preserved maximum=%d, got current=%d max=%d", len(content), len(content)+64, currentSize, maxSize)
	}
	if stats.quarantinedOrphan != 0 {
		t.Fatalf("expected no new orphan rows, got %d", stats.quarantinedOrphan)
	}
}

func TestQuarantineMissingContainersRejectsUnsafeFilename(t *testing.T) {
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
	insertStmt, err := dbconn.Prepare(`
		INSERT INTO container (filename, current_size, max_size, sealed, sealing, quarantine)
		VALUES (?, ?, ?, FALSE, FALSE, FALSE)
	`)
	if err != nil {
		t.Fatalf("prepare unsafe container insert statement: %v", err)
	}
	defer func() { _ = insertStmt.Close() }()

	if _, err := insertStmt.Exec("../escape.bin", container.ContainerHdrLen, container.ContainerHdrLen+128); err != nil {
		t.Fatalf("insert unsafe container row: %v", err)
	}

	stats := &recoveryStats{}
	err = quarantineMissingContainers(dbconn, containersDir, stats)
	if err == nil {
		t.Fatal("expected quarantineMissingContainers to reject unsafe filename")
	}
	if !strings.Contains(err.Error(), "invalid container filename") {
		t.Fatalf("expected invalid container filename error, got: %v", err)
	}
}
