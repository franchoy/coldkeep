package storage

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"path/filepath"
	"strings"
	"testing"

	"github.com/franchoy/coldkeep/internal/container"
	"github.com/franchoy/coldkeep/internal/db"
	filestate "github.com/franchoy/coldkeep/internal/status"
	_ "github.com/mattn/go-sqlite3"
)

func TestRestoreChunkPinningKeepsChunkLiveDuringRemove(t *testing.T) {
	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	defer func() { _ = dbconn.Close() }()

	if err := db.RunMigrations(dbconn); err != nil {
		t.Fatalf("run migrations: %v", err)
	}

	var containerID int64
	if err := dbconn.QueryRow(
		`INSERT INTO container (filename, current_size, max_size, sealed)
		 VALUES ($1, $2, $3, TRUE)
		 RETURNING id`,
		"restore-race-test.bin",
		4096,
		1048576,
	).Scan(&containerID); err != nil {
		t.Fatalf("insert container: %v", err)
	}

	var fileID int64
	if err := dbconn.QueryRow(
		`INSERT INTO logical_file (original_name, total_size, file_hash, status)
		 VALUES ($1, $2, $3, $4)
		 RETURNING id`,
		"sample.txt",
		5,
		"file-hash-1",
		filestate.LogicalFileCompleted,
	).Scan(&fileID); err != nil {
		t.Fatalf("insert logical file: %v", err)
	}

	var chunkID int64
	if err := dbconn.QueryRow(
		`INSERT INTO chunk (chunk_hash, size, status, live_ref_count)
		 VALUES ($1, $2, $3, $4)
		 RETURNING id`,
		"chunk-hash-1",
		5,
		filestate.ChunkCompleted,
		1,
	).Scan(&chunkID); err != nil {
		t.Fatalf("insert chunk: %v", err)
	}

	if _, err := dbconn.Exec(
		`INSERT INTO blocks (chunk_id, codec, format_version, plaintext_size, stored_size, nonce, container_id, block_offset)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
		chunkID,
		"plain",
		1,
		5,
		5,
		[]byte{},
		containerID,
		0,
	); err != nil {
		t.Fatalf("insert block: %v", err)
	}

	if _, err := dbconn.Exec(
		`INSERT INTO file_chunk (logical_file_id, chunk_id, chunk_order)
		 VALUES ($1, $2, $3)`,
		fileID,
		chunkID,
		0,
	); err != nil {
		t.Fatalf("insert file_chunk: %v", err)
	}

	_, _, _, pinnedChunkIDs, err := pinLogicalFileRestoreChunks(dbconn, fileID)
	if err != nil {
		t.Fatalf("pin restore chunks: %v", err)
	}
	if len(pinnedChunkIDs) != 1 || pinnedChunkIDs[0] != chunkID {
		t.Fatalf("unexpected pinned chunk ids: %v", pinnedChunkIDs)
	}

	if _, err := RemoveFileWithDBResult(dbconn, fileID); err != nil {
		t.Fatalf("remove file while pinned: %v", err)
	}

	var refCountAfterRemove int64
	if err := dbconn.QueryRow(`SELECT pin_count FROM chunk WHERE id = $1`, chunkID).Scan(&refCountAfterRemove); err != nil {
		t.Fatalf("read pin_count after remove: %v", err)
	}
	if refCountAfterRemove != 1 {
		t.Fatalf("expected pin_count=1 after remove while pinned, got %d", refCountAfterRemove)
	}

	if err := unpinRestoreChunks(dbconn, pinnedChunkIDs); err != nil {
		t.Fatalf("unpin restore chunks: %v", err)
	}

	var refCountAfterUnpin int64
	if err := dbconn.QueryRow(`SELECT pin_count FROM chunk WHERE id = $1`, chunkID).Scan(&refCountAfterUnpin); err != nil {
		t.Fatalf("read pin_count after unpin: %v", err)
	}
	if refCountAfterUnpin != 0 {
		t.Fatalf("expected pin_count=0 after unpin, got %d", refCountAfterUnpin)
	}
}

func TestRestoreFailsWhenLogicalFileNotFound(t *testing.T) {
	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	defer func() { _ = dbconn.Close() }()
	if err := db.RunMigrations(dbconn); err != nil {
		t.Fatalf("run migrations: %v", err)
	}

	outPath := filepath.Join(t.TempDir(), "out.bin")
	err = RestoreFileWithDB(dbconn, 999, outPath)
	if err == nil || !strings.Contains(err.Error(), "logical file 999 not found") {
		t.Fatalf("expected \"logical file 999 not found\" error, got: %v", err)
	}
}

func TestRestoreFailsWhenContainerFileMissing(t *testing.T) {
	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	defer func() { _ = dbconn.Close() }()
	if err := db.RunMigrations(dbconn); err != nil {
		t.Fatalf("run migrations: %v", err)
	}

	containersDir := t.TempDir()
	payload := []byte("container-missing-payload")
	sum := sha256.Sum256(payload)
	chunkHash := hex.EncodeToString(sum[:])

	var containerID int64
	if err := dbconn.QueryRow(
		`INSERT INTO container (filename, current_size, max_size, sealed)
		 VALUES ($1, $2, $3, TRUE) RETURNING id`,
		"missing-container.bin",
		int64(container.ContainerHdrLen+len(payload)),
		container.GetContainerMaxSize(),
	).Scan(&containerID); err != nil {
		t.Fatalf("insert container: %v", err)
	}

	var chunkID int64
	if err := dbconn.QueryRow(
		`INSERT INTO chunk (chunk_hash, size, status, live_ref_count)
		 VALUES ($1, $2, $3, 1) RETURNING id`,
		chunkHash, int64(len(payload)), filestate.ChunkCompleted,
	).Scan(&chunkID); err != nil {
		t.Fatalf("insert chunk: %v", err)
	}

	if _, err := dbconn.Exec(
		`INSERT INTO blocks (chunk_id, codec, format_version, plaintext_size, stored_size, nonce, container_id, block_offset)
		 VALUES ($1, 'plain', 1, $2, $3, $4, $5, $6)`,
		chunkID, int64(len(payload)), int64(len(payload)), []byte{}, containerID, int64(container.ContainerHdrLen),
	); err != nil {
		t.Fatalf("insert block: %v", err)
	}

	var fileID int64
	if err := dbconn.QueryRow(
		`INSERT INTO logical_file (original_name, total_size, file_hash, status)
		 VALUES ($1, $2, $3, $4) RETURNING id`,
		"missing-container-test.bin", int64(len(payload)), chunkHash, filestate.LogicalFileCompleted,
	).Scan(&fileID); err != nil {
		t.Fatalf("insert logical file: %v", err)
	}

	if _, err := dbconn.Exec(
		`INSERT INTO file_chunk (logical_file_id, chunk_id, chunk_order) VALUES ($1, $2, 0)`,
		fileID, chunkID,
	); err != nil {
		t.Fatalf("insert file_chunk: %v", err)
	}

	sgctx := StorageContext{DB: dbconn, ContainerDir: containersDir}
	outPath := filepath.Join(t.TempDir(), "out.bin")
	err = RestoreFileWithStorageContext(sgctx, fileID, outPath)
	if err == nil || !strings.Contains(err.Error(), "open sealed container") {
		t.Fatalf("expected \"open sealed container\" error, got: %v", err)
	}
}

func TestRestoreFailsOnChunkHashMismatch(t *testing.T) {
	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	defer func() { _ = dbconn.Close() }()
	if err := db.RunMigrations(dbconn); err != nil {
		t.Fatalf("run migrations: %v", err)
	}

	containersDir := t.TempDir()
	payload := []byte("chunk-hash-mismatch-payload")
	wrongChunkHash := strings.Repeat("b", 64)
	sum := sha256.Sum256(payload)
	fileHash := hex.EncodeToString(sum[:])

	containerFilename := "hash-mismatch.bin"
	containerPath := filepath.Join(containersDir, containerFilename)
	if err := writeReusableTestContainerFileWithPayload(containerPath, payload); err != nil {
		t.Fatalf("write test container file: %v", err)
	}

	var containerID int64
	if err := dbconn.QueryRow(
		`INSERT INTO container (filename, current_size, max_size, sealed)
		 VALUES ($1, $2, $3, TRUE) RETURNING id`,
		containerFilename,
		int64(container.ContainerHdrLen+len(payload)),
		container.GetContainerMaxSize(),
	).Scan(&containerID); err != nil {
		t.Fatalf("insert container: %v", err)
	}

	var chunkID int64
	if err := dbconn.QueryRow(
		`INSERT INTO chunk (chunk_hash, size, status, live_ref_count)
		 VALUES ($1, $2, $3, 1) RETURNING id`,
		wrongChunkHash, int64(len(payload)), filestate.ChunkCompleted,
	).Scan(&chunkID); err != nil {
		t.Fatalf("insert chunk: %v", err)
	}

	if _, err := dbconn.Exec(
		`INSERT INTO blocks (chunk_id, codec, format_version, plaintext_size, stored_size, nonce, container_id, block_offset)
		 VALUES ($1, 'plain', 1, $2, $3, $4, $5, $6)`,
		chunkID, int64(len(payload)), int64(len(payload)), []byte{}, containerID, int64(container.ContainerHdrLen),
	); err != nil {
		t.Fatalf("insert block: %v", err)
	}

	var fileID int64
	if err := dbconn.QueryRow(
		`INSERT INTO logical_file (original_name, total_size, file_hash, status)
		 VALUES ($1, $2, $3, $4) RETURNING id`,
		"hash-mismatch-test.bin", int64(len(payload)), fileHash, filestate.LogicalFileCompleted,
	).Scan(&fileID); err != nil {
		t.Fatalf("insert logical file: %v", err)
	}

	if _, err := dbconn.Exec(
		`INSERT INTO file_chunk (logical_file_id, chunk_id, chunk_order) VALUES ($1, $2, 0)`,
		fileID, chunkID,
	); err != nil {
		t.Fatalf("insert file_chunk: %v", err)
	}

	sgctx := StorageContext{DB: dbconn, ContainerDir: containersDir}
	outPath := filepath.Join(t.TempDir(), "out.bin")
	err = RestoreFileWithStorageContext(sgctx, fileID, outPath)
	if err == nil || !strings.Contains(err.Error(), "restored chunk hash mismatch") {
		t.Fatalf("expected \"restored chunk hash mismatch\" error, got: %v", err)
	}
}
