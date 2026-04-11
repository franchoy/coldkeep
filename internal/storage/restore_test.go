package storage

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"os"
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

func TestBuildRestoreDescriptorFromPhysicalPathNotFound(t *testing.T) {
	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	defer func() { _ = dbconn.Close() }()
	if err := db.RunMigrations(dbconn); err != nil {
		t.Fatalf("run migrations: %v", err)
	}

	ctx, cancel := db.NewOperationContext(context.Background())
	defer cancel()

	_, err = buildRestoreDescriptorFromPhysicalPath(ctx, dbconn, "/missing/path.bin")
	if err == nil || !strings.Contains(err.Error(), "physical path \"/missing/path.bin\" not found") {
		t.Fatalf("expected physical path not found error, got: %v", err)
	}
}

func TestRestoreFileByStoredPathUsesPhysicalPathIdentity(t *testing.T) {
	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	defer func() { _ = dbconn.Close() }()
	if err := db.RunMigrations(dbconn); err != nil {
		t.Fatalf("run migrations: %v", err)
	}

	containersDir := t.TempDir()
	payload := []byte("restore-by-physical-path")
	sum := sha256.Sum256(payload)
	hash := hex.EncodeToString(sum[:])

	containerFilename := "restore-by-path.bin"
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
		hash,
		int64(len(payload)),
		filestate.ChunkCompleted,
	).Scan(&chunkID); err != nil {
		t.Fatalf("insert chunk: %v", err)
	}

	if _, err := dbconn.Exec(
		`INSERT INTO blocks (chunk_id, codec, format_version, plaintext_size, stored_size, nonce, container_id, block_offset)
		 VALUES ($1, 'plain', 1, $2, $3, $4, $5, $6)`,
		chunkID,
		int64(len(payload)),
		int64(len(payload)),
		[]byte{},
		containerID,
		int64(container.ContainerHdrLen),
	); err != nil {
		t.Fatalf("insert block: %v", err)
	}

	var fileID int64
	if err := dbconn.QueryRow(
		`INSERT INTO logical_file (original_name, total_size, file_hash, status, ref_count)
		 VALUES ($1, $2, $3, $4, $5) RETURNING id`,
		"legacy-original-name.bin",
		int64(len(payload)),
		hash,
		filestate.LogicalFileCompleted,
		1,
	).Scan(&fileID); err != nil {
		t.Fatalf("insert logical file: %v", err)
	}

	if _, err := dbconn.Exec(
		`INSERT INTO file_chunk (logical_file_id, chunk_id, chunk_order) VALUES ($1, $2, 0)`,
		fileID,
		chunkID,
	); err != nil {
		t.Fatalf("insert file_chunk: %v", err)
	}

	restoreRoot := t.TempDir()
	storedPath := filepath.Join(restoreRoot, "nested", "restored-from-physical-path.bin")
	if _, err := dbconn.Exec(
		`INSERT INTO physical_file (path, logical_file_id, mode, mtime, uid, gid, is_metadata_complete)
		 VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		storedPath,
		fileID,
		nil,
		nil,
		nil,
		nil,
		0,
	); err != nil {
		t.Fatalf("insert physical_file: %v", err)
	}

	var refCountBefore int64
	if err := dbconn.QueryRow(`SELECT ref_count FROM logical_file WHERE id = $1`, fileID).Scan(&refCountBefore); err != nil {
		t.Fatalf("read ref_count before restore: %v", err)
	}

	originalWD, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	defer func() { _ = os.Chdir(originalWD) }()
	if err := os.Chdir(restoreRoot); err != nil {
		t.Fatalf("chdir restore root: %v", err)
	}

	relativeStoredPath := filepath.Join("nested", "restored-from-physical-path.bin")
	sgctx := StorageContext{DB: dbconn, ContainerDir: containersDir}
	result, err := RestoreFileByStoredPathWithStorageContextResultOptions(sgctx, relativeStoredPath, RestoreOptions{Overwrite: true})
	if err != nil {
		t.Fatalf("restore by stored path: %v", err)
	}
	if result.OutputPath != storedPath {
		t.Fatalf("expected restore output path %q, got %q", storedPath, result.OutputPath)
	}

	restored, err := os.ReadFile(storedPath)
	if err != nil {
		t.Fatalf("read restored file: %v", err)
	}
	if string(restored) != string(payload) {
		t.Fatalf("unexpected restored payload: got %q want %q", string(restored), string(payload))
	}

	var refCountAfter int64
	if err := dbconn.QueryRow(`SELECT ref_count FROM logical_file WHERE id = $1`, fileID).Scan(&refCountAfter); err != nil {
		t.Fatalf("read ref_count after restore: %v", err)
	}
	if refCountAfter != refCountBefore {
		t.Fatalf("expected restore to keep ref_count unchanged, before=%d after=%d", refCountBefore, refCountAfter)
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
	if err == nil || !strings.Contains(err.Error(), "no restorable chunks found for file") {
		t.Fatalf("expected no-restorable-chunks error, got: %v", err)
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
		t.Fatalf("expected chunk hash mismatch error, got: %v", err)
	}
}

func TestRestoreFailsWhenNonEmptyFileHasNoChunks(t *testing.T) {
	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	defer func() { _ = dbconn.Close() }()
	if err := db.RunMigrations(dbconn); err != nil {
		t.Fatalf("run migrations: %v", err)
	}

	nonEmptyHash := strings.Repeat("a", 64)
	var fileID int64
	if err := dbconn.QueryRow(
		`INSERT INTO logical_file (original_name, total_size, file_hash, status)
		 VALUES ($1, $2, $3, $4) RETURNING id`,
		"no-chunks.bin", int64(123), nonEmptyHash, filestate.LogicalFileCompleted,
	).Scan(&fileID); err != nil {
		t.Fatalf("insert logical file: %v", err)
	}

	outPath := filepath.Join(t.TempDir(), "out.bin")
	err = RestoreFileWithDB(dbconn, fileID, outPath)
	if err == nil || !strings.Contains(err.Error(), "no restorable chunks found for file") {
		t.Fatalf("expected no-restorable-chunks error, got: %v", err)
	}
}

func TestRestoreFailsOnPlaintextSizeMismatch(t *testing.T) {
	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	defer func() { _ = dbconn.Close() }()
	if err := db.RunMigrations(dbconn); err != nil {
		t.Fatalf("run migrations: %v", err)
	}

	containersDir := t.TempDir()
	payload := []byte("plaintext-size-mismatch")
	sum := sha256.Sum256(payload)
	hash := hex.EncodeToString(sum[:])

	containerFilename := "plaintext-size-mismatch.bin"
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
		hash, int64(len(payload)), filestate.ChunkCompleted,
	).Scan(&chunkID); err != nil {
		t.Fatalf("insert chunk: %v", err)
	}

	// Persist intentionally inconsistent plaintext_size metadata.
	if _, err := dbconn.Exec(
		`INSERT INTO blocks (chunk_id, codec, format_version, plaintext_size, stored_size, nonce, container_id, block_offset)
		 VALUES ($1, 'plain', 1, $2, $3, $4, $5, $6)`,
		chunkID, int64(len(payload)+7), int64(len(payload)), []byte{}, containerID, int64(container.ContainerHdrLen),
	); err != nil {
		t.Fatalf("insert block: %v", err)
	}

	var fileID int64
	if err := dbconn.QueryRow(
		`INSERT INTO logical_file (original_name, total_size, file_hash, status)
		 VALUES ($1, $2, $3, $4) RETURNING id`,
		"plaintext-size-mismatch-test.bin", int64(len(payload)), hash, filestate.LogicalFileCompleted,
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
	if err == nil || !strings.Contains(err.Error(), "plaintext size mismatch") {
		t.Fatalf("expected plaintext size mismatch error, got: %v", err)
	}
}

func TestRestoreFailsOnAESGCMDecodeFailure(t *testing.T) {
	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	defer func() { _ = dbconn.Close() }()
	if err := db.RunMigrations(dbconn); err != nil {
		t.Fatalf("run migrations: %v", err)
	}

	// Valid AES-256 key so restore reaches decode instead of key-loading failure.
	t.Setenv("COLDKEEP_KEY", strings.Repeat("1", 64))

	containersDir := t.TempDir()
	payload := []byte("not-actually-aesgcm-ciphertext")
	sum := sha256.Sum256(payload)
	hash := hex.EncodeToString(sum[:])

	containerFilename := "aesgcm-decode-failure.bin"
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
		hash, int64(len(payload)), filestate.ChunkCompleted,
	).Scan(&chunkID); err != nil {
		t.Fatalf("insert chunk: %v", err)
	}

	if _, err := dbconn.Exec(
		`INSERT INTO blocks (chunk_id, codec, format_version, plaintext_size, stored_size, nonce, container_id, block_offset)
		 VALUES ($1, 'aes-gcm', 1, $2, $3, $4, $5, $6)`,
		chunkID,
		int64(len(payload)),
		int64(len(payload)),
		[]byte("0123456789ab"),
		containerID,
		int64(container.ContainerHdrLen),
	); err != nil {
		t.Fatalf("insert block: %v", err)
	}

	var fileID int64
	if err := dbconn.QueryRow(
		`INSERT INTO logical_file (original_name, total_size, file_hash, status)
		 VALUES ($1, $2, $3, $4) RETURNING id`,
		"aesgcm-decode-failure-test.bin", int64(len(payload)), hash, filestate.LogicalFileCompleted,
	).Scan(&fileID); err != nil {
		t.Fatalf("insert logical file: %v", err)
	}

	if _, err := dbconn.Exec(
		`INSERT INTO file_chunk (logical_file_id, chunk_id, chunk_order) VALUES ($1, $2, 0)`,
		fileID,
		chunkID,
	); err != nil {
		t.Fatalf("insert file_chunk: %v", err)
	}

	sgctx := StorageContext{DB: dbconn, ContainerDir: containersDir}
	outPath := filepath.Join(t.TempDir(), "out.bin")
	err = RestoreFileWithStorageContext(sgctx, fileID, outPath)
	if err == nil || !strings.Contains(err.Error(), "cipher: message authentication failed") {
		t.Fatalf("expected cipher: message authentication failed error, got: %v", err)
	}
}

func TestRestoreNonCompletedChunkMappingReturnsNoRestorableChunksError(t *testing.T) {
	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	defer func() { _ = dbconn.Close() }()
	if err := db.RunMigrations(dbconn); err != nil {
		t.Fatalf("run migrations: %v", err)
	}

	nonEmptyHash := strings.Repeat("c", 64)

	var fileID int64
	if err := dbconn.QueryRow(
		`INSERT INTO logical_file (original_name, total_size, file_hash, status)
		 VALUES ($1, $2, $3, $4) RETURNING id`,
		"processing-chunk-restore.bin", int64(64), nonEmptyHash, filestate.LogicalFileCompleted,
	).Scan(&fileID); err != nil {
		t.Fatalf("insert logical file: %v", err)
	}

	var chunkID int64
	if err := dbconn.QueryRow(
		`INSERT INTO chunk (chunk_hash, size, status, live_ref_count)
		 VALUES ($1, $2, $3, $4) RETURNING id`,
		nonEmptyHash, int64(64), filestate.ChunkProcessing, int64(1),
	).Scan(&chunkID); err != nil {
		t.Fatalf("insert processing chunk: %v", err)
	}

	if _, err := dbconn.Exec(
		`INSERT INTO file_chunk (logical_file_id, chunk_id, chunk_order)
		 VALUES ($1, $2, $3)`,
		fileID, chunkID, int64(0),
	); err != nil {
		t.Fatalf("insert file_chunk: %v", err)
	}

	outPath := filepath.Join(t.TempDir(), "out.bin")
	err = RestoreFileWithDB(dbconn, fileID, outPath)
	if err == nil || !strings.Contains(err.Error(), "no restorable chunks found for file") {
		t.Fatalf("expected no-restorable-chunks error for non-completed chunk mapping, got: %v", err)
	}

	var pinCount int64
	if err := dbconn.QueryRow(`SELECT pin_count FROM chunk WHERE id = $1`, chunkID).Scan(&pinCount); err != nil {
		t.Fatalf("read chunk pin_count: %v", err)
	}
	if pinCount != 0 {
		t.Fatalf("expected chunk pin_count to remain 0 when no chunk is restorable, got %d", pinCount)
	}
}

func TestRestoreFailsWhenAESGCMTransformerKeyIsMissing(t *testing.T) {
	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	defer func() { _ = dbconn.Close() }()
	if err := db.RunMigrations(dbconn); err != nil {
		t.Fatalf("run migrations: %v", err)
	}

	// Force transformer construction failure for schema-valid aes-gcm codec.
	t.Setenv("COLDKEEP_KEY", "")

	containersDir := t.TempDir()
	payload := []byte("aesgcm-missing-key-restore-payload")
	sum := sha256.Sum256(payload)
	hash := hex.EncodeToString(sum[:])

	containerFilename := "aesgcm-missing-key.bin"
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
		hash, int64(len(payload)), filestate.ChunkCompleted,
	).Scan(&chunkID); err != nil {
		t.Fatalf("insert chunk: %v", err)
	}

	if _, err := dbconn.Exec(
		`INSERT INTO blocks (chunk_id, codec, format_version, plaintext_size, stored_size, nonce, container_id, block_offset)
		 VALUES ($1, $2, 1, $3, $4, $5, $6, $7)`,
		chunkID,
		"aes-gcm",
		int64(len(payload)),
		int64(len(payload)),
		[]byte("0123456789ab"),
		containerID,
		int64(container.ContainerHdrLen),
	); err != nil {
		t.Fatalf("insert block: %v", err)
	}

	var fileID int64
	if err := dbconn.QueryRow(
		`INSERT INTO logical_file (original_name, total_size, file_hash, status)
		 VALUES ($1, $2, $3, $4) RETURNING id`,
		"aesgcm-missing-key-test.bin", int64(len(payload)), hash, filestate.LogicalFileCompleted,
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
	if err == nil || !strings.Contains(err.Error(), "aes-gcm requires COLDKEEP_KEY") {
		t.Fatalf("expected aes-gcm requires COLDKEEP_KEY error, got: %v", err)
	}
}

func TestRestoreFailsOnChunkOrderDiscontinuity(t *testing.T) {
	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	defer func() { _ = dbconn.Close() }()
	if err := db.RunMigrations(dbconn); err != nil {
		t.Fatalf("run migrations: %v", err)
	}

	containersDir := t.TempDir()
	payload0 := []byte("chunk-order-zero")
	payload2 := []byte("chunk-order-two")

	containerFile0 := "chunk-order-0.bin"
	containerPath0 := filepath.Join(containersDir, containerFile0)
	if err := writeReusableTestContainerFileWithPayload(containerPath0, payload0); err != nil {
		t.Fatalf("write first container file: %v", err)
	}

	containerFile2 := "chunk-order-2.bin"
	containerPath2 := filepath.Join(containersDir, containerFile2)
	if err := writeReusableTestContainerFileWithPayload(containerPath2, payload2); err != nil {
		t.Fatalf("write second container file: %v", err)
	}

	var containerID0 int64
	if err := dbconn.QueryRow(
		`INSERT INTO container (filename, current_size, max_size, sealed)
		 VALUES ($1, $2, $3, TRUE) RETURNING id`,
		containerFile0,
		int64(container.ContainerHdrLen+len(payload0)),
		container.GetContainerMaxSize(),
	).Scan(&containerID0); err != nil {
		t.Fatalf("insert first container: %v", err)
	}

	var containerID2 int64
	if err := dbconn.QueryRow(
		`INSERT INTO container (filename, current_size, max_size, sealed)
		 VALUES ($1, $2, $3, TRUE) RETURNING id`,
		containerFile2,
		int64(container.ContainerHdrLen+len(payload2)),
		container.GetContainerMaxSize(),
	).Scan(&containerID2); err != nil {
		t.Fatalf("insert second container: %v", err)
	}

	sum0 := sha256.Sum256(payload0)
	hash0 := hex.EncodeToString(sum0[:])
	sum2 := sha256.Sum256(payload2)
	hash2 := hex.EncodeToString(sum2[:])

	var chunkID0 int64
	if err := dbconn.QueryRow(
		`INSERT INTO chunk (chunk_hash, size, status, live_ref_count)
		 VALUES ($1, $2, $3, 1) RETURNING id`,
		hash0, int64(len(payload0)), filestate.ChunkCompleted,
	).Scan(&chunkID0); err != nil {
		t.Fatalf("insert first chunk: %v", err)
	}

	var chunkID2 int64
	if err := dbconn.QueryRow(
		`INSERT INTO chunk (chunk_hash, size, status, live_ref_count)
		 VALUES ($1, $2, $3, 1) RETURNING id`,
		hash2, int64(len(payload2)), filestate.ChunkCompleted,
	).Scan(&chunkID2); err != nil {
		t.Fatalf("insert second chunk: %v", err)
	}

	if _, err := dbconn.Exec(
		`INSERT INTO blocks (chunk_id, codec, format_version, plaintext_size, stored_size, nonce, container_id, block_offset)
		 VALUES ($1, 'plain', 1, $2, $3, $4, $5, $6)`,
		chunkID0, int64(len(payload0)), int64(len(payload0)), []byte{}, containerID0, int64(container.ContainerHdrLen),
	); err != nil {
		t.Fatalf("insert first block: %v", err)
	}

	if _, err := dbconn.Exec(
		`INSERT INTO blocks (chunk_id, codec, format_version, plaintext_size, stored_size, nonce, container_id, block_offset)
		 VALUES ($1, 'plain', 1, $2, $3, $4, $5, $6)`,
		chunkID2, int64(len(payload2)), int64(len(payload2)), []byte{}, containerID2, int64(container.ContainerHdrLen),
	); err != nil {
		t.Fatalf("insert second block: %v", err)
	}

	var fileID int64
	if err := dbconn.QueryRow(
		`INSERT INTO logical_file (original_name, total_size, file_hash, status)
		 VALUES ($1, $2, $3, $4) RETURNING id`,
		"chunk-order-discontinuity.bin", int64(len(payload0)+len(payload2)), strings.Repeat("d", 64), filestate.LogicalFileCompleted,
	).Scan(&fileID); err != nil {
		t.Fatalf("insert logical file: %v", err)
	}

	if _, err := dbconn.Exec(
		`INSERT INTO file_chunk (logical_file_id, chunk_id, chunk_order) VALUES ($1, $2, $3)`,
		fileID, chunkID0, int64(0),
	); err != nil {
		t.Fatalf("insert first file_chunk: %v", err)
	}

	if _, err := dbconn.Exec(
		`INSERT INTO file_chunk (logical_file_id, chunk_id, chunk_order) VALUES ($1, $2, $3)`,
		fileID, chunkID2, int64(2),
	); err != nil {
		t.Fatalf("insert second file_chunk: %v", err)
	}

	sgctx := StorageContext{DB: dbconn, ContainerDir: containersDir}
	outPath := filepath.Join(t.TempDir(), "out.bin")
	err = RestoreFileWithStorageContext(sgctx, fileID, outPath)
	if err == nil {
		t.Fatalf("expected restore to fail for chunk-order discontinuity")
	}
	if !strings.Contains(err.Error(), "no restorable chunks found for file") &&
		!strings.Contains(err.Error(), "restored file hash mismatch") {
		t.Fatalf("expected no-restorable-chunks or hash-mismatch error, got: %v", err)
	}
}

func TestRestoreFailsOnPayloadReadShortRead(t *testing.T) {
	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	defer func() { _ = dbconn.Close() }()
	if err := db.RunMigrations(dbconn); err != nil {
		t.Fatalf("run migrations: %v", err)
	}

	containersDir := t.TempDir()
	payload := []byte("payload-read-short")
	sum := sha256.Sum256(payload)
	hash := hex.EncodeToString(sum[:])

	containerFilename := "payload-read-short.bin"
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
		hash, int64(len(payload)), filestate.ChunkCompleted,
	).Scan(&chunkID); err != nil {
		t.Fatalf("insert chunk: %v", err)
	}

	// Intentionally over-report stored_size so restore reads past available payload bytes.
	if _, err := dbconn.Exec(
		`INSERT INTO blocks (chunk_id, codec, format_version, plaintext_size, stored_size, nonce, container_id, block_offset)
		 VALUES ($1, 'plain', 1, $2, $3, $4, $5, $6)`,
		chunkID,
		int64(len(payload)),
		int64(len(payload)+5),
		[]byte{},
		containerID,
		int64(container.ContainerHdrLen),
	); err != nil {
		t.Fatalf("insert block: %v", err)
	}

	var fileID int64
	if err := dbconn.QueryRow(
		`INSERT INTO logical_file (original_name, total_size, file_hash, status)
		 VALUES ($1, $2, $3, $4) RETURNING id`,
		"payload-read-short-test.bin", int64(len(payload)), hash, filestate.LogicalFileCompleted,
	).Scan(&fileID); err != nil {
		t.Fatalf("insert logical file: %v", err)
	}

	if _, err := dbconn.Exec(
		`INSERT INTO file_chunk (logical_file_id, chunk_id, chunk_order) VALUES ($1, $2, 0)`,
		fileID,
		chunkID,
	); err != nil {
		t.Fatalf("insert file_chunk: %v", err)
	}

	sgctx := StorageContext{DB: dbconn, ContainerDir: containersDir}
	outPath := filepath.Join(t.TempDir(), "out.bin")
	err = RestoreFileWithStorageContext(sgctx, fileID, outPath)
	if err == nil || !strings.Contains(err.Error(), "no restorable chunks found for file") {
		t.Fatalf("expected no-restorable-chunks error, got: %v", err)
	}
}

func TestRestoreFailsWhenOutputParentPathIsFile(t *testing.T) {
	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	defer func() { _ = dbconn.Close() }()
	if err := db.RunMigrations(dbconn); err != nil {
		t.Fatalf("run migrations: %v", err)
	}

	containersDir := t.TempDir()
	payload := []byte("rename-failure-restore-payload")
	sum := sha256.Sum256(payload)
	hash := hex.EncodeToString(sum[:])

	containerFilename := "rename-failure.bin"
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
		hash, int64(len(payload)), filestate.ChunkCompleted,
	).Scan(&chunkID); err != nil {
		t.Fatalf("insert chunk: %v", err)
	}

	if _, err := dbconn.Exec(
		`INSERT INTO blocks (chunk_id, codec, format_version, plaintext_size, stored_size, nonce, container_id, block_offset)
		 VALUES ($1, 'plain', 1, $2, $3, $4, $5, $6)`,
		chunkID,
		int64(len(payload)),
		int64(len(payload)),
		[]byte{},
		containerID,
		int64(container.ContainerHdrLen),
	); err != nil {
		t.Fatalf("insert block: %v", err)
	}

	var fileID int64
	if err := dbconn.QueryRow(
		`INSERT INTO logical_file (original_name, total_size, file_hash, status)
		 VALUES ($1, $2, $3, $4) RETURNING id`,
		"rename-failure-test.bin", int64(len(payload)), hash, filestate.LogicalFileCompleted,
	).Scan(&fileID); err != nil {
		t.Fatalf("insert logical file: %v", err)
	}

	if _, err := dbconn.Exec(
		`INSERT INTO file_chunk (logical_file_id, chunk_id, chunk_order) VALUES ($1, $2, 0)`,
		fileID,
		chunkID,
	); err != nil {
		t.Fatalf("insert file_chunk: %v", err)
	}

	outputBase := t.TempDir()
	blockerFile := filepath.Join(outputBase, "blocker")
	if err := os.WriteFile(blockerFile, []byte("not-a-directory"), 0o644); err != nil {
		t.Fatalf("create blocker file: %v", err)
	}
	outputTarget := filepath.Join(blockerFile, "restored.bin")

	sgctx := StorageContext{DB: dbconn, ContainerDir: containersDir}
	err = RestoreFileWithStorageContext(sgctx, fileID, outputTarget)
	if err == nil || !strings.Contains(err.Error(), "create parent directories for") {
		t.Fatalf("expected create-parent-directories error contract, got: %v", err)
	}
}

func TestRestoreFailsOnCreateTempFilePermissionDenied(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("skipping permission-denial test when running as root")
	}

	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	defer func() { _ = dbconn.Close() }()
	if err := db.RunMigrations(dbconn); err != nil {
		t.Fatalf("run migrations: %v", err)
	}

	containersDir := t.TempDir()
	payload := []byte("create-temp-permission-denied")
	sum := sha256.Sum256(payload)
	hash := hex.EncodeToString(sum[:])

	containerFilename := "create-temp-perm.bin"
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
		hash, int64(len(payload)), filestate.ChunkCompleted,
	).Scan(&chunkID); err != nil {
		t.Fatalf("insert chunk: %v", err)
	}

	if _, err := dbconn.Exec(
		`INSERT INTO blocks (chunk_id, codec, format_version, plaintext_size, stored_size, nonce, container_id, block_offset)
		 VALUES ($1, 'plain', 1, $2, $3, $4, $5, $6)`,
		chunkID,
		int64(len(payload)),
		int64(len(payload)),
		[]byte{},
		containerID,
		int64(container.ContainerHdrLen),
	); err != nil {
		t.Fatalf("insert block: %v", err)
	}

	var fileID int64
	if err := dbconn.QueryRow(
		`INSERT INTO logical_file (original_name, total_size, file_hash, status)
		 VALUES ($1, $2, $3, $4) RETURNING id`,
		"create-temp-perm-test.bin", int64(len(payload)), hash, filestate.LogicalFileCompleted,
	).Scan(&fileID); err != nil {
		t.Fatalf("insert logical file: %v", err)
	}

	if _, err := dbconn.Exec(
		`INSERT INTO file_chunk (logical_file_id, chunk_id, chunk_order) VALUES ($1, $2, 0)`,
		fileID, chunkID,
	); err != nil {
		t.Fatalf("insert file_chunk: %v", err)
	}

	// Create the output parent directory, then revoke write permission so os.CreateTemp
	// fails while os.MkdirAll (on a pre-existing dir) still succeeds.
	outputBase := t.TempDir()
	outputParentDir := filepath.Join(outputBase, "restricted")
	if err := os.MkdirAll(outputParentDir, 0o755); err != nil {
		t.Fatalf("create restricted dir: %v", err)
	}
	if err := os.Chmod(outputParentDir, 0o000); err != nil {
		t.Fatalf("chmod restricted dir: %v", err)
	}
	// Restore permissions before TempDir cleanup removes outputBase.
	t.Cleanup(func() { _ = os.Chmod(outputParentDir, 0o755) })

	outputTarget := filepath.Join(outputParentDir, "restored.bin")
	sgctx := StorageContext{DB: dbconn, ContainerDir: containersDir}
	err = RestoreFileWithStorageContext(sgctx, fileID, outputTarget)
	if err == nil || !strings.Contains(err.Error(), "create temporary output file for") {
		t.Fatalf("expected create-temp-file error contract, got: %v", err)
	}
}

// TestRestoreFailurePreservesExistingOutput verifies that if restore fails after writing the temp file but before rename,
// the original destination file is not modified. This test checks only destination file preservation, not temp file cleanup.
func TestRestoreFailurePreservesExistingOutput(t *testing.T) {
	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	defer func() { _ = dbconn.Close() }()
	if err := db.RunMigrations(dbconn); err != nil {
		t.Fatalf("run migrations: %v", err)
	}

	containersDir := t.TempDir()
	payload := []byte("RESTORED")
	sum := sha256.Sum256(payload)
	hash := hex.EncodeToString(sum[:])

	containerFilename := "atomic-restore-test.bin"
	if err := writeReusableTestContainerFileWithPayload(filepath.Join(containersDir, containerFilename), payload); err != nil {
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
		hash, int64(len(payload)), filestate.ChunkCompleted,
	).Scan(&chunkID); err != nil {
		t.Fatalf("insert chunk: %v", err)
	}

	if _, err := dbconn.Exec(
		`INSERT INTO blocks (chunk_id, codec, format_version, plaintext_size, stored_size, nonce, container_id, block_offset)
		VALUES ($1, 'plain', 1, $2, $3, $4, $5, $6)`,
		chunkID,
		int64(len(payload)),
		int64(len(payload)),
		[]byte{},
		containerID,
		int64(container.ContainerHdrLen),
	); err != nil {
		t.Fatalf("insert block: %v", err)
	}

	var fileID int64
	if err := dbconn.QueryRow(
		`INSERT INTO logical_file (original_name, total_size, file_hash, status)
		VALUES ($1, $2, $3, $4) RETURNING id`,
		"atomic-restore-test.bin", int64(len(payload)), hash, filestate.LogicalFileCompleted,
	).Scan(&fileID); err != nil {
		t.Fatalf("insert logical file: %v", err)
	}

	if _, err := dbconn.Exec(
		`INSERT INTO file_chunk (logical_file_id, chunk_id, chunk_order) VALUES ($1, $2, 0)`,
		fileID, chunkID,
	); err != nil {
		t.Fatalf("insert file_chunk: %v", err)
	}

	// Create destination file with known content
	outputDir := t.TempDir()
	destPath := filepath.Join(outputDir, "restored.bin")
	originalContent := []byte("ORIGINAL_CONTENT")
	if err := os.WriteFile(destPath, originalContent, 0644); err != nil {
		t.Fatalf("write original dest file: %v", err)
	}

	// Set test hook to simulate failure after temp file is written but before rename
	hookCalled := false
	TestRestoreFailBeforeRenameHook = func(tempOutputPath, outputPath string) error {
		hookCalled = true
		return fmt.Errorf("simulated failure before rename")
	}
	defer func() { TestRestoreFailBeforeRenameHook = nil }()

	sgctx := StorageContext{DB: dbconn, ContainerDir: containersDir}
	err = RestoreFileWithStorageContext(sgctx, fileID, destPath)
	// restore should fail
	if err == nil || !hookCalled {
		t.Fatalf("expected restore to fail via hook, got err=%v, hookCalled=%v", err, hookCalled)
	}

	// destination file must be untouched
	data, readErr := os.ReadFile(destPath)
	if readErr != nil {
		t.Fatalf("read dest file: %v", readErr)
	}
	if string(data) != string(originalContent) {
		t.Fatalf("destination file was modified: got %q, want %q", string(data), string(originalContent))
	}
	// (Deliberately do not check for temp file cleanup here)
}

// TestRestoreFailureDoesNotCorruptDestination verifies that if restore fails after writing the temp file but before rename,
// no .coldkeep-restore-* temp files remain in the output directory. This test checks only temp file cleanup, not destination file content.
func TestRestoreFailureDoesNotCorruptDestination(t *testing.T) {
	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	defer func() { _ = dbconn.Close() }()
	if err := db.RunMigrations(dbconn); err != nil {
		t.Fatalf("run migrations: %v", err)
	}

	containersDir := t.TempDir()
	payload := []byte("RESTORED")
	sum := sha256.Sum256(payload)
	hash := hex.EncodeToString(sum[:])

	containerFilename := "atomic-restore-failure-test.bin"
	if err := writeReusableTestContainerFileWithPayload(filepath.Join(containersDir, containerFilename), payload); err != nil {
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
		hash, int64(len(payload)), filestate.ChunkCompleted,
	).Scan(&chunkID); err != nil {
		t.Fatalf("insert chunk: %v", err)
	}

	if _, err := dbconn.Exec(
		`INSERT INTO blocks (chunk_id, codec, format_version, plaintext_size, stored_size, nonce, container_id, block_offset)
		VALUES ($1, 'plain', 1, $2, $3, $4, $5, $6)`,
		chunkID,
		int64(len(payload)),
		int64(len(payload)),
		[]byte{},
		containerID,
		int64(container.ContainerHdrLen),
	); err != nil {
		t.Fatalf("insert block: %v", err)
	}

	var fileID int64
	if err := dbconn.QueryRow(
		`INSERT INTO logical_file (original_name, total_size, file_hash, status)
		VALUES ($1, $2, $3, $4) RETURNING id`,
		"atomic-restore-failure-test.bin", int64(len(payload)), hash, filestate.LogicalFileCompleted,
	).Scan(&fileID); err != nil {
		t.Fatalf("insert logical file: %v", err)
	}

	if _, err := dbconn.Exec(
		`INSERT INTO file_chunk (logical_file_id, chunk_id, chunk_order) VALUES ($1, $2, 0)`,
		fileID, chunkID,
	); err != nil {
		t.Fatalf("insert file_chunk: %v", err)
	}

	// Create destination file with known content
	outputDir := t.TempDir()
	destPath := filepath.Join(outputDir, "restored.bin")
	originalContent := []byte("ORIGINAL_DEST_CONTENT")
	if err := os.WriteFile(destPath, originalContent, 0644); err != nil {
		t.Fatalf("write original dest file: %v", err)
	}

	// Set test hook to simulate failure after temp file is written but before rename
	hookCalled := false
	TestRestoreFailBeforeRenameHook = func(tempOutputPath, outputPath string) error {
		hookCalled = true
		return fmt.Errorf("simulated failure before rename")
	}
	defer func() { TestRestoreFailBeforeRenameHook = nil }()

	sgctx := StorageContext{DB: dbconn, ContainerDir: containersDir}
	err = RestoreFileWithStorageContext(sgctx, fileID, destPath)
	// restore should fail
	if err == nil || !hookCalled {
		t.Fatalf("expected restore to fail via hook, got err=%v, hookCalled=%v", err, hookCalled)
	}

	// Only check for temp file cleanup
	files, listErr := os.ReadDir(outputDir)
	if listErr != nil {
		t.Fatalf("list output dir: %v", listErr)
	}
	for _, f := range files {
		if strings.HasPrefix(f.Name(), ".coldkeep-restore-") {
			t.Fatalf("temp restore file still exists: %s", f.Name())
		}
	}
	// (Deliberately do not check destination file content here)
}

func TestRestoreOptionsOverwriteFalseRejectsExistingDestination(t *testing.T) {
	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	defer func() { _ = dbconn.Close() }()
	if err := db.RunMigrations(dbconn); err != nil {
		t.Fatalf("run migrations: %v", err)
	}

	containersDir := t.TempDir()
	payload := []byte("overwrite-false-payload")
	sum := sha256.Sum256(payload)
	hash := hex.EncodeToString(sum[:])
	originalName := "overwrite-false.bin"

	containerFilename := "overwrite-false-container.bin"
	if err := writeReusableTestContainerFileWithPayload(filepath.Join(containersDir, containerFilename), payload); err != nil {
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
		hash,
		int64(len(payload)),
		filestate.ChunkCompleted,
	).Scan(&chunkID); err != nil {
		t.Fatalf("insert chunk: %v", err)
	}

	if _, err := dbconn.Exec(
		`INSERT INTO blocks (chunk_id, codec, format_version, plaintext_size, stored_size, nonce, container_id, block_offset)
		 VALUES ($1, 'plain', 1, $2, $3, $4, $5, $6)`,
		chunkID,
		int64(len(payload)),
		int64(len(payload)),
		[]byte{},
		containerID,
		int64(container.ContainerHdrLen),
	); err != nil {
		t.Fatalf("insert block: %v", err)
	}

	var fileID int64
	if err := dbconn.QueryRow(
		`INSERT INTO logical_file (original_name, total_size, file_hash, status)
		 VALUES ($1, $2, $3, $4) RETURNING id`,
		originalName,
		int64(len(payload)),
		hash,
		filestate.LogicalFileCompleted,
	).Scan(&fileID); err != nil {
		t.Fatalf("insert logical file: %v", err)
	}

	if _, err := dbconn.Exec(
		`INSERT INTO file_chunk (logical_file_id, chunk_id, chunk_order) VALUES ($1, $2, 0)`,
		fileID,
		chunkID,
	); err != nil {
		t.Fatalf("insert file_chunk: %v", err)
	}

	outputDir := t.TempDir()
	destPath := filepath.Join(outputDir, originalName)
	originalDest := []byte("existing-file-content")
	if err := os.WriteFile(destPath, originalDest, 0o644); err != nil {
		t.Fatalf("write existing destination file: %v", err)
	}

	res, err := RestoreFileWithStorageContextResultOptions(
		StorageContext{DB: dbconn, ContainerDir: containersDir},
		fileID,
		outputDir,
		RestoreOptions{Overwrite: false},
	)
	if err == nil || !strings.Contains(err.Error(), "output file already exists") {
		t.Fatalf("expected overwrite-protection error, got result=%+v err=%v", res, err)
	}

	gotDest, readErr := os.ReadFile(destPath)
	if readErr != nil {
		t.Fatalf("read existing destination file: %v", readErr)
	}
	if string(gotDest) != string(originalDest) {
		t.Fatalf("existing destination file changed unexpectedly: got=%q want=%q", string(gotDest), string(originalDest))
	}
}

func TestRestoreOptionsOverwriteTrueReplacesExistingDestination(t *testing.T) {
	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	defer func() { _ = dbconn.Close() }()
	if err := db.RunMigrations(dbconn); err != nil {
		t.Fatalf("run migrations: %v", err)
	}

	containersDir := t.TempDir()
	payload := []byte("overwrite-true-payload")
	sum := sha256.Sum256(payload)
	hash := hex.EncodeToString(sum[:])
	originalName := "overwrite-true.bin"

	containerFilename := "overwrite-true-container.bin"
	if err := writeReusableTestContainerFileWithPayload(filepath.Join(containersDir, containerFilename), payload); err != nil {
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
		hash,
		int64(len(payload)),
		filestate.ChunkCompleted,
	).Scan(&chunkID); err != nil {
		t.Fatalf("insert chunk: %v", err)
	}

	if _, err := dbconn.Exec(
		`INSERT INTO blocks (chunk_id, codec, format_version, plaintext_size, stored_size, nonce, container_id, block_offset)
		 VALUES ($1, 'plain', 1, $2, $3, $4, $5, $6)`,
		chunkID,
		int64(len(payload)),
		int64(len(payload)),
		[]byte{},
		containerID,
		int64(container.ContainerHdrLen),
	); err != nil {
		t.Fatalf("insert block: %v", err)
	}

	var fileID int64
	if err := dbconn.QueryRow(
		`INSERT INTO logical_file (original_name, total_size, file_hash, status)
		 VALUES ($1, $2, $3, $4) RETURNING id`,
		originalName,
		int64(len(payload)),
		hash,
		filestate.LogicalFileCompleted,
	).Scan(&fileID); err != nil {
		t.Fatalf("insert logical file: %v", err)
	}

	if _, err := dbconn.Exec(
		`INSERT INTO file_chunk (logical_file_id, chunk_id, chunk_order) VALUES ($1, $2, 0)`,
		fileID,
		chunkID,
	); err != nil {
		t.Fatalf("insert file_chunk: %v", err)
	}

	outputDir := t.TempDir()
	destPath := filepath.Join(outputDir, originalName)
	if err := os.WriteFile(destPath, []byte("old-content"), 0o644); err != nil {
		t.Fatalf("write existing destination file: %v", err)
	}

	result, err := RestoreFileWithStorageContextResultOptions(
		StorageContext{DB: dbconn, ContainerDir: containersDir},
		fileID,
		outputDir,
		RestoreOptions{Overwrite: true},
	)
	if err != nil {
		t.Fatalf("restore with overwrite=true: %v", err)
	}
	if result.OutputPath != destPath {
		t.Fatalf("unexpected output path: got=%s want=%s", result.OutputPath, destPath)
	}

	gotDest, readErr := os.ReadFile(destPath)
	if readErr != nil {
		t.Fatalf("read destination file: %v", readErr)
	}
	if string(gotDest) != string(payload) {
		t.Fatalf("destination not replaced with restored payload: got=%q want=%q", string(gotDest), string(payload))
	}
}
