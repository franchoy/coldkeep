package storage

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/franchoy/coldkeep/internal/chunk"
	"github.com/franchoy/coldkeep/internal/container"
	"github.com/franchoy/coldkeep/internal/db"
	filestate "github.com/franchoy/coldkeep/internal/status"
	_ "github.com/mattn/go-sqlite3"
)

type failIfInvokedChunker struct {
	called bool
}

func (c *failIfInvokedChunker) Version() chunk.Version {
	return chunk.Version("v-test-restore-must-not-call-chunker")
}

func (c *failIfInvokedChunker) ChunkFile(path string) ([]chunk.Result, error) {
	c.called = true
	return nil, fmt.Errorf("restore must not invoke chunker")
}

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
		`INSERT INTO logical_file (original_name, total_size, file_hash, status, chunker_version)
		 VALUES ($1, $2, $3, $4, 'v1-simple-rolling')
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
		`INSERT INTO chunk (chunk_hash, size, status, live_ref_count, chunker_version)
		 VALUES ($1, $2, $3, $4, 'v1-simple-rolling')
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

func TestPinLogicalFileRestoreChunksReturnsOrderedChunkRows(t *testing.T) {
	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	defer func() { _ = dbconn.Close() }()

	if err := db.RunMigrations(dbconn); err != nil {
		t.Fatalf("run migrations: %v", err)
	}

	var fileID int64
	if err := dbconn.QueryRow(
		`INSERT INTO logical_file (original_name, total_size, file_hash, status, chunker_version)
		 VALUES ($1, $2, $3, $4, 'v1-simple-rolling')
		 RETURNING id`,
		"ordered-restore.bin",
		9,
		"ordered-restore-file-hash",
		filestate.LogicalFileCompleted,
	).Scan(&fileID); err != nil {
		t.Fatalf("insert logical file: %v", err)
	}

	insertChunkWithContainer := func(name string) (int64, int64) {
		t.Helper()
		var containerID int64
		if err := dbconn.QueryRow(
			`INSERT INTO container (filename, current_size, max_size, sealed)
			 VALUES ($1, $2, $3, TRUE)
			 RETURNING id`,
			name+".bin",
			4096,
			1048576,
		).Scan(&containerID); err != nil {
			t.Fatalf("insert container %s: %v", name, err)
		}

		var chunkID int64
		if err := dbconn.QueryRow(
			`INSERT INTO chunk (chunk_hash, size, status, live_ref_count, chunker_version)
			 VALUES ($1, $2, $3, $4, 'v1-simple-rolling')
			 RETURNING id`,
			"hash-"+name,
			3,
			filestate.ChunkCompleted,
			1,
		).Scan(&chunkID); err != nil {
			t.Fatalf("insert chunk %s: %v", name, err)
		}

		if _, err := dbconn.Exec(
			`INSERT INTO blocks (chunk_id, codec, format_version, plaintext_size, stored_size, nonce, container_id, block_offset)
			 VALUES ($1, 'plain', 1, $2, $3, $4, $5, $6)`,
			chunkID,
			3,
			3,
			[]byte{},
			containerID,
			0,
		); err != nil {
			t.Fatalf("insert block %s: %v", name, err)
		}

		return chunkID, containerID
	}

	chunk0, _ := insertChunkWithContainer("c0")
	chunk1, _ := insertChunkWithContainer("c1")
	chunk2, _ := insertChunkWithContainer("c2")

	// Intentionally insert out of order; restore pinning must still return
	// chunk rows ordered by chunk_order ASC.
	for _, row := range []struct {
		chunkID    int64
		chunkOrder int
	}{
		{chunkID: chunk2, chunkOrder: 2},
		{chunkID: chunk0, chunkOrder: 0},
		{chunkID: chunk1, chunkOrder: 1},
	} {
		if _, err := dbconn.Exec(
			`INSERT INTO file_chunk (logical_file_id, chunk_id, chunk_order)
			 VALUES ($1, $2, $3)`,
			fileID,
			row.chunkID,
			row.chunkOrder,
		); err != nil {
			t.Fatalf("insert file_chunk order=%d: %v", row.chunkOrder, err)
		}
	}

	_, _, chunkRows, pinnedChunkIDs, err := pinLogicalFileRestoreChunks(dbconn, fileID)
	if err != nil {
		t.Fatalf("pin restore chunks: %v", err)
	}
	if len(chunkRows) != 3 {
		t.Fatalf("chunk row count mismatch: got %d, want 3", len(chunkRows))
	}

	wantChunkIDs := []int64{chunk0, chunk1, chunk2}
	for i := range wantChunkIDs {
		if chunkRows[i].chunkOrder != int64(i) {
			t.Fatalf("chunk row order mismatch at %d: got %d, want %d", i, chunkRows[i].chunkOrder, i)
		}
		if chunkRows[i].chunkID != wantChunkIDs[i] {
			t.Fatalf("chunk row chunk id mismatch at %d: got %d, want %d", i, chunkRows[i].chunkID, wantChunkIDs[i])
		}
		if pinnedChunkIDs[i] != wantChunkIDs[i] {
			t.Fatalf("pinned chunk id mismatch at %d: got %d, want %d", i, pinnedChunkIDs[i], wantChunkIDs[i])
		}
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
	if err == nil || !strings.Contains(err.Error(), "logical file id 999 not found") {
		t.Fatalf("expected \"logical file id 999 not found\" error, got: %v", err)
	}
}

func TestRestorePinningFailsOnEmptyLogicalFileChunkerVersion(t *testing.T) {
	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	defer func() { _ = dbconn.Close() }()

	if err := db.RunMigrations(dbconn); err != nil {
		t.Fatalf("run migrations: %v", err)
	}

	var fileID int64
	if err := dbconn.QueryRow(
		`INSERT INTO logical_file (original_name, total_size, file_hash, status, chunker_version)
		 VALUES ($1, $2, $3, $4, 'v1-simple-rolling')
		 RETURNING id`,
		"empty-logical-chunker-version.txt",
		5,
		"file-hash-empty-logical-version",
		filestate.LogicalFileCompleted,
	).Scan(&fileID); err != nil {
		t.Fatalf("insert logical file: %v", err)
	}

	if _, err := dbconn.Exec(`UPDATE logical_file SET chunker_version = '' WHERE id = $1`, fileID); err != nil {
		t.Fatalf("set empty logical_file.chunker_version: %v", err)
	}

	_, _, _, _, err = pinLogicalFileRestoreChunks(dbconn, fileID)
	if err == nil || !strings.Contains(err.Error(), "empty chunker_version") {
		t.Fatalf("expected empty chunker_version error, got: %v", err)
	}
}

func TestRestorePinningFailsOnMalformedLogicalFileChunkerVersion(t *testing.T) {
	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	defer func() { _ = dbconn.Close() }()

	if err := db.RunMigrations(dbconn); err != nil {
		t.Fatalf("run migrations: %v", err)
	}

	var fileID int64
	if err := dbconn.QueryRow(
		`INSERT INTO logical_file (original_name, total_size, file_hash, status, chunker_version)
		 VALUES ($1, $2, $3, $4, 'v1-simple-rolling')
		 RETURNING id`,
		"malformed-logical-chunker-version.txt",
		5,
		"file-hash-malformed-logical-version",
		filestate.LogicalFileCompleted,
	).Scan(&fileID); err != nil {
		t.Fatalf("insert logical file: %v", err)
	}

	if _, err := dbconn.Exec(`UPDATE logical_file SET chunker_version = $1 WHERE id = $2`, "future-v9", fileID); err != nil {
		t.Fatalf("set malformed logical_file.chunker_version: %v", err)
	}

	_, _, _, _, err = pinLogicalFileRestoreChunks(dbconn, fileID)
	if err == nil || !strings.Contains(err.Error(), "malformed chunker_version") {
		t.Fatalf("expected malformed chunker_version error, got: %v", err)
	}
}

func TestRestorePinningFailsOnEmptyChunkChunkerVersion(t *testing.T) {
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
		"restore-empty-chunk-version.bin",
		4096,
		1048576,
	).Scan(&containerID); err != nil {
		t.Fatalf("insert container: %v", err)
	}

	var fileID int64
	if err := dbconn.QueryRow(
		`INSERT INTO logical_file (original_name, total_size, file_hash, status, chunker_version)
		 VALUES ($1, $2, $3, $4, 'v1-simple-rolling')
		 RETURNING id`,
		"sample-empty-chunk-version.txt",
		5,
		"file-hash-empty-chunk-version",
		filestate.LogicalFileCompleted,
	).Scan(&fileID); err != nil {
		t.Fatalf("insert logical file: %v", err)
	}

	var chunkID int64
	if err := dbconn.QueryRow(
		`INSERT INTO chunk (chunk_hash, size, status, live_ref_count, chunker_version)
		 VALUES ($1, $2, $3, $4, 'v1-simple-rolling')
		 RETURNING id`,
		"chunk-hash-empty-version",
		5,
		filestate.ChunkCompleted,
		1,
	).Scan(&chunkID); err != nil {
		t.Fatalf("insert chunk: %v", err)
	}

	if _, err := dbconn.Exec(`UPDATE chunk SET chunker_version = '' WHERE id = $1`, chunkID); err != nil {
		t.Fatalf("set empty chunk.chunker_version: %v", err)
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

	_, _, _, _, err = pinLogicalFileRestoreChunks(dbconn, fileID)
	if err == nil || !strings.Contains(err.Error(), "empty chunker_version") {
		t.Fatalf("expected empty chunker_version error, got: %v", err)
	}
}

func TestRestorePinningFailsOnMalformedChunkChunkerVersion(t *testing.T) {
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
		"restore-malformed-chunk-version.bin",
		4096,
		1048576,
	).Scan(&containerID); err != nil {
		t.Fatalf("insert container: %v", err)
	}

	var fileID int64
	if err := dbconn.QueryRow(
		`INSERT INTO logical_file (original_name, total_size, file_hash, status, chunker_version)
		 VALUES ($1, $2, $3, $4, 'v1-simple-rolling')
		 RETURNING id`,
		"sample-malformed-chunk-version.txt",
		5,
		"file-hash-malformed-chunk-version",
		filestate.LogicalFileCompleted,
	).Scan(&fileID); err != nil {
		t.Fatalf("insert logical file: %v", err)
	}

	var chunkID int64
	if err := dbconn.QueryRow(
		`INSERT INTO chunk (chunk_hash, size, status, live_ref_count, chunker_version)
		 VALUES ($1, $2, $3, $4, 'v1-simple-rolling')
		 RETURNING id`,
		"chunk-hash-malformed-version",
		5,
		filestate.ChunkCompleted,
		1,
	).Scan(&chunkID); err != nil {
		t.Fatalf("insert chunk: %v", err)
	}

	if _, err := dbconn.Exec(`UPDATE chunk SET chunker_version = $1 WHERE id = $2`, "vx-future", chunkID); err != nil {
		t.Fatalf("set malformed chunk.chunker_version: %v", err)
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

	_, _, _, _, err = pinLogicalFileRestoreChunks(dbconn, fileID)
	if err == nil || !strings.Contains(err.Error(), "malformed chunker_version") {
		t.Fatalf("expected malformed chunker_version error, got: %v", err)
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
	if err == nil || !strings.Contains(err.Error(), "physical file path \"/missing/path.bin\" not found") {
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
		`INSERT INTO chunk (chunk_hash, size, status, live_ref_count, chunker_version)
		 VALUES ($1, $2, $3, 1, 'v1-simple-rolling') RETURNING id`,
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
		`INSERT INTO logical_file (original_name, total_size, file_hash, status, ref_count, chunker_version)
		 VALUES ($1, $2, $3, $4, $5, 'v1-simple-rolling') RETURNING id`,
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

func TestRestoreIgnoresConfiguredRuntimeChunker(t *testing.T) {
	dbconn, sgctx, storedPath, payload := setupStoredPathRestoreFixture(
		t,
		sql.NullInt64{Int64: 0o640, Valid: true},
		sql.NullTime{Time: time.Now().Add(-2 * time.Hour), Valid: true},
		sql.NullInt64{},
		sql.NullInt64{},
		true,
	)
	defer func() { _ = dbconn.Close() }()

	failingChunker := &failIfInvokedChunker{}
	sgctx.Chunker = failingChunker

	result, err := RestoreFileByStoredPathWithStorageContextResultOptions(sgctx, storedPath, RestoreOptions{Overwrite: true})
	if err != nil {
		t.Fatalf("restore with configured failing chunker: %v", err)
	}

	restored, err := os.ReadFile(result.OutputPath)
	if err != nil {
		t.Fatalf("read restored file: %v", err)
	}
	if !bytes.Equal(restored, payload) {
		t.Fatalf("restored payload mismatch: got=%q want=%q", string(restored), string(payload))
	}

	if failingChunker.called {
		t.Fatal("restore invoked configured runtime chunker; restore must be recipe-driven")
	}
}

func TestRestoreAllowsNonDefaultChunkerVersionMetadata(t *testing.T) {
	dbconn, sgctx, storedPath, payload := setupStoredPathRestoreFixture(
		t,
		sql.NullInt64{Int64: 0o644, Valid: true},
		sql.NullTime{Time: time.Now().Add(-3 * time.Hour), Valid: true},
		sql.NullInt64{},
		sql.NullInt64{},
		true,
	)
	defer func() { _ = dbconn.Close() }()

	const futureVersion = "v9-future-cdc"
	if _, err := dbconn.Exec(`UPDATE logical_file SET chunker_version = $1`, futureVersion); err != nil {
		t.Fatalf("set logical_file chunker_version: %v", err)
	}
	if _, err := dbconn.Exec(`UPDATE chunk SET chunker_version = $1`, futureVersion); err != nil {
		t.Fatalf("set chunk chunker_version: %v", err)
	}

	result, err := RestoreFileByStoredPathWithStorageContextResultOptions(sgctx, storedPath, RestoreOptions{Overwrite: true})
	if err != nil {
		t.Fatalf("restore with non-default chunker_version metadata: %v", err)
	}

	restored, err := os.ReadFile(result.OutputPath)
	if err != nil {
		t.Fatalf("read restored file: %v", err)
	}
	if !bytes.Equal(restored, payload) {
		t.Fatalf("restored payload mismatch with non-default metadata version: got=%q want=%q", string(restored), string(payload))
	}
}

func TestRestoreAllowsLogicalAndChunkVersionMismatch(t *testing.T) {
	dbconn, sgctx, storedPath, payload := setupStoredPathRestoreFixture(
		t,
		sql.NullInt64{Int64: 0o644, Valid: true},
		sql.NullTime{Time: time.Now().Add(-3 * time.Hour), Valid: true},
		sql.NullInt64{},
		sql.NullInt64{},
		true,
	)
	defer func() { _ = dbconn.Close() }()

	if _, err := dbconn.Exec(`UPDATE logical_file SET chunker_version = $1`, "v2-fastcdc"); err != nil {
		t.Fatalf("set logical_file chunker_version: %v", err)
	}
	if _, err := dbconn.Exec(`UPDATE chunk SET chunker_version = $1`, "v1-simple-rolling"); err != nil {
		t.Fatalf("set chunk chunker_version: %v", err)
	}

	result, err := RestoreFileByStoredPathWithStorageContextResultOptions(sgctx, storedPath, RestoreOptions{Overwrite: true})
	if err != nil {
		t.Fatalf("restore with logical/chunk version mismatch metadata: %v", err)
	}

	restored, err := os.ReadFile(result.OutputPath)
	if err != nil {
		t.Fatalf("read restored file: %v", err)
	}
	if !bytes.Equal(restored, payload) {
		t.Fatalf("restored payload mismatch with logical/chunk version mismatch: got=%q want=%q", string(restored), string(payload))
	}
}

func TestRestoreFileByStoredPathPrefixMode(t *testing.T) {
	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	defer func() { _ = dbconn.Close() }()
	if err := db.RunMigrations(dbconn); err != nil {
		t.Fatalf("run migrations: %v", err)
	}

	containersDir := t.TempDir()
	payload := []byte("restore-by-prefix-mode")
	sum := sha256.Sum256(payload)
	hash := hex.EncodeToString(sum[:])

	containerFilename := "restore-prefix-mode.bin"
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
		`INSERT INTO chunk (chunk_hash, size, status, live_ref_count, chunker_version)
		 VALUES ($1, $2, $3, 1, 'v1-simple-rolling') RETURNING id`,
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
		`INSERT INTO logical_file (original_name, total_size, file_hash, status, ref_count, chunker_version)
		 VALUES ($1, $2, $3, $4, $5, 'v1-simple-rolling') RETURNING id`,
		"prefix-mode-original.bin",
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

	storedPath := filepath.Join(string(os.PathSeparator), "home", "tester", "docs", "prefix-file.bin")
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

	prefixRoot := t.TempDir()
	sgctx := StorageContext{DB: dbconn, ContainerDir: containersDir}
	result, err := RestoreFileByStoredPathWithStorageContextResultOptions(sgctx, storedPath, RestoreOptions{
		Overwrite:       true,
		DestinationMode: RestoreDestinationPrefix,
		Destination:     prefixRoot,
	})
	if err != nil {
		t.Fatalf("restore by path with prefix mode: %v", err)
	}

	expectedOutputPath := filepath.Join(prefixRoot, "home", "tester", "docs", "prefix-file.bin")
	if result.OutputPath != expectedOutputPath {
		t.Fatalf("expected prefixed output path %q, got %q", expectedOutputPath, result.OutputPath)
	}

	restored, err := os.ReadFile(expectedOutputPath)
	if err != nil {
		t.Fatalf("read restored file: %v", err)
	}
	if string(restored) != string(payload) {
		t.Fatalf("unexpected restored payload: got %q want %q", string(restored), string(payload))
	}
}

func TestRestoreFileByStoredPathPrefixModeCreatesMissingParents(t *testing.T) {
	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	defer func() { _ = dbconn.Close() }()
	if err := db.RunMigrations(dbconn); err != nil {
		t.Fatalf("run migrations: %v", err)
	}

	containersDir := t.TempDir()
	payload := []byte("restore-prefix-missing-parents")
	sum := sha256.Sum256(payload)
	hash := hex.EncodeToString(sum[:])

	containerFilename := "restore-prefix-missing-parents.bin"
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
		`INSERT INTO chunk (chunk_hash, size, status, live_ref_count, chunker_version)
		 VALUES ($1, $2, $3, 1, 'v1-simple-rolling') RETURNING id`,
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
		`INSERT INTO logical_file (original_name, total_size, file_hash, status, ref_count, chunker_version)
		 VALUES ($1, $2, $3, $4, $5, 'v1-simple-rolling') RETURNING id`,
		"prefix-mode-original.bin",
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

	storedPath := filepath.Join(string(os.PathSeparator), "home", "tester", "docs", "prefix-file.bin")
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

	prefixRoot := filepath.Join(t.TempDir(), "nested", "restore-out")
	sgctx := StorageContext{DB: dbconn, ContainerDir: containersDir}
	result, err := RestoreFileByStoredPathWithStorageContextResultOptions(sgctx, storedPath, RestoreOptions{
		Overwrite:       true,
		DestinationMode: RestoreDestinationPrefix,
		Destination:     prefixRoot,
	})
	if err != nil {
		t.Fatalf("restore by path with nested prefix mode: %v", err)
	}

	expectedOutputPath := filepath.Join(prefixRoot, "home", "tester", "docs", "prefix-file.bin")
	if result.OutputPath != expectedOutputPath {
		t.Fatalf("expected prefixed output path %q, got %q", expectedOutputPath, result.OutputPath)
	}

	restored, err := os.ReadFile(expectedOutputPath)
	if err != nil {
		t.Fatalf("read restored file: %v", err)
	}
	if string(restored) != string(payload) {
		t.Fatalf("unexpected restored payload: got %q want %q", string(restored), string(payload))
	}
}

func TestRestoreFileByStoredPathRejectsSymlinkedPrefixRoot(t *testing.T) {
	dbconn, sgctx, storedPath, _ := setupStoredPathRestoreFixture(t, sql.NullInt64{}, sql.NullTime{}, sql.NullInt64{}, sql.NullInt64{}, true)
	defer func() { _ = dbconn.Close() }()

	baseDir := t.TempDir()
	realRoot := filepath.Join(baseDir, "real-root")
	if err := os.MkdirAll(realRoot, 0o755); err != nil {
		t.Fatalf("create real root: %v", err)
	}
	symlinkRoot := filepath.Join(baseDir, "prefix-root-link")
	if err := os.Symlink(realRoot, symlinkRoot); err != nil {
		t.Skipf("symlink not supported in test environment: %v", err)
	}

	_, err := RestoreFileByStoredPathWithStorageContextResultOptions(sgctx, storedPath, RestoreOptions{
		Overwrite:       true,
		DestinationMode: RestoreDestinationPrefix,
		Destination:     symlinkRoot,
	})
	if err == nil || !strings.Contains(err.Error(), "symlink") {
		t.Fatalf("expected symlinked prefix root to be rejected, got: %v", err)
	}
}

func TestRestoreFileByStoredPathRejectsSymlinkedOverridePath(t *testing.T) {
	dbconn, sgctx, storedPath, _ := setupStoredPathRestoreFixture(t, sql.NullInt64{}, sql.NullTime{}, sql.NullInt64{}, sql.NullInt64{}, true)
	defer func() { _ = dbconn.Close() }()

	baseDir := t.TempDir()
	realTarget := filepath.Join(baseDir, "override-target.bin")
	symlinkTarget := filepath.Join(baseDir, "override-link.bin")
	if err := os.Symlink(realTarget, symlinkTarget); err != nil {
		t.Skipf("symlink not supported in test environment: %v", err)
	}

	_, err := RestoreFileByStoredPathWithStorageContextResultOptions(sgctx, storedPath, RestoreOptions{
		Overwrite:       true,
		DestinationMode: RestoreDestinationOverride,
		Destination:     symlinkTarget,
	})
	if err == nil || !strings.Contains(err.Error(), "symlink") {
		t.Fatalf("expected symlinked override path to be rejected, got: %v", err)
	}
}

func TestRestoreFileByStoredPathOverrideMode(t *testing.T) {
	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	defer func() { _ = dbconn.Close() }()
	if err := db.RunMigrations(dbconn); err != nil {
		t.Fatalf("run migrations: %v", err)
	}

	containersDir := t.TempDir()
	payload := []byte("restore-by-override-mode")
	sum := sha256.Sum256(payload)
	hash := hex.EncodeToString(sum[:])

	containerFilename := "restore-override-mode.bin"
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
		`INSERT INTO chunk (chunk_hash, size, status, live_ref_count, chunker_version)
		 VALUES ($1, $2, $3, 1, 'v1-simple-rolling') RETURNING id`,
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
		`INSERT INTO logical_file (original_name, total_size, file_hash, status, ref_count, chunker_version)
		 VALUES ($1, $2, $3, $4, $5, 'v1-simple-rolling') RETURNING id`,
		"override-mode-original.bin",
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

	storedPath := filepath.Join(string(os.PathSeparator), "var", "lib", "coldkeep", "override-file.bin")
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

	overridePath := filepath.Join(t.TempDir(), "custom", "override-target.bin")
	sgctx := StorageContext{DB: dbconn, ContainerDir: containersDir}
	result, err := RestoreFileByStoredPathWithStorageContextResultOptions(sgctx, storedPath, RestoreOptions{
		Overwrite:       true,
		DestinationMode: RestoreDestinationOverride,
		Destination:     overridePath,
	})
	if err != nil {
		t.Fatalf("restore by path with override mode: %v", err)
	}

	if result.OutputPath != overridePath {
		t.Fatalf("expected override output path %q, got %q", overridePath, result.OutputPath)
	}

	restored, err := os.ReadFile(overridePath)
	if err != nil {
		t.Fatalf("read restored file: %v", err)
	}
	if string(restored) != string(payload) {
		t.Fatalf("unexpected restored payload: got %q want %q", string(restored), string(payload))
	}
}

func TestRestoreFileByStoredPathWarnsOnIncompleteMetadata(t *testing.T) {
	dbconn, sgctx, storedPath, payload := setupStoredPathRestoreFixture(t, sql.NullInt64{Int64: 0o600, Valid: true}, sql.NullTime{Time: time.Now().Add(-time.Hour), Valid: true}, sql.NullInt64{}, sql.NullInt64{}, false)
	defer func() { _ = dbconn.Close() }()

	var logBuffer bytes.Buffer
	previousWriter := log.Writer()
	previousFlags := log.Flags()
	log.SetOutput(&logBuffer)
	log.SetFlags(0)
	defer func() {
		log.SetOutput(previousWriter)
		log.SetFlags(previousFlags)
	}()

	result, err := RestoreFileByStoredPathWithStorageContextResultOptions(sgctx, storedPath, RestoreOptions{Overwrite: true})
	if err != nil {
		t.Fatalf("restore by stored path with incomplete metadata: %v", err)
	}

	restored, err := os.ReadFile(result.OutputPath)
	if err != nil {
		t.Fatalf("read restored file: %v", err)
	}
	if string(restored) != string(payload) {
		t.Fatalf("unexpected restored payload: got %q want %q", string(restored), string(payload))
	}

	if !strings.Contains(logBuffer.String(), "event=restore_metadata_warning") || !strings.Contains(logBuffer.String(), "incomplete_metadata") {
		t.Fatalf("expected metadata warning log, got: %q", logBuffer.String())
	}
}

func TestRestoreFileByStoredPathStrictFailsWhenMetadataIncomplete(t *testing.T) {
	dbconn, sgctx, storedPath, _ := setupStoredPathRestoreFixture(t, sql.NullInt64{}, sql.NullTime{}, sql.NullInt64{}, sql.NullInt64{}, false)
	defer func() { _ = dbconn.Close() }()

	_, err := RestoreFileByStoredPathWithStorageContextResultOptions(sgctx, storedPath, RestoreOptions{
		Overwrite:      true,
		StrictMetadata: true,
	})
	if err == nil || !strings.Contains(err.Error(), "metadata is incomplete") {
		t.Fatalf("expected strict metadata incomplete error, got: %v", err)
	}

	if _, statErr := os.Stat(storedPath); !os.IsNotExist(statErr) {
		t.Fatalf("expected no output file to be created on strict metadata failure, statErr=%v", statErr)
	}
}

func TestRestoreFileByStoredPathNoMetadataBypassesStrictIncompleteCheck(t *testing.T) {
	dbconn, sgctx, storedPath, payload := setupStoredPathRestoreFixture(t, sql.NullInt64{}, sql.NullTime{}, sql.NullInt64{}, sql.NullInt64{}, false)
	defer func() { _ = dbconn.Close() }()

	result, err := RestoreFileByStoredPathWithStorageContextResultOptions(sgctx, storedPath, RestoreOptions{
		Overwrite:      true,
		StrictMetadata: true,
		NoMetadata:     true,
	})
	if err != nil {
		t.Fatalf("expected no-metadata to bypass strict incomplete metadata check, got: %v", err)
	}

	restored, err := os.ReadFile(result.OutputPath)
	if err != nil {
		t.Fatalf("read restored file: %v", err)
	}
	if string(restored) != string(payload) {
		t.Fatalf("unexpected restored payload: got %q want %q", string(restored), string(payload))
	}
}

func TestRestoreFileByStoredPathRejectsInvalidDestinationMode(t *testing.T) {
	descriptor := RestoreDescriptor{Path: "/a/b/c.bin"}
	_, err := resolveRestoreOutputPath(descriptor, RestoreOptions{DestinationMode: RestoreDestinationMode("unsupported")})
	if err == nil || !strings.Contains(err.Error(), "unsupported restore destination mode") {
		t.Fatalf("expected unsupported destination mode error, got: %v", err)
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
		`INSERT INTO chunk (chunk_hash, size, status, live_ref_count, chunker_version)
		 VALUES ($1, $2, $3, 1, 'v1-simple-rolling') RETURNING id`,
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
		`INSERT INTO logical_file (original_name, total_size, file_hash, status, chunker_version)
		 VALUES ($1, $2, $3, $4, 'v1-simple-rolling') RETURNING id`,
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
		`INSERT INTO chunk (chunk_hash, size, status, live_ref_count, chunker_version)
		 VALUES ($1, $2, $3, 1, 'v1-simple-rolling') RETURNING id`,
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
		`INSERT INTO logical_file (original_name, total_size, file_hash, status, chunker_version)
		 VALUES ($1, $2, $3, $4, 'v1-simple-rolling') RETURNING id`,
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
		`INSERT INTO logical_file (original_name, total_size, file_hash, status, chunker_version)
		 VALUES ($1, $2, $3, $4, 'v1-simple-rolling') RETURNING id`,
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
		`INSERT INTO chunk (chunk_hash, size, status, live_ref_count, chunker_version)
		 VALUES ($1, $2, $3, 1, 'v1-simple-rolling') RETURNING id`,
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
		`INSERT INTO logical_file (original_name, total_size, file_hash, status, chunker_version)
		 VALUES ($1, $2, $3, $4, 'v1-simple-rolling') RETURNING id`,
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
		`INSERT INTO chunk (chunk_hash, size, status, live_ref_count, chunker_version)
		 VALUES ($1, $2, $3, 1, 'v1-simple-rolling') RETURNING id`,
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
		`INSERT INTO logical_file (original_name, total_size, file_hash, status, chunker_version)
		 VALUES ($1, $2, $3, $4, 'v1-simple-rolling') RETURNING id`,
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
		`INSERT INTO logical_file (original_name, total_size, file_hash, status, chunker_version)
		 VALUES ($1, $2, $3, $4, 'v1-simple-rolling') RETURNING id`,
		"processing-chunk-restore.bin", int64(64), nonEmptyHash, filestate.LogicalFileCompleted,
	).Scan(&fileID); err != nil {
		t.Fatalf("insert logical file: %v", err)
	}

	var chunkID int64
	if err := dbconn.QueryRow(
		`INSERT INTO chunk (chunk_hash, size, status, live_ref_count, chunker_version)
		 VALUES ($1, $2, $3, $4, 'v1-simple-rolling') RETURNING id`,
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
		`INSERT INTO chunk (chunk_hash, size, status, live_ref_count, chunker_version)
		 VALUES ($1, $2, $3, 1, 'v1-simple-rolling') RETURNING id`,
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
		`INSERT INTO logical_file (original_name, total_size, file_hash, status, chunker_version)
		 VALUES ($1, $2, $3, $4, 'v1-simple-rolling') RETURNING id`,
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
		`INSERT INTO chunk (chunk_hash, size, status, live_ref_count, chunker_version)
		 VALUES ($1, $2, $3, 1, 'v1-simple-rolling') RETURNING id`,
		hash0, int64(len(payload0)), filestate.ChunkCompleted,
	).Scan(&chunkID0); err != nil {
		t.Fatalf("insert first chunk: %v", err)
	}

	var chunkID2 int64
	if err := dbconn.QueryRow(
		`INSERT INTO chunk (chunk_hash, size, status, live_ref_count, chunker_version)
		 VALUES ($1, $2, $3, 1, 'v1-simple-rolling') RETURNING id`,
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
		`INSERT INTO logical_file (original_name, total_size, file_hash, status, chunker_version)
		 VALUES ($1, $2, $3, $4, 'v1-simple-rolling') RETURNING id`,
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
	// Defensive ordering validation now catches discontinuity early, which is better
	// than allowing it to slip through to the restore loop. Accept either:
	// 1. Ordering validation error (preferred - early detection)
	// 2. Original loop errors (hash-mismatch or no-restorable-chunks)
	if !strings.Contains(err.Error(), "invalid restore recipe ordering") &&
		!strings.Contains(err.Error(), "non-contiguous restore chunk order") &&
		!strings.Contains(err.Error(), "no restorable chunks found for file") &&
		!strings.Contains(err.Error(), "restored file hash mismatch") {
		t.Fatalf("expected ordering/hash/restorable-chunks error, got: %v", err)
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
		`INSERT INTO chunk (chunk_hash, size, status, live_ref_count, chunker_version)
		 VALUES ($1, $2, $3, 1, 'v1-simple-rolling') RETURNING id`,
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
		`INSERT INTO logical_file (original_name, total_size, file_hash, status, chunker_version)
		 VALUES ($1, $2, $3, $4, 'v1-simple-rolling') RETURNING id`,
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
		`INSERT INTO chunk (chunk_hash, size, status, live_ref_count, chunker_version)
		 VALUES ($1, $2, $3, 1, 'v1-simple-rolling') RETURNING id`,
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
		`INSERT INTO logical_file (original_name, total_size, file_hash, status, chunker_version)
		 VALUES ($1, $2, $3, $4, 'v1-simple-rolling') RETURNING id`,
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
		`INSERT INTO chunk (chunk_hash, size, status, live_ref_count, chunker_version)
		 VALUES ($1, $2, $3, 1, 'v1-simple-rolling') RETURNING id`,
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
		`INSERT INTO logical_file (original_name, total_size, file_hash, status, chunker_version)
		 VALUES ($1, $2, $3, $4, 'v1-simple-rolling') RETURNING id`,
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
		`INSERT INTO chunk (chunk_hash, size, status, live_ref_count, chunker_version)
		VALUES ($1, $2, $3, 1, 'v1-simple-rolling') RETURNING id`,
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
		`INSERT INTO logical_file (original_name, total_size, file_hash, status, chunker_version)
		VALUES ($1, $2, $3, $4, 'v1-simple-rolling') RETURNING id`,
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
		`INSERT INTO chunk (chunk_hash, size, status, live_ref_count, chunker_version)
		VALUES ($1, $2, $3, 1, 'v1-simple-rolling') RETURNING id`,
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
		`INSERT INTO logical_file (original_name, total_size, file_hash, status, chunker_version)
		VALUES ($1, $2, $3, $4, 'v1-simple-rolling') RETURNING id`,
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
		`INSERT INTO chunk (chunk_hash, size, status, live_ref_count, chunker_version)
		 VALUES ($1, $2, $3, 1, 'v1-simple-rolling') RETURNING id`,
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
		`INSERT INTO logical_file (original_name, total_size, file_hash, status, chunker_version)
		 VALUES ($1, $2, $3, $4, 'v1-simple-rolling') RETURNING id`,
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
		`INSERT INTO chunk (chunk_hash, size, status, live_ref_count, chunker_version)
		 VALUES ($1, $2, $3, 1, 'v1-simple-rolling') RETURNING id`,
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
		`INSERT INTO logical_file (original_name, total_size, file_hash, status, chunker_version)
		 VALUES ($1, $2, $3, $4, 'v1-simple-rolling') RETURNING id`,
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

func setupStoredPathRestoreFixture(
	t *testing.T,
	mode sql.NullInt64,
	mtime sql.NullTime,
	uid sql.NullInt64,
	gid sql.NullInt64,
	isMetadataComplete bool,
) (*sql.DB, StorageContext, string, []byte) {
	t.Helper()

	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	if err := db.RunMigrations(dbconn); err != nil {
		_ = dbconn.Close()
		t.Fatalf("run migrations: %v", err)
	}

	containersDir := t.TempDir()
	payload := []byte("restore-metadata-fixture")
	sum := sha256.Sum256(payload)
	hash := hex.EncodeToString(sum[:])

	containerFilename := "restore-metadata-fixture.bin"
	containerPath := filepath.Join(containersDir, containerFilename)
	if err := writeReusableTestContainerFileWithPayload(containerPath, payload); err != nil {
		_ = dbconn.Close()
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
		_ = dbconn.Close()
		t.Fatalf("insert container: %v", err)
	}

	var chunkID int64
	if err := dbconn.QueryRow(
		`INSERT INTO chunk (chunk_hash, size, status, live_ref_count, chunker_version)
		 VALUES ($1, $2, $3, 1, 'v1-simple-rolling') RETURNING id`,
		hash,
		int64(len(payload)),
		filestate.ChunkCompleted,
	).Scan(&chunkID); err != nil {
		_ = dbconn.Close()
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
		_ = dbconn.Close()
		t.Fatalf("insert block: %v", err)
	}

	var fileID int64
	if err := dbconn.QueryRow(
		`INSERT INTO logical_file (original_name, total_size, file_hash, status, ref_count, chunker_version)
		 VALUES ($1, $2, $3, $4, 1, 'v1-simple-rolling') RETURNING id`,
		"restore-metadata-fixture.bin",
		int64(len(payload)),
		hash,
		filestate.LogicalFileCompleted,
	).Scan(&fileID); err != nil {
		_ = dbconn.Close()
		t.Fatalf("insert logical file: %v", err)
	}

	if _, err := dbconn.Exec(
		`INSERT INTO file_chunk (logical_file_id, chunk_id, chunk_order) VALUES ($1, $2, 0)`,
		fileID,
		chunkID,
	); err != nil {
		_ = dbconn.Close()
		t.Fatalf("insert file_chunk: %v", err)
	}

	restoreRoot := t.TempDir()
	storedPath := filepath.Join(restoreRoot, "nested", "restore-metadata-target.bin")

	modeValue := any(nil)
	if mode.Valid {
		modeValue = mode.Int64
	}
	mtimeValue := any(nil)
	if mtime.Valid {
		mtimeValue = mtime.Time
	}
	uidValue := any(nil)
	if uid.Valid {
		uidValue = uid.Int64
	}
	gidValue := any(nil)
	if gid.Valid {
		gidValue = gid.Int64
	}

	if _, err := dbconn.Exec(
		`INSERT INTO physical_file (path, logical_file_id, mode, mtime, uid, gid, is_metadata_complete)
		 VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		storedPath,
		fileID,
		modeValue,
		mtimeValue,
		uidValue,
		gidValue,
		isMetadataComplete,
	); err != nil {
		_ = dbconn.Close()
		t.Fatalf("insert physical_file: %v", err)
	}

	return dbconn, StorageContext{DB: dbconn, ContainerDir: containersDir}, storedPath, payload
}

func setupRestorePinningFixture(t *testing.T, chunkPayloads [][]byte) (*sql.DB, StorageContext, int64, []int64, []byte) {
	t.Helper()

	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	if err := db.RunMigrations(dbconn); err != nil {
		_ = dbconn.Close()
		t.Fatalf("run migrations: %v", err)
	}

	containersDir := t.TempDir()
	restoredBytes := make([]byte, 0)
	for _, payload := range chunkPayloads {
		restoredBytes = append(restoredBytes, payload...)
	}
	fileSum := sha256.Sum256(restoredBytes)
	fileHash := hex.EncodeToString(fileSum[:])

	var fileID int64
	if err := dbconn.QueryRow(
		`INSERT INTO logical_file (original_name, total_size, file_hash, status, chunker_version)
		 VALUES ($1, $2, $3, $4, 'v1-simple-rolling') RETURNING id`,
		"restore-step10-pinning.bin",
		int64(len(restoredBytes)),
		fileHash,
		filestate.LogicalFileCompleted,
	).Scan(&fileID); err != nil {
		_ = dbconn.Close()
		t.Fatalf("insert logical file: %v", err)
	}

	chunkIDs := make([]int64, 0, len(chunkPayloads))
	for i, payload := range chunkPayloads {
		containerFilename := fmt.Sprintf("restore-step10-pinning-%d.bin", i)
		containerPath := filepath.Join(containersDir, containerFilename)
		if err := writeReusableTestContainerFileWithPayload(containerPath, payload); err != nil {
			_ = dbconn.Close()
			t.Fatalf("write test container file %d: %v", i, err)
		}

		var containerID int64
		if err := dbconn.QueryRow(
			`INSERT INTO container (filename, current_size, max_size, sealed)
			 VALUES ($1, $2, $3, TRUE) RETURNING id`,
			containerFilename,
			int64(container.ContainerHdrLen+len(payload)),
			container.GetContainerMaxSize(),
		).Scan(&containerID); err != nil {
			_ = dbconn.Close()
			t.Fatalf("insert container %d: %v", i, err)
		}

		chunkSum := sha256.Sum256(payload)
		chunkHash := hex.EncodeToString(chunkSum[:])

		var chunkID int64
		if err := dbconn.QueryRow(
			`INSERT INTO chunk (chunk_hash, size, status, live_ref_count, chunker_version)
			 VALUES ($1, $2, $3, 1, 'v1-simple-rolling') RETURNING id`,
			chunkHash,
			int64(len(payload)),
			filestate.ChunkCompleted,
		).Scan(&chunkID); err != nil {
			_ = dbconn.Close()
			t.Fatalf("insert chunk %d: %v", i, err)
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
			_ = dbconn.Close()
			t.Fatalf("insert block %d: %v", i, err)
		}

		if _, err := dbconn.Exec(
			`INSERT INTO file_chunk (logical_file_id, chunk_id, chunk_order) VALUES ($1, $2, $3)`,
			fileID,
			chunkID,
			i,
		); err != nil {
			_ = dbconn.Close()
			t.Fatalf("insert file_chunk %d: %v", i, err)
		}

		chunkIDs = append(chunkIDs, chunkID)
	}

	return dbconn, StorageContext{DB: dbconn, ContainerDir: containersDir}, fileID, chunkIDs, restoredBytes
}

func readChunkPinCountForRestoreTest(t *testing.T, dbconn *sql.DB, chunkID int64) int64 {
	t.Helper()
	var pinCount int64
	if err := dbconn.QueryRow(`SELECT pin_count FROM chunk WHERE id = $1`, chunkID).Scan(&pinCount); err != nil {
		t.Fatalf("read chunk pin_count for chunk %d: %v", chunkID, err)
	}
	return pinCount
}

func TestRestorePinsChunksBeforeRead(t *testing.T) {
	dbconn, sgctx, fileID, chunkIDs, _ := setupRestorePinningFixture(t, [][]byte{[]byte("pin-before-read")})
	defer func() { _ = dbconn.Close() }()

	hookCalled := false
	TestRestoreBeforeChunkReadHook = func(hookDB *sql.DB, chunkID int64) error {
		hookCalled = true
		var pinCount int64
		if err := hookDB.QueryRow(`SELECT pin_count FROM chunk WHERE id = $1`, chunkID).Scan(&pinCount); err != nil {
			return fmt.Errorf("query pin_count in pre-read hook: %w", err)
		}
		if pinCount != 1 {
			return fmt.Errorf("expected chunk %d pin_count=1 before read, got %d", chunkID, pinCount)
		}
		return fmt.Errorf("stop before read")
	}
	defer func() { TestRestoreBeforeChunkReadHook = nil }()

	outPath := filepath.Join(t.TempDir(), "out.bin")
	err := RestoreFileWithStorageContext(sgctx, fileID, outPath)
	if err == nil || !strings.Contains(err.Error(), "test hook before chunk read: stop before read") {
		t.Fatalf("expected pre-read hook failure, got: %v", err)
	}
	if !hookCalled {
		t.Fatalf("expected pre-read hook to be called")
	}

	if got := readChunkPinCountForRestoreTest(t, dbconn, chunkIDs[0]); got != 0 {
		t.Fatalf("expected pin_count=0 after failed restore cleanup, got %d", got)
	}
}

func TestRestoreUnpinsAfterSuccess(t *testing.T) {
	payloads := [][]byte{[]byte("success-a"), []byte("success-b")}
	dbconn, sgctx, fileID, chunkIDs, restoredBytes := setupRestorePinningFixture(t, payloads)
	defer func() { _ = dbconn.Close() }()

	outPath := filepath.Join(t.TempDir(), "out-success.bin")
	if err := RestoreFileWithStorageContext(sgctx, fileID, outPath); err != nil {
		t.Fatalf("restore success path: %v", err)
	}

	got, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatalf("read restored output: %v", err)
	}
	if !bytes.Equal(got, restoredBytes) {
		t.Fatalf("restored output mismatch: got=%q want=%q", string(got), string(restoredBytes))
	}

	for _, chunkID := range chunkIDs {
		if pinCount := readChunkPinCountForRestoreTest(t, dbconn, chunkID); pinCount != 0 {
			t.Fatalf("expected pin_count=0 after successful restore for chunk %d, got %d", chunkID, pinCount)
		}
	}
}

func TestRestoreUnpinsAfterFailure(t *testing.T) {
	dbconn, sgctx, fileID, chunkIDs, _ := setupRestorePinningFixture(t, [][]byte{[]byte("failure-path")})
	defer func() { _ = dbconn.Close() }()

	hookCalled := false
	TestRestoreFailBeforeRenameHook = func(tempOutputPath, outputPath string) error {
		hookCalled = true
		return fmt.Errorf("forced failure before rename")
	}
	defer func() { TestRestoreFailBeforeRenameHook = nil }()

	err := RestoreFileWithStorageContext(sgctx, fileID, filepath.Join(t.TempDir(), "out-failure.bin"))
	if err == nil || !strings.Contains(err.Error(), "test hook restore failure") {
		t.Fatalf("expected restore failure from rename hook, got: %v", err)
	}
	if !hookCalled {
		t.Fatalf("expected rename failure hook to be called")
	}

	for _, chunkID := range chunkIDs {
		if pinCount := readChunkPinCountForRestoreTest(t, dbconn, chunkID); pinCount != 0 {
			t.Fatalf("expected pin_count=0 after failed restore for chunk %d, got %d", chunkID, pinCount)
		}
	}
}

func TestRestoreFailureDoesNotLeaveStalePins(t *testing.T) {
	payloads := [][]byte{[]byte("stale-a"), []byte("stale-b")}
	dbconn, sgctx, fileID, chunkIDs, _ := setupRestorePinningFixture(t, payloads)
	defer func() { _ = dbconn.Close() }()

	hookCalls := 0
	TestRestoreBeforeChunkReadHook = func(hookDB *sql.DB, _ int64) error {
		hookCalls++
		for _, cid := range chunkIDs {
			var pinCount int64
			if err := hookDB.QueryRow(`SELECT pin_count FROM chunk WHERE id = $1`, cid).Scan(&pinCount); err != nil {
				return fmt.Errorf("query pin_count for chunk %d: %w", cid, err)
			}
			if pinCount != 1 {
				return fmt.Errorf("expected chunk %d to be pinned before read, got pin_count=%d", cid, pinCount)
			}
		}
		return fmt.Errorf("forced pre-read failure")
	}
	defer func() { TestRestoreBeforeChunkReadHook = nil }()

	err := RestoreFileWithStorageContext(sgctx, fileID, filepath.Join(t.TempDir(), "out-stale.bin"))
	if err == nil || !strings.Contains(err.Error(), "test hook before chunk read") {
		t.Fatalf("expected forced pre-read failure, got: %v", err)
	}
	if hookCalls == 0 {
		t.Fatalf("expected pre-read hook to run at least once")
	}

	for _, chunkID := range chunkIDs {
		if pinCount := readChunkPinCountForRestoreTest(t, dbconn, chunkID); pinCount != 0 {
			t.Fatalf("stale pin detected after failed restore for chunk %d: pin_count=%d", chunkID, pinCount)
		}
	}
}

func TestBuildRestoreRecipeCarriesLegacyNilBlockHashes(t *testing.T) {
	rows := []restoreChunkRow{{
		chunkOrder:          0,
		blockOffset:         0,
		plaintextSize:       5,
		storedSize:          5,
		expectedChunkHash:   "abc123",
		blockHash:           nil,
		compressedHash:      nil,
		physicalHash:        nil,
		chunkerVersion:      "v1-simple-rolling",
		chunkSize:           5,
		blocksCodec:         "plain",
		blocksFormatVersion: 1,
		blocksNonce:         nil,
		blocksContainerID:   0,
		filename:            "",
		chunkStatus:         filestate.ChunkCompleted,
		maxSize:             container.GetContainerMaxSize(),
		chunkID:             10,
	}}

	recipe := buildRestoreRecipe(1, "legacy.bin", "filehash", 5, rows, []int64{10})
	if len(recipe.Chunks) != 1 {
		t.Fatalf("expected 1 chunk in recipe, got %d", len(recipe.Chunks))
	}
	h := recipe.Chunks[0].BlockHashes
	if h.LogicalHash != nil || h.CompressedHash != nil || h.PhysicalHash != nil {
		t.Fatalf("expected nil block hash metadata for legacy row, got logical=%x compressed=%x physical=%x", h.LogicalHash, h.CompressedHash, h.PhysicalHash)
	}
}

func TestBuildRestoreRecipeCarriesBlockHashMetadata(t *testing.T) {
	logical := []byte{0x01, 0x02}
	compressed := []byte{0x03, 0x04}
	physical := []byte{0x05, 0x06}

	rows := []restoreChunkRow{{
		chunkOrder:          0,
		blockOffset:         0,
		plaintextSize:       7,
		storedSize:          7,
		expectedChunkHash:   "def456",
		blockHash:           logical,
		compressedHash:      compressed,
		physicalHash:        physical,
		chunkerVersion:      "v2-fastcdc",
		chunkSize:           7,
		blocksCodec:         "none",
		blocksFormatVersion: 1,
		blocksNonce:         nil,
		blocksContainerID:   42,
		filename:            "packed.bin",
		chunkStatus:         filestate.ChunkCompleted,
		maxSize:             container.GetContainerMaxSize(),
		chunkID:             11,
	}}

	recipe := buildRestoreRecipe(2, "packed.bin", "filehash2", 7, rows, []int64{11})
	if len(recipe.Chunks) != 1 {
		t.Fatalf("expected 1 chunk in recipe, got %d", len(recipe.Chunks))
	}

	h := recipe.Chunks[0].BlockHashes
	if !bytes.Equal(h.LogicalHash, logical) {
		t.Fatalf("logical hash mismatch: got %x want %x", h.LogicalHash, logical)
	}
	if !bytes.Equal(h.CompressedHash, compressed) {
		t.Fatalf("compressed hash mismatch: got %x want %x", h.CompressedHash, compressed)
	}
	if !bytes.Equal(h.PhysicalHash, physical) {
		t.Fatalf("physical hash mismatch: got %x want %x", h.PhysicalHash, physical)
	}
}

func TestBuildRestoreRecipeGraphSemanticsIgnoreTransformLayerHashesStep74(t *testing.T) {
	rows := []restoreChunkRow{
		{
			chunkOrder:          0,
			blockOffset:         0,
			plaintextSize:       16,
			storedSize:          16,
			expectedChunkHash:   "chunk-a",
			blockHash:           []byte{0x01},
			compressedHash:      []byte{0x02},
			physicalHash:        []byte{0x03},
			chunkerVersion:      "v2-fastcdc",
			chunkSize:           16,
			blocksCodec:         "none",
			blocksFormatVersion: 1,
			blocksContainerID:   7,
			filename:            "a.bin",
			chunkStatus:         filestate.ChunkCompleted,
			maxSize:             container.GetContainerMaxSize(),
			chunkID:             100,
		},
		{
			chunkOrder:          1,
			blockOffset:         16,
			plaintextSize:       8,
			storedSize:          8,
			expectedChunkHash:   "chunk-b",
			blockHash:           []byte{0x04},
			compressedHash:      []byte{0x05},
			physicalHash:        []byte{0x06},
			chunkerVersion:      "v2-fastcdc",
			chunkSize:           8,
			blocksCodec:         "none",
			blocksFormatVersion: 1,
			blocksContainerID:   7,
			filename:            "a.bin",
			chunkStatus:         filestate.ChunkCompleted,
			maxSize:             container.GetContainerMaxSize(),
			chunkID:             101,
		},
	}

	mutated := append([]restoreChunkRow(nil), rows...)
	mutated[0].compressedHash = []byte{0xAA, 0xAA}
	mutated[0].physicalHash = []byte{0xBB, 0xBB}
	mutated[1].compressedHash = []byte{0xCC, 0xCC}
	mutated[1].physicalHash = []byte{0xDD, 0xDD}

	base := buildRestoreRecipe(99, "file.bin", "file-hash", 24, rows, []int64{100, 101})
	tampered := buildRestoreRecipe(99, "file.bin", "file-hash", 24, mutated, []int64{100, 101})

	if len(base.Chunks) != len(tampered.Chunks) {
		t.Fatalf("chunk count mismatch after transform-hash mutation: base=%d tampered=%d", len(base.Chunks), len(tampered.Chunks))
	}
	for i := range base.Chunks {
		if base.Chunks[i].Index != tampered.Chunks[i].Index {
			t.Fatalf("chunk index changed at pos %d: base=%d tampered=%d", i, base.Chunks[i].Index, tampered.Chunks[i].Index)
		}
		if base.Chunks[i].ID != tampered.Chunks[i].ID {
			t.Fatalf("chunk id changed at pos %d: base=%d tampered=%d", i, base.Chunks[i].ID, tampered.Chunks[i].ID)
		}
		if base.Chunks[i].Offset != tampered.Chunks[i].Offset {
			t.Fatalf("chunk offset changed at pos %d: base=%d tampered=%d", i, base.Chunks[i].Offset, tampered.Chunks[i].Offset)
		}
		if base.Chunks[i].ContainerID != tampered.Chunks[i].ContainerID {
			t.Fatalf("container id changed at pos %d: base=%d tampered=%d", i, base.Chunks[i].ContainerID, tampered.Chunks[i].ContainerID)
		}
	}
}
