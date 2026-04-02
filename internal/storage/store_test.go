package storage

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/franchoy/coldkeep/internal/blocks"
	"github.com/franchoy/coldkeep/internal/chunk"
	"github.com/franchoy/coldkeep/internal/container"

	"github.com/franchoy/coldkeep/internal/db"
	filestate "github.com/franchoy/coldkeep/internal/status"
	_ "github.com/mattn/go-sqlite3"
)

type syncFailWriter struct {
	offset      int64
	syncErr     error
	syncCalls   int
	retireErr   error
	retireCalls int
	db          *sql.DB
}

func (w *syncFailWriter) WriteChunk(c chunk.Info) error {
	_ = c
	return nil
}

func (w *syncFailWriter) FinalizeContainer() error {
	return nil
}

func (w *syncFailWriter) ContainerCount() int {
	return 1
}

func (w *syncFailWriter) AppendPayload(_ db.DBTX, payload []byte) (container.LocalPlacement, error) {
	offset := w.offset
	w.offset += int64(len(payload))
	return container.LocalPlacement{
		ContainerID:      1,
		Filename:         "durability_test_container.bin",
		Offset:           offset,
		StoredSize:       int64(len(payload)),
		NewContainerSize: container.ContainerHdrLen + w.offset,
	}, nil
}

func (w *syncFailWriter) SyncActiveContainer() error {
	w.syncCalls++
	if w.syncErr != nil {
		return w.syncErr
	}
	return nil
}

func (w *syncFailWriter) RetireActiveContainer() error {
	w.retireCalls++
	if w.db != nil {
		if _, err := w.db.Exec(`UPDATE container SET quarantine = TRUE WHERE id = 1`); err != nil {
			return err
		}
	}
	return w.retireErr
}

type commitAckWriter struct {
	offset       int64
	ackCalls     int
	pendingClear bool
}

func (w *commitAckWriter) WriteChunk(c chunk.Info) error {
	_ = c
	return nil
}

func (w *commitAckWriter) FinalizeContainer() error {
	return nil
}

func (w *commitAckWriter) ContainerCount() int {
	return 1
}

func (w *commitAckWriter) AppendPayload(_ db.DBTX, payload []byte) (container.LocalPlacement, error) {
	offset := w.offset
	w.offset += int64(len(payload))
	w.pendingClear = false
	return container.LocalPlacement{
		ContainerID:      1,
		Filename:         "ack_test_container.bin",
		Offset:           offset,
		StoredSize:       int64(len(payload)),
		NewContainerSize: container.ContainerHdrLen + w.offset,
	}, nil
}

func (w *commitAckWriter) AcknowledgeAppendCommitted() {
	w.ackCalls++
	w.pendingClear = true
}

func TestLinkFileChunkIncrementsRefCountOnReuse(t *testing.T) {
	originalContainersDir := container.ContainersDir
	container.ContainersDir = t.TempDir()
	t.Cleanup(func() {
		container.ContainersDir = originalContainersDir
	})

	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	defer func() { _ = dbconn.Close() }()

	if err := db.RunMigrations(dbconn); err != nil {
		t.Fatalf("run migrations: %v", err)
	}

	insertLogicalFile := func(name string, hash string) int64 {
		t.Helper()
		var fileID int64
		err := dbconn.QueryRow(
			`INSERT INTO logical_file (original_name, total_size, file_hash, status)
			 VALUES ($1, $2, $3, $4)
			 RETURNING id`,
			name,
			123,
			hash,
			filestate.LogicalFileCompleted,
		).Scan(&fileID)
		if err != nil {
			t.Fatalf("insert logical_file %s: %v", name, err)
		}
		return fileID
	}

	fileA := insertLogicalFile("a.bin", "hash-a")
	fileB := insertLogicalFile("b.bin", "hash-b")

	chunkID, chunkStatus, isNew, err := claimChunk(dbconn, "shared-chunk-hash", 777)
	if err != nil {
		t.Fatalf("claim first chunk: %v", err)
	}
	if !isNew {
		t.Fatalf("expected first claim to be new")
	}
	if chunkStatus != filestate.ChunkProcessing {
		t.Fatalf("unexpected first chunk status: %s", chunkStatus)
	}

	tx1, err := dbconn.Begin()
	if err != nil {
		t.Fatalf("begin tx1: %v", err)
	}
	if err := linkFileChunk(tx1, fileA, chunkID, 0, true); err != nil {
		_ = tx1.Rollback()
		t.Fatalf("link first file/chunk: %v", err)
	}
	if _, err := tx1.Exec(`UPDATE chunk SET status = $1 WHERE id = $2`, filestate.ChunkCompleted, chunkID); err != nil {
		_ = tx1.Rollback()
		t.Fatalf("mark chunk completed: %v", err)
	}
	var containerID int64
	if err := tx1.QueryRow(
		`INSERT INTO container (filename, sealed, quarantine, current_size, max_size)
		 VALUES ($1, TRUE, FALSE, $2, $3)
		 RETURNING id`,
		"test-reuse-container.bin",
		841,
		container.GetContainerMaxSize(),
	).Scan(&containerID); err != nil {
		_ = tx1.Rollback()
		t.Fatalf("insert container for reusable chunk: %v", err)
	}
	if _, err := tx1.Exec(
		`INSERT INTO blocks (chunk_id, codec, format_version, plaintext_size, stored_size, container_id, block_offset)
		 VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		chunkID,
		"plain",
		1,
		777,
		777,
		containerID,
		64,
	); err != nil {
		_ = tx1.Rollback()
		t.Fatalf("insert block metadata for reusable chunk: %v", err)
	}
	if err := tx1.Commit(); err != nil {
		t.Fatalf("commit tx1: %v", err)
	}

	if err := os.WriteFile(filepath.Join(container.ContainersDir, "test-reuse-container.bin"), make([]byte, 841), 0o600); err != nil {
		t.Fatalf("create reusable container file: %v", err)
	}

	chunkID2, chunkStatus2, isNew2, err := claimChunk(dbconn, "shared-chunk-hash", 777)
	if err != nil {
		t.Fatalf("claim reused chunk: %v", err)
	}
	if chunkID2 != chunkID {
		t.Fatalf("expected same chunk id, got %d vs %d", chunkID2, chunkID)
	}
	if isNew2 {
		t.Fatalf("expected reused claim to be non-new")
	}
	if chunkStatus2 != filestate.ChunkCompleted {
		t.Fatalf("unexpected reused chunk status: %s", chunkStatus2)
	}

	tx2, err := dbconn.Begin()
	if err != nil {
		t.Fatalf("begin tx2: %v", err)
	}
	if err := linkFileChunk(tx2, fileB, chunkID, 0, !isNew2); err != nil {
		_ = tx2.Rollback()
		t.Fatalf("link second file/chunk: %v", err)
	}
	// Idempotency check: same mapping conflict should not increment live_ref_count again.
	if err := linkFileChunk(tx2, fileB, chunkID, 0, !isNew2); err != nil {
		_ = tx2.Rollback()
		t.Fatalf("re-link second file/chunk: %v", err)
	}
	if err := tx2.Commit(); err != nil {
		t.Fatalf("commit tx2: %v", err)
	}

	var refCount int64
	if err := dbconn.QueryRow(`SELECT live_ref_count FROM chunk WHERE id = $1`, chunkID).Scan(&refCount); err != nil {
		t.Fatalf("read live_ref_count: %v", err)
	}
	if refCount != 2 {
		t.Fatalf("expected live_ref_count=2 after two links, got %d", refCount)
	}

	var mappingCount int64
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM file_chunk WHERE chunk_id = $1`, chunkID).Scan(&mappingCount); err != nil {
		t.Fatalf("count mappings: %v", err)
	}
	if mappingCount != 2 {
		t.Fatalf("expected 2 file_chunk mappings, got %d", mappingCount)
	}
}

func TestClaimChunkDoesNotReuseCompletedChunkWithoutValidLocation(t *testing.T) {
	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	defer func() { _ = dbconn.Close() }()

	if err := db.RunMigrations(dbconn); err != nil {
		t.Fatalf("run migrations: %v", err)
	}

	var chunkID int64
	if err := dbconn.QueryRow(
		`INSERT INTO chunk (chunk_hash, size, status, live_ref_count)
		 VALUES ($1, $2, $3, $4)
		 RETURNING id`,
		"orphan-completed-chunk",
		123,
		filestate.ChunkCompleted,
		0,
	).Scan(&chunkID); err != nil {
		t.Fatalf("insert completed chunk: %v", err)
	}

	claimedID, claimedStatus, isNew, err := claimChunk(dbconn, "orphan-completed-chunk", 123)
	if err != nil {
		t.Fatalf("claim malformed completed chunk: %v", err)
	}
	if claimedID != chunkID {
		t.Fatalf("expected same chunk id, got %d vs %d", claimedID, chunkID)
	}
	if isNew {
		t.Fatalf("expected existing chunk to be reclaimed, not inserted as new")
	}
	if claimedStatus != filestate.ChunkProcessing {
		t.Fatalf("expected malformed completed chunk to be reclaimed as PROCESSING, got %s", claimedStatus)
	}

	var latestStatus string
	if err := dbconn.QueryRow(`SELECT status FROM chunk WHERE id = $1`, chunkID).Scan(&latestStatus); err != nil {
		t.Fatalf("read chunk status after claim: %v", err)
	}
	if latestStatus != filestate.ChunkProcessing {
		t.Fatalf("expected chunk row status PROCESSING after reclaim, got %s", latestStatus)
	}
}

func TestClaimChunkDoesNotReuseCompletedChunkInQuarantinedContainer(t *testing.T) {
	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	defer func() { _ = dbconn.Close() }()

	if err := db.RunMigrations(dbconn); err != nil {
		t.Fatalf("run migrations: %v", err)
	}

	var chunkID int64
	if err := dbconn.QueryRow(
		`INSERT INTO chunk (chunk_hash, size, status, live_ref_count)
		 VALUES ($1, $2, $3, $4)
		 RETURNING id`,
		"quarantined-completed-chunk",
		321,
		filestate.ChunkCompleted,
		1,
	).Scan(&chunkID); err != nil {
		t.Fatalf("insert completed chunk: %v", err)
	}

	containerID := insertReusableTestContainer(t, dbconn, "quarantined-reuse.bin", true)
	insertReusableTestBlock(t, dbconn, chunkID, containerID, 64)

	claimedID, claimedStatus, isNew, err := claimChunk(dbconn, "quarantined-completed-chunk", 321)
	if err != nil {
		t.Fatalf("claim quarantined completed chunk: %v", err)
	}
	if claimedID != chunkID {
		t.Fatalf("expected same chunk id, got %d vs %d", claimedID, chunkID)
	}
	if isNew {
		t.Fatalf("expected existing chunk to be reclaimed, not inserted as new")
	}
	if claimedStatus != filestate.ChunkProcessing {
		t.Fatalf("expected quarantined completed chunk to be reclaimed as PROCESSING, got %s", claimedStatus)
	}
}

func TestStoreFileDoesNotUseRedundantPreCommitSync(t *testing.T) {
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
		 VALUES ($1, $2, $3, FALSE)
		 RETURNING id`,
		"durability_test_container.bin",
		container.ContainerHdrLen,
		container.GetContainerMaxSize(),
	).Scan(&containerID); err != nil {
		t.Fatalf("insert container row: %v", err)
	}
	if containerID != 1 {
		t.Fatalf("expected container id 1 for test writer, got %d", containerID)
	}

	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "payload.txt")
	if err := os.WriteFile(path, []byte("durability-gap-regression-test"), 0644); err != nil {
		t.Fatalf("write temp file: %v", err)
	}

	writer := &syncFailWriter{syncErr: errors.New("unused pre-commit sync hook"), db: dbconn}
	sgctx := StorageContext{
		DB:           dbconn,
		Writer:       writer,
		ContainerDir: tmpDir,
	}

	codec, err := blocks.ParseCodec("plain")
	if err != nil {
		t.Fatalf("parse plain codec: %v", err)
	}

	_, err = StoreFileWithStorageContextAndCodecResult(sgctx, path, codec)
	if err != nil {
		t.Fatalf("store with pre-commit sync hook present should succeed: %v", err)
	}
	if writer.syncCalls != 0 {
		t.Fatalf("expected no pre-commit sync calls, got %d", writer.syncCalls)
	}
	if writer.retireCalls != 0 {
		t.Fatalf("expected no container retirement on successful store, got %d", writer.retireCalls)
	}

	var abortedCount int
	if err := dbconn.QueryRow(
		`SELECT COUNT(*) FROM logical_file WHERE status = $1`,
		filestate.LogicalFileAborted,
	).Scan(&abortedCount); err != nil {
		t.Fatalf("count aborted logical files: %v", err)
	}
	if abortedCount != 0 {
		t.Fatalf("expected 0 aborted logical files after successful store, got %d", abortedCount)
	}

	var completedChunks int
	if err := dbconn.QueryRow(
		`SELECT COUNT(*) FROM chunk WHERE status = $1`,
		filestate.ChunkCompleted,
	).Scan(&completedChunks); err != nil {
		t.Fatalf("count completed chunks: %v", err)
	}
	if completedChunks == 0 {
		t.Fatalf("expected completed chunks after successful store")
	}

	var blockCount int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM blocks`).Scan(&blockCount); err != nil {
		t.Fatalf("count blocks: %v", err)
	}
	if blockCount == 0 {
		t.Fatalf("expected persisted block metadata after successful store")
	}

	var quarantined bool
	if err := dbconn.QueryRow(`SELECT quarantine FROM container WHERE id = 1`).Scan(&quarantined); err != nil {
		t.Fatalf("query container quarantine: %v", err)
	}
	if quarantined {
		t.Fatalf("expected container to remain healthy after successful store")
	}

}

func TestStoreFileSuccessfulCommitAcknowledgesWriterAppendState(t *testing.T) {
	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	defer func() { _ = dbconn.Close() }()

	if err := db.RunMigrations(dbconn); err != nil {
		t.Fatalf("run migrations: %v", err)
	}

	if _, err := dbconn.Exec(
		`INSERT INTO container (id, filename, current_size, max_size, sealed)
		 VALUES (1, $1, $2, $3, FALSE)`,
		"ack_test_container.bin",
		container.ContainerHdrLen,
		container.GetContainerMaxSize(),
	); err != nil {
		t.Fatalf("insert container row: %v", err)
	}

	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "payload.txt")
	if err := os.WriteFile(path, []byte("acknowledge-append-after-commit"), 0644); err != nil {
		t.Fatalf("write temp file: %v", err)
	}

	writer := &commitAckWriter{}
	sgctx := StorageContext{
		DB:           dbconn,
		Writer:       writer,
		ContainerDir: tmpDir,
	}

	codec, err := blocks.ParseCodec("plain")
	if err != nil {
		t.Fatalf("parse plain codec: %v", err)
	}

	_, err = StoreFileWithStorageContextAndCodecResult(sgctx, path, codec)
	if err != nil {
		t.Fatalf("store should succeed: %v", err)
	}
	if writer.ackCalls == 0 {
		t.Fatalf("expected AcknowledgeAppendCommitted to be called at least once")
	}
	if !writer.pendingClear {
		t.Fatalf("expected writer pending rollback state to be cleared after commit acknowledgment")
	}
}

func TestClaimChunkDoesNotReuseCompletedChunkWithMissingContainerFile(t *testing.T) {
	originalContainersDir := container.ContainersDir
	container.ContainersDir = t.TempDir()
	t.Cleanup(func() {
		container.ContainersDir = originalContainersDir
	})

	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	defer func() { _ = dbconn.Close() }()

	if err := db.RunMigrations(dbconn); err != nil {
		t.Fatalf("run migrations: %v", err)
	}

	var chunkID int64
	if err := dbconn.QueryRow(
		`INSERT INTO chunk (chunk_hash, size, status, live_ref_count)
		 VALUES ($1, $2, $3, $4)
		 RETURNING id`,
		"missing-file-completed-chunk",
		456,
		filestate.ChunkCompleted,
		1,
	).Scan(&chunkID); err != nil {
		t.Fatalf("insert completed chunk: %v", err)
	}

	containerID := insertReusableTestContainer(t, dbconn, "missing-file-reuse.bin", false)
	insertReusableTestBlock(t, dbconn, chunkID, containerID, 64)

	claimedID, claimedStatus, isNew, err := claimChunk(dbconn, "missing-file-completed-chunk", 456)
	if err != nil {
		t.Fatalf("claim completed chunk with missing file: %v", err)
	}
	if claimedID != chunkID {
		t.Fatalf("expected same chunk id, got %d vs %d", claimedID, chunkID)
	}
	if isNew {
		t.Fatalf("expected existing chunk to be reclaimed, not inserted as new")
	}
	if claimedStatus != filestate.ChunkProcessing {
		t.Fatalf("expected missing-file completed chunk to be reclaimed as PROCESSING, got %s", claimedStatus)
	}

	var latestStatus string
	if err := dbconn.QueryRow(`SELECT status FROM chunk WHERE id = $1`, chunkID).Scan(&latestStatus); err != nil {
		t.Fatalf("read chunk status after claim: %v", err)
	}
	if latestStatus != filestate.ChunkProcessing {
		t.Fatalf("expected chunk row status PROCESSING after reclaim, got %s", latestStatus)
	}
}

func TestValidateReusableLogicalFileGraphRejectsCorruptCompletedGraphs(t *testing.T) {
	testCases := []struct {
		name    string
		setup   func(t *testing.T, dbconn *sql.DB, containersDir string, fileID int64)
		wantErr string
	}{
		{
			name: "missing file chunks",
			setup: func(t *testing.T, dbconn *sql.DB, containersDir string, fileID int64) {
				t.Helper()
				_ = dbconn
				_ = containersDir
				_ = fileID
			},
			wantErr: "has no file_chunk rows",
		},
		{
			name: "broken chunk ordering",
			setup: func(t *testing.T, dbconn *sql.DB, containersDir string, fileID int64) {
				t.Helper()
				containerID := insertReusableTestContainer(t, dbconn, "broken-order.bin", false)
				writeReusableTestContainerFile(t, containersDir, "broken-order.bin")
				chunkA := insertReusableTestChunk(t, dbconn, "broken-order-a", filestate.ChunkCompleted)
				chunkB := insertReusableTestChunk(t, dbconn, "broken-order-b", filestate.ChunkCompleted)
				insertReusableTestBlock(t, dbconn, chunkA, containerID, 64)
				insertReusableTestBlock(t, dbconn, chunkB, containerID, 128)
				insertReusableTestFileChunk(t, dbconn, fileID, chunkA, 0)
				insertReusableTestFileChunk(t, dbconn, fileID, chunkB, 2)
			},
			wantErr: "non-contiguous chunk ordering",
		},
		{
			name: "missing block metadata",
			setup: func(t *testing.T, dbconn *sql.DB, containersDir string, fileID int64) {
				t.Helper()
				_ = containersDir
				chunkID := insertReusableTestChunk(t, dbconn, "missing-block", filestate.ChunkCompleted)
				insertReusableTestFileChunk(t, dbconn, fileID, chunkID, 0)
			},
			wantErr: "without block metadata",
		},
		{
			name: "quarantined container",
			setup: func(t *testing.T, dbconn *sql.DB, containersDir string, fileID int64) {
				t.Helper()
				containerID := insertReusableTestContainer(t, dbconn, "quarantined.bin", true)
				writeReusableTestContainerFile(t, containersDir, "quarantined.bin")
				chunkID := insertReusableTestChunk(t, dbconn, "quarantined-chunk", filestate.ChunkCompleted)
				insertReusableTestBlock(t, dbconn, chunkID, containerID, 64)
				insertReusableTestFileChunk(t, dbconn, fileID, chunkID, 0)
			},
			wantErr: "missing or quarantined containers",
		},
		{
			name: "missing container file on disk",
			setup: func(t *testing.T, dbconn *sql.DB, containersDir string, fileID int64) {
				t.Helper()
				_ = containersDir
				containerID := insertReusableTestContainer(t, dbconn, "missing-on-disk.bin", false)
				chunkID := insertReusableTestChunk(t, dbconn, "missing-file-chunk", filestate.ChunkCompleted)
				insertReusableTestBlock(t, dbconn, chunkID, containerID, 64)
				insertReusableTestFileChunk(t, dbconn, fileID, chunkID, 0)
			},
			wantErr: "references missing container file",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			dbconn, err := sql.Open("sqlite3", ":memory:")
			if err != nil {
				t.Fatalf("open sqlite db: %v", err)
			}
			dbconn.SetMaxOpenConns(1)
			dbconn.SetMaxIdleConns(1)
			defer func() { _ = dbconn.Close() }()

			if err := db.RunMigrations(dbconn); err != nil {
				t.Fatalf("run migrations: %v", err)
			}

			containersDir := t.TempDir()
			fileID := insertReusableTestLogicalFile(t, dbconn, 128)
			tc.setup(t, dbconn, containersDir, fileID)

			ctx, cancel := db.NewOperationContext(context.Background())
			defer cancel()

			err = validateReusableLogicalFileGraphWithContext(ctx, dbconn, fileID, containersDir)
			if err == nil {
				t.Fatalf("expected validation error")
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("expected error containing %q, got %v", tc.wantErr, err)
			}
		})
	}
}

func TestLoadReuseSemanticValidationModeFromEnv(t *testing.T) {
	t.Setenv("COLDKEEP_REUSE_SEMANTIC_VALIDATION", "")
	if got := loadReuseSemanticValidationModeFromEnv(); got != reuseSemanticValidationSuspicious {
		t.Fatalf("expected default mode %q, got %q", reuseSemanticValidationSuspicious, got)
	}

	t.Setenv("COLDKEEP_REUSE_SEMANTIC_VALIDATION", "off")
	if got := loadReuseSemanticValidationModeFromEnv(); got != reuseSemanticValidationOff {
		t.Fatalf("expected mode %q, got %q", reuseSemanticValidationOff, got)
	}

	t.Setenv("COLDKEEP_REUSE_SEMANTIC_VALIDATION", "always")
	if got := loadReuseSemanticValidationModeFromEnv(); got != reuseSemanticValidationAlways {
		t.Fatalf("expected mode %q, got %q", reuseSemanticValidationAlways, got)
	}

	t.Setenv("COLDKEEP_REUSE_SEMANTIC_VALIDATION", "invalid-value")
	if got := loadReuseSemanticValidationModeFromEnv(); got != reuseSemanticValidationSuspicious {
		t.Fatalf("expected invalid mode fallback %q, got %q", reuseSemanticValidationSuspicious, got)
	}
}

func TestValidateReusableLogicalFileForStoreRunsSemanticValidation(t *testing.T) {
	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	dbconn.SetMaxOpenConns(1)
	dbconn.SetMaxIdleConns(1)
	defer func() { _ = dbconn.Close() }()

	if err := db.RunMigrations(dbconn); err != nil {
		t.Fatalf("run migrations: %v", err)
	}

	containersDir := t.TempDir()
	content := []byte("semantic-reuse-validation-regression-payload")
	sum := sha256.Sum256(content)
	hash := hex.EncodeToString(sum[:])

	containerFilename := "semantic-reuse.bin"
	containerPath := filepath.Join(containersDir, containerFilename)
	if err := os.WriteFile(containerPath, content, 0644); err != nil {
		t.Fatalf("write container file: %v", err)
	}

	fileID := insertReusableTestLogicalFile(t, dbconn, int64(len(content)))
	if _, err := dbconn.Exec(`UPDATE logical_file SET file_hash = $1 WHERE id = $2`, hash, fileID); err != nil {
		t.Fatalf("update logical file hash: %v", err)
	}

	chunkID := insertReusableTestChunk(t, dbconn, hash, filestate.ChunkCompleted)
	if _, err := dbconn.Exec(`UPDATE chunk SET size = $1 WHERE id = $2`, int64(len(content)), chunkID); err != nil {
		t.Fatalf("update chunk size: %v", err)
	}

	containerID := insertReusableTestContainer(t, dbconn, containerFilename, false)
	if _, err := dbconn.Exec(`UPDATE container SET current_size = $1 WHERE id = $2`, int64(len(content)), containerID); err != nil {
		t.Fatalf("update container size: %v", err)
	}

	if _, err := dbconn.Exec(
		`INSERT INTO blocks (chunk_id, codec, format_version, plaintext_size, stored_size, container_id, block_offset)
		 VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		chunkID,
		"plain",
		1,
		int64(len(content)),
		int64(len(content)),
		containerID,
		int64(0),
	); err != nil {
		t.Fatalf("insert block row: %v", err)
	}
	insertReusableTestFileChunk(t, dbconn, fileID, chunkID, 0)

	ctx, cancel := db.NewOperationContext(context.Background())
	defer cancel()

	t.Setenv("COLDKEEP_REUSE_SEMANTIC_VALIDATION", "always")
	if err := validateReusableLogicalFileForStoreWithContext(ctx, dbconn, fileID, containersDir); err != nil {
		t.Fatalf("semantic validation should pass for intact reusable file: %v", err)
	}

	if _, err := dbconn.Exec(`UPDATE chunk SET chunk_hash = $1 WHERE id = $2`, strings.Repeat("f", 64), chunkID); err != nil {
		t.Fatalf("tamper chunk hash: %v", err)
	}

	err = validateReusableLogicalFileForStoreWithContext(ctx, dbconn, fileID, containersDir)
	if err == nil {
		t.Fatalf("expected semantic validation failure after chunk hash tamper")
	}
	if !strings.Contains(err.Error(), "semantic reuse validation failed") {
		t.Fatalf("expected semantic validation failure message, got: %v", err)
	}
}

func insertReusableTestLogicalFile(t *testing.T, dbconn *sql.DB, totalSize int64) int64 {
	t.Helper()

	var fileID int64
	err := dbconn.QueryRow(
		`INSERT INTO logical_file (original_name, total_size, file_hash, status)
		 VALUES ($1, $2, $3, $4)
		 RETURNING id`,
		"reusable.bin",
		totalSize,
		fmt.Sprintf("file-hash-%d", totalSize),
		filestate.LogicalFileCompleted,
	).Scan(&fileID)
	if err != nil {
		t.Fatalf("insert reusable logical file: %v", err)
	}

	return fileID
}

func insertReusableTestChunk(t *testing.T, dbconn *sql.DB, hash string, status string) int64 {
	t.Helper()

	var chunkID int64
	err := dbconn.QueryRow(
		`INSERT INTO chunk (chunk_hash, size, status, live_ref_count)
		 VALUES ($1, $2, $3, $4)
		 RETURNING id`,
		hash,
		64,
		status,
		1,
	).Scan(&chunkID)
	if err != nil {
		t.Fatalf("insert reusable chunk %s: %v", hash, err)
	}

	return chunkID
}

func insertReusableTestContainer(t *testing.T, dbconn *sql.DB, filename string, quarantine bool) int64 {
	t.Helper()

	var containerID int64
	err := dbconn.QueryRow(
		`INSERT INTO container (filename, sealed, quarantine, current_size, max_size)
		 VALUES ($1, TRUE, $2, $3, $4)
		 RETURNING id`,
		filename,
		quarantine,
		256,
		container.GetContainerMaxSize(),
	).Scan(&containerID)
	if err != nil {
		t.Fatalf("insert reusable container %s: %v", filename, err)
	}

	return containerID
}

func insertReusableTestBlock(t *testing.T, dbconn *sql.DB, chunkID int64, containerID int64, offset int64) {
	t.Helper()

	if _, err := dbconn.Exec(
		`INSERT INTO blocks (chunk_id, codec, format_version, plaintext_size, stored_size, container_id, block_offset)
		 VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		chunkID,
		"plain",
		1,
		64,
		64,
		containerID,
		offset,
	); err != nil {
		t.Fatalf("insert reusable block for chunk %d: %v", chunkID, err)
	}
}

func insertReusableTestFileChunk(t *testing.T, dbconn *sql.DB, fileID int64, chunkID int64, chunkOrder int) {
	t.Helper()

	if _, err := dbconn.Exec(
		`INSERT INTO file_chunk (logical_file_id, chunk_id, chunk_order)
		 VALUES ($1, $2, $3)`,
		fileID,
		chunkID,
		chunkOrder,
	); err != nil {
		t.Fatalf("insert reusable file_chunk file=%d chunk=%d order=%d: %v", fileID, chunkID, chunkOrder, err)
	}
}

func writeReusableTestContainerFile(t *testing.T, containersDir string, filename string) {
	t.Helper()

	path := filepath.Join(containersDir, filename)
	if err := os.WriteFile(path, []byte("container"), 0644); err != nil {
		t.Fatalf("write reusable container file %s: %v", filename, err)
	}
}

// TestMarkLogicalFileForRebuildClearsFilechunkAndDecrementsRefs verifies that
// markLogicalFileForRebuildWithContext atomically:
//   - marks the logical file ABORTED,
//   - removes all stale file_chunk rows, and
//   - decrements chunk.live_ref_count for each removed mapping.
func TestMarkLogicalFileForRebuildClearsFilechunkAndDecrementsRefs(t *testing.T) {
	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	defer func() { _ = dbconn.Close() }()

	if err := db.RunMigrations(dbconn); err != nil {
		t.Fatalf("run migrations: %v", err)
	}

	// Create a completed logical file.
	var fileID int64
	if err := dbconn.QueryRow(
		`INSERT INTO logical_file (original_name, total_size, file_hash, status)
		 VALUES ($1, $2, $3, $4) RETURNING id`,
		"rebuild_test.bin", 128, "rebuild-file-hash", filestate.LogicalFileCompleted,
	).Scan(&fileID); err != nil {
		t.Fatalf("insert logical_file: %v", err)
	}

	// Create two chunks that already have live_ref_count=1 (as set by linkFileChunk).
	insertChunk := func(hash string) int64 {
		t.Helper()
		var id int64
		if err := dbconn.QueryRow(
			`INSERT INTO chunk (chunk_hash, size, status, live_ref_count)
			 VALUES ($1, 64, $2, 1) RETURNING id`,
			hash, filestate.ChunkCompleted,
		).Scan(&id); err != nil {
			t.Fatalf("insert chunk %s: %v", hash, err)
		}
		return id
	}
	chunkA := insertChunk("rebuild-chunk-a")
	chunkB := insertChunk("rebuild-chunk-b")

	// Wire up file_chunk mappings (simulating what linkFileChunk already did).
	insertReusableTestFileChunk(t, dbconn, fileID, chunkA, 0)
	insertReusableTestFileChunk(t, dbconn, fileID, chunkB, 1)

	// Run the function under test.
	ctx := context.Background()
	if err := markLogicalFileForRebuildWithContext(ctx, dbconn, fileID); err != nil {
		t.Fatalf("markLogicalFileForRebuildWithContext: %v", err)
	}

	// Logical file must be ABORTED.
	var status string
	if err := dbconn.QueryRow(`SELECT status FROM logical_file WHERE id = $1`, fileID).Scan(&status); err != nil {
		t.Fatalf("read logical_file status: %v", err)
	}
	if status != filestate.LogicalFileAborted {
		t.Errorf("expected logical_file status ABORTED, got %s", status)
	}

	// file_chunk rows must be gone.
	var mappingCount int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM file_chunk WHERE logical_file_id = $1`, fileID).Scan(&mappingCount); err != nil {
		t.Fatalf("count file_chunk rows: %v", err)
	}
	if mappingCount != 0 {
		t.Errorf("expected 0 file_chunk rows after rebuild mark, got %d", mappingCount)
	}

	// live_ref_count must have been decremented to 0 for both chunks.
	for _, id := range []int64{chunkA, chunkB} {
		var refCount int64
		if err := dbconn.QueryRow(`SELECT live_ref_count FROM chunk WHERE id = $1`, id).Scan(&refCount); err != nil {
			t.Fatalf("read live_ref_count for chunk %d: %v", id, err)
		}
		if refCount != 0 {
			t.Errorf("expected live_ref_count=0 for chunk %d after rebuild mark, got %d", id, refCount)
		}

		var retryCount int64
		if err := dbconn.QueryRow(`SELECT retry_count FROM chunk WHERE id = $1`, id).Scan(&retryCount); err != nil {
			t.Fatalf("read retry_count for chunk %d: %v", id, err)
		}
		if retryCount != 0 {
			t.Errorf("expected retry_count=0 for chunk %d during generic rebuild cleanup, got %d", id, retryCount)
		}
	}
}

func TestMarkLogicalFileForRebuildRemovesStaleFileChunkGarbage(t *testing.T) {
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
		`INSERT INTO logical_file (original_name, total_size, file_hash, status)
		 VALUES ($1, $2, $3, $4) RETURNING id`,
		"stale-garbage.bin", 256, "stale-garbage-hash", filestate.LogicalFileCompleted,
	).Scan(&fileID); err != nil {
		t.Fatalf("insert logical_file: %v", err)
	}

	insertChunk := func(hash string) int64 {
		t.Helper()
		var id int64
		if err := dbconn.QueryRow(
			`INSERT INTO chunk (chunk_hash, size, status, live_ref_count)
			 VALUES ($1, 64, $2, 1) RETURNING id`,
			hash, filestate.ChunkCompleted,
		).Scan(&id); err != nil {
			t.Fatalf("insert chunk %s: %v", hash, err)
		}
		return id
	}

	chunkValid := insertChunk("stale-garbage-valid")
	chunkStale := insertChunk("stale-garbage-extra")

	insertReusableTestFileChunk(t, dbconn, fileID, chunkValid, 0)
	insertReusableTestFileChunk(t, dbconn, fileID, chunkStale, 99)

	ctx := context.Background()
	if err := markLogicalFileForRebuildWithContext(ctx, dbconn, fileID); err != nil {
		t.Fatalf("markLogicalFileForRebuildWithContext: %v", err)
	}

	var mappingCount int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM file_chunk WHERE logical_file_id = $1`, fileID).Scan(&mappingCount); err != nil {
		t.Fatalf("count file_chunk rows: %v", err)
	}
	if mappingCount != 0 {
		t.Fatalf("expected stale file_chunk garbage to be fully removed, got %d rows", mappingCount)
	}

	for _, id := range []int64{chunkValid, chunkStale} {
		var refCount int64
		if err := dbconn.QueryRow(`SELECT live_ref_count FROM chunk WHERE id = $1`, id).Scan(&refCount); err != nil {
			t.Fatalf("read live_ref_count for chunk %d: %v", id, err)
		}
		if refCount != 0 {
			t.Fatalf("expected live_ref_count=0 for chunk %d after stale garbage cleanup, got %d", id, refCount)
		}

		var retryCount int64
		if err := dbconn.QueryRow(`SELECT retry_count FROM chunk WHERE id = $1`, id).Scan(&retryCount); err != nil {
			t.Fatalf("read retry_count for chunk %d: %v", id, err)
		}
		if retryCount != 0 {
			t.Fatalf("expected retry_count=0 for chunk %d during stale garbage cleanup, got %d", id, retryCount)
		}
	}
}

// TestMarkLogicalFileForRebuildIsIdempotentWhenAlreadyAborted verifies that
// calling markLogicalFileForRebuildWithContext on a file that is already ABORTED
// (i.e. another goroutine already marked it) succeeds without error.
func TestMarkLogicalFileForRebuildIsIdempotentWhenAlreadyAborted(t *testing.T) {
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
		`INSERT INTO logical_file (original_name, total_size, file_hash, status)
		 VALUES ($1, $2, $3, $4) RETURNING id`,
		"idempotent_test.bin", 0, "idempotent-file-hash", filestate.LogicalFileAborted,
	).Scan(&fileID); err != nil {
		t.Fatalf("insert logical_file: %v", err)
	}

	ctx := context.Background()
	if err := markLogicalFileForRebuildWithContext(ctx, dbconn, fileID); err != nil {
		t.Fatalf("markLogicalFileForRebuildWithContext on already-ABORTED file: %v", err)
	}
}

func TestMarkLogicalFileForReuseValidationFailureMarksEachChunkSuspiciousOnce(t *testing.T) {
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
		`INSERT INTO logical_file (original_name, total_size, file_hash, status)
		 VALUES ($1, $2, $3, $4) RETURNING id`,
		"duplicate-chunk-ref.bin", 128, "duplicate-ref-hash", filestate.LogicalFileCompleted,
	).Scan(&fileID); err != nil {
		t.Fatalf("insert logical_file: %v", err)
	}

	var chunkID int64
	if err := dbconn.QueryRow(
		`INSERT INTO chunk (chunk_hash, size, status, live_ref_count)
		 VALUES ($1, 64, $2, 2) RETURNING id`,
		"duplicate-ref-chunk", filestate.ChunkCompleted,
	).Scan(&chunkID); err != nil {
		t.Fatalf("insert chunk: %v", err)
	}

	insertReusableTestFileChunk(t, dbconn, fileID, chunkID, 0)
	insertReusableTestFileChunk(t, dbconn, fileID, chunkID, 1)

	ctx := context.Background()
	if err := markLogicalFileForReuseValidationFailureWithContext(ctx, dbconn, fileID); err != nil {
		t.Fatalf("markLogicalFileForReuseValidationFailureWithContext: %v", err)
	}

	var refCount int64
	if err := dbconn.QueryRow(`SELECT live_ref_count FROM chunk WHERE id = $1`, chunkID).Scan(&refCount); err != nil {
		t.Fatalf("read live_ref_count: %v", err)
	}
	if refCount != 0 {
		t.Fatalf("expected live_ref_count=0 after removing two mappings, got %d", refCount)
	}

	var retryCount int64
	if err := dbconn.QueryRow(`SELECT retry_count FROM chunk WHERE id = $1`, chunkID).Scan(&retryCount); err != nil {
		t.Fatalf("read retry_count: %v", err)
	}
	if retryCount != 1 {
		t.Fatalf("expected retry_count=1 for duplicate chunk references, got %d", retryCount)
	}
}

// TestFinalizeLogicalFileStorageAtomicBoundary verifies that the finalization
// transaction atomically verifies all chunks are linked and marks the file complete.
// This guards against the race where chunks are committed but file completion fails.
func TestFinalizeLogicalFileStorageAtomicBoundary(t *testing.T) {
	testCases := []struct {
		name               string
		linkedChunkCount   int
		expectedChunkCount int
		shouldSucceed      bool
		wantErrSubstr      string
	}{
		{
			name:               "all chunks linked",
			linkedChunkCount:   3,
			expectedChunkCount: 3,
			shouldSucceed:      true,
		},
		{
			name:               "missing chunks (fewer linked)",
			linkedChunkCount:   2,
			expectedChunkCount: 3,
			shouldSucceed:      false,
			wantErrSubstr:      "has 2 linked chunks, expected 3",
		},
		{
			name:               "extra chunks (more linked)",
			linkedChunkCount:   4,
			expectedChunkCount: 3,
			shouldSucceed:      false,
			wantErrSubstr:      "has 4 linked chunks, expected 3",
		},
		{
			name:               "non-contiguous ordering",
			linkedChunkCount:   3,
			expectedChunkCount: 3,
			shouldSucceed:      false,
			wantErrSubstr:      "chunk_order max is",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
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
				`INSERT INTO logical_file (original_name, total_size, file_hash, status)
				 VALUES ($1, $2, $3, $4) RETURNING id`,
				"finalize_test.bin", 1024, "finalize-test-hash", filestate.LogicalFileProcessing,
			).Scan(&fileID); err != nil {
				t.Fatalf("insert logical_file: %v", err)
			}

			// Insert chunks
			for i := 0; i < tc.linkedChunkCount; i++ {
				chunkID := insertReusableTestChunk(t, dbconn, fmt.Sprintf("finalize-chunk-%d", i), filestate.ChunkCompleted)

				chunkOrder := i
				// For the non-contiguous ordering test, skip order 1
				if tc.name == "non-contiguous ordering" && i == 1 {
					chunkOrder = 5 // Gap in sequence
				}

				insertReusableTestFileChunk(t, dbconn, fileID, chunkID, chunkOrder)
			}

			ctx, cancel := db.NewOperationContext(context.Background())
			defer cancel()

			err = finalizeLogicalFileStorageWithContext(ctx, dbconn, fileID, tc.expectedChunkCount)

			if tc.shouldSucceed {
				if err != nil {
					t.Fatalf("expected finalize to succeed, got error: %v", err)
				}

				// Verify file is marked COMPLETED
				var status string
				if err := dbconn.QueryRowContext(ctx, `SELECT status FROM logical_file WHERE id = $1`, fileID).Scan(&status); err != nil {
					t.Fatalf("read logical_file status: %v", err)
				}
				if status != filestate.LogicalFileCompleted {
					t.Fatalf("expected status COMPLETED, got %s", status)
				}
			} else {
				if err == nil {
					t.Fatalf("expected finalize to fail, but succeeded")
				}
				if tc.wantErrSubstr != "" && !strings.Contains(err.Error(), tc.wantErrSubstr) {
					t.Fatalf("expected error containing %q, got: %v", tc.wantErrSubstr, err)
				}

				// Verify file is still PROCESSING (transaction rolled back)
				var status string
				if err := dbconn.QueryRowContext(ctx, `SELECT status FROM logical_file WHERE id = $1`, fileID).Scan(&status); err != nil {
					t.Fatalf("read logical_file status: %v", err)
				}
				if status != filestate.LogicalFileProcessing {
					t.Fatalf("expected file to remain PROCESSING after failed finalize, got %s", status)
				}
			}
		})
	}
}
