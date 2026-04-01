package storage

import (
	"database/sql"
	"errors"
	"os"
	"path/filepath"
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

func TestLinkFileChunkIncrementsRefCountOnReuse(t *testing.T) {
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
	if err := tx1.Commit(); err != nil {
		t.Fatalf("commit tx1: %v", err)
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
	// Idempotency check: same mapping conflict should not increment ref_count again.
	if err := linkFileChunk(tx2, fileB, chunkID, 0, !isNew2); err != nil {
		_ = tx2.Rollback()
		t.Fatalf("re-link second file/chunk: %v", err)
	}
	if err := tx2.Commit(); err != nil {
		t.Fatalf("commit tx2: %v", err)
	}

	var refCount int64
	if err := dbconn.QueryRow(`SELECT ref_count FROM chunk WHERE id = $1`, chunkID).Scan(&refCount); err != nil {
		t.Fatalf("read ref_count: %v", err)
	}
	if refCount != 2 {
		t.Fatalf("expected ref_count=2 after two links, got %d", refCount)
	}

	var mappingCount int64
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM file_chunk WHERE chunk_id = $1`, chunkID).Scan(&mappingCount); err != nil {
		t.Fatalf("count mappings: %v", err)
	}
	if mappingCount != 2 {
		t.Fatalf("expected 2 file_chunk mappings, got %d", mappingCount)
	}
}

func TestStoreFileFailsWhenActiveContainerSyncFails(t *testing.T) {
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

	expectedSyncErr := errors.New("forced sync failure")
	writer := &syncFailWriter{syncErr: expectedSyncErr, db: dbconn}
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
	if err == nil {
		t.Fatalf("expected store to fail when sync fails")
	}
	if !errors.Is(err, expectedSyncErr) {
		t.Fatalf("expected sync failure, got: %v", err)
	}
	if writer.syncCalls == 0 {
		t.Fatalf("expected pre-commit sync to be called at least once")
	}
	if writer.retireCalls == 0 {
		t.Fatalf("expected active container retirement after sync failure")
	}

	var abortedCount int
	if err := dbconn.QueryRow(
		`SELECT COUNT(*) FROM logical_file WHERE status = $1`,
		filestate.LogicalFileAborted,
	).Scan(&abortedCount); err != nil {
		t.Fatalf("count aborted logical files: %v", err)
	}
	if abortedCount != 1 {
		t.Fatalf("expected 1 aborted logical file after sync failure, got %d", abortedCount)
	}

	var completedChunks int
	if err := dbconn.QueryRow(
		`SELECT COUNT(*) FROM chunk WHERE status = $1`,
		filestate.ChunkCompleted,
	).Scan(&completedChunks); err != nil {
		t.Fatalf("count completed chunks: %v", err)
	}
	if completedChunks != 0 {
		t.Fatalf("expected no completed chunks after sync failure, got %d", completedChunks)
	}

	var blockCount int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM blocks`).Scan(&blockCount); err != nil {
		t.Fatalf("count blocks: %v", err)
	}
	if blockCount != 0 {
		t.Fatalf("expected no persisted block metadata after sync failure, got %d", blockCount)
	}

	var quarantined bool
	if err := dbconn.QueryRow(`SELECT quarantine FROM container WHERE id = 1`).Scan(&quarantined); err != nil {
		t.Fatalf("query container quarantine: %v", err)
	}
	if !quarantined {
		t.Fatalf("expected active container to be quarantined after sync failure")
	}

}
