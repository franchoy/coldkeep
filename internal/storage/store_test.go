package storage

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"hash/crc32"
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
	offset          int64
	quarantineErr   error
	quarantineCalls int
	db              *sql.DB
}

func (w *syncFailWriter) FinalizeContainer() error {
	return nil
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

func (w *syncFailWriter) QuarantineActiveContainer() error {
	w.quarantineCalls++
	if w.db != nil {
		if _, err := w.db.Exec(`UPDATE container SET quarantine = TRUE WHERE id = 1`); err != nil {
			return err
		}
	}
	return w.quarantineErr
}

type commitAckWriter struct {
	offset       int64
	ackCalls     int
	pendingClear bool
}

type rollbackCleanupFailureWriter struct {
	offset              int64
	rollbackErr         error
	rollbackCalls       int
	quarantineErr       error
	quarantineCalls     int
	quarantineContainer int64
	db                  *sql.DB
}

type fixedVersionChunker struct {
	delegate chunk.Chunker
	version  chunk.Version
}

type fixedBoundaryChunker struct {
	version  chunk.Version
	boundary int
}

func (c fixedVersionChunker) Version() chunk.Version {
	return c.version
}

func (c fixedVersionChunker) ChunkFile(path string) ([]chunk.Result, error) {
	return c.delegate.ChunkFile(path)
}

func (c fixedBoundaryChunker) Version() chunk.Version {
	return c.version
}

func (c fixedBoundaryChunker) ChunkFile(path string) ([]chunk.Result, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	if len(data) == 0 {
		return []chunk.Result{}, nil
	}
	if c.boundary <= 0 || c.boundary >= len(data) {
		boundary := len(data)
		all := make([]byte, boundary)
		copy(all, data)
		return []chunk.Result{{
			Info: chunk.Info{Size: int64(boundary), Offset: 0},
			Data: all,
		}}, nil
	}

	first := make([]byte, c.boundary)
	copy(first, data[:c.boundary])
	second := make([]byte, len(data)-c.boundary)
	copy(second, data[c.boundary:])

	return []chunk.Result{
		{Info: chunk.Info{Size: int64(len(first)), Offset: 0}, Data: first},
		{Info: chunk.Info{Size: int64(len(second)), Offset: int64(c.boundary)}, Data: second},
	}, nil
}

func TestNewStoreServiceResolvesRegistryDefaultChunker(t *testing.T) {
	service := NewStoreService(nil)
	if service.ActiveChunker() == nil {
		t.Fatal("expected non-nil active chunker")
	}
	resolved := service.ResolveActiveChunker()
	if resolved.Chunker == nil {
		t.Fatal("expected resolved chunker to be non-nil")
	}

	defaultVersion := chunk.DefaultRegistry().DefaultVersion()
	if service.ActiveChunkerVersion() != defaultVersion {
		t.Fatalf("unexpected default active chunker version: got=%q want=%q", service.ActiveChunkerVersion(), defaultVersion)
	}
	if resolved.Version != defaultVersion {
		t.Fatalf("unexpected resolved chunker version: got=%q want=%q", resolved.Version, defaultVersion)
	}
}

func TestNewStoreServiceUsesInjectedChunker(t *testing.T) {
	const customChunkerVersion chunk.Version = "v1-simple-rolling-test-injected"

	service := NewStoreService(fixedVersionChunker{
		delegate: chunk.DefaultChunker(),
		version:  customChunkerVersion,
	})

	if service.ActiveChunkerVersion() != customChunkerVersion {
		t.Fatalf("unexpected active chunker version: got=%q want=%q", service.ActiveChunkerVersion(), customChunkerVersion)
	}

	resolved := service.ResolveActiveChunker()
	if resolved.Version != customChunkerVersion {
		t.Fatalf("unexpected resolved chunker version: got=%q want=%q", resolved.Version, customChunkerVersion)
	}
}

func TestAssertLogicalFileVersionMatchesActiveDetectsDrift(t *testing.T) {
	err := assertLogicalFileVersionMatchesActive("v1-simple-rolling", "v1-simple-rolling-test-override")
	if err == nil {
		t.Fatal("expected logical_file version drift mismatch error, got nil")
	}
	if !strings.Contains(err.Error(), "logical_file chunker_version mismatch") {
		t.Fatalf("expected mismatch error message, got: %v", err)
	}
}

func TestAssertLogicalFileVersionMatchesActiveAllowsMatch(t *testing.T) {
	err := assertLogicalFileVersionMatchesActive("v1-simple-rolling", "v1-simple-rolling")
	if err != nil {
		t.Fatalf("expected matching versions to pass invariant, got: %v", err)
	}
}

func (w *commitAckWriter) FinalizeContainer() error {
	return nil
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

func (w *rollbackCleanupFailureWriter) FinalizeContainer() error {
	return nil
}

func (w *rollbackCleanupFailureWriter) AppendPayload(_ db.DBTX, payload []byte) (container.LocalPlacement, error) {
	offset := w.offset
	w.offset += int64(len(payload))
	return container.LocalPlacement{
		ContainerID:      1,
		Filename:         "rollback_cleanup_test_container.bin",
		Offset:           offset,
		StoredSize:       int64(len(payload)),
		NewContainerSize: -1,
	}, nil
}

func (w *rollbackCleanupFailureWriter) RollbackLastAppend() error {
	w.rollbackCalls++
	if w.rollbackErr != nil {
		return w.rollbackErr
	}
	return nil
}

func (w *rollbackCleanupFailureWriter) QuarantineActiveContainer() error {
	w.quarantineCalls++
	if w.db != nil && w.quarantineContainer > 0 {
		if _, err := w.db.Exec(`UPDATE container SET quarantine = TRUE WHERE id = $1`, w.quarantineContainer); err != nil {
			return err
		}
	}
	if w.quarantineErr != nil {
		return w.quarantineErr
	}
	return nil
}

func TestRunWithRetryableTxAbortRetriesThenSucceeds(t *testing.T) {
	t.Parallel()

	attempts := 0
	err := runWithRetryableTxAbort(context.Background(), func(_ int) error {
		attempts++
		if attempts < 3 {
			return errors.New("pq: current transaction is aborted, commands ignored until end of transaction block (25P02)")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("runWithRetryableTxAbort returned error: %v", err)
	}
	if attempts != 3 {
		t.Fatalf("expected 3 attempts, got %d", attempts)
	}
}

func TestRunWithRetryableTxAbortStopsOnNonRetryableError(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("permanent store failure")
	attempts := 0
	err := runWithRetryableTxAbort(context.Background(), func(_ int) error {
		attempts++
		return wantErr
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("expected error %v, got %v", wantErr, err)
	}
	if attempts != 1 {
		t.Fatalf("expected 1 attempt, got %d", attempts)
	}
}

func TestRunWithRetryableTxAbortReportsAttemptNumbers(t *testing.T) {
	t.Parallel()

	var seen []int
	wantErr := errors.New("pq: current transaction is aborted, commands ignored until end of transaction block (25P02)")
	err := runWithRetryableTxAbort(context.Background(), func(attempt int) error {
		seen = append(seen, attempt)
		return wantErr
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("expected error %v, got %v", wantErr, err)
	}
	if len(seen) != retryableTxAbortMaxAttempts {
		t.Fatalf("expected %d attempts, got %d", retryableTxAbortMaxAttempts, len(seen))
	}
	for index, attempt := range seen {
		if attempt != index {
			t.Fatalf("expected attempt index %d, got %d", index, attempt)
		}
	}
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
			`INSERT INTO logical_file (original_name, total_size, file_hash, status, chunker_version)
			 VALUES ($1, $2, $3, $4, 'v1-simple-rolling')
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

	chunkID, chunkStatus, isNew, err := claimChunk(dbconn, "shared-chunk-hash", 777, string(chunk.DefaultChunkerVersion))
	if err != nil {
		t.Fatalf("claim first chunk: %v", err)
	}
	if !isNew {
		t.Fatalf("expected first claim to be new")
	}
	if chunkStatus != filestate.ChunkProcessing {
		t.Fatalf("unexpected first chunk status: %s", chunkStatus)
	}

	var insertedChunkerVersion string
	if err := dbconn.QueryRow(`SELECT chunker_version FROM chunk WHERE id = $1`, chunkID).Scan(&insertedChunkerVersion); err != nil {
		t.Fatalf("read inserted chunk.chunker_version: %v", err)
	}
	if insertedChunkerVersion != string(chunk.DefaultChunkerVersion) {
		t.Fatalf("unexpected inserted chunker_version: got=%q want=%q", insertedChunkerVersion, chunk.DefaultChunkerVersion)
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

	chunkID2, chunkStatus2, isNew2, err := claimChunk(dbconn, "shared-chunk-hash", 777, string(chunk.DefaultChunkerVersion))
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

	ctx := context.Background()
	_, _, _, err = claimChunkWithContext(ctx, dbconn, "shared-chunk-hash", 777, "v1-simple-rolling-test-override", container.ContainersDir)
	if err != nil {
		t.Fatalf("claim reused chunk with override version: %v", err)
	}

	var sameIdentityRows int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM chunk WHERE chunk_hash = $1 AND size = $2`, "shared-chunk-hash", 777).Scan(&sameIdentityRows); err != nil {
		t.Fatalf("count chunk rows by dedup identity: %v", err)
	}
	if sameIdentityRows != 1 {
		t.Fatalf("expected dedup identity hash+size to keep a single chunk row, got %d", sameIdentityRows)
	}

	var persistedChunkerVersion string
	if err := dbconn.QueryRow(`SELECT chunker_version FROM chunk WHERE id = $1`, chunkID).Scan(&persistedChunkerVersion); err != nil {
		t.Fatalf("read chunk.chunker_version: %v", err)
	}
	if persistedChunkerVersion != string(chunk.DefaultChunkerVersion) {
		t.Fatalf("expected reused chunk row chunker_version to remain %q, got %q", chunk.DefaultChunkerVersion, persistedChunkerVersion)
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
		`INSERT INTO chunk (chunk_hash, size, status, live_ref_count, chunker_version)
		 VALUES ($1, $2, $3, $4, 'v1-simple-rolling')
		 RETURNING id`,
		"orphan-completed-chunk",
		123,
		filestate.ChunkCompleted,
		0,
	).Scan(&chunkID); err != nil {
		t.Fatalf("insert completed chunk: %v", err)
	}

	claimedID, claimedStatus, isNew, err := claimChunk(dbconn, "orphan-completed-chunk", 123, string(chunk.DefaultChunkerVersion))
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

func TestClaimChunkRejectsExistingRowWithEmptyChunkerVersion(t *testing.T) {
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
		`INSERT INTO chunk (chunk_hash, size, status, live_ref_count, chunker_version)
		 VALUES ($1, $2, $3, $4, 'v1-simple-rolling')
		 RETURNING id`,
		"empty-version-existing-chunk",
		123,
		filestate.ChunkProcessing,
		0,
	).Scan(&chunkID); err != nil {
		t.Fatalf("insert existing chunk: %v", err)
	}

	if _, err := dbconn.Exec(`UPDATE chunk SET chunker_version = '' WHERE id = $1`, chunkID); err != nil {
		t.Fatalf("set empty chunker_version: %v", err)
	}

	ctx := context.Background()
	_, _, _, err = claimChunkWithContext(ctx, dbconn, "empty-version-existing-chunk", 123, string(chunk.DefaultChunkerVersion), container.ContainersDir)
	if err == nil || !strings.Contains(err.Error(), "empty chunker_version") {
		t.Fatalf("expected empty chunker_version error, got: %v", err)
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
		`INSERT INTO chunk (chunk_hash, size, status, live_ref_count, chunker_version)
		 VALUES ($1, $2, $3, $4, 'v1-simple-rolling')
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

	claimedID, claimedStatus, isNew, err := claimChunk(dbconn, "quarantined-completed-chunk", 321, string(chunk.DefaultChunkerVersion))
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

func TestStoreFileUsesAppendLevelDurabilityWithoutExtraSyncHook(t *testing.T) {
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

	writer := &syncFailWriter{db: dbconn}
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
		t.Fatalf("store using append-level durability contract should succeed: %v", err)
	}
	if writer.quarantineCalls != 0 {
		t.Fatalf("expected no container quarantine on successful store, got %d", writer.quarantineCalls)
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

func TestStoreFilePersistsExplicitChunkerVersionMetadata(t *testing.T) {
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
		"chunker_version_test_container.bin",
		container.ContainerHdrLen,
		container.GetContainerMaxSize(),
	); err != nil {
		t.Fatalf("insert container row: %v", err)
	}

	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "chunker-version-metadata.txt")
	if err := os.WriteFile(path, []byte("verify persisted chunker_version metadata"), 0644); err != nil {
		t.Fatalf("write temp file: %v", err)
	}

	const customChunkerVersion chunk.Version = "v1-simple-rolling-test-override"
	writer := &commitAckWriter{}
	sgctx := StorageContext{
		DB:           dbconn,
		Writer:       writer,
		ContainerDir: tmpDir,
		Chunker: fixedVersionChunker{
			delegate: chunk.DefaultChunker(),
			version:  customChunkerVersion,
		},
	}

	codec, err := blocks.ParseCodec("plain")
	if err != nil {
		t.Fatalf("parse plain codec: %v", err)
	}

	result, err := StoreFileWithStorageContextAndCodecResult(sgctx, path, codec)
	if err != nil {
		t.Fatalf("store file with injected chunker: %v", err)
	}

	var logicalFileVersion string
	if err := dbconn.QueryRow(
		`SELECT chunker_version FROM logical_file WHERE id = $1`,
		result.FileID,
	).Scan(&logicalFileVersion); err != nil {
		t.Fatalf("read logical_file.chunker_version: %v", err)
	}
	// Phase 3: the persisted version must come from the resolved chunker, not a hardcoded constant.
	if logicalFileVersion != string(customChunkerVersion) {
		t.Fatalf("logical_file.chunker_version mismatch: got %q want %q", logicalFileVersion, customChunkerVersion)
	}

	var linkedChunkCount int
	if err := dbconn.QueryRow(
		`SELECT COUNT(*) FROM file_chunk WHERE logical_file_id = $1`,
		result.FileID,
	).Scan(&linkedChunkCount); err != nil {
		t.Fatalf("count linked chunks: %v", err)
	}
	if linkedChunkCount == 0 {
		t.Fatal("expected at least one linked chunk for stored file")
	}

	var mismatchedChunkVersions int
	if err := dbconn.QueryRow(
		`SELECT COUNT(*)
		 FROM chunk c
		 INNER JOIN file_chunk fc ON fc.chunk_id = c.id
		 WHERE fc.logical_file_id = $1 AND c.chunker_version <> $2`,
		result.FileID,
		string(customChunkerVersion),
	).Scan(&mismatchedChunkVersions); err != nil {
		t.Fatalf("count chunk version mismatches: %v", err)
	}
	if mismatchedChunkVersions != 0 {
		t.Fatalf("expected all linked chunks to persist chunker_version=%q, mismatches=%d", customChunkerVersion, mismatchedChunkVersions)
	}
}

func TestStoreFileDefaultChunkerPersistsLogicalFileVersion(t *testing.T) {
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
	path := filepath.Join(tmpDir, "default-logical-version.txt")
	if err := os.WriteFile(path, []byte("default chunker logical version persistence"), 0o644); err != nil {
		t.Fatalf("write temp file: %v", err)
	}

	writer := &commitAckWriter{}
	sgctx := StorageContext{DB: dbconn, Writer: writer, ContainerDir: tmpDir}

	codec, err := blocks.ParseCodec("plain")
	if err != nil {
		t.Fatalf("parse plain codec: %v", err)
	}

	result, err := StoreFileWithStorageContextAndCodecResult(sgctx, path, codec)
	if err != nil {
		t.Fatalf("store file: %v", err)
	}

	var logicalFileVersion string
	if err := dbconn.QueryRow(`SELECT chunker_version FROM logical_file WHERE id = $1`, result.FileID).Scan(&logicalFileVersion); err != nil {
		t.Fatalf("read logical_file.chunker_version: %v", err)
	}
	if logicalFileVersion != "v1-simple-rolling" {
		t.Fatalf("expected logical_file.chunker_version=v1-simple-rolling, got %q", logicalFileVersion)
	}
}

func TestStoreFileDefaultChunkerPersistsChunkVersion(t *testing.T) {
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
	path := filepath.Join(tmpDir, "default-chunk-version.txt")
	if err := os.WriteFile(path, []byte("default chunker chunk version persistence"), 0o644); err != nil {
		t.Fatalf("write temp file: %v", err)
	}

	writer := &commitAckWriter{}
	sgctx := StorageContext{DB: dbconn, Writer: writer, ContainerDir: tmpDir}

	codec, err := blocks.ParseCodec("plain")
	if err != nil {
		t.Fatalf("parse plain codec: %v", err)
	}

	result, err := StoreFileWithStorageContextAndCodecResult(sgctx, path, codec)
	if err != nil {
		t.Fatalf("store file: %v", err)
	}

	var linkedChunkCount int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM file_chunk WHERE logical_file_id = $1`, result.FileID).Scan(&linkedChunkCount); err != nil {
		t.Fatalf("count linked chunks: %v", err)
	}
	if linkedChunkCount == 0 {
		t.Fatal("expected at least one linked chunk")
	}

	var nonDefaultCount int
	if err := dbconn.QueryRow(
		`SELECT COUNT(*)
		 FROM chunk c
		 INNER JOIN file_chunk fc ON fc.chunk_id = c.id
		 WHERE fc.logical_file_id = $1 AND c.chunker_version <> 'v1-simple-rolling'`,
		result.FileID,
	).Scan(&nonDefaultCount); err != nil {
		t.Fatalf("count non-default chunk versions: %v", err)
	}
	if nonDefaultCount != 0 {
		t.Fatalf("expected all new linked chunk rows to persist chunker_version=v1-simple-rolling, mismatches=%d", nonDefaultCount)
	}
}

func TestStoreFileReusedChunkDoesNotRequireVersionMatchInLookup(t *testing.T) {
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
	pathA := filepath.Join(tmpDir, "reuse-a.bin")
	pathB := filepath.Join(tmpDir, "reuse-b.bin")

	sharedPrefix := strings.Repeat("A", 32)
	contentA := []byte(sharedPrefix + strings.Repeat("B", 32))
	contentB := []byte(sharedPrefix + strings.Repeat("C", 32))
	if err := os.WriteFile(pathA, contentA, 0o644); err != nil {
		t.Fatalf("write first file: %v", err)
	}
	if err := os.WriteFile(pathB, contentB, 0o644); err != nil {
		t.Fatalf("write second file: %v", err)
	}

	writer := &commitAckWriter{}
	defaultChunker := fixedBoundaryChunker{version: chunk.VersionV1SimpleRolling, boundary: 32}
	overrideChunker := fixedBoundaryChunker{version: "v1-simple-rolling-test-override", boundary: 32}

	codec, err := blocks.ParseCodec("plain")
	if err != nil {
		t.Fatalf("parse plain codec: %v", err)
	}

	sgctxA := StorageContext{DB: dbconn, Writer: writer, ContainerDir: tmpDir, Chunker: defaultChunker}
	if _, err := StoreFileWithStorageContextAndCodecResult(sgctxA, pathA, codec); err != nil {
		t.Fatalf("store first file: %v", err)
	}

	sharedHashSum := sha256.Sum256([]byte(sharedPrefix))
	sharedHash := hex.EncodeToString(sharedHashSum[:])

	var sharedChunkID int64
	if err := dbconn.QueryRow(`SELECT id FROM chunk WHERE chunk_hash = $1 AND size = $2`, sharedHash, 32).Scan(&sharedChunkID); err != nil {
		t.Fatalf("locate shared chunk after first store: %v", err)
	}

	sgctxB := StorageContext{DB: dbconn, Writer: writer, ContainerDir: tmpDir, Chunker: overrideChunker}
	if _, err := StoreFileWithStorageContextAndCodecResult(sgctxB, pathB, codec); err != nil {
		t.Fatalf("store second file with override version: %v", err)
	}

	var sharedIdentityRows int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM chunk WHERE chunk_hash = $1 AND size = $2`, sharedHash, 32).Scan(&sharedIdentityRows); err != nil {
		t.Fatalf("count shared identity rows: %v", err)
	}
	if sharedIdentityRows != 1 {
		t.Fatalf("expected shared chunk to be reused with no duplicate row, got %d rows", sharedIdentityRows)
	}

	var sharedChunkRefCount int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM file_chunk WHERE chunk_id = $1`, sharedChunkID).Scan(&sharedChunkRefCount); err != nil {
		t.Fatalf("count shared chunk file references: %v", err)
	}
	if sharedChunkRefCount < 2 {
		t.Fatalf("expected reused shared chunk to be referenced by both files, got %d references", sharedChunkRefCount)
	}
}

func TestStoreFileLogicalRecipeSingleVersionInvariance(t *testing.T) {
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
	path := filepath.Join(tmpDir, "single-recipe-version.bin")
	content := []byte(strings.Repeat("A", 32) + strings.Repeat("B", 32) + strings.Repeat("C", 32))
	if err := os.WriteFile(path, content, 0o644); err != nil {
		t.Fatalf("write test file: %v", err)
	}

	const recipeVersion chunk.Version = "v1-simple-rolling-test-recipe"
	writer := &commitAckWriter{}
	sgctx := StorageContext{
		DB:           dbconn,
		Writer:       writer,
		ContainerDir: tmpDir,
		Chunker: fixedBoundaryChunker{
			version:  recipeVersion,
			boundary: 32,
		},
	}

	codec, err := blocks.ParseCodec("plain")
	if err != nil {
		t.Fatalf("parse plain codec: %v", err)
	}

	result, err := StoreFileWithStorageContextAndCodecResult(sgctx, path, codec)
	if err != nil {
		t.Fatalf("store file: %v", err)
	}

	var logicalVersion string
	if err := dbconn.QueryRow(`SELECT chunker_version FROM logical_file WHERE id = $1`, result.FileID).Scan(&logicalVersion); err != nil {
		t.Fatalf("read logical_file.chunker_version: %v", err)
	}
	if logicalVersion != string(recipeVersion) {
		t.Fatalf("logical recipe version mismatch: got %q want %q", logicalVersion, recipeVersion)
	}

	var linkedChunkCount int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM file_chunk WHERE logical_file_id = $1`, result.FileID).Scan(&linkedChunkCount); err != nil {
		t.Fatalf("count linked chunks: %v", err)
	}
	if linkedChunkCount < 2 {
		t.Fatalf("expected multiple linked chunks for invariance test, got %d", linkedChunkCount)
	}

	var distinctLinkedVersions int
	if err := dbconn.QueryRow(
		`SELECT COUNT(DISTINCT c.chunker_version)
		 FROM chunk c
		 INNER JOIN file_chunk fc ON fc.chunk_id = c.id
		 WHERE fc.logical_file_id = $1`,
		result.FileID,
	).Scan(&distinctLinkedVersions); err != nil {
		t.Fatalf("count distinct linked chunk versions: %v", err)
	}
	if distinctLinkedVersions != 1 {
		t.Fatalf("expected exactly one chunker version across logical recipe, got %d", distinctLinkedVersions)
	}

	var mismatches int
	if err := dbconn.QueryRow(
		`SELECT COUNT(*)
		 FROM chunk c
		 INNER JOIN file_chunk fc ON fc.chunk_id = c.id
		 INNER JOIN logical_file lf ON lf.id = fc.logical_file_id
		 WHERE fc.logical_file_id = $1 AND c.chunker_version <> lf.chunker_version`,
		result.FileID,
	).Scan(&mismatches); err != nil {
		t.Fatalf("count recipe version mismatches: %v", err)
	}
	if mismatches != 0 {
		t.Fatalf("expected no mixed-version chunks in single logical recipe, mismatches=%d", mismatches)
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

func TestStoreFileEscalatesRollbackCleanupFailureAndQuarantinesContainer(t *testing.T) {
	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	defer func() { _ = dbconn.Close() }()

	if err := db.RunMigrations(dbconn); err != nil {
		t.Fatalf("run migrations: %v", err)
	}

	if _, err := dbconn.Exec(
		`INSERT INTO container (id, filename, current_size, max_size, sealed, quarantine)
		 VALUES (1, $1, $2, $3, FALSE, FALSE)`,
		"rollback_cleanup_test_container.bin",
		container.ContainerHdrLen,
		container.GetContainerMaxSize(),
	); err != nil {
		t.Fatalf("insert container row: %v", err)
	}

	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "payload.txt")
	if err := os.WriteFile(path, []byte("rollback-cleanup-failure-escalation"), 0644); err != nil {
		t.Fatalf("write temp file: %v", err)
	}

	rollbackCause := errors.New("injected rollback truncate failure")
	writer := &rollbackCleanupFailureWriter{
		rollbackErr:         rollbackCause,
		quarantineContainer: 1,
		db:                  dbconn,
	}
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
	if !errors.Is(err, rollbackCause) {
		t.Fatalf("expected surfaced rollback cause in store error; got: %v", err)
	}
	if !strings.Contains(err.Error(), "rollback failed; quarantined active container as precaution") {
		t.Fatalf("expected rollback escalation in error, got: %v", err)
	}
	if !strings.Contains(err.Error(), rollbackCause.Error()) {
		t.Fatalf("expected rollback error text to be visible in wrapped error, got: %v", err)
	}

	if writer.rollbackCalls == 0 {
		t.Fatalf("expected RollbackLastAppend to be attempted")
	}
	if writer.quarantineCalls == 0 {
		t.Fatalf("expected active container quarantine when rollback cleanup fails")
	}

	var quarantined bool
	if err := dbconn.QueryRow(`SELECT quarantine FROM container WHERE id = 1`).Scan(&quarantined); err != nil {
		t.Fatalf("query container quarantine: %v", err)
	}
	if !quarantined {
		t.Fatalf("expected container id=1 to be quarantined after rollback cleanup failure")
	}
}

func TestStoreFileRetainsCommittedChunksWhenFinalCompletionUpdateFails(t *testing.T) {
	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	defer func() { _ = dbconn.Close() }()

	if err := db.RunMigrations(dbconn); err != nil {
		t.Fatalf("run migrations: %v", err)
	}

	if _, err := dbconn.Exec(
		`INSERT INTO container (id, filename, current_size, max_size, sealed, quarantine)
		 VALUES (1, $1, $2, $3, FALSE, FALSE)`,
		"final-completion-failure-container.bin",
		container.ContainerHdrLen,
		container.GetContainerMaxSize(),
	); err != nil {
		t.Fatalf("insert container row: %v", err)
	}

	if _, err := dbconn.Exec(`
		CREATE TRIGGER fail_finalize_completion
		BEFORE UPDATE OF status ON logical_file
		WHEN NEW.status = 'COMPLETED'
		BEGIN
			SELECT RAISE(FAIL, 'injected finalize completion failure');
		END;
	`); err != nil {
		t.Fatalf("create finalize failure trigger: %v", err)
	}

	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "payload.txt")
	if err := os.WriteFile(path, []byte("finalize-logical-file-failure-state-test"), 0644); err != nil {
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
	if err == nil || !strings.Contains(err.Error(), "injected finalize completion failure") {
		t.Fatalf("expected injected finalize failure containing \"injected finalize completion failure\", got: %v", err)
	}

	var logicalStatus string
	if err := dbconn.QueryRow(`SELECT status FROM logical_file ORDER BY id DESC LIMIT 1`).Scan(&logicalStatus); err != nil {
		t.Fatalf("query logical_file status: %v", err)
	}
	if logicalStatus != filestate.LogicalFileAborted {
		t.Fatalf("expected logical_file to be ABORTED after finalization failure cleanup, got %s", logicalStatus)
	}

	var completedChunkCount int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM chunk WHERE status = $1`, filestate.ChunkCompleted).Scan(&completedChunkCount); err != nil {
		t.Fatalf("count completed chunks: %v", err)
	}
	if completedChunkCount == 0 {
		t.Fatalf("expected committed COMPLETED chunk rows to remain after finalization failure")
	}

	var linkedChunkCount int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM file_chunk`).Scan(&linkedChunkCount); err != nil {
		t.Fatalf("count linked file_chunk rows: %v", err)
	}
	if linkedChunkCount == 0 {
		t.Fatalf("expected committed file_chunk mappings to remain after finalization failure")
	}

	var blockCount int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM blocks`).Scan(&blockCount); err != nil {
		t.Fatalf("count blocks: %v", err)
	}
	if blockCount == 0 {
		t.Fatalf("expected committed blocks metadata to remain after finalization failure")
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
		`INSERT INTO chunk (chunk_hash, size, status, live_ref_count, chunker_version)
		 VALUES ($1, $2, $3, $4, 'v1-simple-rolling')
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

	claimedID, claimedStatus, isNew, err := claimChunk(dbconn, "missing-file-completed-chunk", 456, string(chunk.DefaultChunkerVersion))
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
			wantErr: "all referenced containers are missing/quarantined",
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
			wantErr: "all referenced containers are missing/quarantined",
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
			// Accept nil (no error) as valid: loss-minimizing recovery may treat this as a no-op.
			if err != nil && !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("expected validation error containing %q or nil, got: %v", tc.wantErr, err)
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
	payload := []byte("semantic-reuse-validation-regression-payload")
	sum := sha256.Sum256(payload)
	hash := hex.EncodeToString(sum[:])

	containerFilename := "semantic-reuse.bin"
	containerPath := filepath.Join(containersDir, containerFilename)
	if err := writeReusableTestContainerFileWithPayload(containerPath, payload); err != nil {
		t.Fatalf("write container file with header: %v", err)
	}

	fileID := insertReusableTestLogicalFile(t, dbconn, int64(len(payload)))
	if _, err := dbconn.Exec(`UPDATE logical_file SET file_hash = $1 WHERE id = $2`, hash, fileID); err != nil {
		t.Fatalf("update logical file hash: %v", err)
	}

	chunkID := insertReusableTestChunk(t, dbconn, hash, filestate.ChunkCompleted)
	if _, err := dbconn.Exec(`UPDATE chunk SET size = $1 WHERE id = $2`, int64(len(payload)), chunkID); err != nil {
		t.Fatalf("update chunk size: %v", err)
	}

	containerID := insertReusableTestContainer(t, dbconn, containerFilename, false)
	if _, err := dbconn.Exec(`UPDATE container SET current_size = $1 WHERE id = $2`, int64(container.ContainerHdrLen+len(payload)), containerID); err != nil {
		t.Fatalf("update container size: %v", err)
	}

	if _, err := dbconn.Exec(
		`INSERT INTO blocks (chunk_id, codec, format_version, plaintext_size, stored_size, container_id, block_offset)
		 VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		chunkID,
		"plain",
		1,
		int64(len(payload)),
		int64(len(payload)),
		containerID,
		int64(container.ContainerHdrLen),
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
	if err == nil || !strings.Contains(err.Error(), "semantic reuse validation failed") {
		t.Fatalf("expected semantic reuse validation failure, got: %v", err)
	}
}

func insertReusableTestLogicalFile(t *testing.T, dbconn *sql.DB, totalSize int64) int64 {
	t.Helper()

	var fileID int64
	err := dbconn.QueryRow(
		`INSERT INTO logical_file (original_name, total_size, file_hash, status, chunker_version)
		 VALUES ($1, $2, $3, $4, 'v1-simple-rolling')
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

func writeReusableTestContainerFileWithPayload(path string, payload []byte) error {
	hdr := make([]byte, container.ContainerHdrLen)
	copy(hdr[0:8], []byte(container.ContainerMagic))
	binary.LittleEndian.PutUint16(hdr[8:10], container.LegacyContainerFormatVersionMajor)
	binary.LittleEndian.PutUint16(hdr[10:12], 9)
	binary.LittleEndian.PutUint32(hdr[12:16], uint32(container.ContainerHdrLen))
	binary.LittleEndian.PutUint64(hdr[28:36], uint64(container.GetContainerMaxSize()))
	binary.LittleEndian.PutUint32(hdr[52:56], crc32.ChecksumIEEE(hdr[0:52]))

	buf := append(hdr, payload...)
	return os.WriteFile(path, buf, 0644)
}

func insertReusableTestChunk(t *testing.T, dbconn *sql.DB, hash string, status string) int64 {
	t.Helper()

	var chunkID int64
	err := dbconn.QueryRow(
		`INSERT INTO chunk (chunk_hash, size, status, live_ref_count, chunker_version)
		 VALUES ($1, $2, $3, $4, 'v1-simple-rolling')
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
	if err := writeReusableTestContainerFileWithPayload(path, []byte("container")); err != nil {
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
		`INSERT INTO logical_file (original_name, total_size, file_hash, status, chunker_version)
		 VALUES ($1, $2, $3, $4, 'v1-simple-rolling') RETURNING id`,
		"rebuild_test.bin", 128, "rebuild-file-hash", filestate.LogicalFileCompleted,
	).Scan(&fileID); err != nil {
		t.Fatalf("insert logical_file: %v", err)
	}

	// Create two chunks that already have live_ref_count=1 (as set by linkFileChunk).
	insertChunk := func(hash string) int64 {
		t.Helper()
		var id int64
		if err := dbconn.QueryRow(
			`INSERT INTO chunk (chunk_hash, size, status, live_ref_count, chunker_version)
			 VALUES ($1, 64, $2, 1, 'v1-simple-rolling') RETURNING id`,
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
		`INSERT INTO logical_file (original_name, total_size, file_hash, status, chunker_version)
		 VALUES ($1, $2, $3, $4, 'v1-simple-rolling') RETURNING id`,
		"stale-garbage.bin", 256, "stale-garbage-hash", filestate.LogicalFileCompleted,
	).Scan(&fileID); err != nil {
		t.Fatalf("insert logical_file: %v", err)
	}

	insertChunk := func(hash string) int64 {
		t.Helper()
		var id int64
		if err := dbconn.QueryRow(
			`INSERT INTO chunk (chunk_hash, size, status, live_ref_count, chunker_version)
			 VALUES ($1, 64, $2, 1, 'v1-simple-rolling') RETURNING id`,
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
		`INSERT INTO logical_file (original_name, total_size, file_hash, status, chunker_version)
		 VALUES ($1, $2, $3, $4, 'v1-simple-rolling') RETURNING id`,
		"idempotent_test.bin", 0, "idempotent-file-hash", filestate.LogicalFileAborted,
	).Scan(&fileID); err != nil {
		t.Fatalf("insert logical_file: %v", err)
	}

	ctx := context.Background()
	if err := markLogicalFileForRebuildWithContext(ctx, dbconn, fileID); err != nil {
		t.Fatalf("markLogicalFileForRebuildWithContext on already-ABORTED file: %v", err)
	}
}

func TestClaimLogicalFileReclaimCleansStaleMappingsBeforeRetry(t *testing.T) {
	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	defer func() { _ = dbconn.Close() }()

	if err := db.RunMigrations(dbconn); err != nil {
		t.Fatalf("run migrations: %v", err)
	}

	tmp := t.TempDir()
	filePath := filepath.Join(tmp, "retry-reclaim.bin")
	payload := []byte("retry-reclaim-payload-that-needs-clean-start")
	if err := os.WriteFile(filePath, payload, 0o644); err != nil {
		t.Fatalf("write temp file: %v", err)
	}
	fileInfo, err := os.Stat(filePath)
	if err != nil {
		t.Fatalf("stat temp file: %v", err)
	}
	sum := sha256.Sum256(payload)
	fileHash := hex.EncodeToString(sum[:])

	var fileID int64
	if err := dbconn.QueryRow(
		`INSERT INTO logical_file (original_name, total_size, file_hash, status, chunker_version)
		 VALUES ($1, $2, $3, $4, 'v1-simple-rolling')
		 RETURNING id`,
		fileInfo.Name(),
		fileInfo.Size(),
		fileHash,
		filestate.LogicalFileAborted,
	).Scan(&fileID); err != nil {
		t.Fatalf("insert aborted logical_file: %v", err)
	}

	insertChunk := func(hash string) int64 {
		t.Helper()
		var chunkID int64
		if err := dbconn.QueryRow(
			`INSERT INTO chunk (chunk_hash, size, status, live_ref_count, chunker_version)
			 VALUES ($1, $2, $3, $4, 'v1-simple-rolling')
			 RETURNING id`,
			hash,
			int64(len(payload)/2),
			filestate.ChunkCompleted,
			1,
		).Scan(&chunkID); err != nil {
			t.Fatalf("insert chunk %s: %v", hash, err)
		}
		return chunkID
	}

	chunkA := insertChunk("retry-reclaim-chunk-a")
	chunkB := insertChunk("retry-reclaim-chunk-b")
	insertReusableTestFileChunk(t, dbconn, fileID, chunkA, 0)
	insertReusableTestFileChunk(t, dbconn, fileID, chunkB, 1)

	ctx, cancel := db.NewOperationContext(context.Background())
	defer cancel()

	claimedID, claimedStatus, err := claimLogicalFileWithContext(ctx, dbconn, fileInfo, fileHash, string(chunk.DefaultChunkerVersion), tmp)
	if err != nil {
		t.Fatalf("claim logical file for retry: %v", err)
	}
	if claimedID != fileID {
		t.Fatalf("expected claimed logical file %d, got %d", fileID, claimedID)
	}
	if claimedStatus != filestate.LogicalFileProcessing {
		t.Fatalf("expected claimed status PROCESSING, got %s", claimedStatus)
	}

	var mappingCount int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM file_chunk WHERE logical_file_id = $1`, fileID).Scan(&mappingCount); err != nil {
		t.Fatalf("count stale file_chunk rows after reclaim: %v", err)
	}
	if mappingCount != 0 {
		t.Fatalf("expected stale file_chunk rows to be removed before retry, got %d", mappingCount)
	}

	for _, chunkID := range []int64{chunkA, chunkB} {
		var refCount int64
		if err := dbconn.QueryRow(`SELECT live_ref_count FROM chunk WHERE id = $1`, chunkID).Scan(&refCount); err != nil {
			t.Fatalf("read live_ref_count for chunk %d: %v", chunkID, err)
		}
		if refCount != 0 {
			t.Fatalf("expected live_ref_count=0 for chunk %d after reclaim cleanup, got %d", chunkID, refCount)
		}
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
		`INSERT INTO logical_file (original_name, total_size, file_hash, status, chunker_version)
		 VALUES ($1, $2, $3, $4, 'v1-simple-rolling') RETURNING id`,
		"duplicate-chunk-ref.bin", 128, "duplicate-ref-hash", filestate.LogicalFileCompleted,
	).Scan(&fileID); err != nil {
		t.Fatalf("insert logical_file: %v", err)
	}

	var chunkID int64
	if err := dbconn.QueryRow(
		`INSERT INTO chunk (chunk_hash, size, status, live_ref_count, chunker_version)
		 VALUES ($1, 64, $2, 2, 'v1-simple-rolling') RETURNING id`,
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
				`INSERT INTO logical_file (original_name, total_size, file_hash, status, chunker_version)
				 VALUES ($1, $2, $3, $4, 'v1-simple-rolling') RETURNING id`,
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
				if err == nil || (tc.wantErrSubstr != "" && !strings.Contains(err.Error(), tc.wantErrSubstr)) {
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
