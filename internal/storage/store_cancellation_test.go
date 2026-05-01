package storage

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/franchoy/coldkeep/internal/blocks"
	"github.com/franchoy/coldkeep/internal/chunk"
	"github.com/franchoy/coldkeep/internal/container"
	"github.com/franchoy/coldkeep/internal/db"
	_ "github.com/mattn/go-sqlite3"
)

type trackingChunker struct {
	called bool
	err    error
	out    []chunk.Result
}

func (c *trackingChunker) Version() chunk.Version {
	return chunk.VersionV1SimpleRolling
}

func (c *trackingChunker) ChunkFile(path string) ([]chunk.Result, error) {
	c.called = true
	if c.err != nil {
		return nil, c.err
	}
	return c.out, nil
}

// cancelAfterNErrContext deterministically flips to context.Canceled after N Err() checks.
// This allows testing cancellation during the prepare loop without timing flakiness.
type cancelAfterNErrContext struct {
	base      context.Context
	remaining int
}

func (c *cancelAfterNErrContext) Deadline() (deadline time.Time, ok bool) {
	return c.base.Deadline()
}

func (c *cancelAfterNErrContext) Done() <-chan struct{} {
	return nil
}

func (c *cancelAfterNErrContext) Err() error {
	if c.remaining <= 0 {
		return context.Canceled
	}
	c.remaining--
	return nil
}

func (c *cancelAfterNErrContext) Value(key any) any {
	return c.base.Value(key)
}

func setupCancellationTestDB(t *testing.T) *sql.DB {
	t.Helper()

	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	if err := db.RunMigrations(dbconn); err != nil {
		_ = dbconn.Close()
		t.Fatalf("run migrations: %v", err)
	}
	return dbconn
}

func TestPrepareFileForStoreWithContextCancellationBeforeRead(t *testing.T) {
	tmp := t.TempDir()
	path := filepath.Join(tmp, "cancel-before-read.bin")
	if err := os.WriteFile(path, []byte("payload"), 0o600); err != nil {
		t.Fatalf("write fixture: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	ck := &trackingChunker{}
	_, err := prepareFileForStoreWithContext(ctx, path, ck, string(ck.Version()), nil)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got: %v", err)
	}
	if ck.called {
		t.Fatal("chunker should not be called when context is canceled before read")
	}
}

func TestPrepareFileForStoreWithContextCancellationMidPrepare(t *testing.T) {
	tmp := t.TempDir()
	path := filepath.Join(tmp, "cancel-mid-prepare.bin")
	if err := os.WriteFile(path, []byte("payload"), 0o600); err != nil {
		t.Fatalf("write fixture: %v", err)
	}

	results := make([]chunk.Result, 0, 4)
	for i := 0; i < 4; i++ {
		results = append(results, chunk.Result{Data: []byte("chunk-mid-cancel")})
	}

	ck := &trackingChunker{out: results}
	ctx := &cancelAfterNErrContext{base: context.Background(), remaining: 2}

	_, err := prepareFileForStoreWithContext(ctx, path, ck, string(ck.Version()), nil)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled during prepare, got: %v", err)
	}
	if !ck.called {
		t.Fatal("expected chunker to be called before mid-prepare cancellation")
	}
}

func TestStoreFilePrepareFailureDoesNotPartiallyCommit(t *testing.T) {
	dbconn := setupCancellationTestDB(t)
	defer func() { _ = dbconn.Close() }()

	containersDir := t.TempDir()
	path := filepath.Join(t.TempDir(), "prepare-failure.bin")
	if err := os.WriteFile(path, []byte("prepare failure payload"), 0o600); err != nil {
		t.Fatalf("write fixture: %v", err)
	}

	prepareErr := errors.New("forced prepare failure")
	ck := &trackingChunker{err: prepareErr}

	sgctx := StorageContext{
		DB:           dbconn,
		Writer:       container.NewLocalWriterWithDirAndDB(containersDir, container.GetContainerMaxSize(), dbconn),
		ContainerDir: containersDir,
		Chunker:      ck,
	}

	_, err := StoreFileWithStorageContextAndCodecResult(sgctx, path, blocks.CodecPlain)
	if err == nil {
		t.Fatal("expected store error from prepare failure")
	}
	if !errors.Is(err, prepareErr) {
		t.Fatalf("expected wrapped prepare error %q, got: %v", prepareErr, err)
	}

	var logicalCount int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM logical_file`).Scan(&logicalCount); err != nil {
		t.Fatalf("count logical_file rows: %v", err)
	}
	if logicalCount != 0 {
		t.Fatalf("expected no logical_file rows after prepare failure, got %d", logicalCount)
	}

	var chunkCount int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM chunk`).Scan(&chunkCount); err != nil {
		t.Fatalf("count chunk rows: %v", err)
	}
	if chunkCount != 0 {
		t.Fatalf("expected no chunk rows after prepare failure, got %d", chunkCount)
	}

	var fileChunkCount int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM file_chunk`).Scan(&fileChunkCount); err != nil {
		t.Fatalf("count file_chunk rows: %v", err)
	}
	if fileChunkCount != 0 {
		t.Fatalf("expected no file_chunk rows after prepare failure, got %d", fileChunkCount)
	}
}
