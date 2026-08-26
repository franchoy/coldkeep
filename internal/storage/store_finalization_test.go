package storage

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/franchoy/coldkeep/internal/blocks"
	"github.com/franchoy/coldkeep/internal/container"
	"github.com/franchoy/coldkeep/internal/db"
	_ "github.com/mattn/go-sqlite3"
)

type singleFileFinalizationWriter struct {
	*container.SimulatedWriter
	finalizeErr error
	calls       int
}

func (w *singleFileFinalizationWriter) FinalizeContainer() error {
	w.calls++
	return w.finalizeErr
}

func newSingleFileFinalizationFixture(t *testing.T, storeErr, finalizeErr error) (StorageContext, string, *singleFileFinalizationWriter) {
	t.Helper()

	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	dbconn.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = dbconn.Close() })
	if err := db.RunMigrations(dbconn); err != nil {
		t.Fatalf("run migrations: %v", err)
	}

	writer := &singleFileFinalizationWriter{
		SimulatedWriter: container.NewSimulatedWriter(1 << 20),
		finalizeErr:     finalizeErr,
	}
	sgctx := StorageContext{DB: dbconn, Writer: writer, ContainerDir: t.TempDir()}
	if storeErr != nil {
		InstallTestStoreInterleavingHooks(&sgctx, func(context.Context, TestStoreInterleavingHookEvent) error {
			return storeErr
		})
	}

	path := filepath.Join(t.TempDir(), "single-file-finalization.txt")
	if err := os.WriteFile(path, []byte("single-file finalization regression"), 0o600); err != nil {
		t.Fatalf("write input: %v", err)
	}
	return sgctx, path, writer
}

func TestSingleFileStoreFinalizationFailureNeverReturnsSuccess(t *testing.T) {
	storeFailure := errors.New("injected store failure")
	finalizeFailure := errors.New("injected finalization failure")

	for _, tc := range []struct {
		name        string
		storeErr    error
		finalizeErr error
	}{
		{name: "store success finalize success"},
		{name: "store failure finalize success", storeErr: storeFailure},
		{name: "store success finalize failure", finalizeErr: finalizeFailure},
		{name: "store failure finalize failure", storeErr: storeFailure, finalizeErr: finalizeFailure},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sgctx, path, writer := newSingleFileFinalizationFixture(t, tc.storeErr, tc.finalizeErr)
			result, err := StoreFileWithStorageContextAndCodecResultContext(context.Background(), sgctx, path, blocks.CodecPlain)

			if writer.calls != 1 {
				t.Fatalf("FinalizeContainer calls = %d, want 1", writer.calls)
			}
			if tc.storeErr == nil && result.FileID <= 0 {
				t.Fatalf("successful store result = %+v, want positive file ID", result)
			}
			if tc.storeErr != nil && !errors.Is(err, tc.storeErr) {
				t.Fatalf("store error = %v, want store cause %v", err, tc.storeErr)
			}
			if tc.finalizeErr != nil && !errors.Is(err, tc.finalizeErr) {
				t.Fatalf("store error = %v, want finalization cause %v", err, tc.finalizeErr)
			}
			if tc.storeErr == nil && tc.finalizeErr == nil && err != nil {
				t.Fatalf("store error = %v, want nil", err)
			}
		})
	}
}

func TestSingleFileStoreFinalizesExactlyOnceAcrossCompatibilityWrappers(t *testing.T) {
	t.Setenv("COLDKEEP_CODEC", "plain")

	for _, tc := range []struct {
		name string
		run  func(context.Context, StorageContext, string) error
	}{
		{name: "default codec result", run: func(ctx context.Context, sgctx StorageContext, path string) error {
			_, err := StoreFileWithStorageContextResultContext(ctx, sgctx, path)
			return err
		}},
		{name: "explicit codec result", run: func(ctx context.Context, sgctx StorageContext, path string) error {
			_, err := StoreFileWithStorageContextAndCodecResultContext(ctx, sgctx, path, blocks.CodecPlain)
			return err
		}},
		{name: "explicit codec non-result", run: func(_ context.Context, sgctx StorageContext, path string) error {
			return StoreFileWithStorageContextAndCodec(sgctx, path, blocks.CodecPlain)
		}},
		{name: "explicit codec policy result", run: func(ctx context.Context, sgctx StorageContext, path string) error {
			_, err := StoreFileWithStorageContextAndCodecResultWithPolicyContext(ctx, sgctx, path, blocks.CodecPlain, true)
			return err
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sgctx, path, writer := newSingleFileFinalizationFixture(t, nil, nil)
			if err := tc.run(context.Background(), sgctx, path); err != nil {
				t.Fatalf("store: %v", err)
			}
			if writer.calls != 1 {
				t.Fatalf("FinalizeContainer calls = %d, want 1", writer.calls)
			}
		})
	}
}

func TestSingleFileStoreEmptyAndAlreadyStoredPathsFinalizeExactlyOnce(t *testing.T) {
	t.Run("empty file", func(t *testing.T) {
		sgctx, path, writer := newSingleFileFinalizationFixture(t, nil, nil)
		if err := os.WriteFile(path, nil, 0o600); err != nil {
			t.Fatalf("write empty input: %v", err)
		}
		if _, err := StoreFileWithStorageContextAndCodecResult(sgctx, path, blocks.CodecPlain); err != nil {
			t.Fatalf("store empty file: %v", err)
		}
		if writer.calls != 1 {
			t.Fatalf("FinalizeContainer calls = %d, want 1", writer.calls)
		}
	})

	t.Run("already stored", func(t *testing.T) {
		sgctx, path, writer := newSingleFileFinalizationFixture(t, nil, nil)
		if err := os.WriteFile(path, nil, 0o600); err != nil {
			t.Fatalf("write empty input: %v", err)
		}
		if _, err := StoreFileWithStorageContextAndCodecResult(sgctx, path, blocks.CodecPlain); err != nil {
			t.Fatalf("store first copy: %v", err)
		}
		writer.calls = 0
		result, err := StoreFileWithStorageContextAndCodecResult(sgctx, path, blocks.CodecPlain)
		if err != nil {
			t.Fatalf("store already-stored copy: %v", err)
		}
		if !result.AlreadyStored {
			t.Fatalf("AlreadyStored = false, want true")
		}
		if writer.calls != 1 {
			t.Fatalf("FinalizeContainer calls = %d, want 1", writer.calls)
		}
	})
}
