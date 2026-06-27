package engine_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/franchoy/coldkeep/internal/container"
	"github.com/franchoy/coldkeep/internal/engine"
	"github.com/franchoy/coldkeep/internal/storage"
)

func TestErrNotImplementedRemainsUnsupportedSentinel(t *testing.T) {
	if !errors.Is(engine.ErrNotImplemented, engine.ErrNotImplemented) {
		t.Fatal("expected ErrNotImplemented to remain errors.Is compatible with itself")
	}
	if !engine.IsUnsupported(engine.ErrNotImplemented) {
		t.Fatal("expected ErrNotImplemented to classify as unsupported")
	}
}

func TestIsUnsupportedRecognizesWrappedErrNotImplemented(t *testing.T) {
	err := fmt.Errorf("wrapped unsupported mode: %w", engine.ErrNotImplemented)
	if !errors.Is(err, engine.ErrNotImplemented) {
		t.Fatalf("expected wrapped error to remain ErrNotImplemented-compatible, got %v", err)
	}
	if !engine.IsUnsupported(err) {
		t.Fatalf("expected wrapped ErrNotImplemented to classify as unsupported, got %v", err)
	}
}

func TestIsUnsupportedRejectsUnrelatedErrors(t *testing.T) {
	for _, err := range []error{
		nil,
		errors.New("plain unrelated error"),
		fmt.Errorf("wrapped unrelated: %w", errors.New("other")),
		fmt.Errorf("engine: store source path is required"),
	} {
		if engine.IsUnsupported(err) {
			t.Fatalf("expected unrelated error to not classify as unsupported: %v", err)
		}
	}
}

func TestUnsupportedEngineModesRemainRecognizedByIsUnsupported(t *testing.T) {
	t.Run("recursive store", func(t *testing.T) {
		db := openSnapshotTestDB(t)
		sgctx := storage.StorageContext{
			DB:           db,
			Writer:       container.NewSimulatedWriter(1024 * 1024),
			ContainerDir: t.TempDir(),
		}
		eng, err := engine.New(engine.Config{DB: db, ContainerDir: sgctx.ContainerDir, StoreContext: &sgctx})
		if err != nil {
			t.Fatalf("engine.New: %v", err)
		}
		_, err = eng.Store(context.Background(), engine.StoreRequest{
			SourcePath: t.TempDir(),
			Recursive:  true,
			Workers:    2,
			Codec:      "plain",
		})
		if !engine.IsUnsupported(err) {
			t.Fatalf("expected recursive store error to classify as unsupported, got %v", err)
		}
	})
}
