package engine

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/franchoy/coldkeep/internal/container"
	"github.com/franchoy/coldkeep/internal/storage"
)

type engineFinalizationWriter struct {
	*container.SimulatedWriter
	finalizeErr error
	calls       int
}

func (w *engineFinalizationWriter) FinalizeContainer() error {
	w.calls++
	return w.finalizeErr
}

func TestEngineStoreFinalizationFailureHasCodecParity(t *testing.T) {
	finalizeFailure := errors.New("injected engine finalization failure")
	storeFailure := errors.New("injected engine store failure")

	for _, codec := range []struct {
		name    string
		request string
	}{
		{name: "default codec"},
		{name: "explicit codec", request: "plain"},
	} {
		for _, storeCase := range []struct {
			name string
			err  error
		}{
			{name: "store success"},
			{name: "store failure", err: storeFailure},
		} {
			t.Run(codec.name+"/"+storeCase.name, func(t *testing.T) {
				t.Setenv("COLDKEEP_CODEC", "plain")
				dbconn := newEngineTestDB(t)
				writer := &engineFinalizationWriter{
					SimulatedWriter: container.NewSimulatedWriter(1 << 20),
					finalizeErr:     finalizeFailure,
				}
				sgctx := storage.StorageContext{DB: dbconn, Writer: writer, ContainerDir: t.TempDir()}
				if storeCase.err != nil {
					storage.InstallTestStoreInterleavingHooks(&sgctx, func(context.Context, storage.TestStoreInterleavingHookEvent) error {
						return storeCase.err
					})
				}
				eng, err := New(Config{DB: dbconn, ContainerDir: sgctx.ContainerDir, StoreContext: &sgctx})
				if err != nil {
					t.Fatalf("New: %v", err)
				}
				input := filepath.Join(t.TempDir(), "engine-store-finalization.txt")
				if err := os.WriteFile(input, []byte("engine finalization regression"), 0o600); err != nil {
					t.Fatalf("write input: %v", err)
				}

				result, err := eng.Store(context.Background(), StoreRequest{SourcePath: input, Codec: codec.request})
				if result != (StoreResult{}) {
					t.Fatalf("Store result = %+v, want zero", result)
				}
				if !errors.Is(err, finalizeFailure) {
					t.Fatalf("Store error = %v, want finalization cause", err)
				}
				if !IsCode(err, ErrorOperationFailed) {
					t.Fatalf("Store error code = %q, want %q", CodeOf(err), ErrorOperationFailed)
				}
				if storeCase.err != nil && !errors.Is(err, storeCase.err) {
					t.Fatalf("Store error = %v, want store cause", err)
				}
				if writer.calls != 1 {
					t.Fatalf("FinalizeContainer calls = %d, want 1", writer.calls)
				}
			})
		}
	}
}

func TestStoreFinalizationFailurePreservesCancellationAndDeadline(t *testing.T) {
	finalizeFailure := errors.New("injected context finalization failure")
	for _, cause := range []error{context.Canceled, context.DeadlineExceeded} {
		t.Run(cause.Error(), func(t *testing.T) {
			dbconn := newEngineTestDB(t)
			writer := &engineFinalizationWriter{
				SimulatedWriter: container.NewSimulatedWriter(1 << 20),
				finalizeErr:     finalizeFailure,
			}
			sgctx := storage.StorageContext{DB: dbconn, Writer: writer, ContainerDir: t.TempDir()}
			var ctx context.Context
			if errors.Is(cause, context.DeadlineExceeded) {
				deadlineCtx, cancel := context.WithDeadline(context.Background(), time.Now().Add(time.Hour))
				t.Cleanup(cancel)
				ctx = deadlineCtx
				storage.InstallTestStoreInterleavingHooks(&sgctx, func(context.Context, storage.TestStoreInterleavingHookEvent) error {
					return context.DeadlineExceeded
				})
			} else {
				cancelCtx, cancel := context.WithCancel(context.Background())
				t.Cleanup(cancel)
				ctx = cancelCtx
				storage.InstallTestStoreInterleavingHooks(&sgctx, func(context.Context, storage.TestStoreInterleavingHookEvent) error {
					cancel()
					return context.Canceled
				})
			}
			eng, err := New(Config{DB: dbconn, ContainerDir: sgctx.ContainerDir, StoreContext: &sgctx})
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			input := filepath.Join(t.TempDir(), "engine-store-context-finalization.txt")
			if err := os.WriteFile(input, []byte("context finalization regression"), 0o600); err != nil {
				t.Fatalf("write input: %v", err)
			}

			result, err := eng.Store(ctx, StoreRequest{SourcePath: input, Codec: "plain"})
			if result != (StoreResult{}) || !errors.Is(err, cause) || !errors.Is(err, finalizeFailure) || !IsCode(err, ErrorCancelled) {
				t.Fatalf("Store result=%+v error=%v, want zero result and joined %v/finalization failure", result, err, cause)
			}
			if writer.calls != 1 {
				t.Fatalf("FinalizeContainer calls = %d, want 1", writer.calls)
			}
		})
	}
}
