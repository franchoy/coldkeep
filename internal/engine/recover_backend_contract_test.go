package engine_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/franchoy/coldkeep/internal/engine"
	filestate "github.com/franchoy/coldkeep/internal/status"
	"github.com/franchoy/coldkeep/internal/testutil/backendtest"
)

func TestEngineRecoverAcrossBackendsAndIsIdempotent(t *testing.T) {
	backendtest.ForEach(t, backendtest.Options{}, func(t *testing.T, backend backendtest.Backend) {
		logicalID, chunkID := seedProcessingRecoveryRows(t, backend)
		eng, err := engine.New(engine.Config{DB: backend.DB, ContainerDir: t.TempDir()})
		if err != nil {
			t.Fatalf("new engine: %v", err)
		}

		result, err := eng.Recover(context.Background(), engine.RecoverRequest{})
		if err != nil {
			t.Fatalf("Recover: %v", err)
		}
		if result.AbortedLogicalFiles != 1 || result.AbortedChunks != 1 || result.QuarantinedMissing != 0 || result.QuarantinedOrphan != 0 {
			t.Fatalf("first recovery result=%+v", result)
		}
		assertRecoveryStatuses(t, backend, logicalID, chunkID)

		again, err := eng.Recover(context.Background(), engine.RecoverRequest{})
		if err != nil {
			t.Fatalf("second Recover: %v", err)
		}
		if again.AbortedLogicalFiles != 0 || again.AbortedChunks != 0 || again.QuarantinedMissing != 0 || again.QuarantinedOrphan != 0 {
			t.Fatalf("idempotent recovery result=%+v", again)
		}
	})
}

func TestEngineRecoverFaultAndCancellationAcrossBackends(t *testing.T) {
	backendtest.ForEach(t, backendtest.Options{}, func(t *testing.T, backend backendtest.Backend) {
		cancelled, cancel := context.WithCancel(context.Background())
		cancel()
		eng, err := engine.New(engine.Config{DB: backend.DB, ContainerDir: t.TempDir()})
		if err != nil {
			t.Fatalf("new engine: %v", err)
		}
		if _, err := eng.Recover(cancelled, engine.RecoverRequest{}); !errors.Is(err, context.Canceled) || !engine.IsCode(err, engine.ErrorCancelled) {
			t.Fatalf("cancelled Recover error=%v code=%q", err, engine.CodeOf(err))
		}

		badRoot := filepath.Join(t.TempDir(), "not-a-directory")
		if err := os.WriteFile(badRoot, []byte("x"), 0o600); err != nil {
			t.Fatalf("create recovery fault fixture: %v", err)
		}
		faulty, err := engine.New(engine.Config{DB: backend.DB, ContainerDir: badRoot})
		if err != nil {
			t.Fatalf("new faulty engine: %v", err)
		}
		if _, err := faulty.Recover(context.Background(), engine.RecoverRequest{}); err == nil || !engine.IsCode(err, engine.ErrorRecoveryFailed) {
			t.Fatalf("faulty Recover error=%v code=%q", err, engine.CodeOf(err))
		}
	})
}

func seedProcessingRecoveryRows(t *testing.T, backend backendtest.Backend) (int64, int64) {
	t.Helper()
	var logicalID int64
	if err := backend.DB.QueryRow(
		`INSERT INTO logical_file (original_name, total_size, file_hash, status, ref_count, chunker_version)
		 VALUES ($1, 0, $2, $3, 0, 'v1-simple-rolling') RETURNING id`,
		"recover-processing.bin", "recover-processing-logical-hash", filestate.LogicalFileProcessing,
	).Scan(&logicalID); err != nil {
		t.Fatalf("insert processing logical file: %v", err)
	}
	var chunkID int64
	if err := backend.DB.QueryRow(
		`INSERT INTO chunk (chunk_hash, size, status, live_ref_count, pin_count, retry_count, chunker_version)
		 VALUES ($1, 1, $2, 0, 0, 0, 'v1-simple-rolling') RETURNING id`,
		"recover-processing-chunk-hash", filestate.ChunkProcessing,
	).Scan(&chunkID); err != nil {
		t.Fatalf("insert processing chunk: %v", err)
	}
	return logicalID, chunkID
}

func assertRecoveryStatuses(t *testing.T, backend backendtest.Backend, logicalID, chunkID int64) {
	t.Helper()
	var logicalStatus, chunkStatus string
	if err := backend.DB.QueryRow(`SELECT status FROM logical_file WHERE id = $1`, logicalID).Scan(&logicalStatus); err != nil {
		t.Fatalf("read logical status: %v", err)
	}
	if err := backend.DB.QueryRow(`SELECT status FROM chunk WHERE id = $1`, chunkID).Scan(&chunkStatus); err != nil {
		t.Fatalf("read chunk status: %v", err)
	}
	if logicalStatus != filestate.LogicalFileAborted || chunkStatus != filestate.ChunkAborted {
		t.Fatalf("recovered statuses=(%q,%q), want (%q,%q)", logicalStatus, chunkStatus, filestate.LogicalFileAborted, filestate.ChunkAborted)
	}
}
