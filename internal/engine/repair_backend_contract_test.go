package engine_test

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/franchoy/coldkeep/internal/engine"
	filestate "github.com/franchoy/coldkeep/internal/status"
	"github.com/franchoy/coldkeep/internal/testutil/backendtest"
)

func TestEngineRepairAcrossBackends(t *testing.T) {
	backendtest.ForEach(t, backendtest.Options{}, func(t *testing.T, backend backendtest.Backend) {
		logicalID, chunkID := seedRepairMismatches(t, backend)
		eng, err := engine.New(engine.Config{DB: backend.DB, ContainerDir: t.TempDir()})
		if err != nil {
			t.Fatalf("new engine: %v", err)
		}

		result, err := eng.Repair(context.Background(), engine.RepairRequest{Targets: []string{
			" ref-counts ", "chunk-live-ref-counts", "ref-counts",
		}})
		if err != nil {
			t.Fatalf("Repair: %v", err)
		}
		if got, want := repairStatuses(result.Targets), []engine.BatchItemStatus{engine.BatchItemOK, engine.BatchItemOK, engine.BatchItemSkipped}; !reflect.DeepEqual(got, want) {
			t.Fatalf("target statuses=%v want %v; result=%+v", got, want, result)
		}
		if result.Summary.OK != 2 || result.Summary.Skipped != 1 || result.Summary.Failed != 0 {
			t.Fatalf("summary=%+v", result.Summary)
		}
		if result.Targets[0].RawTarget != "ref-counts" || result.Targets[0].ScannedRows != 1 || result.Targets[0].UpdatedRows != 1 || result.Targets[0].OrphanRows != 0 {
			t.Fatalf("logical result=%+v", result.Targets[0])
		}
		if result.Targets[1].ScannedRows != 1 || result.Targets[1].UpdatedRows != 1 {
			t.Fatalf("chunk result=%+v", result.Targets[1])
		}
		var logicalRefs, chunkRefs int64
		if err := backend.DB.QueryRow(`SELECT ref_count FROM logical_file WHERE id = $1`, logicalID).Scan(&logicalRefs); err != nil {
			t.Fatalf("read logical ref_count: %v", err)
		}
		if err := backend.DB.QueryRow(`SELECT live_ref_count FROM chunk WHERE id = $1`, chunkID).Scan(&chunkRefs); err != nil {
			t.Fatalf("read chunk live_ref_count: %v", err)
		}
		if logicalRefs != 1 || chunkRefs != 1 {
			t.Fatalf("repaired counts=(%d,%d), want (1,1)", logicalRefs, chunkRefs)
		}
	})
}

func TestEngineRepairValidationOrderingAcrossBackends(t *testing.T) {
	backendtest.ForEach(t, backendtest.Options{}, func(t *testing.T, backend backendtest.Backend) {
		eng, err := engine.New(engine.Config{DB: backend.DB, ContainerDir: t.TempDir()})
		if err != nil {
			t.Fatalf("new engine: %v", err)
		}
		result, err := eng.Repair(context.Background(), engine.RepairRequest{Targets: []string{" ", "unknown", "ref-counts", " ref-counts "}, FailFast: true})
		if err != nil {
			t.Fatalf("Repair validation batch: %v", err)
		}
		if got, want := repairStatuses(result.Targets), []engine.BatchItemStatus{engine.BatchItemFailed, engine.BatchItemFailed, engine.BatchItemOK, engine.BatchItemSkipped}; !reflect.DeepEqual(got, want) {
			t.Fatalf("ordered statuses=%v want %v; result=%+v", got, want, result)
		}
		if result.Summary.Failed != 2 || result.Summary.OK != 1 || result.Summary.Skipped != 1 {
			t.Fatalf("summary=%+v", result.Summary)
		}
		if !strings.Contains(result.Targets[0].Message, "invalid repair target") || !strings.Contains(result.Targets[1].Message, "unknown repair target") {
			t.Fatalf("validation messages=%q, %q", result.Targets[0].Message, result.Targets[1].Message)
		}
	})
}

func TestEngineRepairTypedBoundaryErrors(t *testing.T) {
	backendtest.ForEach(t, backendtest.Options{}, func(t *testing.T, backend backendtest.Backend) {
		eng, err := engine.New(engine.Config{DB: backend.DB, ContainerDir: t.TempDir()})
		if err != nil {
			t.Fatalf("new engine: %v", err)
		}
		if _, err := eng.Repair(context.Background(), engine.RepairRequest{}); !engine.IsCode(err, engine.ErrorInvalidArgument) {
			t.Fatalf("empty request error=%v code=%q", err, engine.CodeOf(err))
		}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if _, err := eng.Repair(ctx, engine.RepairRequest{Targets: []string{"ref-counts"}}); !errors.Is(err, context.Canceled) || !engine.IsCode(err, engine.ErrorCancelled) {
			t.Fatalf("cancelled repair error=%v code=%q", err, engine.CodeOf(err))
		}
	})
}

func TestEngineRepairFailFastOnInvariantViolation(t *testing.T) {
	backendtest.ForEach(t, backendtest.Options{}, func(t *testing.T, backend backendtest.Backend) {
		logicalID, _ := seedRepairMismatches(t, backend)
		if backend.Name == "sqlite" {
			if _, err := backend.DB.Exec(`PRAGMA foreign_keys = OFF`); err != nil {
				t.Fatalf("disable foreign keys: %v", err)
			}
		} else {
			if _, err := backend.DB.Exec(`ALTER TABLE physical_file DISABLE TRIGGER ALL`); err != nil {
				t.Fatalf("disable scratch fixture physical_file triggers: %v", err)
			}
		}
		if _, err := backend.DB.Exec(`INSERT INTO physical_file (path, logical_file_id, is_metadata_complete) VALUES ($1, $2, false)`, "/engine/orphan.bin", int64(999999)); err != nil {
			t.Fatalf("insert orphan physical file: %v", err)
		}
		if backend.Name != "sqlite" {
			if _, err := backend.DB.Exec(`ALTER TABLE physical_file ENABLE TRIGGER ALL`); err != nil {
				t.Fatalf("re-enable scratch fixture physical_file triggers: %v", err)
			}
		}
		eng, err := engine.New(engine.Config{DB: backend.DB, ContainerDir: t.TempDir()})
		if err != nil {
			t.Fatalf("new engine: %v", err)
		}
		result, err := eng.Repair(context.Background(), engine.RepairRequest{
			Targets: []string{"ref-counts", "chunk-live-ref-counts"}, FailFast: true,
		})
		if err != nil {
			t.Fatalf("Repair aggregate: %v", err)
		}
		if len(result.Targets) != 1 || result.Targets[0].Status != engine.BatchItemFailed || result.Summary.Failed != 1 {
			t.Fatalf("fail-fast result=%+v", result)
		}
		if result.Targets[0].InvariantCode == "" || result.Targets[0].RecommendedAction == "" {
			t.Fatalf("missing invariant classification: %+v", result.Targets[0])
		}
		var logicalRefs int64
		if err := backend.DB.QueryRow(`SELECT ref_count FROM logical_file WHERE id = $1`, logicalID).Scan(&logicalRefs); err != nil {
			t.Fatalf("read logical ref_count: %v", err)
		}
		if logicalRefs != 9 {
			t.Fatalf("failed repair changed logical ref_count to %d, want original 9", logicalRefs)
		}
	})
}

func seedRepairMismatches(t *testing.T, backend backendtest.Backend) (int64, int64) {
	t.Helper()
	var logicalID int64
	if err := backend.DB.QueryRow(
		`INSERT INTO logical_file (original_name, total_size, file_hash, status, ref_count, chunker_version)
		 VALUES ($1, $2, $3, $4, $5, 'v1-simple-rolling') RETURNING id`,
		"engine-repair.bin", int64(11), strings.Repeat("a", 64), filestate.LogicalFileCompleted, int64(9),
	).Scan(&logicalID); err != nil {
		t.Fatalf("insert logical file: %v", err)
	}
	if _, err := backend.DB.Exec(`INSERT INTO physical_file (path, logical_file_id, is_metadata_complete) VALUES ($1, $2, false)`, "/engine/repair.bin", logicalID); err != nil {
		t.Fatalf("insert physical file: %v", err)
	}
	var chunkID int64
	if err := backend.DB.QueryRow(
		`INSERT INTO chunk (chunk_hash, size, status, live_ref_count, pin_count, retry_count, chunker_version)
		 VALUES ($1, $2, $3, $4, $5, $6, $7) RETURNING id`,
		strings.Repeat("b", 64), int64(11), filestate.ChunkCompleted, int64(7), int64(0), int64(0), "v1-simple-rolling",
	).Scan(&chunkID); err != nil {
		t.Fatalf("insert chunk: %v", err)
	}
	if _, err := backend.DB.Exec(`INSERT INTO file_chunk (logical_file_id, chunk_id, chunk_order) VALUES ($1, $2, 0)`, logicalID, chunkID); err != nil {
		t.Fatalf("insert file chunk: %v", err)
	}
	return logicalID, chunkID
}

func repairStatuses(items []engine.RepairTargetResult) []engine.BatchItemStatus {
	statuses := make([]engine.BatchItemStatus, len(items))
	for i := range items {
		statuses[i] = items[i].Status
	}
	return statuses
}
