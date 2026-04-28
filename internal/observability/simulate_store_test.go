package observability

import (
	"context"
	"math"
	"reflect"
	"testing"
)

func TestSimulateStoreReportAggregatesExpectedMetrics(t *testing.T) {
	dbconn := openSimulateTestDB(t)
	svc := newServiceForTest(dbconn, nil)

	fileID := insertSimLogicalFile(t, dbconn, "sim-store-a")
	chunkID := insertSimChunk(t, dbconn, "sim-store-chunk", 60, 1, 0, "v2-fastcdc")
	containerID := insertSimContainer(t, dbconn, "sim-store.bin", 60, true, false)
	linkSimFileChunk(t, dbconn, fileID, chunkID, 0)
	insertSimBlock(t, dbconn, chunkID, containerID, 60)

	report, err := svc.SimulateStoreReport(context.Background(), "store", "/tmp/input.bin")
	if err != nil {
		t.Fatalf("SimulateStoreReport: %v", err)
	}
	if report == nil {
		t.Fatal("expected report")
	}
	if report.Subcommand != "store" {
		t.Fatalf("subcommand mismatch: got %q", report.Subcommand)
	}
	if report.Path != "/tmp/input.bin" {
		t.Fatalf("path mismatch: got %q", report.Path)
	}
	if report.Files != 1 {
		t.Fatalf("files mismatch: got %d", report.Files)
	}
	if report.Chunks != 1 {
		t.Fatalf("chunks mismatch: got %d", report.Chunks)
	}
	if report.Containers != 1 {
		t.Fatalf("containers mismatch: got %d", report.Containers)
	}
	if report.LogicalSizeBytes != 100 {
		t.Fatalf("logical size mismatch: got %d", report.LogicalSizeBytes)
	}
	if report.PhysicalSizeBytes != 60 {
		t.Fatalf("physical size mismatch: got %d", report.PhysicalSizeBytes)
	}
	if math.Abs(report.DedupRatioPct-40.0) > 0.000001 {
		t.Fatalf("dedup ratio mismatch: got %f", report.DedupRatioPct)
	}
}

func TestSimulateStoreReportDoesNotMutateState(t *testing.T) {
	dbconn := openSimulateTestDB(t)
	svc := newServiceForTest(dbconn, nil)

	fileID := insertSimLogicalFile(t, dbconn, "sim-store-b")
	chunkID := insertSimChunk(t, dbconn, "sim-store-chunk-b", 32, 1, 0, "v2-fastcdc")
	containerID := insertSimContainer(t, dbconn, "sim-store-b.bin", 32, true, false)
	linkSimFileChunk(t, dbconn, fileID, chunkID, 0)
	insertSimBlock(t, dbconn, chunkID, containerID, 32)

	before := map[string]int64{
		"logical_file": countTableRows(t, dbconn, "logical_file"),
		"chunk":        countTableRows(t, dbconn, "chunk"),
		"container":    countTableRows(t, dbconn, "container"),
		"blocks":       countTableRows(t, dbconn, "blocks"),
	}

	if _, err := dbconn.Exec(`PRAGMA query_only = ON`); err != nil {
		t.Fatalf("enable query_only: %v", err)
	}
	t.Cleanup(func() {
		_, _ = dbconn.Exec(`PRAGMA query_only = OFF`)
	})

	if _, err := svc.SimulateStoreReport(context.Background(), "store-folder", "/tmp/folder"); err != nil {
		t.Fatalf("SimulateStoreReport with query_only db: %v", err)
	}

	after := map[string]int64{
		"logical_file": countTableRows(t, dbconn, "logical_file"),
		"chunk":        countTableRows(t, dbconn, "chunk"),
		"container":    countTableRows(t, dbconn, "container"),
		"blocks":       countTableRows(t, dbconn, "blocks"),
	}
	if !reflect.DeepEqual(before, after) {
		t.Fatalf("state mutated: before=%v after=%v", before, after)
	}
}
