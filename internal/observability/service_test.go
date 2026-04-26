package observability

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/franchoy/coldkeep/internal/db"
	"github.com/franchoy/coldkeep/internal/gc"
	_ "github.com/mattn/go-sqlite3"
)

func openSimulateTestDB(t *testing.T) *sql.DB {
	t.Helper()
	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	t.Cleanup(func() { _ = dbconn.Close() })
	if err := db.RunMigrations(dbconn); err != nil {
		t.Fatalf("run migrations: %v", err)
	}
	return dbconn
}

func insertSimLogicalFile(t *testing.T, dbconn *sql.DB, name string) int64 {
	t.Helper()
	res, err := dbconn.Exec(
		`INSERT INTO logical_file (original_name, total_size, file_hash, status, chunker_version) VALUES (?, 100, ?, 'COMPLETED', 'v2-fastcdc')`,
		name, fmt.Sprintf("hash-%s", name),
	)
	if err != nil {
		t.Fatalf("insert logical_file: %v", err)
	}
	id, _ := res.LastInsertId()
	return id
}

func insertSimChunk(t *testing.T, dbconn *sql.DB, hash string, size, liveRef, pinCount int, version string) int64 {
	t.Helper()
	if version == "" {
		version = "v2-fastcdc"
	}
	res, err := dbconn.Exec(
		`INSERT INTO chunk (chunk_hash, size, status, live_ref_count, pin_count, chunker_version) VALUES (?, ?, 'COMPLETED', ?, ?, ?)`,
		hash, size, liveRef, pinCount, version,
	)
	if err != nil {
		t.Fatalf("insert chunk: %v", err)
	}
	id, _ := res.LastInsertId()
	return id
}

func linkSimFileChunk(t *testing.T, dbconn *sql.DB, fileID, chunkID, order int64) {
	t.Helper()
	if _, err := dbconn.Exec(`INSERT INTO file_chunk (logical_file_id, chunk_id, chunk_order) VALUES (?, ?, ?)`, fileID, chunkID, order); err != nil {
		t.Fatalf("insert file_chunk: %v", err)
	}
}

func insertSimContainer(t *testing.T, dbconn *sql.DB, filename string, size int64, sealed, quarantine bool) int64 {
	t.Helper()
	sealedValue := 0
	if sealed {
		sealedValue = 1
	}
	quarantineValue := 0
	if quarantine {
		quarantineValue = 1
	}
	res, err := dbconn.Exec(
		`INSERT INTO container (filename, sealed, quarantine, current_size, max_size) VALUES (?, ?, ?, ?, 67108864)`,
		filename, sealedValue, quarantineValue, size,
	)
	if err != nil {
		t.Fatalf("insert container: %v", err)
	}
	id, _ := res.LastInsertId()
	return id
}

func insertSimBlock(t *testing.T, dbconn *sql.DB, chunkID, containerID, storedSize int64) {
	t.Helper()
	if _, err := dbconn.Exec(
		`INSERT INTO blocks (chunk_id, codec, format_version, plaintext_size, stored_size, container_id, block_offset) VALUES (?, 'plain', 1, ?, ?, ?, 0)`,
		chunkID, storedSize, storedSize, containerID,
	); err != nil {
		t.Fatalf("insert block: %v", err)
	}
}

func insertSimPhysicalFile(t *testing.T, dbconn *sql.DB, path string, fileID int64) {
	t.Helper()
	if _, err := dbconn.Exec(`INSERT INTO physical_file (path, logical_file_id) VALUES (?, ?)`, path, fileID); err != nil {
		t.Fatalf("insert physical_file: %v", err)
	}
}

func insertSimSnapshot(t *testing.T, dbconn *sql.DB, snapshotID string) {
	t.Helper()
	if _, err := dbconn.Exec(`INSERT INTO snapshot (id, created_at, type) VALUES (?, CURRENT_TIMESTAMP, 'full')`, snapshotID); err != nil {
		t.Fatalf("insert snapshot: %v", err)
	}
}

func insertSimSnapshotFile(t *testing.T, dbconn *sql.DB, snapshotID, path string, fileID int64) {
	t.Helper()
	res, err := dbconn.Exec(`INSERT INTO snapshot_path (path) VALUES (?)`, path)
	if err != nil {
		t.Fatalf("insert snapshot_path: %v", err)
	}
	pathID, _ := res.LastInsertId()
	if _, err := dbconn.Exec(`INSERT INTO snapshot_file (snapshot_id, path_id, logical_file_id) VALUES (?, ?, ?)`, snapshotID, pathID, fileID); err != nil {
		t.Fatalf("insert snapshot_file: %v", err)
	}
}

func countTableRows(t *testing.T, dbconn *sql.DB, table string) int64 {
	t.Helper()
	var count int64
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM ` + table).Scan(&count); err != nil {
		t.Fatalf("count rows for %s: %v", table, err)
	}
	return count
}

func TestSimulateGCRequiresDB(t *testing.T) {
	svc := newServiceForTest(nil, nil)
	result, err := svc.Simulate(context.Background(), SimulationOptions{Kind: SimulationKindGC})
	if err == nil {
		t.Fatal("expected error")
	}
	if result != nil {
		t.Fatal("expected nil result")
	}
}

func TestSimulateGCDoesNotMutateState(t *testing.T) {
	dbconn := openSimulateTestDB(t)
	svc := newServiceForTest(dbconn, nil)

	chunkID := insertSimChunk(t, dbconn, "dead", 64, 0, 0, "v2-fastcdc")
	containerID := insertSimContainer(t, dbconn, "c-no-mutate.bin", 64, true, false)
	insertSimBlock(t, dbconn, chunkID, containerID, 64)
	insertSimSnapshot(t, dbconn, "snap-no-mutate")

	before := map[string]int64{
		"chunk":         countTableRows(t, dbconn, "chunk"),
		"container":     countTableRows(t, dbconn, "container"),
		"blocks":        countTableRows(t, dbconn, "blocks"),
		"snapshot":      countTableRows(t, dbconn, "snapshot"),
		"snapshot_file": countTableRows(t, dbconn, "snapshot_file"),
	}

	result, err := svc.Simulate(context.Background(), SimulationOptions{Kind: SimulationKindGC})
	if err != nil {
		t.Fatalf("Simulate: %v", err)
	}
	if result == nil {
		t.Fatal("expected result")
	}

	after := map[string]int64{
		"chunk":         countTableRows(t, dbconn, "chunk"),
		"container":     countTableRows(t, dbconn, "container"),
		"blocks":        countTableRows(t, dbconn, "blocks"),
		"snapshot":      countTableRows(t, dbconn, "snapshot"),
		"snapshot_file": countTableRows(t, dbconn, "snapshot_file"),
	}

	if !reflect.DeepEqual(before, after) {
		t.Fatalf("state mutated: before=%v after=%v", before, after)
	}
	if result.Mutated {
		t.Fatal("expected Mutated=false")
	}
}

func TestSimulateGCZeroDBWritesOnQueryOnlyConnection(t *testing.T) {
	dbconn := openSimulateTestDB(t)
	svc := newServiceForTest(dbconn, nil)

	chunkID := insertSimChunk(t, dbconn, "dead-query-only", 64, 0, 0, "v2-fastcdc")
	containerID := insertSimContainer(t, dbconn, "c-query-only.bin", 64, true, false)
	insertSimBlock(t, dbconn, chunkID, containerID, 64)

	if _, err := dbconn.Exec(`PRAGMA query_only = ON`); err != nil {
		t.Fatalf("enable query_only: %v", err)
	}
	t.Cleanup(func() {
		_, _ = dbconn.Exec(`PRAGMA query_only = OFF`)
	})

	result, err := svc.Simulate(context.Background(), SimulationOptions{Kind: SimulationKindGC})
	if err != nil {
		t.Fatalf("Simulate with query_only db: %v", err)
	}
	if result == nil || result.GC == nil {
		t.Fatal("expected gc result")
	}
}

func TestSimulateGCMatchesGCPlan(t *testing.T) {
	dbconn := openSimulateTestDB(t)
	svc := newServiceForTest(dbconn, nil)

	deadChunkID := insertSimChunk(t, dbconn, "dead-plan", 90, 0, 0, "v2-fastcdc")
	deadContainerID := insertSimContainer(t, dbconn, "c-plan.bin", 90, true, false)
	insertSimBlock(t, dbconn, deadChunkID, deadContainerID, 90)

	plan, err := gc.BuildPlan(context.Background(), dbconn, gc.PlanOptions{})
	if err != nil {
		t.Fatalf("BuildPlan: %v", err)
	}
	result, err := svc.Simulate(context.Background(), SimulationOptions{Kind: SimulationKindGC})
	if err != nil {
		t.Fatalf("Simulate: %v", err)
	}
	if result == nil || result.GC == nil {
		t.Fatal("expected gc result")
	}

	if result.GC.Summary.ReachableChunks != plan.ReachableChunks {
		t.Fatalf("ReachableChunks mismatch: simulate=%d plan=%d", result.GC.Summary.ReachableChunks, plan.ReachableChunks)
	}
	if result.GC.Summary.UnreachableChunks != plan.UnreachableChunks {
		t.Fatalf("UnreachableChunks mismatch: simulate=%d plan=%d", result.GC.Summary.UnreachableChunks, plan.UnreachableChunks)
	}
	if result.GC.Summary.LogicallyReclaimableBytes != plan.Summary.LogicallyReclaimableBytes {
		t.Fatalf("Logical bytes mismatch: simulate=%d plan=%d", result.GC.Summary.LogicallyReclaimableBytes, plan.Summary.LogicallyReclaimableBytes)
	}
	if result.GC.Summary.PhysicallyReclaimableBytes != plan.Summary.PhysicallyReclaimableBytes {
		t.Fatalf("Physical bytes mismatch: simulate=%d plan=%d", result.GC.Summary.PhysicallyReclaimableBytes, plan.Summary.PhysicallyReclaimableBytes)
	}
	if len(result.GC.Containers) != len(plan.AffectedContainers) {
		t.Fatalf("container count mismatch: simulate=%d plan=%d", len(result.GC.Containers), len(plan.AffectedContainers))
	}
}

func TestSimulateGCSnapshotDeletionKeepsOtherSnapshots(t *testing.T) {
	dbconn := openSimulateTestDB(t)
	svc := newServiceForTest(dbconn, nil)

	fileID := insertSimLogicalFile(t, dbconn, "shared.txt")
	chunkID := insertSimChunk(t, dbconn, "shared-snap", 100, 0, 0, "v2-fastcdc")
	linkSimFileChunk(t, dbconn, fileID, chunkID, 0)
	insertSimSnapshot(t, dbconn, "snap-a")
	insertSimSnapshot(t, dbconn, "snap-b")
	insertSimSnapshotFile(t, dbconn, "snap-a", "/shared-a.txt", fileID)
	insertSimSnapshotFile(t, dbconn, "snap-b", "/shared-b.txt", fileID)
	containerID := insertSimContainer(t, dbconn, "c-other-snapshot.bin", 100, true, false)
	insertSimBlock(t, dbconn, chunkID, containerID, 100)

	result, err := svc.Simulate(context.Background(), SimulationOptions{Kind: SimulationKindGC, AssumeDeletedSnapshots: []string{"snap-a"}})
	if err != nil {
		t.Fatalf("Simulate: %v", err)
	}
	if result.GC.Summary.ReachableChunks != 1 || result.GC.Summary.UnreachableChunks != 0 {
		t.Fatalf("expected other snapshot to keep chunk reachable, got summary=%+v", result.GC.Summary)
	}
}

func TestSimulateGCSnapshotDeletionKeepsCurrentFiles(t *testing.T) {
	dbconn := openSimulateTestDB(t)
	svc := newServiceForTest(dbconn, nil)

	fileID := insertSimLogicalFile(t, dbconn, "current.txt")
	chunkID := insertSimChunk(t, dbconn, "shared-current", 100, 0, 0, "v2-fastcdc")
	linkSimFileChunk(t, dbconn, fileID, chunkID, 0)
	insertSimPhysicalFile(t, dbconn, "/current.txt", fileID)
	insertSimSnapshot(t, dbconn, "snap-current")
	insertSimSnapshotFile(t, dbconn, "snap-current", "/current.txt", fileID)
	containerID := insertSimContainer(t, dbconn, "c-current.bin", 100, true, false)
	insertSimBlock(t, dbconn, chunkID, containerID, 100)

	result, err := svc.Simulate(context.Background(), SimulationOptions{Kind: SimulationKindGC, AssumeDeletedSnapshots: []string{"snap-current"}})
	if err != nil {
		t.Fatalf("Simulate: %v", err)
	}
	if result.GC.Summary.ReachableChunks != 1 || result.GC.Summary.UnreachableChunks != 0 {
		t.Fatalf("expected current file to keep chunk reachable, got summary=%+v", result.GC.Summary)
	}
}

func TestSimulateGCReportsLogicalVsPhysicalReclaim(t *testing.T) {
	dbconn := openSimulateTestDB(t)
	svc := newServiceForTest(dbconn, nil)

	deadChunkID := insertSimChunk(t, dbconn, "dead", 100, 0, 0, "v2-fastcdc")
	liveChunkID := insertSimChunk(t, dbconn, "live", 100, 1, 0, "v2-fastcdc")
	containerID := insertSimContainer(t, dbconn, "c-partial.bin", 200, true, false)
	insertSimBlock(t, dbconn, deadChunkID, containerID, 100)
	insertSimBlock(t, dbconn, liveChunkID, containerID, 100)

	result, err := svc.Simulate(context.Background(), SimulationOptions{Kind: SimulationKindGC})
	if err != nil {
		t.Fatalf("Simulate: %v", err)
	}
	if result.GC.Summary.LogicallyReclaimableBytes != 100 {
		t.Fatalf("logical bytes = %d, want 100", result.GC.Summary.LogicallyReclaimableBytes)
	}
	if result.GC.Summary.PhysicallyReclaimableBytes != 0 {
		t.Fatalf("physical bytes = %d, want 0", result.GC.Summary.PhysicallyReclaimableBytes)
	}
}

func TestSimulateGCContainerDetailsAreSorted(t *testing.T) {
	dbconn := openSimulateTestDB(t)
	svc := newServiceForTest(dbconn, nil)

	chunkA := insertSimChunk(t, dbconn, "dead-a", 10, 0, 0, "v2-fastcdc")
	chunkB := insertSimChunk(t, dbconn, "dead-b", 10, 0, 0, "v2-fastcdc")
	containerB := insertSimContainer(t, dbconn, "b.bin", 10, true, false)
	containerA := insertSimContainer(t, dbconn, "a.bin", 10, true, false)
	insertSimBlock(t, dbconn, chunkA, containerB, 10)
	insertSimBlock(t, dbconn, chunkB, containerA, 10)

	result, err := svc.Simulate(context.Background(), SimulationOptions{Kind: SimulationKindGC})
	if err != nil {
		t.Fatalf("Simulate: %v", err)
	}
	if len(result.GC.Containers) != 2 {
		t.Fatalf("expected 2 containers, got %d", len(result.GC.Containers))
	}
	ids := []int64{result.GC.Containers[0].ContainerID, result.GC.Containers[1].ContainerID}
	sorted := append([]int64(nil), ids...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	if !reflect.DeepEqual(ids, sorted) {
		t.Fatalf("containers not sorted by id: got=%v want=%v", ids, sorted)
	}
}

func TestSimulateGCJSONRoundTrip(t *testing.T) {
	dbconn := openSimulateTestDB(t)
	svc := newServiceForTest(dbconn, nil)

	chunkID := insertSimChunk(t, dbconn, "dead-json", 64, 0, 0, "v2-fastcdc")
	containerID := insertSimContainer(t, dbconn, "c-json.bin", 64, true, false)
	insertSimBlock(t, dbconn, chunkID, containerID, 64)
	insertSimSnapshot(t, dbconn, "s-json")

	result, err := svc.Simulate(context.Background(), SimulationOptions{Kind: SimulationKindGC})
	if err != nil {
		t.Fatalf("Simulate: %v", err)
	}
	encoded, err := json.Marshal(result)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	var decoded SimulationResult
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if decoded.GC == nil {
		t.Fatal("expected decoded gc result")
	}
	if decoded.GC.Kind != SimulationKindGC || !decoded.GC.Exact || decoded.GC.Mutated {
		t.Fatalf("unexpected decoded gc metadata: %+v", decoded.GC)
	}
	if _, hasGeneratedAt := any(decoded).(SimulationResult).Summary["generated_at_utc"]; hasGeneratedAt {
		t.Fatal("simulation summary should not carry generated_at_utc")
	}
}

func TestSimulateGCDeterministicAcrossCalls(t *testing.T) {
	dbconn := openSimulateTestDB(t)

	deadChunkID := insertSimChunk(t, dbconn, "dead-deterministic", 90, 0, 0, "v2-fastcdc")
	deadContainerID := insertSimContainer(t, dbconn, "c-deterministic.bin", 90, true, false)
	insertSimBlock(t, dbconn, deadChunkID, deadContainerID, 90)

	serviceA := newServiceForTest(dbconn, func() time.Time {
		return time.Date(2026, time.April, 26, 10, 0, 0, 0, time.UTC)
	})
	serviceB := newServiceForTest(dbconn, func() time.Time {
		return time.Date(2026, time.April, 26, 10, 5, 0, 0, time.UTC)
	})

	resultA, err := serviceA.Simulate(context.Background(), SimulationOptions{Kind: SimulationKindGC})
	if err != nil {
		t.Fatalf("first Simulate: %v", err)
	}
	resultB, err := serviceB.Simulate(context.Background(), SimulationOptions{Kind: SimulationKindGC})
	if err != nil {
		t.Fatalf("second Simulate: %v", err)
	}

	encodedA, err := json.Marshal(resultA)
	if err != nil {
		t.Fatalf("Marshal first result: %v", err)
	}
	encodedB, err := json.Marshal(resultB)
	if err != nil {
		t.Fatalf("Marshal second result: %v", err)
	}
	if string(encodedA) != string(encodedB) {
		t.Fatalf("simulation output not deterministic:\nfirst=%s\nsecond=%s", string(encodedA), string(encodedB))
	}
}

func TestSimulateGCEmptyRepo(t *testing.T) {
	dbconn := openSimulateTestDB(t)
	svc := newServiceForTest(dbconn, nil)

	result, err := svc.Simulate(context.Background(), SimulationOptions{Kind: SimulationKindGC})
	if err != nil {
		t.Fatalf("Simulate: %v", err)
	}
	if result == nil || result.GC == nil {
		t.Fatal("expected non-nil result with GC field")
	}
	if result.GC.Kind != SimulationKindGC {
		t.Errorf("Kind = %q, want %q", result.GC.Kind, SimulationKindGC)
	}
	if !result.GC.Exact {
		t.Error("expected GC.Exact=true")
	}
	if result.GC.Mutated {
		t.Error("expected GC.Mutated=false")
	}
	if len(result.GC.Assumptions.DeletedSnapshots) != 0 {
		t.Errorf("DeletedSnapshots = %v, want empty", result.GC.Assumptions.DeletedSnapshots)
	}
	if result.GC.Summary.ReachableChunks != 0 {
		t.Errorf("ReachableChunks = %d, want 0", result.GC.Summary.ReachableChunks)
	}
	if result.GC.Summary.UnreachableChunks != 0 {
		t.Errorf("UnreachableChunks = %d, want 0", result.GC.Summary.UnreachableChunks)
	}
	if result.GC.Summary.LogicallyReclaimableBytes != 0 {
		t.Errorf("LogicallyReclaimableBytes = %d, want 0", result.GC.Summary.LogicallyReclaimableBytes)
	}
	if result.GC.Summary.PhysicallyReclaimableBytes != 0 {
		t.Errorf("PhysicallyReclaimableBytes = %d, want 0", result.GC.Summary.PhysicallyReclaimableBytes)
	}
	if len(result.GC.Containers) != 0 {
		t.Errorf("Containers = %d, want 0", len(result.GC.Containers))
	}
	if !result.Exact {
		t.Error("expected Exact=true")
	}
	if result.Mutated {
		t.Error("expected Mutated=false")
	}
}

func TestSimulateGCAssumptionsIncludeDeletedSnapshots(t *testing.T) {
	dbconn := openSimulateTestDB(t)
	svc := newServiceForTest(dbconn, nil)
	if _, err := dbconn.Exec(`INSERT INTO snapshot (id, created_at, type) VALUES ('s1', CURRENT_TIMESTAMP, 'full'), ('s2', CURRENT_TIMESTAMP, 'full')`); err != nil {
		t.Fatalf("insert snapshots: %v", err)
	}

	result, err := svc.Simulate(context.Background(), SimulationOptions{
		Kind:                   SimulationKindGC,
		AssumeDeletedSnapshots: []string{"s1", "s2"},
	})
	if err != nil {
		t.Fatalf("Simulate: %v", err)
	}
	if result == nil || result.GC == nil {
		t.Fatal("expected non-nil result with GC field")
	}
	if len(result.GC.Assumptions.DeletedSnapshots) != 2 {
		t.Fatalf("DeletedSnapshots length = %d, want 2", len(result.GC.Assumptions.DeletedSnapshots))
	}
	if result.GC.Assumptions.DeletedSnapshots[0] != "s1" || result.GC.Assumptions.DeletedSnapshots[1] != "s2" {
		t.Fatalf("DeletedSnapshots = %v, want [s1 s2]", result.GC.Assumptions.DeletedSnapshots)
	}
	if !result.GC.Exact {
		t.Error("expected GC.Exact=true")
	}
	if result.GC.Mutated {
		t.Error("expected GC.Mutated=false")
	}
}

func TestSimulateGCDeleteSnapshotAssumptionDoesNotDeleteSnapshotMetadata(t *testing.T) {
	dbconn := openSimulateTestDB(t)
	svc := newServiceForTest(dbconn, nil)

	fileID := insertSimLogicalFile(t, dbconn, "snapshot-retained.txt")
	chunkID := insertSimChunk(t, dbconn, "snapshot-retained-chunk", 100, 0, 0, "v2-fastcdc")
	linkSimFileChunk(t, dbconn, fileID, chunkID, 0)
	insertSimSnapshot(t, dbconn, "snap-meta")
	insertSimSnapshotFile(t, dbconn, "snap-meta", "/snapshot-retained.txt", fileID)

	beforeSnapshots := countTableRows(t, dbconn, "snapshot")
	beforeSnapshotFiles := countTableRows(t, dbconn, "snapshot_file")

	result, err := svc.Simulate(context.Background(), SimulationOptions{Kind: SimulationKindGC, AssumeDeletedSnapshots: []string{"snap-meta"}})
	if err != nil {
		t.Fatalf("Simulate: %v", err)
	}
	if result == nil || result.GC == nil {
		t.Fatal("expected gc result")
	}
	if got := countTableRows(t, dbconn, "snapshot"); got != beforeSnapshots {
		t.Fatalf("snapshot row count changed: got=%d want=%d", got, beforeSnapshots)
	}
	if got := countTableRows(t, dbconn, "snapshot_file"); got != beforeSnapshotFiles {
		t.Fatalf("snapshot_file row count changed: got=%d want=%d", got, beforeSnapshotFiles)
	}
}

func TestSimulateGCRejectsMissingDeletedSnapshot(t *testing.T) {
	dbconn := openSimulateTestDB(t)
	svc := newServiceForTest(dbconn, nil)

	result, err := svc.Simulate(context.Background(), SimulationOptions{
		Kind:                   SimulationKindGC,
		AssumeDeletedSnapshots: []string{"missing-snapshot"},
	})
	if err == nil {
		t.Fatal("expected error")
	}
	if result != nil {
		t.Fatal("expected nil result")
	}
	if !strings.Contains(err.Error(), `snapshot "missing-snapshot" does not exist`) {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestSimulateGCRejectsMissingSnapshot(t *testing.T) {
	TestSimulateGCRejectsMissingDeletedSnapshot(t)
}

func TestSimulateGCWithNoUnreachableChunks(t *testing.T) {
	dbconn := openSimulateTestDB(t)
	svc := newServiceForTest(dbconn, nil)

	fileID := insertSimLogicalFile(t, dbconn, "live-only.txt")
	chunkID := insertSimChunk(t, dbconn, "live-only", 88, 1, 0, "v2-fastcdc")
	linkSimFileChunk(t, dbconn, fileID, chunkID, 0)
	insertSimPhysicalFile(t, dbconn, "/live-only.txt", fileID)
	containerID := insertSimContainer(t, dbconn, "c-live-only.bin", 88, true, false)
	insertSimBlock(t, dbconn, chunkID, containerID, 88)

	result, err := svc.Simulate(context.Background(), SimulationOptions{Kind: SimulationKindGC})
	if err != nil {
		t.Fatalf("Simulate: %v", err)
	}
	if result.GC.Summary.UnreachableChunks != 0 || result.GC.Summary.LogicallyReclaimableBytes != 0 || result.GC.Summary.PhysicallyReclaimableBytes != 0 {
		t.Fatalf("expected no reclaimable data, got summary=%+v", result.GC.Summary)
	}
	if len(result.GC.Containers) != 0 {
		t.Fatalf("expected no affected containers, got %d", len(result.GC.Containers))
	}
}

func TestSimulateGCWithQuarantinedContainerWarning(t *testing.T) {
	dbconn := openSimulateTestDB(t)
	svc := newServiceForTest(dbconn, nil)

	chunkID := insertSimChunk(t, dbconn, "quarantine-warning", 70, 0, 0, "v2-fastcdc")
	containerID := insertSimContainer(t, dbconn, "q.bin", 70, true, true)
	insertSimBlock(t, dbconn, chunkID, containerID, 70)

	result, err := svc.Simulate(context.Background(), SimulationOptions{Kind: SimulationKindGC})
	if err != nil {
		t.Fatalf("Simulate: %v", err)
	}
	found := false
	for _, warning := range result.GC.Warnings {
		if warning.Code == "QUARANTINED_CONTAINER" {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected QUARANTINED_CONTAINER warning, got %v", result.GC.Warnings)
	}
}

func TestSimulateGCIncludesWarnings(t *testing.T) {
	dbconn := openSimulateTestDB(t)
	svc := newServiceForTest(dbconn, nil)

	if _, err := dbconn.Exec(
		`INSERT INTO chunk (chunk_hash, size, status, live_ref_count, pin_count, chunker_version) VALUES (?, ?, 'COMPLETED', ?, ?, ?)`,
		"orphan-placement", 50, 0, 0, "v9-future-cdc",
	); err != nil {
		t.Fatalf("insert placementless chunk: %v", err)
	}

	result, err := svc.Simulate(context.Background(), SimulationOptions{Kind: SimulationKindGC})
	if err != nil {
		t.Fatalf("Simulate: %v", err)
	}
	if result == nil || result.GC == nil {
		t.Fatal("expected non-nil result with GC field")
	}
	if len(result.GC.Warnings) == 0 {
		t.Fatal("expected nested GC warnings")
	}
	if len(result.Warnings) == 0 {
		t.Fatal("expected top-level warnings")
	}
	if result.GC.Warnings[0].Code == "" || result.Warnings[0].Code == "" {
		t.Fatalf("expected warning codes, got gc=%v top=%v", result.GC.Warnings, result.Warnings)
	}
}

func TestSimulateRejectsUnsupportedKind(t *testing.T) {
	svc := newServiceForTest(nil, nil)

	_, err := svc.Simulate(context.Background(), SimulationOptions{Kind: "store"})
	if err == nil {
		t.Fatal("expected error")
	}
	if !errors.Is(err, ErrInvalidTarget) {
		t.Fatalf("expected ErrInvalidTarget, got %v", err)
	}
	if !strings.Contains(err.Error(), "unsupported simulation kind") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestNewServiceRequiresNonNilDB(t *testing.T) {
	_, err := NewService(nil)
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestNewServiceRejectsNilDB(t *testing.T) {
	_, err := NewService(nil)
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestNewServiceWithDB(t *testing.T) {
	dbconn := &sql.DB{}
	svc, err := NewService(dbconn)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	if svc == nil || svc.db != dbconn {
		t.Fatal("expected service with injected db")
	}
}
