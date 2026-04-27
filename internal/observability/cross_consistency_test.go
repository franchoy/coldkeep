package observability

// cross_consistency_test.go validates that the observability commands agree
// with each other when run against the same repository state.
//
//  1. Stats ↔ Inspect:  stats.total_chunks ≤ sum of chunks across containers
//  2. Inspect bidirectional: file→chunk forward relation exists in chunk←file reverse
//  3. Inspect inspectability: every chunk relation from a logical-file can be inspected
//  4. Simulation ↔ GC plan: simulate gc reports exactly the same numbers as gc.BuildPlan

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/franchoy/coldkeep/internal/gc"
)

// buildCrossConsistencyDB creates a shared in-memory DB with:
//   - 2 logical files, each referencing some chunks
//   - 1 dead chunk (no live logical file references it)
//   - 2 containers, one fully dead (only dead chunks), one live
//   - 1 snapshot covering the live file
func buildCrossConsistencyDB(t *testing.T) (dbconn interface{ Close() error }, fileID, deadChunkID, liveChunkID, liveContainerID, deadContainerID int64) {
	t.Helper()
	conn := openSimulateTestDB(t)

	// Live logical file
	liveFileID := insertSimLogicalFile(t, conn, "live.txt")

	// Live chunk (referenced by live file)
	lc := insertSimChunk(t, conn, "live-chunk-hash", 64, 1, 0, "v2-fastcdc")
	linkSimFileChunk(t, conn, liveFileID, lc, 0)

	// Dead chunk (not referenced by any live file)
	dc := insertSimChunk(t, conn, "dead-chunk-hash", 90, 0, 0, "v2-fastcdc")

	// Containers
	lctr := insertSimContainer(t, conn, "live-ctr.ck", 128, true, false)
	dctr := insertSimContainer(t, conn, "dead-ctr.ck", 90, true, false)

	// Blocks
	insertSimBlock(t, conn, lc, lctr, 64)
	insertSimBlock(t, conn, dc, dctr, 90)

	// Snapshot referencing the live file
	insertSimSnapshot(t, conn, "snap-1")
	insertSimSnapshotFile(t, conn, "snap-1", "live.txt", liveFileID)

	return conn, liveFileID, dc, lc, lctr, dctr
}

// TestCrossConsistency_StatsAndInspectAgreeOnChunkCount verifies that
// stats.total_chunks equals what enumerate-by-container reveals via inspect.
func TestCrossConsistency_StatsAndInspectAgreeOnChunkCount(t *testing.T) {
	conn := openSimulateTestDB(t)

	fileID := insertSimLogicalFile(t, conn, "alpha.txt")
	for i := 0; i < 3; i++ {
		hash := fmt.Sprintf("chunk-alpha-%d", i)
		cid := insertSimChunk(t, conn, hash, 64, 1, 0, "v2-fastcdc")
		linkSimFileChunk(t, conn, fileID, cid, int64(i))
		ctr := insertSimContainer(t, conn, fmt.Sprintf("ctr-%d.ck", i), 64, true, false)
		insertSimBlock(t, conn, cid, ctr, 64)
	}

	fixedNow := time.Date(2026, time.April, 27, 10, 0, 0, 0, time.UTC)
	svc := newServiceForTest(conn, func() time.Time { return fixedNow })

	stats, err := svc.Stats(context.Background(), StatsOptions{IncludeContainers: true})
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}

	// Inspect the logical file and verify chunk_count matches what stats reports
	entityID := strconv.FormatInt(fileID, 10)
	inspectResult, err := svc.Inspect(context.Background(), EntityFile, entityID, InspectOptions{Relations: true})
	if err != nil {
		t.Fatalf("Inspect logical file: %v", err)
	}

	chunkRelations := 0
	for _, rel := range inspectResult.Relations {
		if rel.TargetType == EntityChunk && rel.Direction == RelationOutgoing {
			chunkRelations++
		}
	}

	// The file has 3 chunks; stats must also count exactly 3 total chunks.
	if got := stats.Chunks.TotalChunks; got != 3 {
		t.Fatalf("stats.total_chunks=%d, want 3", got)
	}
	if chunkRelations != 3 {
		t.Fatalf("inspect chunk relations=%d, want 3", chunkRelations)
	}
	if int64(chunkRelations) != stats.Chunks.TotalChunks {
		t.Fatalf("inspect chunk relations (%d) != stats.total_chunks (%d)", chunkRelations, stats.Chunks.TotalChunks)
	}
}

// TestCrossConsistency_InspectBidirectional verifies that if logical-file X
// has a forward relation to chunk Y, then chunk Y has a reverse relation back
// to logical-file X.
func TestCrossConsistency_InspectBidirectional(t *testing.T) {
	conn := openSimulateTestDB(t)

	fileID := insertSimLogicalFile(t, conn, "bi.txt")
	chunkID := insertSimChunk(t, conn, "bi-chunk", 64, 1, 0, "v2-fastcdc")
	linkSimFileChunk(t, conn, fileID, chunkID, 0)
	ctr := insertSimContainer(t, conn, "bi-ctr.ck", 64, true, false)
	insertSimBlock(t, conn, chunkID, ctr, 64)

	fixedNow := time.Date(2026, time.April, 27, 10, 0, 0, 0, time.UTC)
	svc := newServiceForTest(conn, func() time.Time { return fixedNow })

	// Forward: logical-file → chunk
	fileResult, err := svc.Inspect(context.Background(), EntityFile, strconv.FormatInt(fileID, 10), InspectOptions{Relations: true})
	if err != nil {
		t.Fatalf("Inspect logical file: %v", err)
	}

	var forwardChunkIDs []string
	for _, rel := range fileResult.Relations {
		if rel.TargetType == EntityChunk && rel.Direction == RelationOutgoing {
			forwardChunkIDs = append(forwardChunkIDs, rel.TargetID)
		}
	}
	if len(forwardChunkIDs) == 0 {
		t.Fatalf("expected forward chunk relations from logical-file %d, got none", fileID)
	}

	// Reverse: each chunk must reference back to the logical-file
	fileEntityID := strconv.FormatInt(fileID, 10)
	for _, cid := range forwardChunkIDs {
		chunkResult, err := svc.Inspect(context.Background(), EntityChunk, cid, InspectOptions{Reverse: true})
		if err != nil {
			t.Fatalf("Inspect chunk %s: %v", cid, err)
		}

		found := false
		for _, rel := range chunkResult.Relations {
			if rel.TargetType == EntityLogicalFile && rel.Direction == RelationIncoming && rel.TargetID == fileEntityID {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("chunk %s has no reverse relation to logical-file %s; got relations: %+v", cid, fileEntityID, chunkResult.Relations)
		}
	}
}

// TestCrossConsistency_InspectRelationsAreInspectable verifies that every
// chunk relation emitted by inspect-logical-file can itself be inspected.
func TestCrossConsistency_InspectRelationsAreInspectable(t *testing.T) {
	conn := openSimulateTestDB(t)

	fileID := insertSimLogicalFile(t, conn, "inspectable.txt")
	for i := 0; i < 2; i++ {
		cid := insertSimChunk(t, conn, fmt.Sprintf("inspectable-chunk-%d", i), 64, 1, 0, "v2-fastcdc")
		linkSimFileChunk(t, conn, fileID, cid, int64(i))
		ctr := insertSimContainer(t, conn, fmt.Sprintf("inspectable-ctr-%d.ck", i), 64, true, false)
		insertSimBlock(t, conn, cid, ctr, 64)
	}

	fixedNow := time.Date(2026, time.April, 27, 10, 0, 0, 0, time.UTC)
	svc := newServiceForTest(conn, func() time.Time { return fixedNow })

	fileResult, err := svc.Inspect(context.Background(), EntityFile, strconv.FormatInt(fileID, 10), InspectOptions{Relations: true})
	if err != nil {
		t.Fatalf("Inspect logical file: %v", err)
	}

	for _, rel := range fileResult.Relations {
		if rel.TargetType != EntityChunk || rel.Direction != RelationOutgoing {
			continue
		}
		_, err := svc.Inspect(context.Background(), EntityChunk, rel.TargetID, InspectOptions{})
		if err != nil {
			t.Fatalf("chunk relation %s from logical-file %d cannot be inspected: %v", rel.TargetID, fileID, err)
		}
	}
}

// TestCrossConsistency_SimulateGCMatchesGCPlanExactly verifies that
// simulate gc reports exactly the same numbers as a direct gc.BuildPlan call
// on the same database connection.
func TestCrossConsistency_SimulateGCMatchesGCPlanExactly(t *testing.T) {
	conn := openSimulateTestDB(t)

	// Live chunk (referenced)
	liveFileID := insertSimLogicalFile(t, conn, "live2.txt")
	lc1 := insertSimChunk(t, conn, "cross-live-1", 100, 1, 0, "v2-fastcdc")
	lc2 := insertSimChunk(t, conn, "cross-live-2", 100, 1, 0, "v2-fastcdc")
	linkSimFileChunk(t, conn, liveFileID, lc1, 0)
	linkSimFileChunk(t, conn, liveFileID, lc2, 1)

	// Dead chunks (unreachable)
	dc1 := insertSimChunk(t, conn, "cross-dead-1", 80, 0, 0, "v2-fastcdc")
	dc2 := insertSimChunk(t, conn, "cross-dead-2", 60, 0, 0, "v2-fastcdc")

	// Containers
	liveCtr := insertSimContainer(t, conn, "cross-live.ck", 200, true, false)
	deadCtr := insertSimContainer(t, conn, "cross-dead.ck", 140, true, false)

	insertSimBlock(t, conn, lc1, liveCtr, 100)
	insertSimBlock(t, conn, lc2, liveCtr, 100)
	insertSimBlock(t, conn, dc1, deadCtr, 80)
	insertSimBlock(t, conn, dc2, deadCtr, 60)

	insertSimSnapshot(t, conn, "snap-cross")
	insertSimSnapshotFile(t, conn, "snap-cross", "live2.txt", liveFileID)

	fixedNow := time.Date(2026, time.April, 27, 10, 0, 0, 0, time.UTC)
	svc := newServiceForTest(conn, func() time.Time { return fixedNow })

	// Simulate GC via the observability service
	simResult, err := svc.Simulate(context.Background(), SimulationOptions{Kind: SimulationKindGC})
	if err != nil {
		t.Fatalf("Simulate: %v", err)
	}
	gcSim := simResult.GC
	if gcSim == nil {
		t.Fatal("expected GC simulation result, got nil")
	}

	// Direct GC plan
	plan, err := gc.BuildPlan(context.Background(), conn, gc.PlanOptions{})
	if err != nil {
		t.Fatalf("gc.BuildPlan: %v", err)
	}

	if gcSim.Summary.UnreachableChunks != plan.Summary.UnreachableChunks {
		t.Fatalf("unreachable_chunks: simulate=%d gc_plan=%d", gcSim.Summary.UnreachableChunks, plan.Summary.UnreachableChunks)
	}
	if gcSim.Summary.LogicallyReclaimableBytes != plan.Summary.LogicallyReclaimableBytes {
		t.Fatalf("logically_reclaimable_bytes: simulate=%d gc_plan=%d", gcSim.Summary.LogicallyReclaimableBytes, plan.Summary.LogicallyReclaimableBytes)
	}
	if gcSim.Summary.PhysicallyReclaimableBytes != plan.Summary.PhysicallyReclaimableBytes {
		t.Fatalf("physically_reclaimable_bytes: simulate=%d gc_plan=%d", gcSim.Summary.PhysicallyReclaimableBytes, plan.Summary.PhysicallyReclaimableBytes)
	}
	if gcSim.Summary.FullyReclaimableContainers != plan.Summary.FullyReclaimableContainers {
		t.Fatalf("fully_reclaimable_containers: simulate=%d gc_plan=%d", gcSim.Summary.FullyReclaimableContainers, plan.Summary.FullyReclaimableContainers)
	}
	if gcSim.Summary.PartiallyDeadContainers != plan.Summary.PartiallyDeadContainers {
		t.Fatalf("partially_dead_containers: simulate=%d gc_plan=%d", gcSim.Summary.PartiallyDeadContainers, plan.Summary.PartiallyDeadContainers)
	}

	// Reachable + unreachable must sum to total chunks in the plan
	if plan.ReachableChunks+plan.Summary.UnreachableChunks != plan.TotalChunks {
		t.Fatalf("plan invariant violated: reachable(%d) + unreachable(%d) != total(%d)",
			plan.ReachableChunks, plan.Summary.UnreachableChunks, plan.TotalChunks)
	}
}

// TestCrossConsistency_SimulateGCContainersMatchGCPlan verifies that the set
// of containers reported by simulate gc --containers matches the set in
// gc.BuildPlan exactly (same IDs, same reclaimable/total bytes).
func TestCrossConsistency_SimulateGCContainersMatchGCPlan(t *testing.T) {
	conn := openSimulateTestDB(t)

	liveFileID := insertSimLogicalFile(t, conn, "ctr-match.txt")
	lc := insertSimChunk(t, conn, "ctr-live-chunk", 64, 1, 0, "v2-fastcdc")
	dc := insertSimChunk(t, conn, "ctr-dead-chunk", 48, 0, 0, "v2-fastcdc")
	linkSimFileChunk(t, conn, liveFileID, lc, 0)

	liveCtr := insertSimContainer(t, conn, "ctr-live2.ck", 64, true, false)
	deadCtr := insertSimContainer(t, conn, "ctr-dead2.ck", 48, true, false)
	insertSimBlock(t, conn, lc, liveCtr, 64)
	insertSimBlock(t, conn, dc, deadCtr, 48)

	insertSimSnapshot(t, conn, "snap-ctr")
	insertSimSnapshotFile(t, conn, "snap-ctr", "ctr-match.txt", liveFileID)

	fixedNow := time.Date(2026, time.April, 27, 10, 0, 0, 0, time.UTC)
	svc := newServiceForTest(conn, func() time.Time { return fixedNow })

	simResult, err := svc.Simulate(context.Background(), SimulationOptions{Kind: SimulationKindGC})
	if err != nil {
		t.Fatalf("Simulate: %v", err)
	}
	gcSim := simResult.GC
	if gcSim == nil {
		t.Fatal("expected GC simulation result, got nil")
	}

	plan, err := gc.BuildPlan(context.Background(), conn, gc.PlanOptions{})
	if err != nil {
		t.Fatalf("gc.BuildPlan: %v", err)
	}

	if len(gcSim.Containers) != len(plan.AffectedContainers) {
		t.Fatalf("container count mismatch: simulate=%d gc_plan=%d", len(gcSim.Containers), len(plan.AffectedContainers))
	}

	// Build lookup from plan by container ID
	planByID := make(map[int64]gc.ContainerImpact)
	for _, c := range plan.AffectedContainers {
		planByID[c.ContainerID] = c
	}

	for _, sc := range gcSim.Containers {
		pc, ok := planByID[sc.ContainerID]
		if !ok {
			t.Fatalf("simulate gc reports container %d but gc.BuildPlan does not", sc.ContainerID)
		}
		if sc.ReclaimableBytes != pc.ReclaimableBytes {
			t.Fatalf("container %d: simulate reclaimable_bytes=%d gc_plan=%d", sc.ContainerID, sc.ReclaimableBytes, pc.ReclaimableBytes)
		}
		if sc.FullyReclaimable != pc.FullyReclaimable {
			t.Fatalf("container %d: simulate fully_reclaimable=%v gc_plan=%v", sc.ContainerID, sc.FullyReclaimable, pc.FullyReclaimable)
		}
	}
}
