package gc

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"

	"github.com/franchoy/coldkeep/internal/db"
	_ "github.com/mattn/go-sqlite3"
)

func openTestDB(t *testing.T) *sql.DB {
	t.Helper()
	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	t.Cleanup(func() { _ = dbconn.Close() })
	if err := db.RunMigrations(dbconn); err != nil {
		t.Fatalf("run migrations: %v", err)
	}
	return dbconn
}

func insertChunk(t *testing.T, dbconn *sql.DB, hash string, size, liveRef, pinCount int) int64 {
	t.Helper()
	res, err := dbconn.Exec(
		`INSERT INTO chunk (chunk_hash, size, status, live_ref_count, pin_count, chunker_version) VALUES (?, ?, 'COMPLETED', ?, ?, 'v2-fastcdc')`,
		hash, size, liveRef, pinCount,
	)
	if err != nil {
		t.Fatalf("insert chunk %q: %v", hash, err)
	}
	id, _ := res.LastInsertId()
	return id
}

func insertLogicalFile(t *testing.T, dbconn *sql.DB, name string) int64 {
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

func linkFileChunk(t *testing.T, dbconn *sql.DB, fileID, chunkID, order int64) {
	t.Helper()
	if _, err := dbconn.Exec(`INSERT INTO file_chunk (logical_file_id, chunk_id, chunk_order) VALUES (?, ?, ?)`, fileID, chunkID, order); err != nil {
		t.Fatalf("insert file_chunk: %v", err)
	}
}

func insertContainer(t *testing.T, dbconn *sql.DB, filename string, size int64) int64 {
	t.Helper()
	res, err := dbconn.Exec(
		`INSERT INTO container (filename, sealed, quarantine, current_size, max_size) VALUES (?, 1, 0, ?, 67108864)`,
		filename, size,
	)
	if err != nil {
		t.Fatalf("insert container: %v", err)
	}
	id, _ := res.LastInsertId()
	return id
}

func insertBlock(t *testing.T, dbconn *sql.DB, chunkID, containerID, storedSize int64) {
	t.Helper()
	if _, err := dbconn.Exec(
		`INSERT INTO blocks (chunk_id, codec, format_version, plaintext_size, stored_size, container_id, block_offset) VALUES (?, 'plain', 1, ?, ?, ?, 0)`,
		chunkID, storedSize, storedSize, containerID,
	); err != nil {
		t.Fatalf("insert block: %v", err)
	}
}

func TestBuildPlanNilDB(t *testing.T) {
	_, err := BuildPlan(context.Background(), nil, PlanOptions{})
	if err == nil {
		t.Fatal("expected error for nil db")
	}
}

func TestBuildPlanEmptyRepo(t *testing.T) {
	dbconn := openTestDB(t)
	plan, err := BuildPlan(context.Background(), dbconn, PlanOptions{})
	if err != nil {
		t.Fatalf("BuildPlan: %v", err)
	}
	if plan.TotalChunks != 0 {
		t.Errorf("TotalChunks = %d, want 0", plan.TotalChunks)
	}
	if plan.UnreachableChunks != 0 {
		t.Errorf("UnreachableChunks = %d, want 0", plan.UnreachableChunks)
	}
	if plan.ReclaimableBytes != 0 {
		t.Errorf("ReclaimableBytes = %d, want 0", plan.ReclaimableBytes)
	}
	if len(plan.AffectedContainers) != 0 {
		t.Errorf("AffectedContainers = %d, want 0", len(plan.AffectedContainers))
	}
}

func TestBuildPlanChunkReachableFromPhysicalFile(t *testing.T) {
	dbconn := openTestDB(t)

	fileID := insertLogicalFile(t, dbconn, "notes.txt")
	chunkID := insertChunk(t, dbconn, "c1", 100, 1, 0)
	linkFileChunk(t, dbconn, fileID, chunkID, 0)

	// Register physical_file → logical_file
	if _, err := dbconn.Exec(`INSERT INTO physical_file (path, logical_file_id) VALUES (?, ?)`, "/notes.txt", fileID); err != nil {
		t.Fatalf("insert physical_file: %v", err)
	}

	containerID := insertContainer(t, dbconn, "c001.bin", 100)
	insertBlock(t, dbconn, chunkID, containerID, 100)

	plan, err := BuildPlan(context.Background(), dbconn, PlanOptions{})
	if err != nil {
		t.Fatalf("BuildPlan: %v", err)
	}
	if plan.TotalChunks != 1 {
		t.Errorf("TotalChunks = %d, want 1", plan.TotalChunks)
	}
	if plan.ReachableChunks != 1 {
		t.Errorf("ReachableChunks = %d, want 1", plan.ReachableChunks)
	}
	if plan.UnreachableChunks != 0 {
		t.Errorf("UnreachableChunks = %d, want 0", plan.UnreachableChunks)
	}
	if len(plan.AffectedContainers) != 0 {
		t.Errorf("AffectedContainers = %d, want 0", len(plan.AffectedContainers))
	}
}

func TestBuildPlanUnreachableChunkIsReclaimable(t *testing.T) {
	dbconn := openTestDB(t)

	// Chunk exists but has no live refs and no file ownership
	chunkID := insertChunk(t, dbconn, "orphan", 512, 0, 0)
	containerID := insertContainer(t, dbconn, "c002.bin", 512)
	insertBlock(t, dbconn, chunkID, containerID, 512)

	plan, err := BuildPlan(context.Background(), dbconn, PlanOptions{})
	if err != nil {
		t.Fatalf("BuildPlan: %v", err)
	}
	if plan.TotalChunks != 1 {
		t.Errorf("TotalChunks = %d, want 1", plan.TotalChunks)
	}
	if plan.UnreachableChunks != 1 {
		t.Errorf("UnreachableChunks = %d, want 1", plan.UnreachableChunks)
	}
	if plan.ReclaimableBytes != 512 {
		t.Errorf("ReclaimableBytes = %d, want 512", plan.ReclaimableBytes)
	}
	if plan.PhysicallyReclaimableBytes != 512 {
		t.Errorf("PhysicallyReclaimableBytes = %d, want 512", plan.PhysicallyReclaimableBytes)
	}
	if len(plan.AffectedContainers) != 1 {
		t.Fatalf("AffectedContainers = %d, want 1", len(plan.AffectedContainers))
	}
	c := plan.AffectedContainers[0]
	if !c.FullyReclaimable {
		t.Error("FullyReclaimable should be true when all chunks are reclaimable")
	}
	if c.RequiresCompaction {
		t.Error("RequiresCompaction should be false when container is fully reclaimable")
	}
	if c.ReclaimableBytes != 512 {
		t.Errorf("container ReclaimableBytes = %d, want 512", c.ReclaimableBytes)
	}
}

func TestBuildPlanPinnedChunkNotReclaimable(t *testing.T) {
	dbconn := openTestDB(t)

	// Pinned chunk: unreachable from any file root but pin_count > 0
	chunkID := insertChunk(t, dbconn, "pinned", 256, 0, 1)
	containerID := insertContainer(t, dbconn, "c003.bin", 256)
	insertBlock(t, dbconn, chunkID, containerID, 256)

	plan, err := BuildPlan(context.Background(), dbconn, PlanOptions{})
	if err != nil {
		t.Fatalf("BuildPlan: %v", err)
	}
	// Pinned chunk is unreachable from file roots but NOT reclaimable
	if plan.ReclaimableBytes != 0 {
		t.Errorf("ReclaimableBytes = %d, want 0 (pinned chunk must not be reclaimed)", plan.ReclaimableBytes)
	}
	if plan.PhysicallyReclaimableBytes != 0 {
		t.Errorf("PhysicallyReclaimableBytes = %d, want 0", plan.PhysicallyReclaimableBytes)
	}
	if len(plan.AffectedContainers) != 0 {
		t.Errorf("AffectedContainers = %d, want 0", len(plan.AffectedContainers))
	}
}

func TestBuildPlanIgnoresNonCompletedChunks(t *testing.T) {
	dbconn := openTestDB(t)

	// Completed unreachable chunk should be counted/reclaimable.
	completedChunkID := insertChunk(t, dbconn, "completed-unreachable", 128, 0, 0)
	c1 := insertContainer(t, dbconn, "c-noncompleted-a.bin", 128)
	insertBlock(t, dbconn, completedChunkID, c1, 128)

	// PROCESSING and ABORTED chunks should not contribute to total/reclaimable.
	processingRes, err := dbconn.Exec(
		`INSERT INTO chunk (chunk_hash, size, status, live_ref_count, pin_count, chunker_version) VALUES (?, ?, 'PROCESSING', ?, ?, 'v2-fastcdc')`,
		"processing-chunk", 64, 0, 0,
	)
	if err != nil {
		t.Fatalf("insert processing chunk: %v", err)
	}
	processingChunkID, _ := processingRes.LastInsertId()
	c2 := insertContainer(t, dbconn, "c-noncompleted-b.bin", 64)
	insertBlock(t, dbconn, processingChunkID, c2, 64)

	abortedRes, err := dbconn.Exec(
		`INSERT INTO chunk (chunk_hash, size, status, live_ref_count, pin_count, chunker_version) VALUES (?, ?, 'ABORTED', ?, ?, 'v2-fastcdc')`,
		"aborted-chunk", 64, 0, 0,
	)
	if err != nil {
		t.Fatalf("insert aborted chunk: %v", err)
	}
	abortedChunkID, _ := abortedRes.LastInsertId()
	c3 := insertContainer(t, dbconn, "c-noncompleted-c.bin", 64)
	insertBlock(t, dbconn, abortedChunkID, c3, 64)

	plan, err := BuildPlan(context.Background(), dbconn, PlanOptions{})
	if err != nil {
		t.Fatalf("BuildPlan: %v", err)
	}

	if plan.TotalChunks != 1 {
		t.Errorf("TotalChunks = %d, want 1 (only COMPLETED)", plan.TotalChunks)
	}
	if plan.UnreachableChunks != 1 {
		t.Errorf("UnreachableChunks = %d, want 1", plan.UnreachableChunks)
	}
	if plan.ReclaimableBytes != 128 {
		t.Errorf("ReclaimableBytes = %d, want 128", plan.ReclaimableBytes)
	}
	if plan.PhysicallyReclaimableBytes != 128 {
		t.Errorf("PhysicallyReclaimableBytes = %d, want 128", plan.PhysicallyReclaimableBytes)
	}
}

func TestBuildPlanAssumeDeletedSnapshotsMakesChunkReclaimable(t *testing.T) {
	dbconn := openTestDB(t)

	fileID := insertLogicalFile(t, dbconn, "snap.txt")
	chunkID := insertChunk(t, dbconn, "snap-chunk", 1024, 0, 0)
	linkFileChunk(t, dbconn, fileID, chunkID, 0)

	// Register snapshot
	if _, err := dbconn.Exec(`INSERT INTO snapshot (id, created_at, type) VALUES ('snap-001', CURRENT_TIMESTAMP, 'full')`); err != nil {
		t.Fatalf("insert snapshot: %v", err)
	}
	// snapshot_file requires a snapshot_path row
	pathRes, err := dbconn.Exec(`INSERT INTO snapshot_path (path) VALUES ('/snap.txt')`)
	if err != nil {
		t.Fatalf("insert snapshot_path: %v", err)
	}
	pathID, _ := pathRes.LastInsertId()
	if _, err := dbconn.Exec(`INSERT INTO snapshot_file (snapshot_id, path_id, logical_file_id) VALUES ('snap-001', ?, ?)`, pathID, fileID); err != nil {
		t.Fatalf("insert snapshot_file: %v", err)
	}

	containerID := insertContainer(t, dbconn, "c004.bin", 1024)
	insertBlock(t, dbconn, chunkID, containerID, 1024)

	// Without deletion: chunk is reachable
	plan, err := BuildPlan(context.Background(), dbconn, PlanOptions{})
	if err != nil {
		t.Fatalf("BuildPlan (no deletion): %v", err)
	}
	if plan.UnreachableChunks != 0 {
		t.Errorf("without deletion: UnreachableChunks = %d, want 0", plan.UnreachableChunks)
	}

	// With assumed deletion: chunk becomes reclaimable
	plan2, err := BuildPlan(context.Background(), dbconn, PlanOptions{
		AssumeDeletedSnapshots: []string{"snap-001"},
	})
	if err != nil {
		t.Fatalf("BuildPlan (with deletion): %v", err)
	}
	if plan2.UnreachableChunks != 1 {
		t.Errorf("with deletion: UnreachableChunks = %d, want 1", plan2.UnreachableChunks)
	}
	if plan2.ReclaimableBytes != 1024 {
		t.Errorf("with deletion: ReclaimableBytes = %d, want 1024", plan2.ReclaimableBytes)
	}
	if plan2.PhysicallyReclaimableBytes != 1024 {
		t.Errorf("with deletion: PhysicallyReclaimableBytes = %d, want 1024", plan2.PhysicallyReclaimableBytes)
	}
}

func TestBuildPlanAssumeDeletedSnapshotMustExist(t *testing.T) {
	dbconn := openTestDB(t)

	_, err := BuildPlan(context.Background(), dbconn, PlanOptions{
		AssumeDeletedSnapshots: []string{"missing-snapshot"},
	})
	if err == nil {
		t.Fatal("expected error for missing snapshot")
	}
	if !strings.Contains(err.Error(), `snapshot "missing-snapshot" does not exist`) {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestBuildPlanDeleteSnapshotKeepsChunkReachableWhenCurrentFileStillExists(t *testing.T) {
	dbconn := openTestDB(t)

	fileID := insertLogicalFile(t, dbconn, "current-and-snapshot.txt")
	chunkID := insertChunk(t, dbconn, "shared-current-snapshot", 256, 0, 0)
	linkFileChunk(t, dbconn, fileID, chunkID, 0)

	if _, err := dbconn.Exec(`INSERT INTO physical_file (path, logical_file_id) VALUES (?, ?)`, "/current-and-snapshot.txt", fileID); err != nil {
		t.Fatalf("insert physical_file: %v", err)
	}
	if _, err := dbconn.Exec(`INSERT INTO snapshot (id, created_at, type) VALUES ('snap-current', CURRENT_TIMESTAMP, 'full')`); err != nil {
		t.Fatalf("insert snapshot: %v", err)
	}
	pathRes, err := dbconn.Exec(`INSERT INTO snapshot_path (path) VALUES ('/current-and-snapshot.txt')`)
	if err != nil {
		t.Fatalf("insert snapshot_path: %v", err)
	}
	pathID, _ := pathRes.LastInsertId()
	if _, err := dbconn.Exec(`INSERT INTO snapshot_file (snapshot_id, path_id, logical_file_id) VALUES ('snap-current', ?, ?)`, pathID, fileID); err != nil {
		t.Fatalf("insert snapshot_file: %v", err)
	}

	containerID := insertContainer(t, dbconn, "c-live-protected.bin", 256)
	insertBlock(t, dbconn, chunkID, containerID, 256)

	plan, err := BuildPlan(context.Background(), dbconn, PlanOptions{
		AssumeDeletedSnapshots: []string{"snap-current"},
	})
	if err != nil {
		t.Fatalf("BuildPlan: %v", err)
	}
	if plan.ReachableChunks != 1 {
		t.Errorf("ReachableChunks = %d, want 1", plan.ReachableChunks)
	}
	if plan.UnreachableChunks != 0 {
		t.Errorf("UnreachableChunks = %d, want 0", plan.UnreachableChunks)
	}
	if plan.ReclaimableBytes != 0 {
		t.Errorf("ReclaimableBytes = %d, want 0", plan.ReclaimableBytes)
	}
}

func TestBuildPlanDeleteSnapshotKeepsChunkReachableWhenOtherSnapshotStillProtectsIt(t *testing.T) {
	dbconn := openTestDB(t)

	fileID := insertLogicalFile(t, dbconn, "shared-by-two-snaps.txt")
	chunkID := insertChunk(t, dbconn, "shared-two-snaps", 300, 0, 0)
	linkFileChunk(t, dbconn, fileID, chunkID, 0)

	if _, err := dbconn.Exec(`INSERT INTO snapshot (id, created_at, type) VALUES ('snap-a', CURRENT_TIMESTAMP, 'full')`); err != nil {
		t.Fatalf("insert snap-a: %v", err)
	}
	if _, err := dbconn.Exec(`INSERT INTO snapshot (id, created_at, type) VALUES ('snap-b', CURRENT_TIMESTAMP, 'full')`); err != nil {
		t.Fatalf("insert snap-b: %v", err)
	}
	pathRes, err := dbconn.Exec(`INSERT INTO snapshot_path (path) VALUES ('/shared-by-two-snaps.txt')`)
	if err != nil {
		t.Fatalf("insert snapshot_path: %v", err)
	}
	pathID, _ := pathRes.LastInsertId()
	if _, err := dbconn.Exec(`INSERT INTO snapshot_file (snapshot_id, path_id, logical_file_id) VALUES ('snap-a', ?, ?), ('snap-b', ?, ?)`, pathID, fileID, pathID, fileID); err != nil {
		t.Fatalf("insert snapshot_file rows: %v", err)
	}

	containerID := insertContainer(t, dbconn, "c-other-snapshot.bin", 300)
	insertBlock(t, dbconn, chunkID, containerID, 300)

	plan, err := BuildPlan(context.Background(), dbconn, PlanOptions{
		AssumeDeletedSnapshots: []string{"snap-a"},
	})
	if err != nil {
		t.Fatalf("BuildPlan: %v", err)
	}
	if plan.ReachableChunks != 1 {
		t.Errorf("ReachableChunks = %d, want 1", plan.ReachableChunks)
	}
	if plan.UnreachableChunks != 0 {
		t.Errorf("UnreachableChunks = %d, want 0", plan.UnreachableChunks)
	}
	if plan.ReclaimableBytes != 0 {
		t.Errorf("ReclaimableBytes = %d, want 0", plan.ReclaimableBytes)
	}
}

func TestBuildPlanPartiallyDeadContainerDistinguishesLogicalVsPhysical(t *testing.T) {
	dbconn := openTestDB(t)

	// One dead chunk + one live chunk in same sealed container.
	deadChunkID := insertChunk(t, dbconn, "dead", 100, 0, 0)
	liveChunkID := insertChunk(t, dbconn, "live", 100, 1, 0)

	containerID := insertContainer(t, dbconn, "c-partial.bin", 200)
	insertBlock(t, dbconn, deadChunkID, containerID, 100)
	insertBlock(t, dbconn, liveChunkID, containerID, 100)

	plan, err := BuildPlan(context.Background(), dbconn, PlanOptions{})
	if err != nil {
		t.Fatalf("BuildPlan: %v", err)
	}

	if plan.ReclaimableBytes != 100 {
		t.Errorf("ReclaimableBytes = %d, want 100", plan.ReclaimableBytes)
	}
	if plan.PhysicallyReclaimableBytes != 0 {
		t.Errorf("PhysicallyReclaimableBytes = %d, want 0", plan.PhysicallyReclaimableBytes)
	}
	if plan.Summary.FullyReclaimableContainers != 0 {
		t.Errorf("FullyReclaimableContainers = %d, want 0", plan.Summary.FullyReclaimableContainers)
	}
	if plan.Summary.PartiallyDeadContainers != 1 {
		t.Errorf("PartiallyDeadContainers = %d, want 1", plan.Summary.PartiallyDeadContainers)
	}
	if len(plan.AffectedContainers) != 1 {
		t.Fatalf("AffectedContainers = %d, want 1", len(plan.AffectedContainers))
	}
	c := plan.AffectedContainers[0]
	if c.FullyReclaimable {
		t.Error("FullyReclaimable = true, want false")
	}
	if !c.RequiresCompaction {
		t.Error("RequiresCompaction = false, want true")
	}
	if c.LiveBytesAfterGC != 100 {
		t.Errorf("LiveBytesAfterGC = %d, want 100", c.LiveBytesAfterGC)
	}
}

func TestBuildPlanNoDBWritesSideEffect(t *testing.T) {
	dbconn := openTestDB(t)

	chunkID := insertChunk(t, dbconn, "ch", 64, 0, 0)
	containerID := insertContainer(t, dbconn, "c005.bin", 64)
	insertBlock(t, dbconn, chunkID, containerID, 64)

	var chunksBefore, containersBefore int64
	_ = dbconn.QueryRow(`SELECT COUNT(*) FROM chunk`).Scan(&chunksBefore)
	_ = dbconn.QueryRow(`SELECT COUNT(*) FROM container`).Scan(&containersBefore)

	if _, err := BuildPlan(context.Background(), dbconn, PlanOptions{}); err != nil {
		t.Fatalf("BuildPlan: %v", err)
	}

	var chunksAfter, containersAfter int64
	_ = dbconn.QueryRow(`SELECT COUNT(*) FROM chunk`).Scan(&chunksAfter)
	_ = dbconn.QueryRow(`SELECT COUNT(*) FROM container`).Scan(&containersAfter)

	if chunksBefore != chunksAfter {
		t.Errorf("chunk count changed: %d → %d", chunksBefore, chunksAfter)
	}
	if containersBefore != containersAfter {
		t.Errorf("container count changed: %d → %d", containersBefore, containersAfter)
	}
}

// TestReachabilityRootsDocumentation verifies the exact reachability semantics:
//   - For normal simulation: roots = physical_file + all snapshot_file
//   - For --delete-snapshot s1: roots = physical_file + snapshot_file (excluding s1)
//   - Snapshots are never deleted, only excluded from root set
func TestReachabilityRootsDocumentation(t *testing.T) {
	dbconn := openTestDB(t)

	// Setup: 2 physical files, 2 snapshots with different files
	physFile1 := insertLogicalFile(t, dbconn, "current_a.txt")
	physFile2 := insertLogicalFile(t, dbconn, "current_b.txt")
	snapFile1 := insertLogicalFile(t, dbconn, "snap1_file.txt")
	snapFile2 := insertLogicalFile(t, dbconn, "snap2_file.txt")

	chunk1 := insertChunk(t, dbconn, "phys1", 100, 0, 0) // from physFile1
	chunk2 := insertChunk(t, dbconn, "phys2", 100, 0, 0) // from physFile2
	chunk3 := insertChunk(t, dbconn, "snap1", 100, 0, 0) // from snapFile1 (via snap-001)
	chunk4 := insertChunk(t, dbconn, "snap2", 100, 0, 0) // from snapFile2 (via snap-002)

	linkFileChunk(t, dbconn, physFile1, chunk1, 0)
	linkFileChunk(t, dbconn, physFile2, chunk2, 0)
	linkFileChunk(t, dbconn, snapFile1, chunk3, 0)
	linkFileChunk(t, dbconn, snapFile2, chunk4, 0)

	// Register as physical files and snapshots
	if _, err := dbconn.Exec(`INSERT INTO physical_file (path, logical_file_id) VALUES (?, ?)`, "/current_a.txt", physFile1); err != nil {
		t.Fatalf("insert physical_file 1: %v", err)
	}
	if _, err := dbconn.Exec(`INSERT INTO physical_file (path, logical_file_id) VALUES (?, ?)`, "/current_b.txt", physFile2); err != nil {
		t.Fatalf("insert physical_file 2: %v", err)
	}

	// Create snapshots and link files
	if _, err := dbconn.Exec(`INSERT INTO snapshot (id, created_at, type) VALUES ('snap-001', CURRENT_TIMESTAMP, 'full')`); err != nil {
		t.Fatalf("insert snap-001: %v", err)
	}
	if _, err := dbconn.Exec(`INSERT INTO snapshot (id, created_at, type) VALUES ('snap-002', CURRENT_TIMESTAMP, 'full')`); err != nil {
		t.Fatalf("insert snap-002: %v", err)
	}

	pathRes1, _ := dbconn.Exec(`INSERT INTO snapshot_path (path) VALUES ('/snap1_file.txt')`)
	pathID1, _ := pathRes1.LastInsertId()
	pathRes2, _ := dbconn.Exec(`INSERT INTO snapshot_path (path) VALUES ('/snap2_file.txt')`)
	pathID2, _ := pathRes2.LastInsertId()

	if _, err := dbconn.Exec(`INSERT INTO snapshot_file (snapshot_id, path_id, logical_file_id) VALUES ('snap-001', ?, ?)`, pathID1, snapFile1); err != nil {
		t.Fatalf("insert snapshot_file snap-001: %v", err)
	}
	if _, err := dbconn.Exec(`INSERT INTO snapshot_file (snapshot_id, path_id, logical_file_id) VALUES ('snap-002', ?, ?)`, pathID2, snapFile2); err != nil {
		t.Fatalf("insert snapshot_file snap-002: %v", err)
	}

	// Place all chunks in containers
	for _, chunkID := range []int64{chunk1, chunk2, chunk3, chunk4} {
		containerID := insertContainer(t, dbconn, fmt.Sprintf("c_%d.bin", chunkID), 100)
		insertBlock(t, dbconn, chunkID, containerID, 100)
	}

	// Test 1: Normal simulation includes all files
	// Reachable: chunk1 (physFile1), chunk2 (physFile2), chunk3 (snap-001), chunk4 (snap-002)
	plan1, err := BuildPlan(context.Background(), dbconn, PlanOptions{})
	if err != nil {
		t.Fatalf("BuildPlan normal: %v", err)
	}
	if plan1.ReachableChunks != 4 {
		t.Errorf("normal: ReachableChunks = %d, want 4 (all chunks from physical + all snapshots)", plan1.ReachableChunks)
	}
	if plan1.UnreachableChunks != 0 {
		t.Errorf("normal: UnreachableChunks = %d, want 0", plan1.UnreachableChunks)
	}

	// Test 2: With --delete-snapshot snap-001 excluded
	// Reachable: chunk1 (physFile1), chunk2 (physFile2), chunk4 (snap-002)
	// Unreachable: chunk3 (snap-001 is excluded from roots, no live refs)
	plan2, err := BuildPlan(context.Background(), dbconn, PlanOptions{
		AssumeDeletedSnapshots: []string{"snap-001"},
	})
	if err != nil {
		t.Fatalf("BuildPlan with delete snap-001: %v", err)
	}
	if plan2.ReachableChunks != 3 {
		t.Errorf("with delete snap-001: ReachableChunks = %d, want 3 (physical + snap-002)", plan2.ReachableChunks)
	}
	if plan2.UnreachableChunks != 1 {
		t.Errorf("with delete snap-001: UnreachableChunks = %d, want 1 (chunk3 from snap-001)", plan2.UnreachableChunks)
	}

	// Test 3: With both snapshots excluded
	// Reachable: chunk1 (physFile1), chunk2 (physFile2)
	// Unreachable: chunk3, chunk4 (both snapshots excluded)
	plan3, err := BuildPlan(context.Background(), dbconn, PlanOptions{
		AssumeDeletedSnapshots: []string{"snap-001", "snap-002"},
	})
	if err != nil {
		t.Fatalf("BuildPlan with delete both: %v", err)
	}
	if plan3.ReachableChunks != 2 {
		t.Errorf("with delete both: ReachableChunks = %d, want 2 (only physical)", plan3.ReachableChunks)
	}
	if plan3.UnreachableChunks != 2 {
		t.Errorf("with delete both: UnreachableChunks = %d, want 2 (both snap chunks)", plan3.UnreachableChunks)
	}

	// Verify no snapshots were marked/deleted (they are only excluded from root set)
	var snapCount int64
	_ = dbconn.QueryRow(`SELECT COUNT(*) FROM snapshot WHERE id IN ('snap-001', 'snap-002')`).Scan(&snapCount)
	if snapCount != 2 {
		t.Errorf("snapshots were modified: count = %d, want 2", snapCount)
	}
}
