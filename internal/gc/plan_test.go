package gc

import (
	"context"
	"database/sql"
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
		`INSERT INTO logical_file (original_name, total_size, file_hash, status, chunker_version) VALUES (?, 100, 'h', 'COMPLETED', 'v2-fastcdc')`,
		name,
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
	if len(plan.AffectedContainers) != 1 {
		t.Fatalf("AffectedContainers = %d, want 1", len(plan.AffectedContainers))
	}
	c := plan.AffectedContainers[0]
	if c.WouldDeleteFile != true {
		t.Error("WouldDeleteFile should be true when all chunks are reclaimable")
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
	if len(plan.AffectedContainers) != 0 {
		t.Errorf("AffectedContainers = %d, want 0", len(plan.AffectedContainers))
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
