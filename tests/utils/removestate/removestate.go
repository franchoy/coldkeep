package removestate

import (
	"database/sql"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"testing"
)

type Snapshot struct {
	LogicalFileExists bool
	Status            string
	OriginalName      string
	RefCount          int64
	PhysicalPaths     []string
	FileChunkIDs      []int64
	ChunkLiveRefs     map[int64]int64
	ChunkPinCounts    map[int64]int64
	SnapshotRefs      int64
	BlockRefs         int64
	StorageBlockRefs  int64
	ContainerFiles    []string
}

func Capture(t testing.TB, dbconn *sql.DB, logicalFileID int64, containersDir string) Snapshot {
	t.Helper()

	s := Snapshot{
		ChunkLiveRefs:  make(map[int64]int64),
		ChunkPinCounts: make(map[int64]int64),
	}
	captureRemoveStateLogicalFile(t, dbconn, logicalFileID, &s)
	captureRemoveStatePhysicalPaths(t, dbconn, logicalFileID, &s)
	captureRemoveStateChunkGraph(t, dbconn, logicalFileID, &s)
	captureRemoveStateReferenceCounts(t, dbconn, logicalFileID, &s)
	captureRemoveStateContainerFiles(t, containersDir, &s)
	return s
}

func captureRemoveStateLogicalFile(t testing.TB, dbconn *sql.DB, logicalFileID int64, snapshot *Snapshot) {
	t.Helper()
	var logicalCount int64
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM logical_file WHERE id = $1`, logicalFileID).Scan(&logicalCount); err != nil {
		t.Fatalf("count logical_file rows: %v", err)
	}
	snapshot.LogicalFileExists = logicalCount == 1
	if snapshot.LogicalFileExists {
		if err := dbconn.QueryRow(`SELECT status, original_name, ref_count FROM logical_file WHERE id = $1`, logicalFileID).Scan(&snapshot.Status, &snapshot.OriginalName, &snapshot.RefCount); err != nil {
			t.Fatalf("read logical_file state: %v", err)
		}
	}
}

func captureRemoveStatePhysicalPaths(t testing.TB, dbconn *sql.DB, logicalFileID int64, snapshot *Snapshot) {
	t.Helper()
	physicalRows, err := dbconn.Query(`SELECT path FROM physical_file WHERE logical_file_id = $1 ORDER BY path ASC`, logicalFileID)
	if err != nil {
		t.Fatalf("query physical_file paths: %v", err)
	}
	defer func() { _ = physicalRows.Close() }()

	for physicalRows.Next() {
		var path string
		if err := physicalRows.Scan(&path); err != nil {
			t.Fatalf("scan physical_file path: %v", err)
		}
		snapshot.PhysicalPaths = append(snapshot.PhysicalPaths, path)
	}
	if err := physicalRows.Err(); err != nil {
		t.Fatalf("iterate physical_file paths: %v", err)
	}
}

func captureRemoveStateChunkGraph(t testing.TB, dbconn *sql.DB, logicalFileID int64, snapshot *Snapshot) {
	t.Helper()
	fileChunkRows, err := dbconn.Query(`
		SELECT fc.chunk_id, c.live_ref_count, c.pin_count
		FROM file_chunk fc
		JOIN chunk c ON c.id = fc.chunk_id
		WHERE fc.logical_file_id = $1
		ORDER BY fc.chunk_order ASC, fc.chunk_id ASC
	`, logicalFileID)
	if err != nil {
		t.Fatalf("query file_chunk state: %v", err)
	}
	defer func() { _ = fileChunkRows.Close() }()

	for fileChunkRows.Next() {
		var chunkID, liveRefCount, pinCount int64
		if err := fileChunkRows.Scan(&chunkID, &liveRefCount, &pinCount); err != nil {
			t.Fatalf("scan file_chunk state: %v", err)
		}
		snapshot.FileChunkIDs = append(snapshot.FileChunkIDs, chunkID)
		snapshot.ChunkLiveRefs[chunkID] = liveRefCount
		snapshot.ChunkPinCounts[chunkID] = pinCount
	}
	if err := fileChunkRows.Err(); err != nil {
		t.Fatalf("iterate file_chunk state: %v", err)
	}
}

func captureRemoveStateReferenceCounts(t testing.TB, dbconn *sql.DB, logicalFileID int64, snapshot *Snapshot) {
	t.Helper()
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM snapshot_file WHERE logical_file_id = $1`, logicalFileID).Scan(&snapshot.SnapshotRefs); err != nil {
		t.Fatalf("count snapshot_file refs: %v", err)
	}
	if err := dbconn.QueryRow(`
		SELECT COUNT(*)
		FROM blocks b
		JOIN file_chunk fc ON fc.chunk_id = b.chunk_id
		WHERE fc.logical_file_id = $1
	`, logicalFileID).Scan(&snapshot.BlockRefs); err != nil {
		t.Fatalf("count block refs: %v", err)
	}
	if err := dbconn.QueryRow(`
		SELECT COUNT(*)
		FROM chunk_block_refs cbr
		JOIN file_chunk fc ON fc.chunk_id = cbr.chunk_id
		WHERE fc.logical_file_id = $1
	`, logicalFileID).Scan(&snapshot.StorageBlockRefs); err != nil {
		t.Fatalf("count storage block refs: %v", err)
	}
}

func captureRemoveStateContainerFiles(t testing.TB, containersDir string, snapshot *Snapshot) {
	t.Helper()
	if containersDir == "" {
		return
	}
	entries, err := os.ReadDir(containersDir)
	if err != nil && !os.IsNotExist(err) {
		t.Fatalf("read containers dir: %v", err)
	}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		snapshot.ContainerFiles = append(snapshot.ContainerFiles, filepath.Base(entry.Name()))
	}
	sort.Strings(snapshot.ContainerFiles)
}

func AssertEqual(t testing.TB, before, after Snapshot) {
	t.Helper()
	if !reflect.DeepEqual(before, after) {
		t.Fatalf("catalog state changed: before=%+v after=%+v", before, after)
	}
}
