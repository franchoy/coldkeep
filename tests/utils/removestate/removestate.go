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

	var logicalCount int64
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM logical_file WHERE id = $1`, logicalFileID).Scan(&logicalCount); err != nil {
		t.Fatalf("count logical_file rows: %v", err)
	}
	s.LogicalFileExists = logicalCount == 1
	if s.LogicalFileExists {
		if err := dbconn.QueryRow(`SELECT status, original_name, ref_count FROM logical_file WHERE id = $1`, logicalFileID).Scan(&s.Status, &s.OriginalName, &s.RefCount); err != nil {
			t.Fatalf("read logical_file state: %v", err)
		}
	}

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
		s.PhysicalPaths = append(s.PhysicalPaths, path)
	}
	if err := physicalRows.Err(); err != nil {
		t.Fatalf("iterate physical_file paths: %v", err)
	}

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
		s.FileChunkIDs = append(s.FileChunkIDs, chunkID)
		s.ChunkLiveRefs[chunkID] = liveRefCount
		s.ChunkPinCounts[chunkID] = pinCount
	}
	if err := fileChunkRows.Err(); err != nil {
		t.Fatalf("iterate file_chunk state: %v", err)
	}

	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM snapshot_file WHERE logical_file_id = $1`, logicalFileID).Scan(&s.SnapshotRefs); err != nil {
		t.Fatalf("count snapshot_file refs: %v", err)
	}
	if err := dbconn.QueryRow(`
		SELECT COUNT(*)
		FROM blocks b
		JOIN file_chunk fc ON fc.chunk_id = b.chunk_id
		WHERE fc.logical_file_id = $1
	`, logicalFileID).Scan(&s.BlockRefs); err != nil {
		t.Fatalf("count block refs: %v", err)
	}
	if err := dbconn.QueryRow(`
		SELECT COUNT(*)
		FROM chunk_block_refs cbr
		JOIN file_chunk fc ON fc.chunk_id = cbr.chunk_id
		WHERE fc.logical_file_id = $1
	`, logicalFileID).Scan(&s.StorageBlockRefs); err != nil {
		t.Fatalf("count storage block refs: %v", err)
	}

	if containersDir != "" {
		entries, err := os.ReadDir(containersDir)
		if err != nil && !os.IsNotExist(err) {
			t.Fatalf("read containers dir: %v", err)
		}
		for _, entry := range entries {
			if entry.IsDir() {
				continue
			}
			s.ContainerFiles = append(s.ContainerFiles, filepath.Base(entry.Name()))
		}
		sort.Strings(s.ContainerFiles)
	}

	return s
}

func AssertEqual(t testing.TB, before, after Snapshot) {
	t.Helper()
	if !reflect.DeepEqual(before, after) {
		t.Fatalf("catalog state changed: before=%+v after=%+v", before, after)
	}
}
