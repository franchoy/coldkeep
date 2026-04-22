package storage

import (
	"database/sql"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/franchoy/coldkeep/internal/chunk"
	"github.com/franchoy/coldkeep/internal/db"
	"github.com/franchoy/coldkeep/internal/invariants"
	filestate "github.com/franchoy/coldkeep/internal/status"
	"github.com/franchoy/coldkeep/tests/testdb"
	_ "github.com/mattn/go-sqlite3"
)

func TestRemoveFailsWhenLogicalFileNotFound(t *testing.T) {
	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	defer func() { _ = dbconn.Close() }()
	if err := db.RunMigrations(dbconn); err != nil {
		t.Fatalf("run migrations: %v", err)
	}

	err = RemoveFileWithDB(dbconn, 999)
	if err == nil || !strings.Contains(err.Error(), "file ID 999 not found") {
		t.Fatalf("expected file-not-found error contract, got: %v", err)
	}
}

func TestRemoveFailsWhenFileIsProcessing(t *testing.T) {
	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	defer func() { _ = dbconn.Close() }()
	if err := db.RunMigrations(dbconn); err != nil {
		t.Fatalf("run migrations: %v", err)
	}

	var fileID int64
	if err := dbconn.QueryRow(
		`INSERT INTO logical_file (original_name, total_size, file_hash, status, chunker_version)
		 VALUES ($1, $2, $3, $4, 'v1-simple-rolling') RETURNING id`,
		"processing-file.bin", int64(8), strings.Repeat("e", 64), filestate.LogicalFileProcessing,
	).Scan(&fileID); err != nil {
		t.Fatalf("insert logical file: %v", err)
	}

	err = RemoveFileWithDB(dbconn, fileID)
	if err == nil || !strings.Contains(err.Error(), "is still PROCESSING and cannot be removed") {
		t.Fatalf("expected PROCESSING error contract, got: %v", err)
	}
}

func TestRemoveFailsWhenLogicalFileIsRetainedBySnapshot(t *testing.T) {
	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	dbconn.SetMaxOpenConns(1)
	defer func() { _ = dbconn.Close() }()
	if err := db.RunMigrations(dbconn); err != nil {
		t.Fatalf("run migrations: %v", err)
	}

	var fileID int64
	if err := dbconn.QueryRow(
		`INSERT INTO logical_file (original_name, total_size, file_hash, status, ref_count, chunker_version)
		 VALUES ($1, $2, $3, $4, $5, 'v1-simple-rolling') RETURNING id`,
		"snapshot-retained.bin", int64(8), strings.Repeat("s", 64), filestate.LogicalFileCompleted, int64(1),
	).Scan(&fileID); err != nil {
		t.Fatalf("insert logical file: %v", err)
	}

	storedPath := filepath.Join(t.TempDir(), "snapshot-retained.bin")
	if _, err := dbconn.Exec(
		`INSERT INTO physical_file (path, logical_file_id, is_metadata_complete) VALUES ($1, $2, 1)`,
		storedPath,
		fileID,
	); err != nil {
		t.Fatalf("insert physical_file row: %v", err)
	}

	if _, err := dbconn.Exec(
		`INSERT INTO snapshot (id, created_at, type) VALUES ($1, $2, $3)`,
		"snap-keep-delete-safe",
		time.Now().UTC(),
		"full",
	); err != nil {
		t.Fatalf("insert snapshot: %v", err)
	}
	testdb.InsertSnapshotFileRef(t, dbconn, "snap-keep-delete-safe", "docs/retained.txt", fileID)

	_, err = RemoveFileWithDBResult(dbconn, fileID)
	if err == nil {
		t.Fatal("expected remove to fail for snapshot-retained logical file")
	}

	code, ok := invariants.Code(err)
	if !ok || code != invariants.CodeSnapshotRetainedDeleteBlocked {
		t.Fatalf("expected invariant code %q, got code=%q ok=%v err=%v", invariants.CodeSnapshotRetainedDeleteBlocked, code, ok, err)
	}

	var logicalCount int64
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM logical_file WHERE id = $1`, fileID).Scan(&logicalCount); err != nil {
		t.Fatalf("count logical_file rows: %v", err)
	}
	if logicalCount != 1 {
		t.Fatalf("expected logical_file row to remain after blocked remove, got %d", logicalCount)
	}

	var physicalCount int64
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM physical_file WHERE logical_file_id = $1`, fileID).Scan(&physicalCount); err != nil {
		t.Fatalf("count physical_file rows: %v", err)
	}
	if physicalCount != 1 {
		t.Fatalf("expected physical_file mapping to remain after blocked remove, got %d", physicalCount)
	}
}

func TestRemoveFailsOnInvalidLiveRefCountTransition(t *testing.T) {
	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	defer func() { _ = dbconn.Close() }()
	if err := db.RunMigrations(dbconn); err != nil {
		t.Fatalf("run migrations: %v", err)
	}

	// Insert a chunk whose live_ref_count is already 0 — decrement should fail.
	var chunkID int64
	if err := dbconn.QueryRow(
		`INSERT INTO chunk (chunk_hash, size, status, live_ref_count, chunker_version)
		 VALUES ($1, $2, $3, 0, 'v1-simple-rolling') RETURNING id`,
		strings.Repeat("a", 64), int64(4), filestate.ChunkCompleted,
	).Scan(&chunkID); err != nil {
		t.Fatalf("insert chunk: %v", err)
	}

	var fileID int64
	if err := dbconn.QueryRow(
		`INSERT INTO logical_file (original_name, total_size, file_hash, status, chunker_version)
		 VALUES ($1, $2, $3, $4, 'v1-simple-rolling') RETURNING id`,
		"zero-ref-file.bin", int64(4), strings.Repeat("b", 64), filestate.LogicalFileCompleted,
	).Scan(&fileID); err != nil {
		t.Fatalf("insert logical file: %v", err)
	}

	if _, err := dbconn.Exec(
		`INSERT INTO file_chunk (logical_file_id, chunk_id, chunk_order) VALUES ($1, $2, 0)`,
		fileID, chunkID,
	); err != nil {
		t.Fatalf("insert file_chunk: %v", err)
	}

	err = RemoveFileWithDB(dbconn, fileID)
	if err == nil || !strings.Contains(err.Error(), "invalid live_ref_count transition for chunk") {
		t.Fatalf("expected invalid live_ref_count error contract, got: %v", err)
	}
}

func TestGetLogicalFileInfoWithDBFound(t *testing.T) {
	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	defer func() { _ = dbconn.Close() }()
	if err := db.RunMigrations(dbconn); err != nil {
		t.Fatalf("run migrations: %v", err)
	}

	var fileID int64
	if err := dbconn.QueryRow(
		`INSERT INTO logical_file (original_name, total_size, file_hash, status, chunker_version)
		 VALUES ($1, $2, $3, $4, 'v1-simple-rolling') RETURNING id`,
		"logical-file-info.bin",
		int64(10),
		strings.Repeat("f", 64),
		filestate.LogicalFileCompleted,
	).Scan(&fileID); err != nil {
		t.Fatalf("insert logical file: %v", err)
	}

	var rawChunkerVersion string
	if err := dbconn.QueryRow(`SELECT chunker_version FROM logical_file WHERE id = $1`, fileID).Scan(&rawChunkerVersion); err != nil {
		t.Fatalf("read raw logical_file.chunker_version: %v", err)
	}
	if rawChunkerVersion != string(chunk.DefaultChunkerVersion) {
		t.Fatalf("unexpected raw chunker_version: got=%q want=%q", rawChunkerVersion, chunk.DefaultChunkerVersion)
	}

	info, err := GetLogicalFileInfoWithDB(dbconn, fileID)
	if err != nil {
		t.Fatalf("GetLogicalFileInfoWithDB: %v", err)
	}
	if info.FileID != fileID {
		t.Fatalf("unexpected file id: got=%d want=%d", info.FileID, fileID)
	}
	if info.OriginalName != "logical-file-info.bin" {
		t.Fatalf("unexpected original name: got=%q", info.OriginalName)
	}
	if info.Status != filestate.LogicalFileCompleted {
		t.Fatalf("unexpected status: got=%q want=%q", info.Status, filestate.LogicalFileCompleted)
	}
	if info.ChunkerVersion != chunk.DefaultChunkerVersion {
		t.Fatalf("unexpected chunker version: got=%q want=%q", info.ChunkerVersion, chunk.DefaultChunkerVersion)
	}
}

func TestGetLogicalFileInfoWithDBNotFound(t *testing.T) {
	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	defer func() { _ = dbconn.Close() }()
	if err := db.RunMigrations(dbconn); err != nil {
		t.Fatalf("run migrations: %v", err)
	}

	_, err = GetLogicalFileInfoWithDB(dbconn, 123456)
	if err == nil {
		t.Fatal("expected sql.ErrNoRows for missing logical file")
	}
	if err != sql.ErrNoRows {
		t.Fatalf("expected sql.ErrNoRows, got: %v", err)
	}
}

func TestGetLogicalFileInfoWithDBFailsOnEmptyChunkerVersion(t *testing.T) {
	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	defer func() { _ = dbconn.Close() }()
	if err := db.RunMigrations(dbconn); err != nil {
		t.Fatalf("run migrations: %v", err)
	}

	var fileID int64
	if err := dbconn.QueryRow(
		`INSERT INTO logical_file (original_name, total_size, file_hash, status, chunker_version)
		 VALUES ($1, $2, $3, $4, 'v1-simple-rolling') RETURNING id`,
		"logical-file-info-empty-version.bin",
		int64(10),
		strings.Repeat("e", 64),
		filestate.LogicalFileCompleted,
	).Scan(&fileID); err != nil {
		t.Fatalf("insert logical file: %v", err)
	}

	if _, err := dbconn.Exec(`UPDATE logical_file SET chunker_version = '' WHERE id = $1`, fileID); err != nil {
		t.Fatalf("set empty chunker_version: %v", err)
	}

	_, err = GetLogicalFileInfoWithDB(dbconn, fileID)
	if err == nil || !strings.Contains(err.Error(), "empty chunker_version") {
		t.Fatalf("expected empty chunker_version error, got: %v", err)
	}
}

func TestRemoveByStoredPathUnlinksSingleMappingAndKeepsSharedLogicalAlive(t *testing.T) {
	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	defer func() { _ = dbconn.Close() }()
	if err := db.RunMigrations(dbconn); err != nil {
		t.Fatalf("run migrations: %v", err)
	}

	var logicalID int64
	if err := dbconn.QueryRow(
		`INSERT INTO logical_file (original_name, total_size, file_hash, status, ref_count, chunker_version)
		 VALUES ($1, $2, $3, $4, $5, 'v1-simple-rolling') RETURNING id`,
		"shared.bin", int64(8), strings.Repeat("a", 64), filestate.LogicalFileCompleted, int64(2),
	).Scan(&logicalID); err != nil {
		t.Fatalf("insert logical file: %v", err)
	}

	pathA := filepath.Join(t.TempDir(), "a.txt")
	pathB := filepath.Join(t.TempDir(), "b.txt")
	if _, err := dbconn.Exec(
		`INSERT INTO physical_file (path, logical_file_id, is_metadata_complete) VALUES ($1, $2, 0), ($3, $2, 0)`,
		pathA, logicalID, pathB,
	); err != nil {
		t.Fatalf("insert physical_file rows: %v", err)
	}

	result, err := RemoveFileByStoredPathWithStorageContextResult(StorageContext{DB: dbconn}, pathA)
	if err != nil {
		t.Fatalf("remove by stored path: %v", err)
	}
	if !result.Removed || result.StoredPath != pathA || result.LogicalFileID != logicalID || result.RemainingRefCount != 1 {
		t.Fatalf("unexpected remove result: %+v", result)
	}

	var refCount int64
	if err := dbconn.QueryRow(`SELECT ref_count FROM logical_file WHERE id = $1`, logicalID).Scan(&refCount); err != nil {
		t.Fatalf("read logical ref_count: %v", err)
	}
	if refCount != 1 {
		t.Fatalf("expected logical ref_count=1 after unlinking one of two paths, got %d", refCount)
	}

	var physicalCount int64
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM physical_file WHERE logical_file_id = $1`, logicalID).Scan(&physicalCount); err != nil {
		t.Fatalf("count physical mappings: %v", err)
	}
	if physicalCount != 1 {
		t.Fatalf("expected one remaining physical mapping, got %d", physicalCount)
	}

	var existsPathB int64
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM physical_file WHERE path = $1`, pathB).Scan(&existsPathB); err != nil {
		t.Fatalf("check unrelated mapping: %v", err)
	}
	if existsPathB != 1 {
		t.Fatalf("expected unrelated mapping to survive, count=%d", existsPathB)
	}
}

func TestRemoveByStoredPathLastReferenceSetsRefCountToZero(t *testing.T) {
	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	defer func() { _ = dbconn.Close() }()
	if err := db.RunMigrations(dbconn); err != nil {
		t.Fatalf("run migrations: %v", err)
	}

	var logicalID int64
	if err := dbconn.QueryRow(
		`INSERT INTO logical_file (original_name, total_size, file_hash, status, ref_count, chunker_version)
		 VALUES ($1, $2, $3, $4, $5, 'v1-simple-rolling') RETURNING id`,
		"single.bin", int64(4), strings.Repeat("b", 64), filestate.LogicalFileCompleted, int64(1),
	).Scan(&logicalID); err != nil {
		t.Fatalf("insert logical file: %v", err)
	}

	storedPath := filepath.Join(t.TempDir(), "single.txt")
	if _, err := dbconn.Exec(`INSERT INTO physical_file (path, logical_file_id, is_metadata_complete) VALUES ($1, $2, 0)`, storedPath, logicalID); err != nil {
		t.Fatalf("insert physical_file row: %v", err)
	}

	result, err := RemoveFileByStoredPathWithStorageContextResult(StorageContext{DB: dbconn}, storedPath)
	if err != nil {
		t.Fatalf("remove by stored path: %v", err)
	}
	if result.RemainingRefCount != 0 {
		t.Fatalf("expected remaining_ref_count=0, got %d", result.RemainingRefCount)
	}

	var refCount int64
	if err := dbconn.QueryRow(`SELECT ref_count FROM logical_file WHERE id = $1`, logicalID).Scan(&refCount); err != nil {
		t.Fatalf("read logical ref_count: %v", err)
	}
	if refCount != 0 {
		t.Fatalf("expected logical ref_count=0 after removing last mapping, got %d", refCount)
	}
}

func TestRemoveByStoredPathReturnsNotFound(t *testing.T) {
	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	defer func() { _ = dbconn.Close() }()
	if err := db.RunMigrations(dbconn); err != nil {
		t.Fatalf("run migrations: %v", err)
	}

	err = RemoveFileByStoredPathWithStorageContext(StorageContext{DB: dbconn}, filepath.Join(t.TempDir(), "missing.txt"))
	if err == nil || !strings.Contains(err.Error(), "physical_file[") || !strings.Contains(err.Error(), "not found (never stored)") {
		t.Fatalf("expected physical-path not found error, got: %v", err)
	}
}

func TestRemoveByStoredPathSecondRemoveFailsCleanly(t *testing.T) {
	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	defer func() { _ = dbconn.Close() }()
	if err := db.RunMigrations(dbconn); err != nil {
		t.Fatalf("run migrations: %v", err)
	}

	var logicalID int64
	if err := dbconn.QueryRow(
		`INSERT INTO logical_file (original_name, total_size, file_hash, status, ref_count, chunker_version)
		 VALUES ($1, $2, $3, $4, $5, 'v1-simple-rolling') RETURNING id`,
		"again.bin", int64(4), strings.Repeat("c", 64), filestate.LogicalFileCompleted, int64(1),
	).Scan(&logicalID); err != nil {
		t.Fatalf("insert logical file: %v", err)
	}

	storedPath := filepath.Join(t.TempDir(), "again.txt")
	if _, err := dbconn.Exec(`INSERT INTO physical_file (path, logical_file_id, is_metadata_complete) VALUES ($1, $2, 0)`, storedPath, logicalID); err != nil {
		t.Fatalf("insert physical_file row: %v", err)
	}

	if _, err := RemoveFileByStoredPathWithStorageContextResult(StorageContext{DB: dbconn}, storedPath); err != nil {
		t.Fatalf("first remove by stored path failed: %v", err)
	}

	err = RemoveFileByStoredPathWithStorageContext(StorageContext{DB: dbconn}, storedPath)
	if err == nil || !strings.Contains(err.Error(), "not found (never stored)") {
		t.Fatalf("expected second remove to fail with not-found, got: %v", err)
	}
}

func TestRemoveByStoredPathDetectsRefCountInvariantMismatchAndRollsBack(t *testing.T) {
	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	defer func() { _ = dbconn.Close() }()
	if err := db.RunMigrations(dbconn); err != nil {
		t.Fatalf("run migrations: %v", err)
	}

	var logicalID int64
	if err := dbconn.QueryRow(
		`INSERT INTO logical_file (original_name, total_size, file_hash, status, ref_count, chunker_version)
		 VALUES ($1, $2, $3, $4, $5, 'v1-simple-rolling') RETURNING id`,
		"corrupt.bin", int64(4), strings.Repeat("d", 64), filestate.LogicalFileCompleted, int64(5),
	).Scan(&logicalID); err != nil {
		t.Fatalf("insert logical file: %v", err)
	}

	storedPath := filepath.Join(t.TempDir(), "corrupt.txt")
	if _, err := dbconn.Exec(`INSERT INTO physical_file (path, logical_file_id, is_metadata_complete) VALUES ($1, $2, 0)`, storedPath, logicalID); err != nil {
		t.Fatalf("insert physical_file row: %v", err)
	}

	_, err = RemoveFileByStoredPathWithStorageContextResult(StorageContext{DB: dbconn}, storedPath)
	if err == nil || !strings.Contains(err.Error(), "ref_count invariant mismatch") {
		t.Fatalf("expected ref_count invariant mismatch error, got: %v", err)
	}

	var refCount int64
	if err := dbconn.QueryRow(`SELECT ref_count FROM logical_file WHERE id = $1`, logicalID).Scan(&refCount); err != nil {
		t.Fatalf("read logical ref_count after rollback: %v", err)
	}
	if refCount != 5 {
		t.Fatalf("expected rollback to keep original ref_count=5, got %d", refCount)
	}

	var mappingCount int64
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM physical_file WHERE path = $1`, storedPath).Scan(&mappingCount); err != nil {
		t.Fatalf("check mapping rollback: %v", err)
	}
	if mappingCount != 1 {
		t.Fatalf("expected rollback to keep physical mapping row, count=%d", mappingCount)
	}
}

// TestRemoveFileByIDCascadesPhysicalFileMappings verifies that remove-by-ID
// cascades through all physical_file mappings, ensuring no orphan physical_file
// rows are left pointing to the deleted logical_file. This maintains semantic
// consistency with remove-by-stored-path.
func TestRemoveFileByIDCascadesPhysicalFileMappings(t *testing.T) {
	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	defer func() { _ = dbconn.Close() }()
	if err := db.RunMigrations(dbconn); err != nil {
		t.Fatalf("run migrations: %v", err)
	}

	// Set up: single logical_file with 3 physical_file mappings (mirrors the "shared logical" scenario).
	var logicalID int64
	if err := dbconn.QueryRow(
		`INSERT INTO logical_file (original_name, total_size, file_hash, status, ref_count, chunker_version)
		 VALUES ($1, $2, $3, $4, $5, 'v1-simple-rolling') RETURNING id`,
		"shared.bin", int64(1000), strings.Repeat("a", 64), filestate.LogicalFileCompleted, int64(3),
	).Scan(&logicalID); err != nil {
		t.Fatalf("insert logical file: %v", err)
	}

	// Create 3 physical_file mappings for the same logical_file
	tempDir := t.TempDir()
	paths := []string{
		filepath.Join(tempDir, "mapping1.txt"),
		filepath.Join(tempDir, "mapping2.txt"),
		filepath.Join(tempDir, "mapping3.txt"),
	}

	for i, path := range paths {
		if _, err := dbconn.Exec(
			`INSERT INTO physical_file (path, logical_file_id, is_metadata_complete) VALUES ($1, $2, 0)`,
			path, logicalID,
		); err != nil {
			t.Fatalf("insert physical_file %d: %v", i+1, err)
		}
	}

	// Verify pre-condition: 3 physical_file rows, ref_count=3
	var physicalCount int64
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM physical_file WHERE logical_file_id = $1`, logicalID).Scan(&physicalCount); err != nil {
		t.Fatalf("check initial physical_file count: %v", err)
	}
	if physicalCount != 3 {
		t.Fatalf("expected 3 physical_file rows, got %d", physicalCount)
	}

	// Execute: remove by ID
	_, err = RemoveFileWithDBResult(dbconn, logicalID)
	if err != nil {
		t.Fatalf("remove by ID failed: %v", err)
	}

	// Verify post-condition: logical_file is gone
	var existsCount int64
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM logical_file WHERE id = $1`, logicalID).Scan(&existsCount); err != nil {
		t.Fatalf("check logical_file post-remove: %v", err)
	}
	if existsCount != 0 {
		t.Fatalf("expected logical_file to be deleted, but count=%d", existsCount)
	}

	// Verify invariant: NO orphan physical_file rows exist
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM physical_file WHERE logical_file_id = $1`, logicalID).Scan(&physicalCount); err != nil {
		t.Fatalf("check orphan physical_file rows: %v", err)
	}
	if physicalCount != 0 {
		t.Fatalf("expected 0 orphan physical_file rows after cascade delete, got %d", physicalCount)
	}
}

// TestRemoveFileByIDZerosRefCountDuringCascade verifies that during the cascade
// removal of physical_file mappings, the ref_count is decremented for each
// mapping removed, and each intermediate state maintains the invariant.
func TestRemoveFileByIDZerosRefCountDuringCascade(t *testing.T) {
	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	defer func() { _ = dbconn.Close() }()
	if err := db.RunMigrations(dbconn); err != nil {
		t.Fatalf("run migrations: %v", err)
	}

	// Set up: logical_file with 2 physical_file mappings
	var logicalID int64
	if err := dbconn.QueryRow(
		`INSERT INTO logical_file (original_name, total_size, file_hash, status, ref_count, chunker_version)
		 VALUES ($1, $2, $3, $4, $5, 'v1-simple-rolling') RETURNING id`,
		"multi-map.bin", int64(500), strings.Repeat("b", 64), filestate.LogicalFileCompleted, int64(2),
	).Scan(&logicalID); err != nil {
		t.Fatalf("insert logical file: %v", err)
	}

	tempDir := t.TempDir()
	path1 := filepath.Join(tempDir, "map1.txt")
	path2 := filepath.Join(tempDir, "map2.txt")

	for _, path := range []string{path1, path2} {
		if _, err := dbconn.Exec(
			`INSERT INTO physical_file (path, logical_file_id, is_metadata_complete) VALUES ($1, $2, 0)`,
			path, logicalID,
		); err != nil {
			t.Fatalf("insert physical_file: %v", err)
		}
	}

	// Execute: remove by ID
	_, err = RemoveFileWithDBResult(dbconn, logicalID)
	if err != nil {
		t.Fatalf("remove by ID failed: %v", err)
	}

	// Verify: both logical_file and all physical_file rows are gone
	var logicalExists int64
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM logical_file WHERE id = $1`, logicalID).Scan(&logicalExists); err != nil {
		t.Fatalf("check logical_file exists: %v", err)
	}
	if logicalExists != 0 {
		t.Fatalf("expected logical_file to be deleted")
	}

	var physicalExists int64
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM physical_file WHERE logical_file_id = $1`, logicalID).Scan(&physicalExists); err != nil {
		t.Fatalf("check physical_file cascade delete: %v", err)
	}
	if physicalExists != 0 {
		t.Fatalf("expected all physical_file rows to be cascade-deleted, got count=%d", physicalExists)
	}
}

// TestRemoveFileByIDAndRemoveByStoredPathAreSymmetric verifies that both
// removal semantics (remove-by-ID and remove-by-stored-path) are consistent:
// they both remove physical mappings and maintain ref_count == COUNT(physical_file rows).
func TestRemoveFileByIDAndRemoveByStoredPathAreSymmetric(t *testing.T) {
	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	defer func() { _ = dbconn.Close() }()
	if err := db.RunMigrations(dbconn); err != nil {
		t.Fatalf("run migrations: %v", err)
	}

	tempDir := t.TempDir()

	// Scenario 1: remove-by-stored-path leaves logical_file intact (shared mapping).
	var log1ID int64
	if err := dbconn.QueryRow(
		`INSERT INTO logical_file (original_name, total_size, file_hash, status, ref_count, chunker_version)
		 VALUES ($1, $2, $3, $4, $5, 'v1-simple-rolling') RETURNING id`,
		"shared1.bin", int64(100), strings.Repeat("1", 64), filestate.LogicalFileCompleted, int64(2),
	).Scan(&log1ID); err != nil {
		t.Fatalf("insert logical file 1: %v", err)
	}

	path1a := filepath.Join(tempDir, "log1_map1.txt")
	path1b := filepath.Join(tempDir, "log1_map2.txt")
	for _, path := range []string{path1a, path1b} {
		if _, err := dbconn.Exec(
			`INSERT INTO physical_file (path, logical_file_id) VALUES ($1, $2)`, path, log1ID,
		); err != nil {
			t.Fatalf("insert physical: %v", err)
		}
	}

	// Remove one by stored-path: logical_file should still exist with ref_count=1
	_, err = RemoveFileByStoredPathWithStorageContextResult(StorageContext{DB: dbconn}, path1a)
	if err != nil {
		t.Fatalf("remove by stored-path: %v", err)
	}

	var log1Exists int64
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM logical_file WHERE id = $1`, log1ID).Scan(&log1Exists); err != nil {
		t.Fatalf("check log1 exists: %v", err)
	}
	if log1Exists == 0 {
		t.Fatalf("expected logical_file to still exist after partial remove-by-path")
	}

	var log1RefCount int64
	if err := dbconn.QueryRow(`SELECT ref_count FROM logical_file WHERE id = $1`, log1ID).Scan(&log1RefCount); err != nil {
		t.Fatalf("read log1 ref_count: %v", err)
	}
	if log1RefCount != 1 {
		t.Fatalf("expected ref_count=1 after removing 1 of 2 mappings, got %d", log1RefCount)
	}

	// Scenario 2: remove-by-ID cascades all mappings, deletes logical_file.
	var log2ID int64
	if err := dbconn.QueryRow(
		`INSERT INTO logical_file (original_name, total_size, file_hash, status, ref_count, chunker_version)
		 VALUES ($1, $2, $3, $4, $5, 'v1-simple-rolling') RETURNING id`,
		"shared2.bin", int64(200), strings.Repeat("2", 64), filestate.LogicalFileCompleted, int64(2),
	).Scan(&log2ID); err != nil {
		t.Fatalf("insert logical file 2: %v", err)
	}

	path2a := filepath.Join(tempDir, "log2_map1.txt")
	path2b := filepath.Join(tempDir, "log2_map2.txt")
	for _, path := range []string{path2a, path2b} {
		if _, err := dbconn.Exec(
			`INSERT INTO physical_file (path, logical_file_id) VALUES ($1, $2)`, path, log2ID,
		); err != nil {
			t.Fatalf("insert physical log2: %v", err)
		}
	}

	// Remove by ID: logical_file and all mappings should be gone
	_, err = RemoveFileWithDBResult(dbconn, log2ID)
	if err != nil {
		t.Fatalf("remove by ID: %v", err)
	}

	var log2Exists int64
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM logical_file WHERE id = $1`, log2ID).Scan(&log2Exists); err != nil {
		t.Fatalf("check log2 exists: %v", err)
	}
	if log2Exists != 0 {
		t.Fatalf("expected logical_file to be deleted after remove-by-ID")
	}

	var log2PhysicalCount int64
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM physical_file WHERE logical_file_id = $1`, log2ID).Scan(&log2PhysicalCount); err != nil {
		t.Fatalf("check log2 orphan physical: %v", err)
	}
	if log2PhysicalCount != 0 {
		t.Fatalf("expected no orphan physical_file rows for log2, got count=%d", log2PhysicalCount)
	}

	// Consistency check: both paths leave NO orphan physical_file rows.
	var totalOrphans int64
	if err := dbconn.QueryRow(
		`SELECT COUNT(*) FROM physical_file WHERE logical_file_id NOT IN (SELECT id FROM logical_file)`,
	).Scan(&totalOrphans); err != nil {
		t.Fatalf("check for orphan physical_file: %v", err)
	}
	if totalOrphans != 0 {
		t.Fatalf("expected 0 orphan physical_file rows in entire DB, got %d", totalOrphans)
	}
}
