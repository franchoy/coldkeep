package storage

import (
	"database/sql"
	"path/filepath"
	"strings"
	"testing"

	"github.com/franchoy/coldkeep/internal/db"
	filestate "github.com/franchoy/coldkeep/internal/status"
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
		`INSERT INTO logical_file (original_name, total_size, file_hash, status)
		 VALUES ($1, $2, $3, $4) RETURNING id`,
		"processing-file.bin", int64(8), strings.Repeat("e", 64), filestate.LogicalFileProcessing,
	).Scan(&fileID); err != nil {
		t.Fatalf("insert logical file: %v", err)
	}

	err = RemoveFileWithDB(dbconn, fileID)
	if err == nil || !strings.Contains(err.Error(), "is still PROCESSING and cannot be removed") {
		t.Fatalf("expected PROCESSING error contract, got: %v", err)
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
		`INSERT INTO chunk (chunk_hash, size, status, live_ref_count)
		 VALUES ($1, $2, $3, 0) RETURNING id`,
		strings.Repeat("a", 64), int64(4), filestate.ChunkCompleted,
	).Scan(&chunkID); err != nil {
		t.Fatalf("insert chunk: %v", err)
	}

	var fileID int64
	if err := dbconn.QueryRow(
		`INSERT INTO logical_file (original_name, total_size, file_hash, status)
		 VALUES ($1, $2, $3, $4) RETURNING id`,
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
		`INSERT INTO logical_file (original_name, total_size, file_hash, status)
		 VALUES ($1, $2, $3, $4) RETURNING id`,
		"logical-file-info.bin",
		int64(10),
		strings.Repeat("f", 64),
		filestate.LogicalFileCompleted,
	).Scan(&fileID); err != nil {
		t.Fatalf("insert logical file: %v", err)
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
		`INSERT INTO logical_file (original_name, total_size, file_hash, status, ref_count)
		 VALUES ($1, $2, $3, $4, $5) RETURNING id`,
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
		`INSERT INTO logical_file (original_name, total_size, file_hash, status, ref_count)
		 VALUES ($1, $2, $3, $4, $5) RETURNING id`,
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
	if err == nil || !strings.Contains(err.Error(), "physical file path") || !strings.Contains(err.Error(), "not found") {
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
		`INSERT INTO logical_file (original_name, total_size, file_hash, status, ref_count)
		 VALUES ($1, $2, $3, $4, $5) RETURNING id`,
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
	if err == nil || !strings.Contains(err.Error(), "not found") {
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
		`INSERT INTO logical_file (original_name, total_size, file_hash, status, ref_count)
		 VALUES ($1, $2, $3, $4, $5) RETURNING id`,
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
