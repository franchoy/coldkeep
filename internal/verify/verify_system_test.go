package verify

import (
	"database/sql"
	"strings"
	"testing"

	"github.com/franchoy/coldkeep/internal/db"
	"github.com/franchoy/coldkeep/internal/invariants"
	filestate "github.com/franchoy/coldkeep/internal/status"
	_ "github.com/mattn/go-sqlite3"
)

func openVerifyTestDB(t *testing.T) *sql.DB {
	t.Helper()

	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	dbconn.SetMaxOpenConns(1)
	dbconn.SetMaxIdleConns(1)
	if err := db.RunMigrations(dbconn); err != nil {
		_ = dbconn.Close()
		t.Fatalf("run migrations: %v", err)
	}
	return dbconn
}

func TestVerifySystemStandardPassesOnConsistentPhysicalGraph(t *testing.T) {
	dbconn := openVerifyTestDB(t)
	defer func() { _ = dbconn.Close() }()

	var logicalID int64
	if err := dbconn.QueryRow(
		`INSERT INTO logical_file (original_name, total_size, file_hash, status, ref_count, chunker_version)
		 VALUES ($1, $2, $3, $4, $5, 'v1-simple-rolling') RETURNING id`,
		"healthy.bin", int64(0), strings.Repeat("a", 64), filestate.LogicalFileCompleted, int64(2),
	).Scan(&logicalID); err != nil {
		t.Fatalf("insert logical file: %v", err)
	}

	if _, err := dbconn.Exec(
		`INSERT INTO physical_file (path, logical_file_id, is_metadata_complete) VALUES ($1, $2, 0), ($3, $2, 0)`,
		"/healthy/a", logicalID, "/healthy/b",
	); err != nil {
		t.Fatalf("insert physical_file rows: %v", err)
	}

	if err := VerifySystemStandardWithContainersDir(dbconn, t.TempDir()); err != nil {
		t.Fatalf("verify standard should pass on consistent physical graph: %v", err)
	}
}

func TestVerifySystemStandardDetectsOrphanPhysicalFileRows(t *testing.T) {
	dbconn := openVerifyTestDB(t)
	defer func() { _ = dbconn.Close() }()

	if _, err := dbconn.Exec(`PRAGMA foreign_keys = OFF`); err != nil {
		t.Fatalf("disable sqlite foreign_keys: %v", err)
	}
	if _, err := dbconn.Exec(`INSERT INTO physical_file (path, logical_file_id, is_metadata_complete) VALUES ($1, $2, 0)`, "/orphan/path", int64(999)); err != nil {
		t.Fatalf("insert orphan physical_file row: %v", err)
	}

	err := VerifySystemStandardWithContainersDir(dbconn, t.TempDir())
	if err == nil || !strings.Contains(err.Error(), "orphan physical_file rows=1") {
		t.Fatalf("expected orphan physical_file verification error, got: %v", err)
	}
	if code, ok := invariants.Code(err); !ok || code != invariants.CodePhysicalGraphOrphan {
		t.Fatalf("expected invariant code %s, got code=%q ok=%v err=%v", invariants.CodePhysicalGraphOrphan, code, ok, err)
	}
}

func TestVerifySystemStandardDetectsLogicalRefCountMismatch(t *testing.T) {
	dbconn := openVerifyTestDB(t)
	defer func() { _ = dbconn.Close() }()

	var logicalID int64
	if err := dbconn.QueryRow(
		`INSERT INTO logical_file (original_name, total_size, file_hash, status, ref_count, chunker_version)
		 VALUES ($1, $2, $3, $4, $5, 'v1-simple-rolling') RETURNING id`,
		"mismatch.bin", int64(0), strings.Repeat("b", 64), filestate.LogicalFileCompleted, int64(5),
	).Scan(&logicalID); err != nil {
		t.Fatalf("insert logical file: %v", err)
	}
	if _, err := dbconn.Exec(`INSERT INTO physical_file (path, logical_file_id, is_metadata_complete) VALUES ($1, $2, 0)`, "/mismatch/path", logicalID); err != nil {
		t.Fatalf("insert physical_file row: %v", err)
	}

	err := VerifySystemStandardWithContainersDir(dbconn, t.TempDir())
	if err == nil || !strings.Contains(err.Error(), "logical ref_count mismatches=1") {
		t.Fatalf("expected logical ref_count mismatch verification error, got: %v", err)
	}
	if code, ok := invariants.Code(err); !ok || code != invariants.CodePhysicalGraphRefCountMismatch {
		t.Fatalf("expected invariant code %s, got code=%q ok=%v err=%v", invariants.CodePhysicalGraphRefCountMismatch, code, ok, err)
	}
}

func TestVerifySystemStandardDetectsNegativeLogicalRefCount(t *testing.T) {
	dbconn := openVerifyTestDB(t)
	defer func() { _ = dbconn.Close() }()

	if _, err := dbconn.Exec(`PRAGMA ignore_check_constraints = ON`); err != nil {
		t.Fatalf("disable sqlite check constraints: %v", err)
	}
	if _, err := dbconn.Exec(
		`INSERT INTO logical_file (original_name, total_size, file_hash, status, ref_count, chunker_version) VALUES ($1, $2, $3, $4, $5, 'v1-simple-rolling')`,
		"negative.bin", int64(0), strings.Repeat("c", 64), filestate.LogicalFileCompleted, int64(-1),
	); err != nil {
		t.Fatalf("insert logical file with negative ref_count: %v", err)
	}

	err := VerifySystemStandardWithContainersDir(dbconn, t.TempDir())
	if err == nil || !strings.Contains(err.Error(), "negative logical ref_count rows=1") {
		t.Fatalf("expected negative logical ref_count verification error, got: %v", err)
	}
	if code, ok := invariants.Code(err); !ok || code != invariants.CodePhysicalGraphIntegrity {
		t.Fatalf("expected invariant code %s, got code=%q ok=%v err=%v", invariants.CodePhysicalGraphIntegrity, code, ok, err)
	}
}

func TestVerifySystemStandardDetectsEmptyLogicalFileChunkerVersion(t *testing.T) {
	dbconn := openVerifyTestDB(t)
	defer func() { _ = dbconn.Close() }()

	if _, err := dbconn.Exec(`PRAGMA ignore_check_constraints = ON`); err != nil {
		t.Fatalf("disable sqlite check constraints: %v", err)
	}
	if _, err := dbconn.Exec(
		`INSERT INTO logical_file (original_name, total_size, file_hash, status, ref_count, chunker_version) VALUES ($1, $2, $3, $4, $5, $6)`,
		"missing-version.bin", int64(0), strings.Repeat("d", 64), filestate.LogicalFileCompleted, int64(0), "   ",
	); err != nil {
		t.Fatalf("insert logical file with empty chunker_version: %v", err)
	}

	err := VerifySystemStandardWithContainersDir(dbconn, t.TempDir())
	if err == nil || !strings.Contains(err.Error(), "empty logical_file chunker_version rows=1") {
		t.Fatalf("expected logical_file chunker_version verification error, got: %v", err)
	}
	if code, ok := invariants.Code(err); !ok || code != invariants.CodePhysicalGraphIntegrity {
		t.Fatalf("expected invariant code %s, got code=%q ok=%v err=%v", invariants.CodePhysicalGraphIntegrity, code, ok, err)
	}
}

func TestVerifySystemStandardDetectsEmptyChunkChunkerVersion(t *testing.T) {
	dbconn := openVerifyTestDB(t)
	defer func() { _ = dbconn.Close() }()

	if _, err := dbconn.Exec(`PRAGMA ignore_check_constraints = ON`); err != nil {
		t.Fatalf("disable sqlite check constraints: %v", err)
	}
	if _, err := dbconn.Exec(
		`INSERT INTO chunk (chunk_hash, size, status, live_ref_count, pin_count, retry_count, chunker_version) VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		strings.Repeat("e", 64), int64(11), filestate.ChunkAborted, int64(0), int64(0), int64(0), "",
	); err != nil {
		t.Fatalf("insert chunk with empty chunker_version: %v", err)
	}

	err := VerifySystemStandardWithContainersDir(dbconn, t.TempDir())
	if err == nil || !strings.Contains(err.Error(), "empty chunk chunker_version rows=1") {
		t.Fatalf("expected chunk chunker_version verification error, got: %v", err)
	}
	if code, ok := invariants.Code(err); !ok || code != invariants.CodePhysicalGraphIntegrity {
		t.Fatalf("expected invariant code %s, got code=%q ok=%v err=%v", invariants.CodePhysicalGraphIntegrity, code, ok, err)
	}
}

func TestVerifyRepositoryDetectsChunkBlockRefMissingStorageBlock(t *testing.T) {
	dbconn := openVerifyTestDB(t)
	defer func() { _ = dbconn.Close() }()

	if _, err := dbconn.Exec(`PRAGMA foreign_keys = OFF`); err != nil {
		t.Fatalf("disable sqlite foreign_keys: %v", err)
	}

	var chunkID int64
	if err := dbconn.QueryRow(
		`INSERT INTO chunk (chunk_hash, size, status, live_ref_count, pin_count, retry_count, chunker_version)
		 VALUES ($1, $2, $3, $4, $5, $6, $7)
		 RETURNING id`,
		strings.Repeat("f", 64),
		int64(17),
		filestate.ChunkAborted,
		int64(0),
		int64(0),
		int64(0),
		"v1-simple-rolling",
	).Scan(&chunkID); err != nil {
		t.Fatalf("insert chunk: %v", err)
	}

	if _, err := dbconn.Exec(
		`INSERT INTO chunk_block_refs (chunk_id, block_id, offset_in_block, size_in_block)
		 VALUES ($1, $2, $3, $4)`,
		chunkID,
		int64(9999),
		int64(0),
		int64(17),
	); err != nil {
		t.Fatalf("insert orphan chunk_block_ref: %v", err)
	}

	err := VerifyRepository(dbconn, t.TempDir())
	if err == nil || !strings.Contains(err.Error(), "verifyChunkBlockRefs") || !strings.Contains(err.Error(), "missing storage_blocks") {
		t.Fatalf("expected verifyChunkBlockRefs missing storage_blocks error, got: %v", err)
	}
}

func TestVerifyRepositoryDetectsFileChunkMissingChunkRef(t *testing.T) {
	dbconn := openVerifyTestDB(t)
	defer func() { _ = dbconn.Close() }()

	if _, err := dbconn.Exec(`PRAGMA foreign_keys = OFF`); err != nil {
		t.Fatalf("disable sqlite foreign_keys: %v", err)
	}

	var fileID int64
	if err := dbconn.QueryRow(
		`INSERT INTO logical_file (original_name, total_size, file_hash, status, ref_count, chunker_version)
		 VALUES ($1, $2, $3, $4, $5, $6)
		 RETURNING id`,
		"missing-chunk-ref.bin",
		int64(17),
		strings.Repeat("a", 64),
		filestate.LogicalFileCompleted,
		int64(0),
		"v1-simple-rolling",
	).Scan(&fileID); err != nil {
		t.Fatalf("insert logical file: %v", err)
	}

	if _, err := dbconn.Exec(
		`INSERT INTO file_chunk (logical_file_id, chunk_id, chunk_order)
		 VALUES ($1, $2, $3)`,
		fileID,
		int64(9999),
		int64(0),
	); err != nil {
		t.Fatalf("insert invalid file_chunk row: %v", err)
	}

	err := VerifyRepository(dbconn, t.TempDir())
	if err == nil || !strings.Contains(err.Error(), "verifyFileChunkRelationships") || !strings.Contains(err.Error(), "missing chunk refs") {
		t.Fatalf("expected verifyFileChunkRelationships missing chunk ref error, got: %v", err)
	}
}

func TestVerifyRepositoryRejectsInvalidDualLegacyPackedMapping(t *testing.T) {
	dbconn := openVerifyTestDB(t)
	defer func() { _ = dbconn.Close() }()

	var containerID int64
	if err := dbconn.QueryRow(
		`INSERT INTO container (filename, current_size, max_size, sealed, quarantine)
		 VALUES ($1, $2, $3, FALSE, FALSE)
		 RETURNING id`,
		"verify-dual-mapping.bin",
		int64(1<<20),
		int64(1<<20),
	).Scan(&containerID); err != nil {
		t.Fatalf("insert container: %v", err)
	}

	var chunkID int64
	if err := dbconn.QueryRow(
		`INSERT INTO chunk (chunk_hash, size, status, live_ref_count, pin_count, retry_count, chunker_version)
		 VALUES ($1, $2, $3, $4, $5, $6, $7)
		 RETURNING id`,
		strings.Repeat("b", 64),
		int64(64),
		filestate.ChunkCompleted,
		int64(0),
		int64(0),
		int64(0),
		"v1-simple-rolling",
	).Scan(&chunkID); err != nil {
		t.Fatalf("insert chunk: %v", err)
	}

	var storageBlockID int64
	if err := dbconn.QueryRow(
		`INSERT INTO storage_blocks (format_version, codec, plaintext_size, stored_size, container_id, container_offset, block_hash)
		 VALUES (1, 'none', 80, 80, $1, $2, zeroblob(32))
		 RETURNING id`,
		containerID,
		int64(256),
	).Scan(&storageBlockID); err != nil {
		t.Fatalf("insert storage block: %v", err)
	}

	if _, err := dbconn.Exec(
		`INSERT INTO chunk_block_refs (chunk_id, block_id, offset_in_block, size_in_block)
		 VALUES ($1, $2, 0, $3)`,
		chunkID,
		storageBlockID,
		int64(64),
	); err != nil {
		t.Fatalf("insert chunk_block_refs row: %v", err)
	}

	// Invalid dual mapping: legacy container placement doesn't match packed block placement.
	if _, err := dbconn.Exec(
		`INSERT INTO blocks (chunk_id, codec, format_version, plaintext_size, stored_size, nonce, container_id, block_offset)
		 VALUES ($1, 'plain', 1, $2, $3, x'', $4, $5)`,
		chunkID,
		int64(64),
		int64(64),
		containerID,
		int64(1024),
	); err != nil {
		t.Fatalf("insert invalid legacy companion row: %v", err)
	}

	err := VerifyRepository(dbconn, t.TempDir())
	if err == nil || !strings.Contains(err.Error(), "verifyChunkBlockRefs") || !strings.Contains(err.Error(), "outside migration companion contract") {
		t.Fatalf("expected invalid dual mapping error, got: %v", err)
	}
}

func TestVerifyRepositoryDetectsInvalidStorageBlockMetadataFields(t *testing.T) {
	dbconn := openVerifyTestDB(t)
	defer func() { _ = dbconn.Close() }()

	var containerID int64
	if err := dbconn.QueryRow(
		`INSERT INTO container (filename, current_size, max_size, sealed, quarantine)
		 VALUES ($1, $2, $3, FALSE, FALSE)
		 RETURNING id`,
		"verify-invalid-storage-metadata.bin",
		int64(4096),
		int64(4096),
	).Scan(&containerID); err != nil {
		t.Fatalf("insert container: %v", err)
	}

	if _, err := dbconn.Exec(
		`INSERT INTO storage_blocks (format_version, codec, plaintext_size, stored_size, container_id, container_offset, block_hash)
		 VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		2,       // invalid by verify policy: must be 1
		"plain", // invalid by verify policy: must be none
		int64(16),
		int64(16),
		containerID,
		int64(0),
		[]byte{0x01, 0x02}, // invalid: expected 32 bytes
	); err != nil {
		t.Fatalf("insert invalid storage_blocks row: %v", err)
	}

	err := VerifyRepository(dbconn, t.TempDir())
	if err == nil || !strings.Contains(err.Error(), "verifyStorageBlocks") || !strings.Contains(err.Error(), "invalid metadata fields") {
		t.Fatalf("expected verifyStorageBlocks invalid metadata fields error, got: %v", err)
	}
}

func TestVerifyRepositoryDetectsImpossibleStorageBlockContainerRange(t *testing.T) {
	dbconn := openVerifyTestDB(t)
	defer func() { _ = dbconn.Close() }()

	var containerID int64
	if err := dbconn.QueryRow(
		`INSERT INTO container (filename, current_size, max_size, sealed, quarantine)
		 VALUES ($1, $2, $3, FALSE, FALSE)
		 RETURNING id`,
		"verify-impossible-range.bin",
		int64(128),
		int64(128),
	).Scan(&containerID); err != nil {
		t.Fatalf("insert container: %v", err)
	}

	if _, err := dbconn.Exec(
		`INSERT INTO storage_blocks (format_version, codec, plaintext_size, stored_size, container_id, container_offset, block_hash)
		 VALUES (1, 'none', $1, $2, $3, $4, zeroblob(32))`,
		int64(64),
		int64(100),
		containerID,
		int64(64), // 64 + 100 > 128
	); err != nil {
		t.Fatalf("insert impossible-range storage block: %v", err)
	}

	err := VerifyRepository(dbconn, t.TempDir())
	if err == nil || !strings.Contains(err.Error(), "verifyStorageBlocks") || !strings.Contains(err.Error(), "impossible container ranges") {
		t.Fatalf("expected impossible range error, got: %v", err)
	}
}
