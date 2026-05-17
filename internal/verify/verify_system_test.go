package verify

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"encoding/hex"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/franchoy/coldkeep/internal/blocks"
	"github.com/franchoy/coldkeep/internal/container"
	"github.com/franchoy/coldkeep/internal/db"
	"github.com/franchoy/coldkeep/internal/invariants"
	filestate "github.com/franchoy/coldkeep/internal/status"
	storagecompression "github.com/franchoy/coldkeep/internal/storage/compression"
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

func TestVerifyRepositoryDetectsChunkBlockRefInvalidChunkID(t *testing.T) {
	dbconn := openVerifyTestDB(t)
	defer func() { _ = dbconn.Close() }()

	if _, err := dbconn.Exec(`PRAGMA foreign_keys = OFF`); err != nil {
		t.Fatalf("disable sqlite foreign_keys: %v", err)
	}

	var containerID int64
	if err := dbconn.QueryRow(
		`INSERT INTO container (filename, current_size, max_size, sealed, quarantine)
		 VALUES ($1, $2, $3, FALSE, FALSE)
		 RETURNING id`,
		"verify-invalid-chunk-id.bin",
		int64(4096),
		int64(4096),
	).Scan(&containerID); err != nil {
		t.Fatalf("insert container: %v", err)
	}

	var blockID int64
	if err := dbconn.QueryRow(
		`INSERT INTO storage_blocks (format_version, codec, plaintext_size, stored_size, container_id, container_offset, block_hash)
		 VALUES (1, 'none', 32, 32, $1, 0, zeroblob(32))
		 RETURNING id`,
		containerID,
	).Scan(&blockID); err != nil {
		t.Fatalf("insert storage block: %v", err)
	}

	if _, err := dbconn.Exec(
		`INSERT INTO chunk_block_refs (chunk_id, block_id, offset_in_block, size_in_block)
		 VALUES ($1, $2, $3, $4)`,
		int64(9999),
		blockID,
		int64(0),
		int64(32),
	); err != nil {
		t.Fatalf("insert invalid chunk id row: %v", err)
	}

	err := VerifyRepository(dbconn, t.TempDir())
	if err == nil || !strings.Contains(err.Error(), "verifyChunkBlockRefs") || !strings.Contains(err.Error(), "missing chunks") {
		t.Fatalf("expected invalid chunk_id error, got: %v", err)
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
	if err == nil || !strings.HasPrefix(err.Error(), "metadata_invalid:") || !strings.Contains(err.Error(), "verifyChunkBlockRefs") || !strings.Contains(err.Error(), "outside migration companion contract") {
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
		2,      // invalid by verify policy: must be 1
		"none", // canonical codec for plain packed blocks (schema CHECK: 'none' | 'aes-gcm')
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

func TestVerifyRepositoryDetectsStorageBlockWithoutChunkBlockRefs(t *testing.T) {
	dbconn := openVerifyTestDB(t)
	defer func() { _ = dbconn.Close() }()

	containersDir := t.TempDir()
	blockID, _ := seedVerifyPackedBlockFixture(t, dbconn, containersDir, [][]byte{[]byte("manifest-missing-ref")}, nil)

	if _, err := dbconn.Exec(`DELETE FROM chunk_block_refs WHERE block_id = $1`, blockID); err != nil {
		t.Fatalf("delete chunk_block_refs for manifest missing-ref test: %v", err)
	}

	err := VerifyRepository(dbconn, containersDir)
	if err == nil || !strings.Contains(err.Error(), "verifyPackedManifestIndex") || !strings.Contains(err.Error(), "missing chunk_block_refs") {
		t.Fatalf("expected verifyPackedManifestIndex missing chunk_block_refs error, got: %v", err)
	}
}

func TestVerifyRepositoryDetectsConflictingChunkBlockRefOffsets(t *testing.T) {
	dbconn := openVerifyTestDB(t)
	defer func() { _ = dbconn.Close() }()

	containersDir := t.TempDir()
	_, chunkIDs := seedVerifyPackedBlockFixture(t, dbconn, containersDir, [][]byte{[]byte("seg-a"), []byte("seg-b")}, nil)

	if _, err := dbconn.Exec(`UPDATE chunk_block_refs SET offset_in_block = 0 WHERE chunk_id = $1`, chunkIDs[1]); err != nil {
		t.Fatalf("force conflicting chunk_block_refs offsets: %v", err)
	}

	err := VerifyRepository(dbconn, containersDir)
	if err == nil || !strings.Contains(err.Error(), "verifyPackedManifestIndex") || !strings.Contains(err.Error(), "conflicting chunk_block_refs offsets") {
		t.Fatalf("expected verifyPackedManifestIndex conflicting offsets error, got: %v", err)
	}
}

func TestVerifyRepositoryFastRejectsInvalidManifestIndexBeforePayloadRead(t *testing.T) {
	dbconn := openVerifyTestDB(t)
	defer func() { _ = dbconn.Close() }()

	containersDir := t.TempDir()
	blockID, _ := seedVerifyPackedBlockFixture(t, dbconn, containersDir, [][]byte{[]byte("fast-manifest-missing-ref")}, nil)

	if _, err := dbconn.Exec(`DELETE FROM chunk_block_refs WHERE block_id = $1`, blockID); err != nil {
		t.Fatalf("delete chunk_block_refs for fast manifest test: %v", err)
	}

	err := VerifyRepositoryFast(dbconn, containersDir)
	if err == nil || !strings.Contains(err.Error(), "verifyPackedManifestIndex") || !strings.Contains(err.Error(), "missing chunk_block_refs") {
		t.Fatalf("expected VerifyRepositoryFast manifest/index failure, got: %v", err)
	}
	if strings.Contains(err.Error(), "verifyBlockPayloads") {
		t.Fatalf("expected manifest/index failure before payload read stage, got: %v", err)
	}
}

func TestVerifyRepositoryDetectsCompletedChunkWithoutPhysicalLocation(t *testing.T) {
	dbconn := openVerifyTestDB(t)
	defer func() { _ = dbconn.Close() }()

	if _, err := dbconn.Exec(
		`INSERT INTO chunk (chunk_hash, size, status, live_ref_count, pin_count, retry_count, chunker_version)
		 VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		strings.Repeat("c", 64),
		int64(17),
		filestate.ChunkCompleted,
		int64(0),
		int64(0),
		int64(0),
		"v1-simple-rolling",
	); err != nil {
		t.Fatalf("insert completed chunk without location: %v", err)
	}

	err := verifyChunkBlockRefs(dbconn)
	if err == nil || !strings.Contains(err.Error(), "no physical location") {
		t.Fatalf("expected no-physical-location error, got: %v", err)
	}
}

func TestVerifyChunkBlockRefsDetectsDuplicateChunkBlockRefs(t *testing.T) {
	dbconn := openVerifyTestDB(t)
	defer func() { _ = dbconn.Close() }()

	containersDir := t.TempDir()
	blockID, chunkIDs := seedVerifyPackedBlockFixture(t, dbconn, containersDir, [][]byte{[]byte("dup-test")}, nil)

	if _, err := dbconn.Exec(`ALTER TABLE chunk_block_refs RENAME TO chunk_block_refs_old`); err != nil {
		t.Fatalf("rename chunk_block_refs table for duplicate corruption test: %v", err)
	}
	if _, err := dbconn.Exec(`
		CREATE TABLE chunk_block_refs (
			chunk_id INTEGER NOT NULL,
			block_id INTEGER NOT NULL,
			offset_in_block INTEGER NOT NULL,
			size_in_block INTEGER NOT NULL
		)
	`); err != nil {
		t.Fatalf("recreate chunk_block_refs table without PK: %v", err)
	}
	if _, err := dbconn.Exec(`INSERT INTO chunk_block_refs (chunk_id, block_id, offset_in_block, size_in_block) SELECT chunk_id, block_id, offset_in_block, size_in_block FROM chunk_block_refs_old`); err != nil {
		t.Fatalf("copy rows into corrupted chunk_block_refs: %v", err)
	}
	if _, err := dbconn.Exec(`DROP TABLE chunk_block_refs_old`); err != nil {
		t.Fatalf("drop old chunk_block_refs table: %v", err)
	}

	if _, err := dbconn.Exec(
		`INSERT INTO chunk_block_refs (chunk_id, block_id, offset_in_block, size_in_block)
		 VALUES ($1, $2, $3, $4)`,
		chunkIDs[0],
		blockID,
		int64(0),
		int64(len([]byte("dup-test"))),
	); err != nil {
		t.Fatalf("insert duplicate chunk_block_ref row: %v", err)
	}

	err := verifyChunkBlockRefs(dbconn)
	if err == nil || !strings.Contains(err.Error(), "multiple packed refs") {
		t.Fatalf("expected duplicate chunk_block_ref detection, got: %v", err)
	}
}

func packedFixtureBlockStorageMeta(t testing.TB, dbconn *sql.DB, blockID int64, containersDir string) (string, int64, int64, int64) {
	t.Helper()

	var filename string
	var containerOffset int64
	var storedSize int64
	var plaintextSize int64
	if err := dbconn.QueryRow(`
		SELECT c.filename, sb.container_offset, sb.stored_size, sb.plaintext_size
		FROM storage_blocks sb
		JOIN container c ON c.id = sb.container_id
		WHERE sb.id = $1
	`, blockID).Scan(&filename, &containerOffset, &storedSize, &plaintextSize); err != nil {
		t.Fatalf("query storage block metadata: %v", err)
	}

	path := filepath.Join(containersDir, filename)
	return path, containerOffset, storedSize, plaintextSize
}

func readPackedStoredBytesForTest(t testing.TB, path string, offset int64, size int64) []byte {
	t.Helper()

	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open container for read: %v", err)
	}
	defer func() { _ = f.Close() }()

	buf := make([]byte, size)
	n, err := f.ReadAt(buf, offset)
	if err != nil {
		t.Fatalf("read stored bytes: %v", err)
	}
	if int64(n) != size {
		t.Fatalf("short read for stored bytes: got %d want %d", n, size)
	}
	return buf
}

func overwritePackedStoredBytesForTest(t testing.TB, path string, offset int64, payload []byte) {
	t.Helper()

	f, err := os.OpenFile(path, os.O_RDWR, 0644)
	if err != nil {
		t.Fatalf("open container for write: %v", err)
	}
	defer func() { _ = f.Close() }()

	n, err := f.WriteAt(payload, offset)
	if err != nil {
		t.Fatalf("write stored bytes: %v", err)
	}
	if n != len(payload) {
		t.Fatalf("short write for stored bytes: got %d want %d", n, len(payload))
	}
}

func setPackedBlockHashForBytes(t testing.TB, dbconn *sql.DB, blockID int64, payload []byte) {
	t.Helper()

	h := blocks.ComputeBlockHash(payload)
	if _, err := dbconn.Exec(`UPDATE storage_blocks SET block_hash = $1 WHERE id = $2`, h, blockID); err != nil {
		t.Fatalf("update block hash for mutated payload: %v", err)
	}
}

type verifyPackedRefSeed struct {
	chunkID int64
	offset  int64
	size    int64
}

func seedVerifyCompressedPackedBlockFixture(t testing.TB, dbconn *sql.DB, containersDir string, chunkPayloads [][]byte, codec blocks.Codec, compressionCodec string) (int64, []int64) {
	t.Helper()

	storageCodec := "none"
	if codec == blocks.CodecAESGCM {
		storageCodec = string(blocks.CodecAESGCM)
	}

	chunkIDs := make([]int64, 0, len(chunkPayloads))
	packedChunks := make([]blocks.PackedChunk, 0, len(chunkPayloads))
	for _, payload := range chunkPayloads {
		var chunkID int64
		sum := sha256.Sum256(payload)
		hash := hex.EncodeToString(sum[:])
		if err := dbconn.QueryRow(
			`INSERT INTO chunk (chunk_hash, size, status, live_ref_count, pin_count, retry_count, chunker_version)
			 VALUES ($1, $2, $3, 0, 0, 0, 'v1-simple-rolling')
			 RETURNING id`,
			hash,
			int64(len(payload)),
			filestate.ChunkAborted,
		).Scan(&chunkID); err != nil {
			t.Fatalf("insert verify compressed chunk: %v", err)
		}
		chunkIDs = append(chunkIDs, chunkID)
		packedChunks = append(packedChunks, blocks.PackedChunk{ChunkID: uint64(chunkID), Data: payload})
	}

	encoded, err := blocks.EncodePackedBlockV1FromChunks(packedChunks)
	if err != nil {
		t.Fatalf("encode compressed packed block fixture: %v", err)
	}

	compressor, err := storagecompression.Lookup(compressionCodec)
	if err != nil {
		t.Fatalf("lookup compression codec %q: %v", compressionCodec, err)
	}
	compressedBytes, err := compressor.Compress(encoded.Bytes)
	if err != nil {
		t.Fatalf("compress packed block fixture: %v", err)
	}

	transformer, err := blocks.GetBlockTransformer(codec)
	if err != nil {
		t.Fatalf("get transformer for compressed fixture: %v", err)
	}
	transformed, err := transformer.Encode(context.Background(), blocks.EncodeInput{Plaintext: compressedBytes})
	if err != nil {
		t.Fatalf("transform compressed packed block fixture: %v", err)
	}

	storedBytes := append([]byte(nil), transformed.Payload...)
	if codec == blocks.CodecAESGCM {
		storedBytes = append(append([]byte(nil), transformed.Descriptor.Nonce...), transformed.Payload...)
	}

	writer := container.NewLocalWriterWithDirAndDB(containersDir, container.GetContainerMaxSize(), dbconn)
	tx, err := dbconn.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("begin tx for compressed packed fixture: %v", err)
	}
	placement, err := writer.AppendPayload(tx, storedBytes)
	if err != nil {
		_ = tx.Rollback()
		t.Fatalf("append compressed packed fixture payload: %v", err)
	}
	if err := container.UpdateContainerSize(tx, placement.ContainerID, placement.NewContainerSize); err != nil {
		_ = tx.Rollback()
		t.Fatalf("update container size for compressed packed fixture: %v", err)
	}

	var compressionLevel any
	if compressionCodec == storagecompression.CompressionZstd {
		compressionLevel = 3
	}

	var blockID int64
	if err := tx.QueryRow(
		`INSERT INTO storage_blocks (
			format_version, codec, plaintext_size, stored_size, container_id, container_offset,
			block_hash, compression_codec, compression_level, compressed_size, compressed_hash, physical_hash
		 ) VALUES (1, $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
		 RETURNING id`,
		storageCodec,
		int64(len(encoded.Bytes)),
		int64(len(storedBytes)),
		placement.ContainerID,
		placement.Offset,
		blocks.HashLogical(encoded.Bytes),
		compressionCodec,
		compressionLevel,
		int64(len(compressedBytes)),
		blocks.HashCompressed(compressedBytes),
		blocks.HashPhysical(storedBytes),
	).Scan(&blockID); err != nil {
		_ = tx.Rollback()
		t.Fatalf("insert compressed storage_blocks fixture: %v", err)
	}

	for i, entry := range encoded.Entries {
		if _, err := tx.Exec(
			`INSERT INTO chunk_block_refs (chunk_id, block_id, offset_in_block, size_in_block)
			 VALUES ($1, $2, $3, $4)`,
			chunkIDs[i],
			blockID,
			int64(entry.Offset),
			int64(entry.Size),
		); err != nil {
			_ = tx.Rollback()
			t.Fatalf("insert compressed chunk_block_refs fixture: %v", err)
		}
	}

	if err := tx.Commit(); err != nil {
		_ = tx.Rollback()
		t.Fatalf("commit compressed packed fixture: %v", err)
	}

	return blockID, chunkIDs
}

func seedVerifyPackedBlockFixture(t testing.TB, dbconn *sql.DB, containersDir string, chunkPayloads [][]byte, refs []verifyPackedRefSeed) (int64, []int64) {
	t.Helper()

	chunkIDs := make([]int64, 0, len(chunkPayloads))
	packedChunks := make([]blocks.PackedChunk, 0, len(chunkPayloads))
	for _, payload := range chunkPayloads {
		var chunkID int64
		sum := sha256.Sum256(payload)
		hash := hex.EncodeToString(sum[:])
		if err := dbconn.QueryRow(
			`INSERT INTO chunk (chunk_hash, size, status, live_ref_count, pin_count, retry_count, chunker_version)
			 VALUES ($1, $2, $3, 0, 0, 0, 'v1-simple-rolling')
			 RETURNING id`,
			hash,
			int64(len(payload)),
			filestate.ChunkAborted,
		).Scan(&chunkID); err != nil {
			t.Fatalf("insert verify chunk: %v", err)
		}
		chunkIDs = append(chunkIDs, chunkID)
		packedChunks = append(packedChunks, blocks.PackedChunk{ChunkID: uint64(chunkID), Data: payload})
	}

	encoded, err := blocks.EncodePackedBlockV1FromChunks(packedChunks)
	if err != nil {
		t.Fatalf("encode packed block fixture: %v", err)
	}

	writer := container.NewLocalWriterWithDirAndDB(containersDir, container.GetContainerMaxSize(), dbconn)
	tx, err := dbconn.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("begin tx for packed fixture: %v", err)
	}
	placement, err := writer.AppendPayload(tx, encoded.Bytes)
	if err != nil {
		_ = tx.Rollback()
		t.Fatalf("append packed fixture payload: %v", err)
	}
	if err := container.UpdateContainerSize(tx, placement.ContainerID, placement.NewContainerSize); err != nil {
		_ = tx.Rollback()
		t.Fatalf("update container size for packed fixture: %v", err)
	}

	var blockID int64
	if err := tx.QueryRow(
		`INSERT INTO storage_blocks (format_version, codec, plaintext_size, stored_size, container_id, container_offset, block_hash)
		 VALUES (1, 'none', $1, $2, $3, $4, $5)
		 RETURNING id`,
		int64(len(encoded.Bytes)),
		int64(len(encoded.Bytes)),
		placement.ContainerID,
		placement.Offset,
		encoded.BlockHash,
	).Scan(&blockID); err != nil {
		_ = tx.Rollback()
		t.Fatalf("insert storage_blocks for fixture: %v", err)
	}

	if refs == nil {
		refs = make([]verifyPackedRefSeed, 0, len(encoded.Entries))
		for i, entry := range encoded.Entries {
			refs = append(refs, verifyPackedRefSeed{chunkID: chunkIDs[i], offset: int64(entry.Offset), size: int64(entry.Size)})
		}
	}

	for _, r := range refs {
		if _, err := tx.Exec(
			`INSERT INTO chunk_block_refs (chunk_id, block_id, offset_in_block, size_in_block)
			 VALUES ($1, $2, $3, $4)`,
			r.chunkID,
			blockID,
			r.offset,
			r.size,
		); err != nil {
			_ = tx.Rollback()
			t.Fatalf("insert chunk_block_refs for fixture: %v", err)
		}
	}

	if err := tx.Commit(); err != nil {
		_ = tx.Rollback()
		t.Fatalf("commit packed fixture: %v", err)
	}

	return blockID, chunkIDs
}

func seedVerifyLegacyBlockFixture(t testing.TB, dbconn *sql.DB, containersDir string, payload []byte) int64 {
	t.Helper()

	sum := sha256.Sum256(payload)
	hash := hex.EncodeToString(sum[:])

	var chunkID int64
	if err := dbconn.QueryRow(
		`INSERT INTO chunk (chunk_hash, size, status, live_ref_count, pin_count, retry_count, chunker_version)
		 VALUES ($1, $2, $3, 0, 0, 0, 'v1-simple-rolling')
		 RETURNING id`,
		hash,
		int64(len(payload)),
		filestate.ChunkCompleted,
	).Scan(&chunkID); err != nil {
		t.Fatalf("insert legacy verify chunk: %v", err)
	}

	transformer, err := blocks.GetBlockTransformer(blocks.CodecPlain)
	if err != nil {
		t.Fatalf("get plain transformer for legacy fixture: %v", err)
	}
	encoded, err := transformer.Encode(context.Background(), blocks.EncodeInput{
		ChunkID:   chunkID,
		ChunkHash: hash,
		Plaintext: payload,
	})
	if err != nil {
		t.Fatalf("encode legacy payload: %v", err)
	}

	writer := container.NewLocalWriterWithDirAndDB(containersDir, container.GetContainerMaxSize(), dbconn)
	tx, err := dbconn.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("begin tx for legacy fixture: %v", err)
	}
	placement, err := writer.AppendPayload(tx, encoded.Payload)
	if err != nil {
		_ = tx.Rollback()
		t.Fatalf("append legacy payload: %v", err)
	}
	if err := container.UpdateContainerSize(tx, placement.ContainerID, placement.NewContainerSize); err != nil {
		_ = tx.Rollback()
		t.Fatalf("update container size for legacy fixture: %v", err)
	}

	if _, err := tx.Exec(
		`INSERT INTO blocks (chunk_id, codec, format_version, plaintext_size, stored_size, nonce, container_id, block_offset)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
		chunkID,
		string(encoded.Descriptor.Codec),
		encoded.Descriptor.FormatVersion,
		encoded.Descriptor.PlaintextSize,
		encoded.Descriptor.StoredSize,
		encoded.Descriptor.Nonce,
		placement.ContainerID,
		placement.Offset,
	); err != nil {
		_ = tx.Rollback()
		t.Fatalf("insert legacy blocks row: %v", err)
	}

	if err := tx.Commit(); err != nil {
		_ = tx.Rollback()
		t.Fatalf("commit legacy fixture: %v", err)
	}

	return chunkID
}

func TestVerifyRepositorySupportsLegacyOnlyRepo(t *testing.T) {
	dbconn := openVerifyTestDB(t)
	defer func() { _ = dbconn.Close() }()

	containersDir := t.TempDir()
	_ = seedVerifyLegacyBlockFixture(t, dbconn, containersDir, []byte("legacy-only-chunk"))

	if err := VerifyRepository(dbconn, containersDir); err != nil {
		t.Fatalf("expected VerifyRepository to pass for legacy-only repo, got: %v", err)
	}
}

func TestVerifyRepositorySupportsPackedOnlyRepo(t *testing.T) {
	dbconn := openVerifyTestDB(t)
	defer func() { _ = dbconn.Close() }()

	containersDir := t.TempDir()
	_, _ = seedVerifyPackedBlockFixture(t, dbconn, containersDir, [][]byte{[]byte("packed-only")}, nil)

	if err := VerifyRepository(dbconn, containersDir); err != nil {
		t.Fatalf("expected VerifyRepository to pass for packed-only repo, got: %v", err)
	}
}

func TestVerifyRepositorySupportsMixedRepo(t *testing.T) {
	dbconn := openVerifyTestDB(t)
	defer func() { _ = dbconn.Close() }()

	containersDir := t.TempDir()
	_ = seedVerifyLegacyBlockFixture(t, dbconn, containersDir, []byte("legacy-mixed"))
	_, _ = seedVerifyPackedBlockFixture(t, dbconn, containersDir, [][]byte{[]byte("packed-mixed")}, nil)

	if err := VerifyRepository(dbconn, containersDir); err != nil {
		t.Fatalf("expected VerifyRepository to pass for mixed repo, got: %v", err)
	}
}

func TestVerifyRepositorySupportsMixedBlockTypeMatrixInSingleRun(t *testing.T) {
	// Repository defaults are store-time hints and must not influence verify-time
	// decoding for already persisted rows.
	t.Setenv("COLDKEEP_CODEC", "plain")
	t.Setenv("COLDKEEP_COMPRESSION_CODEC", "none")
	t.Setenv("COLDKEEP_COMPRESSION_LEVEL", "1")
	t.Setenv("COLDKEEP_KEY", strings.Repeat("ab", 32))

	dbconn := openVerifyTestDB(t)
	defer func() { _ = dbconn.Close() }()

	containersDir := t.TempDir()

	// 1) legacy uncompressed block
	_ = seedVerifyLegacyBlockFixture(t, dbconn, containersDir, []byte("matrix-legacy-uncompressed"))

	// 2) new uncompressed block with all hashes present
	_, _ = seedVerifyCompressedPackedBlockFixture(t, dbconn, containersDir, [][]byte{[]byte("matrix-new-uncompressed-with-hashes")}, blocks.CodecPlain, storagecompression.CompressionNone)

	// 3) compressed unencrypted block
	_, _ = seedVerifyCompressedPackedBlockFixture(t, dbconn, containersDir, [][]byte{[]byte("matrix-compressed-unencrypted")}, blocks.CodecPlain, storagecompression.CompressionZstd)

	// 4) compressed encrypted block
	_, _ = seedVerifyCompressedPackedBlockFixture(t, dbconn, containersDir, [][]byte{[]byte("matrix-compressed-encrypted")}, blocks.CodecAESGCM, storagecompression.CompressionZstd)

	var legacyCount int64
	if err := dbconn.QueryRow(`
		SELECT COUNT(*)
		FROM blocks b
		WHERE NOT EXISTS (
			SELECT 1 FROM chunk_block_refs r WHERE r.chunk_id = b.chunk_id
		)
	`).Scan(&legacyCount); err != nil {
		t.Fatalf("count legacy blocks in matrix fixture: %v", err)
	}
	if legacyCount < 1 {
		t.Fatalf("expected at least one legacy block in mixed matrix fixture")
	}

	var newUncompressedWithHashes int64
	if err := dbconn.QueryRow(`
		SELECT COUNT(*)
		FROM storage_blocks
		WHERE codec = 'none'
		  AND lower(trim(compression_codec)) = 'none'
		  AND compressed_hash IS NOT NULL AND length(compressed_hash) > 0
		  AND physical_hash IS NOT NULL AND length(physical_hash) > 0
	`).Scan(&newUncompressedWithHashes); err != nil {
		t.Fatalf("count new uncompressed hashed blocks in matrix fixture: %v", err)
	}
	if newUncompressedWithHashes < 1 {
		t.Fatalf("expected at least one new uncompressed block with all hashes in mixed matrix fixture")
	}

	var compressedUnencryptedCount int64
	if err := dbconn.QueryRow(`
		SELECT COUNT(*)
		FROM storage_blocks
		WHERE codec = 'none' AND lower(trim(compression_codec)) = 'zstd'
	`).Scan(&compressedUnencryptedCount); err != nil {
		t.Fatalf("count compressed unencrypted blocks in matrix fixture: %v", err)
	}
	if compressedUnencryptedCount < 1 {
		t.Fatalf("expected at least one compressed unencrypted block in mixed matrix fixture")
	}

	var compressedEncryptedCount int64
	if err := dbconn.QueryRow(`
		SELECT COUNT(*)
		FROM storage_blocks
		WHERE codec = 'aes-gcm' AND lower(trim(compression_codec)) = 'zstd'
	`).Scan(&compressedEncryptedCount); err != nil {
		t.Fatalf("count compressed encrypted blocks in matrix fixture: %v", err)
	}
	if compressedEncryptedCount < 1 {
		t.Fatalf("expected at least one compressed encrypted block in mixed matrix fixture")
	}

	if err := VerifyRepository(dbconn, containersDir); err != nil {
		t.Fatalf("expected one VerifyRepository run to handle mixed legacy/new/compressed/encrypted blocks, got: %v", err)
	}
}

func TestVerifyBlockPayloadsDetectsSegmentOutOfBounds(t *testing.T) {
	dbconn := openVerifyTestDB(t)
	defer func() { _ = dbconn.Close() }()

	containersDir := t.TempDir()
	blockID, chunkIDs := seedVerifyPackedBlockFixture(t, dbconn, containersDir, [][]byte{[]byte("ABCD")}, nil)

	if _, err := dbconn.Exec(`DELETE FROM chunk_block_refs WHERE chunk_id = $1`, chunkIDs[0]); err != nil {
		t.Fatalf("clear default refs for bounds test: %v", err)
	}
	if _, err := dbconn.Exec(
		`INSERT INTO chunk_block_refs (chunk_id, block_id, offset_in_block, size_in_block)
		 VALUES ($1, $2, $3, $4)`,
		chunkIDs[0],
		blockID,
		int64(1),
		int64(4),
	); err != nil {
		t.Fatalf("insert out-of-bounds ref: %v", err)
	}

	err := verifyBlockPayloads(dbconn, containersDir)
	if err == nil || (!strings.Contains(err.Error(), "segment out of payload bounds") && !strings.Contains(err.Error(), "chunk_block_ref references chunk not in encoded block table")) {
		t.Fatalf("expected out-of-bounds-or-table-mismatch error, got: %v", err)
	}
}

func TestVerifyBlockPayloadsDetectsSegmentSizeBeyondPayload(t *testing.T) {
	dbconn := openVerifyTestDB(t)
	defer func() { _ = dbconn.Close() }()

	containersDir := t.TempDir()
	blockID, chunkIDs := seedVerifyPackedBlockFixture(t, dbconn, containersDir, [][]byte{[]byte("ABCD")}, nil)

	if _, err := dbconn.Exec(`DELETE FROM chunk_block_refs WHERE chunk_id = $1`, chunkIDs[0]); err != nil {
		t.Fatalf("clear default refs for size-beyond-payload test: %v", err)
	}
	if _, err := dbconn.Exec(
		`INSERT INTO chunk_block_refs (chunk_id, block_id, offset_in_block, size_in_block)
		 VALUES ($1, $2, $3, $4)`,
		chunkIDs[0],
		blockID,
		int64(0),
		int64(99),
	); err != nil {
		t.Fatalf("insert size-beyond-payload ref: %v", err)
	}

	err := verifyBlockPayloads(dbconn, containersDir)
	if err == nil || (!strings.Contains(err.Error(), "segment out of payload bounds") && !strings.Contains(err.Error(), "chunk_block_ref references chunk not in encoded block table") && !strings.Contains(err.Error(), "size mismatch")) {
		t.Fatalf("expected size-beyond-payload-or-table-mismatch error, got: %v", err)
	}
}

func TestVerifyBlockPayloadsDetectsSegmentOffsetSizeOverflow(t *testing.T) {
	dbconn := openVerifyTestDB(t)
	defer func() { _ = dbconn.Close() }()

	containersDir := t.TempDir()
	blockID, chunkIDs := seedVerifyPackedBlockFixture(t, dbconn, containersDir, [][]byte{[]byte("ABCD")}, nil)

	if _, err := dbconn.Exec(`DELETE FROM chunk_block_refs WHERE chunk_id = $1`, chunkIDs[0]); err != nil {
		t.Fatalf("clear default refs for overflow test: %v", err)
	}
	if _, err := dbconn.Exec(
		`INSERT INTO chunk_block_refs (chunk_id, block_id, offset_in_block, size_in_block)
		 VALUES ($1, $2, $3, $4)`,
		chunkIDs[0],
		blockID,
		int64(math.MaxInt64-1),
		int64(64),
	); err != nil {
		t.Fatalf("insert overflow ref: %v", err)
	}

	err := verifyBlockPayloads(dbconn, containersDir)
	if err == nil || (!strings.Contains(err.Error(), "segment out of payload bounds") && !strings.Contains(err.Error(), "chunk_block_ref references chunk not in encoded block table") && !strings.Contains(err.Error(), "size mismatch")) {
		t.Fatalf("expected overflow-or-table-mismatch error, got: %v", err)
	}
}

func TestVerifyChunkBlockRefsDetectsZeroSizeSegment(t *testing.T) {
	dbconn := openVerifyTestDB(t)
	defer func() { _ = dbconn.Close() }()

	containersDir := t.TempDir()
	_, chunkIDs := seedVerifyPackedBlockFixture(t, dbconn, containersDir, [][]byte{[]byte("ABCD")}, nil)

	if _, err := dbconn.Exec(`PRAGMA ignore_check_constraints = ON`); err != nil {
		t.Fatalf("disable sqlite check constraints: %v", err)
	}
	if _, err := dbconn.Exec(`UPDATE chunk_block_refs SET size_in_block = 0 WHERE chunk_id = $1`, chunkIDs[0]); err != nil {
		t.Fatalf("force zero-size segment row: %v", err)
	}

	err := verifyChunkBlockRefs(dbconn)
	if err == nil || !strings.Contains(err.Error(), "invalid chunk_block_refs ranges") {
		t.Fatalf("expected zero-size segment range error, got: %v", err)
	}
}

func TestVerifyBlockPayloadsDetectsBadBlockMagic(t *testing.T) {
	dbconn := openVerifyTestDB(t)
	defer func() { _ = dbconn.Close() }()

	containersDir := t.TempDir()
	blockID, _ := seedVerifyPackedBlockFixture(t, dbconn, containersDir, [][]byte{[]byte("ABCD")}, nil)

	path, offset, storedSize, _ := packedFixtureBlockStorageMeta(t, dbconn, blockID, containersDir)
	payload := readPackedStoredBytesForTest(t, path, offset, storedSize)
	binary.LittleEndian.PutUint32(payload[0:4], uint32(0x00000000))
	overwritePackedStoredBytesForTest(t, path, offset, payload)
	setPackedBlockHashForBytes(t, dbconn, blockID, payload)

	err := verifyBlockPayloads(dbconn, containersDir)
	if err == nil || !strings.Contains(err.Error(), "decode logical block") {
		t.Fatalf("expected bad magic decode error, got: %v", err)
	}
}

func TestVerifyBlockPayloadsDetectsUnsupportedBlockVersion(t *testing.T) {
	dbconn := openVerifyTestDB(t)
	defer func() { _ = dbconn.Close() }()

	containersDir := t.TempDir()
	blockID, _ := seedVerifyPackedBlockFixture(t, dbconn, containersDir, [][]byte{[]byte("ABCD")}, nil)

	path, offset, storedSize, _ := packedFixtureBlockStorageMeta(t, dbconn, blockID, containersDir)
	payload := readPackedStoredBytesForTest(t, path, offset, storedSize)
	binary.LittleEndian.PutUint16(payload[4:6], uint16(99))
	overwritePackedStoredBytesForTest(t, path, offset, payload)
	setPackedBlockHashForBytes(t, dbconn, blockID, payload)

	err := verifyBlockPayloads(dbconn, containersDir)
	if err == nil || !strings.Contains(err.Error(), "decode logical block") {
		t.Fatalf("expected unsupported version decode error, got: %v", err)
	}
}

func TestVerifyBlockPayloadsDetectsTruncatedEncodedBlock(t *testing.T) {
	dbconn := openVerifyTestDB(t)
	defer func() { _ = dbconn.Close() }()

	containersDir := t.TempDir()
	blockID, _ := seedVerifyPackedBlockFixture(t, dbconn, containersDir, [][]byte{[]byte("ABCD")}, nil)

	path, offset, storedSize, _ := packedFixtureBlockStorageMeta(t, dbconn, blockID, containersDir)
	payload := readPackedStoredBytesForTest(t, path, offset, storedSize)
	if len(payload) < 8 {
		t.Fatalf("fixture payload unexpectedly small: %d", len(payload))
	}
	truncated := payload[:len(payload)-5]

	if _, err := dbconn.Exec(`UPDATE storage_blocks SET stored_size = $1, plaintext_size = $2 WHERE id = $3`, int64(len(truncated)), int64(len(truncated)), blockID); err != nil {
		t.Fatalf("update storage block sizes to truncated length: %v", err)
	}
	overwritePackedStoredBytesForTest(t, path, offset, truncated)
	setPackedBlockHashForBytes(t, dbconn, blockID, truncated)

	err := verifyBlockPayloads(dbconn, containersDir)
	if err == nil || !strings.Contains(err.Error(), "decode logical block") {
		t.Fatalf("expected truncated encoded block decode error, got: %v", err)
	}
}

func TestVerifyBlockPayloadsDetectsCorruptedStoredBytes(t *testing.T) {
	dbconn := openVerifyTestDB(t)
	defer func() { _ = dbconn.Close() }()

	containersDir := t.TempDir()
	blockID, _ := seedVerifyPackedBlockFixture(t, dbconn, containersDir, [][]byte{[]byte("ABCD")}, nil)

	path, offset, storedSize, _ := packedFixtureBlockStorageMeta(t, dbconn, blockID, containersDir)
	payload := readPackedStoredBytesForTest(t, path, offset, storedSize)
	payload[len(payload)-1] ^= 0xFF
	overwritePackedStoredBytesForTest(t, path, offset, payload)
	setPackedBlockHashForBytes(t, dbconn, blockID, payload)

	err := verifyBlockPayloads(dbconn, containersDir)
	if err == nil || !strings.Contains(err.Error(), "hash mismatch") {
		t.Fatalf("expected corrupted stored bytes chunk-hash mismatch error, got: %v", err)
	}
}

func TestVerifyBlockPayloadsPassesWithPhase2HashesPresent(t *testing.T) {
	dbconn := openVerifyTestDB(t)
	defer func() { _ = dbconn.Close() }()

	containersDir := t.TempDir()
	blockID, _ := seedVerifyPackedBlockFixture(t, dbconn, containersDir, [][]byte{[]byte("ABCD")}, nil)

	path, offset, storedSize, _ := packedFixtureBlockStorageMeta(t, dbconn, blockID, containersDir)
	payload := readPackedStoredBytesForTest(t, path, offset, storedSize)
	logical := blocks.HashLogical(payload)
	compressed := blocks.HashCompressed(payload)
	physical := blocks.HashPhysical(payload)

	if _, err := dbconn.Exec(
		`UPDATE storage_blocks SET block_hash = $1, compressed_hash = $2, physical_hash = $3 WHERE id = $4`,
		logical,
		compressed,
		physical,
		blockID,
	); err != nil {
		t.Fatalf("set phase2 hash fields: %v", err)
	}

	if err := verifyBlockPayloads(dbconn, containersDir); err != nil {
		t.Fatalf("expected verifyBlockPayloads to pass with phase2 hashes present, got: %v", err)
	}
}

func TestVerifyBlockPayloadsDetectsPhysicalHashMismatchStage(t *testing.T) {
	dbconn := openVerifyTestDB(t)
	defer func() { _ = dbconn.Close() }()

	containersDir := t.TempDir()
	blockID, _ := seedVerifyPackedBlockFixture(t, dbconn, containersDir, [][]byte{[]byte("ABCD")}, nil)

	path, offset, storedSize, _ := packedFixtureBlockStorageMeta(t, dbconn, blockID, containersDir)
	payload := readPackedStoredBytesForTest(t, path, offset, storedSize)
	logical := blocks.HashLogical(payload)
	compressed := blocks.HashCompressed(payload)
	physical := blocks.HashPhysical(payload)
	physical[0] ^= 0xFF

	if _, err := dbconn.Exec(
		`UPDATE storage_blocks SET block_hash = $1, compressed_hash = $2, physical_hash = $3 WHERE id = $4`,
		logical,
		compressed,
		physical,
		blockID,
	); err != nil {
		t.Fatalf("set hash fields for physical mismatch test: %v", err)
	}

	err := verifyBlockPayloads(dbconn, containersDir)
	if err == nil || !strings.Contains(err.Error(), verifyErrPhysicalHashMismatch) {
		t.Fatalf("expected physical hash mismatch category, got: %v", err)
	}
}

func TestVerifyBlockPayloadsDetectsCompressedHashMismatchStage(t *testing.T) {
	dbconn := openVerifyTestDB(t)
	defer func() { _ = dbconn.Close() }()

	containersDir := t.TempDir()
	blockID, _ := seedVerifyPackedBlockFixture(t, dbconn, containersDir, [][]byte{[]byte("ABCD")}, nil)

	path, offset, storedSize, _ := packedFixtureBlockStorageMeta(t, dbconn, blockID, containersDir)
	payload := readPackedStoredBytesForTest(t, path, offset, storedSize)
	logical := blocks.HashLogical(payload)
	compressed := blocks.HashCompressed(payload)
	physical := blocks.HashPhysical(payload)
	compressed[0] ^= 0xFF

	if _, err := dbconn.Exec(
		`UPDATE storage_blocks SET block_hash = $1, compressed_hash = $2, physical_hash = $3 WHERE id = $4`,
		logical,
		compressed,
		physical,
		blockID,
	); err != nil {
		t.Fatalf("set hash fields for compressed mismatch test: %v", err)
	}

	err := verifyBlockPayloads(dbconn, containersDir)
	if err == nil || !strings.Contains(err.Error(), verifyErrCompressedHashMismatch) {
		t.Fatalf("expected compressed hash mismatch category, got: %v", err)
	}
}

func TestVerifyBlockPayloadsDetectsPhysicalHashMismatchStageOnCompressedBlock(t *testing.T) {
	dbconn := openVerifyTestDB(t)
	defer func() { _ = dbconn.Close() }()

	containersDir := t.TempDir()
	blockID, _ := seedVerifyCompressedPackedBlockFixture(t, dbconn, containersDir, [][]byte{[]byte("compress-physical")}, blocks.CodecPlain, storagecompression.CompressionZstd)

	if _, err := dbconn.Exec(`UPDATE storage_blocks SET physical_hash = zeroblob(32) WHERE id = $1`, blockID); err != nil {
		t.Fatalf("mutate physical_hash: %v", err)
	}

	err := verifyBlockPayloads(dbconn, containersDir)
	if err == nil || !strings.HasPrefix(err.Error(), verifyErrPhysicalHashMismatch+":") {
		t.Fatalf("expected compressed-block physical hash mismatch category, got: %v", err)
	}
}

func TestVerifyBlockPayloadsDetectsDecryptFailureWithLegacyNullPhysicalHashOnCompressedAESBlock(t *testing.T) {
	t.Setenv("COLDKEEP_KEY", strings.Repeat("ab", 32))
	dbconn := openVerifyTestDB(t)
	defer func() { _ = dbconn.Close() }()

	containersDir := t.TempDir()
	blockID, _ := seedVerifyCompressedPackedBlockFixture(t, dbconn, containersDir, [][]byte{[]byte("compress-auth")}, blocks.CodecAESGCM, storagecompression.CompressionZstd)

	path, offset, storedSize, _ := packedFixtureBlockStorageMeta(t, dbconn, blockID, containersDir)
	payload := readPackedStoredBytesForTest(t, path, offset, storedSize)
	payload[packedStorageBlockAESGCMNonceSize] ^= 0xFF
	overwritePackedStoredBytesForTest(t, path, offset, payload)

	if _, err := dbconn.Exec(`UPDATE storage_blocks SET physical_hash = NULL WHERE id = $1`, blockID); err != nil {
		t.Fatalf("set physical_hash null: %v", err)
	}

	err := verifyBlockPayloads(dbconn, containersDir)
	if err == nil || !strings.HasPrefix(err.Error(), "metadata_invalid:") {
		t.Fatalf("expected metadata_invalid category for decrypt failure, got: %v", err)
	}
	if !strings.Contains(err.Error(), "decrypt/transform decode failed") || !strings.Contains(err.Error(), "cipher: message authentication failed") {
		t.Fatalf("expected precise decrypt/auth diagnostic, got: %v", err)
	}
}

func TestVerifyBlockPayloadsDetectsCompressedHashMismatchStageOnCompressedBlock(t *testing.T) {
	dbconn := openVerifyTestDB(t)
	defer func() { _ = dbconn.Close() }()

	containersDir := t.TempDir()
	blockID, _ := seedVerifyCompressedPackedBlockFixture(t, dbconn, containersDir, [][]byte{bytes.Repeat([]byte("compress-hash-payload-"), 128)}, blocks.CodecPlain, storagecompression.CompressionZstd)

	path, offset, storedSize, plaintextSize := packedFixtureBlockStorageMeta(t, dbconn, blockID, containersDir)
	originalCompressed := readPackedStoredBytesForTest(t, path, offset, storedSize)
	defaultCompressor, err := storagecompression.NewZstdCompressor(3)
	if err != nil {
		t.Fatalf("create default zstd compressor: %v", err)
	}
	logicalBytes, err := defaultCompressor.Decompress(originalCompressed, plaintextSize)
	if err != nil {
		t.Fatalf("decompress original compressed bytes: %v", err)
	}
	mid := len(logicalBytes) / 2
	firstFrame, err := defaultCompressor.Compress(logicalBytes[:mid])
	if err != nil {
		t.Fatalf("compress first zstd frame: %v", err)
	}
	secondFrame, err := defaultCompressor.Compress(logicalBytes[mid:])
	if err != nil {
		t.Fatalf("compress second zstd frame: %v", err)
	}
	payload := append(append([]byte(nil), firstFrame...), secondFrame...)
	if bytes.Equal(payload, originalCompressed) {
		t.Fatal("expected alternate zstd stream to differ from original fixture bytes")
	}
	overwritePackedStoredBytesForTest(t, path, offset, payload)

	if _, err := dbconn.Exec(`
		UPDATE storage_blocks
		SET stored_size = $1,
		    compressed_size = $2,
		    physical_hash = $3
		WHERE id = $4
	`, int64(len(payload)), int64(len(payload)), blocks.HashPhysical(payload), blockID); err != nil {
		t.Fatalf("update physical_hash for compressed hash mismatch: %v", err)
	}

	err = verifyBlockPayloads(dbconn, containersDir)
	if err == nil || !strings.HasPrefix(err.Error(), verifyErrCompressedHashMismatch+":") {
		t.Fatalf("expected compressed hash mismatch category, got: %v", err)
	}
}

func TestVerifyBlockPayloadsDetectsDecompressionFailureOnCompressedBlock(t *testing.T) {
	dbconn := openVerifyTestDB(t)
	defer func() { _ = dbconn.Close() }()

	containersDir := t.TempDir()
	blockID, _ := seedVerifyCompressedPackedBlockFixture(t, dbconn, containersDir, [][]byte{[]byte("compress-decompress")}, blocks.CodecPlain, storagecompression.CompressionZstd)

	path, offset, _, _ := packedFixtureBlockStorageMeta(t, dbconn, blockID, containersDir)
	corruptedCompressed := []byte("not-a-valid-zstd-stream")
	overwritePackedStoredBytesForTest(t, path, offset, corruptedCompressed)

	if _, err := dbconn.Exec(`
		UPDATE storage_blocks
		SET stored_size = $1,
		    compressed_size = $2,
		    physical_hash = $3,
		    compressed_hash = $4
		WHERE id = $5
	`, int64(len(corruptedCompressed)), int64(len(corruptedCompressed)), blocks.HashPhysical(corruptedCompressed), blocks.HashCompressed(corruptedCompressed), blockID); err != nil {
		t.Fatalf("update metadata for decompression failure fixture: %v", err)
	}

	err := verifyBlockPayloads(dbconn, containersDir)
	if err == nil || !strings.HasPrefix(err.Error(), "metadata_invalid:") {
		t.Fatalf("expected metadata_invalid category for decompression failure, got: %v", err)
	}
	if !strings.Contains(err.Error(), "decompress codec=zstd") {
		t.Fatalf("expected precise decompression diagnostic, got: %v", err)
	}
}

func TestVerifyBlockPayloadsDetectsLogicalHashMismatchStageOnCompressedBlock(t *testing.T) {
	dbconn := openVerifyTestDB(t)
	defer func() { _ = dbconn.Close() }()

	containersDir := t.TempDir()
	blockID, _ := seedVerifyCompressedPackedBlockFixture(t, dbconn, containersDir, [][]byte{[]byte("compress-logical")}, blocks.CodecPlain, storagecompression.CompressionZstd)

	path, offset, storedSize, plaintextSize := packedFixtureBlockStorageMeta(t, dbconn, blockID, containersDir)
	storedPayload := readPackedStoredBytesForTest(t, path, offset, storedSize)
	compressor, err := storagecompression.NewZstdCompressor(3)
	if err != nil {
		t.Fatalf("create zstd compressor: %v", err)
	}
	logicalBytes, err := compressor.Decompress(storedPayload, plaintextSize)
	if err != nil {
		t.Fatalf("decompress original compressed fixture: %v", err)
	}
	logicalBytes[0] ^= 0x01
	recompressed, err := compressor.Compress(logicalBytes)
	if err != nil {
		t.Fatalf("recompress mutated logical fixture: %v", err)
	}
	overwritePackedStoredBytesForTest(t, path, offset, recompressed)

	if _, err := dbconn.Exec(`
		UPDATE storage_blocks
		SET stored_size = $1,
		    compressed_size = $2,
		    physical_hash = $3,
		    compressed_hash = $4
		WHERE id = $5
	`, int64(len(recompressed)), int64(len(recompressed)), blocks.HashPhysical(recompressed), blocks.HashCompressed(recompressed), blockID); err != nil {
		t.Fatalf("update metadata for logical hash mismatch fixture: %v", err)
	}

	err = verifyBlockPayloads(dbconn, containersDir)
	if err == nil || !strings.HasPrefix(err.Error(), verifyErrBlockHashMismatch+":") {
		t.Fatalf("expected logical block hash mismatch category, got: %v", err)
	}
}

func TestVerifyBlockPayloadsDetectsChunkSizeMismatch(t *testing.T) {
	dbconn := openVerifyTestDB(t)
	defer func() { _ = dbconn.Close() }()

	containersDir := t.TempDir()
	// Create one chunk with payload size 4, then change chunk.size to 5 to force mismatch.
	blockID, _ := seedVerifyPackedBlockFixture(t, dbconn, containersDir, [][]byte{[]byte("WXYZ")}, nil)

	if _, err := dbconn.Exec(`
		UPDATE chunk
		SET size = 5
		WHERE id = (SELECT chunk_id FROM chunk_block_refs WHERE block_id = $1 LIMIT 1)
	`, blockID); err != nil {
		t.Fatalf("mutate chunk size for mismatch test: %v", err)
	}

	err := verifyBlockPayloads(dbconn, containersDir)
	if err == nil || !strings.HasPrefix(err.Error(), "metadata_invalid:") || !strings.Contains(err.Error(), "size mismatch") {
		t.Fatalf("expected chunk size mismatch error, got: %v", err)
	}
}

func TestVerifyBlockPayloadsRequiresBlockHashMatch(t *testing.T) {
	dbconn := openVerifyTestDB(t)
	defer func() { _ = dbconn.Close() }()

	containersDir := t.TempDir()
	blockID, _ := seedVerifyPackedBlockFixture(t, dbconn, containersDir, [][]byte{[]byte("HASH")}, nil)

	if _, err := dbconn.Exec(`UPDATE storage_blocks SET block_hash = zeroblob(32) WHERE id = $1`, blockID); err != nil {
		t.Fatalf("mutate block hash: %v", err)
	}

	err := verifyBlockPayloads(dbconn, containersDir)
	if err == nil || !strings.Contains(err.Error(), "hash mismatch") {
		t.Fatalf("expected mandatory block hash mismatch error, got: %v", err)
	}
}

func TestVerifyBlockPayloadsDetectsDecodedPayloadSizeMismatch(t *testing.T) {
	dbconn := openVerifyTestDB(t)
	defer func() { _ = dbconn.Close() }()

	containersDir := t.TempDir()
	blockID, _ := seedVerifyPackedBlockFixture(t, dbconn, containersDir, [][]byte{[]byte("SIZE")}, nil)

	if _, err := dbconn.Exec(`UPDATE storage_blocks SET plaintext_size = plaintext_size + 1 WHERE id = $1`, blockID); err != nil {
		t.Fatalf("mutate plaintext_size metadata: %v", err)
	}

	err := verifyBlockPayloads(dbconn, containersDir)
	if err == nil || !strings.HasPrefix(err.Error(), "metadata_invalid:") || !strings.Contains(err.Error(), "plaintext size mismatch") {
		t.Fatalf("expected decoded payload size mismatch error, got: %v", err)
	}
}

func TestVerifyBlockPayloadsDetectsDecodedChunkCountMismatchAgainstRefs(t *testing.T) {
	dbconn := openVerifyTestDB(t)
	defer func() { _ = dbconn.Close() }()

	containersDir := t.TempDir()
	blockID, chunkIDs := seedVerifyPackedBlockFixture(t, dbconn, containersDir, [][]byte{[]byte("aa"), []byte("bb")}, nil)

	if _, err := dbconn.Exec(`DELETE FROM chunk_block_refs WHERE block_id = $1 AND chunk_id = $2`, blockID, chunkIDs[0]); err != nil {
		t.Fatalf("delete one chunk ref: %v", err)
	}

	err := verifyBlockPayloads(dbconn, containersDir)
	if err == nil || !strings.Contains(err.Error(), "encoded block table contains chunk not in chunk_block_refs") {
		t.Fatalf("expected encoded-table missing-ref error, got: %v", err)
	}
}

func TestVerifyBlockPayloadsDetectsChunkBlockRefChunkNotInEncodedTable(t *testing.T) {
	dbconn := openVerifyTestDB(t)
	defer func() { _ = dbconn.Close() }()

	containersDir := t.TempDir()
	blockID, chunkIDs := seedVerifyPackedBlockFixture(t, dbconn, containersDir, [][]byte{[]byte("ABCD")}, nil)

	var otherChunkID int64
	if err := dbconn.QueryRow(
		`INSERT INTO chunk (chunk_hash, size, status, live_ref_count, pin_count, retry_count, chunker_version)
		 VALUES ($1, $2, $3, 0, 0, 0, 'v1-simple-rolling')
		 RETURNING id`,
		strings.Repeat("9", 64),
		int64(4),
		filestate.ChunkAborted,
	).Scan(&otherChunkID); err != nil {
		t.Fatalf("insert non-encoded chunk for ref mismatch: %v", err)
	}

	if _, err := dbconn.Exec(`DELETE FROM chunk_block_refs WHERE chunk_id = $1`, chunkIDs[0]); err != nil {
		t.Fatalf("delete original chunk ref: %v", err)
	}
	if _, err := dbconn.Exec(
		`INSERT INTO chunk_block_refs (chunk_id, block_id, offset_in_block, size_in_block)
		 VALUES ($1, $2, $3, $4)`,
		otherChunkID,
		blockID,
		int64(0),
		int64(4),
	); err != nil {
		t.Fatalf("insert chunk ref not present in encoded table: %v", err)
	}

	err := verifyBlockPayloads(dbconn, containersDir)
	if err == nil || !strings.Contains(err.Error(), "chunk_block_ref references chunk not in encoded block table") {
		t.Fatalf("expected chunk_block_ref-not-in-table error, got: %v", err)
	}
}

func TestVerifyBlockPayloadsDetectsDecodedChunkSliceHashMismatch(t *testing.T) {
	dbconn := openVerifyTestDB(t)
	defer func() { _ = dbconn.Close() }()

	containersDir := t.TempDir()
	_, chunkIDs := seedVerifyPackedBlockFixture(t, dbconn, containersDir, [][]byte{[]byte("alpha"), []byte("beta")}, nil)

	if _, err := dbconn.Exec(`UPDATE chunk SET chunk_hash = $1 WHERE id = $2`, strings.Repeat("0", 64), chunkIDs[0]); err != nil {
		t.Fatalf("mutate chunk hash: %v", err)
	}

	err := verifyBlockPayloads(dbconn, containersDir)
	if err == nil || !strings.Contains(err.Error(), "hash mismatch") {
		t.Fatalf("expected decoded chunk slice hash mismatch error, got: %v", err)
	}
}

func TestVerifyBlockPayloadsStrictModeRequiresExactEncodedChunkTable(t *testing.T) {
	dbconn := openVerifyTestDB(t)
	defer func() { _ = dbconn.Close() }()

	old := os.Getenv("COLDKEEP_VERIFY_STRICT_SEGMENTS")
	if err := os.Setenv("COLDKEEP_VERIFY_STRICT_SEGMENTS", "1"); err != nil {
		t.Fatalf("set strict env: %v", err)
	}
	defer func() {
		if old == "" {
			_ = os.Unsetenv("COLDKEEP_VERIFY_STRICT_SEGMENTS")
		} else {
			_ = os.Setenv("COLDKEEP_VERIFY_STRICT_SEGMENTS", old)
		}
	}()

	containersDir := t.TempDir()
	blockID, _ := seedVerifyPackedBlockFixture(t, dbconn, containersDir, [][]byte{[]byte("aa"), []byte("bb")}, nil)

	rows, err := dbconn.Query(`SELECT chunk_id, offset_in_block, size_in_block FROM chunk_block_refs WHERE block_id = $1 ORDER BY offset_in_block`, blockID)
	if err != nil {
		t.Fatalf("query refs for strict mismatch setup: %v", err)
	}

	var firstChunkID, secondChunkID int64
	var firstOffset, secondOffset int64
	var firstSize, secondSize int64
	if rows.Next() {
		if err := rows.Scan(&firstChunkID, &firstOffset, &firstSize); err != nil {
			_ = rows.Close()
			t.Fatalf("scan first ref: %v", err)
		}
	}
	if rows.Next() {
		if err := rows.Scan(&secondChunkID, &secondOffset, &secondSize); err != nil {
			_ = rows.Close()
			t.Fatalf("scan second ref: %v", err)
		}
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		t.Fatalf("iterate refs for strict mismatch setup: %v", err)
	}
	if err := rows.Close(); err != nil {
		t.Fatalf("close refs query for strict mismatch setup: %v", err)
	}

	if _, err := dbconn.Exec(`DELETE FROM chunk_block_refs WHERE block_id = $1`, blockID); err != nil {
		t.Fatalf("clear refs for strict mismatch setup: %v", err)
	}
	// Reinsert same offsets/sizes but swapped chunk IDs; non-strict bounds still valid,
	// strict mode must fail because encoded chunk table order/ids differ.
	if _, err := dbconn.Exec(
		`INSERT INTO chunk_block_refs (chunk_id, block_id, offset_in_block, size_in_block) VALUES ($1, $2, $3, $4), ($5, $2, $6, $7)`,
		secondChunkID, blockID, firstOffset, firstSize,
		firstChunkID, secondOffset, secondSize,
	); err != nil {
		t.Fatalf("insert swapped refs for strict mismatch: %v", err)
	}

	err = verifyBlockPayloads(dbconn, filepath.Clean(containersDir))
	if err == nil || (!strings.Contains(err.Error(), "strict mode") && !strings.Contains(err.Error(), "chunk_block_ref references chunk not in encoded block table")) {
		t.Fatalf("expected strict-or-mandatory table mismatch error, got: %v", err)
	}
}

func TestVerifyRepositoryErrorCategoryMetadataMissing(t *testing.T) {
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
		strings.Repeat("a", 64),
		int64(17),
		filestate.ChunkAborted,
		int64(0),
		int64(0),
		int64(0),
		"v1-simple-rolling",
	).Scan(&chunkID); err != nil {
		t.Fatalf("insert chunk: %v", err)
	}

	if _, err := dbconn.Exec(`INSERT INTO chunk_block_refs (chunk_id, block_id, offset_in_block, size_in_block) VALUES ($1, 9999, 0, 17)`, chunkID); err != nil {
		t.Fatalf("insert orphan chunk_block_ref: %v", err)
	}

	err := VerifyRepository(dbconn, t.TempDir())
	if err == nil || !strings.HasPrefix(err.Error(), "metadata_missing:") {
		t.Fatalf("expected metadata_missing category prefix, got: %v", err)
	}
}

func TestVerifyRepositoryErrorCategoryMetadataInvalid(t *testing.T) {
	dbconn := openVerifyTestDB(t)
	defer func() { _ = dbconn.Close() }()

	var containerID int64
	if err := dbconn.QueryRow(
		`INSERT INTO container (filename, current_size, max_size, sealed, quarantine)
		 VALUES ($1, $2, $3, FALSE, FALSE)
		 RETURNING id`,
		"verify-invalid-storage-metadata-category.bin",
		int64(4096),
		int64(4096),
	).Scan(&containerID); err != nil {
		t.Fatalf("insert container: %v", err)
	}

	if _, err := dbconn.Exec(
		`INSERT INTO storage_blocks (format_version, codec, plaintext_size, stored_size, container_id, container_offset, block_hash)
		 VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		2,
		"none",
		int64(16),
		int64(16),
		containerID,
		int64(0),
		[]byte{0x01, 0x02},
	); err != nil {
		t.Fatalf("insert invalid storage_blocks row: %v", err)
	}

	err := VerifyRepository(dbconn, t.TempDir())
	if err == nil || !strings.HasPrefix(err.Error(), "metadata_invalid:") {
		t.Fatalf("expected metadata_invalid category prefix, got: %v", err)
	}
}

func TestVerifyRepositoryErrorCategoryEmptyBlockHash(t *testing.T) {
	dbconn := openVerifyTestDB(t)
	defer func() { _ = dbconn.Close() }()

	var containerID int64
	if err := dbconn.QueryRow(
		`INSERT INTO container (filename, current_size, max_size, sealed, quarantine)
		 VALUES ($1, $2, $3, FALSE, FALSE)
		 RETURNING id`,
		"verify-empty-block-hash-category.bin",
		int64(4096),
		int64(4096),
	).Scan(&containerID); err != nil {
		t.Fatalf("insert container: %v", err)
	}

	if _, err := dbconn.Exec(
		`INSERT INTO storage_blocks (format_version, codec, plaintext_size, stored_size, container_id, container_offset, block_hash)
		 VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		1,
		"none",
		int64(16),
		int64(16),
		containerID,
		int64(0),
		[]byte{},
	); err != nil {
		t.Fatalf("insert empty-hash storage_blocks row: %v", err)
	}

	err := VerifyRepository(dbconn, t.TempDir())
	if err == nil || !strings.HasPrefix(err.Error(), "metadata_invalid:") {
		t.Fatalf("expected metadata_invalid category prefix, got: %v", err)
	}
	if !strings.Contains(err.Error(), "empty block_hash") {
		t.Fatalf("expected explicit empty block_hash error, got: %v", err)
	}
}

func TestVerifyRepositoryErrorCategoryBlockHashMismatch(t *testing.T) {
	dbconn := openVerifyTestDB(t)
	defer func() { _ = dbconn.Close() }()

	containersDir := t.TempDir()
	blockID, _ := seedVerifyPackedBlockFixture(t, dbconn, containersDir, [][]byte{[]byte("HASH")}, nil)

	if _, err := dbconn.Exec(`UPDATE storage_blocks SET block_hash = zeroblob(32) WHERE id = $1`, blockID); err != nil {
		t.Fatalf("mutate block hash: %v", err)
	}

	err := verifyBlockPayloads(dbconn, containersDir)
	if err == nil || !strings.HasPrefix(err.Error(), "block_hash_mismatch:") {
		t.Fatalf("expected block_hash_mismatch category prefix, got: %v", err)
	}
}

func TestVerifyRepositoryErrorCategoryChunkHashMismatch(t *testing.T) {
	dbconn := openVerifyTestDB(t)
	defer func() { _ = dbconn.Close() }()

	containersDir := t.TempDir()
	_, chunkIDs := seedVerifyPackedBlockFixture(t, dbconn, containersDir, [][]byte{[]byte("alpha"), []byte("beta")}, nil)

	if _, err := dbconn.Exec(`UPDATE chunk SET chunk_hash = $1 WHERE id = $2`, strings.Repeat("0", 64), chunkIDs[0]); err != nil {
		t.Fatalf("mutate chunk hash: %v", err)
	}

	err := verifyBlockPayloads(dbconn, containersDir)
	if err == nil || !strings.HasPrefix(err.Error(), "chunk_hash_mismatch:") {
		t.Fatalf("expected chunk_hash_mismatch category prefix, got: %v", err)
	}
}

func TestVerifyRepositoryErrorCategoryUnsupportedBlockFormat(t *testing.T) {
	dbconn := openVerifyTestDB(t)
	defer func() { _ = dbconn.Close() }()

	containersDir := t.TempDir()
	blockID, _ := seedVerifyPackedBlockFixture(t, dbconn, containersDir, [][]byte{[]byte("ABCD")}, nil)

	path, offset, storedSize, _ := packedFixtureBlockStorageMeta(t, dbconn, blockID, containersDir)
	payload := readPackedStoredBytesForTest(t, path, offset, storedSize)
	binary.LittleEndian.PutUint32(payload[0:4], uint32(0x00000000))
	overwritePackedStoredBytesForTest(t, path, offset, payload)
	setPackedBlockHashForBytes(t, dbconn, blockID, payload)

	err := verifyBlockPayloads(dbconn, containersDir)
	if err == nil || !strings.HasPrefix(err.Error(), "unsupported_block_format:") {
		t.Fatalf("expected unsupported_block_format category prefix, got: %v", err)
	}
}

func TestVerifyRepositoryErrorCategoryPhysicalMissing(t *testing.T) {
	dbconn := openVerifyTestDB(t)
	defer func() { _ = dbconn.Close() }()

	var containerID int64
	if err := dbconn.QueryRow(
		`INSERT INTO container (filename, current_size, max_size, sealed, quarantine)
		 VALUES ($1, $2, $3, FALSE, FALSE)
		 RETURNING id`,
		"missing-legacy-container.bin",
		int64(1024),
		int64(1024),
	).Scan(&containerID); err != nil {
		t.Fatalf("insert container: %v", err)
	}

	var chunkID int64
	if err := dbconn.QueryRow(
		`INSERT INTO chunk (chunk_hash, size, status, live_ref_count, pin_count, retry_count, chunker_version)
		 VALUES ($1, $2, $3, $4, $5, $6, $7)
		 RETURNING id`,
		strings.Repeat("a", 64),
		int64(16),
		filestate.ChunkCompleted,
		int64(0),
		int64(0),
		int64(0),
		"v1-simple-rolling",
	).Scan(&chunkID); err != nil {
		t.Fatalf("insert chunk: %v", err)
	}

	if _, err := dbconn.Exec(
		`INSERT INTO blocks (chunk_id, codec, format_version, plaintext_size, stored_size, nonce, container_id, block_offset)
		 VALUES ($1, 'plain', 1, $2, $3, x'', $4, $5)`,
		chunkID,
		int64(16),
		int64(16),
		containerID,
		int64(0),
	); err != nil {
		t.Fatalf("insert legacy block row: %v", err)
	}

	err := verifyLegacyChunkHashes(dbconn, t.TempDir())
	if err == nil || !strings.HasPrefix(err.Error(), "physical_missing:") {
		t.Fatalf("expected physical_missing category prefix, got: %v", err)
	}
}

func TestVerifyLegacyChunkHashesRejectsUnsafeContainerFilename(t *testing.T) {
	dbconn := openVerifyTestDB(t)
	defer func() { _ = dbconn.Close() }()

	var containerID int64
	if err := dbconn.QueryRow(
		`INSERT INTO container (filename, current_size, max_size, sealed, quarantine)
		 VALUES ($1, $2, $3, FALSE, FALSE)
		 RETURNING id`,
		"../escape.bin",
		int64(1024),
		int64(1024),
	).Scan(&containerID); err != nil {
		t.Fatalf("insert container: %v", err)
	}

	var chunkID int64
	if err := dbconn.QueryRow(
		`INSERT INTO chunk (chunk_hash, size, status, live_ref_count, pin_count, retry_count, chunker_version)
		 VALUES ($1, $2, $3, $4, $5, $6, $7)
		 RETURNING id`,
		strings.Repeat("b", 64),
		int64(16),
		filestate.ChunkCompleted,
		int64(0),
		int64(0),
		int64(0),
		"v1-simple-rolling",
	).Scan(&chunkID); err != nil {
		t.Fatalf("insert chunk: %v", err)
	}

	if _, err := dbconn.Exec(
		`INSERT INTO blocks (chunk_id, codec, format_version, plaintext_size, stored_size, nonce, container_id, block_offset)
		 VALUES ($1, 'plain', 1, $2, $3, x'', $4, $5)`,
		chunkID,
		int64(16),
		int64(16),
		containerID,
		int64(0),
	); err != nil {
		t.Fatalf("insert legacy block row: %v", err)
	}

	err := verifyLegacyChunkHashes(dbconn, t.TempDir())
	if err == nil {
		t.Fatal("expected verifyLegacyChunkHashes to reject unsafe filename")
	}
	if !strings.Contains(err.Error(), "invalid container filename") {
		t.Fatalf("expected invalid container filename error, got: %v", err)
	}
}

// Phase 3 — Offset / Length / Bounds Validation

func TestPackedRangeValidationRejectsNegativeOffset(t *testing.T) {
	err := validatePackedRange("test", -1, 1, 10)
	if err == nil || !strings.Contains(err.Error(), "offset must be non-negative") {
		t.Fatalf("expected negative offset error, got: %v", err)
	}
}

func TestPackedRangeValidationRejectsNegativeLength(t *testing.T) {
	err := validatePackedRange("test", 0, -1, 10)
	if err == nil || !strings.Contains(err.Error(), "length must be non-negative") {
		t.Fatalf("expected negative length error, got: %v", err)
	}
}

func TestPackedRangeValidationRejectsNegativeSize(t *testing.T) {
	err := validatePackedRange("test", 0, 0, -1)
	if err == nil || !strings.Contains(err.Error(), "container size must be non-negative") {
		t.Fatalf("expected negative size error, got: %v", err)
	}
}

func TestPackedRangeValidationRejectsOffsetPastContainerSize(t *testing.T) {
	err := validatePackedRange("test", 11, 0, 10)
	if err == nil || !strings.Contains(err.Error(), "offset exceeds container size") {
		t.Fatalf("expected offset-exceeds-size error, got: %v", err)
	}
}

func TestPackedRangeValidationRejectsRangePastContainerSize(t *testing.T) {
	err := validatePackedRange("test", 9, 2, 10)
	if err == nil || !strings.Contains(err.Error(), "range exceeds container size") {
		t.Fatalf("expected range-exceeds-size error, got: %v", err)
	}
}

func TestPackedRangeValidationRejectsOverflow(t *testing.T) {
	// offset near MaxInt64 — offset+length would overflow int64, but the
	// overflow-safe check (length > size-offset) correctly rejects it.
	err := validatePackedRange("test", math.MaxInt64-1, 10, math.MaxInt64)
	if err == nil || !strings.Contains(err.Error(), "range exceeds container size") {
		t.Fatalf("expected overflow-safe range error, got: %v", err)
	}
}

func TestPackedRangeValidationAllowsExactEndBoundary(t *testing.T) {
	// offset=8, length=2, size=10 — range ends exactly at boundary; valid.
	err := validatePackedRange("test", 8, 2, 10)
	if err != nil {
		t.Fatalf("expected valid exact-end-boundary to pass, got: %v", err)
	}
}

func TestVerifyRepositoryRejectsPackedRangeBeforeRead(t *testing.T) {
	dbconn := openVerifyTestDB(t)
	defer func() { _ = dbconn.Close() }()

	containersDir := t.TempDir()
	blockID, _ := seedVerifyPackedBlockFixture(t, dbconn, containersDir, [][]byte{[]byte("bounds-test-payload")}, nil)

	// Corrupt size_in_block to exceed the block's plaintext_size.
	if _, err := dbconn.Exec(`
		UPDATE chunk_block_refs
		SET size_in_block = (SELECT plaintext_size + 1 FROM storage_blocks WHERE id = block_id)
		WHERE block_id = $1
	`, blockID); err != nil {
		t.Fatalf("corrupt chunk_block_refs size_in_block: %v", err)
	}

	err := VerifyRepository(dbconn, containersDir)
	if err == nil || !strings.Contains(err.Error(), "verifyPackedBounds") {
		t.Fatalf("expected verifyPackedBounds error, got: %v", err)
	}
	if strings.Contains(err.Error(), "verifyBlockPayloads") {
		t.Fatalf("expected bounds failure before payload read stage, got: %v", err)
	}
}

func TestVerifyRepositoryFastRejectsPackedRangeBeforeRead(t *testing.T) {
	dbconn := openVerifyTestDB(t)
	defer func() { _ = dbconn.Close() }()

	containersDir := t.TempDir()
	blockID, _ := seedVerifyPackedBlockFixture(t, dbconn, containersDir, [][]byte{[]byte("fast-bounds-test")}, nil)

	// Corrupt size_in_block to exceed the block's plaintext_size.
	if _, err := dbconn.Exec(`
		UPDATE chunk_block_refs
		SET size_in_block = (SELECT plaintext_size + 1 FROM storage_blocks WHERE id = block_id)
		WHERE block_id = $1
	`, blockID); err != nil {
		t.Fatalf("corrupt chunk_block_refs size_in_block (fast): %v", err)
	}

	err := VerifyRepositoryFast(dbconn, containersDir)
	if err == nil || !strings.Contains(err.Error(), "verifyPackedBounds") {
		t.Fatalf("expected verifyPackedBounds error (fast path), got: %v", err)
	}
	if strings.Contains(err.Error(), "verifyBlockPayloads") {
		t.Fatalf("expected bounds failure before payload read stage (fast), got: %v", err)
	}
}

// Phase 4 — Block Hash / Checksum Consistency

func TestPackedDigestValidationRejectsMissingDigest(t *testing.T) {
	err := validateSHA256HexDigest("chunk hash", "")
	if err == nil || !strings.Contains(err.Error(), "is required") {
		t.Fatalf("expected missing digest error, got: %v", err)
	}
}

func TestPackedDigestValidationRejectsMalformedHex(t *testing.T) {
	err := validateSHA256HexDigest("chunk hash", strings.Repeat("g", 64))
	if err == nil || !strings.Contains(err.Error(), "must be valid hex") {
		t.Fatalf("expected malformed hex error, got: %v", err)
	}
}

func TestPackedDigestValidationRejectsWrongDigestLength(t *testing.T) {
	err := validateSHA256HexDigest("chunk hash", strings.Repeat("a", 63))
	if err == nil || !strings.Contains(err.Error(), "must be 64 hex chars") {
		t.Fatalf("expected wrong digest length error, got: %v", err)
	}
}

func TestPackedDigestValidationAcceptsMatchingPayload(t *testing.T) {
	payload := []byte("phase4-valid-payload")
	sum := sha256.Sum256(payload)
	digest := hex.EncodeToString(sum[:])
	err := validateSHA256HexDigest("chunk hash", digest)
	if err != nil {
		t.Fatalf("expected valid digest to pass, got: %v", err)
	}
}

func TestVerifyRepositoryRejectsInvalidPhysicalHashLength(t *testing.T) {
	dbconn := openVerifyTestDB(t)
	defer func() { _ = dbconn.Close() }()

	containersDir := t.TempDir()
	blockID, _ := seedVerifyPackedBlockFixture(t, dbconn, containersDir, [][]byte{[]byte("phase4-invalid-physical")}, nil)

	if _, err := dbconn.Exec(`UPDATE storage_blocks SET physical_hash = zeroblob(31) WHERE id = $1`, blockID); err != nil {
		t.Fatalf("set invalid physical_hash length: %v", err)
	}

	err := VerifyRepository(dbconn, containersDir)
	if err == nil || !strings.Contains(err.Error(), "invalid physical_hash length") {
		t.Fatalf("expected invalid physical_hash length error, got: %v", err)
	}
}

func TestVerifyRepositoryRejectsInvalidCompressedHashLength(t *testing.T) {
	dbconn := openVerifyTestDB(t)
	defer func() { _ = dbconn.Close() }()

	containersDir := t.TempDir()
	blockID, _ := seedVerifyPackedBlockFixture(t, dbconn, containersDir, [][]byte{[]byte("phase4-invalid-compressed")}, nil)

	if _, err := dbconn.Exec(`UPDATE storage_blocks SET compressed_hash = zeroblob(31) WHERE id = $1`, blockID); err != nil {
		t.Fatalf("set invalid compressed_hash length: %v", err)
	}

	err := VerifyRepository(dbconn, containersDir)
	if err == nil || !strings.Contains(err.Error(), "invalid compressed_hash length") {
		t.Fatalf("expected invalid compressed_hash length error, got: %v", err)
	}
}

func TestVerifyRepositoryRejectsMalformedChunkHashHex(t *testing.T) {
	dbconn := openVerifyTestDB(t)
	defer func() { _ = dbconn.Close() }()

	containersDir := t.TempDir()
	_, chunkIDs := seedVerifyPackedBlockFixture(t, dbconn, containersDir, [][]byte{[]byte("phase4-malformed-chunk-hash")}, nil)

	if _, err := dbconn.Exec(`UPDATE chunk SET chunk_hash = $1 WHERE id = $2`, strings.Repeat("g", 64), chunkIDs[0]); err != nil {
		t.Fatalf("set malformed chunk_hash: %v", err)
	}

	err := VerifyRepository(dbconn, containersDir)
	if err == nil || !strings.Contains(err.Error(), "invalid expected chunk_hash format") {
		t.Fatalf("expected malformed chunk_hash format error, got: %v", err)
	}
}

func TestVerifyRepositoryRejectsWrongChunkHashLength(t *testing.T) {
	dbconn := openVerifyTestDB(t)
	defer func() { _ = dbconn.Close() }()

	containersDir := t.TempDir()
	_, chunkIDs := seedVerifyPackedBlockFixture(t, dbconn, containersDir, [][]byte{[]byte("phase4-short-chunk-hash")}, nil)

	if _, err := dbconn.Exec(`UPDATE chunk SET chunk_hash = $1 WHERE id = $2`, strings.Repeat("a", 63), chunkIDs[0]); err != nil {
		t.Fatalf("set short chunk_hash: %v", err)
	}

	err := VerifyRepository(dbconn, containersDir)
	if err == nil || !strings.Contains(err.Error(), "invalid expected chunk_hash format") {
		t.Fatalf("expected short chunk_hash format error, got: %v", err)
	}
}

func TestVerifyRepositoryRejectsPackedPayloadHashMismatch(t *testing.T) {
	dbconn := openVerifyTestDB(t)
	defer func() { _ = dbconn.Close() }()

	containersDir := t.TempDir()
	blockID, _ := seedVerifyPackedBlockFixture(t, dbconn, containersDir, [][]byte{[]byte("phase4-payload-mismatch")}, nil)

	if _, err := dbconn.Exec(`UPDATE storage_blocks SET block_hash = zeroblob(32) WHERE id = $1`, blockID); err != nil {
		t.Fatalf("mutate block hash for VerifyRepository mismatch: %v", err)
	}

	err := VerifyRepository(dbconn, containersDir)
	if err == nil || !strings.HasPrefix(err.Error(), verifyErrBlockHashMismatch+":") {
		t.Fatalf("expected block_hash_mismatch category, got: %v", err)
	}
}

func TestVerifyRepositoryFastRejectsPackedPayloadHashMismatch(t *testing.T) {
	dbconn := openVerifyTestDB(t)
	defer func() { _ = dbconn.Close() }()

	containersDir := t.TempDir()
	blockID, _ := seedVerifyPackedBlockFixture(t, dbconn, containersDir, [][]byte{[]byte("phase4-fast-payload-mismatch")}, nil)

	if _, err := dbconn.Exec(`UPDATE storage_blocks SET block_hash = zeroblob(32) WHERE id = $1`, blockID); err != nil {
		t.Fatalf("mutate block hash for VerifyRepositoryFast mismatch: %v", err)
	}

	err := VerifyRepositoryFast(dbconn, containersDir)
	if err == nil || !strings.HasPrefix(err.Error(), verifyErrBlockHashMismatch+":") {
		t.Fatalf("expected block_hash_mismatch category in fast mode, got: %v", err)
	}
}

// Phase 5 — Malformed Container Read Safety

func TestVerifyRepositoryRejectsMissingContainerFile(t *testing.T) {
	dbconn := openVerifyTestDB(t)
	defer func() { _ = dbconn.Close() }()

	containersDir := t.TempDir()
	blockID, _ := seedVerifyPackedBlockFixture(t, dbconn, containersDir, [][]byte{[]byte("phase5-missing-container")}, nil)

	path, _, _, _ := packedFixtureBlockStorageMeta(t, dbconn, blockID, containersDir)
	if err := os.Remove(path); err != nil {
		t.Fatalf("remove container file: %v", err)
	}

	err := VerifyRepository(dbconn, containersDir)
	if err == nil || !strings.HasPrefix(err.Error(), verifyErrPhysicalMissing+":") {
		t.Fatalf("expected physical_missing category for missing container file, got: %v", err)
	}
}

func TestVerifyRepositoryFastRejectsMissingContainerFile(t *testing.T) {
	dbconn := openVerifyTestDB(t)
	defer func() { _ = dbconn.Close() }()

	containersDir := t.TempDir()
	blockID, _ := seedVerifyPackedBlockFixture(t, dbconn, containersDir, [][]byte{[]byte("phase5-fast-missing-container")}, nil)

	path, _, _, _ := packedFixtureBlockStorageMeta(t, dbconn, blockID, containersDir)
	if err := os.Remove(path); err != nil {
		t.Fatalf("remove container file: %v", err)
	}

	err := VerifyRepositoryFast(dbconn, containersDir)
	if err == nil || !strings.HasPrefix(err.Error(), verifyErrPhysicalMissing+":") {
		t.Fatalf("expected physical_missing category for missing container file in fast mode, got: %v", err)
	}
}

func TestVerifyRepositoryRejectsTruncatedContainerFile(t *testing.T) {
	dbconn := openVerifyTestDB(t)
	defer func() { _ = dbconn.Close() }()

	containersDir := t.TempDir()
	blockID, _ := seedVerifyPackedBlockFixture(t, dbconn, containersDir, [][]byte{[]byte("phase5-truncated-container")}, nil)

	path, offset, _, _ := packedFixtureBlockStorageMeta(t, dbconn, blockID, containersDir)
	// Truncate to exactly the block offset so the payload bytes are gone.
	if err := os.Truncate(path, offset); err != nil {
		t.Fatalf("truncate container file: %v", err)
	}

	err := VerifyRepository(dbconn, containersDir)
	if err == nil || !strings.HasPrefix(err.Error(), verifyErrPhysicalMissing+":") {
		t.Fatalf("expected physical_missing category for truncated container file, got: %v", err)
	}
}

func TestVerifyRepositoryFastRejectsTruncatedContainerFile(t *testing.T) {
	dbconn := openVerifyTestDB(t)
	defer func() { _ = dbconn.Close() }()

	containersDir := t.TempDir()
	blockID, _ := seedVerifyPackedBlockFixture(t, dbconn, containersDir, [][]byte{[]byte("phase5-fast-truncated-container")}, nil)

	path, offset, _, _ := packedFixtureBlockStorageMeta(t, dbconn, blockID, containersDir)
	if err := os.Truncate(path, offset); err != nil {
		t.Fatalf("truncate container file: %v", err)
	}

	err := VerifyRepositoryFast(dbconn, containersDir)
	if err == nil || !strings.HasPrefix(err.Error(), verifyErrPhysicalMissing+":") {
		t.Fatalf("expected physical_missing category for truncated container file in fast mode, got: %v", err)
	}
}

func TestVerifyRepositoryDoesNotSilentlySkipMalformedContainerRead(t *testing.T) {
	dbconn := openVerifyTestDB(t)
	defer func() { _ = dbconn.Close() }()

	containersDir := t.TempDir()
	blockID, _ := seedVerifyPackedBlockFixture(t, dbconn, containersDir, [][]byte{[]byte("phase5-no-skip")}, nil)

	path, offset, _, _ := packedFixtureBlockStorageMeta(t, dbconn, blockID, containersDir)
	if err := os.Truncate(path, offset); err != nil {
		t.Fatalf("truncate container file: %v", err)
	}

	// Must not return nil — a real error must surface, not silent success.
	err := VerifyRepository(dbconn, containersDir)
	if err == nil {
		t.Fatal("expected non-nil error; VerifyRepository must not silently skip a malformed container read")
	}
}

func TestVerifyRepositoryFastDoesNotSilentlySkipMalformedContainerRead(t *testing.T) {
	dbconn := openVerifyTestDB(t)
	defer func() { _ = dbconn.Close() }()

	containersDir := t.TempDir()
	blockID, _ := seedVerifyPackedBlockFixture(t, dbconn, containersDir, [][]byte{[]byte("phase5-fast-no-skip")}, nil)

	path, offset, _, _ := packedFixtureBlockStorageMeta(t, dbconn, blockID, containersDir)
	if err := os.Truncate(path, offset); err != nil {
		t.Fatalf("truncate container file: %v", err)
	}

	err := VerifyRepositoryFast(dbconn, containersDir)
	if err == nil {
		t.Fatal("expected non-nil error; VerifyRepositoryFast must not silently skip a malformed container read")
	}
}

func TestValidContainerStillVerifiesAfterPhase5Hardening(t *testing.T) {
	dbconn := openVerifyTestDB(t)
	defer func() { _ = dbconn.Close() }()

	containersDir := t.TempDir()
	_, _ = seedVerifyPackedBlockFixture(t, dbconn, containersDir, [][]byte{[]byte("phase5-valid-sanity")}, nil)

	if err := VerifyRepository(dbconn, containersDir); err != nil {
		t.Fatalf("expected valid container to pass VerifyRepository: %v", err)
	}
	if err := VerifyRepositoryFast(dbconn, containersDir); err != nil {
		t.Fatalf("expected valid container to pass VerifyRepositoryFast: %v", err)
	}
}
