package verify

import (
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

func packedFixtureBlockStorageMeta(t *testing.T, dbconn *sql.DB, blockID int64, containersDir string) (string, int64, int64, int64) {
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

func readPackedStoredBytesForTest(t *testing.T, path string, offset int64, size int64) []byte {
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

func overwritePackedStoredBytesForTest(t *testing.T, path string, offset int64, payload []byte) {
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

func setPackedBlockHashForBytes(t *testing.T, dbconn *sql.DB, blockID int64, payload []byte) {
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

func seedVerifyPackedBlockFixture(t *testing.T, dbconn *sql.DB, containersDir string, chunkPayloads [][]byte, refs []verifyPackedRefSeed) (int64, []int64) {
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

func seedVerifyLegacyBlockFixture(t *testing.T, dbconn *sql.DB, containersDir string, payload []byte) int64 {
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
	if err == nil || !strings.Contains(err.Error(), "decode block") {
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
	if err == nil || !strings.Contains(err.Error(), "decode block") {
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
	if err == nil || !strings.Contains(err.Error(), "decode block") {
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
	if err == nil || !strings.Contains(err.Error(), "size mismatch") {
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
	if err == nil || !strings.Contains(err.Error(), "plaintext size mismatch") {
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
		"plain",
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
