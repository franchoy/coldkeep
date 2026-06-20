package verify

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"strings"
	"testing"

	"github.com/franchoy/coldkeep/internal/blocks"
	"github.com/franchoy/coldkeep/internal/container"
	"github.com/franchoy/coldkeep/internal/db"
	filestate "github.com/franchoy/coldkeep/internal/status"
	_ "github.com/mattn/go-sqlite3"
)

func openPreV15MigratedVerifyDB(t *testing.T, containersDir string) *sql.DB {
	t.Helper()

	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	dbconn.SetMaxOpenConns(1)
	dbconn.SetMaxIdleConns(1)

	legacySchema := `
		PRAGMA foreign_keys = ON;
		CREATE TABLE schema_version (version INTEGER PRIMARY KEY);
		INSERT INTO schema_version(version) VALUES (8);

		CREATE TABLE container (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			filename TEXT NOT NULL UNIQUE,
			sealed BOOLEAN NOT NULL DEFAULT 0,
			sealing BOOLEAN NOT NULL DEFAULT 0,
			quarantine BOOLEAN NOT NULL DEFAULT 0,
			current_size INTEGER NOT NULL DEFAULT 0,
			max_size INTEGER NOT NULL DEFAULT 1048576,
			container_hash TEXT,
			created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
			updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
		);

		CREATE TABLE logical_file (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			original_name TEXT NOT NULL,
			total_size INTEGER NOT NULL CHECK (total_size >= 0),
			file_hash TEXT NOT NULL,
			ref_count INTEGER NOT NULL DEFAULT 1 CHECK (ref_count >= 0),
			status TEXT NOT NULL CHECK (status IN ('PROCESSING','COMPLETED','ABORTED')),
			retry_count INTEGER NOT NULL DEFAULT 0 CHECK (retry_count >= 0),
			created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
			updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
			UNIQUE (file_hash, total_size)
		);

		CREATE TABLE chunk (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			chunk_hash TEXT NOT NULL,
			size INTEGER NOT NULL CHECK (size > 0),
			status TEXT NOT NULL CHECK (status IN ('PROCESSING','COMPLETED','ABORTED')),
			live_ref_count INTEGER NOT NULL DEFAULT 0 CHECK (live_ref_count >= 0),
			pin_count INTEGER NOT NULL DEFAULT 0 CHECK (pin_count >= 0),
			retry_count INTEGER NOT NULL DEFAULT 0 CHECK (retry_count >= 0),
			created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
			updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
		);

		CREATE UNIQUE INDEX idx_chunk_hash_size ON chunk(chunk_hash, size);

		CREATE TABLE file_chunk (
			logical_file_id INTEGER NOT NULL REFERENCES logical_file(id),
			chunk_id INTEGER NOT NULL REFERENCES chunk(id),
			chunk_order INTEGER NOT NULL,
			PRIMARY KEY (logical_file_id, chunk_order)
		);

		CREATE TABLE blocks (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			chunk_id INTEGER NOT NULL REFERENCES chunk(id),
			codec TEXT NOT NULL,
			format_version INTEGER NOT NULL,
			plaintext_size INTEGER NOT NULL,
			stored_size INTEGER NOT NULL,
			nonce BLOB,
			container_id INTEGER NOT NULL REFERENCES container(id),
			block_offset INTEGER NOT NULL,
			created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
			UNIQUE(chunk_id, container_id, block_offset)
		);

		CREATE TABLE physical_file (
			path TEXT PRIMARY KEY,
			logical_file_id INTEGER NOT NULL REFERENCES logical_file(id),
			mode INTEGER,
			mtime TIMESTAMP,
			uid INTEGER,
			gid INTEGER,
			is_metadata_complete INTEGER NOT NULL DEFAULT 0
		);
	`
	if _, err := dbconn.Exec(legacySchema); err != nil {
		_ = dbconn.Close()
		t.Fatalf("create legacy pre-v1.5 schema: %v", err)
	}

	var logicalFileID int64
	if err := dbconn.QueryRow(
		`INSERT INTO logical_file (original_name, total_size, file_hash, status, ref_count)
		 VALUES ($1, $2, $3, $4, $5) RETURNING id`,
		"verify-legacy-file.bin",
		int64(11),
		strings.Repeat("a", 64),
		filestate.LogicalFileCompleted,
		int64(1),
	).Scan(&logicalFileID); err != nil {
		_ = dbconn.Close()
		t.Fatalf("insert legacy logical_file: %v", err)
	}

	if _, err := dbconn.Exec(
		`INSERT INTO physical_file (path, logical_file_id, is_metadata_complete) VALUES ($1, $2, 1)`,
		"/legacy/verify-legacy-file.bin",
		logicalFileID,
	); err != nil {
		_ = dbconn.Close()
		t.Fatalf("insert legacy physical_file mapping: %v", err)
	}

	chunkPayload := []byte("legacy-data")
	chunkSum := sha256.Sum256(chunkPayload)
	chunkHash := hex.EncodeToString(chunkSum[:])

	var chunkID int64
	if err := dbconn.QueryRow(
		`INSERT INTO chunk (chunk_hash, size, status, live_ref_count, pin_count)
		 VALUES ($1, $2, $3, $4, $5) RETURNING id`,
		chunkHash,
		int64(len(chunkPayload)),
		filestate.ChunkCompleted,
		int64(1),
		int64(0),
	).Scan(&chunkID); err != nil {
		_ = dbconn.Close()
		t.Fatalf("insert legacy chunk: %v", err)
	}

	if _, err := dbconn.Exec(
		`INSERT INTO file_chunk (logical_file_id, chunk_id, chunk_order) VALUES ($1, $2, 0)`,
		logicalFileID,
		chunkID,
	); err != nil {
		_ = dbconn.Close()
		t.Fatalf("insert legacy file_chunk: %v", err)
	}

	transformer, err := blocks.GetBlockTransformer(blocks.CodecPlain)
	if err != nil {
		_ = dbconn.Close()
		t.Fatalf("get plain transformer for legacy fixture: %v", err)
	}
	encoded, err := transformer.Encode(context.Background(), blocks.EncodeInput{
		ChunkID:   chunkID,
		ChunkHash: chunkHash,
		Plaintext: chunkPayload,
	})
	if err != nil {
		_ = dbconn.Close()
		t.Fatalf("encode legacy payload: %v", err)
	}

	writer := container.NewLocalWriterWithDirAndDB(containersDir, container.GetContainerMaxSize(), dbconn)
	tx, err := dbconn.BeginTx(context.Background(), nil)
	if err != nil {
		_ = dbconn.Close()
		t.Fatalf("begin tx for legacy fixture: %v", err)
	}
	placement, err := writer.AppendPayload(tx, encoded.Payload)
	if err != nil {
		_ = tx.Rollback()
		_ = dbconn.Close()
		t.Fatalf("append legacy payload: %v", err)
	}
	if err := container.UpdateContainerSize(tx, placement.ContainerID, placement.NewContainerSize); err != nil {
		_ = tx.Rollback()
		_ = dbconn.Close()
		t.Fatalf("update container size for legacy fixture: %v", err)
	}

	if _, err := tx.ExecContext(
		context.Background(),
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
		_ = dbconn.Close()
		t.Fatalf("insert legacy block: %v", err)
	}

	if err := tx.Commit(); err != nil {
		_ = tx.Rollback()
		_ = dbconn.Close()
		t.Fatalf("commit legacy payload fixture: %v", err)
	}

	if err := db.RunMigrations(dbconn); err != nil {
		_ = dbconn.Close()
		t.Fatalf("run migrations pre-v1.5 -> current: %v", err)
	}

	return dbconn
}

func TestVerifySystemStandardPassesOnMigratedPreV15Repository(t *testing.T) {
	containersDir := t.TempDir()
	dbconn := openPreV15MigratedVerifyDB(t, containersDir)
	defer func() { _ = dbconn.Close() }()

	if err := VerifySystemStandardWithContainersDir(dbconn, containersDir); err != nil {
		t.Fatalf("verify should pass on healthy migrated pre-v1.5 repository: %v", err)
	}
}

func TestVerifySystemFullDetectsMissingContainerFileForReferencedChunk(t *testing.T) {
	dbconn := openVerifyTestDB(t)
	defer func() { _ = dbconn.Close() }()

	var logicalID int64
	if err := dbconn.QueryRow(
		`INSERT INTO logical_file (original_name, total_size, file_hash, status, ref_count, chunker_version)
		 VALUES ($1, $2, $3, $4, $5, $6) RETURNING id`,
		"missing-container.bin", int64(512), strings.Repeat("c", 64), filestate.LogicalFileCompleted, int64(1), "v1-simple-rolling",
	).Scan(&logicalID); err != nil {
		t.Fatalf("insert logical file: %v", err)
	}

	if _, err := dbconn.Exec(
		`INSERT INTO physical_file (path, logical_file_id, is_metadata_complete) VALUES ($1, $2, 0)`,
		"/healthy/path/missing-container.bin", logicalID,
	); err != nil {
		t.Fatalf("insert physical_file row: %v", err)
	}

	var containerID int64
	if err := dbconn.QueryRow(
		`INSERT INTO container (filename, current_size, max_size, sealed, quarantine) VALUES ($1, $2, $3, $4, $5) RETURNING id`,
		"missing-on-disk.bin", int64(2048), int64(2048), true, false,
	).Scan(&containerID); err != nil {
		t.Fatalf("insert container: %v", err)
	}

	var chunkID int64
	if err := dbconn.QueryRow(
		`INSERT INTO chunk (chunk_hash, size, status, live_ref_count, pin_count, retry_count, chunker_version)
		 VALUES ($1, $2, $3, $4, $5, $6, $7) RETURNING id`,
		strings.Repeat("d", 64), int64(512), filestate.ChunkCompleted, int64(1), int64(0), int64(0), "v1-simple-rolling",
	).Scan(&chunkID); err != nil {
		t.Fatalf("insert chunk: %v", err)
	}

	if _, err := dbconn.Exec(
		`INSERT INTO file_chunk (logical_file_id, chunk_id, chunk_order) VALUES ($1, $2, 0)`,
		logicalID, chunkID,
	); err != nil {
		t.Fatalf("insert file_chunk: %v", err)
	}

	if _, err := dbconn.Exec(
		`INSERT INTO blocks (chunk_id, container_id, block_offset, stored_size, plaintext_size, codec, format_version, nonce)
		 VALUES ($1, $2, $3, $4, $5, 'plain', 1, x'')`,
		chunkID, containerID, int64(0), int64(512), int64(512),
	); err != nil {
		t.Fatalf("insert blocks row: %v", err)
	}

	err := VerifySystemFullWithContainersDir(dbconn, t.TempDir())
	if err == nil || !strings.Contains(err.Error(), "no such file or directory") {
		t.Fatalf("expected full verify to fail for missing referenced container file, got: %v", err)
	}
}

// insertVerifyPackedMissingLogical inserts the logical_file and physical_file
// rows for the packed-missing fixture and returns the logical file ID.
func insertVerifyPackedMissingLogical(t *testing.T, dbconn *sql.DB) int64 {
	t.Helper()

	var logicalID int64
	if err := dbconn.QueryRow(
		`INSERT INTO logical_file (original_name, total_size, file_hash, status, ref_count, chunker_version)
		 VALUES ($1, $2, $3, $4, $5, $6) RETURNING id`,
		"packed-missing.bin", int64(512), strings.Repeat("a", 64), filestate.LogicalFileCompleted, int64(1), "v1-simple-rolling",
	).Scan(&logicalID); err != nil {
		t.Fatalf("insert logical file: %v", err)
	}

	if _, err := dbconn.Exec(
		`INSERT INTO physical_file (path, logical_file_id, is_metadata_complete) VALUES ($1, $2, 0)`,
		"/packed/missing.bin", logicalID,
	); err != nil {
		t.Fatalf("insert physical_file: %v", err)
	}

	return logicalID
}

// insertVerifyPackedMissingBlockChain inserts the container, storage_blocks,
// chunk, file_chunk, and chunk_block_refs rows for the packed-missing fixture.
func insertVerifyPackedMissingBlockChain(t *testing.T, dbconn *sql.DB, logicalID int64) {
	t.Helper()

	var containerID int64
	if err := dbconn.QueryRow(
		`INSERT INTO container (filename, current_size, max_size, sealed, quarantine) VALUES ($1, $2, $3, $4, $5) RETURNING id`,
		"packed-missing-on-disk.bin", int64(4096), int64(4096), true, false,
	).Scan(&containerID); err != nil {
		t.Fatalf("insert container: %v", err)
	}

	var blockID int64
	if err := dbconn.QueryRow(
		`INSERT INTO storage_blocks
		 (format_version, codec, plaintext_size, compression_codec, stored_size, container_id, container_offset, block_hash)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8) RETURNING id`,
		1, "none", int64(512), "none", int64(512), containerID, int64(0),
		[]byte(strings.Repeat("\xab", 32)),
	).Scan(&blockID); err != nil {
		t.Fatalf("insert storage_blocks row: %v", err)
	}

	var chunkID int64
	if err := dbconn.QueryRow(
		`INSERT INTO chunk (chunk_hash, size, status, live_ref_count, pin_count, retry_count, chunker_version)
		 VALUES ($1, $2, $3, $4, $5, $6, $7) RETURNING id`,
		strings.Repeat("ef", 32), int64(512), filestate.ChunkCompleted, int64(1), int64(0), int64(0), "v1-simple-rolling",
	).Scan(&chunkID); err != nil {
		t.Fatalf("insert chunk: %v", err)
	}

	if _, err := dbconn.Exec(
		`INSERT INTO file_chunk (logical_file_id, chunk_id, chunk_order) VALUES ($1, $2, $3)`,
		logicalID, chunkID, 0,
	); err != nil {
		t.Fatalf("insert file_chunk: %v", err)
	}

	if _, err := dbconn.Exec(
		`INSERT INTO chunk_block_refs (chunk_id, block_id, offset_in_block, size_in_block) VALUES ($1, $2, $3, $4)`,
		chunkID, blockID, int64(0), int64(512),
	); err != nil {
		t.Fatalf("insert chunk_block_refs: %v", err)
	}
}

// setupVerifyPackedMissingFixture inserts the full reference chain required by
// TestVerifySystemFullDetectsMissingContainerFileForPackedBlock. The container
// has no corresponding file on disk, so a full verify against an empty
// containersDir must fail.
func setupVerifyPackedMissingFixture(t *testing.T, dbconn *sql.DB) {
	t.Helper()
	logicalID := insertVerifyPackedMissingLogical(t, dbconn)
	insertVerifyPackedMissingBlockChain(t, dbconn, logicalID)
}

func TestVerifySystemFullDetectsMissingContainerFileForPackedBlock(t *testing.T) {
	dbconn := openVerifyTestDB(t)
	defer func() { _ = dbconn.Close() }()

	setupVerifyPackedMissingFixture(t, dbconn)

	err := VerifySystemFullWithContainersDir(dbconn, t.TempDir())
	if err == nil || !strings.Contains(err.Error(), "no such file or directory") {
		t.Fatalf("expected full verify to fail for missing packed container file, got: %v", err)
	}
}

func TestVerifySystemStandardRejectsBlankVersionMetadataAfterMigration(t *testing.T) {
	containersDir := t.TempDir()
	dbconn := openPreV15MigratedVerifyDB(t, containersDir)
	defer func() { _ = dbconn.Close() }()

	if _, err := dbconn.Exec(`UPDATE logical_file SET chunker_version = '   '`); err != nil {
		t.Fatalf("blank logical_file.chunker_version after migration: %v", err)
	}

	err := VerifySystemStandardWithContainersDir(dbconn, containersDir)
	if err == nil || !strings.Contains(err.Error(), "empty logical_file chunker_version rows=1") {
		t.Fatalf("expected chunker_version sanity failure after migration metadata tamper, got: %v", err)
	}
}
