package verify

import (
	"database/sql"
	"strings"
	"testing"

	"github.com/franchoy/coldkeep/internal/db"
	filestate "github.com/franchoy/coldkeep/internal/status"
	_ "github.com/mattn/go-sqlite3"
)

func openPreV15MigratedVerifyDB(t *testing.T) *sql.DB {
	t.Helper()

	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}

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

	var containerID int64
	if err := dbconn.QueryRow(
		`INSERT INTO container (filename, sealed, current_size, max_size)
		 VALUES ($1, 1, $2, $3) RETURNING id`,
		"verify-legacy.bin",
		int64(128),
		int64(1048576),
	).Scan(&containerID); err != nil {
		_ = dbconn.Close()
		t.Fatalf("insert legacy container: %v", err)
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

	var chunkID int64
	if err := dbconn.QueryRow(
		`INSERT INTO chunk (chunk_hash, size, status, live_ref_count, pin_count)
		 VALUES ($1, $2, $3, $4, $5) RETURNING id`,
		strings.Repeat("b", 64),
		int64(11),
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

	if _, err := dbconn.Exec(
		`INSERT INTO blocks (chunk_id, codec, format_version, plaintext_size, stored_size, nonce, container_id, block_offset)
		 VALUES ($1, 'plain', 1, 11, 11, x'', $2, 0)`,
		chunkID,
		containerID,
	); err != nil {
		_ = dbconn.Close()
		t.Fatalf("insert legacy block: %v", err)
	}

	if err := db.RunMigrations(dbconn); err != nil {
		_ = dbconn.Close()
		t.Fatalf("run migrations pre-v1.5 -> current: %v", err)
	}

	return dbconn
}

func TestVerifySystemStandardPassesOnMigratedPreV15Repository(t *testing.T) {
	dbconn := openPreV15MigratedVerifyDB(t)
	defer func() { _ = dbconn.Close() }()

	if err := VerifySystemStandardWithContainersDir(dbconn, t.TempDir()); err != nil {
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
	if err == nil || !strings.Contains(err.Error(), "checkContainersFileExistence") {
		t.Fatalf("expected full verify to fail for missing referenced container file, got: %v", err)
	}
}

func TestVerifySystemStandardRejectsBlankVersionMetadataAfterMigration(t *testing.T) {
	dbconn := openPreV15MigratedVerifyDB(t)
	defer func() { _ = dbconn.Close() }()

	if _, err := dbconn.Exec(`UPDATE logical_file SET chunker_version = '   '`); err != nil {
		t.Fatalf("blank logical_file.chunker_version after migration: %v", err)
	}

	err := VerifySystemStandardWithContainersDir(dbconn, t.TempDir())
	if err == nil || !strings.Contains(err.Error(), "empty logical_file chunker_version rows=1") {
		t.Fatalf("expected chunker_version sanity failure after migration metadata tamper, got: %v", err)
	}
}
