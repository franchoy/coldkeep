package verify

import (
	"strings"
	"testing"

	filestate "github.com/franchoy/coldkeep/internal/status"
)

// TestVerifyFileStandardRejectsEmptyLogicalFileChunkerVersion confirms that
// VerifyFileStandardWithContainersDir returns an error if the logical_file row
// has an empty chunker_version. No chunker algorithm is invoked — only
// metadata presence is checked.
func TestVerifyFileStandardRejectsEmptyLogicalFileChunkerVersion(t *testing.T) {
	dbconn := openVerifyTestDB(t)
	defer func() { _ = dbconn.Close() }()

	var logicalID int64
	if err := dbconn.QueryRow(
		`INSERT INTO logical_file (original_name, total_size, file_hash, status, ref_count, chunker_version)
		 VALUES ($1, $2, $3, $4, $5, $6) RETURNING id`,
		"no-version.txt", int64(0), strings.Repeat("a", 64), filestate.LogicalFileCompleted, int64(0), "",
	).Scan(&logicalID); err != nil {
		t.Fatalf("insert logical file with empty chunker_version: %v", err)
	}

	err := VerifyFileStandardWithContainersDir(dbconn, int(logicalID), t.TempDir())
	if err == nil || !strings.Contains(err.Error(), "empty chunker_version") {
		t.Fatalf("expected chunker_version presence error, got: %v", err)
	}
}

// TestVerifyFileStandardAcceptsNonDefaultChunkerVersion confirms that a
// logical_file with a non-default chunker_version label (simulating a future
// algorithm) passes the metadata check. Verify must not compare the value
// against the current active chunker.
func TestVerifyFileStandardAcceptsNonDefaultChunkerVersion(t *testing.T) {
	dbconn := openVerifyTestDB(t)
	defer func() { _ = dbconn.Close() }()

	// Zero-byte file — no chunks needed, so only the version check runs.
	var logicalID int64
	if err := dbconn.QueryRow(
		`INSERT INTO logical_file (original_name, total_size, file_hash, status, ref_count, chunker_version)
		 VALUES ($1, $2, $3, $4, $5, $6) RETURNING id`,
		"future-algo.txt", int64(0), strings.Repeat("b", 64), filestate.LogicalFileCompleted, int64(0), "v2-fastcdc",
	).Scan(&logicalID); err != nil {
		t.Fatalf("insert logical file with future chunker_version: %v", err)
	}

	if err := VerifyFileStandardWithContainersDir(dbconn, int(logicalID), t.TempDir()); err != nil {
		t.Fatalf("verify should accept any non-empty chunker_version label; got: %v", err)
	}
}

// TestVerifyFileStandardRejectsChunkWithEmptyChunkerVersion confirms that
// VerifyFileStandardWithContainersDir returns an error when a referenced chunk
// has an empty chunker_version. The chunk version need not match the logical
// file version (deduped reuse is valid), but presence is required.
func TestVerifyFileStandardRejectsChunkWithEmptyChunkerVersion(t *testing.T) {
	dbconn := openVerifyTestDB(t)
	defer func() { _ = dbconn.Close() }()

	if _, err := dbconn.Exec(`PRAGMA foreign_keys = OFF`); err != nil {
		t.Fatalf("disable foreign_keys: %v", err)
	}

	var logicalID int64
	if err := dbconn.QueryRow(
		`INSERT INTO logical_file (original_name, total_size, file_hash, status, ref_count, chunker_version)
		 VALUES ($1, $2, $3, $4, $5, $6) RETURNING id`,
		"with-bad-chunk.txt", int64(512), strings.Repeat("c", 64), filestate.LogicalFileCompleted, int64(0), "v1-simple-rolling",
	).Scan(&logicalID); err != nil {
		t.Fatalf("insert logical file: %v", err)
	}

	// Insert a container so the blocks FK is satisfiable.
	var containerID int64
	if err := dbconn.QueryRow(
		`INSERT INTO container (filename, current_size, max_size, sealed, quarantine) VALUES ($1, $2, $3, $4, $5) RETURNING id`,
		"test.bin", int64(4096), int64(4096), true, false,
	).Scan(&containerID); err != nil {
		t.Fatalf("insert container: %v", err)
	}

	// Chunk with empty chunker_version.
	var chunkID int64
	if err := dbconn.QueryRow(
		`INSERT INTO chunk (chunk_hash, size, status, live_ref_count, pin_count, retry_count, chunker_version)
		 VALUES ($1, $2, $3, $4, $5, $6, $7) RETURNING id`,
		strings.Repeat("d", 64), int64(512), filestate.ChunkCompleted, int64(1), int64(0), int64(0), "",
	).Scan(&chunkID); err != nil {
		t.Fatalf("insert chunk with empty chunker_version: %v", err)
	}

	if _, err := dbconn.Exec(
		`INSERT INTO blocks (chunk_id, container_id, block_offset, stored_size, plaintext_size, codec, format_version, nonce)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
		chunkID, containerID, int64(512), int64(512), int64(512), "plain", 1, []byte("nonce00000000000"),
	); err != nil {
		t.Fatalf("insert blocks row: %v", err)
	}

	if _, err := dbconn.Exec(
		`INSERT INTO file_chunk (logical_file_id, chunk_id, chunk_order) VALUES ($1, $2, $3)`,
		logicalID, chunkID, 0,
	); err != nil {
		t.Fatalf("insert file_chunk: %v", err)
	}

	err := VerifyFileStandardWithContainersDir(dbconn, int(logicalID), t.TempDir())
	if err == nil || !strings.Contains(err.Error(), "empty chunker_version") {
		t.Fatalf("expected chunk empty chunker_version error, got: %v", err)
	}
}

// TestVerifyFileStandardAcceptsMismatchedChunkAndFileVersions confirms that
// VerifyFileStandardWithContainersDir does NOT error when a chunk's
// chunker_version differs from the logical file's chunker_version. This is
// valid for deduped chunks that were originally created under a different
// chunker generation.
func TestVerifyFileStandardAcceptsMismatchedChunkAndFileVersions(t *testing.T) {
	dbconn := openVerifyTestDB(t)
	defer func() { _ = dbconn.Close() }()

	if _, err := dbconn.Exec(`PRAGMA foreign_keys = OFF`); err != nil {
		t.Fatalf("disable foreign_keys: %v", err)
	}

	var logicalID int64
	if err := dbconn.QueryRow(
		`INSERT INTO logical_file (original_name, total_size, file_hash, status, ref_count, chunker_version)
		 VALUES ($1, $2, $3, $4, $5, $6) RETURNING id`,
		"mixed-versions.txt", int64(512), strings.Repeat("e", 64), filestate.LogicalFileCompleted, int64(0), "v2-fastcdc",
	).Scan(&logicalID); err != nil {
		t.Fatalf("insert logical file: %v", err)
	}

	var containerID int64
	if err := dbconn.QueryRow(
		`INSERT INTO container (filename, current_size, max_size, sealed, quarantine) VALUES ($1, $2, $3, $4, $5) RETURNING id`,
		"mixed.bin", int64(4096), int64(4096), true, false,
	).Scan(&containerID); err != nil {
		t.Fatalf("insert container: %v", err)
	}

	// Chunk created under a different version (v1-simple-rolling) but reused.
	var chunkID int64
	if err := dbconn.QueryRow(
		`INSERT INTO chunk (chunk_hash, size, status, live_ref_count, pin_count, retry_count, chunker_version)
		 VALUES ($1, $2, $3, $4, $5, $6, $7) RETURNING id`,
		strings.Repeat("f", 64), int64(512), filestate.ChunkCompleted, int64(1), int64(0), int64(0), "v1-simple-rolling",
	).Scan(&chunkID); err != nil {
		t.Fatalf("insert chunk: %v", err)
	}

	if _, err := dbconn.Exec(
		`INSERT INTO blocks (chunk_id, container_id, block_offset, stored_size, plaintext_size, codec, format_version, nonce)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
		chunkID, containerID, int64(512), int64(512), int64(512), "plain", 1, []byte("nonce00000000000"),
	); err != nil {
		t.Fatalf("insert blocks row: %v", err)
	}

	if _, err := dbconn.Exec(
		`INSERT INTO file_chunk (logical_file_id, chunk_id, chunk_order) VALUES ($1, $2, $3)`,
		logicalID, chunkID, 0,
	); err != nil {
		t.Fatalf("insert file_chunk: %v", err)
	}

	// Must succeed — mismatched versions across deduped chunks is valid.
	if err := VerifyFileStandardWithContainersDir(dbconn, int(logicalID), t.TempDir()); err != nil {
		t.Fatalf("verify should accept mismatched chunk/file versions (dedup reuse); got: %v", err)
	}
}

// TestVerifyFileStandardAcceptsUnknownWellFormedVersions ensures verify stays
// decoupled from chunker implementation availability on read paths.
//
// If a future maintainer starts replaying chunkers during verify, this test
// should fail because these versions are intentionally unknown to this binary.
func TestVerifyFileStandardAcceptsUnknownWellFormedVersions(t *testing.T) {
	dbconn := openVerifyTestDB(t)
	defer func() { _ = dbconn.Close() }()

	var logicalID int64
	if err := dbconn.QueryRow(
		`INSERT INTO logical_file (original_name, total_size, file_hash, status, ref_count, chunker_version)
		 VALUES ($1, $2, $3, $4, $5, $6) RETURNING id`,
		"future-unknown-versions.txt",
		int64(512),
		strings.Repeat("1", 64),
		filestate.LogicalFileCompleted,
		int64(0),
		"v99-future-cdc",
	).Scan(&logicalID); err != nil {
		t.Fatalf("insert logical file: %v", err)
	}

	var containerID int64
	if err := dbconn.QueryRow(
		`INSERT INTO container (filename, current_size, max_size, sealed, quarantine) VALUES ($1, $2, $3, $4, $5) RETURNING id`,
		"future-unknown.bin", int64(4096), int64(4096), true, false,
	).Scan(&containerID); err != nil {
		t.Fatalf("insert container: %v", err)
	}

	var chunkID int64
	if err := dbconn.QueryRow(
		`INSERT INTO chunk (chunk_hash, size, status, live_ref_count, pin_count, retry_count, chunker_version)
		 VALUES ($1, $2, $3, $4, $5, $6, $7) RETURNING id`,
		strings.Repeat("2", 64),
		int64(512),
		filestate.ChunkCompleted,
		int64(1),
		int64(0),
		int64(0),
		"v77-origin-meta",
	).Scan(&chunkID); err != nil {
		t.Fatalf("insert chunk: %v", err)
	}

	if _, err := dbconn.Exec(
		`INSERT INTO blocks (chunk_id, container_id, block_offset, stored_size, plaintext_size, codec, format_version, nonce)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
		chunkID,
		containerID,
		int64(512),
		int64(512),
		int64(512),
		"plain",
		1,
		[]byte("nonce00000000000"),
	); err != nil {
		t.Fatalf("insert blocks row: %v", err)
	}

	if _, err := dbconn.Exec(
		`INSERT INTO file_chunk (logical_file_id, chunk_id, chunk_order) VALUES ($1, $2, $3)`,
		logicalID,
		chunkID,
		0,
	); err != nil {
		t.Fatalf("insert file_chunk: %v", err)
	}

	if err := VerifyFileStandardWithContainersDir(dbconn, int(logicalID), t.TempDir()); err != nil {
		t.Fatalf("verify should accept unknown well-formed version metadata; got: %v", err)
	}
}
