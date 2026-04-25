package storage

// Tests for Phase 5 / Step 11: inject fastcdc.Chunker{} into the store service
// and verify that the full store→restore→verify path works end-to-end.
//
// These tests do NOT change the production default. They prove that the Phase 1–4
// architecture (versioned logical_file.chunker_version, recipe-driven restore,
// version-agnostic verify) is correct when driven by v2-fastcdc.

import (
	"bytes"
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	"github.com/franchoy/coldkeep/internal/chunk"
	"github.com/franchoy/coldkeep/internal/chunk/fastcdc"
	"github.com/franchoy/coldkeep/internal/container"
	"github.com/franchoy/coldkeep/internal/db"
	"github.com/franchoy/coldkeep/internal/verify"
	_ "github.com/mattn/go-sqlite3"
)

// setupV2StoreDB creates an in-memory SQLite DB with all migrations applied.
func setupV2StoreDB(t *testing.T) *sql.DB {
	t.Helper()

	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	if err := db.RunMigrations(dbconn); err != nil {
		_ = dbconn.Close()
		t.Fatalf("run migrations: %v", err)
	}

	return dbconn
}

// v2TestStorageContext builds a StorageContext with fastcdc.Chunker{} injected
// and a local writer rooted at containersDir.
func v2TestStorageContext(dbconn *sql.DB, containersDir string) StorageContext {
	return StorageContext{
		DB:           dbconn,
		Writer:       container.NewLocalWriterWithDirAndDB(containersDir, container.GetContainerMaxSize(), dbconn),
		ContainerDir: containersDir,
		Chunker:      fastcdc.Chunker{},
	}
}

// makeV2TestFile writes deterministic content to a temp file of size bytes.
// The content is large enough to produce multiple v2-fastcdc chunks.
func makeV2TestFile(t *testing.T, name string, size int) (path string, wantBytes []byte) {
	t.Helper()
	// Use a non-uniform pattern so the CDC fingerprint triggers natural cuts.
	pattern := []byte("v2-fastcdc-store-path-test-phase5-")
	data := make([]byte, size)
	for i := range data {
		data[i] = pattern[i%len(pattern)]
	}
	dir := t.TempDir()
	path = filepath.Join(dir, name)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("write test file: %v", err)
	}
	return path, data
}

// TestV2ChunkerStoreSucceeds verifies that StoreFileWithStorageContextResult
// completes without error when fastcdc.Chunker{} is injected.
func TestV2ChunkerStoreSucceeds(t *testing.T) {
	t.Setenv("COLDKEEP_CODEC", "plain")
	containersDir := t.TempDir()
	dbconn := setupV2StoreDB(t)
	defer func() { _ = dbconn.Close() }()

	sgctx := v2TestStorageContext(dbconn, containersDir)

	// Use at least 4× AvgChunkSize to guarantee multiple chunks.
	inPath, _ := makeV2TestFile(t, "v2-store.bin", fastcdc.AvgChunkSize*4+512)

	result, err := StoreFileWithStorageContextResult(sgctx, inPath)
	if err != nil {
		t.Fatalf("StoreFileWithStorageContextResult with v2-fastcdc: %v", err)
	}
	if result.FileID == 0 {
		t.Fatal("expected non-zero FileID after store")
	}
}

// TestV2ChunkerLogicalFileVersionIsV2 verifies that logical_file.chunker_version
// is persisted as "v2-fastcdc" when fastcdc.Chunker{} is the active chunker.
func TestV2ChunkerLogicalFileVersionIsV2(t *testing.T) {
	t.Setenv("COLDKEEP_CODEC", "plain")
	containersDir := t.TempDir()
	dbconn := setupV2StoreDB(t)
	defer func() { _ = dbconn.Close() }()

	sgctx := v2TestStorageContext(dbconn, containersDir)
	inPath, _ := makeV2TestFile(t, "v2-logical-version.bin", fastcdc.AvgChunkSize*4+512)

	result, err := StoreFileWithStorageContextResult(sgctx, inPath)
	if err != nil {
		t.Fatalf("store: %v", err)
	}

	var logicalVersion string
	if err := dbconn.QueryRow(
		`SELECT chunker_version FROM logical_file WHERE id = $1`, result.FileID,
	).Scan(&logicalVersion); err != nil {
		t.Fatalf("read logical_file.chunker_version: %v", err)
	}

	if logicalVersion != string(chunk.VersionV2FastCDC) {
		t.Fatalf("logical_file.chunker_version: got %q want %q", logicalVersion, chunk.VersionV2FastCDC)
	}
}

// TestV2ChunkerAllChunkVersionsAreV2 verifies that every chunk.chunker_version
// row linked to the logical file is "v2-fastcdc".
func TestV2ChunkerAllChunkVersionsAreV2(t *testing.T) {
	t.Setenv("COLDKEEP_CODEC", "plain")
	containersDir := t.TempDir()
	dbconn := setupV2StoreDB(t)
	defer func() { _ = dbconn.Close() }()

	sgctx := v2TestStorageContext(dbconn, containersDir)
	inPath, _ := makeV2TestFile(t, "v2-chunk-versions.bin", fastcdc.AvgChunkSize*4+512)

	result, err := StoreFileWithStorageContextResult(sgctx, inPath)
	if err != nil {
		t.Fatalf("store: %v", err)
	}

	var mismatch int
	if err := dbconn.QueryRow(`
		SELECT COUNT(*)
		FROM chunk c
		JOIN file_chunk fc ON fc.chunk_id = c.id
		WHERE fc.logical_file_id = $1 AND c.chunker_version <> $2`,
		result.FileID, string(chunk.VersionV2FastCDC),
	).Scan(&mismatch); err != nil {
		t.Fatalf("count chunk version mismatches: %v", err)
	}
	if mismatch != 0 {
		t.Fatalf("expected all chunk rows to have chunker_version=%q, mismatches=%d",
			chunk.VersionV2FastCDC, mismatch)
	}
}

// TestV2ChunkerRestoreByteIdentical stores a file with v2-fastcdc and then
// restores it, asserting byte-for-byte identity with the original.
func TestV2ChunkerRestoreByteIdentical(t *testing.T) {
	t.Setenv("COLDKEEP_CODEC", "plain")
	containersDir := t.TempDir()
	dbconn := setupV2StoreDB(t)
	defer func() { _ = dbconn.Close() }()

	sgctx := v2TestStorageContext(dbconn, containersDir)
	inPath, wantBytes := makeV2TestFile(t, "v2-restore.bin", fastcdc.AvgChunkSize*4+512)

	result, err := StoreFileWithStorageContextResult(sgctx, inPath)
	if err != nil {
		t.Fatalf("store: %v", err)
	}

	outPath := filepath.Join(t.TempDir(), "v2-restored.bin")
	if _, err := restoreFileWithDBAndDir(dbconn, result.FileID, outPath, containersDir, RestoreOptions{Overwrite: true}); err != nil {
		t.Fatalf("restore: %v", err)
	}

	gotBytes, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatalf("read restored file: %v", err)
	}
	if !bytes.Equal(wantBytes, gotBytes) {
		t.Fatalf("restored content mismatch: original=%d bytes restored=%d bytes", len(wantBytes), len(gotBytes))
	}
}

// TestV2ChunkerVerifyPasses stores a file with v2-fastcdc and calls
// VerifyFileStandardWithContainersDir, asserting no error is returned.
func TestV2ChunkerVerifyPasses(t *testing.T) {
	t.Setenv("COLDKEEP_CODEC", "plain")
	containersDir := t.TempDir()
	dbconn := setupV2StoreDB(t)
	defer func() { _ = dbconn.Close() }()

	sgctx := v2TestStorageContext(dbconn, containersDir)
	inPath, _ := makeV2TestFile(t, "v2-verify.bin", fastcdc.AvgChunkSize*4+512)

	result, err := StoreFileWithStorageContextResult(sgctx, inPath)
	if err != nil {
		t.Fatalf("store: %v", err)
	}

	if err := verify.VerifyFileStandardWithContainersDir(dbconn, int(result.FileID), containersDir); err != nil {
		t.Fatalf("verify failed for v2-fastcdc stored file: %v", err)
	}
}

// TestStoreUsesRepositoryConfiguredDefaultChunker verifies that when no explicit
// StorageContext chunker override is provided, store resolves the write chunker
// from repository_config.default_chunker.
func TestStoreUsesRepositoryConfiguredDefaultChunker(t *testing.T) {
	t.Setenv("COLDKEEP_CODEC", "plain")
	containersDir := t.TempDir()
	dbconn := setupV2StoreDB(t)
	defer func() { _ = dbconn.Close() }()

	if _, err := dbconn.Exec(
		`UPDATE repository_config SET value = $1 WHERE key = 'default_chunker'`,
		string(chunk.VersionV2FastCDC),
	); err != nil {
		t.Fatalf("set repository default chunker to v2: %v", err)
	}

	sgctx := StorageContext{
		DB:           dbconn,
		Writer:       container.NewLocalWriterWithDirAndDB(containersDir, container.GetContainerMaxSize(), dbconn),
		ContainerDir: containersDir,
		Chunker:      nil,
	}

	inPath, _ := makeV2TestFile(t, "repo-default-v2.bin", fastcdc.AvgChunkSize*4+512)
	result, err := StoreFileWithStorageContextResult(sgctx, inPath)
	if err != nil {
		t.Fatalf("store with repository-configured default chunker: %v", err)
	}

	var logicalVersion string
	if err := dbconn.QueryRow(
		`SELECT chunker_version FROM logical_file WHERE id = $1`,
		result.FileID,
	).Scan(&logicalVersion); err != nil {
		t.Fatalf("read logical_file.chunker_version: %v", err)
	}

	if logicalVersion != string(chunk.VersionV2FastCDC) {
		t.Fatalf("logical_file.chunker_version: got %q want %q", logicalVersion, chunk.VersionV2FastCDC)
	}
}
