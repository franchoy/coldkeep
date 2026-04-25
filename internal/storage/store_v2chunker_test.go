package storage

// Tests for Phase 5 / Step 11: inject fastcdc.Chunker{} into the store service
// and verify that the full store→restore→verify path works end-to-end.
//
// These tests do NOT change the production default. They prove that the Phase 1–4
// architecture (versioned logical_file.chunker_version, recipe-driven restore,
// version-agnostic verify) is correct when driven by v2-fastcdc.

import (
	"bytes"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/franchoy/coldkeep/internal/chunk"
	"github.com/franchoy/coldkeep/internal/chunk/fastcdc"
	"github.com/franchoy/coldkeep/internal/container"
	"github.com/franchoy/coldkeep/internal/db"
	"github.com/franchoy/coldkeep/internal/verify"
	_ "github.com/mattn/go-sqlite3"
)

type singleChunkV2TestChunker struct{}

func (singleChunkV2TestChunker) Version() chunk.Version {
	return chunk.VersionV2FastCDC
}

func (singleChunkV2TestChunker) ChunkFile(path string) ([]chunk.Result, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return []chunk.Result{{Data: data}}, nil
}

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

// TestStoreWithDefaultV1PersistsLogicalVersion verifies that with no chunker
// override and no config switch, store uses the repository default v1 chunker.
func TestStoreWithDefaultV1PersistsLogicalVersion(t *testing.T) {
	t.Setenv("COLDKEEP_CODEC", "plain")
	containersDir := t.TempDir()
	dbconn := setupV2StoreDB(t)
	defer func() { _ = dbconn.Close() }()

	sgctx := StorageContext{
		DB:           dbconn,
		Writer:       container.NewLocalWriterWithDirAndDB(containersDir, container.GetContainerMaxSize(), dbconn),
		ContainerDir: containersDir,
		Chunker:      nil,
	}

	inPath, _ := makeV2TestFile(t, "repo-default-v1-explicit.bin", fastcdc.AvgChunkSize*4+701)
	result, err := StoreFileWithStorageContextResult(sgctx, inPath)
	if err != nil {
		t.Fatalf("store with default v1 chunker: %v", err)
	}

	var logicalVersion string
	if err := dbconn.QueryRow(
		`SELECT chunker_version FROM logical_file WHERE id = $1`,
		result.FileID,
	).Scan(&logicalVersion); err != nil {
		t.Fatalf("read logical_file.chunker_version: %v", err)
	}

	if logicalVersion != string(chunk.VersionV1SimpleRolling) {
		t.Fatalf("logical_file.chunker_version: got %q want %q", logicalVersion, chunk.VersionV1SimpleRolling)
	}
}

// TestStoreUsesRepositoryConfiguredDefaultChunker verifies that when no explicit
// StorageContext chunker override is provided, store resolves the write chunker
// from repository_config.default_chunker.
func TestStoreAfterSwitchToV2PersistsLogicalVersion(t *testing.T) {
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

// TestStoreResolvesChunkerOncePerOperation verifies that repository default
// changes are picked up between store operations, while each operation keeps a
// single resolved version for all rows it writes.
func TestStoreResolvesChunkerOncePerOperation(t *testing.T) {
	t.Setenv("COLDKEEP_CODEC", "plain")
	containersDir := t.TempDir()
	dbconn := setupV2StoreDB(t)
	defer func() { _ = dbconn.Close() }()

	sgctx := StorageContext{
		DB:           dbconn,
		Writer:       container.NewLocalWriterWithDirAndDB(containersDir, container.GetContainerMaxSize(), dbconn),
		ContainerDir: containersDir,
		Chunker:      nil,
	}

	if _, err := dbconn.Exec(
		`UPDATE repository_config SET value = $1 WHERE key = 'default_chunker'`,
		string(chunk.VersionV1SimpleRolling),
	); err != nil {
		t.Fatalf("set repository default chunker to v1: %v", err)
	}

	v1Path, v1Want := makeV2TestFile(t, "repo-default-v1.bin", fastcdc.AvgChunkSize*4+512)
	v1Result, err := StoreFileWithStorageContextResult(sgctx, v1Path)
	if err != nil {
		t.Fatalf("store using repository default v1: %v", err)
	}

	if _, err := dbconn.Exec(
		`UPDATE repository_config SET value = $1 WHERE key = 'default_chunker'`,
		string(chunk.VersionV2FastCDC),
	); err != nil {
		t.Fatalf("set repository default chunker to v2: %v", err)
	}

	v2Path, v2Want := makeV2TestFile(t, "repo-default-v2-after-switch.bin", fastcdc.AvgChunkSize*4+513)
	v2Result, err := StoreFileWithStorageContextResult(sgctx, v2Path)
	if err != nil {
		t.Fatalf("store using repository default v2: %v", err)
	}

	var logicalV1 string
	if err := dbconn.QueryRow(
		`SELECT chunker_version FROM logical_file WHERE id = $1`,
		v1Result.FileID,
	).Scan(&logicalV1); err != nil {
		t.Fatalf("read first logical_file.chunker_version: %v", err)
	}
	if logicalV1 != string(chunk.VersionV1SimpleRolling) {
		t.Fatalf("first logical_file.chunker_version: got %q want %q", logicalV1, chunk.VersionV1SimpleRolling)
	}

	var logicalV2 string
	if err := dbconn.QueryRow(
		`SELECT chunker_version FROM logical_file WHERE id = $1`,
		v2Result.FileID,
	).Scan(&logicalV2); err != nil {
		t.Fatalf("read second logical_file.chunker_version: %v", err)
	}
	if logicalV2 != string(chunk.VersionV2FastCDC) {
		t.Fatalf("second logical_file.chunker_version: got %q want %q", logicalV2, chunk.VersionV2FastCDC)
	}

	var v1ChunkMismatch int
	if err := dbconn.QueryRow(`
		SELECT COUNT(*)
		FROM chunk c
		JOIN file_chunk fc ON fc.chunk_id = c.id
		WHERE fc.logical_file_id = $1 AND c.chunker_version <> $2`,
		v1Result.FileID,
		string(chunk.VersionV1SimpleRolling),
	).Scan(&v1ChunkMismatch); err != nil {
		t.Fatalf("count v1 chunker_version mismatches: %v", err)
	}
	if v1ChunkMismatch != 0 {
		t.Fatalf("expected all chunks for first store op to remain %q, mismatches=%d", chunk.VersionV1SimpleRolling, v1ChunkMismatch)
	}

	var v2ChunkMismatch int
	if err := dbconn.QueryRow(`
		SELECT COUNT(*)
		FROM chunk c
		JOIN file_chunk fc ON fc.chunk_id = c.id
		WHERE fc.logical_file_id = $1 AND c.chunker_version <> $2`,
		v2Result.FileID,
		string(chunk.VersionV2FastCDC),
	).Scan(&v2ChunkMismatch); err != nil {
		t.Fatalf("count v2 chunker_version mismatches: %v", err)
	}
	if v2ChunkMismatch != 0 {
		t.Fatalf("expected all chunks for second store op to remain %q, mismatches=%d", chunk.VersionV2FastCDC, v2ChunkMismatch)
	}

	v1OutPath := filepath.Join(t.TempDir(), "mixed-v1-restored.bin")
	if _, err := restoreFileWithDBAndDir(dbconn, v1Result.FileID, v1OutPath, containersDir, RestoreOptions{Overwrite: true}); err != nil {
		t.Fatalf("restore first file: %v", err)
	}
	v1Got, err := os.ReadFile(v1OutPath)
	if err != nil {
		t.Fatalf("read first restored file: %v", err)
	}
	if !bytes.Equal(v1Want, v1Got) {
		t.Fatalf("first restored content mismatch: original=%d bytes restored=%d bytes", len(v1Want), len(v1Got))
	}

	v2OutPath := filepath.Join(t.TempDir(), "mixed-v2-restored.bin")
	if _, err := restoreFileWithDBAndDir(dbconn, v2Result.FileID, v2OutPath, containersDir, RestoreOptions{Overwrite: true}); err != nil {
		t.Fatalf("restore second file: %v", err)
	}
	v2Got, err := os.ReadFile(v2OutPath)
	if err != nil {
		t.Fatalf("read second restored file: %v", err)
	}
	if !bytes.Equal(v2Want, v2Got) {
		t.Fatalf("second restored content mismatch: original=%d bytes restored=%d bytes", len(v2Want), len(v2Got))
	}
}

// TestCrossVersionDedupCompatibility stores a file with v1, switches to v2,
// stores a similar file, and verifies the repository remains operational.
//
// This intentionally does not assert a strict dedup outcome because
// cross-version chunk boundaries may differ by design.
func TestCrossVersionDedupCompatibility(t *testing.T) {
	t.Setenv("COLDKEEP_CODEC", "plain")
	containersDir := t.TempDir()
	dbconn := setupV2StoreDB(t)
	defer func() { _ = dbconn.Close() }()

	sgctx := StorageContext{
		DB:           dbconn,
		Writer:       container.NewLocalWriterWithDirAndDB(containersDir, container.GetContainerMaxSize(), dbconn),
		ContainerDir: containersDir,
		Chunker:      nil,
	}

	if _, err := dbconn.Exec(
		`UPDATE repository_config SET value = $1 WHERE key = 'default_chunker'`,
		string(chunk.VersionV1SimpleRolling),
	); err != nil {
		t.Fatalf("set repository default chunker to v1: %v", err)
	}

	basePattern := []byte("cross-version-dedup-compatibility-pattern-")
	aData := make([]byte, fastcdc.AvgChunkSize*4+1500)
	for i := range aData {
		aData[i] = basePattern[i%len(basePattern)]
	}
	aPath := filepath.Join(t.TempDir(), "cross-version-a.bin")
	if err := os.WriteFile(aPath, aData, 0o600); err != nil {
		t.Fatalf("write file A: %v", err)
	}
	aResult, err := StoreFileWithStorageContextResult(sgctx, aPath)
	if err != nil {
		t.Fatalf("store file A with v1: %v", err)
	}

	if _, err := dbconn.Exec(
		`UPDATE repository_config SET value = $1 WHERE key = 'default_chunker'`,
		string(chunk.VersionV2FastCDC),
	); err != nil {
		t.Fatalf("set repository default chunker to v2: %v", err)
	}

	bData := append([]byte(nil), aData...)
	// Introduce sparse deterministic perturbations so files are similar but not identical.
	for i := 97; i < len(bData); i += 4096 {
		bData[i] ^= 0x1
	}
	bPath := filepath.Join(t.TempDir(), "cross-version-b.bin")
	if err := os.WriteFile(bPath, bData, 0o600); err != nil {
		t.Fatalf("write file B: %v", err)
	}
	bResult, err := StoreFileWithStorageContextResult(sgctx, bPath)
	if err != nil {
		t.Fatalf("store file B with v2: %v", err)
	}

	aOut := filepath.Join(t.TempDir(), "cross-version-a-restored.bin")
	if _, err := restoreFileWithDBAndDir(dbconn, aResult.FileID, aOut, containersDir, RestoreOptions{Overwrite: true}); err != nil {
		t.Fatalf("restore file A: %v", err)
	}
	aRestored, err := os.ReadFile(aOut)
	if err != nil {
		t.Fatalf("read restored file A: %v", err)
	}
	if !bytes.Equal(aData, aRestored) {
		t.Fatalf("file A restore mismatch: original=%d bytes restored=%d bytes", len(aData), len(aRestored))
	}

	bOut := filepath.Join(t.TempDir(), "cross-version-b-restored.bin")
	if _, err := restoreFileWithDBAndDir(dbconn, bResult.FileID, bOut, containersDir, RestoreOptions{Overwrite: true}); err != nil {
		t.Fatalf("restore file B: %v", err)
	}
	bRestored, err := os.ReadFile(bOut)
	if err != nil {
		t.Fatalf("read restored file B: %v", err)
	}
	if !bytes.Equal(bData, bRestored) {
		t.Fatalf("file B restore mismatch: original=%d bytes restored=%d bytes", len(bData), len(bRestored))
	}

	if err := verify.VerifyFileStandardWithContainersDir(dbconn, int(aResult.FileID), containersDir); err != nil {
		t.Fatalf("verify file A failed: %v", err)
	}
	if err := verify.VerifyFileStandardWithContainersDir(dbconn, int(bResult.FileID), containersDir); err != nil {
		t.Fatalf("verify file B failed: %v", err)
	}

	// Compatibility-only observability: query shared chunks across A/B to prove
	// the code path is exercised without constraining dedup outcomes.
	var sharedChunkRows int
	if err := dbconn.QueryRow(`
		SELECT COUNT(*)
		FROM file_chunk fa
		JOIN file_chunk fb ON fb.chunk_id = fa.chunk_id
		WHERE fa.logical_file_id = $1 AND fb.logical_file_id = $2`,
		aResult.FileID,
		bResult.FileID,
	).Scan(&sharedChunkRows); err != nil {
		t.Fatalf("count shared chunk rows across versions: %v", err)
	}
	if sharedChunkRows < 0 {
		t.Fatalf("shared chunk row count must be non-negative, got %d", sharedChunkRows)
	}
}

func TestStoreRejectsCrossVersionChunkReuseWithoutMixingOneFile(t *testing.T) {
	t.Setenv("COLDKEEP_CODEC", "plain")
	containersDir := t.TempDir()
	dbconn := setupV2StoreDB(t)
	defer func() { _ = dbconn.Close() }()

	payload := []byte("no-mixed-chunkers-within-one-file")
	hash := sha256.Sum256(payload)
	chunkHash := hex.EncodeToString(hash[:])

	if _, err := dbconn.Exec(
		`INSERT INTO chunk (chunk_hash, size, status, live_ref_count, chunker_version)
		 VALUES ($1, $2, $3, $4, $5)`,
		chunkHash,
		int64(len(payload)),
		"COMPLETED",
		int64(0),
		string(chunk.VersionV1SimpleRolling),
	); err != nil {
		t.Fatalf("insert existing v1 chunk row: %v", err)
	}

	inPath := filepath.Join(t.TempDir(), "cross-version-collision.bin")
	if err := os.WriteFile(inPath, payload, 0o600); err != nil {
		t.Fatalf("write input file: %v", err)
	}

	sgctx := StorageContext{
		DB:           dbconn,
		Writer:       container.NewLocalWriterWithDirAndDB(containersDir, container.GetContainerMaxSize(), dbconn),
		ContainerDir: containersDir,
		Chunker:      singleChunkV2TestChunker{},
	}

	_, err := StoreFileWithStorageContextResult(sgctx, inPath)
	if err == nil || !strings.Contains(err.Error(), "cross-version chunk reuse rejected") {
		t.Fatalf("expected cross-version chunk reuse rejection, got: %v", err)
	}

	var logicalID int64
	var status string
	if err := dbconn.QueryRow(
		`SELECT id, status FROM logical_file WHERE file_hash = $1 AND total_size = $2`,
		chunkHash,
		int64(len(payload)),
	).Scan(&logicalID, &status); err != nil {
		t.Fatalf("read claimed logical_file row: %v", err)
	}
	if status != "ABORTED" {
		t.Fatalf("expected failed store logical_file status ABORTED, got %q", status)
	}

	var linkedRows int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM file_chunk WHERE logical_file_id = $1`, logicalID).Scan(&linkedRows); err != nil {
		t.Fatalf("count file_chunk rows for failed logical file: %v", err)
	}
	if linkedRows != 0 {
		t.Fatalf("expected no file_chunk rows for failed logical file, got %d", linkedRows)
	}

	var chunkRows int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM chunk WHERE chunk_hash = $1 AND size = $2`, chunkHash, int64(len(payload))).Scan(&chunkRows); err != nil {
		t.Fatalf("count chunk rows by dedup identity: %v", err)
	}
	if chunkRows != 1 {
		t.Fatalf("expected dedup identity hash+size to remain one row, got %d", chunkRows)
	}

	var persistedVersion string
	if err := dbconn.QueryRow(`SELECT chunker_version FROM chunk WHERE chunk_hash = $1 AND size = $2`, chunkHash, int64(len(payload))).Scan(&persistedVersion); err != nil {
		t.Fatalf("read persisted chunk version: %v", err)
	}
	if persistedVersion != string(chunk.VersionV1SimpleRolling) {
		t.Fatalf("expected existing chunk row version to remain %q, got %q", chunk.VersionV1SimpleRolling, persistedVersion)
	}
}
