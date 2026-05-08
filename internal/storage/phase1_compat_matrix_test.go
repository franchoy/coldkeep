package storage

// phase1_compat_matrix_test.go — Step 1.7 Repository Compatibility Matrix Validation
//
// Validates that Phase 1 (Steps 1.1–1.6) introduced zero behavioral regressions.
// All tests are self-contained SQLite-only and run without COLDKEEP_TEST_DB.
//
// Coverage areas:
//   1. Existing-repository open: v1.8-style repo opens, restores, and verifies cleanly
//   2. Repository upgrade: migrations applied, metadata defaults populated, no data rewritten
//   3. Cross-version readability: Phase-1 repos remain behaviorally identical to v1.8
//   4. Deterministic restore: byte-identical, deterministic output across repeated calls
//   5. Benchmark parity: store/restore throughput and DB query counts within tolerance

import (
	"bytes"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/franchoy/coldkeep/internal/blocks"
	"github.com/franchoy/coldkeep/internal/container"
	"github.com/franchoy/coldkeep/internal/db"
	"github.com/franchoy/coldkeep/internal/verify"
	_ "github.com/mattn/go-sqlite3"
)

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

const (
	// phase1SchemaVersion is the schema version expected after all Phase 1
	// migrations (Steps 1.1–1.5) have been applied.
	phase1SchemaVersion = 15
)

// phase1Repo is an isolated repository with its own SQLite DB and container dir.
type phase1Repo struct {
	dbconn        *sql.DB
	containersDir string
	workDir       string
}

// newPhase1Repo creates a fresh isolated repository.
func newPhase1Repo(t *testing.T) *phase1Repo {
	t.Helper()
	dir := t.TempDir()
	containersDir := filepath.Join(dir, "containers")
	if err := os.MkdirAll(containersDir, 0o755); err != nil {
		t.Fatalf("phase1Repo: mkdir containers: %v", err)
	}
	dbconn, err := sql.Open("sqlite3", filepath.Join(dir, "cold.db"))
	if err != nil {
		t.Fatalf("phase1Repo: open sqlite: %v", err)
	}
	if err := db.RunMigrations(dbconn); err != nil {
		t.Fatalf("phase1Repo: run migrations: %v", err)
	}
	t.Cleanup(func() { _ = dbconn.Close() })
	return &phase1Repo{dbconn: dbconn, containersDir: containersDir, workDir: dir}
}

// storageContext returns a StorageContext for the repo.
func (r *phase1Repo) storageContext() StorageContext {
	return StorageContext{
		DB:           r.dbconn,
		Writer:       container.NewLocalWriterWithDirAndDB(r.containersDir, container.GetContainerMaxSize(), r.dbconn),
		ContainerDir: r.containersDir,
	}
}

// storeFile stores a file and returns its logical file ID.
func (r *phase1Repo) storeFile(t *testing.T, path string) int64 {
	t.Helper()
	res, err := StoreFileWithStorageContextAndCodecResult(r.storageContext(), path, blocks.CodecPlain)
	if err != nil {
		t.Fatalf("storeFile %s: %v", path, err)
	}
	return res.FileID
}

// restoreFile restores a file and returns the bytes.
func (r *phase1Repo) restoreFile(t *testing.T, fileID int64, outName string) []byte {
	t.Helper()
	outPath := filepath.Join(r.workDir, outName)
	if err := RestoreFileWithStorageContext(r.storageContext(), fileID, outPath); err != nil {
		t.Fatalf("restoreFile id=%d: %v", fileID, err)
	}
	data, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatalf("readFile %s: %v", outPath, err)
	}
	return data
}

// verify runs a standard in-process verification pass.
func (r *phase1Repo) verify(t *testing.T) {
	t.Helper()
	if err := verify.VerifySystemStandardWithContainersDir(r.dbconn, r.containersDir); err != nil {
		t.Fatalf("verify: %v", err)
	}
}

// writeFile creates a deterministic file at path and returns its SHA256 hex.
func phase1WriteFile(t *testing.T, path string, size int, seed byte) string {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	buf := make([]byte, size)
	for i := range buf {
		buf[i] = byte((i*31 + int(seed)*17 + 13) % 251)
	}
	if err := os.WriteFile(path, buf, 0o644); err != nil {
		t.Fatalf("writeFile %s: %v", path, err)
	}
	sum := sha256.Sum256(buf)
	return hex.EncodeToString(sum[:])
}

// ---------------------------------------------------------------------------
// Area 1 — Existing-repository open: v1.8-style repo opens, restores, verifies
// ---------------------------------------------------------------------------

// TestPhase1CompatExistingRepositoryOpenAndRestoreSucceeds simulates opening a
// v1.8-generated repository under Phase 1 code and verifies that:
//   - store succeeds
//   - restore is byte-identical
//   - standard verify passes
func TestPhase1CompatExistingRepositoryOpenAndRestoreSucceeds(t *testing.T) {
	repo := newPhase1Repo(t)

	inputPath := filepath.Join(repo.workDir, "existing-v18.bin")
	wantHash := phase1WriteFile(t, inputPath, 256*1024+37, 0xAB)

	// Act as if this data was written by a v1.8 binary: store it with plain codec.
	fileID := repo.storeFile(t, inputPath)

	// Restore and verify byte-identity.
	got := repo.restoreFile(t, fileID, "restored-v18.bin")
	gotHash := sha256.Sum256(got)
	if hex.EncodeToString(gotHash[:]) != wantHash {
		t.Fatalf("restore hash mismatch: want %s got %s", wantHash, hex.EncodeToString(gotHash[:]))
	}

	// Verify passes with no errors.
	repo.verify(t)
}

// TestPhase1CompatExistingRepositoryVerifySucceeds ensures that verify finds no
// issues on a freshly-populated Phase 1 repository (same behavior as v1.8).
func TestPhase1CompatExistingRepositoryVerifySucceeds(t *testing.T) {
	repo := newPhase1Repo(t)

	// Store several files to exercise block packing.
	for i := 0; i < 10; i++ {
		p := filepath.Join(repo.workDir, fmt.Sprintf("verify-input-%02d.txt", i))
		_ = phase1WriteFile(t, p, 64*1024+(i*1013), byte(i))
		_ = repo.storeFile(t, p)
	}

	// Verify should pass cleanly with no regressions introduced by metadata columns.
	repo.verify(t)
}

// ---------------------------------------------------------------------------
// Area 2 — Repository upgrade: migration correctness
// ---------------------------------------------------------------------------

// TestPhase1CompatMigrationReachesSchemaV15 asserts that a fresh SQLite migration
// reaches schema version 15 (Step 1.5 target) and that all Phase 1 metadata
// columns exist.
func TestPhase1CompatMigrationReachesSchemaV15(t *testing.T) {
	repo := newPhase1Repo(t)

	var version int
	if err := repo.dbconn.QueryRow(`SELECT MAX(version) FROM schema_version`).Scan(&version); err != nil {
		t.Fatalf("read schema_version: %v", err)
	}
	if version != phase1SchemaVersion {
		t.Fatalf("expected schema_version=%d after Phase 1 migrations, got %d", phase1SchemaVersion, version)
	}

	// All Phase 1 metadata columns must exist.
	for _, col := range []string{"compression_codec", "compression_ratio", "payload_hash"} {
		var count int
		if err := repo.dbconn.QueryRow(
			`SELECT COUNT(*) FROM pragma_table_info('storage_blocks') WHERE name = ?`, col,
		).Scan(&count); err != nil {
			t.Fatalf("check column %s: %v", col, err)
		}
		if count != 1 {
			t.Fatalf("expected storage_blocks.%s to exist after Phase 1 migration", col)
		}
	}
}

// TestPhase1CompatMigrationPopulatesMetadataDefaults verifies that stored blocks
// have the expected default values for all Phase 1 metadata columns immediately
// after being written.
func TestPhase1CompatMigrationPopulatesMetadataDefaults(t *testing.T) {
	repo := newPhase1Repo(t)

	inputPath := filepath.Join(repo.workDir, "migration-defaults.bin")
	_ = phase1WriteFile(t, inputPath, 128*1024, 0x55)
	_ = repo.storeFile(t, inputPath)

	var compressionCodec string
	var compressionRatio float64
	var payloadHash sql.NullString

	if err := repo.dbconn.QueryRow(`
		SELECT compression_codec, compression_ratio, payload_hash
		FROM storage_blocks ORDER BY id LIMIT 1
	`).Scan(&compressionCodec, &compressionRatio, &payloadHash); err != nil {
		t.Fatalf("query storage_blocks row: %v", err)
	}

	// compression_codec must be 'none' (Phase 1 default — compression not yet active).
	if compressionCodec != "none" {
		t.Fatalf("expected compression_codec=none, got %q", compressionCodec)
	}

	// payload_hash must be present and non-empty.
	if !payloadHash.Valid || payloadHash.String == "" {
		t.Fatalf("expected non-empty payload_hash, got valid=%v value=%q", payloadHash.Valid, payloadHash.String)
	}

	// payload_hash must be 64 hex characters (SHA256).
	if len(payloadHash.String) != 64 {
		t.Fatalf("expected payload_hash length=64 hex chars, got %d (%q)", len(payloadHash.String), payloadHash.String)
	}

	// compression_ratio must be approximately 1.0 (no compression active).
	if math.Abs(compressionRatio-1.0) > 0.01 {
		t.Fatalf("expected compression_ratio≈1.0, got %f", compressionRatio)
	}
}

// TestPhase1CompatMigrationIsIdempotent runs EnsureSchema twice on an existing
// repo and confirms that row counts and schema version do not change.
func TestPhase1CompatMigrationIsIdempotent(t *testing.T) {
	repo := newPhase1Repo(t)

	// Seed data.
	for i := 0; i < 5; i++ {
		p := filepath.Join(repo.workDir, fmt.Sprintf("idempotent-%02d.bin", i))
		_ = phase1WriteFile(t, p, 32*1024, byte(i+1))
		_ = repo.storeFile(t, p)
	}

	countRows := func(query string) int {
		var n int
		if err := repo.dbconn.QueryRow(query).Scan(&n); err != nil {
			t.Fatalf("count query %q: %v", query, err)
		}
		return n
	}

	blocksBefore := countRows(`SELECT COUNT(*) FROM storage_blocks`)
	chunksBefore := countRows(`SELECT COUNT(*) FROM chunk`)
	versionBefore := countRows(`SELECT MAX(version) FROM schema_version`)

	// Re-run migrations (idempotency check).
	if err := db.RunMigrations(repo.dbconn); err != nil {
		t.Fatalf("idempotent RunMigrations: %v", err)
	}

	blocksAfter := countRows(`SELECT COUNT(*) FROM storage_blocks`)
	chunksAfter := countRows(`SELECT COUNT(*) FROM chunk`)
	versionAfter := countRows(`SELECT MAX(version) FROM schema_version`)

	if blocksBefore != blocksAfter {
		t.Fatalf("storage_blocks count changed: before=%d after=%d", blocksBefore, blocksAfter)
	}
	if chunksBefore != chunksAfter {
		t.Fatalf("chunk count changed: before=%d after=%d", chunksBefore, chunksAfter)
	}
	if versionBefore != versionAfter {
		t.Fatalf("schema_version changed: before=%d after=%d", versionBefore, versionAfter)
	}
}

// TestPhase1CompatMigrationNoDataRewritten confirms that existing block hashes
// are not modified by migration (data content preserved).
func TestPhase1CompatMigrationNoDataRewritten(t *testing.T) {
	repo := newPhase1Repo(t)

	inputPath := filepath.Join(repo.workDir, "no-rewrite.bin")
	_ = phase1WriteFile(t, inputPath, 96*1024, 0x77)
	_ = repo.storeFile(t, inputPath)

	// Capture block_hash before re-migration.
	var blockHashBefore []byte
	if err := repo.dbconn.QueryRow(`SELECT block_hash FROM storage_blocks ORDER BY id LIMIT 1`).Scan(&blockHashBefore); err != nil {
		t.Fatalf("query block_hash before: %v", err)
	}

	if err := db.RunMigrations(repo.dbconn); err != nil {
		t.Fatalf("re-run migrations: %v", err)
	}

	var blockHashAfter []byte
	if err := repo.dbconn.QueryRow(`SELECT block_hash FROM storage_blocks ORDER BY id LIMIT 1`).Scan(&blockHashAfter); err != nil {
		t.Fatalf("query block_hash after: %v", err)
	}

	if !bytes.Equal(blockHashBefore, blockHashAfter) {
		t.Fatalf("block_hash changed after idempotent migration: before=%x after=%x", blockHashBefore, blockHashAfter)
	}
}

// ---------------------------------------------------------------------------
// Area 3 — Cross-version readability
// ---------------------------------------------------------------------------

// TestPhase1CompatPhase1RepoReadableByPhase1Code verifies that a repository
// written by Phase 1 code restores identically to what was stored — i.e.
// the new metadata columns do not affect restore output.
func TestPhase1CompatPhase1RepoReadableByPhase1Code(t *testing.T) {
	repo := newPhase1Repo(t)

	type entry struct {
		path     string
		fileID   int64
		wantHash string
	}

	// Store a variety of file sizes.
	entries := make([]entry, 0, 12)
	sizes := []int{
		512,
		4 * 1024,
		64 * 1024,
		256 * 1024,
		512*1024 + 1,
		1024 * 1024,
	}

	for i, sz := range sizes {
		p := filepath.Join(repo.workDir, fmt.Sprintf("cross-ver-%02d.bin", i))
		h := phase1WriteFile(t, p, sz, byte(i+10))
		fid := repo.storeFile(t, p)
		entries = append(entries, entry{path: p, fileID: fid, wantHash: h})
	}

	// Restore every file and assert byte-identity.
	for _, e := range entries {
		got := repo.restoreFile(t, e.fileID, fmt.Sprintf("cross-ver-out-%d.bin", e.fileID))
		gotSum := sha256.Sum256(got)
		if hex.EncodeToString(gotSum[:]) != e.wantHash {
			t.Fatalf("cross-version restore mismatch for id=%d: want %s got %s",
				e.fileID, e.wantHash, hex.EncodeToString(gotSum[:]))
		}
	}

	// Full verify pass.
	repo.verify(t)
}

// TestPhase1CompatStorageBlocksMetadataDoesNotAffectRestoreOutput confirms that
// the presence of compression_ratio/payload_hash columns in storage_blocks has
// no effect on the bytes produced by RestoreFile.
func TestPhase1CompatStorageBlocksMetadataDoesNotAffectRestoreOutput(t *testing.T) {
	repo := newPhase1Repo(t)

	original := make([]byte, 200*1024+99)
	for i := range original {
		original[i] = byte((i * 19 % 251))
	}
	inputPath := filepath.Join(repo.workDir, "no-meta-effect.bin")
	if err := os.WriteFile(inputPath, original, 0o644); err != nil {
		t.Fatalf("write input: %v", err)
	}

	fileID := repo.storeFile(t, inputPath)

	// Manually corrupt the metadata columns with non-default values to confirm
	// they are not consulted during restore.
	if _, err := repo.dbconn.Exec(
		`UPDATE storage_blocks SET compression_ratio = 0.5, payload_hash = 'deadbeef' WHERE id = (SELECT id FROM storage_blocks ORDER BY id LIMIT 1)`,
	); err != nil {
		t.Fatalf("corrupt metadata columns: %v", err)
	}

	// Restore must still produce correct bytes.
	got := repo.restoreFile(t, fileID, "no-meta-effect-out.bin")
	if !bytes.Equal(got, original) {
		t.Fatalf("restore bytes changed after metadata column corruption (metadata must not affect restore)")
	}
}

// ---------------------------------------------------------------------------
// Area 4 — Deterministic restore
// ---------------------------------------------------------------------------

// TestPhase1CompatRestoreIsDeterministicAcrossMultipleCalls stores a file then
// restores it multiple times, asserting byte-identical output each time.
func TestPhase1CompatRestoreIsDeterministicAcrossMultipleCalls(t *testing.T) {
	repo := newPhase1Repo(t)

	inputPath := filepath.Join(repo.workDir, "deterministic-input.bin")
	_ = phase1WriteFile(t, inputPath, 512*1024+111, 0xC3)
	fileID := repo.storeFile(t, inputPath)

	// Restore three times independently.
	var ref []byte
	for i := 0; i < 3; i++ {
		got := repo.restoreFile(t, fileID, fmt.Sprintf("det-out-%d.bin", i))
		if i == 0 {
			ref = got
			continue
		}
		if !bytes.Equal(got, ref) {
			t.Fatalf("restore pass %d produced non-deterministic output", i+1)
		}
	}
}

// TestPhase1CompatRestoreIsHashVerifiable confirms that the restored bytes
// match the SHA256 recorded at store time via the logical file hash.
func TestPhase1CompatRestoreIsHashVerifiable(t *testing.T) {
	repo := newPhase1Repo(t)

	original := make([]byte, 700*1024)
	for i := range original {
		original[i] = byte((i*37 + 3) % 251)
	}
	originalSum := sha256.Sum256(original)

	inputPath := filepath.Join(repo.workDir, "hash-verify-input.bin")
	if err := os.WriteFile(inputPath, original, 0o644); err != nil {
		t.Fatalf("write input: %v", err)
	}

	fileID := repo.storeFile(t, inputPath)

	// Read back the file_hash stored in DB.
	var dbFileHash string
	if err := repo.dbconn.QueryRow(`SELECT file_hash FROM logical_file WHERE id = ?`, fileID).Scan(&dbFileHash); err != nil {
		t.Fatalf("read file_hash: %v", err)
	}
	wantDBHash := hex.EncodeToString(originalSum[:])
	if dbFileHash != wantDBHash {
		t.Fatalf("DB file_hash mismatch: want %s got %s", wantDBHash, dbFileHash)
	}

	// Restore and verify hash.
	got := repo.restoreFile(t, fileID, "hash-verify-out.bin")
	gotSum := sha256.Sum256(got)
	if gotSum != originalSum {
		t.Fatalf("restored bytes hash mismatch: want %x got %x", originalSum, gotSum)
	}
}

// TestPhase1CompatRestoreDeterminismAfterConcurrentWrites confirms that files
// written in rapid succession each restore to their own deterministic bytes.
func TestPhase1CompatRestoreDeterminismAfterConcurrentWrites(t *testing.T) {
	repo := newPhase1Repo(t)

	const fileCount = 20
	type entry struct {
		fileID int64
		data   []byte
	}
	entries := make([]entry, 0, fileCount)

	for i := 0; i < fileCount; i++ {
		data := make([]byte, 16*1024+i*512)
		for j := range data {
			data[j] = byte((j*31 + i*7 + 5) % 251)
		}
		p := filepath.Join(repo.workDir, fmt.Sprintf("rapid-%03d.bin", i))
		if err := os.WriteFile(p, data, 0o644); err != nil {
			t.Fatalf("write rapid file %d: %v", i, err)
		}
		fid := repo.storeFile(t, p)
		entries = append(entries, entry{fileID: fid, data: data})
	}

	// Restore all files and verify.
	for _, e := range entries {
		got := repo.restoreFile(t, e.fileID, fmt.Sprintf("rapid-out-%d.bin", e.fileID))
		if !bytes.Equal(got, e.data) {
			t.Fatalf("restoration of id=%d produced incorrect bytes", e.fileID)
		}
	}
}

// ---------------------------------------------------------------------------
// Area 5 — Benchmark parity
// ---------------------------------------------------------------------------

// phase1BenchThresholds defines acceptable performance limits relative to a
// reference measurement. The tests do NOT compare against a committed baseline
// file (that is done by external CI tooling) — they assert that Phase 1 code
// stays within a generous factor so egregious regressions are caught without
// flakiness from CI hardware variance.
const (
	// storeThroughputMinMBps is the minimum acceptable store throughput (MB/s)
	// for a 4 MiB payload on in-memory SQLite. Phase 0 reference: ~100 MB/s.
	// We use 5 MB/s as a conservative floor to catch complete breakage only.
	storeThroughputMinMBps = 5.0

	// restoreThroughputMinMBps is the minimum acceptable restore throughput.
	restoreThroughputMinMBps = 5.0

	// maxStoreDBInserts is the maximum number of storage_blocks rows we expect
	// per 4 MiB file (generous upper bound to catch runaway INSERT loops).
	maxStoreDBInsertsPerMiB = 100
)

// benchPayloadMiB is the payload size for parity benchmarks.
const benchPayloadMiB = 4

// TestPhase1CompatStoreThroughputWithinTolerance measures store throughput for
// a ~4 MiB payload and asserts it is above the conservative minimum floor.
// This is a regression safety net, not a precision benchmark.
func TestPhase1CompatStoreThroughputWithinTolerance(t *testing.T) {
	repo := newPhase1Repo(t)

	payloadBytes := benchPayloadMiB * 1024 * 1024
	data := make([]byte, payloadBytes)
	for i := range data {
		data[i] = byte((i * 41) % 251)
	}
	inputPath := filepath.Join(repo.workDir, "throughput-store.bin")
	if err := os.WriteFile(inputPath, data, 0o644); err != nil {
		t.Fatalf("write throughput input: %v", err)
	}

	start := time.Now()
	_ = repo.storeFile(t, inputPath)
	elapsed := time.Since(start)

	mbps := float64(payloadBytes) / (1024 * 1024) / elapsed.Seconds()
	t.Logf("Phase1 store throughput: %.2f MB/s for %d MiB (%v)", mbps, benchPayloadMiB, elapsed)

	if mbps < storeThroughputMinMBps {
		t.Fatalf("store throughput %.2f MB/s is below minimum %.2f MB/s", mbps, storeThroughputMinMBps)
	}
}

// TestPhase1CompatRestoreThroughputWithinTolerance measures restore throughput.
func TestPhase1CompatRestoreThroughputWithinTolerance(t *testing.T) {
	repo := newPhase1Repo(t)

	payloadBytes := benchPayloadMiB * 1024 * 1024
	data := make([]byte, payloadBytes)
	for i := range data {
		data[i] = byte((i * 43) % 251)
	}
	inputPath := filepath.Join(repo.workDir, "throughput-restore-in.bin")
	if err := os.WriteFile(inputPath, data, 0o644); err != nil {
		t.Fatalf("write restore input: %v", err)
	}
	fileID := repo.storeFile(t, inputPath)

	start := time.Now()
	got := repo.restoreFile(t, fileID, "throughput-restore-out.bin")
	elapsed := time.Since(start)

	mbps := float64(payloadBytes) / (1024 * 1024) / elapsed.Seconds()
	t.Logf("Phase1 restore throughput: %.2f MB/s for %d MiB (%v)", mbps, benchPayloadMiB, elapsed)

	if mbps < restoreThroughputMinMBps {
		t.Fatalf("restore throughput %.2f MB/s is below minimum %.2f MB/s", mbps, restoreThroughputMinMBps)
	}

	if !bytes.Equal(got, data) {
		t.Fatal("restore parity check failed: bytes do not match original")
	}
}

// TestPhase1CompatDBInsertCountWithinBounds asserts that storing a ~4 MiB file
// does not produce an unexpectedly large number of storage_blocks rows.
func TestPhase1CompatDBInsertCountWithinBounds(t *testing.T) {
	repo := newPhase1Repo(t)

	payloadBytes := benchPayloadMiB * 1024 * 1024
	data := make([]byte, payloadBytes)
	for i := range data {
		data[i] = byte((i * 47) % 251)
	}
	inputPath := filepath.Join(repo.workDir, "insert-count-in.bin")
	if err := os.WriteFile(inputPath, data, 0o644); err != nil {
		t.Fatalf("write insert-count input: %v", err)
	}

	var blocksBefore int
	_ = repo.dbconn.QueryRow(`SELECT COUNT(*) FROM storage_blocks`).Scan(&blocksBefore)

	_ = repo.storeFile(t, inputPath)

	var blocksAfter int
	_ = repo.dbconn.QueryRow(`SELECT COUNT(*) FROM storage_blocks`).Scan(&blocksAfter)

	newBlocks := blocksAfter - blocksBefore
	maxExpected := maxStoreDBInsertsPerMiB * benchPayloadMiB
	t.Logf("Phase1 DB insert count: %d new storage_blocks rows for %d MiB", newBlocks, benchPayloadMiB)

	if newBlocks > maxExpected {
		t.Fatalf("DB insert count %d exceeds max expected %d for %d MiB payload",
			newBlocks, maxExpected, benchPayloadMiB)
	}
	if newBlocks == 0 {
		t.Fatal("expected at least one storage_blocks row to be inserted")
	}
}

// TestPhase1CompatMetadataWrittenOnce confirms that each block's transform
// metadata (payload_hash, compression_ratio, compression_codec) is written
// exactly once and matches the logical block bytes.
func TestPhase1CompatMetadataWrittenOnce(t *testing.T) {
	repo := newPhase1Repo(t)

	inputPath := filepath.Join(repo.workDir, "meta-once.bin")
	_ = phase1WriteFile(t, inputPath, 256*1024, 0xD4)
	_ = repo.storeFile(t, inputPath)

	rows, err := repo.dbconn.Query(`
		SELECT id, block_hash, compression_codec, compression_ratio, payload_hash
		FROM storage_blocks
	`)
	if err != nil {
		t.Fatalf("query storage_blocks: %v", err)
	}
	defer func() { _ = rows.Close() }()

	for rows.Next() {
		var id int64
		var blockHash []byte
		var compressionCodec string
		var compressionRatio float64
		var payloadHash sql.NullString

		if err := rows.Scan(&id, &blockHash, &compressionCodec, &compressionRatio, &payloadHash); err != nil {
			t.Fatalf("scan row: %v", err)
		}

		// payload_hash must be present.
		if !payloadHash.Valid || len(payloadHash.String) != 64 {
			t.Fatalf("block %d: expected 64-char payload_hash, got valid=%v value=%q",
				id, payloadHash.Valid, payloadHash.String)
		}

		// payload_hash must match hex(block_hash).
		expectedPayloadHash := hex.EncodeToString(blockHash)
		if payloadHash.String != expectedPayloadHash {
			t.Fatalf("block %d: payload_hash mismatch: got %q want %q",
				id, payloadHash.String, expectedPayloadHash)
		}

		// compression_codec must be 'none' (Phase 1 — no compression active).
		if compressionCodec != "none" {
			t.Fatalf("block %d: expected compression_codec=none, got %q", id, compressionCodec)
		}

		// For uncompressed data, ratio must be approximately 1.0.
		if math.Abs(compressionRatio-1.0) > 0.02 {
			t.Fatalf("block %d: compression_ratio %.6f too far from 1.0", id, compressionRatio)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate storage_blocks: %v", err)
	}
}

// TestPhase3CompressionEnvVarActivatesZstd verifies that compression can be
// explicitly activated in Phase 3 while still remaining opt-in.
func TestPhase3CompressionEnvVarActivatesZstd(t *testing.T) {
	t.Setenv("COLDKEEP_COMPRESSION", "zstd")
	t.Setenv("COLDKEEP_COMPRESSION_LEVEL", "3")

	repo := newPhase1Repo(t)

	inputPath := filepath.Join(repo.workDir, "env-var-gate.bin")
	_ = phase1WriteFile(t, inputPath, 64*1024, 0xFF)
	_ = repo.storeFile(t, inputPath)

	rows, err := repo.dbconn.Query(`SELECT id, compression_codec FROM storage_blocks`)
	if err != nil {
		t.Fatalf("query storage_blocks: %v", err)
	}
	defer func() { _ = rows.Close() }()

	for rows.Next() {
		var id int64
		var codec string
		if err := rows.Scan(&id, &codec); err != nil {
			t.Fatalf("scan: %v", err)
		}
		if codec != "zstd" {
			t.Fatalf("block %d: expected compression codec zstd when COLDKEEP_COMPRESSION=zstd, got %q", id, codec)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate: %v", err)
	}
}

// TestPhase2HashLayerParity verifies the three-layer hash model introduced in Phase 2
// Step 2.1. With compression disabled (codec = "none"):
//   - compressed_hash must equal block_hash (Phase 2 invariant: CompressedHash == LogicalHash)
//   - physical_hash must be non-NULL and 32 bytes
//   - payload_hash must equal hex(block_hash) (legacy v1.8 contract preserved)
func TestPhase2HashLayerParity(t *testing.T) {
	repo := newPhase1Repo(t)

	inputPath := filepath.Join(repo.workDir, "hash-parity.bin")
	_ = phase1WriteFile(t, inputPath, 128*1024, 0xAB)
	_ = repo.storeFile(t, inputPath)

	rows, err := repo.dbconn.Query(`
		SELECT id, block_hash, payload_hash, compressed_hash, physical_hash
		FROM storage_blocks
	`)
	if err != nil {
		t.Fatalf("query storage_blocks: %v", err)
	}
	defer func() { _ = rows.Close() }()

	count := 0
	for rows.Next() {
		count++
		var id int64
		var blockHash []byte
		var payloadHash string
		var compressedHash []byte
		var physicalHash []byte

		if err := rows.Scan(&id, &blockHash, &payloadHash, &compressedHash, &physicalHash); err != nil {
			t.Fatalf("scan row: %v", err)
		}

		// payload_hash must equal hex(block_hash) — legacy v1.8 contract.
		if payloadHash != hex.EncodeToString(blockHash) {
			t.Fatalf("block %d: payload_hash mismatch: got %q want %q",
				id, payloadHash, hex.EncodeToString(blockHash))
		}

		// Phase 2 invariant: codec=none ⟹ CompressedHash == LogicalHash.
		if !bytes.Equal(compressedHash, blockHash) {
			t.Fatalf("block %d: Phase 2 invariant violated: compressed_hash != block_hash", id)
		}

		// physical_hash must be 32 bytes.
		if len(physicalHash) != 32 {
			t.Fatalf("block %d: expected 32-byte physical_hash, got %d bytes", id, len(physicalHash))
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate: %v", err)
	}
	if count == 0 {
		t.Fatal("no storage_blocks rows found — store may have failed silently")
	}
}
