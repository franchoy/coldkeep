package main

import (
	"bytes"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/franchoy/coldkeep/internal/blocks"
	"github.com/franchoy/coldkeep/internal/chunk"
	"github.com/franchoy/coldkeep/internal/container"
	"github.com/franchoy/coldkeep/internal/db"
	"github.com/franchoy/coldkeep/internal/maintenance"
	"github.com/franchoy/coldkeep/internal/recovery"
	"github.com/franchoy/coldkeep/internal/storage"
	storagecompression "github.com/franchoy/coldkeep/internal/storage/compression"
	"github.com/franchoy/coldkeep/internal/verify"
	testutils "github.com/franchoy/coldkeep/tests/utils"
	"github.com/franchoy/coldkeep/tests/utils/testgate"
)

// Step 6.12 — Validate Legacy Repository Upgrade Path
//
// Core guarantee:
//   v1.8 repositories upgrade safely to v1.9 without rewriting existing data.
//
// Required validations:
//   ✔ old data untouched
//   ✔ old blocks readable
//   ✔ new blocks coexist safely
//   ✔ migration only additive

type step612BlockRow struct {
	ChunkID       int64
	Codec         string
	FormatVersion int
	PlainSize     int64
	StoredSize    int64
	ContainerID   int64
	BlockOffset   int64
	Nonce         []byte
}

type step612ContainerSnapshot struct {
	ID       int64
	Filename string
	Size     int64
	Hash     string
}

func TestStep612LegacyUpgradePathNoRewriteIntegration(t *testing.T) {
	testgate.RequireDB(t)

	tmp := prepareReadPathRegressionRepo(t)
	setRepoChunkerVersion(t, chunk.VersionV1SimpleRolling)

	dbconn, err := db.ConnectDB()
	if err != nil {
		t.Fatalf("connect DB for step 6.12 fixture: %v", err)
	}

	legacyRoot := filepath.Join(tmp, "step612-legacy-input")
	legacySet := createPhase7FixtureInputSet(t, legacyRoot)

	legacyPaths := []string{
		legacySet.largePath,
		legacySet.manySmallPaths[0],
		legacySet.manySmallPaths[1],
		legacySet.duplicatePathA,
		legacySet.duplicatePathB,
	}

	legacyExpectedHashes := make(map[string]string, len(legacyPaths))
	for _, p := range legacyPaths {
		legacyExpectedHashes[p] = testutils.SHA256File(t, p)
	}

	sgctx := storage.StorageContext{
		DB:           dbconn,
		Writer:       container.NewLocalWriter(container.GetContainerMaxSize()),
		ContainerDir: container.ContainersDir,
	}

	for _, p := range legacyPaths {
		if _, err := storage.StoreFileWithStorageContextAndCodecResultWithPolicy(sgctx, p, blocks.CodecPlain, false); err != nil {
			_ = dbconn.Close()
			t.Fatalf("seed v1.8-style file %s: %v", p, err)
		}
	}

	legacyLogicalIDs := make(map[string]int64, len(legacyPaths))
	for _, p := range legacyPaths {
		legacyLogicalIDs[p] = logicalIDForStoredPath(t, dbconn, filepath.ToSlash(p))
	}

	var oldMaxChunkID int64
	if err := dbconn.QueryRow(`SELECT COALESCE(MAX(id), 0) FROM chunk`).Scan(&oldMaxChunkID); err != nil {
		_ = dbconn.Close()
		t.Fatalf("query old max chunk id: %v", err)
	}

	oldBlocks := snapshotLegacyBlockRowsStep612(t, dbconn, oldMaxChunkID)
	oldContainers := snapshotLegacyContainersStep612(t, dbconn, oldMaxChunkID, container.ContainersDir)

	// Force old containers sealed before migration/new writes to avoid append reuse.
	if _, err := dbconn.Exec(`
		UPDATE container
		SET sealed = TRUE
		WHERE id IN (
			SELECT DISTINCT b.container_id
			FROM blocks b
			JOIN chunk c ON c.id = b.chunk_id
			WHERE c.id <= $1
		)
	`, oldMaxChunkID); err != nil {
		_ = dbconn.Close()
		t.Fatalf("seal old containers before upgrade: %v", err)
	}

	// Emulate true v1.8 metadata shape (legacy blocks table only).
	if _, err := dbconn.Exec(`DELETE FROM chunk_block_refs`); err != nil {
		_ = dbconn.Close()
		t.Fatalf("delete chunk_block_refs for v1.8 emulation: %v", err)
	}
	if _, err := dbconn.Exec(`DELETE FROM storage_blocks`); err != nil {
		_ = dbconn.Close()
		t.Fatalf("delete storage_blocks for v1.8 emulation: %v", err)
	}

	if err := dbconn.Close(); err != nil {
		t.Fatalf("close db before upgrade reopen: %v", err)
	}

	// Upgrade to v1.9 runtime path.
	if err := recovery.SystemRecoveryWithContainersDir(container.ContainersDir); err != nil {
		t.Fatalf("upgrade v1.8 repository to v1.9 runtime: %v", err)
	}

	dbconn, err = db.ConnectDB()
	if err != nil {
		t.Fatalf("reconnect DB after upgrade: %v", err)
	}
	defer func() { _ = dbconn.Close() }()

	// Store new compressed data post-upgrade.
	setCompressionDefaultsStep612(t, dbconn, storagecompression.CompressionZstd, 3)

	newRoot := filepath.Join(tmp, "step612-new-input")
	newCompressedPath := filepath.Join(newRoot, "new-compressed.bin")
	if err := os.MkdirAll(newRoot, 0o755); err != nil {
		t.Fatalf("mkdir new root: %v", err)
	}
	writeHighlyCompressibleFileStep612(t, newCompressedPath, 2*1024*1024+777)
	newCompressedHash := testutils.SHA256File(t, newCompressedPath)

	newSgctx := storage.StorageContext{
		DB:           dbconn,
		Writer:       container.NewLocalWriter(container.GetContainerMaxSize()),
		ContainerDir: container.ContainersDir,
	}

	newRes, err := storage.StoreFileWithStorageContextAndCodecResult(newSgctx, newCompressedPath, blocks.CodecPlain)
	if err != nil {
		t.Fatalf("store new compressed file after upgrade: %v", err)
	}

	var newCompressedBlocks int64
	if err := dbconn.QueryRow(`
		SELECT COUNT(*)
		FROM file_chunk fc
		JOIN chunk_block_refs r ON r.chunk_id = fc.chunk_id
		JOIN storage_blocks sb ON sb.id = r.block_id
		WHERE fc.logical_file_id = $1 AND sb.compression_codec = 'zstd'
	`, newRes.FileID).Scan(&newCompressedBlocks); err != nil {
		t.Fatalf("count zstd blocks for new file: %v", err)
	}
	if newCompressedBlocks == 0 {
		t.Fatalf("expected new file to produce compressed zstd blocks")
	}

	// No automatic recompression / no block rewriting: old chunks must remain on legacy path.
	var oldPackedRefs int64
	if err := dbconn.QueryRow(`
		SELECT COUNT(*)
		FROM chunk c
		JOIN chunk_block_refs r ON r.chunk_id = c.id
		WHERE c.id <= $1
	`, oldMaxChunkID).Scan(&oldPackedRefs); err != nil {
		t.Fatalf("count packed refs for old chunks: %v", err)
	}
	if oldPackedRefs != 0 {
		t.Fatalf("expected no packed refs for old chunks (no auto recompression), got=%d", oldPackedRefs)
	}

	postUpgradeBlocks := snapshotLegacyBlockRowsStep612(t, dbconn, oldMaxChunkID)
	if !equalLegacyBlocksStep612(oldBlocks, postUpgradeBlocks) {
		t.Fatalf("legacy blocks rows changed during upgrade/new stores (block rewriting detected)")
	}

	postUpgradeContainers := snapshotLegacyContainersStep612(t, dbconn, oldMaxChunkID, container.ContainersDir)
	if !equalLegacyContainersStep612(oldContainers, postUpgradeContainers) {
		t.Fatalf("legacy container files changed during upgrade/new stores (container rewriting detected)")
	}

	// Restore old + new data.
	restoreDir := filepath.Join(tmp, "step612-restore")
	if err := os.MkdirAll(restoreDir, 0o755); err != nil {
		t.Fatalf("mkdir restore dir: %v", err)
	}

	for path, logicalID := range legacyLogicalIDs {
		out := filepath.Join(restoreDir, "old-"+filepath.Base(path))
		if err := storage.RestoreFileWithDB(dbconn, logicalID, out); err != nil {
			t.Fatalf("restore old file %s (id=%d): %v", path, logicalID, err)
		}
		if got := testutils.SHA256File(t, out); got != legacyExpectedHashes[path] {
			t.Fatalf("old file hash mismatch for %s: got=%s want=%s", path, got, legacyExpectedHashes[path])
		}
	}

	newOut := filepath.Join(restoreDir, "new-compressed.restored.bin")
	if err := storage.RestoreFileWithDB(dbconn, newRes.FileID, newOut); err != nil {
		t.Fatalf("restore new compressed file (id=%d): %v", newRes.FileID, err)
	}
	if got := testutils.SHA256File(t, newOut); got != newCompressedHash {
		t.Fatalf("new file hash mismatch: got=%s want=%s", got, newCompressedHash)
	}

	// Verify old + new data health together.
	t.Setenv("COLDKEEP_VERIFY_STRICT_SEGMENTS", "1")
	if err := maintenance.VerifyCommandWithContainersDir(container.ContainersDir, "system", 0, verify.VerifyStandard); err != nil {
		t.Fatalf("verify mixed old+new repository after upgrade: %v", err)
	}

	// Migration only additive for old data path.
	var oldMissingLegacyRows int64
	if err := dbconn.QueryRow(`
		SELECT COUNT(*)
		FROM chunk c
		LEFT JOIN blocks b ON b.chunk_id = c.id
		WHERE c.id <= $1 AND c.status = 'COMPLETED' AND b.id IS NULL
	`, oldMaxChunkID).Scan(&oldMissingLegacyRows); err != nil {
		t.Fatalf("count old completed chunks missing legacy rows: %v", err)
	}
	if oldMissingLegacyRows != 0 {
		t.Fatalf("old completed chunks lost legacy rows after upgrade, missing=%d", oldMissingLegacyRows)
	}

	var newPackedRows int64
	if err := dbconn.QueryRow(`
		SELECT COUNT(*)
		FROM chunk c
		JOIN chunk_block_refs r ON r.chunk_id = c.id
		JOIN storage_blocks sb ON sb.id = r.block_id
		WHERE c.id > $1
	`, oldMaxChunkID).Scan(&newPackedRows); err != nil {
		t.Fatalf("count packed rows for new chunks: %v", err)
	}
	if newPackedRows == 0 {
		t.Fatalf("expected packed rows for new chunks after upgrade")
	}
}

func setCompressionDefaultsStep612(t *testing.T, dbconn *sql.DB, codec string, level int) {
	t.Helper()

	t.Setenv("COLDKEEP_COMPRESSION", codec)
	t.Setenv("COLDKEEP_COMPRESSION_LEVEL", fmt.Sprintf("%d", level))

	tx, err := dbconn.Begin()
	if err != nil {
		t.Fatalf("begin compression tx: %v", err)
	}
	if err := storage.SetDefaultCompression(tx, codec); err != nil {
		t.Fatalf("set default compression: %v", err)
	}
	if codec == storagecompression.CompressionZstd {
		if err := storage.SetDefaultCompressionLevel(tx, level); err != nil {
			t.Fatalf("set default compression level: %v", err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit compression tx: %v", err)
	}
}

func writeHighlyCompressibleFileStep612(t *testing.T, path string, size int) {
	t.Helper()

	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir parent for %s: %v", path, err)
	}
	p := make([]byte, size)
	seed := []byte("step-612-compressible-upgrade-path-")
	for i := range p {
		p[i] = seed[i%len(seed)]
	}
	if err := os.WriteFile(path, p, 0o644); err != nil {
		t.Fatalf("write compressible file %s: %v", path, err)
	}
}

func snapshotLegacyBlockRowsStep612(t *testing.T, dbconn *sql.DB, oldMaxChunkID int64) []step612BlockRow {
	t.Helper()

	rows, err := dbconn.Query(`
		SELECT chunk_id, codec, format_version, plaintext_size, stored_size, container_id, block_offset, nonce
		FROM blocks
		WHERE chunk_id <= $1
		ORDER BY chunk_id ASC
	`, oldMaxChunkID)
	if err != nil {
		t.Fatalf("query legacy block rows snapshot: %v", err)
	}
	defer rows.Close()

	out := make([]step612BlockRow, 0, 256)
	for rows.Next() {
		var r step612BlockRow
		if err := rows.Scan(&r.ChunkID, &r.Codec, &r.FormatVersion, &r.PlainSize, &r.StoredSize, &r.ContainerID, &r.BlockOffset, &r.Nonce); err != nil {
			t.Fatalf("scan legacy block row: %v", err)
		}
		if r.Nonce != nil {
			r.Nonce = append([]byte(nil), r.Nonce...)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate legacy block rows: %v", err)
	}
	return out
}

func snapshotLegacyContainersStep612(t *testing.T, dbconn *sql.DB, oldMaxChunkID int64, containersDir string) []step612ContainerSnapshot {
	t.Helper()

	rows, err := dbconn.Query(`
		SELECT DISTINCT ctr.id, ctr.filename, ctr.current_size
		FROM container ctr
		JOIN blocks b ON b.container_id = ctr.id
		JOIN chunk c ON c.id = b.chunk_id
		WHERE c.id <= $1
		ORDER BY ctr.id ASC
	`, oldMaxChunkID)
	if err != nil {
		t.Fatalf("query legacy containers snapshot: %v", err)
	}
	defer rows.Close()

	out := make([]step612ContainerSnapshot, 0, 32)
	for rows.Next() {
		var s step612ContainerSnapshot
		if err := rows.Scan(&s.ID, &s.Filename, &s.Size); err != nil {
			t.Fatalf("scan legacy container snapshot: %v", err)
		}
		path := filepath.Join(containersDir, s.Filename)
		s.Hash = testutils.SHA256File(t, path)
		out = append(out, s)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate legacy container snapshot rows: %v", err)
	}
	return out
}

func equalLegacyBlocksStep612(a, b []step612BlockRow) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].ChunkID != b[i].ChunkID ||
			a[i].Codec != b[i].Codec ||
			a[i].FormatVersion != b[i].FormatVersion ||
			a[i].PlainSize != b[i].PlainSize ||
			a[i].StoredSize != b[i].StoredSize ||
			a[i].ContainerID != b[i].ContainerID ||
			a[i].BlockOffset != b[i].BlockOffset ||
			!bytes.Equal(a[i].Nonce, b[i].Nonce) {
			return false
		}
	}
	return true
}

func equalLegacyContainersStep612(a, b []step612ContainerSnapshot) bool {
	if len(a) != len(b) {
		return false
	}
	index := make(map[int64]step612ContainerSnapshot, len(a))
	for _, s := range a {
		index[s.ID] = s
	}
	sort.Slice(b, func(i, j int) bool { return b[i].ID < b[j].ID })
	for _, s := range b {
		base, ok := index[s.ID]
		if !ok {
			return false
		}
		if base.Filename != s.Filename || base.Size != s.Size || base.Hash != s.Hash {
			return false
		}
	}
	return true
}
