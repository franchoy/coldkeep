package storage

import (
	"bytes"
	"database/sql"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/franchoy/coldkeep/internal/blocks"
	storagecompression "github.com/franchoy/coldkeep/internal/storage/compression"
	verifypkg "github.com/franchoy/coldkeep/internal/verify"
)

type chunkGraphRef struct {
	ChunkHash string
	Offset    int64
	Size      int64
}

type blockSnapshot struct {
	ID               int64
	BlockHash        []byte
	CompressedHash   []byte
	PhysicalHash     []byte
	ContainerID      int64
	ContainerFile    string
	ContainerOffset  int64
	StoredSize       int64
	CompressionCodec string
	CompressionLevel sql.NullInt64
	PlaintextSize    int64
	CompressedSize   int64
	StoredBytes      []byte
}

func writeFeatureGateInputFile(t *testing.T, data []byte) string {
	t.Helper()
	inPath := filepath.Join(t.TempDir(), "feature-gate-input.bin")
	if err := os.WriteFile(inPath, data, 0o600); err != nil {
		t.Fatalf("write input file: %v", err)
	}
	return inPath
}

func readStoredBlockCompressionMetaForFile(t *testing.T, dbconn *sql.DB, fileID int64) (codec string, plaintextSize, compressedSize, storedSize int64) {
	t.Helper()
	if err := dbconn.QueryRow(`
		SELECT b.compression_codec, b.plaintext_size, b.compressed_size, b.stored_size
		FROM storage_blocks b
		JOIN chunk_block_refs r ON r.block_id = b.id
		JOIN file_chunk fc ON fc.chunk_id = r.chunk_id
		WHERE fc.logical_file_id = $1
		ORDER BY b.id ASC
		LIMIT 1
	`, fileID).Scan(&codec, &plaintextSize, &compressedSize, &storedSize); err != nil {
		t.Fatalf("read storage block compression metadata for file %d: %v", fileID, err)
	}
	return codec, plaintextSize, compressedSize, storedSize
}

func TestCompressionFixtureDefaultsToNone(t *testing.T) {
	repo := NewTestRepository(t, WithCompression("none"))
	payload := bytes.Repeat([]byte("none-default-feature-gate-"), 128)

	inPath := writeFeatureGateInputFile(t, payload)
	result, err := StoreFileWithStorageContextAndCodecResult(repo.Storage, inPath, blocks.CodecPlain)
	if err != nil {
		t.Fatalf("store with explicit none compression fixture: %v", err)
	}

	codec, plaintextSize, compressedSize, storedSize := readStoredBlockCompressionMetaForFile(t, repo.DB, result.FileID)
	if codec != "none" {
		t.Fatalf("expected compression_codec=none in default fixture, got %q", codec)
	}
	if plaintextSize <= 0 || compressedSize <= 0 || storedSize <= 0 {
		t.Fatalf("expected positive size metadata, got plaintext=%d compressed=%d stored=%d", plaintextSize, compressedSize, storedSize)
	}
	if compressedSize != plaintextSize {
		t.Fatalf("expected none compression to keep compressed_size==plaintext_size, got compressed=%d plaintext=%d", compressedSize, plaintextSize)
	}
}

func TestFeatureGatedCompressionZstdStoreAndRestore(t *testing.T) {
	RequireTestCompression(t, "zstd")

	repo := NewTestRepository(t, WithCompression("zstd"), WithCompressionLevel(3))
	payload := bytes.Repeat([]byte("zstd-feature-gate-repetitive-payload-"), 4096)

	inPath := writeFeatureGateInputFile(t, payload)
	result, err := StoreFileWithStorageContextAndCodecResult(repo.Storage, inPath, blocks.CodecPlain)
	if err != nil {
		t.Fatalf("store with zstd feature gate: %v", err)
	}

	codec, plaintextSize, compressedSize, _ := readStoredBlockCompressionMetaForFile(t, repo.DB, result.FileID)
	if codec != "zstd" {
		t.Fatalf("expected compression_codec=zstd, got %q", codec)
	}
	if compressedSize >= plaintextSize {
		t.Fatalf("expected zstd to reduce repetitive payload size, got compressed=%d plaintext=%d", compressedSize, plaintextSize)
	}

	outPath := filepath.Join(t.TempDir(), "feature-gate-zstd-restore.bin")
	if _, err := restoreFileWithDBAndDir(repo.DB, result.FileID, outPath, repo.ContainersDir, RestoreOptions{Overwrite: true}); err != nil {
		t.Fatalf("restore zstd feature-gated file: %v", err)
	}

	restored, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatalf("read restored file: %v", err)
	}
	if !bytes.Equal(restored, payload) {
		t.Fatalf("restored payload mismatch for zstd feature-gated test")
	}
}

type blockCompressionMetadata struct {
	Codec          string
	CompressionLvl sql.NullInt64
	PlaintextSize  int64
	CompressedSize int64
	StoredSize     int64
	BlockHash      []byte
	CompressedHash []byte
	PhysicalHash   []byte
}

func readStoredBlockMetaWithHashesForFile(t *testing.T, dbconn *sql.DB, fileID int64) blockCompressionMetadata {
	t.Helper()

	var row blockCompressionMetadata
	if err := dbconn.QueryRow(`
		SELECT b.compression_codec, b.compression_level,
		       b.plaintext_size, b.compressed_size, b.stored_size,
		       b.block_hash, b.compressed_hash, b.physical_hash
		FROM storage_blocks b
		JOIN chunk_block_refs r ON r.block_id = b.id
		JOIN file_chunk fc ON fc.chunk_id = r.chunk_id
		WHERE fc.logical_file_id = $1
		ORDER BY b.id ASC
		LIMIT 1
	`, fileID).Scan(
		&row.Codec,
		&row.CompressionLvl,
		&row.PlaintextSize,
		&row.CompressedSize,
		&row.StoredSize,
		&row.BlockHash,
		&row.CompressedHash,
		&row.PhysicalHash,
	); err != nil {
		t.Fatalf("read storage block metadata with hashes for file %d: %v", fileID, err)
	}
	return row
}

func deterministicNoise(size int) []byte {
	out := make([]byte, size)
	var x uint32 = 0x9e3779b9
	for i := 0; i < size; i++ {
		x ^= x << 13
		x ^= x >> 17
		x ^= x << 5
		out[i] = byte(x)
	}
	return out
}

func readFileChunkGraph(t *testing.T, dbconn *sql.DB, fileID int64) []chunkGraphRef {
	t.Helper()

	rows, err := dbconn.Query(`
		SELECT c.chunk_hash, r.offset_in_block, r.size_in_block
		FROM file_chunk fc
		JOIN chunk c ON c.id = fc.chunk_id
		JOIN chunk_block_refs r ON r.chunk_id = c.id
		WHERE fc.logical_file_id = $1
		ORDER BY fc.chunk_order, r.offset_in_block
	`, fileID)
	if err != nil {
		t.Fatalf("query file chunk graph for file %d: %v", fileID, err)
	}
	defer func() { _ = rows.Close() }()

	graph := make([]chunkGraphRef, 0)
	for rows.Next() {
		var ref chunkGraphRef
		if err := rows.Scan(&ref.ChunkHash, &ref.Offset, &ref.Size); err != nil {
			t.Fatalf("scan chunk graph row for file %d: %v", fileID, err)
		}
		graph = append(graph, ref)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate chunk graph rows for file %d: %v", fileID, err)
	}
	return graph
}

func readDistinctBlockIDsForFile(t *testing.T, dbconn *sql.DB, fileID int64) []int64 {
	t.Helper()

	rows, err := dbconn.Query(`
		SELECT DISTINCT r.block_id
		FROM file_chunk fc
		JOIN chunk_block_refs r ON r.chunk_id = fc.chunk_id
		WHERE fc.logical_file_id = $1
		ORDER BY r.block_id
	`, fileID)
	if err != nil {
		t.Fatalf("query distinct block ids for file %d: %v", fileID, err)
	}
	defer func() { _ = rows.Close() }()

	ids := make([]int64, 0)
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			t.Fatalf("scan block id for file %d: %v", fileID, err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate block ids for file %d: %v", fileID, err)
	}
	return ids
}

func snapshotBlocksForFile(t *testing.T, repo *TestRepository, fileID int64) []blockSnapshot {
	t.Helper()

	rows, err := repo.DB.Query(`
		SELECT DISTINCT
			sb.id,
			sb.block_hash,
			sb.compressed_hash,
			sb.physical_hash,
			sb.container_id,
			c.filename,
			sb.container_offset,
			sb.stored_size,
			sb.compression_codec,
			sb.compression_level,
			sb.plaintext_size,
			sb.compressed_size
		FROM storage_blocks sb
		JOIN container c ON c.id = sb.container_id
		JOIN chunk_block_refs r ON r.block_id = sb.id
		JOIN file_chunk fc ON fc.chunk_id = r.chunk_id
		WHERE fc.logical_file_id = $1
		ORDER BY sb.id
	`, fileID)
	if err != nil {
		t.Fatalf("query block snapshot for file %d: %v", fileID, err)
	}
	defer func() { _ = rows.Close() }()

	out := make([]blockSnapshot, 0)
	for rows.Next() {
		var snap blockSnapshot
		if err := rows.Scan(
			&snap.ID,
			&snap.BlockHash,
			&snap.CompressedHash,
			&snap.PhysicalHash,
			&snap.ContainerID,
			&snap.ContainerFile,
			&snap.ContainerOffset,
			&snap.StoredSize,
			&snap.CompressionCodec,
			&snap.CompressionLevel,
			&snap.PlaintextSize,
			&snap.CompressedSize,
		); err != nil {
			t.Fatalf("scan block snapshot row for file %d: %v", fileID, err)
		}

		containerPath := filepath.Join(repo.ContainersDir, snap.ContainerFile)
		fh, err := os.Open(containerPath)
		if err != nil {
			t.Fatalf("open container file for block %d: %v", snap.ID, err)
		}
		payload := make([]byte, snap.StoredSize)
		if _, err := fh.ReadAt(payload, snap.ContainerOffset); err != nil {
			_ = fh.Close()
			t.Fatalf("read stored bytes for block %d: %v", snap.ID, err)
		}
		if err := fh.Close(); err != nil {
			t.Fatalf("close container file for block %d: %v", snap.ID, err)
		}
		snap.StoredBytes = payload
		out = append(out, snap)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate block snapshot rows for file %d: %v", fileID, err)
	}
	return out
}

func setRepositoryCompressionForTest(t *testing.T, dbconn *sql.DB, codec string, level int) {
	t.Helper()

	tx, err := dbconn.Begin()
	if err != nil {
		t.Fatalf("begin repository compression update: %v", err)
	}
	if err := SetDefaultCompression(tx, codec); err != nil {
		_ = tx.Rollback()
		t.Fatalf("set repository compression=%q: %v", codec, err)
	}
	if codec == storagecompression.CompressionZstd {
		if err := SetDefaultCompressionLevel(tx, level); err != nil {
			_ = tx.Rollback()
			t.Fatalf("set repository compression_level=%d: %v", level, err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit repository compression update: %v", err)
	}
}

func TestFeatureGatedCompressionStoreIfSmallerDeterministicCases(t *testing.T) {
	RequireTestCompression(t, "zstd")

	repo := NewTestRepository(t, WithCompression("zstd"), WithCompressionLevel(3))

	testCases := []struct {
		name          string
		payload       []byte
		expectedCodec string
	}{
		{
			name:          "compressible",
			payload:       bytes.Repeat([]byte("store-if-smaller-compressible-"), 8192),
			expectedCodec: storagecompression.CompressionZstd,
		},
		{
			name:          "random",
			payload:       deterministicNoise(64 * 1024),
			expectedCodec: storagecompression.CompressionNone,
		},
		{
			name:          "tiny",
			payload:       []byte("tiny"),
			expectedCodec: storagecompression.CompressionNone,
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			inPath := writeFeatureGateInputFile(t, tc.payload)
			result, err := StoreFileWithStorageContextAndCodecResult(repo.Storage, inPath, blocks.CodecPlain)
			if err != nil {
				t.Fatalf("store payload: %v", err)
			}

			meta := readStoredBlockMetaWithHashesForFile(t, repo.DB, result.FileID)
			if meta.Codec != tc.expectedCodec {
				t.Fatalf("expected codec=%q, got %q", tc.expectedCodec, meta.Codec)
			}
			if meta.PlaintextSize <= 0 || meta.CompressedSize <= 0 || meta.StoredSize <= 0 {
				t.Fatalf("expected positive sizes, got plaintext=%d compressed=%d stored=%d", meta.PlaintextSize, meta.CompressedSize, meta.StoredSize)
			}
			if len(meta.BlockHash) != 32 || len(meta.CompressedHash) != 32 || len(meta.PhysicalHash) != 32 {
				t.Fatalf("expected 32-byte hashes, got logical=%d compressed=%d physical=%d", len(meta.BlockHash), len(meta.CompressedHash), len(meta.PhysicalHash))
			}

			if meta.Codec == storagecompression.CompressionNone {
				if meta.CompressedSize != meta.PlaintextSize {
					t.Fatalf("codec=none invariant violated: compressed=%d plaintext=%d", meta.CompressedSize, meta.PlaintextSize)
				}
				if !bytes.Equal(meta.CompressedHash, meta.BlockHash) {
					t.Fatalf("codec=none hash invariant violated: compressed_hash != block_hash")
				}
				if meta.CompressionLvl.Valid {
					t.Fatalf("expected NULL compression_level for codec=none, got %d", meta.CompressionLvl.Int64)
				}
			} else {
				if meta.CompressedSize >= meta.PlaintextSize {
					t.Fatalf("codec=zstd invariant violated: compressed=%d plaintext=%d", meta.CompressedSize, meta.PlaintextSize)
				}
				if !meta.CompressionLvl.Valid || meta.CompressionLvl.Int64 != 3 {
					t.Fatalf("expected compression_level=3 for codec=zstd, got %+v", meta.CompressionLvl)
				}
			}

			if meta.StoredSize != meta.CompressedSize {
				t.Fatalf("plain transform invariant violated: stored_size=%d compressed_size=%d", meta.StoredSize, meta.CompressedSize)
			}
			if !bytes.Equal(meta.PhysicalHash, meta.CompressedHash) {
				t.Fatalf("plain transform invariant violated: physical_hash != compressed_hash")
			}

			outPath := filepath.Join(t.TempDir(), tc.name+"-restored.bin")
			if _, err := restoreFileWithDBAndDir(repo.DB, result.FileID, outPath, repo.ContainersDir, RestoreOptions{Overwrite: true}); err != nil {
				t.Fatalf("restore payload: %v", err)
			}

			restored, err := os.ReadFile(outPath)
			if err != nil {
				t.Fatalf("read restored payload: %v", err)
			}
			if !bytes.Equal(restored, tc.payload) {
				t.Fatalf("restored payload mismatch")
			}

			if err := verifypkg.VerifyFileStandardWithContainersDir(repo.DB, int(result.FileID), repo.ContainersDir); err != nil {
				t.Fatalf("verify file standard: %v", err)
			}
		})
	}
}

func TestFeatureGatedCompressionDuplicateStorePreservesDedupAndCompressionStep512(t *testing.T) {
	RequireTestCompression(t, "zstd")

	repo := NewTestRepository(t, WithCompression("zstd"), WithCompressionLevel(3))
	payload := bytes.Repeat([]byte("step512-duplicate-compressible-payload-"), 8192)

	pathA := writeFeatureGateInputFile(t, payload)
	resultA, err := StoreFileWithStorageContextAndCodecResult(repo.Storage, pathA, blocks.CodecPlain)
	if err != nil {
		t.Fatalf("store first duplicate payload: %v", err)
	}

	graphA := readFileChunkGraph(t, repo.DB, resultA.FileID)
	if len(graphA) == 0 {
		t.Fatal("expected non-empty chunk graph for first store")
	}
	blockIDsA := readDistinctBlockIDsForFile(t, repo.DB, resultA.FileID)
	if len(blockIDsA) == 0 {
		t.Fatal("expected non-empty block id set for first store")
	}

	metaA := readStoredBlockMetaWithHashesForFile(t, repo.DB, resultA.FileID)
	if metaA.Codec != storagecompression.CompressionZstd {
		t.Fatalf("expected first store codec=zstd, got %q", metaA.Codec)
	}
	if metaA.CompressedSize >= metaA.PlaintextSize {
		t.Fatalf("expected first store compression savings, got compressed=%d plaintext=%d", metaA.CompressedSize, metaA.PlaintextSize)
	}

	var chunkRowsBefore int
	if err := repo.DB.QueryRow(`SELECT COUNT(*) FROM chunk`).Scan(&chunkRowsBefore); err != nil {
		t.Fatalf("count chunk rows before duplicate store: %v", err)
	}
	var blockRowsBefore int
	if err := repo.DB.QueryRow(`SELECT COUNT(*) FROM storage_blocks`).Scan(&blockRowsBefore); err != nil {
		t.Fatalf("count storage_blocks rows before duplicate store: %v", err)
	}
	var refsRowsBefore int
	if err := repo.DB.QueryRow(`SELECT COUNT(*) FROM chunk_block_refs`).Scan(&refsRowsBefore); err != nil {
		t.Fatalf("count chunk_block_refs rows before duplicate store: %v", err)
	}

	pathB := writeFeatureGateInputFile(t, payload)
	resultB, err := StoreFileWithStorageContextAndCodecResult(repo.Storage, pathB, blocks.CodecPlain)
	if err != nil {
		t.Fatalf("store second duplicate payload: %v", err)
	}
	if !resultB.AlreadyStored {
		t.Fatalf("expected duplicate payload to resolve as already stored under compression")
	}

	graphB := readFileChunkGraph(t, repo.DB, resultB.FileID)
	if !reflect.DeepEqual(graphA, graphB) {
		t.Fatalf("expected duplicate store to preserve logical chunk graph; first=%v second=%v", graphA, graphB)
	}
	blockIDsB := readDistinctBlockIDsForFile(t, repo.DB, resultB.FileID)
	if !reflect.DeepEqual(blockIDsA, blockIDsB) {
		t.Fatalf("expected duplicate store to preserve block packing map; first=%v second=%v", blockIDsA, blockIDsB)
	}

	var chunkRowsAfter int
	if err := repo.DB.QueryRow(`SELECT COUNT(*) FROM chunk`).Scan(&chunkRowsAfter); err != nil {
		t.Fatalf("count chunk rows after duplicate store: %v", err)
	}
	if chunkRowsAfter != chunkRowsBefore {
		t.Fatalf("expected no duplicate chunk rows from compression-enabled duplicate store; before=%d after=%d", chunkRowsBefore, chunkRowsAfter)
	}
	var blockRowsAfter int
	if err := repo.DB.QueryRow(`SELECT COUNT(*) FROM storage_blocks`).Scan(&blockRowsAfter); err != nil {
		t.Fatalf("count storage_blocks rows after duplicate store: %v", err)
	}
	if blockRowsAfter != blockRowsBefore {
		t.Fatalf("expected no new storage_blocks rows from duplicate store; before=%d after=%d", blockRowsBefore, blockRowsAfter)
	}
	var refsRowsAfter int
	if err := repo.DB.QueryRow(`SELECT COUNT(*) FROM chunk_block_refs`).Scan(&refsRowsAfter); err != nil {
		t.Fatalf("count chunk_block_refs rows after duplicate store: %v", err)
	}
	if refsRowsAfter != refsRowsBefore {
		t.Fatalf("expected no new chunk_block_refs rows from duplicate store; before=%d after=%d", refsRowsBefore, refsRowsAfter)
	}

	for _, blockID := range blockIDsB {
		var codec string
		if err := repo.DB.QueryRow(`SELECT compression_codec FROM storage_blocks WHERE id = $1`, blockID).Scan(&codec); err != nil {
			t.Fatalf("read compression codec for deduped block %d: %v", blockID, err)
		}
		if codec != storagecompression.CompressionZstd {
			t.Fatalf("expected deduped/packed block %d to keep zstd codec, got %q", blockID, codec)
		}
	}

	for _, restore := range []struct {
		fileID  int64
		outName string
	}{
		{fileID: resultA.FileID, outName: "step512-restore-a.bin"},
		{fileID: resultB.FileID, outName: "step512-restore-b.bin"},
	} {
		outPath := filepath.Join(t.TempDir(), restore.outName)
		if _, err := restoreFileWithDBAndDir(repo.DB, restore.fileID, outPath, repo.ContainersDir, RestoreOptions{Overwrite: true}); err != nil {
			t.Fatalf("restore %s: %v", restore.outName, err)
		}
		got, err := os.ReadFile(outPath)
		if err != nil {
			t.Fatalf("read restored file %s: %v", restore.outName, err)
		}
		if !bytes.Equal(got, payload) {
			t.Fatalf("restored bytes mismatch for %s", restore.outName)
		}
		if err := verifypkg.VerifyFileStandardWithContainersDir(repo.DB, int(restore.fileID), repo.ContainersDir); err != nil {
			t.Fatalf("verify restored file %s: %v", restore.outName, err)
		}
	}
}

func TestFeatureGatedCompressionSwitchDoesNotMigrateExistingBlocksStep513(t *testing.T) {
	RequireTestCompression(t, "zstd")

	repo := NewTestRepository(t, WithCompression("none"))

	payloadOld := bytes.Repeat([]byte("step513-old-none-payload-"), 4096)
	pathOld := writeFeatureGateInputFile(t, payloadOld)
	resultOld, err := StoreFileWithStorageContextAndCodecResult(repo.Storage, pathOld, blocks.CodecPlain)
	if err != nil {
		t.Fatalf("store old file with compression=none: %v", err)
	}

	oldBefore := snapshotBlocksForFile(t, repo, resultOld.FileID)
	if len(oldBefore) == 0 {
		t.Fatal("expected old file to create at least one block")
	}
	for _, b := range oldBefore {
		if b.CompressionCodec != storagecompression.CompressionNone {
			t.Fatalf("expected old block %d codec=none before switch, got %q", b.ID, b.CompressionCodec)
		}
	}

	setRepositoryCompressionForTest(t, repo.DB, storagecompression.CompressionZstd, 3)

	payloadNew := bytes.Repeat([]byte("step513-new-zstd-payload-"), 8192)
	pathNew := writeFeatureGateInputFile(t, payloadNew)
	resultNew, err := StoreFileWithStorageContextAndCodecResult(repo.Storage, pathNew, blocks.CodecPlain)
	if err != nil {
		t.Fatalf("store new file with compression=zstd: %v", err)
	}

	oldAfter := snapshotBlocksForFile(t, repo, resultOld.FileID)
	if !reflect.DeepEqual(oldBefore, oldAfter) {
		t.Fatalf("expected old block rows and bytes to remain unchanged after compression switch; before=%v after=%v", oldBefore, oldAfter)
	}

	newBlocks := snapshotBlocksForFile(t, repo, resultNew.FileID)
	if len(newBlocks) == 0 {
		t.Fatal("expected new file to create at least one block")
	}
	var zstdBlocks int
	for _, b := range newBlocks {
		if b.CompressionCodec == storagecompression.CompressionZstd {
			zstdBlocks++
		}
	}
	if zstdBlocks == 0 {
		t.Fatal("expected at least one new block to use zstd compression after config switch")
	}

	for _, restore := range []struct {
		fileID  int64
		payload []byte
		name    string
	}{
		{fileID: resultOld.FileID, payload: payloadOld, name: "old"},
		{fileID: resultNew.FileID, payload: payloadNew, name: "new"},
	} {
		outPath := filepath.Join(t.TempDir(), "step513-"+restore.name+".restore")
		if _, err := restoreFileWithDBAndDir(repo.DB, restore.fileID, outPath, repo.ContainersDir, RestoreOptions{Overwrite: true}); err != nil {
			t.Fatalf("restore %s file: %v", restore.name, err)
		}
		got, err := os.ReadFile(outPath)
		if err != nil {
			t.Fatalf("read restored %s file: %v", restore.name, err)
		}
		if !bytes.Equal(got, restore.payload) {
			t.Fatalf("restored bytes mismatch for %s file", restore.name)
		}
		if err := verifypkg.VerifyFileStandardWithContainersDir(repo.DB, int(restore.fileID), repo.ContainersDir); err != nil {
			t.Fatalf("verify %s file: %v", restore.name, err)
		}
	}

	if err := verifypkg.VerifySystemStandardWithContainersDir(repo.DB, repo.ContainersDir); err != nil {
		t.Fatalf("verify repository after mixed compression files: %v", err)
	}
}
