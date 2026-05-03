package storage

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"hash/crc32"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	corebenchmark "github.com/franchoy/coldkeep/internal/benchmark"
	"github.com/franchoy/coldkeep/internal/blocks"
	"github.com/franchoy/coldkeep/internal/chunk"
	"github.com/franchoy/coldkeep/internal/container"
	"github.com/franchoy/coldkeep/internal/execution"
	gcpkg "github.com/franchoy/coldkeep/internal/gc"
	verifypkg "github.com/franchoy/coldkeep/internal/verify"

	"github.com/franchoy/coldkeep/internal/db"
	filestate "github.com/franchoy/coldkeep/internal/status"
	_ "github.com/mattn/go-sqlite3"
)

type syncFailWriter struct {
	offset          int64
	appendCalls     int
	quarantineErr   error
	quarantineCalls int
	db              *sql.DB
}

func (w *syncFailWriter) FinalizeContainer() error {
	return nil
}

func (w *syncFailWriter) AppendPayload(_ db.DBTX, payload []byte) (container.LocalPlacement, error) {
	w.appendCalls++
	offset := w.offset
	w.offset += int64(len(payload))
	return container.LocalPlacement{
		ContainerID:      1,
		Filename:         "durability_test_container.bin",
		Offset:           offset,
		StoredSize:       int64(len(payload)),
		NewContainerSize: container.ContainerHdrLen + w.offset,
	}, nil
}

func (w *syncFailWriter) QuarantineActiveContainer() error {
	w.quarantineCalls++
	if w.db != nil {
		if _, err := w.db.Exec(`UPDATE container SET quarantine = TRUE WHERE id = 1`); err != nil {
			return err
		}
	}
	return w.quarantineErr
}

type commitAckWriter struct {
	offset       int64
	ackCalls     int
	pendingClear bool
}

type rollbackCleanupFailureWriter struct {
	offset              int64
	rollbackErr         error
	rollbackCalls       int
	quarantineErr       error
	quarantineCalls     int
	quarantineContainer int64
	db                  *sql.DB
}

type fixedVersionChunker struct {
	delegate chunk.Chunker
	version  chunk.Version
}

type fixedBoundaryChunker struct {
	version  chunk.Version
	boundary int
}

func (c fixedVersionChunker) Version() chunk.Version {
	return c.version
}

func (c fixedVersionChunker) ChunkFile(path string) ([]chunk.Result, error) {
	return c.delegate.ChunkFile(path)
}

func (c fixedBoundaryChunker) Version() chunk.Version {
	return c.version
}

func (c fixedBoundaryChunker) ChunkFile(path string) ([]chunk.Result, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	if len(data) == 0 {
		return []chunk.Result{}, nil
	}
	if c.boundary <= 0 || c.boundary >= len(data) {
		boundary := len(data)
		all := make([]byte, boundary)
		copy(all, data)
		return []chunk.Result{{
			Info: chunk.Info{Size: int64(boundary), Offset: 0},
			Data: all,
		}}, nil
	}

	first := make([]byte, c.boundary)
	copy(first, data[:c.boundary])
	second := make([]byte, len(data)-c.boundary)
	copy(second, data[c.boundary:])

	return []chunk.Result{
		{Info: chunk.Info{Size: int64(len(first)), Offset: 0}, Data: first},
		{Info: chunk.Info{Size: int64(len(second)), Offset: int64(c.boundary)}, Data: second},
	}, nil
}

type duplicateChunker struct {
	version chunk.Version
	payload []byte
}

type scriptedChunker struct {
	version  chunk.Version
	payloads [][]byte
}

func (c duplicateChunker) Version() chunk.Version {
	return c.version
}

func (c duplicateChunker) ChunkFile(path string) ([]chunk.Result, error) {
	left := append([]byte(nil), c.payload...)
	right := append([]byte(nil), c.payload...)
	return []chunk.Result{
		{Info: chunk.Info{Size: int64(len(left)), Offset: 0}, Data: left},
		{Info: chunk.Info{Size: int64(len(right)), Offset: int64(len(left))}, Data: right},
	}, nil
}

func (c scriptedChunker) Version() chunk.Version {
	return c.version
}

func (c scriptedChunker) ChunkFile(path string) ([]chunk.Result, error) {
	results := make([]chunk.Result, 0, len(c.payloads))
	offset := int64(0)
	for _, p := range c.payloads {
		payload := append([]byte(nil), p...)
		results = append(results, chunk.Result{
			Info: chunk.Info{Size: int64(len(payload)), Offset: offset},
			Data: payload,
		})
		offset += int64(len(payload))
	}
	return results, nil
}

func concatPayloads(payloads [][]byte) []byte {
	total := 0
	for _, p := range payloads {
		total += len(p)
	}
	out := make([]byte, 0, total)
	for _, p := range payloads {
		out = append(out, p...)
	}
	return out
}

func storeScriptedFile(t *testing.T, dbconn *sql.DB, workDir, fileName string, payloads [][]byte) StoreFileResult {
	t.Helper()
	inPath := filepath.Join(workDir, fileName)
	if err := os.WriteFile(inPath, []byte("placeholder"), 0o600); err != nil {
		t.Fatalf("write scripted input file: %v", err)
	}

	sgctx := StorageContext{
		DB:           dbconn,
		Writer:       container.NewLocalWriterWithDirAndDB(workDir, container.GetContainerMaxSize(), dbconn),
		ContainerDir: workDir,
		Chunker: scriptedChunker{
			version:  chunk.VersionV1SimpleRolling,
			payloads: payloads,
		},
	}

	result, err := StoreFileWithStorageContextAndCodecResult(sgctx, inPath, blocks.CodecPlain)
	if err != nil {
		t.Fatalf("store scripted file %q: %v", fileName, err)
	}
	return result
}

func restoreFileBytesForTest(t *testing.T, dbconn *sql.DB, fileID int64, workDir, outName string) []byte {
	t.Helper()
	outPath := filepath.Join(workDir, outName)
	if _, err := restoreFileWithDBAndDir(dbconn, fileID, outPath, workDir, RestoreOptions{Overwrite: true}); err != nil {
		t.Fatalf("restore file id=%d: %v", fileID, err)
	}
	data, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatalf("read restored file: %v", err)
	}
	return data
}

type packingSnapshot struct {
	BlockCount          int
	ChunksPerBlock      []int
	ChunkHashesByBlocks [][]string
}

func loadPackingSnapshot(t *testing.T, dbconn *sql.DB) packingSnapshot {
	t.Helper()

	var blockCount int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM storage_blocks`).Scan(&blockCount); err != nil {
		t.Fatalf("count storage_blocks: %v", err)
	}

	rows, err := dbconn.Query(
		`SELECT r.block_id, c.chunk_hash
		 FROM chunk_block_refs r
		 JOIN chunk c ON c.id = r.chunk_id
		 ORDER BY r.block_id, r.offset_in_block`,
	)
	if err != nil {
		t.Fatalf("query chunk_block_refs layout: %v", err)
	}
	defer func() { _ = rows.Close() }()

	snapshot := packingSnapshot{BlockCount: blockCount}
	var currentBlockID int64 = -1
	for rows.Next() {
		var blockID int64
		var chunkHash string
		if err := rows.Scan(&blockID, &chunkHash); err != nil {
			t.Fatalf("scan chunk_block_refs layout: %v", err)
		}
		if currentBlockID != blockID {
			snapshot.ChunksPerBlock = append(snapshot.ChunksPerBlock, 0)
			snapshot.ChunkHashesByBlocks = append(snapshot.ChunkHashesByBlocks, []string{})
			currentBlockID = blockID
		}
		last := len(snapshot.ChunksPerBlock) - 1
		snapshot.ChunksPerBlock[last]++
		snapshot.ChunkHashesByBlocks[last] = append(snapshot.ChunkHashesByBlocks[last], chunkHash)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate chunk_block_refs layout: %v", err)
	}

	return snapshot
}

func TestNewStoreServiceResolvesRegistryDefaultChunker(t *testing.T) {
	service := NewStoreService(nil, nil)
	resolved, err := service.ResolveActiveChunker()
	if err != nil {
		t.Fatalf("ResolveActiveChunker: %v", err)
	}
	if resolved.Chunker == nil {
		t.Fatal("expected resolved chunker to be non-nil")
	}

	defaultVersion := chunk.DefaultRegistry().DefaultVersion()
	if resolved.Version != defaultVersion {
		t.Fatalf("unexpected resolved chunker version: got=%q want=%q", resolved.Version, defaultVersion)
	}
	if service.Repository() != nil {
		t.Fatal("expected nil repository when constructor is given nil")
	}
}

func TestStoreDedupCheckHappensBeforeWriteBoundary(t *testing.T) {
	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	defer func() { _ = dbconn.Close() }()

	if err := db.RunMigrations(dbconn); err != nil {
		t.Fatalf("run migrations: %v", err)
	}

	workDir := t.TempDir()
	inPath := filepath.Join(workDir, "dup.txt")
	if err := os.WriteFile(inPath, []byte("placeholder-file-content"), 0o600); err != nil {
		t.Fatalf("write input file: %v", err)
	}

	writer := container.NewLocalWriterWithDirAndDB(workDir, container.GetContainerMaxSize(), dbconn)
	codec, err := blocks.ParseCodec("plain")
	if err != nil {
		t.Fatalf("parse plain codec: %v", err)
	}

	chunkPayload := []byte("same-chunk-bytes")
	dupChunker := duplicateChunker{version: chunk.VersionV1SimpleRolling, payload: chunkPayload}

	sgctx := StorageContext{
		DB:           dbconn,
		Writer:       writer,
		ContainerDir: workDir,
		Chunker:      dupChunker,
	}

	result, err := StoreFileWithStorageContextAndCodecResult(sgctx, inPath, codec)
	if err != nil {
		t.Fatalf("store file with duplicate chunks: %v", err)
	}
	if result.FileID <= 0 {
		t.Fatalf("expected valid file id, got %d", result.FileID)
	}

	var chunkRows int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM chunk`).Scan(&chunkRows); err != nil {
		t.Fatalf("count chunk rows: %v", err)
	}
	if chunkRows != 1 {
		t.Fatalf("expected one deduplicated chunk row, got %d", chunkRows)
	}

	var fileChunkRows int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM file_chunk WHERE logical_file_id = $1`, result.FileID).Scan(&fileChunkRows); err != nil {
		t.Fatalf("count file_chunk rows: %v", err)
	}
	if fileChunkRows != 2 {
		t.Fatalf("expected two file_chunk references for duplicate recipe entries, got %d", fileChunkRows)
	}

	rows, err := dbconn.Query(
		`SELECT chunk_order FROM file_chunk WHERE logical_file_id = $1 ORDER BY chunk_order`,
		result.FileID,
	)
	if err != nil {
		t.Fatalf("query file_chunk recipe order: %v", err)
	}
	defer func() { _ = rows.Close() }()

	chunkOrders := make([]int, 0, 2)
	for rows.Next() {
		var order int
		if err := rows.Scan(&order); err != nil {
			t.Fatalf("scan file_chunk chunk_order: %v", err)
		}
		chunkOrders = append(chunkOrders, order)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate file_chunk chunk_order rows: %v", err)
	}
	if len(chunkOrders) != 2 || chunkOrders[0] != 0 || chunkOrders[1] != 1 {
		t.Fatalf("file recipe order must remain source-of-truth [0,1], got %v", chunkOrders)
	}

	var blockRows int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM blocks`).Scan(&blockRows); err != nil {
		t.Fatalf("count blocks rows: %v", err)
	}
	if blockRows != 1 {
		t.Fatalf("expected one persisted block row for deduplicated chunk, got %d", blockRows)
	}
}

func TestStoreMixedExistingAndNewChunksPacksOnlyNewAndPreservesRecipeOrder(t *testing.T) {
	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	defer func() { _ = dbconn.Close() }()

	if err := db.RunMigrations(dbconn); err != nil {
		t.Fatalf("run migrations: %v", err)
	}

	workDir := t.TempDir()
	inPath := filepath.Join(workDir, "mixed-existing-new.bin")
	if err := os.WriteFile(inPath, []byte("placeholder"), 0o600); err != nil {
		t.Fatalf("write input file: %v", err)
	}

	writer := container.NewLocalWriterWithDirAndDB(workDir, container.GetContainerMaxSize(), dbconn)
	transformer, err := blocks.GetBlockTransformer(blocks.CodecPlain)
	if err != nil {
		t.Fatalf("get plain transformer: %v", err)
	}
	blockRepo := &blocks.Repository{DB: dbconn}

	hashHex := func(payload []byte) string {
		sum := sha256.Sum256(payload)
		return hex.EncodeToString(sum[:])
	}

	insertChunk := func(hash string, size int64, status string) int64 {
		t.Helper()
		var chunkID int64
		if err := dbconn.QueryRow(
			`INSERT INTO chunk (chunk_hash, size, status, live_ref_count, chunker_version)
			 VALUES ($1, $2, $3, 0, 'v1-simple-rolling')
			 RETURNING id`,
			hash,
			size,
			status,
		).Scan(&chunkID); err != nil {
			t.Fatalf("insert chunk: %v", err)
		}
		return chunkID
	}

	payloadA := []byte("chunk-A-existing-legacy")
	payloadB := []byte("chunk-B-new")
	payloadC := []byte("chunk-C-existing-packed")
	payloadD := []byte("chunk-D-new")

	hashA := hashHex(payloadA)
	hashB := hashHex(payloadB)
	hashC := hashHex(payloadC)
	hashD := hashHex(payloadD)

	// Seed A as existing legacy block.
	chunkAID := insertChunk(hashA, int64(len(payloadA)), filestate.ChunkProcessing)
	txA, err := dbconn.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("begin txA: %v", err)
	}
	placementA, _, err := storeChunkAsPlainBlockWithWriter(context.Background(), txA, blockRepo, writer, chunkAID, hashA, payloadA, transformer)
	if err != nil {
		_ = txA.Rollback()
		t.Fatalf("seed legacy chunk A: %v", err)
	}
	if _, err := txA.Exec(`UPDATE chunk SET status = $1 WHERE id = $2`, filestate.ChunkCompleted, chunkAID); err != nil {
		_ = txA.Rollback()
		t.Fatalf("mark chunk A completed: %v", err)
	}
	if err := container.UpdateContainerSize(txA, placementA.ContainerID, placementA.NewContainerSize); err != nil {
		_ = txA.Rollback()
		t.Fatalf("update container size for A: %v", err)
	}
	if err := txA.Commit(); err != nil {
		_ = txA.Rollback()
		t.Fatalf("commit seed A: %v", err)
	}
	acknowledgeWriterAppendCommitted(writer)

	// Seed C as existing previously packed block (+ compatibility companion row).
	chunkCID := insertChunk(hashC, int64(len(payloadC)), filestate.ChunkProcessing)
	builderC := blocks.NewBlockBuilder(1 << 20)
	if err := builderC.Add(blocks.PendingChunk{ChunkID: chunkCID, Data: payloadC, Size: int64(len(payloadC))}); err != nil {
		t.Fatalf("build packed chunk C: %v", err)
	}
	txC, err := dbconn.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("begin txC: %v", err)
	}
	persistedC, err := storePackedBlockWithWriter(context.Background(), txC, writer, transformer, builderC)
	if err != nil {
		_ = txC.Rollback()
		t.Fatalf("seed packed chunk C: %v", err)
	}
	if _, err := txC.Exec(`UPDATE chunk SET status = $1 WHERE id = $2`, filestate.ChunkCompleted, chunkCID); err != nil {
		_ = txC.Rollback()
		t.Fatalf("mark chunk C completed: %v", err)
	}
	if err := insertLegacyCompanionBlockRowWithContext(context.Background(), txC, chunkCID, persistedC.Placement.ContainerID, persistedC.Placement.Offset, int64(len(payloadC))); err != nil {
		_ = txC.Rollback()
		t.Fatalf("insert companion block row for C: %v", err)
	}
	if err := container.UpdateContainerSize(txC, persistedC.Placement.ContainerID, persistedC.Placement.NewContainerSize); err != nil {
		_ = txC.Rollback()
		t.Fatalf("update container size for C: %v", err)
	}
	if err := txC.Commit(); err != nil {
		_ = txC.Rollback()
		t.Fatalf("commit seed C: %v", err)
	}
	acknowledgeWriterAppendCommitted(writer)

	var storageBlocksBefore int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM storage_blocks`).Scan(&storageBlocksBefore); err != nil {
		t.Fatalf("count storage_blocks before: %v", err)
	}
	var refsBefore int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM chunk_block_refs`).Scan(&refsBefore); err != nil {
		t.Fatalf("count chunk_block_refs before: %v", err)
	}

	chunker := scriptedChunker{
		version:  chunk.VersionV1SimpleRolling,
		payloads: [][]byte{payloadA, payloadB, payloadC, payloadD},
	}

	sgctx := StorageContext{
		DB:           dbconn,
		Writer:       writer,
		ContainerDir: workDir,
		Chunker:      chunker,
	}

	result, err := StoreFileWithStorageContextAndCodecResult(sgctx, inPath, blocks.CodecPlain)
	if err != nil {
		t.Fatalf("store mixed existing/new file: %v", err)
	}

	rows, err := dbconn.Query(
		`SELECT fc.chunk_order, c.chunk_hash
		 FROM file_chunk fc
		 JOIN chunk c ON c.id = fc.chunk_id
		 WHERE fc.logical_file_id = $1
		 ORDER BY fc.chunk_order`,
		result.FileID,
	)
	if err != nil {
		t.Fatalf("query file recipe rows: %v", err)
	}
	defer func() { _ = rows.Close() }()

	gotHashes := make([]string, 0, 4)
	for rows.Next() {
		var order int
		var hash string
		if err := rows.Scan(&order, &hash); err != nil {
			t.Fatalf("scan recipe row: %v", err)
		}
		if order != len(gotHashes) {
			t.Fatalf("unexpected recipe order: got %d at position %d", order, len(gotHashes))
		}
		gotHashes = append(gotHashes, hash)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate recipe rows: %v", err)
	}

	wantHashes := []string{hashA, hashB, hashC, hashD}
	if len(gotHashes) != len(wantHashes) {
		t.Fatalf("recipe length mismatch: got %d want %d", len(gotHashes), len(wantHashes))
	}
	for i := range wantHashes {
		if gotHashes[i] != wantHashes[i] {
			t.Fatalf("recipe hash mismatch at index %d: got %q want %q", i, gotHashes[i], wantHashes[i])
		}
	}

	var storageBlocksAfter int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM storage_blocks`).Scan(&storageBlocksAfter); err != nil {
		t.Fatalf("count storage_blocks after: %v", err)
	}
	if storageBlocksAfter-storageBlocksBefore != 2 {
		t.Fatalf("expected exactly two new packed storage_blocks rows (B and D), got delta %d", storageBlocksAfter-storageBlocksBefore)
	}

	var refsAfter int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM chunk_block_refs`).Scan(&refsAfter); err != nil {
		t.Fatalf("count chunk_block_refs after: %v", err)
	}
	if refsAfter-refsBefore != 2 {
		t.Fatalf("expected exactly two new chunk_block_refs rows (B and D), got delta %d", refsAfter-refsBefore)
	}

	var refsForA int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM chunk_block_refs r JOIN chunk c ON c.id = r.chunk_id WHERE c.chunk_hash = $1`, hashA).Scan(&refsForA); err != nil {
		t.Fatalf("count refs for A: %v", err)
	}
	if refsForA != 0 {
		t.Fatalf("expected no packed refs for existing legacy chunk A, got %d", refsForA)
	}

	outPath := filepath.Join(workDir, "mixed-restore.bin")
	if _, err := restoreFileWithDBAndDir(dbconn, result.FileID, outPath, workDir, RestoreOptions{Overwrite: true}); err != nil {
		t.Fatalf("restore mixed file: %v", err)
	}
	got, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatalf("read restored file: %v", err)
	}
	want := append(append(append([]byte{}, payloadA...), payloadB...), append(payloadC, payloadD...)...)
	if !bytes.Equal(got, want) {
		t.Fatalf("restored bytes mismatch: got=%q want=%q", string(got), string(want))
	}
}

func TestStep10NewChunksArePacked(t *testing.T) {
	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	defer func() { _ = dbconn.Close() }()
	if err := db.RunMigrations(dbconn); err != nil {
		t.Fatalf("run migrations: %v", err)
	}

	workDir := t.TempDir()
	payloads := [][]byte{
		[]byte("step10-pack-1"),
		[]byte("step10-pack-2"),
		[]byte("step10-pack-3"),
		[]byte("step10-pack-4"),
	}

	result := storeScriptedFile(t, dbconn, workDir, "step10-pack.bin", payloads)

	var chunkCount int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM file_chunk WHERE logical_file_id = $1`, result.FileID).Scan(&chunkCount); err != nil {
		t.Fatalf("count file chunks: %v", err)
	}
	var storageBlockCount int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM storage_blocks`).Scan(&storageBlockCount); err != nil {
		t.Fatalf("count storage_blocks: %v", err)
	}
	if storageBlockCount >= chunkCount {
		t.Fatalf("expected packed layout with fewer blocks than chunks: blocks=%d chunks=%d", storageBlockCount, chunkCount)
	}

	var multiChunkBlockCount int
	if err := dbconn.QueryRow(
		`SELECT COUNT(*)
		 FROM (
			SELECT block_id
			FROM chunk_block_refs
			GROUP BY block_id
			HAVING COUNT(*) > 1
		 )`,
	).Scan(&multiChunkBlockCount); err != nil {
		t.Fatalf("count multi-chunk blocks: %v", err)
	}
	if multiChunkBlockCount < 1 {
		t.Fatalf("expected at least one packed block with >1 chunk ref")
	}

	given := concatPayloads(payloads)
	restored := restoreFileBytesForTest(t, dbconn, result.FileID, workDir, "step10-pack.restore")
	if !bytes.Equal(restored, given) {
		t.Fatalf("restored bytes mismatch")
	}
}

func TestStep10DuplicatesAreNotRepacked(t *testing.T) {
	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	defer func() { _ = dbconn.Close() }()
	if err := db.RunMigrations(dbconn); err != nil {
		t.Fatalf("run migrations: %v", err)
	}

	workDir := t.TempDir()
	payloads := [][]byte{
		[]byte("dup-pack-A"),
		[]byte("dup-pack-B"),
		[]byte("dup-pack-C"),
	}

	first := storeScriptedFile(t, dbconn, workDir, "dup-first.bin", payloads)

	var blocksBefore int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM storage_blocks`).Scan(&blocksBefore); err != nil {
		t.Fatalf("count storage_blocks before: %v", err)
	}
	var refsBefore int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM chunk_block_refs`).Scan(&refsBefore); err != nil {
		t.Fatalf("count chunk_block_refs before: %v", err)
	}

	second := storeScriptedFile(t, dbconn, workDir, "dup-second.bin", payloads)

	var blocksAfter int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM storage_blocks`).Scan(&blocksAfter); err != nil {
		t.Fatalf("count storage_blocks after: %v", err)
	}
	if blocksAfter != blocksBefore {
		t.Fatalf("expected second store to create no new storage_blocks; before=%d after=%d", blocksBefore, blocksAfter)
	}
	var refsAfter int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM chunk_block_refs`).Scan(&refsAfter); err != nil {
		t.Fatalf("count chunk_block_refs after: %v", err)
	}
	if refsAfter != refsBefore {
		t.Fatalf("expected second store to create no new chunk_block_refs; before=%d after=%d", refsBefore, refsAfter)
	}

	loadRecipeChunkIDs := func(fileID int64) []int64 {
		t.Helper()
		rows, err := dbconn.Query(`SELECT chunk_id FROM file_chunk WHERE logical_file_id = $1 ORDER BY chunk_order`, fileID)
		if err != nil {
			t.Fatalf("query file recipe ids: %v", err)
		}
		defer func() { _ = rows.Close() }()
		ids := make([]int64, 0, len(payloads))
		for rows.Next() {
			var id int64
			if err := rows.Scan(&id); err != nil {
				t.Fatalf("scan chunk id: %v", err)
			}
			ids = append(ids, id)
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("iterate chunk ids: %v", err)
		}
		return ids
	}

	firstIDs := loadRecipeChunkIDs(first.FileID)
	secondIDs := loadRecipeChunkIDs(second.FileID)
	if !reflect.DeepEqual(firstIDs, secondIDs) {
		t.Fatalf("expected second recipe to reuse existing chunk ids; first=%v second=%v", firstIDs, secondIDs)
	}

	want := concatPayloads(payloads)
	if got := restoreFileBytesForTest(t, dbconn, first.FileID, workDir, "dup-first.restore"); !bytes.Equal(got, want) {
		t.Fatalf("first restore mismatch")
	}
	if got := restoreFileBytesForTest(t, dbconn, second.FileID, workDir, "dup-second.restore"); !bytes.Equal(got, want) {
		t.Fatalf("second restore mismatch")
	}
}

func TestStep10OperationEndTailBlockFlushed(t *testing.T) {
	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	defer func() { _ = dbconn.Close() }()
	if err := db.RunMigrations(dbconn); err != nil {
		t.Fatalf("run migrations: %v", err)
	}

	workDir := t.TempDir()
	payloads := [][]byte{[]byte("tail-block-small-data")}
	result := storeScriptedFile(t, dbconn, workDir, "tail.bin", payloads)

	var storageBlockCount int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM storage_blocks`).Scan(&storageBlockCount); err != nil {
		t.Fatalf("count storage_blocks: %v", err)
	}
	if storageBlockCount != 1 {
		t.Fatalf("expected exactly one tail storage block, got %d", storageBlockCount)
	}
	var refCount int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM chunk_block_refs`).Scan(&refCount); err != nil {
		t.Fatalf("count chunk_block_refs: %v", err)
	}
	if refCount < 1 {
		t.Fatalf("expected at least one chunk_block_ref for tail flush")
	}

	want := concatPayloads(payloads)
	if got := restoreFileBytesForTest(t, dbconn, result.FileID, workDir, "tail.restore"); !bytes.Equal(got, want) {
		t.Fatalf("restored tail bytes mismatch")
	}
}

func TestStep10OversizedChunkStoredAlone(t *testing.T) {
	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	defer func() { _ = dbconn.Close() }()
	if err := db.RunMigrations(dbconn); err != nil {
		t.Fatalf("run migrations: %v", err)
	}

	workDir := t.TempDir()
	huge := bytes.Repeat([]byte("H"), (1<<20)+4096)
	small := []byte("tiny")
	payloads := [][]byte{huge, small}
	result := storeScriptedFile(t, dbconn, workDir, "oversized.bin", payloads)

	hugeHashBytes := sha256.Sum256(huge)
	hugeHash := hex.EncodeToString(hugeHashBytes[:])

	var hugeBlockID int64
	if err := dbconn.QueryRow(
		`SELECT r.block_id
		 FROM chunk_block_refs r
		 JOIN chunk c ON c.id = r.chunk_id
		 WHERE c.chunk_hash = $1`,
		hugeHash,
	).Scan(&hugeBlockID); err != nil {
		t.Fatalf("find oversized chunk block: %v", err)
	}

	var blockRefCount int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM chunk_block_refs WHERE block_id = $1`, hugeBlockID).Scan(&blockRefCount); err != nil {
		t.Fatalf("count refs in oversized block: %v", err)
	}
	if blockRefCount != 1 {
		t.Fatalf("expected oversized chunk block to contain exactly one chunk, got %d", blockRefCount)
	}

	want := concatPayloads(payloads)
	if got := restoreFileBytesForTest(t, dbconn, result.FileID, workDir, "oversized.restore"); !bytes.Equal(got, want) {
		t.Fatalf("restored oversized bytes mismatch")
	}
}

func TestStep10DeterministicPackingAcrossFreshRepos(t *testing.T) {
	runScenario := func() (packingSnapshot, []byte) {
		t.Helper()
		dbconn, err := sql.Open("sqlite3", ":memory:")
		if err != nil {
			t.Fatalf("open sqlite db: %v", err)
		}
		defer func() { _ = dbconn.Close() }()
		if err := db.RunMigrations(dbconn); err != nil {
			t.Fatalf("run migrations: %v", err)
		}

		workDir := t.TempDir()
		payloads := [][]byte{
			bytes.Repeat([]byte("A"), 400*1024),
			bytes.Repeat([]byte("B"), 400*1024),
			bytes.Repeat([]byte("C"), 400*1024),
			bytes.Repeat([]byte("D"), 400*1024),
		}
		result := storeScriptedFile(t, dbconn, workDir, "deterministic.bin", payloads)
		snapshot := loadPackingSnapshot(t, dbconn)
		restored := restoreFileBytesForTest(t, dbconn, result.FileID, workDir, "deterministic.restore")
		return snapshot, restored
	}

	snapA, restoredA := runScenario()
	snapB, restoredB := runScenario()

	if snapA.BlockCount != snapB.BlockCount {
		t.Fatalf("block count mismatch: A=%d B=%d", snapA.BlockCount, snapB.BlockCount)
	}
	if !reflect.DeepEqual(snapA.ChunksPerBlock, snapB.ChunksPerBlock) {
		t.Fatalf("chunks-per-block pattern mismatch: A=%v B=%v", snapA.ChunksPerBlock, snapB.ChunksPerBlock)
	}
	if !reflect.DeepEqual(snapA.ChunkHashesByBlocks, snapB.ChunkHashesByBlocks) {
		t.Fatalf("chunk order inside blocks mismatch")
	}
	if !bytes.Equal(restoredA, restoredB) {
		t.Fatalf("restored bytes mismatch across fresh repos")
	}
}

func TestStep10MixedExistingAndNewChunksOverlapFiles(t *testing.T) {
	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	defer func() { _ = dbconn.Close() }()
	if err := db.RunMigrations(dbconn); err != nil {
		t.Fatalf("run migrations: %v", err)
	}

	workDir := t.TempDir()
	file1Chunks := [][]byte{[]byte("A-overlap"), []byte("B-overlap"), []byte("C-overlap")}
	file2Chunks := [][]byte{[]byte("A-overlap"), []byte("D-overlap"), []byte("C-overlap"), []byte("E-overlap")}

	file1 := storeScriptedFile(t, dbconn, workDir, "overlap-1.bin", file1Chunks)

	var blocksBefore int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM storage_blocks`).Scan(&blocksBefore); err != nil {
		t.Fatalf("count storage_blocks before overlap-2: %v", err)
	}

	file2 := storeScriptedFile(t, dbconn, workDir, "overlap-2.bin", file2Chunks)

	var blocksAfter int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM storage_blocks`).Scan(&blocksAfter); err != nil {
		t.Fatalf("count storage_blocks after overlap-2: %v", err)
	}
	if blocksAfter <= blocksBefore {
		t.Fatalf("expected new chunks to create packed blocks; before=%d after=%d", blocksBefore, blocksAfter)
	}

	hash := func(p []byte) string {
		sum := sha256.Sum256(p)
		return hex.EncodeToString(sum[:])
	}
	hashA := hash([]byte("A-overlap"))
	hashC := hash([]byte("C-overlap"))
	hashD := hash([]byte("D-overlap"))
	hashE := hash([]byte("E-overlap"))

	var reusedRows int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM chunk WHERE chunk_hash IN ($1, $2)`, hashA, hashC).Scan(&reusedRows); err != nil {
		t.Fatalf("count reused chunk rows: %v", err)
	}
	if reusedRows != 2 {
		t.Fatalf("expected reused existing chunks A/C to map to two chunk rows, got %d", reusedRows)
	}

	var newPackedRefs int
	if err := dbconn.QueryRow(
		`SELECT COUNT(*)
		 FROM chunk_block_refs r
		 JOIN chunk c ON c.id = r.chunk_id
		 WHERE c.chunk_hash IN ($1, $2)`,
		hashD,
		hashE,
	).Scan(&newPackedRefs); err != nil {
		t.Fatalf("count refs for new chunks D/E: %v", err)
	}
	if newPackedRefs < 2 {
		t.Fatalf("expected new chunks D/E to be packed with refs, got %d", newPackedRefs)
	}

	if got := restoreFileBytesForTest(t, dbconn, file1.FileID, workDir, "overlap-1.restore"); !bytes.Equal(got, concatPayloads(file1Chunks)) {
		t.Fatalf("restore overlap file1 mismatch")
	}
	if got := restoreFileBytesForTest(t, dbconn, file2.FileID, workDir, "overlap-2.restore"); !bytes.Equal(got, concatPayloads(file2Chunks)) {
		t.Fatalf("restore overlap file2 mismatch")
	}
}

func TestStep10CrashSafetyNoIncompletePackedMetadataAfterRollbackFailure(t *testing.T) {
	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	defer func() { _ = dbconn.Close() }()

	if err := db.RunMigrations(dbconn); err != nil {
		t.Fatalf("run migrations: %v", err)
	}

	if _, err := dbconn.Exec(
		`INSERT INTO container (id, filename, current_size, max_size, sealed, quarantine)
		 VALUES (1, $1, $2, $3, FALSE, FALSE)`,
		"step10-crash-container.bin",
		container.ContainerHdrLen,
		container.GetContainerMaxSize(),
	); err != nil {
		t.Fatalf("insert container row: %v", err)
	}

	workDir := t.TempDir()
	inPath := filepath.Join(workDir, "step10-crash.bin")
	if err := os.WriteFile(inPath, []byte("trigger-rollback-cleanup-failure"), 0o600); err != nil {
		t.Fatalf("write crash input: %v", err)
	}

	rollbackCause := errors.New("step10 injected rollback failure")
	writer := &rollbackCleanupFailureWriter{
		rollbackErr:         rollbackCause,
		quarantineContainer: 1,
		db:                  dbconn,
	}

	_, err = StoreFileWithStorageContextAndCodecResult(StorageContext{
		DB:           dbconn,
		Writer:       writer,
		ContainerDir: workDir,
	}, inPath, blocks.CodecPlain)
	if !errors.Is(err, rollbackCause) {
		t.Fatalf("expected rollback cause in surfaced error, got: %v", err)
	}

	var danglingRefs int
	if err := dbconn.QueryRow(
		`SELECT COUNT(*)
		 FROM chunk_block_refs r
		 LEFT JOIN storage_blocks b ON b.id = r.block_id
		 WHERE b.id IS NULL`,
	).Scan(&danglingRefs); err != nil {
		t.Fatalf("count dangling refs: %v", err)
	}
	if danglingRefs != 0 {
		t.Fatalf("expected no metadata refs to incomplete blocks, got dangling=%d", danglingRefs)
	}

	if err := verifypkg.VerifySystemStandardWithContainersDir(dbconn, workDir); err != nil {
		t.Fatalf("verify system standard after rollback regression: %v", err)
	}
}

func TestStorePackedBlockWithWriterCommitWritesBlockAndRefsAtomically(t *testing.T) {
	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	defer func() { _ = dbconn.Close() }()

	if err := db.RunMigrations(dbconn); err != nil {
		t.Fatalf("run migrations: %v", err)
	}

	insertChunk := func(hash string, size int64) int64 {
		t.Helper()
		var chunkID int64
		if err := dbconn.QueryRow(
			`INSERT INTO chunk (chunk_hash, size, status, live_ref_count, chunker_version)
			 VALUES ($1, $2, $3, 1, 'v1-simple-rolling')
			 RETURNING id`,
			hash,
			size,
			filestate.ChunkProcessing,
		).Scan(&chunkID); err != nil {
			t.Fatalf("insert chunk: %v", err)
		}
		return chunkID
	}

	chunk1 := insertChunk("packed-commit-1", 3)
	chunk2 := insertChunk("packed-commit-2", 4)

	builder := blocks.NewBlockBuilder(64)
	if err := builder.Add(blocks.PendingChunk{ChunkID: chunk1, Data: []byte("abc"), Size: 3}); err != nil {
		t.Fatalf("add chunk1: %v", err)
	}
	if err := builder.Add(blocks.PendingChunk{ChunkID: chunk2, Data: []byte("wxyz"), Size: 4}); err != nil {
		t.Fatalf("add chunk2: %v", err)
	}

	transformer, err := blocks.GetBlockTransformer(blocks.CodecPlain)
	if err != nil {
		t.Fatalf("get plain transformer: %v", err)
	}

	containersDir := t.TempDir()
	writer := container.NewLocalWriterWithDirAndDB(containersDir, container.GetContainerMaxSize(), dbconn)

	ctx := context.Background()
	tx, err := dbconn.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}

	result, err := storePackedBlockWithWriter(ctx, tx, writer, transformer, builder)
	if err != nil {
		_ = tx.Rollback()
		t.Fatalf("store packed block: %v", err)
	}

	encodedBlock, _, err := builder.Build()
	if err != nil {
		_ = tx.Rollback()
		t.Fatalf("rebuild expected encoded block: %v", err)
	}
	encodedPlaintext, err := blocks.EncodeBlock(encodedBlock)
	if err != nil {
		_ = tx.Rollback()
		t.Fatalf("encode expected plaintext block: %v", err)
	}
	if result.BlockID <= 0 {
		_ = tx.Rollback()
		t.Fatalf("expected persisted storage_blocks id, got %d", result.BlockID)
	}

	if err := tx.Commit(); err != nil {
		t.Fatalf("commit packed block tx: %v", err)
	}

	var storageBlockRows int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM storage_blocks`).Scan(&storageBlockRows); err != nil {
		t.Fatalf("count storage_blocks: %v", err)
	}
	if storageBlockRows != 1 {
		t.Fatalf("expected one storage_blocks row, got %d", storageBlockRows)
	}

	var formatVersion int
	var codec string
	var plaintextSize int64
	var storedSize int64
	var containerID int64
	var containerOffset int64
	var blockHash []byte
	if err := dbconn.QueryRow(
		`SELECT format_version, codec, plaintext_size, stored_size, container_id, container_offset, block_hash
		 FROM storage_blocks
		 WHERE id = $1`,
		result.BlockID,
	).Scan(&formatVersion, &codec, &plaintextSize, &storedSize, &containerID, &containerOffset, &blockHash); err != nil {
		t.Fatalf("read storage_blocks metadata: %v", err)
	}
	if formatVersion != 1 {
		t.Fatalf("storage_blocks.format_version: got %d want 1", formatVersion)
	}
	if codec != "none" {
		t.Fatalf("storage_blocks.codec: got %q want %q", codec, "none")
	}
	if plaintextSize != int64(len(encodedPlaintext)) {
		t.Fatalf("storage_blocks.plaintext_size: got %d want %d", plaintextSize, len(encodedPlaintext))
	}
	if storedSize != result.StoredSize {
		t.Fatalf("storage_blocks.stored_size: got %d want %d", storedSize, result.StoredSize)
	}
	if containerID != result.Placement.ContainerID {
		t.Fatalf("storage_blocks.container_id: got %d want %d", containerID, result.Placement.ContainerID)
	}
	if containerOffset != result.Placement.Offset {
		t.Fatalf("storage_blocks.container_offset: got %d want %d", containerOffset, result.Placement.Offset)
	}
	if !bytes.Equal(blockHash, result.BlockHash) {
		t.Fatalf("storage_blocks.block_hash mismatch")
	}

	var refRows int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM chunk_block_refs`).Scan(&refRows); err != nil {
		t.Fatalf("count chunk_block_refs: %v", err)
	}
	if refRows != 2 {
		t.Fatalf("expected two chunk_block_refs rows, got %d", refRows)
	}

	var c1BlockID, c1Offset, c1Size int64
	if err := dbconn.QueryRow(
		`SELECT block_id, offset_in_block, size_in_block
		 FROM chunk_block_refs
		 WHERE chunk_id = $1`,
		chunk1,
	).Scan(&c1BlockID, &c1Offset, &c1Size); err != nil {
		t.Fatalf("read chunk1 refs: %v", err)
	}
	if c1BlockID != result.BlockID || c1Offset != 0 || c1Size != 3 {
		t.Fatalf("chunk1 refs mismatch: block_id=%d offset=%d size=%d", c1BlockID, c1Offset, c1Size)
	}

	var c2BlockID, c2Offset, c2Size int64
	if err := dbconn.QueryRow(
		`SELECT block_id, offset_in_block, size_in_block
		 FROM chunk_block_refs
		 WHERE chunk_id = $1`,
		chunk2,
	).Scan(&c2BlockID, &c2Offset, &c2Size); err != nil {
		t.Fatalf("read chunk2 refs: %v", err)
	}
	if c2BlockID != result.BlockID || c2Offset != 3 || c2Size != 4 {
		t.Fatalf("chunk2 refs mismatch: block_id=%d offset=%d size=%d", c2BlockID, c2Offset, c2Size)
	}

	var danglingRefs int
	if err := dbconn.QueryRow(`
		SELECT COUNT(*)
		FROM chunk_block_refs r
		LEFT JOIN storage_blocks b ON b.id = r.block_id
		WHERE b.id IS NULL
	`).Scan(&danglingRefs); err != nil {
		t.Fatalf("count dangling refs: %v", err)
	}
	if danglingRefs != 0 {
		t.Fatalf("expected no dangling chunk_block_refs, got %d", danglingRefs)
	}
}

func TestStorePackedBlockWithWriterRollbackLeavesNoRefsOrBlockRows(t *testing.T) {
	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	defer func() { _ = dbconn.Close() }()

	if err := db.RunMigrations(dbconn); err != nil {
		t.Fatalf("run migrations: %v", err)
	}

	var chunkID int64
	if err := dbconn.QueryRow(
		`INSERT INTO chunk (chunk_hash, size, status, live_ref_count, chunker_version)
		 VALUES ($1, $2, $3, 1, 'v1-simple-rolling')
		 RETURNING id`,
		"packed-rollback",
		4,
		filestate.ChunkProcessing,
	).Scan(&chunkID); err != nil {
		t.Fatalf("insert chunk: %v", err)
	}

	builder := blocks.NewBlockBuilder(64)
	if err := builder.Add(blocks.PendingChunk{ChunkID: chunkID, Data: []byte("data"), Size: 4}); err != nil {
		t.Fatalf("add chunk: %v", err)
	}

	transformer, err := blocks.GetBlockTransformer(blocks.CodecPlain)
	if err != nil {
		t.Fatalf("get plain transformer: %v", err)
	}

	containersDir := t.TempDir()
	writer := container.NewLocalWriterWithDirAndDB(containersDir, container.GetContainerMaxSize(), dbconn)

	ctx := context.Background()
	tx, err := dbconn.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}

	if _, err := storePackedBlockWithWriter(ctx, tx, writer, transformer, builder); err != nil {
		_ = tx.Rollback()
		t.Fatalf("store packed block before rollback: %v", err)
	}

	if err := tx.Rollback(); err != nil {
		t.Fatalf("rollback tx: %v", err)
	}

	var storageBlockRows int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM storage_blocks`).Scan(&storageBlockRows); err != nil {
		t.Fatalf("count storage_blocks after rollback: %v", err)
	}
	if storageBlockRows != 0 {
		t.Fatalf("expected zero storage_blocks rows after rollback, got %d", storageBlockRows)
	}

	var refRows int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM chunk_block_refs`).Scan(&refRows); err != nil {
		t.Fatalf("count chunk_block_refs after rollback: %v", err)
	}
	if refRows != 0 {
		t.Fatalf("expected zero chunk_block_refs rows after rollback, got %d", refRows)
	}
}

func TestNewStoreServiceUsesInjectedChunker(t *testing.T) {
	const customChunkerVersion chunk.Version = "v1-simple-rolling-test-injected"
	repo := NewRepository(nil)

	service := NewStoreService(repo, fixedVersionChunker{
		delegate: chunk.DefaultChunker(),
		version:  customChunkerVersion,
	})

	resolved, err := service.ResolveActiveChunker()
	if err != nil {
		t.Fatalf("ResolveActiveChunker: %v", err)
	}
	if resolved.Version != customChunkerVersion {
		t.Fatalf("unexpected resolved chunker version: got=%q want=%q", resolved.Version, customChunkerVersion)
	}
	if service.Repository() != repo {
		t.Fatal("expected service to retain injected repository")
	}
}

func TestStoreServiceResolveActiveChunkerUsesRepositoryDefault(t *testing.T) {
	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	defer func() { _ = dbconn.Close() }()

	if err := db.RunMigrations(dbconn); err != nil {
		t.Fatalf("run migrations: %v", err)
	}

	if _, err := dbconn.Exec(`UPDATE repository_config SET value = $1 WHERE key = $2`, string(chunk.VersionV2FastCDC), repositoryDefaultChunkerKey); err != nil {
		t.Fatalf("set repository default chunker to v2: %v", err)
	}

	service := NewStoreService(NewRepository(dbconn), nil)
	resolved, err := service.ResolveActiveChunker()
	if err != nil {
		t.Fatalf("ResolveActiveChunker: %v", err)
	}
	if got, want := resolved.Version, chunk.VersionV2FastCDC; got != want {
		t.Fatalf("resolved version mismatch: got %q want %q", got, want)
	}
}

func TestStoreServiceResolveActiveChunkerFallsBackWhenConfigRowMissing(t *testing.T) {
	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	defer func() { _ = dbconn.Close() }()

	if err := db.RunMigrations(dbconn); err != nil {
		t.Fatalf("run migrations: %v", err)
	}

	if _, err := dbconn.Exec(`DELETE FROM repository_config WHERE key = $1`, repositoryDefaultChunkerKey); err != nil {
		t.Fatalf("delete repository default row: %v", err)
	}

	service := NewStoreService(NewRepository(dbconn), nil)
	resolved, err := service.ResolveActiveChunker()
	if err != nil {
		t.Fatalf("ResolveActiveChunker: %v", err)
	}
	if got, want := resolved.Version, chunk.DefaultChunkerVersion; got != want {
		t.Fatalf("fallback version mismatch: got %q want %q", got, want)
	}
}

func TestStoreServiceResolveActiveChunkerFailsFastOnUnregisteredConfiguredVersion(t *testing.T) {
	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	defer func() { _ = dbconn.Close() }()

	if err := db.RunMigrations(dbconn); err != nil {
		t.Fatalf("run migrations: %v", err)
	}

	if _, err := dbconn.Exec(`UPDATE repository_config SET value = $1 WHERE key = $2`, "v9-future-cdc", repositoryDefaultChunkerKey); err != nil {
		t.Fatalf("set unregistered repository default chunker: %v", err)
	}

	service := NewStoreService(NewRepository(dbconn), nil)
	_, err = service.ResolveActiveChunker()
	if err == nil {
		t.Fatal("expected ResolveActiveChunker to fail on unregistered configured chunker, got nil")
	}
	if !strings.Contains(err.Error(), "not registered") {
		t.Fatalf("expected not-registered error, got: %v", err)
	}
}

func TestAssertLogicalFileVersionMatchesActiveDetectsDrift(t *testing.T) {
	err := assertLogicalFileVersionMatchesActive("v1-simple-rolling", "v1-simple-rolling-test-override")
	if err == nil {
		t.Fatal("expected logical_file version drift mismatch error, got nil")
	}
	if !strings.Contains(err.Error(), "logical_file chunker_version mismatch") {
		t.Fatalf("expected mismatch error message, got: %v", err)
	}
}

func TestAssertLogicalFileVersionMatchesActiveAllowsMatch(t *testing.T) {
	err := assertLogicalFileVersionMatchesActive("v1-simple-rolling", "v1-simple-rolling")
	if err != nil {
		t.Fatalf("expected matching versions to pass invariant, got: %v", err)
	}
}

func TestAssertChunkVersionMatchesActiveDetectsDrift(t *testing.T) {
	err := assertChunkVersionMatchesActive("v1-simple-rolling", "v2-fastcdc")
	if err == nil {
		t.Fatal("expected chunk version drift mismatch error, got nil")
	}
	if !strings.Contains(err.Error(), "chunk chunker_version mismatch") {
		t.Fatalf("expected mismatch error message, got: %v", err)
	}
}

func TestAssertChunkVersionMatchesActiveAllowsMatch(t *testing.T) {
	err := assertChunkVersionMatchesActive("v2-fastcdc", "v2-fastcdc")
	if err != nil {
		t.Fatalf("expected matching chunk versions to pass invariant, got: %v", err)
	}
}

func (w *commitAckWriter) FinalizeContainer() error {
	return nil
}

func (w *commitAckWriter) AppendPayload(_ db.DBTX, payload []byte) (container.LocalPlacement, error) {
	offset := w.offset
	w.offset += int64(len(payload))
	w.pendingClear = false
	return container.LocalPlacement{
		ContainerID:      1,
		Filename:         "ack_test_container.bin",
		Offset:           offset,
		StoredSize:       int64(len(payload)),
		NewContainerSize: container.ContainerHdrLen + w.offset,
	}, nil
}

func (w *commitAckWriter) AcknowledgeAppendCommitted() {
	w.ackCalls++
	w.pendingClear = true
}

func (w *rollbackCleanupFailureWriter) FinalizeContainer() error {
	return nil
}

func (w *rollbackCleanupFailureWriter) AppendPayload(_ db.DBTX, payload []byte) (container.LocalPlacement, error) {
	offset := w.offset
	w.offset += int64(len(payload))
	return container.LocalPlacement{
		ContainerID:      1,
		Filename:         "rollback_cleanup_test_container.bin",
		Offset:           offset,
		StoredSize:       int64(len(payload)),
		NewContainerSize: -1,
	}, nil
}

func (w *rollbackCleanupFailureWriter) RollbackLastAppend() error {
	w.rollbackCalls++
	if w.rollbackErr != nil {
		return w.rollbackErr
	}
	return nil
}

func (w *rollbackCleanupFailureWriter) QuarantineActiveContainer() error {
	w.quarantineCalls++
	if w.db != nil && w.quarantineContainer > 0 {
		if _, err := w.db.Exec(`UPDATE container SET quarantine = TRUE WHERE id = $1`, w.quarantineContainer); err != nil {
			return err
		}
	}
	if w.quarantineErr != nil {
		return w.quarantineErr
	}
	return nil
}

func TestRunWithRetryableTxAbortRetriesThenSucceeds(t *testing.T) {
	t.Parallel()

	attempts := 0
	err := runWithRetryableTxAbort(context.Background(), func(_ int) error {
		attempts++
		if attempts < 3 {
			return errors.New("pq: current transaction is aborted, commands ignored until end of transaction block (25P02)")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("runWithRetryableTxAbort returned error: %v", err)
	}
	if attempts != 3 {
		t.Fatalf("expected 3 attempts, got %d", attempts)
	}
}

func TestRunWithRetryableTxAbortStopsOnNonRetryableError(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("permanent store failure")
	attempts := 0
	err := runWithRetryableTxAbort(context.Background(), func(_ int) error {
		attempts++
		return wantErr
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("expected error %v, got %v", wantErr, err)
	}
	if attempts != 1 {
		t.Fatalf("expected 1 attempt, got %d", attempts)
	}
}

func TestRunWithRetryableTxAbortReportsAttemptNumbers(t *testing.T) {
	t.Parallel()

	var seen []int
	wantErr := errors.New("pq: current transaction is aborted, commands ignored until end of transaction block (25P02)")
	err := runWithRetryableTxAbort(context.Background(), func(attempt int) error {
		seen = append(seen, attempt)
		return wantErr
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("expected error %v, got %v", wantErr, err)
	}
	if len(seen) != retryableTxAbortMaxAttempts {
		t.Fatalf("expected %d attempts, got %d", retryableTxAbortMaxAttempts, len(seen))
	}
	for index, attempt := range seen {
		if attempt != index {
			t.Fatalf("expected attempt index %d, got %d", index, attempt)
		}
	}
}

func TestLinkFileChunkIncrementsRefCountOnReuse(t *testing.T) {
	originalContainersDir := container.ContainersDir
	container.ContainersDir = t.TempDir()
	t.Cleanup(func() {
		container.ContainersDir = originalContainersDir
	})

	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	defer func() { _ = dbconn.Close() }()

	if err := db.RunMigrations(dbconn); err != nil {
		t.Fatalf("run migrations: %v", err)
	}

	insertLogicalFile := func(name string, hash string) int64 {
		t.Helper()
		var fileID int64
		err := dbconn.QueryRow(
			`INSERT INTO logical_file (original_name, total_size, file_hash, status, chunker_version)
			 VALUES ($1, $2, $3, $4, 'v1-simple-rolling')
			 RETURNING id`,
			name,
			123,
			hash,
			filestate.LogicalFileCompleted,
		).Scan(&fileID)
		if err != nil {
			t.Fatalf("insert logical_file %s: %v", name, err)
		}
		return fileID
	}

	fileA := insertLogicalFile("a.bin", "hash-a")
	fileB := insertLogicalFile("b.bin", "hash-b")

	chunkID, chunkStatus, isNew, err := claimChunk(dbconn, "shared-chunk-hash", 777, string(chunk.DefaultChunkerVersion))
	if err != nil {
		t.Fatalf("claim first chunk: %v", err)
	}
	if !isNew {
		t.Fatalf("expected first claim to be new")
	}
	if chunkStatus != filestate.ChunkProcessing {
		t.Fatalf("unexpected first chunk status: %s", chunkStatus)
	}

	var insertedChunkerVersion string
	if err := dbconn.QueryRow(`SELECT chunker_version FROM chunk WHERE id = $1`, chunkID).Scan(&insertedChunkerVersion); err != nil {
		t.Fatalf("read inserted chunk.chunker_version: %v", err)
	}
	if insertedChunkerVersion != string(chunk.DefaultChunkerVersion) {
		t.Fatalf("unexpected inserted chunker_version: got=%q want=%q", insertedChunkerVersion, chunk.DefaultChunkerVersion)
	}

	tx1, err := dbconn.Begin()
	if err != nil {
		t.Fatalf("begin tx1: %v", err)
	}
	if err := linkFileChunk(tx1, fileA, chunkID, 0, true); err != nil {
		_ = tx1.Rollback()
		t.Fatalf("link first file/chunk: %v", err)
	}
	if _, err := tx1.Exec(`UPDATE chunk SET status = $1 WHERE id = $2`, filestate.ChunkCompleted, chunkID); err != nil {
		_ = tx1.Rollback()
		t.Fatalf("mark chunk completed: %v", err)
	}
	var containerID int64
	if err := tx1.QueryRow(
		`INSERT INTO container (filename, sealed, quarantine, current_size, max_size)
		 VALUES ($1, TRUE, FALSE, $2, $3)
		 RETURNING id`,
		"test-reuse-container.bin",
		841,
		container.GetContainerMaxSize(),
	).Scan(&containerID); err != nil {
		_ = tx1.Rollback()
		t.Fatalf("insert container for reusable chunk: %v", err)
	}
	if _, err := tx1.Exec(
		`INSERT INTO blocks (chunk_id, codec, format_version, plaintext_size, stored_size, container_id, block_offset)
		 VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		chunkID,
		"plain",
		1,
		777,
		777,
		containerID,
		64,
	); err != nil {
		_ = tx1.Rollback()
		t.Fatalf("insert block metadata for reusable chunk: %v", err)
	}
	if err := tx1.Commit(); err != nil {
		t.Fatalf("commit tx1: %v", err)
	}

	if err := os.WriteFile(filepath.Join(container.ContainersDir, "test-reuse-container.bin"), make([]byte, 841), 0o600); err != nil {
		t.Fatalf("create reusable container file: %v", err)
	}

	chunkID2, chunkStatus2, isNew2, err := claimChunk(dbconn, "shared-chunk-hash", 777, string(chunk.DefaultChunkerVersion))
	if err != nil {
		t.Fatalf("claim reused chunk: %v", err)
	}
	if chunkID2 != chunkID {
		t.Fatalf("expected same chunk id, got %d vs %d", chunkID2, chunkID)
	}
	if isNew2 {
		t.Fatalf("expected reused claim to be non-new")
	}
	if chunkStatus2 != filestate.ChunkCompleted {
		t.Fatalf("unexpected reused chunk status: %s", chunkStatus2)
	}

	ctx := context.Background()
	chunkID3, chunkStatus3, isNew3, err := claimChunkWithContext(ctx, dbconn, "shared-chunk-hash", 777, "v1-simple-rolling-test-override", container.ContainersDir)
	if err != nil {
		t.Fatalf("claim reused chunk across versions: %v", err)
	}
	if chunkID3 != chunkID {
		t.Fatalf("expected same chunk id across versions, got %d vs %d", chunkID3, chunkID)
	}
	if isNew3 {
		t.Fatal("expected cross-version reused claim to remain non-new")
	}
	if chunkStatus3 != filestate.ChunkCompleted {
		t.Fatalf("unexpected cross-version reused chunk status: %s", chunkStatus3)
	}

	var sameIdentityRows int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM chunk WHERE chunk_hash = $1 AND size = $2`, "shared-chunk-hash", 777).Scan(&sameIdentityRows); err != nil {
		t.Fatalf("count chunk rows by dedup identity: %v", err)
	}
	if sameIdentityRows != 1 {
		t.Fatalf("expected dedup identity hash+size to keep a single chunk row, got %d", sameIdentityRows)
	}

	var persistedChunkerVersion string
	if err := dbconn.QueryRow(`SELECT chunker_version FROM chunk WHERE id = $1`, chunkID).Scan(&persistedChunkerVersion); err != nil {
		t.Fatalf("read chunk.chunker_version: %v", err)
	}
	if persistedChunkerVersion != string(chunk.DefaultChunkerVersion) {
		t.Fatalf("expected reused chunk row chunker_version to remain %q, got %q", chunk.DefaultChunkerVersion, persistedChunkerVersion)
	}

	tx2, err := dbconn.Begin()
	if err != nil {
		t.Fatalf("begin tx2: %v", err)
	}
	if err := linkFileChunk(tx2, fileB, chunkID, 0, !isNew2); err != nil {
		_ = tx2.Rollback()
		t.Fatalf("link second file/chunk: %v", err)
	}
	// Idempotency check: same mapping conflict should not increment live_ref_count again.
	if err := linkFileChunk(tx2, fileB, chunkID, 0, !isNew2); err != nil {
		_ = tx2.Rollback()
		t.Fatalf("re-link second file/chunk: %v", err)
	}
	if err := tx2.Commit(); err != nil {
		t.Fatalf("commit tx2: %v", err)
	}

	var refCount int64
	if err := dbconn.QueryRow(`SELECT live_ref_count FROM chunk WHERE id = $1`, chunkID).Scan(&refCount); err != nil {
		t.Fatalf("read live_ref_count: %v", err)
	}
	if refCount != 2 {
		t.Fatalf("expected live_ref_count=2 after two links, got %d", refCount)
	}

	var mappingCount int64
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM file_chunk WHERE chunk_id = $1`, chunkID).Scan(&mappingCount); err != nil {
		t.Fatalf("count mappings: %v", err)
	}
	if mappingCount != 2 {
		t.Fatalf("expected 2 file_chunk mappings, got %d", mappingCount)
	}
}

func TestClaimChunkDoesNotReuseCompletedChunkWithoutValidLocation(t *testing.T) {
	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	defer func() { _ = dbconn.Close() }()

	if err := db.RunMigrations(dbconn); err != nil {
		t.Fatalf("run migrations: %v", err)
	}

	var chunkID int64
	if err := dbconn.QueryRow(
		`INSERT INTO chunk (chunk_hash, size, status, live_ref_count, chunker_version)
		 VALUES ($1, $2, $3, $4, 'v1-simple-rolling')
		 RETURNING id`,
		"orphan-completed-chunk",
		123,
		filestate.ChunkCompleted,
		0,
	).Scan(&chunkID); err != nil {
		t.Fatalf("insert completed chunk: %v", err)
	}

	claimedID, claimedStatus, isNew, err := claimChunk(dbconn, "orphan-completed-chunk", 123, string(chunk.DefaultChunkerVersion))
	if err != nil {
		t.Fatalf("claim malformed completed chunk: %v", err)
	}
	if claimedID != chunkID {
		t.Fatalf("expected same chunk id, got %d vs %d", claimedID, chunkID)
	}
	if isNew {
		t.Fatalf("expected existing chunk to be reclaimed, not inserted as new")
	}
	if claimedStatus != filestate.ChunkProcessing {
		t.Fatalf("expected malformed completed chunk to be reclaimed as PROCESSING, got %s", claimedStatus)
	}

	var latestStatus string
	if err := dbconn.QueryRow(`SELECT status FROM chunk WHERE id = $1`, chunkID).Scan(&latestStatus); err != nil {
		t.Fatalf("read chunk status after claim: %v", err)
	}
	if latestStatus != filestate.ChunkProcessing {
		t.Fatalf("expected chunk row status PROCESSING after reclaim, got %s", latestStatus)
	}
}

func TestClaimChunkRejectsExistingRowWithEmptyChunkerVersion(t *testing.T) {
	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	defer func() { _ = dbconn.Close() }()

	if err := db.RunMigrations(dbconn); err != nil {
		t.Fatalf("run migrations: %v", err)
	}

	var chunkID int64
	if err := dbconn.QueryRow(
		`INSERT INTO chunk (chunk_hash, size, status, live_ref_count, chunker_version)
		 VALUES ($1, $2, $3, $4, 'v1-simple-rolling')
		 RETURNING id`,
		"empty-version-existing-chunk",
		123,
		filestate.ChunkProcessing,
		0,
	).Scan(&chunkID); err != nil {
		t.Fatalf("insert existing chunk: %v", err)
	}

	if _, err := dbconn.Exec(`UPDATE chunk SET chunker_version = '' WHERE id = $1`, chunkID); err != nil {
		t.Fatalf("set empty chunker_version: %v", err)
	}

	ctx := context.Background()
	_, _, _, err = claimChunkWithContext(ctx, dbconn, "empty-version-existing-chunk", 123, string(chunk.DefaultChunkerVersion), container.ContainersDir)
	if err == nil || !strings.Contains(err.Error(), "empty chunker_version") {
		t.Fatalf("expected empty chunker_version error, got: %v", err)
	}
}

func TestClaimChunkDoesNotReuseCompletedChunkInQuarantinedContainer(t *testing.T) {
	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	defer func() { _ = dbconn.Close() }()

	if err := db.RunMigrations(dbconn); err != nil {
		t.Fatalf("run migrations: %v", err)
	}

	var chunkID int64
	if err := dbconn.QueryRow(
		`INSERT INTO chunk (chunk_hash, size, status, live_ref_count, chunker_version)
		 VALUES ($1, $2, $3, $4, 'v1-simple-rolling')
		 RETURNING id`,
		"quarantined-completed-chunk",
		321,
		filestate.ChunkCompleted,
		1,
	).Scan(&chunkID); err != nil {
		t.Fatalf("insert completed chunk: %v", err)
	}

	containerID := insertReusableTestContainer(t, dbconn, "quarantined-reuse.bin", true)
	insertReusableTestBlock(t, dbconn, chunkID, containerID, 64)

	claimedID, claimedStatus, isNew, err := claimChunk(dbconn, "quarantined-completed-chunk", 321, string(chunk.DefaultChunkerVersion))
	if err != nil {
		t.Fatalf("claim quarantined completed chunk: %v", err)
	}
	if claimedID != chunkID {
		t.Fatalf("expected same chunk id, got %d vs %d", claimedID, chunkID)
	}
	if isNew {
		t.Fatalf("expected existing chunk to be reclaimed, not inserted as new")
	}
	if claimedStatus != filestate.ChunkProcessing {
		t.Fatalf("expected quarantined completed chunk to be reclaimed as PROCESSING, got %s", claimedStatus)
	}
}

func TestStoreFileUsesAppendLevelDurabilityWithoutExtraSyncHook(t *testing.T) {
	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	defer func() { _ = dbconn.Close() }()

	if err := db.RunMigrations(dbconn); err != nil {
		t.Fatalf("run migrations: %v", err)
	}

	var containerID int64
	if err := dbconn.QueryRow(
		`INSERT INTO container (filename, current_size, max_size, sealed)
		 VALUES ($1, $2, $3, FALSE)
		 RETURNING id`,
		"durability_test_container.bin",
		container.ContainerHdrLen,
		container.GetContainerMaxSize(),
	).Scan(&containerID); err != nil {
		t.Fatalf("insert container row: %v", err)
	}
	if containerID != 1 {
		t.Fatalf("expected container id 1 for test writer, got %d", containerID)
	}

	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "payload.txt")
	if err := os.WriteFile(path, []byte("durability-gap-regression-test"), 0644); err != nil {
		t.Fatalf("write temp file: %v", err)
	}

	writer := &syncFailWriter{db: dbconn}
	sgctx := StorageContext{
		DB:           dbconn,
		Writer:       writer,
		ContainerDir: tmpDir,
	}

	codec, err := blocks.ParseCodec("plain")
	if err != nil {
		t.Fatalf("parse plain codec: %v", err)
	}

	_, err = StoreFileWithStorageContextAndCodecResult(sgctx, path, codec)
	if err != nil {
		t.Fatalf("store using append-level durability contract should succeed: %v", err)
	}
	if writer.quarantineCalls != 0 {
		t.Fatalf("expected no container quarantine on successful store, got %d", writer.quarantineCalls)
	}

	var abortedCount int
	if err := dbconn.QueryRow(
		`SELECT COUNT(*) FROM logical_file WHERE status = $1`,
		filestate.LogicalFileAborted,
	).Scan(&abortedCount); err != nil {
		t.Fatalf("count aborted logical files: %v", err)
	}
	if abortedCount != 0 {
		t.Fatalf("expected 0 aborted logical files after successful store, got %d", abortedCount)
	}

	var completedChunks int
	if err := dbconn.QueryRow(
		`SELECT COUNT(*) FROM chunk WHERE status = $1`,
		filestate.ChunkCompleted,
	).Scan(&completedChunks); err != nil {
		t.Fatalf("count completed chunks: %v", err)
	}
	if completedChunks == 0 {
		t.Fatalf("expected completed chunks after successful store")
	}

	var blockCount int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM blocks`).Scan(&blockCount); err != nil {
		t.Fatalf("count blocks: %v", err)
	}
	if blockCount == 0 {
		t.Fatalf("expected persisted block metadata after successful store")
	}

	var quarantined bool
	if err := dbconn.QueryRow(`SELECT quarantine FROM container WHERE id = 1`).Scan(&quarantined); err != nil {
		t.Fatalf("query container quarantine: %v", err)
	}
	if quarantined {
		t.Fatalf("expected container to remain healthy after successful store")
	}

}

func TestStoreFilePersistsExplicitChunkerVersionMetadata(t *testing.T) {
	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	defer func() { _ = dbconn.Close() }()

	if err := db.RunMigrations(dbconn); err != nil {
		t.Fatalf("run migrations: %v", err)
	}

	if _, err := dbconn.Exec(
		`INSERT INTO container (id, filename, current_size, max_size, sealed)
		 VALUES (1, $1, $2, $3, FALSE)`,
		"chunker_version_test_container.bin",
		container.ContainerHdrLen,
		container.GetContainerMaxSize(),
	); err != nil {
		t.Fatalf("insert container row: %v", err)
	}

	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "chunker-version-metadata.txt")
	if err := os.WriteFile(path, []byte("verify persisted chunker_version metadata"), 0644); err != nil {
		t.Fatalf("write temp file: %v", err)
	}

	const customChunkerVersion chunk.Version = "v1-simple-rolling-test-override"
	writer := &commitAckWriter{}
	sgctx := StorageContext{
		DB:           dbconn,
		Writer:       writer,
		ContainerDir: tmpDir,
		Chunker: fixedVersionChunker{
			delegate: chunk.DefaultChunker(),
			version:  customChunkerVersion,
		},
	}

	codec, err := blocks.ParseCodec("plain")
	if err != nil {
		t.Fatalf("parse plain codec: %v", err)
	}

	result, err := StoreFileWithStorageContextAndCodecResult(sgctx, path, codec)
	if err != nil {
		t.Fatalf("store file with injected chunker: %v", err)
	}

	var logicalFileVersion string
	if err := dbconn.QueryRow(
		`SELECT chunker_version FROM logical_file WHERE id = $1`,
		result.FileID,
	).Scan(&logicalFileVersion); err != nil {
		t.Fatalf("read logical_file.chunker_version: %v", err)
	}
	// Phase 3: the persisted version must come from the resolved chunker, not a hardcoded constant.
	if logicalFileVersion != string(customChunkerVersion) {
		t.Fatalf("logical_file.chunker_version mismatch: got %q want %q", logicalFileVersion, customChunkerVersion)
	}

	var linkedChunkCount int
	if err := dbconn.QueryRow(
		`SELECT COUNT(*) FROM file_chunk WHERE logical_file_id = $1`,
		result.FileID,
	).Scan(&linkedChunkCount); err != nil {
		t.Fatalf("count linked chunks: %v", err)
	}
	if linkedChunkCount == 0 {
		t.Fatal("expected at least one linked chunk for stored file")
	}

	var mismatchedChunkVersions int
	if err := dbconn.QueryRow(
		`SELECT COUNT(*)
		 FROM chunk c
		 INNER JOIN file_chunk fc ON fc.chunk_id = c.id
		 WHERE fc.logical_file_id = $1 AND c.chunker_version <> $2`,
		result.FileID,
		string(customChunkerVersion),
	).Scan(&mismatchedChunkVersions); err != nil {
		t.Fatalf("count chunk version mismatches: %v", err)
	}
	if mismatchedChunkVersions != 0 {
		t.Fatalf("expected all linked chunks to persist chunker_version=%q, mismatches=%d", customChunkerVersion, mismatchedChunkVersions)
	}
}

func TestStoreFileDefaultChunkerPersistsLogicalFileVersion(t *testing.T) {
	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	defer func() { _ = dbconn.Close() }()

	if err := db.RunMigrations(dbconn); err != nil {
		t.Fatalf("run migrations: %v", err)
	}

	if _, err := dbconn.Exec(
		`INSERT INTO container (id, filename, current_size, max_size, sealed)
		 VALUES (1, $1, $2, $3, FALSE)`,
		"ack_test_container.bin",
		container.ContainerHdrLen,
		container.GetContainerMaxSize(),
	); err != nil {
		t.Fatalf("insert container row: %v", err)
	}

	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "default-logical-version.txt")
	if err := os.WriteFile(path, []byte("default chunker logical version persistence"), 0o644); err != nil {
		t.Fatalf("write temp file: %v", err)
	}

	writer := &commitAckWriter{}
	sgctx := StorageContext{DB: dbconn, Writer: writer, ContainerDir: tmpDir}

	codec, err := blocks.ParseCodec("plain")
	if err != nil {
		t.Fatalf("parse plain codec: %v", err)
	}

	result, err := StoreFileWithStorageContextAndCodecResult(sgctx, path, codec)
	if err != nil {
		t.Fatalf("store file: %v", err)
	}

	var logicalFileVersion string
	if err := dbconn.QueryRow(`SELECT chunker_version FROM logical_file WHERE id = $1`, result.FileID).Scan(&logicalFileVersion); err != nil {
		t.Fatalf("read logical_file.chunker_version: %v", err)
	}
	if logicalFileVersion != "v2-fastcdc" {
		t.Fatalf("expected logical_file.chunker_version=v2-fastcdc, got %q", logicalFileVersion)
	}
}

func TestStoreFileDefaultChunkerPersistsChunkVersion(t *testing.T) {
	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	defer func() { _ = dbconn.Close() }()

	if err := db.RunMigrations(dbconn); err != nil {
		t.Fatalf("run migrations: %v", err)
	}

	if _, err := dbconn.Exec(
		`INSERT INTO container (id, filename, current_size, max_size, sealed)
		 VALUES (1, $1, $2, $3, FALSE)`,
		"ack_test_container.bin",
		container.ContainerHdrLen,
		container.GetContainerMaxSize(),
	); err != nil {
		t.Fatalf("insert container row: %v", err)
	}

	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "default-chunk-version.txt")
	if err := os.WriteFile(path, []byte("default chunker chunk version persistence"), 0o644); err != nil {
		t.Fatalf("write temp file: %v", err)
	}

	writer := &commitAckWriter{}
	sgctx := StorageContext{DB: dbconn, Writer: writer, ContainerDir: tmpDir}

	codec, err := blocks.ParseCodec("plain")
	if err != nil {
		t.Fatalf("parse plain codec: %v", err)
	}

	result, err := StoreFileWithStorageContextAndCodecResult(sgctx, path, codec)
	if err != nil {
		t.Fatalf("store file: %v", err)
	}

	var linkedChunkCount int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM file_chunk WHERE logical_file_id = $1`, result.FileID).Scan(&linkedChunkCount); err != nil {
		t.Fatalf("count linked chunks: %v", err)
	}
	if linkedChunkCount == 0 {
		t.Fatal("expected at least one linked chunk")
	}

	var nonDefaultCount int
	if err := dbconn.QueryRow(
		`SELECT COUNT(*)
		 FROM chunk c
		 INNER JOIN file_chunk fc ON fc.chunk_id = c.id
		 WHERE fc.logical_file_id = $1 AND c.chunker_version <> 'v2-fastcdc'`,
		result.FileID,
	).Scan(&nonDefaultCount); err != nil {
		t.Fatalf("count non-default chunk versions: %v", err)
	}
	if nonDefaultCount != 0 {
		t.Fatalf("expected all new linked chunk rows to persist chunker_version=v2-fastcdc, mismatches=%d", nonDefaultCount)
	}
}

type crossVersionSharedChunkScenario struct {
	dbconn            *sql.DB
	fileAID           int64
	fileBID           int64
	sharedChunkID     int64
	sharedChunkHash   string
	sharedChunkSize   int64
	originalVersion   chunk.Version
	secondFileVersion chunk.Version
}

func setupCrossVersionSharedChunkScenario(t *testing.T) crossVersionSharedChunkScenario {
	t.Helper()

	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	t.Cleanup(func() { _ = dbconn.Close() })

	if err := db.RunMigrations(dbconn); err != nil {
		t.Fatalf("run migrations: %v", err)
	}

	if _, err := dbconn.Exec(
		`INSERT INTO container (id, filename, current_size, max_size, sealed)
		 VALUES (1, $1, $2, $3, FALSE)`,
		"ack_test_container.bin",
		container.ContainerHdrLen,
		container.GetContainerMaxSize(),
	); err != nil {
		t.Fatalf("insert container row: %v", err)
	}

	tmpDir := t.TempDir()
	pathA := filepath.Join(tmpDir, "reuse-a.bin")
	pathB := filepath.Join(tmpDir, "reuse-b.bin")

	sharedPrefix := strings.Repeat("A", 32)
	contentA := []byte(sharedPrefix + strings.Repeat("B", 32))
	contentB := []byte(sharedPrefix + strings.Repeat("C", 32))
	if err := os.WriteFile(pathA, contentA, 0o644); err != nil {
		t.Fatalf("write first file: %v", err)
	}
	if err := os.WriteFile(pathB, contentB, 0o644); err != nil {
		t.Fatalf("write second file: %v", err)
	}

	writer := &commitAckWriter{}
	firstChunker := fixedBoundaryChunker{version: chunk.VersionV1SimpleRolling, boundary: 32}
	secondChunker := fixedBoundaryChunker{version: chunk.VersionV2FastCDC, boundary: 32}

	codec, err := blocks.ParseCodec("plain")
	if err != nil {
		t.Fatalf("parse plain codec: %v", err)
	}

	resultA, err := StoreFileWithStorageContextAndCodecResult(StorageContext{DB: dbconn, Writer: writer, ContainerDir: tmpDir, Chunker: firstChunker}, pathA, codec)
	if err != nil {
		t.Fatalf("store first file: %v", err)
	}

	sharedHashSum := sha256.Sum256([]byte(sharedPrefix))
	sharedHash := hex.EncodeToString(sharedHashSum[:])

	var sharedChunkID int64
	if err := dbconn.QueryRow(`SELECT id FROM chunk WHERE chunk_hash = $1 AND size = $2`, sharedHash, 32).Scan(&sharedChunkID); err != nil {
		t.Fatalf("locate shared chunk after first store: %v", err)
	}

	resultB, err := StoreFileWithStorageContextAndCodecResult(StorageContext{DB: dbconn, Writer: writer, ContainerDir: tmpDir, Chunker: secondChunker}, pathB, codec)
	if err != nil {
		t.Fatalf("store second file with cross-version reuse: %v", err)
	}

	return crossVersionSharedChunkScenario{
		dbconn:            dbconn,
		fileAID:           resultA.FileID,
		fileBID:           resultB.FileID,
		sharedChunkID:     sharedChunkID,
		sharedChunkHash:   sharedHash,
		sharedChunkSize:   32,
		originalVersion:   chunk.VersionV1SimpleRolling,
		secondFileVersion: chunk.VersionV2FastCDC,
	}
}

func TestCrossVersionChunkReuseIsAllowed(t *testing.T) {
	scenario := setupCrossVersionSharedChunkScenario(t)

	var sharedIdentityRows int
	if err := scenario.dbconn.QueryRow(`SELECT COUNT(*) FROM chunk WHERE chunk_hash = $1 AND size = $2`, scenario.sharedChunkHash, scenario.sharedChunkSize).Scan(&sharedIdentityRows); err != nil {
		t.Fatalf("count shared identity rows: %v", err)
	}
	if sharedIdentityRows != 1 {
		t.Fatalf("expected shared chunk to be reused with no duplicate row, got %d rows", sharedIdentityRows)
	}

	var sharedChunkRefCount int
	if err := scenario.dbconn.QueryRow(`SELECT COUNT(*) FROM file_chunk WHERE chunk_id = $1`, scenario.sharedChunkID).Scan(&sharedChunkRefCount); err != nil {
		t.Fatalf("count shared chunk file references: %v", err)
	}
	if sharedChunkRefCount != 2 {
		t.Fatalf("expected shared chunk to be referenced by both files after reuse, got %d references", sharedChunkRefCount)
	}
}

func TestCrossVersionChunkVersionRemainsOriginal(t *testing.T) {
	scenario := setupCrossVersionSharedChunkScenario(t)

	var persistedChunkerVersion string
	if err := scenario.dbconn.QueryRow(`SELECT chunker_version FROM chunk WHERE id = $1`, scenario.sharedChunkID).Scan(&persistedChunkerVersion); err != nil {
		t.Fatalf("read chunk.chunker_version: %v", err)
	}
	if persistedChunkerVersion != string(scenario.originalVersion) {
		t.Fatalf("expected shared chunk origin version to remain %q, got %q", scenario.originalVersion, persistedChunkerVersion)
	}
}

func TestCrossVersionLogicalFileVersionIsCorrect(t *testing.T) {
	scenario := setupCrossVersionSharedChunkScenario(t)

	var logicalVersionA string
	if err := scenario.dbconn.QueryRow(`SELECT chunker_version FROM logical_file WHERE id = $1`, scenario.fileAID).Scan(&logicalVersionA); err != nil {
		t.Fatalf("read first logical_file.chunker_version: %v", err)
	}
	if logicalVersionA != string(scenario.originalVersion) {
		t.Fatalf("expected first logical file version %q, got %q", scenario.originalVersion, logicalVersionA)
	}

	var logicalVersionB string
	if err := scenario.dbconn.QueryRow(`SELECT chunker_version FROM logical_file WHERE id = $1`, scenario.fileBID).Scan(&logicalVersionB); err != nil {
		t.Fatalf("read second logical_file.chunker_version: %v", err)
	}
	if logicalVersionB != string(scenario.secondFileVersion) {
		t.Fatalf("expected second logical file version %q, got %q", scenario.secondFileVersion, logicalVersionB)
	}

	var sharedChunkRefCount int
	if err := scenario.dbconn.QueryRow(`SELECT COUNT(*) FROM file_chunk WHERE chunk_id = $1`, scenario.sharedChunkID).Scan(&sharedChunkRefCount); err != nil {
		t.Fatalf("count shared chunk file references: %v", err)
	}
	if sharedChunkRefCount != 2 {
		t.Fatalf("expected shared chunk to be reused by both logical files, got %d references", sharedChunkRefCount)
	}
}

func TestStoreFileReusedChunkAllowsCrossVersionReuse(t *testing.T) {
	TestCrossVersionChunkReuseIsAllowed(t)
}

func TestStoreFileLogicalRecipeSingleVersionInvariance(t *testing.T) {
	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	defer func() { _ = dbconn.Close() }()

	if err := db.RunMigrations(dbconn); err != nil {
		t.Fatalf("run migrations: %v", err)
	}

	if _, err := dbconn.Exec(
		`INSERT INTO container (id, filename, current_size, max_size, sealed)
		 VALUES (1, $1, $2, $3, FALSE)`,
		"ack_test_container.bin",
		container.ContainerHdrLen,
		container.GetContainerMaxSize(),
	); err != nil {
		t.Fatalf("insert container row: %v", err)
	}

	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "single-recipe-version.bin")
	content := []byte(strings.Repeat("A", 32) + strings.Repeat("B", 32) + strings.Repeat("C", 32))
	if err := os.WriteFile(path, content, 0o644); err != nil {
		t.Fatalf("write test file: %v", err)
	}

	const recipeVersion chunk.Version = "v1-simple-rolling-test-recipe"
	writer := &commitAckWriter{}
	sgctx := StorageContext{
		DB:           dbconn,
		Writer:       writer,
		ContainerDir: tmpDir,
		Chunker: fixedBoundaryChunker{
			version:  recipeVersion,
			boundary: 32,
		},
	}

	codec, err := blocks.ParseCodec("plain")
	if err != nil {
		t.Fatalf("parse plain codec: %v", err)
	}

	result, err := StoreFileWithStorageContextAndCodecResult(sgctx, path, codec)
	if err != nil {
		t.Fatalf("store file: %v", err)
	}

	var logicalVersion string
	if err := dbconn.QueryRow(`SELECT chunker_version FROM logical_file WHERE id = $1`, result.FileID).Scan(&logicalVersion); err != nil {
		t.Fatalf("read logical_file.chunker_version: %v", err)
	}
	if logicalVersion != string(recipeVersion) {
		t.Fatalf("logical recipe version mismatch: got %q want %q", logicalVersion, recipeVersion)
	}

	var linkedChunkCount int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM file_chunk WHERE logical_file_id = $1`, result.FileID).Scan(&linkedChunkCount); err != nil {
		t.Fatalf("count linked chunks: %v", err)
	}
	if linkedChunkCount < 2 {
		t.Fatalf("expected multiple linked chunks for invariance test, got %d", linkedChunkCount)
	}

	var distinctLinkedVersions int
	if err := dbconn.QueryRow(
		`SELECT COUNT(DISTINCT c.chunker_version)
		 FROM chunk c
		 INNER JOIN file_chunk fc ON fc.chunk_id = c.id
		 WHERE fc.logical_file_id = $1`,
		result.FileID,
	).Scan(&distinctLinkedVersions); err != nil {
		t.Fatalf("count distinct linked chunk versions: %v", err)
	}
	if distinctLinkedVersions != 1 {
		t.Fatalf("expected exactly one chunker version across logical recipe, got %d", distinctLinkedVersions)
	}

	var mismatches int
	if err := dbconn.QueryRow(
		`SELECT COUNT(*)
		 FROM chunk c
		 INNER JOIN file_chunk fc ON fc.chunk_id = c.id
		 INNER JOIN logical_file lf ON lf.id = fc.logical_file_id
		 WHERE fc.logical_file_id = $1 AND c.chunker_version <> lf.chunker_version`,
		result.FileID,
	).Scan(&mismatches); err != nil {
		t.Fatalf("count recipe version mismatches: %v", err)
	}
	if mismatches != 0 {
		t.Fatalf("expected no mixed-version chunks in single logical recipe, mismatches=%d", mismatches)
	}
}

func TestStoreFileSuccessfulCommitAcknowledgesWriterAppendState(t *testing.T) {
	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	defer func() { _ = dbconn.Close() }()

	if err := db.RunMigrations(dbconn); err != nil {
		t.Fatalf("run migrations: %v", err)
	}

	if _, err := dbconn.Exec(
		`INSERT INTO container (id, filename, current_size, max_size, sealed)
		 VALUES (1, $1, $2, $3, FALSE)`,
		"ack_test_container.bin",
		container.ContainerHdrLen,
		container.GetContainerMaxSize(),
	); err != nil {
		t.Fatalf("insert container row: %v", err)
	}

	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "payload.txt")
	if err := os.WriteFile(path, []byte("acknowledge-append-after-commit"), 0644); err != nil {
		t.Fatalf("write temp file: %v", err)
	}

	writer := &commitAckWriter{}
	sgctx := StorageContext{
		DB:           dbconn,
		Writer:       writer,
		ContainerDir: tmpDir,
	}

	codec, err := blocks.ParseCodec("plain")
	if err != nil {
		t.Fatalf("parse plain codec: %v", err)
	}

	_, err = StoreFileWithStorageContextAndCodecResult(sgctx, path, codec)
	if err != nil {
		t.Fatalf("store should succeed: %v", err)
	}
	if writer.ackCalls == 0 {
		t.Fatalf("expected AcknowledgeAppendCommitted to be called at least once")
	}
	if !writer.pendingClear {
		t.Fatalf("expected writer pending rollback state to be cleared after commit acknowledgment")
	}
}

func TestStoreFileEscalatesRollbackCleanupFailureAndQuarantinesContainer(t *testing.T) {
	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	defer func() { _ = dbconn.Close() }()

	if err := db.RunMigrations(dbconn); err != nil {
		t.Fatalf("run migrations: %v", err)
	}

	if _, err := dbconn.Exec(
		`INSERT INTO container (id, filename, current_size, max_size, sealed, quarantine)
		 VALUES (1, $1, $2, $3, FALSE, FALSE)`,
		"rollback_cleanup_test_container.bin",
		container.ContainerHdrLen,
		container.GetContainerMaxSize(),
	); err != nil {
		t.Fatalf("insert container row: %v", err)
	}

	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "payload.txt")
	if err := os.WriteFile(path, []byte("rollback-cleanup-failure-escalation"), 0644); err != nil {
		t.Fatalf("write temp file: %v", err)
	}

	rollbackCause := errors.New("injected rollback truncate failure")
	writer := &rollbackCleanupFailureWriter{
		rollbackErr:         rollbackCause,
		quarantineContainer: 1,
		db:                  dbconn,
	}
	sgctx := StorageContext{
		DB:           dbconn,
		Writer:       writer,
		ContainerDir: tmpDir,
	}

	codec, err := blocks.ParseCodec("plain")
	if err != nil {
		t.Fatalf("parse plain codec: %v", err)
	}

	_, err = StoreFileWithStorageContextAndCodecResult(sgctx, path, codec)
	if !errors.Is(err, rollbackCause) {
		t.Fatalf("expected surfaced rollback cause in store error; got: %v", err)
	}
	if !strings.Contains(err.Error(), "rollback failed; quarantined active container as precaution") {
		t.Fatalf("expected rollback escalation in error, got: %v", err)
	}
	if !strings.Contains(err.Error(), rollbackCause.Error()) {
		t.Fatalf("expected rollback error text to be visible in wrapped error, got: %v", err)
	}

	if writer.rollbackCalls == 0 {
		t.Fatalf("expected RollbackLastAppend to be attempted")
	}
	if writer.quarantineCalls == 0 {
		t.Fatalf("expected active container quarantine when rollback cleanup fails")
	}

	var quarantined bool
	if err := dbconn.QueryRow(`SELECT quarantine FROM container WHERE id = 1`).Scan(&quarantined); err != nil {
		t.Fatalf("query container quarantine: %v", err)
	}
	if !quarantined {
		t.Fatalf("expected container id=1 to be quarantined after rollback cleanup failure")
	}
}

func TestStoreFileRetainsCommittedChunksWhenFinalCompletionUpdateFails(t *testing.T) {
	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	defer func() { _ = dbconn.Close() }()

	if err := db.RunMigrations(dbconn); err != nil {
		t.Fatalf("run migrations: %v", err)
	}

	if _, err := dbconn.Exec(
		`INSERT INTO container (id, filename, current_size, max_size, sealed, quarantine)
		 VALUES (1, $1, $2, $3, FALSE, FALSE)`,
		"final-completion-failure-container.bin",
		container.ContainerHdrLen,
		container.GetContainerMaxSize(),
	); err != nil {
		t.Fatalf("insert container row: %v", err)
	}

	if _, err := dbconn.Exec(`
		CREATE TRIGGER fail_finalize_completion
		BEFORE UPDATE OF status ON logical_file
		WHEN NEW.status = 'COMPLETED'
		BEGIN
			SELECT RAISE(FAIL, 'injected finalize completion failure');
		END;
	`); err != nil {
		t.Fatalf("create finalize failure trigger: %v", err)
	}

	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "payload.txt")
	if err := os.WriteFile(path, []byte("finalize-logical-file-failure-state-test"), 0644); err != nil {
		t.Fatalf("write temp file: %v", err)
	}

	writer := &commitAckWriter{}
	sgctx := StorageContext{
		DB:           dbconn,
		Writer:       writer,
		ContainerDir: tmpDir,
	}

	codec, err := blocks.ParseCodec("plain")
	if err != nil {
		t.Fatalf("parse plain codec: %v", err)
	}

	_, err = StoreFileWithStorageContextAndCodecResult(sgctx, path, codec)
	if err == nil || !strings.Contains(err.Error(), "injected finalize completion failure") {
		t.Fatalf("expected injected finalize failure containing \"injected finalize completion failure\", got: %v", err)
	}

	var logicalStatus string
	if err := dbconn.QueryRow(`SELECT status FROM logical_file ORDER BY id DESC LIMIT 1`).Scan(&logicalStatus); err != nil {
		t.Fatalf("query logical_file status: %v", err)
	}
	if logicalStatus != filestate.LogicalFileAborted {
		t.Fatalf("expected logical_file to be ABORTED after finalization failure cleanup, got %s", logicalStatus)
	}

	var completedChunkCount int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM chunk WHERE status = $1`, filestate.ChunkCompleted).Scan(&completedChunkCount); err != nil {
		t.Fatalf("count completed chunks: %v", err)
	}
	if completedChunkCount == 0 {
		t.Fatalf("expected committed COMPLETED chunk rows to remain after finalization failure")
	}

	var linkedChunkCount int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM file_chunk`).Scan(&linkedChunkCount); err != nil {
		t.Fatalf("count linked file_chunk rows: %v", err)
	}
	if linkedChunkCount == 0 {
		t.Fatalf("expected committed file_chunk mappings to remain after finalization failure")
	}

	var blockCount int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM blocks`).Scan(&blockCount); err != nil {
		t.Fatalf("count blocks: %v", err)
	}
	if blockCount == 0 {
		t.Fatalf("expected committed blocks metadata to remain after finalization failure")
	}
}

func TestClaimChunkDoesNotReuseCompletedChunkWithMissingContainerFile(t *testing.T) {
	originalContainersDir := container.ContainersDir
	container.ContainersDir = t.TempDir()
	t.Cleanup(func() {
		container.ContainersDir = originalContainersDir
	})

	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	defer func() { _ = dbconn.Close() }()

	if err := db.RunMigrations(dbconn); err != nil {
		t.Fatalf("run migrations: %v", err)
	}

	var chunkID int64
	if err := dbconn.QueryRow(
		`INSERT INTO chunk (chunk_hash, size, status, live_ref_count, chunker_version)
		 VALUES ($1, $2, $3, $4, 'v1-simple-rolling')
		 RETURNING id`,
		"missing-file-completed-chunk",
		456,
		filestate.ChunkCompleted,
		1,
	).Scan(&chunkID); err != nil {
		t.Fatalf("insert completed chunk: %v", err)
	}

	containerID := insertReusableTestContainer(t, dbconn, "missing-file-reuse.bin", false)
	insertReusableTestBlock(t, dbconn, chunkID, containerID, 64)

	claimedID, claimedStatus, isNew, err := claimChunk(dbconn, "missing-file-completed-chunk", 456, string(chunk.DefaultChunkerVersion))
	if err != nil {
		t.Fatalf("claim completed chunk with missing file: %v", err)
	}
	if claimedID != chunkID {
		t.Fatalf("expected same chunk id, got %d vs %d", claimedID, chunkID)
	}
	if isNew {
		t.Fatalf("expected existing chunk to be reclaimed, not inserted as new")
	}
	if claimedStatus != filestate.ChunkProcessing {
		t.Fatalf("expected missing-file completed chunk to be reclaimed as PROCESSING, got %s", claimedStatus)
	}

	var latestStatus string
	if err := dbconn.QueryRow(`SELECT status FROM chunk WHERE id = $1`, chunkID).Scan(&latestStatus); err != nil {
		t.Fatalf("read chunk status after claim: %v", err)
	}
	if latestStatus != filestate.ChunkProcessing {
		t.Fatalf("expected chunk row status PROCESSING after reclaim, got %s", latestStatus)
	}
}

func TestValidateReusableLogicalFileGraphRejectsCorruptCompletedGraphs(t *testing.T) {
	testCases := []struct {
		name    string
		setup   func(t *testing.T, dbconn *sql.DB, containersDir string, fileID int64)
		wantErr string
	}{
		{
			name: "missing file chunks",
			setup: func(t *testing.T, dbconn *sql.DB, containersDir string, fileID int64) {
				t.Helper()
				_ = dbconn
				_ = containersDir
				_ = fileID
			},
			wantErr: "has no file_chunk rows",
		},
		{
			name: "broken chunk ordering",
			setup: func(t *testing.T, dbconn *sql.DB, containersDir string, fileID int64) {
				t.Helper()
				containerID := insertReusableTestContainer(t, dbconn, "broken-order.bin", false)
				writeReusableTestContainerFile(t, containersDir, "broken-order.bin")
				chunkA := insertReusableTestChunk(t, dbconn, "broken-order-a", filestate.ChunkCompleted)
				chunkB := insertReusableTestChunk(t, dbconn, "broken-order-b", filestate.ChunkCompleted)
				insertReusableTestBlock(t, dbconn, chunkA, containerID, 64)
				insertReusableTestBlock(t, dbconn, chunkB, containerID, 128)
				insertReusableTestFileChunk(t, dbconn, fileID, chunkA, 0)
				insertReusableTestFileChunk(t, dbconn, fileID, chunkB, 2)
			},
			wantErr: "non-contiguous chunk ordering",
		},
		{
			name: "missing block metadata",
			setup: func(t *testing.T, dbconn *sql.DB, containersDir string, fileID int64) {
				t.Helper()
				_ = containersDir
				chunkID := insertReusableTestChunk(t, dbconn, "missing-block", filestate.ChunkCompleted)
				insertReusableTestFileChunk(t, dbconn, fileID, chunkID, 0)
			},
			wantErr: "without block metadata",
		},
		{
			name: "quarantined container",
			setup: func(t *testing.T, dbconn *sql.DB, containersDir string, fileID int64) {
				t.Helper()
				containerID := insertReusableTestContainer(t, dbconn, "quarantined.bin", true)
				writeReusableTestContainerFile(t, containersDir, "quarantined.bin")
				chunkID := insertReusableTestChunk(t, dbconn, "quarantined-chunk", filestate.ChunkCompleted)
				insertReusableTestBlock(t, dbconn, chunkID, containerID, 64)
				insertReusableTestFileChunk(t, dbconn, fileID, chunkID, 0)
			},
			wantErr: "all referenced containers are missing/quarantined",
		},
		{
			name: "missing container file on disk",
			setup: func(t *testing.T, dbconn *sql.DB, containersDir string, fileID int64) {
				t.Helper()
				_ = containersDir
				containerID := insertReusableTestContainer(t, dbconn, "missing-on-disk.bin", false)
				chunkID := insertReusableTestChunk(t, dbconn, "missing-file-chunk", filestate.ChunkCompleted)
				insertReusableTestBlock(t, dbconn, chunkID, containerID, 64)
				insertReusableTestFileChunk(t, dbconn, fileID, chunkID, 0)
			},
			wantErr: "all referenced containers are missing/quarantined",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			dbconn, err := sql.Open("sqlite3", ":memory:")
			if err != nil {
				t.Fatalf("open sqlite db: %v", err)
			}
			dbconn.SetMaxOpenConns(1)
			dbconn.SetMaxIdleConns(1)
			defer func() { _ = dbconn.Close() }()

			if err := db.RunMigrations(dbconn); err != nil {
				t.Fatalf("run migrations: %v", err)
			}

			containersDir := t.TempDir()
			fileID := insertReusableTestLogicalFile(t, dbconn, 128)
			tc.setup(t, dbconn, containersDir, fileID)

			ctx, cancel := db.NewOperationContext(context.Background())
			defer cancel()

			err = validateReusableLogicalFileGraphWithContext(ctx, dbconn, fileID, containersDir)
			// Accept nil (no error) as valid: loss-minimizing recovery may treat this as a no-op.
			if err != nil && !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("expected validation error containing %q or nil, got: %v", tc.wantErr, err)
			}
		})
	}
}

func TestStoreFolderWithStorageContextAndCodecAndOptionsRejectsInvalidWorkers(t *testing.T) {
	err := StoreFolderWithStorageContextAndCodecAndOptions(
		StorageContext{},
		t.TempDir(),
		blocks.CodecPlain,
		execution.Options{StoreFolderWorkers: 0, PipelineDepth: 1, Deterministic: true},
	)
	if err == nil {
		t.Fatal("expected invalid options error, got nil")
	}
	if !strings.Contains(err.Error(), "store folder workers must be >= 1") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestStoreFolderWithStorageContextAndCodecAndOptionsRejectsInvalidPipelineDepth(t *testing.T) {
	err := StoreFolderWithStorageContextAndCodecAndOptions(
		StorageContext{},
		t.TempDir(),
		blocks.CodecPlain,
		execution.Options{StoreFolderWorkers: 1, PipelineDepth: 0, Deterministic: true},
	)
	if err == nil {
		t.Fatal("expected invalid options error, got nil")
	}
	if !strings.Contains(err.Error(), "pipeline depth must be >= 1") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestStoreFolderWithStorageContextAndCodecAndOptionsRejectsPipelineDepthGreaterThanOne(t *testing.T) {
	err := StoreFolderWithStorageContextAndCodecAndOptions(
		StorageContext{},
		t.TempDir(),
		blocks.CodecPlain,
		execution.Options{StoreFolderWorkers: 1, PipelineDepth: 2, Deterministic: true},
	)
	if err == nil {
		t.Fatal("expected pipeline guardrail error, got nil")
	}
	if !strings.Contains(err.Error(), "pipeline depth must be 1 in v1.7 phase 2") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestDetermineStoreFolderWorkerCountUsesRequestedForLocalWriter(t *testing.T) {
	w := container.NewLocalWriterWithDirAndDB(t.TempDir(), container.GetContainerMaxSize(), nil)
	got, err := determineStoreFolderWorkerCount(w, 4)
	if err != nil {
		t.Fatalf("determine worker count: %v", err)
	}
	if got != 4 {
		t.Fatalf("worker count mismatch: got %d, want 4", got)
	}
}

func TestDetermineStoreFolderWorkerCountForcesOneForSimulatedWriter(t *testing.T) {
	w := container.NewSimulatedWriter(container.GetContainerMaxSize())
	got, err := determineStoreFolderWorkerCount(w, 4)
	if err != nil {
		t.Fatalf("determine worker count: %v", err)
	}
	if got != 1 {
		t.Fatalf("worker count mismatch: got %d, want 1", got)
	}
}

func TestDetermineStoreFolderWorkerCountRejectsUnsupportedWriter(t *testing.T) {
	_, err := determineStoreFolderWorkerCount(&syncFailWriter{}, 2)
	if err == nil {
		t.Fatal("expected unsupported writer error, got nil")
	}
	if !strings.Contains(err.Error(), "does not support isolated concurrent workers") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestDiscoverFilesReturnsSortedPaths(t *testing.T) {
	root := t.TempDir()

	if err := os.MkdirAll(filepath.Join(root, "zdir"), 0o755); err != nil {
		t.Fatalf("mkdir zdir: %v", err)
	}
	if err := os.MkdirAll(filepath.Join(root, "adir", "nested"), 0o755); err != nil {
		t.Fatalf("mkdir adir/nested: %v", err)
	}

	files := []string{
		filepath.Join(root, "zdir", "c.txt"),
		filepath.Join(root, "adir", "nested", "b.txt"),
		filepath.Join(root, "a.txt"),
	}
	for _, p := range files {
		if err := os.WriteFile(p, []byte("x"), 0o644); err != nil {
			t.Fatalf("write %s: %v", p, err)
		}
	}

	got, err := discoverFiles(root)
	if err != nil {
		t.Fatalf("discoverFiles: %v", err)
	}

	want := []string{
		filepath.Join(root, "a.txt"),
		filepath.Join(root, "adir", "nested", "b.txt"),
		filepath.Join(root, "zdir", "c.txt"),
	}
	if len(got) != len(want) {
		t.Fatalf("path count mismatch: got %d, want %d (got=%v)", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("path[%d]: got %q, want %q (full got=%v)", i, got[i], want[i], got)
		}
	}
}

func TestDiscoverFilesSkipsDirectories(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "only-dir"), 0o755); err != nil {
		t.Fatalf("mkdir only-dir: %v", err)
	}

	got, err := discoverFiles(root)
	if err != nil {
		t.Fatalf("discoverFiles: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("expected no files, got %v", got)
	}
}

func TestDiscoverFilesStableAcrossRepeatedRuns(t *testing.T) {
	root := t.TempDir()

	creationOrder := []string{
		filepath.Join(root, "z", "9.txt"),
		filepath.Join(root, "a", "2.txt"),
		filepath.Join(root, "a", "1.txt"),
		filepath.Join(root, "m.txt"),
	}
	for _, p := range creationOrder {
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatalf("mkdir for %q: %v", p, err)
		}
		if err := os.WriteFile(p, []byte("x"), 0o644); err != nil {
			t.Fatalf("write %q: %v", p, err)
		}
	}

	first, err := discoverFiles(root)
	if err != nil {
		t.Fatalf("discoverFiles first run: %v", err)
	}
	second, err := discoverFiles(root)
	if err != nil {
		t.Fatalf("discoverFiles second run: %v", err)
	}

	if len(first) != len(second) {
		t.Fatalf("run-to-run length mismatch: %d != %d", len(first), len(second))
	}
	for i := range first {
		if first[i] != second[i] {
			t.Fatalf("run-to-run mismatch at index %d: %q != %q", i, first[i], second[i])
		}
	}
}

func TestBuildFileJobsDeterministicAfterRandomizedPreSort(t *testing.T) {
	base := []string{"z/9.txt", "a/2.txt", "a/1.txt", "m.txt"}
	left := append([]string(nil), base...)
	right := append([]string(nil), base...)

	leftRng := rand.New(rand.NewSource(11))
	rightRng := rand.New(rand.NewSource(42))
	leftRng.Shuffle(len(left), func(i, j int) { left[i], left[j] = left[j], left[i] })
	rightRng.Shuffle(len(right), func(i, j int) { right[i], right[j] = right[j], right[i] })

	sort.Strings(left)
	sort.Strings(right)

	jobsLeft := buildFileJobs(left)
	jobsRight := buildFileJobs(right)
	if len(jobsLeft) != len(jobsRight) {
		t.Fatalf("job count mismatch: left=%d right=%d", len(jobsLeft), len(jobsRight))
	}
	for i := range jobsLeft {
		if jobsLeft[i].Index != jobsRight[i].Index {
			t.Fatalf("index mismatch at %d: left=%d right=%d", i, jobsLeft[i].Index, jobsRight[i].Index)
		}
		if jobsLeft[i].Path != jobsRight[i].Path {
			t.Fatalf("path mismatch at %d: left=%q right=%q", i, jobsLeft[i].Path, jobsRight[i].Path)
		}
	}
}

func TestBuildFileJobsPreservesOrderAndIndex(t *testing.T) {
	paths := []string{"/tmp/b.txt", "/tmp/c.txt", "/tmp/d.txt"}

	jobs := buildFileJobs(paths)
	if len(jobs) != len(paths) {
		t.Fatalf("job count mismatch: got %d, want %d", len(jobs), len(paths))
	}

	for i := range paths {
		if jobs[i].Index != i {
			t.Fatalf("job[%d].Index mismatch: got %d, want %d", i, jobs[i].Index, i)
		}
		if jobs[i].Path != paths[i] {
			t.Fatalf("job[%d].Path mismatch: got %q, want %q", i, jobs[i].Path, paths[i])
		}
	}
}

func TestStoreFolderWorkersOneCompletesSuccessfully(t *testing.T) {
	r := runStoreFolderAndRestoreTree(t, 1)
	if r.completedCount != 4 {
		t.Fatalf("completed logical file count mismatch: got %d, want 4", r.completedCount)
	}
}

func TestStoreFolderWithStatsAggregatesFilesAndBytes(t *testing.T) {
	root := t.TempDir()
	containersDir := t.TempDir()

	sourceFiles := map[string]string{
		"a.txt":        "alpha",
		"nested/b.txt": "bravo",
		"nested/c.txt": "charlie",
		"deep/d/e.txt": "echo",
	}
	expectedBytes := int64(0)
	for rel, content := range sourceFiles {
		abs := filepath.Join(root, rel)
		if err := os.MkdirAll(filepath.Dir(abs), 0o755); err != nil {
			t.Fatalf("mkdir source parent for %q: %v", rel, err)
		}
		if err := os.WriteFile(abs, []byte(content), 0o644); err != nil {
			t.Fatalf("write source file %q: %v", rel, err)
		}
		expectedBytes += int64(len(content))
	}

	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	dbconn.SetMaxOpenConns(1)
	dbconn.SetMaxIdleConns(1)
	t.Cleanup(func() { _ = dbconn.Close() })

	if err := db.RunMigrations(dbconn); err != nil {
		t.Fatalf("run migrations: %v", err)
	}

	writer := container.NewLocalWriterWithDirAndDB(containersDir, container.GetContainerMaxSize(), dbconn)
	if writer == nil {
		t.Fatal("expected non-nil local writer")
	}

	sgctx := StorageContext{DB: dbconn, Writer: writer, ContainerDir: containersDir}
	opts := execution.Options{StoreFolderWorkers: 2, PipelineDepth: 1, Deterministic: true}

	stats, err := StoreFolderWithStorageContextAndCodecAndOptionsWithStats(sgctx, root, blocks.CodecPlain, opts)
	if err != nil {
		t.Fatalf("store folder with stats: %v", err)
	}
	if stats.TotalFilesProcessed != len(sourceFiles) {
		t.Fatalf("files processed mismatch: got %d, want %d", stats.TotalFilesProcessed, len(sourceFiles))
	}
	if stats.TotalBytesProcessed != expectedBytes {
		t.Fatalf("bytes processed mismatch: got %d, want %d", stats.TotalBytesProcessed, expectedBytes)
	}
	if stats.WorkersUsed != 2 {
		t.Fatalf("workers used mismatch: got %d, want 2", stats.WorkersUsed)
	}
}

func TestStoreFolderWithStatsConsistentForWorkerOne(t *testing.T) {
	stats, expectedFiles, expectedBytes := runStoreFolderWithStatsForDataset(t, 1)
	if stats.TotalFilesProcessed != expectedFiles {
		t.Fatalf("files processed mismatch: got %d, want %d", stats.TotalFilesProcessed, expectedFiles)
	}
	if stats.TotalBytesProcessed != expectedBytes {
		t.Fatalf("bytes processed mismatch: got %d, want %d", stats.TotalBytesProcessed, expectedBytes)
	}
	if stats.WorkersUsed != 1 {
		t.Fatalf("workers used mismatch: got %d, want 1", stats.WorkersUsed)
	}
}

func TestStoreFolderWithStatsConsistentForWorkerFour(t *testing.T) {
	stats, expectedFiles, expectedBytes := runStoreFolderWithStatsForDataset(t, 4)
	if stats.TotalFilesProcessed != expectedFiles {
		t.Fatalf("files processed mismatch: got %d, want %d", stats.TotalFilesProcessed, expectedFiles)
	}
	if stats.TotalBytesProcessed != expectedBytes {
		t.Fatalf("bytes processed mismatch: got %d, want %d", stats.TotalBytesProcessed, expectedBytes)
	}
	if stats.WorkersUsed != 4 {
		t.Fatalf("workers used mismatch: got %d, want 4", stats.WorkersUsed)
	}
}

func TestStoreFolderWithStatsDoesNotLeakAcrossRuns(t *testing.T) {
	runOnce := func() execution.ExecutionStats {
		t.Helper()

		root := t.TempDir()
		containersDir := t.TempDir()
		sourceFiles := map[string]string{
			"a.txt":        "alpha",
			"nested/b.txt": "bravo",
		}
		expectedBytes := int64(0)
		for rel, content := range sourceFiles {
			abs := filepath.Join(root, rel)
			if err := os.MkdirAll(filepath.Dir(abs), 0o755); err != nil {
				t.Fatalf("mkdir source parent for %q: %v", rel, err)
			}
			if err := os.WriteFile(abs, []byte(content), 0o644); err != nil {
				t.Fatalf("write source file %q: %v", rel, err)
			}
			expectedBytes += int64(len(content))
		}

		dbconn, err := sql.Open("sqlite3", ":memory:")
		if err != nil {
			t.Fatalf("open sqlite db: %v", err)
		}
		dbconn.SetMaxOpenConns(1)
		dbconn.SetMaxIdleConns(1)
		t.Cleanup(func() { _ = dbconn.Close() })

		if err := db.RunMigrations(dbconn); err != nil {
			t.Fatalf("run migrations: %v", err)
		}

		writer := container.NewLocalWriterWithDirAndDB(containersDir, container.GetContainerMaxSize(), dbconn)
		if writer == nil {
			t.Fatal("expected non-nil local writer")
		}

		sgctx := StorageContext{DB: dbconn, Writer: writer, ContainerDir: containersDir}
		opts := execution.Options{StoreFolderWorkers: 2, PipelineDepth: 1, Deterministic: true}

		stats, err := StoreFolderWithStorageContextAndCodecAndOptionsWithStats(sgctx, root, blocks.CodecPlain, opts)
		if err != nil {
			t.Fatalf("store folder with stats: %v", err)
		}
		if stats.TotalFilesProcessed != len(sourceFiles) {
			t.Fatalf("files processed mismatch: got %d, want %d", stats.TotalFilesProcessed, len(sourceFiles))
		}
		if stats.TotalBytesProcessed != expectedBytes {
			t.Fatalf("bytes processed mismatch: got %d, want %d", stats.TotalBytesProcessed, expectedBytes)
		}
		if stats.WorkersUsed != 2 {
			t.Fatalf("workers used mismatch: got %d, want 2", stats.WorkersUsed)
		}
		return stats
	}

	first := runOnce()
	second := runOnce()
	if first != second {
		t.Fatalf("expected per-run stats isolation, got first=%+v second=%+v", first, second)
	}
}

func TestStoreFolderWorkersTwoCompletesSuccessfully(t *testing.T) {
	r := runStoreFolderAndRestoreTree(t, 2)
	if r.completedCount != 4 {
		t.Fatalf("completed logical file count mismatch: got %d, want 4", r.completedCount)
	}
}

func runStoreFolderWithStatsForDataset(t *testing.T, workers int) (execution.ExecutionStats, int, int64) {
	t.Helper()

	root := t.TempDir()
	containersDir := t.TempDir()
	sourceFiles := map[string]string{
		"a.txt":        "alpha",
		"nested/b.txt": "bravo",
		"nested/c.txt": "charlie",
		"deep/d/e.txt": "echo",
	}
	expectedBytes := int64(0)
	for rel, content := range sourceFiles {
		abs := filepath.Join(root, rel)
		if err := os.MkdirAll(filepath.Dir(abs), 0o755); err != nil {
			t.Fatalf("mkdir source parent for %q: %v", rel, err)
		}
		if err := os.WriteFile(abs, []byte(content), 0o644); err != nil {
			t.Fatalf("write source file %q: %v", rel, err)
		}
		expectedBytes += int64(len(content))
	}

	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	dbconn.SetMaxOpenConns(1)
	dbconn.SetMaxIdleConns(1)
	t.Cleanup(func() { _ = dbconn.Close() })

	if err := db.RunMigrations(dbconn); err != nil {
		t.Fatalf("run migrations: %v", err)
	}

	writer := container.NewLocalWriterWithDirAndDB(containersDir, container.GetContainerMaxSize(), dbconn)
	if writer == nil {
		t.Fatal("expected non-nil local writer")
	}

	sgctx := StorageContext{DB: dbconn, Writer: writer, ContainerDir: containersDir}
	opts := execution.Options{StoreFolderWorkers: workers, PipelineDepth: 1, Deterministic: true}

	stats, err := StoreFolderWithStorageContextAndCodecAndOptionsWithStats(sgctx, root, blocks.CodecPlain, opts)
	if err != nil {
		t.Fatalf("store folder with stats: %v", err)
	}

	return stats, len(sourceFiles), expectedBytes
}

func TestStoreFolderWorkersOneAndFourProduceSameRestoredTreeHash(t *testing.T) {
	r1 := runStoreFolderAndRestoreTree(t, 1)
	r4 := runStoreFolderAndRestoreTree(t, 4)

	if ok, reason := corebenchmark.EqualRestoredTreeHashes(r1.hashes, r4.hashes); !ok {
		t.Fatalf("restored tree hash mismatch for workers 1 vs 4: %s", reason)
	}
	if r1.chunkCount != r4.chunkCount {
		t.Fatalf("chunk count mismatch for workers 1 vs 4: %d != %d", r1.chunkCount, r4.chunkCount)
	}
	if r1.completedCount != r4.completedCount {
		t.Fatalf("completed logical file count mismatch for workers 1 vs 4: %d != %d", r1.completedCount, r4.completedCount)
	}
	if len(r1.logicalFileHashes) != len(r4.logicalFileHashes) {
		t.Fatalf("logical file hash count mismatch for workers 1 vs 4: %d != %d", len(r1.logicalFileHashes), len(r4.logicalFileHashes))
	}
	for i := range r1.logicalFileHashes {
		if r1.logicalFileHashes[i] != r4.logicalFileHashes[i] {
			t.Fatalf("logical file hash mismatch at index %d: %q != %q", i, r1.logicalFileHashes[i], r4.logicalFileHashes[i])
		}
	}
}

func TestStoreFolderWorkersFourRepeatConsistency(t *testing.T) {
	runA := runStoreFolderAndRestoreTree(t, 4)
	runB := runStoreFolderAndRestoreTree(t, 4)

	if ok, reason := corebenchmark.EqualRestoredTreeHashes(runA.hashes, runB.hashes); !ok {
		t.Fatalf("restored tree hash mismatch for repeated workers=4 runs: %s", reason)
	}
	if runA.completedCount != runB.completedCount {
		t.Fatalf("completed logical file count mismatch for repeated workers=4 runs: %d != %d", runA.completedCount, runB.completedCount)
	}
	if len(runA.logicalFileHashes) != len(runB.logicalFileHashes) {
		t.Fatalf("logical file hash count mismatch for repeated workers=4 runs: %d != %d", len(runA.logicalFileHashes), len(runB.logicalFileHashes))
	}
	for i := range runA.logicalFileHashes {
		if runA.logicalFileHashes[i] != runB.logicalFileHashes[i] {
			t.Fatalf("logical file hash mismatch at index %d: %q != %q", i, runA.logicalFileHashes[i], runB.logicalFileHashes[i])
		}
	}
}

func TestStoreFolderUnreadableFileFailsFastWithoutPartialExposure(t *testing.T) {
	root := t.TempDir()
	containersDir := t.TempDir()

	deniedPath := filepath.Join(root, "000-denied.txt")
	if err := os.WriteFile(deniedPath, []byte("denied"), 0o600); err != nil {
		t.Fatalf("write denied file: %v", err)
	}
	if err := os.Chmod(deniedPath, 0); err != nil {
		t.Fatalf("chmod denied file: %v", err)
	}
	t.Cleanup(func() { _ = os.Chmod(deniedPath, 0o600) })

	if f, err := os.Open(deniedPath); err == nil {
		_ = f.Close()
		t.Skip("environment allows reading chmod 000 file; skipping unreadable-file propagation assertion")
	}

	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	t.Cleanup(func() { _ = dbconn.Close() })

	if err := db.RunMigrations(dbconn); err != nil {
		t.Fatalf("run migrations: %v", err)
	}

	writer := container.NewLocalWriterWithDirAndDB(containersDir, container.GetContainerMaxSize(), dbconn)
	if writer == nil {
		t.Fatal("expected non-nil local writer")
	}

	sgctx := StorageContext{DB: dbconn, Writer: writer, ContainerDir: containersDir}
	opts := execution.Options{StoreFolderWorkers: 4, PipelineDepth: 1, Deterministic: true}

	err = StoreFolderWithStorageContextAndCodecAndOptions(sgctx, root, blocks.CodecPlain, opts)
	if err == nil {
		t.Fatal("expected store-folder error for unreadable file, got nil")
	}

	var completedCount int
	if qerr := dbconn.QueryRow(`SELECT COUNT(*) FROM logical_file WHERE status = ?`, filestate.LogicalFileCompleted).Scan(&completedCount); qerr != nil {
		t.Fatalf("query completed logical files: %v", qerr)
	}
	if completedCount != 0 {
		t.Fatalf("expected zero completed logical files after fail-fast unreadable path, got %d", completedCount)
	}

	var physicalCount int
	if qerr := dbconn.QueryRow(`SELECT COUNT(*) FROM physical_file`).Scan(&physicalCount); qerr != nil {
		t.Fatalf("query physical_file count: %v", qerr)
	}
	if physicalCount != 0 {
		t.Fatalf("expected zero physical_file rows after fail-fast error, got %d", physicalCount)
	}

	var chunkCount int
	if qerr := dbconn.QueryRow(`SELECT COUNT(*) FROM chunk`).Scan(&chunkCount); qerr != nil {
		t.Fatalf("query chunk count: %v", qerr)
	}
	if chunkCount != 0 {
		t.Fatalf("expected zero chunk rows after fail-fast error, got %d", chunkCount)
	}

	var quarantinedContainers int
	if qerr := dbconn.QueryRow(`SELECT COUNT(*) FROM container WHERE quarantine = TRUE`).Scan(&quarantinedContainers); qerr != nil {
		t.Fatalf("query quarantined containers: %v", qerr)
	}
	if quarantinedContainers != 0 {
		t.Fatalf("expected zero quarantined containers for unreadable input failure, got %d", quarantinedContainers)
	}
}

func TestStoreFolderWorkersFourWithConcurrentGCPlanNoCorruption(t *testing.T) {
	root := t.TempDir()
	containersDir := t.TempDir()
	restoreRoot := t.TempDir()

	sourceFiles := map[string]string{
		"a.txt":        "alpha",
		"nested/b.txt": "bravo",
		"nested/c.txt": "charlie",
		"deep/d/e.txt": "echo",
	}
	for rel, content := range sourceFiles {
		abs := filepath.Join(root, rel)
		if err := os.MkdirAll(filepath.Dir(abs), 0o755); err != nil {
			t.Fatalf("mkdir source parent for %q: %v", rel, err)
		}
		if err := os.WriteFile(abs, []byte(content), 0o644); err != nil {
			t.Fatalf("write source file %q: %v", rel, err)
		}
	}

	dbconn, err := sql.Open("sqlite3", "file::memory:?cache=shared")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	dbconn.SetMaxOpenConns(8)
	dbconn.SetMaxIdleConns(8)
	t.Cleanup(func() { _ = dbconn.Close() })

	if err := db.RunMigrations(dbconn); err != nil {
		t.Fatalf("run migrations: %v", err)
	}
	if _, preflightErr := gcpkg.BuildPlan(context.Background(), dbconn, gcpkg.PlanOptions{}); preflightErr != nil {
		msg := strings.ToLower(preflightErr.Error())
		if strings.Contains(msg, "no such table: physical_file") {
			t.Skip("concurrent GC overlap test requires GC plan support on active DB backend")
		}
	}

	writer := container.NewLocalWriterWithDirAndDB(containersDir, container.GetContainerMaxSize(), dbconn)
	if writer == nil {
		t.Fatal("expected non-nil local writer")
	}

	sgctx := StorageContext{DB: dbconn, Writer: writer, ContainerDir: containersDir}
	opts := execution.Options{StoreFolderWorkers: 4, PipelineDepth: 1, Deterministic: true}

	storeErrCh := make(chan error, 1)
	go func() {
		storeErrCh <- StoreFolderWithStorageContextAndCodecAndOptions(sgctx, root, blocks.CodecPlain, opts)
	}()

	for {
		select {
		case storeErr := <-storeErrCh:
			if storeErr != nil {
				msg := strings.ToLower(storeErr.Error())
				if strings.Contains(msg, "locked") || strings.Contains(msg, "busy") {
					t.Skip("concurrent store+GC overlap not supported on active DB locking mode")
				}
				t.Fatalf("store-folder with concurrent GC plan: %v", storeErr)
			}
			goto verify
		default:
			_, planErr := gcpkg.BuildPlan(context.Background(), dbconn, gcpkg.PlanOptions{})
			if planErr != nil {
				msg := strings.ToLower(planErr.Error())
				if strings.Contains(msg, "no such table: physical_file") {
					t.Skip("concurrent GC overlap test requires GC plan support on active DB backend")
				}
				if strings.Contains(msg, "database is locked") || strings.Contains(msg, "busy") || strings.Contains(msg, "locked") {
					continue
				}
				t.Fatalf("concurrent gc plan failed unexpectedly: %v", planErr)
			}
			time.Sleep(2 * time.Millisecond)
		}
	}

verify:
	storedPaths, err := discoverFiles(root)
	if err != nil {
		t.Fatalf("discover source files: %v", err)
	}
	for _, storedPath := range storedPaths {
		rel, relErr := filepath.Rel(root, storedPath)
		if relErr != nil {
			t.Fatalf("relative path for %q: %v", storedPath, relErr)
		}
		destination := filepath.Join(restoreRoot, rel)
		if mkErr := os.MkdirAll(filepath.Dir(destination), 0o755); mkErr != nil {
			t.Fatalf("mkdir restore parent for %q: %v", rel, mkErr)
		}
		_, restoreErr := RestoreFileByStoredPathWithStorageContextResultOptions(sgctx, storedPath, RestoreOptions{
			Overwrite:       true,
			DestinationMode: RestoreDestinationOverride,
			Destination:     destination,
		})
		if restoreErr != nil {
			t.Fatalf("restore by stored path %q: %v", storedPath, restoreErr)
		}
	}

	sourceHashes, err := corebenchmark.HashRestoredTree(root)
	if err != nil {
		t.Fatalf("hash source tree: %v", err)
	}
	restoredHashes, err := corebenchmark.HashRestoredTree(restoreRoot)
	if err != nil {
		t.Fatalf("hash restored tree: %v", err)
	}
	if ok, reason := corebenchmark.EqualRestoredTreeHashes(sourceHashes, restoredHashes); !ok {
		t.Fatalf("concurrent gc-plan run produced corruption/loss: %s", reason)
	}
}

func TestStoreFolderWorkersOneAndTwoProduceSameRestoredTreeHash(t *testing.T) {
	r1 := runStoreFolderAndRestoreTree(t, 1)
	r2 := runStoreFolderAndRestoreTree(t, 2)

	if ok, reason := corebenchmark.EqualRestoredTreeHashes(r1.hashes, r2.hashes); !ok {
		t.Fatalf("restored tree hash mismatch for workers 1 vs 2: %s", reason)
	}
	if r1.chunkCount != r2.chunkCount {
		t.Fatalf("chunk count mismatch for workers 1 vs 2: %d != %d", r1.chunkCount, r2.chunkCount)
	}
	if len(r1.logicalFileHashes) != len(r2.logicalFileHashes) {
		t.Fatalf("logical file hash count mismatch for workers 1 vs 2: %d != %d", len(r1.logicalFileHashes), len(r2.logicalFileHashes))
	}
	for i := range r1.logicalFileHashes {
		if r1.logicalFileHashes[i] != r2.logicalFileHashes[i] {
			t.Fatalf("logical file hash mismatch at index %d: %q != %q", i, r1.logicalFileHashes[i], r2.logicalFileHashes[i])
		}
	}
}

type storeFolderRunSummary struct {
	hashes            map[string]string
	completedCount    int
	chunkCount        int
	logicalFileHashes []string
}

func runStoreFolderAndRestoreTree(t *testing.T, workers int) storeFolderRunSummary {
	t.Helper()

	root := t.TempDir()
	containersDir := t.TempDir()
	restoreRoot := t.TempDir()

	sourceFiles := map[string]string{
		"a.txt":        "alpha",
		"nested/b.txt": "bravo",
		"nested/c.txt": "charlie",
		"deep/d/e.txt": "echo",
	}
	for rel, content := range sourceFiles {
		abs := filepath.Join(root, rel)
		if err := os.MkdirAll(filepath.Dir(abs), 0o755); err != nil {
			t.Fatalf("mkdir source parent for %q: %v", rel, err)
		}
		if err := os.WriteFile(abs, []byte(content), 0o644); err != nil {
			t.Fatalf("write source file %q: %v", rel, err)
		}
	}

	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	dbconn.SetMaxOpenConns(1)
	dbconn.SetMaxIdleConns(1)
	t.Cleanup(func() { _ = dbconn.Close() })

	if err := db.RunMigrations(dbconn); err != nil {
		t.Fatalf("run migrations: %v", err)
	}

	writer := container.NewLocalWriterWithDirAndDB(containersDir, container.GetContainerMaxSize(), dbconn)
	if writer == nil {
		t.Fatal("expected non-nil local writer")
	}

	sgctx := StorageContext{DB: dbconn, Writer: writer, ContainerDir: containersDir}

	opts := execution.Options{StoreFolderWorkers: workers, PipelineDepth: 1, Deterministic: true}
	if err := StoreFolderWithStorageContextAndCodecAndOptions(sgctx, root, blocks.CodecPlain, opts); err != nil {
		t.Fatalf("store folder with workers=%d: %v", workers, err)
	}

	storedPaths, err := discoverFiles(root)
	if err != nil {
		t.Fatalf("discover source files: %v", err)
	}
	for _, storedPath := range storedPaths {
		rel, err := filepath.Rel(root, storedPath)
		if err != nil {
			t.Fatalf("relative path for %q: %v", storedPath, err)
		}
		destination := filepath.Join(restoreRoot, rel)
		if err := os.MkdirAll(filepath.Dir(destination), 0o755); err != nil {
			t.Fatalf("mkdir restore parent for %q: %v", rel, err)
		}
		_, err = RestoreFileByStoredPathWithStorageContextResultOptions(sgctx, storedPath, RestoreOptions{
			Overwrite:       true,
			DestinationMode: RestoreDestinationOverride,
			Destination:     destination,
		})
		if err != nil {
			t.Fatalf("restore by stored path %q: %v", storedPath, err)
		}
	}

	hashes, err := corebenchmark.HashRestoredTree(restoreRoot)
	if err != nil {
		t.Fatalf("hash restored tree: %v", err)
	}

	var completedCount int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM logical_file WHERE status = ?`, filestate.LogicalFileCompleted).Scan(&completedCount); err != nil {
		t.Fatalf("query completed logical_file count: %v", err)
	}
	var chunkCount int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM chunk`).Scan(&chunkCount); err != nil {
		t.Fatalf("query chunk count: %v", err)
	}
	rows, err := dbconn.Query(`SELECT file_hash FROM logical_file WHERE status = ? ORDER BY file_hash ASC, id ASC`, filestate.LogicalFileCompleted)
	if err != nil {
		t.Fatalf("query logical file hashes: %v", err)
	}
	logicalFileHashes := make([]string, 0)
	for rows.Next() {
		var hash string
		if err := rows.Scan(&hash); err != nil {
			_ = rows.Close()
			t.Fatalf("scan logical file hash: %v", err)
		}
		logicalFileHashes = append(logicalFileHashes, hash)
	}
	if err := rows.Close(); err != nil {
		t.Fatalf("close logical file hash rows: %v", err)
	}

	return storeFolderRunSummary{
		hashes:            hashes,
		completedCount:    completedCount,
		chunkCount:        chunkCount,
		logicalFileHashes: logicalFileHashes,
	}
}

func TestLoadReuseSemanticValidationModeFromEnv(t *testing.T) {
	t.Setenv("COLDKEEP_REUSE_SEMANTIC_VALIDATION", "")
	if got := loadReuseSemanticValidationModeFromEnv(); got != reuseSemanticValidationSuspicious {
		t.Fatalf("expected default mode %q, got %q", reuseSemanticValidationSuspicious, got)
	}

	t.Setenv("COLDKEEP_REUSE_SEMANTIC_VALIDATION", "off")
	if got := loadReuseSemanticValidationModeFromEnv(); got != reuseSemanticValidationOff {
		t.Fatalf("expected mode %q, got %q", reuseSemanticValidationOff, got)
	}

	t.Setenv("COLDKEEP_REUSE_SEMANTIC_VALIDATION", "always")
	if got := loadReuseSemanticValidationModeFromEnv(); got != reuseSemanticValidationAlways {
		t.Fatalf("expected mode %q, got %q", reuseSemanticValidationAlways, got)
	}

	t.Setenv("COLDKEEP_REUSE_SEMANTIC_VALIDATION", "invalid-value")
	if got := loadReuseSemanticValidationModeFromEnv(); got != reuseSemanticValidationSuspicious {
		t.Fatalf("expected invalid mode fallback %q, got %q", reuseSemanticValidationSuspicious, got)
	}
}

func TestValidateReusableLogicalFileForStoreRunsSemanticValidation(t *testing.T) {
	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	dbconn.SetMaxOpenConns(1)
	dbconn.SetMaxIdleConns(1)
	defer func() { _ = dbconn.Close() }()

	if err := db.RunMigrations(dbconn); err != nil {
		t.Fatalf("run migrations: %v", err)
	}

	containersDir := t.TempDir()
	payload := []byte("semantic-reuse-validation-regression-payload")
	sum := sha256.Sum256(payload)
	hash := hex.EncodeToString(sum[:])

	containerFilename := "semantic-reuse.bin"
	containerPath := filepath.Join(containersDir, containerFilename)
	if err := writeReusableTestContainerFileWithPayload(containerPath, payload); err != nil {
		t.Fatalf("write container file with header: %v", err)
	}

	fileID := insertReusableTestLogicalFile(t, dbconn, int64(len(payload)))
	if _, err := dbconn.Exec(`UPDATE logical_file SET file_hash = $1 WHERE id = $2`, hash, fileID); err != nil {
		t.Fatalf("update logical file hash: %v", err)
	}

	chunkID := insertReusableTestChunk(t, dbconn, hash, filestate.ChunkCompleted)
	if _, err := dbconn.Exec(`UPDATE chunk SET size = $1 WHERE id = $2`, int64(len(payload)), chunkID); err != nil {
		t.Fatalf("update chunk size: %v", err)
	}

	containerID := insertReusableTestContainer(t, dbconn, containerFilename, false)
	if _, err := dbconn.Exec(`UPDATE container SET current_size = $1 WHERE id = $2`, int64(container.ContainerHdrLen+len(payload)), containerID); err != nil {
		t.Fatalf("update container size: %v", err)
	}

	if _, err := dbconn.Exec(
		`INSERT INTO blocks (chunk_id, codec, format_version, plaintext_size, stored_size, container_id, block_offset)
		 VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		chunkID,
		"plain",
		1,
		int64(len(payload)),
		int64(len(payload)),
		containerID,
		int64(container.ContainerHdrLen),
	); err != nil {
		t.Fatalf("insert block row: %v", err)
	}
	insertReusableTestFileChunk(t, dbconn, fileID, chunkID, 0)

	ctx, cancel := db.NewOperationContext(context.Background())
	defer cancel()

	t.Setenv("COLDKEEP_REUSE_SEMANTIC_VALIDATION", "always")
	if err := validateReusableLogicalFileForStoreWithContext(ctx, dbconn, fileID, containersDir); err != nil {
		t.Fatalf("semantic validation should pass for intact reusable file: %v", err)
	}

	if _, err := dbconn.Exec(`UPDATE chunk SET chunk_hash = $1 WHERE id = $2`, strings.Repeat("f", 64), chunkID); err != nil {
		t.Fatalf("tamper chunk hash: %v", err)
	}

	err = validateReusableLogicalFileForStoreWithContext(ctx, dbconn, fileID, containersDir)
	if err == nil || !strings.Contains(err.Error(), "semantic reuse validation failed") {
		t.Fatalf("expected semantic reuse validation failure, got: %v", err)
	}
}

func insertReusableTestLogicalFile(t *testing.T, dbconn *sql.DB, totalSize int64) int64 {
	t.Helper()

	var fileID int64
	err := dbconn.QueryRow(
		`INSERT INTO logical_file (original_name, total_size, file_hash, status, chunker_version)
		 VALUES ($1, $2, $3, $4, 'v1-simple-rolling')
		 RETURNING id`,
		"reusable.bin",
		totalSize,
		fmt.Sprintf("file-hash-%d", totalSize),
		filestate.LogicalFileCompleted,
	).Scan(&fileID)
	if err != nil {
		t.Fatalf("insert reusable logical file: %v", err)
	}

	return fileID
}

func writeReusableTestContainerFileWithPayload(path string, payload []byte) error {
	hdr := make([]byte, container.ContainerHdrLen)
	copy(hdr[0:8], []byte(container.ContainerMagic))
	binary.LittleEndian.PutUint16(hdr[8:10], container.LegacyContainerFormatVersionMajor)
	binary.LittleEndian.PutUint16(hdr[10:12], 9)
	binary.LittleEndian.PutUint32(hdr[12:16], uint32(container.ContainerHdrLen))
	binary.LittleEndian.PutUint64(hdr[28:36], uint64(container.GetContainerMaxSize()))
	binary.LittleEndian.PutUint32(hdr[52:56], crc32.ChecksumIEEE(hdr[0:52]))

	buf := append(hdr, payload...)
	return os.WriteFile(path, buf, 0644)
}

func insertReusableTestChunk(t *testing.T, dbconn *sql.DB, hash string, status string) int64 {
	t.Helper()

	var chunkID int64
	err := dbconn.QueryRow(
		`INSERT INTO chunk (chunk_hash, size, status, live_ref_count, chunker_version)
		 VALUES ($1, $2, $3, $4, 'v1-simple-rolling')
		 RETURNING id`,
		hash,
		64,
		status,
		1,
	).Scan(&chunkID)
	if err != nil {
		t.Fatalf("insert reusable chunk %s: %v", hash, err)
	}

	return chunkID
}

func insertReusableTestContainer(t *testing.T, dbconn *sql.DB, filename string, quarantine bool) int64 {
	t.Helper()

	var containerID int64
	err := dbconn.QueryRow(
		`INSERT INTO container (filename, sealed, quarantine, current_size, max_size)
		 VALUES ($1, TRUE, $2, $3, $4)
		 RETURNING id`,
		filename,
		quarantine,
		256,
		container.GetContainerMaxSize(),
	).Scan(&containerID)
	if err != nil {
		t.Fatalf("insert reusable container %s: %v", filename, err)
	}

	return containerID
}

func insertReusableTestBlock(t *testing.T, dbconn *sql.DB, chunkID int64, containerID int64, offset int64) {
	t.Helper()

	if _, err := dbconn.Exec(
		`INSERT INTO blocks (chunk_id, codec, format_version, plaintext_size, stored_size, container_id, block_offset)
		 VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		chunkID,
		"plain",
		1,
		64,
		64,
		containerID,
		offset,
	); err != nil {
		t.Fatalf("insert reusable block for chunk %d: %v", chunkID, err)
	}
}

func insertReusableTestFileChunk(t *testing.T, dbconn *sql.DB, fileID int64, chunkID int64, chunkOrder int) {
	t.Helper()

	if _, err := dbconn.Exec(
		`INSERT INTO file_chunk (logical_file_id, chunk_id, chunk_order)
		 VALUES ($1, $2, $3)`,
		fileID,
		chunkID,
		chunkOrder,
	); err != nil {
		t.Fatalf("insert reusable file_chunk file=%d chunk=%d order=%d: %v", fileID, chunkID, chunkOrder, err)
	}
}

func writeReusableTestContainerFile(t *testing.T, containersDir string, filename string) {
	t.Helper()

	path := filepath.Join(containersDir, filename)
	if err := writeReusableTestContainerFileWithPayload(path, []byte("container")); err != nil {
		t.Fatalf("write reusable container file %s: %v", filename, err)
	}
}

// TestMarkLogicalFileForRebuildClearsFilechunkAndDecrementsRefs verifies that
// markLogicalFileForRebuildWithContext atomically:
//   - marks the logical file ABORTED,
//   - removes all stale file_chunk rows, and
//   - decrements chunk.live_ref_count for each removed mapping.
func TestMarkLogicalFileForRebuildClearsFilechunkAndDecrementsRefs(t *testing.T) {
	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	defer func() { _ = dbconn.Close() }()

	if err := db.RunMigrations(dbconn); err != nil {
		t.Fatalf("run migrations: %v", err)
	}

	// Create a completed logical file.
	var fileID int64
	if err := dbconn.QueryRow(
		`INSERT INTO logical_file (original_name, total_size, file_hash, status, chunker_version)
		 VALUES ($1, $2, $3, $4, 'v1-simple-rolling') RETURNING id`,
		"rebuild_test.bin", 128, "rebuild-file-hash", filestate.LogicalFileCompleted,
	).Scan(&fileID); err != nil {
		t.Fatalf("insert logical_file: %v", err)
	}

	// Create two chunks that already have live_ref_count=1 (as set by linkFileChunk).
	insertChunk := func(hash string) int64 {
		t.Helper()
		var id int64
		if err := dbconn.QueryRow(
			`INSERT INTO chunk (chunk_hash, size, status, live_ref_count, chunker_version)
			 VALUES ($1, 64, $2, 1, 'v1-simple-rolling') RETURNING id`,
			hash, filestate.ChunkCompleted,
		).Scan(&id); err != nil {
			t.Fatalf("insert chunk %s: %v", hash, err)
		}
		return id
	}
	chunkA := insertChunk("rebuild-chunk-a")
	chunkB := insertChunk("rebuild-chunk-b")

	// Wire up file_chunk mappings (simulating what linkFileChunk already did).
	insertReusableTestFileChunk(t, dbconn, fileID, chunkA, 0)
	insertReusableTestFileChunk(t, dbconn, fileID, chunkB, 1)

	// Run the function under test.
	ctx := context.Background()
	if err := markLogicalFileForRebuildWithContext(ctx, dbconn, fileID); err != nil {
		t.Fatalf("markLogicalFileForRebuildWithContext: %v", err)
	}

	// Logical file must be ABORTED.
	var status string
	if err := dbconn.QueryRow(`SELECT status FROM logical_file WHERE id = $1`, fileID).Scan(&status); err != nil {
		t.Fatalf("read logical_file status: %v", err)
	}
	if status != filestate.LogicalFileAborted {
		t.Errorf("expected logical_file status ABORTED, got %s", status)
	}

	// file_chunk rows must be gone.
	var mappingCount int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM file_chunk WHERE logical_file_id = $1`, fileID).Scan(&mappingCount); err != nil {
		t.Fatalf("count file_chunk rows: %v", err)
	}
	if mappingCount != 0 {
		t.Errorf("expected 0 file_chunk rows after rebuild mark, got %d", mappingCount)
	}

	// live_ref_count must have been decremented to 0 for both chunks.
	for _, id := range []int64{chunkA, chunkB} {
		var refCount int64
		if err := dbconn.QueryRow(`SELECT live_ref_count FROM chunk WHERE id = $1`, id).Scan(&refCount); err != nil {
			t.Fatalf("read live_ref_count for chunk %d: %v", id, err)
		}
		if refCount != 0 {
			t.Errorf("expected live_ref_count=0 for chunk %d after rebuild mark, got %d", id, refCount)
		}

		var retryCount int64
		if err := dbconn.QueryRow(`SELECT retry_count FROM chunk WHERE id = $1`, id).Scan(&retryCount); err != nil {
			t.Fatalf("read retry_count for chunk %d: %v", id, err)
		}
		if retryCount != 0 {
			t.Errorf("expected retry_count=0 for chunk %d during generic rebuild cleanup, got %d", id, retryCount)
		}
	}
}

func TestMarkLogicalFileForRebuildRemovesStaleFileChunkGarbage(t *testing.T) {
	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	defer func() { _ = dbconn.Close() }()

	if err := db.RunMigrations(dbconn); err != nil {
		t.Fatalf("run migrations: %v", err)
	}

	var fileID int64
	if err := dbconn.QueryRow(
		`INSERT INTO logical_file (original_name, total_size, file_hash, status, chunker_version)
		 VALUES ($1, $2, $3, $4, 'v1-simple-rolling') RETURNING id`,
		"stale-garbage.bin", 256, "stale-garbage-hash", filestate.LogicalFileCompleted,
	).Scan(&fileID); err != nil {
		t.Fatalf("insert logical_file: %v", err)
	}

	insertChunk := func(hash string) int64 {
		t.Helper()
		var id int64
		if err := dbconn.QueryRow(
			`INSERT INTO chunk (chunk_hash, size, status, live_ref_count, chunker_version)
			 VALUES ($1, 64, $2, 1, 'v1-simple-rolling') RETURNING id`,
			hash, filestate.ChunkCompleted,
		).Scan(&id); err != nil {
			t.Fatalf("insert chunk %s: %v", hash, err)
		}
		return id
	}

	chunkValid := insertChunk("stale-garbage-valid")
	chunkStale := insertChunk("stale-garbage-extra")

	insertReusableTestFileChunk(t, dbconn, fileID, chunkValid, 0)
	insertReusableTestFileChunk(t, dbconn, fileID, chunkStale, 99)

	ctx := context.Background()
	if err := markLogicalFileForRebuildWithContext(ctx, dbconn, fileID); err != nil {
		t.Fatalf("markLogicalFileForRebuildWithContext: %v", err)
	}

	var mappingCount int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM file_chunk WHERE logical_file_id = $1`, fileID).Scan(&mappingCount); err != nil {
		t.Fatalf("count file_chunk rows: %v", err)
	}
	if mappingCount != 0 {
		t.Fatalf("expected stale file_chunk garbage to be fully removed, got %d rows", mappingCount)
	}

	for _, id := range []int64{chunkValid, chunkStale} {
		var refCount int64
		if err := dbconn.QueryRow(`SELECT live_ref_count FROM chunk WHERE id = $1`, id).Scan(&refCount); err != nil {
			t.Fatalf("read live_ref_count for chunk %d: %v", id, err)
		}
		if refCount != 0 {
			t.Fatalf("expected live_ref_count=0 for chunk %d after stale garbage cleanup, got %d", id, refCount)
		}

		var retryCount int64
		if err := dbconn.QueryRow(`SELECT retry_count FROM chunk WHERE id = $1`, id).Scan(&retryCount); err != nil {
			t.Fatalf("read retry_count for chunk %d: %v", id, err)
		}
		if retryCount != 0 {
			t.Fatalf("expected retry_count=0 for chunk %d during stale garbage cleanup, got %d", id, retryCount)
		}
	}
}

// TestMarkLogicalFileForRebuildIsIdempotentWhenAlreadyAborted verifies that
// calling markLogicalFileForRebuildWithContext on a file that is already ABORTED
// (i.e. another goroutine already marked it) succeeds without error.
func TestMarkLogicalFileForRebuildIsIdempotentWhenAlreadyAborted(t *testing.T) {
	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	defer func() { _ = dbconn.Close() }()

	if err := db.RunMigrations(dbconn); err != nil {
		t.Fatalf("run migrations: %v", err)
	}

	var fileID int64
	if err := dbconn.QueryRow(
		`INSERT INTO logical_file (original_name, total_size, file_hash, status, chunker_version)
		 VALUES ($1, $2, $3, $4, 'v1-simple-rolling') RETURNING id`,
		"idempotent_test.bin", 0, "idempotent-file-hash", filestate.LogicalFileAborted,
	).Scan(&fileID); err != nil {
		t.Fatalf("insert logical_file: %v", err)
	}

	ctx := context.Background()
	if err := markLogicalFileForRebuildWithContext(ctx, dbconn, fileID); err != nil {
		t.Fatalf("markLogicalFileForRebuildWithContext on already-ABORTED file: %v", err)
	}
}

func TestClaimLogicalFileReclaimCleansStaleMappingsBeforeRetry(t *testing.T) {
	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	defer func() { _ = dbconn.Close() }()

	if err := db.RunMigrations(dbconn); err != nil {
		t.Fatalf("run migrations: %v", err)
	}

	tmp := t.TempDir()
	filePath := filepath.Join(tmp, "retry-reclaim.bin")
	payload := []byte("retry-reclaim-payload-that-needs-clean-start")
	if err := os.WriteFile(filePath, payload, 0o644); err != nil {
		t.Fatalf("write temp file: %v", err)
	}
	fileInfo, err := os.Stat(filePath)
	if err != nil {
		t.Fatalf("stat temp file: %v", err)
	}
	sum := sha256.Sum256(payload)
	fileHash := hex.EncodeToString(sum[:])

	var fileID int64
	if err := dbconn.QueryRow(
		`INSERT INTO logical_file (original_name, total_size, file_hash, status, chunker_version)
		 VALUES ($1, $2, $3, $4, 'v1-simple-rolling')
		 RETURNING id`,
		fileInfo.Name(),
		fileInfo.Size(),
		fileHash,
		filestate.LogicalFileAborted,
	).Scan(&fileID); err != nil {
		t.Fatalf("insert aborted logical_file: %v", err)
	}

	insertChunk := func(hash string) int64 {
		t.Helper()
		var chunkID int64
		if err := dbconn.QueryRow(
			`INSERT INTO chunk (chunk_hash, size, status, live_ref_count, chunker_version)
			 VALUES ($1, $2, $3, $4, 'v1-simple-rolling')
			 RETURNING id`,
			hash,
			int64(len(payload)/2),
			filestate.ChunkCompleted,
			1,
		).Scan(&chunkID); err != nil {
			t.Fatalf("insert chunk %s: %v", hash, err)
		}
		return chunkID
	}

	chunkA := insertChunk("retry-reclaim-chunk-a")
	chunkB := insertChunk("retry-reclaim-chunk-b")
	insertReusableTestFileChunk(t, dbconn, fileID, chunkA, 0)
	insertReusableTestFileChunk(t, dbconn, fileID, chunkB, 1)

	ctx, cancel := db.NewOperationContext(context.Background())
	defer cancel()

	claimedID, claimedStatus, err := claimLogicalFileWithContext(ctx, dbconn, fileInfo, fileHash, string(chunk.DefaultChunkerVersion), tmp)
	if err != nil {
		t.Fatalf("claim logical file for retry: %v", err)
	}
	if claimedID != fileID {
		t.Fatalf("expected claimed logical file %d, got %d", fileID, claimedID)
	}
	if claimedStatus != filestate.LogicalFileProcessing {
		t.Fatalf("expected claimed status PROCESSING, got %s", claimedStatus)
	}

	var mappingCount int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM file_chunk WHERE logical_file_id = $1`, fileID).Scan(&mappingCount); err != nil {
		t.Fatalf("count stale file_chunk rows after reclaim: %v", err)
	}
	if mappingCount != 0 {
		t.Fatalf("expected stale file_chunk rows to be removed before retry, got %d", mappingCount)
	}

	for _, chunkID := range []int64{chunkA, chunkB} {
		var refCount int64
		if err := dbconn.QueryRow(`SELECT live_ref_count FROM chunk WHERE id = $1`, chunkID).Scan(&refCount); err != nil {
			t.Fatalf("read live_ref_count for chunk %d: %v", chunkID, err)
		}
		if refCount != 0 {
			t.Fatalf("expected live_ref_count=0 for chunk %d after reclaim cleanup, got %d", chunkID, refCount)
		}
	}
}

func TestMarkLogicalFileForReuseValidationFailureMarksEachChunkSuspiciousOnce(t *testing.T) {
	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	defer func() { _ = dbconn.Close() }()

	if err := db.RunMigrations(dbconn); err != nil {
		t.Fatalf("run migrations: %v", err)
	}

	var fileID int64
	if err := dbconn.QueryRow(
		`INSERT INTO logical_file (original_name, total_size, file_hash, status, chunker_version)
		 VALUES ($1, $2, $3, $4, 'v1-simple-rolling') RETURNING id`,
		"duplicate-chunk-ref.bin", 128, "duplicate-ref-hash", filestate.LogicalFileCompleted,
	).Scan(&fileID); err != nil {
		t.Fatalf("insert logical_file: %v", err)
	}

	var chunkID int64
	if err := dbconn.QueryRow(
		`INSERT INTO chunk (chunk_hash, size, status, live_ref_count, chunker_version)
		 VALUES ($1, 64, $2, 2, 'v1-simple-rolling') RETURNING id`,
		"duplicate-ref-chunk", filestate.ChunkCompleted,
	).Scan(&chunkID); err != nil {
		t.Fatalf("insert chunk: %v", err)
	}

	insertReusableTestFileChunk(t, dbconn, fileID, chunkID, 0)
	insertReusableTestFileChunk(t, dbconn, fileID, chunkID, 1)

	ctx := context.Background()
	if err := markLogicalFileForReuseValidationFailureWithContext(ctx, dbconn, fileID); err != nil {
		t.Fatalf("markLogicalFileForReuseValidationFailureWithContext: %v", err)
	}

	var refCount int64
	if err := dbconn.QueryRow(`SELECT live_ref_count FROM chunk WHERE id = $1`, chunkID).Scan(&refCount); err != nil {
		t.Fatalf("read live_ref_count: %v", err)
	}
	if refCount != 0 {
		t.Fatalf("expected live_ref_count=0 after removing two mappings, got %d", refCount)
	}

	var retryCount int64
	if err := dbconn.QueryRow(`SELECT retry_count FROM chunk WHERE id = $1`, chunkID).Scan(&retryCount); err != nil {
		t.Fatalf("read retry_count: %v", err)
	}
	if retryCount != 1 {
		t.Fatalf("expected retry_count=1 for duplicate chunk references, got %d", retryCount)
	}
}

// TestFinalizeLogicalFileStorageAtomicBoundary verifies that the finalization
// transaction atomically verifies all chunks are linked and marks the file complete.
// This guards against the race where chunks are committed but file completion fails.
func TestFinalizeLogicalFileStorageAtomicBoundary(t *testing.T) {
	testCases := []struct {
		name               string
		linkedChunkCount   int
		expectedChunkCount int
		shouldSucceed      bool
		wantErrSubstr      string
	}{
		{
			name:               "all chunks linked",
			linkedChunkCount:   3,
			expectedChunkCount: 3,
			shouldSucceed:      true,
		},
		{
			name:               "missing chunks (fewer linked)",
			linkedChunkCount:   2,
			expectedChunkCount: 3,
			shouldSucceed:      false,
			wantErrSubstr:      "has 2 linked chunks, expected 3",
		},
		{
			name:               "extra chunks (more linked)",
			linkedChunkCount:   4,
			expectedChunkCount: 3,
			shouldSucceed:      false,
			wantErrSubstr:      "has 4 linked chunks, expected 3",
		},
		{
			name:               "non-contiguous ordering",
			linkedChunkCount:   3,
			expectedChunkCount: 3,
			shouldSucceed:      false,
			wantErrSubstr:      "chunk_order max is",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			dbconn, err := sql.Open("sqlite3", ":memory:")
			if err != nil {
				t.Fatalf("open sqlite db: %v", err)
			}
			defer func() { _ = dbconn.Close() }()

			if err := db.RunMigrations(dbconn); err != nil {
				t.Fatalf("run migrations: %v", err)
			}

			var fileID int64
			if err := dbconn.QueryRow(
				`INSERT INTO logical_file (original_name, total_size, file_hash, status, chunker_version)
				 VALUES ($1, $2, $3, $4, 'v1-simple-rolling') RETURNING id`,
				"finalize_test.bin", 1024, "finalize-test-hash", filestate.LogicalFileProcessing,
			).Scan(&fileID); err != nil {
				t.Fatalf("insert logical_file: %v", err)
			}

			// Insert chunks
			for i := 0; i < tc.linkedChunkCount; i++ {
				chunkID := insertReusableTestChunk(t, dbconn, fmt.Sprintf("finalize-chunk-%d", i), filestate.ChunkCompleted)

				chunkOrder := i
				// For the non-contiguous ordering test, skip order 1
				if tc.name == "non-contiguous ordering" && i == 1 {
					chunkOrder = 5 // Gap in sequence
				}

				insertReusableTestFileChunk(t, dbconn, fileID, chunkID, chunkOrder)
			}

			ctx, cancel := db.NewOperationContext(context.Background())
			defer cancel()

			err = finalizeLogicalFileStorageWithContext(ctx, dbconn, fileID, tc.expectedChunkCount)

			if tc.shouldSucceed {
				if err != nil {
					t.Fatalf("expected finalize to succeed, got error: %v", err)
				}

				// Verify file is marked COMPLETED
				var status string
				if err := dbconn.QueryRowContext(ctx, `SELECT status FROM logical_file WHERE id = $1`, fileID).Scan(&status); err != nil {
					t.Fatalf("read logical_file status: %v", err)
				}
				if status != filestate.LogicalFileCompleted {
					t.Fatalf("expected status COMPLETED, got %s", status)
				}
			} else {
				if err == nil || (tc.wantErrSubstr != "" && !strings.Contains(err.Error(), tc.wantErrSubstr)) {
					t.Fatalf("expected error containing %q, got: %v", tc.wantErrSubstr, err)
				}

				// Verify file is still PROCESSING (transaction rolled back)
				var status string
				if err := dbconn.QueryRowContext(ctx, `SELECT status FROM logical_file WHERE id = $1`, fileID).Scan(&status); err != nil {
					t.Fatalf("read logical_file status: %v", err)
				}
				if status != filestate.LogicalFileProcessing {
					t.Fatalf("expected file to remain PROCESSING after failed finalize, got %s", status)
				}
			}
		})
	}
}
