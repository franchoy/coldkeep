package maintenance

import (
	"context"
	"database/sql"
	"math"
	"testing"
	"time"

	idb "github.com/franchoy/coldkeep/internal/db"
	"github.com/franchoy/coldkeep/tests/testdb"
	_ "github.com/mattn/go-sqlite3"
)

func openStatsTestDB(t *testing.T) *sql.DB {
	t.Helper()
	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	t.Cleanup(func() { _ = dbconn.Close() })

	if err := idb.RunMigrations(dbconn); err != nil {
		t.Fatalf("run migrations: %v", err)
	}

	return dbconn
}

func insertStatsLogicalFile(t *testing.T, dbconn *sql.DB, name string, totalSize int64) int64 {
	t.Helper()
	res, err := dbconn.Exec(
		`INSERT INTO logical_file (original_name, total_size, file_hash, status, chunker_version) VALUES (?, ?, ?, ?, 'v1-simple-rolling')`,
		name, totalSize, name+"-hash", "COMPLETED",
	)
	if err != nil {
		t.Fatalf("insert logical_file %q: %v", name, err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		t.Fatalf("last insert id: %v", err)
	}
	return id
}

func TestRunStatsResultIncludesSnapshotRetentionVisibility(t *testing.T) {
	dbconn := openStatsTestDB(t)
	ctx := context.Background()

	currentOnlyID := insertStatsLogicalFile(t, dbconn, "current-only", 11)
	snapshotOnlyID := insertStatsLogicalFile(t, dbconn, "snapshot-only", 22)
	sharedID := insertStatsLogicalFile(t, dbconn, "shared", 33)
	insertStatsLogicalFile(t, dbconn, "unreferenced", 44)

	if _, err := dbconn.Exec(
		`INSERT INTO physical_file (path, logical_file_id, is_metadata_complete) VALUES (?, ?, 1), (?, ?, 1)`,
		"/data/current-only", currentOnlyID,
		"/data/shared", sharedID,
	); err != nil {
		t.Fatalf("insert physical_file rows: %v", err)
	}

	if _, err := dbconn.Exec(
		`INSERT INTO snapshot (id, created_at, type) VALUES (?, ?, ?)`,
		"snap-stats-retention", time.Now().UTC(), "full",
	); err != nil {
		t.Fatalf("insert snapshot: %v", err)
	}
	testdb.InsertSnapshotFileRef(t, dbconn, "snap-stats-retention", "snap/snapshot-only", snapshotOnlyID)
	testdb.InsertSnapshotFileRef(t, dbconn, "snap-stats-retention", "snap/shared", sharedID)

	stats, err := runStatsResultWithDB(ctx, dbconn)
	if err != nil {
		t.Fatalf("runStatsResultWithDB: %v", err)
	}

	if stats.SnapshotRetention.CurrentOnlyLogicalFiles != 1 || stats.SnapshotRetention.CurrentOnlyBytes != 11 {
		t.Fatalf("unexpected current-only stats: %+v", stats.SnapshotRetention)
	}
	if stats.SnapshotRetention.SnapshotReferencedLogicalFiles != 2 || stats.SnapshotRetention.SnapshotReferencedBytes != 55 {
		t.Fatalf("unexpected snapshot-referenced stats: %+v", stats.SnapshotRetention)
	}
	if stats.SnapshotRetention.SnapshotOnlyLogicalFiles != 1 || stats.SnapshotRetention.SnapshotOnlyBytes != 22 {
		t.Fatalf("unexpected snapshot-only stats: %+v", stats.SnapshotRetention)
	}
	if stats.SnapshotRetention.SharedLogicalFiles != 1 || stats.SnapshotRetention.SharedBytes != 33 {
		t.Fatalf("unexpected shared stats: %+v", stats.SnapshotRetention)
	}
	if got := stats.SnapshotRetention.CurrentOnlyLogicalFiles + stats.SnapshotRetention.SnapshotOnlyLogicalFiles + stats.SnapshotRetention.SharedLogicalFiles; got != 3 {
		t.Fatalf("unexpected retained logical file total=%d stats=%+v", got, stats.SnapshotRetention)
	}
}

func TestRunStatsResultIncludesChunkCountsByVersion(t *testing.T) {
	dbconn := openStatsTestDB(t)
	ctx := context.Background()

	if _, err := dbconn.Exec(
		`INSERT INTO logical_file (original_name, total_size, file_hash, status, chunker_version) VALUES
		 ('lf-v1-a', 101, 'lf-v1-a-hash', 'COMPLETED', 'v1-simple-rolling'),
		 ('lf-v1-b', 102, 'lf-v1-b-hash', 'COMPLETED', 'v1-simple-rolling'),
		 ('lf-v2-a', 103, 'lf-v2-a-hash', 'COMPLETED', 'v2-fastcdc')`,
	); err != nil {
		t.Fatalf("insert logical_file rows: %v", err)
	}

	if _, err := dbconn.Exec(
		`INSERT INTO chunk (chunk_hash, size, status, live_ref_count, chunker_version) VALUES
		 ('stats-v1-a', 10, 'COMPLETED', 0, 'v1-simple-rolling'),
		 ('stats-v1-b', 11, 'PROCESSING', 0, 'v1-simple-rolling'),
		 ('stats-v2-a', 12, 'ABORTED', 0, 'v2-fastcdc')`,
	); err != nil {
		t.Fatalf("insert chunk rows: %v", err)
	}

	if _, err := dbconn.Exec(
		`INSERT INTO file_chunk (logical_file_id, chunk_id, chunk_order) VALUES
		 (1, 1, 0),
		 (1, 2, 1),
		 (2, 1, 0),
		 (3, 3, 0)`,
	); err != nil {
		t.Fatalf("insert file_chunk rows: %v", err)
	}

	stats, err := runStatsResultWithDB(ctx, dbconn)
	if err != nil {
		t.Fatalf("runStatsResultWithDB: %v", err)
	}

	if stats.ChunkCountsByVersion == nil {
		t.Fatal("expected chunk_counts_by_version map to be initialized")
	}
	if stats.ChunkBytesByVersion == nil {
		t.Fatal("expected chunk_bytes_by_version map to be initialized")
	}
	if got := stats.ChunkCountsByVersion["v1-simple-rolling"]; got != 2 {
		t.Fatalf("expected v1-simple-rolling count=2, got %d", got)
	}
	if got := stats.ChunkCountsByVersion["v2-fastcdc"]; got != 1 {
		t.Fatalf("expected v2-fastcdc count=1, got %d", got)
	}
	if got := stats.ChunkBytesByVersion["v1-simple-rolling"]; got != 21 {
		t.Fatalf("expected v1-simple-rolling bytes=21, got %d", got)
	}
	if got := stats.ChunkBytesByVersion["v2-fastcdc"]; got != 12 {
		t.Fatalf("expected v2-fastcdc bytes=12, got %d", got)
	}
	if stats.LogicalFileCountsByVersion == nil {
		t.Fatal("expected logical_file_counts_by_version map to be initialized")
	}
	if got := stats.ActiveWriteChunker; got != "v2-fastcdc" {
		t.Fatalf("expected active write chunker=v2-fastcdc, got %q", got)
	}
	if got := stats.LogicalFileCountsByVersion["v1-simple-rolling"]; got != 2 {
		t.Fatalf("expected logical file count for v1-simple-rolling=2, got %d", got)
	}
	if got := stats.LogicalFileCountsByVersion["v2-fastcdc"]; got != 1 {
		t.Fatalf("expected logical file count for v2-fastcdc=1, got %d", got)
	}
	if stats.TotalChunkReferences != 4 {
		t.Fatalf("expected total_chunk_references=4, got %d", stats.TotalChunkReferences)
	}
	if stats.UniqueReferencedChunks != 3 {
		t.Fatalf("expected unique_referenced_chunks=3, got %d", stats.UniqueReferencedChunks)
	}
	if stats.EstimatedDedupRatioPct != 25 {
		t.Fatalf("expected estimated_dedup_ratio_pct=25, got %.2f", stats.EstimatedDedupRatioPct)
	}
}

func TestRunStatsResultBucketsUnknownChunkerMetadata(t *testing.T) {
	dbconn := openStatsTestDB(t)
	ctx := context.Background()

	if _, err := dbconn.Exec(`UPDATE repository_config SET value = ? WHERE key = ?`, " v9-future ", "default_chunker"); err != nil {
		t.Fatalf("update repository default chunker: %v", err)
	}

	if _, err := dbconn.Exec(
		`INSERT INTO logical_file (original_name, total_size, file_hash, status, chunker_version) VALUES
		 ('lf-unknown-a', 201, 'lf-unknown-a-hash', 'COMPLETED', ''),
		 ('lf-unknown-b', 202, 'lf-unknown-b-hash', 'COMPLETED', '   ')`,
	); err != nil {
		t.Fatalf("insert logical_file unknown rows: %v", err)
	}

	if _, err := dbconn.Exec(
		`INSERT INTO chunk (chunk_hash, size, status, live_ref_count, chunker_version) VALUES
		 ('stats-unknown-a', 13, 'COMPLETED', 0, ''),
		 ('stats-unknown-b', 17, 'PROCESSING', 0, '   ')`,
	); err != nil {
		t.Fatalf("insert chunk unknown rows: %v", err)
	}

	stats, err := runStatsResultWithDB(ctx, dbconn)
	if err != nil {
		t.Fatalf("runStatsResultWithDB: %v", err)
	}

	if got := stats.ChunkCountsByVersion["unknown"]; got != 2 {
		t.Fatalf("expected unknown chunk count=2, got %d", got)
	}
	if got := stats.ChunkBytesByVersion["unknown"]; got != 30 {
		t.Fatalf("expected unknown chunk bytes=30, got %d", got)
	}
	if got := stats.LogicalFileCountsByVersion["unknown"]; got != 2 {
		t.Fatalf("expected unknown logical file count=2, got %d", got)
	}
	if got := stats.ActiveWriteChunker; got != "unknown" {
		t.Fatalf("expected active write chunker=unknown, got %q", got)
	}
}

func TestRunStatsResultEmptyRepository(t *testing.T) {
	dbconn := openStatsTestDB(t)
	ctx := context.Background()

	stats, err := runStatsResultWithDB(ctx, dbconn)
	if err != nil {
		t.Fatalf("runStatsResultWithDB: %v", err)
	}

	if stats.TotalFiles != 0 {
		t.Fatalf("expected total files=0, got %d", stats.TotalFiles)
	}
	if stats.TotalChunks != 0 {
		t.Fatalf("expected total chunks=0, got %d", stats.TotalChunks)
	}
	if stats.ChunkCountsByVersion == nil {
		t.Fatal("expected chunk_counts_by_version map to be initialized")
	}
	if len(stats.ChunkCountsByVersion) != 0 {
		t.Fatalf("expected no chunk count buckets, got %v", stats.ChunkCountsByVersion)
	}
	if stats.ChunkBytesByVersion == nil {
		t.Fatal("expected chunk_bytes_by_version map to be initialized")
	}
	if len(stats.ChunkBytesByVersion) != 0 {
		t.Fatalf("expected no chunk byte buckets, got %v", stats.ChunkBytesByVersion)
	}
	if stats.LogicalFileCountsByVersion == nil {
		t.Fatal("expected logical_file_counts_by_version map to be initialized")
	}
	if len(stats.LogicalFileCountsByVersion) != 0 {
		t.Fatalf("expected no logical file buckets, got %v", stats.LogicalFileCountsByVersion)
	}
	if got := stats.ActiveWriteChunker; got != "v2-fastcdc" {
		t.Fatalf("expected fresh repo active write chunker=v2-fastcdc, got %q", got)
	}
	if stats.TotalChunkReferences != 0 || stats.UniqueReferencedChunks != 0 || stats.EstimatedDedupRatioPct != 0 {
		t.Fatalf("expected zero dedup signal for empty repo, got refs=%d unique=%d ratio=%.2f", stats.TotalChunkReferences, stats.UniqueReferencedChunks, stats.EstimatedDedupRatioPct)
	}
}

func TestRunStatsResultPureV1RepositoryReportsOnlyV1(t *testing.T) {
	dbconn := openStatsTestDB(t)
	ctx := context.Background()

	if _, err := dbconn.Exec(
		`INSERT INTO logical_file (original_name, total_size, file_hash, status, chunker_version) VALUES
		 ('lf-v1-a', 101, 'lf-v1-a-hash', 'COMPLETED', 'v1-simple-rolling'),
		 ('lf-v1-b', 102, 'lf-v1-b-hash', 'COMPLETED', 'v1-simple-rolling')`,
	); err != nil {
		t.Fatalf("insert logical_file rows: %v", err)
	}

	if _, err := dbconn.Exec(
		`INSERT INTO chunk (chunk_hash, size, status, live_ref_count, chunker_version) VALUES
		 ('v1-only-a', 10, 'COMPLETED', 0, 'v1-simple-rolling'),
		 ('v1-only-b', 11, 'PROCESSING', 0, 'v1-simple-rolling')`,
	); err != nil {
		t.Fatalf("insert chunk rows: %v", err)
	}

	stats, err := runStatsResultWithDB(ctx, dbconn)
	if err != nil {
		t.Fatalf("runStatsResultWithDB: %v", err)
	}

	if got := stats.ChunkCountsByVersion["v1-simple-rolling"]; got != 2 {
		t.Fatalf("expected v1-only chunk count=2, got %d", got)
	}
	if got := stats.LogicalFileCountsByVersion["v1-simple-rolling"]; got != 2 {
		t.Fatalf("expected v1-only logical file count=2, got %d", got)
	}
	if _, exists := stats.ChunkCountsByVersion["v2-fastcdc"]; exists {
		t.Fatalf("expected v2-fastcdc chunk bucket to be absent in pure v1 repo, got map=%v", stats.ChunkCountsByVersion)
	}
	if _, exists := stats.LogicalFileCountsByVersion["v2-fastcdc"]; exists {
		t.Fatalf("expected v2-fastcdc logical file bucket to be absent in pure v1 repo, got map=%v", stats.LogicalFileCountsByVersion)
	}
}

func TestRunStatsResultPureV2RepositoryReportsOnlyV2(t *testing.T) {
	dbconn := openStatsTestDB(t)
	ctx := context.Background()

	if _, err := dbconn.Exec(
		`INSERT INTO logical_file (original_name, total_size, file_hash, status, chunker_version) VALUES
		 ('lf-v2-a', 201, 'lf-v2-a-hash', 'COMPLETED', 'v2-fastcdc'),
		 ('lf-v2-b', 202, 'lf-v2-b-hash', 'COMPLETED', 'v2-fastcdc')`,
	); err != nil {
		t.Fatalf("insert logical_file rows: %v", err)
	}

	if _, err := dbconn.Exec(
		`INSERT INTO chunk (chunk_hash, size, status, live_ref_count, chunker_version) VALUES
		 ('v2-only-a', 20, 'COMPLETED', 0, 'v2-fastcdc'),
		 ('v2-only-b', 21, 'PROCESSING', 0, 'v2-fastcdc')`,
	); err != nil {
		t.Fatalf("insert chunk rows: %v", err)
	}

	stats, err := runStatsResultWithDB(ctx, dbconn)
	if err != nil {
		t.Fatalf("runStatsResultWithDB: %v", err)
	}

	if got := stats.ChunkCountsByVersion["v2-fastcdc"]; got != 2 {
		t.Fatalf("expected v2-only chunk count=2, got %d", got)
	}
	if got := stats.LogicalFileCountsByVersion["v2-fastcdc"]; got != 2 {
		t.Fatalf("expected v2-only logical file count=2, got %d", got)
	}
	if _, exists := stats.ChunkCountsByVersion["v1-simple-rolling"]; exists {
		t.Fatalf("expected v1-simple-rolling chunk bucket to be absent in pure v2 repo, got map=%v", stats.ChunkCountsByVersion)
	}
	if _, exists := stats.LogicalFileCountsByVersion["v1-simple-rolling"]; exists {
		t.Fatalf("expected v1-simple-rolling logical file bucket to be absent in pure v2 repo, got map=%v", stats.LogicalFileCountsByVersion)
	}
}

func TestRunStatsResultMixedRepositoryReportsBothVersions(t *testing.T) {
	dbconn := openStatsTestDB(t)
	ctx := context.Background()

	if _, err := dbconn.Exec(
		`INSERT INTO logical_file (original_name, total_size, file_hash, status, chunker_version) VALUES
		 ('lf-v1-a', 101, 'lf-v1-a-hash', 'COMPLETED', 'v1-simple-rolling'),
		 ('lf-v2-a', 201, 'lf-v2-a-hash', 'COMPLETED', 'v2-fastcdc'),
		 ('lf-v2-b', 202, 'lf-v2-b-hash', 'COMPLETED', 'v2-fastcdc')`,
	); err != nil {
		t.Fatalf("insert logical_file rows: %v", err)
	}

	if _, err := dbconn.Exec(
		`INSERT INTO chunk (chunk_hash, size, status, live_ref_count, chunker_version) VALUES
		 ('mixed-v1-a', 10, 'COMPLETED', 0, 'v1-simple-rolling'),
		 ('mixed-v1-b', 11, 'PROCESSING', 0, 'v1-simple-rolling'),
		 ('mixed-v2-a', 20, 'COMPLETED', 0, 'v2-fastcdc')`,
	); err != nil {
		t.Fatalf("insert chunk rows: %v", err)
	}

	stats, err := runStatsResultWithDB(ctx, dbconn)
	if err != nil {
		t.Fatalf("runStatsResultWithDB: %v", err)
	}

	if got := stats.ChunkCountsByVersion["v1-simple-rolling"]; got != 2 {
		t.Fatalf("expected mixed v1 chunk count=2, got %d", got)
	}
	if got := stats.ChunkCountsByVersion["v2-fastcdc"]; got != 1 {
		t.Fatalf("expected mixed v2 chunk count=1, got %d", got)
	}
	if got := stats.LogicalFileCountsByVersion["v1-simple-rolling"]; got != 1 {
		t.Fatalf("expected mixed v1 logical file count=1, got %d", got)
	}
	if got := stats.LogicalFileCountsByVersion["v2-fastcdc"]; got != 2 {
		t.Fatalf("expected mixed v2 logical file count=2, got %d", got)
	}
}

func TestRunStatsResultVersionTotalsMatchDatabaseReality(t *testing.T) {
	dbconn := openStatsTestDB(t)
	ctx := context.Background()

	if _, err := dbconn.Exec(
		`INSERT INTO logical_file (original_name, total_size, file_hash, status, chunker_version) VALUES
		 ('lf-v1-a', 101, 'lf-v1-a-hash', 'COMPLETED', 'v1-simple-rolling'),
		 ('lf-v1-b', 102, 'lf-v1-b-hash', 'COMPLETED', 'v1-simple-rolling'),
		 ('lf-v2-a', 201, 'lf-v2-a-hash', 'COMPLETED', 'v2-fastcdc')`,
	); err != nil {
		t.Fatalf("insert logical_file rows: %v", err)
	}

	if _, err := dbconn.Exec(
		`INSERT INTO chunk (chunk_hash, size, status, live_ref_count, chunker_version) VALUES
		 ('totals-v1-a', 10, 'COMPLETED', 0, 'v1-simple-rolling'),
		 ('totals-v1-b', 11, 'PROCESSING', 0, 'v1-simple-rolling'),
		 ('totals-v2-a', 20, 'ABORTED', 0, 'v2-fastcdc')`,
	); err != nil {
		t.Fatalf("insert chunk rows: %v", err)
	}

	stats, err := runStatsResultWithDB(ctx, dbconn)
	if err != nil {
		t.Fatalf("runStatsResultWithDB: %v", err)
	}

	var dbChunkCount int64
	if err := dbconn.QueryRowContext(ctx, `SELECT COUNT(*) FROM chunk`).Scan(&dbChunkCount); err != nil {
		t.Fatalf("query total chunk count: %v", err)
	}
	var dbChunkBytes int64
	if err := dbconn.QueryRowContext(ctx, `SELECT COALESCE(SUM(size),0) FROM chunk`).Scan(&dbChunkBytes); err != nil {
		t.Fatalf("query total chunk bytes: %v", err)
	}

	var statsChunkCount int64
	for _, c := range stats.ChunkCountsByVersion {
		statsChunkCount += c
	}
	if statsChunkCount != dbChunkCount {
		t.Fatalf("chunk count totals mismatch: stats=%d db=%d map=%v", statsChunkCount, dbChunkCount, stats.ChunkCountsByVersion)
	}

	var statsChunkBytes int64
	for _, b := range stats.ChunkBytesByVersion {
		statsChunkBytes += b
	}
	if statsChunkBytes != dbChunkBytes {
		t.Fatalf("chunk byte totals mismatch: stats=%d db=%d map=%v", statsChunkBytes, dbChunkBytes, stats.ChunkBytesByVersion)
	}
}

func TestCollectBlockStatsAndRunStatsExposure(t *testing.T) {
	dbconn := openStatsTestDB(t)
	ctx := context.Background()

	t.Setenv("COLDKEEP_BLOCK_TARGET_SIZE_MB", "2")

	containerRes, err := dbconn.Exec(`INSERT INTO container (filename, current_size, max_size, sealed, quarantine) VALUES (?, ?, ?, 1, 0)`, "stats-blocks.bin", 0, 64*1024*1024)
	if err != nil {
		t.Fatalf("insert container: %v", err)
	}
	containerID, err := containerRes.LastInsertId()
	if err != nil {
		t.Fatalf("container id: %v", err)
	}

	chunkARes, err := dbconn.Exec(`INSERT INTO chunk (chunk_hash, size, status, live_ref_count, chunker_version) VALUES (?, ?, 'COMPLETED', 1, 'v2-fastcdc')`, "blk-stats-a", 128)
	if err != nil {
		t.Fatalf("insert chunk A: %v", err)
	}
	chunkAID, _ := chunkARes.LastInsertId()
	chunkBRes, err := dbconn.Exec(`INSERT INTO chunk (chunk_hash, size, status, live_ref_count, chunker_version) VALUES (?, ?, 'COMPLETED', 0, 'v2-fastcdc')`, "blk-stats-b", 256)
	if err != nil {
		t.Fatalf("insert chunk B: %v", err)
	}
	chunkBID, _ := chunkBRes.LastInsertId()

	if _, err := dbconn.Exec(`INSERT INTO blocks (chunk_id, codec, format_version, plaintext_size, stored_size, container_id, block_offset) VALUES (?, 'plain', 1, 128, 128, ?, 0)`, chunkAID, containerID); err != nil {
		t.Fatalf("insert legacy block: %v", err)
	}

	storageBlockRes, err := dbconn.Exec(`INSERT INTO storage_blocks (format_version, codec, plaintext_size, stored_size, container_id, container_offset, block_hash) VALUES (1, 'none', 512, 480, ?, 128, x'010203')`, containerID)
	if err != nil {
		t.Fatalf("insert storage_block: %v", err)
	}
	storageBlockID, _ := storageBlockRes.LastInsertId()
	if _, err := dbconn.Exec(`INSERT INTO chunk_block_refs (chunk_id, block_id, offset_in_block, size_in_block) VALUES (?, ?, 0, 128), (?, ?, 128, 256)`, chunkAID, storageBlockID, chunkBID, storageBlockID); err != nil {
		t.Fatalf("insert chunk_block_refs: %v", err)
	}

	blockStats, err := CollectBlockStats(ctx, dbconn)
	if err != nil {
		t.Fatalf("CollectBlockStats: %v", err)
	}

	if blockStats.StorageBlocks != 1 {
		t.Fatalf("storage blocks mismatch: got=%d want=1", blockStats.StorageBlocks)
	}
	if blockStats.ChunkBlockRefs != 2 {
		t.Fatalf("chunk block refs mismatch: got=%d want=2", blockStats.ChunkBlockRefs)
	}
	if blockStats.LegacyBlocks != 1 {
		t.Fatalf("legacy blocks mismatch: got=%d want=1", blockStats.LegacyBlocks)
	}
	if blockStats.PackedBlocks != 1 {
		t.Fatalf("packed blocks mismatch: got=%d want=1", blockStats.PackedBlocks)
	}
	if blockStats.AvgChunksPerBlock != 2 {
		t.Fatalf("avg chunks per block mismatch: got=%.2f want=2.00", blockStats.AvgChunksPerBlock)
	}
	if blockStats.AvgPlaintextSize != 512 {
		t.Fatalf("avg plaintext size mismatch: got=%.2f want=512", blockStats.AvgPlaintextSize)
	}
	if blockStats.AvgStoredSize != 480 {
		t.Fatalf("avg stored size mismatch: got=%.2f want=480", blockStats.AvgStoredSize)
	}
	if blockStats.LogicalBytes != 512 {
		t.Fatalf("logical bytes mismatch: got=%d want=512", blockStats.LogicalBytes)
	}
	if blockStats.CompressedBytes != 512 {
		t.Fatalf("compressed bytes mismatch with legacy NULL compressed_size fallback: got=%d want=512", blockStats.CompressedBytes)
	}
	if blockStats.StoredBytes != 480 {
		t.Fatalf("stored bytes mismatch: got=%d want=480", blockStats.StoredBytes)
	}
	if math.Abs(blockStats.CompressionRatio-1.0) > 1e-9 {
		t.Fatalf("compression ratio mismatch for compression=none: got=%.9f want=1.000000000", blockStats.CompressionRatio)
	}
	wantPhysicalRatio := float64(512) / float64(480)
	if math.Abs(blockStats.PhysicalRatio-wantPhysicalRatio) > 1e-9 {
		t.Fatalf("physical ratio mismatch: got=%.9f want=%.9f", blockStats.PhysicalRatio, wantPhysicalRatio)
	}
	wantFillRatio := float64(512) / float64(2*1024*1024)
	if math.Abs(blockStats.FillRatio-wantFillRatio) > 1e-9 {
		t.Fatalf("fill ratio mismatch: got=%.9f want=%.9f", blockStats.FillRatio, wantFillRatio)
	}
	if got := blockStats.CodecDistribution["none"]; got != 1 {
		t.Fatalf("codec distribution mismatch for none: got=%d want=1", got)
	}

	stats, err := runStatsResultWithDB(ctx, dbconn)
	if err != nil {
		t.Fatalf("runStatsResultWithDB: %v", err)
	}
	if stats.BlockStats.StorageBlocks != 1 || stats.BlockStats.ChunkBlockRefs != 2 {
		t.Fatalf("runStats block stats mismatch: %+v", stats.BlockStats)
	}
}

func TestCollectBlockStatsCompressionAggregatesMixedRepository(t *testing.T) {
	dbconn := openStatsTestDB(t)
	ctx := context.Background()

	containerRes, err := dbconn.Exec(`INSERT INTO container (filename, current_size, max_size, sealed, quarantine) VALUES (?, ?, ?, 1, 0)`, "stats-compression.bin", 0, 64*1024*1024)
	if err != nil {
		t.Fatalf("insert container: %v", err)
	}
	containerID, err := containerRes.LastInsertId()
	if err != nil {
		t.Fatalf("container id: %v", err)
	}

	if _, err := dbconn.Exec(`
		INSERT INTO storage_blocks (format_version, codec, plaintext_size, stored_size, container_id, container_offset, block_hash, compression_codec)
		VALUES
			(1, 'none', 100, 100, ?, 0, x'11', 'none'),
			(1, 'none', 1000, 350, ?, 100, x'22', 'zstd')
	`, containerID, containerID); err != nil {
		t.Fatalf("insert storage blocks: %v", err)
	}
	if _, err := dbconn.Exec(`UPDATE storage_blocks SET compressed_size = 300 WHERE block_hash = x'22'`); err != nil {
		t.Fatalf("set zstd compressed_size: %v", err)
	}

	blockStats, err := CollectBlockStats(ctx, dbconn)
	if err != nil {
		t.Fatalf("CollectBlockStats: %v", err)
	}

	if blockStats.StorageBlocks != 2 {
		t.Fatalf("storage blocks mismatch: got=%d want=2", blockStats.StorageBlocks)
	}
	if blockStats.LogicalBytes != 1100 {
		t.Fatalf("logical bytes mismatch: got=%d want=1100", blockStats.LogicalBytes)
	}
	if blockStats.CompressedBytes != 400 {
		t.Fatalf("compressed bytes mismatch: got=%d want=400", blockStats.CompressedBytes)
	}
	if blockStats.StoredBytes != 450 {
		t.Fatalf("stored bytes mismatch: got=%d want=450", blockStats.StoredBytes)
	}
	if blockStats.CompressionRatio <= 1.0 {
		t.Fatalf("expected compression ratio > 1.0 for repetitive zstd data, got=%.4f", blockStats.CompressionRatio)
	}
	wantCompressionRatio := float64(1100) / float64(400)
	if math.Abs(blockStats.CompressionRatio-wantCompressionRatio) > 1e-9 {
		t.Fatalf("compression ratio mismatch: got=%.9f want=%.9f", blockStats.CompressionRatio, wantCompressionRatio)
	}
	wantPhysicalRatio := float64(1100) / float64(450)
	if math.Abs(blockStats.PhysicalRatio-wantPhysicalRatio) > 1e-9 {
		t.Fatalf("physical ratio mismatch: got=%.9f want=%.9f", blockStats.PhysicalRatio, wantPhysicalRatio)
	}

	stats, err := runStatsResultWithDB(ctx, dbconn)
	if err != nil {
		t.Fatalf("runStatsResultWithDB: %v", err)
	}
	if stats.BlockStats.LogicalBytes != 1100 || stats.BlockStats.CompressedBytes != 400 || stats.BlockStats.StoredBytes != 450 {
		t.Fatalf("runStats compression aggregate mismatch: %+v", stats.BlockStats)
	}
}
