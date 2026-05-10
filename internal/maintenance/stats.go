package maintenance

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/franchoy/coldkeep/internal/chunk"
	"github.com/franchoy/coldkeep/internal/db"
	"github.com/franchoy/coldkeep/internal/retention"
	filestate "github.com/franchoy/coldkeep/internal/status"
	"github.com/franchoy/coldkeep/internal/utils_env"
)

const unknownChunkerBucket = "unknown"

func normalizeChunkerVersionBucket(raw string) string {
	version := chunk.Version(strings.TrimSpace(raw))
	if !chunk.IsWellFormedVersion(version) {
		return unknownChunkerBucket
	}
	return string(version)
}

// SnapshotRetentionStats explains how retained logical content is distributed
// across current-state references and snapshot history.
type SnapshotRetentionStats struct {
	CurrentOnlyLogicalFiles        int64 `json:"current_only_logical_files"`
	CurrentOnlyBytes               int64 `json:"current_only_bytes"`
	SnapshotReferencedLogicalFiles int64 `json:"snapshot_referenced_logical_files"`
	SnapshotReferencedBytes        int64 `json:"snapshot_referenced_bytes"`
	SnapshotOnlyLogicalFiles       int64 `json:"snapshot_only_logical_files"`
	SnapshotOnlyBytes              int64 `json:"snapshot_only_bytes"`
	SharedLogicalFiles             int64 `json:"shared_logical_files"`
	SharedBytes                    int64 `json:"shared_bytes"`
}

// BlockStats summarizes legacy and packed block layout metrics for benchmarking
// and operator visibility.
type BlockStats struct {
	StorageBlocks             int64            `json:"storage_blocks_count"`
	ChunkBlockRefs            int64            `json:"chunk_block_refs_count"`
	AvgChunksPerBlock         float64          `json:"avg_chunks_per_block"`
	AvgPlaintextSize          float64          `json:"avg_block_plaintext_size"`
	AvgStoredSize             float64          `json:"avg_block_stored_size"`
	LogicalBytes              int64            `json:"logical_bytes"`
	CompressedBytes           int64            `json:"compressed_bytes"`
	StoredBytes               int64            `json:"stored_bytes"`
	CompressionSizeRatio      float64          `json:"compression_size_ratio"`
	CompressionFactor         float64          `json:"compression_factor"`
	PhysicalSizeRatio         float64          `json:"physical_size_ratio"`
	PhysicalFactor            float64          `json:"physical_factor"`
	CompressedBlocks          int64            `json:"compressed_blocks"`
	UncompressedBlocks        int64            `json:"uncompressed_blocks"`
	FillRatio                 float64          `json:"avg_block_fill_ratio"`
	LegacyBlocks              int64            `json:"legacy_block_count"`
	PackedBlocks              int64            `json:"packed_block_count"`
	CodecDistribution         map[string]int64 `json:"codec_distribution"`
	CompressionCodecBreakdown map[string]int64 `json:"compression_codec_breakdown"`
}

// StatsResult holds the snapshot emitted by RunStatsResult.
type StatsResult struct {
	TotalFiles                 int64                  `json:"total_files"`
	TotalLogicalSizeBytes      int64                  `json:"total_logical_size_bytes"`
	CompletedFiles             int64                  `json:"completed_files"`
	CompletedSizeBytes         int64                  `json:"completed_size_bytes"`
	ProcessingFiles            int64                  `json:"processing_files"`
	ProcessingSizeBytes        int64                  `json:"processing_size_bytes"`
	AbortedFiles               int64                  `json:"aborted_files"`
	AbortedSizeBytes           int64                  `json:"aborted_size_bytes"`
	HealthyContainers          int64                  `json:"healthy_containers"`
	HealthyContainerBytes      int64                  `json:"healthy_container_bytes"`
	QuarantineContainers       int64                  `json:"quarantine_containers"`
	QuarantineContainerBytes   int64                  `json:"quarantine_container_bytes"`
	TotalContainers            int64                  `json:"total_containers"`
	TotalContainerBytes        int64                  `json:"total_container_bytes"`
	LiveBlockBytes             int64                  `json:"live_block_bytes"`
	DeadBlockBytes             int64                  `json:"dead_block_bytes"`
	GlobalDedupRatioPct        float64                `json:"global_dedup_ratio_pct"`
	FragmentationRatioPct      float64                `json:"fragmentation_ratio_pct"`
	TotalChunks                int64                  `json:"total_chunks"`
	CompletedChunks            int64                  `json:"completed_chunks"`
	CompletedChunkBytes        int64                  `json:"completed_chunk_bytes"`
	ProcessingChunks           int64                  `json:"processing_chunks"`
	AbortedChunks              int64                  `json:"aborted_chunks"`
	ChunkCountsByVersion       map[string]int64       `json:"chunk_counts_by_version"`
	ChunkBytesByVersion        map[string]int64       `json:"chunk_bytes_by_version"`
	LogicalFileCountsByVersion map[string]int64       `json:"logical_file_counts_by_version"`
	ActiveWriteChunker         string                 `json:"active_write_chunker"`
	TotalChunkReferences       int64                  `json:"total_chunk_references"`
	UniqueReferencedChunks     int64                  `json:"unique_referenced_chunks"`
	EstimatedDedupRatioPct     float64                `json:"estimated_dedup_ratio_pct"`
	TotalFileRetries           int64                  `json:"total_file_retries"`
	AvgFileRetries             float64                `json:"avg_file_retries"`
	MaxFileRetries             int64                  `json:"max_file_retries"`
	TotalChunkRetries          int64                  `json:"total_chunk_retries"`
	AvgChunkRetries            float64                `json:"avg_chunk_retries"`
	MaxChunkRetries            int64                  `json:"max_chunk_retries"`
	SnapshotRetention          SnapshotRetentionStats `json:"snapshot_retention"`
	BlockStats                 BlockStats             `json:"block_stats"`
	Containers                 []ContainerStatRecord  `json:"containers"`
}

// ContainerStatRecord holds per-container data.
type ContainerStatRecord struct {
	ID           int64   `json:"id"`
	Filename     string  `json:"filename"`
	TotalBytes   int64   `json:"total_bytes"`
	LiveBytes    int64   `json:"live_bytes"`
	DeadBytes    int64   `json:"dead_bytes"`
	Quarantine   bool    `json:"quarantine"`
	LiveRatioPct float64 `json:"live_ratio_pct"`
}

// RunStatsResult collects and returns all stats without printing.
func RunStatsResult() (*StatsResult, error) {
	dbconn, err := db.ConnectDB()
	if err != nil {
		return nil, fmt.Errorf("failed to connect to DB: %w", err)
	}
	defer func() { _ = dbconn.Close() }()
	ctx, cancel := db.NewOperationContext(context.Background())
	defer cancel()

	return runStatsResultWithDB(ctx, dbconn)
}

// RunStatsResultWithDB collects and returns all stats using the provided DB.
// The caller owns DB lifecycle and context cancellation.
func RunStatsResultWithDB(ctx context.Context, dbconn *sql.DB) (*StatsResult, error) {
	return runStatsResultWithDB(ctx, dbconn)
}

func runStatsResultWithDB(ctx context.Context, dbconn *sql.DB) (*StatsResult, error) {
	if dbconn == nil {
		return nil, fmt.Errorf("db connection is nil")
	}
	if ctx == nil {
		ctx = context.Background()
	}

	r := &StatsResult{}
	activeWriteChunker, err := resolveActiveWriteChunker(ctx, dbconn)
	if err != nil {
		return nil, err
	}
	r.ActiveWriteChunker = activeWriteChunker

	var liveBytes, deadBytes sql.NullInt64
	var totalFileRetries, maxFileRetries, totalChunkRetries, maxChunkRetries sql.NullInt64
	var avgFileRetries, avgChunkRetries sql.NullFloat64

	if err := dbconn.QueryRowContext(ctx, `
		SELECT
			COUNT(*) AS total_files,
			COALESCE(SUM(total_size), 0) AS total_size,
			COALESCE(SUM(CASE WHEN status = $1 THEN 1 ELSE 0 END), 0) AS completed_files,
			COALESCE(SUM(CASE WHEN status = $1 THEN total_size ELSE 0 END), 0) AS completed_size,
			COALESCE(SUM(CASE WHEN status = $2 THEN 1 ELSE 0 END), 0) AS processing_files,
			COALESCE(SUM(CASE WHEN status = $2 THEN total_size ELSE 0 END), 0) AS processing_size,
			COALESCE(SUM(CASE WHEN status = $3 THEN 1 ELSE 0 END), 0) AS aborted_files,
			COALESCE(SUM(CASE WHEN status = $3 THEN total_size ELSE 0 END), 0) AS aborted_size
		FROM logical_file
	`, filestate.LogicalFileCompleted, filestate.LogicalFileProcessing, filestate.LogicalFileAborted).Scan(
		&r.TotalFiles,
		&r.TotalLogicalSizeBytes,
		&r.CompletedFiles,
		&r.CompletedSizeBytes,
		&r.ProcessingFiles,
		&r.ProcessingSizeBytes,
		&r.AbortedFiles,
		&r.AbortedSizeBytes,
	); err != nil {
		return nil, fmt.Errorf("failed to query logical file aggregate stats: %w", err)
	}

	if err := dbconn.QueryRowContext(ctx, `
		SELECT
			COALESCE(SUM(CASE WHEN quarantine = FALSE THEN 1 ELSE 0 END), 0) AS healthy_containers,
			COALESCE(SUM(CASE WHEN quarantine = FALSE THEN current_size ELSE 0 END), 0) AS healthy_size,
			COALESCE(SUM(CASE WHEN quarantine = TRUE THEN 1 ELSE 0 END), 0) AS quarantine_containers,
			COALESCE(SUM(CASE WHEN quarantine = TRUE THEN current_size ELSE 0 END), 0) AS quarantine_size
		FROM container
	`).Scan(&r.HealthyContainers, &r.HealthyContainerBytes, &r.QuarantineContainers, &r.QuarantineContainerBytes); err != nil {
		return nil, fmt.Errorf("failed to query container aggregate stats: %w", err)
	}

	r.TotalContainers = r.HealthyContainers + r.QuarantineContainers
	r.TotalContainerBytes = r.HealthyContainerBytes + r.QuarantineContainerBytes

	if err := dbconn.QueryRowContext(ctx, `
		SELECT
			COALESCE(SUM(CASE WHEN (ch.live_ref_count > 0 OR ch.pin_count > 0) THEN b.stored_size ELSE 0 END),0),
			COALESCE(SUM(CASE WHEN (ch.live_ref_count = 0 AND ch.pin_count = 0) THEN b.stored_size ELSE 0 END),0)
		FROM blocks b
		JOIN chunk ch ON ch.id = b.chunk_id
	`).Scan(&liveBytes, &deadBytes); err != nil {
		return nil, fmt.Errorf("failed to query chunk live/dead stats: %w", err)
	}
	r.LiveBlockBytes = liveBytes.Int64
	r.DeadBlockBytes = deadBytes.Int64

	if r.CompletedSizeBytes > 0 {
		r.GlobalDedupRatioPct = (1.0 - float64(r.LiveBlockBytes)/float64(r.CompletedSizeBytes)) * 100
	}
	if r.HealthyContainerBytes > 0 {
		r.FragmentationRatioPct = float64(r.DeadBlockBytes) / float64(r.HealthyContainerBytes) * 100
	}

	if err := dbconn.QueryRowContext(ctx, `
		SELECT COALESCE(SUM(retry_count),0), COALESCE(AVG(retry_count),0), COALESCE(MAX(retry_count),0)
		FROM logical_file
	`).Scan(&totalFileRetries, &avgFileRetries, &maxFileRetries); err != nil {
		return nil, fmt.Errorf("failed to query logical file retry stats: %w", err)
	}
	r.TotalFileRetries = totalFileRetries.Int64
	r.AvgFileRetries = avgFileRetries.Float64
	r.MaxFileRetries = maxFileRetries.Int64

	if err := dbconn.QueryRowContext(ctx, `
		SELECT COALESCE(SUM(retry_count),0), COALESCE(AVG(retry_count),0), COALESCE(MAX(retry_count),0)
		FROM chunk
	`).Scan(&totalChunkRetries, &avgChunkRetries, &maxChunkRetries); err != nil {
		return nil, fmt.Errorf("failed to query chunk retry stats: %w", err)
	}
	r.TotalChunkRetries = totalChunkRetries.Int64
	r.AvgChunkRetries = avgChunkRetries.Float64
	r.MaxChunkRetries = maxChunkRetries.Int64

	snapshotRetention, err := computeSnapshotRetentionStats(ctx, dbconn)
	if err != nil {
		return nil, fmt.Errorf("failed to compute snapshot retention stats: %w", err)
	}
	r.SnapshotRetention = snapshotRetention

	blockStats, err := CollectBlockStats(ctx, dbconn)
	if err != nil {
		return nil, fmt.Errorf("failed to collect block stats: %w", err)
	}
	r.BlockStats = blockStats

	// Chunk status breakdown
	chunkRows, err := dbconn.QueryContext(ctx, `SELECT status, COUNT(*), COALESCE(SUM(size),0) FROM chunk GROUP BY status`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = chunkRows.Close() }()

	for chunkRows.Next() {
		var status string
		var count, bytes int64
		if err := chunkRows.Scan(&status, &count, &bytes); err != nil {
			return nil, err
		}
		switch status {
		case filestate.ChunkCompleted:
			r.CompletedChunks = count
			r.CompletedChunkBytes = bytes
		case filestate.ChunkProcessing:
			r.ProcessingChunks = count
		case filestate.ChunkAborted:
			r.AbortedChunks = count
		}
	}
	if err := chunkRows.Err(); err != nil {
		return nil, err
	}
	r.TotalChunks = r.CompletedChunks + r.ProcessingChunks + r.AbortedChunks

	if err := dbconn.QueryRowContext(ctx, `
		SELECT
			COUNT(*) AS total_chunk_references,
			COALESCE(COUNT(DISTINCT chunk_id), 0) AS unique_referenced_chunks
		FROM file_chunk
	`).Scan(&r.TotalChunkReferences, &r.UniqueReferencedChunks); err != nil {
		return nil, fmt.Errorf("failed to query dedup signal stats: %w", err)
	}
	if r.TotalChunkReferences > 0 {
		r.EstimatedDedupRatioPct = (1.0 - float64(r.UniqueReferencedChunks)/float64(r.TotalChunkReferences)) * 100
	}

	versionRows, err := dbconn.QueryContext(ctx, `
		SELECT chunker_version, COUNT(*), COALESCE(SUM(size),0)
		FROM chunk
		GROUP BY chunker_version
	`)
	if err != nil {
		return nil, fmt.Errorf("failed to query chunk counts by version: %w", err)
	}
	defer func() { _ = versionRows.Close() }()

	r.ChunkCountsByVersion = make(map[string]int64)
	r.ChunkBytesByVersion = make(map[string]int64)
	for versionRows.Next() {
		var version string
		var count int64
		var bytes int64
		if err := versionRows.Scan(&version, &count, &bytes); err != nil {
			return nil, err
		}
		bucket := normalizeChunkerVersionBucket(version)
		r.ChunkCountsByVersion[bucket] += count
		r.ChunkBytesByVersion[bucket] += bytes
	}
	if err := versionRows.Err(); err != nil {
		return nil, err
	}

	logicalVersionRows, err := dbconn.QueryContext(ctx, `
		SELECT chunker_version, COUNT(*)
		FROM logical_file
		GROUP BY chunker_version
	`)
	if err != nil {
		return nil, fmt.Errorf("failed to query logical file counts by version: %w", err)
	}
	defer func() { _ = logicalVersionRows.Close() }()

	r.LogicalFileCountsByVersion = make(map[string]int64)
	for logicalVersionRows.Next() {
		var version string
		var count int64
		if err := logicalVersionRows.Scan(&version, &count); err != nil {
			return nil, err
		}
		bucket := normalizeChunkerVersionBucket(version)
		r.LogicalFileCountsByVersion[bucket] += count
	}
	if err := logicalVersionRows.Err(); err != nil {
		return nil, err
	}

	// Per-container breakdown
	ctrRows, err := dbconn.QueryContext(ctx, `
		SELECT
			c.id,
			c.filename,
			c.current_size,
			COALESCE(SUM(CASE WHEN (ch.live_ref_count > 0 OR ch.pin_count > 0) THEN b.stored_size ELSE 0 END),0) AS live,
			COALESCE(SUM(CASE WHEN (ch.live_ref_count = 0 AND ch.pin_count = 0) THEN b.stored_size ELSE 0 END),0) AS dead,
			c.quarantine
		FROM container c
		LEFT JOIN blocks b ON b.container_id = c.id
		LEFT JOIN chunk ch ON ch.id = b.chunk_id
		GROUP BY c.id
		ORDER BY c.id
	`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = ctrRows.Close() }()

	for ctrRows.Next() {
		var c ContainerStatRecord
		if err := ctrRows.Scan(&c.ID, &c.Filename, &c.TotalBytes, &c.LiveBytes, &c.DeadBytes, &c.Quarantine); err != nil {
			return nil, err
		}
		if c.TotalBytes > 0 {
			c.LiveRatioPct = float64(c.LiveBytes) / float64(c.TotalBytes) * 100
		}
		r.Containers = append(r.Containers, c)
	}
	if err := ctrRows.Err(); err != nil {
		return nil, err
	}

	return r, nil
}

func blockTargetSizeBytesForStats() int64 {
	const defaultTargetBytes int64 = 1 << 20
	const defaultTargetMB int64 = 1

	targetMB := int64(0)
	if _, ok := lookupEnv("COLDKEEP_BLOCK_TARGET_SIZE_MB"); ok {
		targetMB = utils_env.GetenvOrDefaultInt64("COLDKEEP_BLOCK_TARGET_SIZE_MB", defaultTargetMB)
	} else {
		targetMB = utils_env.GetenvOrDefaultInt64("COLDKEEP_PACKED_BLOCK_SIZE_MIB", defaultTargetMB)
	}

	if targetMB <= 0 {
		return defaultTargetBytes
	}
	if targetMB > (1<<63-1)/(1<<20) {
		return defaultTargetBytes
	}
	return targetMB << 20
}

var lookupEnv = func(key string) (string, bool) {
	return os.LookupEnv(key)
}

// CollectBlockStats gathers packed/legacy block metrics used in stats and
// benchmarking analysis.
func CollectBlockStats(ctx context.Context, dbconn *sql.DB) (BlockStats, error) {
	if dbconn == nil {
		return BlockStats{}, fmt.Errorf("db connection is nil")
	}
	if ctx == nil {
		ctx = context.Background()
	}

	out := BlockStats{
		CodecDistribution:         map[string]int64{},
		CompressionCodecBreakdown: map[string]int64{},
	}

	if err := dbconn.QueryRowContext(ctx, `SELECT COUNT(*) FROM storage_blocks`).Scan(&out.StorageBlocks); err != nil {
		return BlockStats{}, fmt.Errorf("count storage_blocks: %w", err)
	}
	out.PackedBlocks = out.StorageBlocks

	if err := dbconn.QueryRowContext(ctx, `SELECT COUNT(*) FROM chunk_block_refs`).Scan(&out.ChunkBlockRefs); err != nil {
		return BlockStats{}, fmt.Errorf("count chunk_block_refs: %w", err)
	}

	if err := dbconn.QueryRowContext(ctx, `SELECT COUNT(*) FROM blocks`).Scan(&out.LegacyBlocks); err != nil {
		return BlockStats{}, fmt.Errorf("count legacy blocks: %w", err)
	}

	if err := dbconn.QueryRowContext(ctx, `
		SELECT
			COALESCE(AVG(plaintext_size), 0),
			COALESCE(AVG(stored_size), 0)
		FROM storage_blocks
	`).Scan(&out.AvgPlaintextSize, &out.AvgStoredSize); err != nil {
		return BlockStats{}, fmt.Errorf("avg packed block sizes: %w", err)
	}

	if err := dbconn.QueryRowContext(ctx, `
		SELECT
			COALESCE(SUM(plaintext_size), 0),
			COALESCE(SUM(COALESCE(compressed_size, CASE WHEN COALESCE(compression_codec, 'none') = 'none' THEN plaintext_size END, stored_size)), 0),
			COALESCE(SUM(stored_size), 0)
		FROM storage_blocks
	`).Scan(&out.LogicalBytes, &out.CompressedBytes, &out.StoredBytes); err != nil {
		return BlockStats{}, fmt.Errorf("aggregate compression sizes: %w", err)
	}

	if err := dbconn.QueryRowContext(ctx, `
		SELECT
			COALESCE(SUM(CASE WHEN lower(trim(COALESCE(compression_codec, 'none'))) != 'none' THEN 1 ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN lower(trim(COALESCE(compression_codec, 'none'))) = 'none' THEN 1 ELSE 0 END), 0)
		FROM storage_blocks
	`).Scan(&out.CompressedBlocks, &out.UncompressedBlocks); err != nil {
		return BlockStats{}, fmt.Errorf("aggregate compressed/uncompressed block counts: %w", err)
	}
	if out.LogicalBytes > 0 {
		out.CompressionSizeRatio = float64(out.CompressedBytes) / float64(out.LogicalBytes)
		out.PhysicalSizeRatio = float64(out.StoredBytes) / float64(out.LogicalBytes)
	}
	if out.CompressedBytes > 0 {
		out.CompressionFactor = float64(out.LogicalBytes) / float64(out.CompressedBytes)
	}
	if out.StoredBytes > 0 {
		out.PhysicalFactor = float64(out.LogicalBytes) / float64(out.StoredBytes)
	}

	if out.StorageBlocks > 0 {
		out.AvgChunksPerBlock = float64(out.ChunkBlockRefs) / float64(out.StorageBlocks)
	}

	// FillRatio is calculated using the currently configured target block size
	// (from COLDKEEP_BLOCK_TARGET_SIZE_MB or COLDKEEP_PACKED_BLOCK_SIZE_MIB),
	// not the historical target size used when each block was written.
	// If the env override has been changed historically, FillRatio may not
	// accurately reflect the packing efficiency of existing blocks.
	// Operators should note any block-size overrides when interpreting this metric.
	// For decision-grade analysis, prefer reviewing individual block sizes from
	// the storage_blocks table (plaintext_size, stored_size) directly.
	targetBlockSizeBytes := blockTargetSizeBytesForStats()
	if targetBlockSizeBytes > 0 {
		out.FillRatio = out.AvgPlaintextSize / float64(targetBlockSizeBytes)
	}

	rows, err := dbconn.QueryContext(ctx, `
		SELECT codec, COUNT(*)
		FROM storage_blocks
		GROUP BY codec
		ORDER BY codec
	`)
	if err != nil {
		return BlockStats{}, fmt.Errorf("codec distribution: %w", err)
	}
	defer func() { _ = rows.Close() }()

	for rows.Next() {
		var codec string
		var count int64
		if err := rows.Scan(&codec, &count); err != nil {
			return BlockStats{}, fmt.Errorf("scan codec distribution: %w", err)
		}
		out.CodecDistribution[codec] = count
	}
	if err := rows.Err(); err != nil {
		return BlockStats{}, fmt.Errorf("iterate codec distribution: %w", err)
	}

	compressionCodecRows, err := dbconn.QueryContext(ctx, `
		SELECT lower(trim(COALESCE(compression_codec, 'none'))) AS compression_codec, COUNT(*)
		FROM storage_blocks
		GROUP BY lower(trim(COALESCE(compression_codec, 'none')))
		ORDER BY compression_codec
	`)
	if err != nil {
		return BlockStats{}, fmt.Errorf("compression codec breakdown: %w", err)
	}
	defer func() { _ = compressionCodecRows.Close() }()

	for compressionCodecRows.Next() {
		var codec string
		var count int64
		if err := compressionCodecRows.Scan(&codec, &count); err != nil {
			return BlockStats{}, fmt.Errorf("scan compression codec breakdown: %w", err)
		}
		if codec == "" {
			codec = "none"
		}
		out.CompressionCodecBreakdown[codec] = count
	}
	if err := compressionCodecRows.Err(); err != nil {
		return BlockStats{}, fmt.Errorf("iterate compression codec breakdown: %w", err)
	}

	return out, nil
}

func isRegisteredChunkerVersion(version chunk.Version) bool {
	defer func() {
		_ = recover()
	}()
	_ = chunk.DefaultRegistry().MustGet(version)
	return true
}

func resolveActiveWriteChunker(ctx context.Context, dbconn *sql.DB) (string, error) {
	var raw string
	err := dbconn.QueryRowContext(
		ctx,
		`SELECT value FROM repository_config WHERE key = $1`,
		"default_chunker",
	).Scan(&raw)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return string(chunk.DefaultChunkerVersion), nil
		}
		return "", fmt.Errorf("failed to query active write chunker: %w", err)
	}

	version := chunk.Version(strings.TrimSpace(raw))
	if !chunk.IsWellFormedVersion(version) || !isRegisteredChunkerVersion(version) {
		return unknownChunkerBucket, nil
	}

	return string(version), nil
}

func computeSnapshotRetentionStats(ctx context.Context, dbconn *sql.DB) (SnapshotRetentionStats, error) {
	summary, err := retention.ComputeReachabilitySummary(ctx, dbconn)
	if err != nil {
		return SnapshotRetentionStats{}, err
	}

	snapshotReferencedCount, err := retention.CountSnapshotReferencedLogicalFiles(ctx, dbconn)
	if err != nil {
		return SnapshotRetentionStats{}, err
	}

	snapshotOnlyCount, err := retention.CountSnapshotOnlyLogicalFiles(ctx, dbconn)
	if err != nil {
		return SnapshotRetentionStats{}, err
	}

	snapshotReferencedBytes, err := retention.SumSnapshotReferencedLogicalBytes(ctx, dbconn)
	if err != nil {
		return SnapshotRetentionStats{}, err
	}

	rows, err := dbconn.QueryContext(ctx, `SELECT id, total_size FROM logical_file`)
	if err != nil {
		return SnapshotRetentionStats{}, err
	}
	defer func() { _ = rows.Close() }()

	stats := SnapshotRetentionStats{
		SnapshotReferencedLogicalFiles: snapshotReferencedCount,
		SnapshotReferencedBytes:        snapshotReferencedBytes,
		SnapshotOnlyLogicalFiles:       snapshotOnlyCount,
		SharedLogicalFiles:             snapshotReferencedCount - snapshotOnlyCount,
	}
	for rows.Next() {
		var logicalFileID int64
		var totalSize int64
		if err := rows.Scan(&logicalFileID, &totalSize); err != nil {
			return SnapshotRetentionStats{}, err
		}

		_, currentReferenced := summary.CurrentLogicalIDs[logicalFileID]
		_, snapshotReferenced := summary.SnapshotLogicalIDs[logicalFileID]

		switch {
		case currentReferenced && snapshotReferenced:
			stats.SharedBytes += totalSize
		case snapshotReferenced:
			stats.SnapshotOnlyBytes += totalSize
		case currentReferenced:
			stats.CurrentOnlyLogicalFiles++
			stats.CurrentOnlyBytes += totalSize
		}
	}

	if err := rows.Err(); err != nil {
		return SnapshotRetentionStats{}, err
	}

	return stats, nil
}
