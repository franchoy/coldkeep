package maintenance

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/franchoy/coldkeep/internal/db"
	filestate "github.com/franchoy/coldkeep/internal/status"
)

// StatsResult holds the snapshot emitted by RunStatsResult.
type StatsResult struct {
	TotalFiles               int64                 `json:"total_files"`
	TotalLogicalSizeBytes    int64                 `json:"total_logical_size_bytes"`
	CompletedFiles           int64                 `json:"completed_files"`
	CompletedSizeBytes       int64                 `json:"completed_size_bytes"`
	ProcessingFiles          int64                 `json:"processing_files"`
	ProcessingSizeBytes      int64                 `json:"processing_size_bytes"`
	AbortedFiles             int64                 `json:"aborted_files"`
	AbortedSizeBytes         int64                 `json:"aborted_size_bytes"`
	HealthyContainers        int64                 `json:"healthy_containers"`
	HealthyContainerBytes    int64                 `json:"healthy_container_bytes"`
	QuarantineContainers     int64                 `json:"quarantine_containers"`
	QuarantineContainerBytes int64                 `json:"quarantine_container_bytes"`
	TotalContainers          int64                 `json:"total_containers"`
	TotalContainerBytes      int64                 `json:"total_container_bytes"`
	LiveBlockBytes           int64                 `json:"live_block_bytes"`
	DeadBlockBytes           int64                 `json:"dead_block_bytes"`
	GlobalDedupRatioPct      float64               `json:"global_dedup_ratio_pct"`
	FragmentationRatioPct    float64               `json:"fragmentation_ratio_pct"`
	TotalChunks              int64                 `json:"total_chunks"`
	CompletedChunks          int64                 `json:"completed_chunks"`
	CompletedChunkBytes      int64                 `json:"completed_chunk_bytes"`
	ProcessingChunks         int64                 `json:"processing_chunks"`
	AbortedChunks            int64                 `json:"aborted_chunks"`
	TotalFileRetries         int64                 `json:"total_file_retries"`
	AvgFileRetries           float64               `json:"avg_file_retries"`
	MaxFileRetries           int64                 `json:"max_file_retries"`
	TotalChunkRetries        int64                 `json:"total_chunk_retries"`
	AvgChunkRetries          float64               `json:"avg_chunk_retries"`
	MaxChunkRetries          int64                 `json:"max_chunk_retries"`
	Containers               []ContainerStatRecord `json:"containers"`
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

	r := &StatsResult{}

	var totalLogical, completedLogical, processingLogical, abortedLogical sql.NullInt64
	var healthySize, quarantineSize, totalContainerSize, liveBytes, deadBytes sql.NullInt64
	var totalFileRetries, maxFileRetries, totalChunkRetries, maxChunkRetries sql.NullInt64
	var avgFileRetries, avgChunkRetries sql.NullFloat64

	if err := dbconn.QueryRowContext(ctx, `SELECT COUNT(*), COALESCE(SUM(total_size),0) FROM logical_file`).
		Scan(&r.TotalFiles, &totalLogical); err != nil {
		return nil, fmt.Errorf("failed to query total logical files: %w", err)
	}
	r.TotalLogicalSizeBytes = totalLogical.Int64

	if err := dbconn.QueryRowContext(ctx, `SELECT COUNT(*), COALESCE(SUM(total_size),0) FROM logical_file WHERE status = $1`, filestate.LogicalFileCompleted).
		Scan(&r.CompletedFiles, &completedLogical); err != nil {
		return nil, fmt.Errorf("failed to query completed logical files: %w", err)
	}
	r.CompletedSizeBytes = completedLogical.Int64

	if err := dbconn.QueryRowContext(ctx, `SELECT COUNT(*), COALESCE(SUM(total_size),0) FROM logical_file WHERE status = $1`, filestate.LogicalFileProcessing).
		Scan(&r.ProcessingFiles, &processingLogical); err != nil {
		return nil, fmt.Errorf("failed to query processing logical files: %w", err)
	}
	r.ProcessingSizeBytes = processingLogical.Int64

	if err := dbconn.QueryRowContext(ctx, `SELECT COUNT(*), COALESCE(SUM(total_size),0) FROM logical_file WHERE status = $1`, filestate.LogicalFileAborted).
		Scan(&r.AbortedFiles, &abortedLogical); err != nil {
		return nil, fmt.Errorf("failed to query aborted logical files: %w", err)
	}
	r.AbortedSizeBytes = abortedLogical.Int64

	if err := dbconn.QueryRowContext(ctx, `SELECT COUNT(*), COALESCE(SUM(current_size),0) FROM container WHERE quarantine = FALSE`).
		Scan(&r.HealthyContainers, &healthySize); err != nil {
		return nil, fmt.Errorf("failed to query healthy containers: %w", err)
	}
	r.HealthyContainerBytes = healthySize.Int64

	if err := dbconn.QueryRowContext(ctx, `SELECT COUNT(*), COALESCE(SUM(current_size),0) FROM container WHERE quarantine = TRUE`).
		Scan(&r.QuarantineContainers, &quarantineSize); err != nil {
		return nil, fmt.Errorf("failed to query quarantined containers: %w", err)
	}
	r.QuarantineContainerBytes = quarantineSize.Int64

	r.TotalContainers = r.HealthyContainers + r.QuarantineContainers
	totalContainerSize = sql.NullInt64{Int64: r.HealthyContainerBytes + r.QuarantineContainerBytes, Valid: true}
	r.TotalContainerBytes = totalContainerSize.Int64

	if err := dbconn.QueryRowContext(ctx, `
		SELECT
			COALESCE(SUM(CASE WHEN ch.ref_count > 0 THEN b.stored_size ELSE 0 END),0),
			COALESCE(SUM(CASE WHEN ch.ref_count = 0 THEN b.stored_size ELSE 0 END),0)
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

	// Per-container breakdown
	ctrRows, err := dbconn.QueryContext(ctx, `
		SELECT
			c.id,
			c.filename,
			c.current_size,
			COALESCE(SUM(CASE WHEN ch.ref_count > 0 THEN b.stored_size ELSE 0 END),0) AS live,
			COALESCE(SUM(CASE WHEN ch.ref_count = 0 THEN b.stored_size ELSE 0 END),0) AS dead,
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
