package maintenance

import (
	"database/sql"
	"fmt"

	"github.com/franchoy/coldkeep/internal/db"
)

func bytesToMB(bytes int64) float64 {
	return float64(bytes) / (1024 * 1024)
}

func RunStats() error {
	db, err := db.ConnectDB()
	if err != nil {
		return fmt.Errorf("Failed to connect to DB: %w", err)
	}
	defer db.Close()

	var totalFiles int64
	var totalLogicalSize sql.NullInt64
	var completedFiles int64
	var completedLogicalSize sql.NullInt64
	var processingFiles int64
	var processingLogicalSize sql.NullInt64
	var abortedFiles int64
	var abortedLogicalSize sql.NullInt64
	var healthyContainers int64
	var quarantinedContainers int64
	var totalContainers int64
	var healthyContainerSize sql.NullInt64
	var healthyCompressedSize sql.NullInt64
	var quarantinedContainerSize sql.NullInt64
	var quarantinedCompressedSize sql.NullInt64

	var totalContainerSize sql.NullInt64
	var totalCompressedSize sql.NullInt64
	var liveBytes sql.NullInt64
	var deadBytes sql.NullInt64

	// Logical file stats - total
	err = db.QueryRow(`SELECT COUNT(*), COALESCE(SUM(total_size),0) FROM logical_file`).
		Scan(&totalFiles, &totalLogicalSize)
	if err != nil {
		return fmt.Errorf("Failed to query total logical files: %w", err)
	}

	// Logical file stats - completed
	err = db.QueryRow(`SELECT COUNT(*), COALESCE(SUM(total_size),0) FROM logical_file WHERE status = 'COMPLETED'`).
		Scan(&completedFiles, &completedLogicalSize)
	if err != nil {
		return fmt.Errorf("Failed to query completed logical files: %w", err)
	}

	// Logical file stats - processing
	err = db.QueryRow(`SELECT COUNT(*), COALESCE(SUM(total_size),0) FROM logical_file WHERE status = 'PROCESSING'`).
		Scan(&processingFiles, &processingLogicalSize)
	if err != nil {
		return fmt.Errorf("Failed to query processing logical files: %w", err)
	}

	// Logical file stats - aborted
	err = db.QueryRow(`SELECT COUNT(*), COALESCE(SUM(total_size),0) FROM logical_file WHERE status = 'ABORTED'`).
		Scan(&abortedFiles, &abortedLogicalSize)
	if err != nil {
		return fmt.Errorf("Failed to query aborted logical files: %w", err)
	}

	// healthy Container stats
	err = db.QueryRow(`
		SELECT COUNT(*),
		       COALESCE(SUM(current_size),0),
		       COALESCE(SUM(compressed_size),0)
		FROM container
		WHERE quarantine = FALSE
	`).Scan(&healthyContainers, &healthyContainerSize, &healthyCompressedSize)
	if err != nil {
		return fmt.Errorf("Failed to query healthy containers: %w", err)
	}

	// quarantined Container stats
	err = db.QueryRow(`
		SELECT COUNT(*),
		       COALESCE(SUM(current_size),0),
		       COALESCE(SUM(compressed_size),0)
		FROM container
		WHERE quarantine = TRUE
	`).Scan(&quarantinedContainers, &quarantinedContainerSize, &quarantinedCompressedSize)
	if err != nil {
		return fmt.Errorf("Failed to query quarantined containers: %w", err)
	}

	// Total container stats
	totalContainers = healthyContainers + quarantinedContainers
	totalContainerSize = sql.NullInt64{
		Int64: healthyContainerSize.Int64 + quarantinedContainerSize.Int64,
		Valid: healthyContainerSize.Valid || quarantinedContainerSize.Valid,
	}
	totalCompressedSize = sql.NullInt64{
		Int64: healthyCompressedSize.Int64 + quarantinedCompressedSize.Int64,
		Valid: healthyCompressedSize.Valid || quarantinedCompressedSize.Valid,
	}

	// Chunk live/dead stats
	err = db.QueryRow(`
		SELECT
			COALESCE(SUM(CASE WHEN ref_count > 0 THEN size ELSE 0 END),0),
			COALESCE(SUM(CASE WHEN ref_count = 0 THEN size ELSE 0 END),0)
		FROM chunk
	`).Scan(&liveBytes, &deadBytes)
	if err != nil {
		return fmt.Errorf("Failed to query chunk live/dead stats: %w", err)
	}

	fmt.Println("\n====== coldkeep Stats ======")

	fmt.Printf("Logical files (total):           %d\n", totalFiles)
	fmt.Printf("Logical stored size (total):     %.2f MB\n", bytesToMB(totalLogicalSize.Int64))
	fmt.Printf("  Completed files:               %d (%.2f MB)\n", completedFiles, bytesToMB(completedLogicalSize.Int64))
	fmt.Printf("  Processing files:              %d (%.2f MB)\n", processingFiles, bytesToMB(processingLogicalSize.Int64))
	fmt.Printf("  Aborted files:                 %d (%.2f MB)\n", abortedFiles, bytesToMB(abortedLogicalSize.Int64))
	fmt.Printf("Healthy containers:              %d\n", healthyContainers)
	fmt.Printf("Healthy container bytes:         %.2f MB\n", bytesToMB(healthyContainerSize.Int64))
	fmt.Printf("Healthy compressed bytes:        %.2f MB\n", bytesToMB(healthyCompressedSize.Int64))
	fmt.Printf("Quarantined containers:          %d\n", quarantinedContainers)
	fmt.Printf("Quarantined container bytes:     %.2f MB\n", bytesToMB(quarantinedContainerSize.Int64))
	fmt.Printf("Quarantined compressed bytes:    %.2f MB\n", bytesToMB(quarantinedCompressedSize.Int64))
	fmt.Printf("Total containers:                %d\n", totalContainers)
	fmt.Printf("Total container bytes:           %.2f MB\n", bytesToMB(totalContainerSize.Int64))
	fmt.Printf("Total compressed bytes:          %.2f MB\n", bytesToMB(totalCompressedSize.Int64))

	fmt.Printf("Live chunk bytes:                %.2f MB\n", bytesToMB(liveBytes.Int64))
	fmt.Printf("Dead chunk bytes:                %.2f MB\n", bytesToMB(deadBytes.Int64))

	if completedLogicalSize.Int64 > 0 {
		dedupRatio := 1.0 - (float64(liveBytes.Int64) / float64(completedLogicalSize.Int64))
		fmt.Printf("Global dedup ratio:              %.2f%%\n", dedupRatio*100)
	}

	if totalContainerSize.Int64 > 0 {
		deadRatio := float64(deadBytes.Int64) / float64(healthyContainerSize.Int64)
		fmt.Printf("Fragmentation ratio:             %.2f%%\n", deadRatio*100)
	}

	if totalLogicalSize.Int64 > 0 && totalCompressedSize.Int64 > 0 {
		compressionRatio := float64(totalLogicalSize.Int64) / float64(totalCompressedSize.Int64)
		fmt.Printf("Compression ratio:               %.2f\n", compressionRatio)
	}

	fmt.Println("============================")

	type chunkStats struct {
    status string
    count  int64
    bytes  int64
	}

	rows, err := db.Query(`
		SELECT status, COUNT(*), COALESCE(SUM(size),0)
		FROM chunk
		GROUP BY status`)
	if err != nil {
		return err
	}
	defer rows.Close()

	var completedCount, processingCount, abortedCount int64
	var completedBytes int64

	for rows.Next() {
		var status string
		var count int64
		var bytes int64

		if err := rows.Scan(&status, &count, &bytes); err != nil {
			return err
		}

		switch status {
		case "COMPLETED":
			completedCount = count
			completedBytes = bytes
		case "PROCESSING":
			processingCount = count
		case "ABORTED":
			abortedCount = count
		}
	}

	fmt.Printf("Chunks (total):           %d\n", completedCount+processingCount+abortedCount)
	fmt.Printf("  Completed chunks:       %d (%.2f MB)\n", completedCount, bytesToMB(completedBytes))
	fmt.Printf("  Processing chunks:      %d\n", processingCount)
	fmt.Printf("  Aborted chunks:         %d\n", abortedCount)

	fmt.Println("============================")

	// ---- Per container breakdown ----
	fmt.Println("\nPer-container breakdown:")

	rows, err := db.Query(`
		SELECT
			c.id,
			c.filename,
			c.current_size,
			COALESCE(SUM(CASE WHEN ch.ref_count > 0 THEN ch.size ELSE 0 END),0) AS live,
			COALESCE(SUM(CASE WHEN ch.ref_count = 0 THEN ch.size ELSE 0 END),0) AS dead,
			c.quarantine
		FROM container c
		LEFT JOIN chunk ch ON ch.container_id = c.id
		GROUP BY c.id
		ORDER BY c.id
	`)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var id int64
		var filename string
		var totalSize int64
		var live int64
		var dead int64
		var quarantine bool

		if err := rows.Scan(&id, &filename, &totalSize, &live, &dead, &quarantine); err != nil {
			return err
		}

		liveRatio := 0.0
		if totalSize > 0 {
			liveRatio = float64(live) / float64(totalSize) * 100
		}

		fmt.Printf("Container %d (%s): quarantined=%t : total=%.2fMB live=%.2fMB dead=%.2fMB live_ratio=%.2f%%\n",
			id,
			filename,
			quarantine,
			float64(totalSize)/(1024*1024),
			float64(live)/(1024*1024),
			float64(dead)/(1024*1024),
			liveRatio,
		)
	}
	return nil
}
