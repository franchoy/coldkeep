package main

import (
	"database/sql"
	"fmt"
	"log"
)

func runStats() {
	db := connectDB()
	defer db.Close()

	var totalFiles int
	var totalLogicalSize sql.NullInt64
	var totalContainers int
	var totalContainerSize sql.NullInt64
	var totalCompressedSize sql.NullInt64
	var liveBytes sql.NullInt64
	var deadBytes sql.NullInt64

	// Logical file stats
	db.QueryRow(`SELECT COUNT(*), COALESCE(SUM(total_size),0) FROM logical_file`).
		Scan(&totalFiles, &totalLogicalSize)

	// Container stats
	db.QueryRow(`
		SELECT COUNT(*),
		       COALESCE(SUM(current_size),0),
		       COALESCE(SUM(compressed_size),0)
		FROM container
	`).Scan(&totalContainers, &totalContainerSize, &totalCompressedSize)

	// Chunk live/dead stats
	db.QueryRow(`
		SELECT
			COALESCE(SUM(CASE WHEN ref_count > 0 THEN size ELSE 0 END),0),
			COALESCE(SUM(CASE WHEN ref_count = 0 THEN size ELSE 0 END),0)
		FROM chunk
	`).Scan(&liveBytes, &deadBytes)

	fmt.Println("\n====== Capsule Stats ======")

	fmt.Printf("Logical files:           %d\n", totalFiles)
	fmt.Printf("Logical stored size:     %.2f MB\n", float64(totalLogicalSize.Int64)/(1024*1024))

	fmt.Printf("Containers:              %d\n", totalContainers)
	fmt.Printf("Raw container bytes:     %.2f MB\n", float64(totalContainerSize.Int64)/(1024*1024))
	fmt.Printf("Compressed bytes:        %.2f MB\n", float64(totalCompressedSize.Int64)/(1024*1024))

	fmt.Printf("Live chunk bytes:        %.2f MB\n", float64(liveBytes.Int64)/(1024*1024))
	fmt.Printf("Dead chunk bytes:        %.2f MB\n", float64(deadBytes.Int64)/(1024*1024))

	if totalLogicalSize.Int64 > 0 {
		dedupRatio := 1.0 - (float64(liveBytes.Int64) / float64(totalLogicalSize.Int64))
		fmt.Printf("Global dedup ratio:      %.2f%%\n", dedupRatio*100)
	}

	if totalContainerSize.Int64 > 0 {
		deadRatio := float64(deadBytes.Int64) / float64(totalContainerSize.Int64)
		fmt.Printf("Fragmentation ratio:     %.2f%%\n", deadRatio*100)
	}

	fmt.Println("============================")

	// ---- Per container breakdown ----
	fmt.Println("\nPer-container breakdown:")

	rows, err := db.Query(`
		SELECT
			c.id,
			c.filename,
			c.current_size,
			COALESCE(SUM(CASE WHEN ch.ref_count > 0 THEN ch.size ELSE 0 END),0) AS live,
			COALESCE(SUM(CASE WHEN ch.ref_count = 0 THEN ch.size ELSE 0 END),0) AS dead
		FROM container c
		LEFT JOIN chunk ch ON ch.container_id = c.id
		GROUP BY c.id
		ORDER BY c.id
	`)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	for rows.Next() {
		var id int
		var filename string
		var totalSize int64
		var live int64
		var dead int64

		_ = rows.Scan(&id, &filename, &totalSize, &live, &dead)

		liveRatio := 0.0
		if totalSize > 0 {
			liveRatio = float64(live) / float64(totalSize) * 100
		}

		fmt.Printf("Container %d (%s): total=%.2fMB live=%.2fMB dead=%.2fMB live_ratio=%.2f%%\n",
			id,
			filename,
			float64(totalSize)/(1024*1024),
			float64(live)/(1024*1024),
			float64(dead)/(1024*1024),
			liveRatio,
		)
	}
}
