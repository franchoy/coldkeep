package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
)

func runGC() {
	db := connectDB()
	defer db.Close()

	rows, err := db.Query(`
		SELECT c.id, c.filename, c.compression_algorithm
		FROM container c
		WHERE NOT EXISTS (
			SELECT 1 FROM chunk ch
			WHERE ch.container_id = c.id
			AND ch.ref_count > 0
		)
	`)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	var deletedContainers int

	for rows.Next() {
		var containerID int64
		var filename string
		var algo string

		if err := rows.Scan(&containerID, &filename, &algo); err != nil {
			log.Fatal(err)
		}

		containerPath := filepath.Join(storageDir, filename)
		if algo != "" && algo != string(CompressionNone) {
			containerPath += "." + algo
		}

		// Delete file from disk
		if err := os.Remove(containerPath); err != nil {
			log.Println("Failed to delete file:", err)
			continue
		}

		// Delete chunk rows
		_, err = db.Exec(`DELETE FROM chunk WHERE container_id = $1`, containerID)
		if err != nil {
			log.Fatal(err)
		}

		// Delete container row
		_, err = db.Exec(`DELETE FROM container WHERE id = $1`, containerID)
		if err != nil {
			log.Fatal(err)
		}

		deletedContainers++
		fmt.Println("Deleted container:", filename)
	}

	fmt.Printf("GC completed. Containers deleted: %d\n", deletedContainers)
}
