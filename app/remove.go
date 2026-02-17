package main

import (
	"fmt"
	"log"
)

func removeFile(fileID string) {
	db := connectDB()
	defer db.Close()

	// Check existence first
	var exists bool
	err := db.QueryRow(
		"SELECT EXISTS (SELECT 1 FROM logical_file WHERE id=$1)",
		fileID,
	).Scan(&exists)
	if err != nil {
		log.Fatal(err)
	}

	if !exists {
		fmt.Println("File ID", fileID, "not found.")
		return
	}

	tx, err := db.Begin()
	if err != nil {
		log.Fatal(err)
	}

	// Get chunk IDs
	rows, err := tx.Query(`
		SELECT chunk_id
		FROM file_chunk
		WHERE logical_file_id = $1
	`, fileID)
	if err != nil {
		_ = tx.Rollback()
		log.Fatal(err)
	}
	defer rows.Close()

	var chunkIDs []int
	for rows.Next() {
		var id int
		_ = rows.Scan(&id)
		chunkIDs = append(chunkIDs, id)
	}

	// Decrement ref_count
	for _, chunkID := range chunkIDs {
		_, err := tx.Exec(`
			UPDATE chunk
			SET ref_count = ref_count - 1
			WHERE id = $1
		`, chunkID)
		if err != nil {
			_ = tx.Rollback()
			log.Fatal(err)
		}
	}

	// Remove mappings
	_, err = tx.Exec(`DELETE FROM file_chunk WHERE logical_file_id = $1`, fileID)
	if err != nil {
		_ = tx.Rollback()
		log.Fatal(err)
	}

	// Remove logical file
	_, err = tx.Exec(`DELETE FROM logical_file WHERE id = $1`, fileID)
	if err != nil {
		_ = tx.Rollback()
		log.Fatal(err)
	}

	if err := tx.Commit(); err != nil {
		log.Fatal(err)
	}

	fmt.Println("Logical file", fileID, "removed. Ref counts updated.")
}
