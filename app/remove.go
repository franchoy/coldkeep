package main

import (
	"fmt"
)

func removeFile(fileID string) error {
	db, err := connectDB()
	if err != nil {
		return fmt.Errorf("Failed to connect to DB: %w", err)
	}
	defer db.Close()

	// Check existence first
	var exists bool
	err = db.QueryRow(
		"SELECT EXISTS (SELECT 1 FROM logical_file WHERE id=$1)",
		fileID,
	).Scan(&exists)
	if err != nil {
		return err
	}

	if !exists {
		fmt.Println("File ID", fileID, "not found.")
		return fmt.Errorf("file ID %s not found", fileID)
	}

	tx, err := db.Begin()
	if err != nil {
		return err
	}

	// Get chunk IDs
	rows, err := tx.Query(`
		SELECT chunk_id
		FROM file_chunk
		WHERE logical_file_id = $1
	`, fileID)
	if err != nil {
		_ = tx.Rollback()
		return err
	}
	defer rows.Close()

	var chunkIDs []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			_ = tx.Rollback()
			return err
		}
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
			return err
		}
	}

	// Remove mappings
	_, err = tx.Exec(`DELETE FROM file_chunk WHERE logical_file_id = $1`, fileID)
	if err != nil {
		_ = tx.Rollback()
		return err
	}

	// Remove logical file
	_, err = tx.Exec(`DELETE FROM logical_file WHERE id = $1`, fileID)
	if err != nil {
		_ = tx.Rollback()
		return err
	}

	if err := tx.Commit(); err != nil {
		return err
	}

	fmt.Println("Logical file", fileID, "removed. Ref counts updated.")
	return nil
}
