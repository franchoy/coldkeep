package main

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

type chunkRef struct {
	containerFilename string
	algo              string
	chunkOffset       int64 // start of record
	chunkSize         int   // data size only
	sha256            string
}

func restoreFile(id int64, outputPath string) error {
	db, err := connectDB()
	if err != nil {
		return fmt.Errorf("Failed to connect to DB: %w", err)
	}
	defer db.Close()

	if err := restoreFileWithDB(db, id, outputPath); err != nil {
		return err
	}
	return nil
}

func restoreFileWithDB(db *sql.DB, fileID int64, outputPath string) error {

	// ------------------------------------------------------------
	// Fetch logical file metadata
	// ------------------------------------------------------------
	var expectedFileHash string
	var originalName string

	err := db.QueryRow(
		"SELECT original_name, file_hash FROM logical_file WHERE id = $1",
		fileID,
	).Scan(&originalName, &expectedFileHash)

	if err == sql.ErrNoRows {
		return fmt.Errorf("logical file %d not found", fileID)
	}
	if err != nil {
		return fmt.Errorf("query logical_file: %w", err)
	}

	// ------------------------------------------------------------
	// Fetch chunks in correct order
	// ------------------------------------------------------------
	rows, err := db.Query(`
		SELECT c.container_id, c.chunk_offset, c.size, ct.filename
		FROM file_chunk fc
		JOIN chunk c ON fc.chunk_id = c.id
		JOIN container ct ON c.container_id = ct.id
		WHERE fc.logical_file_id = $1
		ORDER BY fc.chunk_order ASC
	`, fileID)

	if err != nil {
		return fmt.Errorf("query file chunks: %w", err)
	}
	defer rows.Close()

	// ------------------------------------------------------------
	// Prepare output file
	// ------------------------------------------------------------
	if st, err := os.Stat(outputPath); err == nil && st.IsDir() {
		outputPath = filepath.Join(outputPath, originalName)
	} else if strings.HasSuffix(outputPath, string(os.PathSeparator)) {
		// if user passed a non-existing dir with trailing slash
		if err := os.MkdirAll(outputPath, 0755); err != nil {
			return fmt.Errorf("create output directory: %w", err)
		}
		outputPath = filepath.Join(outputPath, originalName)
	}

	outFile, err := os.Create(outputPath)
	if err != nil {
		return fmt.Errorf("create output file: %w", err)
	}
	defer outFile.Close()

	hasher := sha256.New()

	// ------------------------------------------------------------
	// Restore chunk by chunk
	// ------------------------------------------------------------
	for rows.Next() {

		var containerID int64
		var offset int64
		var size int64
		var filename string

		if err := rows.Scan(&containerID, &offset, &size, &filename); err != nil {
			return fmt.Errorf("scan chunk row: %w", err)
		}

		containerPath := filepath.Join(storageDir, filename)

		containerFile, err := os.Open(containerPath)
		if err != nil {
			return fmt.Errorf("open container %s: %w", filename, err)
		}

		// Seek to chunk offset
		_, err = containerFile.Seek(offset, io.SeekStart)
		if err != nil {
			containerFile.Close()
			return fmt.Errorf("seek container offset: %w", err)
		}

		chunkData := make([]byte, size)
		_, err = io.ReadFull(containerFile, chunkData)
		containerFile.Close()

		if err != nil {
			return fmt.Errorf("read chunk data: %w", err)
		}

		// Validate chunk size
		if int64(len(chunkData)) != size {
			return fmt.Errorf("chunk size mismatch (expected %d got %d)", size, len(chunkData))
		}

		// Write to output
		if _, err := outFile.Write(chunkData); err != nil {
			return fmt.Errorf("write output file: %w", err)
		}

		// Update hash
		if _, err := hasher.Write(chunkData); err != nil {
			return fmt.Errorf("hash restored data: %w", err)
		}
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate chunk rows: %w", err)
	}

	// ------------------------------------------------------------
	// Final Integrity Validation
	// ------------------------------------------------------------
	restoredHash := hex.EncodeToString(hasher.Sum(nil))

	if restoredHash != expectedFileHash {
		return fmt.Errorf(
			"restore integrity check failed: expected %s got %s",
			expectedFileHash,
			restoredHash,
		)
	}

	return nil
}
