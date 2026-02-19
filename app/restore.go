package main

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

type chunkRef struct {
	containerFilename string
	algo              string
	chunkOffset       int64 // start of record
	chunkSize         int   // data size only
	sha256            string
}

func restoreFile(id int64, outputPath string) error {
	db := connectDB()
	defer db.Close()
	// ------------------------------------------------------------
	// 1) Resolve logical file ID (latest version)
	// ------------------------------------------------------------
	var logicalFileID int64
	var originalName string

	err := db.QueryRow(`
		SELECT id,
		original_name
		FROM logical_file
		WHERE id = $1
		ORDER BY created_at DESC
		LIMIT 1
	`, id).Scan(&logicalFileID, &originalName)

	if err != nil {
		return fmt.Errorf("logical file not found: %w", err)
	}

	// ------------------------------------------------------------
	// 2) Fetch ordered chunks
	// ------------------------------------------------------------
	rows, err := db.Query(`
		SELECT
			ct.filename,
			ct.compression_algorithm,
			ch.chunk_offset,
			ch.size,
			ch.sha256
		FROM file_chunk fc
		JOIN chunk ch ON fc.chunk_id = ch.id
		JOIN container ct ON ch.container_id = ct.id
		WHERE fc.logical_file_id = $1
		ORDER BY fc.chunk_order ASC
	`, logicalFileID)
	if err != nil {
		return err
	}
	defer rows.Close()

	var chunks []chunkRef

	for rows.Next() {
		var ref chunkRef
		if err := rows.Scan(
			&ref.containerFilename,
			&ref.algo,
			&ref.chunkOffset,
			&ref.chunkSize,
			&ref.sha256,
		); err != nil {
			return err
		}
		chunks = append(chunks, ref)
	}

	if len(chunks) == 0 {
		return fmt.Errorf("no chunks found for file")
	}

	// ------------------------------------------------------------
	// 3) Prepare output file
	// ------------------------------------------------------------
	if err := os.MkdirAll(outputPath, 0755); err != nil {
		return err
	}

	outFile, err := os.Create(filepath.Join(outputPath, originalName))
	if err != nil {
		return err
	}
	defer outFile.Close()

	// ------------------------------------------------------------
	// 4) Restore sequentially (preserve order)
	// ------------------------------------------------------------
	containerCache := make(map[string][]byte)

	for _, ch := range chunks {

		// Build physical path
		containerPath := filepath.Join(storageDir, ch.containerFilename)

		if ch.algo != "" && ch.algo != "none" {
			containerPath += "." + ch.algo
		}

		data, ok := containerCache[containerPath]
		if !ok {
			reader, err := OpenDecompressionReader(containerPath, CompressionType(ch.algo))
			if err != nil {
				return err
			}

			fullData, err := io.ReadAll(reader)
			_ = reader.Close()
			if err != nil {
				return err
			}

			containerCache[containerPath] = fullData
			data = fullData
		}

		// ---- IMPORTANT PART ----
		// Record layout:
		// [32 bytes sha256][4 bytes size][chunk data]

		const recordHeaderSize = int64(32 + 4)

		start := ch.chunkOffset + recordHeaderSize
		end := start + int64(ch.chunkSize)

		if start < 0 || end > int64(len(data)) {
			return fmt.Errorf(
				"chunk exceeds container bounds (start=%d end=%d len=%d)",
				start, end, len(data),
			)
		}

		sum := sha256.Sum256(data[start:end])
		if hex.EncodeToString(sum[:]) != ch.sha256 {
			return fmt.Errorf("chunk hash mismatch")
		}

		if _, err := outFile.Write(data[start:end]); err != nil {
			return err
		}

	}

	return nil
}
