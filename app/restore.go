package app

import (
	"bytes"
	"database/sql"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/klauspost/compress/zstd"
)

type chunkRef struct {
	containerFilename string
	chunkOffset       int64
	chunkSize         int
}

// restoreFile restores a logical file by name
func restoreFile(db *sql.DB, name string, outputPath string) error {

	// ------------------------------------------------------------
	// 1) Fetch logical file ID
	// ------------------------------------------------------------
	var logicalFileID int64

	err := db.QueryRow(`
        SELECT id
        FROM logical_file
        WHERE original_name = $1
        ORDER BY created_at DESC
        LIMIT 1
    `, name).Scan(&logicalFileID)

	if err != nil {
		return fmt.Errorf("logical file not found: %w", err)
	}

	// ------------------------------------------------------------
	// 2) Fetch ordered chunks
	// ------------------------------------------------------------
	rows, err := db.Query(`
        SELECT c.container_id,
               c.chunk_offset,
               c.size,
               ct.filename
        FROM file_chunk fc
        JOIN chunk c ON fc.chunk_id = c.id
        JOIN container ct ON c.container_id = ct.id
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
		var containerID int64

		err := rows.Scan(
			&containerID,
			&ref.chunkOffset,
			&ref.chunkSize,
			&ref.containerFilename,
		)
		if err != nil {
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
	if err := os.MkdirAll(filepath.Dir(outputPath), 0755); err != nil {
		return err
	}

	outFile, err := os.Create(outputPath)
	if err != nil {
		return err
	}
	defer outFile.Close()

	// ------------------------------------------------------------
	// 4) Restore sequentially (ORDERED)
	// ------------------------------------------------------------
	containerCache := make(map[string][]byte)

	for _, ch := range chunks {

		// Load + decompress container only once
		data, ok := containerCache[ch.containerFilename]
		if !ok {

			containerPath := filepath.Join("/storage/containers", ch.containerFilename)

			f, err := os.Open(containerPath)
			if err != nil {
				return err
			}

			reader, err := zstd.NewReader(f)
			if err != nil {
				f.Close()
				return err
			}

			fullData, err := io.ReadAll(reader)
			reader.Close()
			f.Close()

			if err != nil {
				return err
			}

			containerCache[ch.containerFilename] = fullData
			data = fullData
		}

		// Bounds check (important safety)
		if int(ch.chunkOffset)+ch.chunkSize > len(data) {
			return fmt.Errorf("chunk exceeds container bounds")
		}

		// Write chunk slice in strict order
		_, err := outFile.Write(
			data[ch.chunkOffset : ch.chunkOffset+int64(ch.chunkSize)],
		)
		if err != nil {
			return err
		}
	}

	return nil
}
