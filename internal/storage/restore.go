package storage

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/franchoy/coldkeep/internal/container"
	"github.com/franchoy/coldkeep/internal/db"
	"github.com/franchoy/coldkeep/internal/utils_print"
)

func RestoreFile(id int64, outputPath string) error {
	dbconn, err := db.ConnectDB()
	if err != nil {
		return fmt.Errorf("Failed to connect to DB: %w", err)
	}
	defer func() { _ = dbconn.Close() }()

	if err := RestoreFileWithDB(dbconn, id, outputPath); err != nil {
		return err
	}
	return nil
}

func RestoreFileWithDB(dbconn *sql.DB, fileID int64, outputPath string) error {
	start := time.Now()
	// ------------------------------------------------------------
	// Fetch logical file metadata
	// ------------------------------------------------------------
	var expectedFileHash string
	var originalName string

	err := dbconn.QueryRow(
		"SELECT original_name, file_hash FROM logical_file WHERE status = 'COMPLETED' AND id = $1",
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
	// NOTE: chunk_offset points to the *start of the record* inside the container:
	//   [32 bytes sha256][4 bytes little-endian uint32 size][<size> bytes data]
	// ------------------------------------------------------------
	rows, err := dbconn.Query(`
		SELECT
			fc.chunk_order,
			c.chunk_offset,
			c.size,
			c.chunk_hash,
			ct.filename,
			c.status,
			ct.max_size
		FROM file_chunk fc
		JOIN chunk c ON fc.chunk_id = c.id
		JOIN container ct ON c.container_id = ct.id
		WHERE fc.logical_file_id = $1 
		ORDER BY fc.chunk_order ASC
	`, fileID)

	if err != nil {
		return fmt.Errorf("query file chunks: %w", err)
	}
	defer func() { _ = rows.Close() }()

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
	defer func() { _ = outFile.Close() }()

	hasher := sha256.New()

	var filecontainer *container.FileContainer
	var containerfilename string
	// ------------------------------------------------------------
	// Restore chunk by chunk
	// ------------------------------------------------------------
	var expectedOrder int64 = 0
	for rows.Next() {

		var chunkOrder int64
		var offset int64
		var expectedSize int64
		var expectedChunkHash string
		var filename string
		var chunkStatus string
		var maxSize int64

		if err := rows.Scan(&chunkOrder, &offset, &expectedSize, &expectedChunkHash, &filename, &chunkStatus, &maxSize); err != nil {
			return fmt.Errorf("scan chunk row: %w", err)
		}

		if chunkStatus != "COMPLETED" {
			return fmt.Errorf("chunk %d in file %d is not completed (status: %s)", chunkOrder, fileID, chunkStatus)
		}

		// Validate monotonically contiguous chunk sequence
		if chunkOrder != expectedOrder {
			return fmt.Errorf(
				"chunk order discontinuity for file %d: expected order %d got %d",
				fileID, expectedOrder, chunkOrder,
			)
		}
		expectedOrder++

		if containerfilename != filename {
			if filecontainer != nil {
				if err := filecontainer.Close(); err != nil {
					return fmt.Errorf("close container %q: %w", containerfilename, err)
				}
			}

			containerPath := filepath.Join(container.ContainersDir, filename)
			filecontainer, err = container.OpeneExistingContainer(true, containerPath, maxSize)
			if err != nil {
				return fmt.Errorf("open container %q: %w", filename, err)
			}
			containerfilename = filename
		}

		chunkData, err := container.ReadChunkDataAt(filecontainer, offset, expectedSize)
		if err != nil {
			return fmt.Errorf("read chunk data: %w", err)
		}

		// Validate hashes (DB hash and on-disk record hash)
		sum := sha256.Sum256(chunkData)
		sumHex := hex.EncodeToString(sum[:])

		if sumHex != expectedChunkHash {
			return fmt.Errorf("chunk hash mismatch at offset %d (db=%s computed=%s)", offset, expectedChunkHash, sumHex)
		}

		// Write to output
		if _, err := outFile.Write(chunkData); err != nil {
			return fmt.Errorf("write output file: %w", err)
		}

		// Update file hash
		if _, err := hasher.Write(chunkData); err != nil {
			return fmt.Errorf("hash restored data: %w", err)
		}
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate chunk rows: %w", err)
	}

	if filecontainer != nil {
		if err := filecontainer.Close(); err != nil {
			return fmt.Errorf("close container %q: %w", containerfilename, err)
		}
	}

	if expectedOrder == 0 {
		// valid ONLY if expectedFileHash == sha256("")
		const emptyFileSHA256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
		if expectedFileHash != emptyFileSHA256 {
			return fmt.Errorf("no chunks found for file %d but expected hash is not empty", fileID)
		}
		// else: empty file, no chunks, valid
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
	fmt.Printf("File %s restored successfully\n", originalName)
	fmt.Printf("  Output: %s\n", outputPath)
	fmt.Printf("  SHA256: %s\n", restoredHash)
	utils_print.PrintDuration(start)

	return nil
}
