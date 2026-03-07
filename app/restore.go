package main

import (
	"bytes"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"
)

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
	start := time.Now()
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
	// NOTE: chunk_offset points to the *start of the record* inside the container:
	//   [32 bytes sha256][4 bytes little-endian uint32 size][<size> bytes data]
	// ------------------------------------------------------------
	rows, err := db.Query(`
		SELECT
			c.chunk_offset,
			c.size,
			c.chunk_hash,
			ct.filename,
			ct.compression_algorithm
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

		var offset int64
		var expectedSize int64
		var expectedChunkHash string
		var filename string
		var algoStr string

		if err := rows.Scan(&offset, &expectedSize, &expectedChunkHash, &filename, &algoStr); err != nil {
			return fmt.Errorf("scan chunk row: %w", err)
		}

		algo := CompressionType(algoStr)

		// Container filename changes when compressed (CompressFile removes the original and writes filename.<algo>)
		containerFilename := filename
		if algo != CompressionNone {
			containerFilename = filename + "." + algoStr
		}

		containerPath := filepath.Join(storageDir, containerFilename)

		// Open as plain file (seek) or as decompressed stream (skip bytes)
		var r io.ReadCloser
		var f *os.File

		if algo == CompressionNone {
			f, err = os.Open(containerPath)
			if err != nil {
				return fmt.Errorf("open container %q: %w", containerFilename, err)
			}
			// Seek to record start
			if _, err := f.Seek(offset, io.SeekStart); err != nil {
				_ = f.Close()
				return fmt.Errorf("seek container offset: %w", err)
			}
			// Use file as reader; close via f.Close() below
			r = f
		} else {
			r, err = OpenDecompressionReader(containerPath, algo)
			if err != nil {
				return fmt.Errorf("open compressed container %q: %w", containerFilename, err)
			}
			// Skip to record offset inside the *uncompressed* stream
			if _, err := io.CopyN(io.Discard, r, offset); err != nil {
				_ = r.Close()
				return fmt.Errorf("skip to chunk offset in decompressed stream: %w", err)
			}
		}

		// Read record header
		headerHash := make([]byte, 32)
		if _, err := io.ReadFull(r, headerHash); err != nil {
			_ = r.Close()
			return fmt.Errorf("read chunk header hash: %w", err)
		}

		sizeBuf := make([]byte, 4)
		if _, err := io.ReadFull(r, sizeBuf); err != nil {
			_ = r.Close()
			return fmt.Errorf("read chunk header size: %w", err)
		}
		recordSize := int64(binary.LittleEndian.Uint32(sizeBuf))

		if recordSize != expectedSize {
			_ = r.Close()
			return fmt.Errorf("chunk size mismatch at offset %d (db=%d record=%d)", offset, expectedSize, recordSize)
		}

		// Read chunk data
		chunkData := make([]byte, recordSize)
		if _, err := io.ReadFull(r, chunkData); err != nil {
			_ = r.Close()
			return fmt.Errorf("read chunk data: %w", err)
		}

		// Close container reader
		if err := r.Close(); err != nil {
			return fmt.Errorf("close container reader: %w", err)
		}

		// Validate hashes (DB hash and on-disk record hash)
		sum := sha256.Sum256(chunkData)
		sumHex := hex.EncodeToString(sum[:])

		if sumHex != expectedChunkHash {
			return fmt.Errorf("chunk hash mismatch at offset %d (db=%s computed=%s)", offset, expectedChunkHash, sumHex)
		}
		if !bytes.Equal(sum[:], headerHash) {
			return fmt.Errorf("chunk record header hash mismatch at offset %d", offset)
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
	printDuration(start)

	return nil
}
