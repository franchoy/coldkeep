package main

import (
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

var storageDir = getEnv("coldkeep_STORAGE_DIR", "./storage/containers")

var containerMaxSize = getEnvInt64("coldkeep_CONTAINER_MAX_SIZE_MB", 64) * 1024 * 1024 //MB

func getEnv(key, fallback string) string {
	if val, ok := os.LookupEnv(key); ok {
		return val
	}
	return fallback
}

func getEnvInt64(key string, fallback int64) int64 {
	if val, ok := os.LookupEnv(key); ok {
		var result int64
		if _, err := fmt.Sscanf(val, "%d", &result); err == nil {
			return result
		}
	}
	return fallback
}

func getOrCreateOpenContainer(db DBTX) (int64, string, int64, error) {
	var id int64
	var filename string
	var currentSize int64

	// 1️⃣ Try to find an existing open container
	err := db.QueryRow(`
		SELECT id, filename, current_size
		FROM container
		WHERE sealed = FALSE
		ORDER BY id
		LIMIT 1
		FOR UPDATE SKIP LOCKED
	`).Scan(&id, &filename, &currentSize)

	if err == nil {
		// Found existing open container
		return id, filename, currentSize, nil
	}

	if err != sql.ErrNoRows {
		return 0, "", 0, err
	}

	// 2️⃣ No open container found → create new one

	filename = fmt.Sprintf("container_%d.bin", time.Now().UnixNano())

	// Insert DB row with current_size initialized to header size
	err = db.QueryRow(`
		INSERT INTO container (filename, current_size, max_size, sealed)
		VALUES ($1, $2, $3, FALSE)
		RETURNING id
	`, filename, ContainerHdrLenV0, containerMaxSize).Scan(&id)

	if err != nil {
		return 0, "", 0, err
	}

	// 3️⃣ Create physical file

	if err := os.MkdirAll(storageDir, 0755); err != nil {
		return 0, "", 0, err
	}

	fullPath := filepath.Join(storageDir, filename)

	f, err := os.OpenFile(fullPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0666)
	if err != nil {
		return 0, "", 0, err
	}
	defer f.Close()

	// 4️⃣ Write V0 header
	if err := writeNewContainerHeaderV0(f, containerMaxSize); err != nil {
		return 0, "", 0, err
	}

	// Ensure header is flushed
	if err := f.Sync(); err != nil {
		return 0, "", 0, err
	}

	currentSize = ContainerHdrLenV0

	return id, filename, currentSize, nil
}

func appendChunkPhysical(filename string, currentSize int64, chunk []byte) (int64, int64, error) {
	containerDir := storageDir
	containerPath := filepath.Join(containerDir, filename)

	f, err := os.OpenFile(containerPath, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return 0, 0, err
	}
	defer f.Close()

	// offset where this chunk starts
	offset := currentSize

	// Write record format:
	// [32 bytes sha256][4 bytes size][data]

	sum := sha256.Sum256(chunk)

	// Write hash
	if _, err := f.Write(sum[:]); err != nil {
		return 0, 0, err
	}

	// Write size (little endian uint32)
	sizeBuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(sizeBuf, uint32(len(chunk)))
	if _, err := f.Write(sizeBuf); err != nil {
		return 0, 0, err
	}

	// Write data
	if _, err := f.Write(chunk); err != nil {
		return 0, 0, err
	}

	newSize := currentSize + int64(32+4+len(chunk))

	return offset, newSize, nil
}

func updateContainerSize(tx DBTX, containerID int64, newSize int64) error {
	_, err := tx.Exec(
		`UPDATE container SET current_size = $1 WHERE id = $2`,
		newSize,
		containerID,
	)
	return err
}

func sealContainer(tx DBTX, containerID int64, filename string) error {
	containerDir := storageDir
	originalPath := filepath.Join(containerDir, filename)

	// Compress file
	compressedPath, compressed_size, err := CompressFile(originalPath, defaultCompression)
	if err != nil {
		return err
	}

	// Update DB
	_, err = tx.Exec(`
		UPDATE container
		SET sealed = TRUE,
		    compression_algorithm = $1,
			compressed_size = $2
		WHERE id = $3
	`, string(defaultCompression), compressed_size, containerID)

	if err != nil {
		return fmt.Errorf("update/seal container failed: %w", err)
	}

	fmt.Printf("Container %d sealed and compressed with type %s : %s\n", containerID, defaultCompression, compressedPath)
	return nil
}
