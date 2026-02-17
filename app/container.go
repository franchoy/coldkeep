package main

import (
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"
)

const containerMaxSize int64 = 64 * 1024 * 1024

var storageDir = getEnv("CAPSULE_STORAGE_DIR", "./storage/containers")

func getEnv(key, fallback string) string {
	if val, ok := os.LookupEnv(key); ok {
		return val
	}
	return fallback
}

func getOrCreateOpenContainer(db *sql.DB) (int64, string, int64) {
	var id int64
	var filename string
	var currentSize int64

	// 1️⃣ Try to find an existing open container
	err := db.QueryRow(`
		SELECT id, filename, current_size
		FROM container
		WHERE sealed = FALSE
		LIMIT 1
	`).Scan(&id, &filename, &currentSize)

	if err == nil {
		// Found existing open container
		return id, filename, currentSize
	}

	if err != sql.ErrNoRows {
		log.Fatal(err)
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
		log.Fatal(err)
	}

	// 3️⃣ Create physical file

	if err := os.MkdirAll(storageDir, 0755); err != nil {
		log.Fatal(err)
	}

	fullPath := filepath.Join(storageDir, filename)

	f, err := os.OpenFile(fullPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0666)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	// 4️⃣ Write V0 header
	if err := writeNewContainerHeaderV0(f, containerMaxSize); err != nil {
		log.Fatal(err)
	}

	// Ensure header is flushed
	if err := f.Sync(); err != nil {
		log.Fatal(err)
	}

	currentSize = ContainerHdrLenV0

	return id, filename, currentSize
}
func appendChunk(db *sql.DB, containerID int64, filename string, currentSize int64, chunk []byte) (int64, error) {
	fullPath := filepath.Join("/storage/containers", filename)

	f, err := os.OpenFile(fullPath, os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	offset := currentSize

	// Compute chunk hash (must match what you store in DB)
	hash := sha256.Sum256(chunk)

	// Write hash (32 bytes)
	if _, err := f.Write(hash[:]); err != nil {
		return 0, err
	}

	var sealed bool
	err = db.QueryRow(`SELECT sealed FROM container WHERE id = $1`, containerID).Scan(&sealed)
	if err != nil {
		return 0, err
	}
	if sealed {
		return 0, fmt.Errorf("attempt to write to sealed container")
	}

	// Write chunk size (4 bytes)
	sizeBuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(sizeBuf, uint32(len(chunk)))

	if _, err := f.Write(sizeBuf); err != nil {
		return 0, err
	}

	// Write chunk data
	if _, err := f.Write(chunk); err != nil {
		return 0, err
	}

	// Update container size
	newSize := currentSize + int64(32+4+len(chunk))

	_, err = db.Exec(`
		UPDATE container
		SET current_size = $1
		WHERE id = $2
	`, newSize, containerID)
	if err != nil {
		return 0, err
	}

	// Check if container reached max size
	var maxSize int64
	err = db.QueryRow(`SELECT max_size FROM container WHERE id = $1`, containerID).Scan(&maxSize)
	if err != nil {
		return 0, err
	}

	if newSize >= maxSize {
		if err := sealContainer(db, containerID, filename); err != nil {
			return 0, err
		}
	}

	return offset, nil

}

func compressAndMark(db *sql.DB, containerID int64, filename string) {
	path := filepath.Join("/storage/containers", filename)

	newPath, size, err := CompressFile(path, defaultCompression)
	if err != nil {
		log.Fatal(err)
	}

	_, err = db.Exec(`
		UPDATE container
		SET compression_algorithm=$1,
		    compressed_size=$2
		WHERE id=$3
	`, defaultCompression, size, containerID)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Container compressed:", newPath)
}

func sealContainer(db *sql.DB, containerID int64, filename string) error {
	containerDir := "/storage/containers"
	originalPath := filepath.Join(containerDir, filename)

	// Compress file
	compressedPath, _, err := CompressFile(originalPath, CompressionZstd)
	if err != nil {
		return err
	}

	// Update DB
	_, err = db.Exec(`
		UPDATE container
		SET sealed = TRUE,
		    compression_algorithm = $1
		WHERE id = $2
	`, string(CompressionZstd), containerID)

	if err != nil {
		return err
	}

	fmt.Printf("Container %d sealed and compressed: %s\n", containerID, compressedPath)

	return nil
}
