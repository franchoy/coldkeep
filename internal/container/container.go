package container

import (
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/franchoy/coldkeep/internal/db"
	"github.com/franchoy/coldkeep/internal/utils_hash"
)

func GetOrCreateOpenContainer(db db.DBTX) (int64, string, int64, error) {
	var id int64
	var filename string
	var currentSize int64

	// 1️⃣ Try to find an existing open container
	err := db.QueryRow(`
		SELECT id, filename, current_size
		FROM container
		WHERE sealed = FALSE and quarantine = FALSE
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
	`, filename, ContainerHdrLen, containerMaxSize).Scan(&id)

	if err != nil {
		return 0, "", 0, err
	}

	// 3️⃣ Create physical file

	if err := os.MkdirAll(ContainersDir, 0755); err != nil {
		return 0, "", 0, err
	}

	fullPath := filepath.Join(ContainersDir, filename)

	f, err := os.OpenFile(fullPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0666)
	if err != nil {
		return 0, "", 0, err
	}
	defer func() { _ = f.Close() }()

	// 4️⃣ Write V0 header
	if err := writeNewContainerHeaderV0(f, containerMaxSize); err != nil {
		return 0, "", 0, err
	}

	// Ensure header is flushed
	if err := f.Sync(); err != nil {
		return 0, "", 0, err
	}

	currentSize = ContainerHdrLen

	return id, filename, currentSize, nil
}

func AppendChunkPhysical(filename string, currentSize int64, chunk []byte) (int64, int64, error) {

	containerPath := filepath.Join(ContainersDir, filename)

	f, err := os.OpenFile(containerPath, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return 0, 0, err
	}
	defer func() { _ = f.Close() }()

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
	// Ensure data is flushed to disk
	if err := f.Sync(); err != nil {
		return 0, 0, err
	}

	newSize := currentSize + int64(32+4+len(chunk))

	return offset, newSize, nil
}

func UpdateContainerSize(tx db.DBTX, containerID int64, newSize int64) error {
	_, err := tx.Exec(
		`UPDATE container SET current_size = $1 WHERE id = $2`,
		newSize,
		containerID,
	)
	return err
}

func SealContainer(tx db.DBTX, containerID int64, filename string) error {

	originalPath := filepath.Join(ContainersDir, filename)

	// Compute file hash
	sumHex, err := utils_hash.ComputeFileHashHex(originalPath)
	if err != nil {
		return fmt.Errorf("compute container file hash: %w", err)
	}

	// Update DB
	_, err = tx.Exec(`
		UPDATE container
		SET sealed = TRUE,
			container_hash = $1
		WHERE id = $2
	`, sumHex, containerID)

	if err != nil {
		return fmt.Errorf("update/seal container failed: %w", err)
	}

	fmt.Printf("Container %d sealed successfully: %s\n", containerID, originalPath)
	return nil
}

func CheckContainerHashFile(id int, filename, storedHash string) error {
	containerPath := filepath.Join(ContainersDir, filename)

	computedHash, err := utils_hash.ComputeFileHashHex(containerPath)
	if err != nil {
		return fmt.Errorf("compute container file hash: %w", err)
	}

	//if stored has is null or empty, we can skip the check (for backward compatibility with old containers)
	if len(storedHash) == 0 || storedHash == "null" || storedHash == "NULL" {
		return fmt.Errorf("container file hash is missing in db for container %d, calculated hash: %s", id, computedHash)
	}

	if computedHash != storedHash {
		return fmt.Errorf("container file hash mismatch for container %d: expected %s, got %s", id, storedHash, computedHash)
	}

	return nil
}
