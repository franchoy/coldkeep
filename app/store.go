package main

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

func storeFile(path string) error {
	db, err := connectDB()
	if err != nil {
		return fmt.Errorf("Failed to connect to DB: %w", err)
	}
	defer db.Close()

	if err := storeFileWithDB(db, path); err != nil {
		return err
	}
	return nil
}

func storeFileWithDB(db *sql.DB, path string) error {

	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()

	fileInfo, err := file.Stat()
	if err != nil {
		return err
	}

	totalSize := fileInfo.Size()

	// Compute full file hash
	hasher := sha256.New()
	if _, err := io.Copy(hasher, file); err != nil {
		return err
	}
	fileHash := hex.EncodeToString(hasher.Sum(nil))

	// Check if logical file already exists
	var existingID int64
	err = db.QueryRow(
		"SELECT id FROM logical_file WHERE file_hash=$1",
		fileHash,
	).Scan(&existingID)

	if err == nil {
		return nil // already stored
	}
	if err != sql.ErrNoRows {
		return err
	}

	// Insert logical file
	var fileID int64
	err = db.QueryRow(
		`INSERT INTO logical_file (original_name, total_size, file_hash)
		 VALUES ($1, $2, $3)
		 RETURNING id`,
		fileInfo.Name(),
		totalSize,
		fileHash,
	).Scan(&fileID)

	if err != nil {
		return err
	}

	chunks, err := chunkFile(path)
	if err != nil {
		return err
	}

	chunkOrder := 0

	for _, chunkData := range chunks {

		sum := sha256.Sum256(chunkData)
		hash := hex.EncodeToString(sum[:])

		var chunkID int64

		err = db.QueryRow(
			"SELECT id FROM chunk WHERE sha256=$1",
			hash,
		).Scan(&chunkID)

		if err == nil {

			_, err = db.Exec(
				"UPDATE chunk SET ref_count = ref_count + 1 WHERE id = $1",
				chunkID,
			)
			if err != nil {
				return err
			}

		} else if err == sql.ErrNoRows {

			containerMutex.Lock()
			defer containerMutex.Unlock()

			containerID, filename, currentSize, err := getOrCreateOpenContainer(db)
			if err != nil {
				return err
			}
			writtenOffset, err := appendChunk(db, containerID, filename, currentSize, chunkData)
			if err != nil {
				containerMutex.Unlock()
				return err
			}

			err = db.QueryRow(
				`INSERT INTO chunk (sha256, size, container_id, chunk_offset, ref_count)
				 VALUES ($1, $2, $3, $4, 1)
				 RETURNING id`,
				hash,
				len(chunkData),
				containerID,
				writtenOffset,
			).Scan(&chunkID)

			containerMutex.Unlock()

			if err != nil {
				return err
			}

		} else {
			return err
		}

		_, err = db.Exec(
			`INSERT INTO file_chunk (logical_file_id, chunk_id, chunk_order)
			 VALUES ($1, $2, $3)`,
			fileID,
			chunkID,
			chunkOrder,
		)
		if err != nil {
			return err
		}

		chunkOrder++
	}

	return nil
}

func storeFolder(root string) error {
	db, err := connectDB()
	if err != nil {
		return fmt.Errorf("Failed to connect to DB: %w", err)
	}
	defer db.Close()

	const workerCount = 4

	fileChan := make(chan string, 100)
	done := make(chan error, workerCount)

	for i := 0; i < workerCount; i++ {
		go func() {
			for path := range fileChan {
				if err := storeFileWithDB(db, path); err != nil {
					done <- err
					return
				}
			}
			done <- nil
		}()
	}

	err = filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() {
			fileChan <- path
		}
		return nil
	})
	if err != nil {
		return err
	}

	close(fileChan)

	for i := 0; i < workerCount; i++ {
		if err := <-done; err != nil {
			return err
		}
	}

	return nil
}
