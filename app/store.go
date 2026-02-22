package main

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"time"
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

// DBTX is implemented by *sql.DB and *sql.Tx (so we can reuse helpers inside a tx).
type DBTX interface {
	Exec(query string, args ...any) (sql.Result, error)
	QueryRow(query string, args ...any) *sql.Row
}

func storeFileWithDB(db *sql.DB, path string) (err error) {
	start := time.Now()

	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()

	info, err := file.Stat()
	if err != nil {
		return err
	}
	totalSize := info.Size()

	// Compute full file hash
	hasher := sha256.New()
	if _, err := io.Copy(hasher, file); err != nil {
		return err
	}
	fileHash := hex.EncodeToString(hasher.Sum(nil))

	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()

	// Check dedup
	var existingID int64
	qerr := tx.QueryRow(
		"SELECT id FROM logical_file WHERE file_hash=$1",
		fileHash,
	).Scan(&existingID)

	if qerr == nil {
		// Already stored
		if err = tx.Commit(); err != nil {
			return err
		}
		fmt.Printf("File '%s' already stored\n", path)
		fmt.Printf("  SHA256: %s\n", fileHash)
		return nil
	}
	if qerr != sql.ErrNoRows {
		err = qerr
		return err
	}

	// Insert logical file
	var fileID int64
	err = tx.QueryRow(
		`INSERT INTO logical_file (original_name, total_size, file_hash)
		 VALUES ($1, $2, $3)
		 RETURNING id`,
		info.Name(),
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
		cerr := tx.QueryRow(
			"SELECT id FROM chunk WHERE sha256=$1",
			hash,
		).Scan(&chunkID)

		if cerr == nil {
			// Chunk exists -> bump refcount
			_, err = tx.Exec(
				"UPDATE chunk SET ref_count = ref_count + 1 WHERE id = $1",
				chunkID,
			)
			if err != nil {
				return err
			}
		} else if cerr == sql.ErrNoRows {
			// New chunk -> append physically and insert chunk row
			containerID, filename, currentSize, err2 := getOrCreateOpenContainer(tx)
			if err2 != nil {
				err = err2
				return err
			}

			offset, newSize, err2 := appendChunkPhysical(filename, currentSize, chunkData)
			if err2 != nil {
				err = err2
				return err
			}

			if err2 := updateContainerSize(tx, containerID, newSize); err2 != nil {
				err = err2
				return err
			}

			// Seal if reached max size
			var maxSize int64
			if err2 := tx.QueryRow(`SELECT max_size FROM container WHERE id = $1`, containerID).Scan(&maxSize); err2 != nil {
				err = err2
				return err
			}

			if newSize >= maxSize {
				if err2 := sealContainer(tx, containerID, filename); err2 != nil {
					err = err2
					return err
				}
			}

			if err2 := tx.QueryRow(
				`INSERT INTO chunk (sha256, size, container_id, chunk_offset, ref_count)
				 VALUES ($1, $2, $3, $4, 1)
				 RETURNING id`,
				hash,
				len(chunkData),
				containerID,
				offset,
			).Scan(&chunkID); err2 != nil {
				err = err2
				return err
			}
		} else {
			err = cerr
			return err
		}

		// Link file ↔ chunk
		_, err = tx.Exec(
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

	if err = tx.Commit(); err != nil {
		return err
	}

	printSuccess("File stored successfully")
	fmt.Printf("  Path:   %s\n", path)
	fmt.Printf("  SHA256: %s\n", fileHash)
	printDuration(start)

	return nil
}

func storeFolder(root string) error {
	start := time.Now()

	db, err := connectDB()
	if err != nil {
		return fmt.Errorf("Failed to connect to DB: %w", err)
	}
	defer db.Close()

	workerCount := runtime.NumCPU()
	fileChan := make(chan string, 128)

	errChan := make(chan error, workerCount)

	// Workers
	for i := 0; i < workerCount; i++ {
		go func() {
			for p := range fileChan {
				if err := storeFileWithDB(db, p); err != nil {
					errChan <- err
					return
				}
			}
			errChan <- nil
		}()
	}

	// Producer
	walkErr := filepath.WalkDir(root, func(path string, d os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if !d.IsDir() {
			fileChan <- path
		}
		return nil
	})
	close(fileChan)

	if walkErr != nil {
		return walkErr
	}

	// Wait workers
	for i := 0; i < workerCount; i++ {
		if werr := <-errChan; werr != nil {
			return werr
		}
	}

	printSuccess("Folder stored successfully")
	printDuration(start)
	return nil
}
