package main

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"time"
)

func storeFile(path string) {
	db := connectDB()
	defer db.Close()

	file, err := os.Open(path)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	fileInfo, err := file.Stat()
	if err != nil {
		log.Fatal(err)
	}

	totalSize := fileInfo.Size()

	// ------------------------------------------------------------
	// Compute full file hash
	// ------------------------------------------------------------
	hasher := sha256.New()
	if _, err := io.Copy(hasher, file); err != nil {
		log.Fatal(err)
	}
	fileHash := hex.EncodeToString(hasher.Sum(nil))

	// ------------------------------------------------------------
	// Check if logical file already exists
	// ------------------------------------------------------------
	var existingID int64
	err = db.QueryRow(
		"SELECT id FROM logical_file WHERE file_hash=$1",
		fileHash,
	).Scan(&existingID)

	if err == nil {
		log.Printf("File already stored with ID %d\n", existingID)
		return
	}
	if err != sql.ErrNoRows {
		log.Fatal(err)
	}

	// ------------------------------------------------------------
	// Insert logical file
	// ------------------------------------------------------------
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
		log.Fatal(err)
	}

	log.Printf("Storing file ID %d\n", fileID)

	// ------------------------------------------------------------
	// Chunk file using CDC
	// ------------------------------------------------------------
	chunks, err := chunkFile(path)
	if err != nil {
		log.Fatal(err)
	}

	chunkOrder := 0

	for _, chunkData := range chunks {

		sum := sha256.Sum256(chunkData)
		hash := hex.EncodeToString(sum[:])

		var chunkID int64
		var refCount int64

		// --------------------------------------------------------
		// Try to find existing chunk
		// --------------------------------------------------------
		err = db.QueryRow(
			"SELECT id, ref_count FROM chunk WHERE sha256=$1",
			hash,
		).Scan(&chunkID, &refCount)

		if err == nil {
			// Existing chunk → increment ref_count
			_, err = db.Exec(
				"UPDATE chunk SET ref_count = ref_count + 1 WHERE id = $1",
				chunkID,
			)
			if err != nil {
				log.Fatal(err)
			}

		} else if err == sql.ErrNoRows {

			// ----------------------------------------------------
			// NEW CHUNK → find or create open container
			// ----------------------------------------------------
			var containerID int64
			var currentSize int64
			var maxSize int64
			var filename string

			err = db.QueryRow(`
				SELECT id, filename, current_size, max_size
				FROM container
				WHERE sealed = FALSE
				ORDER BY id DESC
				LIMIT 1
			`).Scan(&containerID, &filename, &currentSize, &maxSize)

			if err == sql.ErrNoRows {
				// Create new container
				filename = fmt.Sprintf("container_%d.bin", time.Now().UnixNano())

				err = db.QueryRow(`
					INSERT INTO container (filename, current_size, max_size, sealed, compression_algorithm, compressed_size)
					VALUES ($1, 0, $2, FALSE, 'none', 0)
					RETURNING id, current_size, max_size
				`, filename, containerMaxSize).Scan(&containerID, &currentSize, &maxSize)

				if err != nil {
					log.Fatal(err)
				}
			} else if err != nil {
				log.Fatal(err)
			}

			recordSize := int64(32+4) + int64(len(chunkData))

			// If container full → seal and create new
			if currentSize+recordSize > maxSize {

				if err := sealContainer(db, containerID, filename); err != nil {
					log.Fatal(err)
				}

				// Create new container
				filename = fmt.Sprintf("container_%d.bin", time.Now().UnixNano())

				err = db.QueryRow(`
					INSERT INTO container (filename, current_size, max_size, sealed, compression_algorithm, compressed_size)
					VALUES ($1, 0, $2, FALSE, 'none', 0)
					RETURNING id, current_size, max_size
				`, filename, containerMaxSize).Scan(&containerID, &currentSize, &maxSize)

				if err != nil {
					log.Fatal(err)
				}
			}

			offset := currentSize

			writtenOffset, err := appendChunk(db, containerID, hash, offset, chunkData)
			if err != nil {
				log.Fatal(err)
			}

			newSize := offset + recordSize

			_, err = db.Exec(
				"UPDATE container SET current_size = $1 WHERE id = $2",
				newSize,
				containerID,
			)
			if err != nil {
				log.Fatal(err)
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

			if err != nil {
				log.Fatal(err)
			}

		} else {
			log.Fatal(err)
		}

		// --------------------------------------------------------
		// Insert mapping
		// --------------------------------------------------------
		_, err = db.Exec(
			`INSERT INTO file_chunk (logical_file_id, chunk_id, chunk_order)
			 VALUES ($1, $2, $3)`,
			fileID,
			chunkID,
			chunkOrder,
		)
		if err != nil {
			log.Fatal(err)
		}

		chunkOrder++
	}

	log.Println("File stored successfully")
}

func storeFolder(root string) {
	const workerCount = 4

	fileChan := make(chan string, 100)
	done := make(chan bool)

	// Workers
	for i := 0; i < workerCount; i++ {
		go func() {
			for path := range fileChan {
				fmt.Println("Storing:", path)
				storeFile(path)
			}
			done <- true
		}()
	}

	// Walk directory
	filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() {
			fileChan <- path
		}
		return nil
	})

	close(fileChan)

	// Wait workers
	for i := 0; i < workerCount; i++ {
		<-done
	}

	fmt.Println("Folder store complete.")
}
