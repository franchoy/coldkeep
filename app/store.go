package main

import (
	"crypto/sha256"
	"database/sql"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
)

func storeFile(filePath string) {
	db := connectDB()
	defer db.Close()

	// ---- Compute full file hash ----
	file, err := os.Open(filePath)
	if err != nil {
		log.Println("Error opening file:", err)
		return
	}
	defer file.Close()

	hasher := sha256.New()
	totalSize, err := io.Copy(hasher, file)
	if err != nil {
		log.Println("Hash error:", err)
		return
	}
	fileHash := fmt.Sprintf("%x", hasher.Sum(nil))

	// ---- File-level dedup ----
	var existingID int
	err = db.QueryRow(
		"SELECT id FROM logical_file WHERE file_hash=$1",
		fileHash,
	).Scan(&existingID)
	if err == nil {
		fmt.Println("File already exists. ID:", existingID)
		return
	}

	// ---- Run CDC ----
	chunks, err := chunkFile(filePath)
	if err != nil {
		log.Println("CDC error:", err)
		return
	}

	fileInfo, _ := os.Stat(filePath)

	var fileID int
	err = db.QueryRow(
		`INSERT INTO logical_file (original_name, total_size, file_hash)
		 VALUES ($1, $2, $3) RETURNING id`,
		fileInfo.Name(),
		totalSize,
		fileHash,
	).Scan(&fileID)
	if err != nil {
		log.Println("Insert logical_file error:", err)
		return
	}

	fmt.Println("Logical file ID:", fileID)

	totalChunks := len(chunks)
	reusedChunks := 0
	newChunks := 0
	newBytes := 0

	for index, chunkData := range chunks {
		hash := fmt.Sprintf("%x", sha256.Sum256(chunkData))

		var chunkID int

		// First check if chunk exists
		err := db.QueryRow(
			"SELECT id FROM chunk WHERE sha256=$1",
			hash,
		).Scan(&chunkID)

		if err == nil {
			// Reuse
			_, err = db.Exec(`
				UPDATE chunk
				SET ref_count = ref_count + 1
				WHERE id = $1
			`, chunkID)
			if err != nil {
				log.Fatal(err)
			}
			reusedChunks++
		} else {
			// New chunk path
			containerMutex.Lock()

			containerID, filename, currentSize := getOrCreateOpenContainer(db)

			offset, err := appendChunk(db, containerID, filename, currentSize, chunkData)
			if err != nil {
				containerMutex.Unlock()
				log.Fatal(err)
			}

			// Insert chunk row (dedup race handled via ON CONFLICT)
			err = db.QueryRow(`
				INSERT INTO chunk (sha256, size, container_id, chunk_offset, ref_count)
				VALUES ($1, $2, $3, $4, 1)
				ON CONFLICT (sha256) DO NOTHING
				RETURNING id
			`, hash, len(chunkData), containerID, offset).Scan(&chunkID)

			if err == sql.ErrNoRows {
				// Conflict → another goroutine inserted it, reuse instead
				err = db.QueryRow(
					"SELECT id FROM chunk WHERE sha256=$1",
					hash,
				).Scan(&chunkID)
				if err != nil {
					containerMutex.Unlock()
					log.Fatal(err)
				}

				_, err = db.Exec(`
					UPDATE chunk
					SET ref_count = ref_count + 1
					WHERE id = $1
				`, chunkID)
				if err != nil {
					containerMutex.Unlock()
					log.Fatal(err)
				}

				reusedChunks++
			} else if err != nil {
				containerMutex.Unlock()
				log.Fatal(err)
			} else {
				newChunks++
				newBytes += len(chunkData)
			}

			containerMutex.Unlock()
		}

		// Map file → chunk
		_, err = db.Exec(
			"INSERT INTO file_chunk (file_id, chunk_id, chunk_order) VALUES ($1, $2, $3)",
			fileID, chunkID, index,
		)
		if err != nil {
			log.Fatal(err)
		}
	}

	// ---- Stats ----
	dedupRatio := 0.0
	if totalChunks > 0 {
		dedupRatio = float64(reusedChunks) / float64(totalChunks) * 100
	}

	fmt.Println("\nCDC Summary:")
	fmt.Printf("  File ID:             %d\n", fileID)
	fmt.Printf("  Total chunks:        %d\n", totalChunks)
	fmt.Printf("  Reused chunks:       %d\n", reusedChunks)
	fmt.Printf("  New chunks:          %d\n", newChunks)
	fmt.Printf("  Logical size:        %.2f MB\n", float64(totalSize)/(1024*1024))
	fmt.Printf("  New physical bytes:  %.2f MB\n", float64(newBytes)/(1024*1024))
	fmt.Printf("  Dedup ratio:         %.2f%%\n", dedupRatio)
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
