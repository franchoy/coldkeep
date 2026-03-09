package storage

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

	"github.com/franchoy/coldkeep/internal/chunk"
	"github.com/franchoy/coldkeep/internal/container"
	"github.com/franchoy/coldkeep/internal/db"
	"github.com/franchoy/coldkeep/internal/utils"
)

func StoreFile(path string) error {
	db, err := db.ConnectDB()
	if err != nil {
		return fmt.Errorf("Failed to connect to DB: %w", err)
	}
	defer db.Close()

	if err := StoreFileWithDB(db, path); err != nil {
		return err
	}
	return nil

}

func claimLogicalFile(db *sql.DB, fileinfo os.FileInfo, fileHash string) (fileID int64, filestatus string, err error) {

	tx, err := db.Begin()
	if err != nil {
		return 0, "", err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()

	// Insert logical file (concurrency-safe)
	// If another goroutine inserts the same hash at the same time, we won't error.

	insErr := tx.QueryRow(
		`INSERT INTO logical_file (original_name, total_size, file_hash, status)
		VALUES ($1, $2, $3, 'PROCESSING')
		ON CONFLICT (file_hash, total_size) DO NOTHING
		RETURNING id`,
		fileinfo.Name(),
		fileinfo.Size(),
		fileHash,
	).Scan(&fileID)

	if insErr == sql.ErrNoRows {
		// Conflict happened: someone else already stored this file hash
		var existingID int64
		if err := tx.QueryRow(
			`SELECT id, status FROM logical_file WHERE file_hash = $1`,
			fileHash,
		).Scan(&existingID, &filestatus); err != nil {
			return 0, "", err
		}

		switch filestatus {
		case "COMPLETED":
			// File already stored and ready: we can reuse it
			_ = tx.Rollback() // Don't hold locks while waiting

			fmt.Printf("File '%s' already stored\n", fileinfo.Name())
			fmt.Printf("  FileID: %d\n", existingID)
			fmt.Printf("  SHA256: %s\n", fileHash)
			return existingID, filestatus, nil
		case "PROCESSING":
			// Another process is currently storing this file: we can wait and reuse it once done
			_ = tx.Rollback() // Don't hold locks while waiting
			fmt.Printf("File '%s' is currently being stored by another process. Waiting...\n", fileinfo.Name())

			for keep_waiting := true; keep_waiting; {

				// Poll every logicalFileWaitingtime milliseconds until the other process finishes
				time.Sleep(logicalFileWaitingtime)

				var finalStatus string
				if err := db.QueryRow(
					`SELECT status FROM logical_file WHERE id = $1`,
					existingID,
				).Scan(&finalStatus); err != nil {
					return 0, "", err
				}

				switch finalStatus {
				case "COMPLETED":
					fmt.Printf("File '%s' already stored\n", fileinfo.Name())
					fmt.Printf("  FileID: %d\n", existingID)
					fmt.Printf("  SHA256: %s\n", fileHash)
					return existingID, finalStatus, nil
				case "ABORTED":
					// Previous attempt was aborted while we were waiting: we can try to store again
					filestatus = finalStatus // Update status to break the loop and retry storing
					keep_waiting = false
				}
			}
		}

		// If we reach here, it means the previous attempt was aborted while we were waiting: we can try to store again
		if filestatus == "ABORTED" {
			// Previous attempt was aborted while we were waiting: we can try to store again
			// We can reuse the same logical_file row since it has the same file_hash
			tx2, err := db.Begin()
			if err != nil {
				return 0, "", err
			}
			if _, err := tx2.Exec(
				`UPDATE logical_file SET status = 'PROCESSING', retry_count = retry_count + 1 WHERE id = $1`,
				existingID,
			); err != nil {
				_ = tx2.Rollback()
				return 0, "", err
			}
			if err := tx2.Commit(); err != nil {
				_ = tx2.Rollback()
				return 0, "", err
			}
			fileID = existingID
		}
	} else if insErr == nil {
		// We won: this file is new and we should store it
		filestatus = "PROCESSING"
	} else {
		return 0, insErr
	}

	if err := tx.Commit(); err != nil {
		return 0, "", err
	}

	return fileID, filestatus, nil
}

func claimChunk(db *sql.DB, chunkHash string, chunksize int64) (chunkID int64, chunkstatus string, err error) {

	tx, err := db.Begin()
	if err != nil {
		return 0, "", err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()

	// Insert chunk (concurrency-safe)
	// If another goroutine inserts the same hash at the same time, we won't error.
	insErr := tx.QueryRow(
		`INSERT INTO chunk (chunk_hash, size, status, ref_count)
				VALUES ($1, $2, $3, 1)
				ON CONFLICT (chunk_hash, size) DO NOTHING
				RETURNING id`,
		chunkHash,
		chunksize,
		"PROCESSING",
	).Scan(&chunkID)

	if insErr == nil {
		// We won: this chunk is new
		chunkstatus = "PROCESSING"
	} else if insErr == sql.ErrNoRows {
		// Someone else inserted it first
		if err := tx.QueryRow(`SELECT id, status FROM chunk WHERE chunk_hash = $1 AND size = $2`, chunkHash, chunksize).Scan(&chunkID, &chunkstatus); err != nil {
			return 0, "", err
		}
		switch chunkstatus {
		case "COMPLETED":
			// Chunk already stored and ready: we can reuse it
			_ = tx.Rollback() // Don't hold locks while waiting
			return chunkID, chunkstatus, nil
		case "PROCESSING":
			// Another process is currently storing this chunk: we can wait and reuse it once done
			_ = tx.Rollback() // Don't hold locks while waiting
			fmt.Printf("Chunk '%s' is currently being stored by another process. Waiting...\n", chunkHash)

			for keep_waiting := true; keep_waiting; {

				// Poll every chunkWaitingtime milliseconds until the other process finishes
				time.Sleep(chunkWaitingtime)

				var finalStatus string
				if err := db.QueryRow(
					`SELECT status FROM chunk WHERE id = $1`,
					chunkID,
				).Scan(&finalStatus); err != nil {
					return 0, "", err
				}
				switch finalStatus {
				case "COMPLETED":
					fmt.Printf("Chunk '%s' already stored\n", chunkHash)
					return chunkID, finalStatus, nil
				case "ABORTED":
					// Previous attempt was aborted while we were waiting: we can try to store again
					chunkstatus = finalStatus // Update status to break the loop and retry storing
					keep_waiting = false
				}
			}
		}

		// If we reach here, it means the previous attempt was aborted while we were waiting: we can try to store again
		if chunkstatus == "ABORTED" {
			// Previous attempt was aborted while we were waiting: we can try to store again
			// We can reuse the same chunk row since it has the same chunk_hash and size
			tx2, err := db.Begin()
			if err != nil {
				return 0, "", err
			}
			if _, err := tx2.Exec(
				`UPDATE chunk SET status = 'PROCESSING', retry_count = retry_count + 1 WHERE id = $1`,
				chunkID,
			); err != nil {
				_ = tx2.Rollback()
				return 0, chunkstatus, err
			}
			if err := tx2.Commit(); err != nil {
				_ = tx2.Rollback()
				return 0, chunkstatus, err
			}
			chunkstatus = "PROCESSING"
		}
	} else {
		return 0, "", insErr
	}

	if err := tx.Commit(); err != nil {
		return 0, chunkstatus, err
	}

	return chunkID, chunkstatus, nil
}

func StoreFileWithDB(db *sql.DB, path string) (err error) {
	start := time.Now()

	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()

	fileinfo, err := file.Stat()
	if err != nil {
		return err
	}

	// Compute full file hash
	hasher := sha256.New()
	if _, err := io.Copy(hasher, file); err != nil {
		return err
	}
	fileHash := hex.EncodeToString(hasher.Sum(nil))

	if _, err := file.Seek(0,0); err != nil {
		return err
	}

	// Try to claim logical file for this hash (concurrency-safe)
	fileID, filestatus, err := claimLogicalFile(db, fileinfo, fileHash)
	if err != nil {
		return err
	}

	if filestatus == "COMPLETED" {
		// File already stored and ready: we can reuse it
		return nil
	}

	completed := false
	defer func() {
		if !completed {
			db.Exec(`UPDATE logical_file SET status='ABORTED' WHERE id=$1`, fileID)
		}
	}()

	// At this point, we have a logical_file row in "PROCESSING" status for this file hash, either created by us or by another process.
	chunks, err := chunk.ChunkFile(path)
	if err != nil {
		return err
	}

	chunkOrder := 0

	for _, chunkData := range chunks {
		sum := sha256.Sum256(chunkData)
		hash := hex.EncodeToString(sum[:])
		// Try to claim chunk for this hash (concurrency-safe)
		claimedChunkID, chunkStatus, err := claimChunk(db, hash, int64(len(chunkData)))
		if err != nil {
			return err
		}

		if chunkStatus == "COMPLETED" {
			// Chunk already stored and ready: we can reuse it, just need to link it to the logical file
			_, err = db.Exec(
				`INSERT INTO file_chunk (logical_file_id, chunk_id, chunk_order)
			 VALUES ($1, $2, $3)`,
				fileID,
				claimedChunkID,
				chunkOrder,
			)
			if err != nil {
				return err
			}
			fmt.Printf("Reusing existing chunk %s for file '%s'\n", hash, path)
			chunkOrder++
			continue // Move to next chunk
		}

		tx, err := db.Begin()
		if err != nil {
			return err
		}

		// At this point, we have a chunk row in "PROCESSING" status for this chunk hash, either created by us or by another process.
		containerID, containerfilename, containercurrentSize, err2 := container.GetOrCreateOpenContainer(tx)
		if err2 != nil {
			_ = tx.Rollback()
			return err2
		}
		// Append chunk data to container file
		offset, newSize, err2 := container.AppendChunkPhysical(containerfilename, containercurrentSize, chunkData)
		if err2 != nil {
			_ = tx.Rollback()

			if _, err3 := db.Exec(
				`UPDATE chunk SET status = 'ABORTED' WHERE id = $1`,
				claimedChunkID,
			); err3 != nil {
				return err3
			}
			return err2
		}
		
		// Update chunk row with container_id and chunk_offset, and mark it as "COMPLETED"
		if _, err2 := tx.Exec(
			`UPDATE chunk SET container_id = $1, chunk_offset = $2, status = 'COMPLETED' WHERE id = $3`,
			containerID,
			offset,
			claimedChunkID,
		); err2 != nil {
			_ = tx.Rollback()
			return err2
		}

		// Update container current size
		if err2 := container.UpdateContainerSize(tx, containerID, newSize); err2 != nil {
			_ = tx.Rollback()
			return err2
		}

		// Seal if reached max size
		var maxSize int64
		if err2 := tx.QueryRow(`SELECT max_size FROM container WHERE id = $1`, containerID).Scan(&maxSize); err2 != nil {
			_ = tx.Rollback()
			return err2
		}

		if newSize >= maxSize {
			if err2 := container.SealContainer(tx, containerID, containerfilename); err2 != nil {
				_ = tx.Rollback()
				return err2
			}
		}

		// Link file ↔ chunk
		_, err = tx.Exec(
			`INSERT INTO file_chunk (logical_file_id, chunk_id, chunk_order)
			 VALUES ($1, $2, $3)`,
			fileID,
			claimedChunkID,
			chunkOrder,
		)
		if err != nil {
			_ = tx.Rollback()
			return err
		}

		if err = tx.Commit(); err != nil {
			return err
		}

		fmt.Printf("Stored new chunk %s for file '%s'\n", hash, path)

		chunkOrder++
	}

	// After all chunks are stored and linked, mark logical file as "COMPLETED"
	_, err = db.Exec(
		`UPDATE logical_file SET status='COMPLETED' WHERE id=$1`,
		fileID,
	)
	if err != nil {
		return err
	}
	// Mark the operation as completed to avoid aborting it in the deferred function
	completed = true

	utils.PrintSuccess("File stored successfully")
	fmt.Printf("  FileID:   %d\n", fileID)
	fmt.Printf("  Path:   %s\n", path)
	fmt.Printf("  SHA256: %s\n", fileHash)
	utils.PrintDuration(start)

	return nil
}

