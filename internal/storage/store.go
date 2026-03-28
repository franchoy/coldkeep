package storage

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"time"

	"github.com/franchoy/coldkeep/internal/blocks"
	"github.com/franchoy/coldkeep/internal/chunk"
	"github.com/franchoy/coldkeep/internal/container"
	"github.com/franchoy/coldkeep/internal/db"
	"github.com/franchoy/coldkeep/internal/utils_print"
	"github.com/lib/pq"
)

var errContainerLockContention = errors.New("container row lock contention")

func StoreFile(path string) error {
	codec, err := blocks.LoadDefaultCodec()
	if err != nil {
		return err
	}

	return storeFile(path, codec)
}

func StoreFileWithCodec(path string, codecName string) error {
	codec, err := blocks.ParseCodec(codecName)
	if err != nil {
		return err
	}

	return storeFile(path, codec)
}

func storeFile(path string, codec blocks.Codec) error {
	dbconn, err := db.ConnectDB()
	if err != nil {
		return fmt.Errorf("Failed to connect to DB: %w", err)
	}
	defer func() { _ = dbconn.Close() }()

	if err := StoreFileWithDBAndCodec(dbconn, path, codec); err != nil {
		return err
	}
	return nil
}

func claimLogicalFile(dbconn *sql.DB, fileinfo os.FileInfo, fileHash string) (fileID int64, filestatus string, err error) {

	tx, err := dbconn.Begin()
	if err != nil {
		return 0, "", err
	}
	txclosed := false
	defer func() {
		if err != nil && !txclosed {
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
			`SELECT id, status FROM logical_file WHERE file_hash = $1 and total_size = $2`,
			fileHash,
			fileinfo.Size(),
		).Scan(&existingID, &filestatus); err != nil {
			return 0, "", err
		}

		switch filestatus {
		case "COMPLETED":
			// File already stored and ready: we can reuse it
			_ = tx.Rollback() // Don't hold locks while waiting
			txclosed = true
			fmt.Printf("File '%s' already stored\n", fileinfo.Name())
			fmt.Printf("  FileID: %d\n", existingID)
			fmt.Printf("  SHA256: %s\n", fileHash)
			return existingID, filestatus, nil
		case "PROCESSING":
			// Another process is currently storing this file: we can wait and reuse it once done
			_ = tx.Rollback() // Don't hold locks while waiting
			txclosed = true
			fmt.Printf("File '%s' is currently being stored by another process. Waiting...\n", fileinfo.Name())

			for keep_waiting := true; keep_waiting; {

				// Poll every logicalFileWaitingtime milliseconds until the other process finishes
				time.Sleep(logicalFileWaitingtime)

				var finalStatus string
				if err := dbconn.QueryRow(
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
			tx2, err := dbconn.Begin()
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
			filestatus = "PROCESSING"
		}
	} else if insErr == nil {
		// We won: this file is new and we should store it
		filestatus = "PROCESSING"
	} else {
		return 0, "", insErr
	}
	if !txclosed {
		if err := tx.Commit(); err != nil {
			return 0, "", err
		}
	}

	return fileID, filestatus, nil
}

func claimChunk(dbconn *sql.DB, chunkHash string, chunksize int64) (chunkID int64, chunkstatus string, err error) {

	tx, err := dbconn.Begin()
	if err != nil {
		return 0, "", err
	}
	txclosed := false
	defer func() {
		if err != nil && !txclosed {
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
			txclosed = true
			return chunkID, chunkstatus, nil
		case "PROCESSING":
			// Another process is currently storing this chunk: we can wait and reuse it once done
			_ = tx.Rollback() // Don't hold locks while waiting
			txclosed = true
			fmt.Printf("Chunk '%s' is currently being stored by another process. Waiting...\n", chunkHash)

			for keep_waiting := true; keep_waiting; {

				// Poll every chunkWaitingtime milliseconds until the other process finishes
				time.Sleep(chunkWaitingtime)

				var finalStatus string
				if err := dbconn.QueryRow(
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
			tx2, err := dbconn.Begin()
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

	if !txclosed {
		if err := tx.Commit(); err != nil {
			return 0, chunkstatus, err
		}
	}
	return chunkID, chunkstatus, nil
}

func StoreFileWithDB(dbconn *sql.DB, path string) (err error) {
	codec, err := blocks.LoadDefaultCodec()
	if err != nil {
		return err
	}

	return StoreFileWithDBAndCodec(dbconn, path, codec)
}

func StoreFileWithDBAndCodec(dbconn *sql.DB, path string, codec blocks.Codec) (err error) {
	start := time.Now()

	transformer, err := blocks.GetBlockTransformer(codec)
	if err != nil {
		if codec == blocks.CodecAESGCM {
			return fmt.Errorf("encryption key required for aes-gcm: %w", err)
		}
		return fmt.Errorf("initialize codec %s: %w", codec, err)
	}

	blockRepo := &blocks.Repository{
		DB: dbconn,
	}

	ctx := context.Background()

	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() { _ = file.Close() }()

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

	if _, err := file.Seek(0, 0); err != nil {
		return err
	}

	// Try to claim logical file for this hash (concurrency-safe)
	fileID, filestatus, err := claimLogicalFile(dbconn, fileinfo, fileHash)
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
			if _, err := dbconn.Exec(`UPDATE logical_file SET status='ABORTED' WHERE id=$1`, fileID); err != nil {
				fmt.Printf("failed to mark logical file %d as ABORTED: %v\n", fileID, err)
			}
		}
	}()

	// At this point, we have a logical_file row in "PROCESSING" status for this file hash, either created by us or by another process.
	chunks, err := chunk.ChunkFile(path)
	if err != nil {
		return err
	}

	chunkOrder := 0
	var activeContainer container.ActiveContainer
	hasContainer := false
	defer func() {
		if hasContainer && activeContainer.Container != nil {
			_ = activeContainer.Container.Close()
		}
	}()

	for _, chunkData := range chunks {
		sum := sha256.Sum256(chunkData)
		chunkHash := hex.EncodeToString(sum[:])
		// Try to claim chunk for this hash (concurrency-safe)
		claimedChunkID, chunkStatus, err := claimChunk(dbconn, chunkHash, int64(len(chunkData)))
		if err != nil {
			return err
		}

		if chunkStatus == "COMPLETED" {
			// Chunk already stored and ready: we can reuse it, just need to link it to the logical file
			tx, err := dbconn.Begin()
			if err != nil {
				return err
			}

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
				_ = tx.Rollback()
				return err
			}

			fmt.Printf("Reusing existing chunk %s for file '%s'\n", chunkHash, path)
			chunkOrder++
			continue // Move to next chunk
		}

		// At this point, we have a chunk row in "PROCESSING" status for this chunk hash, either created by us or by another process.

		for {
			tx, err := dbconn.Begin()
			if err != nil {
				return err
			}

			if !hasContainer {
				activeContainer, err = container.GetOrCreateOpenContainer(tx)
				if err != nil {
					_ = tx.Rollback()
					return err
				}
				hasContainer = true
			}

			// Ensure we keep a row-level lease for this container during this chunk write.
			if err = lockContainerRowNowaitWithRetry(tx, activeContainer.ID, containerLockRetryAttempts, containerLockRetryWait); err != nil {
				_ = tx.Rollback()
				if errors.Is(err, errContainerLockContention) {
					if activeContainer.Container != nil {
						_ = activeContainer.Container.Close()
						activeContainer.Container = nil
					}
					hasContainer = false
					continue
				}
				return err
			}

			// Append chunk data to container file
			offset, newSize, desc, err := storeChunkAsPlainBlock(
				ctx,
				tx,
				blockRepo,
				activeContainer,
				claimedChunkID,
				chunkHash,
				chunkData,
				transformer,
			)
			_ = offset
			_ = desc

			if err != nil {
				_ = tx.Rollback()
				if activeContainer.Container != nil {
					_ = activeContainer.Container.Close()
					activeContainer.Container = nil
				}
				hasContainer = false

				if _, err3 := dbconn.Exec(
					`UPDATE chunk SET status = 'ABORTED' WHERE id = $1`,
					claimedChunkID,
				); err3 != nil {
					return err3
				}
				return err
			}

			_ = desc

			// Mark chunk as completed
			if _, err := tx.Exec(
				`UPDATE chunk SET status = 'COMPLETED' WHERE id = $1`,
				claimedChunkID,
			); err != nil {
				_ = tx.Rollback()
				return err
			}

			// Update container current size
			if err := container.UpdateContainerSize(tx, activeContainer.ID, newSize); err != nil {
				_ = tx.Rollback()
				return err
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

			// Seal if reached max size.
			// NOTE: container max size is best-effort under concurrency.
			if newSize >= activeContainer.MaxSize {
				// After sealing the full container, we need to sync and close the file handle before we can open a new one for the next chunks
				if err := SyncCloseAndSealContainer(tx, activeContainer); err != nil {
					_ = tx.Rollback()
					return err
				}
				fmt.Printf("Container %d sealed at size %d bytes.\n", activeContainer.ID, newSize)
				activeContainer.Container = nil
				hasContainer = false
				// Note: it's possible that multiple containers get sealed around the same time if we have many concurrent writers, which is fine.
				// The important part is that we don't exceed the max size for any container, and that we can continue writing new chunks to new containers as needed.
			}

			if err = tx.Commit(); err != nil {
				return err
			}

			fmt.Printf("Stored new chunk %s for file '%s'\n", chunkHash, path)

			chunkOrder++
			break
		}
	}

	// After all chunks are stored and linked, mark logical file as "COMPLETED"
	_, err = dbconn.Exec(
		`UPDATE logical_file SET status='COMPLETED' WHERE id=$1`,
		fileID,
	)
	if err != nil {
		return err
	}
	// Mark the operation as completed to avoid aborting it in the deferred function
	completed = true

	utils_print.PrintSuccess("File stored successfully")
	fmt.Printf("  FileID:   %d\n", fileID)
	fmt.Printf("  Path:   %s\n", path)
	fmt.Printf("  SHA256: %s\n", fileHash)
	utils_print.PrintDuration(start)

	return nil
}

func isLockNotAvailable(err error) bool {
	var pqErr *pq.Error
	if errors.As(err, &pqErr) {
		return string(pqErr.Code) == "55P03"
	}
	return false
}

func lockContainerRowNowaitWithRetry(tx db.DBTX, containerID int64, attempts int, wait time.Duration) error {
	for attempt := 0; attempt < attempts; attempt++ {
		_, err := tx.Exec(`SELECT id FROM container WHERE id = $1 FOR UPDATE NOWAIT`, containerID)
		if err == nil {
			return nil
		}
		if !isLockNotAvailable(err) {
			return err
		}
		if attempt < attempts-1 {
			time.Sleep(wait)
		}
	}

	return fmt.Errorf("%w for container %d after %d attempts", errContainerLockContention, containerID, attempts)
}

func StoreFolder(root string) error {
	codec, err := blocks.LoadDefaultCodec()
	if err != nil {
		return err
	}

	return storeFolder(root, codec)
}

func StoreFolderWithCodec(root string, codecName string) error {
	codec, err := blocks.ParseCodec(codecName)
	if err != nil {
		return err
	}

	return storeFolder(root, codec)
}

func storeFolder(root string, codec blocks.Codec) error {
	start := time.Now()

	dbconn, err := db.ConnectDB()
	if err != nil {
		return fmt.Errorf("failed to connect DB: %w", err)
	}
	defer func() { _ = dbconn.Close() }()

	workerCount := runtime.NumCPU()

	fileChan := make(chan string, 256)
	errChan := make(chan error, workerCount)

	// Workers
	for i := 0; i < workerCount; i++ {
		go func() {
			for p := range fileChan {
				if err := StoreFileWithDBAndCodec(dbconn, p, codec); err != nil {
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

	utils_print.PrintSuccess("Folder stored successfully")
	utils_print.PrintDuration(start)

	return nil
}

// --------------------------------------------------------------------------
// --------------------------------------------------------------------------

// StoreBlock is the new version of StoreChunk that takes care of block encoding and metadata management in the blocks table
func storeChunkAsPlainBlock(
	ctx context.Context,
	tx *sql.Tx,
	repo *blocks.Repository,
	ac container.ActiveContainer,
	chunkID int64,
	chunkHash string,
	chunk []byte,
	transformer blocks.Transformer,
) (offset int64, newSize int64, desc *blocks.Descriptor, err error) {
	encoded, err := transformer.Encode(ctx, blocks.EncodeInput{
		ChunkID:   chunkID,
		ChunkHash: chunkHash,
		Plaintext: chunk,
	})
	if err != nil {
		return 0, 0, nil, err
	}

	offset, newSize, err = StoreBlockPayload(ac.Container, encoded.Payload)
	if err != nil {
		return 0, 0, nil, err
	}

	encoded.Descriptor.ContainerID = ac.ID
	encoded.Descriptor.BlockOffset = offset

	if err := repo.Insert(ctx, tx, &encoded.Descriptor); err != nil {
		return 0, 0, nil, err
	}

	return offset, newSize, &encoded.Descriptor, nil
}

// new store payload data into a container directly, without the chunk record wrapper
func StoreBlockPayload(c container.Container, payload []byte) (offset int64, newSize int64, err error) {
	offset, err = c.Append(payload)
	if err != nil {
		return 0, 0, err
	}

	return offset, offset + int64(len(payload)), nil
}

func SyncCloseAndSealContainer(tx db.DBTX, activecontainer container.ActiveContainer) error {
	// sync active container to disk
	if err := activecontainer.Container.Sync(); err != nil {
		return err
	}
	// close active container file
	if err := activecontainer.Container.Close(); err != nil {
		return err
	}
	// seal active container in DB
	if err := container.SealContainer(tx, activecontainer.ID, activecontainer.Filename); err != nil {
		return err
	}

	return nil
}
