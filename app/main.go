package main

import (
	"compress/gzip"
	"crypto/sha256"
	"database/sql"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/klauspost/compress/zstd"
	_ "github.com/lib/pq"
)

const containerMaxSize int64 = 64 * 1024 * 1024

var defaultCompression = CompressionZstd // change if needed

var containerMutex sync.Mutex

func main() {

	if len(os.Args) < 2 {
		log.Fatal("Usage: capsule <store|restore>")
	}

	switch os.Args[1] {

	case "store":
		storeFile(os.Args[2])

	case "store-folder":
		storeFolder(os.Args[2])

	case "restore":
		restoreFile(os.Args[2], os.Args[3])

	case "remove":
		removeFile(os.Args[2])

	case "gc":
		runGC()

	case "stats":
		runStats()

	default:
		log.Fatal("Unknown command")
	}

}

func connectDB() *sql.DB {

	connStr := "host=" + os.Getenv("DB_HOST") +
		" port=" + os.Getenv("DB_PORT") +
		" user=" + os.Getenv("DB_USER") +
		" password=" + os.Getenv("DB_PASSWORD") +
		" dbname=" + os.Getenv("DB_NAME") +
		" sslmode=disable"

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Fatal(err)
	}

	return db
}

//
// ====================== STORE ======================
//

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

			// Try inserting with real container info
			err = db.QueryRow(`
			INSERT INTO chunk (sha256, size, container_id, chunk_offset, ref_count)
			VALUES ($1, $2, $3, $4, 1)
			ON CONFLICT (sha256) DO NOTHING
			RETURNING id
		`, hash, len(chunkData), containerID, offset).Scan(&chunkID)

			if err == sql.ErrNoRows {
				// Conflict happened → another goroutine inserted it
				// So reuse instead
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

//
// ====================== RESTORE ======================
//

func restoreFile(idStr string, outputPath string) {

	db := connectDB()
	defer db.Close()

	rows, err := db.Query(`
		SELECT c.chunk_offset, c.size, ct.filename, ct.compression_algorithm
		FROM file_chunk fc
		JOIN chunk c ON fc.chunk_id = c.id
		JOIN container ct ON c.container_id = ct.id
		WHERE fc.file_id = $1
		ORDER BY fc.chunk_order ASC
	`, idStr)

	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	os.MkdirAll(filepath.Dir(outputPath), 0777)

	dst, err := os.OpenFile(outputPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0666)
	if err != nil {
		log.Fatal(err)
	}
	defer dst.Close()

	for rows.Next() {

		var offset int64
		var size int
		var filename string
		var algo string

		rows.Scan(&offset, &size, &filename, &algo)

		containerPath := filepath.Join("/storage/containers", filename)

		reader, err := openReader(containerPath, CompressionType(algo))
		if err != nil {
			log.Fatal(err)
		}

		fullData, err := io.ReadAll(reader)
		reader.Close()
		if err != nil {
			log.Fatal(err)
		}

		chunkData := fullData[offset : offset+int64(size)]
		dst.Write(chunkData)
	}

	fmt.Println("File restored.")
}

//
// ====================== CONTAINER ======================
//

func getOrCreateOpenContainer(db *sql.DB) (int, string, int64) {

	var id int
	var filename string
	var currentSize int64

	err := db.QueryRow(`
		SELECT id, filename, current_size
		FROM container
		WHERE sealed=FALSE
		LIMIT 1
	`).Scan(&id, &filename, &currentSize)

	if err == nil {
		return id, filename, currentSize
	}

	filename = fmt.Sprintf("container_%d.bin", time.Now().UnixNano())

	err = db.QueryRow(`
		INSERT INTO container (filename, current_size, max_size, sealed)
		VALUES ($1, 0, $2, FALSE)
		RETURNING id
	`, filename, containerMaxSize).Scan(&id)

	if err != nil {
		log.Fatal(err)
	}

	return id, filename, 0
}

func appendChunk(db *sql.DB, containerID int, filename string, currentSize int64, data []byte) (int64, error) {

	path := filepath.Join("/storage/containers", filename)

	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	offset := currentSize

	_, err = f.Write(data)
	if err != nil {
		return 0, err
	}

	newSize := currentSize + int64(len(data))

	_, err = db.Exec(`UPDATE container SET current_size=$1 WHERE id=$2`,
		newSize, containerID)
	if err != nil {
		return 0, err
	}

	if newSize >= containerMaxSize {

		_, err = db.Exec(`UPDATE container SET sealed=TRUE WHERE id=$1`,
			containerID)
		if err != nil {
			return 0, err
		}

		compressAndMark(db, containerID, filename)
	}

	return offset, nil
}

//
// ====================== COMPRESSION ======================
//

func compressAndMark(db *sql.DB, containerID int, filename string) {

	path := filepath.Join("/storage/containers", filename)

	newPath, size, err := compressFile(path, defaultCompression)
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

func compressFile(path string, algo CompressionType) (string, int64, error) {

	if algo == CompressionNone {
		info, _ := os.Stat(path)
		return path, info.Size(), nil
	}

	input, _ := os.Open(path)
	defer input.Close()

	outputPath := path + "." + string(algo)
	output, _ := os.Create(outputPath)
	defer output.Close()

	var writer io.WriteCloser

	if algo == CompressionGzip {
		writer = gzip.NewWriter(output)
	} else {
		writer, _ = zstd.NewWriter(output)
	}

	io.Copy(writer, input)
	writer.Close()
	output.Close()
	input.Close()

	os.Remove(path)

	info, _ := os.Stat(outputPath)
	return outputPath, info.Size(), nil
}

func openReader(path string, algo CompressionType) (io.ReadCloser, error) {

	switch algo {

	case CompressionNone:
		return os.Open(path)

	case CompressionGzip:
		f, err := os.Open(path + ".gzip")
		if err != nil {
			return nil, err
		}
		gr, err := gzip.NewReader(f)
		if err != nil {
			f.Close()
			return nil, err
		}
		return struct {
			io.Reader
			io.Closer
		}{
			Reader: gr,
			Closer: f,
		}, nil

	case CompressionZstd:
		f, err := os.Open(path + ".zstd")
		if err != nil {
			return nil, err
		}

		decoder, err := zstd.NewReader(f)
		if err != nil {
			f.Close()
			return nil, err
		}

		return &zstdReadCloser{
			decoder: decoder,
			file:    f,
		}, nil

	default:
		return os.Open(path)
	}
}

//
// ====================== CDC (same as before) ======================
//

const (
	minChunkSize = 512 * 1024
	maxChunkSize = 2 * 1024 * 1024
	mask         = 0x3FFFF
)

func chunkFile(filePath string) ([][]byte, error) {

	file, _ := os.Open(filePath)
	defer file.Close()

	var chunks [][]byte
	buffer := make([]byte, 0, maxChunkSize)
	var rolling uint32

	temp := make([]byte, 32*1024)

	for {
		n, err := file.Read(temp)
		if n > 0 {

			for i := 0; i < n; i++ {

				b := temp[i]
				buffer = append(buffer, b)
				rolling = (rolling << 1) + uint32(b)

				if len(buffer) >= minChunkSize &&
					((rolling&mask) == 0 || len(buffer) >= maxChunkSize) {

					chunk := make([]byte, len(buffer))
					copy(chunk, buffer)
					chunks = append(chunks, chunk)
					buffer = make([]byte, 0, maxChunkSize)
					rolling = 0
				}
			}
		}

		if err == io.EOF {
			break
		}
	}

	if len(buffer) > 0 {
		chunk := make([]byte, len(buffer))
		copy(chunk, buffer)
		chunks = append(chunks, chunk)
	}

	return chunks, nil
}

func removeFile(fileID string) {

	db := connectDB()
	defer db.Close()

	// Check existence first
	var exists bool
	err := db.QueryRow(
		"SELECT EXISTS (SELECT 1 FROM logical_file WHERE id=$1)",
		fileID,
	).Scan(&exists)

	if err != nil {
		log.Fatal(err)
	}

	if !exists {
		fmt.Println("File ID", fileID, "not found.")
		return
	}

	tx, err := db.Begin()
	if err != nil {
		log.Fatal(err)
	}

	// Get chunk IDs
	rows, err := tx.Query(`
		SELECT chunk_id
		FROM file_chunk
		WHERE file_id = $1
	`, fileID)
	if err != nil {
		tx.Rollback()
		log.Fatal(err)
	}
	defer rows.Close()

	var chunkIDs []int

	for rows.Next() {
		var id int
		rows.Scan(&id)
		chunkIDs = append(chunkIDs, id)
	}

	// Decrement ref_count
	for _, chunkID := range chunkIDs {
		_, err := tx.Exec(`
			UPDATE chunk
			SET ref_count = ref_count - 1
			WHERE id = $1
		`, chunkID)
		if err != nil {
			tx.Rollback()
			log.Fatal(err)
		}
	}

	// Remove mappings
	_, err = tx.Exec(`DELETE FROM file_chunk WHERE file_id = $1`, fileID)
	if err != nil {
		tx.Rollback()
		log.Fatal(err)
	}

	// Remove logical file
	_, err = tx.Exec(`DELETE FROM logical_file WHERE id = $1`, fileID)
	if err != nil {
		tx.Rollback()
		log.Fatal(err)
	}

	err = tx.Commit()
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Logical file", fileID, "removed. Ref counts updated.")
}

func runGC() {

	db := connectDB()
	defer db.Close()

	rows, err := db.Query(`
		SELECT c.id, c.filename, c.compression_algorithm
		FROM container c
		WHERE NOT EXISTS (
			SELECT 1 FROM chunk ch
			WHERE ch.container_id = c.id
			AND ch.ref_count > 0
		)
	`)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	var deletedContainers int

	for rows.Next() {

		var containerID int
		var filename string
		var algo string

		rows.Scan(&containerID, &filename, &algo)

		containerPath := filepath.Join("/storage/containers", filename)

		if algo != "none" {
			containerPath += "." + algo
		}

		// Delete file from disk
		err := os.Remove(containerPath)
		if err != nil {
			log.Println("Failed to delete file:", err)
			continue
		}

		// Delete chunk rows
		_, err = db.Exec(`
			DELETE FROM chunk
			WHERE container_id = $1
		`, containerID)
		if err != nil {
			log.Fatal(err)
		}

		// Delete container row
		_, err = db.Exec(`
			DELETE FROM container
			WHERE id = $1
		`, containerID)
		if err != nil {
			log.Fatal(err)
		}

		deletedContainers++
		fmt.Println("Deleted container:", filename)
	}

	fmt.Printf("GC completed. Containers deleted: %d\n", deletedContainers)
}

func runStats() {

	db := connectDB()
	defer db.Close()

	var totalFiles int
	var totalLogicalSize sql.NullInt64
	var totalContainers int
	var totalContainerSize sql.NullInt64
	var totalCompressedSize sql.NullInt64
	var liveBytes sql.NullInt64
	var deadBytes sql.NullInt64

	// Logical file stats
	db.QueryRow(`SELECT COUNT(*), COALESCE(SUM(total_size),0) FROM logical_file`).
		Scan(&totalFiles, &totalLogicalSize)

	// Container stats
	db.QueryRow(`
		SELECT COUNT(*),
		       COALESCE(SUM(current_size),0),
		       COALESCE(SUM(compressed_size),0)
		FROM container
	`).Scan(&totalContainers, &totalContainerSize, &totalCompressedSize)

	// Chunk live/dead stats
	db.QueryRow(`
		SELECT
			COALESCE(SUM(CASE WHEN ref_count > 0 THEN size ELSE 0 END),0),
			COALESCE(SUM(CASE WHEN ref_count = 0 THEN size ELSE 0 END),0)
		FROM chunk
	`).Scan(&liveBytes, &deadBytes)

	fmt.Println("\n====== Capsule Stats ======")

	fmt.Printf("Logical files:           %d\n", totalFiles)
	fmt.Printf("Logical stored size:     %.2f MB\n",
		float64(totalLogicalSize.Int64)/(1024*1024))

	fmt.Printf("Containers:              %d\n", totalContainers)
	fmt.Printf("Raw container bytes:     %.2f MB\n",
		float64(totalContainerSize.Int64)/(1024*1024))
	fmt.Printf("Compressed bytes:        %.2f MB\n",
		float64(totalCompressedSize.Int64)/(1024*1024))

	fmt.Printf("Live chunk bytes:        %.2f MB\n",
		float64(liveBytes.Int64)/(1024*1024))
	fmt.Printf("Dead chunk bytes:        %.2f MB\n",
		float64(deadBytes.Int64)/(1024*1024))

	if totalLogicalSize.Int64 > 0 {
		dedupRatio := 1.0 - (float64(liveBytes.Int64) / float64(totalLogicalSize.Int64))
		fmt.Printf("Global dedup ratio:      %.2f%%\n", dedupRatio*100)
	}

	if totalContainerSize.Int64 > 0 {
		deadRatio := float64(deadBytes.Int64) / float64(totalContainerSize.Int64)
		fmt.Printf("Fragmentation ratio:     %.2f%%\n", deadRatio*100)
	}

	fmt.Println("============================")

	// ---- Per container breakdown ----
	fmt.Println("\nPer-container breakdown:")

	rows, err := db.Query(`
		SELECT
			c.id,
			c.filename,
			c.current_size,
			COALESCE(SUM(CASE WHEN ch.ref_count > 0 THEN ch.size ELSE 0 END),0) AS live,
			COALESCE(SUM(CASE WHEN ch.ref_count = 0 THEN ch.size ELSE 0 END),0) AS dead
		FROM container c
		LEFT JOIN chunk ch ON ch.container_id = c.id
		GROUP BY c.id
		ORDER BY c.id
	`)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	for rows.Next() {

		var id int
		var filename string
		var totalSize int64
		var live int64
		var dead int64

		rows.Scan(&id, &filename, &totalSize, &live, &dead)

		liveRatio := 0.0
		if totalSize > 0 {
			liveRatio = float64(live) / float64(totalSize) * 100
		}

		fmt.Printf("Container %d (%s): total=%.2fMB live=%.2fMB dead=%.2fMB live_ratio=%.2f%%\n",
			id,
			filename,
			float64(totalSize)/(1024*1024),
			float64(live)/(1024*1024),
			float64(dead)/(1024*1024),
			liveRatio,
		)
	}
}
