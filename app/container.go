package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"
)

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

	if _, err := f.Write(data); err != nil {
		return 0, err
	}

	newSize := currentSize + int64(len(data))

	_, err = db.Exec(`UPDATE container SET current_size=$1 WHERE id=$2`, newSize, containerID)
	if err != nil {
		return 0, err
	}

	if newSize >= containerMaxSize {
		_, err = db.Exec(`UPDATE container SET sealed=TRUE WHERE id=$1`, containerID)
		if err != nil {
			return 0, err
		}
		compressAndMark(db, containerID, filename)
	}

	return offset, nil
}

func compressAndMark(db *sql.DB, containerID int, filename string) {
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
