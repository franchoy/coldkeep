package main

import (
	"database/sql"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
)

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

	if err := os.MkdirAll(filepath.Dir(outputPath), 0777); err != nil {
		log.Fatal(err)
	}

	dst, err := os.OpenFile(outputPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0666)
	if err != nil {
		log.Fatal(err)
	}
	defer dst.Close()

	for rows.Next() {
		var offset int64
		var size int
		var filename string
		var algo sql.NullString

		if err := rows.Scan(&offset, &size, &filename, &algo); err != nil {
			log.Fatal(err)
		}

		algoVal := CompressionNone
		if algo.Valid && algo.String != "" {
			algoVal = CompressionType(algo.String)
		}

		containerPath := filepath.Join("/storage/containers", filename)
		// If container is compressed, the on-disk filename includes ".<algo>"
		if algoVal != CompressionNone {
			containerPath = containerPath + "." + string(algoVal)
		}

		reader, err := OpenDecompressionReader(containerPath, algoVal)
		if err != nil {
			log.Fatal(err)
		}

		fullData, err := io.ReadAll(reader)
		_ = reader.Close()
		if err != nil {
			log.Fatal(err)
		}

		chunkData := fullData[offset : offset+int64(size)]
		if _, err := dst.Write(chunkData); err != nil {
			log.Fatal(err)
		}
	}

	fmt.Println("File restored.")
}
