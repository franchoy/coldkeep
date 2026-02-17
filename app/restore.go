package main

import (
	"bytes"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
)

type chunkRef struct {
	Order    int
	Offset   int64
	Size     int
	Filename string
	Algo     CompressionType
}

func restoreFile(idStr string, outputPath string) {
	db := connectDB()
	defer db.Close()

	rows, err := db.Query(`
		SELECT fc.chunk_order,
		       c.chunk_offset,
		       c.size,
		       ct.filename,
		       ct.compression_algorithm
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

	// Collect all chunks first
	var chunks []chunkRef

	for rows.Next() {
		var r chunkRef
		var algo sql.NullString

		if err := rows.Scan(&r.Order, &r.Offset, &r.Size, &r.Filename, &algo); err != nil {
			log.Fatal(err)
		}

		r.Algo = CompressionNone
		if algo.Valid && algo.String != "" {
			r.Algo = CompressionType(algo.String)
		}

		chunks = append(chunks, r)
	}

	if err := rows.Err(); err != nil {
		log.Fatal(err)
	}

	if len(chunks) == 0 {
		log.Fatal("no chunks found for file")
	}

	if err := os.MkdirAll(filepath.Dir(outputPath), 0777); err != nil {
		log.Fatal(err)
	}

	out, err := os.OpenFile(outputPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0666)
	if err != nil {
		log.Fatal(err)
	}
	defer out.Close()

	// Group chunks by container filename
	containerMap := make(map[string][]chunkRef)

	for _, c := range chunks {
		containerMap[c.Filename] = append(containerMap[c.Filename], c)
	}

	// Process containers one by one
	for filename, chunkList := range containerMap {

		algo := chunkList[0].Algo

		containerPath := filepath.Join("/storage/containers", filename)
		if algo != CompressionNone {
			containerPath = containerPath + "." + string(algo)
		}

		reader, err := OpenDecompressionReader(containerPath, algo)
		if err != nil {
			log.Fatal(err)
		}

		fullData, err := io.ReadAll(reader)
		_ = reader.Close()
		if err != nil {
			log.Fatal(err)
		}

		for _, c := range chunkList {

			offset := c.Offset
			expectedSize := c.Size

			const recordHeaderSize = 32 + 4

			if offset < 0 || offset+recordHeaderSize > int64(len(fullData)) {
				log.Fatalf("invalid chunk offset %d in container %s", offset, filename)
			}

			// Read stored hash
			storedHash := fullData[offset : offset+32]

			// Read stored size
			sizeBuf := fullData[offset+32 : offset+36]
			storedSize := int(binary.LittleEndian.Uint32(sizeBuf))

			if storedSize != expectedSize {
				log.Fatalf(
					"chunk size mismatch at offset %d in container %s (db=%d header=%d)",
					offset, filename, expectedSize, storedSize,
				)
			}

			dataStart := offset + recordHeaderSize
			dataEnd := dataStart + int64(storedSize)

			if dataEnd > int64(len(fullData)) {
				log.Fatalf("chunk data out of bounds at offset %d in container %s", offset, filename)
			}

			chunkData := fullData[dataStart:dataEnd]

			// Verify hash integrity
			computed := sha256.Sum256(chunkData)
			if !bytes.Equal(storedHash, computed[:]) {
				log.Fatalf("chunk hash mismatch at offset %d in container %s", offset, filename)
			}

			if _, err := out.Write(chunkData); err != nil {
				log.Fatal(err)
			}
		}
	}

	fmt.Println("File restored.")
}
