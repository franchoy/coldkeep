package chunk

import (
	"io"
	"os"
)

const (
	minChunkSize = 512 * 1024
	MaxChunkSize = 2 * 1024 * 1024
	mask         = 0x3FFFF
)

func ChunkFile(filePath string) ([][]byte, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var chunks [][]byte
	buffer := make([]byte, 0, MaxChunkSize)
	var rolling uint32

	temp := make([]byte, 32*1024)

	for {
		n, err := file.Read(temp)
		if n > 0 {
			for i := 0; i < n; i++ {
				b := temp[i]
				buffer = append(buffer, b)
				rolling = (rolling << 1) + uint32(b)

				if len(buffer) >= minChunkSize && ((rolling&mask) == 0 || len(buffer) >= MaxChunkSize) {
					chunk := make([]byte, len(buffer))
					copy(chunk, buffer)
					chunks = append(chunks, chunk)
					buffer = make([]byte, 0, MaxChunkSize)
					rolling = 0
				}
			}
		}

		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
	}

	if len(buffer) > 0 {
		chunk := make([]byte, len(buffer))
		copy(chunk, buffer)
		chunks = append(chunks, chunk)
	}

	return chunks, nil
}
