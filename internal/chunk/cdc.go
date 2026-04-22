package chunk

import "github.com/franchoy/coldkeep/internal/chunk/simplecdc"

const MaxChunkSize = simplecdc.MaxChunkSize

func ChunkFile(filePath string) ([][]byte, error) {
	results, err := defaultChunker.ChunkFile(filePath)
	if err != nil {
		return nil, err
	}

	chunks := make([][]byte, 0, len(results))
	for _, result := range results {
		chunks = append(chunks, result.Data)
	}

	return chunks, nil
}
