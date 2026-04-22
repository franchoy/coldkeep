package chunk

const MaxChunkSize = 2 * 1024 * 1024

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
