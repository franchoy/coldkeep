package chunk

const MaxChunkSize = 2 * 1024 * 1024

func ChunkFile(filePath string) ([][]byte, error) {
	return defaultChunker.ChunkFile(filePath)
}
