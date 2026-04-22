package chunk

// Chunker defines a file chunking strategy implementation.
type Chunker interface {
	Version() string
	ChunkFile(filePath string) ([][]byte, error)
}
