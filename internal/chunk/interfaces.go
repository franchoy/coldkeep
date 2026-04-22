package chunk

// Chunker defines a file chunking strategy implementation.
type Chunker interface {
	Version() Version
	ChunkFile(path string) ([][]byte, error)
}
