package chunk

// Chunker defines a versioned file chunking strategy.
//
// Implementations are expected to be deterministic for a given Version and
// input bytes. Once a Version is published and used to write repository data,
// its behavior is treated as immutable for compatibility.
type Chunker interface {
	Version() Version
	ChunkFile(path string) ([]Result, error)
}
