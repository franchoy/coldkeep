package chunk

import "github.com/franchoy/coldkeep/internal/chunk/simplecdc"

// MaxChunkSize is the hard upper bound on chunk payload size.
// Sourced from the concrete v1-simple-rolling implementation.
const MaxChunkSize = simplecdc.MaxChunkSize

// ChunkFile is a compatibility shim retained for callers that have not yet
// migrated to the interface-based path. New code should use:
//
//	chunk.DefaultChunker().ChunkFile(path)
//
// or inject a chunk.Chunker via StorageContext.Chunker.
//
// Deprecated: transitional — will be removed once all callers use the interface.
func ChunkFile(filePath string) ([][]byte, error) {
	results, err := DefaultChunker().ChunkFile(filePath)
	if err != nil {
		return nil, err
	}
	chunks := make([][]byte, 0, len(results))
	for _, r := range results {
		chunks = append(chunks, r.Data)
	}
	return chunks, nil
}
