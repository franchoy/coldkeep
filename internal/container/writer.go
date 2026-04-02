package container

import "github.com/franchoy/coldkeep/internal/chunk"

// ContainerWriter remains the storage-context writer surface for compatibility
// with simulated and test writers; the production local write path augments this
// with append-oriented optional interfaces in internal/storage.
type ContainerWriter interface {
	WriteChunk(chunk chunk.Info) error
	FinalizeContainer() error
	ContainerCount() int
}
