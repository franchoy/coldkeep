package container

import "github.com/franchoy/coldkeep/internal/chunk"

type ContainerWriter interface {
	WriteChunk(chunk chunk.Info) error
	FinalizeContainer() error
	ContainerCount() int
}
