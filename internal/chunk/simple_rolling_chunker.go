package chunk

import "github.com/franchoy/coldkeep/internal/chunk/simplecdc"

type simpleRollingChunker struct {
	impl *simplecdc.Chunker
}

func newSimpleRollingChunker() Chunker {
	return &simpleRollingChunker{impl: simplecdc.New()}
}

func (c *simpleRollingChunker) Version() Version {
	return VersionV1SimpleRolling
}

func (c *simpleRollingChunker) ChunkFile(path string) ([][]byte, error) {
	return c.impl.ChunkFile(path)
}
