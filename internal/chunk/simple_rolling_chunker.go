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

func (c *simpleRollingChunker) ChunkFile(path string) ([]Result, error) {
	chunks, err := c.impl.ChunkFile(path)
	if err != nil {
		return nil, err
	}

	results := make([]Result, 0, len(chunks))
	var offset int64
	for _, chunkData := range chunks {
		results = append(results, Result{
			Info: Info{
				Size:   int64(len(chunkData)),
				Offset: offset,
			},
			Data: chunkData,
		})
		offset += int64(len(chunkData))
	}

	return results, nil
}
