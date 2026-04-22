package simplecdc

import (
	"io"
	"os"

	"github.com/franchoy/coldkeep/internal/chunk/shared"
)

const (
	Version      = shared.VersionV1SimpleRolling
	minChunkSize = 512 * 1024
	MaxChunkSize = 2 * 1024 * 1024
	mask         = 0x3FFFF
)

type Chunker struct{}

func New() Chunker {
	return Chunker{}
}

func (c Chunker) Version() shared.Version {
	return Version
}

func (c Chunker) ChunkFile(filePath string) ([]shared.Result, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer func() { _ = file.Close() }()

	var chunks []shared.Result
	buffer := make([]byte, 0, MaxChunkSize)
	var rolling uint32
	var offset int64

	temp := make([]byte, 32*1024)

	for {
		n, err := file.Read(temp)
		if n > 0 {
			for i := 0; i < n; i++ {
				b := temp[i]
				buffer = append(buffer, b)
				rolling = (rolling << 1) + uint32(b)

				if len(buffer) >= minChunkSize && ((rolling&mask) == 0 || len(buffer) >= MaxChunkSize) {
					chunk := make([]byte, len(buffer))
					copy(chunk, buffer)
					chunks = append(chunks, shared.Result{
						Info: shared.Info{
							Size:   int64(len(chunk)),
							Offset: offset,
						},
						Data: chunk,
					})
					offset += int64(len(chunk))
					buffer = make([]byte, 0, MaxChunkSize)
					rolling = 0
				}
			}
		}

		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
	}

	if len(buffer) > 0 {
		chunk := make([]byte, len(buffer))
		copy(chunk, buffer)
		chunks = append(chunks, shared.Result{
			Info: shared.Info{
				Size:   int64(len(chunk)),
				Offset: offset,
			},
			Data: chunk,
		})
	}

	return chunks, nil
}
