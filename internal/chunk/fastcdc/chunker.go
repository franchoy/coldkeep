// Package fastcdc provides the isolated v2-fastcdc chunker implementation.
//
// Phase 5 scope rule: this package must remain implementation-only until later
// steps explicitly register/select it. Adding this package must not change
// production defaults.
package fastcdc

import (
	"io"
	"os"

	"github.com/franchoy/coldkeep/internal/chunk/shared"
)

const (
	// Version is the persisted identifier for this chunker generation.
	Version = shared.Version("v2-fastcdc")

	// Tuning targets for the first v2-fastcdc implementation.
	//
	// Phase 5 comparison rule: keep target sizes aligned for fair behavior
	// comparisons while we validate implementation isolation.
	MinChunkSize = 32 * 1024
	AvgChunkSize = 64 * 1024
	MaxChunkSize = 128 * 1024

	// Between MinChunkSize and AvgChunkSize, cut only on a stricter condition.
	strictMask = (1 << 16) - 1
	// Between AvgChunkSize and MaxChunkSize, cut on a looser condition.
	normalMask = (1 << 15) - 1
)

var gearTable = initGearTable()

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

	results := make([]shared.Result, 0)
	buffer := make([]byte, 0, MaxChunkSize)
	chunkOffset := int64(0)
	var fp uint64

	tmp := make([]byte, 64*1024)
	for {
		n, readErr := file.Read(tmp)
		if n > 0 {
			for i := 0; i < n; i++ {
				b := tmp[i]
				buffer = append(buffer, b)
				fp = (fp << 1) + gearTable[b]

				if shouldCut(len(buffer), fp) {
					chunkData := make([]byte, len(buffer))
					copy(chunkData, buffer)
					results = append(results, shared.Result{
						Info: shared.Info{Size: int64(len(chunkData)), Offset: chunkOffset},
						Data: chunkData,
					})
					chunkOffset += int64(len(chunkData))
					buffer = buffer[:0]
					fp = 0
				}
			}
		}

		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return nil, readErr
		}
	}

	if len(buffer) > 0 {
		chunkData := make([]byte, len(buffer))
		copy(chunkData, buffer)
		results = append(results, shared.Result{
			Info: shared.Info{Size: int64(len(chunkData)), Offset: chunkOffset},
			Data: chunkData,
		})
	}

	return results, nil
}

func shouldCut(size int, fp uint64) bool {
	if size < MinChunkSize {
		return false
	}

	// min -> avg: strict cut mask
	if size < AvgChunkSize {
		return (fp & strictMask) == 0
	}

	// avg -> max: normal/looser cut mask, with force-cut cap at max
	return (fp&normalMask) == 0 || size >= MaxChunkSize
}

func initGearTable() [256]uint64 {
	// Deterministically generate a 256-entry table via SplitMix64 so chunk
	// boundaries are stable across platforms and process runs.
	var table [256]uint64
	var state uint64 = 0x9e3779b97f4a7c15
	for i := 0; i < len(table); i++ {
		state += 0x9e3779b97f4a7c15
		z := state
		z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9
		z = (z ^ (z >> 27)) * 0x94d049bb133111eb
		table[i] = z ^ (z >> 31)
	}
	return table
}
