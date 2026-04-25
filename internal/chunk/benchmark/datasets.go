package benchmark

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/rand"
)

// Variant is one deterministic dataset input to feed through a chunker.
type Variant struct {
	Name string
	Data []byte
}

// Dataset groups a baseline input with realistic deterministic mutations.
type Dataset struct {
	Name      string
	Base      Variant
	Mutations []Variant
}

// Variants returns the baseline followed by all deterministic mutations.
func (d Dataset) Variants() []Variant {
	variants := make([]Variant, 0, 1+len(d.Mutations))
	variants = append(variants, d.Base)
	variants = append(variants, d.Mutations...)
	return variants
}

// DefaultDatasets returns the initial deterministic benchmark corpus for
// chunker validation and comparison work. The corpus is intentionally static so
// results are comparable over time and across machines.
func DefaultDatasets() []Dataset {
	randomBase := GenerateBase(1337, 8*1024*1024)
	modifiedBase := GenerateBase(2025, 8*1024*1024)
	shiftedBase := GenerateBase(8080, 8*1024*1024)
	structuredBase := GenerateStructuredLogData(4242, 180000)

	return []Dataset{
		{
			Name: "random-base",
			Base: Variant{Name: "base", Data: randomBase},
		},
		{
			Name: "slight-modifications",
			Base: Variant{Name: "base", Data: modifiedBase},
			Mutations: []Variant{
				{
					Name: "modified-fixed-offsets",
					Data: ModifyAtOffsets(
						modifiedBase,
						[]int{64 * 1024, 512 * 1024, 2 * 1024 * 1024, 6 * 1024 * 1024},
					),
				},
			},
		},
		{
			Name: "shifted-data",
			Base: Variant{Name: "base", Data: shiftedBase},
			Mutations: []Variant{
				{
					Name: "shifted-prefix",
					Data: ShiftData(
						shiftedBase,
						bytes.Repeat([]byte("shifted-prefix-block|"), 2048),
					),
				},
			},
		},
		{
			Name: "structured-data",
			Base: Variant{Name: "base", Data: structuredBase},
			Mutations: []Variant{
				{
					Name: "structured-modified-offsets",
					Data: ModifyAtOffsets(
						structuredBase,
						[]int{32 * 1024, 256 * 1024, 1024 * 1024, len(structuredBase) - 96*1024},
					),
				},
				{
					Name: "structured-shifted-prefix",
					Data: ShiftData(
						structuredBase,
						[]byte("ts=2026-04-25T00:00:00Z level=INFO msg=prefix-shift dataset=structured-data\n"),
					),
				},
			},
		},
	}
}

// GenerateBase produces deterministic pseudo-random bytes for reproducible
// chunker comparisons.
func GenerateBase(seed int64, size int) []byte {
	if size <= 0 {
		return nil
	}
	rng := rand.New(rand.NewSource(seed))
	data := make([]byte, size)
	for i := 0; i < len(data); i += 8 {
		value := rng.Uint64()
		var word [8]byte
		binary.LittleEndian.PutUint64(word[:], value)
		copy(data[i:], word[:])
	}
	return data
}

// ModifyAtOffsets copies data and overwrites fixed-width mutation windows at
// the requested offsets using deterministic content derived from each offset.
func ModifyAtOffsets(data []byte, offsets []int) []byte {
	mutated := cloneBytes(data)
	for index, offset := range offsets {
		if offset < 0 || offset >= len(mutated) {
			continue
		}
		replacement := deterministicMutationBlock(int64(index+1), 4096)
		replaceAt(mutated, offset, replacement)
	}
	return mutated
}

// ShiftData returns a new slice with prefix inserted at the beginning of data.
func ShiftData(data []byte, prefix []byte) []byte {
	shifted := make([]byte, 0, len(prefix)+len(data))
	shifted = append(shifted, prefix...)
	shifted = append(shifted, data...)
	return shifted
}

// GenerateStructuredLogData builds deterministic repeated text/log-like data.
func GenerateStructuredLogData(seed int64, records int) []byte {
	if records <= 0 {
		return nil
	}
	rng := rand.New(rand.NewSource(seed))
	var buffer bytes.Buffer
	buffer.Grow(records * 64)
	levels := []string{"INFO", "WARN", "DEBUG", "ERROR"}
	services := []string{"ingest", "verify", "snapshot", "restore"}
	regions := []string{"us-east", "eu-west", "ap-south"}
	for i := 0; i < records; i++ {
		fmt.Fprintf(
			&buffer,
			"ts=2026-04-25T%02d:%02d:%02dZ level=%s service=%s region=%s shard=%03d msg=record-%06d checksum=%08x\n",
			(i/3600)%24,
			(i/60)%60,
			i%60,
			levels[i%len(levels)],
			services[(i+rng.Intn(len(services)))%len(services)],
			regions[(i+rng.Intn(len(regions)))%len(regions)],
			i%128,
			i,
			rng.Uint32(),
		)
	}
	return buffer.Bytes()
}

func cloneBytes(input []byte) []byte {
	return append([]byte(nil), input...)
}

func replaceAt(target []byte, start int, replacement []byte) {
	if start < 0 {
		start = 0
	}
	if start > len(target) {
		start = len(target)
	}
	end := start + len(replacement)
	if end > len(target) {
		end = len(target)
	}
	copy(target[start:end], replacement[:end-start])
}

func deterministicMutationBlock(seed int64, size int) []byte {
	if size <= 0 {
		return nil
	}
	rng := rand.New(rand.NewSource(seed))
	block := make([]byte, size)
	for i := 0; i < len(block); i++ {
		block[i] = byte(rng.Intn(256))
	}
	return block
}
