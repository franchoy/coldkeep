package benchmark

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
)

const (
	patternRandom   = "random"
	patternRepeated = "repeated"
	patternMixed    = "mixed"

	mixedBlockSize = 4096
)

// DatasetConfig controls synthetic dataset generation for benchmark runs.
type DatasetConfig struct {
	NumFiles      int
	FileSizeBytes int
	Pattern       string // "random" | "repeated" | "mixed"
	Seed          int64
}

// GenerateDataset creates a deterministic benchmark dataset under baseDir.
//
// Every generated file is named deterministically as:
// file_0001.bin, file_0002.bin, ...
func GenerateDataset(baseDir string, cfg DatasetConfig) error {
	if err := validateDatasetConfig(cfg); err != nil {
		return err
	}
	if err := os.MkdirAll(baseDir, 0o755); err != nil {
		return fmt.Errorf("create dataset directory %q: %w", baseDir, err)
	}

	rng := rand.New(rand.NewSource(cfg.Seed))
	var repeatedTemplate []byte
	if cfg.Pattern == patternRepeated || cfg.Pattern == patternMixed {
		repeatedTemplate = make([]byte, mixedBlockSize)
		fillRandomBytes(rng, repeatedTemplate)
	}

	for i := 0; i < cfg.NumFiles; i++ {
		name := fmt.Sprintf("file_%04d.bin", i+1)
		path := filepath.Join(baseDir, name)

		data := make([]byte, cfg.FileSizeBytes)
		switch cfg.Pattern {
		case patternRandom:
			fillRandomBytes(rng, data)
		case patternRepeated:
			fillWithTemplate(data, repeatedTemplate)
		case patternMixed:
			fillMixedContent(rng, data, repeatedTemplate)
		default:
			return fmt.Errorf("unsupported dataset pattern %q", cfg.Pattern)
		}

		if err := os.WriteFile(path, data, 0o600); err != nil {
			return fmt.Errorf("write dataset file %q: %w", path, err)
		}
	}

	return nil
}

func validateDatasetConfig(cfg DatasetConfig) error {
	if cfg.NumFiles <= 0 {
		return fmt.Errorf("num files must be > 0")
	}
	if cfg.FileSizeBytes <= 0 {
		return fmt.Errorf("file size bytes must be > 0")
	}
	switch cfg.Pattern {
	case patternRandom, patternRepeated, patternMixed:
		return nil
	default:
		return fmt.Errorf("pattern must be one of %q, %q, %q", patternRandom, patternRepeated, patternMixed)
	}
}

func fillRandomBytes(rng *rand.Rand, data []byte) {
	for i := range data {
		data[i] = byte(rng.Intn(256))
	}
}

func fillWithTemplate(dst []byte, template []byte) {
	for pos := 0; pos < len(dst); {
		n := copy(dst[pos:], template)
		pos += n
	}
}

func fillMixedContent(rng *rand.Rand, dst []byte, repeatedTemplate []byte) {
	for start := 0; start < len(dst); start += mixedBlockSize {
		end := start + mixedBlockSize
		if end > len(dst) {
			end = len(dst)
		}

		if rng.Float64() < 0.7 {
			copy(dst[start:end], repeatedTemplate[:end-start])
			continue
		}
		fillRandomBytes(rng, dst[start:end])
	}
}
