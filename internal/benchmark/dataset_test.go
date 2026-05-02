package benchmark

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestGenerateDatasetDeterministicForSameConfig(t *testing.T) {
	cfg := DatasetConfig{
		NumFiles:      5,
		FileSizeBytes: 32 * 1024,
		Pattern:       "mixed",
		Seed:          42,
	}

	dirA := filepath.Join(t.TempDir(), "a")
	dirB := filepath.Join(t.TempDir(), "b")

	if err := GenerateDataset(dirA, cfg); err != nil {
		t.Fatalf("GenerateDataset dirA: %v", err)
	}
	if err := GenerateDataset(dirB, cfg); err != nil {
		t.Fatalf("GenerateDataset dirB: %v", err)
	}

	for i := 1; i <= cfg.NumFiles; i++ {
		name := fileName(i)
		first, err := os.ReadFile(filepath.Join(dirA, name))
		if err != nil {
			t.Fatalf("read first file %q: %v", name, err)
		}
		second, err := os.ReadFile(filepath.Join(dirB, name))
		if err != nil {
			t.Fatalf("read second file %q: %v", name, err)
		}
		if !bytes.Equal(first, second) {
			t.Fatalf("expected deterministic bytes for %q", name)
		}
	}
}

func TestGenerateDatasetRepeatedPatternProducesIdenticalFiles(t *testing.T) {
	cfg := DatasetConfig{
		NumFiles:      4,
		FileSizeBytes: 8 * 1024,
		Pattern:       "repeated",
		Seed:          7,
	}

	dir := t.TempDir()
	if err := GenerateDataset(dir, cfg); err != nil {
		t.Fatalf("GenerateDataset: %v", err)
	}

	base, err := os.ReadFile(filepath.Join(dir, fileName(1)))
	if err != nil {
		t.Fatalf("read base file: %v", err)
	}
	for i := 2; i <= cfg.NumFiles; i++ {
		candidate, err := os.ReadFile(filepath.Join(dir, fileName(i)))
		if err != nil {
			t.Fatalf("read candidate file %d: %v", i, err)
		}
		if !bytes.Equal(base, candidate) {
			t.Fatalf("expected repeated dataset files to be identical: %d", i)
		}
	}
}

func TestGenerateDatasetFileNamesAndSizes(t *testing.T) {
	cfg := DatasetConfig{
		NumFiles:      3,
		FileSizeBytes: 12345,
		Pattern:       "random",
		Seed:          99,
	}

	dir := t.TempDir()
	if err := GenerateDataset(dir, cfg); err != nil {
		t.Fatalf("GenerateDataset: %v", err)
	}

	for i := 1; i <= cfg.NumFiles; i++ {
		name := fileName(i)
		info, err := os.Stat(filepath.Join(dir, name))
		if err != nil {
			t.Fatalf("expected file %q: %v", name, err)
		}
		if got := int(info.Size()); got != cfg.FileSizeBytes {
			t.Fatalf("file %q size mismatch: got=%d want=%d", name, got, cfg.FileSizeBytes)
		}
	}
}

func TestGenerateDatasetInvalidConfig(t *testing.T) {
	tests := []DatasetConfig{
		{NumFiles: 0, FileSizeBytes: 1, Pattern: "random", Seed: 1},
		{NumFiles: 1, FileSizeBytes: 0, Pattern: "random", Seed: 1},
		{NumFiles: 1, FileSizeBytes: 1, Pattern: "unknown", Seed: 1},
	}

	for _, cfg := range tests {
		if err := GenerateDataset(t.TempDir(), cfg); err == nil {
			t.Fatalf("expected error for invalid config: %+v", cfg)
		}
	}
}

func TestGenerateDatasetMixedHasBothStableAndNoisySegments(t *testing.T) {
	cfg := DatasetConfig{
		NumFiles:      2,
		FileSizeBytes: 64 * 1024,
		Pattern:       "mixed",
		Seed:          123,
	}

	dir := t.TempDir()
	if err := GenerateDataset(dir, cfg); err != nil {
		t.Fatalf("GenerateDataset: %v", err)
	}

	first, err := os.ReadFile(filepath.Join(dir, fileName(1)))
	if err != nil {
		t.Fatalf("read first mixed file: %v", err)
	}
	second, err := os.ReadFile(filepath.Join(dir, fileName(2)))
	if err != nil {
		t.Fatalf("read second mixed file: %v", err)
	}

	if bytes.Equal(first, second) {
		t.Fatal("expected mixed pattern files to differ due to noise segments")
	}

	sharedPrefix := 0
	for i := 0; i < len(first) && i < len(second); i++ {
		if first[i] == second[i] {
			sharedPrefix++
		}
	}
	if sharedPrefix == 0 {
		t.Fatal("expected mixed pattern files to retain some shared bytes")
	}
}

func fileName(i int) string {
	return fmt.Sprintf("file_%04d.bin", i)
}
