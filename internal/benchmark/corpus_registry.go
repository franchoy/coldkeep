package benchmark

import (
	"fmt"
	"os"
	"path/filepath"
)

// CorpusRegistry manages benchmark corpora for reproducible performance testing.
type CorpusRegistry struct {
	baseDir   string
	builder   *CorpusBuilder
	generated map[string]bool // track which corpora have been generated
}

// NewCorpusRegistry creates a new corpus registry at the specified base directory.
func NewCorpusRegistry(baseDir string) *CorpusRegistry {
	return &CorpusRegistry{
		baseDir:   baseDir,
		builder:   NewCorpusBuilder(baseDir),
		generated: make(map[string]bool),
	}
}

// EnsureCorpora ensures all standard corpora are generated and validated.
func (cr *CorpusRegistry) EnsureCorpora() error {
	corpora := StandardCorpora()

	for _, corpus := range corpora {
		key := fmt.Sprintf("%s:%s", corpus.Type, corpus.Version)
		corpusDir := filepath.Join(cr.baseDir, string(corpus.Type), corpus.Version)

		// Try to validate existing corpus
		if _, err := os.Stat(corpusDir); err == nil {
			// Directory exists, validate it
			valid, err := cr.builder.ValidateCorpus(corpus)
			if err != nil {
				// Validation error or corrupted - remove and regenerate
				if removeErr := os.RemoveAll(corpusDir); removeErr != nil {
					return fmt.Errorf("remove corrupted corpus %s: %w", key, removeErr)
				}
			} else if valid {
				// Valid- no need to regenerate
				cr.generated[key] = true
				continue
			} else {
				// Validation failed - remove and regenerate
				if removeErr := os.RemoveAll(corpusDir); removeErr != nil {
					return fmt.Errorf("remove invalid corpus %s: %w", key, removeErr)
				}
			}
		}

		// Generate corpus
		if err := cr.builder.GenerateCorpus(corpus); err != nil {
			return fmt.Errorf("generate corpus %s: %w", key, err)
		}

		cr.generated[key] = true
	}

	return nil
}

// GetCorpusPath returns the directory path for a specific corpus.
func (cr *CorpusRegistry) GetCorpusPath(corpusType CorpusType, version string) string {
	return filepath.Join(cr.baseDir, string(corpusType), version)
}

// GetCorpusFiles returns the list of files in a generated corpus.
func (cr *CorpusRegistry) GetCorpusFiles(corpusType CorpusType, version string) ([]string, error) {
	corpusDir := cr.GetCorpusPath(corpusType, version)

	entries, err := os.ReadDir(corpusDir)
	if err != nil {
		return nil, fmt.Errorf("read corpus directory %q: %w", corpusDir, err)
	}

	var files []string
	for _, entry := range entries {
		if !entry.IsDir() && entry.Name() != "CORPUS_MANIFEST.txt" {
			files = append(files, entry.Name())
		}
	}

	return files, nil
}

// GetCorpusManifest returns the manifest content for a corpus.
func (cr *CorpusRegistry) GetCorpusManifest(corpusType CorpusType, version string) (string, error) {
	manifestPath := filepath.Join(cr.GetCorpusPath(corpusType, version), "CORPUS_MANIFEST.txt")
	data, err := os.ReadFile(manifestPath)
	if err != nil {
		return "", fmt.Errorf("read corpus manifest: %w", err)
	}
	return string(data), nil
}

// CorpusStats provides summary statistics about a corpus.
type CorpusStats struct {
	Type      CorpusType
	Version   string
	Name      string
	FileCount int
	TotalSize int64
	AvgRatio  float64
	Generated bool
}

// GetCorpusStats returns statistics for a specific corpus.
func (cr *CorpusRegistry) GetCorpusStats(corpusType CorpusType) CorpusStats {
	for _, c := range StandardCorpora() {
		if c.Type == corpusType {
			key := fmt.Sprintf("%s:%s", c.Type, c.Version)
			totalSize := int64(0)
			sumRatio := 0.0

			for _, f := range c.Files {
				totalSize += f.Size
				sumRatio += f.Content.CompressionRatio
			}

			avgRatio := sumRatio / float64(len(c.Files))
			return CorpusStats{
				Type:      c.Type,
				Version:   c.Version,
				Name:      c.Name,
				FileCount: len(c.Files),
				TotalSize: totalSize,
				AvgRatio:  avgRatio,
				Generated: cr.generated[key],
			}
		}
	}

	return CorpusStats{}
}

// PrintCorpusStats prints human-readable corpus statistics.
func (cr *CorpusRegistry) PrintCorpusStats() {
	fmt.Println("Benchmark Corpora Summary")
	fmt.Println("========================")

	for _, corpus := range StandardCorpora() {
		stats := cr.GetCorpusStats(corpus.Type)
		status := "not generated"
		if stats.Generated {
			status = "ready"
		}

		fmt.Printf("\n%s (%s)\n", stats.Name, status)
		fmt.Printf("  Type: %s\n", stats.Type)
		fmt.Printf("  Version: %s\n", stats.Version)
		fmt.Printf("  Files: %d\n", stats.FileCount)
		fmt.Printf("  Total Size: %.2f MB\n", float64(stats.TotalSize)/(1024*1024))
		fmt.Printf("  Avg Compression Ratio: %.2f\n", stats.AvgRatio)
		fmt.Printf("  Description: %s\n", corpus.Description)
	}

	fmt.Println("\nNote: Compression ratio indicates expected compression effectiveness:")
	fmt.Println("  < 0.5: Highly compressible (good compression gains expected)")
	fmt.Println("  0.5-0.8: Mixed realistic (moderate compression)")
	fmt.Println("  > 0.95: Already compressed (minimal gains, expect expansion)")
}

// ValidateAllCorpora checks integrity of all generated corpora.
func (cr *CorpusRegistry) ValidateAllCorpora() []error {
	var errors []error

	for _, corpus := range StandardCorpora() {
		valid, err := cr.builder.ValidateCorpus(corpus)
		if err != nil {
			errors = append(errors, fmt.Errorf("validate %s:%s: %w", corpus.Type, corpus.Version, err))
			continue
		}
		if !valid {
			errors = append(errors, fmt.Errorf("corpus %s:%s failed validation", corpus.Type, corpus.Version))
		}
	}

	return errors
}

// CleanupCorpora removes all generated corpora to force regeneration.
func (cr *CorpusRegistry) CleanupCorpora() error {
	for _, corpus := range StandardCorpora() {
		corpusDir := cr.GetCorpusPath(corpus.Type, corpus.Version)
		if err := os.RemoveAll(corpusDir); err != nil {
			return fmt.Errorf("remove corpus %s:%s: %w", corpus.Type, corpus.Version, err)
		}
		key := fmt.Sprintf("%s:%s", corpus.Type, corpus.Version)
		delete(cr.generated, key)
	}
	return nil
}
