package benchmark

import (
	"os"
	"path/filepath"
	"testing"
)

// TestCorpusRegistry tests the corpus registry system.
func TestCorpusRegistry(t *testing.T) {
	tempDir := t.TempDir()
	registry := NewCorpusRegistry(tempDir)

	// Should successfully ensure all corpora
	if err := registry.EnsureCorpora(); err != nil {
		t.Fatalf("EnsureCorpora failed: %v", err)
	}

	// All corpora should be marked as generated
	for _, corpus := range StandardCorpora() {
		stats := registry.GetCorpusStats(corpus.Type)
		if !stats.Generated {
			t.Errorf("corpus %s not marked as generated", corpus.Type)
		}
	}
}

// TestCorpusRegistryRecoveryFromCorruption verifies registry handles corrupted corpora.
func TestCorpusRegistryRecoveryFromCorruption(t *testing.T) {
	tempDir := t.TempDir()
	registry := NewCorpusRegistry(tempDir)

	// Generate initial corpora
	if err := registry.EnsureCorpora(); err != nil {
		t.Fatalf("initial EnsureCorpora failed: %v", err)
	}

	// Corrupt a corpus by removing a file
	corpusType := CorpusTypeHighlyCompressible
	corpusVersion := "v1.0"
	corpusDir := registry.GetCorpusPath(corpusType, corpusVersion)
	corpora := StandardCorpora()
	var corpus CorpusDefinition
	for _, c := range corpora {
		if c.Type == corpusType && c.Version == corpusVersion {
			corpus = c
			break
		}
	}

	if len(corpus.Files) > 0 {
		filePath := filepath.Join(corpusDir, corpus.Files[0].Name)
		if err := os.Remove(filePath); err != nil {
			t.Fatalf("failed to corrupt corpus: %v", err)
		}

		// Registry should detect corruption and regenerate
		if err := registry.EnsureCorpora(); err != nil {
			t.Fatalf("EnsureCorpora should recover from corruption: %v", err)
		}

		// File should be restored
		if _, err := os.Stat(filePath); err != nil {
			t.Errorf("file not recovered after corruption: %v", err)
		}
	}
}

// TestCorpusRegistryGetFiles tests file retrieval.
func TestCorpusRegistryGetFiles(t *testing.T) {
	tempDir := t.TempDir()
	registry := NewCorpusRegistry(tempDir)

	if err := registry.EnsureCorpora(); err != nil {
		t.Fatalf("EnsureCorpora failed: %v", err)
	}

	// Get files from a corpus
	files, err := registry.GetCorpusFiles(CorpusTypeMixedRealistic, "v1.0")
	if err != nil {
		t.Fatalf("GetCorpusFiles failed: %v", err)
	}

	if len(files) == 0 {
		t.Errorf("expected files in corpus, got none")
	}

	// Verify manifest is not included in file list
	for _, f := range files {
		if f == "CORPUS_MANIFEST.txt" {
			t.Errorf("manifest should not be included in file list")
		}
	}
}

// TestCorpusRegistryManifest tests manifest retrieval.
func TestCorpusRegistryManifest(t *testing.T) {
	tempDir := t.TempDir()
	registry := NewCorpusRegistry(tempDir)

	if err := registry.EnsureCorpora(); err != nil {
		t.Fatalf("EnsureCorpora failed: %v", err)
	}

	manifest, err := registry.GetCorpusManifest(CorpusTypeAdversarial, "v1.0")
	if err != nil {
		t.Fatalf("GetCorpusManifest failed: %v", err)
	}

	if manifest == "" {
		t.Errorf("manifest is empty")
	}

	// Verify manifest contains expected content
	expectedContent := []string{
		"Benchmark Corpus Manifest",
		"Type:",
		"Version:",
		"Files",
		"Summary",
	}

	for _, content := range expectedContent {
		found := false
		for i := 0; i < len(manifest)-len(content); i++ {
			if manifest[i:i+len(content)] == content {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("manifest missing expected content: %s", content)
		}
	}
}

// TestCorpusRegistryStats tests statistics generation.
func TestCorpusRegistryStats(t *testing.T) {
	tempDir := t.TempDir()
	registry := NewCorpusRegistry(tempDir)

	if err := registry.EnsureCorpora(); err != nil {
		t.Fatalf("EnsureCorpora failed: %v", err)
	}

	// Check stats for each corpus type
	for _, corpus := range StandardCorpora() {
		stats := registry.GetCorpusStats(corpus.Type)

		if stats.Type != corpus.Type {
			t.Errorf("stats type mismatch: expected %s, got %s", corpus.Type, stats.Type)
		}
		if stats.FileCount != len(corpus.Files) {
			t.Errorf("stats file count mismatch: expected %d, got %d", len(corpus.Files), stats.FileCount)
		}
		if stats.TotalSize <= 0 {
			t.Errorf("stats total size invalid: %d", stats.TotalSize)
		}
		if !stats.Generated {
			t.Errorf("stats not marked as generated")
		}
	}
}

// TestCorpusRegistryCleanup tests corpus cleanup.
func TestCorpusRegistryCleanup(t *testing.T) {
	tempDir := t.TempDir()
	registry := NewCorpusRegistry(tempDir)

	// Generate corpora
	if err := registry.EnsureCorpora(); err != nil {
		t.Fatalf("EnsureCorpora failed: %v", err)
	}

	// Cleanup
	if err := registry.CleanupCorpora(); err != nil {
		t.Fatalf("CleanupCorpora failed: %v", err)
	}

	// Verify cleanup
	for _, corpus := range StandardCorpora() {
		corpusDir := registry.GetCorpusPath(corpus.Type, corpus.Version)
		if _, err := os.Stat(corpusDir); err == nil {
			t.Errorf("corpus directory still exists after cleanup: %s", corpusDir)
		}
	}
}

// TestCorpusRegistryValidateAllCorpora tests validation of all corpora.
func TestCorpusRegistryValidateAllCorpora(t *testing.T) {
	tempDir := t.TempDir()
	registry := NewCorpusRegistry(tempDir)

	// Generate corpora
	if err := registry.EnsureCorpora(); err != nil {
		t.Fatalf("EnsureCorpora failed: %v", err)
	}

	// Validate all
	errors := registry.ValidateAllCorpora()
	if len(errors) > 0 {
		for _, err := range errors {
			t.Logf("validation error: %v", err)
		}
		t.Fatalf("validation found %d errors", len(errors))
	}
}

// TestCorpusRegistryIdempotence verifies EnsureCorpora is idempotent.
func TestCorpusRegistryIdempotence(t *testing.T) {
	tempDir := t.TempDir()
	registry := NewCorpusRegistry(tempDir)

	// First call
	if err := registry.EnsureCorpora(); err != nil {
		t.Fatalf("first EnsureCorpora failed: %v", err)
	}

	// Get original file hashes
	originalHashes := make(map[string]string)
	for _, corpus := range StandardCorpora() {
		for _, file := range corpus.Files {
			filePath := filepath.Join(
				registry.GetCorpusPath(corpus.Type, corpus.Version),
				file.Name,
			)
			data, _ := os.ReadFile(filePath)
			originalHashes[filePath] = string(data[:10]) // Simple prefix check
		}
	}

	// Second call should not regenerate
	if err := registry.EnsureCorpora(); err != nil {
		t.Fatalf("second EnsureCorpora failed: %v", err)
	}

	// Verify files unchanged
	for path, origHash := range originalHashes {
		data, _ := os.ReadFile(path)
		if string(data[:10]) != origHash {
			t.Errorf("file modified on second EnsureCorpora: %s", path)
		}
	}
}
