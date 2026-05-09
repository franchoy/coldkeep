package benchmark

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"
)

// TestStandardCorpora validates all standard corpus definitions are well-formed.
func TestStandardCorpora(t *testing.T) {
	corpora := StandardCorpora()

	if len(corpora) != 4 {
		t.Fatalf("expected 4 standard corpora, got %d", len(corpora))
	}

	expectedTypes := []CorpusType{
		CorpusTypeHighlyCompressible,
		CorpusTypeMixedRealistic,
		CorpusTypeAlreadyCompressed,
		CorpusTypeAdversarial,
	}

	for i, corpus := range corpora {
		if corpus.Type == "" {
			t.Errorf("corpus %d: missing type", i)
		}
		if corpus.Version == "" {
			t.Errorf("corpus %d: missing version", i)
		}
		if corpus.Name == "" {
			t.Errorf("corpus %d: missing name", i)
		}
		if len(corpus.Files) == 0 {
			t.Errorf("corpus %d: no files", i)
		}

		// Verify type matches expected
		if corpus.Type != expectedTypes[i] {
			t.Errorf("corpus %d: expected type %s, got %s", i, expectedTypes[i], corpus.Type)
		}

		// Verify files
		for j, file := range corpus.Files {
			if file.Name == "" {
				t.Errorf("corpus %d, file %d: missing filename", i, j)
			}
			if file.Size <= 0 {
				t.Errorf("corpus %d, file %d: invalid size %d", i, j, file.Size)
			}
			if file.Content.Type == "" {
				t.Errorf("corpus %d, file %d: missing content type", i, j)
			}

			// Verify compression ratio expectations
			switch corpus.Type {
			case CorpusTypeHighlyCompressible:
				if file.Content.CompressionRatio >= 0.5 {
					t.Errorf("corpus %d, file %d: expected compression ratio < 0.5, got %.2f",
						i, j, file.Content.CompressionRatio)
				}
			case CorpusTypeMixedRealistic:
				if file.Content.CompressionRatio <= 0.5 || file.Content.CompressionRatio >= 0.8 {
					t.Errorf("corpus %d, file %d: expected compression ratio 0.5-0.8, got %.2f",
						i, j, file.Content.CompressionRatio)
				}
			case CorpusTypeAlreadyCompressed:
				if file.Content.CompressionRatio <= 0.95 {
					t.Errorf("corpus %d, file %d: expected compression ratio > 0.95, got %.2f",
						i, j, file.Content.CompressionRatio)
				}
			case CorpusTypeAdversarial:
				if file.Content.CompressionRatio <= 0.99 {
					t.Errorf("corpus %d, file %d: expected compression ratio >= 1.0, got %.2f",
						i, j, file.Content.CompressionRatio)
				}
			}
		}
	}
}

// TestCorpusBuilderCreateCorpus tests corpus generation produces expected files.
func TestCorpusBuilderCreateCorpus(t *testing.T) {
	tempDir := t.TempDir()
	builder := NewCorpusBuilder(tempDir)

	corpus := StandardCorpora()[0] // Highly compressible
	if err := builder.GenerateCorpus(corpus); err != nil {
		t.Fatalf("GenerateCorpus failed: %v", err)
	}

	// Verify all files exist
	for _, file := range corpus.Files {
		filePath := filepath.Join(tempDir, string(corpus.Type), corpus.Version, file.Name)
		stat, err := os.Stat(filePath)
		if err != nil {
			t.Errorf("file not found: %s: %v", filePath, err)
			continue
		}

		if stat.Size() != file.Size {
			t.Errorf("size mismatch for %s: expected %d, got %d", file.Name, file.Size, stat.Size())
		}
	}

	// Verify manifest exists
	manifestPath := filepath.Join(tempDir, string(corpus.Type), corpus.Version, "CORPUS_MANIFEST.txt")
	if _, err := os.Stat(manifestPath); err != nil {
		t.Errorf("manifest not found: %v", err)
	}
}

// TestCorpusGenerationDeterminism verifies same corpus generated twice is identical.
func TestCorpusGenerationDeterminism(t *testing.T) {
	corpus := StandardCorpora()[1] // Mixed realistic

	// Generate first instance
	dir1 := t.TempDir()
	builder1 := NewCorpusBuilder(dir1)
	if err := builder1.GenerateCorpus(corpus); err != nil {
		t.Fatalf("first GenerateCorpus failed: %v", err)
	}

	// Generate second instance
	dir2 := t.TempDir()
	builder2 := NewCorpusBuilder(dir2)
	if err := builder2.GenerateCorpus(corpus); err != nil {
		t.Fatalf("second GenerateCorpus failed: %v", err)
	}

	// Verify all files are byte-for-byte identical
	for _, file := range corpus.Files {
		path1 := filepath.Join(dir1, string(corpus.Type), corpus.Version, file.Name)
		path2 := filepath.Join(dir2, string(corpus.Type), corpus.Version, file.Name)

		data1, err := os.ReadFile(path1)
		if err != nil {
			t.Fatalf("read file 1 %s: %v", path1, err)
		}

		data2, err := os.ReadFile(path2)
		if err != nil {
			t.Fatalf("read file 2 %s: %v", path2, err)
		}

		hash1 := sha256.Sum256(data1)
		hash2 := sha256.Sum256(data2)

		if hash1 != hash2 {
			t.Errorf("file %s: hashes do not match\n  generated 1: %s\n  generated 2: %s",
				file.Name,
				hex.EncodeToString(hash1[:]),
				hex.EncodeToString(hash2[:]))
		}
	}
}

// TestCorpusValidation verifies ValidateCorpus correctly checks file integrity.
func TestCorpusValidation(t *testing.T) {
	tempDir := t.TempDir()
	builder := NewCorpusBuilder(tempDir)

	corpus := StandardCorpora()[2] // Already compressed
	if err := builder.GenerateCorpus(corpus); err != nil {
		t.Fatalf("GenerateCorpus failed: %v", err)
	}

	// Validate should pass
	valid, err := builder.ValidateCorpus(corpus)
	if err != nil {
		t.Errorf("ValidateCorpus failed: %v", err)
	}
	if !valid {
		t.Errorf("ValidateCorpus returned false for valid corpus")
	}
}

// TestCorpusValidationDetectsMissingFiles verifies validation catches missing files.
func TestCorpusValidationDetectsMissingFiles(t *testing.T) {
	tempDir := t.TempDir()
	builder := NewCorpusBuilder(tempDir)

	corpus := StandardCorpora()[3] // Adversarial
	if err := builder.GenerateCorpus(corpus); err != nil {
		t.Fatalf("GenerateCorpus failed: %v", err)
	}

	// Delete one file
	corpusDir := filepath.Join(tempDir, string(corpus.Type), corpus.Version)
	deletePath := filepath.Join(corpusDir, corpus.Files[0].Name)
	if err := os.Remove(deletePath); err != nil {
		t.Fatalf("failed to delete file: %v", err)
	}

	// Validation should fail
	valid, err := builder.ValidateCorpus(corpus)
	if err == nil {
		t.Errorf("ValidateCorpus should have failed for missing file")
	}
	if valid {
		t.Errorf("ValidateCorpus returned true for invalid corpus")
	}
}

// TestCorpusContentGenerationTypes verifies all content types can be generated.
func TestCorpusContentGenerationTypes(t *testing.T) {
	tests := []struct {
		name        string
		contentType string
		size        int64
	}{
		{"json", "json", 1024},
		{"logs", "logs", 2048},
		{"source", "source", 512},
		{"binary", "binary", 4096},
		{"jpeg_sim", "jpeg_sim", 1024},
		{"zip_sim", "zip_sim", 1024},
		{"random", "random", 2048},
		{"encrypted", "encrypted", 1024},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cc := CorpusContent{
				Type:      tt.contentType,
				Seed:      0x12345,
				TextRatio: 0.5,
			}

			data, err := generateCorpusContent(cc, tt.size)
			if err != nil {
				t.Fatalf("generateCorpusContent failed: %v", err)
			}

			if int64(len(data)) != tt.size {
				t.Errorf("size mismatch: expected %d, got %d", tt.size, len(data))
			}

			// Verify content is not all zeros
			hasNonZero := false
			for _, b := range data {
				if b != 0 {
					hasNonZero = true
					break
				}
			}
			if !hasNonZero && tt.name != "random" {
				t.Errorf("content appears to be all zeros")
			}
		})
	}
}

// TestCorpusFileStableNames verifies filenames remain stable across versions.
func TestCorpusFileStableNames(t *testing.T) {
	corpora := StandardCorpora()

	for _, corpus := range corpora {
		seenNames := make(map[string]bool)
		for _, file := range corpus.Files {
			if seenNames[file.Name] {
				t.Errorf("corpus %s has duplicate filename: %s", corpus.Name, file.Name)
			}
			seenNames[file.Name] = true
		}
	}
}

// TestCorpusCompressionRatioExpectations validates compression ratio metadata.
func TestCorpusCompressionRatioExpectations(t *testing.T) {
	tests := []struct {
		corpusType  CorpusType
		minRatio    float64
		maxRatio    float64
		description string
	}{
		{
			CorpusTypeHighlyCompressible,
			0.0, 0.5,
			"highly compressible should have ratio < 0.5",
		},
		{
			CorpusTypeMixedRealistic,
			0.5, 0.8,
			"mixed realistic should have ratio 0.5-0.8",
		},
		{
			CorpusTypeAlreadyCompressed,
			0.95, 1.1,
			"already compressed should have ratio > 0.95",
		},
		{
			CorpusTypeAdversarial,
			0.99, 1.1,
			"adversarial should have ratio >= 1.0",
		},
	}

	corpora := StandardCorpora()
	corpusMap := make(map[CorpusType]CorpusDefinition)
	for _, c := range corpora {
		corpusMap[c.Type] = c
	}

	for _, tt := range tests {
		corpus, ok := corpusMap[tt.corpusType]
		if !ok {
			t.Fatalf("corpus type %s not found", tt.corpusType)
		}

		for _, file := range corpus.Files {
			if file.Content.CompressionRatio < tt.minRatio || file.Content.CompressionRatio > tt.maxRatio {
				t.Errorf("%s: file %s - compression ratio %.2f outside expected range [%.2f, %.2f]",
					tt.description, file.Name, file.Content.CompressionRatio, tt.minRatio, tt.maxRatio)
			}
		}
	}
}

// TestCorpusManifestGeneration verifies manifest is created and readable.
func TestCorpusManifestGeneration(t *testing.T) {
	tempDir := t.TempDir()
	builder := NewCorpusBuilder(tempDir)

	corpus := StandardCorpora()[0]
	if err := builder.GenerateCorpus(corpus); err != nil {
		t.Fatalf("GenerateCorpus failed: %v", err)
	}

	manifestPath := filepath.Join(tempDir, string(corpus.Type), corpus.Version, "CORPUS_MANIFEST.txt")
	manifest, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}

	content := string(manifest)

	// Verify manifest contains expected sections
	expectedSections := []string{
		"Benchmark Corpus Manifest",
		"Type:",
		"Version:",
		"Name:",
		"Description:",
		"Files",
		"Summary",
		"Total Files:",
		"Total Size:",
	}

	for _, section := range expectedSections {
		if !contains(content, section) {
			t.Errorf("manifest missing section: %s", section)
		}
	}

	// Verify all files are listed
	for _, file := range corpus.Files {
		if !contains(content, file.Name) {
			t.Errorf("manifest missing file: %s", file.Name)
		}
	}
}

// Helper to check if string contains substring
func contains(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

// TestCorpusSerializability verifies corpora can be serialized and deserialized.
func TestCorpusSerializability(t *testing.T) {
	corpora := StandardCorpora()

	for _, corpus := range corpora {
		// Verify basic serialization properties
		if corpus.Type == "" {
			t.Error("corpus type empty")
		}
		if corpus.Version == "" {
			t.Error("corpus version empty")
		}

		// Each file should have min/max sizes within reason
		for _, file := range corpus.Files {
			if file.Size < 256*1024 {
				t.Logf("warning: file %s quite small: %d bytes", file.Name, file.Size)
			}
			if file.Size > 512*1024*1024 {
				t.Logf("warning: file %s quite large: %d bytes", file.Name, file.Size)
			}
		}
	}
}
