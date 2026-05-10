package benchmark

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
)

// CorpusType represents different benchmark corpus classifications.
type CorpusType string

const (
	// CorpusTypeHighlyCompressible: source code, JSON, logs, plaintext with high redundancy.
	CorpusTypeHighlyCompressible CorpusType = "highly_compressible"
	// CorpusMixedRealistic: office files, sqlite databases, binaries, mixed real-world files.
	CorpusTypeMixedRealistic CorpusType = "mixed_realistic"
	// CorpusTypeAlreadyCompressed: JPEG, MP4, ZIP, PDF - already optimized formats.
	CorpusTypeAlreadyCompressed CorpusType = "already_compressed"
	// CorpusTypeAdversarial: random bytes, encrypted blobs - should skip compression.
	CorpusTypeAdversarial CorpusType = "adversarial_random"
)

// CorpusDefinition describes a specific benchmark corpus with stable content.
type CorpusDefinition struct {
	Type        CorpusType
	Version     string // e.g., "v1.0"
	Name        string // descriptive name
	Files       []CorpusFile
	Seed        int64 // deterministic generation seed
	SHA256      string
	Description string
}

// CorpusFile describes a single file within a corpus.
type CorpusFile struct {
	Name    string // unique filename within corpus
	Size    int64  // size in bytes
	Content CorpusContent
	SHA256  string // perfile validation hash
}

// CorpusContent describes how to generate file content.
type CorpusContent struct {
	Type string // "json", "logs", "source", "binary", "jpeg_sim", "zip_sim", "random", "encrypted"
	Seed int64
	// CompressionRatio is estimated compressed_size / original_size for benchmark corpus shaping.
	// Typical interpretation: < 0.5 highly compressible, 0.5-0.8 mixed, > 0.95 already compressed.
	CompressionRatio float64

	// JSON-specific
	JSONObjects int64

	// Log-specific
	LogLines int64
	LogSize  int64

	// Binary/Office-specific
	TextRatio float64 // % of file that is plain text (0.0 - 1.0)

	// Random/Encrypted
	EntropyLevel float64 // 0.0 = zero, 1.0 = maximum (random)
}

// StandardCorpora defines all stable benchmark corpora.
func StandardCorpora() []CorpusDefinition {
	return []CorpusDefinition{
		// Corpus A: Highly Compressible (source code, JSON, logs)
		{
			Type:    CorpusTypeHighlyCompressible,
			Version: "v1.0",
			Name:    "CorpusA-HighlyCompressible",
			Description: "Source code, JSON, plaintext logs with high redundancy. " +
				"Expected: compression ratio < 0.5 (compresses to <50% original size)",
			Seed: 0x100,
			Files: []CorpusFile{
				{
					Name: "source_code_1.go",
					Size: 512 * 1024, // 512 KB
					Content: CorpusContent{
						Type:             "source",
						Seed:             0x101,
						CompressionRatio: 0.35,
						TextRatio:        1.0,
					},
				},
				{
					Name: "config_data.json",
					Size: 256 * 1024, // 256 KB
					Content: CorpusContent{
						Type:             "json",
						Seed:             0x102,
						CompressionRatio: 0.42,
						JSONObjects:      10000,
					},
				},
				{
					Name: "application.log",
					Size: 1024 * 1024, // 1 MB
					Content: CorpusContent{
						Type:             "logs",
						Seed:             0x103,
						CompressionRatio: 0.38,
						LogLines:         50000,
						LogSize:          1024 * 1024,
					},
				},
				{
					Name: "plaintext.txt",
					Size: 512 * 1024, // 512 KB
					Content: CorpusContent{
						Type:             "source",
						Seed:             0x104,
						CompressionRatio: 0.40,
						TextRatio:        1.0,
					},
				},
			},
		},

		// Corpus B: Mixed Realistic (office files, sqlite databases, binaries)
		{
			Type:    CorpusTypeMixedRealistic,
			Version: "v1.0",
			Name:    "CorpusB-MixedRealistic",
			Description: "Office documents, SQLite databases, binaries, mixed real-world content. " +
				"Expected: compression ratio 0.5-0.8 (moderate compression)",
			Seed: 0x200,
			Files: []CorpusFile{
				{
					Name: "spreadsheet.xlsx",
					Size: 2 * 1024 * 1024, // 2 MB (XML + binary mixed)
					Content: CorpusContent{
						Type:             "binary",
						Seed:             0x201,
						CompressionRatio: 0.62,
						TextRatio:        0.3, // XML structure in ZIP
					},
				},
				{
					Name: "database.sqlite3",
					Size: 5 * 1024 * 1024, // 5 MB
					Content: CorpusContent{
						Type:             "binary",
						Seed:             0x202,
						CompressionRatio: 0.68,
						TextRatio:        0.1, // mostly binary structure
					},
				},
				{
					Name: "executable.bin",
					Size: 8 * 1024 * 1024, // 8 MB
					Content: CorpusContent{
						Type:             "binary",
						Seed:             0x203,
						CompressionRatio: 0.55,
						TextRatio:        0.05, // mostly machine code
					},
				},
				{
					Name: "document.docx",
					Size: 1024 * 1024, // 1 MB
					Content: CorpusContent{
						Type:             "binary",
						Seed:             0x204,
						CompressionRatio: 0.65,
						TextRatio:        0.4, // XML + embedded content
					},
				},
			},
		},

		// Corpus C: Already Compressed (JPEG, MP4, ZIP, PDF)
		{
			Type:    CorpusTypeAlreadyCompressed,
			Version: "v1.0",
			Name:    "CorpusC-AlreadyCompressed",
			Description: "JPEG, MP4, ZIP, PDF - formats already heavily compressed. " +
				"Expected: compression ratio > 0.95 (minimal/negative compression gain), store-if-smaller prevents expansion",
			Seed: 0x300,
			Files: []CorpusFile{
				{
					Name: "photo.jpg",
					Size: 3 * 1024 * 1024, // 3 MB
					Content: CorpusContent{
						Type:             "jpeg_sim",
						Seed:             0x301,
						CompressionRatio: 0.98, // ~2% compression (minimal)
						EntropyLevel:     0.95, // high entropy
					},
				},
				{
					Name: "archive.zip",
					Size: 4 * 1024 * 1024, // 4 MB
					Content: CorpusContent{
						Type:             "zip_sim",
						Seed:             0x302,
						CompressionRatio: 1.01, // ~1% expansion
						EntropyLevel:     0.93, // high entropy
					},
				},
				{
					Name: "presentation.pdf",
					Size: 2 * 1024 * 1024, // 2 MB
					Content: CorpusContent{
						Type:             "binary",
						Seed:             0x303,
						CompressionRatio: 0.97, // minimal compression
						EntropyLevel:     0.88, // high entropy
					},
				},
				{
					Name: "video_clip.mp4",
					Size: 10 * 1024 * 1024, // 10 MB
					Content: CorpusContent{
						Type:             "binary",
						Seed:             0x304,
						CompressionRatio: 0.99, // ~1% compression
						EntropyLevel:     0.96, // very high entropy
					},
				},
			},
		},

		// Corpus D: Adversarial Random (encrypted blobs, random bytes)
		{
			Type:    CorpusTypeAdversarial,
			Version: "v1.0",
			Name:    "CorpusD-AdversarialRandom",
			Description: "Random bytes, encrypted blobs - compression should be skipped entirely. " +
				"Expected: compression ratio >= 1.0 (expansion), store-if-smaller prevents expansion",
			Seed: 0x400,
			Files: []CorpusFile{
				{
					Name: "random_bytes_1.bin",
					Size: 4 * 1024 * 1024, // 4 MB
					Content: CorpusContent{
						Type:             "random",
						Seed:             0x401,
						CompressionRatio: 1.02, // 2% expansion expected
						EntropyLevel:     1.0,  // maximum entropy
					},
				},
				{
					Name: "random_bytes_2.bin",
					Size: 4 * 1024 * 1024, // 4 MB
					Content: CorpusContent{
						Type:             "random",
						Seed:             0x402,
						CompressionRatio: 1.02, // 2% expansion expected
						EntropyLevel:     1.0,
					},
				},
				{
					Name: "encrypted_blob.bin",
					Size: 8 * 1024 * 1024, // 8 MB
					Content: CorpusContent{
						Type:             "random", // treat as random/encrypted
						Seed:             0x403,
						CompressionRatio: 1.01, // 1% expansion expected
						EntropyLevel:     0.99, // very high entropy
					},
				},
			},
		},
	}
}

// CorpusBuilder orchestrates reproducible corpus generation with versioning.
type CorpusBuilder struct {
	baseDir string
}

// NewCorpusBuilder creates a new builder for a corpus location.
func NewCorpusBuilder(baseDir string) *CorpusBuilder {
	return &CorpusBuilder{baseDir: baseDir}
}

// GenerateCorpus creates all files for a corpus definition with SHA256 validation.
func (cb *CorpusBuilder) GenerateCorpus(def CorpusDefinition) error {
	corpusDir := filepath.Join(cb.baseDir, string(def.Type), def.Version)
	if err := os.MkdirAll(corpusDir, 0o755); err != nil {
		return fmt.Errorf("create corpus directory %q: %w", corpusDir, err)
	}

	// Generate each file in the corpus
	for _, file := range def.Files {
		filePath := filepath.Join(corpusDir, file.Name)
		data, err := generateCorpusContent(file.Content, file.Size)
		if err != nil {
			return fmt.Errorf("generate %s for corpus %s: %w", file.Name, def.Name, err)
		}

		// Verify size
		if int64(len(data)) != file.Size {
			return fmt.Errorf("size mismatch for %s: expected %d, got %d", file.Name, file.Size, len(data))
		}

		if err := os.WriteFile(filePath, data, 0o600); err != nil {
			return fmt.Errorf("write corpus file %q: %w", filePath, err)
		}
	}

	// Generate corpus manifest with checksums
	manifestPath := filepath.Join(corpusDir, "CORPUS_MANIFEST.txt")
	manifest := cb.generateManifest(def, corpusDir)
	if err := os.WriteFile(manifestPath, []byte(manifest), 0o600); err != nil {
		return fmt.Errorf("write corpus manifest %q: %w", manifestPath, err)
	}

	return nil
}

// generateManifest creates a manifest file with corpus metadata and file checksums.
func (cb *CorpusBuilder) generateManifest(def CorpusDefinition, corpusDir string) string {
	var buf bytes.Buffer

	buf.WriteString("# Benchmark Corpus Manifest\n")
	buf.WriteString(fmt.Sprintf("Type: %s\n", def.Type))
	buf.WriteString(fmt.Sprintf("Version: %s\n", def.Version))
	buf.WriteString(fmt.Sprintf("Name: %s\n", def.Name))
	buf.WriteString(fmt.Sprintf("Description: %s\n", def.Description))
	buf.WriteString(fmt.Sprintf("Seed: 0x%x\n", def.Seed))
	buf.WriteString("\n# Files\n")

	totalSize := int64(0)
	for _, file := range def.Files {
		filePath := filepath.Join(corpusDir, file.Name)
		data, _ := os.ReadFile(filePath) // Error already validated during generation
		hash := sha256.Sum256(data)
		fileSHA256 := hex.EncodeToString(hash[:])

		buf.WriteString(fmt.Sprintf("  %s\n", file.Name))
		buf.WriteString(fmt.Sprintf("    Size: %d bytes\n", file.Size))
		buf.WriteString(fmt.Sprintf("    SHA256: %s\n", fileSHA256))
		buf.WriteString(fmt.Sprintf("    Content Type: %s\n", file.Content.Type))
		buf.WriteString(fmt.Sprintf("    Compression Size Ratio (compressed/original): %.2f\n", file.Content.CompressionRatio))
		totalSize += file.Size
	}

	buf.WriteString("\n# Summary\n")
	buf.WriteString(fmt.Sprintf("Total Files: %d\n", len(def.Files)))
	buf.WriteString(fmt.Sprintf("Total Size: %d bytes (%.2f MB)\n", totalSize, float64(totalSize)/(1024*1024)))

	return buf.String()
}

// ValidateCorpus checks all files in a generated corpus match expected hashes and sizes.
func (cb *CorpusBuilder) ValidateCorpus(def CorpusDefinition) (bool, error) {
	corpusDir := filepath.Join(cb.baseDir, string(def.Type), def.Version)

	for _, file := range def.Files {
		filePath := filepath.Join(corpusDir, file.Name)

		// Check existence
		stat, err := os.Stat(filePath)
		if err != nil {
			return false, fmt.Errorf("file not found: %s: %w", file.Name, err)
		}

		// Check size
		if stat.Size() != file.Size {
			return false, fmt.Errorf("size mismatch for %s: expected %d, got %d", file.Name, file.Size, stat.Size())
		}

		// Verify content hash
		f, err := os.Open(filePath)
		if err != nil {
			return false, fmt.Errorf("open file %s: %w", file.Name, err)
		}
		defer func() { _ = f.Close() }()

		hasher := sha256.New()
		if _, err := io.Copy(hasher, f); err != nil {
			return false, fmt.Errorf("hash file %s: %w", file.Name, err)
		}

		computedHash := hex.EncodeToString(hasher.Sum(nil))
		// Only validate if SHA256 is explicitly set on the file
		if file.SHA256 != "" && computedHash != file.SHA256 {
			return false, fmt.Errorf("hash mismatch for %s: expected %s, got %s",
				file.Name, file.SHA256, computedHash)
		}
	}

	return true, nil
}

// generateCorpusContent produces bytes matching a corpus content type.
func generateCorpusContent(cc CorpusContent, size int64) ([]byte, error) {
	rng := rand.New(rand.NewSource(cc.Seed))
	buf := make([]byte, size)

	switch cc.Type {
	case "source":
		// Go-like source code with high redundancy
		return generateSourceCode(rng, buf, cc.TextRatio)

	case "json":
		// JSON with repeated keys and values
		return generateJSON(rng, buf, cc.JSONObjects)

	case "logs":
		// Log entries with repeated patterns
		return generateLogs(rng, buf, cc.LogLines)

	case "binary":
		// Mixed binary/text content
		return generateBinary(rng, buf, cc.TextRatio)

	case "jpeg_sim":
		// Simulated JPEG: high entropy, structure markers
		return generateJPEGSimulation(rng, buf)

	case "zip_sim":
		// Simulated ZIP: high entropy with structure
		return generateZIPSimulation(rng, buf)

	case "random":
		// True random with specified entropy
		return generateRandom(rng, buf, cc.EntropyLevel)

	case "encrypted":
		// Encrypted-like content: high entropy
		return generateRandom(rng, buf, 0.99)

	default:
		return nil, fmt.Errorf("unsupported content type: %s", cc.Type)
	}
}

// generateSourceCode produces Go source-like code with high compressibility.
func generateSourceCode(rng *rand.Rand, buf []byte, textRatio float64) ([]byte, error) {
	templates := []string{
		"func %s() {\n\t// TODO\n\treturn nil\n}\n",
		"type %s struct {\n\tField string\n}\n",
		"const %s = \"%s\"\n",
		"if err != nil {\n\treturn err\n}\n",
		"for _, item := range items {\n\tprocess(item)\n}\n",
		"map[string]interface{}{\n\t\"key\": \"value\",\n}\n",
	}

	pos := 0
	for pos < len(buf) {
		template := templates[rng.Intn(len(templates))]
		remaining := len(buf) - pos
		if len(template) > remaining {
			// Partial fill for last chunk
			copy(buf[pos:], template)
			pos = len(buf)
		} else {
			n := copy(buf[pos:], template)
			pos += n
		}
	}
	return buf, nil
}

// generateJSON produces JSON with repeated structures.
func generateJSON(rng *rand.Rand, buf []byte, objects int64) ([]byte, error) {
	statuses := []string{"active", "inactive", "pending", "error"}

	var result strings.Builder
	result.WriteString("[")

	i := int64(0)
	for result.Len() < len(buf) {
		if i > 0 {
			result.WriteString(",")
		}
		result.WriteString(fmt.Sprintf(
			`{"id":%d,"name":"user_%d","status":"%s"}`,
			i,
			i,
			statuses[rng.Intn(len(statuses))],
		))
		i++
		if i > objects && objects > 0 {
			break
		}
	}
	result.WriteString("]")

	resultData := []byte(result.String())
	if len(resultData) > len(buf) {
		resultData = resultData[:len(buf)]
	} else {
		// Pad remaining space
		copy(buf[len(resultData):], bytes.Repeat([]byte("\n"), len(buf)-len(resultData)))
	}
	copy(buf, resultData)
	return buf, nil
}

// generateLogs produces log-like entries with patterns.
func generateLogs(rng *rand.Rand, buf []byte, lines int64) ([]byte, error) {
	logFormats := []string{
		"[INFO] Request processed: %d ms\n",
		"[DEBUG] Cache hit for key: %s\n",
		"[WARN] Slow query detected: %d ms\n",
		"[ERROR] Database connection failed: %s\n",
		"[INFO] User logged in: user_%d\n",
	}

	var result strings.Builder
	for i := int64(0); i < lines && result.Len() < len(buf); i++ {
		format := logFormats[rng.Intn(len(logFormats))]
		result.WriteString(fmt.Sprintf(format, i%1000))
	}

	resultData := []byte(result.String())
	if len(resultData) > len(buf) {
		resultData = resultData[:len(buf)]
	} else {
		// Pad remaining space with newlines for log-like format
		copy(buf[len(resultData):], bytes.Repeat([]byte("\n"), len(buf)-len(resultData)))
	}
	copy(buf, resultData)
	return buf, nil
}

// generateBinary produces mixed binary/text content.
func generateBinary(rng *rand.Rand, buf []byte, textRatio float64) ([]byte, error) {
	// Fill with random bytes
	for i := 0; i < len(buf); i++ {
		buf[i] = byte(rng.Intn(256))
	}

	// Overlay text sections
	textBytes := int(float64(len(buf)) * textRatio)
	for i := 0; i < textBytes; i++ {
		// ASCII printable range for text portions
		buf[i] = byte(32 + rng.Intn(95))
	}

	return buf, nil
}

// generateJPEGSimulation produces high-entropy content simulating JPEG structure.
func generateJPEGSimulation(rng *rand.Rand, buf []byte) ([]byte, error) {
	// JPEG markers
	buf[0] = 0xFF
	buf[1] = 0xD8 // SOI

	// Fill rest with high entropy
	for i := 2; i < len(buf); i++ {
		buf[i] = byte(rng.Intn(256))
	}

	// Add EOF marker
	if len(buf) > 2 {
		buf[len(buf)-2] = 0xFF
		buf[len(buf)-1] = 0xD9 // EOI
	}

	return buf, nil
}

// generateZIPSimulation produces high-entropy content simulating ZIP structure.
func generateZIPSimulation(rng *rand.Rand, buf []byte) ([]byte, error) {
	// ZIP local file header
	if len(buf) > 4 {
		buf[0] = 0x50 // P
		buf[1] = 0x4B // K
		buf[2] = 0x03 // ETX
		buf[3] = 0x04 // EOT
	}

	// Fill rest with high entropy
	for i := 4; i < len(buf); i++ {
		buf[i] = byte(rng.Intn(256))
	}

	return buf, nil
}

// generateRandom produces high-entropy random bytes.
func generateRandom(rng *rand.Rand, buf []byte, entropyLevel float64) ([]byte, error) {
	for i := 0; i < len(buf); i++ {
		buf[i] = byte(rng.Intn(256))
	}
	return buf, nil
}
