// Command compress_v1_18_fixture_generator creates a benign persisted
// repository using the dependency version in the checked-out module.
//
// The accepted persisted fixtures and original exact-H generation evidence are
// immutable. This maintained source includes later refactors; it is not the
// byte-identical generator used for that historical run. Any new use requires
// its own source-bound provenance review. The payloads are deterministic test data.
package main

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strings"

	"github.com/franchoy/coldkeep/internal/blocks"
	"github.com/franchoy/coldkeep/internal/container"
	"github.com/franchoy/coldkeep/internal/db"
	"github.com/franchoy/coldkeep/internal/storage"
	_ "github.com/mattn/go-sqlite3"
)

const (
	fixtureSchema = "coldkeep.compress-old-writer-fixture.v2"
	oldVersion    = "v1.18.0"
	testKeyHex    = "abababababababababababababababababababababababababababababababab"
)

type fixtureFile struct {
	CaseID        string             `json:"case_id"`
	PayloadKind   string             `json:"payload_kind"`
	FileID        int64              `json:"file_id"`
	FileHash      string             `json:"file_hash"`
	PayloadSHA256 string             `json:"payload_sha256"`
	PayloadSize   int                `json:"payload_size"`
	PackedBlocks  int                `json:"packed_blocks"`
	Transforms    []fixtureTransform `json:"transforms"`
}

type fixtureTransform struct {
	StorageCodec     string `json:"storage_codec"`
	CompressionCodec string `json:"compression_codec"`
	CompressionLevel *int   `json:"compression_level"`
	BlockCount       int    `json:"block_count"`
}

type fixtureManifest struct {
	Schema            string        `json:"schema"`
	WriterCommit      string        `json:"writer_commit"`
	GoVersion         string        `json:"go_version"`
	CompressVersion   string        `json:"compress_version"`
	CompressModuleSum string        `json:"compress_module_sum"`
	AESKeyHex         string        `json:"aes_key_hex"`
	Catalog           string        `json:"catalog"`
	Containers        string        `json:"containers"`
	Files             []fixtureFile `json:"files"`
}

type fixtureCase struct {
	id          string
	payloadKind string
	payload     []byte
	codec       blocks.Codec
	compression string
	level       int
}

func main() {
	outDir := requiredOutputDirectory()
	version, sum := compressBuildIdentity()
	if version != oldVersion {
		fatalf("generator requires github.com/klauspost/compress %s, got %s", oldVersion, version)
	}
	output := createFixtureRoot(outDir)
	defer func() { _ = output.Close() }()
	inputDir, err := os.MkdirTemp("", "coldkeep-compress-v1-18-fixture-inputs-")
	if err != nil {
		fatalf("create temporary inputs: %v", err)
	}
	defer func() { _ = os.RemoveAll(inputDir) }()
	ctx := openFixtureRepository(output)
	manifest := newFixtureManifest(version, sum)
	for _, tc := range fixtureCases() {
		manifest.Files = append(manifest.Files, storeFixtureCase(ctx, inputDir, tc))
	}
	if err := ctx.Close(); err != nil {
		fatalf("close fixture repository: %v", err)
	}
	writeFixtureManifest(output, manifest)
}

func requiredOutputDirectory() string {
	if len(os.Args) != 2 {
		fatalf("usage: %s OUTPUT_DIRECTORY (parent must exist)", os.Args[0])
	}
	outDir, err := filepath.Abs(os.Args[1])
	if err != nil {
		fatalf("resolve output directory: %v", err)
	}
	if _, err := os.Lstat(outDir); !os.IsNotExist(err) {
		fatalf("output path must not exist: %s", outDir)
	}
	return outDir
}

func createFixtureRoot(outDir string) *os.Root {
	// Anchor creation in an existing parent; reserve the requested directory
	// atomically and give only its owner traversal and read/write permission.
	parent, err := os.OpenRoot(filepath.Dir(outDir))
	if err != nil {
		fatalf("open output parent: %v", err)
	}
	defer func() { _ = parent.Close() }()
	name := filepath.Base(outDir)
	if err := parent.Mkdir(name, 0o700); err != nil {
		fatalf("create fixture root: %v", err)
	}
	output, err := parent.OpenRoot(name)
	if err != nil {
		fatalf("open fixture root: %v", err)
	}
	if err := output.Mkdir("containers", 0o700); err != nil {
		fatalf("create fixture containers: %v", err)
	}
	return output
}

func openFixtureRepository(output *os.Root) storage.StorageContext {
	catalogPath := filepath.Join(output.Name(), "catalog.db")
	dbconn, err := sql.Open("sqlite3", catalogPath)
	if err != nil {
		fatalf("open fixture catalog: %v", err)
	}
	if err := db.RunMigrations(dbconn); err != nil {
		fatalf("migrate fixture catalog: %v", err)
	}
	containersDir := filepath.Join(output.Name(), "containers")
	ctx := storage.StorageContext{
		DB:           dbconn,
		Writer:       container.NewLocalWriterWithDirAndDB(containersDir, container.GetContainerMaxSize(), dbconn),
		ContainerDir: containersDir,
	}
	if err := os.Setenv("COLDKEEP_KEY", testKeyHex); err != nil {
		fatalf("set fixture encryption key: %v", err)
	}
	return ctx
}

func fixtureCases() []fixtureCase {
	return []fixtureCase{
		{id: "empty-none-plain", payloadKind: "empty", payload: []byte{}, codec: blocks.CodecPlain, compression: "none"},
		{id: "tiny-zstd-plain-l1", payloadKind: "literal", payload: []byte("tiny"), codec: blocks.CodecPlain, compression: "zstd", level: 1},
		{id: "small-none-aes", payloadKind: "deterministic-noise", payload: deterministicNoise(4 << 10), codec: blocks.CodecAESGCM, compression: "none"},
		{id: "compressible-zstd-plain-l1", payloadKind: "repeated-text", payload: repeatedPayload("compressible-zstd-plain-l1", 256<<10), codec: blocks.CodecPlain, compression: "zstd", level: 1},
		{id: "incompressible-zstd-aes-l3", payloadKind: "deterministic-noise", payload: deterministicNoise(256 << 10), codec: blocks.CodecAESGCM, compression: "zstd", level: 3},
		{id: "compressible-zstd-aes-l9", payloadKind: "repeated-text", payload: repeatedPayload("compressible-zstd-aes-l9", 256<<10), codec: blocks.CodecAESGCM, compression: "zstd", level: 9},
		{id: "multi-packed-zstd-plain-l3", payloadKind: "structured-compressible", payload: structuredCompressiblePayload(3 << 20), codec: blocks.CodecPlain, compression: "zstd", level: 3},
	}

}

// WriterCommit below names the expected historical writer for this template;
// it is not a runtime attestation. A future generation must independently prove
// that source binding or revise the manifest claim before its output is accepted.
func newFixtureManifest(version, sum string) fixtureManifest {
	return fixtureManifest{
		Schema:            fixtureSchema,
		WriterCommit:      "550374d40d102bb5cbb5184799ef9b03fdaeefac",
		GoVersion:         runtime.Version(),
		CompressVersion:   version,
		CompressModuleSum: sum,
		AESKeyHex:         testKeyHex,
		Catalog:           "catalog.db",
		Containers:        "containers",
	}

}

func storeFixtureCase(ctx storage.StorageContext, inputDir string, tc fixtureCase) fixtureFile {
	if err := setCompression(ctx.DB, tc.compression, tc.level); err != nil {
		fatalf("configure %s: %v", tc.id, err)
	}
	inputPath := filepath.Join(inputDir, tc.id+".bin")
	if err := os.WriteFile(inputPath, tc.payload, 0o600); err != nil {
		fatalf("write %s input: %v", tc.id, err)
	}
	result, err := storage.StoreFileWithStorageContextAndCodecResult(ctx, inputPath, tc.codec)
	if err != nil {
		fatalf("store %s: %v", tc.id, err)
	}
	transforms, packedBlocks, err := storedTransforms(ctx.DB, result.FileID)
	if err != nil {
		fatalf("inspect %s transforms: %v", tc.id, err)
	}
	sum := sha256.Sum256(tc.payload)
	return fixtureFile{
		CaseID:        tc.id,
		PayloadKind:   tc.payloadKind,
		FileID:        result.FileID,
		FileHash:      result.FileHash,
		PayloadSHA256: hex.EncodeToString(sum[:]),
		PayloadSize:   len(tc.payload),
		PackedBlocks:  packedBlocks,
		Transforms:    transforms,
	}
}

func writeFixtureManifest(output *os.Root, manifest fixtureManifest) {
	manifestBytes, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		fatalf("encode manifest: %v", err)
	}
	manifestBytes = append(manifestBytes, '\n')
	if err := output.WriteFile("manifest.json", manifestBytes, 0o600); err != nil {
		fatalf("write manifest: %v", err)
	}
}

func compressBuildIdentity() (string, string) {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		fatalf("read Go build identity")
	}
	for _, dep := range info.Deps {
		if dep.Path == "github.com/klauspost/compress" {
			return dep.Version, dep.Sum
		}
	}
	fatalf("github.com/klauspost/compress absent from build identity")
	return "", ""
}

func repeatedPayload(name string, target int) []byte {
	line := []byte("coldkeep benign old-writer compatibility fixture: " + name + "\n")
	out := make([]byte, target)
	for offset := 0; offset < len(out); {
		offset += copy(out[offset:], line)
	}
	return out
}

func deterministicNoise(target int) []byte {
	out := make([]byte, target)
	var state uint32 = 0x9e3779b9
	for index := range out {
		state ^= state << 13
		state ^= state >> 17
		state ^= state << 5
		out[index] = byte(state)
	}
	return out
}

func structuredCompressiblePayload(target int) []byte {
	out := make([]byte, target)
	noise := deterministicNoise((target + 15) / 16)
	for index := range out {
		if index%16 == 0 {
			out[index] = noise[index/16]
		}
	}
	return out
}

func setCompression(dbconn *sql.DB, codec string, level int) error {
	tx, err := dbconn.Begin()
	if err != nil {
		return err
	}
	if err := storage.SetDefaultCompression(tx, codec); err != nil {
		_ = tx.Rollback()
		return err
	}
	if codec == "zstd" {
		if err := storage.SetDefaultCompressionLevel(tx, level); err != nil {
			_ = tx.Rollback()
			return err
		}
	}
	return tx.Commit()
}

func storedTransforms(dbconn *sql.DB, fileID int64) ([]fixtureTransform, int, error) {
	rows, err := dbconn.Query(`
		SELECT b.codec, b.compression_codec, b.compression_level, COUNT(DISTINCT b.id)
		FROM storage_blocks b
		JOIN chunk_block_refs r ON r.block_id = b.id
		JOIN file_chunk fc ON fc.chunk_id = r.chunk_id
		WHERE fc.logical_file_id = $1
		GROUP BY b.codec, b.compression_codec, b.compression_level
		ORDER BY b.codec, b.compression_codec, b.compression_level
	`, fileID)
	if err != nil {
		return nil, 0, err
	}
	defer func() { _ = rows.Close() }()
	var transforms []fixtureTransform
	packedBlocks := 0
	for rows.Next() {
		var transform fixtureTransform
		var level sql.NullInt64
		if err := rows.Scan(&transform.StorageCodec, &transform.CompressionCodec, &level, &transform.BlockCount); err != nil {
			return nil, 0, err
		}
		if level.Valid {
			value := int(level.Int64)
			transform.CompressionLevel = &value
		}
		transforms = append(transforms, transform)
		packedBlocks += transform.BlockCount
	}
	if err := rows.Err(); err != nil {
		return nil, 0, err
	}
	return transforms, packedBlocks, nil
}

func fatalf(format string, args ...any) {
	_, _ = fmt.Fprintf(os.Stderr, strings.TrimSuffix(format, "\n")+"\n", args...)
	os.Exit(1)
}
