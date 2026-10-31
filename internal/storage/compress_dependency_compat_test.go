package storage

import (
	"bytes"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/franchoy/coldkeep/internal/blocks"
	"github.com/franchoy/coldkeep/internal/container"
	"github.com/franchoy/coldkeep/internal/db"
	verifypkg "github.com/franchoy/coldkeep/internal/verify"
	_ "github.com/mattn/go-sqlite3"
)

type oldCompressFixtureFile struct {
	Name             string `json:"name"`
	FileID           int64  `json:"file_id"`
	FileHash         string `json:"file_hash"`
	PayloadSHA256    string `json:"payload_sha256"`
	PayloadSize      int    `json:"payload_size"`
	StorageCodec     string `json:"storage_codec"`
	CompressionCodec string `json:"compression_codec"`
	CompressionLevel int    `json:"compression_level"`
}

type oldCompressFixtureManifest struct {
	Schema            string                   `json:"schema"`
	WriterCommit      string                   `json:"writer_commit"`
	GoVersion         string                   `json:"go_version"`
	CompressVersion   string                   `json:"compress_version"`
	CompressModuleSum string                   `json:"compress_module_sum"`
	AESKeyHex         string                   `json:"aes_key_hex"`
	Catalog           string                   `json:"catalog"`
	Containers        string                   `json:"containers"`
	Files             []oldCompressFixtureFile `json:"files"`
}

// TestCompressV118PersistedRepositoryRestoresWithCurrentReader checks ordinary
// forward compatibility using a repository written by exact H with compress
// v1.18.0. The fixture contains only deterministic benign payloads. It covers
// packed zstd blocks at levels 1, 3, and 9 in both plaintext and AES-GCM modes.
func TestCompressV118PersistedRepositoryRestoresWithCurrentReader(t *testing.T) {
	const (
		fixtureSchema      = "coldkeep.compress-old-writer-fixture.v1"
		writerCommit       = "550374d40d102bb5cbb5184799ef9b03fdaeefac"
		oldCompressVersion = "v1.18.0"
		oldCompressSum     = "h1:c/Cqfb0r+Yi+JtIEq73FWXVkRonBlf0CRNYc8Zttxdo="
	)

	fixtureDir := filepath.Join("testdata", "compress_v1_18_persisted")
	manifestBytes, err := os.ReadFile(filepath.Join(fixtureDir, "manifest.json"))
	if err != nil {
		t.Fatalf("read old-writer manifest: %v", err)
	}
	var manifest oldCompressFixtureManifest
	if err := json.Unmarshal(manifestBytes, &manifest); err != nil {
		t.Fatalf("parse old-writer manifest: %v", err)
	}
	if manifest.Schema != fixtureSchema || manifest.WriterCommit != writerCommit {
		t.Fatalf("unexpected old-writer identity: schema=%q commit=%q", manifest.Schema, manifest.WriterCommit)
	}
	if manifest.CompressVersion != oldCompressVersion || manifest.CompressModuleSum != oldCompressSum {
		t.Fatalf("unexpected old dependency identity: version=%q sum=%q", manifest.CompressVersion, manifest.CompressModuleSum)
	}
	if len(manifest.Files) != 6 {
		t.Fatalf("old-writer fixture file count: got %d want 6", len(manifest.Files))
	}

	workingDir := t.TempDir()
	catalogPath := filepath.Join(workingDir, "catalog.db")
	copyTestFile(t, filepath.Join(fixtureDir, manifest.Catalog), catalogPath)
	dbconn, err := sql.Open("sqlite3", catalogPath)
	if err != nil {
		t.Fatalf("open copied old-writer catalog: %v", err)
	}
	t.Cleanup(func() { _ = dbconn.Close() })
	if err := db.RunMigrations(dbconn); err != nil {
		t.Fatalf("open old-writer catalog with current migrations: %v", err)
	}

	t.Setenv("COLDKEEP_KEY", manifest.AESKeyHex)
	containersDir := filepath.Join(fixtureDir, manifest.Containers)
	for _, fixtureFile := range manifest.Files {
		fixtureFile := fixtureFile
		t.Run(fixtureFile.Name, func(t *testing.T) {
			outputPath := filepath.Join(workingDir, fixtureFile.Name+".restored")
			if _, err := restoreFileWithDBAndDir(dbconn, fixtureFile.FileID, outputPath, containersDir, RestoreOptions{}); err != nil {
				t.Fatalf("restore old-writer file %d: %v", fixtureFile.FileID, err)
			}
			restored, err := os.ReadFile(outputPath)
			if err != nil {
				t.Fatalf("read restored output: %v", err)
			}
			if len(restored) != fixtureFile.PayloadSize {
				t.Fatalf("restored size: got %d want %d", len(restored), fixtureFile.PayloadSize)
			}
			sum := sha256.Sum256(restored)
			gotSHA := hex.EncodeToString(sum[:])
			if gotSHA != fixtureFile.PayloadSHA256 || gotSHA != fixtureFile.FileHash {
				t.Fatalf("restored identity: got %s payload=%s file=%s", gotSHA, fixtureFile.PayloadSHA256, fixtureFile.FileHash)
			}
			assertOldWriterTransforms(t, dbconn, fixtureFile)
			if err := verifypkg.VerifyFileStandardWithContainersDir(dbconn, int(fixtureFile.FileID), containersDir); err != nil {
				t.Fatalf("verify old-writer file: %v", err)
			}
		})
	}
	if err := verifypkg.VerifySystemStandardWithContainersDir(dbconn, containersDir); err != nil {
		t.Fatalf("verify old-writer repository: %v", err)
	}
}

func assertOldWriterTransforms(t *testing.T, dbconn *sql.DB, fixtureFile oldCompressFixtureFile) {
	t.Helper()
	rows, err := dbconn.Query(`
		SELECT DISTINCT b.codec, b.compression_codec, b.compression_level
		FROM storage_blocks b
		JOIN chunk_block_refs r ON r.block_id = b.id
		JOIN file_chunk fc ON fc.chunk_id = r.chunk_id
		WHERE fc.logical_file_id = $1
	`, fixtureFile.FileID)
	if err != nil {
		t.Fatalf("query old-writer transforms: %v", err)
	}
	defer func() { _ = rows.Close() }()

	count := 0
	for rows.Next() {
		var storageCodec, compressionCodec string
		var compressionLevel int
		if err := rows.Scan(&storageCodec, &compressionCodec, &compressionLevel); err != nil {
			t.Fatalf("scan old-writer transforms: %v", err)
		}
		count++
		if storageCodec != fixtureFile.StorageCodec || compressionCodec != fixtureFile.CompressionCodec || compressionLevel != fixtureFile.CompressionLevel {
			t.Fatalf(
				"old-writer transforms: got storage=%q compression=%q level=%d want storage=%q compression=%q level=%d",
				storageCodec,
				compressionCodec,
				compressionLevel,
				fixtureFile.StorageCodec,
				fixtureFile.CompressionCodec,
				fixtureFile.CompressionLevel,
			)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate old-writer transforms: %v", err)
	}
	if count != 1 {
		t.Fatalf("old-writer transform combinations: got %d want 1", count)
	}
}

func copyTestFile(t *testing.T, source, destination string) {
	t.Helper()
	payload, err := os.ReadFile(source)
	if err != nil {
		t.Fatalf("read fixture file %s: %v", source, err)
	}
	if err := os.WriteFile(destination, payload, 0o600); err != nil {
		t.Fatalf("copy fixture file %s: %v", source, err)
	}
}

type supplementalCompressTransform struct {
	StorageCodec     string `json:"storage_codec"`
	CompressionCodec string `json:"compression_codec"`
	CompressionLevel *int   `json:"compression_level"`
	BlockCount       int    `json:"block_count"`
}

type supplementalCompressFile struct {
	CaseID        string                          `json:"case_id"`
	PayloadKind   string                          `json:"payload_kind"`
	FileID        int64                           `json:"file_id"`
	FileHash      string                          `json:"file_hash"`
	PayloadSHA256 string                          `json:"payload_sha256"`
	PayloadSize   int                             `json:"payload_size"`
	PackedBlocks  int                             `json:"packed_blocks"`
	Transforms    []supplementalCompressTransform `json:"transforms"`
}

type supplementalCompressManifest struct {
	Schema            string                     `json:"schema"`
	WriterCommit      string                     `json:"writer_commit"`
	GoVersion         string                     `json:"go_version"`
	CompressVersion   string                     `json:"compress_version"`
	CompressModuleSum string                     `json:"compress_module_sum"`
	AESKeyHex         string                     `json:"aes_key_hex"`
	Catalog           string                     `json:"catalog"`
	Containers        string                     `json:"containers"`
	Files             []supplementalCompressFile `json:"files"`
}

// TestCompressV118SupplementalRepositoryWithCurrentReader covers the ordinary
// old-writer/new-reader matrix omitted by the original six-case fixture. The
// fixture was written and closed by exact H before this candidate opens it.
func TestCompressV118SupplementalRepositoryWithCurrentReader(t *testing.T) {
	const (
		fixtureSchema      = "coldkeep.compress-old-writer-fixture.v2"
		writerCommit       = "550374d40d102bb5cbb5184799ef9b03fdaeefac"
		oldCompressVersion = "v1.18.0"
		oldCompressSum     = "h1:c/Cqfb0r+Yi+JtIEq73FWXVkRonBlf0CRNYc8Zttxdo="
	)

	fixtureDir := filepath.Join("testdata", "compress_v1_18_supplemental")
	manifestBytes, err := os.ReadFile(filepath.Join(fixtureDir, "manifest.json"))
	if err != nil {
		t.Fatalf("read supplemental manifest: %v", err)
	}
	var manifest supplementalCompressManifest
	if err := json.Unmarshal(manifestBytes, &manifest); err != nil {
		t.Fatalf("parse supplemental manifest: %v", err)
	}
	if manifest.Schema != fixtureSchema || manifest.WriterCommit != writerCommit ||
		manifest.CompressVersion != oldCompressVersion || manifest.CompressModuleSum != oldCompressSum {
		t.Fatalf("unexpected supplemental writer identity: schema=%q commit=%q compress=%q sum=%q", manifest.Schema, manifest.WriterCommit, manifest.CompressVersion, manifest.CompressModuleSum)
	}
	assertSupplementalManifestMatrix(t, manifest.Files)

	workingRepo := filepath.Join(t.TempDir(), "old-writer-repository")
	if err := os.MkdirAll(workingRepo, 0o700); err != nil {
		t.Fatalf("create working repository: %v", err)
	}
	catalogPath := filepath.Join(workingRepo, manifest.Catalog)
	copyTestFile(t, filepath.Join(fixtureDir, manifest.Catalog), catalogPath)
	containersDir := filepath.Join(workingRepo, manifest.Containers)
	copyTestDirectory(t, filepath.Join(fixtureDir, manifest.Containers), containersDir)

	dbconn, err := sql.Open("sqlite3", catalogPath)
	if err != nil {
		t.Fatalf("open supplemental catalog: %v", err)
	}
	t.Cleanup(func() { _ = dbconn.Close() })
	if err := db.RunMigrations(dbconn); err != nil {
		t.Fatalf("open supplemental catalog with current migrations: %v", err)
	}
	t.Setenv("COLDKEEP_KEY", manifest.AESKeyHex)

	filesByID := make(map[string]supplementalCompressFile, len(manifest.Files))
	for _, fixtureFile := range manifest.Files {
		fixtureFile := fixtureFile
		filesByID[fixtureFile.CaseID] = fixtureFile
		t.Run("restore-"+fixtureFile.CaseID, func(t *testing.T) {
			outputPath := filepath.Join(t.TempDir(), fixtureFile.CaseID+".restored")
			if _, err := restoreFileWithDBAndDir(dbconn, fixtureFile.FileID, outputPath, containersDir, RestoreOptions{}); err != nil {
				t.Fatalf("restore supplemental file %d: %v", fixtureFile.FileID, err)
			}
			restored, err := os.ReadFile(outputPath)
			if err != nil {
				t.Fatalf("read restored output: %v", err)
			}
			sum := sha256.Sum256(restored)
			gotSHA := hex.EncodeToString(sum[:])
			if len(restored) != fixtureFile.PayloadSize || gotSHA != fixtureFile.PayloadSHA256 || gotSHA != fixtureFile.FileHash {
				t.Fatalf("restored identity: size=%d/%d sha=%s payload=%s file=%s", len(restored), fixtureFile.PayloadSize, gotSHA, fixtureFile.PayloadSHA256, fixtureFile.FileHash)
			}
			actualTransforms, actualBlockCount := readSupplementalTransforms(t, dbconn, fixtureFile.FileID)
			if !reflect.DeepEqual(actualTransforms, fixtureFile.Transforms) || actualBlockCount != fixtureFile.PackedBlocks {
				t.Fatalf("persisted transforms: got=%+v blocks=%d want=%+v blocks=%d", actualTransforms, actualBlockCount, fixtureFile.Transforms, fixtureFile.PackedBlocks)
			}
			if err := verifypkg.VerifyFileStandardWithContainersDir(dbconn, int(fixtureFile.FileID), containersDir); err != nil {
				t.Fatalf("verify supplemental old-writer file: %v", err)
			}
		})
	}
	if err := verifypkg.VerifySystemStandardWithContainersDir(dbconn, containersDir); err != nil {
		t.Fatalf("verify supplemental old-writer repository: %v", err)
	}

	repeated := filesByID["compressible-zstd-plain-l1"]
	repeatedPayload := compatibilityRepeatedPayload(repeated.CaseID, repeated.PayloadSize)
	if got := sha256.Sum256(repeatedPayload); hex.EncodeToString(got[:]) != repeated.PayloadSHA256 {
		t.Fatalf("recreated repeated payload identity mismatch")
	}
	var logicalRefBefore, chunkRowsBefore, blockRowsBefore, refsRowsBefore int
	if err := dbconn.QueryRow(`SELECT ref_count FROM logical_file WHERE id = $1`, repeated.FileID).Scan(&logicalRefBefore); err != nil {
		t.Fatalf("read repeated logical refcount: %v", err)
	}
	countCompatibilityRows(t, dbconn, &chunkRowsBefore, &blockRowsBefore, &refsRowsBefore)
	context := StorageContext{
		DB:           dbconn,
		Writer:       container.NewLocalWriterWithDirAndDB(containersDir, container.GetContainerMaxSize(), dbconn),
		ContainerDir: containersDir,
	}
	t.Cleanup(func() { _ = context.Close() })
	repeatPath := filepath.Join(t.TempDir(), "candidate-repeat.bin")
	if err := os.WriteFile(repeatPath, repeatedPayload, 0o600); err != nil {
		t.Fatalf("write repeated candidate input: %v", err)
	}
	repeatResult, err := StoreFileWithStorageContextAndCodecResult(context, repeatPath, blocks.CodecPlain)
	if err != nil {
		t.Fatalf("candidate repeat against old data: %v", err)
	}
	if !repeatResult.AlreadyStored || repeatResult.FileID != repeated.FileID || repeatResult.FileHash != repeated.FileHash {
		t.Fatalf("candidate repeat identity: %+v want file_id=%d hash=%s already_stored", repeatResult, repeated.FileID, repeated.FileHash)
	}
	var logicalRefAfter, chunkRowsAfter, blockRowsAfter, refsRowsAfter int
	if err := dbconn.QueryRow(`SELECT ref_count FROM logical_file WHERE id = $1`, repeated.FileID).Scan(&logicalRefAfter); err != nil {
		t.Fatalf("read repeated logical refcount after reuse: %v", err)
	}
	countCompatibilityRows(t, dbconn, &chunkRowsAfter, &blockRowsAfter, &refsRowsAfter)
	if logicalRefAfter != logicalRefBefore+1 || chunkRowsAfter != chunkRowsBefore || blockRowsAfter != blockRowsBefore || refsRowsAfter != refsRowsBefore {
		t.Fatalf("candidate reuse accounting: logical_refs=%d/%d chunks=%d/%d blocks=%d/%d refs=%d/%d", logicalRefBefore, logicalRefAfter, chunkRowsBefore, chunkRowsAfter, blockRowsBefore, blockRowsAfter, refsRowsBefore, refsRowsAfter)
	}

	setRepositoryCompressionForTest(t, dbconn, "zstd", 3)
	newPayload := compatibilityRepeatedPayload("candidate-v1.20.2-new-write", 384<<10)
	newPath := filepath.Join(t.TempDir(), "candidate-new.bin")
	if err := os.WriteFile(newPath, newPayload, 0o600); err != nil {
		t.Fatalf("write candidate new input: %v", err)
	}
	newResult, err := StoreFileWithStorageContextAndCodecResult(context, newPath, blocks.CodecPlain)
	if err != nil {
		t.Fatalf("candidate new write into old repository: %v", err)
	}
	if newResult.AlreadyStored {
		t.Fatal("candidate new input unexpectedly reported already stored")
	}
	newOutput := filepath.Join(t.TempDir(), "candidate-new.restored")
	if _, err := restoreFileWithDBAndDir(dbconn, newResult.FileID, newOutput, containersDir, RestoreOptions{}); err != nil {
		t.Fatalf("restore candidate new write: %v", err)
	}
	gotNew, err := os.ReadFile(newOutput)
	if err != nil || !bytes.Equal(gotNew, newPayload) {
		t.Fatalf("candidate new-write roundtrip: read_err=%v equal=%t", err, bytes.Equal(gotNew, newPayload))
	}
	if err := verifypkg.VerifyFileStandardWithContainersDir(dbconn, int(repeated.FileID), containersDir); err != nil {
		t.Fatalf("verify reused old file after candidate writes: %v", err)
	}
	if err := verifypkg.VerifyFileStandardWithContainersDir(dbconn, int(newResult.FileID), containersDir); err != nil {
		t.Fatalf("verify candidate new file: %v", err)
	}
	if err := verifypkg.VerifySystemStandardWithContainersDir(dbconn, containersDir); err != nil {
		t.Fatalf("verify mixed old/new repository: %v", err)
	}
}

func TestCompressUpgradeGenuineLegacyOnlyReadPath(t *testing.T) {
	dbconn := setupStep8DB(t)
	defer func() { _ = dbconn.Close() }()
	containersDir := t.TempDir()
	payloadA := []byte("compress-upgrade-legacy-only-a-")
	payloadB := []byte("compress-upgrade-legacy-only-b")
	chunks := []restoreChunkSeed{
		insertChunkForRestore(t, dbconn, payloadA, "v1-simple-rolling"),
		insertChunkForRestore(t, dbconn, payloadB, "v1-simple-rolling"),
	}
	containerA := insertContainerWithPayload(t, dbconn, containersDir, "legacy-only-a.bin", payloadA)
	containerB := insertContainerWithPayload(t, dbconn, containersDir, "legacy-only-b.bin", payloadB)
	insertLegacyBlocksRows(t, dbconn, containerA, chunks[:1])
	insertLegacyBlocksRows(t, dbconn, containerB, chunks[1:])
	fileID := insertLogicalFileForRestore(t, dbconn, "compress-upgrade-legacy-only.bin", chunks, "v1-simple-rolling")
	var packedRows, packedRefs int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM storage_blocks`).Scan(&packedRows); err != nil {
		t.Fatalf("count legacy-only storage blocks: %v", err)
	}
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM chunk_block_refs`).Scan(&packedRefs); err != nil {
		t.Fatalf("count legacy-only packed refs: %v", err)
	}
	if packedRows != 0 || packedRefs != 0 {
		t.Fatalf("fixture is not legacy-only: storage_blocks=%d chunk_block_refs=%d", packedRows, packedRefs)
	}
	got := restoreAndReadBytes(t, dbconn, fileID, containersDir)
	want := append(append([]byte{}, payloadA...), payloadB...)
	if !bytes.Equal(got, want) {
		t.Fatalf("legacy-only restore mismatch: got=%q want=%q", got, want)
	}
	if err := verifypkg.VerifyFileStandardWithContainersDir(dbconn, int(fileID), containersDir); err != nil {
		t.Fatalf("verify genuine legacy-only file: %v", err)
	}
}

func assertSupplementalManifestMatrix(t *testing.T, files []supplementalCompressFile) {
	t.Helper()
	level1, level3, level9 := 1, 3, 9
	want := map[string]struct {
		size       int
		kind       string
		blocks     int
		transforms []supplementalCompressTransform
	}{
		"empty-none-plain":           {0, "empty", 0, nil},
		"tiny-zstd-plain-l1":         {4, "literal", 1, []supplementalCompressTransform{{"none", "none", nil, 1}}},
		"small-none-aes":             {4 << 10, "deterministic-noise", 1, []supplementalCompressTransform{{"aes-gcm", "none", nil, 1}}},
		"compressible-zstd-plain-l1": {256 << 10, "repeated-text", 1, []supplementalCompressTransform{{"none", "zstd", &level1, 1}}},
		"incompressible-zstd-aes-l3": {256 << 10, "deterministic-noise", 1, []supplementalCompressTransform{{"aes-gcm", "none", nil, 1}}},
		"compressible-zstd-aes-l9":   {256 << 10, "repeated-text", 1, []supplementalCompressTransform{{"aes-gcm", "zstd", &level9, 1}}},
		"multi-packed-zstd-plain-l3": {3 << 20, "structured-compressible", 3, []supplementalCompressTransform{{"none", "zstd", &level3, 3}}},
	}
	if len(files) != len(want) {
		t.Fatalf("supplemental case count: got=%d want=%d", len(files), len(want))
	}
	seen := make(map[string]bool, len(files))
	for _, file := range files {
		expected, ok := want[file.CaseID]
		if !ok || seen[file.CaseID] {
			t.Fatalf("unexpected or duplicate supplemental case %q", file.CaseID)
		}
		seen[file.CaseID] = true
		if file.PayloadSize != expected.size || file.PayloadKind != expected.kind || file.PackedBlocks != expected.blocks || !reflect.DeepEqual(file.Transforms, expected.transforms) {
			t.Fatalf("supplemental case %q: got size=%d kind=%q blocks=%d transforms=%+v want size=%d kind=%q blocks=%d transforms=%+v", file.CaseID, file.PayloadSize, file.PayloadKind, file.PackedBlocks, file.Transforms, expected.size, expected.kind, expected.blocks, expected.transforms)
		}
	}
}

func readSupplementalTransforms(t *testing.T, dbconn *sql.DB, fileID int64) ([]supplementalCompressTransform, int) {
	t.Helper()
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
		t.Fatalf("query supplemental transforms: %v", err)
	}
	defer func() { _ = rows.Close() }()
	var transforms []supplementalCompressTransform
	blocks := 0
	for rows.Next() {
		var transform supplementalCompressTransform
		var level sql.NullInt64
		if err := rows.Scan(&transform.StorageCodec, &transform.CompressionCodec, &level, &transform.BlockCount); err != nil {
			t.Fatalf("scan supplemental transform: %v", err)
		}
		if level.Valid {
			value := int(level.Int64)
			transform.CompressionLevel = &value
		}
		transforms = append(transforms, transform)
		blocks += transform.BlockCount
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate supplemental transforms: %v", err)
	}
	return transforms, blocks
}

func compatibilityRepeatedPayload(name string, target int) []byte {
	line := []byte("coldkeep benign old-writer compatibility fixture: " + name + "\n")
	out := make([]byte, target)
	for offset := 0; offset < len(out); {
		offset += copy(out[offset:], line)
	}
	return out
}

func countCompatibilityRows(t *testing.T, dbconn *sql.DB, chunks, blocksCount, refs *int) {
	t.Helper()
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM chunk`).Scan(chunks); err != nil {
		t.Fatalf("count compatibility chunks: %v", err)
	}
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM storage_blocks`).Scan(blocksCount); err != nil {
		t.Fatalf("count compatibility blocks: %v", err)
	}
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM chunk_block_refs`).Scan(refs); err != nil {
		t.Fatalf("count compatibility refs: %v", err)
	}
}

func copyTestDirectory(t *testing.T, source, destination string) {
	t.Helper()
	if err := os.MkdirAll(destination, 0o700); err != nil {
		t.Fatalf("create fixture copy directory: %v", err)
	}
	entries, err := os.ReadDir(source)
	if err != nil {
		t.Fatalf("read fixture directory %s: %v", source, err)
	}
	for _, entry := range entries {
		if entry.IsDir() || entry.Type()&os.ModeSymlink != 0 {
			t.Fatalf("unexpected non-regular fixture entry %s", entry.Name())
		}
		copyTestFile(t, filepath.Join(source, entry.Name()), filepath.Join(destination, entry.Name()))
	}
}
