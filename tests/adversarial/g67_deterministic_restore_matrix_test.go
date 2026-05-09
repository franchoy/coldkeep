package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/franchoy/coldkeep/internal/blocks"
	"github.com/franchoy/coldkeep/internal/chunk"
	"github.com/franchoy/coldkeep/internal/container"
	"github.com/franchoy/coldkeep/internal/db"
	"github.com/franchoy/coldkeep/internal/maintenance"
	"github.com/franchoy/coldkeep/internal/snapshot"
	"github.com/franchoy/coldkeep/internal/storage"
	storagecompression "github.com/franchoy/coldkeep/internal/storage/compression"
	testutils "github.com/franchoy/coldkeep/tests/utils"
	"github.com/franchoy/coldkeep/tests/utils/testgate"
)

// Step 6.7 — Revalidate Deterministic Restore Across Matrix
//
// Core roadmap guarantee: Compression must not affect deterministic restore.
//
// Validation per requirement:
//   ✔ byte-identical restore everywhere
//   ✔ repeated restores identical
//   ✔ restore after GC identical
//   ✔ restore after snapshots identical
//   ✔ same input → same logical output independent of compression/encryption
//
// Test matrix:
//   - Compression modes: "none", "zstd"
//   - Encryption codecs: "plain", "aes-gcm"
//   - Scenarios: baseline, after GC, after snapshot operations, repeated runs

func TestStep67DeterministicRestoreCompressionMatrix(t *testing.T) {
	testgate.RequireDB(t)

	// Matrix: (encryption × compression)
	for _, encryption := range []string{"plain", "aes-gcm"} {
		for _, compression := range []string{
			storagecompression.CompressionNone,
			storagecompression.CompressionZstd,
		} {
			name := fmt.Sprintf("encryption-%s-compression-%s", encryption, compression)
			t.Run(name, func(t *testing.T) {
				testStep67MatrixVariant(t, encryption, compression)
			})
		}
	}
}

// testStep67MatrixVariant tests one (encryption, compression) combination.
func testStep67MatrixVariant(t *testing.T, encryptionCodec, compressionCodec string) {
	t.Helper()

	// Setup environment
	tmp := t.TempDir()
	origContainersDir := container.ContainersDir
	container.ContainersDir = filepath.Join(tmp, "containers")
	t.Cleanup(func() { container.ContainersDir = origContainersDir })

	if err := os.MkdirAll(container.ContainersDir, 0o755); err != nil {
		t.Fatalf("mkdir containers: %v", err)
	}

	t.Setenv("COLDKEEP_STORAGE_DIR", container.ContainersDir)
	testutils.ResetStorage(t)

	// Configure encryption
	if encryptionCodec == "aes-gcm" {
		testutils.SetTestAESGCMKey(t)
	} else {
		if err := os.Setenv("COLDKEEP_KEY", ""); err != nil {
			t.Fatalf("setenv: %v", err)
		}
	}

	// Configure compression
	if err := os.Setenv("COLDKEEP_COMPRESSION", compressionCodec); err != nil {
		t.Fatalf("setenv compression: %v", err)
	}
	if compressionCodec == storagecompression.CompressionZstd {
		if err := os.Setenv("COLDKEEP_COMPRESSION_LEVEL", "3"); err != nil {
			t.Fatalf("setenv level: %v", err)
		}
	}

	dbconn, err := db.ConnectDB()
	if err != nil {
		t.Fatalf("connectDB: %v", err)
	}
	defer dbconn.Close()

	testutils.ApplySchema(t, dbconn)
	testutils.ResetDB(t, dbconn)

	// Set repository compression config
	tx, err := dbconn.Begin()
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	if err := storage.SetDefaultCompression(tx, compressionCodec); err != nil {
		t.Fatalf("set default compression: %v", err)
	}
	if compressionCodec == storagecompression.CompressionZstd {
		if err := storage.SetDefaultCompressionLevel(tx, 3); err != nil {
			t.Fatalf("set compression level: %v", err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit config: %v", err)
	}

	// Generate deterministic test payload
	payload := generateStep67Payload(t, 5*1024*1024)
	payloadHash := sha256Hex(payload)

	// Write input file
	inputFile := filepath.Join(tmp, "input.bin")
	if err := os.WriteFile(inputFile, payload, 0o600); err != nil {
		t.Fatalf("write input: %v", err)
	}

	// Test 1: Store and restore — byte-identical
	codec := blocks.Codec(encryptionCodec)
	fileID := storeStep67File(t, dbconn, tmp, inputFile, codec)
	t.Logf("stored file ID=%d encryption=%s compression=%s", fileID, encryptionCodec, compressionCodec)

	restored := restoreStep67(t, dbconn, tmp, fileID, "restored-baseline.bin")
	validateStep67(t, restored, payload, payloadHash, "baseline")

	// Test 2: Repeated restores must be identical
	for i := 0; i < 3; i++ {
		restored := restoreStep67(t, dbconn, tmp, fileID, fmt.Sprintf("restored-repeat-%d.bin", i))
		validateStep67(t, restored, payload, payloadHash, fmt.Sprintf("repeat-%d", i))
	}

	// Test 3: Store more files for consistency test
	inputFile2 := filepath.Join(tmp, "input2.bin")
	os.WriteFile(inputFile2, generateStep67Payload(t, 3*1024*1024), 0o600)
	fileID2 := storeStep67File(t, dbconn, tmp, inputFile2, codec)

	// Test 4: Restore after GC
	if _, err := maintenance.RunGCWithContainersDirResult(true, container.ContainersDir); err != nil {
		t.Fatalf("gc dry: %v", err)
	}
	if _, err := maintenance.RunGCWithContainersDirResult(false, container.ContainersDir); err != nil {
		t.Fatalf("gc real: %v", err)
	}

	restored = restoreStep67(t, dbconn, tmp, fileID, "restored-post-gc.bin")
	validateStep67(t, restored, payload, payloadHash, "post-gc")

	// Test 5: Restore after snapshot operations
	snapID := fmt.Sprintf("snap-%s-%s-%d", encryptionCodec, compressionCodec, int64(len(payload)))
	label := fmt.Sprintf("snapshot-%s-%s", encryptionCodec, compressionCodec)
	err = snapshot.CreateSnapshot(
		context.Background(),
		dbconn,
		snapID,
		"full",
		&label,
		nil,
		nil,
	)
	if err != nil {
		t.Fatalf("create snapshot: %v", err)
	}

	restored = restoreStep67(t, dbconn, tmp, fileID, "restored-with-snapshot.bin")
	validateStep67(t, restored, payload, payloadHash, "with-snapshot")

	// Test 6: Delete snapshot and restore
	if err := snapshot.DeleteSnapshot(context.Background(), dbconn, snapID); err != nil {
		t.Fatalf("delete snapshot: %v", err)
	}
	restored = restoreStep67(t, dbconn, tmp, fileID, "restored-post-delete.bin")
	validateStep67(t, restored, payload, payloadHash, "post-snapshot-delete")

	// Test 7: Verify other files still restore correctly
	restored2 := restoreStep67(t, dbconn, tmp, fileID2, "restored-other.bin")
	if len(restored2) != 3*1024*1024 {
		t.Fatalf("other file size mismatch: got %d", len(restored2))
	}

	// Test 8: Final consistency check
	restored = restoreStep67(t, dbconn, tmp, fileID, "restored-final.bin")
	validateStep67(t, restored, payload, payloadHash, "final")

	t.Logf("✓ deterministic restore matrix validated: encryption=%s compression=%s",
		encryptionCodec, compressionCodec)
}

// restoreStep67 restores a file and returns the bytes.
func restoreStep67(t *testing.T, dbconn *sql.DB, workDir string, fileID int64, filename string) []byte {
	t.Helper()
	path := filepath.Join(workDir, filename)
	if err := storage.RestoreFileWithDB(dbconn, fileID, path); err != nil {
		t.Fatalf("restore file %d: %v", fileID, err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read restored: %v", err)
	}
	return data
}

// validateStep67 validates that restored bytes match the original.
func validateStep67(t *testing.T, restored, original []byte, originalHash, context string) {
	t.Helper()
	if !bytes.Equal(restored, original) {
		t.Fatalf("%s: restored bytes differ from original (len %d vs %d)",
			context, len(restored), len(original))
	}
	hash := sha256Hex(restored)
	if hash != originalHash {
		t.Fatalf("%s: hash mismatch: got %s want %s", context, hash, originalHash)
	}
}

// generateStep67Payload creates deterministic test payload.
func generateStep67Payload(t *testing.T, size int) []byte {
	t.Helper()
	payload := make([]byte, size)
	pattern := "step-6-7-deterministic-restore-matrix-validation-"
	for i := 0; i < size; i++ {
		payload[i] = pattern[i%len(pattern)]
		if i%256 == 0 {
			payload[i] ^= byte(i / 256)
		}
	}
	return payload
}

// sha256Hex returns SHA256 hash as hex string.
func sha256Hex(data []byte) string {
	h := sha256.Sum256(data)
	return fmt.Sprintf("%x", h[:])
}

// storeStep67File stores a file using the storage API.
func storeStep67File(t *testing.T, dbconn *sql.DB, workDir string, path string, codec blocks.Codec) int64 {
	t.Helper()

	writer := container.NewLocalWriterWithDirAndDB(workDir, container.GetContainerMaxSize(), dbconn)
	sgctx := storage.StorageContext{
		DB:           dbconn,
		Writer:       writer,
		ContainerDir: workDir,
		Chunker:      chunk.DefaultChunker(),
	}

	result, err := storage.StoreFileWithStorageContextAndCodecResult(sgctx, path, codec)
	if err != nil {
		t.Fatalf("store file: %v", err)
	}

	return result.FileID
}

// TestStep67CrossModeDeterminism validates byte-identical restores across modes.
func TestStep67CrossModeDeterminism(t *testing.T) {
	testgate.RequireDB(t)

	if testing.Short() {
		t.Skip("skipping cross-mode in short mode")
	}

	// Create the same file in different modes and verify identical restores
	payload := generateStep67Payload(t, 4*1024*1024)
	payloadHash := sha256Hex(payload)

	modes := []struct {
		name        string
		encryption  string
		compression string
	}{
		{"plain-none", "plain", storagecompression.CompressionNone},
		{"plain-zstd", "plain", storagecompression.CompressionZstd},
		{"aes-none", "aes-gcm", storagecompression.CompressionNone},
		{"aes-zstd", "aes-gcm", storagecompression.CompressionZstd},
	}

	hashes := make(map[string]string)

	for _, mode := range modes {
		// Setup fresh environment for each mode
		tmp := t.TempDir()
		origContainers := container.ContainersDir
		container.ContainersDir = filepath.Join(tmp, "containers")
		os.MkdirAll(container.ContainersDir, 0o755)

		t.Setenv("COLDKEEP_STORAGE_DIR", container.ContainersDir)
		testutils.ResetStorage(t)

		if mode.encryption == "aes-gcm" {
			testutils.SetTestAESGCMKey(t)
		} else {
			os.Setenv("COLDKEEP_KEY", "")
		}

		os.Setenv("COLDKEEP_COMPRESSION", mode.compression)
		if mode.compression == storagecompression.CompressionZstd {
			os.Setenv("COLDKEEP_COMPRESSION_LEVEL", "3")
		}

		dbconn, err := db.ConnectDB()
		if err != nil {
			t.Fatalf("connectDB %s: %v", mode.name, err)
		}

		testutils.ApplySchema(t, dbconn)
		testutils.ResetDB(t, dbconn)

		tx, _ := dbconn.Begin()
		storage.SetDefaultCompression(tx, mode.compression)
		if mode.compression == storagecompression.CompressionZstd {
			storage.SetDefaultCompressionLevel(tx, 3)
		}
		tx.Commit()

		// Store and restore
		inputFile := filepath.Join(tmp, "input.bin")
		os.WriteFile(inputFile, payload, 0o600)

		codec := blocks.Codec(mode.encryption)
		fileID := storeStep67File(t, dbconn, tmp, inputFile, codec)
		restored := restoreStep67(t, dbconn, tmp, fileID, "restored.bin")

		// Verify original and collect hash
		if !bytes.Equal(restored, payload) {
			t.Fatalf("%s: restored bytes mismatch", mode.name)
		}
		hash := sha256Hex(restored)
		if hash != payloadHash {
			t.Fatalf("%s: hash mismatch %s vs %s", mode.name, hash, payloadHash)
		}
		hashes[mode.name] = hash

		dbconn.Close()
		container.ContainersDir = origContainers
	}

	// Verify all modes produced identical hashes
	baseline := hashes["plain-none"]
	for name, hash := range hashes {
		if hash != baseline {
			t.Fatalf("cross-mode hash mismatch %s: got %s want %s", name, hash, baseline)
		}
	}

	t.Logf("✓ cross-mode determinism: all %d modes produce identical restores", len(modes))
}
