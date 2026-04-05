package main

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	dbschema "github.com/franchoy/coldkeep/db"
	"github.com/franchoy/coldkeep/internal/container"
	"github.com/franchoy/coldkeep/internal/db"
	"github.com/franchoy/coldkeep/internal/maintenance"
	"github.com/franchoy/coldkeep/internal/storage"
	"github.com/franchoy/coldkeep/internal/verify"
	testutils "github.com/franchoy/coldkeep/tests/utils"
	"github.com/franchoy/coldkeep/tests/utils/testgate"
)

// G7 — Deep corruption detection
//
// Adversarial goals:
//   - payload / metadata / container corruption must always be detected
//   - deep verification must not silently accept tampered ciphertext or metadata
//   - restore must never succeed with corrupted content
//   - multi-corruption situations should still surface as deep verification failure
//
// Notes:
//   - This file uses the current Postgres-backed adversarial harness.
//   - Plain and aes-gcm are both covered where relevant.
//   - AES-GCM-specific tests explicitly validate authenticated decode failures.

func adversarialG7Codecs() []string {
	return []string{"plain", "aes-gcm"}
}

func configureAdversarialG7Codec(t *testing.T, codec string) {
	t.Helper()
	if codec == "aes-gcm" {
		testutils.SetTestAESGCMKey(t)
	}
}

func setupAdversarialG7Env(t *testing.T) (*sql.DB, map[string]string, string, string, string) {
	t.Helper()

	tmp := t.TempDir()
	container.ContainersDir = filepath.Join(tmp, "containers")
	_ = os.Setenv("COLDKEEP_STORAGE_DIR", container.ContainersDir)
	testutils.ResetStorage(t)

	env := testutils.DefaultCLIEnv(container.ContainersDir)
	for k, v := range env {
		_ = os.Setenv(k, v)
	}

	dbconn, err := db.ConnectDB()
	if err != nil {
		t.Fatalf("connectDB: %v", err)
	}

	testutils.ApplySchema(t, dbconn)
	testutils.ResetDB(t, dbconn)

	repoRoot := testutils.FindRepoRoot(t)
	binPath := testutils.BuildColdkeepBinary(t, repoRoot)

	return dbconn, env, repoRoot, binPath, tmp
}

func storeFileWithCodecCLIG7(t *testing.T, repoRoot, binPath string, env map[string]string, codec, path string) int64 {
	t.Helper()

	payload := testutils.AssertCLIJSONOK(
		t,
		testutils.RunColdkeepCommand(t, repoRoot, binPath, env, "store", "--codec", codec, path, "--output", "json"),
		"store",
	)
	data := testutils.JSONMap(t, payload, "data")
	return testutils.JSONInt64(t, data, "file_id")
}

func deepVerifyMustFailG7(t *testing.T, err error, context string) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected %s deep verify failure but got nil", context)
	}
}

func restoreMustFailG7(t *testing.T, dbconn *sql.DB, fileID int64, outPath string) {
	t.Helper()
	if err := storage.RestoreFileWithDB(dbconn, fileID, outPath); err == nil {
		t.Fatalf("restore unexpectedly succeeded for corrupted file %d", fileID)
	}
}

func TestAdversarialG7DeepVerifyDetectsPayloadCorruption(t *testing.T) {
	testgate.RequireDB(t)

	for _, codec := range adversarialG7Codecs() {
		t.Run(codec, func(t *testing.T) {
			configureAdversarialG7Codec(t, codec)

			dbconn, env, repoRoot, binPath, tmp := setupAdversarialG7Env(t)
			defer dbconn.Close()

			inputDir := filepath.Join(tmp, "input")
			restoreDir := filepath.Join(tmp, "restore")
			if err := os.MkdirAll(inputDir, 0o755); err != nil {
				t.Fatalf("mkdir input: %v", err)
			}
			if err := os.MkdirAll(restoreDir, 0o755); err != nil {
				t.Fatalf("mkdir restore: %v", err)
			}

			inPath := testutils.CreateTempFile(t, inputDir, "g7-payload.bin", 768*1024)
			fileID := storeFileWithCodecCLIG7(t, repoRoot, binPath, env, codec, inPath)

			testutils.CorruptFirstCompletedChunkByte(t, dbconn, container.ContainersDir)

			err := maintenance.VerifyCommandWithContainersDir(container.ContainersDir, "system", 0, verify.VerifyDeep)
			deepVerifyMustFailG7(t, err, "payload corruption")

			restoreMustFailG7(t, dbconn, fileID, filepath.Join(restoreDir, "payload.restored.bin"))
		})
	}
}

func TestAdversarialG7DeepVerifyDetectsChunkHashMetadataCorruption(t *testing.T) {
	testgate.RequireDB(t)

	for _, codec := range adversarialG7Codecs() {
		t.Run(codec, func(t *testing.T) {
			configureAdversarialG7Codec(t, codec)

			dbconn, env, repoRoot, binPath, tmp := setupAdversarialG7Env(t)
			defer dbconn.Close()

			inputDir := filepath.Join(tmp, "input")
			restoreDir := filepath.Join(tmp, "restore")
			if err := os.MkdirAll(inputDir, 0o755); err != nil {
				t.Fatalf("mkdir input: %v", err)
			}
			if err := os.MkdirAll(restoreDir, 0o755); err != nil {
				t.Fatalf("mkdir restore: %v", err)
			}

			inPath := testutils.CreateTempFile(t, inputDir, "g7-hash-meta.bin", 1024*1024)
			fileID := storeFileWithCodecCLIG7(t, repoRoot, binPath, env, codec, inPath)

			record := testutils.FetchFirstFileChunkRecord(t, dbconn, fileID)
			if _, err := dbconn.Exec(`UPDATE chunk SET chunk_hash = $1 WHERE id = $2`, fmt.Sprintf("%064x", 777), record.ChunkID); err != nil {
				t.Fatalf("corrupt chunk hash metadata: %v", err)
			}

			err := maintenance.VerifyCommandWithContainersDir(container.ContainersDir, "system", 0, verify.VerifyDeep)
			deepVerifyMustFailG7(t, err, "chunk hash metadata corruption")

			restoreMustFailG7(t, dbconn, fileID, filepath.Join(restoreDir, "hash-meta.restored.bin"))
		})
	}
}

func TestAdversarialG7DeepVerifyDetectsTrailingGarbageAfterLastBlock(t *testing.T) {
	testgate.RequireDB(t)

	for _, codec := range adversarialG7Codecs() {
		t.Run(codec, func(t *testing.T) {
			configureAdversarialG7Codec(t, codec)

			dbconn, env, repoRoot, binPath, tmp := setupAdversarialG7Env(t)
			defer dbconn.Close()

			inputDir := filepath.Join(tmp, "input")
			if err := os.MkdirAll(inputDir, 0o755); err != nil {
				t.Fatalf("mkdir input: %v", err)
			}

			inPath := testutils.CreateTempFile(t, inputDir, "g7-tail.bin", 640*1024)
			fileID := storeFileWithCodecCLIG7(t, repoRoot, binPath, env, codec, inPath)

			record := testutils.FetchFirstFileChunkRecord(t, dbconn, fileID)
			containerPath := testutils.ContainerPathForRecord(record)

			f, err := os.OpenFile(containerPath, os.O_WRONLY|os.O_APPEND, 0)
			if err != nil {
				t.Fatalf("open container for trailing garbage append: %v", err)
			}
			if _, err := f.Write([]byte("G7_TRAILING_GARBAGE")); err != nil {
				_ = f.Close()
				t.Fatalf("append trailing garbage: %v", err)
			}
			if err := f.Close(); err != nil {
				t.Fatalf("close container after trailing garbage append: %v", err)
			}

			err = maintenance.VerifyCommandWithContainersDir(container.ContainersDir, "system", 0, verify.VerifyDeep)
			deepVerifyMustFailG7(t, err, "trailing garbage")
		})
	}
}

func TestAdversarialG7AESGCMDetectsWrongKeyMismatch(t *testing.T) {
	testgate.RequireDB(t)

	const codec = "aes-gcm"
	configureAdversarialG7Codec(t, codec)

	dbconn, env, repoRoot, binPath, tmp := setupAdversarialG7Env(t)
	defer dbconn.Close()

	inputDir := filepath.Join(tmp, "input")
	restoreDir := filepath.Join(tmp, "restore")
	if err := os.MkdirAll(inputDir, 0o755); err != nil {
		t.Fatalf("mkdir input: %v", err)
	}
	if err := os.MkdirAll(restoreDir, 0o755); err != nil {
		t.Fatalf("mkdir restore: %v", err)
	}

	inPath := testutils.CreateTempFile(t, inputDir, "g7-wrong-key.bin", 768*1024)
	fileID := storeFileWithCodecCLIG7(t, repoRoot, binPath, env, codec, inPath)

	if err := os.Setenv("COLDKEEP_KEY", "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"); err != nil {
		t.Fatalf("set wrong key: %v", err)
	}

	err := maintenance.VerifyCommandWithContainersDir(container.ContainersDir, "system", 0, verify.VerifyDeep)
	deepVerifyMustFailG7(t, err, "aes-gcm wrong key mismatch")

	restoreMustFailG7(t, dbconn, fileID, filepath.Join(restoreDir, "wrong-key.restored.bin"))
}

func TestAdversarialG7AESGCMDetectsNonceMetadataTampering(t *testing.T) {
	testgate.RequireDB(t)

	const codec = "aes-gcm"
	configureAdversarialG7Codec(t, codec)

	dbconn, env, repoRoot, binPath, tmp := setupAdversarialG7Env(t)
	defer dbconn.Close()

	inputDir := filepath.Join(tmp, "input")
	restoreDir := filepath.Join(tmp, "restore")
	if err := os.MkdirAll(inputDir, 0o755); err != nil {
		t.Fatalf("mkdir input: %v", err)
	}
	if err := os.MkdirAll(restoreDir, 0o755); err != nil {
		t.Fatalf("mkdir restore: %v", err)
	}

	inPath := testutils.CreateTempFile(t, inputDir, "g7-nonce.bin", 1024*1024)
	fileID := storeFileWithCodecCLIG7(t, repoRoot, binPath, env, codec, inPath)

	record := testutils.FetchFirstFileChunkRecord(t, dbconn, fileID)
	// Use a valid-length (12 bytes) but incorrect nonce for AES-GCM tampering
	badNonce := make([]byte, 12)
	for i := range badNonce {
		badNonce[i] = 0xAA
	}
	if _, err := dbconn.Exec(`UPDATE blocks SET nonce = $1 WHERE chunk_id = $2`, badNonce, record.ChunkID); err != nil {
		t.Fatalf("tamper nonce metadata: %v", err)
	}

	err := maintenance.VerifyCommandWithContainersDir(container.ContainersDir, "system", 0, verify.VerifyDeep)
	deepVerifyMustFailG7(t, err, "aes-gcm nonce metadata tampering")

	restoreMustFailG7(t, dbconn, fileID, filepath.Join(restoreDir, "nonce-tamper.restored.bin"))
}

func TestAdversarialG7AESGCMDetectsInvalidKeyConfiguration(t *testing.T) {
	testgate.RequireDB(t)

	const codec = "aes-gcm"
	configureAdversarialG7Codec(t, codec)

	dbconn, env, repoRoot, binPath, tmp := setupAdversarialG7Env(t)
	defer dbconn.Close()

	inputDir := filepath.Join(tmp, "input")
	restoreDir := filepath.Join(tmp, "restore")
	if err := os.MkdirAll(inputDir, 0o755); err != nil {
		t.Fatalf("mkdir input: %v", err)
	}
	if err := os.MkdirAll(restoreDir, 0o755); err != nil {
		t.Fatalf("mkdir restore: %v", err)
	}

	inPath := testutils.CreateTempFile(t, inputDir, "g7-invalid-key.bin", 640*1024)
	fileID := storeFileWithCodecCLIG7(t, repoRoot, binPath, env, codec, inPath)

	if err := os.Setenv("COLDKEEP_KEY", "1234"); err != nil {
		t.Fatalf("set invalid key config: %v", err)
	}

	err := maintenance.VerifyCommandWithContainersDir(container.ContainersDir, "system", 0, verify.VerifyDeep)
	deepVerifyMustFailG7(t, err, "aes-gcm invalid key configuration")

	restoreMustFailG7(t, dbconn, fileID, filepath.Join(restoreDir, "invalid-key.restored.bin"))
}

func TestAdversarialG7DeepVerifyFailsWhenMultipleChunksAreCorrupted(t *testing.T) {
	testgate.RequireDB(t)
	testgate.RequireLongRun(t)

	for _, codec := range adversarialG7Codecs() {
		t.Run(codec, func(t *testing.T) {
			configureAdversarialG7Codec(t, codec)

			dbconn, env, repoRoot, binPath, tmp := setupAdversarialG7Env(t)
			defer dbconn.Close()

			inputDir := filepath.Join(tmp, "input")
			if err := os.MkdirAll(inputDir, 0o755); err != nil {
				t.Fatalf("mkdir input: %v", err)
			}

			firstPath := testutils.CreateTempFile(t, inputDir, "g7-multi-a.bin", 1024*1024+111)
			secondPath := filepath.Join(inputDir, "g7-multi-b.bin")
			secondData := make([]byte, 1024*1024+777)
			for i := range secondData {
				secondData[i] = byte((i*17 + 3) % 251)
			}
			if err := os.WriteFile(secondPath, secondData, 0o644); err != nil {
				t.Fatalf("write second file: %v", err)
			}

			_ = storeFileWithCodecCLIG7(t, repoRoot, binPath, env, codec, firstPath)
			_ = storeFileWithCodecCLIG7(t, repoRoot, binPath, env, codec, secondPath)

			testutils.CorruptFirstCompletedChunkByte(t, dbconn, container.ContainersDir)

			// Corrupt another completed chunk/container location.
			var secondFilename string
			var secondOffset int64
			var secondStoredSize int64
			err := dbconn.QueryRow(`
				SELECT ctr.filename, b.block_offset, b.stored_size
				FROM chunk c
				JOIN blocks b ON b.chunk_id = c.id
				JOIN container ctr ON ctr.id = b.container_id
				WHERE c.status = 'COMPLETED'
				ORDER BY c.id DESC
				LIMIT 1
			`).Scan(&secondFilename, &secondOffset, &secondStoredSize)
			if err != nil {
				t.Fatalf("query second completed chunk for corruption: %v", err)
			}

			secondContainerPath := filepath.Join(container.ContainersDir, secondFilename)
			f, err := os.OpenFile(secondContainerPath, os.O_RDWR, 0)
			if err != nil {
				t.Fatalf("open second container for corruption: %v", err)
			}
			secondCorruptionOffset := secondOffset
			if secondStoredSize > 20 {
				secondCorruptionOffset += 20
			}
			if _, err := f.WriteAt([]byte{0x7E}, secondCorruptionOffset); err != nil {
				_ = f.Close()
				t.Fatalf("write second corruption byte: %v", err)
			}
			if err := f.Close(); err != nil {
				t.Fatalf("close second container after corruption: %v", err)
			}

			err = maintenance.VerifyCommandWithContainersDir(container.ContainersDir, "system", 0, verify.VerifyDeep)
			deepVerifyMustFailG7(t, err, "multiple chunk corruption")
		})
	}
}

var _ = dbschema.PostgresSchema
