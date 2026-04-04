package main

import (
	"bytes"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	dbschema "github.com/franchoy/coldkeep/db"
	"github.com/franchoy/coldkeep/internal/container"
	"github.com/franchoy/coldkeep/internal/db"
	"github.com/franchoy/coldkeep/internal/maintenance"
	"github.com/franchoy/coldkeep/internal/recovery"
	"github.com/franchoy/coldkeep/internal/storage"
	"github.com/franchoy/coldkeep/internal/verify"
	testutils "github.com/franchoy/coldkeep/tests/utils"
	"github.com/franchoy/coldkeep/tests/utils/testgate"
)

// G1 — Deterministic, byte-identical restore
//
// Adversarial goals:
//   - restore is never "almost correct"
//   - corruption must either fail explicitly or be caught by byte/hash mismatch
//   - healthy unrelated data must keep restoring identically
//
// Notes:
//   - This file intentionally runs a codec matrix for plain + aes-gcm because
//     codec behavior is part of the restore contract.
//   - It does not currently run a DB-backend matrix. The uploaded integration
//     harness and shared helpers are Postgres-oriented (DefaultCLIEnv / RequireDB),
//     while SQLite coverage currently appears in lower-level restore tests rather
//     than this new integration/adversarial structure.

func adversarialG1Codecs() []string {
	return []string{"plain", "aes-gcm"}
}

func configureAdversarialG1Codec(t *testing.T, codec string) {
	t.Helper()
	if codec == "aes-gcm" {
		testutils.SetTestAESGCMKey(t)
	}
}

func setupAdversarialG1Env(t *testing.T) (*sql.DB, map[string]string, string, string, string) {
	t.Helper()

	tmp := t.TempDir()
	container.ContainersDir = filepath.Join(tmp, "containers")
	_ = os.Setenv("COLDKEEP_STORAGE_DIR", container.ContainersDir)
	testutils.ResetStorage(t)

	env := testutils.DefaultCLIEnv(container.ContainersDir)

	for k, v := range env {
		os.Setenv(k, v)
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

func storeFileWithCodecCLI(t *testing.T, repoRoot, binPath string, env map[string]string, codec, path string) int64 {
	t.Helper()

	payload := testutils.AssertCLIJSONOK(
		t,
		testutils.RunColdkeepCommand(t, repoRoot, binPath, env, "store", "--codec", codec, path, "--output", "json"),
		"store",
	)
	data := testutils.JSONMap(t, payload, "data")
	return testutils.JSONInt64(t, data, "file_id")
}

func restoreMustMatchBytes(t *testing.T, dbconn *sql.DB, fileID int64, outPath string, want []byte, wantHash string) {
	t.Helper()

	if err := storage.RestoreFileWithDB(dbconn, fileID, outPath); err != nil {
		t.Fatalf("restore file %d: %v", fileID, err)
	}
	got := testutils.MustRead(t, outPath)
	if !bytes.Equal(got, want) {
		t.Fatalf("restored bytes differ from original")
	}
	if gotHash := testutils.SHA256File(t, outPath); gotHash != wantHash {
		t.Fatalf("restored hash mismatch: want %s got %s", wantHash, gotHash)
	}
}

func restoreMustFailOrMatchOriginal(t *testing.T, dbconn *sql.DB, fileID int64, outPath string, want []byte, wantHash string) {
	t.Helper()

	err := storage.RestoreFileWithDB(dbconn, fileID, outPath)
	if err != nil {
		return
	}

	got := testutils.MustRead(t, outPath)
	gotHash := testutils.SHA256File(t, outPath)
	if !bytes.Equal(got, want) || gotHash != wantHash {
		t.Fatalf("restore silently produced mutated output: wantHash=%s gotHash=%s", wantHash, gotHash)
	}
}

func rotateContainerWithDistinctFile(t *testing.T, repoRoot, binPath string, env map[string]string, codec, inputDir, name string, size int) int64 {
	t.Helper()

	p := testutils.CreateTempFile(t, inputDir, name, size)
	b := testutils.MustRead(t, p)
	if len(b) > 0 {
		b[0] ^= 0xA7
	}
	if err := os.WriteFile(p, b, 0o644); err != nil {
		t.Fatalf("rewrite distinct file %s: %v", name, err)
	}
	return storeFileWithCodecCLI(t, repoRoot, binPath, env, codec, p)
}

func TestAdversarialG1RepeatRestoreAcrossRecoveryGCAndRotation(t *testing.T) {
	testgate.RequireDB(t)

	for _, codec := range adversarialG1Codecs() {
		t.Run(codec, func(t *testing.T) {
			configureAdversarialG1Codec(t, codec)

			dbconn, env, repoRoot, binPath, tmp := setupAdversarialG1Env(t)
			defer dbconn.Close()

			originalMaxSize := container.GetContainerMaxSize()
			container.SetContainerMaxSize(3 * 1024 * 1024)
			defer container.SetContainerMaxSize(originalMaxSize)

			inputDir := filepath.Join(tmp, "input")
			restoreDir := filepath.Join(tmp, "restore")
			if err := os.MkdirAll(inputDir, 0o755); err != nil {
				t.Fatalf("mkdir input: %v", err)
			}
			if err := os.MkdirAll(restoreDir, 0o755); err != nil {
				t.Fatalf("mkdir restore: %v", err)
			}

			inPath := testutils.CreateTempFile(t, inputDir, "g1-repeat.bin", 2*1024*1024+257)
			wantHash := testutils.SHA256File(t, inPath)
			wantBytes := testutils.MustRead(t, inPath)
			fileID := storeFileWithCodecCLI(t, repoRoot, binPath, env, codec, inPath)

			_ = rotateContainerWithDistinctFile(t, repoRoot, binPath, env, codec, inputDir, "rotation-a.bin", 2*1024*1024+1024)
			_ = rotateContainerWithDistinctFile(t, repoRoot, binPath, env, codec, inputDir, "rotation-b.bin", 2*1024*1024+2048)

			const rounds = 5
			for i := 0; i < rounds; i++ {
				outPath := filepath.Join(restoreDir, fmt.Sprintf("repeat-%d.bin", i))
				restoreMustMatchBytes(t, dbconn, fileID, outPath, wantBytes, wantHash)

				if _, err := maintenance.RunGCWithContainersDirResult(true, container.ContainersDir); err != nil {
					t.Fatalf("round %d gc dry-run: %v", i, err)
				}
				if _, err := maintenance.RunGCWithContainersDirResult(false, container.ContainersDir); err != nil {
					t.Fatalf("round %d gc real: %v", i, err)
				}

				if err := dbconn.Close(); err != nil {
					t.Fatalf("round %d close db before recovery: %v", i, err)
				}
				if err := recovery.SystemRecoveryWithContainersDir(container.ContainersDir); err != nil {
					t.Fatalf("round %d recovery: %v", i, err)
				}
				reopened, err := db.ConnectDB()
				if err != nil {
					t.Fatalf("round %d reconnect db: %v", i, err)
				}
				dbconn = reopened
				testutils.ApplySchema(t, dbconn)
			}
		})
	}
}

func TestAdversarialG1CorruptedChunkPayloadDoesNotAlmostRestore(t *testing.T) {
	testgate.RequireDB(t)

	for _, codec := range adversarialG1Codecs() {
		t.Run(codec, func(t *testing.T) {
			configureAdversarialG1Codec(t, codec)

			dbconn, env, repoRoot, binPath, tmp := setupAdversarialG1Env(t)
			defer dbconn.Close()

			inputDir := filepath.Join(tmp, "input")
			restoreDir := filepath.Join(tmp, "restore")
			if err := os.MkdirAll(inputDir, 0o755); err != nil {
				t.Fatalf("mkdir input: %v", err)
			}
			if err := os.MkdirAll(restoreDir, 0o755); err != nil {
				t.Fatalf("mkdir restore: %v", err)
			}

			inPath := testutils.CreateTempFile(t, inputDir, "payload-corruption.bin", 768*1024)
			wantHash := testutils.SHA256File(t, inPath)
			wantBytes := testutils.MustRead(t, inPath)
			fileID := storeFileWithCodecCLI(t, repoRoot, binPath, env, codec, inPath)

			testutils.CorruptFirstCompletedChunkByte(t, dbconn, container.ContainersDir)

			if err := maintenance.VerifyCommandWithContainersDir(container.ContainersDir, "system", 0, verify.VerifyDeep); err == nil {
				t.Fatalf("expected deep verify failure after payload corruption")
			}

			outPath := filepath.Join(restoreDir, "payload-corruption.restored.bin")
			restoreMustFailOrMatchOriginal(t, dbconn, fileID, outPath, wantBytes, wantHash)
		})
	}
}

func TestAdversarialG1CorruptedChunkHashMetadataDoesNotAlmostRestore(t *testing.T) {
	testgate.RequireDB(t)

	for _, codec := range adversarialG1Codecs() {
		t.Run(codec, func(t *testing.T) {
			configureAdversarialG1Codec(t, codec)

			dbconn, env, repoRoot, binPath, tmp := setupAdversarialG1Env(t)
			defer dbconn.Close()

			inputDir := filepath.Join(tmp, "input")
			restoreDir := filepath.Join(tmp, "restore")
			if err := os.MkdirAll(inputDir, 0o755); err != nil {
				t.Fatalf("mkdir input: %v", err)
			}
			if err := os.MkdirAll(restoreDir, 0o755); err != nil {
				t.Fatalf("mkdir restore: %v", err)
			}

			inPath := testutils.CreateTempFile(t, inputDir, "hash-metadata-corruption.bin", 1024*1024)
			wantHash := testutils.SHA256File(t, inPath)
			wantBytes := testutils.MustRead(t, inPath)
			fileID := storeFileWithCodecCLI(t, repoRoot, binPath, env, codec, inPath)

			record := testutils.FetchFirstFileChunkRecord(t, dbconn, fileID)
			if _, err := dbconn.Exec(`UPDATE chunk SET chunk_hash = $1 WHERE id = $2`, strings.Repeat("f", 64), record.ChunkID); err != nil {
				t.Fatalf("corrupt chunk hash metadata: %v", err)
			}

			outPath := filepath.Join(restoreDir, "hash-metadata-corruption.restored.bin")
			restoreMustFailOrMatchOriginal(t, dbconn, fileID, outPath, wantBytes, wantHash)
		})
	}
}

func TestAdversarialG1CorruptedChunkOrderDoesNotAlmostRestore(t *testing.T) {
	testgate.RequireDB(t)

	for _, codec := range adversarialG1Codecs() {
		t.Run(codec, func(t *testing.T) {
			configureAdversarialG1Codec(t, codec)

			dbconn, env, repoRoot, binPath, tmp := setupAdversarialG1Env(t)
			defer dbconn.Close()

			inputDir := filepath.Join(tmp, "input")
			restoreDir := filepath.Join(tmp, "restore")
			if err := os.MkdirAll(inputDir, 0o755); err != nil {
				t.Fatalf("mkdir input: %v", err)
			}
			if err := os.MkdirAll(restoreDir, 0o755); err != nil {
				t.Fatalf("mkdir restore: %v", err)
			}

			inPath := testutils.CreateTempFile(t, inputDir, "order-corruption.bin", 3*1024*1024+777)
			wantHash := testutils.SHA256File(t, inPath)
			wantBytes := testutils.MustRead(t, inPath)
			fileID := storeFileWithCodecCLI(t, repoRoot, binPath, env, codec, inPath)

			var chunkCount int
			if err := dbconn.QueryRow(`SELECT COUNT(*) FROM file_chunk WHERE logical_file_id = $1`, fileID).Scan(&chunkCount); err != nil {
				t.Fatalf("count file_chunk rows: %v", err)
			}
			if chunkCount < 2 {
				t.Fatalf("expected multi-chunk file for order corruption test, got %d chunk rows", chunkCount)
			}

			// 3-step swap to avoid unique constraint violation and respect chunk_order >= 0
			_, err := dbconn.Exec(`
				       UPDATE file_chunk SET chunk_order = 1000000
				       WHERE logical_file_id = $1 AND chunk_order = 0
			       `, fileID)
			if err != nil {
				t.Fatalf("step 1 swap: %v", err)
			}

			_, err = dbconn.Exec(`
				       UPDATE file_chunk SET chunk_order = 0
				       WHERE logical_file_id = $1 AND chunk_order = 1
			       `, fileID)
			if err != nil {
				t.Fatalf("step 2 swap: %v", err)
			}

			_, err = dbconn.Exec(`
				       UPDATE file_chunk SET chunk_order = 1
				       WHERE logical_file_id = $1 AND chunk_order = 1000000
			       `, fileID)
			if err != nil {
				t.Fatalf("step 3 swap: %v", err)
			}

			outPath := filepath.Join(restoreDir, "order-corruption.restored.bin")
			restoreMustFailOrMatchOriginal(t, dbconn, fileID, outPath, wantBytes, wantHash)
		})
	}
}

func TestAdversarialG1UnrelatedContainerDamagePreservesHealthyRestore(t *testing.T) {
	testgate.RequireDB(t)

	for _, codec := range adversarialG1Codecs() {
		t.Run(codec, func(t *testing.T) {
			configureAdversarialG1Codec(t, codec)

			dbconn, env, repoRoot, binPath, tmp := setupAdversarialG1Env(t)
			defer dbconn.Close()

			originalMaxSize := container.GetContainerMaxSize()
			// Force container separation: set max size below file size
			container.SetContainerMaxSize(600 * 1024)
			defer container.SetContainerMaxSize(originalMaxSize)

			inputDir := filepath.Join(tmp, "input")
			restoreDir := filepath.Join(tmp, "restore")
			if err := os.MkdirAll(inputDir, 0o755); err != nil {
				t.Fatalf("mkdir input: %v", err)
			}
			if err := os.MkdirAll(restoreDir, 0o755); err != nil {
				t.Fatalf("mkdir restore: %v", err)
			}

			anchorPath := testutils.CreateTempFile(t, inputDir, "anchor.bin", 700*1024)
			anchorHash := testutils.SHA256File(t, anchorPath)
			anchorBytes := testutils.MustRead(t, anchorPath)
			anchorID := storeFileWithCodecCLI(t, repoRoot, binPath, env, codec, anchorPath)

			// Fill the first container to force rollover for the next file
			for i := 0; i < 3; i++ {
				_ = rotateContainerWithDistinctFile(t, repoRoot, binPath, env, codec, inputDir, fmt.Sprintf("filler-%d.bin", i), 300*1024)
			}

			otherPath := filepath.Join(inputDir, "other.bin")
			other := make([]byte, 700*1024)
			for i := range other {
				other[i] = byte((i*17 + 11) % 251)
			}
			if err := os.WriteFile(otherPath, other, 0o644); err != nil {
				t.Fatalf("write other file: %v", err)
			}
			otherID := storeFileWithCodecCLI(t, repoRoot, binPath, env, codec, otherPath)

			anchorRecord := testutils.FetchFirstFileChunkRecord(t, dbconn, anchorID)
			otherRecord := testutils.FetchFirstFileChunkRecord(t, dbconn, otherID)
			if anchorRecord.ContainerID == otherRecord.ContainerID {
				t.Skip("files ended up in same container due to packing; skipping isolation assertion")
			}

			damagedPath := testutils.ContainerPathForRecord(otherRecord)
			f, err := os.OpenFile(damagedPath, os.O_RDWR, 0)
			if err != nil {
				t.Fatalf("open unrelated container for corruption: %v", err)
			}
			damageOffset := otherRecord.BlockOffset
			if otherRecord.StoredSize > 10 {
				damageOffset += 10
			}
			if _, err := f.WriteAt([]byte{0xD3}, damageOffset); err != nil {
				_ = f.Close()
				t.Fatalf("write unrelated corruption byte: %v", err)
			}
			if err := f.Close(); err != nil {
				t.Fatalf("close unrelated container after corruption: %v", err)
			}

			anchorOut := filepath.Join(restoreDir, "anchor.restored.bin")
			restoreMustMatchBytes(t, dbconn, anchorID, anchorOut, anchorBytes, anchorHash)

			otherHash := testutils.SHA256File(t, otherPath)
			restoreMustFailOrMatchOriginal(t, dbconn, otherID, filepath.Join(restoreDir, "other.restored.bin"), other, otherHash)
		})
	}
}

var _ = dbschema.PostgresSchema
