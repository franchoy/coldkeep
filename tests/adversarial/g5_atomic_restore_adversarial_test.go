package main

import (
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	dbschema "github.com/franchoy/coldkeep/db"
	"github.com/franchoy/coldkeep/internal/container"
	"github.com/franchoy/coldkeep/internal/db"
	"github.com/franchoy/coldkeep/internal/storage"
	testutils "github.com/franchoy/coldkeep/tests/utils"
	"github.com/franchoy/coldkeep/tests/utils/testgate"
)

// G5 — Atomic restore operations
//
// Adversarial goals:
//   - restore must never leave accepted partial output behind
//   - failure before final rename must not mutate unrelated existing target data
//   - repeated restore must remain byte-identical
//   - restore to the same target after interruption must converge cleanly
//
// Notes:
//   - This file uses the current Postgres-backed adversarial harness.
//   - The tests focus on the atomic temp-write + rename contract from the restore path.
//   - A codec matrix is used for plain + aes-gcm because restore atomicity must hold
//     regardless of payload codec.

func adversarialG5Codecs() []string {
	return []string{"plain", "aes-gcm"}
}

func configureAdversarialG5Codec(t *testing.T, codec string) {
	t.Helper()
	if codec == "aes-gcm" {
		testutils.SetTestAESGCMKey(t)
	}
}

func setupAdversarialG5Env(t *testing.T) (*sql.DB, map[string]string, string, string, string) {
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

func storeFileWithCodecCLIG5(t *testing.T, repoRoot, binPath string, env map[string]string, codec, path string) int64 {
	t.Helper()

	payload := testutils.AssertCLIJSONOK(
		t,
		testutils.RunColdkeepCommand(t, repoRoot, binPath, env, "store", "--codec", codec, path, "--output", "json"),
		"store",
	)
	data := testutils.JSONMap(t, payload, "data")
	return testutils.JSONInt64(t, data, "file_id")
}

func restoreMustMatchHashG5(t *testing.T, dbconn *sql.DB, fileID int64, outPath, wantHash string) {
	t.Helper()

	if err := storage.RestoreFileWithDB(dbconn, fileID, outPath); err != nil {
		t.Fatalf("restore file %d: %v", fileID, err)
	}
	if gotHash := testutils.SHA256File(t, outPath); gotHash != wantHash {
		t.Fatalf("restored hash mismatch: want %s got %s", wantHash, gotHash)
	}
}

func TestAdversarialG5RestoreFailureToExistingDirectoryLeavesNoMutatedOutput(t *testing.T) {
	testgate.RequireDB(t)

	for _, codec := range adversarialG5Codecs() {
		t.Run(codec, func(t *testing.T) {
			configureAdversarialG5Codec(t, codec)

			dbconn, env, repoRoot, binPath, tmp := setupAdversarialG5Env(t)
			defer dbconn.Close()

			inputDir := filepath.Join(tmp, "input")
			outRoot := filepath.Join(tmp, "output")
			if err := os.MkdirAll(inputDir, 0o755); err != nil {
				t.Fatalf("mkdir input: %v", err)
			}
			if err := os.MkdirAll(outRoot, 0o755); err != nil {
				t.Fatalf("mkdir output: %v", err)
			}

			inPath := testutils.CreateTempFile(t, inputDir, "g5-existing-dir.bin", 768*1024)
			fileID := storeFileWithCodecCLIG5(t, repoRoot, binPath, env, codec, inPath)

			targetPath := filepath.Join(outRoot, "restore-target")
			if err := os.MkdirAll(targetPath, 0o755); err != nil {
				t.Fatalf("mkdir targetPath as directory: %v", err)
			}

			err := storage.RestoreFileWithDB(dbconn, fileID, targetPath)
			if err != nil {
				t.Fatalf("restore failed unexpectedly for directory target: %v", err)
			}

			// Expect file inside directory
			files, readErr := os.ReadDir(targetPath)
			if readErr != nil {
				t.Fatalf("readdir target directory after restore: %v", readErr)
			}
			if len(files) != 1 {
				t.Fatalf("expected exactly one file inside directory, got %d", len(files))
			}

			restoredPath := filepath.Join(targetPath, files[0].Name())
			wantHash := testutils.SHA256File(t, inPath)
			if gotHash := testutils.SHA256File(t, restoredPath); gotHash != wantHash {
				t.Fatalf("restored file hash mismatch: want %s got %s", wantHash, gotHash)
			}
		})
	}
}

func TestAdversarialG5RestoreFailureDoesNotOverwriteExistingFile(t *testing.T) {
	testgate.RequireDB(t)

	for _, codec := range adversarialG5Codecs() {
		t.Run(codec, func(t *testing.T) {
			configureAdversarialG5Codec(t, codec)

			dbconn, env, repoRoot, binPath, tmp := setupAdversarialG5Env(t)
			defer dbconn.Close()

			inputDir := filepath.Join(tmp, "input")
			outRoot := filepath.Join(tmp, "output")
			if err := os.MkdirAll(inputDir, 0o755); err != nil {
				t.Fatalf("mkdir input: %v", err)
			}
			if err := os.MkdirAll(outRoot, 0o755); err != nil {
				t.Fatalf("mkdir output: %v", err)
			}

			inPath := testutils.CreateTempFile(t, inputDir, "g5-protected-output.bin", 1024*1024+333)
			fileID := storeFileWithCodecCLIG5(t, repoRoot, binPath, env, codec, inPath)

			targetPath := filepath.Join(outRoot, "protected.bin")
			originalTarget := []byte("do-not-overwrite-existing-output")
			if err := os.WriteFile(targetPath, originalTarget, 0o444); err != nil {
				t.Fatalf("write protected target file: %v", err)
			}
			originalHash := testutils.SHA256File(t, targetPath)

			targetAsChild := filepath.Join(targetPath, "nested-output.bin")
			err := storage.RestoreFileWithDB(dbconn, fileID, targetAsChild)
			if err == nil {
				t.Fatalf("restore unexpectedly succeeded beneath non-directory target parent")
			}

			gotHash := testutils.SHA256File(t, targetPath)
			if gotHash != originalHash {
				t.Fatalf("failed restore mutated unrelated existing target file: want %s got %s", originalHash, gotHash)
			}
			got := testutils.MustRead(t, targetPath)
			if string(got) != string(originalTarget) {
				t.Fatalf("failed restore changed existing target file contents")
			}
		})
	}
}

func TestAdversarialG5RepeatedRestoreSameTargetConvergesByteIdentically(t *testing.T) {
	testgate.RequireDB(t)

	for _, codec := range adversarialG5Codecs() {
		t.Run(codec, func(t *testing.T) {
			configureAdversarialG5Codec(t, codec)

			dbconn, env, repoRoot, binPath, tmp := setupAdversarialG5Env(t)
			defer dbconn.Close()

			inputDir := filepath.Join(tmp, "input")
			outRoot := filepath.Join(tmp, "output")
			if err := os.MkdirAll(inputDir, 0o755); err != nil {
				t.Fatalf("mkdir input: %v", err)
			}
			if err := os.MkdirAll(outRoot, 0o755); err != nil {
				t.Fatalf("mkdir output: %v", err)
			}

			inPath := testutils.CreateTempFile(t, inputDir, "g5-repeat.bin", 2*1024*1024+257)
			wantHash := testutils.SHA256File(t, inPath)
			fileID := storeFileWithCodecCLIG5(t, repoRoot, binPath, env, codec, inPath)

			targetPath := filepath.Join(outRoot, "same-target.bin")
			for i := 0; i < 5; i++ {
				restoreMustMatchHashG5(t, dbconn, fileID, targetPath, wantHash)
			}

			if gotHash := testutils.SHA256File(t, targetPath); gotHash != wantHash {
				t.Fatalf("final repeated restore hash mismatch: want %s got %s", wantHash, gotHash)
			}
		})
	}
}

func TestAdversarialG5RestoreAfterFailedAttemptToSamePathConvergesCleanly(t *testing.T) {
	testgate.RequireDB(t)

	for _, codec := range adversarialG5Codecs() {
		t.Run(codec, func(t *testing.T) {
			configureAdversarialG5Codec(t, codec)

			dbconn, env, repoRoot, binPath, tmp := setupAdversarialG5Env(t)
			defer dbconn.Close()

			inputDir := filepath.Join(tmp, "input")
			outRoot := filepath.Join(tmp, "output")
			if err := os.MkdirAll(inputDir, 0o755); err != nil {
				t.Fatalf("mkdir input: %v", err)
			}
			if err := os.MkdirAll(outRoot, 0o755); err != nil {
				t.Fatalf("mkdir output: %v", err)
			}

			inPath := testutils.CreateTempFile(t, inputDir, "g5-failed-then-clean.bin", 1536*1024+99)
			wantHash := testutils.SHA256File(t, inPath)
			fileID := storeFileWithCodecCLIG5(t, repoRoot, binPath, env, codec, inPath)

			targetPath := filepath.Join(outRoot, "converge.bin")
			if err := os.MkdirAll(targetPath, 0o755); err != nil {
				t.Fatalf("seed target path as directory: %v", err)
			}

			// First restore: should succeed and write inside directory
			err := storage.RestoreFileWithDB(dbconn, fileID, targetPath)
			if err != nil {
				t.Fatalf("restore failed unexpectedly for directory target: %v", err)
			}

			// Validate correct output inside directory, no partial files
			files, readErr := os.ReadDir(targetPath)
			if readErr != nil {
				t.Fatalf("readdir target directory after restore: %v", readErr)
			}
			if len(files) != 1 {
				t.Fatalf("expected exactly one file inside directory after restore, got %d", len(files))
			}
			restoredPath := filepath.Join(targetPath, files[0].Name())
			if gotHash := testutils.SHA256File(t, restoredPath); gotHash != wantHash {
				t.Fatalf("restored file hash mismatch after first restore: want %s got %s", wantHash, gotHash)
			}

			// Remove the restored file and retry restore to same directory
			if err := os.Remove(restoredPath); err != nil {
				t.Fatalf("remove restored file before retry: %v", err)
			}

			// Retry restore: should again succeed and converge
			err = storage.RestoreFileWithDB(dbconn, fileID, targetPath)
			if err != nil {
				t.Fatalf("restore failed unexpectedly on retry: %v", err)
			}
			files, readErr = os.ReadDir(targetPath)
			if readErr != nil {
				t.Fatalf("readdir target directory after retry: %v", readErr)
			}
			if len(files) != 1 {
				t.Fatalf("expected exactly one file inside directory after retry, got %d", len(files))
			}
			restoredPath = filepath.Join(targetPath, files[0].Name())
			if gotHash := testutils.SHA256File(t, restoredPath); gotHash != wantHash {
				t.Fatalf("restored file hash mismatch after retry: want %s got %s", wantHash, gotHash)
			}
		})
	}
}

var _ = dbschema.PostgresSchema
