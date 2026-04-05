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
	filestate "github.com/franchoy/coldkeep/internal/status"
	"github.com/franchoy/coldkeep/internal/storage"
	"github.com/franchoy/coldkeep/internal/verify"
	testutils "github.com/franchoy/coldkeep/tests/utils"
	"github.com/franchoy/coldkeep/tests/utils/testgate"
)

// G4 — Non-destructive garbage collection
//
// Adversarial goals:
//   - GC must never reclaim data that is still reachable through either
//     live_ref_count OR pin_count.
//   - remove + GC sequences must not bypass pin protection.
//   - once the last protection is released, GC should converge and reclaim the
//     now-unreachable data.
//   - repeated GC over mixed live/dead state must preserve healthy live restore.
//
// Notes:
//   - This file keeps the current Postgres-backed adversarial harness.
//   - The pin/liveness tests assert semantic outcomes rather than lock timing.
//   - The live-data tests run the plain + aes-gcm codec matrix.

func adversarialG4Codecs() []string {
	return []string{"plain", "aes-gcm"}
}

func configureAdversarialG4Codec(t *testing.T, codec string) {
	t.Helper()
	if codec == "aes-gcm" {
		testutils.SetTestAESGCMKey(t)
	}
}

func setupAdversarialG4Env(t *testing.T) (*sql.DB, map[string]string, string, string, string) {
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

func storeFileWithCodecCLIG4(t *testing.T, repoRoot, binPath string, env map[string]string, codec, path string) int64 {
	t.Helper()

	payload := testutils.AssertCLIJSONOK(
		t,
		testutils.RunColdkeepCommand(t, repoRoot, binPath, env, "store", "--codec", codec, path, "--output", "json"),
		"store",
	)
	data := testutils.JSONMap(t, payload, "data")
	return testutils.JSONInt64(t, data, "file_id")
}

func restoreMustMatchHashG4(t *testing.T, dbconn *sql.DB, fileID int64, outPath, wantHash string) {
	t.Helper()

	if err := storage.RestoreFileWithDB(dbconn, fileID, outPath); err != nil {
		t.Fatalf("restore file %d: %v", fileID, err)
	}
	if gotHash := testutils.SHA256File(t, outPath); gotHash != wantHash {
		t.Fatalf("restored hash mismatch: want %s got %s", wantHash, gotHash)
	}
}

func countInt64QueryG4(t *testing.T, dbconn *sql.DB, query string, args ...any) int64 {
	t.Helper()

	var n int64
	if err := dbconn.QueryRow(query, args...).Scan(&n); err != nil {
		t.Fatalf("count query failed (%s): %v", query, err)
	}
	return n
}

func TestAdversarialG4PinnedChunkWithZeroLiveRefsSurvivesGC(t *testing.T) {
	testgate.RequireDB(t)

	dbconn, _, _, _, _ := setupAdversarialG4Env(t)
	defer dbconn.Close()

	containerFile := "g4-pinned-zero-live.bin"
	containerPath := filepath.Join(container.ContainersDir, containerFile)
	if err := os.WriteFile(containerPath, []byte("g4-pinned-zero-live-payload"), 0o644); err != nil {
		t.Fatalf("write container file: %v", err)
	}

	var containerID int64
	if err := dbconn.QueryRow(
		`INSERT INTO container (filename, current_size, max_size, sealed, quarantine)
		 VALUES ($1, $2, $3, TRUE, FALSE)
		 RETURNING id`,
		containerFile,
		int64(len("g4-pinned-zero-live-payload")),
		container.GetContainerMaxSize(),
	).Scan(&containerID); err != nil {
		t.Fatalf("insert container: %v", err)
	}

	var chunkID int64
	if err := dbconn.QueryRow(
		`INSERT INTO chunk (chunk_hash, size, status, live_ref_count, pin_count)
		 VALUES ($1, $2, $3, $4, $5)
		 RETURNING id`,
		"g4-pinned-zero-live-chunk",
		int64(27),
		filestate.ChunkCompleted,
		int64(0),
		int64(1),
	).Scan(&chunkID); err != nil {
		t.Fatalf("insert chunk: %v", err)
	}

	if _, err := dbconn.Exec(
		`INSERT INTO blocks (chunk_id, codec, format_version, plaintext_size, stored_size, nonce, container_id, block_offset)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
		chunkID,
		"plain",
		1,
		int64(27),
		int64(27),
		[]byte{},
		containerID,
		int64(0),
	); err != nil {
		t.Fatalf("insert block: %v", err)
	}

	if _, err := maintenance.RunGCWithContainersDirResult(false, container.ContainersDir); err != nil {
		t.Fatalf("gc with pin_count=1: %v", err)
	}

	if got := countInt64QueryG4(t, dbconn, `SELECT COUNT(*) FROM container WHERE id = $1`, containerID); got != 1 {
		t.Fatalf("expected container to survive gc while pin_count=1, got %d", got)
	}
	if got := countInt64QueryG4(t, dbconn, `SELECT COUNT(*) FROM chunk WHERE id = $1`, chunkID); got != 1 {
		t.Fatalf("expected chunk to survive gc while pin_count=1, got %d", got)
	}

	if _, err := dbconn.Exec(`UPDATE chunk SET pin_count = 0 WHERE id = $1`, chunkID); err != nil {
		t.Fatalf("clear pin_count: %v", err)
	}

	if _, err := maintenance.RunGCWithContainersDirResult(false, container.ContainersDir); err != nil {
		t.Fatalf("gc after clearing pin_count: %v", err)
	}

	if got := countInt64QueryG4(t, dbconn, `SELECT COUNT(*) FROM container WHERE id = $1`, containerID); got != 0 {
		t.Fatalf("expected container to be collected after pin_count cleared, got %d", got)
	}
	if got := countInt64QueryG4(t, dbconn, `SELECT COUNT(*) FROM chunk WHERE id = $1`, chunkID); got != 0 {
		t.Fatalf("expected chunk to be collected after pin_count cleared, got %d", got)
	}
}

func TestAdversarialG4RemoveThenGCRespectsCommittedPinProtection(t *testing.T) {
	testgate.RequireDB(t)

	for _, codec := range adversarialG4Codecs() {
		t.Run(codec, func(t *testing.T) {
			configureAdversarialG4Codec(t, codec)

			dbconn, env, repoRoot, binPath, tmp := setupAdversarialG4Env(t)
			defer dbconn.Close()

			inputDir := filepath.Join(tmp, "input")
			restoreDir := filepath.Join(tmp, "restore")
			if err := os.MkdirAll(inputDir, 0o755); err != nil {
				t.Fatalf("mkdir input: %v", err)
			}
			if err := os.MkdirAll(restoreDir, 0o755); err != nil {
				t.Fatalf("mkdir restore: %v", err)
			}

			inPath := testutils.CreateTempFile(t, inputDir, "g4-committed-pin.bin", 1024*1024+777)
			wantHash := testutils.SHA256File(t, inPath)
			fileID := storeFileWithCodecCLIG4(t, repoRoot, binPath, env, codec, inPath)

			record := testutils.FetchFirstFileChunkRecord(t, dbconn, fileID)

			if _, err := dbconn.Exec(`UPDATE chunk SET pin_count = pin_count + 1 WHERE id = $1`, record.ChunkID); err != nil {
				t.Fatalf("pin first chunk: %v", err)
			}

			if err := storage.RemoveFileWithDB(dbconn, fileID); err != nil {
				t.Fatalf("remove file with committed pin: %v", err)
			}

			if _, err := maintenance.RunGCWithContainersDirResult(false, container.ContainersDir); err != nil {
				t.Fatalf("gc with committed pin: %v", err)
			}

			if got := countInt64QueryG4(t, dbconn, `SELECT COUNT(*) FROM chunk WHERE id = $1`, record.ChunkID); got != 1 {
				t.Fatalf("expected pinned chunk to survive gc, got %d", got)
			}
			if got := countInt64QueryG4(t, dbconn, `SELECT pin_count FROM chunk WHERE id = $1`, record.ChunkID); got != 1 {
				t.Fatalf("expected pin_count=1 after gc, got %d", got)
			}

			if _, err := dbconn.Exec(`UPDATE chunk SET pin_count = pin_count - 1 WHERE id = $1 AND pin_count > 0`, record.ChunkID); err != nil {
				t.Fatalf("unpin first chunk: %v", err)
			}

			if _, err := maintenance.RunGCWithContainersDirResult(false, container.ContainersDir); err != nil {
				t.Fatalf("final gc after unpin: %v", err)
			}

			testutils.AssertNoProcessingRows(t, dbconn)

			outPath := filepath.Join(restoreDir, "g4-committed-pin.restored.bin")
			if err := storage.RestoreFileWithDB(dbconn, fileID, outPath); err == nil {
				t.Fatalf("removed file unexpectedly restored successfully after final gc")
			}

			var completedForHash int64
			if err := dbconn.QueryRow(`SELECT COUNT(*) FROM logical_file WHERE file_hash = $1 AND status = 'COMPLETED'`, wantHash).Scan(&completedForHash); err != nil {
				t.Fatalf("count completed logical_file rows for removed file: %v", err)
			}
			if completedForHash != 0 {
				t.Fatalf("expected no COMPLETED logical_file rows for removed file hash, got %d", completedForHash)
			}
		})
	}
}

func TestAdversarialG4RepeatedGCMixedLiveDeadStatePreservesHealthyRestore(t *testing.T) {
	testgate.RequireDB(t)

	for _, codec := range adversarialG4Codecs() {
		t.Run(codec, func(t *testing.T) {
			configureAdversarialG4Codec(t, codec)

			dbconn, env, repoRoot, binPath, tmp := setupAdversarialG4Env(t)
			defer dbconn.Close()

			inputDir := filepath.Join(tmp, "input")
			restoreDir := filepath.Join(tmp, "restore")
			if err := os.MkdirAll(inputDir, 0o755); err != nil {
				t.Fatalf("mkdir input: %v", err)
			}
			if err := os.MkdirAll(restoreDir, 0o755); err != nil {
				t.Fatalf("mkdir restore: %v", err)
			}

			anchorPath := testutils.CreateTempFile(t, inputDir, "g4-anchor.bin", 512*1024)
			anchorHash := testutils.SHA256File(t, anchorPath)
			anchorID := storeFileWithCodecCLIG4(t, repoRoot, binPath, env, codec, anchorPath)

			victimPath := filepath.Join(inputDir, "g4-victim.bin")
			victimData := make([]byte, 768*1024)
			for i := range victimData {
				victimData[i] = byte((i*19 + 5) % 251)
			}
			if err := os.WriteFile(victimPath, victimData, 0o644); err != nil {
				t.Fatalf("write victim file: %v", err)
			}
			victimID := storeFileWithCodecCLIG4(t, repoRoot, binPath, env, codec, victimPath)

			if err := storage.RemoveFileWithDB(dbconn, victimID); err != nil {
				t.Fatalf("remove victim: %v", err)
			}

			for i := 0; i < 4; i++ {
				if _, err := maintenance.RunGCWithContainersDirResult(false, container.ContainersDir); err != nil {
					t.Fatalf("gc run %d: %v", i, err)
				}
				if err := maintenance.VerifyCommandWithContainersDir(container.ContainersDir, "system", 0, verify.VerifyFull); err != nil {
					t.Fatalf("verify full after gc run %d: %v", i, err)
				}
				testutils.AssertNoProcessingRows(t, dbconn)

				outAnchor := filepath.Join(restoreDir, fmt.Sprintf("g4-anchor-%d.restored.bin", i))
				restoreMustMatchHashG4(t, dbconn, anchorID, outAnchor, anchorHash)
			}
		})
	}
}

func TestAdversarialG4RemoveOneLiveFileThenRepeatedGCPreservesOtherLiveFile(t *testing.T) {
	testgate.RequireDB(t)
	testgate.RequireLongRun(t)

	for _, codec := range adversarialG4Codecs() {
		t.Run(codec, func(t *testing.T) {
			configureAdversarialG4Codec(t, codec)

			dbconn, env, repoRoot, binPath, tmp := setupAdversarialG4Env(t)
			defer dbconn.Close()

			inputDir := filepath.Join(tmp, "input")
			restoreDir := filepath.Join(tmp, "restore")
			if err := os.MkdirAll(inputDir, 0o755); err != nil {
				t.Fatalf("mkdir input: %v", err)
			}
			if err := os.MkdirAll(restoreDir, 0o755); err != nil {
				t.Fatalf("mkdir restore: %v", err)
			}

			firstPath := testutils.CreateTempFile(t, inputDir, "g4-first.bin", 1024*1024+555)
			firstHash := testutils.SHA256File(t, firstPath)
			firstID := storeFileWithCodecCLIG4(t, repoRoot, binPath, env, codec, firstPath)

			secondPath := filepath.Join(inputDir, "g4-second.bin")
			secondData := make([]byte, 1024*1024+777)
			for i := range secondData {
				secondData[i] = byte((i*23 + 9) % 251)
			}
			if err := os.WriteFile(secondPath, secondData, 0o644); err != nil {
				t.Fatalf("write second file: %v", err)
			}
			secondHash := testutils.SHA256File(t, secondPath)
			secondID := storeFileWithCodecCLIG4(t, repoRoot, binPath, env, codec, secondPath)

			if err := storage.RemoveFileWithDB(dbconn, firstID); err != nil {
				t.Fatalf("remove first live file: %v", err)
			}

			for i := 0; i < 3; i++ {
				if _, err := maintenance.RunGCWithContainersDirResult(false, container.ContainersDir); err != nil {
					t.Fatalf("gc run %d: %v", i, err)
				}
				if err := maintenance.VerifyCommandWithContainersDir(container.ContainersDir, "system", 0, verify.VerifyFull); err != nil {
					t.Fatalf("verify full after gc run %d: %v", i, err)
				}

				outSecond := filepath.Join(restoreDir, fmt.Sprintf("g4-second-%d.restored.bin", i))
				restoreMustMatchHashG4(t, dbconn, secondID, outSecond, secondHash)
			}

			outFirst := filepath.Join(restoreDir, "g4-first.restored.bin")
			if err := storage.RestoreFileWithDB(dbconn, firstID, outFirst); err == nil {
				t.Fatalf("removed first file unexpectedly restored successfully after repeated gc")
			}

			var completedFirst int64
			if err := dbconn.QueryRow(`SELECT COUNT(*) FROM logical_file WHERE file_hash = $1 AND status = 'COMPLETED'`, firstHash).Scan(&completedFirst); err != nil {
				t.Fatalf("count completed logical_file rows for first file: %v", err)
			}
			if completedFirst != 0 {
				t.Fatalf("expected no COMPLETED logical_file rows for removed first file hash, got %d", completedFirst)
			}
		})
	}
}

var _ = dbschema.PostgresSchema
