package main

import (
	"database/sql"
	"fmt"
	"math/rand"
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

// G3 — No data loss under valid operation sequences
//
// Adversarial goals:
//   - valid lifecycle sequences must never lose still-reachable data
//   - GC must never reclaim data required by surviving logical files
//   - repeated GC must converge and remain stable
//   - removing and re-storing content must not poison future restores
//
// Notes:
//   - This file runs a codec matrix for plain + aes-gcm.
//   - It uses the current Postgres-backed adversarial/integration harness.

func adversarialG3Codecs() []string {
	return []string{"plain", "aes-gcm"}
}

func configureAdversarialG3Codec(t *testing.T, codec string) {
	t.Helper()
	if codec == "aes-gcm" {
		testutils.SetTestAESGCMKey(t)
	}
}

func setupAdversarialG3Env(t *testing.T) (*sql.DB, map[string]string, string, string, string) {
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

func storeFileWithCodecCLIG3(t *testing.T, repoRoot, binPath string, env map[string]string, codec, path string) int64 {
	t.Helper()

	payload := testutils.AssertCLIJSONOK(
		t,
		testutils.RunColdkeepCommand(t, repoRoot, binPath, env, "store", "--codec", codec, path, "--output", "json"),
		"store",
	)
	data := testutils.JSONMap(t, payload, "data")
	return testutils.JSONInt64(t, data, "file_id")
}

func restoreMustMatchHashG3(t *testing.T, dbconn *sql.DB, fileID int64, outPath, wantHash string) {
	t.Helper()

	if err := storage.RestoreFileWithDB(dbconn, fileID, outPath); err != nil {
		t.Fatalf("restore file %d: %v", fileID, err)
	}
	if gotHash := testutils.SHA256File(t, outPath); gotHash != wantHash {
		t.Fatalf("restored hash mismatch: want %s got %s", wantHash, gotHash)
	}
}

func countInt64QueryG3(t *testing.T, dbconn *sql.DB, query string, args ...any) int64 {
	t.Helper()

	var n int64
	if err := dbconn.QueryRow(query, args...).Scan(&n); err != nil {
		t.Fatalf("count query failed (%s): %v", query, err)
	}
	return n
}

func TestAdversarialG3RemoveOneSharedChunkFileThenGCPreservesOther(t *testing.T) {
	testgate.RequireDB(t)

	for _, codec := range adversarialG3Codecs() {
		t.Run(codec, func(t *testing.T) {
			configureAdversarialG3Codec(t, codec)

			dbconn, env, repoRoot, binPath, tmp := setupAdversarialG3Env(t)
			defer dbconn.Close()

			inputDir := filepath.Join(tmp, "input")
			restoreDir := filepath.Join(tmp, "restore")
			if err := os.MkdirAll(inputDir, 0o755); err != nil {
				t.Fatalf("mkdir input: %v", err)
			}
			if err := os.MkdirAll(restoreDir, 0o755); err != nil {
				t.Fatalf("mkdir restore: %v", err)
			}

			paths := testutils.CreateSampleDataset(t, inputDir)
			hybridA := paths["hybrid_a.bin"]
			hybridB := paths["hybrid_b.bin"]

			hashA := testutils.SHA256File(t, hybridA)
			hashB := testutils.SHA256File(t, hybridB)

			fileAID := storeFileWithCodecCLIG3(t, repoRoot, binPath, env, codec, hybridA)
			fileBID := storeFileWithCodecCLIG3(t, repoRoot, binPath, env, codec, hybridB)

			if err := storage.RemoveFileWithDB(dbconn, fileAID); err != nil {
				t.Fatalf("remove hybrid_a: %v", err)
			}

			if _, err := maintenance.RunGCWithContainersDirResult(true, container.ContainersDir); err != nil {
				t.Fatalf("gc dry-run: %v", err)
			}
			if _, err := maintenance.RunGCWithContainersDirResult(false, container.ContainersDir); err != nil {
				t.Fatalf("gc real: %v", err)
			}

			if err := maintenance.VerifyCommandWithContainersDir(container.ContainersDir, "system", 0, verify.VerifyFull); err != nil {
				t.Fatalf("verify full after remove+gc: %v", err)
			}
			testutils.AssertNoProcessingRows(t, dbconn)

			outB := filepath.Join(restoreDir, "hybrid_b.restored.bin")
			restoreMustMatchHashG3(t, dbconn, fileBID, outB, hashB)

			outA := filepath.Join(restoreDir, "hybrid_a.restored.bin")
			if err := storage.RestoreFileWithDB(dbconn, fileAID, outA); err == nil {
				t.Fatalf("removed file A unexpectedly restored successfully after GC")
			}

			var countACompleted int64
			if err := dbconn.QueryRow(`SELECT COUNT(*) FROM logical_file WHERE file_hash = $1 AND status = 'COMPLETED'`, hashA).Scan(&countACompleted); err != nil {
				t.Fatalf("count completed logical_file rows for removed file: %v", err)
			}
			if countACompleted != 0 {
				t.Fatalf("expected no COMPLETED logical_file rows for removed file hash after remove+gc, got %d", countACompleted)
			}
		})
	}
}

func TestAdversarialG3RepeatedGCConvergesAndPreservesReachableData(t *testing.T) {
	testgate.RequireDB(t)

	for _, codec := range adversarialG3Codecs() {
		t.Run(codec, func(t *testing.T) {
			configureAdversarialG3Codec(t, codec)

			dbconn, env, repoRoot, binPath, tmp := setupAdversarialG3Env(t)
			defer dbconn.Close()

			inputDir := filepath.Join(tmp, "input")
			restoreDir := filepath.Join(tmp, "restore")
			if err := os.MkdirAll(inputDir, 0o755); err != nil {
				t.Fatalf("mkdir input: %v", err)
			}
			if err := os.MkdirAll(restoreDir, 0o755); err != nil {
				t.Fatalf("mkdir restore: %v", err)
			}

			anchorPath := testutils.CreateTempFile(t, inputDir, "g3-anchor.bin", 512*1024)
			anchorHash := testutils.SHA256File(t, anchorPath)
			anchorID := storeFileWithCodecCLIG3(t, repoRoot, binPath, env, codec, anchorPath)

			victimPath := filepath.Join(inputDir, "g3-victim.bin")
			victimData := make([]byte, 512*1024)
			for i := range victimData {
				victimData[i] = byte((i*17 + 11) % 251)
			}
			if err := os.WriteFile(victimPath, victimData, 0o644); err != nil {
				t.Fatalf("write victim file: %v", err)
			}
			victimID := storeFileWithCodecCLIG3(t, repoRoot, binPath, env, codec, victimPath)

			if err := storage.RemoveFileWithDB(dbconn, victimID); err != nil {
				t.Fatalf("remove victim: %v", err)
			}

			if _, err := maintenance.RunGCWithContainersDirResult(false, container.ContainersDir); err != nil {
				t.Fatalf("first gc: %v", err)
			}

			containersAfterFirst := countInt64QueryG3(t, dbconn, `SELECT COUNT(*) FROM container`)
			chunksAfterFirst := countInt64QueryG3(t, dbconn, `SELECT COUNT(*) FROM chunk`)
			liveCompletedAfterFirst := countInt64QueryG3(t, dbconn, `SELECT COUNT(*) FROM logical_file WHERE status = 'COMPLETED'`)

			if _, err := maintenance.RunGCWithContainersDirResult(false, container.ContainersDir); err != nil {
				t.Fatalf("second gc: %v", err)
			}

			containersAfterSecond := countInt64QueryG3(t, dbconn, `SELECT COUNT(*) FROM container`)
			chunksAfterSecond := countInt64QueryG3(t, dbconn, `SELECT COUNT(*) FROM chunk`)
			liveCompletedAfterSecond := countInt64QueryG3(t, dbconn, `SELECT COUNT(*) FROM logical_file WHERE status = 'COMPLETED'`)

			if containersAfterFirst != containersAfterSecond {
				t.Fatalf("repeated gc did not converge on container count: first=%d second=%d", containersAfterFirst, containersAfterSecond)
			}
			if chunksAfterFirst != chunksAfterSecond {
				t.Fatalf("repeated gc did not converge on chunk count: first=%d second=%d", chunksAfterFirst, chunksAfterSecond)
			}
			if liveCompletedAfterFirst != liveCompletedAfterSecond {
				t.Fatalf("repeated gc changed completed logical file count: first=%d second=%d", liveCompletedAfterFirst, liveCompletedAfterSecond)
			}

			if err := maintenance.VerifyCommandWithContainersDir(container.ContainersDir, "system", 0, verify.VerifyFull); err != nil {
				t.Fatalf("verify full after repeated gc: %v", err)
			}
			testutils.AssertNoProcessingRows(t, dbconn)

			outAnchor := filepath.Join(restoreDir, "g3-anchor.restored.bin")
			restoreMustMatchHashG3(t, dbconn, anchorID, outAnchor, anchorHash)
		})
	}
}

func TestAdversarialG3StoreDeleteStoreSameContentAfterGCStillRestores(t *testing.T) {
	testgate.RequireDB(t)

	for _, codec := range adversarialG3Codecs() {
		t.Run(codec, func(t *testing.T) {
			configureAdversarialG3Codec(t, codec)

			dbconn, env, repoRoot, binPath, tmp := setupAdversarialG3Env(t)
			defer dbconn.Close()

			inputDir := filepath.Join(tmp, "input")
			restoreDir := filepath.Join(tmp, "restore")
			if err := os.MkdirAll(inputDir, 0o755); err != nil {
				t.Fatalf("mkdir input: %v", err)
			}
			if err := os.MkdirAll(restoreDir, 0o755); err != nil {
				t.Fatalf("mkdir restore: %v", err)
			}

			inPath := testutils.CreateTempFile(t, inputDir, "g3-restore-after-delete.bin", 640*1024)
			wantHash := testutils.SHA256File(t, inPath)

			firstID := storeFileWithCodecCLIG3(t, repoRoot, binPath, env, codec, inPath)

			if err := storage.RemoveFileWithDB(dbconn, firstID); err != nil {
				t.Fatalf("remove first logical file: %v", err)
			}
			if _, err := maintenance.RunGCWithContainersDirResult(false, container.ContainersDir); err != nil {
				t.Fatalf("gc after first remove: %v", err)
			}

			secondID := storeFileWithCodecCLIG3(t, repoRoot, binPath, env, codec, inPath)

			if err := maintenance.VerifyCommandWithContainersDir(container.ContainersDir, "system", 0, verify.VerifyFull); err != nil {
				t.Fatalf("verify full after re-store: %v", err)
			}
			testutils.AssertNoProcessingRows(t, dbconn)

			outPath := filepath.Join(restoreDir, "g3-restore-after-delete.restored.bin")
			restoreMustMatchHashG3(t, dbconn, secondID, outPath, wantHash)

			var completedForHash int64
			if err := dbconn.QueryRow(`SELECT COUNT(*) FROM logical_file WHERE file_hash = $1 AND status = 'COMPLETED'`, wantHash).Scan(&completedForHash); err != nil {
				t.Fatalf("count completed logical_file rows for hash: %v", err)
			}
			if completedForHash != 1 {
				t.Fatalf("expected exactly 1 COMPLETED logical_file row for hash after re-store, got %d", completedForHash)
			}
		})
	}
}

func TestAdversarialG3SeededValidLifecycleSequencePreservesReachableFiles(t *testing.T) {
	testgate.RequireDB(t)
	testgate.RequireLongRun(t)

	for _, codec := range adversarialG3Codecs() {
		t.Run(codec, func(t *testing.T) {
			configureAdversarialG3Codec(t, codec)

			dbconn, env, repoRoot, binPath, tmp := setupAdversarialG3Env(t)
			defer dbconn.Close()

			inputDir := filepath.Join(tmp, "input")
			restoreDir := filepath.Join(tmp, "restore")
			if err := os.MkdirAll(inputDir, 0o755); err != nil {
				t.Fatalf("mkdir input: %v", err)
			}
			if err := os.MkdirAll(restoreDir, 0o755); err != nil {
				t.Fatalf("mkdir restore: %v", err)
			}

			type trackedFile struct {
				Path   string
				Hash   string
				FileID int64
				Alive  bool
			}

			files := []*trackedFile{
				{Path: testutils.CreateTempFile(t, inputDir, "g3-seq-a.bin", 256*1024)},
				{Path: testutils.CreateTempFile(t, inputDir, "g3-seq-b.bin", 384*1024)},
				{Path: testutils.CreateTempFile(t, inputDir, "g3-seq-c.bin", 768*1024)},
				{Path: testutils.CreateTempFile(t, inputDir, "g3-seq-d.bin", 1024*1024+333)},
			}

			for _, f := range files {
				f.Hash = testutils.SHA256File(t, f.Path)
				f.FileID = storeFileWithCodecCLIG3(t, repoRoot, binPath, env, codec, f.Path)
				f.Alive = true
			}

			rng := rand.New(rand.NewSource(20260404))
			const rounds = 24

			for round := 0; round < rounds; round++ {
				op := rng.Intn(4)

				switch op {
				case 0: // restore a live file
					live := make([]*trackedFile, 0, len(files))
					for _, f := range files {
						if f.Alive {
							live = append(live, f)
						}
					}
					if len(live) == 0 {
						continue
					}
					chosen := live[rng.Intn(len(live))]
					outPath := filepath.Join(restoreDir, fmt.Sprintf("round-%02d-%s", round, filepath.Base(chosen.Path)))
					restoreMustMatchHashG3(t, dbconn, chosen.FileID, outPath, chosen.Hash)

				case 1: // remove a live file, but never remove the last one
					live := make([]*trackedFile, 0, len(files))
					for _, f := range files {
						if f.Alive {
							live = append(live, f)
						}
					}
					if len(live) <= 1 {
						continue
					}
					chosen := live[rng.Intn(len(live))]
					if err := storage.RemoveFileWithDB(dbconn, chosen.FileID); err != nil {
						t.Fatalf("round %d remove %s: %v", round, filepath.Base(chosen.Path), err)
					}
					chosen.Alive = false

				case 2: // re-store a removed file
					removed := make([]*trackedFile, 0, len(files))
					for _, f := range files {
						if !f.Alive {
							removed = append(removed, f)
						}
					}
					if len(removed) == 0 {
						continue
					}
					chosen := removed[rng.Intn(len(removed))]
					chosen.FileID = storeFileWithCodecCLIG3(t, repoRoot, binPath, env, codec, chosen.Path)
					chosen.Alive = true

				case 3: // gc
					if _, err := maintenance.RunGCWithContainersDirResult(false, container.ContainersDir); err != nil {
						t.Fatalf("round %d gc: %v", round, err)
					}
				}

				if round%4 == 3 {
					if err := maintenance.VerifyCommandWithContainersDir(container.ContainersDir, "system", 0, verify.VerifyFull); err != nil {
						t.Fatalf("round %d verify full: %v", round, err)
					}
					testutils.AssertNoProcessingRows(t, dbconn)
				}
			}

			for _, f := range files {
				if !f.Alive {
					continue
				}
				outPath := filepath.Join(restoreDir, "final-"+filepath.Base(f.Path))
				restoreMustMatchHashG3(t, dbconn, f.FileID, outPath, f.Hash)
			}

			if err := maintenance.VerifyCommandWithContainersDir(container.ContainersDir, "system", 0, verify.VerifyFull); err != nil {
				t.Fatalf("final verify full: %v", err)
			}
			testutils.AssertNoProcessingRows(t, dbconn)
		})
	}
}

var _ = dbschema.PostgresSchema
