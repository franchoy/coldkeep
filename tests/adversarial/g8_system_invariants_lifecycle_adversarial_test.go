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
	"github.com/franchoy/coldkeep/internal/recovery"
	"github.com/franchoy/coldkeep/internal/storage"
	"github.com/franchoy/coldkeep/internal/verify"
	testutils "github.com/franchoy/coldkeep/tests/utils"
	"github.com/franchoy/coldkeep/tests/utils/testgate"
)

// G8 — System invariants under adversarial lifecycle
//
// Adversarial goals:
//   - the system must not drift into an invalid state under long valid operation sequences
//   - restart/recovery between operations must converge safely
//   - repeated store/restore/remove/gc/verify/doctor cycles must preserve invariants
//   - surviving live files must always remain restorable byte-identically
//
// Notes:
//   - This file uses the current Postgres-backed adversarial harness.
//   - It intentionally validates invariant convergence after mixed lifecycle steps.
//   - Concurrency-specific guarantees are already exercised by G6; G8 focuses on
//     whole-system lifecycle/state-machine correctness.

func adversarialG8Codecs() []string {
	return []string{"plain", "aes-gcm"}
}

func configureAdversarialG8Codec(t *testing.T, codec string) {
	t.Helper()
	if codec == "aes-gcm" {
		testutils.SetTestAESGCMKey(t)
	}
}

func setupAdversarialG8Env(t *testing.T) (*sql.DB, map[string]string, string, string, string) {
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

func storeFileWithCodecCLIG8(t *testing.T, repoRoot, binPath string, env map[string]string, codec, path string) int64 {
	t.Helper()

	payload := testutils.AssertCLIJSONOK(
		t,
		testutils.RunColdkeepCommand(t, repoRoot, binPath, env, "store", "--codec", codec, path, "--output", "json"),
		"store",
	)
	data := testutils.JSONMap(t, payload, "data")
	return testutils.JSONInt64(t, data, "file_id")
}

func restoreMustMatchHashG8(t *testing.T, dbconn *sql.DB, fileID int64, outPath, wantHash string) {
	t.Helper()

	if err := storage.RestoreFileWithDB(dbconn, fileID, outPath); err != nil {
		t.Fatalf("restore file %d: %v", fileID, err)
	}
	if gotHash := testutils.SHA256File(t, outPath); gotHash != wantHash {
		t.Fatalf("restored hash mismatch: want %s got %s", wantHash, gotHash)
	}
}

func countInt64QueryG8(t *testing.T, dbconn *sql.DB, query string, args ...any) int64 {
	t.Helper()

	var n int64
	if err := dbconn.QueryRow(query, args...).Scan(&n); err != nil {
		t.Fatalf("count query failed (%s): %v", query, err)
	}
	return n
}

func verifyInvariantConvergenceG8(t *testing.T, dbconn *sql.DB, repoRoot, binPath string, env map[string]string, runDoctor bool) {
	t.Helper()

	if runDoctor {
		doctorPayload := testutils.AssertCLIJSONOK(
			t,
			testutils.RunColdkeepCommand(t, repoRoot, binPath, env, "doctor", "--output", "json"),
			"doctor",
		)
		doctorData := testutils.JSONMap(t, doctorPayload, "data")
		if verifyStatus, _ := doctorData["verify_status"].(string); verifyStatus != "ok" {
			t.Fatalf("doctor verify_status not ok: payload=%v", doctorPayload)
		}
	}

	if err := maintenance.VerifyCommandWithContainersDir(container.ContainersDir, "system", 0, verify.VerifyFull); err != nil {
		t.Fatalf("verify full: %v", err)
	}
	testutils.AssertNoProcessingRows(t, dbconn)
	testutils.AssertUniqueFileChunkOrders(t, dbconn)

	if got := countInt64QueryG8(t, dbconn, `SELECT COUNT(*) FROM chunk WHERE live_ref_count < 0`); got != 0 {
		t.Fatalf("expected no negative live_ref_count rows, got %d", got)
	}
	if got := countInt64QueryG8(t, dbconn, `SELECT COUNT(*) FROM chunk WHERE pin_count < 0`); got != 0 {
		t.Fatalf("expected no negative pin_count rows, got %d", got)
	}
	if got := countInt64QueryG8(t, dbconn, `SELECT COUNT(*) FROM logical_file WHERE ref_count < 0`); got != 0 {
		t.Fatalf("expected no negative logical_file.ref_count rows, got %d", got)
	}
	if got := countInt64QueryG8(t, dbconn, `
		SELECT COUNT(*)
		FROM chunk c
		LEFT JOIN blocks b ON b.chunk_id = c.id
		WHERE c.status = 'COMPLETED' AND b.chunk_id IS NULL
	`); got != 0 {
		t.Fatalf("expected no COMPLETED chunk without blocks row, got %d", got)
	}
	if got := countInt64QueryG8(t, dbconn, `
		SELECT COUNT(*)
		FROM chunk c
		WHERE c.live_ref_count > 0
		  AND NOT EXISTS (SELECT 1 FROM file_chunk fc WHERE fc.chunk_id = c.id)
	`); got != 0 {
		t.Fatalf("expected no orphan chunk with live_ref_count > 0, got %d", got)
	}
	if got := countInt64QueryG8(t, dbconn, `
		SELECT COUNT(*)
		FROM physical_file pf
		LEFT JOIN logical_file lf ON lf.id = pf.logical_file_id
		WHERE lf.id IS NULL
	`); got != 0 {
		t.Fatalf("expected no orphan physical_file rows, got %d", got)
	}
	if got := countInt64QueryG8(t, dbconn, `
		SELECT COUNT(*)
		FROM logical_file lf
		WHERE lf.ref_count <> (
			SELECT COUNT(*)
			FROM physical_file pf
			WHERE pf.logical_file_id = lf.id
		)
	`); got != 0 {
		t.Fatalf("expected no logical_file.ref_count drift vs physical_file rows, got %d", got)
	}
}

func restartAndRecoverG8(t *testing.T, dbconn *sql.DB) *sql.DB {
	t.Helper()

	if err := dbconn.Close(); err != nil {
		t.Fatalf("close db before recovery: %v", err)
	}
	if err := recovery.SystemRecoveryWithContainersDir(container.ContainersDir); err != nil {
		t.Fatalf("system recovery: %v", err)
	}
	reopened, err := db.ConnectDB()
	if err != nil {
		t.Fatalf("reconnect db after recovery: %v", err)
	}
	testutils.ApplySchema(t, reopened)
	return reopened
}

type g8TrackedFile struct {
	Path   string
	Hash   string
	FileID int64
	Alive  bool
	Name   string
}

func TestAdversarialG8SeededLifecycleChaosMaintainsSystemInvariants(t *testing.T) {
	testgate.RequireDB(t)
	testgate.RequireLongRun(t)

	for _, codec := range adversarialG8Codecs() {
		t.Run(codec, func(t *testing.T) {
			configureAdversarialG8Codec(t, codec)

			dbconn, env, repoRoot, binPath, tmp := setupAdversarialG8Env(t)
			defer dbconn.Close()

			inputDir := filepath.Join(tmp, "input")
			restoreDir := filepath.Join(tmp, "restore")
			if err := os.MkdirAll(inputDir, 0o755); err != nil {
				t.Fatalf("mkdir input: %v", err)
			}
			if err := os.MkdirAll(restoreDir, 0o755); err != nil {
				t.Fatalf("mkdir restore: %v", err)
			}

			files := []*g8TrackedFile{
				{Name: "g8-a.bin", Path: testutils.CreateTempFile(t, inputDir, "g8-a.bin", 256*1024)},
				{Name: "g8-b.bin", Path: testutils.CreateTempFile(t, inputDir, "g8-b.bin", 384*1024)},
				{Name: "g8-c.bin", Path: testutils.CreateTempFile(t, inputDir, "g8-c.bin", 768*1024)},
				{Name: "g8-d.bin", Path: testutils.CreateTempFile(t, inputDir, "g8-d.bin", 1024*1024+333)},
			}

			for _, f := range files {
				f.Hash = testutils.SHA256File(t, f.Path)
				f.FileID = storeFileWithCodecCLIG8(t, repoRoot, binPath, env, codec, f.Path)
				f.Alive = true
			}

			verifyInvariantConvergenceG8(t, dbconn, repoRoot, binPath, env, true)

			rng := rand.New(rand.NewSource(20260404))
			const rounds = 32

			for round := 0; round < rounds; round++ {
				op := rng.Intn(6)

				switch op {
				case 0: // restore a live file
					live := make([]*g8TrackedFile, 0, len(files))
					for _, f := range files {
						if f.Alive {
							live = append(live, f)
						}
					}
					if len(live) == 0 {
						continue
					}
					chosen := live[rng.Intn(len(live))]
					outPath := filepath.Join(restoreDir, fmt.Sprintf("round-%02d-%s", round, chosen.Name))
					restoreMustMatchHashG8(t, dbconn, chosen.FileID, outPath, chosen.Hash)

				case 1: // remove a live file, but preserve at least one survivor
					live := make([]*g8TrackedFile, 0, len(files))
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
						t.Fatalf("round %d remove %s: %v", round, chosen.Name, err)
					}
					chosen.Alive = false

				case 2: // re-store a removed file
					removed := make([]*g8TrackedFile, 0, len(files))
					for _, f := range files {
						if !f.Alive {
							removed = append(removed, f)
						}
					}
					if len(removed) == 0 {
						continue
					}
					chosen := removed[rng.Intn(len(removed))]
					chosen.FileID = storeFileWithCodecCLIG8(t, repoRoot, binPath, env, codec, chosen.Path)
					chosen.Alive = true

				case 3: // gc
					if _, err := maintenance.RunGCWithContainersDirResult(false, container.ContainersDir); err != nil {
						t.Fatalf("round %d gc: %v", round, err)
					}

				case 4: // verify + doctor convergence
					verifyInvariantConvergenceG8(t, dbconn, repoRoot, binPath, env, true)

				case 5: // restart + recovery
					dbconn = restartAndRecoverG8(t, dbconn)
				}

				if round%3 == 2 {
					verifyInvariantConvergenceG8(t, dbconn, repoRoot, binPath, env, false)
				}
			}

			for _, f := range files {
				if !f.Alive {
					continue
				}
				outPath := filepath.Join(restoreDir, "final-"+f.Name)
				restoreMustMatchHashG8(t, dbconn, f.FileID, outPath, f.Hash)
			}

			verifyInvariantConvergenceG8(t, dbconn, repoRoot, binPath, env, true)
		})
	}
}

func TestAdversarialG8RestartRecoveryBetweenLifecycleStepsConvergesCleanly(t *testing.T) {
	testgate.RequireDB(t)
	testgate.RequireLongRun(t)

	for _, codec := range adversarialG8Codecs() {
		t.Run(codec, func(t *testing.T) {
			configureAdversarialG8Codec(t, codec)

			dbconn, env, repoRoot, binPath, tmp := setupAdversarialG8Env(t)
			defer dbconn.Close()

			inputDir := filepath.Join(tmp, "input")
			restoreDir := filepath.Join(tmp, "restore")
			if err := os.MkdirAll(inputDir, 0o755); err != nil {
				t.Fatalf("mkdir input: %v", err)
			}
			if err := os.MkdirAll(restoreDir, 0o755); err != nil {
				t.Fatalf("mkdir restore: %v", err)
			}

			firstPath := testutils.CreateTempFile(t, inputDir, "g8-first.bin", 512*1024)
			firstHash := testutils.SHA256File(t, firstPath)
			firstID := storeFileWithCodecCLIG8(t, repoRoot, binPath, env, codec, firstPath)
			verifyInvariantConvergenceG8(t, dbconn, repoRoot, binPath, env, true)

			dbconn = restartAndRecoverG8(t, dbconn)
			restoreMustMatchHashG8(t, dbconn, firstID, filepath.Join(restoreDir, "first-round1.bin"), firstHash)

			secondPath := filepath.Join(inputDir, "g8-second.bin")
			secondData := make([]byte, 1024*1024+777)
			for i := range secondData {
				secondData[i] = byte((i*17 + 9) % 251)
			}
			if err := os.WriteFile(secondPath, secondData, 0o644); err != nil {
				t.Fatalf("write second file: %v", err)
			}
			secondHash := testutils.SHA256File(t, secondPath)
			secondID := storeFileWithCodecCLIG8(t, repoRoot, binPath, env, codec, secondPath)
			verifyInvariantConvergenceG8(t, dbconn, repoRoot, binPath, env, false)

			dbconn = restartAndRecoverG8(t, dbconn)
			if err := storage.RemoveFileWithDB(dbconn, firstID); err != nil {
				t.Fatalf("remove first file after recovery: %v", err)
			}
			verifyInvariantConvergenceG8(t, dbconn, repoRoot, binPath, env, false)

			dbconn = restartAndRecoverG8(t, dbconn)
			if _, err := maintenance.RunGCWithContainersDirResult(false, container.ContainersDir); err != nil {
				t.Fatalf("gc after recovery: %v", err)
			}
			verifyInvariantConvergenceG8(t, dbconn, repoRoot, binPath, env, true)

			if err := storage.RestoreFileWithDB(dbconn, firstID, filepath.Join(restoreDir, "first-after-remove.bin")); err == nil {
				t.Fatalf("removed first file unexpectedly restored successfully after gc + recovery")
			}

			restoreMustMatchHashG8(t, dbconn, secondID, filepath.Join(restoreDir, "second-final.bin"), secondHash)
		})
	}
}

var _ = dbschema.PostgresSchema
