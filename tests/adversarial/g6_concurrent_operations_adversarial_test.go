package main

import (
	"database/sql"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
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

// G6 — Safe concurrent storage operations
//
// Adversarial goals:
//   - concurrent store/remove/GC activity must not corrupt metadata or graph shape
//   - identical concurrent stores must converge on deterministic chunk graphs
//   - mixed concurrent operations must preserve healthy restores for surviving files
//   - verification invariants must remain true after each stress phase
//
// Notes:
//   - This file uses the current Postgres-backed adversarial harness.
//   - It runs a codec matrix for plain + aes-gcm where data-path behavior matters.
//   - It intentionally validates semantics after concurrency rather than imposing
//     scheduler-specific timing assumptions.

func adversarialG6Codecs() []string {
	return []string{"plain", "aes-gcm"}
}

func configureAdversarialG6Codec(t *testing.T, codec string) {
	t.Helper()
	if codec == "aes-gcm" {
		testutils.SetTestAESGCMKey(t)
	}
}

func setupAdversarialG6Env(t *testing.T) (*sql.DB, map[string]string, string, string, string) {
	t.Helper()

	tmp := t.TempDir()
	origContainersDir := container.ContainersDir
	container.ContainersDir = filepath.Join(tmp, "containers")
	t.Cleanup(func() { container.ContainersDir = origContainersDir })
	t.Setenv("COLDKEEP_STORAGE_DIR", container.ContainersDir)
	testutils.ResetStorage(t)

	env := testutils.DefaultCLIEnv(container.ContainersDir)
	for k, v := range env {
		t.Setenv(k, v)
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

func storeFileWithCodecCLIG6(t *testing.T, repoRoot, binPath string, env map[string]string, codec, path string) int64 {
	t.Helper()

	res := testutils.RunColdkeepCommand(t, repoRoot, binPath, env, "store", "--codec", codec, path, "--output", "json")

	payload := testutils.AssertCLIJSONOK(
		t,
		res,
		"store",
	)
	data := testutils.JSONMap(t, payload, "data")
	return testutils.JSONInt64(t, data, "file_id")
}

// storeFileWithCodecCLIG6Async is safe to call from goroutines because it
// returns an error instead of calling t.Fatal/t.FailNow.
func storeFileWithCodecCLIG6Async(repoRoot, binPath string, env map[string]string, codec, path string) (int64, error) {
	cmd := exec.Command(binPath, "store", "--codec", codec, path, "--output", "json")
	cmd.Dir = repoRoot
	cmd.Env = testutils.BuildCommandEnv(env)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return 0, fmt.Errorf("store command: %w; output=%s", err, out)
	}
	payload, ok := testutils.TryParseLastJSONLine(string(out))
	if !ok {
		return 0, fmt.Errorf("no JSON in store output: %s", out)
	}
	data, ok := payload["data"].(map[string]any)
	if !ok {
		return 0, fmt.Errorf("store payload missing data: %v", payload)
	}
	idF, ok := data["file_id"].(float64)
	if !ok {
		return 0, fmt.Errorf("store payload missing file_id: %v", data)
	}
	return int64(idF), nil
}

func restoreMustMatchHashG6(t *testing.T, dbconn *sql.DB, fileID int64, outPath, wantHash string) {
	t.Helper()

	if err := storage.RestoreFileWithDB(dbconn, fileID, outPath); err != nil {
		t.Fatalf("restore file %d: %v", fileID, err)
	}
	if gotHash := testutils.SHA256File(t, outPath); gotHash != wantHash {
		t.Fatalf("restored hash mismatch: want %s got %s", wantHash, gotHash)
	}
}

func countInt64QueryG6(t *testing.T, dbconn *sql.DB, query string, args ...any) int64 {
	t.Helper()

	var n int64
	if err := dbconn.QueryRow(query, args...).Scan(&n); err != nil {
		t.Fatalf("count query failed (%s): %v", query, err)
	}
	return n
}

func verifyConcurrentInvariantsG6(t *testing.T, dbconn *sql.DB) {
	t.Helper()

	if err := maintenance.VerifyCommandWithContainersDir(container.ContainersDir, "system", 0, verify.VerifyFull); err != nil {
		t.Fatalf("verify full: %v", err)
	}
	testutils.AssertNoProcessingRows(t, dbconn)
	testutils.AssertUniqueFileChunkOrders(t, dbconn)

	negativeLiveRefs := countInt64QueryG6(t, dbconn, `SELECT COUNT(*) FROM chunk WHERE live_ref_count < 0`)
	if negativeLiveRefs != 0 {
		t.Fatalf("expected no negative live_ref_count rows, got %d", negativeLiveRefs)
	}

	negativePinRefs := countInt64QueryG6(t, dbconn, `SELECT COUNT(*) FROM chunk WHERE pin_count < 0`)
	if negativePinRefs != 0 {
		t.Fatalf("expected no negative pin_count rows, got %d", negativePinRefs)
	}
}

func TestAdversarialG6ConcurrentStoresSameFileConvergeDeterministically(t *testing.T) {
	testgate.RequireDB(t)
	testgate.RequireLongRun(t)

	for _, codec := range adversarialG6Codecs() {
		t.Run(codec, func(t *testing.T) {
			configureAdversarialG6Codec(t, codec)

			dbconn, env, repoRoot, binPath, tmp := setupAdversarialG6Env(t)
			defer dbconn.Close()

			inputDir := filepath.Join(tmp, "input")
			restoreDir := filepath.Join(tmp, "restore")
			if err := os.MkdirAll(inputDir, 0o755); err != nil {
				t.Fatalf("mkdir input: %v", err)
			}
			if err := os.MkdirAll(restoreDir, 0o755); err != nil {
				t.Fatalf("mkdir restore: %v", err)
			}

			inPath := testutils.CreateTempFile(t, inputDir, "g6-same-file.bin", 2*1024*1024+313)
			wantHash := testutils.SHA256File(t, inPath)

			const workers = 6
			type g6storeResult struct {
				idx int
				id  int64
				err error
			}
			resultCh := make(chan g6storeResult, workers)
			for i := 0; i < workers; i++ {
				i := i
				go func() {
					id, err := storeFileWithCodecCLIG6Async(repoRoot, binPath, env, codec, inPath)
					resultCh <- g6storeResult{idx: i, id: id, err: err}
				}()
			}
			ids := make([]int64, workers)
			for i := 0; i < workers; i++ {
				res := <-resultCh
				if res.err != nil {
					t.Fatalf("concurrent store worker %d failed: %v", res.idx, res.err)
				}
				ids[res.idx] = res.id
			}

			verifyConcurrentInvariantsG6(t, dbconn)

			baseGraph := testutils.QueryChunkGraph(t, dbconn, ids[0])
			if len(baseGraph) == 0 {
				t.Fatalf("expected non-empty chunk graph for first stored file")
			}
			for i := 1; i < len(ids); i++ {
				graph := testutils.QueryChunkGraph(t, dbconn, ids[i])
				if len(graph) != len(baseGraph) {
					t.Fatalf("graph length mismatch for file %d: want %d got %d", i, len(baseGraph), len(graph))
				}
				for j := range baseGraph {
					if baseGraph[j] != graph[j] {
						t.Fatalf("chunk graph drift between concurrent stores at file=%d index=%d: base=%+v got=%+v", i, j, baseGraph[j], graph[j])
					}
				}
			}

			for i, id := range ids {
				outPath := filepath.Join(restoreDir, fmt.Sprintf("g6-same-file-%02d.bin", i))
				restoreMustMatchHashG6(t, dbconn, id, outPath, wantHash)
			}
		})
	}
}

func TestAdversarialG6ConcurrentStoresSharedChunkInputsPreserveHealthyRestores(t *testing.T) {
	testgate.RequireDB(t)
	testgate.RequireLongRun(t)

	for _, codec := range adversarialG6Codecs() {
		t.Run(codec, func(t *testing.T) {
			configureAdversarialG6Codec(t, codec)

			dbconn, env, repoRoot, binPath, tmp := setupAdversarialG6Env(t)
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

			resultACh := make(chan error, 1)
			resultBCh := make(chan error, 1)
			var fileAID, fileBID int64
			go func() {
				var err error
				fileAID, err = storeFileWithCodecCLIG6Async(repoRoot, binPath, env, codec, hybridA)
				resultACh <- err
			}()
			go func() {
				var err error
				fileBID, err = storeFileWithCodecCLIG6Async(repoRoot, binPath, env, codec, hybridB)
				resultBCh <- err
			}()
			if errA := <-resultACh; errA != nil {
				t.Fatalf("concurrent store hybrid_a failed: %v", errA)
			}
			if errB := <-resultBCh; errB != nil {
				t.Fatalf("concurrent store hybrid_b failed: %v", errB)
			}

			verifyConcurrentInvariantsG6(t, dbconn)

			outA := filepath.Join(restoreDir, "hybrid_a.restored.bin")
			outB := filepath.Join(restoreDir, "hybrid_b.restored.bin")
			restoreMustMatchHashG6(t, dbconn, fileAID, outA, hashA)
			restoreMustMatchHashG6(t, dbconn, fileBID, outB, hashB)
		})
	}
}

func TestAdversarialG6ConcurrentStoreAndGCDoNotLoseReachableData(t *testing.T) {
	testgate.RequireDB(t)

	for _, codec := range adversarialG6Codecs() {
		t.Run(codec, func(t *testing.T) {
			configureAdversarialG6Codec(t, codec)

			dbconn, env, repoRoot, binPath, tmp := setupAdversarialG6Env(t)
			defer dbconn.Close()

			inputDir := filepath.Join(tmp, "input")
			restoreDir := filepath.Join(tmp, "restore")
			if err := os.MkdirAll(inputDir, 0o755); err != nil {
				t.Fatalf("mkdir input: %v", err)
			}
			if err := os.MkdirAll(restoreDir, 0o755); err != nil {
				t.Fatalf("mkdir restore: %v", err)
			}

			anchorPath := testutils.CreateTempFile(t, inputDir, "g6-anchor.bin", 768*1024)
			anchorHash := testutils.SHA256File(t, anchorPath)
			anchorID := storeFileWithCodecCLIG6(t, repoRoot, binPath, env, codec, anchorPath)

			newPath := filepath.Join(inputDir, "g6-new.bin")
			newData := make([]byte, 1024*1024+411)
			for i := range newData {
				newData[i] = byte((i*29 + 13) % 251)
			}
			if err := os.WriteFile(newPath, newData, 0o644); err != nil {
				t.Fatalf("write new file: %v", err)
			}
			newHash := testutils.SHA256File(t, newPath)

			var newID int64
			storeCh := make(chan error, 1)
			gcErrCh := make(chan error, 1)
			go func() {
				var err error
				newID, err = storeFileWithCodecCLIG6Async(repoRoot, binPath, env, codec, newPath)
				storeCh <- err
			}()
			go func() {
				gcErrCh <- maintenance.RunGCWithContainersDir(false, container.ContainersDir)
			}()
			if storeErr := <-storeCh; storeErr != nil {
				t.Fatalf("concurrent store failed: %v", storeErr)
			}
			if gcErr := <-gcErrCh; gcErr != nil {
				t.Fatalf("concurrent gc failed: %v", gcErr)
			}

			verifyConcurrentInvariantsG6(t, dbconn)

			restoreMustMatchHashG6(t, dbconn, anchorID, filepath.Join(restoreDir, "anchor.restored.bin"), anchorHash)
			restoreMustMatchHashG6(t, dbconn, newID, filepath.Join(restoreDir, "new.restored.bin"), newHash)
		})
	}
}

func TestAdversarialG6ConcurrentRemoveAndGCPreserveOtherLiveFiles(t *testing.T) {
	testgate.RequireDB(t)
	testgate.RequireLongRun(t)

	for _, codec := range adversarialG6Codecs() {
		t.Run(codec, func(t *testing.T) {
			configureAdversarialG6Codec(t, codec)

			dbconn, env, repoRoot, binPath, tmp := setupAdversarialG6Env(t)
			defer dbconn.Close()

			inputDir := filepath.Join(tmp, "input")
			restoreDir := filepath.Join(tmp, "restore")
			if err := os.MkdirAll(inputDir, 0o755); err != nil {
				t.Fatalf("mkdir input: %v", err)
			}
			if err := os.MkdirAll(restoreDir, 0o755); err != nil {
				t.Fatalf("mkdir restore: %v", err)
			}

			firstPath := testutils.CreateTempFile(t, inputDir, "g6-first.bin", 1024*1024+123)
			firstHash := testutils.SHA256File(t, firstPath)
			firstID := storeFileWithCodecCLIG6(t, repoRoot, binPath, env, codec, firstPath)

			secondPath := filepath.Join(inputDir, "g6-second.bin")
			secondData := make([]byte, 1024*1024+777)
			for i := range secondData {
				secondData[i] = byte((i*17 + 9) % 251)
			}
			if err := os.WriteFile(secondPath, secondData, 0o644); err != nil {
				t.Fatalf("write second file: %v", err)
			}
			secondHash := testutils.SHA256File(t, secondPath)
			secondID := storeFileWithCodecCLIG6(t, repoRoot, binPath, env, codec, secondPath)

			var removeErr, gcErr error
			var wg sync.WaitGroup
			wg.Add(2)
			go func() {
				defer wg.Done()
				removeErr = storage.RemoveFileWithDB(dbconn, firstID)
			}()
			go func() {
				defer wg.Done()
				gcErr = maintenance.RunGCWithContainersDir(false, container.ContainersDir)
			}()
			wg.Wait()

			if removeErr != nil {
				t.Fatalf("concurrent remove failed: %v", removeErr)
			}
			if gcErr != nil {
				t.Fatalf("concurrent gc failed: %v", gcErr)
			}

			verifyConcurrentInvariantsG6(t, dbconn)

			restoreMustMatchHashG6(t, dbconn, secondID, filepath.Join(restoreDir, "second.restored.bin"), secondHash)

			if err := storage.RestoreFileWithDB(dbconn, firstID, filepath.Join(restoreDir, "first.restored.bin")); err == nil {
				t.Fatalf("removed first file unexpectedly restored successfully")
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

func TestAdversarialG6ConcurrentSnapshotCreateAndGCPreserveRetainedData(t *testing.T) {
	testgate.RequireDB(t)

	for _, codec := range adversarialG6Codecs() {
		t.Run(codec, func(t *testing.T) {
			configureAdversarialG6Codec(t, codec)

			dbconn, env, repoRoot, binPath, tmp := setupAdversarialG6Env(t)
			defer dbconn.Close()

			inputDir := filepath.Join(tmp, "input")
			restoreDir := filepath.Join(tmp, "restore")
			if err := os.MkdirAll(inputDir, 0o755); err != nil {
				t.Fatalf("mkdir input: %v", err)
			}
			if err := os.MkdirAll(restoreDir, 0o755); err != nil {
				t.Fatalf("mkdir restore: %v", err)
			}

			retainedPath := testutils.CreateTempFile(t, inputDir, "g6-snap-retained.bin", 512*1024)
			retainedHash := testutils.SHA256File(t, retainedPath)
			retainedID := storeFileWithCodecCLIG6(t, repoRoot, binPath, env, codec, retainedPath)
			snapshotID := fmt.Sprintf("g6-concurrent-snap-%s", codec)

			var snapErr, gcErr error
			var wg sync.WaitGroup
			wg.Add(2)
			go func() {
				defer wg.Done()
				defer func() {
					if r := recover(); r != nil {
						snapErr = fmt.Errorf("panic: %v", r)
					}
				}()
				args := []string{"snapshot", "create", "--id", snapshotID, "--output", "json"}
				res := testutils.RunColdkeepCommand(t, repoRoot, binPath, env, args...)
				if res.ExitCode != 0 {
					snapErr = fmt.Errorf("snapshot create exited %d\nstdout:\n%s\nstderr:\n%s", res.ExitCode, res.Stdout, res.Stderr)
				}
			}()
			go func() {
				defer wg.Done()
				gcErr = maintenance.RunGCWithContainersDir(false, container.ContainersDir)
			}()
			wg.Wait()

			if snapErr != nil {
				t.Fatalf("concurrent snapshot create failed: %v", snapErr)
			}
			if gcErr != nil {
				t.Fatalf("concurrent gc failed: %v", gcErr)
			}

			verifyConcurrentInvariantsG6(t, dbconn)

			// snapshot must still exist in the DB
			var snapCount int64
			if err := dbconn.QueryRow(`SELECT COUNT(*) FROM snapshot WHERE id = $1`, snapshotID).Scan(&snapCount); err != nil {
				t.Fatalf("query snapshot after concurrent ops: %v", err)
			}
			if snapCount == 0 {
				t.Fatalf("expected snapshot %q to exist after concurrent GC, not found", snapshotID)
			}

			// retained file must still restore correctly
			restoreMustMatchHashG6(t, dbconn, retainedID, filepath.Join(restoreDir, "retained.restored.bin"), retainedHash)
		})
	}
}

var _ = dbschema.PostgresSchema
