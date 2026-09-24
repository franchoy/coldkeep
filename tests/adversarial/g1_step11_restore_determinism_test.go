package main

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	corebenchmark "github.com/franchoy/coldkeep/internal/benchmark"
	testutils "github.com/franchoy/coldkeep/tests/utils"
	"github.com/franchoy/coldkeep/tests/utils/testgate"
)

type step11Dataset struct {
	inputRoot   string
	storedPaths []string
	largePath   string
	largeHash   string
}

func makeStep11DeterministicBytes(size int, seed byte) []byte {
	buf := make([]byte, size)
	for i := range buf {
		buf[i] = byte((i*31 + int(seed)*17 + 13) % 251)
	}
	return buf
}

func writeStep11File(t *testing.T, path string, data []byte) string {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir parent for %s: %v", path, err)
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("write file %s: %v", path, err)
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func buildStep11Dataset(t *testing.T, root string) step11Dataset {
	t.Helper()

	inputRoot := filepath.Join(root, "input-step11")
	if err := os.MkdirAll(inputRoot, 0o755); err != nil {
		t.Fatalf("mkdir input root: %v", err)
	}

	storedPaths := make([]string, 0, 97)

	largePath := filepath.Join(inputRoot, "large", "large-file.bin")
	largeHash := writeStep11File(t, largePath, makeStep11DeterministicBytes(6*1024*1024+321, 91))
	storedPaths = append(storedPaths, largePath)

	for i := 0; i < 96; i++ {
		rel := filepath.Join("small", fmt.Sprintf("bucket-%02d", i%8), fmt.Sprintf("file-%03d.txt", i))
		path := filepath.Join(inputRoot, rel)
		size := 128 + (i % 73)
		_ = writeStep11File(t, path, makeStep11DeterministicBytes(size, byte(i+7)))
		storedPaths = append(storedPaths, path)
	}

	sort.Strings(storedPaths)
	return step11Dataset{
		inputRoot:   inputRoot,
		storedPaths: storedPaths,
		largePath:   largePath,
		largeHash:   largeHash,
	}
}

func runStoreFolderWithWorkersStep11(t *testing.T, repoRoot, binPath string, env map[string]string, workers int, inputRoot string) {
	t.Helper()

	testutils.AssertCLIJSONOK(t, testutils.RunColdkeepCommand(
		t,
		repoRoot,
		binPath,
		env,
		"store-folder",
		"--codec", "plain",
		"--workers", fmt.Sprintf("%d", workers),
		inputRoot,
		"--output", "json",
	), "store-folder")
}

func restoreStoredPathsStep11(t *testing.T, repoRoot, binPath string, env map[string]string, dataset step11Dataset, restoreRoot string) map[string]string {
	t.Helper()

	if err := os.MkdirAll(restoreRoot, 0o755); err != nil {
		t.Fatalf("mkdir restore root: %v", err)
	}

	for _, storedPath := range dataset.storedPaths {
		rel, err := filepath.Rel(dataset.inputRoot, storedPath)
		if err != nil {
			t.Fatalf("relative path for %s: %v", storedPath, err)
		}
		dest := filepath.Join(restoreRoot, rel)
		if err := os.MkdirAll(filepath.Dir(dest), 0o755); err != nil {
			t.Fatalf("mkdir restore parent for %s: %v", dest, err)
		}

		testutils.AssertCLIJSONOK(t, testutils.RunColdkeepCommand(
			t,
			repoRoot,
			binPath,
			env,
			"restore",
			"--stored-path", storedPath,
			"--mode", "override",
			"--destination", dest,
			"--overwrite",
			"--output", "json",
		), "restore")
	}

	hashes, err := corebenchmark.HashRestoredTree(restoreRoot)
	if err != nil {
		t.Fatalf("hash restored tree at %s: %v", restoreRoot, err)
	}
	return hashes
}

func hashRestoredLargeFileStep11(t *testing.T, dataset step11Dataset, restoreRoot string) string {
	t.Helper()
	rel, err := filepath.Rel(dataset.inputRoot, dataset.largePath)
	if err != nil {
		t.Fatalf("large file relative path: %v", err)
	}
	largeOut := filepath.Join(restoreRoot, rel)
	return testutils.SHA256File(t, largeOut)
}

func snapshotRestoreTreeStep11(t *testing.T, repoRoot, binPath string, env map[string]string, snapshotID, restoreRoot string, extraArgs ...string) map[string]string {
	t.Helper()
	if err := os.MkdirAll(restoreRoot, 0o755); err != nil {
		t.Fatalf("mkdir snapshot restore root: %v", err)
	}

	args := []string{"snapshot", "restore", snapshotID, "--mode", "prefix", "--destination", restoreRoot, "--overwrite"}
	args = append(args, extraArgs...)
	args = append(args, "--output", "json")
	testutils.AssertCLIJSONOK(t, testutils.RunColdkeepCommand(t, repoRoot, binPath, env, args...), "snapshot")

	hashes, err := corebenchmark.HashRestoredTree(restoreRoot)
	if err != nil {
		t.Fatalf("hash snapshot restored tree: %v", err)
	}
	return hashes
}

func assertTreeHashesEqualStep11(t *testing.T, want, got map[string]string, context string) {
	t.Helper()
	if ok, reason := corebenchmark.EqualRestoredTreeHashes(want, got); !ok {
		t.Fatalf("%s tree hash mismatch: %s", context, reason)
	}
}

func runStep11WorkerScenario(t *testing.T, workers int) map[string]string {
	t.Helper()

	dbconn, env, repoRoot, binPath, tmp := setupAdversarialG1Env(t)
	defer func() { _ = dbconn.Close() }()
	env["COLDKEEP_CODEC"] = "plain"

	dataset := buildStep11Dataset(t, tmp)
	runStoreFolderWithWorkersStep11(t, repoRoot, binPath, env, workers, dataset.inputRoot)

	restoreRoot := filepath.Join(tmp, fmt.Sprintf("restore-workers-%d", workers))
	hashes := restoreStoredPathsStep11(t, repoRoot, binPath, env, dataset, restoreRoot)

	if len(hashes) < 90 {
		t.Fatalf("expected many restored files for workers=%d scenario, got %d", workers, len(hashes))
	}
	if gotLarge := hashRestoredLargeFileStep11(t, dataset, restoreRoot); gotLarge != dataset.largeHash {
		t.Fatalf("workers=%d large file restore hash mismatch: want=%s got=%s", workers, dataset.largeHash, gotLarge)
	}

	return hashes
}

func TestAdversarialStep11RestoreDeterminismMatrix(t *testing.T) {
	testgate.RequireDB(t)

	t.Run("same_repo_restore_twice_identical_tree_hash_and_gc_stable", func(t *testing.T) {
		dbconn, env, repoRoot, binPath, tmp := setupAdversarialG1Env(t)
		defer func() { _ = dbconn.Close() }()
		env["COLDKEEP_CODEC"] = "plain"

		dataset := buildStep11Dataset(t, tmp)
		runStoreFolderWithWorkersStep11(t, repoRoot, binPath, env, 4, dataset.inputRoot)

		r1 := restoreStoredPathsStep11(t, repoRoot, binPath, env, dataset, filepath.Join(tmp, "restore-run-1"))
		r2 := restoreStoredPathsStep11(t, repoRoot, binPath, env, dataset, filepath.Join(tmp, "restore-run-2"))
		assertTreeHashesEqualStep11(t, r1, r2, "same repo restore twice")

		if gotLarge := hashRestoredLargeFileStep11(t, dataset, filepath.Join(tmp, "restore-run-1")); gotLarge != dataset.largeHash {
			t.Fatalf("large file restore hash mismatch: want=%s got=%s", dataset.largeHash, gotLarge)
		}

		testutils.AssertCLIJSONOK(t, testutils.RunColdkeepCommand(
			t,
			repoRoot,
			binPath,
			env,
			"gc",
			"--output", "json",
		), "gc")

		rAfterGC := restoreStoredPathsStep11(t, repoRoot, binPath, env, dataset, filepath.Join(tmp, "restore-after-gc"))
		assertTreeHashesEqualStep11(t, r1, rAfterGC, "restore after GC")
	})

	t.Run("workers_1_vs_4_stored_repo_restore_hash", func(t *testing.T) {
		r1 := runStep11WorkerScenario(t, 1)
		r4 := runStep11WorkerScenario(t, 4)
		assertTreeHashesEqualStep11(t, r1, r4, "workers=1 vs workers=4 restored tree")
	})

	t.Run("snapshot_restore_full_partial_and_after_current_state_changes", func(t *testing.T) {
		dbconn, env, repoRoot, binPath, tmp := setupAdversarialG1Env(t)
		defer func() { _ = dbconn.Close() }()
		env["COLDKEEP_CODEC"] = "plain"

		dataset := buildStep11Dataset(t, tmp)
		runStoreFolderWithWorkersStep11(t, repoRoot, binPath, env, 4, dataset.inputRoot)

		snapshotID := "step11-snapshot-a"
		testutils.AssertCLIJSONOK(t, testutils.RunColdkeepCommand(
			t,
			dataset.inputRoot,
			binPath,
			env,
			"snapshot", "create", "--id", snapshotID, "--output", "json",
		), "snapshot")

		full1 := snapshotRestoreTreeStep11(t, repoRoot, binPath, env, snapshotID, filepath.Join(tmp, "snapshot-full-1"))
		full2 := snapshotRestoreTreeStep11(t, repoRoot, binPath, env, snapshotID, filepath.Join(tmp, "snapshot-full-2"))
		assertTreeHashesEqualStep11(t, full1, full2, "snapshot restore full twice")

		paths, matched := snapshotShowPathsAndCount(t, repoRoot, binPath, env, snapshotID, nil)
		if matched == 0 || len(paths) == 0 {
			t.Fatalf("expected snapshot to contain files for partial/filter restore test")
		}

		prefix := filepath.ToSlash(filepath.Dir(paths[0])) + "/"
		partial1 := snapshotRestoreTreeStep11(t, repoRoot, binPath, env, snapshotID, filepath.Join(tmp, "snapshot-partial-1"), "--prefix", prefix)
		partial2 := snapshotRestoreTreeStep11(t, repoRoot, binPath, env, snapshotID, filepath.Join(tmp, "snapshot-partial-2"), "--prefix", prefix)
		if len(partial1) == 0 {
			t.Fatalf("expected non-empty filtered snapshot restore output for prefix %q", prefix)
		}
		assertTreeHashesEqualStep11(t, partial1, partial2, "snapshot restore partial/filter determinism")

		mutatePath := dataset.storedPaths[0]
		mutated := makeStep11DeterministicBytes(4096, 203)
		if err := os.WriteFile(mutatePath, mutated, 0o644); err != nil {
			t.Fatalf("mutate current-state file %s: %v", mutatePath, err)
		}
		testutils.AssertCLIJSONOK(t, testutils.RunColdkeepCommand(
			t,
			repoRoot,
			binPath,
			env,
			"store", "--codec", "plain", mutatePath, "--output", "json",
		), "store")

		afterChange := snapshotRestoreTreeStep11(t, repoRoot, binPath, env, snapshotID, filepath.Join(tmp, "snapshot-after-current-change"))
		assertTreeHashesEqualStep11(t, full1, afterChange, "snapshot restore after current state changes")

		if strings.TrimSpace(prefix) == "" {
			t.Fatalf("prefix should not be empty")
		}
	})
}
