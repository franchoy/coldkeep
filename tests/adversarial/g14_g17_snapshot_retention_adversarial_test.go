package main

import (
	"database/sql"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/franchoy/coldkeep/internal/container"
	"github.com/franchoy/coldkeep/internal/db"
	"github.com/franchoy/coldkeep/tests/testdb"
	testutils "github.com/franchoy/coldkeep/tests/utils"
	"github.com/franchoy/coldkeep/tests/utils/testgate"
)

func setupAdversarialG14Env(t *testing.T) (*sql.DB, map[string]string, string, string, string) {
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
	if _, err := dbconn.Exec(`
		TRUNCATE TABLE
			snapshot_file,
			snapshot,
			physical_file,
			file_chunk,
			chunk,
			logical_file,
			container
		RESTART IDENTITY CASCADE
	`); err != nil {
		t.Fatalf("truncate adversarial v1.3 tables: %v", err)
	}

	repoRoot := testutils.FindRepoRoot(t)
	binPath := testutils.BuildColdkeepBinary(t, repoRoot)

	return dbconn, env, repoRoot, binPath, tmp
}

func storeAdversarialSnapshotFile(t *testing.T, repoRoot, binPath string, env map[string]string, inputDir, name string, size int) (int64, string, string) {
	t.Helper()
	inPath := testutils.CreateTempFile(t, inputDir, name, size)
	wantHash := testutils.SHA256File(t, inPath)

	store := testutils.AssertCLIJSONOK(t, testutils.RunColdkeepCommand(t, repoRoot, binPath, env,
		"store", inPath, "--output", "json"), "store")
	data := testutils.JSONMap(t, store, "data")
	fileID := testutils.JSONInt64(t, data, "file_id")
	storedPath, _ := data["stored_path"].(string)
	if strings.TrimSpace(storedPath) == "" {
		t.Fatalf("store payload missing stored_path: %v", store)
	}
	return fileID, storedPath, wantHash
}

func assertRemoveStoredPathBlocked(t *testing.T, repoRoot, binPath string, env map[string]string, storedPath string) {
	t.Helper()
	res := testutils.RunColdkeepCommand(t, repoRoot, binPath, env,
		"remove", "--stored-path", storedPath, "--output", "json")
	if res.ExitCode == 0 {
		t.Fatalf("expected remove --stored-path to be blocked while snapshot-retained\nstdout:\n%s\nstderr:\n%s", res.Stdout, res.Stderr)
	}
	errPayload, ok := testutils.FindCLIErrorPayload(res.Stderr)
	if !ok {
		errPayload, ok = testutils.FindCLIErrorPayload(res.Stdout + "\n" + res.Stderr)
	}
	if !ok {
		t.Fatalf("remove blocked path produced no machine-readable error\nstdout:\n%s\nstderr:\n%s", res.Stdout, res.Stderr)
	}
	if got, _ := errPayload["invariant_code"].(string); got != "SNAPSHOT_RETAINED_DELETE_BLOCKED" {
		t.Fatalf("expected invariant_code SNAPSHOT_RETAINED_DELETE_BLOCKED, got %q payload=%v", got, errPayload)
	}
}

func gcDryRunData(t *testing.T, repoRoot, binPath string, env map[string]string) map[string]any {
	t.Helper()
	payload := testutils.AssertCLIJSONOK(t, testutils.RunColdkeepCommand(t, repoRoot, binPath, env,
		"gc", "--dry-run", "--output", "json"), "gc")
	return testutils.JSONMap(t, payload, "data")
}

func snapshotCreateWithID(t *testing.T, repoRoot, binPath string, env map[string]string, snapshotID string, paths ...string) {
	t.Helper()
	args := []string{"snapshot", "create"}
	args = append(args, paths...)
	args = append(args, "--id", snapshotID, "--output", "json")
	testutils.AssertCLIJSONOK(t, testutils.RunColdkeepCommand(t, repoRoot, binPath, env, args...), "snapshot")
}

func snapshotDeleteWithForce(t *testing.T, repoRoot, binPath string, env map[string]string, snapshotID string) {
	t.Helper()
	testutils.AssertCLIJSONOK(t, testutils.RunColdkeepCommand(t, repoRoot, binPath, env,
		"snapshot", "delete", snapshotID, "--force", "--output", "json"), "snapshot")
}

func restoreByIDMustMatch(t *testing.T, repoRoot, binPath string, env map[string]string, fileID int64, outDir, wantHash string) {
	t.Helper()
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		t.Fatalf("mkdir restore dir: %v", err)
	}
	testutils.AssertCLIJSONOK(t, testutils.RunColdkeepCommand(t, repoRoot, binPath, env,
		"restore", fmt.Sprintf("%d", fileID), outDir, "--output", "json"), "restore")

	var restoredPath string
	if walkErr := filepath.WalkDir(outDir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		restoredPath = path
		return errors.New("done")
	}); walkErr != nil && !strings.Contains(walkErr.Error(), "done") {
		t.Fatalf("walk restore dir: %v", walkErr)
	}
	if strings.TrimSpace(restoredPath) == "" {
		t.Fatalf("restore produced no output file in %s", outDir)
	}
	if got := testutils.SHA256File(t, restoredPath); got != wantHash {
		t.Fatalf("restored hash mismatch: want=%s got=%s", wantHash, got)
	}
}

func TestAdversarialG14SnapshotRetainedGCGuardUnderChurn(t *testing.T) {
	testgate.RequireDB(t)

	dbconn, env, repoRoot, binPath, tmp := setupAdversarialG14Env(t)
	defer dbconn.Close()

	inputDir := filepath.Join(tmp, "input")
	if err := os.MkdirAll(inputDir, 0o755); err != nil {
		t.Fatalf("mkdir input: %v", err)
	}

	r := rand.New(rand.NewSource(14017))
	for round := 0; round < 3; round++ {
		name := fmt.Sprintf("g14-round-%d.bin", round)
		fileID, storedPath, wantHash := storeAdversarialSnapshotFile(t, repoRoot, binPath, env, inputDir, name, 64*1024+round*997)

		s1 := fmt.Sprintf("g14-snap-%d-a", round)
		snapshotCreateWithID(t, repoRoot, binPath, env, s1)

		var snapshots []string
		snapshots = append(snapshots, s1)
		if r.Intn(2) == 0 {
			s2 := fmt.Sprintf("g14-snap-%d-b", round)
			snapshotCreateWithID(t, repoRoot, binPath, env, s2)
			snapshots = append(snapshots, s2)
		}

		assertRemoveStoredPathBlocked(t, repoRoot, binPath, env, storedPath)
		before := gcDryRunData(t, repoRoot, binPath, env)
		if got := testutils.JSONInt64(t, before, "snapshot_retained_logical_files"); got < 1 {
			t.Fatalf("expected snapshot_retained_logical_files >= 1 before snapshot delete, got %d payload=%v", got, before)
		}

		// Real GC under retention should not reclaim the snapshot-protected file.
		testutils.AssertCLIJSONOK(t, testutils.RunColdkeepCommand(t, repoRoot, binPath, env,
			"gc", "--output", "json"), "gc")
		restoreByIDMustMatch(t, repoRoot, binPath, env, fileID, filepath.Join(tmp, fmt.Sprintf("g14-restore-%d", round)), wantHash)

		testutils.AssertCLIJSONOK(t, testutils.RunColdkeepCommand(t, repoRoot, binPath, env,
			"verify", "system", "--standard", "--output", "json"), "verify")

		// Delete snapshots in randomized order, then current-path remove should become eligible.
		if len(snapshots) == 2 && r.Intn(2) == 0 {
			snapshots[0], snapshots[1] = snapshots[1], snapshots[0]
		}
		for _, sid := range snapshots {
			snapshotDeleteWithForce(t, repoRoot, binPath, env, sid)
		}

		testutils.AssertCLIJSONOK(t, testutils.RunColdkeepCommand(t, repoRoot, binPath, env,
			"remove", "--stored-path", storedPath, "--output", "json"), "remove")
		after := gcDryRunData(t, repoRoot, binPath, env)
		if got := testutils.JSONInt64(t, after, "snapshot_retained_logical_files"); got != 0 {
			t.Fatalf("expected snapshot_retained_logical_files=0 after deleting all snapshots and removing current mapping, got %d payload=%v", got, after)
		}
	}
}

func TestAdversarialG15CorruptedSnapshotMetadataDetectionConservativeGC(t *testing.T) {
	testgate.RequireDB(t)

	dbconn, env, repoRoot, binPath, tmp := setupAdversarialG14Env(t)
	defer dbconn.Close()

	inputDir := filepath.Join(tmp, "input")
	if err := os.MkdirAll(inputDir, 0o755); err != nil {
		t.Fatalf("mkdir input: %v", err)
	}

	_, storedPath, _ := storeAdversarialSnapshotFile(t, repoRoot, binPath, env, inputDir, "g15-valid.bin", 80*1024)
	snapshotCreateWithID(t, repoRoot, binPath, env, "g15-valid-snap")

	// 1) Invalid lifecycle state referenced by snapshot_file.
	var invalidLifecycleID int64
	if err := dbconn.QueryRow(`
		INSERT INTO logical_file (original_name, total_size, file_hash, status, ref_count)
		VALUES ('g15-invalid-lifecycle.bin', 1, repeat('a', 64), 'ABORTED', 0)
		RETURNING id
	`).Scan(&invalidLifecycleID); err != nil {
		t.Fatalf("insert invalid lifecycle logical_file: %v", err)
	}
	if _, err := dbconn.Exec(`
		INSERT INTO snapshot (id, created_at, type) VALUES ('g15-invalid-lifecycle-snap', now(), 'full')
	`); err != nil {
		t.Fatalf("insert invalid lifecycle snapshot: %v", err)
	}
	testdb.InsertSnapshotFileRef(t, dbconn, "g15-invalid-lifecycle-snap", "docs/invalid-lifecycle.bin", invalidLifecycleID)

	// 2) Retained non-empty logical file with missing chunk graph.
	var missingGraphID int64
	if err := dbconn.QueryRow(`
		INSERT INTO logical_file (original_name, total_size, file_hash, status, ref_count)
		VALUES ('g15-missing-graph.bin', 128, repeat('b', 64), 'COMPLETED', 0)
		RETURNING id
	`).Scan(&missingGraphID); err != nil {
		t.Fatalf("insert missing graph logical_file: %v", err)
	}
	if _, err := dbconn.Exec(`
		INSERT INTO snapshot (id, created_at, type) VALUES ('g15-missing-graph-snap', now(), 'full')
	`); err != nil {
		t.Fatalf("insert missing graph snapshot: %v", err)
	}
	testdb.InsertSnapshotFileRef(t, dbconn, "g15-missing-graph-snap", "docs/missing-graph.bin", missingGraphID)

	// 3) Orphan snapshot reference (best-effort: requires elevated privilege).
	orphanInjected := false
	if _, err := dbconn.Exec(`SET session_replication_role = replica`); err == nil {
		defer func() {
			_, _ = dbconn.Exec(`SET session_replication_role = origin`)
		}()
		if _, err := dbconn.Exec(`
			INSERT INTO snapshot (id, created_at, type) VALUES ('g15-orphan-snap', now(), 'full')
		`); err != nil {
			t.Fatalf("insert orphan snapshot row: %v", err)
		}
		pathID := testdb.EnsureSnapshotPathID(t, dbconn, "docs/orphan.bin")
		if _, err := dbconn.Exec(`
			INSERT INTO snapshot_file (snapshot_id, path_id, logical_file_id)
			VALUES ('g15-orphan-snap', $1, 999999999)
		`, pathID); err != nil {
			t.Fatalf("insert orphan snapshot_file row with replication_role=replica: %v", err)
		}
		orphanInjected = true
	} else {
		t.Logf("could not set session_replication_role=replica; orphan-injection subcase skipped: %v", err)
	}

	verifyRes := testutils.RunColdkeepCommand(t, repoRoot, binPath, env,
		"verify", "system", "--standard", "--output", "json")
	if verifyRes.ExitCode == 0 {
		t.Fatalf("expected verify system --standard to fail on corrupted snapshot metadata\nstdout:\n%s\nstderr:\n%s", verifyRes.Stdout, verifyRes.Stderr)
	}
	errPayload, ok := testutils.FindCLIErrorPayload(verifyRes.Stderr)
	if !ok {
		errPayload, ok = testutils.FindCLIErrorPayload(verifyRes.Stdout + "\n" + verifyRes.Stderr)
	}
	if !ok {
		t.Fatalf("verify failure produced no machine-readable error payload\nstdout:\n%s\nstderr:\n%s", verifyRes.Stdout, verifyRes.Stderr)
	}
	if got, _ := errPayload["error_class"].(string); got != "VERIFY" {
		t.Fatalf("expected error_class=VERIFY, got %q payload=%v", got, errPayload)
	}
	code, _ := errPayload["invariant_code"].(string)
	if code != "SNAPSHOT_GRAPH_INTEGRITY" && code != "SNAPSHOT_GRAPH_ORPHAN_LOGICAL_REF" && code != "SNAPSHOT_GRAPH_INVALID_LIFECYCLE" {
		t.Fatalf("expected snapshot graph invariant code, got %q payload=%v", code, errPayload)
	}
	if orphanInjected {
		t.Log("orphan snapshot_file.logical_file_id subcase injected successfully")
	}

	// GC remains conservative: it should not enable removal of valid snapshot-retained data.
	assertRemoveStoredPathBlocked(t, repoRoot, binPath, env, storedPath)
	before := gcDryRunData(t, repoRoot, binPath, env)
	if got := testutils.JSONInt64(t, before, "snapshot_retained_logical_files"); got < 1 {
		t.Fatalf("expected snapshot_retained_logical_files >= 1 on corrupted state, got %d payload=%v", got, before)
	}
}

func randomSnapshotQueryArgs(r *rand.Rand, exactPaths []string, prefixes []string) []string {
	args := make([]string, 0, 16)
	if len(exactPaths) > 0 && r.Intn(4) == 0 {
		args = append(args, "--path", exactPaths[r.Intn(len(exactPaths))])
	}
	if len(prefixes) > 0 && r.Intn(4) == 0 {
		args = append(args, "--prefix", prefixes[r.Intn(len(prefixes))])
	}
	if r.Intn(4) == 0 {
		args = append(args, "--pattern", "*g16*")
	}
	if r.Intn(4) == 0 {
		args = append(args, "--regex", ".*g16.*")
	}
	if r.Intn(3) == 0 {
		args = append(args, "--min-size", strconv.Itoa(r.Intn(1024)))
	}
	if r.Intn(3) == 0 {
		args = append(args, "--max-size", strconv.Itoa(1024*1024))
	}
	if r.Intn(3) == 0 {
		args = append(args, "--modified-after", "1970-01-01T00:00:00Z")
	}
	if r.Intn(3) == 0 {
		args = append(args, "--modified-before", "2100-01-01T00:00:00Z")
	}
	return args
}

func snapshotShowPathsAndCount(t *testing.T, repoRoot, binPath string, env map[string]string, snapshotID string, filterArgs []string) ([]string, int64) {
	t.Helper()
	args := []string{"snapshot", "show", snapshotID}
	args = append(args, filterArgs...)
	args = append(args, "--output", "json")
	payload := testutils.AssertCLIJSONOK(t, testutils.RunColdkeepCommand(t, repoRoot, binPath, env, args...), "snapshot")
	data := testutils.JSONMap(t, payload, "data")

	matched := testutils.JSONInt64(t, data, "matched_file_count")
	fileCount := testutils.JSONInt64(t, data, "file_count")
	if matched != fileCount {
		t.Fatalf("snapshot show count mismatch: matched_file_count=%d file_count=%d payload=%v", matched, fileCount, payload)
	}

	rawFiles, ok := data["files"].([]any)
	if !ok {
		t.Fatalf("snapshot show payload missing files array: %v", payload)
	}
	if int64(len(rawFiles)) != matched {
		t.Fatalf("snapshot show files length mismatch: len(files)=%d matched_file_count=%d payload=%v", len(rawFiles), matched, payload)
	}
	paths := make([]string, 0, len(rawFiles))
	for _, raw := range rawFiles {
		item, _ := raw.(map[string]any)
		path, _ := item["path"].(string)
		paths = append(paths, path)
	}
	return paths, matched
}

func snapshotDiffEntriesAndCount(t *testing.T, repoRoot, binPath string, env map[string]string, baseID, targetID string, filterArgs []string) ([]string, int64) {
	t.Helper()
	args := []string{"snapshot", "diff", baseID, targetID}
	args = append(args, filterArgs...)
	args = append(args, "--output", "json")
	payload := testutils.AssertCLIJSONOK(t, testutils.RunColdkeepCommand(t, repoRoot, binPath, env, args...), "snapshot diff")
	data := testutils.JSONMap(t, payload, "data")

	matched := testutils.JSONInt64(t, data, "matched_entry_count")
	entryCount := testutils.JSONInt64(t, data, "entry_count")
	if matched != entryCount {
		t.Fatalf("snapshot diff count mismatch: matched_entry_count=%d entry_count=%d payload=%v", matched, entryCount, payload)
	}
	rawEntries, ok := data["entries"].([]any)
	if !ok {
		t.Fatalf("snapshot diff payload missing entries array: %v", payload)
	}
	if int64(len(rawEntries)) != matched {
		t.Fatalf("snapshot diff entries length mismatch: len(entries)=%d matched_entry_count=%d payload=%v", len(rawEntries), matched, payload)
	}
	summary := testutils.JSONMap(t, data, "summary")
	total := testutils.JSONInt64(t, summary, "added") + testutils.JSONInt64(t, summary, "removed") + testutils.JSONInt64(t, summary, "modified")
	if total != matched {
		t.Fatalf("snapshot diff summary mismatch: added+removed+modified=%d matched_entry_count=%d payload=%v", total, matched, payload)
	}

	keys := make([]string, 0, len(rawEntries))
	for _, raw := range rawEntries {
		item, _ := raw.(map[string]any)
		path, _ := item["path"].(string)
		typ, _ := item["type"].(string)
		keys = append(keys, typ+":"+path)
	}
	return keys, matched
}

func TestAdversarialG16SnapshotQueryContractChaos(t *testing.T) {
	testgate.RequireDB(t)

	dbconn, env, repoRoot, binPath, tmp := setupAdversarialG14Env(t)
	defer dbconn.Close()

	inputDir := filepath.Join(tmp, "input")
	if err := os.MkdirAll(filepath.Join(inputDir, "docs"), 0o755); err != nil {
		t.Fatalf("mkdir docs: %v", err)
	}
	if err := os.MkdirAll(filepath.Join(inputDir, "img"), 0o755); err != nil {
		t.Fatalf("mkdir img: %v", err)
	}

	_, p1, _ := storeAdversarialSnapshotFile(t, repoRoot, binPath, env, filepath.Join(inputDir, "docs"), "g16-a.txt", 10*1024)
	_, p2, _ := storeAdversarialSnapshotFile(t, repoRoot, binPath, env, filepath.Join(inputDir, "docs"), "g16-b.txt", 14*1024)
	_, p3, _ := storeAdversarialSnapshotFile(t, repoRoot, binPath, env, filepath.Join(inputDir, "img"), "g16-c.png", 12*1024)

	snapshotCreateWithID(t, repoRoot, binPath, env, "g16-snap-1", strings.TrimLeft(filepath.ToSlash(p1), "/"), strings.TrimLeft(filepath.ToSlash(p2), "/"))
	snapshotCreateWithID(t, repoRoot, binPath, env, "g16-snap-2", strings.TrimLeft(filepath.ToSlash(p2), "/"), strings.TrimLeft(filepath.ToSlash(p3), "/"))

	exactPaths := []string{strings.TrimLeft(filepath.ToSlash(p1), "/"), strings.TrimLeft(filepath.ToSlash(p2), "/"), strings.TrimLeft(filepath.ToSlash(p3), "/")}
	prefixes := []string{filepath.ToSlash(filepath.Dir(exactPaths[0])) + "/", filepath.ToSlash(filepath.Dir(exactPaths[2])) + "/"}

	r := rand.New(rand.NewSource(16016))
	for i := 0; i < 12; i++ {
		filterArgs := randomSnapshotQueryArgs(r, exactPaths, prefixes)

		// snapshot show: deterministic ordering + consistent counters.
		showPaths1, showCount1 := snapshotShowPathsAndCount(t, repoRoot, binPath, env, "g16-snap-1", filterArgs)
		showPaths2, showCount2 := snapshotShowPathsAndCount(t, repoRoot, binPath, env, "g16-snap-1", filterArgs)
		if showCount1 != showCount2 || !reflect.DeepEqual(showPaths1, showPaths2) {
			t.Fatalf("snapshot show is not deterministic for args=%v: run1=%v/%d run2=%v/%d", filterArgs, showPaths1, showCount1, showPaths2, showCount2)
		}

		// snapshot restore: same filters should restore exactly matched count.
		restoreDir := filepath.Join(tmp, "g16-restore", fmt.Sprintf("iter-%d", i))
		if err := os.MkdirAll(restoreDir, 0o755); err != nil {
			t.Fatalf("mkdir restore dir: %v", err)
		}
		restoreArgs := []string{"snapshot", "restore", "g16-snap-1"}
		restoreArgs = append(restoreArgs, filterArgs...)
		restoreArgs = append(restoreArgs, "--mode", "prefix", "--destination", restoreDir, "--output", "json")
		restorePayload := testutils.AssertCLIJSONOK(t, testutils.RunColdkeepCommand(t, repoRoot, binPath, env, restoreArgs...), "snapshot")
		restoreData := testutils.JSONMap(t, restorePayload, "data")
		restoredFiles := testutils.JSONInt64(t, restoreData, "restored_files")
		if restoredFiles != showCount1 {
			t.Fatalf("snapshot restore count mismatch for args=%v: restored_files=%d show_matched=%d payload=%v", filterArgs, restoredFiles, showCount1, restorePayload)
		}

		// snapshot diff: deterministic ordering + consistent counters/summary.
		diffKeys1, diffCount1 := snapshotDiffEntriesAndCount(t, repoRoot, binPath, env, "g16-snap-1", "g16-snap-2", filterArgs)
		diffKeys2, diffCount2 := snapshotDiffEntriesAndCount(t, repoRoot, binPath, env, "g16-snap-1", "g16-snap-2", filterArgs)
		if diffCount1 != diffCount2 || !reflect.DeepEqual(diffKeys1, diffKeys2) {
			t.Fatalf("snapshot diff is not deterministic for args=%v: run1=%v/%d run2=%v/%d", filterArgs, diffKeys1, diffCount1, diffKeys2, diffCount2)
		}
	}
}

func TestAdversarialG17RetentionRootTransitionChurn(t *testing.T) {
	testgate.RequireDB(t)

	dbconn, env, repoRoot, binPath, tmp := setupAdversarialG14Env(t)
	defer dbconn.Close()

	inputDir := filepath.Join(tmp, "input")
	if err := os.MkdirAll(inputDir, 0o755); err != nil {
		t.Fatalf("mkdir input: %v", err)
	}

	_, storedPath, _ := storeAdversarialSnapshotFile(t, repoRoot, binPath, env, inputDir, "g17-root.bin", 70*1024)

	r := rand.New(rand.NewSource(17017))
	snapshots := make([]string, 0, 8)
	for i := 0; i < 6; i++ {
		sid := fmt.Sprintf("g17-snap-%d", i)
		snapshotCreateWithID(t, repoRoot, binPath, env, sid)
		snapshots = append(snapshots, sid)

		// While at least one snapshot retains the file, current-path removal must be blocked.
		assertRemoveStoredPathBlocked(t, repoRoot, binPath, env, storedPath)
		if got := testutils.JSONInt64(t, gcDryRunData(t, repoRoot, binPath, env), "snapshot_retained_logical_files"); got < 1 {
			t.Fatalf("expected retained snapshot roots while snapshots exist, got %d", got)
		}

		// Randomly prune one earlier snapshot to churn transition ordering.
		if len(snapshots) > 2 && r.Intn(2) == 0 {
			idx := r.Intn(len(snapshots) - 1)
			snapshotDeleteWithForce(t, repoRoot, binPath, env, snapshots[idx])
			snapshots = append(snapshots[:idx], snapshots[idx+1:]...)
		}
	}

	// Delete all remaining snapshots; eligibility should transition only now.
	for _, sid := range snapshots {
		snapshotDeleteWithForce(t, repoRoot, binPath, env, sid)
	}

	testutils.AssertCLIJSONOK(t, testutils.RunColdkeepCommand(t, repoRoot, binPath, env,
		"remove", "--stored-path", storedPath, "--output", "json"), "remove")
	if got := testutils.JSONInt64(t, gcDryRunData(t, repoRoot, binPath, env), "snapshot_retained_logical_files"); got != 0 {
		t.Fatalf("expected snapshot_retained_logical_files=0 after deleting last retaining root, got %d", got)
	}
}
