package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/franchoy/coldkeep/internal/blocks"
	"github.com/franchoy/coldkeep/internal/chunk"
	"github.com/franchoy/coldkeep/internal/container"
	"github.com/franchoy/coldkeep/internal/db"
	"github.com/franchoy/coldkeep/internal/maintenance"
	"github.com/franchoy/coldkeep/internal/snapshot"
	"github.com/franchoy/coldkeep/internal/storage"
	"github.com/franchoy/coldkeep/internal/verify"
	testutils "github.com/franchoy/coldkeep/tests/utils"
	"github.com/franchoy/coldkeep/tests/utils/testgate"
)

type phase7FixtureInputSet struct {
	allStorePaths        []string
	largePath            string
	largeHash            string
	largeSize            int
	manySmallPaths       []string
	duplicatePathA       string
	duplicatePathB       string
	retainedPath         string
	retainedOriginalHash string
	retainedUpdatedHash  string
}

func writeDeterministicFileWithSalt(t *testing.T, path string, size int, salt int) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir parent for %s: %v", path, err)
	}
	buf := make([]byte, size)
	for i := 0; i < size; i++ {
		buf[i] = byte((i*31 + salt*17 + 7) % 251)
	}
	if err := os.WriteFile(path, buf, 0o644); err != nil {
		t.Fatalf("write deterministic file %s: %v", path, err)
	}
}

func createPhase7FixtureInputSet(t *testing.T, root string) phase7FixtureInputSet {
	t.Helper()

	largePath := filepath.Join(root, "large", "large-v17-fixture.bin")
	largeSize := 3*1024*1024 + 17
	writeDeterministicFileWithSalt(t, largePath, largeSize, 11)

	manySmall := make([]string, 0, 40)
	for i := 0; i < 40; i++ {
		p := filepath.Join(root, "many-small", fmt.Sprintf("small-%03d.txt", i))
		writeDeterministicFileWithSalt(t, p, 96+(i%23), 101+i)
		manySmall = append(manySmall, p)
	}

	dupA := filepath.Join(root, "duplicates", "dup-a.bin")
	dupB := filepath.Join(root, "duplicates", "dup-b.bin")
	dupSize := 64 * 1024
	writeDeterministicFileWithSalt(t, dupA, dupSize, 313)
	writeDeterministicFileWithSalt(t, dupB, dupSize, 313)

	retainedPath := filepath.Join(root, "retained", "deleted-but-snapshot-retained.bin")
	writeDeterministicFileWithSalt(t, retainedPath, 128*1024+33, 701)
	retainedOriginalHash := testutils.SHA256File(t, retainedPath)

	all := make([]string, 0, 1+len(manySmall)+3)
	all = append(all, largePath)
	all = append(all, manySmall...)
	all = append(all, dupA, dupB, retainedPath)
	sort.Strings(all)

	writeDeterministicFileWithSalt(t, retainedPath, 160*1024+7, 911)
	retainedUpdatedHash := testutils.SHA256File(t, retainedPath)

	// Restore original bytes so initial store seeds the pre-upgrade retained file.
	writeDeterministicFileWithSalt(t, retainedPath, 128*1024+33, 701)

	return phase7FixtureInputSet{
		allStorePaths:        all,
		largePath:            largePath,
		largeHash:            testutils.SHA256File(t, largePath),
		largeSize:            largeSize,
		manySmallPaths:       manySmall,
		duplicatePathA:       dupA,
		duplicatePathB:       dupB,
		retainedPath:         retainedPath,
		retainedOriginalHash: retainedOriginalHash,
		retainedUpdatedHash:  retainedUpdatedHash,
	}
}

func logicalIDForStoredPath(t *testing.T, dbconn *sql.DB, storedPath string) int64 {
	t.Helper()
	var logicalID int64
	if err := dbconn.QueryRow(`SELECT logical_file_id FROM physical_file WHERE path = $1`, filepath.ToSlash(storedPath)).Scan(&logicalID); err != nil {
		t.Fatalf("query logical id for path %s: %v", storedPath, err)
	}
	return logicalID
}

func TestPhase7BuildDeterministicV17StyleFixtureIntegration(t *testing.T) {
	testgate.RequireDB(t)

	tmp := prepareReadPathRegressionRepo(t)
	setRepoChunkerVersion(t, chunk.VersionV1SimpleRolling)

	dbconn, err := db.ConnectDB()
	if err != nil {
		t.Fatalf("connect DB for phase7 fixture: %v", err)
	}
	defer func() { _ = dbconn.Close() }()

	inputsRoot := filepath.Join(tmp, "fixture-input")
	inputSet := createPhase7FixtureInputSet(t, inputsRoot)

	sgctx := storage.StorageContext{
		DB:           dbconn,
		Writer:       container.NewLocalWriter(container.GetContainerMaxSize()),
		ContainerDir: container.ContainersDir,
	}

	for _, p := range inputSet.allStorePaths {
		if _, err := storage.StoreFileWithStorageContextAndCodecResultWithPolicy(sgctx, p, blocks.CodecPlain, false); err != nil {
			t.Fatalf("seed legacy-style fixture store %s: %v", p, err)
		}
	}

	retainedStoredPath := filepath.ToSlash(inputSet.retainedPath)
	retainedLogicalBefore := logicalIDForStoredPath(t, dbconn, retainedStoredPath)

	if err := snapshot.CreateSnapshotWithOptions(context.Background(), dbconn, snapshot.SnapshotCreateOptions{
		ID:   "phase7-v17-fixture-snap",
		Type: "full",
	}); err != nil {
		t.Fatalf("create full snapshot for phase7 fixture: %v", err)
	}

	writeDeterministicFileWithSalt(t, inputSet.retainedPath, 160*1024+7, 911)
	if _, err := storage.StoreFileWithStorageContextAndCodecResultWithPolicy(sgctx, inputSet.retainedPath, blocks.CodecPlain, true); err != nil {
		t.Fatalf("replace retained path after snapshot: %v", err)
	}

	retainedLogicalAfter := logicalIDForStoredPath(t, dbconn, retainedStoredPath)
	if retainedLogicalAfter == retainedLogicalBefore {
		t.Fatalf("expected retained path logical id to change after replacement, got same id=%d", retainedLogicalAfter)
	}

	var snapshotRetainsOld int
	if err := dbconn.QueryRow(`
		SELECT COUNT(*)
		FROM snapshot_file
		WHERE snapshot_id = $1 AND logical_file_id = $2
	`, "phase7-v17-fixture-snap", retainedLogicalBefore).Scan(&snapshotRetainsOld); err != nil {
		t.Fatalf("count snapshot_file retained rows: %v", err)
	}
	if snapshotRetainsOld == 0 {
		t.Fatal("expected snapshot to retain old logical file after replacement")
	}

	var currentRefsToOld int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM physical_file WHERE logical_file_id = $1`, retainedLogicalBefore).Scan(&currentRefsToOld); err != nil {
		t.Fatalf("count current physical refs to old logical file: %v", err)
	}
	if currentRefsToOld != 0 {
		t.Fatalf("expected old retained logical file to be deleted from current state, refs=%d", currentRefsToOld)
	}

	var duplicateLogicalA, duplicateLogicalB int64
	if err := dbconn.QueryRow(`SELECT logical_file_id FROM physical_file WHERE path = $1`, filepath.ToSlash(inputSet.duplicatePathA)).Scan(&duplicateLogicalA); err != nil {
		t.Fatalf("query duplicate A logical id: %v", err)
	}
	if err := dbconn.QueryRow(`SELECT logical_file_id FROM physical_file WHERE path = $1`, filepath.ToSlash(inputSet.duplicatePathB)).Scan(&duplicateLogicalB); err != nil {
		t.Fatalf("query duplicate B logical id: %v", err)
	}
	if duplicateLogicalA != duplicateLogicalB {
		t.Fatalf("expected duplicate content to map to one logical file, got %d and %d", duplicateLogicalA, duplicateLogicalB)
	}

	var manySmallCount int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM physical_file WHERE path LIKE $1`, filepath.ToSlash(filepath.Join(inputsRoot, "many-small")) + "/%").Scan(&manySmallCount); err != nil {
		t.Fatalf("count many-small fixture paths: %v", err)
	}
	if manySmallCount != len(inputSet.manySmallPaths) {
		t.Fatalf("many-small fixture path count mismatch: got=%d want=%d", manySmallCount, len(inputSet.manySmallPaths))
	}

	var missingLegacyRows int
	if err := dbconn.QueryRow(`
		SELECT COUNT(*)
		FROM chunk c
		LEFT JOIN blocks b ON b.chunk_id = c.id
		WHERE c.status = 'COMPLETED' AND b.id IS NULL
	`).Scan(&missingLegacyRows); err != nil {
		t.Fatalf("count completed chunks missing legacy blocks rows: %v", err)
	}
	if missingLegacyRows != 0 {
		t.Fatalf("expected v1.7-style companion blocks rows for all completed chunks, missing=%d", missingLegacyRows)
	}

	var nonV17ChunkerRows int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM chunk WHERE chunker_version <> $1`, string(chunk.VersionV1SimpleRolling)).Scan(&nonV17ChunkerRows); err != nil {
		t.Fatalf("count non-v1 chunker rows: %v", err)
	}
	if nonV17ChunkerRows != 0 {
		t.Fatalf("expected all fixture chunks to use v1-simple-rolling, mismatches=%d", nonV17ChunkerRows)
	}

	var largeLogicalID int64
	if err := dbconn.QueryRow(`SELECT logical_file_id FROM physical_file WHERE path = $1`, filepath.ToSlash(inputSet.largePath)).Scan(&largeLogicalID); err != nil {
		t.Fatalf("query large-file logical id: %v", err)
	}

	restoreOut := filepath.Join(tmp, "restored-large.bin")
	if err := storage.RestoreFileWithStorageContext(sgctx, largeLogicalID, restoreOut); err != nil {
		t.Fatalf("restore large fixture file: %v", err)
	}
	if got := testutils.SHA256File(t, restoreOut); got != inputSet.largeHash {
		t.Fatalf("restored large fixture hash mismatch: got=%s want=%s", got, inputSet.largeHash)
	}

	retainedOut := filepath.Join(tmp, "restored-retained-updated.bin")
	if err := storage.RestoreFileWithStorageContext(sgctx, retainedLogicalAfter, retainedOut); err != nil {
		t.Fatalf("restore updated retained file: %v", err)
	}
	if got := testutils.SHA256File(t, retainedOut); got != inputSet.retainedUpdatedHash {
		t.Fatalf("restored updated retained hash mismatch: got=%s want=%s", got, inputSet.retainedUpdatedHash)
	}

	if err := maintenance.VerifyCommandWithContainersDir(container.ContainersDir, "system", 0, verify.VerifyStandard); err != nil {
		t.Fatalf("verify fixture after build: %v", err)
	}
	if err := maintenance.RunGCWithContainersDir(true, container.ContainersDir); err != nil {
		t.Fatalf("gc dry-run fixture after build: %v", err)
	}

	var largeLogicalSize int64
	if err := dbconn.QueryRow(`SELECT total_size FROM logical_file WHERE id = $1`, largeLogicalID).Scan(&largeLogicalSize); err != nil {
		t.Fatalf("query large logical total_size: %v", err)
	}
	if largeLogicalSize != int64(inputSet.largeSize) {
		t.Fatalf("large logical file size mismatch: got=%d want=%d", largeLogicalSize, inputSet.largeSize)
	}

	if inputSet.retainedOriginalHash == inputSet.retainedUpdatedHash {
		t.Fatal("fixture retained old/new hashes must differ")
	}
}

func TestPhase7BuildFixtureWithActualV17BinaryIntegration(t *testing.T) {
	testgate.RequireDB(t)

	v17Bin := os.Getenv("COLDKEEP_V17_BIN")
	if v17Bin == "" {
		t.Skip("Set COLDKEEP_V17_BIN=/path/to/released-v1.7-binary to run actual v1.7 fixture integration")
	}
	if _, err := os.Stat(v17Bin); err != nil {
		t.Fatalf("stat COLDKEEP_V17_BIN path: %v", err)
	}

	tmp := prepareReadPathRegressionRepo(t)
	inputsRoot := filepath.Join(tmp, "v17-bin-input")
	inputSet := createPhase7FixtureInputSet(t, inputsRoot)

	repoRoot := testutils.FindRepoRoot(t)
	env := testutils.DefaultCLIEnv(container.ContainersDir)

	for _, p := range inputSet.allStorePaths {
		res := testutils.RunColdkeepCommand(t, repoRoot, v17Bin, env, "store", p)
		if res.ExitCode != 0 {
			t.Fatalf("v1.7 binary store failed for %s: exit=%d stderr=%s", p, res.ExitCode, res.Stderr)
		}
	}

	res := testutils.RunColdkeepCommand(t, repoRoot, v17Bin, env, "snapshot", "create", "--id", "phase7-v17-bin-snap")
	if res.ExitCode != 0 {
		t.Fatalf("v1.7 binary snapshot create failed: exit=%d stderr=%s", res.ExitCode, res.Stderr)
	}

	writeDeterministicFileWithSalt(t, inputSet.retainedPath, 160*1024+7, 911)
	res = testutils.RunColdkeepCommand(t, repoRoot, v17Bin, env, "store", inputSet.retainedPath, "--replace")
	if res.ExitCode != 0 {
		t.Fatalf("v1.7 binary replace store failed: exit=%d stderr=%s", res.ExitCode, res.Stderr)
	}

	dbconn, err := db.ConnectDB()
	if err != nil {
		t.Fatalf("connect db after v1.7 fixture build: %v", err)
	}
	defer func() { _ = dbconn.Close() }()

	var hasSnapshot int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM snapshot WHERE id = $1`, "phase7-v17-bin-snap").Scan(&hasSnapshot); err != nil {
		t.Fatalf("count v1.7 snapshot rows: %v", err)
	}
	if hasSnapshot != 1 {
		t.Fatalf("expected one v1.7-created snapshot row, got=%d", hasSnapshot)
	}

	if err := maintenance.VerifyCommandWithContainersDir(container.ContainersDir, "system", 0, verify.VerifyStandard); err != nil {
		t.Fatalf("v1.8 verify failed on v1.7-built fixture: %v", err)
	}
	if err := maintenance.RunGCWithContainersDir(true, container.ContainersDir); err != nil {
		t.Fatalf("v1.8 gc dry-run failed on v1.7-built fixture: %v", err)
	}
}
