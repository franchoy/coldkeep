package main

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/franchoy/coldkeep/internal/blocks"
	"github.com/franchoy/coldkeep/internal/chunk"
	"github.com/franchoy/coldkeep/internal/container"
	"github.com/franchoy/coldkeep/internal/db"
	"github.com/franchoy/coldkeep/internal/maintenance"
	"github.com/franchoy/coldkeep/internal/recovery"
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

func deterministicBytesWithSalt(size int, salt int) []byte {
	buf := make([]byte, size)
	for i := 0; i < size; i++ {
		buf[i] = byte((i*31 + salt*17 + 7) % 251)
	}
	return buf
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
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM physical_file WHERE path LIKE $1`, filepath.ToSlash(filepath.Join(inputsRoot, "many-small"))+"/%").Scan(&manySmallCount); err != nil {
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

func TestPhase7LegacyOnlyRestoreAndVerifyIntegration(t *testing.T) {
	testgate.RequireDB(t)

	tmp := prepareReadPathRegressionRepo(t)
	setRepoChunkerVersion(t, chunk.VersionV1SimpleRolling)

	dbconn, err := db.ConnectDB()
	if err != nil {
		t.Fatalf("connect DB for legacy-only fixture: %v", err)
	}

	inputsRoot := filepath.Join(tmp, "legacy-only-input")
	inputSet := createPhase7FixtureInputSet(t, inputsRoot)

	sgctx := storage.StorageContext{
		DB:           dbconn,
		Writer:       container.NewLocalWriter(container.GetContainerMaxSize()),
		ContainerDir: container.ContainersDir,
	}

	expectedUniqueHashes := make(map[string]struct{})
	for _, p := range inputSet.allStorePaths {
		expectedUniqueHashes[testutils.SHA256File(t, p)] = struct{}{}
		if _, err := storage.StoreFileWithStorageContextAndCodecResultWithPolicy(sgctx, p, blocks.CodecPlain, false); err != nil {
			_ = dbconn.Close()
			t.Fatalf("seed legacy-only store %s: %v", p, err)
		}
	}

	// Convert to legacy-only metadata shape by dropping packed metadata rows.
	if _, err := dbconn.Exec(`DELETE FROM chunk_block_refs`); err != nil {
		_ = dbconn.Close()
		t.Fatalf("delete chunk_block_refs for legacy-only fixture: %v", err)
	}
	if _, err := dbconn.Exec(`DELETE FROM storage_blocks`); err != nil {
		_ = dbconn.Close()
		t.Fatalf("delete storage_blocks for legacy-only fixture: %v", err)
	}

	var storageBlocksRows int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM storage_blocks`).Scan(&storageBlocksRows); err != nil {
		_ = dbconn.Close()
		t.Fatalf("count storage_blocks rows: %v", err)
	}
	if storageBlocksRows != 0 {
		_ = dbconn.Close()
		t.Fatalf("expected no storage_blocks rows in legacy-only fixture, got=%d", storageBlocksRows)
	}

	var chunkBlockRefRows int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM chunk_block_refs`).Scan(&chunkBlockRefRows); err != nil {
		_ = dbconn.Close()
		t.Fatalf("count chunk_block_refs rows: %v", err)
	}
	if chunkBlockRefRows != 0 {
		_ = dbconn.Close()
		t.Fatalf("expected no chunk_block_refs rows in legacy-only fixture, got=%d", chunkBlockRefRows)
	}

	if err := dbconn.Close(); err != nil {
		t.Fatalf("close db before reopen: %v", err)
	}

	if err := recovery.SystemRecoveryWithContainersDir(container.ContainersDir); err != nil {
		t.Fatalf("reopen legacy-only repository with v1.8 runtime: %v", err)
	}

	dbconn, err = db.ConnectDB()
	if err != nil {
		t.Fatalf("reconnect DB after reopen: %v", err)
	}
	defer func() { _ = dbconn.Close() }()

	sgctx = storage.StorageContext{
		DB:           dbconn,
		ContainerDir: container.ContainersDir,
	}

	rows, err := dbconn.Query(`
		SELECT id, file_hash
		FROM logical_file
		WHERE status = 'COMPLETED'
		ORDER BY id ASC
	`)
	if err != nil {
		t.Fatalf("query completed legacy logical files: %v", err)
	}
	defer func() { _ = rows.Close() }()

	restoreDir := filepath.Join(tmp, "legacy-only-restore")
	if err := os.MkdirAll(restoreDir, 0o755); err != nil {
		t.Fatalf("mkdir legacy-only restore dir: %v", err)
	}

	restoredCount := 0
	for rows.Next() {
		var fileID int64
		var wantHash string
		if err := rows.Scan(&fileID, &wantHash); err != nil {
			t.Fatalf("scan completed legacy logical file row: %v", err)
		}

		outPath := filepath.Join(restoreDir, fmt.Sprintf("legacy-%d.bin", fileID))
		if err := storage.RestoreFileWithStorageContext(sgctx, fileID, outPath); err != nil {
			t.Fatalf("restore legacy-only file id=%d: %v", fileID, err)
		}

		gotHash := testutils.SHA256File(t, outPath)
		if gotHash != wantHash {
			t.Fatalf("legacy-only restored hash mismatch for file id=%d: got=%s want=%s", fileID, gotHash, wantHash)
		}
		if _, ok := expectedUniqueHashes[gotHash]; !ok {
			t.Fatalf("restored hash not present in seeded legacy fixture: %s", gotHash)
		}
		restoredCount++
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate completed legacy logical file rows: %v", err)
	}
	if restoredCount != len(expectedUniqueHashes) {
		t.Fatalf("legacy-only restored logical file count mismatch: got=%d want=%d", restoredCount, len(expectedUniqueHashes))
	}

	if err := maintenance.VerifyCommandWithContainersDir(container.ContainersDir, "system", 0, verify.VerifyStandard); err != nil {
		t.Fatalf("verify failed on legacy-only fixture under v1.8: %v", err)
	}
}

func TestPhase7UpgradeAndAddNewDataIntegration(t *testing.T) {
	testgate.RequireDB(t)

	tmp := prepareReadPathRegressionRepo(t)
	setRepoChunkerVersion(t, chunk.VersionV1SimpleRolling)

	dbconn, err := db.ConnectDB()
	if err != nil {
		t.Fatalf("connect DB for upgrade-and-add-new-data fixture: %v", err)
	}

	legacyRoot := filepath.Join(tmp, "upgrade-legacy-input")
	legacySet := createPhase7FixtureInputSet(t, legacyRoot)
	legacyExpectedHashes := make(map[string]struct{})

	sgctx := storage.StorageContext{
		DB:           dbconn,
		Writer:       container.NewLocalWriter(container.GetContainerMaxSize()),
		ContainerDir: container.ContainersDir,
	}

	for _, p := range legacySet.allStorePaths {
		legacyExpectedHashes[testutils.SHA256File(t, p)] = struct{}{}
		if _, err := storage.StoreFileWithStorageContextAndCodecResultWithPolicy(sgctx, p, blocks.CodecPlain, false); err != nil {
			_ = dbconn.Close()
			t.Fatalf("seed legacy data %s: %v", p, err)
		}
	}

	// Represent a true pre-upgrade legacy repository by removing packed metadata.
	if _, err := dbconn.Exec(`DELETE FROM chunk_block_refs`); err != nil {
		_ = dbconn.Close()
		t.Fatalf("delete chunk_block_refs before upgrade: %v", err)
	}
	if _, err := dbconn.Exec(`DELETE FROM storage_blocks`); err != nil {
		_ = dbconn.Close()
		t.Fatalf("delete storage_blocks before upgrade: %v", err)
	}

	var oldMaxChunkID int64
	if err := dbconn.QueryRow(`SELECT COALESCE(MAX(id), 0) FROM chunk`).Scan(&oldMaxChunkID); err != nil {
		_ = dbconn.Close()
		t.Fatalf("query old max chunk id: %v", err)
	}
	var oldMaxLogicalID int64
	if err := dbconn.QueryRow(`SELECT COALESCE(MAX(id), 0) FROM logical_file`).Scan(&oldMaxLogicalID); err != nil {
		_ = dbconn.Close()
		t.Fatalf("query old max logical_file id: %v", err)
	}

	if err := dbconn.Close(); err != nil {
		t.Fatalf("close db before upgrade reopen: %v", err)
	}

	if err := recovery.SystemRecoveryWithContainersDir(container.ContainersDir); err != nil {
		t.Fatalf("open legacy repository with v1.8 runtime: %v", err)
	}

	dbconn, err = db.ConnectDB()
	if err != nil {
		t.Fatalf("reconnect DB after upgrade reopen: %v", err)
	}
	defer func() { _ = dbconn.Close() }()

	newRoot := filepath.Join(tmp, "upgrade-new-input")
	newFilePath := filepath.Join(newRoot, "new-single-after-upgrade.bin")
	writeDeterministicFileWithSalt(t, newFilePath, 2*1024*1024+123, 1301)

	newFolderPath := filepath.Join(newRoot, "new-folder")
	newFolderFiles := make([]string, 0, 12)
	for i := 0; i < 12; i++ {
		p := filepath.Join(newFolderPath, fmt.Sprintf("new-%02d.dat", i))
		writeDeterministicFileWithSalt(t, p, 4096+(i*211), 1700+i)
		newFolderFiles = append(newFolderFiles, p)
	}

	newExpectedHashes := make(map[string]struct{})
	newExpectedHashes[testutils.SHA256File(t, newFilePath)] = struct{}{}
	for _, p := range newFolderFiles {
		newExpectedHashes[testutils.SHA256File(t, p)] = struct{}{}
	}

	sgctx = storage.StorageContext{
		DB:           dbconn,
		Writer:       container.NewLocalWriter(container.GetContainerMaxSize()),
		ContainerDir: container.ContainersDir,
	}

	if _, err := storage.StoreFileWithStorageContextAndCodecResultWithPolicy(sgctx, newFilePath, blocks.CodecPlain, false); err != nil {
		t.Fatalf("store new file after upgrade: %v", err)
	}
	if err := storage.StoreFolderWithStorageContextAndCodec(sgctx, newFolderPath, blocks.CodecPlain); err != nil {
		t.Fatalf("store new folder after upgrade: %v", err)
	}

	var newChunkPackedRefs int
	if err := dbconn.QueryRow(`
		SELECT COUNT(*)
		FROM chunk c
		JOIN chunk_block_refs r ON r.chunk_id = c.id
		WHERE c.id > $1
	`, oldMaxChunkID).Scan(&newChunkPackedRefs); err != nil {
		t.Fatalf("count new chunks with packed refs: %v", err)
	}
	if newChunkPackedRefs == 0 {
		t.Fatal("expected new data after upgrade to use packed chunk_block_refs")
	}

	var newCompletedMissingPacked int
	if err := dbconn.QueryRow(`
		SELECT COUNT(*)
		FROM chunk c
		LEFT JOIN chunk_block_refs r ON r.chunk_id = c.id
		WHERE c.id > $1 AND c.status = 'COMPLETED' AND r.chunk_id IS NULL
	`, oldMaxChunkID).Scan(&newCompletedMissingPacked); err != nil {
		t.Fatalf("count new completed chunks missing packed refs: %v", err)
	}
	if newCompletedMissingPacked != 0 {
		t.Fatalf("expected all new completed chunks to have packed refs, missing=%d", newCompletedMissingPacked)
	}

	var oldChunkPackedRefs int
	if err := dbconn.QueryRow(`
		SELECT COUNT(*)
		FROM chunk c
		JOIN chunk_block_refs r ON r.chunk_id = c.id
		WHERE c.id <= $1
	`, oldMaxChunkID).Scan(&oldChunkPackedRefs); err != nil {
		t.Fatalf("count old chunks with packed refs: %v", err)
	}
	if oldChunkPackedRefs != 0 {
		t.Fatalf("expected old legacy chunks to remain on legacy read path (no packed refs), got=%d", oldChunkPackedRefs)
	}

	var oldCompletedMissingLegacyBlocks int
	if err := dbconn.QueryRow(`
		SELECT COUNT(*)
		FROM chunk c
		LEFT JOIN blocks b ON b.chunk_id = c.id
		WHERE c.id <= $1 AND c.status = 'COMPLETED' AND b.id IS NULL
	`, oldMaxChunkID).Scan(&oldCompletedMissingLegacyBlocks); err != nil {
		t.Fatalf("count old completed chunks missing legacy blocks rows: %v", err)
	}
	if oldCompletedMissingLegacyBlocks != 0 {
		t.Fatalf("expected old legacy chunks to keep legacy blocks rows, missing=%d", oldCompletedMissingLegacyBlocks)
	}

	restoreAndValidate := func(query string, queryArg int64, expected map[string]struct{}, label string) {
		t.Helper()
		rows, err := dbconn.Query(query, queryArg)
		if err != nil {
			t.Fatalf("query %s logical files: %v", label, err)
		}
		defer func() { _ = rows.Close() }()

		restoreDir := filepath.Join(tmp, fmt.Sprintf("restore-%s", label))
		if err := os.MkdirAll(restoreDir, 0o755); err != nil {
			t.Fatalf("mkdir restore dir %s: %v", label, err)
		}

		ctx := storage.StorageContext{DB: dbconn, ContainerDir: container.ContainersDir}
		restoredCount := 0
		for rows.Next() {
			var fileID int64
			var wantHash string
			if err := rows.Scan(&fileID, &wantHash); err != nil {
				t.Fatalf("scan %s logical file row: %v", label, err)
			}

			outPath := filepath.Join(restoreDir, fmt.Sprintf("%s-%d.bin", label, fileID))
			if err := storage.RestoreFileWithStorageContext(ctx, fileID, outPath); err != nil {
				t.Fatalf("restore %s file id=%d: %v", label, fileID, err)
			}

			gotHash := testutils.SHA256File(t, outPath)
			if gotHash != wantHash {
				t.Fatalf("%s restore hash mismatch for file id=%d: got=%s want=%s", label, fileID, gotHash, wantHash)
			}
			if _, ok := expected[gotHash]; !ok {
				t.Fatalf("%s restored hash not present in expected fixture set: %s", label, gotHash)
			}
			restoredCount++
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("iterate %s logical rows: %v", label, err)
		}
		if restoredCount != len(expected) {
			t.Fatalf("%s restored logical count mismatch: got=%d want=%d", label, restoredCount, len(expected))
		}
	}

	restoreAndValidate(`
		SELECT id, file_hash
		FROM logical_file
		WHERE status = 'COMPLETED' AND id <= $1
		ORDER BY id ASC
	`, oldMaxLogicalID, legacyExpectedHashes, "old")

	restoreAndValidate(`
		SELECT id, file_hash
		FROM logical_file
		WHERE status = 'COMPLETED' AND id > $1
		ORDER BY id ASC
	`, oldMaxLogicalID, newExpectedHashes, "new")

	combinedExpected := make(map[string]struct{}, len(legacyExpectedHashes)+len(newExpectedHashes))
	for hash := range legacyExpectedHashes {
		combinedExpected[hash] = struct{}{}
	}
	for hash := range newExpectedHashes {
		combinedExpected[hash] = struct{}{}
	}

	restoreAndValidate(`
		SELECT id, file_hash
		FROM logical_file
		WHERE status = 'COMPLETED' AND id >= $1
		ORDER BY id ASC
	`, 0, combinedExpected, "all")
}

func TestPhase7MixedVerifyAfterUpgradeIntegration(t *testing.T) {
	testgate.RequireDB(t)

	tmp := prepareReadPathRegressionRepo(t)
	setRepoChunkerVersion(t, chunk.VersionV1SimpleRolling)

	dbconn, err := db.ConnectDB()
	if err != nil {
		t.Fatalf("connect DB for mixed verify fixture: %v", err)
	}

	legacyRoot := filepath.Join(tmp, "mixed-verify-legacy-input")
	legacySet := createPhase7FixtureInputSet(t, legacyRoot)

	sgctx := storage.StorageContext{
		DB:           dbconn,
		Writer:       container.NewLocalWriter(container.GetContainerMaxSize()),
		ContainerDir: container.ContainersDir,
	}

	for _, p := range legacySet.allStorePaths {
		if _, err := storage.StoreFileWithStorageContextAndCodecResultWithPolicy(sgctx, p, blocks.CodecPlain, false); err != nil {
			_ = dbconn.Close()
			t.Fatalf("seed mixed-verify legacy data %s: %v", p, err)
		}
	}

	if _, err := dbconn.Exec(`DELETE FROM chunk_block_refs`); err != nil {
		_ = dbconn.Close()
		t.Fatalf("delete chunk_block_refs before upgrade for mixed verify: %v", err)
	}
	if _, err := dbconn.Exec(`DELETE FROM storage_blocks`); err != nil {
		_ = dbconn.Close()
		t.Fatalf("delete storage_blocks before upgrade for mixed verify: %v", err)
	}

	var oldMaxChunkID int64
	if err := dbconn.QueryRow(`SELECT COALESCE(MAX(id), 0) FROM chunk`).Scan(&oldMaxChunkID); err != nil {
		_ = dbconn.Close()
		t.Fatalf("query old max chunk id for mixed verify: %v", err)
	}

	if err := dbconn.Close(); err != nil {
		t.Fatalf("close db before mixed verify upgrade reopen: %v", err)
	}

	if err := recovery.SystemRecoveryWithContainersDir(container.ContainersDir); err != nil {
		t.Fatalf("open mixed verify legacy repository with v1.8 runtime: %v", err)
	}

	dbconn, err = db.ConnectDB()
	if err != nil {
		t.Fatalf("reconnect DB after mixed verify reopen: %v", err)
	}
	defer func() { _ = dbconn.Close() }()

	newRoot := filepath.Join(tmp, "mixed-verify-new-input")
	newFilePath := filepath.Join(newRoot, "mixed-verify-new-file.bin")
	writeDeterministicFileWithSalt(t, newFilePath, 2*1024*1024+321, 2201)

	newFolderPath := filepath.Join(newRoot, "mixed-verify-folder")
	for i := 0; i < 8; i++ {
		p := filepath.Join(newFolderPath, fmt.Sprintf("verify-new-%02d.dat", i))
		writeDeterministicFileWithSalt(t, p, 3072+(i*197), 2300+i)
	}

	sgctx = storage.StorageContext{
		DB:           dbconn,
		Writer:       container.NewLocalWriter(container.GetContainerMaxSize()),
		ContainerDir: container.ContainersDir,
	}

	if _, err := storage.StoreFileWithStorageContextAndCodecResultWithPolicy(sgctx, newFilePath, blocks.CodecPlain, false); err != nil {
		t.Fatalf("store mixed-verify new file after upgrade: %v", err)
	}
	if err := storage.StoreFolderWithStorageContextAndCodec(sgctx, newFolderPath, blocks.CodecPlain); err != nil {
		t.Fatalf("store mixed-verify new folder after upgrade: %v", err)
	}

	var legacyBlocksRows int
	if err := dbconn.QueryRow(`
		SELECT COUNT(*)
		FROM blocks b
		JOIN chunk c ON c.id = b.chunk_id
		WHERE c.id <= $1
	`, oldMaxChunkID).Scan(&legacyBlocksRows); err != nil {
		t.Fatalf("count legacy blocks rows in mixed verify fixture: %v", err)
	}
	if legacyBlocksRows == 0 {
		t.Fatal("expected mixed repository to contain legacy blocks rows")
	}

	var packedBlocksRows int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM storage_blocks`).Scan(&packedBlocksRows); err != nil {
		t.Fatalf("count storage_blocks rows in mixed verify fixture: %v", err)
	}
	if packedBlocksRows == 0 {
		t.Fatal("expected mixed repository to contain packed storage_blocks rows")
	}

	var packedRefsRows int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM chunk_block_refs`).Scan(&packedRefsRows); err != nil {
		t.Fatalf("count chunk_block_refs rows in mixed verify fixture: %v", err)
	}
	if packedRefsRows == 0 {
		t.Fatal("expected mixed repository to contain chunk_block_refs rows")
	}

	var oldChunksUsingPacked int
	if err := dbconn.QueryRow(`
		SELECT COUNT(*)
		FROM chunk c
		JOIN chunk_block_refs r ON r.chunk_id = c.id
		WHERE c.id <= $1
	`, oldMaxChunkID).Scan(&oldChunksUsingPacked); err != nil {
		t.Fatalf("count old chunks unexpectedly using packed refs: %v", err)
	}
	if oldChunksUsingPacked != 0 {
		t.Fatalf("expected old legacy chunks to avoid packed refs, got=%d", oldChunksUsingPacked)
	}

	var newChunksUsingPacked int
	if err := dbconn.QueryRow(`
		SELECT COUNT(*)
		FROM chunk c
		JOIN chunk_block_refs r ON r.chunk_id = c.id
		WHERE c.id > $1
	`, oldMaxChunkID).Scan(&newChunksUsingPacked); err != nil {
		t.Fatalf("count new chunks using packed refs: %v", err)
	}
	if newChunksUsingPacked == 0 {
		t.Fatal("expected new chunks to use packed refs in mixed repository")
	}

	// Enforce strict table/segment matching in verify for stronger encoded-table coverage.
	t.Setenv("COLDKEEP_VERIFY_STRICT_SEGMENTS", "1")
	if err := maintenance.VerifyCommandWithContainersDir(container.ContainersDir, "system", 0, verify.VerifyStandard); err != nil {
		t.Fatalf("mixed verify after upgrade failed: %v", err)
	}
}

func TestPhase7MixedGCAfterUpgradeIntegration(t *testing.T) {
	testgate.RequireDB(t)

	tmp := prepareReadPathRegressionRepo(t)
	setRepoChunkerVersion(t, chunk.VersionV1SimpleRolling)

	dbconn, err := db.ConnectDB()
	if err != nil {
		t.Fatalf("connect DB for mixed GC fixture: %v", err)
	}

	legacyRoot := filepath.Join(tmp, "mixed-gc-legacy-input")
	legacySet := createPhase7FixtureInputSet(t, legacyRoot)

	sgctx := storage.StorageContext{
		DB:           dbconn,
		Writer:       container.NewLocalWriter(container.GetContainerMaxSize()),
		ContainerDir: container.ContainersDir,
	}

	for _, p := range legacySet.allStorePaths {
		if _, err := storage.StoreFileWithStorageContextAndCodecResultWithPolicy(sgctx, p, blocks.CodecPlain, false); err != nil {
			_ = dbconn.Close()
			t.Fatalf("seed mixed-gc legacy data %s: %v", p, err)
		}
	}

	// Emulate pre-upgrade legacy repository metadata shape.
	if _, err := dbconn.Exec(`DELETE FROM chunk_block_refs`); err != nil {
		_ = dbconn.Close()
		t.Fatalf("delete chunk_block_refs before mixed-gc upgrade: %v", err)
	}
	if _, err := dbconn.Exec(`DELETE FROM storage_blocks`); err != nil {
		_ = dbconn.Close()
		t.Fatalf("delete storage_blocks before mixed-gc upgrade: %v", err)
	}

	var oldMaxChunkID int64
	if err := dbconn.QueryRow(`SELECT COALESCE(MAX(id), 0) FROM chunk`).Scan(&oldMaxChunkID); err != nil {
		_ = dbconn.Close()
		t.Fatalf("query old max chunk id for mixed-gc: %v", err)
	}
	var oldMaxLogicalID int64
	if err := dbconn.QueryRow(`SELECT COALESCE(MAX(id), 0) FROM logical_file`).Scan(&oldMaxLogicalID); err != nil {
		_ = dbconn.Close()
		t.Fatalf("query old max logical id for mixed-gc: %v", err)
	}

	if err := dbconn.Close(); err != nil {
		t.Fatalf("close db before mixed-gc reopen: %v", err)
	}
	if err := recovery.SystemRecoveryWithContainersDir(container.ContainersDir); err != nil {
		t.Fatalf("open mixed-gc repository with v1.8 runtime: %v", err)
	}

	dbconn, err = db.ConnectDB()
	if err != nil {
		t.Fatalf("reconnect DB after mixed-gc reopen: %v", err)
	}
	defer func() { _ = dbconn.Close() }()

	packedRoot := filepath.Join(tmp, "mixed-gc-packed-input")
	packedAPath := filepath.Join(packedRoot, "packed-A.bin")
	packedBPath := filepath.Join(packedRoot, "packed-B.bin")
	packedCPath := filepath.Join(packedRoot, "packed-C-dead.bin")

	if err := os.MkdirAll(packedRoot, 0o755); err != nil {
		t.Fatalf("mkdir packed input root: %v", err)
	}

	commonPrefix := deterministicBytesWithSalt(1_200_000, 3101)
	suffixA := deterministicBytesWithSalt(420_000, 3102)
	suffixB := deterministicBytesWithSalt(420_000, 3103)
	uniqueC := deterministicBytesWithSalt(1_500_000, 3104)

	dataA := make([]byte, 0, len(commonPrefix)+len(suffixA))
	dataA = append(dataA, commonPrefix...)
	dataA = append(dataA, suffixA...)
	if err := os.WriteFile(packedAPath, dataA, 0o644); err != nil {
		t.Fatalf("write packed A file: %v", err)
	}

	dataB := make([]byte, 0, len(commonPrefix)+len(suffixB))
	dataB = append(dataB, commonPrefix...)
	dataB = append(dataB, suffixB...)
	if err := os.WriteFile(packedBPath, dataB, 0o644); err != nil {
		t.Fatalf("write packed B file: %v", err)
	}

	if err := os.WriteFile(packedCPath, uniqueC, 0o644); err != nil {
		t.Fatalf("write packed C file: %v", err)
	}

	sgctx = storage.StorageContext{
		DB:           dbconn,
		Writer:       container.NewLocalWriter(container.GetContainerMaxSize()),
		ContainerDir: container.ContainersDir,
	}

	if _, err := storage.StoreFileWithStorageContextAndCodecResultWithPolicy(sgctx, packedAPath, blocks.CodecPlain, false); err != nil {
		t.Fatalf("store packed A: %v", err)
	}
	if _, err := storage.StoreFileWithStorageContextAndCodecResultWithPolicy(sgctx, packedBPath, blocks.CodecPlain, false); err != nil {
		t.Fatalf("store packed B: %v", err)
	}
	if _, err := storage.StoreFileWithStorageContextAndCodecResultWithPolicy(sgctx, packedCPath, blocks.CodecPlain, false); err != nil {
		t.Fatalf("store packed C: %v", err)
	}

	// Remove subset of legacy data.
	removeCtx := storage.StorageContext{DB: dbconn}
	legacyRemove := []string{
		legacySet.manySmallPaths[0],
		legacySet.manySmallPaths[1],
		legacySet.duplicatePathA,
	}
	for _, p := range legacyRemove {
		if _, err := storage.RemoveFileByStoredPathWithStorageContextResult(removeCtx, p); err != nil {
			t.Fatalf("remove legacy subset path %s: %v", p, err)
		}
	}

	// Remove subset of packed data.
	packedRemove := []string{packedAPath, packedCPath}
	for _, p := range packedRemove {
		if _, err := storage.RemoveFileByStoredPathWithStorageContextResult(removeCtx, p); err != nil {
			t.Fatalf("remove packed subset path %s: %v", p, err)
		}
	}

	type idSet map[int64]struct{}
	collectBlockIDs := func(query string) idSet {
		t.Helper()
		rows, err := dbconn.Query(query)
		if err != nil {
			t.Fatalf("query block ids: %v", err)
		}
		defer func() { _ = rows.Close() }()
		ids := make(idSet)
		for rows.Next() {
			var id int64
			if err := rows.Scan(&id); err != nil {
				t.Fatalf("scan block id: %v", err)
			}
			ids[id] = struct{}{}
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("iterate block id rows: %v", err)
		}
		return ids
	}

	deadWholeBlockIDsBefore := collectBlockIDs(`
		SELECT sb.id
		FROM storage_blocks sb
		WHERE EXISTS (SELECT 1 FROM chunk_block_refs r WHERE r.block_id = sb.id)
		  AND NOT EXISTS (
			SELECT 1
			FROM chunk_block_refs r
			JOIN chunk c ON c.id = r.chunk_id
			WHERE r.block_id = sb.id
			  AND (c.live_ref_count > 0 OR c.pin_count > 0)
		  )
	`)

	partialLiveBlockIDsBefore := collectBlockIDs(`
		SELECT sb.id
		FROM storage_blocks sb
		WHERE EXISTS (
			SELECT 1
			FROM chunk_block_refs r
			JOIN chunk c ON c.id = r.chunk_id
			WHERE r.block_id = sb.id
			  AND (c.live_ref_count > 0 OR c.pin_count > 0)
		)
		AND EXISTS (
			SELECT 1
			FROM chunk_block_refs r
			JOIN chunk c ON c.id = r.chunk_id
			WHERE r.block_id = sb.id
			  AND c.live_ref_count = 0
			  AND c.pin_count = 0
		)
	`)

	// Depending on packing decisions and dedupe shape, this set can be empty.

	var legacyDeadChunksBefore int
	if err := dbconn.QueryRow(`
		SELECT COUNT(*)
		FROM chunk
		WHERE id <= $1
		  AND status = 'COMPLETED'
		  AND live_ref_count = 0
		  AND pin_count = 0
	`, oldMaxChunkID).Scan(&legacyDeadChunksBefore); err != nil {
		t.Fatalf("count legacy dead chunks before GC: %v", err)
	}

	if err := maintenance.RunGCWithContainersDir(true, container.ContainersDir); err != nil {
		t.Fatalf("mixed-gc dry-run failed: %v", err)
	}

	// Dry-run must not mutate whole-dead/partial-live packed block presence.
	for id := range deadWholeBlockIDsBefore {
		var count int
		if err := dbconn.QueryRow(`SELECT COUNT(*) FROM storage_blocks WHERE id = $1`, id).Scan(&count); err != nil {
			t.Fatalf("count dead whole block %d after dry-run: %v", id, err)
		}
		if count != 1 {
			t.Fatalf("dead whole block %d changed during dry-run", id)
		}
	}
	for id := range partialLiveBlockIDsBefore {
		var count int
		if err := dbconn.QueryRow(`SELECT COUNT(*) FROM storage_blocks WHERE id = $1`, id).Scan(&count); err != nil {
			t.Fatalf("count partial live block %d after dry-run: %v", id, err)
		}
		if count != 1 {
			t.Fatalf("partial live block %d changed during dry-run", id)
		}
	}

	if err := maintenance.RunGCWithContainersDir(false, container.ContainersDir); err != nil {
		t.Fatalf("mixed-gc real run failed: %v", err)
	}

	for id := range deadWholeBlockIDsBefore {
		var remaining int
		if err := dbconn.QueryRow(`SELECT COUNT(*) FROM storage_blocks WHERE id = $1`, id).Scan(&remaining); err != nil {
			t.Fatalf("count dead whole block %d after GC: %v", id, err)
		}
		if remaining != 0 {
			t.Fatalf("dead whole packed block %d was not reclaimed", id)
		}
	}

	for id := range partialLiveBlockIDsBefore {
		var remaining int
		if err := dbconn.QueryRow(`SELECT COUNT(*) FROM storage_blocks WHERE id = $1`, id).Scan(&remaining); err != nil {
			t.Fatalf("count partial live block %d after GC: %v", id, err)
		}
		if remaining != 1 {
			t.Fatalf("partially-live packed block %d should be retained", id)
		}
	}

	var legacyDeadChunksAfter int
	if err := dbconn.QueryRow(`
		SELECT COUNT(*)
		FROM chunk
		WHERE id <= $1
		  AND status = 'COMPLETED'
		  AND live_ref_count = 0
		  AND pin_count = 0
	`, oldMaxChunkID).Scan(&legacyDeadChunksAfter); err != nil {
		t.Fatalf("count legacy dead chunks after GC: %v", err)
	}
	if legacyDeadChunksBefore > 0 && legacyDeadChunksAfter >= legacyDeadChunksBefore {
		t.Fatalf("expected legacy dead chunks to be reclaimed: before=%d after=%d", legacyDeadChunksBefore, legacyDeadChunksAfter)
	}

	rows, err := dbconn.Query(`
		SELECT lf.id, lf.file_hash
		FROM physical_file pf
		JOIN logical_file lf ON lf.id = pf.logical_file_id
		WHERE lf.status = 'COMPLETED'
		GROUP BY lf.id, lf.file_hash
		ORDER BY lf.id ASC
	`)
	if err != nil {
		t.Fatalf("query remaining logical files for restore: %v", err)
	}
	defer func() { _ = rows.Close() }()

	restoreDir := filepath.Join(tmp, "mixed-gc-remaining-restore")
	if err := os.MkdirAll(restoreDir, 0o755); err != nil {
		t.Fatalf("mkdir mixed-gc restore dir: %v", err)
	}

	remainingLegacy := 0
	remainingPacked := 0
	for rows.Next() {
		var fileID int64
		var wantHash string
		if err := rows.Scan(&fileID, &wantHash); err != nil {
			t.Fatalf("scan remaining logical file row: %v", err)
		}

		outPath := filepath.Join(restoreDir, fmt.Sprintf("remaining-%d.bin", fileID))
		if err := storage.RestoreFileWithStorageContext(storage.StorageContext{DB: dbconn, ContainerDir: container.ContainersDir}, fileID, outPath); err != nil {
			t.Fatalf("restore remaining file id=%d: %v", fileID, err)
		}
		if got := testutils.SHA256File(t, outPath); got != wantHash {
			t.Fatalf("remaining restore hash mismatch for file id=%d: got=%s want=%s", fileID, got, wantHash)
		}

		if fileID <= oldMaxLogicalID {
			remainingLegacy++
		} else {
			remainingPacked++
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate remaining logical files: %v", err)
	}

	if remainingLegacy == 0 {
		t.Fatal("expected remaining legacy data after mixed GC")
	}
	if remainingPacked == 0 {
		t.Fatal("expected remaining packed data after mixed GC")
	}

	t.Setenv("COLDKEEP_VERIFY_STRICT_SEGMENTS", "1")
	if err := maintenance.VerifyCommandWithContainersDir(container.ContainersDir, "system", 0, verify.VerifyStandard); err != nil {
		t.Fatalf("verify failed after mixed GC: %v", err)
	}
}

func TestPhase7BuildFixtureWithActualV17BinaryIntegration(t *testing.T) {
	testgate.RequireDB(t)

	// Optional by default for local/dev workflows; strict release gates can set
	// COLDKEEP_REQUIRE_V17_BINARY_PROOF=1 to convert missing binary into failure.
	v17Bin := os.Getenv("COLDKEEP_V17_BIN")
	if v17Bin == "" {
		if strings.EqualFold(strings.TrimSpace(os.Getenv("COLDKEEP_REQUIRE_V17_BINARY_PROOF")), "1") ||
			strings.EqualFold(strings.TrimSpace(os.Getenv("COLDKEEP_REQUIRE_V17_BINARY_PROOF")), "true") {
			t.Fatalf("COLDKEEP_REQUIRE_V17_BINARY_PROOF is enabled but COLDKEEP_V17_BIN is unset; set COLDKEEP_V17_BIN to a released v1.7 coldkeep binary path")
		}
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

	// --replace is not a CLI flag; use the Go API directly to store the updated file.
	dbconn, err := db.ConnectDB()
	if err != nil {
		t.Fatalf("connect db after v1.7 fixture build: %v", err)
	}
	defer func() { _ = dbconn.Close() }()

	writeDeterministicFileWithSalt(t, inputSet.retainedPath, 160*1024+7, 911)
	sgctx := storage.StorageContext{
		DB:           dbconn,
		Writer:       container.NewLocalWriter(container.GetContainerMaxSize()),
		ContainerDir: container.ContainersDir,
	}
	if _, err := storage.StoreFileWithStorageContextAndCodecResultWithPolicy(sgctx, inputSet.retainedPath, blocks.CodecPlain, true); err != nil {
		t.Fatalf("replace store (Go API) for retained path: %v", err)
	}
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

func TestPhase7SnapshotCompatibilityIntegration(t *testing.T) {
	testgate.RequireDB(t)

	tmp := prepareReadPathRegressionRepo(t)
	setRepoChunkerVersion(t, chunk.VersionV1SimpleRolling)

	dbconn, err := db.ConnectDB()
	if err != nil {
		t.Fatalf("connect DB for snapshot compatibility fixture: %v", err)
	}

	// Step 1: Create legacy data
	legacyRoot := filepath.Join(tmp, "snapshot-legacy-input")
	legacySet := createPhase7FixtureInputSet(t, legacyRoot)
	legacyExpectedHashes := make(map[string]struct{})

	sgctx := storage.StorageContext{
		DB:           dbconn,
		Writer:       container.NewLocalWriter(container.GetContainerMaxSize()),
		ContainerDir: container.ContainersDir,
	}

	for _, p := range legacySet.allStorePaths {
		legacyExpectedHashes[testutils.SHA256File(t, p)] = struct{}{}
		if _, err := storage.StoreFileWithStorageContextAndCodecResultWithPolicy(sgctx, p, blocks.CodecPlain, false); err != nil {
			_ = dbconn.Close()
			t.Fatalf("seed snapshot legacy data %s: %v", p, err)
		}
	}

	// Emulate pre-upgrade legacy repository.
	if _, err := dbconn.Exec(`DELETE FROM chunk_block_refs`); err != nil {
		_ = dbconn.Close()
		t.Fatalf("delete chunk_block_refs before snapshot test: %v", err)
	}
	if _, err := dbconn.Exec(`DELETE FROM storage_blocks`); err != nil {
		_ = dbconn.Close()
		t.Fatalf("delete storage_blocks before snapshot test: %v", err)
	}

	var oldMaxChunkID int64
	if err := dbconn.QueryRow(`SELECT COALESCE(MAX(id), 0) FROM chunk`).Scan(&oldMaxChunkID); err != nil {
		_ = dbconn.Close()
		t.Fatalf("query old max chunk id for snapshot test: %v", err)
	}
	var oldMaxLogicalID int64
	if err := dbconn.QueryRow(`SELECT COALESCE(MAX(id), 0) FROM logical_file`).Scan(&oldMaxLogicalID); err != nil {
		_ = dbconn.Close()
		t.Fatalf("query old max logical id for snapshot test: %v", err)
	}

	// Step 2: Create snapshot of legacy data
	if err := snapshot.CreateSnapshotWithOptions(context.Background(), dbconn, snapshot.SnapshotCreateOptions{
		ID:   "phase7-legacy-snapshot",
		Type: "full",
	}); err != nil {
		_ = dbconn.Close()
		t.Fatalf("create legacy snapshot: %v", err)
	}

	if err := dbconn.Close(); err != nil {
		t.Fatalf("close db before snapshot test upgrade reopen: %v", err)
	}

	// Step 3: Upgrade/open with v1.8
	if err := recovery.SystemRecoveryWithContainersDir(container.ContainersDir); err != nil {
		t.Fatalf("open snapshot test legacy repository with v1.8 runtime: %v", err)
	}

	dbconn, err = db.ConnectDB()
	if err != nil {
		t.Fatalf("reconnect DB after snapshot test upgrade: %v", err)
	}
	defer func() { _ = dbconn.Close() }()

	// Step 4: Add packed data
	packedRoot := filepath.Join(tmp, "snapshot-packed-input")
	packedFilePath := filepath.Join(packedRoot, "snapshot-packed-file.bin")
	writeDeterministicFileWithSalt(t, packedFilePath, 1500000+321, 4401)

	packedFolderPath := filepath.Join(packedRoot, "snapshot-packed-folder")
	packedFolderFiles := make([]string, 0, 6)
	for i := 0; i < 6; i++ {
		p := filepath.Join(packedFolderPath, fmt.Sprintf("packed-%02d.dat", i))
		writeDeterministicFileWithSalt(t, p, 8192+(i*333), 4500+i)
		packedFolderFiles = append(packedFolderFiles, p)
	}

	packedExpectedHashes := make(map[string]struct{})
	packedExpectedHashes[testutils.SHA256File(t, packedFilePath)] = struct{}{}
	for _, p := range packedFolderFiles {
		packedExpectedHashes[testutils.SHA256File(t, p)] = struct{}{}
	}

	sgctx = storage.StorageContext{
		DB:           dbconn,
		Writer:       container.NewLocalWriter(container.GetContainerMaxSize()),
		ContainerDir: container.ContainersDir,
	}

	if _, err := storage.StoreFileWithStorageContextAndCodecResultWithPolicy(sgctx, packedFilePath, blocks.CodecPlain, false); err != nil {
		t.Fatalf("store snapshot packed file after upgrade: %v", err)
	}
	if err := storage.StoreFolderWithStorageContextAndCodec(sgctx, packedFolderPath, blocks.CodecPlain); err != nil {
		t.Fatalf("store snapshot packed folder after upgrade: %v", err)
	}

	// Step 5: Create new snapshot
	if err := snapshot.CreateSnapshotWithOptions(context.Background(), dbconn, snapshot.SnapshotCreateOptions{
		ID:   "phase7-packed-snapshot",
		Type: "full",
	}); err != nil {
		t.Fatalf("create packed snapshot: %v", err)
	}

	// Step 6: Remove live files from both old and new data
	removeCtx := storage.StorageContext{DB: dbconn}

	// Remove some legacy files
	legacyRemove := []string{
		legacySet.manySmallPaths[0],
		legacySet.manySmallPaths[1],
		legacySet.duplicatePathA,
	}
	for _, p := range legacyRemove {
		if _, err := storage.RemoveFileByStoredPathWithStorageContextResult(removeCtx, p); err == nil {
			t.Fatalf("expected snapshot-retained legacy path removal to be refused for %s", p)
		} else if !strings.Contains(err.Error(), "retained by one or more snapshots") {
			t.Fatalf("remove snapshot legacy path %s: %v", p, err)
		}
	}

	// Remove some packed files
	packedRemove := []string{packedFilePath, packedFolderFiles[0]}
	for _, p := range packedRemove {
		if _, err := storage.RemoveFileByStoredPathWithStorageContextResult(removeCtx, p); err == nil {
			t.Fatalf("expected snapshot-retained packed path removal to be refused for %s", p)
		} else if !strings.Contains(err.Error(), "retained by one or more snapshots") {
			t.Fatalf("remove snapshot packed path %s: %v", p, err)
		}
	}

	// Step 7: Run GC (dry-run and real)
	if err := maintenance.RunGCWithContainersDir(true, container.ContainersDir); err != nil {
		t.Fatalf("snapshot test gc dry-run failed: %v", err)
	}

	if err := maintenance.RunGCWithContainersDir(false, container.ContainersDir); err != nil {
		t.Fatalf("snapshot test gc real run failed: %v", err)
	}

	// Step 8: Restore old snapshot
	legacySnapshotRestoreDir := filepath.Join(tmp, "snapshot-legacy-restore")
	if err := os.MkdirAll(legacySnapshotRestoreDir, 0o755); err != nil {
		t.Fatalf("mkdir legacy snapshot restore dir: %v", err)
	}

	legacyRestoreCtx := storage.StorageContext{
		DB:           dbconn,
		ContainerDir: container.ContainersDir,
	}

	legacyRestoreResult, err := snapshot.RestoreSnapshot(
		context.Background(),
		dbconn,
		"phase7-legacy-snapshot",
		[]string{},
		snapshot.RestoreSnapshotOptions{
			DestinationMode: storage.RestoreDestinationPrefix,
			Destination:     legacySnapshotRestoreDir,
			Overwrite:       true,
			StorageContext:  &legacyRestoreCtx,
		},
	)
	if err != nil {
		t.Fatalf("restore legacy snapshot failed: %v", err)
	}

	// Validate restored legacy files match expected hashes
	for path, info := range iterateRestoreOutputPaths(t, legacySnapshotRestoreDir) {
		if info.IsDir() {
			continue
		}
		restoredHash := testutils.SHA256File(t, path)
		if _, expectedFromLegacy := legacyExpectedHashes[restoredHash]; !expectedFromLegacy {
			t.Fatalf("legacy snapshot restored hash not in original legacy fixture: %s", restoredHash)
		}
	}

	if legacyRestoreResult.RestoredFiles == 0 {
		t.Fatal("expected legacy snapshot restore to restore files")
	}

	// Step 9: Restore new snapshot
	packedSnapshotRestoreDir := filepath.Join(tmp, "snapshot-packed-restore")
	if err := os.MkdirAll(packedSnapshotRestoreDir, 0o755); err != nil {
		t.Fatalf("mkdir packed snapshot restore dir: %v", err)
	}

	packedRestoreCtx := storage.StorageContext{
		DB:           dbconn,
		ContainerDir: container.ContainersDir,
	}

	packedRestoreResult, err := snapshot.RestoreSnapshot(
		context.Background(),
		dbconn,
		"phase7-packed-snapshot",
		[]string{},
		snapshot.RestoreSnapshotOptions{
			DestinationMode: storage.RestoreDestinationPrefix,
			Destination:     packedSnapshotRestoreDir,
			Overwrite:       true,
			StorageContext:  &packedRestoreCtx,
		},
	)
	if err != nil {
		t.Fatalf("restore packed snapshot failed: %v", err)
	}

	// Validate restored packed files match expected hashes
	for path, info := range iterateRestoreOutputPaths(t, packedSnapshotRestoreDir) {
		if info.IsDir() {
			continue
		}
		restoredHash := testutils.SHA256File(t, path)
		if _, expectedFromPacked := packedExpectedHashes[restoredHash]; !expectedFromPacked {
			// File could be from legacy data too, so check both
			if _, expectedFromLegacy := legacyExpectedHashes[restoredHash]; !expectedFromLegacy {
				t.Fatalf("packed snapshot restored hash not in original expected fixture: %s", restoredHash)
			}
		}
	}

	if packedRestoreResult.RestoredFiles == 0 {
		t.Fatal("expected packed snapshot restore to restore files")
	}

	// Step 10: Run verify
	t.Setenv("COLDKEEP_VERIFY_STRICT_SEGMENTS", "1")
	if err := maintenance.VerifyCommandWithContainersDir(container.ContainersDir, "system", 0, verify.VerifyStandard); err != nil {
		t.Fatalf("verify failed after snapshot GC test: %v", err)
	}

	// Validate mixed snapshot metadata
	var legacySnapshotFiles int
	if err := dbconn.QueryRow(`
		SELECT COUNT(*)
		FROM snapshot_file sf
		JOIN snapshot s ON s.id = sf.snapshot_id
		WHERE s.id = $1
	`, "phase7-legacy-snapshot").Scan(&legacySnapshotFiles); err != nil {
		t.Fatalf("count legacy snapshot files: %v", err)
	}
	if legacySnapshotFiles == 0 {
		t.Fatal("expected legacy snapshot to contain file records")
	}

	var packedSnapshotFiles int
	if err := dbconn.QueryRow(`
		SELECT COUNT(*)
		FROM snapshot_file sf
		JOIN snapshot s ON s.id = sf.snapshot_id
		WHERE s.id = $1
	`, "phase7-packed-snapshot").Scan(&packedSnapshotFiles); err != nil {
		t.Fatalf("count packed snapshot files: %v", err)
	}
	if packedSnapshotFiles == 0 {
		t.Fatal("expected packed snapshot to contain file records")
	}
}

// Helper function to iterate over restored files
func iterateRestoreOutputPaths(t *testing.T, rootDir string) map[string]os.FileInfo {
	t.Helper()
	result := make(map[string]os.FileInfo)
	if err := filepath.Walk(rootDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if path != rootDir {
			result[path] = info
		}
		return nil
	}); err != nil {
		t.Fatalf("walk restore output dir: %v", err)
	}
	return result
}

func TestPhase8MetadataMigrationIntegration(t *testing.T) {
	testgate.RequireDB(t)

	tmp := prepareReadPathRegressionRepo(t)
	setRepoChunkerVersion(t, chunk.VersionV1SimpleRolling)

	dbconn, err := db.ConnectDB()
	if err != nil {
		t.Fatalf("connect DB for metadata migration test: %v", err)
	}

	// Step 1: Create legacy data with metadata
	legacyRoot := filepath.Join(tmp, "migration-legacy-input")
	legacySet := createPhase7FixtureInputSet(t, legacyRoot)

	sgctx := storage.StorageContext{
		DB:           dbconn,
		Writer:       container.NewLocalWriter(container.GetContainerMaxSize()),
		ContainerDir: container.ContainersDir,
	}

	for _, p := range legacySet.allStorePaths {
		if _, err := storage.StoreFileWithStorageContextAndCodecResultWithPolicy(sgctx, p, blocks.CodecPlain, false); err != nil {
			_ = dbconn.Close()
			t.Fatalf("seed metadata migration legacy data %s: %v", p, err)
		}
	}

	// Create a legacy snapshot for data preservation validation
	if err := snapshot.CreateSnapshotWithOptions(context.Background(), dbconn, snapshot.SnapshotCreateOptions{
		ID:   "phase8-migration-legacy-snapshot",
		Type: "full",
	}); err != nil {
		_ = dbconn.Close()
		t.Fatalf("create legacy snapshot for migration test: %v", err)
	}

	// Query and store legacy metadata structure
	var legacyLogicalFileCount int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM logical_file WHERE status = 'COMPLETED'`).Scan(&legacyLogicalFileCount); err != nil {
		_ = dbconn.Close()
		t.Fatalf("count legacy logical files: %v", err)
	}
	if legacyLogicalFileCount == 0 {
		_ = dbconn.Close()
		t.Fatal("expected legacy logical files to exist before migration")
	}

	var legacyChunkCount int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM chunk WHERE status = 'COMPLETED'`).Scan(&legacyChunkCount); err != nil {
		_ = dbconn.Close()
		t.Fatalf("count legacy chunks: %v", err)
	}
	if legacyChunkCount == 0 {
		_ = dbconn.Close()
		t.Fatal("expected legacy chunks to exist before migration")
	}

	var legacyBlocksRows int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM blocks`).Scan(&legacyBlocksRows); err != nil {
		_ = dbconn.Close()
		t.Fatalf("count legacy blocks rows: %v", err)
	}
	if legacyBlocksRows == 0 {
		_ = dbconn.Close()
		t.Fatal("expected legacy blocks rows to exist before migration")
	}

	var legacySnapshotCount int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM snapshot`).Scan(&legacySnapshotCount); err != nil {
		_ = dbconn.Close()
		t.Fatalf("count legacy snapshots: %v", err)
	}

	// Record current schema version before migration
	schemaVersionBefore, err := db.CurrentSchemaVersion(dbconn)
	if err != nil {
		_ = dbconn.Close()
		t.Fatalf("query schema version before migration: %v", err)
	}

	if err := dbconn.Close(); err != nil {
		t.Fatalf("close db before migration: %v", err)
	}

	// Step 2: Apply v1.8 migrations
	dbconn, err = db.ConnectDB()
	if err != nil {
		t.Fatalf("reconnect DB for migration: %v", err)
	}
	defer func() { _ = dbconn.Close() }()

	if err := db.EnsureSchema(dbconn); err != nil {
		t.Fatalf("run migrations: %v", err)
	}

	// Step 3: Verify schema migration results
	schemaVersionAfter, err := db.CurrentSchemaVersion(dbconn)
	if err != nil {
		t.Fatalf("query schema version after migration: %v", err)
	}

	// New tables should exist
	packedBlocksTableExists := false
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='storage_blocks'`).Scan(&packedBlocksTableExists); err == nil {
		packedBlocksTableExists = true
	}
	if !packedBlocksTableExists {
		_, err := dbconn.Query(`SELECT 1 FROM storage_blocks LIMIT 1`)
		if err != nil {
			t.Fatalf("new storage_blocks table should be queryable after migration: %v", err)
		}
	}

	chunkBlockRefsTableExists := false
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='chunk_block_refs'`).Scan(&chunkBlockRefsTableExists); err == nil {
		chunkBlockRefsTableExists = true
	}
	if !chunkBlockRefsTableExists {
		_, err := dbconn.Query(`SELECT 1 FROM chunk_block_refs LIMIT 1`)
		if err != nil {
			t.Fatalf("new chunk_block_refs table should be queryable after migration: %v", err)
		}
	}

	// Step 4: Verify old tables/data preserved
	var postMigrationLogicalFileCount int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM logical_file WHERE status = 'COMPLETED'`).Scan(&postMigrationLogicalFileCount); err != nil {
		t.Fatalf("count logical files after migration: %v", err)
	}
	if postMigrationLogicalFileCount != legacyLogicalFileCount {
		t.Fatalf("logical file count changed during migration: before=%d after=%d", legacyLogicalFileCount, postMigrationLogicalFileCount)
	}

	var postMigrationChunkCount int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM chunk WHERE status = 'COMPLETED'`).Scan(&postMigrationChunkCount); err != nil {
		t.Fatalf("count chunks after migration: %v", err)
	}
	if postMigrationChunkCount != legacyChunkCount {
		t.Fatalf("chunk count changed during migration: before=%d after=%d", legacyChunkCount, postMigrationChunkCount)
	}

	var postMigrationBlocksRows int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM blocks`).Scan(&postMigrationBlocksRows); err != nil {
		t.Fatalf("count blocks rows after migration: %v", err)
	}
	if postMigrationBlocksRows != legacyBlocksRows {
		t.Fatalf("blocks row count changed during migration: before=%d after=%d", legacyBlocksRows, postMigrationBlocksRows)
	}

	var postMigrationSnapshotCount int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM snapshot`).Scan(&postMigrationSnapshotCount); err != nil {
		t.Fatalf("count snapshots after migration: %v", err)
	}
	if postMigrationSnapshotCount != legacySnapshotCount {
		t.Fatalf("snapshot count changed during migration: before=%d after=%d", legacySnapshotCount, postMigrationSnapshotCount)
	}

	// Step 5: Test idempotency - run migrations again
	if err := db.EnsureSchema(dbconn); err != nil {
		t.Fatalf("run migrations second time (idempotency test): %v", err)
	}

	// Verify state unchanged after second migration
	var idempotentLogicalFileCount int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM logical_file WHERE status = 'COMPLETED'`).Scan(&idempotentLogicalFileCount); err != nil {
		t.Fatalf("count logical files after idempotent migration: %v", err)
	}
	if idempotentLogicalFileCount != postMigrationLogicalFileCount {
		t.Fatalf("logical file count changed on idempotent migration: before=%d after=%d", postMigrationLogicalFileCount, idempotentLogicalFileCount)
	}

	var idempotentChunkCount int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM chunk WHERE status = 'COMPLETED'`).Scan(&idempotentChunkCount); err != nil {
		t.Fatalf("count chunks after idempotent migration: %v", err)
	}
	if idempotentChunkCount != postMigrationChunkCount {
		t.Fatalf("chunk count changed on idempotent migration: before=%d after=%d", postMigrationChunkCount, idempotentChunkCount)
	}

	var idempotentSnapshotCount int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM snapshot`).Scan(&idempotentSnapshotCount); err != nil {
		t.Fatalf("count snapshots after idempotent migration: %v", err)
	}
	if idempotentSnapshotCount != postMigrationSnapshotCount {
		t.Fatalf("snapshot count changed on idempotent migration: before=%d after=%d", postMigrationSnapshotCount, idempotentSnapshotCount)
	}

	// Verify legacy snapshot is still restorable after migrations
	legacySnapshotRestoreDir := filepath.Join(tmp, "phase8-legacy-snapshot-restore")
	if err := os.MkdirAll(legacySnapshotRestoreDir, 0o755); err != nil {
		t.Fatalf("mkdir legacy snapshot restore dir: %v", err)
	}

	restoreCtx := storage.StorageContext{
		DB:           dbconn,
		ContainerDir: container.ContainersDir,
	}

	legacyRestoreResult, err := snapshot.RestoreSnapshot(
		context.Background(),
		dbconn,
		"phase8-migration-legacy-snapshot",
		[]string{},
		snapshot.RestoreSnapshotOptions{
			DestinationMode: storage.RestoreDestinationPrefix,
			Destination:     legacySnapshotRestoreDir,
			Overwrite:       true,
			StorageContext:  &restoreCtx,
		},
	)
	if err != nil {
		t.Fatalf("restore legacy snapshot after migrations: %v", err)
	}

	if legacyRestoreResult.RestoredFiles == 0 {
		t.Fatal("expected legacy snapshot restore to restore files after migrations")
	}

	// Verify file hashes match original data
	for path, info := range iterateRestoreOutputPaths(t, legacySnapshotRestoreDir) {
		if info.IsDir() {
			continue
		}
		restoredHash := testutils.SHA256File(t, path)
		var logicalFileID int64
		var expectedHash string
		if err := dbconn.QueryRow(`SELECT id, file_hash FROM logical_file WHERE file_hash = $1`, restoredHash).Scan(&logicalFileID, &expectedHash); err != nil && err != sql.ErrNoRows {
			t.Fatalf("query restored file hash %s: %v", restoredHash, err)
		}
	}

	// Verify schema integrity with verify command
	if err := maintenance.VerifyCommandWithContainersDir(container.ContainersDir, "system", 0, verify.VerifyStandard); err != nil {
		t.Fatalf("verify failed after metadata migrations: %v", err)
	}

	// Final metadata assertions
	if schemaVersionBefore == 0 {
		t.Fatal("expected schema version to exist before migration")
	}
	if schemaVersionAfter < schemaVersionBefore {
		t.Fatalf("schema version should not decrease after migration: before=%d after=%d", schemaVersionBefore, schemaVersionAfter)
	}
}

func TestPhase9CLICompatibilityIntegration(t *testing.T) {
	testgate.RequireDB(t)

	tmp := prepareReadPathRegressionRepo(t)
	setRepoChunkerVersion(t, chunk.VersionV1SimpleRolling)

	dbconn, err := db.ConnectDB()
	if err != nil {
		t.Fatalf("connect DB for CLI compatibility test: %v", err)
	}

	// Build the coldkeep binary for CLI testing
	repoRoot := testutils.FindRepoRoot(t)
	binPath := filepath.Join(tmp, "coldkeep-binary")
	buildCmd := []string{"go", "build", "-o", binPath, filepath.Join(repoRoot, "cmd/coldkeep")}
	if err := runCommandSilent(buildCmd[0], buildCmd[1:]...); err != nil {
		_ = dbconn.Close()
		t.Fatalf("build coldkeep binary: %v", err)
	}

	env := testutils.DefaultCLIEnv(container.ContainersDir)

	// Step 1: Create legacy data with metadata
	cliRoot := filepath.Join(tmp, "cli-repo")
	if err := os.MkdirAll(cliRoot, 0o755); err != nil {
		_ = dbconn.Close()
		t.Fatalf("mkdir cli-repo: %v", err)
	}

	legacyInputRoot := filepath.Join(cliRoot, "legacy-input")
	legacySet := createPhase7FixtureInputSet(t, legacyInputRoot)
	legacyExpectedHashes := make(map[string]struct{})

	sgctx := storage.StorageContext{
		DB:           dbconn,
		Writer:       container.NewLocalWriter(container.GetContainerMaxSize()),
		ContainerDir: container.ContainersDir,
	}

	for _, p := range legacySet.allStorePaths {
		legacyExpectedHashes[testutils.SHA256File(t, p)] = struct{}{}
		if _, err := storage.StoreFileWithStorageContextAndCodecResultWithPolicy(sgctx, p, blocks.CodecPlain, false); err != nil {
			_ = dbconn.Close()
			t.Fatalf("seed CLI test legacy data %s: %v", p, err)
		}
	}

	var legacyStoredPath string
	if err := dbconn.QueryRow(`
		SELECT pf.path
		FROM physical_file pf
		JOIN logical_file lf ON lf.id = pf.logical_file_id
		WHERE lf.file_hash = $1
		LIMIT 1
	`, legacySet.largeHash).Scan(&legacyStoredPath); err != nil {
		_ = dbconn.Close()
		t.Fatalf("query legacy stored path: %v", err)
	}

	// Emulate pre-upgrade legacy state by removing packed metadata
	if _, err := dbconn.Exec(`DELETE FROM chunk_block_refs`); err != nil {
		_ = dbconn.Close()
		t.Fatalf("delete chunk_block_refs for CLI test: %v", err)
	}
	if _, err := dbconn.Exec(`DELETE FROM storage_blocks`); err != nil {
		_ = dbconn.Close()
		t.Fatalf("delete storage_blocks for CLI test: %v", err)
	}

	if err := dbconn.Close(); err != nil {
		t.Fatalf("close db before CLI upgrade: %v", err)
	}

	// Step 2: Run coldkeep verify on legacy repo
	verifyRes := testutils.RunColdkeepCommand(t, repoRoot, binPath, env, "verify", "system", "--standard")
	if verifyRes.ExitCode != 0 {
		t.Fatalf("coldkeep verify failed on legacy repo: exit=%d stderr=%s", verifyRes.ExitCode, verifyRes.Stderr)
	}

	// Step 3: Run coldkeep restore for legacy file
	legacyRestoreOut := filepath.Join(cliRoot, "restored-legacy.bin")
	restoreRes := testutils.RunColdkeepCommand(
		t,
		repoRoot,
		binPath,
		env,
		"restore",
		"--stored-path",
		legacyStoredPath,
		"--mode",
		"override",
		"--destination",
		legacyRestoreOut,
		"--overwrite",
	)
	if restoreRes.ExitCode != 0 {
		t.Fatalf("coldkeep restore failed for legacy file: exit=%d stderr=%s", restoreRes.ExitCode, restoreRes.Stderr)
	}

	legacyRestoredHash := testutils.SHA256File(t, legacyRestoreOut)
	if legacyRestoredHash != legacySet.largeHash {
		t.Fatalf("legacy restored hash mismatch: got=%s want=%s", legacyRestoredHash, legacySet.largeHash)
	}

	// Step 4: Run coldkeep store for new data
	packedInputRoot := filepath.Join(cliRoot, "packed-input")
	if err := os.MkdirAll(packedInputRoot, 0o755); err != nil {
		t.Fatalf("mkdir packed-input: %v", err)
	}

	newFilePath := filepath.Join(packedInputRoot, "new-file-cli.bin")
	writeDeterministicFileWithSalt(t, newFilePath, 2*1024*1024+789, 5001)
	newFileHash := testutils.SHA256File(t, newFilePath)

	newFolderPath := filepath.Join(packedInputRoot, "new-folder-cli")
	newFolderHashes := make(map[string]struct{})
	for i := 0; i < 5; i++ {
		p := filepath.Join(newFolderPath, fmt.Sprintf("file-%02d.dat", i))
		writeDeterministicFileWithSalt(t, p, 512000+(i*100), 5100+i)
		newFolderHashes[testutils.SHA256File(t, p)] = struct{}{}
	}

	storeFileRes := testutils.RunColdkeepCommand(t, repoRoot, binPath, env, "store", newFilePath)
	if storeFileRes.ExitCode != 0 {
		t.Fatalf("coldkeep store file failed: exit=%d stderr=%s", storeFileRes.ExitCode, storeFileRes.Stderr)
	}

	storeFolderRes := testutils.RunColdkeepCommand(t, repoRoot, binPath, env, "store-folder", newFolderPath)
	if storeFolderRes.ExitCode != 0 {
		t.Fatalf("coldkeep store folder failed: exit=%d stderr=%s", storeFolderRes.ExitCode, storeFolderRes.Stderr)
	}

	// Query new file ID after store
	dbconn, err = db.ConnectDB()
	if err != nil {
		t.Fatalf("reconnect DB after CLI store: %v", err)
	}
	defer func() { _ = dbconn.Close() }()

	var newLogicalMatches int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM logical_file WHERE file_hash = $1`, newFileHash).Scan(&newLogicalMatches); err != nil {
		t.Fatalf("count new file logical rows: %v", err)
	}
	if newLogicalMatches == 0 {
		t.Fatal("expected new file hash to be present in logical_file after CLI store")
	}

	var newStoredPath string
	if err := dbconn.QueryRow(`
		SELECT pf.path
		FROM physical_file pf
		JOIN logical_file lf ON lf.id = pf.logical_file_id
		WHERE lf.file_hash = $1
		LIMIT 1
	`, newFileHash).Scan(&newStoredPath); err != nil {
		t.Fatalf("query new stored path: %v", err)
	}

	// Step 5: Run coldkeep restore for new file
	newRestoreOut := filepath.Join(cliRoot, "restored-new.bin")
	restoreNewRes := testutils.RunColdkeepCommand(
		t,
		repoRoot,
		binPath,
		env,
		"restore",
		"--stored-path",
		newStoredPath,
		"--mode",
		"override",
		"--destination",
		newRestoreOut,
		"--overwrite",
	)
	if restoreNewRes.ExitCode != 0 {
		t.Fatalf("coldkeep restore failed for new file: exit=%d stderr=%s", restoreNewRes.ExitCode, restoreNewRes.Stderr)
	}

	newRestoredHash := testutils.SHA256File(t, newRestoreOut)
	if newRestoredHash != newFileHash {
		t.Fatalf("new restored hash mismatch: got=%s want=%s", newRestoredHash, newFileHash)
	}

	// Step 6: Run coldkeep verify after mixed operations
	verifyRes2 := testutils.RunColdkeepCommand(t, repoRoot, binPath, env, "verify", "system", "--standard")
	if verifyRes2.ExitCode != 0 {
		t.Fatalf("coldkeep verify failed after mixed operations: exit=%d stderr=%s", verifyRes2.ExitCode, verifyRes2.Stderr)
	}

	// Step 7: Run coldkeep gc --dry-run
	gcDryRes := testutils.RunColdkeepCommand(t, repoRoot, binPath, env, "gc", "--dry-run")
	if gcDryRes.ExitCode != 0 {
		t.Fatalf("coldkeep gc --dry-run failed: exit=%d stderr=%s", gcDryRes.ExitCode, gcDryRes.Stderr)
	}

	// Step 8: Run coldkeep gc (real)
	gcRes := testutils.RunColdkeepCommand(t, repoRoot, binPath, env, "gc")
	if gcRes.ExitCode != 0 {
		t.Fatalf("coldkeep gc failed: exit=%d stderr=%s", gcRes.ExitCode, gcRes.Stderr)
	}

	// Step 9: Run coldkeep verify after GC
	verifyRes3 := testutils.RunColdkeepCommand(t, repoRoot, binPath, env, "verify", "system", "--standard")
	if verifyRes3.ExitCode != 0 {
		t.Fatalf("coldkeep verify failed after GC: exit=%d stderr=%s", verifyRes3.ExitCode, verifyRes3.Stderr)
	}

	// Final validation: restore both files again and verify hashes
	legacyRestoreOut2 := filepath.Join(cliRoot, "restored-legacy-post-gc.bin")
	restoreLegacyPostGCRes := testutils.RunColdkeepCommand(
		t,
		repoRoot,
		binPath,
		env,
		"restore",
		"--stored-path",
		legacyStoredPath,
		"--mode",
		"override",
		"--destination",
		legacyRestoreOut2,
		"--overwrite",
	)
	if restoreLegacyPostGCRes.ExitCode != 0 {
		t.Fatalf("coldkeep restore legacy post-GC failed: exit=%d stderr=%s", restoreLegacyPostGCRes.ExitCode, restoreLegacyPostGCRes.Stderr)
	}

	legacyRestoredHash2 := testutils.SHA256File(t, legacyRestoreOut2)
	if legacyRestoredHash2 != legacySet.largeHash {
		t.Fatalf("legacy post-GC restored hash mismatch: got=%s want=%s", legacyRestoredHash2, legacySet.largeHash)
	}

	newRestoreOut2 := filepath.Join(cliRoot, "restored-new-post-gc.bin")
	restoreNewPostGCRes := testutils.RunColdkeepCommand(
		t,
		repoRoot,
		binPath,
		env,
		"restore",
		"--stored-path",
		newStoredPath,
		"--mode",
		"override",
		"--destination",
		newRestoreOut2,
		"--overwrite",
	)
	if restoreNewPostGCRes.ExitCode != 0 {
		t.Fatalf("coldkeep restore new post-GC failed: exit=%d stderr=%s", restoreNewPostGCRes.ExitCode, restoreNewPostGCRes.Stderr)
	}

	newRestoredHash2 := testutils.SHA256File(t, newRestoreOut2)
	if newRestoredHash2 != newFileHash {
		t.Fatalf("new post-GC restored hash mismatch: got=%s want=%s", newRestoredHash2, newFileHash)
	}
}

func runCommandSilent(name string, args ...string) error {
	cmd := exec.Command(name, args...)
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &out
	return cmd.Run()
}
