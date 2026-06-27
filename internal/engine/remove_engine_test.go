package engine_test

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/franchoy/coldkeep/internal/blocks"
	"github.com/franchoy/coldkeep/internal/container"
	dbpkg "github.com/franchoy/coldkeep/internal/db"
	"github.com/franchoy/coldkeep/internal/engine"
	"github.com/franchoy/coldkeep/internal/invariants"
	"github.com/franchoy/coldkeep/internal/storage"
	verifypkg "github.com/franchoy/coldkeep/internal/verify"
	"github.com/franchoy/coldkeep/tests/utils/removestate"
	"github.com/franchoy/coldkeep/tests/utils/testgate"
)

type sharedChunkRemoveFixture struct {
	db            *sql.DB
	engine        *engine.DefaultEngine
	containerDir  string
	targetFileID  int64
	peerFileID    int64
	sharedChunkID int64
	beforeTarget  removestate.Snapshot
	beforePeer    removestate.Snapshot
}

func TestRemoveByIDThroughEngine(t *testing.T) {
	db, sgctx, stored := storeRemoveFixture(t, "remove-engine.txt", "phase9-remove")
	eng := newRemoveTestEngine(t, db, sgctx.ContainerDir)

	res, err := eng.Remove(context.Background(), engine.RemoveRequest{
		FileIDs:  []int64{stored.FileID},
		FailFast: true,
	})
	if err != nil {
		t.Fatalf("Remove: %v", err)
	}

	assertRemoveSuccess(t, res)
	assertLogicalFileRemoved(t, db, stored.FileID)
}

func TestRemoveByIDDryRunThroughEngine(t *testing.T) {
	db, sgctx, stored := storeRemoveFixture(t, "remove-dry-run.txt", "phase9-remove-dry-run")
	eng := newRemoveTestEngine(t, db, sgctx.ContainerDir)

	res, err := eng.Remove(context.Background(), engine.RemoveRequest{
		FileIDs:  []int64{stored.FileID},
		DryRun:   true,
		FailFast: true,
	})
	if err != nil {
		t.Fatalf("Remove dry-run: %v", err)
	}

	assertDryRunRemoveSuccess(t, res)
	assertLogicalFileStillExists(t, db, stored.FileID)
}

func TestRemoveByIDRetainedSnapshotFailsClosed(t *testing.T) {
	db, sgctx, stored := storeRemoveFixture(t, "retained-engine.txt", "phase10-remove-retained")
	seedRetainedSnapshotReference(t, db, stored.FileID)
	before := removestate.Capture(t, db, stored.FileID, sgctx.ContainerDir)

	eng := newRemoveTestEngine(t, db, sgctx.ContainerDir)
	res, err := eng.Remove(context.Background(), engine.RemoveRequest{
		FileIDs:  []int64{stored.FileID},
		FailFast: true,
	})
	if err != nil {
		t.Fatalf("Remove retained: %v", err)
	}

	assertRetainedSnapshotRemoveFailed(t, res)
	after := removestate.Capture(t, db, stored.FileID, sgctx.ContainerDir)
	removestate.AssertEqual(t, before, after)
}

func TestRemoveByIDDeletesLogicalIdentityAndAssociationsOnly(t *testing.T) {
	fixture := newSQLiteSharedChunkRemoveFixture(t)

	res, err := fixture.engine.Remove(context.Background(), engine.RemoveRequest{
		FileIDs:  []int64{fixture.targetFileID},
		FailFast: true,
	})
	if err != nil {
		t.Fatalf("Remove target by ID: %v", err)
	}
	assertRemoveSuccess(t, res)
	assertSharedChunkRemoveEffects(t, fixture, "peer verify after target remove")
}

func TestRemoveByIDPostgresPreservesSharedChunks(t *testing.T) {
	testgate.RequireDB(t)
	t.Setenv("COLDKEEP_DB_AUTO_BOOTSTRAP", "true")

	fixture := newPostgresSharedChunkRemoveFixture(t)

	res, err := fixture.engine.Remove(context.Background(), engine.RemoveRequest{
		FileIDs:  []int64{fixture.targetFileID},
		FailFast: true,
	})
	if err != nil {
		t.Fatalf("Remove postgres target by ID: %v", err)
	}
	assertRemoveSuccess(t, res)
	assertSharedChunkRemoveEffects(t, fixture, "postgres peer verify after target remove")
}

func newSQLiteSharedChunkRemoveFixture(t *testing.T) sharedChunkRemoveFixture {
	t.Helper()
	db := openSnapshotTestDB(t)
	return newSharedChunkRemoveFixture(t, db, t.TempDir(), "phase10-target.txt", "phase10-shared-payload", "phase10-peer.txt", "phase10-peer-hash")
}

func newPostgresSharedChunkRemoveFixture(t *testing.T) sharedChunkRemoveFixture {
	t.Helper()
	db := openTempPostgresEngineDatabase(t, "coldkeep_phase10_remove_by_id_pg")
	if err := dbpkg.EnsurePostgresSchema(db); err != nil {
		t.Fatalf("EnsurePostgresSchema: %v", err)
	}
	return newSharedChunkRemoveFixture(t, db, t.TempDir(), "phase10-pg-target.txt", "phase10-postgres-shared-payload", "phase10-pg-peer.txt", "phase10-pg-peer-hash")
}

func newSharedChunkRemoveFixture(t *testing.T, db *sql.DB, containerDir, targetFileName, payload, peerName, peerHash string) sharedChunkRemoveFixture {
	t.Helper()
	sgctx := storage.StorageContext{
		DB:           db,
		Writer:       container.NewLocalWriterWithDirAndDB(containerDir, container.GetContainerMaxSize(), db),
		ContainerDir: containerDir,
	}
	t.Cleanup(func() { _ = sgctx.Close() })

	target := storeSharedChunkTarget(t, sgctx, targetFileName, payload)
	sharedChunkID := fetchSharedChunkID(t, db, target.FileID)
	peerFileID := cloneSharedChunkPeer(t, db, target.FileID, sharedChunkID, peerName, peerHash, filepath.Join(t.TempDir(), peerName))

	beforeTarget := removestate.Capture(t, db, target.FileID, containerDir)
	beforePeer := removestate.Capture(t, db, peerFileID, containerDir)
	if len(beforeTarget.ContainerFiles) == 0 {
		t.Fatal("expected stored payload container files before remove")
	}

	return sharedChunkRemoveFixture{
		db:            db,
		engine:        newRemoveTestEngine(t, db, containerDir),
		containerDir:  containerDir,
		targetFileID:  target.FileID,
		peerFileID:    peerFileID,
		sharedChunkID: sharedChunkID,
		beforeTarget:  beforeTarget,
		beforePeer:    beforePeer,
	}
}

func storeSharedChunkTarget(t *testing.T, sgctx storage.StorageContext, filename, content string) storage.StoreFileResult {
	t.Helper()
	targetInput := filepath.Join(t.TempDir(), filename)
	if err := os.WriteFile(targetInput, []byte(content), 0o600); err != nil {
		t.Fatalf("write target input: %v", err)
	}
	target, err := storage.StoreFileWithStorageContextAndCodecResult(sgctx, targetInput, blocks.CodecPlain)
	if err != nil {
		t.Fatalf("store target fixture: %v", err)
	}
	return target
}

func fetchSharedChunkID(t *testing.T, db *sql.DB, fileID int64) int64 {
	t.Helper()
	var sharedChunkID int64
	if err := db.QueryRow(`
		SELECT fc.chunk_id
		FROM file_chunk fc
		WHERE fc.logical_file_id = $1
		ORDER BY fc.chunk_order ASC
		LIMIT 1
	`, fileID).Scan(&sharedChunkID); err != nil {
		t.Fatalf("query shared chunk id: %v", err)
	}
	return sharedChunkID
}

func cloneSharedChunkPeer(t *testing.T, db *sql.DB, sourceFileID, sharedChunkID int64, peerName, peerHash, peerStoredPath string) int64 {
	t.Helper()
	var peerFileID int64
	if err := db.QueryRow(
		`INSERT INTO logical_file (original_name, total_size, file_hash, status, ref_count, chunker_version)
		 SELECT $1, total_size, $2, status, $3, chunker_version
		 FROM logical_file
		 WHERE id = $4
		 RETURNING id`,
		peerName,
		peerHash,
		int64(1),
		sourceFileID,
	).Scan(&peerFileID); err != nil {
		t.Fatalf("insert peer logical file: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO file_chunk (logical_file_id, chunk_id, chunk_order) VALUES ($1, $2, 0)`, peerFileID, sharedChunkID); err != nil {
		t.Fatalf("insert peer file_chunk: %v", err)
	}
	if _, err := db.Exec(`UPDATE chunk SET live_ref_count = live_ref_count + 1 WHERE id = $1`, sharedChunkID); err != nil {
		t.Fatalf("increment shared chunk live_ref_count: %v", err)
	}
	if _, err := db.Exec(
		`INSERT INTO physical_file (path, logical_file_id, is_metadata_complete) VALUES ($1, $2, 0)`,
		peerStoredPath,
		peerFileID,
	); err != nil {
		t.Fatalf("insert peer physical mapping: %v", err)
	}
	return peerFileID
}

func assertSharedChunkRemoveEffects(t *testing.T, fixture sharedChunkRemoveFixture, verifyContext string) {
	t.Helper()
	assertRemovedTargetGraph(t, fixture.db, fixture.targetFileID)

	afterPeer := removestate.Capture(t, fixture.db, fixture.peerFileID, fixture.containerDir)
	if !afterPeer.LogicalFileExists {
		t.Fatalf("peer logical file unexpectedly removed: %+v", afterPeer)
	}
	if !reflect.DeepEqual(fixture.beforePeer.PhysicalPaths, afterPeer.PhysicalPaths) || !reflect.DeepEqual(fixture.beforePeer.FileChunkIDs, afterPeer.FileChunkIDs) {
		t.Fatalf("peer graph changed unexpectedly: before=%+v after=%+v", fixture.beforePeer, afterPeer)
	}
	if afterPeer.ChunkLiveRefs[fixture.sharedChunkID] != 1 {
		t.Fatalf("expected shared chunk live_ref_count=1 after target removal, got %d", afterPeer.ChunkLiveRefs[fixture.sharedChunkID])
	}
	if !reflect.DeepEqual(fixture.beforeTarget.ContainerFiles, afterPeer.ContainerFiles) {
		t.Fatalf("payload container files changed during remove: before=%v after=%v", fixture.beforeTarget.ContainerFiles, afterPeer.ContainerFiles)
	}
	if err := verifypkg.VerifyFileStandardWithContainersDir(fixture.db, int(fixture.peerFileID), fixture.containerDir); err != nil {
		t.Fatalf("%s: %v", verifyContext, err)
	}
}

func assertRemovedTargetGraph(t *testing.T, db *sql.DB, fileID int64) {
	t.Helper()
	if _, err := storage.GetLogicalFileInfoWithDB(db, fileID); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("expected target logical file removed, got err=%v", err)
	}
	var targetPhysicalCount int64
	if err := db.QueryRow(`SELECT COUNT(*) FROM physical_file WHERE logical_file_id = $1`, fileID).Scan(&targetPhysicalCount); err != nil {
		t.Fatalf("count target physical mappings: %v", err)
	}
	if targetPhysicalCount != 0 {
		t.Fatalf("expected target physical mappings removed, got %d", targetPhysicalCount)
	}
	var targetFileChunkCount int64
	if err := db.QueryRow(`SELECT COUNT(*) FROM file_chunk WHERE logical_file_id = $1`, fileID).Scan(&targetFileChunkCount); err != nil {
		t.Fatalf("count target file_chunk rows: %v", err)
	}
	if targetFileChunkCount != 0 {
		t.Fatalf("expected target file_chunk rows removed, got %d", targetFileChunkCount)
	}
}

func storeRemoveFixture(t *testing.T, filename, content string) (*sql.DB, storage.StorageContext, storage.StoreFileResult) {
	t.Helper()

	db := openSnapshotTestDB(t)
	sgctx := storage.StorageContext{
		DB:           db,
		Writer:       container.NewSimulatedWriter(1024 * 1024),
		ContainerDir: t.TempDir(),
	}

	inPath := filepath.Join(t.TempDir(), filename)
	if err := os.WriteFile(inPath, []byte(content), 0600); err != nil {
		t.Fatalf("write input: %v", err)
	}
	stored, err := storage.StoreFileWithStorageContextAndCodecResult(sgctx, inPath, blocks.CodecPlain)
	if err != nil {
		t.Fatalf("store fixture: %v", err)
	}

	return db, sgctx, stored
}

func newRemoveTestEngine(t *testing.T, db *sql.DB, containerDir string) *engine.DefaultEngine {
	t.Helper()

	eng, err := engine.New(engine.Config{DB: db, ContainerDir: containerDir})
	if err != nil {
		t.Fatalf("engine.New: %v", err)
	}
	return eng
}

func assertRemoveSuccess(t *testing.T, res engine.RemoveResult) {
	t.Helper()

	if res.Summary.OK != 1 || res.Summary.Failed != 0 {
		t.Fatalf("unexpected summary: %+v", res.Summary)
	}
	if len(res.Items) != 1 {
		t.Fatalf("expected one item, got %d", len(res.Items))
	}
	item := res.Items[0]
	if item.Status != engine.BatchItemOK {
		t.Fatalf("expected status ok, got %q", item.Status)
	}
	if item.RemovedChunkAssociations <= 0 {
		t.Fatalf("expected RemovedChunkAssociations > 0, got %d", item.RemovedChunkAssociations)
	}
	if !item.LogicalFileRemoved {
		t.Fatalf("expected LogicalFileRemoved=true")
	}
}

func assertDryRunRemoveSuccess(t *testing.T, res engine.RemoveResult) {
	t.Helper()

	if len(res.Items) != 1 || res.Items[0].Status != engine.BatchItemOK {
		t.Fatalf("unexpected item result: %+v", res.Items)
	}
}

func assertLogicalFileRemoved(t *testing.T, db *sql.DB, fileID int64) {
	t.Helper()

	_, err := storage.GetLogicalFileInfoWithDB(db, fileID)
	if !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("expected removed logical file, got err=%v", err)
	}
}

func assertLogicalFileStillExists(t *testing.T, db *sql.DB, fileID int64) {
	t.Helper()

	_, err := storage.GetLogicalFileInfoWithDB(db, fileID)
	if err != nil {
		t.Fatalf("expected logical file to remain after dry-run, got %v", err)
	}
}

func seedRetainedSnapshotReference(t *testing.T, db *sql.DB, retainedID int64) {
	t.Helper()

	ctx := context.Background()
	execRetainedSnapshotSQL(t, db, ctx,
		`INSERT INTO snapshot (id, created_at, type, label) VALUES ($1, $2, $3, $4)`,
		"snap-retained", time.Now().UTC().Format(time.RFC3339), "full", "retained")
	execRetainedSnapshotSQL(t, db, ctx,
		`INSERT INTO snapshot_path (id, path) VALUES ($1, $2)`,
		int64(901), "retained.txt")
	execRetainedSnapshotSQL(t, db, ctx,
		`INSERT INTO snapshot_file (snapshot_id, path_id, logical_file_id, size) VALUES ($1, $2, $3, $4)`,
		"snap-retained", int64(901), retainedID, int64(10))
}

func execRetainedSnapshotSQL(t *testing.T, db *sql.DB, ctx context.Context, query string, args ...any) {
	t.Helper()

	if _, err := db.ExecContext(ctx, query, args...); err != nil {
		t.Fatalf("seed retained snapshot fixture: %v", err)
	}
}

func assertRetainedSnapshotRemoveFailed(t *testing.T, res engine.RemoveResult) {
	t.Helper()

	if len(res.Items) != 1 {
		t.Fatalf("expected one item, got %d", len(res.Items))
	}
	item := res.Items[0]
	if item.Status != engine.BatchItemFailed {
		t.Fatalf("expected failed status, got %q", item.Status)
	}
	if item.InvariantCode != invariants.CodeSnapshotRetainedDeleteBlocked {
		t.Fatalf("expected invariant code %q, got %q", invariants.CodeSnapshotRetainedDeleteBlocked, item.InvariantCode)
	}
	if item.RecommendedAction == "" {
		t.Fatalf("expected recommended action for retained snapshot failure")
	}
}
