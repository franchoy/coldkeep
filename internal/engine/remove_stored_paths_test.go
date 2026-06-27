package engine_test

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	dbpkg "github.com/franchoy/coldkeep/internal/db"
	"github.com/franchoy/coldkeep/internal/engine"
	"github.com/franchoy/coldkeep/internal/invariants"
)

type removeStoredPathFixture struct {
	db         *sql.DB
	engine     *engine.DefaultEngine
	logicalID  int64
	storedPath string
}

type removeStoredPathState struct {
	logicalExists  bool
	originalName   string
	refCount       int64
	physicalCount  int
	snapshotCount  int
	fileChunkCount int
	chunkLiveRefs  map[int64]int64
	chunkPinCounts map[int64]int64
}

func TestRemoveStoredPathsRefusesSnapshotRetainedMapping(t *testing.T) {
	fixture := newRemoveStoredPathFixture(t, []string{"snapshot-retained.txt"}, 1)
	seedSnapshotRetentionReference(t, fixture.db, fixture.logicalID, fixture.storedPath)
	before := queryRemoveStoredPathState(t, fixture.db, fixture.logicalID)

	result, err := fixture.engine.RemoveStoredPaths(context.Background(), engine.RemoveStoredPathsRequest{
		StoredPaths: []string{fixture.storedPath},
	})
	if err != nil {
		t.Fatalf("RemoveStoredPaths snapshot retained: %v", err)
	}

	item := result.Items[0]
	if item.Status != engine.BatchItemFailed || item.InvariantCode != invariants.CodeSnapshotRetainedDeleteBlocked || item.RecommendedAction == "" || item.MappingRemoved {
		t.Fatalf("unexpected retained refusal item: %+v", item)
	}
	after := queryRemoveStoredPathState(t, fixture.db, fixture.logicalID)
	assertRemoveStoredPathStateEqual(t, before, after)
}

func newRemoveStoredPathFixture(t *testing.T, names []string, refCount int64) removeStoredPathFixture {
	t.Helper()

	dbconn := openSnapshotTestDB(t)
	logicalID, paths := seedRemoveStoredPathFixture(t, dbconn, names, refCount)
	return removeStoredPathFixture{
		db:         dbconn,
		engine:     newRemoveTestEngine(t, dbconn, t.TempDir()),
		logicalID:  logicalID,
		storedPath: paths[0],
	}
}

func addStandaloneStoredPathFixture(t *testing.T, dbconn *sql.DB, name string) string {
	t.Helper()

	_, paths := seedRemoveStoredPathFixture(t, dbconn, []string{name}, 1)
	return paths[0]
}

func addStoredPathMapping(t *testing.T, dbconn *sql.DB, logicalID int64, name string) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), name)
	if _, err := dbconn.Exec(
		`INSERT INTO physical_file (path, logical_file_id, is_metadata_complete) VALUES ($1, $2, $3)`,
		path,
		logicalID,
		false,
	); err != nil {
		t.Fatalf("insert physical_file mapping: %v", err)
	}
	if _, err := dbconn.Exec(`UPDATE logical_file SET ref_count = ref_count + 1 WHERE id = $1`, logicalID); err != nil {
		t.Fatalf("increment logical_file.ref_count: %v", err)
	}
	return path
}

func seedRemoveStoredPathFixture(t *testing.T, dbconn *sql.DB, names []string, refCount int64) (int64, []string) {
	t.Helper()

	hashes := buildRemoveStoredPathFixtureHashes(names, refCount)
	logicalID := insertRemoveStoredPathLogicalFile(t, dbconn, names[0], hashes.logicalHash, refCount)

	var chunkID int64
	if err := dbconn.QueryRow(
		`INSERT INTO chunk (chunk_hash, size, status, live_ref_count, pin_count)
		 VALUES ($1, $2, $3, $4, $5) RETURNING id`,
		hashes.chunkHash,
		int64(8),
		"COMPLETED",
		1,
		0,
	).Scan(&chunkID); err != nil {
		t.Fatalf("insert chunk: %v", err)
	}
	if _, err := dbconn.Exec(`INSERT INTO file_chunk (logical_file_id, chunk_id, chunk_order) VALUES ($1, $2, 0)`, logicalID, chunkID); err != nil {
		t.Fatalf("insert file_chunk: %v", err)
	}

	paths := make([]string, 0, len(names))
	for _, name := range names {
		path := filepath.Join(t.TempDir(), name)
		paths = append(paths, path)
		if _, err := dbconn.Exec(
			`INSERT INTO physical_file (path, logical_file_id, is_metadata_complete) VALUES ($1, $2, $3)`,
			path,
			logicalID,
			false,
		); err != nil {
			t.Fatalf("insert physical_file row: %v", err)
		}
	}
	return logicalID, paths
}

func insertRemoveStoredPathLogicalFile(t *testing.T, dbconn *sql.DB, name, fileHash string, refCount int64) int64 {
	t.Helper()
	var logicalID int64
	if err := dbconn.QueryRow(
		`INSERT INTO logical_file (original_name, total_size, file_hash, status, ref_count, chunker_version)
		 VALUES ($1, $2, $3, $4, $5, 'v1-simple-rolling') RETURNING id`,
		name,
		int64(8),
		fileHash,
		"COMPLETED",
		refCount,
	).Scan(&logicalID); err != nil {
		t.Fatalf("insert logical_file: %v", err)
	}
	return logicalID
}

type removeStoredPathFixtureHashes struct {
	logicalHash string
	chunkHash   string
}

func buildRemoveStoredPathFixtureHashes(names []string, refCount int64) removeStoredPathFixtureHashes {
	base := removeStoredPathFixtureHashBase(names, refCount)
	return removeStoredPathFixtureHashes{
		logicalHash: base,
		chunkHash:   base + "_chunk",
	}
}

func removeStoredPathFixtureHashBase(names []string, refCount int64) string {
	var builder strings.Builder
	builder.WriteString("phase7-remove")
	for _, name := range names {
		builder.WriteByte('-')
		for _, r := range name {
			switch {
			case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
				builder.WriteRune(r)
			default:
				builder.WriteByte('_')
			}
		}
	}
	return fmt.Sprintf("%s-r%d", builder.String(), refCount)
}

func openFileBackedSQLiteDB(t *testing.T, dbPath string) *sql.DB {
	t.Helper()

	dbconn, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	if err := dbpkg.RunMigrations(dbconn); err != nil {
		t.Fatalf("run migrations: %v", err)
	}
	return dbconn
}

func assertLastStoredPathReopenState(t *testing.T, reopened *sql.DB, logicalID int64) {
	t.Helper()
	var refCount int64
	if err := reopened.QueryRow(`SELECT ref_count FROM logical_file WHERE id = $1`, logicalID).Scan(&refCount); err != nil {
		t.Fatalf("read ref_count after reopen: %v", err)
	}
	if refCount != 0 {
		t.Fatalf("expected ref_count=0 after reopen, got %d", refCount)
	}
	var physicalCount int64
	if err := reopened.QueryRow(`SELECT COUNT(*) FROM physical_file WHERE logical_file_id = $1`, logicalID).Scan(&physicalCount); err != nil {
		t.Fatalf("count physical_file after reopen: %v", err)
	}
	if physicalCount != 0 {
		t.Fatalf("expected zero physical mappings after reopen, got %d", physicalCount)
	}
	var migratedCount int64
	if err := reopened.QueryRow(`SELECT COUNT(*) FROM physical_file WHERE path LIKE '/migrated/%' AND logical_file_id = $1`, logicalID).Scan(&migratedCount); err != nil {
		t.Fatalf("count migrated mappings after reopen: %v", err)
	}
	if migratedCount != 0 {
		t.Fatalf("expected no migrated mapping resurrection, got %d", migratedCount)
	}
}

func queryRemoveStoredPathState(t *testing.T, dbconn *sql.DB, logicalID int64) removeStoredPathState {
	t.Helper()

	state := removeStoredPathState{
		chunkLiveRefs:  make(map[int64]int64),
		chunkPinCounts: make(map[int64]int64),
	}

	loadRemoveStoredPathLogicalState(t, dbconn, logicalID, &state)
	loadRemoveStoredPathCount(t, dbconn, `SELECT COUNT(*) FROM physical_file WHERE logical_file_id = $1`, logicalID, &state.physicalCount, "count physical_file rows")
	loadRemoveStoredPathCount(t, dbconn, `SELECT COUNT(*) FROM snapshot_file WHERE logical_file_id = $1`, logicalID, &state.snapshotCount, "count snapshot_file rows")
	loadRemoveStoredPathCount(t, dbconn, `SELECT COUNT(*) FROM file_chunk WHERE logical_file_id = $1`, logicalID, &state.fileChunkCount, "count file_chunk rows")
	loadRemoveStoredPathChunkState(t, dbconn, logicalID, &state)
	return state
}

func loadRemoveStoredPathLogicalState(t *testing.T, dbconn *sql.DB, logicalID int64, state *removeStoredPathState) {
	t.Helper()
	var logicalCount int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM logical_file WHERE id = $1`, logicalID).Scan(&logicalCount); err != nil {
		t.Fatalf("count logical_file rows: %v", err)
	}
	state.logicalExists = logicalCount == 1
	if !state.logicalExists {
		return
	}
	if err := dbconn.QueryRow(`SELECT original_name, ref_count FROM logical_file WHERE id = $1`, logicalID).Scan(&state.originalName, &state.refCount); err != nil {
		t.Fatalf("read logical_file state: %v", err)
	}
}

func loadRemoveStoredPathCount(t *testing.T, dbconn *sql.DB, query string, logicalID int64, dest *int, context string) {
	t.Helper()
	if err := dbconn.QueryRow(query, logicalID).Scan(dest); err != nil {
		t.Fatalf("%s: %v", context, err)
	}
}

func loadRemoveStoredPathChunkState(t *testing.T, dbconn *sql.DB, logicalID int64, state *removeStoredPathState) {
	t.Helper()
	rows, err := dbconn.Query(`
		SELECT c.id, c.live_ref_count, c.pin_count
		FROM file_chunk fc
		JOIN chunk c ON c.id = fc.chunk_id
		WHERE fc.logical_file_id = $1
		ORDER BY fc.chunk_order ASC
	`, logicalID)
	if err != nil {
		t.Fatalf("query chunk state: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var chunkID, liveRefCount, pinCount int64
		if err := rows.Scan(&chunkID, &liveRefCount, &pinCount); err != nil {
			t.Fatalf("scan chunk state: %v", err)
		}
		state.chunkLiveRefs[chunkID] = liveRefCount
		state.chunkPinCounts[chunkID] = pinCount
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate chunk state: %v", err)
	}
}

func assertRemoveStoredPathStateEqual(t *testing.T, before, after removeStoredPathState) {
	t.Helper()
	if !reflect.DeepEqual(before, after) {
		t.Fatalf("catalog state changed: before=%+v after=%+v", before, after)
	}
}

func assertRemoveStoredPathsValidationError(t *testing.T, result engine.RemoveStoredPathsResult, err error, want string) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected validation error %q", want)
	}
	if engine.IsUnsupported(err) {
		t.Fatalf("expected validation error to remain non-unsupported: %v", err)
	}
	if err.Error() != want {
		t.Fatalf("expected validation error %q, got %q", want, err.Error())
	}
	if result.DryRun || result.ExecutionMode != "" || len(result.Items) != 0 || result.Summary != (engine.BatchSummary{}) {
		t.Fatalf("expected zero result on validation failure, got %+v", result)
	}
}

func assertStoredPathStatuses(t *testing.T, items []engine.RemoveStoredPathItemResult, want []engine.BatchItemStatus) {
	t.Helper()
	if len(items) != len(want) {
		t.Fatalf("status length mismatch: got %d want %d (%+v)", len(items), len(want), items)
	}
	for i, status := range want {
		if items[i].Status != status {
			t.Fatalf("item %d status mismatch: got %q want %q (%+v)", i, items[i].Status, status, items[i])
		}
	}
}
