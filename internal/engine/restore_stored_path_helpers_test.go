package engine_test

import (
	"bytes"
	"database/sql"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"sync/atomic"
	"testing"

	dbpkg "github.com/franchoy/coldkeep/internal/db"

	_ "github.com/lib/pq"
)

var trustedPostgresIdentifierPattern = regexp.MustCompile(`^[A-Za-z0-9_]+$`)
var restorePostgresDatabaseSequence uint64

func assertRestoredBytes(t *testing.T, path string, want []byte) {
	t.Helper()
	got, err := readTrustedRestoreOutputBytes(path)
	if err != nil {
		t.Fatalf("read restored bytes: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("restored bytes mismatch: got=%q want=%q", string(got), string(want))
	}
}

func readTrustedRestoreOutputBytes(path string) ([]byte, error) {
	file, err := os.Open(filepath.Clean(path))
	if err != nil {
		return nil, err
	}
	defer file.Close()
	return io.ReadAll(file)
}

func expectedPrefixModeOutputPath(prefixRoot string, storedPath string) string {
	relativePath := storedPath
	if vol := filepath.VolumeName(relativePath); vol != "" {
		relativePath = strings.TrimPrefix(relativePath, vol)
	}
	relativePath = strings.TrimLeft(relativePath, `/\`)
	return filepath.Join(prefixRoot, relativePath)
}

func setPhysicalFileMetadataComplete(t *testing.T, db *sql.DB, fileID int64, complete bool) {
	t.Helper()
	if _, err := db.Exec(`UPDATE physical_file SET is_metadata_complete = $1, mode = NULL, mtime = NULL, uid = NULL, gid = NULL WHERE logical_file_id = $2`, complete, fileID); err != nil {
		t.Fatalf("update physical_file metadata completeness: %v", err)
	}
}

func seedSnapshotRetentionReference(t *testing.T, db *sql.DB, fileID int64, storedPath string) {
	t.Helper()

	snapshotID, pathID := snapshotRestoreReferenceIDs(fileID)
	insertRestoreSnapshotRow(t, db, snapshotID)
	insertRestoreSnapshotPathRow(t, db, pathID, storedPath)
	insertRestoreSnapshotFileRow(t, db, snapshotID, pathID, fileID, int64(len(storedPath)))
}

func insertRestoreSnapshotRow(t *testing.T, db *sql.DB, snapshotID string) {
	t.Helper()
	if _, err := insertRestoreSnapshotFixtureRow(db, snapshotID); err != nil {
		t.Fatalf("insert snapshot: %v", err)
	}
}

func insertRestoreSnapshotPathRow(t *testing.T, db *sql.DB, pathID int64, storedPath string) {
	t.Helper()
	if _, err := insertRestoreSnapshotPathFixtureRow(db, pathID, storedPath); err != nil {
		t.Fatalf("insert snapshot_path: %v", err)
	}
}

func insertRestoreSnapshotFileRow(t *testing.T, db *sql.DB, snapshotID string, pathID, fileID, size int64) {
	t.Helper()
	if _, err := insertRestoreSnapshotFileFixtureRow(db, snapshotID, pathID, fileID, size); err != nil {
		t.Fatalf("insert snapshot_file: %v", err)
	}
}

func snapshotRestoreReferenceIDs(fileID int64) (string, int64) {
	return fmt.Sprintf("snap-restore-%d", fileID), fileID*10 + 1
}

func snapshotRestoreCatalogState(t *testing.T, db *sql.DB, fileID int64) restoreCatalogState {
	t.Helper()

	state := restoreCatalogState{
		chunkLiveRefs:  make(map[int64]int64),
		chunkPinCounts: make(map[int64]int64),
	}
	loadRestoreLogicalState(t, db, fileID, &state)
	loadRestorePhysicalState(t, db, fileID, &state)
	loadRestoreSnapshotCount(t, db, fileID, &state)
	loadRestoreFileChunkCount(t, db, fileID, &state)
	loadRestoreChunkState(t, db, fileID, &state)
	return state
}

func loadRestoreLogicalState(t *testing.T, db *sql.DB, fileID int64, state *restoreCatalogState) {
	t.Helper()
	if err := db.QueryRow(`SELECT COUNT(*) FROM logical_file`).Scan(&state.logicalFileCount); err != nil {
		t.Fatalf("count logical_file rows: %v", err)
	}
	if err := db.QueryRow(`SELECT ref_count FROM logical_file WHERE id = $1`, fileID).Scan(&state.refCount); err != nil {
		t.Fatalf("read logical_file.ref_count: %v", err)
	}
}

func loadRestorePhysicalState(t *testing.T, db *sql.DB, fileID int64, state *restoreCatalogState) {
	t.Helper()
	if err := db.QueryRow(`SELECT COUNT(*) FROM physical_file WHERE logical_file_id = $1`, fileID).Scan(&state.physicalCount); err != nil {
		t.Fatalf("count physical_file rows: %v", err)
	}
	if state.physicalCount == 0 {
		return
	}
	if err := db.QueryRow(`SELECT path FROM physical_file WHERE logical_file_id = $1 ORDER BY path LIMIT 1`, fileID).Scan(&state.physicalPath); err != nil {
		t.Fatalf("read physical_file.path: %v", err)
	}
}

func loadRestoreSnapshotCount(t *testing.T, db *sql.DB, fileID int64, state *restoreCatalogState) {
	t.Helper()
	if err := db.QueryRow(`SELECT COUNT(*) FROM snapshot_file WHERE logical_file_id = $1`, fileID).Scan(&state.snapshotCount); err != nil {
		t.Fatalf("count snapshot_file rows: %v", err)
	}
}

func loadRestoreFileChunkCount(t *testing.T, db *sql.DB, fileID int64, state *restoreCatalogState) {
	t.Helper()
	if err := db.QueryRow(`SELECT COUNT(*) FROM file_chunk WHERE logical_file_id = $1`, fileID).Scan(&state.fileChunkCount); err != nil {
		t.Fatalf("count file_chunk rows: %v", err)
	}
}

func loadRestoreChunkState(t *testing.T, db *sql.DB, fileID int64, state *restoreCatalogState) {
	t.Helper()
	rows, err := db.Query(`
		SELECT c.id, c.live_ref_count, c.pin_count
		FROM file_chunk fc
		JOIN chunk c ON c.id = fc.chunk_id
		WHERE fc.logical_file_id = $1
		ORDER BY fc.chunk_order ASC
	`, fileID)
	if err != nil {
		t.Fatalf("query chunk refs: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var chunkID, liveRefCount, pinCount int64
		if err := rows.Scan(&chunkID, &liveRefCount, &pinCount); err != nil {
			t.Fatalf("scan chunk refs: %v", err)
		}
		state.chunkLiveRefs[chunkID] = liveRefCount
		state.chunkPinCounts[chunkID] = pinCount
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate chunk refs: %v", err)
	}
}

func assertRestoreCatalogStateEqual(t *testing.T, before, after restoreCatalogState) {
	t.Helper()
	assertRestoreLogicalStateEqual(t, before, after)
	assertRestorePhysicalStateEqual(t, before, after)
	assertRestoreSnapshotStateEqual(t, before, after)
	assertRestoreChunkStateEqual(t, before, after)
}

func assertRestoreLogicalStateEqual(t *testing.T, before, after restoreCatalogState) {
	t.Helper()
	if before.logicalFileCount != after.logicalFileCount {
		t.Fatalf("logical_file count changed: before=%d after=%d", before.logicalFileCount, after.logicalFileCount)
	}
	if before.refCount != after.refCount {
		t.Fatalf("logical_file.ref_count changed: before=%d after=%d", before.refCount, after.refCount)
	}
}

func assertRestorePhysicalStateEqual(t *testing.T, before, after restoreCatalogState) {
	t.Helper()
	if before.physicalCount != after.physicalCount {
		t.Fatalf("physical_file count changed: before=%d after=%d", before.physicalCount, after.physicalCount)
	}
	if before.physicalPath != after.physicalPath {
		t.Fatalf("physical_file.path changed: before=%q after=%q", before.physicalPath, after.physicalPath)
	}
}

func assertRestoreSnapshotStateEqual(t *testing.T, before, after restoreCatalogState) {
	t.Helper()
	if before.snapshotCount != after.snapshotCount {
		t.Fatalf("snapshot_file count changed: before=%d after=%d", before.snapshotCount, after.snapshotCount)
	}
	if before.fileChunkCount != after.fileChunkCount {
		t.Fatalf("file_chunk count changed: before=%d after=%d", before.fileChunkCount, after.fileChunkCount)
	}
}

func assertRestoreChunkStateEqual(t *testing.T, before, after restoreCatalogState) {
	t.Helper()
	if len(before.chunkLiveRefs) != len(after.chunkLiveRefs) {
		t.Fatalf("chunk.live_ref_count set changed: before=%v after=%v", before.chunkLiveRefs, after.chunkLiveRefs)
	}
	for chunkID, beforeCount := range before.chunkLiveRefs {
		if after.chunkLiveRefs[chunkID] != beforeCount {
			t.Fatalf("chunk.live_ref_count changed for chunk %d: before=%d after=%d", chunkID, beforeCount, after.chunkLiveRefs[chunkID])
		}
		if after.chunkPinCounts[chunkID] != before.chunkPinCounts[chunkID] {
			t.Fatalf("chunk.pin_count changed for chunk %d: before=%d after=%d", chunkID, before.chunkPinCounts[chunkID], after.chunkPinCounts[chunkID])
		}
	}
}

func logicalFileChunkIDs(t *testing.T, db *sql.DB, fileID int64) []int64 {
	t.Helper()

	rows, err := db.Query(`SELECT chunk_id FROM file_chunk WHERE logical_file_id = $1 ORDER BY chunk_order ASC`, fileID)
	if err != nil {
		t.Fatalf("query file chunks: %v", err)
	}
	defer rows.Close()

	var ids []int64
	for rows.Next() {
		var chunkID int64
		if err := rows.Scan(&chunkID); err != nil {
			t.Fatalf("scan chunk id: %v", err)
		}
		ids = append(ids, chunkID)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate file chunks: %v", err)
	}
	return ids
}

func chunkPinCount(t *testing.T, db *sql.DB, chunkID int64) int64 {
	t.Helper()
	var pinCount int64
	if err := db.QueryRow(`SELECT pin_count FROM chunk WHERE id = $1`, chunkID).Scan(&pinCount); err != nil {
		t.Fatalf("read chunk pin_count: %v", err)
	}
	return pinCount
}

func openTempPostgresEngineDatabase(t *testing.T, prefix string) *sql.DB {
	t.Helper()

	adminDB := openRawPostgresDB(t, "")
	testDBName := trustedPostgresDatabaseName(prefix)
	if err := execTrustedPostgresDatabaseDDL(adminDB, trustedCreateDatabaseStatement(testDBName)); err != nil {
		_ = adminDB.Close()
		t.Fatalf("create temporary postgres database %s: %v", testDBName, err)
	}

	t.Cleanup(func() {
		_ = terminateRestorePostgresSessions(adminDB, testDBName)
		_ = dropRestorePostgresDatabase(adminDB, testDBName)
		_ = adminDB.Close()
	})

	db := openRawPostgresDB(t, testDBName)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func trustedPostgresDatabaseName(prefix string) string {
	safePrefix := strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
			return r
		default:
			return '_'
		}
	}, prefix)
	return fmt.Sprintf("%s_%d", safePrefix, atomic.AddUint64(&restorePostgresDatabaseSequence, 1))
}

func trustedCreateDatabaseStatement(identifier string) string {
	return fmt.Sprintf("CREATE DATABASE %s", trustedQuotedPostgresIdentifier(identifier))
}

func trustedDropDatabaseStatement(identifier string) string {
	return fmt.Sprintf("DROP DATABASE IF EXISTS %s", trustedQuotedPostgresIdentifier(identifier))
}

func execTrustedPostgresDatabaseDDL(dbconn *sql.DB, statement string) error {
	_, err := callRestoreTrustedSQLExec(dbconn, statement)
	return err
}

func terminateRestorePostgresSessions(adminDB *sql.DB, dbName string) error {
	_, err := callRestoreTrustedSQLExec(adminDB, `
		SELECT pg_terminate_backend(pid)
		FROM pg_stat_activity
		WHERE datname = $1 AND pid <> pg_backend_pid()
	`, dbName)
	return err
}

func dropRestorePostgresDatabase(adminDB *sql.DB, dbName string) error {
	return execTrustedPostgresDatabaseDDL(adminDB, trustedDropDatabaseStatement(dbName))
}

func insertRestoreSnapshotFixtureRow(db *sql.DB, snapshotID string) (sql.Result, error) {
	return callRestoreTrustedSQLExec(
		db,
		`INSERT INTO snapshot (id, created_at, type, label) VALUES ($1, $2, $3, $4)`,
		snapshotID,
		"2026-06-01T00:00:00Z",
		"full",
		"restore-stored-path",
	)
}

func insertRestoreSnapshotPathFixtureRow(db *sql.DB, pathID int64, storedPath string) (sql.Result, error) {
	return callRestoreTrustedSQLExec(db, `INSERT INTO snapshot_path (id, path) VALUES ($1, $2)`, pathID, storedPath)
}

func insertRestoreSnapshotFileFixtureRow(db *sql.DB, snapshotID string, pathID, fileID, size int64) (sql.Result, error) {
	return callRestoreTrustedSQLExec(
		db,
		`INSERT INTO snapshot_file (snapshot_id, path_id, logical_file_id, size) VALUES ($1, $2, $3, $4)`,
		snapshotID,
		pathID,
		fileID,
		size,
	)
}

func callRestoreTrustedSQLExec(dbconn *sql.DB, query string, args ...any) (sql.Result, error) {
	out := reflect.ValueOf(dbconn).MethodByName("Exec").CallSlice([]reflect.Value{
		reflect.ValueOf(query),
		reflect.ValueOf(args),
	})
	var result sql.Result
	if !out[0].IsNil() {
		result = out[0].Interface().(sql.Result)
	}
	var err error
	if !out[1].IsNil() {
		err = out[1].Interface().(error)
	}
	return result, err
}

func trustedQuotedPostgresIdentifier(identifier string) string {
	if !trustedPostgresIdentifierPattern.MatchString(identifier) {
		panic("unexpected postgres test identifier")
	}
	return `"` + identifier + `"`
}

func openRawPostgresDB(t *testing.T, dbName string) *sql.DB {
	t.Helper()

	connStr, err := dbpkg.BuildPostgresConnStringFromEnv(dbName)
	if err != nil {
		t.Fatalf("BuildPostgresConnStringFromEnv(%q): %v", dbName, err)
	}
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		t.Fatalf("sql.Open postgres %q: %v", dbName, err)
	}
	if err := db.Ping(); err != nil {
		_ = db.Close()
		t.Fatalf("ping postgres %q: %v", dbName, err)
	}
	return db
}
