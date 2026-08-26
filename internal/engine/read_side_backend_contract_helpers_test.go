package engine_test

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/franchoy/coldkeep/internal/blocks"
	"github.com/franchoy/coldkeep/internal/container"
	"github.com/franchoy/coldkeep/internal/engine"
	"github.com/franchoy/coldkeep/internal/storage"
	"github.com/franchoy/coldkeep/internal/testutil/backendtest"
)

var engineReadFixtureTime = time.Date(2026, time.February, 3, 4, 5, 6, 0, time.UTC)

type engineReadFixture struct {
	backend      backendtest.Backend
	engine       *engine.DefaultEngine
	containerDir string
	logicalA     int64
	logicalB     int64
	chunkA       int64
	containerID  int64
}

func newEngineReadFixture(t *testing.T, backend backendtest.Backend) engineReadFixture {
	t.Helper()
	containerDir := filepath.Join(t.TempDir(), "containers")
	writer := container.NewLocalWriterWithDirAndDB(containerDir, container.GetContainerMaxSize(), backend.DB)
	storageContext := storage.StorageContext{DB: backend.DB, Writer: writer, ContainerDir: containerDir}
	t.Cleanup(func() { _ = storageContext.Close() })

	storedA := storeEngineReadFixtureFile(t, storageContext, "phase7-alpha.txt", "phase7 deterministic alpha payload")
	storedB := storeEngineReadFixtureFile(t, storageContext, "phase7-beta.txt", "phase7 deterministic beta payload")
	if err := writer.FinalizeContainer(); err != nil {
		t.Fatalf("finalize Phase 7 fixture container: %v", err)
	}

	chunkA := requireEngineReadInt64(t, backend.DB,
		`SELECT chunk_id FROM file_chunk WHERE logical_file_id = $1 ORDER BY chunk_order LIMIT 1`, storedA.FileID)
	containerID := requireEngineReadInt64(t, backend.DB,
		`SELECT container_id FROM blocks WHERE chunk_id = $1`, chunkA)

	seedEngineReadSnapshots(t, backend.DB, storedA.FileID, storedB.FileID)
	eng, err := engine.New(engine.Config{DB: backend.DB, ContainerDir: containerDir, StoreContext: &storageContext})
	if err != nil {
		t.Fatalf("engine.New: %v", err)
	}
	return engineReadFixture{
		backend: backend, engine: eng, containerDir: containerDir,
		logicalA: storedA.FileID, logicalB: storedB.FileID,
		chunkA: chunkA, containerID: containerID,
	}
}

func storeEngineReadFixtureFile(t *testing.T, storageContext storage.StorageContext, name, payload string) storage.StoreFileResult {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, []byte(payload), 0o600); err != nil {
		t.Fatalf("write fixture input %q: %v", name, err)
	}
	stored, err := storage.StoreFileWithStorageContextAndCodecResult(storageContext, path, blocks.CodecPlain)
	if err != nil {
		t.Fatalf("store fixture input %q: %v", name, err)
	}
	return stored
}

func seedEngineReadSnapshots(t *testing.T, dbconn *sql.DB, logicalA, logicalB int64) {
	t.Helper()
	exec := func(query string, args ...any) {
		t.Helper()
		if _, err := dbconn.ExecContext(context.Background(), query, args...); err != nil {
			t.Fatalf("seed engine read fixture: %v\nquery: %s", err, query)
		}
	}
	for _, row := range []struct {
		id, kind string
		created  time.Time
		label    any
		parent   any
	}{
		{"snap-root", "full", engineReadFixtureTime, nil, nil},
		{"snap-base", "full", engineReadFixtureTime.Add(time.Minute), "base", "snap-root"},
		{"snap-target", "full", engineReadFixtureTime.Add(time.Minute), "target", "snap-base"},
	} {
		exec(`INSERT INTO snapshot (id, created_at, type, label, parent_id) VALUES ($1, $2, $3, $4, $5)`,
			row.id, row.created, row.kind, row.label, row.parent)
	}
	paths := []string{"docs/common.txt", "docs/removed.txt", "docs/added.txt"}
	for _, path := range paths {
		exec(`INSERT INTO snapshot_path (path) VALUES ($1)`, path)
	}
	pathID := func(path string) int64 {
		return requireEngineReadInt64(t, dbconn, `SELECT id FROM snapshot_path WHERE path = $1`, path)
	}
	insert := func(snapshotID, path string, logicalID int64) {
		exec(`INSERT INTO snapshot_file (snapshot_id, path_id, logical_file_id, size, mode, mtime)
              VALUES ($1, $2, $3, $4, $5, $6)`,
			snapshotID, pathID(path), logicalID, 10, 0o644, engineReadFixtureTime)
	}
	insert("snap-base", "docs/common.txt", logicalA)
	insert("snap-base", "docs/removed.txt", logicalA)
	insert("snap-target", "docs/common.txt", logicalA)
	insert("snap-target", "docs/added.txt", logicalB)
}

func requireEngineReadInt64(t *testing.T, dbconn *sql.DB, query string, args ...any) int64 {
	t.Helper()
	var value int64
	if err := dbconn.QueryRowContext(context.Background(), query, args...).Scan(&value); err != nil {
		t.Fatalf("query fixture integer: %v\nquery: %s", err, query)
	}
	return value
}

type engineReadState struct {
	tables     map[string]int64
	logical    []string
	chunks     []string
	containers []string
	files      []string
}

func captureEngineReadState(t *testing.T, dbconn *sql.DB, containerDir string) engineReadState {
	t.Helper()
	state := engineReadState{tables: map[string]int64{}}
	for _, table := range []string{"logical_file", "physical_file", "snapshot", "snapshot_path", "snapshot_file", "chunk", "file_chunk", "blocks", "container", "storage_blocks", "chunk_block_refs"} {
		state.tables[table] = requireEngineReadInt64(t, dbconn, "SELECT COUNT(*) FROM "+table)
	}
	state.logical = engineReadRows(t, dbconn, `SELECT id, status, ref_count, retry_count, chunker_version, updated_at FROM logical_file ORDER BY id`)
	state.chunks = engineReadRows(t, dbconn, `SELECT id, status, live_ref_count, pin_count, retry_count, chunker_version, updated_at FROM chunk ORDER BY id`)
	state.containers = engineReadRows(t, dbconn, `SELECT id, filename, sealed, sealing, quarantine, current_size, max_size, updated_at FROM container ORDER BY id`)
	state.files = engineReadFileManifest(t, containerDir)
	return state
}

func engineReadRows(t *testing.T, dbconn *sql.DB, query string) []string {
	t.Helper()
	rows, err := dbconn.QueryContext(context.Background(), query)
	if err != nil {
		t.Fatalf("capture engine read state: %v", err)
	}
	defer func() { _ = rows.Close() }()
	columns, err := rows.Columns()
	if err != nil {
		t.Fatalf("capture engine read state columns: %v", err)
	}
	result := make([]string, 0)
	for rows.Next() {
		values := make([]any, len(columns))
		scans := make([]any, len(columns))
		for i := range values {
			scans[i] = &values[i]
		}
		if err := rows.Scan(scans...); err != nil {
			t.Fatalf("capture engine read state row: %v", err)
		}
		parts := make([]string, len(values))
		for i, value := range values {
			parts[i] = fmt.Sprint(value)
		}
		result = append(result, strings.Join(parts, "|"))
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("capture engine read state iteration: %v", err)
	}
	return result
}

func engineReadFileManifest(t *testing.T, root string) []string {
	t.Helper()
	entries := make([]string, 0)
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		hash := sha256.Sum256(data)
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		entries = append(entries, rel+"|"+hex.EncodeToString(hash[:]))
		return nil
	})
	if err != nil {
		t.Fatalf("capture container manifest: %v", err)
	}
	sort.Strings(entries)
	return entries
}

func assertEngineReadStateUnchanged(t *testing.T, before, after engineReadState) {
	t.Helper()
	if fmt.Sprint(before.tables) != fmt.Sprint(after.tables) ||
		fmt.Sprint(before.logical) != fmt.Sprint(after.logical) ||
		fmt.Sprint(before.chunks) != fmt.Sprint(after.chunks) ||
		fmt.Sprint(before.containers) != fmt.Sprint(after.containers) ||
		fmt.Sprint(before.files) != fmt.Sprint(after.files) {
		t.Fatalf("engine read mutated repository state:\n before=%+v\n after=%+v", before, after)
	}
}
