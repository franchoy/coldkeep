package main

// snapshot_engine_routing_test.go — integration tests verifying that the
// 4 read-side snapshot subcommands are correctly routed through the engine
// without mocking the engine-backed phase vars. These tests use a real
// in-memory SQLite DB with schema applied and manually inserted snapshot data.

import (
	"context"
	"database/sql"
	"encoding/json"
	"strings"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"

	dbpkg "github.com/franchoy/coldkeep/internal/db"
	"github.com/franchoy/coldkeep/internal/storage"
)

// openSnapshotRoutingDB opens an in-memory SQLite DB with coldkeep schema.
func openSnapshotRoutingDB(t *testing.T) *sql.DB {
	t.Helper()
	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	t.Cleanup(func() { _ = dbconn.Close() })
	if err := dbpkg.RunMigrations(dbconn); err != nil {
		t.Fatalf("RunMigrations: %v", err)
	}
	return dbconn
}

// injectSnapshotRoutingDB injects the given DB into loadDefaultStorageContextPhase
// and returns a cleanup function (via t.Cleanup). The caller's original value
// is restored automatically.
func injectSnapshotRoutingDB(t *testing.T, dbconn *sql.DB) {
	t.Helper()
	orig := loadDefaultStorageContextPhase
	t.Cleanup(func() { loadDefaultStorageContextPhase = orig })
	loadDefaultStorageContextPhase = func() (storage.StorageContext, error) {
		return storage.StorageContext{DB: dbconn}, nil
	}
}

// insertRoutingSnapshot inserts a minimal snapshot row for routing tests.
func insertRoutingSnapshot(t *testing.T, dbconn *sql.DB, id, snapType, label string, ts time.Time) {
	t.Helper()
	_, err := dbconn.ExecContext(context.Background(),
		`INSERT INTO snapshot (id, created_at, type, label) VALUES ($1, $2, $3, $4)`,
		id, ts.UTC().Format(time.RFC3339), snapType, label)
	if err != nil {
		t.Fatalf("insertRoutingSnapshot %s: %v", id, err)
	}
}

// insertRoutingSnapshotFile inserts a logical_file + snapshot_path + snapshot_file row.
func insertRoutingSnapshotFile(t *testing.T, dbconn *sql.DB, snapshotID, path string, size int64) {
	t.Helper()
	ctx := context.Background()
	hash := snapshotID + ":" + path + ":hash"

	_, err := dbconn.ExecContext(ctx,
		`INSERT OR IGNORE INTO logical_file (original_name, total_size, file_hash, ref_count, status) VALUES (?, ?, ?, 1, 'COMPLETED')`,
		path, size, hash)
	if err != nil {
		t.Fatalf("insert logical_file %s/%s: %v", snapshotID, path, err)
	}
	lfID := lookupRoutingLogicalFileID(t, dbconn, ctx, snapshotID, path, hash)

	_, err = dbconn.ExecContext(ctx, `INSERT OR IGNORE INTO snapshot_path (path) VALUES (?)`, path)
	if err != nil {
		t.Fatalf("insert snapshot_path %s: %v", path, err)
	}
	pathID := lookupRoutingSnapshotPathID(t, dbconn, ctx, path)

	_, err = dbconn.ExecContext(ctx,
		`INSERT INTO snapshot_file (snapshot_id, path_id, logical_file_id, size) VALUES (?, ?, ?, ?)`,
		snapshotID, pathID, lfID, size)
	if err != nil {
		t.Fatalf("insert snapshot_file %s/%s: %v", snapshotID, path, err)
	}
}

func lookupRoutingLogicalFileID(
	t *testing.T,
	dbconn *sql.DB,
	ctx context.Context,
	snapshotID string,
	path string,
	hash string,
) int64 {
	t.Helper()
	stmt, err := dbconn.PrepareContext(ctx, `SELECT id FROM logical_file WHERE file_hash = ?`)
	if err != nil {
		t.Fatalf("prepare logical_file lookup %s/%s: %v", snapshotID, path, err)
	}
	defer func() { _ = stmt.Close() }()

	var lfID int64
	if err := stmt.QueryRowContext(ctx, hash).Scan(&lfID); err != nil {
		t.Fatalf("lookup logical_file %s/%s: %v", snapshotID, path, err)
	}
	return lfID
}

func lookupRoutingSnapshotPathID(t *testing.T, dbconn *sql.DB, ctx context.Context, path string) int64 {
	t.Helper()
	stmt, err := dbconn.PrepareContext(ctx, `SELECT id FROM snapshot_path WHERE path = ?`)
	if err != nil {
		t.Fatalf("prepare snapshot_path lookup %s: %v", path, err)
	}
	defer func() { _ = stmt.Close() }()

	var pathID int64
	if err := stmt.QueryRowContext(ctx, path).Scan(&pathID); err != nil {
		t.Fatalf("lookup snapshot_path %s: %v", path, err)
	}
	return pathID
}

// TestSnapshotListEngineRoutingJSON verifies that snapshot list uses the
// engine-backed phase var to return results from a real DB.
func TestSnapshotListEngineRoutingJSON(t *testing.T) {
	dbconn := openSnapshotRoutingDB(t)
	injectSnapshotRoutingDB(t, dbconn)

	now := time.Now().UTC().Truncate(time.Second)
	insertRoutingSnapshot(t, dbconn, "eng-list-1", "full", "first", now)
	insertRoutingSnapshot(t, dbconn, "eng-list-2", "partial", "second", now.Add(time.Second))

	output := captureStdout(t, func() {
		err := runSnapshotCommand(parsedCommandLine{
			method:      "snapshot",
			positionals: []string{"list"},
			flags:       map[string][]string{"output": {"json"}},
		}, outputModeJSON)
		if err != nil {
			t.Fatalf("snapshot list error: %v", err)
		}
	})

	var payload map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(output)), &payload); err != nil {
		t.Fatalf("parse JSON: %v output=%q", err, output)
	}
	data := payload["data"].(map[string]any)
	if got := data["action"].(string); got != "list" {
		t.Errorf("action: got %q, want list", got)
	}
	if got := int(data["count"].(float64)); got != 2 {
		t.Errorf("count: got %d, want 2", got)
	}
	snapshots := data["snapshots"].([]any)
	// newest first
	first := snapshots[0].(map[string]any)
	if first["id"] != "eng-list-2" {
		t.Errorf("first snapshot id: got %v, want eng-list-2", first["id"])
	}
}

// TestSnapshotShowEngineRoutingJSON verifies that snapshot show uses the
// engine-backed phase var to return snapshot metadata and files from a real DB.
func TestSnapshotShowEngineRoutingJSON(t *testing.T) {
	dbconn := openSnapshotRoutingDB(t)
	injectSnapshotRoutingDB(t, dbconn)

	now := time.Now().UTC().Truncate(time.Second)
	insertRoutingSnapshot(t, dbconn, "eng-show-1", "full", "show-me", now)
	insertRoutingSnapshotFile(t, dbconn, "eng-show-1", "docs/a.txt", 100)
	insertRoutingSnapshotFile(t, dbconn, "eng-show-1", "docs/b.txt", 200)

	output := captureStdout(t, func() {
		err := runSnapshotCommand(parsedCommandLine{
			method:      "snapshot",
			positionals: []string{"show", "eng-show-1"},
			flags:       map[string][]string{"output": {"json"}},
		}, outputModeJSON)
		if err != nil {
			t.Fatalf("snapshot show error: %v", err)
		}
	})

	var payload map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(output)), &payload); err != nil {
		t.Fatalf("parse JSON: %v output=%q", err, output)
	}
	data := payload["data"].(map[string]any)
	snap := data["snapshot"].(map[string]any)
	if snap["id"] != "eng-show-1" {
		t.Errorf("snapshot id: got %v, want eng-show-1", snap["id"])
	}
	if snap["type"] != "full" {
		t.Errorf("snapshot type: got %v, want full", snap["type"])
	}
	if snap["label"] != "show-me" {
		t.Errorf("snapshot label: got %v, want show-me", snap["label"])
	}
	if got := int(data["total_snapshot_file_count"].(float64)); got != 2 {
		t.Errorf("total_snapshot_file_count: got %d, want 2", got)
	}
	files := data["files"].([]any)
	if len(files) != 2 {
		t.Errorf("files: got %d, want 2", len(files))
	}
}

// TestSnapshotStatsEngineRoutingJSON verifies that snapshot stats uses the
// engine-backed phase var to return aggregate statistics from a real DB.
func TestSnapshotStatsEngineRoutingJSON(t *testing.T) {
	dbconn := openSnapshotRoutingDB(t)
	injectSnapshotRoutingDB(t, dbconn)

	now := time.Now().UTC().Truncate(time.Second)
	insertRoutingSnapshot(t, dbconn, "eng-stats-1", "full", "", now)
	insertRoutingSnapshotFile(t, dbconn, "eng-stats-1", "a.txt", 512)
	insertRoutingSnapshotFile(t, dbconn, "eng-stats-1", "b.txt", 1024)

	output := captureStdout(t, func() {
		err := runSnapshotCommand(parsedCommandLine{
			method:      "snapshot",
			positionals: []string{"stats", "eng-stats-1"},
			flags:       map[string][]string{"output": {"json"}},
		}, outputModeJSON)
		if err != nil {
			t.Fatalf("snapshot stats error: %v", err)
		}
	})

	var payload map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(output)), &payload); err != nil {
		t.Fatalf("parse JSON: %v output=%q", err, output)
	}
	data := payload["data"].(map[string]any)
	if got := int(data["snapshot_file_count"].(float64)); got != 2 {
		t.Errorf("snapshot_file_count: got %d, want 2", got)
	}
	if got := int(data["total_size_bytes"].(float64)); got != 1536 {
		t.Errorf("total_size_bytes: got %d, want 1536", got)
	}
}

// TestSnapshotDiffEngineRoutingJSON verifies that snapshot diff uses the
// engine-backed phase vars to return diff results from a real DB.
func TestSnapshotDiffEngineRoutingJSON(t *testing.T) {
	dbconn := openSnapshotRoutingDB(t)
	injectSnapshotRoutingDB(t, dbconn)

	now := time.Now().UTC().Truncate(time.Second)
	insertRoutingSnapshot(t, dbconn, "eng-diff-base", "full", "", now)
	insertRoutingSnapshot(t, dbconn, "eng-diff-target", "full", "", now.Add(time.Second))
	insertRoutingSnapshotFile(t, dbconn, "eng-diff-base", "kept.txt", 100)
	insertRoutingSnapshotFile(t, dbconn, "eng-diff-base", "removed.txt", 200)
	insertRoutingSnapshotFile(t, dbconn, "eng-diff-target", "kept.txt", 100)
	insertRoutingSnapshotFile(t, dbconn, "eng-diff-target", "added.txt", 300)

	output := captureStdout(t, func() {
		err := runSnapshotCommand(parsedCommandLine{
			method:      "snapshot",
			positionals: []string{"diff", "eng-diff-base", "eng-diff-target"},
			flags:       map[string][]string{"output": {"json"}},
		}, outputModeJSON)
		if err != nil {
			t.Fatalf("snapshot diff error: %v", err)
		}
	})

	var payload map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(output)), &payload); err != nil {
		t.Fatalf("parse JSON: %v output=%q", err, output)
	}
	data := payload["data"].(map[string]any)
	if data["base"] != "eng-diff-base" || data["target"] != "eng-diff-target" {
		t.Errorf("base/target: got base=%v target=%v", data["base"], data["target"])
	}
	summary := data["summary"].(map[string]any)
	if got := int(summary["added"].(float64)); got != 1 {
		t.Errorf("summary.added: got %d, want 1", got)
	}
	if got := int(summary["removed"].(float64)); got != 1 {
		t.Errorf("summary.removed: got %d, want 1", got)
	}
}

// TestSnapshotDiffSummaryEngineRoutingJSON verifies the fast-path (summary-only)
// diff uses the engine-backed summary phase var.
func TestSnapshotDiffSummaryEngineRoutingJSON(t *testing.T) {
	dbconn := openSnapshotRoutingDB(t)
	injectSnapshotRoutingDB(t, dbconn)

	now := time.Now().UTC().Truncate(time.Second)
	insertRoutingSnapshot(t, dbconn, "eng-summ-base", "full", "", now)
	insertRoutingSnapshot(t, dbconn, "eng-summ-target", "full", "", now.Add(time.Second))
	insertRoutingSnapshotFile(t, dbconn, "eng-summ-base", "only-in-base.txt", 100)
	insertRoutingSnapshotFile(t, dbconn, "eng-summ-target", "only-in-target.txt", 200)

	output := captureStdout(t, func() {
		err := runSnapshotCommand(parsedCommandLine{
			method:      "snapshot",
			positionals: []string{"diff", "eng-summ-base", "eng-summ-target"},
			flags:       map[string][]string{"output": {"json"}, "summary": {""}},
		}, outputModeJSON)
		if err != nil {
			t.Fatalf("snapshot diff --summary error: %v", err)
		}
	})

	var payload map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(output)), &payload); err != nil {
		t.Fatalf("parse JSON: %v output=%q", err, output)
	}
	data := payload["data"].(map[string]any)
	if summaryMode, _ := data["summary_mode"].(bool); !summaryMode {
		t.Error("expected summary_mode=true")
	}
	summary := data["summary"].(map[string]any)
	if got := int(summary["added"].(float64)); got != 1 {
		t.Errorf("summary.added: got %d, want 1", got)
	}
	if got := int(summary["removed"].(float64)); got != 1 {
		t.Errorf("summary.removed: got %d, want 1", got)
	}
}
