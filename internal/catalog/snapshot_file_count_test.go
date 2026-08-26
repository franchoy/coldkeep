package catalog_test

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/franchoy/coldkeep/internal/catalog"
)

func TestSnapshotMetaFileCountIsTotalPersistedMembership(t *testing.T) {
	db := openTestDB(t)
	now := time.Date(2026, 8, 25, 10, 0, 0, 0, time.UTC)
	insertSnapshotFixture(t, db, snapshotFixture{ID: "count-empty", SnapType: "full", Created: now})
	insertSnapshotFixture(t, db, snapshotFixture{ID: "count-full", SnapType: "full", Created: now.Add(time.Second)})
	insertCatalogSnapshotMembership(t, db, "count-full", 901, 801, "docs/a.txt", 10)
	insertCatalogSnapshotMembership(t, db, "count-full", 902, 802, "docs/b.txt", 20)

	svc := catalog.NewServiceFromSQL(db)
	full, err := svc.FindSnapshot(context.Background(), "count-full")
	if err != nil {
		t.Fatalf("FindSnapshot: %v", err)
	}
	if got, want := full.FileCount, 2; got != want {
		t.Errorf("FindSnapshot FileCount = %d, want %d", got, want)
	}
	empty, err := svc.FindSnapshot(context.Background(), "count-empty")
	if err != nil {
		t.Fatalf("FindSnapshot empty: %v", err)
	}
	if got := empty.FileCount; got != 0 {
		t.Errorf("empty FindSnapshot FileCount = %d, want 0", got)
	}

	refs, err := svc.ListSnapshots(context.Background(), catalog.SnapshotFilter{})
	if err != nil {
		t.Fatalf("ListSnapshots: %v", err)
	}
	assertCatalogSnapshotFileCounts(t, refs, map[string]int{"count-empty": 0, "count-full": 2})

	limited, err := svc.ListSnapshots(context.Background(), catalog.SnapshotFilter{Limit: 1})
	if err != nil {
		t.Fatalf("ListSnapshots limited: %v", err)
	}
	assertCatalogSnapshotFileCounts(t, limited, map[string]int{"count-full": 2})

	graph, err := svc.LoadSnapshotGraph(context.Background())
	if err != nil {
		t.Fatalf("LoadSnapshotGraph: %v", err)
	}
	graphRefs := make([]catalog.SnapshotRef, len(graph.Nodes))
	for i, node := range graph.Nodes {
		graphRefs[i] = node.Snapshot
	}
	assertCatalogSnapshotFileCounts(t, graphRefs, map[string]int{"count-empty": 0, "count-full": 2})
}

func insertCatalogSnapshotMembership(t *testing.T, db *sql.DB, snapshotID string, logicalID, pathID int64, path string, size int64) {
	t.Helper()
	ctx := context.Background()
	if _, err := db.ExecContext(ctx,
		`INSERT INTO logical_file (id, original_name, total_size, file_hash, ref_count, status) VALUES (?, ?, ?, ?, 1, 'COMPLETED')`,
		logicalID, path, size, path+":phase9"); err != nil {
		t.Fatalf("insert logical file %q: %v", path, err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO snapshot_path (id, path) VALUES (?, ?)`, pathID, path); err != nil {
		t.Fatalf("insert snapshot path %q: %v", path, err)
	}
	if _, err := db.ExecContext(ctx,
		`INSERT INTO snapshot_file (snapshot_id, path_id, logical_file_id, size) VALUES (?, ?, ?, ?)`,
		snapshotID, pathID, logicalID, size); err != nil {
		t.Fatalf("insert snapshot membership %q/%q: %v", snapshotID, path, err)
	}
}

func assertCatalogSnapshotFileCounts(t *testing.T, refs []catalog.SnapshotRef, want map[string]int) {
	t.Helper()
	if len(refs) != len(want) {
		t.Fatalf("snapshot refs length = %d, want %d: %+v", len(refs), len(want), refs)
	}
	for _, ref := range refs {
		wantCount, ok := want[ref.ID]
		if !ok {
			t.Errorf("unexpected snapshot ref %q", ref.ID)
			continue
		}
		if ref.FileCount != wantCount {
			t.Errorf("snapshot %q FileCount = %d, want persisted membership %d", ref.ID, ref.FileCount, wantCount)
		}
	}
}
