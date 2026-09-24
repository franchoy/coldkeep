package engine_test

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/franchoy/coldkeep/internal/engine"
	"github.com/franchoy/coldkeep/internal/testutil/backendtest"
)

func TestEngineSnapshotSelectorsAcrossBackends(t *testing.T) {
	backendtest.ForEach(t, backendtest.Options{}, func(t *testing.T, backend backendtest.Backend) {
		fixture := newEngineReadFixture(t, backend)
		if _, err := backend.DB.ExecContext(context.Background(), `UPDATE snapshot SET label = $1 WHERE id = $2`, "Target", "snap-target"); err != nil {
			t.Fatalf("update mixed-case snapshot label: %v", err)
		}
		before := captureEngineReadState(t, backend.DB, fixture.containerDir)

		atTie := engineReadFixtureTime.Add(time.Minute)
		listed, err := fixture.engine.SnapshotList(context.Background(), engine.SnapshotListRequest{
			Type:  engine.SnapshotTypeFull,
			Since: &atTie,
			Until: &atTie,
			Limit: 2,
		})
		if err != nil || !reflect.DeepEqual(snapshotIDs(listed), []string{"snap-target", "snap-base"}) {
			t.Fatalf("equal-time SnapshotList: got (%+v, %v)", listed, err)
		}
		filtered, err := fixture.engine.SnapshotList(context.Background(), engine.SnapshotListRequest{Label: "target"})
		if err != nil || !reflect.DeepEqual(snapshotIDs(filtered), []string{"snap-target"}) {
			t.Fatalf("label SnapshotList: got (%+v, %v)", filtered, err)
		}
		tree, err := fixture.engine.SnapshotList(context.Background(), engine.SnapshotListRequest{Tree: true})
		if err != nil || tree.Graph == nil || !tree.TreeMode || !reflect.DeepEqual(snapshotIDs(tree), []string{"snap-target", "snap-base", "snap-root"}) {
			t.Fatalf("tree SnapshotList: got (%+v, %v)", tree, err)
		}
		if got, want := tree.Graph.RootIDs, []string{"snap-root"}; !reflect.DeepEqual(got, want) {
			t.Fatalf("tree roots: got %v want %v", got, want)
		}
		if got, want := tree.Graph.Nodes[0].ChildIDs, []string{"snap-base"}; !reflect.DeepEqual(got, want) {
			t.Fatalf("tree root children: got %v want %v", got, want)
		}
		filteredTree, err := fixture.engine.SnapshotList(context.Background(), engine.SnapshotListRequest{Label: "target", Tree: true})
		if err != nil || filteredTree.Graph == nil || !reflect.DeepEqual(filteredTree.Graph.RootIDs, []string{"snap-target"}) {
			t.Fatalf("filtered tree projection: got (%+v, %v)", filteredTree, err)
		}
		if filteredTree.Graph.Nodes[0].ParentState != engine.SnapshotParentPresent {
			t.Fatalf("filtered tree lost existing-parent state: %+v", filteredTree.Graph.Nodes[0])
		}

		query := engine.SnapshotQuery{
			Paths:          []string{"docs/added.txt"},
			Prefixes:       []string{"docs/"},
			Pattern:        "docs/*.txt",
			Regex:          "added\\.txt$",
			MinSize:        int64Pointer(10),
			MaxSize:        int64Pointer(10),
			ModifiedAfter:  timePointer(engineReadFixtureTime),
			ModifiedBefore: timePointer(engineReadFixtureTime),
		}
		show, err := fixture.engine.SnapshotShow(context.Background(), engine.SnapshotShowRequest{SnapshotID: "snap-target", Query: query})
		if err != nil || !reflect.DeepEqual(snapshotPaths(show), []string{"docs/added.txt"}) || show.MatchedFileCount != 1 || show.TotalFileCount != 2 {
			t.Fatalf("filtered SnapshotShow: got (%+v, %v)", show, err)
		}
		diff, err := fixture.engine.SnapshotDiff(context.Background(), engine.SnapshotDiffRequest{BaseID: "snap-base", TargetID: "snap-target", Query: query})
		if err != nil || !reflect.DeepEqual(diffPaths(diff), []string{"docs/added.txt"}) || diff.Summary.Added != 1 || diff.Summary.Removed != 0 {
			t.Fatalf("filtered SnapshotDiff: got (%+v, %v)", diff, err)
		}
		unfilteredShow, err := fixture.engine.SnapshotShow(context.Background(), engine.SnapshotShowRequest{SnapshotID: "snap-target"})
		if err != nil || !reflect.DeepEqual(snapshotPaths(unfilteredShow), []string{"docs/added.txt", "docs/common.txt"}) {
			t.Fatalf("ordered SnapshotShow: got (%+v, %v)", unfilteredShow, err)
		}
		unfilteredDiff, err := fixture.engine.SnapshotDiff(context.Background(), engine.SnapshotDiffRequest{BaseID: "snap-base", TargetID: "snap-target"})
		if err != nil || !reflect.DeepEqual(diffPaths(unfilteredDiff), []string{"docs/added.txt", "docs/removed.txt"}) {
			t.Fatalf("ordered SnapshotDiff: got (%+v, %v)", unfilteredDiff, err)
		}
		for i := 0; i < 2; i++ {
			again, err := fixture.engine.SnapshotList(context.Background(), engine.SnapshotListRequest{Since: &atTie, Until: &atTie})
			if err != nil || !reflect.DeepEqual(snapshotIDs(again), []string{"snap-target", "snap-base"}) {
				t.Fatalf("repeated SnapshotList %d: got (%+v, %v)", i, again, err)
			}
		}
		assertEngineReadStateUnchanged(t, before, captureEngineReadState(t, backend.DB, fixture.containerDir))

		restored, err := fixture.engine.SnapshotRestore(context.Background(), engine.SnapshotRestoreRequest{
			SnapshotID: "snap-target",
			Selection: engine.SnapshotRestoreSelection{
				ExactPaths:     []string{"docs/added.txt"},
				Prefixes:       []string{"docs/"},
				Pattern:        "docs/*.txt",
				Regex:          "added\\.txt$",
				MinSize:        int64Pointer(10),
				MaxSize:        int64Pointer(10),
				ModifiedAfter:  timePointer(engineReadFixtureTime),
				ModifiedBefore: timePointer(engineReadFixtureTime),
			},
			Destination: engine.SnapshotRestoreDestination{
				Mode: engine.SnapshotRestoreDestinationOriginal,
				Path: t.TempDir(),
			},
		})
		if err != nil || restored.RestoredFiles != 1 || len(restored.OutputPaths) != 1 {
			t.Fatalf("filtered SnapshotRestore: got (%+v, %v)", restored, err)
		}
	})
}

func TestEngineSnapshotSelectorErrorsAcrossBackends(t *testing.T) {
	backendtest.ForEach(t, backendtest.Options{}, func(t *testing.T, backend backendtest.Backend) {
		fixture := newEngineReadFixture(t, backend)
		before := captureEngineReadState(t, backend.DB, fixture.containerDir)
		invalid := engine.SnapshotQuery{Regex: "("}
		if _, err := fixture.engine.SnapshotShow(context.Background(), engine.SnapshotShowRequest{SnapshotID: "snap-target", Query: invalid}); err == nil || !strings.Contains(err.Error(), "invalid snapshot query regex") {
			t.Fatalf("invalid SnapshotShow regex: %v", err)
		}
		if _, err := fixture.engine.SnapshotDiff(context.Background(), engine.SnapshotDiffRequest{BaseID: "snap-base", TargetID: "snap-target", Query: invalid}); err == nil || !strings.Contains(err.Error(), "invalid snapshot query regex") {
			t.Fatalf("invalid SnapshotDiff regex: %v", err)
		}
		if _, err := fixture.engine.SnapshotRestore(context.Background(), engine.SnapshotRestoreRequest{
			SnapshotID:  "snap-target",
			Selection:   engine.SnapshotRestoreSelection{Regex: "("},
			Destination: engine.SnapshotRestoreDestination{Mode: engine.SnapshotRestoreDestinationOriginal, Path: t.TempDir()},
		}); !engine.IsCode(err, engine.ErrorInvalidArgument) || !strings.Contains(err.Error(), "invalid snapshot query regex") {
			t.Fatalf("invalid SnapshotRestore regex: %v", err)
		}
		if _, err := fixture.engine.SnapshotShow(context.Background(), engine.SnapshotShowRequest{SnapshotID: "snap-target", Query: engine.SnapshotQuery{Paths: []string{" docs/common.txt"}}}); err == nil || !strings.Contains(err.Error(), "leading or trailing whitespace") {
			t.Fatalf("invalid SnapshotShow path: %v", err)
		}
		if _, err := fixture.engine.SnapshotDiff(context.Background(), engine.SnapshotDiffRequest{BaseID: "snap-base", TargetID: "snap-target", Query: engine.SnapshotQuery{Prefixes: []string{"docs"}}}); err == nil || !strings.Contains(err.Error(), "must end with '/'") {
			t.Fatalf("invalid SnapshotDiff prefix: %v", err)
		}
		if _, err := fixture.engine.SnapshotRestore(context.Background(), engine.SnapshotRestoreRequest{
			SnapshotID:  "snap-target",
			Selection:   engine.SnapshotRestoreSelection{Prefixes: []string{"docs"}},
			Destination: engine.SnapshotRestoreDestination{Mode: engine.SnapshotRestoreDestinationOriginal, Path: t.TempDir()},
		}); !engine.IsCode(err, engine.ErrorInvalidArgument) || !strings.Contains(err.Error(), "must end with '/'") {
			t.Fatalf("invalid SnapshotRestore prefix: %v", err)
		}
		cancelled, cancel := context.WithCancel(context.Background())
		cancel()
		if _, err := fixture.engine.SnapshotList(cancelled, engine.SnapshotListRequest{}); !errors.Is(err, context.Canceled) {
			t.Fatalf("cancelled SnapshotList: %v", err)
		}
		if _, err := fixture.engine.SnapshotShow(cancelled, engine.SnapshotShowRequest{SnapshotID: "snap-target"}); !errors.Is(err, context.Canceled) {
			t.Fatalf("cancelled SnapshotShow: %v", err)
		}
		if _, err := fixture.engine.SnapshotDiff(cancelled, engine.SnapshotDiffRequest{BaseID: "snap-base", TargetID: "snap-target"}); !errors.Is(err, context.Canceled) {
			t.Fatalf("cancelled SnapshotDiff: %v", err)
		}
		if _, err := fixture.engine.SnapshotShow(context.Background(), engine.SnapshotShowRequest{SnapshotID: "missing"}); err == nil || !strings.Contains(err.Error(), "not found") {
			t.Fatalf("missing SnapshotShow: %v", err)
		}
		if _, err := fixture.engine.SnapshotDiff(context.Background(), engine.SnapshotDiffRequest{BaseID: "", TargetID: "snap-target"}); err == nil || !strings.Contains(err.Error(), "base snapshot id cannot be empty") {
			t.Fatalf("blank SnapshotDiff base: %v", err)
		}
		same, err := fixture.engine.SnapshotDiff(context.Background(), engine.SnapshotDiffRequest{BaseID: "snap-base", TargetID: "snap-base"})
		if err != nil || len(same.Entries) != 0 || same.Summary != (engine.SnapshotDiffSummary{}) {
			t.Fatalf("same-ID SnapshotDiff: got (%+v, %v)", same, err)
		}
		assertEngineReadStateUnchanged(t, before, captureEngineReadState(t, backend.DB, fixture.containerDir))
	})
}

func TestEngineSnapshotCreateSelectorAtomicityAcrossBackends(t *testing.T) {
	backendtest.ForEach(t, backendtest.Options{}, func(t *testing.T, backend backendtest.Backend) {
		fixture := newMutationBackendFixture(t, backend)
		fixture.store(t, "docs/a.txt", []byte("selector atomicity backend payload"))
		fixture.useAbsoluteStoredPath(t, "docs/a.txt")
		fixture.finalize(t)
		eng := fixture.readEngine(t)

		if _, err := backend.DB.ExecContext(context.Background(),
			`INSERT INTO snapshot_path (path) VALUES ($1)`, "shared/preexisting.txt"); err != nil {
			t.Fatalf("seed preexisting snapshot_path: %v", err)
		}
		initialPathRows := requireEngineReadInt64(t, backend.DB, `SELECT COUNT(*) FROM snapshot_path`)

		assertRolledBack := func(snapshotID string) {
			t.Helper()
			if got := requireEngineReadInt64(t, backend.DB, `SELECT COUNT(*) FROM snapshot WHERE id = $1`, snapshotID); got != 0 {
				t.Fatalf("failed create left %d snapshot rows for %q", got, snapshotID)
			}
			if got := requireEngineReadInt64(t, backend.DB, `SELECT COUNT(*) FROM snapshot_file WHERE snapshot_id = $1`, snapshotID); got != 0 {
				t.Fatalf("failed create left %d snapshot_file rows for %q", got, snapshotID)
			}
			if got := requireEngineReadInt64(t, backend.DB, `SELECT COUNT(*) FROM snapshot_path`); got != initialPathRows {
				t.Fatalf("failed create changed snapshot_path rows: got %d want %d", got, initialPathRows)
			}
			if got := requireEngineReadInt64(t, backend.DB, `SELECT COUNT(*) FROM snapshot_path WHERE path = $1`, "shared/preexisting.txt"); got != 1 {
				t.Fatalf("failed create changed preexisting snapshot_path row: got %d", got)
			}
		}

		if _, err := eng.SnapshotCreate(context.Background(), engine.SnapshotCreateRequest{
			ID: "selector-missing-prefix", SelectionBase: fixture.inputRoot, Paths: []string{"missing/"},
		}); !engine.IsCode(err, engine.ErrorNotFound) {
			t.Fatalf("missing prefix: expected not_found, got code=%q err=%v", engine.CodeOf(err), err)
		}
		assertRolledBack("selector-missing-prefix")

		if _, err := eng.SnapshotCreate(context.Background(), engine.SnapshotCreateRequest{
			ID: "selector-mixed-miss", SelectionBase: fixture.inputRoot, Paths: []string{"docs/a.txt", "missing/"},
		}); !engine.IsCode(err, engine.ErrorNotFound) {
			t.Fatalf("mixed selector miss: expected not_found, got code=%q err=%v", engine.CodeOf(err), err)
		}
		assertRolledBack("selector-mixed-miss")

		result, err := eng.SnapshotCreate(context.Background(), engine.SnapshotCreateRequest{
			ID: "selector-overlap", SelectionBase: fixture.inputRoot,
			Paths: []string{"docs/", "./docs//", "docs/a.txt", "./docs/a.txt"},
		})
		if err != nil || result.FilesInserted != 1 {
			t.Fatalf("duplicate/overlap create: got result=%+v err=%v", result, err)
		}
		if got := requireEngineReadInt64(t, backend.DB, `SELECT COUNT(*) FROM snapshot_file WHERE snapshot_id = $1`, result.SnapshotID); got != 1 {
			t.Fatalf("duplicate/overlap create inserted %d members, want 1", got)
		}
	})
}

func int64Pointer(value int64) *int64 { return &value }

func timePointer(value time.Time) *time.Time { return &value }
