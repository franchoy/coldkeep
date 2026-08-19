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

		query := engine.SnapshotQuery{
			Path:           "docs/added.txt",
			Prefix:         "docs/",
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

func int64Pointer(value int64) *int64 { return &value }

func timePointer(value time.Time) *time.Time { return &value }
