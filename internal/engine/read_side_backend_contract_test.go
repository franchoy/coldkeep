package engine_test

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/franchoy/coldkeep/internal/catalog"
	"github.com/franchoy/coldkeep/internal/engine"
	"github.com/franchoy/coldkeep/internal/observability"
	"github.com/franchoy/coldkeep/internal/testutil/backendtest"
)

func TestEngineReadStatsAndInspectAcrossBackends(t *testing.T) {
	backendtest.ForEach(t, backendtest.Options{}, func(t *testing.T, backend backendtest.Backend) {
		fixture := newEngineReadFixture(t, backend)
		before := captureEngineReadState(t, backend.DB, fixture.containerDir)

		stats, err := fixture.engine.Stats(context.Background(), engine.StatsRequest{IncludeContainers: true})
		if err != nil {
			t.Fatalf("Stats: %v", err)
		}
		if stats.Logical.TotalFiles != 2 || len(stats.Containers.Records) != 1 || stats.Snapshots.TotalSnapshots != 3 {
			t.Fatalf("Stats result: %+v", stats)
		}
		if again, err := fixture.engine.Stats(context.Background(), engine.StatsRequest{IncludeContainers: true}); err != nil || !equivalentStats(stats, again) {
			t.Fatalf("repeated Stats: got (%+v, %v)", again, err)
		}

		assertInspectSummary(t, fixture, engine.InspectRepository, "", "total_snapshots", int64(3))
		assertInspectSummary(t, fixture, engine.InspectLogicalFile, fmt.Sprint(fixture.logicalA), "file_id", fixture.logicalA)
		assertInspectSummary(t, fixture, engine.InspectChunk, fmt.Sprint(fixture.chunkA), "chunk_id", fixture.chunkA)
		assertInspectSummary(t, fixture, engine.InspectContainer, fmt.Sprint(fixture.containerID), "container_id", fixture.containerID)
		assertInspectSummary(t, fixture, engine.InspectSnapshot, "snap-target", "snapshot_id", "snap-target")

		withRelations, err := fixture.engine.Inspect(context.Background(), engine.InspectRequest{
			Entity: engine.InspectSnapshot, EntityID: "snap-target",
			Options: engine.InspectOptions{Relations: true, Deep: true, Limit: 10},
		})
		if err != nil || !relationsSorted(withRelations.Relations) {
			t.Fatalf("Inspect snapshot relations: got (%+v, %v)", withRelations, err)
		}

		_, err = fixture.engine.Inspect(context.Background(), engine.InspectRequest{Entity: engine.InspectPhysicalFile, EntityID: "1"})
		if !errors.Is(err, observability.ErrUnsupportedEntity) || engine.IsUnsupported(err) || catalog.IsDeferred(err) {
			t.Fatalf("physical-file inspect classification: %v", err)
		}
		assertEngineReadStateUnchanged(t, before, captureEngineReadState(t, backend.DB, fixture.containerDir))
	})
}

func TestEngineReadSnapshotViewsAcrossBackends(t *testing.T) {
	backendtest.ForEach(t, backendtest.Options{}, func(t *testing.T, backend backendtest.Backend) {
		fixture := newEngineReadFixture(t, backend)
		before := captureEngineReadState(t, backend.DB, fixture.containerDir)

		list, err := fixture.engine.SnapshotList(context.Background(), engine.SnapshotListRequest{Limit: 2})
		if err != nil || list.Count != 2 || !reflect.DeepEqual(snapshotIDs(list), []string{"snap-target", "snap-base"}) {
			t.Fatalf("SnapshotList: got (%+v, %v)", list, err)
		}
		filtered, err := fixture.engine.SnapshotList(context.Background(), engine.SnapshotListRequest{Type: engine.SnapshotTypeFull, Label: "base"})
		if err != nil || !reflect.DeepEqual(snapshotIDs(filtered), []string{"snap-base"}) {
			t.Fatalf("SnapshotList filtered: got (%+v, %v)", filtered, err)
		}

		show, err := fixture.engine.SnapshotShow(context.Background(), engine.SnapshotShowRequest{SnapshotID: "snap-target"})
		if err != nil || show.Snapshot.ParentID != "snap-base" || show.MatchedFileCount != 2 || show.TotalFileCount != 2 || !reflect.DeepEqual(snapshotPaths(show), []string{"docs/added.txt", "docs/common.txt"}) {
			t.Fatalf("SnapshotShow: got (%+v, %v)", show, err)
		}
		aggregate, err := fixture.engine.SnapshotStats(context.Background(), engine.SnapshotStatsRequest{})
		if err != nil || aggregate.SnapshotCount != 3 || aggregate.SnapshotFileCount != 4 {
			t.Fatalf("SnapshotStats aggregate: got (%+v, %v)", aggregate, err)
		}
		perSnapshot, err := fixture.engine.SnapshotStats(context.Background(), engine.SnapshotStatsRequest{SnapshotID: "snap-target"})
		if err != nil || !perSnapshot.HasReuse || perSnapshot.ParentSnapshotID != "snap-base" || perSnapshot.Reused != 1 || perSnapshot.New != 1 {
			t.Fatalf("SnapshotStats target: got (%+v, %v)", perSnapshot, err)
		}
		detailed, err := fixture.engine.SnapshotDiff(context.Background(), engine.SnapshotDiffRequest{BaseID: "snap-base", TargetID: "snap-target"})
		if err != nil || detailed.SummaryMode || !reflect.DeepEqual(diffPaths(detailed), []string{"docs/added.txt", "docs/removed.txt"}) {
			t.Fatalf("SnapshotDiff detailed: got (%+v, %v)", detailed, err)
		}
		summary, err := fixture.engine.SnapshotDiff(context.Background(), engine.SnapshotDiffRequest{BaseID: "snap-base", TargetID: "snap-target", Summary: true})
		if err != nil || !summary.SummaryMode || summary.Entries != nil || summary.Summary.Added != 1 || summary.Summary.Removed != 1 || summary.MatchedEntryCount != 2 {
			t.Fatalf("SnapshotDiff summary: got (%+v, %v)", summary, err)
		}
		assertEngineReadStateUnchanged(t, before, captureEngineReadState(t, backend.DB, fixture.containerDir))
	})
}

func TestEngineReadVerifyAcrossBackends(t *testing.T) {
	backendtest.ForEach(t, backendtest.Options{}, func(t *testing.T, backend backendtest.Backend) {
		fixture := newEngineReadFixture(t, backend)
		before := captureEngineReadState(t, backend.DB, fixture.containerDir)
		for _, level := range []string{"fast", "standard", "full", "deep"} {
			if _, err := fixture.engine.Verify(context.Background(), engine.VerifyRequest{Target: "system", Level: level}); err != nil {
				t.Fatalf("Verify system %s: %v", level, err)
			}
		}
		assertEngineReadStateUnchanged(t, before, captureEngineReadState(t, backend.DB, fixture.containerDir))

		if _, err := backend.DB.ExecContext(context.Background(), `INSERT INTO logical_file (original_name, total_size, file_hash, ref_count, status) VALUES ($1, $2, $3, $4, $5)`, "phase7-invalid.txt", 1, "phase7-invalid-hash", 0, "COMPLETED"); err != nil {
			t.Fatalf("seed verification inconsistency: %v", err)
		}
		_, err := fixture.engine.Verify(context.Background(), engine.VerifyRequest{Target: "system", Level: "standard"})
		if err == nil || !strings.Contains(err.Error(), "system standard verification failed") || engine.IsUnsupported(err) || catalog.IsDeferred(err) {
			t.Fatalf("Verify inconsistency classification: %v", err)
		}
	})
}

func TestEngineReadContextAndErrorsAcrossBackends(t *testing.T) {
	backendtest.ForEach(t, backendtest.Options{}, func(t *testing.T, backend backendtest.Backend) {
		fixture := newEngineReadFixture(t, backend)
		before := captureEngineReadState(t, backend.DB, fixture.containerDir)
		cancelled, cancel := context.WithCancel(context.Background())
		cancel()
		operations := []struct {
			name string
			call func(context.Context) error
		}{
			{"Stats", func(ctx context.Context) error {
				_, err := fixture.engine.Stats(ctx, engine.StatsRequest{})
				return err
			}},
			{"Inspect", func(ctx context.Context) error {
				_, err := fixture.engine.Inspect(ctx, engine.InspectRequest{Entity: engine.InspectRepository})
				return err
			}},
			{"Verify", func(ctx context.Context) error {
				_, err := fixture.engine.Verify(ctx, engine.VerifyRequest{Target: "system", Level: "fast"})
				return err
			}},
			{"SnapshotList", func(ctx context.Context) error {
				_, err := fixture.engine.SnapshotList(ctx, engine.SnapshotListRequest{})
				return err
			}},
			{"SnapshotShow", func(ctx context.Context) error {
				_, err := fixture.engine.SnapshotShow(ctx, engine.SnapshotShowRequest{SnapshotID: "snap-target"})
				return err
			}},
			{"SnapshotStats", func(ctx context.Context) error {
				_, err := fixture.engine.SnapshotStats(ctx, engine.SnapshotStatsRequest{SnapshotID: "snap-target"})
				return err
			}},
			{"SnapshotDiff", func(ctx context.Context) error {
				_, err := fixture.engine.SnapshotDiff(ctx, engine.SnapshotDiffRequest{BaseID: "snap-base", TargetID: "snap-target"})
				return err
			}},
		}
		for _, operation := range operations {
			t.Run(operation.name, func(t *testing.T) {
				err := operation.call(cancelled)
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("expected context cancellation, got %v", err)
				}
			})
		}
		if _, err := fixture.engine.SnapshotShow(context.Background(), engine.SnapshotShowRequest{SnapshotID: "missing"}); err == nil || !strings.Contains(err.Error(), "not found") || engine.IsUnsupported(err) || catalog.IsDeferred(err) {
			t.Fatalf("missing snapshot classification: %v", err)
		}
		if _, err := fixture.engine.Inspect(context.Background(), engine.InspectRequest{Entity: "unknown", EntityID: "1"}); err == nil || engine.IsUnsupported(err) || catalog.IsDeferred(err) {
			t.Fatalf("invalid inspect classification: %v", err)
		}
		if _, err := fixture.engine.Verify(context.Background(), engine.VerifyRequest{Target: "unknown"}); err == nil || engine.IsUnsupported(err) || catalog.IsDeferred(err) {
			t.Fatalf("invalid verify classification: %v", err)
		}
		assertEngineReadStateUnchanged(t, before, captureEngineReadState(t, backend.DB, fixture.containerDir))
	})
}

func equivalentStats(first, second engine.StatsResult) bool {
	a, b := first, second
	a.GeneratedAtUTC = time.Time{}
	b.GeneratedAtUTC = time.Time{}
	return reflect.DeepEqual(a, b)
}

func assertInspectSummary(t *testing.T, fixture engineReadFixture, entity engine.InspectEntity, id, key string, want any) {
	t.Helper()
	result, err := fixture.engine.Inspect(context.Background(), engine.InspectRequest{Entity: entity, EntityID: id})
	if value, ok := result.Summary[key]; err != nil || !ok || !engineValueMatches(value, want) {
		t.Fatalf("Inspect %s/%s: got (%+v, %v), want summary[%q]=%v", entity, id, result, err, key, want)
	}
}

func engineValueMatches(value engine.Value, want any) bool {
	switch expected := want.(type) {
	case int64:
		return value.Kind == engine.ValueInteger && value.Integer == fmt.Sprint(expected)
	case string:
		return value.Kind == engine.ValueString && value.String == expected
	default:
		return false
	}
}

func relationsSorted(relations []engine.InspectRelation) bool {
	for i := 1; i < len(relations); i++ {
		left := fmt.Sprintf("%s|%s|%s|%s", relations[i-1].Direction, relations[i-1].Type, relations[i-1].TargetType, relations[i-1].TargetID)
		right := fmt.Sprintf("%s|%s|%s|%s", relations[i].Direction, relations[i].Type, relations[i].TargetType, relations[i].TargetID)
		if left > right {
			return false
		}
	}
	return true
}

func snapshotIDs(result engine.SnapshotListResult) []string {
	ids := make([]string, len(result.Snapshots))
	for i, snapshot := range result.Snapshots {
		ids[i] = snapshot.ID
	}
	return ids
}

func snapshotPaths(result engine.SnapshotShowResult) []string {
	paths := make([]string, len(result.Files))
	for i, file := range result.Files {
		paths[i] = file.StoredPath
	}
	return paths
}

func diffPaths(result engine.SnapshotDiffResult) []string {
	paths := make([]string, len(result.Entries))
	for i, entry := range result.Entries {
		paths[i] = entry.StoredPath
	}
	return paths
}
