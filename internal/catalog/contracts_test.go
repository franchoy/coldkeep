package catalog_test

import (
	"errors"
	"reflect"
	"testing"

	"github.com/franchoy/coldkeep/internal/catalog"
)

func TestRestorePlanSelectorContract(t *testing.T) {
	valid := []catalog.RestorePlanInput{
		{Selector: catalog.RestoreByFileID, FileID: 7},
		{Selector: catalog.RestoreByStoredPath, StoredPath: "folder/file.txt"},
		{Selector: catalog.RestoreBySnapshotPath, SnapshotID: "snap-1", SnapshotPath: "folder/file.txt"},
	}
	for _, input := range valid {
		if err := catalog.ValidateRestorePlanInput(input); err != nil {
			t.Errorf("valid input %+v: %v", input, err)
		}
	}

	invalid := []catalog.RestorePlanInput{
		{},
		{Selector: catalog.RestoreByFileID},
		{Selector: catalog.RestoreByFileID, FileID: 1, StoredPath: "also-set"},
		{Selector: catalog.RestoreByStoredPath, FileID: 1, StoredPath: "file"},
		{Selector: catalog.RestoreBySnapshotPath, SnapshotID: "snap"},
		{Selector: "future", FileID: 1},
	}
	for _, input := range invalid {
		err := catalog.ValidateRestorePlanInput(input)
		if !catalog.IsCode(err, catalog.ErrorInvalidArgument) {
			t.Errorf("invalid input %+v: got %v", input, err)
		}
		var typed *catalog.Error
		if !errors.As(err, &typed) || typed.Invariant != "exactly_one_restore_selector" {
			t.Errorf("invalid input %+v lacks selector invariant: %v", input, err)
		}
	}
}

func TestGCPlanInputNormalizationIsSortedUniqueAndNonMutating(t *testing.T) {
	input := catalog.GCPlanInput{ExcludeSnapshotIDs: []string{"z", "a", "z", "m"}}
	got, err := catalog.NormalizeGCPlanInput(input)
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"a", "m", "z"}; !reflect.DeepEqual(got.ExcludeSnapshotIDs, want) {
		t.Fatalf("got %v want %v", got.ExcludeSnapshotIDs, want)
	}
	if want := []string{"z", "a", "z", "m"}; !reflect.DeepEqual(input.ExcludeSnapshotIDs, want) {
		t.Fatalf("input mutated: %v", input.ExcludeSnapshotIDs)
	}
	if _, err := catalog.NormalizeGCPlanInput(catalog.GCPlanInput{ExcludeSnapshotIDs: []string{"ok", " "}}); !catalog.IsCode(err, catalog.ErrorInvalidArgument) {
		t.Fatalf("empty ID error: %v", err)
	}
}

func TestChunkPlacementTaggedUnionContract(t *testing.T) {
	container := catalog.ContainerPlacementRef{ID: 2, Filename: "c-2", CurrentSize: 4096, MaxSize: 4096}
	base := catalog.ChunkPlacementRef{ChunkOrder: 0, ChunkID: 3, ChunkHash: "abc", ChunkSize: 9, ChunkerVersion: "v2-fastcdc", ChunkStatus: "COMPLETED"}
	legacy := base
	legacy.Kind = catalog.PlacementLegacy
	legacy.Legacy = &catalog.LegacyChunkPlacement{BlockID: 4, Codec: "plain", FormatVersion: 1, PlaintextSize: 9, StoredSize: 9, Container: container}
	if err := catalog.ValidateChunkPlacement(legacy); err != nil {
		t.Fatalf("valid legacy: %v", err)
	}

	packed := base
	packed.Kind = catalog.PlacementPacked
	packed.Packed = &catalog.PackedChunkPlacement{BlockID: 5, Codec: "none", FormatVersion: 1, PlaintextSize: 20, CompressionCodec: "none", StoredSize: 20, BlockHash: []byte{1}, Container: container, OffsetInBlock: 2, SizeInBlock: 9}
	if err := catalog.ValidateChunkPlacement(packed); err != nil {
		t.Fatalf("valid packed: %v", err)
	}

	invalid := packed
	invalid.Legacy = legacy.Legacy
	if err := catalog.ValidateChunkPlacement(invalid); !catalog.IsCode(err, catalog.ErrorInvariantViolation) {
		t.Fatalf("invalid union: %v", err)
	}
	invalid = packed
	invalid.Packed.SizeInBlock = 0
	if err := catalog.ValidateChunkPlacement(invalid); !catalog.IsCode(err, catalog.ErrorInvariantViolation) {
		t.Fatalf("invalid range: %v", err)
	}
}

func TestCatalogErrorPreservesStableContextAndCause(t *testing.T) {
	cause := errors.New("backend detail")
	err := catalog.NewError(catalog.ErrorConflict, "load graph", "acyclic_snapshot_graph", "graph conflict", cause)
	if !errors.Is(err, cause) || !catalog.IsCode(err, catalog.ErrorConflict) {
		t.Fatalf("typed error classification failed: %v", err)
	}
	if err.Operation != "load graph" || err.Invariant != "acyclic_snapshot_graph" {
		t.Fatalf("typed context lost: %+v", err)
	}
	if !catalog.IsCode(catalog.ErrNotImplemented, catalog.ErrorUnsupported) || !catalog.IsDeferred(catalog.ErrNotImplemented) {
		t.Fatalf("deferred sentinel not typed: %v", catalog.ErrNotImplemented)
	}
}
