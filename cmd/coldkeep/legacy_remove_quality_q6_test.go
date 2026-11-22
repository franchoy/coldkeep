package main

import (
	"reflect"
	"testing"

	"github.com/franchoy/coldkeep/v2/internal/batch"
	"github.com/franchoy/coldkeep/v2/internal/engine"
)

func TestLegacyRemoveProjectionFixedOutputs(t *testing.T) {
	failed := engine.RemoveItemResult{FileID: 99, Status: engine.BatchItemFailed,
		Error: "fixed failure", InvariantCode: "fixed invariant", RecommendedAction: "fixed action"}
	wantFailure := batch.ItemResult{ID: 7, Status: batch.ResultFailed,
		Message: "fixed failure", InvariantCode: "fixed invariant", RecommendedAction: "fixed action"}
	cases := []struct {
		name   string
		dryRun bool
		item   engine.RemoveItemResult
		want   batch.ItemResult
	}{
		{"failure", false, failed, wantFailure},
		{"failure_during_dry_run", true, failed, wantFailure},
		{"planned", true, engine.RemoveItemResult{FileID: 99, Status: engine.BatchItemPlanned},
			batch.ItemResult{ID: 7, Status: batch.ResultPlanned, Message: "would remove"}},
		{"success", false, engine.RemoveItemResult{FileID: 99, Status: engine.BatchItemOK, RemovedChunkAssociations: 3},
			batch.ItemResult{ID: 7, Status: batch.ResultSuccess, Message: "removed mappings=3"}},
		{"success_zero", false, engine.RemoveItemResult{FileID: 99, Status: engine.BatchItemOK},
			batch.ItemResult{ID: 7, Status: batch.ResultSuccess, Message: "removed mappings=0"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := projectLegacyRemoveResult(7, tc.dryRun, tc.item); !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("projection = %+v, want %+v", got, tc.want)
			}
		})
	}
}
