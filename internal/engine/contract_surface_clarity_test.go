package engine

import "testing"

func TestSnapshotQueryOrNilReturnsNilForEmptyQuery(t *testing.T) {
	got, err := snapshotQueryOrNil(SnapshotQuery{})
	if err != nil || got != nil {
		t.Fatalf("snapshotQueryOrNil(empty): got (%#v, %v), want (nil, nil)", got, err)
	}
}

func TestEngineQueryToSnapshotQueryPreservesRepeatedPathAndPrefixShape(t *testing.T) {
	min := int64(1)
	max := int64(8)
	got, err := engineQueryToSnapshotQuery(SnapshotQuery{
		Paths:    []string{"docs/a.txt", "docs/b.txt"},
		Prefixes: []string{"docs/", "images/"},
		Pattern:  "*.txt",
		Regex:    "^docs/",
		MinSize:  &min,
		MaxSize:  &max,
		Limit:    5,
	})
	if err != nil {
		t.Fatalf("engineQueryToSnapshotQuery: %v", err)
	}
	if got == nil {
		t.Fatal("engineQueryToSnapshotQuery: got nil")
	}
	if len(got.ExactPaths) != 2 {
		t.Fatalf("expected two exact paths, got %#v", got.ExactPaths)
	}
	if _, ok := got.ExactPaths["docs/a.txt"]; !ok {
		t.Fatalf("expected exact path docs/a.txt, got %#v", got.ExactPaths)
	}
	if len(got.Prefixes) != 2 || got.Prefixes[0] != "docs/" || got.Prefixes[1] != "images/" {
		t.Fatalf("expected repeated prefixes, got %#v", got.Prefixes)
	}
}

func TestEngineQueryToSnapshotQueryRejectsInvalidRegex(t *testing.T) {
	if _, err := engineQueryToSnapshotQuery(SnapshotQuery{Regex: "("}); err == nil {
		t.Fatal("expected invalid regex error")
	}
}

func TestBuildSnapshotDiffResultSummaryModeOmitsEntries(t *testing.T) {
	result := buildSnapshotDiffResult(
		SnapshotDiffRequest{BaseID: "base", TargetID: "target", Summary: true},
		[]SnapshotDiffEntry{{StoredPath: "docs/a.txt", Change: SnapshotDiffChangeAdded}},
		SnapshotDiffSummary{Added: 1},
		3,
	)
	if !result.SummaryMode {
		t.Fatal("expected SummaryMode=true")
	}
	if len(result.Entries) != 0 {
		t.Fatalf("expected summary-mode result to omit entries, got %#v", result.Entries)
	}
	if result.MatchedEntryCount != 1 {
		t.Fatalf("expected MatchedEntryCount=1, got %d", result.MatchedEntryCount)
	}
	if result.TotalEntryCount != 3 {
		t.Fatalf("expected TotalEntryCount=3, got %d", result.TotalEntryCount)
	}
}

func TestBuildSnapshotDiffResultDetailedModeKeepsMatchedAndTotalDistinct(t *testing.T) {
	result := buildSnapshotDiffResult(
		SnapshotDiffRequest{BaseID: "base", TargetID: "target"},
		[]SnapshotDiffEntry{{StoredPath: "docs/a.txt", Change: SnapshotDiffChangeAdded}},
		SnapshotDiffSummary{Added: 1},
		4,
	)
	if result.SummaryMode {
		t.Fatal("expected SummaryMode=false")
	}
	if len(result.Entries) != 1 {
		t.Fatalf("expected one entry, got %#v", result.Entries)
	}
	if result.MatchedEntryCount != 1 {
		t.Fatalf("expected MatchedEntryCount=1, got %d", result.MatchedEntryCount)
	}
	if result.TotalEntryCount != 4 {
		t.Fatalf("expected TotalEntryCount=4, got %d", result.TotalEntryCount)
	}
}

func TestVerifyResultRepresentsCompleteCLISummary(t *testing.T) {
	result := VerifyResult{
		BlocksChecked: 5, PhysicalHashChecked: 4, CompressedHashChecked: 3,
		LogicalHashChecked: 2, CompressedBlocksChecked: 1,
	}
	if result.BlocksChecked != 5 || result.CompressedBlocksChecked != 1 {
		t.Fatalf("verify summary not representable: %#v", result)
	}
}
