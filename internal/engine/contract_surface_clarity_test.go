package engine

import "testing"

func TestSnapshotQueryOrNilReturnsNilForEmptyQuery(t *testing.T) {
	if got := snapshotQueryOrNil(SnapshotQuery{}); got != nil {
		t.Fatalf("snapshotQueryOrNil(empty): got %#v, want nil", got)
	}
}

func TestEngineQueryToSnapshotQueryPreservesSinglePathAndPrefixShape(t *testing.T) {
	min := int64(1)
	max := int64(8)
	got := engineQueryToSnapshotQuery(SnapshotQuery{
		Path:    "docs/a.txt",
		Prefix:  "docs/",
		Pattern: "*.txt",
		Regex:   "^docs/",
		MinSize: &min,
		MaxSize: &max,
		Limit:   5,
	})
	if got == nil {
		t.Fatal("engineQueryToSnapshotQuery: got nil")
	}
	if len(got.ExactPaths) != 1 {
		t.Fatalf("expected exactly one exact path, got %#v", got.ExactPaths)
	}
	if _, ok := got.ExactPaths["docs/a.txt"]; !ok {
		t.Fatalf("expected exact path docs/a.txt, got %#v", got.ExactPaths)
	}
	if len(got.Prefixes) != 1 || got.Prefixes[0] != "docs/" {
		t.Fatalf("expected exactly one prefix docs/, got %#v", got.Prefixes)
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

func TestVerifyResultRemainsIntentionalPlaceholder(t *testing.T) {
	var result VerifyResult
	if result != (VerifyResult{}) {
		t.Fatalf("expected empty VerifyResult placeholder, got %#v", result)
	}
}
