package listing

import (
	"strings"
	"testing"
)

func TestParsePaginationArgsDefaultsToNil(t *testing.T) {
	limit, offset, err := parsePaginationArgs(nil)
	if err != nil {
		t.Fatalf("parse pagination args: %v", err)
	}
	if limit != nil {
		t.Fatalf("expected nil limit, got %v", *limit)
	}
	if offset != nil {
		t.Fatalf("expected nil offset, got %v", *offset)
	}
}

func TestParsePaginationArgsUsesLastValue(t *testing.T) {
	limit, offset, err := parsePaginationArgs([]string{"--limit", "10", "--offset", "5", "--limit", "25"})
	if err != nil {
		t.Fatalf("parse pagination args: %v", err)
	}
	if limit == nil || *limit != 25 {
		t.Fatalf("expected limit 25, got %v", limit)
	}
	if offset == nil || *offset != 5 {
		t.Fatalf("expected offset 5, got %v", offset)
	}
}

func TestParsePaginationArgsRejectsNegativeValues(t *testing.T) {
	_, _, err := parsePaginationArgs([]string{"--offset", "-1"})
	if err == nil || !strings.Contains(err.Error(), "invalid --offset") {
		t.Fatalf("expected error containing \"invalid --offset\", got: %v", err)
	}
}

func TestParsePaginationArgsRejectsLimitAboveMaximum(t *testing.T) {
	_, _, err := parsePaginationArgs([]string{"--limit", "10001"})
	if err == nil || !strings.Contains(err.Error(), "invalid --limit") {
		t.Fatalf("expected error containing \"invalid --limit\", got: %v", err)
	}
}

func TestParsePaginationArgsRejectsMissingLimitValue(t *testing.T) {
	_, _, err := parsePaginationArgs([]string{"--limit"})
	if err == nil || !strings.Contains(err.Error(), "missing argument for --limit") {
		t.Fatalf("expected missing --limit argument contract, got: %v", err)
	}
}

func TestParsePaginationArgsRejectsMissingOffsetValue(t *testing.T) {
	_, _, err := parsePaginationArgs([]string{"--offset"})
	if err == nil || !strings.Contains(err.Error(), "missing argument for --offset") {
		t.Fatalf("expected missing --offset argument contract, got: %v", err)
	}
}

func TestParsePaginationArgsRejectsNonIntegerLimitValue(t *testing.T) {
	_, _, err := parsePaginationArgs([]string{"--limit", "NaN"})
	if err == nil || !strings.Contains(err.Error(), "invalid --limit") {
		t.Fatalf("expected invalid --limit parse contract, got: %v", err)
	}
}

func TestApplyPaginationAppendsLimitThenOffset(t *testing.T) {
	limitValue := int64(10)
	offsetValue := int64(5)

	query, params := applyPagination("SELECT * FROM logical_file ORDER BY created_at DESC", []interface{}{"completed", "report"}, 3, &limitValue, &offsetValue)

	if !strings.Contains(query, "LIMIT $3 OFFSET $4") {
		t.Fatalf("unexpected query: %s", query)
	}
	if len(params) != 4 {
		t.Fatalf("expected 4 params, got %d", len(params))
	}
	if got := params[2]; got != int64(10) {
		t.Fatalf("expected limit param 10, got %v", got)
	}
	if got := params[3]; got != int64(5) {
		t.Fatalf("expected offset param 5, got %v", got)
	}
}
