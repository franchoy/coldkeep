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
	if err == nil {
		t.Fatal("expected error for negative offset")
	}
	if !strings.Contains(err.Error(), "invalid --offset") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestParsePaginationArgsRejectsLimitAboveMaximum(t *testing.T) {
	_, _, err := parsePaginationArgs([]string{"--limit", "10001"})
	if err == nil {
		t.Fatal("expected error for limit above maximum")
	}
	if !strings.Contains(err.Error(), "invalid --limit") {
		t.Fatalf("unexpected error: %v", err)
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
