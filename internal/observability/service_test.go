package observability

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
)

func TestSimulateGCNotImplementedYet(t *testing.T) {
	svc := newServiceForTest(nil, nil)

	result, err := svc.Simulate(context.Background(), SimulationOptions{Kind: SimulationKindGC})
	if err == nil {
		t.Fatal("expected error")
	}
	if result != nil {
		t.Fatal("expected nil result")
	}
	if !strings.Contains(err.Error(), "gc simulation not implemented yet") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestSimulateRejectsUnsupportedKind(t *testing.T) {
	svc := newServiceForTest(nil, nil)

	_, err := svc.Simulate(context.Background(), SimulationOptions{Kind: "store"})
	if err == nil {
		t.Fatal("expected error")
	}
	if !errors.Is(err, ErrInvalidTarget) {
		t.Fatalf("expected ErrInvalidTarget, got %v", err)
	}
	if !strings.Contains(err.Error(), "unsupported simulation kind") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestNewServiceRequiresNonNilDB(t *testing.T) {
	_, err := NewService(nil)
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestNewServiceWithDB(t *testing.T) {
	dbconn := &sql.DB{}
	svc, err := NewService(dbconn)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	if svc == nil || svc.db != dbconn {
		t.Fatal("expected service with injected db")
	}
}
