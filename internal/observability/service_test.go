package observability

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"

	"github.com/franchoy/coldkeep/internal/db"
	_ "github.com/mattn/go-sqlite3"
)

func openSimulateTestDB(t *testing.T) *sql.DB {
	t.Helper()
	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	t.Cleanup(func() { _ = dbconn.Close() })
	if err := db.RunMigrations(dbconn); err != nil {
		t.Fatalf("run migrations: %v", err)
	}
	return dbconn
}

func TestSimulateGCRequiresDB(t *testing.T) {
	svc := newServiceForTest(nil, nil)
	result, err := svc.Simulate(context.Background(), SimulationOptions{Kind: SimulationKindGC})
	if err == nil {
		t.Fatal("expected error")
	}
	if result != nil {
		t.Fatal("expected nil result")
	}
}

func TestSimulateGCEmptyRepo(t *testing.T) {
	dbconn := openSimulateTestDB(t)
	svc := newServiceForTest(dbconn, nil)

	result, err := svc.Simulate(context.Background(), SimulationOptions{Kind: SimulationKindGC})
	if err != nil {
		t.Fatalf("Simulate: %v", err)
	}
	if result == nil || result.GC == nil {
		t.Fatal("expected non-nil result with GC field")
	}
	if result.GC.TotalChunks != 0 {
		t.Errorf("TotalChunks = %d, want 0", result.GC.TotalChunks)
	}
	if result.GC.UnreachableChunks != 0 {
		t.Errorf("UnreachableChunks = %d, want 0", result.GC.UnreachableChunks)
	}
	if result.GC.LogicallyReclaimableBytes != 0 {
		t.Errorf("LogicallyReclaimableBytes = %d, want 0", result.GC.LogicallyReclaimableBytes)
	}
	if result.GC.PhysicallyReclaimableBytes != 0 {
		t.Errorf("PhysicallyReclaimableBytes = %d, want 0", result.GC.PhysicallyReclaimableBytes)
	}
	if len(result.GC.AffectedContainers) != 0 {
		t.Errorf("AffectedContainers = %d, want 0", len(result.GC.AffectedContainers))
	}
	if !result.Exact {
		t.Error("expected Exact=true")
	}
	if result.Mutated {
		t.Error("expected Mutated=false")
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

func TestNewServiceRejectsNilDB(t *testing.T) {
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
