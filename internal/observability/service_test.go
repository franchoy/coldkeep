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
	if result.GC.Kind != SimulationKindGC {
		t.Errorf("Kind = %q, want %q", result.GC.Kind, SimulationKindGC)
	}
	if !result.GC.Exact {
		t.Error("expected GC.Exact=true")
	}
	if result.GC.Mutated {
		t.Error("expected GC.Mutated=false")
	}
	if len(result.GC.Assumptions.DeletedSnapshots) != 0 {
		t.Errorf("DeletedSnapshots = %v, want empty", result.GC.Assumptions.DeletedSnapshots)
	}
	if result.GC.Summary.ReachableChunks != 0 {
		t.Errorf("ReachableChunks = %d, want 0", result.GC.Summary.ReachableChunks)
	}
	if result.GC.Summary.UnreachableChunks != 0 {
		t.Errorf("UnreachableChunks = %d, want 0", result.GC.Summary.UnreachableChunks)
	}
	if result.GC.Summary.LogicallyReclaimableBytes != 0 {
		t.Errorf("LogicallyReclaimableBytes = %d, want 0", result.GC.Summary.LogicallyReclaimableBytes)
	}
	if result.GC.Summary.PhysicallyReclaimableBytes != 0 {
		t.Errorf("PhysicallyReclaimableBytes = %d, want 0", result.GC.Summary.PhysicallyReclaimableBytes)
	}
	if len(result.GC.Containers) != 0 {
		t.Errorf("Containers = %d, want 0", len(result.GC.Containers))
	}
	if !result.Exact {
		t.Error("expected Exact=true")
	}
	if result.Mutated {
		t.Error("expected Mutated=false")
	}
}

func TestSimulateGCAssumptionsIncludeDeletedSnapshots(t *testing.T) {
	dbconn := openSimulateTestDB(t)
	svc := newServiceForTest(dbconn, nil)
	if _, err := dbconn.Exec(`INSERT INTO snapshot (id, created_at, type) VALUES ('s1', CURRENT_TIMESTAMP, 'full'), ('s2', CURRENT_TIMESTAMP, 'full')`); err != nil {
		t.Fatalf("insert snapshots: %v", err)
	}

	result, err := svc.Simulate(context.Background(), SimulationOptions{
		Kind:                   SimulationKindGC,
		AssumeDeletedSnapshots: []string{"s1", "s2"},
	})
	if err != nil {
		t.Fatalf("Simulate: %v", err)
	}
	if result == nil || result.GC == nil {
		t.Fatal("expected non-nil result with GC field")
	}
	if len(result.GC.Assumptions.DeletedSnapshots) != 2 {
		t.Fatalf("DeletedSnapshots length = %d, want 2", len(result.GC.Assumptions.DeletedSnapshots))
	}
	if result.GC.Assumptions.DeletedSnapshots[0] != "s1" || result.GC.Assumptions.DeletedSnapshots[1] != "s2" {
		t.Fatalf("DeletedSnapshots = %v, want [s1 s2]", result.GC.Assumptions.DeletedSnapshots)
	}
	if !result.GC.Exact {
		t.Error("expected GC.Exact=true")
	}
	if result.GC.Mutated {
		t.Error("expected GC.Mutated=false")
	}
}

func TestSimulateGCRejectsMissingDeletedSnapshot(t *testing.T) {
	dbconn := openSimulateTestDB(t)
	svc := newServiceForTest(dbconn, nil)

	result, err := svc.Simulate(context.Background(), SimulationOptions{
		Kind:                   SimulationKindGC,
		AssumeDeletedSnapshots: []string{"missing-snapshot"},
	})
	if err == nil {
		t.Fatal("expected error")
	}
	if result != nil {
		t.Fatal("expected nil result")
	}
	if !strings.Contains(err.Error(), `snapshot "missing-snapshot" does not exist`) {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestSimulateGCIncludesWarnings(t *testing.T) {
	dbconn := openSimulateTestDB(t)
	svc := newServiceForTest(dbconn, nil)

	if _, err := dbconn.Exec(
		`INSERT INTO chunk (chunk_hash, size, status, live_ref_count, pin_count, chunker_version) VALUES (?, ?, 'COMPLETED', ?, ?, ?)`,
		"orphan-placement", 50, 0, 0, "v9-future-cdc",
	); err != nil {
		t.Fatalf("insert placementless chunk: %v", err)
	}

	result, err := svc.Simulate(context.Background(), SimulationOptions{Kind: SimulationKindGC})
	if err != nil {
		t.Fatalf("Simulate: %v", err)
	}
	if result == nil || result.GC == nil {
		t.Fatal("expected non-nil result with GC field")
	}
	if len(result.GC.Warnings) == 0 {
		t.Fatal("expected nested GC warnings")
	}
	if len(result.Warnings) == 0 {
		t.Fatal("expected top-level warnings")
	}
	if result.GC.Warnings[0].Code == "" || result.Warnings[0].Code == "" {
		t.Fatalf("expected warning codes, got gc=%v top=%v", result.GC.Warnings, result.Warnings)
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
