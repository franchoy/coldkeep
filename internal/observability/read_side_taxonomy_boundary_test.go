package observability_test

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/franchoy/coldkeep/internal/catalog"
	"github.com/franchoy/coldkeep/internal/db"
	"github.com/franchoy/coldkeep/internal/engine"
	"github.com/franchoy/coldkeep/internal/observability"
	_ "github.com/mattn/go-sqlite3"
)

func TestInspectDirectObservabilityFailuresRemainOutsideUnsupportedAndDeferredClassification(t *testing.T) {
	dbconn := openReadSideObservabilityTestDB(t)
	svc, err := observability.NewService(dbconn)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}

	_, err = svc.Inspect(context.Background(), observability.EntityChunk, "999", observability.InspectOptions{})
	if err == nil {
		t.Fatal("expected missing chunk inspect error")
	}
	if !errors.Is(err, observability.ErrNotFound) {
		t.Fatalf("expected ErrNotFound-compatible error, got %v", err)
	}
	if engine.IsUnsupported(err) {
		t.Fatalf("expected missing inspect target to stay outside unsupported classification: %v", err)
	}
	if catalog.IsDeferred(err) {
		t.Fatalf("expected missing inspect target to stay outside deferred classification: %v", err)
	}
}

func TestInspectInvalidTargetRemainsOutsideUnsupportedAndDeferredClassification(t *testing.T) {
	dbconn := openReadSideObservabilityTestDB(t)
	svc, err := observability.NewService(dbconn)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}

	_, err = svc.Inspect(context.Background(), observability.EntityChunk, "abc", observability.InspectOptions{})
	if err == nil {
		t.Fatal("expected invalid target error")
	}
	if !errors.Is(err, observability.ErrInvalidTarget) {
		t.Fatalf("expected ErrInvalidTarget-compatible error, got %v", err)
	}
	if engine.IsUnsupported(err) {
		t.Fatalf("expected invalid target to remain outside unsupported classification: %v", err)
	}
	if catalog.IsDeferred(err) {
		t.Fatalf("expected invalid target to remain outside deferred classification: %v", err)
	}
}

func TestInspectUnsupportedEntityRemainsOutsideEngineUnsupportedAndCatalogDeferred(t *testing.T) {
	dbconn := openReadSideObservabilityTestDB(t)
	svc, err := observability.NewService(dbconn)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}

	_, err = svc.Inspect(context.Background(), observability.EntityPhysicalFile, "1", observability.InspectOptions{})
	if err == nil {
		t.Fatal("expected unsupported entity error")
	}
	if !errors.Is(err, observability.ErrUnsupportedEntity) {
		t.Fatalf("expected ErrUnsupportedEntity-compatible error, got %v", err)
	}
	if engine.IsUnsupported(err) {
		t.Fatalf("expected unsupported inspect entity to stay outside engine unsupported classification: %v", err)
	}
	if catalog.IsDeferred(err) {
		t.Fatalf("expected unsupported inspect entity to stay outside catalog deferred classification: %v", err)
	}
}

func openReadSideObservabilityTestDB(t *testing.T) *sql.DB {
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
