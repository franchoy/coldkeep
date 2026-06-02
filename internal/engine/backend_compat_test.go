package engine_test

import (
	"database/sql"
	"reflect"
	"strings"
	"testing"

	_ "github.com/mattn/go-sqlite3"

	"github.com/franchoy/coldkeep/internal/engine"
)

// TestEnginePublicTypesDoNotExposeBackendSpecificFields verifies that
// engine.Config, engine.StatsRequest, engine.InspectRequest, and
// engine.VerifyRequest do not have exported fields whose names expose a
// backend-specific assumption (sqlite, postgres, dsn, driver, wal, etc.).
//
// engine.Config.DB (*sql.DB) is intentionally backend-neutral; the field
// name "DB" does not match any forbidden term.
func TestEnginePublicTypesDoNotExposeBackendSpecificFields(t *testing.T) {
	for _, tc := range enginePublicTypesForBackendCompatibility() {
		t.Run(tc.name, func(t *testing.T) {
			assertEngineTypeHasNoBackendSpecificFields(t, tc.name, reflect.TypeOf(tc.val))
		})
	}
}

type enginePublicTypeCase struct {
	name string
	val  any
}

func backendSpecificFieldTerms() []string {
	return []string{
		"sqlite", "postgres", "dsn", "driver", "wal", "sslmode", "backend", "dialect",
	}
}

func enginePublicTypesForBackendCompatibility() []enginePublicTypeCase {
	return []enginePublicTypeCase{
		{"Config", engine.Config{}},
		{"StatsRequest", engine.StatsRequest{}},
		{"InspectRequest", engine.InspectRequest{}},
		{"VerifyRequest", engine.VerifyRequest{}},
		// Operation candidates (inactive; expanded in v1.12 Phase 2).
		{"OperationWarning", engine.OperationWarning{}},
		{"BatchSummary", engine.BatchSummary{}},
		{"SnapshotQuery", engine.SnapshotQuery{}},
		{"StoreRequest", engine.StoreRequest{}},
		{"StoreResult", engine.StoreResult{}},
		{"RestoreRequest", engine.RestoreRequest{}},
		{"RestoreItemResult", engine.RestoreItemResult{}},
		{"RestoreResult", engine.RestoreResult{}},
		{"RemoveRequest", engine.RemoveRequest{}},
		{"RemoveItemResult", engine.RemoveItemResult{}},
		{"RemoveResult", engine.RemoveResult{}},
		{"GarbageCollectRequest", engine.GarbageCollectRequest{}},
		{"GarbageCollectResult", engine.GarbageCollectResult{}},
		{"SnapshotMeta", engine.SnapshotMeta{}},
		{"SnapshotCreateRequest", engine.SnapshotCreateRequest{}},
		{"SnapshotCreateResult", engine.SnapshotCreateResult{}},
		{"SnapshotListRequest", engine.SnapshotListRequest{}},
		{"SnapshotListResult", engine.SnapshotListResult{}},
		{"SnapshotFile", engine.SnapshotFile{}},
		{"SnapshotShowRequest", engine.SnapshotShowRequest{}},
		{"SnapshotShowResult", engine.SnapshotShowResult{}},
		{"SnapshotStatsRequest", engine.SnapshotStatsRequest{}},
		{"SnapshotStatsResult", engine.SnapshotStatsResult{}},
		{"SnapshotDiffEntry", engine.SnapshotDiffEntry{}},
		{"SnapshotDiffRequest", engine.SnapshotDiffRequest{}},
		{"SnapshotDiffSummary", engine.SnapshotDiffSummary{}},
		{"SnapshotDiffResult", engine.SnapshotDiffResult{}},
		{"SnapshotRestoreRequest", engine.SnapshotRestoreRequest{}},
		{"SnapshotRestoreResult", engine.SnapshotRestoreResult{}},
		{"SnapshotDeleteRequest", engine.SnapshotDeleteRequest{}},
		{"SnapshotDeleteResult", engine.SnapshotDeleteResult{}},
		{"RepairRequest", engine.RepairRequest{}},
		{"RepairTargetResult", engine.RepairTargetResult{}},
		{"RepairResult", engine.RepairResult{}},
		{"RecoverRequest", engine.RecoverRequest{}},
		{"RecoverResult", engine.RecoverResult{}},
	}
}

func assertEngineTypeHasNoBackendSpecificFields(t *testing.T, typeName string, rt reflect.Type) {
	t.Helper()
	for i := 0; i < rt.NumField(); i++ {
		assertEngineFieldNameIsBackendNeutral(t, typeName, rt.Field(i))
	}
}

func assertEngineFieldNameIsBackendNeutral(t *testing.T, typeName string, field reflect.StructField) {
	t.Helper()
	if !field.IsExported() {
		return
	}
	lower := strings.ToLower(field.Name)
	for _, term := range backendSpecificFieldTerms() {
		if strings.Contains(lower, term) {
			t.Errorf("engine.%s has backend-specific field %q (contains %q)",
				typeName, field.Name, term)
		}
	}
}

// TestConfigRequiresCallerProvidedDB verifies two things:
//  1. engine.New fails when Config.DB is nil — the engine does not open a DB itself.
//  2. engine.Config has a DB field of type *sql.DB — confirming the caller-provided pattern.
func TestConfigRequiresCallerProvidedDB(t *testing.T) {
	// 1. Nil DB must fail.
	_, err := engine.New(engine.Config{})
	if err == nil {
		t.Fatal("engine.New with nil DB must return error; got nil")
	}

	// 2. Config must have an exported DB field of type *sql.DB.
	rt := reflect.TypeOf(engine.Config{})
	dbField, ok := rt.FieldByName("DB")
	if !ok {
		t.Fatal("engine.Config must have an exported DB field")
	}
	want := reflect.TypeOf((*sql.DB)(nil))
	if dbField.Type != want {
		t.Errorf("engine.Config.DB type: got %v, want %v", dbField.Type, want)
	}
}

// TestEngineNewAcceptsBackendNeutralDB confirms that engine.New succeeds when
// given a caller-provided *sql.DB regardless of the underlying backend.
//
// This test uses an in-memory SQLite connection as a concrete backend-neutral
// example. Because engine.New and observability.NewService do not execute any
// schema queries at construction, the bare :memory: database is sufficient.
func TestEngineNewAcceptsBackendNeutralDB(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("failed to open in-memory SQLite DB: %v", err)
	}
	defer db.Close()

	eng, err := engine.New(engine.Config{DB: db, ContainerDir: t.TempDir()})
	if err != nil {
		t.Fatalf("engine.New with a valid *sql.DB must succeed; got: %v", err)
	}
	if eng == nil {
		t.Fatal("engine.New returned nil engine without error")
	}
}
