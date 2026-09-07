package main

import (
	"database/sql"
	"fmt"
	"strings"
	"testing"

	"github.com/franchoy/coldkeep/internal/db"
	"github.com/franchoy/coldkeep/internal/testutil/backendtest"
)

const schemaV17FenceVersion = 17

func schemaVersionColumns(t *testing.T, backend backendtest.Backend) []string {
	t.Helper()

	var (
		rows *sql.Rows
		err  error
	)
	switch backend.Kind {
	case db.BackendSQLite:
		rows, err = backend.DB.Query(`PRAGMA table_info(schema_version)`)
	case db.BackendPostgres:
		rows, err = backend.DB.Query(`
			SELECT column_name
			FROM information_schema.columns
			WHERE table_schema = 'public' AND table_name = 'schema_version'
			ORDER BY ordinal_position
		`)
	default:
		t.Fatalf("unsupported backend %s", backend.Kind)
	}
	if err != nil {
		t.Fatalf("inspect schema_version columns: %v", err)
	}
	defer func() { _ = rows.Close() }()

	var columns []string
	for rows.Next() {
		if backend.Kind == db.BackendSQLite {
			var (
				cid       int
				name      string
				columnTyp string
				notNull   int
				defaultV  sql.NullString
				primaryK  int
			)
			if err := rows.Scan(&cid, &name, &columnTyp, &notNull, &defaultV, &primaryK); err != nil {
				t.Fatalf("scan SQLite schema_version column: %v", err)
			}
			columns = append(columns, name)
			continue
		}

		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatalf("scan PostgreSQL schema_version column: %v", err)
		}
		columns = append(columns, name)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate schema_version columns: %v", err)
	}
	return columns
}

func assertSchemaV17Fence(t *testing.T, backend backendtest.Backend) {
	t.Helper()

	columns := schemaVersionColumns(t, backend)
	if len(columns) != 1 || columns[0] != "catalog_version" {
		t.Fatalf("schema_version columns = %v, want exactly [catalog_version]", columns)
	}

	var rowCount, version int
	if err := backend.DB.QueryRow(`SELECT COUNT(*), MIN(catalog_version) FROM schema_version`).Scan(&rowCount, &version); err != nil {
		t.Fatalf("read fenced schema metadata: %v", err)
	}
	if rowCount != 1 || version != schemaV17FenceVersion {
		t.Fatalf("fenced schema metadata = rows:%d version:%d, want rows:1 version:%d", rowCount, version, schemaV17FenceVersion)
	}

	if err := backend.DB.QueryRow(`SELECT MAX(version) FROM schema_version`).Scan(new(int)); err == nil {
		t.Fatal("legacy MAX(version) query unexpectedly succeeded against fenced schema 17")
	}
	if err := backend.DB.QueryRow(`SELECT version FROM schema_version ORDER BY version DESC LIMIT 1`).Scan(new(int)); err == nil {
		t.Fatal("legacy ordered version query unexpectedly succeeded against fenced schema 17")
	}
}

func convertCurrentMetadataToLegacy(t *testing.T, backend backendtest.Backend, versions ...int) {
	t.Helper()

	columns := schemaVersionColumns(t, backend)
	if len(columns) == 1 && columns[0] == "catalog_version" {
		if _, err := backend.DB.Exec(`ALTER TABLE schema_version RENAME COLUMN catalog_version TO version`); err != nil {
			t.Fatalf("restore legacy schema_version column: %v", err)
		}
	} else if len(columns) != 1 || columns[0] != "version" {
		t.Fatalf("cannot construct legacy metadata from columns %v", columns)
	}

	if _, err := backend.DB.Exec(`DELETE FROM schema_version`); err != nil {
		t.Fatalf("clear schema_version: %v", err)
	}
	for _, version := range versions {
		if _, err := backend.DB.Exec(`INSERT INTO schema_version(version) VALUES ($1)`, version); err != nil {
			t.Fatalf("insert legacy schema version %d: %v", version, err)
		}
	}
}

func TestSchemaV17BackwardFenceBootstrapAndReopen(t *testing.T) {
	t.Setenv("COLDKEEP_DB_AUTO_BOOTSTRAP", "true")
	backendtest.ForEach(t, backendtest.Options{Schema: backendtest.EmptySchema}, func(t *testing.T, backend backendtest.Backend) {
		if err := db.EnsureSchema(backend.DB); err != nil {
			t.Fatalf("bootstrap schema 17: %v", err)
		}
		assertSchemaV17Fence(t, backend)

		if err := db.EnsureSchema(backend.DB); err != nil {
			t.Fatalf("reopen fenced schema 17: %v", err)
		}
		assertSchemaV17Fence(t, backend)
	})
}

func TestSchemaV17BackwardFenceNormalizesLegacyMetadata(t *testing.T) {
	t.Setenv("COLDKEEP_DB_AUTO_BOOTSTRAP", "true")
	backendtest.ForEach(t, backendtest.Options{Schema: backendtest.EmptySchema}, func(t *testing.T, backend backendtest.Backend) {
		if err := db.EnsureSchema(backend.DB); err != nil {
			t.Fatalf("bootstrap migration fixture: %v", err)
		}

		if backend.Kind == db.BackendSQLite {
			convertCurrentMetadataToLegacy(t, backend, 8, 9, 16)
		} else {
			convertCurrentMetadataToLegacy(t, backend, 16)
		}
		if err := db.EnsureSchema(backend.DB); err != nil {
			t.Fatalf("migrate valid legacy metadata: %v", err)
		}
		assertSchemaV17Fence(t, backend)

		convertCurrentMetadataToLegacy(t, backend, 17)
		if err := db.EnsureSchema(backend.DB); err != nil {
			t.Fatalf("normalize unfenced local schema 17 metadata: %v", err)
		}
		assertSchemaV17Fence(t, backend)
	})
}

func TestSchemaV17BackwardFenceRejectsFutureLegacyMetadataBeforeMutation(t *testing.T) {
	backendtest.ForEach(t, backendtest.Options{Schema: backendtest.EmptySchema}, func(t *testing.T, backend backendtest.Backend) {
		if _, err := backend.DB.Exec(`CREATE TABLE schema_version (version INTEGER PRIMARY KEY)`); err != nil {
			t.Fatalf("create future legacy metadata: %v", err)
		}
		if _, err := backend.DB.Exec(`INSERT INTO schema_version(version) VALUES (18)`); err != nil {
			t.Fatalf("insert future legacy metadata: %v", err)
		}

		err := db.EnsureSchema(backend.DB)
		if err == nil {
			t.Fatal("future legacy schema metadata was silently accepted")
		}
		if !strings.Contains(strings.ToLower(err.Error()), "future") && !strings.Contains(err.Error(), "18") {
			t.Fatalf("future metadata error = %q, want explicit future/version rejection", err)
		}

		var version int
		if err := backend.DB.QueryRow(`SELECT version FROM schema_version`).Scan(&version); err != nil {
			t.Fatalf("future metadata changed before rejection: %v", err)
		}
		if version != 18 {
			t.Fatalf("future metadata changed to %d before rejection", version)
		}

		var applicationTables int
		var query string
		if backend.Kind == db.BackendSQLite {
			query = `SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'container'`
		} else {
			query = `SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'container'`
		}
		if err := backend.DB.QueryRow(query).Scan(&applicationTables); err != nil {
			t.Fatalf("inspect mutation boundary: %v", err)
		}
		if applicationTables != 0 {
			t.Fatalf("future metadata rejection created %d application table(s)", applicationTables)
		}
	})
}

func TestSchemaV17BackwardFenceRejectsMalformedMetadataBeforeMutation(t *testing.T) {
	testCases := []struct {
		name      string
		createSQL string
	}{
		{name: "zero_rows", createSQL: `CREATE TABLE schema_version (version INTEGER PRIMARY KEY)`},
		{name: "both_columns", createSQL: `CREATE TABLE schema_version (version INTEGER PRIMARY KEY, catalog_version INTEGER)`},
		{name: "neither_column", createSQL: `CREATE TABLE schema_version (unrelated INTEGER PRIMARY KEY)`},
		{name: "malformed_type", createSQL: `CREATE TABLE schema_version (version TEXT PRIMARY KEY); INSERT INTO schema_version(version) VALUES ('sixteen')`},
		{name: "fenced_below_17", createSQL: `CREATE TABLE schema_version (catalog_version INTEGER PRIMARY KEY); INSERT INTO schema_version(catalog_version) VALUES (16)`},
		{name: "fenced_above_17", createSQL: `CREATE TABLE schema_version (catalog_version INTEGER PRIMARY KEY); INSERT INTO schema_version(catalog_version) VALUES (18)`},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			backendtest.ForEach(t, backendtest.Options{Schema: backendtest.EmptySchema}, func(t *testing.T, backend backendtest.Backend) {
				if _, err := backend.DB.Exec(testCase.createSQL); err != nil {
					t.Fatalf("create malformed metadata fixture: %v", err)
				}
				if err := db.EnsureSchema(backend.DB); err == nil {
					t.Fatalf("malformed metadata %s was accepted", testCase.name)
				}

				var applicationTables int
				query := `SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'container'`
				if backend.Kind == db.BackendSQLite {
					query = `SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'container'`
				}
				if err := backend.DB.QueryRow(query).Scan(&applicationTables); err != nil {
					t.Fatalf("inspect malformed-state mutation boundary: %v", err)
				}
				if applicationTables != 0 {
					t.Fatalf("malformed metadata %s created %d application table(s)", testCase.name, applicationTables)
				}
			})
		})
	}
}

func TestSchemaV17BackwardFenceRejectsMultiplePostgresRows(t *testing.T) {
	backendtest.ForEach(t, backendtest.Options{Postgres: backendtest.PostgresOptional, Schema: backendtest.EmptySchema}, func(t *testing.T, backend backendtest.Backend) {
		if backend.Kind != db.BackendPostgres {
			return
		}
		if _, err := backend.DB.Exec(`CREATE TABLE schema_version (version INTEGER PRIMARY KEY); INSERT INTO schema_version(version) VALUES (15), (16)`); err != nil {
			t.Fatalf("create multiple-row PostgreSQL fixture: %v", err)
		}
		if err := db.EnsureSchema(backend.DB); err == nil {
			t.Fatal("multiple PostgreSQL schema-version rows were accepted")
		}
		var got string
		if err := backend.DB.QueryRow(`SELECT string_agg(version::text, ',' ORDER BY version) FROM schema_version`).Scan(&got); err != nil {
			t.Fatalf("read rejected PostgreSQL metadata: %v", err)
		}
		if got != "15,16" {
			t.Fatalf("rejected PostgreSQL metadata = %q, want 15,16", got)
		}
	})
}

func TestSchemaV17BackwardFenceMigrationRollbackRestoresLegacyMetadata(t *testing.T) {
	backendtest.ForEach(t, backendtest.Options{Schema: backendtest.EmptySchema}, func(t *testing.T, backend backendtest.Backend) {
		if _, err := backend.DB.Exec(`CREATE TABLE schema_version (version INTEGER PRIMARY KEY); INSERT INTO schema_version(version) VALUES (16)`); err != nil {
			t.Fatalf("create rollback metadata fixture: %v", err)
		}
		// This deliberately incompatible table makes schema application fail after
		// metadata inspection. The migration transaction must retain the exact v16
		// column and row rather than exposing a partial fence.
		if _, err := backend.DB.Exec(`CREATE TABLE repository_config (key TEXT PRIMARY KEY)`); err != nil {
			t.Fatalf("create rollback failure fixture: %v", err)
		}

		if err := db.EnsureSchema(backend.DB); err == nil {
			t.Fatal("incompatible migration fixture unexpectedly succeeded")
		}
		columns := schemaVersionColumns(t, backend)
		if fmt.Sprint(columns) != "[version]" {
			t.Fatalf("rollback schema_version columns = %v, want [version]", columns)
		}
		var version, rows int
		if err := backend.DB.QueryRow(`SELECT COUNT(*), MIN(version) FROM schema_version`).Scan(&rows, &version); err != nil {
			t.Fatalf("read rolled-back legacy metadata: %v", err)
		}
		if rows != 1 || version != 16 {
			t.Fatalf("rolled-back legacy metadata = rows:%d version:%d, want rows:1 version:16", rows, version)
		}
	})
}
