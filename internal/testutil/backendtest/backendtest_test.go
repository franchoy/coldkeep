package backendtest

import (
	"database/sql"
	"errors"
	"os"
	"strings"
	"testing"

	"github.com/franchoy/coldkeep/internal/db"
)

func TestForEachSQLiteFixture(t *testing.T) {
	var path string
	var names []string
	ForEach(t, Options{}, func(t *testing.T, backend Backend) {
		names = append(names, backend.Name)
		if backend.Name == "postgres" {
			if backend.Kind != db.BackendPostgres || !backend.Capabilities.SelectForUpdate || !backend.Capabilities.LiveGC {
				t.Fatalf("unexpected PostgreSQL backend: %+v", backend)
			}
			if _, err := backend.DB.Exec("CREATE TABLE fixture_probe_postgres (id INTEGER PRIMARY KEY)"); err != nil {
				t.Fatal(err)
			}
			return
		}
		if backend.Name != "sqlite" || backend.Kind != db.BackendSQLite {
			t.Fatalf("backend = %+v", backend)
		}
		if backend.Capabilities.SelectForUpdate || backend.Capabilities.LiveGC {
			t.Fatalf("unexpected SQLite capabilities: %+v", backend.Capabilities)
		}
		if _, err := backend.DB.Exec("CREATE TABLE fixture_probe (id INTEGER PRIMARY KEY)"); err != nil {
			t.Fatal(err)
		}
		if _, err := backend.DB.Exec("INSERT INTO fixture_probe (id) VALUES (1)"); err != nil {
			t.Fatal(err)
		}
		if err := backend.DB.QueryRow("PRAGMA database_list").Scan(new(int), new(string), &path); err != nil {
			t.Fatal(err)
		}
		other, err := sql.Open("sqlite3", path)
		if err != nil {
			t.Fatal(err)
		}
		defer other.Close()
		if err := db.ApplySQLiteSessionPragmas(other); err != nil {
			t.Fatal(err)
		}
		var got int
		if err := other.QueryRow("SELECT COUNT(*) FROM fixture_probe").Scan(&got); err != nil || got != 1 {
			t.Fatalf("second SQLite connection got %d, %v", got, err)
		}
	})
	if path == "" {
		t.Fatal("SQLite callback did not run")
	}
	wantNames := "sqlite"
	if os.Getenv("COLDKEEP_TEST_DB") != "" {
		wantNames += ",postgres"
	}
	if got := strings.Join(names, ","); got != wantNames {
		t.Fatalf("selected callback names = %q, want %q", got, wantNames)
	}
}

func TestScratchDatabaseName(t *testing.T) {
	a, b := scratchDatabaseName("Test Name/With Punctuation"), scratchDatabaseName("Test Name/With Punctuation")
	if a == b || len(a) > 63 || !validIdentifier.MatchString(a) {
		t.Fatalf("invalid scratch names %q %q", a, b)
	}
}

func TestSQLiteFixtureStateAndFilesAreIsolated(t *testing.T) {
	var closedPath string
	t.Run("first", func(t *testing.T) {
		ForEach(t, Options{}, func(t *testing.T, backend Backend) {
			if backend.Kind != db.BackendSQLite {
				return
			}
			if err := backend.DB.QueryRow("SELECT COUNT(*) FROM fixture_isolation_probe").Scan(new(int)); err == nil {
				t.Fatal("unexpected state from another SQLite fixture")
			}
			if _, err := backend.DB.Exec("CREATE TABLE fixture_isolation_probe (id INTEGER PRIMARY KEY)"); err != nil {
				t.Fatal(err)
			}
			if err := backend.DB.QueryRow("PRAGMA database_list").Scan(new(int), new(string), &closedPath); err != nil {
				t.Fatal(err)
			}
		})
	})
	if closedPath == "" {
		t.Fatal("SQLite fixture path was not observed")
	}
	if _, err := os.Stat(closedPath); !os.IsNotExist(err) {
		t.Fatalf("SQLite fixture path remains after callback cleanup: %q, err=%v", closedPath, err)
	}
	ForEach(t, Options{}, func(t *testing.T, backend Backend) {
		if backend.Kind != db.BackendSQLite {
			return
		}
		if err := backend.DB.QueryRow("SELECT COUNT(*) FROM fixture_isolation_probe").Scan(new(int)); err == nil {
			t.Fatal("SQLite state leaked into a separate harness invocation")
		}
	})
}

func TestPostgresSelectionPolicy(t *testing.T) {
	if err := postgresSelectionError(PostgresOptional, false); !errors.Is(err, errPostgresNotConfigured) {
		t.Fatalf("optional absent = %v", err)
	}
	if err := postgresSelectionError(PostgresRequired, false); !errors.Is(err, errPostgresNotConfigured) {
		t.Fatalf("required absent = %v", err)
	}
	if err := postgresSelectionError(PostgresRequired, true); err != nil {
		t.Fatal(err)
	}
	if err := postgresSelectionError(PostgresMode(99), true); err == nil {
		t.Fatal("invalid mode succeeded")
	}
}

type recordedExec struct {
	calls    []string
	failures map[string]error
}

func (e *recordedExec) Exec(query string, _ ...any) (sql.Result, error) {
	e.calls = append(e.calls, query)
	for key, err := range e.failures {
		if strings.Contains(query, key) {
			return nil, err
		}
	}
	return nil, nil
}

type recordedCloser struct {
	closed bool
	err    error
}

func (c *recordedCloser) Close() error { c.closed = true; return c.err }

func TestCleanupScratchDatabaseContinuesAfterErrors(t *testing.T) {
	exec := &recordedExec{failures: map[string]error{"pg_terminate": errors.New("terminate"), "DROP DATABASE": errors.New("drop")}}
	closer := &recordedCloser{err: errors.New("close")}
	var reports []string
	cleanupScratchDatabase(exec, closer, "coldkeep_bt_test_1", func(format string, args ...any) {
		reports = append(reports, format)
	})
	if !closer.closed || len(exec.calls) != 2 || !strings.Contains(exec.calls[1], "DROP DATABASE") {
		t.Fatalf("cleanup order: closed=%v calls=%v", closer.closed, exec.calls)
	}
	if len(reports) != 3 || !strings.Contains(reports[0], "close") || !strings.Contains(reports[2], "drop") {
		t.Fatalf("cleanup reports = %v", reports)
	}
}
