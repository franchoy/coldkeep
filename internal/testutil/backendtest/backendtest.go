// Package backendtest provides isolated SQLite and optional PostgreSQL fixtures
// for package-level backend contract tests. It is test support, not a runtime
// database abstraction.
package backendtest

import (
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/franchoy/coldkeep/internal/db"
)

// PostgresMode controls how ForEach handles PostgreSQL.
type PostgresMode int

const (
	// PostgresOptional runs PostgreSQL only when COLDKEEP_TEST_DB is set.
	PostgresOptional PostgresMode = iota
	// PostgresRequired fails when PostgreSQL was not explicitly configured.
	PostgresRequired
)

// SchemaMode controls whether a fixture is bootstrapped before its callback.
type SchemaMode int

const (
	// CurrentSchema applies the current Coldkeep schema.
	CurrentSchema SchemaMode = iota
	// EmptySchema opens the database without applying schema.
	EmptySchema
)

// Options configures a dual-backend fixture. The zero value uses CurrentSchema
// and optional PostgreSQL.
type Options struct {
	Postgres PostgresMode
	Schema   SchemaMode
}

// Capabilities records current backend-specific behavior. It does not cause
// automatic skips; callers must assert their intended backend behavior.
type Capabilities struct {
	SelectForUpdate bool
	SkipLocked      bool
	Nowait          bool
	LiveGC          bool
}

// Backend is one isolated fixture passed to a contract-test callback.
type Backend struct {
	Name         string
	Kind         db.Backend
	DB           *sql.DB
	Capabilities Capabilities
}

var scratchCounter atomic.Uint64

// ForEach invokes fn for SQLite and for PostgreSQL when selected by options.
// PostgreSQL subtests are skipped only for optional mode with no
// COLDKEEP_TEST_DB setting; any configured setup failure is a test failure.
func ForEach(t *testing.T, options Options, fn func(t *testing.T, backend Backend)) {
	t.Helper()
	t.Run("sqlite", func(t *testing.T) {
		fn(t, openSQLite(t, options.Schema))
	})
	t.Run("postgres", func(t *testing.T) {
		if err := postgresSelectionError(options.Postgres, os.Getenv("COLDKEEP_TEST_DB") != ""); err != nil {
			if errors.Is(err, errPostgresNotConfigured) && options.Postgres == PostgresOptional {
				t.Skip("set COLDKEEP_TEST_DB=1 (with DB_* connection settings) to run PostgreSQL backend tests")
			}
			t.Fatal(err)
		}
		fn(t, openPostgres(t, options.Schema))
	})
}

func openSQLite(t *testing.T, schema SchemaMode) Backend {
	t.Helper()
	path := filepath.Join(t.TempDir(), "coldkeep.sqlite")
	conn, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatalf("open SQLite fixture: %v", err)
	}
	conn.SetMaxOpenConns(1)
	t.Cleanup(func() {
		if err := conn.Close(); err != nil {
			t.Errorf("close SQLite fixture %q: %v", path, err)
		}
	})
	if err := db.ApplySQLiteSessionPragmas(conn); err != nil {
		t.Fatalf("initialize SQLite fixture session: %v", err)
	}
	ensureSchema(t, conn, schema, "SQLite")
	return backendFor("sqlite", conn)
}

func openPostgres(t *testing.T, schema SchemaMode) Backend {
	t.Helper()
	adminName := getenvOrDefault("COLDKEEP_TEST_DB_MAINTENANCE", "postgres")
	admin := openPostgresConnection(t, adminName, "admin")
	t.Cleanup(func() {
		if err := admin.Close(); err != nil {
			t.Errorf("close PostgreSQL admin connection: %v", err)
		}
	})

	name := scratchDatabaseName(t.Name())
	if _, err := admin.Exec("CREATE DATABASE " + quoteIdentifier(name)); err != nil {
		t.Fatalf("create PostgreSQL scratch database %q: %v", name, err)
	}

	var tested *sql.DB
	// This cleanup is registered after the admin close cleanup, so it runs first.
	// It remains valid if a later open or bootstrap step calls Fatal.
	t.Cleanup(func() {
		cleanupScratchDatabase(admin, tested, name, func(format string, args ...any) {
			t.Errorf(format, args...)
		})
	})
	tested = openPostgresConnection(t, name, "scratch database")
	ensureSchema(t, tested, schema, "PostgreSQL")
	return backendFor("postgres", tested)
}

func ensureSchema(t *testing.T, conn *sql.DB, schema SchemaMode, backend string) {
	t.Helper()
	if schema == EmptySchema {
		return
	}
	if schema != CurrentSchema {
		t.Fatalf("unsupported %s fixture schema mode %d", backend, schema)
	}
	if err := db.EnsureSchema(conn); err != nil {
		t.Fatalf("bootstrap %s fixture schema: %v", backend, err)
	}
}

func backendFor(name string, conn *sql.DB) Backend {
	kind := db.BackendFromDB(conn)
	locks := db.SupportsSelectForUpdate(conn)
	return Backend{Name: name, Kind: kind, DB: conn, Capabilities: Capabilities{
		SelectForUpdate: locks,
		SkipLocked:      db.SupportsSelectForUpdateSkipLocked(conn),
		Nowait:          db.SupportsSelectForUpdateNowait(conn),
		LiveGC:          kind == db.BackendPostgres,
	}}
}

func openPostgresConnection(t *testing.T, databaseName, purpose string) *sql.DB {
	t.Helper()
	connString, err := db.BuildPostgresConnStringFromEnv(databaseName)
	if err != nil {
		t.Fatalf("build PostgreSQL %s connection string: %v", purpose, err)
	}
	conn, err := sql.Open("postgres", connString)
	if err != nil {
		t.Fatalf("open PostgreSQL %s connection: %v", purpose, err)
	}
	if err := conn.Ping(); err != nil {
		_ = conn.Close()
		t.Fatalf("ping PostgreSQL %s connection: %v", purpose, err)
	}
	return conn
}

type sqlExecutor interface {
	Exec(query string, args ...any) (sql.Result, error)
}
type dbCloser interface{ Close() error }

func cleanupScratchDatabase(admin sqlExecutor, tested dbCloser, name string, report func(string, ...any)) {
	if tested != nil {
		if err := tested.Close(); err != nil {
			report("close PostgreSQL scratch database %q: %v", name, err)
		}
	}
	if _, err := admin.Exec(`SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = $1 AND pid <> pg_backend_pid()`, name); err != nil {
		report("terminate sessions for PostgreSQL scratch database %q: %v", name, err)
	}
	if _, err := admin.Exec("DROP DATABASE IF EXISTS " + quoteIdentifier(name)); err != nil {
		report("drop PostgreSQL scratch database %q: %v", name, err)
	}
}

var validIdentifier = regexp.MustCompile(`^[a-z][a-z0-9_]*$`)

func scratchDatabaseName(testName string) string {
	const prefix = "coldkeep_bt_"
	suffix := fmt.Sprintf("_%x_%x", os.Getpid(), scratchCounter.Add(1))
	name := strings.ToLower(testName)
	name = strings.Map(func(r rune) rune {
		if r >= 'a' && r <= 'z' || r >= '0' && r <= '9' {
			return r
		}
		return '_'
	}, name)
	name = strings.Trim(name, "_")
	if name == "" {
		name = "test"
	}
	maxBase := 63 - len(prefix) - len(suffix)
	if len(name) > maxBase {
		name = name[:maxBase]
	}
	return prefix + name + suffix
}

func quoteIdentifier(name string) string {
	if !validIdentifier.MatchString(name) {
		panic("invalid generated PostgreSQL identifier: " + name)
	}
	return `"` + name + `"`
}

var errPostgresNotConfigured = errors.New("COLDKEEP_TEST_DB is required for PostgreSQL backend tests")

func postgresSelectionError(mode PostgresMode, configured bool) error {
	if mode != PostgresOptional && mode != PostgresRequired {
		return fmt.Errorf("unsupported PostgreSQL mode %d", mode)
	}
	if !configured {
		return errPostgresNotConfigured
	}
	return nil
}

func getenvOrDefault(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
