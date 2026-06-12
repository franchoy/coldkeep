package catalog_test

import (
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"

	"github.com/franchoy/coldkeep/internal/db"
)

// catalogBackend describes a database backend the catalog contract tests run
// against. SQLite always runs; PostgreSQL runs only when COLDKEEP_TEST_DB is set
// (the project-wide convention) and skips cleanly otherwise.
type catalogBackend struct {
	Name string
	Open func(t *testing.T) *sql.DB
}

// catalogBackends returns the backends the dual-backend contract suite exercises.
// The SQLite backend is unconditional. The PostgreSQL backend follows the
// existing project convention used by internal/db/migrations_test.go: it reads a
// DSN from the environment and is gated by COLDKEEP_TEST_DB. CI provides a
// postgres service and sets COLDKEEP_TEST_DB=1, so the PostgreSQL path runs in CI
// even though it skips during local development without a configured database.
func catalogBackends() []catalogBackend {
	return []catalogBackend{
		{Name: "sqlite", Open: openSQLiteCatalogTestDB},
		{Name: "postgres", Open: openPostgresCatalogTestDBOrSkip},
	}
}

// openSQLiteCatalogTestDB opens an in-memory SQLite database with the coldkeep
// schema applied. It mirrors the openTestDB helper used by the other catalog
// tests and is always available.
func openSQLiteCatalogTestDB(t *testing.T) *sql.DB {
	t.Helper()
	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("sql.Open sqlite3: %v", err)
	}
	t.Cleanup(func() { _ = dbconn.Close() })
	if err := db.RunMigrations(dbconn); err != nil {
		t.Fatalf("RunMigrations: %v", err)
	}
	return dbconn
}

// openPostgresCatalogTestDBOrSkip opens the configured PostgreSQL test database
// and applies the coldkeep schema to it. The fixture seeding is idempotent, so
// this helper provisions a fresh temporary database per test so idempotent
// fixture inserts cannot leak state between PostgreSQL subtests.
func openPostgresCatalogTestDBOrSkip(t *testing.T) *sql.DB {
	t.Helper()
	if os.Getenv("COLDKEEP_TEST_DB") == "" {
		t.Skip("Set COLDKEEP_TEST_DB=1 (with DB_* DSN env) to run PostgreSQL catalog contract tests")
	}

	cfg := loadPostgresCatalogTestConfig()
	adminDB := openPostgresCatalogAdminConnection(t, cfg)
	testDBName := fmt.Sprintf("coldkeep_catalog_contract_%d", time.Now().UnixNano())
	if _, err := adminDB.Exec(fmt.Sprintf("CREATE DATABASE %s", testDBName)); err != nil {
		t.Fatalf("create temporary postgres catalog database %s: %v", testDBName, err)
	}
	t.Cleanup(func() {
		_, _ = adminDB.Exec(`
			SELECT pg_terminate_backend(pid)
			FROM pg_stat_activity
			WHERE datname = $1 AND pid <> pg_backend_pid()
		`, testDBName)
		_, _ = adminDB.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS %s", testDBName))
		_ = adminDB.Close()
	})

	dbconn := openPostgresCatalogTestConnection(t, cfg, testDBName, "test database")

	t.Setenv("COLDKEEP_DB_AUTO_BOOTSTRAP", "true")
	if err := db.EnsurePostgresSchema(dbconn); err != nil {
		_ = dbconn.Close()
		t.Fatalf("apply postgres schema to %s: %v", cfg.Database, err)
	}

	t.Cleanup(func() { _ = dbconn.Close() })
	return dbconn
}

func openPostgresCatalogAdminConnection(t *testing.T, cfg postgresCatalogTestConfig) *sql.DB {
	t.Helper()
	maintenanceDB := getenvOrDefaultCatalogTest("COLDKEEP_TEST_DB_MAINTENANCE", "postgres")
	return openPostgresCatalogTestConnection(t, cfg, maintenanceDB, "admin")
}

type postgresCatalogTestConfig struct {
	Host     string
	Port     string
	User     string
	Password string
	SSLMode  string
	Database string
}

func loadPostgresCatalogTestConfig() postgresCatalogTestConfig {
	return postgresCatalogTestConfig{
		Host:     getenvOrDefaultCatalogTest("DB_HOST", "127.0.0.1"),
		Port:     getenvOrDefaultCatalogTest("DB_PORT", "5432"),
		User:     getenvOrDefaultCatalogTest("DB_USER", "coldkeep"),
		Password: getenvOrDefaultCatalogTest("DB_PASSWORD", "coldkeep"),
		SSLMode:  getenvOrDefaultCatalogTest("DB_SSLMODE", "disable"),
		Database: getenvOrDefaultCatalogTest("DB_NAME", "coldkeep"),
	}
}

func openPostgresCatalogTestConnection(t *testing.T, cfg postgresCatalogTestConfig, databaseName, purpose string) *sql.DB {
	t.Helper()
	dbconn, err := sql.Open("postgres", postgresCatalogTestConnString(cfg, databaseName))
	if err != nil {
		t.Fatalf("open postgres %s connection: %v", purpose, err)
	}
	if err := dbconn.Ping(); err != nil {
		_ = dbconn.Close()
		t.Fatalf("ping postgres %s connection: %v", purpose, err)
	}
	return dbconn
}

func postgresCatalogTestConnString(cfg postgresCatalogTestConfig, databaseName string) string {
	return fmt.Sprintf(
		"host=%s port=%s user=%s password=%s dbname=%s sslmode=%s connect_timeout=5",
		cfg.Host, cfg.Port, cfg.User, cfg.Password, databaseName, cfg.SSLMode,
	)
}

func getenvOrDefaultCatalogTest(key, fallback string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return fallback
}
