package testutils

import (
	"database/sql"
	"fmt"
	"os"
	"regexp"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/franchoy/coldkeep/internal/db"
	_ "github.com/lib/pq"
)

const isolatedDBMaintenanceEnv = "COLDKEEP_TEST_DB_MAINTENANCE"

var isolatedPostgresIdentifierPattern = regexp.MustCompile(`^[a-z0-9_]+$`)
var isolatedPostgresDBCounter uint64

// RunWithIsolatedPostgresDB executes a package test suite against a unique
// PostgreSQL database so concurrently running test binaries do not share rows.
func RunWithIsolatedPostgresDB(packageLabel string, m *testing.M) int {
	if os.Getenv("COLDKEEP_TEST_DB") == "" {
		return m.Run()
	}

	adminDB := OpenRawPostgresDBForMaintenance(packageLabel)
	if adminDB == nil {
		return 1
	}
	defer func() { _ = adminDB.Close() }()

	dbName := isolatedPostgresDBName(packageLabel)
	if err := createIsolatedPostgresDB(adminDB, dbName); err != nil {
		fmt.Fprintf(os.Stderr, "create isolated postgres database %s: %v\n", dbName, err)
		return 1
	}

	previousDBName, hadPreviousDBName := os.LookupEnv("DB_NAME")
	if err := os.Setenv("DB_NAME", dbName); err != nil {
		fmt.Fprintf(os.Stderr, "set DB_NAME=%s: %v\n", dbName, err)
		if dropErr := terminateAndDropIsolatedPostgresDB(adminDB, dbName); dropErr != nil {
			fmt.Fprintf(os.Stderr, "drop isolated postgres database %s after setup failure: %v\n", dbName, dropErr)
		}
		return 1
	}

	exitCode := m.Run()

	if hadPreviousDBName {
		_ = os.Setenv("DB_NAME", previousDBName)
	} else {
		_ = os.Unsetenv("DB_NAME")
	}

	if err := terminateAndDropIsolatedPostgresDB(adminDB, dbName); err != nil {
		fmt.Fprintf(os.Stderr, "drop isolated postgres database %s: %v\n", dbName, err)
		if exitCode == 0 {
			return 1
		}
	}

	return exitCode
}

func OpenRawPostgresDBForMaintenance(purpose string) *sql.DB {
	maintenanceDB := strings.TrimSpace(os.Getenv(isolatedDBMaintenanceEnv))
	if maintenanceDB == "" {
		maintenanceDB = "postgres"
	}
	connStr, err := db.BuildPostgresConnStringFromEnv(maintenanceDB)
	if err != nil {
		fmt.Fprintf(os.Stderr, "build postgres DSN for %s: %v\n", purpose, err)
		return nil
	}
	rawDB, err := sql.Open("postgres", connStr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "open postgres admin DB for %s: %v\n", purpose, err)
		return nil
	}
	if err := rawDB.Ping(); err != nil {
		_ = rawDB.Close()
		fmt.Fprintf(os.Stderr, "ping postgres admin DB for %s: %v\n", purpose, err)
		return nil
	}
	return rawDB
}

func isolatedPostgresDBName(packageLabel string) string {
	sanitized := strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z':
			return r
		case r >= 'A' && r <= 'Z':
			return r + ('a' - 'A')
		case r >= '0' && r <= '9':
			return r
		default:
			return '_'
		}
	}, packageLabel)
	sanitized = strings.Trim(sanitized, "_")
	if sanitized == "" {
		sanitized = "pkg"
	}
	return fmt.Sprintf("coldkeep_%s_%d", sanitized, atomic.AddUint64(&isolatedPostgresDBCounter, 1))
}

func terminateAndDropIsolatedPostgresDB(adminDB *sql.DB, dbName string) error {
	if adminDB == nil {
		return fmt.Errorf("admin database handle is nil")
	}
	if _, err := adminDB.Exec(`
		SELECT pg_terminate_backend(pid)
		FROM pg_stat_activity
		WHERE datname = $1 AND pid <> pg_backend_pid()
	`, dbName); err != nil {
		return fmt.Errorf("terminate active sessions for %s: %w", dbName, err)
	}
	if err := dropIsolatedPostgresDB(adminDB, dbName); err != nil {
		return fmt.Errorf("drop database %s: %w", dbName, err)
	}
	return nil
}

func createIsolatedPostgresDB(adminDB *sql.DB, dbName string) error {
	_, err := adminDB.Exec(trustedCreateIsolatedPostgresDBStatement(dbName))
	return err
}

func dropIsolatedPostgresDB(adminDB *sql.DB, dbName string) error {
	_, err := adminDB.Exec(trustedDropIsolatedPostgresDBStatement(dbName))
	return err
}

func trustedCreateIsolatedPostgresDBStatement(dbName string) string {
	return fmt.Sprintf("CREATE DATABASE %s", trustedIsolatedPostgresIdentifier(dbName))
}

func trustedDropIsolatedPostgresDBStatement(dbName string) string {
	return fmt.Sprintf("DROP DATABASE IF EXISTS %s", trustedIsolatedPostgresIdentifier(dbName))
}

func trustedIsolatedPostgresIdentifier(dbName string) string {
	if !isolatedPostgresIdentifierPattern.MatchString(dbName) {
		panic("unexpected isolated postgres database name")
	}
	return `"` + dbName + `"`
}
