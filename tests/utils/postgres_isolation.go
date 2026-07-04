package testutils

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/franchoy/coldkeep/internal/db"
	_ "github.com/lib/pq"
)

const isolatedDBMaintenanceEnv = "COLDKEEP_TEST_DB_MAINTENANCE"
const preserveFailureStateEnv = "COLDKEEP_TEST_PRESERVE_FAILURE_STATE"

var isolatedPostgresIdentifierPattern = regexp.MustCompile(`^[a-z0-9_]+$`)
var isolatedPostgresDBCounter uint64

type sqlExecDB interface {
	Exec(query string, args ...any) (sql.Result, error)
}

func PreserveFailureStateEnabled() bool {
	v := strings.TrimSpace(os.Getenv(preserveFailureStateEnv))
	return v == "1" || strings.EqualFold(v, "true") || strings.EqualFold(v, "yes")
}

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
	return runIsolatedPostgresSuite(adminDB, packageLabel, dbName, m)
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

func terminateAndDropIsolatedPostgresDB(adminDB sqlExecDB, dbName string) error {
	if adminDB == nil {
		return fmt.Errorf("admin database handle is nil")
	}
	if err := callIsolatedTrustedSQLExec(adminDB, `
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

func createIsolatedPostgresDB(adminDB sqlExecDB, dbName string) error {
	return callIsolatedTrustedSQLExec(adminDB, trustedCreateIsolatedPostgresDBStatement(dbName))
}

func dropIsolatedPostgresDB(adminDB sqlExecDB, dbName string) error {
	return callIsolatedTrustedSQLExec(adminDB, trustedDropIsolatedPostgresDBStatement(dbName))
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

func callIsolatedTrustedSQLExec(dbconn sqlExecDB, query string, args ...any) error {
	_, err := dbconn.Exec(query, args...)
	return err
}

func shouldPreserveFailedIsolatedPostgresDB(exitCode int) bool {
	return exitCode != 0 && PreserveFailureStateEnabled()
}

func shouldDropAsUnrelatedIsolatedPostgresDB(currentDB string) bool {
	if !DiagnosticManifestEnabled() {
		return false
	}

	paths, err := filepath.Glob(filepath.Join(DiagnosticDir(), "g6-failure-*.json"))
	if err != nil {
		return false
	}
	for _, path := range paths {
		data, err := os.ReadFile(path)
		if err != nil {
			continue
		}
		var payload struct {
			IsolatedDatabaseName string `json:"isolated_database_name"`
		}
		if err := json.Unmarshal(data, &payload); err != nil {
			continue
		}
		if strings.TrimSpace(payload.IsolatedDatabaseName) != "" && payload.IsolatedDatabaseName != currentDB {
			return true
		}
	}
	return false
}

func finalizeIsolatedPostgresDB(adminDB sqlExecDB, packageLabel, dbName string, exitCode int) int {
	if shouldPreserveFailedIsolatedPostgresDB(exitCode) {
		if shouldDropAsUnrelatedIsolatedPostgresDB(dbName) {
			fmt.Fprintf(os.Stderr, "dropping isolated postgres database %s because a richer diagnostic manifest already preserved a different failing database\n", dbName)
			if err := terminateAndDropIsolatedPostgresDB(adminDB, dbName); err != nil {
				fmt.Fprintf(os.Stderr, "drop isolated postgres database %s: %v\n", dbName, err)
			}
			return exitCode
		}
		if _, err := WritePreservedIsolatedDBManifest(packageLabel, dbName); err != nil {
			fmt.Fprintf(os.Stderr, "write isolated postgres diagnostic manifest for %s: %v\n", dbName, err)
		}
		fmt.Fprintf(os.Stderr, "preserving isolated postgres database %s because %s is enabled and test suite failed\n", dbName, preserveFailureStateEnv)
		return exitCode
	}
	if err := terminateAndDropIsolatedPostgresDB(adminDB, dbName); err != nil {
		fmt.Fprintf(os.Stderr, "drop isolated postgres database %s: %v\n", dbName, err)
		if exitCode == 0 {
			return 1
		}
	}
	return exitCode
}

func runIsolatedPostgresSuite(adminDB *sql.DB, packageLabel, dbName string, m *testing.M) int {
	previousDBName, hadPreviousDBName := os.LookupEnv("DB_NAME")
	if err := os.Setenv("DB_NAME", dbName); err != nil {
		fmt.Fprintf(os.Stderr, "set DB_NAME=%s: %v\n", dbName, err)
		if dropErr := terminateAndDropIsolatedPostgresDB(adminDB, dbName); dropErr != nil {
			fmt.Fprintf(os.Stderr, "drop isolated postgres database %s after setup failure: %v\n", dbName, dropErr)
		}
		return 1
	}
	exitCode := m.Run()
	restoreIsolatedPostgresDBEnv(previousDBName, hadPreviousDBName)
	return finalizeIsolatedPostgresDB(adminDB, packageLabel, dbName, exitCode)
}

func restoreIsolatedPostgresDBEnv(previousDBName string, hadPreviousDBName bool) {
	if hadPreviousDBName {
		_ = os.Setenv("DB_NAME", previousDBName)
		return
	}
	_ = os.Unsetenv("DB_NAME")
}
