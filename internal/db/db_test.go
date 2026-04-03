package db

import (
	"database/sql"
	"strings"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

func TestLoadSessionTimeoutFallsBackOnNonPositiveValues(t *testing.T) {
	const envVar = "COLDKEEP_DB_TEST_TIMEOUT_MS"
	defaultTimeout := 7 * time.Second

	t.Setenv(envVar, "0")
	if got := loadSessionTimeout(envVar, defaultTimeout); got != defaultTimeout {
		t.Fatalf("expected default timeout for zero value, got %v", got)
	}

	t.Setenv(envVar, "-25")
	if got := loadSessionTimeout(envVar, defaultTimeout); got != defaultTimeout {
		t.Fatalf("expected default timeout for negative value, got %v", got)
	}
}

func TestLoadSessionTimeoutUsesPositiveOverride(t *testing.T) {
	const envVar = "COLDKEEP_DB_TEST_TIMEOUT_MS"
	t.Setenv(envVar, "1234")

	got := loadSessionTimeout(envVar, 2*time.Second)
	want := 1234 * time.Millisecond
	if got != want {
		t.Fatalf("expected %v, got %v", want, got)
	}
}

func TestLoadMaxOpenConnsFallsBackOnNonPositive(t *testing.T) {
	t.Setenv("COLDKEEP_DB_MAX_OPEN_CONNS", "0")
	if got := loadMaxOpenConns(); got != 25 {
		t.Fatalf("expected default max open conns 25 for zero, got %d", got)
	}

	t.Setenv("COLDKEEP_DB_MAX_OPEN_CONNS", "-3")
	if got := loadMaxOpenConns(); got != 25 {
		t.Fatalf("expected default max open conns 25 for negative, got %d", got)
	}
}

func TestLoadMaxIdleConnsAllowsZeroButNotNegative(t *testing.T) {
	t.Setenv("COLDKEEP_DB_MAX_IDLE_CONNS", "0")
	if got := loadMaxIdleConns(); got != 0 {
		t.Fatalf("expected max idle conns 0, got %d", got)
	}

	t.Setenv("COLDKEEP_DB_MAX_IDLE_CONNS", "-1")
	if got := loadMaxIdleConns(); got != 5 {
		t.Fatalf("expected default max idle conns 5 for negative, got %d", got)
	}
}

func TestLoadConnMaxLifetimeFallsBackOnNegative(t *testing.T) {
	t.Setenv("COLDKEEP_DB_CONN_MAX_LIFETIME_MS", "-1")
	if got := loadConnMaxLifetime(); got != 30*time.Minute {
		t.Fatalf("expected default conn max lifetime 30m for negative, got %v", got)
	}
}

func TestLoadConnectTimeoutFallbackAndOverride(t *testing.T) {
	t.Setenv("COLDKEEP_DB_CONNECT_TIMEOUT_MS", "0")
	if got := loadConnectTimeout(); got != 5*time.Second {
		t.Fatalf("expected default connect timeout 5s for zero, got %v", got)
	}

	t.Setenv("COLDKEEP_DB_CONNECT_TIMEOUT_MS", "1500")
	if got := loadConnectTimeout(); got != 1500*time.Millisecond {
		t.Fatalf("expected connect timeout 1500ms, got %v", got)
	}
}

func TestLoadOperationTimeoutFallbackAndOverride(t *testing.T) {
	t.Setenv("COLDKEEP_DB_OPERATION_TIMEOUT_MS", "-10")
	if got := loadOperationTimeout(); got != 5*time.Minute {
		t.Fatalf("expected default operation timeout 5m for negative, got %v", got)
	}

	t.Setenv("COLDKEEP_DB_OPERATION_TIMEOUT_MS", "60000")
	if got := loadOperationTimeout(); got != 60*time.Second {
		t.Fatalf("expected operation timeout 60s, got %v", got)
	}
}

func TestLoadConnMaxIdleTimeFallbackAndOverride(t *testing.T) {
	t.Setenv("COLDKEEP_DB_CONN_MAX_IDLE_TIME_MS", "-1")
	if got := loadConnMaxIdleTime(); got != 5*time.Minute {
		t.Fatalf("expected default conn max idle time 5m for negative, got %v", got)
	}

	t.Setenv("COLDKEEP_DB_CONN_MAX_IDLE_TIME_MS", "90000")
	if got := loadConnMaxIdleTime(); got != 90*time.Second {
		t.Fatalf("expected conn max idle time 90s, got %v", got)
	}
}

func TestApplySQLiteSessionPragmasSetsBusyTimeout(t *testing.T) {
	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	defer func() { _ = dbconn.Close() }()

	if err := ApplySQLiteSessionPragmas(dbconn); err != nil {
		t.Fatalf("apply sqlite session pragmas: %v", err)
	}

	var busyTimeout int
	if err := dbconn.QueryRow(`PRAGMA busy_timeout;`).Scan(&busyTimeout); err != nil {
		t.Fatalf("query busy_timeout pragma: %v", err)
	}

	want := int(DefaultStatementTimeout() / time.Millisecond)
	if want <= 0 {
		want = 1
	}
	if busyTimeout != want {
		t.Fatalf("unexpected busy_timeout: got %d want %d", busyTimeout, want)
	}
}

func TestApplySQLiteSessionPragmasFailsWhenDBIsClosed(t *testing.T) {
	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	if err := dbconn.Close(); err != nil {
		t.Fatalf("close sqlite db: %v", err)
	}

	err = ApplySQLiteSessionPragmas(dbconn)
	if err == nil {
		t.Fatalf("expected error when applying pragmas to closed db")
	}
}

func TestBuildConnectionOptionsUsesCurrentTimeoutGlobals(t *testing.T) {
	origStatement := statementTimeout
	origLock := lockTimeout
	origIdleInTx := idleInTransactionTimeout
	t.Cleanup(func() {
		statementTimeout = origStatement
		lockTimeout = origLock
		idleInTransactionTimeout = origIdleInTx
	})

	statementTimeout = 1500 * time.Millisecond
	lockTimeout = 2750 * time.Millisecond
	idleInTransactionTimeout = 4200 * time.Millisecond

	options := buildConnectionOptions()
	if !strings.Contains(options, "-c statement_timeout=1500") {
		t.Fatalf("expected statement timeout option in %q", options)
	}
	if !strings.Contains(options, "-c lock_timeout=2750") {
		t.Fatalf("expected lock timeout option in %q", options)
	}
	if !strings.Contains(options, "-c idle_in_transaction_session_timeout=4200") {
		t.Fatalf("expected idle-in-tx timeout option in %q", options)
	}
}
