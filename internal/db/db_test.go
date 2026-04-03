package db

import (
	"context"
	"database/sql"
	"errors"
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

func TestNewOperationContextWithNilParentUsesOperationTimeout(t *testing.T) {
	origOperationTimeout := operationTimeout
	t.Cleanup(func() { operationTimeout = origOperationTimeout })
	operationTimeout = 75 * time.Millisecond

	ctx, cancel := NewOperationContext(nil)
	defer cancel()

	if ctx == nil {
		t.Fatalf("expected non-nil context")
	}
	deadline, ok := ctx.Deadline()
	if !ok {
		t.Fatalf("expected context deadline to be set")
	}
	remaining := time.Until(deadline)
	if remaining <= 0 || remaining > 250*time.Millisecond {
		t.Fatalf("unexpected remaining deadline window: %v", remaining)
	}
}

func TestNewOperationContextCancelFuncCancelsContext(t *testing.T) {
	ctx, cancel := NewOperationContext(nil)
	cancel()

	if !errors.Is(ctx.Err(), context.Canceled) {
		t.Fatalf("expected canceled context after cancel(), got: %v", ctx.Err())
	}
}

func TestLoadStatementTimeoutFallbackAndOverride(t *testing.T) {
	t.Setenv("COLDKEEP_DB_STATEMENT_TIMEOUT_MS", "0")
	if got := loadStatementTimeout(); got != 30*time.Second {
		t.Fatalf("expected default statement timeout 30s, got %v", got)
	}

	t.Setenv("COLDKEEP_DB_STATEMENT_TIMEOUT_MS", "2500")
	if got := loadStatementTimeout(); got != 2500*time.Millisecond {
		t.Fatalf("expected statement timeout 2500ms, got %v", got)
	}
}

func TestLoadLockTimeoutFallbackAndOverride(t *testing.T) {
	t.Setenv("COLDKEEP_DB_LOCK_TIMEOUT_MS", "-1")
	if got := loadLockTimeout(); got != 5*time.Second {
		t.Fatalf("expected default lock timeout 5s, got %v", got)
	}

	t.Setenv("COLDKEEP_DB_LOCK_TIMEOUT_MS", "3200")
	if got := loadLockTimeout(); got != 3200*time.Millisecond {
		t.Fatalf("expected lock timeout 3200ms, got %v", got)
	}
}

func TestLoadIdleInTransactionTimeoutFallbackAndOverride(t *testing.T) {
	t.Setenv("COLDKEEP_DB_IDLE_IN_TX_TIMEOUT_MS", "0")
	if got := loadIdleInTransactionTimeout(); got != 60*time.Second {
		t.Fatalf("expected default idle-in-tx timeout 60s, got %v", got)
	}

	t.Setenv("COLDKEEP_DB_IDLE_IN_TX_TIMEOUT_MS", "7000")
	if got := loadIdleInTransactionTimeout(); got != 7*time.Second {
		t.Fatalf("expected idle-in-tx timeout 7s, got %v", got)
	}
}

func TestDefaultTimeoutAccessorsReflectCurrentGlobals(t *testing.T) {
	origOperation := operationTimeout
	origStatement := statementTimeout
	t.Cleanup(func() {
		operationTimeout = origOperation
		statementTimeout = origStatement
	})

	operationTimeout = 12 * time.Second
	statementTimeout = 34 * time.Second

	if got := DefaultOperationTimeout(); got != 12*time.Second {
		t.Fatalf("expected DefaultOperationTimeout=12s, got %v", got)
	}
	if got := DefaultStatementTimeout(); got != 34*time.Second {
		t.Fatalf("expected DefaultStatementTimeout=34s, got %v", got)
	}
}

func TestLoadMaxOpenConnsUsesPositiveOverride(t *testing.T) {
	t.Setenv("COLDKEEP_DB_MAX_OPEN_CONNS", "33")
	if got := loadMaxOpenConns(); got != 33 {
		t.Fatalf("expected max open conns 33, got %d", got)
	}
}

func TestLoadMaxIdleConnsUsesPositiveOverride(t *testing.T) {
	t.Setenv("COLDKEEP_DB_MAX_IDLE_CONNS", "8")
	if got := loadMaxIdleConns(); got != 8 {
		t.Fatalf("expected max idle conns 8, got %d", got)
	}
}

func TestLoadConnMaxLifetimeAllowsZero(t *testing.T) {
	t.Setenv("COLDKEEP_DB_CONN_MAX_LIFETIME_MS", "0")
	if got := loadConnMaxLifetime(); got != 0 {
		t.Fatalf("expected conn max lifetime 0 for zero override, got %v", got)
	}
}

func TestLoadConnMaxLifetimeUsesPositiveOverride(t *testing.T) {
	t.Setenv("COLDKEEP_DB_CONN_MAX_LIFETIME_MS", "45000")
	if got := loadConnMaxLifetime(); got != 45*time.Second {
		t.Fatalf("expected conn max lifetime 45s, got %v", got)
	}
}

func TestLoadConnMaxIdleTimeAllowsZero(t *testing.T) {
	t.Setenv("COLDKEEP_DB_CONN_MAX_IDLE_TIME_MS", "0")
	if got := loadConnMaxIdleTime(); got != 0 {
		t.Fatalf("expected conn max idle time 0 for zero override, got %v", got)
	}
}

func TestLoadConnectTimeoutFallsBackOnNegativeValues(t *testing.T) {
	t.Setenv("COLDKEEP_DB_CONNECT_TIMEOUT_MS", "-1")
	if got := loadConnectTimeout(); got != 5*time.Second {
		t.Fatalf("expected default connect timeout 5s for negative value, got %v", got)
	}
}
