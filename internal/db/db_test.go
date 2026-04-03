package db

import (
	"testing"
	"time"
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
