package testgate

import (
	"os"
	"strings"
	"testing"
)

const testAESGCMKeyHex = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

func RequireDB(t *testing.T) {
	t.Helper()
	if os.Getenv("COLDKEEP_TEST_DB") == "" {
		t.Skip("Set COLDKEEP_TEST_DB=1 to run integration tests")
	}
	if strings.TrimSpace(os.Getenv("COLDKEEP_CODEC")) == "aes-gcm" && strings.TrimSpace(os.Getenv("COLDKEEP_KEY")) == "" {
		t.Setenv("COLDKEEP_KEY", testAESGCMKeyHex)
	}
}

func RequireStress(t *testing.T) {
	t.Helper()
	if testing.Short() {
		t.Skip("skipping stress test in -short mode")
	}
}

func RequireLongRun(t *testing.T) {
	t.Helper()
	if testing.Short() {
		t.Skip("skipping long-run test in -short mode")
	}
	if os.Getenv("COLDKEEP_LONG_RUN") == "" {
		t.Skip("Set COLDKEEP_LONG_RUN=1 to run long-run integration tests")
	}
}
