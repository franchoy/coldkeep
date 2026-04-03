package blocks

import (
	"strings"
	"testing"
)

func TestLoadEncryptionKeyFailsWhenEnvNotSet(t *testing.T) {
	t.Setenv("COLDKEEP_KEY", "")

	_, err := LoadEncryptionKey()
	if err == nil || !strings.Contains(err.Error(), "COLDKEEP_KEY not set") {
		t.Fatalf("expected COLDKEEP_KEY-not-set error contract, got: %v", err)
	}
}

func TestLoadEncryptionKeyFailsOnInvalidHexEncoding(t *testing.T) {
	t.Setenv("COLDKEEP_KEY", "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz")

	_, err := LoadEncryptionKey()
	if err == nil || !strings.Contains(err.Error(), "invalid key encoding") {
		t.Fatalf("expected invalid-key-encoding error contract, got: %v", err)
	}
}

func TestLoadEncryptionKeyFailsWhenKeyIsTooShort(t *testing.T) {
	// 31 bytes = 62 hex chars — valid hex but wrong length.
	t.Setenv("COLDKEEP_KEY", strings.Repeat("ab", 31))

	_, err := LoadEncryptionKey()
	if err == nil || !strings.Contains(err.Error(), "key must be 32 bytes (AES-256)") {
		t.Fatalf("expected key-length error contract, got: %v", err)
	}
}
