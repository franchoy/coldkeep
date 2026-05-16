package blocks

import (
	"os"
	"strings"
	"testing"
)

func TestLoadEncryptionKeyFailsWhenEnvNotSet(t *testing.T) {
	prev, hadPrev := os.LookupEnv("COLDKEEP_KEY")
	if err := os.Unsetenv("COLDKEEP_KEY"); err != nil {
		t.Fatalf("unset COLDKEEP_KEY: %v", err)
	}
	t.Cleanup(func() {
		if hadPrev {
			_ = os.Setenv("COLDKEEP_KEY", prev)
		} else {
			_ = os.Unsetenv("COLDKEEP_KEY")
		}
	})

	_, err := LoadEncryptionKey()
	if err == nil || !strings.Contains(err.Error(), "COLDKEEP_KEY must not be empty") {
		t.Fatalf("expected COLDKEEP_KEY-not-set error contract, got: %v", err)
	}
}

func TestLoadEncryptionKeyRejectsWhitespaceOnlyValue(t *testing.T) {
	t.Setenv("COLDKEEP_KEY", "   ")

	_, err := LoadEncryptionKey()
	if err == nil || !strings.Contains(err.Error(), "COLDKEEP_KEY must not be empty") {
		t.Fatalf("expected whitespace-empty key error, got: %v", err)
	}
}

func TestLoadEncryptionKeyRejectsNULValue(t *testing.T) {
	_, err := parseEncryptionKeyHex("aa\x00bb")
	if err == nil || !strings.Contains(err.Error(), "COLDKEEP_KEY must not contain NUL byte") {
		t.Fatalf("expected NUL key rejection error, got: %v", err)
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
