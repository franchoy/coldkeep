package blocks

import (
	"encoding/hex"
	"testing"
)

func TestGenerateKeyHexReturns64HexChars(t *testing.T) {
	keyHex, err := GenerateKeyHex()
	if err != nil {
		t.Fatalf("generate key hex: %v", err)
	}

	if len(keyHex) != 64 {
		t.Fatalf("expected 64 hex chars, got %d: %q", len(keyHex), keyHex)
	}

	decoded, err := hex.DecodeString(keyHex)
	if err != nil {
		t.Fatalf("generated key is not valid hex: %v", err)
	}
	if len(decoded) != 32 {
		t.Fatalf("expected 32 decoded bytes, got %d", len(decoded))
	}
}

func TestGenerateKeyHexProducesDifferentValuesAcrossCalls(t *testing.T) {
	first, err := GenerateKeyHex()
	if err != nil {
		t.Fatalf("first generate key hex: %v", err)
	}
	second, err := GenerateKeyHex()
	if err != nil {
		t.Fatalf("second generate key hex: %v", err)
	}

	if first == second {
		t.Fatalf("expected distinct generated keys across calls, got duplicate: %q", first)
	}
}
