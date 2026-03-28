package blocks

import (
	"encoding/hex"
	"fmt"
	"os"
)

func LoadEncryptionKey() ([]byte, error) {
	keyHex := os.Getenv("COLDKEEP_KEY")
	if keyHex == "" {
		return nil, fmt.Errorf("COLDKEEP_KEY not set")
	}

	key, err := hex.DecodeString(keyHex)
	if err != nil {
		return nil, fmt.Errorf("invalid key encoding: %w", err)
	}

	if len(key) != 32 {
		return nil, fmt.Errorf("key must be 32 bytes (AES-256)")
	}

	return key, nil
}
