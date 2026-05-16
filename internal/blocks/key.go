package blocks

import (
	"encoding/hex"
	"fmt"
	"os"
	"strings"
)

func LoadEncryptionKey() ([]byte, error) {
	keyHex, isSet := os.LookupEnv("COLDKEEP_KEY")
	if !isSet {
		return nil, fmt.Errorf("COLDKEEP_KEY must not be empty")
	}
	return parseEncryptionKeyHex(keyHex)
}

func parseEncryptionKeyHex(raw string) ([]byte, error) {
	keyHex := strings.TrimSpace(raw)
	if keyHex == "" {
		return nil, fmt.Errorf("COLDKEEP_KEY must not be empty")
	}
	if strings.ContainsRune(keyHex, '\x00') {
		return nil, fmt.Errorf("COLDKEEP_KEY must not contain NUL byte")
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
