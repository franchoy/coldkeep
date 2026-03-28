package blocks

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
)

func GenerateKeyHex() (string, error) {
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		return "", fmt.Errorf("generate key: %w", err)
	}
	return hex.EncodeToString(key), nil
}
