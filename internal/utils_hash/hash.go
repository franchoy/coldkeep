package utils_hash

import (
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
)

func ComputeFileHashHex(path string) (string, error) {

	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer func() { _ = file.Close() }()

	hash := sha256.New()

	_, err = io.Copy(hash, file)
	if err != nil {
		return "", err
	}

	return hex.EncodeToString(hash.Sum(nil)), nil
}
