package utils_hash

import (
	"os"
	"path/filepath"
	"testing"
)

func TestComputeFileHashHexKnownPayload(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "payload.txt")
	if err := os.WriteFile(path, []byte("hello\n"), 0o644); err != nil {
		t.Fatalf("write payload: %v", err)
	}

	got, err := ComputeFileHashHex(path)
	if err != nil {
		t.Fatalf("ComputeFileHashHex error: %v", err)
	}

	const want = "5891b5b522d5df086d0ff0b110fbd9d21bb4fc7163af34d08286a2e846f6be03"
	if got != want {
		t.Fatalf("hash mismatch: got=%q want=%q", got, want)
	}
}

func TestComputeFileHashHexMissingFileReturnsError(t *testing.T) {
	if _, err := ComputeFileHashHex(filepath.Join(t.TempDir(), "missing.txt")); err == nil {
		t.Fatalf("expected error for missing file")
	}
}
