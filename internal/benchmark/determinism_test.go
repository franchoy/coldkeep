package benchmark

import (
	"os"
	"path/filepath"
	"testing"
)

func TestHashRestoredTreeEmptyDirectory(t *testing.T) {
	dir := t.TempDir()
	hashes, err := HashRestoredTree(dir)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(hashes) != 0 {
		t.Fatalf("expected empty map for empty directory, got %v", hashes)
	}
}

func TestHashRestoredTreeSingleFile(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "a.txt"), []byte("hello"), 0o644); err != nil {
		t.Fatal(err)
	}

	hashes, err := HashRestoredTree(dir)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(hashes) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(hashes))
	}
	const wantHex = "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
	if got := hashes["a.txt"]; got != wantHex {
		t.Errorf("hash mismatch: got %s, want %s", got, wantHex)
	}
}

func TestHashRestoredTreeNestedFiles(t *testing.T) {
	dir := t.TempDir()
	sub := filepath.Join(dir, "sub")
	if err := os.MkdirAll(sub, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "root.txt"), []byte("r"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(sub, "leaf.txt"), []byte("l"), 0o644); err != nil {
		t.Fatal(err)
	}

	hashes, err := HashRestoredTree(dir)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(hashes) != 2 {
		t.Fatalf("expected 2 entries, got %d: %v", len(hashes), hashes)
	}
	if _, ok := hashes["root.txt"]; !ok {
		t.Error("missing root.txt in hash map")
	}
	if _, ok := hashes["sub/leaf.txt"]; !ok {
		t.Errorf("missing sub/leaf.txt in hash map, keys: %v", hashes)
	}
}

func TestHashRestoredTreeIsContentDetermistic(t *testing.T) {
	dir1 := t.TempDir()
	dir2 := t.TempDir()
	for _, dir := range []string{dir1, dir2} {
		if err := os.WriteFile(filepath.Join(dir, "f.bin"), []byte("same content"), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	h1, err := HashRestoredTree(dir1)
	if err != nil {
		t.Fatal(err)
	}
	h2, err := HashRestoredTree(dir2)
	if err != nil {
		t.Fatal(err)
	}

	ok, reason := EqualRestoredTreeHashes(h1, h2)
	if !ok {
		t.Errorf("identical content dirs should match: %s", reason)
	}
}

func TestEqualRestoredTreeHashesDetectsCountMismatch(t *testing.T) {
	left := map[string]string{"a.txt": "aaa"}
	right := map[string]string{"a.txt": "aaa", "b.txt": "bbb"}
	ok, reason := EqualRestoredTreeHashes(left, right)
	if ok {
		t.Error("expected mismatch, got equal")
	}
	if reason == "" {
		t.Error("expected non-empty reason")
	}
}

func TestEqualRestoredTreeHashesDetectsHashMismatch(t *testing.T) {
	left := map[string]string{"a.txt": "aaa"}
	right := map[string]string{"a.txt": "bbb"}
	ok, reason := EqualRestoredTreeHashes(left, right)
	if ok {
		t.Error("expected mismatch, got equal")
	}
	if reason == "" {
		t.Error("expected non-empty reason")
	}
}

func TestEqualRestoredTreeHashesDetectsMissingFile(t *testing.T) {
	left := map[string]string{"a.txt": "aaa", "b.txt": "bbb"}
	right := map[string]string{"a.txt": "aaa"}
	ok, reason := EqualRestoredTreeHashes(left, right)
	if ok {
		t.Error("expected mismatch, got equal")
	}
	if reason == "" {
		t.Error("expected non-empty reason")
	}
}

func TestEqualRestoredTreeHashesIdentical(t *testing.T) {
	m := map[string]string{"a.txt": "aaa", "b.txt": "bbb"}
	ok, reason := EqualRestoredTreeHashes(m, m)
	if !ok {
		t.Errorf("expected equal, got: %s", reason)
	}
}
