package batch

import (
	"os"
	"path/filepath"
	"slices"
	"testing"
)

func TestLoadRawTargets(t *testing.T) {
	tmp := t.TempDir()
	inputFile := filepath.Join(tmp, "ids.txt")
	content := "# first comment\n22\n\ninvalid-id\n 18 \n# trailing comment\n"
	if err := os.WriteFile(inputFile, []byte(content), 0o644); err != nil {
		t.Fatalf("write input file: %v", err)
	}

	raw, err := LoadRawTargets([]string{"12"}, inputFile)
	if err != nil {
		t.Fatalf("load raw targets: %v", err)
	}

	got := make([]string, 0, len(raw))
	for _, item := range raw {
		got = append(got, item.Value)
	}
	want := []string{"12", "22", "invalid-id", "18"}
	if !slices.Equal(got, want) {
		t.Fatalf("raw target values mismatch: want=%v got=%v", want, got)
	}
}

func TestLoadRawTargetsMissingFile(t *testing.T) {
	_, err := LoadRawTargets(nil, filepath.Join(t.TempDir(), "missing.txt"))
	if err == nil {
		t.Fatalf("expected missing-file error")
	}
}
