package storage

import (
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/franchoy/coldkeep/internal/blocks"
	"github.com/franchoy/coldkeep/internal/pathsafe"
)

func makeOuterAliasRoot(t *testing.T, rootName string) string {
	t.Helper()

	realParent := t.TempDir()
	aliasLink := filepath.Join(t.TempDir(), "outer-link")
	requireSymlink(t, realParent, aliasLink)

	realRoot, err := os.MkdirTemp(realParent, rootName+"-")
	if err != nil {
		t.Fatalf("mkdir real root: %v", err)
	}
	return filepath.Join(aliasLink, filepath.Base(realRoot))
}

func requireOutputBytes(t *testing.T, path string, want []byte) {
	t.Helper()

	got, err := readTrustedOutputBytes(path)
	if err != nil {
		t.Fatalf("read output file %q: %v", path, err)
	}
	if string(got) != string(want) {
		t.Fatalf("output bytes mismatch: got=%q want=%q", string(got), string(want))
	}
}

func readTrustedOutputBytes(path string) ([]byte, error) {
	root, err := pathsafe.NearestExistingAncestorDir(path)
	if err != nil {
		return nil, err
	}
	rel, err := filepath.Rel(root, filepath.Clean(path))
	if err != nil {
		return nil, err
	}
	return fs.ReadFile(os.DirFS(root), filepath.ToSlash(rel))
}

func TestRestoreWithTrustedRootAllowsOuterAliasForExactOutputPath(t *testing.T) {
	t.Parallel()

	repo := NewTestRepository(t)
	payload := []byte("restore-trusted-root-exact-output")
	srcFile := filepath.Join(t.TempDir(), "source.bin")
	mustNoErr(t, os.WriteFile(srcFile, payload, 0o600), "write source file")

	storeResult, err := StoreFileWithStorageContextAndCodecResult(repo.Storage, srcFile, blocks.CodecPlain)
	mustNoErr(t, err, "store file")

	aliasRoot := makeOuterAliasRoot(t, "trusted-root")
	outputPath := filepath.Join(aliasRoot, "restored.bin")

	result, err := RestoreFileWithStorageContextResultOptions(
		repo.Storage,
		storeResult.FileID,
		outputPath,
		RestoreOptions{Overwrite: true, TrustedRoot: aliasRoot},
	)
	mustNoErr(t, err, "restore file with trusted root")

	if result.OutputPath != outputPath {
		t.Fatalf("output path mismatch: got=%q want=%q", result.OutputPath, outputPath)
	}
	requireOutputBytes(t, outputPath, payload)
}

func TestRestoreStoredPathPrefixAllowsOuterAliasAboveTrustedRoot(t *testing.T) {
	t.Parallel()

	repo := NewTestRepository(t)
	payload := []byte("restore-stored-path-prefix-outer-alias")
	srcFile := filepath.Join(t.TempDir(), "source.bin")
	mustNoErr(t, os.WriteFile(srcFile, payload, 0o600), "write source file")

	storeResult, err := StoreFileWithStorageContextAndCodecResult(repo.Storage, srcFile, blocks.CodecPlain)
	mustNoErr(t, err, "store file")

	aliasRoot := makeOuterAliasRoot(t, "prefix-root")
	result, err := RestoreFileByStoredPathWithStorageContextResultOptions(
		repo.Storage,
		storeResult.Path,
		RestoreOptions{
			Overwrite:       true,
			DestinationMode: RestoreDestinationPrefix,
			Destination:     aliasRoot,
			TrustedRoot:     aliasRoot,
		},
	)
	mustNoErr(t, err, "restore stored path prefix through outer alias")

	expectedPath := filepath.Join(aliasRoot, strings.TrimLeft(storeResult.Path, `/\`))
	if result.OutputPath != expectedPath {
		t.Fatalf("output path mismatch: got=%q want=%q", result.OutputPath, expectedPath)
	}
	requireOutputBytes(t, result.OutputPath, payload)
}

func TestRestoreStoredPathOverrideAllowsOuterAliasAboveDerivedRoot(t *testing.T) {
	t.Parallel()

	repo := NewTestRepository(t)
	payload := []byte("restore-stored-path-override-outer-alias")
	srcFile := filepath.Join(t.TempDir(), "source.bin")
	mustNoErr(t, os.WriteFile(srcFile, payload, 0o600), "write source file")

	storeResult, err := StoreFileWithStorageContextAndCodecResult(repo.Storage, srcFile, blocks.CodecPlain)
	mustNoErr(t, err, "store file")

	aliasRoot := makeOuterAliasRoot(t, "override-root")
	outputPath := filepath.Join(aliasRoot, "restored.bin")

	result, err := RestoreFileByStoredPathWithStorageContextResultOptions(
		repo.Storage,
		storeResult.Path,
		RestoreOptions{
			Overwrite:       true,
			DestinationMode: RestoreDestinationOverride,
			Destination:     outputPath,
		},
	)
	mustNoErr(t, err, "restore stored path override through outer alias")

	if result.OutputPath != filepath.Clean(outputPath) {
		t.Fatalf("output path mismatch: got=%q want=%q", result.OutputPath, filepath.Clean(outputPath))
	}
	requireOutputBytes(t, result.OutputPath, payload)
}

func TestRestoreNativeDarwinTempPathWithoutEvalSymlinks(t *testing.T) {
	if runtime.GOOS != "darwin" {
		t.Skip("darwin-only temp-path alias regression test")
	}

	repo := NewTestRepository(t)
	payload := []byte("restore-native-darwin-temp-path")
	srcFile := filepath.Join(t.TempDir(), "source.bin")
	mustNoErr(t, os.WriteFile(srcFile, payload, 0o600), "write source file")

	storeResult, err := StoreFileWithStorageContextAndCodecResult(repo.Storage, srcFile, blocks.CodecPlain)
	mustNoErr(t, err, "store file")

	rawTempRoot := t.TempDir()
	if !strings.Contains(rawTempRoot, "/var/") {
		t.Skipf("raw temp path does not demonstrate outer alias: %q", rawTempRoot)
	}

	outputPath := filepath.Join(rawTempRoot, "restored.bin")
	result, err := RestoreFileWithStorageContextResultOptions(
		repo.Storage,
		storeResult.FileID,
		outputPath,
		RestoreOptions{Overwrite: true, TrustedRoot: rawTempRoot},
	)
	mustNoErr(t, err, "restore file through native darwin temp path")
	requireOutputBytes(t, result.OutputPath, payload)
}
