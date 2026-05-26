package storage

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/franchoy/coldkeep/internal/blocks"
	"github.com/franchoy/coldkeep/internal/fsx"
	"github.com/franchoy/coldkeep/internal/fsx/faultfs"
)

func storeRestoreFaultFSTestFile(t *testing.T) (*TestRepository, StoreFileResult, []byte) {
	t.Helper()

	repo := NewTestRepository(t)
	content := []byte("coldkeep-phase2-restore-faultfs-payload")
	srcFile := filepath.Join(t.TempDir(), "source.txt")
	mustNoErr(t, os.WriteFile(filepath.Clean(srcFile), content, 0o600), "write source file")

	storeResult, err := StoreFileWithStorageContextAndCodecResult(repo.Storage, srcFile, blocks.CodecPlain)
	mustNoErr(t, err, "store file")

	return repo, storeResult, content
}

func restoreWithFaultFS(t *testing.T, repo *TestRepository, fileID int64, outPath string, overwrite bool, script *faultfs.Script) (RestoreFileResult, error) {
	t.Helper()

	return RestoreFileWithStorageContextResultOptions(
		repo.Storage,
		fileID,
		outPath,
		RestoreOptions{Overwrite: overwrite, fs: faultfs.New(fsx.Default(), script)},
	)
}

func TestRestoreFaultFSMkdirFailureFailsClosed(t *testing.T) {
	repo, storeResult, _ := storeRestoreFaultFSTestFile(t)
	outPath := filepath.Join(t.TempDir(), "missing", "nested", "restored.txt")
	script := faultfs.NewScript(faultfs.Fault{Op: faultfs.OpMkdirAll, Err: faultfs.ErrFaultMkdir})

	_, err := restoreWithFaultFS(t, repo, storeResult.FileID, outPath, true, script)
	if !errors.Is(err, faultfs.ErrFaultMkdir) {
		t.Fatalf("restore error = %v, want ErrFaultMkdir", err)
	}
	if _, statErr := os.Stat(outPath); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("restored destination stat error = %v, want not exists", statErr)
	}
	if got := script.CallCount(faultfs.OpMkdirAll); got != 1 {
		t.Fatalf("mkdir call count = %d, want 1", got)
	}
}

func TestRestoreFaultFSStatFailureFailsClosed(t *testing.T) {
	repo, storeResult, _ := storeRestoreFaultFSTestFile(t)
	outDir := t.TempDir()
	outPath := filepath.Join(outDir, "restored.txt")
	original := []byte("keep-me-intact")
	mustNoErr(t, os.WriteFile(filepath.Clean(outPath), original, 0o600), "write existing destination")

	script := faultfs.NewScript(faultfs.Fault{Op: faultfs.OpStat, After: 2, Err: faultfs.ErrFaultStat})
	_, err := restoreWithFaultFS(t, repo, storeResult.FileID, outPath, false, script)
	if !errors.Is(err, faultfs.ErrFaultStat) {
		t.Fatalf("restore error = %v, want ErrFaultStat", err)
	}
	got, readErr := os.ReadFile(outPath)
	mustNoErr(t, readErr, "read existing destination")
	if !bytes.Equal(got, original) {
		t.Fatalf("existing destination changed: got %q want %q", got, original)
	}
	if got := script.CallCount(faultfs.OpStat); got != 2 {
		t.Fatalf("stat call count = %d, want 2", got)
	}
}

func TestRestoreFaultFSRenameFailureDoesNotReportSuccess(t *testing.T) {
	repo, storeResult, _ := storeRestoreFaultFSTestFile(t)
	outPath := filepath.Join(t.TempDir(), "restored.txt")
	script := faultfs.NewScript(faultfs.Fault{Op: faultfs.OpRename, Err: faultfs.ErrFaultRename})

	result, err := restoreWithFaultFS(t, repo, storeResult.FileID, outPath, true, script)
	if !errors.Is(err, faultfs.ErrFaultRename) {
		t.Fatalf("restore error = %v, want ErrFaultRename", err)
	}
	if result != (RestoreFileResult{}) {
		t.Fatalf("restore result = %+v, want zero value on failure", result)
	}
	if _, statErr := os.Stat(outPath); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("restored destination stat error = %v, want not exists", statErr)
	}
	if got := script.CallCount(faultfs.OpRename); got != 1 {
		t.Fatalf("rename call count = %d, want 1", got)
	}
	if got := script.CallCount(faultfs.OpRemove); got != 1 {
		t.Fatalf("cleanup remove call count = %d, want 1", got)
	}
}

func TestRestoreFaultFSRemoveFailureDuringCleanupIsReportedOrPreserved(t *testing.T) {
	repo, storeResult, _ := storeRestoreFaultFSTestFile(t)
	outDir := t.TempDir()
	outPath := filepath.Join(outDir, "restored.txt")
	cleanupErr := errors.New("phase2 cleanup trigger")

	prevHook := TestRestoreFailBeforeRenameHook
	TestRestoreFailBeforeRenameHook = func(tempOutputPath, outputPath string) error {
		return cleanupErr
	}
	t.Cleanup(func() {
		TestRestoreFailBeforeRenameHook = prevHook
	})

	script := faultfs.NewScript(faultfs.Fault{Op: faultfs.OpRemove, Err: faultfs.ErrFaultRemove})
	result, err := restoreWithFaultFS(t, repo, storeResult.FileID, outPath, true, script)
	if !errors.Is(err, cleanupErr) {
		t.Fatalf("restore error = %v, want cleanup trigger", err)
	}
	if result != (RestoreFileResult{}) {
		t.Fatalf("restore result = %+v, want zero value on failure", result)
	}
	if _, statErr := os.Stat(outPath); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("restored destination stat error = %v, want not exists", statErr)
	}
	if got := script.CallCount(faultfs.OpRemove); got != 1 {
		t.Fatalf("cleanup remove call count = %d, want 1", got)
	}
	entries, readErr := os.ReadDir(outDir)
	mustNoErr(t, readErr, "read output directory")
	if len(entries) == 0 {
		t.Fatalf("expected temp file to remain after cleanup remove fault")
	}
}

func TestRestoreFaultFSSyncFailureDoesNotReportSuccess(t *testing.T) {
	repo, storeResult, content := storeRestoreFaultFSTestFile(t)
	outPath := filepath.Join(t.TempDir(), "restored.txt")
	script := faultfs.NewScript(faultfs.Fault{Op: faultfs.OpSync, Err: faultfs.ErrFaultSync})

	result, err := restoreWithFaultFS(t, repo, storeResult.FileID, outPath, true, script)
	if !errors.Is(err, faultfs.ErrFaultSync) {
		t.Fatalf("restore error = %v, want ErrFaultSync", err)
	}
	if result != (RestoreFileResult{}) {
		t.Fatalf("restore result = %+v, want zero value on failure", result)
	}
	got, readErr := os.ReadFile(outPath)
	mustNoErr(t, readErr, "read restored destination after sync failure")
	if !bytes.Equal(got, content) {
		t.Fatalf("restored destination corrupted: got %q want %q", got, content)
	}
	if got := script.CallCount(faultfs.OpSync); got != 1 {
		t.Fatalf("sync call count = %d, want 1", got)
	}
}