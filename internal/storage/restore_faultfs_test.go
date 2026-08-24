package storage

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/franchoy/coldkeep/internal/blocks"
	"github.com/franchoy/coldkeep/internal/fsx"
	"github.com/franchoy/coldkeep/internal/fsx/faultfs"
	"github.com/franchoy/coldkeep/internal/fsx/secureinstall"
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

func TestRestoreFaultFSStatFailureFailsClosed(t *testing.T) {
	repo, storeResult, _ := storeRestoreFaultFSTestFile(t)
	outPath := filepath.Join(t.TempDir(), "restored.txt")
	original := []byte("keep-me-intact")
	mustNoErr(t, os.WriteFile(filepath.Clean(outPath), original, 0o600), "write existing destination")
	script := faultfs.NewScript(faultfs.Fault{Op: faultfs.OpStat, After: 2, Err: faultfs.ErrFaultStat})
	_, err := RestoreFileWithStorageContextResultOptions(repo.Storage, storeResult.FileID, outPath, RestoreOptions{
		Overwrite: false,
		fs:        faultfs.New(fsx.Default(), script),
	})
	if !errors.Is(err, faultfs.ErrFaultStat) {
		t.Fatalf("restore error=%v, want ErrFaultStat", err)
	}
	got, readErr := os.ReadFile(outPath)
	mustNoErr(t, readErr, "read existing destination")
	if !bytes.Equal(got, original) {
		t.Fatalf("existing destination changed: got %q want %q", got, original)
	}
}

func TestRestoreSecureInstallerSyncFailureFailsBeforePublication(t *testing.T) {
	repo, stored, _ := storeRestoreFaultFSTestFile(t)
	destination := filepath.Join(t.TempDir(), "restored.txt")
	syncErr := errors.New("secure installer sync failure")
	fake := &fakeRestoreInstallation{syncErr: syncErr}
	_, err := restoreWithFakeInstallation(repo, stored.FileID, destination, fake)
	if !errors.Is(err, syncErr) {
		t.Fatalf("restore error=%v, want sync failure", err)
	}
	if fake.publishCalls != 0 || fake.abortCalls != 1 {
		t.Fatalf("lifecycle publish=%d abort=%d", fake.publishCalls, fake.abortCalls)
	}
	if _, statErr := os.Stat(destination); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("destination stat error=%v, want not exists", statErr)
	}
}

func TestRestoreSecureInstallerPublicationFailureFailsClosed(t *testing.T) {
	repo, stored, _ := storeRestoreFaultFSTestFile(t)
	destination := filepath.Join(t.TempDir(), "restored.txt")
	publishErr := errors.New("secure installer publication failure")
	fake := &fakeRestoreInstallation{publishErr: publishErr}
	result, err := restoreWithFakeInstallation(repo, stored.FileID, destination, fake)
	if !errors.Is(err, publishErr) {
		t.Fatalf("restore error=%v, want publication failure", err)
	}
	if result != (RestoreFileResult{}) {
		t.Fatalf("restore result=%+v, want zero", result)
	}
	if fake.publishCalls != 1 || fake.abortCalls != 1 {
		t.Fatalf("lifecycle publish=%d abort=%d", fake.publishCalls, fake.abortCalls)
	}
}

func TestRestoreSecureInstallerAbortFailureIsReturned(t *testing.T) {
	repo, stored, _ := storeRestoreFaultFSTestFile(t)
	destination := filepath.Join(t.TempDir(), "restored.txt")
	publishErr := errors.New("publication failure")
	abortErr := errors.New("abort failure")
	fake := &fakeRestoreInstallation{publishErr: publishErr, abortErr: abortErr}
	_, err := restoreWithFakeInstallation(repo, stored.FileID, destination, fake)
	if !errors.Is(err, publishErr) || !errors.Is(err, abortErr) {
		t.Fatalf("restore error=%v, want publication and abort failures", err)
	}
}

func TestRestoreCancellationUsesBoundedCleanupAndJoinsAbortFailure(t *testing.T) {
	dbconn, sgctx, fileID, chunkIDs, _ := setupRestorePinningFixture(t, [][]byte{[]byte("cancel-cleanup")})
	defer func() { _ = dbconn.Close() }()

	ctx, cancel := context.WithCancel(context.Background())
	abortErr := errors.New("forced cancellation abort failure")
	fake := &fakeRestoreInstallation{abortErr: abortErr}
	ConfigureRestoreTestHooksForTesting(&sgctx, func(_ *sql.DB, _ int64) error {
		cancel()
		return ctx.Err()
	}, nil)

	result, err := RestoreFileWithStorageContextResultOptionsContext(ctx, sgctx, fileID, filepath.Join(t.TempDir(), "cancelled.bin"), RestoreOptions{
		Overwrite: true,
		installFactory: func(secureinstall.Request) (restoreInstallation, error) {
			return fake, nil
		},
	})
	if result != (RestoreFileResult{}) {
		t.Fatalf("restore result=%+v, want zero", result)
	}
	if !errors.Is(err, context.Canceled) || !errors.Is(err, abortErr) {
		t.Fatalf("restore error=%v, want joined cancellation and abort failure", err)
	}
	if fake.abortCalls != 1 {
		t.Fatalf("Abort calls=%d, want 1", fake.abortCalls)
	}
	for _, chunkID := range chunkIDs {
		if got := readChunkPinCountForRestoreTest(t, dbconn, chunkID); got != 0 {
			t.Fatalf("chunk %d pin_count=%d after cancellation cleanup", chunkID, got)
		}
	}
}

func TestRestoreSecureInstallerMetadataWarningsArePreserved(t *testing.T) {
	repo, stored, _ := storeRestoreFaultFSTestFile(t)
	destination := filepath.Join(t.TempDir(), "restored.txt")
	fake := &fakeRestoreInstallation{publishResult: secureinstall.Result{
		Destination: destination,
		Warnings: []secureinstall.Warning{{
			Operation: "chmod",
			Detail:    "permission denied",
		}},
	}}
	result, err := restoreWithFakeInstallation(repo, stored.FileID, destination, fake)
	if err != nil {
		t.Fatalf("restore: %v", err)
	}
	if result.MetadataWarnings == nil || len(result.MetadataWarnings.Items) != 1 {
		t.Fatalf("metadata warnings=%+v", result.MetadataWarnings)
	}
	warning := result.MetadataWarnings.Items[0]
	if warning.Operation != "chmod" || warning.Detail != "permission denied" {
		t.Fatalf("metadata warning=%+v", warning)
	}
}

type fakeRestoreInstallation struct {
	buffer        bytes.Buffer
	syncErr       error
	publishErr    error
	publishResult secureinstall.Result
	abortErr      error
	publishCalls  int
	abortCalls    int
}

func (f *fakeRestoreInstallation) Writer() io.Writer { return &f.buffer }
func (f *fakeRestoreInstallation) SyncAndCloseWriter() error {
	return f.syncErr
}
func (f *fakeRestoreInstallation) Publish() (secureinstall.Result, error) {
	f.publishCalls++
	return f.publishResult, f.publishErr
}
func (f *fakeRestoreInstallation) Abort() error {
	f.abortCalls++
	return f.abortErr
}

func restoreWithFakeInstallation(repo *TestRepository, fileID int64, destination string, fake *fakeRestoreInstallation) (RestoreFileResult, error) {
	return RestoreFileWithStorageContextResultOptions(repo.Storage, fileID, destination, RestoreOptions{
		Overwrite: true,
		installFactory: func(secureinstall.Request) (restoreInstallation, error) {
			return fake, nil
		},
	})
}
