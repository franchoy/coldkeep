package storage

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRestoreExactDestinationDoesNotReinterpretExistingDirectory(t *testing.T) {
	repo, stored, _ := storeRestoreFaultFSTestFile(t)
	exactDestination := t.TempDir()
	reinterpretedChild := filepath.Join(exactDestination, filepath.Base(stored.Path))

	result, err := RestoreFileWithStorageContextResultOptions(
		repo.Storage,
		stored.FileID,
		exactDestination,
		RestoreOptions{Overwrite: true},
	)
	if err == nil {
		t.Fatalf("restore unexpectedly reinterpreted exact directory destination: %+v", result)
	}
	if !strings.Contains(err.Error(), "exact destination") {
		t.Fatalf("restore error=%v, want exact-destination classification", err)
	}
	if _, statErr := os.Stat(reinterpretedChild); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("reinterpreted child stat error=%v, want not exists", statErr)
	}
}

func TestRestoreExactDestinationRejectsTrailingSeparatorWithoutCreatingIt(t *testing.T) {
	repo, stored, _ := storeRestoreFaultFSTestFile(t)
	exactDestination := filepath.Join(t.TempDir(), "must-be-a-file") + string(os.PathSeparator)

	_, err := RestoreFileWithStorageContextResultOptions(
		repo.Storage,
		stored.FileID,
		exactDestination,
		RestoreOptions{Overwrite: true},
	)
	if err == nil || !strings.Contains(err.Error(), "exact destination") {
		t.Fatalf("restore error=%v, want exact-destination rejection", err)
	}
	if _, statErr := os.Stat(filepath.Clean(exactDestination)); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("trailing-separator destination stat error=%v, want not exists", statErr)
	}
}
