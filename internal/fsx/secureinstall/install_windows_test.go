//go:build windows

package secureinstall

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestWindowsNoReplaceAndOverwrite(t *testing.T) {
	root := t.TempDir()
	destination := filepath.Join(root, "restored.bin")
	if _, err := installWindowsTestContent(destination, root, false, []byte("first")); err != nil {
		t.Fatalf("no-replace install: %v", err)
	}
	if _, err := installWindowsTestContent(destination, root, false, []byte("second")); !errors.Is(err, ErrDestinationExists) {
		t.Fatalf("existing destination error=%v", err)
	}
	if _, err := installWindowsTestContent(destination, root, true, []byte("replacement")); err != nil {
		t.Fatalf("overwrite install: %v", err)
	}
	got, err := os.ReadFile(destination)
	if err != nil || string(got) != "replacement" {
		t.Fatalf("destination bytes=%q err=%v", got, err)
	}
}

func TestWindowsPublicationUsesRetainedTemporaryObject(t *testing.T) {
	root := t.TempDir()
	destination := filepath.Join(root, "restored.bin")
	original := windowsOps
	t.Cleanup(func() { windowsOps = original })
	windowsOps.beforePublish = func() error {
		entries, err := os.ReadDir(root)
		if err != nil {
			return err
		}
		for _, entry := range entries {
			if !strings.HasPrefix(entry.Name(), ".coldkeep-restore-") {
				continue
			}
			originalName := filepath.Join(root, entry.Name())
			if err := os.Rename(originalName, filepath.Join(root, "retained-object-moved.bin")); err != nil {
				return err
			}
			return os.WriteFile(originalName, []byte("path-replacement"), 0o600)
		}
		return errors.New("temporary object name not found")
	}
	if _, err := installWindowsTestContent(destination, root, false, []byte("retained-object")); err != nil {
		t.Fatalf("retained-object publication: %v", err)
	}
	got, err := os.ReadFile(destination)
	if err != nil || string(got) != "retained-object" {
		t.Fatalf("destination bytes=%q err=%v", got, err)
	}
}

func installWindowsTestContent(destination, root string, overwrite bool, content []byte) (Result, error) {
	pending, err := Begin(Request{Destination: destination, TrustedRoot: root, Overwrite: overwrite})
	if err != nil {
		return Result{}, err
	}
	defer pending.Abort()
	if err := writeAll(pending, content); err != nil {
		return Result{}, err
	}
	if err := pending.SyncAndCloseWriter(); err != nil {
		return Result{}, err
	}
	return pending.Publish()
}
