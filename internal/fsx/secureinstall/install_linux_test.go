//go:build linux

package secureinstall

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/sys/unix"
)

func TestLinuxNoReplacePublishesAtomically(t *testing.T) {
	root := t.TempDir()
	destination := filepath.Join(root, "restored.bin")
	result, err := installLinuxTestContent(destination, root, false, []byte("new-content"))
	if err != nil {
		t.Fatalf("install: %v", err)
	}
	if result.Destination != destination {
		t.Fatalf("destination=%q want=%q", result.Destination, destination)
	}
	requireLinuxFileBytes(t, destination, []byte("new-content"))
}

func TestLinuxNoReplacePreservesExistingDestination(t *testing.T) {
	root := t.TempDir()
	destination := filepath.Join(root, "restored.bin")
	if err := os.WriteFile(destination, []byte("existing"), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := installLinuxTestContent(destination, root, false, []byte("new-content"))
	if !errors.Is(err, ErrDestinationExists) {
		t.Fatalf("install error=%v, want ErrDestinationExists", err)
	}
	requireLinuxFileBytes(t, destination, []byte("existing"))
}

func TestLinuxNoReplaceClosesFinalWindow(t *testing.T) {
	root := t.TempDir()
	destination := filepath.Join(root, "restored.bin")
	withLinuxOps(t, nil, nil, nil, func() error {
		return os.WriteFile(destination, []byte("raced"), 0o600)
	})
	_, err := installLinuxTestContent(destination, root, false, []byte("new-content"))
	if !errors.Is(err, ErrDestinationExists) {
		t.Fatalf("install error=%v, want ErrDestinationExists", err)
	}
	requireLinuxFileBytes(t, destination, []byte("raced"))
}

func TestLinuxNoReplaceUsesAtomicLinkFallback(t *testing.T) {
	root := t.TempDir()
	destination := filepath.Join(root, "restored.bin")
	withLinuxOps(t,
		func(int, string, int, string, uint) error { return unix.ENOSYS },
		nil,
		nil,
		nil,
	)
	if _, err := installLinuxTestContent(destination, root, false, []byte("fallback")); err != nil {
		t.Fatalf("fallback install: %v", err)
	}
	requireLinuxFileBytes(t, destination, []byte("fallback"))
	requireNoLinuxTemporaryNames(t, root)
}

func TestLinuxNoReplaceFailsClosedWhenAtomicPrimitivesUnavailable(t *testing.T) {
	root := t.TempDir()
	destination := filepath.Join(root, "restored.bin")
	withLinuxOps(t,
		func(int, string, int, string, uint) error { return unix.ENOSYS },
		func(int, string, int, string, int) error { return unix.EOPNOTSUPP },
		nil,
		nil,
	)
	_, err := installLinuxTestContent(destination, root, false, []byte("must-not-publish"))
	if !errors.Is(err, ErrAtomicNoReplaceUnsupported) {
		t.Fatalf("install error=%v, want ErrAtomicNoReplaceUnsupported", err)
	}
	if _, statErr := os.Stat(destination); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("destination stat error=%v, want not exists", statErr)
	}
	requireNoLinuxTemporaryNames(t, root)
}

func TestLinuxOverwriteIntentionallyReplacesDestination(t *testing.T) {
	root := t.TempDir()
	destination := filepath.Join(root, "restored.bin")
	if err := os.WriteFile(destination, []byte("old"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := installLinuxTestContent(destination, root, true, []byte("new")); err != nil {
		t.Fatalf("overwrite install: %v", err)
	}
	requireLinuxFileBytes(t, destination, []byte("new"))
}

func TestLinuxParentReplacementBeforeCreateFailsClosed(t *testing.T) {
	testLinuxParentReplacement(t, true)
}

func TestLinuxParentReplacementBeforePublishFailsClosed(t *testing.T) {
	testLinuxParentReplacement(t, false)
}

func testLinuxParentReplacement(t *testing.T, beforeCreate bool) {
	base := t.TempDir()
	root := filepath.Join(base, "trusted")
	if err := os.Mkdir(root, 0o755); err != nil {
		t.Fatal(err)
	}
	relocated := filepath.Join(base, "retained")
	outside := t.TempDir()
	destination := filepath.Join(root, "restored.bin")
	swap := func() error {
		if err := os.Rename(root, relocated); err != nil {
			return err
		}
		return os.Symlink(outside, root)
	}
	if beforeCreate {
		withLinuxOps(t, nil, nil, swap, nil)
	} else {
		withLinuxOps(t, nil, nil, nil, swap)
	}
	_, err := installLinuxTestContent(destination, root, false, []byte("confined"))
	if !errors.Is(err, ErrParentChanged) {
		t.Fatalf("install error=%v, want ErrParentChanged", err)
	}
	if _, statErr := os.Stat(filepath.Join(outside, "restored.bin")); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("outside destination stat error=%v, want not exists", statErr)
	}
	if _, statErr := os.Stat(filepath.Join(relocated, "restored.bin")); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("retained destination stat error=%v, want not exists", statErr)
	}
}

func installLinuxTestContent(destination, root string, overwrite bool, content []byte) (Result, error) {
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

func withLinuxOps(
	t *testing.T,
	rename func(int, string, int, string, uint) error,
	link func(int, string, int, string, int) error,
	beforeCreate func() error,
	beforePublish func() error,
) {
	t.Helper()
	original := linuxOps
	if rename != nil {
		linuxOps.renameNoReplace = rename
	}
	if link != nil {
		linuxOps.link = link
	}
	linuxOps.beforeCreate = beforeCreate
	linuxOps.beforePublish = beforePublish
	t.Cleanup(func() { linuxOps = original })
}

func requireLinuxFileBytes(t *testing.T, path string, want []byte) {
	t.Helper()
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if string(got) != string(want) {
		t.Fatalf("file bytes=%q want=%q", got, want)
	}
}

func requireNoLinuxTemporaryNames(t *testing.T, directory string) {
	t.Helper()
	entries, err := os.ReadDir(directory)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	for _, entry := range entries {
		if len(entry.Name()) >= len(".coldkeep-restore-") && entry.Name()[:len(".coldkeep-restore-")] == ".coldkeep-restore-" {
			t.Fatalf("temporary name remained: %s", entry.Name())
		}
	}
}
