//go:build linux

package secureinstall

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

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
	withLinuxOps(t, linuxOpsOverride{beforePublish: func() error {
		return os.WriteFile(destination, []byte("raced"), 0o600)
	}})
	_, err := installLinuxTestContent(destination, root, false, []byte("new-content"))
	if !errors.Is(err, ErrDestinationExists) {
		t.Fatalf("install error=%v, want ErrDestinationExists", err)
	}
	requireLinuxFileBytes(t, destination, []byte("raced"))
}

func TestLinuxNoReplaceUsesAtomicLinkFallback(t *testing.T) {
	root := t.TempDir()
	destination := filepath.Join(root, "restored.bin")
	withLinuxOps(t, linuxOpsOverride{
		renameNoReplace: func(int, string, int, string, uint) error { return unix.ENOSYS },
	})
	if _, err := installLinuxTestContent(destination, root, false, []byte("fallback")); err != nil {
		t.Fatalf("fallback install: %v", err)
	}
	requireLinuxFileBytes(t, destination, []byte("fallback"))
	requireNoLinuxTemporaryNames(t, root)
}

func TestLinuxNoReplaceFailsClosedWhenAtomicPrimitivesUnavailable(t *testing.T) {
	root := t.TempDir()
	destination := filepath.Join(root, "restored.bin")
	withLinuxOps(t, linuxOpsOverride{
		renameNoReplace: func(int, string, int, string, uint) error { return unix.ENOSYS },
		link:            func(int, string, int, string, int) error { return unix.EOPNOTSUPP },
	})
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

func TestLinuxParentSyncFailureIsTruthfulAfterPublication(t *testing.T) {
	root := t.TempDir()
	destination := filepath.Join(root, "restored.bin")
	syncErr := errors.New("parent sync failure")
	original := linuxOps
	linuxOps.syncParent = func(int) error { return syncErr }
	t.Cleanup(func() { linuxOps = original })
	_, err := installLinuxTestContent(destination, root, false, []byte("published"))
	if !errors.Is(err, syncErr) {
		t.Fatalf("install error=%v, want parent sync failure", err)
	}
	requireLinuxFileBytes(t, destination, []byte("published"))
}

func TestLinuxStrictMetadataFailureLeavesPublishedBytesVisible(t *testing.T) {
	root := t.TempDir()
	destination := filepath.Join(root, "restored.bin")
	metadataErr := errors.New("metadata failure")
	original := linuxOps
	linuxOps.chmod = func(int, uint32) error { return metadataErr }
	t.Cleanup(func() { linuxOps = original })
	mode := os.FileMode(0o600)
	pending, err := Begin(Request{
		Destination: destination,
		TrustedRoot: root,
		Metadata:    Metadata{Mode: &mode, Strict: true},
	})
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	defer pending.Abort()
	if err := writeAll(pending, []byte("published")); err != nil {
		t.Fatal(err)
	}
	if err := pending.SyncAndCloseWriter(); err != nil {
		t.Fatal(err)
	}
	_, err = pending.Publish()
	if !errors.Is(err, metadataErr) {
		t.Fatalf("Publish error=%v, want metadata failure", err)
	}
	requireLinuxFileBytes(t, destination, []byte("published"))
}

func TestLinuxBestEffortMetadataFailureReturnsWarning(t *testing.T) {
	root := t.TempDir()
	destination := filepath.Join(root, "restored.bin")
	metadataErr := errors.New("metadata failure")
	original := linuxOps
	linuxOps.chmod = func(int, uint32) error { return metadataErr }
	t.Cleanup(func() { linuxOps = original })
	mode := os.FileMode(0o600)
	pending, err := Begin(Request{
		Destination: destination,
		TrustedRoot: root,
		Metadata:    Metadata{Mode: &mode},
	})
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	defer pending.Abort()
	if err := writeAll(pending, []byte("published")); err != nil {
		t.Fatal(err)
	}
	if err := pending.SyncAndCloseWriter(); err != nil {
		t.Fatal(err)
	}
	result, err := pending.Publish()
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if len(result.Warnings) != 1 || result.Warnings[0].Operation != "chmod" || result.Warnings[0].Detail != metadataErr.Error() {
		t.Fatalf("metadata warnings=%+v", result.Warnings)
	}
}

func TestLinuxMetadataTargetsRetainedPublishedObject(t *testing.T) {
	fixture, pending := beginLinuxRetainedMetadataProof(t)
	if _, err := pending.Publish(); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	requireLinuxRetainedMetadata(t, fixture)
}

type linuxRetainedMetadataFixture struct {
	destination string
	moved       string
	mode        os.FileMode
	mtime       time.Time
}

func beginLinuxRetainedMetadataProof(t *testing.T) (linuxRetainedMetadataFixture, *Pending) {
	t.Helper()
	root := t.TempDir()
	destination := filepath.Join(root, "restored.bin")
	moved := filepath.Join(root, "retained-object.bin")
	original := linuxOps
	linuxOps.beforeMetadata = func() error {
		if err := os.Rename(destination, moved); err != nil {
			return err
		}
		return os.WriteFile(destination, []byte("path replacement"), 0o666)
	}
	t.Cleanup(func() { linuxOps = original })
	mode := os.FileMode(0o600)
	mtime := time.Date(2020, 6, 15, 10, 30, 0, 0, time.UTC)
	pending, err := Begin(Request{
		Destination: destination,
		TrustedRoot: root,
		Metadata:    Metadata{Mode: &mode, ModifiedAt: &mtime, Strict: true},
	})
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	t.Cleanup(func() { _ = pending.Abort() })
	if err := writeAll(pending, []byte("retained")); err != nil {
		t.Fatal(err)
	}
	if err := pending.SyncAndCloseWriter(); err != nil {
		t.Fatal(err)
	}
	return linuxRetainedMetadataFixture{destination: destination, moved: moved, mode: mode, mtime: mtime}, pending
}

func requireLinuxRetainedMetadata(t *testing.T, fixture linuxRetainedMetadataFixture) {
	t.Helper()
	movedInfo, err := os.Stat(fixture.moved)
	if err != nil {
		t.Fatal(err)
	}
	if movedInfo.Mode().Perm() != fixture.mode || !movedInfo.ModTime().UTC().Equal(fixture.mtime) {
		t.Fatalf("retained object metadata mode=%o mtime=%v", movedInfo.Mode().Perm(), movedInfo.ModTime())
	}
	replacementInfo, err := os.Stat(fixture.destination)
	if err != nil {
		t.Fatal(err)
	}
	if replacementInfo.Mode().Perm() == fixture.mode {
		t.Fatalf("pathname replacement unexpectedly received retained-object mode %o", fixture.mode)
	}
}

func TestLinuxParentReplacementBeforeCreateFailsClosed(t *testing.T) {
	testLinuxParentReplacement(t, true)
}

func TestLinuxParentReplacementBeforePublishFailsClosed(t *testing.T) {
	testLinuxParentReplacement(t, false)
}

func TestLinuxTrustedRootSymlinkFailsClosed(t *testing.T) {
	realRoot := t.TempDir()
	alias := filepath.Join(t.TempDir(), "trusted-root-link")
	if err := os.Symlink(realRoot, alias); err != nil {
		t.Skipf("symlink unavailable: %v", err)
	}
	_, err := installLinuxTestContent(filepath.Join(alias, "restored.bin"), alias, false, []byte("must-not-write"))
	if err == nil {
		t.Fatal("secure installer unexpectedly accepted a symlink trusted root")
	}
	if _, statErr := os.Stat(filepath.Join(realRoot, "restored.bin")); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("symlink target stat error=%v, want not exists", statErr)
	}
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
		withLinuxOps(t, linuxOpsOverride{beforeCreate: swap})
	} else {
		withLinuxOps(t, linuxOpsOverride{beforePublish: swap})
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
	requireNoLinuxTemporaryNames(t, relocated)
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

type linuxOpsOverride struct {
	renameNoReplace func(int, string, int, string, uint) error
	link            func(int, string, int, string, int) error
	beforeCreate    func() error
	beforePublish   func() error
}

func withLinuxOps(t *testing.T, override linuxOpsOverride) {
	t.Helper()
	original := linuxOps
	if override.renameNoReplace != nil {
		linuxOps.renameNoReplace = override.renameNoReplace
	}
	if override.link != nil {
		linuxOps.link = override.link
	}
	linuxOps.beforeCreate = override.beforeCreate
	linuxOps.beforePublish = override.beforePublish
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
