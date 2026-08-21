package storage

import (
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/franchoy/coldkeep/internal/fsx/secureinstall"
)

// The name intentionally extends an existing cross-platform selector so the
// frozen native restore proof runs in the established Linux/macOS/Windows job.
func TestRestoreWithTrustedRootAllowsOuterAliasForExactOutputPathPhase9NativeProof(t *testing.T) {
	t.Run("atomic-no-replace-final-window", func(t *testing.T) {
		root := t.TempDir()
		destination := filepath.Join(root, "restored.bin")
		pending := beginPhase9NativeInstall(t, destination, root, false, []byte("restored"), secureinstall.Metadata{})
		if err := os.WriteFile(destination, []byte("raced"), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := pending.Publish(); !errors.Is(err, secureinstall.ErrDestinationExists) {
			t.Fatalf("Publish error=%v, want ErrDestinationExists", err)
		}
		if got, err := os.ReadFile(destination); err != nil || string(got) != "raced" {
			t.Fatalf("raced destination bytes=%q err=%v", got, err)
		}
		if err := pending.Abort(); err != nil {
			t.Fatalf("Abort after no-replace failure: %v", err)
		}
		requirePhase9NoRestoreTemps(t, root)
	})

	t.Run("intentional-overwrite", func(t *testing.T) {
		root := t.TempDir()
		destination := filepath.Join(root, "restored.bin")
		if err := os.WriteFile(destination, []byte("old"), 0o600); err != nil {
			t.Fatal(err)
		}
		pending := beginPhase9NativeInstall(t, destination, root, true, []byte("new"), secureinstall.Metadata{})
		if _, err := pending.Publish(); err != nil {
			t.Fatalf("overwrite Publish: %v", err)
		}
		if got, err := os.ReadFile(destination); err != nil || string(got) != "new" {
			t.Fatalf("overwrite destination bytes=%q err=%v", got, err)
		}
	})

	t.Run("retained-parent-rejects-path-replacement", func(t *testing.T) {
		base := t.TempDir()
		root := filepath.Join(base, "trusted")
		if err := os.Mkdir(root, 0o755); err != nil {
			t.Fatal(err)
		}
		relocated := filepath.Join(base, "retained")
		outside := t.TempDir()
		destination := filepath.Join(root, "restored.bin")
		pending := beginPhase9NativeInstall(t, destination, root, false, []byte("confined"), secureinstall.Metadata{})
		if err := os.Rename(root, relocated); err != nil {
			if runtime.GOOS != "windows" || !errors.Is(err, os.ErrPermission) {
				t.Fatalf("relocate retained parent: %v", err)
			}
			// Windows may deny directory relocation while the production
			// installer retains its parent handle. That is a fail-closed native
			// outcome for the same frozen replacement threat, not a reason to
			// weaken the proof or require a pathname reopen.
			if _, publishErr := pending.Publish(); publishErr != nil {
				t.Fatalf("publish after denied parent relocation: %v", publishErr)
			}
			if got, readErr := os.ReadFile(destination); readErr != nil || string(got) != "confined" {
				t.Fatalf("retained destination bytes=%q err=%v", got, readErr)
			}
			if _, statErr := os.Stat(filepath.Join(outside, "restored.bin")); !errors.Is(statErr, os.ErrNotExist) {
				t.Fatalf("outside destination stat=%v, want not exists", statErr)
			}
			requirePhase9NoRestoreTemps(t, root)
			return
		}
		if err := os.Symlink(outside, root); err != nil {
			t.Fatalf("install replacement symlink/reparse point: %v", err)
		}
		if _, err := pending.Publish(); !errors.Is(err, secureinstall.ErrParentChanged) {
			t.Fatalf("Publish error=%v, want ErrParentChanged", err)
		}
		if err := pending.Abort(); err != nil {
			t.Fatalf("Abort after parent replacement: %v", err)
		}
		if _, err := os.Stat(filepath.Join(outside, "restored.bin")); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("outside destination stat=%v, want not exists", err)
		}
		if _, err := os.Stat(filepath.Join(relocated, "restored.bin")); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("retained destination stat=%v, want not exists", err)
		}
		requirePhase9NoRestoreTemps(t, relocated)
	})

	t.Run("retained-object-metadata", func(t *testing.T) {
		root := t.TempDir()
		destination := filepath.Join(root, "restored.bin")
		mode := os.FileMode(0o600)
		mtime := time.Date(2020, 6, 15, 10, 30, 0, 0, time.UTC)
		pending := beginPhase9NativeInstall(t, destination, root, false, []byte("metadata"), secureinstall.Metadata{
			Mode: &mode, ModifiedAt: &mtime, Strict: true,
		})
		if _, err := pending.Publish(); err != nil {
			t.Fatalf("metadata Publish: %v", err)
		}
		info, err := os.Stat(destination)
		if err != nil {
			t.Fatal(err)
		}
		if runtime.GOOS != "windows" && info.Mode().Perm() != mode {
			t.Fatalf("mode=%o want=%o", info.Mode().Perm(), mode)
		}
		if !info.ModTime().UTC().Equal(mtime) {
			t.Fatalf("mtime=%v want=%v", info.ModTime().UTC(), mtime)
		}
	})
}

func beginPhase9NativeInstall(t *testing.T, destination, root string, overwrite bool, content []byte, metadata secureinstall.Metadata) *secureinstall.Pending {
	t.Helper()
	pending, err := secureinstall.Begin(secureinstall.Request{
		Destination: destination,
		TrustedRoot: root,
		Overwrite:   overwrite,
		Metadata:    metadata,
	})
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	t.Cleanup(func() { _ = pending.Abort() })
	if _, err := pending.Writer().Write(content); err != nil {
		t.Fatalf("write temporary content: %v", err)
	}
	if err := pending.SyncAndCloseWriter(); err != nil {
		t.Fatalf("SyncAndCloseWriter: %v", err)
	}
	return pending
}

func requirePhase9NoRestoreTemps(t *testing.T, directory string) {
	t.Helper()
	entries, err := os.ReadDir(directory)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), ".coldkeep-restore-") {
			t.Fatalf("temporary restore name remained: %s", entry.Name())
		}
	}
}
