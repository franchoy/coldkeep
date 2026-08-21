//go:build darwin

package secureinstall

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/sys/unix"
)

func TestDarwinNoReplaceAndOverwrite(t *testing.T) {
	root := t.TempDir()
	destination := filepath.Join(root, "restored.bin")
	if _, err := installDarwinTestContent(destination, root, false, []byte("first")); err != nil {
		t.Fatalf("no-replace install: %v", err)
	}
	if _, err := installDarwinTestContent(destination, root, false, []byte("second")); !errors.Is(err, ErrDestinationExists) {
		t.Fatalf("existing destination error=%v", err)
	}
	if _, err := installDarwinTestContent(destination, root, true, []byte("replacement")); err != nil {
		t.Fatalf("overwrite install: %v", err)
	}
	got, err := os.ReadFile(destination)
	if err != nil || string(got) != "replacement" {
		t.Fatalf("destination bytes=%q err=%v", got, err)
	}
}

func TestDarwinNoReplaceUsesLinkFallbackAndFailsClosed(t *testing.T) {
	original := darwinOps
	t.Cleanup(func() { darwinOps = original })
	darwinOps.renameNoReplace = func(int, string, int, string, uint32) error { return unix.ENOTSUP }

	root := t.TempDir()
	destination := filepath.Join(root, "fallback.bin")
	if _, err := installDarwinTestContent(destination, root, false, []byte("fallback")); err != nil {
		t.Fatalf("link fallback: %v", err)
	}

	darwinOps.link = func(int, string, int, string, int) error { return unix.ENOTSUP }
	_, err := installDarwinTestContent(filepath.Join(root, "unsupported.bin"), root, false, []byte("no-publish"))
	if !errors.Is(err, ErrAtomicNoReplaceUnsupported) {
		t.Fatalf("unsupported error=%v", err)
	}
}

func installDarwinTestContent(destination, root string, overwrite bool, content []byte) (Result, error) {
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
