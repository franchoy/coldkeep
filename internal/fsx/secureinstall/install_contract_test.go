package secureinstall

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestPendingLifecycleRequiresSyncBeforePublish(t *testing.T) {
	root := t.TempDir()
	pending, err := Begin(Request{
		Destination: filepath.Join(root, "restored.bin"),
		TrustedRoot: root,
	})
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	t.Cleanup(func() { _ = pending.Abort() })

	if _, err := pending.Publish(); !errors.Is(err, ErrInvalidState) {
		t.Fatalf("Publish before sync error=%v, want ErrInvalidState", err)
	}
	if err := writeAll(pending, []byte("content")); err != nil {
		t.Fatalf("write pending content: %v", err)
	}
	if err := pending.SyncAndCloseWriter(); err != nil {
		t.Fatalf("SyncAndCloseWriter: %v", err)
	}
	result, err := pending.Publish()
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if result.Destination != filepath.Join(root, "restored.bin") {
		t.Fatalf("destination=%q", result.Destination)
	}
	if _, err := pending.Publish(); !errors.Is(err, ErrInvalidState) {
		t.Fatalf("second Publish error=%v, want ErrInvalidState", err)
	}
}

func TestBeginRejectsDestinationOutsideTrustedRoot(t *testing.T) {
	root := t.TempDir()
	_, err := Begin(Request{
		Destination: filepath.Join(filepath.Dir(root), "outside.bin"),
		TrustedRoot: root,
	})
	if err == nil {
		t.Fatal("Begin unexpectedly accepted escaping destination")
	}
}

func TestAbortRemovesOnlyOwnedTemporaryObject(t *testing.T) {
	root := t.TempDir()
	destination := filepath.Join(root, "restored.bin")
	pending, err := Begin(Request{Destination: destination, TrustedRoot: root})
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	if err := writeAll(pending, []byte("temporary")); err != nil {
		t.Fatalf("write pending content: %v", err)
	}
	if err := pending.Abort(); err != nil {
		t.Fatalf("Abort: %v", err)
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("temporary object remained after abort: %v", entries)
	}
}

func writeAll(pending *Pending, content []byte) error {
	writer := pending.Writer()
	if writer == nil {
		return ErrInvalidState
	}
	_, err := writer.Write(content)
	return err
}
