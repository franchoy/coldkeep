package container

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func createTestContainerFile(t *testing.T, path string, maxSize int64) {
	t.Helper()
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("create container file: %v", err)
	}
	defer func() { _ = f.Close() }()
	if err := writeNewContainerHeader(f, maxSize); err != nil {
		t.Fatalf("write container header: %v", err)
	}
}

func openWritableTestContainer(t *testing.T, maxSize int64) *FileContainer {
	t.Helper()
	path := filepath.Join(t.TempDir(), "container.bin")
	createTestContainerFile(t, path, maxSize)
	c, err := OpenWritableContainer(path, maxSize)
	if err != nil {
		t.Fatalf("open writable container: %v", err)
	}
	return c
}

func TestFileContainerMethodsFailWhenClosed(t *testing.T) {
	c := openWritableTestContainer(t, ContainerHdrLen+32)
	if err := c.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	if _, err := c.Append([]byte("x")); err == nil || !strings.Contains(err.Error(), "container is closed") {
		t.Fatalf("expected append closed-container contract, got: %v", err)
	}
	if _, err := c.ReadAt(ContainerHdrLen, 1); err == nil || !strings.Contains(err.Error(), "container is closed") {
		t.Fatalf("expected read closed-container contract, got: %v", err)
	}
	if err := c.Truncate(ContainerHdrLen); err == nil || !strings.Contains(err.Error(), "container is closed") {
		t.Fatalf("expected truncate closed-container contract, got: %v", err)
	}
	if err := c.Sync(); err == nil || !strings.Contains(err.Error(), "container is closed") {
		t.Fatalf("expected sync closed-container contract, got: %v", err)
	}
}

func TestFileContainerAppendFailsWhenFull(t *testing.T) {
	// Start at ContainerHdrLen and allow only one extra byte.
	c := openWritableTestContainer(t, ContainerHdrLen+1)
	defer func() { _ = c.Close() }()

	_, err := c.Append([]byte("xx"))
	if !errors.Is(err, ErrContainerFull) {
		t.Fatalf("expected ErrContainerFull, got: %v", err)
	}
}

func TestFileContainerReadAtFailsOnShortRead(t *testing.T) {
	c := openWritableTestContainer(t, ContainerHdrLen+32)
	defer func() { _ = c.Close() }()

	// No payload has been written, so reads from the payload region are short.
	_, err := c.ReadAt(ContainerHdrLen, 1)
	if err == nil || !strings.Contains(err.Error(), "short read") {
		t.Fatalf("expected short-read error contract, got: %v", err)
	}
}

func TestOpenWritableContainerFailsOnInvalidHeader(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bad-header.bin")
	if err := os.WriteFile(path, []byte("not-a-valid-container-header"), 0o644); err != nil {
		t.Fatalf("write invalid container file: %v", err)
	}

	_, err := OpenWritableContainer(path, ContainerHdrLen+32)
	if err == nil || !strings.Contains(err.Error(), "validate container header") {
		t.Fatalf("expected wrapped header-validation contract, got: %v", err)
	}
}

func TestOpenReadOnlyContainerFailsOnInvalidHeader(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bad-header-readonly.bin")
	if err := os.WriteFile(path, []byte("still-not-a-valid-container-header"), 0o644); err != nil {
		t.Fatalf("write invalid container file: %v", err)
	}

	_, err := OpenReadOnlyContainer(path, ContainerHdrLen+32)
	if err == nil || !strings.Contains(err.Error(), "validate container header") {
		t.Fatalf("expected wrapped readonly header-validation contract, got: %v", err)
	}
}
