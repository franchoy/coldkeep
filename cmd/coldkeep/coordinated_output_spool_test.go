package main

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"
	"runtime"
	"testing"
)

func TestCoordinatedOutputSpoolStreamsLargePayloadAndCleansUp(t *testing.T) {
	const payloadSize = 8 * 1024 * 1024
	const chunkSize = 32 * 1024

	spoolDirectory := t.TempDir()
	spool, err := newCoordinatedOutputSpool(spoolDirectory)
	if err != nil {
		t.Fatalf("newCoordinatedOutputSpool: %v", err)
	}
	spoolPath := spool.path

	chunk := make([]byte, chunkSize)
	for index := range chunk {
		chunk[index] = byte((index*31 + 17) % 251)
	}
	expectedHash := sha256.New()
	_, captureErr := spool.capture(func() error {
		remaining := payloadSize
		for remaining > 0 {
			writeSize := min(remaining, len(chunk))
			written, err := os.Stdout.Write(chunk[:writeSize])
			if err != nil {
				return err
			}
			if written != writeSize {
				return io.ErrShortWrite
			}
			if _, err := expectedHash.Write(chunk[:writeSize]); err != nil {
				return err
			}
			remaining -= writeSize
		}
		return nil
	})
	if captureErr != nil {
		t.Fatalf("capture: %v", captureErr)
	}

	actualHash := sha256.New()
	counter := &countingHashWriter{hash: actualHash}
	if err := spool.replayTo(counter); err != nil {
		t.Fatalf("replayTo: %v", err)
	}
	if counter.count != payloadSize {
		t.Fatalf("replayed bytes=%d want=%d", counter.count, payloadSize)
	}
	if got, want := fmt.Sprintf("%x", actualHash.Sum(nil)), fmt.Sprintf("%x", expectedHash.Sum(nil)); got != want {
		t.Fatalf("replayed hash=%s want=%s", got, want)
	}

	if err := spool.cleanup(); err != nil {
		t.Fatalf("cleanup: %v", err)
	}
	if _, err := os.Lstat(spoolPath); !os.IsNotExist(err) {
		t.Fatalf("spool still exists after cleanup, stat err=%v", err)
	}
	requireDirectoryEmpty(t, spoolDirectory)
}

func TestCoordinatedOutputSpoolUsesRestrictiveCreateTempPermissions(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Windows temporary-file confidentiality is controlled by inherited ACLs")
	}

	spool, err := newCoordinatedOutputSpool(t.TempDir())
	if err != nil {
		t.Fatalf("newCoordinatedOutputSpool: %v", err)
	}
	defer func() {
		if err := spool.cleanup(); err != nil {
			t.Errorf("cleanup: %v", err)
		}
	}()

	info, err := spool.file.Stat()
	if err != nil {
		t.Fatalf("stat spool: %v", err)
	}
	if permissions := info.Mode().Perm(); permissions&0o077 != 0 {
		t.Fatalf("spool permissions=%#o expose group/other access", permissions)
	}
}

func TestCoordinatedOutputSpoolCleanupIsIdempotent(t *testing.T) {
	spoolDirectory := t.TempDir()
	spool, err := newCoordinatedOutputSpool(spoolDirectory)
	if err != nil {
		t.Fatalf("newCoordinatedOutputSpool: %v", err)
	}
	if err := spool.cleanup(); err != nil {
		t.Fatalf("first cleanup: %v", err)
	}
	if err := spool.cleanup(); err != nil {
		t.Fatalf("second cleanup: %v", err)
	}
	requireDirectoryEmpty(t, spoolDirectory)
}

func TestCoordinatedOutputSpoolReplayFailureIsReturned(t *testing.T) {
	spool, err := newCoordinatedOutputSpool(t.TempDir())
	if err != nil {
		t.Fatalf("newCoordinatedOutputSpool: %v", err)
	}
	defer func() { _ = spool.cleanup() }()

	_, err = spool.capture(func() error {
		_, writeErr := io.WriteString(os.Stdout, "payload")
		return writeErr
	})
	if err != nil {
		t.Fatalf("capture: %v", err)
	}

	wantErr := errors.New("replay failure")
	if err := spool.replayTo(failingOutputWriter{err: wantErr}); !errors.Is(err, wantErr) {
		t.Fatalf("replay error=%v want errors.Is(%v)", err, wantErr)
	}
}

type countingHashWriter struct {
	hash  hash.Hash
	count int
}

func (writer *countingHashWriter) Write(data []byte) (int, error) {
	written, err := writer.hash.Write(data)
	writer.count += written
	return written, err
}

type failingOutputWriter struct {
	err error
}

func (writer failingOutputWriter) Write([]byte) (int, error) {
	return 0, writer.err
}
