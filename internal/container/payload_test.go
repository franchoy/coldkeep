package container

import (
	"errors"
	"strings"
	"testing"
)

// errContainer is a minimal Container stub whose ReadAt always returns a fixed error.
type errContainer struct{ err error }

func (e errContainer) Append(_ []byte) (int64, error)          { return 0, nil }
func (e errContainer) ReadAt(_ int64, _ int64) ([]byte, error) { return nil, e.err }
func (e errContainer) Size() int64                             { return 0 }
func (e errContainer) Truncate(_ int64) error                  { return nil }
func (e errContainer) Sync() error                             { return nil }
func (e errContainer) Close() error                            { return nil }

func TestReadPayloadAtFailsOnNegativeSize(t *testing.T) {
	// size < 0 check fires before c is used, so nil container is safe.
	_, err := ReadPayloadAt(nil, 0, -1)
	if err == nil || !strings.Contains(err.Error(), "invalid payload size") {
		t.Fatalf("expected invalid-payload-size error contract, got: %v", err)
	}
}

func TestReadPayloadAtWrapsReadError(t *testing.T) {
	readErr := errors.New("disk I/O failure")
	c := errContainer{err: readErr}

	_, err := ReadPayloadAt(c, 64, 16)
	if err == nil || !strings.Contains(err.Error(), "read payload at offset") ||
		!strings.Contains(err.Error(), "disk I/O failure") {
		t.Fatalf("expected wrapped read-error contract, got: %v", err)
	}
}

func TestReadPayloadAtReturnsPayloadOnSuccess(t *testing.T) {
	want := []byte("hello-payload")
	c := okContainer{data: want}

	got, err := ReadPayloadAt(c, 64, int64(len(want)))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(got) != string(want) {
		t.Fatalf("payload mismatch: got %q want %q", got, want)
	}
}

// okContainer returns a fixed byte slice from ReadAt regardless of offset/size.
type okContainer struct{ data []byte }

func (o okContainer) Append(_ []byte) (int64, error)          { return 0, nil }
func (o okContainer) ReadAt(_ int64, _ int64) ([]byte, error) { return o.data, nil }
func (o okContainer) Size() int64                             { return int64(len(o.data)) }
func (o okContainer) Truncate(_ int64) error                  { return nil }
func (o okContainer) Sync() error                             { return nil }
func (o okContainer) Close() error                            { return nil }
