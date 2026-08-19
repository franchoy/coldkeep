package container

import (
	"errors"
	"strings"
	"testing"
)

// errContainer is a minimal Container stub whose ReadAt always returns a fixed error.
type errContainer struct {
	err  error
	size int64
}

func (e errContainer) Append(_ []byte) (int64, error)          { return 0, nil }
func (e errContainer) ReadAt(_ int64, _ int64) ([]byte, error) { return nil, e.err }
func (e errContainer) Size() int64                             { return e.size }
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
	c := errContainer{err: readErr, size: ContainerHdrLen + 16}

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

type trackingContainer struct {
	size      int64
	readCalls int
}

func (c *trackingContainer) Append(_ []byte) (int64, error) { return 0, nil }
func (c *trackingContainer) ReadAt(_ int64, size int64) ([]byte, error) {
	c.readCalls++
	return make([]byte, int(size)), nil
}
func (c *trackingContainer) Size() int64            { return c.size }
func (c *trackingContainer) Truncate(_ int64) error { return nil }
func (c *trackingContainer) Sync() error            { return nil }
func (c *trackingContainer) Close() error           { return nil }

func TestReadPayloadAtRejectsInvalidRangeBeforeRead(t *testing.T) {
	tests := []struct {
		name   string
		offset int64
		size   int64
	}{
		{name: "negative offset", offset: -1, size: 1},
		{name: "header overlap", offset: ContainerHdrLen - 1, size: 1},
		{name: "past EOF", offset: ContainerHdrLen + 5, size: 1},
		{name: "overflow shape", offset: ContainerHdrLen, size: int64(^uint64(0) >> 1)},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := &trackingContainer{size: ContainerHdrLen + 4}
			if _, err := ReadPayloadAt(c, tc.offset, tc.size); err == nil {
				t.Fatal("expected invalid payload range error")
			}
			if c.readCalls != 0 {
				t.Fatalf("expected invalid range to be rejected before ReadAt, got %d calls", c.readCalls)
			}
		})
	}
}

func TestReadPayloadAtAllowsFirstPayloadAndExactEOF(t *testing.T) {
	c := &trackingContainer{size: ContainerHdrLen + 4}

	if got, err := ReadPayloadAt(c, ContainerHdrLen, 4); err != nil || len(got) != 4 {
		t.Fatalf("read first payload through EOF: len=%d err=%v", len(got), err)
	}
	if got, err := ReadPayloadAt(c, c.Size(), 0); err != nil || len(got) != 0 {
		t.Fatalf("read zero bytes at EOF: len=%d err=%v", len(got), err)
	}
	if c.readCalls != 2 {
		t.Fatalf("expected two delegated reads, got %d", c.readCalls)
	}
}

// okContainer returns a fixed byte slice from ReadAt regardless of offset/size.
type okContainer struct{ data []byte }

func (o okContainer) Append(_ []byte) (int64, error)          { return 0, nil }
func (o okContainer) ReadAt(_ int64, _ int64) ([]byte, error) { return o.data, nil }
func (o okContainer) Size() int64                             { return ContainerHdrLen + int64(len(o.data)) }
func (o okContainer) Truncate(_ int64) error                  { return nil }
func (o okContainer) Sync() error                             { return nil }
func (o okContainer) Close() error                            { return nil }
