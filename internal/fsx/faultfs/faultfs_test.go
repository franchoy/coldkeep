package faultfs

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/franchoy/coldkeep/internal/fsx"
)

func TestFaultFSNoScriptPreservesOSBehavior(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "payload.txt")

	fsys := New(fsx.Default(), nil)

	f, err := fsys.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o600)
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}

	if _, err := f.Write([]byte("hello")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := f.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		t.Fatalf("Seek: %v", err)
	}

	buf := make([]byte, 5)
	if _, err := f.ReadAt(buf, 0); err != nil {
		t.Fatalf("ReadAt: %v", err)
	}
	if string(buf) != "hello" {
		t.Fatalf("payload mismatch: got %q", string(buf))
	}
	if err := f.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestFaultFSNilBaseFallsBackToDefault(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "payload.txt")

	fsys := New(nil, nil)

	f, err := fsys.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o600)
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	if _, err := f.Write([]byte("ok")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if string(got) != "ok" {
		t.Fatalf("payload mismatch: got %q", string(got))
	}
}

func TestFaultFSWriteFaultIsDeterministic(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "payload.txt")
	script := NewScript(Fault{Op: OpWrite, After: 2, Err: ErrFaultWrite})
	fsys := New(fsx.Default(), script)

	f, err := fsys.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	defer f.Close()

	if _, err := f.Write([]byte("first")); err != nil {
		t.Fatalf("first Write: %v", err)
	}
	if _, err := f.Write([]byte("second")); !errors.Is(err, ErrFaultWrite) {
		t.Fatalf("second Write error = %v, want ErrFaultWrite", err)
	}

	if got := script.CallCount(OpWrite); got != 2 {
		t.Fatalf("write call count = %d, want 2", got)
	}
	if got := script.BytesWritten(); got != 5 {
		t.Fatalf("bytes written = %d, want 5", got)
	}
}

func TestFaultFSPartialWriteRecordsPartialBytes(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "payload.txt")
	script := NewScript(Fault{
		Op:            OpWrite,
		After:         1,
		PartialWriteN: 3,
		Err:           ErrFaultWrite,
	})
	fsys := New(fsx.Default(), script)

	f, err := fsys.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	defer f.Close()

	n, err := f.Write([]byte("abcdef"))
	if !errors.Is(err, ErrFaultWrite) {
		t.Fatalf("Write error = %v, want ErrFaultWrite", err)
	}
	if n != 3 {
		t.Fatalf("Write n = %d, want 3", n)
	}
	if got := script.BytesWritten(); got != 3 {
		t.Fatalf("bytes written = %d, want 3", got)
	}

	got, readErr := os.ReadFile(path)
	if readErr != nil {
		t.Fatalf("ReadFile: %v", readErr)
	}
	if string(got) != "abc" {
		t.Fatalf("payload mismatch: got %q", string(got))
	}
}

func TestFaultFSENOSPCAfterBytesIsDeterministic(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "payload.txt")
	script := NewScript(Fault{
		Op:          OpWrite,
		ENOSPCAfter: 5,
		Err:         ErrFaultENOSPC,
	})
	fsys := New(fsx.Default(), script)

	f, err := fsys.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	defer f.Close()

	if _, err := f.Write([]byte("12345")); err != nil {
		t.Fatalf("first Write: %v", err)
	}
	if _, err := f.Write([]byte("6")); !errors.Is(err, ErrFaultENOSPC) {
		t.Fatalf("second Write error = %v, want ErrFaultENOSPC", err)
	}
	if got := script.BytesWritten(); got != 5 {
		t.Fatalf("bytes written = %d, want 5", got)
	}
	if got := script.CallCount(OpWrite); got != 2 {
		t.Fatalf("write call count = %d, want 2", got)
	}
}

func TestFaultFSSyncFaultIsDeterministic(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "payload.txt")
	script := NewScript(Fault{Op: OpSync, After: 1, Err: ErrFaultSync})
	fsys := New(fsx.Default(), script)

	f, err := fsys.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	defer f.Close()

	if _, err := f.Write([]byte("payload")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := f.Sync(); !errors.Is(err, ErrFaultSync) {
		t.Fatalf("Sync error = %v, want ErrFaultSync", err)
	}
	if got := script.CallCount(OpSync); got != 1 {
		t.Fatalf("sync call count = %d, want 1", got)
	}
}

func TestFaultFSPathOperationFaultsAreDeterministic(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	src := filepath.Join(dir, "src.txt")
	dst := filepath.Join(dir, "dst.txt")
	if err := os.WriteFile(src, []byte("payload"), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	script := NewScript(
		Fault{Op: OpStat, After: 1, Err: ErrFaultStat},
		Fault{Op: OpMkdirAll, After: 1, Err: ErrFaultMkdir},
		Fault{Op: OpRename, After: 1, Err: ErrFaultRename},
		Fault{Op: OpRemove, After: 1, Err: ErrFaultRemove},
	)
	fsys := New(fsx.Default(), script)

	if _, err := fsys.Stat(src); !errors.Is(err, ErrFaultStat) {
		t.Fatalf("Stat error = %v, want ErrFaultStat", err)
	}
	if err := fsys.MkdirAll(filepath.Join(dir, "sub"), 0o700); !errors.Is(err, ErrFaultMkdir) {
		t.Fatalf("MkdirAll error = %v, want ErrFaultMkdir", err)
	}
	if err := fsys.Rename(src, dst); !errors.Is(err, ErrFaultRename) {
		t.Fatalf("Rename error = %v, want ErrFaultRename", err)
	}
	if err := fsys.Remove(src); !errors.Is(err, ErrFaultRemove) {
		t.Fatalf("Remove error = %v, want ErrFaultRemove", err)
	}

	if got := script.CallCount(OpStat); got != 1 {
		t.Fatalf("stat call count = %d, want 1", got)
	}
	if got := script.CallCount(OpMkdirAll); got != 1 {
		t.Fatalf("mkdir call count = %d, want 1", got)
	}
	if got := script.CallCount(OpRename); got != 1 {
		t.Fatalf("rename call count = %d, want 1", got)
	}
	if got := script.CallCount(OpRemove); got != 1 {
		t.Fatalf("remove call count = %d, want 1", got)
	}
}
