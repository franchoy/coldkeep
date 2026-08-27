//go:build windows

package secureinstall

import (
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"unsafe"

	"golang.org/x/sys/windows"
)

func TestWindowsRenameBufferUTF16Boundaries(t *testing.T) {
	nonBMPBelow := strings.Repeat("😀", windowsRenameMaxCodeUnits/2)
	nonBMPAt := nonBMPBelow + "a"
	tests := []struct {
		name      string
		finalName string
		wantUnits int
		wantErr   bool
	}{
		{name: "ascii below", finalName: strings.Repeat("a", windowsRenameMaxCodeUnits-1), wantUnits: windowsRenameMaxCodeUnits - 1},
		{name: "ascii at", finalName: strings.Repeat("a", windowsRenameMaxCodeUnits), wantUnits: windowsRenameMaxCodeUnits},
		{name: "ascii above", finalName: strings.Repeat("a", windowsRenameMaxCodeUnits+1), wantErr: true},
		{name: "non-BMP below", finalName: nonBMPBelow, wantUnits: windowsRenameMaxCodeUnits - 1},
		{name: "non-BMP at", finalName: nonBMPAt, wantUnits: windowsRenameMaxCodeUnits},
		{name: "non-BMP above", finalName: nonBMPAt + "a", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buffer, err := windowsRenameBuffer(windows.Handle(42), tt.finalName, true)
			if tt.wantErr {
				if !errors.Is(err, errWindowsRenameNameTooLong) {
					t.Fatalf("boundary error=%v", err)
				}
				if buffer != nil {
					t.Fatalf("boundary buffer length=%d, want nil", len(buffer))
				}
				return
			}
			if err != nil {
				t.Fatalf("rename buffer: %v", err)
			}
			var layout fileRenameInformation
			nameOffset := int(unsafe.Offsetof(layout.FileName))
			if got, want := len(buffer), nameOffset+tt.wantUnits*2; got != want {
				t.Fatalf("buffer length=%d want=%d", got, want)
			}
			rename := (*fileRenameInformation)(unsafe.Pointer(&buffer[0]))
			if got, want := rename.FileNameLength, uint32(tt.wantUnits*2); got != want {
				t.Fatalf("FileNameLength=%d want=%d", got, want)
			}
			if rename.RootDirectory != windows.Handle(42) {
				t.Fatalf("RootDirectory=%v", rename.RootDirectory)
			}
			if rename.ReplaceIfExists != windows.FILE_RENAME_REPLACE_IF_EXISTS {
				t.Fatalf("ReplaceIfExists=%d", rename.ReplaceIfExists)
			}
			encoded, encodeErr := windows.UTF16FromString(tt.finalName)
			if encodeErr != nil {
				t.Fatalf("encode expected name: %v", encodeErr)
			}
			for index, want := range encoded[:len(encoded)-1] {
				got := binary.LittleEndian.Uint16(buffer[nameOffset+index*2:])
				if got != want {
					t.Fatalf("UTF-16 unit %d=%#x want=%#x", index, got, want)
				}
			}
		})
	}
}

func TestWindowsRenameBufferRejectsEmbeddedNUL(t *testing.T) {
	buffer, err := windowsRenameBuffer(0, "invalid\x00name", false)
	if err == nil || !strings.Contains(err.Error(), "encode destination name") {
		t.Fatalf("embedded-NUL error=%v", err)
	}
	if buffer != nil {
		t.Fatalf("embedded-NUL buffer length=%d, want nil", len(buffer))
	}
}

func TestWindowsNoReplaceAndOverwrite(t *testing.T) {
	root := t.TempDir()
	destination := filepath.Join(root, "restored.bin")
	if _, err := installWindowsTestContent(destination, root, false, []byte("first")); err != nil {
		t.Fatalf("no-replace install: %v", err)
	}
	if _, err := installWindowsTestContent(destination, root, false, []byte("second")); !errors.Is(err, ErrDestinationExists) {
		t.Fatalf("existing destination error=%v", err)
	}
	if _, err := installWindowsTestContent(destination, root, true, []byte("replacement")); err != nil {
		t.Fatalf("overwrite install: %v", err)
	}
	got, err := os.ReadFile(destination)
	if err != nil || string(got) != "replacement" {
		t.Fatalf("destination bytes=%q err=%v", got, err)
	}
}

func TestWindowsPublicationUsesRetainedTemporaryObject(t *testing.T) {
	root := t.TempDir()
	destination := filepath.Join(root, "restored.bin")
	original := windowsOps
	t.Cleanup(func() { windowsOps = original })
	windowsOps.beforePublish = func() error {
		entries, err := os.ReadDir(root)
		if err != nil {
			return err
		}
		for _, entry := range entries {
			if !strings.HasPrefix(entry.Name(), ".coldkeep-restore-") {
				continue
			}
			originalName := filepath.Join(root, entry.Name())
			if err := os.Rename(originalName, filepath.Join(root, "retained-object-moved.bin")); err != nil {
				return err
			}
			return os.WriteFile(originalName, []byte("path-replacement"), 0o600)
		}
		return errors.New("temporary object name not found")
	}
	if _, err := installWindowsTestContent(destination, root, false, []byte("retained-object")); err != nil {
		t.Fatalf("retained-object publication: %v", err)
	}
	got, err := os.ReadFile(destination)
	if err != nil || string(got) != "retained-object" {
		t.Fatalf("destination bytes=%q err=%v", got, err)
	}
}

func installWindowsTestContent(destination, root string, overwrite bool, content []byte) (Result, error) {
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
