package container

import (
	"encoding/binary"
	"hash/crc32"
	"os"
	"testing"
)

func TestWriteNewContainerHeader_UsesStableFormatVersionAndCodecHint(t *testing.T) {
	tmp, err := os.CreateTemp(t.TempDir(), "container-header-*.bin")
	if err != nil {
		t.Fatalf("create temp file: %v", err)
	}
	defer func() { _ = tmp.Close() }()

	if err := writeNewContainerHeader(tmp, 1<<20); err != nil {
		t.Fatalf("write header: %v", err)
	}

	h, err := readAndValidateContainerHeader(tmp)
	if err != nil {
		t.Fatalf("read+validate header: %v", err)
	}
	if h.FormatMajor != ContainerFormatVersionMajor || h.FormatMinor != ContainerFormatVersionMinor {
		t.Fatalf("unexpected format version: got %d.%d want %d.%d", h.FormatMajor, h.FormatMinor, ContainerFormatVersionMajor, ContainerFormatVersionMinor)
	}
	if h.CodecID != ContainerCodecUnknown {
		t.Fatalf("unexpected codec id: got %d want %d", h.CodecID, ContainerCodecUnknown)
	}
}

func TestReadAndValidateContainerHeader_AcceptsLegacyHeaderCRCWindow(t *testing.T) {
	tmp, err := os.CreateTemp(t.TempDir(), "legacy-header-*.bin")
	if err != nil {
		t.Fatalf("create temp file: %v", err)
	}
	defer func() { _ = tmp.Close() }()

	hdr := make([]byte, ContainerHdrLen)
	copy(hdr[0:8], []byte(ContainerMagic))
	binary.LittleEndian.PutUint16(hdr[8:10], LegacyContainerFormatVersionMajor)
	binary.LittleEndian.PutUint16(hdr[10:12], 9)
	binary.LittleEndian.PutUint32(hdr[12:16], uint32(ContainerHdrLen))
	binary.LittleEndian.PutUint64(hdr[20:28], 123)
	binary.LittleEndian.PutUint64(hdr[28:36], uint64(2<<20))
	crc := crc32.ChecksumIEEE(hdr[0:52])
	binary.LittleEndian.PutUint32(hdr[52:56], crc)

	if _, err := tmp.Write(hdr); err != nil {
		t.Fatalf("write legacy header: %v", err)
	}

	parsed, err := readAndValidateContainerHeader(tmp)
	if err != nil {
		t.Fatalf("legacy header should validate: %v", err)
	}
	if parsed.FormatMajor != LegacyContainerFormatVersionMajor {
		t.Fatalf("unexpected legacy format major: %d", parsed.FormatMajor)
	}
	if parsed.CodecID != ContainerCodecUnknown {
		t.Fatalf("legacy header must default codec to unknown: got %d", parsed.CodecID)
	}
}

func TestReadAndValidateContainerHeader_RejectsInvalidMagic(t *testing.T) {
	tmp, err := os.CreateTemp(t.TempDir(), "bad-magic-*.bin")
	if err != nil {
		t.Fatalf("create temp file: %v", err)
	}
	defer func() { _ = tmp.Close() }()

	hdr := make([]byte, ContainerHdrLen)
	copy(hdr[0:8], []byte("NotKeep!"))
	binary.LittleEndian.PutUint16(hdr[8:10], ContainerFormatVersionMajor)
	binary.LittleEndian.PutUint16(hdr[10:12], ContainerFormatVersionMinor)
	binary.LittleEndian.PutUint32(hdr[12:16], uint32(ContainerHdrLen))
	crc := crc32.ChecksumIEEE(hdr[0:60])
	binary.LittleEndian.PutUint32(hdr[52:56], crc)

	if _, err := tmp.Write(hdr); err != nil {
		t.Fatalf("write header: %v", err)
	}

	if _, err := readAndValidateContainerHeader(tmp); err == nil {
		t.Fatalf("expected invalid magic error")
	}
}
