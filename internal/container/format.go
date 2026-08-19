package container

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"time"

	"github.com/franchoy/coldkeep/internal/fsx"
)

const (
	ContainerMagic  = "ColdKeep" // must be exactly 8 bytes
	ContainerHdrLen = 64         // fixed header size in bytes
)

// Container format versioning is intentionally decoupled from the app version.
// This keeps on-disk compatibility stable while CLI releases evolve.
const (
	ContainerFormatVersionMajor uint16 = 1
	ContainerFormatVersionMinor uint16 = 0
)

// Supported legacy format major from pre-v1 headers that used app semantic
// version fields in bytes 8..11.
const LegacyContainerFormatVersionMajor uint16 = 0

// Optional container-level codec hint for future migrations.
// Current write path keeps this as unknown because blocks already persist codec
// per payload, and mixed-codec containers are allowed.
const (
	ContainerCodecUnknown uint16 = 0
	ContainerCodecPlain   uint16 = 1
	ContainerCodecAESGCM  uint16 = 2
)

// Reserved future flags (not used yet)
const (
	FLAG_BLOCK_LAYOUT  = 1 << 0 // container uses block-based layout
	FLAG_HAS_BLOCK_TBL = 1 << 1 // optional block index present
)

// Header offsets (all little-endian).
const (
	hdrMagicStart   = 0
	hdrMagicEnd     = 8
	hdrVersionMajor = 8
	hdrVersionMinor = 10
	hdrHeaderLen    = 12
	hdrFlags        = 16
	hdrCreatedAt    = 20
	hdrMaxSize      = 28
	hdrUIDStart     = 36
	hdrUIDEnd       = 52
	hdrCRC          = 52
	hdrCodecID      = 56

	hdrLegacyCRCDataEnd = 52 // legacy headers checksummed bytes [0..51]
	hdrV1CRCDataEnd     = 60 // v1 headers checksum includes codec/reserved bytes [0..59]
)

type Header struct {
	FormatMajor uint16
	FormatMinor uint16
	HeaderLen   uint32
	Flags       uint32
	CreatedAt   int64
	MaxSize     int64
	CodecID     uint16
}

func writeNewContainerHeader(f fsx.File, maxSize int64) error {
	if maxSize <= ContainerHdrLen {
		return fmt.Errorf("invalid container max size: %d", maxSize)
	}

	h := make([]byte, ContainerHdrLen)

	// 0..7 magic
	copy(h[hdrMagicStart:hdrMagicEnd], []byte(ContainerMagic))

	// Stable container format version (not app semantic version).
	binary.LittleEndian.PutUint16(h[hdrVersionMajor:hdrVersionMinor], ContainerFormatVersionMajor)
	binary.LittleEndian.PutUint16(h[hdrVersionMinor:hdrHeaderLen], ContainerFormatVersionMinor)

	// header length
	binary.LittleEndian.PutUint32(h[hdrHeaderLen:hdrFlags], uint32(ContainerHdrLen))

	// flags (structure only; no compression/encryption semantics)
	binary.LittleEndian.PutUint32(h[hdrFlags:hdrCreatedAt], 0)

	// created timestampZ UTC
	binary.LittleEndian.PutUint64(h[hdrCreatedAt:hdrMaxSize], uint64(time.Now().UTC().UnixNano()))

	// max container size policy
	binary.LittleEndian.PutUint64(h[hdrMaxSize:hdrUIDStart], uint64(maxSize))

	// container UID (random 16 bytes)
	_, _ = rand.Read(h[hdrUIDStart:hdrUIDEnd])

	// optional codec hint (currently unknown/mixed)
	binary.LittleEndian.PutUint16(h[hdrCodecID:hdrCodecID+2], ContainerCodecUnknown)

	// CRC32 of v1 checksum window with CRC field zeroed.
	crc := computeHeaderCRC(h, ContainerFormatVersionMajor)
	binary.LittleEndian.PutUint32(h[hdrCRC:hdrCodecID], crc)

	// 56..63 reserved (left as zero)

	n, err := f.Write(h)
	if err != nil {
		return err
	}
	if n != len(h) {
		return io.ErrShortWrite
	}
	return nil
}

func readAndValidateContainerHeader(f fsx.File) (Header, error) {
	h := make([]byte, ContainerHdrLen)
	n, err := f.ReadAt(h, 0)
	if !isHeaderReadComplete(err) {
		return Header{}, fmt.Errorf("read container header: %w", err)
	}
	if n < ContainerHdrLen {
		return Header{}, fmt.Errorf("container too small: size=%d header=%d", n, ContainerHdrLen)
	}

	if string(h[hdrMagicStart:hdrMagicEnd]) != ContainerMagic {
		return Header{}, fmt.Errorf("invalid container magic")
	}

	major := binary.LittleEndian.Uint16(h[hdrVersionMajor:hdrVersionMinor])
	minor := binary.LittleEndian.Uint16(h[hdrVersionMinor:hdrHeaderLen])
	hdrLen := binary.LittleEndian.Uint32(h[hdrHeaderLen:hdrFlags])
	if hdrLen != uint32(ContainerHdrLen) {
		return Header{}, fmt.Errorf("unsupported container header length: %d", hdrLen)
	}

	if !isSupportedContainerMajorVersion(major) {
		return Header{}, fmt.Errorf("unsupported container format version: %d.%d", major, minor)
	}

	storedCRC := binary.LittleEndian.Uint32(h[hdrCRC:hdrCodecID])
	expectedCRC := computeHeaderCRC(h, major)
	if storedCRC != expectedCRC {
		return Header{}, fmt.Errorf("container header crc mismatch")
	}

	codecID := uint16(ContainerCodecUnknown)
	if major >= ContainerFormatVersionMajor {
		codecID = binary.LittleEndian.Uint16(h[hdrCodecID : hdrCodecID+2])
	}

	maxSize := int64(binary.LittleEndian.Uint64(h[hdrMaxSize:hdrUIDStart]))
	if maxSize <= ContainerHdrLen {
		return Header{}, fmt.Errorf("invalid container max size: %d", maxSize)
	}

	return Header{
		FormatMajor: major,
		FormatMinor: minor,
		HeaderLen:   hdrLen,
		Flags:       binary.LittleEndian.Uint32(h[hdrFlags:hdrCreatedAt]),
		CreatedAt:   int64(binary.LittleEndian.Uint64(h[hdrCreatedAt:hdrMaxSize])),
		MaxSize:     maxSize,
		CodecID:     codecID,
	}, nil
}

// isHeaderReadComplete reports whether a ReadAt error should be treated as a
// complete read. ReadAt on a file smaller than ContainerHdrLen returns (n, io.EOF),
// which is handled separately by the size check. Any other non-nil error is fatal.
func isHeaderReadComplete(err error) bool {
	return err == nil || err == io.EOF
}

// isSupportedContainerMajorVersion reports whether major is a known container
// format major version. Both the current v1 format and the legacy v0 format
// (pre-v1 headers that encoded the app semantic version) are supported.
func isSupportedContainerMajorVersion(major uint16) bool {
	return major == ContainerFormatVersionMajor || major == LegacyContainerFormatVersionMajor
}

func computeHeaderCRC(h []byte, major uint16) uint32 {
	buf := make([]byte, len(h))
	copy(buf, h)
	binary.LittleEndian.PutUint32(buf[hdrCRC:hdrCodecID], 0)

	if major == LegacyContainerFormatVersionMajor {
		return crc32.ChecksumIEEE(buf[0:hdrLegacyCRCDataEnd])
	}
	return crc32.ChecksumIEEE(buf[0:hdrV1CRCDataEnd])
}
