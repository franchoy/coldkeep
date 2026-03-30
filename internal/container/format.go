package container

import (
	"crypto/rand"
	"encoding/binary"
	"hash/crc32"
	"os"
	"time"

	"github.com/franchoy/coldkeep/internal/version"
)

const (
	ContainerMagic  = "ColdKeep" // must be exactly 8 bytes
	ContainerHdrLen = 64         // fixed header size in bytes
)

// Reserved future flags (not used yet)
const (
	FLAG_BLOCK_LAYOUT  = 1 << 0 // container uses block-based layout
	FLAG_HAS_BLOCK_TBL = 1 << 1 // optional block index present
)

func writeNewContainerHeader(f *os.File, maxSize int64) error {
	h := make([]byte, ContainerHdrLen)

	// 0..7 magic
	copy(h[0:8], []byte(ContainerMagic))

	// version
	binary.LittleEndian.PutUint16(h[8:10], uint16(version.Major))  // major
	binary.LittleEndian.PutUint16(h[10:12], uint16(version.Minor)) // minor

	// header length
	binary.LittleEndian.PutUint32(h[12:16], uint32(ContainerHdrLen))

	// flags (structure only; no compression/encryption semantics)
	binary.LittleEndian.PutUint32(h[16:20], 0)

	// created timestampZ UTC
	binary.LittleEndian.PutUint64(h[20:28], uint64(time.Now().UTC().UnixNano()))

	// max container size policy
	binary.LittleEndian.PutUint64(h[28:36], uint64(maxSize))

	// container UID (random 16 bytes)
	_, _ = rand.Read(h[36:52])

	// CRC32 of header bytes 0..51
	crc := crc32.ChecksumIEEE(h[0:52])
	binary.LittleEndian.PutUint32(h[52:56], crc)

	// 56..63 reserved (left as zero)

	_, err := f.Write(h)
	return err
}
