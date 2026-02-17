package main

import (
	"crypto/rand"
	"encoding/binary"
	"hash/crc32"
	"os"
	"time"
)

const (
	ContainerMagic    = "CAPSULE0" // must be exactly 8 bytes
	ContainerHdrLenV0 = 64         // fixed header size in bytes
)

// Reserved future flags (not used yet)
const (
	FLAG_WHOLE_COMPRESS = 1 << 0
	FLAG_WHOLE_ENCRYPT  = 1 << 1
	FLAG_BLOCK_LAYOUT   = 1 << 2
	FLAG_HAS_BLOCK_TBL  = 1 << 3
)

func writeNewContainerHeaderV0(f *os.File, maxSize int64) error {
	h := make([]byte, ContainerHdrLenV0)

	// 0..7 magic
	copy(h[0:8], []byte(ContainerMagic))

	// version
	binary.LittleEndian.PutUint16(h[8:10], 0)  // major
	binary.LittleEndian.PutUint16(h[10:12], 1) // minor

	// header length
	binary.LittleEndian.PutUint32(h[12:16], uint32(ContainerHdrLenV0))

	// flags (none for V0)
	binary.LittleEndian.PutUint32(h[16:20], 0)

	// created timestamp
	binary.LittleEndian.PutUint64(h[20:28], uint64(time.Now().UnixNano()))

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
