package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"path/filepath"

	"github.com/franchoy/coldkeep/internal/blocks"
	"github.com/franchoy/coldkeep/internal/container"
	"github.com/franchoy/coldkeep/internal/iodebug"
	storagemetadata "github.com/franchoy/coldkeep/internal/storage/metadata"
	"github.com/franchoy/coldkeep/internal/verify"
)

// StorageBlockReader implements blocks.BlockReader for reading blocks from storage.
// The read path mirrors the write pipeline in reverse:
//
//	read payload → reverse transforms → verify logical hash → decode block
//
// Layer semantics (inverse of write path):
//
//	Layer 3 (persisted payload): raw bytes read from the container file.
//	Layer 2 (transformed payload): output of reverseTransforms (e.g. decrypted).
//	Layer 1 (logical block): plaintext encoded block bytes, verified against block_hash.
//
// block_hash verification (Stage 4) is fail-closed: a mismatch or missing hash
// is always fatal. This anchors restore correctness to the logical layer regardless
// of which transforms are active.
//
// v1.9 reverse transform behavior:
//
//	verify physical_hash (if present) -> decrypt (if aes-gcm)
//	-> verify compressed_hash (if present) -> decompress (if zstd)
//	-> verify block_hash -> decode logical block.
type StorageBlockReader struct {
	db            *sql.DB
	containersDir string
	verifyHash    bool
}

type hashLayer string

const (
	hashLayerPhysical   hashLayer = "physical"
	hashLayerCompressed hashLayer = "compressed"
	hashLayerLogical    hashLayer = "logical"
)

var (
	ErrPhysicalPayloadHashMismatch   = errors.New("physical payload hash mismatch")
	ErrCompressedPayloadHashMismatch = errors.New("compressed payload hash mismatch")
	ErrLogicalBlockHashMismatch      = errors.New("logical block hash mismatch")
)

// HashMismatchError is a typed, layer-specific hash mismatch with safe context.
// It includes identifiers and digests only (never raw payload bytes or key material).
type HashMismatchError struct {
	Layer       hashLayer
	BlockID     int64
	ContainerID int64
	Offset      int64
	Expected    string
	Actual      string
}

func (e *HashMismatchError) Error() string {
	prefix := "hash mismatch"
	switch e.Layer {
	case hashLayerPhysical:
		prefix = "physical payload hash mismatch"
	case hashLayerCompressed:
		prefix = "compressed payload hash mismatch"
	case hashLayerLogical:
		prefix = "logical block hash mismatch"
	}
	return fmt.Sprintf("%s: block_id=%d container_id=%d offset=%d expected=%s actual=%s",
		prefix, e.BlockID, e.ContainerID, e.Offset, e.Expected, e.Actual)
}

func (e *HashMismatchError) Unwrap() error {
	switch e.Layer {
	case hashLayerPhysical:
		return ErrPhysicalPayloadHashMismatch
	case hashLayerCompressed:
		return ErrCompressedPayloadHashMismatch
	case hashLayerLogical:
		return ErrLogicalBlockHashMismatch
	default:
		return nil
	}
}

// NewStorageBlockReader creates a BlockReader for storage_blocks-based block storage.
func NewStorageBlockReader(db *sql.DB, containersDir string) *StorageBlockReader {
	return &StorageBlockReader{
		db:            db,
		containersDir: containersDir,
		verifyHash:    true, // fail-closed logical hash verification is mandatory
	}
}

// ReadBlock implements blocks.BlockReader.
//
// Read pipeline (reverse of write):
//  1. Load block metadata from storage_blocks
//  2. Read stored bytes from container
//  3. Reverse transforms (verify/decrypt/decompress per block metadata)
//  4. Verify logical hash (mandatory, fail-closed)
//  5. Decode block to reconstruct EncodedBlock
func (r *StorageBlockReader) ReadBlock(ctx context.Context, blockID int64) (*blocks.EncodedBlock, error) {
	if blockID <= 0 {
		return nil, fmt.Errorf("invalid block ID: %d", blockID)
	}

	// Stage 1: Load block metadata.
	metadata, err := r.loadBlockMetadata(ctx, blockID)
	if err != nil {
		return nil, fmt.Errorf("load block %d metadata: %w", blockID, err)
	}
	if r.verifyHash && len(metadata.Metadata.Hashes.LogicalHash) == 0 {
		return nil, fmt.Errorf("block %d has empty block_hash; fail-closed hash verification requires non-empty block_hash", blockID)
	}

	verified, err := verify.VerifyStoredBlock(ctx, toVerifyBlockStorageMetadata(metadata), storageVerifyContainerReader{reader: r})
	if err != nil {
		return nil, r.mapVerifyPipelineFailure(err)
	}

	iodebug.IncBlockDecode()
	return verified.DecodedBlock, nil
}

type storageVerifyContainerReader struct {
	reader *StorageBlockReader
}

func (r storageVerifyContainerReader) ReadStoredPayload(_ context.Context, meta verify.BlockStorageMetadata) ([]byte, error) {
	converted := &blockMetadata{
		ID:              meta.BlockID,
		FormatVersion:   int(meta.FormatVersion),
		Codec:           meta.Codec,
		ContainerID:     meta.ContainerID,
		ContainerName:   meta.ContainerName,
		ContainerOffset: meta.ContainerOffset,
		Metadata: storagemetadata.BlockStorageMetadata{
			Compression: storagemetadata.CompressionMetadata{
				Codec: meta.CompressionCodec,
				Level: meta.CompressionLevel,
			},
			Sizes: storagemetadata.PayloadMetadata{
				PlaintextSize:  meta.PlaintextSize,
				CompressedSize: meta.CompressedSize,
				StoredSize:     meta.StoredSize,
			},
			Hashes: storagemetadata.HashMetadata{
				LogicalHash:    meta.LogicalHash,
				CompressedHash: meta.CompressedHash,
				PhysicalHash:   meta.PhysicalHash,
			},
		},
	}

	return r.reader.readStoredPayload(converted)
}

func toVerifyBlockStorageMetadata(meta *blockMetadata) verify.BlockStorageMetadata {
	if meta == nil {
		return verify.BlockStorageMetadata{}
	}

	return verify.BlockStorageMetadata{
		BlockID:          meta.ID,
		ContainerID:      meta.ContainerID,
		ContainerOffset:  meta.ContainerOffset,
		ContainerName:    meta.ContainerName,
		ContainerMaxSize: container.GetContainerMaxSize(),
		FormatVersion:    int64(meta.FormatVersion),
		Codec:            meta.Codec,
		PlaintextSize:    meta.Metadata.Sizes.PlaintextSize,
		CompressedSize:   meta.Metadata.Sizes.CompressedSize,
		StoredSize:       meta.Metadata.Sizes.StoredSize,
		CompressionCodec: meta.Metadata.Compression.Codec,
		CompressionLevel: meta.Metadata.Compression.Level,
		LogicalHash:      meta.Metadata.Hashes.LogicalHash,
		CompressedHash:   meta.Metadata.Hashes.CompressedHash,
		PhysicalHash:     meta.Metadata.Hashes.PhysicalHash,
	}
}

func (r *StorageBlockReader) mapVerifyPipelineFailure(err error) error {
	var vf *verify.VerifyFailure
	if !errors.As(err, &vf) || vf == nil {
		return err
	}

	if vf.BlockID == nil || vf.ContainerID == nil || vf.Offset == nil {
		return err
	}

	switch vf.Category {
	case "physical_hash_mismatch":
		mapped := &HashMismatchError{
			Layer:       hashLayerPhysical,
			BlockID:     *vf.BlockID,
			ContainerID: *vf.ContainerID,
			Offset:      *vf.Offset,
			Expected:    vf.ExpectedHash,
			Actual:      vf.ActualHash,
		}
		return errors.Join(mapped, err)
	case "compressed_hash_mismatch":
		mapped := &HashMismatchError{
			Layer:       hashLayerCompressed,
			BlockID:     *vf.BlockID,
			ContainerID: *vf.ContainerID,
			Offset:      *vf.Offset,
			Expected:    vf.ExpectedHash,
			Actual:      vf.ActualHash,
		}
		return errors.Join(mapped, err)
	case "block_hash_mismatch":
		mapped := &HashMismatchError{
			Layer:       hashLayerLogical,
			BlockID:     *vf.BlockID,
			ContainerID: *vf.ContainerID,
			Offset:      *vf.Offset,
			Expected:    vf.ExpectedHash,
			Actual:      vf.ActualHash,
		}
		return errors.Join(mapped, err)
	default:
		return err
	}
}

// blockMetadata represents the persistent metadata about a block.
type blockMetadata struct {
	ID              int64
	FormatVersion   int
	Codec           string
	Metadata        storagemetadata.BlockStorageMetadata
	ContainerID     int64
	ContainerName   string
	ContainerOffset int64
	Nonce           []byte
}

// loadBlockMetadata queries storage_blocks and container tables to get full block metadata.
func (r *StorageBlockReader) loadBlockMetadata(ctx context.Context, blockID int64) (*blockMetadata, error) {
	if r.db == nil {
		return nil, fmt.Errorf("database connection is nil")
	}

	query := `
		SELECT 
			b.id, b.format_version, b.codec, b.plaintext_size,
			b.compression_codec, b.compression_level, b.compressed_size,
			b.stored_size, b.container_id, b.container_offset, b.block_hash,
			b.compressed_hash, b.physical_hash, c.filename
		FROM storage_blocks b
		JOIN container c ON b.container_id = c.id
		WHERE b.id = $1
		LIMIT 1
	`

	var meta blockMetadata
	var codecStr string
	var compressionCodec string
	var compressionLevel sql.NullInt64
	var compressedSize sql.NullInt64
	var compressedHash []byte
	var physicalHash []byte

	err := r.db.QueryRowContext(ctx, query, blockID).Scan(
		&meta.ID,
		&meta.FormatVersion,
		&codecStr,
		&meta.Metadata.Sizes.PlaintextSize,
		&compressionCodec,
		&compressionLevel,
		&compressedSize,
		&meta.Metadata.Sizes.StoredSize,
		&meta.ContainerID,
		&meta.ContainerOffset,
		&meta.Metadata.Hashes.LogicalHash,
		&compressedHash,
		&physicalHash,
		&meta.ContainerName,
	)
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("block %d not found", blockID)
	}
	if err != nil {
		return nil, fmt.Errorf("query storage_blocks: %w", err)
	}

	meta.Codec = codecStr
	meta.Metadata.Compression.Codec = compressionCodec
	if compressionLevel.Valid {
		level := int(compressionLevel.Int64)
		meta.Metadata.Compression.Level = &level
	}
	if compressedSize.Valid {
		size := compressedSize.Int64
		meta.Metadata.Sizes.CompressedSize = &size
	}
	meta.Metadata.Hashes.CompressedHash = compressedHash
	meta.Metadata.Hashes.PhysicalHash = physicalHash

	// Validate metadata
	if meta.Metadata.Sizes.PlaintextSize <= 0 {
		return nil, fmt.Errorf("block %d has invalid plaintext_size: %d", blockID, meta.Metadata.Sizes.PlaintextSize)
	}
	if meta.Metadata.Sizes.StoredSize <= 0 {
		return nil, fmt.Errorf("block %d has invalid stored_size: %d", blockID, meta.Metadata.Sizes.StoredSize)
	}
	if meta.ContainerOffset < 0 {
		return nil, fmt.Errorf("block %d has invalid container_offset: %d", blockID, meta.ContainerOffset)
	}

	return &meta, nil
}

// readStoredPayload reads the raw stored bytes for a block from its container file.
// This is Stage 2 of the read pipeline.
func (r *StorageBlockReader) readStoredPayload(meta *blockMetadata) ([]byte, error) {
	containerPath := filepath.Join(r.containersDir, meta.ContainerName)

	// Open container file for reading
	// Note: We use the maximum container size here; this is just for validation purposes
	fc, err := container.OpenReadOnlyContainer(containerPath, container.GetContainerMaxSize())
	if err != nil {
		return nil, fmt.Errorf("open container %s: %w", meta.ContainerName, err)
	}
	defer func() { _ = fc.Close() }()

	// Read block bytes at offset using the Container interface
	payload, err := container.ReadPayloadAt(fc, meta.ContainerOffset, meta.Metadata.Sizes.StoredSize)
	if err != nil {
		return nil, fmt.Errorf("read from container at offset %d: %w", meta.ContainerOffset, err)
	}

	if int64(len(payload)) != meta.Metadata.Sizes.StoredSize {
		return nil, fmt.Errorf("read %d bytes but expected %d", len(payload), meta.Metadata.Sizes.StoredSize)
	}

	return payload, nil
}

// DisableHashVerification turns off mandatory hash verification (only for testing).
func (r *StorageBlockReader) DisableHashVerification() {
	r.verifyHash = false
}

// LogBlockRead logs block read operation for debugging.
func (r *StorageBlockReader) LogBlockRead(blockID int64, success bool, err error) {
	if success {
		log.Printf("event=block_read action=success block_id=%d", blockID)
	} else {
		log.Printf("event=block_read action=failed block_id=%d error=%v", blockID, err)
	}
}
