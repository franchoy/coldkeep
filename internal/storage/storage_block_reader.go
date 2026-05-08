package storage

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"path/filepath"
	"strings"
	"sync"

	"github.com/franchoy/coldkeep/internal/blocks"
	"github.com/franchoy/coldkeep/internal/container"
	"github.com/franchoy/coldkeep/internal/iodebug"
	storagecompression "github.com/franchoy/coldkeep/internal/storage/compression"
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
// Compression insertion point (Phase 3):
//
//	reverseTransforms will apply decompression after decryption.
//	verifyLogicalHash and decodeLogicalBlock require no changes.
type StorageBlockReader struct {
	db            *sql.DB
	containersDir string
	verifyHash    bool
	// transformerCache is shared by reads and guarded by transformerMu.
	// StorageBlockReader may be reused across goroutines.
	transformerMu    sync.RWMutex
	transformerCache map[blocks.Codec]blocks.Transformer
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

// NewStorageBlockReader creates a BlockReader for v1.8 block-based storage.
func NewStorageBlockReader(db *sql.DB, containersDir string) *StorageBlockReader {
	return &StorageBlockReader{
		db:               db,
		containersDir:    containersDir,
		verifyHash:       true, // mandatory verification in Phase 3+
		transformerCache: make(map[blocks.Codec]blocks.Transformer),
	}
}

// normalizeStorageBlockCodec maps persisted storage_blocks.codec values to runtime codecs.
// Canonical persisted values are "none" and "aes-gcm".
// "plain" is accepted as a legacy/test synonym for "none".
func normalizeStorageBlockCodec(raw string) (blocks.Codec, error) {
	codecText := strings.TrimSpace(strings.ToLower(raw))
	switch codecText {
	case packedStorageBlockCodecNone, "plain":
		return blocks.CodecPlain, nil
	case string(blocks.CodecAESGCM):
		return blocks.CodecAESGCM, nil
	default:
		return "", fmt.Errorf("unsupported storage_blocks codec %q (expected %q or %q)", raw, packedStorageBlockCodecNone, blocks.CodecAESGCM)
	}
}

// ReadBlock implements blocks.BlockReader.
//
// Read pipeline (reverse of write):
//  1. Load block metadata from storage_blocks
//  2. Read stored bytes from container
//  3. Reverse transforms (decrypt and/or future stages)
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

// verifyLogicalHash checks the plaintext encoded bytes against the stored block_hash.
// Verification is fail-closed: a mismatch or empty hash (when enabled) is fatal.
// This is Stage 4 of the read pipeline.
func (r *StorageBlockReader) verifyLogicalHash(meta *blockMetadata, plaintextBytes []byte, blockHash []byte) error {
	if !r.verifyHash {
		return nil
	}
	computed := blocks.HashLogical(plaintextBytes)
	if !bytes.Equal(computed, blockHash) {
		return &HashMismatchError{
			Layer:       hashLayerLogical,
			BlockID:     meta.ID,
			ContainerID: meta.ContainerID,
			Offset:      meta.ContainerOffset,
			Expected:    hex.EncodeToString(blockHash),
			Actual:      hex.EncodeToString(computed),
		}
	}
	return nil
}

// verifyPhysicalHash checks the raw persisted payload bytes against
// storage_blocks.physical_hash when present.
//
// Compatibility rule: legacy rows may have NULL/empty physical_hash and must
// not fail this stage.
func (r *StorageBlockReader) verifyPhysicalHash(meta *blockMetadata, storedBytes []byte, physicalHash []byte) error {
	if !r.verifyHash {
		return nil
	}
	if len(physicalHash) == 0 {
		return nil
	}

	computed := blocks.HashPhysical(storedBytes)
	if !bytes.Equal(computed, physicalHash) {
		return &HashMismatchError{
			Layer:       hashLayerPhysical,
			BlockID:     meta.ID,
			ContainerID: meta.ContainerID,
			Offset:      meta.ContainerOffset,
			Expected:    hex.EncodeToString(physicalHash),
			Actual:      hex.EncodeToString(computed),
		}
	}
	return nil
}

// verifyCompressedHash checks decrypted pre-decompression payload bytes against
// storage_blocks.compressed_hash when present.
//
// Compatibility rule: legacy rows may have NULL/empty compressed_hash and must
// not fail this stage.
func (r *StorageBlockReader) verifyCompressedHash(meta *blockMetadata, preDecompressionPayload []byte, compressedHash []byte) error {
	if !r.verifyHash {
		return nil
	}
	if len(compressedHash) == 0 {
		return nil
	}

	computed := blocks.HashCompressed(preDecompressionPayload)
	if !bytes.Equal(computed, compressedHash) {
		return &HashMismatchError{
			Layer:       hashLayerCompressed,
			BlockID:     meta.ID,
			ContainerID: meta.ContainerID,
			Offset:      meta.ContainerOffset,
			Expected:    hex.EncodeToString(compressedHash),
			Actual:      hex.EncodeToString(computed),
		}
	}
	return nil
}

// decodeLogicalBlock deserializes plaintext encoded bytes into an EncodedBlock.
// This is Stage 5 of the read pipeline.
func (r *StorageBlockReader) decodeLogicalBlock(blockID int64, plaintextBytes []byte) (*blocks.EncodedBlock, error) {
	encodedBlock, err := blocks.DecodeBlock(plaintextBytes)
	if err != nil {
		return nil, fmt.Errorf("decode block %d: %w", blockID, err)
	}
	if encodedBlock == nil {
		return nil, fmt.Errorf("decode block %d returned nil", blockID)
	}
	return encodedBlock, nil
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

// reverseTransforms applies the inverse of every transform stage used during the
// write path, in reverse order. Currently the only transform is AES-GCM encryption;
// the structure is ready for additional stages (e.g. decompression in Phase 3).
// This is Stage 3 of the read pipeline.
func (r *StorageBlockReader) reverseTransforms(ctx context.Context, meta *blockMetadata, storedBytes []byte) ([]byte, error) {
	codec, err := normalizeStorageBlockCodec(meta.Codec)
	if err != nil {
		return nil, err
	}

	decodePayload := storedBytes
	if codec == blocks.CodecAESGCM {
		if len(storedBytes) <= packedStorageBlockAESGCMNonceSize {
			return nil, fmt.Errorf("stored payload too small for aes-gcm nonce prefix: size=%d", len(storedBytes))
		}
		meta.Nonce = append([]byte(nil), storedBytes[:packedStorageBlockAESGCMNonceSize]...)
		decodePayload = storedBytes[packedStorageBlockAESGCMNonceSize:]
	}

	// Get or create transformer for this codec
	r.transformerMu.RLock()
	transformer, ok := r.transformerCache[codec]
	r.transformerMu.RUnlock()
	if !ok {
		r.transformerMu.Lock()
		// Re-check after taking write lock to avoid duplicate initialization.
		transformer, ok = r.transformerCache[codec]
		if !ok {
			transformer, err = blocks.GetBlockTransformer(codec)
			if err != nil {
				r.transformerMu.Unlock()
				return nil, fmt.Errorf("get transformer for codec %s: %w", meta.Codec, err)
			}
			r.transformerCache[codec] = transformer
		}
		r.transformerMu.Unlock()
	}

	// Create descriptor for decode
	descriptor := blocks.Descriptor{
		ChunkID:       0, // N/A for block-level decode
		Codec:         codec,
		FormatVersion: meta.FormatVersion,
		PlaintextSize: meta.Metadata.Sizes.PlaintextSize,
		StoredSize:    meta.Metadata.Sizes.StoredSize,
		Nonce:         meta.Nonce,
		ContainerID:   0, // N/A for this context
		BlockOffset:   meta.ContainerOffset,
	}

	// Decode (decrypt if needed)
	plaintext, err := transformer.Decode(ctx, blocks.DecodeInput{
		Descriptor: descriptor,
		Payload:    decodePayload,
	})
	if err != nil {
		return nil, fmt.Errorf("decode block: %w", err)
	}

	// Stage 3a: Verify compressed hash at the pre-decompression boundary.
	if err := r.verifyCompressedHash(meta, plaintext, meta.Metadata.Hashes.CompressedHash); err != nil {
		return nil, err
	}

	// Stage 3b: Decompress after decrypt to restore logical block bytes.
	compressionCodec := strings.TrimSpace(strings.ToLower(meta.Metadata.Compression.Codec))
	if compressionCodec == "" {
		compressionCodec = storagecompression.CompressionNone
	}

	var compressor storagecompression.Compressor
	if compressionCodec == storagecompression.CompressionZstd {
		level := storagecompression.DefaultCompressionLevel
		if meta.Metadata.Compression.Level != nil {
			level = *meta.Metadata.Compression.Level
		}
		comp, err := storagecompression.NewZstdCompressor(level)
		if err != nil {
			return nil, fmt.Errorf("block %d: initialize compression codec=%q level=%d: %w", meta.ID, compressionCodec, level, err)
		}
		compressor = comp
	} else {
		comp, err := storagecompression.Lookup(compressionCodec)
		if err != nil {
			return nil, fmt.Errorf("block %d: resolve compression codec=%q: %w", meta.ID, compressionCodec, err)
		}
		compressor = comp
	}

	logicalBytes, err := compressor.Decompress(plaintext, meta.Metadata.Sizes.PlaintextSize)
	if err != nil {
		return nil, fmt.Errorf("block %d: decompress codec=%q: %w", meta.ID, compressionCodec, err)
	}

	if int64(len(logicalBytes)) != meta.Metadata.Sizes.PlaintextSize {
		return nil, fmt.Errorf("plaintext size mismatch: expected %d got %d", meta.Metadata.Sizes.PlaintextSize, len(logicalBytes))
	}

	return logicalBytes, nil
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
