package storage

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"path/filepath"
	"strings"
	"sync"

	"github.com/franchoy/coldkeep/internal/blocks"
	"github.com/franchoy/coldkeep/internal/container"
	"github.com/franchoy/coldkeep/internal/iodebug"
)

// StorageBlockReader implements blocks.BlockReader for reading blocks from storage.
// It handles the full lifecycle: load metadata, read container bytes, decrypt, decode.
type StorageBlockReader struct {
	db            *sql.DB
	containersDir string
	verifyHash    bool
	// transformerCache is shared by reads and guarded by transformerMu.
	// StorageBlockReader may be reused across goroutines.
	transformerMu    sync.RWMutex
	transformerCache map[blocks.Codec]blocks.Transformer
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
// Steps:
// 1. Load block metadata from storage_blocks table
// 2. Read stored bytes from container
// 3. Decrypt block (if codec is AES-GCM)
// 4. Decode block to reconstruct EncodedBlock
// 5. Verify hash (mandatory in production)
func (r *StorageBlockReader) ReadBlock(ctx context.Context, blockID int64) (*blocks.EncodedBlock, error) {
	if blockID <= 0 {
		return nil, fmt.Errorf("invalid block ID: %d", blockID)
	}

	// Step 1: Load block metadata
	metadata, err := r.loadBlockMetadata(ctx, blockID)
	if err != nil {
		return nil, fmt.Errorf("load block %d metadata: %w", blockID, err)
	}
	if r.verifyHash && len(metadata.BlockHash) == 0 {
		return nil, fmt.Errorf("block %d has empty block_hash; fail-closed hash verification requires non-empty block_hash", blockID)
	}

	// Step 2: Read stored bytes from container
	storedBytes, err := r.readBlockFromContainer(metadata)
	if err != nil {
		return nil, fmt.Errorf("read block %d from container: %w", blockID, err)
	}

	// Step 3: Decrypt block (if encrypted)
	plaintextBytes, err := r.decryptBlock(ctx, metadata, storedBytes)
	if err != nil {
		return nil, fmt.Errorf("decrypt block %d: %w", blockID, err)
	}

	// Step 4: Decode block to reconstruct EncodedBlock
	encodedBlock, err := blocks.DecodeBlock(plaintextBytes)
	if err != nil {
		return nil, fmt.Errorf("decode block %d: %w", blockID, err)
	}

	if encodedBlock == nil {
		return nil, fmt.Errorf("decode block %d returned nil", blockID)
	}

	iodebug.IncBlockDecode()

	// Step 5: Verify hash (mandatory)
	if r.verifyHash {
		if err := blocks.VerifyBlockHash(plaintextBytes, metadata.BlockHash); err != nil {
			return nil, fmt.Errorf("verify block %d hash: %w", blockID, err)
		}
	}

	return encodedBlock, nil
}

// blockMetadata represents the persistent metadata about a block.
type blockMetadata struct {
	ID              int64
	FormatVersion   int
	Codec           string
	PlaintextSize   int64
	StoredSize      int64
	ContainerName   string
	ContainerOffset int64
	BlockHash       []byte
	Nonce           []byte
}

// loadBlockMetadata queries storage_blocks and container tables to get full block metadata.
func (r *StorageBlockReader) loadBlockMetadata(ctx context.Context, blockID int64) (*blockMetadata, error) {
	if r.db == nil {
		return nil, fmt.Errorf("database connection is nil")
	}

	query := `
		SELECT 
			b.id, b.format_version, b.codec, b.plaintext_size, b.stored_size,
			b.container_offset, b.block_hash, c.filename
		FROM storage_blocks b
		JOIN container c ON b.container_id = c.id
		WHERE b.id = $1
		LIMIT 1
	`

	var meta blockMetadata
	var codecStr string

	err := r.db.QueryRowContext(ctx, query, blockID).Scan(
		&meta.ID,
		&meta.FormatVersion,
		&codecStr,
		&meta.PlaintextSize,
		&meta.StoredSize,
		&meta.ContainerOffset,
		&meta.BlockHash,
		&meta.ContainerName,
	)
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("block %d not found", blockID)
	}
	if err != nil {
		return nil, fmt.Errorf("query storage_blocks: %w", err)
	}

	meta.Codec = codecStr

	// Validate metadata
	if meta.PlaintextSize <= 0 {
		return nil, fmt.Errorf("block %d has invalid plaintext_size: %d", blockID, meta.PlaintextSize)
	}
	if meta.StoredSize <= 0 {
		return nil, fmt.Errorf("block %d has invalid stored_size: %d", blockID, meta.StoredSize)
	}
	if meta.ContainerOffset < 0 {
		return nil, fmt.Errorf("block %d has invalid container_offset: %d", blockID, meta.ContainerOffset)
	}

	return &meta, nil
}

// readBlockFromContainer reads block bytes from the container file.
func (r *StorageBlockReader) readBlockFromContainer(meta *blockMetadata) ([]byte, error) {
	containerPath := filepath.Join(r.containersDir, meta.ContainerName)

	// Open container file for reading
	// Note: We use the maximum container size here; this is just for validation purposes
	fc, err := container.OpenReadOnlyContainer(containerPath, container.GetContainerMaxSize())
	if err != nil {
		return nil, fmt.Errorf("open container %s: %w", meta.ContainerName, err)
	}
	defer func() { _ = fc.Close() }()

	// Read block bytes at offset using the Container interface
	payload, err := container.ReadPayloadAt(fc, meta.ContainerOffset, meta.StoredSize)
	if err != nil {
		return nil, fmt.Errorf("read from container at offset %d: %w", meta.ContainerOffset, err)
	}

	if int64(len(payload)) != meta.StoredSize {
		return nil, fmt.Errorf("read %d bytes but expected %d", len(payload), meta.StoredSize)
	}

	return payload, nil
}

// decryptBlock decrypts the stored bytes using the block's codec.
// For "plain" codec, returns bytes unchanged.
// For "aes-gcm", decrypts and returns plaintext.
func (r *StorageBlockReader) decryptBlock(ctx context.Context, meta *blockMetadata, storedBytes []byte) ([]byte, error) {
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
		PlaintextSize: meta.PlaintextSize,
		StoredSize:    meta.StoredSize,
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

	if int64(len(plaintext)) != meta.PlaintextSize {
		return nil, fmt.Errorf("plaintext size mismatch: expected %d got %d", meta.PlaintextSize, len(plaintext))
	}

	return plaintext, nil
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
