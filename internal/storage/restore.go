package storage

import (
	"bufio"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/franchoy/coldkeep/internal/blocks"
	"github.com/franchoy/coldkeep/internal/catalog"
	"github.com/franchoy/coldkeep/internal/chunk"
	"github.com/franchoy/coldkeep/internal/container"
	"github.com/franchoy/coldkeep/internal/db"
	"github.com/franchoy/coldkeep/internal/fsx"
	"github.com/franchoy/coldkeep/internal/pathsafe"
	filestate "github.com/franchoy/coldkeep/internal/status"
)

// RestoreFileResult contains structured metadata about a restore operation.
type RestoreFileResult struct {
	FileID       int64  `json:"file_id"`
	OriginalName string `json:"original_name"`
	OutputPath   string `json:"output_path"`
	RestoredHash string `json:"restored_hash"`
}

// RestoreOptions controls restore-file behavior.
type RestoreOptions struct {
	Overwrite       bool
	DestinationMode RestoreDestinationMode
	Destination     string
	TrustedRoot     string
	StrictMetadata  bool
	NoMetadata      bool
	fs              fsx.FS
}

type RestoreDestinationMode string

const (
	RestoreDestinationOriginal RestoreDestinationMode = "original"
	RestoreDestinationPrefix   RestoreDestinationMode = "prefix"
	RestoreDestinationOverride RestoreDestinationMode = "override"
)

// RestoreDescriptor describes a current-state restore target resolved from physical_file.
// It is the stable restore input shape that v1.3 snapshot/history restore can also produce.
type RestoreDescriptor struct {
	Path               string
	LogicalFileID      int64
	Mode               sql.NullInt64
	MTime              sql.NullTime
	UID                sql.NullInt64
	GID                sql.NullInt64
	IsMetadataComplete bool
}

// ================================================================
// Phase 3 Step 5: Block Grouping Types
// ================================================================
// BlockRequest represents a physical block read with all chunks that reference it.
// Multiple chunks from the same file may reference the same block (possibly non-contiguously).
// Once a block is read and cached, all chunks referencing it can be sliced from the cached bytes.
type BlockRequest struct {
	// BlockID: identifier of the physical block to read
	BlockID int64
	// Segments: list of chunks that reside in this block
	// IMPORTANT: These are NOT in file output order; they're just grouped by block.
	// The restore loop maintains chunk_index order separately.
	Segments []*blocks.ChunkSegment
}

// blockReadPlan represents the execution strategy for reading blocks during restore.
// It maintains:
// - Which blocks to read and when (block-read order)
// - Which chunks reference each block
// - The output order for chunk writes (always chunk_index order, never block order)
type blockReadPlan struct {
	// BlockRequests: ordered list of unique blocks to read (in first-appearance order)
	BlockRequests []*BlockRequest
	// ChunkToBlock: map chunk index -> BlockID for quick lookup during output phase
	ChunkToBlock map[int]*blocks.ChunkSegment
	// BlockDedup: tracks which BlockIDs we've already added to avoid duplicates
	BlockDedup map[int64]bool
}

// BlockCache is a per-restore in-memory cache for decoded blocks.
// Eviction policy: FIFO by first insertion order.
type BlockCache struct {
	entries map[int64]*blocks.EncodedBlock
	order   []int64
	maxSize int
}

func newBlockCache(maxSize int) *BlockCache {
	if maxSize <= 0 {
		maxSize = 4
	}
	return &BlockCache{
		entries: make(map[int64]*blocks.EncodedBlock),
		order:   make([]int64, 0, maxSize),
		maxSize: maxSize,
	}
}

// Get returns a cached block and whether it exists.
func (c *BlockCache) Get(blockID int64) (*blocks.EncodedBlock, bool) {
	if c == nil {
		return nil, false
	}
	b, ok := c.entries[blockID]
	return b, ok
}

// Put inserts a block into the cache and evicts the oldest entry when full.
func (c *BlockCache) Put(blockID int64, block *blocks.EncodedBlock) {
	if c == nil || block == nil {
		return
	}

	if _, exists := c.entries[blockID]; exists {
		c.entries[blockID] = block
		return
	}

	if len(c.entries) >= c.maxSize && len(c.order) > 0 {
		evictID := c.order[0]
		c.order = c.order[1:]
		delete(c.entries, evictID)
	}

	c.entries[blockID] = block
	c.order = append(c.order, blockID)
}

type restoreChunkRow struct {
	chunkOrder          int64
	blockOffset         int64
	plaintextSize       int64
	storedSize          int64
	expectedChunkHash   string
	blockHash           []byte
	compressedHash      []byte
	physicalHash        []byte
	chunkerVersion      string
	chunkSize           int64
	blocksCodec         string
	blocksFormatVersion int
	blocksNonce         []byte
	blocksContainerID   int64
	filename            string
	chunkStatus         string
	maxSize             int64
	chunkID             int64
}

// ================================================================
// Internal semantic restore recipe types (Phase 6)
// ================================================================
// These types abstract away v1.7 schema details and provide explicit,
// ordered structure for restore execution. Benefits:
// - Explicit ordering semantics independent of DB row layout
// - Simpler to benchmark (recipe is a cohesive unit)
// - Simpler for future daemon/API execution (first-class recipe type)
// - Easier v1.8 block abstraction adaptation (decouples from schema)

// restoreChunk represents a single chunk decoded from the recipe.
type restoreChunk struct {
	// Index: sequential position in recipe (0-based chunk_order)
	Index int64
	// ID: chunk row identifier (used for logging and cleanup)
	ID int64
	// Hash: expected SHA-256 hash of chunk plaintext (verification)
	Hash string
	// PlaintextSize: expected size of decoded chunk
	PlaintextSize int64
	// StoredSize: compressed/encrypted size on disk
	StoredSize int64
	// Offset: byte offset into container file (v1.7 legacy) OR byte offset within decoded block (v1.8)
	Offset int64
	// Codec: block encoding (e.g., "aesgcm", "plain")
	Codec string
	// FormatVersion: block format version
	FormatVersion int
	// Nonce: encryption nonce (if codec uses it)
	Nonce []byte
	// ContainerID: database container row ID
	ContainerID int64
	// ContainerName: filesystem filename of container
	ContainerName string
	// ContainerMaxSize: max size of container file
	ContainerMaxSize int64
	// Status: chunk status (should be ChunkCompleted)
	Status string
	// BlockID: v1.8 block ID (0 for v1.7 legacy)
	BlockID int64
	// BlockHashes carries packed-block hash metadata when this chunk is restored
	// from storage_blocks (v1.8 path). Legacy v1.7 chunks may have empty values.
	BlockHashes blocks.BlockHashes
}

// restoreRecipe represents a complete restore plan for one logical file.
type restoreRecipe struct {
	// LogicalFileID: database logical_file row ID
	LogicalFileID int64
	// OriginalName: original filename from logical_file
	OriginalName string
	// ExpectedHash: expected SHA-256 of complete restored file
	ExpectedHash string
	// FileSize: total size of file after restoration
	FileSize int64
	// Chunks: ordered list of chunks to restore
	Chunks []restoreChunk
	// PinnedChunkIDs: chunk IDs to be unpinned after restore
	// (captured separately to prevent GC during restore)
	PinnedChunkIDs []int64
}

// buildRestoreRecipe converts database rows and metadata into a semantic recipe.
// This builder abstracts away v1.7 schema details and provides explicit structure.
func buildRestoreRecipe(logicalFileID int64, originalName, expectedHash string, fileSize int64, chunkRows []restoreChunkRow, pinnedChunkIDs []int64) restoreRecipe {
	chunks := make([]restoreChunk, len(chunkRows))
	for i, row := range chunkRows {
		chunks[i] = restoreChunk{
			Index: row.chunkOrder,
			ID:    row.chunkID,
			Hash:  row.expectedChunkHash,
			BlockHashes: blocks.BlockHashes{
				LogicalHash:    row.blockHash,
				CompressedHash: row.compressedHash,
				PhysicalHash:   row.physicalHash,
			},
			PlaintextSize:    row.plaintextSize,
			StoredSize:       row.storedSize,
			Offset:           row.blockOffset,
			Codec:            row.blocksCodec,
			FormatVersion:    row.blocksFormatVersion,
			Nonce:            row.blocksNonce,
			ContainerID:      row.blocksContainerID,
			ContainerName:    row.filename,
			ContainerMaxSize: row.maxSize,
			Status:           row.chunkStatus,
		}
	}
	return restoreRecipe{
		LogicalFileID:  logicalFileID,
		OriginalName:   originalName,
		ExpectedHash:   expectedHash,
		FileSize:       fileSize,
		Chunks:         chunks,
		PinnedChunkIDs: pinnedChunkIDs,
	}
}

// validateRestoreRecipeOrdering defensively checks that recipe chunks are
// contiguous and in order, even though the DB query uses ORDER BY.
// This prevents silent failures if DB ordering is violated or corrupted.
//
// Returns an error if:
// - Chunk Index does not equal its position in Chunks slice
// - Empty recipe with non-zero starting index
// - Any other ordering anomaly
func validateRestoreRecipeOrdering(recipe *restoreRecipe) error {
	if len(recipe.Chunks) == 0 {
		// Empty file is valid (zero chunks)
		return nil
	}

	for i, chunk := range recipe.Chunks {
		// Chunks must be zero-indexed and contiguous
		if chunk.Index != int64(i) {
			return fmt.Errorf("non-contiguous restore chunk order at position %d: got Index=%d want Index=%d (chunk_id=%d)", i, chunk.Index, int64(i), chunk.ID)
		}
	}

	// Validate consistency between recipe and pinned IDs
	if len(recipe.Chunks) != len(recipe.PinnedChunkIDs) {
		return fmt.Errorf("recipe ordering mismatch: %d chunks but %d pinned IDs", len(recipe.Chunks), len(recipe.PinnedChunkIDs))
	}

	return nil
}

// ================================================================
// restoreReaderCache: Restore-local container reader cache (Phase 6 Step 6)
// ================================================================
// Reduces repeated open/close overhead when chunks from the same container
// are accessed during restoration. Significantly improves performance when
// chunk order interleaves containers (e.g., containerA→containerB→containerA).
//
// SAFETY INVARIANTS:
// - Scoped to one restore operation only (not global, not reused)
// - Closed at end of restore (defer ensures cleanup on error)
// - No mutation of container contents (read-only)
// - Safe with GC: chunks are pinned during restore, preventing GC of container refs
//
// PERFORMANCE: Amortizes open/close cost (typically ~1-2ms per container on disk)
// across multiple chunk reads. For files with interleaved chunks, expected
// improvement: 5-15% throughput gain depending on container switching frequency.

type restoreReaderCache struct {
	// readers maps container filename -> opened *FileContainer
	readers map[string]*container.FileContainer
	// fileID for logging context
	fileID int64
}

func newRestoreReaderCache(fileID int64) *restoreReaderCache {
	return &restoreReaderCache{
		readers: make(map[string]*container.FileContainer),
		fileID:  fileID,
	}
}

// GetReader returns a cached reader for the given container, opening it if needed.
// Ownership: reader remains owned by cache and must not be closed by caller.
// The cache ensures cleanup via Close() method.
func (c *restoreReaderCache) GetReader(containerPath string, maxSize int64) (*container.FileContainer, error) {
	containerName := filepath.Base(containerPath)

	// Check cache
	if reader, ok := c.readers[containerName]; ok {
		return reader, nil
	}

	// Not cached: open new reader
	reader, err := container.OpenReadOnlyContainer(containerPath, maxSize)
	if err != nil {
		log.Printf("event=restore_cache_open_failed action=reader_open file_id=%d container=%s err=%v", c.fileID, containerName, err)
		return nil, err
	}

	// Cache it
	c.readers[containerName] = reader
	return reader, nil
}

// Close closes all cached readers and clears the cache.
// MUST be called via defer to ensure cleanup even on error.
// All close errors are aggregated and returned via errors.Join.
func (c *restoreReaderCache) Close() error {
	var errs []error
	for containerName, reader := range c.readers {
		if reader == nil {
			continue
		}
		if err := reader.Close(); err != nil {
			log.Printf("event=restore_cache_close_failed action=reader_close file_id=%d container=%s err=%v", c.fileID, containerName, err)
			errs = append(errs, err)
		}
	}
	c.readers = make(map[string]*container.FileContainer)
	return errors.Join(errs...)
}

func validateRestoreLogicalFileChunkerVersion(fileID int64, version string) error {
	trimmed := strings.TrimSpace(version)
	if trimmed == "" {
		return fmt.Errorf("logical file %d has empty chunker_version (migration failure, schema corruption, or unsupported stale repository state)", fileID)
	}
	// Restore policy: require syntactic sanity for persisted version metadata so
	// corruption/migration issues fail fast, but do not require runtime support
	// for the specific version string to replay already-persisted recipes.
	if !chunk.IsWellFormedVersion(chunk.Version(trimmed)) {
		return fmt.Errorf("logical file %d has malformed chunker_version %q (expected format like v1-simple-rolling)", fileID, trimmed)
	}

	// Restore remains recipe-driven. Unknown versions are tolerated as persisted
	// compatibility metadata as long as the value is well-formed.
	//
	// Critical invariant: restore replays persisted chunk references and bytes; it
	// does not recompute chunk boundaries with the active runtime chunker.
	if _, ok := chunk.DefaultRegistry().Get(chunk.Version(trimmed)); !ok {
		log.Printf("event=restore_metadata_warning action=unknown_chunker_version file_id=%d chunker_version=%q", fileID, trimmed)
	}

	return nil
}

// ================================================================
// Phase 3 Step 5: Block Grouping Algorithm
// ================================================================
// buildBlockReadPlan constructs a block reading strategy from resolved chunk segments.
//
// Algorithm:
// 1. Iterate chunks in file order (chunk_index)
// 2. For each chunk, get its BlockID from the resolved segment
// 3. Group chunks by BlockID (dedup: each BlockID appears once in BlockRequests)
// 4. Maintain chunk_index order separately for output
//
// Edge case: Same block appears non-contiguously
//
//	Example:
//	  chunk[0] → block_1 (offset 0, size 100)
//	  chunk[1] → block_2 (offset 0, size 200)
//	  chunk[2] → block_1 (offset 100, size 150)  ← same block again
//
// Solution: Use block cache
//   - Read block_1 once (first encounter at chunk[0])
//   - Cache decoded block bytes
//   - When chunk[2] encounters block_1, reuse cached bytes
//   - No re-read needed, output order respected
//
// Returns:
//   - BlockReadPlan with ordered blocks and segment mappings
//   - Error if chunk resolution failed
func buildBlockReadPlan(chunkSegments []*blocks.ChunkSegment) *blockReadPlan {
	plan := &blockReadPlan{
		BlockRequests: make([]*BlockRequest, 0),
		ChunkToBlock:  make(map[int]*blocks.ChunkSegment),
		BlockDedup:    make(map[int64]bool),
	}

	// Iterate chunks in file order
	for chunkIdx, seg := range chunkSegments {
		if seg == nil {
			continue // unresolved chunk
		}

		// Record this chunk's segment for quick lookup
		plan.ChunkToBlock[chunkIdx] = seg

		// Check if we've already added this block to the read list
		if plan.BlockDedup[seg.BlockID] {
			continue // already scheduled to read this block
		}

		// First time seeing this block: add it to read plan
		blockReq := &BlockRequest{
			BlockID:  seg.BlockID,
			Segments: make([]*blocks.ChunkSegment, 0),
		}

		// Collect all segments for this block (may include future chunks too)
		// This allows us to identify all chunks in a single block upfront
		for _, s := range chunkSegments {
			if s != nil && s.BlockID == seg.BlockID {
				blockReq.Segments = append(blockReq.Segments, s)
			}
		}

		plan.BlockRequests = append(plan.BlockRequests, blockReq)
		plan.BlockDedup[seg.BlockID] = true
	}

	return plan
}

func pinLogicalFileRestoreChunks(dbconn *sql.DB, fileID int64) (string, string, []restoreChunkRow, []int64, error) {
	ctx, cancel := db.NewOperationContext(context.Background())
	defer cancel()
	return pinLogicalFileRestoreChunksWithContext(ctx, dbconn, fileID)
}

func pinLogicalFileRestoreChunksWithContext(ctx context.Context, dbconn *sql.DB, fileID int64) (string, string, []restoreChunkRow, []int64, error) {
	tx, err := dbconn.BeginTx(ctx, nil)
	if err != nil {
		return "", "", nil, nil, err
	}
	defer func() {
		if tx != nil {
			_ = tx.Rollback()
		}
	}()

	plan, err := catalog.NewService(tx).LoadRestorePlanMetadata(ctx, catalog.RestorePlanInput{
		Selector: catalog.RestoreByFileID,
		FileID:   fileID,
	})
	if err != nil {
		if catalog.IsCode(err, catalog.ErrorNotFound) || catalog.IsCode(err, catalog.ErrorConflict) {
			return "", "", nil, nil, fmt.Errorf("logical file id %d not found", fileID)
		}
		var catalogErr *catalog.Error
		if errors.As(err, &catalogErr) {
			if strings.Contains(catalogErr.Message, "empty chunker_version") {
				return "", "", nil, nil, errors.New(catalogErr.Message)
			}
			switch catalogErr.Invariant {
			case "nonempty_file_has_recipe", "exactly_one_valid_placement_per_chunk":
				return "", "", nil, nil, fmt.Errorf("no restorable chunks found for file %d (all referenced containers missing or quarantined)", fileID)
			case "contiguous_chunk_order":
				return "", "", nil, nil, fmt.Errorf("invalid restore recipe ordering: non-contiguous restore chunk order: %s", catalogErr.Message)
			}
		}
		return "", "", nil, nil, fmt.Errorf("load catalog restore plan for logical file %d: %w", fileID, err)
	}
	if err := validateRestoreLogicalFileChunkerVersion(fileID, plan.LogicalFile.ChunkerVersion); err != nil {
		return "", "", nil, nil, err
	}
	chunkRows, pinnedChunkIDs, err := restoreRowsFromCatalogPlan(plan)
	if err != nil {
		return "", "", nil, nil, err
	}
	for _, chunkID := range pinnedChunkIDs {
		result, execErr := tx.ExecContext(ctx, `UPDATE chunk SET pin_count = pin_count + 1 WHERE id = $1`, chunkID)
		if execErr != nil {
			return "", "", nil, nil, fmt.Errorf("pin chunk %d for restore: %w", chunkID, execErr)
		}
		rowsAffected, rowsErr := result.RowsAffected()
		if rowsErr != nil {
			return "", "", nil, nil, fmt.Errorf("rows affected when pinning chunk %d: %w", chunkID, rowsErr)
		}
		if rowsAffected != 1 {
			return "", "", nil, nil, fmt.Errorf("chunk %d disappeared while pinning restore", chunkID)
		}
	}
	if err := tx.Commit(); err != nil {
		return "", "", nil, nil, err
	}
	tx = nil
	return plan.LogicalFile.OriginalName, plan.LogicalFile.FileHash, chunkRows, pinnedChunkIDs, nil
}

func restoreRowsFromCatalogPlan(plan *catalog.RestorePlanMetadata) ([]restoreChunkRow, []int64, error) {
	if plan == nil {
		return nil, nil, errors.New("catalog restore plan is nil")
	}
	rows := make([]restoreChunkRow, 0, len(plan.Placements))
	pinned := make([]int64, 0, len(plan.Placements))
	for _, placement := range plan.Placements {
		trimmedVersion := strings.TrimSpace(placement.ChunkerVersion)
		if trimmedVersion == "" {
			return nil, nil, fmt.Errorf("chunk %d has empty chunker_version (repository corruption or incomplete migration)", placement.ChunkID)
		}
		if !chunk.IsWellFormedVersion(chunk.Version(trimmedVersion)) {
			return nil, nil, fmt.Errorf("chunk %d has malformed chunker_version %q (expected format like v1-simple-rolling)", placement.ChunkID, trimmedVersion)
		}
		row := restoreChunkRow{chunkOrder: placement.ChunkOrder, expectedChunkHash: placement.ChunkHash, chunkerVersion: placement.ChunkerVersion, chunkSize: placement.ChunkSize, chunkStatus: placement.ChunkStatus, chunkID: placement.ChunkID}
		switch placement.Kind {
		case catalog.PlacementLegacy:
			legacy := placement.Legacy
			row.blockOffset, row.plaintextSize, row.storedSize = legacy.ContainerOffset, legacy.PlaintextSize, legacy.StoredSize
			row.blocksCodec, row.blocksFormatVersion, row.blocksNonce = legacy.Codec, legacy.FormatVersion, append([]byte(nil), legacy.Nonce...)
			row.blocksContainerID, row.filename, row.maxSize = legacy.Container.ID, legacy.Container.Filename, legacy.Container.MaxSize
		case catalog.PlacementPacked:
			packed := placement.Packed
			row.blockOffset, row.plaintextSize, row.storedSize = packed.ContainerOffset, placement.ChunkSize, packed.StoredSize
			row.blockHash, row.compressedHash, row.physicalHash = append([]byte(nil), packed.BlockHash...), append([]byte(nil), packed.CompressedHash...), append([]byte(nil), packed.PhysicalHash...)
			row.blocksCodec, row.blocksFormatVersion = packed.Codec, packed.FormatVersion
			row.blocksContainerID, row.filename, row.maxSize = packed.Container.ID, packed.Container.Filename, packed.Container.MaxSize
		default:
			return nil, nil, fmt.Errorf("chunk %d has unsupported catalog placement %q", placement.ChunkID, placement.Kind)
		}
		rows = append(rows, row)
		pinned = append(pinned, placement.ChunkID)
	}
	return rows, pinned, nil
}

func unpinRestoreChunks(dbconn *sql.DB, chunkIDs []int64) error {
	ctx, cancel := db.NewOperationContext(context.Background())
	defer cancel()
	return unpinRestoreChunksWithContext(ctx, dbconn, chunkIDs)
}

func unpinRestoreChunksWithContext(ctx context.Context, dbconn *sql.DB, chunkIDs []int64) error {
	if len(chunkIDs) == 0 {
		return nil
	}

	tx, err := dbconn.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		if tx != nil {
			_ = tx.Rollback()
		}
	}()

	// ================================================================
	// STAGE 7: Unpin chunks (allow GC after restore)
	// ================================================================
	// Action: DECREMENT pin_count for each chunk
	// Guarantee: ✓ Called in defer even if restore fails
	// Guarantee: ✓ Chunks become eligible for GC after unpin
	// Note: Current implementation: one UPDATE per chunk
	// Optimization target: batch into single SQL statement
	//
	for _, chunkID := range chunkIDs {
		result, execErr := tx.ExecContext(
			ctx,
			`UPDATE chunk SET pin_count = pin_count - 1 WHERE id = $1 AND pin_count > 0`,
			chunkID,
		)
		if execErr != nil {
			return fmt.Errorf("unpin chunk %d after restore: %w", chunkID, execErr)
		}
		rowsAffected, rowsErr := result.RowsAffected()
		if rowsErr != nil {
			return fmt.Errorf("rows affected when unpinning chunk %d: %w", chunkID, rowsErr)
		}
		if rowsAffected != 1 {
			return fmt.Errorf("invalid pin_count transition while unpinning chunk %d", chunkID)
		}
	}

	if err := tx.Commit(); err != nil {
		return err
	}
	tx = nil

	return nil
}

func RestoreFile(id int64, outputPath string) error {
	dbconn, err := db.ConnectDB()
	if err != nil {
		return fmt.Errorf("failed to connect to DB: %w", err)
	}
	defer func() { _ = dbconn.Close() }()

	if _, err := RestoreFileWithDBResult(dbconn, id, outputPath); err != nil {
		return err
	}
	return nil
}

func RestoreFileWithDB(dbconn *sql.DB, fileID int64, outputPath string) error {
	_, err := RestoreFileWithDBResult(dbconn, fileID, outputPath)
	return err
}

func RestoreFileWithDBResult(dbconn *sql.DB, fileID int64, outputPath string) (RestoreFileResult, error) {
	return restoreFileWithDBAndDir(dbconn, fileID, outputPath, container.ContainersDir, RestoreOptions{Overwrite: true})
}

func RestoreFileWithStorageContext(sgctx StorageContext, fileID int64, outputPath string) error {
	_, err := RestoreFileWithStorageContextResult(sgctx, fileID, outputPath)
	return err
}

func RestoreFileWithStorageContextResult(sgctx StorageContext, fileID int64, outputPath string) (RestoreFileResult, error) {
	return RestoreFileWithStorageContextResultOptions(sgctx, fileID, outputPath, RestoreOptions{Overwrite: true})
}

func RestoreFileWithStorageContextResultOptions(sgctx StorageContext, fileID int64, outputPath string, opts RestoreOptions) (RestoreFileResult, error) {
	return restoreFileWithDBAndDir(sgctx.DB, fileID, outputPath, sgctx.EffectiveContainerDir(), opts)
}

func buildRestoreDescriptorFromPhysicalPath(ctx context.Context, dbconn *sql.DB, storedPaths []string, notFoundPath string) (RestoreDescriptor, error) {
	if len(storedPaths) == 0 {
		return RestoreDescriptor{}, fmt.Errorf("physical file path cannot be empty")
	}

	var descriptor RestoreDescriptor

	for _, storedPath := range storedPaths {
		err := scanRestoreDescriptorByPhysicalPath(ctx, dbconn, storedPath, &descriptor)
		if err == nil {
			return descriptor, nil
		}
		if err != sql.ErrNoRows {
			return RestoreDescriptor{}, fmt.Errorf("resolve restore descriptor for path %q: %w", storedPath, err)
		}
	}

	descriptor, err := buildRestoreDescriptorFromCanonicalIdentity(ctx, dbconn, storedPaths[0])
	if err == nil {
		return descriptor, nil
	}
	if err != sql.ErrNoRows {
		return RestoreDescriptor{}, err
	}

	if strings.TrimSpace(notFoundPath) == "" {
		notFoundPath = storedPaths[0]
	}
	return RestoreDescriptor{}, fmt.Errorf("physical file path %q not found", notFoundPath)
}

func scanRestoreDescriptorByPhysicalPath(ctx context.Context, dbconn *sql.DB, storedPath string, descriptor *RestoreDescriptor) error {
	return dbconn.QueryRowContext(
		ctx,
		`SELECT
			pf.path,
			pf.logical_file_id,
			pf.mode,
			pf.mtime,
			pf.uid,
			pf.gid,
			pf.is_metadata_complete
		FROM physical_file pf
		JOIN logical_file lf ON lf.id = pf.logical_file_id
		WHERE pf.path = $1 AND lf.status = $2`,
		storedPath,
		filestate.LogicalFileCompleted,
	).Scan(
		&descriptor.Path,
		&descriptor.LogicalFileID,
		&descriptor.Mode,
		&descriptor.MTime,
		&descriptor.UID,
		&descriptor.GID,
		&descriptor.IsMetadataComplete,
	)
}

func buildRestoreDescriptorFromCanonicalIdentity(ctx context.Context, dbconn *sql.DB, normalizedStoredPath string) (RestoreDescriptor, error) {
	rows, err := dbconn.QueryContext(
		ctx,
		`SELECT
			pf.path,
			pf.logical_file_id,
			pf.mode,
			pf.mtime,
			pf.uid,
			pf.gid,
			pf.is_metadata_complete
		FROM physical_file pf
		JOIN logical_file lf ON lf.id = pf.logical_file_id
		WHERE lf.status = $1`,
		filestate.LogicalFileCompleted,
	)
	if err != nil {
		return RestoreDescriptor{}, fmt.Errorf("resolve restore descriptor by canonical identity: %w", err)
	}
	defer func() {
		_ = rows.Close()
	}()

	for rows.Next() {
		var descriptor RestoreDescriptor
		if err := rows.Scan(
			&descriptor.Path,
			&descriptor.LogicalFileID,
			&descriptor.Mode,
			&descriptor.MTime,
			&descriptor.UID,
			&descriptor.GID,
			&descriptor.IsMetadataComplete,
		); err != nil {
			return RestoreDescriptor{}, fmt.Errorf("scan canonical restore descriptor candidate: %w", err)
		}

		candidatePath, err := normalizeRestorePhysicalPathIdentity(descriptor.Path)
		if err != nil {
			continue
		}
		if candidatePath == normalizedStoredPath {
			return descriptor, nil
		}
	}
	if err := rows.Err(); err != nil {
		return RestoreDescriptor{}, fmt.Errorf("iterate canonical restore descriptor candidates: %w", err)
	}
	return RestoreDescriptor{}, sql.ErrNoRows
}

func restoreDescriptorLookupPaths(storedPath string) ([]string, string, error) {
	requestedPath := strings.TrimSpace(storedPath)
	normalizedPath, err := normalizeRestorePhysicalPathIdentity(storedPath)
	if err != nil {
		return nil, "", err
	}

	lookupPaths := []string{normalizedPath}
	absPath, err := filepath.Abs(requestedPath)
	if err != nil {
		return nil, "", fmt.Errorf("resolve absolute physical file path: %w", err)
	}
	lexicalPath := filepath.Clean(absPath)
	if lexicalPath != normalizedPath {
		lookupPaths = append(lookupPaths, lexicalPath)
	}
	return lookupPaths, requestedPath, nil
}

func normalizeRestorePhysicalPathIdentity(path string) (string, error) {
	trimmed := strings.TrimSpace(path)
	if trimmed == "" {
		return "", errors.New("physical file path cannot be empty")
	}

	absPath, err := filepath.Abs(trimmed)
	if err != nil {
		return "", fmt.Errorf("resolve absolute physical file path: %w", err)
	}
	return canonicalizeRestorePhysicalPath(filepath.Clean(absPath))
}

func canonicalizeRestorePhysicalPath(cleaned string) (string, error) {
	resolved, err := filepath.EvalSymlinks(cleaned)
	if err == nil {
		return cleanResolvedRestorePhysicalPath(resolved)
	}
	if !errors.Is(err, fs.ErrNotExist) {
		return "", fmt.Errorf("canonicalize physical file path: %w", err)
	}
	return canonicalizeMissingRestorePhysicalPath(cleaned)
}

func cleanResolvedRestorePhysicalPath(resolved string) (string, error) {
	resolvedAbs, err := filepath.Abs(resolved)
	if err != nil {
		return "", fmt.Errorf("resolve absolute canonical physical file path: %w", err)
	}
	return filepath.Clean(resolvedAbs), nil
}

func canonicalizeMissingRestorePhysicalPath(cleaned string) (string, error) {
	ancestor, err := pathsafe.NearestExistingAncestorDir(cleaned)
	if err != nil {
		return cleaned, nil
	}
	resolvedAncestor, err := filepath.EvalSymlinks(ancestor)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return cleaned, nil
		}
		return "", fmt.Errorf("canonicalize physical file ancestor: %w", err)
	}
	resolvedAncestorAbs, err := cleanResolvedRestorePhysicalPath(resolvedAncestor)
	if err != nil {
		return "", err
	}
	relativeSuffix, err := filepath.Rel(ancestor, cleaned)
	if err != nil {
		return "", fmt.Errorf("derive physical file relative suffix: %w", err)
	}
	return filepath.Clean(filepath.Join(resolvedAncestorAbs, relativeSuffix)), nil
}

func deriveRestorePrefixRelativePath(storedPath string) (string, error) {
	trimmed := strings.TrimSpace(storedPath)
	if trimmed == "" || containsRestorePrefixTraversal(trimmed) {
		return "", invalidRestorePrefixPathError(storedPath)
	}

	relativePath, err := projectRestorePrefixRelativePath(trimmed)
	if err != nil {
		return "", invalidRestorePrefixPathError(storedPath)
	}
	return relativePath, nil
}

func projectRestorePrefixRelativePath(path string) (string, error) {
	switch {
	case isCanonicalWindowsUNCPath(path):
		return deriveCanonicalWindowsUNCRelativePath(path)
	case isWindowsDriveAbsolutePath(path):
		return deriveWindowsDriveRelativePath(path)
	case runtime.GOOS != "windows" && filepath.IsAbs(path):
		return deriveNativeAbsoluteRestorePrefixPath(path)
	default:
		return "", fmt.Errorf("unsupported restore prefix path")
	}
}

func deriveNativeAbsoluteRestorePrefixPath(path string) (string, error) {
	relativePath, err := filepath.Rel(string(filepath.Separator), filepath.Clean(path))
	if err != nil {
		return "", err
	}
	if err := validateRestorePrefixRelativePath(relativePath); err != nil {
		return "", err
	}
	return relativePath, nil
}

func invalidRestorePrefixPathError(storedPath string) error {
	return fmt.Errorf("cannot derive relative path from stored path %q", storedPath)
}

func containsRestorePrefixTraversal(path string) bool {
	for _, segment := range strings.FieldsFunc(path, func(r rune) bool {
		return r == '/' || r == '\\'
	}) {
		if segment == ".." {
			return true
		}
	}
	return false
}

func isWindowsDriveLikePath(path string) bool {
	return len(path) >= 2 && isWindowsDriveLetter(path[0]) && path[1] == ':'
}

func isWindowsDriveAbsolutePath(path string) bool {
	if !isWindowsDriveLikePath(path) || len(path) < 3 {
		return false
	}
	if strings.Contains(path, "/") {
		return false
	}
	return path[2] == '\\'
}

func isCanonicalWindowsUNCPath(path string) bool {
	return strings.HasPrefix(path, `\\`) && !strings.Contains(path, "/")
}

func deriveWindowsDriveRelativePath(path string) (string, error) {
	if !isWindowsDriveAbsolutePath(path) {
		return "", fmt.Errorf("unsupported drive path")
	}
	relativePath := strings.TrimPrefix(path[2:], `\`)
	if err := validateRestorePrefixRelativePath(relativePath); err != nil {
		return "", err
	}
	return relativePath, nil
}

func deriveCanonicalWindowsUNCRelativePath(path string) (string, error) {
	parts := strings.Split(strings.TrimPrefix(path, `\\`), `\`)
	if len(parts) < 3 || parts[0] == "" || parts[1] == "" {
		return "", fmt.Errorf("malformed UNC path")
	}
	relativePath := strings.Join(parts[2:], `\`)
	if err := validateRestorePrefixRelativePath(relativePath); err != nil {
		return "", err
	}
	return relativePath, nil
}

func isWindowsDriveLetter(c byte) bool {
	return (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z')
}

func validateRestorePrefixRelativePath(relativePath string) error {
	return pathsafe.ValidateStoredRelativePath(relativePath)
}

// RestoreFileByStoredPathWithStorageContextResultOptions restores a file using the
// current-state physical_file path as identity (v1.2 model).
// This is original destination mode: output path is the stored physical path.
func RestoreFileByStoredPathWithStorageContextResultOptions(sgctx StorageContext, storedPath string, opts RestoreOptions) (RestoreFileResult, error) {
	if sgctx.DB == nil {
		return RestoreFileResult{}, fmt.Errorf("db connection is nil")
	}

	lookupPaths, notFoundPath, err := restoreDescriptorLookupPaths(storedPath)
	if err != nil {
		return RestoreFileResult{}, err
	}

	ctx, cancel := db.NewOperationContext(context.Background())
	defer cancel()

	descriptor, err := buildRestoreDescriptorFromPhysicalPath(ctx, sgctx.DB, lookupPaths, notFoundPath)
	if err != nil {
		return RestoreFileResult{}, err
	}

	return restoreFromDescriptorWithStorageContextResultOptions(sgctx, descriptor, opts)
}

func restoreFromDescriptorWithStorageContextResultOptions(sgctx StorageContext, descriptor RestoreDescriptor, opts RestoreOptions) (RestoreFileResult, error) {
	if sgctx.DB == nil {
		return RestoreFileResult{}, fmt.Errorf("db connection is nil")
	}
	if strings.TrimSpace(descriptor.Path) == "" {
		return RestoreFileResult{}, fmt.Errorf("restore descriptor path cannot be empty")
	}
	if descriptor.LogicalFileID <= 0 {
		return RestoreFileResult{}, fmt.Errorf("restore descriptor logical file id must be positive")
	}

	if opts.StrictMetadata && !opts.NoMetadata && !descriptor.IsMetadataComplete {
		return RestoreFileResult{}, fmt.Errorf("metadata is incomplete for %q (use --no-metadata to bypass)", descriptor.Path)
	}

	resolvedOutputPath, resolvedTrustedRoot, err := resolveRestoreOutputPath(descriptor, opts)
	if err != nil {
		return RestoreFileResult{}, err
	}

	opts.TrustedRoot = resolvedTrustedRoot
	result, err := restoreFileWithDBAndDir(sgctx.DB, descriptor.LogicalFileID, resolvedOutputPath, sgctx.EffectiveContainerDir(), opts)
	if err != nil {
		return RestoreFileResult{}, err
	}

	// ================================================================
	// STAGE 8: Apply physical metadata (optional)
	// ================================================================
	// - Set file mode, mtime, uid, gid if present and not skipped
	// - May fail but does not invalidate restored content
	//
	if err := applyPhysicalMetadata(result.OutputPath, descriptor, opts); err != nil {
		return RestoreFileResult{}, err
	}

	return result, nil
}

func RestoreFileByStoredPathWithStorageContextResult(sgctx StorageContext, storedPath string) (RestoreFileResult, error) {
	return RestoreFileByStoredPathWithStorageContextResultOptions(sgctx, storedPath, RestoreOptions{Overwrite: true})
}

func RestoreFileByStoredPathWithStorageContext(sgctx StorageContext, storedPath string) error {
	_, err := RestoreFileByStoredPathWithStorageContextResult(sgctx, storedPath)
	return err
}

func validateRestoreWritePath(path string, trustedRoot string) error {
	absPath, err := filepath.Abs(path)
	if err != nil {
		return fmt.Errorf("resolve restore write path: %w", err)
	}

	root := strings.TrimSpace(trustedRoot)
	if root == "" {
		root, err = pathsafe.NearestExistingAncestorDir(absPath)
		if err != nil {
			return fmt.Errorf("derive restore trusted root: %w", err)
		}
	} else {
		root, err = pathsafe.ValidateTrustedRootPath(root)
		if err != nil {
			return fmt.Errorf("validate restore trusted root: %w", err)
		}
	}
	if err := pathsafe.ValidateWritePathUnderTrustedRoot(root, absPath); err != nil {
		return fmt.Errorf("restore write path contains unsafe symlink component: %w", err)
	}
	return nil
}

// syncRestoredFileDir fsyncs the output directory to make the preceding rename
// durable on filesystems that require it. On Windows, directory sync is not
// supported and the call is skipped without error.
func syncRestoredFileDir(fsys fsx.FS, outputPath string) error {
	if runtime.GOOS == "windows" {
		return nil
	}
	dir, err := fsys.Open(filepath.Dir(outputPath))
	if err != nil {
		return fmt.Errorf("open output directory for fsync: %w", err)
	}
	if err := dir.Sync(); err != nil {
		_ = dir.Close()
		return fmt.Errorf("fsync output directory: %w", err)
	}
	return dir.Close()
}

func resolveRestoreOutputPath(descriptor RestoreDescriptor, opts RestoreOptions) (string, string, error) {
	mode := opts.DestinationMode
	if mode == "" {
		mode = RestoreDestinationOriginal
	}

	switch mode {
	case RestoreDestinationOriginal:
		return resolveOriginalRestoreOutputPath(descriptor, opts)
	case RestoreDestinationPrefix:
		return resolvePrefixRestoreOutputPath(descriptor, opts)
	case RestoreDestinationOverride:
		return resolveOverrideRestoreOutputPath(opts)
	default:
		return "", "", fmt.Errorf("unsupported restore destination mode: %s", mode)
	}
}

func resolveOriginalRestoreOutputPath(descriptor RestoreDescriptor, opts RestoreOptions) (string, string, error) {
	trustedRoot := strings.TrimSpace(opts.TrustedRoot)
	if trustedRoot == "" {
		var err error
		trustedRoot, err = pathsafe.NearestExistingAncestorDir(descriptor.Path)
		if err != nil {
			return "", "", fmt.Errorf("resolve restore original trusted root: %w", err)
		}
	}
	if err := validateRestoreWritePath(descriptor.Path, trustedRoot); err != nil {
		return "", "", fmt.Errorf("resolve restore original destination: %w", err)
	}
	return descriptor.Path, trustedRoot, nil
}

func resolvePrefixRestoreOutputPath(descriptor RestoreDescriptor, opts RestoreOptions) (string, string, error) {
	prefix := strings.TrimSpace(opts.Destination)
	if prefix == "" {
		return "", "", fmt.Errorf("restore prefix destination is required for mode %q", RestoreDestinationPrefix)
	}
	trustedRoot, err := pathsafe.ValidateTrustedRootPath(prefix)
	if err != nil {
		return "", "", fmt.Errorf("resolve restore prefix destination: %w", err)
	}

	relativePath, err := deriveRestorePrefixRelativePath(descriptor.Path)
	if err != nil {
		return "", "", err
	}
	joinedPath, err := pathsafe.SafeJoin(trustedRoot, relativePath)
	if err != nil {
		return "", "", fmt.Errorf("resolve restore prefix destination: %w", err)
	}
	if err := validateRestoreWritePath(joinedPath, trustedRoot); err != nil {
		return "", "", fmt.Errorf("resolve restore prefix destination: %w", err)
	}
	return joinedPath, trustedRoot, nil
}

func resolveOverrideRestoreOutputPath(opts RestoreOptions) (string, string, error) {
	overridePath := strings.TrimSpace(opts.Destination)
	if overridePath == "" {
		return "", "", fmt.Errorf("restore override destination is required for mode %q", RestoreDestinationOverride)
	}
	absOverridePath, err := filepath.Abs(overridePath)
	if err != nil {
		return "", "", fmt.Errorf("resolve restore override destination: %w", err)
	}
	trustedRoot := strings.TrimSpace(opts.TrustedRoot)
	if trustedRoot == "" {
		trustedRoot, err = pathsafe.NearestExistingAncestorDir(absOverridePath)
		if err != nil {
			return "", "", fmt.Errorf("resolve restore override trusted root: %w", err)
		}
	}
	if err := validateRestoreWritePath(absOverridePath, trustedRoot); err != nil {
		return "", "", fmt.Errorf("resolve restore override destination: %w", err)
	}
	return filepath.Clean(absOverridePath), trustedRoot, nil
}

func restoreFileWithDBAndDir(dbconn *sql.DB, fileID int64, outputPath string, containersDir string, opts RestoreOptions) (result RestoreFileResult, err error) {
	result.FileID = fileID
	fsys := opts.fs
	if fsys == nil {
		fsys = fsx.Default()
	}
	ctx, cancel := db.NewOperationContext(context.Background())
	defer cancel()

	// ================================================================
	// STAGE 1-4: Resolve target, pin chunks, and load metadata/recipe
	// ================================================================
	// - Resolves restore target (fileID)
	// - Pins all chunks BEFORE any read to protect from GC
	// - Loads: logical_file metadata (name, expected hash)
	// - Loads: ordered chunk recipe (container, offsets, hashes, codec)
	// * CRITICAL: Pin count incremented during this phase
	// * CRITICAL: Defer unpins at end to ensure cleanup even on error
	//
	originalName, expectedFileHash, chunkRows, pinnedChunkIDs, err := pinLogicalFileRestoreChunksWithContext(ctx, dbconn, fileID)
	if err != nil {
		return RestoreFileResult{}, err
	}
	defer func() {
		// STAGE 7: Unpin chunks (allow GC after restore completes or fails)
		cleanupCtx, cleanupCancel := db.NewOperationContext(context.Background())
		defer cleanupCancel()
		if unpinErr := unpinRestoreChunksWithContext(cleanupCtx, dbconn, pinnedChunkIDs); unpinErr != nil {
			log.Printf("event=restore_cleanup action=unpin_chunks file_id=%d error=%v", fileID, unpinErr)
			if err == nil {
				err = unpinErr
			}
		}
	}()

	// Build semantic restore recipe from database rows
	recipe := buildRestoreRecipe(fileID, originalName, expectedFileHash, int64(0), chunkRows, pinnedChunkIDs)
	result.OriginalName = recipe.OriginalName

	// Defensively validate recipe ordering before proceeding to restore
	// This catches any DB ordering issues or data corruption early
	if err := validateRestoreRecipeOrdering(&recipe); err != nil {
		return RestoreFileResult{}, fmt.Errorf("invalid restore recipe ordering: %w", err)
	}

	// ================================================================
	// STAGE 5a: Prepare output file and harness
	// ================================================================
	if st, err := fsys.Stat(outputPath); err == nil && st.IsDir() {
		outputPath = filepath.Join(outputPath, originalName)
	} else if strings.HasSuffix(outputPath, string(os.PathSeparator)) {
		// if user passed a non-existing dir with trailing slash
		if err := fsys.MkdirAll(outputPath, 0755); err != nil {
			return RestoreFileResult{}, fmt.Errorf("create output directory: %w", err)
		}
		outputPath = filepath.Join(outputPath, originalName)
	}
	if err := validateRestoreWritePath(outputPath, opts.TrustedRoot); err != nil {
		return RestoreFileResult{}, fmt.Errorf("validate output path %s: %w", outputPath, err)
	}
	result.OutputPath = outputPath
	if !opts.Overwrite {
		if _, statErr := fsys.Stat(outputPath); statErr == nil {
			return RestoreFileResult{}, fmt.Errorf("output file already exists: %s (use --overwrite)", outputPath)
		} else if !os.IsNotExist(statErr) {
			return RestoreFileResult{}, fmt.Errorf("check output path %s: %w", outputPath, statErr)
		}
	}

	// Create parent directories if they don't exist
	if err := fsys.MkdirAll(filepath.Dir(outputPath), 0755); err != nil {
		return RestoreFileResult{}, fmt.Errorf("create parent directories for %s: %w", outputPath, err)
	}

	outFile, err := os.CreateTemp(filepath.Dir(outputPath), ".coldkeep-restore-*")
	if err != nil {
		return RestoreFileResult{}, fmt.Errorf("create temporary output file for %s: %w", outputPath, err)
	}
	tempOutputPath := outFile.Name()
	cleanupTemp := true
	defer func() {
		if outFile != nil {
			_ = outFile.Close()
		}
		if cleanupTemp {
			if shouldCleanupRestoreTempPath(tempOutputPath, outputPath) {
				_ = fsys.Remove(tempOutputPath)
			} else {
				log.Printf("event=restore_temp_cleanup_skip action=path_not_owned file_id=%d temp_path=%q output_path=%q", fileID, tempOutputPath, outputPath)
			}
		}
	}()

	// Phase 6 Step 7: Buffered output writer (1MB buffer)
	// Reduces syscall overhead for small chunk writes
	// Flush() is called before Sync() to preserve durability
	bufw := bufio.NewWriterSize(outFile, 1<<20)
	defer func() {
		if bufw != nil {
			if flushErr := bufw.Flush(); flushErr != nil {
				log.Printf("event=restore_buffered_writer_error action=flush_failed file_id=%d err=%v", fileID, flushErr)
			}
		}
	}()

	hasher := sha256.New()

	// Phase 6 Step 6: Initialize restore-local reader cache
	readerCache := newRestoreReaderCache(fileID)
	defer func() {
		if err := readerCache.Close(); err != nil {
			log.Printf("event=restore_reader_cache_error action=cache_cleanup file_id=%d err=%v", fileID, err)
		}
	}()

	// Cache transformers by codec to avoid repeated allocations
	transformerCache := make(map[blocks.Codec]blocks.Transformer)

	// ================================================================
	// STAGE 5b: v1.8 Block-based restore flow
	// ================================================================
	// NEW MODEL: Group chunks by block_id and read blocks once
	// CRITICAL: Preserve output order by chunk_index
	//
	// 1. Load file_chunk ordered
	// 2. Resolve each chunk → segment (determines block_id)
	// 3. Group by block_id
	// 4. For each block:
	//    - read block once
	//    - slice needed chunks
	//    - emit in correct order
	//
	// IMPORTANT: Output MUST follow chunk_index order even if grouped by block
	// Never reorder output by block
	//
	// Initialize RestoreService for v1.8 block-based reads
	restoreService := &RestoreService{
		ChunkResolver: NewDualCompatChunkResolver(dbconn),
		BlockReader:   NewStorageBlockReader(dbconn, containersDir),
	}

	// Phase 3 Step 5: Resolve all chunks and build block read plan
	chunkSegments := make([]*blocks.ChunkSegment, len(recipe.Chunks))

	// Resolve all chunks to their block segments
	for i, chunk := range recipe.Chunks {
		// Resolve chunk location (determines BlockID for v1.8)
		seg, err := restoreService.ResolveChunkLocation(ctx, chunk.ID)
		if err != nil {
			log.Printf("event=restore_chunk_resolution_failed action=resolve chunk_id=%d file_id=%d err=%v", chunk.ID, fileID, err)
			continue
		}
		if seg == nil {
			log.Printf("event=restore_chunk_not_found action=resolve chunk_id=%d file_id=%d", chunk.ID, fileID)
			continue
		}

		chunkSegments[i] = seg
	}

	// Build block read plan: groups blocks by ID while maintaining chunk_index order
	// This plan identifies which blocks need to be read and in what order.
	// Non-contiguous block references are handled via block cache.
	blockPlan := buildBlockReadPlan(chunkSegments)

	// Per-restore block cache (recommended default: maxSize=4).
	blockCache := newBlockCache(4)

	// Read blocks and extract chunks, emitting in original chunk index order
	var expectedOrder int64 = 0
	validChunks := 0
	var firstRestoreError error
	const emptyFileSHA256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
	isExpectedEmptyFile := len(recipe.Chunks) == 0 && recipe.ExpectedHash == emptyFileSHA256

	// Process chunks in original order to preserve output sequence
	for chunkIdx, chunk := range recipe.Chunks {
		if err := ctx.Err(); err != nil {
			return RestoreFileResult{}, err
		}

		if chunk.Status != filestate.ChunkCompleted {
			continue // skip incomplete chunks (should not happen)
		}

		// Get chunk segment (resolved block placement)
		seg := blockPlan.ChunkToBlock[chunkIdx]
		if seg == nil {
			log.Printf("event=restore_skip_chunk action=segment_not_resolved file_id=%d chunk_id=%d", fileID, chunk.ID)
			continue
		}

		// Validate monotonically contiguous chunk sequence
		if chunk.Index != expectedOrder {
			log.Printf("event=restore_skip_chunk action=order_discontinuity file_id=%d chunk_order=%d expected=%d", fileID, chunk.Index, expectedOrder)
			continue
		}
		expectedOrder++

		// v1.8 block-based path: Read block once and slice chunk from it
		if seg.BlockID > 0 {
			cachedBlock, cachedOK := blockCache.Get(seg.BlockID)
			if cachedOK {
				log.Printf("event=restore_block_cache action=hit file_id=%d chunk_id=%d block_id=%d", fileID, chunk.ID, seg.BlockID)
			}
			if !cachedOK {
				// Block not yet cached: read it now
				log.Printf("event=restore_block_read action=start file_id=%d chunk_id=%d block_id=%d", fileID, chunk.ID, seg.BlockID)
				block, err := restoreService.BlockReader.ReadBlock(ctx, seg.BlockID)
				if err != nil {
					log.Printf("event=restore_skip_chunk action=block_read_failed file_id=%d chunk_id=%d block_id=%d err=%v", fileID, chunk.ID, seg.BlockID, err)
					if firstRestoreError == nil {
						firstRestoreError = err
					}
					continue
				}
				log.Printf("event=restore_block_read action=success file_id=%d chunk_id=%d block_id=%d", fileID, chunk.ID, seg.BlockID)

				if block == nil || block.Payload == nil {
					log.Printf("event=restore_skip_chunk action=block_payload_nil file_id=%d chunk_id=%d block_id=%d", fileID, chunk.ID, seg.BlockID)
					continue
				}

				// Insert newly read block into cache.
				blockCache.Put(seg.BlockID, block)
				cachedBlock = block

				// TEST HOOK: assert state just before first block read.
				if TestRestoreBeforeChunkReadHook != nil {
					if hookErr := TestRestoreBeforeChunkReadHook(dbconn, chunk.ID); hookErr != nil {
						return RestoreFileResult{}, fmt.Errorf("test hook before chunk read: %w", hookErr)
					}
				}
			}

			// Step 7: Integrate chunk slicing from decoded block payload.
			log.Printf("event=restore_block_slice action=start file_id=%d chunk_id=%d block_id=%d offset=%d size=%d", fileID, chunk.ID, seg.BlockID, seg.Offset, seg.Size)
			plaintext, err := blocks.SliceChunkFromPayload(cachedBlock.Payload, blocks.ChunkEntry{
				Offset: uint64(seg.Offset),
				Size:   uint64(seg.Size),
			})
			if err != nil {
				log.Printf("event=restore_skip_chunk action=segment_out_of_bounds file_id=%d chunk_id=%d block_id=%d offset=%d size=%d block_size=%d err=%v", fileID, chunk.ID, seg.BlockID, seg.Offset, seg.Size, len(cachedBlock.Payload), err)
				if firstRestoreError == nil {
					firstRestoreError = err
				}
				continue
			}

			// Validate plaintext size
			if int64(len(plaintext)) != chunk.PlaintextSize {
				sizeErr := fmt.Errorf("plaintext size mismatch for chunk %d: expected %d got %d", chunk.ID, chunk.PlaintextSize, len(plaintext))
				log.Printf("event=restore_skip_chunk action=plaintext_size_mismatch file_id=%d chunk_id=%d expected=%d got=%d", fileID, chunk.ID, chunk.PlaintextSize, len(plaintext))
				if firstRestoreError == nil {
					firstRestoreError = sizeErr
				}
				continue
			}

			// Validate hashes
			sum := sha256.Sum256(plaintext)
			gotHash := hex.EncodeToString(sum[:])
			if gotHash != chunk.Hash {
				hashErr := fmt.Errorf("restored chunk hash mismatch: expected %s got %s", chunk.Hash, gotHash)
				log.Printf("event=restore_skip_chunk action=hash_mismatch file_id=%d chunk_id=%d expected=%s got=%s", fileID, chunk.ID, chunk.Hash, gotHash)
				if firstRestoreError == nil {
					firstRestoreError = hashErr
				}
				continue
			}

			// Write to buffered output
			if _, err := bufw.Write(plaintext); err != nil {
				log.Printf("event=restore_skip_chunk action=write_failed file_id=%d chunk_id=%d err=%v", fileID, chunk.ID, err)
				continue
			}

			// Update file hash
			if _, err := hasher.Write(plaintext); err != nil {
				log.Printf("event=restore_skip_chunk action=hash_failed file_id=%d chunk_id=%d err=%v", fileID, chunk.ID, err)
				continue
			}
			validChunks++

		} else {
			// v1.7 legacy path (BlockID == 0)
			// If the container is missing (quarantined), skip this chunk but continue restoring others
			if chunk.ContainerName == "" {
				log.Printf("event=restore_skip_chunk action=missing_container file_id=%d chunk_id=%d", fileID, chunk.ID)
				continue
			}

			// Phase 6 Step 6: Get container reader from cache (reduces open/close overhead)
			containerPath, err := container.SafeContainerPath(containersDir, chunk.ContainerName)
			if err != nil {
				return RestoreFileResult{}, fmt.Errorf("invalid container filename %q: %w", chunk.ContainerName, err)
			}
			filecontainer, err := readerCache.GetReader(containerPath, chunk.ContainerMaxSize)
			if err != nil {
				log.Printf("event=restore_skip_chunk action=container_read_failed file_id=%d chunk_id=%d container=%s err=%v", fileID, chunk.ID, chunk.ContainerName, err)
				continue
			}

			// TEST HOOK: assert state just before first payload read.
			if TestRestoreBeforeChunkReadHook != nil {
				if hookErr := TestRestoreBeforeChunkReadHook(dbconn, chunk.ID); hookErr != nil {
					return RestoreFileResult{}, fmt.Errorf("test hook before chunk read: %w", hookErr)
				}
			}

			// Read block payload
			payload, err := container.ReadPayloadAt(filecontainer, chunk.Offset, chunk.StoredSize)
			if err != nil {
				log.Printf("event=restore_skip_chunk action=read_payload_failed file_id=%d chunk_id=%d container=%s err=%v", fileID, chunk.ID, chunk.ContainerName, err)
				continue
			}

			// Use cached transformer to avoid repeated allocations
			codec := blocks.Codec(chunk.Codec)
			transformer, ok := transformerCache[codec]
			if !ok {
				var err error
				transformer, err = blocks.GetBlockTransformer(codec)
				if err != nil {
					log.Printf("event=restore_skip_chunk action=transformer_failed file_id=%d chunk_id=%d codec=%s err=%v", fileID, chunk.ID, chunk.Codec, err)
					if firstRestoreError == nil {
						firstRestoreError = err
					}
					continue
				}
				transformerCache[codec] = transformer
			}

			plaintext, err := transformer.Decode(ctx, blocks.DecodeInput{
				ChunkHash: chunk.Hash,
				Descriptor: blocks.Descriptor{
					ChunkID:       chunk.ID,
					Codec:         codec,
					FormatVersion: chunk.FormatVersion,
					PlaintextSize: chunk.PlaintextSize,
					StoredSize:    chunk.StoredSize,
					Nonce:         chunk.Nonce,
					ContainerID:   chunk.ContainerID,
					BlockOffset:   chunk.Offset,
				},
				Payload: payload,
			})
			if err != nil {
				log.Printf("event=restore_skip_chunk action=decode_failed file_id=%d chunk_id=%d codec=%s err=%v", fileID, chunk.ID, chunk.Codec, err)
				if firstRestoreError == nil {
					firstRestoreError = err
				}
				continue
			}

			// Validate plaintext size
			if int64(len(plaintext)) != chunk.PlaintextSize {
				sizeErr := fmt.Errorf("plaintext size mismatch for chunk %d: expected %d got %d", chunk.ID, chunk.PlaintextSize, len(plaintext))
				log.Printf("event=restore_skip_chunk action=plaintext_size_mismatch file_id=%d chunk_id=%d expected=%d got=%d", fileID, chunk.ID, chunk.PlaintextSize, len(plaintext))
				if firstRestoreError == nil {
					firstRestoreError = sizeErr
				}
				continue
			}

			// Validate hashes (DB hash and on-disk record hash)
			sum := sha256.Sum256(plaintext)
			gotHash := hex.EncodeToString(sum[:])
			if gotHash != chunk.Hash {
				hashErr := fmt.Errorf("restored chunk hash mismatch: expected %s got %s", chunk.Hash, gotHash)
				log.Printf("event=restore_skip_chunk action=hash_mismatch file_id=%d chunk_id=%d expected=%s got=%s", fileID, chunk.ID, chunk.Hash, gotHash)
				if firstRestoreError == nil {
					firstRestoreError = hashErr
				}
				continue
			}

			// Write to buffered output
			if _, err := bufw.Write(plaintext); err != nil {
				log.Printf("event=restore_skip_chunk action=write_failed file_id=%d chunk_id=%d err=%v", fileID, chunk.ID, err)
				continue
			}

			// Update file hash
			if _, err := hasher.Write(plaintext); err != nil {
				log.Printf("event=restore_skip_chunk action=hash_failed file_id=%d chunk_id=%d err=%v", fileID, chunk.ID, err)
				continue
			}
			validChunks++
		}
	}

	if validChunks == 0 {
		if isExpectedEmptyFile {
			// Valid completed empty file: restore emits an empty output file.
			result.RestoredHash = emptyFileSHA256
		} else if firstRestoreError != nil {
			return RestoreFileResult{}, firstRestoreError
		} else {
			return RestoreFileResult{}, fmt.Errorf("no restorable chunks found for file %d (all referenced containers missing or quarantined)", fileID)
		}
	}

	// Compute the final hash before flush/fsync/close/rename
	restoredHash := hex.EncodeToString(hasher.Sum(nil))
	result.RestoredHash = restoredHash
	if restoredHash != recipe.ExpectedHash {
		log.Printf("event=restore_partial_warning file_id=%d expected_hash=%s restored_hash=%s", fileID, recipe.ExpectedHash, restoredHash)
		return RestoreFileResult{}, fmt.Errorf("restored file hash mismatch: expected %s got %s", recipe.ExpectedHash, restoredHash)
	}

	// ================================================================
	// STAGE 6: Finalize and commit to destination
	// ================================================================
	// - Flush: buffered writer to underlying file
	// - Fsync: temporary output file to ensure durability
	// - Rename: atomic replace of temporary file with target path
	// - Fsync: directory metadata to ensure rename is durable
	// * CRITICAL: These operations preserve durability on crash
	// * CRITICAL: Final hash check before rename ensures early corruption detection
	//
	// Flush buffered writer before fsync
	if err := bufw.Flush(); err != nil {
		return RestoreFileResult{}, fmt.Errorf("flush buffered writer: %w", err)
	}
	bufw = nil // prevent deferred flush from writing to already-closed outFile

	// Fsync ensures data is written to disk before returning
	if err := outFile.Sync(); err != nil {
		return RestoreFileResult{}, fmt.Errorf("fsync output file: %w", err)
	}

	// Close temp file before rename
	if err := outFile.Close(); err != nil {
		return RestoreFileResult{}, fmt.Errorf("close temporary output file: %w", err)
	}
	outFile = nil

	// TEST HOOK: simulate failure after temp file is written but before rename
	if TestRestoreFailBeforeRenameHook != nil {
		if hookErr := TestRestoreFailBeforeRenameHook(tempOutputPath, outputPath); hookErr != nil {
			return RestoreFileResult{}, fmt.Errorf("test hook restore failure: %w", hookErr)
		}
	}
	if !opts.Overwrite {
		if _, statErr := fsys.Stat(outputPath); statErr == nil {
			return RestoreFileResult{}, fmt.Errorf("output file already exists: %s (use --overwrite)", outputPath)
		} else if !os.IsNotExist(statErr) {
			return RestoreFileResult{}, fmt.Errorf("check output path %s before rename: %w", outputPath, statErr)
		}
	}

	if err := fsys.Rename(tempOutputPath, outputPath); err != nil {
		return RestoreFileResult{}, fmt.Errorf("atomically replace output file %s: %w", outputPath, err)
	}
	// Flush directory metadata so the rename is durable across crashes on stricter filesystems.
	// Skipped on Windows where directory sync is not supported.
	if err := syncRestoredFileDir(fsys, outputPath); err != nil {
		return RestoreFileResult{}, err
	}
	cleanupTemp = false

	// Set result hash
	result.RestoredHash = restoredHash
	return result, nil
}

func shouldCleanupRestoreTempPath(tempOutputPath, outputPath string) bool {
	if tempOutputPath == "" || outputPath == "" {
		return false
	}
	tempDir := filepath.Clean(filepath.Dir(tempOutputPath))
	outputDir := filepath.Clean(filepath.Dir(outputPath))
	if tempDir != outputDir {
		return false
	}
	return strings.HasPrefix(filepath.Base(tempOutputPath), ".coldkeep-restore-")
}

func applyPhysicalMetadata(outputPath string, descriptor RestoreDescriptor, opts RestoreOptions) error {
	if opts.NoMetadata {
		return nil
	}

	metadataErrs := make([]string, 0)

	if !descriptor.IsMetadataComplete {
		msg := fmt.Sprintf("restore metadata incomplete for %q (mode=%t mtime=%t uid=%t gid=%t)",
			descriptor.Path,
			descriptor.Mode.Valid,
			descriptor.MTime.Valid,
			descriptor.UID.Valid,
			descriptor.GID.Valid,
		)
		if opts.StrictMetadata {
			return errors.New(msg)
		}
		log.Printf("event=restore_metadata_warning path=%q reason=incomplete_metadata details=%q", outputPath, msg)
	}

	if descriptor.Mode.Valid {
		if err := os.Chmod(outputPath, os.FileMode(descriptor.Mode.Int64)); err != nil {
			metadataErrs = append(metadataErrs, fmt.Sprintf("chmod: %v", err))
		}
	}

	if descriptor.MTime.Valid {
		mtime := descriptor.MTime.Time
		if err := os.Chtimes(outputPath, mtime, mtime); err != nil {
			metadataErrs = append(metadataErrs, fmt.Sprintf("chtimes: %v", err))
		}
	}

	if descriptor.UID.Valid && descriptor.GID.Valid {
		if err := os.Chown(outputPath, int(descriptor.UID.Int64), int(descriptor.GID.Int64)); err != nil {
			metadataErrs = append(metadataErrs, fmt.Sprintf("chown: %v", err))
		}
	}

	if len(metadataErrs) == 0 {
		return nil
	}

	metadataErr := fmt.Errorf("apply restored metadata for %q: %s", outputPath, strings.Join(metadataErrs, "; "))
	if opts.StrictMetadata {
		return metadataErr
	}
	log.Printf("event=restore_metadata_warning path=%q reason=apply_failed error=%q", outputPath, metadataErr.Error())
	return nil
}

// testRestoreFailBeforeRenameHook is a test-only hook for simulating restore failures after temp file is written but before rename.
// It should only be set in tests.
var TestRestoreFailBeforeRenameHook func(tempOutputPath, outputPath string) error

// TestRestoreBeforeChunkReadHook is a test-only hook invoked immediately before
// reading chunk payload bytes from a container. It should only be set in tests.
var TestRestoreBeforeChunkReadHook func(dbconn *sql.DB, chunkID int64) error
