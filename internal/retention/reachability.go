package retention

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/franchoy/coldkeep/internal/catalog"
	"github.com/franchoy/coldkeep/internal/graph"
)

// ProtectionOptions controls which retained snapshots participate in a
// protected-storage calculation.
type ProtectionOptions struct {
	ExcludeSnapshotIDs []string
}

// ProtectedStorageSet is the authoritative read-only classification used by
// storage accounting and GC planning. GraphReachableCompletedChunkIDs records
// the mark traversal result. ProtectedCompletedChunkIDs additionally includes
// completed chunks protected by live references or pins. A packed block is
// protected when any completed chunk slice in that block is protected.
type ProtectedStorageSet struct {
	GraphReachableCompletedChunkIDs map[int64]struct{}
	ProtectedCompletedChunkIDs      map[int64]struct{}
	ProtectedPackedBlockIDs         map[int64]struct{}
}

type protectionChunk struct {
	status       string
	size         int64
	liveRefCount int64
	pinCount     int64
}

type protectionLegacyBlock struct {
	codec         string
	formatVersion int64
	plaintextSize int64
	storedSize    int64
	nonce         []byte
	containerID   int64
	blockOffset   int64
}

type protectionStorageBlock struct {
	formatVersion   int64
	codec           string
	plaintextSize   int64
	storedSize      int64
	containerID     int64
	containerOffset int64
	refCount        int64
	referencedSize  int64
}

type protectionPackedRef struct {
	chunkID int64
	blockID int64
	offset  int64
	size    int64
}

// BuildProtectedStorageSet computes the complete storage-protection authority
// without modifying catalog state. It also validates packed placement metadata
// and fails closed when protection cannot be determined unambiguously.
func BuildProtectedStorageSet(ctx context.Context, dbconn *sql.DB, opts ProtectionOptions) (*ProtectedStorageSet, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if dbconn == nil {
		return nil, fmt.Errorf("build protected storage set: db connection is nil")
	}

	metadata, err := catalog.NewServiceFromSQL(dbconn).LoadGCPlanMetadata(ctx, catalog.GCPlanInput{
		ExcludeSnapshotIDs: opts.ExcludeSnapshotIDs,
	})
	if err != nil {
		return nil, fmt.Errorf("build protected storage set: load retention roots: %w", err)
	}
	roots := make([]graph.NodeID, 0, len(metadata.Roots))
	for _, root := range metadata.Roots {
		roots = append(roots, graph.NodeID{Type: graph.EntityLogicalFile, ID: root.LogicalFileID})
	}
	reachable, err := graph.NewService(dbconn).ReachableChunksFromRoots(ctx, roots)
	if err != nil {
		return nil, fmt.Errorf("build protected storage set: traverse retention roots: %w", err)
	}

	chunks, err := loadProtectionChunks(ctx, dbconn)
	if err != nil {
		return nil, err
	}
	result := &ProtectedStorageSet{
		GraphReachableCompletedChunkIDs: make(map[int64]struct{}),
		ProtectedCompletedChunkIDs:      make(map[int64]struct{}),
		ProtectedPackedBlockIDs:         make(map[int64]struct{}),
	}
	for chunkID, chunk := range chunks {
		if chunk.status != "COMPLETED" {
			continue
		}
		if _, ok := reachable[chunkID]; ok {
			result.GraphReachableCompletedChunkIDs[chunkID] = struct{}{}
			result.ProtectedCompletedChunkIDs[chunkID] = struct{}{}
		}
		if chunk.liveRefCount > 0 || chunk.pinCount > 0 {
			result.ProtectedCompletedChunkIDs[chunkID] = struct{}{}
		}
	}

	containers, err := loadProtectionContainerIDs(ctx, dbconn)
	if err != nil {
		return nil, err
	}
	legacyBlocks, err := loadAndValidateLegacyPlacements(ctx, dbconn, chunks, containers)
	if err != nil {
		return nil, err
	}
	packedBlocks, err := loadProtectionStorageBlocks(ctx, dbconn, containers)
	if err != nil {
		return nil, err
	}
	if err := loadAndValidatePackedRefs(ctx, dbconn, chunks, legacyBlocks, packedBlocks, result.ProtectedCompletedChunkIDs, result.ProtectedPackedBlockIDs); err != nil {
		return nil, err
	}
	for blockID, block := range packedBlocks {
		if block.refCount == 0 {
			return nil, fmt.Errorf("build protected storage set: storage block %d has no chunk references", blockID)
		}
	}

	return result, nil
}

func loadProtectionChunks(ctx context.Context, dbconn *sql.DB) (map[int64]protectionChunk, error) {
	rows, err := dbconn.QueryContext(ctx, `SELECT id, status, size, live_ref_count, pin_count FROM chunk ORDER BY id`)
	if err != nil {
		return nil, fmt.Errorf("build protected storage set: query chunks: %w", err)
	}
	defer func() { _ = rows.Close() }()

	chunks := make(map[int64]protectionChunk)
	for rows.Next() {
		var id int64
		var chunk protectionChunk
		if err := rows.Scan(&id, &chunk.status, &chunk.size, &chunk.liveRefCount, &chunk.pinCount); err != nil {
			return nil, fmt.Errorf("build protected storage set: scan chunk: %w", err)
		}
		if id <= 0 || chunk.size <= 0 || chunk.liveRefCount < 0 || chunk.pinCount < 0 {
			return nil, fmt.Errorf("build protected storage set: invalid chunk metadata for chunk %d", id)
		}
		chunks[id] = chunk
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("build protected storage set: iterate chunks: %w", err)
	}
	return chunks, nil
}

func loadProtectionContainerIDs(ctx context.Context, dbconn *sql.DB) (map[int64]struct{}, error) {
	rows, err := dbconn.QueryContext(ctx, `SELECT id FROM container ORDER BY id`)
	if err != nil {
		return nil, fmt.Errorf("build protected storage set: query containers: %w", err)
	}
	defer func() { _ = rows.Close() }()
	containers := make(map[int64]struct{})
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("build protected storage set: scan container: %w", err)
		}
		containers[id] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("build protected storage set: iterate containers: %w", err)
	}
	return containers, nil
}

func loadAndValidateLegacyPlacements(ctx context.Context, dbconn *sql.DB, chunks map[int64]protectionChunk, containers map[int64]struct{}) (map[int64]protectionLegacyBlock, error) {
	rows, err := dbconn.QueryContext(ctx, `SELECT id, chunk_id, codec, format_version, plaintext_size, stored_size, nonce, container_id, block_offset FROM blocks ORDER BY id`)
	if err != nil {
		return nil, fmt.Errorf("build protected storage set: query legacy blocks: %w", err)
	}
	defer func() { _ = rows.Close() }()
	legacyBlocks := make(map[int64]protectionLegacyBlock)
	for rows.Next() {
		var blockID, chunkID int64
		var block protectionLegacyBlock
		if err := rows.Scan(&blockID, &chunkID, &block.codec, &block.formatVersion, &block.plaintextSize, &block.storedSize, &block.nonce, &block.containerID, &block.blockOffset); err != nil {
			return nil, fmt.Errorf("build protected storage set: scan legacy block: %w", err)
		}
		if block.plaintextSize <= 0 || block.storedSize <= 0 || block.blockOffset < 0 {
			return nil, fmt.Errorf("build protected storage set: invalid legacy block %d extent", blockID)
		}
		if _, ok := chunks[chunkID]; !ok {
			return nil, fmt.Errorf("build protected storage set: legacy block %d references missing chunk %d", blockID, chunkID)
		}
		if _, ok := containers[block.containerID]; !ok {
			return nil, fmt.Errorf("build protected storage set: legacy block %d references missing container %d", blockID, block.containerID)
		}
		if _, duplicate := legacyBlocks[chunkID]; duplicate {
			return nil, fmt.Errorf("build protected storage set: chunk %d has multiple legacy placements", chunkID)
		}
		legacyBlocks[chunkID] = block
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("build protected storage set: iterate legacy blocks: %w", err)
	}
	return legacyBlocks, nil
}

func loadProtectionStorageBlocks(ctx context.Context, dbconn *sql.DB, containers map[int64]struct{}) (map[int64]*protectionStorageBlock, error) {
	rows, err := dbconn.QueryContext(ctx, `SELECT id, format_version, codec, plaintext_size, stored_size, container_id, container_offset FROM storage_blocks ORDER BY id`)
	if err != nil {
		return nil, fmt.Errorf("build protected storage set: query storage blocks: %w", err)
	}
	defer func() { _ = rows.Close() }()
	blocks := make(map[int64]*protectionStorageBlock)
	for rows.Next() {
		var id int64
		block := &protectionStorageBlock{}
		if err := rows.Scan(&id, &block.formatVersion, &block.codec, &block.plaintextSize, &block.storedSize, &block.containerID, &block.containerOffset); err != nil {
			return nil, fmt.Errorf("build protected storage set: scan storage block: %w", err)
		}
		if block.plaintextSize <= 0 || block.storedSize <= 0 || block.containerOffset < 0 {
			return nil, fmt.Errorf("build protected storage set: invalid storage block %d extent", id)
		}
		if _, ok := containers[block.containerID]; !ok {
			return nil, fmt.Errorf("build protected storage set: storage block %d references missing container %d", id, block.containerID)
		}
		blocks[id] = block
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("build protected storage set: iterate storage blocks: %w", err)
	}
	return blocks, nil
}

func loadAndValidatePackedRefs(
	ctx context.Context,
	dbconn *sql.DB,
	chunks map[int64]protectionChunk,
	legacyBlocks map[int64]protectionLegacyBlock,
	blocks map[int64]*protectionStorageBlock,
	protectedChunks map[int64]struct{},
	protectedBlocks map[int64]struct{},
) error {
	rows, err := dbconn.QueryContext(ctx, `SELECT chunk_id, block_id, offset_in_block, size_in_block FROM chunk_block_refs ORDER BY chunk_id`)
	if err != nil {
		return fmt.Errorf("build protected storage set: query packed references: %w", err)
	}
	defer func() { _ = rows.Close() }()
	seenChunks := make(map[int64]struct{})
	refs := make([]protectionPackedRef, 0)
	for rows.Next() {
		var chunkID, blockID, offset, size int64
		if err := rows.Scan(&chunkID, &blockID, &offset, &size); err != nil {
			return fmt.Errorf("build protected storage set: scan packed reference: %w", err)
		}
		chunk, ok := chunks[chunkID]
		if !ok {
			return fmt.Errorf("build protected storage set: packed reference for chunk %d references a missing chunk", chunkID)
		}
		block, ok := blocks[blockID]
		if !ok {
			return fmt.Errorf("build protected storage set: packed reference for chunk %d references missing storage block %d", chunkID, blockID)
		}
		if _, duplicate := seenChunks[chunkID]; duplicate {
			return fmt.Errorf("build protected storage set: chunk %d has multiple packed references", chunkID)
		}
		seenChunks[chunkID] = struct{}{}
		if offset < 0 || size <= 0 || offset > block.plaintextSize || size > block.plaintextSize-offset {
			return fmt.Errorf("build protected storage set: packed reference for chunk %d lies outside storage block %d plaintext extent", chunkID, blockID)
		}
		if block.referencedSize > block.plaintextSize-size {
			return fmt.Errorf("build protected storage set: storage block %d referenced sizes exceed its plaintext extent", blockID)
		}
		block.refCount++
		block.referencedSize += size
		refs = append(refs, protectionPackedRef{chunkID: chunkID, blockID: blockID, offset: offset, size: size})
		if chunk.status == "COMPLETED" {
			if _, protected := protectedChunks[chunkID]; protected {
				protectedBlocks[blockID] = struct{}{}
			}
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("build protected storage set: iterate packed references: %w", err)
	}
	for _, ref := range refs {
		legacy, dual := legacyBlocks[ref.chunkID]
		if !dual {
			continue
		}
		block := blocks[ref.blockID]
		chunk := chunks[ref.chunkID]
		if !validPackedLegacyCompanion(chunk, legacy, block, ref) {
			return fmt.Errorf("build protected storage set: chunk %d has impossible simultaneous legacy and packed placement", ref.chunkID)
		}
	}
	return nil
}

func validPackedLegacyCompanion(chunk protectionChunk, legacy protectionLegacyBlock, block *protectionStorageBlock, ref protectionPackedRef) bool {
	if legacy.formatVersion != 1 || block.formatVersion != 1 || block.referencedSize > block.plaintextSize {
		return false
	}
	if legacy.containerID != block.containerID {
		return false
	}
	payloadPrefix := block.plaintextSize - block.referencedSize
	switch legacy.codec {
	case "plain":
		if block.containerOffset > 9223372036854775807-payloadPrefix || block.containerOffset+payloadPrefix > 9223372036854775807-ref.offset {
			return false
		}
		return block.codec == "none" &&
			legacy.plaintextSize == chunk.size &&
			legacy.storedSize == chunk.size &&
			ref.size == chunk.size &&
			legacy.blockOffset == block.containerOffset+payloadPrefix+ref.offset
	case "aes-gcm":
		return block.codec == "aes-gcm" &&
			legacy.plaintextSize == chunk.size &&
			legacy.storedSize > 0 &&
			len(legacy.nonce) == 12 &&
			legacy.blockOffset == block.containerOffset
	default:
		return false
	}
}

// ListCurrentReferencedLogicalFileIDs returns logical_file IDs referenced by
// current-state physical_file mappings.
func ListCurrentReferencedLogicalFileIDs(ctx context.Context, dbconn *sql.DB) (map[int64]struct{}, error) {
	return listDistinctLogicalFileIDs(ctx, dbconn, `SELECT DISTINCT logical_file_id FROM physical_file`)
}

// ListSnapshotReferencedLogicalFileIDs returns logical_file IDs referenced by
// retained snapshot_file entries.
func ListSnapshotReferencedLogicalFileIDs(ctx context.Context, dbconn *sql.DB) (map[int64]struct{}, error) {
	ids, err := listDistinctLogicalFileIDs(ctx, dbconn, `SELECT DISTINCT logical_file_id FROM snapshot_file`)
	if err != nil {
		if isMissingSnapshotTableError(err) {
			return map[int64]struct{}{}, nil
		}
		return nil, err
	}
	return ids, nil
}

// ListAllRetainedLogicalFileIDs returns the union of current-state and
// snapshot-referenced logical_file IDs.
func ListAllRetainedLogicalFileIDs(ctx context.Context, dbconn *sql.DB) (map[int64]struct{}, error) {
	currentIDs, err := ListCurrentReferencedLogicalFileIDs(ctx, dbconn)
	if err != nil {
		return nil, err
	}

	snapshotIDs, err := ListSnapshotReferencedLogicalFileIDs(ctx, dbconn)
	if err != nil {
		return nil, err
	}

	all := make(map[int64]struct{}, len(currentIDs)+len(snapshotIDs))
	for id := range currentIDs {
		all[id] = struct{}{}
	}
	for id := range snapshotIDs {
		all[id] = struct{}{}
	}

	return all, nil
}

// CountSnapshotReferencedLogicalFiles returns the number of distinct logical
// files referenced by retained snapshot_file rows.
func CountSnapshotReferencedLogicalFiles(ctx context.Context, dbconn *sql.DB) (int64, error) {
	snapshotIDs, err := ListSnapshotReferencedLogicalFileIDs(ctx, dbconn)
	if err != nil {
		return 0, err
	}
	return int64(len(snapshotIDs)), nil
}

// CountSnapshotOnlyLogicalFiles returns the number of distinct logical files
// referenced by retained snapshot_file rows but absent from physical_file.
func CountSnapshotOnlyLogicalFiles(ctx context.Context, dbconn *sql.DB) (int64, error) {
	currentIDs, err := ListCurrentReferencedLogicalFileIDs(ctx, dbconn)
	if err != nil {
		return 0, err
	}

	snapshotIDs, err := ListSnapshotReferencedLogicalFileIDs(ctx, dbconn)
	if err != nil {
		return 0, err
	}

	var count int64
	for id := range snapshotIDs {
		if _, inCurrent := currentIDs[id]; !inCurrent {
			count++
		}
	}

	return count, nil
}

// SumSnapshotReferencedLogicalBytes returns the sum(total_size) for distinct
// logical files referenced by retained snapshot_file rows.
func SumSnapshotReferencedLogicalBytes(ctx context.Context, dbconn *sql.DB) (int64, error) {
	snapshotIDs, err := ListSnapshotReferencedLogicalFileIDs(ctx, dbconn)
	if err != nil {
		return 0, err
	}

	if len(snapshotIDs) == 0 {
		return 0, nil
	}

	return sumLogicalFileSizesByIDs(ctx, dbconn, snapshotIDs)
}

type snapshotQueryRower interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// IsLogicalFileReferencedBySnapshot reports whether any retained snapshot_file
// row references logicalFileID.
func IsLogicalFileReferencedBySnapshot(ctx context.Context, rower snapshotQueryRower, logicalFileID int64) (bool, error) {
	if rower == nil {
		return false, fmt.Errorf("query executor is nil")
	}
	if logicalFileID <= 0 {
		return false, fmt.Errorf("logical_file_id must be positive, got %d", logicalFileID)
	}
	if ctx == nil {
		ctx = context.Background()
	}

	var referenced bool
	if err := rower.QueryRowContext(
		ctx,
		`SELECT EXISTS(SELECT 1 FROM snapshot_file WHERE logical_file_id = $1)`,
		logicalFileID,
	).Scan(&referenced); err != nil {
		if isMissingSnapshotTableError(err) {
			return false, nil
		}
		return false, fmt.Errorf("query snapshot retention for logical_file_id=%d: %w", logicalFileID, err)
	}

	return referenced, nil
}

func listDistinctLogicalFileIDs(ctx context.Context, dbconn *sql.DB, query string) (map[int64]struct{}, error) {
	if dbconn == nil {
		return nil, fmt.Errorf("db connection is nil")
	}
	if ctx == nil {
		ctx = context.Background()
	}

	rows, err := dbconn.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	ids := make(map[int64]struct{})
	for rows.Next() {
		var logicalFileID int64
		if err := rows.Scan(&logicalFileID); err != nil {
			return nil, err
		}
		if logicalFileID > 0 {
			ids[logicalFileID] = struct{}{}
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return ids, nil
}

func sumLogicalFileSizesByIDs(ctx context.Context, dbconn *sql.DB, ids map[int64]struct{}) (int64, error) {
	if dbconn == nil {
		return 0, fmt.Errorf("db connection is nil")
	}
	if ctx == nil {
		ctx = context.Background()
	}

	rows, err := dbconn.QueryContext(ctx, `SELECT id, total_size FROM logical_file`)
	if err != nil {
		return 0, err
	}
	defer func() { _ = rows.Close() }()

	var total int64
	for rows.Next() {
		var logicalFileID int64
		var totalSize int64
		if err := rows.Scan(&logicalFileID, &totalSize); err != nil {
			return 0, err
		}
		if _, ok := ids[logicalFileID]; ok {
			total += totalSize
		}
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}

	return total, nil
}

// ReachabilitySummary aggregates logical-file reachability across all
// retention dimensions at a single point in time.
type ReachabilitySummary struct {
	// CurrentLogicalIDs contains logical_file IDs referenced by current-state
	// physical_file mappings.
	CurrentLogicalIDs map[int64]struct{}
	// SnapshotLogicalIDs contains logical_file IDs referenced by retained
	// snapshot_file entries.
	SnapshotLogicalIDs map[int64]struct{}
	// RetainedLogicalIDs is the union of CurrentLogicalIDs and
	// SnapshotLogicalIDs. A logical file ID present here must not be reclaimed
	// by GC.
	RetainedLogicalIDs map[int64]struct{}
}

// ComputeReachabilitySummary queries the database for all retained logical-file
// IDs and returns a populated ReachabilitySummary for use in GC and other
// retention decisions.
func ComputeReachabilitySummary(ctx context.Context, dbconn *sql.DB) (*ReachabilitySummary, error) {
	currentIDs, err := ListCurrentReferencedLogicalFileIDs(ctx, dbconn)
	if err != nil {
		return nil, fmt.Errorf("compute reachability summary: list current: %w", err)
	}

	snapshotIDs, err := ListSnapshotReferencedLogicalFileIDs(ctx, dbconn)
	if err != nil {
		return nil, fmt.Errorf("compute reachability summary: list snapshot: %w", err)
	}

	retained := make(map[int64]struct{}, len(currentIDs)+len(snapshotIDs))
	for id := range currentIDs {
		retained[id] = struct{}{}
	}
	for id := range snapshotIDs {
		retained[id] = struct{}{}
	}

	return &ReachabilitySummary{
		CurrentLogicalIDs:  currentIDs,
		SnapshotLogicalIDs: snapshotIDs,
		RetainedLogicalIDs: retained,
	}, nil
}

// RetentionClassification partitions the retained logical-file ID set into
// three mutually exclusive buckets useful for operator-facing reporting.
type RetentionClassification struct {
	// CurrentOnly contains logical_file IDs referenced by current-state
	// physical_file mappings only (not by any snapshot).
	CurrentOnly map[int64]struct{}
	// SnapshotOnly contains logical_file IDs referenced by retained
	// snapshot_file entries only (not by any current-state physical_file).
	SnapshotOnly map[int64]struct{}
	// Shared contains logical_file IDs referenced by both current-state
	// physical_file mappings and retained snapshot_file entries.
	Shared map[int64]struct{}
}

// ClassifyRetention partitions the logical-file IDs in summary into the three
// mutually exclusive buckets of RetentionClassification.
// It does not query the database; all inputs come from the pre-computed summary.
func ClassifyRetention(summary *ReachabilitySummary) *RetentionClassification {
	c := &RetentionClassification{
		CurrentOnly:  make(map[int64]struct{}),
		SnapshotOnly: make(map[int64]struct{}),
		Shared:       make(map[int64]struct{}),
	}
	for id := range summary.CurrentLogicalIDs {
		if _, inSnapshot := summary.SnapshotLogicalIDs[id]; inSnapshot {
			c.Shared[id] = struct{}{}
		} else {
			c.CurrentOnly[id] = struct{}{}
		}
	}
	for id := range summary.SnapshotLogicalIDs {
		if _, inCurrent := summary.CurrentLogicalIDs[id]; !inCurrent {
			c.SnapshotOnly[id] = struct{}{}
		}
	}
	return c
}

func isMissingSnapshotTableError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "no such table: snapshot_file") ||
		strings.Contains(msg, "relation \"snapshot_file\" does not exist")
}
