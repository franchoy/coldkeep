package observability

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/franchoy/coldkeep/internal/storage"
)

func (s *Service) Inspect(ctx context.Context, entity EntityType, id string, opts InspectOptions) (*InspectResult, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	opts = normalizeInspectOptions(opts)

	switch entity {
	case EntityRepository:
		return s.inspectRepository(ctx, opts)
	case EntitySnapshot:
		return s.inspectSnapshot(ctx, id, opts)
	case EntityFile, EntityLogicalFile:
		return s.inspectLogicalFile(ctx, id, opts)
	case EntityChunk:
		return s.inspectChunk(ctx, id, opts)
	case EntityContainer:
		return s.inspectContainer(ctx, id, opts)
	default:
		return nil, fmt.Errorf("%w: %q", ErrUnsupportedEntity, entity)
	}
}

func (s *Service) inspectRepository(ctx context.Context, _ InspectOptions) (*InspectResult, error) {
	if s == nil || s.db == nil {
		return nil, fmt.Errorf("observability service requires non-nil db")
	}

	activeChunker := ""
	if err := s.db.QueryRowContext(ctx, `SELECT value FROM repository_config WHERE key = 'default_chunker'`).Scan(&activeChunker); err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("inspect repository: %w", err)
		}
	}

	var logicalFiles int64
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM logical_file`).Scan(&logicalFiles); err != nil {
		return nil, fmt.Errorf("inspect repository logical_file count: %w", err)
	}

	var chunks int64
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM chunk`).Scan(&chunks); err != nil {
		return nil, fmt.Errorf("inspect repository chunk count: %w", err)
	}

	var containers int64
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM container`).Scan(&containers); err != nil {
		return nil, fmt.Errorf("inspect repository container count: %w", err)
	}

	return &InspectResult{
		GeneratedAtUTC: s.now(),
		EntityType:     EntityRepository,
		EntityID:       "repository",
		Summary: map[string]any{
			"active_write_chunker": activeChunker,
			"logical_files":        logicalFiles,
			"chunks":               chunks,
			"containers":           containers,
		},
	}, nil
}

func (s *Service) inspectSnapshot(ctx context.Context, id string, _ InspectOptions) (*InspectResult, error) {
	snapshotID := strings.TrimSpace(id)
	if snapshotID == "" {
		return nil, fmt.Errorf("%w: invalid snapshot id %q", ErrInvalidTarget, id)
	}
	if s == nil || s.db == nil {
		return nil, fmt.Errorf("observability service requires non-nil db")
	}

	var createdAt string
	var snapshotType string
	var label sql.NullString
	var parentID sql.NullString
	err := s.db.QueryRowContext(
		ctx,
		`SELECT CAST(created_at AS TEXT), type, label, parent_id FROM snapshot WHERE id = ?`,
		snapshotID,
	).Scan(&createdAt, &snapshotType, &label, &parentID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("%w: snapshot %s", ErrNotFound, snapshotID)
		}
		return nil, fmt.Errorf("inspect snapshot %s: %w", snapshotID, err)
	}

	var fileCount int64
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM snapshot_file WHERE snapshot_id = ?`, snapshotID).Scan(&fileCount); err != nil {
		return nil, fmt.Errorf("inspect snapshot %s file count: %w", snapshotID, err)
	}

	return &InspectResult{
		GeneratedAtUTC: s.now(),
		EntityType:     EntitySnapshot,
		EntityID:       snapshotID,
		Summary: map[string]any{
			"snapshot_id": snapshotID,
			"created_at":  createdAt,
			"type":        snapshotType,
			"label":       nullableString(label),
			"parent_id":   nullableString(parentID),
			"file_count":  fileCount,
		},
	}, nil
}

func (s *Service) inspectLogicalFile(ctx context.Context, id string, _ InspectOptions) (*InspectResult, error) {
	fileIDText := strings.TrimSpace(id)
	fileID, err := strconv.ParseInt(fileIDText, 10, 64)
	if err != nil || fileID <= 0 {
		return nil, fmt.Errorf("%w: invalid logical file id %q", ErrInvalidTarget, id)
	}
	if s == nil || s.db == nil {
		return nil, fmt.Errorf("observability service requires non-nil db")
	}

	info, err := storage.GetLogicalFileInspectInfoWithDBContext(ctx, s.db, fileID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("%w: logical file %d", ErrNotFound, fileID)
		}
		return nil, fmt.Errorf("inspect logical file %d: %w", fileID, err)
	}

	result := &InspectResult{
		GeneratedAtUTC: s.now(),
		EntityType:     EntityLogicalFile,
		EntityID:       strconv.FormatInt(info.FileID, 10),
		Summary: map[string]any{
			"file_id":              info.FileID,
			"original_name":        info.OriginalName,
			"status":               info.Status,
			"chunker_version":      string(info.ChunkerVersion),
			"chunk_count":          info.ChunkCount,
			"avg_chunk_size_bytes": info.AvgChunkSizeBytes,
		},
	}

	return result, nil
}

func (s *Service) inspectChunk(ctx context.Context, id string, _ InspectOptions) (*InspectResult, error) {
	chunkID, err := parsePositiveInt64(id)
	if err != nil {
		return nil, fmt.Errorf("%w: invalid chunk id %q", ErrInvalidTarget, id)
	}
	if s == nil || s.db == nil {
		return nil, fmt.Errorf("observability service requires non-nil db")
	}

	var chunkHash string
	var size int64
	var status string
	var liveRefCount int64
	var pinCount int64
	var retryCount int64
	var chunkerVersion string
	var containerID sql.NullInt64
	var storedSize sql.NullInt64
	err = s.db.QueryRowContext(
		ctx,
		`SELECT c.chunk_hash, c.size, c.status, c.live_ref_count, c.pin_count, c.retry_count, c.chunker_version, b.container_id, b.stored_size
		 FROM chunk c
		 LEFT JOIN blocks b ON b.chunk_id = c.id
		 WHERE c.id = ?`,
		chunkID,
	).Scan(&chunkHash, &size, &status, &liveRefCount, &pinCount, &retryCount, &chunkerVersion, &containerID, &storedSize)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("%w: chunk %d", ErrNotFound, chunkID)
		}
		return nil, fmt.Errorf("inspect chunk %d: %w", chunkID, err)
	}

	return &InspectResult{
		GeneratedAtUTC: s.now(),
		EntityType:     EntityChunk,
		EntityID:       strconv.FormatInt(chunkID, 10),
		Summary: map[string]any{
			"chunk_id":          chunkID,
			"chunk_hash":        chunkHash,
			"size":              size,
			"status":            status,
			"live_ref_count":    liveRefCount,
			"pin_count":         pinCount,
			"retry_count":       retryCount,
			"chunker_version":   chunkerVersion,
			"container_id":      nullableInt64(containerID),
			"stored_size_bytes": nullableInt64(storedSize),
		},
	}, nil
}

func (s *Service) inspectContainer(ctx context.Context, id string, _ InspectOptions) (*InspectResult, error) {
	containerID, err := parsePositiveInt64(id)
	if err != nil {
		return nil, fmt.Errorf("%w: invalid container id %q", ErrInvalidTarget, id)
	}
	if s == nil || s.db == nil {
		return nil, fmt.Errorf("observability service requires non-nil db")
	}

	var filename string
	var sealed int64
	var sealing int64
	var quarantine int64
	var currentSize int64
	var maxSize int64
	err = s.db.QueryRowContext(
		ctx,
		`SELECT filename, sealed, sealing, quarantine, current_size, max_size
		 FROM container
		 WHERE id = ?`,
		containerID,
	).Scan(&filename, &sealed, &sealing, &quarantine, &currentSize, &maxSize)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("%w: container %d", ErrNotFound, containerID)
		}
		return nil, fmt.Errorf("inspect container %d: %w", containerID, err)
	}

	var blockCount int64
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM blocks WHERE container_id = ?`, containerID).Scan(&blockCount); err != nil {
		return nil, fmt.Errorf("inspect container %d block count: %w", containerID, err)
	}

	return &InspectResult{
		GeneratedAtUTC: s.now(),
		EntityType:     EntityContainer,
		EntityID:       strconv.FormatInt(containerID, 10),
		Summary: map[string]any{
			"container_id": containerID,
			"filename":     filename,
			"sealed":       sealed == 1,
			"sealing":      sealing == 1,
			"quarantine":   quarantine == 1,
			"current_size": currentSize,
			"max_size":     maxSize,
			"block_count":  blockCount,
		},
	}, nil
}

func parsePositiveInt64(id string) (int64, error) {
	v := strings.TrimSpace(id)
	parsed, err := strconv.ParseInt(v, 10, 64)
	if err != nil || parsed <= 0 {
		return 0, fmt.Errorf("invalid positive int64 id %q", id)
	}
	return parsed, nil
}

func nullableString(v sql.NullString) any {
	if !v.Valid {
		return nil
	}
	return v.String
}

func nullableInt64(v sql.NullInt64) any {
	if !v.Valid {
		return nil
	}
	return v.Int64
}
