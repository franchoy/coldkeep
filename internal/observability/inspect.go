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
	case EntityFile, EntityLogicalFile:
		return s.inspectLogicalFile(ctx, id, opts)
	default:
		return nil, fmt.Errorf("%w: %q", ErrUnsupportedEntity, entity)
	}
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
