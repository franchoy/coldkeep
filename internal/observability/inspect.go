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

	switch entity {
	case EntityFile, EntityLogicalFile:
		return s.inspectLogicalFile(ctx, id, opts)
	default:
		return nil, fmt.Errorf("unsupported inspect entity %q", entity)
	}
}

func (s *Service) inspectLogicalFile(ctx context.Context, id string, _ InspectOptions) (*InspectResult, error) {
	fileIDText := strings.TrimSpace(id)
	fileID, err := strconv.ParseInt(fileIDText, 10, 64)
	if err != nil || fileID <= 0 {
		return nil, fmt.Errorf("invalid logical file id %q", id)
	}
	if s == nil || s.db == nil {
		return nil, fmt.Errorf("observability service requires non-nil db")
	}

	info, err := storage.GetLogicalFileInspectInfoWithDBContext(ctx, s.db, fileID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("logical file %d not found", fileID)
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

func (s *Service) Simulate(ctx context.Context, target SimulationTarget) (SimulationResult, error) {
	if err := contextErr(ctx); err != nil {
		return SimulationResult{}, err
	}
	return s.defaultSimulationRunner(ctx, target)
}

func (s *Service) defaultSimulationRunner(_ context.Context, target SimulationTarget) (SimulationResult, error) {
	if strings.TrimSpace(target.Kind) == "" {
		return SimulationResult{}, fmt.Errorf("%w: empty kind", ErrUnsupportedSimulateTarget)
	}

	return SimulationResult{
		GeneratedAtUTC: s.now().UTC(),
		Kind:           target.Kind,
		Exact:          false,
		Mutated:        false,
		Summary: map[string]any{
			"phase": "v1.6-foundation",
		},
		Warnings: []ObservationWarning{
			{
				Code:    "SIMULATION_NOT_IMPLEMENTED",
				Message: "simulation foundation is wired, but this target is not implemented in phase 1",
			},
		},
	}, nil
}
