package observability

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

func (s *Service) Inspect(ctx context.Context, target InspectTarget) (InspectResult, error) {
	if err := contextErr(ctx); err != nil {
		return InspectResult{}, err
	}

	entityType := normalizeInspectEntityType(target.EntityType)
	if entityType != EntityFile {
		return InspectResult{}, fmt.Errorf("%w: %q", ErrUnsupportedInspectTarget, target.EntityType)
	}

	fileIDText := strings.TrimSpace(target.EntityID)
	fileID, err := strconv.ParseInt(fileIDText, 10, 64)
	if err != nil || fileID <= 0 {
		return InspectResult{}, fmt.Errorf("%w: invalid file id %q", ErrUnsupportedInspectTarget, target.EntityID)
	}

	storageCtx, err := s.storageLoader()
	if err != nil {
		return InspectResult{}, err
	}
	defer func() { _ = storageCtx.Close() }()

	info, err := s.inspectLogicalFileReader(storageCtx.DB, fileID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return InspectResult{}, EntityNotFoundError{EntityType: EntityFile, EntityID: fileIDText}
		}
		return InspectResult{}, err
	}

	return InspectResult{
		GeneratedAtUTC: s.now().UTC(),
		EntityType:     EntityFile,
		EntityID:       fileIDText,
		Summary: map[string]any{
			"chunker":              string(info.ChunkerVersion),
			"chunks":               info.ChunkCount,
			"avg_chunk_size_bytes": info.AvgChunkSizeBytes,
		},
		Metadata: map[string]any{
			"logical_file_id": info.FileID,
			"original_name":   info.OriginalName,
			"status":          info.Status,
		},
	}, nil
}

func (s *Service) Simulate(ctx context.Context, target SimulationTarget) (SimulationResult, error) {
	if err := contextErr(ctx); err != nil {
		return SimulationResult{}, err
	}
	return s.simulationRunner(ctx, target)
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

func normalizeInspectEntityType(entityType EntityType) EntityType {
	trimmed := EntityType(strings.TrimSpace(string(entityType)))
	switch trimmed {
	case EntityFile, EntityLogicalFile:
		return EntityFile
	default:
		return trimmed
	}
}
