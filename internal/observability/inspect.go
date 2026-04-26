package observability

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/franchoy/coldkeep/internal/graph"
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

func (s *Service) inspectRepository(ctx context.Context, opts InspectOptions) (*InspectResult, error) {
	if s == nil || s.db == nil {
		return nil, fmt.Errorf("observability service requires non-nil db")
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

	var snapshots int64
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM snapshot`).Scan(&snapshots); err != nil {
		return nil, fmt.Errorf("inspect repository snapshot count: %w", err)
	}

	result := &InspectResult{
		GeneratedAtUTC: s.now(),
		EntityType:     EntityRepository,
		EntityID:       "repository",
		Summary: map[string]any{
			"total_files":     logicalFiles,
			"total_chunks":    chunks,
			"total_snapshots": snapshots,
		},
	}

	if opts.Relations {
		snapshotRelations, err := s.inspectRepositorySnapshotRelations(ctx, opts.Limit)
		if err != nil {
			return nil, err
		}
		aggregateRelations := []Relation{
			{
				Type:       "aggregate",
				Direction:  RelationOutgoing,
				TargetType: EntityLogicalFile,
				TargetID:   "all",
				Metadata:   map[string]any{"count": logicalFiles},
			},
			{
				Type:       "aggregate",
				Direction:  RelationOutgoing,
				TargetType: EntityChunk,
				TargetID:   "all",
				Metadata:   map[string]any{"count": chunks},
			},
			{
				Type:       "aggregate",
				Direction:  RelationOutgoing,
				TargetType: EntitySnapshot,
				TargetID:   "all",
				Metadata:   map[string]any{"count": snapshots},
			},
			{
				Type:       "aggregate",
				Direction:  RelationOutgoing,
				TargetType: EntityContainer,
				TargetID:   "all",
				Metadata:   map[string]any{"count": containers},
			},
		}
		result.Relations = append(result.Relations, aggregateRelations...)
		result.Relations = append(result.Relations, snapshotRelations...)
	}

	return result, nil
}

func (s *Service) inspectSnapshot(ctx context.Context, id string, opts InspectOptions) (*InspectResult, error) {
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

	var totalSizeBytes int64
	if err := s.db.QueryRowContext(
		ctx,
		`SELECT COALESCE(SUM(lf.total_size), 0)
		 FROM snapshot_file sf
		 JOIN logical_file lf ON lf.id = sf.logical_file_id
		 WHERE sf.snapshot_id = ?`,
		snapshotID,
	).Scan(&totalSizeBytes); err != nil {
		return nil, fmt.Errorf("inspect snapshot %s total size: %w", snapshotID, err)
	}

	result := &InspectResult{
		GeneratedAtUTC: s.now(),
		EntityType:     EntitySnapshot,
		EntityID:       snapshotID,
		Summary: map[string]any{
			"snapshot_id":        snapshotID,
			"created_at":         createdAt,
			"logical_file_count": fileCount,
			"total_size_bytes":   totalSizeBytes,
			"type":               snapshotType,
			"label":              nullableString(label),
			"parent_id":          nullableString(parentID),
		},
	}

	if opts.Relations {
		relations, err := s.getSnapshotOutgoingRelations(ctx, snapshotID, opts.Limit)
		if err != nil {
			return nil, err
		}
		result.Relations = append(result.Relations, relations...)
	}

	return result, nil
}

func (s *Service) inspectLogicalFile(ctx context.Context, id string, opts InspectOptions) (*InspectResult, error) {
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

	if opts.Relations {
		relations, err := s.getGraphOutgoingRelations(ctx, graph.NodeID{Type: graph.EntityLogicalFile, ID: fileID}, opts.Limit, "contains", RelationOutgoing)
		if err != nil {
			return nil, fmt.Errorf("inspect logical file %d forward relations: %w", fileID, err)
		}
		result.Relations = append(result.Relations, relations...)
	}
	if opts.Reverse {
		reverse, err := s.getGraphReverseRelations(ctx, graph.NodeID{Type: graph.EntityLogicalFile, ID: fileID}, opts.Limit, "contains")
		if err != nil {
			return nil, fmt.Errorf("inspect logical file %d reverse relations: %w", fileID, err)
		}
		result.Relations = append(result.Relations, reverse...)
	}

	return result, nil
}

func (s *Service) inspectChunk(ctx context.Context, id string, opts InspectOptions) (*InspectResult, error) {
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

	result := &InspectResult{
		GeneratedAtUTC: s.now(),
		EntityType:     EntityChunk,
		EntityID:       strconv.FormatInt(chunkID, 10),
		Summary: map[string]any{
			"chunk_id":          chunkID,
			"chunk_hash":        chunkHash,
			"size_bytes":        size,
			"chunker_version":   chunkerVersion,
			"container_id":      nullableInt64(containerID),
			"stored_size_bytes": nullableInt64(storedSize),
			"status":            status,
			"live_ref_count":    liveRefCount,
			"pin_count":         pinCount,
			"retry_count":       retryCount,
		},
	}

	if opts.Relations {
		relations, err := s.getGraphOutgoingRelations(ctx, graph.NodeID{Type: graph.EntityChunk, ID: chunkID}, opts.Limit, "stored_in", RelationOutgoing)
		if err != nil {
			return nil, fmt.Errorf("inspect chunk %d forward relations: %w", chunkID, err)
		}
		result.Relations = append(result.Relations, relations...)
	}
	if opts.Reverse {
		reverse, err := s.getGraphReverseRelations(ctx, graph.NodeID{Type: graph.EntityChunk, ID: chunkID}, opts.Limit, "contains")
		if err != nil {
			return nil, fmt.Errorf("inspect chunk %d reverse relations: %w", chunkID, err)
		}
		result.Relations = append(result.Relations, reverse...)
	}

	return result, nil
}

func (s *Service) inspectContainer(ctx context.Context, id string, opts InspectOptions) (*InspectResult, error) {
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

	var chunkCount int64
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM blocks WHERE container_id = ?`, containerID).Scan(&chunkCount); err != nil {
		return nil, fmt.Errorf("inspect container %d chunk count: %w", containerID, err)
	}

	result := &InspectResult{
		GeneratedAtUTC: s.now(),
		EntityType:     EntityContainer,
		EntityID:       strconv.FormatInt(containerID, 10),
		Summary: map[string]any{
			"container_id": containerID,
			"filename":     filename,
			"size_bytes":   currentSize,
			"chunk_count":  chunkCount,
			"quarantine":   quarantine == 1,
			"sealed":       sealed == 1,
			"sealing":      sealing == 1,
			"current_size": currentSize,
			"max_size":     maxSize,
		},
	}

	if opts.Reverse {
		reverse, err := s.getGraphReverseRelations(ctx, graph.NodeID{Type: graph.EntityContainer, ID: containerID}, opts.Limit, "stored_in")
		if err != nil {
			return nil, fmt.Errorf("inspect container %d reverse relations: %w", containerID, err)
		}
		result.Relations = append(result.Relations, reverse...)
	}

	return result, nil
}

func (s *Service) inspectRepositorySnapshotRelations(ctx context.Context, limit int) ([]Relation, error) {
	rows, err := s.db.QueryContext(
		ctx,
		`SELECT id
		 FROM snapshot
		 ORDER BY created_at DESC, id DESC
		 LIMIT ?`,
		limit,
	)
	if err != nil {
		return nil, fmt.Errorf("inspect repository snapshot relations: %w", err)
	}
	defer func() { _ = rows.Close() }()

	relations := make([]Relation, 0)
	for rows.Next() {
		var snapshotID string
		if err := rows.Scan(&snapshotID); err != nil {
			return nil, fmt.Errorf("inspect repository snapshot relation row: %w", err)
		}
		relations = append(relations, Relation{
			Type:       "contains",
			Direction:  RelationOutgoing,
			TargetType: EntitySnapshot,
			TargetID:   snapshotID,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("inspect repository snapshot relations rows: %w", err)
	}

	return relations, nil
}

func (s *Service) getSnapshotOutgoingRelations(ctx context.Context, snapshotID string, limit int) ([]Relation, error) {
	graphID, err := strconv.ParseInt(snapshotID, 10, 64)
	if err == nil {
		return s.getGraphOutgoingRelations(ctx, graph.NodeID{Type: graph.EntitySnapshot, ID: graphID}, limit, "contains", RelationOutgoing)
	}

	rows, queryErr := s.db.QueryContext(
		ctx,
		`SELECT logical_file_id
		 FROM snapshot_file
		 WHERE snapshot_id = ?
		 ORDER BY id
		 LIMIT ?`,
		snapshotID,
		limit,
	)
	if queryErr != nil {
		return nil, fmt.Errorf("inspect snapshot %s relations: %w", snapshotID, queryErr)
	}
	defer func() { _ = rows.Close() }()

	out := make([]Relation, 0)
	for rows.Next() {
		var logicalFileID int64
		if scanErr := rows.Scan(&logicalFileID); scanErr != nil {
			return nil, fmt.Errorf("inspect snapshot %s relation row: %w", snapshotID, scanErr)
		}
		out = append(out, Relation{
			Type:       "contains",
			Direction:  RelationOutgoing,
			TargetType: EntityLogicalFile,
			TargetID:   strconv.FormatInt(logicalFileID, 10),
		})
	}
	if rowsErr := rows.Err(); rowsErr != nil {
		return nil, fmt.Errorf("inspect snapshot %s relations rows: %w", snapshotID, rowsErr)
	}

	return out, nil
}

func (s *Service) getGraphOutgoingRelations(ctx context.Context, node graph.NodeID, limit int, relationType string, direction RelationDirection) ([]Relation, error) {
	if s == nil || s.graph == nil {
		return nil, nil
	}
	outgoing, err := s.graph.GetOutgoing(ctx, node)
	if err != nil {
		return nil, err
	}
	return graphNodesToRelations(outgoing, limit, relationType, direction), nil
}

func (s *Service) getGraphReverseRelations(ctx context.Context, node graph.NodeID, limit int, relationType string) ([]Relation, error) {
	if s == nil || s.graph == nil {
		return nil, nil
	}
	references, err := s.graph.GetReverseReferences(ctx, node)
	if err != nil {
		return nil, err
	}
	return graphNodesToRelations(references, limit, relationType, RelationIncoming), nil
}

func graphNodesToRelations(nodes []graph.NodeID, limit int, relationType string, direction RelationDirection) []Relation {
	if limit <= 0 || len(nodes) == 0 {
		return nil
	}
	out := make([]Relation, 0, minInt(limit, len(nodes)))
	for i, node := range nodes {
		if i >= limit {
			break
		}
		targetType, ok := mapGraphEntityType(node.Type)
		if !ok {
			continue
		}
		out = append(out, Relation{
			Type:       relationType,
			Direction:  direction,
			TargetType: targetType,
			TargetID:   strconv.FormatInt(node.ID, 10),
		})
	}
	return out
}

func mapGraphEntityType(entity graph.EntityType) (EntityType, bool) {
	switch entity {
	case graph.EntitySnapshot:
		return EntitySnapshot, true
	case graph.EntityLogicalFile:
		return EntityLogicalFile, true
	case graph.EntityChunk:
		return EntityChunk, true
	case graph.EntityContainer:
		return EntityContainer, true
	default:
		return "", false
	}
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
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
