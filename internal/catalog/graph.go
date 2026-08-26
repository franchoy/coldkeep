package catalog

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"time"
)

// LoadSnapshotGraph implements SnapshotGraphCatalog for SQLite and PostgreSQL.
// It performs one ordered read and builds relationships entirely in memory.
func (s *Service) LoadSnapshotGraph(ctx context.Context) (*SnapshotGraph, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, graphCatalogError(err)
	}
	rows, err := s.db.QueryContext(ctx, `
SELECT s.id, s.type, COALESCE(s.label, ''), COALESCE(s.parent_id, ''), s.created_at,
       (SELECT COUNT(*) FROM snapshot_file sf WHERE sf.snapshot_id = s.id) AS file_count
FROM snapshot s
ORDER BY s.created_at ASC, s.id ASC`)
	if err != nil {
		return nil, graphCatalogError(fmt.Errorf("query snapshot graph: %w", err))
	}
	defer func() { _ = rows.Close() }()

	refs := make([]SnapshotRef, 0)
	for rows.Next() {
		var ref SnapshotRef
		var createdAt any
		if err := rows.Scan(&ref.ID, &ref.Type, &ref.Label, &ref.ParentID, &createdAt, &ref.FileCount); err != nil {
			return nil, graphCatalogError(fmt.Errorf("scan snapshot graph row: %w", err))
		}
		parsed, err := catalogTimestamp(createdAt)
		if err != nil {
			return nil, NewError(ErrorInvariantViolation, "load snapshot graph", "valid_snapshot_timestamp", fmt.Sprintf("snapshot %q has invalid created_at metadata", ref.ID), err)
		}
		ref.CreatedAt = parsed
		refs = append(refs, ref)
	}
	if err := rows.Err(); err != nil {
		return nil, graphCatalogError(fmt.Errorf("iterate snapshot graph rows: %w", err))
	}
	return buildSnapshotGraph(refs)
}

func buildSnapshotGraph(refs []SnapshotRef) (*SnapshotGraph, error) {
	ordered := append([]SnapshotRef(nil), refs...)
	sort.Slice(ordered, func(i, j int) bool {
		if ordered[i].CreatedAt.Equal(ordered[j].CreatedAt) {
			return ordered[i].ID < ordered[j].ID
		}
		return ordered[i].CreatedAt.Before(ordered[j].CreatedAt)
	})
	graph := &SnapshotGraph{
		Nodes:   make([]SnapshotGraphNode, len(ordered)),
		RootIDs: make([]string, 0),
	}
	index := make(map[string]int, len(ordered))
	for i, ref := range ordered {
		if ref.ID == "" {
			return nil, NewError(ErrorInvariantViolation, "load snapshot graph", "unique_nonempty_snapshot_id", "snapshot graph contains an empty ID", nil)
		}
		if _, exists := index[ref.ID]; exists {
			return nil, NewError(ErrorInvariantViolation, "load snapshot graph", "unique_nonempty_snapshot_id", fmt.Sprintf("snapshot graph contains duplicate ID %q", ref.ID), nil)
		}
		index[ref.ID] = i
		graph.Nodes[i] = SnapshotGraphNode{Snapshot: ref, ChildIDs: make([]string, 0)}
	}

	for i := range graph.Nodes {
		node := &graph.Nodes[i]
		parentID := node.Snapshot.ParentID
		if parentID == "" {
			node.ParentState = SnapshotParentNone
			graph.RootIDs = append(graph.RootIDs, node.Snapshot.ID)
			continue
		}
		parentIndex, exists := index[parentID]
		if !exists {
			node.ParentState = SnapshotParentMissing
			continue
		}
		node.ParentState = SnapshotParentPresent
		graph.Nodes[parentIndex].ChildIDs = append(graph.Nodes[parentIndex].ChildIDs, node.Snapshot.ID)
	}

	if cycleID := snapshotGraphCycle(graph.Nodes, index); cycleID != "" {
		return nil, NewError(ErrorInvariantViolation, "load snapshot graph", "acyclic_snapshot_graph", fmt.Sprintf("snapshot graph contains a parent cycle involving %q", cycleID), nil)
	}
	return graph, nil
}

func snapshotGraphCycle(nodes []SnapshotGraphNode, index map[string]int) string {
	const (
		unvisited uint8 = iota
		visiting
		visited
	)
	state := make([]uint8, len(nodes))
	var visit func(int) string
	visit = func(i int) string {
		switch state[i] {
		case visiting:
			return nodes[i].Snapshot.ID
		case visited:
			return ""
		}
		state[i] = visiting
		parentID := nodes[i].Snapshot.ParentID
		if parentIndex, exists := index[parentID]; exists {
			if cycleID := visit(parentIndex); cycleID != "" {
				return cycleID
			}
		}
		state[i] = visited
		return ""
	}
	for i := range nodes {
		if cycleID := visit(i); cycleID != "" {
			return cycleID
		}
	}
	return ""
}

func catalogTimestamp(value any) (time.Time, error) {
	switch typed := value.(type) {
	case time.Time:
		return typed.UTC(), nil
	case string:
		return parseCatalogTimestampString(typed)
	case []byte:
		return parseCatalogTimestampString(string(typed))
	default:
		return time.Time{}, fmt.Errorf("unsupported timestamp type %T", value)
	}
}

func parseCatalogTimestampString(value string) (time.Time, error) {
	for _, layout := range []string{
		time.RFC3339Nano,
		time.RFC3339,
		"2006-01-02 15:04:05.999999999Z07:00",
		"2006-01-02 15:04:05.999999999-07:00",
		"2006-01-02 15:04:05.999999999",
		"2006-01-02 15:04:05",
	} {
		if parsed, err := time.Parse(layout, value); err == nil {
			return parsed.UTC(), nil
		}
	}
	return time.Time{}, fmt.Errorf("invalid timestamp %q", value)
}

func graphCatalogError(err error) error {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return NewError(ErrorCancelled, "load snapshot graph", "", "snapshot graph load cancelled", err)
	}
	if errors.Is(err, sql.ErrNoRows) {
		return NewError(ErrorNotFound, "load snapshot graph", "", "snapshot graph row not found", err)
	}
	return NewError(ErrorOperationFailed, "load snapshot graph", "", "snapshot graph query failed", err)
}
