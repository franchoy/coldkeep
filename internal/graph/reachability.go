package graph

import (
	"context"
	"strconv"
)

// GCRootOptions configures root collection for GC mark traversal.
type GCRootOptions struct {
	ExcludeSnapshots []string
}

// CurrentLogicalFileRoots returns logical-file roots from the current
// repository state (physical_file table).
func (s *Service) CurrentLogicalFileRoots(ctx context.Context) ([]NodeID, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if s == nil || s.db == nil {
		return nil, nil
	}

	rows, err := s.db.QueryContext(ctx, `SELECT DISTINCT logical_file_id FROM physical_file ORDER BY logical_file_id`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	out := make([]NodeID, 0)
	for rows.Next() {
		var logicalFileID int64
		if err := rows.Scan(&logicalFileID); err != nil {
			return nil, err
		}
		out = append(out, NodeID{Type: EntityLogicalFile, ID: logicalFileID})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return out, nil
}

// SnapshotRoots returns logical-file roots retained by snapshots, excluding
// any snapshot IDs explicitly listed in excludeSnapshotIDs.
func (s *Service) SnapshotRoots(ctx context.Context, excludeSnapshotIDs []string) ([]NodeID, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if s == nil || s.db == nil {
		return nil, nil
	}

	excluded := make(map[string]struct{}, len(excludeSnapshotIDs))
	for _, id := range excludeSnapshotIDs {
		excluded[id] = struct{}{}
	}

	rows, err := s.db.QueryContext(ctx, `SELECT DISTINCT snapshot_id, logical_file_id FROM snapshot_file ORDER BY logical_file_id`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	seen := make(map[int64]struct{})
	out := make([]NodeID, 0)
	for rows.Next() {
		var snapshotID string
		var logicalFileID int64
		if err := rows.Scan(&snapshotID, &logicalFileID); err != nil {
			return nil, err
		}
		if _, skip := excluded[snapshotID]; skip {
			continue
		}
		if _, exists := seen[logicalFileID]; exists {
			continue
		}
		seen[logicalFileID] = struct{}{}
		out = append(out, NodeID{Type: EntityLogicalFile, ID: logicalFileID})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return out, nil
}

// GCRoots returns the full GC root set: current logical files plus snapshot
// logical files, with optional snapshot exclusions.
func (s *Service) GCRoots(ctx context.Context, opts GCRootOptions) ([]NodeID, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	currentRoots, err := s.CurrentLogicalFileRoots(ctx)
	if err != nil {
		return nil, err
	}
	snapshotRoots, err := s.SnapshotRoots(ctx, opts.ExcludeSnapshots)
	if err != nil {
		return nil, err
	}

	seen := make(map[int64]struct{}, len(currentRoots)+len(snapshotRoots))
	roots := make([]NodeID, 0, len(currentRoots)+len(snapshotRoots))
	for _, n := range currentRoots {
		if n.Type != EntityLogicalFile {
			continue
		}
		if _, exists := seen[n.ID]; exists {
			continue
		}
		seen[n.ID] = struct{}{}
		roots = append(roots, n)
	}
	for _, n := range snapshotRoots {
		if n.Type != EntityLogicalFile {
			continue
		}
		if _, exists := seen[n.ID]; exists {
			continue
		}
		seen[n.ID] = struct{}{}
		roots = append(roots, n)
	}

	return roots, nil
}

// ReachableChunksFromRoots traverses the graph from arbitrary roots and returns
// all reachable chunk IDs.
func (s *Service) ReachableChunksFromRoots(ctx context.Context, roots []NodeID) (map[int64]struct{}, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	reachable := make(map[int64]struct{})
	if len(roots) == 0 {
		return reachable, nil
	}

	err := s.Traverse(ctx, roots, func(n NodeID) error {
		if n.Type == EntityChunk {
			reachable[n.ID] = struct{}{}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	return reachable, nil
}

func (s *Service) GetReachableChunks(ctx context.Context, snapshotIDs []int64) (map[int64]struct{}, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if s == nil || s.db == nil {
		return nil, nil
	}

	allowed := make(map[string]struct{}, len(snapshotIDs))
	for _, id := range snapshotIDs {
		allowed[strconv.FormatInt(id, 10)] = struct{}{}
	}

	rows, err := s.db.QueryContext(ctx, `SELECT DISTINCT snapshot_id, logical_file_id FROM snapshot_file ORDER BY logical_file_id`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	seen := make(map[int64]struct{})
	start := make([]NodeID, 0)
	for rows.Next() {
		var snapshotID string
		var logicalFileID int64
		if err := rows.Scan(&snapshotID, &logicalFileID); err != nil {
			return nil, err
		}
		if _, ok := allowed[snapshotID]; !ok {
			continue
		}
		if _, exists := seen[logicalFileID]; exists {
			continue
		}
		seen[logicalFileID] = struct{}{}
		start = append(start, NodeID{Type: EntityLogicalFile, ID: logicalFileID})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return s.ReachableChunksFromRoots(ctx, start)
}
