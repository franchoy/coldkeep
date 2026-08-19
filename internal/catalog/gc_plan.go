package catalog

import (
	"context"
	"errors"
	"fmt"
	"sort"
)

// LoadGCPlanMetadata implements GCPlanCatalog.
//
// Phase 9 implements and adopts deterministic reachability metadata.
func (s *Service) LoadGCPlanMetadata(ctx context.Context, input GCPlanInput) (*GCPlanMetadata, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, gcCatalogError(err)
	}
	normalized, err := NormalizeGCPlanInput(input)
	if err != nil {
		return nil, err
	}
	excluded := make(map[string]struct{}, len(normalized.ExcludeSnapshotIDs))
	for _, id := range normalized.ExcludeSnapshotIDs {
		var exists bool
		if err := s.db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM snapshot WHERE id = $1)`, id).Scan(&exists); err != nil {
			return nil, gcCatalogError(fmt.Errorf("validate excluded snapshot %q: %w", id, err))
		}
		if !exists {
			return nil, NewError(ErrorNotFound, "load GC plan", "excluded_snapshot_exists", fmt.Sprintf("snapshot %q does not exist", id), nil)
		}
		excluded[id] = struct{}{}
	}

	snapshots, err := s.ListSnapshots(ctx, SnapshotFilter{})
	if err != nil {
		return nil, gcCatalogError(fmt.Errorf("load protected snapshots: %w", err))
	}
	protected := make([]SnapshotRef, 0, len(snapshots))
	for _, snapshot := range snapshots {
		if _, skip := excluded[snapshot.ID]; !skip {
			protected = append(protected, snapshot)
		}
	}
	sort.Slice(protected, func(i, j int) bool {
		if !protected[i].CreatedAt.Equal(protected[j].CreatedAt) {
			return protected[i].CreatedAt.Before(protected[j].CreatedAt)
		}
		return protected[i].ID < protected[j].ID
	})
	snapshotRank := make(map[string]int, len(protected))
	for i, snapshot := range protected {
		snapshotRank[snapshot.ID] = i
	}

	roots := make(map[int64]*GCReachabilityRoot)
	currentRows, err := s.db.QueryContext(ctx, `SELECT DISTINCT logical_file_id FROM physical_file ORDER BY logical_file_id`)
	if err != nil {
		return nil, gcCatalogError(fmt.Errorf("load current GC roots: %w", err))
	}
	for currentRows.Next() {
		var id int64
		if err := currentRows.Scan(&id); err != nil {
			_ = currentRows.Close()
			return nil, gcCatalogError(fmt.Errorf("scan current GC root: %w", err))
		}
		root := ensureGCRoot(roots, id)
		root.Current = true
	}
	if err := currentRows.Err(); err != nil {
		_ = currentRows.Close()
		return nil, gcCatalogError(fmt.Errorf("iterate current GC roots: %w", err))
	}
	if err := currentRows.Close(); err != nil {
		return nil, gcCatalogError(fmt.Errorf("close current GC roots: %w", err))
	}

	snapshotRows, err := s.db.QueryContext(ctx, `SELECT DISTINCT snapshot_id, logical_file_id FROM snapshot_file ORDER BY logical_file_id, snapshot_id`)
	if err != nil {
		return nil, gcCatalogError(fmt.Errorf("load snapshot GC roots: %w", err))
	}
	for snapshotRows.Next() {
		var snapshotID string
		var logicalID int64
		if err := snapshotRows.Scan(&snapshotID, &logicalID); err != nil {
			_ = snapshotRows.Close()
			return nil, gcCatalogError(fmt.Errorf("scan snapshot GC root: %w", err))
		}
		if _, skip := excluded[snapshotID]; skip {
			continue
		}
		if _, known := snapshotRank[snapshotID]; !known {
			_ = snapshotRows.Close()
			return nil, NewError(ErrorInvariantViolation, "load GC plan", "snapshot_root_has_snapshot", fmt.Sprintf("snapshot root %q has no protected snapshot metadata", snapshotID), nil)
		}
		root := ensureGCRoot(roots, logicalID)
		root.SnapshotIDs = append(root.SnapshotIDs, snapshotID)
	}
	if err := snapshotRows.Err(); err != nil {
		_ = snapshotRows.Close()
		return nil, gcCatalogError(fmt.Errorf("iterate snapshot GC roots: %w", err))
	}
	if err := snapshotRows.Close(); err != nil {
		return nil, gcCatalogError(fmt.Errorf("close snapshot GC roots: %w", err))
	}

	logicalIDs := make([]int64, 0, len(roots))
	for id := range roots {
		logicalIDs = append(logicalIDs, id)
	}
	sort.Slice(logicalIDs, func(i, j int) bool { return logicalIDs[i] < logicalIDs[j] })
	orderedRoots := make([]GCReachabilityRoot, 0, len(logicalIDs))
	for _, id := range logicalIDs {
		root := roots[id]
		sort.SliceStable(root.SnapshotIDs, func(i, j int) bool {
			return snapshotRank[root.SnapshotIDs[i]] < snapshotRank[root.SnapshotIDs[j]]
		})
		orderedRoots = append(orderedRoots, *root)
	}
	return &GCPlanMetadata{Roots: orderedRoots, ProtectedSnapshots: protected}, nil
}

func ensureGCRoot(roots map[int64]*GCReachabilityRoot, id int64) *GCReachabilityRoot {
	if root := roots[id]; root != nil {
		return root
	}
	root := &GCReachabilityRoot{LogicalFileID: id, SnapshotIDs: make([]string, 0)}
	roots[id] = root
	return root
}

func gcCatalogError(err error) error {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return NewError(ErrorCancelled, "load GC plan", "", "GC plan load cancelled", err)
	}
	return NewError(ErrorOperationFailed, "load GC plan", "", "GC plan query failed", err)
}
