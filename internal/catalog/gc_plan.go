package catalog

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"time"
)

const gcPlanMetadataQuery = `
SELECT
    CAST(0 AS BIGINT) AS row_kind,
    CAST(s.id AS TEXT) AS snapshot_id,
    CAST(NULL AS BIGINT) AS logical_file_id,
    CAST(s.type AS TEXT) AS snapshot_type,
    CAST(COALESCE(s.label, '') AS TEXT) AS snapshot_label,
    CAST(COALESCE(s.parent_id, '') AS TEXT) AS snapshot_parent_id,
    CAST(s.created_at AS TEXT) AS snapshot_created_at
FROM snapshot AS s
UNION ALL
SELECT
    CAST(1 AS BIGINT) AS row_kind,
    CAST(sf.snapshot_id AS TEXT) AS snapshot_id,
    CAST(sf.logical_file_id AS BIGINT) AS logical_file_id,
    CAST('' AS TEXT) AS snapshot_type,
    CAST('' AS TEXT) AS snapshot_label,
    CAST('' AS TEXT) AS snapshot_parent_id,
    CAST('' AS TEXT) AS snapshot_created_at
FROM (
    SELECT DISTINCT snapshot_id, logical_file_id
    FROM snapshot_file
) AS sf
UNION ALL
SELECT
    CAST(2 AS BIGINT) AS row_kind,
    CAST('' AS TEXT) AS snapshot_id,
    CAST(pf.logical_file_id AS BIGINT) AS logical_file_id,
    CAST('' AS TEXT) AS snapshot_type,
    CAST('' AS TEXT) AS snapshot_label,
    CAST('' AS TEXT) AS snapshot_parent_id,
    CAST('' AS TEXT) AS snapshot_created_at
FROM (
    SELECT DISTINCT logical_file_id
    FROM physical_file
) AS pf`

const (
	gcPlanSnapshotMetadataRow int64 = iota
	gcPlanSnapshotRootRow
	gcPlanCurrentRootRow
)

// gcPlanScanBarrier is an internal deterministic-concurrency test seam. The
// production path always passes nil, and no database detail crosses the public
// catalog contract.
type gcPlanScanBarrier func()

type gcPlanSnapshotRoot struct {
	snapshotID    string
	logicalFileID int64
}

// LoadGCPlanMetadata implements GCPlanCatalog.
//
// All reachability inputs are read by one compound statement so PostgreSQL and
// SQLite cannot expose metadata and roots from different statement snapshots.
func (s *Service) LoadGCPlanMetadata(ctx context.Context, input GCPlanInput) (*GCPlanMetadata, error) {
	return s.loadGCPlanMetadata(ctx, input, nil)
}

func (s *Service) loadGCPlanMetadata(ctx context.Context, input GCPlanInput, barrier gcPlanScanBarrier) (*GCPlanMetadata, error) {
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

	rows, err := s.db.QueryContext(ctx, gcPlanMetadataQuery)
	if err != nil {
		return nil, gcCatalogError(err)
	}
	defer func() { _ = rows.Close() }()

	snapshots := make([]SnapshotRef, 0)
	snapshotIDs := make(map[string]struct{})
	snapshotRoots := make([]gcPlanSnapshotRoot, 0)
	currentLogicalIDs := make([]int64, 0)
	rowNumber := 0
	for rows.Next() {
		var (
			rowKind       int64
			snapshotID    sql.NullString
			logicalFileID sql.NullInt64
			snapshotType  sql.NullString
			label         sql.NullString
			parentID      sql.NullString
			createdAt     sql.NullString
		)
		if err := rows.Scan(&rowKind, &snapshotID, &logicalFileID, &snapshotType, &label, &parentID, &createdAt); err != nil {
			return nil, gcCatalogError(fmt.Errorf("scan GC metadata row: %w", err))
		}
		rowNumber++
		if rowNumber == 1 && barrier != nil {
			barrier()
		}

		switch rowKind {
		case gcPlanSnapshotMetadataRow:
			if !snapshotID.Valid || snapshotID.String == "" || !snapshotType.Valid || !createdAt.Valid {
				return nil, NewError(ErrorInvariantViolation, "load GC plan", "snapshot_metadata_complete", "snapshot metadata row is incomplete", nil)
			}
			if _, duplicate := snapshotIDs[snapshotID.String]; duplicate {
				return nil, NewError(ErrorInvariantViolation, "load GC plan", "snapshot_metadata_unique", fmt.Sprintf("snapshot metadata %q is duplicated", snapshotID.String), nil)
			}
			snapshotIDs[snapshotID.String] = struct{}{}
			snapshots = append(snapshots, SnapshotRef{
				ID:        snapshotID.String,
				Type:      snapshotType.String,
				Label:     label.String,
				ParentID:  parentID.String,
				CreatedAt: parseGCTimestamp(createdAt.String),
			})
		case gcPlanSnapshotRootRow:
			if !snapshotID.Valid || snapshotID.String == "" || !logicalFileID.Valid {
				return nil, NewError(ErrorInvariantViolation, "load GC plan", "snapshot_root_complete", "snapshot root row is incomplete", nil)
			}
			snapshotRoots = append(snapshotRoots, gcPlanSnapshotRoot{snapshotID: snapshotID.String, logicalFileID: logicalFileID.Int64})
		case gcPlanCurrentRootRow:
			if !logicalFileID.Valid {
				return nil, NewError(ErrorInvariantViolation, "load GC plan", "current_root_complete", "current root row is incomplete", nil)
			}
			currentLogicalIDs = append(currentLogicalIDs, logicalFileID.Int64)
		default:
			return nil, NewError(ErrorInvariantViolation, "load GC plan", "metadata_row_kind", fmt.Sprintf("unknown GC metadata row kind %d", rowKind), nil)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, gcCatalogError(fmt.Errorf("iterate GC metadata rows: %w", err))
	}
	if err := rows.Close(); err != nil {
		return nil, gcCatalogError(fmt.Errorf("close GC metadata rows: %w", err))
	}

	excluded := make(map[string]struct{}, len(normalized.ExcludeSnapshotIDs))
	for _, id := range normalized.ExcludeSnapshotIDs {
		if _, exists := snapshotIDs[id]; !exists {
			return nil, NewError(ErrorNotFound, "load GC plan", "excluded_snapshot_exists", fmt.Sprintf("snapshot %q does not exist", id), nil)
		}
		excluded[id] = struct{}{}
	}
	for _, root := range snapshotRoots {
		if _, exists := snapshotIDs[root.snapshotID]; !exists {
			return nil, NewError(ErrorInvariantViolation, "load GC plan", "snapshot_root_has_snapshot", fmt.Sprintf("snapshot root %q has no protected snapshot metadata", root.snapshotID), nil)
		}
	}

	protected := make([]SnapshotRef, 0, len(snapshots)-len(excluded))
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
	for _, logicalID := range currentLogicalIDs {
		ensureGCRoot(roots, logicalID).Current = true
	}
	for _, snapshotRoot := range snapshotRoots {
		if _, skip := excluded[snapshotRoot.snapshotID]; skip {
			continue
		}
		root := ensureGCRoot(roots, snapshotRoot.logicalFileID)
		root.SnapshotIDs = append(root.SnapshotIDs, snapshotRoot.snapshotID)
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

func parseGCTimestamp(value string) time.Time {
	if parsed := parseTimestamp(value); !parsed.IsZero() {
		return parsed
	}
	for _, layout := range []string{
		"2006-01-02 15:04:05.999999999Z07:00",
		"2006-01-02 15:04:05.999999999Z07",
	} {
		if parsed, err := time.Parse(layout, value); err == nil {
			return parsed
		}
	}
	return time.Time{}
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
