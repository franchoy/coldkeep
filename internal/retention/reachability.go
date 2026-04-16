package retention

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

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

func isMissingSnapshotTableError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "no such table: snapshot_file") ||
		strings.Contains(msg, "relation \"snapshot_file\" does not exist")
}
