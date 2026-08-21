package catalog

import (
	"context"
	"fmt"
)

// LoadReachabilityRoots implements ReachabilityCatalog.
// It queries the two reachability sources — physical_file (active working set)
// and snapshot_file (snapshot-protected set) — and returns both sets of logical
// file IDs. These two SELECT DISTINCT queries work identically on SQLite and
// PostgreSQL; no packed/legacy distinction exists at the logical-file level.
func (s *Service) LoadReachabilityRoots(ctx context.Context) (roots *ReachabilityRoots, err error) {
	defer func() {
		err = translateServiceError("load reachability roots", "reachability root load failed", err)
	}()

	current, err := s.loadCurrentRoots(ctx)
	if err != nil {
		return nil, err
	}
	snapshot, err := s.loadSnapshotRoots(ctx)
	if err != nil {
		return nil, err
	}
	return &ReachabilityRoots{Current: current, Snapshot: snapshot}, nil
}

func (s *Service) loadCurrentRoots(ctx context.Context) (map[int64]struct{}, error) {
	const q = `SELECT DISTINCT logical_file_id FROM physical_file`
	rows, err := s.db.QueryContext(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("catalog: load current reachability roots: %w", err)
	}
	defer func() { _ = rows.Close() }()
	return scanIDSet(rows)
}

func (s *Service) loadSnapshotRoots(ctx context.Context) (map[int64]struct{}, error) {
	const q = `SELECT DISTINCT logical_file_id FROM snapshot_file`
	rows, err := s.db.QueryContext(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("catalog: load snapshot reachability roots: %w", err)
	}
	defer func() { _ = rows.Close() }()
	return scanIDSet(rows)
}

// scanIDSet scans a single-column int64 result set into a set.
func scanIDSet(rows interface {
	Next() bool
	Scan(...any) error
	Err() error
}) (map[int64]struct{}, error) {
	set := make(map[int64]struct{})
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("catalog: scan id: %w", err)
		}
		set[id] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("catalog: iterate id rows: %w", err)
	}
	return set, nil
}
