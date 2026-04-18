package snapshot

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

// pathResolverDB is the DB surface required by the path resolver helpers.
// Both *sql.DB and *sql.Tx satisfy this interface.
type pathResolverDB interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// ResolveSnapshotPath resolves a single already-normalized path to its
// snapshot_path.id, inserting the row if it does not yet exist.
//
// Callers are responsible for normalizing the path before calling this function.
// An empty path is rejected because the schema enforces a non-empty path check.
func ResolveSnapshotPath(ctx context.Context, exec pathResolverDB, normalizedPath string) (int64, error) {
	if normalizedPath == "" {
		return 0, fmt.Errorf("resolve snapshot_path: path must not be empty")
	}

	// Upsert: insert the path if absent, otherwise do nothing.
	// Both SQLite (INSERT OR IGNORE) and PostgreSQL (ON CONFLICT DO NOTHING)
	// behave identically for our purposes; we use the portable plain INSERT
	// variant and fall through to the SELECT on conflict.
	_, err := exec.ExecContext(ctx,
		`INSERT INTO snapshot_path (path) VALUES ($1) ON CONFLICT (path) DO NOTHING`,
		normalizedPath,
	)
	if err != nil {
		return 0, fmt.Errorf("upsert snapshot_path %q: %w", normalizedPath, err)
	}

	var id int64
	if err := exec.QueryRowContext(ctx,
		`SELECT id FROM snapshot_path WHERE path = $1`,
		normalizedPath,
	).Scan(&id); err != nil {
		return 0, fmt.Errorf("resolve snapshot_path id for %q: %w", normalizedPath, err)
	}

	return id, nil
}

// LoadSnapshotPathByID resolves a persisted snapshot_path.id back to its path string.
//
// This helper is intended for integrity/debugging flows or explicit hydration.
// Normal snapshot reads should continue to prefer SQL joins on snapshot_path.
func LoadSnapshotPathByID(ctx context.Context, exec pathResolverDB, pathID int64) (string, error) {
	if pathID <= 0 {
		return "", fmt.Errorf("load snapshot_path: path_id must be positive, got %d", pathID)
	}

	var path string
	if err := exec.QueryRowContext(ctx,
		`SELECT path FROM snapshot_path WHERE id = $1`,
		pathID,
	).Scan(&path); err != nil {
		if err == sql.ErrNoRows {
			return "", fmt.Errorf("snapshot_path not found for path_id=%d", pathID)
		}
		return "", fmt.Errorf("load snapshot_path for path_id=%d: %w", pathID, err)
	}

	return path, nil
}

// LoadSnapshotPathsByID bulk-loads path strings for a set of snapshot_path IDs.
// It returns a map[pathID]path and fails if any requested ID is missing.
//
// This helper is intended for integrity/debugging flows or explicit hydration.
// Normal snapshot reads should continue to prefer SQL joins on snapshot_path.
func LoadSnapshotPathsByID(ctx context.Context, exec pathResolverDB, pathIDs []int64) (map[int64]string, error) {
	if len(pathIDs) == 0 {
		return map[int64]string{}, nil
	}

	seen := make(map[int64]struct{}, len(pathIDs))
	unique := make([]int64, 0, len(pathIDs))
	for _, pathID := range pathIDs {
		if pathID <= 0 {
			return nil, fmt.Errorf("load snapshot_paths: path_id must be positive, got %d", pathID)
		}
		if _, ok := seen[pathID]; ok {
			continue
		}
		seen[pathID] = struct{}{}
		unique = append(unique, pathID)
	}

	placeholders := make([]string, len(unique))
	args := make([]any, len(unique))
	for i, pathID := range unique {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
		args[i] = pathID
	}

	rows, err := exec.QueryContext(ctx,
		`SELECT id, path FROM snapshot_path WHERE id IN (`+strings.Join(placeholders, ", ")+`)`,
		args...,
	)
	if err != nil {
		return nil, fmt.Errorf("bulk load snapshot_path rows: %w", err)
	}
	defer rows.Close()

	result := make(map[int64]string, len(unique))
	for rows.Next() {
		var pathID int64
		var path string
		if err := rows.Scan(&pathID, &path); err != nil {
			return nil, fmt.Errorf("scan snapshot_path rows: %w", err)
		}
		result[pathID] = path
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate snapshot_path rows: %w", err)
	}

	for _, pathID := range unique {
		if _, ok := result[pathID]; !ok {
			return nil, fmt.Errorf("snapshot_path not found for path_id=%d", pathID)
		}
	}

	return result, nil
}

// ResolveSnapshotPaths resolves a batch of already-normalized paths to their
// snapshot_path.ids. It returns a map[path]id covering every input path.
//
// The function:
//  1. Deduplicates the input in memory.
//  2. Fetches IDs for paths that already exist in a single bulk SELECT.
//  3. Inserts only the missing paths (INSERT … ON CONFLICT DO NOTHING).
//  4. Fetches IDs for the newly inserted paths.
//
// Callers are responsible for normalizing paths before calling this function.
// Any empty string in paths is rejected immediately.
func ResolveSnapshotPaths(ctx context.Context, exec pathResolverDB, paths []string) (map[string]int64, error) {
	if len(paths) == 0 {
		return map[string]int64{}, nil
	}

	// 1. Deduplicate.
	seen := make(map[string]struct{}, len(paths))
	unique := make([]string, 0, len(paths))
	for _, p := range paths {
		if p == "" {
			return nil, fmt.Errorf("resolve snapshot_paths: path must not be empty")
		}
		if _, ok := seen[p]; !ok {
			seen[p] = struct{}{}
			unique = append(unique, p)
		}
	}

	result := make(map[string]int64, len(unique))

	// 2. Bulk-fetch existing IDs.
	placeholders := make([]string, len(unique))
	args := make([]any, len(unique))
	for i, p := range unique {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
		args[i] = p
	}
	selectQuery := `SELECT path, id FROM snapshot_path WHERE path IN (` +
		strings.Join(placeholders, ", ") + `)`

	rows, err := exec.QueryContext(ctx, selectQuery, args...)
	if err != nil {
		return nil, fmt.Errorf("bulk fetch snapshot_path ids: %w", err)
	}
	func() {
		defer rows.Close()
		for rows.Next() {
			var p string
			var id int64
			if scanErr := rows.Scan(&p, &id); scanErr == nil {
				result[p] = id
			}
		}
	}()
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("scan snapshot_path rows: %w", err)
	}

	// 3. Collect missing paths and insert them.
	var missing []string
	for _, p := range unique {
		if _, found := result[p]; !found {
			missing = append(missing, p)
		}
	}

	const insertBatchSize = 200
	for start := 0; start < len(missing); start += insertBatchSize {
		end := start + insertBatchSize
		if end > len(missing) {
			end = len(missing)
		}
		batch := missing[start:end]

		valuePlaceholders := make([]string, len(batch))
		insertArgs := make([]any, len(batch))
		for i, p := range batch {
			valuePlaceholders[i] = fmt.Sprintf("($%d)", i+1)
			insertArgs[i] = p
		}

		insertQuery := `INSERT INTO snapshot_path (path) VALUES ` +
			strings.Join(valuePlaceholders, ", ") +
			` ON CONFLICT (path) DO NOTHING`

		if _, err := exec.ExecContext(ctx, insertQuery, insertArgs...); err != nil {
			return nil, fmt.Errorf("bulk insert snapshot_path rows: %w", err)
		}
	}

	// 4. Fetch IDs for newly inserted paths.
	if len(missing) > 0 {
		placeholders2 := make([]string, len(missing))
		args2 := make([]any, len(missing))
		for i, p := range missing {
			placeholders2[i] = fmt.Sprintf("$%d", i+1)
			args2[i] = p
		}
		rows2, err := exec.QueryContext(ctx,
			`SELECT path, id FROM snapshot_path WHERE path IN (`+strings.Join(placeholders2, ", ")+`)`,
			args2...,
		)
		if err != nil {
			return nil, fmt.Errorf("fetch newly inserted snapshot_path ids: %w", err)
		}
		func() {
			defer rows2.Close()
			for rows2.Next() {
				var p string
				var id int64
				if scanErr := rows2.Scan(&p, &id); scanErr == nil {
					result[p] = id
				}
			}
		}()
		if err := rows2.Err(); err != nil {
			return nil, fmt.Errorf("scan newly inserted snapshot_path rows: %w", err)
		}
	}

	// Sanity check: every input path must have been resolved.
	for _, p := range unique {
		if _, ok := result[p]; !ok {
			return nil, fmt.Errorf("snapshot_path resolution incomplete: no id for %q", p)
		}
	}

	return result, nil
}
