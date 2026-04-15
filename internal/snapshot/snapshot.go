package snapshot

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/franchoy/coldkeep/internal/storage"
)

// Snapshot represents an immutable point-in-time snapshot of a set of files.
// Snapshot rows are never modified after insert.
type Snapshot struct {
	ID        string
	CreatedAt time.Time
	Type      string // "full" | "partial"
	Label     sql.NullString
}

// SnapshotFile represents a single file entry within a snapshot.
// SnapshotFile rows are insert-only.
type SnapshotFile struct {
	ID            int64
	SnapshotID    string
	Path          string
	LogicalFileID int64
	Size          sql.NullInt64
	Mode          sql.NullInt64
	MTime         sql.NullTime
}

type RestoreSnapshotOptions struct {
	DestinationMode storage.RestoreDestinationMode
	Destination     string
	Overwrite       bool
	StrictMetadata  bool
	NoMetadata      bool
	StorageContext  *storage.StorageContext
}

type RestoreSnapshotResult struct {
	SnapshotID     string
	RestoredFiles  int64
	RequestedPaths int64
	OutputPaths    []string
}

type snapshotRestoreRow struct {
	Path          string
	LogicalFileID int64
	Mode          sql.NullInt64
	MTime         sql.NullTime
}

type snapshotRestorePlanItem struct {
	Path          string
	LogicalFileID int64
	Mode          sql.NullInt64
	MTime         sql.NullTime
	OutputPath    string
}

var multiSlash = regexp.MustCompile(`/{2,}`)

type sqlExecutor interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// NormalizeSnapshotPath normalizes a snapshot-relative path:
//   - rejects empty paths
//   - rejects leading/trailing whitespace
//   - converts Windows separators "\\" to "/"
//   - removes leading "./"
//   - collapses duplicate "/" separators into one
//   - rejects absolute paths (paths starting with "/")
func NormalizeSnapshotPath(path string) (string, error) {
	if path == "" || strings.TrimSpace(path) == "" {
		return "", errors.New("snapshot path cannot be empty")
	}

	if path != strings.TrimSpace(path) {
		return "", fmt.Errorf("snapshot path cannot have leading or trailing whitespace, got %q", path)
	}

	normalized := path

	// Normalize separators to ensure stable cross-platform snapshot paths.
	normalized = strings.ReplaceAll(normalized, "\\", "/")

	// Reject absolute paths – snapshots use relative paths.
	if strings.HasPrefix(normalized, "/") {
		return "", fmt.Errorf("snapshot path must be relative, got %q", normalized)
	}

	// Strip leading "./"
	for strings.HasPrefix(normalized, "./") {
		normalized = normalized[2:]
	}

	// Collapse consecutive slashes.
	normalized = multiSlash.ReplaceAllString(normalized, "/")

	// After stripping, path must not be empty.
	if normalized == "" {
		return "", errors.New("snapshot path cannot be empty after normalization")
	}

	return normalized, nil
}

// InsertSnapshot inserts an immutable snapshot row. id must be non-empty.
// snapshotType must be "full" or "partial".
func InsertSnapshot(ctx context.Context, db *sql.DB, s Snapshot) error {
	return insertSnapshot(ctx, db, s)
}

func insertSnapshot(ctx context.Context, exec sqlExecutor, s Snapshot) error {
	if s.ID == "" {
		return errors.New("snapshot id cannot be empty")
	}
	if s.Type != "full" && s.Type != "partial" {
		return fmt.Errorf("snapshot type must be 'full' or 'partial', got %q", s.Type)
	}
	if s.CreatedAt.IsZero() {
		return errors.New("snapshot created_at cannot be zero")
	}

	_, err := exec.ExecContext(
		ctx,
		`INSERT INTO snapshot (id, created_at, type, label) VALUES ($1, $2, $3, $4)`,
		s.ID,
		s.CreatedAt.UTC(),
		s.Type,
		s.Label,
	)
	if err != nil {
		return fmt.Errorf("insert snapshot id=%s: %w", s.ID, err)
	}

	log.Printf("snapshot: inserted id=%s type=%s", s.ID, s.Type)
	return nil
}

// InsertSnapshotFile inserts a snapshot_file row. The path is normalized before
// insert. The logical_file referenced by logicalFileID must exist.
func InsertSnapshotFile(ctx context.Context, db *sql.DB, sf SnapshotFile) (int64, error) {
	return insertSnapshotFile(ctx, db, sf)
}

func insertSnapshotFile(ctx context.Context, exec sqlExecutor, sf SnapshotFile) (int64, error) {
	if sf.SnapshotID == "" {
		return 0, errors.New("snapshot_file snapshot_id cannot be empty")
	}

	normalizedPath, err := NormalizeSnapshotPath(sf.Path)
	if err != nil {
		return 0, fmt.Errorf("normalize snapshot_file path: %w", err)
	}

	if sf.LogicalFileID <= 0 {
		return 0, fmt.Errorf("snapshot_file logical_file_id must be positive, got %d", sf.LogicalFileID)
	}

	// Verify the logical_file exists before inserting.
	var exists int
	err = exec.QueryRowContext(ctx, `SELECT 1 FROM logical_file WHERE id = $1`, sf.LogicalFileID).Scan(&exists)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, fmt.Errorf("snapshot_file references non-existent logical_file id=%d", sf.LogicalFileID)
		}
		return 0, fmt.Errorf("check logical_file existence id=%d: %w", sf.LogicalFileID, err)
	}

	var id int64
	err = exec.QueryRowContext(
		ctx,
		`INSERT INTO snapshot_file (snapshot_id, path, logical_file_id, size, mode, mtime)
		 VALUES ($1, $2, $3, $4, $5, $6)
		 RETURNING id`,
		sf.SnapshotID,
		normalizedPath,
		sf.LogicalFileID,
		sf.Size,
		sf.Mode,
		sf.MTime,
	).Scan(&id)
	if err != nil {
		return 0, fmt.Errorf("insert snapshot_file snapshot_id=%s path=%q: %w", sf.SnapshotID, normalizedPath, err)
	}

	log.Printf("snapshot: inserted snapshot_file id=%d snapshot_id=%s path=%q", id, sf.SnapshotID, normalizedPath)
	return id, nil
}

func normalizeSourcePathForSnapshot(path string) (string, error) {
	normalized := strings.ReplaceAll(path, "\\", "/")
	for strings.HasPrefix(normalized, "/") {
		normalized = normalized[1:]
	}
	return NormalizeSnapshotPath(normalized)
}

func normalizeSnapshotRestoreInputFilters(paths []string) (exactFilters []string, dirPrefixes []string, exactSet map[string]struct{}, err error) {
	seenInput := make(map[string]struct{})
	exactSet = make(map[string]struct{})

	for _, rawPath := range paths {
		normalizedPath, normErr := NormalizeSnapshotPath(rawPath)
		if normErr != nil {
			return nil, nil, nil, fmt.Errorf("normalize input path %q: %w", rawPath, normErr)
		}
		if _, exists := seenInput[normalizedPath]; exists {
			continue
		}
		seenInput[normalizedPath] = struct{}{}

		if strings.HasSuffix(normalizedPath, "/") {
			dirPrefixes = append(dirPrefixes, normalizedPath)
			continue
		}

		exactFilters = append(exactFilters, normalizedPath)
		exactSet[normalizedPath] = struct{}{}
	}

	sort.Strings(exactFilters)
	sort.Strings(dirPrefixes)
	return exactFilters, dirPrefixes, exactSet, nil
}

func resolveSnapshotRestoreSelection(
	ctx context.Context,
	db *sql.DB,
	snapshotID string,
	requestedPaths []string,
) ([]snapshotRestoreRow, []string, error) {
	var snapshotExists int
	if err := db.QueryRowContext(ctx, `SELECT 1 FROM snapshot WHERE id = $1`, snapshotID).Scan(&snapshotExists); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil, fmt.Errorf("snapshot not found: %s", snapshotID)
		}
		return nil, nil, fmt.Errorf("check snapshot existence id=%s: %w", snapshotID, err)
	}

	exactFilters, dirPrefixes, exactSet, err := normalizeSnapshotRestoreInputFilters(requestedPaths)
	if err != nil {
		return nil, nil, err
	}

	rows, err := db.QueryContext(ctx, `
		SELECT sf.path, sf.logical_file_id, sf.mode, sf.mtime
		FROM snapshot_file sf
		WHERE sf.snapshot_id = $1
		ORDER BY sf.path, sf.logical_file_id
	`, snapshotID)
	if err != nil {
		return nil, nil, fmt.Errorf("query snapshot rows for restore snapshot_id=%s: %w", snapshotID, err)
	}
	defer func() { _ = rows.Close() }()

	selected := make([]snapshotRestoreRow, 0)
	seenSelectedPaths := make(map[string]struct{})
	foundExact := make(map[string]struct{})

	hasFilters := len(exactFilters) > 0 || len(dirPrefixes) > 0

	for rows.Next() {
		var row snapshotRestoreRow
		if err := rows.Scan(&row.Path, &row.LogicalFileID, &row.Mode, &row.MTime); err != nil {
			return nil, nil, fmt.Errorf("scan snapshot restore row: %w", err)
		}

		normalizedPath, normErr := NormalizeSnapshotPath(row.Path)
		if normErr != nil {
			return nil, nil, fmt.Errorf("normalize snapshot_file path %q: %w", row.Path, normErr)
		}
		row.Path = normalizedPath

		if hasFilters {
			matched := false
			if _, isExact := exactSet[row.Path]; isExact {
				foundExact[row.Path] = struct{}{}
				matched = true
			}
			if !matched {
				for _, prefix := range dirPrefixes {
					if strings.HasPrefix(row.Path, prefix) {
						matched = true
						break
					}
				}
			}
			if !matched {
				continue
			}
		}

		if _, exists := seenSelectedPaths[row.Path]; exists {
			continue
		}
		seenSelectedPaths[row.Path] = struct{}{}
		selected = append(selected, row)
	}
	if err := rows.Err(); err != nil {
		return nil, nil, fmt.Errorf("iterate snapshot restore rows: %w", err)
	}

	for _, exactPath := range exactFilters {
		if _, ok := foundExact[exactPath]; !ok {
			return nil, nil, fmt.Errorf("path not found in snapshot %s: %s", snapshotID, exactPath)
		}
	}

	outputExactFilters := make([]string, len(exactFilters))
	copy(outputExactFilters, exactFilters)
	return selected, outputExactFilters, nil
}

func planSnapshotRestoreOutputs(rows []snapshotRestoreRow, requestedPaths []string, opts RestoreSnapshotOptions) ([]snapshotRestorePlanItem, error) {
	mode := opts.DestinationMode
	if mode == "" {
		mode = storage.RestoreDestinationOriginal
	}

	if opts.StrictMetadata && opts.NoMetadata {
		return nil, errors.New("--strict and --no-metadata cannot be used together")
	}

	if mode == storage.RestoreDestinationOriginal && strings.TrimSpace(opts.Destination) != "" {
		return nil, errors.New("destination is only supported with prefix or override mode")
	}
	if (mode == storage.RestoreDestinationPrefix || mode == storage.RestoreDestinationOverride) && strings.TrimSpace(opts.Destination) == "" {
		return nil, fmt.Errorf("destination is required with mode %s", mode)
	}

	if mode == storage.RestoreDestinationOverride {
		if len(requestedPaths) != 1 || strings.HasSuffix(requestedPaths[0], "/") {
			return nil, errors.New("override mode is only allowed for single exact-path snapshot restore")
		}
		if len(rows) != 1 {
			return nil, errors.New("override mode requires exactly one matched snapshot file")
		}
	}

	plans := make([]snapshotRestorePlanItem, 0, len(rows))
	seenOutput := make(map[string]struct{})

	for _, row := range rows {
		outputPath := row.Path
		switch mode {
		case storage.RestoreDestinationOriginal:
			// Snapshot path is already normalized and relative.
			outputPath = row.Path
		case storage.RestoreDestinationPrefix:
			prefix := strings.TrimSpace(opts.Destination)
			absPrefix, err := filepath.Abs(prefix)
			if err != nil {
				return nil, fmt.Errorf("resolve prefix destination: %w", err)
			}
			outputPath = filepath.Join(absPrefix, filepath.FromSlash(row.Path))
		case storage.RestoreDestinationOverride:
			overridePath := strings.TrimSpace(opts.Destination)
			absOverride, err := filepath.Abs(overridePath)
			if err != nil {
				return nil, fmt.Errorf("resolve override destination: %w", err)
			}
			outputPath = filepath.Clean(absOverride)
		default:
			return nil, fmt.Errorf("unsupported restore destination mode: %s", mode)
		}

		cleanOutputPath := filepath.Clean(outputPath)
		if _, exists := seenOutput[cleanOutputPath]; exists {
			return nil, fmt.Errorf("restore output path collision: %s", cleanOutputPath)
		}
		seenOutput[cleanOutputPath] = struct{}{}

		plans = append(plans, snapshotRestorePlanItem{
			Path:          row.Path,
			LogicalFileID: row.LogicalFileID,
			Mode:          row.Mode,
			MTime:         row.MTime,
			OutputPath:    cleanOutputPath,
		})
	}

	for _, plan := range plans {
		if !opts.Overwrite {
			if _, err := os.Stat(plan.OutputPath); err == nil {
				return nil, fmt.Errorf("output file already exists: %s (use --overwrite)", plan.OutputPath)
			} else if !os.IsNotExist(err) {
				return nil, fmt.Errorf("check output path %s: %w", plan.OutputPath, err)
			}
		}
	}

	return plans, nil
}

func applySnapshotMetadata(outputPath string, mode sql.NullInt64, mtime sql.NullTime, opts RestoreSnapshotOptions) error {
	if opts.NoMetadata {
		return nil
	}

	metadataErrs := make([]string, 0)

	if mode.Valid {
		if err := os.Chmod(outputPath, os.FileMode(mode.Int64)); err != nil {
			metadataErrs = append(metadataErrs, fmt.Sprintf("chmod: %v", err))
		}
	}

	if mtime.Valid {
		mt := mtime.Time
		if err := os.Chtimes(outputPath, mt, mt); err != nil {
			metadataErrs = append(metadataErrs, fmt.Sprintf("chtimes: %v", err))
		}
	}

	if len(metadataErrs) == 0 {
		return nil
	}

	metadataErr := fmt.Errorf("apply snapshot metadata for %q: %s", outputPath, strings.Join(metadataErrs, "; "))
	if opts.StrictMetadata {
		return metadataErr
	}
	log.Printf("snapshot: restore metadata warning path=%q error=%q", outputPath, metadataErr.Error())
	return nil
}

func executeSnapshotRestorePlan(ctx context.Context, plans []snapshotRestorePlanItem, opts RestoreSnapshotOptions) (*RestoreSnapshotResult, error) {
	if opts.StorageContext == nil {
		return nil, errors.New("storage context is required for snapshot restore")
	}

	result := &RestoreSnapshotResult{
		RestoredFiles: int64(0),
		OutputPaths:   make([]string, 0, len(plans)),
	}

	for _, plan := range plans {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		restoreResult, err := storage.RestoreFileWithStorageContextResultOptions(
			*opts.StorageContext,
			plan.LogicalFileID,
			plan.OutputPath,
			storage.RestoreOptions{Overwrite: opts.Overwrite, NoMetadata: true},
		)
		if err != nil {
			return nil, fmt.Errorf("restore snapshot path %q logical_file_id=%d: %w", plan.Path, plan.LogicalFileID, err)
		}

		if err := applySnapshotMetadata(restoreResult.OutputPath, plan.Mode, plan.MTime, opts); err != nil {
			return nil, err
		}

		result.RestoredFiles++
		result.OutputPaths = append(result.OutputPaths, restoreResult.OutputPath)
	}

	return result, nil
}

func RestoreSnapshot(
	ctx context.Context,
	db *sql.DB,
	snapshotID string,
	paths []string,
	opts RestoreSnapshotOptions,
) (*RestoreSnapshotResult, error) {
	if db == nil {
		return nil, errors.New("snapshot db cannot be nil")
	}
	if strings.TrimSpace(snapshotID) == "" {
		return nil, errors.New("snapshot id cannot be empty")
	}
	if opts.StorageContext == nil {
		return nil, errors.New("storage context is required")
	}

	selected, normalizedExactPaths, err := resolveSnapshotRestoreSelection(ctx, db, snapshotID, paths)
	if err != nil {
		return nil, err
	}

	plans, err := planSnapshotRestoreOutputs(selected, normalizedExactPaths, opts)
	if err != nil {
		return nil, err
	}

	result, err := executeSnapshotRestorePlan(ctx, plans, opts)
	if err != nil {
		return nil, err
	}

	result.SnapshotID = snapshotID
	result.RequestedPaths = int64(len(paths))
	return result, nil
}

// CreateSnapshot creates an atomic point-in-time snapshot from current physical_file rows.
// When paths is nil or empty, all physical_file rows are copied into the snapshot.
// When paths is non-empty, rows are filtered by exact paths and directory prefixes ending with '/'.
func CreateSnapshot(
	ctx context.Context,
	db *sql.DB,
	snapshotID string,
	snapshotType string,
	label *string,
	paths []string,
) error {
	if db == nil {
		return errors.New("snapshot db cannot be nil")
	}
	if snapshotID == "" {
		return errors.New("snapshot id cannot be empty")
	}
	if snapshotType != "full" && snapshotType != "partial" {
		return fmt.Errorf("snapshot type must be 'full' or 'partial', got %q", snapshotType)
	}

	hasPaths := len(paths) > 0
	if hasPaths && snapshotType != "partial" {
		return fmt.Errorf("snapshot type must be 'partial' when paths are provided, got %q", snapshotType)
	}
	if !hasPaths && snapshotType != "full" {
		return fmt.Errorf("snapshot type must be 'full' when no paths are provided, got %q", snapshotType)
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin snapshot transaction: %w", err)
	}
	defer func() {
		_ = tx.Rollback()
	}()

	s := Snapshot{
		ID:        snapshotID,
		CreatedAt: time.Now().UTC(),
		Type:      snapshotType,
	}
	if label != nil {
		s.Label = sql.NullString{String: *label, Valid: true}
	}

	if err := insertSnapshot(ctx, tx, s); err != nil {
		return err
	}

	var (
		exactFilters   []string
		dirPrefixes    []string
		exactFilterSet = make(map[string]struct{})
	)

	if hasPaths {
		seenInput := make(map[string]struct{})
		for _, rawPath := range paths {
			normalizedPath, normErr := NormalizeSnapshotPath(rawPath)
			if normErr != nil {
				return fmt.Errorf("normalize input path %q: %w", rawPath, normErr)
			}
			if _, exists := seenInput[normalizedPath]; exists {
				continue
			}
			seenInput[normalizedPath] = struct{}{}

			if strings.HasSuffix(normalizedPath, "/") {
				dirPrefixes = append(dirPrefixes, normalizedPath)
				continue
			}

			exactFilters = append(exactFilters, normalizedPath)
			exactFilterSet[normalizedPath] = struct{}{}
		}

		sort.Strings(exactFilters)
		sort.Strings(dirPrefixes)

		if len(exactFilters) == 0 && len(dirPrefixes) == 0 {
			return errors.New("partial snapshot requires at least one valid path filter")
		}
	}

	query := `
		SELECT pf.path, pf.logical_file_id, lf.total_size, pf.mode, pf.mtime
		FROM physical_file pf
		JOIN logical_file lf ON lf.id = pf.logical_file_id
	`
	query += " ORDER BY pf.path, pf.logical_file_id"

	rows, err := tx.QueryContext(ctx, query)
	if err != nil {
		return fmt.Errorf("query snapshot source rows: %w", err)
	}
	defer func() { _ = rows.Close() }()

	seenSnapshotPaths := make(map[string]struct{})
	foundExact := make(map[string]struct{})
	insertedCount := 0

	for rows.Next() {
		var (
			path          string
			logicalFileID int64
			totalSize     int64
			mode          sql.NullInt64
			mtime         sql.NullTime
		)
		if err := rows.Scan(&path, &logicalFileID, &totalSize, &mode, &mtime); err != nil {
			return fmt.Errorf("scan snapshot source row: %w", err)
		}

		normalizedPath, err := normalizeSourcePathForSnapshot(path)
		if err != nil {
			return fmt.Errorf("normalize source physical_file path %q: %w", path, err)
		}

		if hasPaths {
			matched := false
			if _, isExact := exactFilterSet[normalizedPath]; isExact {
				foundExact[normalizedPath] = struct{}{}
				matched = true
			}
			if !matched {
				for _, prefix := range dirPrefixes {
					if strings.HasPrefix(normalizedPath, prefix) {
						matched = true
						break
					}
				}
			}
			if !matched {
				continue
			}
		}

		if _, duplicate := seenSnapshotPaths[normalizedPath]; duplicate {
			continue
		}
		seenSnapshotPaths[normalizedPath] = struct{}{}

		_, err = insertSnapshotFile(ctx, tx, SnapshotFile{
			SnapshotID:    snapshotID,
			Path:          normalizedPath,
			LogicalFileID: logicalFileID,
			Size:          sql.NullInt64{Int64: totalSize, Valid: true},
			Mode:          mode,
			MTime:         mtime,
		})
		if err != nil {
			return err
		}
		insertedCount++
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate snapshot source rows: %w", err)
	}

	for _, exactPath := range exactFilters {
		if _, ok := foundExact[exactPath]; !ok {
			return fmt.Errorf("path not found in current state: %s", exactPath)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit snapshot transaction: %w", err)
	}

	if insertedCount == 0 {
		log.Printf("snapshot: created id=%s type=%s files=0 (empty snapshot)", snapshotID, snapshotType)
		return nil
	}

	log.Printf("snapshot: created id=%s type=%s files=%d", snapshotID, snapshotType, insertedCount)
	return nil
}
