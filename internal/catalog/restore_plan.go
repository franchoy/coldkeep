package catalog

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"
)

var restoreChunkerVersionPattern = regexp.MustCompile(`^v[0-9]+(?:-[a-z0-9]+)+$`)

// LoadRestorePlanMetadata resolves one selector and constructs a complete
// immutable recipe using the Service's injected DB or caller-owned transaction.
func (s *Service) LoadRestorePlanMetadata(ctx context.Context, input RestorePlanInput) (*RestorePlanMetadata, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, restoreCatalogError(err)
	}
	if err := ValidateRestorePlanInput(input); err != nil {
		return nil, err
	}

	logicalID, source, err := s.resolveRestoreSelector(ctx, input)
	if err != nil {
		return nil, err
	}
	logical, err := s.loadRestoreLogicalFile(ctx, logicalID)
	if err != nil {
		return nil, err
	}
	if logical.Status != "COMPLETED" {
		return nil, NewError(ErrorConflict, "load restore plan", "completed_logical_file", fmt.Sprintf("logical file %d is not completed", logicalID), nil)
	}
	placements, err := s.LoadChunkPlacements(ctx, logicalID)
	if err != nil {
		return nil, err
	}
	if (logical.TotalSize == 0 && len(placements) != 0) || (logical.TotalSize > 0 && len(placements) == 0) {
		return nil, NewError(ErrorInvariantViolation, "load restore plan", "recipe_matches_logical_file_size", fmt.Sprintf("logical file %d has inconsistent zero-length recipe metadata", logicalID), nil)
	}
	return &RestorePlanMetadata{Selector: cloneRestoreSelector(input), LogicalFile: logical, Source: source, Placements: placements}, nil
}

func (s *Service) resolveRestoreSelector(ctx context.Context, input RestorePlanInput) (int64, RestoreSourceRef, error) {
	switch input.Selector {
	case RestoreByFileID:
		return input.FileID, RestoreSourceRef{}, nil
	case RestoreByStoredPath:
		return s.resolveStoredPath(ctx, input.StoredPath)
	case RestoreBySnapshotPath:
		return s.resolveSnapshotPath(ctx, input.SnapshotID, input.SnapshotPath)
	default:
		return 0, RestoreSourceRef{}, NewError(ErrorInvalidArgument, "load restore plan", "exactly_one_restore_selector", "unsupported restore selector", nil)
	}
}

func (s *Service) resolveStoredPath(ctx context.Context, path string) (int64, RestoreSourceRef, error) {
	var logicalID int64
	var mode, uid, gid sql.NullInt64
	var mtime sql.NullTime
	var complete bool
	err := s.db.QueryRowContext(ctx, `
SELECT logical_file_id, mode, mtime, uid, gid, is_metadata_complete
FROM physical_file WHERE path = $1`, path).Scan(&logicalID, &mode, &mtime, &uid, &gid, &complete)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, RestoreSourceRef{}, NewError(ErrorNotFound, "load restore plan", "", fmt.Sprintf("stored path %q not found", path), err)
		}
		return 0, RestoreSourceRef{}, restoreCatalogError(fmt.Errorf("resolve stored path %q: %w", path, err))
	}
	return logicalID, RestoreSourceRef{StoredPath: path, Mode: nullInt64Pointer(mode), MTime: nullTimePointer(mtime), UID: nullInt64Pointer(uid), GID: nullInt64Pointer(gid), IsMetadataComplete: complete}, nil
}

func (s *Service) resolveSnapshotPath(ctx context.Context, snapshotID, path string) (int64, RestoreSourceRef, error) {
	var logicalID int64
	var size, mode sql.NullInt64
	var mtime sql.NullTime
	err := s.db.QueryRowContext(ctx, `
SELECT sf.logical_file_id, sf.size, sf.mode, sf.mtime
FROM snapshot_file sf
JOIN snapshot_path sp ON sp.id = sf.path_id
WHERE sf.snapshot_id = $1 AND sp.path = $2`, snapshotID, path).Scan(&logicalID, &size, &mode, &mtime)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, RestoreSourceRef{}, NewError(ErrorNotFound, "load restore plan", "", fmt.Sprintf("snapshot path %q in snapshot %q not found", path, snapshotID), err)
		}
		return 0, RestoreSourceRef{}, restoreCatalogError(fmt.Errorf("resolve snapshot path %q in snapshot %q: %w", path, snapshotID, err))
	}
	return logicalID, RestoreSourceRef{SnapshotID: snapshotID, SnapshotPath: path, Size: nullInt64Pointer(size), Mode: nullInt64Pointer(mode), MTime: nullTimePointer(mtime), IsMetadataComplete: size.Valid && mode.Valid && mtime.Valid}, nil
}

func (s *Service) loadRestoreLogicalFile(ctx context.Context, id int64) (RestoreLogicalFileRef, error) {
	var ref RestoreLogicalFileRef
	err := s.db.QueryRowContext(ctx, `
SELECT id, original_name, total_size, file_hash, chunker_version, status
FROM logical_file WHERE id = $1`, id).Scan(&ref.ID, &ref.OriginalName, &ref.TotalSize, &ref.FileHash, &ref.ChunkerVersion, &ref.Status)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return RestoreLogicalFileRef{}, NewError(ErrorNotFound, "load restore plan", "", fmt.Sprintf("logical file %d not found", id), err)
		}
		return RestoreLogicalFileRef{}, restoreCatalogError(fmt.Errorf("load logical file %d: %w", id, err))
	}
	if ref.OriginalName == "" || ref.FileHash == "" || ref.TotalSize < 0 {
		return RestoreLogicalFileRef{}, NewError(ErrorInvariantViolation, "load restore plan", "complete_logical_file_metadata", fmt.Sprintf("logical file %d metadata is incomplete", id), nil)
	}
	trimmedChunkerVersion := strings.TrimSpace(ref.ChunkerVersion)
	if trimmedChunkerVersion == "" {
		return RestoreLogicalFileRef{}, NewError(ErrorInvariantViolation, "load restore plan", "complete_logical_file_metadata", fmt.Sprintf("logical file %d has empty chunker_version (repository corruption or incomplete migration)", id), nil)
	}
	if !restoreChunkerVersionPattern.MatchString(trimmedChunkerVersion) {
		return RestoreLogicalFileRef{}, NewError(ErrorInvariantViolation, "load restore plan", "complete_logical_file_metadata", fmt.Sprintf("logical file %d has malformed chunker_version %q (expected format like v1-simple-rolling)", id, ref.ChunkerVersion), nil)
	}
	return ref, nil
}

func cloneRestoreSelector(input RestorePlanInput) RestorePlanInput { return input }

func nullInt64Pointer(value sql.NullInt64) *int64 {
	if !value.Valid {
		return nil
	}
	converted := value.Int64
	return &converted
}

func nullTimePointer(value sql.NullTime) *time.Time {
	if !value.Valid {
		return nil
	}
	converted := value.Time.UTC()
	return &converted
}

func restoreCatalogError(err error) error {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return NewError(ErrorCancelled, "load restore plan", "", "restore plan load cancelled", err)
	}
	return NewError(ErrorOperationFailed, "load restore plan", "", "restore plan query failed", err)
}
