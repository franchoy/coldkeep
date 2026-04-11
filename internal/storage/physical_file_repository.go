package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/franchoy/coldkeep/internal/db"
)

type queryRower interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

type execer interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

type queryExecer interface {
	queryRower
	execer
}

type logicalFileRecord struct {
	ID       int64
	Status   string
	RefCount int64
}

type physicalFileRecord struct {
	Path               string
	LogicalFileID      int64
	Mode               sql.NullInt64
	MTime              sql.NullTime
	UID                sql.NullInt64
	GID                sql.NullInt64
	IsMetadataComplete bool
}

type physicalFileMetadata struct {
	Mode               sql.NullInt64
	MTime              sql.NullTime
	UID                sql.NullInt64
	GID                sql.NullInt64
	IsMetadataComplete bool
}

type physicalPathConflictError struct {
	Path                  string
	ExistingLogicalFileID int64
	NewLogicalFileID      int64
}

func (e *physicalPathConflictError) Error() string {
	return fmt.Sprintf(
		"physical path conflict for %q: mapped to logical_file_id=%d, requested logical_file_id=%d",
		e.Path,
		e.ExistingLogicalFileID,
		e.NewLogicalFileID,
	)
}

func normalizePhysicalFilePath(path string) (string, error) {
	trimmed := strings.TrimSpace(path)
	if trimmed == "" {
		return "", errors.New("physical file path cannot be empty")
	}

	// TODO(v1.3+): Consider canonicalizing identity with filepath.EvalSymlinks
	// so symlinked aliases of the same inode do not produce distinct
	// physical_file identities. Keep deferred for now to avoid changing v1.2
	// path semantics and to preserve current behavior on missing/late-bound links.

	absPath, err := filepath.Abs(trimmed)
	if err != nil {
		return "", fmt.Errorf("resolve absolute physical file path: %w", err)
	}

	cleaned := filepath.Clean(absPath)

	return cleaned, nil
}

func buildPhysicalFileMetadata(fileInfo os.FileInfo) physicalFileMetadata {
	meta := physicalFileMetadata{}
	if fileInfo == nil {
		return meta
	}

	meta.Mode = sql.NullInt64{Int64: int64(fileInfo.Mode()), Valid: true}
	meta.MTime = sql.NullTime{Time: fileInfo.ModTime().UTC(), Valid: true}

	if stat, ok := fileInfo.Sys().(*syscall.Stat_t); ok {
		meta.UID = sql.NullInt64{Int64: int64(stat.Uid), Valid: true}
		meta.GID = sql.NullInt64{Int64: int64(stat.Gid), Valid: true}
	}

	meta.IsMetadataComplete = meta.Mode.Valid && meta.MTime.Valid && meta.UID.Valid && meta.GID.Valid
	return meta
}

func getLogicalFileByHashAndSize(ctx context.Context, q queryRower, fileHash string, totalSize int64) (logicalFileRecord, error) {
	var rec logicalFileRecord
	err := q.QueryRowContext(
		ctx,
		`SELECT id, status, ref_count FROM logical_file WHERE file_hash = $1 AND total_size = $2`,
		fileHash,
		totalSize,
	).Scan(&rec.ID, &rec.Status, &rec.RefCount)
	if err != nil {
		return logicalFileRecord{}, err
	}
	return rec, nil
}

func insertLogicalFile(ctx context.Context, q queryRower, originalName string, totalSize int64, fileHash string, status string, refCount int64) (int64, error) {
	var id int64
	err := q.QueryRowContext(
		ctx,
		`INSERT INTO logical_file (original_name, total_size, file_hash, status, ref_count)
		 VALUES ($1, $2, $3, $4, $5)
		 RETURNING id`,
		originalName,
		totalSize,
		fileHash,
		status,
		refCount,
	).Scan(&id)
	if err != nil {
		return 0, err
	}
	return id, nil
}

func getPhysicalFileByPath(ctx context.Context, q queryRower, path string) (physicalFileRecord, error) {
	var rec physicalFileRecord
	err := q.QueryRowContext(
		ctx,
		`SELECT path, logical_file_id, mode, mtime, uid, gid, is_metadata_complete
		 FROM physical_file
		 WHERE path = $1`,
		path,
	).Scan(
		&rec.Path,
		&rec.LogicalFileID,
		&rec.Mode,
		&rec.MTime,
		&rec.UID,
		&rec.GID,
		&rec.IsMetadataComplete,
	)
	if err != nil {
		return physicalFileRecord{}, err
	}
	return rec, nil
}

func insertPhysicalFile(ctx context.Context, ex execer, path string, logicalFileID int64, meta physicalFileMetadata) error {
	_, err := ex.ExecContext(
		ctx,
		`INSERT INTO physical_file (path, logical_file_id, mode, mtime, uid, gid, is_metadata_complete)
		 VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		path,
		logicalFileID,
		meta.Mode,
		meta.MTime,
		meta.UID,
		meta.GID,
		meta.IsMetadataComplete,
	)
	return err
}

func updatePhysicalFile(ctx context.Context, ex execer, path string, logicalFileID int64, meta physicalFileMetadata) error {
	result, err := ex.ExecContext(
		ctx,
		`UPDATE physical_file
		 SET logical_file_id = $2,
		     mode = $3,
		     mtime = $4,
		     uid = $5,
		     gid = $6,
		     is_metadata_complete = $7
		 WHERE path = $1`,
		path,
		logicalFileID,
		meta.Mode,
		meta.MTime,
		meta.UID,
		meta.GID,
		meta.IsMetadataComplete,
	)
	if err != nil {
		return err
	}
	_, _ = result.RowsAffected()
	return nil
}

func incrementLogicalFileRefCount(ctx context.Context, ex execer, logicalFileID int64) error {
	_, err := ex.ExecContext(ctx, `UPDATE logical_file SET ref_count = ref_count + 1 WHERE id = $1`, logicalFileID)
	return err
}

func decrementLogicalFileRefCount(ctx context.Context, ex execer, logicalFileID int64) error {
	_, err := ex.ExecContext(
		ctx,
		`UPDATE logical_file
		 SET ref_count = ref_count - 1
		 WHERE id = $1 AND ref_count > 0`,
		logicalFileID,
	)
	return err
}

func ensurePhysicalFileForPathDefaultPolicyWithTx(ctx context.Context, dbconn *sql.DB, tx *sql.Tx, path string, logicalFileID int64, meta physicalFileMetadata) (bool, error) {
	selectQuery := db.QueryWithOptionalForUpdate(dbconn, `
		SELECT path, logical_file_id, mode, mtime, uid, gid, is_metadata_complete
		FROM physical_file
		WHERE path = $1
	`)

	var existing physicalFileRecord
	err := tx.QueryRowContext(ctx, selectQuery, path).Scan(
		&existing.Path,
		&existing.LogicalFileID,
		&existing.Mode,
		&existing.MTime,
		&existing.UID,
		&existing.GID,
		&existing.IsMetadataComplete,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			if err := insertPhysicalFile(ctx, tx, path, logicalFileID, meta); err != nil {
				return false, fmt.Errorf("insert physical_file %q: %w", path, err)
			}
			if err := incrementLogicalFileRefCount(ctx, tx, logicalFileID); err != nil {
				return false, fmt.Errorf("increment logical_file.ref_count id=%d: %w", logicalFileID, err)
			}
			return false, nil
		}
		return false, err
	}

	if existing.LogicalFileID != logicalFileID {
		return false, &physicalPathConflictError{
			Path:                  path,
			ExistingLogicalFileID: existing.LogicalFileID,
			NewLogicalFileID:      logicalFileID,
		}
	}

	if err := updatePhysicalFile(ctx, tx, path, logicalFileID, meta); err != nil {
		return true, fmt.Errorf("update physical_file metadata for %q: %w", path, err)
	}

	return true, nil
}

func ensurePhysicalFileForPathWithPolicyWithTx(ctx context.Context, dbconn *sql.DB, tx *sql.Tx, path string, logicalFileID int64, meta physicalFileMetadata, replace bool) (bool, error) {
	alreadyMapped, err := ensurePhysicalFileForPathDefaultPolicyWithTx(ctx, dbconn, tx, path, logicalFileID, meta)
	if err == nil {
		return alreadyMapped, nil
	}

	if !replace {
		return false, err
	}

	var conflictErr *physicalPathConflictError
	if !errors.As(err, &conflictErr) {
		return false, err
	}

	if err := replacePhysicalFileLogicalTargetTx(ctx, dbconn, tx, path, logicalFileID, meta); err != nil {
		return false, err
	}

	return false, nil
}

func replacePhysicalFileLogicalTargetTx(ctx context.Context, dbconn *sql.DB, tx *sql.Tx, path string, newLogicalFileID int64, meta physicalFileMetadata) error {
	selectQuery := db.QueryWithOptionalForUpdate(dbconn, `SELECT logical_file_id FROM physical_file WHERE path = $1`)

	var oldLogicalFileID int64
	if err := tx.QueryRowContext(ctx, selectQuery, path).Scan(&oldLogicalFileID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("physical_file row not found for replace path %q", path)
		}
		return err
	}

	if oldLogicalFileID == newLogicalFileID {
		if err := updatePhysicalFile(ctx, tx, path, newLogicalFileID, meta); err != nil {
			return fmt.Errorf("update physical_file metadata for %q: %w", path, err)
		}
		return nil
	}

	// TODO(v1.2+): Prefer an in-place UPDATE logical_file_id once we fully
	// understand and resolve SQLite behavior observed in tests where UPDATE did
	// not persist the logical target reliably in this replace path.
	if _, err := tx.ExecContext(ctx, `DELETE FROM physical_file WHERE path = $1`, path); err != nil {
		return fmt.Errorf("delete physical_file row for replace path %q: %w", path, err)
	}
	if err := insertPhysicalFile(ctx, tx, path, newLogicalFileID, meta); err != nil {
		return fmt.Errorf("insert replacement physical_file row for %q: %w", path, err)
	}

	if err := decrementLogicalFileRefCount(ctx, tx, oldLogicalFileID); err != nil {
		return fmt.Errorf("decrement old logical_file.ref_count id=%d: %w", oldLogicalFileID, err)
	}
	if err := incrementLogicalFileRefCount(ctx, tx, newLogicalFileID); err != nil {
		return fmt.Errorf("increment new logical_file.ref_count id=%d: %w", newLogicalFileID, err)
	}

	return nil
}

func nullableTime(t time.Time) sql.NullTime {
	if t.IsZero() {
		return sql.NullTime{}
	}
	return sql.NullTime{Time: t.UTC(), Valid: true}
}
