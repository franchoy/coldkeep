package container

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/franchoy/coldkeep/internal/db"
	"github.com/franchoy/coldkeep/internal/fsx"
	"github.com/franchoy/coldkeep/internal/utils_hash"
)

var (
	repairStagingOpenFile = func(path string, flag int, perm os.FileMode) (fsx.File, error) {
		return os.OpenFile(path, flag, perm)
	}
	repairStagingRemove   = os.Remove
	repairStagingSyncDir  = fsx.SyncDir
	repairStagingFileHash = utils_hash.ComputeFileHashHex
)

// RepairStagingContainer describes an attempt-owned, sealed container whose
// bytes already occupy their final immutable pathname. It has no active
// placement references until the repair publication transaction commits.
type RepairStagingContainer struct {
	ID            int64
	Filename      string
	Path          string
	PayloadOffset int64
	PhysicalSize  int64
	PhysicalHash  string
}

// CreateRepairStagingContainer creates one quarantined repair container. The
// physical file is exclusively created, synced, closed, and directory-synced
// before the catalog marks it DURABLE.
func CreateRepairStagingContainer(
	ctx context.Context,
	dbconn *sql.DB,
	attemptID int64,
	containersDir string,
	payload []byte,
	maxSize int64,
) (RepairStagingContainer, error) {
	if dbconn == nil || attemptID <= 0 {
		return RepairStagingContainer{}, fmt.Errorf("invalid repair staging identity")
	}
	if maxSize <= ContainerHdrLen || int64(len(payload)) > maxSize-ContainerHdrLen {
		return RepairStagingContainer{}, fmt.Errorf("repair payload does not fit immutable container: payload=%d max=%d", len(payload), maxSize)
	}
	if err := os.MkdirAll(containersDir, 0o755); err != nil {
		return RepairStagingContainer{}, fmt.Errorf("create repair container directory: %w", err)
	}

	var random [12]byte
	if _, err := rand.Read(random[:]); err != nil {
		return RepairStagingContainer{}, fmt.Errorf("generate repair container identity: %w", err)
	}
	filename := fmt.Sprintf("container_repair_%d_%s.bin", attemptID, hex.EncodeToString(random[:]))
	path, err := SafeContainerPath(containersDir, filename)
	if err != nil {
		return RepairStagingContainer{}, err
	}
	physicalSize := int64(ContainerHdrLen + len(payload))

	tx, err := dbconn.BeginTx(ctx, nil)
	if err != nil {
		return RepairStagingContainer{}, err
	}
	defer func() { _ = tx.Rollback() }()
	var containerID int64
	if err := tx.QueryRowContext(ctx,
		`INSERT INTO container (filename, current_size, max_size, sealed, sealing, quarantine)
		 VALUES ($1, $2, $3, FALSE, FALSE, TRUE)
		 RETURNING id`,
		filename, physicalSize, maxSize,
	).Scan(&containerID); err != nil {
		return RepairStagingContainer{}, fmt.Errorf("allocate repair container row: %w", err)
	}
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO store_repair_container
		 (attempt_id, container_id, physical_size, physical_hash, status)
		 VALUES ($1, $2, $3, '', 'ALLOCATED')`,
		attemptID, containerID, physicalSize,
	); err != nil {
		return RepairStagingContainer{}, fmt.Errorf("link repair container to attempt: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return RepairStagingContainer{}, fmt.Errorf("commit repair container allocation: %w", err)
	}

	cleanup := true
	defer func() {
		if cleanup {
			removeErr := repairStagingRemove(path)
			if removeErr == nil || os.IsNotExist(removeErr) {
				if syncErr := repairStagingSyncDir(filepath.Dir(path)); syncErr == nil {
					_, _ = dbconn.ExecContext(context.Background(), `DELETE FROM store_repair_container WHERE container_id = $1`, containerID)
					_, _ = dbconn.ExecContext(context.Background(), `DELETE FROM container WHERE id = $1`, containerID)
				}
			}
		}
	}()

	file, err := repairStagingOpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return RepairStagingContainer{}, fmt.Errorf("exclusively create repair container: %w", err)
	}
	closeWith := func(primary error) error {
		if closeErr := file.Close(); closeErr != nil {
			return errors.Join(primary, closeErr)
		}
		return primary
	}
	if err := writeNewContainerHeader(file, maxSize); err != nil {
		return RepairStagingContainer{}, closeWith(fmt.Errorf("write repair container header: %w", err))
	}
	if written, err := file.Write(payload); err != nil || written != len(payload) {
		if err == nil {
			err = errors.New("short repair container payload write")
		}
		return RepairStagingContainer{}, closeWith(err)
	}
	if err := file.Sync(); err != nil {
		return RepairStagingContainer{}, closeWith(fmt.Errorf("sync repair container: %w", err))
	}
	if err := file.Close(); err != nil {
		return RepairStagingContainer{}, fmt.Errorf("close repair container: %w", err)
	}
	if err := repairStagingSyncDir(filepath.Dir(path)); err != nil {
		return RepairStagingContainer{}, fmt.Errorf("sync repair container directory: %w", err)
	}
	physicalHash, err := repairStagingFileHash(path)
	if err != nil {
		return RepairStagingContainer{}, fmt.Errorf("hash durable repair container: %w", err)
	}

	result, err := dbconn.ExecContext(ctx,
		`UPDATE container
		 SET sealed = TRUE, sealing = FALSE, current_size = $1, container_hash = $2
		 WHERE id = $3 AND quarantine = TRUE`,
		physicalSize, physicalHash, containerID,
	)
	if err != nil {
		return RepairStagingContainer{}, fmt.Errorf("seal repair container row: %w", err)
	}
	if err := db.RequireExactlyOneRow(result, "seal repair staging container"); err != nil {
		return RepairStagingContainer{}, err
	}
	if _, err := dbconn.ExecContext(ctx,
		`UPDATE store_repair_container
		 SET physical_hash = $1, status = 'DURABLE'
		 WHERE attempt_id = $2 AND container_id = $3`,
		physicalHash, attemptID, containerID,
	); err != nil {
		return RepairStagingContainer{}, fmt.Errorf("mark repair container durable: %w", err)
	}

	cleanup = false
	return RepairStagingContainer{
		ID: containerID, Filename: filename, Path: path,
		PayloadOffset: ContainerHdrLen, PhysicalSize: physicalSize, PhysicalHash: physicalHash,
	}, nil
}
