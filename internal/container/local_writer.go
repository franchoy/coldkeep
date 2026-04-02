package container

import (
	"database/sql"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"time"

	"github.com/franchoy/coldkeep/internal/chunk"
	"github.com/franchoy/coldkeep/internal/db"
	"github.com/lib/pq"
)

var ErrContainerLockContention = errors.New("container row lock contention")

var defaultLockRetryAttempts = 8

// defaultLockRetryBaseWait is the base duration for the first backoff interval.
// Subsequent intervals grow exponentially: baseWait * 2^attempt, capped at defaultLockRetryMaxWait.
var defaultLockRetryBaseWait = 10 * time.Millisecond

// defaultLockRetryMaxWait caps exponential growth to avoid unbounded sleep under sustained contention.
var defaultLockRetryMaxWait = 500 * time.Millisecond

// LocalPlacement describes where a payload was physically appended.
type LocalPlacement struct {
	ContainerID      int64
	Filename         string
	Offset           int64
	StoredSize       int64
	NewContainerSize int64
	Rotated          bool
	PreviousID       int64
	PreviousFilename string
	PreviousSize     int64
	Full             bool
}

type LocalWriter struct {
	containers      int
	lastContainerID int64
	dir             string
	maxSize         int64

	// dbconn is used to commit a durable sealing marker before physical
	// finalization (rotation and full-container paths). May be nil in tests
	// or simulated mode — the marking step is skipped when nil.
	dbconn *sql.DB

	hasActive    bool
	active       ActiveContainer
	activeSize   int64
	activeID     int64
	activeFile   string
	activeHandle Container

	// pendingAppend is true when payload bytes have been physically written to the
	// container file but the enclosing DB transaction has not yet committed.
	// Canonical append lifecycle state machine contract:
	// internal/storage/store.go (Append lifecycle state machine).
	// RollbackLastAppend uses prevAppendSize/prevAppendFile to truncate the file
	// back to its pre-write offset if the transaction is rolled back or fails to commit.
	// Invariant: after AppendPayload succeeds, caller must eventually resolve this
	// pending state via exactly one terminal path: RollbackLastAppend (rollback path)
	// or AcknowledgeAppendCommitted (commit path).
	pendingAppend  bool
	prevAppendSize int64
	prevAppendFile string
}

func NewLocalWriter(maxSize int64) *LocalWriter {
	return NewLocalWriterWithDir(ContainersDir, maxSize)
}

func NewLocalWriterWithDir(dir string, maxSize int64) *LocalWriter {
	return NewLocalWriterWithDirAndDB(dir, maxSize, nil)
}

// NewLocalWriterWithDirAndDB creates a LocalWriter that commits a durable
// sealing marker (via dbconn) before physical finalization of each container.
// Passing a non-nil dbconn is required for the full sealing-safety guarantee;
// passing nil is still correct but skips the pre-finalization DB marker.
func NewLocalWriterWithDirAndDB(dir string, maxSize int64, dbconn *sql.DB) *LocalWriter {
	if dir == "" {
		dir = ContainersDir
	}
	// Best-effort: pre-create directory to reduce first-write surprises.
	_ = os.MkdirAll(dir, 0755)
	if maxSize <= ContainerHdrLen {
		maxSize = GetContainerMaxSize()
	}

	return &LocalWriter{
		dir:     dir,
		maxSize: maxSize,
		dbconn:  dbconn,
	}
}

func (w *LocalWriter) WriteChunk(c chunk.Info) error {
	_ = c
	return fmt.Errorf("WriteChunk(chunk.Info) is not supported by LocalWriter; use AppendPayload(tx, payload) instead")

}

// AppendPayload appends already-encoded payload bytes to the active local container.
// DB lifecycle decisions (size update/seal/chunk linking) remain outside this writer.
// If there is no active container (including after FinalizeContainer), this method lazily opens one.
// Canonical append lifecycle state machine contract:
// internal/storage/store.go (Append lifecycle state machine).
// Contract: after successful return, caller must later call either
// RollbackLastAppend() if commit does not succeed, or AcknowledgeAppendCommitted()
// exactly once after a successful commit.
func (w *LocalWriter) AppendPayload(tx db.DBTX, payload []byte) (LocalPlacement, error) {
	if len(payload) == 0 {
		return LocalPlacement{}, fmt.Errorf("payload is empty")
	}
	maxPayload := w.maxSize - ContainerHdrLen
	if maxPayload <= 0 || int64(len(payload)) > maxPayload {
		return LocalPlacement{}, fmt.Errorf("payload too large: %d bytes (max %d)", len(payload), maxPayload)
	}

	if err := w.ensureActive(tx); err != nil {
		return LocalPlacement{}, fmt.Errorf("ensure active container: %w", err)
	}

	rotated := false
	var previousID int64
	var previousFilename string
	var previousSize int64
	if w.activeSize+int64(len(payload)) > w.maxSize {
		previousID = w.activeID
		previousFilename = w.activeFile
		previousSize = w.activeSize

		// Mark sealing inside the transaction that already owns the container row.
		if _, err := tx.Exec(`UPDATE container SET sealing = TRUE WHERE id = $1`, previousID); err != nil {
			return LocalPlacement{}, fmt.Errorf("mark rotation container %d sealing: %w", previousID, err)
		}

		if err := w.finalizePhysicalOnly(); err != nil {
			retireErr := w.retireContainer(previousID)
			if retireErr != nil {
				return LocalPlacement{}, errors.Join(fmt.Errorf("rotate finalize active container: %w", err), fmt.Errorf("retire container %d after finalize failure: %w", previousID, retireErr))
			}
			return LocalPlacement{}, fmt.Errorf("rotate finalize active container: %w", err)
		}
		w.clearActive()
		if err := w.ensureActiveExcluding(tx, previousID); err != nil {
			return LocalPlacement{}, fmt.Errorf("rotate ensure new active container: %w", err)
		}
		rotated = true
	}

	// Lock the container row that will actually receive the append payload.
	if err := lockContainerRowNowaitWithRetry(tx, w.activeID, defaultLockRetryAttempts, defaultLockRetryBaseWait); err != nil {
		return LocalPlacement{}, err
	}

	// Record pre-write state for rollback; pendingAppend is set to true only after
	// the physical write succeeds so that lock-contention retries (which return before
	// reaching this point) never corrupt prevAppend* from a previously committed write.
	w.pendingAppend = false
	w.prevAppendSize = w.activeSize
	w.prevAppendFile = w.activeFile

	offset, err := w.activeHandle.Append(payload)
	if err != nil {
		containerID := w.activeID
		retireErr := w.RetireActiveContainer()
		if retireErr != nil {
			return LocalPlacement{}, errors.Join(fmt.Errorf("append payload to container %d: %w", containerID, err), fmt.Errorf("retire active container %d after append failure: %w", containerID, retireErr))
		}
		return LocalPlacement{}, fmt.Errorf("append payload to container %d: %w", containerID, err)
	}

	// Make payload durable before it becomes visible in committed DB metadata.
	if err := w.activeHandle.Sync(); err != nil {
		containerID := w.activeID
		if truncErr := w.activeHandle.Truncate(w.prevAppendSize); truncErr != nil {
			retireErr := w.RetireActiveContainer()
			if retireErr != nil {
				return LocalPlacement{}, errors.Join(
					fmt.Errorf("sync payload in container %d: %w", containerID, err),
					fmt.Errorf("rollback append in container %d: %w", containerID, truncErr),
					fmt.Errorf("retire active container %d after sync+rollback failure: %w", containerID, retireErr),
				)
			}
			return LocalPlacement{}, errors.Join(
				fmt.Errorf("sync payload in container %d: %w", containerID, err),
				fmt.Errorf("rollback append in container %d: %w", containerID, truncErr),
			)
		}

		// Truncate succeeded, but sync failed: retire this container to avoid reuse.
		retireErr := w.RetireActiveContainer()
		if retireErr != nil {
			return LocalPlacement{}, errors.Join(
				fmt.Errorf("sync payload in container %d: %w", containerID, err),
				fmt.Errorf("retire active container %d after sync failure: %w", containerID, retireErr),
			)
		}
		return LocalPlacement{}, fmt.Errorf("sync payload in container %d: %w", containerID, err)
	}

	w.pendingAppend = true

	newSize := offset + int64(len(payload))
	w.activeSize = newSize

	return LocalPlacement{
		ContainerID:      w.activeID,
		Filename:         w.activeFile,
		Offset:           offset,
		StoredSize:       int64(len(payload)),
		NewContainerSize: newSize,
		Rotated:          rotated,
		PreviousID:       previousID,
		PreviousFilename: previousFilename,
		PreviousSize:     previousSize,
		Full:             newSize >= w.maxSize,
	}, nil

}

func lockContainerRowNowaitWithRetry(tx db.DBTX, containerID int64, attempts int, baseWait time.Duration) error {
	for attempt := 0; attempt < attempts; attempt++ {
		_, err := tx.Exec(`SELECT id FROM container WHERE id = $1 FOR UPDATE NOWAIT`, containerID)
		if err == nil {
			return nil
		}
		if !isLockNotAvailable(err) {
			return err
		}
		if attempt < attempts-1 {
			backoff := baseWait * (1 << uint(attempt))
			if backoff > defaultLockRetryMaxWait {
				backoff = defaultLockRetryMaxWait
			}
			jitter := time.Duration(rand.Int63n(int64(baseWait)))
			time.Sleep(backoff + jitter)
		}
	}

	return fmt.Errorf("%w for container %d after %d attempts", ErrContainerLockContention, containerID, attempts)
}

func isLockNotAvailable(err error) bool {
	var pqErr *pq.Error
	if errors.As(err, &pqErr) {
		return string(pqErr.Code) == "55P03"
	}
	return false
}

// ActiveContainerState returns the currently opened local container state, if any.
func (w *LocalWriter) ActiveContainerState() (ActiveContainer, int64, bool) {
	if !w.hasActive {
		return ActiveContainer{}, 0, false
	}
	return w.active, w.activeSize, true

}

func (w *LocalWriter) ensureActive(tx db.DBTX) error {
	return w.ensureActiveExcluding(tx, 0)
}

func (w *LocalWriter) ensureActiveExcluding(tx db.DBTX, excludeID int64) error {
	if w.hasActive {
		return nil
	}
	if err := os.MkdirAll(w.dir, 0755); err != nil {
		return fmt.Errorf("ensure container directory %s: %w", w.dir, err)
	}

	var ac ActiveContainer
	var err error
	if w.dbconn != nil {
		ac, err = getOrCreateOpenContainerInDirExcluding(tx, w.dbconn, w.dir, excludeID)
	} else {
		ac, err = GetOrCreateOpenContainerInDirExcluding(tx, w.dir, excludeID)
	}
	if err != nil {
		return fmt.Errorf("get or create open container in %s: %w", w.dir, err)
	}

	w.hasActive = true
	w.active = ac
	w.activeID = ac.ID
	w.activeFile = ac.Filename
	w.activeHandle = ac.Container
	w.activeSize = ac.Container.Size()
	if w.activeSize < ContainerHdrLen {
		w.activeSize = ContainerHdrLen
	}
	if w.activeID != w.lastContainerID {
		w.containers++
		w.lastContainerID = w.activeID
	}

	return nil

}

func (w *LocalWriter) finalizePhysicalOnly() error {
	if !w.hasActive {
		return nil
	}
	if w.activeHandle == nil {
		return nil
	}

	if err := w.activeHandle.Sync(); err != nil {
		return fmt.Errorf("sync container %d: %w", w.activeID, err)
	}

	if err := w.activeHandle.Close(); err != nil {
		return fmt.Errorf("close container %d: %w", w.activeID, err)
	}

	return nil

}

func (w *LocalWriter) clearActive() {
	w.hasActive = false
	w.active = ActiveContainer{}
	w.activeSize = 0
	w.activeID = 0
	w.activeFile = ""
	w.activeHandle = nil

}

// FinalizeContainer performs physical sync/close for the active container and clears
// local active state. If physical finalization fails, the container is retired and
// quarantined before returning so no future writes can reuse a potentially unsafe file.
func (w *LocalWriter) FinalizeContainer() error {
	if err := w.finalizePhysicalOnly(); err != nil {
		containerID := w.activeID
		retireErr := w.RetireActiveContainer()
		if retireErr != nil {
			return errors.Join(err, fmt.Errorf("retire active container %d after finalize failure: %w", containerID, retireErr))
		}
		return err
	}
	w.clearActive()
	return nil
}

// AcknowledgeAppendCommitted clears the rollback bookkeeping after the enclosing
// DB transaction has successfully committed. This completes the commit path of the
// state machine: pendingAppend and the pre-write size/file fields are zeroed so a
// future rollback call cannot accidentally truncate already-committed bytes.
// Must be called exactly once after each successful commit that followed an
// AppendPayload success. Safe to call when no append is pending (no-op).
func (w *LocalWriter) AcknowledgeAppendCommitted() {
	w.pendingAppend = false
	w.prevAppendSize = 0
	w.prevAppendFile = ""
}

// RollbackLastAppend truncates the active container file back to its pre-append
// offset when the enclosing DB transaction was rolled back or failed to commit.
// Safe to call even if no append is pending (no-op). After a successful rollback
// the writer's active state is reset so the next AppendPayload selects a fresh
// open container from the database.
// If rollback itself fails, caller must retire/quarantine the active container.
func (w *LocalWriter) RollbackLastAppend() error {
	if !w.pendingAppend {
		return nil
	}
	w.pendingAppend = false

	filename := w.prevAppendFile
	target := w.prevAppendSize

	if w.hasActive && w.activeHandle != nil && w.activeFile == filename {
		// File handle is still open. Truncate via the handle, then close and reset
		// so the next write does a fresh DB lookup (the DB row may have been rolled back).
		truncErr := w.activeHandle.Truncate(target)
		_ = w.activeHandle.Close()
		w.clearActive()
		if truncErr != nil {
			return fmt.Errorf("rollback append: truncate container %s to %d: %w", filename, target, truncErr)
		}
		if filename != "" {
			fullPath := filepath.Join(w.dir, filename)
			if info, statErr := os.Stat(fullPath); statErr == nil {
				if info.Size() != target {
					return fmt.Errorf("rollback append: truncate verification failed for %s: expected %d bytes, got %d", fullPath, target, info.Size())
				}
			} else if !os.IsNotExist(statErr) {
				return fmt.Errorf("rollback append: stat container %s after truncate: %w", fullPath, statErr)
			}
		}
	} else if !w.hasActive && filename != "" {
		// FinalizeContainer was already called for a full container: the file handle is
		// closed but the file exists on disk. Truncate by path.
		fullPath := filepath.Join(w.dir, filename)
		if err := os.Truncate(fullPath, target); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("rollback append: truncate closed container %s to %d: %w", fullPath, target, err)
		}
		if info, statErr := os.Stat(fullPath); statErr == nil {
			if info.Size() != target {
				return fmt.Errorf("rollback append: truncate verification failed for %s: expected %d bytes, got %d", fullPath, target, info.Size())
			}
		} else if !os.IsNotExist(statErr) {
			return fmt.Errorf("rollback append: stat closed container %s after truncate: %w", fullPath, statErr)
		}
	}
	// If the active file was switched to a different name (shouldn't happen within
	// one AppendPayload call), there is nothing reliable to truncate.
	return nil
}

func (w *LocalWriter) ContainerCount() int {
	return w.containers
}

// MarkSealingForContainer commits sealing=TRUE for the given container ID in a
// short independent transaction. Safe to call when dbconn is nil (no-op).
func (w *LocalWriter) MarkSealingForContainer(containerID int64) error {
	if w.dbconn == nil {
		return nil
	}
	return MarkContainerSealing(w.dbconn, containerID)
}

func (w *LocalWriter) BindDB(dbconn *sql.DB) {
	if dbconn == nil {
		return
	}
	w.dbconn = dbconn
}

// RetireActiveContainer closes current handles, clears pending append state, and
// marks the DB row quarantined to prevent future reuse.
func (w *LocalWriter) RetireActiveContainer() error {
	if !w.hasActive {
		return nil
	}
	return w.retireContainer(w.activeID)
}

func (w *LocalWriter) retireContainer(containerID int64) error {
	var retireErr error
	if w.activeHandle != nil {
		if err := w.activeHandle.Close(); err != nil {
			retireErr = err
		}
	}
	w.pendingAppend = false
	w.prevAppendFile = ""
	w.prevAppendSize = 0
	w.clearActive()
	if err := QuarantineContainer(w.dbconn, containerID); err != nil {
		if retireErr != nil {
			return errors.Join(retireErr, err)
		}
		return err
	}
	return retireErr
}

// DB returns the underlying *sql.DB held by this writer (may be nil).
// Used when cloning a writer per worker to propagate the DB connection.
func (w *LocalWriter) DB() *sql.DB {
	return w.dbconn
}

func (w *LocalWriter) Dir() string {
	return w.dir
}

func (w *LocalWriter) MaxSize() int64 {
	return w.maxSize
}
