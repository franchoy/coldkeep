package container

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/franchoy/coldkeep/internal/db"
	"github.com/franchoy/coldkeep/internal/utils_hash"
)

// ErrContainerFull is returned by Container.Append when the payload would exceed the container's max size.
var ErrContainerFull = errors.New("container full")

type BrokenOpenContainerError struct {
	ContainerID int64
	Err         error
}

func (e *BrokenOpenContainerError) Error() string {
	if e == nil {
		return "broken open container"
	}
	return fmt.Sprintf("open container %d: %v", e.ContainerID, e.Err)
}

func (e *BrokenOpenContainerError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

// --------------------------------------------------------------------------
// structures
// --------------------------------------------------------------------------

type FileContainer struct {
	f       *os.File
	offset  int64 // current write position
	maxSize int64 // maximum allowed size for this container (including header)
}

type Container interface {
	Append(data []byte) (offset int64, err error)
	ReadAt(offset int64, size int64) ([]byte, error)
	Size() int64
	Truncate(size int64) error
	Sync() error
	Close() error
}

type ActiveContainer struct {
	ID        int64
	Filename  string
	Container Container
	MaxSize   int64
}

// --------------------------------------------------------------------------
// api
// --------------------------------------------------------------------------

// openExistingContainer opens an existing container using the provided mode.
func openExistingContainer(readonly bool, path string, maxSize int64) (*FileContainer, error) {
	var f *os.File
	var err error
	if readonly {
		f, err = os.OpenFile(path, os.O_RDONLY, 0644)
	} else {
		f, err = os.OpenFile(path, os.O_RDWR, 0644)
	}
	if err != nil {
		return nil, err
	}

	stat, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, err
	}

	if _, err := readAndValidateContainerHeader(f); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("validate container header %s: %w", path, err)
	}

	return &FileContainer{
		f:       f,
		offset:  stat.Size(),
		maxSize: maxSize,
	}, nil
}

// OpenReadOnlyContainer opens an existing container in read-only mode.
//
// This wrapper avoids ambiguous boolean call sites like
// openExistingContainer(true, ...) and makes intent explicit.
func OpenReadOnlyContainer(path string, maxSize int64) (*FileContainer, error) {
	return openExistingContainer(true, path, maxSize)
}

// OpenWritableContainer opens an existing container in writable mode.
//
// This wrapper avoids ambiguous boolean call sites like
// openExistingContainer(false, ...) and makes intent explicit.
func OpenWritableContainer(path string, maxSize int64) (*FileContainer, error) {
	return openExistingContainer(false, path, maxSize)
}

func (c *FileContainer) Append(data []byte) (int64, error) {
	if c.f == nil {
		return 0, fmt.Errorf("container is closed")
	}

	if c.offset+int64(len(data)) > c.maxSize {
		return 0, ErrContainerFull
	}

	off := c.offset

	n, err := c.f.WriteAt(data, off)
	if err != nil {
		return 0, err
	}

	if n != len(data) {
		return 0, fmt.Errorf("partial write")
	}

	c.offset += int64(n)
	return off, nil
}

func (c *FileContainer) ReadAt(offset int64, size int64) ([]byte, error) {
	if c.f == nil {
		return nil, fmt.Errorf("container is closed")
	}

	buf := make([]byte, size)

	n, err := c.f.ReadAt(buf, offset)
	if err != nil && err != io.EOF {
		return nil, err
	}

	if int64(n) != size {
		return nil, fmt.Errorf("short read")
	}

	return buf, nil
}

func (c *FileContainer) Size() int64 {
	return c.offset
}

func (c *FileContainer) Truncate(size int64) error {
	if c.f == nil {
		return fmt.Errorf("container is closed")
	}
	if err := c.f.Truncate(size); err != nil {
		return err
	}
	c.offset = size
	return nil
}

func (c *FileContainer) Sync() error {
	if c.f == nil {
		return fmt.Errorf("container is closed")
	}

	return c.f.Sync()
}

func (c *FileContainer) Close() error {
	if c.f == nil {
		return nil
	}
	err := c.f.Close()
	c.f = nil
	return err
}

func containersDirOrDefault(dir string) string {
	if dir == "" {
		return ContainersDir
	}
	return dir
}

// --------------------------------------------------------------------------
// functions
// --------------------------------------------------------------------------

// newContainerFilename returns a collision-resistant filename by combining the
// current nanosecond timestamp with 8 random bytes. This prevents the
// container_filename_key unique constraint from being violated when multiple
// goroutines attempt to create a new container at the same instant.
func newContainerFilename() string {
	var rnd [8]byte
	if _, err := rand.Read(rnd[:]); err != nil {
		// Extremely unlikely; fall back to an extra timestamp component.
		return fmt.Sprintf("container_%d_%d.bin", time.Now().UnixNano(), time.Now().UnixNano())
	}
	return fmt.Sprintf("container_%d_%s.bin", time.Now().UnixNano(), hex.EncodeToString(rnd[:]))
}

func getOrCreateOpenContainerInDirExcluding(tx db.DBTX, dbconn *sql.DB, containersDir string, excludeID int64) (ActiveContainer, error) {
	containersDir = containersDirOrDefault(containersDir)

	var id int64
	var filename string
	var maxSize int64

	// 1 Try to find an existing open container.
	// During rotation we may need to skip the previously active container until
	// the caller seals it in the same transaction.
	var err error
	if excludeID > 0 {
		query := `
			SELECT id, filename, max_size
			FROM container
			WHERE sealed = FALSE AND sealing = FALSE AND quarantine = FALSE AND id <> $1
			ORDER BY id
			LIMIT 1
		`
		if dbconn != nil {
			query = db.QueryWithOptionalForUpdateSkipLocked(dbconn, query)
		} else {
			query += " FOR UPDATE SKIP LOCKED"
		}
		err = tx.QueryRow(query, excludeID).Scan(&id, &filename, &maxSize)
	} else {
		query := `
			SELECT id, filename, max_size
			FROM container
			WHERE sealed = FALSE AND sealing = FALSE AND quarantine = FALSE
			ORDER BY id
			LIMIT 1
		`
		if dbconn != nil {
			query = db.QueryWithOptionalForUpdateSkipLocked(dbconn, query)
		} else {
			query += " FOR UPDATE SKIP LOCKED"
		}
		err = tx.QueryRow(query).Scan(&id, &filename, &maxSize)
	}
	if err == nil {
		// Found existing open container
		fullPath := filepath.Join(containersDir, filename)

		container, err := OpenWritableContainer(fullPath, maxSize)
		if err != nil {
			return ActiveContainer{}, &BrokenOpenContainerError{ContainerID: id, Err: err}
		}

		return ActiveContainer{
			ID:        id,
			Filename:  filename,
			Container: container,
			MaxSize:   maxSize,
		}, nil
	}

	if err != sql.ErrNoRows {
		return ActiveContainer{}, err
	}

	// 2 No open container found → create new one

	filename = newContainerFilename()

	// Insert DB row with current_size initialized to header size
	err = tx.QueryRow(`
		INSERT INTO container (filename, current_size, max_size, sealed)
		VALUES ($1, $2, $3, FALSE)
		RETURNING id
	`, filename, ContainerHdrLen, containerMaxSize).Scan(&id)

	if err != nil {
		return ActiveContainer{}, err
	}

	// 3 Create physical file

	if err := os.MkdirAll(containersDir, 0755); err != nil {
		return ActiveContainer{}, err
	}

	fullPath := filepath.Join(containersDir, filename)
	retireNewContainer := func(openErr error) error {
		retireErr := QuarantineContainer(dbconn, id)
		removeErr := os.Remove(fullPath)
		var errs []error
		errs = append(errs, openErr)
		if retireErr != nil {
			errs = append(errs, fmt.Errorf("quarantine broken new container %d: %w", id, retireErr))
		}
		if removeErr != nil && !os.IsNotExist(removeErr) {
			errs = append(errs, fmt.Errorf("remove partial container file %s: %w", fullPath, removeErr))
		}
		return errors.Join(errs...)
	}

	f, err := os.OpenFile(fullPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0666)
	if err != nil {
		return ActiveContainer{}, retireNewContainer(err)
	}
	closeOnError := true
	defer func() {
		if closeOnError {
			_ = f.Close()
		}
	}()

	// 4 Write container header
	if err := writeNewContainerHeader(f, containerMaxSize); err != nil {
		return ActiveContainer{}, retireNewContainer(err)
	}

	// Ensure header is flushed
	if err := f.Sync(); err != nil {
		return ActiveContainer{}, retireNewContainer(err)
	}
	//close file
	if err = f.Close(); err != nil {
		return ActiveContainer{}, retireNewContainer(err)
	}
	closeOnError = false

	container, err := OpenWritableContainer(fullPath, containerMaxSize)
	if err != nil {
		return ActiveContainer{}, retireNewContainer(err)
	}

	return ActiveContainer{
		ID:        id,
		Filename:  filename,
		Container: container,
		MaxSize:   containerMaxSize,
	}, nil
}

func GetOrCreateOpenContainerInDirExcluding(db db.DBTX, containersDir string, excludeID int64) (ActiveContainer, error) {
	return getOrCreateOpenContainerInDirExcluding(db, nil, containersDir, excludeID)
}

func UpdateContainerSize(tx db.DBTX, containerID int64, newSize int64) error {
	_, err := tx.Exec(
		`UPDATE container SET current_size = $1 WHERE id = $2`,
		newSize,
		containerID,
	)
	return err
}

func SealContainer(tx db.DBTX, containerID int64, filename string) error {
	return SealContainerInDir(tx, containerID, filename, ContainersDir)
}

func SealContainerInDir(tx db.DBTX, containerID int64, filename string, containersDir string) error {
	containersDir = containersDirOrDefault(containersDir)

	originalPath := filepath.Join(containersDir, filename)

	info, err := os.Stat(originalPath)
	if err != nil {
		return fmt.Errorf("stat container file before seal: %w", err)
	}

	var currentSize int64
	if err := tx.QueryRow(`SELECT current_size FROM container WHERE id = $1`, containerID).Scan(&currentSize); err != nil {
		return fmt.Errorf("query container current_size before seal: %w", err)
	}

	physicalSize := info.Size()
	if physicalSize != currentSize {
		if physicalSize > currentSize {
			return fmt.Errorf("seal container %d: ghost bytes detected (physical=%d, db_current_size=%d)", containerID, physicalSize, currentSize)
		}
		return fmt.Errorf("seal container %d: truncated file detected (physical=%d, db_current_size=%d)", containerID, physicalSize, currentSize)
	}

	// Compute file hash
	sumHex, err := utils_hash.ComputeFileHashHex(originalPath)
	if err != nil {
		return fmt.Errorf("compute container file hash: %w", err)
	}

	// Update DB: mark sealed and clear the sealing-in-progress flag atomically.
	_, err = tx.Exec(`
		UPDATE container
		SET sealed = TRUE,
			sealing = FALSE,
			container_hash = $1
		WHERE id = $2
	`, sumHex, containerID)

	if err != nil {
		return fmt.Errorf("update/seal container failed: %w", err)
	}

	return nil
}

func QuarantineContainer(dbconn *sql.DB, containerID int64) error {
	if dbconn == nil || containerID <= 0 {
		return nil
	}

	ctx, cancel := db.NewOperationContext(context.Background())
	defer cancel()

	tx, err := dbconn.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin quarantine tx for container %d: %w", containerID, err)
	}
	if _, err = tx.ExecContext(ctx,
		`UPDATE container SET quarantine = TRUE, sealing = FALSE WHERE id = $1`,
		containerID,
	); err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("mark container %d quarantine: %w", containerID, err)
	}
	if err = tx.Commit(); err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("commit quarantine for container %d: %w", containerID, err)
	}
	return nil
}

func CheckContainerHashFile(id int, filename, storedHash string) error {
	return CheckContainerHashFileInDir(id, filename, storedHash, ContainersDir)
}

func CheckContainerHashFileInDir(id int, filename, storedHash string, containersDir string) error {
	containersDir = containersDirOrDefault(containersDir)
	containerPath := filepath.Join(containersDir, filename)

	computedHash, err := utils_hash.ComputeFileHashHex(containerPath)
	if err != nil {
		return fmt.Errorf("compute container file hash: %w", err)
	}

	// If stored hash is missing, fail verification explicitly.
	if len(storedHash) == 0 || storedHash == "null" || storedHash == "NULL" {
		return fmt.Errorf("container file hash is missing in db for container %d, calculated hash: %s", id, computedHash)
	}

	if computedHash != storedHash {
		return fmt.Errorf("container file hash mismatch for container %d: expected %s, got %s", id, storedHash, computedHash)
	}

	return nil
}
