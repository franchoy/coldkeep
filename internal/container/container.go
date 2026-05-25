package container

import (
	"bytes"
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
	"github.com/franchoy/coldkeep/internal/fsx"
	"github.com/franchoy/coldkeep/internal/iodebug"
	"github.com/franchoy/coldkeep/internal/pathsafe"
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
	f             fsx.File
	offset        int64 // logical write position including buffered bytes
	persistedSize int64 // physically flushed size on disk
	maxSize       int64 // maximum allowed size for this container (including header)
	readonly      bool
	dirty         bool
	pending       bytes.Buffer
}

const fileContainerWriteBufferSize = 256 * 1024

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

// SafeContainerPath validates a container filename before joining it under a
// container root.
func SafeContainerPath(containersDir, filename string) (string, error) {
	if err := pathsafe.ValidateSafeFileName(filename); err != nil {
		return "", err
	}
	return filepath.Join(containersDir, filename), nil
}

// --------------------------------------------------------------------------
// api
// --------------------------------------------------------------------------

// openExistingContainer opens an existing container using the provided mode.
func openExistingContainer(readonly bool, path string, maxSize int64, fsys fsx.FS) (*FileContainer, error) {
	var f fsx.File
	var err error
	if readonly {
		f, err = fsys.OpenFile(path, os.O_RDONLY, 0644)
	} else {
		f, err = fsys.OpenFile(path, os.O_RDWR, 0644)
	}
	if err != nil {
		return nil, err
	}
	iodebug.IncContainerOpen()

	stat, err := fsys.Stat(path)
	if err != nil {
		_ = f.Close()
		return nil, err
	}

	if _, err := readAndValidateContainerHeader(f); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("validate container header %s: %w", path, err)
	}
	if !readonly {
		if _, err := f.Seek(stat.Size(), io.SeekStart); err != nil {
			_ = f.Close()
			return nil, fmt.Errorf("seek container end %s: %w", path, err)
		}
	}

	return &FileContainer{
		f:             f,
		offset:        stat.Size(),
		persistedSize: stat.Size(),
		maxSize:       maxSize,
		readonly:      readonly,
		dirty:         false,
	}, nil
}

// OpenReadOnlyContainer opens an existing container in read-only mode.
//
// This wrapper avoids ambiguous boolean call sites like
// openExistingContainer(true, ...) and makes intent explicit.
func OpenReadOnlyContainer(path string, maxSize int64) (*FileContainer, error) {
	return openExistingContainer(true, path, maxSize, fsx.Default())
}

// OpenWritableContainer opens an existing container in writable mode.
//
// This wrapper avoids ambiguous boolean call sites like
// openExistingContainer(false, ...) and makes intent explicit.
func OpenWritableContainer(path string, maxSize int64) (*FileContainer, error) {
	return openExistingContainer(false, path, maxSize, fsx.Default())
}

func (c *FileContainer) Append(data []byte) (int64, error) {
	if c.f == nil {
		return 0, fmt.Errorf("container is closed")
	}
	if c.readonly {
		return 0, fmt.Errorf("container is read-only")
	}

	if c.offset+int64(len(data)) > c.maxSize {
		return 0, ErrContainerFull
	}

	off := c.offset
	if len(data) >= fileContainerWriteBufferSize && c.pending.Len() == 0 {
		n, err := c.writeDirect(data)
		if err != nil {
			return 0, err
		}
		c.offset += int64(n)
	} else {
		if c.pending.Len()+len(data) > fileContainerWriteBufferSize {
			if err := c.flushPending(); err != nil {
				return 0, err
			}
		}
		n, err := c.pending.Write(data)
		if err != nil {
			return 0, err
		}
		if n != len(data) {
			return 0, fmt.Errorf("partial buffered write")
		}
		c.offset += int64(n)
	}

	iodebug.IncContainerAppend()
	return off, nil
}

func (c *FileContainer) ReadAt(offset int64, size int64) ([]byte, error) {
	if c.f == nil {
		return nil, fmt.Errorf("container is closed")
	}
	if !c.readonly {
		if err := c.flushPending(); err != nil {
			return nil, err
		}
	}

	buf := make([]byte, size)

	n, err := c.f.ReadAt(buf, offset)
	if err != nil && err != io.EOF {
		return nil, err
	}

	if int64(n) != size {
		return nil, fmt.Errorf("short read")
	}
	iodebug.AddBytesRead(int64(n))

	return buf, nil
}

func (c *FileContainer) Size() int64 {
	return c.offset
}

func (c *FileContainer) Truncate(size int64) error {
	if c.f == nil {
		return fmt.Errorf("container is closed")
	}
	if c.readonly {
		return fmt.Errorf("container is read-only")
	}

	if size < 0 {
		return fmt.Errorf("invalid truncate size %d", size)
	}

	if size < c.persistedSize {
		c.pending.Reset()
		if err := c.f.Truncate(size); err != nil {
			return err
		}
		if _, err := c.f.Seek(size, io.SeekStart); err != nil {
			return err
		}
		c.persistedSize = size
		c.offset = size
		c.dirty = true
		return nil
	}

	if size <= c.offset {
		pendingBytes := size - c.persistedSize
		if pendingBytes < 0 {
			pendingBytes = 0
		}
		if pendingBytes < int64(c.pending.Len()) {
			c.pending.Truncate(int(pendingBytes))
		}
		c.offset = size
		if size != c.persistedSize {
			c.dirty = true
		}
		return nil
	}

	c.pending.Reset()
	if err := c.f.Truncate(size); err != nil {
		return err
	}
	if _, err := c.f.Seek(size, io.SeekStart); err != nil {
		return err
	}
	c.persistedSize = size
	c.offset = size
	c.dirty = true
	return nil
}

func (c *FileContainer) Sync() error {
	if c.f == nil {
		return fmt.Errorf("container is closed")
	}
	if !c.readonly {
		if err := c.flushPending(); err != nil {
			return err
		}
	}
	if !c.dirty {
		return nil
	}

	if err := c.f.Sync(); err != nil {
		return err
	}
	c.dirty = false
	iodebug.IncFsync()
	return nil
}

func (c *FileContainer) Close() error {
	if c.f == nil {
		return nil
	}
	if !c.readonly {
		if err := c.flushPending(); err != nil {
			return err
		}
	}
	err := c.f.Close()
	c.f = nil
	if err == nil {
		iodebug.IncContainerClose()
	}
	return err
}

func (c *FileContainer) flushPending() error {
	if c == nil || c.f == nil || c.readonly || c.pending.Len() == 0 {
		return nil
	}
	for c.pending.Len() > 0 {
		n, err := c.f.Write(c.pending.Bytes())
		if n > 0 {
			c.persistedSize += int64(n)
			c.dirty = true
			iodebug.AddBytesWritten(int64(n))
			c.pending.Next(n)
		}
		if err != nil {
			return err
		}
		if n == 0 {
			return fmt.Errorf("partial write")
		}
	}
	return nil
}

func (c *FileContainer) writeDirect(data []byte) (int, error) {
	n, err := c.f.Write(data)
	if n > 0 {
		c.persistedSize += int64(n)
		c.dirty = true
		iodebug.AddBytesWritten(int64(n))
	}
	if err != nil {
		return n, err
	}
	if n != len(data) {
		return n, fmt.Errorf("partial write")
	}
	return n, nil
}

func (c *FileContainer) SetSize(size int64) {
	if c == nil {
		return
	}
	if size < 0 {
		size = 0
	}
	c.pending.Reset()
	c.offset = size
	c.persistedSize = size
	c.dirty = false
	if c.f != nil && !c.readonly {
		_, _ = c.f.Seek(size, io.SeekStart)
	}
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

func getOrCreateOpenContainerInDirExcluding(tx db.DBTX, dbconn *sql.DB, containersDir string, excludeID int64, fsys fsx.FS) (ActiveContainer, error) {
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
		fullPath, err := SafeContainerPath(containersDir, filename)
		if err != nil {
			return ActiveContainer{}, fmt.Errorf("invalid container filename %q: %w", filename, err)
		}

		container, err := openExistingContainer(false, fullPath, maxSize, fsys)
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

	if err := fsys.MkdirAll(containersDir, 0755); err != nil {
		return ActiveContainer{}, err
	}

	fullPath, err := SafeContainerPath(containersDir, filename)
	if err != nil {
		return ActiveContainer{}, fmt.Errorf("invalid container filename %q: %w", filename, err)
	}
	retireNewContainer := func(openErr error) error {
		retireErr := quarantineContainerInDirWithFS(dbconn, id, containersDir, fsys)
		removeErr := fsys.Remove(fullPath)
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

	f, err := fsys.OpenFile(fullPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0666)
	if err != nil {
		return ActiveContainer{}, retireNewContainer(err)
	}
	iodebug.IncContainerOpen()
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
	iodebug.IncFsync()
	//close file
	if err = f.Close(); err != nil {
		return ActiveContainer{}, retireNewContainer(err)
	}
	iodebug.IncContainerClose()
	closeOnError = false

	container, err := openExistingContainer(false, fullPath, containerMaxSize, fsys)
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
	return getOrCreateOpenContainerInDirExcluding(db, nil, containersDir, excludeID, fsx.Default())
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
	return sealContainerInDirWithFS(tx, containerID, filename, containersDir, fsx.Default())
}

func sealContainerInDirWithFS(tx db.DBTX, containerID int64, filename string, containersDir string, fsys fsx.FS) error {
	containersDir = containersDirOrDefault(containersDir)

	originalPath, err := SafeContainerPath(containersDir, filename)
	if err != nil {
		return fmt.Errorf("invalid container filename %q: %w", filename, err)
	}

	info, err := fsys.Stat(originalPath)
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
	return QuarantineContainerInDir(dbconn, containerID, ContainersDir)
}

func QuarantineContainerInDir(dbconn *sql.DB, containerID int64, containersDir string) error {
	return quarantineContainerInDirWithFS(dbconn, containerID, containersDir, fsx.Default())
}

func quarantineContainerInDirWithFS(dbconn *sql.DB, containerID int64, containersDir string, fsys fsx.FS) error {
	if dbconn == nil || containerID <= 0 {
		return nil
	}
	containersDir = containersDirOrDefault(containersDir)

	ctx, cancel := db.NewOperationContext(context.Background())
	defer cancel()
	backend := db.BackendFromDB(dbconn)

	selectFilenameQuery := `SELECT filename FROM container WHERE id = $1`
	updateQuarantineQuery := `UPDATE container SET quarantine = TRUE, sealing = FALSE WHERE id = $1`
	updateQuarantineWithSizeQuery := `UPDATE container SET quarantine = TRUE, sealing = FALSE, current_size = $2, max_size = $2 WHERE id = $1`
	if backend == db.BackendSQLite {
		selectFilenameQuery = `SELECT filename FROM container WHERE id = ?`
		updateQuarantineQuery = `UPDATE container SET quarantine = TRUE, sealing = FALSE WHERE id = ?`
		updateQuarantineWithSizeQuery = `UPDATE container SET quarantine = TRUE, sealing = FALSE, current_size = ?, max_size = ? WHERE id = ?`
	}

	var filename string
	if err := dbconn.QueryRowContext(ctx, selectFilenameQuery, containerID).Scan(&filename); err != nil {
		if err == sql.ErrNoRows {
			return nil
		}
		return fmt.Errorf("query container %d before quarantine: %w", containerID, err)
	}

	containerPath, err := SafeContainerPath(containersDir, filename)
	if err != nil {
		return fmt.Errorf("invalid container filename %q: %w", filename, err)
	}

	var updateQuery string
	var updateArgs []any
	if info, statErr := fsys.Stat(containerPath); statErr == nil {
		updateQuery = updateQuarantineWithSizeQuery
		if backend == db.BackendSQLite {
			updateArgs = []any{info.Size(), info.Size(), containerID}
		} else {
			updateArgs = []any{containerID, info.Size()}
		}
	} else if os.IsNotExist(statErr) {
		updateQuery = updateQuarantineQuery
		updateArgs = []any{containerID}
	} else {
		return fmt.Errorf("stat container %d before quarantine: %w", containerID, statErr)
	}

	result, err := dbconn.ExecContext(ctx, updateQuery, updateArgs...)
	if err != nil {
		return fmt.Errorf("mark container %d quarantine: %w", containerID, err)
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("rows affected while quarantining container %d: %w", containerID, err)
	}
	if rowsAffected == 0 {
		return nil
	}
	return nil
}

func CheckContainerHashFile(id int, filename, storedHash string) error {
	return CheckContainerHashFileInDir(id, filename, storedHash, ContainersDir)
}

func CheckContainerHashFileInDir(id int, filename, storedHash string, containersDir string) error {
	containersDir = containersDirOrDefault(containersDir)
	containerPath, err := SafeContainerPath(containersDir, filename)
	if err != nil {
		return fmt.Errorf("invalid container filename %q: %w", filename, err)
	}

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
