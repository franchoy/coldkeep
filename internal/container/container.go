package container

import (
	"database/sql"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/franchoy/coldkeep/internal/db"
	"github.com/franchoy/coldkeep/internal/utils_hash"
)

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

func OpenExistingContainer(readonly bool, path string, maxSize int64) (*FileContainer, error) {
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

	return &FileContainer{
		f:       f,
		offset:  stat.Size(),
		maxSize: maxSize,
	}, nil
}

func (c *FileContainer) Append(data []byte) (int64, error) {
	if c.f == nil {
		return 0, fmt.Errorf("container is closed")
	}

	if c.offset+int64(len(data)) > c.maxSize {
		return 0, fmt.Errorf("container full")
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

func GetOrCreateOpenContainer(db db.DBTX) (ActiveContainer, error) {
	return GetOrCreateOpenContainerInDir(db, ContainersDir)
}

func GetOrCreateOpenContainerInDir(db db.DBTX, containersDir string) (ActiveContainer, error) {
	containersDir = containersDirOrDefault(containersDir)

	var id int64
	var filename string
	var maxSize int64

	// 1 Try to find an existing open container
	err := db.QueryRow(`
		SELECT id, filename, max_size
		FROM container
		WHERE sealed = FALSE and quarantine = FALSE
		ORDER BY id
		LIMIT 1
		FOR UPDATE SKIP LOCKED
	`).Scan(&id, &filename, &maxSize)

	if err == nil {
		// Found existing open container
		fullPath := filepath.Join(containersDir, filename)

		container, err := OpenExistingContainer(false, fullPath, maxSize)
		if err != nil {
			return ActiveContainer{}, err
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

	filename = fmt.Sprintf("container_%d.bin", time.Now().UnixNano())

	// Insert DB row with current_size initialized to header size
	err = db.QueryRow(`
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

	f, err := os.OpenFile(fullPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0666)
	if err != nil {
		return ActiveContainer{}, err
	}
	closeOnError := true
	defer func() {
		if closeOnError {
			_ = f.Close()
		}
	}()

	// 4 Write V0 header
	if err := writeNewContainerHeader(f, containerMaxSize); err != nil {
		return ActiveContainer{}, err
	}

	// Ensure header is flushed
	if err := f.Sync(); err != nil {
		return ActiveContainer{}, err
	}
	//close file
	if err = f.Close(); err != nil {
		return ActiveContainer{}, err
	}
	closeOnError = false

	container, err := OpenExistingContainer(false, fullPath, containerMaxSize)
	if err != nil {
		return ActiveContainer{}, err
	}

	return ActiveContainer{
		ID:        id,
		Filename:  filename,
		Container: container,
		MaxSize:   containerMaxSize,
	}, nil
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

	// Compute file hash
	sumHex, err := utils_hash.ComputeFileHashHex(originalPath)
	if err != nil {
		return fmt.Errorf("compute container file hash: %w", err)
	}

	// Update DB
	_, err = tx.Exec(`
		UPDATE container
		SET sealed = TRUE,
			container_hash = $1
		WHERE id = $2
	`, sumHex, containerID)

	if err != nil {
		return fmt.Errorf("update/seal container failed: %w", err)
	}

	fmt.Printf("Container %d sealed successfully: %s\n", containerID, originalPath)
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
