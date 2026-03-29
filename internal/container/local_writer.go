package container

import (
	"fmt"
	"os"

	"github.com/franchoy/coldkeep/internal/chunk"
	"github.com/franchoy/coldkeep/internal/db"
)

// LocalPlacement describes where a payload was physically appended.
type LocalPlacement struct {
	ContainerID      int64
	Filename         string
	Offset           int64
	StoredSize       int64
	NewContainerSize int64
	Rotated          bool
	Full             bool
}

type LocalWriter struct {
	containers      int
	lastContainerID int64
	dir             string
	maxSize         int64

	hasActive    bool
	active       ActiveContainer
	activeSize   int64
	activeID     int64
	activeFile   string
	activeHandle Container
}

func NewLocalWriter(maxSize int64) *LocalWriter {
	return NewLocalWriterWithDir(ContainersDir, maxSize)
}

func NewLocalWriterWithDir(dir string, maxSize int64) *LocalWriter {
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
	}
}

func (w *LocalWriter) WriteChunk(c chunk.Info) error {
	_ = c
	return fmt.Errorf("WriteChunk(chunk.Info) is not supported by LocalWriter; use AppendPayload(tx, payload) instead")

}

// AppendPayload appends already-encoded payload bytes to the active local container.
// DB lifecycle decisions (size update/seal/chunk linking) remain outside this writer.
// If there is no active container (including after FinalizeContainer), this method lazily opens one.
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
	if w.activeSize+int64(len(payload)) > w.maxSize {
		if err := w.finalizePhysicalOnly(); err != nil {
			return LocalPlacement{}, fmt.Errorf("rotate finalize active container: %w", err)
		}
		w.clearActive()
		if err := w.ensureActive(tx); err != nil {
			return LocalPlacement{}, fmt.Errorf("rotate ensure new active container: %w", err)
		}
		rotated = true
	}

	offset, err := w.activeHandle.Append(payload)
	if err != nil {
		return LocalPlacement{}, fmt.Errorf("append payload to container %d: %w", w.activeID, err)
	}

	newSize := offset + int64(len(payload))
	w.activeSize = newSize

	return LocalPlacement{
		ContainerID:      w.activeID,
		Filename:         w.activeFile,
		Offset:           offset,
		StoredSize:       int64(len(payload)),
		NewContainerSize: newSize,
		Rotated:          rotated,
		Full:             newSize >= w.maxSize,
	}, nil

}

// ActiveContainerState returns the currently opened local container state, if any.
func (w *LocalWriter) ActiveContainerState() (ActiveContainer, int64, bool) {
	if !w.hasActive {
		return ActiveContainer{}, 0, false
	}
	return w.active, w.activeSize, true

}

func (w *LocalWriter) ensureActive(tx db.DBTX) error {
	if w.hasActive {
		return nil
	}
	if err := os.MkdirAll(w.dir, 0755); err != nil {
		return fmt.Errorf("ensure container directory %s: %w", w.dir, err)
	}

	ac, err := GetOrCreateOpenContainerInDir(tx, w.dir)
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

func (w *LocalWriter) FinalizeContainer() error {
	if err := w.finalizePhysicalOnly(); err != nil {
		return err
	}
	w.clearActive()
	return nil
}

func (w *LocalWriter) ContainerCount() int {
	return w.containers
}
