package container

import (
	"fmt"
	"sync"

	"github.com/franchoy/coldkeep/internal/chunk"
	"github.com/franchoy/coldkeep/internal/db"
)

type SimulatedWriter struct {
	mu sync.Mutex

	maxSize         int64
	hasActive       bool
	currentID       int64
	currentFilename string
	currentSize     int64

	containers int
	nextSeq    int64
}

func NewSimulatedWriter(maxSize int64) *SimulatedWriter {
	if maxSize <= ContainerHdrLen {
		maxSize = GetContainerMaxSize()
	}

	return &SimulatedWriter{
		maxSize: maxSize,
	}
}

func (w *SimulatedWriter) WriteChunk(c chunk.Info) error {
	_ = c
	return nil
}

func (w *SimulatedWriter) FinalizeContainer() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.hasActive = false
	w.currentID = 0
	w.currentFilename = ""
	w.currentSize = 0
	return nil
}

func (w *SimulatedWriter) ContainerCount() int {
	w.mu.Lock()
	defer w.mu.Unlock()

	return w.containers
}

func (w *SimulatedWriter) MaxSize() int64 {
	w.mu.Lock()
	defer w.mu.Unlock()

	return w.maxSize
}

func (w *SimulatedWriter) AppendPayload(tx db.DBTX, payload []byte) (LocalPlacement, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if len(payload) == 0 {
		return LocalPlacement{}, fmt.Errorf("payload is empty")
	}
	maxPayload := w.maxSize - ContainerHdrLen
	if maxPayload <= 0 || int64(len(payload)) > maxPayload {
		return LocalPlacement{}, fmt.Errorf("payload too large: %d bytes (max %d)", len(payload), maxPayload)
	}

	if err := w.ensureActive(tx); err != nil {
		return LocalPlacement{}, err
	}

	rotated := false
	var previousID int64
	var previousFilename string
	var previousSize int64

	if w.currentSize+int64(len(payload)) > w.maxSize {
		previousID = w.currentID
		previousFilename = w.currentFilename
		previousSize = w.currentSize

		w.hasActive = false
		w.currentID = 0
		w.currentFilename = ""
		w.currentSize = 0

		if err := w.ensureActive(tx); err != nil {
			return LocalPlacement{}, err
		}
		rotated = true
	}

	offset := w.currentSize
	newSize := offset + int64(len(payload))
	w.currentSize = newSize

	return LocalPlacement{
		ContainerID:      w.currentID,
		Filename:         w.currentFilename,
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

func (w *SimulatedWriter) SealContainer(tx db.DBTX, containerID int64, _ string, _ string) error {
	_, err := tx.Exec(
		`UPDATE container SET sealed = TRUE, container_hash = $1 WHERE id = $2`,
		"SIMULATED",
		containerID,
	)
	return err
}

func (w *SimulatedWriter) ensureActive(tx db.DBTX) error {
	if w.hasActive {
		return nil
	}

	w.nextSeq++
	filename := fmt.Sprintf("sim_container_%06d.bin", w.nextSeq)

	var id int64
	if err := tx.QueryRow(
		`INSERT INTO container (filename, current_size, max_size, sealed)
		 VALUES ($1, $2, $3, FALSE)
		 RETURNING id`,
		filename,
		ContainerHdrLen,
		w.maxSize,
	).Scan(&id); err != nil {
		return fmt.Errorf("create simulated container row: %w", err)
	}

	w.hasActive = true
	w.currentID = id
	w.currentFilename = filename
	w.currentSize = ContainerHdrLen
	w.containers++

	return nil
}
