package container

import "github.com/franchoy/coldkeep/internal/chunk"

type SimulatedWriter struct {
	currentSize int64
	maxSize     int64
	containers  int
}

func NewSimulatedWriter(maxSize int64) *SimulatedWriter {
	return &SimulatedWriter{
		maxSize:    maxSize,
		containers: 1, // start with first container
	}
}

func (w *SimulatedWriter) WriteChunk(c chunk.Info) error {
	if w.currentSize+c.Size > w.maxSize {
		w.containers++
		w.currentSize = 0
	}

	w.currentSize += c.Size
	return nil
}

func (w *SimulatedWriter) FinalizeContainer() error {
	return nil
}

func (w *SimulatedWriter) ContainerCount() int {
	return w.containers
}
