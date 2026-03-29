package container

import "github.com/franchoy/coldkeep/internal/chunk"

type LocalWriter struct {
	containers int
	dir        string
}

func NewLocalWriter(maxSize int64) *LocalWriter {
	return NewLocalWriterWithDir(ContainersDir, maxSize)
}

func NewLocalWriterWithDir(dir string, maxSize int64) *LocalWriter {
	_ = maxSize
	return &LocalWriter{containers: 1, dir: dir}
}

func (w *LocalWriter) WriteChunk(c chunk.Info) error {
	// existing write logic

	return nil
}

func (w *LocalWriter) FinalizeContainer() error {
	// existing seal logic
	return nil
}

func (w *LocalWriter) ContainerCount() int {
	if w.containers == 0 {
		return 1
	}
	return w.containers
}
