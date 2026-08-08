package main

import (
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
)

const coordinatedOutputSpoolPattern = "coldkeep-output-*"

// coordinatedOutputSpool keeps command stdout off heap until the repository
// Lease has been released and the CLI can decide whether the payload is safe
// to emit. It is transient CLI plumbing, not repository state.
type coordinatedOutputSpool struct {
	file *os.File
	path string

	cleanupOnce sync.Once
	cleanupErr  error
}

func newDefaultCoordinatedOutputSpool() (*coordinatedOutputSpool, error) {
	return newCoordinatedOutputSpool("")
}

func newCoordinatedOutputSpool(directory string) (*coordinatedOutputSpool, error) {
	file, err := os.CreateTemp(directory, coordinatedOutputSpoolPattern)
	if err != nil {
		return nil, fmt.Errorf("create coordinated command output spool: %w", err)
	}
	return &coordinatedOutputSpool{file: file, path: file.Name()}, nil
}

func (spool *coordinatedOutputSpool) capture(fn func() error) (outputDestination *os.File, err error) {
	if spool == nil || spool.file == nil {
		return nil, fmt.Errorf("capture coordinated command output: output spool is unavailable")
	}
	if fn == nil {
		return nil, fmt.Errorf("capture coordinated command output: operation is required")
	}

	stdoutRedirectMu.Lock()
	defer stdoutRedirectMu.Unlock()

	reader, writer, err := os.Pipe()
	if err != nil {
		return nil, fmt.Errorf("create coordinated command output pipe: %w", err)
	}
	outputDestination = os.Stdout
	os.Stdout = writer
	copyDone := make(chan error, 1)
	go func() {
		_, copyErr := io.Copy(spool.file, reader)
		closeReaderErr := reader.Close()
		if copyErr != nil {
			copyErr = fmt.Errorf("write coordinated command output spool: %w", copyErr)
		}
		if closeReaderErr != nil {
			closeReaderErr = fmt.Errorf("close coordinated command output pipe reader: %w", closeReaderErr)
		}
		copyDone <- errors.Join(copyErr, closeReaderErr)
	}()

	defer func() {
		os.Stdout = outputDestination
		closeWriterErr := writer.Close()
		copyErr := <-copyDone
		if closeWriterErr != nil {
			closeWriterErr = fmt.Errorf("close coordinated command output pipe writer: %w", closeWriterErr)
		}
		err = errors.Join(err, closeWriterErr, copyErr)
	}()

	return outputDestination, fn()
}

func (spool *coordinatedOutputSpool) replayTo(destination io.Writer) error {
	if spool == nil || spool.file == nil {
		return fmt.Errorf("replay coordinated command output: output spool is unavailable")
	}
	if destination == nil {
		return fmt.Errorf("replay coordinated command output: destination is required")
	}
	if _, err := spool.file.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("seek coordinated command output spool: %w", err)
	}
	if _, err := io.Copy(destination, spool.file); err != nil {
		return fmt.Errorf("replay coordinated command output spool: %w", err)
	}
	return nil
}

func (spool *coordinatedOutputSpool) cleanup() error {
	if spool == nil {
		return nil
	}
	spool.cleanupOnce.Do(func() {
		var closeErr error
		if spool.file != nil {
			if err := spool.file.Close(); err != nil {
				closeErr = fmt.Errorf("close coordinated command output spool: %w", err)
			}
		}

		var removeErr error
		if spool.path != "" {
			if err := os.Remove(spool.path); err != nil && !os.IsNotExist(err) {
				removeErr = fmt.Errorf("remove coordinated command output spool: %w", err)
			}
		}
		spool.cleanupErr = errors.Join(closeErr, removeErr)
	})
	return spool.cleanupErr
}
