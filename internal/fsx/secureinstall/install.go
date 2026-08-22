// Package secureinstall atomically installs restored bytes at one exact local
// destination while retaining the parent and temporary object identities.
package secureinstall

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"
)

var (
	ErrDestinationExists          = errors.New("secure install destination exists")
	ErrAtomicNoReplaceUnsupported = errors.New("secure install atomic no-replace unsupported")
	ErrParentChanged              = errors.New("secure install retained parent changed")
	ErrInvalidState               = errors.New("secure install invalid lifecycle state")
)

type Metadata struct {
	Mode       *os.FileMode
	ModifiedAt *time.Time
	UID        *int
	GID        *int
	Strict     bool
}

type Request struct {
	Destination string
	TrustedRoot string
	Overwrite   bool
	Metadata    Metadata
}

type Warning struct {
	Operation string
	Detail    string
}

type Result struct {
	Destination string
	Warnings    []Warning
}

type nativePending interface {
	writer() *os.File
	publish(overwrite bool) error
	applyMetadata(Metadata) []metadataFailure
	abort() error
}

type metadataFailure struct {
	operation string
	err       error
}

type Pending struct {
	request      Request
	native       nativePending
	writerClosed bool
	published    bool
	finished     bool
}

func Begin(request Request) (*Pending, error) {
	normalized, err := normalizeRequest(request)
	if err != nil {
		return nil, err
	}
	native, err := beginPlatform(normalized)
	if err != nil {
		return nil, err
	}
	return &Pending{request: normalized, native: native}, nil
}

func (p *Pending) Writer() io.Writer {
	if p == nil || p.native == nil || p.writerClosed || p.finished {
		return nil
	}
	return p.native.writer()
}

func (p *Pending) SyncAndCloseWriter() error {
	if p == nil || p.native == nil || p.writerClosed || p.finished {
		return ErrInvalidState
	}
	writer := p.native.writer()
	if writer == nil {
		return ErrInvalidState
	}
	if err := writer.Sync(); err != nil {
		return fmt.Errorf("secure install sync temporary file: %w", err)
	}
	if err := writer.Close(); err != nil {
		return fmt.Errorf("secure install close temporary writer: %w", err)
	}
	p.writerClosed = true
	return nil
}

func (p *Pending) Publish() (Result, error) {
	if p == nil || p.native == nil || !p.writerClosed || p.finished {
		return Result{}, ErrInvalidState
	}
	if err := p.native.publish(p.request.Overwrite); err != nil {
		return Result{}, err
	}
	p.published = true

	failures := p.native.applyMetadata(p.request.Metadata)
	result := metadataResult(p.request.Destination, failures)
	closeErr := p.native.abort()
	p.finished = true
	return p.finishPublish(result, failures, closeErr)
}

func metadataResult(destination string, failures []metadataFailure) Result {
	result := Result{Destination: destination}
	for _, failure := range failures {
		result.Warnings = append(result.Warnings, Warning{
			Operation: failure.operation,
			Detail:    failure.err.Error(),
		})
	}
	return result
}

func (p *Pending) finishPublish(result Result, failures []metadataFailure, closeErr error) (Result, error) {
	if len(failures) > 0 && p.request.Metadata.Strict {
		return result, errors.Join(metadataFailuresError(p.request.Destination, failures), closeErr)
	}
	if closeErr != nil {
		return result, closeErr
	}
	return result, nil
}

func (p *Pending) Abort() error {
	if p == nil || p.native == nil || p.finished {
		return nil
	}
	err := p.native.abort()
	p.finished = true
	return err
}

func normalizeRequest(request Request) (Request, error) {
	destination := strings.TrimSpace(request.Destination)
	trustedRoot := strings.TrimSpace(request.TrustedRoot)
	if err := validateRequestPaths(destination, trustedRoot); err != nil {
		return Request{}, err
	}
	absDestination, err := absoluteInstallPath(destination, "destination")
	if err != nil {
		return Request{}, err
	}
	absRoot, err := absoluteInstallPath(trustedRoot, "trusted root")
	if err != nil {
		return Request{}, err
	}
	if err := validateDestinationContainment(absRoot, absDestination); err != nil {
		return Request{}, err
	}
	request.Destination = filepath.Clean(absDestination)
	request.TrustedRoot = filepath.Clean(absRoot)
	return request, nil
}

func validateRequestPaths(destination, trustedRoot string) error {
	if destination == "" || trustedRoot == "" {
		return fmt.Errorf("secure install destination and trusted root are required")
	}
	if strings.HasSuffix(destination, "/") || strings.HasSuffix(destination, string(os.PathSeparator)) {
		return fmt.Errorf("secure install destination must name an exact file")
	}
	return nil
}

func absoluteInstallPath(path, label string) (string, error) {
	absPath, err := filepath.Abs(path)
	if err != nil {
		return "", fmt.Errorf("secure install resolve %s: %w", label, err)
	}
	return absPath, nil
}

func validateDestinationContainment(absRoot, absDestination string) error {
	rel, err := filepath.Rel(absRoot, absDestination)
	if err != nil || rel == "." || rel == ".." || strings.HasPrefix(rel, ".."+string(os.PathSeparator)) || filepath.IsAbs(rel) {
		return fmt.Errorf("secure install destination escapes trusted root")
	}
	return nil
}

func temporaryName() (string, error) {
	var entropy [12]byte
	if _, err := rand.Read(entropy[:]); err != nil {
		return "", fmt.Errorf("secure install generate temporary name: %w", err)
	}
	return ".coldkeep-restore-" + hex.EncodeToString(entropy[:]), nil
}

func nearestExistingDirectory(path string) (string, error) {
	candidate := filepath.Clean(path)
	for {
		info, err := os.Stat(candidate)
		if err == nil {
			if !info.IsDir() {
				return "", fmt.Errorf("secure install retained root ancestor is not a directory: %s", candidate)
			}
			return candidate, nil
		}
		if !errors.Is(err, os.ErrNotExist) {
			return "", fmt.Errorf("secure install inspect retained root ancestor %s: %w", candidate, err)
		}
		parent := filepath.Dir(candidate)
		if parent == candidate {
			return "", fmt.Errorf("secure install found no existing directory ancestor for %s", path)
		}
		candidate = parent
	}
}

func metadataFailuresError(destination string, failures []metadataFailure) error {
	errs := make([]error, 0, len(failures))
	for _, failure := range failures {
		errs = append(errs, fmt.Errorf("%s: %w", failure.operation, failure.err))
	}
	return fmt.Errorf("secure install metadata for %q: %w", destination, errors.Join(errs...))
}
