package engine

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/franchoy/coldkeep/internal/snapshot"
)

type preparedSnapshotCreateRequest struct {
	snapshotID    string
	label         string
	parentID      string
	selectionBase string
	paths         []string
	snapshotType  SnapshotType
}

func (e *DefaultEngine) SnapshotCreate(ctx context.Context, req SnapshotCreateRequest) (_ SnapshotCreateResult, outErr error) {
	defer func() { outErr = TranslateError("snapshot_create", outErr) }()
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return SnapshotCreateResult{}, err
	}

	prepared, err := e.prepareSnapshotCreateRequest(req)
	if err != nil {
		return SnapshotCreateResult{}, err
	}

	opts := snapshot.SnapshotCreateOptions{
		ID:            prepared.snapshotID,
		Type:          string(prepared.snapshotType),
		SelectionBase: prepared.selectionBase,
		Paths:         prepared.paths,
	}
	if prepared.label != "" {
		label := prepared.label
		opts.Label = &label
	}
	if prepared.parentID != "" {
		parentID := prepared.parentID
		opts.ParentID = &parentID
	}

	result, err := snapshot.CreateSnapshotWithOptionsResult(ctx, e.config.DB, opts)
	if err != nil {
		return SnapshotCreateResult{}, err
	}

	return SnapshotCreateResult{
		SnapshotID:    result.SnapshotID,
		Type:          SnapshotType(result.Type),
		PathsCount:    result.PathsCount,
		FilesInserted: result.FilesInserted,
		Label:         result.Label,
		ParentID:      result.ParentID,
	}, nil
}

func (e *DefaultEngine) prepareSnapshotCreateRequest(req SnapshotCreateRequest) (preparedSnapshotCreateRequest, error) {
	paths := append([]string(nil), req.Paths...)
	if err := validateSnapshotCreateSelectionBase(req.SelectionBase); err != nil {
		return preparedSnapshotCreateRequest{}, TranslateErrorAs("snapshot_create", ErrorInvalidArgument, err)
	}
	snapshotID := strings.TrimSpace(req.ID)
	if snapshotID == "" {
		generatedID, err := e.generateSnapshotID()
		if err != nil {
			return preparedSnapshotCreateRequest{}, err
		}
		snapshotID = generatedID
	}

	label := strings.TrimSpace(req.Label)
	parentID := strings.TrimSpace(req.ParentID)
	snapshotType, err := deriveSnapshotCreateType(paths)
	if err != nil {
		return preparedSnapshotCreateRequest{}, err
	}
	if err := validateSnapshotCreateParent(snapshotID, parentID, len(paths) > 0); err != nil {
		return preparedSnapshotCreateRequest{}, TranslateErrorAs("snapshot_create", ErrorInvalidArgument, err)
	}
	if err := validateSnapshotCreatePaths(paths); err != nil {
		return preparedSnapshotCreateRequest{}, TranslateErrorAs("snapshot_create", ErrorInvalidArgument, err)
	}

	return preparedSnapshotCreateRequest{
		snapshotID:    snapshotID,
		label:         label,
		parentID:      parentID,
		selectionBase: req.SelectionBase,
		paths:         paths,
		snapshotType:  snapshotType,
	}, nil
}

func validateSnapshotCreateSelectionBase(base string) error {
	if strings.TrimSpace(base) == "" {
		return errors.New("snapshot selection base cannot be empty")
	}
	if strings.IndexByte(base, 0) >= 0 {
		return errors.New("snapshot selection base contains NUL byte")
	}
	if !filepath.IsAbs(base) {
		return fmt.Errorf("snapshot selection base must be absolute: %q", base)
	}
	return nil
}

func deriveSnapshotCreateType(paths []string) (SnapshotType, error) {
	if len(paths) == 0 {
		return SnapshotTypeFull, nil
	}
	return SnapshotTypePartial, nil
}

func validateSnapshotCreateParent(snapshotID, parentID string, hasPaths bool) error {
	if parentID == "" {
		return nil
	}
	if hasPaths {
		return fmt.Errorf("--from is currently supported only for full snapshots")
	}
	if parentID == snapshotID {
		return fmt.Errorf("parent snapshot %q cannot reference itself", parentID)
	}
	return nil
}

func (e *DefaultEngine) generateSnapshotID() (string, error) {
	generator := e.snapshotIDGenerator
	if generator == nil {
		generator = secureSnapshotIDGenerator
	}
	generatedID, err := generator()
	if err != nil {
		return "", err
	}
	trimmed := strings.TrimSpace(generatedID)
	if trimmed == "" {
		return "", fmt.Errorf("engine: generated snapshot id cannot be empty")
	}
	return trimmed, nil
}

func validateSnapshotCreatePaths(paths []string) error {
	for _, rawPath := range paths {
		if _, err := snapshot.NormalizeSnapshotPath(rawPath); err != nil {
			return fmt.Errorf("validate snapshot create path %q: %w", rawPath, err)
		}
	}
	return nil
}
