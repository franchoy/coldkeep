package engine

import (
	"context"
	"fmt"
	"strings"

	"github.com/franchoy/coldkeep/internal/snapshot"
)

type preparedSnapshotDeleteRequest struct {
	snapshotID string
	mode       SnapshotDeleteMode
}

func (e *DefaultEngine) SnapshotDelete(ctx context.Context, req SnapshotDeleteRequest) (_ SnapshotDeleteResult, outErr error) {
	defer func() { outErr = TranslateError("snapshot_delete", outErr) }()
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return SnapshotDeleteResult{}, err
	}

	prepared, err := prepareSnapshotDeleteRequest(req)
	if err != nil {
		return SnapshotDeleteResult{}, TranslateErrorAs("snapshot_delete", ErrorInvalidArgument, err)
	}

	switch prepared.mode {
	case SnapshotDeleteModePreview:
		preview, err := snapshot.LoadDeleteLineagePreview(ctx, e.config.DB, prepared.snapshotID)
		if err != nil {
			return SnapshotDeleteResult{}, err
		}
		return SnapshotDeleteResult{
			SnapshotID: preview.SnapshotID,
			Mode:       SnapshotDeleteModePreview,
			Deleted:    false,
			Preview:    mapSnapshotDeletePreview(preview),
		}, nil
	case SnapshotDeleteModeExecute:
		result, err := snapshot.DeleteSnapshotWithResult(ctx, e.config.DB, prepared.snapshotID)
		if err != nil {
			return SnapshotDeleteResult{}, err
		}
		return SnapshotDeleteResult{
			SnapshotID: result.SnapshotID,
			Mode:       SnapshotDeleteModeExecute,
			Deleted:    result.Deleted,
		}, nil
	default:
		return SnapshotDeleteResult{}, fmt.Errorf("engine: unsupported snapshot delete mode %q", prepared.mode)
	}
}

func prepareSnapshotDeleteRequest(req SnapshotDeleteRequest) (preparedSnapshotDeleteRequest, error) {
	snapshotID := strings.TrimSpace(req.SnapshotID)
	if snapshotID == "" {
		return preparedSnapshotDeleteRequest{}, fmt.Errorf("snapshot id cannot be empty")
	}

	switch req.Mode {
	case SnapshotDeleteModePreview, SnapshotDeleteModeExecute:
		return preparedSnapshotDeleteRequest{
			snapshotID: snapshotID,
			mode:       req.Mode,
		}, nil
	case "":
		return preparedSnapshotDeleteRequest{}, fmt.Errorf("engine: snapshot delete mode is required")
	default:
		return preparedSnapshotDeleteRequest{}, fmt.Errorf("engine: unknown snapshot delete mode %q", req.Mode)
	}
}

func mapSnapshotDeletePreview(preview *snapshot.DeleteLineagePreview) *SnapshotDeletePreviewResult {
	if preview == nil {
		return nil
	}

	parent := SnapshotDeleteParent{State: SnapshotDeleteParentNone}
	if preview.ParentID.Valid {
		parent.ID = preview.ParentID.String
		parent.State = SnapshotDeleteParentPresent
		if preview.ParentMissing {
			parent.State = SnapshotDeleteParentMissing
		}
	}

	return &SnapshotDeletePreviewResult{
		Parent:      parent,
		Children:    append([]string(nil), preview.ChildSnapshotIDs...),
		TotalFiles:  preview.TotalFiles,
		UniqueFiles: preview.UniqueFiles,
		SharedFiles: preview.SharedFiles,
	}
}
