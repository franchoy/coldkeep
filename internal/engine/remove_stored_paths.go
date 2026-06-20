package engine

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/franchoy/coldkeep/internal/invariants"
	"github.com/franchoy/coldkeep/internal/storage"
)

type preparedRemoveStoredPathTarget struct {
	rawTarget  string
	storedPath string
	executable bool
	item       RemoveStoredPathItemResult
}

func validateRemoveStoredPathsRequest(req RemoveStoredPathsRequest) error {
	if len(req.StoredPaths) == 0 {
		return fmt.Errorf("engine: remove stored paths requires at least one target")
	}
	return nil
}

func (e *DefaultEngine) validateRemoveStoredPathsDependencies() error {
	if e == nil || e.config.DB == nil {
		return fmt.Errorf("engine: remove stored paths database is required")
	}
	return nil
}

func prepareRemoveStoredPathTargets(targets []string) []preparedRemoveStoredPathTarget {
	prepared := make([]preparedRemoveStoredPathTarget, 0, len(targets))
	seen := make(map[string]struct{}, len(targets))

	for _, rawTarget := range targets {
		storedPath := strings.TrimSpace(rawTarget)
		if storedPath == "" {
			prepared = append(prepared, preparedRemoveStoredPathTarget{
				rawTarget: rawTarget,
				item: RemoveStoredPathItemResult{
					RawTarget:      rawTarget,
					StoredPath:     "",
					MappingRemoved: false,
					Status:         BatchItemFailed,
					Error:          "stored path is required",
				},
			})
			continue
		}

		if _, duplicate := seen[storedPath]; duplicate {
			prepared = append(prepared, preparedRemoveStoredPathTarget{
				rawTarget:  rawTarget,
				storedPath: storedPath,
				item: RemoveStoredPathItemResult{
					RawTarget:      rawTarget,
					StoredPath:     storedPath,
					MappingRemoved: false,
					Status:         BatchItemSkipped,
					Error:          "duplicate target",
				},
			})
			continue
		}

		seen[storedPath] = struct{}{}
		prepared = append(prepared, preparedRemoveStoredPathTarget{
			rawTarget:  rawTarget,
			storedPath: storedPath,
			executable: true,
		})
	}

	return prepared
}

func (e *DefaultEngine) removeStoredPaths(req RemoveStoredPathsRequest) RemoveStoredPathsResult {
	prepared := prepareRemoveStoredPathTargets(req.StoredPaths)
	result := RemoveStoredPathsResult{
		DryRun:        req.DryRun,
		ExecutionMode: ExecutionModeSequential,
		Items:         make([]RemoveStoredPathItemResult, 0, len(prepared)),
	}

	for _, target := range prepared {
		if !target.executable {
			appendRemoveStoredPathItem(&result, target.item)
			continue
		}

		var item RemoveStoredPathItemResult
		if req.DryRun {
			item = e.dryRunRemoveStoredPath(target)
		} else {
			item = e.removeStoredPath(target)
		}
		appendRemoveStoredPathItem(&result, item)
		if req.FailFast && item.Status == BatchItemFailed {
			break
		}
	}

	finalizeBatchSummary(&result.Summary, len(req.StoredPaths))
	return result
}

func (e *DefaultEngine) dryRunRemoveStoredPath(target preparedRemoveStoredPathTarget) RemoveStoredPathItemResult {
	logicalFileID, err := storage.LookupLogicalFileIDByStoredPath(e.config.DB, target.storedPath)
	if err != nil {
		item := failedRemoveStoredPathItem(target.rawTarget, target.storedPath, err.Error())
		annotateRemoveStoredPathInvariant(&item, err)
		if err == sql.ErrNoRows {
			item.Error = fmt.Sprintf("physical_file[%q]: not found (never stored)", target.storedPath)
		}
		return item
	}

	return RemoveStoredPathItemResult{
		RawTarget:         target.rawTarget,
		StoredPath:        target.storedPath,
		LogicalFileID:     logicalFileID,
		MappingRemoved:    false,
		Status:            BatchItemPlanned,
		RemainingRefCount: 0,
	}
}

func (e *DefaultEngine) removeStoredPath(target preparedRemoveStoredPathTarget) RemoveStoredPathItemResult {
	result, err := storage.RemoveFileByStoredPathWithStorageContextResult(storage.StorageContext{DB: e.config.DB}, target.storedPath)
	if err != nil {
		item := failedRemoveStoredPathItem(target.rawTarget, target.storedPath, err.Error())
		annotateRemoveStoredPathInvariant(&item, err)
		return item
	}

	return RemoveStoredPathItemResult{
		RawTarget:         target.rawTarget,
		StoredPath:        target.storedPath,
		LogicalFileID:     result.LogicalFileID,
		RemainingRefCount: result.RemainingRefCount,
		MappingRemoved:    result.Removed,
		Status:            BatchItemOK,
	}
}

func failedRemoveStoredPathItem(rawTarget string, storedPath string, message string) RemoveStoredPathItemResult {
	return RemoveStoredPathItemResult{
		RawTarget:      rawTarget,
		StoredPath:     storedPath,
		MappingRemoved: false,
		Status:         BatchItemFailed,
		Error:          message,
	}
}

func annotateRemoveStoredPathInvariant(item *RemoveStoredPathItemResult, err error) {
	if item == nil || err == nil {
		return
	}

	if code, ok := invariants.Code(err); ok {
		item.InvariantCode = code
		item.RecommendedAction = invariants.RecommendedActionForCode(code)
		return
	}

	message := err.Error()
	switch {
	case strings.Contains(message, "logical_file.ref_count invariant mismatch"),
		strings.Contains(message, "invalid logical_file.ref_count transition"):
		item.InvariantCode = invariants.CodePhysicalGraphRefCountMismatch
		item.RecommendedAction = invariants.RecommendedActionForCode(invariants.CodePhysicalGraphRefCountMismatch)
	}
}

func appendRemoveStoredPathItem(result *RemoveStoredPathsResult, item RemoveStoredPathItemResult) {
	result.Items = append(result.Items, item)
	switch item.Status {
	case BatchItemFailed:
		result.Summary.Failed++
	case BatchItemSkipped:
		result.Summary.Skipped++
	default:
		result.Summary.OK++
	}
}
