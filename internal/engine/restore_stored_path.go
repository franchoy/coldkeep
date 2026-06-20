package engine

import (
	"context"
	"fmt"
	"strings"

	"github.com/franchoy/coldkeep/internal/storage"
)

func normalizeRestoreStoredPathRequest(req RestoreStoredPathRequest) (RestoreStoredPathRequest, error) {
	req.StoredPath = strings.TrimSpace(req.StoredPath)
	if req.StoredPath == "" {
		return RestoreStoredPathRequest{}, fmt.Errorf("engine: restore stored path is required")
	}
	if req.StrictMetadata && req.NoMetadata {
		return RestoreStoredPathRequest{}, fmt.Errorf("engine: restore stored path strict metadata and no metadata are mutually exclusive")
	}

	switch req.DestinationMode {
	case "":
		req.DestinationMode = RestoreDestinationOriginal
	case RestoreDestinationOriginal, RestoreDestinationPrefix, RestoreDestinationOverride:
	default:
		return RestoreStoredPathRequest{}, fmt.Errorf("engine: invalid restore stored-path destination mode %q", req.DestinationMode)
	}

	hasDestinationRoot := strings.TrimSpace(req.DestinationRoot) != ""
	hasDestinationPath := strings.TrimSpace(req.DestinationPath) != ""

	switch req.DestinationMode {
	case RestoreDestinationOriginal:
		if hasDestinationRoot {
			return RestoreStoredPathRequest{}, fmt.Errorf("engine: restore stored path original mode does not accept a destination root")
		}
		if hasDestinationPath {
			return RestoreStoredPathRequest{}, fmt.Errorf("engine: restore stored path original mode does not accept a destination path")
		}
	case RestoreDestinationPrefix:
		if !hasDestinationRoot {
			return RestoreStoredPathRequest{}, fmt.Errorf("engine: restore stored path prefix mode requires a destination root")
		}
		if hasDestinationPath {
			return RestoreStoredPathRequest{}, fmt.Errorf("engine: restore stored path prefix mode does not accept an exact destination path")
		}
	case RestoreDestinationOverride:
		if !hasDestinationPath {
			return RestoreStoredPathRequest{}, fmt.Errorf("engine: restore stored path override mode requires an exact destination path")
		}
		if hasDestinationRoot {
			return RestoreStoredPathRequest{}, fmt.Errorf("engine: restore stored path override mode does not accept a destination root")
		}
	}

	return req, nil
}

func (e *DefaultEngine) validateRestoreStoredPathDependencies() error {
	if e.config.DB == nil {
		return fmt.Errorf("engine: restore stored path database is required")
	}
	if strings.TrimSpace(e.config.ContainerDir) == "" {
		return fmt.Errorf("engine: restore stored path container directory is required")
	}
	return nil
}

func (e *DefaultEngine) restoreStorageContext() storage.StorageContext {
	return storage.StorageContext{
		DB:           e.config.DB,
		ContainerDir: e.config.ContainerDir,
	}
}

func toStorageRestoreDestinationMode(mode RestoreDestinationMode) storage.RestoreDestinationMode {
	switch mode {
	case RestoreDestinationOriginal:
		return storage.RestoreDestinationOriginal
	case RestoreDestinationPrefix:
		return storage.RestoreDestinationPrefix
	case RestoreDestinationOverride:
		return storage.RestoreDestinationOverride
	default:
		panic("unexpected restore stored-path destination mode")
	}
}

func storageRestoreDestination(req RestoreStoredPathRequest) string {
	switch req.DestinationMode {
	case RestoreDestinationPrefix:
		return req.DestinationRoot
	case RestoreDestinationOverride:
		return req.DestinationPath
	default:
		return ""
	}
}

func (e *DefaultEngine) RestoreStoredPath(ctx context.Context, req RestoreStoredPathRequest) (RestoreStoredPathResult, error) {
	if err := ctx.Err(); err != nil {
		return RestoreStoredPathResult{}, err
	}

	normalized, err := normalizeRestoreStoredPathRequest(req)
	if err != nil {
		return RestoreStoredPathResult{}, err
	}
	if err := e.validateRestoreStoredPathDependencies(); err != nil {
		return RestoreStoredPathResult{}, err
	}

	storageResult, err := storage.RestoreFileByStoredPathWithStorageContextResultOptions(
		e.restoreStorageContext(),
		normalized.StoredPath,
		storage.RestoreOptions{
			Overwrite:       normalized.Overwrite,
			DestinationMode: toStorageRestoreDestinationMode(normalized.DestinationMode),
			Destination:     storageRestoreDestination(normalized),
			StrictMetadata:  normalized.StrictMetadata,
			NoMetadata:      normalized.NoMetadata,
		},
	)
	if err != nil {
		return RestoreStoredPathResult{}, err
	}

	return RestoreStoredPathResult{
		StoredPath:      normalized.StoredPath,
		FileID:          storageResult.FileID,
		DestinationMode: normalized.DestinationMode,
		DestinationPath: storageResult.OutputPath,
		RestoredHash:    storageResult.RestoredHash,
	}, nil
}
