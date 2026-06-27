package engine

import (
	"context"
	"fmt"
	"strings"

	"github.com/franchoy/coldkeep/internal/storage"
)

func normalizeRestoreStoredPathRequest(req RestoreStoredPathRequest) (RestoreStoredPathRequest, error) {
	normalized, err := normalizeRestoreStoredPath(req)
	if err != nil {
		return RestoreStoredPathRequest{}, err
	}
	if err := validateRestoreStoredPathMetadataOptions(normalized); err != nil {
		return RestoreStoredPathRequest{}, err
	}
	if normalized.DestinationMode, err = normalizeRestoreStoredPathMode(normalized.DestinationMode); err != nil {
		return RestoreStoredPathRequest{}, err
	}
	if err := validateRestoreStoredPathDestination(normalized); err != nil {
		return RestoreStoredPathRequest{}, err
	}
	return normalized, nil
}

func normalizeRestoreStoredPath(req RestoreStoredPathRequest) (RestoreStoredPathRequest, error) {
	req.StoredPath = strings.TrimSpace(req.StoredPath)
	if req.StoredPath == "" {
		return RestoreStoredPathRequest{}, fmt.Errorf("engine: restore stored path is required")
	}
	return req, nil
}

func validateRestoreStoredPathMetadataOptions(req RestoreStoredPathRequest) error {
	if req.StrictMetadata && req.NoMetadata {
		return fmt.Errorf("engine: restore stored path strict metadata and no metadata are mutually exclusive")
	}
	return nil
}

func normalizeRestoreStoredPathMode(mode RestoreDestinationMode) (RestoreDestinationMode, error) {
	switch mode {
	case "":
		return RestoreDestinationOriginal, nil
	case RestoreDestinationOriginal, RestoreDestinationPrefix, RestoreDestinationOverride:
		return mode, nil
	default:
		return "", fmt.Errorf("engine: invalid restore stored-path destination mode %q", mode)
	}
}

func validateRestoreStoredPathDestination(req RestoreStoredPathRequest) error {
	hasDestinationRoot := strings.TrimSpace(req.DestinationRoot) != ""
	hasDestinationPath := strings.TrimSpace(req.DestinationPath) != ""

	switch req.DestinationMode {
	case RestoreDestinationOriginal:
		return validateRestoreStoredPathOriginalDestination(hasDestinationRoot, hasDestinationPath)
	case RestoreDestinationPrefix:
		return validateRestoreStoredPathPrefixDestination(hasDestinationRoot, hasDestinationPath)
	case RestoreDestinationOverride:
		return validateRestoreStoredPathOverrideDestination(hasDestinationRoot, hasDestinationPath)
	}

	return nil
}

func validateRestoreStoredPathOriginalDestination(hasDestinationRoot, hasDestinationPath bool) error {
	if hasDestinationRoot {
		return fmt.Errorf("engine: restore stored path original mode does not accept a destination root")
	}
	if hasDestinationPath {
		return fmt.Errorf("engine: restore stored path original mode does not accept a destination path")
	}
	return nil
}

func validateRestoreStoredPathPrefixDestination(hasDestinationRoot, hasDestinationPath bool) error {
	if !hasDestinationRoot {
		return fmt.Errorf("engine: restore stored path prefix mode requires a destination root")
	}
	if hasDestinationPath {
		return fmt.Errorf("engine: restore stored path prefix mode does not accept an exact destination path")
	}
	return nil
}

func validateRestoreStoredPathOverrideDestination(hasDestinationRoot, hasDestinationPath bool) error {
	if !hasDestinationPath {
		return fmt.Errorf("engine: restore stored path override mode requires an exact destination path")
	}
	if hasDestinationRoot {
		return fmt.Errorf("engine: restore stored path override mode does not accept a destination root")
	}
	return nil
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
