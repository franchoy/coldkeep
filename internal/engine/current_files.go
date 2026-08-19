package engine

import (
	"context"
	"fmt"
	"strings"

	"github.com/franchoy/coldkeep/internal/catalog"
)

func (e *DefaultEngine) ListFiles(ctx context.Context, req ListFilesRequest) (ListFilesResult, error) {
	if err := validateFileQueryPage(req.Limit, req.Offset); err != nil {
		return ListFilesResult{}, TranslateErrorAs("list files", ErrorInvalidArgument, err)
	}
	refs, err := catalog.NewServiceFromSQL(e.config.DB).ListCurrentFiles(ctx, catalog.CurrentFilePage{
		Limit: req.Limit, Offset: req.Offset,
	})
	if err != nil {
		return ListFilesResult{}, TranslateError("list files", err)
	}
	return ListFilesResult{Files: currentFilesFromCatalog(refs)}, nil
}

func (e *DefaultEngine) SearchFiles(ctx context.Context, req SearchFilesRequest) (SearchFilesResult, error) {
	if err := validateSearchFilesRequest(req); err != nil {
		return SearchFilesResult{}, TranslateErrorAs("search files", ErrorInvalidArgument, err)
	}
	refs, err := catalog.NewServiceFromSQL(e.config.DB).SearchCurrentFiles(ctx, catalog.CurrentFileSearch{
		NameContains: append([]string(nil), req.NameContains...),
		MinSizeBytes: append([]int64(nil), req.MinSizeBytes...),
		MaxSizeBytes: append([]int64(nil), req.MaxSizeBytes...),
		Page: catalog.CurrentFilePage{
			Limit: req.Limit, Offset: req.Offset,
		},
	})
	if err != nil {
		return SearchFilesResult{}, TranslateError("search files", err)
	}
	return SearchFilesResult{Files: currentFilesFromCatalog(refs)}, nil
}

func validateSearchFilesRequest(req SearchFilesRequest) error {
	if err := validateFileQueryPage(req.Limit, req.Offset); err != nil {
		return err
	}
	for _, name := range req.NameContains {
		if strings.TrimSpace(name) == "" {
			return fmt.Errorf("engine: search files name filter cannot be empty")
		}
	}
	for _, size := range req.MinSizeBytes {
		if size < 0 {
			return fmt.Errorf("engine: search files minimum size must be non-negative")
		}
	}
	for _, size := range req.MaxSizeBytes {
		if size < 0 {
			return fmt.Errorf("engine: search files maximum size must be non-negative")
		}
	}
	return nil
}

func validateFileQueryPage(limit, offset *int64) error {
	if limit != nil {
		if *limit < 0 {
			return fmt.Errorf("engine: file query limit must be non-negative")
		}
		if *limit > MaxFileQueryLimit {
			return fmt.Errorf("engine: file query limit must be <= %d", MaxFileQueryLimit)
		}
	}
	if offset != nil && *offset < 0 {
		return fmt.Errorf("engine: file query offset must be non-negative")
	}
	return nil
}

func currentFilesFromCatalog(refs []catalog.CurrentFileRef) []CurrentFile {
	files := make([]CurrentFile, len(refs))
	for i, ref := range refs {
		files[i] = CurrentFile{
			ID: ref.LogicalFileID, Name: ref.Path, FileHash: ref.FileHash,
			SizeBytes: ref.SizeBytes, CreatedAt: ref.CreatedAt.Format("2006-01-02 15:04:05"),
		}
	}
	return files
}
