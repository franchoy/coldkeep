package catalog

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	filestate "github.com/franchoy/coldkeep/internal/status"
)

func (s *Service) ListCurrentFiles(ctx context.Context, page CurrentFilePage) ([]CurrentFileRef, error) {
	if err := validateCurrentFilePage(page); err != nil {
		return nil, err
	}
	return s.queryCurrentFiles(ctx, CurrentFileSearch{Page: page}, "list current files")
}

func (s *Service) SearchCurrentFiles(ctx context.Context, filter CurrentFileSearch) ([]CurrentFileRef, error) {
	if err := validateCurrentFileSearch(filter); err != nil {
		return nil, err
	}
	return s.queryCurrentFiles(ctx, filter, "search current files")
}

func validateCurrentFileSearch(filter CurrentFileSearch) error {
	if err := validateCurrentFilePage(filter.Page); err != nil {
		return err
	}
	for _, name := range filter.NameContains {
		if strings.TrimSpace(name) == "" {
			return NewError(ErrorInvalidArgument, "search current files", "name_filter_nonblank", "name filter cannot be empty", nil)
		}
	}
	for _, size := range filter.MinSizeBytes {
		if size < 0 {
			return NewError(ErrorInvalidArgument, "search current files", "minimum_size_nonnegative", "minimum size must be non-negative", nil)
		}
	}
	for _, size := range filter.MaxSizeBytes {
		if size < 0 {
			return NewError(ErrorInvalidArgument, "search current files", "maximum_size_nonnegative", "maximum size must be non-negative", nil)
		}
	}
	return nil
}

func validateCurrentFilePage(page CurrentFilePage) error {
	if page.Limit != nil {
		if *page.Limit < 0 {
			return NewError(ErrorInvalidArgument, "query current files", "limit_nonnegative", "limit must be non-negative", nil)
		}
		if *page.Limit > MaxCurrentFilePageSize {
			return NewError(ErrorInvalidArgument, "query current files", "limit_bounded", fmt.Sprintf("limit must be <= %d", MaxCurrentFilePageSize), nil)
		}
	}
	if page.Offset != nil && *page.Offset < 0 {
		return NewError(ErrorInvalidArgument, "query current files", "offset_nonnegative", "offset must be non-negative", nil)
	}
	return nil
}

func (s *Service) queryCurrentFiles(ctx context.Context, filter CurrentFileSearch, operation string) ([]CurrentFileRef, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, currentFileCatalogError(operation, err)
	}

	var query strings.Builder
	query.WriteString(`
SELECT lf.id, pf.path, lf.file_hash, lf.total_size, lf.created_at
FROM physical_file pf
JOIN logical_file lf ON lf.id = pf.logical_file_id
WHERE lf.status = $1`)
	params := []any{filestate.LogicalFileCompleted}
	for _, name := range filter.NameContains {
		query.WriteString(fmt.Sprintf(" AND LOWER(pf.path) LIKE LOWER($%d)", len(params)+1))
		params = append(params, "%"+name+"%")
	}
	for _, size := range filter.MinSizeBytes {
		query.WriteString(fmt.Sprintf(" AND lf.total_size >= $%d", len(params)+1))
		params = append(params, size)
	}
	for _, size := range filter.MaxSizeBytes {
		query.WriteString(fmt.Sprintf(" AND lf.total_size <= $%d", len(params)+1))
		params = append(params, size)
	}

	rows, err := s.db.QueryContext(ctx, query.String(), params...)
	if err != nil {
		return nil, currentFileCatalogError(operation, err)
	}
	defer func() { _ = rows.Close() }()

	refs := make([]CurrentFileRef, 0)
	for rows.Next() {
		var ref CurrentFileRef
		if err := rows.Scan(&ref.LogicalFileID, &ref.Path, &ref.FileHash, &ref.SizeBytes, &ref.CreatedAt); err != nil {
			return nil, currentFileCatalogError(operation, err)
		}
		refs = append(refs, ref)
	}
	if err := rows.Err(); err != nil {
		return nil, currentFileCatalogError(operation, err)
	}
	sort.Slice(refs, func(i, j int) bool {
		if refs[i].Path != refs[j].Path {
			return refs[i].Path < refs[j].Path
		}
		return refs[i].LogicalFileID < refs[j].LogicalFileID
	})
	return paginateCurrentFiles(refs, filter.Page), nil
}

func paginateCurrentFiles(refs []CurrentFileRef, page CurrentFilePage) []CurrentFileRef {
	start := 0
	if page.Offset != nil && *page.Offset < int64(len(refs)) {
		start = int(*page.Offset)
	} else if page.Offset != nil {
		start = len(refs)
	}
	end := len(refs)
	if page.Limit != nil && *page.Limit < int64(end-start) {
		end = start + int(*page.Limit)
	}
	return refs[start:end]
}

func currentFileCatalogError(operation string, err error) error {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return NewError(ErrorCancelled, operation, "", operation+" cancelled", err)
	}
	return NewError(ErrorOperationFailed, operation, "", operation+" query failed", err)
}
