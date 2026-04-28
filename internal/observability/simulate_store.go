package observability

import (
	"context"
	"fmt"

	filestate "github.com/franchoy/coldkeep/internal/status"
)

// SimulateStoreReport collects read-only aggregate stats for simulate store/store-folder.
func (s *Service) SimulateStoreReport(ctx context.Context, subcommand, path string) (*SimulateStoreReport, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if s == nil || s.db == nil {
		return nil, fmt.Errorf("collect simulate-store report: observability service requires non-nil db")
	}

	r := &SimulateStoreReport{
		Subcommand: subcommand,
		Path:       path,
	}

	queries := []struct {
		dest  any
		query string
		args  []any
	}{
		{&r.Files, `SELECT COUNT(*) FROM logical_file WHERE status = $1`, []any{filestate.LogicalFileCompleted}},
		{&r.LogicalSizeBytes, `SELECT COALESCE(SUM(total_size),0) FROM logical_file WHERE status = $1`, []any{filestate.LogicalFileCompleted}},
		{&r.Chunks, `SELECT COUNT(*) FROM chunk WHERE status = $1`, []any{filestate.ChunkCompleted}},
		{&r.Containers, `SELECT COUNT(DISTINCT b.container_id) FROM blocks b JOIN chunk c ON c.id = b.chunk_id WHERE c.status = $1`, []any{filestate.ChunkCompleted}},
		{&r.PhysicalSizeBytes, `SELECT COALESCE(SUM(b.stored_size),0) FROM blocks b JOIN chunk c ON c.id = b.chunk_id WHERE c.live_ref_count > 0 OR c.pin_count > 0`, nil},
	}
	for _, q := range queries {
		if err := s.db.QueryRowContext(ctx, q.query, q.args...).Scan(q.dest); err != nil {
			return nil, fmt.Errorf("collect simulate-store report: %w", err)
		}
	}

	if r.LogicalSizeBytes > 0 {
		r.DedupRatioPct = (1.0 - float64(r.PhysicalSizeBytes)/float64(r.LogicalSizeBytes)) * 100
	}

	return r, nil
}
