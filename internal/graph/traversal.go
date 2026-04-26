package graph

import (
	"context"
	"database/sql"
)

type Service struct {
	db *sql.DB
}

func NewService(db *sql.DB) *Service {
	return &Service{db: db}
}

func (s *Service) Traverse(
	ctx context.Context,
	start []NodeID,
	visit func(NodeID) error,
) error {
	if ctx == nil {
		ctx = context.Background()
	}

	queue := make([]NodeID, len(start))
	copy(queue, start)

	visited := make(map[NodeID]struct{}, len(start))

	for len(queue) > 0 {
		if err := ctx.Err(); err != nil {
			return err
		}

		n := queue[0]
		queue = queue[1:]

		if _, ok := visited[n]; ok {
			continue
		}
		visited[n] = struct{}{}

		if err := visit(n); err != nil {
			return err
		}

		neighbors, err := s.getOutgoing(ctx, n)
		if err != nil {
			return err
		}

		queue = append(queue, neighbors...)
	}

	return nil
}

// getOutgoing returns graph neighbors in deterministic order.
// Phase 2 step 3 only introduces traversal primitive; relation resolution is added later.
func (s *Service) getOutgoing(_ context.Context, _ NodeID) ([]NodeID, error) {
	return nil, nil
}
