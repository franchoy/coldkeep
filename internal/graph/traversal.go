package graph

import (
	"context"
	"database/sql"
	"strconv"
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
	return s.TraverseWithOptions(ctx, start, visit, TraversalOptions{})
}

func (s *Service) TraverseWithOptions(
	ctx context.Context,
	start []NodeID,
	visit func(NodeID) error,
	opts TraversalOptions,
) error {
	if ctx == nil {
		ctx = context.Background()
	}
	emitTrace(opts.Trace, TraceEvent{
		Step:    "graph.traverse.start",
		Message: "starting graph traversal",
		Metadata: map[string]any{
			"start_nodes": len(start),
		},
	})

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
			emitTrace(opts.Trace, TraceEvent{
				Step:    "graph.node.skip_already_visited",
				Node:    n,
				Message: "skipping already visited node",
			})
			continue
		}
		visited[n] = struct{}{}
		emitTrace(opts.Trace, TraceEvent{
			Step:    "graph.node.visit",
			Node:    n,
			Message: "visiting node",
		})

		if err := visit(n); err != nil {
			return err
		}
		emitTrace(opts.Trace, TraceEvent{
			Step:    "graph.edge.load",
			Node:    n,
			Message: "loading outgoing edges",
		})

		neighbors, err := s.getOutgoing(ctx, n)
		if err != nil {
			return err
		}
		emitTrace(opts.Trace, TraceEvent{
			Step:    "graph.edge.loaded",
			Node:    n,
			Message: "loaded outgoing edges",
			Metadata: map[string]any{
				"neighbors": len(neighbors),
			},
		})

		queue = append(queue, neighbors...)
	}
	emitTrace(opts.Trace, TraceEvent{
		Step:    "graph.traverse.complete",
		Message: "completed graph traversal",
		Metadata: map[string]any{
			"visited_nodes": len(visited),
		},
	})

	return nil
}

func (s *Service) getOutgoing(ctx context.Context, n NodeID) ([]NodeID, error) {
	if s == nil || s.db == nil {
		return nil, nil
	}

	switch n.Type {
	case EntitySnapshot:
		return s.getSnapshotFiles(ctx, n.ID)
	case EntityLogicalFile:
		return s.getFileChunks(ctx, n.ID)
	case EntityChunk:
		return s.getChunkContainer(ctx, n.ID)
	default:
		return nil, nil
	}
}

func (s *Service) GetOutgoing(ctx context.Context, n NodeID) ([]NodeID, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	return s.getOutgoing(ctx, n)
}

func (s *Service) getSnapshotFiles(ctx context.Context, snapshotID int64) ([]NodeID, error) {
	rows, err := s.db.QueryContext(
		ctx,
		`SELECT logical_file_id
		 FROM snapshot_file
		 WHERE snapshot_id = ?
		 ORDER BY id`,
		strconv.FormatInt(snapshotID, 10),
	)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	out := make([]NodeID, 0)
	for rows.Next() {
		var logicalFileID int64
		if err := rows.Scan(&logicalFileID); err != nil {
			return nil, err
		}
		out = append(out, NodeID{Type: EntityLogicalFile, ID: logicalFileID})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return out, nil
}

func (s *Service) getFileChunks(ctx context.Context, logicalFileID int64) ([]NodeID, error) {
	rows, err := s.db.QueryContext(
		ctx,
		`SELECT chunk_id
		 FROM file_chunk
		 WHERE logical_file_id = ?
		 ORDER BY chunk_id`,
		logicalFileID,
	)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	out := make([]NodeID, 0)
	for rows.Next() {
		var chunkID int64
		if err := rows.Scan(&chunkID); err != nil {
			return nil, err
		}
		out = append(out, NodeID{Type: EntityChunk, ID: chunkID})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return out, nil
}

func (s *Service) getChunkContainer(ctx context.Context, chunkID int64) ([]NodeID, error) {
	var containerID int64
	err := s.db.QueryRowContext(
		ctx,
		`SELECT container_id
		 FROM blocks
		 WHERE chunk_id = ?`,
		chunkID,
	).Scan(&containerID)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}

	return []NodeID{{Type: EntityContainer, ID: containerID}}, nil
}
