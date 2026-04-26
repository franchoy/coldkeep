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

func (s *Service) getSnapshotFiles(ctx context.Context, snapshotID int64) ([]NodeID, error) {
	rows, err := s.db.QueryContext(
		ctx,
		`SELECT logical_file_id
		 FROM snapshot_file
		 WHERE snapshot_id = ?
		 ORDER BY logical_file_id`,
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
		 ORDER BY chunk_order, chunk_id`,
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
