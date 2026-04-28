package graph

import (
	"context"
)

func (s *Service) GetReverseReferences(ctx context.Context, target NodeID) ([]NodeID, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if s == nil || s.db == nil {
		return nil, nil
	}

	switch target.Type {
	case EntityChunk:
		return s.getChunkFiles(ctx, target.ID)
	case EntityLogicalFile:
		return s.getFileSnapshots(ctx, target.ID)
	case EntityContainer:
		return s.getContainerChunks(ctx, target.ID)
	default:
		return nil, nil
	}
}

func (s *Service) getChunkFiles(ctx context.Context, chunkID int64) ([]NodeID, error) {
	rows, err := s.db.QueryContext(
		ctx,
		`SELECT logical_file_id
		 FROM file_chunk
		 WHERE chunk_id = $1
		 ORDER BY logical_file_id`,
		chunkID,
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

func (s *Service) getFileSnapshots(ctx context.Context, logicalFileID int64) ([]NodeID, error) {
	rows, err := s.db.QueryContext(
		ctx,
		`SELECT snapshot_id
		 FROM snapshot_file
		 WHERE logical_file_id = $1
		 ORDER BY id`,
		logicalFileID,
	)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	out := make([]NodeID, 0)
	for rows.Next() {
		var snapshotIDRaw string
		if err := rows.Scan(&snapshotIDRaw); err != nil {
			return nil, err
		}
		out = append(out, NodeID{Type: EntitySnapshot, SID: snapshotIDRaw})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func (s *Service) getContainerChunks(ctx context.Context, containerID int64) ([]NodeID, error) {
	rows, err := s.db.QueryContext(
		ctx,
		`SELECT chunk_id
		 FROM blocks
		 WHERE container_id = $1
		 ORDER BY chunk_id`,
		containerID,
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
