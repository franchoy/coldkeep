package graph

import "context"

func (s *Service) GetReachableChunks(ctx context.Context, snapshotIDs []int64) (map[int64]struct{}, error) {
	reachable := make(map[int64]struct{})

	var start []NodeID
	for _, id := range snapshotIDs {
		start = append(start, NodeID{Type: EntitySnapshot, ID: id})
	}

	err := s.Traverse(ctx, start, func(n NodeID) error {
		if n.Type == EntityChunk {
			reachable[n.ID] = struct{}{}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	return reachable, nil
}
