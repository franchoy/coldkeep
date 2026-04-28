package graph

import "strconv"

type EntityType string

const (
	EntitySnapshot    EntityType = "snapshot"
	EntityLogicalFile EntityType = "logical_file"
	EntityChunk       EntityType = "chunk"
	EntityContainer   EntityType = "container"
)

type NodeID struct {
	Type EntityType
	ID   int64
	SID  string
}

func (n NodeID) SnapshotID() string {
	if n.Type != EntitySnapshot {
		return ""
	}
	if n.SID != "" {
		return n.SID
	}
	if n.ID == 0 {
		return ""
	}
	return int64ToString(n.ID)
}

func int64ToString(v int64) string {
	return strconv.FormatInt(v, 10)
}

type Edge struct {
	From NodeID
	To   NodeID
	Type string // "contains", "references", etc.
}
