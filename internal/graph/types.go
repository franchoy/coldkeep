package graph

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
}

type Edge struct {
	From NodeID
	To   NodeID
	Type string // "contains", "references", etc.
}
