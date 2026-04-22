package chunk

type Version string

const (
	VersionV1SimpleRolling Version = "v1-simple-rolling"
	DefaultChunkerVersion  Version = VersionV1SimpleRolling
)
