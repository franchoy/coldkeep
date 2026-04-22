package chunk

import "github.com/franchoy/coldkeep/internal/chunk/shared"

type Version = shared.Version

const (
	VersionV1SimpleRolling = shared.VersionV1SimpleRolling
	DefaultChunkerVersion  = VersionV1SimpleRolling
)
