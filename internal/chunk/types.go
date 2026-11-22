package chunk

import "github.com/franchoy/coldkeep/v2/internal/chunk/shared"

// Info is the shared chunk metadata contract returned by all chunkers.
type Info = shared.Info

// Result contains one emitted chunk and its metadata.
type Result = shared.Result
