package chunk

import (
	"regexp"
	"strings"

	"github.com/franchoy/coldkeep/internal/chunk/shared"
)

// Version is the chunker version identifier type used across the abstraction
// layer and persisted metadata.
type Version = shared.Version

const (
	// VersionV1SimpleRolling is the canonical persisted value for repository data
	// produced by the historical v1 simple rolling chunker.
	VersionV1SimpleRolling = shared.VersionV1SimpleRolling
	// VersionV2FastCDC is the canonical persisted value for the v2 fastcdc chunker.
	VersionV2FastCDC      = Version("v2-fastcdc")
	DefaultChunkerVersion = VersionV1SimpleRolling
)

var chunkerVersionPattern = regexp.MustCompile(`^v[0-9]+(?:-[a-z0-9]+)+$`)

// IsWellFormedVersion reports whether a persisted chunker version string is
// syntactically valid metadata.
//
// Policy:
// - restore/replay paths only require metadata to be well-formed and non-empty
// - they do NOT require the local binary to implement that specific version
//
// Examples of accepted versions: "v1-simple-rolling", "v9-future-cdc".
func IsWellFormedVersion(version Version) bool {
	trimmed := strings.TrimSpace(string(version))
	if trimmed == "" {
		return false
	}
	return chunkerVersionPattern.MatchString(trimmed)
}
