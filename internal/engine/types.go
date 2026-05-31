package engine

import "github.com/franchoy/coldkeep/internal/observability"

// StatsRequest carries parameters for the Stats operation.
type StatsRequest struct {
	// IncludeContainers requests container-level statistics in the result.
	IncludeContainers bool
}

// StatsResult carries the result of the Stats operation.
type StatsResult struct {
	// Raw is the underlying observability result.
	Raw *observability.StatsResult
}

// InspectRequest carries parameters for the Inspect operation.
type InspectRequest struct {
	// Entity is the type of entity to inspect.
	Entity observability.EntityType
	// EntityID is the string identifier for the entity.
	// For EntityRepository this field is ignored.
	EntityID string
	// Options controls relation traversal, depth, and trace behavior.
	Options observability.InspectOptions
}

// InspectResult carries the result of the Inspect operation.
type InspectResult struct {
	// Raw is the underlying observability result.
	Raw *observability.InspectResult
}

// VerifyRequest carries parameters for the Verify operation.
type VerifyRequest struct {
	// Level is the verification level: "fast", "standard", "full", or "deep".
	// Defaults to "standard" if empty.
	Level string
	// Target is the verification target: "system" or "file".
	// Defaults to "system" if empty.
	Target string
	// FileID is the logical file ID to verify when Target is "file".
	FileID int
}

// VerifyResult carries the result of the Verify operation.
// Verify is pass-or-fail; a nil error from Engine.Verify means the repository
// passed at the requested level. Non-nil errors preserve the underlying
// verify.VerifyFailure chain.
type VerifyResult struct{}
