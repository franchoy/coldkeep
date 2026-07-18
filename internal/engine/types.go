package engine

import "github.com/franchoy/coldkeep/internal/observability"

// StatsRequest carries parameters for the Stats operation.
type StatsRequest struct {
	// IncludeContainers requests container-level statistics in the result.
	IncludeContainers bool
	// Trace controls optional trace-event emission during stats collection.
	Trace observability.TraceOptions
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

// VerifyResult is the active result type for Engine.Verify. It is intentionally
// empty and success-only: a nil error means verification passed at the
// requested level, while non-nil errors preserve the verification failure
// chain. No rich verification payload is frozen; any future result expansion
// requires an explicit contract change.
type VerifyResult struct{}
