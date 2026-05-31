package engine

// StatsRequest carries parameters for the Stats operation.
type StatsRequest struct{}

// StatsResult carries the result of the Stats operation.
// Fields will be populated in Phase 2 wrapper-only implementation.
type StatsResult struct{}

// InspectRequest carries parameters for the Inspect operation.
type InspectRequest struct {
	// FileID is the logical file identifier to inspect.
	FileID int64
}

// InspectResult carries the result of the Inspect operation.
// Fields will be populated in Phase 2 wrapper-only implementation.
type InspectResult struct{}

// VerifyRequest carries parameters for the Verify operation.
type VerifyRequest struct {
	// Level is the verification level: "standard", "full", or "deep".
	Level string
}

// VerifyResult carries the result of the Verify operation.
// Fields will be populated in Phase 2 wrapper-only implementation.
type VerifyResult struct{}
