package shared

type Version string

const (
	// VersionV1SimpleRolling is the canonical persisted identifier for the
	// historical v1 simple rolling chunker. Database metadata, validation, and
	// tests must use this exact value.
	VersionV1SimpleRolling Version = "v1-simple-rolling"
)
