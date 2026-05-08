// Package transforms defines the composable transform pipeline used during
// block persistence and retrieval. Each Transform operates on raw bytes,
// making stages independently testable and composable without side effects.
//
// The pipeline model:
//
//	Write: plaintext → [transform 1] → [transform 2] → ... → stored bytes
//	Read:  stored bytes → [reverse 2] → [reverse 1] → ... → plaintext
package transforms

// Transform is a single composable stage in the storage transform pipeline.
// Implementations must be stateless with respect to individual Encode/Decode calls;
// any key material or configuration is held in the struct fields.
//
// Encode transforms plaintext bytes into stored bytes for a single stage.
// Decode reverses the transformation, recovering the original bytes.
//
// Both directions must be deterministic for the same input when the transform
// is non-random (e.g. compression). For transforms that introduce randomness
// (e.g. encryption with a random nonce), the stored bytes carry the necessary
// metadata to reverse the operation without any external state.
type Transform interface {
	// Name returns a stable, lowercase identifier for this transform stage.
	// Used for metadata, diagnostics, and future capability negotiation.
	Name() string

	// Encode transforms input bytes into the stored representation.
	Encode(input []byte) ([]byte, error)

	// Decode reverses the transformation, recovering the original bytes.
	Decode(input []byte) ([]byte, error)
}
