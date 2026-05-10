package transforms

// IdentityTransform is the no-op transform: it passes bytes through unchanged.
// It represents the "none" storage mode and is the default when no transforms
// are configured. This is the baseline against which all other transforms are measured.
type IdentityTransform struct{}

// Name returns the stable identifier for the identity (no-op) transform.
func (t *IdentityTransform) Name() string { return "none" }

// Encode returns input unchanged.
func (t *IdentityTransform) Encode(input []byte) ([]byte, error) { return input, nil }

// Decode returns input unchanged.
func (t *IdentityTransform) Decode(input []byte) ([]byte, error) { return input, nil }
