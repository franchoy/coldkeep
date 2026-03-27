package blocks

import "context"

// Transformer defines how a chunk is transformed into a stored block
// and how a stored block is transformed back into plaintext.
type Transformer interface {
	Encode(ctx context.Context, in EncodeInput) (*EncodedBlock, error)
	Decode(ctx context.Context, in DecodeInput) ([]byte, error)
}
